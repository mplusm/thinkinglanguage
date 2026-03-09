use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use postgres::{Client, NoTls};
use std::sync::Arc;

use crate::engine::DataEngine;

/// Extract detailed error message from a postgres::Error.
/// The Display impl only shows "db error" — this extracts the actual
/// PostgreSQL error message, detail, hint, and SQLSTATE code.
fn pg_error_detail(e: &postgres::Error) -> String {
    if let Some(db_err) = e.as_db_error() {
        let mut msg = format!("{}: {}", db_err.severity(), db_err.message());
        if let Some(detail) = db_err.detail() {
            msg.push_str(&format!(" DETAIL: {detail}"));
        }
        if let Some(hint) = db_err.hint() {
            msg.push_str(&format!(" HINT: {hint}"));
        }
        if let Some(where_) = db_err.where_() {
            msg.push_str(&format!(" WHERE: {where_}"));
        }
        msg.push_str(&format!(" (SQLSTATE {})", db_err.code().code()));
        msg
    } else {
        format!("{e}")
    }
}

/// Connect to PostgreSQL, trying TLS first then falling back to NoTls.
fn pg_connect(conn_str: &str) -> Result<Client, String> {
    // Try TLS first (required by most cloud/remote PostgreSQL)
    if let Ok(tls_connector) = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()
    {
        let connector = postgres_native_tls::MakeTlsConnector::new(tls_connector);
        if let Ok(client) = Client::connect(conn_str, connector) {
            return Ok(client);
        }
    }
    // Fall back to NoTls (local connections, Unix sockets)
    Client::connect(conn_str, NoTls)
        .map_err(|e| format!("PostgreSQL connection error: {}", pg_error_detail(&e)))
}

/// Cursor FETCH size: rows transferred per network round trip.
/// 1M rows per fetch = ~50 round trips for a 50M row table.
const PG_CURSOR_FETCH_SIZE: usize = 1_000_000;

/// RecordBatch size for DataFusion parallelism (local chunking, no network).
const PG_BATCH_SIZE: usize = 100_000;

/// Map PostgreSQL type names to Arrow DataTypes.
fn pg_type_to_arrow(pg_type: &postgres::types::Type) -> DataType {
    match *pg_type {
        postgres::types::Type::BOOL => DataType::Boolean,
        postgres::types::Type::INT2 => DataType::Int16,
        postgres::types::Type::INT4 => DataType::Int32,
        postgres::types::Type::INT8 => DataType::Int64,
        postgres::types::Type::FLOAT4 => DataType::Float32,
        postgres::types::Type::FLOAT8 => DataType::Float64,
        postgres::types::Type::TEXT
        | postgres::types::Type::VARCHAR
        | postgres::types::Type::BPCHAR => DataType::Utf8,
        _ => DataType::Utf8, // fallback: convert to string
    }
}

/// Build a RecordBatch from a slice of postgres Rows using the given schema.
fn build_record_batch(rows: &[postgres::Row], schema: &Arc<Schema>) -> Result<RecordBatch, String> {
    let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array: Arc<dyn Array> = match field.data_type() {
            DataType::Boolean => {
                let values: Vec<Option<bool>> = rows.iter().map(|r| r.get(col_idx)).collect();
                Arc::new(BooleanArray::from(values))
            }
            DataType::Int16 => {
                let values: Vec<Option<i16>> = rows.iter().map(|r| r.get(col_idx)).collect();
                Arc::new(Int16Array::from(values))
            }
            DataType::Int32 => {
                let values: Vec<Option<i32>> = rows.iter().map(|r| r.get(col_idx)).collect();
                Arc::new(Int32Array::from(values))
            }
            DataType::Int64 => {
                let values: Vec<Option<i64>> = rows.iter().map(|r| r.get(col_idx)).collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::Float32 => {
                let values: Vec<Option<f32>> = rows.iter().map(|r| r.get(col_idx)).collect();
                Arc::new(Float32Array::from(values))
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = rows.iter().map(|r| r.get(col_idx)).collect();
                Arc::new(Float64Array::from(values))
            }
            _ => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| r.try_get::<_, String>(col_idx).ok())
                    .collect();
                Arc::new(StringArray::from(values))
            }
        };
        arrays.push(array);
    }

    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| format!("Arrow RecordBatch creation error: {e}"))
}

impl DataEngine {
    /// Read a PostgreSQL table into a DataFusion DataFrame.
    pub fn read_postgres(
        &self,
        conn_str: &str,
        table_name: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let query = format!("SELECT * FROM \"{}\"", table_name.replace('"', "\"\""));
        self.query_postgres(conn_str, &query, table_name)
    }

    /// Execute a custom SQL query against PostgreSQL and return a DataFusion DataFrame.
    /// Fetches 1M rows per cursor round trip, then subdivides into 100K-row RecordBatches
    /// for DataFusion parallelism. Falls back to a simple query if cursors are not
    /// supported (e.g., PgBouncer, some cloud proxies, or restricted permissions).
    pub fn query_postgres(
        &self,
        conn_str: &str,
        query: &str,
        register_as: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let mut client = pg_connect(conn_str)?;

        // Try cursor-based streaming first, fall back to simple query
        let result = self.query_postgres_cursor(&mut client, query);
        let (batches, final_schema) = match result {
            Ok(v) => v,
            Err(_) => {
                // Cursor failed — reconnect and use simple query fallback
                let mut client2 = pg_connect(conn_str)?;
                self.query_postgres_simple(&mut client2, query)?
            }
        };

        if batches.is_empty() {
            return Err(format!("Query returned no rows: {query}"));
        }

        let _ = self.ctx.deregister_table(register_as);
        self.register_batches(register_as, final_schema, batches)?;

        self.rt
            .block_on(self.ctx.table(register_as))
            .map_err(|e| format!("Table reference error: {e}"))
    }

    /// Cursor-based streaming: DECLARE CURSOR + FETCH in large chunks,
    /// then subdivide locally into smaller RecordBatches for DataFusion parallelism.
    fn query_postgres_cursor(
        &self,
        client: &mut Client,
        query: &str,
    ) -> Result<(Vec<RecordBatch>, Arc<Schema>), String> {
        let mut txn = client
            .transaction()
            .map_err(|e| format!("PostgreSQL transaction error: {}", pg_error_detail(&e)))?;

        txn.execute(
            &format!("DECLARE _tl_cursor NO SCROLL CURSOR FOR {query}"),
            &[],
        )
        .map_err(|e| format!("PostgreSQL DECLARE CURSOR error: {}", pg_error_detail(&e)))?;

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut schema: Option<Arc<Schema>> = None;

        loop {
            let rows = txn
                .query(
                    &format!("FETCH {PG_CURSOR_FETCH_SIZE} FROM _tl_cursor"),
                    &[],
                )
                .map_err(|e| format!("PostgreSQL FETCH error: {}", pg_error_detail(&e)))?;

            if rows.is_empty() {
                break;
            }

            if schema.is_none() {
                let columns = rows[0].columns();
                let fields: Vec<Field> = columns
                    .iter()
                    .map(|col| Field::new(col.name(), pg_type_to_arrow(col.type_()), true))
                    .collect();
                schema = Some(Arc::new(Schema::new(fields)));
            }

            // Subdivide the fetched chunk into smaller RecordBatches
            let s = schema.as_ref().unwrap();
            for chunk in rows.chunks(PG_BATCH_SIZE) {
                let batch = build_record_batch(chunk, s)?;
                batches.push(batch);
            }
        }

        txn.execute("CLOSE _tl_cursor", &[]).ok();
        txn.commit()
            .map_err(|e| format!("PostgreSQL COMMIT error: {}", pg_error_detail(&e)))?;

        let final_schema = schema.ok_or_else(|| "No schema from cursor query".to_string())?;
        Ok((batches, final_schema))
    }

    /// Simple query fallback: runs the full query and batches results locally.
    fn query_postgres_simple(
        &self,
        client: &mut Client,
        query: &str,
    ) -> Result<(Vec<RecordBatch>, Arc<Schema>), String> {
        let rows = client
            .query(query, &[])
            .map_err(|e| format!("PostgreSQL query error: {}", pg_error_detail(&e)))?;

        if rows.is_empty() {
            return Err("Query returned no rows".to_string());
        }

        let columns = rows[0].columns();
        let fields: Vec<Field> = columns
            .iter()
            .map(|col| Field::new(col.name(), pg_type_to_arrow(col.type_()), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let mut batches: Vec<RecordBatch> = Vec::new();
        for chunk in rows.chunks(PG_BATCH_SIZE) {
            let batch = build_record_batch(chunk, &schema)?;
            batches.push(batch);
        }

        Ok((batches, schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore] // Requires a running PostgreSQL instance
    fn test_read_postgres() {
        let engine = DataEngine::new();
        let df = engine
            .read_postgres(
                "host=localhost user=postgres password=postgres dbname=testdb",
                "users",
            )
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
    }

    #[test]
    #[ignore] // Requires a running PostgreSQL instance
    fn test_query_postgres_custom_sql() {
        let engine = DataEngine::new();
        let df = engine
            .query_postgres(
                "host=localhost user=postgres password=postgres dbname=testdb",
                "SELECT * FROM users LIMIT 10",
                "limited_users",
            )
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total <= 10);
    }
}
