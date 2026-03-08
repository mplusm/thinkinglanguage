use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use postgres::{Client, NoTls};
use std::sync::Arc;

use crate::engine::DataEngine;

/// Default batch size for cursor-based reads (rows per batch).
const PG_BATCH_SIZE: usize = 50_000;

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
fn build_record_batch(
    rows: &[postgres::Row],
    schema: &Arc<Schema>,
) -> Result<RecordBatch, String> {
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
    /// Uses server-side cursors to stream results in batches of 50K rows,
    /// avoiding full materialization in memory and enabling DataFusion parallelism.
    pub fn query_postgres(
        &self,
        conn_str: &str,
        query: &str,
        register_as: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let mut client = Client::connect(conn_str, NoTls)
            .map_err(|e| format!("PostgreSQL connection error: {e}"))?;

        // Use a server-side cursor for streaming large result sets
        let mut txn = client
            .transaction()
            .map_err(|e| format!("PostgreSQL transaction error: {e}"))?;

        txn.execute(
            &format!("DECLARE _tl_cursor NO SCROLL CURSOR FOR {query}"),
            &[],
        )
        .map_err(|e| format!("PostgreSQL DECLARE CURSOR error: {e}"))?;

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut schema: Option<Arc<Schema>> = None;

        loop {
            let rows = txn
                .query(
                    &format!("FETCH {PG_BATCH_SIZE} FROM _tl_cursor"),
                    &[],
                )
                .map_err(|e| format!("PostgreSQL FETCH error: {e}"))?;

            if rows.is_empty() {
                break;
            }

            // Build schema from first batch
            if schema.is_none() {
                let columns = rows[0].columns();
                let fields: Vec<Field> = columns
                    .iter()
                    .map(|col| Field::new(col.name(), pg_type_to_arrow(col.type_()), true))
                    .collect();
                schema = Some(Arc::new(Schema::new(fields)));
            }

            let batch = build_record_batch(&rows, schema.as_ref().unwrap())?;
            batches.push(batch);
        }

        txn.execute("CLOSE _tl_cursor", &[])
            .map_err(|e| format!("PostgreSQL CLOSE CURSOR error: {e}"))?;
        txn.commit()
            .map_err(|e| format!("PostgreSQL COMMIT error: {e}"))?;

        if batches.is_empty() {
            return Err(format!("Query returned no rows: {query}"));
        }

        let final_schema = schema.unwrap();
        self.register_batches(register_as, final_schema, batches)?;

        self.rt
            .block_on(self.ctx.table(register_as))
            .map_err(|e| format!("Table reference error: {e}"))
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
