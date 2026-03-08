// ThinkingLanguage — DuckDB Connector
// Licensed under MIT OR Apache-2.0
//
// DuckDB uses arrow 54 while DataFusion uses arrow 53. We bridge the version
// gap via Arrow IPC serialization (write with v54, read with v53).

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::ipc::reader::StreamReader as StreamReader53;
use std::sync::Arc;

use crate::engine::DataEngine;

/// Convert a DuckDB arrow 54 RecordBatch to DataFusion's arrow 53 RecordBatch
/// via Arrow IPC serialization.
fn convert_duckdb_batch(
    duckdb_batch: &duckdb::arrow::array::RecordBatch,
) -> Result<RecordBatch, String> {
    // duckdb::arrow::array::RecordBatch IS arrow_array v54.2.1::RecordBatch
    // arrow_ipc_054 uses arrow_array v54 — same types, direct compatibility.
    let mut ipc_buf = Vec::new();
    {
        let mut writer =
            arrow_ipc_054::writer::StreamWriter::try_new(&mut ipc_buf, &duckdb_batch.schema())
                .map_err(|e| format!("IPC write error: {e}"))?;
        writer
            .write(duckdb_batch)
            .map_err(|e| format!("IPC write batch error: {e}"))?;
        writer
            .finish()
            .map_err(|e| format!("IPC finish error: {e}"))?;
    }

    // Deserialize with datafusion's arrow v53
    let cursor = std::io::Cursor::new(ipc_buf);
    let mut reader =
        StreamReader53::try_new(cursor, None).map_err(|e| format!("IPC read error: {e}"))?;

    reader
        .next()
        .ok_or_else(|| "IPC stream contained no batches".to_string())?
        .map_err(|e| format!("IPC read batch error: {e}"))
}

impl DataEngine {
    /// Read from DuckDB using a database file path and SQL query.
    /// DuckDB returns Arrow RecordBatches natively — converted via IPC bridge.
    pub fn read_duckdb(
        &self,
        db_path: &str,
        query: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let conn = if db_path == ":memory:" {
            duckdb::Connection::open_in_memory()
        } else {
            duckdb::Connection::open(db_path)
        }
        .map_err(|e| format!("DuckDB connection error: {e}"))?;

        let mut stmt = conn
            .prepare(query)
            .map_err(|e| format!("DuckDB prepare error: {e}"))?;

        let arrow_result = stmt
            .query_arrow([])
            .map_err(|e| format!("DuckDB query error: {e}"))?;

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut schema = None;

        for duckdb_batch in arrow_result {
            let batch = convert_duckdb_batch(&duckdb_batch)?;
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            batches.push(batch);
        }

        let table_name = "__duckdb_result";
        let _ = self.ctx.deregister_table(table_name);

        if batches.is_empty() {
            let empty_schema = schema.unwrap_or_else(|| {
                Arc::new(datafusion::arrow::datatypes::Schema::empty())
            });
            self.register_batches(table_name, empty_schema, vec![])?;
        } else {
            self.register_batches(table_name, schema.unwrap(), batches)?;
        }

        self.rt
            .block_on(self.ctx.table(table_name))
            .map_err(|e| format!("Table reference error: {e}"))
    }

    /// Write a DataFrame to a DuckDB database table.
    /// Uses row-by-row INSERT for cross-Arrow-version safety.
    pub fn write_duckdb(
        &self,
        df: datafusion::prelude::DataFrame,
        db_path: &str,
        table_name: &str,
    ) -> Result<(), String> {
        let batches = self.collect(df)?;
        if batches.is_empty() {
            return Ok(());
        }

        let mut conn = duckdb::Connection::open(db_path)
            .map_err(|e| format!("DuckDB connection error: {e}"))?;

        let schema = batches[0].schema();

        // Build CREATE TABLE from Arrow schema
        let col_defs: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| {
                let duckdb_type = arrow_to_duckdb_type(f.data_type());
                format!(
                    "\"{}\" {}",
                    f.name().replace('"', "\"\""),
                    duckdb_type
                )
            })
            .collect();

        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS \"{}\" ({})",
            table_name.replace('"', "\"\""),
            col_defs.join(", ")
        );
        conn.execute(&create_sql, [])
            .map_err(|e| format!("DuckDB CREATE TABLE error: {e}"))?;

        // Insert rows via parameterized SQL within a transaction
        let ncols = schema.fields().len();
        let placeholders: Vec<String> = (1..=ncols).map(|i| format!("${i}")).collect();
        let insert_sql = format!(
            "INSERT INTO \"{}\" VALUES ({})",
            table_name.replace('"', "\"\""),
            placeholders.join(", ")
        );

        let txn = conn
            .transaction()
            .map_err(|e| format!("DuckDB transaction error: {e}"))?;

        for batch in &batches {
            for row_idx in 0..batch.num_rows() {
                use datafusion::arrow::array::*;
                use datafusion::arrow::datatypes::DataType;
                let mut params: Vec<Box<dyn duckdb::ToSql>> = Vec::with_capacity(ncols);

                for col_idx in 0..ncols {
                    let col = batch.column(col_idx);
                    if col.is_null(row_idx) {
                        params.push(Box::new(Option::<String>::None));
                        continue;
                    }
                    match schema.field(col_idx).data_type() {
                        DataType::Boolean => {
                            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                            params.push(Box::new(arr.value(row_idx)));
                        }
                        DataType::Int8 => {
                            let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
                            params.push(Box::new(arr.value(row_idx) as i32));
                        }
                        DataType::Int16 => {
                            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
                            params.push(Box::new(arr.value(row_idx) as i32));
                        }
                        DataType::Int32 => {
                            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                            params.push(Box::new(arr.value(row_idx)));
                        }
                        DataType::Int64 => {
                            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                            params.push(Box::new(arr.value(row_idx)));
                        }
                        DataType::UInt8 => {
                            let arr = col.as_any().downcast_ref::<UInt8Array>().unwrap();
                            params.push(Box::new(arr.value(row_idx) as i32));
                        }
                        DataType::UInt16 => {
                            let arr = col.as_any().downcast_ref::<UInt16Array>().unwrap();
                            params.push(Box::new(arr.value(row_idx) as i32));
                        }
                        DataType::UInt32 => {
                            let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                            params.push(Box::new(arr.value(row_idx) as i64));
                        }
                        DataType::UInt64 => {
                            let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                            params.push(Box::new(arr.value(row_idx) as i64));
                        }
                        DataType::Float32 => {
                            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                            params.push(Box::new(arr.value(row_idx) as f64));
                        }
                        DataType::Float64 => {
                            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                            params.push(Box::new(arr.value(row_idx)));
                        }
                        _ => {
                            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                            params.push(Box::new(arr.value(row_idx).to_string()));
                        }
                    }
                }

                let param_refs: Vec<&dyn duckdb::ToSql> =
                    params.iter().map(|p| p.as_ref()).collect();
                txn.execute(&insert_sql, param_refs.as_slice())
                    .map_err(|e| format!("DuckDB insert error: {e}"))?;
            }
        }

        txn.commit()
            .map_err(|e| format!("DuckDB commit error: {e}"))?;

        Ok(())
    }
}

/// Map Arrow DataType to DuckDB SQL type name.
fn arrow_to_duckdb_type(dt: &datafusion::arrow::datatypes::DataType) -> &'static str {
    use datafusion::arrow::datatypes::DataType;
    match dt {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float16 | DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",
        DataType::Binary | DataType::LargeBinary => "BLOB",
        DataType::Date32 | DataType::Date64 => "DATE",
        _ => "VARCHAR",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_duckdb_memory() {
        let engine = DataEngine::new();
        let df = engine
            .read_duckdb(":memory:", "SELECT 42 AS num, 'hello' AS greeting")
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
    }

    #[test]
    fn test_duckdb_file_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");
        let db_str = db_path.to_str().unwrap();

        // Create a table via direct DuckDB
        let conn = duckdb::Connection::open(db_str).unwrap();
        conn.execute_batch(
            "CREATE TABLE users (id INTEGER, name VARCHAR, score DOUBLE);
             INSERT INTO users VALUES (1, 'Alice', 95.5);
             INSERT INTO users VALUES (2, 'Bob', 87.3);
             INSERT INTO users VALUES (3, 'Charlie', 91.0);",
        )
        .unwrap();
        drop(conn);

        let engine = DataEngine::new();
        let df = engine
            .read_duckdb(db_str, "SELECT * FROM users ORDER BY id")
            .unwrap();
        let batches = engine.collect(df).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn test_duckdb_large_query() {
        let engine = DataEngine::new();
        // Generate 100K rows to test batching behavior
        let df = engine
            .read_duckdb(
                ":memory:",
                "SELECT i AS id, 'row_' || i AS name FROM generate_series(1, 100000) t(i)",
            )
            .unwrap();
        let batches = engine.collect(df).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 100_000);
    }

    #[test]
    fn test_write_duckdb() {
        let dir = tempfile::tempdir().unwrap();
        let src_path = dir.path().join("src.duckdb");
        let dst_path = dir.path().join("dst.duckdb");
        let src_str = src_path.to_str().unwrap();
        let dst_str = dst_path.to_str().unwrap();

        // Create source data
        let conn = duckdb::Connection::open(src_str).unwrap();
        conn.execute_batch(
            "CREATE TABLE src (id INTEGER, val DOUBLE);
             INSERT INTO src VALUES (1, 1.1), (2, 2.2), (3, 3.3);",
        )
        .unwrap();
        drop(conn);

        let engine = DataEngine::new();
        let df = engine
            .read_duckdb(src_str, "SELECT * FROM src")
            .unwrap();
        engine.write_duckdb(df, dst_str, "dst_table").unwrap();

        // Read back
        let df2 = engine
            .read_duckdb(dst_str, "SELECT * FROM dst_table")
            .unwrap();
        let batches = engine.collect(df2).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
    }
}
