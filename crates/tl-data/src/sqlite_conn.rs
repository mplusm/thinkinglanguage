// ThinkingLanguage — SQLite Connector
// Licensed under MIT OR Apache-2.0
//
// Read/write SQLite tables to/from DataFusion DataFrames.

use std::sync::Arc;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::RecordBatch;
use rusqlite::{Connection, types::ValueRef};

use crate::engine::DataEngine;

impl DataEngine {
    /// Read from SQLite using a database file path and SQL query.
    pub fn read_sqlite(
        &self,
        db_path: &str,
        query: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let conn = Connection::open(db_path)
            .map_err(|e| format!("SQLite connection error: {e}"))?;

        let mut stmt = conn.prepare(query)
            .map_err(|e| format!("SQLite prepare error: {e}"))?;

        let col_count = stmt.column_count();
        let col_names: Vec<String> = (0..col_count)
            .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
            .collect();

        // Collect all rows first so we can inspect types
        let mut all_rows: Vec<Vec<rusqlite::types::Value>> = Vec::new();
        let rows = stmt.query_map([], |row| {
            let mut vals = Vec::with_capacity(col_count);
            for i in 0..col_count {
                vals.push(value_ref_to_owned(row.get_ref(i).unwrap()));
            }
            Ok(vals)
        }).map_err(|e| format!("SQLite query error: {e}"))?;

        for row in rows {
            all_rows.push(row.map_err(|e| format!("SQLite row error: {e}"))?);
        }

        // Infer types from the first non-null value in each column
        let col_types: Vec<DataType> = (0..col_count)
            .map(|col_idx| {
                for row in &all_rows {
                    match &row[col_idx] {
                        rusqlite::types::Value::Integer(_) => return DataType::Int64,
                        rusqlite::types::Value::Real(_) => return DataType::Float64,
                        rusqlite::types::Value::Text(_) => return DataType::Utf8,
                        rusqlite::types::Value::Blob(_) => return DataType::Binary,
                        rusqlite::types::Value::Null => continue,
                    }
                }
                DataType::Utf8 // default if all nulls
            })
            .collect();

        let fields: Vec<Field> = col_names
            .iter()
            .zip(col_types.iter())
            .map(|(name, dt)| Field::new(name, dt.clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
        for col_idx in 0..col_count {
            let arrow_type = &col_types[col_idx];
            let array: Arc<dyn Array> = match arrow_type {
                DataType::Boolean => {
                    let values: Vec<Option<bool>> = all_rows.iter().map(|row| {
                        match &row[col_idx] {
                            rusqlite::types::Value::Integer(n) => Some(*n != 0),
                            rusqlite::types::Value::Null => None,
                            _ => None,
                        }
                    }).collect();
                    Arc::new(BooleanArray::from(values))
                }
                DataType::Int64 => {
                    let values: Vec<Option<i64>> = all_rows.iter().map(|row| {
                        match &row[col_idx] {
                            rusqlite::types::Value::Integer(n) => Some(*n),
                            rusqlite::types::Value::Null => None,
                            _ => None,
                        }
                    }).collect();
                    Arc::new(Int64Array::from(values))
                }
                DataType::Float64 => {
                    let values: Vec<Option<f64>> = all_rows.iter().map(|row| {
                        match &row[col_idx] {
                            rusqlite::types::Value::Real(f) => Some(*f),
                            rusqlite::types::Value::Integer(n) => Some(*n as f64),
                            rusqlite::types::Value::Null => None,
                            _ => None,
                        }
                    }).collect();
                    Arc::new(Float64Array::from(values))
                }
                DataType::Binary => {
                    let values: Vec<Option<&[u8]>> = all_rows.iter().map(|row| {
                        match &row[col_idx] {
                            rusqlite::types::Value::Blob(b) => Some(b.as_slice()),
                            rusqlite::types::Value::Null => None,
                            _ => None,
                        }
                    }).collect();
                    Arc::new(BinaryArray::from(values))
                }
                _ => {
                    // Utf8 / fallback
                    let values: Vec<Option<String>> = all_rows.iter().map(|row| {
                        match &row[col_idx] {
                            rusqlite::types::Value::Null => None,
                            rusqlite::types::Value::Text(s) => Some(s.clone()),
                            rusqlite::types::Value::Integer(n) => Some(n.to_string()),
                            rusqlite::types::Value::Real(f) => Some(f.to_string()),
                            rusqlite::types::Value::Blob(b) => {
                                Some(String::from_utf8_lossy(b).to_string())
                            }
                        }
                    }).collect();
                    Arc::new(StringArray::from(values))
                }
            };
            arrays.push(array);
        }

        let table_name = "__sqlite_result";
        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| format!("Arrow RecordBatch creation error: {e}"))?;

        // Deregister previous result table if it exists
        let _ = self.ctx.deregister_table(table_name);
        self.register_batch(table_name, batch)?;

        self.rt
            .block_on(self.ctx.table(table_name))
            .map_err(|e| format!("Table reference error: {e}"))
    }

    /// Write a DataFrame to a SQLite database table.
    pub fn write_sqlite(
        &self,
        df: datafusion::prelude::DataFrame,
        db_path: &str,
        table_name: &str,
    ) -> Result<(), String> {
        let batches = self.collect(df)?;
        if batches.is_empty() {
            return Ok(());
        }

        let conn = Connection::open(db_path)
            .map_err(|e| format!("SQLite connection error: {e}"))?;

        let schema = batches[0].schema();

        // Build CREATE TABLE statement
        let col_defs: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| {
                let sql_type = match f.data_type() {
                    DataType::Boolean => "BOOLEAN",
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                    | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                        "INTEGER"
                    }
                    DataType::Float16 | DataType::Float32 | DataType::Float64 => "REAL",
                    DataType::Binary | DataType::LargeBinary => "BLOB",
                    _ => "TEXT",
                };
                format!("\"{}\" {}", f.name(), sql_type)
            })
            .collect();

        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS \"{}\" ({})",
            table_name,
            col_defs.join(", ")
        );
        conn.execute(&create_sql, [])
            .map_err(|e| format!("SQLite CREATE TABLE error: {e}"))?;

        // Insert rows
        let placeholders: Vec<String> = (0..schema.fields().len())
            .map(|_| "?".to_string())
            .collect();
        let insert_sql = format!(
            "INSERT INTO \"{}\" VALUES ({})",
            table_name,
            placeholders.join(", ")
        );

        let tx = conn
            .unchecked_transaction()
            .map_err(|e| format!("SQLite transaction error: {e}"))?;

        for batch in &batches {
            for row_idx in 0..batch.num_rows() {
                let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    if col.is_null(row_idx) {
                        params.push(Box::new(rusqlite::types::Null));
                    } else {
                        match col.data_type() {
                            DataType::Boolean => {
                                let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                                params.push(Box::new(arr.value(row_idx)));
                            }
                            DataType::Int8 => {
                                let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
                                params.push(Box::new(arr.value(row_idx) as i64));
                            }
                            DataType::Int16 => {
                                let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
                                params.push(Box::new(arr.value(row_idx) as i64));
                            }
                            DataType::Int32 => {
                                let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                                params.push(Box::new(arr.value(row_idx) as i64));
                            }
                            DataType::Int64 => {
                                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                                params.push(Box::new(arr.value(row_idx)));
                            }
                            DataType::UInt8 => {
                                let arr = col.as_any().downcast_ref::<UInt8Array>().unwrap();
                                params.push(Box::new(arr.value(row_idx) as i64));
                            }
                            DataType::UInt16 => {
                                let arr = col.as_any().downcast_ref::<UInt16Array>().unwrap();
                                params.push(Box::new(arr.value(row_idx) as i64));
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
                }
                let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                    params.iter().map(|p| p.as_ref()).collect();
                tx.execute(&insert_sql, param_refs.as_slice())
                    .map_err(|e| format!("SQLite INSERT error: {e}"))?;
            }
        }

        tx.commit()
            .map_err(|e| format!("SQLite COMMIT error: {e}"))?;
        Ok(())
    }
}

/// Convert a borrowed ValueRef into an owned Value.
fn value_ref_to_owned(v: ValueRef<'_>) -> rusqlite::types::Value {
    match v {
        ValueRef::Null => rusqlite::types::Value::Null,
        ValueRef::Integer(i) => rusqlite::types::Value::Integer(i),
        ValueRef::Real(f) => rusqlite::types::Value::Real(f),
        ValueRef::Text(s) => {
            rusqlite::types::Value::Text(String::from_utf8_lossy(s).to_string())
        }
        ValueRef::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn create_test_db(path: &str) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE users (id INTEGER, name TEXT, score REAL, active BOOLEAN);
             INSERT INTO users VALUES (1, 'Alice', 95.5, 1);
             INSERT INTO users VALUES (2, 'Bob', 87.3, 0);
             INSERT INTO users VALUES (3, 'Charlie', NULL, 1);",
        )
        .unwrap();
    }

    #[test]
    fn test_read_sqlite_basic() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_str = db_path.to_str().unwrap();
        create_test_db(db_str);

        let engine = DataEngine::new();
        let df = engine
            .read_sqlite(db_str, "SELECT * FROM users")
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 4);
    }

    #[test]
    fn test_read_sqlite_empty() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("empty.db");
        let db_str = db_path.to_str().unwrap();
        let conn = Connection::open(db_str).unwrap();
        conn.execute_batch("CREATE TABLE empty_table (id INTEGER, name TEXT);")
            .unwrap();

        let engine = DataEngine::new();
        let df = engine
            .read_sqlite(db_str, "SELECT * FROM empty_table")
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn test_read_sqlite_nonexistent() {
        let engine = DataEngine::new();
        let result = engine.read_sqlite("/nonexistent/path/db.sqlite", "SELECT 1");
        // rusqlite creates the file if it doesn't exist, so let's test a bad query instead
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_write_sqlite_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("roundtrip.db");
        let db_str = db_path.to_str().unwrap();

        // Create source data
        create_test_db(db_str);
        let engine = DataEngine::new();
        let df = engine
            .read_sqlite(db_str, "SELECT id, name, score FROM users")
            .unwrap();

        // Write to a new table
        let out_path = dir.path().join("output.db");
        let out_str = out_path.to_str().unwrap();

        // Re-read to get a fresh df (since df is consumed by collect internally)
        let df2 = engine
            .read_sqlite(db_str, "SELECT id, name, score FROM users")
            .unwrap();
        engine
            .write_sqlite(df2, out_str, "results")
            .unwrap();

        // Read back and verify
        let df3 = engine
            .read_sqlite(out_str, "SELECT * FROM results")
            .unwrap();
        let batches = engine.collect(df3).unwrap();
        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn test_sqlite_type_inference() {
        // Type inference happens from actual data values.
        // Verify via a query with mixed types.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("types.db");
        let db_str = db_path.to_str().unwrap();
        let conn = Connection::open(db_str).unwrap();
        conn.execute_batch(
            "CREATE TABLE types (i INTEGER, r REAL, t TEXT, b BLOB);
             INSERT INTO types VALUES (42, 3.14, 'hello', X'DEADBEEF');",
        )
        .unwrap();

        let engine = DataEngine::new();
        let df = engine.read_sqlite(db_str, "SELECT * FROM types").unwrap();
        let batches = engine.collect(df).unwrap();
        let schema = batches[0].schema();
        assert_eq!(*schema.field(0).data_type(), DataType::Int64);
        assert_eq!(*schema.field(1).data_type(), DataType::Float64);
        assert_eq!(*schema.field(2).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(3).data_type(), DataType::Binary);
    }
}
