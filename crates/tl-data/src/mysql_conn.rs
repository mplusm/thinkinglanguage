// ThinkingLanguage — MySQL Connector
// Licensed under MIT OR Apache-2.0
//
// Read MySQL tables into DataFusion DataFrames.
// Uses batched Arrow conversion for large result sets.

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use mysql::prelude::*;
use mysql::{Pool, Value as MysqlValue};
use std::sync::Arc;

use crate::engine::DataEngine;

/// Default batch size for MySQL reads (rows per Arrow batch).
const MYSQL_BATCH_SIZE: usize = 50_000;

/// Map MySQL column types to Arrow DataTypes.
fn mysql_type_to_arrow(col_type: mysql::consts::ColumnType) -> DataType {
    use mysql::consts::ColumnType::*;
    match col_type {
        MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT => DataType::Int16,
        MYSQL_TYPE_LONG | MYSQL_TYPE_INT24 => DataType::Int32,
        MYSQL_TYPE_LONGLONG => DataType::Int64,
        MYSQL_TYPE_FLOAT => DataType::Float32,
        MYSQL_TYPE_DOUBLE | MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => DataType::Float64,
        MYSQL_TYPE_BIT => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

/// Build a RecordBatch from a chunk of MySQL rows.
fn build_mysql_batch(
    rows: &[mysql::Row],
    schema: &Arc<Schema>,
    col_types: &[DataType],
) -> Result<RecordBatch, String> {
    let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
    for (col_idx, arrow_type) in col_types.iter().enumerate() {
        let array: Arc<dyn Array> = match arrow_type {
            DataType::Boolean => {
                let values: Vec<Option<bool>> = rows
                    .iter()
                    .map(|r| match &r[col_idx] {
                        MysqlValue::Int(n) => Some(*n != 0),
                        MysqlValue::UInt(n) => Some(*n != 0),
                        MysqlValue::NULL => None,
                        _ => None,
                    })
                    .collect();
                Arc::new(BooleanArray::from(values))
            }
            DataType::Int16 => {
                let values: Vec<Option<i16>> = rows
                    .iter()
                    .map(|r| match &r[col_idx] {
                        MysqlValue::Int(n) => Some(*n as i16),
                        MysqlValue::UInt(n) => Some(*n as i16),
                        MysqlValue::NULL => None,
                        _ => None,
                    })
                    .collect();
                Arc::new(Int16Array::from(values))
            }
            DataType::Int32 => {
                let values: Vec<Option<i32>> = rows
                    .iter()
                    .map(|r| match &r[col_idx] {
                        MysqlValue::Int(n) => Some(*n as i32),
                        MysqlValue::UInt(n) => Some(*n as i32),
                        MysqlValue::NULL => None,
                        _ => None,
                    })
                    .collect();
                Arc::new(Int32Array::from(values))
            }
            DataType::Int64 => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| match &r[col_idx] {
                        MysqlValue::Int(n) => Some(*n),
                        MysqlValue::UInt(n) => Some(*n as i64),
                        MysqlValue::NULL => None,
                        _ => None,
                    })
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::Float32 => {
                let values: Vec<Option<f32>> = rows
                    .iter()
                    .map(|r| match &r[col_idx] {
                        MysqlValue::Float(f) => Some(*f as f32),
                        MysqlValue::Double(f) => Some(*f as f32),
                        MysqlValue::Int(n) => Some(*n as f32),
                        MysqlValue::NULL => None,
                        _ => None,
                    })
                    .collect();
                Arc::new(Float32Array::from(values))
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = rows
                    .iter()
                    .map(|r| match &r[col_idx] {
                        MysqlValue::Float(f) => Some(*f as f64),
                        MysqlValue::Double(f) => Some(*f),
                        MysqlValue::Int(n) => Some(*n as f64),
                        MysqlValue::NULL => None,
                        _ => None,
                    })
                    .collect();
                Arc::new(Float64Array::from(values))
            }
            _ => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| match &r[col_idx] {
                        MysqlValue::NULL => None,
                        MysqlValue::Bytes(b) => Some(String::from_utf8_lossy(b).to_string()),
                        other => Some(format!("{other:?}")),
                    })
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
    /// Read from MySQL using a connection string and SQL query.
    /// Uses batched Arrow conversion (50K rows per batch) to reduce peak memory
    /// and enable DataFusion partition parallelism on large result sets.
    pub fn read_mysql(
        &self,
        conn_str: &str,
        query: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let pool = Pool::new(conn_str).map_err(|e| format!("MySQL connection error: {e}"))?;
        let mut conn = pool
            .get_conn()
            .map_err(|e| format!("MySQL connection error: {e}"))?;

        let result: Vec<mysql::Row> = conn
            .query(query)
            .map_err(|e| format!("MySQL query error: {e}"))?;

        if result.is_empty() {
            return Err("MySQL query returned no rows".to_string());
        }

        // Build schema from first row
        let columns = result[0].columns_ref();
        let col_types: Vec<DataType> = columns
            .iter()
            .map(|c| mysql_type_to_arrow(c.column_type()))
            .collect();
        let fields: Vec<Field> = columns
            .iter()
            .map(|c| {
                Field::new(
                    c.name_str().to_string(),
                    mysql_type_to_arrow(c.column_type()),
                    true,
                )
            })
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Build batches in chunks for large result sets
        let mut batches: Vec<RecordBatch> = Vec::new();
        for chunk in result.chunks(MYSQL_BATCH_SIZE) {
            let batch = build_mysql_batch(chunk, &schema, &col_types)?;
            batches.push(batch);
        }

        let table_name = "__mysql_result";
        self.register_batches(table_name, schema, batches)?;

        self.rt
            .block_on(self.ctx.table(table_name))
            .map_err(|e| format!("Table reference error: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore] // Requires a running MySQL instance
    fn test_read_mysql() {
        let engine = DataEngine::new();
        let df = engine
            .read_mysql(
                "mysql://root:password@localhost:3306/testdb",
                "SELECT * FROM users",
            )
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
    }

    #[test]
    #[ignore] // Requires a running MySQL instance
    fn test_mysql_type_mapping() {
        let engine = DataEngine::new();
        let df = engine
            .read_mysql(
                "mysql://root:password@localhost:3306/testdb",
                "SELECT 1 as num, 'hello' as text, 3.14 as float_val",
            )
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
    }
}
