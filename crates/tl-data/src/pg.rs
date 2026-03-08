use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use postgres::{Client, NoTls};
use std::sync::Arc;

use crate::engine::DataEngine;

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
    pub fn query_postgres(
        &self,
        conn_str: &str,
        query: &str,
        register_as: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let mut client = Client::connect(conn_str, NoTls)
            .map_err(|e| format!("PostgreSQL connection error: {e}"))?;

        let rows = client
            .query(query, &[])
            .map_err(|e| format!("PostgreSQL query error: {e}"))?;

        if rows.is_empty() {
            return Err(format!("Query returned no rows: {query}"));
        }

        let columns = rows[0].columns();
        let fields: Vec<Field> = columns
            .iter()
            .map(|col| Field::new(col.name(), pg_type_to_arrow(col.type_()), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
        for (col_idx, col) in columns.iter().enumerate() {
            let arrow_type = pg_type_to_arrow(col.type_());
            let array: Arc<dyn Array> = match arrow_type {
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

        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| format!("Arrow RecordBatch creation error: {e}"))?;

        self.register_batch(register_as, batch)?;

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
}
