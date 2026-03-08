// ThinkingLanguage — MSSQL / SQL Server Connector
// Licensed under MIT OR Apache-2.0
//
// Uses tiberius (async) with batched Arrow conversion.
// Streams rows via query result stream, batching 50K rows per RecordBatch.

use datafusion::arrow::array::*;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use tiberius::{Client, Config, AuthMethod, ColumnType};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::engine::DataEngine;

/// Default batch size for MSSQL reads (rows per Arrow batch).
const MSSQL_BATCH_SIZE: usize = 50_000;

/// Map tiberius ColumnType to Arrow DataType.
fn mssql_type_to_arrow(col_type: &ColumnType) -> DataType {
    match col_type {
        ColumnType::Bit => DataType::Boolean,
        ColumnType::Int1 => DataType::Int8,
        ColumnType::Int2 => DataType::Int16,
        ColumnType::Int4 => DataType::Int32,
        ColumnType::Int8 => DataType::Int64,
        ColumnType::Float4 => DataType::Float32,
        ColumnType::Float8 | ColumnType::Numericn | ColumnType::Decimaln => DataType::Float64,
        _ => DataType::Utf8,
    }
}

/// Extracted row data for batch building.
struct MssqlRowData {
    values: Vec<Option<MssqlValue>>,
}

enum MssqlValue {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Str(String),
}

/// Build a RecordBatch from accumulated row data.
fn build_mssql_batch(
    rows: &[MssqlRowData],
    schema: &Arc<Schema>,
    col_types: &[DataType],
) -> Result<RecordBatch, String> {
    let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
    for (col_idx, arrow_type) in col_types.iter().enumerate() {
        let array: Arc<dyn Array> = match arrow_type {
            DataType::Boolean => {
                let values: Vec<Option<bool>> = rows
                    .iter()
                    .map(|r| match &r.values[col_idx] {
                        Some(MssqlValue::Bool(b)) => Some(*b),
                        _ => None,
                    })
                    .collect();
                Arc::new(BooleanArray::from(values))
            }
            DataType::Int8 => {
                let values: Vec<Option<i8>> = rows
                    .iter()
                    .map(|r| match &r.values[col_idx] {
                        Some(MssqlValue::I8(n)) => Some(*n),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int8Array::from(values))
            }
            DataType::Int16 => {
                let values: Vec<Option<i16>> = rows
                    .iter()
                    .map(|r| match &r.values[col_idx] {
                        Some(MssqlValue::I16(n)) => Some(*n),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int16Array::from(values))
            }
            DataType::Int32 => {
                let values: Vec<Option<i32>> = rows
                    .iter()
                    .map(|r| match &r.values[col_idx] {
                        Some(MssqlValue::I32(n)) => Some(*n),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int32Array::from(values))
            }
            DataType::Int64 => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| match &r.values[col_idx] {
                        Some(MssqlValue::I64(n)) => Some(*n),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::Float32 => {
                let values: Vec<Option<f32>> = rows
                    .iter()
                    .map(|r| match &r.values[col_idx] {
                        Some(MssqlValue::F32(n)) => Some(*n),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float32Array::from(values))
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = rows
                    .iter()
                    .map(|r| match &r.values[col_idx] {
                        Some(MssqlValue::F64(n)) => Some(*n),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float64Array::from(values))
            }
            _ => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| match &r.values[col_idx] {
                        Some(MssqlValue::Str(s)) => Some(s.clone()),
                        Some(MssqlValue::Bool(b)) => Some(b.to_string()),
                        Some(MssqlValue::I8(n)) => Some(n.to_string()),
                        Some(MssqlValue::I16(n)) => Some(n.to_string()),
                        Some(MssqlValue::I32(n)) => Some(n.to_string()),
                        Some(MssqlValue::I64(n)) => Some(n.to_string()),
                        Some(MssqlValue::F32(n)) => Some(n.to_string()),
                        Some(MssqlValue::F64(n)) => Some(n.to_string()),
                        None => None,
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
    /// Read from MSSQL using an ADO-style connection string and SQL query.
    /// Uses tiberius async client with row streaming and batched Arrow conversion.
    pub fn read_mssql(
        &self,
        conn_str: &str,
        query: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        self.rt.block_on(async {
            let config = parse_mssql_config(conn_str)?;
            let tcp = TcpStream::connect(config.get_addr())
                .await
                .map_err(|e| format!("MSSQL TCP connection error: {e}"))?;
            tcp.set_nodelay(true)
                .map_err(|e| format!("TCP nodelay error: {e}"))?;

            let mut client = Client::connect(config, tcp.compat_write())
                .await
                .map_err(|e| format!("MSSQL connection error: {e}"))?;

            let result = client
                .query(query, &[])
                .await
                .map_err(|e| format!("MSSQL query error: {e}"))?;

            let mut schema: Option<Arc<Schema>> = None;
            let mut col_types: Vec<DataType> = Vec::new();
            let mut chunk: Vec<MssqlRowData> = Vec::with_capacity(MSSQL_BATCH_SIZE);
            let mut batches: Vec<RecordBatch> = Vec::new();

            // Use into_first_result for simplicity (loads all rows into memory)
            // For truly large result sets, would use into_row_stream()
            let rows = result
                .into_first_result()
                .await
                .map_err(|e| format!("MSSQL result error: {e}"))?;

            for row in &rows {
                // Build schema from first row
                if schema.is_none() {
                    let columns = row.columns();
                    let fields: Vec<Field> = columns
                        .iter()
                        .map(|col| {
                            let arrow_type = mssql_type_to_arrow(&col.column_type());
                            Field::new(col.name(), arrow_type, true)
                        })
                        .collect();
                    col_types = columns
                        .iter()
                        .map(|col| mssql_type_to_arrow(&col.column_type()))
                        .collect();
                    schema = Some(Arc::new(Schema::new(fields)));
                }

                // Extract row values
                let ncols = col_types.len();
                let mut values = Vec::with_capacity(ncols);
                for (i, ct) in col_types.iter().enumerate() {
                    let val = extract_mssql_value(row, i, ct);
                    values.push(val);
                }
                chunk.push(MssqlRowData { values });

                if chunk.len() >= MSSQL_BATCH_SIZE {
                    let batch = build_mssql_batch(&chunk, schema.as_ref().unwrap(), &col_types)?;
                    batches.push(batch);
                    chunk.clear();
                }
            }

            if !chunk.is_empty() {
                let batch = build_mssql_batch(&chunk, schema.as_ref().unwrap(), &col_types)?;
                batches.push(batch);
            }

            if batches.is_empty() {
                return Err("MSSQL query returned no rows".to_string());
            }

            let final_schema = schema.unwrap();
            let table_name = "__mssql_result";
            let _ = self.ctx.deregister_table(table_name);
            self.register_batches(table_name, final_schema, batches)?;

            self.ctx
                .table(table_name)
                .await
                .map_err(|e| format!("Table reference error: {e}"))
        })
    }
}

/// Extract a value from a tiberius Row by column index and expected type.
fn extract_mssql_value(
    row: &tiberius::Row,
    idx: usize,
    arrow_type: &DataType,
) -> Option<MssqlValue> {
    match arrow_type {
        DataType::Boolean => row.try_get::<bool, _>(idx).ok().flatten().map(MssqlValue::Bool),
        DataType::Int8 => row.try_get::<i16, _>(idx).ok().flatten().map(|v| MssqlValue::I8(v as i8)),
        DataType::Int16 => row.try_get::<i16, _>(idx).ok().flatten().map(MssqlValue::I16),
        DataType::Int32 => row.try_get::<i32, _>(idx).ok().flatten().map(MssqlValue::I32),
        DataType::Int64 => row.try_get::<i64, _>(idx).ok().flatten().map(MssqlValue::I64),
        DataType::Float32 => row.try_get::<f32, _>(idx).ok().flatten().map(MssqlValue::F32),
        DataType::Float64 => row.try_get::<f64, _>(idx).ok().flatten().map(MssqlValue::F64),
        _ => row
            .try_get::<&str, _>(idx)
            .ok()
            .flatten()
            .map(|s| MssqlValue::Str(s.to_string())),
    }
}

/// Parse an ADO-style or key=value MSSQL connection string into tiberius Config.
fn parse_mssql_config(conn_str: &str) -> Result<Config, String> {
    // Try ADO-style first
    if conn_str.contains("Server=") || conn_str.contains("server=") {
        let mut config = Config::from_ado_string(conn_str).map_err(|e| format!("MSSQL config parse error: {e}"))?;
        config.encryption(tiberius::EncryptionLevel::Off);
        Ok(config)
    } else {
        // Simple format: host=X port=Y user=U password=P database=D
        let mut config = Config::new();
        for part in conn_str.split_whitespace() {
            if let Some((key, value)) = part.split_once('=') {
                match key.to_lowercase().as_str() {
                    "host" | "server" => config.host(value),
                    "port" => {
                        config.port(
                            value
                                .parse::<u16>()
                                .map_err(|_| "Invalid port".to_string())?,
                        );
                    }
                    "user" | "username" => {
                        // Store for auth below
                        config.authentication(AuthMethod::sql_server(value, ""));
                    }
                    "database" | "dbname" => config.database(value),
                    _ => {}
                }
            }
        }
        // Re-parse for user+password pair
        let user = conn_str
            .split_whitespace()
            .find_map(|p| p.strip_prefix("user=").or_else(|| p.strip_prefix("username=")))
            .unwrap_or("");
        let pass = conn_str
            .split_whitespace()
            .find_map(|p| p.strip_prefix("password="))
            .unwrap_or("");
        config.authentication(AuthMethod::sql_server(user, pass));
        config.encryption(tiberius::EncryptionLevel::Off);

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore] // Requires a running MSSQL instance
    fn test_read_mssql() {
        let engine = DataEngine::new();
        let df = engine
            .read_mssql(
                "Server=tcp:localhost,1433;User Id=sa;Password=TestPass123!;Database=master",
                "SELECT 1 AS num, 'hello' AS greeting",
            )
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
    }
}
