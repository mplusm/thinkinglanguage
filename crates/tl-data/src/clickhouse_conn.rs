// ThinkingLanguage — ClickHouse Connector
// Licensed under MIT OR Apache-2.0
//
// Uses ClickHouse HTTP interface with JSONEachRow format.
// No additional crate dependency — uses reqwest (already in workspace).

use datafusion::arrow::array::*;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use crate::engine::DataEngine;

const CLICKHOUSE_BATCH_SIZE: usize = 50_000;

/// Infer Arrow DataType from a serde_json::Value.
fn json_value_to_arrow_type(value: &serde_json::Value) -> DataType {
    match value {
        serde_json::Value::Bool(_) => DataType::Boolean,
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                DataType::Int64
            } else {
                DataType::Float64
            }
        }
        serde_json::Value::String(_) => DataType::Utf8,
        serde_json::Value::Null => DataType::Utf8, // nullable string fallback
        _ => DataType::Utf8,
    }
}

/// Build a RecordBatch from a chunk of parsed JSON rows.
fn build_clickhouse_batch(
    rows: &[serde_json::Map<String, serde_json::Value>],
    schema: &Arc<Schema>,
    col_names: &[String],
    col_types: &[DataType],
) -> Result<RecordBatch, String> {
    let mut arrays: Vec<Arc<dyn Array>> = Vec::new();

    for (col_idx, arrow_type) in col_types.iter().enumerate() {
        let col_name = &col_names[col_idx];
        let array: Arc<dyn Array> = match arrow_type {
            DataType::Boolean => {
                let values: Vec<Option<bool>> = rows
                    .iter()
                    .map(|row| match row.get(col_name) {
                        Some(serde_json::Value::Bool(b)) => Some(*b),
                        Some(serde_json::Value::Null) | None => None,
                        _ => None,
                    })
                    .collect();
                Arc::new(BooleanArray::from(values))
            }
            DataType::Int64 => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|row| match row.get(col_name) {
                        Some(serde_json::Value::Number(n)) => n.as_i64(),
                        Some(serde_json::Value::String(s)) => s.parse::<i64>().ok(),
                        Some(serde_json::Value::Null) | None => None,
                        _ => None,
                    })
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = rows
                    .iter()
                    .map(|row| match row.get(col_name) {
                        Some(serde_json::Value::Number(n)) => n.as_f64(),
                        Some(serde_json::Value::String(s)) => s.parse::<f64>().ok(),
                        Some(serde_json::Value::Null) | None => None,
                        _ => None,
                    })
                    .collect();
                Arc::new(Float64Array::from(values))
            }
            _ => {
                // Utf8 fallback — stringify everything
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| match row.get(col_name) {
                        Some(serde_json::Value::Null) | None => None,
                        Some(serde_json::Value::String(s)) => Some(s.clone()),
                        Some(other) => Some(other.to_string()),
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
    /// Read from ClickHouse using its HTTP interface and a SQL query.
    /// Uses JSONEachRow format for streaming-friendly line-delimited JSON.
    /// Batches rows into 50K-row RecordBatches and registers them with
    /// DataFusion for partition parallelism on large result sets.
    ///
    /// # Arguments
    /// * `url` — ClickHouse HTTP endpoint, e.g. `"http://localhost:8123"`
    /// * `query` — SQL query to execute
    pub fn read_clickhouse(
        &self,
        url: &str,
        query: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let client = reqwest::blocking::Client::new();

        let endpoint = format!("{}/?default_format=JSONEachRow", url.trim_end_matches('/'));

        let response = client
            .post(&endpoint)
            .body(query.to_string())
            .send()
            .map_err(|e| format!("ClickHouse HTTP request error: {e}"))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .unwrap_or_else(|_| "<unreadable body>".to_string());
            return Err(format!(
                "ClickHouse query error (HTTP {status}): {body}"
            ));
        }

        let body = response
            .text()
            .map_err(|e| format!("ClickHouse response read error: {e}"))?;

        // Parse JSONEachRow: one JSON object per line
        let mut rows: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
        for line in body.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let obj: serde_json::Value = serde_json::from_str(trimmed)
                .map_err(|e| format!("ClickHouse JSON parse error: {e}"))?;
            match obj {
                serde_json::Value::Object(map) => rows.push(map),
                _ => {
                    return Err(format!(
                        "ClickHouse returned non-object JSON line: {trimmed}"
                    ))
                }
            }
        }

        if rows.is_empty() {
            return Err("ClickHouse query returned no rows".to_string());
        }

        // Infer schema from the first row
        let first_row = &rows[0];
        let col_names: Vec<String> = first_row.keys().cloned().collect();
        let col_types: Vec<DataType> = col_names
            .iter()
            .map(|name| json_value_to_arrow_type(&first_row[name]))
            .collect();
        let fields: Vec<Field> = col_names
            .iter()
            .zip(col_types.iter())
            .map(|(name, dtype)| Field::new(name, dtype.clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Build batches in chunks for large result sets
        let mut batches: Vec<RecordBatch> = Vec::new();
        for chunk in rows.chunks(CLICKHOUSE_BATCH_SIZE) {
            let batch = build_clickhouse_batch(chunk, &schema, &col_names, &col_types)?;
            batches.push(batch);
        }

        let table_name = "__clickhouse_result";
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
    #[ignore] // Requires a running ClickHouse instance
    fn test_read_clickhouse() {
        let engine = DataEngine::new();
        let df = engine
            .read_clickhouse(
                "http://localhost:8123",
                "SELECT * FROM system.numbers LIMIT 10",
            )
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
    }

    #[test]
    #[ignore] // Requires a running ClickHouse instance
    fn test_clickhouse_type_mapping() {
        let engine = DataEngine::new();
        let df = engine
            .read_clickhouse(
                "http://localhost:8123",
                "SELECT 1 AS num, 'hello' AS text, 3.14 AS float_val, true AS bool_val",
            )
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
    }

    #[test]
    fn test_json_value_to_arrow_type() {
        assert_eq!(
            json_value_to_arrow_type(&serde_json::Value::Bool(true)),
            DataType::Boolean
        );
        assert_eq!(
            json_value_to_arrow_type(&serde_json::json!(42)),
            DataType::Int64
        );
        assert_eq!(
            json_value_to_arrow_type(&serde_json::json!(3.14)),
            DataType::Float64
        );
        assert_eq!(
            json_value_to_arrow_type(&serde_json::json!("hello")),
            DataType::Utf8
        );
        assert_eq!(
            json_value_to_arrow_type(&serde_json::Value::Null),
            DataType::Utf8
        );
    }

    #[test]
    fn test_build_clickhouse_batch() {
        let row1: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
            r#"{"id": 1, "name": "Alice", "score": 95.5, "active": true}"#,
        )
        .unwrap();
        let row2: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
            r#"{"id": 2, "name": "Bob", "score": 87.3, "active": false}"#,
        )
        .unwrap();
        let rows = vec![row1, row2];

        let col_names = vec![
            "id".to_string(),
            "name".to_string(),
            "score".to_string(),
            "active".to_string(),
        ];
        let col_types = vec![DataType::Int64, DataType::Utf8, DataType::Float64, DataType::Boolean];
        let fields: Vec<Field> = col_names
            .iter()
            .zip(col_types.iter())
            .map(|(name, dtype)| Field::new(name, dtype.clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let batch = build_clickhouse_batch(&rows, &schema, &col_names, &col_types).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
    }
}
