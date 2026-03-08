// ThinkingLanguage — Databricks SQL Connector
// Licensed under MIT OR Apache-2.0
//
// Uses Databricks SQL Statement Execution API (REST).
// Parses JSON result sets into Arrow RecordBatches.

use datafusion::arrow::array::*;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use crate::engine::DataEngine;

const DATABRICKS_BATCH_SIZE: usize = 50_000;

/// Map Databricks SQL type names to Arrow DataTypes.
fn databricks_type_to_arrow(type_name: &str) -> DataType {
    let upper = type_name.to_uppercase();
    if upper.contains("BOOLEAN") {
        DataType::Boolean
    } else if upper.contains("TINYINT") || upper.contains("BYTE") {
        DataType::Int8
    } else if upper.contains("SMALLINT") || upper.contains("SHORT") {
        DataType::Int16
    } else if upper.contains("INT") && !upper.contains("BIGINT") {
        DataType::Int32
    } else if upper.contains("BIGINT") || upper.contains("LONG") {
        DataType::Int64
    } else if upper.contains("FLOAT") {
        DataType::Float32
    } else if upper.contains("DOUBLE") || upper.contains("DECIMAL") {
        DataType::Float64
    } else {
        DataType::Utf8
    }
}

/// Build a RecordBatch from Databricks JSON row data.
fn build_databricks_batch(
    rows: &[Vec<Option<String>>],
    schema: &Arc<Schema>,
    col_types: &[DataType],
) -> Result<RecordBatch, String> {
    let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
    for (col_idx, arrow_type) in col_types.iter().enumerate() {
        let array: Arc<dyn Array> = match arrow_type {
            DataType::Boolean => {
                let values: Vec<Option<bool>> = rows
                    .iter()
                    .map(|r| r[col_idx].as_ref().and_then(|s| s.parse::<bool>().ok()))
                    .collect();
                Arc::new(BooleanArray::from(values))
            }
            DataType::Int8 => {
                let values: Vec<Option<i8>> = rows
                    .iter()
                    .map(|r| r[col_idx].as_ref().and_then(|s| s.parse::<i8>().ok()))
                    .collect();
                Arc::new(Int8Array::from(values))
            }
            DataType::Int16 => {
                let values: Vec<Option<i16>> = rows
                    .iter()
                    .map(|r| r[col_idx].as_ref().and_then(|s| s.parse::<i16>().ok()))
                    .collect();
                Arc::new(Int16Array::from(values))
            }
            DataType::Int32 => {
                let values: Vec<Option<i32>> = rows
                    .iter()
                    .map(|r| r[col_idx].as_ref().and_then(|s| s.parse::<i32>().ok()))
                    .collect();
                Arc::new(Int32Array::from(values))
            }
            DataType::Int64 => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| r[col_idx].as_ref().and_then(|s| s.parse::<i64>().ok()))
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::Float32 => {
                let values: Vec<Option<f32>> = rows
                    .iter()
                    .map(|r| r[col_idx].as_ref().and_then(|s| s.parse::<f32>().ok()))
                    .collect();
                Arc::new(Float32Array::from(values))
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = rows
                    .iter()
                    .map(|r| r[col_idx].as_ref().and_then(|s| s.parse::<f64>().ok()))
                    .collect();
                Arc::new(Float64Array::from(values))
            }
            _ => {
                let values: Vec<Option<&str>> = rows
                    .iter()
                    .map(|r| r[col_idx].as_deref())
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
    /// Read from Databricks using a config string and SQL query.
    /// Config: `{"host":"adb-xxx.azuredatabricks.net","token":"dapi...","warehouse_id":"abc123"}`
    /// Or: `host=adb-xxx.azuredatabricks.net token=dapi... warehouse_id=abc123`
    pub fn read_databricks(
        &self,
        config_str: &str,
        query: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let (host, token, warehouse_id) = parse_databricks_config(config_str)?;

        let url = format!("https://{host}/api/2.0/sql/statements");

        let client = reqwest::blocking::Client::new();
        let body = serde_json::json!({
            "statement": query,
            "warehouse_id": warehouse_id,
            "wait_timeout": "60s",
            "on_wait_timeout": "CANCEL",
            "format": "JSON_ARRAY"
        });

        let resp = client
            .post(&url)
            .header("Content-Type", "application/json")
            .bearer_auth(&token)
            .json(&body)
            .send()
            .map_err(|e| format!("Databricks HTTP error: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().unwrap_or_default();
            return Err(format!("Databricks API error {status}: {text}"));
        }

        let resp_json: serde_json::Value = resp
            .json()
            .map_err(|e| format!("Databricks JSON parse error: {e}"))?;

        // Check statement status
        let status = resp_json["status"]["state"]
            .as_str()
            .unwrap_or("UNKNOWN");
        if status != "SUCCEEDED" {
            return Err(format!("Databricks statement status: {status}"));
        }

        // Extract schema from manifest
        let columns = resp_json["manifest"]["schema"]["columns"]
            .as_array()
            .ok_or("Missing manifest.schema.columns")?;

        let fields: Vec<Field> = columns
            .iter()
            .map(|col| {
                let name = col["name"].as_str().unwrap_or("unknown").to_string();
                let type_name = col["type_name"].as_str().unwrap_or("STRING");
                Field::new(name, databricks_type_to_arrow(type_name), true)
            })
            .collect();
        let col_types: Vec<DataType> = fields.iter().map(|f| f.data_type().clone()).collect();
        let schema = Arc::new(Schema::new(fields));

        // Extract data rows
        let data_rows = resp_json["result"]["data_array"]
            .as_array()
            .ok_or("Missing result.data_array")?;

        let mut chunk: Vec<Vec<Option<String>>> = Vec::with_capacity(DATABRICKS_BATCH_SIZE);
        let mut batches: Vec<RecordBatch> = Vec::new();

        for row in data_rows {
            let row_arr = row.as_array().ok_or("Invalid row format")?;
            let values: Vec<Option<String>> = row_arr
                .iter()
                .map(|v: &serde_json::Value| {
                    if v.is_null() {
                        None
                    } else {
                        Some(v.as_str().unwrap_or(&v.to_string()).to_string())
                    }
                })
                .collect();
            chunk.push(values);

            if chunk.len() >= DATABRICKS_BATCH_SIZE {
                let batch = build_databricks_batch(&chunk, &schema, &col_types)?;
                batches.push(batch);
                chunk.clear();
            }
        }

        if !chunk.is_empty() {
            let batch = build_databricks_batch(&chunk, &schema, &col_types)?;
            batches.push(batch);
        }

        if batches.is_empty() {
            return Err("Databricks query returned no data".to_string());
        }

        let table_name = "__databricks_result";
        let _ = self.ctx.deregister_table(table_name);
        self.register_batches(table_name, schema, batches)?;

        self.rt
            .block_on(self.ctx.table(table_name))
            .map_err(|e| format!("Table reference error: {e}"))
    }
}

/// Parse Databricks config.
fn parse_databricks_config(config_str: &str) -> Result<(String, String, String), String> {
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(config_str) {
        let host = json["host"].as_str().unwrap_or("").to_string();
        let token = json["token"].as_str().unwrap_or("").to_string();
        let warehouse_id = json["warehouse_id"].as_str().unwrap_or("").to_string();
        if host.is_empty() {
            return Err("Databricks config missing 'host'".to_string());
        }
        return Ok((host, token, warehouse_id));
    }

    let mut host = String::new();
    let mut token = String::new();
    let mut warehouse_id = String::new();

    for part in config_str.split_whitespace() {
        if let Some((key, value)) = part.split_once('=') {
            match key.to_lowercase().as_str() {
                "host" => host = value.to_string(),
                "token" => token = value.to_string(),
                "warehouse_id" => warehouse_id = value.to_string(),
                _ => {}
            }
        }
    }

    if host.is_empty() {
        return Err("Databricks config missing 'host'".to_string());
    }

    Ok((host, token, warehouse_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_databricks_config() {
        let (host, token, wh) = parse_databricks_config(
            r#"{"host":"adb-123.azuredatabricks.net","token":"dapi-test","warehouse_id":"abc"}"#,
        )
        .unwrap();
        assert_eq!(host, "adb-123.azuredatabricks.net");
        assert_eq!(token, "dapi-test");
        assert_eq!(wh, "abc");
    }

    #[test]
    #[ignore] // Requires Databricks access
    fn test_read_databricks() {
        let engine = DataEngine::new();
        let df = engine
            .read_databricks(
                "host=adb-123.azuredatabricks.net token=dapi-test warehouse_id=abc",
                "SELECT 1 AS num",
            )
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
    }
}
