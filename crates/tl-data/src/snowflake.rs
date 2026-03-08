// ThinkingLanguage — Snowflake Connector
// Licensed under MIT OR Apache-2.0
//
// Uses Snowflake SQL REST API (v2/statements) with JWT or basic auth.
// Parses JSON result sets into Arrow RecordBatches.

use datafusion::arrow::array::*;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use crate::engine::DataEngine;

const SNOWFLAKE_BATCH_SIZE: usize = 50_000;

/// Infer Arrow DataType from a Snowflake type name string.
fn snowflake_type_to_arrow(type_name: &str) -> DataType {
    let upper = type_name.to_uppercase();
    if upper.contains("BOOLEAN") {
        DataType::Boolean
    } else if upper.contains("TINYINT") || upper.contains("BYTEINT") {
        DataType::Int8
    } else if upper.contains("SMALLINT") {
        DataType::Int16
    } else if upper.contains("INT") && !upper.contains("BIGINT") {
        DataType::Int32
    } else if upper.contains("BIGINT") {
        DataType::Int64
    } else if upper.contains("FLOAT") || upper.contains("REAL") {
        DataType::Float32
    } else if upper.contains("DOUBLE") || upper.contains("NUMBER") || upper.contains("DECIMAL") || upper.contains("NUMERIC") {
        DataType::Float64
    } else {
        DataType::Utf8
    }
}

/// Build a RecordBatch from JSON row data.
fn build_snowflake_batch(
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
    /// Read from Snowflake using a JSON config string and SQL query.
    /// Config format: `{"account":"abc123","user":"USER","password":"...","database":"DB","warehouse":"WH"}`
    /// Or simple format: `account=abc123 user=USER password=... database=DB warehouse=WH`
    pub fn read_snowflake(
        &self,
        config_str: &str,
        query: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let (account, user, password, database, warehouse, schema_name) =
            parse_snowflake_config(config_str)?;

        let url = format!(
            "https://{account}.snowflakecomputing.com/api/v2/statements"
        );

        let client = reqwest::blocking::Client::new();
        let mut body = serde_json::json!({
            "statement": query,
            "timeout": 600,
            "resultSetMetaData": {
                "format": "jsonv2"
            }
        });

        if !database.is_empty() {
            body["database"] = serde_json::Value::String(database);
        }
        if !warehouse.is_empty() {
            body["warehouse"] = serde_json::Value::String(warehouse);
        }
        if !schema_name.is_empty() {
            body["schema"] = serde_json::Value::String(schema_name);
        }

        let resp = client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .basic_auth(&user, Some(&password))
            .json(&body)
            .send()
            .map_err(|e| format!("Snowflake HTTP error: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().unwrap_or_default();
            return Err(format!("Snowflake API error {status}: {text}"));
        }

        let resp_json: serde_json::Value = resp
            .json()
            .map_err(|e| format!("Snowflake JSON parse error: {e}"))?;

        // Extract schema from resultSetMetaData
        let columns = resp_json["resultSetMetaData"]["rowType"]
            .as_array()
            .ok_or("Missing resultSetMetaData.rowType")?;

        let fields: Vec<Field> = columns
            .iter()
            .map(|col| {
                let name = col["name"].as_str().unwrap_or("unknown").to_string();
                let type_name = col["type"].as_str().unwrap_or("TEXT");
                Field::new(name, snowflake_type_to_arrow(type_name), true)
            })
            .collect();
        let col_types: Vec<DataType> = fields.iter().map(|f| f.data_type().clone()).collect();
        let schema = Arc::new(Schema::new(fields));

        // Extract data rows
        let data_rows = resp_json["data"]
            .as_array()
            .ok_or("Missing data array in Snowflake response")?;

        let mut chunk: Vec<Vec<Option<String>>> = Vec::with_capacity(SNOWFLAKE_BATCH_SIZE);
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

            if chunk.len() >= SNOWFLAKE_BATCH_SIZE {
                let batch = build_snowflake_batch(&chunk, &schema, &col_types)?;
                batches.push(batch);
                chunk.clear();
            }
        }

        if !chunk.is_empty() {
            let batch = build_snowflake_batch(&chunk, &schema, &col_types)?;
            batches.push(batch);
        }

        if batches.is_empty() {
            return Err("Snowflake query returned no data".to_string());
        }

        let table_name = "__snowflake_result";
        let _ = self.ctx.deregister_table(table_name);
        self.register_batches(table_name, schema, batches)?;

        self.rt
            .block_on(self.ctx.table(table_name))
            .map_err(|e| format!("Table reference error: {e}"))
    }
}

/// Parse Snowflake config from JSON or key=value format.
fn parse_snowflake_config(config_str: &str) -> Result<(String, String, String, String, String, String), String> {
    // Try JSON first
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(config_str) {
        let account = json["account"].as_str().unwrap_or("").to_string();
        let user = json["user"].as_str().unwrap_or("").to_string();
        let password = json["password"].as_str().unwrap_or("").to_string();
        let database = json["database"].as_str().unwrap_or("").to_string();
        let warehouse = json["warehouse"].as_str().unwrap_or("").to_string();
        let schema = json["schema"].as_str().unwrap_or("").to_string();
        return Ok((account, user, password, database, warehouse, schema));
    }

    // Key=value format
    let mut account = String::new();
    let mut user = String::new();
    let mut password = String::new();
    let mut database = String::new();
    let mut warehouse = String::new();
    let mut schema = String::new();

    for part in config_str.split_whitespace() {
        if let Some((key, value)) = part.split_once('=') {
            match key.to_lowercase().as_str() {
                "account" => account = value.to_string(),
                "user" => user = value.to_string(),
                "password" => password = value.to_string(),
                "database" => database = value.to_string(),
                "warehouse" => warehouse = value.to_string(),
                "schema" => schema = value.to_string(),
                _ => {}
            }
        }
    }

    if account.is_empty() {
        return Err("Snowflake config missing 'account'".to_string());
    }

    Ok((account, user, password, database, warehouse, schema))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_snowflake_config_json() {
        let (account, user, _pass, db, wh, _schema) = parse_snowflake_config(
            r#"{"account":"abc123","user":"ETL_USER","password":"secret","database":"ANALYTICS","warehouse":"COMPUTE_WH"}"#
        ).unwrap();
        assert_eq!(account, "abc123");
        assert_eq!(user, "ETL_USER");
        assert_eq!(db, "ANALYTICS");
        assert_eq!(wh, "COMPUTE_WH");
    }

    #[test]
    fn test_parse_snowflake_config_kv() {
        let (account, user, _pass, db, wh, _schema) = parse_snowflake_config(
            "account=abc123 user=ETL_USER password=secret database=ANALYTICS warehouse=COMPUTE_WH"
        ).unwrap();
        assert_eq!(account, "abc123");
        assert_eq!(user, "ETL_USER");
        assert_eq!(db, "ANALYTICS");
        assert_eq!(wh, "COMPUTE_WH");
    }

    #[test]
    #[ignore] // Requires a running Snowflake account
    fn test_read_snowflake() {
        let engine = DataEngine::new();
        let df = engine
            .read_snowflake(
                "account=abc123 user=USER password=pass database=DB warehouse=WH",
                "SELECT 1 AS num",
            )
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
    }
}
