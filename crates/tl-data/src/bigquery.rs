// ThinkingLanguage — BigQuery Connector
// Licensed under MIT OR Apache-2.0
//
// Uses BigQuery REST API (jobs.query) with service account or API key auth.
// Parses JSON result sets into Arrow RecordBatches.

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use crate::engine::DataEngine;

const BIGQUERY_BATCH_SIZE: usize = 50_000;

/// Map BigQuery type names to Arrow DataTypes.
fn bq_type_to_arrow(bq_type: &str) -> DataType {
    match bq_type.to_uppercase().as_str() {
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "INTEGER" | "INT64" => DataType::Int64,
        "FLOAT" | "FLOAT64" => DataType::Float64,
        "NUMERIC" | "BIGNUMERIC" | "DECIMAL" | "BIGDECIMAL" => DataType::Float64,
        _ => DataType::Utf8,
    }
}

/// Build a RecordBatch from BigQuery JSON row data.
fn build_bq_batch(
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
            DataType::Int64 => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| r[col_idx].as_ref().and_then(|s| s.parse::<i64>().ok()))
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = rows
                    .iter()
                    .map(|r| r[col_idx].as_ref().and_then(|s| s.parse::<f64>().ok()))
                    .collect();
                Arc::new(Float64Array::from(values))
            }
            _ => {
                let values: Vec<Option<&str>> =
                    rows.iter().map(|r| r[col_idx].as_deref()).collect();
                Arc::new(StringArray::from(values))
            }
        };
        arrays.push(array);
    }

    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| format!("Arrow RecordBatch creation error: {e}"))
}

impl DataEngine {
    /// Read from BigQuery using a project ID and SQL query.
    /// Requires `GOOGLE_APPLICATION_CREDENTIALS` env var or `TL_BIGQUERY_KEY` for API key.
    /// Config can be JSON: `{"project":"my-project","credentials_file":"/path/to/sa.json"}`
    /// Or simple: `project=my-project`
    pub fn read_bigquery(
        &self,
        config_str: &str,
        query: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let (project_id, access_token) = parse_bq_config(config_str)?;

        let url =
            format!("https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/queries");

        let client = reqwest::blocking::Client::new();
        let body = serde_json::json!({
            "query": query,
            "useLegacySql": false,
            "maxResults": 100000
        });

        let resp = client
            .post(&url)
            .header("Content-Type", "application/json")
            .bearer_auth(&access_token)
            .json(&body)
            .send()
            .map_err(|e| format!("BigQuery HTTP error: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().unwrap_or_default();
            return Err(format!("BigQuery API error {status}: {text}"));
        }

        let resp_json: serde_json::Value = resp
            .json()
            .map_err(|e| format!("BigQuery JSON parse error: {e}"))?;

        // Extract schema
        let bq_fields = resp_json["schema"]["fields"]
            .as_array()
            .ok_or("Missing schema.fields in BigQuery response")?;

        let fields: Vec<Field> = bq_fields
            .iter()
            .map(|f| {
                let name = f["name"].as_str().unwrap_or("unknown").to_string();
                let bq_type = f["type"].as_str().unwrap_or("STRING");
                Field::new(name, bq_type_to_arrow(bq_type), true)
            })
            .collect();
        let col_types: Vec<DataType> = fields.iter().map(|f| f.data_type().clone()).collect();
        let schema = Arc::new(Schema::new(fields));

        // Extract rows — BigQuery returns {f: [{v: "value"}, ...]} format
        let bq_rows = resp_json["rows"]
            .as_array()
            .ok_or("Missing rows in BigQuery response")?;

        let mut chunk: Vec<Vec<Option<String>>> = Vec::with_capacity(BIGQUERY_BATCH_SIZE);
        let mut batches: Vec<RecordBatch> = Vec::new();

        for row in bq_rows {
            let cells = row["f"]
                .as_array()
                .ok_or("Invalid row format in BigQuery response")?;
            let values: Vec<Option<String>> = cells
                .iter()
                .map(|cell: &serde_json::Value| {
                    let v = &cell["v"];
                    if v.is_null() {
                        None
                    } else {
                        Some(v.as_str().unwrap_or(&v.to_string()).to_string())
                    }
                })
                .collect();
            chunk.push(values);

            if chunk.len() >= BIGQUERY_BATCH_SIZE {
                let batch = build_bq_batch(&chunk, &schema, &col_types)?;
                batches.push(batch);
                chunk.clear();
            }
        }

        if !chunk.is_empty() {
            let batch = build_bq_batch(&chunk, &schema, &col_types)?;
            batches.push(batch);
        }

        if batches.is_empty() {
            return Err("BigQuery query returned no rows".to_string());
        }

        let table_name = "__bigquery_result";
        let _ = self.ctx.deregister_table(table_name);
        self.register_batches(table_name, schema, batches)?;

        self.rt
            .block_on(self.ctx.table(table_name))
            .map_err(|e| format!("Table reference error: {e}"))
    }
}

/// Parse BigQuery config and obtain access token.
fn parse_bq_config(config_str: &str) -> Result<(String, String), String> {
    let project_id;
    let access_token;

    if let Ok(json) = serde_json::from_str::<serde_json::Value>(config_str) {
        project_id = json["project"].as_str().unwrap_or("").to_string();
        access_token = json["access_token"]
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                std::env::var("TL_BIGQUERY_TOKEN")
                    .or_else(|_| std::env::var("GOOGLE_ACCESS_TOKEN"))
                    .unwrap_or_default()
            });
    } else {
        // key=value format
        project_id = config_str
            .split_whitespace()
            .find_map(|p| p.strip_prefix("project="))
            .unwrap_or("")
            .to_string();
        access_token = std::env::var("TL_BIGQUERY_TOKEN")
            .or_else(|_| std::env::var("GOOGLE_ACCESS_TOKEN"))
            .unwrap_or_default();
    }

    if project_id.is_empty() {
        return Err("BigQuery config missing 'project'".to_string());
    }

    Ok((project_id, access_token))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bq_config() {
        let (project, _token) = parse_bq_config(r#"{"project":"my-gcp-project"}"#).unwrap();
        assert_eq!(project, "my-gcp-project");
    }

    #[test]
    #[ignore] // Requires BigQuery access
    fn test_read_bigquery() {
        let engine = DataEngine::new();
        let df = engine
            .read_bigquery(
                r#"{"project":"my-project"}"#,
                "SELECT 1 AS num, 'hello' AS greeting",
            )
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
    }
}
