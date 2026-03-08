// ThinkingLanguage — MongoDB Connector
// Licensed under MIT OR Apache-2.0
//
// Reads MongoDB collections into DataFusion DataFrames.
// Flattens BSON documents to tabular Arrow format.

use datafusion::arrow::array::*;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use mongodb::bson::{doc, Bson, Document};
use mongodb::options::ClientOptions;
use mongodb::Client;
use std::sync::Arc;

use crate::engine::DataEngine;

/// Default batch size for MongoDB reads (documents per Arrow batch).
const MONGO_BATCH_SIZE: usize = 50_000;

/// Number of documents to sample for schema inference.
const SCHEMA_SAMPLE_SIZE: usize = 100;

/// Map a BSON value to an Arrow DataType.
fn bson_type_to_arrow(bson: &Bson) -> DataType {
    match bson {
        Bson::Double(_) => DataType::Float64,
        Bson::String(_) => DataType::Utf8,
        Bson::Boolean(_) => DataType::Boolean,
        Bson::Int32(_) => DataType::Int32,
        Bson::Int64(_) => DataType::Int64,
        Bson::ObjectId(_) => DataType::Utf8,
        Bson::DateTime(_) => DataType::Int64,
        Bson::Null => DataType::Utf8, // default nullable to Utf8
        _ => DataType::Utf8,
    }
}

/// Infer an Arrow schema from a sample of BSON documents.
/// Builds the union of all field names across all sampled documents,
/// inferring the type from the first non-null occurrence of each field.
fn infer_schema(docs: &[Document]) -> (Arc<Schema>, Vec<String>) {
    let mut field_names: Vec<String> = Vec::new();
    let mut field_types: Vec<DataType> = Vec::new();

    for document in docs {
        for (key, value) in document.iter() {
            if let Some(idx) = field_names.iter().position(|n| n == key) {
                // If we previously inferred Utf8 from null, upgrade to the real type
                if field_types[idx] == DataType::Utf8 && !matches!(value, Bson::Null) {
                    let inferred = bson_type_to_arrow(value);
                    if inferred != DataType::Utf8 || matches!(value, Bson::String(_)) {
                        field_types[idx] = inferred;
                    }
                }
            } else {
                field_names.push(key.clone());
                field_types.push(bson_type_to_arrow(value));
            }
        }
    }

    let fields: Vec<Field> = field_names
        .iter()
        .zip(field_types.iter())
        .map(|(name, dt)| Field::new(name, dt.clone(), true))
        .collect();

    (Arc::new(Schema::new(fields)), field_names)
}

/// Extract a value from a BSON document field, converting to the target Arrow type.
fn extract_bson_value(bson: Option<&Bson>, target_type: &DataType) -> BsonScalar {
    match bson {
        None | Some(Bson::Null) => BsonScalar::Null,
        Some(value) => match target_type {
            DataType::Boolean => match value {
                Bson::Boolean(b) => BsonScalar::Bool(*b),
                _ => BsonScalar::Null,
            },
            DataType::Int32 => match value {
                Bson::Int32(n) => BsonScalar::I32(*n),
                Bson::Int64(n) => BsonScalar::I32(*n as i32),
                Bson::Double(f) => BsonScalar::I32(*f as i32),
                _ => BsonScalar::Null,
            },
            DataType::Int64 => match value {
                Bson::Int64(n) => BsonScalar::I64(*n),
                Bson::Int32(n) => BsonScalar::I64(*n as i64),
                Bson::Double(f) => BsonScalar::I64(*f as i64),
                Bson::DateTime(dt) => BsonScalar::I64(dt.timestamp_millis()),
                _ => BsonScalar::Null,
            },
            DataType::Float64 => match value {
                Bson::Double(f) => BsonScalar::F64(*f),
                Bson::Int32(n) => BsonScalar::F64(*n as f64),
                Bson::Int64(n) => BsonScalar::F64(*n as f64),
                _ => BsonScalar::Null,
            },
            _ => {
                // Utf8 fallback
                let s = match value {
                    Bson::String(s) => s.clone(),
                    Bson::ObjectId(oid) => oid.to_hex(),
                    Bson::DateTime(dt) => dt.try_to_rfc3339_string().unwrap_or_default(),
                    other => format!("{other}"),
                };
                BsonScalar::Str(s)
            }
        },
    }
}

/// Intermediate scalar for building Arrow arrays from BSON values.
enum BsonScalar {
    Null,
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    Str(String),
}

/// Build a RecordBatch from a chunk of BSON documents.
fn build_mongo_batch(
    docs: &[Document],
    schema: &Arc<Schema>,
    field_names: &[String],
    col_types: &[DataType],
) -> Result<RecordBatch, String> {
    let mut arrays: Vec<Arc<dyn Array>> = Vec::new();

    for (col_idx, arrow_type) in col_types.iter().enumerate() {
        let field_name = &field_names[col_idx];
        let array: Arc<dyn Array> = match arrow_type {
            DataType::Boolean => {
                let values: Vec<Option<bool>> = docs
                    .iter()
                    .map(|d| match extract_bson_value(d.get(field_name), arrow_type) {
                        BsonScalar::Bool(b) => Some(b),
                        _ => None,
                    })
                    .collect();
                Arc::new(BooleanArray::from(values))
            }
            DataType::Int32 => {
                let values: Vec<Option<i32>> = docs
                    .iter()
                    .map(|d| match extract_bson_value(d.get(field_name), arrow_type) {
                        BsonScalar::I32(n) => Some(n),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int32Array::from(values))
            }
            DataType::Int64 => {
                let values: Vec<Option<i64>> = docs
                    .iter()
                    .map(|d| match extract_bson_value(d.get(field_name), arrow_type) {
                        BsonScalar::I64(n) => Some(n),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = docs
                    .iter()
                    .map(|d| match extract_bson_value(d.get(field_name), arrow_type) {
                        BsonScalar::F64(f) => Some(f),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float64Array::from(values))
            }
            _ => {
                // Utf8 fallback
                let values: Vec<Option<String>> = docs
                    .iter()
                    .map(|d| match extract_bson_value(d.get(field_name), arrow_type) {
                        BsonScalar::Str(s) => Some(s),
                        BsonScalar::Null => None,
                        _ => None,
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
    /// Read from MongoDB using a connection string, database, collection, and optional filter.
    /// Uses schema inference from the first 100 documents and batched Arrow conversion
    /// (50K docs per batch) to reduce peak memory and enable DataFusion partition parallelism.
    ///
    /// The `filter_json` parameter is a JSON string representing a MongoDB query filter,
    /// e.g. `"{\"age\": {\"$gt\": 21}}"`. Pass `"{}"` for no filter.
    pub fn read_mongo(
        &self,
        conn_str: &str,
        database: &str,
        collection: &str,
        filter_json: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        self.rt.block_on(async {
            // Parse connection options
            let client_options = ClientOptions::parse(conn_str)
                .await
                .map_err(|e| format!("MongoDB connection string error: {e}"))?;
            let client = Client::with_options(client_options)
                .map_err(|e| format!("MongoDB client error: {e}"))?;

            let db = client.database(database);
            let coll = db.collection::<Document>(collection);

            // Parse filter JSON to BSON Document
            let filter_doc: Document = if filter_json.is_empty() || filter_json == "{}" {
                doc! {}
            } else {
                let json_value: serde_json::Value = serde_json::from_str(filter_json)
                    .map_err(|e| format!("Invalid filter JSON: {e}"))?;
                mongodb::bson::to_document(&json_value)
                    .map_err(|e| format!("Filter to BSON conversion error: {e}"))?
            };

            // Phase 1: Sample documents for schema inference
            let mut sample_cursor = coll
                .find(filter_doc.clone())
                .batch_size(SCHEMA_SAMPLE_SIZE as u32)
                .await
                .map_err(|e| format!("MongoDB query error: {e}"))?;

            let mut sample_docs: Vec<Document> = Vec::with_capacity(SCHEMA_SAMPLE_SIZE);
            use futures::StreamExt;
            while let Some(result) = sample_cursor.next().await {
                let document = result.map_err(|e| format!("MongoDB cursor error: {e}"))?;
                sample_docs.push(document);
                if sample_docs.len() >= SCHEMA_SAMPLE_SIZE {
                    break;
                }
            }
            // Explicitly drop cursor before opening a new one
            drop(sample_cursor);

            if sample_docs.is_empty() {
                return Err("MongoDB query returned no documents".to_string());
            }

            let (schema, field_names) = infer_schema(&sample_docs);
            let col_types: Vec<DataType> = schema.fields().iter().map(|f| f.data_type().clone()).collect();

            // Phase 2: Full cursor iteration — start fresh to include all docs
            let mut cursor = coll
                .find(filter_doc)
                .batch_size(MONGO_BATCH_SIZE as u32)
                .await
                .map_err(|e| format!("MongoDB query error: {e}"))?;

            let mut all_docs: Vec<Document> = Vec::new();
            while let Some(result) = cursor.next().await {
                let document = result.map_err(|e| format!("MongoDB cursor error: {e}"))?;
                all_docs.push(document);
            }

            // Build batches in chunks for large result sets
            let mut batches: Vec<RecordBatch> = Vec::new();
            for chunk in all_docs.chunks(MONGO_BATCH_SIZE) {
                let batch = build_mongo_batch(chunk, &schema, &field_names, &col_types)?;
                batches.push(batch);
            }

            let table_name = "__mongo_result";
            self.register_batches(table_name, schema, batches)?;

            self.ctx
                .table(table_name)
                .await
                .map_err(|e| format!("Table reference error: {e}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore] // Requires a running MongoDB instance
    fn test_read_mongo() {
        let engine = DataEngine::new();
        let df = engine
            .read_mongo(
                "mongodb://localhost:27017",
                "testdb",
                "users",
                "{}",
            )
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
    }

    #[test]
    #[ignore] // Requires a running MongoDB instance
    fn test_read_mongo_with_filter() {
        let engine = DataEngine::new();
        let df = engine
            .read_mongo(
                "mongodb://localhost:27017",
                "testdb",
                "users",
                r#"{"age": {"$gt": 21}}"#,
            )
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
    }

    #[test]
    fn test_bson_type_mapping() {
        assert_eq!(bson_type_to_arrow(&Bson::Double(1.0)), DataType::Float64);
        assert_eq!(bson_type_to_arrow(&Bson::String("hi".into())), DataType::Utf8);
        assert_eq!(bson_type_to_arrow(&Bson::Boolean(true)), DataType::Boolean);
        assert_eq!(bson_type_to_arrow(&Bson::Int32(42)), DataType::Int32);
        assert_eq!(bson_type_to_arrow(&Bson::Int64(999)), DataType::Int64);
        assert_eq!(bson_type_to_arrow(&Bson::Null), DataType::Utf8);
    }

    #[test]
    fn test_infer_schema_union() {
        let docs = vec![
            doc! { "name": "Alice", "age": 30 },
            doc! { "name": "Bob", "score": 95.5 },
            doc! { "name": "Carol", "age": 25, "score": 88.0 },
        ];
        let (schema, field_names) = infer_schema(&docs);
        assert_eq!(field_names, vec!["name", "age", "score"]);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Int32);
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
    }

    #[test]
    fn test_build_batch_with_missing_fields() {
        let docs = vec![
            doc! { "name": "Alice", "age": 30 },
            doc! { "name": "Bob" }, // missing "age"
        ];
        let (schema, field_names) = infer_schema(&docs);
        let col_types: Vec<DataType> = schema.fields().iter().map(|f| f.data_type().clone()).collect();
        let batch = build_mongo_batch(&docs, &schema, &field_names, &col_types).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        // Second row's "age" should be null
        let age_col = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(age_col.is_valid(0));
        assert!(!age_col.is_valid(1));
    }
}
