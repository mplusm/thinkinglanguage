use std::sync::Arc;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::*;
use datafusion::execution::context::SessionContext;
use tokio::runtime::Runtime;

/// Synchronous wrapper around DataFusion's async SessionContext.
pub struct DataEngine {
    pub ctx: SessionContext,
    pub rt: Arc<Runtime>,
}

impl DataEngine {
    pub fn new() -> Self {
        let rt = Arc::new(
            Runtime::new().expect("Failed to create tokio runtime for DataEngine"),
        );
        DataEngine {
            ctx: SessionContext::new(),
            rt,
        }
    }

    /// Execute a DataFusion DataFrame and collect results synchronously.
    pub fn collect(&self, df: DataFrame) -> Result<Vec<RecordBatch>, String> {
        self.rt
            .block_on(df.collect())
            .map_err(|e| format!("DataFusion collect error: {e}"))
    }

    /// Format collected batches as a pretty table string.
    pub fn format_batches(batches: &[RecordBatch]) -> Result<String, String> {
        pretty_format_batches(batches)
            .map(|t| t.to_string())
            .map_err(|e| format!("Format error: {e}"))
    }

    /// Register a RecordBatch as a named table in the session.
    pub fn register_batch(
        &self,
        name: &str,
        batch: RecordBatch,
    ) -> Result<(), String> {
        let schema = batch.schema();
        let provider = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| format!("MemTable error: {e}"))?;
        self.ctx
            .register_table(name, Arc::new(provider))
            .map_err(|e| format!("Register table error: {e}"))?;
        Ok(())
    }

    /// Run a SQL query and return results.
    pub fn sql(&self, query: &str) -> Result<DataFrame, String> {
        self.rt
            .block_on(self.ctx.sql(query))
            .map_err(|e| format!("SQL error: {e}"))
    }

    /// Get the underlying session context for DataFusion operations.
    pub fn session_ctx(&self) -> &SessionContext {
        &self.ctx
    }

    /// Get a reference to the tokio Runtime.
    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.rt
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_engine_basic() {
        let engine = DataEngine::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap();

        engine.register_batch("test_table", batch).unwrap();
        let df = engine.sql("SELECT * FROM test_table WHERE id > 1").unwrap();
        let results = engine.collect(df).unwrap();
        assert_eq!(results[0].num_rows(), 2);
    }
}
