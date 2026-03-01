use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::context::SessionContext;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Configuration for the DataEngine.
pub struct DataEngineConfig {
    /// Maximum memory in bytes for the DataFusion pool (default: 512MB).
    pub max_memory_bytes: usize,
    /// Enable spill-to-disk when memory limit is reached.
    pub spill_to_disk: bool,
    /// Directory for spill files (default: system temp dir).
    pub spill_path: Option<String>,
}

impl Default for DataEngineConfig {
    fn default() -> Self {
        DataEngineConfig {
            max_memory_bytes: 512 * 1024 * 1024, // 512 MB
            spill_to_disk: true,
            spill_path: None,
        }
    }
}

/// Synchronous wrapper around DataFusion's async SessionContext.
pub struct DataEngine {
    pub ctx: SessionContext,
    pub rt: Arc<Runtime>,
}

impl Default for DataEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl DataEngine {
    /// Create a new DataEngine with default configuration.
    /// Backward-compatible with existing code.
    pub fn new() -> Self {
        Self::with_config(DataEngineConfig::default())
    }

    /// Create a new DataEngine with custom configuration.
    pub fn with_config(config: DataEngineConfig) -> Self {
        let rt = Arc::new(Runtime::new().expect("Failed to create tokio runtime for DataEngine"));

        // Build runtime environment with memory pool and disk manager
        let pool = FairSpillPool::new(config.max_memory_bytes);

        let mut rt_builder = RuntimeEnvBuilder::new().with_memory_pool(Arc::new(pool));

        if config.spill_to_disk {
            let disk_config = if let Some(ref path) = config.spill_path {
                DiskManagerConfig::new_specified(vec![path.clone().into()])
            } else {
                DiskManagerConfig::NewOs
            };
            rt_builder = rt_builder.with_disk_manager(disk_config);
        }

        let runtime_env = rt_builder.build().expect("Failed to build RuntimeEnv");

        // Configure session with parallelism
        let target_partitions = num_cpus::get();
        let session_config = SessionConfig::new().with_target_partitions(target_partitions);

        let ctx = SessionContext::new_with_config_rt(session_config, Arc::new(runtime_env));

        DataEngine { ctx, rt }
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
    pub fn register_batch(&self, name: &str, batch: RecordBatch) -> Result<(), String> {
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

    #[test]
    fn test_engine_with_config() {
        let config = DataEngineConfig {
            max_memory_bytes: 256 * 1024 * 1024,
            spill_to_disk: true,
            spill_path: None,
        };
        let engine = DataEngine::with_config(config);
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
        engine.register_batch("t", batch).unwrap();
        let df = engine.sql("SELECT * FROM t").unwrap();
        let results = engine.collect(df).unwrap();
        assert_eq!(results[0].num_rows(), 3);
    }
}
