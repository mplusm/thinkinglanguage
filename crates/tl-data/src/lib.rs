pub mod engine;
pub mod io;
pub mod pg;
pub mod quality;
pub mod translate;
pub mod validate;

#[cfg(feature = "bigquery")]
pub mod bigquery;
#[cfg(feature = "clickhouse")]
pub mod clickhouse_conn;
#[cfg(feature = "databricks")]
pub mod databricks;
#[cfg(feature = "duckdb")]
pub mod duckdb_conn;
#[cfg(feature = "mongodb")]
pub mod mongo_conn;
#[cfg(feature = "mssql")]
pub mod mssql_conn;
#[cfg(feature = "mysql")]
pub mod mysql_conn;
#[cfg(feature = "redis")]
pub mod redis_conn;
pub mod redshift;
#[cfg(feature = "s3")]
pub mod s3;
#[cfg(feature = "sftp")]
pub mod sftp_conn;
#[cfg(feature = "snowflake")]
pub mod snowflake;
#[cfg(feature = "sqlite")]
pub mod sqlite_conn;

pub use engine::DataEngine;

// Re-export datafusion types needed by the interpreter
pub use datafusion;
pub use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
pub use datafusion::prelude::DataFrame;
pub use datafusion::prelude::JoinType;
pub use datafusion::prelude::{col, lit};
