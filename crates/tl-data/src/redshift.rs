// ThinkingLanguage — Amazon Redshift Connector
// Licensed under MIT OR Apache-2.0
//
// Redshift uses the PostgreSQL wire protocol. This thin wrapper delegates
// to the existing PG cursor-batching code with SSL mode enforcement.

use crate::engine::DataEngine;

impl DataEngine {
    /// Read from Amazon Redshift using a connection string and SQL query.
    /// Delegates to the PostgreSQL connector (Redshift is PG wire-compatible).
    /// Ensures SSL mode is set for Redshift cluster connections.
    pub fn read_redshift(
        &self,
        conn_str: &str,
        query: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let conn_str = if !conn_str.contains("sslmode") {
            format!("{conn_str} sslmode=require")
        } else {
            conn_str.to_string()
        };
        self.query_postgres(&conn_str, query, "__redshift_result")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore] // Requires a running Redshift cluster
    fn test_read_redshift() {
        let engine = DataEngine::new();
        let df = engine
            .read_redshift(
                "host=cluster.abc.redshift.amazonaws.com port=5439 dbname=prod user=admin password=secret",
                "SELECT 1 AS test_col",
            )
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
    }
}
