// ThinkingLanguage — S3 I/O Connector
// Licensed under MIT OR Apache-2.0
//
// Register S3 object store with DataFusion, then existing read_csv/read_parquet
// work with s3:// URLs automatically.

use std::sync::Arc;
use object_store::aws::AmazonS3Builder;
use url::Url;

use crate::engine::DataEngine;

impl DataEngine {
    /// Register an S3 bucket as an object store in the DataFusion session.
    /// After registration, `read_csv("s3://bucket/file.csv")` works.
    pub fn register_s3(
        &self,
        bucket: &str,
        region: &str,
        access_key: Option<&str>,
        secret_key: Option<&str>,
        endpoint: Option<&str>,
    ) -> Result<(), String> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(region);

        if let Some(ak) = access_key {
            builder = builder.with_access_key_id(ak);
        }
        if let Some(sk) = secret_key {
            builder = builder.with_secret_access_key(sk);
        }
        if let Some(ep) = endpoint {
            builder = builder.with_endpoint(ep).with_allow_http(true);
        }

        let store = builder.build()
            .map_err(|e| format!("S3 store creation error: {e}"))?;

        let url = Url::parse(&format!("s3://{bucket}"))
            .map_err(|e| format!("S3 URL parse error: {e}"))?;

        self.rt.block_on(self.ctx.register_object_store(&url, Arc::new(store)));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore] // Requires S3 access or MinIO
    fn test_register_s3() {
        let engine = DataEngine::new();
        let result = engine.register_s3(
            "test-bucket",
            "us-east-1",
            Some("test-key"),
            Some("test-secret"),
            Some("http://localhost:9000"),
        );
        assert!(result.is_ok());
    }

    #[test]
    #[ignore] // Requires S3 access or MinIO
    fn test_s3_read_csv() {
        let engine = DataEngine::new();
        engine.register_s3(
            "test-bucket",
            "us-east-1",
            Some("minioadmin"),
            Some("minioadmin"),
            Some("http://localhost:9000"),
        ).unwrap();
        // After registration, read_csv with s3:// URL should work
        let _df = engine.read_csv("s3://test-bucket/test.csv");
    }
}
