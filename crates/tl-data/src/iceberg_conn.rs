// ThinkingLanguage — Apache Iceberg Connector
// Licensed under MIT OR Apache-2.0
//
// Reads Apache Iceberg tables into DataFusion DataFrames.
//
// We deliberately depend on the *core* `iceberg` crate (not `iceberg-datafusion`).
// iceberg-datafusion tracks a much newer DataFusion (52+), which would conflict
// with TL's DataFusion 44. The core crate at 0.4.0 uses arrow 53 — the SAME arrow
// as DataFusion 44 — so Iceberg's RecordBatches drop straight into our engine
// with no IPC version bridge (unlike the DuckDB connector).
//
// The entry point is `StaticTable`, which reads a table directly from its
// `metadata.json` location with no catalog server — ideal for files on local
// disk or object storage (s3://, gs://). Object-store credentials/region are
// passed through as FileIO properties.

use datafusion::arrow::array::RecordBatch;
use futures::StreamExt;
use iceberg::TableIdent;
use iceberg::io::FileIO;
use iceberg::table::StaticTable;

use crate::engine::DataEngine;

impl DataEngine {
    /// Read an Apache Iceberg table from its metadata file location.
    ///
    /// `metadata_location` is the path/URL to a table's `metadata.json`
    /// (e.g. `/warehouse/db/table/metadata/00003-....metadata.json` or
    /// `s3://bucket/warehouse/db/table/metadata/00003-....metadata.json`).
    ///
    /// `props` are FileIO properties forwarded to the storage backend — for S3
    /// this is typically `s3.endpoint`, `s3.region`, `s3.access-key-id`,
    /// `s3.secret-access-key`. For local files `props` may be empty.
    pub fn read_iceberg(
        &self,
        metadata_location: &str,
        props: Vec<(String, String)>,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        // Local filesystem paths must be absolute for FileIO scheme detection.
        let location = normalize_location(metadata_location)?;

        self.rt.block_on(async {
            // Build FileIO with the scheme inferred from the location.
            let mut builder = FileIO::from_path(&location)
                .map_err(|e| format!("Iceberg FileIO error: {e}"))?;
            for (k, v) in &props {
                builder = builder.with_prop(k, v);
            }
            let file_io = builder
                .build()
                .map_err(|e| format!("Iceberg FileIO build error: {e}"))?;

            // A static table needs an identifier even though it is catalog-less.
            // Derive a best-effort name from the table directory.
            let table_name = derive_table_name(&location);
            let table_ident = TableIdent::from_strs(["default", &table_name])
                .map_err(|e| format!("Iceberg table ident error: {e}"))?;

            let static_table =
                StaticTable::from_metadata_file(&location, table_ident, file_io)
                    .await
                    .map_err(|e| format!("Iceberg metadata read error: {e}"))?;
            let table = static_table.into_table();

            // Scan all columns of the current snapshot.
            let scan = table
                .scan()
                .select_all()
                .build()
                .map_err(|e| format!("Iceberg scan build error: {e}"))?;

            let mut stream = scan
                .to_arrow()
                .await
                .map_err(|e| format!("Iceberg scan error: {e}"))?;

            let mut batches: Vec<RecordBatch> = Vec::new();
            while let Some(batch) = stream.next().await {
                let batch = batch.map_err(|e| format!("Iceberg batch error: {e}"))?;
                batches.push(batch);
            }

            // Prefer the Arrow schema of the returned data; fall back to nothing
            // useful if the table is empty (no rows scanned).
            let schema = match batches.first() {
                Some(b) => b.schema(),
                None => {
                    return Err(
                        "Iceberg table produced no data (empty snapshot or all files pruned)"
                            .to_string(),
                    );
                }
            };

            let table_ref = "__iceberg_result";
            self.register_batches(table_ref, schema, batches)?;

            self.ctx
                .table(table_ref)
                .await
                .map_err(|e| format!("Table reference error: {e}"))
        })
    }
}

/// Turn a user-supplied location into something FileIO can resolve. URLs
/// (`s3://`, `gs://`, `file://`, …) pass through; bare local paths are made
/// absolute so FileIO's scheme detection treats them as `file://`.
fn normalize_location(loc: &str) -> Result<String, String> {
    if loc.contains("://") {
        return Ok(loc.to_string());
    }
    let path = std::path::Path::new(loc);
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| format!("cannot resolve current dir: {e}"))?
            .join(path)
    };
    abs.to_str()
        .map(|s| s.to_string())
        .ok_or_else(|| "Iceberg location is not valid UTF-8".to_string())
}

/// Best-effort table name from a `.../<table>/metadata/<file>.metadata.json`
/// layout. Used only as the catalog-less table identifier.
fn derive_table_name(location: &str) -> String {
    let trimmed = location.split("://").last().unwrap_or(location);
    let parts: Vec<&str> = trimmed
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    // Walk up past the `metadata/<file>` tail to the table directory.
    if let Some(pos) = parts.iter().rposition(|p| *p == "metadata") {
        if pos > 0 {
            return parts[pos - 1].to_string();
        }
    }
    "table".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_locations_pass_through_unchanged() {
        let s3 = "s3://bucket/wh/db/orders/metadata/00001-x.metadata.json";
        assert_eq!(normalize_location(s3).unwrap(), s3);
        let gs = "gs://bucket/orders/metadata/v.metadata.json";
        assert_eq!(normalize_location(gs).unwrap(), gs);
    }

    #[test]
    fn relative_local_paths_become_absolute() {
        let out = normalize_location("data/orders/metadata/v.metadata.json").unwrap();
        assert!(
            std::path::Path::new(&out).is_absolute(),
            "expected absolute, got {out}"
        );
    }

    #[test]
    fn table_name_derived_from_metadata_layout() {
        assert_eq!(
            derive_table_name("s3://bucket/wh/sales/orders/metadata/00002-x.metadata.json"),
            "orders"
        );
        assert_eq!(
            derive_table_name("/wh/db/customers/metadata/00000-y.metadata.json"),
            "customers"
        );
        // No recognizable layout falls back to a placeholder.
        assert_eq!(derive_table_name("/tmp/loose.metadata.json"), "table");
    }
}
