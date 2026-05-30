// ThinkingLanguage — Apache Iceberg Connector
// Licensed under MIT OR Apache-2.0
//
// Reads Apache Iceberg tables into DataFusion DataFrames, plus table
// introspection (snapshot history, schema).
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

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use futures::StreamExt;
use iceberg::TableIdent;
use iceberg::io::FileIO;
use iceberg::table::{StaticTable, Table};

use crate::engine::DataEngine;

/// Options controlling an Iceberg read.
#[derive(Default)]
pub struct IcebergReadOptions {
    /// Columns to project. Empty = all columns (`select_all`).
    pub columns: Vec<String>,
    /// Read an older snapshot for time-travel. `None` = current snapshot.
    pub snapshot_id: Option<i64>,
    /// FileIO / object-store properties (e.g. `s3.region`, `s3.access-key-id`).
    pub props: Vec<(String, String)>,
}

impl DataEngine {
    /// Read an Apache Iceberg table from its metadata file location.
    ///
    /// Supports column projection (`opts.columns`) and time-travel
    /// (`opts.snapshot_id`) — both pushed into the Iceberg scan so only the
    /// requested columns / snapshot's data files are read.
    pub fn read_iceberg(
        &self,
        metadata_location: &str,
        opts: IcebergReadOptions,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let location = normalize_location(metadata_location)?;

        self.rt.block_on(async {
            let table = load_table(&location, &opts.props).await?;

            // Build the scan: projection + optional snapshot (time-travel).
            let mut builder = table.scan();
            builder = if opts.columns.is_empty() {
                builder.select_all()
            } else {
                builder.select(opts.columns.iter())
            };
            if let Some(id) = opts.snapshot_id {
                builder = builder.snapshot_id(id);
            }
            let scan = builder
                .build()
                .map_err(|e| format!("Iceberg scan build error: {e}"))?;

            let mut stream = scan
                .to_arrow()
                .await
                .map_err(|e| format!("Iceberg scan error: {e}"))?;

            let mut batches: Vec<RecordBatch> = Vec::new();
            while let Some(batch) = stream.next().await {
                batches.push(batch.map_err(|e| format!("Iceberg batch error: {e}"))?);
            }

            let schema = match batches.first() {
                Some(b) => b.schema(),
                None => {
                    return Err(
                        "Iceberg scan produced no data (empty snapshot or all files pruned)"
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

    /// Return the snapshot history of an Iceberg table as a table:
    /// `snapshot_id, parent_snapshot_id, sequence_number, timestamp_ms,
    /// operation, summary, manifest_list`. The rows let you pick a
    /// `snapshot_id` for time-travel reads.
    pub fn iceberg_snapshots(
        &self,
        metadata_location: &str,
        props: Vec<(String, String)>,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let location = normalize_location(metadata_location)?;

        self.rt.block_on(async {
            let static_table = load_static_table(&location, &props).await?;
            let meta = static_table.metadata();
            let current = meta.current_snapshot_id();

            let mut ids = Vec::new();
            let mut parents: Vec<Option<i64>> = Vec::new();
            let mut seqs = Vec::new();
            let mut timestamps = Vec::new();
            let mut operations = Vec::new();
            let mut summaries = Vec::new();
            let mut manifests = Vec::new();
            let mut is_current = Vec::new();

            // Snapshots are returned in an arbitrary order; sort by time so the
            // history reads top-to-bottom oldest → newest.
            let mut snaps: Vec<_> = meta.snapshots().collect();
            snaps.sort_by_key(|s| s.timestamp_ms());
            for snap in snaps {
                ids.push(snap.snapshot_id());
                parents.push(snap.parent_snapshot_id());
                seqs.push(snap.sequence_number());
                timestamps.push(snap.timestamp_ms());
                let summary = snap.summary();
                operations.push(format!("{:?}", summary.operation).to_lowercase());
                summaries.push(summarize_props(&summary.additional_properties));
                manifests.push(snap.manifest_list().to_string());
                is_current.push(Some(snap.snapshot_id()) == current);
            }

            let schema = Arc::new(Schema::new(vec![
                Field::new("snapshot_id", DataType::Int64, false),
                Field::new("parent_snapshot_id", DataType::Int64, true),
                Field::new("sequence_number", DataType::Int64, false),
                Field::new("timestamp_ms", DataType::Int64, false),
                Field::new("operation", DataType::Utf8, false),
                Field::new("summary", DataType::Utf8, false),
                Field::new("manifest_list", DataType::Utf8, false),
                Field::new("is_current", DataType::Boolean, false),
            ]));

            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(parents)),
                Arc::new(Int64Array::from(seqs)),
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(StringArray::from(operations)),
                Arc::new(StringArray::from(summaries)),
                Arc::new(StringArray::from(manifests)),
                Arc::new(BooleanArray::from(is_current)),
            ];

            let batch = RecordBatch::try_new(schema.clone(), columns)
                .map_err(|e| format!("Iceberg snapshot batch error: {e}"))?;
            self.register_batches("__iceberg_snapshots", schema, vec![batch])?;
            self.ctx
                .table("__iceberg_snapshots")
                .await
                .map_err(|e| format!("Table reference error: {e}"))
        })
    }

    /// Return the current schema of an Iceberg table as a table:
    /// `field_id, name, type, required`.
    pub fn iceberg_schema(
        &self,
        metadata_location: &str,
        props: Vec<(String, String)>,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let location = normalize_location(metadata_location)?;

        self.rt.block_on(async {
            let static_table = load_static_table(&location, &props).await?;
            let meta = static_table.metadata();
            let schema = meta.current_schema();

            let mut ids = Vec::new();
            let mut names = Vec::new();
            let mut types = Vec::new();
            let mut required = Vec::new();
            for field in schema.as_struct().fields() {
                ids.push(field.id as i64);
                names.push(field.name.clone());
                types.push(format!("{}", field.field_type));
                required.push(field.required);
            }

            let arrow_schema = Arc::new(Schema::new(vec![
                Field::new("field_id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("type", DataType::Utf8, false),
                Field::new("required", DataType::Boolean, false),
            ]));
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(types)),
                Arc::new(BooleanArray::from(required)),
            ];

            let batch = RecordBatch::try_new(arrow_schema.clone(), columns)
                .map_err(|e| format!("Iceberg schema batch error: {e}"))?;
            self.register_batches("__iceberg_schema", arrow_schema, vec![batch])?;
            self.ctx
                .table("__iceberg_schema")
                .await
                .map_err(|e| format!("Table reference error: {e}"))
        })
    }
}

/// Build a `FileIO` for a metadata location, applying object-store props.
fn build_file_io(location: &str, props: &[(String, String)]) -> Result<FileIO, String> {
    let mut builder =
        FileIO::from_path(location).map_err(|e| format!("Iceberg FileIO error: {e}"))?;
    for (k, v) in props {
        builder = builder.with_prop(k, v);
    }
    builder
        .build()
        .map_err(|e| format!("Iceberg FileIO build error: {e}"))
}

/// Load a catalog-less `StaticTable` from a metadata location.
async fn load_static_table(
    location: &str,
    props: &[(String, String)],
) -> Result<StaticTable, String> {
    let file_io = build_file_io(location, props)?;
    let table_name = derive_table_name(location);
    let table_ident = TableIdent::from_strs(["default", &table_name])
        .map_err(|e| format!("Iceberg table ident error: {e}"))?;
    StaticTable::from_metadata_file(location, table_ident, file_io)
        .await
        .map_err(|e| format!("Iceberg metadata read error: {e}"))
}

async fn load_table(location: &str, props: &[(String, String)]) -> Result<Table, String> {
    Ok(load_static_table(location, props).await?.into_table())
}

/// Compact a snapshot's summary properties into a stable `k=v, k=v` string,
/// surfacing the most useful counters first.
fn summarize_props(props: &std::collections::HashMap<String, String>) -> String {
    const PREFERRED: [&str; 4] = [
        "added-records",
        "deleted-records",
        "total-records",
        "added-data-files",
    ];
    let mut parts = Vec::new();
    for key in PREFERRED {
        if let Some(v) = props.get(key) {
            parts.push(format!("{key}={v}"));
        }
    }
    parts.join(", ")
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
    let parts: Vec<&str> = trimmed.split('/').filter(|s| !s.is_empty()).collect();
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
        assert_eq!(derive_table_name("/tmp/loose.metadata.json"), "table");
    }

    #[test]
    fn summary_props_orders_preferred_keys() {
        let mut m = std::collections::HashMap::new();
        m.insert("total-records".to_string(), "11".to_string());
        m.insert("added-records".to_string(), "3".to_string());
        m.insert("spark.app.id".to_string(), "x".to_string());
        // added-records before total-records; unknown keys dropped.
        assert_eq!(summarize_props(&m), "added-records=3, total-records=11");
    }
}
