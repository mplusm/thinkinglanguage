// ThinkingLanguage — Apache Iceberg Connector
//
// Reads an Apache Iceberg table *natively* — no Spark, no Trino, no Python glue.
// The table's `metadata.json` location is passed via the TL_ICEBERG_META env var
// (see examples/iceberg_demo.sh, which generates a sample table and sets it).
//
//   read_iceberg(metadata_location)            // local path or s3:// URL
//   read_iceberg(metadata_location, props_map) // props = FileIO/object-store config
//
// Build/run with the `iceberg` feature:
//   cargo run --features iceberg -- run examples/data_07_iceberg.tl

let meta = env_get("TL_ICEBERG_META")

print("Reading Iceberg table from:")
print("  " + meta)
print("")

// One call: Iceberg metadata + manifests + Parquet data → a DataFusion table.
let orders = read_iceberg(meta)

print("=== Raw Iceberg snapshot ===")
orders.clone() |> show()

print("")
print("=== Revenue & volume by region (DataFusion over Iceberg) ===")
orders
    |> aggregate(by: region, revenue: sum(revenue), units: sum(units))
    |> sort(revenue, "desc")
    |> show()
