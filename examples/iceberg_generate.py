#!/usr/bin/env python3
"""Generate a small Apache Iceberg table for the ThinkingLanguage demo.

Creates a sqlite-backed Iceberg catalog + a `sales.orders` table with two
snapshots, then prints the latest `metadata.json` path on the last line so a
shell driver can capture it (see iceberg_demo.sh).

    python3 iceberg_generate.py [output_root]

Requires: pyiceberg[sql-sqlite], pyarrow
"""
import os
import sys
import glob
import shutil

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

root = sys.argv[1] if len(sys.argv) > 1 else "/tmp/iceberg_demo"
warehouse = os.path.join(root, "warehouse")
shutil.rmtree(root, ignore_errors=True)
os.makedirs(warehouse, exist_ok=True)

catalog = SqlCatalog(
    "demo",
    uri=f"sqlite:///{os.path.join(root, 'catalog.db')}",
    warehouse=f"file://{warehouse}",
)
catalog.create_namespace("sales")

data = pa.table({
    "region":  ["NA", "EMEA", "APAC", "NA", "EMEA", "APAC", "NA", "LATAM"],
    "product": ["Widget A", "Gadget X", "Pro Suite", "Widget A",
                "Pro Suite", "Gadget X", "Pro Suite", "Widget A"],
    "units":   pa.array([1240, 2100, 560, 880, 340, 1500, 720, 410], pa.int64()),
    "revenue": pa.array([62000, 105000, 168000, 44000, 102000, 75000, 216000, 20500], pa.int64()),
})

table = catalog.create_table("sales.orders", schema=data.schema)
table.append(data)                 # snapshot 1
table.append(data.slice(0, 3))     # snapshot 2 — proves snapshot selection

meta = sorted(glob.glob(os.path.join(warehouse, "sales", "orders", "metadata", "*.metadata.json")))
print(meta[-1])
