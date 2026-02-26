"""Python data benchmarks — using pandas/polars for comparison."""
import time
import csv
import tempfile
import os

def create_test_csv(rows):
    f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    writer = csv.writer(f)
    writer.writerow(['id', 'name', 'value', 'category'])
    for i in range(rows):
        cat = 'A' if i % 3 == 0 else ('B' if i % 3 == 1 else 'C')
        writer.writerow([i, f'item_{i}', i * 1.5, cat])
    f.close()
    return f.name

def bench_pandas_pipeline(csv_path):
    try:
        import pandas as pd
    except ImportError:
        print("pandas not installed, skipping")
        return

    start = time.perf_counter()
    iterations = 5
    for _ in range(iterations):
        df = pd.read_csv(csv_path)
        result = (df[df['value'] > 100.0]
                  [['id', 'name', 'value']]
                  .sort_values('value'))
        count = len(result)
    elapsed = (time.perf_counter() - start) / iterations
    print(f"100k pipeline pandas:   {elapsed*1000:.2f} ms  (rows={count})")

def bench_polars_pipeline(csv_path):
    try:
        import polars as pl
    except ImportError:
        print("polars not installed, skipping")
        return

    start = time.perf_counter()
    iterations = 5
    for _ in range(iterations):
        df = pl.read_csv(csv_path)
        result = (df.filter(pl.col('value') > 100.0)
                  .select(['id', 'name', 'value'])
                  .sort('value'))
        count = len(result)
    elapsed = (time.perf_counter() - start) / iterations
    print(f"100k pipeline polars:   {elapsed*1000:.2f} ms  (rows={count})")

if __name__ == "__main__":
    csv_path = create_test_csv(100_000)
    try:
        bench_pandas_pipeline(csv_path)
        bench_polars_pipeline(csv_path)
    finally:
        os.unlink(csv_path)
