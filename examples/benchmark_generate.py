#!/usr/bin/env python3
"""Generate benchmark CSV data for ThinkingLanguage data engine testing."""

import csv
import random
import sys

def generate(num_rows, output_path):
    departments = ["Engineering", "Marketing", "Sales", "Finance", "HR", "Operations"]
    cities = ["New York", "San Francisco", "Chicago", "Austin", "Seattle", "Boston"]

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "age", "city", "department", "salary"])
        for i in range(1, num_rows + 1):
            writer.writerow([
                i,
                f"user_{i}",
                random.randint(22, 65),
                random.choice(cities),
                random.choice(departments),
                round(random.uniform(50000, 200000), 2),
            ])

    print(f"Generated {num_rows} rows to {output_path}")

if __name__ == "__main__":
    num_rows = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    output_path = sys.argv[2] if len(sys.argv) > 2 else "/tmp/tl_benchmark.csv"
    generate(num_rows, output_path)
