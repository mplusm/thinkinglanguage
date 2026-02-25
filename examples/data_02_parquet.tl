// Data Engine Example 02: Parquet I/O
// Read CSV, write to Parquet, read back

let users = read_csv("examples/test_data/users.csv")

// Write to Parquet
users |> write_parquet("/tmp/tl_users_parquet")
print("Written to Parquet")

// Read back from Parquet
let from_parquet = read_parquet("/tmp/tl_users_parquet")
print("\nRead back from Parquet:")
from_parquet |> show()

// Describe schema
print("\nSchema:")
from_parquet |> describe()
