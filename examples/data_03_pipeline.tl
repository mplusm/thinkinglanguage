// Data Engine Example 03: Full Pipeline
// Schema definition, derived columns, aggregation

// Define a schema
schema User {
    id: int64,
    name: string,
    age: int64,
    city: string,
    department: string
}

// Load data
let users = read_csv("examples/test_data/users.csv")

// Add derived columns
// NOTE: `|>` moves its input, so reused tables are `.clone()`d (the final use need not be).
print("Users with age group:")
users.clone()
    |> with { senior = age > 35 }
    |> select(name, age, senior)
    |> show()

// Aggregate: count and average age by department
print("\nDepartment statistics:")
users.clone()
    |> aggregate(by: department, count: count(), avg_age: avg(age))
    |> sort(count, "desc")
    |> show()

// Aggregate by city
print("\nCity statistics:")
users
    |> aggregate(by: city, headcount: count(), youngest: min(age), oldest: max(age))
    |> show()
