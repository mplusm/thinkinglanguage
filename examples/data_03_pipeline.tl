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
print("Users with age group:")
users
    |> with { senior = age > 35 }
    |> select(name, age, senior)
    |> show()

// Aggregate: count and average age by department
print("\nDepartment statistics:")
users
    |> aggregate(by: department, count: count(), avg_age: avg(age))
    |> sort(count, "desc")
    |> show()

// Aggregate by city
print("\nCity statistics:")
users
    |> aggregate(by: city, headcount: count(), youngest: min(age), oldest: max(age))
    |> show()
