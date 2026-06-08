// Data Engine Example 01: CSV Basics
// Read, filter, select, sort, and display CSV data

// Read a CSV file into a table
let users = read_csv("examples/test_data/users.csv")

// Show all users
// NOTE: `|>` moves its input, so reused tables are `.clone()`d (the final use need not be).
print("All users:")
users.clone() |> show()

// Filter: users older than 30
print("\nUsers older than 30:")
users.clone() |> filter(age > 30) |> show()

// Select specific columns
print("\nNames and cities:")
users.clone() |> select(name, city) |> show()

// Sort by age descending
print("\nSorted by age (descending):")
users.clone() |> sort(age, "desc") |> show()

// Chain operations: filter, select, sort
print("\nEngineers in New York, sorted by age:")
users
    |> filter(department == "Engineering")
    |> filter(city == "New York")
    |> select(name, age)
    |> sort(age)
    |> show()
