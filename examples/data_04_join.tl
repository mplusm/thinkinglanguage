// Data Engine Example 04: Joins
// Join users with orders

let users = read_csv("examples/test_data/users.csv")
let orders = read_csv("examples/test_data/orders.csv")

// Inner join: users with their orders
print("Users with orders:")
users
    |> join(orders, on: id == user_id)
    |> select(name, product, amount, quantity)
    |> show()

// Total spend per user
print("\nTotal spend per user:")
users
    |> join(orders, on: id == user_id)
    |> aggregate(by: name, total_spend: sum(amount), num_orders: count())
    |> sort(total_spend, "desc")
    |> show()
