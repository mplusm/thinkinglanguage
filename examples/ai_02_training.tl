// ThinkingLanguage — AI Example 02: Model Training
// Train a linear regression model from CSV data.
//
// Usage: tl run examples/ai_02_training.tl
// Requires: examples/test_data/train.csv (created below)

// First, create training data (y = 2x + 1)
let data = read_csv("examples/test_data/train_linreg.csv")
data |> show(5)

// Train a linear regression model
model linreg = train linear {
    data: data,
    target: "y",
    features: ["x"]
}

print("Model trained:", type_of(linreg))

// Save the model
model_save(linreg, "/tmp/tl_example_linreg")
print("Model saved to /tmp/tl_example_linreg")

// Load it back
let loaded = model_load("/tmp/tl_example_linreg")
print("Model loaded:", type_of(loaded))

// Predict on new data
let input = tensor([11.0, 12.0, 13.0], [3, 1])
let predictions = predict(loaded, input)
print("Predictions for x=11,12,13:", predictions)
