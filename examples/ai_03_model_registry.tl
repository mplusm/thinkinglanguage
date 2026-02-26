// ThinkingLanguage — AI Example 03: Model Registry
// Demonstrates model registration and listing.
//
// Usage: tl run examples/ai_03_model_registry.tl
// CLI:   tl models list
//        tl models info <name>
//        tl models delete <name>

// List models in registry
let models = model_list()
print("Registered models:", models)

// Train a quick model and register it
let data = read_csv("examples/test_data/train_linreg.csv")
model m = train linear {
    data: data,
    target: "y",
    features: ["x"]
}

model_register("example_linreg", m)
print("Registered 'example_linreg'")

// List again
let models2 = model_list()
print("Models after registration:", models2)

// Retrieve from registry
let retrieved = model_get("example_linreg")
print("Retrieved model type:", type_of(retrieved))
