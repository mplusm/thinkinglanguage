# Machine Learning

TL provides built-in support for model training (via linfa), ONNX inference (via ort), embeddings, LLM API access, and [AI agents with tool-use](agents.md).

## Model Training

Train models using the linfa machine learning framework:

```tl
let model = train_model(data, target: "column", algorithm: "random_forest")
```

### Supported Algorithms

- `linear_regression`
- `logistic_regression`
- `random_forest`
- `kmeans`

Models are first-class values in TL -- they can be stored in variables, passed to functions, and saved to the model registry.

## ONNX Inference

Load and run pre-trained ONNX models using the ort runtime:

```tl
let model = load_onnx("model.onnx")
let result = predict(model, input_data)
```

This enables integration with models trained in PyTorch, TensorFlow, scikit-learn, or any framework that exports ONNX.

## Embeddings

Generate vector embeddings from text using the OpenAI embeddings API:

```tl
let emb = embed("Hello, world!")                          // default model
let emb = embed("Hello, world!", "text-embedding-3-small") // explicit model
```

Requires `TL_OPENAI_KEY` environment variable. Returns a tensor (1D vector).

Use `similarity()` to compare embeddings:

```tl
let a = embed("machine learning")
let b = embed("artificial intelligence")
println(similarity(a, b))   // ~0.85 (high similarity)
```

## LLM API

Call large language model APIs directly:

```tl
// Single-shot completion (defaults to claude-sonnet-4-20250514)
let response = ai_complete("Explain quantum computing in one sentence")

// With a specific model
let response = ai_complete("Hello", "gpt-4o-mini")

// Multi-turn chat
let response = ai_chat("gpt-4o", "You are a tutor.", [
    ["user", "What is 2+2?"],
    ["assistant", "4"],
    ["user", "And 3+3?"]
])
```

Configure API keys via environment variables: `TL_OPENAI_KEY`, `TL_ANTHROPIC_KEY`, or `TL_LLM_KEY`.

## AI Agents

For autonomous AI agents with tool-use, multi-turn conversations, and lifecycle hooks, see the [Agent Framework Guide](agents.md).

## Model Registry

Save, list, inspect, and delete models from a local registry:

```tl
register_model("my_model", model)
```

CLI commands for model management:

```sh
tl models list            # list all registered models
tl models info <name>     # show model metadata
tl models delete <name>   # delete a model
```

## Prediction Pipeline

Combine data transforms with model predictions in a single pipe chain:

```tl
let data = read_csv("new_data.csv")
let model = load_onnx("churn_model.onnx")

data
    |> filter(is_active == true)
    |> predict(model, output: "churn_score")
    |> sort(churn_score, "desc")
    |> show()
```

This reads new data, filters for active users, runs the churn prediction model, sorts by predicted score in descending order, and displays the results.
