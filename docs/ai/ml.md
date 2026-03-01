# Machine Learning

TL provides built-in support for model training (via linfa), ONNX inference (via ort), embeddings, and LLM API access.

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

Generate vector embeddings from text:

```tl
let emb = embed(text, model: "default")
```

## LLM API

Call large language model APIs:

```tl
let response = llm(prompt, model: "gpt-4")
```

Configure API keys via environment variables.

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
