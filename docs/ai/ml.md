# Machine Learning

TL provides built-in support for model training (via linfa), ONNX inference (via ort), embeddings, LLM API access, and [AI agents with tool-use](agents.md).

## Model Training

Train models with the declarative `train` statement (backed by linfa):

```tl
model clf = train logistic {
    data: data,
    target: "label",
    features: ["f1", "f2"]
}
let result = predict(clf, input_tensor)
```

### Supported Algorithms

The identifier after `train`:

Regression
- `linear` -- ordinary least squares
- `ridge` -- L2-regularized regression (hyperparam `alpha`, default 1.0)

Classification
- `logistic` -- binary logistic classification
- `tree` (alias `decision_tree`) -- decision-tree classifier (hyperparam `max_depth`)
- `random_forest` -- bagged decision-tree ensemble (hyperparam `n_trees`, default 10)
- `knn` -- k-nearest-neighbors (hyperparam `k`, default 5)
- `naive_bayes` (alias `gaussian_nb`) -- Gaussian Naive Bayes

Boosting (regression **or** binary classification -- task auto-detected from the target)
- `gradient_boosting` (aliases `gbt`, `gbm`, `xgboost`) -- gradient-boosted trees with
  second-order (Newton) leaves, the XGBoost family. Hyperparameters: `n_estimators`
  (default 100), `learning_rate` (0.1), `max_depth` (3). Like all tree models it does
  not extrapolate beyond the training range.

Clustering (unsupervised -- `target` is ignored)
- `kmeans` -- k-means (hyperparam `k`, default 3)
- `dbscan` -- density-based clustering (hyperparams `eps` default 0.5, `min_samples` default 3; predict returns the nearest cluster, or -1 for noise)

Targets must be **numeric** (integer-encoded classes); string labels are rejected.
Models are first-class values -- store them in variables, pass them around, and
save them with `model_save` / load with `model_load`.

## ONNX Inference

Run pre-trained ONNX models (via the ort runtime) by wrapping the model in a
`.tlmodel` directory (a `metadata.json` with `"type": "onnx"` next to `model.onnx`)
and loading it with `model_load`:

```tl
let model = model_load("model.tlmodel")
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
let model = model_load("churn_model.tlmodel")

data
    |> filter(is_active == true)
    |> predict(model, output: "churn_score")
    |> sort(churn_score, "desc")
    |> show()
```

This reads new data, filters for active users, runs the churn prediction model, sorts by predicted score in descending order, and displays the results.
