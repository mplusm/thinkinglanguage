// ThinkingLanguage — Training dispatcher
// Uses linfa for pure-Rust ML training.

use std::collections::{HashMap, HashSet};

use linfa::Dataset;
use linfa::prelude::*;
use ndarray::{Array1, Array2, Axis};

use crate::model::{LinfaKind, ModelMeta, TlModel};
use crate::tensor::TlTensor;

/// Training configuration extracted from TL source.
pub struct TrainConfig {
    /// Feature data (2D: samples x features).
    pub features: TlTensor,
    /// Target data (1D: samples).
    pub target: TlTensor,
    /// Feature column names.
    pub feature_names: Vec<String>,
    /// Target column name.
    pub target_name: String,
    /// Model name.
    pub model_name: String,
    /// Train/test split ratio (0.0 to 1.0, fraction for training).
    pub split_ratio: f64,
    /// Hyperparameters.
    pub hyperparams: HashMap<String, f64>,
}

/// Train a model using the specified algorithm.
pub fn train(algorithm: &str, config: &TrainConfig) -> Result<TlModel, String> {
    match algorithm {
        "linear" => train_linear(config),
        "logistic" => train_logistic(config),
        "tree" | "decision_tree" => train_decision_tree(config),
        "random_forest" | "forest" => train_random_forest(config),
        "kmeans" | "k_means" => train_kmeans(config),
        "knn" | "k_nearest_neighbors" => train_knn(config),
        "naive_bayes" | "gaussian_nb" | "nb" => train_naive_bayes(config),
        "dbscan" => train_dbscan(config),
        "ridge" => train_ridge(config),
        "gradient_boosting" | "gbt" | "gbm" | "xgboost" => train_gradient_boosting(config),
        _ => Err(format!(
            "Unknown training algorithm: '{algorithm}'. Supported: linear, ridge, logistic, \
             tree, random_forest, gradient_boosting, knn, naive_bayes, kmeans, dbscan"
        )),
    }
}

// ---- shared helpers --------------------------------------------------------

/// Apply a per-row prediction closure over a 1D (single sample) or 2D
/// (samples × features) input tensor, returning a 1D tensor of predictions.
fn apply_rowwise<P: Fn(&[f64]) -> f64>(input: &TlTensor, predict_row: P) -> Result<TlTensor, String> {
    let shape = input.shape();
    let flat = input.to_vec();
    if shape.len() == 1 {
        Ok(TlTensor::from_list(vec![predict_row(&flat)]))
    } else if shape.len() == 2 {
        let (rows, cols) = (shape[0], shape[1]);
        let mut preds = Vec::with_capacity(rows);
        for i in 0..rows {
            preds.push(predict_row(&flat[i * cols..(i + 1) * cols]));
        }
        Ok(TlTensor::from_list(preds))
    } else {
        Err(format!("Input must be 1D or 2D, got {}D", shape.len()))
    }
}

/// Serialize a fitted linfa decision tree into a self-contained JSON node so it
/// can be reloaded and used for inference (linfa's tree isn't serde-serializable).
fn tree_node_to_json(node: &linfa_trees::TreeNode<f64, usize>) -> serde_json::Value {
    if node.is_leaf() {
        serde_json::json!({ "leaf": true, "value": node.prediction().unwrap_or(0) })
    } else {
        let (feature, threshold, _) = node.split();
        let children = node.children(); // [left, right]
        let left = children[0].as_ref().map(|c| tree_node_to_json(c)).unwrap_or(serde_json::Value::Null);
        let right = children[1].as_ref().map(|c| tree_node_to_json(c)).unwrap_or(serde_json::Value::Null);
        serde_json::json!({ "leaf": false, "feature": feature, "threshold": threshold, "left": left, "right": right })
    }
}

/// Traverse a serialized tree (see `tree_node_to_json`) for one feature row.
/// Matches linfa's split rule: `x[feature] < threshold` goes left, else right.
fn predict_tree_json(node: &serde_json::Value, row: &[f64]) -> f64 {
    if node["leaf"].as_bool().unwrap_or(true) {
        return node["value"].as_f64().unwrap_or(0.0);
    }
    let f = node["feature"].as_u64().unwrap_or(0) as usize;
    let thr = node["threshold"].as_f64().unwrap_or(0.0);
    let xv = row.get(f).copied().unwrap_or(0.0);
    if xv < thr { predict_tree_json(&node["left"], row) } else { predict_tree_json(&node["right"], row) }
}

/// Majority-vote class over a set of serialized trees (used by random forest;
/// a single decision tree is the one-tree case).
fn vote_trees(trees: &[serde_json::Value], row: &[f64]) -> f64 {
    let mut counts: HashMap<i64, usize> = HashMap::new();
    for t in trees {
        *counts.entry(predict_tree_json(t, row) as i64).or_insert(0) += 1;
    }
    counts.into_iter().max_by_key(|(_, c)| *c).map(|(v, _)| v as f64).unwrap_or(0.0)
}

fn features_to_array2(features: &TlTensor) -> Result<Array2<f64>, String> {
    let shape = features.shape();
    if shape.len() != 2 {
        return Err(format!("Features must be 2D, got {}D", shape.len()));
    }
    let rows = shape[0];
    let cols = shape[1];
    let flat = features.to_vec();
    Array2::from_shape_vec((rows, cols), flat).map_err(|e| format!("Shape error: {e}"))
}

fn target_to_array1(target: &TlTensor) -> Result<Array1<f64>, String> {
    let shape = target.shape();
    if shape.len() != 1 {
        return Err(format!("Target must be 1D, got {}D", shape.len()));
    }
    Ok(Array1::from_vec(target.to_vec()))
}

fn train_linear(config: &TrainConfig) -> Result<TlModel, String> {
    let x = features_to_array2(&config.features)?;
    let y = target_to_array1(&config.target)?;
    let dataset = Dataset::new(x, y);

    let model = linfa_linear::LinearRegression::default()
        .fit(&dataset)
        .map_err(|e| format!("Linear regression training failed: {e}"))?;

    // Compute R² on training data
    let pred = model.predict(&dataset);
    let r2 = pred
        .r2(&dataset)
        .map_err(|e| format!("R² computation failed: {e}"))?;

    // Serialize model params
    let params = model.params();
    let intercept = model.intercept();
    let model_data = serde_json::json!({
        "params": params.as_slice().unwrap_or(&[]),
        "intercept": intercept,
    });
    let data = serde_json::to_vec(&model_data).map_err(|e| format!("Serialization failed: {e}"))?;

    let mut metrics = HashMap::new();
    metrics.insert("r2".to_string(), r2);

    Ok(TlModel::Linfa {
        kind: LinfaKind::LinearRegression,
        data,
        metadata: ModelMeta {
            name: config.model_name.clone(),
            version: "0.1.0".to_string(),
            created_at: String::new(),
            features: config.feature_names.clone(),
            target: config.target_name.clone(),
            metrics,
        },
    })
}

fn train_logistic(config: &TrainConfig) -> Result<TlModel, String> {
    let x = features_to_array2(&config.features)?;
    let y_float = target_to_array1(&config.target)?;

    // Convert targets to bool for binary classification
    let y_bool: Array1<bool> = y_float.mapv(|v| v > 0.5);

    let dataset = Dataset::new(x, y_bool);

    let model = linfa_logistic::LogisticRegression::default()
        .max_iterations(100)
        .fit(&dataset)
        .map_err(|e| format!("Logistic regression training failed: {e}"))?;

    // Compute accuracy
    let pred = model.predict(&dataset);
    let correct = pred
        .iter()
        .zip(dataset.targets().iter())
        .filter(|(p, t)| p == t)
        .count();
    let accuracy = correct as f64 / dataset.targets().len() as f64;

    // Serialize model params
    let params = model.params();
    let intercept = model.intercept();
    let params_slice = params.as_slice().unwrap_or(&[]);

    // Map the decision function (sigmoid(x·w + b) > 0.5) back to the original
    // class labels. linfa's internal class ordering does NOT guarantee that the
    // "1" class sits on the positive side, so derive the mapping from linfa's own
    // predictions instead of hard-coding 0/1 — this is what caused inverted labels.
    let (mut pos_label, mut neg_label) = (1.0_f64, 0.0_f64);
    {
        let records = dataset.records();
        for (i, p) in pred.iter().enumerate() {
            let row = records.row(i);
            let logit: f64 =
                row.iter().zip(params_slice.iter()).map(|(a, b)| a * b).sum::<f64>() + intercept;
            let label = if *p { 1.0 } else { 0.0 };
            if logit > 0.0 { pos_label = label; } else { neg_label = label; }
        }
    }

    let model_data = serde_json::json!({
        "params": params_slice,
        "intercept": intercept,
        "pos_label": pos_label,
        "neg_label": neg_label,
    });
    let data = serde_json::to_vec(&model_data).map_err(|e| format!("Serialization failed: {e}"))?;

    let mut metrics = HashMap::new();
    metrics.insert("accuracy".to_string(), accuracy);

    Ok(TlModel::Linfa {
        kind: LinfaKind::LogisticRegression,
        data,
        metadata: ModelMeta {
            name: config.model_name.clone(),
            version: "0.1.0".to_string(),
            created_at: String::new(),
            features: config.feature_names.clone(),
            target: config.target_name.clone(),
            metrics,
        },
    })
}

fn train_decision_tree(config: &TrainConfig) -> Result<TlModel, String> {
    let x = features_to_array2(&config.features)?;
    let y_float = target_to_array1(&config.target)?;

    // Convert targets to usize for classification
    let y_usize: Array1<usize> = y_float.mapv(|v| v as usize);

    let max_depth = config
        .hyperparams
        .get("max_depth")
        .copied()
        .map(|d| d as usize);

    let dataset = Dataset::new(x, y_usize);

    let mut builder = linfa_trees::DecisionTree::params();
    if let Some(depth) = max_depth {
        builder = builder.max_depth(Some(depth));
    }
    let model = builder
        .fit(&dataset)
        .map_err(|e| format!("Decision tree training failed: {e}"))?;

    // Compute accuracy
    let pred = model.predict(&dataset);
    let correct = pred
        .iter()
        .zip(dataset.targets().iter())
        .filter(|(p, t)| p == t)
        .count();
    let accuracy = correct as f64 / dataset.targets().len() as f64;

    // Serialize the full tree structure so inference works after reload.
    let model_data = serde_json::json!({
        "type": "decision_tree",
        "accuracy": accuracy,
        "tree": tree_node_to_json(model.root_node()),
    });
    let data = serde_json::to_vec(&model_data).map_err(|e| format!("Serialization failed: {e}"))?;

    let mut metrics = HashMap::new();
    metrics.insert("accuracy".to_string(), accuracy);

    Ok(TlModel::Linfa {
        kind: LinfaKind::DecisionTree,
        data,
        metadata: ModelMeta {
            name: config.model_name.clone(),
            version: "0.1.0".to_string(),
            created_at: String::new(),
            features: config.feature_names.clone(),
            target: config.target_name.clone(),
            metrics,
        },
    })
}

/// Random forest: bootstrap-aggregated ensemble of linfa decision trees.
/// Predicts by majority vote. Hyperparameters: `n_trees` (default 10),
/// `max_depth` (optional).
fn train_random_forest(config: &TrainConfig) -> Result<TlModel, String> {
    let x = features_to_array2(&config.features)?;
    let y_float = target_to_array1(&config.target)?;
    let y_usize: Array1<usize> = y_float.mapv(|v| v as usize);

    let n = x.nrows();
    if n == 0 {
        return Err("Random forest: no training samples".to_string());
    }
    let n_trees = config
        .hyperparams
        .get("n_trees")
        .or_else(|| config.hyperparams.get("trees"))
        .copied()
        .map(|v| (v as usize).max(1))
        .unwrap_or(10);
    let max_depth = config.hyperparams.get("max_depth").copied().map(|d| d as usize);

    // Deterministic xorshift RNG for bootstrap sampling (no extra dependency).
    let mut seed: u64 = 0x2545F4914F6CDD1D;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };

    let mut trees: Vec<serde_json::Value> = Vec::with_capacity(n_trees);
    for _ in 0..n_trees {
        let rows: Vec<usize> = (0..n).map(|_| (next() as usize) % n).collect();
        let xb = x.select(Axis(0), &rows);
        let yb = y_usize.select(Axis(0), &rows);
        let ds = Dataset::new(xb, yb);
        let mut builder = linfa_trees::DecisionTree::params();
        if let Some(d) = max_depth {
            builder = builder.max_depth(Some(d));
        }
        let tree = builder
            .fit(&ds)
            .map_err(|e| format!("Random forest tree training failed: {e}"))?;
        trees.push(tree_node_to_json(tree.root_node()));
    }

    // Training accuracy via majority vote.
    let flat = x.iter().copied().collect::<Vec<f64>>();
    let cols = x.ncols();
    let mut correct = 0usize;
    for i in 0..n {
        let row = &flat[i * cols..(i + 1) * cols];
        if vote_trees(&trees, row) as usize == y_usize[i] {
            correct += 1;
        }
    }
    let accuracy = correct as f64 / n as f64;

    let model_data = serde_json::json!({ "type": "random_forest", "trees": trees });
    let data = serde_json::to_vec(&model_data).map_err(|e| format!("Serialization failed: {e}"))?;

    let mut metrics = HashMap::new();
    metrics.insert("accuracy".to_string(), accuracy);
    metrics.insert("n_trees".to_string(), n_trees as f64);

    Ok(TlModel::Linfa {
        kind: LinfaKind::RandomForest,
        data,
        metadata: ModelMeta {
            name: config.model_name.clone(),
            version: "0.1.0".to_string(),
            created_at: String::new(),
            features: config.feature_names.clone(),
            target: config.target_name.clone(),
            metrics,
        },
    })
}

/// K-means clustering (Lloyd's algorithm, pure Rust — unsupervised, the target
/// column is ignored). Hyperparameters: `k` (default 3), `max_iter` (default
/// 100). Predict returns the nearest-centroid cluster index per row.
fn train_kmeans(config: &TrainConfig) -> Result<TlModel, String> {
    let x = features_to_array2(&config.features)?;
    let n = x.nrows();
    let d = x.ncols();
    if n == 0 {
        return Err("K-means: no training samples".to_string());
    }
    let k = config
        .hyperparams
        .get("k")
        .or_else(|| config.hyperparams.get("clusters"))
        .copied()
        .map(|v| (v as usize).max(1))
        .unwrap_or(3)
        .min(n);
    let max_iter = config
        .hyperparams
        .get("max_iter")
        .copied()
        .map(|v| (v as usize).max(1))
        .unwrap_or(100);

    // Deterministic init: evenly spaced rows as initial centroids.
    let mut centroids: Vec<Vec<f64>> = (0..k).map(|i| x.row((i * n) / k).to_vec()).collect();
    let mut assign = vec![0usize; n];

    for _ in 0..max_iter {
        let mut changed = false;
        for i in 0..n {
            let row = x.row(i);
            let mut best = 0usize;
            let mut best_d = f64::INFINITY;
            for (c, cen) in centroids.iter().enumerate() {
                let dist: f64 = row.iter().zip(cen).map(|(a, b)| (a - b) * (a - b)).sum();
                if dist < best_d {
                    best_d = dist;
                    best = c;
                }
            }
            if assign[i] != best {
                assign[i] = best;
                changed = true;
            }
        }
        let mut sums = vec![vec![0.0f64; d]; k];
        let mut counts = vec![0usize; k];
        for i in 0..n {
            let row = x.row(i);
            counts[assign[i]] += 1;
            for j in 0..d {
                sums[assign[i]][j] += row[j];
            }
        }
        for c in 0..k {
            if counts[c] > 0 {
                for j in 0..d {
                    centroids[c][j] = sums[c][j] / counts[c] as f64;
                }
            }
        }
        if !changed {
            break;
        }
    }

    // Inertia (within-cluster sum of squares) as a quality metric.
    let mut inertia = 0.0f64;
    for i in 0..n {
        let row = x.row(i);
        let cen = &centroids[assign[i]];
        inertia += row.iter().zip(cen).map(|(a, b)| (a - b) * (a - b)).sum::<f64>();
    }

    let model_data = serde_json::json!({ "type": "kmeans", "centroids": centroids });
    let data = serde_json::to_vec(&model_data).map_err(|e| format!("Serialization failed: {e}"))?;

    let mut metrics = HashMap::new();
    metrics.insert("k".to_string(), k as f64);
    metrics.insert("inertia".to_string(), inertia);

    Ok(TlModel::Linfa {
        kind: LinfaKind::KMeans,
        data,
        metadata: ModelMeta {
            name: config.model_name.clone(),
            version: "0.1.0".to_string(),
            created_at: String::new(),
            features: config.feature_names.clone(),
            target: config.target_name.clone(),
            metrics,
        },
    })
}

/// Squared Euclidean distance between two equal-length rows.
fn sq_dist(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// k-NN majority vote of `ytrain` over the `k` nearest rows of `xtrain`.
fn knn_vote(xtrain: &[Vec<f64>], ytrain: &[f64], k: usize, row: &[f64]) -> f64 {
    let mut dists: Vec<(f64, f64)> =
        xtrain.iter().zip(ytrain).map(|(p, &l)| (sq_dist(p, row), l)).collect();
    dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let mut counts: HashMap<i64, usize> = HashMap::new();
    for (_, l) in dists.iter().take(k.min(dists.len())) {
        *counts.entry(*l as i64).or_insert(0) += 1;
    }
    counts.into_iter().max_by_key(|(_, c)| *c).map(|(v, _)| v as f64).unwrap_or(0.0)
}

/// Solve A·x = b (n×n) by Gauss-Jordan elimination with partial pivoting.
/// Returns None if the matrix is (numerically) singular.
fn solve_linear_system(mut a: Vec<Vec<f64>>, mut b: Vec<f64>) -> Option<Vec<f64>> {
    let n = b.len();
    for col in 0..n {
        let mut piv = col;
        for r in (col + 1)..n {
            if a[r][col].abs() > a[piv][col].abs() {
                piv = r;
            }
        }
        if a[piv][col].abs() < 1e-12 {
            return None;
        }
        a.swap(col, piv);
        b.swap(col, piv);
        let d = a[col][col];
        for j in 0..n {
            a[col][j] /= d;
        }
        b[col] /= d;
        for r in 0..n {
            if r != col {
                let f = a[r][col];
                if f != 0.0 {
                    for j in 0..n {
                        a[r][j] -= f * a[col][j];
                    }
                    b[r] -= f * b[col];
                }
            }
        }
    }
    Some(b)
}

/// k-nearest-neighbors classifier. Stores the training set; predicts by majority
/// vote of the `k` (default 5) nearest rows.
fn train_knn(config: &TrainConfig) -> Result<TlModel, String> {
    let x = features_to_array2(&config.features)?;
    let y = target_to_array1(&config.target)?;
    let k = config
        .hyperparams
        .get("k")
        .or_else(|| config.hyperparams.get("neighbors"))
        .copied()
        .map(|v| (v as usize).max(1))
        .unwrap_or(5);
    let rows: Vec<Vec<f64>> = (0..x.nrows()).map(|i| x.row(i).to_vec()).collect();
    let labels: Vec<f64> = y.to_vec();

    let mut correct = 0usize;
    for i in 0..rows.len() {
        if knn_vote(&rows, &labels, k, &rows[i]) == labels[i] {
            correct += 1;
        }
    }
    let accuracy = if rows.is_empty() { 0.0 } else { correct as f64 / rows.len() as f64 };

    let model_data = serde_json::json!({ "type": "knn", "k": k, "x": rows, "y": labels });
    let data = serde_json::to_vec(&model_data).map_err(|e| format!("Serialization failed: {e}"))?;
    let mut metrics = HashMap::new();
    metrics.insert("accuracy".to_string(), accuracy);
    metrics.insert("k".to_string(), k as f64);
    Ok(linfa_model(LinfaKind::Knn, data, config, metrics))
}

/// Gaussian Naive Bayes classifier. Stores per-class priors and per-feature
/// mean/variance; predicts the maximum-a-posteriori class.
fn train_naive_bayes(config: &TrainConfig) -> Result<TlModel, String> {
    let x = features_to_array2(&config.features)?;
    let y = target_to_array1(&config.target)?;
    let n = x.nrows();
    let d = x.ncols();
    if n == 0 {
        return Err("Naive Bayes: no training samples".to_string());
    }
    // Group row indices by class label.
    let mut by_class: HashMap<i64, Vec<usize>> = HashMap::new();
    for i in 0..n {
        by_class.entry(y[i] as i64).or_default().push(i);
    }
    let mut classes: Vec<serde_json::Value> = Vec::new();
    for (label, idxs) in &by_class {
        let cnt = idxs.len();
        let mut means = vec![0.0f64; d];
        for &i in idxs {
            let row = x.row(i);
            for j in 0..d {
                means[j] += row[j];
            }
        }
        for m in &mut means {
            *m /= cnt as f64;
        }
        let mut vars = vec![0.0f64; d];
        for &i in idxs {
            let row = x.row(i);
            for j in 0..d {
                vars[j] += (row[j] - means[j]).powi(2);
            }
        }
        for v in &mut vars {
            *v = (*v / cnt as f64).max(1e-9); // floor to avoid div-by-zero
        }
        classes.push(serde_json::json!({
            "label": *label as f64,
            "prior": (cnt as f64 / n as f64).ln(),
            "means": means,
            "vars": vars,
        }));
    }

    // Training accuracy.
    let nb = NaiveBayesModel::from_json(&classes);
    let correct = (0..n).filter(|&i| nb.predict(&x.row(i).to_vec()) == y[i].round()).count();
    let accuracy = correct as f64 / n as f64;

    let model_data = serde_json::json!({ "type": "naive_bayes", "classes": classes });
    let data = serde_json::to_vec(&model_data).map_err(|e| format!("Serialization failed: {e}"))?;
    let mut metrics = HashMap::new();
    metrics.insert("accuracy".to_string(), accuracy);
    metrics.insert("classes".to_string(), by_class.len() as f64);
    Ok(linfa_model(LinfaKind::NaiveBayes, data, config, metrics))
}

/// Parsed Gaussian-NB model used for scoring (train accuracy + inference).
struct NaiveBayesModel {
    classes: Vec<(f64, f64, Vec<f64>, Vec<f64>)>, // (label, log_prior, means, vars)
}
impl NaiveBayesModel {
    fn from_json(classes: &[serde_json::Value]) -> Self {
        let classes = classes
            .iter()
            .map(|c| {
                let label = c["label"].as_f64().unwrap_or(0.0);
                let prior = c["prior"].as_f64().unwrap_or(0.0);
                let means: Vec<f64> = serde_json::from_value(c["means"].clone()).unwrap_or_default();
                let vars: Vec<f64> = serde_json::from_value(c["vars"].clone()).unwrap_or_default();
                (label, prior, means, vars)
            })
            .collect();
        Self { classes }
    }
    fn predict(&self, row: &[f64]) -> f64 {
        let mut best_label = 0.0;
        let mut best_score = f64::NEG_INFINITY;
        for (label, log_prior, means, vars) in &self.classes {
            let mut score = *log_prior;
            for j in 0..row.len().min(means.len()) {
                let v = vars[j].max(1e-9);
                score += -0.5 * ((row[j] - means[j]).powi(2) / v + (2.0 * std::f64::consts::PI * v).ln());
            }
            if score > best_score {
                best_score = score;
                best_label = *label;
            }
        }
        best_label
    }
}

/// DBSCAN density clustering (unsupervised; `target` ignored). Hyperparameters:
/// `eps` (neighborhood radius, default 0.5) and `min_samples` (default 3).
/// Predict assigns a new point to the cluster of the nearest core point within
/// `eps`, or -1 (noise) if none.
fn train_dbscan(config: &TrainConfig) -> Result<TlModel, String> {
    let x = features_to_array2(&config.features)?;
    let n = x.nrows();
    if n == 0 {
        return Err("DBSCAN: no training samples".to_string());
    }
    let pts: Vec<Vec<f64>> = (0..n).map(|i| x.row(i).to_vec()).collect();
    let eps = config.hyperparams.get("eps").copied().unwrap_or(0.5);
    let min_samples = config
        .hyperparams
        .get("min_samples")
        .or_else(|| config.hyperparams.get("min_points"))
        .copied()
        .map(|v| (v as usize).max(1))
        .unwrap_or(3);
    let eps2 = eps * eps;
    let neighbors = |i: usize| -> Vec<usize> { (0..n).filter(|&j| sq_dist(&pts[i], &pts[j]) <= eps2).collect() };

    let mut labels = vec![-1i64; n];
    let mut visited = vec![false; n];
    let mut cid = 0i64;
    for i in 0..n {
        if visited[i] {
            continue;
        }
        visited[i] = true;
        let nb = neighbors(i);
        if nb.len() < min_samples {
            continue; // provisional noise (may be absorbed as a border point)
        }
        labels[i] = cid;
        let mut queue = nb;
        let mut qi = 0;
        while qi < queue.len() {
            let q = queue[qi];
            qi += 1;
            if labels[q] < 0 {
                labels[q] = cid;
            }
            if !visited[q] {
                visited[q] = true;
                let qnb = neighbors(q);
                if qnb.len() >= min_samples {
                    for m in qnb {
                        if !queue.contains(&m) {
                            queue.push(m);
                        }
                    }
                }
            }
        }
        cid += 1;
    }

    let mut cores: Vec<serde_json::Value> = Vec::new();
    let mut n_noise = 0usize;
    for i in 0..n {
        if labels[i] < 0 {
            n_noise += 1;
        } else if neighbors(i).len() >= min_samples {
            cores.push(serde_json::json!({ "p": pts[i], "c": labels[i] as f64 }));
        }
    }

    let model_data = serde_json::json!({ "type": "dbscan", "eps": eps, "cores": cores });
    let data = serde_json::to_vec(&model_data).map_err(|e| format!("Serialization failed: {e}"))?;
    let mut metrics = HashMap::new();
    metrics.insert("clusters".to_string(), cid as f64);
    metrics.insert("noise".to_string(), n_noise as f64);
    Ok(linfa_model(LinfaKind::Dbscan, data, config, metrics))
}

/// Ridge regression (L2-regularized least squares) solved via the normal
/// equations. Hyperparameter `alpha` (a.k.a. `lambda`, default 1.0); the
/// intercept is not regularized.
fn train_ridge(config: &TrainConfig) -> Result<TlModel, String> {
    let x = features_to_array2(&config.features)?;
    let y = target_to_array1(&config.target)?;
    let n = x.nrows();
    let d = x.ncols();
    if n == 0 {
        return Err("Ridge: no training samples".to_string());
    }
    let lambda = config
        .hyperparams
        .get("alpha")
        .or_else(|| config.hyperparams.get("lambda"))
        .copied()
        .unwrap_or(1.0);

    let p = d + 1; // trailing intercept column
    let row_aug = |i: usize| -> Vec<f64> {
        let mut r = x.row(i).to_vec();
        r.push(1.0);
        r
    };
    let mut a = vec![vec![0.0f64; p]; p];
    let mut bvec = vec![0.0f64; p];
    for i in 0..n {
        let r = row_aug(i);
        let yi = y[i];
        for j in 0..p {
            for k2 in 0..p {
                a[j][k2] += r[j] * r[k2];
            }
            bvec[j] += r[j] * yi;
        }
    }
    for j in 0..d {
        a[j][j] += lambda; // regularize coefficients, not the intercept (index d)
    }
    let w = solve_linear_system(a, bvec)
        .ok_or("Ridge: singular system — try a larger alpha or fewer collinear features")?;
    let coef: Vec<f64> = w[0..d].to_vec();
    let intercept = w[d];

    // R² on the training data.
    let mean_y = y.iter().sum::<f64>() / n as f64;
    let (mut ss_res, mut ss_tot) = (0.0, 0.0);
    for i in 0..n {
        let row = x.row(i);
        let pred: f64 = row.iter().zip(&coef).map(|(a, b)| a * b).sum::<f64>() + intercept;
        ss_res += (y[i] - pred).powi(2);
        ss_tot += (y[i] - mean_y).powi(2);
    }
    let r2 = if ss_tot > 0.0 { 1.0 - ss_res / ss_tot } else { 0.0 };

    let model_data = serde_json::json!({ "type": "ridge", "params": coef, "intercept": intercept });
    let data = serde_json::to_vec(&model_data).map_err(|e| format!("Serialization failed: {e}"))?;
    let mut metrics = HashMap::new();
    metrics.insert("r2".to_string(), r2);
    Ok(linfa_model(LinfaKind::Ridge, data, config, metrics))
}

/// Build a weighted CART **regression** tree over sample indices `idx`, fitting
/// targets `r` with weights `w` by minimizing weighted SSE. Emits the same JSON
/// node format as the decision-tree builder, so `predict_tree_json` traverses it.
/// (linfa-trees is classifier-only, so gradient boosting needs this.)
fn build_reg_tree(
    idx: &[usize],
    x: &Array2<f64>,
    r: &[f64],
    w: &[f64],
    depth: usize,
    max_depth: usize,
    min_leaf: usize,
) -> serde_json::Value {
    let (mut sw, mut swr, mut swr2) = (0.0f64, 0.0f64, 0.0f64);
    for &i in idx {
        sw += w[i];
        swr += w[i] * r[i];
        swr2 += w[i] * r[i] * r[i];
    }
    let leaf_val = if sw > 0.0 { swr / sw } else { 0.0 };
    let leaf = serde_json::json!({ "leaf": true, "value": leaf_val });
    if depth >= max_depth || idx.len() <= min_leaf.max(1) || sw <= 0.0 {
        return leaf;
    }
    let parent_sse = swr2 - swr * swr / sw;

    let d = x.ncols();
    let mut best: Option<(usize, f64, f64)> = None; // (feature, threshold, sse)
    for f in 0..d {
        let mut order: Vec<usize> = idx.to_vec();
        order.sort_by(|&a, &b| x[[a, f]].partial_cmp(&x[[b, f]]).unwrap_or(std::cmp::Ordering::Equal));
        let (mut lw, mut lwr, mut lwr2) = (0.0f64, 0.0f64, 0.0f64);
        for k in 0..order.len() - 1 {
            let i = order[k];
            lw += w[i];
            lwr += w[i] * r[i];
            lwr2 += w[i] * r[i] * r[i];
            let (xi, xnext) = (x[[order[k], f]], x[[order[k + 1], f]]);
            if xi == xnext {
                continue;
            }
            let rw = sw - lw;
            if lw <= 0.0 || rw <= 0.0 {
                continue;
            }
            let sse_l = lwr2 - lwr * lwr / lw;
            let sse_r = (swr2 - lwr2) - (swr - lwr) * (swr - lwr) / rw;
            let sse = sse_l + sse_r;
            if best.map_or(true, |(_, _, bs)| sse < bs) {
                best = Some((f, (xi + xnext) / 2.0, sse));
            }
        }
    }

    match best {
        Some((f, thr, sse)) if sse < parent_sse - 1e-12 => {
            let left: Vec<usize> = idx.iter().copied().filter(|&i| x[[i, f]] < thr).collect();
            let right: Vec<usize> = idx.iter().copied().filter(|&i| x[[i, f]] >= thr).collect();
            if left.is_empty() || right.is_empty() {
                return leaf;
            }
            serde_json::json!({
                "leaf": false, "feature": f, "threshold": thr,
                "left": build_reg_tree(&left, x, r, w, depth + 1, max_depth, min_leaf),
                "right": build_reg_tree(&right, x, r, w, depth + 1, max_depth, min_leaf),
            })
        }
        _ => leaf,
    }
}

/// Gradient-boosted decision trees (the XGBoost family). Fits shallow regression
/// trees to the gradient/Hessian of the loss (second-order / Newton leaves, like
/// XGBoost). Auto-detects the task: ≤2 distinct 0/1 targets ⇒ binary logistic
/// classification, otherwise squared-error regression (override with hyperparam
/// `objective`: 1 = binary, 0 = regression). Hyperparameters: `n_estimators`
/// (default 100), `learning_rate` (0.1), `max_depth` (3), `min_leaf` (1).
fn train_gradient_boosting(config: &TrainConfig) -> Result<TlModel, String> {
    let x = features_to_array2(&config.features)?;
    let y = target_to_array1(&config.target)?;
    let n = x.nrows();
    if n == 0 {
        return Err("Gradient boosting: no training samples".to_string());
    }
    let hp_usize = |a: &str, b: &str, def: usize| -> usize {
        config.hyperparams.get(a).or_else(|| config.hyperparams.get(b)).copied().map(|v| (v as usize).max(1)).unwrap_or(def)
    };
    let n_est = hp_usize("n_estimators", "trees", 100);
    let max_depth = hp_usize("max_depth", "depth", 3);
    let min_leaf = hp_usize("min_leaf", "min_samples_leaf", 1);
    let lr = config.hyperparams.get("learning_rate").or_else(|| config.hyperparams.get("eta")).copied().unwrap_or(0.1);

    let all01 = y.iter().all(|v| *v == 0.0 || *v == 1.0);
    let distinct: HashSet<i64> = y.iter().map(|v| *v as i64).collect();
    let binary = match config.hyperparams.get("objective") {
        Some(o) => *o > 0.5,
        None => all01 && distinct.len() <= 2,
    };
    if binary && !all01 {
        return Err("Gradient boosting (binary objective) requires 0/1 targets".to_string());
    }

    // Initial raw score: mean (regression) or log-odds (classification).
    let init = if binary {
        let pos = y.iter().filter(|&&v| v == 1.0).count() as f64;
        let p = (pos / n as f64).clamp(1e-6, 1.0 - 1e-6);
        (p / (1.0 - p)).ln()
    } else {
        y.iter().sum::<f64>() / n as f64
    };

    let mut f_scores = vec![init; n];
    let all_idx: Vec<usize> = (0..n).collect();
    let mut trees: Vec<serde_json::Value> = Vec::with_capacity(n_est);

    for _ in 0..n_est {
        // Per-sample gradient g and Hessian h of the loss; Newton pseudo-residual
        // r = -g/h with weight h reproduces XGBoost's optimal leaf weight -G/H.
        let mut r = vec![0.0f64; n];
        let mut w = vec![0.0f64; n];
        for i in 0..n {
            let (g, h) = if binary {
                let p = 1.0 / (1.0 + (-f_scores[i]).exp());
                (p - y[i], (p * (1.0 - p)).max(1e-6))
            } else {
                (f_scores[i] - y[i], 1.0)
            };
            r[i] = -g / h;
            w[i] = h;
        }
        let tree = build_reg_tree(&all_idx, &x, &r, &w, 0, max_depth, min_leaf);
        for i in 0..n {
            f_scores[i] += lr * predict_tree_json(&tree, &x.row(i).to_vec());
        }
        trees.push(tree);
    }

    // Training metric.
    let mut metrics = HashMap::new();
    if binary {
        let correct = (0..n)
            .filter(|&i| ((1.0 / (1.0 + (-f_scores[i]).exp()) > 0.5) as i32 as f64) == y[i])
            .count();
        metrics.insert("accuracy".to_string(), correct as f64 / n as f64);
    } else {
        let mean_y = y.iter().sum::<f64>() / n as f64;
        let (mut ss_res, mut ss_tot) = (0.0, 0.0);
        for i in 0..n {
            ss_res += (y[i] - f_scores[i]).powi(2);
            ss_tot += (y[i] - mean_y).powi(2);
        }
        metrics.insert("r2".to_string(), if ss_tot > 0.0 { 1.0 - ss_res / ss_tot } else { 0.0 });
    }
    metrics.insert("n_estimators".to_string(), n_est as f64);

    let model_data = serde_json::json!({
        "type": "gradient_boosting", "binary": binary, "init": init, "lr": lr, "trees": trees,
    });
    let data = serde_json::to_vec(&model_data).map_err(|e| format!("Serialization failed: {e}"))?;
    Ok(linfa_model(LinfaKind::GradientBoosting, data, config, metrics))
}

/// Build a `TlModel::Linfa` with standard metadata (shared by the new algorithms).
fn linfa_model(kind: LinfaKind, data: Vec<u8>, config: &TrainConfig, metrics: HashMap<String, f64>) -> TlModel {
    TlModel::Linfa {
        kind,
        data,
        metadata: ModelMeta {
            name: config.model_name.clone(),
            version: "0.1.0".to_string(),
            created_at: String::new(),
            features: config.feature_names.clone(),
            target: config.target_name.clone(),
            metrics,
        },
    }
}

/// Predict using a linfa model.
pub fn predict_linfa(model: &TlModel, input: &TlTensor) -> Result<TlTensor, String> {
    match model {
        TlModel::Linfa { kind, data, .. } => match kind {
            LinfaKind::LinearRegression | LinfaKind::Ridge => {
                let model_data: serde_json::Value = serde_json::from_slice(data)
                    .map_err(|e| format!("Deserialization failed: {e}"))?;
                let params: Vec<f64> = model_data["params"]
                    .as_array()
                    .ok_or("Missing params")?
                    .iter()
                    .map(|v| v.as_f64().unwrap_or(0.0))
                    .collect();
                let intercept: f64 = model_data["intercept"].as_f64().unwrap_or(0.0);

                let shape = input.shape();
                if shape.len() == 1 {
                    let x = input.to_vec();
                    let pred: f64 =
                        x.iter().zip(params.iter()).map(|(a, b)| a * b).sum::<f64>() + intercept;
                    Ok(TlTensor::from_list(vec![pred]))
                } else if shape.len() == 2 {
                    let rows = shape[0];
                    let cols = shape[1];
                    let flat = input.to_vec();
                    let mut preds = Vec::with_capacity(rows);
                    for i in 0..rows {
                        let row = &flat[i * cols..(i + 1) * cols];
                        let pred: f64 = row
                            .iter()
                            .zip(params.iter())
                            .map(|(a, b)| a * b)
                            .sum::<f64>()
                            + intercept;
                        preds.push(pred);
                    }
                    Ok(TlTensor::from_list(preds))
                } else {
                    Err(format!("Input must be 1D or 2D, got {}D", shape.len()))
                }
            }
            LinfaKind::LogisticRegression => {
                let model_data: serde_json::Value = serde_json::from_slice(data)
                    .map_err(|e| format!("Deserialization failed: {e}"))?;
                let params: Vec<f64> = model_data["params"]
                    .as_array()
                    .ok_or("Missing params")?
                    .iter()
                    .map(|v| v.as_f64().unwrap_or(0.0))
                    .collect();
                let intercept: f64 = model_data["intercept"].as_f64().unwrap_or(0.0);
                // Class labels for each side of the decision boundary (persisted at
                // train time). Default to 1/0 for models saved before this fix.
                let pos_label = model_data["pos_label"].as_f64().unwrap_or(1.0);
                let neg_label = model_data["neg_label"].as_f64().unwrap_or(0.0);

                apply_rowwise(input, |row| {
                    let logit: f64 =
                        row.iter().zip(params.iter()).map(|(a, b)| a * b).sum::<f64>() + intercept;
                    let prob = 1.0 / (1.0 + (-logit).exp());
                    if prob > 0.5 { pos_label } else { neg_label }
                })
            }
            LinfaKind::DecisionTree => {
                let model_data: serde_json::Value = serde_json::from_slice(data)
                    .map_err(|e| format!("Deserialization failed: {e}"))?;
                let tree = model_data["tree"].clone();
                if tree.is_null() {
                    return Err(
                        "This decision-tree model was saved without its tree structure; retrain it."
                            .to_string(),
                    );
                }
                apply_rowwise(input, |row| predict_tree_json(&tree, row))
            }
            LinfaKind::RandomForest => {
                let model_data: serde_json::Value = serde_json::from_slice(data)
                    .map_err(|e| format!("Deserialization failed: {e}"))?;
                let trees: Vec<serde_json::Value> = model_data["trees"]
                    .as_array()
                    .ok_or("Missing trees")?
                    .clone();
                apply_rowwise(input, |row| vote_trees(&trees, row))
            }
            LinfaKind::KMeans => {
                let model_data: serde_json::Value = serde_json::from_slice(data)
                    .map_err(|e| format!("Deserialization failed: {e}"))?;
                let centroids: Vec<Vec<f64>> = serde_json::from_value(model_data["centroids"].clone())
                    .map_err(|e| format!("Missing centroids: {e}"))?;
                apply_rowwise(input, |row| {
                    let mut best = 0usize;
                    let mut best_d = f64::INFINITY;
                    for (c, cen) in centroids.iter().enumerate() {
                        let dist: f64 = row.iter().zip(cen).map(|(a, b)| (a - b) * (a - b)).sum();
                        if dist < best_d {
                            best_d = dist;
                            best = c;
                        }
                    }
                    best as f64
                })
            }
            LinfaKind::Knn => {
                let model_data: serde_json::Value = serde_json::from_slice(data)
                    .map_err(|e| format!("Deserialization failed: {e}"))?;
                let k = model_data["k"].as_u64().unwrap_or(5) as usize;
                let xtrain: Vec<Vec<f64>> = serde_json::from_value(model_data["x"].clone())
                    .map_err(|e| format!("Missing training data: {e}"))?;
                let ytrain: Vec<f64> = serde_json::from_value(model_data["y"].clone())
                    .map_err(|e| format!("Missing labels: {e}"))?;
                apply_rowwise(input, |row| knn_vote(&xtrain, &ytrain, k, row))
            }
            LinfaKind::NaiveBayes => {
                let model_data: serde_json::Value = serde_json::from_slice(data)
                    .map_err(|e| format!("Deserialization failed: {e}"))?;
                let classes = model_data["classes"].as_array().ok_or("Missing classes")?.clone();
                let nb = NaiveBayesModel::from_json(&classes);
                apply_rowwise(input, |row| nb.predict(row))
            }
            LinfaKind::Dbscan => {
                let model_data: serde_json::Value = serde_json::from_slice(data)
                    .map_err(|e| format!("Deserialization failed: {e}"))?;
                let eps = model_data["eps"].as_f64().unwrap_or(0.5);
                let eps2 = eps * eps;
                let cores: Vec<(Vec<f64>, f64)> = model_data["cores"]
                    .as_array()
                    .ok_or("Missing cores")?
                    .iter()
                    .map(|c| {
                        let p: Vec<f64> = serde_json::from_value(c["p"].clone()).unwrap_or_default();
                        (p, c["c"].as_f64().unwrap_or(-1.0))
                    })
                    .collect();
                apply_rowwise(input, |row| {
                    let mut best = -1.0;
                    let mut best_d = f64::INFINITY;
                    for (p, c) in &cores {
                        let dist = sq_dist(p, row);
                        if dist <= eps2 && dist < best_d {
                            best_d = dist;
                            best = *c;
                        }
                    }
                    best
                })
            }
            LinfaKind::GradientBoosting => {
                let model_data: serde_json::Value = serde_json::from_slice(data)
                    .map_err(|e| format!("Deserialization failed: {e}"))?;
                let binary = model_data["binary"].as_bool().unwrap_or(false);
                let init = model_data["init"].as_f64().unwrap_or(0.0);
                let lr = model_data["lr"].as_f64().unwrap_or(0.1);
                let trees: Vec<serde_json::Value> =
                    model_data["trees"].as_array().ok_or("Missing trees")?.clone();
                apply_rowwise(input, |row| {
                    let mut score = init;
                    for t in &trees {
                        score += lr * predict_tree_json(t, row);
                    }
                    if binary {
                        if 1.0 / (1.0 + (-score).exp()) > 0.5 { 1.0 } else { 0.0 }
                    } else {
                        score
                    }
                })
            }
        },
        _ => Err("predict_linfa called on non-Linfa model".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_train_linear_regression() {
        // y = 2*x1 + 3*x2 + 1
        let features = TlTensor::from_vec(
            vec![
                1.0, 1.0, 2.0, 1.0, 3.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 2.0, 1.0, 3.0, 2.0, 3.0,
                3.0, 3.0, 4.0, 4.0,
            ],
            &[10, 2],
        )
        .unwrap();

        let target = TlTensor::from_list(vec![
            6.0, 8.0, 10.0, 9.0, 11.0, 13.0, 12.0, 14.0, 16.0, 21.0,
        ]);

        let config = TrainConfig {
            features,
            target,
            feature_names: vec!["x1".to_string(), "x2".to_string()],
            target_name: "y".to_string(),
            model_name: "test_linear".to_string(),
            split_ratio: 1.0,
            hyperparams: HashMap::new(),
        };

        let model = train("linear", &config).unwrap();
        if let TlModel::Linfa { metadata, .. } = &model {
            assert!(metadata.metrics["r2"] > 0.9, "R² should be > 0.9");
        } else {
            panic!("Expected Linfa model");
        }
    }

    #[test]
    fn test_predict_linear() {
        let features =
            TlTensor::from_vec(vec![1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 2.0, 0.0], &[4, 2]).unwrap();
        let target = TlTensor::from_list(vec![2.0, 3.0, 5.0, 4.0]);

        let config = TrainConfig {
            features,
            target,
            feature_names: vec!["x1".to_string(), "x2".to_string()],
            target_name: "y".to_string(),
            model_name: "test".to_string(),
            split_ratio: 1.0,
            hyperparams: HashMap::new(),
        };

        let model = train("linear", &config).unwrap();
        let input = TlTensor::from_vec(vec![1.0, 0.0], &[1, 2]).unwrap();
        let pred = predict_linfa(&model, &input).unwrap();
        // Should be close to 2.0
        assert!((pred.to_vec()[0] - 2.0).abs() < 1.0);
    }
}
