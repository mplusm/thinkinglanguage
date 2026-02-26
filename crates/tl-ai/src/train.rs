// ThinkingLanguage — Training dispatcher
// Uses linfa for pure-Rust ML training.

use std::collections::HashMap;

use linfa::prelude::*;
use linfa::Dataset;
use ndarray::{Array1, Array2};

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
        _ => Err(format!(
            "Unknown training algorithm: '{algorithm}'. Supported: linear, logistic, tree"
        )),
    }
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
    let data =
        serde_json::to_vec(&model_data).map_err(|e| format!("Serialization failed: {e}"))?;

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
    let model_data = serde_json::json!({
        "params": params.as_slice().unwrap_or(&[]),
        "intercept": intercept,
    });
    let data =
        serde_json::to_vec(&model_data).map_err(|e| format!("Serialization failed: {e}"))?;

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

    // Serialize — for tree we store a JSON representation
    let model_data = serde_json::json!({
        "type": "decision_tree",
        "accuracy": accuracy,
    });
    let data =
        serde_json::to_vec(&model_data).map_err(|e| format!("Serialization failed: {e}"))?;

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

/// Predict using a linfa model.
pub fn predict_linfa(model: &TlModel, input: &TlTensor) -> Result<TlTensor, String> {
    match model {
        TlModel::Linfa { kind, data, .. } => match kind {
            LinfaKind::LinearRegression => {
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
                        let pred: f64 =
                            row.iter().zip(params.iter()).map(|(a, b)| a * b).sum::<f64>()
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

                let shape = input.shape();
                if shape.len() == 1 {
                    let x = input.to_vec();
                    let logit: f64 =
                        x.iter().zip(params.iter()).map(|(a, b)| a * b).sum::<f64>() + intercept;
                    let prob = 1.0 / (1.0 + (-logit).exp());
                    Ok(TlTensor::from_list(vec![if prob > 0.5 {
                        1.0
                    } else {
                        0.0
                    }]))
                } else if shape.len() == 2 {
                    let rows = shape[0];
                    let cols = shape[1];
                    let flat = input.to_vec();
                    let mut preds = Vec::with_capacity(rows);
                    for i in 0..rows {
                        let row = &flat[i * cols..(i + 1) * cols];
                        let logit: f64 =
                            row.iter().zip(params.iter()).map(|(a, b)| a * b).sum::<f64>()
                                + intercept;
                        let prob = 1.0 / (1.0 + (-logit).exp());
                        preds.push(if prob > 0.5 { 1.0 } else { 0.0 });
                    }
                    Ok(TlTensor::from_list(preds))
                } else {
                    Err(format!("Input must be 1D or 2D, got {}D", shape.len()))
                }
            }
            LinfaKind::DecisionTree => Err(
                "Decision tree prediction from serialized model not yet supported. \
                 Use predict immediately after training."
                    .to_string(),
            ),
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
                1.0, 1.0, 2.0, 1.0, 3.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 2.0, 1.0, 3.0, 2.0,
                3.0, 3.0, 3.0, 4.0, 4.0,
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
