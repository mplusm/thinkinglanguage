// ThinkingLanguage — Prediction (ONNX Runtime + linfa)

use std::path::Path;

use crate::model::TlModel;
use crate::tensor::TlTensor;
use crate::train::predict_linfa;

/// Run prediction on a model.
pub fn predict(model: &TlModel, input: &TlTensor) -> Result<TlTensor, String> {
    match model {
        TlModel::Onnx { path, .. } => predict_onnx(path, input),
        TlModel::Linfa { .. } => predict_linfa(model, input),
        TlModel::LlmEndpoint { .. } => {
            Err("Cannot use predict() on an LLM endpoint. Use ai_complete() instead.".to_string())
        }
    }
}

/// Run prediction using ONNX Runtime.
pub fn predict_onnx(model_path: &Path, input: &TlTensor) -> Result<TlTensor, String> {
    use ort::session::Session;

    let mut session = Session::builder()
        .and_then(|b| b.commit_from_file(model_path))
        .map_err(|e| format!("Failed to load ONNX model: {e}"))?;

    let shape = input.shape();
    let flat_data: Vec<f32> = input.to_vec().iter().map(|&x| x as f32).collect();
    let shape_i64: Vec<i64> = shape.iter().map(|&s| s as i64).collect();

    // Create ORT tensor value: needs (shape, Vec<T>)
    let input_value = ort::value::Tensor::from_array((shape_i64, flat_data))
        .map_err(|e| format!("Failed to create ORT tensor: {e}"))?;

    let outputs = session
        .run(ort::inputs![input_value])
        .map_err(|e| format!("ONNX inference failed: {e}"))?;

    // Extract first output
    let output = outputs.values().next().ok_or("No output from ONNX model")?;

    let (out_shape_ref, out_flat) = output
        .try_extract_tensor::<f32>()
        .map_err(|e| format!("Failed to extract output: {e}"))?;

    let out_shape: Vec<usize> = out_shape_ref.iter().map(|&d| d as usize).collect();
    let out_data: Vec<f64> = out_flat.iter().map(|&x| x as f64).collect();

    TlTensor::from_vec(out_data, &out_shape)
}

/// Batch prediction: split input into batches, predict, reassemble.
pub fn predict_batch(
    model: &TlModel,
    input: &TlTensor,
    batch_size: usize,
) -> Result<TlTensor, String> {
    let shape = input.shape();
    if shape.len() < 2 {
        return predict(model, input);
    }

    let n_samples = shape[0];
    if n_samples <= batch_size {
        return predict(model, input);
    }

    let mut all_preds = Vec::new();

    for start in (0..n_samples).step_by(batch_size) {
        let end = (start + batch_size).min(n_samples);
        let batch = input.slice(start, end)?;
        let preds = predict(model, &batch)?;
        all_preds.extend(preds.to_vec());
    }

    Ok(TlTensor::from_list(all_preds))
}
