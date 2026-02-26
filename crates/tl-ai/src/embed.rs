// ThinkingLanguage — Embedding functions
// Text embedding via ONNX models or API calls.

use std::path::Path;

use crate::tensor::TlTensor;

/// Generate text embedding using an ONNX sentence-transformer model.
pub fn embed_onnx(_text: &str, model_path: &Path) -> Result<TlTensor, String> {
    // This requires a tokenizer + ONNX model. For now, provide a stub
    // that will work when a proper sentence-transformer ONNX model is available.
    Err(format!(
        "ONNX embedding requires a sentence-transformer model at {:?}. \
         Use embed_api() with an API key instead.",
        model_path
    ))
}

/// Generate text embedding via API (OpenAI embeddings endpoint).
pub fn embed_api(
    text: &str,
    provider: &str,
    model: &str,
    api_key: &str,
) -> Result<TlTensor, String> {
    let client = reqwest::blocking::Client::new();

    match provider {
        "openai" => {
            let resp = client
                .post("https://api.openai.com/v1/embeddings")
                .header("Authorization", format!("Bearer {api_key}"))
                .json(&serde_json::json!({
                    "input": text,
                    "model": model,
                }))
                .send()
                .map_err(|e| format!("Embedding API request failed: {e}"))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().unwrap_or_default();
                return Err(format!("Embedding API error ({status}): {body}"));
            }

            let json: serde_json::Value = resp
                .json()
                .map_err(|e| format!("Failed to parse embedding response: {e}"))?;

            let embedding = json["data"][0]["embedding"]
                .as_array()
                .ok_or("Missing embedding in response")?
                .iter()
                .map(|v| v.as_f64().unwrap_or(0.0))
                .collect::<Vec<f64>>();

            Ok(TlTensor::from_list(embedding))
        }
        _ => Err(format!(
            "Unsupported embedding provider: '{provider}'. Supported: openai"
        )),
    }
}

/// Cosine similarity between two tensors (convenience wrapper).
pub fn similarity(a: &TlTensor, b: &TlTensor) -> Result<f64, String> {
    a.cosine_similarity(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_similarity() {
        let a = TlTensor::from_list(vec![1.0, 0.0, 0.0]);
        let b = TlTensor::from_list(vec![1.0, 0.0, 0.0]);
        let sim = similarity(&a, &b).unwrap();
        assert!((sim - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_similarity_orthogonal() {
        let a = TlTensor::from_list(vec![1.0, 0.0]);
        let b = TlTensor::from_list(vec![0.0, 1.0]);
        let sim = similarity(&a, &b).unwrap();
        assert!(sim.abs() < 1e-10);
    }
}
