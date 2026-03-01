// ThinkingLanguage — Model type
// Represents trained ML models (linfa, ONNX, LLM endpoints).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

/// A trained model in ThinkingLanguage.
#[derive(Clone)]
pub enum TlModel {
    /// An ONNX model loaded from disk.
    Onnx { path: PathBuf, metadata: ModelMeta },
    /// A linfa-trained model (serialized).
    Linfa {
        kind: LinfaKind,
        data: Vec<u8>,
        metadata: ModelMeta,
    },
    /// An LLM endpoint (Claude, OpenAI, etc.)
    LlmEndpoint {
        provider: String,
        model_name: String,
        api_key: Option<String>,
    },
}

impl fmt::Debug for TlModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TlModel::Onnx { metadata, .. } => write!(f, "<model:onnx {}>", metadata.name),
            TlModel::Linfa { kind, metadata, .. } => {
                write!(f, "<model:{kind:?} {}>", metadata.name)
            }
            TlModel::LlmEndpoint {
                provider,
                model_name,
                ..
            } => write!(f, "<model:llm {provider}/{model_name}>"),
        }
    }
}

impl fmt::Display for TlModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TlModel::Onnx { metadata, .. } => write!(f, "<model {}>", metadata.name),
            TlModel::Linfa { metadata, .. } => write!(f, "<model {}>", metadata.name),
            TlModel::LlmEndpoint {
                provider,
                model_name,
                ..
            } => write!(f, "<model {provider}/{model_name}>"),
        }
    }
}

/// What kind of linfa model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinfaKind {
    LinearRegression,
    LogisticRegression,
    DecisionTree,
}

/// Model metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMeta {
    pub name: String,
    pub version: String,
    pub created_at: String,
    pub features: Vec<String>,
    pub target: String,
    pub metrics: HashMap<String, f64>,
}

impl Default for ModelMeta {
    fn default() -> Self {
        ModelMeta {
            name: String::new(),
            version: "0.1.0".to_string(),
            created_at: String::new(),
            features: Vec::new(),
            target: String::new(),
            metrics: HashMap::new(),
        }
    }
}

impl TlModel {
    /// Save a model to a .tlmodel directory.
    pub fn save(&self, path: &Path) -> Result<(), String> {
        fs::create_dir_all(path).map_err(|e| format!("Failed to create dir: {e}"))?;

        match self {
            TlModel::Linfa {
                kind,
                data,
                metadata,
            } => {
                let meta = serde_json::json!({
                    "type": "linfa",
                    "kind": kind,
                    "metadata": metadata,
                });
                fs::write(
                    path.join("metadata.json"),
                    serde_json::to_string_pretty(&meta).unwrap(),
                )
                .map_err(|e| format!("Failed to write metadata: {e}"))?;
                fs::write(path.join("model.bin"), data)
                    .map_err(|e| format!("Failed to write model: {e}"))?;
            }
            TlModel::Onnx {
                path: onnx_path,
                metadata,
            } => {
                let meta = serde_json::json!({
                    "type": "onnx",
                    "onnx_path": onnx_path.display().to_string(),
                    "metadata": metadata,
                });
                fs::write(
                    path.join("metadata.json"),
                    serde_json::to_string_pretty(&meta).unwrap(),
                )
                .map_err(|e| format!("Failed to write metadata: {e}"))?;
                // Copy the ONNX file if it exists
                if onnx_path.exists() {
                    fs::copy(onnx_path, path.join("model.onnx"))
                        .map_err(|e| format!("Failed to copy ONNX model: {e}"))?;
                }
            }
            TlModel::LlmEndpoint {
                provider,
                model_name,
                ..
            } => {
                let meta = serde_json::json!({
                    "type": "llm",
                    "provider": provider,
                    "model_name": model_name,
                });
                fs::write(
                    path.join("metadata.json"),
                    serde_json::to_string_pretty(&meta).unwrap(),
                )
                .map_err(|e| format!("Failed to write metadata: {e}"))?;
            }
        }
        Ok(())
    }

    /// Load a model from a .tlmodel directory.
    pub fn load(path: &Path) -> Result<Self, String> {
        let meta_path = path.join("metadata.json");
        let meta_str =
            fs::read_to_string(&meta_path).map_err(|e| format!("Failed to read metadata: {e}"))?;
        let meta: serde_json::Value =
            serde_json::from_str(&meta_str).map_err(|e| format!("Invalid metadata: {e}"))?;

        let model_type = meta["type"].as_str().ok_or("Missing 'type' in metadata")?;

        match model_type {
            "linfa" => {
                let kind: LinfaKind = serde_json::from_value(meta["kind"].clone())
                    .map_err(|e| format!("Invalid linfa kind: {e}"))?;
                let metadata: ModelMeta = serde_json::from_value(meta["metadata"].clone())
                    .map_err(|e| format!("Invalid metadata: {e}"))?;
                let data = fs::read(path.join("model.bin"))
                    .map_err(|e| format!("Failed to read model: {e}"))?;
                Ok(TlModel::Linfa {
                    kind,
                    data,
                    metadata,
                })
            }
            "onnx" => {
                let onnx_path = path.join("model.onnx");
                let metadata: ModelMeta = serde_json::from_value(meta["metadata"].clone())
                    .map_err(|e| format!("Invalid metadata: {e}"))?;
                Ok(TlModel::Onnx {
                    path: onnx_path,
                    metadata,
                })
            }
            "llm" => {
                let provider = meta["provider"].as_str().unwrap_or("unknown").to_string();
                let model_name = meta["model_name"].as_str().unwrap_or("unknown").to_string();
                Ok(TlModel::LlmEndpoint {
                    provider,
                    model_name,
                    api_key: None,
                })
            }
            _ => Err(format!("Unknown model type: {model_type}")),
        }
    }

    /// Get model metadata (if available).
    pub fn metadata(&self) -> Option<&ModelMeta> {
        match self {
            TlModel::Onnx { metadata, .. } => Some(metadata),
            TlModel::Linfa { metadata, .. } => Some(metadata),
            TlModel::LlmEndpoint { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_save_load_linfa_model() {
        let dir = tempfile::tempdir().unwrap();
        let model_path = dir.path().join("test.tlmodel");

        let model = TlModel::Linfa {
            kind: LinfaKind::LinearRegression,
            data: vec![1, 2, 3, 4],
            metadata: ModelMeta {
                name: "test_model".to_string(),
                version: "0.1.0".to_string(),
                created_at: "2024-01-01".to_string(),
                features: vec!["x1".to_string(), "x2".to_string()],
                target: "y".to_string(),
                metrics: {
                    let mut m = HashMap::new();
                    m.insert("r2".to_string(), 0.95);
                    m
                },
            },
        };

        model.save(&model_path).unwrap();
        let loaded = TlModel::load(&model_path).unwrap();

        if let TlModel::Linfa {
            kind,
            data,
            metadata,
        } = loaded
        {
            assert_eq!(kind, LinfaKind::LinearRegression);
            assert_eq!(data, vec![1, 2, 3, 4]);
            assert_eq!(metadata.name, "test_model");
            assert_eq!(metadata.features.len(), 2);
            assert!((metadata.metrics["r2"] - 0.95).abs() < 1e-10);
        } else {
            panic!("Expected Linfa model");
        }
    }

    #[test]
    fn test_model_display() {
        let model = TlModel::Linfa {
            kind: LinfaKind::LinearRegression,
            data: vec![],
            metadata: ModelMeta {
                name: "my_model".to_string(),
                ..Default::default()
            },
        };
        assert_eq!(format!("{model}"), "<model my_model>");
    }
}
