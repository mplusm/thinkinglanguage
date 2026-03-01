// ThinkingLanguage — Model Registry
// Local model storage at ~/.tl/models/

use std::fs;
use std::path::PathBuf;

use crate::model::TlModel;

/// Local model registry.
pub struct ModelRegistry {
    pub root: PathBuf,
}

impl ModelRegistry {
    /// Create a registry at the default location (~/.tl/models/).
    pub fn default_location() -> Self {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        ModelRegistry {
            root: PathBuf::from(home).join(".tl").join("models"),
        }
    }

    /// Create a registry at a custom location.
    pub fn new(root: PathBuf) -> Self {
        ModelRegistry { root }
    }

    /// Register (save) a model by name.
    pub fn register(&self, name: &str, model: &TlModel) -> Result<(), String> {
        let model_dir = self.root.join(name);
        model.save(&model_dir)
    }

    /// Get (load) a model by name.
    pub fn get(&self, name: &str) -> Result<TlModel, String> {
        let model_dir = self.root.join(name);
        if !model_dir.exists() {
            return Err(format!("Model '{name}' not found in registry"));
        }
        TlModel::load(&model_dir)
    }

    /// List all registered model names.
    pub fn list(&self) -> Vec<String> {
        if !self.root.exists() {
            return Vec::new();
        }
        let mut names = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.root) {
            for entry in entries.flatten() {
                if entry.path().is_dir()
                    && let Some(name) = entry.file_name().to_str()
                {
                    names.push(name.to_string());
                }
            }
        }
        names.sort();
        names
    }

    /// Delete a model from the registry.
    pub fn delete(&self, name: &str) -> Result<(), String> {
        let model_dir = self.root.join(name);
        if !model_dir.exists() {
            return Err(format!("Model '{name}' not found in registry"));
        }
        fs::remove_dir_all(&model_dir).map_err(|e| format!("Failed to delete model: {e}"))
    }

    /// Check if a model exists.
    pub fn exists(&self, name: &str) -> bool {
        self.root.join(name).exists()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{LinfaKind, ModelMeta};

    #[test]
    fn test_registry_crud() {
        let dir = tempfile::tempdir().unwrap();
        let registry = ModelRegistry::new(dir.path().to_path_buf());

        // Initially empty
        assert!(registry.list().is_empty());

        // Register a model
        let model = TlModel::Linfa {
            kind: LinfaKind::LinearRegression,
            data: vec![10, 20, 30],
            metadata: ModelMeta {
                name: "test".to_string(),
                ..Default::default()
            },
        };
        registry.register("my_model", &model).unwrap();

        // List should have it
        assert_eq!(registry.list(), vec!["my_model"]);
        assert!(registry.exists("my_model"));

        // Load it back
        let loaded = registry.get("my_model").unwrap();
        if let TlModel::Linfa { data, .. } = loaded {
            assert_eq!(data, vec![10, 20, 30]);
        } else {
            panic!("Expected Linfa model");
        }

        // Delete it
        registry.delete("my_model").unwrap();
        assert!(registry.list().is_empty());
        assert!(!registry.exists("my_model"));
    }
}
