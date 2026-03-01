// ThinkingLanguage — AI/ML Integration
// Licensed under MIT OR Apache-2.0
//
// Phase 3: Tensor type, model training (linfa), prediction (ONNX Runtime),
// embeddings, LLM integration (Claude/OpenAI), model registry.

pub mod embed;
pub mod llm;
pub mod model;
pub mod predict;
pub mod registry;
pub mod tensor;
pub mod train;

pub use embed::similarity;
pub use llm::{chat as ai_chat, complete as ai_complete};
pub use model::{LinfaKind, ModelMeta, TlModel};
pub use predict::{predict, predict_batch};
pub use registry::ModelRegistry;
pub use tensor::TlTensor;
pub use train::{TrainConfig, train};
