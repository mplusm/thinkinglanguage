// ThinkingLanguage — AI/ML Integration
// Licensed under MIT OR Apache-2.0
//
// Phase 3: Tensor type, model training (linfa), prediction (ONNX Runtime),
// embeddings, LLM integration (Claude/OpenAI), model registry.

pub mod tensor;
pub mod model;
pub mod registry;
pub mod train;
pub mod predict;
pub mod embed;
pub mod llm;

pub use tensor::TlTensor;
pub use model::{TlModel, LinfaKind, ModelMeta};
pub use registry::ModelRegistry;
pub use train::{train, TrainConfig};
pub use predict::{predict, predict_batch};
pub use embed::similarity;
pub use llm::{complete as ai_complete, chat as ai_chat};
