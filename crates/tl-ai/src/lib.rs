// ThinkingLanguage — AI/ML Integration
// Licensed under Apache-2.0
//
// Phase 3: Tensor type, model training (linfa), prediction (ONNX Runtime),
// embeddings, LLM integration (Claude/OpenAI), model registry.
// Phase 34: Multi-provider LLM, tool-use, agent framework.

pub mod embed;
pub mod llm;
pub mod model;
pub mod predict;
pub mod registry;
pub mod tensor;
pub mod train;

pub use embed::similarity;
pub use llm::{
    LlmResponse, StreamReader, ToolCall, chat as ai_chat, chat_with_tools, complete as ai_complete,
    format_tool_result_messages, stream_chat,
};
pub use model::{LinfaKind, ModelMeta, TlModel};
pub use predict::{predict, predict_batch};
pub use registry::ModelRegistry;
pub use tensor::TlTensor;
pub use train::{TrainConfig, train};
