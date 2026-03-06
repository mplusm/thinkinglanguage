// ThinkingLanguage — Agent Definition
// Phase 34: AI Agent Framework

/// A tool available to an agent.
#[derive(Debug, Clone)]
pub struct AgentTool {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

/// Agent definition — runtime configuration for an AI agent.
#[derive(Debug, Clone)]
pub struct AgentDef {
    pub name: String,
    pub model: String,
    pub system_prompt: Option<String>,
    pub tools: Vec<AgentTool>,
    pub max_turns: u32,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u32>,
    pub base_url: Option<String>,
    pub api_key: Option<String>,
}
