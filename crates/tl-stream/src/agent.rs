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
#[derive(Clone)]
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
    pub output_format: Option<String>,
}

impl std::fmt::Debug for AgentDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentDef")
            .field("name", &self.name)
            .field("model", &self.model)
            .field("system_prompt", &self.system_prompt)
            .field("tools", &self.tools)
            .field("max_turns", &self.max_turns)
            .field("temperature", &self.temperature)
            .field("max_tokens", &self.max_tokens)
            .field("base_url", &self.base_url)
            .field(
                "api_key",
                &if self.api_key.is_some() {
                    "***".to_string()
                } else {
                    "None".to_string()
                },
            )
            .finish()
    }
}
