// ThinkingLanguage — LLM Integration
// HTTP-based integration with Claude and OpenAI APIs.

use serde_json::json;

/// LLM client configuration.
pub struct LlmClient {
    pub provider: String,
    pub model: String,
    pub api_key: String,
    pub system_prompt: Option<String>,
    pub temperature: f64,
    pub max_tokens: u32,
}

impl LlmClient {
    /// Create a new LLM client, resolving the API key from params or env vars.
    pub fn new(
        provider: &str,
        model: &str,
        api_key: Option<&str>,
        system_prompt: Option<&str>,
        temperature: Option<f64>,
        max_tokens: Option<u32>,
    ) -> Result<Self, String> {
        let resolved_key = match api_key {
            Some(k) if !k.is_empty() => k.to_string(),
            _ => resolve_api_key(provider)?,
        };

        Ok(LlmClient {
            provider: provider.to_string(),
            model: model.to_string(),
            api_key: resolved_key,
            system_prompt: system_prompt.map(|s| s.to_string()),
            temperature: temperature.unwrap_or(0.7),
            max_tokens: max_tokens.unwrap_or(1024),
        })
    }
}

/// Resolve API key from environment variables.
fn resolve_api_key(provider: &str) -> Result<String, String> {
    let var_name = if provider.starts_with("claude") || provider == "anthropic" {
        "TL_ANTHROPIC_KEY"
    } else if provider.starts_with("gpt") || provider == "openai" {
        "TL_OPENAI_KEY"
    } else {
        return Err(format!(
            "Unknown provider '{provider}'. Set TL_ANTHROPIC_KEY or TL_OPENAI_KEY."
        ));
    };

    std::env::var(var_name).map_err(|_| {
        format!(
            "API key not found. Set the {var_name} environment variable or pass api_key parameter."
        )
    })
}

/// Single completion: send a prompt, get a response string.
pub fn complete(
    prompt: &str,
    model: Option<&str>,
    temperature: Option<f64>,
    max_tokens: Option<u32>,
) -> Result<String, String> {
    let model = model.unwrap_or("claude-sonnet-4-20250514");
    let provider = if model.starts_with("claude") {
        "anthropic"
    } else if model.starts_with("gpt") {
        "openai"
    } else {
        "anthropic"
    };

    let client = LlmClient::new(provider, model, None, None, temperature, max_tokens)?;
    do_complete(&client, prompt)
}

/// Multi-turn chat: send messages, get a response.
pub fn chat(
    model: &str,
    system: Option<&str>,
    messages: &[(String, String)],
) -> Result<String, String> {
    let provider = if model.starts_with("claude") {
        "anthropic"
    } else if model.starts_with("gpt") {
        "openai"
    } else {
        "anthropic"
    };

    let client = LlmClient::new(provider, model, None, system, None, None)?;
    do_chat(&client, messages)
}

fn do_complete(client: &LlmClient, prompt: &str) -> Result<String, String> {
    let http = reqwest::blocking::Client::new();
    let mut last_err = String::new();

    for attempt in 0..3 {
        let result = if client.provider == "anthropic" || client.model.starts_with("claude") {
            complete_anthropic(&http, client, prompt)
        } else {
            complete_openai(&http, client, prompt)
        };

        match result {
            Ok(text) => return Ok(text),
            Err(e) => {
                last_err = e;
                if attempt < 2 {
                    std::thread::sleep(std::time::Duration::from_millis(
                        500 * (attempt as u64 + 1),
                    ));
                }
            }
        }
    }

    Err(format!("LLM request failed after 3 attempts: {last_err}"))
}

fn do_chat(client: &LlmClient, messages: &[(String, String)]) -> Result<String, String> {
    let http = reqwest::blocking::Client::new();

    if client.provider == "anthropic" || client.model.starts_with("claude") {
        chat_anthropic(&http, client, messages)
    } else {
        chat_openai(&http, client, messages)
    }
}

fn complete_anthropic(
    http: &reqwest::blocking::Client,
    client: &LlmClient,
    prompt: &str,
) -> Result<String, String> {
    let body = json!({
        "model": client.model,
        "max_tokens": client.max_tokens,
        "temperature": client.temperature,
        "messages": [{"role": "user", "content": prompt}],
    });

    let resp = http
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", &client.api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .map_err(|e| format!("Request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        return Err(format!("Anthropic API error ({status}): {body}"));
    }

    let json: serde_json::Value = resp
        .json()
        .map_err(|e| format!("Failed to parse response: {e}"))?;

    json["content"][0]["text"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| "No text in Anthropic response".to_string())
}

fn complete_openai(
    http: &reqwest::blocking::Client,
    client: &LlmClient,
    prompt: &str,
) -> Result<String, String> {
    let body = json!({
        "model": client.model,
        "max_tokens": client.max_tokens,
        "temperature": client.temperature,
        "messages": [{"role": "user", "content": prompt}],
    });

    let resp = http
        .post("https://api.openai.com/v1/chat/completions")
        .header("Authorization", format!("Bearer {}", client.api_key))
        .json(&body)
        .send()
        .map_err(|e| format!("Request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        return Err(format!("OpenAI API error ({status}): {body}"));
    }

    let json: serde_json::Value = resp
        .json()
        .map_err(|e| format!("Failed to parse response: {e}"))?;

    json["choices"][0]["message"]["content"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| "No content in OpenAI response".to_string())
}

fn chat_anthropic(
    http: &reqwest::blocking::Client,
    client: &LlmClient,
    messages: &[(String, String)],
) -> Result<String, String> {
    let msgs: Vec<serde_json::Value> = messages
        .iter()
        .map(|(role, content)| json!({"role": role, "content": content}))
        .collect();

    let mut body = json!({
        "model": client.model,
        "max_tokens": client.max_tokens,
        "temperature": client.temperature,
        "messages": msgs,
    });

    if let Some(ref system) = client.system_prompt {
        body["system"] = json!(system);
    }

    let resp = http
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", &client.api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .map_err(|e| format!("Request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        return Err(format!("Anthropic API error ({status}): {body}"));
    }

    let json: serde_json::Value = resp
        .json()
        .map_err(|e| format!("Failed to parse response: {e}"))?;

    json["content"][0]["text"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| "No text in Anthropic response".to_string())
}

fn chat_openai(
    http: &reqwest::blocking::Client,
    client: &LlmClient,
    messages: &[(String, String)],
) -> Result<String, String> {
    let mut msgs: Vec<serde_json::Value> = Vec::new();
    if let Some(ref system) = client.system_prompt {
        msgs.push(json!({"role": "system", "content": system}));
    }
    for (role, content) in messages {
        msgs.push(json!({"role": role, "content": content}));
    }

    let body = json!({
        "model": client.model,
        "max_tokens": client.max_tokens,
        "temperature": client.temperature,
        "messages": msgs,
    });

    let resp = http
        .post("https://api.openai.com/v1/chat/completions")
        .header("Authorization", format!("Bearer {}", client.api_key))
        .json(&body)
        .send()
        .map_err(|e| format!("Request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        return Err(format!("OpenAI API error ({status}): {body}"));
    }

    let json: serde_json::Value = resp
        .json()
        .map_err(|e| format!("Failed to parse response: {e}"))?;

    json["choices"][0]["message"]["content"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| "No content in OpenAI response".to_string())
}
