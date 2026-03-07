// ThinkingLanguage — LLM Integration
// HTTP-based integration with Claude, OpenAI, and any OpenAI-compatible endpoint.

use serde_json::json;

/// Structured LLM response — either text or tool-use requests.
#[derive(Debug, Clone)]
pub enum LlmResponse {
    Text(String),
    ToolUse(Vec<ToolCall>),
}

/// A tool call requested by the model.
#[derive(Debug, Clone)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub input: serde_json::Value,
}

/// LLM client configuration.
pub struct LlmClient {
    pub provider: String,
    pub model: String,
    pub api_key: String,
    pub system_prompt: Option<String>,
    pub temperature: f64,
    pub max_tokens: u32,
    pub base_url: Option<String>,
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
            base_url: None,
        })
    }
}

/// Resolve API key from environment variables.
fn resolve_api_key(provider: &str) -> Result<String, String> {
    // Try generic key first
    if let Ok(key) = std::env::var("TL_LLM_KEY") {
        return Ok(key);
    }

    let var_name = if provider.starts_with("claude") || provider == "anthropic" {
        "TL_ANTHROPIC_KEY"
    } else if provider.starts_with("gpt") || provider == "openai" {
        "TL_OPENAI_KEY"
    } else {
        // For unknown providers, try generic key or OpenAI-compatible key
        return std::env::var("TL_LLM_KEY").map_err(|_| {
            format!(
                "API key not found for provider '{provider}'. Set TL_LLM_KEY, TL_ANTHROPIC_KEY, or TL_OPENAI_KEY."
            )
        });
    };

    std::env::var(var_name).map_err(|_| {
        format!(
            "API key not found. Set the {var_name} environment variable or pass api_key parameter."
        )
    })
}

/// Determine provider from model name.
fn detect_provider(model: &str) -> &str {
    if model.starts_with("claude") {
        "anthropic"
    } else {
        "openai"
    }
}

/// Single completion: send a prompt, get a response string.
pub fn complete(
    prompt: &str,
    model: Option<&str>,
    temperature: Option<f64>,
    max_tokens: Option<u32>,
) -> Result<String, String> {
    let model = model.unwrap_or("claude-sonnet-4-20250514");
    let provider = detect_provider(model);

    let client = LlmClient::new(provider, model, None, None, temperature, max_tokens)?;
    do_complete(&client, prompt)
}

/// Multi-turn chat: send messages, get a response.
pub fn chat(
    model: &str,
    system: Option<&str>,
    messages: &[(String, String)],
) -> Result<String, String> {
    let provider = detect_provider(model);

    let client = LlmClient::new(provider, model, None, system, None, None)?;
    do_chat(&client, messages)
}

/// Multi-turn chat with tool definitions. Returns structured LlmResponse.
pub fn chat_with_tools(
    model: &str,
    system: Option<&str>,
    messages: &[serde_json::Value],
    tools: &[serde_json::Value],
    base_url: Option<&str>,
    api_key: Option<&str>,
    output_format: Option<&str>,
) -> Result<LlmResponse, String> {
    let provider = detect_provider(model);

    // Resolve API key
    let resolved_key = match api_key {
        Some(k) if !k.is_empty() => k.to_string(),
        _ => resolve_api_key(provider)?,
    };

    // Determine the actual base URL
    let effective_base_url = base_url
        .map(|s| s.to_string())
        .or_else(|| std::env::var("TL_LLM_BASE_URL").ok());

    let http = reqwest::blocking::Client::new();

    // If base_url is set, always use OpenAI-compatible protocol
    let use_anthropic = provider == "anthropic" && effective_base_url.is_none();

    // Retry with exponential backoff for transient errors
    let max_retries = 3u32;
    let mut last_err = String::new();
    for attempt in 0..=max_retries {
        let result = if use_anthropic {
            call_anthropic(&http, model, system, messages, tools, &resolved_key)
        } else {
            let url = effective_base_url
                .clone()
                .unwrap_or_else(|| "https://api.openai.com/v1".to_string());
            call_openai(
                &http,
                model,
                system,
                messages,
                tools,
                &resolved_key,
                &url,
                output_format,
            )
        };
        match result {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                let is_transient = e.contains("429")
                    || e.contains("500")
                    || e.contains("502")
                    || e.contains("503")
                    || e.contains("rate limit")
                    || e.contains("overloaded");
                if is_transient && attempt < max_retries {
                    let delay_ms = 1000 * 2u64.pow(attempt); // 1s, 2s, 4s
                    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                    last_err = e;
                    continue;
                }
                return Err(e);
            }
        }
    }
    Err(last_err)
}

/// Format tool results back into messages for the next turn.
pub fn format_tool_result_messages(
    provider: &str,
    tool_calls: &[ToolCall],
    results: &[(String, String)],
) -> Vec<serde_json::Value> {
    let use_anthropic = provider == "anthropic";

    if use_anthropic {
        // Anthropic: single user message with tool_result content blocks
        let content: Vec<serde_json::Value> = tool_calls
            .iter()
            .zip(results.iter())
            .map(|(tc, (_name, result))| {
                json!({
                    "type": "tool_result",
                    "tool_use_id": tc.id,
                    "content": result
                })
            })
            .collect();
        vec![json!({"role": "user", "content": content})]
    } else {
        // OpenAI: separate tool message per result
        tool_calls
            .iter()
            .zip(results.iter())
            .map(|(tc, (_name, result))| {
                json!({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result
                })
            })
            .collect()
    }
}

// --- Internal: Anthropic API with tools ---

fn call_anthropic(
    http: &reqwest::blocking::Client,
    model: &str,
    system: Option<&str>,
    messages: &[serde_json::Value],
    tools: &[serde_json::Value],
    api_key: &str,
) -> Result<LlmResponse, String> {
    let mut body = json!({
        "model": model,
        "max_tokens": 4096,
        "messages": messages,
    });

    if let Some(sys) = system {
        body["system"] = json!(sys);
    }

    if !tools.is_empty() {
        // Convert from OpenAI tool format to Anthropic format
        let anthropic_tools: Vec<serde_json::Value> = tools
            .iter()
            .filter_map(|t| {
                let func = t.get("function")?;
                Some(json!({
                    "name": func["name"],
                    "description": func["description"],
                    "input_schema": func["parameters"]
                }))
            })
            .collect();
        body["tools"] = json!(anthropic_tools);
    }

    let resp = http
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", api_key)
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

    parse_anthropic_response(&json)
}

fn parse_anthropic_response(json: &serde_json::Value) -> Result<LlmResponse, String> {
    let content = json["content"]
        .as_array()
        .ok_or("No content in Anthropic response")?;

    let mut tool_calls = Vec::new();
    let mut text_parts = Vec::new();

    for block in content {
        match block["type"].as_str() {
            Some("tool_use") => {
                tool_calls.push(ToolCall {
                    id: block["id"].as_str().unwrap_or("").to_string(),
                    name: block["name"].as_str().unwrap_or("").to_string(),
                    input: block["input"].clone(),
                });
            }
            Some("text") => {
                if let Some(t) = block["text"].as_str() {
                    text_parts.push(t.to_string());
                }
            }
            _ => {}
        }
    }

    if !tool_calls.is_empty() {
        Ok(LlmResponse::ToolUse(tool_calls))
    } else {
        Ok(LlmResponse::Text(text_parts.join("")))
    }
}

// --- Internal: OpenAI-compatible API with tools ---

#[allow(clippy::too_many_arguments)]
fn call_openai(
    http: &reqwest::blocking::Client,
    model: &str,
    system: Option<&str>,
    messages: &[serde_json::Value],
    tools: &[serde_json::Value],
    api_key: &str,
    base_url: &str,
    output_format: Option<&str>,
) -> Result<LlmResponse, String> {
    let mut msgs: Vec<serde_json::Value> = Vec::new();
    if let Some(sys) = system {
        msgs.push(json!({"role": "system", "content": sys}));
    }
    msgs.extend_from_slice(messages);

    let mut body = json!({
        "model": model,
        "messages": msgs,
    });

    if !tools.is_empty() {
        body["tools"] = json!(tools);
    }

    // JSON mode: request structured output
    if output_format == Some("json") {
        body["response_format"] = json!({"type": "json_object"});
    }

    let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));

    let resp = http
        .post(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .header("content-type", "application/json")
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

    parse_openai_response(&json)
}

fn parse_openai_response(json: &serde_json::Value) -> Result<LlmResponse, String> {
    let message = &json["choices"][0]["message"];

    // Check for tool calls
    if let Some(tool_calls_arr) = message["tool_calls"].as_array()
        && !tool_calls_arr.is_empty()
    {
        let tool_calls: Vec<ToolCall> = tool_calls_arr
            .iter()
            .filter_map(|tc| {
                let func = tc.get("function")?;
                let input: serde_json::Value = func["arguments"]
                    .as_str()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));
                Some(ToolCall {
                    id: tc["id"].as_str().unwrap_or("").to_string(),
                    name: func["name"].as_str().unwrap_or("").to_string(),
                    input,
                })
            })
            .collect();
        return Ok(LlmResponse::ToolUse(tool_calls));
    }

    // Text response
    message["content"]
        .as_str()
        .map(|s| LlmResponse::Text(s.to_string()))
        .ok_or_else(|| "No content in OpenAI response".to_string())
}

/// Streaming chat completion. Calls `on_chunk` with each text delta.
/// Returns the full accumulated text.
pub fn stream_chat(
    model: &str,
    system: Option<&str>,
    messages: &[serde_json::Value],
    base_url: Option<&str>,
    api_key: Option<&str>,
) -> Result<StreamReader, String> {
    let provider = detect_provider(model);
    let resolved_key = match api_key {
        Some(k) if !k.is_empty() => k.to_string(),
        _ => resolve_api_key(provider)?,
    };
    let effective_base_url = base_url
        .map(|s| s.to_string())
        .or_else(|| std::env::var("TL_LLM_BASE_URL").ok());

    let http = reqwest::blocking::Client::new();
    let use_anthropic = provider == "anthropic" && effective_base_url.is_none();

    if use_anthropic {
        stream_anthropic(&http, model, system, messages, &resolved_key)
    } else {
        let url = effective_base_url.unwrap_or_else(|| "https://api.openai.com/v1".to_string());
        stream_openai(&http, model, system, messages, &resolved_key, &url)
    }
}

/// A streaming response reader that yields text chunks.
pub struct StreamReader {
    lines: std::io::BufReader<reqwest::blocking::Response>,
    is_anthropic: bool,
    done: bool,
}

impl StreamReader {
    /// Read the next text chunk. Returns None when stream is done.
    pub fn next_chunk(&mut self) -> Result<Option<String>, String> {
        use std::io::BufRead;
        if self.done {
            return Ok(None);
        }
        loop {
            let mut line = String::new();
            match self.lines.read_line(&mut line) {
                Ok(0) => {
                    self.done = true;
                    return Ok(None);
                }
                Ok(_) => {}
                Err(e) => return Err(format!("Stream read error: {e}")),
            }
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if !line.starts_with("data: ") {
                continue;
            }
            let data = &line[6..];
            if data == "[DONE]" {
                self.done = true;
                return Ok(None);
            }

            let json: serde_json::Value = match serde_json::from_str(data) {
                Ok(v) => v,
                Err(_) => continue,
            };

            if self.is_anthropic {
                // Anthropic SSE: {"type":"content_block_delta","delta":{"type":"text_delta","text":"..."}}
                if json["type"].as_str() == Some("content_block_delta") {
                    if let Some(text) = json["delta"]["text"].as_str()
                        && !text.is_empty()
                    {
                        return Ok(Some(text.to_string()));
                    }
                } else if json["type"].as_str() == Some("message_stop") {
                    self.done = true;
                    return Ok(None);
                }
            } else {
                // OpenAI SSE: {"choices":[{"delta":{"content":"..."}}]}
                if let Some(content) = json["choices"][0]["delta"]["content"].as_str()
                    && !content.is_empty()
                {
                    return Ok(Some(content.to_string()));
                }
                // Check for finish_reason
                if json["choices"][0]["finish_reason"].as_str().is_some() {
                    self.done = true;
                    return Ok(None);
                }
            }
        }
    }
}

fn stream_openai(
    http: &reqwest::blocking::Client,
    model: &str,
    system: Option<&str>,
    messages: &[serde_json::Value],
    api_key: &str,
    base_url: &str,
) -> Result<StreamReader, String> {
    let mut msgs: Vec<serde_json::Value> = Vec::new();
    if let Some(sys) = system {
        msgs.push(json!({"role": "system", "content": sys}));
    }
    msgs.extend_from_slice(messages);

    let body = json!({
        "model": model,
        "messages": msgs,
        "stream": true,
    });

    let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));
    let resp = http
        .post(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .map_err(|e| format!("Stream request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        return Err(format!("OpenAI streaming API error ({status}): {body}"));
    }

    Ok(StreamReader {
        lines: std::io::BufReader::new(resp),
        is_anthropic: false,
        done: false,
    })
}

fn stream_anthropic(
    http: &reqwest::blocking::Client,
    model: &str,
    system: Option<&str>,
    messages: &[serde_json::Value],
    api_key: &str,
) -> Result<StreamReader, String> {
    let mut body = json!({
        "model": model,
        "max_tokens": 4096,
        "messages": messages,
        "stream": true,
    });
    if let Some(sys) = system {
        body["system"] = json!(sys);
    }

    let resp = http
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .map_err(|e| format!("Stream request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        return Err(format!("Anthropic streaming API error ({status}): {body}"));
    }

    Ok(StreamReader {
        lines: std::io::BufReader::new(resp),
        is_anthropic: true,
        done: false,
    })
}

// --- Backward-compatible internal helpers ---

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
