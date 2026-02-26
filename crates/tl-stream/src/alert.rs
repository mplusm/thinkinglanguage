// ThinkingLanguage — Alerting for pipeline failures/successes

use std::fmt;

/// Target for sending alerts.
#[derive(Debug, Clone)]
pub enum AlertTarget {
    /// Slack webhook URL
    Slack(String),
    /// Generic webhook URL
    Webhook(String),
}

impl fmt::Display for AlertTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlertTarget::Slack(url) => write!(f, "slack:{url}"),
            AlertTarget::Webhook(url) => write!(f, "webhook:{url}"),
        }
    }
}

/// Send an alert message to the specified target.
pub fn send_alert(target: &AlertTarget, message: &str) -> Result<(), String> {
    match target {
        AlertTarget::Slack(url) => send_slack_alert(url, message),
        AlertTarget::Webhook(url) => send_webhook_alert(url, message),
    }
}

fn send_slack_alert(url: &str, message: &str) -> Result<(), String> {
    let payload = serde_json::json!({
        "text": message,
    });

    let client = reqwest::blocking::Client::new();
    let resp = client
        .post(url)
        .json(&payload)
        .send()
        .map_err(|e| format!("Slack alert failed: {e}"))?;

    if resp.status().is_success() {
        Ok(())
    } else {
        Err(format!(
            "Slack alert returned status {}",
            resp.status()
        ))
    }
}

fn send_webhook_alert(url: &str, message: &str) -> Result<(), String> {
    let payload = serde_json::json!({
        "alert": true,
        "message": message,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });

    let client = reqwest::blocking::Client::new();
    let resp = client
        .post(url)
        .json(&payload)
        .send()
        .map_err(|e| format!("Webhook alert failed: {e}"))?;

    if resp.status().is_success() {
        Ok(())
    } else {
        Err(format!(
            "Webhook alert returned status {}",
            resp.status()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alert_target_display() {
        let slack = AlertTarget::Slack("https://hooks.slack.com/test".to_string());
        assert_eq!(format!("{slack}"), "slack:https://hooks.slack.com/test");

        let webhook = AlertTarget::Webhook("https://example.com/webhook".to_string());
        assert_eq!(format!("{webhook}"), "webhook:https://example.com/webhook");
    }

    // Note: actual alert sending requires external services, tested via integration tests
}
