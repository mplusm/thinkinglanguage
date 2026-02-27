// ThinkingLanguage — Connector abstraction for streaming I/O

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Configuration for a connector instance.
#[derive(Debug, Clone)]
pub struct ConnectorConfig {
    pub name: String,
    pub connector_type: String,
    pub properties: HashMap<String, String>,
}

impl fmt::Display for ConnectorConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<connector {}:{}>", self.connector_type, self.name)
    }
}

/// Trait for connectors that can send and receive string messages.
pub trait Connector: Send + Sync {
    fn name(&self) -> &str;
    fn connector_type(&self) -> &str;
    fn send(&self, message: &str) -> Result<(), String>;
    fn recv(&self, timeout_ms: u64) -> Result<Option<String>, String>;
}

/// In-memory channel connector for testing and inter-pipeline communication.
pub struct ChannelConnector {
    name: String,
    tx: mpsc::Sender<String>,
    rx: Arc<Mutex<mpsc::Receiver<String>>>,
}

impl ChannelConnector {
    pub fn new(name: &str, buffer_size: usize) -> Self {
        let (tx, rx) = mpsc::channel(buffer_size);
        ChannelConnector {
            name: name.to_string(),
            tx,
            rx: Arc::new(Mutex::new(rx)),
        }
    }

    /// Create a paired set of connectors (source + sink) sharing a channel.
    pub fn pair(name: &str, buffer_size: usize) -> (ChannelSender, ChannelReceiver) {
        let (tx, rx) = mpsc::channel(buffer_size);
        (
            ChannelSender {
                name: name.to_string(),
                tx,
            },
            ChannelReceiver {
                name: name.to_string(),
                rx: Arc::new(Mutex::new(rx)),
            },
        )
    }
}

impl Connector for ChannelConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn connector_type(&self) -> &str {
        "channel"
    }

    fn send(&self, message: &str) -> Result<(), String> {
        self.tx
            .blocking_send(message.to_string())
            .map_err(|e| format!("Channel send failed: {e}"))
    }

    fn recv(&self, timeout_ms: u64) -> Result<Option<String>, String> {
        let mut rx = self.rx.lock().map_err(|e| format!("Lock error: {e}"))?;
        let rt = tokio::runtime::Handle::try_current();
        match rt {
            Ok(handle) => {
                // We're inside a tokio runtime
                handle.block_on(async {
                    match tokio::time::timeout(
                        std::time::Duration::from_millis(timeout_ms),
                        rx.recv(),
                    )
                    .await
                    {
                        Ok(Some(msg)) => Ok(Some(msg)),
                        Ok(None) => Ok(None), // channel closed
                        Err(_) => Ok(None),   // timeout
                    }
                })
            }
            Err(_) => {
                // No runtime — create one
                let rt = tokio::runtime::Runtime::new()
                    .map_err(|e| format!("Failed to create runtime: {e}"))?;
                rt.block_on(async {
                    match tokio::time::timeout(
                        std::time::Duration::from_millis(timeout_ms),
                        rx.recv(),
                    )
                    .await
                    {
                        Ok(Some(msg)) => Ok(Some(msg)),
                        Ok(None) => Ok(None),
                        Err(_) => Ok(None),
                    }
                })
            }
        }
    }
}

/// Send-only half of a channel pair.
pub struct ChannelSender {
    name: String,
    tx: mpsc::Sender<String>,
}

impl Connector for ChannelSender {
    fn name(&self) -> &str {
        &self.name
    }
    fn connector_type(&self) -> &str {
        "channel"
    }
    fn send(&self, message: &str) -> Result<(), String> {
        self.tx
            .blocking_send(message.to_string())
            .map_err(|e| format!("Channel send failed: {e}"))
    }
    fn recv(&self, _timeout_ms: u64) -> Result<Option<String>, String> {
        Err("ChannelSender does not support recv".to_string())
    }
}

/// Receive-only half of a channel pair.
pub struct ChannelReceiver {
    name: String,
    rx: Arc<Mutex<mpsc::Receiver<String>>>,
}

impl Connector for ChannelReceiver {
    fn name(&self) -> &str {
        &self.name
    }
    fn connector_type(&self) -> &str {
        "channel"
    }
    fn send(&self, _message: &str) -> Result<(), String> {
        Err("ChannelReceiver does not support send".to_string())
    }
    fn recv(&self, timeout_ms: u64) -> Result<Option<String>, String> {
        let mut rx = self.rx.lock().map_err(|e| format!("Lock error: {e}"))?;
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| format!("Failed to create runtime: {e}"))?;
        rt.block_on(async {
            match tokio::time::timeout(
                std::time::Duration::from_millis(timeout_ms),
                rx.recv(),
            )
            .await
            {
                Ok(Some(msg)) => Ok(Some(msg)),
                Ok(None) => Ok(None),
                Err(_) => Ok(None),
            }
        })
    }
}

/// Kafka connector (feature-gated).
#[cfg(feature = "kafka")]
pub mod kafka;

/// User-defined connector wrapping TL struct methods.
/// Allows TL code to create custom connectors by implementing send/recv.
pub struct UserDefinedConnector {
    name: String,
    send_fn: Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>,
    recv_fn: Box<dyn Fn(u64) -> Result<Option<String>, String> + Send + Sync>,
}

impl UserDefinedConnector {
    pub fn new(
        name: String,
        send_fn: Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>,
        recv_fn: Box<dyn Fn(u64) -> Result<Option<String>, String> + Send + Sync>,
    ) -> Self {
        UserDefinedConnector { name, send_fn, recv_fn }
    }
}

impl Connector for UserDefinedConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn connector_type(&self) -> &str {
        "user_defined"
    }

    fn send(&self, message: &str) -> Result<(), String> {
        (self.send_fn)(message)
    }

    fn recv(&self, timeout_ms: u64) -> Result<Option<String>, String> {
        (self.recv_fn)(timeout_ms)
    }
}

/// Factory function to create a connector from config.
pub fn create_connector(config: &ConnectorConfig) -> Result<Box<dyn Connector>, String> {
    match config.connector_type.as_str() {
        "channel" => {
            let buffer = config
                .properties
                .get("buffer")
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(1000);
            Ok(Box::new(ChannelConnector::new(&config.name, buffer)))
        }
        #[cfg(feature = "kafka")]
        "kafka" => kafka::create_kafka_connector(config),
        other => Err(format!("Unknown connector type: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_connector_send_recv() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let conn = ChannelConnector::new("test", 10);
            conn.tx.send("hello".to_string()).await.unwrap();
            let msg = {
                let mut rx = conn.rx.lock().unwrap();
                rx.recv().await
            };
            assert_eq!(msg, Some("hello".to_string()));
        });
    }

    #[test]
    fn test_channel_pair() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (sender, receiver) = ChannelConnector::pair("test_pair", 10);
            sender.tx.send("world".to_string()).await.unwrap();
            let msg = {
                let mut rx = receiver.rx.lock().unwrap();
                rx.recv().await
            };
            assert_eq!(msg, Some("world".to_string()));
        });
    }

    #[test]
    fn test_connector_config_display() {
        let config = ConnectorConfig {
            name: "my_source".to_string(),
            connector_type: "channel".to_string(),
            properties: HashMap::new(),
        };
        assert_eq!(format!("{config}"), "<connector channel:my_source>");
    }

    #[test]
    fn test_create_connector_channel() {
        let config = ConnectorConfig {
            name: "test".to_string(),
            connector_type: "channel".to_string(),
            properties: HashMap::new(),
        };
        let conn = create_connector(&config);
        assert!(conn.is_ok());
        let conn = conn.unwrap();
        assert_eq!(conn.name(), "test");
        assert_eq!(conn.connector_type(), "channel");
    }

    #[test]
    fn test_create_connector_unknown() {
        let config = ConnectorConfig {
            name: "test".to_string(),
            connector_type: "unknown".to_string(),
            properties: HashMap::new(),
        };
        let conn = create_connector(&config);
        assert!(conn.is_err());
    }

    #[test]
    fn test_user_defined_connector() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sent_clone = sent.clone();

        let conn = UserDefinedConnector::new(
            "my_connector".to_string(),
            Box::new(move |msg: &str| {
                sent_clone.lock().unwrap().push(msg.to_string());
                Ok(())
            }),
            Box::new(|_timeout_ms| Ok(Some("received".to_string()))),
        );

        assert_eq!(conn.name(), "my_connector");
        assert_eq!(conn.connector_type(), "user_defined");
        conn.send("hello").unwrap();
        assert_eq!(sent.lock().unwrap().len(), 1);

        let msg = conn.recv(1000).unwrap();
        assert_eq!(msg, Some("received".to_string()));
    }

    #[test]
    fn test_user_defined_connector_as_trait_object() {
        let conn: Box<dyn Connector> = Box::new(UserDefinedConnector::new(
            "test_udc".to_string(),
            Box::new(|_msg| Ok(())),
            Box::new(|_timeout| Ok(None)),
        ));
        assert_eq!(conn.name(), "test_udc");
        assert_eq!(conn.connector_type(), "user_defined");
        let msg = conn.recv(100).unwrap();
        assert_eq!(msg, None);
    }
}
