// ThinkingLanguage — Kafka connector (feature-gated behind `kafka`)

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};

use super::{Connector, ConnectorConfig};

/// Kafka connector wrapping rdkafka StreamConsumer + FutureProducer.
pub struct KafkaConnector {
    name: String,
    topic: String,
    producer: FutureProducer,
    consumer: Arc<Mutex<StreamConsumer>>,
}

impl KafkaConnector {
    pub fn new(config: &ConnectorConfig) -> Result<Self, String> {
        let brokers = config
            .properties
            .get("brokers")
            .cloned()
            .unwrap_or_else(|| "localhost:9092".to_string());
        let topic = config
            .properties
            .get("topic")
            .cloned()
            .ok_or("Kafka connector requires 'topic' property")?;
        let group = config
            .properties
            .get("group")
            .cloned()
            .unwrap_or_else(|| format!("tl-{}", config.name));

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(|e| format!("Kafka producer creation failed: {e}"))?;

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", &group)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(|e| format!("Kafka consumer creation failed: {e}"))?;

        consumer
            .subscribe(&[&topic])
            .map_err(|e| format!("Kafka subscribe failed: {e}"))?;

        Ok(KafkaConnector {
            name: config.name.clone(),
            topic,
            producer,
            consumer: Arc::new(Mutex::new(consumer)),
        })
    }
}

impl Connector for KafkaConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn connector_type(&self) -> &str {
        "kafka"
    }

    fn send(&self, message: &str) -> Result<(), String> {
        let rt =
            tokio::runtime::Runtime::new().map_err(|e| format!("Failed to create runtime: {e}"))?;
        rt.block_on(async {
            let record = FutureRecord::to(&self.topic).payload(message).key("");
            self.producer
                .send(record, Duration::from_secs(5))
                .await
                .map_err(|(e, _)| format!("Kafka send failed: {e}"))?;
            Ok(())
        })
    }

    fn recv(&self, timeout_ms: u64) -> Result<Option<String>, String> {
        let consumer = self
            .consumer
            .lock()
            .map_err(|e| format!("Lock error: {e}"))?;
        let rt =
            tokio::runtime::Runtime::new().map_err(|e| format!("Failed to create runtime: {e}"))?;
        rt.block_on(async {
            match tokio::time::timeout(Duration::from_millis(timeout_ms), consumer.recv()).await {
                Ok(Ok(msg)) => {
                    let payload = msg.payload_view::<str>().unwrap_or(Ok("")).unwrap_or("");
                    Ok(Some(payload.to_string()))
                }
                Ok(Err(e)) => Err(format!("Kafka recv error: {e}")),
                Err(_) => Ok(None), // timeout
            }
        })
    }
}

/// Factory function for creating Kafka connectors.
pub fn create_kafka_connector(config: &ConnectorConfig) -> Result<Box<dyn Connector>, String> {
    Ok(Box::new(KafkaConnector::new(config)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore] // Requires a running Kafka broker
    fn test_kafka_connector_create() {
        let mut props = HashMap::new();
        props.insert("topic".to_string(), "test-topic".to_string());
        props.insert("brokers".to_string(), "localhost:9092".to_string());
        let config = ConnectorConfig {
            name: "test_kafka".to_string(),
            connector_type: "kafka".to_string(),
            properties: props,
        };
        let conn = KafkaConnector::new(&config);
        assert!(conn.is_ok());
    }

    #[test]
    #[ignore] // Requires a running Kafka broker
    fn test_kafka_send_recv() {
        let mut props = HashMap::new();
        props.insert("topic".to_string(), "tl-test".to_string());
        props.insert("brokers".to_string(), "localhost:9092".to_string());
        let config = ConnectorConfig {
            name: "test_kafka".to_string(),
            connector_type: "kafka".to_string(),
            properties: props,
        };
        let conn = KafkaConnector::new(&config).unwrap();
        conn.send("hello kafka").unwrap();
        let msg = conn.recv(5000).unwrap();
        assert!(msg.is_some());
        assert_eq!(msg.unwrap(), "hello kafka");
    }
}
