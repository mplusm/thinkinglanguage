// ThinkingLanguage — Streaming & Pipeline Engine
// Licensed under MIT OR Apache-2.0
//
// Phase 4: Provides streaming data processing, ETL pipelines,
// connectors, windowing, lineage tracking, alerting, and metrics.

pub mod connector;
pub mod pipeline;
pub mod stream;
pub mod window;
pub mod lineage;
pub mod alert;
pub mod schedule;
pub mod metrics;

pub use connector::{Connector, ConnectorConfig, ChannelConnector, create_connector};
pub use pipeline::{PipelineDef, PipelineStatus, PipelineResult, PipelineRunner};
pub use stream::{StreamDef, StreamEvent, StreamRunner};
pub use window::{WindowType, WindowState};
pub use lineage::{LineageTracker, LineageNode};
pub use alert::{AlertTarget, send_alert};
pub use schedule::parse_duration;
pub use metrics::MetricsRegistry;
