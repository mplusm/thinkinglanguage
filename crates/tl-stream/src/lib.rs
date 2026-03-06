// ThinkingLanguage — Streaming & Pipeline Engine
// Licensed under Apache-2.0
//
// Phase 4: Provides streaming data processing, ETL pipelines,
// connectors, windowing, lineage tracking, alerting, and metrics.
// Phase 34: Agent definitions.

pub mod agent;
pub mod alert;
pub mod connector;
pub mod lineage;
pub mod metrics;
pub mod pipeline;
pub mod schedule;
pub mod stream;
pub mod window;

pub use agent::{AgentDef, AgentTool};
pub use alert::{AlertTarget, send_alert};
pub use connector::{ChannelConnector, Connector, ConnectorConfig, create_connector};
pub use lineage::{LineageNode, LineageTracker};
pub use metrics::MetricsRegistry;
pub use pipeline::{PipelineDef, PipelineResult, PipelineRunner, PipelineStatus};
pub use schedule::parse_duration;
pub use stream::{StreamDef, StreamEvent, StreamRunner};
pub use window::{WindowState, WindowType};
