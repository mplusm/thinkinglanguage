// ThinkingLanguage — Data lineage tracking

use std::collections::HashMap;
use chrono::Utc;

/// A node in the data lineage graph.
#[derive(Debug, Clone)]
pub struct LineageNode {
    pub id: String,
    pub stage: String,
    pub operation: String,
    pub timestamp: String,
    pub row_count: Option<u64>,
    pub parent_ids: Vec<String>,
}

/// Tracks data lineage through pipeline stages.
#[derive(Debug, Clone, Default)]
pub struct LineageTracker {
    nodes: Vec<LineageNode>,
    node_map: HashMap<String, usize>,
    next_id: u64,
}

impl LineageTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a lineage node.
    pub fn record(
        &mut self,
        stage: &str,
        operation: &str,
        row_count: Option<u64>,
        parent_ids: Vec<String>,
    ) -> String {
        let id = format!("node_{}", self.next_id);
        self.next_id += 1;

        let node = LineageNode {
            id: id.clone(),
            stage: stage.to_string(),
            operation: operation.to_string(),
            timestamp: Utc::now().to_rfc3339(),
            row_count,
            parent_ids,
        };

        let idx = self.nodes.len();
        self.node_map.insert(id.clone(), idx);
        self.nodes.push(node);
        id
    }

    /// Get all lineage nodes.
    pub fn nodes(&self) -> &[LineageNode] {
        &self.nodes
    }

    /// Export lineage as DOT graph format.
    pub fn to_dot(&self) -> String {
        let mut dot = String::from("digraph lineage {\n");
        dot.push_str("  rankdir=LR;\n");
        dot.push_str("  node [shape=box];\n\n");

        for node in &self.nodes {
            let label = match node.row_count {
                Some(n) => format!("{}\\n{}\\n{} rows", node.stage, node.operation, n),
                None => format!("{}\\n{}", node.stage, node.operation),
            };
            dot.push_str(&format!("  {} [label=\"{}\"];\n", node.id, label));
        }

        dot.push('\n');

        for node in &self.nodes {
            for parent_id in &node.parent_ids {
                dot.push_str(&format!("  {} -> {};\n", parent_id, node.id));
            }
        }

        dot.push_str("}\n");
        dot
    }

    /// Export lineage as JSON.
    pub fn to_json(&self) -> String {
        let nodes: Vec<serde_json::Value> = self
            .nodes
            .iter()
            .map(|n| {
                serde_json::json!({
                    "id": n.id,
                    "stage": n.stage,
                    "operation": n.operation,
                    "timestamp": n.timestamp,
                    "row_count": n.row_count,
                    "parent_ids": n.parent_ids,
                })
            })
            .collect();
        serde_json::to_string_pretty(&serde_json::json!({ "lineage": nodes }))
            .unwrap_or_else(|_| "{}".to_string())
    }

    /// Export lineage as plain text.
    pub fn to_text(&self) -> String {
        let mut text = String::new();
        for node in &self.nodes {
            let rows = node
                .row_count
                .map(|n| format!(" ({n} rows)"))
                .unwrap_or_default();
            text.push_str(&format!(
                "[{}] {}: {}{}\n",
                node.id, node.stage, node.operation, rows
            ));
            for parent in &node.parent_ids {
                text.push_str(&format!("  <- {}\n", parent));
            }
        }
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lineage_record() {
        let mut tracker = LineageTracker::new();
        let id1 = tracker.record("extract", "read_csv", Some(1000), vec![]);
        let id2 = tracker.record("transform", "filter", Some(500), vec![id1.clone()]);
        let _id3 = tracker.record("load", "write_parquet", Some(500), vec![id2.clone()]);

        assert_eq!(tracker.nodes().len(), 3);
        assert_eq!(tracker.nodes()[0].stage, "extract");
        assert_eq!(tracker.nodes()[1].parent_ids, vec![id1]);
        assert_eq!(tracker.nodes()[2].parent_ids, vec![id2]);
    }

    #[test]
    fn test_lineage_dot_output() {
        let mut tracker = LineageTracker::new();
        let id1 = tracker.record("extract", "read_csv", Some(100), vec![]);
        tracker.record("transform", "filter", Some(50), vec![id1]);

        let dot = tracker.to_dot();
        assert!(dot.contains("digraph lineage"));
        assert!(dot.contains("node_0"));
        assert!(dot.contains("node_1"));
        assert!(dot.contains("node_0 -> node_1"));
        assert!(dot.contains("100 rows"));
    }

    #[test]
    fn test_lineage_json_output() {
        let mut tracker = LineageTracker::new();
        tracker.record("extract", "read_csv", Some(100), vec![]);

        let json = tracker.to_json();
        assert!(json.contains("\"lineage\""));
        assert!(json.contains("\"extract\""));
        assert!(json.contains("\"read_csv\""));
    }

    #[test]
    fn test_lineage_text_output() {
        let mut tracker = LineageTracker::new();
        let id1 = tracker.record("extract", "read_csv", Some(100), vec![]);
        tracker.record("transform", "filter", None, vec![id1]);

        let text = tracker.to_text();
        assert!(text.contains("[node_0] extract: read_csv (100 rows)"));
        assert!(text.contains("[node_1] transform: filter"));
        assert!(text.contains("<- node_0"));
    }
}
