// ThinkingLanguage — Pipeline definition and execution

use chrono::Utc;
use std::fmt;

/// Definition of a pipeline (ETL or general).
#[derive(Debug, Clone)]
pub struct PipelineDef {
    pub name: String,
    pub schedule: Option<String>,
    pub timeout_ms: Option<u64>,
    pub retries: u32,
}

impl fmt::Display for PipelineDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<pipeline {}>", self.name)
    }
}

/// Status of a pipeline execution.
#[derive(Debug, Clone, PartialEq)]
pub enum PipelineStatus {
    Running,
    Success,
    Failed(String),
    TimedOut,
}

impl fmt::Display for PipelineStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipelineStatus::Running => write!(f, "running"),
            PipelineStatus::Success => write!(f, "success"),
            PipelineStatus::Failed(msg) => write!(f, "failed: {msg}"),
            PipelineStatus::TimedOut => write!(f, "timed_out"),
        }
    }
}

/// Result of a pipeline execution.
#[derive(Debug, Clone)]
pub struct PipelineResult {
    pub name: String,
    pub status: PipelineStatus,
    pub started_at: String,
    pub ended_at: String,
    pub rows_processed: u64,
    pub attempts: u32,
}

impl fmt::Display for PipelineResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Pipeline '{}': {} (rows: {}, attempts: {}, duration: {} → {})",
            self.name,
            self.status,
            self.rows_processed,
            self.attempts,
            self.started_at,
            self.ended_at
        )
    }
}

/// Pipeline runner that executes extract/transform/load with retry logic.
pub struct PipelineRunner {
    pub def: PipelineDef,
}

impl PipelineRunner {
    pub fn new(def: PipelineDef) -> Self {
        PipelineRunner { def }
    }

    /// Execute the pipeline with retry logic.
    /// `run_fn` is called for each attempt — it should return Ok(rows) or Err(message).
    pub fn execute<F>(&self, mut run_fn: F) -> PipelineResult
    where
        F: FnMut(u32) -> Result<u64, String>,
    {
        let started_at = Utc::now().to_rfc3339();
        let max_attempts = self.def.retries + 1;

        for attempt in 1..=max_attempts {
            match run_fn(attempt) {
                Ok(rows) => {
                    return PipelineResult {
                        name: self.def.name.clone(),
                        status: PipelineStatus::Success,
                        started_at: started_at.clone(),
                        ended_at: Utc::now().to_rfc3339(),
                        rows_processed: rows,
                        attempts: attempt,
                    };
                }
                Err(msg) => {
                    if attempt == max_attempts {
                        return PipelineResult {
                            name: self.def.name.clone(),
                            status: PipelineStatus::Failed(msg),
                            started_at: started_at.clone(),
                            ended_at: Utc::now().to_rfc3339(),
                            rows_processed: 0,
                            attempts: attempt,
                        };
                    }
                    // retry
                }
            }
        }

        // Should not reach here, but just in case
        PipelineResult {
            name: self.def.name.clone(),
            status: PipelineStatus::Failed("exhausted retries".to_string()),
            started_at,
            ended_at: Utc::now().to_rfc3339(),
            rows_processed: 0,
            attempts: max_attempts,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_success() {
        let def = PipelineDef {
            name: "test_etl".to_string(),
            schedule: None,
            timeout_ms: None,
            retries: 0,
        };
        let runner = PipelineRunner::new(def);
        let result = runner.execute(|_| Ok(100));
        assert_eq!(result.status, PipelineStatus::Success);
        assert_eq!(result.rows_processed, 100);
        assert_eq!(result.attempts, 1);
    }

    #[test]
    fn test_pipeline_failure() {
        let def = PipelineDef {
            name: "failing".to_string(),
            schedule: None,
            timeout_ms: None,
            retries: 0,
        };
        let runner = PipelineRunner::new(def);
        let result = runner.execute(|_| Err("boom".to_string()));
        assert!(matches!(result.status, PipelineStatus::Failed(ref m) if m == "boom"));
        assert_eq!(result.attempts, 1);
    }

    #[test]
    fn test_pipeline_retry_then_success() {
        let def = PipelineDef {
            name: "retry_me".to_string(),
            schedule: None,
            timeout_ms: None,
            retries: 2,
        };
        let runner = PipelineRunner::new(def);
        let result = runner.execute(|attempt| {
            if attempt < 3 {
                Err("not yet".to_string())
            } else {
                Ok(50)
            }
        });
        assert_eq!(result.status, PipelineStatus::Success);
        assert_eq!(result.rows_processed, 50);
        assert_eq!(result.attempts, 3);
    }

    #[test]
    fn test_pipeline_retry_exhausted() {
        let def = PipelineDef {
            name: "always_fail".to_string(),
            schedule: None,
            timeout_ms: None,
            retries: 2,
        };
        let runner = PipelineRunner::new(def);
        let result = runner.execute(|_| Err("always fails".to_string()));
        assert!(matches!(result.status, PipelineStatus::Failed(_)));
        assert_eq!(result.attempts, 3);
    }

    #[test]
    fn test_pipeline_result_display() {
        let result = PipelineResult {
            name: "test".to_string(),
            status: PipelineStatus::Success,
            started_at: "2024-01-01T00:00:00Z".to_string(),
            ended_at: "2024-01-01T00:01:00Z".to_string(),
            rows_processed: 1000,
            attempts: 1,
        };
        let s = format!("{result}");
        assert!(s.contains("test"));
        assert!(s.contains("success"));
        assert!(s.contains("1000"));
    }
}
