// ThinkingLanguage — Stream processing definitions

use crate::window::{WindowEvent, WindowState, WindowType};
use std::fmt;

/// Definition of a stream processor.
#[derive(Debug, Clone)]
pub struct StreamDef {
    pub name: String,
    pub window: Option<WindowType>,
    pub watermark_ms: Option<u64>,
}

impl fmt::Display for StreamDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.window {
            Some(w) => write!(f, "<stream {} window={}>", self.name, w),
            None => write!(f, "<stream {}>", self.name),
        }
    }
}

/// A streaming event with key, value, and timestamp.
#[derive(Debug, Clone)]
pub struct StreamEvent {
    pub key: Option<String>,
    pub value: String,
    pub timestamp: u64,
}

/// Stream runner that processes events through optional windowing.
pub struct StreamRunner {
    pub def: StreamDef,
    window_state: Option<WindowState>,
    events_processed: u64,
}

impl StreamRunner {
    pub fn new(def: StreamDef) -> Self {
        let window_state = def.window.as_ref().map(|w| WindowState::new(w.clone()));
        StreamRunner {
            def,
            window_state,
            events_processed: 0,
        }
    }

    /// Process a single event. Returns any window outputs.
    pub fn process_event(&mut self, event: StreamEvent) -> Vec<Vec<WindowEvent>> {
        self.events_processed += 1;
        let mut outputs = Vec::new();

        if let Some(ref mut state) = self.window_state {
            state.add_event(event.value, event.timestamp);
            if state.should_fire(event.timestamp) {
                outputs.push(state.fire());
            }
        }
        // If no window, events pass through immediately (handled by caller)

        outputs
    }

    /// Force-flush any remaining window state.
    pub fn flush(&mut self) -> Option<Vec<WindowEvent>> {
        if let Some(ref mut state) = self.window_state
            && !state.is_empty()
        {
            return Some(state.fire());
        }
        None
    }

    /// Number of events processed so far.
    pub fn events_processed(&self) -> u64 {
        self.events_processed
    }

    /// Whether this stream has windowing enabled.
    pub fn has_window(&self) -> bool {
        self.window_state.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::window::WindowType;

    #[test]
    fn test_stream_runner_no_window() {
        let def = StreamDef {
            name: "passthrough".to_string(),
            window: None,
            watermark_ms: None,
        };
        let mut runner = StreamRunner::new(def);
        assert!(!runner.has_window());

        let outputs = runner.process_event(StreamEvent {
            key: None,
            value: "hello".to_string(),
            timestamp: 0,
        });
        assert!(outputs.is_empty()); // no window, no buffered output
        assert_eq!(runner.events_processed(), 1);
    }

    #[test]
    fn test_stream_runner_tumbling_window() {
        let def = StreamDef {
            name: "windowed".to_string(),
            window: Some(WindowType::Tumbling { duration_ms: 1000 }),
            watermark_ms: None,
        };
        let mut runner = StreamRunner::new(def);
        assert!(runner.has_window());

        // Add events within window
        let out1 = runner.process_event(StreamEvent {
            key: None,
            value: "a".to_string(),
            timestamp: 0,
        });
        assert!(out1.is_empty());

        let out2 = runner.process_event(StreamEvent {
            key: None,
            value: "b".to_string(),
            timestamp: 500,
        });
        assert!(out2.is_empty());

        // Event at window boundary triggers fire
        let out3 = runner.process_event(StreamEvent {
            key: None,
            value: "c".to_string(),
            timestamp: 1000,
        });
        assert_eq!(out3.len(), 1);
        assert_eq!(out3[0].len(), 3); // a, b, c
    }

    #[test]
    fn test_stream_runner_flush() {
        let def = StreamDef {
            name: "flush_test".to_string(),
            window: Some(WindowType::Tumbling { duration_ms: 5000 }),
            watermark_ms: None,
        };
        let mut runner = StreamRunner::new(def);

        runner.process_event(StreamEvent {
            key: None,
            value: "x".to_string(),
            timestamp: 100,
        });

        // Flush before window fires
        let flushed = runner.flush();
        assert!(flushed.is_some());
        assert_eq!(flushed.unwrap().len(), 1);

        // Second flush is empty
        let flushed2 = runner.flush();
        assert!(flushed2.is_none());
    }

    #[test]
    fn test_stream_def_display() {
        let def = StreamDef {
            name: "test_stream".to_string(),
            window: Some(WindowType::Tumbling { duration_ms: 5000 }),
            watermark_ms: None,
        };
        let s = format!("{def}");
        assert!(s.contains("test_stream"));
        assert!(s.contains("tumbling"));
    }
}
