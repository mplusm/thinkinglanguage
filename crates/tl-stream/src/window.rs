// ThinkingLanguage — Window types for stream processing

use std::fmt;

/// Window type with duration in milliseconds.
#[derive(Debug, Clone)]
pub enum WindowType {
    /// Fixed-size, non-overlapping window.
    Tumbling { duration_ms: u64 },
    /// Overlapping window with window size and slide interval.
    Sliding { window_ms: u64, slide_ms: u64 },
    /// Session window based on activity gap.
    Session { gap_ms: u64 },
}

impl fmt::Display for WindowType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WindowType::Tumbling { duration_ms } => write!(f, "tumbling({duration_ms}ms)"),
            WindowType::Sliding { window_ms, slide_ms } => {
                write!(f, "sliding({window_ms}ms, {slide_ms}ms)")
            }
            WindowType::Session { gap_ms } => write!(f, "session({gap_ms}ms)"),
        }
    }
}

/// State for a window, buffering events and determining when to fire.
#[derive(Debug)]
pub struct WindowState {
    pub window_type: WindowType,
    pub events: Vec<WindowEvent>,
    pub window_start: u64,
    pub last_event_time: u64,
}

/// An event in a window buffer.
#[derive(Debug, Clone)]
pub struct WindowEvent {
    pub value: String,
    pub timestamp: u64,
}

impl WindowState {
    pub fn new(window_type: WindowType) -> Self {
        WindowState {
            window_type,
            events: Vec::new(),
            window_start: 0,
            last_event_time: 0,
        }
    }

    /// Add an event to the window buffer.
    pub fn add_event(&mut self, value: String, timestamp: u64) {
        if self.events.is_empty() && self.window_start == 0 && self.last_event_time == 0 {
            // First event ever — initialize window start
            self.window_start = timestamp;
        }
        self.last_event_time = timestamp;
        self.events.push(WindowEvent { value, timestamp });
    }

    /// Check if the window should fire (emit results).
    pub fn should_fire(&self, current_time: u64) -> bool {
        if self.events.is_empty() {
            return false;
        }
        match &self.window_type {
            WindowType::Tumbling { duration_ms } => {
                current_time >= self.window_start + duration_ms
            }
            WindowType::Sliding { slide_ms, .. } => {
                current_time >= self.window_start + slide_ms
            }
            WindowType::Session { gap_ms } => {
                current_time >= self.last_event_time + gap_ms
            }
        }
    }

    /// Fire the window: drain events and reset.
    /// Returns the events that were in the window.
    pub fn fire(&mut self) -> Vec<WindowEvent> {
        let events = std::mem::take(&mut self.events);
        match &self.window_type {
            WindowType::Tumbling { duration_ms } => {
                self.window_start += duration_ms;
            }
            WindowType::Sliding { slide_ms, .. } => {
                // For sliding windows, keep events within the window
                self.window_start += slide_ms;
            }
            WindowType::Session { .. } => {
                self.window_start = 0;
                self.last_event_time = 0;
            }
        }
        events
    }

    /// Number of buffered events.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tumbling_window_fires_after_duration() {
        let mut state = WindowState::new(WindowType::Tumbling { duration_ms: 1000 });
        state.add_event("a".to_string(), 0);
        state.add_event("b".to_string(), 500);

        assert!(!state.should_fire(500));
        assert!(!state.should_fire(999));
        assert!(state.should_fire(1000));
        assert!(state.should_fire(1500));

        let events = state.fire();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].value, "a");
        assert_eq!(events[1].value, "b");
        assert!(state.is_empty());
    }

    #[test]
    fn test_tumbling_window_resets_after_fire() {
        let mut state = WindowState::new(WindowType::Tumbling { duration_ms: 1000 });
        state.add_event("a".to_string(), 0);
        assert!(state.should_fire(1000));
        state.fire();

        // New window starts at 1000
        state.add_event("b".to_string(), 1200);
        assert!(!state.should_fire(1500));
        assert!(state.should_fire(2000));
    }

    #[test]
    fn test_sliding_window_fires_on_slide() {
        let mut state = WindowState::new(WindowType::Sliding {
            window_ms: 1000,
            slide_ms: 500,
        });
        state.add_event("a".to_string(), 0);
        state.add_event("b".to_string(), 300);

        assert!(!state.should_fire(400));
        assert!(state.should_fire(500));
    }

    #[test]
    fn test_session_window_fires_on_gap() {
        let mut state = WindowState::new(WindowType::Session { gap_ms: 500 });
        state.add_event("a".to_string(), 0);
        state.add_event("b".to_string(), 200);

        assert!(!state.should_fire(600));
        assert!(state.should_fire(700));
        assert!(state.should_fire(800));

        let events = state.fire();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_empty_window_never_fires() {
        let state = WindowState::new(WindowType::Tumbling { duration_ms: 1000 });
        assert!(!state.should_fire(5000));
    }

    #[test]
    fn test_window_type_display() {
        assert_eq!(
            format!("{}", WindowType::Tumbling { duration_ms: 5000 }),
            "tumbling(5000ms)"
        );
        assert_eq!(
            format!("{}", WindowType::Sliding { window_ms: 10000, slide_ms: 1000 }),
            "sliding(10000ms, 1000ms)"
        );
        assert_eq!(
            format!("{}", WindowType::Session { gap_ms: 30000 }),
            "session(30000ms)"
        );
    }
}
