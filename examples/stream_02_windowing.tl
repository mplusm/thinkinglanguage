// Stream & Pipeline Example 02: Stream with Windowing
// Demonstrates stream declarations with tumbling windows.

source event_source = connector channel {
    buffer: 1000
}

stream click_events {
    source: event_source,
    window: tumbling(5m),
    watermark: "10s",
    transform: {
        let x = 1
    }
}

println("Stream 'click_events' configured with 5-minute tumbling window")
