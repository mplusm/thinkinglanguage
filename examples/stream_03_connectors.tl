// Stream & Pipeline Example 03: Source/Sink Connector Declarations
// Demonstrates connector configuration for data sources and sinks.

// Define a channel source (in-memory, for testing)
source input = connector channel {
    buffer: 100
}

// Define a channel sink
sink output = connector channel {
    buffer: 100
}

// Use connectors in a pipeline
pipeline data_flow {
    extract {
        let data = [1, 2, 3, 4, 5]
    }
    transform {
        let processed = map(data, (x) => x * 10)
    }
    load {
        println("Processed: " + str(processed))
    }
}

// Emit a value (streaming primitive)
emit("pipeline_complete")
