# Streaming and Pipelines

TL provides first-class constructs for building ETL/ELT workflows and real-time stream processing.

## Pipeline Construct

Pipelines define declarative, schedulable data workflows with extract, transform, and load steps:

```tl
pipeline daily_etl {
    schedule: "0 6 * * *"
    steps {
        extract read_csv("input.csv")
        transform |> filter(status == "active") |> select(id, name)
        load write_parquet("output.parquet")
    }
}
```

- **schedule** -- cron expression for recurring execution
- **steps** -- ordered sequence of extract, transform, and load operations

## Stream Construct

Streams handle real-time data processing with windowing and watermark support:

```tl
stream process_events {
    from: events
    window: tumbling(5m)
    watermark: 30s
    process(batch) {
        batch |> aggregate(by: region, count: count()) |> emit
    }
}
```

## Window Types

- **tumbling** -- fixed-size, non-overlapping windows (e.g., `tumbling(5m)`)
- **sliding** -- overlapping windows with a slide interval
- **session** -- windows based on activity gaps

## Kafka Integration

**Feature flag:** `kafka`

Kafka source and sink connectors for reading from and writing to Kafka topics. Requires rdkafka and a running Kafka broker.

## Connectors

Streams use source and sink abstractions:

- **Source** -- input side, reading from files, databases, or message queues
- **Sink** -- output side, writing to files, databases, or message queues

## Data Lineage

Trace data flow through pipeline steps:

```sh
tl lineage file.tl
```

Output formats:

- **text** -- human-readable summary (default)
- **dot** -- Graphviz DOT format for visualization
- **json** -- machine-readable JSON

## Deploy

Generate deployment artifacts from pipeline definitions:

```sh
tl deploy file.tl --target docker
```

Targets:

- **docker** -- generates a Dockerfile
- **k8s** -- generates Kubernetes manifests

## Alerting and Metrics

Pipelines support error handlers and success callbacks for operational monitoring. Define handlers to receive notifications when pipeline steps fail or complete successfully.
