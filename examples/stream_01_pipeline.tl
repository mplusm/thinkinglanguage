// Stream & Pipeline Example 01: Basic ETL Pipeline
// Demonstrates the pipeline construct with extract/transform/load blocks.

pipeline sales_etl {
    schedule: "0 * * * *",
    timeout: "30m",
    retries: 3,

    extract {
        let raw_data = [100, 200, 300, 400, 500]
        println("Extracted " + str(len(raw_data)) + " records")
    }

    transform {
        let cleaned = filter(raw_data, (x) => x > 150)
        let totals = map(cleaned, (x) => x * 1.1)
        println("Transformed " + str(len(totals)) + " records")
    }

    load {
        println("Loading data: " + str(totals))
    }

    on_success {
        println("Pipeline completed successfully!")
    }
}
