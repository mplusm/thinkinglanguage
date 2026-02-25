use crate::engine::DataEngine;
use datafusion::prelude::*;
use std::path::Path;

impl DataEngine {
    /// Read a CSV file into a DataFusion DataFrame.
    pub fn read_csv(&self, path: &str) -> Result<DataFrame, String> {
        let path = Path::new(path);
        if !path.exists() {
            return Err(format!("CSV file not found: {}", path.display()));
        }
        self.rt
            .block_on(self.ctx.read_csv(path.to_str().unwrap(), CsvReadOptions::default()))
            .map_err(|e| format!("CSV read error: {e}"))
    }

    /// Read a Parquet file into a DataFusion DataFrame.
    pub fn read_parquet(&self, path: &str) -> Result<DataFrame, String> {
        let path = Path::new(path);
        if !path.exists() {
            return Err(format!("Parquet file not found: {}", path.display()));
        }
        self.rt
            .block_on(
                self.ctx
                    .read_parquet(path.to_str().unwrap(), ParquetReadOptions::default()),
            )
            .map_err(|e| format!("Parquet read error: {e}"))
    }

    /// Write a DataFrame to a CSV file.
    pub fn write_csv(&self, df: DataFrame, path: &str) -> Result<(), String> {
        self.rt
            .block_on(
                df.write_csv(path, datafusion::dataframe::DataFrameWriteOptions::default(), None),
            )
            .map_err(|e| format!("CSV write error: {e}"))?;
        Ok(())
    }

    /// Write a DataFrame to a Parquet file.
    pub fn write_parquet(&self, df: DataFrame, path: &str) -> Result<(), String> {
        self.rt
            .block_on(df.write_parquet(
                path,
                datafusion::dataframe::DataFrameWriteOptions::default(),
                None,
            ))
            .map_err(|e| format!("Parquet write error: {e}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_csv_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let csv_path = dir.path().join("test.csv");

        // Write a test CSV
        fs::write(&csv_path, "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n").unwrap();

        let engine = DataEngine::new();
        let df = engine.read_csv(csv_path.to_str().unwrap()).unwrap();

        // Verify read
        let batches = engine.collect(df).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        // Write back
        let df = engine.read_csv(csv_path.to_str().unwrap()).unwrap();
        let out_dir = dir.path().join("output");
        engine.write_csv(df, out_dir.to_str().unwrap()).unwrap();

        // Verify output directory was created
        assert!(out_dir.exists());
    }
}
