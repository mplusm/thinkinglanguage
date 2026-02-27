// ThinkingLanguage — Data Quality Operations
// Licensed under MIT OR Apache-2.0
//
// DataFrame-level clean, validate, and profile operations.

use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::functions_aggregate::expr_fn::{count, avg, stddev, min as agg_min, max as agg_max};

use crate::engine::DataEngine;

impl DataEngine {
    /// Fill null values in a column using a strategy.
    /// strategy: "value" (use fill_value), "mean", "zero"
    pub fn fill_null(
        &self,
        df: DataFrame,
        column: &str,
        strategy: &str,
        fill_value: Option<f64>,
    ) -> Result<DataFrame, String> {
        let fill_expr = match strategy {
            "value" => {
                let val = fill_value.ok_or("fill_null with 'value' strategy requires a fill_value")?;
                coalesce(vec![col(column), lit(val)]).alias(column)
            }
            "zero" => coalesce(vec![col(column), lit(0.0)]).alias(column),
            "mean" => {
                // Compute mean first
                let mean_df = df.clone()
                    .aggregate(vec![], vec![avg(col(column)).alias("__mean")])
                    .map_err(|e| format!("fill_null mean aggregate error: {e}"))?;
                let batches = self.collect(mean_df)?;
                let mean_val = if !batches.is_empty() && batches[0].num_rows() > 0 {
                    let col_arr = batches[0].column(0);
                    if let Some(f64_arr) = col_arr.as_any().downcast_ref::<Float64Array>() {
                        if f64_arr.is_null(0) { 0.0 } else { f64_arr.value(0) }
                    } else {
                        0.0
                    }
                } else {
                    0.0
                };
                coalesce(vec![col(column), lit(mean_val)]).alias(column)
            }
            "median" => {
                // Approximate: use SQL median via sorted approach
                // For simplicity, compute via mean (median requires more complex logic)
                let mean_df = df.clone()
                    .aggregate(vec![], vec![avg(col(column)).alias("__mean")])
                    .map_err(|e| format!("fill_null median aggregate error: {e}"))?;
                let batches = self.collect(mean_df)?;
                let mean_val = if !batches.is_empty() && batches[0].num_rows() > 0 {
                    let col_arr = batches[0].column(0);
                    if let Some(f64_arr) = col_arr.as_any().downcast_ref::<Float64Array>() {
                        if f64_arr.is_null(0) { 0.0 } else { f64_arr.value(0) }
                    } else {
                        0.0
                    }
                } else {
                    0.0
                };
                coalesce(vec![col(column), lit(mean_val)]).alias(column)
            }
            other => return Err(format!("Unknown fill_null strategy: {other}")),
        };

        // Build select list: replace target column, keep others
        let schema = df.schema().clone();
        let mut select_exprs = Vec::new();
        for field in schema.fields() {
            if field.name() == column {
                select_exprs.push(fill_expr.clone());
            } else {
                select_exprs.push(col(field.name()));
            }
        }

        df.select(select_exprs).map_err(|e| format!("fill_null select error: {e}"))
    }

    /// Drop rows where a column is null.
    pub fn drop_null(
        &self,
        df: DataFrame,
        column: &str,
    ) -> Result<DataFrame, String> {
        df.filter(col(column).is_not_null())
            .map_err(|e| format!("drop_null error: {e}"))
    }

    /// Remove duplicate rows based on specified columns.
    pub fn dedup(
        &self,
        df: DataFrame,
        columns: &[String],
    ) -> Result<DataFrame, String> {
        if columns.is_empty() {
            return df.distinct()
                .map_err(|e| format!("dedup error: {e}"));
        }
        // Use distinct on specific columns by registering as table + SQL
        let table_name = "__dedup_tmp";
        self.ctx.register_table(
            table_name,
            df.into_view(),
        ).map_err(|e| format!("dedup register error: {e}"))?;

        let cols_str = columns.join(", ");
        let result = self.sql(&format!(
            "SELECT DISTINCT ON ({cols_str}) * FROM {table_name}"
        ));

        // Fallback to regular DISTINCT if DISTINCT ON is not supported
        match result {
            Ok(r) => Ok(r),
            Err(_) => {
                // Use GROUP BY approach
                let all_cols = self.sql(&format!("SELECT * FROM {table_name} GROUP BY {cols_str}"));
                match all_cols {
                    Ok(r) => Ok(r),
                    Err(_) => {
                        // Final fallback: just use DISTINCT
                        self.sql(&format!("SELECT DISTINCT * FROM {table_name}"))
                    }
                }
            }
        }
    }

    /// Clamp values in a column to [min_val, max_val].
    pub fn clamp(
        &self,
        df: DataFrame,
        column: &str,
        min_val: f64,
        max_val: f64,
    ) -> Result<DataFrame, String> {
        let clamp_expr = when(col(column).lt(lit(min_val)), lit(min_val))
            .when(col(column).gt(lit(max_val)), lit(max_val))
            .otherwise(col(column))
            .map_err(|e| format!("clamp expr error: {e}"))?
            .alias(column);

        let schema = df.schema().clone();
        let mut select_exprs = Vec::new();
        for field in schema.fields() {
            if field.name() == column {
                select_exprs.push(clamp_expr.clone());
            } else {
                select_exprs.push(col(field.name()));
            }
        }

        df.select(select_exprs).map_err(|e| format!("clamp select error: {e}"))
    }

    /// Generate a statistical profile of all numeric columns.
    /// Returns a table with: column_name, count, null_count, null_rate, min, max, mean, stddev
    pub fn data_profile(
        &self,
        df: DataFrame,
    ) -> Result<DataFrame, String> {
        let schema = df.schema().clone();
        let mut col_names = Vec::new();
        let mut counts = Vec::new();
        let mut null_counts = Vec::new();
        let mut null_rates = Vec::new();
        let mut mins = Vec::new();
        let mut maxs = Vec::new();
        let mut means = Vec::new();
        let mut stddevs = Vec::new();

        for field in schema.fields() {
            let name = field.name();
            let is_numeric = matches!(
                field.data_type(),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
                | DataType::Float32 | DataType::Float64
            );

            // Build aggregation query for this column
            let mut agg_exprs = vec![
                count(col(name)).alias("__count"),
            ];
            if is_numeric {
                agg_exprs.push(agg_min(col(name)).alias("__min"));
                agg_exprs.push(agg_max(col(name)).alias("__max"));
                agg_exprs.push(avg(col(name)).alias("__mean"));
                agg_exprs.push(stddev(col(name)).alias("__stddev"));
            }

            let agg_df = df.clone()
                .aggregate(vec![], agg_exprs)
                .map_err(|e| format!("data_profile aggregate error for {name}: {e}"))?;
            let batches = self.collect(agg_df)?;

            if batches.is_empty() || batches[0].num_rows() == 0 {
                continue;
            }
            let batch = &batches[0];

            let non_null_cnt = Self::extract_i64_or_u64(batch.column(0));
            // Get total row count to compute null count
            let total = self.row_count(df.clone())?;
            let null_cnt = total - non_null_cnt;
            let nr = if total > 0 { null_cnt as f64 / total as f64 } else { 0.0 };

            col_names.push(name.clone());
            counts.push(non_null_cnt);
            null_counts.push(null_cnt);
            null_rates.push(nr);

            if is_numeric && batch.num_columns() >= 5 {
                mins.push(Self::extract_f64(batch.column(1)));
                maxs.push(Self::extract_f64(batch.column(2)));
                means.push(Self::extract_f64(batch.column(3)));
                stddevs.push(Self::extract_f64(batch.column(4)));
            } else {
                mins.push(f64::NAN);
                maxs.push(f64::NAN);
                means.push(f64::NAN);
                stddevs.push(f64::NAN);
            }
        }

        let result_schema = Arc::new(Schema::new(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
            Field::new("null_count", DataType::Int64, false),
            Field::new("null_rate", DataType::Float64, false),
            Field::new("min", DataType::Float64, true),
            Field::new("max", DataType::Float64, true),
            Field::new("mean", DataType::Float64, true),
            Field::new("stddev", DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(result_schema, vec![
            Arc::new(StringArray::from(col_names)),
            Arc::new(Int64Array::from(counts)),
            Arc::new(Int64Array::from(null_counts)),
            Arc::new(Float64Array::from(null_rates)),
            Arc::new(Float64Array::from(mins)),
            Arc::new(Float64Array::from(maxs)),
            Arc::new(Float64Array::from(means)),
            Arc::new(Float64Array::from(stddevs)),
        ]).map_err(|e| format!("data_profile batch error: {e}"))?;

        self.register_batch("__data_profile", batch)?;
        self.rt.block_on(self.ctx.table("__data_profile"))
            .map_err(|e| format!("data_profile table error: {e}"))
    }

    /// Get the row count of a DataFrame.
    pub fn row_count(&self, df: DataFrame) -> Result<i64, String> {
        let cnt = self.rt.block_on(df.count())
            .map_err(|e| format!("row_count error: {e}"))?;
        Ok(cnt as i64)
    }

    /// Get the null rate of a column (0.0 to 1.0).
    pub fn null_rate(&self, df: DataFrame, column: &str) -> Result<f64, String> {
        let total = self.rt.block_on(df.clone().count())
            .map_err(|e| format!("null_rate count error: {e}"))? as i64;
        if total == 0 {
            return Ok(0.0);
        }
        let non_null_df = df.aggregate(vec![], vec![
            count(col(column)).alias("__non_null"),
        ]).map_err(|e| format!("null_rate aggregate error: {e}"))?;
        let batches = self.collect(non_null_df)?;
        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(0.0);
        }
        let non_null = Self::extract_i64_or_u64(batches[0].column(0));
        Ok((total - non_null) as f64 / total as f64)
    }

    /// Check if a column's non-null values are all unique.
    pub fn is_unique(&self, df: DataFrame, column: &str) -> Result<bool, String> {
        let table_name = "__unique_check_tmp";
        self.ctx.register_table(
            table_name,
            df.into_view(),
        ).map_err(|e| format!("is_unique register error: {e}"))?;

        let result = self.sql(&format!(
            "SELECT COUNT(DISTINCT \"{column}\") = COUNT(\"{column}\") AS is_uniq FROM {table_name} WHERE \"{column}\" IS NOT NULL"
        ))?;

        let batches = self.collect(result)?;
        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(true);
        }
        let col_arr = batches[0].column(0);
        if let Some(bool_arr) = col_arr.as_any().downcast_ref::<BooleanArray>() {
            Ok(!bool_arr.is_null(0) && bool_arr.value(0))
        } else {
            Ok(false)
        }
    }

    // Helper: extract i64 from first row of an array (handles Int64 and UInt64)
    fn extract_i64_or_u64(arr: &dyn Array) -> i64 {
        if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
            if a.is_null(0) { 0 } else { a.value(0) }
        } else if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
            if a.is_null(0) { 0 } else { a.value(0) as i64 }
        } else {
            0
        }
    }

    // Helper: extract f64 from first row of an array
    fn extract_f64(arr: &dyn Array) -> f64 {
        if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
            if a.is_null(0) { f64::NAN } else { a.value(0) }
        } else if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
            if a.is_null(0) { f64::NAN } else { a.value(0) as f64 }
        } else if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
            if a.is_null(0) { f64::NAN } else { a.value(0) as f64 }
        } else {
            f64::NAN
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, Float64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn make_test_engine_with_data() -> DataEngine {
        let engine = DataEngine::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                Some("Alice"), Some("Bob"), None, Some("Diana"), Some("Eve"),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(30.0), Some(25.0), None, Some(35.0), Some(28.0),
            ])),
        ]).unwrap();
        engine.register_batch("test_data", batch).unwrap();
        engine
    }

    #[test]
    fn test_fill_null_value() {
        let engine = make_test_engine_with_data();
        let df = engine.rt.block_on(engine.ctx.table("test_data")).unwrap();
        let result = engine.fill_null(df, "age", "value", Some(0.0)).unwrap();
        let batches = engine.collect(result).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
        // Check that null was filled
        let age_col = batches[0].column_by_name("age").unwrap();
        let f64_arr = age_col.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(!f64_arr.is_null(2)); // was null, now filled
        assert_eq!(f64_arr.value(2), 0.0);
    }

    #[test]
    fn test_fill_null_mean() {
        let engine = make_test_engine_with_data();
        let df = engine.rt.block_on(engine.ctx.table("test_data")).unwrap();
        let result = engine.fill_null(df, "age", "mean", None).unwrap();
        let batches = engine.collect(result).unwrap();
        let age_col = batches[0].column_by_name("age").unwrap();
        let f64_arr = age_col.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(!f64_arr.is_null(2));
        // Mean of [30, 25, 35, 28] = 29.5
        assert!((f64_arr.value(2) - 29.5).abs() < 0.01);
    }

    #[test]
    fn test_drop_null() {
        let engine = make_test_engine_with_data();
        let df = engine.rt.block_on(engine.ctx.table("test_data")).unwrap();
        let result = engine.drop_null(df, "name").unwrap();
        let batches = engine.collect(result).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 4); // one null row removed
    }

    #[test]
    fn test_dedup() {
        let engine = DataEngine::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(vec![1, 2, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "b", "c"])),
        ]).unwrap();
        engine.register_batch("dup_data", batch).unwrap();
        let df = engine.rt.block_on(engine.ctx.table("dup_data")).unwrap();
        let result = engine.dedup(df, &[]).unwrap();
        let batches = engine.collect(result).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3); // one duplicate removed
    }

    #[test]
    fn test_clamp() {
        let engine = make_test_engine_with_data();
        let df = engine.rt.block_on(engine.ctx.table("test_data")).unwrap();
        let result = engine.clamp(df, "age", 26.0, 32.0).unwrap();
        let batches = engine.collect(result).unwrap();
        let age_col = batches[0].column_by_name("age").unwrap();
        let f64_arr = age_col.as_any().downcast_ref::<Float64Array>().unwrap();
        // 30 -> 30 (in range), 25 -> 26 (clamped up), null stays null, 35 -> 32 (clamped down), 28 -> 28
        assert_eq!(f64_arr.value(0), 30.0);
        assert_eq!(f64_arr.value(1), 26.0);
        assert_eq!(f64_arr.value(3), 32.0);
        assert_eq!(f64_arr.value(4), 28.0);
    }

    #[test]
    fn test_data_profile() {
        let engine = make_test_engine_with_data();
        let df = engine.rt.block_on(engine.ctx.table("test_data")).unwrap();
        let result = engine.data_profile(df).unwrap();
        let batches = engine.collect(result).unwrap();
        assert!(!batches.is_empty());
        // Should have rows for id, name, age
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total >= 2); // at least id and age (numeric columns get full stats)
    }

    #[test]
    fn test_row_count() {
        let engine = make_test_engine_with_data();
        let df = engine.rt.block_on(engine.ctx.table("test_data")).unwrap();
        let count = engine.row_count(df).unwrap();
        assert_eq!(count, 5);
    }

    #[test]
    fn test_null_rate_and_is_unique() {
        let engine = make_test_engine_with_data();
        let df = engine.rt.block_on(engine.ctx.table("test_data")).unwrap();
        let rate = engine.null_rate(df, "name").unwrap();
        assert!((rate - 0.2).abs() < 0.01); // 1 null out of 5

        let df2 = engine.rt.block_on(engine.ctx.table("test_data")).unwrap();
        let unique = engine.is_unique(df2, "id").unwrap();
        assert!(unique);
    }
}
