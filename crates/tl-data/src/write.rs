// ThinkingLanguage — Shared SQL write layer
// Licensed under MIT OR Apache-2.0
//
// Turns collected Arrow RecordBatches into SQL statements (CREATE + batched
// INSERT) for any SQL dialect. Networked write connectors (Postgres, MySQL,
// MSSQL, ClickHouse, …) implement `SqlDialect` and reuse `build_write_statements`
// so the row-extraction / value-rendering logic lives in exactly one place.
//
// Values are rendered as escaped SQL literals (not driver parameters) so the
// same path works uniformly across native drivers and REST/HTTP warehouses.

use datafusion::arrow::array::{Array, Float32Array, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;

/// How a write should treat an existing target table.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WriteMode {
    /// `CREATE TABLE IF NOT EXISTS` then insert (default).
    Create,
    /// Insert only; the table is assumed to exist.
    Append,
    /// `DROP TABLE IF EXISTS` + `CREATE TABLE` + insert.
    Overwrite,
}

impl WriteMode {
    /// Parse a mode string. Empty/`"create"` → Create.
    pub fn parse(s: &str) -> Result<WriteMode, String> {
        match s.trim().to_lowercase().as_str() {
            "" | "create" => Ok(WriteMode::Create),
            "append" => Ok(WriteMode::Append),
            "overwrite" | "replace" => Ok(WriteMode::Overwrite),
            other => Err(format!(
                "unknown write mode '{other}' (use create|append|overwrite)"
            )),
        }
    }
}

/// SQL dialect quirks. Defaults match ANSI SQL; override per database.
pub trait SqlDialect {
    /// Map an Arrow type to a column type for `CREATE TABLE`.
    fn column_type(&self, dt: &DataType) -> String;

    /// Quote an identifier (table/column name).
    fn quote_ident(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    /// Quote a string value. Default: single quotes, `''` escaping.
    fn quote_str(&self, s: &str) -> String {
        format!("'{}'", s.replace('\'', "''"))
    }

    /// Boolean literal rendering.
    fn bool_literal(&self, b: bool) -> String {
        if b {
            "TRUE".to_string()
        } else {
            "FALSE".to_string()
        }
    }

    /// Max rows per multi-row `INSERT` statement.
    fn max_rows_per_insert(&self) -> usize {
        1000
    }
}

/// Render one Arrow cell as a SQL literal for the given dialect.
fn render_cell(dialect: &dyn SqlDialect, array: &dyn Array, row: usize) -> Result<String, String> {
    if array.is_null(row) {
        return Ok("NULL".to_string());
    }
    match array.data_type() {
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                .ok_or("boolean downcast failed")?;
            Ok(dialect.bool_literal(a.value(row)))
        }
        // Integers and decimals render as bare numeric literals.
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => {
            array_value_to_string(array, row).map_err(|e| format!("value render error: {e}"))
        }
        // Floats: guard against NaN/Inf which have no portable SQL literal.
        DataType::Float32 => {
            let v = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or("f32 downcast failed")?
                .value(row);
            Ok(if v.is_finite() {
                v.to_string()
            } else {
                "NULL".to_string()
            })
        }
        DataType::Float64 => {
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("f64 downcast failed")?
                .value(row);
            Ok(if v.is_finite() {
                v.to_string()
            } else {
                "NULL".to_string()
            })
        }
        // Strings, dates, timestamps, etc. → quoted string literal.
        _ => {
            let s = array_value_to_string(array, row)
                .map_err(|e| format!("value render error: {e}"))?;
            Ok(dialect.quote_str(&s))
        }
    }
}

/// Build the full list of SQL statements to write `batches` into `table`.
/// Returns (in execution order): optional DROP, optional CREATE, then one or
/// more batched multi-row INSERTs.
pub fn build_write_statements(
    dialect: &dyn SqlDialect,
    table: &str,
    batches: &[RecordBatch],
    mode: WriteMode,
) -> Result<Vec<String>, String> {
    let schema = match batches.first() {
        Some(b) => b.schema(),
        None => return Ok(Vec::new()),
    };
    let qtable = dialect.quote_ident(table);
    let mut stmts: Vec<String> = Vec::new();

    // DDL
    let col_defs: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| {
            format!(
                "{} {}",
                dialect.quote_ident(f.name()),
                dialect.column_type(f.data_type())
            )
        })
        .collect();
    match mode {
        WriteMode::Overwrite => {
            stmts.push(format!("DROP TABLE IF EXISTS {qtable}"));
            stmts.push(format!("CREATE TABLE {qtable} ({})", col_defs.join(", ")));
        }
        WriteMode::Create => {
            stmts.push(format!(
                "CREATE TABLE IF NOT EXISTS {qtable} ({})",
                col_defs.join(", ")
            ));
        }
        WriteMode::Append => {}
    }

    // Column list for INSERT
    let col_list: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| dialect.quote_ident(f.name()))
        .collect();
    let col_list = col_list.join(", ");

    // INSERT rows, chunked into multi-row VALUES.
    let chunk = dialect.max_rows_per_insert().max(1);
    for batch in batches {
        let ncols = batch.num_columns();
        let mut row = 0;
        while row < batch.num_rows() {
            let end = (row + chunk).min(batch.num_rows());
            let mut value_groups: Vec<String> = Vec::with_capacity(end - row);
            for r in row..end {
                let mut cells: Vec<String> = Vec::with_capacity(ncols);
                for c in 0..ncols {
                    cells.push(render_cell(dialect, batch.column(c).as_ref(), r)?);
                }
                value_groups.push(format!("({})", cells.join(", ")));
            }
            stmts.push(format!(
                "INSERT INTO {qtable} ({col_list}) VALUES {}",
                value_groups.join(", ")
            ));
            row = end;
        }
    }
    Ok(stmts)
}

/// PostgreSQL / Redshift dialect.
pub struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn column_type(&self, dt: &DataType) -> String {
        match dt {
            DataType::Boolean => "BOOLEAN",
            DataType::Int8 | DataType::Int16 => "SMALLINT",
            DataType::Int32 | DataType::UInt8 | DataType::UInt16 => "INTEGER",
            DataType::Int64 | DataType::UInt32 | DataType::UInt64 => "BIGINT",
            DataType::Float32 => "REAL",
            DataType::Float64 => "DOUBLE PRECISION",
            DataType::Date32 | DataType::Date64 => "DATE",
            DataType::Timestamp(_, _) => "TIMESTAMP",
            DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
                return format!("NUMERIC({p}, {s})");
            }
            _ => "TEXT",
        }
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a'b"), None])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn mode_parsing() {
        assert_eq!(WriteMode::parse("").unwrap(), WriteMode::Create);
        assert_eq!(WriteMode::parse("APPEND").unwrap(), WriteMode::Append);
        assert_eq!(WriteMode::parse("overwrite").unwrap(), WriteMode::Overwrite);
        assert!(WriteMode::parse("nonsense").is_err());
    }

    #[test]
    fn create_mode_emits_create_and_insert() {
        let stmts =
            build_write_statements(&PostgresDialect, "t", &[sample_batch()], WriteMode::Create)
                .unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].starts_with("CREATE TABLE IF NOT EXISTS \"t\""));
        assert!(stmts[0].contains("\"id\" BIGINT"));
        assert!(stmts[0].contains("\"name\" TEXT"));
        // String escaping and NULL handling in VALUES.
        assert!(stmts[1].contains("(1, 'a''b')"));
        assert!(stmts[1].contains("(2, NULL)"));
    }

    #[test]
    fn overwrite_mode_drops_first() {
        let stmts = build_write_statements(
            &PostgresDialect,
            "t",
            &[sample_batch()],
            WriteMode::Overwrite,
        )
        .unwrap();
        assert!(stmts[0].starts_with("DROP TABLE IF EXISTS \"t\""));
        assert!(stmts[1].starts_with("CREATE TABLE \"t\""));
    }

    #[test]
    fn append_mode_is_insert_only() {
        let stmts =
            build_write_statements(&PostgresDialect, "t", &[sample_batch()], WriteMode::Append)
                .unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].starts_with("INSERT INTO \"t\""));
    }
}
