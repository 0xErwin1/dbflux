//! Parser for InfluxDB v2's annotated CSV (Flux query response format).
//!
//! The annotated CSV format looks like:
//! ```text
//! #datatype,string,long,dateTime:RFC3339,double,string,string
//! #group,false,false,false,false,true,true
//! #default,_result,,,,,
//! ,result,table,_time,_value,_field,_measurement
//! ,_result,0,2024-01-01T00:00:00Z,0.5,usage_idle,cpu
//! ```
//!
//! Multiple tables are separated by blank lines. Each table begins with a fresh
//! set of annotation rows (`#datatype`, `#group`, `#default`). When annotations
//! are missing, all columns default to string type.
//!
//! Supported `#datatype` values:
//! - `string` → `Value::Text`
//! - `long`, `unsignedLong` → `Value::Int`
//! - `double` → `Value::Float`
//! - `boolean` → `Value::Bool`
//! - `dateTime:RFC3339`, `dateTime:RFC3339Nano` → `Value::Text` (ISO timestamp)

use csv::ReaderBuilder;
use dbflux_core::{ColumnMeta, Value};

use super::{ParseError, build_query_result, parse_typed_value};
use dbflux_core::QueryResult;

const ANNOTATION_DATATYPE: &str = "#datatype";
const ANNOTATION_GROUP: &str = "#group";
const ANNOTATION_DEFAULT: &str = "#default";
const ANNOTATION_PREFIX: char = '#';

/// State machine for parsing a single Flux annotated-CSV table block.
#[derive(Default)]
struct TableState {
    /// Parsed `#datatype` row values (one per column, index 0 = empty/annotation marker).
    datatypes: Vec<String>,
    /// Parsed header row (column names), index 0 is the annotation marker column.
    headers: Vec<String>,
    /// Whether annotations were present (if false, all columns default to string).
    has_annotations: bool,
}

impl TableState {
    fn column_count(&self) -> usize {
        // Skip the leading empty annotation-marker column (index 0).
        self.headers.len().saturating_sub(1)
    }

    /// Map a datatype string to a type label understood by `parse_typed_value`.
    fn map_datatype(datatype: &str) -> &'static str {
        match datatype {
            "long" | "unsignedLong" => "integer",
            "double" => "float",
            "boolean" => "boolean",
            dt if dt.starts_with("dateTime") => "datetime",
            _ => "text",
        }
    }
}

/// Parse the annotated CSV body from a Flux query response.
///
/// Returns a flattened `QueryResult` from all tables in the response.
/// Tables are distinguished by the `result` and `table` annotation columns
/// (if present); these columns are included as-is in the output.
pub fn parse_flux_csv(body: &str) -> Result<QueryResult, ParseError> {
    if body.trim().is_empty() {
        return Ok(QueryResult::empty());
    }

    let mut all_columns: Vec<ColumnMeta> = Vec::new();
    let mut all_rows: Vec<Vec<Value>> = Vec::new();
    let mut columns_initialized = false;

    // Split on blank lines to get per-table blocks.
    let blocks = split_into_blocks(body);

    for block in &blocks {
        if block.trim().is_empty() {
            continue;
        }

        let (state, data_lines) = parse_table_block(block)?;

        if state.headers.is_empty() {
            continue;
        }

        if !columns_initialized && !data_lines.is_empty() {
            all_columns = build_column_meta(&state);
            columns_initialized = true;
        }

        let rows = parse_data_rows(&state, &data_lines)?;
        all_rows.extend(rows);
    }

    Ok(build_query_result(all_columns, all_rows))
}

// ---------------------------------------------------------------------------
// Block splitting
// ---------------------------------------------------------------------------

/// Split the full CSV body into table blocks separated by blank lines.
fn split_into_blocks(body: &str) -> Vec<String> {
    let mut blocks: Vec<String> = Vec::new();
    let mut current = String::new();

    for line in body.lines() {
        if line.trim().is_empty() {
            if !current.trim().is_empty() {
                blocks.push(current.clone());
                current.clear();
            }
        } else {
            if !current.is_empty() {
                current.push('\n');
            }
            current.push_str(line);
        }
    }

    if !current.trim().is_empty() {
        blocks.push(current);
    }

    blocks
}

// ---------------------------------------------------------------------------
// Table-block parsing
// ---------------------------------------------------------------------------

/// Parse annotation rows and header from a single table block.
///
/// Returns `(TableState, Vec<data_line_strings>)`.
fn parse_table_block(block: &str) -> Result<(TableState, Vec<String>), ParseError> {
    let mut state = TableState::default();
    let mut data_lines: Vec<String> = Vec::new();
    let mut header_found = false;

    let lines: Vec<&str> = block.lines().collect();

    for line in &lines {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if trimmed.starts_with(ANNOTATION_PREFIX) {
            // Annotation row.
            let cells = split_csv_line(trimmed);
            let tag = cells.first().map(|s| s.as_str()).unwrap_or("");

            match tag {
                ANNOTATION_DATATYPE => {
                    state.datatypes = cells;
                    state.has_annotations = true;
                }
                ANNOTATION_GROUP | ANNOTATION_DEFAULT => {
                    // We read these but don't need them for basic parsing.
                }
                _ => {}
            }
        } else if !header_found {
            // First non-annotation line is the header.
            state.headers = split_csv_line(trimmed);
            header_found = true;
        } else {
            // Data row.
            data_lines.push(trimmed.to_string());
        }
    }

    Ok((state, data_lines))
}

// ---------------------------------------------------------------------------
// Column meta construction
// ---------------------------------------------------------------------------

fn build_column_meta(state: &TableState) -> Vec<ColumnMeta> {
    let col_count = state.column_count();
    let mut columns = Vec::with_capacity(col_count);

    for i in 1..=col_count {
        let name = state
            .headers
            .get(i)
            .cloned()
            .unwrap_or_else(|| format!("col_{i}"));

        let type_name = if state.has_annotations {
            let raw_dt = state
                .datatypes
                .get(i)
                .map(|s| s.as_str())
                .unwrap_or("string");
            TableState::map_datatype(raw_dt).to_string()
        } else {
            "text".to_string()
        };

        columns.push(ColumnMeta {
            name,
            type_name,
            nullable: true,
            is_primary_key: false,
        });
    }

    columns
}

// ---------------------------------------------------------------------------
// Data row parsing
// ---------------------------------------------------------------------------

fn parse_data_rows(
    state: &TableState,
    data_lines: &[String],
) -> Result<Vec<Vec<Value>>, ParseError> {
    let col_count = state.column_count();
    let mut rows = Vec::with_capacity(data_lines.len());

    for line in data_lines {
        let cells = split_csv_line(line);

        let mut row = Vec::with_capacity(col_count);
        for i in 1..=col_count {
            let raw = cells.get(i).map(|s| s.as_str()).unwrap_or("");

            let type_name = if state.has_annotations {
                let raw_dt = state
                    .datatypes
                    .get(i)
                    .map(|s| s.as_str())
                    .unwrap_or("string");
                TableState::map_datatype(raw_dt)
            } else {
                "text"
            };

            // datetime values stay as text (ISO 8601 strings).
            let value = if type_name == "datetime" {
                if raw.is_empty() {
                    Value::Null
                } else {
                    Value::Text(raw.to_string())
                }
            } else {
                parse_typed_value(raw, type_name)
            };

            row.push(value);
        }

        rows.push(row);
    }

    Ok(rows)
}

// ---------------------------------------------------------------------------
// CSV line splitting
// ---------------------------------------------------------------------------

/// Split a single CSV line into a `Vec<String>`, respecting quoted fields.
fn split_csv_line(line: &str) -> Vec<String> {
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(line.as_bytes());

    match reader.records().next() {
        Some(Ok(record)) => record.iter().map(|s| s.to_string()).collect(),
        _ => {
            // Fallback: naive comma split (handles annotation rows).
            line.split(',').map(|s| s.to_string()).collect()
        }
    }
}

// ---------------------------------------------------------------------------
// Tests (C.4.1 – C.4.5)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // C.4.1 — single table, all #datatype variants
    #[test]
    fn single_table_all_datatype_variants_parsed() {
        let csv = "\
#datatype,string,long,double,string,boolean,dateTime:RFC3339,dateTime:RFC3339Nano\n\
#group,false,false,false,true,false,false,false\n\
#default,_result,,,,,,\n\
,result,table,_value,_field,active,_time,_nano\n\
,_result,0,1704067200000,usage_idle,true,2024-01-01T00:00:00Z,2024-01-01T00:00:00.000000000Z\n";

        let result = parse_flux_csv(csv).expect("parse must succeed");
        assert!(!result.columns.is_empty(), "columns must be present");
        assert!(!result.rows.is_empty(), "rows must be present");

        // Verify types
        let col_types: Vec<&str> = result
            .columns
            .iter()
            .map(|c| c.type_name.as_str())
            .collect();
        assert!(col_types.contains(&"integer"), "long should map to integer");
        assert!(col_types.contains(&"float"), "double should map to float");
        assert!(col_types.contains(&"boolean"), "boolean mapped");
        assert!(col_types.contains(&"datetime"), "dateTime mapped");
    }

    // C.4.2 — multiple tables flattened
    #[test]
    fn multiple_tables_are_flattened() {
        let csv = "\
#datatype,string,long,double,string\n\
#group,false,false,false,true\n\
#default,_result,,,\n\
,result,table,_value,_measurement\n\
,_result,0,0.5,cpu\n\
\n\
#datatype,string,long,double,string\n\
#group,false,false,false,true\n\
#default,_result,,,\n\
,result,table,_value,_measurement\n\
,_result,1,1.0,mem\n\
,_result,1,2.0,mem\n";

        let result = parse_flux_csv(csv).expect("parse must succeed");
        // Both tables flattened into rows
        assert_eq!(
            result.rows.len(),
            3,
            "rows from both tables expected, got: {}",
            result.rows.len()
        );
    }

    // C.4.3 — only #datatype annotation
    #[test]
    fn only_datatype_annotation_still_parses() {
        let csv = "\
#datatype,string,double,string\n\
,result,_value,_field\n\
,_result,0.5,temperature\n";

        let result = parse_flux_csv(csv).expect("parse must succeed");
        assert_eq!(result.rows.len(), 1);
    }

    // C.4.4 — no annotations: columns default to string
    #[test]
    fn no_annotations_defaults_to_string_type() {
        let csv = "\
,result,table,_value\n\
,_result,0,hello\n";

        let result = parse_flux_csv(csv).expect("parse must succeed");
        for col in &result.columns {
            assert_eq!(
                col.type_name, "text",
                "without annotations, type must be text"
            );
        }
    }

    // C.4.5 — empty body
    #[test]
    fn empty_body_returns_empty_result() {
        let result = parse_flux_csv("").expect("parse must succeed");
        assert!(result.rows.is_empty());
        assert!(result.columns.is_empty());
    }

    // Whitespace-only body
    #[test]
    fn whitespace_only_body_returns_empty_result() {
        let result = parse_flux_csv("   \n\n  ").expect("parse must succeed");
        assert!(result.rows.is_empty());
    }
}
