use crate::schema::ColumnInfo;
use crate::sql_dialect::{PlaceholderStyle, SqlDialect};
use crate::Value;

/// Type of SQL statement to generate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlOperation {
    SelectWhere,
    Insert,
    Update,
    Delete,
}

/// How values should be represented in generated SQL.
#[derive(Debug, Clone)]
pub enum SqlValueMode<'a> {
    /// Include actual values as literals.
    WithValues(&'a [Value]),
    /// Use placeholders (? or $1, $2, etc.).
    WithPlaceholders,
}

/// Options for SQL generation.
#[derive(Debug, Clone, Default)]
pub struct SqlGenerationOptions {
    /// Include schema/database prefix in table name.
    pub fully_qualified: bool,
    /// Generate compact single-line SQL.
    pub compact: bool,
}

/// Request for SQL generation.
pub struct SqlGenerationRequest<'a> {
    pub operation: SqlOperation,
    pub schema: Option<&'a str>,
    pub table: &'a str,
    pub columns: &'a [ColumnInfo],
    pub values: SqlValueMode<'a>,
    pub pk_indices: &'a [usize],
    pub options: SqlGenerationOptions,
}

/// Generate SQL using the provided dialect.
pub fn generate_sql(dialect: &dyn SqlDialect, request: &SqlGenerationRequest) -> String {
    let table_ref = if request.options.fully_qualified {
        dialect.qualified_table(request.schema, request.table)
    } else {
        dialect.quote_identifier(request.table)
    };

    let separator = if request.options.compact {
        " "
    } else {
        "\n    "
    };
    let newline = if request.options.compact { " " } else { "\n" };

    match request.operation {
        SqlOperation::SelectWhere => {
            generate_select_where(dialect, request, &table_ref, newline)
        }
        SqlOperation::Insert => {
            generate_insert(dialect, request, &table_ref, newline)
        }
        SqlOperation::Update => {
            generate_update(dialect, request, &table_ref, separator, newline)
        }
        SqlOperation::Delete => {
            generate_delete(dialect, request, &table_ref, newline)
        }
    }
}

fn generate_select_where(
    dialect: &dyn SqlDialect,
    request: &SqlGenerationRequest,
    table_ref: &str,
    newline: &str,
) -> String {
    let where_clause = build_where_clause(dialect, request);

    if request.options.compact {
        format!("SELECT * FROM {} WHERE {};", table_ref, where_clause)
    } else {
        format!("SELECT *{}FROM {}{}WHERE {};", newline, table_ref, newline, where_clause)
    }
}

fn generate_insert(
    dialect: &dyn SqlDialect,
    request: &SqlGenerationRequest,
    table_ref: &str,
    newline: &str,
) -> String {
    let columns: Vec<String> = request
        .columns
        .iter()
        .map(|c| dialect.quote_identifier(&c.name))
        .collect();

    let cols_str = columns.join(", ");
    let vals_str = build_values_list(dialect, request);

    if request.options.compact {
        format!("INSERT INTO {} ({}) VALUES ({});", table_ref, cols_str, vals_str)
    } else {
        format!(
            "INSERT INTO {} ({}){}VALUES ({});",
            table_ref, cols_str, newline, vals_str
        )
    }
}

fn generate_update(
    dialect: &dyn SqlDialect,
    request: &SqlGenerationRequest,
    table_ref: &str,
    separator: &str,
    newline: &str,
) -> String {
    let set_clause = build_set_clause(dialect, request, separator);
    let where_clause = build_where_clause(dialect, request);

    if request.options.compact {
        format!("UPDATE {} SET {} WHERE {};", table_ref, set_clause, where_clause)
    } else {
        format!(
            "UPDATE {}{}SET {}{}WHERE {};",
            table_ref, newline, set_clause, newline, where_clause
        )
    }
}

fn generate_delete(
    dialect: &dyn SqlDialect,
    request: &SqlGenerationRequest,
    table_ref: &str,
    newline: &str,
) -> String {
    let where_clause = build_where_clause(dialect, request);

    if request.options.compact {
        format!("DELETE FROM {} WHERE {};", table_ref, where_clause)
    } else {
        format!("DELETE FROM {}{}WHERE {};", table_ref, newline, where_clause)
    }
}

fn build_where_clause(dialect: &dyn SqlDialect, request: &SqlGenerationRequest) -> String {
    let indices: Vec<usize> = if request.pk_indices.is_empty() {
        (0..request.columns.len()).collect()
    } else {
        request.pk_indices.to_vec()
    };

    let conditions: Vec<String> = match &request.values {
        SqlValueMode::WithValues(values) => {
            indices
                .iter()
                .filter_map(|&idx| {
                    let col = request.columns.get(idx)?;
                    let val = values.get(idx)?;
                    let col_name = dialect.quote_identifier(&col.name);

                    if val.is_null() {
                        Some(format!("{} IS NULL", col_name))
                    } else {
                        Some(format!("{} = {}", col_name, dialect.value_to_literal(val)))
                    }
                })
                .collect()
        }
        SqlValueMode::WithPlaceholders => {
            indices
                .iter()
                .enumerate()
                .filter_map(|(placeholder_idx, &col_idx)| {
                    let col = request.columns.get(col_idx)?;
                    let col_name = dialect.quote_identifier(&col.name);
                    let placeholder = format_placeholder(dialect, placeholder_idx);
                    Some(format!("{} = {}", col_name, placeholder))
                })
                .collect()
        }
    };

    if conditions.is_empty() {
        "1=1".to_string()
    } else {
        conditions.join(" AND ")
    }
}

fn build_set_clause(
    dialect: &dyn SqlDialect,
    request: &SqlGenerationRequest,
    separator: &str,
) -> String {
    let set_parts: Vec<String> = match &request.values {
        SqlValueMode::WithValues(values) => {
            request
                .columns
                .iter()
                .enumerate()
                .map(|(idx, col)| {
                    let col_name = dialect.quote_identifier(&col.name);
                    let val_str = values
                        .get(idx)
                        .map(|v| dialect.value_to_literal(v))
                        .unwrap_or_else(|| "NULL".to_string());
                    format!("{} = {}", col_name, val_str)
                })
                .collect()
        }
        SqlValueMode::WithPlaceholders => {
            request
                .columns
                .iter()
                .enumerate()
                .map(|(idx, col)| {
                    let col_name = dialect.quote_identifier(&col.name);
                    let placeholder = format_placeholder(dialect, idx);
                    format!("{} = {}", col_name, placeholder)
                })
                .collect()
        }
    };

    set_parts.join(&format!(",{}", separator))
}

fn build_values_list(dialect: &dyn SqlDialect, request: &SqlGenerationRequest) -> String {
    match &request.values {
        SqlValueMode::WithValues(values) => {
            let vals: Vec<String> = request
                .columns
                .iter()
                .enumerate()
                .map(|(idx, _)| {
                    values
                        .get(idx)
                        .map(|v| dialect.value_to_literal(v))
                        .unwrap_or_else(|| "NULL".to_string())
                })
                .collect();
            vals.join(", ")
        }
        SqlValueMode::WithPlaceholders => {
            let placeholders: Vec<String> = (0..request.columns.len())
                .map(|idx| format_placeholder(dialect, idx))
                .collect();
            placeholders.join(", ")
        }
    }
}

fn format_placeholder(dialect: &dyn SqlDialect, index: usize) -> String {
    match dialect.placeholder_style() {
        PlaceholderStyle::QuestionMark => "?".to_string(),
        PlaceholderStyle::DollarNumber => format!("${}", index + 1),
    }
}
