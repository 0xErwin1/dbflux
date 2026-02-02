use crate::schema::{ColumnInfo, TableInfo};
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

/// Generate SELECT * with LIMIT for browsing.
pub fn generate_select_star(dialect: &dyn SqlDialect, table: &TableInfo, limit: u32) -> String {
    let table_ref = dialect.qualified_table(table.schema.as_deref(), &table.name);
    format!("SELECT * FROM {} LIMIT {};", table_ref, limit)
}

/// Generate INSERT template with placeholders.
pub fn generate_insert_template(dialect: &dyn SqlDialect, table: &TableInfo) -> String {
    let table_ref = dialect.qualified_table(table.schema.as_deref(), &table.name);
    let cols = table.columns.as_deref().unwrap_or(&[]);

    if cols.is_empty() {
        return format!("INSERT INTO {} DEFAULT VALUES;", table_ref);
    }

    let columns: Vec<String> = cols
        .iter()
        .map(|c| dialect.quote_identifier(&c.name))
        .collect();

    let placeholders: Vec<String> = (0..cols.len())
        .map(|i| format_placeholder(dialect, i))
        .collect();

    format!(
        "INSERT INTO {} ({})\nVALUES ({});",
        table_ref,
        columns.join(", "),
        placeholders.join(", ")
    )
}

/// Generate UPDATE template with placeholders.
pub fn generate_update_template(dialect: &dyn SqlDialect, table: &TableInfo) -> String {
    let table_ref = dialect.qualified_table(table.schema.as_deref(), &table.name);
    let cols = table.columns.as_deref().unwrap_or(&[]);

    if cols.is_empty() {
        return format!("UPDATE {}\nSET -- no columns\nWHERE <condition>;", table_ref);
    }

    let pk_columns: Vec<&ColumnInfo> = cols.iter().filter(|c| c.is_primary_key).collect();

    let non_pk_columns: Vec<&ColumnInfo> = cols.iter().filter(|c| !c.is_primary_key).collect();

    let set_columns: &[&ColumnInfo] = if non_pk_columns.is_empty() {
        &cols.iter().collect::<Vec<_>>()
    } else {
        &non_pk_columns
    };

    let set_clauses: Vec<String> = set_columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            format!(
                "{} = {}",
                dialect.quote_identifier(&col.name),
                format_placeholder(dialect, i)
            )
        })
        .collect();

    let where_clause = if pk_columns.is_empty() {
        "<condition>".to_string()
    } else {
        let start_idx = set_columns.len();
        pk_columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                format!(
                    "{} = {}",
                    dialect.quote_identifier(&col.name),
                    format_placeholder(dialect, start_idx + i)
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    };

    format!(
        "UPDATE {}\nSET {}\nWHERE {};",
        table_ref,
        set_clauses.join(",\n    "),
        where_clause
    )
}

/// Generate DELETE template with placeholders.
pub fn generate_delete_template(dialect: &dyn SqlDialect, table: &TableInfo) -> String {
    let table_ref = dialect.qualified_table(table.schema.as_deref(), &table.name);
    let cols = table.columns.as_deref().unwrap_or(&[]);

    let pk_columns: Vec<&ColumnInfo> = cols.iter().filter(|c| c.is_primary_key).collect();

    let where_clause = if pk_columns.is_empty() {
        "<condition>".to_string()
    } else {
        pk_columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                format!(
                    "{} = {}",
                    dialect.quote_identifier(&col.name),
                    format_placeholder(dialect, i)
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    };

    format!("DELETE FROM {}\nWHERE {};", table_ref, where_clause)
}

/// Generate CREATE TABLE DDL from table metadata.
pub fn generate_create_table(dialect: &dyn SqlDialect, table: &TableInfo) -> String {
    let table_ref = dialect.qualified_table(table.schema.as_deref(), &table.name);
    let cols = table.columns.as_deref().unwrap_or(&[]);

    if cols.is_empty() {
        return format!("CREATE TABLE {} ();", table_ref);
    }

    let pk_columns: Vec<&ColumnInfo> = cols.iter().filter(|c| c.is_primary_key).collect();

    let mut lines: Vec<String> = Vec::with_capacity(cols.len() + 1);

    for col in cols {
        let mut line = if col.type_name.is_empty() {
            format!("    {}", dialect.quote_identifier(&col.name))
        } else {
            format!(
                "    {} {}",
                dialect.quote_identifier(&col.name),
                col.type_name
            )
        };

        if !col.nullable {
            line.push_str(" NOT NULL");
        }

        if let Some(ref default) = col.default_value {
            line.push_str(&format!(" DEFAULT {}", default));
        }

        lines.push(line);
    }

    if !pk_columns.is_empty() {
        let pk_quoted: Vec<String> = pk_columns
            .iter()
            .map(|c| dialect.quote_identifier(&c.name))
            .collect();
        lines.push(format!("    PRIMARY KEY ({})", pk_quoted.join(", ")));
    }

    format!("CREATE TABLE {} (\n{}\n);", table_ref, lines.join(",\n"))
}

/// Generate TRUNCATE statement.
pub fn generate_truncate(dialect: &dyn SqlDialect, table: &TableInfo) -> String {
    let table_ref = dialect.qualified_table(table.schema.as_deref(), &table.name);
    format!("TRUNCATE TABLE {};", table_ref)
}

/// Generate DROP TABLE statement.
pub fn generate_drop_table(dialect: &dyn SqlDialect, table: &TableInfo) -> String {
    let table_ref = dialect.qualified_table(table.schema.as_deref(), &table.name);
    format!("DROP TABLE {};", table_ref)
}
