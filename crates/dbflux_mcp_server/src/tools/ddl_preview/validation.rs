use super::types::*;
use dbflux_core::DbError;

/// Validate DDL statements for safety and correctness.
pub fn validate_ddl(
    sql: &str,
    diff: &SchemaDiff,
    cascade_impact: &CascadeImpact,
) -> Result<ValidationResult, DbError> {
    let mut validation = ValidationResult::empty();

    // Validate SQL syntax (basic checks)
    validate_sql_syntax(sql, &mut validation)?;

    // Check for dangerous operations
    validate_dangerous_operations(sql, &mut validation)?;

    // Validate schema changes
    validate_schema_changes(diff, &mut validation)?;

    // Validate cascade impact
    validate_cascade_impact(cascade_impact, &mut validation)?;

    // Check for data loss risks
    validate_data_loss_risks(diff, &mut validation)?;

    Ok(validation)
}

/// Validate SQL syntax (basic checks).
fn validate_sql_syntax(sql: &str, validation: &mut ValidationResult) -> Result<(), DbError> {
    let sql_trimmed = sql.trim();

    if sql_trimmed.is_empty() {
        validation.add_error("SQL statement is empty");
        return Ok(());
    }

    // Check for common syntax errors
    let sql_upper = sql_trimmed.to_uppercase();

    // Check for balanced parentheses
    let open_count = sql.chars().filter(|&c| c == '(').count();
    let close_count = sql.chars().filter(|&c| c == ')').count();
    if open_count != close_count {
        validation.add_error(format!(
            "Unbalanced parentheses: {} open, {} close",
            open_count, close_count
        ));
    }

    // Check for unterminated strings
    if sql.matches('\'').count() % 2 != 0 {
        validation.add_error("Unterminated string literal");
    }

    // Validate DDL keywords
    let valid_ddl_starts = [
        "CREATE", "ALTER", "DROP", "RENAME", "TRUNCATE", "COMMENT", "GRANT", "REVOKE",
    ];

    if !valid_ddl_starts.iter().any(|kw| sql_upper.starts_with(kw)) {
        validation
            .add_warning("Statement does not appear to be a DDL operation (CREATE/ALTER/DROP)");
    }

    Ok(())
}

/// Check for dangerous operations.
fn validate_dangerous_operations(
    sql: &str,
    validation: &mut ValidationResult,
) -> Result<(), DbError> {
    let sql_upper = sql.to_uppercase();

    // DROP DATABASE is extremely dangerous
    if sql_upper.contains("DROP DATABASE") {
        validation.add_error("DROP DATABASE is not allowed in preview mode");
    }

    // DROP SCHEMA without CASCADE
    if sql_upper.contains("DROP SCHEMA") && !sql_upper.contains("CASCADE") {
        validation
            .add_warning("DROP SCHEMA without CASCADE may fail if the schema contains objects");
    }

    // DROP TABLE without CASCADE
    if sql_upper.contains("DROP TABLE") && !sql_upper.contains("CASCADE") {
        validation.add_info("DROP TABLE without CASCADE will fail if there are dependent objects");
    }

    // TRUNCATE TABLE
    if sql_upper.contains("TRUNCATE") {
        validation.add_warning("TRUNCATE will delete all data from the table");
    }

    // ALTER COLUMN TYPE without USING clause (PostgreSQL)
    if sql_upper.contains("ALTER COLUMN") && sql_upper.contains("TYPE") {
        if !sql_upper.contains("USING") {
            validation.add_warning(
                "ALTER COLUMN TYPE without USING clause may fail if data is not compatible",
            );
        }
    }

    // DROP COLUMN
    if sql_upper.contains("DROP COLUMN") {
        validation.add_warning("DROP COLUMN will permanently delete column data");
    }

    Ok(())
}

/// Validate schema changes.
fn validate_schema_changes(
    diff: &SchemaDiff,
    validation: &mut ValidationResult,
) -> Result<(), DbError> {
    // Warn about tables being dropped
    if !diff.tables_dropped.is_empty() {
        validation.add_warning(format!(
            "{} table(s) will be dropped",
            diff.tables_dropped.len()
        ));
    }

    // Warn about columns being dropped
    if !diff.columns_dropped.is_empty() {
        validation.add_warning(format!(
            "{} column(s) will be dropped",
            diff.columns_dropped.len()
        ));
    }

    // Info about new tables
    if !diff.tables_created.is_empty() {
        validation.add_info(format!(
            "{} table(s) will be created",
            diff.tables_created.len()
        ));
    }

    // Info about new columns
    if !diff.columns_added.is_empty() {
        validation.add_info(format!(
            "{} column(s) will be added",
            diff.columns_added.len()
        ));
    }

    // Check for column type changes
    for col_mod in &diff.columns_modified {
        validation.add_warning(format!(
            "Column {}.{} type will change from {} to {}",
            col_mod.table_name, col_mod.column_name, col_mod.old_type, col_mod.new_type
        ));

        // Check for nullable changes
        if col_mod.old_nullable && !col_mod.new_nullable {
            validation.add_warning(format!(
                "Column {}.{} will become NOT NULL (may fail if NULL values exist)",
                col_mod.table_name, col_mod.column_name
            ));
        }
    }

    // Warn about foreign key changes
    if !diff.foreign_keys_dropped.is_empty() {
        validation.add_warning(format!(
            "{} foreign key(s) will be dropped",
            diff.foreign_keys_dropped.len()
        ));
    }

    if !diff.foreign_keys_added.is_empty() {
        validation.add_info(format!(
            "{} foreign key(s) will be added",
            diff.foreign_keys_added.len()
        ));
    }

    Ok(())
}

/// Validate cascade impact.
fn validate_cascade_impact(
    cascade_impact: &CascadeImpact,
    validation: &mut ValidationResult,
) -> Result<(), DbError> {
    if cascade_impact.has_cascade {
        validation.add_warning("CASCADE operations will drop dependent objects automatically");

        for obj in &cascade_impact.affected_objects {
            validation.add_warning(format!(
                "CASCADE will affect {}: {}",
                obj.object_type, obj.reason
            ));
        }
    }

    if let Some(row_count) = cascade_impact.estimated_row_impact {
        validation.add_warning(format!(
            "Estimated impact: {} row(s) may be affected",
            row_count
        ));
    }

    Ok(())
}

/// Check for data loss risks.
fn validate_data_loss_risks(
    diff: &SchemaDiff,
    validation: &mut ValidationResult,
) -> Result<(), DbError> {
    // Tables being dropped = potential data loss
    if !diff.tables_dropped.is_empty() {
        validation.add_error(format!(
            "DATA LOSS RISK: {} table(s) will be dropped with all their data",
            diff.tables_dropped.len()
        ));
    }

    // Columns being dropped = potential data loss
    if !diff.columns_dropped.is_empty() {
        validation.add_error(format!(
            "DATA LOSS RISK: {} column(s) will be dropped with all their data",
            diff.columns_dropped.len()
        ));
    }

    // Column type changes = potential data conversion issues
    if !diff.columns_modified.is_empty() {
        validation.add_warning(format!(
            "DATA CONVERSION RISK: {} column(s) will have their type changed (may lose precision or fail)",
            diff.columns_modified.len()
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_empty_sql() {
        let mut validation = ValidationResult::empty();
        validate_sql_syntax("", &mut validation).unwrap();
        assert!(validation.has_errors());
    }

    #[test]
    fn test_validate_unbalanced_parens() {
        let mut validation = ValidationResult::empty();
        validate_sql_syntax("CREATE TABLE users (id INT", &mut validation).unwrap();
        assert!(validation.has_errors());
    }

    #[test]
    fn test_validate_unterminated_string() {
        let mut validation = ValidationResult::empty();
        validate_sql_syntax("CREATE TABLE users (name VARCHAR('test)", &mut validation).unwrap();
        assert!(validation.has_errors());
    }

    #[test]
    fn test_validate_drop_database() {
        let mut validation = ValidationResult::empty();
        validate_dangerous_operations("DROP DATABASE mydb", &mut validation).unwrap();
        assert!(validation.has_errors());
    }

    #[test]
    fn test_validate_truncate() {
        let mut validation = ValidationResult::empty();
        validate_dangerous_operations("TRUNCATE TABLE users", &mut validation).unwrap();
        assert!(!validation.warnings.is_empty());
    }

    #[test]
    fn test_validate_drop_column() {
        let mut validation = ValidationResult::empty();
        validate_dangerous_operations("ALTER TABLE users DROP COLUMN email", &mut validation)
            .unwrap();
        assert!(!validation.warnings.is_empty());
    }

    #[test]
    fn test_validate_cascade_warning() {
        let cascade_impact = CascadeImpact {
            has_cascade: true,
            affected_objects: vec![AffectedObject {
                object_type: "views".to_string(),
                schema: None,
                name: "user_view".to_string(),
                reason: "dependent view".to_string(),
            }],
            estimated_row_impact: None,
        };

        let mut validation = ValidationResult::empty();
        validate_cascade_impact(&cascade_impact, &mut validation).unwrap();
        assert!(!validation.warnings.is_empty());
    }

    #[test]
    fn test_validate_data_loss_on_drop_table() {
        let mut diff = SchemaDiff::empty();
        diff.tables_dropped.push(TableDiffEntry {
            schema: None,
            name: "users".to_string(),
            column_count: 5,
        });

        let mut validation = ValidationResult::empty();
        validate_data_loss_risks(&diff, &mut validation).unwrap();
        assert!(validation.has_errors());
    }

    #[test]
    fn test_validate_data_loss_on_drop_column() {
        let mut diff = SchemaDiff::empty();
        diff.columns_dropped.push(ColumnDiffEntry {
            table_schema: None,
            table_name: "users".to_string(),
            column_name: "email".to_string(),
            data_type: "varchar".to_string(),
            nullable: true,
        });

        let mut validation = ValidationResult::empty();
        validate_data_loss_risks(&diff, &mut validation).unwrap();
        assert!(validation.has_errors());
    }
}
