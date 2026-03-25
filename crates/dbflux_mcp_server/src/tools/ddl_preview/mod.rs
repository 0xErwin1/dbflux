#![allow(clippy::result_large_err)]

pub mod cascade;
pub mod diff;
pub mod dry_run;
pub mod types;
pub mod validation;

use self::cascade::analyze_cascade_impact;
use self::diff::compute_schema_diff;
use self::dry_run::dry_run_ddl;
use self::types::DdlPreviewResult;
use self::validation::validate_ddl;
use dbflux_core::{Connection, DbDriver, DbError, DriverCapabilities};
use std::sync::Arc;

/// Main DDL preview implementation.
///
/// Executes DDL in a transaction, captures schema changes, rolls back,
/// and returns a comprehensive preview result.
pub fn preview_ddl_impl(
    driver: Arc<dyn DbDriver>,
    connection: Arc<dyn Connection>,
    database: Option<&str>,
    sql: &str,
) -> Result<DdlPreviewResult, DbError> {
    // Check if driver supports transactional DDL
    let metadata = driver.metadata();
    if !metadata
        .capabilities
        .contains(DriverCapabilities::TRANSACTIONAL_DDL)
    {
        return Err(DbError::query_failed(format!(
            "Driver {} does not support transactional DDL preview",
            metadata.id
        )));
    }

    // Execute DDL in transaction and capture snapshots
    let (before_snapshot, after_snapshot) =
        dry_run_ddl(driver.clone(), connection.clone(), database, sql)?;

    // Compute schema diff
    let schema_diff = compute_schema_diff(&before_snapshot, &after_snapshot)?;

    // Analyze cascade impact
    let cascade_impact = analyze_cascade_impact(sql, &before_snapshot, &after_snapshot)?;

    // Validate DDL
    let validation = validate_ddl(sql, &schema_diff, &cascade_impact)?;

    // Determine if DDL is safe (no critical errors)
    let is_safe = !validation.has_errors();

    // Generate summary
    let summary = generate_summary(&schema_diff, &cascade_impact, &validation);

    // Extract SQL statements (for now, just the input SQL)
    let sql_statements = vec![sql.to_string()];

    Ok(DdlPreviewResult {
        sql_statements,
        schema_diff,
        cascade_impact,
        validation,
        is_safe,
        summary,
    })
}

/// Generate human-readable summary of the DDL operation.
fn generate_summary(
    diff: &self::types::SchemaDiff,
    cascade: &self::types::CascadeImpact,
    validation: &self::types::ValidationResult,
) -> String {
    let mut parts = Vec::new();

    // Schema changes summary
    if !diff.is_empty() {
        let mut changes = Vec::new();

        if !diff.tables_created.is_empty() {
            changes.push(format!("{} table(s) created", diff.tables_created.len()));
        }
        if !diff.tables_dropped.is_empty() {
            changes.push(format!("{} table(s) dropped", diff.tables_dropped.len()));
        }
        if !diff.tables_altered.is_empty() {
            changes.push(format!("{} table(s) altered", diff.tables_altered.len()));
        }
        if !diff.columns_added.is_empty() {
            changes.push(format!("{} column(s) added", diff.columns_added.len()));
        }
        if !diff.columns_dropped.is_empty() {
            changes.push(format!("{} column(s) dropped", diff.columns_dropped.len()));
        }
        if !diff.columns_modified.is_empty() {
            changes.push(format!(
                "{} column(s) modified",
                diff.columns_modified.len()
            ));
        }
        if !diff.indexes_created.is_empty() {
            changes.push(format!("{} index(es) created", diff.indexes_created.len()));
        }
        if !diff.indexes_dropped.is_empty() {
            changes.push(format!("{} index(es) dropped", diff.indexes_dropped.len()));
        }
        if !diff.foreign_keys_added.is_empty() {
            changes.push(format!(
                "{} foreign key(s) added",
                diff.foreign_keys_added.len()
            ));
        }
        if !diff.foreign_keys_dropped.is_empty() {
            changes.push(format!(
                "{} foreign key(s) dropped",
                diff.foreign_keys_dropped.len()
            ));
        }

        if !changes.is_empty() {
            parts.push(format!("Schema changes: {}", changes.join(", ")));
        }
    } else {
        parts.push("No schema changes detected".to_string());
    }

    // Cascade impact summary
    if cascade.has_cascade {
        parts.push(format!(
            "CASCADE operations will affect {} object(s)",
            cascade.affected_objects.len()
        ));
    }

    // Validation summary
    if validation.has_errors() {
        parts.push(format!(
            "{} error(s), {} warning(s)",
            validation.errors.len(),
            validation.warnings.len()
        ));
    } else if !validation.warnings.is_empty() {
        parts.push(format!("{} warning(s)", validation.warnings.len()));
    } else {
        parts.push("No validation issues".to_string());
    }

    parts.join(". ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::ddl_preview::types::*;

    #[test]
    fn test_generate_summary_empty() {
        let diff = SchemaDiff::empty();
        let cascade = CascadeImpact::empty();
        let validation = ValidationResult::empty();

        let summary = generate_summary(&diff, &cascade, &validation);
        assert!(summary.contains("No schema changes"));
        assert!(summary.contains("No validation issues"));
    }

    #[test]
    fn test_generate_summary_with_changes() {
        let mut diff = SchemaDiff::empty();
        diff.tables_created.push(TableDiffEntry {
            schema: None,
            name: "users".to_string(),
            column_count: 3,
        });

        let cascade = CascadeImpact::empty();
        let validation = ValidationResult::empty();

        let summary = generate_summary(&diff, &cascade, &validation);
        assert!(summary.contains("1 table(s) created"));
    }

    #[test]
    fn test_generate_summary_with_cascade() {
        let diff = SchemaDiff::empty();
        let mut cascade = CascadeImpact::empty();
        cascade.has_cascade = true;
        cascade.affected_objects.push(AffectedObject {
            object_type: "view".to_string(),
            schema: None,
            name: "user_view".to_string(),
            reason: "depends on table".to_string(),
        });

        let validation = ValidationResult::empty();

        let summary = generate_summary(&diff, &cascade, &validation);
        assert!(summary.contains("CASCADE"));
        assert!(summary.contains("1 object(s)"));
    }

    #[test]
    fn test_generate_summary_with_errors() {
        let diff = SchemaDiff::empty();
        let cascade = CascadeImpact::empty();
        let mut validation = ValidationResult::empty();
        validation.add_error("Test error");
        validation.add_warning("Test warning");

        let summary = generate_summary(&diff, &cascade, &validation);
        assert!(summary.contains("1 error(s)"));
        assert!(summary.contains("1 warning(s)"));
    }
}
