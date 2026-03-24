use super::types::*;
use dbflux_core::DbError;

/// Analyze cascade impact of DDL operations.
pub fn analyze_cascade_impact(
    sql: &str,
    _before: &SchemaStateSnapshot,
    _after: &SchemaStateSnapshot,
) -> Result<CascadeImpact, DbError> {
    let mut impact = CascadeImpact::empty();

    // Detect CASCADE keyword in DDL
    let sql_upper = sql.to_uppercase();
    impact.has_cascade = sql_upper.contains("CASCADE");

    if impact.has_cascade {
        // Parse and analyze CASCADE operations
        analyze_cascade_operations(&sql_upper, &mut impact)?;
    }

    Ok(impact)
}

/// Analyze CASCADE operations in DDL statements.
fn analyze_cascade_operations(sql: &str, impact: &mut CascadeImpact) -> Result<(), DbError> {
    // DROP TABLE ... CASCADE
    if sql.contains("DROP TABLE") && sql.contains("CASCADE") {
        impact.affected_objects.push(AffectedObject {
            object_type: "dependent_views".to_string(),
            schema: None,
            name: "unknown".to_string(),
            reason: "DROP TABLE CASCADE will drop dependent views".to_string(),
        });
    }

    // DROP COLUMN ... CASCADE
    if sql.contains("DROP COLUMN") && sql.contains("CASCADE") {
        impact.affected_objects.push(AffectedObject {
            object_type: "dependent_objects".to_string(),
            schema: None,
            name: "unknown".to_string(),
            reason: "DROP COLUMN CASCADE will drop dependent objects (views, indexes, constraints)"
                .to_string(),
        });
    }

    // ALTER TYPE ... CASCADE
    if sql.contains("ALTER TYPE") && sql.contains("CASCADE") {
        impact.affected_objects.push(AffectedObject {
            object_type: "dependent_columns".to_string(),
            schema: None,
            name: "unknown".to_string(),
            reason: "ALTER TYPE CASCADE will affect dependent columns".to_string(),
        });
    }

    // DROP SCHEMA ... CASCADE
    if sql.contains("DROP SCHEMA") && sql.contains("CASCADE") {
        impact.affected_objects.push(AffectedObject {
            object_type: "schema_objects".to_string(),
            schema: None,
            name: "unknown".to_string(),
            reason: "DROP SCHEMA CASCADE will drop all objects in the schema".to_string(),
        });
    }

    Ok(())
}

/// Estimate the impact on data rows.
///
/// This is a placeholder for future functionality where we could query
/// actual row counts from the database to estimate data loss.
pub fn estimate_row_impact(
    _sql: &str,
    _schema: &SchemaStateSnapshot,
) -> Result<Option<usize>, DbError> {
    // Future: Query actual row counts for tables being dropped
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_cascade() {
        let sql = "DROP TABLE users";
        let before = SchemaStateSnapshot::new();
        let after = SchemaStateSnapshot::new();

        let impact = analyze_cascade_impact(sql, &before, &after).unwrap();
        assert!(!impact.has_cascade);
        assert_eq!(impact.affected_objects.len(), 0);
    }

    #[test]
    fn test_drop_table_cascade() {
        let sql = "DROP TABLE users CASCADE";
        let before = SchemaStateSnapshot::new();
        let after = SchemaStateSnapshot::new();

        let impact = analyze_cascade_impact(sql, &before, &after).unwrap();
        assert!(impact.has_cascade);
        assert!(impact.affected_objects.len() > 0);
        assert_eq!(impact.affected_objects[0].object_type, "dependent_views");
    }

    #[test]
    fn test_drop_column_cascade() {
        let sql = "ALTER TABLE users DROP COLUMN email CASCADE";
        let before = SchemaStateSnapshot::new();
        let after = SchemaStateSnapshot::new();

        let impact = analyze_cascade_impact(sql, &before, &after).unwrap();
        assert!(impact.has_cascade);
        assert!(impact.affected_objects.len() > 0);
    }

    #[test]
    fn test_drop_schema_cascade() {
        let sql = "DROP SCHEMA public CASCADE";
        let before = SchemaStateSnapshot::new();
        let after = SchemaStateSnapshot::new();

        let impact = analyze_cascade_impact(sql, &before, &after).unwrap();
        assert!(impact.has_cascade);
        assert!(impact.affected_objects.len() > 0);
        assert_eq!(impact.affected_objects[0].object_type, "schema_objects");
    }

    #[test]
    fn test_case_insensitive() {
        let sql = "drop table users cascade";
        let before = SchemaStateSnapshot::new();
        let after = SchemaStateSnapshot::new();

        let impact = analyze_cascade_impact(sql, &before, &after).unwrap();
        assert!(impact.has_cascade);
    }
}
