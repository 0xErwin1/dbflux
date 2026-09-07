//! Pinning tests for ClickHouse's dangerous-query detection.
//!
//! `ClickHouseConnection` never overrides `Connection::language_service()`,
//! so it inherits `dbflux_core`'s default `&SqlLanguageService`. Verified
//! against ClickHouse's own mutation syntax: `ALTER TABLE ... DELETE WHERE`
//! and `ALTER TABLE ... UPDATE ... WHERE` are already caught, because
//! `detect_dangerous_sql` flags any statement starting with the `ALTER`
//! keyword regardless of its sub-command, and `TRUNCATE` / `DROP` are caught
//! by the same generic rules relational drivers share.
//!
//! `KILL QUERY` / `KILL MUTATION` and `OPTIMIZE TABLE ... FINAL` are not
//! flagged. Neither is a row-level data-loss operation in the sense the
//! existing `DangerousQueryKind` variants model (no rows are deleted or
//! table structure changed); `KILL` cancels a running query/mutation and
//! `OPTIMIZE ... FINAL` forces a merge. That mirrors the product's existing
//! posture on other relational drivers, which also do not flag comparable
//! non-destructive administrative statements (e.g. PostgreSQL's `VACUUM` or
//! `pg_terminate_backend`). This module exists to pin that behavior with
//! tests, not to add a new `ClickHouseLanguageService`.
#[cfg(test)]
mod tests {
    use dbflux_core::{DangerousQueryKind, detect_dangerous_query};

    #[test]
    fn alter_table_delete_where_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("ALTER TABLE events DELETE WHERE event_date < '2020-01-01'"),
            Some(DangerousQueryKind::Alter)
        );
    }

    #[test]
    fn alter_table_delete_with_tautological_where_is_still_dangerous() {
        // ClickHouse's ALTER ... DELETE requires a WHERE clause, so the
        // "delete everything" shape is a tautological predicate rather than a
        // missing one. It is still caught because ALTER is always flagged.
        assert_eq!(
            detect_dangerous_query("ALTER TABLE events DELETE WHERE 1 = 1"),
            Some(DangerousQueryKind::Alter)
        );
    }

    #[test]
    fn alter_table_update_where_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("ALTER TABLE events UPDATE status = 'archived' WHERE id = 1"),
            Some(DangerousQueryKind::Alter)
        );
    }

    #[test]
    fn alter_table_add_column_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("ALTER TABLE events ADD COLUMN region String"),
            Some(DangerousQueryKind::Alter)
        );
    }

    #[test]
    fn truncate_table_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("TRUNCATE TABLE events"),
            Some(DangerousQueryKind::Truncate)
        );
    }

    #[test]
    fn drop_table_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("DROP TABLE events"),
            Some(DangerousQueryKind::Drop)
        );
    }

    #[test]
    fn drop_database_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("DROP DATABASE analytics"),
            Some(DangerousQueryKind::Drop)
        );
    }

    #[test]
    fn kill_query_is_not_flagged() {
        assert_eq!(
            detect_dangerous_query("KILL QUERY WHERE query_id = 'abc-123'"),
            None
        );
    }

    #[test]
    fn kill_mutation_is_not_flagged() {
        assert_eq!(
            detect_dangerous_query("KILL MUTATION WHERE mutation_id = '0000000001'"),
            None
        );
    }

    #[test]
    fn optimize_table_final_is_not_flagged() {
        assert_eq!(detect_dangerous_query("OPTIMIZE TABLE events FINAL"), None);
    }

    #[test]
    fn select_is_safe() {
        assert_eq!(
            detect_dangerous_query("SELECT * FROM events WHERE event_date = today()"),
            None
        );
    }
}
