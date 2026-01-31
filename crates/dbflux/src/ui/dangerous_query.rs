//! Detection of potentially dangerous SQL queries.
//!
//! This module provides heuristic detection of queries that may cause
//! unintended data loss or structural changes. It is NOT a SQL parser -
//! it uses simple pattern matching that may have false positives/negatives.
//!
//! TODO: Re-integrate with SqlQueryDocument for dangerous query warnings.

#![allow(dead_code)]

/// Categories of dangerous queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DangerousQueryKind {
    DeleteNoWhere,
    UpdateNoWhere,
    Truncate,
    Drop,
    Alter,
    /// Multi-statement script containing at least one dangerous query.
    Script,
}

impl DangerousQueryKind {
    pub fn message(&self) -> &'static str {
        match self {
            Self::DeleteNoWhere => "DELETE without WHERE may affect all rows",
            Self::UpdateNoWhere => "UPDATE without WHERE may affect all rows",
            Self::Truncate => "TRUNCATE will delete all rows",
            Self::Drop => "DROP will permanently remove the object",
            Self::Alter => "ALTER will modify the structure",
            Self::Script => "This script contains potentially destructive statements",
        }
    }
}

/// Detect if SQL contains dangerous statements.
///
/// For multi-statement scripts (containing `;`), returns `Script` kind
/// if any statement is dangerous.
pub fn detect_dangerous_query(sql: &str) -> Option<DangerousQueryKind> {
    let clean = strip_leading_comments(sql);
    if clean.is_empty() {
        return None;
    }

    let statements: Vec<&str> = clean
        .split(';')
        .map(strip_leading_comments)
        .filter(|s| !s.is_empty())
        .collect();

    if statements.is_empty() {
        return None;
    }

    if statements.len() > 1 {
        for stmt in &statements {
            if detect_dangerous_single(stmt).is_some() {
                return Some(DangerousQueryKind::Script);
            }
        }
        return None;
    }

    detect_dangerous_single(statements[0])
}

/// Detect if a single statement (no `;`) is dangerous.
fn detect_dangerous_single(sql: &str) -> Option<DangerousQueryKind> {
    let normalized = sql.trim().to_lowercase();
    let main_stmt = skip_cte_prefix(&normalized);

    if main_stmt.starts_with("delete") && !contains_where_clause(&normalized) {
        return Some(DangerousQueryKind::DeleteNoWhere);
    }

    if main_stmt.starts_with("update") && !contains_where_clause(&normalized) {
        return Some(DangerousQueryKind::UpdateNoWhere);
    }

    if main_stmt.starts_with("truncate") {
        return Some(DangerousQueryKind::Truncate);
    }

    if main_stmt.starts_with("drop") {
        return Some(DangerousQueryKind::Drop);
    }

    if main_stmt.starts_with("alter") {
        return Some(DangerousQueryKind::Alter);
    }

    None
}

/// Skip CTE prefix (WITH ... AS (...)) to find the main statement.
fn skip_cte_prefix(sql: &str) -> &str {
    if !sql.starts_with("with ") {
        return sql;
    }

    // Find last ) followed by a DML keyword (handles any whitespace between)
    for (i, _) in sql.rmatch_indices(')') {
        let after = sql[i + 1..].trim_start();
        for keyword in ["delete", "update", "insert", "select", "truncate"] {
            if after.starts_with(keyword) {
                return after;
            }
        }
    }

    sql
}

/// Check if SQL contains a WHERE clause.
fn contains_where_clause(normalized_sql: &str) -> bool {
    normalized_sql.contains(" where ")
}

/// Strip leading SQL comments (line and block).
///
/// Returns the SQL after removing leading `-- ...` and `/* ... */` comments.
/// If a block comment is incomplete, returns empty string (safe default).
pub fn strip_leading_comments(sql: &str) -> &str {
    let mut s = sql.trim_start();

    loop {
        if s.starts_with("--") {
            match s.find('\n') {
                Some(i) => s = s[i + 1..].trim_start(),
                None => return "",
            }
        } else if s.starts_with("/*") {
            match s.find("*/") {
                Some(i) => s = s[i + 2..].trim_start(),
                None => return "",
            }
        } else {
            break;
        }
    }

    s
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== DELETE tests ====================

    #[test]
    fn delete_without_where_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("DELETE FROM users"),
            Some(DangerousQueryKind::DeleteNoWhere)
        );
    }

    #[test]
    fn delete_with_where_is_safe() {
        assert_eq!(
            detect_dangerous_query("DELETE FROM users WHERE id = 1"),
            None
        );
    }

    #[test]
    fn delete_case_insensitive() {
        assert_eq!(
            detect_dangerous_query("delete from users"),
            Some(DangerousQueryKind::DeleteNoWhere)
        );
        assert_eq!(
            detect_dangerous_query("DELETE from users"),
            Some(DangerousQueryKind::DeleteNoWhere)
        );
    }

    #[test]
    fn delete_with_where_in_subquery_but_no_outer_where() {
        // This has WHERE in a subquery but no outer WHERE - still dangerous
        // Our simple heuristic will see " where " and pass it though
        // This is a known limitation - we accept false negatives here
        let sql = "DELETE FROM users WHERE id IN (SELECT id FROM temp WHERE active = 1)";
        assert_eq!(detect_dangerous_query(sql), None);
    }

    // ==================== UPDATE tests ====================

    #[test]
    fn update_without_where_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("UPDATE users SET active = false"),
            Some(DangerousQueryKind::UpdateNoWhere)
        );
    }

    #[test]
    fn update_with_where_is_safe() {
        assert_eq!(
            detect_dangerous_query("UPDATE users SET active = false WHERE id = 1"),
            None
        );
    }

    #[test]
    fn update_case_insensitive() {
        assert_eq!(
            detect_dangerous_query("update users set active = false"),
            Some(DangerousQueryKind::UpdateNoWhere)
        );
    }

    // ==================== TRUNCATE tests ====================

    #[test]
    fn truncate_is_always_dangerous() {
        assert_eq!(
            detect_dangerous_query("TRUNCATE TABLE users"),
            Some(DangerousQueryKind::Truncate)
        );
        assert_eq!(
            detect_dangerous_query("truncate users"),
            Some(DangerousQueryKind::Truncate)
        );
    }

    #[test]
    fn truncate_cascade_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("TRUNCATE TABLE users CASCADE"),
            Some(DangerousQueryKind::Truncate)
        );
        assert_eq!(
            detect_dangerous_query("TRUNCATE users, orders CASCADE"),
            Some(DangerousQueryKind::Truncate)
        );
    }

    #[test]
    fn truncate_restart_identity_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("TRUNCATE TABLE users RESTART IDENTITY CASCADE"),
            Some(DangerousQueryKind::Truncate)
        );
    }

    // ==================== DROP tests ====================

    #[test]
    fn drop_table_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("DROP TABLE users"),
            Some(DangerousQueryKind::Drop)
        );
    }

    #[test]
    fn drop_index_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("DROP INDEX idx_users_email"),
            Some(DangerousQueryKind::Drop)
        );
    }

    #[test]
    fn drop_database_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("DROP DATABASE mydb"),
            Some(DangerousQueryKind::Drop)
        );
    }

    #[test]
    fn drop_if_exists_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("DROP TABLE IF EXISTS users"),
            Some(DangerousQueryKind::Drop)
        );
    }

    // ==================== ALTER tests ====================

    #[test]
    fn alter_table_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
            Some(DangerousQueryKind::Alter)
        );
    }

    #[test]
    fn alter_drop_column_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("ALTER TABLE users DROP COLUMN email"),
            Some(DangerousQueryKind::Alter)
        );
    }

    // ==================== Safe queries ====================

    #[test]
    fn select_is_safe() {
        assert_eq!(detect_dangerous_query("SELECT * FROM users"), None);
        assert_eq!(
            detect_dangerous_query("SELECT * FROM users WHERE id = 1"),
            None
        );
    }

    #[test]
    fn insert_is_safe() {
        assert_eq!(
            detect_dangerous_query("INSERT INTO users (name) VALUES ('test')"),
            None
        );
    }

    #[test]
    fn create_table_is_safe() {
        assert_eq!(
            detect_dangerous_query("CREATE TABLE users (id INT PRIMARY KEY)"),
            None
        );
    }

    #[test]
    fn create_index_is_safe() {
        assert_eq!(
            detect_dangerous_query("CREATE INDEX idx_users_email ON users(email)"),
            None
        );
    }

    // ==================== Comment handling ====================

    #[test]
    fn line_comment_before_delete() {
        assert_eq!(
            detect_dangerous_query("-- This deletes all users\nDELETE FROM users"),
            Some(DangerousQueryKind::DeleteNoWhere)
        );
    }

    #[test]
    fn block_comment_before_delete() {
        assert_eq!(
            detect_dangerous_query("/* Clean up */\nDELETE FROM users"),
            Some(DangerousQueryKind::DeleteNoWhere)
        );
    }

    #[test]
    fn multiple_comments_before_statement() {
        let sql = "-- First comment\n/* Block */\n-- Another\nDELETE FROM users";
        assert_eq!(
            detect_dangerous_query(sql),
            Some(DangerousQueryKind::DeleteNoWhere)
        );
    }

    #[test]
    fn comment_only_is_safe() {
        assert_eq!(detect_dangerous_query("-- just a comment"), None);
        assert_eq!(detect_dangerous_query("/* just a block comment */"), None);
    }

    #[test]
    fn incomplete_block_comment_is_safe() {
        assert_eq!(
            detect_dangerous_query("/* incomplete DELETE FROM users"),
            None
        );
    }

    #[test]
    fn select_with_leading_comment_is_safe() {
        assert_eq!(
            detect_dangerous_query("-- Get users\nSELECT * FROM users"),
            None
        );
    }

    // ==================== Multi-statement (script) tests ====================

    #[test]
    fn script_with_dangerous_statement() {
        let sql = "SELECT 1; DELETE FROM users; SELECT 2";
        assert_eq!(
            detect_dangerous_query(sql),
            Some(DangerousQueryKind::Script)
        );
    }

    #[test]
    fn script_all_safe_statements() {
        let sql = "SELECT 1; SELECT 2; INSERT INTO log VALUES (1)";
        assert_eq!(detect_dangerous_query(sql), None);
    }

    #[test]
    fn script_with_truncate() {
        let sql = "BEGIN; TRUNCATE users; COMMIT";
        assert_eq!(
            detect_dangerous_query(sql),
            Some(DangerousQueryKind::Script)
        );
    }

    #[test]
    fn script_with_drop() {
        let sql = "DROP TABLE temp; DROP TABLE temp2";
        assert_eq!(
            detect_dangerous_query(sql),
            Some(DangerousQueryKind::Script)
        );
    }

    #[test]
    fn script_with_comments_between() {
        let sql = "SELECT 1; -- comment\nDELETE FROM users";
        assert_eq!(
            detect_dangerous_query(sql),
            Some(DangerousQueryKind::Script)
        );
    }

    #[test]
    fn trailing_semicolon_single_statement() {
        assert_eq!(
            detect_dangerous_query("DELETE FROM users;"),
            Some(DangerousQueryKind::DeleteNoWhere)
        );
    }

    #[test]
    fn empty_statements_ignored() {
        assert_eq!(detect_dangerous_query(";;;"), None);
        assert_eq!(
            detect_dangerous_query("; ; DELETE FROM users; ;"),
            Some(DangerousQueryKind::DeleteNoWhere)
        );
    }

    // ==================== Edge cases ====================

    #[test]
    fn empty_input() {
        assert_eq!(detect_dangerous_query(""), None);
        assert_eq!(detect_dangerous_query("   "), None);
        assert_eq!(detect_dangerous_query("\n\t"), None);
    }

    #[test]
    fn where_in_string_literal_not_detected() {
        // Our simple heuristic sees " where " even in strings
        // This is a known limitation - false negative
        let sql = "DELETE FROM users WHERE name = 'test'";
        assert_eq!(detect_dangerous_query(sql), None);
    }

    #[test]
    fn delete_where_1_equals_1_is_technically_safe() {
        let sql = "DELETE FROM users WHERE 1 = 1";
        assert_eq!(detect_dangerous_query(sql), None);
    }

    #[test]
    fn update_from_syntax_postgresql() {
        let sql = "UPDATE users SET active = true FROM temp WHERE users.id = temp.id";
        assert_eq!(detect_dangerous_query(sql), None);
    }

    // ==================== CTE (WITH clause) tests ====================

    #[test]
    fn cte_delete_without_where_is_dangerous() {
        let sql = "WITH cte AS (SELECT 1) DELETE FROM users";
        assert_eq!(
            detect_dangerous_query(sql),
            Some(DangerousQueryKind::DeleteNoWhere)
        );
    }

    #[test]
    fn cte_delete_with_where_is_safe() {
        let sql =
            "WITH cte AS (SELECT id FROM temp) DELETE FROM users WHERE id IN (SELECT id FROM cte)";
        assert_eq!(detect_dangerous_query(sql), None);
    }

    #[test]
    fn cte_update_without_where_is_dangerous() {
        let sql = "WITH vals AS (SELECT 1) UPDATE users SET active = false";
        assert_eq!(
            detect_dangerous_query(sql),
            Some(DangerousQueryKind::UpdateNoWhere)
        );
    }

    #[test]
    fn cte_select_is_safe() {
        let sql = "WITH cte AS (SELECT 1) SELECT * FROM cte";
        assert_eq!(detect_dangerous_query(sql), None);
    }

    #[test]
    fn nested_cte_delete_is_dangerous() {
        let sql = "WITH a AS (SELECT 1), b AS (SELECT * FROM a) DELETE FROM users";
        assert_eq!(
            detect_dangerous_query(sql),
            Some(DangerousQueryKind::DeleteNoWhere)
        );
    }

    #[test]
    fn cte_with_newline_before_delete_is_dangerous() {
        let sql = "WITH cte AS (SELECT 1)\nDELETE FROM users";
        assert_eq!(
            detect_dangerous_query(sql),
            Some(DangerousQueryKind::DeleteNoWhere)
        );
    }

    // ==================== strip_leading_comments tests ====================

    #[test]
    fn strip_no_comments() {
        assert_eq!(strip_leading_comments("SELECT 1"), "SELECT 1");
    }

    #[test]
    fn strip_line_comment() {
        assert_eq!(strip_leading_comments("-- comment\nSELECT 1"), "SELECT 1");
    }

    #[test]
    fn strip_block_comment() {
        assert_eq!(strip_leading_comments("/* block */SELECT 1"), "SELECT 1");
    }

    #[test]
    fn strip_mixed_comments() {
        assert_eq!(
            strip_leading_comments("-- line\n/* block */\n-- another\nSELECT 1"),
            "SELECT 1"
        );
    }

    #[test]
    fn strip_preserves_inline_comment() {
        assert_eq!(
            strip_leading_comments("SELECT 1 -- inline"),
            "SELECT 1 -- inline"
        );
    }

    #[test]
    fn strip_incomplete_block_returns_empty() {
        assert_eq!(strip_leading_comments("/* incomplete"), "");
    }

    #[test]
    fn strip_comment_only_returns_empty() {
        assert_eq!(strip_leading_comments("-- only comment"), "");
    }

    // ==================== DangerousQueryKind::message tests ====================

    #[test]
    fn kind_messages_are_non_empty() {
        assert!(!DangerousQueryKind::DeleteNoWhere.message().is_empty());
        assert!(!DangerousQueryKind::UpdateNoWhere.message().is_empty());
        assert!(!DangerousQueryKind::Truncate.message().is_empty());
        assert!(!DangerousQueryKind::Drop.message().is_empty());
        assert!(!DangerousQueryKind::Alter.message().is_empty());
        assert!(!DangerousQueryKind::Script.message().is_empty());
    }
}
