use crate::QueryLanguage;

/// Categories of potentially dangerous queries that require user confirmation.
///
/// Each variant maps to a destructive pattern detected by heuristic analysis.
/// Both SQL and document-database patterns are represented here so the core
/// owns the full definition and the UI never needs to know query syntax.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DangerousQueryKind {
    // SQL patterns
    DeleteNoWhere,
    UpdateNoWhere,
    Truncate,
    Drop,
    Alter,
    /// Multi-statement script containing at least one dangerous query.
    Script,

    // MongoDB patterns
    /// deleteMany with empty or missing filter ({} or no arguments)
    MongoDeleteMany,
    /// updateMany with empty filter
    MongoUpdateMany,
    /// db.collection.drop()
    MongoDropCollection,
    /// db.dropDatabase()
    MongoDropDatabase,
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
            Self::MongoDeleteMany => "deleteMany with empty filter will delete all documents",
            Self::MongoUpdateMany => "updateMany with empty filter will update all documents",
            Self::MongoDropCollection => "drop() will permanently remove the collection",
            Self::MongoDropDatabase => "dropDatabase() will permanently remove the entire database",
        }
    }
}

/// Result of validating a query before execution.
#[derive(Debug, Clone)]
pub enum ValidationResult {
    /// Query is valid and ready to execute.
    Valid,

    /// Query has a syntax error.
    SyntaxError(String),

    /// Query uses syntax from the wrong language (e.g., SQL on a MongoDB connection).
    WrongLanguage {
        expected: QueryLanguage,
        message: String,
    },
}

/// Language-specific services provided by a database driver.
///
/// The core resolves the appropriate `LanguageService` for a session based on
/// the driver's `QueryLanguage`. The UI/editor calls these methods without
/// knowing which database engine is behind them.
pub trait LanguageService: Send + Sync {
    /// Validate a query string before execution.
    ///
    /// Returns `Valid` if the query can be executed, or an error describing
    /// what is wrong. This is a lightweight check (no server round-trip).
    fn validate(&self, query: &str) -> ValidationResult;

    /// Detect if a query is potentially dangerous and requires confirmation.
    ///
    /// Returns `None` for safe queries. The UI shows a confirmation dialog
    /// for dangerous queries without needing to understand the syntax.
    fn detect_dangerous(&self, query: &str) -> Option<DangerousQueryKind>;
}

/// Default SQL language service that handles standard SQL dangerous-query detection.
///
/// This is used by relational drivers (Postgres, MySQL, SQLite) that share
/// common SQL patterns for destructive operations.
pub struct SqlLanguageService;

impl LanguageService for SqlLanguageService {
    fn validate(&self, _query: &str) -> ValidationResult {
        ValidationResult::Valid
    }

    fn detect_dangerous(&self, query: &str) -> Option<DangerousQueryKind> {
        detect_dangerous_sql(query)
    }
}

/// Detect dangerous SQL queries using heuristic pattern matching.
///
/// For multi-statement scripts (containing `;`), returns `Script` if any
/// statement is dangerous. Not a full parser — may have false positives.
pub fn detect_dangerous_sql(query: &str) -> Option<DangerousQueryKind> {
    let clean = strip_leading_comments(query);
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

/// Detect dangerous MongoDB shell commands using heuristic pattern matching.
pub fn detect_dangerous_mongo(query: &str) -> Option<DangerousQueryKind> {
    let normalized = query.trim().to_lowercase();

    if normalized.contains(".dropdatabase(") {
        return Some(DangerousQueryKind::MongoDropDatabase);
    }

    if normalized.contains(".drop(") && !normalized.contains(".dropdatabase(") {
        return Some(DangerousQueryKind::MongoDropCollection);
    }

    if let Some(pos) = normalized.find(".deletemany(") {
        let after_paren = &normalized[pos + 12..];
        if is_empty_filter(after_paren) {
            return Some(DangerousQueryKind::MongoDeleteMany);
        }
    }

    if let Some(pos) = normalized.find(".updatemany(") {
        let after_paren = &normalized[pos + 12..];
        if is_empty_filter(after_paren) {
            return Some(DangerousQueryKind::MongoUpdateMany);
        }
    }

    None
}

/// Unified entry point: auto-detects language and checks for dangerous patterns.
///
/// Detects MongoDB shell syntax (`db.`) vs SQL automatically.
pub fn detect_dangerous_query(query: &str) -> Option<DangerousQueryKind> {
    let clean = strip_leading_comments(query);
    if clean.is_empty() {
        return None;
    }

    if clean.trim().starts_with("db.") {
        return detect_dangerous_mongo(clean);
    }

    detect_dangerous_sql(query)
}

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

fn skip_cte_prefix(sql: &str) -> &str {
    if !sql.starts_with("with ") {
        return sql;
    }

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

fn contains_where_clause(normalized_sql: &str) -> bool {
    normalized_sql.contains(" where ")
}

fn is_empty_filter(args_start: &str) -> bool {
    let trimmed = args_start.trim();

    if trimmed.starts_with(')') {
        return true;
    }

    if trimmed.starts_with("{}") {
        return true;
    }

    if let Some(brace_end) = trimmed.find('}') {
        let inside = &trimmed[1..brace_end];
        if inside.trim().is_empty() {
            return true;
        }
    }

    false
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
        assert!(!DangerousQueryKind::MongoDeleteMany.message().is_empty());
        assert!(!DangerousQueryKind::MongoUpdateMany.message().is_empty());
        assert!(!DangerousQueryKind::MongoDropCollection.message().is_empty());
        assert!(!DangerousQueryKind::MongoDropDatabase.message().is_empty());
    }

    // ==================== MongoDB tests ====================

    #[test]
    fn mongo_delete_many_empty_filter_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("db.users.deleteMany({})"),
            Some(DangerousQueryKind::MongoDeleteMany)
        );
    }

    #[test]
    fn mongo_delete_many_no_args_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("db.users.deleteMany()"),
            Some(DangerousQueryKind::MongoDeleteMany)
        );
    }

    #[test]
    fn mongo_delete_many_with_filter_is_safe() {
        assert_eq!(
            detect_dangerous_query(r#"db.users.deleteMany({"archived": true})"#),
            None
        );
    }

    #[test]
    fn mongo_update_many_empty_filter_is_dangerous() {
        assert_eq!(
            detect_dangerous_query(r#"db.users.updateMany({}, {"$set": {"active": false}})"#),
            Some(DangerousQueryKind::MongoUpdateMany)
        );
    }

    #[test]
    fn mongo_update_many_with_filter_is_safe() {
        assert_eq!(
            detect_dangerous_query(
                r#"db.users.updateMany({"status": "old"}, {"$set": {"archived": true}})"#
            ),
            None
        );
    }

    #[test]
    fn mongo_drop_collection_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("db.temp_collection.drop()"),
            Some(DangerousQueryKind::MongoDropCollection)
        );
    }

    #[test]
    fn mongo_drop_database_is_dangerous() {
        assert_eq!(
            detect_dangerous_query("db.dropDatabase()"),
            Some(DangerousQueryKind::MongoDropDatabase)
        );
    }

    #[test]
    fn mongo_find_is_safe() {
        assert_eq!(detect_dangerous_query("db.users.find()"), None);
        assert_eq!(detect_dangerous_query("db.users.find({})"), None);
        assert_eq!(
            detect_dangerous_query(r#"db.users.find({"name": "John"})"#),
            None
        );
    }

    #[test]
    fn mongo_delete_one_is_safe() {
        assert_eq!(
            detect_dangerous_query(r#"db.users.deleteOne({"_id": "123"})"#),
            None
        );
    }

    #[test]
    fn mongo_insert_is_safe() {
        assert_eq!(
            detect_dangerous_query(r#"db.users.insertOne({"name": "Alice"})"#),
            None
        );
        assert_eq!(
            detect_dangerous_query(r#"db.users.insertMany([{"name": "A"}, {"name": "B"}])"#),
            None
        );
    }

    #[test]
    fn mongo_aggregate_is_safe() {
        assert_eq!(
            detect_dangerous_query(r#"db.orders.aggregate([{"$match": {"status": "active"}}])"#),
            None
        );
    }

    #[test]
    fn mongo_case_insensitive() {
        assert_eq!(
            detect_dangerous_query("db.users.DELETEMANY({})"),
            Some(DangerousQueryKind::MongoDeleteMany)
        );
        assert_eq!(
            detect_dangerous_query("db.users.DeleteMany({})"),
            Some(DangerousQueryKind::MongoDeleteMany)
        );
    }
}
