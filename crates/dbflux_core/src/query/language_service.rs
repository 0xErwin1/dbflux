use crate::QueryLanguage;
use dbflux_policy::ExecutionClassification;
use tree_sitter::{Node, Parser};

use super::safety::classify_sql_execution;

/// Severity level for a diagnostic message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticSeverity {
    Error,
    Warning,
    Info,
    Hint,
}

/// Byte range within a query string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextRange {
    pub start: usize,
    pub end: usize,
}

/// A position in a document (zero-based line and column).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextPosition {
    pub line: u32,
    pub column: u32,
}

impl TextPosition {
    pub fn new(line: u32, column: u32) -> Self {
        Self { line, column }
    }
}

/// A range of positions in a document for editor diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextPositionRange {
    pub start: TextPosition,
    pub end: TextPosition,
}

impl TextPositionRange {
    pub fn new(start: TextPosition, end: TextPosition) -> Self {
        Self { start, end }
    }
}

/// A diagnostic with precise line/column position for editor display.
#[derive(Debug, Clone)]
pub struct EditorDiagnostic {
    pub severity: DiagnosticSeverity,
    pub message: String,
    pub range: TextPositionRange,
}

/// Structured diagnostic message from query validation.
#[derive(Debug, Clone)]
pub struct Diagnostic {
    pub severity: DiagnosticSeverity,
    pub message: String,
    pub hint: Option<String>,
    pub code: Option<String>,
    pub range: Option<TextRange>,
}

impl Diagnostic {
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            severity: DiagnosticSeverity::Error,
            message: message.into(),
            hint: None,
            code: None,
            range: None,
        }
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    pub fn with_range(mut self, range: TextRange) -> Self {
        self.range = Some(range);
        self
    }
}

impl From<String> for Diagnostic {
    fn from(message: String) -> Self {
        Self::error(message)
    }
}

impl From<&str> for Diagnostic {
    fn from(message: &str) -> Self {
        Self::error(message)
    }
}

/// Categories of potentially dangerous queries that require user confirmation.
///
/// Each variant maps to a destructive pattern detected by driver-local or
/// shared heuristic analysis.
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

    // Redis patterns
    /// FLUSHALL — wipes all databases
    RedisFlushAll,
    /// FLUSHDB — wipes current database
    RedisFlushDb,
    /// DEL with multiple keys
    RedisMultiDelete,
    /// KEYS * — performance hazard on large databases
    RedisKeysPattern,

    // Visual mutation builder
    /// At least one SET assignment uses a raw SQL expression rather than a
    /// bound parameter. Forces the hard-confirm modal regardless of other gates.
    RawExpressionInSet,
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
            Self::RedisFlushAll => "FLUSHALL will delete all keys in all databases",
            Self::RedisFlushDb => "FLUSHDB will delete all keys in the current database",
            Self::RedisMultiDelete => "DEL with multiple keys will delete them all",
            Self::RedisKeysPattern => {
                "KEYS with a pattern is a performance hazard on large databases"
            }
            Self::RawExpressionInSet => {
                "SET clause contains a raw SQL expression — bypasses parameter binding"
            }
        }
    }
}

/// The result of the double-gate classification applied to a `VisualMutationSpec`.
///
/// Both `spec_kinds` and `text_kinds` are checked independently; `effective`
/// is their union (deduplicated). `requires_hard_confirm` is true when the
/// effective set is non-empty OR when `RawExpressionInSet` is present.
#[derive(Debug, Clone, PartialEq)]
pub struct ClassifiedMutation {
    /// Dangers detected by inspecting the spec structure (no-WHERE, raw expression).
    pub spec_kinds: Vec<DangerousQueryKind>,
    /// Dangers detected by running the generated SQL text through the language gate.
    /// Always empty for visual-spec classification (text check is caller's responsibility).
    pub text_kinds: Vec<DangerousQueryKind>,
    /// Deduplicated union of `spec_kinds` and `text_kinds`.
    pub effective: Vec<DangerousQueryKind>,
    /// True when any effective kind warrants the hard-confirm flow (Danger modal +
    /// TypeToConfirm). All current effective kinds qualify.
    pub requires_hard_confirm: bool,
}

/// Classifies a `VisualMutationSpec` for dangerous patterns at the spec level.
///
/// This is one half of the double-gate (DR-7.1, DR-7.2, DR-7.4). The other
/// half — text-level classification of the generated SQL — is the caller's
/// responsibility via `classify_query_for_language`.
pub fn classify_visual_mutation(
    spec: &crate::query::visual_query::VisualMutationSpec,
) -> ClassifiedMutation {
    use crate::query::visual_query::{AssignmentValue, MutationKind};

    let mut spec_kinds: Vec<DangerousQueryKind> = Vec::new();

    match &spec.kind {
        MutationKind::Delete => {
            if spec.filter.is_none() {
                spec_kinds.push(DangerousQueryKind::DeleteNoWhere);
            }
        }

        MutationKind::Update { assignments } => {
            if spec.filter.is_none() {
                spec_kinds.push(DangerousQueryKind::UpdateNoWhere);
            }

            let has_raw_expression = assignments
                .iter()
                .any(|a| matches!(&a.value, AssignmentValue::Expression(_)));

            if has_raw_expression {
                spec_kinds.push(DangerousQueryKind::RawExpressionInSet);
            }
        }
    }

    let effective = spec_kinds.clone();
    let requires_hard_confirm = !effective.is_empty();

    ClassifiedMutation {
        spec_kinds,
        text_kinds: Vec::new(),
        effective,
        requires_hard_confirm,
    }
}

pub fn classify_query_for_language(
    query_language: &QueryLanguage,
    query: &str,
) -> ExecutionClassification {
    match query_language {
        QueryLanguage::Sql => classify_sql_execution(query),
        QueryLanguage::CloudWatchLogsInsightsQl
        | QueryLanguage::OpenSearchPpl
        | QueryLanguage::OpenSearchSql => ExecutionClassification::Read,
        QueryLanguage::MongoQuery => classify_mongo_query(query),
        QueryLanguage::RedisCommands => classify_redis_query(query),
        _ => ExecutionClassification::Write,
    }
}

fn classify_mongo_query(query: &str) -> ExecutionClassification {
    let normalized = query.trim().to_ascii_lowercase();

    if normalized.is_empty() {
        return ExecutionClassification::Metadata;
    }

    if normalized.contains(".dropdatabase(") {
        return ExecutionClassification::Destructive;
    }

    if normalized.contains(".drop(") && !normalized.contains(".dropdatabase(") {
        return ExecutionClassification::Destructive;
    }

    if let Some(pos) = normalized.find(".deletemany(") {
        let after_paren = &normalized[pos + 12..];
        if is_empty_filter(after_paren) {
            return ExecutionClassification::Write;
        }
    }

    if let Some(pos) = normalized.find(".updatemany(") {
        let after_paren = &normalized[pos + 12..];
        if is_empty_filter(after_paren) {
            return ExecutionClassification::Write;
        }
    }

    if normalized.contains(".find(") || normalized.contains(".aggregate(") {
        return ExecutionClassification::Read;
    }

    if normalized.contains(".countdocuments(") || normalized.contains(".estimateddocumentcount(") {
        return ExecutionClassification::Metadata;
    }

    if normalized.contains(".insert")
        || normalized.contains(".update")
        || normalized.contains(".delete")
        || normalized.contains(".replace")
    {
        return ExecutionClassification::Write;
    }

    if normalized.contains("createuser")
        || normalized.contains("grantroles")
        || normalized.contains("revoke")
        || normalized.contains("shutdownserver")
    {
        return ExecutionClassification::Admin;
    }

    ExecutionClassification::Write
}

fn classify_redis_query(query: &str) -> ExecutionClassification {
    let normalized = query.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return ExecutionClassification::Metadata;
    }

    let Some(command) = normalized.split_whitespace().next() else {
        return ExecutionClassification::Metadata;
    };

    match command {
        "flushall" | "flushdb" => return ExecutionClassification::Destructive,
        "del" if normalized.split_whitespace().skip(1).count() > 1 => {
            return ExecutionClassification::Write;
        }
        "keys" => return ExecutionClassification::Read,
        _ => {}
    }

    match command {
        "info" | "dbsize" | "type" | "ttl" | "pttl" | "exists" | "llen" | "hget" | "hmget"
        | "hkeys" | "hvals" | "lrange" | "zrange" | "smembers" | "get" | "mget" | "scan" => {
            ExecutionClassification::Read
        }
        "command" | "help" => ExecutionClassification::Metadata,
        "config" | "acl" | "script" | "module" => ExecutionClassification::Admin,
        _ => ExecutionClassification::Write,
    }
}

/// Result of validating a query before execution.
#[derive(Debug, Clone)]
pub enum ValidationResult {
    /// Query is valid and ready to execute.
    Valid,

    /// Query has a syntax error.
    SyntaxError(Diagnostic),

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

    /// Produce diagnostics for the editor with precise line/column positions.
    ///
    /// Returns an empty vec if no problems are found. The UI may call this on
    /// frequent text changes to provide live feedback as the user types.
    fn editor_diagnostics(&self, _query: &str) -> Vec<EditorDiagnostic> {
        vec![]
    }
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

    fn editor_diagnostics(&self, query: &str) -> Vec<EditorDiagnostic> {
        sql_editor_diagnostics(query)
    }
}

/// Produce editor diagnostics for SQL using tree-sitter error nodes.
fn sql_editor_diagnostics(query: &str) -> Vec<EditorDiagnostic> {
    if query.trim().is_empty() {
        return vec![];
    }

    let mut parser = Parser::new();
    let language = tree_sitter::Language::new(tree_sitter_sequel::LANGUAGE);

    if parser.set_language(&language).is_err() {
        return vec![];
    }

    let Some(tree) = parser.parse(query, None) else {
        return vec![];
    };

    if !tree.root_node().has_error() {
        return vec![];
    }

    if should_skip_sql_parse_diagnostics(query) {
        return vec![];
    }

    let mut diagnostics = Vec::new();
    collect_error_nodes(tree.root_node(), query, &mut diagnostics);
    diagnostics
}

/// Detect whether the query contains a complete PostgreSQL dollar-quoted block
/// (e.g. `$$ ... $$` or `$tag$ ... $tag$`).
///
/// The bundled tree-sitter SQL grammar does not understand dollar quoting or
/// PL/pgSQL bodies, so any query using them produces spurious ERROR nodes.
/// Requiring a *closed* block avoids masking genuine syntax errors in plain
/// SQL that merely contains a stray `$`.
fn contains_dollar_quoted_block(query: &str) -> bool {
    let bytes = query.as_bytes();
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] != b'$' {
            index += 1;
            continue;
        }

        let mut tag_end = index + 1;
        while tag_end < bytes.len()
            && (bytes[tag_end] == b'_' || bytes[tag_end].is_ascii_alphanumeric())
        {
            tag_end += 1;
        }

        if tag_end < bytes.len() && bytes[tag_end] == b'$' {
            let tag = &query[index..=tag_end];

            if query[tag_end + 1..].contains(tag) {
                return true;
            }
        }

        index += 1;
    }

    false
}

fn should_skip_sql_parse_diagnostics(query: &str) -> bool {
    if contains_dollar_quoted_block(query) {
        return true;
    }

    query
        .split(';')
        .map(strip_leading_comments)
        .filter(|statement| !statement.is_empty())
        .all(is_postgres_grant_or_revoke_statement)
}

fn is_postgres_grant_or_revoke_statement(statement: &str) -> bool {
    let normalized = statement.trim_start().to_ascii_uppercase();

    if !(normalized.starts_with("GRANT ") || normalized.starts_with("REVOKE ")) {
        return false;
    }

    normalized.contains(" ON SCHEMA ") || normalized.contains(" ON ALL TABLES IN SCHEMA ")
}

/// Walk the tree-sitter parse tree and collect ERROR / MISSING nodes.
fn collect_error_nodes(node: Node, source: &str, diagnostics: &mut Vec<EditorDiagnostic>) {
    if node.is_error() {
        let start = node.start_position();
        let end = node.end_position();

        let snippet = source.get(node.byte_range()).unwrap_or("");
        let display = crate::truncate_string_safe(snippet, 40);

        diagnostics.push(EditorDiagnostic {
            severity: DiagnosticSeverity::Error,
            message: format!("Unexpected: {display}"),
            range: TextPositionRange::new(
                TextPosition::new(start.row as u32, start.column as u32),
                TextPosition::new(end.row as u32, end.column as u32),
            ),
        });
        return;
    }

    if node.is_missing() {
        let start = node.start_position();
        let end = node.end_position();
        let kind = node.kind();

        diagnostics.push(EditorDiagnostic {
            severity: DiagnosticSeverity::Error,
            message: format!("Missing: {kind}"),
            range: TextPositionRange::new(
                TextPosition::new(start.row as u32, start.column as u32),
                TextPosition::new(end.row as u32, end.column as u32),
            ),
        });
        return;
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_error_nodes(child, source, diagnostics);
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

/// Unified entry point for shared SQL dangerous-query checks.
pub fn detect_dangerous_query(query: &str) -> Option<DangerousQueryKind> {
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
        assert!(!DangerousQueryKind::RedisFlushAll.message().is_empty());
        assert!(!DangerousQueryKind::RedisFlushDb.message().is_empty());
        assert!(!DangerousQueryKind::RedisMultiDelete.message().is_empty());
        assert!(!DangerousQueryKind::RedisKeysPattern.message().is_empty());
        assert!(!DangerousQueryKind::RawExpressionInSet.message().is_empty());
    }

    // T-05/T-06 — RawExpressionInSet variant (spec scenario DR-2.5, design R-D1)
    #[test]
    fn raw_expression_in_set_variant_exists_and_has_message() {
        let kind = DangerousQueryKind::RawExpressionInSet;
        let msg = kind.message();
        assert!(
            !msg.is_empty(),
            "RawExpressionInSet must have a non-empty message"
        );
        assert!(
            msg.contains("expression") || msg.contains("binding") || msg.contains("SET"),
            "message should describe the risk: {msg}"
        );
    }

    #[test]
    fn valid_sql_has_no_diagnostics() {
        let diags = sql_editor_diagnostics("SELECT * FROM users WHERE id = 1");
        assert!(diags.is_empty());
    }

    #[test]
    fn empty_sql_has_no_diagnostics() {
        assert!(sql_editor_diagnostics("").is_empty());
        assert!(sql_editor_diagnostics("   ").is_empty());
    }

    #[test]
    fn syntax_error_produces_diagnostic() {
        let diags = sql_editor_diagnostics("SELEC * FROM users");
        assert!(!diags.is_empty());
        assert_eq!(diags[0].severity, DiagnosticSeverity::Error);
    }

    #[test]
    fn unclosed_paren_produces_diagnostic() {
        let diags = sql_editor_diagnostics("SELECT * FROM users WHERE id IN (1, 2");
        assert!(!diags.is_empty());
    }

    #[test]
    fn multiple_valid_statements_no_diagnostics() {
        let diags = sql_editor_diagnostics("SELECT 1; SELECT 2;");
        assert!(diags.is_empty());
    }

    #[test]
    fn postgres_grant_on_schema_has_no_diagnostics() {
        let diags = sql_editor_diagnostics("GRANT USAGE ON SCHEMA public TO sesquire;");
        assert!(diags.is_empty());
    }

    #[test]
    fn postgres_grant_on_all_tables_in_schema_has_no_diagnostics() {
        let diags = sql_editor_diagnostics(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO sesquire;",
        );
        assert!(diags.is_empty());
    }

    #[test]
    fn postgres_do_dollar_quoted_block_has_no_diagnostics() {
        let query = r#"DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    LOOP
        EXECUTE format(
            'GRANT SELECT ON TABLE %I.%I TO sesquire',
            r.schemaname,
            r.tablename
        );
    END LOOP;
END $$;"#;
        assert!(sql_editor_diagnostics(query).is_empty());
    }

    #[test]
    fn postgres_tagged_dollar_quoted_block_has_no_diagnostics() {
        let diags = sql_editor_diagnostics("DO $body$ BEGIN PERFORM 1; END $body$;");
        assert!(diags.is_empty());
    }

    #[test]
    fn stray_dollar_sign_still_reports_diagnostics() {
        // A lone `$` is not a closed dollar-quoted block, so real errors still surface.
        let diags = sql_editor_diagnostics("SELEC $ FROM users");
        assert!(!diags.is_empty());
    }

    #[test]
    fn language_classification_escalates_ambiguous_queries() {
        assert_eq!(
            classify_query_for_language(&QueryLanguage::MongoQuery, "db.users.customOp()"),
            ExecutionClassification::Write
        );

        assert_eq!(
            classify_query_for_language(&QueryLanguage::RedisCommands, "CONFIG SET a b"),
            ExecutionClassification::Admin
        );
    }

    // T-11 — [RED] Tests for classify_visual_mutation (spec scenarios C-1 through C-6, DR-7.1–DR-7.6)

    #[cfg(test)]
    mod classify_visual_mutation_tests {
        use super::*;
        use crate::query::table_browser::TableRef;
        use crate::query::visual_query::{
            Assignment, AssignmentValue, Comparator, FilterNode, LiteralValue, MutationKind,
            Predicate, PredicateValue, ScalarLiteral, VisualMutationSpec,
        };

        fn table_ref(name: &str) -> TableRef {
            TableRef {
                schema: None,
                name: name.to_string(),
            }
        }

        fn filter_id_eq_1() -> FilterNode {
            FilterNode::Predicate(Predicate {
                source_alias: "t".to_string(),
                column: "id".to_string(),
                comparator: Comparator::Eq,
                value: PredicateValue::Single(LiteralValue::Integer(1)),
                node_id: 0,
            })
        }

        fn literal_assignment(col: &str) -> Assignment {
            Assignment {
                column: col.to_string(),
                value: AssignmentValue::Literal(ScalarLiteral::Text("v".to_string())),
            }
        }

        fn expr_assignment(col: &str) -> Assignment {
            Assignment {
                column: col.to_string(),
                value: AssignmentValue::Expression("price * 1.1".to_string()),
            }
        }

        // C-1: spec-level gate — Delete no WHERE → DeleteNoWhere
        #[test]
        fn c1_delete_no_filter_returns_delete_no_where() {
            let spec = VisualMutationSpec {
                from: table_ref("orders"),
                filter: None,
                kind: MutationKind::Delete,
            };
            let result = classify_visual_mutation(&spec);
            assert!(
                result
                    .spec_kinds
                    .contains(&DangerousQueryKind::DeleteNoWhere)
            );
            assert!(
                result
                    .effective
                    .contains(&DangerousQueryKind::DeleteNoWhere)
            );
        }

        // C-2: spec-level gate — Update no WHERE → UpdateNoWhere
        #[test]
        fn c2_update_no_filter_returns_update_no_where() {
            let spec = VisualMutationSpec {
                from: table_ref("users"),
                filter: None,
                kind: MutationKind::Update {
                    assignments: vec![literal_assignment("name")],
                },
            };
            let result = classify_visual_mutation(&spec);
            assert!(
                result
                    .spec_kinds
                    .contains(&DangerousQueryKind::UpdateNoWhere)
            );
            assert!(
                result
                    .effective
                    .contains(&DangerousQueryKind::UpdateNoWhere)
            );
        }

        // C-3: spec-level gate passes with filter
        #[test]
        fn c3_delete_with_filter_no_spec_danger() {
            let spec = VisualMutationSpec {
                from: table_ref("orders"),
                filter: Some(filter_id_eq_1()),
                kind: MutationKind::Delete,
            };
            let result = classify_visual_mutation(&spec);
            assert!(
                !result
                    .spec_kinds
                    .contains(&DangerousQueryKind::DeleteNoWhere)
            );
            assert!(
                !result
                    .spec_kinds
                    .contains(&DangerousQueryKind::UpdateNoWhere)
            );
        }

        // C-6: both checks clear for safe mutation
        #[test]
        fn c6_update_with_filter_and_literal_is_safe() {
            let spec = VisualMutationSpec {
                from: table_ref("users"),
                filter: Some(filter_id_eq_1()),
                kind: MutationKind::Update {
                    assignments: vec![literal_assignment("name")],
                },
            };
            let result = classify_visual_mutation(&spec);
            assert!(result.spec_kinds.is_empty());
            assert!(!result.requires_hard_confirm);
        }

        // RawExpressionInSet when any assignment uses Expression variant
        #[test]
        fn raw_expression_triggers_raw_expression_in_set() {
            let spec = VisualMutationSpec {
                from: table_ref("products"),
                filter: Some(filter_id_eq_1()),
                kind: MutationKind::Update {
                    assignments: vec![expr_assignment("price")],
                },
            };
            let result = classify_visual_mutation(&spec);
            assert!(
                result
                    .spec_kinds
                    .contains(&DangerousQueryKind::RawExpressionInSet)
            );
            assert!(
                result
                    .effective
                    .contains(&DangerousQueryKind::RawExpressionInSet)
            );
            assert!(result.requires_hard_confirm);
        }

        // RawExpressionInSet forces hard confirm even when filter is present
        #[test]
        fn raw_expression_with_filter_still_hard_confirm() {
            let spec = VisualMutationSpec {
                from: table_ref("products"),
                filter: Some(filter_id_eq_1()),
                kind: MutationKind::Update {
                    assignments: vec![literal_assignment("name"), expr_assignment("computed")],
                },
            };
            let result = classify_visual_mutation(&spec);
            assert!(result.requires_hard_confirm);
        }

        // effective contains union of spec_kinds (no text_kinds for visual specs)
        #[test]
        fn effective_is_superset_of_spec_kinds() {
            let spec = VisualMutationSpec {
                from: table_ref("orders"),
                filter: None,
                kind: MutationKind::Delete,
            };
            let result = classify_visual_mutation(&spec);
            for k in &result.spec_kinds {
                assert!(
                    result.effective.contains(k),
                    "effective must include all spec_kinds"
                );
            }
        }

        // Delete with no filter is hard confirm
        #[test]
        fn delete_no_where_requires_hard_confirm() {
            let spec = VisualMutationSpec {
                from: table_ref("orders"),
                filter: None,
                kind: MutationKind::Delete,
            };
            let result = classify_visual_mutation(&spec);
            assert!(result.requires_hard_confirm);
        }

        // Update with no filter is hard confirm
        #[test]
        fn update_no_where_requires_hard_confirm() {
            let spec = VisualMutationSpec {
                from: table_ref("users"),
                filter: None,
                kind: MutationKind::Update {
                    assignments: vec![literal_assignment("status")],
                },
            };
            let result = classify_visual_mutation(&spec);
            assert!(result.requires_hard_confirm);
        }
    }
}
