use dbflux_core::{
    DangerousQueryKind, DiagnosticSeverity, EditorDiagnostic, ExecutionClassification,
    LanguageService, QueryLanguage, TextPosition, TextPositionRange, ValidationResult,
};
use dbflux_js::StaticScanOutcome;

/// MongoDB language service with lightweight syntax/language checks.
pub struct MongoLanguageService;

impl LanguageService for MongoLanguageService {
    fn validate(&self, query: &str) -> ValidationResult {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return ValidationResult::Valid;
        }

        let lower = trimmed.to_ascii_lowercase();
        if lower.starts_with("select ")
            || lower.starts_with("insert into")
            || lower.starts_with("update ")
            || lower.starts_with("delete from")
        {
            return ValidationResult::WrongLanguage {
                expected: QueryLanguage::MongoQuery,
                message: "SQL syntax not supported for MongoDB. Use db.collection.method() or db.method() syntax."
                    .to_string(),
            };
        }

        match crate::query_parser::validate_query(query) {
            Ok(_) => ValidationResult::Valid,
            Err(error) => ValidationResult::SyntaxError(
                dbflux_core::Diagnostic::error(format!("Invalid MongoDB query: {}", error))
                    .with_hint("Use db.collection.method() or db.method() syntax"),
            ),
        }
    }

    fn detect_dangerous(&self, query: &str) -> Option<DangerousQueryKind> {
        if crate::query_parser::is_script_input(query) {
            return match dbflux_js::static_scan(query) {
                // The scan proves the script cannot reach anything worse
                // than a read: no up-front confirmation needed.
                StaticScanOutcome::Read => None,
                // Either the scan cannot prove safety, or the script uses a
                // construct the engine rejects outright — both cases get one
                // up-front confirmation. A rejected construct still aborts
                // at the interpreter with a clear error either way; treating
                // it as dangerous here is the conservative choice, not a
                // correctness requirement.
                StaticScanOutcome::RequiresConfirmation | StaticScanOutcome::Rejected { .. } => {
                    Some(DangerousQueryKind::Script)
                }
            };
        }

        detect_dangerous_mongo(query)
    }

    /// Classifies execution impact for dispatch-boundary governance
    /// (`classify_query_for_language_with_service`).
    ///
    /// Non-script input defers to the core text heuristic by returning
    /// `None` — stage-1 single-statement classification is unchanged. A
    /// JS-looking script's ceiling comes from the same conservative static
    /// scan `detect_dangerous` uses: proven read-only classifies as `Read`;
    /// anything the scan cannot prove safe (including a rejected construct,
    /// which never dispatches) classifies as `Destructive`, the worst case a
    /// dispatch-boundary classification could produce.
    fn classify_execution(&self, query: &str) -> Option<ExecutionClassification> {
        if !crate::query_parser::is_script_input(query) {
            return None;
        }

        Some(match dbflux_js::static_scan(query) {
            StaticScanOutcome::Read => ExecutionClassification::Read,
            StaticScanOutcome::RequiresConfirmation | StaticScanOutcome::Rejected { .. } => {
                ExecutionClassification::Destructive
            }
        })
    }

    fn editor_diagnostics(&self, query: &str) -> Vec<EditorDiagnostic> {
        let trimmed = query.trim();

        if trimmed.is_empty() {
            return vec![];
        }

        let lower = trimmed.to_ascii_lowercase();
        if lower.starts_with("select ")
            || lower.starts_with("insert into")
            || lower.starts_with("update ")
            || lower.starts_with("delete from")
        {
            return vec![EditorDiagnostic {
                severity: DiagnosticSeverity::Error,
                message: "SQL syntax not supported for MongoDB. Use db.collection.method() or db.method() syntax."
                    .to_string(),
                range: full_first_line_range(query),
            }];
        }

        crate::query_parser::validate_query_positional(query)
            .into_iter()
            .map(|error| EditorDiagnostic {
                severity: DiagnosticSeverity::Error,
                message: error.message,
                range: byte_offset_to_range(query, error.offset, error.len),
            })
            .collect()
    }
}

/// Detect dangerous MongoDB shell commands using heuristic pattern matching.
pub(crate) fn detect_dangerous_mongo(query: &str) -> Option<DangerousQueryKind> {
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

fn byte_offset_to_range(source: &str, offset: usize, len: usize) -> TextPositionRange {
    let clamped_offset = offset.min(source.len());
    let clamped_end = (offset + len.max(1))
        .min(source.len())
        .max(clamped_offset + 1);

    let start = byte_offset_to_position(source, clamped_offset);
    let end = byte_offset_to_position(source, clamped_end);

    if start == end {
        return TextPositionRange::new(start, TextPosition::new(start.line, start.column + 1));
    }

    TextPositionRange::new(start, end)
}

fn byte_offset_to_position(source: &str, offset: usize) -> TextPosition {
    let before = &source[..offset.min(source.len())];
    let line = before.matches('\n').count() as u32;
    let last_newline = before.rfind('\n').map(|index| index + 1).unwrap_or(0);
    let column = before[last_newline..].chars().count() as u32;
    TextPosition::new(line, column)
}

fn full_first_line_range(query: &str) -> TextPositionRange {
    let first_line_len = query
        .lines()
        .next()
        .map(|line| line.chars().count())
        .unwrap_or(1) as u32;

    TextPositionRange::new(
        TextPosition::new(0, 0),
        TextPosition::new(0, first_line_len.max(1)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mongo_delete_many_empty_filter_is_dangerous() {
        assert_eq!(
            MongoLanguageService.detect_dangerous("db.users.deleteMany({})"),
            Some(DangerousQueryKind::MongoDeleteMany)
        );
    }

    #[test]
    fn mongo_delete_many_no_args_is_dangerous() {
        assert_eq!(
            MongoLanguageService.detect_dangerous("db.users.deleteMany()"),
            Some(DangerousQueryKind::MongoDeleteMany)
        );
    }

    #[test]
    fn mongo_delete_many_with_filter_is_safe() {
        assert_eq!(
            MongoLanguageService.detect_dangerous(r#"db.users.deleteMany({"archived": true})"#),
            None
        );
    }

    #[test]
    fn mongo_update_many_empty_filter_is_dangerous() {
        assert_eq!(
            MongoLanguageService
                .detect_dangerous(r#"db.users.updateMany({}, {"$set": {"active": false}})"#),
            Some(DangerousQueryKind::MongoUpdateMany)
        );
    }

    #[test]
    fn mongo_update_many_with_filter_is_safe() {
        assert_eq!(
            MongoLanguageService.detect_dangerous(
                r#"db.users.updateMany({"active": true}, {"$set": {"active": false}})"#,
            ),
            None
        );
    }

    #[test]
    fn mongo_drop_collection_is_dangerous() {
        assert_eq!(
            MongoLanguageService.detect_dangerous("db.temp_collection.drop()"),
            Some(DangerousQueryKind::MongoDropCollection)
        );
    }

    #[test]
    fn mongo_drop_database_is_dangerous() {
        assert_eq!(
            MongoLanguageService.detect_dangerous("db.dropDatabase()"),
            Some(DangerousQueryKind::MongoDropDatabase)
        );
    }

    #[test]
    fn mongo_safe_queries_are_safe() {
        assert_eq!(
            MongoLanguageService.detect_dangerous("db.users.find()"),
            None
        );
        assert_eq!(
            MongoLanguageService.detect_dangerous(r#"db.users.find({"name": "John"})"#),
            None
        );
        assert_eq!(
            MongoLanguageService.detect_dangerous(r#"db.users.deleteOne({"_id": "123"})"#),
            None
        );
        assert_eq!(
            MongoLanguageService.detect_dangerous(r#"db.users.insertOne({"name": "Alice"})"#),
            None
        );
        assert_eq!(
            MongoLanguageService
                .detect_dangerous(r#"db.orders.aggregate([{"$match": {"status": "active"}}])"#),
            None
        );
    }

    #[test]
    fn mongo_detection_is_case_insensitive() {
        assert_eq!(
            MongoLanguageService.detect_dangerous("db.users.DELETEMANY({})"),
            Some(DangerousQueryKind::MongoDeleteMany)
        );
        assert_eq!(
            MongoLanguageService.detect_dangerous("db.users.DeleteMany({})"),
            Some(DangerousQueryKind::MongoDeleteMany)
        );
    }

    #[test]
    fn mongo_editor_diagnostics_flag_wrong_language() {
        let diagnostics = MongoLanguageService.editor_diagnostics("SELECT * FROM users");
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, DiagnosticSeverity::Error);
    }

    // ==================== T10: classify_execution / JS-aware detect_dangerous ====================

    #[test]
    fn classify_execution_defers_to_core_heuristic_for_a_single_statement() {
        // Non-script input must return None, letting the core fallback
        // (`classify_mongo_query`) decide — stage-1 parity.
        assert_eq!(
            MongoLanguageService.classify_execution("db.users.find({})"),
            None
        );
        assert_eq!(
            MongoLanguageService.classify_execution("db.dropDatabase()"),
            None
        );
    }

    #[test]
    fn classify_execution_on_read_only_script_returns_read() {
        let script = "db.users.find({}); db.orders.aggregate([]);";
        assert_eq!(
            MongoLanguageService.classify_execution(script),
            Some(ExecutionClassification::Read)
        );
    }

    #[test]
    fn classify_execution_on_unproven_script_returns_destructive() {
        let script = "const m = 'deleteMany'; db.users[m]({});";
        assert_eq!(
            MongoLanguageService.classify_execution(script),
            Some(ExecutionClassification::Destructive)
        );
    }

    #[test]
    fn detect_dangerous_on_proven_read_only_script_requires_no_confirmation() {
        let script = "db.users.find({}); db.orders.aggregate([]);";
        assert_eq!(MongoLanguageService.detect_dangerous(script), None);
    }

    #[test]
    fn detect_dangerous_on_unproven_script_requires_confirmation() {
        let script = "db.users.insertOne({name: 'a'}); db.users.find({});";
        assert_eq!(
            MongoLanguageService.detect_dangerous(script),
            Some(DangerousQueryKind::Script)
        );
    }
}
