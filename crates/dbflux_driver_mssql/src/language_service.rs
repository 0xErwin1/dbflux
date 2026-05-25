use dbflux_core::{
    DangerousQueryKind, EditorDiagnostic, LanguageService, ValidationResult, detect_dangerous_sql,
};

/// Language service for T-SQL (Microsoft SQL Server).
///
/// Behaves like the generic `SqlLanguageService` for dangerous-query detection
/// (the destructive keywords DROP/TRUNCATE/DELETE are spelled identically), but
/// suppresses live parse diagnostics entirely. The bundled `tree-sitter-sequel`
/// grammar follows generic ANSI SQL and flags T-SQL-only constructs as errors —
/// `SELECT TOP n`, `OUTPUT INSERTED.*`, `MERGE`, `CROSS APPLY / OUTER APPLY`,
/// table hints like `WITH (NOLOCK)`, `OFFSET … ROWS FETCH NEXT … ROWS ONLY`,
/// etc. The server is the source of truth for syntax validity; surfacing parser
/// false-positives in the editor only adds noise.
pub struct TSqlLanguageService;

impl LanguageService for TSqlLanguageService {
    fn validate(&self, _query: &str) -> ValidationResult {
        ValidationResult::Valid
    }

    fn detect_dangerous(&self, query: &str) -> Option<DangerousQueryKind> {
        detect_dangerous_sql(query)
    }

    fn editor_diagnostics(&self, _query: &str) -> Vec<EditorDiagnostic> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_returns_valid_for_any_input() {
        let svc = TSqlLanguageService;

        assert!(matches!(
            svc.validate("SELECT TOP 10 * FROM sys.tables"),
            ValidationResult::Valid
        ));
        assert!(matches!(svc.validate(""), ValidationResult::Valid));
        assert!(matches!(
            svc.validate("SELECT FROM WHERE"),
            ValidationResult::Valid
        ));
    }

    #[test]
    fn editor_diagnostics_returns_empty() {
        let svc = TSqlLanguageService;

        assert!(svc
            .editor_diagnostics("SELECT TOP 1 id FROM dbo.users WITH (NOLOCK)")
            .is_empty());
        assert!(svc
            .editor_diagnostics("EXEC sp_helpdb 'master'")
            .is_empty());
        assert!(svc.editor_diagnostics("").is_empty());
    }

    #[test]
    fn detect_dangerous_drops_table() {
        let svc = TSqlLanguageService;

        assert_eq!(
            svc.detect_dangerous("DROP TABLE dbo.orders"),
            Some(DangerousQueryKind::Drop)
        );
        assert_eq!(svc.detect_dangerous("SELECT * FROM dbo.orders"), None);
    }
}
