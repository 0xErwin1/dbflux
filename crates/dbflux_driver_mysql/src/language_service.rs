use dbflux_core::{
    DangerousQueryKind, EditorDiagnostic, LanguageService, ValidationResult, detect_dangerous_sql,
};

/// Language service for MySQL and MariaDB.
///
/// Behaves like the generic `SqlLanguageService` for dangerous-query detection
/// (DROP, TRUNCATE, DELETE without WHERE are spelled identically in MySQL),
/// but suppresses live parse diagnostics entirely. The bundled
/// `tree-sitter-sequel` grammar follows generic ANSI SQL and produces ERROR
/// nodes for legal MySQL DCL statements such as
/// `CREATE USER 'u'@'h' IDENTIFIED BY '...'`,
/// `GRANT ALL PRIVILEGES ON db.* TO 'u'@'h'`, and `FLUSH PRIVILEGES`. The
/// server is the source of truth for syntax validity; surfacing parser
/// false-positives in the editor only adds noise.
pub struct MySqlLanguageService;

impl LanguageService for MySqlLanguageService {
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
    fn validate_always_valid() {
        let svc = MySqlLanguageService;
        assert!(matches!(svc.validate("SELECT 1"), ValidationResult::Valid));
        assert!(matches!(
            svc.validate("GRANT ALL PRIVILEGES ON db.* TO 'u'@'h'"),
            ValidationResult::Valid
        ));
        assert!(matches!(
            svc.validate("CREATE USER 'u'@'h' IDENTIFIED BY 'pass'"),
            ValidationResult::Valid
        ));
    }

    #[test]
    fn editor_diagnostics_empty_for_create_user() {
        let svc = MySqlLanguageService;
        let diags = svc.editor_diagnostics("CREATE USER 'user'@'host' IDENTIFIED BY 'secret'");
        assert!(diags.is_empty());
    }

    #[test]
    fn editor_diagnostics_empty_for_grant_with_user_host() {
        let svc = MySqlLanguageService;
        let diags = svc.editor_diagnostics("GRANT ALL PRIVILEGES ON db.* TO 'u'@'h'");
        assert!(diags.is_empty());
    }

    #[test]
    fn editor_diagnostics_empty_for_flush_privileges() {
        let svc = MySqlLanguageService;
        let diags = svc.editor_diagnostics("FLUSH PRIVILEGES");
        assert!(diags.is_empty());
    }

    #[test]
    fn detect_dangerous_drop_still_fires() {
        let svc = MySqlLanguageService;
        assert_eq!(
            svc.detect_dangerous("DROP TABLE users"),
            Some(DangerousQueryKind::Drop)
        );
    }
}
