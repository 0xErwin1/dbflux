use dbflux_core::{
    DangerousQueryKind, DiagnosticSeverity, EditorDiagnostic, LanguageService, QueryLanguage,
    ValidationResult,
};

/// Redis language service with lightweight syntax/language checks.
pub struct RedisLanguageService;

impl LanguageService for RedisLanguageService {
    fn validate(&self, query: &str) -> ValidationResult {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return ValidationResult::Valid;
        }

        let lower = trimmed.to_ascii_lowercase();
        if lower.starts_with("select ")
            || lower.starts_with("insert ")
            || lower.starts_with("update ")
            || lower.starts_with("delete ")
        {
            return ValidationResult::WrongLanguage {
                expected: QueryLanguage::RedisCommands,
                message:
                    "SQL syntax not supported for Redis. Use Redis command syntax (e.g. GET key)."
                        .to_string(),
            };
        }

        match crate::driver::parse_command(query) {
            Ok(_) => ValidationResult::Valid,
            Err(error) => ValidationResult::SyntaxError(
                dbflux_core::Diagnostic::error(format!("Invalid Redis command: {}", error))
                    .with_hint("Use Redis command syntax, for example: SET mykey myvalue"),
            ),
        }
    }

    fn detect_dangerous(&self, query: &str) -> Option<DangerousQueryKind> {
        detect_dangerous_redis(query)
    }

    fn editor_diagnostics(&self, query: &str) -> Vec<EditorDiagnostic> {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return vec![];
        }

        let lower = trimmed.to_ascii_lowercase();
        if lower.starts_with("select ")
            || lower.starts_with("insert ")
            || lower.starts_with("update ")
            || lower.starts_with("delete ")
        {
            return vec![EditorDiagnostic {
                severity: DiagnosticSeverity::Error,
                message:
                    "SQL syntax not supported for Redis. Use Redis command syntax (e.g. GET key)."
                        .to_string(),
                range: crate::driver::redis_first_line_range(query),
            }];
        }

        match crate::driver::parse_command(query) {
            Ok(tokens) => crate::driver::check_redis_arity(&tokens, query),
            Err(error) => vec![EditorDiagnostic {
                severity: DiagnosticSeverity::Error,
                message: format!("Invalid Redis command: {}", error),
                range: crate::driver::redis_first_line_range(query),
            }],
        }
    }
}

/// Detect dangerous Redis commands using keyword matching.
pub(crate) fn detect_dangerous_redis(query: &str) -> Option<DangerousQueryKind> {
    let normalized = query.trim().to_lowercase();

    let first_word = normalized.split_whitespace().next().unwrap_or("");

    if first_word == "flushall" {
        return Some(DangerousQueryKind::RedisFlushAll);
    }

    if first_word == "flushdb" {
        return Some(DangerousQueryKind::RedisFlushDb);
    }

    if first_word == "del" {
        let args: Vec<&str> = normalized.split_whitespace().skip(1).collect();
        if args.len() > 1 {
            return Some(DangerousQueryKind::RedisMultiDelete);
        }
    }

    if first_word == "keys" {
        return Some(DangerousQueryKind::RedisKeysPattern);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_flushall_is_dangerous() {
        assert_eq!(
            RedisLanguageService.detect_dangerous("FLUSHALL"),
            Some(DangerousQueryKind::RedisFlushAll)
        );
        assert_eq!(
            RedisLanguageService.detect_dangerous("flushall"),
            Some(DangerousQueryKind::RedisFlushAll)
        );
        assert_eq!(
            RedisLanguageService.detect_dangerous("FLUSHALL ASYNC"),
            Some(DangerousQueryKind::RedisFlushAll)
        );
    }

    #[test]
    fn redis_flushdb_is_dangerous() {
        assert_eq!(
            RedisLanguageService.detect_dangerous("FLUSHDB"),
            Some(DangerousQueryKind::RedisFlushDb)
        );
        assert_eq!(
            RedisLanguageService.detect_dangerous("flushdb ASYNC"),
            Some(DangerousQueryKind::RedisFlushDb)
        );
    }

    #[test]
    fn redis_del_multi_key_is_dangerous() {
        assert_eq!(
            RedisLanguageService.detect_dangerous("DEL key1 key2"),
            Some(DangerousQueryKind::RedisMultiDelete)
        );
        assert_eq!(
            RedisLanguageService.detect_dangerous("del a b c"),
            Some(DangerousQueryKind::RedisMultiDelete)
        );
    }

    #[test]
    fn redis_del_single_key_is_safe() {
        assert_eq!(RedisLanguageService.detect_dangerous("DEL mykey"), None);
    }

    #[test]
    fn redis_keys_is_dangerous() {
        assert_eq!(
            RedisLanguageService.detect_dangerous("KEYS *"),
            Some(DangerousQueryKind::RedisKeysPattern)
        );
        assert_eq!(
            RedisLanguageService.detect_dangerous("keys user:*"),
            Some(DangerousQueryKind::RedisKeysPattern)
        );
    }

    #[test]
    fn redis_safe_reads_and_writes_are_safe() {
        assert_eq!(RedisLanguageService.detect_dangerous("GET mykey"), None);
        assert_eq!(
            RedisLanguageService.detect_dangerous("SET mykey myvalue"),
            None
        );
    }

    #[test]
    fn redis_editor_diagnostics_flag_wrong_language() {
        let diagnostics = RedisLanguageService.editor_diagnostics("SELECT * FROM users");
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, DiagnosticSeverity::Error);
    }
}
