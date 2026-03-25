use dbflux_core::{detect_dangerous_redis, DangerousQueryKind, LanguageService, ValidationResult};

/// Redis language service with lightweight syntax/language checks.
pub struct RedisLanguageService;

impl LanguageService for RedisLanguageService {
    fn validate(&self, _query: &str) -> ValidationResult {
        ValidationResult::Valid
    }

    fn detect_dangerous(&self, query: &str) -> Option<DangerousQueryKind> {
        detect_dangerous_redis(query)
    }
}
