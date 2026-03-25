use dbflux_core::{
    detect_dangerous_mongo, DangerousQueryKind, LanguageService, QueryLanguage, ValidationResult,
};

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

        ValidationResult::Valid
    }

    fn detect_dangerous(&self, query: &str) -> Option<DangerousQueryKind> {
        detect_dangerous_mongo(query)
    }
}
