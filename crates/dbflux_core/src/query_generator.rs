use crate::crud::MutationRequest;
use crate::driver_capabilities::QueryLanguage;
use crate::sql_dialect::SqlDialect;
use crate::sql_query_builder::SqlQueryBuilder;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MutationCategory {
    Sql,
    Document,
    KeyValue,
}

impl MutationRequest {
    pub fn category(&self) -> MutationCategory {
        if self.is_sql() {
            MutationCategory::Sql
        } else if self.is_document() {
            MutationCategory::Document
        } else {
            MutationCategory::KeyValue
        }
    }
}

#[derive(Debug, Clone)]
pub struct GeneratedQuery {
    pub language: QueryLanguage,
    pub text: String,
}

/// Produces native query/command text from a `MutationRequest`.
///
/// Accessed via `Connection::query_generator()`.
pub trait QueryGenerator: Send + Sync {
    fn supported_categories(&self) -> &'static [MutationCategory];

    fn generate_mutation(&self, mutation: &MutationRequest) -> Option<GeneratedQuery>;
}

// =============================================================================
// SQL Mutation Generator
// =============================================================================

/// `QueryGenerator` for SQL drivers, backed by `SqlQueryBuilder`.
///
/// Each SQL driver creates a static instance with its dialect:
/// ```ignore
/// static GENERATOR: SqlMutationGenerator = SqlMutationGenerator::new(&POSTGRES_DIALECT);
/// ```
pub struct SqlMutationGenerator {
    dialect: &'static dyn SqlDialect,
}

impl SqlMutationGenerator {
    pub const fn new(dialect: &'static dyn SqlDialect) -> Self {
        Self { dialect }
    }
}

impl QueryGenerator for SqlMutationGenerator {
    fn supported_categories(&self) -> &'static [MutationCategory] {
        &[MutationCategory::Sql]
    }

    fn generate_mutation(&self, mutation: &MutationRequest) -> Option<GeneratedQuery> {
        let builder = SqlQueryBuilder::new(self.dialect);

        let text = match mutation {
            MutationRequest::SqlUpdate(patch) => builder.build_update(patch, false)?,
            MutationRequest::SqlInsert(insert) => builder.build_insert(insert, false)?,
            MutationRequest::SqlDelete(delete) => builder.build_delete(delete, false)?,
            _ => return None,
        };

        Some(GeneratedQuery {
            language: QueryLanguage::Sql,
            text,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{MutationCategory, QueryGenerator, SqlMutationGenerator};
    use crate::{
        DefaultSqlDialect, DocumentFilter, DocumentUpdate, KeySetRequest, MutationRequest,
        RowDelete, RowIdentity, RowInsert, RowPatch, Value,
    };

    static DIALECT: DefaultSqlDialect = DefaultSqlDialect;

    #[test]
    fn categories_are_classified_by_mutation_kind() {
        let sql = MutationRequest::sql_insert(RowInsert::new(
            "users".to_string(),
            Some("public".to_string()),
            vec!["id".to_string()],
            vec![Value::Int(1)],
        ));

        let document = MutationRequest::document_update(DocumentUpdate::new(
            "users".to_string(),
            DocumentFilter::new(serde_json::json!({"_id": "a"})),
            serde_json::json!({"$set": {"name": "alice"}}),
        ));

        let key_value = MutationRequest::KeyValueSet(KeySetRequest::new("k", b"v".to_vec()));

        assert_eq!(sql.category(), MutationCategory::Sql);
        assert_eq!(document.category(), MutationCategory::Document);
        assert_eq!(key_value.category(), MutationCategory::KeyValue);
    }

    #[test]
    fn sql_generator_supports_only_sql_category() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        assert_eq!(generator.supported_categories(), &[MutationCategory::Sql]);
    }

    #[test]
    fn sql_generator_handles_insert_update_delete_and_rejects_non_sql() {
        let generator = SqlMutationGenerator::new(&DIALECT);

        let insert = MutationRequest::sql_insert(RowInsert::new(
            "users".to_string(),
            Some("public".to_string()),
            vec!["id".to_string(), "name".to_string()],
            vec![Value::Int(1), Value::Text("alice".to_string())],
        ));

        let update = MutationRequest::sql_update(RowPatch::new(
            RowIdentity::composite(vec!["id".to_string()], vec![Value::Int(1)]),
            "users".to_string(),
            Some("public".to_string()),
            vec![("name".to_string(), Value::Text("bob".to_string()))],
        ));

        let delete = MutationRequest::sql_delete(RowDelete::new(
            RowIdentity::composite(vec!["id".to_string()], vec![Value::Int(1)]),
            "users".to_string(),
            Some("public".to_string()),
        ));

        let doc = MutationRequest::document_update(DocumentUpdate::new(
            "users".to_string(),
            DocumentFilter::new(serde_json::json!({"_id": "a"})),
            serde_json::json!({"$set": {"name": "alice"}}),
        ));

        let insert_query = generator.generate_mutation(&insert);
        assert!(insert_query.is_some());

        let update_query = generator.generate_mutation(&update);
        assert!(update_query.is_some());

        let delete_query = generator.generate_mutation(&delete);
        assert!(delete_query.is_some());

        let doc_query = generator.generate_mutation(&doc);
        assert!(doc_query.is_none());
    }
}
