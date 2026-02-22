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
