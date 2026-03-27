use crate::data::crud::MutationRequest;
use crate::driver::capabilities::QueryLanguage;
use crate::query::semantic::{PlannedQuery, SemanticPlan, SemanticPlanKind};
use crate::schema::types::ColumnInfo;
use crate::sql::dialect::SqlDialect;
use crate::sql::generation::{
    SqlGenerationOptions, SqlGenerationRequest, SqlOperation, SqlValueMode,
};
use crate::sql::query_builder::SqlQueryBuilder;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationTemplateOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct MutationTemplateRequest<'a> {
    pub operation: MutationTemplateOperation,
    pub schema: Option<&'a str>,
    pub table: &'a str,
    pub columns: &'a [ColumnInfo],
    pub options: SqlGenerationOptions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadTemplateOperation {
    SelectAll,
    SelectWhere,
}

#[derive(Debug, Clone)]
pub struct ReadTemplateRequest<'a> {
    pub operation: ReadTemplateOperation,
    pub schema: Option<&'a str>,
    pub table: &'a str,
    pub columns: &'a [ColumnInfo],
    pub options: SqlGenerationOptions,
}

impl From<GeneratedQuery> for PlannedQuery {
    fn from(value: GeneratedQuery) -> Self {
        Self::new(value.language, value.text)
    }
}

/// Produces native query/command text from a `MutationRequest`.
///
/// Accessed via `Connection::query_generator()`.
pub trait QueryGenerator: Send + Sync {
    fn supported_categories(&self) -> &'static [MutationCategory];

    fn generate_mutation(&self, mutation: &MutationRequest) -> Option<GeneratedQuery>;

    fn generate_template(&self, _request: &MutationTemplateRequest<'_>) -> Option<GeneratedQuery> {
        None
    }

    fn generate_read_template(&self, _request: &ReadTemplateRequest<'_>) -> Option<GeneratedQuery> {
        None
    }

    fn plan_mutation(&self, mutation: &MutationRequest) -> Option<SemanticPlan> {
        self.generate_mutation(mutation).map(|query| {
            SemanticPlan::single_query(SemanticPlanKind::MutationPreview, query.into())
        })
    }
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
            MutationRequest::SqlUpdateMany(update) => builder.build_update_many(update)?,
            MutationRequest::SqlInsert(insert) => builder.build_insert(insert, false)?,
            MutationRequest::SqlUpsert(upsert) => builder.build_upsert(upsert)?,
            MutationRequest::SqlDelete(delete) => builder.build_delete(delete, false)?,
            MutationRequest::SqlDeleteMany(delete) => builder.build_delete_many(delete)?,
            _ => return None,
        };

        Some(GeneratedQuery {
            language: QueryLanguage::Sql,
            text,
        })
    }

    fn generate_template(&self, request: &MutationTemplateRequest<'_>) -> Option<GeneratedQuery> {
        let operation = match request.operation {
            MutationTemplateOperation::Insert => SqlOperation::Insert,
            MutationTemplateOperation::Update => SqlOperation::Update,
            MutationTemplateOperation::Delete => SqlOperation::Delete,
        };

        let pk_indices: Vec<usize> = request
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| column.is_primary_key.then_some(index))
            .collect();

        let sql = crate::generate_sql(
            self.dialect,
            &SqlGenerationRequest {
                operation,
                schema: request.schema,
                table: request.table,
                columns: request.columns,
                values: SqlValueMode::WithPlaceholders,
                pk_indices: &pk_indices,
                options: request.options.clone(),
            },
        );

        Some(GeneratedQuery {
            language: QueryLanguage::Sql,
            text: sql,
        })
    }

    fn generate_read_template(&self, request: &ReadTemplateRequest<'_>) -> Option<GeneratedQuery> {
        let operation = match request.operation {
            ReadTemplateOperation::SelectAll => None,
            ReadTemplateOperation::SelectWhere => Some(SqlOperation::SelectWhere),
        };

        let sql = if let Some(operation) = operation {
            crate::generate_sql(
                self.dialect,
                &SqlGenerationRequest {
                    operation,
                    schema: request.schema,
                    table: request.table,
                    columns: request.columns,
                    values: SqlValueMode::WithPlaceholders,
                    pk_indices: &[],
                    options: request.options.clone(),
                },
            )
        } else {
            let table_ref = if request.options.fully_qualified {
                self.dialect.qualified_table(request.schema, request.table)
            } else {
                self.dialect.quote_identifier(request.table)
            };

            if request.options.compact {
                format!("SELECT * FROM {};", table_ref)
            } else {
                format!("SELECT *\nFROM {};", table_ref)
            }
        };

        Some(GeneratedQuery {
            language: QueryLanguage::Sql,
            text: sql,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MutationCategory, MutationTemplateOperation, MutationTemplateRequest, QueryGenerator,
        ReadTemplateOperation, ReadTemplateRequest, SqlMutationGenerator,
    };
    use crate::{
        ColumnInfo, DefaultSqlDialect, DocumentFilter, DocumentUpdate, KeySetRequest,
        MutationRequest, QueryLanguage, RowDelete, RowIdentity, RowInsert, RowPatch,
        SemanticFilter, SemanticPlanKind, SqlDeleteRequest, SqlGenerationOptions, SqlUpdateRequest,
        SqlUpsertRequest, Value, WhereOperator,
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
    fn sql_generator_handles_insert_update_delete_upsert_and_rejects_non_sql() {
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

        let upsert = MutationRequest::sql_upsert(SqlUpsertRequest::new(
            "users".to_string(),
            Some("public".to_string()),
            vec!["id".to_string(), "name".to_string()],
            vec![Value::Int(1), Value::Text("alice".to_string())],
            vec!["id".to_string()],
            vec![("name".to_string(), Value::Text("bob".to_string()))],
        ));

        let filtered_update = MutationRequest::sql_update_many(SqlUpdateRequest::new(
            "users".to_string(),
            Some("public".to_string()),
            SemanticFilter::compare("status", WhereOperator::Eq, Value::Text("active".into())),
            vec![("archived".to_string(), Value::Bool(true))],
        ));

        let filtered_delete = MutationRequest::sql_delete_many(
            SqlDeleteRequest::new(
                "users".to_string(),
                Some("public".to_string()),
                SemanticFilter::compare(
                    "status",
                    WhereOperator::Eq,
                    Value::Text("inactive".into()),
                ),
            )
            .with_returning(vec!["id".to_string()]),
        );

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

        let upsert_query = generator.generate_mutation(&upsert);
        assert!(upsert_query.is_some());
        assert!(upsert_query
            .as_ref()
            .is_some_and(|query| query.text.contains("INSERT INTO")));

        let filtered_update_query = generator.generate_mutation(&filtered_update);
        assert!(filtered_update_query.is_some());
        assert!(filtered_update_query
            .as_ref()
            .is_some_and(|query| query.text.contains("WHERE")));

        let filtered_delete_query = generator.generate_mutation(&filtered_delete);
        assert!(filtered_delete_query.is_some());
        assert!(filtered_delete_query
            .as_ref()
            .is_some_and(|query| query.text.contains("DELETE FROM")));

        let doc_query = generator.generate_mutation(&doc);
        assert!(doc_query.is_none());
    }

    #[test]
    fn query_generator_plan_mutation_wraps_generated_query() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let insert = MutationRequest::sql_insert(RowInsert::new(
            "users".to_string(),
            Some("public".to_string()),
            vec!["id".to_string()],
            vec![Value::Int(1)],
        ));

        let plan = generator
            .plan_mutation(&insert)
            .expect("sql mutation should produce a plan");

        assert_eq!(plan.kind, SemanticPlanKind::MutationPreview);
        assert_eq!(plan.queries.len(), 1);
        assert_eq!(plan.queries[0].language, QueryLanguage::Sql);
        assert!(plan.queries[0].text.contains("INSERT"));
    }

    #[test]
    fn sql_generator_builds_placeholder_templates_for_table_preview() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                type_name: "integer".to_string(),
                nullable: false,
                is_primary_key: true,
                default_value: None,
                enum_values: None,
            },
            ColumnInfo {
                name: "name".to_string(),
                type_name: "text".to_string(),
                nullable: true,
                is_primary_key: false,
                default_value: None,
                enum_values: None,
            },
        ];

        let generated = generator
            .generate_template(&MutationTemplateRequest {
                operation: MutationTemplateOperation::Update,
                schema: Some("public"),
                table: "users",
                columns: &columns,
                options: SqlGenerationOptions {
                    fully_qualified: true,
                    compact: false,
                },
            })
            .expect("sql template should generate");

        assert_eq!(generated.language, QueryLanguage::Sql);
        assert!(generated.text.contains("UPDATE \"public\".\"users\""));
        assert!(generated.text.contains("WHERE \"id\" ="));
    }

    #[test]
    fn sql_generator_builds_select_where_read_templates() {
        let generator = SqlMutationGenerator::new(&DIALECT);
        let columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                type_name: "integer".to_string(),
                nullable: false,
                is_primary_key: true,
                default_value: None,
                enum_values: None,
            },
            ColumnInfo {
                name: "name".to_string(),
                type_name: "text".to_string(),
                nullable: true,
                is_primary_key: false,
                default_value: None,
                enum_values: None,
            },
        ];

        let generated = generator
            .generate_read_template(&ReadTemplateRequest {
                operation: ReadTemplateOperation::SelectWhere,
                schema: Some("public"),
                table: "users",
                columns: &columns,
                options: SqlGenerationOptions {
                    fully_qualified: true,
                    compact: false,
                },
            })
            .expect("read template should generate");

        assert_eq!(generated.language, QueryLanguage::Sql);
        assert!(generated.text.contains("SELECT *"));
        assert!(generated.text.contains("FROM \"public\".\"users\""));
        assert!(generated.text.contains("WHERE \"id\" ="));
        assert!(generated.text.contains("AND \"name\" ="));
    }

    #[test]
    fn sql_generator_builds_select_all_read_templates_without_columns() {
        let generator = SqlMutationGenerator::new(&DIALECT);

        let generated = generator
            .generate_read_template(&ReadTemplateRequest {
                operation: ReadTemplateOperation::SelectAll,
                schema: Some("public"),
                table: "active_users",
                columns: &[],
                options: SqlGenerationOptions {
                    fully_qualified: true,
                    compact: false,
                },
            })
            .expect("select all template should generate");

        assert_eq!(generated.language, QueryLanguage::Sql);
        assert!(generated.text.contains("SELECT *"));
        assert!(generated.text.contains("FROM \"public\".\"active_users\";"));
    }
}
