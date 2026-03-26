use dbflux_core::{ColumnMeta, DefaultSqlDialect, DriverKey, KeySetRequest};
use dbflux_core::{
    Connection, ConnectionProfile, DbConfig, DbDriver, DbKind, DocumentFilter, DocumentUpdate,
    GeneratedQuery, MutationCategory, MutationRequest, QueryGenerator, QueryLanguage, QueryResult,
    RowInsert, SemanticFilter, SemanticRequest, SqlMutationGenerator, SqlUpdateRequest,
    TableBrowseRequest, TableCountRequest, TableRef, Value, WhereOperator,
};
use dbflux_test_support::FakeDriver;
use std::sync::Mutex;
use std::time::Duration;

static DIALECT: DefaultSqlDialect = DefaultSqlDialect;

#[test]
fn mutation_request_category_is_consistent() {
    let sql = MutationRequest::sql_insert(RowInsert::new(
        "users".to_string(),
        Some("public".to_string()),
        vec!["id".to_string()],
        vec![Value::Int(1)],
    ));

    let document = MutationRequest::document_update(DocumentUpdate::new(
        "users".to_string(),
        DocumentFilter::new(serde_json::json!({"_id": "abc"})),
        serde_json::json!({"$set": {"name": "alice"}}),
    ));

    let filtered_update = MutationRequest::sql_update_many(SqlUpdateRequest::new(
        "users".to_string(),
        Some("public".to_string()),
        SemanticFilter::compare("status", WhereOperator::Eq, Value::Text("active".into())),
        vec![("archived".to_string(), Value::Bool(true))],
    ));

    let key_value = MutationRequest::KeyValueSet(KeySetRequest::new("k", b"v".to_vec()));

    assert_eq!(sql.category(), MutationCategory::Sql);
    assert_eq!(filtered_update.category(), MutationCategory::Sql);
    assert_eq!(document.category(), MutationCategory::Document);
    assert_eq!(key_value.category(), MutationCategory::KeyValue);
}

#[test]
fn sql_generator_ignores_non_sql_mutations() {
    let generator = SqlMutationGenerator::new(&DIALECT);

    let document = MutationRequest::document_update(DocumentUpdate::new(
        "users".to_string(),
        DocumentFilter::new(serde_json::json!({"_id": "abc"})),
        serde_json::json!({"$set": {"name": "alice"}}),
    ));

    let key_value = MutationRequest::KeyValueSet(KeySetRequest::new("k", b"v".to_vec()));

    assert!(generator.generate_mutation(&document).is_none());
    assert!(generator.generate_mutation(&key_value).is_none());

    let sql = MutationRequest::sql_insert(RowInsert::new(
        "users".to_string(),
        Some("public".to_string()),
        vec!["id".to_string()],
        vec![Value::Int(1)],
    ));

    let generated: GeneratedQuery = generator
        .generate_mutation(&sql)
        .expect("sql mutation should generate query text");

    assert_eq!(generated.language, QueryLanguage::Sql);
    assert!(generated.text.contains("INSERT"));
}

#[test]
fn count_table_parses_integer_count_from_first_cell() {
    let sql = "SELECT COUNT(*) FROM \"public\".\"users\"";
    let result = QueryResult::table(
        vec![ColumnMeta {
            name: "count".to_string(),
            type_name: "int8".to_string(),
            nullable: false,
            is_primary_key: false,
        }],
        vec![vec![Value::Int(7)]],
        None,
        Duration::ZERO,
    );

    let driver = FakeDriver::new(DbKind::Postgres).with_query_result(sql, result);
    let profile = ConnectionProfile::new("fake", DbConfig::default_postgres());
    let connection = driver
        .connect(&profile)
        .expect("fake driver should connect");

    let count = connection
        .count_table(&TableCountRequest::new(TableRef::with_schema(
            "public", "users",
        )))
        .expect("count_table should succeed");

    assert_eq!(count, 7);
    assert_eq!(driver.stats().executed_requests.len(), 1);
}

#[test]
fn count_table_falls_back_to_zero_for_non_integer_cell() {
    let sql = "SELECT COUNT(*) FROM \"public\".\"users\"";
    let result = QueryResult::table(
        vec![ColumnMeta {
            name: "count".to_string(),
            type_name: "text".to_string(),
            nullable: false,
            is_primary_key: false,
        }],
        vec![vec![Value::Text("not-a-number".to_string())]],
        None,
        Duration::ZERO,
    );

    let driver = FakeDriver::new(DbKind::Postgres).with_query_result(sql, result);
    let profile = ConnectionProfile::new("fake", DbConfig::default_postgres());
    let connection = driver
        .connect(&profile)
        .expect("fake driver should connect");

    let count = connection
        .count_table(&TableCountRequest::new(TableRef::with_schema(
            "public", "users",
        )))
        .expect("count_table should succeed");

    assert_eq!(count, 0);
}

// =========================================================================
// driver_key() and settings_schema()
// =========================================================================

#[test]
fn driver_key_has_builtin_prefix_for_fake_drivers() {
    let kinds = [
        DbKind::Postgres,
        DbKind::SQLite,
        DbKind::MySQL,
        DbKind::MariaDB,
        DbKind::MongoDB,
        DbKind::Redis,
        DbKind::DynamoDB,
    ];

    for kind in &kinds {
        let driver = FakeDriver::new(*kind);
        let key: DriverKey = driver.driver_key();

        assert!(
            key.starts_with("builtin:"),
            "driver_key for {:?} should start with 'builtin:', got '{}'",
            kind,
            key
        );
    }
}

#[test]
fn driver_key_is_unique_per_kind() {
    let kinds = [
        DbKind::Postgres,
        DbKind::SQLite,
        DbKind::MySQL,
        DbKind::MariaDB,
        DbKind::MongoDB,
        DbKind::Redis,
        DbKind::DynamoDB,
    ];

    let keys: Vec<DriverKey> = kinds
        .iter()
        .map(|k| FakeDriver::new(*k).driver_key())
        .collect();

    for (i, key) in keys.iter().enumerate() {
        for (j, other) in keys.iter().enumerate() {
            if i != j {
                assert_ne!(
                    key, other,
                    "driver_key collision between {:?} and {:?}: '{}'",
                    kinds[i], kinds[j], key
                );
            }
        }
    }
}

#[test]
fn settings_schema_defaults_to_none() {
    let driver = FakeDriver::new(DbKind::Postgres);
    assert!(
        driver.settings_schema().is_none(),
        "default settings_schema() should return None"
    );
}

#[test]
fn table_browse_request_can_store_semantic_filter_without_losing_legacy_fields() {
    let request = TableBrowseRequest::new(TableRef::with_schema("public", "users"))
        .with_filter("status = 'active'")
        .with_semantic_filter(SemanticFilter::compare(
            "status",
            WhereOperator::Eq,
            Value::Text("active".into()),
        ));

    assert_eq!(request.filter.as_deref(), Some("status = 'active'"));
    assert!(request.semantic_filter.is_some());
}

#[test]
fn semantic_planning_defaults_to_not_supported_when_connection_has_no_planner() {
    let driver = FakeDriver::new(DbKind::Postgres);
    let profile = ConnectionProfile::new("fake", DbConfig::default_postgres());
    let connection = driver
        .connect(&profile)
        .expect("fake driver should connect");

    let error = connection
        .plan_semantic_request(&SemanticRequest::TableCount(TableCountRequest::new(
            TableRef::new("users"),
        )))
        .expect_err("semantic planning should use the default not-supported path");

    assert!(
        error
            .to_string()
            .contains("Semantic planning not supported")
    );
}

struct PlannedExecutionConnection {
    executed_queries: Mutex<Vec<String>>,
}

impl PlannedExecutionConnection {
    fn new() -> Self {
        Self {
            executed_queries: Mutex::new(Vec::new()),
        }
    }
}

impl dbflux_core::Connection for PlannedExecutionConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        static METADATA: std::sync::LazyLock<dbflux_core::DriverMetadata> =
            std::sync::LazyLock::new(|| dbflux_core::DriverMetadata {
                id: "planned-test".into(),
                display_name: "Planned Test".into(),
                description: "planned test".into(),
                category: dbflux_core::DatabaseCategory::Relational,
                query_language: QueryLanguage::Sql,
                capabilities: dbflux_core::DriverCapabilities::empty(),
                default_port: None,
                uri_scheme: "planned-test".into(),
                icon: dbflux_core::Icon::Database,
                syntax: None,
                query: None,
                mutation: None,
                ddl: None,
                transactions: None,
                limits: None,
                classification_override: None,
            });

        &METADATA
    }

    fn ping(&self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn execute(
        &self,
        req: &dbflux_core::QueryRequest,
    ) -> Result<QueryResult, dbflux_core::DbError> {
        self.executed_queries
            .lock()
            .expect("executed queries mutex poisoned")
            .push(req.sql.clone());

        Ok(QueryResult::table(
            Vec::new(),
            Vec::new(),
            Some(4),
            Duration::ZERO,
        ))
    }

    fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }

    fn kind(&self) -> DbKind {
        DbKind::Postgres
    }

    fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
        dbflux_core::SchemaLoadingStrategy::ConnectionPerDatabase
    }

    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        &DefaultSqlDialect
    }

    fn plan_semantic_request(
        &self,
        _request: &SemanticRequest,
    ) -> Result<dbflux_core::SemanticPlan, dbflux_core::DbError> {
        Ok(dbflux_core::SemanticPlan::single_query(
            dbflux_core::SemanticPlanKind::MutationPreview,
            dbflux_core::PlannedQuery::new(QueryLanguage::Sql, "UPDATE users SET active = TRUE"),
        ))
    }
}

#[test]
fn execute_semantic_request_runs_the_primary_planned_query() {
    let connection = PlannedExecutionConnection::new();
    let request =
        SemanticRequest::Mutation(MutationRequest::sql_update_many(SqlUpdateRequest::new(
            "users".to_string(),
            None,
            SemanticFilter::compare("status", WhereOperator::Eq, Value::Text("active".into())),
            vec![("archived".to_string(), Value::Bool(true))],
        )));

    let result = connection
        .execute_semantic_request(&request)
        .expect("planned semantic execution should succeed");

    assert_eq!(result.affected_rows, Some(4));
    assert_eq!(
        connection
            .executed_queries
            .lock()
            .expect("executed queries mutex poisoned")
            .as_slice(),
        ["UPDATE users SET active = TRUE"]
    );
}
