use dbflux_core::{ColumnMeta, DefaultSqlDialect, KeySetRequest};
use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbKind, DocumentFilter, DocumentUpdate, GeneratedQuery,
    MutationCategory, MutationRequest, QueryGenerator, QueryLanguage, QueryResult, RowInsert,
    SqlMutationGenerator, TableCountRequest, TableRef, Value,
};
use dbflux_test_support::FakeDriver;
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

    let key_value = MutationRequest::KeyValueSet(KeySetRequest::new("k", b"v".to_vec()));

    assert_eq!(sql.category(), MutationCategory::Sql);
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
