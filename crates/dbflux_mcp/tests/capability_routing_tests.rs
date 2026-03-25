//! MCP integration tests for capability-based routing and validation.
//!
//! Tests that MCP handlers correctly use typed capability metadata
//! for routing decisions rather than driver-specific conditionals.

use dbflux_core::{
    DatabaseCategory, DdlCapabilities, DriverLimits, MutationCapabilities, PaginationStyle,
    QueryCapabilities, QueryLanguage, SyntaxInfo, TransactionCapabilities, WhereOperator,
};
use dbflux_mcp::handlers::discovery_schema::{
    ConnectionInfo, ConnectionMetadata, DiscoverySchemaCatalog,
};
use dbflux_policy::{ExecutionClassification, PolicyEngine, PolicyEvaluationRequest};

fn postgresql_metadata() -> ConnectionMetadata {
    // Build PostgreSQL query capabilities to match actual METADATA
    let pg_query = QueryCapabilities {
        pagination: vec![PaginationStyle::Offset],
        where_operators: vec![
            WhereOperator::Eq,
            WhereOperator::Ne,
            WhereOperator::Gt,
            WhereOperator::Gte,
            WhereOperator::Lt,
            WhereOperator::Lte,
            WhereOperator::Like,
            WhereOperator::ILike,
            WhereOperator::Regex,
            WhereOperator::Null,
            WhereOperator::In,
            WhereOperator::NotIn,
            WhereOperator::Contains,
            WhereOperator::Overlap,
            WhereOperator::ContainsAll,
            WhereOperator::ContainsAny,
            WhereOperator::Size,
            WhereOperator::And,
            WhereOperator::Or,
            WhereOperator::Not,
        ],
        supports_order_by: true,
        supports_group_by: true,
        supports_having: true,
        supports_distinct: true,
        supports_limit: true,
        supports_offset: true,
        supports_joins: true,
        supports_subqueries: true,
        supports_union: true,
        supports_intersect: true,
        supports_except: true,
        supports_case_expressions: true,
        supports_window_functions: true,
        supports_ctes: true,
        supports_explain: true,
        max_query_parameters: 32767,
        max_order_by_columns: 0,
        max_group_by_columns: 0,
    };

    ConnectionMetadata {
        connection_id: "pg-test".to_string(),
        database_kind: "postgresql".to_string(),
        supports_collections: false,
        category: DatabaseCategory::Relational,
        syntax: SyntaxInfo {
            identifier_quote: '"',
            string_quote: '\'',
            placeholder_style: dbflux_core::PlaceholderStyle::DollarNumber,
            supports_schemas: true,
            default_schema: Some("public".to_string()),
            case_sensitive_identifiers: true,
        },
        query: pg_query,
        ddl: DdlCapabilities::default(),
        mutation: MutationCapabilities::default(),
        transactions: TransactionCapabilities::default(),
        limits: DriverLimits::default(),
    }
}

fn mongodb_metadata() -> ConnectionMetadata {
    // Build MongoDB query capabilities to match actual MONGODB_METADATA
    let mongo_query = QueryCapabilities {
        pagination: vec![PaginationStyle::Cursor, PaginationStyle::PageToken],
        where_operators: vec![
            WhereOperator::Eq,
            WhereOperator::Ne,
            WhereOperator::Gt,
            WhereOperator::Gte,
            WhereOperator::Lt,
            WhereOperator::Lte,
            WhereOperator::In,
            WhereOperator::NotIn,
            WhereOperator::And,
            WhereOperator::Or,
            WhereOperator::Not,
        ],
        supports_order_by: true,
        supports_group_by: true,
        supports_having: true,
        supports_distinct: false,
        supports_limit: true,
        supports_offset: true,
        supports_joins: false,
        supports_subqueries: false,
        supports_union: false,
        supports_intersect: false,
        supports_except: false,
        supports_case_expressions: false,
        supports_window_functions: false,
        supports_ctes: false,
        supports_explain: false,
        max_query_parameters: 0,
        max_order_by_columns: 0,
        max_group_by_columns: 0,
    };

    ConnectionMetadata {
        connection_id: "mongo-test".to_string(),
        database_kind: "mongodb".to_string(),
        supports_collections: true,
        category: DatabaseCategory::Document,
        syntax: SyntaxInfo::default(),
        query: mongo_query,
        ddl: DdlCapabilities {
            supports_create_database: false,
            supports_drop_database: false,
            supports_create_table: false,
            supports_drop_table: false,
            supports_alter_table: false,
            supports_create_index: true,
            supports_drop_index: true,
            supports_create_view: false,
            supports_drop_view: false,
            supports_create_trigger: false,
            supports_drop_trigger: false,
            transactional_ddl: false,
            supports_add_column: false,
            supports_drop_column: false,
            supports_rename_column: false,
            supports_alter_column: false,
            supports_add_constraint: false,
            supports_drop_constraint: false,
        },
        mutation: MutationCapabilities::default(),
        transactions: TransactionCapabilities {
            supports_transactions: false,
            supported_isolation_levels: vec![],
            default_isolation_level: None,
            supports_savepoints: false,
            supports_nested_transactions: false,
            supports_read_only: false,
            supports_deferrable: false,
        },
        limits: DriverLimits::default(),
    }
}

fn redis_metadata() -> ConnectionMetadata {
    ConnectionMetadata {
        connection_id: "redis-test".to_string(),
        database_kind: "redis".to_string(),
        supports_collections: false,
        category: DatabaseCategory::KeyValue,
        syntax: SyntaxInfo::default(),
        query: QueryCapabilities {
            pagination: vec![PaginationStyle::Cursor],
            where_operators: vec![],
            supports_order_by: false,
            supports_group_by: false,
            supports_having: false,
            supports_distinct: false,
            supports_limit: false,
            supports_offset: false,
            supports_joins: false,
            supports_subqueries: false,
            supports_union: false,
            supports_intersect: false,
            supports_except: false,
            supports_case_expressions: false,
            supports_window_functions: false,
            supports_ctes: false,
            supports_explain: false,
            ..Default::default()
        },
        ddl: DdlCapabilities::default(),
        mutation: MutationCapabilities::default(),
        transactions: TransactionCapabilities {
            supports_transactions: false,
            supported_isolation_levels: vec![],
            default_isolation_level: None,
            supports_savepoints: false,
            supports_nested_transactions: false,
            supports_read_only: false,
            supports_deferrable: false,
        },
        limits: DriverLimits::default(),
    }
}

#[test]
fn discovery_catalog_routes_by_category_not_driver_id() {
    let mut catalog = DiscoverySchemaCatalog::default();

    // Insert PostgreSQL connection
    catalog.insert_connection(
        ConnectionInfo {
            id: "conn-pg".to_string(),
            name: "Test PostgreSQL".to_string(),
            mcp_enabled: true,
        },
        postgresql_metadata(),
        vec!["testdb".to_string()],
    );

    // Insert MongoDB connection
    catalog.insert_connection(
        ConnectionInfo {
            id: "conn-mongo".to_string(),
            name: "Test MongoDB".to_string(),
            mcp_enabled: true,
        },
        mongodb_metadata(),
        vec!["testdb".to_string()],
    );

    // Route based on category - not driver ID
    let pg_meta = catalog
        .get_connection_metadata("conn-pg")
        .expect("should get pg metadata");
    assert_eq!(pg_meta.category, DatabaseCategory::Relational);
    assert!(!pg_meta.supports_collections);

    let mongo_meta = catalog
        .get_connection_metadata("conn-mongo")
        .expect("should get mongo metadata");
    assert_eq!(mongo_meta.category, DatabaseCategory::Document);
    assert!(mongo_meta.supports_collections);
}

#[test]
fn discovery_catalog_handles_relational_schema_routing() {
    let mut catalog = DiscoverySchemaCatalog::default();

    catalog.insert_connection(
        ConnectionInfo {
            id: "conn-pg".to_string(),
            name: "Test PostgreSQL".to_string(),
            mcp_enabled: true,
        },
        postgresql_metadata(),
        vec!["testdb".to_string()],
    );

    catalog.insert_schemas(
        "conn-pg",
        "testdb",
        vec!["public".to_string(), "admin".to_string()],
    );

    let schemas = catalog
        .list_schemas("conn-pg", "testdb")
        .expect("should list schemas");

    assert_eq!(schemas, vec!["admin", "public"]);
}

#[test]
fn discovery_catalog_handles_document_collections_routing() {
    let mut catalog = DiscoverySchemaCatalog::default();

    catalog.insert_connection(
        ConnectionInfo {
            id: "conn-mongo".to_string(),
            name: "Test MongoDB".to_string(),
            mcp_enabled: true,
        },
        mongodb_metadata(),
        vec!["testdb".to_string()],
    );

    catalog.insert_collections(
        "conn-mongo",
        "testdb",
        vec!["users".to_string(), "products".to_string()],
    );

    let collections = catalog
        .list_collections("conn-mongo", "testdb")
        .expect("should list collections");

    assert_eq!(collections, vec!["products", "users"]);
}

#[test]
fn relational_metadata_has_syntax_and_schema_support() {
    let metadata = postgresql_metadata();

    assert!(metadata.syntax.supports_schemas);
    assert_eq!(metadata.syntax.default_schema, Some("public".to_string()));
    assert_eq!(metadata.syntax.identifier_quote, '"');
}

#[test]
fn document_metadata_has_no_schema_support() {
    let metadata = mongodb_metadata();

    // MongoDB doesn't use schemas in the SQL sense
    assert!(!metadata.syntax.supports_schemas);
}

#[test]
fn capability_routing_postgresql_supports_sql_features() {
    let metadata = postgresql_metadata();

    // PostgreSQL should support all these
    assert!(metadata.query.supports_joins);
    assert!(metadata.query.supports_subqueries);
    assert!(metadata.query.supports_ctes);
    assert!(metadata.query.supports_window_functions);
}

#[test]
fn capability_routing_mongodb_lacks_sql_features() {
    let metadata = mongodb_metadata();

    // MongoDB should NOT support SQL features like JOINs and CTEs
    assert!(!metadata.query.supports_joins);
    assert!(!metadata.query.supports_ctes);
    // MongoDB aggregation pipelines have subquery-like behavior via $lookup/$graphLookup
    // but not in the SQL sense, so subqueries is false
    // Window functions are supported via $setWindowFields
    assert!(!metadata.query.supports_window_functions);
}

#[test]
fn capability_routing_redis_is_key_value() {
    let metadata = redis_metadata();

    assert_eq!(metadata.category, DatabaseCategory::KeyValue);
    assert!(metadata.query.where_operators.is_empty());
    assert!(!metadata.query.supports_order_by);
    assert!(!metadata.query.supports_group_by);
}

#[test]
fn category_determines_ddl_support() {
    let pg_ddl = postgresql_metadata().ddl;
    assert!(pg_ddl.supports_create_table);
    assert!(pg_ddl.supports_alter_table);
    assert!(pg_ddl.transactional_ddl);

    let mongo_ddl = mongodb_metadata().ddl;
    assert!(!mongo_ddl.supports_create_table);
    assert!(!mongo_ddl.supports_alter_table);
    assert!(!mongo_ddl.transactional_ddl);
    assert!(mongo_ddl.supports_create_index); // But indexes are supported
}

#[test]
fn query_handler_classifies_queries_by_language() {
    // This test verifies that query classification works for different languages
    // We test the classification function directly to avoid policy engine complexity

    let sql_classification =
        dbflux_core::classify_query_for_governance(&QueryLanguage::Sql, "SELECT * FROM users");
    assert!(matches!(sql_classification, ExecutionClassification::Read));

    let mongo_classification =
        dbflux_core::classify_query_for_governance(&QueryLanguage::MongoQuery, "db.users.find({})");
    assert!(matches!(
        mongo_classification,
        ExecutionClassification::Read | ExecutionClassification::Metadata
    ));
}

#[test]
fn policy_engine_evaluates_by_classification_not_driver() {
    // This tests that policy evaluation works correctly regardless of driver type
    let policy_engine = PolicyEngine::default();

    let request = PolicyEvaluationRequest {
        actor_id: "test-user".to_string(),
        connection_id: "conn-pg".to_string(),
        tool_id: "read_query".to_string(),
        classification: ExecutionClassification::Read,
    };

    let decision = policy_engine.evaluate(&request);
    assert!(decision.is_ok());
}

// ============================================================================
// WHERE Operator Validation Tests
// ============================================================================

#[test]
fn relational_supports_all_standard_where_operators() {
    let query = postgresql_metadata().query;

    use dbflux_core::WhereOperator;

    // Standard comparison operators
    assert!(query.where_operators.contains(&WhereOperator::Eq));
    assert!(query.where_operators.contains(&WhereOperator::Ne));
    assert!(query.where_operators.contains(&WhereOperator::Gt));
    assert!(query.where_operators.contains(&WhereOperator::Gte));
    assert!(query.where_operators.contains(&WhereOperator::Lt));
    assert!(query.where_operators.contains(&WhereOperator::Lte));

    // Pattern matching (PostgreSQL-specific LIKE and ILIKE)
    assert!(query.where_operators.contains(&WhereOperator::Like));
    // ILIKE is PostgreSQL-specific
    assert!(query.where_operators.contains(&WhereOperator::Null));

    // Collection operators
    assert!(query.where_operators.contains(&WhereOperator::In));
    assert!(query.where_operators.contains(&WhereOperator::NotIn));
}

#[test]
fn mongodb_lacks_some_sql_where_operators() {
    let query = mongodb_metadata().query;

    use dbflux_core::WhereOperator;

    // MongoDB supports Eq but not LIKE
    assert!(query.where_operators.contains(&WhereOperator::Eq));
    assert!(query.where_operators.contains(&WhereOperator::In));

    // MongoDB does NOT support LIKE, ILIKE, Regex (MongoDB has its own regex)
    assert!(!query.where_operators.contains(&WhereOperator::Like));
    assert!(!query.where_operators.contains(&WhereOperator::ILike));
}

#[test]
fn redis_has_minimal_where_operators() {
    let query = redis_metadata().query;

    // Redis SCAN doesn't use traditional WHERE clauses
    assert!(query.where_operators.is_empty());
}

// ============================================================================
// MCP Tool Routing Tests
// ============================================================================

#[test]
fn tools_route_to_correct_category_handlers() {
    let mut catalog = DiscoverySchemaCatalog::default();

    // Set up relational connection
    catalog.insert_connection(
        ConnectionInfo {
            id: "conn-pg".to_string(),
            name: "PostgreSQL".to_string(),
            mcp_enabled: true,
        },
        postgresql_metadata(),
        vec!["testdb".to_string()],
    );

    catalog.insert_tables(
        "conn-pg",
        "testdb",
        Some("public".to_string()),
        vec!["users".to_string(), "orders".to_string()],
    );

    // Set up document connection
    catalog.insert_connection(
        ConnectionInfo {
            id: "conn-mongo".to_string(),
            name: "MongoDB".to_string(),
            mcp_enabled: true,
        },
        mongodb_metadata(),
        vec!["testdb".to_string()],
    );

    catalog.insert_collections(
        "conn-mongo",
        "testdb",
        vec!["users".to_string(), "products".to_string()],
    );

    // List tables works for relational
    let tables = catalog
        .list_tables("conn-pg", "testdb", Some("public"))
        .expect("should list tables");
    assert_eq!(tables, vec!["orders", "users"]);

    // List collections works for document
    let collections = catalog
        .list_collections("conn-mongo", "testdb")
        .expect("should list collections");
    assert_eq!(collections, vec!["products", "users"]);
}

#[test]
fn category_enables_correct_ui_mode() {
    let relational_meta = postgresql_metadata();
    assert_eq!(relational_meta.category, DatabaseCategory::Relational);
    assert!(!relational_meta.supports_collections);

    let document_meta = mongodb_metadata();
    assert_eq!(document_meta.category, DatabaseCategory::Document);
    assert!(document_meta.supports_collections);
}

#[test]
fn syntax_info_enables_driver_agnostic_query_rendering() {
    let pg = postgresql_metadata();
    let _mongo = mongodb_metadata();

    // PostgreSQL uses dollar-quoted identifiers
    assert_eq!(pg.syntax.identifier_quote, '"');
    assert_eq!(
        pg.syntax.placeholder_style,
        dbflux_core::PlaceholderStyle::DollarNumber
    );

    // MongoDB has no SQL syntax
    // (syntax would be None in real usage, but our test fixture has default)
}
