use dbflux_core::{
    DatabaseCategory, DdlCapabilities, QueryCapabilities, QueryLanguage, SyntaxInfo,
};
use dbflux_mcp::handlers::discovery_schema::{
    ConnectionInfo, ConnectionMetadata, DescribeObjectRequest, DiscoverySchemaCatalog,
    ObjectDescription,
};
use dbflux_mcp::handlers::query::{QueryExecutionRequest, handle_query_tool};
use dbflux_policy::{
    ConnectionPolicyAssignment, ExecutionClassification, PolicyBindingScope, PolicyEngine,
    ToolPolicy,
};

fn allow_engine(tool_id: &str, class: ExecutionClassification) -> PolicyEngine {
    PolicyEngine::new(
        vec![ConnectionPolicyAssignment {
            actor_id: "agent-a".to_string(),
            scope: PolicyBindingScope {
                connection_id: "conn-a".to_string(),
            },
            role_ids: Vec::new(),
            policy_ids: vec!["policy-a".to_string()],
        }],
        Vec::new(),
        vec![ToolPolicy {
            id: "policy-a".to_string(),
            allowed_tools: vec![tool_id.to_string()],
            allowed_classes: vec![class],
        }],
    )
}

fn build_catalog() -> DiscoverySchemaCatalog {
    let mut catalog = DiscoverySchemaCatalog::default();

    catalog.insert_connection(
        ConnectionInfo {
            id: "conn-a".to_string(),
            name: "Primary".to_string(),
            mcp_enabled: true,
        },
        ConnectionMetadata {
            connection_id: "conn-a".to_string(),
            database_kind: "postgres".to_string(),
            supports_collections: false,
            category: DatabaseCategory::Relational,
            syntax: SyntaxInfo::ansi(),
            query: QueryCapabilities::relational(),
            ddl: DdlCapabilities::default(),
        },
        vec!["db_main".to_string()],
    );

    catalog.insert_schemas("conn-a", "db_main", vec!["public".to_string()]);
    catalog.insert_tables(
        "conn-a",
        "db_main",
        Some("public".to_string()),
        vec!["users".to_string(), "orders".to_string()],
    );

    catalog.insert_collections(
        "conn-a",
        "db_main",
        vec!["users_docs".to_string(), "audit_docs".to_string()],
    );

    catalog.insert_object_description(ObjectDescription {
        connection_id: "conn-a".to_string(),
        database: "db_main".to_string(),
        schema: Some("public".to_string()),
        object_name: "users".to_string(),
        object_kind: "table".to_string(),
        columns: vec!["id".to_string(), "email".to_string()],
    });

    catalog
}

#[test]
fn discovery_and_schema_handlers_return_runtime_data() {
    let catalog = build_catalog();

    let connections = catalog.list_connections();
    assert_eq!(connections.len(), 1);
    assert_eq!(connections[0].id, "conn-a");

    let metadata = catalog
        .get_connection_metadata("conn-a")
        .expect("metadata should exist");
    assert_eq!(metadata.database_kind, "postgres");

    let databases = catalog
        .list_databases("conn-a")
        .expect("databases should exist");
    assert_eq!(databases, vec!["db_main".to_string()]);

    let schemas = catalog
        .list_schemas("conn-a", "db_main")
        .expect("schemas should exist");
    assert_eq!(schemas, vec!["public".to_string()]);

    let tables = catalog
        .list_tables("conn-a", "db_main", Some("public"))
        .expect("tables should exist");
    assert_eq!(tables, vec!["orders".to_string(), "users".to_string()]);

    let collections = catalog
        .list_collections("conn-a", "db_main")
        .expect("collections should exist");
    assert_eq!(
        collections,
        vec!["audit_docs".to_string(), "users_docs".to_string()]
    );

    let object = catalog
        .describe_object(&DescribeObjectRequest {
            connection_id: "conn-a".to_string(),
            database: "db_main".to_string(),
            schema: Some("public".to_string()),
            object_name: "users".to_string(),
        })
        .expect("object should exist");

    assert_eq!(object.object_kind, "table");
    assert_eq!(object.columns, vec!["id".to_string(), "email".to_string()]);
}

#[test]
fn read_and_explain_query_paths_return_expected_semantics() {
    let read_engine = allow_engine("read_query", ExecutionClassification::Read);
    let read_response = handle_query_tool(
        &QueryExecutionRequest {
            actor_id: "agent-a".to_string(),
            connection_id: "conn-a".to_string(),
            tool_id: "read_query".to_string(),
            query_language: QueryLanguage::Sql,
            query: "SELECT id, email FROM users".to_string(),
        },
        &read_engine,
    )
    .expect("read query should pass policy and execute");

    assert_eq!(read_response.classification, ExecutionClassification::Read);
    assert!(read_response.execute);
    assert!(!read_response.preview_only);

    let explain_engine = allow_engine("explain_query", ExecutionClassification::Metadata);
    let explain_response = handle_query_tool(
        &QueryExecutionRequest {
            actor_id: "agent-a".to_string(),
            connection_id: "conn-a".to_string(),
            tool_id: "explain_query".to_string(),
            query_language: QueryLanguage::Sql,
            query: "EXPLAIN SELECT id FROM users".to_string(),
        },
        &explain_engine,
    )
    .expect("explain query should pass policy and execute");

    assert_eq!(
        explain_response.classification,
        ExecutionClassification::Metadata
    );
    assert!(explain_response.execute);
    assert!(!explain_response.preview_only);
}
