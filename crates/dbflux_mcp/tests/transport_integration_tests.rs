use dbflux_core::{
    DatabaseCategory, DdlCapabilities, QueryCapabilities, QueryLanguage, SyntaxInfo,
};
use dbflux_mcp::handlers::discovery_schema::{
    ConnectionInfo, ConnectionMetadata, DiscoverySchemaCatalog,
};
use dbflux_mcp::handlers::query::{
    QueryExecutionRequest, QueryExecutionResponse, handle_query_tool,
};
use dbflux_mcp::server::bootstrap::{
    BootstrapConfig, TransportKind, validate_v1_transport_profile,
};
use dbflux_mcp::server::router::{RouteTarget, route_tool};
use dbflux_policy::{
    ConnectionPolicyAssignment, ExecutionClassification, PolicyBindingScope, PolicyEngine,
    ToolPolicy,
};
use serde_json::Value;

struct IntegrationHarness {
    transport: TransportKind,
    catalog: DiscoverySchemaCatalog,
    read_engine: PolicyEngine,
}

enum ToolCallResponse {
    Databases(Vec<String>),
    Query(QueryExecutionResponse),
}

impl IntegrationHarness {
    fn new(transport: TransportKind) -> Self {
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

        let read_engine = PolicyEngine::new(
            vec![ConnectionPolicyAssignment {
                actor_id: "agent-a".to_string(),
                scope: PolicyBindingScope {
                    connection_id: "conn-a".to_string(),
                },
                role_ids: Vec::new(),
                policy_ids: vec!["policy-read".to_string()],
            }],
            Vec::new(),
            vec![ToolPolicy {
                id: "policy-read".to_string(),
                allowed_tools: vec!["read_query".to_string()],
                allowed_classes: vec![ExecutionClassification::Read],
            }],
        );

        Self {
            transport,
            catalog,
            read_engine,
        }
    }

    fn handshake(&self) -> Vec<&'static str> {
        validate_v1_transport_profile(&BootstrapConfig {
            enabled_transports: vec![self.transport],
        })
        .expect("transport profile should satisfy v1");

        vec!["list_databases", "read_query"]
    }

    fn call_tool(
        &self,
        tool_id: &str,
        payload: &Value,
    ) -> Result<ToolCallResponse, Box<dyn std::error::Error>> {
        let route = route_tool(tool_id)?;

        match route {
            RouteTarget::Schema if tool_id == "list_databases" => {
                let connection_id = payload
                    .get("connection_id")
                    .and_then(Value::as_str)
                    .ok_or("missing connection_id")?;

                let databases = self.catalog.list_databases(connection_id)?;
                Ok(ToolCallResponse::Databases(databases))
            }
            RouteTarget::Query if tool_id == "read_query" => {
                let request = QueryExecutionRequest {
                    actor_id: "agent-a".to_string(),
                    connection_id: payload
                        .get("connection_id")
                        .and_then(Value::as_str)
                        .ok_or("missing connection_id")?
                        .to_string(),
                    tool_id: "read_query".to_string(),
                    query_language: QueryLanguage::Sql,
                    query: payload
                        .get("query")
                        .and_then(Value::as_str)
                        .ok_or("missing query")?
                        .to_string(),
                };

                let response = handle_query_tool(&request, &self.read_engine)?;
                Ok(ToolCallResponse::Query(response))
            }
            _ => Err("unsupported tool in integration harness".into()),
        }
    }
}

#[test]
fn stdio_transport_supports_handshake_and_schema_tool_call() {
    let harness = IntegrationHarness::new(TransportKind::Stdio);

    let tools = harness.handshake();
    assert!(tools.contains(&"list_databases"));

    let response = harness
        .call_tool(
            "list_databases",
            &serde_json::json!({"connection_id": "conn-a"}),
        )
        .expect("schema tool call should succeed");

    let ToolCallResponse::Databases(databases) = response else {
        panic!("expected database list response");
    };

    assert_eq!(databases, vec!["db_main".to_string()]);
}

#[test]
fn unix_socket_transport_supports_handshake_and_query_tool_call() {
    let harness = IntegrationHarness::new(TransportKind::UnixSocket);

    let tools = harness.handshake();
    assert!(tools.contains(&"read_query"));

    let response = harness
        .call_tool(
            "read_query",
            &serde_json::json!({"connection_id": "conn-a", "query": "SELECT id FROM users"}),
        )
        .expect("query tool call should succeed");

    let ToolCallResponse::Query(query_response) = response else {
        panic!("expected query response");
    };

    assert_eq!(query_response.classification, ExecutionClassification::Read);
    assert!(query_response.execute);
    assert!(!query_response.preview_only);
}
