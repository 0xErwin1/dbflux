//! Example custom driver for DBFlux
//!
//! This is a minimal implementation of the DBFlux driver protocol.
//! It simulates a simple key-value database for testing purposes.
//!
//! Usage:
//!   cargo run --bin custom-driver -- --socket my-driver.sock
//!
//! Then add the socket to ~/.config/dbflux/config.json so DBFlux can discover it.

use std::collections::HashMap;
use std::io::{self};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dbflux_core::{
    ColumnMeta, ConnectionProfile, DatabaseInfo, DbConfig, DbError, DbKind, DriverCapabilities,
    DriverFormDef, FormFieldDef, FormFieldKind, FormSection, FormTab, KeyValueSchema, QueryResult,
    QueryResultShape, SchemaLoadingStrategy, SchemaSnapshot, Value,
};
use dbflux_ipc::{
    driver_protocol::{
        DriverFormDefDto, DriverHelloResponse, DriverMetadataDto, DriverRequestBody,
        DriverRequestEnvelope, DriverResponseBody, DriverResponseEnvelope, DriverRpcErrorCode,
        QueryLanguageDto, QueryResultDto,
    },
    framing, DRIVER_RPC_VERSION,
};
use interprocess::local_socket::{
    prelude::*, GenericNamespaced, ListenerOptions, Stream as IpcStream,
};
use uuid::Uuid;

/// Mock database state - stores key-value pairs per session
struct MockDatabase {
    data: HashMap<String, String>,
}

impl MockDatabase {
    fn new() -> Self {
        let mut data = HashMap::new();
        // Pre-populate with some test data
        data.insert("key1".to_string(), "value1".to_string());
        data.insert("key2".to_string(), "value2".to_string());
        data.insert(
            "users:1".to_string(),
            r#"{"id": 1, "name": "Alice"}"#.to_string(),
        );
        data.insert(
            "users:2".to_string(),
            r#"{"id": 2, "name": "Bob"}"#.to_string(),
        );

        Self { data }
    }
}

/// Session manager - tracks active connections
struct SessionManager {
    sessions: HashMap<Uuid, MockDatabase>,
}

impl SessionManager {
    fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }

    fn create_session(&mut self) -> Uuid {
        let id = Uuid::new_v4();
        self.sessions.insert(id, MockDatabase::new());
        id
    }

    fn close_session(&mut self, id: &Uuid) -> bool {
        self.sessions.remove(id).is_some()
    }

    fn get_session(&mut self, id: &Uuid) -> Option<&mut MockDatabase> {
        self.sessions.get_mut(id)
    }
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = parse_args();

    // Create socket name using interprocess naming
    let socket_name = format!("{}", args.socket)
        .to_ns_name::<GenericNamespaced>()
        .unwrap_or_else(|e| {
            eprintln!("Invalid socket name: {}", e);
            std::process::exit(1);
        });

    // Bind to the socket
    let listener = ListenerOptions::new()
        .name(socket_name)
        .create_sync()
        .unwrap_or_else(|e| {
            eprintln!("Failed to bind socket: {}", e);
            std::process::exit(1);
        });

    log::info!("Custom driver listening on socket: {}", args.socket);
    log::info!("Press Ctrl+C to stop");

    let sessions = Arc::new(Mutex::new(SessionManager::new()));

    // Accept connections
    loop {
        match listener.accept() {
            Ok(stream) => {
                log::info!("Client connected");
                let sessions = Arc::clone(&sessions);

                // Handle each connection in a separate thread
                std::thread::spawn(move || {
                    if let Err(e) = handle_connection(stream, sessions) {
                        log::warn!("Connection error: {}", e);
                    }
                    log::info!("Client disconnected");
                });
            }
            Err(e) => {
                log::error!("Accept failed: {}", e);
                break;
            }
        }
    }
}

fn handle_connection(
    mut stream: IpcStream,
    sessions: Arc<Mutex<SessionManager>>,
) -> io::Result<()> {
    let mut hello_done = false;
    let mut current_session: Option<Uuid> = None;

    loop {
        // Read request
        let envelope: DriverRequestEnvelope = match framing::recv_msg(&mut stream) {
            Ok(env) => env,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                log::debug!("Client closed connection");
                break;
            }
            Err(e) => {
                log::warn!("Failed to read request: {}", e);
                break;
            }
        };

        let request_id = envelope.request_id;
        let session_id = envelope.session_id;

        // Dispatch request
        let response = match envelope.body {
            // Step 1: Hello handshake (required first)
            DriverRequestBody::Hello(hello_req) => {
                hello_done = true;

                // Check if we support any of the client's versions
                let compatible = hello_req
                    .supported_versions
                    .iter()
                    .any(|v| v.major == DRIVER_RPC_VERSION.major);

                if !compatible {
                    DriverResponseEnvelope::error(
                        request_id,
                        None,
                        DriverRpcErrorCode::VersionMismatch,
                        format!(
                            "No compatible protocol version. Server: {}.{}",
                            DRIVER_RPC_VERSION.major, DRIVER_RPC_VERSION.minor
                        ),
                        false,
                    )
                } else {
                    DriverResponseEnvelope::ok(
                        request_id,
                        None,
                        DriverResponseBody::Hello(DriverHelloResponse {
                            server_name: "custom-mock-driver".to_string(),
                            server_version: env!("CARGO_PKG_VERSION").to_string(),
                            selected_version: DRIVER_RPC_VERSION,
                            capabilities: hello_req.requested_capabilities,
                            driver_kind: DbKind::SQLite,
                            driver_metadata: create_metadata_dto(),
                            form_definition: DriverFormDefDto::from(&MOCK_FORM),
                        }),
                    )
                }
            }

            // Step 2: Open a session
            DriverRequestBody::OpenSession {
                profile_json,
                password,
                ssh_secret,
            } => {
                if !hello_done {
                    return_error(request_id, "Hello required before OpenSession")
                } else {
                    match serde_json::from_str::<ConnectionProfile>(&profile_json) {
                        Ok(profile) => {
                            if let DbConfig::External { values, .. } = &profile.config {
                                let endpoint = values.get("endpoint").cloned().unwrap_or_default();
                                if endpoint.trim().is_empty() {
                                    DriverResponseEnvelope::error(
                                        request_id,
                                        None,
                                        DriverRpcErrorCode::InvalidRequest,
                                        "endpoint is required".to_string(),
                                        false,
                                    )
                                } else {
                                    let api_key_present = values
                                        .get("api_key")
                                        .map(|value| !value.is_empty())
                                        .unwrap_or(false);

                                    log::info!(
                                        "Opening session to endpoint='{}' api_key_present={}",
                                        endpoint,
                                        api_key_present
                                    );

                                    if password.is_some() {
                                        log::info!(
                                            "Password provided (length: {})",
                                            password.as_ref().unwrap().len()
                                        );
                                    }
                                    if ssh_secret.is_some() {
                                        log::info!("SSH secret provided");
                                    }

                                    let session_id = sessions.lock().unwrap().create_session();
                                    current_session = Some(session_id);

                                    DriverResponseEnvelope::ok(
                                        request_id,
                                        Some(session_id),
                                        DriverResponseBody::SessionOpened {
                                            session_id,
                                            kind: DbKind::SQLite,
                                            metadata: create_metadata_dto(),
                                            schema_loading_strategy:
                                                SchemaLoadingStrategy::LazyPerDatabase,
                                            schema_features: dbflux_core::SchemaFeatures::empty(),
                                            code_gen_capabilities:
                                                dbflux_core::CodeGenCapabilities::empty(),
                                        },
                                    )
                                }
                            } else {
                                DriverResponseEnvelope::error(
                                    request_id,
                                    None,
                                    DriverRpcErrorCode::InvalidRequest,
                                    "Expected DbConfig::External for custom driver".to_string(),
                                    false,
                                )
                            }
                        }
                        Err(error) => DriverResponseEnvelope::error(
                            request_id,
                            None,
                            DriverRpcErrorCode::InvalidRequest,
                            format!("Invalid profile JSON: {error}"),
                            false,
                        ),
                    }
                }
            }

            // Close session
            DriverRequestBody::CloseSession => {
                if let Some(sid) = session_id {
                    sessions.lock().unwrap().close_session(&sid);
                    DriverResponseEnvelope::ok(
                        request_id,
                        Some(sid),
                        DriverResponseBody::SessionClosed,
                    )
                } else {
                    return_error(request_id, "No session_id provided")
                }
            }

            // Ping - health check
            DriverRequestBody::Ping => {
                DriverResponseEnvelope::ok(request_id, session_id, DriverResponseBody::Pong)
            }

            // Execute a query
            DriverRequestBody::Execute { request } => match session_id {
                Some(sid) => {
                    let result = execute_query(&request.sql, &mut sessions.lock().unwrap(), &sid);
                    match result {
                        Ok(query_result) => DriverResponseEnvelope::ok(
                            request_id,
                            Some(sid),
                            DriverResponseBody::ExecuteResult {
                                result: QueryResultDto::from(&query_result),
                            },
                        ),
                        Err(e) => DriverResponseEnvelope::error(
                            request_id,
                            Some(sid),
                            DriverRpcErrorCode::Driver,
                            e.to_string(),
                            false,
                        ),
                    }
                }
                None => return_error(request_id, "No session_id provided"),
            },

            // List databases
            DriverRequestBody::ListDatabases => DriverResponseEnvelope::ok(
                request_id,
                session_id,
                DriverResponseBody::Databases {
                    databases: vec![DatabaseInfo {
                        name: "mockdb".to_string(),
                        is_current: true,
                    }],
                },
            ),

            // Get schema
            DriverRequestBody::Schema => DriverResponseEnvelope::ok(
                request_id,
                session_id,
                DriverResponseBody::Schema {
                    schema: SchemaSnapshot::key_value(KeyValueSchema::default()),
                },
            ),

            // All other requests - not implemented in this example
            _ => DriverResponseEnvelope::error(
                request_id,
                session_id,
                DriverRpcErrorCode::UnsupportedMethod,
                "Method not implemented in this example driver".to_string(),
                false,
            ),
        };

        // Send response
        if let Err(e) = framing::send_msg(&mut stream, &response) {
            log::warn!("Failed to send response: {}", e);
            break;
        }
    }

    // Cleanup any session on disconnect
    if let Some(sid) = current_session {
        sessions.lock().unwrap().close_session(&sid);
    }

    Ok(())
}

/// Execute a mock query and return results
fn execute_query(
    sql: &str,
    sessions: &mut SessionManager,
    session_id: &Uuid,
) -> Result<QueryResult, DbError> {
    let session = sessions
        .get_session(session_id)
        .ok_or_else(|| DbError::ConnectionFailed("Session not found".into()))?;

    let sql_lower = sql.to_lowercase();

    // Handle different query types
    if sql_lower.starts_with("select") {
        // Return mock data for SELECT
        let keys: Vec<&String> = session.data.keys().collect();

        let columns = vec![
            ColumnMeta {
                name: "key".to_string(),
                type_name: "TEXT".to_string(),
                nullable: false,
            },
            ColumnMeta {
                name: "value".to_string(),
                type_name: "TEXT".to_string(),
                nullable: true,
            },
        ];

        let rows: Vec<Vec<Value>> = keys
            .iter()
            .map(|k| {
                vec![
                    Value::Text(k.to_string()),
                    Value::Text(session.data.get(*k).cloned().unwrap_or_default()),
                ]
            })
            .collect();

        Ok(QueryResult {
            shape: QueryResultShape::Table,
            columns,
            rows,
            affected_rows: None,
            execution_time: Duration::ZERO,
            text_body: None,
            raw_bytes: None,
        })
    } else if sql_lower.starts_with("insert") {
        // Mock INSERT
        log::info!("Executing INSERT: {}", sql);
        Ok(QueryResult {
            shape: QueryResultShape::Table,
            columns: vec![],
            rows: vec![],
            affected_rows: Some(1),
            execution_time: Duration::ZERO,
            text_body: None,
            raw_bytes: None,
        })
    } else if sql_lower.starts_with("update") {
        // Mock UPDATE
        log::info!("Executing UPDATE: {}", sql);
        Ok(QueryResult {
            shape: QueryResultShape::Table,
            columns: vec![],
            rows: vec![],
            affected_rows: Some(1),
            execution_time: Duration::ZERO,
            text_body: None,
            raw_bytes: None,
        })
    } else if sql_lower.starts_with("delete") {
        // Mock DELETE
        log::info!("Executing DELETE: {}", sql);
        Ok(QueryResult {
            shape: QueryResultShape::Table,
            columns: vec![],
            rows: vec![],
            affected_rows: Some(0),
            execution_time: Duration::ZERO,
            text_body: None,
            raw_bytes: None,
        })
    } else {
        Err(DbError::QueryFailed(
            format!("Unsupported query: {}", sql).into(),
        ))
    }
}

/// Create driver metadata for the mock database
fn create_metadata_dto() -> DriverMetadataDto {
    DriverMetadataDto {
        id: "mockdb".to_string(),
        display_name: "Mock Database".to_string(),
        description: "Example custom driver for testing DBFlux integration".to_string(),
        category: dbflux_core::DatabaseCategory::KeyValue,
        query_language: QueryLanguageDto::Sql,
        capabilities: DriverCapabilities::KEYVALUE_BASE.bits(),
        default_port: None,
        uri_scheme: "mock".to_string(),
        icon: dbflux_core::Icon::Database,
    }
}

const MOCK_FIELD_ENDPOINT: FormFieldDef = FormFieldDef {
    id: "endpoint",
    label: "Endpoint",
    kind: FormFieldKind::Text,
    placeholder: "localhost:8080",
    required: true,
    default_value: "localhost:8080",
    enabled_when_checked: None,
    enabled_when_unchecked: None,
};

const MOCK_FIELD_API_KEY: FormFieldDef = FormFieldDef {
    id: "api_key",
    label: "API Key",
    kind: FormFieldKind::Password,
    placeholder: "Optional API key",
    required: false,
    default_value: "",
    enabled_when_checked: None,
    enabled_when_unchecked: None,
};

static MOCK_FORM: DriverFormDef = DriverFormDef {
    tabs: &[FormTab {
        id: "main",
        label: "Main",
        sections: &[FormSection {
            title: "Connection",
            fields: &[MOCK_FIELD_ENDPOINT, MOCK_FIELD_API_KEY],
        }],
    }],
};

fn return_error(request_id: u64, message: &str) -> DriverResponseEnvelope {
    DriverResponseEnvelope::error(
        request_id,
        None,
        DriverRpcErrorCode::InvalidRequest,
        message.to_string(),
        false,
    )
}

struct Args {
    socket: String,
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let mut socket = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--socket" => socket = args.next(),
            "--help" | "-h" => {
                println!("Custom Driver Example for DBFlux");
                println!();
                println!("Usage: custom-driver --socket <name>");
                println!();
                println!("Options:");
                println!("  --socket <name>  Socket name to bind (required)");
                println!("  --help, -h       Show this help");
                std::process::exit(0);
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                std::process::exit(1);
            }
        }
    }

    Args {
        socket: socket.unwrap_or_else(|| {
            eprintln!("Error: --socket is required");
            eprintln!("Use --help for usage information");
            std::process::exit(1);
        }),
    }
}
