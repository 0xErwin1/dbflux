use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dbflux_core::DbError;
use dbflux_ipc::{
    DRIVER_RPC_AUTH_TOKEN_ENV, DRIVER_RPC_VERSION, ExternalAuditEmitter, ExternalAuditSource,
    ProtocolVersion, RpcApiFamily,
    driver_protocol::{
        DriverCapability, DriverHelloRequest, DriverHelloResponse, DriverRequestBody,
        DriverRequestEnvelope, DriverResponseBody, DriverResponseEnvelope,
    },
    driver_rpc_supported_versions, framing, process_auth_token,
};
use interprocess::local_socket::{Name, Stream as IpcStream, prelude::*};
use uuid::Uuid;

/// Holds the mutable transport state protected by a single mutex.
///
/// Both the stream and the request-ID counter live here so that ID assignment
/// and the subsequent send happen atomically — eliminating the gap between
/// `next_request_id()` releasing the old ID lock and `send_raw()` acquiring the
/// stream lock that existed when they were two separate mutexes.
///
/// `session_correlation_ids` is a SEPARATE mutex. The only lock ordering that
/// occurs at runtime is `inner → session_correlation_ids`: `send_raw` holds
/// `inner` when it calls `correlation_id_for_session`, which then acquires
/// `session_correlation_ids`. The reverse order (`session_correlation_ids →
/// inner`) never occurs, so there is no lock-ordering cycle.
struct RpcClientInner {
    stream: IpcStream,
    next_id: u64,
}

pub struct RpcClient {
    inner: Arc<Mutex<RpcClientInner>>,
    hello: DriverHelloResponse,
    /// Socket registry ID (`rpc:<socket_id>`) for correlation and logging.
    socket_id: String,
    /// Whether the driver advertised `DriverCapability::AuditEmit` in its hello.
    audit_emit_capability: bool,
    /// Sanitizing sink for audit frames emitted by this driver.
    audit_emitter: Option<Arc<dyn ExternalAuditEmitter>>,
    /// Per-session correlation IDs, allocated lazily on first audit emit for a session.
    session_correlation_ids: Mutex<HashMap<Uuid, String>>,
}

#[derive(thiserror::Error, Debug)]
pub enum RpcError {
    #[error("connection failed: {0}")]
    ConnectionFailed(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("session not found")]
    SessionNotFound,
    #[error("driver error: {0}")]
    Driver(String),
    #[error("unsupported method: {0}")]
    UnsupportedMethod(String),
    #[error("timeout")]
    Timeout,
}

impl From<RpcError> for DbError {
    fn from(err: RpcError) -> Self {
        match err {
            RpcError::SessionNotFound => DbError::QueryFailed("Session not found".into()),
            RpcError::Timeout => DbError::Timeout,
            RpcError::Driver(msg) => DbError::QueryFailed(msg.into()),
            RpcError::UnsupportedMethod(msg) => DbError::NotSupported(msg),
            RpcError::Protocol(msg) => DbError::QueryFailed(msg.into()),
            RpcError::ConnectionFailed(msg) => DbError::ConnectionFailed(msg.into()),
            RpcError::Io(e) => DbError::IoError(e),
        }
    }
}

impl RpcClient {
    /// Connects to a driver-host via a local socket name and performs the Hello handshake.
    pub fn connect(name: Name<'_>) -> Result<Self, RpcError> {
        Self::connect_with_audit(name, String::new(), None)
    }

    /// Connects with an audit emitter attached for handling `EmitAuditEvent` frames.
    ///
    /// `socket_id` is the registry key (`rpc:<socket_id>`) used for correlation.
    /// `audit_emitter` is `None` when audit emission is not wired (e.g. in tests).
    pub fn connect_with_audit(
        name: Name<'_>,
        socket_id: String,
        audit_emitter: Option<Arc<dyn ExternalAuditEmitter>>,
    ) -> Result<Self, RpcError> {
        let stream =
            IpcStream::connect(name).map_err(|e| RpcError::ConnectionFailed(e.to_string()))?;

        let mut inner = RpcClientInner { stream, next_id: 0 };
        let hello = Self::perform_hello(&mut inner)?;

        let audit_emit_capability = hello.capabilities.contains(&DriverCapability::AuditEmit);

        let client = Self {
            inner: Arc::new(Mutex::new(inner)),
            hello,
            socket_id,
            audit_emit_capability,
            audit_emitter,
            session_correlation_ids: Mutex::new(HashMap::new()),
        };

        Ok(client)
    }

    pub fn hello_response(&self) -> &DriverHelloResponse {
        &self.hello
    }

    pub fn selected_version(&self) -> ProtocolVersion {
        self.hello.selected_version
    }

    pub fn plan_semantic_request(
        &self,
        session_id: Uuid,
        request: dbflux_core::SemanticRequest,
    ) -> Result<dbflux_core::SemanticPlan, RpcError> {
        if !protocol_supports_semantic_planning(self.selected_version()) {
            return Err(RpcError::UnsupportedMethod(
                "Driver RPC host does not support semantic planning yet".to_string(),
            ));
        }

        let body = self.call(
            Some(session_id),
            DriverRequestBody::PlanSemantic { request },
        )?;

        match body {
            DriverResponseBody::SemanticPlan { plan } => Ok(plan),
            DriverResponseBody::Error(error) => match error.code {
                dbflux_ipc::driver_protocol::DriverRpcErrorCode::UnsupportedMethod => {
                    Err(RpcError::UnsupportedMethod(error.message))
                }
                _ => Err(RpcError::Driver(error.message)),
            },
            _ => Err(RpcError::Protocol(
                "Unexpected response to PlanSemantic".into(),
            )),
        }
    }

    fn perform_hello(inner: &mut RpcClientInner) -> Result<DriverHelloResponse, RpcError> {
        // The process-global store is the primary source; the caller-supplied
        // environment variable remains the fallback for processes that never
        // initialized the store (e.g. standalone `dbflux mcp`).
        let auth_token = process_auth_token()
            .filter(|token| !token.is_empty())
            .map(str::to_string)
            .or_else(|| {
                std::env::var(DRIVER_RPC_AUTH_TOKEN_ENV)
                    .ok()
                    .filter(|token| !token.is_empty())
            });

        let request = DriverRequestEnvelope::new(
            DRIVER_RPC_VERSION,
            0,
            DriverRequestBody::Hello(DriverHelloRequest {
                client_name: "dbflux_driver_ipc".to_string(),
                client_version: env!("CARGO_PKG_VERSION").to_string(),
                supported_versions: driver_rpc_supported_versions().to_vec(),
                requested_capabilities: vec![
                    DriverCapability::Cancellation,
                    DriverCapability::ChunkedResults,
                    DriverCapability::SchemaIntrospection,
                    DriverCapability::MultiDatabase,
                ],
                auth_token,
            }),
        );

        framing::send_msg(&mut inner.stream, &request)?;
        let response: DriverResponseEnvelope = framing::recv_msg(&mut inner.stream)?;

        if response.request_id != request.request_id {
            return Err(RpcError::Protocol(format!(
                "Request ID mismatch during hello: sent {}, got {}",
                request.request_id, response.request_id
            )));
        }

        match response.body {
            DriverResponseBody::Hello(hello) => {
                validate_hello_selected_version(
                    hello.selected_version,
                    driver_rpc_supported_versions(),
                )?;

                log::info!(
                    "Connected to driver host: {} v{}",
                    hello.server_name,
                    hello.server_version
                );
                Ok(hello)
            }
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol("Unexpected response to Hello".into())),
        }
    }

    /// Sends an OpenSession request and returns the full response body (caller
    /// needs both session_id and metadata).
    pub fn open_session(
        &self,
        profile_json: &str,
        password: Option<&str>,
        ssh_secret: Option<&str>,
    ) -> Result<DriverResponseBody, RpcError> {
        let body = self.call(
            None,
            DriverRequestBody::OpenSession {
                profile_json: profile_json.to_string(),
                password: password.map(|s| s.to_string()),
                ssh_secret: ssh_secret.map(|s| s.to_string()),
            },
        )?;

        match &body {
            DriverResponseBody::SessionOpened { .. } => Ok(body),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message.clone())),
            _ => Err(RpcError::Protocol(
                "Unexpected response to OpenSession".into(),
            )),
        }
    }

    pub fn close_session(&self, session_id: Uuid) -> Result<(), RpcError> {
        self.expect_variant(
            Some(session_id),
            DriverRequestBody::CloseSession,
            |body| matches!(body, DriverResponseBody::SessionClosed),
            "CloseSession",
        )
    }

    pub fn ping(&self, session_id: Uuid) -> Result<(), RpcError> {
        self.expect_variant(
            Some(session_id),
            DriverRequestBody::Ping,
            |body| matches!(body, DriverResponseBody::Pong),
            "Ping",
        )
    }

    pub fn schema(&self, session_id: Uuid) -> Result<dbflux_core::SchemaSnapshot, RpcError> {
        let body = self.call(Some(session_id), DriverRequestBody::Schema)?;
        match body {
            DriverResponseBody::Schema { schema } => Ok(schema),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol("Unexpected response to Schema".into())),
        }
    }

    pub fn execute(
        &self,
        session_id: Uuid,
        request: &dbflux_core::QueryRequest,
    ) -> Result<dbflux_core::QueryResult, RpcError> {
        let dto = dbflux_ipc::driver_protocol::QueryRequestDto::from(request);
        let body = self.call(
            Some(session_id),
            DriverRequestBody::Execute { request: dto },
        )?;

        match body {
            DriverResponseBody::ExecuteResult { result } => Ok(result.into()),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol("Unexpected response to Execute".into())),
        }
    }

    pub fn execute_with_handle(
        &self,
        session_id: Uuid,
        request: &dbflux_core::QueryRequest,
    ) -> Result<(Uuid, dbflux_core::QueryResult), RpcError> {
        let dto = dbflux_ipc::driver_protocol::QueryRequestDto::from(request);
        let body = self.call(
            Some(session_id),
            DriverRequestBody::ExecuteWithHandle { request: dto },
        )?;

        match body {
            DriverResponseBody::ExecuteWithHandleResult { handle_id, result } => {
                Ok((handle_id, result.into()))
            }
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to ExecuteWithHandle".into(),
            )),
        }
    }

    pub fn cancel(&self, session_id: Uuid, handle_id: Uuid) -> Result<(), RpcError> {
        self.expect_variant(
            Some(session_id),
            DriverRequestBody::Cancel { handle_id },
            |body| matches!(body, DriverResponseBody::Cancelled),
            "Cancel",
        )
    }

    pub fn cancel_active(&self, session_id: Uuid) -> Result<(), RpcError> {
        self.expect_variant(
            Some(session_id),
            DriverRequestBody::CancelActive,
            |body| matches!(body, DriverResponseBody::Cancelled),
            "CancelActive",
        )
    }

    pub fn cleanup_after_cancel(&self, session_id: Uuid) -> Result<(), RpcError> {
        self.expect_variant(
            Some(session_id),
            DriverRequestBody::CleanupAfterCancel,
            |body| matches!(body, DriverResponseBody::CleanupComplete),
            "CleanupAfterCancel",
        )
    }

    pub fn list_databases(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<dbflux_core::DatabaseInfo>, RpcError> {
        let body = self.call(Some(session_id), DriverRequestBody::ListDatabases)?;
        match body {
            DriverResponseBody::Databases { databases } => Ok(databases),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to ListDatabases".into(),
            )),
        }
    }

    pub fn schema_for_database(
        &self,
        session_id: Uuid,
        database: &str,
    ) -> Result<dbflux_core::DbSchemaInfo, RpcError> {
        let body = self.call(
            Some(session_id),
            DriverRequestBody::SchemaForDatabase {
                database: database.to_string(),
            },
        )?;

        match body {
            DriverResponseBody::SchemaForDatabase { schema } => Ok(schema),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to SchemaForDatabase".into(),
            )),
        }
    }

    pub fn table_details(
        &self,
        session_id: Uuid,
        database: &str,
        schema: Option<&str>,
        table: &str,
    ) -> Result<dbflux_core::TableInfo, RpcError> {
        let body = self.call(
            Some(session_id),
            DriverRequestBody::TableDetails {
                database: database.to_string(),
                schema: schema.map(|s| s.to_string()),
                table: table.to_string(),
            },
        )?;

        match body {
            DriverResponseBody::TableDetails { table } => Ok(table),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to TableDetails".into(),
            )),
        }
    }

    pub fn view_details(
        &self,
        session_id: Uuid,
        database: &str,
        schema: Option<&str>,
        view: &str,
    ) -> Result<dbflux_core::ViewInfo, RpcError> {
        let body = self.call(
            Some(session_id),
            DriverRequestBody::ViewDetails {
                database: database.to_string(),
                schema: schema.map(|s| s.to_string()),
                view: view.to_string(),
            },
        )?;

        match body {
            DriverResponseBody::ViewDetails { view } => Ok(view),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to ViewDetails".into(),
            )),
        }
    }

    pub fn set_active_database(
        &self,
        session_id: Uuid,
        database: Option<&str>,
    ) -> Result<(), RpcError> {
        self.expect_variant(
            Some(session_id),
            DriverRequestBody::SetActiveDatabase {
                database: database.map(|s| s.to_string()),
            },
            |body| matches!(body, DriverResponseBody::ActiveDatabaseSet),
            "SetActiveDatabase",
        )
    }

    pub fn active_database(&self, session_id: Uuid) -> Result<Option<String>, RpcError> {
        let body = self.call(Some(session_id), DriverRequestBody::ActiveDatabase)?;
        match body {
            DriverResponseBody::ActiveDatabaseResult { database } => Ok(database),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to ActiveDatabase".into(),
            )),
        }
    }

    // === Browse operations ===

    pub fn browse_table(
        &self,
        session_id: Uuid,
        request: dbflux_core::TableBrowseRequest,
    ) -> Result<dbflux_core::QueryResult, RpcError> {
        let body = self.call(Some(session_id), DriverRequestBody::BrowseTable { request })?;

        match body {
            DriverResponseBody::BrowseResult { result } => Ok(result.into()),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to BrowseTable".into(),
            )),
        }
    }

    pub fn count_table(
        &self,
        session_id: Uuid,
        request: dbflux_core::TableCountRequest,
    ) -> Result<u64, RpcError> {
        let body = self.call(Some(session_id), DriverRequestBody::CountTable { request })?;

        match body {
            DriverResponseBody::CountResult { count } => Ok(count),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to CountTable".into(),
            )),
        }
    }

    pub fn browse_collection(
        &self,
        session_id: Uuid,
        request: dbflux_core::CollectionBrowseRequest,
    ) -> Result<dbflux_core::QueryResult, RpcError> {
        let body = self.call(
            Some(session_id),
            DriverRequestBody::BrowseCollection { request },
        )?;

        match body {
            DriverResponseBody::BrowseResult { result } => Ok(result.into()),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to BrowseCollection".into(),
            )),
        }
    }

    pub fn count_collection(
        &self,
        session_id: Uuid,
        request: dbflux_core::CollectionCountRequest,
    ) -> Result<u64, RpcError> {
        let body = self.call(
            Some(session_id),
            DriverRequestBody::CountCollection { request },
        )?;

        match body {
            DriverResponseBody::CountResult { count } => Ok(count),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to CountCollection".into(),
            )),
        }
    }

    pub fn explain(
        &self,
        session_id: Uuid,
        request: dbflux_core::ExplainRequest,
    ) -> Result<dbflux_core::QueryResult, RpcError> {
        let body = self.call(Some(session_id), DriverRequestBody::Explain { request })?;

        match body {
            DriverResponseBody::BrowseResult { result } => Ok(result.into()),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol("Unexpected response to Explain".into())),
        }
    }

    pub fn describe_table(
        &self,
        session_id: Uuid,
        request: dbflux_core::DescribeRequest,
    ) -> Result<dbflux_core::QueryResult, RpcError> {
        let body = self.call(
            Some(session_id),
            DriverRequestBody::DescribeTable { request },
        )?;

        match body {
            DriverResponseBody::BrowseResult { result } => Ok(result.into()),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to DescribeTable".into(),
            )),
        }
    }

    // === CRUD operations ===

    pub fn update_row(
        &self,
        session_id: Uuid,
        patch: dbflux_core::RowPatch,
    ) -> Result<dbflux_core::CrudResult, RpcError> {
        let body = self.call(Some(session_id), DriverRequestBody::UpdateRow { patch })?;

        match body {
            DriverResponseBody::CrudResult { result } => Ok(result),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to UpdateRow".into(),
            )),
        }
    }

    pub fn insert_row(
        &self,
        session_id: Uuid,
        insert: dbflux_core::RowInsert,
    ) -> Result<dbflux_core::CrudResult, RpcError> {
        let body = self.call(Some(session_id), DriverRequestBody::InsertRow { insert })?;

        match body {
            DriverResponseBody::CrudResult { result } => Ok(result),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to InsertRow".into(),
            )),
        }
    }

    pub fn delete_row(
        &self,
        session_id: Uuid,
        delete: dbflux_core::RowDelete,
    ) -> Result<dbflux_core::CrudResult, RpcError> {
        let body = self.call(Some(session_id), DriverRequestBody::DeleteRow { delete })?;

        match body {
            DriverResponseBody::CrudResult { result } => Ok(result),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to DeleteRow".into(),
            )),
        }
    }

    // === Document mutations ===

    pub fn update_document(
        &self,
        session_id: Uuid,
        update: dbflux_core::DocumentUpdate,
    ) -> Result<dbflux_core::CrudResult, RpcError> {
        let body = self.call(
            Some(session_id),
            DriverRequestBody::UpdateDocument { update },
        )?;

        match body {
            DriverResponseBody::CrudResult { result } => Ok(result),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to UpdateDocument".into(),
            )),
        }
    }

    pub fn insert_document(
        &self,
        session_id: Uuid,
        insert: dbflux_core::DocumentInsert,
    ) -> Result<dbflux_core::CrudResult, RpcError> {
        let body = self.call(
            Some(session_id),
            DriverRequestBody::InsertDocument { insert },
        )?;

        match body {
            DriverResponseBody::CrudResult { result } => Ok(result),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to InsertDocument".into(),
            )),
        }
    }

    pub fn delete_document(
        &self,
        session_id: Uuid,
        delete: dbflux_core::DocumentDelete,
    ) -> Result<dbflux_core::CrudResult, RpcError> {
        let body = self.call(
            Some(session_id),
            DriverRequestBody::DeleteDocument { delete },
        )?;

        match body {
            DriverResponseBody::CrudResult { result } => Ok(result),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to DeleteDocument".into(),
            )),
        }
    }

    // === Schema extras ===

    pub fn schema_types(
        &self,
        session_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<dbflux_core::CustomTypeInfo>, RpcError> {
        let body = self.call(
            Some(session_id),
            DriverRequestBody::SchemaTypes {
                database: database.to_string(),
                schema: schema.map(|s| s.to_string()),
            },
        )?;

        match body {
            DriverResponseBody::SchemaTypes { types } => Ok(types),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to SchemaTypes".into(),
            )),
        }
    }

    pub fn schema_indexes(
        &self,
        session_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<dbflux_core::SchemaIndexInfo>, RpcError> {
        let body = self.call(
            Some(session_id),
            DriverRequestBody::SchemaIndexes {
                database: database.to_string(),
                schema: schema.map(|s| s.to_string()),
            },
        )?;

        match body {
            DriverResponseBody::SchemaIndexes { indexes } => Ok(indexes),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to SchemaIndexes".into(),
            )),
        }
    }

    pub fn schema_foreign_keys(
        &self,
        session_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<dbflux_core::SchemaForeignKeyInfo>, RpcError> {
        let body = self.call(
            Some(session_id),
            DriverRequestBody::SchemaForeignKeys {
                database: database.to_string(),
                schema: schema.map(|s| s.to_string()),
            },
        )?;

        match body {
            DriverResponseBody::SchemaForeignKeys { foreign_keys } => Ok(foreign_keys),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to SchemaForeignKeys".into(),
            )),
        }
    }

    pub fn schema_columns(
        &self,
        session_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<dbflux_core::SchemaColumnInfo>, RpcError> {
        let body = self.call(
            Some(session_id),
            DriverRequestBody::SchemaColumns {
                database: database.to_string(),
                schema: schema.map(|s| s.to_string()),
            },
        )?;

        match body {
            DriverResponseBody::SchemaColumns { columns } => Ok(columns),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to SchemaColumns".into(),
            )),
        }
    }

    // === Key-Value operations ===

    pub fn kv_call(
        &self,
        session_id: Uuid,
        request_body: DriverRequestBody,
    ) -> Result<DriverResponseBody, RpcError> {
        self.call(Some(session_id), request_body)
    }

    // === Code generation ===

    pub fn code_generators(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<dbflux_core::CodeGeneratorInfo>, RpcError> {
        let body = self.call(Some(session_id), DriverRequestBody::CodeGenerators)?;
        match body {
            DriverResponseBody::CodeGeneratorsResult { generators } => Ok(generators),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to CodeGenerators".into(),
            )),
        }
    }

    pub fn generate_code(
        &self,
        session_id: Uuid,
        generator_id: &str,
        table: &dbflux_core::TableInfo,
    ) -> Result<String, RpcError> {
        let body = self.call(
            Some(session_id),
            DriverRequestBody::GenerateCode {
                generator_id: generator_id.to_string(),
                table: table.clone(),
            },
        )?;

        match body {
            DriverResponseBody::GenerateCodeResult { code } => Ok(code),
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(
                "Unexpected response to GenerateCode".into(),
            )),
        }
    }

    // === Internal helpers ===

    /// Send a request and return the response body, handling errors generically.
    fn call(
        &self,
        session_id: Option<Uuid>,
        body: DriverRequestBody,
    ) -> Result<DriverResponseBody, RpcError> {
        // Pass request_id=0; send_raw assigns the actual ID inside the lock.
        let envelope = build_call_request_envelope(self.selected_version(), 0, body, session_id);
        let response = self.send_raw(envelope)?;
        Ok(response.body)
    }

    /// Helper for requests where we only need to verify the response matches
    /// an expected variant (no data to extract).
    fn expect_variant(
        &self,
        session_id: Option<Uuid>,
        body: DriverRequestBody,
        check: impl Fn(&DriverResponseBody) -> bool,
        label: &str,
    ) -> Result<(), RpcError> {
        let response_body = self.call(session_id, body)?;

        if check(&response_body) {
            return Ok(());
        }

        match response_body {
            DriverResponseBody::Error(e) => Err(RpcError::Driver(e.message)),
            _ => Err(RpcError::Protocol(format!(
                "Unexpected response to {label}"
            ))),
        }
    }

    /// Low-level send/receive with request-ID correlation.
    ///
    /// Acquires the single `inner` lock, increments `next_id`, assigns it to the
    /// request envelope, sends, and receives all frames until `done = true`. The
    /// lock is held across the entire send+receive transaction so that ID assignment
    /// and transport are atomic — no other caller can interleave on the stream.
    ///
    /// `session_correlation_ids` is acquired inside this function while `inner`
    /// is already held (`inner → session_correlation_ids`). The reverse order never
    /// occurs anywhere, so the one-directional ordering is deadlock-free.
    fn send_raw(
        &self,
        mut request: DriverRequestEnvelope,
    ) -> Result<DriverResponseEnvelope, RpcError> {
        let request_session_id = request.session_id;

        let mut guard = self
            .inner
            .lock()
            .map_err(|_| RpcError::Protocol("RPC client mutex poisoned".into()))?;

        guard.next_id += 1;
        let expected_id = guard.next_id;
        request.request_id = expected_id;

        framing::send_msg(&mut guard.stream, &request).map_err(RpcError::Io)?;

        loop {
            let response: DriverResponseEnvelope =
                framing::recv_msg(&mut guard.stream).map_err(RpcError::Io)?;

            if response.request_id != expected_id {
                return Err(RpcError::Protocol("Request ID mismatch".into()));
            }

            validate_response_protocol_version(
                request.protocol_version,
                response.protocol_version,
            )?;

            match response.body {
                DriverResponseBody::EmitAuditEvent(ref dto) if !response.done => {
                    if self.audit_emit_capability
                        && let Some(sink) = &self.audit_emitter
                    {
                        let session_id = response.session_id.or(request_session_id);
                        let correlation_id = self.correlation_id_for_session(session_id);
                        sink.emit(
                            ExternalAuditSource::Driver {
                                socket_id: self.socket_id.clone(),
                                session_id,
                                correlation_id,
                            },
                            dto.clone(),
                        );
                    }
                    // Loop to consume the next frame regardless of capability/emitter.
                    continue;
                }
                _ => return Ok(response),
            }
        }
    }

    fn correlation_id_for_session(&self, session_id: Option<Uuid>) -> String {
        let Some(session_id) = session_id else {
            return Uuid::new_v4().to_string();
        };

        let mut map = self
            .session_correlation_ids
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        map.entry(session_id)
            .or_insert_with(|| Uuid::new_v4().to_string())
            .clone()
    }
}

fn protocol_supports_semantic_planning(version: ProtocolVersion) -> bool {
    version.major > DRIVER_RPC_VERSION.major
        || (version.major == DRIVER_RPC_VERSION.major && version.minor >= 1)
}

fn build_call_request_envelope(
    selected_version: ProtocolVersion,
    request_id: u64,
    body: DriverRequestBody,
    session_id: Option<Uuid>,
) -> DriverRequestEnvelope {
    let mut envelope = DriverRequestEnvelope::new(selected_version, request_id, body);

    if let Some(session_id) = session_id {
        envelope = envelope.with_session(session_id);
    }

    envelope
}

fn validate_hello_selected_version(
    selected_version: ProtocolVersion,
    client_supported_versions: &[ProtocolVersion],
) -> Result<(), RpcError> {
    let selected_contract =
        dbflux_ipc::RpcApiContract::new(RpcApiFamily::DriverRpc, selected_version);
    let client_contract =
        dbflux_ipc::RpcApiContract::new(RpcApiFamily::DriverRpc, DRIVER_RPC_VERSION);

    if !client_contract.is_compatible_with(selected_contract)
        || !client_supported_versions.contains(&selected_version)
    {
        return Err(RpcError::Protocol(format!(
            "Driver host returned unsupported selected_version {}.{}",
            selected_version.major, selected_version.minor
        )));
    }

    Ok(())
}

fn validate_response_protocol_version(
    negotiated_version: ProtocolVersion,
    response_version: ProtocolVersion,
) -> Result<(), RpcError> {
    if negotiated_version != response_version {
        return Err(RpcError::Protocol(format!(
            "Driver host responded with protocol {}.{} but negotiated version is {}.{}",
            response_version.major,
            response_version.minor,
            negotiated_version.major,
            negotiated_version.minor
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        RpcClient, build_call_request_envelope, protocol_supports_semantic_planning,
        validate_hello_selected_version, validate_response_protocol_version,
    };
    use dbflux_ipc::audit::{
        AuditEventEmitDto, EventCategoryDto, EventOutcomeDto, EventSeverityDto,
        ExternalAuditEmitter, ExternalAuditSource,
    };
    use dbflux_ipc::driver_protocol::DriverRequestBody;
    use dbflux_ipc::{ProtocolVersion, driver_rpc_supported_versions, driver_socket_name};
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    struct RecordingEmitter {
        calls: Mutex<Vec<(ExternalAuditSource, AuditEventEmitDto)>>,
    }

    impl RecordingEmitter {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
            })
        }

        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    impl ExternalAuditEmitter for RecordingEmitter {
        fn emit(&self, source: ExternalAuditSource, dto: AuditEventEmitDto) {
            self.calls.lock().unwrap().push((source, dto));
        }
    }

    fn fake_audit_dto() -> AuditEventEmitDto {
        AuditEventEmitDto {
            ts_ms: 1_000_000,
            level: EventSeverityDto::Info,
            category: EventCategoryDto::Connection,
            outcome: EventOutcomeDto::Success,
            action: "test_action".to_string(),
            summary: "test summary".to_string(),
            object_type: None,
            object_id: None,
            duration_ms: None,
            error_code: None,
            error_message: None,
            details_json: None,
        }
    }

    #[test]
    fn rpc_client_with_audit_capability_dispatches_emit_frame_to_emitter() {
        use dbflux_test_support::{FakeDriverAction, FakeDriverRpcConfig, FakeDriverRpcServer};

        let socket_id = format!("test-audit-emit-{}", Uuid::new_v4());
        let server = FakeDriverRpcServer::start(
            FakeDriverRpcConfig::new(&socket_id)
                .with_audit_emit_capability()
                .with_actions(vec![FakeDriverAction::EmitAuditThenPong(fake_audit_dto())]),
        )
        .expect("fake driver server must start");

        let emitter = RecordingEmitter::new();
        let socket_name = driver_socket_name(&socket_id).expect("socket name");
        let client = RpcClient::connect_with_audit(
            socket_name.borrow(),
            socket_id.clone(),
            Some(emitter.clone() as Arc<dyn ExternalAuditEmitter>),
        )
        .expect("connect must succeed");

        client.ping(Uuid::nil()).expect("ping must succeed");

        server.wait().expect("server must exit cleanly");

        assert_eq!(emitter.call_count(), 1, "emitter must be called once");
    }

    #[test]
    fn rpc_client_without_audit_capability_drops_emit_frame_silently() {
        use dbflux_test_support::{FakeDriverAction, FakeDriverRpcConfig, FakeDriverRpcServer};

        let socket_id = format!("test-audit-no-cap-{}", Uuid::new_v4());
        let server = FakeDriverRpcServer::start(
            FakeDriverRpcConfig::new(&socket_id)
                .with_actions(vec![FakeDriverAction::EmitAuditThenPong(fake_audit_dto())]),
        )
        .expect("fake driver server must start");

        let emitter = RecordingEmitter::new();
        let socket_name = driver_socket_name(&socket_id).expect("socket name");
        let client = RpcClient::connect_with_audit(
            socket_name.borrow(),
            socket_id.clone(),
            Some(emitter.clone() as Arc<dyn ExternalAuditEmitter>),
        )
        .expect("connect must succeed");

        client.ping(Uuid::nil()).expect("ping must succeed");

        server.wait().expect("server must exit cleanly");

        assert_eq!(
            emitter.call_count(),
            0,
            "emitter must not be called when capability is absent"
        );
    }

    #[test]
    fn semantic_planning_requires_driver_rpc_v1_1_or_newer() {
        assert!(!protocol_supports_semantic_planning(ProtocolVersion::new(
            1, 0
        )));
        assert!(protocol_supports_semantic_planning(ProtocolVersion::new(
            1, 1
        )));
        assert!(protocol_supports_semantic_planning(ProtocolVersion::new(
            2, 0
        )));
    }

    #[test]
    fn hello_selected_version_must_be_supported_by_both_peers() {
        let error = validate_hello_selected_version(
            ProtocolVersion::new(1, 99),
            driver_rpc_supported_versions(),
        )
        .expect_err("unsupported selection should be rejected");

        assert!(error.to_string().contains("unsupported selected_version"));
    }

    #[test]
    fn hello_selected_version_accepts_supported_downgrade() {
        validate_hello_selected_version(
            ProtocolVersion::new(1, 0),
            driver_rpc_supported_versions(),
        )
        .expect("selected downgrade within client support should be accepted");
    }

    #[test]
    fn hello_selected_version_rejects_major_mismatch_even_if_listed() {
        let error = validate_hello_selected_version(
            ProtocolVersion::new(2, 0),
            &[ProtocolVersion::new(2, 0)],
        )
        .expect_err("major mismatch should be rejected");

        assert!(error.to_string().contains("unsupported selected_version"));
    }

    #[test]
    fn response_protocol_version_must_match_negotiated_version() {
        let error = validate_response_protocol_version(
            ProtocolVersion::new(1, 0),
            ProtocolVersion::new(1, 1),
        )
        .expect_err("response drift should be rejected");

        assert!(error.to_string().contains("negotiated version"));
    }

    #[test]
    fn call_request_envelope_uses_negotiated_version_and_session_id() {
        let session_id = Uuid::nil();

        let envelope = build_call_request_envelope(
            ProtocolVersion::new(1, 0),
            7,
            DriverRequestBody::Ping,
            Some(session_id),
        );

        assert_eq!(envelope.protocol_version, ProtocolVersion::new(1, 0));
        assert_eq!(envelope.request_id, 7);
        assert_eq!(envelope.session_id, Some(session_id));
    }

    #[test]
    fn call_request_envelope_omits_session_id_when_absent() {
        let envelope = build_call_request_envelope(
            ProtocolVersion::new(1, 0),
            8,
            DriverRequestBody::Ping,
            None,
        );

        assert_eq!(envelope.protocol_version, ProtocolVersion::new(1, 0));
        assert_eq!(envelope.request_id, 8);
        assert_eq!(envelope.session_id, None);
    }

    // =========================================================================
    // Single RpcClientInner mutex — concurrent call correctness
    // =========================================================================

    #[test]
    fn test_concurrent_calls_correct_correlation() {
        use dbflux_test_support::{FakeDriverAction, FakeDriverRpcConfig, FakeDriverRpcServer};
        use std::sync::Arc;

        const THREAD_COUNT: usize = 100;

        let socket_id = format!("test-concurrent-rpc-{}", Uuid::new_v4());

        // Serve exactly 100 Pong responses on one connection.
        let server = FakeDriverRpcServer::start(
            FakeDriverRpcConfig::new(&socket_id)
                .with_actions(vec![FakeDriverAction::Pong; THREAD_COUNT])
                .with_expected_connections(1),
        )
        .expect("fake driver server must start");

        let socket_name = driver_socket_name(&socket_id).expect("socket name");
        let client =
            Arc::new(RpcClient::connect(socket_name.borrow()).expect("connect must succeed"));

        let handles: Vec<_> = (0..THREAD_COUNT)
            .map(|_| {
                let client = client.clone();
                std::thread::spawn(move || client.ping(Uuid::nil()))
            })
            .collect();

        for handle in handles {
            handle
                .join()
                .expect("thread must not panic")
                .expect("ping must succeed — no request-ID mismatch under concurrent load");
        }

        server.wait().expect("server must exit cleanly");
    }

    /// IT-07: rate-limit exhausted on a socket, then a subsequent Ping still succeeds.
    ///
    /// REQ-R-03 / Scenario R-03-a: rate-limit drops must not set error state on the IPC session.
    #[test]
    fn rate_limit_exhausted_session_continues_for_subsequent_request() {
        use dbflux_test_support::{FakeDriverAction, FakeDriverRpcConfig, FakeDriverRpcServer};

        let socket_id = format!("test-rate-limit-session-{}", Uuid::new_v4());

        // Two requests: first sends 200 audit frames (100 accepted, 100 dropped), then a plain Pong.
        let server = FakeDriverRpcServer::start(
            FakeDriverRpcConfig::new(&socket_id)
                .with_audit_emit_capability()
                .with_actions(vec![
                    FakeDriverAction::EmitNAuditThenPong(200, fake_audit_dto()),
                    FakeDriverAction::Pong,
                ])
                .with_expected_connections(1),
        )
        .expect("fake driver server must start");

        let emitter = RecordingEmitter::new();
        let socket_name = driver_socket_name(&socket_id).expect("socket name");
        let client = RpcClient::connect_with_audit(
            socket_name.borrow(),
            socket_id.clone(),
            Some(emitter.clone() as Arc<dyn ExternalAuditEmitter>),
        )
        .expect("connect must succeed");

        // First request: 200 audit frames sent by the server; emitter receives all 200
        // because the emitter here is the raw RecordingEmitter (no rate limiter at this layer).
        // The important invariant is that the RpcClient loop handles them all and returns Ok.
        client
            .ping(Uuid::nil())
            .expect("first ping must succeed despite burst of audit frames");

        // Second request: no audit frames, plain Pong. Session must still be usable.
        client
            .ping(Uuid::nil())
            .expect("second ping must succeed — IPC session must be intact after rate-limit burst");

        server.wait().expect("server must exit cleanly");

        assert_eq!(
            emitter.call_count(),
            200,
            "all 200 audit frames must be forwarded to the emitter by the transport loop"
        );
    }

    // =========================================================================
    // Process token store — delivery to spawned hosts and env fallback
    // =========================================================================

    const ISOLATED_MARKER_VAR: &str = "DBFLUX_DRIVER_IPC_TOKEN_ISOLATED_FIXTURE";

    /// OS variables required to locate and run executables on each platform.
    const OS_ESSENTIAL_VARS: &[&str] =
        &["PATH", "SystemRoot", "SystemDrive", "TEMP", "TMP", "TMPDIR"];

    /// Runs `test_name` in an isolated child process with a cleared environment
    /// (the approach of `dbflux_core::isolated_env`). Returns `Ok(false)` when
    /// the caller is the isolated child; `Ok(true)` when the parent ran the
    /// child and it passed exactly the named test once; `Err` otherwise.
    fn run_in_isolated_fixture(
        test_name: &str,
        fixture_variables: &[(&str, &std::ffi::OsStr)],
    ) -> Result<bool, String> {
        if std::env::var_os(ISOLATED_MARKER_VAR).is_some_and(|marker| marker == test_name) {
            return Ok(false);
        }

        let test_executable = std::env::current_exe()
            .map_err(|error| format!("current test binary must be locatable: {error}"))?;
        let mut command = std::process::Command::new(test_executable);
        command
            .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
            .env_clear();
        for key in OS_ESSENTIAL_VARS {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        for (key, value) in fixture_variables {
            command.env(key, value);
        }
        command.env(ISOLATED_MARKER_VAR, test_name);

        let output = command.output().map_err(|error| {
            format!("isolated fixture child process must be spawnable: {error}")
        })?;

        let summary = String::from_utf8_lossy(&output.stdout);
        if !(output.status.success() && summary.contains("1 passed; 0 failed")) {
            return Err(format!(
                "isolated fixture child must run and pass exactly the selected test (anti-vacuum); status: {:?}; child stderr (first 8k chars): {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
                    .chars()
                    .take(8192)
                    .collect::<String>()
            ));
        }
        Ok(true)
    }

    /// Unique scratch data directory for a fixture child, so the token file
    /// write never touches user data.
    fn fixture_data_dir(label: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir().join(format!(
            "dbflux-driver-ipc-token-fixture-{}-{}-{}",
            label,
            std::process::id(),
            nanos
        ))
    }

    /// A managed driver-host launch whose "host" is a shell command that
    /// records the token it received into a file, so the spawn-time delivery
    /// path is observable without running a real driver host. Unix only: it
    /// relies on `sh`.
    #[cfg(unix)]
    #[test]
    fn spawned_driver_host_receives_store_token_explicitly() {
        use crate::driver::{IpcDriver, IpcDriverLaunchConfig};

        let data_dir = fixture_data_dir("driver-spawn");

        if run_in_isolated_fixture(
            "transport::tests::spawned_driver_host_receives_store_token_explicitly",
            &[
                ("XDG_DATA_HOME", data_dir.as_os_str()),
                ("HOME", data_dir.as_os_str()),
            ],
        )
        .expect("isolated fixture must pass")
        {
            return;
        }

        // Child side: the store holds the token; the environment does not.
        let token =
            dbflux_ipc::init_process_auth_tokens().expect("token initialization must succeed");

        // The scratch dir is whatever the parent provisioned via the fixture.
        let data_dir = std::env::var_os("XDG_DATA_HOME")
            .map(std::path::PathBuf::from)
            .expect("fixture must provide XDG_DATA_HOME");

        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default();
        let socket_id = format!("drv-token-{}-{}", std::process::id(), nanos);
        let token_out = data_dir.join("driver_host_token");

        let launch = IpcDriverLaunchConfig {
            program: "sh".to_string(),
            args: vec![
                "-c".to_string(),
                "printf '%s' \"$DBFLUX_DRIVER_IPC_TOKEN\" > \"$TOKEN_OUT_FILE\"".to_string(),
            ],
            env: vec![(
                "TOKEN_OUT_FILE".to_string(),
                token_out.display().to_string(),
            )],
            startup_timeout: std::time::Duration::from_millis(500),
        };

        // The probe fails by design: the shell host exits without serving a
        // socket. Its error is irrelevant; the delivered token is the assertion.
        let _probe_error = IpcDriver::probe_driver(&socket_id, Some(&launch));
        if let Err(error) = &_probe_error {
            eprintln!("probe error (expected, host is a stub): {error}");
        }

        let delivered = std::fs::read_to_string(&token_out)
            .expect("spawned driver host must write the delivered token");
        assert!(
            delivered == token,
            "spawned driver host must receive the store token explicitly \
             (delivered length {}, expected length {})",
            delivered.len(),
            token.len()
        );
    }

    /// Regression: with an empty store the parent authenticates with the
    /// caller-supplied environment token, so the child must receive that same
    /// token — a launch-config entry for the same variable must not win.
    #[cfg(unix)]
    #[test]
    fn spawned_driver_host_env_token_beats_launch_config_when_store_is_empty() {
        use crate::driver::{IpcDriver, IpcDriverLaunchConfig};

        let data_dir = fixture_data_dir("driver-spawn-env");

        if run_in_isolated_fixture(
            "transport::tests::spawned_driver_host_env_token_beats_launch_config_when_store_is_empty",
            &[
                ("XDG_DATA_HOME", data_dir.as_os_str()),
                ("HOME", data_dir.as_os_str()),
                    (
                    "DBFLUX_DRIVER_IPC_TOKEN",
                    std::ffi::OsStr::new("env-fallback-token"),
                ),
            ],
        )
        .expect("isolated fixture must pass")
        {
            return;
        }

        // Child side: no init call, so the store is empty and the environment is
        // the only token source the parent uses.
        let data_dir = std::env::var_os("XDG_DATA_HOME")
            .map(std::path::PathBuf::from)
            .expect("fixture must provide XDG_DATA_HOME");

        // Nothing here initializes the token store, so unlike the store test
        // this fixture must create the directory it writes into.
        std::fs::create_dir_all(&data_dir).expect("fixture data dir must be creatable");

        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default();
        let socket_id = format!("drv-env-token-{}-{}", std::process::id(), nanos);
        let token_out = data_dir.join("driver_host_env_token");

        let launch = IpcDriverLaunchConfig {
            program: "sh".to_string(),
            args: vec![
                "-c".to_string(),
                "printf '%s' \"$DBFLUX_DRIVER_IPC_TOKEN\" > \"$TOKEN_OUT_FILE\"".to_string(),
            ],
            env: vec![
                (
                    "TOKEN_OUT_FILE".to_string(),
                    token_out.display().to_string(),
                ),
                (
                    dbflux_ipc::DRIVER_RPC_AUTH_TOKEN_ENV.to_string(),
                    "stale-launch-config-token".to_string(),
                ),
            ],
            startup_timeout: std::time::Duration::from_millis(500),
        };

        // The probe fails by design: the shell host exits without serving a
        // socket. Its error is irrelevant; the delivered token is the assertion.
        let _probe_error = IpcDriver::probe_driver(&socket_id, Some(&launch));

        let delivered = std::fs::read_to_string(&token_out)
            .expect("spawned driver host must write the delivered token");
        assert!(
            delivered == "env-fallback-token",
            "an empty store must inject the caller supplied environment token, \
             not the launch-config entry (delivered {delivered:?})"
        );
    }

    /// Previous behavior preserved: when the store is empty (no
    /// `init_process_auth_tokens()` call, e.g. standalone `dbflux mcp`), a
    /// caller-supplied `DBFLUX_DRIVER_IPC_TOKEN` must still reach the Hello.
    #[test]
    fn driver_hello_carries_env_token_when_store_is_empty() {
        use interprocess::local_socket::traits::Listener;
        use interprocess::local_socket::{ListenerNonblockingMode::Neither, ListenerOptions};

        let data_dir = fixture_data_dir("driver-fallback");

        if run_in_isolated_fixture(
            "transport::tests::driver_hello_carries_env_token_when_store_is_empty",
            &[
                ("XDG_DATA_HOME", data_dir.as_os_str()),
                ("HOME", data_dir.as_os_str()),
                (
                    "DBFLUX_DRIVER_IPC_TOKEN",
                    std::ffi::OsStr::new("fallback-env-token"),
                ),
            ],
        )
        .expect("isolated fixture must pass")
        {
            return;
        }

        // Child side: no init call, so the store is empty; the fixture-provided
        // environment variable is the only token source.
        let expected = std::env::var(dbflux_ipc::DRIVER_RPC_AUTH_TOKEN_ENV)
            .ok()
            .filter(|token| !token.is_empty())
            .expect("fixture must provide the fallback token variable");

        let socket_id = format!(
            "drv-fallback-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|since| since.as_nanos())
                .unwrap_or_default()
        );
        let name = dbflux_ipc::driver_socket_name(&socket_id).expect("socket name");

        let listener = ListenerOptions::new()
            .name(name.borrow())
            .nonblocking(Neither)
            .create_sync()
            .expect("fake driver listener must bind");

        let server = std::thread::spawn(move || {
            let mut stream = listener.accept().expect("accept must succeed");
            let request: dbflux_ipc::driver_protocol::DriverRequestEnvelope =
                dbflux_ipc::framing::recv_msg(&mut stream).expect("hello request must be readable");
            request
        });

        // The client fails once the server drops the stream after reading the
        // Hello; the captured request is the assertion.
        let _client = RpcClient::connect(name.borrow());

        let request = server.join().expect("server thread must not panic");
        let dbflux_ipc::driver_protocol::DriverRequestBody::Hello(hello) = request.body else {
            panic!("expected a Hello request");
        };
        assert_eq!(
            hello.auth_token.as_deref(),
            Some(expected.as_str()),
            "hello must carry the caller-supplied environment token when the store is empty"
        );
    }
}
