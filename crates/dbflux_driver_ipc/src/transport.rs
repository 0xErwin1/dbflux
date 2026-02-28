use std::sync::{Arc, Mutex};

use dbflux_core::DbError;
use dbflux_ipc::{
    driver_protocol::{
        DriverCapability, DriverHelloRequest, DriverHelloResponse, DriverRequestBody,
        DriverRequestEnvelope, DriverResponseBody, DriverResponseEnvelope,
    },
    framing, DRIVER_RPC_VERSION,
};
use interprocess::local_socket::{prelude::*, Name, Stream as IpcStream};
use uuid::Uuid;

pub struct RpcClient {
    stream: Arc<Mutex<IpcStream>>,
    request_id: Arc<Mutex<u64>>,
    hello: DriverHelloResponse,
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
    #[error("timeout")]
    Timeout,
}

impl From<RpcError> for DbError {
    fn from(err: RpcError) -> Self {
        match err {
            RpcError::SessionNotFound => DbError::QueryFailed("Session not found".into()),
            RpcError::Timeout => DbError::Timeout,
            RpcError::Driver(msg) => DbError::QueryFailed(msg.into()),
            RpcError::Protocol(msg) => DbError::QueryFailed(msg.into()),
            RpcError::ConnectionFailed(msg) => DbError::ConnectionFailed(msg.into()),
            RpcError::Io(e) => DbError::IoError(e),
        }
    }
}

impl RpcClient {
    /// Connects to a driver-host via a local socket name and performs the Hello handshake.
    pub fn connect(name: Name<'_>) -> Result<Self, RpcError> {
        let stream =
            IpcStream::connect(name).map_err(|e| RpcError::ConnectionFailed(e.to_string()))?;

        let stream = Arc::new(Mutex::new(stream));
        let request_id = Arc::new(Mutex::new(0));

        let hello = Self::perform_hello(&stream, &request_id)?;

        let client = Self {
            stream,
            request_id,
            hello,
        };

        Ok(client)
    }

    pub fn hello_response(&self) -> &DriverHelloResponse {
        &self.hello
    }

    fn perform_hello(
        stream: &Arc<Mutex<IpcStream>>,
        request_id: &Arc<Mutex<u64>>,
    ) -> Result<DriverHelloResponse, RpcError> {
        let request = DriverRequestEnvelope::new(
            0,
            DriverRequestBody::Hello(DriverHelloRequest {
                client_name: "dbflux_driver_ipc".to_string(),
                client_version: env!("CARGO_PKG_VERSION").to_string(),
                supported_versions: vec![DRIVER_RPC_VERSION],
                requested_capabilities: vec![
                    DriverCapability::Cancellation,
                    DriverCapability::ChunkedResults,
                    DriverCapability::SchemaIntrospection,
                    DriverCapability::MultiDatabase,
                ],
            }),
        );

        let response = Self::send_raw_with(stream, request_id, request)?;

        match response.body {
            DriverResponseBody::Hello(hello) => {
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

    fn send_raw_with(
        stream: &Arc<Mutex<IpcStream>>,
        _request_id: &Arc<Mutex<u64>>,
        request: DriverRequestEnvelope,
    ) -> Result<DriverResponseEnvelope, RpcError> {
        let mut stream_guard = stream
            .lock()
            .map_err(|_| RpcError::Protocol("Stream mutex poisoned".into()))?;

        framing::send_msg(&mut *stream_guard, &request)?;
        let response: DriverResponseEnvelope = framing::recv_msg(&mut *stream_guard)?;

        if response.request_id != request.request_id {
            return Err(RpcError::Protocol(format!(
                "Request ID mismatch: sent {}, got {}",
                request.request_id, response.request_id
            )));
        }

        Ok(response)
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
        let request_id = self.next_request_id();
        let mut envelope = DriverRequestEnvelope::new(request_id, body);

        if let Some(sid) = session_id {
            envelope = envelope.with_session(sid);
        }

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
    fn send_raw(&self, request: DriverRequestEnvelope) -> Result<DriverResponseEnvelope, RpcError> {
        let expected_id = request.request_id;

        let mut stream = self.stream.lock().unwrap();

        framing::send_msg(&mut *stream, &request).map_err(RpcError::Io)?;

        let response: DriverResponseEnvelope =
            framing::recv_msg(&mut *stream).map_err(RpcError::Io)?;

        if response.request_id != expected_id {
            return Err(RpcError::Protocol("Request ID mismatch".into()));
        }

        Ok(response)
    }

    fn next_request_id(&self) -> u64 {
        let mut id = self.request_id.lock().unwrap();
        *id += 1;
        *id
    }
}
