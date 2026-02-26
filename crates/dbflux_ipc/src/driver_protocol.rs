use crate::envelope::{ProtocolVersion, DRIVER_RPC_VERSION};
use dbflux_core::{
    ColumnMeta, DatabaseInfo, DbSchemaInfo, QueryRequest, QueryResult, QueryResultShape,
    SchemaSnapshot, TableInfo, Value, ViewInfo,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

/// Feature flags advertised during driver RPC handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DriverCapability {
    Cancellation,
    ChunkedResults,
    SchemaIntrospection,
    MultiDatabase,
}

/// Well-known error categories for driver RPC responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DriverRpcErrorCode {
    InvalidRequest,
    UnsupportedMethod,
    VersionMismatch,
    SessionNotFound,
    Timeout,
    Cancelled,
    Transport,
    Driver,
    Internal,
}

/// Structured error returned by the driver RPC protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverRpcError {
    pub code: DriverRpcErrorCode,
    pub message: String,
    pub retriable: bool,
}

/// Serializable representation of `QueryRequest`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequestDto {
    pub sql: String,
    pub params: Vec<Value>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub statement_timeout_ms: Option<u64>,
    pub database: Option<String>,
}

impl From<&QueryRequest> for QueryRequestDto {
    fn from(value: &QueryRequest) -> Self {
        Self {
            sql: value.sql.clone(),
            params: value.params.clone(),
            limit: value.limit,
            offset: value.offset,
            statement_timeout_ms: value
                .statement_timeout
                .map(|timeout| timeout.as_millis() as u64),
            database: value.database.clone(),
        }
    }
}

impl From<QueryRequestDto> for QueryRequest {
    fn from(value: QueryRequestDto) -> Self {
        Self {
            sql: value.sql,
            params: value.params,
            limit: value.limit,
            offset: value.offset,
            statement_timeout: value.statement_timeout_ms.map(Duration::from_millis),
            database: value.database,
        }
    }
}

/// Serializable representation of `QueryResultShape`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryResultShapeDto {
    Table,
    Json,
    Text,
    Binary,
}

impl From<QueryResultShape> for QueryResultShapeDto {
    fn from(value: QueryResultShape) -> Self {
        match value {
            QueryResultShape::Table => Self::Table,
            QueryResultShape::Json => Self::Json,
            QueryResultShape::Text => Self::Text,
            QueryResultShape::Binary => Self::Binary,
        }
    }
}

impl From<QueryResultShapeDto> for QueryResultShape {
    fn from(value: QueryResultShapeDto) -> Self {
        match value {
            QueryResultShapeDto::Table => Self::Table,
            QueryResultShapeDto::Json => Self::Json,
            QueryResultShapeDto::Text => Self::Text,
            QueryResultShapeDto::Binary => Self::Binary,
        }
    }
}

/// Serializable representation of `QueryResult`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResultDto {
    pub shape: QueryResultShapeDto,
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Vec<Value>>,
    pub affected_rows: Option<u64>,
    pub execution_time_ms: u64,
    pub text_body: Option<String>,
    pub raw_bytes: Option<Vec<u8>>,
}

impl From<&QueryResult> for QueryResultDto {
    fn from(value: &QueryResult) -> Self {
        Self {
            shape: value.shape.clone().into(),
            columns: value.columns.clone(),
            rows: value.rows.clone(),
            affected_rows: value.affected_rows,
            execution_time_ms: value.execution_time.as_millis() as u64,
            text_body: value.text_body.clone(),
            raw_bytes: value.raw_bytes.clone(),
        }
    }
}

impl From<QueryResultDto> for QueryResult {
    fn from(value: QueryResultDto) -> Self {
        Self {
            shape: value.shape.into(),
            columns: value.columns,
            rows: value.rows,
            affected_rows: value.affected_rows,
            execution_time: Duration::from_millis(value.execution_time_ms),
            text_body: value.text_body,
            raw_bytes: value.raw_bytes,
        }
    }
}

/// Payload for optional chunked query responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResultChunk {
    pub chunk_index: u32,
    pub rows: Vec<Vec<Value>>,
    pub done: bool,
}

/// Handshake request sent by IPC clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverHelloRequest {
    pub client_name: String,
    pub client_version: String,
    pub supported_versions: Vec<ProtocolVersion>,
    pub requested_capabilities: Vec<DriverCapability>,
}

/// Handshake response sent by driver hosts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverHelloResponse {
    pub server_name: String,
    pub server_version: String,
    pub selected_version: ProtocolVersion,
    pub capabilities: Vec<DriverCapability>,
}

/// Request body for a single driver RPC call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DriverRequestBody {
    Hello(DriverHelloRequest),
    OpenSession,
    CloseSession,
    Ping,
    Execute {
        request: QueryRequestDto,
    },
    ExecuteWithHandle {
        request: QueryRequestDto,
    },
    Cancel {
        handle_id: Uuid,
    },
    CancelActive,
    CleanupAfterCancel,
    Schema,
    ListDatabases,
    SchemaForDatabase {
        database: String,
    },
    TableDetails {
        database: String,
        schema: Option<String>,
        table: String,
    },
    ViewDetails {
        database: String,
        schema: Option<String>,
        view: String,
    },
    SetActiveDatabase {
        database: Option<String>,
    },
}

/// Request envelope for driver RPC operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverRequestEnvelope {
    pub protocol_version: ProtocolVersion,
    pub request_id: u64,
    pub session_id: Option<Uuid>,
    pub timeout_ms: Option<u64>,
    pub body: DriverRequestBody,
}

impl DriverRequestEnvelope {
    pub fn new(request_id: u64, body: DriverRequestBody) -> Self {
        Self {
            protocol_version: DRIVER_RPC_VERSION,
            request_id,
            session_id: None,
            timeout_ms: None,
            body,
        }
    }

    pub fn with_session(mut self, session_id: Uuid) -> Self {
        self.session_id = Some(session_id);
        self
    }

    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }
}

/// Response body for a single driver RPC call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DriverResponseBody {
    Hello(DriverHelloResponse),
    SessionOpened {
        session_id: Uuid,
    },
    SessionClosed,
    Pong,
    ExecuteResult {
        result: QueryResultDto,
    },
    ExecuteWithHandleResult {
        handle_id: Uuid,
        result: QueryResultDto,
    },
    QueryChunk(QueryResultChunk),
    Cancelled,
    CleanupComplete,
    Schema {
        schema: SchemaSnapshot,
    },
    Databases {
        databases: Vec<DatabaseInfo>,
    },
    SchemaForDatabase {
        schema: DbSchemaInfo,
    },
    TableDetails {
        table: TableInfo,
    },
    ViewDetails {
        view: ViewInfo,
    },
    ActiveDatabaseSet,
    Error(DriverRpcError),
}

/// Response envelope for driver RPC operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverResponseEnvelope {
    pub protocol_version: ProtocolVersion,
    pub request_id: u64,
    pub session_id: Option<Uuid>,
    pub done: bool,
    pub body: DriverResponseBody,
}

impl DriverResponseEnvelope {
    pub fn ok(request_id: u64, session_id: Option<Uuid>, body: DriverResponseBody) -> Self {
        Self {
            protocol_version: DRIVER_RPC_VERSION,
            request_id,
            session_id,
            done: true,
            body,
        }
    }

    pub fn stream_chunk(
        request_id: u64,
        session_id: Option<Uuid>,
        chunk: QueryResultChunk,
    ) -> Self {
        Self {
            protocol_version: DRIVER_RPC_VERSION,
            request_id,
            session_id,
            done: chunk.done,
            body: DriverResponseBody::QueryChunk(chunk),
        }
    }

    pub fn error(
        request_id: u64,
        session_id: Option<Uuid>,
        code: DriverRpcErrorCode,
        message: impl Into<String>,
        retriable: bool,
    ) -> Self {
        Self {
            protocol_version: DRIVER_RPC_VERSION,
            request_id,
            session_id,
            done: true,
            body: DriverResponseBody::Error(DriverRpcError {
                code,
                message: message.into(),
                retriable,
            }),
        }
    }
}
