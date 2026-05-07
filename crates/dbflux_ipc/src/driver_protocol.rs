use crate::envelope::ProtocolVersion;
use dbflux_core::{
    CodeGenCapabilities, CodeGeneratorInfo, CollectionBrowseRequest, CollectionCountRequest,
    ColumnMeta, CrudResult, CustomTypeInfo, DatabaseInfo, DbSchemaInfo, DescribeRequest,
    DocumentDelete, DocumentInsert, DocumentUpdate, DriverFormDef, DriverMetadata,
    ExecutionContext, ExplainRequest, QueryRequest, QueryResult, QueryResultShape, RowDelete,
    RowInsert, RowPatch, SchemaFeatures, SchemaForeignKeyInfo, SchemaIndexInfo,
    SchemaLoadingStrategy, SchemaSnapshot, SemanticPlan, SemanticRequest, TableBrowseRequest,
    TableCountRequest, TableInfo, Value, ViewInfo,
};
use dbflux_core::{
    HashDeleteRequest, HashSetRequest, KeyBulkGetRequest, KeyDeleteRequest, KeyExistsRequest,
    KeyExpireRequest, KeyGetRequest, KeyGetResult, KeyPersistRequest, KeyRenameRequest,
    KeyScanPage, KeyScanRequest, KeySetRequest, KeyTtlRequest, KeyTypeRequest, ListPushRequest,
    ListRemoveRequest, ListSetRequest, SetAddRequest, SetRemoveRequest, StreamAddRequest,
    StreamDeleteRequest, ZSetAddRequest, ZSetRemoveRequest,
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
    pub execution_context: Option<ExecutionContext>,
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
            execution_context: value.execution_context.clone(),
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
            execution_context: value.execution_context,
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
    pub next_page_token: Option<String>,
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
            next_page_token: value.next_page_token.clone(),
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
            next_page_token: value.next_page_token,
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
    #[serde(default)]
    pub auth_token: Option<String>,
}

/// Handshake response sent by driver hosts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverHelloResponse {
    pub server_name: String,
    pub server_version: String,
    pub selected_version: ProtocolVersion,
    pub capabilities: Vec<DriverCapability>,
    pub driver_kind: dbflux_core::DbKind,
    pub driver_metadata: DriverMetadata,
    pub form_definition: DriverFormDef,
    #[serde(default)]
    pub settings_schema: Option<DriverFormDef>,
}

/// Request body for a single driver RPC call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DriverRequestBody {
    Hello(DriverHelloRequest),
    OpenSession {
        profile_json: String,
        password: Option<String>,
        ssh_secret: Option<String>,
    },
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
    // === Browse operations ===
    BrowseTable {
        request: TableBrowseRequest,
    },
    CountTable {
        request: TableCountRequest,
    },
    BrowseCollection {
        request: CollectionBrowseRequest,
    },
    CountCollection {
        request: CollectionCountRequest,
    },
    Explain {
        request: ExplainRequest,
    },
    DescribeTable {
        request: DescribeRequest,
    },
    PlanSemantic {
        request: SemanticRequest,
    },
    // === CRUD operations ===
    UpdateRow {
        patch: RowPatch,
    },
    InsertRow {
        insert: RowInsert,
    },
    DeleteRow {
        delete: RowDelete,
    },
    // === Document mutations ===
    UpdateDocument {
        update: DocumentUpdate,
    },
    InsertDocument {
        insert: DocumentInsert,
    },
    DeleteDocument {
        delete: DocumentDelete,
    },
    // === Schema extras ===
    SchemaTypes {
        database: String,
        schema: Option<String>,
    },
    SchemaIndexes {
        database: String,
        schema: Option<String>,
    },
    SchemaForeignKeys {
        database: String,
        schema: Option<String>,
    },
    ActiveDatabase,
    // === Key-Value operations ===
    KvScanKeys {
        request: KeyScanRequest,
    },
    KvGetKey {
        request: KeyGetRequest,
    },
    KvSetKey {
        request: KeySetRequest,
    },
    KvDeleteKey {
        request: KeyDeleteRequest,
    },
    KvExistsKey {
        request: KeyExistsRequest,
    },
    KvKeyType {
        request: KeyTypeRequest,
    },
    KvKeyTtl {
        request: KeyTtlRequest,
    },
    KvExpireKey {
        request: KeyExpireRequest,
    },
    KvPersistKey {
        request: KeyPersistRequest,
    },
    KvRenameKey {
        request: KeyRenameRequest,
    },
    KvBulkGet {
        request: KeyBulkGetRequest,
    },
    KvHashSet {
        request: HashSetRequest,
    },
    KvHashDelete {
        request: HashDeleteRequest,
    },
    KvListSet {
        request: ListSetRequest,
    },
    KvListPush {
        request: ListPushRequest,
    },
    KvListRemove {
        request: ListRemoveRequest,
    },
    KvSetAdd {
        request: SetAddRequest,
    },
    KvSetRemove {
        request: SetRemoveRequest,
    },
    KvZSetAdd {
        request: ZSetAddRequest,
    },
    KvZSetRemove {
        request: ZSetRemoveRequest,
    },
    KvStreamAdd {
        request: StreamAddRequest,
    },
    KvStreamDelete {
        request: StreamDeleteRequest,
    },
    // === Code generation ===
    CodeGenerators,
    GenerateCode {
        generator_id: String,
        table: TableInfo,
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
    pub fn new(
        protocol_version: ProtocolVersion,
        request_id: u64,
        body: DriverRequestBody,
    ) -> Self {
        Self {
            protocol_version,
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
        kind: dbflux_core::DbKind,
        metadata: DriverMetadata,
        schema_loading_strategy: SchemaLoadingStrategy,
        schema_features: SchemaFeatures,
        code_gen_capabilities: CodeGenCapabilities,
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
    // === Browse results ===
    BrowseResult {
        result: QueryResultDto,
    },
    CountResult {
        count: u64,
    },
    SemanticPlan {
        plan: SemanticPlan,
    },
    // === CRUD results ===
    CrudResult {
        result: CrudResult,
    },
    // === Document results ===
    // Same as CrudResult, no separate variant needed
    // === Schema extras ===
    SchemaTypes {
        types: Vec<CustomTypeInfo>,
    },
    SchemaIndexes {
        indexes: Vec<SchemaIndexInfo>,
    },
    SchemaForeignKeys {
        foreign_keys: Vec<SchemaForeignKeyInfo>,
    },
    ActiveDatabaseResult {
        database: Option<String>,
    },
    // === Key-Value results ===
    KvScanResult {
        page: KeyScanPage,
    },
    KvGetResult {
        result: KeyGetResult,
    },
    KvBoolResult {
        value: bool,
    },
    KvStringResult {
        value: String,
    },
    KvU64Result {
        value: u64,
    },
    KvBulkGetResult {
        results: Vec<Option<KeyGetResult>>,
    },
    // === Code generation results ===
    CodeGeneratorsResult {
        generators: Vec<CodeGeneratorInfo>,
    },
    GenerateCodeResult {
        code: String,
    },
    // === Error ===
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
    pub fn ok(
        protocol_version: ProtocolVersion,
        request_id: u64,
        session_id: Option<Uuid>,
        body: DriverResponseBody,
    ) -> Self {
        Self {
            protocol_version,
            request_id,
            session_id,
            done: true,
            body,
        }
    }

    pub fn stream_chunk(
        protocol_version: ProtocolVersion,
        request_id: u64,
        session_id: Option<Uuid>,
        chunk: QueryResultChunk,
    ) -> Self {
        Self {
            protocol_version,
            request_id,
            session_id,
            done: chunk.done,
            body: DriverResponseBody::QueryChunk(chunk),
        }
    }

    pub fn error(
        protocol_version: ProtocolVersion,
        request_id: u64,
        session_id: Option<Uuid>,
        code: DriverRpcErrorCode,
        message: impl Into<String>,
        retriable: bool,
    ) -> Self {
        Self {
            protocol_version,
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

#[cfg(test)]
mod tests {
    use super::{
        DriverRequestBody, DriverRequestEnvelope, DriverResponseBody, DriverResponseEnvelope,
        QueryRequestDto,
    };
    use crate::ProtocolVersion;
    use dbflux_core::{ExecutionContext, ExecutionSourceContext, QueryRequest};
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn request_envelope_uses_explicit_protocol_version() {
        let envelope =
            DriverRequestEnvelope::new(ProtocolVersion::new(1, 0), 41, DriverRequestBody::Ping);

        assert_eq!(envelope.protocol_version, ProtocolVersion::new(1, 0));
        assert_eq!(envelope.request_id, 41);
    }

    #[test]
    fn response_envelope_uses_explicit_protocol_version() {
        let response = DriverResponseEnvelope::ok(
            ProtocolVersion::new(1, 0),
            41,
            None,
            DriverResponseBody::Pong,
        );

        assert_eq!(response.protocol_version, ProtocolVersion::new(1, 0));
        assert_eq!(response.request_id, 41);
    }

    #[test]
    fn query_request_dto_roundtrips_execution_context() {
        let request = QueryRequest {
            sql: "SELECT * FROM logs".into(),
            params: Vec::new(),
            limit: Some(250),
            offset: Some(5),
            statement_timeout: Some(Duration::from_secs(30)),
            database: Some("analytics".into()),
            execution_context: Some(ExecutionContext {
                connection_id: Some(
                    Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
                ),
                database: Some("analytics".into()),
                schema: Some("public".into()),
                container: None,
                source: Some(ExecutionSourceContext::CollectionWindow {
                    targets: vec!["/aws/lambda/app".into(), "/aws/ecs/api".into()],
                    start_ms: 1_710_000_000_000,
                    end_ms: 1_710_000_300_000,
                    query_mode: Some("cwli".into()),
                }),
            }),
        };

        let dto = QueryRequestDto::from(&request);
        let restored = QueryRequest::from(dto.clone());

        assert_eq!(dto.database.as_deref(), Some("analytics"));
        assert_eq!(restored.database.as_deref(), Some("analytics"));

        match restored.execution_context {
            Some(ExecutionContext {
                source:
                    Some(ExecutionSourceContext::CollectionWindow {
                        targets,
                        start_ms,
                        end_ms,
                        query_mode,
                    }),
                ..
            }) => {
                assert_eq!(targets, vec!["/aws/lambda/app", "/aws/ecs/api"]);
                assert_eq!(start_ms, 1_710_000_000_000);
                assert_eq!(end_ms, 1_710_000_300_000);
                assert_eq!(query_mode.as_deref(), Some("cwli"));
            }
            other => panic!("unexpected execution context: {other:?}"),
        }
    }
}
