use crate::audit::AuditEventEmitDto;
use crate::envelope::{DRIVER_RPC_V1_6, DRIVER_RPC_V1_7, ProtocolVersion};
use dbflux_core::{
    CodeGenCapabilities, CodeGeneratorInfo, CollectionBrowseRequest, CollectionCountRequest,
    ColumnMeta, CrudResult, CustomTypeInfo, DatabaseInfo, DbSchemaInfo, DescribeRequest,
    DocumentDelete, DocumentInsert, DocumentUpdate, DriverFormDef, DriverMetadata,
    ExecutionContext, ExplainRequest, QueryRequest, QueryResult, QueryResultShape, RowDelete,
    RowInsert, RowPatch, SchemaColumnInfo, SchemaFeatures, SchemaForeignKeyInfo, SchemaIndexInfo,
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
    /// Driver supports emitting audit events as intermediate response frames.
    /// Requires protocol version >= 1.2.
    AuditEmit,
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
            // `confirmed_ceiling` deliberately has no wire representation on
            // this DTO (see `QueryRequest::confirmed_ceiling`'s invariant) —
            // an RPC-backed driver always sees the restrictive `None`
            // default rather than a value that crossed a process boundary.
            // `read_only` has none either: `IpcConnection` refuses a request
            // that requires it before anything is sent.
            ..Default::default()
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
            // Resolved window and metadata_extra are not part of the IPC DTO; drivers set them locally.
            resolved_window: None,
            metadata_extra: None,
            // A single DTO is one result set; additional sets travel in
            // `QueryResultSetsDto` from v1.7.
            additional_results: Vec::new(),
        }
    }
}

/// One result set together with the truncation flag `QueryResultDto` has no
/// room for.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultSetDto {
    pub result: QueryResultDto,
    pub rows_truncated: bool,
}

impl From<&QueryResult> for ResultSetDto {
    fn from(value: &QueryResult) -> Self {
        Self {
            result: QueryResultDto::from(value),
            rows_truncated: value.rows_truncated(),
        }
    }
}

impl From<ResultSetDto> for QueryResult {
    fn from(value: ResultSetDto) -> Self {
        let mut result = QueryResult::from(value.result);
        result.set_rows_truncated(value.rows_truncated);
        result
    }
}

/// Every result set of a query, execute or browse answer (v1.7+): the primary
/// set first, then the additional ones in the order the driver returned them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResultSetsDto {
    pub primary: ResultSetDto,
    pub additional: Vec<ResultSetDto>,
}

impl From<&QueryResult> for QueryResultSetsDto {
    fn from(value: &QueryResult) -> Self {
        Self {
            primary: ResultSetDto::from(value),
            additional: value
                .additional_results
                .iter()
                .map(ResultSetDto::from)
                .collect(),
        }
    }
}

impl From<QueryResultSetsDto> for QueryResult {
    fn from(value: QueryResultSetsDto) -> Self {
        let mut result = QueryResult::from(value.primary);
        for additional in value.additional {
            result.push_additional_result(additional.into());
        }
        result
    }
}

/// Whether `Execute`, `ExecuteWithHandle` and the browse requests are answered
/// with every result set (`ExecuteResultSets`, `ExecuteWithHandleResultSets`,
/// `BrowseResultSets`) on a session that negotiated `version`. Older peers
/// cannot decode those variants and receive only the primary result set.
pub fn protocol_carries_result_sets(version: ProtocolVersion) -> bool {
    version.major > DRIVER_RPC_V1_7.major
        || (version.major == DRIVER_RPC_V1_7.major && version.minor >= DRIVER_RPC_V1_7.minor)
}

/// Whether `Execute` and `ExecuteWithHandle` may carry `QueryRequestDto::limit`
/// on a session that negotiated `version`.
///
/// From v1.6 a host whose driver declares `DriverCapabilities::REQUEST_ROW_LIMIT`
/// accepts the limit and answers with `ExecuteResultWithTruncation` /
/// `ExecuteWithHandleResultWithTruncation`. Older peers cannot report that a
/// result set dropped rows, so neither side sends nor accepts a limit with them.
pub fn protocol_carries_request_row_limit(version: ProtocolVersion) -> bool {
    version.major > DRIVER_RPC_V1_6.major
        || (version.major == DRIVER_RPC_V1_6.major && version.minor >= DRIVER_RPC_V1_6.minor)
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
    // Appended last on purpose: the wire encodes enum variants as a varint
    // discriminant index (see framing.rs, postcard), so a new variant is
    // appended. Inserting it mid-enum would shift every later variant's index
    // for any peer that negotiated an older minor and desynchronise the stream.
    SchemaColumns {
        database: String,
        schema: Option<String>,
    },
    // Appended after the last v1.4 variant for the same wire-index reason.
    KvKeyCount {
        keyspace: Option<u32>,
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
    // === Audit emission (intermediate frame, done=false) ===
    /// Emitted by drivers that advertise `DriverCapability::AuditEmit`.
    /// Always arrives with `done=false`; the host intercepts it and never
    /// forwards it to the caller of `RpcClient::call`.
    EmitAuditEvent(AuditEventEmitDto),
    // === Error ===
    Error(DriverRpcError),
    // Appended last on purpose: the wire encodes enum variants as a varint
    // discriminant index (see framing.rs, postcard), so a new variant is
    // appended. Inserting it mid-enum would shift every later variant's index
    // for any peer that negotiated an older minor and desynchronise the stream.
    SchemaColumns {
        columns: Vec<SchemaColumnInfo>,
    },
    // Appended after the last v1.4 variant for the same wire-index reason.
    KvKeyCountResult {
        count: u64,
    },
    // v1.6: `QueryResultDto` cannot change shape, so the truncation flag rides
    // on new variants appended after the last v1.5 variant. A host sends them
    // only on sessions that negotiated v1.6 or newer.
    ExecuteResultWithTruncation {
        result: QueryResultDto,
        rows_truncated: bool,
    },
    ExecuteWithHandleResultWithTruncation {
        handle_id: Uuid,
        result: QueryResultDto,
        rows_truncated: bool,
    },
    // v1.7: every result set with its own truncation flag, appended after the
    // last v1.6 variant. A host sends them only on sessions that negotiated
    // v1.7 or newer.
    ExecuteResultSets {
        results: QueryResultSetsDto,
    },
    ExecuteWithHandleResultSets {
        handle_id: Uuid,
        results: QueryResultSetsDto,
    },
    BrowseResultSets {
        results: QueryResultSetsDto,
    },
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
        DriverRpcError, DriverRpcErrorCode, QueryRequestDto, QueryResultDto, QueryResultSetsDto,
        QueryResultShapeDto, ResultSetDto, protocol_carries_request_row_limit,
        protocol_carries_result_sets,
    };
    use crate::ProtocolVersion;
    use dbflux_core::{
        ExecutionContext, ExecutionSourceContext, KeyGetRequest, KeyGetResult, QueryRequest,
        QueryResult,
    };
    use std::time::Duration;
    use uuid::Uuid;

    /// `TableBrowseRequest` and `TableCountRequest` cross the driver RPC wire,
    /// and `framing.rs` encodes every frame with postcard, which is positional.
    /// A field that is skipped is absent from the bytes while the decoder still
    /// consumes the next field's bytes as that field, shifting everything after
    /// it, so a field that is only read in-process has to be skipped on both
    /// sides of the encoding. Then a request carrying it encodes exactly like
    /// one that does not and every other field keeps its place.
    #[test]
    fn browse_and_count_database_never_shifts_the_wire_layout() {
        use dbflux_core::{
            Pagination, SemanticFilter, TableBrowseRequest, TableCountRequest, TableRef, Value,
            WhereOperator,
        };

        let filter = SemanticFilter::compare("id", WhereOperator::Eq, Value::Int(1));

        // The shape an external driver RPC actually sends for a browse: no
        // projection at all. Only the encoded bytes are compared here, because
        // an empty projection has a pre-existing trailing-skip hazard of its
        // own that the last assertion in this test does not touch.
        let bare = TableBrowseRequest::new(TableRef::with_schema("dbo", "events"));
        let bare_scoped = bare.clone().with_database(Some("reporting".to_string()));
        assert_eq!(
            postcard::to_allocvec(&DriverRequestBody::BrowseTable { request: bare })
                .expect("serialize bare browse"),
            postcard::to_allocvec(&DriverRequestBody::BrowseTable {
                request: bare_scoped,
            })
            .expect("serialize scoped bare browse"),
            "the database must be zero-width even with no projection"
        );

        // A count with a string filter and no semantic filter, which is also a
        // pre-existing undecodable shape, so again bytes only.
        let plain_count =
            TableCountRequest::new(TableRef::with_schema("dbo", "events")).with_filter("id = 1");
        let plain_count_scoped = plain_count
            .clone()
            .with_database(Some("reporting".to_string()));
        assert_eq!(
            postcard::to_allocvec(&DriverRequestBody::CountTable {
                request: plain_count,
            })
            .expect("serialize plain count"),
            postcard::to_allocvec(&DriverRequestBody::CountTable {
                request: plain_count_scoped,
            })
            .expect("serialize scoped plain count"),
            "the database must be zero-width with no semantic filter"
        );

        // A projection is part of the fixture because an empty one carries a
        // postcard hazard of its own, independent of this field: `columns` has
        // `skip_serializing_if`, so an empty vector shifts whatever follows it.
        let browse = TableBrowseRequest::new(TableRef::with_schema("dbo", "events"))
            .with_pagination(Pagination::Offset {
                limit: 10,
                offset: 20,
            })
            .with_columns(vec!["id".to_string()])
            .with_semantic_filter(filter.clone());
        let browse_scoped = browse.clone().with_database(Some("reporting".to_string()));

        assert_eq!(
            postcard::to_allocvec(&DriverRequestBody::BrowseTable {
                request: browse.clone(),
            })
            .expect("serialize browse"),
            postcard::to_allocvec(&DriverRequestBody::BrowseTable {
                request: browse_scoped,
            })
            .expect("serialize scoped browse"),
            "a database on the request must not change the encoded bytes"
        );

        let bytes = postcard::to_allocvec(&DriverRequestBody::BrowseTable {
            request: browse.clone(),
        })
        .expect("serialize browse");
        let decoded: DriverRequestBody = postcard::from_bytes(&bytes).expect("decode browse");
        let DriverRequestBody::BrowseTable { request } = decoded else {
            panic!("expected BrowseTable");
        };
        assert_eq!(request.table, browse.table);
        assert_eq!(request.pagination, browse.pagination);
        assert_eq!(request.semantic_filter, browse.semantic_filter);

        let count = TableCountRequest::new(TableRef::with_schema("dbo", "events"))
            .with_filter("id = 1")
            .with_semantic_filter(filter.clone());
        let count_scoped = count.clone().with_database(Some("reporting".to_string()));

        assert_eq!(
            postcard::to_allocvec(&DriverRequestBody::CountTable {
                request: count.clone(),
            })
            .expect("serialize count"),
            postcard::to_allocvec(&DriverRequestBody::CountTable {
                request: count_scoped,
            })
            .expect("serialize scoped count"),
            "a database on the count must not change the encoded bytes"
        );

        let bytes = postcard::to_allocvec(&DriverRequestBody::CountTable {
            request: count.clone(),
        })
        .expect("serialize count");
        let decoded: DriverRequestBody = postcard::from_bytes(&bytes).expect("decode count");
        let DriverRequestBody::CountTable { request } = decoded else {
            panic!("expected CountTable");
        };
        assert_eq!(request.filter, count.filter);
        assert_eq!(
            request.semantic_filter, count.semantic_filter,
            "the count's filter must survive the wire"
        );
    }

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
            ..Default::default()
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

    #[test]
    fn kv_get_key_request_round_trips_max_value_bytes_through_json() {
        let request = DriverRequestBody::KvGetKey {
            request: KeyGetRequest::new("some-key").with_max_value_bytes(4_096),
        };

        let json = serde_json::to_string(&request).expect("serialize");
        let restored: DriverRequestBody = serde_json::from_str(&json).expect("deserialize");

        match restored {
            DriverRequestBody::KvGetKey { request } => {
                assert_eq!(request.max_value_bytes, Some(4_096));
            }
            other => panic!("unexpected request body: {other:?}"),
        }
    }

    #[test]
    fn kv_get_key_request_missing_max_value_bytes_defaults_to_unbounded() {
        // Simulates a request sent by a peer on an older protocol minor
        // that predates the byte-budget field.
        let json = serde_json::json!({
            "KvGetKey": {
                "request": {
                    "key": "some-key",
                    "keyspace": null,
                    "include_type": true,
                    "include_ttl": true,
                    "include_size": true,
                }
            }
        });

        let restored: DriverRequestBody =
            serde_json::from_value(json).expect("deserialize legacy request");

        match restored {
            DriverRequestBody::KvGetKey { request } => {
                assert_eq!(request.max_value_bytes, None);
            }
            other => panic!("unexpected request body: {other:?}"),
        }
    }

    #[test]
    fn kv_get_result_response_round_trips_load_state_through_json() {
        let response = DriverResponseBody::KvGetResult {
            result: KeyGetResult {
                entry: dbflux_core::KeyEntry::new("some-key"),
                value: Vec::new(),
                repr: dbflux_core::ValueRepr::Binary,
                load_state: dbflux_core::KeyLoadState::TooLarge {
                    size_bytes: 5_000,
                    limit_bytes: 1_000,
                },
            },
        };

        let json = serde_json::to_string(&response).expect("serialize");
        let restored: DriverResponseBody = serde_json::from_str(&json).expect("deserialize");

        match restored {
            DriverResponseBody::KvGetResult { result } => {
                assert_eq!(
                    result.load_state,
                    dbflux_core::KeyLoadState::TooLarge {
                        size_bytes: 5_000,
                        limit_bytes: 1_000,
                    }
                );
            }
            other => panic!("unexpected response body: {other:?}"),
        }
    }

    #[test]
    fn schema_columns_request_round_trips_through_json() {
        let request = DriverRequestBody::SchemaColumns {
            database: "analytics".into(),
            schema: Some("public".into()),
        };

        let json = serde_json::to_string(&request).expect("serialize");
        let restored: DriverRequestBody = serde_json::from_str(&json).expect("deserialize");

        match restored {
            DriverRequestBody::SchemaColumns { database, schema } => {
                assert_eq!(database, "analytics");
                assert_eq!(schema.as_deref(), Some("public"));
            }
            other => panic!("unexpected request body: {other:?}"),
        }
    }

    #[test]
    fn schema_columns_response_round_trips_through_json() {
        let response = DriverResponseBody::SchemaColumns {
            columns: vec![dbflux_core::SchemaColumnInfo {
                table_name: "users".into(),
                column: dbflux_core::ColumnInfo {
                    name: "email".into(),
                    type_name: "varchar(255)".into(),
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                    enum_values: None,
                },
            }],
        };

        let json = serde_json::to_string(&response).expect("serialize");
        let restored: DriverResponseBody = serde_json::from_str(&json).expect("deserialize");

        match restored {
            DriverResponseBody::SchemaColumns { columns } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].table_name, "users");
                assert_eq!(columns[0].column.name, "email");
            }
            other => panic!("unexpected response body: {other:?}"),
        }
    }

    /// Decodes the leading LEB128 varint postcard uses to tag enum variants.
    ///
    /// JSON round-trip tests cannot catch discriminant shifts: serde_json tags
    /// variants by name, while the wire format (postcard, see framing.rs) tags
    /// them by declaration-order index. That is why the JSON round-trip tests
    /// passed while the v1.4 enum order broke the wire for v1.3 peers.
    fn wire_variant_index(value: &impl serde::Serialize) -> u64 {
        let bytes = postcard::to_allocvec(value).expect("serialize");
        let mut index = 0u64;
        let mut shift = 0;
        for &byte in &bytes {
            index |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return index;
            }
            shift += 7;
        }
        panic!("varint without terminator");
    }

    fn dummy_table_info() -> dbflux_core::TableInfo {
        dbflux_core::TableInfo {
            name: "t".into(),
            schema: None,
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: Default::default(),
            child_items: None,
            storage_hints: None,
            pseudo_columns: Box::default(),
        }
    }

    /// Fixture shape for the golden byte tests below. Populated fields:
    /// `schema`, two `columns` (one with default and enum values, one with an
    /// empty `enum_values` list), `foreign_keys`, `constraints`, and
    /// `storage_hints`. Deliberately left empty: `indexes` (postcard cannot
    /// decode the adjacently tagged `IndexData` enum), `sample_fields`
    /// (document-database only), and `child_items` (driver child sources).
    fn nontrivial_table_info() -> dbflux_core::TableInfo {
        dbflux_core::TableInfo {
            name: "orders".into(),
            schema: Some("sales".into()),
            columns: Some(vec![
                dbflux_core::ColumnInfo {
                    name: "id".into(),
                    type_name: "int".into(),
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    // NOTE: `enum_values` carries `skip_serializing_if`,
                    // which postcard cannot round-trip (a skipped field
                    // misaligns every later byte on decode). The fixture
                    // keeps it `Some` so the bytes are decodable; real
                    // multi-column payloads with a `None` here have the same
                    // pre-existing decode hazard.
                    enum_values: Some(Vec::new()),
                },
                dbflux_core::ColumnInfo {
                    name: "status".into(),
                    type_name: "varchar(16)".into(),
                    nullable: true,
                    is_primary_key: false,
                    default_value: Some("'new'".into()),
                    enum_values: Some(vec!["new".into(), "paid".into()]),
                },
            ]),
            // `IndexData` is adjacently tagged
            // (`#[serde(tag = "kind", content = "items")]`), which postcard
            // encodes but cannot decode (`WontImplement`), so wire-carried
            // `TableInfo` always leaves `indexes` as `None`.
            indexes: None,
            foreign_keys: Some(vec![dbflux_core::ForeignKeyInfo {
                name: "fk_orders_customer".into(),
                columns: vec!["customer_id".into()],
                referenced_table: "customers".into(),
                referenced_schema: Some("sales".into()),
                referenced_columns: vec!["id".into()],
                on_delete: Some("CASCADE".into()),
                on_update: None,
            }]),
            constraints: Some(vec![dbflux_core::ConstraintInfo {
                name: "ck_orders_total".into(),
                kind: dbflux_core::ConstraintKind::Check,
                columns: vec!["total".into()],
                check_clause: Some("total >= 0".into()),
            }]),
            sample_fields: None,
            presentation: Default::default(),
            child_items: None,
            storage_hints: Some(vec![dbflux_core::TableStorageHint {
                label: "Distribution Key".into(),
                columns: vec!["id".into()],
                detail: Some("KEY".into()),
            }]),
            pseudo_columns: Box::default(),
        }
    }

    #[test]
    fn request_variants_after_the_schema_block_keep_their_v1_3_wire_indices() {
        // These literals are the v1.3 wire indices. Inserting a variant
        // mid-enum shifts every later index; a peer that negotiated the older
        // minor would then decode those variants as the wrong ones and leave
        // the rest of the frame undecoded, desynchronising the stream.
        // SchemaColumns must therefore stay appended after the last v1.3
        // variant (index 56).
        assert_eq!(wire_variant_index(&DriverRequestBody::ActiveDatabase), 31);
        assert_eq!(
            wire_variant_index(&DriverRequestBody::GenerateCode {
                generator_id: "t".to_string(),
                table: dummy_table_info(),
            }),
            55
        );
        assert_eq!(
            wire_variant_index(&DriverRequestBody::SchemaColumns {
                database: "d".to_string(),
                schema: None,
            }),
            56
        );
    }

    #[test]
    fn kv_key_count_is_appended_after_the_last_v1_4_request_variant() {
        assert_eq!(
            wire_variant_index(&DriverRequestBody::KvKeyCount { keyspace: Some(2) }),
            57
        );
    }

    #[test]
    fn kv_key_count_result_is_appended_after_the_last_v1_4_response_variant() {
        assert_eq!(
            wire_variant_index(&DriverResponseBody::KvKeyCountResult { count: 42 }),
            34
        );
    }

    #[test]
    fn kv_key_count_request_and_result_round_trip_through_postcard() {
        let request = DriverRequestBody::KvKeyCount { keyspace: Some(2) };
        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let restored: DriverRequestBody = postcard::from_bytes(&bytes).expect("deserialize");

        match restored {
            DriverRequestBody::KvKeyCount { keyspace } => assert_eq!(keyspace, Some(2)),
            other => panic!("unexpected request body: {other:?}"),
        }

        let response = DriverResponseBody::KvKeyCountResult { count: 1_234_567 };
        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let restored: DriverResponseBody = postcard::from_bytes(&bytes).expect("deserialize");

        match restored {
            DriverResponseBody::KvKeyCountResult { count } => assert_eq!(count, 1_234_567),
            other => panic!("unexpected response body: {other:?}"),
        }
    }

    fn limited_result_dto() -> QueryResultDto {
        QueryResultDto {
            shape: QueryResultShapeDto::Table,
            columns: Vec::new(),
            rows: vec![
                vec![dbflux_core::Value::Int(1)],
                vec![dbflux_core::Value::Int(2)],
            ],
            affected_rows: None,
            execution_time_ms: 7,
            text_body: None,
            raw_bytes: None,
            next_page_token: None,
        }
    }

    #[test]
    fn truncation_results_are_appended_after_the_last_v1_5_response_variant() {
        assert_eq!(
            wire_variant_index(&DriverResponseBody::ExecuteResultWithTruncation {
                result: limited_result_dto(),
                rows_truncated: true,
            }),
            35
        );
        assert_eq!(
            wire_variant_index(&DriverResponseBody::ExecuteWithHandleResultWithTruncation {
                handle_id: Uuid::nil(),
                result: limited_result_dto(),
                rows_truncated: true,
            }),
            36
        );
    }

    #[test]
    fn execute_result_with_truncation_round_trips_through_postcard() {
        for truncated in [true, false] {
            let response = DriverResponseBody::ExecuteResultWithTruncation {
                result: limited_result_dto(),
                rows_truncated: truncated,
            };
            let bytes = postcard::to_allocvec(&response).expect("serialize");
            let restored: DriverResponseBody = postcard::from_bytes(&bytes).expect("deserialize");

            match restored {
                DriverResponseBody::ExecuteResultWithTruncation {
                    result,
                    rows_truncated,
                } => {
                    assert_eq!(rows_truncated, truncated);
                    assert_eq!(result.rows.len(), 2);
                    assert_eq!(result.execution_time_ms, 7);
                }
                other => panic!("unexpected response body: {other:?}"),
            }
        }
    }

    #[test]
    fn execute_with_handle_result_with_truncation_round_trips_through_postcard() {
        let handle = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").expect("uuid");
        let response = DriverResponseBody::ExecuteWithHandleResultWithTruncation {
            handle_id: handle,
            result: limited_result_dto(),
            rows_truncated: true,
        };
        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let restored: DriverResponseBody = postcard::from_bytes(&bytes).expect("deserialize");

        match restored {
            DriverResponseBody::ExecuteWithHandleResultWithTruncation {
                handle_id,
                result,
                rows_truncated,
            } => {
                assert_eq!(handle_id, handle);
                assert!(rows_truncated);
                assert_eq!(result.rows.len(), 2);
            }
            other => panic!("unexpected response body: {other:?}"),
        }
    }

    #[test]
    fn legacy_execute_results_keep_their_wire_indices() {
        assert_eq!(
            wire_variant_index(&DriverResponseBody::ExecuteResult {
                result: limited_result_dto(),
            }),
            4
        );
        assert_eq!(
            wire_variant_index(&DriverResponseBody::ExecuteWithHandleResult {
                handle_id: Uuid::nil(),
                result: limited_result_dto(),
            }),
            5
        );
    }

    fn result_set_dto(label: &str, rows_truncated: bool) -> ResultSetDto {
        let mut result = limited_result_dto();
        result.text_body = Some(label.to_string());
        ResultSetDto {
            result,
            rows_truncated,
        }
    }

    fn result_sets_dto() -> QueryResultSetsDto {
        QueryResultSetsDto {
            primary: result_set_dto("primary", true),
            additional: vec![
                result_set_dto("second", false),
                result_set_dto("third", true),
            ],
        }
    }

    #[test]
    fn result_set_responses_are_appended_after_the_last_v1_6_response_variant() {
        assert_eq!(
            wire_variant_index(&DriverResponseBody::ExecuteResultSets {
                results: result_sets_dto(),
            }),
            37
        );
        assert_eq!(
            wire_variant_index(&DriverResponseBody::ExecuteWithHandleResultSets {
                handle_id: Uuid::nil(),
                results: result_sets_dto(),
            }),
            38
        );
        assert_eq!(
            wire_variant_index(&DriverResponseBody::BrowseResultSets {
                results: result_sets_dto(),
            }),
            39
        );
    }

    #[test]
    fn result_set_responses_round_trip_through_postcard() {
        let handle = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").expect("uuid");
        let responses = [
            DriverResponseBody::ExecuteResultSets {
                results: result_sets_dto(),
            },
            DriverResponseBody::ExecuteWithHandleResultSets {
                handle_id: handle,
                results: result_sets_dto(),
            },
            DriverResponseBody::BrowseResultSets {
                results: result_sets_dto(),
            },
        ];

        for response in responses {
            let bytes = postcard::to_allocvec(&response).expect("serialize");
            let restored: DriverResponseBody = postcard::from_bytes(&bytes).expect("deserialize");

            let results = match restored {
                DriverResponseBody::ExecuteResultSets { results }
                | DriverResponseBody::BrowseResultSets { results } => results,
                DriverResponseBody::ExecuteWithHandleResultSets { handle_id, results } => {
                    assert_eq!(handle_id, handle);
                    results
                }
                other => panic!("unexpected response body: {other:?}"),
            };

            let labels: Vec<(Option<String>, bool)> = std::iter::once(&results.primary)
                .chain(&results.additional)
                .map(|set| (set.result.text_body.clone(), set.rows_truncated))
                .collect();
            assert_eq!(
                labels,
                vec![
                    (Some("primary".to_string()), true),
                    (Some("second".to_string()), false),
                    (Some("third".to_string()), true),
                ]
            );
            assert_eq!(results.primary.result.rows.len(), 2);
        }
    }

    #[test]
    fn result_sets_carry_every_set_and_its_truncation_flag() {
        let mut primary = QueryResult::text("primary".to_string(), Duration::from_millis(3));
        primary.set_rows_truncated(true);
        let second = QueryResult::text("second".to_string(), Duration::from_millis(4));
        let mut third = QueryResult::text("third".to_string(), Duration::from_millis(5));
        third.set_rows_truncated(true);
        primary.push_additional_result(second);
        primary.push_additional_result(third);

        let dto = QueryResultSetsDto::from(&primary);
        assert!(dto.primary.rows_truncated);
        assert!(dto.primary.result.text_body.as_deref() == Some("primary"));
        assert_eq!(dto.additional.len(), 2);

        let bytes = postcard::to_allocvec(&dto).expect("serialize");
        let restored: QueryResult = postcard::from_bytes::<QueryResultSetsDto>(&bytes)
            .expect("deserialize")
            .into();

        let sets: Vec<(Option<&str>, bool, Duration)> = restored
            .iter_result_sets()
            .map(|set| {
                (
                    set.text_body.as_deref(),
                    set.rows_truncated(),
                    set.execution_time,
                )
            })
            .collect();
        assert_eq!(
            sets,
            vec![
                (Some("primary"), true, Duration::from_millis(3)),
                (Some("second"), false, Duration::from_millis(4)),
                (Some("third"), true, Duration::from_millis(5)),
            ]
        );
        assert!(
            restored
                .additional_results
                .iter()
                .all(|set| set.additional_results.is_empty())
        );
    }

    #[test]
    fn result_sets_travel_only_from_v1_7() {
        assert!(!protocol_carries_result_sets(ProtocolVersion::new(1, 0)));
        assert!(!protocol_carries_result_sets(ProtocolVersion::new(1, 6)));
        assert!(protocol_carries_result_sets(ProtocolVersion::new(1, 7)));
        assert!(protocol_carries_result_sets(ProtocolVersion::new(1, 8)));
        assert!(protocol_carries_result_sets(ProtocolVersion::new(2, 0)));
    }

    #[test]
    fn request_row_limit_travels_only_from_v1_6() {
        assert!(!protocol_carries_request_row_limit(ProtocolVersion::new(
            1, 0
        )));
        assert!(!protocol_carries_request_row_limit(ProtocolVersion::new(
            1, 5
        )));
        assert!(protocol_carries_request_row_limit(ProtocolVersion::new(
            1, 6
        )));
        assert!(protocol_carries_request_row_limit(ProtocolVersion::new(
            1, 7
        )));
        assert!(protocol_carries_request_row_limit(ProtocolVersion::new(
            2, 0
        )));
    }

    #[test]
    fn response_variants_after_the_schema_block_keep_their_v1_3_wire_indices() {
        // Same contract as the request enum: these literals are the v1.3 wire
        // indices, and SchemaColumns must stay appended after the last v1.3
        // variant (index 33).
        assert_eq!(
            wire_variant_index(&DriverResponseBody::ActiveDatabaseResult { database: None }),
            22
        );
        assert_eq!(
            wire_variant_index(&DriverResponseBody::Error(DriverRpcError {
                code: DriverRpcErrorCode::Driver,
                message: "m".into(),
                retriable: false,
            })),
            32
        );
        assert_eq!(
            wire_variant_index(&DriverResponseBody::SchemaColumns {
                columns: Vec::new()
            }),
            33
        );
    }

    #[test]
    fn kv_get_result_response_missing_load_state_defaults_to_loaded() {
        // Simulates a response sent by a peer on an older protocol minor
        // that predates the load-state field.
        let json = serde_json::json!({
            "KvGetResult": {
                "result": {
                    "entry": {
                        "key": "some-key",
                        "key_type": null,
                        "ttl_seconds": null,
                        "size_bytes": null,
                    },
                    "value": [],
                    "repr": "Binary",
                }
            }
        });

        let restored: DriverResponseBody =
            serde_json::from_value(json).expect("deserialize legacy response");

        match restored {
            DriverResponseBody::KvGetResult { result } => {
                assert_eq!(result.load_state, dbflux_core::KeyLoadState::Loaded);
            }
            other => panic!("unexpected response body: {other:?}"),
        }
    }

    /// Decodes a hex string into bytes for the golden fixtures below.
    fn hex_bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
            .collect()
    }

    /// Golden postcard bytes for `DriverRequestBody::GenerateCode`, captured
    /// from the unchanged v1.4 wire shapes before the creation-metadata work.
    ///
    /// Scope note: these fixtures freeze exactly the two payloads they encode.
    /// They prove the `TableInfo` legacy shape and the `GenerateCode` /
    /// `TableDetails` discriminants are byte-stable; they do not cover every
    /// historical protocol version or every payload variant.
    const GOLDEN_GENERATE_CODE_BYTES: &str = "370c6372656174655f7461626c65066f7264657273010573616c6573010202696403696e740001000100067374617475730b766172636861722831362901000105276e6577270102036e6577047061696400010112666b5f6f72646572735f637573746f6d6572010b637573746f6d65725f696409637573746f6d657273010573616c6573010269640107434153434144450001010f636b5f6f72646572735f746f74616c000105746f74616c010a746f74616c203e3d2030000000010110446973747269627574696f6e204b65790102696401034b4559";

    /// Golden postcard bytes for `DriverResponseBody::TableDetails`, same
    /// capture origin and scope as [`GOLDEN_GENERATE_CODE_BYTES`].
    const GOLDEN_TABLE_DETAILS_BYTES: &str = "0c066f7264657273010573616c6573010202696403696e740001000100067374617475730b766172636861722831362901000105276e6577270102036e6577047061696400010112666b5f6f72646572735f637573746f6d6572010b637573746f6d65725f696409637573746f6d657273010573616c6573010269640107434153434144450001010f636b5f6f72646572735f746f74616c000105746f74616c010a746f74616c203e3d2030000000010110446973747269627574696f6e204b65790102696401034b4559";

    #[test]
    fn generate_code_request_encodes_to_frozen_legacy_bytes() {
        let request = DriverRequestBody::GenerateCode {
            generator_id: "create_table".to_string(),
            table: nontrivial_table_info(),
        };

        let encoded = postcard::to_allocvec(&request).expect("serialize");
        assert_eq!(
            encoded,
            hex_bytes(GOLDEN_GENERATE_CODE_BYTES),
            "GenerateCode wire encoding drifted from the captured legacy bytes"
        );
    }

    #[test]
    fn generate_code_request_frozen_bytes_decode_to_expected_value() {
        let bytes = hex_bytes(GOLDEN_GENERATE_CODE_BYTES);
        let decoded: DriverRequestBody = postcard::from_bytes(&bytes).expect("decode frozen bytes");

        match decoded {
            DriverRequestBody::GenerateCode {
                generator_id,
                table,
            } => {
                assert_eq!(generator_id, "create_table");
                assert_decode_matches_fixture_shape(&table);
            }
            other => panic!("frozen bytes decode to unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn table_details_response_encodes_to_frozen_legacy_bytes() {
        let response = DriverResponseBody::TableDetails {
            table: nontrivial_table_info(),
        };

        let encoded = postcard::to_allocvec(&response).expect("serialize");
        assert_eq!(
            encoded,
            hex_bytes(GOLDEN_TABLE_DETAILS_BYTES),
            "TableDetails wire encoding drifted from the captured legacy bytes"
        );
    }

    #[test]
    fn pseudo_columns_never_reach_the_wire() {
        let mut table = nontrivial_table_info();
        table.pseudo_columns = Box::new(["rowid".into(), "ctid".into()]);

        let encoded =
            postcard::to_allocvec(&DriverResponseBody::TableDetails { table }).expect("serialize");
        assert_eq!(
            encoded,
            hex_bytes(GOLDEN_TABLE_DETAILS_BYTES),
            "declaring pseudo-columns changed the TableDetails wire encoding"
        );

        let decoded: DriverResponseBody = postcard::from_bytes(&encoded).expect("deserialize");
        match decoded {
            DriverResponseBody::TableDetails { table } => assert!(table.pseudo_columns.is_empty()),
            other => panic!("unexpected response body: {other:?}"),
        }
    }

    #[test]
    fn table_details_response_frozen_bytes_decode_to_expected_value() {
        let bytes = hex_bytes(GOLDEN_TABLE_DETAILS_BYTES);
        let decoded: DriverResponseBody =
            postcard::from_bytes(&bytes).expect("decode frozen bytes");

        match decoded {
            DriverResponseBody::TableDetails { table } => {
                assert_decode_matches_fixture_shape(&table);
            }
            other => panic!("frozen bytes decode to unexpected variant: {other:?}"),
        }
    }

    /// Field-by-field comparison against the fixture shape; `TableInfo` does
    /// not implement `PartialEq` and must stay unchanged.
    fn assert_decode_matches_fixture_shape(table: &dbflux_core::TableInfo) {
        assert_eq!(table.name, "orders");
        assert_eq!(table.schema.as_deref(), Some("sales"));
        let columns = table.columns.as_ref().expect("columns decoded");
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert!(columns[0].is_primary_key);
        assert_eq!(columns[1].default_value.as_deref(), Some("'new'"));
        assert_eq!(
            columns[1].enum_values.as_deref(),
            Some(["new".to_string(), "paid".to_string()].as_slice())
        );
        let fks = table.foreign_keys.as_ref().expect("fks decoded");
        assert_eq!(fks.len(), 1);
        assert_eq!(fks[0].referenced_table, "customers");
        assert_eq!(fks[0].on_delete.as_deref(), Some("CASCADE"));
        let constraints = table.constraints.as_ref().expect("constraints decoded");
        assert_eq!(constraints[0].check_clause.as_deref(), Some("total >= 0"));
        let hints = table.storage_hints.as_ref().expect("hints decoded");
        assert_eq!(hints[0].label, "Distribution Key");
    }
}
