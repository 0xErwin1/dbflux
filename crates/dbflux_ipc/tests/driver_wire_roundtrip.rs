//! Postcard wire-format regression tests for the driver RPC protocol.
//!
//! `postcard` is not self-describing: enum variants are varint discriminants
//! and struct fields are positional. A field skipped by
//! `#[serde(skip_serializing_if = ...)]` therefore does not merely shorten the
//! frame — it shifts every following byte, so a peer either fails to decode or
//! silently misreads the payload. Separately, `serde_json::Value` deserializes
//! exclusively through `deserialize_any`, which postcard rejects, so raw
//! `Value` payloads can be written but never read back.
//!
//! These tests round-trip every request and response variant with the
//! version-1.8 envelope and assert that the decoded value re-encodes to the
//! exact bytes it came from, which catches both truncation and field
//! omission. Targeted tests cover the payload shapes that regressed: JSON documents
//! carried as `JsonText`, and every formerly-skipped field on a wire-crossing
//! type.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::time::Duration;

use dbflux_core::{
    CodeGenCapabilities, CodeGenScope, CodeGeneratorInfo, CollectionBrowseRequest,
    CollectionCountRequest, CollectionRef, ColumnInfo, ColumnKind, ColumnMeta, CrudResult,
    CustomTypeInfo, CustomTypeKind, DatabaseCategory, DatabaseInfo, DbKind, DbSchemaInfo,
    DescribeRequest, DocumentDelete, DocumentFilter, DocumentInsert, DocumentUpdate, DriverFormDef,
    DriverMetadata, DriverMetadataBuilder, ExecutionContext, ExecutionSourceContext,
    ExplainRequest, FormSection, FormTab, HashDeleteRequest, HashSetRequest, KeyBulkGetRequest,
    KeyDeleteRequest, KeyEntry, KeyExistsRequest, KeyExpireRequest, KeyGetRequest, KeyGetResult,
    KeyLoadState, KeyPersistRequest, KeyRenameRequest, KeyScanPage, KeyScanRequest, KeySetRequest,
    KeyTtlRequest, KeyType, KeyTypeRequest, ListEnd, ListPushRequest, ListRemoveRequest,
    ListSetRequest, PlannedQuery, QueryLanguage, QueryRequest, QueryResult, RelationalSchema,
    RowDelete, RowIdentity, RowInsert, RowPatch, SchemaColumnInfo, SchemaFeatures,
    SchemaForeignKeyInfo, SchemaIndexInfo, SchemaLoadingStrategy, SchemaSnapshot, SemanticPlan,
    SemanticPlanKind, SemanticRequest, SetAddRequest, SetRemoveRequest, StreamAddRequest,
    StreamDeleteRequest, StreamEntryId, StreamMaxLen, TableBrowseRequest, TableCountRequest,
    TableInfo, TableRef, Value, ValueRepr, ViewInfo, ZSetAddRequest, ZSetRemoveRequest,
};
use dbflux_ipc::driver_protocol::{
    CollectionBrowseRequestDto, CollectionCountRequestDto, CrudResultDto, DocumentDeleteDto,
    DocumentInsertDto, DocumentUpdateDto, JsonText,
};
use dbflux_ipc::{
    AuditEventEmitDto, DRIVER_RPC_VERSION, DriverCapability, DriverHelloRequest,
    DriverHelloResponse, DriverRequestBody, DriverRequestEnvelope, DriverResponseBody,
    DriverResponseEnvelope, DriverRpcError, DriverRpcErrorCode, EventCategoryDto, EventOutcomeDto,
    EventSeverityDto, ProtocolVersion, QueryRequestDto, QueryResultChunk, QueryResultDto,
    QueryResultSetsDto, ResultSetDto,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use uuid::Uuid;

/// Encodes `value`, decodes it back, and re-encodes the decoded value.
///
/// `postcard` is positional, so a field that serializes on encode but not on
/// re-encode (or vice versa) changes the frame; equal bytes prove the frame
/// carried every field both ways.
fn assert_postcard_roundtrip<T>(value: &T)
where
    T: Serialize + DeserializeOwned,
{
    let encoded = postcard::to_allocvec(value).expect("encode");
    let decoded: T = postcard::from_bytes(&encoded).expect("decode");
    let reencoded = postcard::to_allocvec(&decoded).expect("re-encode");
    assert_eq!(encoded, reencoded, "frame drifted after decode/re-encode");
}

/// [`assert_postcard_roundtrip`] with the fixture name in every failure.
fn assert_postcard_roundtrip_named<T>(name: &str, value: &T)
where
    T: Serialize + DeserializeOwned,
{
    let encoded =
        postcard::to_allocvec(value).unwrap_or_else(|error| panic!("{name}: encode: {error}"));
    let decoded: T =
        postcard::from_bytes(&encoded).unwrap_or_else(|error| panic!("{name}: decode: {error}"));
    let reencoded = postcard::to_allocvec(&decoded)
        .unwrap_or_else(|error| panic!("{name}: re-encode: {error}"));
    assert_eq!(
        encoded, reencoded,
        "{name}: frame drifted after decode/re-encode"
    );
}

fn session_id() -> Uuid {
    Uuid::from_u128(7)
}

fn request_envelope(body: DriverRequestBody) -> DriverRequestEnvelope {
    DriverRequestEnvelope::new(DRIVER_RPC_VERSION, 41, body)
        .with_session(session_id())
        .with_timeout_ms(30_000)
}

fn response_envelope(body: DriverResponseBody) -> DriverResponseEnvelope {
    DriverResponseEnvelope::ok(DRIVER_RPC_VERSION, 41, Some(session_id()), body)
}

fn driver_metadata() -> DriverMetadata {
    DriverMetadataBuilder::new(
        "sqlite",
        "SQLite",
        DatabaseCategory::Relational,
        QueryLanguage::Sql,
    )
    .description("Embedded SQL database")
    .uri_scheme("sqlite")
    .build()
}

fn hello_request() -> DriverHelloRequest {
    DriverHelloRequest {
        client_name: "dbflux".to_string(),
        client_version: "0.9.0-dev.0".to_string(),
        supported_versions: vec![DRIVER_RPC_VERSION, ProtocolVersion::new(1, 5)],
        requested_capabilities: vec![DriverCapability::Cancellation, DriverCapability::AuditEmit],
        auth_token: Some("auth-token".to_string()),
    }
}

fn hello_response() -> DriverHelloResponse {
    DriverHelloResponse {
        server_name: "dbflux-driver".to_string(),
        server_version: "0.9.0-dev.0".to_string(),
        selected_version: DRIVER_RPC_VERSION,
        capabilities: vec![
            DriverCapability::Cancellation,
            DriverCapability::ChunkedResults,
        ],
        driver_kind: DbKind::SQLite,
        driver_metadata: driver_metadata(),
        form_definition: DriverFormDef { tabs: vec![] },
        settings_schema: None,
    }
}

fn query_request_dto() -> QueryRequestDto {
    QueryRequestDto::from(&QueryRequest::new("SELECT 1"))
}

fn query_result_dto() -> QueryResultDto {
    let result = QueryResult::table(
        vec![ColumnMeta {
            name: "id".to_string(),
            type_name: "int4".to_string(),
            kind: ColumnKind::Integer,
            nullable: false,
            is_primary_key: true,
        }],
        vec![vec![Value::Int(1)], vec![Value::Null]],
        Some(2),
        Duration::from_millis(3),
    );
    QueryResultDto::from(&result)
}

fn query_result_sets_dto() -> QueryResultSetsDto {
    QueryResultSetsDto {
        primary: ResultSetDto {
            result: query_result_dto(),
            rows_truncated: true,
        },
        additional: vec![ResultSetDto {
            result: query_result_dto(),
            rows_truncated: false,
        }],
    }
}

fn dummy_table_info() -> TableInfo {
    TableInfo {
        name: "users".to_string(),
        schema: Some("public".to_string()),
        columns: Some(vec![ColumnInfo {
            name: "id".to_string(),
            type_name: "int4".to_string(),
            nullable: false,
            is_primary_key: true,
            default_value: None,
            enum_values: None,
        }]),
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

fn schema_snapshot() -> SchemaSnapshot {
    let mut relational = RelationalSchema::default();
    relational.databases.push(DatabaseInfo {
        name: "analytics".to_string(),
        is_current: true,
    });
    relational.current_database = Some("analytics".to_string());
    relational.schemas.push(DbSchemaInfo {
        name: "public".to_string(),
        tables: vec![dummy_table_info()],
        views: vec![],
        custom_types: None,
    });
    relational.tables.push(dummy_table_info());
    relational.views.push(ViewInfo {
        name: "active_users".to_string(),
        schema: Some("public".to_string()),
    });
    SchemaSnapshot::relational(relational)
}

fn custom_type_info() -> CustomTypeInfo {
    CustomTypeInfo {
        name: "mood".to_string(),
        schema: Some("public".to_string()),
        kind: CustomTypeKind::Enum,
        enum_values: Some(vec!["happy".to_string(), "sad".to_string()]),
        base_type: None,
    }
}

fn key_get_result() -> KeyGetResult {
    KeyGetResult {
        entry: KeyEntry::new("user:1"),
        value: b"payload".to_vec(),
        repr: ValueRepr::Text,
        load_state: KeyLoadState::Loaded,
    }
}

fn code_generator_info() -> CodeGeneratorInfo {
    CodeGeneratorInfo {
        id: "select_star".to_string(),
        label: "SELECT *".to_string(),
        scope: CodeGenScope::Table,
        order: 1,
        destructive: false,
    }
}

fn audit_event() -> AuditEventEmitDto {
    AuditEventEmitDto {
        ts_ms: 1_710_000_000_000,
        level: EventSeverityDto::Info,
        category: EventCategoryDto::Query,
        action: "query.execute".to_string(),
        outcome: EventOutcomeDto::Success,
        summary: "Executed a query".to_string(),
        object_type: Some("table".to_string()),
        object_id: Some("users".to_string()),
        duration_ms: Some(12),
        error_code: None,
        error_message: None,
        details_json: Some(r#"{"rows":1}"#.to_string()),
    }
}

fn row_patch() -> RowPatch {
    RowPatch::new(
        RowIdentity::composite(vec!["id".to_string()], vec![Value::Int(1)]),
        "users".to_string(),
        Some("public".to_string()),
        vec![
            ("name".to_string(), Value::Text("Ada".to_string())),
            ("age".to_string(), Value::Int(36)),
        ],
    )
}

fn row_insert() -> RowInsert {
    RowInsert::new(
        "users".to_string(),
        Some("public".to_string()),
        vec!["name".to_string(), "age".to_string()],
        vec![Value::Text("Ada".to_string()), Value::Int(36)],
    )
}

fn row_delete() -> RowDelete {
    RowDelete {
        identity: RowIdentity::composite(vec!["id".to_string()], vec![Value::Int(1)]),
        table: "users".to_string(),
        schema: Some("public".to_string()),
    }
}

fn document_update() -> DocumentUpdate {
    DocumentUpdate::new(
        "events".to_string(),
        DocumentFilter::new(json!({"status": "pending"})),
        json!({"$set": {"status": "paid"}, "$inc": {"attempts": 1}}),
    )
    .many()
    .upsert()
    .with_database("analytics".to_string())
}

fn document_insert() -> DocumentInsert {
    DocumentInsert::many(
        "events".to_string(),
        vec![json!({"_id": "a", "status": "paid"}), json!({"_id": "b"})],
    )
    .with_database("analytics".to_string())
}

fn document_delete() -> DocumentDelete {
    DocumentDelete::new(
        "events".to_string(),
        DocumentFilter::new(json!({"status": "paid"})),
    )
    .many()
    .with_database("analytics".to_string())
}

/// One representative value for every `DriverRequestBody` variant.
fn request_variants() -> Vec<(&'static str, DriverRequestBody)> {
    vec![
        ("Hello", DriverRequestBody::Hello(hello_request())),
        (
            "OpenSession",
            DriverRequestBody::OpenSession {
                profile_json: r#"{"kind":"sqlite"}"#.to_string(),
                password: Some("password".to_string()),
                ssh_secret: Some("ssh-secret".to_string()),
            },
        ),
        ("CloseSession", DriverRequestBody::CloseSession),
        ("Ping", DriverRequestBody::Ping),
        (
            "Execute",
            DriverRequestBody::Execute {
                request: query_request_dto(),
            },
        ),
        (
            "ExecuteWithHandle",
            DriverRequestBody::ExecuteWithHandle {
                request: query_request_dto(),
            },
        ),
        (
            "Cancel",
            DriverRequestBody::Cancel {
                handle_id: session_id(),
            },
        ),
        ("CancelActive", DriverRequestBody::CancelActive),
        ("CleanupAfterCancel", DriverRequestBody::CleanupAfterCancel),
        ("Schema", DriverRequestBody::Schema),
        ("ListDatabases", DriverRequestBody::ListDatabases),
        (
            "SchemaForDatabase",
            DriverRequestBody::SchemaForDatabase {
                database: "analytics".to_string(),
            },
        ),
        (
            "TableDetails",
            DriverRequestBody::TableDetails {
                database: "analytics".to_string(),
                schema: Some("public".to_string()),
                table: "users".to_string(),
            },
        ),
        (
            "ViewDetails",
            DriverRequestBody::ViewDetails {
                database: "analytics".to_string(),
                schema: Some("public".to_string()),
                view: "active_users".to_string(),
            },
        ),
        (
            "SetActiveDatabase",
            DriverRequestBody::SetActiveDatabase {
                database: Some("analytics".to_string()),
            },
        ),
        (
            "BrowseTable",
            DriverRequestBody::BrowseTable {
                request: TableBrowseRequest::new(TableRef::new("users")),
            },
        ),
        (
            "CountTable",
            DriverRequestBody::CountTable {
                request: TableCountRequest::new(TableRef::new("users")),
            },
        ),
        (
            "BrowseCollection",
            DriverRequestBody::BrowseCollection {
                request: CollectionBrowseRequestDto::from(CollectionBrowseRequest::new(
                    CollectionRef::new("analytics", "events"),
                )),
            },
        ),
        (
            "CountCollection",
            DriverRequestBody::CountCollection {
                request: CollectionCountRequestDto::from(CollectionCountRequest::new(
                    CollectionRef::new("analytics", "events"),
                )),
            },
        ),
        (
            "Explain",
            DriverRequestBody::Explain {
                request: ExplainRequest::new(TableRef::new("users")),
            },
        ),
        (
            "DescribeTable",
            DriverRequestBody::DescribeTable {
                request: DescribeRequest::new(TableRef::new("users")),
            },
        ),
        (
            "PlanSemantic",
            DriverRequestBody::PlanSemantic {
                request: SemanticRequest::Explain(ExplainRequest::new(TableRef::new("users"))),
            },
        ),
        (
            "UpdateRow",
            DriverRequestBody::UpdateRow { patch: row_patch() },
        ),
        (
            "InsertRow",
            DriverRequestBody::InsertRow {
                insert: row_insert(),
            },
        ),
        (
            "DeleteRow",
            DriverRequestBody::DeleteRow {
                delete: row_delete(),
            },
        ),
        (
            "UpdateDocument",
            DriverRequestBody::UpdateDocument {
                update: DocumentUpdateDto::from(document_update()),
            },
        ),
        (
            "InsertDocument",
            DriverRequestBody::InsertDocument {
                insert: DocumentInsertDto::from(document_insert()),
            },
        ),
        (
            "DeleteDocument",
            DriverRequestBody::DeleteDocument {
                delete: DocumentDeleteDto::from(document_delete()),
            },
        ),
        (
            "SchemaTypes",
            DriverRequestBody::SchemaTypes {
                database: "analytics".to_string(),
                schema: Some("public".to_string()),
            },
        ),
        (
            "SchemaIndexes",
            DriverRequestBody::SchemaIndexes {
                database: "analytics".to_string(),
                schema: Some("public".to_string()),
            },
        ),
        (
            "SchemaForeignKeys",
            DriverRequestBody::SchemaForeignKeys {
                database: "analytics".to_string(),
                schema: Some("public".to_string()),
            },
        ),
        ("ActiveDatabase", DriverRequestBody::ActiveDatabase),
        (
            "KvScanKeys",
            DriverRequestBody::KvScanKeys {
                request: KeyScanRequest::new(100)
                    .with_cursor("cursor-1")
                    .with_filter("user:*")
                    .with_keyspace(1)
                    .with_type_filter(KeyType::String),
            },
        ),
        (
            "KvGetKey",
            DriverRequestBody::KvGetKey {
                request: KeyGetRequest::new("user:1")
                    .with_keyspace(0)
                    .with_max_value_bytes(4_096),
            },
        ),
        (
            "KvSetKey",
            DriverRequestBody::KvSetKey {
                request: KeySetRequest::new("user:1", b"payload".to_vec())
                    .with_repr(ValueRepr::Text)
                    .with_keyspace(0)
                    .with_ttl(60)
                    .keeping_ttl()
                    .if_not_exists(),
            },
        ),
        (
            "KvDeleteKey",
            DriverRequestBody::KvDeleteKey {
                request: KeyDeleteRequest::new("user:1").with_keyspace(0),
            },
        ),
        (
            "KvExistsKey",
            DriverRequestBody::KvExistsKey {
                request: KeyExistsRequest::new("user:1"),
            },
        ),
        (
            "KvKeyType",
            DriverRequestBody::KvKeyType {
                request: KeyTypeRequest::new("user:1"),
            },
        ),
        (
            "KvKeyTtl",
            DriverRequestBody::KvKeyTtl {
                request: KeyTtlRequest::new("user:1"),
            },
        ),
        (
            "KvExpireKey",
            DriverRequestBody::KvExpireKey {
                request: KeyExpireRequest::new("user:1", 60),
            },
        ),
        (
            "KvPersistKey",
            DriverRequestBody::KvPersistKey {
                request: KeyPersistRequest::new("user:1"),
            },
        ),
        (
            "KvRenameKey",
            DriverRequestBody::KvRenameKey {
                request: KeyRenameRequest::new("user:1", "user:2"),
            },
        ),
        (
            "KvBulkGet",
            DriverRequestBody::KvBulkGet {
                request: KeyBulkGetRequest::new(vec!["user:1".to_string(), "user:2".to_string()]),
            },
        ),
        (
            "KvHashSet",
            DriverRequestBody::KvHashSet {
                request: HashSetRequest {
                    key: "hash".to_string(),
                    fields: vec![("field".to_string(), "value".to_string())],
                    keyspace: Some(0),
                },
            },
        ),
        (
            "KvHashDelete",
            DriverRequestBody::KvHashDelete {
                request: HashDeleteRequest {
                    key: "hash".to_string(),
                    fields: vec!["field".to_string()],
                    keyspace: Some(0),
                },
            },
        ),
        (
            "KvListSet",
            DriverRequestBody::KvListSet {
                request: ListSetRequest {
                    key: "list".to_string(),
                    index: 0,
                    value: "value".to_string(),
                    keyspace: Some(0),
                },
            },
        ),
        (
            "KvListPush",
            DriverRequestBody::KvListPush {
                request: ListPushRequest {
                    key: "list".to_string(),
                    values: vec!["a".to_string(), "b".to_string()],
                    end: ListEnd::Tail,
                    keyspace: Some(0),
                },
            },
        ),
        (
            "KvListRemove",
            DriverRequestBody::KvListRemove {
                request: ListRemoveRequest {
                    key: "list".to_string(),
                    value: "a".to_string(),
                    count: 1,
                    keyspace: Some(0),
                },
            },
        ),
        (
            "KvSetAdd",
            DriverRequestBody::KvSetAdd {
                request: SetAddRequest {
                    key: "set".to_string(),
                    members: vec!["member".to_string()],
                    keyspace: Some(0),
                },
            },
        ),
        (
            "KvSetRemove",
            DriverRequestBody::KvSetRemove {
                request: SetRemoveRequest {
                    key: "set".to_string(),
                    members: vec!["member".to_string()],
                    keyspace: Some(0),
                },
            },
        ),
        (
            "KvZSetAdd",
            DriverRequestBody::KvZSetAdd {
                request: ZSetAddRequest {
                    key: "zset".to_string(),
                    members: vec![("member".to_string(), 1.5)],
                    keyspace: Some(0),
                },
            },
        ),
        (
            "KvZSetRemove",
            DriverRequestBody::KvZSetRemove {
                request: ZSetRemoveRequest {
                    key: "zset".to_string(),
                    members: vec!["member".to_string()],
                    keyspace: Some(0),
                },
            },
        ),
        (
            "KvStreamAdd",
            DriverRequestBody::KvStreamAdd {
                request: StreamAddRequest {
                    key: "stream".to_string(),
                    id: StreamEntryId::Explicit("1-1".to_string()),
                    fields: vec![("field".to_string(), "value".to_string())],
                    maxlen: Some(StreamMaxLen {
                        count: 10,
                        approximate: true,
                    }),
                    keyspace: Some(0),
                },
            },
        ),
        (
            "KvStreamDelete",
            DriverRequestBody::KvStreamDelete {
                request: StreamDeleteRequest {
                    key: "stream".to_string(),
                    ids: vec!["1-1".to_string()],
                    keyspace: Some(0),
                },
            },
        ),
        ("CodeGenerators", DriverRequestBody::CodeGenerators),
        (
            "GenerateCode",
            DriverRequestBody::GenerateCode {
                generator_id: "create_table".to_string(),
                table: dummy_table_info(),
            },
        ),
        (
            "SchemaColumns",
            DriverRequestBody::SchemaColumns {
                database: "analytics".to_string(),
                schema: Some("public".to_string()),
            },
        ),
        (
            "KvKeyCount",
            DriverRequestBody::KvKeyCount { keyspace: Some(2) },
        ),
    ]
}

/// One representative value for every `DriverResponseBody` variant.
fn response_variants() -> Vec<(&'static str, DriverResponseBody)> {
    vec![
        ("Hello", DriverResponseBody::Hello(hello_response())),
        (
            "SessionOpened",
            DriverResponseBody::SessionOpened {
                session_id: session_id(),
                kind: DbKind::SQLite,
                metadata: driver_metadata(),
                schema_loading_strategy: SchemaLoadingStrategy::SingleDatabase,
                schema_features: SchemaFeatures::all(),
                code_gen_capabilities: CodeGenCapabilities::all(),
            },
        ),
        ("SessionClosed", DriverResponseBody::SessionClosed),
        ("Pong", DriverResponseBody::Pong),
        (
            "ExecuteResult",
            DriverResponseBody::ExecuteResult {
                result: query_result_dto(),
            },
        ),
        (
            "ExecuteWithHandleResult",
            DriverResponseBody::ExecuteWithHandleResult {
                handle_id: session_id(),
                result: query_result_dto(),
            },
        ),
        (
            "QueryChunk",
            DriverResponseBody::QueryChunk(QueryResultChunk {
                chunk_index: 0,
                rows: vec![vec![Value::Int(1)], vec![Value::Null]],
                done: false,
            }),
        ),
        ("Cancelled", DriverResponseBody::Cancelled),
        ("CleanupComplete", DriverResponseBody::CleanupComplete),
        (
            "Schema",
            DriverResponseBody::Schema {
                schema: schema_snapshot(),
            },
        ),
        (
            "Databases",
            DriverResponseBody::Databases {
                databases: vec![DatabaseInfo {
                    name: "analytics".to_string(),
                    is_current: true,
                }],
            },
        ),
        (
            "SchemaForDatabase",
            DriverResponseBody::SchemaForDatabase {
                schema: DbSchemaInfo {
                    name: "public".to_string(),
                    tables: vec![dummy_table_info()],
                    views: vec![ViewInfo {
                        name: "active_users".to_string(),
                        schema: Some("public".to_string()),
                    }],
                    custom_types: Some(vec![custom_type_info()]),
                },
            },
        ),
        (
            "TableDetails",
            DriverResponseBody::TableDetails {
                table: dummy_table_info(),
            },
        ),
        (
            "ViewDetails",
            DriverResponseBody::ViewDetails {
                view: ViewInfo {
                    name: "active_users".to_string(),
                    schema: Some("public".to_string()),
                },
            },
        ),
        ("ActiveDatabaseSet", DriverResponseBody::ActiveDatabaseSet),
        (
            "BrowseResult",
            DriverResponseBody::BrowseResult {
                result: query_result_dto(),
            },
        ),
        ("CountResult", DriverResponseBody::CountResult { count: 42 }),
        (
            "SemanticPlan",
            DriverResponseBody::SemanticPlan {
                plan: SemanticPlan::single_query(
                    SemanticPlanKind::Query,
                    PlannedQuery::new(QueryLanguage::Sql, "SELECT 1"),
                ),
            },
        ),
        (
            "CrudResult",
            DriverResponseBody::CrudResult {
                result: CrudResultDto::from(CrudResult::new(1, None)),
            },
        ),
        (
            "SchemaTypes",
            DriverResponseBody::SchemaTypes {
                types: vec![custom_type_info()],
            },
        ),
        (
            "SchemaIndexes",
            DriverResponseBody::SchemaIndexes {
                indexes: vec![SchemaIndexInfo {
                    name: "users_pkey".to_string(),
                    table_name: "users".to_string(),
                    columns: vec!["id".to_string()],
                    is_unique: true,
                    is_primary: true,
                }],
            },
        ),
        (
            "SchemaForeignKeys",
            DriverResponseBody::SchemaForeignKeys {
                foreign_keys: vec![SchemaForeignKeyInfo {
                    name: "fk_users_org".to_string(),
                    table_name: "users".to_string(),
                    columns: vec!["org_id".to_string()],
                    referenced_schema: Some("public".to_string()),
                    referenced_table: "orgs".to_string(),
                    referenced_columns: vec!["id".to_string()],
                    on_delete: Some("CASCADE".to_string()),
                    on_update: None,
                }],
            },
        ),
        (
            "ActiveDatabaseResult",
            DriverResponseBody::ActiveDatabaseResult {
                database: Some("analytics".to_string()),
            },
        ),
        (
            "KvScanResult",
            DriverResponseBody::KvScanResult {
                page: KeyScanPage {
                    entries: vec![KeyEntry::new("user:1")],
                    next_cursor: Some("cursor-2".to_string()),
                    scanned_keys: Some(17),
                },
            },
        ),
        (
            "KvGetResult",
            DriverResponseBody::KvGetResult {
                result: key_get_result(),
            },
        ),
        (
            "KvBoolResult",
            DriverResponseBody::KvBoolResult { value: true },
        ),
        (
            "KvStringResult",
            DriverResponseBody::KvStringResult {
                value: "OK".to_string(),
            },
        ),
        ("KvU64Result", DriverResponseBody::KvU64Result { value: 7 }),
        (
            "KvBulkGetResult",
            DriverResponseBody::KvBulkGetResult {
                results: vec![Some(key_get_result()), None],
            },
        ),
        (
            "CodeGeneratorsResult",
            DriverResponseBody::CodeGeneratorsResult {
                generators: vec![code_generator_info()],
            },
        ),
        (
            "GenerateCodeResult",
            DriverResponseBody::GenerateCodeResult {
                code: "SELECT * FROM users;".to_string(),
            },
        ),
        (
            "EmitAuditEvent",
            DriverResponseBody::EmitAuditEvent(audit_event()),
        ),
        (
            "Error",
            DriverResponseBody::Error(DriverRpcError {
                code: DriverRpcErrorCode::Driver,
                message: "boom".to_string(),
                retriable: true,
            }),
        ),
        (
            "SchemaColumns",
            DriverResponseBody::SchemaColumns {
                columns: vec![SchemaColumnInfo {
                    table_name: "users".to_string(),
                    column: ColumnInfo {
                        name: "email".to_string(),
                        type_name: "varchar(255)".to_string(),
                        nullable: true,
                        is_primary_key: false,
                        default_value: None,
                        enum_values: None,
                    },
                }],
            },
        ),
        (
            "KvKeyCountResult",
            DriverResponseBody::KvKeyCountResult { count: 1_234 },
        ),
        (
            "ExecuteResultWithTruncation",
            DriverResponseBody::ExecuteResultWithTruncation {
                result: query_result_dto(),
                rows_truncated: true,
            },
        ),
        (
            "ExecuteWithHandleResultWithTruncation",
            DriverResponseBody::ExecuteWithHandleResultWithTruncation {
                handle_id: Uuid::from_u128(11),
                result: query_result_dto(),
                rows_truncated: false,
            },
        ),
        (
            "ExecuteResultSets",
            DriverResponseBody::ExecuteResultSets {
                results: query_result_sets_dto(),
            },
        ),
        (
            "ExecuteWithHandleResultSets",
            DriverResponseBody::ExecuteWithHandleResultSets {
                handle_id: Uuid::from_u128(12),
                results: query_result_sets_dto(),
            },
        ),
        (
            "BrowseResultSets",
            DriverResponseBody::BrowseResultSets {
                results: query_result_sets_dto(),
            },
        ),
    ]
}

/// Names a request variant. The match has no wildcard arm, so adding a
/// `DriverRequestBody` variant fails to compile here until it is named, and
/// `request_fixtures_cover_every_variant` then fails until it has a fixture.
fn request_variant_name(body: &DriverRequestBody) -> &'static str {
    match body {
        DriverRequestBody::Hello(_) => "Hello",
        DriverRequestBody::OpenSession { .. } => "OpenSession",
        DriverRequestBody::CloseSession => "CloseSession",
        DriverRequestBody::Ping => "Ping",
        DriverRequestBody::Execute { .. } => "Execute",
        DriverRequestBody::ExecuteWithHandle { .. } => "ExecuteWithHandle",
        DriverRequestBody::Cancel { .. } => "Cancel",
        DriverRequestBody::CancelActive => "CancelActive",
        DriverRequestBody::CleanupAfterCancel => "CleanupAfterCancel",
        DriverRequestBody::Schema => "Schema",
        DriverRequestBody::ListDatabases => "ListDatabases",
        DriverRequestBody::SchemaForDatabase { .. } => "SchemaForDatabase",
        DriverRequestBody::TableDetails { .. } => "TableDetails",
        DriverRequestBody::ViewDetails { .. } => "ViewDetails",
        DriverRequestBody::SetActiveDatabase { .. } => "SetActiveDatabase",
        DriverRequestBody::BrowseTable { .. } => "BrowseTable",
        DriverRequestBody::CountTable { .. } => "CountTable",
        DriverRequestBody::BrowseCollection { .. } => "BrowseCollection",
        DriverRequestBody::CountCollection { .. } => "CountCollection",
        DriverRequestBody::Explain { .. } => "Explain",
        DriverRequestBody::DescribeTable { .. } => "DescribeTable",
        DriverRequestBody::PlanSemantic { .. } => "PlanSemantic",
        DriverRequestBody::UpdateRow { .. } => "UpdateRow",
        DriverRequestBody::InsertRow { .. } => "InsertRow",
        DriverRequestBody::DeleteRow { .. } => "DeleteRow",
        DriverRequestBody::UpdateDocument { .. } => "UpdateDocument",
        DriverRequestBody::InsertDocument { .. } => "InsertDocument",
        DriverRequestBody::DeleteDocument { .. } => "DeleteDocument",
        DriverRequestBody::SchemaTypes { .. } => "SchemaTypes",
        DriverRequestBody::SchemaIndexes { .. } => "SchemaIndexes",
        DriverRequestBody::SchemaForeignKeys { .. } => "SchemaForeignKeys",
        DriverRequestBody::ActiveDatabase => "ActiveDatabase",
        DriverRequestBody::KvScanKeys { .. } => "KvScanKeys",
        DriverRequestBody::KvGetKey { .. } => "KvGetKey",
        DriverRequestBody::KvSetKey { .. } => "KvSetKey",
        DriverRequestBody::KvDeleteKey { .. } => "KvDeleteKey",
        DriverRequestBody::KvExistsKey { .. } => "KvExistsKey",
        DriverRequestBody::KvKeyType { .. } => "KvKeyType",
        DriverRequestBody::KvKeyTtl { .. } => "KvKeyTtl",
        DriverRequestBody::KvExpireKey { .. } => "KvExpireKey",
        DriverRequestBody::KvPersistKey { .. } => "KvPersistKey",
        DriverRequestBody::KvRenameKey { .. } => "KvRenameKey",
        DriverRequestBody::KvBulkGet { .. } => "KvBulkGet",
        DriverRequestBody::KvHashSet { .. } => "KvHashSet",
        DriverRequestBody::KvHashDelete { .. } => "KvHashDelete",
        DriverRequestBody::KvListSet { .. } => "KvListSet",
        DriverRequestBody::KvListPush { .. } => "KvListPush",
        DriverRequestBody::KvListRemove { .. } => "KvListRemove",
        DriverRequestBody::KvSetAdd { .. } => "KvSetAdd",
        DriverRequestBody::KvSetRemove { .. } => "KvSetRemove",
        DriverRequestBody::KvZSetAdd { .. } => "KvZSetAdd",
        DriverRequestBody::KvZSetRemove { .. } => "KvZSetRemove",
        DriverRequestBody::KvStreamAdd { .. } => "KvStreamAdd",
        DriverRequestBody::KvStreamDelete { .. } => "KvStreamDelete",
        DriverRequestBody::CodeGenerators => "CodeGenerators",
        DriverRequestBody::GenerateCode { .. } => "GenerateCode",
        DriverRequestBody::SchemaColumns { .. } => "SchemaColumns",
        DriverRequestBody::KvKeyCount { .. } => "KvKeyCount",
    }
}

/// Names a response variant; see [`request_variant_name`].
fn response_variant_name(body: &DriverResponseBody) -> &'static str {
    match body {
        DriverResponseBody::Hello(_) => "Hello",
        DriverResponseBody::SessionOpened { .. } => "SessionOpened",
        DriverResponseBody::SessionClosed => "SessionClosed",
        DriverResponseBody::Pong => "Pong",
        DriverResponseBody::ExecuteResult { .. } => "ExecuteResult",
        DriverResponseBody::ExecuteWithHandleResult { .. } => "ExecuteWithHandleResult",
        DriverResponseBody::QueryChunk(_) => "QueryChunk",
        DriverResponseBody::Cancelled => "Cancelled",
        DriverResponseBody::CleanupComplete => "CleanupComplete",
        DriverResponseBody::Schema { .. } => "Schema",
        DriverResponseBody::Databases { .. } => "Databases",
        DriverResponseBody::SchemaForDatabase { .. } => "SchemaForDatabase",
        DriverResponseBody::TableDetails { .. } => "TableDetails",
        DriverResponseBody::ViewDetails { .. } => "ViewDetails",
        DriverResponseBody::ActiveDatabaseSet => "ActiveDatabaseSet",
        DriverResponseBody::BrowseResult { .. } => "BrowseResult",
        DriverResponseBody::CountResult { .. } => "CountResult",
        DriverResponseBody::SemanticPlan { .. } => "SemanticPlan",
        DriverResponseBody::CrudResult { .. } => "CrudResult",
        DriverResponseBody::SchemaTypes { .. } => "SchemaTypes",
        DriverResponseBody::SchemaIndexes { .. } => "SchemaIndexes",
        DriverResponseBody::SchemaForeignKeys { .. } => "SchemaForeignKeys",
        DriverResponseBody::ActiveDatabaseResult { .. } => "ActiveDatabaseResult",
        DriverResponseBody::KvScanResult { .. } => "KvScanResult",
        DriverResponseBody::KvGetResult { .. } => "KvGetResult",
        DriverResponseBody::KvBoolResult { .. } => "KvBoolResult",
        DriverResponseBody::KvStringResult { .. } => "KvStringResult",
        DriverResponseBody::KvU64Result { .. } => "KvU64Result",
        DriverResponseBody::KvBulkGetResult { .. } => "KvBulkGetResult",
        DriverResponseBody::CodeGeneratorsResult { .. } => "CodeGeneratorsResult",
        DriverResponseBody::GenerateCodeResult { .. } => "GenerateCodeResult",
        DriverResponseBody::EmitAuditEvent(_) => "EmitAuditEvent",
        DriverResponseBody::Error(_) => "Error",
        DriverResponseBody::SchemaColumns { .. } => "SchemaColumns",
        DriverResponseBody::KvKeyCountResult { .. } => "KvKeyCountResult",
        DriverResponseBody::ExecuteResultWithTruncation { .. } => "ExecuteResultWithTruncation",
        DriverResponseBody::ExecuteWithHandleResultWithTruncation { .. } => {
            "ExecuteWithHandleResultWithTruncation"
        }
        DriverResponseBody::ExecuteResultSets { .. } => "ExecuteResultSets",
        DriverResponseBody::ExecuteWithHandleResultSets { .. } => "ExecuteWithHandleResultSets",
        DriverResponseBody::BrowseResultSets { .. } => "BrowseResultSets",
    }
}

/// Deserializer that only records the variant list serde derives for an enum,
/// so the fixture lists can be checked against every variant the type has
/// rather than against a list written by hand.
struct EnumVariantNames(Option<&'static [&'static str]>);

impl<'de> serde::Deserializer<'de> for &mut EnumVariantNames {
    type Error = serde::de::value::Error;

    fn deserialize_any<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(serde::de::Error::custom("only enums are inspected"))
    }

    fn deserialize_enum<V: serde::de::Visitor<'de>>(
        self,
        _name: &'static str,
        variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.0 = Some(variants);
        Err(serde::de::Error::custom("variant names recorded"))
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        option unit unit_struct newtype_struct seq tuple tuple_struct map struct identifier
        ignored_any
    }
}

fn enum_variant_names<T: DeserializeOwned>() -> BTreeSet<&'static str> {
    let mut recorder = EnumVariantNames(None);
    assert!(
        T::deserialize(&mut recorder).is_err(),
        "the recorder never produces a value"
    );
    recorder
        .0
        .expect("the type deserializes as an enum")
        .iter()
        .copied()
        .collect()
}

#[test]
fn request_fixtures_cover_every_variant() {
    let mut covered = BTreeSet::new();
    for (name, body) in request_variants() {
        assert_eq!(name, request_variant_name(&body), "fixture label");
        assert!(covered.insert(name), "duplicate fixture for {name}");
    }
    assert_eq!(covered, enum_variant_names::<DriverRequestBody>());
}

#[test]
fn response_fixtures_cover_every_variant() {
    let mut covered = BTreeSet::new();
    for (name, body) in response_variants() {
        assert_eq!(name, response_variant_name(&body), "fixture label");
        assert!(covered.insert(name), "duplicate fixture for {name}");
    }
    assert_eq!(covered, enum_variant_names::<DriverResponseBody>());
}

#[test]
fn every_request_variant_round_trips() {
    for (name, body) in request_variants() {
        assert_postcard_roundtrip_named(name, &request_envelope(body));
    }
}

#[test]
fn every_response_variant_round_trips() {
    for (name, body) in response_variants() {
        assert_postcard_roundtrip_named(name, &response_envelope(body));
    }
}

#[test]
fn json_text_round_trips_through_postcard() {
    let value = json!({"a": 1, "nested": [true, null, "x"]});
    let text = JsonText::new(value.clone());

    assert_postcard_roundtrip(&text);

    let encoded = postcard::to_allocvec(&text).expect("encode");
    let decoded: JsonText = postcard::from_bytes(&encoded).expect("decode");
    assert_eq!(decoded.value(), &value);
}

#[test]
fn document_insert_round_trips_with_filter_values() {
    let documents = vec![
        json!({"_id": "a", "status": "paid", "tags": ["x", "y"]}),
        json!({"_id": "b", "nested": {"ok": true, "n": null}}),
    ];
    let insert = DocumentInsert::many("events".to_string(), documents.clone());
    let dto = DocumentInsertDto::from(insert);

    assert_postcard_roundtrip(&dto);

    let encoded = postcard::to_allocvec(&dto).expect("encode");
    let decoded: DocumentInsertDto = postcard::from_bytes(&encoded).expect("decode");
    let restored = DocumentInsert::from(decoded);
    assert_eq!(restored.documents, documents);
    assert_eq!(restored.database, None);
}

#[test]
fn document_update_and_delete_round_trip() {
    let update = document_update();
    let dto = DocumentUpdateDto::from(update.clone());

    assert_postcard_roundtrip(&dto);

    let encoded = postcard::to_allocvec(&dto).expect("encode");
    let decoded: DocumentUpdateDto = postcard::from_bytes(&encoded).expect("decode");
    let restored = DocumentUpdate::from(decoded);
    assert_eq!(restored.collection, "events");
    assert_eq!(restored.database.as_deref(), Some("analytics"));
    assert_eq!(restored.filter.filter, json!({"status": "pending"}));
    assert_eq!(
        restored.update,
        json!({"$set": {"status": "paid"}, "$inc": {"attempts": 1}})
    );
    assert!(restored.many);
    assert!(restored.upsert);

    let delete = document_delete();
    let dto = DocumentDeleteDto::from(delete.clone());

    assert_postcard_roundtrip(&dto);

    let encoded = postcard::to_allocvec(&dto).expect("encode");
    let decoded: DocumentDeleteDto = postcard::from_bytes(&encoded).expect("decode");
    let restored = DocumentDelete::from(decoded);
    assert_eq!(restored.collection, "events");
    assert_eq!(restored.database.as_deref(), Some("analytics"));
    assert_eq!(restored.filter.filter, json!({"status": "paid"}));
    assert!(restored.many);
}

#[test]
fn filtered_collection_browse_round_trips() {
    let request = CollectionBrowseRequest::new(CollectionRef::new("analytics", "events"))
        .with_filter(json!({"status": "paid"}))
        .with_projection(json!({"status": 1, "total": 1}))
        .with_sort(json!({"total": -1}));
    let dto = CollectionBrowseRequestDto::from(request);

    assert_postcard_roundtrip(&dto);

    let encoded = postcard::to_allocvec(&dto).expect("encode");
    let decoded: CollectionBrowseRequestDto = postcard::from_bytes(&encoded).expect("decode");
    let restored = CollectionBrowseRequest::from(decoded);
    assert_eq!(restored.filter, Some(json!({"status": "paid"})));
    assert_eq!(restored.projection, Some(json!({"status": 1, "total": 1})));
    assert_eq!(restored.sort, Some(json!({"total": -1})));
    assert!(restored.semantic_filter.is_none());
}

#[test]
fn collection_browse_without_a_semantic_filter_still_encodes_semantic_filter() {
    let request = CollectionBrowseRequest::new(CollectionRef::new("analytics", "events"));
    assert!(request.semantic_filter.is_none());

    let dto = CollectionBrowseRequestDto::from(request);
    let encoded = postcard::to_allocvec(&dto).expect("encode");
    let decoded: CollectionBrowseRequestDto = postcard::from_bytes(&encoded).expect("decode");
    assert!(
        decoded.semantic_filter.is_none(),
        "a None semantic_filter must still occupy a frame slot"
    );

    let reencoded = postcard::to_allocvec(&decoded).expect("re-encode");
    assert_eq!(
        encoded, reencoded,
        "semantic_filter omission desynchronised the frame"
    );
}

#[test]
fn unfiltered_count_collection_round_trips() {
    let request = CollectionCountRequest::new(CollectionRef::new("analytics", "events"));
    assert!(request.filter.is_none());
    assert!(request.semantic_filter.is_none());

    let dto = CollectionCountRequestDto::from(request);

    assert_postcard_roundtrip(&dto);

    let encoded = postcard::to_allocvec(&dto).expect("encode");
    let decoded: CollectionCountRequestDto = postcard::from_bytes(&encoded).expect("decode");
    assert!(decoded.filter.is_none());
    assert!(decoded.semantic_filter.is_none());
}

#[test]
fn table_browse_and_count_without_semantic_filter_round_trip() {
    let browse = TableBrowseRequest::new(TableRef::new("users"));
    assert!(browse.semantic_filter.is_none());
    assert!(browse.columns.is_empty());

    assert_postcard_roundtrip(&browse);

    let encoded = postcard::to_allocvec(&browse).expect("encode");
    let decoded: TableBrowseRequest = postcard::from_bytes(&encoded).expect("decode");
    assert!(decoded.semantic_filter.is_none());
    assert!(decoded.columns.is_empty());

    let count = TableCountRequest::new(TableRef::new("users"));
    assert!(count.semantic_filter.is_none());

    assert_postcard_roundtrip(&count);

    let encoded = postcard::to_allocvec(&count).expect("encode");
    let decoded: TableCountRequest = postcard::from_bytes(&encoded).expect("decode");
    assert!(decoded.semantic_filter.is_none());
}

#[test]
fn column_info_without_enum_values_round_trips() {
    let columns = vec![
        SchemaColumnInfo {
            table_name: "users".to_string(),
            column: ColumnInfo {
                name: "id".to_string(),
                type_name: "int4".to_string(),
                nullable: false,
                is_primary_key: true,
                default_value: None,
                enum_values: None,
            },
        },
        SchemaColumnInfo {
            table_name: "users".to_string(),
            column: ColumnInfo {
                name: "email".to_string(),
                type_name: "varchar(255)".to_string(),
                nullable: true,
                is_primary_key: false,
                default_value: Some("''".to_string()),
                enum_values: None,
            },
        },
    ];

    assert_postcard_roundtrip(&columns);

    let encoded = postcard::to_allocvec(&columns).expect("encode");
    let decoded: Vec<SchemaColumnInfo> = postcard::from_bytes(&encoded).expect("decode");
    assert_eq!(decoded.len(), 2);
    assert!(
        decoded
            .iter()
            .all(|column| column.column.enum_values.is_none())
    );
    assert_eq!(decoded[1].column.default_value.as_deref(), Some("''"));
}

#[test]
fn crud_result_without_metadata_round_trips() {
    let dto = CrudResultDto::from(CrudResult::new(1, None));

    assert_postcard_roundtrip(&dto);

    let encoded = postcard::to_allocvec(&dto).expect("encode");
    let decoded: CrudResultDto = postcard::from_bytes(&encoded).expect("decode");
    let restored = CrudResult::from(decoded);
    assert_eq!(restored.affected_rows, 1);
    assert!(restored.returning_row.is_none());
    assert!(restored.metadata_extra.is_none());

    let dto = CrudResultDto::from(CrudResult::empty());

    assert_postcard_roundtrip(&dto);

    let encoded = postcard::to_allocvec(&dto).expect("encode");
    let decoded: CrudResultDto = postcard::from_bytes(&encoded).expect("decode");
    let restored = CrudResult::from(decoded);
    assert_eq!(restored.affected_rows, 0);
    assert!(restored.returning_row.is_none());
    assert!(restored.metadata_extra.is_none());
}

#[test]
fn row_patch_and_insert_without_type_names_round_trip() {
    let patch = row_patch();

    assert_postcard_roundtrip(&patch);

    let encoded = postcard::to_allocvec(&patch).expect("encode");
    let decoded: RowPatch = postcard::from_bytes(&encoded).expect("decode");
    assert_eq!(decoded.changes.len(), 2);
    assert_eq!(decoded.changes[0].name, "name");
    assert_eq!(decoded.changes[0].value, Value::Text("Ada".to_string()));
    assert_eq!(decoded.changes[1].name, "age");
    assert_eq!(decoded.changes[1].value, Value::Int(36));
    assert!(
        decoded
            .changes
            .iter()
            .all(|change| change.type_name.is_none())
    );

    let insert = row_insert();

    assert_postcard_roundtrip(&insert);

    let encoded = postcard::to_allocvec(&insert).expect("encode");
    let decoded: RowInsert = postcard::from_bytes(&encoded).expect("decode");
    assert_eq!(decoded.assignments.len(), 2);
    assert_eq!(decoded.assignments[0].name, "name");
    assert_eq!(decoded.assignments[0].value, Value::Text("Ada".to_string()));
    assert!(
        decoded
            .assignments
            .iter()
            .all(|assignment| assignment.type_name.is_none())
    );
}

#[test]
fn execution_context_without_source_round_trips() {
    let request = QueryRequest {
        execution_context: Some(ExecutionContext {
            connection_id: Some(session_id()),
            database: Some("analytics".to_string()),
            schema: Some("public".to_string()),
            container: None,
            source: None,
        }),
        ..Default::default()
    };
    let dto = QueryRequestDto::from(&request);

    assert_postcard_roundtrip(&dto);

    let encoded = postcard::to_allocvec(&dto).expect("encode");
    let decoded: QueryRequestDto = postcard::from_bytes(&encoded).expect("decode");
    let restored = QueryRequest::from(decoded);
    match restored.execution_context {
        Some(ExecutionContext {
            database,
            source: None,
            ..
        }) => assert_eq!(database.as_deref(), Some("analytics")),
        other => panic!("unexpected execution context: {other:?}"),
    }

    let request = QueryRequest {
        execution_context: Some(ExecutionContext {
            connection_id: Some(session_id()),
            database: Some("analytics".to_string()),
            schema: None,
            container: Some("events".to_string()),
            source: Some(ExecutionSourceContext::CollectionWindow {
                targets: vec!["/aws/lambda/app".to_string(), "/aws/ecs/api".to_string()],
                start_ms: 1_710_000_000_000,
                end_ms: 1_710_000_300_000,
                query_mode: None,
            }),
        }),
        ..Default::default()
    };
    let dto = QueryRequestDto::from(&request);

    assert_postcard_roundtrip(&dto);

    let encoded = postcard::to_allocvec(&dto).expect("encode");
    let decoded: QueryRequestDto = postcard::from_bytes(&encoded).expect("decode");
    let restored = QueryRequest::from(decoded);
    match restored.execution_context {
        Some(ExecutionContext {
            source:
                Some(ExecutionSourceContext::CollectionWindow {
                    targets,
                    query_mode,
                    ..
                }),
            ..
        }) => {
            assert!(
                query_mode.is_none(),
                "a None query_mode must still occupy a frame slot"
            );
            assert_eq!(targets.len(), 2);
        }
        other => panic!("unexpected execution context: {other:?}"),
    }
}

#[test]
fn form_section_without_icon_round_trips() {
    let form = DriverFormDef {
        tabs: vec![FormTab {
            id: "main".to_string(),
            label: "Main".to_string(),
            sections: vec![FormSection {
                title: "Server".to_string(),
                icon: None,
                fields: Vec::new(),
            }],
        }],
    };

    assert_postcard_roundtrip(&form);

    let encoded = postcard::to_allocvec(&form).expect("encode");
    let decoded: DriverFormDef = postcard::from_bytes(&encoded).expect("decode");
    assert_eq!(decoded.tabs.len(), 1);
    assert_eq!(decoded.tabs[0].sections.len(), 1);
    assert!(
        decoded.tabs[0].sections[0].icon.is_none(),
        "a None icon must still occupy a frame slot"
    );

    let response = DriverResponseBody::Hello(hello_response());
    assert_postcard_roundtrip(&response);
}

#[test]
fn data_structure_variants_keep_their_postcard_indices() {
    use dbflux_core::{DataStructure, DocumentSchema, KeyValueSchema, TimeSeriesSchema};

    let cases = [
        (DataStructure::Relational(RelationalSchema::default()), 0u8),
        (DataStructure::Document(DocumentSchema::default()), 1),
        (DataStructure::KeyValue(KeyValueSchema::default()), 2),
        (DataStructure::ReservedGraph, 3),
        (DataStructure::ReservedWideColumn, 4),
        (DataStructure::TimeSeries(TimeSeriesSchema::default()), 5),
    ];

    for (structure, expected_index) in cases {
        let bytes = postcard::to_allocvec(&structure).expect("encode data structure");
        assert_eq!(bytes.first(), Some(&expected_index), "{structure:?}");

        let decoded: DataStructure = postcard::from_bytes(&bytes).expect("decode data structure");
        let reencoded = postcard::to_allocvec(&decoded).expect("re-encode data structure");
        assert_eq!(reencoded, bytes, "{structure:?}");
    }
}

#[test]
fn database_category_variants_keep_their_postcard_indices() {
    use dbflux_core::DatabaseCategory;

    let cases = [
        (DatabaseCategory::Relational, 0u8),
        (DatabaseCategory::Document, 1),
        (DatabaseCategory::KeyValue, 2),
        (DatabaseCategory::ReservedGraph, 3),
        (DatabaseCategory::TimeSeries, 4),
        (DatabaseCategory::ReservedWideColumn, 5),
        (DatabaseCategory::LogStream, 6),
        (DatabaseCategory::ObjectStorage, 7),
    ];

    for (category, expected_index) in cases {
        let bytes = postcard::to_allocvec(&category).expect("encode category");
        assert_eq!(bytes, vec![expected_index], "{category:?}");

        let decoded: DatabaseCategory = postcard::from_bytes(&bytes).expect("decode category");
        assert_eq!(decoded, category);
    }
}
