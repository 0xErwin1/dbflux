use std::sync::{Arc, OnceLock};

use dbflux_core::{
    CodeGenCapabilities, CollectionBrowseRequest, CollectionCountRequest, Connection, CrudResult,
    CustomTypeInfo, DatabaseInfo, DbError, DbKind, DbSchemaInfo, DescribeRequest, DocumentDelete,
    DocumentInsert, DocumentUpdate, DriverCapabilities, DriverMetadata, ExplainRequest,
    HashDeleteRequest, HashSetRequest, KeyBulkGetRequest, KeyDeleteRequest, KeyExistsRequest,
    KeyExpireRequest, KeyGetRequest, KeyGetResult, KeyPersistRequest, KeyRenameRequest,
    KeyScanPage, KeyScanRequest, KeySetRequest, KeyTtlRequest, KeyType, KeyTypeRequest,
    KeyValueApi, LanguageService, ListPushRequest, ListRemoveRequest, ListSetRequest, QueryHandle,
    QueryRequest, QueryResult, RowDelete, RowInsert, RowPatch, SchemaFeatures,
    SchemaForeignKeyInfo, SchemaIndexInfo, SchemaLoadingStrategy, SchemaSnapshot, SetAddRequest,
    SetRemoveRequest, SqlDialect, StreamAddRequest, StreamDeleteRequest, TableBrowseRequest,
    TableCountRequest, TableInfo, ViewInfo, ZSetAddRequest, ZSetRemoveRequest,
};
use dbflux_ipc::driver_protocol::{CodeGeneratorInfoDto, DriverRequestBody, DriverResponseBody};

use crate::transport::RpcClient;
use uuid::Uuid;

/// IPC-proxied connection that delegates all operations to a remote driver-host.
pub struct IpcConnection {
    client: Arc<RpcClient>,
    session_id: Uuid,
    kind: DbKind,
    metadata: &'static DriverMetadata,
    capabilities: DriverCapabilities,
    schema_loading_strategy: SchemaLoadingStrategy,
    schema_features: SchemaFeatures,
    code_gen_capabilities: CodeGenCapabilities,
    cached_code_generators: OnceLock<&'static [dbflux_core::CodeGeneratorInfo]>,
}

impl IpcConnection {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Arc<RpcClient>,
        session_id: Uuid,
        kind: DbKind,
        metadata: &'static DriverMetadata,
        capabilities: DriverCapabilities,
        schema_loading_strategy: SchemaLoadingStrategy,
        schema_features: SchemaFeatures,
        code_gen_capabilities: CodeGenCapabilities,
    ) -> Self {
        Self {
            client,
            session_id,
            kind,
            metadata,
            capabilities,
            schema_loading_strategy,
            schema_features,
            code_gen_capabilities,
            cached_code_generators: OnceLock::new(),
        }
    }

    #[allow(clippy::result_large_err)]
    fn kv_call(&self, body: DriverRequestBody) -> Result<DriverResponseBody, DbError> {
        self.client
            .kv_call(self.session_id, body)
            .map_err(DbError::from)
    }

    #[allow(clippy::result_large_err)]
    fn expect_kv_bool(&self, body: DriverRequestBody) -> Result<bool, DbError> {
        match self.kv_call(body)? {
            DriverResponseBody::KvBoolResult { value } => Ok(value),
            DriverResponseBody::Error(e) => Err(DbError::QueryFailed(e.message.into())),
            _ => Err(DbError::QueryFailed("Unexpected KV response".into())),
        }
    }

    #[allow(clippy::result_large_err)]
    fn expect_ok(&self, body: DriverRequestBody) -> Result<(), DbError> {
        match self.kv_call(body)? {
            DriverResponseBody::KvBoolResult { .. }
            | DriverResponseBody::Pong
            | DriverResponseBody::SessionClosed => Ok(()),
            DriverResponseBody::Error(e) => Err(DbError::QueryFailed(e.message.into())),
            _ => Err(DbError::QueryFailed("Unexpected KV response".into())),
        }
    }
}

impl Connection for IpcConnection {
    fn metadata(&self) -> &'static DriverMetadata {
        self.metadata
    }

    fn capabilities(&self) -> DriverCapabilities {
        self.capabilities
    }

    fn kind(&self) -> DbKind {
        self.kind
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        self.schema_loading_strategy
    }

    fn schema_features(&self) -> SchemaFeatures {
        self.schema_features
    }

    fn ping(&self) -> Result<(), DbError> {
        self.client.ping(self.session_id).map_err(DbError::from)
    }

    fn close(&mut self) -> Result<(), DbError> {
        self.client
            .close_session(self.session_id)
            .map_err(DbError::from)
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        self.client
            .execute(self.session_id, req)
            .map_err(DbError::from)
    }

    fn execute_with_handle(
        &self,
        req: &QueryRequest,
    ) -> Result<(QueryHandle, QueryResult), DbError> {
        let (handle_id, result) = self
            .client
            .execute_with_handle(self.session_id, req)
            .map_err(DbError::from)?;

        Ok((QueryHandle { id: handle_id }, result))
    }

    fn cancel(&self, handle: &QueryHandle) -> Result<(), DbError> {
        self.client
            .cancel(self.session_id, handle.id)
            .map_err(DbError::from)
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        self.client
            .cancel_active(self.session_id)
            .map_err(DbError::from)
    }

    fn cleanup_after_cancel(&self) -> Result<(), DbError> {
        self.client
            .cleanup_after_cancel(self.session_id)
            .map_err(DbError::from)
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        self.client.schema(self.session_id).map_err(DbError::from)
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        self.client
            .list_databases(self.session_id)
            .map_err(DbError::from)
    }

    fn schema_for_database(&self, database: &str) -> Result<DbSchemaInfo, DbError> {
        self.client
            .schema_for_database(self.session_id, database)
            .map_err(DbError::from)
    }

    fn table_details(
        &self,
        database: &str,
        schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, DbError> {
        self.client
            .table_details(self.session_id, database, schema, table)
            .map_err(DbError::from)
    }

    fn view_details(
        &self,
        database: &str,
        schema: Option<&str>,
        view: &str,
    ) -> Result<ViewInfo, DbError> {
        self.client
            .view_details(self.session_id, database, schema, view)
            .map_err(DbError::from)
    }

    fn set_active_database(&self, database: Option<&str>) -> Result<(), DbError> {
        self.client
            .set_active_database(self.session_id, database)
            .map_err(DbError::from)
    }

    fn active_database(&self) -> Option<String> {
        self.client.active_database(self.session_id).ok().flatten()
    }

    fn schema_types(
        &self,
        database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<CustomTypeInfo>, DbError> {
        self.client
            .schema_types(self.session_id, database, schema)
            .map_err(DbError::from)
    }

    fn schema_indexes(
        &self,
        database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<SchemaIndexInfo>, DbError> {
        self.client
            .schema_indexes(self.session_id, database, schema)
            .map_err(DbError::from)
    }

    fn schema_foreign_keys(
        &self,
        database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
        self.client
            .schema_foreign_keys(self.session_id, database, schema)
            .map_err(DbError::from)
    }

    fn browse_table(&self, request: &TableBrowseRequest) -> Result<QueryResult, DbError> {
        self.client
            .browse_table(self.session_id, request.clone())
            .map_err(DbError::from)
    }

    fn count_table(&self, request: &TableCountRequest) -> Result<u64, DbError> {
        self.client
            .count_table(self.session_id, request.clone())
            .map_err(DbError::from)
    }

    fn browse_collection(&self, request: &CollectionBrowseRequest) -> Result<QueryResult, DbError> {
        self.client
            .browse_collection(self.session_id, request.clone())
            .map_err(DbError::from)
    }

    fn count_collection(&self, request: &CollectionCountRequest) -> Result<u64, DbError> {
        self.client
            .count_collection(self.session_id, request.clone())
            .map_err(DbError::from)
    }

    fn explain(&self, request: &ExplainRequest) -> Result<QueryResult, DbError> {
        self.client
            .explain(self.session_id, request.clone())
            .map_err(DbError::from)
    }

    fn describe_table(&self, request: &DescribeRequest) -> Result<QueryResult, DbError> {
        self.client
            .describe_table(self.session_id, request.clone())
            .map_err(DbError::from)
    }

    fn update_row(&self, patch: &RowPatch) -> Result<CrudResult, DbError> {
        self.client
            .update_row(self.session_id, patch.clone())
            .map_err(DbError::from)
    }

    fn insert_row(&self, insert: &RowInsert) -> Result<CrudResult, DbError> {
        self.client
            .insert_row(self.session_id, insert.clone())
            .map_err(DbError::from)
    }

    fn delete_row(&self, delete: &RowDelete) -> Result<CrudResult, DbError> {
        self.client
            .delete_row(self.session_id, delete.clone())
            .map_err(DbError::from)
    }

    fn update_document(&self, update: &DocumentUpdate) -> Result<CrudResult, DbError> {
        self.client
            .update_document(self.session_id, update.clone())
            .map_err(DbError::from)
    }

    fn insert_document(&self, insert: &DocumentInsert) -> Result<CrudResult, DbError> {
        self.client
            .insert_document(self.session_id, insert.clone())
            .map_err(DbError::from)
    }

    fn delete_document(&self, delete: &DocumentDelete) -> Result<CrudResult, DbError> {
        self.client
            .delete_document(self.session_id, delete.clone())
            .map_err(DbError::from)
    }

    fn key_value_api(&self) -> Option<&dyn KeyValueApi> {
        if self.capabilities.contains(DriverCapabilities::KV_SCAN) {
            Some(self as &dyn KeyValueApi)
        } else {
            None
        }
    }

    fn dialect(&self) -> &dyn SqlDialect {
        // The IPC connection uses the default ANSI SQL dialect.
        // The driver-host does all SQL generation server-side via browse/count/CRUD
        // methods, so the client-side dialect is only used as a fallback.
        &dbflux_core::DefaultSqlDialect
    }

    fn language_service(&self) -> &dyn LanguageService {
        // Language service runs client-side based on the query language from metadata.
        // This is correct because dangerous-query detection and validation are
        // syntactic operations that don't need a server round-trip.
        match self.metadata.query_language {
            dbflux_core::QueryLanguage::Sql => &dbflux_core::SqlLanguageService,
            _ => &dbflux_core::SqlLanguageService,
        }
    }

    fn code_gen_capabilities(&self) -> CodeGenCapabilities {
        self.code_gen_capabilities
    }

    fn code_generators(&self) -> &'static [dbflux_core::CodeGeneratorInfo] {
        self.cached_code_generators.get_or_init(|| {
            let generators = self
                .client
                .code_generators(self.session_id)
                .unwrap_or_default();

            let infos: Vec<dbflux_core::CodeGeneratorInfo> = generators
                .into_iter()
                .map(code_generator_info_from_dto)
                .collect();

            Box::leak(infos.into_boxed_slice())
        })
    }

    fn generate_code(&self, generator_id: &str, table: &TableInfo) -> Result<String, DbError> {
        self.client
            .generate_code(self.session_id, generator_id, table)
            .map_err(DbError::from)
    }
}

impl KeyValueApi for IpcConnection {
    fn scan_keys(&self, request: &KeyScanRequest) -> Result<KeyScanPage, DbError> {
        match self.kv_call(DriverRequestBody::KvScanKeys {
            request: request.clone(),
        })? {
            DriverResponseBody::KvScanResult { page } => Ok(page),
            DriverResponseBody::Error(e) => Err(DbError::QueryFailed(e.message.into())),
            _ => Err(DbError::QueryFailed("Unexpected KV response".into())),
        }
    }

    fn get_key(&self, request: &KeyGetRequest) -> Result<KeyGetResult, DbError> {
        match self.kv_call(DriverRequestBody::KvGetKey {
            request: request.clone(),
        })? {
            DriverResponseBody::KvGetResult { result } => Ok(result),
            DriverResponseBody::Error(e) => Err(DbError::QueryFailed(e.message.into())),
            _ => Err(DbError::QueryFailed("Unexpected KV response".into())),
        }
    }

    fn set_key(&self, request: &KeySetRequest) -> Result<(), DbError> {
        self.expect_ok(DriverRequestBody::KvSetKey {
            request: request.clone(),
        })
    }

    fn delete_key(&self, request: &KeyDeleteRequest) -> Result<bool, DbError> {
        self.expect_kv_bool(DriverRequestBody::KvDeleteKey {
            request: request.clone(),
        })
    }

    fn exists_key(&self, request: &KeyExistsRequest) -> Result<bool, DbError> {
        self.expect_kv_bool(DriverRequestBody::KvExistsKey {
            request: request.clone(),
        })
    }

    fn key_type(&self, request: &KeyTypeRequest) -> Result<KeyType, DbError> {
        match self.kv_call(DriverRequestBody::KvKeyType {
            request: request.clone(),
        })? {
            DriverResponseBody::KvStringResult { value } => serde_json::from_str(&value)
                .map_err(|e| DbError::QueryFailed(format!("Failed to parse key type: {e}").into())),
            DriverResponseBody::Error(e) => Err(DbError::QueryFailed(e.message.into())),
            _ => Err(DbError::QueryFailed("Unexpected KV response".into())),
        }
    }

    fn key_ttl(&self, request: &KeyTtlRequest) -> Result<Option<i64>, DbError> {
        match self.kv_call(DriverRequestBody::KvKeyTtl {
            request: request.clone(),
        })? {
            DriverResponseBody::KvStringResult { value } => serde_json::from_str(&value)
                .map_err(|e| DbError::QueryFailed(format!("Failed to parse key TTL: {e}").into())),
            DriverResponseBody::Error(e) => Err(DbError::QueryFailed(e.message.into())),
            _ => Err(DbError::QueryFailed("Unexpected KV response".into())),
        }
    }

    fn expire_key(&self, request: &KeyExpireRequest) -> Result<bool, DbError> {
        self.expect_kv_bool(DriverRequestBody::KvExpireKey {
            request: request.clone(),
        })
    }

    fn persist_key(&self, request: &KeyPersistRequest) -> Result<bool, DbError> {
        self.expect_kv_bool(DriverRequestBody::KvPersistKey {
            request: request.clone(),
        })
    }

    fn rename_key(&self, request: &KeyRenameRequest) -> Result<(), DbError> {
        self.expect_ok(DriverRequestBody::KvRenameKey {
            request: request.clone(),
        })
    }

    fn bulk_get(&self, request: &KeyBulkGetRequest) -> Result<Vec<Option<KeyGetResult>>, DbError> {
        match self.kv_call(DriverRequestBody::KvBulkGet {
            request: request.clone(),
        })? {
            DriverResponseBody::KvBulkGetResult { results } => Ok(results),
            DriverResponseBody::Error(e) => Err(DbError::QueryFailed(e.message.into())),
            _ => Err(DbError::QueryFailed("Unexpected KV response".into())),
        }
    }

    fn hash_set(&self, request: &HashSetRequest) -> Result<(), DbError> {
        self.expect_ok(DriverRequestBody::KvHashSet {
            request: request.clone(),
        })
    }

    fn hash_delete(&self, request: &HashDeleteRequest) -> Result<bool, DbError> {
        self.expect_kv_bool(DriverRequestBody::KvHashDelete {
            request: request.clone(),
        })
    }

    fn list_set(&self, request: &ListSetRequest) -> Result<(), DbError> {
        self.expect_ok(DriverRequestBody::KvListSet {
            request: request.clone(),
        })
    }

    fn list_push(&self, request: &ListPushRequest) -> Result<(), DbError> {
        self.expect_ok(DriverRequestBody::KvListPush {
            request: request.clone(),
        })
    }

    fn list_remove(&self, request: &ListRemoveRequest) -> Result<bool, DbError> {
        self.expect_kv_bool(DriverRequestBody::KvListRemove {
            request: request.clone(),
        })
    }

    fn set_add(&self, request: &SetAddRequest) -> Result<bool, DbError> {
        self.expect_kv_bool(DriverRequestBody::KvSetAdd {
            request: request.clone(),
        })
    }

    fn set_remove(&self, request: &SetRemoveRequest) -> Result<bool, DbError> {
        self.expect_kv_bool(DriverRequestBody::KvSetRemove {
            request: request.clone(),
        })
    }

    fn zset_add(&self, request: &ZSetAddRequest) -> Result<bool, DbError> {
        self.expect_kv_bool(DriverRequestBody::KvZSetAdd {
            request: request.clone(),
        })
    }

    fn zset_remove(&self, request: &ZSetRemoveRequest) -> Result<bool, DbError> {
        self.expect_kv_bool(DriverRequestBody::KvZSetRemove {
            request: request.clone(),
        })
    }

    fn stream_add(&self, request: &StreamAddRequest) -> Result<String, DbError> {
        match self.kv_call(DriverRequestBody::KvStreamAdd {
            request: request.clone(),
        })? {
            DriverResponseBody::KvStringResult { value } => Ok(value),
            DriverResponseBody::Error(e) => Err(DbError::QueryFailed(e.message.into())),
            _ => Err(DbError::QueryFailed("Unexpected KV response".into())),
        }
    }

    fn stream_delete(&self, request: &StreamDeleteRequest) -> Result<u64, DbError> {
        match self.kv_call(DriverRequestBody::KvStreamDelete {
            request: request.clone(),
        })? {
            DriverResponseBody::KvU64Result { value } => Ok(value),
            DriverResponseBody::Error(e) => Err(DbError::QueryFailed(e.message.into())),
            _ => Err(DbError::QueryFailed("Unexpected KV response".into())),
        }
    }
}

fn code_generator_info_from_dto(dto: CodeGeneratorInfoDto) -> dbflux_core::CodeGeneratorInfo {
    dbflux_core::CodeGeneratorInfo {
        id: Box::leak(dto.id.into_boxed_str()),
        label: Box::leak(dto.label.into_boxed_str()),
        scope: dto.scope,
        order: dto.order,
        destructive: dto.destructive,
    }
}
