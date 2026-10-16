use std::collections::HashMap;

use dbflux_core::{
    CollectionBrowseRequest, CollectionCountRequest, Connection, DbError, DocumentDelete,
    DocumentInsert, DocumentUpdate, KeyValueApi,
};
use dbflux_ipc::ProtocolVersion;
use dbflux_ipc::driver_protocol::{
    DriverRequestBody, DriverResponseBody, DriverRpcErrorCode, QueryResultDto, QueryResultSetsDto,
    protocol_carries_request_row_limit, protocol_carries_result_sets,
};
use uuid::Uuid;

/// Manages active sessions, each backed by a real `Connection`.
pub(crate) struct SessionManager {
    sessions: HashMap<Uuid, Box<dyn Connection>>,
}

impl SessionManager {
    pub(crate) fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }

    pub(crate) fn insert(&mut self, id: Uuid, connection: Box<dyn Connection>) {
        self.sessions.insert(id, connection);
    }

    pub(crate) fn remove(&mut self, id: &Uuid) -> Option<Box<dyn Connection>> {
        self.sessions.remove(id)
    }

    pub(crate) fn get(&self, id: &Uuid) -> Option<&dyn Connection> {
        self.sessions.get(id).map(|c| c.as_ref())
    }

    pub(crate) fn close_all(&mut self) {
        for (_, mut conn) in self.sessions.drain() {
            if let Err(e) = conn.close() {
                log::warn!("Error closing session: {e}");
            }
        }
    }
}

/// Maps a `DriverRequestBody` to the appropriate `Connection` method call
/// and returns the corresponding `DriverResponseBody`.
///
/// `Hello` and `OpenSession` are handled by the caller (main loop) since they
/// don't operate on an existing session. This function handles everything else.
/// `negotiated_version` is the version the connection settled on in `Hello`.
#[allow(clippy::result_large_err)]
pub(crate) fn dispatch(
    conn: &dyn Connection,
    negotiated_version: ProtocolVersion,
    body: DriverRequestBody,
) -> DriverResponseBody {
    let reports_truncation = protocol_carries_request_row_limit(negotiated_version);
    let carries_result_sets = protocol_carries_result_sets(negotiated_version);
    let browse_response = |result: Result<dbflux_core::QueryResult, DbError>| match result {
        Ok(result) if carries_result_sets => DriverResponseBody::BrowseResultSets {
            results: QueryResultSetsDto::from(&result),
        },
        Ok(result) => DriverResponseBody::BrowseResult {
            result: QueryResultDto::from(&result),
        },
        Err(e) => db_error_to_response(e),
    };
    match body {
        DriverRequestBody::Ping => match conn.ping() {
            Ok(()) => DriverResponseBody::Pong,
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::Schema => match conn.schema() {
            Ok(schema) => DriverResponseBody::Schema { schema },
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::Execute { request } => {
            let req = request.into();
            if let Err(error) = reject_protected_query(&req, conn, reports_truncation) {
                return db_error_to_response(error);
            }
            match conn.execute(&req) {
                Ok(result) if carries_result_sets => DriverResponseBody::ExecuteResultSets {
                    results: QueryResultSetsDto::from(&result),
                },
                Ok(result) if reports_truncation => {
                    DriverResponseBody::ExecuteResultWithTruncation {
                        result: QueryResultDto::from(&result),
                        rows_truncated: result.rows_truncated(),
                    }
                }
                Ok(result) => DriverResponseBody::ExecuteResult {
                    result: QueryResultDto::from(&result),
                },
                Err(e) => db_error_to_response(e),
            }
        }

        DriverRequestBody::ExecuteWithHandle { request } => {
            let req = request.into();
            if let Err(error) = reject_protected_query(&req, conn, reports_truncation) {
                return db_error_to_response(error);
            }
            match conn.execute_with_handle(&req) {
                Ok((handle, result)) if carries_result_sets => {
                    DriverResponseBody::ExecuteWithHandleResultSets {
                        handle_id: handle.id,
                        results: QueryResultSetsDto::from(&result),
                    }
                }
                Ok((handle, result)) if reports_truncation => {
                    DriverResponseBody::ExecuteWithHandleResultWithTruncation {
                        handle_id: handle.id,
                        result: QueryResultDto::from(&result),
                        rows_truncated: result.rows_truncated(),
                    }
                }
                Ok((handle, result)) => DriverResponseBody::ExecuteWithHandleResult {
                    handle_id: handle.id,
                    result: QueryResultDto::from(&result),
                },
                Err(e) => db_error_to_response(e),
            }
        }

        DriverRequestBody::Cancel { handle_id } => {
            let handle = dbflux_core::QueryHandle { id: handle_id };
            match conn.cancel(&handle) {
                Ok(()) => DriverResponseBody::Cancelled,
                Err(e) => db_error_to_response(e),
            }
        }

        DriverRequestBody::CancelActive => match conn.cancel_active() {
            Ok(()) => DriverResponseBody::Cancelled,
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::CleanupAfterCancel => match conn.cleanup_after_cancel() {
            Ok(()) => DriverResponseBody::CleanupComplete,
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::ListDatabases => match conn.list_databases() {
            Ok(databases) => DriverResponseBody::Databases { databases },
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::SchemaForDatabase { database } => {
            match conn.schema_for_database(&database) {
                Ok(schema) => DriverResponseBody::SchemaForDatabase { schema },
                Err(e) => db_error_to_response(e),
            }
        }

        DriverRequestBody::TableDetails {
            database,
            schema,
            table,
        } => match conn.table_details(&database, schema.as_deref(), &table) {
            Ok(table) => DriverResponseBody::TableDetails { table },
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::ViewDetails {
            database,
            schema,
            view,
        } => match conn.view_details(&database, schema.as_deref(), &view) {
            Ok(view) => DriverResponseBody::ViewDetails { view },
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::SetActiveDatabase { database } => {
            match conn.set_active_database(database.as_deref()) {
                Ok(()) => DriverResponseBody::ActiveDatabaseSet,
                Err(e) => db_error_to_response(e),
            }
        }

        DriverRequestBody::ActiveDatabase => DriverResponseBody::ActiveDatabaseResult {
            database: conn.active_database(),
        },

        // === Browse operations ===
        DriverRequestBody::BrowseTable { request } => browse_response(conn.browse_table(&request)),

        DriverRequestBody::CountTable { request } => match conn.count_table(&request) {
            Ok(count) => DriverResponseBody::CountResult { count },
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::BrowseCollection { request } => {
            let request: CollectionBrowseRequest = request.into();
            browse_response(conn.browse_collection(&request))
        }

        DriverRequestBody::CountCollection { request } => {
            let request: CollectionCountRequest = request.into();
            match conn.count_collection(&request) {
                Ok(count) => DriverResponseBody::CountResult { count },
                Err(e) => db_error_to_response(e),
            }
        }

        DriverRequestBody::Explain { request } => browse_response(conn.explain(&request)),

        DriverRequestBody::DescribeTable { request } => {
            browse_response(conn.describe_table(&request))
        }

        DriverRequestBody::PlanSemantic { request } => match conn.plan_semantic_request(&request) {
            Ok(plan) => DriverResponseBody::SemanticPlan { plan },
            Err(e) => db_error_to_response(e),
        },

        // === CRUD operations ===
        DriverRequestBody::UpdateRow { patch } => match conn.update_row(&patch) {
            Ok(result) => DriverResponseBody::CrudResult {
                result: result.into(),
            },
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::InsertRow { insert } => match conn.insert_row(&insert) {
            Ok(result) => DriverResponseBody::CrudResult {
                result: result.into(),
            },
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::DeleteRow { delete } => match conn.delete_row(&delete) {
            Ok(result) => DriverResponseBody::CrudResult {
                result: result.into(),
            },
            Err(e) => db_error_to_response(e),
        },

        // === Document mutations ===
        DriverRequestBody::UpdateDocument { update } => {
            let update: DocumentUpdate = update.into();
            match conn.update_document(&update) {
                Ok(result) => DriverResponseBody::CrudResult {
                    result: result.into(),
                },
                Err(e) => db_error_to_response(e),
            }
        }

        DriverRequestBody::InsertDocument { insert } => {
            let insert: DocumentInsert = insert.into();
            match conn.insert_document(&insert) {
                Ok(result) => DriverResponseBody::CrudResult {
                    result: result.into(),
                },
                Err(e) => db_error_to_response(e),
            }
        }

        DriverRequestBody::DeleteDocument { delete } => {
            let delete: DocumentDelete = delete.into();
            match conn.delete_document(&delete) {
                Ok(result) => DriverResponseBody::CrudResult {
                    result: result.into(),
                },
                Err(e) => db_error_to_response(e),
            }
        }

        // === Schema extras ===
        DriverRequestBody::SchemaTypes { database, schema } => {
            match conn.schema_types(&database, schema.as_deref()) {
                Ok(types) => DriverResponseBody::SchemaTypes { types },
                Err(e) => db_error_to_response(e),
            }
        }

        DriverRequestBody::SchemaIndexes { database, schema } => {
            match conn.schema_indexes(&database, schema.as_deref()) {
                Ok(indexes) => DriverResponseBody::SchemaIndexes { indexes },
                Err(e) => db_error_to_response(e),
            }
        }

        DriverRequestBody::SchemaForeignKeys { database, schema } => {
            match conn.schema_foreign_keys(&database, schema.as_deref()) {
                Ok(foreign_keys) => DriverResponseBody::SchemaForeignKeys { foreign_keys },
                Err(e) => db_error_to_response(e),
            }
        }

        DriverRequestBody::SchemaColumns { database, schema } => {
            match conn.schema_columns(&database, schema.as_deref()) {
                Ok(columns) => DriverResponseBody::SchemaColumns { columns },
                Err(e) => db_error_to_response(e),
            }
        }

        // === Key-Value operations ===
        DriverRequestBody::KvScanKeys { request } => dispatch_kv(conn, |kv| {
            kv.scan_keys(&request)
                .map(|page| DriverResponseBody::KvScanResult { page })
        }),

        DriverRequestBody::KvKeyCount { keyspace } => dispatch_kv(conn, |kv| {
            kv.key_count(keyspace)
                .map(|count| DriverResponseBody::KvKeyCountResult { count })
        }),

        DriverRequestBody::KvGetKey { request } => dispatch_kv(conn, |kv| {
            kv.get_key(&request)
                .map(|result| DriverResponseBody::KvGetResult { result })
        }),

        DriverRequestBody::KvSetKey { request } => dispatch_kv(conn, |kv| {
            kv.set_key(&request)
                .map(|()| DriverResponseBody::KvBoolResult { value: true })
        }),

        DriverRequestBody::KvDeleteKey { request } => dispatch_kv(conn, |kv| {
            kv.delete_key(&request)
                .map(|value| DriverResponseBody::KvBoolResult { value })
        }),

        DriverRequestBody::KvExistsKey { request } => dispatch_kv(conn, |kv| {
            kv.exists_key(&request)
                .map(|value| DriverResponseBody::KvBoolResult { value })
        }),

        DriverRequestBody::KvKeyType { request } => dispatch_kv(conn, |kv| {
            kv.key_type(&request).map(|kt| {
                let value = serde_json::to_string(&kt).unwrap_or_else(|_| "\"String\"".to_string());
                DriverResponseBody::KvStringResult { value }
            })
        }),

        DriverRequestBody::KvKeyTtl { request } => dispatch_kv(conn, |kv| {
            kv.key_ttl(&request).map(|ttl| {
                let value = serde_json::to_string(&ttl).unwrap_or_else(|_| "null".to_string());
                DriverResponseBody::KvStringResult { value }
            })
        }),

        DriverRequestBody::KvExpireKey { request } => dispatch_kv(conn, |kv| {
            kv.expire_key(&request)
                .map(|value| DriverResponseBody::KvBoolResult { value })
        }),

        DriverRequestBody::KvPersistKey { request } => dispatch_kv(conn, |kv| {
            kv.persist_key(&request)
                .map(|value| DriverResponseBody::KvBoolResult { value })
        }),

        DriverRequestBody::KvRenameKey { request } => dispatch_kv(conn, |kv| {
            kv.rename_key(&request)
                .map(|()| DriverResponseBody::KvBoolResult { value: true })
        }),

        DriverRequestBody::KvBulkGet { request } => dispatch_kv(conn, |kv| {
            kv.bulk_get(&request)
                .map(|results| DriverResponseBody::KvBulkGetResult { results })
        }),

        DriverRequestBody::KvHashSet { request } => dispatch_kv(conn, |kv| {
            kv.hash_set(&request)
                .map(|()| DriverResponseBody::KvBoolResult { value: true })
        }),

        DriverRequestBody::KvHashDelete { request } => dispatch_kv(conn, |kv| {
            kv.hash_delete(&request)
                .map(|value| DriverResponseBody::KvBoolResult { value })
        }),

        DriverRequestBody::KvListSet { request } => dispatch_kv(conn, |kv| {
            kv.list_set(&request)
                .map(|()| DriverResponseBody::KvBoolResult { value: true })
        }),

        DriverRequestBody::KvListPush { request } => dispatch_kv(conn, |kv| {
            kv.list_push(&request)
                .map(|()| DriverResponseBody::KvBoolResult { value: true })
        }),

        DriverRequestBody::KvListRemove { request } => dispatch_kv(conn, |kv| {
            kv.list_remove(&request)
                .map(|value| DriverResponseBody::KvBoolResult { value })
        }),

        DriverRequestBody::KvSetAdd { request } => dispatch_kv(conn, |kv| {
            kv.set_add(&request)
                .map(|value| DriverResponseBody::KvBoolResult { value })
        }),

        DriverRequestBody::KvSetRemove { request } => dispatch_kv(conn, |kv| {
            kv.set_remove(&request)
                .map(|value| DriverResponseBody::KvBoolResult { value })
        }),

        DriverRequestBody::KvZSetAdd { request } => dispatch_kv(conn, |kv| {
            kv.zset_add(&request)
                .map(|value| DriverResponseBody::KvBoolResult { value })
        }),

        DriverRequestBody::KvZSetRemove { request } => dispatch_kv(conn, |kv| {
            kv.zset_remove(&request)
                .map(|value| DriverResponseBody::KvBoolResult { value })
        }),

        DriverRequestBody::KvStreamAdd { request } => dispatch_kv(conn, |kv| {
            kv.stream_add(&request)
                .map(|value| DriverResponseBody::KvStringResult { value })
        }),

        DriverRequestBody::KvStreamDelete { request } => dispatch_kv(conn, |kv| {
            kv.stream_delete(&request)
                .map(|value| DriverResponseBody::KvU64Result { value })
        }),

        // === Code generation ===
        DriverRequestBody::CodeGenerators => {
            let generators = conn.code_generators();
            DriverResponseBody::CodeGeneratorsResult { generators }
        }

        DriverRequestBody::GenerateCode {
            generator_id,
            table,
        } => match conn.generate_code(&generator_id, &table) {
            Ok(code) => DriverResponseBody::GenerateCodeResult { code },
            Err(e) => db_error_to_response(e),
        },

        // Hello/OpenSession/CloseSession are handled by the main loop, not here.
        DriverRequestBody::Hello(_)
        | DriverRequestBody::OpenSession { .. }
        | DriverRequestBody::CloseSession => rpc_error(
            DriverRpcErrorCode::InvalidRequest,
            "Request type handled at session level, not dispatch level",
        ),
    }
}

/// Helper to dispatch KV operations, returning NotSupported if no KV API.
fn dispatch_kv(
    conn: &dyn Connection,
    f: impl FnOnce(&dyn KeyValueApi) -> Result<DriverResponseBody, DbError>,
) -> DriverResponseBody {
    match conn.key_value_api() {
        Some(kv) => match f(kv) {
            Ok(resp) => resp,
            Err(e) => db_error_to_response(e),
        },
        None => rpc_error(
            DriverRpcErrorCode::UnsupportedMethod,
            "Driver does not support key-value operations",
        ),
    }
}

/// Refuses what the wrapped driver cannot be shown to enforce before it runs.
/// A row limit passes only on a session that reports truncation back (v1.6+)
/// and only to a driver whose metadata declares `REQUEST_ROW_LIMIT`.
#[allow(clippy::result_large_err)]
fn reject_protected_query(
    request: &dbflux_core::QueryRequest,
    conn: &dyn Connection,
    reports_truncation: bool,
) -> Result<(), DbError> {
    let limit_enforced = reports_truncation
        && conn
            .metadata()
            .supports(dbflux_core::DriverCapabilities::REQUEST_ROW_LIMIT);

    if (request.limit.is_some() && !limit_enforced) || request.statement_timeout.is_some() {
        return Err(DbError::NotSupported(
            "IPC drivers cannot certify query limit or statement timeout enforcement".into(),
        ));
    }
    Ok(())
}

fn db_error_to_response(err: DbError) -> DriverResponseBody {
    let (code, retriable) = match &err {
        DbError::Timeout => (DriverRpcErrorCode::Timeout, true),
        DbError::NotSupported(_) => (DriverRpcErrorCode::UnsupportedMethod, false),
        DbError::ConnectionFailed(_) => (DriverRpcErrorCode::Transport, true),
        _ => (DriverRpcErrorCode::Driver, false),
    };

    DriverResponseBody::Error(dbflux_ipc::driver_protocol::DriverRpcError {
        code,
        message: err.to_string(),
        retriable,
    })
}

fn rpc_error(code: DriverRpcErrorCode, message: &str) -> DriverResponseBody {
    DriverResponseBody::Error(dbflux_ipc::driver_protocol::DriverRpcError {
        code,
        message: message.to_string(),
        retriable: false,
    })
}

#[cfg(test)]
mod tests {
    use super::{DriverRpcErrorCode, dispatch};
    use dbflux_core::{
        Connection, DatabaseCategory, DbError, DbKind, DefaultSqlDialect, DriverMetadataBuilder,
        QueryLanguage, QueryRequest, QueryResult, SchemaLoadingStrategy, SchemaSnapshot,
        SemanticPlan, SemanticPlanKind, SemanticRequest, TableBrowseRequest, TableRef,
    };
    use dbflux_ipc::driver_protocol::{DriverRequestBody, DriverResponseBody};
    use dbflux_ipc::{DRIVER_RPC_V1_5, DRIVER_RPC_V1_6, DRIVER_RPC_V1_7, DRIVER_RPC_VERSION};
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    struct SemanticPlanConnection {
        metadata: dbflux_core::DriverMetadata,
        plan: SemanticPlan,
    }

    impl SemanticPlanConnection {
        fn new(plan: SemanticPlan) -> Self {
            Self {
                metadata: DriverMetadataBuilder::new(
                    "test",
                    "Test",
                    DatabaseCategory::Relational,
                    QueryLanguage::Sql,
                )
                .build(),
                plan,
            }
        }
    }

    impl Connection for SemanticPlanConnection {
        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            &self.metadata
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, _req: &QueryRequest) -> Result<QueryResult, DbError> {
            Err(DbError::NotSupported("execute not used in test".into()))
        }

        fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Ok(SchemaSnapshot::default())
        }

        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::SingleDatabase
        }

        fn kind(&self) -> DbKind {
            DbKind::SQLite
        }

        fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
            &DefaultSqlDialect
        }

        fn plan_semantic_request(
            &self,
            _request: &SemanticRequest,
        ) -> Result<SemanticPlan, DbError> {
            Ok(self.plan.clone())
        }
    }

    struct UnsupportedSemanticPlanConnection {
        metadata: dbflux_core::DriverMetadata,
    }

    impl UnsupportedSemanticPlanConnection {
        fn new() -> Self {
            Self {
                metadata: DriverMetadataBuilder::new(
                    "test",
                    "Test",
                    DatabaseCategory::Relational,
                    QueryLanguage::Sql,
                )
                .build(),
            }
        }
    }

    impl Connection for UnsupportedSemanticPlanConnection {
        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            &self.metadata
        }

        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn execute(&self, _req: &QueryRequest) -> Result<QueryResult, DbError> {
            Err(DbError::NotSupported("execute not used in test".into()))
        }

        fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Ok(SchemaSnapshot::default())
        }

        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::SingleDatabase
        }

        fn kind(&self) -> DbKind {
            DbKind::SQLite
        }

        fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
            &DefaultSqlDialect
        }

        fn plan_semantic_request(
            &self,
            _request: &SemanticRequest,
        ) -> Result<SemanticPlan, DbError> {
            Err(DbError::NotSupported(
                "semantic planning is intentionally unsupported".into(),
            ))
        }
    }

    struct CountingConnection {
        metadata: dbflux_core::DriverMetadata,
        executes: Arc<AtomicUsize>,
        handles: Arc<AtomicUsize>,
    }

    impl Connection for CountingConnection {
        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            &self.metadata
        }
        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }
        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }
        fn execute(&self, _req: &QueryRequest) -> Result<QueryResult, DbError> {
            self.executes.fetch_add(1, Ordering::SeqCst);
            Ok(QueryResult::empty())
        }
        fn execute_with_handle(
            &self,
            _req: &QueryRequest,
        ) -> Result<(dbflux_core::QueryHandle, QueryResult), DbError> {
            self.handles.fetch_add(1, Ordering::SeqCst);
            Ok((
                dbflux_core::QueryHandle {
                    id: uuid::Uuid::nil(),
                },
                QueryResult::empty(),
            ))
        }
        fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
            Ok(())
        }
        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Ok(SchemaSnapshot::default())
        }
        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::SingleDatabase
        }
        fn kind(&self) -> DbKind {
            DbKind::SQLite
        }
        fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
            &DefaultSqlDialect
        }
    }

    #[test]
    fn protected_host_calls_do_not_reach_driver() {
        let executes = Arc::new(AtomicUsize::new(0));
        let handles = Arc::new(AtomicUsize::new(0));
        let connection = CountingConnection {
            metadata: DriverMetadataBuilder::new(
                "test",
                "Test",
                DatabaseCategory::Relational,
                QueryLanguage::Sql,
            )
            .build(),
            executes: executes.clone(),
            handles: handles.clone(),
        };
        for mut request in [
            QueryRequest::new("SELECT 1").with_limit(0),
            QueryRequest::new("SELECT 1"),
        ] {
            if request.limit.is_none() {
                request.statement_timeout = Some(Duration::ZERO);
            }
            for body in [
                DriverRequestBody::Execute {
                    request: (&request).into(),
                },
                DriverRequestBody::ExecuteWithHandle {
                    request: (&request).into(),
                },
            ] {
                match dispatch(&connection, DRIVER_RPC_V1_5, body) {
                    DriverResponseBody::Error(error) => {
                        assert_eq!(error.code, DriverRpcErrorCode::UnsupportedMethod);
                        assert!(!error.retriable);
                        assert!(error.message.contains("cannot certify"));
                    }
                    other => panic!("expected protected request rejection, got {other:?}"),
                }
            }
        }
        assert_eq!(executes.load(Ordering::SeqCst), 0);
        assert_eq!(handles.load(Ordering::SeqCst), 0);
        assert!(matches!(
            dispatch(
                &connection,
                DRIVER_RPC_V1_5,
                DriverRequestBody::Execute {
                    request: (&QueryRequest::new("SELECT 1")).into()
                }
            ),
            DriverResponseBody::ExecuteResult { .. }
        ));
        assert!(matches!(
            dispatch(
                &connection,
                DRIVER_RPC_V1_5,
                DriverRequestBody::ExecuteWithHandle {
                    request: (&QueryRequest::new("SELECT 1")).into()
                }
            ),
            DriverResponseBody::ExecuteWithHandleResult { .. }
        ));
        assert_eq!(executes.load(Ordering::SeqCst), 1);
        assert_eq!(handles.load(Ordering::SeqCst), 1);
    }

    /// Driver that declares `REQUEST_ROW_LIMIT`, records every limit it
    /// receives, and marks its result truncated whenever a limit was set.
    struct LimitingConnection {
        metadata: dbflux_core::DriverMetadata,
        received_limits: Arc<Mutex<Vec<Option<u32>>>>,
    }

    impl LimitingConnection {
        fn new(capabilities: dbflux_core::DriverCapabilities) -> Self {
            Self {
                metadata: DriverMetadataBuilder::new(
                    "test",
                    "Test",
                    DatabaseCategory::Relational,
                    QueryLanguage::Sql,
                )
                .capabilities(capabilities)
                .build(),
                received_limits: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn run(&self, request: &QueryRequest) -> QueryResult {
            self.received_limits
                .lock()
                .expect("limits lock")
                .push(request.limit);
            let mut result = QueryResult::empty();
            result.set_rows_truncated(request.limit.is_some());
            result
        }

        fn received_limits(&self) -> Vec<Option<u32>> {
            self.received_limits.lock().expect("limits lock").clone()
        }
    }

    impl Connection for LimitingConnection {
        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            &self.metadata
        }
        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }
        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }
        fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
            Ok(self.run(req))
        }
        fn execute_with_handle(
            &self,
            req: &QueryRequest,
        ) -> Result<(dbflux_core::QueryHandle, QueryResult), DbError> {
            Ok((
                dbflux_core::QueryHandle {
                    id: uuid::Uuid::nil(),
                },
                self.run(req),
            ))
        }
        fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
            Ok(())
        }
        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Ok(SchemaSnapshot::default())
        }
        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::SingleDatabase
        }
        fn kind(&self) -> DbKind {
            DbKind::SQLite
        }
        fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
            &DefaultSqlDialect
        }
    }

    /// Answers every query, execute and browse call with three result sets,
    /// the first and last of them truncated.
    struct MultiSetConnection {
        metadata: dbflux_core::DriverMetadata,
    }

    impl MultiSetConnection {
        fn new() -> Self {
            Self {
                metadata: DriverMetadataBuilder::new(
                    "test",
                    "Test",
                    DatabaseCategory::Relational,
                    QueryLanguage::Sql,
                )
                .build(),
            }
        }

        fn result_sets() -> QueryResult {
            let labelled = |label: &str, truncated: bool| {
                let mut result = QueryResult::text(label.to_string(), Duration::from_millis(1));
                result.set_rows_truncated(truncated);
                result
            };
            let mut primary = labelled("primary", true);
            primary.push_additional_result(labelled("second", false));
            primary.push_additional_result(labelled("third", true));
            primary
        }
    }

    impl Connection for MultiSetConnection {
        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            &self.metadata
        }
        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }
        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }
        fn execute(&self, _req: &QueryRequest) -> Result<QueryResult, DbError> {
            Ok(Self::result_sets())
        }
        fn execute_with_handle(
            &self,
            _req: &QueryRequest,
        ) -> Result<(dbflux_core::QueryHandle, QueryResult), DbError> {
            Ok((
                dbflux_core::QueryHandle {
                    id: uuid::Uuid::nil(),
                },
                Self::result_sets(),
            ))
        }
        fn browse_table(&self, _request: &TableBrowseRequest) -> Result<QueryResult, DbError> {
            Ok(Self::result_sets())
        }
        fn browse_collection(
            &self,
            _request: &dbflux_core::CollectionBrowseRequest,
        ) -> Result<QueryResult, DbError> {
            Ok(Self::result_sets())
        }
        fn explain(&self, _request: &dbflux_core::ExplainRequest) -> Result<QueryResult, DbError> {
            Ok(Self::result_sets())
        }
        fn describe_table(
            &self,
            _request: &dbflux_core::DescribeRequest,
        ) -> Result<QueryResult, DbError> {
            Ok(Self::result_sets())
        }
        fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
            Ok(())
        }
        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Ok(SchemaSnapshot::default())
        }
        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::SingleDatabase
        }
        fn kind(&self) -> DbKind {
            DbKind::SQLite
        }
        fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
            &DefaultSqlDialect
        }
    }

    /// Every request whose answer carries a `QueryResult`.
    fn result_set_bodies() -> Vec<DriverRequestBody> {
        let table = TableRef::new("events");
        let mut bodies = execute_bodies(&QueryRequest::new("SELECT 1; SELECT 2")).to_vec();
        bodies.extend([
            DriverRequestBody::BrowseTable {
                request: TableBrowseRequest::new(table.clone()),
            },
            DriverRequestBody::BrowseCollection {
                request: dbflux_core::CollectionBrowseRequest::new(
                    dbflux_core::CollectionRef::new("app", "events"),
                )
                .into(),
            },
            DriverRequestBody::Explain {
                request: dbflux_core::ExplainRequest::new(table.clone()),
            },
            DriverRequestBody::DescribeTable {
                request: dbflux_core::DescribeRequest::new(table),
            },
        ]);
        bodies
    }

    #[test]
    fn v1_7_host_sends_every_result_set_with_its_truncation_flag() {
        let connection = MultiSetConnection::new();

        for body in result_set_bodies() {
            let results = match dispatch(&connection, DRIVER_RPC_V1_7, body) {
                DriverResponseBody::ExecuteResultSets { results }
                | DriverResponseBody::BrowseResultSets { results } => results,
                DriverResponseBody::ExecuteWithHandleResultSets { handle_id, results } => {
                    assert_eq!(handle_id, uuid::Uuid::nil());
                    results
                }
                other => panic!("expected a result-sets response, got {other:?}"),
            };
            let restored = QueryResult::from(results);
            let sets: Vec<(Option<&str>, bool)> = restored
                .iter_result_sets()
                .map(|set| (set.text_body.as_deref(), set.rows_truncated()))
                .collect();
            assert_eq!(
                sets,
                vec![
                    (Some("primary"), true),
                    (Some("second"), false),
                    (Some("third"), true),
                ]
            );
        }
    }

    #[test]
    fn v1_6_host_keeps_the_v1_6_responses_for_a_v1_6_peer() {
        let connection = MultiSetConnection::new();

        let responses: Vec<DriverResponseBody> = result_set_bodies()
            .into_iter()
            .map(|body| dispatch(&connection, DRIVER_RPC_V1_6, body))
            .collect();

        match &responses[..] {
            [
                DriverResponseBody::ExecuteResultWithTruncation {
                    result: execute,
                    rows_truncated: true,
                },
                DriverResponseBody::ExecuteWithHandleResultWithTruncation {
                    result: execute_with_handle,
                    rows_truncated: true,
                    ..
                },
                DriverResponseBody::BrowseResult {
                    result: browse_table,
                },
                DriverResponseBody::BrowseResult {
                    result: browse_collection,
                },
                DriverResponseBody::BrowseResult { result: explain },
                DriverResponseBody::BrowseResult { result: describe },
            ] => {
                for result in [
                    execute,
                    execute_with_handle,
                    browse_table,
                    browse_collection,
                    explain,
                    describe,
                ] {
                    assert_eq!(result.text_body.as_deref(), Some("primary"));
                }
            }
            other => panic!("expected the v1.6 responses, got {other:?}"),
        }
    }

    fn execute_bodies(request: &QueryRequest) -> [DriverRequestBody; 2] {
        [
            DriverRequestBody::Execute {
                request: request.into(),
            },
            DriverRequestBody::ExecuteWithHandle {
                request: request.into(),
            },
        ]
    }

    fn assert_refused(response: DriverResponseBody) {
        match response {
            DriverResponseBody::Error(error) => {
                assert_eq!(error.code, DriverRpcErrorCode::UnsupportedMethod);
                assert!(!error.retriable);
                assert!(error.message.contains("cannot certify"));
            }
            other => panic!("expected protected request rejection, got {other:?}"),
        }
    }

    #[test]
    fn v1_6_host_forwards_limit_to_a_declaring_driver_and_reports_truncation() {
        let connection =
            LimitingConnection::new(dbflux_core::DriverCapabilities::REQUEST_ROW_LIMIT);

        let [execute, execute_with_handle] =
            execute_bodies(&QueryRequest::new("SELECT 1").with_limit(3));
        match dispatch(&connection, DRIVER_RPC_V1_6, execute) {
            DriverResponseBody::ExecuteResultWithTruncation { rows_truncated, .. } => {
                assert!(rows_truncated);
            }
            other => panic!("expected a truncation-carrying result, got {other:?}"),
        }
        match dispatch(&connection, DRIVER_RPC_V1_6, execute_with_handle) {
            DriverResponseBody::ExecuteWithHandleResultWithTruncation {
                rows_truncated, ..
            } => {
                assert!(rows_truncated);
            }
            other => panic!("expected a truncation-carrying result, got {other:?}"),
        }

        let [execute, _] = execute_bodies(&QueryRequest::new("SELECT 1"));
        match dispatch(&connection, DRIVER_RPC_V1_6, execute) {
            DriverResponseBody::ExecuteResultWithTruncation { rows_truncated, .. } => {
                assert!(!rows_truncated);
            }
            other => panic!("expected a truncation-carrying result, got {other:?}"),
        }

        assert_eq!(connection.received_limits(), vec![Some(3), Some(3), None]);
    }

    #[test]
    fn v1_6_host_still_refuses_statement_timeout() {
        let connection =
            LimitingConnection::new(dbflux_core::DriverCapabilities::REQUEST_ROW_LIMIT);
        let mut request = QueryRequest::new("SELECT 1").with_limit(3);
        request.statement_timeout = Some(Duration::ZERO);

        for body in execute_bodies(&request) {
            assert_refused(dispatch(&connection, DRIVER_RPC_V1_6, body));
        }
        assert!(connection.received_limits().is_empty());
    }

    #[test]
    fn v1_6_host_refuses_limit_for_a_driver_that_does_not_declare_it() {
        let connection = LimitingConnection::new(dbflux_core::DriverCapabilities::empty());

        for body in execute_bodies(&QueryRequest::new("SELECT 1").with_limit(3)) {
            assert_refused(dispatch(&connection, DRIVER_RPC_V1_6, body));
        }
        assert!(connection.received_limits().is_empty());
    }

    #[test]
    fn v1_5_host_refuses_limit_even_for_a_declaring_driver() {
        let connection =
            LimitingConnection::new(dbflux_core::DriverCapabilities::REQUEST_ROW_LIMIT);

        for body in execute_bodies(&QueryRequest::new("SELECT 1").with_limit(3)) {
            assert_refused(dispatch(&connection, DRIVER_RPC_V1_5, body));
        }
        assert!(connection.received_limits().is_empty());

        // A 1.5 peer cannot decode the 1.6 variants, so it keeps the old ones.
        let [execute, execute_with_handle] = execute_bodies(&QueryRequest::new("SELECT 1"));
        assert!(matches!(
            dispatch(&connection, DRIVER_RPC_V1_5, execute),
            DriverResponseBody::ExecuteResult { .. }
        ));
        assert!(matches!(
            dispatch(&connection, DRIVER_RPC_V1_5, execute_with_handle),
            DriverResponseBody::ExecuteWithHandleResult { .. }
        ));
    }

    #[test]
    fn dispatch_returns_semantic_plan_response() {
        let plan = SemanticPlan::single_query(
            SemanticPlanKind::Query,
            dbflux_core::PlannedQuery::new(QueryLanguage::Sql, "SELECT 1"),
        );
        let connection = SemanticPlanConnection::new(plan.clone());

        let response = dispatch(
            &connection,
            DRIVER_RPC_VERSION,
            DriverRequestBody::PlanSemantic {
                request: SemanticRequest::TableBrowse(TableBrowseRequest::new(TableRef::new(
                    "users",
                ))),
            },
        );

        match response {
            DriverResponseBody::SemanticPlan { plan: actual } => assert_eq!(actual, plan),
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[test]
    fn dispatch_maps_unsupported_semantic_plans_to_rpc_error() {
        let connection = UnsupportedSemanticPlanConnection::new();

        let response = dispatch(
            &connection,
            DRIVER_RPC_VERSION,
            DriverRequestBody::PlanSemantic {
                request: SemanticRequest::TableBrowse(TableBrowseRequest::new(TableRef::new(
                    "users",
                ))),
            },
        );

        match response {
            DriverResponseBody::Error(error) => {
                assert_eq!(
                    error.code,
                    dbflux_ipc::driver_protocol::DriverRpcErrorCode::UnsupportedMethod
                );
                assert!(error.message.contains("intentionally unsupported"));
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }
}
