use std::collections::HashMap;

use dbflux_core::{Connection, DbError, KeyValueApi};
use dbflux_ipc::driver_protocol::{
    DriverRequestBody, DriverResponseBody, DriverRpcErrorCode, QueryResultDto,
};
use uuid::Uuid;

/// Manages active sessions, each backed by a real `Connection`.
pub struct SessionManager {
    sessions: HashMap<Uuid, Box<dyn Connection>>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }

    pub fn insert(&mut self, id: Uuid, connection: Box<dyn Connection>) {
        self.sessions.insert(id, connection);
    }

    pub fn remove(&mut self, id: &Uuid) -> Option<Box<dyn Connection>> {
        self.sessions.remove(id)
    }

    pub fn get(&self, id: &Uuid) -> Option<&dyn Connection> {
        self.sessions.get(id).map(|c| c.as_ref())
    }

    pub fn close_all(&mut self) {
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
pub fn dispatch(conn: &dyn Connection, body: DriverRequestBody) -> DriverResponseBody {
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
            match conn.execute(&req) {
                Ok(result) => DriverResponseBody::ExecuteResult {
                    result: QueryResultDto::from(&result),
                },
                Err(e) => db_error_to_response(e),
            }
        }

        DriverRequestBody::ExecuteWithHandle { request } => {
            let req = request.into();
            match conn.execute_with_handle(&req) {
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
        DriverRequestBody::BrowseTable { request } => match conn.browse_table(&request) {
            Ok(result) => DriverResponseBody::BrowseResult {
                result: QueryResultDto::from(&result),
            },
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::CountTable { request } => match conn.count_table(&request) {
            Ok(count) => DriverResponseBody::CountResult { count },
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::BrowseCollection { request } => match conn.browse_collection(&request) {
            Ok(result) => DriverResponseBody::BrowseResult {
                result: QueryResultDto::from(&result),
            },
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::CountCollection { request } => match conn.count_collection(&request) {
            Ok(count) => DriverResponseBody::CountResult { count },
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::Explain { request } => match conn.explain(&request) {
            Ok(result) => DriverResponseBody::BrowseResult {
                result: QueryResultDto::from(&result),
            },
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::DescribeTable { request } => match conn.describe_table(&request) {
            Ok(result) => DriverResponseBody::BrowseResult {
                result: QueryResultDto::from(&result),
            },
            Err(e) => db_error_to_response(e),
        },

        // === CRUD operations ===
        DriverRequestBody::UpdateRow { patch } => match conn.update_row(&patch) {
            Ok(result) => DriverResponseBody::CrudResult { result },
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::InsertRow { insert } => match conn.insert_row(&insert) {
            Ok(result) => DriverResponseBody::CrudResult { result },
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::DeleteRow { delete } => match conn.delete_row(&delete) {
            Ok(result) => DriverResponseBody::CrudResult { result },
            Err(e) => db_error_to_response(e),
        },

        // === Document mutations ===
        DriverRequestBody::UpdateDocument { update } => match conn.update_document(&update) {
            Ok(result) => DriverResponseBody::CrudResult { result },
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::InsertDocument { insert } => match conn.insert_document(&insert) {
            Ok(result) => DriverResponseBody::CrudResult { result },
            Err(e) => db_error_to_response(e),
        },

        DriverRequestBody::DeleteDocument { delete } => match conn.delete_document(&delete) {
            Ok(result) => DriverResponseBody::CrudResult { result },
            Err(e) => db_error_to_response(e),
        },

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

        // === Key-Value operations ===
        DriverRequestBody::KvScanKeys { request } => dispatch_kv(conn, |kv| {
            kv.scan_keys(&request)
                .map(|page| DriverResponseBody::KvScanResult { page })
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
