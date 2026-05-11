use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use dbflux_core::{
    CodeGenCapabilities, CodeGenerator, CodeGeneratorInfo, CollectionBrowseRequest,
    CollectionCountRequest, Connection, CrudResult, CustomTypeInfo, DatabaseInfo, DbError, DbKind,
    DbSchemaInfo, DescribeRequest, DocumentDelete, DocumentInsert, DocumentUpdate, ExplainRequest,
    KeyValueApi, LanguageService, OrderByColumn, QueryCancelHandle, QueryGenerator, QueryHandle,
    QueryRequest, QueryResult, RowDelete, RowInsert, RowPatch, SchemaFeatures,
    SchemaForeignKeyInfo, SchemaIndexInfo, SchemaLoadingStrategy, SchemaSnapshot, SemanticPlan,
    SemanticPlanner, SemanticRequest, SqlDialect, SqlGenerationRequest, TableBrowseRequest,
    TableCountRequest, TableInfo, Value, ViewInfo,
};

pub struct CachedConnection {
    connection: Arc<dyn Connection>,
    _access_handle: Option<Box<dyn Any + Send + Sync>>,
}

impl CachedConnection {
    pub fn new(
        connection: Arc<dyn Connection>,
        access_handle: Option<Box<dyn Any + Send + Sync>>,
    ) -> Self {
        Self {
            connection,
            _access_handle: access_handle,
        }
    }

    pub fn connection(&self) -> Arc<dyn Connection> {
        self.connection.clone()
    }
}

impl Connection for CachedConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        self.connection.metadata()
    }

    fn ping(&self) -> Result<(), DbError> {
        self.connection.ping()
    }

    fn close(&mut self) -> Result<(), DbError> {
        if let Some(connection) = Arc::get_mut(&mut self.connection) {
            connection.close()
        } else {
            Ok(())
        }
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        self.connection.execute(req)
    }

    fn execute_with_handle(
        &self,
        req: &QueryRequest,
    ) -> Result<(QueryHandle, QueryResult), DbError> {
        self.connection.execute_with_handle(req)
    }

    fn cancel(&self, handle: &QueryHandle) -> Result<(), DbError> {
        self.connection.cancel(handle)
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        self.connection.cancel_active()
    }

    fn cancel_handle(&self) -> Arc<dyn QueryCancelHandle> {
        self.connection.cancel_handle()
    }

    fn cleanup_after_cancel(&self) -> Result<(), DbError> {
        self.connection.cleanup_after_cancel()
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        self.connection.schema()
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        self.connection.list_databases()
    }

    fn schema_for_database(&self, database: &str) -> Result<DbSchemaInfo, DbError> {
        self.connection.schema_for_database(database)
    }

    fn table_details(
        &self,
        database: &str,
        schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, DbError> {
        self.connection.table_details(database, schema, table)
    }

    fn view_details(
        &self,
        database: &str,
        schema: Option<&str>,
        view: &str,
    ) -> Result<ViewInfo, DbError> {
        self.connection.view_details(database, schema, view)
    }

    fn set_active_database(&self, database: Option<&str>) -> Result<(), DbError> {
        self.connection.set_active_database(database)
    }

    fn active_database(&self) -> Option<String> {
        self.connection.active_database()
    }

    fn kind(&self) -> DbKind {
        self.connection.kind()
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        self.connection.schema_loading_strategy()
    }

    fn schema_features(&self) -> SchemaFeatures {
        self.connection.schema_features()
    }

    fn schema_types(
        &self,
        database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<CustomTypeInfo>, DbError> {
        self.connection.schema_types(database, schema)
    }

    fn schema_indexes(
        &self,
        database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<SchemaIndexInfo>, DbError> {
        self.connection.schema_indexes(database, schema)
    }

    fn schema_foreign_keys(
        &self,
        database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
        self.connection.schema_foreign_keys(database, schema)
    }

    fn browse_table(&self, request: &TableBrowseRequest) -> Result<QueryResult, DbError> {
        self.connection.browse_table(request)
    }

    fn count_table(&self, request: &TableCountRequest) -> Result<u64, DbError> {
        self.connection.count_table(request)
    }

    fn browse_collection(&self, request: &CollectionBrowseRequest) -> Result<QueryResult, DbError> {
        self.connection.browse_collection(request)
    }

    fn count_collection(&self, request: &CollectionCountRequest) -> Result<u64, DbError> {
        self.connection.count_collection(request)
    }

    fn explain(&self, request: &ExplainRequest) -> Result<QueryResult, DbError> {
        self.connection.explain(request)
    }

    fn describe_table(&self, request: &DescribeRequest) -> Result<QueryResult, DbError> {
        self.connection.describe_table(request)
    }

    fn code_generators(&self) -> Vec<CodeGeneratorInfo> {
        self.connection.code_generators()
    }

    fn generate_code(&self, generator_id: &str, table: &TableInfo) -> Result<String, DbError> {
        self.connection.generate_code(generator_id, table)
    }

    fn update_row(&self, patch: &RowPatch) -> Result<CrudResult, DbError> {
        self.connection.update_row(patch)
    }

    fn insert_row(&self, insert: &RowInsert) -> Result<CrudResult, DbError> {
        self.connection.insert_row(insert)
    }

    fn delete_row(&self, delete: &RowDelete) -> Result<CrudResult, DbError> {
        self.connection.delete_row(delete)
    }

    fn update_document(&self, update: &DocumentUpdate) -> Result<CrudResult, DbError> {
        self.connection.update_document(update)
    }

    fn insert_document(&self, insert: &DocumentInsert) -> Result<CrudResult, DbError> {
        self.connection.insert_document(insert)
    }

    fn delete_document(&self, delete: &DocumentDelete) -> Result<CrudResult, DbError> {
        self.connection.delete_document(delete)
    }

    fn key_value_api(&self) -> Option<&dyn KeyValueApi> {
        self.connection.key_value_api()
    }

    fn language_service(&self) -> &dyn LanguageService {
        self.connection.language_service()
    }

    fn dialect(&self) -> &dyn SqlDialect {
        self.connection.dialect()
    }

    fn code_gen_capabilities(&self) -> CodeGenCapabilities {
        self.connection.code_gen_capabilities()
    }

    fn code_generator(&self) -> &dyn CodeGenerator {
        self.connection.code_generator()
    }

    fn query_generator(&self) -> Option<&dyn QueryGenerator> {
        self.connection.query_generator()
    }

    fn semantic_planner(&self) -> Option<&dyn SemanticPlanner> {
        self.connection.semantic_planner()
    }

    fn plan_semantic_request(&self, request: &SemanticRequest) -> Result<SemanticPlan, DbError> {
        self.connection.plan_semantic_request(request)
    }

    fn generate_sql(&self, request: &SqlGenerationRequest) -> Result<String, DbError> {
        self.connection.generate_sql(request)
    }

    fn build_select_sql(
        &self,
        table: &str,
        columns: &[String],
        filter: Option<&Value>,
        order_by: &[OrderByColumn],
        limit: u32,
        offset: u32,
    ) -> String {
        self.connection
            .build_select_sql(table, columns, filter, order_by, limit, offset)
    }

    fn build_insert_sql(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
    ) -> (String, Vec<Value>) {
        self.connection.build_insert_sql(table, columns, values)
    }

    fn build_update_sql(
        &self,
        table: &str,
        set: &[(String, Value)],
        filter: Option<&Value>,
    ) -> (String, Vec<Value>) {
        self.connection.build_update_sql(table, set, filter)
    }

    fn build_delete_sql(&self, table: &str, filter: Option<&Value>) -> (String, Vec<Value>) {
        self.connection.build_delete_sql(table, filter)
    }

    fn build_upsert_sql(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        conflict_columns: &[String],
        update_columns: &[String],
    ) -> (String, Vec<Value>) {
        self.connection
            .build_upsert_sql(table, columns, values, conflict_columns, update_columns)
    }

    fn build_count_sql(&self, table: &str, filter: Option<&Value>) -> String {
        self.connection.build_count_sql(table, filter)
    }

    fn build_truncate_sql(&self, table: &str) -> String {
        self.connection.build_truncate_sql(table)
    }

    fn build_drop_index_sql(
        &self,
        index_name: &str,
        table_name: Option<&str>,
        if_exists: bool,
    ) -> String {
        self.connection
            .build_drop_index_sql(index_name, table_name, if_exists)
    }

    fn version_query(&self) -> &'static str {
        self.connection.version_query()
    }

    fn supports_transactional_ddl(&self) -> bool {
        self.connection.supports_transactional_ddl()
    }

    fn translate_filter(&self, filter: &Value) -> Result<String, DbError> {
        self.connection.translate_filter(filter)
    }
}

/// Caches live driver connections keyed by `connection_id` (profile UUID as string).
///
/// Connections are established lazily on first use and reused for subsequent calls.
/// The cache is single-threaded and lives for the lifetime of the server process.
pub struct ConnectionCache {
    inner: HashMap<String, Arc<CachedConnection>>,
}

impl Default for ConnectionCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionCache {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Returns the cached connection for `connection_id`, or `None` if not yet established.
    pub fn get(&self, connection_id: &str) -> Option<Arc<CachedConnection>> {
        self.inner.get(connection_id).cloned()
    }

    /// Inserts or replaces the connection for `connection_id`.
    pub fn insert(&mut self, connection_id: String, connection: Arc<CachedConnection>) {
        self.inner.insert(connection_id, connection);
    }

    /// Removes the connection for `connection_id` from the cache.
    /// Returns `true` if a connection was removed, `false` if it was not cached.
    pub fn remove(&mut self, connection_id: &str) -> bool {
        self.inner.remove(connection_id).is_some()
    }

    /// Removes the base connection entry and any per-database variants.
    pub fn remove_connection_variants(&mut self, connection_id: &str) -> usize {
        let prefix = format!("{}:", connection_id);
        let original_len = self.inner.len();

        self.inner
            .retain(|key, _| key != connection_id && !key.starts_with(&prefix));

        original_len.saturating_sub(self.inner.len())
    }
}

#[cfg(test)]
mod tests {
    use super::{CachedConnection, ConnectionCache};
    use dbflux_core::{ConnectionProfile, DbConfig, DbDriver, DbKind};
    use dbflux_test_support::FakeDriver;
    use std::sync::Arc;

    #[test]
    fn cache_keeps_wrapped_connection_available() {
        let driver = FakeDriver::new(DbKind::Postgres);
        let profile = ConnectionProfile::new(
            "test",
            DbConfig::Postgres {
                use_uri: false,
                uri: None,
                host: "localhost".to_string(),
                port: 5432,
                user: "postgres".to_string(),
                database: "app".to_string(),
                ssl_mode: None,
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
        );

        let connection = driver.connect(&profile).expect("fake driver connects");
        let cached = Arc::new(CachedConnection::new(Arc::from(connection), None));

        let mut cache = ConnectionCache::new();
        cache.insert(profile.id.to_string(), cached.clone());

        let retrieved = cache
            .get(&profile.id.to_string())
            .expect("connection cached");
        assert_eq!(
            retrieved.connection().active_database(),
            Some("app".to_string())
        );
    }
}
