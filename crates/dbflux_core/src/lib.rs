#![allow(clippy::result_large_err)]

mod app_config;
mod code_generation;
mod connection_tree;
mod connection_tree_store;
mod crud;
mod data_view;
mod driver_capabilities;
mod driver_form;
mod error;
mod error_formatter;
mod execution_context;
mod history;
mod key_value;
mod language_service;
mod profile;
mod query;
mod query_generator;
mod query_safety;
mod recent_files;
mod refresh_policy;
mod saved_query;
mod schema;
mod schema_builder;
pub mod schema_node_id;
mod scripts_directory;
mod secrets;
mod session_store;
mod shutdown;
mod sql_dialect;
mod sql_generation;
mod sql_query_builder;
mod store;
mod table_browser;
mod task;
mod traits;
mod value;

pub mod connection_manager;
pub mod connection_tree_manager;
pub mod history_manager;
pub mod profile_manager;
pub mod saved_query_manager;
pub mod secret_manager;
pub mod session_facade;
pub mod ssh_tunnel_manager;

pub use code_generation::{
    AddEnumValueRequest, AddForeignKeyRequest, CodeGenCapabilities, CodeGenerator,
    CreateIndexRequest, CreateTypeRequest, DropForeignKeyRequest, DropIndexRequest,
    DropTypeRequest, NoOpCodeGenerator, ReindexRequest, TypeDefinition,
};
pub use connection_tree::{ConnectionTree, ConnectionTreeNode, ConnectionTreeNodeKind};
pub use connection_tree_store::ConnectionTreeStore;
pub use crud::{
    CrudResult, DocumentDelete, DocumentFilter, DocumentInsert, DocumentUpdate, MutationRequest,
    RecordIdentity, RowDelete, RowIdentity, RowInsert, RowPatch, RowState,
};
pub use data_view::DataViewKind;
pub use driver_capabilities::{
    DatabaseCategory, DriverCapabilities, DriverMetadata, Icon, QueryLanguage,
};
pub use driver_form::{
    DriverFormDef, FormFieldDef, FormFieldKind, FormSection, FormTab, FormValues, MONGODB_FORM,
    MYSQL_FORM, POSTGRES_FORM, REDIS_FORM, SQLITE_FORM, SelectOption,
};
pub use error::DbError;
pub use error_formatter::{
    ConnectionErrorFormatter, DefaultErrorFormatter, ErrorLocation, FormattedError,
    QueryErrorFormatter, sanitize_uri,
};
pub use execution_context::ExecutionContext;
pub use history::{HistoryEntry, HistoryStore};
pub use key_value::{
    HashDeleteRequest, HashSetRequest, KeyBulkGetRequest, KeyDeleteRequest, KeyEntry,
    KeyExistsRequest, KeyExpireRequest, KeyGetRequest, KeyGetResult, KeyPersistRequest,
    KeyRenameRequest, KeyScanPage, KeyScanRequest, KeySetRequest, KeyTtlRequest, KeyType,
    KeyTypeRequest, ListEnd, ListPushRequest, ListRemoveRequest, ListSetRequest, SetAddRequest,
    SetCondition, SetRemoveRequest, StreamAddRequest, StreamDeleteRequest, StreamEntryId,
    StreamMaxLen, ValueRepr, ZSetAddRequest, ZSetRemoveRequest,
};
pub use language_service::{
    DangerousQueryKind, Diagnostic, DiagnosticSeverity, EditorDiagnostic, LanguageService,
    RedisLanguageService, SqlLanguageService, TextPosition, TextPositionRange, TextRange,
    ValidationResult, detect_dangerous_mongo, detect_dangerous_query, detect_dangerous_redis,
    detect_dangerous_sql, strip_leading_comments,
};
pub use profile::{
    ConnectionProfile, DbConfig, DbKind, SshAuthMethod, SshTunnelConfig, SshTunnelProfile, SslMode,
};
pub use query::{ColumnMeta, QueryHandle, QueryRequest, QueryResult, QueryResultShape, Row};
pub use query_generator::{GeneratedQuery, MutationCategory, QueryGenerator, SqlMutationGenerator};
pub use query_safety::is_safe_read_query;
pub use recent_files::{RecentFile, RecentFilesStore};
pub use refresh_policy::RefreshPolicy;
pub use saved_query::{SavedQuery, SavedQueryStore};
pub use schema::{
    CollectionIndexInfo, CollectionInfo, ColumnFamilyInfo, ColumnInfo, ConstraintInfo,
    ConstraintKind, ContainerInfo, CustomTypeInfo, CustomTypeKind, DataStructure, DatabaseInfo,
    DbSchemaInfo, DocumentSchema, FieldInfo, ForeignKeyInfo, GraphInfo, GraphSchema, IndexData,
    IndexDirection, IndexInfo, KeyInfo, KeySpaceInfo, KeyValueSchema, MeasurementInfo,
    MultiModelCapabilities, MultiModelSchema, NodeLabelInfo, PropertyInfo, RelationalSchema,
    RelationshipTypeInfo, RetentionPolicyInfo, SchemaForeignKeyInfo, SchemaIndexInfo,
    SchemaSnapshot, SearchIndexInfo, SearchMappingInfo, SearchSchema, TableInfo,
    TimeSeriesFieldInfo, TimeSeriesSchema, VectorCollectionInfo, VectorMetadataField, VectorMetric,
    VectorSchema, ViewInfo, WideColumnInfo, WideColumnKeyspaceInfo, WideColumnSchema,
};
pub use schema_node_id::{ParseSchemaNodeIdError, SchemaNodeId, SchemaNodeKind};
pub use scripts_directory::{ScriptEntry, ScriptsDirectory, all_script_extensions, filter_entries};
pub use secrets::{
    KeyringSecretStore, NoopSecretStore, SecretStore, connection_secret_ref, create_secret_store,
    ssh_tunnel_secret_ref,
};
pub use session_store::{SessionManifest, SessionStore, SessionTab, SessionTabKind};
pub use shutdown::{ShutdownCoordinator, ShutdownPhase};
pub use store::{ProfileStore, SshTunnelStore};
pub use table_browser::{
    CollectionBrowseRequest, CollectionCountRequest, CollectionRef, DescribeRequest,
    ExplainRequest, OrderByColumn, Pagination, SortDirection, TableBrowseRequest,
    TableCountRequest, TableRef,
};
pub use task::{CancelToken, TaskId, TaskKind, TaskManager, TaskSlot, TaskSnapshot, TaskStatus};
pub use traits::{
    CodeGenScope, CodeGeneratorInfo, Connection, DbDriver, KeyValueApi, NoopCancelHandle,
    QueryCancelHandle, SchemaFeatures, SchemaLoadingStrategy,
};
pub use value::Value;

pub use chrono;

pub use schema_builder::{
    ForeignKeyBuilder, IndexBuilder, SchemaForeignKeyBuilder, SchemaIndexBuilder,
};
pub use sql_dialect::{DefaultSqlDialect, PlaceholderStyle, SqlDialect};
pub use sql_generation::{
    SqlGenerationOptions, SqlGenerationRequest, SqlOperation, SqlValueMode, generate_create_table,
    generate_delete_template, generate_drop_table, generate_insert_template, generate_select_star,
    generate_sql, generate_truncate, generate_update_template,
};
pub use sql_query_builder::SqlQueryBuilder;

pub use app_config::{AppConfig, AppConfigStore, ServiceConfig};
pub use connection_manager::{
    CacheEntry, CacheKey, ConnectProfileParams, ConnectProfileResult, ConnectedProfile,
    ConnectionManager, DatabaseConnection, FetchDatabaseSchemaParams, FetchDatabaseSchemaResult,
    FetchSchemaForeignKeysParams, FetchSchemaForeignKeysResult, FetchSchemaIndexesParams,
    FetchSchemaIndexesResult, FetchSchemaTypesParams, FetchSchemaTypesResult,
    FetchTableDetailsParams, FetchTableDetailsResult, OwnedCacheEntry, PendingOperation,
    RedisKeyCache, RedisKeyCacheEntry, SchemaCacheKey, SwitchDatabaseParams, SwitchDatabaseResult,
};
pub use connection_tree_manager::ConnectionTreeManager;
pub use history_manager::HistoryManager;
pub use profile_manager::ProfileManager;
pub use saved_query_manager::SavedQueryManager;
pub use secret_manager::SecretManager;
pub use session_facade::{DangerousQuerySuppressions, SessionFacade};
pub use ssh_tunnel_manager::SshTunnelManager;

/// Safely truncate a string at a character boundary, appending "..." if truncated.
pub fn truncate_string_safe(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        return s.to_string();
    }

    let truncate_at = max_len.saturating_sub(3);
    let safe_end = s
        .char_indices()
        .take_while(|(idx, _)| *idx <= truncate_at)
        .last()
        .map(|(idx, _)| idx)
        .unwrap_or(0);

    format!("{}...", &s[..safe_end])
}
