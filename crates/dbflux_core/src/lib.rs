#![allow(clippy::result_large_err)]

pub mod access;
pub mod auth;
mod config;
mod connection;
mod core;
mod data;
mod driver;
mod facade;
pub mod pipeline;
mod query;
mod schema;
mod sql;
mod storage;
pub mod values;

pub use access::{AccessHandle, AccessKind, AccessManager};

pub use auth::{
    AuthFormDef, AuthProfile, AuthProfileSummary, AuthProvider, AuthSession, AuthSessionState,
    DynAuthProvider, ImportableProfile, ResolvedCredentials,
};

pub use config::{
    AppConfig, AppConfigStore, DangerousAction, DriverKey, EffectiveSettings, GeneralSettings,
    GlobalOverrides, GovernanceSettings, PolicyRoleConfig, RefreshPolicy, RefreshPolicySetting,
    ScriptEntry, ScriptsDirectory, ServiceConfig, StartupFocus, ThemeSetting, ToolPolicyConfig,
    TrustedClientConfig, all_script_extensions, driver_maps_differ, filter_entries,
    hook_script_path, is_openable_script,
};

pub use connection::{
    AuthProfileManager, CacheEntry, CacheKey, ConnectProfileParams, ConnectProfileResult,
    ConnectedProfile, ConnectionHook, ConnectionHookBindings, ConnectionHooks, ConnectionManager,
    ConnectionMcpGovernance, ConnectionMcpPolicyBinding, ConnectionProfile,
    ConnectionResolutionError, ConnectionTree, ConnectionTreeManager, ConnectionTreeNode,
    ConnectionTreeNodeKind, ConnectionTreeStore, DatabaseConnection, DbConfig, DbKind,
    DetachedProcessHandle, DetachedProcessReceiver, DetachedProcessSender, ExecutionContext,
    FetchDatabaseSchemaParams, FetchDatabaseSchemaResult, FetchSchemaForeignKeysParams,
    FetchSchemaForeignKeysResult, FetchSchemaIndexesParams, FetchSchemaIndexesResult,
    FetchSchemaTypesParams, FetchSchemaTypesResult, FetchTableDetailsParams,
    FetchTableDetailsResult, HookContext, HookExecution, HookExecutionContext, HookExecutionMode,
    HookExecutor, HookFailureMode, HookKind, HookPhase, HookPhaseOutcome, HookResult, HookRunner,
    Identifiable, ItemManager, LuaCapabilities, OutputEvent, OutputReceiver, OutputSender,
    OutputStreamKind, OwnedCacheEntry, PendingOperation, ProcessExecutionError, ProcessExecutor,
    ProfileManager, ProxyAuth, ProxyKind, ProxyManager, ProxyProfile, RedisKeyCache,
    RedisKeyCacheEntry, ResolvedProxy, SchemaCacheKey, ScriptLanguage, ScriptSource, SshAuthMethod,
    SshTunnelConfig, SshTunnelManager, SshTunnelProfile, SslMode, SwitchDatabaseParams,
    SwitchDatabaseResult, detached_process_channel, execute_streaming_process, output_channel,
};

pub use core::{
    CancelToken, CodeGenScope, CodeGeneratorInfo, Connection, ConnectionErrorFormatter,
    ConnectionExt, ConnectionOverrides, DbDriver, DbError, DefaultErrorFormatter,
    DocumentConnection, ErrorLocation, FormattedError, KeyValueApi, KeyValueConnection,
    NoopCancelHandle, QueryCancelHandle, QueryErrorFormatter, RelationalConnection, SchemaFeatures,
    SchemaLoadingStrategy, ShutdownCoordinator, ShutdownPhase, TaskId, TaskKind, TaskManager,
    TaskSlot, TaskSnapshot, TaskStatus, TaskTarget, Value, sanitize_uri,
};

pub use data::{
    CrudResult, DataViewKind, DocumentDelete, DocumentFilter, DocumentInsert, DocumentUpdate,
    HashDeleteRequest, HashSetRequest, KeyBulkGetRequest, KeyDeleteRequest, KeyEntry,
    KeyExistsRequest, KeyExpireRequest, KeyGetRequest, KeyGetResult, KeyPersistRequest,
    KeyRenameRequest, KeyScanPage, KeyScanRequest, KeySetRequest, KeyTtlRequest, KeyType,
    KeyTypeRequest, ListEnd, ListPushRequest, ListRemoveRequest, ListSetRequest, MutationRequest,
    RecordIdentity, RowDelete, RowIdentity, RowInsert, RowPatch, RowState, SetAddRequest,
    SetCondition, SetRemoveRequest, StreamAddRequest, StreamDeleteRequest, StreamEntryId,
    StreamMaxLen, ValueRepr, ZSetAddRequest, ZSetRemoveRequest,
};

pub use driver::{
    DYNAMODB_FORM, DatabaseCategory, DdlCapabilities, DriverCapabilities, DriverFormDef,
    DriverLimits, DriverMetadata, DriverMetadataBuilder, ExecutionClassification, FormFieldDef,
    FormFieldKind, FormSection, FormTab, FormValues, Icon, IsolationLevel, MONGODB_FORM,
    MYSQL_FORM, MutationCapabilities, OperationClassifier, POSTGRES_FORM, PaginationStyle,
    QueryCapabilities, QueryLanguage, REDIS_FORM, SQLITE_FORM, SelectOption, SyntaxInfo,
    TransactionCapabilities, WhereOperator, field_file_path, field_password, field_use_uri,
    ssh_tab,
};

pub use facade::{DangerousQuerySuppressions, SessionFacade};

pub use query::{
    CollectionBrowseRequest, CollectionCountRequest, CollectionRef, ColumnMeta, ColumnRef,
    DangerousQueryKind, DescribeRequest, Diagnostic, DiagnosticSeverity, EditorDiagnostic,
    ExplainRequest, GeneratedQuery, LanguageService, MutationCategory, OrderByColumn, Pagination,
    QueryGenerator, QueryHandle, QueryRequest, QueryResult, QueryResultShape, RedisLanguageService,
    Row, SortDirection, SqlLanguageService, SqlMutationGenerator, TableBrowseRequest,
    TableCountRequest, TableRef, TextPosition, TextPositionRange, TextRange, ValidationResult,
    classify_query_for_governance, classify_query_for_language, classify_sql_execution,
    detect_dangerous_mongo, detect_dangerous_query, detect_dangerous_redis, detect_dangerous_sql,
    is_safe_read_query, language_service_for_query_language, strip_leading_comments,
};

pub use schema::node_id as schema_node_id;
pub use schema::{
    CollectionIndexInfo, CollectionInfo, ColumnFamilyInfo, ColumnInfo, ConstraintInfo,
    ConstraintKind, ContainerInfo, CustomTypeInfo, CustomTypeKind, DataStructure, DatabaseInfo,
    DbSchemaInfo, DocumentSchema, FieldInfo, ForeignKeyBuilder, ForeignKeyInfo, GraphInfo,
    GraphSchema, IndexBuilder, IndexData, IndexDirection, IndexInfo, KeyInfo, KeySpaceInfo,
    KeyValueSchema, MeasurementInfo, MultiModelCapabilities, MultiModelSchema, NodeLabelInfo,
    ParseSchemaNodeIdError, PropertyInfo, RelationalSchema, RelationshipTypeInfo,
    RetentionPolicyInfo, SchemaForeignKeyBuilder, SchemaForeignKeyInfo, SchemaIndexBuilder,
    SchemaIndexInfo, SchemaNodeId, SchemaNodeKind, SchemaSnapshot, SearchIndexInfo,
    SearchMappingInfo, SearchSchema, TableInfo, TimeSeriesFieldInfo, TimeSeriesSchema,
    VectorCollectionInfo, VectorMetadataField, VectorMetric, VectorSchema, ViewInfo,
    WideColumnInfo, WideColumnKeyspaceInfo, WideColumnSchema,
};

pub use sql::{
    AddEnumValueRequest, AddForeignKeyRequest, CodeGenCapabilities, CodeGenerator,
    CreateIndexRequest, CreateTypeRequest, DefaultSqlDialect, DropForeignKeyRequest,
    DropIndexRequest, DropTypeRequest, NoOpCodeGenerator, PlaceholderStyle, ReindexRequest,
    SqlDialect, SqlGenerationOptions, SqlGenerationRequest, SqlOperation, SqlQueryBuilder,
    SqlValueMode, TypeDefinition, generate_create_table, generate_delete_template,
    generate_drop_table, generate_insert_template, generate_select_star, generate_sql,
    generate_truncate, generate_update_template,
};

pub use pipeline::{
    PipelineError, PipelineInput, PipelineOutput, PipelineState, StateSender, StateWatcher,
    pipeline_state_channel, resolve_profile_values, run_pipeline,
};

pub use values::{
    CachedValue, CompositeValueResolver, DynParameterProvider, DynSecretProvider, FieldValue,
    ParameterProvider, ProviderError, ResolveContext, ResolvedValue, SecretProvider, ValueCache,
    ValueCacheKey, ValueOrigin, ValueRef,
};

pub use chrono;
pub use secrecy;
pub use storage::{
    AuthProfileStore, HasSecretRef, HistoryEntry, HistoryManager, HistoryStore, JsonStore,
    KeyringSecretStore, NoopSecretStore, ProfileStore, ProxyStore, RecentFile, RecentFilesStore,
    SavedQuery, SavedQueryManager, SavedQueryStore, SecretManager, SecretStore, SessionManifest,
    SessionStore, SessionTab, SessionTabKind, SshTunnelStore, UiState, UiStateStore,
    connection_secret_ref, create_secret_store, proxy_secret_ref, ssh_tunnel_secret_ref,
};

// Backward-compatible public module paths for external crates that use
// `dbflux_core::connection_manager::*` etc.
pub use connection::manager as connection_manager;
pub use connection::profile_manager;
pub use connection::proxy_manager;
pub use connection::ssh_tunnel_manager;
pub use connection::tree_manager as connection_tree_manager;
pub use facade::session as session_facade;
pub use storage::history_manager;
pub use storage::saved_query_manager;
pub use storage::secret_manager;

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
