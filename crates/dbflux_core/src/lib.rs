mod connection_tree;
mod connection_tree_store;
mod crud;
mod driver_capabilities;
mod driver_form;
mod error;
mod history;
mod profile;
mod query;
mod saved_query;
mod schema;
mod schema_builder;
mod secrets;
mod shutdown;
mod sql_dialect;
mod sql_generation;
mod sql_query_builder;
mod store;
mod table_browser;
mod task;
mod traits;
mod value;

pub use connection_tree::{ConnectionTree, ConnectionTreeNode, ConnectionTreeNodeKind};
pub use connection_tree_store::ConnectionTreeStore;
pub use crud::{CrudResult, RecordIdentity, RowDelete, RowIdentity, RowInsert, RowPatch, RowState};
pub use driver_capabilities::{
    DatabaseCategory, DriverCapabilities, DriverMetadata, Icon, QueryLanguage,
};
pub use driver_form::{
    DriverFormDef, FormFieldDef, FormFieldKind, FormSection, FormTab, FormValues, MYSQL_FORM,
    POSTGRES_FORM, SQLITE_FORM,
};
pub use error::DbError;
pub use history::{HistoryEntry, HistoryStore};
pub use profile::{
    ConnectionProfile, DbConfig, DbKind, SshAuthMethod, SshTunnelConfig, SshTunnelProfile, SslMode,
};
pub use query::{ColumnMeta, QueryHandle, QueryRequest, QueryResult, Row};
pub use saved_query::{SavedQuery, SavedQueryStore};
pub use schema::{
    CollectionIndexInfo, CollectionInfo, ColumnInfo, ConstraintInfo, ConstraintKind, ContainerInfo,
    CustomTypeInfo, CustomTypeKind, DatabaseInfo, DbSchemaInfo, FieldInfo, ForeignKeyInfo,
    IndexDirection, IndexInfo, KeyInfo, KeySpaceInfo, KeyValueType, SchemaForeignKeyInfo,
    SchemaIndexInfo, SchemaSnapshot, TableInfo, ViewInfo,
};
pub use secrets::{
    KeyringSecretStore, NoopSecretStore, SecretStore, connection_secret_ref, create_secret_store,
    ssh_tunnel_secret_ref,
};
pub use shutdown::{ShutdownCoordinator, ShutdownPhase};
pub use store::{ProfileStore, SshTunnelStore};
pub use table_browser::{OrderByColumn, Pagination, SortDirection, TableBrowseRequest, TableRef};
pub use task::{CancelToken, TaskId, TaskKind, TaskManager, TaskSnapshot, TaskStatus};
pub use traits::{
    CodeGenScope, CodeGeneratorInfo, Connection, DbDriver, NoopCancelHandle, QueryCancelHandle,
    SchemaFeatures, SchemaLoadingStrategy,
};
pub use value::Value;

pub use sql_dialect::{DefaultSqlDialect, PlaceholderStyle, SqlDialect};
pub use sql_generation::{
    SqlGenerationOptions, SqlGenerationRequest, SqlOperation, SqlValueMode, generate_create_table,
    generate_delete_template, generate_insert_template, generate_select_star, generate_sql,
    generate_truncate, generate_update_template, generate_drop_table,
};
pub use sql_query_builder::SqlQueryBuilder;
pub use schema_builder::{
    ForeignKeyBuilder, IndexBuilder, SchemaForeignKeyBuilder, SchemaIndexBuilder,
};

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
