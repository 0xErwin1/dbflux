pub(crate) mod context;
pub(crate) mod hook;
pub(crate) mod item_manager;
pub mod manager;
pub(crate) mod profile;
pub mod profile_manager;
pub(crate) mod proxy;
pub mod proxy_manager;
pub mod ssh_tunnel_manager;
pub(crate) mod tree;
pub mod tree_manager;
pub(crate) mod tree_store;

use crate::DbError;

/// Backend for persisting a connection tree.
pub trait TreeStore {
    /// Loads the connection tree from the store.
    fn load(&self) -> Result<ConnectionTree, DbError>;
    /// Saves the connection tree to the store.
    fn save(&self, tree: &ConnectionTree) -> Result<(), DbError>;
}

pub use context::ExecutionContext;
pub use hook::{
    ConnectionHook, ConnectionHookBindings, ConnectionHooks, DetachedProcessHandle,
    DetachedProcessReceiver, DetachedProcessSender, HookContext, HookExecution, HookExecutionMode,
    HookExecutor, HookFailureMode, HookKind, HookPhase, HookPhaseOutcome, HookResult, HookRunner,
    LuaCapabilities, OutputEvent, OutputReceiver, OutputSender, OutputStreamKind,
    ProcessExecutionError, ProcessExecutor, ScriptLanguage, ScriptSource, detached_process_channel,
    execute_streaming_process, output_channel,
};
pub use item_manager::{AuthProfileManager, Identifiable, ItemManager};
pub use manager::{
    CacheEntry, CacheKey, ConnectProfileParams, ConnectProfileResult, ConnectedProfile,
    ConnectionManager, ConnectionResolutionError, DatabaseConnection, FetchDatabaseSchemaParams,
    FetchDatabaseSchemaResult, FetchSchemaForeignKeysParams, FetchSchemaForeignKeysResult,
    FetchSchemaIndexesParams, FetchSchemaIndexesResult, FetchSchemaTypesParams,
    FetchSchemaTypesResult, FetchTableDetailsParams, FetchTableDetailsResult, HookExecutionContext,
    OwnedCacheEntry, PendingOperation, RedisKeyCache, RedisKeyCacheEntry, ResolvedProxy,
    SchemaCacheKey, SwitchDatabaseParams, SwitchDatabaseResult,
};
pub use profile::{
    ConnectionMcpGovernance, ConnectionMcpPolicyBinding, ConnectionProfile, DbConfig, DbKind,
    SshAuthMethod, SshTunnelConfig, SshTunnelProfile, SslMode,
};
pub use profile_manager::ProfileManager;
pub use proxy::{ProxyAuth, ProxyKind, ProxyProfile, host_matches_no_proxy};
pub use proxy_manager::ProxyManager;
pub use ssh_tunnel_manager::SshTunnelManager;
pub use tree::{ConnectionTree, ConnectionTreeNode, ConnectionTreeNodeKind};
pub use tree_manager::ConnectionTreeManager;
pub use tree_store::ConnectionTreeStore;
