pub(crate) mod history;
pub mod history_manager;
pub(crate) mod json_store;
pub(crate) mod recent_files;
pub(crate) mod saved_query;
pub mod saved_query_manager;
pub mod secret_manager;
pub(crate) mod secrets;
pub(crate) mod session;
pub(crate) mod ui_state;

pub use history::{HistoryEntry, HistoryStore};
pub use history_manager::HistoryManager;
pub use json_store::{AuthProfileStore, JsonStore, ProfileStore, ProxyStore, SshTunnelStore};
pub use recent_files::{RecentFile, RecentFilesStore};
pub use saved_query::{SavedQuery, SavedQueryStore};
pub use saved_query_manager::SavedQueryManager;
pub use secret_manager::{HasSecretRef, SecretManager};
pub use secrets::{
    KeyringSecretStore, NoopSecretStore, SecretStore, connection_secret_ref, create_secret_store,
    proxy_secret_ref, ssh_tunnel_secret_ref,
};
pub use session::{SessionManifest, SessionStore, SessionTab, SessionTabKind};
pub use ui_state::{UiState, UiStateStore};
