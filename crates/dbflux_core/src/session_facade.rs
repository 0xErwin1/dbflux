use crate::connection_manager::ConnectionManager;
use crate::connection_tree_manager::ConnectionTreeManager;
use crate::history_manager::HistoryManager;
use crate::profile_manager::ProfileManager;
use crate::saved_query_manager::SavedQueryManager;
use crate::secret_manager::SecretManager;
use crate::ssh_tunnel_manager::SshTunnelManager;
use crate::{
    create_secret_store, ConnectionProfile, DangerousQueryKind, DbDriver, ShutdownCoordinator,
    ShutdownPhase, TaskManager,
};
use log::info;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Session-based suppressions for dangerous query confirmations.
#[derive(Default)]
pub struct DangerousQuerySuppressions {
    delete_no_where: bool,
    update_no_where: bool,
    truncate: bool,
    drop: bool,
    alter: bool,
    script: bool,
    mongo_delete_many: bool,
    mongo_update_many: bool,
    mongo_drop_collection: bool,
    mongo_drop_database: bool,
    redis_flush_all: bool,
    redis_flush_db: bool,
    redis_multi_delete: bool,
    redis_keys_pattern: bool,
}

impl DangerousQuerySuppressions {
    pub fn is_suppressed(&self, kind: DangerousQueryKind) -> bool {
        match kind {
            DangerousQueryKind::DeleteNoWhere => self.delete_no_where,
            DangerousQueryKind::UpdateNoWhere => self.update_no_where,
            DangerousQueryKind::Truncate => self.truncate,
            DangerousQueryKind::Drop => self.drop,
            DangerousQueryKind::Alter => self.alter,
            DangerousQueryKind::Script => self.script,
            DangerousQueryKind::MongoDeleteMany => self.mongo_delete_many,
            DangerousQueryKind::MongoUpdateMany => self.mongo_update_many,
            DangerousQueryKind::MongoDropCollection => self.mongo_drop_collection,
            DangerousQueryKind::MongoDropDatabase => self.mongo_drop_database,
            DangerousQueryKind::RedisFlushAll => self.redis_flush_all,
            DangerousQueryKind::RedisFlushDb => self.redis_flush_db,
            DangerousQueryKind::RedisMultiDelete => self.redis_multi_delete,
            DangerousQueryKind::RedisKeysPattern => self.redis_keys_pattern,
        }
    }

    pub fn set_suppressed(&mut self, kind: DangerousQueryKind) {
        match kind {
            DangerousQueryKind::DeleteNoWhere => self.delete_no_where = true,
            DangerousQueryKind::UpdateNoWhere => self.update_no_where = true,
            DangerousQueryKind::Truncate => self.truncate = true,
            DangerousQueryKind::Drop => self.drop = true,
            DangerousQueryKind::Alter => self.alter = true,
            DangerousQueryKind::Script => self.script = true,
            DangerousQueryKind::MongoDeleteMany => self.mongo_delete_many = true,
            DangerousQueryKind::MongoUpdateMany => self.mongo_update_many = true,
            DangerousQueryKind::MongoDropCollection => self.mongo_drop_collection = true,
            DangerousQueryKind::MongoDropDatabase => self.mongo_drop_database = true,
            DangerousQueryKind::RedisFlushAll => self.redis_flush_all = true,
            DangerousQueryKind::RedisFlushDb => self.redis_flush_db = true,
            DangerousQueryKind::RedisMultiDelete => self.redis_multi_delete = true,
            DangerousQueryKind::RedisKeysPattern => self.redis_keys_pattern = true,
        }
    }
}

/// Composes all sub-managers into a unified session interface.
///
/// Cross-cutting operations that span multiple managers live here,
/// while single-concern operations delegate directly to the sub-manager.
pub struct SessionFacade {
    pub connections: ConnectionManager,
    pub profiles: ProfileManager,
    pub secrets: SecretManager,
    pub ssh_tunnels: SshTunnelManager,
    pub history: HistoryManager,
    pub saved_queries: SavedQueryManager,
    pub tree: ConnectionTreeManager,
    pub tasks: TaskManager,
    pub shutdown: ShutdownCoordinator,
    pub dangerous_query_suppressions: DangerousQuerySuppressions,
}

impl SessionFacade {
    pub fn new(drivers: HashMap<String, Arc<dyn DbDriver>>) -> Self {
        let secret_store = create_secret_store();
        info!("Secret store available: {}", secret_store.is_available());

        let secrets = SecretManager::new(secret_store);
        let profiles = ProfileManager::new();
        let ssh_tunnels = SshTunnelManager::new();
        let history = HistoryManager::new();
        let saved_queries = SavedQueryManager::new();
        let mut tree = ConnectionTreeManager::new();

        tree.sync_with_profiles(&profiles.profile_ids());

        Self {
            connections: ConnectionManager::new(drivers),
            profiles,
            secrets,
            ssh_tunnels,
            history,
            saved_queries,
            tree,
            tasks: TaskManager::new(),
            shutdown: ShutdownCoordinator::new(),
            dangerous_query_suppressions: DangerousQuerySuppressions::default(),
        }
    }

    // --- Cross-cutting orchestration ---

    /// Adds a profile and places it in a folder in the connection tree.
    pub fn add_profile_in_folder(&mut self, profile: ConnectionProfile, folder_id: Option<Uuid>) {
        let profile_id = profile.id;
        self.profiles.add(profile);
        self.tree.add_profile_node(profile_id, folder_id);
    }

    /// Removes a profile by index, disconnecting and cleaning up secrets and tree.
    pub fn remove_profile(&mut self, idx: usize) -> Option<ConnectionProfile> {
        if idx >= self.profiles.profiles.len() {
            return None;
        }

        let removed = self.profiles.remove(idx)?;
        self.connections.disconnect(removed.id);
        self.secrets.delete_password(&removed);
        self.tree.remove_profile_node(removed.id);

        Some(removed)
    }

    /// Removes an SSH tunnel by index, cleaning up its secret.
    #[allow(dead_code)]
    pub fn remove_ssh_tunnel(&mut self, idx: usize) -> Option<crate::SshTunnelProfile> {
        let removed = self.ssh_tunnels.remove(idx)?;
        self.secrets.delete_ssh_tunnel_secret(&removed);
        Some(removed)
    }

    // --- Shutdown orchestration ---

    pub fn begin_shutdown(&self) -> bool {
        self.shutdown.request_shutdown()
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.is_shutdown_requested()
    }

    pub fn shutdown_phase(&self) -> ShutdownPhase {
        self.shutdown.phase()
    }

    pub fn cancel_all_tasks(&mut self) -> usize {
        if !self
            .shutdown
            .advance_phase(ShutdownPhase::SignalSent, ShutdownPhase::CancellingTasks)
        {
            return 0;
        }

        let count = self.tasks.cancel_all();
        info!("Cancelled {} running tasks during shutdown", count);
        count
    }

    pub fn close_all_connections(&mut self) {
        self.connections.close_all_connections(&self.shutdown);
    }

    pub fn complete_shutdown(&self) {
        self.shutdown.complete();
    }

    #[allow(dead_code)]
    pub fn fail_shutdown(&self) {
        self.shutdown.fail();
    }
}
