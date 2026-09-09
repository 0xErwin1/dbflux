use crate::*;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};

fn duplicate_profile_in_state(
    state: &mut AppStateEntity,
    profile_id: Uuid,
    cx: &mut Context<AppStateEntity>,
) -> Result<Option<Uuid>, dbflux_core::DbError> {
    let Some(original) = state
        .profiles()
        .iter()
        .find(|profile| profile.id == profile_id)
        .cloned()
    else {
        return Ok(None);
    };

    let folder_id = state
        .connection_tree()
        .find_by_profile(profile_id)
        .and_then(|node| node.parent_id);
    let password = state.get_password(&original);
    let ssh_password = state.get_ssh_password(&original);

    let mut cloned = original;
    cloned.id = Uuid::new_v4();
    cloned.name = format!("{} (Copy)", cloned.name);
    let new_id = cloned.id;

    if let Some(password) = password.as_ref() {
        state.save_password(&cloned, password)?;
    }

    state.add_profile_in_folder(cloned.clone(), folder_id);

    if let Some(ssh_password) = ssh_password.as_ref() {
        state.save_ssh_password(&cloned, ssh_password);
    }

    cx.emit(AppStateChanged);
    Ok(Some(new_id))
}

impl Sidebar {
    pub(super) fn collect_subtree_item_ids(
        items: &[TreeItem],
        root_item_id: &str,
        collected: &mut Vec<String>,
    ) -> bool {
        for item in items {
            if item.id.as_ref() == root_item_id {
                Self::collect_descendant_item_ids(&item.children, collected);
                return true;
            }

            if Self::collect_subtree_item_ids(&item.children, root_item_id, collected) {
                return true;
            }
        }

        false
    }

    fn collect_descendant_item_ids(items: &[TreeItem], collected: &mut Vec<String>) {
        for item in items {
            collected.push(item.id.to_string());
            Self::collect_descendant_item_ids(&item.children, collected);
        }
    }

    /// Creates a new folder at the root level.
    pub fn create_root_folder(&mut self, cx: &mut Context<Self>) {
        let folder_id = self.app_state.update(cx, |state, cx| {
            let id = state.create_folder(dbflux_i18n::t!("sidebar.tree.folder.new_default"), None);
            cx.emit(AppStateChanged);
            id
        });

        self.refresh_tree(cx);

        let item_id = SchemaNodeId::ConnectionFolder { node_id: folder_id }.to_string();

        self.select_and_rename_item(&item_id, cx);
    }

    pub(crate) fn create_folder_from_context(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let parent_id = match parse_node_id(item_id) {
            Some(SchemaNodeId::ConnectionFolder { node_id }) => Some(node_id),
            _ => None,
        };

        if parent_id.is_some() {
            self.set_expanded(item_id, true, cx);
        }

        let folder_id = self.app_state.update(cx, |state, cx| {
            let id = state.create_folder(
                dbflux_i18n::t!("sidebar.tree.folder.new_default"),
                parent_id,
            );
            cx.emit(AppStateChanged);
            id
        });

        self.refresh_tree(cx);

        let new_item_id = SchemaNodeId::ConnectionFolder { node_id: folder_id }.to_string();

        self.select_and_rename_item(&new_item_id, cx);
    }

    /// Selects the item, scrolls to it, and queues a rename for the next render.
    pub(super) fn select_and_rename_item(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let tree_state = self.active_tree_state().clone();

        if let Some(index) = self.find_item_index(item_id, cx) {
            tree_state.update(cx, |state, cx| {
                state.set_selected_index(Some(index), cx);
                state.scroll_to_item(index, gpui::ScrollStrategy::Center);
            });
        }

        self.pending_rename_item = Some(item_id.to_string());
        cx.notify();
    }

    pub(crate) fn duplicate_profile(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(SchemaNodeId::Profile { profile_id }) = parse_node_id(item_id) else {
            return;
        };

        let clone_result = self.app_state.update(cx, |state, cx| {
            duplicate_profile_in_state(state, profile_id, cx)
        });

        let new_id = match clone_result {
            Ok(Some(new_id)) => new_id,
            Ok(None) => return,
            Err(_) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Storage,
                        "Unable to save the copied connection password to the system keyring. Unlock it and try again.",
                    ),
                    cx,
                );
                return;
            }
        };

        self.refresh_tree(cx);

        let new_item_id = SchemaNodeId::Profile { profile_id: new_id }.to_string();

        self.select_and_rename_item(&new_item_id, cx);
    }

    pub(crate) fn create_connection_in_folder(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(SchemaNodeId::ConnectionFolder { node_id: folder_id }) = parse_node_id(item_id)
        else {
            return;
        };

        cx.emit(SidebarEvent::RequestOpenConnectionManagerInFolder { folder_id });
    }

    pub(crate) fn start_rename(
        &mut self,
        item_id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Handle folder rename
        if let Some(SchemaNodeId::ConnectionFolder { node_id: folder_id }) = parse_node_id(item_id)
        {
            let current_name = self
                .app_state
                .read(cx)
                .connection_tree()
                .find_by_id(folder_id)
                .map(|f| f.name.clone())
                .unwrap_or_default();

            self.editing_id = Some(folder_id);
            self.editing_is_folder = true;
            self.rename_input.update(cx, |input, cx| {
                input.set_value(&current_name, window, cx);
                input.focus(window, cx);
            });
            cx.notify();
            return;
        }

        // Handle profile rename
        if let Some(SchemaNodeId::Profile { profile_id }) = parse_node_id(item_id) {
            let current_name = self
                .app_state
                .read(cx)
                .profiles()
                .iter()
                .find(|p| p.id == profile_id)
                .map(|p| p.name.clone())
                .unwrap_or_default();

            self.editing_id = Some(profile_id);
            self.editing_is_folder = false;
            self.rename_input.update(cx, |input, cx| {
                input.set_value(&current_name, window, cx);
                input.focus(window, cx);
            });
            cx.notify();
            return;
        }

        let script_path = match parse_node_id(item_id) {
            Some(SchemaNodeId::ScriptFile { path }) => Some(std::path::PathBuf::from(path)),
            Some(SchemaNodeId::ScriptsFolder { path: Some(p) }) => {
                Some(std::path::PathBuf::from(p))
            }
            _ => None,
        };

        if let Some(path) = script_path {
            let current_name = path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            self.editing_script_path = Some(path);
            self.rename_input.update(cx, |input, cx| {
                input.set_value(&current_name, window, cx);
                input.focus(window, cx);
            });
            cx.notify();
        }
    }

    pub(crate) fn delete_folder_from_context(&mut self, item_id: &str, cx: &mut Context<Self>) {
        if let Some(SchemaNodeId::ConnectionFolder { node_id: folder_id }) = parse_node_id(item_id)
        {
            self.app_state.update(cx, |state, cx| {
                state.delete_folder(folder_id);
                cx.emit(AppStateChanged);
            });

            self.refresh_tree(cx);
        }
    }

    pub(crate) fn move_item_to_folder(
        &mut self,
        item_id: &str,
        target_folder_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) {
        let node_id = match parse_node_id(item_id) {
            Some(SchemaNodeId::Profile { profile_id }) => self
                .app_state
                .read(cx)
                .connection_tree()
                .find_by_profile(profile_id)
                .map(|n| n.id),
            Some(SchemaNodeId::ConnectionFolder { node_id }) => Some(node_id),
            _ => None,
        };

        if let Some(node_id) = node_id {
            self.app_state.update(cx, |state, cx| {
                if state.move_tree_node(node_id, target_folder_id) {
                    cx.emit(AppStateChanged);
                }
            });
            self.refresh_tree(cx);
        }
    }

    pub fn commit_rename(&mut self, cx: &mut Context<Self>) {
        if let Some(old_path) = self.editing_script_path.take() {
            let new_name = self.rename_input.read(cx).value().to_string();

            if new_name.trim().is_empty() {
                self.refresh_scripts_tree(cx);
                cx.emit(SidebarEvent::RequestFocus);
                return;
            }

            let result = self.app_state.update(cx, |state, _cx| {
                let dir = state.scripts_directory_mut()?;
                dir.rename(&old_path, new_name.trim()).ok()
            });

            if result.is_some() {
                self.app_state.update(cx, |state, _cx| {
                    state.refresh_scripts();
                });
                self.refresh_scripts_tree(cx);
            }

            cx.emit(SidebarEvent::RequestFocus);
            return;
        }

        let Some(id) = self.editing_id.take() else {
            return;
        };

        let new_name = self.rename_input.read(cx).value().to_string();

        if new_name.trim().is_empty() {
            self.refresh_tree(cx);
            return;
        }

        let is_folder = self.editing_is_folder;

        self.app_state.update(cx, |state, cx| {
            if is_folder {
                if state.rename_folder(id, &new_name) {
                    cx.emit(AppStateChanged);
                }
            } else if let Some(profile) = state.profiles_mut().iter_mut().find(|p| p.id == id) {
                profile.name = new_name;
                state.save_profiles();
                cx.emit(AppStateChanged);
            }
        });

        self.refresh_tree(cx);
        cx.emit(SidebarEvent::RequestFocus);
    }

    /// Cancels the rename operation.
    pub fn cancel_rename(&mut self, cx: &mut Context<Self>) {
        self.editing_id = None;
        self.editing_script_path = None;
        cx.emit(SidebarEvent::RequestFocus);
        cx.notify();
    }

    pub fn start_rename_selected(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(entry) = self.active_tree_state().read(cx).selected_entry().cloned() else {
            return;
        };

        let item_id = entry.item().id.to_string();
        let kind = parse_node_kind(&item_id);

        match kind {
            SchemaNodeKind::ConnectionFolder | SchemaNodeKind::Profile => {
                self.start_rename(&item_id, window, cx);
            }
            SchemaNodeKind::ScriptFile => {
                self.start_rename(&item_id, window, cx);
            }
            SchemaNodeKind::ScriptsFolder => {
                // Only allow renaming subfolders, not root
                if let Some(SchemaNodeId::ScriptsFolder { path: Some(_) }) = parse_node_id(&item_id)
                {
                    self.start_rename(&item_id, window, cx);
                }
            }
            _ => {}
        }
    }

    pub fn toggle_add_menu(&mut self, cx: &mut Context<Self>) {
        self.add_menu_open = !self.add_menu_open;
        cx.notify();
    }

    pub fn close_add_menu(&mut self, cx: &mut Context<Self>) {
        if self.add_menu_open {
            self.add_menu_open = false;
            cx.notify();
        }
    }

    #[allow(dead_code)]
    pub fn is_add_menu_open(&self) -> bool {
        self.add_menu_open
    }

    pub fn is_renaming(&self) -> bool {
        self.editing_id.is_some() || self.editing_script_path.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::duplicate_profile_in_state;
    use dbflux_core::secrecy::{ExposeSecret, SecretString};
    use dbflux_core::{ConnectionProfile, DbConfig, SecretStore};
    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_ui_base::AppStateEntity;
    use gpui::{AppContext as _, Entity, TestAppContext};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    #[derive(Clone, Copy)]
    enum PasswordWriteOutcome {
        Success,
        FailBeforeWrite,
        WriteThenFail,
    }

    #[derive(Clone)]
    struct SecretStoreFixture {
        outcome: Arc<Mutex<PasswordWriteOutcome>>,
        values: Arc<Mutex<HashMap<String, SecretString>>>,
        writes: Arc<Mutex<Vec<String>>>,
    }

    impl SecretStoreFixture {
        fn new() -> Self {
            Self {
                outcome: Arc::new(Mutex::new(PasswordWriteOutcome::Success)),
                values: Arc::new(Mutex::new(HashMap::new())),
                writes: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn set_outcome(&self, outcome: PasswordWriteOutcome) {
            *self
                .outcome
                .lock()
                .expect("test store outcome lock poisoned") = outcome;
        }

        fn last_write(&self) -> Option<String> {
            self.writes
                .lock()
                .expect("test store write log lock poisoned")
                .last()
                .cloned()
        }
    }

    impl SecretStore for SecretStoreFixture {
        fn is_available(&self) -> bool {
            true
        }

        fn get(&self, secret_ref: &str) -> Result<Option<SecretString>, dbflux_core::DbError> {
            Ok(self
                .values
                .lock()
                .expect("test store value lock poisoned")
                .get(secret_ref)
                .cloned())
        }

        fn set(&self, secret_ref: &str, value: &SecretString) -> Result<(), dbflux_core::DbError> {
            self.writes
                .lock()
                .expect("test store write log lock poisoned")
                .push(secret_ref.to_string());

            match *self
                .outcome
                .lock()
                .expect("test store outcome lock poisoned")
            {
                PasswordWriteOutcome::Success => {
                    self.values
                        .lock()
                        .expect("test store value lock poisoned")
                        .insert(secret_ref.to_string(), value.clone());
                    Ok(())
                }
                PasswordWriteOutcome::FailBeforeWrite => Err(dbflux_core::DbError::IoError(
                    std::io::Error::other("test keyring pre-write failure"),
                )),
                PasswordWriteOutcome::WriteThenFail => {
                    self.values
                        .lock()
                        .expect("test store value lock poisoned")
                        .insert(secret_ref.to_string(), value.clone());
                    Err(dbflux_core::DbError::IoError(std::io::Error::other(
                        "test keyring write may have persisted",
                    )))
                }
            }
        }

        fn delete(&self, secret_ref: &str) -> Result<(), dbflux_core::DbError> {
            self.values
                .lock()
                .expect("test store value lock poisoned")
                .remove(secret_ref);
            Ok(())
        }
    }

    fn test_app_state(cx: &mut TestAppContext) -> Entity<AppStateEntity> {
        cx.update(|cx| {
            cx.new(|_| {
                AppStateEntity::new_with_storage_runtime(
                    StorageRuntime::in_memory().expect("test storage runtime"),
                )
                .expect("test app state")
            })
        })
    }

    fn install_secret_store(
        app_state: &Entity<AppStateEntity>,
        fixture: SecretStoreFixture,
        cx: &mut TestAppContext,
    ) {
        app_state.update(cx, |state, _| {
            let secret_store = state.secret_store();
            *secret_store
                .write()
                .expect("test secret store lock poisoned") = Box::new(fixture);
        });
    }

    fn source_profile() -> ConnectionProfile {
        let mut profile = ConnectionProfile::new("source", DbConfig::default_sqlite());
        profile.save_password = true;
        profile
    }

    fn seed_source(
        app_state: &Entity<AppStateEntity>,
        profile: &ConnectionProfile,
        cx: &mut TestAppContext,
    ) {
        app_state.update(cx, |state, _| {
            state.add_profile_in_folder(profile.clone(), None);
            state
                .save_password(profile, &SecretString::from("source password"))
                .expect("source password saves before failure fixture is enabled");
        });
    }

    fn profiles(
        app_state: &Entity<AppStateEntity>,
        cx: &mut TestAppContext,
    ) -> Vec<ConnectionProfile> {
        cx.update(|cx| app_state.read(cx).profiles().to_vec())
    }

    fn password(
        app_state: &Entity<AppStateEntity>,
        profile: &ConnectionProfile,
        cx: &mut TestAppContext,
    ) -> String {
        cx.update(|cx| {
            app_state
                .read(cx)
                .get_password(profile)
                .expect("profile secret remains readable")
                .expose_secret()
                .to_string()
        })
    }

    #[gpui::test]
    fn duplicate_profile_pre_write_failure_uses_a_fresh_target_without_committing_a_clone(
        cx: &mut TestAppContext,
    ) {
        let app_state = test_app_state(cx);
        let fixture = SecretStoreFixture::new();
        install_secret_store(&app_state, fixture.clone(), cx);
        let source = source_profile();
        seed_source(&app_state, &source, cx);
        fixture.set_outcome(PasswordWriteOutcome::FailBeforeWrite);

        let result = app_state.update(cx, |state, cx| {
            duplicate_profile_in_state(state, source.id, cx)
        });

        assert!(result.is_err());
        let persisted_profiles = profiles(&app_state, cx);
        persisted_profiles.iter().for_each(|profile| {
            assert_eq!(profile.id, source.id, "no clone profile is committed");
        });
        assert_eq!(persisted_profiles.len(), 1);
        assert_eq!(password(&app_state, &source, cx), "source password");
        assert_eq!(
            fixture
                .values
                .lock()
                .expect("test store value lock poisoned")
                .len(),
            1,
            "the pre-write error creates no clone secret slot"
        );

        let target_ref = fixture
            .last_write()
            .expect("clone attempt records a target slot");
        assert_ne!(target_ref, source.secret_ref());
        let target_id = target_ref
            .strip_prefix("dbflux:conn:")
            .expect("clone target uses the canonical connection secret namespace")
            .parse::<Uuid>()
            .expect("clone target secret reference contains a UUID");
        assert_ne!(target_id, source.id, "clone target UUID is fresh");
    }

    #[gpui::test]
    fn duplicate_profile_partial_write_error_never_commits_a_profile_or_claims_rollback(
        cx: &mut TestAppContext,
    ) {
        let app_state = test_app_state(cx);
        let fixture = SecretStoreFixture::new();
        install_secret_store(&app_state, fixture.clone(), cx);
        let source = source_profile();
        seed_source(&app_state, &source, cx);
        fixture.set_outcome(PasswordWriteOutcome::WriteThenFail);

        let result = app_state.update(cx, |state, cx| {
            duplicate_profile_in_state(state, source.id, cx)
        });

        assert!(result.is_err());
        assert_eq!(profiles(&app_state, cx).len(), 1);
        assert_eq!(password(&app_state, &source, cx), "source password");
        assert_eq!(
            fixture
                .values
                .lock()
                .expect("test store value lock poisoned")
                .len(),
            2,
            "a possible backend partial write is retained rather than rolled back"
        );
    }

    #[gpui::test]
    fn duplicate_profile_success_commits_a_new_profile_and_its_primary_password(
        cx: &mut TestAppContext,
    ) {
        let app_state = test_app_state(cx);
        let fixture = SecretStoreFixture::new();
        install_secret_store(&app_state, fixture, cx);
        let source = source_profile();
        seed_source(&app_state, &source, cx);

        let cloned_id = app_state
            .update(cx, |state, cx| {
                duplicate_profile_in_state(state, source.id, cx)
            })
            .expect("clone succeeds")
            .expect("source profile exists");
        let persisted_profiles = profiles(&app_state, cx);
        let cloned = persisted_profiles
            .iter()
            .find(|profile| profile.id == cloned_id)
            .expect("clone is committed")
            .clone();

        assert_ne!(cloned.id, source.id);
        assert_eq!(persisted_profiles.len(), 2);
        assert_eq!(password(&app_state, &cloned, cx), "source password");
    }
}
