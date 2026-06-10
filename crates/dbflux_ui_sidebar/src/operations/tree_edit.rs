use crate::*;
use dbflux_ui_base::platform;

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
            let id = state.create_folder("New Folder", None);
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
            let id = state.create_folder("New Folder", parent_id);
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

        let Some(new_id) = self.app_state.update(cx, |state, cx| {
            let original = state
                .profiles()
                .iter()
                .find(|p| p.id == profile_id)?
                .clone();

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

            state.add_profile_in_folder(cloned.clone(), folder_id);

            if let Some(ref pw) = password {
                state.save_password(&cloned, pw);
            }
            if let Some(ref pw) = ssh_password {
                state.save_ssh_password(&cloned, pw);
            }

            cx.emit(AppStateChanged);
            Some(new_id)
        }) else {
            return;
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

        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(600.0), px(550.0)), cx);

        let mut options = WindowOptions {
            app_id: Some(dbflux_core::ReleaseChannel::current().app_id().into()),
            titlebar: Some(TitlebarOptions {
                title: Some("Connection Manager".into()),
                ..Default::default()
            }),
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            ..Default::default()
        };
        platform::apply_window_options(&mut options, 600.0, 500.0);

        if let Err(error) = cx.open_window(options, |window, cx| {
            let manager = cx
                .new(|cx| ConnectionManagerWindow::new_in_folder(app_state, folder_id, window, cx));
            cx.new(|cx| Root::new(manager, window, cx))
        }) {
            log::warn!(
                "Failed to open connection manager window for folder: {:?}",
                error
            );
        }
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
