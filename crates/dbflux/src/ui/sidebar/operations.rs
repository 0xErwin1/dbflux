use super::*;

impl Sidebar {
    pub(super) fn handle_database_click(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(SchemaNodeId::Database {
            profile_id,
            name: db_name,
        }) = parse_node_id(item_id)
        else {
            return;
        };

        let strategy = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.schema_loading_strategy());

        match strategy {
            Some(SchemaLoadingStrategy::LazyPerDatabase) => {
                self.handle_lazy_database_click(profile_id, &db_name, cx);
            }
            Some(SchemaLoadingStrategy::ConnectionPerDatabase) => {
                self.handle_connection_per_database_click(profile_id, &db_name, cx);
            }
            Some(SchemaLoadingStrategy::SingleDatabase) | None => {
                log::info!("Database click not applicable for this database type");
            }
        }
    }

    pub(super) fn close_database(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(SchemaNodeId::Database {
            profile_id,
            name: db_name,
        }) = parse_node_id(item_id)
        else {
            return;
        };

        self.app_state.update(cx, |state, cx| {
            if let Some(conn) = state.connections_mut().get_mut(&profile_id) {
                conn.database_schemas.remove(&db_name);

                if let Some(db_conn) = conn.database_connections.remove(&db_name) {
                    std::thread::spawn(move || {
                        let _ = db_conn.connection.cancel_active();
                        drop(db_conn);
                    });
                }

                if conn.active_database.as_deref() == Some(db_name.as_str()) {
                    conn.active_database = None;
                }
            }
            cx.emit(AppStateChanged);
        });

        // Collapse the database node in the tree
        self.set_expanded(item_id, false, cx);

        self.refresh_tree(cx);
    }

    /// Creates a new folder at the root level.
    pub fn create_root_folder(&mut self, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            state.create_folder("New Folder", None);
            cx.emit(AppStateChanged);
        });

        self.refresh_tree(cx);
    }

    pub(super) fn create_folder_from_context(&mut self, item_id: &str, cx: &mut Context<Self>) {
        // Determine parent folder ID from item_id
        let parent_id = match parse_node_id(item_id) {
            Some(SchemaNodeId::ConnectionFolder { node_id }) => Some(node_id),
            _ => None,
        };

        // Create folder with default name
        self.app_state.update(cx, |state, cx| {
            state.create_folder("New Folder", parent_id);
            cx.emit(AppStateChanged);
        });

        self.refresh_tree(cx);
    }

    pub(super) fn create_connection_in_folder(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(SchemaNodeId::ConnectionFolder { node_id: folder_id }) = parse_node_id(item_id)
        else {
            return;
        };

        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(600.0), px(550.0)), cx);

        cx.open_window(
            WindowOptions {
                app_id: Some("dbflux".into()),
                titlebar: Some(TitlebarOptions {
                    title: Some("Connection Manager".into()),
                    ..Default::default()
                }),
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                kind: WindowKind::Floating,
                ..Default::default()
            },
            |window, cx| {
                let manager = cx.new(|cx| {
                    ConnectionManagerWindow::new_in_folder(app_state, folder_id, window, cx)
                });
                cx.new(|cx| Root::new(manager, window, cx))
            },
        )
        .ok();
    }

    pub(super) fn start_rename(
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

    pub(super) fn delete_folder_from_context(&mut self, item_id: &str, cx: &mut Context<Self>) {
        if let Some(SchemaNodeId::ConnectionFolder { node_id: folder_id }) = parse_node_id(item_id)
        {
            self.app_state.update(cx, |state, cx| {
                state.delete_folder(folder_id);
                cx.emit(AppStateChanged);
            });

            self.refresh_tree(cx);
        }
    }

    pub(super) fn move_item_to_folder(
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

    fn handle_lazy_database_click(
        &mut self,
        profile_id: Uuid,
        db_name: &str,
        cx: &mut Context<Self>,
    ) {
        let needs_fetch = self
            .app_state
            .read(cx)
            .needs_database_schema(profile_id, db_name);

        // UI state only; driver issues USE at query time via QueryRequest.database
        self.app_state.update(cx, |state, cx| {
            state.set_active_database(profile_id, Some(db_name.to_string()));
            cx.emit(AppStateChanged);
        });

        if !needs_fetch {
            self.refresh_tree(cx);
            return;
        }

        let params = match self.app_state.update(cx, |state, cx| {
            if state.is_operation_pending(profile_id, Some(db_name)) {
                return Err("Operation already pending".to_string());
            }

            let result = state.prepare_fetch_database_schema(profile_id, db_name);

            if result.is_ok() && !state.start_pending_operation(profile_id, Some(db_name)) {
                return Err("Operation started by another thread".to_string());
            }

            cx.notify();
            result
        }) {
            Ok(p) => p,
            Err(e) => {
                // Only show toast for unexpected errors, not for expected skips
                let is_expected = e.contains("already cached")
                    || e.contains("already pending")
                    || e.contains("another thread");

                if is_expected {
                    log::info!("Fetch database schema skipped: {}", e);
                } else {
                    log::error!("Failed to load database schema: {}", e);
                    self.pending_toast = Some(PendingToast {
                        message: format!("Failed to load schema: {}", e),
                        is_error: true,
                    });
                }

                self.refresh_tree(cx);
                return;
            }
        };

        if self.app_state.read(cx).is_background_task_limit_reached() {
            self.app_state.update(cx, |state, _cx| {
                state.finish_pending_operation(profile_id, Some(db_name));
            });
            self.pending_toast = Some(PendingToast {
                message: "Too many background tasks running, please wait".to_string(),
                is_error: true,
            });
            self.refresh_tree(cx);
            cx.notify();
            return;
        }

        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let result =
                state.start_task(TaskKind::LoadSchema, format!("Loading schema: {}", db_name));
            cx.emit(AppStateChanged);
            result
        });

        self.refresh_tree(cx);

        let app_state = self.app_state.clone();
        let db_name_owned = db_name.to_string();
        let sidebar = cx.entity().clone();
        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                if cancel_token.is_cancelled() {
                    log::info!("Fetch database schema task was cancelled");
                    app_state.update(cx, |state, cx| {
                        state.finish_pending_operation(profile_id, Some(&db_name_owned));
                        cx.emit(AppStateChanged);
                    });
                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.refresh_tree(cx);
                    });
                    return;
                }

                let (toast, failed) = match &result {
                    Ok(_) => {
                        app_state.update(cx, |state, _| {
                            state.complete_task(task_id);
                        });
                        (None, false)
                    }
                    Err(e) => {
                        app_state.update(cx, |state, _| {
                            state.fail_task(task_id, e.clone());
                        });
                        (
                            Some(PendingToast {
                                message: format!("Failed to load schema: {}", e),
                                is_error: true,
                            }),
                            true,
                        )
                    }
                };

                app_state.update(cx, |state, cx| {
                    state.finish_pending_operation(profile_id, Some(&db_name_owned));

                    if let Ok(res) = result {
                        state.set_database_schema(res.profile_id, res.database, res.schema);
                    }

                    cx.emit(AppStateChanged);
                    cx.notify();
                });

                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_toast = toast;

                    // Collapse database on failure
                    if failed {
                        let db_item_id = SchemaNodeId::Database {
                            profile_id,
                            name: db_name_owned.clone(),
                        }
                        .to_string();
                        sidebar.expansion_overrides.remove(&db_item_id);
                    }

                    sidebar.refresh_tree(cx);
                });
            })
            .ok();
        })
        .detach();
    }

    fn handle_connection_per_database_click(
        &mut self,
        profile_id: Uuid,
        db_name: &str,
        cx: &mut Context<Self>,
    ) {
        let already_connected = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .is_some_and(|conn| {
                conn.database_connections.contains_key(db_name)
                    || conn
                        .schema
                        .as_ref()
                        .and_then(|schema| schema.current_database())
                        .is_some_and(|current| current == db_name)
            });

        if already_connected {
            self.app_state.update(cx, |state, cx| {
                if state.get_active_database(profile_id).as_deref() != Some(db_name) {
                    state.set_active_database(profile_id, Some(db_name.to_string()));
                    cx.emit(AppStateChanged);
                }
            });

            self.refresh_tree(cx);
            return;
        }

        let params = match self.app_state.update(cx, |state, cx| {
            if state.is_operation_pending(profile_id, Some(db_name)) {
                return Err("Operation already pending".to_string());
            }

            let result = state.prepare_database_connection(profile_id, db_name);

            if result.is_ok() && !state.start_pending_operation(profile_id, Some(db_name)) {
                return Err("Operation started by another thread".to_string());
            }

            cx.notify();
            result
        }) {
            Ok(p) => p,
            Err(e) => {
                log::info!("Database connection skipped: {}", e);
                return;
            }
        };

        if self.app_state.read(cx).is_background_task_limit_reached() {
            self.app_state.update(cx, |state, _cx| {
                state.finish_pending_operation(profile_id, Some(db_name));
            });
            self.pending_toast = Some(PendingToast {
                message: "Too many background tasks running, please wait".to_string(),
                is_error: true,
            });
            self.refresh_tree(cx);
            cx.notify();
            return;
        }

        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let result = state.start_task(
                TaskKind::SwitchDatabase,
                format!("Connecting to database: {}", db_name),
            );
            cx.emit(AppStateChanged);
            result
        });

        self.refresh_tree(cx);

        let app_state = self.app_state.clone();
        let db_name_owned = db_name.to_string();
        let sidebar = cx.entity().clone();
        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                if cancel_token.is_cancelled() {
                    log::info!("Database connection task was cancelled, discarding result");
                    app_state.update(cx, |state, cx| {
                        state.finish_pending_operation(profile_id, Some(&db_name_owned));
                        cx.emit(AppStateChanged);
                    });
                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.refresh_tree(cx);
                    });
                    return;
                }

                let toast = match &result {
                    Ok(_) => {
                        app_state.update(cx, |state, _| {
                            state.complete_task(task_id);
                        });
                        None
                    }
                    Err(e) => {
                        app_state.update(cx, |state, _| {
                            state.fail_task(task_id, e.clone());
                        });
                        Some(PendingToast {
                            message: format!("Failed to connect to database: {}", e),
                            is_error: true,
                        })
                    }
                };

                app_state.update(cx, |state, cx| {
                    state.finish_pending_operation(profile_id, Some(&db_name_owned));

                    if let Ok(res) = result {
                        state.add_database_connection(
                            profile_id,
                            db_name_owned.clone(),
                            res.connection,
                            res.schema,
                        );
                        state.set_active_database(profile_id, Some(db_name_owned.clone()));
                    }

                    cx.emit(AppStateChanged);
                    cx.notify();
                });

                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_toast = toast;
                    sidebar.refresh_tree(cx);
                });
            })
            .ok();
        })
        .detach();
    }

    pub(crate) fn connect_to_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        let (params, profile_name) = match self.app_state.update(cx, |state, _cx| {
            if state.is_operation_pending(profile_id, None) {
                return Err("Connection already pending".to_string());
            }

            let result = state.prepare_connect_profile(profile_id);

            if result.is_ok() && !state.start_pending_operation(profile_id, None) {
                return Err("Operation started by another thread".to_string());
            }

            result.map(|p| {
                let name = p.profile.name.clone();
                (p, name)
            })
        }) {
            Ok(p) => p,
            Err(e) => {
                log::info!("Connect skipped: {}", e);
                return;
            }
        };

        if self.app_state.read(cx).is_background_task_limit_reached() {
            self.app_state.update(cx, |state, _cx| {
                state.finish_pending_operation(profile_id, None);
            });
            self.pending_toast = Some(PendingToast {
                message: "Too many background tasks running, please wait".to_string(),
                is_error: true,
            });
            self.refresh_tree(cx);
            cx.notify();
            return;
        }

        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let result =
                state.start_task(TaskKind::Connect, format!("Connecting to {}", profile_name));
            cx.emit(AppStateChanged);
            result
        });

        self.refresh_tree(cx);

        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();
        let task = cx
            .background_executor()
            .spawn(async move { params.execute() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                if cancel_token.is_cancelled() {
                    log::info!("Connection task was cancelled, discarding result");
                    app_state.update(cx, |state, cx| {
                        state.finish_pending_operation(profile_id, None);
                        cx.emit(AppStateChanged);
                    });
                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.refresh_tree(cx);
                    });
                    return;
                }

                let toast = match &result {
                    Ok(res) => {
                        app_state.update(cx, |state, _| {
                            state.complete_task(task_id);
                        });
                        Some(PendingToast {
                            message: format!("Connected to {}", res.profile.name),
                            is_error: false,
                        })
                    }
                    Err(e) => {
                        app_state.update(cx, |state, _| {
                            state.fail_task(task_id, e.clone());
                        });
                        Some(PendingToast {
                            message: e.clone(),
                            is_error: true,
                        })
                    }
                };

                app_state.update(cx, |state, cx| {
                    state.finish_pending_operation(profile_id, None);

                    if let Ok(res) = result {
                        state.apply_connect_profile(res.profile, res.connection, res.schema);
                    }

                    cx.emit(AppStateChanged);
                    cx.notify();
                });

                sidebar.update(cx, |sidebar, cx| {
                    sidebar.pending_toast = toast;
                    sidebar.refresh_tree(cx);
                });
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn disconnect_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            state.disconnect(profile_id);
            log::info!("Disconnected profile {}", profile_id);
            cx.notify();
        });
        self.refresh_tree(cx);
    }

    pub(super) fn refresh_connection(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            state.disconnect(profile_id);
            log::info!("Refreshing connection for profile {}", profile_id);
            cx.notify();
        });
        self.refresh_tree(cx);
        self.connect_to_profile(profile_id, cx);
    }

    pub(super) fn delete_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            if let Some(idx) = state.profiles().iter().position(|p| p.id == profile_id)
                && let Some(removed) = state.remove_profile(idx)
            {
                log::info!("Deleted profile: {}", removed.name);
            }
            cx.emit(crate::app::AppStateChanged);
        });
    }

    pub(super) fn edit_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        let profile = self
            .app_state
            .read(cx)
            .profiles()
            .iter()
            .find(|p| p.id == profile_id)
            .cloned();

        let Some(profile) = profile else {
            log::error!("Profile not found: {}", profile_id);
            return;
        };

        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(600.0), px(550.0)), cx);

        cx.open_window(
            WindowOptions {
                app_id: Some("dbflux".into()),
                titlebar: Some(TitlebarOptions {
                    title: Some("Edit Connection".into()),
                    ..Default::default()
                }),
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                kind: WindowKind::Floating,
                ..Default::default()
            },
            |window, cx| {
                let manager = cx.new(|cx| {
                    ConnectionManagerWindow::new_for_edit(app_state, &profile, window, cx)
                });
                cx.new(|cx| Root::new(manager, window, cx))
            },
        )
        .ok();
    }

    fn selected_scripts_parent_dir(&self, cx: &App) -> Option<std::path::PathBuf> {
        let entry = self.scripts_tree_state.read(cx).selected_entry()?;
        let item_id = entry.item().id.to_string();
        let node_id = parse_node_id(&item_id)?;

        match node_id {
            SchemaNodeId::ScriptsFolder { path: Some(p) } => Some(std::path::PathBuf::from(p)),
            SchemaNodeId::ScriptFile { path } => std::path::Path::new(&path)
                .parent()
                .map(|p| p.to_path_buf()),
            _ => None,
        }
    }

    fn default_script_extension(&self, cx: &App) -> &'static str {
        let state = self.app_state.read(cx);
        state
            .active_connection()
            .map(|c| c.connection.metadata().query_language.default_extension())
            .unwrap_or("sql")
    }

    /// For folders returns the folder path; for files returns the parent directory.
    pub(super) fn parent_dir_from_item_id(item_id: &str) -> Option<std::path::PathBuf> {
        match parse_node_id(item_id) {
            Some(SchemaNodeId::ScriptsFolder { path: Some(p) }) => {
                Some(std::path::PathBuf::from(p))
            }
            Some(SchemaNodeId::ScriptFile { path }) => std::path::Path::new(&path)
                .parent()
                .map(|p| p.to_path_buf()),
            _ => None,
        }
    }

    pub(super) fn create_script_file_in(
        &mut self,
        parent: Option<std::path::PathBuf>,
        cx: &mut Context<Self>,
    ) {
        let extension = self.default_script_extension(cx);
        let name = self.generate_unique_script_name(parent.as_deref(), extension, cx);

        let path = self.app_state.update(cx, |state, _cx| {
            let dir = state.scripts_directory_mut()?;
            dir.create_file(parent.as_deref(), &name, extension).ok()
        });

        if let Some(path) = path {
            self.app_state.update(cx, |state, _cx| {
                state.refresh_scripts();
            });
            self.refresh_scripts_tree(cx);

            cx.emit(SidebarEvent::OpenScript { path });
        }
    }

    pub(super) fn create_script_file(&mut self, cx: &mut Context<Self>) {
        let parent = self.selected_scripts_parent_dir(cx);
        self.create_script_file_in(parent, cx);
    }

    pub(super) fn create_script_folder_in(
        &mut self,
        parent: Option<std::path::PathBuf>,
        cx: &mut Context<Self>,
    ) {
        let name = "new_folder";

        let result = self.app_state.update(cx, |state, _cx| {
            let dir = state.scripts_directory_mut()?;
            dir.create_folder(parent.as_deref(), name).ok()
        });

        if result.is_some() {
            self.app_state.update(cx, |state, _cx| {
                state.refresh_scripts();
            });
            self.refresh_scripts_tree(cx);
        }
    }

    pub fn create_script_folder(&mut self, cx: &mut Context<Self>) {
        let parent = self.selected_scripts_parent_dir(cx);
        self.create_script_folder_in(parent, cx);
    }

    pub(super) fn import_script(&mut self, cx: &mut Context<Self>) {
        let parent = self.selected_scripts_parent_dir(cx);
        let extensions = dbflux_core::all_script_extensions();
        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();

        let task = cx.background_executor().spawn(async move {
            let mut dialog = rfd::FileDialog::new().set_title("Import Script");
            for ext in &extensions {
                dialog = dialog.add_filter("Script files", &[ext]);
            }
            dialog.pick_file()
        });

        cx.spawn(async move |_this, cx| {
            let source = match task.await {
                Some(path) => path,
                None => return,
            };

            cx.update(|cx| {
                let path = app_state.update(cx, |state, _cx| {
                    let dir = state.scripts_directory_mut()?;
                    let imported = dir.import(&source, parent.as_deref()).ok()?;
                    state.refresh_scripts();
                    Some(imported)
                });

                if let Some(path) = path {
                    sidebar.update(cx, |this, cx| {
                        this.refresh_scripts_tree(cx);
                        cx.emit(SidebarEvent::OpenScript { path });
                    });
                }
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn handle_script_drop(
        &mut self,
        state: &ScriptsDragState,
        target_item_id: &str,
        cx: &mut Context<Self>,
    ) {
        let target_dir = match parse_node_id(target_item_id) {
            Some(SchemaNodeId::ScriptsFolder { path: Some(p) }) => std::path::PathBuf::from(p),
            Some(SchemaNodeId::ScriptsFolder { path: None }) => {
                match dirs::data_dir().map(|d| d.join("dbflux").join("scripts")) {
                    Some(p) => p,
                    None => return,
                }
            }
            _ => return,
        };

        self.move_script(&state.path, &target_dir, cx);
    }

    pub(super) fn handle_script_drop_to_root(
        &mut self,
        state: &ScriptsDragState,
        cx: &mut Context<Self>,
    ) {
        let root = match self.app_state.read(cx).scripts_directory() {
            Some(dir) => dir.root_path().to_path_buf(),
            None => return,
        };

        self.move_script(&state.path, &root, cx);
    }

    fn move_script(
        &mut self,
        source: &std::path::Path,
        target_dir: &std::path::Path,
        cx: &mut Context<Self>,
    ) {
        let result = self.app_state.update(cx, |state, _cx| {
            state
                .scripts_directory_mut()?
                .move_entry(source, target_dir)
                .ok()
        });

        if result.is_some() {
            self.app_state.update(cx, |state, _cx| {
                state.refresh_scripts();
            });
            self.refresh_scripts_tree(cx);
        }
    }

    pub(super) fn delete_script(&mut self, path: &std::path::Path, cx: &mut Context<Self>) {
        let path = path.to_path_buf();
        let result = self.app_state.update(cx, |state, _cx| {
            state.scripts_directory_mut()?.delete(&path).ok()
        });

        if result.is_some() {
            self.app_state.update(cx, |state, _cx| {
                state.refresh_scripts();
            });
            self.refresh_scripts_tree(cx);
        }
    }

    fn resolve_script_path(item_id: &str) -> Option<std::path::PathBuf> {
        match parse_node_id(item_id) {
            Some(SchemaNodeId::ScriptFile { path }) => Some(std::path::PathBuf::from(path)),
            Some(SchemaNodeId::ScriptsFolder { path: Some(p) }) => {
                Some(std::path::PathBuf::from(p))
            }
            Some(SchemaNodeId::ScriptsFolder { path: None }) => {
                dirs::data_dir().map(|d| d.join("dbflux").join("scripts"))
            }
            _ => None,
        }
    }

    pub(super) fn reveal_in_file_manager(&self, item_id: &str) {
        let Some(path) = Self::resolve_script_path(item_id) else {
            return;
        };

        #[cfg(target_os = "macos")]
        {
            if path.is_file() {
                if let Err(e) = std::process::Command::new("open")
                    .arg("-R")
                    .arg(&path)
                    .spawn()
                {
                    log::error!("Failed to reveal in file manager: {}", e);
                }
            } else if let Err(e) = std::process::Command::new("open").arg(&path).spawn() {
                log::error!("Failed to reveal in file manager: {}", e);
            }
        }

        #[cfg(target_os = "windows")]
        {
            if path.is_file() {
                let select_arg = format!("/select,{}", path.display());
                if let Err(e) = std::process::Command::new("explorer")
                    .arg(&select_arg)
                    .spawn()
                {
                    log::error!("Failed to reveal in file manager: {}", e);
                }
            } else if let Err(e) = std::process::Command::new("explorer").arg(&path).spawn() {
                log::error!("Failed to reveal in file manager: {}", e);
            }
        }

        #[cfg(target_os = "linux")]
        {
            let target = if path.is_file() {
                path.parent().unwrap_or(&path).to_path_buf()
            } else {
                path
            };

            if let Err(_e) = std::process::Command::new("xdg-open").arg(&target).spawn()
                && let Err(e) = std::process::Command::new("gio")
                    .arg("open")
                    .arg(&target)
                    .spawn()
            {
                log::error!("Failed to reveal in file manager: {}", e);
            }
        }
    }

    pub(super) fn copy_path_to_clipboard(&self, item_id: &str, cx: &mut Context<Self>) {
        let Some(path) = Self::resolve_script_path(item_id) else {
            return;
        };

        cx.write_to_clipboard(ClipboardItem::new_string(
            path.to_string_lossy().to_string(),
        ));
    }

    fn generate_unique_script_name(
        &self,
        parent: Option<&std::path::Path>,
        extension: &str,
        cx: &App,
    ) -> String {
        let state = self.app_state.read(cx);
        let dir = match state.scripts_directory() {
            Some(d) => d,
            None => return format!("untitled.{}", extension),
        };

        let base_dir = parent.unwrap_or_else(|| dir.root_path());

        for i in 1u32.. {
            let name = if i == 1 {
                format!("untitled.{}", extension)
            } else {
                format!("untitled_{}.{}", i, extension)
            };

            if !base_dir.join(&name).exists() {
                return name;
            }
        }

        format!("untitled.{}", extension)
    }
}
