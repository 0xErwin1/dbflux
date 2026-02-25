use super::*;

impl Workspace {
    pub(super) fn handle_command(
        &mut self,
        command_id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match command_id {
            // Editor/Document commands - route to active document
            "new_query_tab" => {
                self.new_query_tab(window, cx);
            }
            "run_query" => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::RunQuery, window, cx);
                }
            }
            "run_query_in_new_tab" => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::RunQueryInNewTab, window, cx);
                }
            }
            "save_query" => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::SaveQuery, window, cx);
                }
            }
            "open_history" => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ToggleHistoryDropdown, window, cx);
                }
            }
            "cancel_query" => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::CancelQuery, window, cx);
                }
            }

            // Tabs
            "close_tab" => {
                self.close_active_tab(window, cx);
            }
            "next_tab" => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.next_visual_tab(cx);
                });
            }
            "prev_tab" => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.prev_visual_tab(cx);
                });
            }

            // Results - route to active document
            "export_results" => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ExportResults, window, cx);
                }
            }

            // Connections
            "open_connection_manager" => {
                self.open_connection_manager(cx);
            }
            "disconnect" => {
                self.disconnect_active(window, cx);
            }
            "refresh_schema" => {
                self.refresh_schema(window, cx);
            }

            // Focus
            "focus_sidebar" => {
                self.set_focus(FocusTarget::Sidebar, window, cx);
            }
            "focus_editor" => {
                self.set_focus(FocusTarget::Document, window, cx);
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::FocusUp, window, cx);
                }
            }
            "focus_results" => {
                self.set_focus(FocusTarget::Document, window, cx);
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::FocusDown, window, cx);
                }
            }
            "focus_tasks" => {
                self.set_focus(FocusTarget::BackgroundTasks, window, cx);
            }

            // View
            "toggle_sidebar" => {
                self.toggle_sidebar(cx);
            }
            "toggle_editor" => {
                // Route to active document if it supports layout toggling
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ToggleEditor, window, cx);
                }
            }
            "toggle_results" => {
                // Route to active document if it supports layout toggling
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ToggleResults, window, cx);
                }
            }
            "toggle_tasks" => {
                self.toggle_tasks_panel(cx);
            }
            "open_settings" => {
                self.open_settings(cx);
            }

            // File operations
            "open_script_file" => {
                self.open_script_file(window, cx);
            }
            "save_file_as" => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::SaveFileAs, window, cx);
                }
            }

            _ => {
                log::warn!("Unknown command: {}", command_id);
            }
        }
    }

    pub(super) fn open_connection_manager(&self, cx: &mut Context<Self>) {
        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(700.0), px(650.0)), cx);

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
                let manager = cx.new(|cx| ConnectionManagerWindow::new(app_state, window, cx));
                cx.new(|cx| Root::new(manager, window, cx))
            },
        )
        .ok();
    }

    pub(super) fn open_settings(&self, cx: &mut Context<Self>) {
        if let Some(handle) = self.app_state.read(cx).settings_window {
            if handle
                .update(cx, |_root, window, _cx| window.activate_window())
                .is_ok()
            {
                return;
            }
            self.app_state.update(cx, |state, _| {
                state.settings_window = None;
            });
        }

        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(950.0), px(700.0)), cx);

        if let Ok(handle) = cx.open_window(
            WindowOptions {
                app_id: Some("dbflux".into()),
                titlebar: Some(TitlebarOptions {
                    title: Some("Settings".into()),
                    ..Default::default()
                }),
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                kind: WindowKind::Floating,
                focus: true,
                ..Default::default()
            },
            |window, cx| {
                let settings = cx.new(|cx| SettingsWindow::new(app_state.clone(), window, cx));
                cx.new(|cx| Root::new(settings, window, cx))
            },
        ) {
            self.app_state.update(cx, |state, _| {
                state.settings_window = Some(handle);
            });
        }
    }

    pub(super) fn disconnect_active(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::toast::ToastExt;

        let profile_id = self.app_state.read(cx).active_connection_id();

        if let Some(id) = profile_id {
            let name = self
                .app_state
                .read(cx)
                .connections()
                .get(&id)
                .map(|c| c.profile.name.clone());

            self.app_state.update(cx, |state, cx| {
                state.disconnect(id);
                cx.emit(AppStateChanged);
            });

            if let Some(name) = name {
                cx.toast_info(format!("Disconnected from {}", name), window);
            }
        }
    }

    pub(super) fn refresh_schema(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::toast::ToastExt;

        let active = self.app_state.read(cx).active_connection();

        let Some(active) = active else {
            cx.toast_warning("No active connection", window);
            return;
        };

        let conn = active.connection.clone();
        let profile_id = active.profile.id;
        let app_state = self.app_state.clone();

        let task = cx.background_executor().spawn(async move { conn.schema() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| match result {
                Ok(schema) => {
                    app_state.update(cx, |state, cx| {
                        if let Some(connected) = state.connections_mut().get_mut(&profile_id) {
                            connected.schema = Some(schema);
                        }
                        cx.emit(AppStateChanged);
                    });
                }
                Err(e) => {
                    log::error!("Failed to refresh schema: {:?}", e);
                }
            })
            .ok();
        })
        .detach();

        cx.toast_info("Refreshing schema...", window);
    }

    /// Opens a table in a new DataDocument tab, or focuses the existing one.
    pub(super) fn open_table_document(
        &mut self,
        profile_id: uuid::Uuid,
        table: dbflux_core::TableRef,
        database: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::toast::ToastExt;

        // Check if connection exists
        if !self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id)
        {
            cx.toast_error("No active connection for this table", window);
            return;
        }

        // Check if table is already open - if so, focus that tab
        let existing_id = self
            .tab_manager
            .read(cx)
            .documents()
            .iter()
            .find(|doc| doc.is_table(&table, cx))
            .map(|doc| doc.id());

        if let Some(id) = existing_id {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(id, cx);
            });
            log::info!(
                "Focused existing table document: {:?}.{:?}",
                table.schema,
                table.name
            );
            return;
        }

        // Create a DataDocument for the table
        let doc = cx.new(|cx| {
            DataDocument::new_for_table(
                profile_id,
                table.clone(),
                database.clone(),
                self.app_state.clone(),
                window,
                cx,
            )
        });
        let handle = DocumentHandle::data(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(handle, cx);
        });

        log::info!("Opened table document: {:?}.{:?}", table.schema, table.name);
    }

    pub(super) fn open_collection_document(
        &mut self,
        profile_id: uuid::Uuid,
        collection: dbflux_core::CollectionRef,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::toast::ToastExt;

        // Check if connection exists
        if !self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id)
        {
            cx.toast_error("No active connection for this collection", window);
            return;
        }

        // Check if collection is already open - if so, focus that tab
        let existing_id = self
            .tab_manager
            .read(cx)
            .documents()
            .iter()
            .find(|doc| doc.is_collection(&collection, cx))
            .map(|doc| doc.id());

        if let Some(id) = existing_id {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(id, cx);
            });
            log::info!(
                "Focused existing collection document: {}.{}",
                collection.database,
                collection.name
            );
            return;
        }

        // Create a DataDocument for the collection
        let doc = cx.new(|cx| {
            DataDocument::new_for_collection(
                profile_id,
                collection.clone(),
                self.app_state.clone(),
                window,
                cx,
            )
        });
        let handle = DocumentHandle::data(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(handle, cx);
        });

        log::info!(
            "Opened collection document: {}.{}",
            collection.database,
            collection.name
        );
    }

    pub(super) fn open_key_value_document(
        &mut self,
        profile_id: uuid::Uuid,
        database: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::toast::ToastExt;

        if !self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id)
        {
            cx.toast_error("No active connection for this key-value database", window);
            return;
        }

        let existing_id = self
            .tab_manager
            .read(cx)
            .documents()
            .iter()
            .find(|doc| doc.is_key_value_database(profile_id, &database, cx))
            .map(|doc| doc.id());

        if let Some(id) = existing_id {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(id, cx);
            });
            return;
        }

        let doc = cx.new(|cx| {
            crate::ui::document::KeyValueDocument::new(
                profile_id,
                database.clone(),
                self.app_state.clone(),
                window,
                cx,
            )
        });
        let handle = DocumentHandle::key_value(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(handle, cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }

    /// Closes the active tab.
    pub(super) fn close_active_tab(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let active_id = self.tab_manager.read(cx).active_id();
        let Some(doc_id) = active_id else {
            return;
        };

        // Delete backing file for empty file-backed scripts on close.
        let empty_script_path = self
            .tab_manager
            .read(cx)
            .active_document()
            .and_then(|handle| {
                if let crate::ui::document::DocumentHandle::SqlQuery { entity, .. } = handle {
                    let doc = entity.read(cx);
                    if doc.is_file_backed() && doc.is_content_empty(cx) {
                        return doc.path().cloned();
                    }
                }
                None
            });

        if let Some(path) = empty_script_path {
            self.app_state.update(cx, |state, cx| {
                if let Some(dir) = state.scripts_directory_mut()
                    && dir.delete(&path).is_ok()
                {
                    cx.emit(AppStateChanged);
                }
            });
        }

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.close(doc_id, cx);
        });
    }

    /// Opens a file dialog to pick a script file and opens it in a new tab.
    pub(super) fn open_script_file(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let tab_manager = self.tab_manager.clone();

        cx.spawn(async move |this, cx| {
            let file_handle = rfd::AsyncFileDialog::new()
                .set_title("Open Script")
                .add_filter("SQL Files", &["sql"])
                .add_filter("JavaScript (MongoDB)", &["js", "mongodb"])
                .add_filter("Redis", &["redis", "red"])
                .add_filter("All Files", &["*"])
                .pick_file()
                .await;

            let Some(handle) = file_handle else {
                return;
            };

            let path = handle.path().to_path_buf();

            // Check if this file is already open
            let already_open = cx
                .update(|cx| {
                    tab_manager
                        .read(cx)
                        .documents()
                        .iter()
                        .find(|doc| doc.is_file(&path, cx))
                        .map(|doc| doc.id())
                })
                .ok()
                .flatten();

            if let Some(id) = already_open {
                cx.update(|cx| {
                    tab_manager.update(cx, |mgr, cx| {
                        mgr.activate(id, cx);
                    });
                })
                .ok();
                return;
            }

            // Read file content on background thread
            let read_path = path.clone();
            let content = cx
                .background_executor()
                .spawn(async move { std::fs::read_to_string(&read_path) })
                .await;

            let content = match content {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to read file {}: {}", path.display(), e);
                    return;
                }
            };

            cx.update(|cx| {
                this.update(cx, |ws, cx| {
                    ws.open_script_with_content(path, content, cx);
                })
                .ok();
            })
            .ok();
        })
        .detach();
    }

    /// Opens a script file from a known path (e.g., from sidebar recent files).
    pub(super) fn open_script_from_path(
        &mut self,
        path: std::path::PathBuf,
        cx: &mut Context<Self>,
    ) {
        let tab_manager = self.tab_manager.clone();

        // Check if already open
        let already_open = tab_manager
            .read(cx)
            .documents()
            .iter()
            .find(|doc| doc.is_file(&path, cx))
            .map(|doc| doc.id());

        if let Some(id) = already_open {
            tab_manager.update(cx, |mgr, cx| {
                mgr.activate(id, cx);
            });
            return;
        }

        cx.spawn(async move |this, cx| {
            let read_path = path.clone();
            let content = cx
                .background_executor()
                .spawn(async move { std::fs::read_to_string(&read_path) })
                .await;

            let content = match content {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to read file {}: {}", path.display(), e);
                    return;
                }
            };

            cx.update(|cx| {
                this.update(cx, |ws, cx| {
                    ws.open_script_with_content(path, content, cx);
                })
                .ok();
            })
            .ok();
        })
        .detach();
    }

    /// Opens a script file from a known path and content (called after file read).
    fn open_script_with_content(
        &mut self,
        path: std::path::PathBuf,
        content: String,
        cx: &mut Context<Self>,
    ) {
        use dbflux_core::{ExecutionContext, QueryLanguage};

        let language = QueryLanguage::from_path(&path).unwrap_or(QueryLanguage::Sql);
        let exec_ctx = ExecutionContext::parse_from_content(&content, language);

        // Determine connection from exec_ctx or fall back to active
        let connection_id = exec_ctx
            .connection_id
            .filter(|id| self.app_state.read(cx).connections().contains_key(id))
            .or_else(|| self.app_state.read(cx).active_connection_id());

        // Strip annotation header from content before setting editor text
        let body = Self::strip_annotation_header(&content, language);

        // Track in recent files
        self.app_state.update(cx, |state, cx| {
            state.record_recent_file(path.clone());
            cx.emit(AppStateChanged);
        });

        // We need window access; use pending_open_script pattern
        self.pending_open_script = Some(PendingOpenScript {
            path,
            body: body.to_string(),
            language,
            connection_id,
            exec_ctx,
        });
        cx.notify();
    }

    /// Strip leading annotation comments from file content.
    fn strip_annotation_header(content: &str, language: QueryLanguage) -> &str {
        let prefix = language.comment_prefix();
        let mut end = 0;

        for line in content.lines() {
            let trimmed = line.trim();

            if trimmed.is_empty() {
                end += line.len() + 1;
                continue;
            }

            if let Some(after_prefix) = trimmed.strip_prefix(prefix)
                && after_prefix.trim().starts_with('@')
            {
                end += line.len() + 1;
                continue;
            }

            break;
        }

        if end >= content.len() {
            ""
        } else {
            &content[end..]
        }
    }

    pub(super) fn finalize_open_script(
        &mut self,
        pending: PendingOpenScript,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let doc = cx.new(|cx| {
            let mut doc = SqlQueryDocument::new_with_language(
                self.app_state.clone(),
                pending.connection_id,
                pending.language,
                window,
                cx,
            )
            .with_path(pending.path)
            .with_exec_ctx(pending.exec_ctx);

            doc.set_content(&pending.body, window, cx);
            doc
        });

        let handle = DocumentHandle::sql_query(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(handle, cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }

    /// Creates a new SQL query tab backed by a script file.
    pub(super) fn new_query_tab(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let query_language = self
            .app_state
            .read(cx)
            .active_connection_id()
            .and_then(|id| self.app_state.read(cx).connections().get(&id))
            .map(|conn| conn.connection.metadata().query_language)
            .unwrap_or(dbflux_core::QueryLanguage::Sql);

        let extension = query_language.default_extension();

        let script_path = self.app_state.update(cx, |state, cx| {
            let dir = state.scripts_directory_mut()?;
            let name = dir.next_available_name("Query", extension);
            let path = dir.create_file(None, &name, extension).ok();
            if path.is_some() {
                cx.emit(AppStateChanged);
            }
            path
        });

        let doc = cx.new(|cx| {
            let mut doc = SqlQueryDocument::new(self.app_state.clone(), window, cx);
            if let Some(path) = script_path {
                let title = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("Query")
                    .to_string();
                doc = doc.with_title(title).with_path(path);
            }
            doc
        });

        if !doc.read(cx).is_file_backed() {
            doc.read(cx).initial_auto_save(cx);
        }

        let handle = DocumentHandle::sql_query(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(handle, cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }

    pub(super) fn new_query_tab_with_content(
        &mut self,
        sql: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let query_language = self
            .app_state
            .read(cx)
            .active_connection_id()
            .and_then(|id| self.app_state.read(cx).connections().get(&id))
            .map(|conn| conn.connection.metadata().query_language)
            .unwrap_or(dbflux_core::QueryLanguage::Sql);

        let extension = query_language.default_extension();

        let script_path = self.app_state.update(cx, |state, cx| {
            let dir = state.scripts_directory_mut()?;
            let name = dir.next_available_name("Query", extension);
            let path = dir.create_file(None, &name, extension).ok();
            if path.is_some() {
                cx.emit(AppStateChanged);
            }
            path
        });

        let doc = cx.new(|cx| {
            let mut doc = SqlQueryDocument::new(self.app_state.clone(), window, cx);
            if let Some(ref path) = script_path {
                let title = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("Query")
                    .to_string();
                doc = doc.with_title(title).with_path(path.clone());
            }
            doc.set_content(&sql, window, cx);
            doc
        });

        if !doc.read(cx).is_file_backed() {
            doc.read(cx).initial_auto_save(cx);
        }

        // Write initial content to the script file (with annotation headers)
        if let Some(path) = script_path {
            let content = doc.read(cx).build_file_content(cx);
            if let Err(e) = std::fs::write(&path, &content) {
                log::error!("Failed to write initial script content: {}", e);
            }
        }

        let handle = DocumentHandle::sql_query(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(handle, cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }

    // === Session persistence ===

    /// Write the current tab state to the session manifest.
    pub(super) fn write_session_manifest(&self, cx: &App) {
        use dbflux_core::{SessionManifest, SessionTab, SessionTabKind};

        let Some(store) = self.app_state.read(cx).session_store() else {
            return;
        };

        let manager = self.tab_manager.read(cx);
        let mut tabs = Vec::new();

        for doc_handle in manager.documents() {
            let DocumentHandle::SqlQuery { entity, .. } = doc_handle else {
                continue;
            };

            let doc = entity.read(cx);

            let kind = if let Some(path) = doc.path() {
                SessionTabKind::FileBacked {
                    file_path: path.clone(),
                    shadow_path: doc.shadow_path().cloned(),
                }
            } else if let Some(scratch) = doc.scratch_path() {
                SessionTabKind::Scratch {
                    scratch_path: scratch.clone(),
                    title: doc.title(),
                }
            } else {
                continue;
            };

            tabs.push(SessionTab {
                id: doc.id().0.to_string(),
                kind,
                language: SessionTab::language_key(doc.query_language()),
                exec_ctx: doc.exec_ctx().clone(),
            });
        }

        let active_index = manager.active_id().and_then(|active_id| {
            tabs.iter()
                .position(|tab| tab.id == active_id.0.to_string())
        });

        let manifest = SessionManifest {
            version: 1,
            active_index,
            tabs,
        };

        if let Err(e) = store.save_manifest(&manifest) {
            log::error!("Failed to save session manifest: {}", e);
        }
    }

    /// Restore tabs from the session manifest on startup.
    pub(super) fn restore_session(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use dbflux_core::SessionTabKind;

        let manifest = {
            let app = self.app_state.read(cx);
            let Some(store) = app.session_store() else {
                return;
            };

            let Some(manifest) = store.load_manifest() else {
                return;
            };

            store.cleanup_orphans(&manifest);
            manifest
        };

        if manifest.tabs.is_empty() {
            return;
        }

        for tab in &manifest.tabs {
            let language = tab.query_language();

            let (content, path, scratch_path, shadow_path) = match &tab.kind {
                SessionTabKind::Scratch {
                    scratch_path,
                    title: _,
                } => {
                    let content = std::fs::read_to_string(scratch_path).unwrap_or_default();
                    (content, None, Some(scratch_path.clone()), None)
                }
                SessionTabKind::FileBacked {
                    file_path,
                    shadow_path,
                } => {
                    let content = if let Some(shadow) = shadow_path {
                        // Shadow exists: check for conflict
                        let shadow_content = std::fs::read_to_string(shadow).unwrap_or_default();
                        let original_modified = std::fs::metadata(file_path)
                            .ok()
                            .and_then(|m| m.modified().ok());
                        let shadow_modified = std::fs::metadata(shadow)
                            .ok()
                            .and_then(|m| m.modified().ok());

                        if let (Some(orig_t), Some(shad_t)) = (original_modified, shadow_modified) {
                            if orig_t > shad_t {
                                // Original was modified after shadow — external edit.
                                // Prefer the original file content (user can undo).
                                log::warn!(
                                    "External edit detected for {}: using original file",
                                    file_path.display()
                                );
                                std::fs::read_to_string(file_path).unwrap_or(shadow_content)
                            } else {
                                shadow_content
                            }
                        } else {
                            shadow_content
                        }
                    } else {
                        std::fs::read_to_string(file_path).unwrap_or_default()
                    };

                    (content, Some(file_path.clone()), None, shadow_path.clone())
                }
            };

            let connection_id = tab
                .exec_ctx
                .connection_id
                .filter(|id| self.app_state.read(cx).connections().contains_key(id));

            let exec_ctx = tab.exec_ctx.clone();

            let body = Self::strip_annotation_header(&content, language);

            let title = match &tab.kind {
                SessionTabKind::Scratch { title, .. } => title.clone(),
                SessionTabKind::FileBacked { file_path, .. } => file_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("Untitled")
                    .to_string(),
            };

            let doc = cx.new(|cx| {
                let mut doc = SqlQueryDocument::new_with_language(
                    self.app_state.clone(),
                    connection_id,
                    language,
                    window,
                    cx,
                );

                doc.set_session_paths(scratch_path, shadow_path);

                if let Some(p) = path {
                    doc = doc.with_path(p);
                }

                doc = doc.with_title(title).with_exec_ctx(exec_ctx);
                doc.set_content(body, window, cx);

                // If there was a shadow, the tab had unsaved changes — mark dirty
                if matches!(
                    &tab.kind,
                    SessionTabKind::FileBacked {
                        shadow_path: Some(_),
                        ..
                    }
                ) {
                    doc.restore_dirty(cx);
                }

                doc
            });

            let handle = DocumentHandle::sql_query(doc, cx);

            self.tab_manager.update(cx, |mgr, cx| {
                mgr.open(handle, cx);
            });
        }

        // Restore active tab
        if let Some(active_idx) = manifest.active_index {
            let docs: Vec<_> = self
                .tab_manager
                .read(cx)
                .documents()
                .iter()
                .map(|d| d.id())
                .collect();

            if let Some(id) = docs.get(active_idx) {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.activate(*id, cx);
                });
            }
        }
    }
}
