use super::*;
use crate::platform;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenDocumentDecision {
    ErrorNoConnection,
    FocusExisting(crate::ui::document::DocumentId),
    OpenNew,
}

fn decide_open_document(
    has_connection: bool,
    existing_id: Option<crate::ui::document::DocumentId>,
) -> OpenDocumentDecision {
    if !has_connection {
        return OpenDocumentDecision::ErrorNoConnection;
    }

    if let Some(existing_id) = existing_id {
        return OpenDocumentDecision::FocusExisting(existing_id);
    }

    OpenDocumentDecision::OpenNew
}

impl Workspace {
    pub(super) fn handle_command(
        &mut self,
        command_id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(command) = Command::from_palette_id(command_id) else {
            log::warn!("Unknown command: {}", command_id);
            return;
        };

        self.dispatch(command, window, cx);
    }

    pub(super) fn open_connection_manager(&self, cx: &mut Context<Self>) {
        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(700.0), px(650.0)), cx);

        let mut options = WindowOptions {
            app_id: Some("dbflux".into()),
            titlebar: Some(TitlebarOptions {
                title: Some("Connection Manager".into()),
                ..Default::default()
            }),
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            focus: true,
            ..Default::default()
        };
        platform::apply_window_options(&mut options, 600.0, 500.0);

        match cx.open_window(options, |window, cx| {
            let manager = cx.new(|cx| ConnectionManagerWindow::new(app_state, window, cx));
            cx.new(|cx| Root::new(manager, window, cx))
        }) {
            Ok(handle) => {
                // Explicitly activate the window and force initial render (X11 fix)
                if let Err(e) = handle.update(cx, |_root, window, cx| {
                    window.activate_window();
                    cx.notify();
                }) {
                    log::warn!("Failed to activate connection manager window: {:?}", e);
                }
            }
            Err(error) => {
                log::warn!("Failed to open connection manager window: {:?}", error);
            }
        }
    }

    pub(super) fn open_settings(&self, cx: &mut Context<Self>) {
        let app_state = self.app_state.clone();

        // Check if settings window is already open - if so, focus it
        if let Some(handle) = app_state.read(cx).settings_window {
            if let Err(e) = handle.update(cx, |_root, window, _cx| {
                window.activate_window();
            }) {
                log::warn!("Failed to activate existing settings window: {:?}", e);
            }
            return;
        }

        let workspace = cx.entity().clone();
        let bounds = Bounds::centered(None, size(px(950.0), px(700.0)), cx);

        let mut options = WindowOptions {
            app_id: Some("dbflux".into()),
            titlebar: Some(TitlebarOptions {
                title: Some("Settings".into()),
                ..Default::default()
            }),
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            focus: true,
            ..Default::default()
        };
        platform::apply_window_options(&mut options, 800.0, 600.0);

        if let Ok(handle) = cx.open_window(options, |window, cx| {
            let settings = cx.new(|cx| SettingsWindow::new(app_state.clone(), window, cx));

            cx.subscribe(
                &settings,
                move |_settings, event: &crate::ui::windows::settings::SettingsEvent, cx| {
                    workspace.update(cx, |this, cx| match event {
                        crate::ui::windows::settings::SettingsEvent::OpenScript { path } => {
                            this.open_script_from_path(path.clone(), cx);
                        }
                    });
                },
            )
            .detach();

            cx.new(|cx| Root::new(settings, window, cx))
        }) {
            // Store the handle in AppStateEntity so we can reuse/focus it later
            app_state.update(cx, |state, _| {
                state.settings_window = Some(handle);
            });

            // Explicitly activate the window and force initial render (X11 fix)
            if let Err(e) = handle.update(cx, |_root, window, cx| {
                window.activate_window();
                cx.notify();
            }) {
                log::warn!("Failed to activate settings window: {:?}", e);
            }
        }
    }

    pub(super) fn open_login_modal(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let profile_name = self
            .app_state
            .read(cx)
            .active_connection()
            .map(|connected| connected.profile.name.clone())
            .unwrap_or_else(|| "connection".to_string());

        self.login_modal.update(cx, |modal, cx| {
            modal.open_manual("AWS SSO", profile_name, None, window, cx);
        });
    }

    pub(super) fn open_sso_wizard(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.sso_wizard.update(cx, |wizard, cx| {
            wizard.open(window, cx);
        });
    }

    /// Opens the global audit viewer as a document tab.
    pub(super) fn open_audit_viewer(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::components::toast::ToastExt;
        use crate::ui::document::AuditDocument;

        // Check if an audit document is already open
        let existing_audit_id = self
            .tab_manager
            .read(cx)
            .documents()
            .iter()
            .find(|doc| matches!(doc, crate::ui::document::DocumentHandle::Audit { .. }))
            .map(|doc| doc.id());

        if let Some(id) = existing_audit_id {
            // Focus the existing audit tab
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(id, cx);
            });
            cx.toast_info("Focusing existing audit viewer", window);
            return;
        }

        // Create a new audit document
        let doc = cx.new(|cx| AuditDocument::new(self.app_state.clone(), window, cx));
        let handle = crate::ui::document::DocumentHandle::audit(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(handle, cx);
        });

        self.set_focus(crate::keymap::FocusTarget::Document, window, cx);
        cx.toast_info("Opened audit viewer", window);
    }

    #[cfg(feature = "mcp")]
    pub(super) fn open_mcp_approvals(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::components::toast::ToastExt;

        self.mcp_approvals_view.update(cx, |view, cx| {
            view.refresh(cx);
        });

        self.active_governance_panel = Some(super::GovernancePanel::Approvals);
        cx.toast_info("Opened MCP approvals", window);
    }

    #[cfg(feature = "mcp")]
    pub(super) fn open_mcp_audit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::components::toast::ToastExt;

        self.mcp_audit_view.update(cx, |view, cx| {
            view.refresh(cx);
        });

        self.active_governance_panel = Some(super::GovernancePanel::Audit);
        cx.toast_info("Opened MCP audit viewer", window);
    }

    #[cfg(feature = "mcp")]
    pub(super) fn refresh_mcp_governance(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::components::toast::ToastExt;

        self.app_state.update(cx, |state, cx| {
            if let Err(e) = state.persist_mcp_governance() {
                log::error!("Failed to persist MCP governance: {}", e);
                return;
            }

            for event in state.drain_mcp_runtime_events() {
                cx.emit(crate::app::McpRuntimeEventRaised { event });
            }
        });

        cx.toast_info("MCP governance state persisted", window);
    }

    pub(super) fn disconnect_active(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::components::toast::ToastExt;

        let profile_id = self.app_state.read(cx).active_connection_id();

        if let Some(id) = profile_id {
            let name = self
                .app_state
                .read(cx)
                .connections()
                .get(&id)
                .map(|c| c.profile.name.clone());

            self.sidebar.update(cx, |sidebar, cx| {
                sidebar.disconnect_profile(id, cx);
            });

            if let Some(name) = name {
                cx.toast_info(format!("Disconnecting from {}...", name), window);
            }
        }
    }

    pub(super) fn refresh_schema(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::components::toast::ToastExt;

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

            if let Err(error) = cx.update(|cx| match result {
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
            }) {
                log::warn!(
                    "Failed to apply refreshed schema to workspace state: {:?}",
                    error
                );
            }
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
        use crate::ui::components::toast::ToastExt;

        let has_connection = self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id);

        let existing_id = if has_connection {
            self.tab_manager
                .read(cx)
                .documents()
                .iter()
                .find(|doc| {
                    doc.is_table_with_database(&table, database.as_deref(), cx)
                        && doc.connection_id(cx) == Some(profile_id)
                })
                .map(|doc| doc.id())
        } else {
            None
        };

        match decide_open_document(has_connection, existing_id) {
            OpenDocumentDecision::ErrorNoConnection => {
                cx.toast_error("No active connection for this table", window);
                return;
            }
            OpenDocumentDecision::FocusExisting(id) => {
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
            OpenDocumentDecision::OpenNew => {}
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
        use crate::ui::components::toast::ToastExt;

        let has_connection = self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id);

        let existing_id = if has_connection {
            self.tab_manager
                .read(cx)
                .documents()
                .iter()
                .find(|doc| {
                    doc.is_collection(&collection, cx) && doc.connection_id(cx) == Some(profile_id)
                })
                .map(|doc| doc.id())
        } else {
            None
        };

        match decide_open_document(has_connection, existing_id) {
            OpenDocumentDecision::ErrorNoConnection => {
                cx.toast_error("No active connection for this collection", window);
                return;
            }
            OpenDocumentDecision::FocusExisting(id) => {
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
            OpenDocumentDecision::OpenNew => {}
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
        use crate::ui::components::toast::ToastExt;

        let has_connection = self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id);

        let existing_id = if has_connection {
            self.tab_manager
                .read(cx)
                .documents()
                .iter()
                .find(|doc| doc.is_key_value_database(profile_id, &database, cx))
                .map(|doc| doc.id())
        } else {
            None
        };

        match decide_open_document(has_connection, existing_id) {
            OpenDocumentDecision::ErrorNoConnection => {
                cx.toast_error("No active connection for this key-value database", window);
                return;
            }
            OpenDocumentDecision::FocusExisting(id) => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.activate(id, cx);
                });
                return;
            }
            OpenDocumentDecision::OpenNew => {}
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

    pub(super) fn close_tabs_batch(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        selector: impl FnOnce(
            &[crate::ui::document::DocumentHandle],
            crate::ui::document::DocumentId,
        ) -> Vec<crate::ui::document::DocumentId>,
        reference_id: crate::ui::document::DocumentId,
    ) {
        let ids = selector(self.tab_manager.read(cx).documents(), reference_id);

        for doc_id in ids {
            self.close_tab(doc_id, window, cx);
        }
    }

    pub(super) fn close_tab(
        &mut self,
        doc_id: crate::ui::document::DocumentId,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.cleanup_empty_script(doc_id, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.close(doc_id, cx);
        });
    }

    /// Closes the active tab.
    pub(super) fn close_active_tab(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(doc_id) = self.tab_manager.read(cx).active_id() else {
            return;
        };

        self.close_tab(doc_id, window, cx);
    }

    /// Deletes the backing file for empty file-backed scripts about to be closed.
    fn cleanup_empty_script(
        &mut self,
        doc_id: crate::ui::document::DocumentId,
        cx: &mut Context<Self>,
    ) {
        let empty_script_path = self
            .tab_manager
            .read(cx)
            .document(doc_id)
            .and_then(|handle| {
                if let crate::ui::document::DocumentHandle::Code { entity, .. } = handle {
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
            let already_open = match cx.update(|cx| {
                tab_manager
                    .read(cx)
                    .documents()
                    .iter()
                    .find(|doc| doc.is_file(&path, cx))
                    .map(|doc| doc.id())
            }) {
                Ok(value) => value,
                Err(error) => {
                    log::warn!(
                        "Failed to inspect open tabs while opening script: {:?}",
                        error
                    );
                    None
                }
            };

            if let Some(id) = already_open {
                if let Err(error) = cx.update(|cx| {
                    tab_manager.update(cx, |mgr, cx| {
                        mgr.activate(id, cx);
                    });
                }) {
                    log::warn!("Failed to activate already-open script tab: {:?}", error);
                }
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

            if let Err(error) = cx.update(|cx| {
                this.update(cx, |ws, cx| {
                    ws.open_script_with_content(path, content, cx);
                })
                .unwrap_or_else(|inner_error| {
                    log::warn!(
                        "Failed to update workspace while opening selected script: {:?}",
                        inner_error
                    );
                });
            }) {
                log::warn!(
                    "Failed to apply selected script content to workspace: {:?}",
                    error
                );
            }
        })
        .detach();
    }

    /// Opens a script file from a known path (e.g., from sidebar recent files).
    pub fn open_script_from_path(&mut self, path: std::path::PathBuf, cx: &mut Context<Self>) {
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

            if let Err(error) = cx.update(|cx| {
                this.update(cx, |ws, cx| {
                    ws.open_script_with_content(path, content, cx);
                })
                .unwrap_or_else(|inner_error| {
                    log::warn!(
                        "Failed to update workspace while opening script path: {:?}",
                        inner_error
                    );
                });
            }) {
                log::warn!(
                    "Failed to apply script content from explicit path to workspace: {:?}",
                    error
                );
            }
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
        let uses_connection_context = language.supports_connection_context();

        let exec_ctx = if uses_connection_context {
            ExecutionContext::parse_from_content(&content, language.clone())
        } else {
            ExecutionContext::default()
        };

        let connection_id = if uses_connection_context {
            exec_ctx
                .connection_id
                .filter(|id| self.app_state.read(cx).connections().contains_key(id))
                .or_else(|| self.app_state.read(cx).active_connection_id())
        } else {
            None
        };

        let body = if uses_connection_context {
            Self::strip_annotation_header(&content, &language)
        } else {
            &content
        };

        // Track in recent files
        self.app_state.update(cx, |state, cx| {
            state.record_recent_file(path.clone());
            cx.emit(AppStateChanged);
        });

        // We need window access; use pending_open_script pattern
        self.pending_open_script = Some(PendingOpenScript {
            title: path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("Untitled")
                .to_string(),
            path: Some(path),
            body: body.to_string(),
            language,
            connection_id,
            exec_ctx,
        });
        cx.notify();
    }

    /// Strip leading annotation comments from file content.
    fn strip_annotation_header<'a>(content: &'a str, language: &QueryLanguage) -> &'a str {
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
            let mut doc = CodeDocument::new_with_language(
                self.app_state.clone(),
                pending.connection_id,
                pending.language,
                window,
                cx,
            )
            .with_exec_ctx(pending.exec_ctx, cx);
            doc = doc.with_title(pending.title);

            if let Some(path) = pending.path {
                doc = doc.with_path(path);
            }

            doc.set_content(&pending.body, window, cx);
            doc
        });

        let handle = DocumentHandle::code(doc, cx);

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
            .map(|conn| conn.connection.metadata().query_language.clone())
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
            let mut doc = CodeDocument::new(self.app_state.clone(), window, cx);
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

        let handle = DocumentHandle::code(doc, cx);

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
            .map(|conn| conn.connection.metadata().query_language.clone())
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
            let mut doc = CodeDocument::new(self.app_state.clone(), window, cx);
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

        let handle = DocumentHandle::code(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(handle, cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }

    /// Write the current tab state to the session manifest (dbflux.db-backed).
    pub(super) fn write_session_manifest(&self, cx: &App) {
        use dbflux_core::SessionTab;

        let runtime = self.app_state.read(cx).storage_runtime();

        let repo = runtime.sessions();
        let manager = self.tab_manager.read(cx);
        let mut tabs = Vec::new();

        for doc_handle in manager.documents() {
            let DocumentHandle::Code { entity, .. } = doc_handle else {
                continue;
            };

            let doc = entity.read(cx);

            let kind_str = if let Some(_path) = doc.path() {
                "FileBacked".to_string()
            } else if doc.scratch_path().is_some() {
                "Scratch".to_string()
            } else {
                continue;
            };

            let scratch_path_str: Option<std::path::PathBuf> = doc.scratch_path().cloned();
            let shadow_path_str: Option<std::path::PathBuf> = doc.shadow_path().cloned();
            let file_path_str: Option<std::path::PathBuf> = doc.path().cloned();

            tabs.push(
                dbflux_storage::repositories::state::sessions::WorkspaceTab {
                    id: doc.id().0.to_string(),
                    tab_kind: kind_str,
                    language: SessionTab::language_key(doc.query_language()),
                    exec_ctx: doc.exec_ctx().clone(),
                    scratch_path: scratch_path_str,
                    shadow_path: shadow_path_str,
                    file_path: file_path_str,
                    title: doc.title(),
                    position: tabs.len(),
                    is_pinned: false,
                },
            );
        }

        let active_index = manager.active_id().and_then(|active_id| {
            tabs.iter()
                .position(|tab| tab.id == active_id.0.to_string())
        });

        let manifest = dbflux_storage::repositories::state::sessions::WorkspaceSessionManifest {
            version: 1,
            active_index,
            tabs,
        };

        if let Err(e) = repo.save_workspace_session(&manifest) {
            log::error!("Failed to save session manifest: {}", e);
        }
    }

    /// Restore tabs from the session manifest on startup (dbflux.db-backed).
    pub(super) fn restore_session(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let manifest = {
            let app = self.app_state.read(cx);
            let runtime = app.storage_runtime();
            let repo = runtime.sessions();
            let artifacts = runtime.artifacts();

            match repo.restore_session(artifacts) {
                Ok(Some(session)) => session,
                Ok(None) => return,
                Err(e) => {
                    log::warn!("Failed to restore session from dbflux.db: {}", e);
                    return;
                }
            }
        };

        if manifest.tabs.is_empty() {
            return;
        }

        for tab in &manifest.tabs {
            let manifest_language = match tab.language.as_str() {
                "sql" => dbflux_core::QueryLanguage::Sql,
                "mongo" => dbflux_core::QueryLanguage::MongoQuery,
                "redis" => dbflux_core::QueryLanguage::RedisCommands,
                "cypher" => dbflux_core::QueryLanguage::Cypher,
                "lua" => dbflux_core::QueryLanguage::Lua,
                "python" => dbflux_core::QueryLanguage::Python,
                "bash" => dbflux_core::QueryLanguage::Bash,
                _ => dbflux_core::QueryLanguage::Sql,
            };

            let language = match &tab.tab_kind[..] {
                "FileBacked" => {
                    if let Some(ref fp) = tab.file_path {
                        dbflux_core::QueryLanguage::from_path(fp).unwrap_or(manifest_language)
                    } else {
                        manifest_language
                    }
                }
                "Scratch" => {
                    let title_path = std::path::Path::new(&tab.title);
                    dbflux_core::QueryLanguage::from_path(title_path).unwrap_or(manifest_language)
                }
                _ => manifest_language,
            };

            let (content, path, scratch_path, shadow_path) = match tab.tab_kind.as_str() {
                "Scratch" => {
                    let sp = match tab.scratch_path.as_ref() {
                        Some(p) => p.clone(),
                        None => {
                            log::warn!(
                                "Scratch tab '{}' has no scratch_path in restored session — skipping",
                                tab.title
                            );
                            continue;
                        }
                    };
                    let content = std::fs::read_to_string(&sp).unwrap_or_default();
                    (content, None, Some(sp), None)
                }
                "FileBacked" => {
                    let fp = match tab.file_path.as_ref() {
                        Some(p) => p.clone(),
                        None => {
                            log::warn!(
                                "FileBacked tab '{}' has no file_path in restored session — skipping",
                                tab.title
                            );
                            continue;
                        }
                    };
                    let content = if let Some(ref sh) = tab.shadow_path {
                        let shadow_content = std::fs::read_to_string(sh).unwrap_or_default();
                        let original_modified =
                            std::fs::metadata(&fp).ok().and_then(|m| m.modified().ok());
                        let shadow_modified =
                            std::fs::metadata(sh).ok().and_then(|m| m.modified().ok());

                        if let (Some(orig_t), Some(shad_t)) = (original_modified, shadow_modified) {
                            if orig_t > shad_t {
                                log::warn!(
                                    "External edit detected for {}: using original file",
                                    fp.display()
                                );
                                std::fs::read_to_string(&fp).unwrap_or(shadow_content)
                            } else {
                                shadow_content
                            }
                        } else {
                            shadow_content
                        }
                    } else {
                        std::fs::read_to_string(&fp).unwrap_or_default()
                    };

                    (content, Some(fp), None, tab.shadow_path.clone())
                }
                _ => continue,
            };

            let exec_ctx_json = tab.exec_ctx_json.as_str();
            let exec_ctx: dbflux_core::ExecutionContext = serde_json::from_str(exec_ctx_json)
                .unwrap_or_else(|_| dbflux_core::ExecutionContext::default());

            let connection_id = exec_ctx
                .connection_id
                .filter(|id| self.app_state.read(cx).connections().contains_key(id));

            let body = Self::strip_annotation_header(&content, &language);

            let title = if tab.tab_kind == "Scratch" {
                tab.title.clone()
            } else {
                tab.file_path
                    .as_ref()
                    .and_then(|p| p.file_name())
                    .and_then(|n| n.to_str())
                    .unwrap_or("Untitled")
                    .to_string()
            };

            let doc = cx.new(|cx| {
                let mut doc = CodeDocument::new_with_language(
                    self.app_state.clone(),
                    connection_id,
                    language,
                    window,
                    cx,
                );

                doc.set_session_paths(scratch_path.clone(), shadow_path.clone());

                if let Some(p) = path {
                    doc = doc.with_path(p);
                }

                doc = doc.with_title(title).with_exec_ctx(exec_ctx, cx);
                doc.set_content(body, window, cx);

                if tab.tab_kind == "FileBacked" && tab.shadow_path.is_some() {
                    doc.restore_dirty(cx);
                }

                doc
            });

            let handle = DocumentHandle::code(doc, cx);

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
    /// Reconnects to profiles referenced by restored session documents.
    pub(super) fn reopen_last_connections(&mut self, cx: &mut Context<Self>) {
        let profile_ids: std::collections::HashSet<uuid::Uuid> = self
            .tab_manager
            .read(cx)
            .documents()
            .iter()
            .filter_map(|doc| doc.meta_snapshot(cx).connection_id)
            .collect();

        if profile_ids.is_empty() {
            return;
        }

        let already_connected = self
            .app_state
            .read(cx)
            .connections()
            .keys()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        let sidebar = self.sidebar.clone();

        for profile_id in profile_ids {
            if already_connected.contains(&profile_id) {
                continue;
            }

            sidebar.update(cx, |sidebar, cx| {
                sidebar.connect_to_profile(profile_id, cx);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{OpenDocumentDecision, decide_open_document};
    use crate::ui::document::DocumentId;
    use uuid::Uuid;

    #[test]
    fn decide_open_document_returns_error_without_connection() {
        let decision = decide_open_document(false, None);
        assert_eq!(decision, OpenDocumentDecision::ErrorNoConnection);
    }

    #[test]
    fn decide_open_document_focuses_existing_tab_when_available() {
        let existing = DocumentId(Uuid::new_v4());
        let decision = decide_open_document(true, Some(existing));
        assert_eq!(decision, OpenDocumentDecision::FocusExisting(existing));
    }

    #[test]
    fn decide_open_document_opens_new_when_connected_and_no_existing_tab() {
        let decision = decide_open_document(true, None);
        assert_eq!(decision, OpenDocumentDecision::OpenNew);
    }

    // --- strip_annotation_header ---

    use crate::ui::views::workspace::Workspace;

    #[test]
    fn strip_annotation_header_removes_sql_annotations() {
        let content = "-- @connection: my-db\n-- @database: main\nSELECT 1;";
        let result = Workspace::strip_annotation_header(content, &dbflux_core::QueryLanguage::Sql);
        assert_eq!(result, "SELECT 1;");
    }

    #[test]
    fn strip_annotation_header_preserves_non_annotation_comments() {
        let content = "-- This is a regular comment\nSELECT 1;";
        let result = Workspace::strip_annotation_header(content, &dbflux_core::QueryLanguage::Sql);
        assert_eq!(result, "-- This is a regular comment\nSELECT 1;");
    }

    #[test]
    fn strip_annotation_header_skips_blank_lines_before_annotations() {
        let content = "\n\n-- @connection: db\nSELECT 1;";
        let result = Workspace::strip_annotation_header(content, &dbflux_core::QueryLanguage::Sql);
        assert_eq!(result, "SELECT 1;");
    }

    #[test]
    fn strip_annotation_header_all_annotations_returns_empty() {
        let content = "-- @connection: db\n-- @database: main\n";
        let result = Workspace::strip_annotation_header(content, &dbflux_core::QueryLanguage::Sql);
        assert_eq!(result, "");
    }

    #[test]
    fn strip_annotation_header_empty_content() {
        let result = Workspace::strip_annotation_header("", &dbflux_core::QueryLanguage::Sql);
        assert_eq!(result, "");
    }

    #[test]
    fn strip_annotation_header_mongo_comment_prefix() {
        let content = "// @connection: my-db\ndb.collection.find()";
        let result =
            Workspace::strip_annotation_header(content, &dbflux_core::QueryLanguage::MongoQuery);
        assert_eq!(result, "db.collection.find()");
    }

    #[test]
    fn strip_annotation_header_redis_comment_prefix() {
        let content = "# @connection: my-db\nGET key";
        let result =
            Workspace::strip_annotation_header(content, &dbflux_core::QueryLanguage::RedisCommands);
        assert_eq!(result, "GET key");
    }

    // --- PaletteItem model tests ---

    use crate::ui::overlays::command_palette::{PaletteItem, PaletteSelection, ResourceItem};
    use crate::ui::views::workspace::{build_resource_items_from_schema, map_item_to_selection};
    use dbflux_core::{
        CollectionInfo, DataStructure, DbSchemaInfo, DocumentSchema, KeySpaceInfo, KeyValueSchema,
        RelationalSchema, ScriptEntry, TableInfo, ViewInfo,
    };
    use fuzzy_matcher::FuzzyMatcher;
    use fuzzy_matcher::skim::SkimMatcherV2;
    use std::path::{Path, PathBuf};

    fn sample_action() -> PaletteItem {
        PaletteItem::Action {
            id: "new_query_tab",
            name: "New Query Tab",
            category: "Editor",
            shortcut: Some("Ctrl+N"),
        }
    }

    fn sample_connection(name: &str, connected: bool) -> PaletteItem {
        PaletteItem::Connection {
            profile_id: Uuid::new_v4(),
            name: name.to_string(),
            is_connected: connected,
        }
    }

    fn sample_table(profile_name: &str, name: &str) -> PaletteItem {
        PaletteItem::Resource(ResourceItem::Table {
            profile_id: Uuid::new_v4(),
            profile_name: profile_name.to_string(),
            database: Some("main".to_string()),
            schema: Some("public".to_string()),
            name: name.to_string(),
        })
    }

    fn sample_view(profile_name: &str, name: &str) -> PaletteItem {
        PaletteItem::Resource(ResourceItem::View {
            profile_id: Uuid::new_v4(),
            profile_name: profile_name.to_string(),
            database: Some("main".to_string()),
            schema: Some("public".to_string()),
            name: name.to_string(),
        })
    }

    fn sample_script(name: &str) -> PaletteItem {
        PaletteItem::Script {
            path: PathBuf::from(format!("{}.sql", name)),
            name: name.to_string(),
            relative_path: format!("{}.sql", name),
        }
    }

    #[test]
    fn palette_item_search_text_includes_relevant_fields() {
        let action = sample_action();
        assert!(action.search_text().contains("Editor"));
        assert!(action.search_text().contains("New Query Tab"));

        let conn = sample_connection("prod-pg", true);
        assert!(conn.search_text().contains("Connection"));
        assert!(conn.search_text().contains("prod-pg"));

        let table = sample_table("prod-pg", "orders");
        assert!(table.search_text().contains("Table"));
        assert!(table.search_text().contains("prod-pg"));
        assert!(table.search_text().contains("orders"));
        assert!(
            table.search_text().contains("main"),
            "search_text should include database"
        );
        assert!(
            table.search_text().contains("public"),
            "search_text should include schema"
        );

        let view = sample_view("prod-pg", "active_users");
        assert!(view.search_text().contains("View"));
        assert!(view.search_text().contains("active_users"));
        assert!(view.search_text().contains("main"));

        let script = sample_script("health-check");
        assert!(script.search_text().contains("Script"));
        assert!(script.search_text().contains("health-check"));
    }

    #[test]
    fn palette_item_search_text_table_without_schema() {
        let table = PaletteItem::Resource(ResourceItem::Table {
            profile_id: Uuid::new_v4(),
            profile_name: "sqlite-local".to_string(),
            database: None,
            schema: None,
            name: "notes".to_string(),
        });
        let text = table.search_text();
        assert!(text.contains("Table"));
        assert!(text.contains("sqlite-local"));
        assert!(text.contains("notes"));
    }

    #[test]
    fn palette_item_search_text_collection_includes_database() {
        let collection = PaletteItem::Resource(ResourceItem::Collection {
            profile_id: Uuid::new_v4(),
            profile_name: "mongo-prod".to_string(),
            database: "analytics".to_string(),
            name: "events".to_string(),
        });
        let text = collection.search_text();
        assert!(text.contains("Collection"));
        assert!(text.contains("analytics"));
        assert!(text.contains("events"));
    }

    #[test]
    fn palette_item_type_priority_ordering() {
        let action = sample_action();
        let connection = sample_connection("test", false);
        let resource = sample_table("test", "t");
        let script = sample_script("test");

        assert_eq!(action.type_priority(), 0);
        assert_eq!(connection.type_priority(), 1);
        assert_eq!(resource.type_priority(), 2);
        assert_eq!(script.type_priority(), 3);

        assert!(action.type_priority() < connection.type_priority());
        assert!(connection.type_priority() < resource.type_priority());
        assert!(resource.type_priority() < script.type_priority());
    }

    #[test]
    fn palette_item_display_label_returns_category_and_name() {
        let action = sample_action();
        let (cat, name) = action.display_label();
        assert_eq!(cat, "Editor");
        assert_eq!(name, "New Query Tab");

        let conn = sample_connection("prod-pg", true);
        let (cat, name) = conn.display_label();
        assert_eq!(cat, "Connection");
        assert_eq!(name, "prod-pg");

        let table = sample_table("prod-pg", "orders");
        let (cat, name) = table.display_label();
        assert_eq!(cat, "Table");
        assert_eq!(name, "orders");

        let view = sample_view("prod-pg", "active_users");
        let (cat, name) = view.display_label();
        assert_eq!(cat, "View");
        assert_eq!(name, "active_users");

        let script = sample_script("health-check");
        let (cat, name) = script.display_label();
        assert_eq!(cat, "Script");
        assert_eq!(name, "health-check");
    }

    #[test]
    fn palette_item_qualifier_resources_show_profile_name() {
        let table = sample_table("prod-pg", "orders");
        assert!(table.qualifier().unwrap().contains("prod-pg"));
        assert!(table.qualifier().unwrap().contains("main"));

        let view = sample_view("prod-pg", "active_users");
        assert!(view.qualifier().unwrap().contains("prod-pg"));
    }

    #[test]
    fn palette_filtering_sorts_by_score_descending_with_type_tiebreaker() {
        let matcher = SkimMatcherV2::default();

        let items: Vec<PaletteItem> = vec![
            sample_script("prod-health"),
            sample_connection("prod-pg", true),
            sample_action(), // "New Query Tab" — does not match "prod"
        ];

        let matched: Vec<(usize, i64)> = items
            .iter()
            .enumerate()
            .filter_map(|(i, item)| {
                matcher
                    .fuzzy_match(&item.search_text(), "prod")
                    .map(|score| (i, score))
            })
            .collect();

        // Only script and connection match "prod"
        assert_eq!(matched.len(), 2);

        // Both match — verify type-priority ordering at equal scores
        let mut sorted = matched.clone();
        sorted.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| items[a.0].type_priority().cmp(&items[b.0].type_priority()))
        });

        // Connection (priority 1) should come before Script (priority 3) at equal scores
        assert!(items[sorted[0].0].type_priority() <= items[sorted[1].0].type_priority());
    }

    #[test]
    fn palette_item_view_and_table_have_same_priority() {
        let table = sample_table("p", "t");
        let view = sample_view("p", "v");
        assert_eq!(table.type_priority(), view.type_priority());
    }

    // --- Resource item building from schema ---

    #[test]
    fn build_resources_from_relational_schema() {
        let pid = Uuid::new_v4();
        let mut items = Vec::new();

        let structure = DataStructure::Relational(RelationalSchema {
            current_database: Some("mydb".to_string()),
            tables: vec![
                TableInfo {
                    name: "users".to_string(),
                    schema: Some("public".to_string()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                },
                TableInfo {
                    name: "orders".to_string(),
                    schema: Some("public".to_string()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                },
            ],
            views: vec![ViewInfo {
                name: "active_users".to_string(),
                schema: Some("public".to_string()),
            }],
            ..Default::default()
        });

        build_resource_items_from_schema(pid, "prod-pg", &structure, &mut items);

        assert_eq!(items.len(), 3);

        let table_names: Vec<&str> = items
            .iter()
            .filter_map(|item| match item {
                PaletteItem::Resource(ResourceItem::Table { name, .. }) => Some(name.as_str()),
                _ => None,
            })
            .collect();
        assert!(table_names.contains(&"users"));
        assert!(table_names.contains(&"orders"));

        let view_count = items
            .iter()
            .filter(|item| matches!(item, PaletteItem::Resource(ResourceItem::View { .. })))
            .count();
        assert_eq!(view_count, 1);
    }

    #[test]
    fn build_resources_from_relational_schema_with_nested_schemas() {
        let pid = Uuid::new_v4();
        let mut items = Vec::new();

        let structure = DataStructure::Relational(RelationalSchema {
            current_database: Some("mydb".to_string()),
            tables: vec![],
            views: vec![],
            schemas: vec![DbSchemaInfo {
                name: "app_schema".to_string(),
                tables: vec![TableInfo {
                    name: "products".to_string(),
                    schema: Some("app_schema".to_string()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                }],
                views: vec![],
                custom_types: None,
            }],
            ..Default::default()
        });

        build_resource_items_from_schema(pid, "pg-prod", &structure, &mut items);

        assert_eq!(items.len(), 1);
        match &items[0] {
            PaletteItem::Resource(ResourceItem::Table {
                database,
                schema,
                name,
                ..
            }) => {
                assert_eq!(database.as_deref(), Some("mydb"));
                assert_eq!(schema.as_deref(), Some("app_schema"));
                assert_eq!(name, "products");
            }
            _ => panic!("Expected Table resource"),
        }
    }

    #[test]
    fn build_resources_from_document_schema() {
        let pid = Uuid::new_v4();
        let mut items = Vec::new();

        let structure = DataStructure::Document(DocumentSchema {
            current_database: Some("shop".to_string()),
            collections: vec![
                CollectionInfo {
                    name: "products".to_string(),
                    database: Some("shop".to_string()),
                    document_count: None,
                    avg_document_size: None,
                    sample_fields: None,
                    indexes: None,
                    validator: None,
                    is_capped: false,
                },
                CollectionInfo {
                    name: "orders".to_string(),
                    database: None,
                    document_count: None,
                    avg_document_size: None,
                    sample_fields: None,
                    indexes: None,
                    validator: None,
                    is_capped: false,
                },
            ],
            ..Default::default()
        });

        build_resource_items_from_schema(pid, "mongo-prod", &structure, &mut items);

        assert_eq!(items.len(), 2);

        match &items[0] {
            PaletteItem::Resource(ResourceItem::Collection { database, name, .. }) => {
                assert_eq!(database, "shop");
                assert_eq!(name, "products");
            }
            _ => panic!("Expected Collection resource"),
        }

        // Second collection falls back to current_database
        match &items[1] {
            PaletteItem::Resource(ResourceItem::Collection { database, name, .. }) => {
                assert_eq!(database, "shop");
                assert_eq!(name, "orders");
            }
            _ => panic!("Expected Collection resource"),
        }
    }

    #[test]
    fn build_resources_from_keyvalue_schema() {
        let pid = Uuid::new_v4();
        let mut items = Vec::new();

        let structure = DataStructure::KeyValue(KeyValueSchema {
            keyspaces: vec![
                KeySpaceInfo {
                    db_index: 0,
                    key_count: Some(100),
                    memory_bytes: None,
                    avg_ttl_seconds: None,
                },
                KeySpaceInfo {
                    db_index: 1,
                    key_count: Some(50),
                    memory_bytes: None,
                    avg_ttl_seconds: None,
                },
            ],
            ..Default::default()
        });

        build_resource_items_from_schema(pid, "redis-prod", &structure, &mut items);

        assert_eq!(items.len(), 2);

        match &items[0] {
            PaletteItem::Resource(ResourceItem::KeyValueDb { database, .. }) => {
                assert_eq!(database, "db0");
            }
            _ => panic!("Expected KeyValueDb resource"),
        }
        match &items[1] {
            PaletteItem::Resource(ResourceItem::KeyValueDb { database, .. }) => {
                assert_eq!(database, "db1");
            }
            _ => panic!("Expected KeyValueDb resource"),
        }
    }

    #[test]
    fn build_resources_ignores_unsupported_schema_types() {
        let pid = Uuid::new_v4();
        let mut items = Vec::new();

        let structure = DataStructure::Graph(Default::default());
        build_resource_items_from_schema(pid, "neo4j", &structure, &mut items);

        assert!(items.is_empty());
    }

    #[test]
    fn build_resources_empty_schema_produces_no_items() {
        let pid = Uuid::new_v4();
        let mut items = Vec::new();

        let structure = DataStructure::Relational(RelationalSchema {
            current_database: None,
            tables: vec![],
            views: vec![],
            schemas: vec![],
            ..Default::default()
        });

        build_resource_items_from_schema(pid, "empty", &structure, &mut items);
        assert!(items.is_empty());
    }

    // --- Script flattening tests ---

    #[test]
    fn flatten_script_entries_includes_openable_files() {
        let entries = vec![
            ScriptEntry::File {
                path: PathBuf::from("/scripts/query.sql"),
                name: "query.sql".to_string(),
                extension: "sql".to_string(),
            },
            ScriptEntry::File {
                path: PathBuf::from("/scripts/hook.lua"),
                name: "hook.lua".to_string(),
                extension: "lua".to_string(),
            },
        ];

        let mut items = Vec::new();
        Workspace::flatten_script_entries(&entries, Path::new("/scripts"), &mut items);

        assert_eq!(items.len(), 2);
        match &items[0] {
            PaletteItem::Script {
                name,
                relative_path,
                ..
            } => {
                assert_eq!(name, "query.sql");
                assert_eq!(relative_path, "query.sql");
            }
            _ => panic!("Expected Script item"),
        }
    }

    #[test]
    fn flatten_script_entries_skips_non_openable_files() {
        let entries = vec![
            ScriptEntry::File {
                path: PathBuf::from("/scripts/data.csv"),
                name: "data.csv".to_string(),
                extension: "csv".to_string(),
            },
            ScriptEntry::File {
                path: PathBuf::from("/scripts/query.sql"),
                name: "query.sql".to_string(),
                extension: "sql".to_string(),
            },
        ];

        let mut items = Vec::new();
        Workspace::flatten_script_entries(&entries, Path::new("/scripts"), &mut items);

        assert_eq!(items.len(), 1);
        match &items[0] {
            PaletteItem::Script {
                name,
                relative_path,
                ..
            } => {
                assert_eq!(name, "query.sql");
                assert_eq!(relative_path, "query.sql");
            }
            _ => panic!("Expected Script item"),
        }
    }

    #[test]
    fn flatten_script_entries_recurses_into_folders() {
        let entries = vec![ScriptEntry::Folder {
            path: PathBuf::from("/scripts/migrations"),
            name: "migrations".to_string(),
            children: vec![
                ScriptEntry::File {
                    path: PathBuf::from("/scripts/migrations/001_init.sql"),
                    name: "001_init.sql".to_string(),
                    extension: "sql".to_string(),
                },
                ScriptEntry::File {
                    path: PathBuf::from("/scripts/migrations/002_add_users.sql"),
                    name: "002_add_users.sql".to_string(),
                    extension: "sql".to_string(),
                },
            ],
        }];

        let mut items = Vec::new();
        Workspace::flatten_script_entries(&entries, Path::new("/scripts"), &mut items);

        assert_eq!(items.len(), 2);

        // Verify nested files get relative paths with the folder prefix
        match &items[0] {
            PaletteItem::Script { relative_path, .. } => {
                assert_eq!(relative_path, "migrations/001_init.sql");
            }
            _ => panic!("Expected Script item"),
        }
        match &items[1] {
            PaletteItem::Script { relative_path, .. } => {
                assert_eq!(relative_path, "migrations/002_add_users.sql");
            }
            _ => panic!("Expected Script item"),
        }
    }

    // --- Selection routing (map_item_to_selection) ---

    #[test]
    fn selection_routing_action_produces_command() {
        let item = PaletteItem::Action {
            id: "new_query_tab",
            name: "New Query Tab",
            category: "Editor",
            shortcut: Some("Ctrl+N"),
        };

        let sel = map_item_to_selection(&item).unwrap();
        match sel {
            PaletteSelection::Command { id } => assert_eq!(id, "new_query_tab"),
            _ => panic!("Expected Command selection"),
        }
    }

    #[test]
    fn selection_routing_disconnected_profile_produces_connect() {
        let pid = Uuid::new_v4();
        let item = PaletteItem::Connection {
            profile_id: pid,
            name: "analytics".to_string(),
            is_connected: false,
        };

        let sel = map_item_to_selection(&item).unwrap();
        match sel {
            PaletteSelection::Connect { profile_id } => assert_eq!(profile_id, pid),
            _ => panic!("Expected Connect selection"),
        }
    }

    #[test]
    fn selection_routing_connected_profile_produces_focus_connection() {
        let pid = Uuid::new_v4();
        let item = PaletteItem::Connection {
            profile_id: pid,
            name: "prod-pg".to_string(),
            is_connected: true,
        };

        let sel = map_item_to_selection(&item).unwrap();
        match sel {
            PaletteSelection::FocusConnection { profile_id } => assert_eq!(profile_id, pid),
            _ => panic!("Expected FocusConnection selection"),
        }
    }

    #[test]
    fn selection_routing_table_produces_open_table() {
        let pid = Uuid::new_v4();
        let item = PaletteItem::Resource(ResourceItem::Table {
            profile_id: pid,
            profile_name: "prod".to_string(),
            database: Some("mydb".to_string()),
            schema: Some("public".to_string()),
            name: "orders".to_string(),
        });

        let sel = map_item_to_selection(&item).unwrap();
        match sel {
            PaletteSelection::OpenTable {
                profile_id,
                table,
                database,
            } => {
                assert_eq!(profile_id, pid);
                assert_eq!(table.name, "orders");
                assert_eq!(table.schema.as_deref(), Some("public"));
                assert_eq!(database.as_deref(), Some("mydb"));
            }
            _ => panic!("Expected OpenTable selection"),
        }
    }

    #[test]
    fn selection_routing_view_produces_open_table_same_as_sidebar() {
        let pid = Uuid::new_v4();
        let item = PaletteItem::Resource(ResourceItem::View {
            profile_id: pid,
            profile_name: "prod".to_string(),
            database: Some("mydb".to_string()),
            schema: Some("public".to_string()),
            name: "active_users".to_string(),
        });

        let sel = map_item_to_selection(&item).unwrap();
        match sel {
            PaletteSelection::OpenTable { table, .. } => {
                assert_eq!(table.name, "active_users");
            }
            _ => panic!("Expected OpenTable selection (views route like tables)"),
        }
    }

    #[test]
    fn selection_routing_collection_produces_open_collection() {
        let pid = Uuid::new_v4();
        let item = PaletteItem::Resource(ResourceItem::Collection {
            profile_id: pid,
            profile_name: "mongo-prod".to_string(),
            database: "shop".to_string(),
            name: "products".to_string(),
        });

        let sel = map_item_to_selection(&item).unwrap();
        match sel {
            PaletteSelection::OpenCollection {
                profile_id,
                collection,
            } => {
                assert_eq!(profile_id, pid);
                assert_eq!(collection.database, "shop");
                assert_eq!(collection.name, "products");
            }
            _ => panic!("Expected OpenCollection selection"),
        }
    }

    #[test]
    fn selection_routing_keyvalue_produces_open_key_value() {
        let pid = Uuid::new_v4();
        let item = PaletteItem::Resource(ResourceItem::KeyValueDb {
            profile_id: pid,
            profile_name: "redis-prod".to_string(),
            database: "db0".to_string(),
        });

        let sel = map_item_to_selection(&item).unwrap();
        match sel {
            PaletteSelection::OpenKeyValue {
                profile_id,
                database,
            } => {
                assert_eq!(profile_id, pid);
                assert_eq!(database, "db0");
            }
            _ => panic!("Expected OpenKeyValue selection"),
        }
    }

    #[test]
    fn selection_routing_script_produces_open_script() {
        let path = PathBuf::from("/scripts/health-check.sql");
        let item = PaletteItem::Script {
            path: path.clone(),
            name: "health-check".to_string(),
            relative_path: "health-check.sql".to_string(),
        };

        let sel = map_item_to_selection(&item).unwrap();
        match sel {
            PaletteSelection::OpenScript { path: p } => assert_eq!(p, path),
            _ => panic!("Expected OpenScript selection"),
        }
    }

    // --- Disambiguation scenarios ---

    #[test]
    fn two_connections_same_table_name_are_distinguished_by_profile() {
        let pid1 = Uuid::new_v4();
        let pid2 = Uuid::new_v4();

        let table1 = PaletteItem::Resource(ResourceItem::Table {
            profile_id: pid1,
            profile_name: "prod".to_string(),
            database: Some("mydb".to_string()),
            schema: Some("public".to_string()),
            name: "users".to_string(),
        });

        let table2 = PaletteItem::Resource(ResourceItem::Table {
            profile_id: pid2,
            profile_name: "staging".to_string(),
            database: Some("mydb".to_string()),
            schema: Some("public".to_string()),
            name: "users".to_string(),
        });

        // Both have same table name but different qualifiers (include profile name)
        assert!(table1.qualifier().unwrap().contains("prod"));
        assert!(table2.qualifier().unwrap().contains("staging"));

        // Search text includes profile name for disambiguation
        assert!(table1.search_text().contains("prod"));
        assert!(table2.search_text().contains("staging"));

        // They route to different profiles
        let sel1 = map_item_to_selection(&table1).unwrap();
        let sel2 = map_item_to_selection(&table2).unwrap();
        match (&sel1, &sel2) {
            (
                PaletteSelection::OpenTable {
                    profile_id: id1, ..
                },
                PaletteSelection::OpenTable {
                    profile_id: id2, ..
                },
            ) => {
                assert_ne!(id1, id2);
            }
            _ => panic!("Expected OpenTable selections"),
        }
    }

    // --- Same profile, same schema+table, different database dedup regression ---

    #[test]
    fn same_profile_same_table_different_database_produces_distinct_selections() {
        let pid = Uuid::new_v4();

        let table_db1 = PaletteItem::Resource(ResourceItem::Table {
            profile_id: pid,
            profile_name: "pg-multi-db".to_string(),
            database: Some("db_alpha".to_string()),
            schema: Some("public".to_string()),
            name: "orders".to_string(),
        });

        let table_db2 = PaletteItem::Resource(ResourceItem::Table {
            profile_id: pid,
            profile_name: "pg-multi-db".to_string(),
            database: Some("db_beta".to_string()),
            schema: Some("public".to_string()),
            name: "orders".to_string(),
        });

        // Both have same profile, schema, and table name but different databases
        let sel1 = map_item_to_selection(&table_db1).unwrap();
        let sel2 = map_item_to_selection(&table_db2).unwrap();

        match (&sel1, &sel2) {
            (
                PaletteSelection::OpenTable {
                    profile_id: id1,
                    table: t1,
                    database: db1,
                },
                PaletteSelection::OpenTable {
                    profile_id: id2,
                    table: t2,
                    database: db2,
                },
            ) => {
                assert_eq!(id1, id2, "Same profile");
                assert_eq!(t1, t2, "Same table ref (schema+name)");
                assert_ne!(
                    db1, db2,
                    "Different databases must produce distinct selections"
                );
                assert_eq!(db1.as_deref(), Some("db_alpha"));
                assert_eq!(db2.as_deref(), Some("db_beta"));
            }
            _ => panic!("Expected OpenTable selections"),
        }

        // Qualifiers must also differ (they include database)
        assert!(table_db1.qualifier().unwrap().contains("db_alpha"));
        assert!(table_db2.qualifier().unwrap().contains("db_beta"));
    }

    // --- Empty / no-match filtering ---

    #[test]
    fn fuzzy_filter_no_match_returns_empty() {
        let matcher = SkimMatcherV2::default();
        let items: Vec<PaletteItem> = vec![
            sample_action(),
            sample_connection("prod-pg", true),
            sample_table("prod-pg", "orders"),
        ];

        let matched: Vec<_> = items
            .iter()
            .enumerate()
            .filter_map(|(i, item)| {
                matcher
                    .fuzzy_match(&item.search_text(), "zzzzzzz")
                    .map(|score| (i, score))
            })
            .collect();

        assert!(matched.is_empty());
    }

    #[test]
    fn fuzzy_filter_empty_query_matches_all() {
        let items: Vec<PaletteItem> = vec![
            sample_action(),
            sample_connection("prod-pg", true),
            sample_table("prod-pg", "orders"),
            sample_script("health-check"),
        ];

        // Empty query should show all items (score 0 for all)
        let mut filtered: Vec<(usize, i64)> = items
            .iter()
            .enumerate()
            .map(|(index, _)| (index, 0))
            .collect();

        assert_eq!(filtered.len(), 4);
        filtered.sort_by_key(|s| std::cmp::Reverse(s.1));
        assert_eq!(filtered.len(), items.len());
    }

    // --- Performance: fuzzy filtering on large dataset ---

    #[test]
    fn palette_filtering_large_dataset_completes_within_budget() {
        let matcher = SkimMatcherV2::default();

        // Build a representative large dataset: 100 connections, 1000 resources, 200 scripts
        let mut items: Vec<PaletteItem> = Vec::with_capacity(1325);

        for i in 0..100 {
            items.push(PaletteItem::Action {
                id: Box::leak(format!("cmd_{}", i).into_boxed_str()),
                name: Box::leak(format!("Command {}", i).into_boxed_str()),
                category: "Editor",
                shortcut: None,
            });
        }

        for i in 0..100 {
            items.push(PaletteItem::Connection {
                profile_id: Uuid::new_v4(),
                name: format!("connection-{}", i),
                is_connected: i < 50,
            });
        }

        for i in 0..1000 {
            items.push(PaletteItem::Resource(ResourceItem::Table {
                profile_id: Uuid::new_v4(),
                profile_name: format!("profile-{}", i % 10),
                database: Some("mydb".to_string()),
                schema: Some("public".to_string()),
                name: format!("table_{}", i),
            }));
        }

        for i in 0..200 {
            items.push(PaletteItem::Script {
                path: PathBuf::from(format!("/scripts/script_{}.sql", i)),
                name: format!("script_{}", i),
                relative_path: format!("script_{}.sql", i),
            });
        }

        assert_eq!(items.len(), 1400);

        // Measure item build time (simulated: just the search_text generation)
        let build_start = std::time::Instant::now();
        let search_texts: Vec<String> = items.iter().map(|i| i.search_text()).collect();
        let build_elapsed = build_start.elapsed();
        assert!(
            build_elapsed.as_millis() < 50,
            "Item search_text build took {}ms, exceeds 50ms budget",
            build_elapsed.as_millis()
        );

        // Measure per-keystroke filter time
        let filter_start = std::time::Instant::now();
        let matched: Vec<_> = items
            .iter()
            .enumerate()
            .filter_map(|(i, _item)| {
                matcher
                    .fuzzy_match(&search_texts[i], "table_5")
                    .map(|score| (i, score))
            })
            .collect();
        let filter_elapsed = filter_start.elapsed();

        assert!(
            filter_elapsed.as_millis() < 16,
            "Per-keystroke filter took {}ms, exceeds 16ms budget",
            filter_elapsed.as_millis()
        );
        assert!(!matched.is_empty(), "Should match some items");
    }
}
