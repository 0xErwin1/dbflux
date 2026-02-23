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

    /// Attempts to close the active tab. If the document has unsaved changes,
    /// shows a warning toast on the first attempt. If closed again within 3
    /// seconds, force-closes regardless of unsaved changes.
    pub(super) fn close_active_tab(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::toast::ToastExt;

        let active_id = self.tab_manager.read(cx).active_id();
        let Some(doc_id) = active_id else {
            return;
        };

        // Check if this is a repeat close within the grace period
        if let Some((prev_id, timestamp)) = self.pending_force_close.take()
            && prev_id == doc_id
            && timestamp.elapsed() < std::time::Duration::from_secs(3)
        {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.force_close(doc_id, cx);
            });
            return;
        }

        let closed = self.tab_manager.update(cx, |mgr, cx| mgr.close(doc_id, cx));

        if !closed {
            self.pending_force_close = Some((doc_id, std::time::Instant::now()));
            cx.toast_warning("Unsaved changes. Close again to discard.", window);
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

    /// Creates a new SQL query tab.
    pub(super) fn new_query_tab(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        // Count existing query tabs for naming
        let query_count = self
            .tab_manager
            .read(cx)
            .documents()
            .iter()
            .filter(|d| matches!(d.kind(), crate::ui::document::DocumentKind::Script))
            .count();

        let title = format!("Query {}", query_count + 1);

        let doc = cx
            .new(|cx| SqlQueryDocument::new(self.app_state.clone(), window, cx).with_title(title));
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
        // Count existing query tabs for naming
        let query_count = self
            .tab_manager
            .read(cx)
            .documents()
            .iter()
            .filter(|d| matches!(d.kind(), crate::ui::document::DocumentKind::Script))
            .count();

        let title = format!("Query {}", query_count + 1);

        let doc = cx.new(|cx| {
            let mut doc =
                SqlQueryDocument::new(self.app_state.clone(), window, cx).with_title(title);
            doc.set_content(&sql, window, cx);
            doc
        });
        let handle = DocumentHandle::sql_query(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(handle, cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }
}
