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
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.close_active(cx);
                });
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

    /// Opens a table in a new DataDocument tab (v0.3).
    /// If the table is already open, focuses the existing tab instead.
    pub(super) fn open_table_document(
        &mut self,
        profile_id: uuid::Uuid,
        table: dbflux_core::TableRef,
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

    /// Creates a new SQL query tab (v0.3).
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
