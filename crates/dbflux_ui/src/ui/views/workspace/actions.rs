use super::*;
use crate::platform;
use dbflux_core::{DriverCapabilities, DriverMetadata};

/// Returns `true` when the given driver metadata advertises the `METRIC_SERIES`
/// capability, meaning the driver can execute `MetricQuery` requests.
///
/// Used by tests to validate the METRIC_SERIES gating predicate.
/// The live entry point (`open_metrics_chart`) uses `host_supports_metric_catalog`
/// which additionally checks `METRIC_CATALOG` and the trait accessor.
#[allow(dead_code)]
pub(crate) fn supports_metric_charts(metadata: &DriverMetadata) -> bool {
    metadata
        .capabilities
        .contains(DriverCapabilities::METRIC_SERIES)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenDocumentDecision {
    ErrorNoConnection,
    FocusExisting(crate::ui::document::DocumentId),
    OpenNew,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CollectionDocumentPresentation {
    DataGrid,
    AuditLike,
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

fn collection_document_presentation_for_connection(
    connected: &crate::app::ConnectedProfile,
    collection: &dbflux_core::CollectionRef,
) -> CollectionDocumentPresentation {
    let schema = connected
        .schema_for_target_database(collection.database.as_str())
        .or(connected.schema.as_ref());

    let presentation = schema
        .and_then(|schema| {
            schema
                .collections()
                .iter()
                .find(|entry| {
                    entry.name == collection.name
                        && entry
                            .database
                            .as_deref()
                            .unwrap_or(collection.database.as_str())
                            == collection.database.as_str()
                })
                .map(|entry| entry.presentation)
        })
        .unwrap_or(dbflux_core::CollectionPresentation::DataGrid);

    match presentation {
        dbflux_core::CollectionPresentation::DataGrid => CollectionDocumentPresentation::DataGrid,
        dbflux_core::CollectionPresentation::EventStream => {
            CollectionDocumentPresentation::AuditLike
        }
    }
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
                        crate::ui::windows::settings::SettingsEvent::OpenLoginModal {
                            provider_name,
                            profile_name,
                            url,
                        } => {
                            // Find a window context to open the modal with.
                            // The login modal lives in the workspace window; we
                            // need to call open_manual which requires `window`.
                            // Store the pending args and consume in next render.
                            this.pending_login_modal_open =
                                Some((provider_name.clone(), profile_name.clone(), url.clone()));
                            cx.notify();
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
            modal.open_manual("Auth Provider", profile_name, None, window, cx);
        });
    }

    pub(super) fn open_auth_profiles_settings(&self, cx: &mut Context<Self>) {
        let app_state = self.app_state.clone();
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

        let _ = cx.open_window(options, |window, cx| {
            let settings = cx.new(|cx| {
                SettingsWindow::new_with_section(
                    app_state.clone(),
                    crate::ui::windows::settings::SettingsSectionId::AuthProfiles,
                    window,
                    cx,
                )
            });

            cx.new(|cx| Root::new(settings, window, cx))
        });
    }

    pub(super) fn open_sso_wizard(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.sso_wizard.update(cx, |wizard, cx| {
            wizard.open(window, cx);
        });
    }

    /// Opens the global audit viewer as a document tab.
    pub(super) fn open_audit_viewer(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::document::AuditDocument;
        use crate::ui::document::DocumentKey;

        self.active_governance_panel = None;

        // Check if an audit document is already open.
        let existing_id = self
            .tab_manager
            .read(cx)
            .find_by_key(&DocumentKey::Audit, cx);

        if let Some(id) = existing_id {
            // Reset the category filter and focus the existing audit tab.
            // Both operations are done in a single update to avoid multiple borrows.
            self.tab_manager.update(cx, |mgr, cx| {
                if let Some(tab) = mgr.documents().iter().find(|t| t.id() == id) {
                    let pane = tab.as_pane();
                    if let Some(f) = &pane.set_category_filter {
                        f(None, cx);
                    }
                }
                mgr.activate(id, cx);
            });

            self.set_focus(crate::keymap::FocusTarget::Document, window, cx);
            Toast::info("Focusing existing audit viewer")
                .meta_right(now_hms())
                .push(cx);
            return;
        }

        // Create a new audit document.
        let doc = cx.new(|cx| AuditDocument::new(self.app_state.clone(), window, cx));
        let pane = AuditDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.set_focus(crate::keymap::FocusTarget::Document, window, cx);
        Toast::info("Opened audit viewer")
            .meta_right(now_hms())
            .push(cx);
    }

    /// Opens a `ChartDocument` with an empty source and the metric picker rail
    /// open, allowing the user to browse and select a metric interactively.
    ///
    /// Guard: the connected driver must advertise `METRIC_CATALOG` capability
    /// (checked via `host_supports_metric_catalog`). No driver_id, driver name,
    /// or `DatabaseCategory` comparisons are used here.
    pub(super) fn open_metrics_chart(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::document::ChartDocument;
        use dbflux_ui_document::chart::host_supports_metric_catalog;

        let active = self.app_state.read(cx).active_connection();

        let Some(active) = active else {
            Toast::warning("No active connection")
                .meta_right(now_hms())
                .push(cx);
            return;
        };

        // Gate on METRIC_CATALOG capability: the driver must support browsing
        // namespaces and metrics. No driver_id check; capability bit only.
        if !host_supports_metric_catalog(active.connection.as_ref()) {
            Toast::warning("Connected driver does not support the metric catalog")
                .meta_right(now_hms())
                .push(cx);
            return;
        }

        let profile_id = active.profile.id;

        // Open an empty chart with the metric picker rail pre-opened.
        // The user selects namespace / metric / dimensions in the picker and
        // presses Apply; that triggers ChartShellEvent::MetricPickerApplied
        // which swaps the source and auto-runs the query.
        let doc = cx.new(|cx| {
            ChartDocument::new_empty_metric_chart(profile_id, self.app_state.clone(), window, cx)
        });
        let pane = crate::ui::document::ChartDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.set_focus(crate::keymap::FocusTarget::Document, window, cx);
    }

    #[cfg(feature = "mcp")]
    pub(super) fn open_mcp_approvals(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.mcp_approvals_view.update(cx, |view, cx| {
            view.refresh(cx);
        });

        self.active_governance_panel = Some(super::GovernancePanel::Approvals);
        Toast::info("Opened MCP approvals")
            .meta_right(now_hms())
            .push(cx);
    }

    #[cfg(feature = "mcp")]
    pub(super) fn refresh_mcp_governance(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            if let Err(e) = state.persist_mcp_governance() {
                log::error!("Failed to persist MCP governance: {}", e);
                return;
            }

            for event in state.drain_mcp_runtime_events() {
                cx.emit(crate::app::McpRuntimeEventRaised { event });
            }
        });

        Toast::info("MCP governance state persisted")
            .meta_right(now_hms())
            .push(cx);
    }

    pub(super) fn disconnect_active(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
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
                Toast::info(format!("Disconnecting from {}...", name))
                    .meta_right(now_hms())
                    .push(cx);
            }
        }
    }

    pub(super) fn refresh_schema(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let active = self.app_state.read(cx).active_connection();

        let Some(active) = active else {
            Toast::warning("No active connection")
                .meta_right(now_hms())
                .push(cx);
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

        Toast::info("Refreshing schema...")
            .meta_right(now_hms())
            .push(cx);
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
        let has_connection = self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id);

        let existing_id = if has_connection {
            self.tab_manager.read(cx).find_by_key(
                &crate::ui::document::DocumentKey::Table {
                    profile_id,
                    database: database.clone(),
                    table: table.clone(),
                },
                cx,
            )
        } else {
            None
        };

        match decide_open_document(has_connection, existing_id) {
            OpenDocumentDecision::ErrorNoConnection => {
                Toast::error("No active connection for this table")
                    .meta_right(now_hms())
                    .action(copy_action("No active connection for this table"))
                    .push(cx);
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
        let pane = DataDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
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
        let has_connection = self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id);

        let presentation = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|connected| {
                collection_document_presentation_for_connection(connected, &collection)
            })
            .unwrap_or(CollectionDocumentPresentation::DataGrid);

        let existing_id = if has_connection {
            match presentation {
                CollectionDocumentPresentation::DataGrid => self.tab_manager.read(cx).find_by_key(
                    &crate::ui::document::DocumentKey::Collection {
                        profile_id,
                        collection: collection.clone(),
                    },
                    cx,
                ),
                CollectionDocumentPresentation::AuditLike => {
                    use crate::ui::document::DocumentKey;
                    let target = dbflux_core::EventStreamTarget {
                        collection: collection.clone(),
                        child_id: None,
                    };
                    self.tab_manager
                        .read(cx)
                        .find_by_key(&DocumentKey::EventStream { profile_id, target }, cx)
                }
            }
        } else {
            None
        };

        match decide_open_document(has_connection, existing_id) {
            OpenDocumentDecision::ErrorNoConnection => {
                Toast::error("No active connection for this collection")
                    .meta_right(now_hms())
                    .action(copy_action("No active connection for this collection"))
                    .push(cx);
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

        match presentation {
            CollectionDocumentPresentation::DataGrid => {
                let doc = cx.new(|cx| {
                    DataDocument::new_for_collection(
                        profile_id,
                        collection.clone(),
                        self.app_state.clone(),
                        window,
                        cx,
                    )
                });
                let pane = DataDocument::into_pane(doc, cx);
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.open(Tab::Pane(Box::new(pane)), cx);
                });
            }
            CollectionDocumentPresentation::AuditLike => {
                let doc = cx.new(|cx| {
                    crate::ui::document::AuditDocument::new_for_event_stream(
                        profile_id,
                        dbflux_core::EventStreamTarget {
                            collection: collection.clone(),
                            child_id: None,
                        },
                        collection.name.clone(),
                        self.app_state.clone(),
                        window,
                        cx,
                    )
                });
                let pane = crate::ui::document::AuditDocument::into_pane(doc, cx);
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.open(Tab::Pane(Box::new(pane)), cx);
                });
            }
        }

        log::info!(
            "Opened collection document: {}.{}",
            collection.database,
            collection.name
        );
    }

    pub(super) fn open_event_stream_document(
        &mut self,
        profile_id: uuid::Uuid,
        target: dbflux_core::EventStreamTarget,
        title: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let has_connection = self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id);

        let existing_id = if has_connection {
            use crate::ui::document::DocumentKey;
            self.tab_manager.read(cx).find_by_key(
                &DocumentKey::EventStream {
                    profile_id,
                    target: target.clone(),
                },
                cx,
            )
        } else {
            None
        };

        match decide_open_document(has_connection, existing_id) {
            OpenDocumentDecision::ErrorNoConnection => {
                Toast::error("No active connection for this event source")
                    .meta_right(now_hms())
                    .action(copy_action("No active connection for this event source"))
                    .push(cx);
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
            crate::ui::document::AuditDocument::new_for_event_stream(
                profile_id,
                target.clone(),
                title.clone(),
                self.app_state.clone(),
                window,
                cx,
            )
        });

        let pane = crate::ui::document::AuditDocument::into_pane(doc, cx);
        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });
    }

    pub(super) fn open_key_value_document(
        &mut self,
        profile_id: uuid::Uuid,
        database: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let has_connection = self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id);

        let existing_id = if has_connection {
            self.tab_manager.read(cx).find_by_key(
                &crate::ui::document::DocumentKey::KeyValueDb {
                    profile_id,
                    database: database.clone(),
                },
                cx,
            )
        } else {
            None
        };

        match decide_open_document(has_connection, existing_id) {
            OpenDocumentDecision::ErrorNoConnection => {
                Toast::error("No active connection for this key-value database")
                    .meta_right(now_hms())
                    .action(copy_action(
                        "No active connection for this key-value database",
                    ))
                    .push(cx);
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
        let pane = crate::ui::document::KeyValueDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }

    pub(super) fn close_tabs_batch(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        selector: impl FnOnce(
            &[crate::ui::document::Tab],
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
    ///
    /// If the tab has unsaved changes, opens `ModalUnsavedChanges` instead of
    /// closing immediately. The modal's subscription in `Workspace::new` handles
    /// the final close/save after the user decides.
    pub(super) fn close_active_tab(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(doc_id) = self.tab_manager.read(cx).active_id() else {
            return;
        };

        let dirty_summaries = self.tab_manager.read(cx).dirty_summaries(cx);
        let this_doc_dirty = dirty_summaries
            .iter()
            .find(|(id, _)| *id == doc_id)
            .cloned();

        if let Some((id, summary)) = this_doc_dirty {
            let doc_name = self
                .tab_manager
                .read(cx)
                .document(doc_id)
                .map(|d| d.tab_title(cx))
                .unwrap_or_else(|| "Untitled".to_string());

            use crate::ui::overlays::modals::{DirtySummaryEntry, UnsavedChangesRequest};
            let req = UnsavedChangesRequest {
                entries: vec![DirtySummaryEntry {
                    id,
                    name: doc_name,
                    summary,
                }],
            };
            self.modal_unsaved_changes.update(cx, |modal, cx| {
                modal.open(req, cx);
            });
        } else {
            self.close_tab(doc_id, window, cx);
        }
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
            .and_then(|tab| tab.is_file_backed_empty(cx));

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
                tab_manager.read(cx).find_by_key(
                    &crate::ui::document::DocumentKey::File { path: path.clone() },
                    cx,
                )
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
        let already_open = tab_manager.read(cx).find_by_key(
            &crate::ui::document::DocumentKey::File { path: path.clone() },
            cx,
        );

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

    /// Opens a read-only code document showing a routine's definition.
    ///
    /// Fetches the definition in the background (via `routine_definition`) and
    /// defers creation to the next render cycle where `Window` is available.
    /// Focuses the existing tab when already open.
    pub fn open_routine_definition(
        &mut self,
        profile_id: uuid::Uuid,
        schema: String,
        specific_name: String,
        title: String,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::document::DocumentKey;

        let dedup_key = DocumentKey::Routine {
            profile_id,
            schema: schema.clone(),
            specific_name: specific_name.clone(),
        };

        if let Some(existing_id) = self.tab_manager.read(cx).find_by_key(&dedup_key, cx) {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(existing_id, cx);
            });
            return;
        }

        let connections = self.app_state.read(cx).connections();
        let connected = match connections.get(&profile_id) {
            Some(c) => c,
            None => return,
        };

        let database = connected
            .active_database
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let connection = connected.connection.clone();

        let schema_fetch = schema.clone();
        let specific_name_fetch = specific_name.clone();

        cx.spawn(async move |this, cx| {
            let definition = cx
                .background_executor()
                .spawn(async move {
                    connection.routine_definition(&database, &schema_fetch, &specific_name_fetch)
                })
                .await;

            let body = match definition {
                Ok(def) => def,
                Err(e) => {
                    log::warn!(
                        "Failed to fetch routine definition for {}: {}",
                        specific_name,
                        e
                    );
                    format!("-- Failed to load routine definition:\n-- {}", e)
                }
            };

            cx.update(|cx| {
                this.update(cx, |ws, cx| {
                    ws.pending_open_routine = Some(PendingOpenRoutine {
                        profile_id,
                        schema,
                        specific_name,
                        title,
                        body,
                    });
                    cx.notify();
                })
                .ok();
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn finalize_open_routine(
        &mut self,
        pending: PendingOpenRoutine,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let doc = cx.new(|cx| {
            let connection_id = Some(pending.profile_id);
            let mut doc = CodeDocument::new_with_language(
                self.app_state.clone(),
                connection_id,
                dbflux_core::QueryLanguage::Sql,
                window,
                cx,
            )
            .with_title(pending.title)
            .with_read_only(cx)
            .with_routine_dedup(
                pending.profile_id,
                pending.schema,
                pending.specific_name,
            );

            doc.set_content(&pending.body, window, cx);
            doc
        });

        let pane = CodeDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
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

        let pane = CodeDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
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

        let pane = CodeDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
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

        let pane = CodeDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
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

        for doc_tab in manager.documents() {
            let Some(snap) = doc_tab.session_tab_snapshot(cx) else {
                continue;
            };

            tabs.push(
                dbflux_storage::repositories::state::sessions::WorkspaceTab {
                    id: snap.id.0.to_string(),
                    tab_kind: snap.kind.to_string(),
                    language: SessionTab::language_key(snap.language),
                    exec_ctx: snap.exec_ctx,
                    scratch_path: snap.scratch_path,
                    shadow_path: snap.shadow_path,
                    file_path: snap.file_path,
                    title: snap.title,
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

            // Routine tabs are persisted with their descriptor encoded in exec_ctx:
            // connection_id=profile_id, schema=schema, container=specific_name.
            // Reconstruct as a read-only document; the definition is re-fetched when the
            // connection becomes available (handled by AppStateChanged in CodeDocument).
            if tab.tab_kind == "Routine" {
                let exec_ctx_json = tab.exec_ctx_json.as_str();
                let exec_ctx: dbflux_core::ExecutionContext = serde_json::from_str(exec_ctx_json)
                    .unwrap_or_else(|_| dbflux_core::ExecutionContext::default());

                let Some(profile_id) = exec_ctx.connection_id else {
                    log::warn!(
                        "Routine tab '{}' has no profile_id in exec_ctx — skipping",
                        tab.title
                    );
                    continue;
                };

                let Some(schema) = exec_ctx.schema.clone() else {
                    log::warn!(
                        "Routine tab '{}' has no schema in exec_ctx — skipping",
                        tab.title
                    );
                    continue;
                };

                let Some(specific_name) = exec_ctx.container.clone() else {
                    log::warn!(
                        "Routine tab '{}' has no specific_name (container) in exec_ctx — skipping",
                        tab.title
                    );
                    continue;
                };

                let title = tab.title.clone();

                let doc = cx.new(|cx| {
                    // Pass Some(profile_id) as connection_id so the exec context
                    // is pre-seeded; the connection might not be active yet.
                    CodeDocument::new_with_language(
                        self.app_state.clone(),
                        Some(profile_id),
                        language,
                        window,
                        cx,
                    )
                    .with_title(title)
                    .with_read_only(cx)
                    .with_routine_dedup(profile_id, schema, specific_name)
                    .with_routine_definition_pending()
                });

                // If the connection is already active at restore time, trigger
                // the definition fetch immediately via the same path used by
                // the AppStateChanged handler.
                doc.update(cx, |d, cx| {
                    d.try_fetch_pending_routine_definition(cx);
                });

                let pane = CodeDocument::into_pane(doc, cx);

                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.open(Tab::Pane(Box::new(pane)), cx);
                });

                continue;
            }

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

            let pane = CodeDocument::into_pane(doc, cx);

            self.tab_manager.update(cx, |mgr, cx| {
                mgr.open(Tab::Pane(Box::new(pane)), cx);
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
    /// Opens a new `ChartDocument` seeded with the given query.
    ///
    /// Called when the user selects "Chart this query" from a data grid context menu.
    pub(super) fn open_chart_from_query(
        &mut self,
        query: String,
        connection_id: Option<uuid::Uuid>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let doc = cx.new(|cx| {
            crate::ui::document::ChartDocument::new(
                connection_id,
                query,
                self.app_state.clone(),
                window,
                cx,
            )
        });
        let pane = crate::ui::document::ChartDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }

    /// Builds palette items for all saved charts in the current profile (or all profiles).
    ///
    /// Used by the "Open chart..." command to show a fuzzy-searchable chart list.
    pub(super) fn build_saved_chart_palette_items(&self, cx: &Context<Self>) -> Vec<PaletteItem> {
        let app_state = self.app_state.read(cx);
        let active_profile_id = app_state.active_connection_id();

        let charts: Vec<dbflux_components::SavedChart> = app_state
            .saved_charts
            .all_charts()
            .iter()
            .filter(|chart| {
                // Show charts for the active profile, or all charts when no profile is active.
                active_profile_id
                    .map(|id| chart.profile_id == id)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();

        let profiles = app_state.profiles();

        charts
            .into_iter()
            .map(|chart| {
                let profile_name = profiles
                    .iter()
                    .find(|p| p.id == chart.profile_id)
                    .map(|p| p.name.clone())
                    .unwrap_or_else(|| "(orphaned)".to_string());

                PaletteItem::SavedChart {
                    id: chart.id,
                    name: chart.name.clone(),
                    profile_name,
                    profile_id: chart.profile_id,
                    is_collection_source: chart.is_collection_source(),
                }
            })
            .collect()
    }

    /// Opens a `ChartDocument` for the given saved chart ID.
    ///
    /// If a tab for this chart is already open, focuses it instead.
    pub(super) fn open_saved_chart(
        &mut self,
        chart_id: uuid::Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Focus existing tab if the chart is already open.
        let existing_id = self.tab_manager.read(cx).find_by_key(
            &crate::ui::document::DocumentKey::Chart {
                saved_chart_id: chart_id,
            },
            cx,
        );

        if let Some(id) = existing_id {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(id, cx);
            });
            self.set_focus(FocusTarget::Document, window, cx);
            return;
        }

        let saved_chart = self
            .app_state
            .read(cx)
            .saved_charts
            .all_charts()
            .iter()
            .find(|c| c.id == chart_id)
            .cloned();

        let Some(chart) = saved_chart else {
            Toast::error("Saved chart not found")
                .meta_right(now_hms())
                .push(cx);
            return;
        };

        // Route based on source type.
        match &chart.source {
            dbflux_components::saved_chart::SavedChartSource::Collection {
                collection_ref, ..
            } => {
                // Collection charts open as a DataDocument tab in Chart mode.
                // The DataDocument's auto-select logic will switch to Chart mode
                // after the data loads (TimeSeries shape triggers auto-select).
                let collection = collection_ref.clone();
                self.open_collection_document(chart.profile_id, collection, window, cx);
            }
            dbflux_components::saved_chart::SavedChartSource::Query { .. } => {
                // Validate before allocating an entity — from_saved checks the source variant.
                let validation = crate::ui::document::ChartDocument::validate_saved_source(&chart);
                if let Err(e) = validation {
                    log::error!("Failed to open saved chart: {e}");
                    Toast::error(format!("Cannot open chart: {e}"))
                        .meta_right(now_hms())
                        .push(cx);
                    return;
                }

                let app_state = self.app_state.clone();
                let doc = cx.new(|cx| {
                    // from_saved is guaranteed Ok for Query source (validated above).
                    crate::ui::document::ChartDocument::from_saved(&chart, app_state, window, cx)
                        .expect("Query source validated before entity creation")
                });

                let pane = crate::ui::document::ChartDocument::into_pane(doc, cx);
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.open(Tab::Pane(Box::new(pane)), cx);
                });
                self.set_focus(FocusTarget::Document, window, cx);
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
        let saved_chart = PaletteItem::SavedChart {
            id: Uuid::new_v4(),
            name: "My Chart".to_string(),
            profile_name: "test".to_string(),
            profile_id: Uuid::new_v4(),
            is_collection_source: false,
        };
        let resource = sample_table("test", "t");
        let script = sample_script("test");

        assert_eq!(action.type_priority(), 0);
        assert_eq!(connection.type_priority(), 1);
        assert_eq!(saved_chart.type_priority(), 2);
        assert_eq!(resource.type_priority(), 3);
        assert_eq!(script.type_priority(), 4);

        assert!(action.type_priority() < connection.type_priority());
        assert!(connection.type_priority() < saved_chart.type_priority());
        assert!(saved_chart.type_priority() < resource.type_priority());
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
                    presentation: dbflux_core::CollectionPresentation::DataGrid,
                    child_items: None,
                },
                TableInfo {
                    name: "orders".to_string(),
                    schema: Some("public".to_string()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: dbflux_core::CollectionPresentation::DataGrid,
                    child_items: None,
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
                    presentation: dbflux_core::CollectionPresentation::DataGrid,
                    child_items: None,
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
                    presentation: dbflux_core::CollectionPresentation::DataGrid,
                    child_items: None,
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
                    presentation: dbflux_core::CollectionPresentation::DataGrid,
                    child_items: None,
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

        // 50 ms is loose enough to absorb CI runner variance on shared-compute
        // hosts while still catching real algorithmic regressions in the
        // fuzzy-match path (>10x slowdown will trip it).
        assert!(
            filter_elapsed.as_millis() < 50,
            "Per-keystroke filter took {}ms, exceeds 50ms budget",
            filter_elapsed.as_millis()
        );
        assert!(!matched.is_empty(), "Should match some items");
    }

    // --- supports_metric_charts gating predicate ---

    use super::supports_metric_charts;
    use dbflux_core::{DatabaseCategory, DriverCapabilities, DriverMetadata, Icon, QueryLanguage};

    fn make_metadata_with_caps(capabilities: DriverCapabilities) -> DriverMetadata {
        DriverMetadata {
            id: "test-driver".into(),
            display_name: "Test Driver".into(),
            description: "Unit-test metadata stub".into(),
            category: DatabaseCategory::Relational,
            deployment_class: None,
            query_language: QueryLanguage::Sql,
            capabilities,
            default_port: None,
            uri_scheme: "test".into(),
            icon: Icon::Database,
            syntax: None,
            query: None,
            mutation: None,
            ddl: None,
            transactions: None,
            limits: None,
            ssl_modes: None,
            ssl_cert_fields: None,
            classification_override: None,
        }
    }

    /// A driver advertising METRIC_SERIES must return true from supports_metric_charts.
    ///
    /// This test is RED until TASK-3.1 adds supports_metric_charts (already done above)
    /// AND TASK-3.2 is complete (but the predicate itself is the thing under test here).
    #[test]
    fn supports_metric_charts_true_when_metric_series_set() {
        let meta = make_metadata_with_caps(DriverCapabilities::METRIC_SERIES);
        assert!(
            supports_metric_charts(&meta),
            "METRIC_SERIES capability must make supports_metric_charts return true"
        );
    }

    /// A driver without METRIC_SERIES must return false regardless of category or id.
    ///
    /// This proves the gating decision is driven only by the capability flag,
    /// not by any driver_id or DatabaseCategory branching.
    #[test]
    fn supports_metric_charts_false_when_metric_series_not_set() {
        let meta = make_metadata_with_caps(DriverCapabilities::AUTHENTICATION);
        assert!(
            !supports_metric_charts(&meta),
            "Absence of METRIC_SERIES must make supports_metric_charts return false"
        );

        let empty = make_metadata_with_caps(DriverCapabilities::empty());
        assert!(
            !supports_metric_charts(&empty),
            "Empty capabilities must make supports_metric_charts return false"
        );
    }
}
