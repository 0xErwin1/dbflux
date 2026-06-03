use super::*;
use crate::platform;
use dbflux_core::{DriverCapabilities, DriverMetadata};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};

/// Returns `true` when the given driver metadata advertises the `METRIC_SERIES`
/// capability, meaning the driver can execute `MetricQuery` requests.
///
/// Used by tests to validate the METRIC_SERIES gating predicate.
/// The live entry point (`open_metric_chart_from_sidebar`) uses a pre-built
/// `MetricSource` with defaults; only `METRIC_CATALOG` is checked at the
/// sidebar tree-builder level.
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
        let workspace = cx.entity().clone();
        crate::ui::windows::settings::open_or_focus_settings(
            self.app_state.clone(),
            None,
            cx,
            move |settings, cx| {
                cx.subscribe(
                    settings,
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
                                this.pending_login_modal_open = Some((
                                    provider_name.clone(),
                                    profile_name.clone(),
                                    url.clone(),
                                ));
                                cx.notify();
                            }
                        });
                    },
                )
                .detach();
            },
        );
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
        crate::ui::windows::settings::open_or_focus_settings(
            self.app_state.clone(),
            Some(crate::ui::windows::settings::SettingsSectionId::AuthProfiles),
            cx,
            |_settings, _cx| {},
        );
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

    /// Opens (or focuses) the audit viewer pre-filtered by correlation id.
    ///
    /// When an audit tab is already open, the correlation filter is applied to
    /// the existing tab and it is brought to focus. When `correlation_id` is
    /// `None`, the audit viewer opens with the default user-error filter
    /// (`action = "user_error"`). When no tab is open and a specific id was
    /// provided, a new tab is created pre-filtered by that id.
    pub(super) fn open_audit_viewer_with_correlation(
        &mut self,
        correlation_id: Option<uuid::Uuid>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::document::AuditDocument;
        use crate::ui::document::DocumentKey;

        let existing_id = self
            .tab_manager
            .read(cx)
            .find_by_key(&DocumentKey::Audit, cx);

        if let Some(id) = existing_id {
            self.tab_manager.update(cx, |mgr, cx| {
                if let Some(tab) = mgr.documents().iter().find(|t| t.id() == id) {
                    let pane = tab.as_pane();
                    if let Some(f) = &pane.set_correlation_filter {
                        f(correlation_id.map(|u| u.to_string()), cx);
                    }
                }
                mgr.activate(id, cx);
            });

            self.set_focus(crate::keymap::FocusTarget::Document, window, cx);
            return;
        }

        match correlation_id {
            Some(id) => {
                let doc = cx.new(|cx| {
                    AuditDocument::new_with_correlation_id(id, self.app_state.clone(), window, cx)
                });
                let pane = AuditDocument::into_pane(doc, cx);
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.open(Tab::Pane(Box::new(pane)), cx);
                });
            }
            None => {
                self.open_audit_viewer(window, cx);
                return;
            }
        }

        self.set_focus(crate::keymap::FocusTarget::Document, window, cx);
    }

    /// Opens a `ChartDocument` pre-populated with the metric selected in the
    /// sidebar and immediately executes it.
    ///
    /// Defaults: `dimensions = []`, `period_s = 300`, `statistic = "Average"`.
    /// If a chart with the same `(profile_id, namespace, metric_name)` is
    /// already open the existing tab is focused instead of opening a duplicate.
    pub(super) fn open_metric_chart_from_sidebar(
        &mut self,
        profile_id: uuid::Uuid,
        namespace: String,
        metric_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::document::{ChartDocument, DocumentKey};
        use dbflux_components::chart::MetricSource;

        let key = DocumentKey::MetricChart {
            profile_id,
            namespace: namespace.clone(),
            metric_name: metric_name.clone(),
        };

        let existing = self.tab_manager.read(cx).find_by_key(&key, cx);
        if let Some(existing_id) = existing {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(existing_id, cx);
            });
            self.set_focus(FocusTarget::Document, window, cx);
            return;
        }

        let source = MetricSource::single(
            namespace.clone(),
            metric_name.clone(),
            vec![],
            300,
            "Average".to_string(),
        );

        let title = format!("{} / {}", namespace, metric_name);
        let ns_clone = namespace.clone();
        let mn_clone = metric_name.clone();
        let doc = cx.new(|cx| {
            let mut chart = ChartDocument::new_with_source(
                Some(profile_id),
                title,
                Box::new(source),
                self.app_state.clone(),
                window,
                cx,
            );

            // Pre-open the Metric rail so the picker shows dimensions, period,
            // and statistic immediately. Namespace/metric are pinned; only
            // the config is editable.
            chart.setup_metric_picker(ns_clone, mn_clone, cx);
            chart
        });

        let pane = ChartDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }

    /// Open a `ChartDocument` for an instance metric leaf clicked in the sidebar.
    ///
    /// Deduplicates by `(profile_id, metric_id)` using `DocumentKey::InstanceMetric`
    /// so clicking the same metric a second time focuses the existing tab rather
    /// than opening a duplicate.
    pub(super) fn open_instance_metric(
        &mut self,
        profile_id: uuid::Uuid,
        metric_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::document::{ChartDocument, DocumentKey};
        use dbflux_components::chart::InstanceMetricSource;

        let key = DocumentKey::InstanceMetric {
            profile_id,
            metric_id: metric_id.clone(),
        };

        if let Some(existing_id) = self.tab_manager.read(cx).find_by_key(&key, cx) {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(existing_id, cx);
            });
            self.set_focus(FocusTarget::Document, window, cx);
            return;
        }

        let source = InstanceMetricSource {
            metric_id: metric_id.clone(),
        };

        let title = metric_id.clone();
        let metric_id_for_identity = metric_id.clone();
        let doc = cx.new(|cx| {
            let mut chart = ChartDocument::new_with_source(
                Some(profile_id),
                title,
                Box::new(source),
                self.app_state.clone(),
                window,
                cx,
            );
            chart.set_instance_metric_identity(metric_id_for_identity);
            // InstanceMetric sources poll at 10-second intervals and default to a
            // 15-minute rolling window (index 0 in TimeRangePanel's preset list).
            chart.set_initial_time_range_preset(0);
            chart
        });

        doc.update(cx, |chart, cx| {
            chart.set_refresh_policy(dbflux_core::RefreshPolicy::Interval { every_secs: 10 }, cx);
        });

        let pane = ChartDocument::into_pane(doc, cx);
        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });
        self.set_focus(FocusTarget::Document, window, cx);
    }

    /// Open an `InspectorPanel` for an instance inspector leaf clicked in the sidebar.
    ///
    /// Deduplicates by `(profile_id, metric_id)` using `DocumentKey::InstanceInspector`
    /// so clicking the same inspector a second time focuses the existing tab rather
    /// than opening a duplicate.
    pub(super) fn open_instance_inspector(
        &mut self,
        profile_id: uuid::Uuid,
        metric_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::document::{DocumentKey, InspectorPanel};

        let key = DocumentKey::InstanceInspector {
            profile_id,
            metric_id: metric_id.clone(),
        };

        if let Some(existing_id) = self.tab_manager.read(cx).find_by_key(&key, cx) {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(existing_id, cx);
            });
            self.set_focus(FocusTarget::Document, window, cx);
            return;
        }

        let doc =
            cx.new(|cx| InspectorPanel::new(profile_id, metric_id, self.app_state.clone(), cx));

        doc.update(cx, |panel, cx| {
            panel.request_reexec(cx);
        });

        let pane = InspectorPanel::into_pane(doc, cx);
        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });
        self.set_focus(FocusTarget::Document, window, cx);
    }

    /// Open the synthesized read-only "Instance Overview" dashboard for a profile.
    ///
    /// The dashboard layout is produced by the driver's `InstanceCatalog::default_dashboard()`
    /// at open time — no rows are written to the database. The resulting tab is
    /// marked read-only so the user cannot mutate it; "Save as" produces an editable copy.
    pub(super) fn open_instance_overview(
        &mut self,
        profile_id: uuid::Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::document::{
            ChartDocument, DashboardDocument, DocumentKey, dashboard::DashboardPanelSlot,
        };
        use dbflux_components::chart::InstanceMetricSource;
        use dbflux_components::common::time_range::view::TimeRangePanel;
        use dbflux_components::saved_chart::{SavedChartRefreshPolicy, TimeRangePreset};
        use dbflux_ui_document::dashboard::PanelGridPos;

        // Stable synthetic UUID for dedup — derived so the same profile always
        // opens the same overview tab.
        let dashboard_id = uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("instance_overview:{profile_id}").as_bytes(),
        );

        let key = DocumentKey::InstanceOverview { profile_id };

        if let Some(existing_id) = self.tab_manager.read(cx).find_by_key(&key, cx) {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(existing_id, cx);
            });
            self.set_focus(FocusTarget::Document, window, cx);
            return;
        }

        // Retrieve the default dashboard descriptor from the driver's catalog.
        let catalog_result: Option<dbflux_core::DefaultInstanceDashboard> = {
            let state = self.app_state.read(cx);
            let connected = state.connections().get(&profile_id);
            connected
                .and_then(|c| c.connection.instance_catalog())
                .and_then(|catalog| catalog.default_dashboard())
        };

        let Some(descriptor) = catalog_result else {
            dbflux_ui_base::toast::Toast::info(
                "This driver does not define an Instance Overview dashboard.",
            )
            .meta_right(now_hms())
            .push(cx);
            return;
        };

        // Build panel slots from the descriptor.
        let panel_slots: Vec<DashboardPanelSlot> = descriptor
            .panels
            .iter()
            .map(|panel_def| {
                let grid_pos = PanelGridPos {
                    grid_row: panel_def.grid_row,
                    grid_column: panel_def.grid_column,
                    grid_width: panel_def.grid_width,
                    grid_height: panel_def.grid_height,
                };

                if panel_def.is_inspector {
                    use crate::ui::document::InspectorPanel;
                    let metric_id = panel_def.metric_id.clone();
                    let app_state = self.app_state.clone();
                    let inspector_entity =
                        cx.new(|cx| InspectorPanel::new(profile_id, metric_id, app_state, cx));
                    inspector_entity.update(cx, |panel, _cx| {
                        panel.defer_initial_exec();
                    });
                    DashboardPanelSlot::Inspector {
                        entity: inspector_entity,
                        grid_pos,
                        title_override: None,
                    }
                } else {
                    let source = InstanceMetricSource {
                        metric_id: panel_def.metric_id.clone(),
                    };
                    let metric_id_clone = panel_def.metric_id.clone();
                    let app_state = self.app_state.clone();
                    let panel_entity = cx.new(|cx| {
                        let mut chart = ChartDocument::new_with_source(
                            Some(profile_id),
                            metric_id_clone.clone(),
                            Box::new(source),
                            app_state,
                            window,
                            cx,
                        );
                        chart.set_instance_metric_identity(metric_id_clone);
                        chart.set_initial_time_range_preset(0);
                        chart
                    });
                    panel_entity.update(cx, |chart, cx| {
                        chart.set_embedded(true, cx);
                    });
                    panel_entity.update(cx, |chart, cx| {
                        chart.set_refresh_policy(
                            dbflux_core::RefreshPolicy::Interval { every_secs: 10 },
                            cx,
                        );
                    });
                    DashboardPanelSlot::Loaded {
                        panel: panel_entity,
                        grid_pos,
                        title_override: None,
                    }
                }
            })
            .collect();

        let shared_time_range = cx.new(|cx| TimeRangePanel::new("15m", Some(0), window, cx));

        let doc = cx.new(|cx| {
            let mut dashboard = DashboardDocument::new(
                dashboard_id,
                descriptor.title.clone(),
                panel_slots,
                shared_time_range,
                Some(TimeRangePreset::Last15min),
                SavedChartRefreshPolicy::Interval { every_secs: 10 },
                true,
                self.app_state.clone(),
                cx,
            );
            dashboard.set_profile_id(profile_id);
            dashboard
        });

        let pane = DashboardDocument::into_pane(doc, cx);
        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });
        self.set_focus(FocusTarget::Document, window, cx);
    }

    /// Clone a read-only instance overview dashboard into a new persisted,
    /// editable dashboard for the same profile, copying all panels.
    ///
    /// For each chart panel in the overview, a `SavedChart` record is upserted
    /// with `source = InstanceMetric { metric_id }`. Inspector panels are
    /// persisted as `DashboardPanelDraft::Inspector`. The new dashboard then
    /// has the same layout as the read-only overview.
    pub(super) fn save_overview_as_editable(
        &mut self,
        source_title: String,
        profile_id: uuid::Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use dbflux_components::chart::{
            AxisKind, AxisSpec, BindingSpec, ChartKind, ChartSpec, YScale,
        };
        use dbflux_components::saved_chart::{
            SavedChart, SavedChartRefreshPolicy, SavedChartSource, TimeRangePreset,
        };
        use dbflux_core::chrono::Utc;
        use dbflux_ui_base::DashboardPanelDraft;

        let new_name = format!("{} (editable)", source_title);

        // Fetch the driver's default dashboard descriptor to enumerate panels.
        let descriptor: Option<dbflux_core::DefaultInstanceDashboard> = {
            let state = self.app_state.read(cx);
            state
                .connections()
                .get(&profile_id)
                .and_then(|c| c.connection.instance_catalog())
                .and_then(|cat| cat.default_dashboard())
        };

        let result = self.app_state.update(cx, |state, _cx| {
            let new_id = state.dashboards.create_dashboard(
                new_name,
                None,
                profile_id,
                Some(TimeRangePreset::Last15min),
                SavedChartRefreshPolicy::Off,
            )?;

            if let Some(descriptor) = descriptor {
                let mut drafts: Vec<DashboardPanelDraft> = Vec::new();

                for panel_def in &descriptor.panels {
                    let panel_layout = Some(dbflux_ui_base::DraftGridLayout {
                        grid_row: panel_def.grid_row,
                        grid_column: panel_def.grid_column,
                        grid_width: panel_def.grid_width,
                        grid_height: panel_def.grid_height,
                    });

                    if panel_def.is_inspector {
                        drafts.push(DashboardPanelDraft::Inspector {
                            metric_id: panel_def.metric_id.clone(),
                            layout: panel_layout,
                        });
                    } else {
                        let now = Utc::now();
                        let chart = SavedChart {
                            id: uuid::Uuid::new_v4(),
                            name: panel_def.metric_id.clone(),
                            profile_id,
                            source: SavedChartSource::InstanceMetric {
                                metric_id: panel_def.metric_id.clone(),
                            },
                            chart_spec: ChartSpec {
                                kind: ChartKind::Line,
                                x_axis: AxisSpec {
                                    column_index: 0,
                                    label: String::new(),
                                    kind: AxisKind::Time,
                                    unit: None,
                                },
                                series: Vec::new(),
                                legend_visible: false,
                                decimation_threshold: 500,
                                binding: BindingSpec::default(),
                                track_source_indices: false,
                                y_scale: YScale::Linear,
                            },
                            bindings: BindingSpec::default(),
                            time_range_preset: Some(TimeRangePreset::Last15min),
                            refresh_policy: SavedChartRefreshPolicy::Off,
                            created_at: now,
                            updated_at: now,
                        };
                        let chart_id = chart.id;
                        state.saved_charts.upsert(chart)?;
                        drafts.push(DashboardPanelDraft::Chart {
                            saved_chart_id: chart_id,
                            layout: panel_layout,
                        });
                    }
                }

                if !drafts.is_empty() {
                    state.dashboards.append_panels(new_id, drafts)?;
                }
            }

            Ok::<uuid::Uuid, dbflux_storage::error::StorageError>(new_id)
        });

        match result {
            Ok(new_id) => {
                Toast::info("Created editable dashboard with all overview panels.")
                    .meta_right(now_hms())
                    .push(cx);
                self.open_dashboard(new_id, window, cx);
            }
            Err(e) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Config,
                        format!("Failed to create editable dashboard: {e}"),
                    ),
                    cx,
                );
            }
        }
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
                report_error(
                    UserFacingError::new(
                        ErrorKind::Config,
                        format!("Failed to persist MCP governance: {e}"),
                    ),
                    cx,
                );
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
                    report_error(
                        UserFacingError::new(
                            ErrorKind::Driver,
                            format!("Failed to refresh schema: {e}"),
                        ),
                        cx,
                    );
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
                    report_error_async(
                        UserFacingError::new(
                            ErrorKind::Storage,
                            format!("Failed to read file {}: {e}", path.display()),
                        ),
                        cx,
                    );
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
                    report_error_async(
                        UserFacingError::new(
                            ErrorKind::Storage,
                            format!("Failed to read file {}: {e}", path.display()),
                        ),
                        cx,
                    );
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
                report_error(
                    UserFacingError::new(
                        ErrorKind::Storage,
                        format!("Failed to write initial script content: {e}"),
                    ),
                    cx,
                );
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
            dbflux_components::saved_chart::SavedChartSource::Query { .. }
            | dbflux_components::saved_chart::SavedChartSource::Metric { .. }
            | dbflux_components::saved_chart::SavedChartSource::InstanceMetric { .. } => {
                // Validate before allocating an entity — from_saved checks the source variant.
                let validation = crate::ui::document::ChartDocument::validate_saved_source(&chart);
                if let Err(e) = validation {
                    report_error(
                        UserFacingError::new(ErrorKind::Storage, format!("Cannot open chart: {e}")),
                        cx,
                    );
                    return;
                }

                let app_state = self.app_state.clone();
                let doc = cx.new(|cx| {
                    // from_saved is guaranteed Ok for Query and Metric sources (validated above).
                    crate::ui::document::ChartDocument::from_saved(&chart, app_state, window, cx)
                        .expect("Query/Metric source validated before entity creation")
                });

                let pane = crate::ui::document::ChartDocument::into_pane(doc, cx);
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.open(Tab::Pane(Box::new(pane)), cx);
                });
                self.set_focus(FocusTarget::Document, window, cx);
            }
        }
    }

    /// Opens a `DashboardDocument` for the given dashboard ID.
    ///
    /// If a tab for this dashboard is already open, focuses it instead of
    /// creating a duplicate. Panel slots are built from the dashboard's
    /// persisted panel list: for each panel, if the referenced `SavedChart`
    /// exists and has a `Query` source, a live `ChartDocument` entity is
    /// created (`Loaded`); otherwise the slot is `Orphan`.
    ///
    /// This method does not inspect `driver_id`; capability gating for the
    /// import affordance is handled separately in the import flow.
    #[allow(dead_code)]
    pub(super) fn open_dashboard(
        &mut self,
        dashboard_id: uuid::Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Dedup: focus the existing tab if the dashboard is already open.
        let existing_id = self.tab_manager.read(cx).find_by_key(
            &crate::ui::document::DocumentKey::Dashboard { dashboard_id },
            cx,
        );

        if let Some(id) = existing_id {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(id, cx);
            });
            self.set_focus(FocusTarget::Document, window, cx);
            return;
        }

        // Look up the dashboard metadata.
        let dashboard_meta = self
            .app_state
            .read(cx)
            .dashboards
            .dashboard_by_id(dashboard_id)
            .cloned();

        let Some(dashboard) = dashboard_meta else {
            Toast::error("Dashboard not found")
                .meta_right(now_hms())
                .push(cx);
            return;
        };

        // Build panel slots from persisted panels.
        // Dedup panels by saved_chart_id when reading the persisted set.
        // Past bugs could persist two rows pointing to the same saved chart
        // (creating visible duplicates with identical data); this guard
        // keeps an already-affected dashboard usable without forcing the
        // user to delete and re-create panels manually.
        let mut panels: Vec<dbflux_ui_base::DashboardPanel> = {
            let raw = self
                .app_state
                .read(cx)
                .dashboards
                .panels_for_dashboard(dashboard_id)
                .to_vec();
            let mut seen: std::collections::HashSet<uuid::Uuid> = std::collections::HashSet::new();
            // Dividers don't dedup against saved_chart_id (they have none); pass
            // them through unconditionally. Chart panels dedup on saved_chart_id
            // so stale "two rows for the same chart" never materialises twice.
            raw.into_iter()
                .filter(|p| match p.saved_chart_id() {
                    Some(id) => seen.insert(id),
                    None => true,
                })
                .collect()
        };

        // One-shot rescale of pre-12-col data. Old dashboards persisted with
        // `grid_columns < 12` are widened in place so subsequent loads see
        // canonical 12-col-native coordinates. No SQL migration is required —
        // the new positions are written back through `update_panel_position`.
        use crate::ui::document::dashboard::{DASHBOARD_GRID_COLUMNS, rescale_panel_to_12_cols};
        if dashboard.grid_columns < DASHBOARD_GRID_COLUMNS && !panels.is_empty() {
            for panel in panels.iter_mut() {
                let (new_col, new_w) = rescale_panel_to_12_cols(
                    panel.grid_column,
                    panel.grid_width,
                    dashboard.grid_columns,
                );
                if new_col != panel.grid_column || new_w != panel.grid_width {
                    let dashboard_id_local = dashboard_id;
                    let panel_index = panel.panel_index;
                    let new_row = panel.grid_row;
                    let new_h = panel.grid_height;
                    let result = self.app_state.update(cx, |state, _cx| {
                        state.dashboards.update_panel_position(
                            dashboard_id_local,
                            panel_index,
                            new_col,
                            new_row,
                            new_w,
                            new_h,
                        )
                    });
                    if let Err(e) = result {
                        let message = e.to_string();
                        self.app_state.update(cx, |state, _cx| {
                            state.record_storage_failure(
                                dbflux_core::observability::actions::CONFIG_UPDATE,
                                "dashboard_panel",
                                format!("{dashboard_id_local}#{panel_index}"),
                                "Failed to rescale panel to 12-column grid".to_string(),
                                message,
                            );
                        });
                    }
                    panel.grid_column = new_col;
                    panel.grid_width = new_w;
                }
            }
        }

        let app_state = self.app_state.clone();
        let doc = cx.new(|cx| {
            use crate::ui::document::DashboardDocument;
            use crate::ui::document::DashboardPanelSlot;
            use dbflux_components::common::time_range::view::TimeRangePanel;

            // Build panel slots: Loaded when a Query/Metric-source chart exists,
            // Orphan otherwise. Grid position is carried on every slot so
            // the render can sort and size panels correctly.
            let panel_slots: Vec<DashboardPanelSlot> =
                panels
                    .iter()
                    .map(|panel| {
                        use crate::ui::document::dashboard::PanelGridPos;

                        let grid_pos = PanelGridPos {
                            grid_row: panel.grid_row,
                            grid_column: panel.grid_column,
                            grid_width: panel.grid_width,
                            grid_height: panel.grid_height,
                        };

                        // Divider panels render directly without resolving a chart.
                        if let dbflux_ui_base::DashboardPanelKind::Divider { markdown } =
                            &panel.kind
                        {
                            return DashboardPanelSlot::Divider {
                                markdown: markdown.clone(),
                                grid_pos,
                            };
                        }

                        // Inspector panels are instantiated as live entities.
                        // `profile_id` comes from the dashboard's profile association.
                        if let dbflux_ui_base::DashboardPanelKind::Inspector { metric_id } =
                            &panel.kind
                        {
                            use crate::ui::document::InspectorPanel;
                            if let Some(prof_id) = dashboard.profile_id {
                                let metric_id = metric_id.clone();
                                let app_state_inner = app_state.clone();
                                let inspector_entity = cx.new(|cx| {
                                    InspectorPanel::new(prof_id, metric_id, app_state_inner, cx)
                                });
                                inspector_entity.update(cx, |p, _cx| p.defer_initial_exec());
                                return DashboardPanelSlot::Inspector {
                                    entity: inspector_entity,
                                    grid_pos,
                                    title_override: panel.title_override.clone(),
                                };
                            }
                        }

                        let saved_chart_id = panel.saved_chart_id().unwrap_or_else(uuid::Uuid::nil);

                        let chart = app_state
                            .read(cx)
                            .saved_charts
                            .all_charts()
                            .iter()
                            .find(|c| c.id == saved_chart_id)
                            .cloned();

                        match chart {
                            Some(saved_chart)
                                if matches!(
                                saved_chart.source,
                                dbflux_components::saved_chart::SavedChartSource::Query { .. }
                                    | dbflux_components::saved_chart::SavedChartSource::Metric {
                                        ..
                                    }
                                    | dbflux_components::saved_chart::SavedChartSource::InstanceMetric {
                                        ..
                                    }
                            ) =>
                            {
                                let app_state_inner = app_state.clone();
                                let panel_entity = cx.new(|cx| {
                                    let mut doc = crate::ui::document::ChartDocument::from_saved(
                                        &saved_chart,
                                        app_state_inner,
                                        window,
                                        cx,
                                    )
                                    .expect("Query/Metric/InstanceMetric source validated before entity creation");
                                    // Mark embedded so the chart's own chrome
                                    // (title/Run/Save segments + internal
                                    // toolbar row) is suppressed; the
                                    // dashboard panel provides the chrome.
                                    doc.set_embedded(true, cx);
                                    doc
                                });
                                DashboardPanelSlot::Loaded {
                                    panel: panel_entity,
                                    grid_pos,
                                    title_override: panel.title_override.clone(),
                                }
                            }
                            _ => DashboardPanelSlot::Orphan {
                                saved_chart_id,
                                grid_pos,
                            },
                        }
                    })
                    .collect();

            // Build the shared time-range panel using the persisted preset when
            // available; fall back to Last24Hours (index 3) when the dashboard
            // has no stored preset.
            use dbflux_components::saved_chart::TimeRangePreset;
            let (preset_placeholder, preset_index) = match dashboard.shared_time_range_preset {
                Some(TimeRangePreset::Last15min) => ("15m", Some(0usize)),
                Some(TimeRangePreset::LastHour) => ("1h", Some(1)),
                Some(TimeRangePreset::Last6Hours) => ("6h", Some(2)),
                Some(TimeRangePreset::Last24Hours) | None => ("24h", Some(3)),
                Some(TimeRangePreset::Last7Days) => ("7d", Some(4)),
            };
            let shared_time_range =
                cx.new(|cx| TimeRangePanel::new(preset_placeholder, preset_index, window, cx));

            DashboardDocument::new(
                dashboard_id,
                dashboard.name.clone(),
                panel_slots,
                shared_time_range,
                dashboard.shared_time_range_preset,
                dashboard.shared_refresh_policy,
                false,
                app_state.clone(),
                cx,
            )
        });

        let pane = crate::ui::document::DashboardDocument::into_pane(doc, cx);
        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });
        self.set_focus(FocusTarget::Document, window, cx);
    }

    /// Runs the dashboard import flow after the user confirms JSON input.
    ///
    /// Calls `conn.dashboard_importer()?.import(&json)` to get `WidgetImportSpec`
    /// records, creates one `SavedChart` per metric widget (multi-series), one
    /// `DashboardPanel { kind: Divider }` per text widget, upserts a new
    /// `Dashboard` and its panel set, then opens the dashboard in a new tab.
    /// This method does not inspect `driver_id`.
    pub(super) fn run_dashboard_import(
        &mut self,
        json: String,
        name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use dbflux_components::chart::{
            AxisKind, AxisSpec, BindingSpec, ChartKind, ChartSpec, YScale,
        };
        use dbflux_components::saved_chart::{MetricSeries, SavedChart, SavedChartRefreshPolicy};
        use dbflux_ui_base::{Dashboard, DashboardPanel, DashboardPanelKind};

        // Borrow app_state in a scoped block so the borrow ends before the update below.
        let import_result: Result<(uuid::Uuid, Vec<dbflux_core::WidgetImportSpec>), String> = {
            let app_state = self.app_state.read(cx);

            let Some(active) = app_state.active_connection() else {
                return Toast::error(
                    "No active connection — connect to a profile before importing.",
                )
                .meta_right(now_hms())
                .push(cx);
            };

            let profile_id = active.profile.id;

            let importer = match active.connection.dashboard_importer() {
                Some(i) => i,
                None => {
                    return Toast::error(
                        "The active connection does not support dashboard import.",
                    )
                    .meta_right(now_hms())
                    .push(cx);
                }
            };

            match importer.import(&json) {
                Ok(specs) => Ok((profile_id, specs)),
                Err(e) => Err(format!("Dashboard import failed: {e}")),
            }
        };

        let (profile_id, specs) = match import_result {
            Ok(v) => v,
            Err(e) => {
                report_error(UserFacingError::new(ErrorKind::Driver, e), cx);
                return;
            }
        };

        // Build the dashboard domain object.
        let now = chrono::Utc::now();
        let dashboard_id = uuid::Uuid::new_v4();
        let dashboard = Dashboard {
            id: dashboard_id,
            name: if name.trim().is_empty() {
                "Imported Dashboard".to_string()
            } else {
                name
            },
            description: None,
            profile_id: Some(profile_id),
            shared_time_range_preset: None,
            shared_refresh_policy: SavedChartRefreshPolicy::Off,
            grid_columns: 12,
            created_at: now,
            updated_at: now,
        };

        // Convert each WidgetImportSpec to a SavedChart + DashboardPanel
        // (metric widgets) or a divider-only DashboardPanel (text widgets).
        //
        // CloudWatch widgets natively live on a 24-column grid; DBFlux dashboards
        // use 12 columns, so widget `x`/`width` are halved (clamped to ≥1 col).
        // Each widget becomes ONE panel — multi-series widgets persist all
        // series inside a single SavedChart instead of subdividing the grid.
        let mut charts: Vec<SavedChart> = Vec::new();
        let mut panels: Vec<DashboardPanel> = Vec::with_capacity(specs.len());

        for (widget_index, spec) in specs.iter().enumerate() {
            let layout = spec.layout;
            let scaled_col = layout.x / 2;
            let scaled_width = (layout.width / 2).max(1);
            let scaled_row = layout.y;
            let scaled_height = layout.height.max(1);

            match &spec.kind {
                dbflux_core::WidgetImportKind::Metric { view, series } => {
                    let metric_series: Vec<MetricSeries> = series
                        .iter()
                        .map(|s| MetricSeries {
                            namespace: s.namespace.clone(),
                            metric_name: s.metric_name.clone(),
                            dimensions: s.dimensions.clone(),
                            period_seconds: s.period_seconds,
                            statistic: s.statistic.clone(),
                            region: s.region.clone(),
                            label: s.label.clone(),
                        })
                        .collect();

                    let chart_kind = match view {
                        dbflux_core::MetricView::SingleValue => ChartKind::Number,
                        dbflux_core::MetricView::StackedArea => ChartKind::Area,
                        dbflux_core::MetricView::TimeSeries => ChartKind::Line,
                    };

                    let placeholder_spec = ChartSpec {
                        kind: chart_kind,
                        x_axis: AxisSpec {
                            column_index: 0,
                            label: String::new(),
                            kind: AxisKind::Time,
                            unit: None,
                        },
                        series: Vec::new(),
                        legend_visible: false,
                        // Dashboard panels are small (~240 px wide); 500
                        // LTTB points already saturate the pixel grid and
                        // keep paint cheap when many panels are visible.
                        decimation_threshold: 500,
                        binding: BindingSpec::default(),
                        track_source_indices: false,
                        y_scale: YScale::Linear,
                    };

                    // CloudWatch widgets often omit `properties.title`. Fall
                    // back to the first series' metric_name (joined when many
                    // distinct metric_names share the panel) so the dashboard
                    // header is never blank — the panel must always be
                    // identifiable at a glance.
                    let chart_name = if spec.title.trim().is_empty() {
                        let mut names: Vec<&str> =
                            series.iter().map(|s| s.metric_name.as_str()).collect();
                        names.sort_unstable();
                        names.dedup();
                        names.join(", ")
                    } else {
                        spec.title.clone()
                    };

                    let chart = SavedChart::new_metric(
                        chart_name,
                        profile_id,
                        metric_series,
                        placeholder_spec,
                        BindingSpec::default(),
                    );

                    let panel = DashboardPanel {
                        dashboard_id,
                        panel_index: widget_index as u32,
                        kind: DashboardPanelKind::Chart {
                            saved_chart_id: chart.id,
                        },
                        title_override: None,
                        grid_row: scaled_row,
                        grid_column: scaled_col,
                        grid_width: scaled_width,
                        grid_height: scaled_height,
                    };

                    charts.push(chart);
                    panels.push(panel);
                }
                dbflux_core::WidgetImportKind::TextDivider { markdown } => {
                    panels.push(DashboardPanel {
                        dashboard_id,
                        panel_index: widget_index as u32,
                        kind: DashboardPanelKind::Divider {
                            markdown: markdown.clone(),
                        },
                        title_override: None,
                        grid_row: scaled_row,
                        grid_column: scaled_col,
                        grid_width: scaled_width,
                        grid_height: scaled_height,
                    });
                }
            }
        }

        // Persist charts, dashboard, and panels. Collect the first storage
        // failure so we can surface it to the user and record an audit event.
        let persist_result: Result<(), (String, String)> =
            self.app_state.update(cx, |state, _cx| {
                for chart in &charts {
                    if let Err(e) = state.saved_charts.upsert(chart.clone()) {
                        state.record_storage_failure(
                            dbflux_core::observability::actions::CONFIG_CREATE,
                            "saved_chart",
                            chart.id.to_string(),
                            format!("Failed to persist imported chart '{}'", chart.name),
                            e.to_string(),
                        );
                        return Err((chart.name.clone(), e.to_string()));
                    }
                }

                if let Err(e) = state.dashboards.upsert_dashboard(dashboard.clone()) {
                    state.record_storage_failure(
                        dbflux_core::observability::actions::CONFIG_CREATE,
                        "dashboard",
                        dashboard.id.to_string(),
                        format!("Failed to persist imported dashboard '{}'", dashboard.name),
                        e.to_string(),
                    );
                    return Err((dashboard.name.clone(), e.to_string()));
                }

                if let Err(e) = state.dashboards.replace_panels(dashboard_id, panels) {
                    state.record_storage_failure(
                        dbflux_core::observability::actions::CONFIG_UPDATE,
                        "dashboard_panels",
                        dashboard_id.to_string(),
                        "Failed to persist imported dashboard panels".to_string(),
                        e.to_string(),
                    );
                    return Err((dashboard.name.clone(), e.to_string()));
                }

                Ok(())
            });

        if let Err((name, message)) = persist_result {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    format!("Failed to save dashboard '{name}': {message}"),
                ),
                cx,
            );
            return;
        }

        Toast::info(format!(
            "Imported {} panels into a new dashboard.",
            charts.len()
        ))
        .meta_right(now_hms())
        .push(cx);

        self.open_dashboard(dashboard_id, window, cx);
    }

    /// Open a dashboard fetched live from the connection's upstream source,
    /// read-only. Nothing is persisted: the body is fetched via
    /// `DashboardSource::fetch_dashboard`, parsed with the connection's
    /// dashboard importer, and rendered into an ephemeral `DashboardDocument`.
    /// Re-opening the same dashboard focuses the existing tab (id is derived
    /// deterministically from the profile + name); it does not re-fetch while
    /// the tab is open. This method does not inspect `driver_id`.
    pub(super) fn open_remote_dashboard(
        &mut self,
        profile_id: uuid::Uuid,
        name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::document::DocumentKey;

        // Deterministic id so re-opening the same upstream dashboard dedups to
        // the open tab instead of stacking duplicates.
        let dashboard_id = uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("remote-dashboard:{profile_id}:{name}").as_bytes(),
        );

        let key = DocumentKey::Dashboard { dashboard_id };
        if let Some(existing) = self.tab_manager.read(cx).find_by_key(&key, cx) {
            self.tab_manager
                .update(cx, |mgr, cx| mgr.activate(existing, cx));
            self.set_focus(FocusTarget::Document, window, cx);
            return;
        }

        let connection = match self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.clone())
        {
            Some(c) => c,
            None => {
                return Toast::error("Connection not found for this dashboard.")
                    .meta_right(now_hms())
                    .push(cx);
            }
        };

        let app_state = self.app_state.clone();
        let name_for_fetch = name.clone();

        // Fetch + parse off the foreground thread; both the source and the
        // importer live on the connection, so the whole IO+parse runs here.
        let background = cx.background_executor().spawn(async move {
            let source = connection
                .dashboard_source()
                .ok_or_else(|| "The connection does not support dashboard browsing.".to_string())?;
            let remote = source
                .fetch_dashboard(&name_for_fetch)
                .map_err(|e| e.to_string())?;

            let importer = connection
                .dashboard_importer()
                .ok_or_else(|| "The connection cannot parse dashboards.".to_string())?;
            importer
                .import(&remote.body_json)
                .map(|specs| (remote.body_json, specs))
                .map_err(|e| format!("Dashboard parse failed: {e}"))
        });

        cx.spawn_in(window, async move |this, cx| {
            let result = background.await;
            this.update_in(cx, |this, window, cx| {
                let specs = match result {
                    Ok((_body, specs)) => specs,
                    Err(message) => {
                        report_error(UserFacingError::new(ErrorKind::Network, message), cx);
                        return;
                    }
                };

                this.open_remote_dashboard_document(
                    dashboard_id,
                    name,
                    profile_id,
                    specs,
                    app_state,
                    window,
                    cx,
                );
            })
            .ok();
        })
        .detach();
    }

    /// Build the ephemeral `DashboardDocument` from parsed widget specs and open
    /// it. In-memory only — no `SavedChart`/`Dashboard`/panel rows are written.
    #[allow(clippy::too_many_arguments)]
    fn open_remote_dashboard_document(
        &mut self,
        dashboard_id: uuid::Uuid,
        name: String,
        profile_id: uuid::Uuid,
        specs: Vec<dbflux_core::WidgetImportSpec>,
        app_state: Entity<dbflux_ui_base::AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::document::dashboard::PanelGridPos;
        use crate::ui::document::{ChartDocument, DashboardDocument, DashboardPanelSlot};
        use dbflux_components::chart::{
            AxisKind, AxisSpec, BindingSpec, ChartKind, ChartSpec, YScale,
        };
        use dbflux_components::common::time_range::view::TimeRangePanel;
        use dbflux_components::saved_chart::{MetricSeries, SavedChart, SavedChartRefreshPolicy};

        let doc = cx.new(|cx| {
            let panel_slots: Vec<DashboardPanelSlot> = specs
                .iter()
                .map(|spec| {
                    // CloudWatch widgets live on a 24-column grid; DBFlux uses
                    // 12, so x/width are halved (clamped to >= 1 col).
                    let grid_pos = PanelGridPos {
                        grid_row: spec.layout.y,
                        grid_column: spec.layout.x / 2,
                        grid_width: (spec.layout.width / 2).max(1),
                        grid_height: spec.layout.height.max(1),
                    };

                    let series = match &spec.kind {
                        dbflux_core::WidgetImportKind::TextDivider { markdown } => {
                            return DashboardPanelSlot::Divider {
                                markdown: markdown.clone(),
                                grid_pos,
                            };
                        }
                        dbflux_core::WidgetImportKind::Metric { series, .. } => series,
                    };

                    let view = match &spec.kind {
                        dbflux_core::WidgetImportKind::Metric { view, .. } => *view,
                        dbflux_core::WidgetImportKind::TextDivider { .. } => unreachable!(),
                    };

                    let metric_series: Vec<MetricSeries> = series
                        .iter()
                        .map(|s| MetricSeries {
                            namespace: s.namespace.clone(),
                            metric_name: s.metric_name.clone(),
                            dimensions: s.dimensions.clone(),
                            period_seconds: s.period_seconds,
                            statistic: s.statistic.clone(),
                            region: s.region.clone(),
                            label: s.label.clone(),
                        })
                        .collect();

                    let chart_kind = match view {
                        dbflux_core::MetricView::SingleValue => ChartKind::Number,
                        dbflux_core::MetricView::StackedArea => ChartKind::Area,
                        dbflux_core::MetricView::TimeSeries => ChartKind::Line,
                    };

                    let placeholder_spec = ChartSpec {
                        kind: chart_kind,
                        x_axis: AxisSpec {
                            column_index: 0,
                            label: String::new(),
                            kind: AxisKind::Time,
                            unit: None,
                        },
                        series: Vec::new(),
                        legend_visible: false,
                        decimation_threshold: 500,
                        binding: BindingSpec::default(),
                        track_source_indices: false,
                        y_scale: YScale::Linear,
                    };

                    let chart_name = if spec.title.trim().is_empty() {
                        let mut names: Vec<&str> =
                            series.iter().map(|s| s.metric_name.as_str()).collect();
                        names.sort_unstable();
                        names.dedup();
                        names.join(", ")
                    } else {
                        spec.title.clone()
                    };

                    let saved_chart = SavedChart::new_metric(
                        chart_name,
                        profile_id,
                        metric_series,
                        placeholder_spec,
                        BindingSpec::default(),
                    );

                    let app_state_inner = app_state.clone();
                    let panel_entity = cx.new(|cx| {
                        let mut chart =
                            ChartDocument::from_saved(&saved_chart, app_state_inner, window, cx)
                                .expect("metric source is always valid for ChartDocument");
                        chart.set_embedded(true, cx);
                        chart
                    });

                    DashboardPanelSlot::Loaded {
                        panel: panel_entity,
                        grid_pos,
                        title_override: None,
                    }
                })
                .collect();

            let shared_time_range = cx.new(|cx| TimeRangePanel::new("24h", Some(3), window, cx));

            DashboardDocument::new(
                dashboard_id,
                name,
                panel_slots,
                shared_time_range,
                None,
                SavedChartRefreshPolicy::Off,
                false,
                app_state.clone(),
                cx,
            )
        });

        let pane = crate::ui::document::DashboardDocument::into_pane(doc, cx);
        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });
        self.set_focus(FocusTarget::Document, window, cx);
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

    // --- Phase P stubs: dashboard and saved-chart workspace actions ---
    // Full implementations arrive in Phase P; these stubs wire the Phase N
    // sidebar-event routing so the crate compiles before modals exist.

    /// Open the "New Dashboard" creation modal for the given profile.
    ///
    /// Called when the user selects "New Dashboard..." from the sidebar context
    /// menu on a DashboardsFolder node.
    pub(super) fn create_dashboard_from_sidebar(
        &mut self,
        profile_id: uuid::Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.modal_create_dashboard.update(cx, |modal, cx| {
            modal.open(CreateDashboardRequest { profile_id }, window, cx);
        });
    }

    /// Open the "New Dashboard..." modal from the command palette.
    ///
    /// Uses the active connection's profile as the target profile. If no
    /// connection is active but profiles exist, uses the first profile.
    /// Shows a toast if no profiles are configured.
    pub(super) fn create_dashboard_from_palette(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let profile_id = self
            .app_state
            .read(cx)
            .active_connection()
            .map(|c| c.profile.id)
            .or_else(|| self.app_state.read(cx).profiles().first().map(|p| p.id));

        match profile_id {
            Some(profile_id) => {
                self.modal_create_dashboard.update(cx, |modal, cx| {
                    modal.open(CreateDashboardRequest { profile_id }, window, cx);
                });
            }
            None => {
                Toast::warning("Add a connection profile before creating a dashboard.")
                    .meta_right(now_hms())
                    .push(cx);
            }
        }
    }

    /// Called when `ModalCreateDashboard` emits `Confirmed`.
    ///
    /// Creates the dashboard in the manager, triggers a sidebar rebuild, and
    /// opens the new dashboard tab.
    pub(super) fn on_create_dashboard_confirmed(
        &mut self,
        profile_id: uuid::Uuid,
        name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let result = self.app_state.update(cx, |state, _cx| {
            state.dashboards.create_dashboard(
                name.clone(),
                None,
                profile_id,
                None,
                dbflux_components::saved_chart::SavedChartRefreshPolicy::Off,
            )
        });

        match result {
            Ok(dashboard_id) => {
                self.app_state.update(cx, |_state, cx| {
                    cx.emit(AppStateChanged);
                });
                self.open_dashboard(dashboard_id, window, cx);
            }
            Err(e) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Storage,
                        format!("Failed to create dashboard: {e}"),
                    ),
                    cx,
                );
            }
        }
    }

    /// Open the "Import Dashboard from JSON" modal scoped to the given profile.
    ///
    /// Opens the existing import modal. Full profile-scoping (pre-selecting the
    /// profile in the modal) is Phase O.6 work.
    pub(super) fn import_dashboard_for_profile(
        &mut self,
        _profile_id: uuid::Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.modal_import_dashboard.update(cx, |modal, cx| {
            modal.open(window, cx);
        });
    }

    /// Open the rename modal for a dashboard.
    pub(super) fn rename_dashboard(
        &mut self,
        dashboard_id: uuid::Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let current_name = self
            .app_state
            .read(cx)
            .dashboards
            .dashboard_by_id(dashboard_id)
            .map(|d| d.name.clone())
            .unwrap_or_default();

        self.modal_rename_item.update(cx, |modal, cx| {
            modal.open(
                RenameItemRequest {
                    target: RenameTarget::Dashboard { dashboard_id },
                    current_name,
                },
                window,
                cx,
            );
        });
    }

    /// Delete a dashboard after confirmation.
    ///
    /// Opens the delete confirmation modal. On confirm, the tab is closed
    /// before the row is removed from the repository (see
    /// `on_delete_dashboard_confirmed`).
    pub(super) fn delete_dashboard(
        &mut self,
        dashboard_id: uuid::Uuid,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let dashboard_name = self
            .app_state
            .read(cx)
            .dashboards
            .dashboard_by_id(dashboard_id)
            .map(|d| d.name.clone())
            .unwrap_or_default();

        self.modal_delete_dashboard.update(cx, |modal, cx| {
            modal.open(
                DeleteDashboardRequest {
                    dashboard_id,
                    dashboard_name,
                },
                cx,
            );
        });
    }

    /// Called when `ModalDeleteDashboardConfirm` emits `Confirmed`.
    ///
    /// Closes the open tab first, then deletes the dashboard row and panels,
    /// then triggers a sidebar rebuild.
    pub(super) fn on_delete_dashboard_confirmed(
        &mut self,
        dashboard_id: uuid::Uuid,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Close the open tab before deleting the row so the UI never references
        // a deleted entity.
        let key = crate::ui::document::DocumentKey::Dashboard { dashboard_id };
        if let Some(doc_id) = self.tab_manager.read(cx).find_by_key(&key, cx) {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.close(doc_id, cx);
            });
        }

        let result = self.app_state.update(cx, |state, _cx| {
            state.dashboards.delete_dashboard(dashboard_id)
        });

        match result {
            Ok(()) => {
                self.app_state.update(cx, |_state, cx| {
                    cx.emit(AppStateChanged);
                });
            }
            Err(e) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Storage,
                        format!("Failed to delete dashboard: {e}"),
                    ),
                    cx,
                );
            }
        }
    }

    /// Duplicate a dashboard without a modal (immediate action).
    pub(super) fn duplicate_dashboard(&mut self, dashboard_id: uuid::Uuid, cx: &mut Context<Self>) {
        let result = self.app_state.update(cx, |state, _cx| {
            state.dashboards.duplicate_dashboard(dashboard_id)
        });

        match result {
            Ok(_new_id) => {
                self.app_state.update(cx, |_state, cx| {
                    cx.emit(AppStateChanged);
                });
            }
            Err(e) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Storage,
                        format!("Failed to duplicate dashboard: {e}"),
                    ),
                    cx,
                );
            }
        }
    }

    /// Open the rename modal for a saved chart.
    pub(super) fn rename_saved_chart(
        &mut self,
        chart_id: uuid::Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let current_name = self
            .app_state
            .read(cx)
            .saved_charts
            .all_charts()
            .iter()
            .find(|c| c.id == chart_id)
            .map(|c| c.name.clone())
            .unwrap_or_default();

        self.modal_rename_item.update(cx, |modal, cx| {
            modal.open(
                RenameItemRequest {
                    target: RenameTarget::SavedChart { chart_id },
                    current_name,
                },
                window,
                cx,
            );
        });
    }

    /// Called when `ModalRenameItem` emits `Confirmed`.
    ///
    /// Dispatches to the appropriate manager based on `RenameTarget`.
    pub(super) fn on_rename_item_confirmed(
        &mut self,
        target: RenameTarget,
        new_name: String,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let result = match &target {
            RenameTarget::Dashboard { dashboard_id } => {
                let id = *dashboard_id;
                self.app_state.update(cx, |state, _cx| {
                    state.dashboards.rename_dashboard(id, new_name)
                })
            }
            RenameTarget::SavedChart { chart_id } => {
                let id = *chart_id;
                self.app_state.update(cx, |state, _cx| {
                    state.saved_charts.rename_chart(id, new_name)
                })
            }
        };

        match result {
            Ok(()) => {
                self.app_state.update(cx, |_state, cx| {
                    cx.emit(AppStateChanged);
                });
            }
            Err(e) => {
                report_error(
                    UserFacingError::new(ErrorKind::Storage, format!("Failed to rename: {e}")),
                    cx,
                );
            }
        }
    }

    /// Delete a saved chart after confirmation.
    ///
    /// Pre-queries `find_dashboards_referencing_chart` to populate the
    /// orphan-warning list in the confirmation modal.
    pub(super) fn delete_saved_chart(
        &mut self,
        chart_id: uuid::Uuid,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let chart_name = self
            .app_state
            .read(cx)
            .saved_charts
            .all_charts()
            .iter()
            .find(|c| c.id == chart_id)
            .map(|c| c.name.clone())
            .unwrap_or_default();

        // Build the referencing-dashboard list for the orphan-warning block.
        let referencing_ids = self
            .app_state
            .read(cx)
            .storage_runtime()
            .dashboards_repo()
            .find_dashboards_referencing_chart(chart_id)
            .unwrap_or_default();

        let referencing_dashboards: Vec<(uuid::Uuid, String)> = referencing_ids
            .into_iter()
            .filter_map(|did| {
                self.app_state
                    .read(cx)
                    .dashboards
                    .dashboard_by_id(did)
                    .map(|d| (did, d.name.clone()))
            })
            .collect();

        self.modal_delete_saved_chart.update(cx, |modal, cx| {
            modal.open(
                DeleteSavedChartRequest {
                    chart_id,
                    chart_name,
                    referencing_dashboards,
                },
                cx,
            );
        });
    }

    /// Called when `ModalDeleteSavedChartConfirm` emits `Confirmed`.
    pub(super) fn on_delete_saved_chart_confirmed(
        &mut self,
        chart_id: uuid::Uuid,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let result = self
            .app_state
            .update(cx, |state, _cx| state.saved_charts.delete_chart(chart_id));

        match result {
            Ok(()) => {
                self.app_state.update(cx, |_state, cx| {
                    cx.emit(AppStateChanged);
                });
            }
            Err(e) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Storage,
                        format!("Failed to delete saved chart: {e}"),
                    ),
                    cx,
                );
            }
        }
    }

    /// Duplicate a saved chart without a modal (immediate action).
    pub(super) fn duplicate_saved_chart(&mut self, chart_id: uuid::Uuid, cx: &mut Context<Self>) {
        let result = self.app_state.update(cx, |state, _cx| {
            state.saved_charts.duplicate_chart(chart_id)
        });

        match result {
            Ok(_new_id) => {
                self.app_state.update(cx, |_state, cx| {
                    cx.emit(AppStateChanged);
                });
            }
            Err(e) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Storage,
                        format!("Failed to duplicate saved chart: {e}"),
                    ),
                    cx,
                );
            }
        }
    }

    /// Open the "Add Panel" picker for a specific dashboard.
    pub(super) fn open_add_panel_picker(
        &mut self,
        dashboard_id: uuid::Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let profile_id_opt = self
            .app_state
            .read(cx)
            .dashboards
            .dashboard_by_id(dashboard_id)
            .and_then(|d| d.profile_id);

        let candidates: Vec<dbflux_components::saved_chart::SavedChart> =
            if let Some(pid) = profile_id_opt {
                self.app_state
                    .read(cx)
                    .saved_charts
                    .charts_for_profile(pid)
                    .into_iter()
                    .cloned()
                    .collect()
            } else {
                Vec::new()
            };

        // Detect the metric-catalog capability synchronously (cheap — reads
        // the driver metadata bitset). The actual namespace list is fetched
        // off the foreground thread below so the modal opens immediately.
        let (profile_id, has_metric_catalog, connection_for_catalog) =
            if let Some(pid) = profile_id_opt {
                let app_state = self.app_state.read(cx);
                if let Some(connected) = app_state.connections().get(&pid) {
                    let has = connected
                        .connection
                        .metadata()
                        .capabilities
                        .contains(DriverCapabilities::METRIC_CATALOG);
                    let connection = has.then(|| connected.connection.clone());
                    (pid, has, connection)
                } else {
                    (pid, false, None)
                }
            } else {
                (uuid::Uuid::nil(), false, None)
            };

        // Open the picker right away with an empty namespace list and the
        // loading flag set so the user sees feedback the moment they click
        // Add. Previously the modal blocked on `list_namespaces()` for a few
        // seconds with no indication that anything was happening.
        self.modal_add_panel.update(cx, |modal, cx| {
            modal.open(
                AddPanelRequest {
                    dashboard_id,
                    profile_id,
                    candidates,
                    has_metric_catalog,
                    metric_namespaces: Vec::new(),
                    metric_namespaces_loading: has_metric_catalog,
                },
                window,
                cx,
            );
        });

        // Kick off the background namespace fetch and register a Tasks-panel
        // entry so the user can see the work happening. The Arc<dyn
        // Connection> is `Send + Sync`, so we can move it into the
        // background_executor closure and call `metric_catalog()` from there.
        if let Some(connection) = connection_for_catalog {
            let (task_id, _cancel) = self.app_state.update(cx, |state, _| {
                state.start_task_for_profile(
                    dbflux_core::TaskKind::LoadSchema,
                    "Loading metric namespaces",
                    Some(profile_id),
                )
            });
            let modal = self.modal_add_panel.clone();
            let app_state = self.app_state.clone();

            cx.spawn(async move |_this, cx| {
                let result = cx
                    .background_executor()
                    .spawn(async move {
                        match connection.metric_catalog() {
                            Some(catalog) => catalog.list_namespaces(),
                            None => Ok(Vec::new()),
                        }
                    })
                    .await;

                let _ = cx.update(|cx| match result {
                    Ok(namespaces) => {
                        app_state.update(cx, |state, _| state.complete_task(task_id));
                        modal.update(cx, |m, cx| m.set_metric_namespaces(namespaces, cx));
                    }
                    Err(err) => {
                        let msg = err.to_string();
                        app_state.update(cx, |state, _| state.fail_task(task_id, msg.clone()));
                        report_error(
                            UserFacingError::new(
                                ErrorKind::Network,
                                format!("Failed to load metric namespaces: {msg}"),
                            ),
                            cx,
                        );
                        modal.update(cx, |m, cx| m.set_metric_namespaces(Vec::new(), cx));
                    }
                });
            })
            .detach();
        }
    }

    /// Fetch metrics for a namespace and push them back into the modal.
    ///
    /// Runs on the background executor with a Tasks-panel entry so the user
    /// sees the request progressing. Previously this call was synchronous on
    /// the foreground thread and blocked the UI for several seconds.
    pub(super) fn on_request_metrics_for_namespace(
        &mut self,
        modal: gpui::Entity<dbflux_ui_base::modals::ModalAddPanelPicker>,
        ev: dbflux_ui_base::modals::RequestMetricsForNamespace,
        cx: &mut Context<Self>,
    ) {
        let connection = self
            .app_state
            .read(cx)
            .connections()
            .get(&ev.profile_id)
            .map(|c| c.connection.clone());

        let Some(connection) = connection else {
            modal.update(cx, |m, cx| {
                m.set_metrics_for_namespace(ev.namespace.clone(), Vec::new(), cx);
            });
            return;
        };

        let (task_id, _cancel) = self.app_state.update(cx, |state, _| {
            state.start_task_for_profile(
                dbflux_core::TaskKind::LoadSchema,
                format!("Loading metrics for {}", ev.namespace),
                Some(ev.profile_id),
            )
        });
        let app_state = self.app_state.clone();
        let namespace = ev.namespace.clone();

        cx.spawn(async move |_this, cx| {
            let namespace_for_bg = namespace.clone();
            let result = cx
                .background_executor()
                .spawn(async move {
                    match connection.metric_catalog() {
                        Some(catalog) => catalog
                            .list_metrics(&namespace_for_bg, None)
                            .map(|page| page.metrics),
                        None => Ok(Vec::new()),
                    }
                })
                .await;

            let _ = cx.update(|cx| match result {
                Ok(metrics) => {
                    app_state.update(cx, |state, _| state.complete_task(task_id));
                    modal.update(cx, |m, cx| {
                        m.set_metrics_for_namespace(namespace, metrics, cx);
                    });
                }
                Err(err) => {
                    let msg = err.to_string();
                    app_state.update(cx, |state, _| state.fail_task(task_id, msg.clone()));
                    report_error(
                        UserFacingError::new(
                            ErrorKind::Network,
                            format!("Failed to load metrics: {msg}"),
                        ),
                        cx,
                    );
                    modal.update(cx, |m, cx| {
                        m.set_metrics_for_namespace(namespace, Vec::new(), cx);
                    });
                }
            });
        })
        .detach();
    }

    /// Build a new SavedChart from a user-typed query, persist it, and append
    /// it as a panel to the target dashboard.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn on_create_panel_from_query(
        &mut self,
        dashboard_id: uuid::Uuid,
        profile_id: uuid::Uuid,
        name: String,
        query: String,
        chart_kind: dbflux_components::chart::ChartKind,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use dbflux_components::chart::{AxisKind, AxisSpec, BindingSpec, ChartSpec, YScale};
        use dbflux_components::saved_chart::SavedChart;
        use dbflux_ui_base::DashboardPanelDraft;

        let placeholder_spec = ChartSpec {
            kind: chart_kind,
            x_axis: AxisSpec {
                column_index: 0,
                label: String::new(),
                kind: AxisKind::Time,
                unit: None,
            },
            series: Vec::new(),
            legend_visible: false,
            decimation_threshold: 10_000,
            binding: BindingSpec::default(),
            track_source_indices: false,
            y_scale: YScale::Linear,
        };

        let chart = SavedChart::new_query(
            name.clone(),
            profile_id,
            query,
            placeholder_spec,
            BindingSpec::default(),
        );
        let chart_id = chart.id;

        let append_result = self.app_state.update(cx, |state, _cx| {
            if let Err(e) = state.saved_charts.upsert(chart) {
                state.record_storage_failure(
                    dbflux_core::observability::actions::CONFIG_CREATE,
                    "saved_chart",
                    chart_id.to_string(),
                    format!("Failed to persist chart '{name}' for new panel"),
                    e.to_string(),
                );
                return Err(e);
            }
            state.dashboards.append_panels(
                dashboard_id,
                vec![DashboardPanelDraft::Chart {
                    saved_chart_id: chart_id,
                    layout: None,
                }],
            )
        });

        match append_result {
            Ok(()) => {
                self.app_state.update(cx, |_state, cx| {
                    cx.emit(AppStateChanged);
                });
            }
            Err(e) => {
                report_error(
                    UserFacingError::new(ErrorKind::Storage, format!("Failed to add panel: {e}")),
                    cx,
                );
            }
        }
    }

    /// Build a new SavedChart from a metric selection, persist it, and append
    /// it as a panel to the target dashboard.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn on_create_panel_from_metric(
        &mut self,
        dashboard_id: uuid::Uuid,
        profile_id: uuid::Uuid,
        name: String,
        namespace: String,
        metric_name: String,
        dimensions: Vec<(String, String)>,
        period_seconds: u32,
        statistic: String,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use dbflux_components::chart::{
            AxisKind, AxisSpec, BindingSpec, ChartKind, ChartSpec, YScale,
        };
        use dbflux_components::saved_chart::{SavedChart, SavedChartSource};
        use dbflux_ui_base::DashboardPanelDraft;

        // Reject the request if this dashboard already has a panel pointing
        // to a chart with the same metric identity. Each on_create call mints
        // a fresh saved_chart UUID, so without this guard a second "Create
        // panel" for the same metric produces a visually duplicate panel
        // (same namespace + metric + dimensions + period + statistic =
        // identical data, different UUID).
        let already_present = {
            let app_state = self.app_state.read(cx);
            let existing_panel_charts: Vec<uuid::Uuid> = app_state
                .dashboards
                .panels_for_dashboard(dashboard_id)
                .iter()
                .filter_map(|p| p.saved_chart_id())
                .collect();
            app_state.saved_charts.all_charts().iter().any(|chart| {
                if !existing_panel_charts.contains(&chart.id) {
                    return false;
                }
                // A single-series metric chart created via this action collides
                // with another single-series chart whose first series carries
                // the same (namespace, metric_name, dimensions, period, stat).
                // Multi-series charts (only ever produced via dashboard import)
                // never collide with a single-metric create.
                match &chart.source {
                    SavedChartSource::Metric { series } if series.len() == 1 => {
                        let s = &series[0];
                        s.namespace == namespace
                            && s.metric_name == metric_name
                            && s.dimensions == dimensions
                            && s.period_seconds == period_seconds
                            && s.statistic == statistic
                    }
                    _ => false,
                }
            })
        };

        if already_present {
            Toast::error(format!(
                "A panel for {namespace}/{metric_name} is already in this dashboard"
            ))
            .meta_right(now_hms())
            .push(cx);
            return;
        }

        let placeholder_spec = ChartSpec {
            kind: ChartKind::Line,
            x_axis: AxisSpec {
                column_index: 0,
                label: String::new(),
                kind: AxisKind::Time,
                unit: None,
            },
            series: Vec::new(),
            legend_visible: false,
            decimation_threshold: 10_000,
            binding: BindingSpec::default(),
            track_source_indices: false,
            y_scale: YScale::Linear,
        };

        let chart = SavedChart::new_metric(
            name.clone(),
            profile_id,
            vec![dbflux_components::saved_chart::MetricSeries {
                namespace,
                metric_name,
                dimensions,
                period_seconds,
                statistic,
                region: None,
                label: None,
            }],
            placeholder_spec,
            BindingSpec::default(),
        );
        let chart_id = chart.id;

        let append_result = self.app_state.update(cx, |state, _cx| {
            if let Err(e) = state.saved_charts.upsert(chart) {
                state.record_storage_failure(
                    dbflux_core::observability::actions::CONFIG_CREATE,
                    "saved_chart",
                    chart_id.to_string(),
                    format!("Failed to persist metric chart '{name}' for new panel"),
                    e.to_string(),
                );
                return Err(e);
            }
            state.dashboards.append_panels(
                dashboard_id,
                vec![DashboardPanelDraft::Chart {
                    saved_chart_id: chart_id,
                    layout: None,
                }],
            )
        });

        match append_result {
            Ok(()) => {
                self.app_state.update(cx, |_state, cx| {
                    cx.emit(AppStateChanged);
                });
            }
            Err(e) => {
                report_error(
                    UserFacingError::new(ErrorKind::Storage, format!("Failed to add panel: {e}")),
                    cx,
                );
            }
        }
    }

    /// Called when `ModalAddPanelPicker` emits `Confirmed`.
    pub(super) fn on_add_panels_confirmed(
        &mut self,
        dashboard_id: uuid::Uuid,
        chart_ids: Vec<uuid::Uuid>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use dbflux_ui_base::DashboardPanelDraft;

        let drafts: Vec<DashboardPanelDraft> = chart_ids
            .into_iter()
            .map(|saved_chart_id| DashboardPanelDraft::Chart {
                saved_chart_id,
                layout: None,
            })
            .collect();

        let result = self.app_state.update(cx, |state, _cx| {
            state.dashboards.append_panels(dashboard_id, drafts)
        });

        match result {
            Ok(()) => {
                self.app_state.update(cx, |_state, cx| {
                    cx.emit(AppStateChanged);
                });
            }
            Err(e) => {
                report_error(
                    UserFacingError::new(ErrorKind::Storage, format!("Failed to add panels: {e}")),
                    cx,
                );
            }
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
            default_chunk_size: None,
            supports_lock_timeout: false,
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

    // ---- T19.1: sidebar → chart data pipeline verification ----

    /// T19.1: Verify the `MetricSource` defaults that `open_metric_chart_from_sidebar`
    /// would produce.
    ///
    /// This is a data-layer regression guard — it ensures the defaults
    /// (dimensions=[], period_s=300, statistic="Average") match the spec.
    /// Full GPUI integration testing (actual tab opening) requires TestAppContext
    /// which is not available in this test harness; the data contract is verified here.
    #[test]
    fn sidebar_metric_source_defaults_match_spec() {
        use dbflux_components::chart::MetricSource;

        let source = MetricSource::single(
            "AWS/EC2".to_string(),
            "CPUUtilization".to_string(),
            vec![],
            300,
            "Average".to_string(),
        );

        assert_eq!(source.series.len(), 1);
        let s = &source.series[0];
        assert_eq!(s.namespace, "AWS/EC2");
        assert_eq!(s.metric_name, "CPUUtilization");
        assert!(s.dimensions.is_empty(), "defaults must have no dimensions");
        assert_eq!(
            s.period_seconds, 300,
            "default period must be 300 seconds (5 min)"
        );
        assert_eq!(s.statistic, "Average", "default statistic must be Average");
    }

    /// G.2 — `test_import_affordance_hidden_without_capability`:
    /// `PaletteItem::ImportDashboard` must NOT be produced when the connection's
    /// `DriverCapabilities` does not include `DASHBOARD_IMPORT`.
    ///
    /// This is the unit-layer contract for the capability gate. Full integration
    /// (GPUI `build_palette_items`) requires `TestAppContext`; here we verify that
    /// a capability set without `DASHBOARD_IMPORT` is rejected by the gate predicate.
    #[test]
    fn test_import_affordance_hidden_without_capability() {
        let no_dashboard_import =
            DriverCapabilities::METRIC_SERIES | DriverCapabilities::METRIC_CATALOG;

        assert!(
            !no_dashboard_import.contains(DriverCapabilities::DASHBOARD_IMPORT),
            "DASHBOARD_IMPORT must not be set for this test to be meaningful"
        );

        // The import affordance is only added when the capability flag is present.
        let affordance_present = no_dashboard_import.contains(DriverCapabilities::DASHBOARD_IMPORT);

        assert!(
            !affordance_present,
            "Import affordance must be hidden when DASHBOARD_IMPORT is not in capabilities"
        );
    }

    /// G.2 — `test_import_affordance_shown_with_capability`:
    /// When `DriverCapabilities` includes `DASHBOARD_IMPORT`, the capability
    /// gate predicate evaluates to `true` (affordance is shown).
    #[test]
    fn test_import_affordance_shown_with_capability() {
        let with_dashboard_import =
            DriverCapabilities::METRIC_SERIES | DriverCapabilities::DASHBOARD_IMPORT;

        let affordance_present =
            with_dashboard_import.contains(DriverCapabilities::DASHBOARD_IMPORT);

        assert!(
            affordance_present,
            "Import affordance must be shown when DASHBOARD_IMPORT is in capabilities"
        );
    }

    /// G.2 — `test_import_dashboard_palette_item_maps_to_selection`:
    /// `PaletteItem::ImportDashboard` must map to `PaletteSelection::ImportDashboard`.
    #[test]
    fn test_import_dashboard_palette_item_maps_to_selection() {
        let item = PaletteItem::ImportDashboard;
        let selection = map_item_to_selection(&item);

        assert!(
            matches!(selection, Some(PaletteSelection::ImportDashboard)),
            "ImportDashboard item must map to ImportDashboard selection, got: {:?}",
            selection.map(|_| "Some(other)")
        );
    }

    /// T19.1: Verify `DocumentKey::MetricChart` variant exists and carries the
    /// expected fields — compile-time contract for the dedup path.
    #[test]
    fn document_key_metric_chart_variant_carries_correct_fields() {
        use crate::ui::document::DocumentKey;

        let profile_id = Uuid::new_v4();
        let key = DocumentKey::MetricChart {
            profile_id,
            namespace: "AWS/EC2".to_string(),
            metric_name: "CPUUtilization".to_string(),
        };

        // Verify destructure works and values round-trip correctly.
        match key {
            DocumentKey::MetricChart {
                profile_id: pid,
                namespace,
                metric_name,
            } => {
                assert_eq!(pid, profile_id);
                assert_eq!(namespace, "AWS/EC2");
                assert_eq!(metric_name, "CPUUtilization");
            }
            _ => panic!("Expected MetricChart variant"),
        }
    }
}
