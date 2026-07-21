use super::*;

impl Workspace {
    /// Opens a `ChartDocument` pre-populated with the metric selected in the
    /// sidebar and immediately executes it.
    ///
    /// Defaults: `dimensions = []`, `period_s = 300`, `statistic = "Average"`.
    /// If a chart with the same `(profile_id, namespace, metric_name)` is
    /// already open the existing tab is focused instead of opening a duplicate.
    pub(in crate::ui::views::workspace) fn open_metric_chart_from_sidebar(
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
    pub(in crate::ui::views::workspace) fn open_instance_metric(
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
    pub(in crate::ui::views::workspace) fn open_instance_inspector(
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
    pub(in crate::ui::views::workspace) fn open_instance_overview(
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
    pub(in crate::ui::views::workspace) fn save_overview_as_editable(
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
}
