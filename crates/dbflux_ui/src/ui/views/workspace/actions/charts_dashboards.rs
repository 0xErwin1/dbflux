use super::*;

impl Workspace {
    /// Opens a new `ChartDocument` seeded with the given query.
    ///
    /// Called when the user selects "Chart this query" from a data grid context menu.
    pub(in crate::ui::views::workspace) fn open_chart_from_query(
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
    pub(in crate::ui::views::workspace) fn build_saved_chart_palette_items(
        &self,
        cx: &Context<Self>,
    ) -> Vec<PaletteItem> {
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
    pub(in crate::ui::views::workspace) fn open_saved_chart(
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
    pub(in crate::ui::views::workspace) fn open_dashboard(
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
    pub(in crate::ui::views::workspace) fn run_dashboard_import(
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
    pub(in crate::ui::views::workspace) fn open_remote_dashboard(
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
    pub(in crate::ui::views::workspace) fn reopen_last_connections(
        &mut self,
        cx: &mut Context<Self>,
    ) {
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
    pub(in crate::ui::views::workspace) fn create_dashboard_from_sidebar(
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
    pub(in crate::ui::views::workspace) fn create_dashboard_from_palette(
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
    pub(in crate::ui::views::workspace) fn on_create_dashboard_confirmed(
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
    pub(in crate::ui::views::workspace) fn import_dashboard_for_profile(
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
    pub(in crate::ui::views::workspace) fn rename_dashboard(
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
    pub(in crate::ui::views::workspace) fn delete_dashboard(
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
    pub(in crate::ui::views::workspace) fn on_delete_dashboard_confirmed(
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
    pub(in crate::ui::views::workspace) fn duplicate_dashboard(
        &mut self,
        dashboard_id: uuid::Uuid,
        cx: &mut Context<Self>,
    ) {
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
    pub(in crate::ui::views::workspace) fn rename_saved_chart(
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
    pub(in crate::ui::views::workspace) fn on_rename_item_confirmed(
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
    pub(in crate::ui::views::workspace) fn delete_saved_chart(
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
        // dashboards_repo() is fallible; degrade gracefully on open failure while logging.
        let referencing_ids = self
            .app_state
            .read(cx)
            .storage_runtime()
            .dashboards_repo()
            .map_err(|e| {
                log::warn!("Failed to open dashboards repo for orphan check: {e:?}");
                e
            })
            .ok()
            .and_then(|r| {
                r.find_dashboards_referencing_chart(chart_id)
                    .map_err(|e| {
                        log::warn!(
                            "Failed to query dashboards referencing chart {chart_id}: {e:?}"
                        );
                        e
                    })
                    .ok()
            })
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
    pub(in crate::ui::views::workspace) fn on_delete_saved_chart_confirmed(
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
    pub(in crate::ui::views::workspace) fn duplicate_saved_chart(
        &mut self,
        chart_id: uuid::Uuid,
        cx: &mut Context<Self>,
    ) {
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
    pub(in crate::ui::views::workspace) fn open_add_panel_picker(
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
    pub(in crate::ui::views::workspace) fn on_request_metrics_for_namespace(
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
    pub(in crate::ui::views::workspace) fn on_create_panel_from_query(
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
    pub(in crate::ui::views::workspace) fn on_create_panel_from_metric(
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
    pub(in crate::ui::views::workspace) fn on_add_panels_confirmed(
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
