use super::*;

impl DataGridPanel {
    // ---- Collection chart save flow ----

    /// Open the name-prompt overlay for saving a chart from a Collection or
    /// QueryResult source.
    ///
    /// Captures the current chart spec and bindings from the shell. No-op when
    /// no chart shell exists or the source has no associated profile.
    pub fn open_collection_chart_save(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !matches!(
            &self.source,
            DataSource::Collection { .. } | DataSource::QueryResult { .. }
        ) {
            return;
        }

        let Some(shell) = &self.chart.chart_shell else {
            return;
        };

        let columns = self.result.columns.clone();
        let spec = shell.read(cx).current_chart_spec(&columns);
        let bindings = shell.read(cx).active_bindings();

        let name_input = cx.new(|cx| {
            dbflux_components::controls::InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("document.data.grid.placeholder.chart_name"))
        });

        let sub = cx.subscribe_in(
            &name_input,
            window,
            |_this: &mut Self,
             _input: &Entity<dbflux_components::controls::InputState>,
             _event: &dbflux_components::controls::InputEvent,
             _window,
             _cx| {},
        );

        self.pending_collection_chart_save = Some(CollectionChartSaveState {
            name_input,
            chart_spec: spec,
            bindings,
            _subscription: sub,
        });

        cx.notify();
    }

    /// Confirm the collection-chart name prompt and persist the chart.
    pub fn confirm_collection_chart_save(&mut self, cx: &mut Context<Self>) {
        let Some(state) = self.pending_collection_chart_save.take() else {
            return;
        };

        let name = state.name_input.read(cx).value().trim().to_string();
        if name.is_empty() {
            // Put it back — user must enter a name.
            self.pending_collection_chart_save = Some(state);
            return;
        }

        let chart = match &self.source {
            DataSource::Collection {
                profile_id,
                collection,
                ..
            } => {
                let time_window = self.result.resolved_window.clone();
                dbflux_components::saved_chart::SavedChart::new_collection(
                    name.clone(),
                    *profile_id,
                    collection.clone(),
                    time_window,
                    state.chart_spec,
                    state.bindings,
                )
            }
            DataSource::QueryResult {
                profile_id,
                original_query,
                ..
            } => {
                let Some(profile_id) = profile_id else {
                    self.pending.toast = Some(dbflux_ui_base::toast::PendingToast {
                        message: crate::labels::chart_save_no_profile_binding_error(),
                        is_error: true,
                    });
                    cx.notify();
                    return;
                };
                dbflux_components::saved_chart::SavedChart::new_query(
                    name.clone(),
                    *profile_id,
                    original_query.clone(),
                    state.chart_spec,
                    state.bindings,
                )
            }
            _ => return,
        };

        let chart_id = chart.id;
        let persist_result = self.app_state.update(cx, |app, _cx| {
            app.saved_charts.upsert(chart).inspect_err(|e| {
                app.record_storage_failure(
                    dbflux_core::observability::actions::CONFIG_CREATE,
                    "saved_chart",
                    chart_id.to_string(),
                    format!("Failed to save chart '{name}'"),
                    e.to_string(),
                );
            })
        });

        self.pending.toast = Some(match persist_result {
            Ok(_) => dbflux_ui_base::toast::PendingToast {
                message: crate::labels::chart_saved_toast(&name),
                is_error: false,
            },
            Err(e) => dbflux_ui_base::toast::PendingToast {
                message: crate::labels::chart_save_failed_error(&name, &e.to_string()),
                is_error: true,
            },
        });

        cx.notify();
    }

    /// Cancel the collection-chart name prompt without saving.
    pub fn cancel_collection_chart_save(&mut self, cx: &mut Context<Self>) {
        self.pending_collection_chart_save = None;
        cx.notify();
    }

    /// Returns `true` when the current result has a `Timestamp` column and at
    /// least one numeric column — i.e., chart mode is available.
    pub(crate) fn chart_available(&self, cx: &App) -> bool {
        self.chart
            .chart_shell
            .as_ref()
            .is_some_and(|s| s.read(cx).chart_available())
    }

    /// Build or return the existing `ChartView` entity for the current result.
    ///
    /// Delegates to `ChartShell::ensure_chart_view`. Returns `None` when no
    /// shell exists or when detection failed.
    pub(crate) fn ensure_chart_view(
        &mut self,
        cx: &mut Context<Self>,
    ) -> Option<Entity<ChartView>> {
        let result = self.result.clone();
        self.chart
            .chart_shell
            .as_ref()?
            .update(cx, |shell, cx| shell.ensure_chart_view(&result, cx))
    }

    /// Toggle the hidden state of a series by index.
    ///
    /// Delegates to `ChartShell::toggle_chart_series_hidden`.
    pub(crate) fn toggle_chart_series_hidden(&mut self, idx: usize, cx: &mut Context<Self>) {
        if let Some(shell) = &self.chart.chart_shell {
            shell.update(cx, |s, cx| s.toggle_chart_series_hidden(idx, cx));
        }
    }

    /// Wire the source-context time-range panel into this chart panel.
    ///
    /// Called by `CodeDocument` after it lazily creates the `TimeRangePanel`.
    /// The chart toolbar reads and writes the panel to drive RANGE chip selection.
    pub fn set_chart_time_range_panel(
        &mut self,
        panel: Option<Entity<dbflux_components::common::time_range::view::TimeRangePanel>>,
        cx: &mut Context<Self>,
    ) {
        self.chart.chart_source_time_range_panel = panel;

        let enabled = self.supports_auto_refresh();
        self.filter_bar.refresh_dropdown.update(cx, |dd, cx| {
            dd.set_disabled(!enabled, cx);
        });

        cx.notify();
    }

    /// Prime the rail Configure picker from the current chart spec.
    ///
    /// Called when the rail is toggled open so the controls reflect what is
    /// currently rendered (either auto-detected or manual).
    ///
    /// Only invoked from the (now-dead) Configure rail tab.
    #[allow(dead_code)]
    pub(crate) fn prime_chart_rail_picker_from_spec(&mut self, cx: &mut Context<Self>) {
        let result = self.result.clone();
        if let Some(shell) = &self.chart.chart_shell {
            shell.update(cx, |s, _cx| s.prime_rail_picker_from_spec(&result));
        }
    }

    /// Apply the current rail Configure picker state as a `ManualChartSelection`.
    ///
    /// Clears the existing `chart_view` so the next render triggers a rebuild.
    /// Only invoked from the (now-dead) Configure rail tab.
    #[allow(dead_code)]
    pub(crate) fn apply_chart_rail_selection(&mut self, cx: &mut Context<Self>) {
        let result = self.result.clone();
        if let Some(shell) = &self.chart.chart_shell {
            shell.update(cx, |s, cx| s.apply_rail_selection(&result, cx));
        }
    }

    /// Reset chart selection to auto-detection, clearing any manual override.
    ///
    /// Disabled (no-op) when detection did not produce an `Ok` result.
    /// Only invoked from the (now-dead) Configure rail tab.
    #[allow(dead_code)]
    pub(crate) fn reset_chart_rail_to_auto(&mut self, cx: &mut Context<Self>) {
        let result = self.result.clone();
        if let Some(shell) = &self.chart.chart_shell {
            shell.update(cx, |s, cx| s.reset_rail_to_auto(&result, cx));
        }
    }

    /// Picks the result view for a fresh result and keeps the chart shell in
    /// step with it.
    ///
    /// Shared by query results (`set_result`) and collection browses
    /// (`apply_collection_result`), so a time-series collection opens as a
    /// chart the same way a time-series query result can.
    pub(super) fn apply_chart_for_result(&mut self, result: &QueryResult, cx: &mut Context<Self>) {
        let was_chart_mode = self.chrome.result_view_mode.shows_chart();

        let detection = detect_chart_columns(result);
        let detection_ok = matches!(detection, ChartDetection::Ok { .. });

        let time_series_collection = self.chart.time_series_source;
        let first_time_series_result =
            time_series_collection && !self.chart.time_series_result_seen;

        self.chrome.result_view_mode = result_view_mode_for_fresh_result(
            self.chrome.result_view_mode,
            &result.shape,
            &detection,
            time_series_collection,
            first_time_series_result,
        );

        if time_series_collection {
            self.chart.time_series_result_seen = true;
        }

        if !detection_ok && self.chart.chart_shell.is_none() {
            return;
        }

        let track_source_rows = Self::chart_tracks_source_rows(&self.source);

        if let Some(shell) = &self.chart.chart_shell {
            shell.update(cx, |s, cx| {
                s.set_track_source_indices(track_source_rows, cx);
                s.set_result(result, was_chart_mode, cx);
            });
        } else {
            let host = crate::chart::HostAdapter::DataGrid(cx.entity().clone());
            let shell = cx.new(|cx| {
                let mut shell = crate::chart::ChartShell::new(host, cx);
                shell.set_track_source_indices(track_source_rows, cx);
                shell.set_result(result, false, cx);
                shell
            });
            self.chart.chart_shell = Some(shell);
        }

        // Seed the axis bar (time, first numeric, first text tag) only for the
        // first time-series collection result, so a refresh never clobbers
        // bindings the user adjusted.
        if first_time_series_result
            && let ChartDetection::Ok {
                time_col,
                ref numeric_cols,
            } = detection
        {
            // The time column the driver named wins over the first timestamp
            // column detection found.
            let time_col = self
                .chart
                .time_series_column
                .as_deref()
                .and_then(|name| {
                    result.columns.iter().position(|column| {
                        column.name == name && column.kind == dbflux_core::ColumnKind::Timestamp
                    })
                })
                .unwrap_or(time_col);
            let numeric_cols: Vec<usize> = numeric_cols
                .iter()
                .copied()
                .filter(|column| *column != time_col)
                .collect();
            let bindings = default_bindings_for_time_series(time_col, &numeric_cols, result);
            if let Some(shell) = &self.chart.chart_shell {
                shell.update(cx, |s, cx| s.apply_bindings(bindings, cx));
            }
        }
    }

    // ---- ChartHost delegation methods ----
    // These are called by `HostAdapter::DataGrid` to implement `ChartHost`
    // without requiring a mutable self-borrow in read contexts.

    /// Returns the original query text for the current `QueryResult` source.
    ///
    /// Returns `None` for `Table` and `Collection` sources that do not expose
    /// a user-authored query string.
    pub(crate) fn chart_host_current_query(&self, _cx: &App) -> Option<String> {
        match &self.source {
            DataSource::QueryResult { original_query, .. } => {
                if original_query.is_empty() {
                    None
                } else {
                    Some(original_query.clone())
                }
            }
            _ => None,
        }
    }

    /// Returns the profile ID for the current source, if any.
    pub(crate) fn chart_host_connection_id(&self, _cx: &App) -> Option<Uuid> {
        match &self.source {
            DataSource::Table { profile_id, .. } => Some(*profile_id),
            DataSource::Collection { profile_id, .. } => Some(*profile_id),
            DataSource::QueryResult { profile_id, .. } => *profile_id,
        }
    }

    /// Returns the time-range panel wired in by the parent document.
    pub(crate) fn chart_host_time_range_panel(
        &self,
        _cx: &App,
    ) -> Option<Entity<dbflux_components::common::time_range::view::TimeRangePanel>> {
        self.chart.chart_source_time_range_panel.clone()
    }

    /// Returns the refresh-policy dropdown entity.
    ///
    /// The dropdown is created at construction time and lives here for the
    /// panel's lifetime. The chart toolbar uses it so the user can change the
    /// policy while viewing a chart.
    pub(crate) fn chart_host_refresh_dropdown(&self, _cx: &App) -> Option<Entity<Dropdown>> {
        Some(self.filter_bar.refresh_dropdown.clone())
    }

    /// Returns the current result as a shared `Arc<QueryResult>`.
    ///
    /// For `QueryResult` sources the result is already `Arc`-wrapped in the
    /// source; for other sources we wrap the live `result` field in a new
    /// `Arc` (shallow clone, no data copy).
    pub(crate) fn chart_host_current_result(&self, _cx: &App) -> Option<Arc<QueryResult>> {
        match &self.source {
            DataSource::QueryResult { result, .. } => Some(result.clone()),
            DataSource::Table { .. } | DataSource::Collection { .. } => {
                Some(Arc::new(self.result.clone()))
            }
        }
    }

    /// Trigger a re-execution of the current query.
    ///
    /// For `QueryResult` sources this emits the time-range panel's initial
    /// event, which causes `CodeDocument` to re-run the query. For table /
    /// collection sources this calls `refresh`.
    pub(crate) fn chart_host_request_reexecute(&mut self, cx: &mut Context<Self>) {
        match &self.source {
            DataSource::QueryResult { .. } => {
                if let Some(trp) = self.chart.chart_source_time_range_panel.clone() {
                    trp.update(cx, |p, cx| p.emit_initial(cx));
                }
            }
            _ => {
                if self.reload_blocked_by_pending_edits(cx) {
                    return;
                }

                self.pending.refresh = true;
                cx.notify();
            }
        }
    }

    /// Look up the source row for a decimated chart point.
    ///
    /// Consults the `RenderModel.source_indices` built by `ChartView::build`
    /// when `ChartSpec.track_source_indices` was enabled. Returns `None` when
    /// source tracking is disabled (e.g. CodeDocument-backed charts) or when
    /// the index is out of range.
    pub(crate) fn chart_host_source_for_point(
        &self,
        point: DataPointRef,
        cx: &App,
    ) -> Option<SourceRowRef> {
        let shell = self.chart.chart_shell.as_ref()?.read(cx);
        let chart_entity = shell.chart_view()?.clone();
        let chart = chart_entity.read(cx);

        let src_indices = chart.source_indices()?;
        let series_indices = src_indices.get(point.series_idx)?;
        let row_idx = *series_indices.get(point.point_idx_in_series)?;

        Some(SourceRowRef { row_idx })
    }

    /// Whether this grid's charts record the source row of each point, which
    /// the point inspector and its "Show in tree" need.
    ///
    /// A table or collection tab keeps its rows and can move to one, so it
    /// tracks them. A query result (the query editor's results) does not, to
    /// spare the per-point index memory.
    pub(crate) fn chart_tracks_source_rows(source: &DataSource) -> bool {
        matches!(
            source,
            DataSource::Table { .. } | DataSource::Collection { .. }
        )
    }

    /// Show a chart point's source row in the table: leave a chart-only view
    /// for the table, then select, scroll to and focus that row.
    ///
    /// Keeps the selected column when the table has one. A no-op when the
    /// grid has no table or the row is out of range.
    pub(crate) fn chart_host_scroll_to_row(
        &mut self,
        row_idx: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = self.grid_table.table_state.clone() else {
            return;
        };

        if row_idx >= self.result.rows.len() {
            return;
        }

        if self.chrome.result_view_mode == ResultViewMode::Chart {
            self.set_result_view_mode(ResultViewMode::Table, cx);
        }

        table_state.update(cx, |state, cx| {
            let col = state.selection().active.map_or(0, |cell| cell.col);
            state.select_cell(CellCoord::new(row_idx, col), cx);
            state.scroll_to_row(row_idx);
        });

        self.focus_table(window, cx);
    }
}
