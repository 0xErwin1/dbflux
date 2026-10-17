use super::*;

impl DataGridPanel {
    /// Enable panel control buttons (hide, maximize) for embedded panels.
    #[allow(dead_code)]
    pub fn with_panel_controls(mut self) -> Self {
        self.chrome.show_panel_controls = true;
        self
    }

    /// Update the maximized state (called by parent).
    pub fn set_maximized(&mut self, maximized: bool, cx: &mut Context<Self>) {
        self.chrome.is_maximized = maximized;
        cx.notify();
    }

    /// Toggle between available view modes for the current data source.
    // next_idx is a modular step over available, whose length is > 1 after
    // the early return.
    #[expect(
        clippy::indexing_slicing,
        reason = "next_idx = (current_idx + 1) % available.len() and available.len() > 1 after the early return"
    )]
    pub fn toggle_view_mode(&mut self, cx: &mut Context<Self>) {
        if self.is_document_collection(cx) {
            self.cycle_document_view(cx);
            return;
        }

        let available = self.data_view_modes(cx);
        if available.len() <= 1 {
            return;
        }

        let current_idx = available
            .iter()
            .position(|m| *m == self.view_config.mode)
            .unwrap_or(0);

        let next_idx = (current_idx + 1) % available.len();
        self.view_config.mode = available[next_idx];
        cx.notify();
    }

    /// Check if view mode toggle is available for the current source.
    pub fn can_toggle_view(&self, cx: &App) -> bool {
        self.data_view_modes(cx).len() > 1
    }

    /// The data views this source offers: a table's from its facets (a
    /// `jsonb` column adds the document tree), every other source's from its
    /// kind.
    pub(super) fn data_view_modes(&self, cx: &App) -> Vec<super::super::data_view::DataViewMode> {
        use super::super::data_view::DataViewMode;

        match &self.source {
            DataSource::Table { .. } => DataViewMode::for_facets(&self.facets(cx)),
            _ => DataViewMode::available_for(&self.source),
        }
    }

    pub fn record_mode(&self) -> bool {
        self.chrome.record_mode
    }

    /// Switch the result area between the grid and the record view.
    ///
    /// The flag lives on the panel rather than only on `DataTableState`
    /// because `rebuild_table` creates a fresh state on every refresh and
    /// requery; `apply_record_mode` re-applies it there.
    pub fn set_record_mode(&mut self, record_mode: bool, cx: &mut Context<Self>) {
        if record_mode && !self.record_view_available() {
            return;
        }

        if self.chrome.record_mode == record_mode {
            return;
        }

        self.chrome.record_mode = record_mode;
        self.apply_record_mode(cx);
        cx.notify();
    }

    /// Whether the record view can be entered right now.
    ///
    /// It is a presentation of the data grid, so it exists only where the grid
    /// itself is on screen: a result shown as JSON, text, raw bytes or a chart
    /// has no grid to transpose, the document tree is not a grid, and a grouped
    /// aggregate has no addressable source row. The status-bar toggle, the
    /// keyboard command and `set_record_mode` all go through this one check so
    /// none of them can flip a mode the user cannot see.
    pub fn record_view_available(&self) -> bool {
        self.grid_table.table_state.is_some()
            && self.shows_table_content()
            && !self.is_grouped_result()
    }

    /// Whether the result area renders the data grid, as opposed to a result
    /// view, the document tree or the empty fallback.
    pub(super) fn shows_table_content(&self) -> bool {
        let has_data = !self.result.rows.is_empty()
            || self.result.text_body.is_some()
            || self.result.raw_bytes.is_some();
        let has_columns = !self.result.columns.is_empty();
        let content_mode = render::content_mode_for_result(
            self.uses_result_view(),
            self.view_config.mode,
            has_columns,
            has_data,
        );
        matches!(content_mode, render::DataGridContentMode::Table)
    }

    /// Push the panel's record-mode flag onto the current `DataTableState`.
    ///
    /// This runs after every rebuild, so it is also where a result that cannot
    /// be shown as a record clears the flag: a query that was in record mode
    /// and comes back grouped would otherwise reapply the cached `true` around
    /// the guard in `set_record_mode`, leaving the aggregate in a mode whose
    /// toggle has just disappeared.
    pub(super) fn apply_record_mode(&mut self, cx: &mut Context<Self>) {
        if self.chrome.record_mode && self.is_grouped_result() {
            self.chrome.record_mode = false;
        }

        let record_mode = self.chrome.record_mode;
        if let Some(table_state) = &self.grid_table.table_state {
            table_state.update(cx, |state, cx| state.set_record_mode(record_mode, cx));
        }
    }

    pub fn result_view_mode(&self) -> ResultViewMode {
        self.chrome.result_view_mode
    }

    /// Declares that the hosting document forwards this grid's
    /// `side_panels` to the workspace. Until then the grid docks its chart
    /// stats rail inside itself, so a host that does not forward them keeps
    /// the rail.
    pub fn set_side_panels_hosted(&mut self, hosted: bool) {
        self.side_panels_hosted = hosted;
    }

    /// The mode currently displayed in the result view. Alias of
    /// `result_view_mode` used by `ResultPanel` wiring in `DataDocument`.
    pub fn current_result_view_mode(&self) -> ResultViewMode {
        self.chrome.result_view_mode
    }

    /// Modes available for the current result shape and connection category.
    ///
    /// Returns an empty slice for sources without result views (a document
    /// collection browse). Otherwise returns the modes available for the
    /// shape, plus Chart when chart detection succeeded. Independent of the currently active mode — switching to
    /// Chart and back must not change which modes are offered.
    pub fn available_result_view_modes(&self, cx: &App) -> Vec<ResultViewMode> {
        if !self.has_result_views() {
            return vec![];
        }

        let mut modes = ResultViewMode::available_for_shape(&self.result.shape);

        // Chart follows the shape's own views (Data | JSON | Chart on the
        // boards) when chart detection succeeded.
        if self.chart_available(cx) && !modes.contains(&ResultViewMode::Chart) {
            modes.push(ResultViewMode::Chart);
        }

        // A time-series measurement also offers the chart above the grid.
        if self.chart.time_series_source
            && self.chart_available(cx)
            && !modes.contains(&ResultViewMode::Both)
            && let Some(pos) = modes.iter().position(|m| *m == ResultViewMode::Chart)
        {
            modes.insert(pos + 1, ResultViewMode::Both);
        }

        modes
    }

    pub fn set_result_view_mode(&mut self, mode: ResultViewMode, cx: &mut Context<Self>) {
        if self.chrome.result_view_mode == mode {
            return;
        }

        self.chrome.result_view_mode = mode;
        cx.notify();
    }

    /// Shows `mode` from the keyboard and keeps the keyboard in the grid:
    /// the table element the keyboard may have been on is not drawn in the
    /// other views.
    pub(super) fn show_result_view(
        &mut self,
        mode: ResultViewMode,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.set_result_view_mode(mode, cx);
        self.focus_table(window, cx);
    }

    /// Shows the view after the current one among those the result offers
    /// (`Command::CycleResultView`), wrapping. Returns false when the result
    /// has a single view.
    // modes.len() >= 2 after the early return and next is either 0 or a
    // modular step over modes.len().
    #[expect(
        clippy::indexing_slicing,
        reason = "modes.len() >= 2 after the early return and next is 0 or a modular step over modes.len()"
    )]
    pub(super) fn cycle_result_view(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let modes = self.available_result_view_modes(cx);
        if modes.len() < 2 {
            return false;
        }

        let next = modes
            .iter()
            .position(|mode| *mode == self.chrome.result_view_mode)
            .map_or(0, |index| (index + 1) % modes.len());

        self.show_result_view(modes[next], window, cx);
        true
    }

    /// Whether the source offers the Data / Chart / JSON result views: every
    /// query result and table browse, and a collection on a time-series
    /// connection.
    pub(super) fn has_result_views(&self) -> bool {
        matches!(
            self.source,
            DataSource::QueryResult { .. } | DataSource::Table { .. }
        ) || self.chart.time_series_source
    }

    /// Whether the footer carries the view switch (Grid / JSON / Chart): a
    /// table or collection draws it there (AppByzTable), while a query result
    /// uses the `ResultPanel` mode bar above its content.
    pub(super) fn footer_hosts_view_switch(&self) -> bool {
        matches!(
            self.source,
            DataSource::Table { .. } | DataSource::Collection { .. }
        )
    }

    pub(super) fn uses_result_view(&self) -> bool {
        self.has_result_views() && !self.chrome.result_view_mode.is_table()
    }

    pub(super) fn derived_text(&mut self) -> &str {
        if self.chrome.derived_text.is_none() {
            self.chrome.derived_text = Some(self.compute_derived_text());
        }
        self.chrome.derived_text.as_deref().unwrap_or("")
    }

    pub(super) fn derived_json(&mut self) -> &str {
        if self.chrome.derived_json.is_none() {
            self.chrome.derived_json = Some(self.compute_derived_json());
        }
        self.chrome.derived_json.as_deref().unwrap_or("")
    }

    pub(super) fn compute_derived_text(&self) -> String {
        if let Some(body) = &self.result.text_body {
            return body.clone();
        }

        // Fall back to rendering rows as text
        self.result
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|v| v.as_display_string())
                    .collect::<Vec<_>>()
                    .join("\t")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub(super) fn compute_derived_json(&self) -> String {
        use utils::value_to_json;

        if let Some(body) = &self.result.text_body {
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(body) {
                return serde_json::to_string_pretty(&parsed).unwrap_or_else(|_| body.clone());
            }
            return body.clone();
        }

        // Build JSON from rows
        // row.len() == 1 is checked right before the [0] index in the
        // single-value branch.
        #[expect(
            clippy::indexing_slicing,
            reason = "row[0] is reached only in the branch guarded by row.len() == 1"
        )]
        let json_rows: Vec<serde_json::Value> = self
            .result
            .rows
            .iter()
            .map(|row| {
                if self.result.columns.is_empty() {
                    // Single-value rows
                    if row.len() == 1 {
                        value_to_json(&row[0])
                    } else {
                        serde_json::Value::Array(row.iter().map(value_to_json).collect())
                    }
                } else {
                    let obj: serde_json::Map<String, serde_json::Value> = self
                        .result
                        .columns
                        .iter()
                        .zip(row.iter())
                        .map(|(col, val)| (col.name.clone(), value_to_json(val)))
                        .collect();
                    serde_json::Value::Object(obj)
                }
            })
            .collect();

        if json_rows.len() == 1 {
            #[expect(
                clippy::indexing_slicing,
                reason = "json_rows.len() == 1 is checked by the enclosing if"
            )]
            serde_json::to_string_pretty(&json_rows[0]).unwrap_or_default()
        } else {
            serde_json::to_string_pretty(&json_rows).unwrap_or_default()
        }
    }

    pub fn supports_auto_refresh(&self) -> bool {
        matches!(
            self.source,
            DataSource::Table { .. } | DataSource::Collection { .. }
        ) || matches!(self.source, DataSource::QueryResult { .. })
            && self.chart.chart_source_time_range_panel.is_some()
    }
}
