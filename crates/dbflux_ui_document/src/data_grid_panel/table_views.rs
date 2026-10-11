//! The Columns and Partitions views of a table, next to its Data grid.
//!
//! A table gets them only when its connection reports the matching
//! [`TableViewFeatures`]; the grid never checks a driver id. The profile
//! behind the Columns view is read from metadata when the table opens (the
//! driver promises not to read rows for it), and it also feeds the Data grid:
//! the second header line of each column, the column projection picker and
//! the read estimate. Reading a sample of rows is an explicit, audited action.

use std::sync::Arc;
use std::time::Instant;

use dbflux_components::components::column_facts::{
    format_optional_bytes, format_percent, format_ratio,
};
use dbflux_components::components::column_profile_view::{
    ColumnProfileActionRequested, ColumnProfileView, ProfileAction, ProfileActions,
};
use dbflux_components::components::column_projection::{ColumnProjectionPicker, ProjectionChanged};
use dbflux_components::components::data_table::HeaderAnnotation;
use dbflux_components::components::partition_list_view::{
    PartitionListRefreshRequested, PartitionListView,
};
use dbflux_components::components::read_estimate_bar::ReadEstimateBar;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{SegmentedControl, SegmentedItem};
use dbflux_components::tokens::TableViewMetrics;
use dbflux_core::observability::actions as audit_actions;
use dbflux_core::observability::{
    EventCategory, EventOrigin, EventOutcome, EventRecord, EventSeverity,
};
use dbflux_core::{
    ColumnBadge, ColumnProfile, ColumnProjection, Connection, DbError, ProfileSource,
    TableBrowseRequest, TableProfile, TableRef, TableViewFeatures,
};
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::*;
use gpui_component::ActiveTheme;

use super::{DataGridPanel, DataSource, TableReload};

/// Rows a sample profile reads. The limit goes inside the query the driver
/// runs, so a sample never reads more.
pub(super) const SAMPLE_PROFILE_ROWS: u64 = 1_000_000;

/// Data, Columns or Partitions, the views of a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum TableTab {
    #[default]
    Data,
    Columns,
    Partitions,
}

/// The table views `features` offer, in header order: Data always, Columns
/// with a column profile, Partitions with partitions.
pub(super) fn available_table_tabs(features: TableViewFeatures) -> Vec<TableTab> {
    let mut tabs = vec![TableTab::Data];

    if features.contains(TableViewFeatures::COLUMN_PROFILE) {
        tabs.push(TableTab::Columns);
    }

    if features.contains(TableViewFeatures::PARTITIONS) {
        tabs.push(TableTab::Partitions);
    }

    tabs
}

/// The table views of a table source. Inert for other sources and for
/// connections that report no table view features.
#[derive(Default)]
pub(super) struct TableViewsState {
    pub(super) features: TableViewFeatures,
    pub(super) tab: TableTab,
    /// The metadata profile, with a sample merged into it once one ran.
    pub(super) profile: Option<Arc<TableProfile>>,
    /// Bumped on every profile read, so a late answer to an older read is
    /// dropped.
    profile_generation: u64,
    pub(super) profile_view: Option<Entity<ColumnProfileView>>,
    pub(super) partitions_view: Option<Entity<PartitionListView>>,
    partitions_requested: bool,
    /// Bumped on every partitions read, so a late answer is dropped.
    partitions_generation: u64,
    pub(super) picker: Option<Entity<ColumnProjectionPicker>>,
    /// The columns of `profile` the Data grid reads. `None` reads them all.
    pub(super) projection: Option<ColumnProjection>,
    pub(super) estimate: Option<ReadEstimateBar>,
    estimate_generation: u64,
    /// The last browse the Data grid ran, so a later estimate describes the
    /// read that is on screen rather than a filter still being typed.
    last_browse: Option<TableBrowseRequest>,
    /// The result columns and profile generation the header annotations were
    /// last computed for.
    annotated_for: Option<(u64, Vec<String>)>,
    _subscriptions: Vec<Subscription>,
}

impl DataGridPanel {
    /// The connection a table source runs against, resolved for its database.
    pub(super) fn table_view_connection(&self, cx: &App) -> Option<Arc<dyn Connection>> {
        let DataSource::Table {
            profile_id,
            database,
            ..
        } = &self.source
        else {
            return None;
        };

        self.app_state
            .read(cx)
            .connections()
            .get(profile_id)?
            .resolve_connection_for_execution(database.as_deref())
            .ok()
    }

    /// The table this panel shows, with the active database as its schema
    /// when it has none, as the browse request names it.
    fn table_view_target(&self, cx: &App) -> Option<TableRef> {
        let DataSource::Table {
            profile_id, table, ..
        } = &self.source
        else {
            return None;
        };

        let mut table = table.clone();

        if table.schema.is_none() {
            table.schema = self
                .app_state
                .read(cx)
                .connections()
                .get(profile_id)
                .and_then(|connected| connected.active_database.clone());
        }

        Some(table)
    }

    /// Reads the connection's table view features and, when it offers a
    /// column profile, builds the Columns (and Partitions) views and starts
    /// reading the profile from metadata.
    pub(super) fn init_table_views(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(connection) = self.table_view_connection(cx) else {
            return;
        };

        let features = connection.table_view_features();
        self.table_views.features = features;

        if !features.contains(TableViewFeatures::COLUMN_PROFILE) {
            return;
        }

        let profile_view = cx.new(|cx| ColumnProfileView::new(window, cx));
        profile_view.update(cx, |view, cx| {
            view.set_actions(
                ProfileActions {
                    refresh: true,
                    sample_rows: features
                        .contains(TableViewFeatures::SAMPLE_PROFILE)
                        .then_some(SAMPLE_PROFILE_ROWS),
                    full_scan: false,
                },
                cx,
            )
        });

        self.table_views._subscriptions.push(cx.subscribe_in(
            &profile_view,
            window,
            |this, _view, event: &ProjectionChanged, window, cx| {
                this.apply_table_projection(event.0.clone(), window, cx);
            },
        ));
        self.table_views._subscriptions.push(cx.subscribe(
            &profile_view,
            |this, _view, event: &ColumnProfileActionRequested, cx| match event.0 {
                ProfileAction::Refresh => this.load_table_profile(cx),
                ProfileAction::Sample => this.sample_table_profile(cx),
                ProfileAction::FullScan => {}
            },
        ));
        self.table_views.profile_view = Some(profile_view);

        if features.contains(TableViewFeatures::PARTITIONS) {
            let partitions_view = cx.new(PartitionListView::new);

            self.table_views._subscriptions.push(cx.subscribe(
                &partitions_view,
                |this, _view, _: &PartitionListRefreshRequested, cx| {
                    this.load_table_partitions(cx);
                },
            ));
            self.table_views.partitions_view = Some(partitions_view);
        }

        self.load_table_profile(cx);
    }

    pub(super) fn table_tabs(&self) -> Vec<TableTab> {
        if !matches!(self.source, DataSource::Table { .. }) {
            return vec![TableTab::Data];
        }

        available_table_tabs(self.table_views.features)
    }

    pub(super) fn table_tab(&self) -> TableTab {
        self.table_views.tab
    }

    /// Shows `tab`. Leaving Data throws away a draft of the column picker,
    /// which only Data draws.
    pub(super) fn set_table_tab(
        &mut self,
        tab: TableTab,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.table_views.tab == tab || !self.table_tabs().contains(&tab) {
            return;
        }

        if let Some(picker) = self.table_views.picker.clone() {
            picker.update(cx, |picker, cx| picker.discard(window, cx));
        }

        self.table_views.tab = tab;

        if tab == TableTab::Partitions && !self.table_views.partitions_requested {
            self.load_table_partitions(cx);
        }

        cx.notify();
    }

    /// Moves to the next (or previous) table view. Returns false when the
    /// table has only its Data view.
    pub(super) fn step_table_tab(
        &mut self,
        forward: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let tabs = self.table_tabs();
        if tabs.len() < 2 {
            return false;
        }

        let current = tabs
            .iter()
            .position(|tab| *tab == self.table_views.tab)
            .unwrap_or(0);
        let next = if forward {
            (current + 1) % tabs.len()
        } else {
            (current + tabs.len() - 1) % tabs.len()
        };

        let Some(&next_tab) = tabs.get(next) else {
            return false;
        };

        self.set_table_tab(next_tab, window, cx);
        true
    }

    /// Focuses the view of the current table tab. Returns false on the Data
    /// tab, whose focus the grid owns.
    pub(super) fn focus_table_view(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        match self.table_views.tab {
            TableTab::Data => false,
            TableTab::Columns => {
                if let Some(view) = self.table_views.profile_view.clone() {
                    view.update(cx, |view, cx| view.focus(window, cx));
                }
                true
            }
            TableTab::Partitions => {
                if let Some(view) = self.table_views.partitions_view.clone() {
                    view.update(cx, |view, cx| view.focus(window, cx));
                }
                true
            }
        }
    }

    /// Whether the Columns view offers to profile a sample of rows.
    pub(super) fn offers_sample_profile(&self) -> bool {
        self.table_views
            .features
            .contains(TableViewFeatures::SAMPLE_PROFILE)
            && self.table_views.profile.is_some()
    }

    /// Starts a sample profile through the Columns view, which refuses it
    /// while another profile action runs.
    pub(super) fn request_sample_profile(&mut self, cx: &mut Context<Self>) {
        if let Some(view) = self.table_views.profile_view.clone() {
            view.update(cx, |view, cx| {
                view.request_action(ProfileAction::Sample, cx)
            });
        }
    }

    /// Commands for the Columns and Partitions views. `None` lets the
    /// command reach the grid's own handling (menus, focus moves); the grid
    /// itself is hidden, so table edits and reads are swallowed here.
    pub(super) fn dispatch_table_view_command(
        &mut self,
        cmd: dbflux_app::keymap::Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<bool> {
        use dbflux_app::keymap::Command;

        match (self.table_views.tab, cmd) {
            (TableTab::Data, _) => None,
            (TableTab::Columns, Command::CycleResultView) => {
                let view = self.table_views.profile_view.clone()?;
                view.update(cx, |view, cx| view.cycle_sort(cx));
                Some(true)
            }
            (TableTab::Columns, Command::FocusSearch) => {
                let view = self.table_views.profile_view.clone()?;
                view.update(cx, |view, cx| view.focus_filter(window, cx));
                Some(true)
            }
            (TableTab::Columns, Command::RefreshSchema) => {
                self.load_table_profile(cx);
                Some(true)
            }
            (TableTab::Partitions, Command::RefreshSchema) => {
                self.load_table_partitions(cx);
                Some(true)
            }
            (
                _,
                Command::OpenContextMenu
                | Command::MenuSelect
                | Command::Cancel
                | Command::FocusToolbar
                | Command::ToggleRowInspector,
            ) => None,
            _ => Some(false),
        }
    }

    /// Reads the table's profile from metadata.
    pub(super) fn load_table_profile(&mut self, cx: &mut Context<Self>) {
        let (Some(connection), Some(table)) =
            (self.table_view_connection(cx), self.table_view_target(cx))
        else {
            return;
        };

        self.table_views.profile_generation += 1;
        let generation = self.table_views.profile_generation;

        if self.table_views.profile.is_none()
            && let Some(view) = self.table_views.profile_view.clone()
        {
            view.update(cx, |view, cx| view.set_loading(cx));
        }

        if let Some(view) = self.table_views.profile_view.clone() {
            view.update(cx, |view, cx| {
                view.set_running(Some(ProfileAction::Refresh), cx)
            });
        }

        let task = cx
            .background_executor()
            .spawn(async move { connection.table_profile(&table) });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            let update = this.update(cx, |panel, cx| {
                if panel.table_views.profile_generation != generation {
                    return;
                }

                if let Some(view) = panel.table_views.profile_view.clone() {
                    view.update(cx, |view, cx| view.set_running(None, cx));
                }

                match result {
                    Ok(profile) => panel.apply_table_profile(profile, cx),
                    Err(error) => {
                        let message = error.to_string();

                        match panel.table_views.profile_view.clone() {
                            // Nothing on screen yet: the view says why.
                            Some(view) if panel.table_views.profile.is_none() => {
                                view.update(cx, |view, cx| view.set_failed(message, cx));
                            }
                            // A refresh over a shown profile keeps it and
                            // reports the failure.
                            _ => report_error(
                                UserFacingError::new(
                                    ErrorKind::Driver,
                                    dbflux_i18n::t!(
                                        "document.data.table_views.refresh_failed",
                                        error = message
                                    ),
                                ),
                                cx,
                            ),
                        }
                    }
                }
            });

            if let Err(error) = update {
                log::debug!("data grid released before its column profile arrived: {error}");
            }
        })
        .detach();
    }

    /// Takes a freshly read metadata profile. A projection the user applied
    /// survives when the columns did not change.
    fn apply_table_profile(&mut self, profile: TableProfile, cx: &mut Context<Self>) {
        let profile = Arc::new(profile);

        let projection = match (&self.table_views.projection, &self.table_views.profile) {
            (Some(projection), Some(previous)) if same_columns(previous, &profile) => {
                projection.clone()
            }
            _ => ColumnProjection::all(profile.columns.len()),
        };

        if self
            .table_views
            .profile
            .as_ref()
            .is_some_and(|previous| !same_columns(previous, &profile))
        {
            self.table_views.projection = None;
        }

        self.table_views.profile = Some(profile.clone());
        self.table_views.annotated_for = None;

        if let Some(view) = self.table_views.profile_view.clone() {
            view.update(cx, |view, cx| {
                view.set_profile(profile.clone(), projection.clone(), cx)
            });
        }

        // The picker is built (or refreshed) on the next render, which has a
        // window; see `ensure_table_view_controls`.
        self.table_views.picker = None;
        self.refresh_table_read_estimate(cx);
        cx.notify();
    }

    /// Builds the column picker once a profile is known. It needs the
    /// window, which the profile read does not have.
    pub(super) fn ensure_table_view_controls(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.sync_table_header_annotations(cx);
        self.sync_columns_view_projection(cx);

        if self.table_views.picker.is_some()
            || !self
                .table_views
                .features
                .contains(TableViewFeatures::COLUMN_PROJECTION)
        {
            return;
        }

        let Some(profile) = self.table_views.profile.clone() else {
            return;
        };

        let applied = self
            .table_views
            .projection
            .clone()
            .unwrap_or_else(|| ColumnProjection::all(profile.columns.len()));

        let picker = cx.new(|cx| {
            ColumnProjectionPicker::new("table-column-picker", profile, applied, window, cx)
        });

        self.table_views._subscriptions.push(cx.subscribe_in(
            &picker,
            window,
            |this, _picker, event: &ProjectionChanged, window, cx| {
                this.apply_table_projection(event.0.clone(), window, cx);
            },
        ));
        self.table_views.picker = Some(picker);
    }

    /// Whether the Data filter row shows the column picker: the connection
    /// honours a projection, a profile is known and no builder spec owns the
    /// column list.
    pub(super) fn shows_table_column_picker(&self) -> bool {
        self.table_views.tab == TableTab::Data
            && self.table_views.picker.is_some()
            && self.table_projection_applies()
    }

    /// Whether a projection decides the columns the Data grid reads: the
    /// connection honours one and no builder query chooses its own columns.
    fn table_projection_applies(&self) -> bool {
        self.table_views
            .features
            .contains(TableViewFeatures::COLUMN_PROJECTION)
            && self.builder.visual_select.is_none()
            && self.builder.current_visual_spec.is_none()
    }

    /// Offers the Columns view's column toggles only while a projection
    /// applies, so it never shows a selection the grid is ignoring.
    fn sync_columns_view_projection(&mut self, cx: &mut Context<Self>) {
        let applies = self.table_projection_applies();

        if let Some(view) = self.table_views.profile_view.clone()
            && view.read(cx).projection_enabled() != applies
        {
            view.update(cx, |view, cx| view.set_projection_enabled(applies, cx));
        }
    }

    /// Applies `projection` to the Data grid and reads the page again with
    /// only those columns. The primary key of an editable table stays in, so
    /// edits keep addressing exactly one row; the user is told when it was
    /// added back.
    pub(super) fn apply_table_projection(
        &mut self,
        projection: ColumnProjection,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(profile) = self.table_views.profile.clone() else {
            return;
        };

        if !projection.is_applicable() {
            return;
        }

        let (projection, restored_keys) = restore_keys(&profile, projection, &self.pk_columns);

        if !restored_keys.is_empty() {
            Toast::info(dbflux_i18n::t!(
                "document.data.table_views.projection_keeps_keys",
                columns = restored_keys.join(", ")
            ))
            .meta_right(now_hms())
            .push(cx);
        }

        if self.table_views.projection.as_ref() == Some(&projection) {
            self.sync_projection_controls(&projection, cx);
            return;
        }

        if self.reload_blocked_by_pending_edits(cx) {
            self.sync_projection_controls(
                &self
                    .table_views
                    .projection
                    .clone()
                    .unwrap_or_else(|| ColumnProjection::all(profile.columns.len())),
                cx,
            );
            return;
        }

        let reads_all = projection.selected_count() == projection.total_count();
        self.table_views.projection = (!reads_all).then(|| projection.clone());
        self.sync_projection_controls(&projection, cx);

        self.grid_table.reload = TableReload::NewColumns;
        self.refresh(window, cx);
    }

    /// Adds the primary key columns of an editable table back to the applied
    /// projection, telling the user which ones. Returns whether it changed.
    /// The caller reads the table again when it did.
    pub(super) fn keep_keys_in_table_projection(&mut self, cx: &mut Context<Self>) -> bool {
        let (Some(profile), Some(projection)) = (
            self.table_views.profile.clone(),
            self.table_views.projection.clone(),
        ) else {
            return false;
        };

        let (projection, restored) = restore_keys(&profile, projection, &self.pk_columns);

        if restored.is_empty() {
            return false;
        }

        Toast::info(dbflux_i18n::t!(
            "document.data.table_views.projection_keeps_keys",
            columns = restored.join(", ")
        ))
        .meta_right(now_hms())
        .push(cx);

        let reads_all = projection.selected_count() == projection.total_count();
        self.table_views.projection = (!reads_all).then(|| projection.clone());
        self.sync_projection_controls(&projection, cx);
        self.grid_table.reload = TableReload::NewColumns;
        true
    }

    fn sync_projection_controls(&mut self, projection: &ColumnProjection, cx: &mut Context<Self>) {
        if let Some(picker) = self.table_views.picker.clone() {
            picker.update(cx, |picker, cx| picker.set_applied(projection.clone(), cx));
        }

        if let Some(view) = self.table_views.profile_view.clone() {
            view.update(cx, |view, cx| view.set_applied(projection.clone(), cx));
        }
    }

    /// The columns the next table read asks for: empty to read them all.
    pub(super) fn table_projection_columns(&self) -> Vec<String> {
        let (Some(projection), Some(profile)) =
            (&self.table_views.projection, &self.table_views.profile)
        else {
            return Vec::new();
        };

        if !self
            .table_views
            .features
            .contains(TableViewFeatures::COLUMN_PROJECTION)
        {
            return Vec::new();
        }

        projection
            .selected_indices()
            .into_iter()
            .filter_map(|index| profile.columns.get(index))
            .map(|column| column.name.to_string())
            .collect()
    }

    /// Asks the connection what the browse `request` would read, and shows it
    /// above the grid. Only for connections that estimate reads.
    pub(super) fn estimate_table_read(
        &mut self,
        request: TableBrowseRequest,
        cx: &mut Context<Self>,
    ) {
        if !self
            .table_views
            .features
            .contains(TableViewFeatures::READ_ESTIMATE)
        {
            return;
        }

        self.table_views.last_browse = Some(request.clone());

        let (Some(connection), Some(profile)) = (
            self.table_view_connection(cx),
            self.table_views.profile.clone(),
        ) else {
            return;
        };

        self.table_views.estimate_generation += 1;
        let generation = self.table_views.estimate_generation;

        let task = cx.background_executor().spawn({
            let profile = profile.clone();
            async move { connection.estimate_table_read(&request, &profile) }
        });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            let update = this.update(cx, |panel, cx| {
                if panel.table_views.estimate_generation != generation {
                    return;
                }

                panel.table_views.estimate = match result {
                    Ok(estimate) => Some(ReadEstimateBar::new(&estimate, &profile)),
                    Err(error) => {
                        log::warn!("Estimating the table read failed: {error}");
                        None
                    }
                };
                cx.notify();
            });

            if let Err(error) = update {
                log::debug!("data grid released before its read estimate arrived: {error}");
            }
        })
        .detach();
    }

    /// Estimates the last read the Data grid ran again, now that a profile
    /// is known. Nothing is estimated before the first read.
    fn refresh_table_read_estimate(&mut self, cx: &mut Context<Self>) {
        if let Some(request) = self.table_views.last_browse.clone() {
            self.estimate_table_read(request, cx);
        }
    }

    /// Whether the Data tab shows the read estimate strip.
    pub(super) fn shows_table_read_estimate(&self) -> bool {
        self.table_views.tab == TableTab::Data
            && self.table_views.estimate.is_some()
            && self.builder.visual_select.is_none()
    }

    /// Lists the table's partitions from metadata.
    pub(super) fn load_table_partitions(&mut self, cx: &mut Context<Self>) {
        let (Some(connection), Some(table), Some(view)) = (
            self.table_view_connection(cx),
            self.table_view_target(cx),
            self.table_views.partitions_view.clone(),
        ) else {
            return;
        };

        self.table_views.partitions_requested = true;
        self.table_views.partitions_generation += 1;
        let generation = self.table_views.partitions_generation;
        view.update(cx, |view, cx| view.set_loading(cx));

        let task = cx
            .background_executor()
            .spawn(async move { connection.table_partitions(&table) });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            let update = this.update(cx, |panel, cx| {
                if panel.table_views.partitions_generation != generation {
                    return;
                }

                let Some(view) = panel.table_views.partitions_view.clone() else {
                    return;
                };

                view.update(cx, |view, cx| match result {
                    Ok(partitions) => view.set_partitions(Arc::new(partitions), cx),
                    Err(error) => view.set_failed(error.to_string(), cx),
                });
            });

            if let Err(error) = update {
                log::debug!("data grid released before its partitions arrived: {error}");
            }
        })
        .detach();
    }

    /// Computes distributions from a bounded sample of rows and merges them
    /// into the profile. It reads table data, so it runs only from its button
    /// and leaves an audit event.
    pub(super) fn sample_table_profile(&mut self, cx: &mut Context<Self>) {
        let (Some(connection), Some(table), Some(profile), Some(view)) = (
            self.table_view_connection(cx),
            self.table_view_target(cx),
            self.table_views.profile.clone(),
            self.table_views.profile_view.clone(),
        ) else {
            return;
        };

        let DataSource::Table {
            profile_id,
            database,
            ..
        } = &self.source
        else {
            return;
        };

        let columns: Vec<String> = profile
            .columns
            .iter()
            .map(|column| column.name.to_string())
            .collect();

        view.update(cx, |view, cx| {
            view.set_running(Some(ProfileAction::Sample), cx)
        });

        let audit_service = self.app_state.read(cx).audit_service().clone();
        let driver_id = self
            .app_state
            .read(cx)
            .connections()
            .get(profile_id)
            .map(|connected| connected.profile.driver_id())
            .unwrap_or_default();
        let audit_template = EventRecord::new(
            now_ms(),
            EventSeverity::Info,
            EventCategory::Query,
            EventOutcome::Success,
        )
        .with_connection_context(
            profile_id.to_string(),
            database.clone().unwrap_or_default(),
            driver_id,
        );
        let qualified = table.qualified_name();
        let generation = self.table_views.profile_generation;
        let started_at = Instant::now();

        let task = cx.background_executor().spawn({
            let table = table.clone();
            async move { connection.sample_table_profile(&table, &columns, SAMPLE_PROFILE_ROWS) }
        });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            let event = sample_audit_event(
                audit_template,
                &qualified,
                &result,
                started_at.elapsed().as_millis() as i64,
            );
            if let Err(error) = audit_service.record(event) {
                log::warn!("Failed to emit the sample profile audit event: {error}");
            }

            match result {
                Ok(sample) => {
                    let update = this.update(cx, |panel, cx| {
                        if let Some(view) = panel.table_views.profile_view.clone() {
                            view.update(cx, |view, cx| view.set_running(None, cx));
                        }

                        if panel.table_views.profile_generation != generation {
                            return;
                        }

                        let Some(current) = panel.table_views.profile.clone() else {
                            return;
                        };

                        let mut merged = (*current).clone();
                        merged.merge_sample(&sample);
                        let merged = Arc::new(merged);
                        let applied = panel
                            .table_views
                            .projection
                            .clone()
                            .unwrap_or_else(|| ColumnProjection::all(merged.columns.len()));

                        panel.table_views.profile = Some(merged.clone());
                        panel.table_views.annotated_for = None;

                        if let Some(view) = panel.table_views.profile_view.clone() {
                            view.update(cx, |view, cx| view.set_profile(merged, applied, cx));
                        }

                        cx.notify();
                    });

                    if let Err(error) = update {
                        log::debug!("data grid released before its sample arrived: {error}");
                    }
                }
                Err(error) => {
                    let update = this.update(cx, |panel, cx| {
                        if let Some(view) = panel.table_views.profile_view.clone() {
                            view.update(cx, |view, cx| view.set_running(None, cx));
                        }
                    });

                    if let Err(error) = update {
                        log::debug!("data grid released before its sample failed: {error}");
                    }

                    report_error_async(
                        UserFacingError::new(
                            ErrorKind::Driver,
                            dbflux_i18n::t!(
                                "document.data.table_views.sample_failed",
                                error = error.to_string()
                            ),
                        ),
                        cx,
                    );
                }
            }
        })
        .detach();
    }

    /// Data / Columns / Partitions switch at the right of the header; only
    /// the views the connection offers are shown.
    pub(super) fn render_table_tabs(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let panel = cx.entity().downgrade();

        let items = self
            .table_tabs()
            .into_iter()
            .map(|tab| match tab {
                TableTab::Data => SegmentedItem::new(
                    table_tab_id(tab),
                    dbflux_i18n::t!("document.data.table_views.tab.data"),
                )
                .icon(AppIcon::Table),
                TableTab::Columns => SegmentedItem::new(
                    table_tab_id(tab),
                    dbflux_i18n::t!("document.data.table_views.tab.columns"),
                )
                .icon(AppIcon::Columns),
                TableTab::Partitions => SegmentedItem::new(
                    table_tab_id(tab),
                    dbflux_i18n::t!("document.data.table_views.tab.partitions"),
                )
                .icon(AppIcon::Layers),
            })
            .collect();

        SegmentedControl::new(
            items,
            table_tab_id(self.table_views.tab),
            move |selected, window, cx| {
                let tab = match selected.as_ref() {
                    "table-view-columns" => TableTab::Columns,
                    "table-view-partitions" => TableTab::Partitions,
                    _ => TableTab::Data,
                };

                if let Err(error) = panel.update(cx, |this, cx| this.set_table_tab(tab, window, cx))
                {
                    log::debug!("data grid released before its tab switch: {error}");
                }
            },
        )
    }

    /// The strip under the filter row that says what the next read costs.
    pub(super) fn render_table_read_estimate(&self, cx: &Context<Self>) -> Option<AnyElement> {
        let estimate = self.table_views.estimate.clone()?;

        Some(
            div()
                .id("table-read-estimate")
                .flex_shrink_0()
                .px(TableViewMetrics::FILTER_ROW_PADDING_X)
                .py(TableViewMetrics::FILTER_ROW_GAP)
                .border_b_1()
                .border_color(cx.theme().border)
                .child(estimate)
                .into_any_element(),
        )
    }

    /// The body of the Columns or Partitions tab.
    pub(super) fn render_table_view_body(&self, _cx: &mut Context<Self>) -> AnyElement {
        let view = match self.table_views.tab {
            TableTab::Columns => self
                .table_views
                .profile_view
                .clone()
                .map(IntoElement::into_any_element),
            TableTab::Partitions => self
                .table_views
                .partitions_view
                .clone()
                .map(IntoElement::into_any_element),
            TableTab::Data => None,
        };

        div()
            .id("table-view-body")
            .flex()
            .flex_col()
            .flex_1()
            .min_h_0()
            .overflow_hidden()
            .children(view)
            .into_any_element()
    }

    /// Puts the profile's facts on the second header line of each result
    /// column that matches a profile column by name. Recomputed only when the
    /// result columns or the profile change.
    fn sync_table_header_annotations(&mut self, cx: &mut Context<Self>) {
        let Some(profile) = self.table_views.profile.clone() else {
            return;
        };

        let Some(table_state) = self.grid_table.table_state.clone() else {
            return;
        };

        let names: Vec<String> = self
            .result
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect();
        let key = (self.table_views.profile_generation, names);

        if self.table_views.annotated_for.as_ref() == Some(&key) {
            return;
        }

        let annotations = header_annotations(&profile, &key.1);
        table_state.update(cx, |state, cx| {
            state.set_header_annotations(annotations, cx)
        });
        self.table_views.annotated_for = Some(key);
    }
}

/// The element id of a table tab's segment.
pub(super) fn table_tab_id(tab: TableTab) -> &'static str {
    match tab {
        TableTab::Data => "table-view-data",
        TableTab::Columns => "table-view-columns",
        TableTab::Partitions => "table-view-partitions",
    }
}

/// The second header line of each of `names`: the column's badges, ratio and
/// size on disk, and its share of nulls when the profile knows them.
pub(super) fn header_annotations(
    profile: &TableProfile,
    names: &[String],
) -> Vec<Option<HeaderAnnotation>> {
    names
        .iter()
        .map(|name| {
            profile
                .columns
                .iter()
                .find(|column| column.name.as_ref() == name.as_str())
                .and_then(|column| header_annotation(column, profile.row_count))
        })
        .collect()
}

fn header_annotation(column: &ColumnProfile, row_count: Option<u64>) -> Option<HeaderAnnotation> {
    let mut leading = Vec::new();

    for badge in &column.badges {
        leading.push(match badge {
            ColumnBadge::SortKey {
                position: Some(position),
            } => {
                dbflux_i18n::t!(
                    "document.data.table_views.badge.sort_position",
                    position = position
                )
            }
            ColumnBadge::SortKey { position: None } => {
                dbflux_i18n::t!("document.data.table_views.badge.sort")
            }
            ColumnBadge::PartitionKey => {
                dbflux_i18n::t!("document.data.table_views.badge.partition")
            }
            ColumnBadge::DistributionKey => {
                dbflux_i18n::t!("document.data.table_views.badge.distribution")
            }
        });
    }

    if column.compressed_bytes.is_some() {
        leading.push(format!(
            "{} · {}",
            format_ratio(column.compression_ratio()),
            format_optional_bytes(column.compressed_bytes)
        ));
    }

    let nulls = column.null_fraction(row_count).map(|fraction| {
        dbflux_i18n::t!(
            "document.data.table_views.header_nulls",
            percent = format_percent(Some(fraction))
        )
    });

    if leading.is_empty() && nulls.is_none() {
        return None;
    }

    let annotation = HeaderAnnotation::new(leading.join("  ·  "));

    Some(match nulls {
        Some(nulls) => annotation.trailing(nulls),
        None => annotation,
    })
}

/// `projection` with every column of `keys` the profile knows selected, and
/// the names of the ones that had to be added.
pub(super) fn restore_keys(
    profile: &TableProfile,
    mut projection: ColumnProjection,
    keys: &[String],
) -> (ColumnProjection, Vec<String>) {
    let mut restored = Vec::new();

    for key in keys {
        if let Some(index) = profile
            .columns
            .iter()
            .position(|column| column.name.as_ref() == key.as_str())
            && !projection.is_selected(index)
        {
            projection.toggle(index);
            restored.push(key.clone());
        }
    }

    (projection, restored)
}

/// The row a save returned, laid out in the grid's columns.
///
/// A driver returns the whole table row (`RETURNING *` or a re-read), in the
/// table's column order, while a projected grid shows only some columns.
/// With the table's columns known, each grid column takes its value by name.
/// Otherwise the row is used only when it has exactly the grid's columns.
/// `None` means the row cannot be matched and must not be shown.
pub(super) fn returned_row_for_grid(
    returned: &[dbflux_core::Value],
    grid_columns: &[String],
    table_columns: Option<&[String]>,
) -> Option<Vec<dbflux_core::Value>> {
    if let Some(table_columns) = table_columns
        && table_columns.len() == returned.len()
    {
        return grid_columns
            .iter()
            .map(|name| {
                let index = table_columns.iter().position(|column| column == name)?;
                returned.get(index).cloned()
            })
            .collect();
    }

    (returned.len() == grid_columns.len()).then(|| returned.to_vec())
}

/// Whether two profiles describe the same columns in the same order.
fn same_columns(left: &TableProfile, right: &TableProfile) -> bool {
    left.columns.len() == right.columns.len()
        && left
            .columns
            .iter()
            .zip(&right.columns)
            .all(|(left, right)| left.name == right.name)
}

fn sample_audit_event(
    template: EventRecord,
    table: &str,
    result: &Result<TableProfile, DbError>,
    duration_ms: i64,
) -> EventRecord {
    let mut event = template.with_origin(EventOrigin::local());
    event.ts_ms = now_ms();
    event.duration_ms = Some(duration_ms);

    let mut details = serde_json::Map::new();
    details.insert(
        "source".into(),
        serde_json::Value::from("column_profile_sample"),
    );
    details.insert("table".into(), serde_json::Value::from(table));
    details.insert(
        "row_limit".into(),
        serde_json::Value::from(SAMPLE_PROFILE_ROWS),
    );

    match result {
        Ok(sample) => {
            let rows = match sample.source_label {
                ProfileSource::Sample { rows } => rows,
                _ => 0,
            };

            details.insert("rows_read".into(), serde_json::Value::from(rows));
            event = event
                .with_typed_action(audit_actions::QUERY_EXECUTE)
                .with_summary(format!(
                    "Column sample profiled on {table}: {rows} rows read"
                ));
        }
        Err(error) => {
            event.level = EventSeverity::Error;
            event.outcome = EventOutcome::Failure;
            event.error_message = Some(error.to_string());
            event = event
                .with_typed_action(audit_actions::QUERY_EXECUTE_FAILED)
                .with_summary(format!("Column sample failed on {table}"));
        }
    }

    event.with_details_json(serde_json::Value::Object(details).to_string())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
#[path = "table_views_tests.rs"]
mod tests;
