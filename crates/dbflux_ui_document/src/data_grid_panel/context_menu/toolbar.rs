//! The results toolbar and header buttons, offered from the table's context
//! menu (`m`) so the keyboard reaches every one of them.
//!
//! The menu ends with a Toolbar submenu listing the buttons the grid shows at
//! that moment. Each entry runs exactly what its button runs; the entries a
//! key binding also reaches show that shortcut.

use super::sections::{MenuRowCursor, submenu_flyout, submenu_frame};
use crate::DataViewMode;
use crate::chart::keyboard::step_time_range;
use crate::data_grid_panel::documents::CollectionTab;
use crate::data_grid_panel::documents::builder::BuilderSupport;
use crate::data_grid_panel::{ChartRailTab, DataGridPanel, DataSource};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::chart::AxisPill;
use dbflux_components::common::time_range::state::TimeRange;
use dbflux_components::composites::{MenuItem, menu_row, render_separator};
use dbflux_components::icons::AppIcon;
use dbflux_ui_base::keymap::{chord_display_parts, effective_keymap};
use gpui::prelude::FluentBuilder;
use gpui::*;

/// Width of the Toolbar flyout, sized to its longest label.
const TOOLBAR_SUBMENU_WIDTH: Pixels = px(240.0);

/// A button of the results toolbar or header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ToolbarAction {
    /// The Export button: opens the export menu.
    Export,
    /// The × in the WHERE field.
    ClearFilter,
    /// The table / tree view switch.
    ToggleView,
    /// The Builder button.
    OpenBuilder,
    /// Reset in the "rows come from the builder query" notice.
    ResetBuilder,
    /// Save in the edit bar.
    SaveChanges,
    /// Revert in the edit bar.
    RevertChanges,
    /// The chevron of the Refresh split: the auto-refresh interval.
    AutoRefresh,
    /// Stats in the chart toolbar.
    ToggleStatsRail,
    /// Save chart in the chart toolbar.
    SaveChart,
    /// "Show in tree" in the chart's point inspector: scrolls the table to
    /// the source row of the point under the pointer.
    ShowPointInTable(usize),
    /// The chart kind switch: the kind after the current one.
    NextChartKind,
    /// An axis-bar pill: opens its picker for the keyboard.
    AxisPicker(AxisPill),
    /// The time presets of the chart toolbar: the next or previous one.
    TimeRange { forward: bool },
    /// A control of the custom range row (date range, hour, minute).
    CustomRange(CustomRangeControl),
    /// Apply in the custom range row.
    ApplyCustomRange,
    /// Find in a document collection's query bar.
    Find,
    /// The history button of a document collection's query bar: opens its
    /// menu for the keyboard.
    QueryHistory,
    /// The first breadcrumb segment while stepped into a nested value.
    StepToRoot,
    /// A view tab of a document collection (Documents, Schema, Aggregate).
    CollectionView(CollectionTab),
    /// Reload in the commit conflict panel.
    ConflictReload,
    /// Apply anyway in the commit conflict panel.
    ConflictApply,
    /// Maximize or restore in the embedded panel's header.
    ToggleMaximize,
    /// Hide in the embedded panel's header.
    HidePanel,
}

/// A control of the chart's custom range row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CustomRangeControl {
    DateRange,
    StartHour,
    StartMinute,
    EndHour,
    EndMinute,
}

impl CustomRangeControl {
    const ALL: [CustomRangeControl; 5] = [
        CustomRangeControl::DateRange,
        CustomRangeControl::StartHour,
        CustomRangeControl::StartMinute,
        CustomRangeControl::EndHour,
        CustomRangeControl::EndMinute,
    ];
}

impl ToolbarAction {
    /// Stable, locale-independent id, used for the row's element id.
    pub(crate) fn id(self) -> &'static str {
        match self {
            ToolbarAction::Export => "export",
            ToolbarAction::ClearFilter => "clear-filter",
            ToolbarAction::ToggleView => "toggle-view",
            ToolbarAction::OpenBuilder => "open-builder",
            ToolbarAction::ResetBuilder => "reset-builder",
            ToolbarAction::SaveChanges => "save-changes",
            ToolbarAction::RevertChanges => "revert-changes",
            ToolbarAction::AutoRefresh => "auto-refresh",
            ToolbarAction::ToggleStatsRail => "stats",
            ToolbarAction::SaveChart => "save-chart",
            ToolbarAction::ShowPointInTable(_) => "show-in-tree",
            ToolbarAction::NextChartKind => "next-chart-kind",
            ToolbarAction::AxisPicker(AxisPill::X) => "axis-x",
            ToolbarAction::AxisPicker(AxisPill::Y) => "axis-y",
            ToolbarAction::AxisPicker(AxisPill::Group) => "axis-group",
            ToolbarAction::AxisPicker(AxisPill::Agg) => "axis-agg",
            ToolbarAction::TimeRange { forward: true } => "next-time-range",
            ToolbarAction::TimeRange { forward: false } => "prev-time-range",
            ToolbarAction::CustomRange(CustomRangeControl::DateRange) => "custom-date-range",
            ToolbarAction::CustomRange(CustomRangeControl::StartHour) => "custom-start-hour",
            ToolbarAction::CustomRange(CustomRangeControl::StartMinute) => "custom-start-minute",
            ToolbarAction::CustomRange(CustomRangeControl::EndHour) => "custom-end-hour",
            ToolbarAction::CustomRange(CustomRangeControl::EndMinute) => "custom-end-minute",
            ToolbarAction::ApplyCustomRange => "custom-apply",
            ToolbarAction::Find => "find",
            ToolbarAction::QueryHistory => "query-history",
            ToolbarAction::StepToRoot => "step-to-root",
            ToolbarAction::CollectionView(CollectionTab::Documents) => "view-documents",
            ToolbarAction::CollectionView(CollectionTab::Schema) => "view-schema",
            ToolbarAction::CollectionView(CollectionTab::Aggregate) => "view-aggregate",
            ToolbarAction::ConflictReload => "conflict-reload",
            ToolbarAction::ConflictApply => "conflict-apply",
            ToolbarAction::ToggleMaximize => "maximize",
            ToolbarAction::HidePanel => "hide",
        }
    }

    fn icon(self) -> AppIcon {
        match self {
            ToolbarAction::Export => AppIcon::FileSpreadsheet,
            ToolbarAction::ClearFilter => AppIcon::X,
            ToolbarAction::ToggleView => AppIcon::Braces,
            ToolbarAction::OpenBuilder => AppIcon::ListFilter,
            ToolbarAction::ResetBuilder => AppIcon::RotateCcw,
            ToolbarAction::SaveChanges => AppIcon::Save,
            ToolbarAction::RevertChanges => AppIcon::RotateCcw,
            ToolbarAction::AutoRefresh => AppIcon::Clock,
            ToolbarAction::ToggleStatsRail => AppIcon::Sigma,
            ToolbarAction::SaveChart => AppIcon::Save,
            ToolbarAction::ShowPointInTable(_) => AppIcon::Table,
            ToolbarAction::NextChartKind => AppIcon::ChartSpline,
            ToolbarAction::AxisPicker(_) => AppIcon::Hash,
            ToolbarAction::TimeRange { .. } | ToolbarAction::CustomRange(_) => AppIcon::Clock,
            ToolbarAction::ApplyCustomRange => AppIcon::Check,
            ToolbarAction::Find => AppIcon::Play,
            ToolbarAction::QueryHistory => AppIcon::History,
            ToolbarAction::StepToRoot => AppIcon::ArrowUp,
            ToolbarAction::CollectionView(CollectionTab::Documents) => AppIcon::Table,
            ToolbarAction::CollectionView(CollectionTab::Schema) => AppIcon::Layers,
            ToolbarAction::CollectionView(CollectionTab::Aggregate) => AppIcon::Braces,
            ToolbarAction::ConflictReload => AppIcon::RefreshCcw,
            ToolbarAction::ConflictApply => AppIcon::Check,
            ToolbarAction::ToggleMaximize => AppIcon::Maximize2,
            ToolbarAction::HidePanel => AppIcon::PanelBottomClose,
        }
    }

    /// The command a Results key binding runs for the same button, if any.
    fn command(self) -> Option<Command> {
        match self {
            ToolbarAction::Export => Some(Command::ExportResults),
            ToolbarAction::ClearFilter => Some(Command::ClearFilter),
            ToolbarAction::ToggleView => Some(Command::CycleDocumentView),
            ToolbarAction::CollectionView(_) => Some(Command::NextResultTab),
            _ => None,
        }
    }

    /// The shortcut shown on the row, from the effective keymap.
    fn shortcut(self) -> Option<SharedString> {
        let command = self.command()?;

        effective_keymap()
            .chord_for_command(ContextId::Results, command)
            .map(|chord| chord_display_parts(chord).join(" ").into())
    }
}

impl DataGridPanel {
    /// The toolbar and header buttons the grid shows right now: the ones
    /// with a key binding first (Export, clear filter), then the filter row,
    /// the edit bar, the chart toolbar and the embedded panel's header.
    pub(crate) fn toolbar_actions(&self, cx: &App) -> Vec<ToolbarAction> {
        let mut actions = Vec::new();

        let has_data = !self.result.rows.is_empty()
            || self.result.text_body.is_some()
            || self.result.raw_bytes.is_some();
        let document_collection = self.collection.raw.is_some() || self.is_document_collection(cx);
        let filter_row_shown = matches!(
            self.source,
            DataSource::Table { .. } | DataSource::Collection { .. }
        ) && !document_collection;
        let shows_chart = self.result_view_mode().shows_chart() && self.chart.chart_shell.is_some();

        if has_data {
            actions.push(ToolbarAction::Export);
        }

        if self.filter_is_clearable(cx) {
            actions.push(ToolbarAction::ClearFilter);
        }

        if filter_row_shown && self.builder_notice_visible() {
            actions.push(ToolbarAction::ResetBuilder);
        }

        if filter_row_shown && self.can_toggle_view() {
            actions.push(ToolbarAction::ToggleView);
        }

        if filter_row_shown && !self.builder.builder_open && self.can_open_builder(cx) {
            actions.push(ToolbarAction::OpenBuilder);
        }

        if (filter_row_shown || shows_chart) && self.supports_auto_refresh() {
            actions.push(ToolbarAction::AutoRefresh);
        }

        if document_collection {
            actions.extend(self.document_query_bar_actions(cx));
        }

        if self.has_pending_edits(cx) || self.has_pending_document_commit(cx) {
            actions.push(ToolbarAction::SaveChanges);
            actions.push(ToolbarAction::RevertChanges);
        }

        if shows_chart {
            actions.push(ToolbarAction::ToggleStatsRail);

            if matches!(
                self.source,
                DataSource::Collection { .. } | DataSource::QueryResult { .. }
            ) {
                actions.push(ToolbarAction::SaveChart);
            }

            let hovered_source = self
                .chart
                .chart_shell
                .as_ref()
                .and_then(|shell| shell.read(cx).hovered_data_point(cx))
                .and_then(|point| self.chart_host_source_for_point(point, cx));

            if let Some(source) = hovered_source {
                actions.push(ToolbarAction::ShowPointInTable(source.row_idx));
            }

            actions.push(ToolbarAction::NextChartKind);

            if !self.result.columns.is_empty() {
                actions.extend(
                    [AxisPill::X, AxisPill::Y, AxisPill::Group, AxisPill::Agg]
                        .map(ToolbarAction::AxisPicker),
                );
            }

            if let Some(panel) = &self.chart.chart_source_time_range_panel {
                actions.push(ToolbarAction::TimeRange { forward: true });
                actions.push(ToolbarAction::TimeRange { forward: false });

                if panel.read(cx).selected_time_range == Some(TimeRange::Custom) {
                    actions.extend(CustomRangeControl::ALL.map(ToolbarAction::CustomRange));
                    actions.push(ToolbarAction::ApplyCustomRange);
                }
            }
        }

        if self.chrome.show_panel_controls {
            actions.push(ToolbarAction::ToggleMaximize);
            actions.push(ToolbarAction::HidePanel);
        }

        actions
    }

    /// The query bar and view row of a document collection: Find and the
    /// history in the Documents view, the first breadcrumb while stepped in,
    /// the other views, and the conflict panel's buttons while it shows.
    fn document_query_bar_actions(&self, cx: &App) -> Vec<ToolbarAction> {
        let mut actions = Vec::new();

        if self.document_builder_support(cx) == BuilderSupport::Available {
            actions.push(ToolbarAction::OpenBuilder);
        }

        if self.collection.tab == CollectionTab::Documents {
            actions.push(ToolbarAction::Find);

            if !self.collection.history.is_empty() {
                actions.push(ToolbarAction::QueryHistory);
            }

            if self.is_stepped_into() {
                actions.push(ToolbarAction::StepToRoot);
            }
        }

        actions.extend(
            self.collection_tabs(cx)
                .into_iter()
                .filter(|tab| *tab != self.collection.tab)
                .map(ToolbarAction::CollectionView),
        );

        if self.collection.conflict.is_some() {
            actions.push(ToolbarAction::ConflictReload);
            actions.push(ToolbarAction::ConflictApply);
        }

        actions
    }

    /// Whether a document collection shows Commit and Revert for staged
    /// field edits (or a changed JSON view).
    fn has_pending_document_commit(&self, cx: &App) -> bool {
        self.commits_document_patches(cx)
            && self.pending_document_edit_count(cx) > 0
            && !self.collection.committing
    }

    fn toolbar_action_label(&self, action: ToolbarAction) -> String {
        match action {
            ToolbarAction::Export => {
                dbflux_i18n::t!("document.data.context_menu.toolbar.export")
            }
            ToolbarAction::ClearFilter => {
                dbflux_i18n::t!("document.data.context_menu.toolbar.clear_filter")
            }
            ToolbarAction::ToggleView => match self.view_config.mode {
                DataViewMode::Table => {
                    dbflux_i18n::t!("document.data.grid.toolbar.switch_to_document")
                }
                DataViewMode::Document | DataViewMode::Json => {
                    dbflux_i18n::t!("document.data.grid.toolbar.switch_to_table")
                }
            },
            ToolbarAction::OpenBuilder if self.collection.builder.open => {
                dbflux_i18n::t!("document.collection.builder.close")
            }
            ToolbarAction::OpenBuilder => {
                dbflux_i18n::t!("document.data.context_menu.toolbar.open_builder")
            }
            ToolbarAction::ResetBuilder => {
                dbflux_i18n::t!("document.data.context_menu.toolbar.reset_builder")
            }
            ToolbarAction::SaveChanges => {
                dbflux_i18n::t!("document.data.context_menu.toolbar.save_changes")
            }
            ToolbarAction::RevertChanges => {
                dbflux_i18n::t!("document.data.context_menu.toolbar.revert_changes")
            }
            ToolbarAction::AutoRefresh => {
                dbflux_i18n::t!("document.data.context_menu.toolbar.auto_refresh")
            }
            ToolbarAction::ToggleStatsRail => dbflux_i18n::t!("document.chart.toolbar.stats"),
            ToolbarAction::SaveChart => dbflux_i18n::t!("document.chart.toolbar.save_chart"),
            ToolbarAction::ShowPointInTable(_) => {
                dbflux_i18n::t!("chart.point_inspector.show_in_tree")
            }
            ToolbarAction::NextChartKind => {
                dbflux_i18n::t!("document.chart.pane_actions.next_kind")
            }
            ToolbarAction::AxisPicker(AxisPill::X) => {
                dbflux_i18n::t!("document.chart.pane_actions.x_axis")
            }
            ToolbarAction::AxisPicker(AxisPill::Y) => {
                dbflux_i18n::t!("document.chart.pane_actions.y_axis")
            }
            ToolbarAction::AxisPicker(AxisPill::Group) => {
                dbflux_i18n::t!("document.chart.pane_actions.group_by")
            }
            ToolbarAction::AxisPicker(AxisPill::Agg) => {
                dbflux_i18n::t!("document.chart.pane_actions.aggregation")
            }
            ToolbarAction::TimeRange { forward: true } => {
                dbflux_i18n::t!("document.chart.pane_actions.next_time_range")
            }
            ToolbarAction::TimeRange { forward: false } => {
                dbflux_i18n::t!("document.chart.pane_actions.prev_time_range")
            }
            ToolbarAction::CustomRange(CustomRangeControl::DateRange) => {
                dbflux_i18n::t!("document.chart.pane_actions.date_range")
            }
            ToolbarAction::CustomRange(CustomRangeControl::StartHour) => {
                dbflux_i18n::t!("document.chart.pane_actions.start_hour")
            }
            ToolbarAction::CustomRange(CustomRangeControl::StartMinute) => {
                dbflux_i18n::t!("document.chart.pane_actions.start_minute")
            }
            ToolbarAction::CustomRange(CustomRangeControl::EndHour) => {
                dbflux_i18n::t!("document.chart.pane_actions.end_hour")
            }
            ToolbarAction::CustomRange(CustomRangeControl::EndMinute) => {
                dbflux_i18n::t!("document.chart.pane_actions.end_minute")
            }
            ToolbarAction::ApplyCustomRange => {
                dbflux_i18n::t!("document.data.chart_dock.toolbar.apply")
            }
            ToolbarAction::Find => dbflux_i18n::t!("document.collection.find"),
            ToolbarAction::QueryHistory => {
                dbflux_i18n::t!("document.data.context_menu.toolbar.query_history")
            }
            ToolbarAction::StepToRoot => {
                dbflux_i18n::t!("document.data.context_menu.toolbar.step_to_root")
            }
            ToolbarAction::CollectionView(CollectionTab::Documents) => {
                dbflux_i18n::t!("document.data.context_menu.toolbar.view_documents")
            }
            ToolbarAction::CollectionView(CollectionTab::Schema) => {
                dbflux_i18n::t!("document.data.context_menu.toolbar.view_schema")
            }
            ToolbarAction::CollectionView(CollectionTab::Aggregate) => {
                dbflux_i18n::t!("document.data.context_menu.toolbar.view_aggregate")
            }
            ToolbarAction::ConflictReload => {
                dbflux_i18n::t!("document.collection.conflict.reload")
            }
            ToolbarAction::ConflictApply => {
                dbflux_i18n::t!("document.collection.conflict.apply")
            }
            ToolbarAction::ToggleMaximize if self.chrome.is_maximized => {
                dbflux_i18n::t!("document.data.context_menu.toolbar.restore")
            }
            ToolbarAction::ToggleMaximize => {
                dbflux_i18n::t!("document.data.context_menu.toolbar.maximize")
            }
            ToolbarAction::HidePanel => {
                dbflux_i18n::t!("document.data.context_menu.toolbar.hide")
            }
        }
    }

    /// Whether the WHERE field shows a value its × would clear, or a
    /// document collection's filter slot holds a filter.
    fn filter_is_clearable(&self, cx: &App) -> bool {
        let document_collection = self.collection.raw.is_some() || self.is_document_collection(cx);
        let has_filter = !self.filter_bar.filter_input.read(cx).value().is_empty();

        if document_collection {
            return self.collection.tab == CollectionTab::Documents && has_filter;
        }

        matches!(
            self.source,
            DataSource::Table { .. } | DataSource::Collection { .. }
        ) && self.filter_input_visible()
            && has_filter
    }

    /// Clears the WHERE filter and reloads the rows (`Command::ClearFilter`,
    /// the × in the field); in a document collection, empties the filter
    /// slot and finds. Returns false when there is no filter to clear.
    pub(in crate::data_grid_panel) fn clear_filter(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if !self.filter_is_clearable(cx) {
            return false;
        }

        if self.collection.raw.is_some() || self.is_document_collection(cx) {
            if self.reload_blocked_by_pending_edits(cx) {
                return true;
            }

            self.filter_bar
                .filter_input
                .update(cx, |input, cx| input.set_value("", window, cx));
            self.find_documents(window, cx);
            return true;
        }

        self.replace_filter_and_reload("", window, cx);
        true
    }

    /// Runs what the button behind `action` runs.
    pub(in crate::data_grid_panel) fn run_toolbar_action(
        &mut self,
        action: ToolbarAction,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match action {
            ToolbarAction::Export => self.export_results(window, cx),
            ToolbarAction::ClearFilter => {
                self.clear_filter(window, cx);
            }
            ToolbarAction::ToggleView => self.toggle_view_mode(cx),
            ToolbarAction::OpenBuilder if self.is_document_collection(cx) => {
                self.toggle_document_builder(window, cx)
            }
            ToolbarAction::OpenBuilder => self.open_query_builder(window, cx),
            ToolbarAction::ResetBuilder => {
                self.reset_builder_query(window, cx);
                cx.notify();
            }
            // A document collection commits and reverts through its edit
            // bar's Commit and Revert, which patch fields per document.
            ToolbarAction::SaveChanges if self.commits_document_patches(cx) => {
                self.commit_document_edits(cx)
            }
            ToolbarAction::RevertChanges if self.commits_document_patches(cx) => {
                self.revert_document_edits(window, cx)
            }
            ToolbarAction::SaveChanges => {
                if let Some(table_state) = &self.grid_table.table_state {
                    table_state.update(cx, |state, cx| state.request_save_all(cx));
                }
            }
            ToolbarAction::RevertChanges => {
                if let Some(table_state) = &self.grid_table.table_state {
                    table_state.update(cx, |state, cx| state.revert_all(cx));
                }
            }
            ToolbarAction::AutoRefresh => {
                self.filter_bar
                    .refresh_dropdown
                    .update(cx, |dropdown, cx| dropdown.focus_and_open(window, cx));
            }
            ToolbarAction::ToggleStatsRail => {
                if let Some(shell) = &self.chart.chart_shell {
                    shell.update(cx, |shell, cx| {
                        if shell.chart_rail_open && shell.chart_rail_tab == ChartRailTab::Stats {
                            shell.chart_rail_open = false;
                        } else {
                            shell.chart_rail_open = true;
                            shell.chart_rail_tab = ChartRailTab::Stats;
                        }
                        cx.notify();
                    });
                }
            }
            ToolbarAction::SaveChart => self.open_collection_chart_save(window, cx),
            ToolbarAction::ShowPointInTable(row_idx) => {
                self.chart_host_scroll_to_row(row_idx, window, cx)
            }
            ToolbarAction::NextChartKind => {
                if let Some(shell) = &self.chart.chart_shell {
                    shell.update(cx, |shell, cx| {
                        shell.keyboard_command(Command::NextPanelTab, &[], cx)
                    });
                }
            }
            ToolbarAction::AxisPicker(pill) => {
                let columns = self.result.columns.clone();
                if let Some(shell) = &self.chart.chart_shell {
                    shell.update(cx, |shell, cx| shell.open_axis_picker(pill, &columns, cx));
                }
            }
            ToolbarAction::TimeRange { forward } => {
                if let Some(panel) = &self.chart.chart_source_time_range_panel {
                    step_time_range(panel, if forward { 1 } else { -1 }, cx);
                }
            }
            ToolbarAction::CustomRange(control) => {
                self.focus_custom_range_control(control, window, cx)
            }
            ToolbarAction::ApplyCustomRange => {
                if let Some(panel) = &self.chart.chart_source_time_range_panel {
                    // The bounds are discarded as the Apply button does: the
                    // parent document's TimeRangeChanged subscription re-runs.
                    let applied = panel.update(cx, |panel, cx| panel.apply_custom_range(cx));
                    if let Err(error) = applied {
                        log::debug!("custom range not applied: {error}");
                    }
                }
            }
            ToolbarAction::Find => self.find_documents(window, cx),
            ToolbarAction::QueryHistory => self.open_query_history(window, cx),
            ToolbarAction::StepToRoot => self.step_to_depth(0, cx),
            ToolbarAction::CollectionView(tab) => self.set_collection_tab(tab, cx),
            ToolbarAction::ConflictReload => self.reload_conflicting_document(window, cx),
            ToolbarAction::ConflictApply => self.apply_conflicting_commit(cx),
            ToolbarAction::ToggleMaximize => self.request_toggle_maximize(cx),
            ToolbarAction::HidePanel => self.request_hide(cx),
        }

        cx.notify();
    }

    /// Hands the keyboard to a control of the custom range row: the date
    /// range picker (Enter opens its calendar) or a time list, opened.
    fn focus_custom_range_control(
        &self,
        control: CustomRangeControl,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(panel) = self.chart.chart_source_time_range_panel.clone() else {
            return;
        };

        let panel = panel.read(cx);
        let dropdown = match control {
            CustomRangeControl::DateRange => {
                let handle = panel.custom_date_range_picker.read(cx).focus_handle(cx);
                handle.focus(window, cx);
                return;
            }
            CustomRangeControl::StartHour => panel.custom_start_hour_dropdown.clone(),
            CustomRangeControl::StartMinute => panel.custom_start_minute_dropdown.clone(),
            CustomRangeControl::EndHour => panel.custom_end_hour_dropdown.clone(),
            CustomRangeControl::EndMinute => panel.custom_end_minute_dropdown.clone(),
        };

        dropdown.update(cx, |dropdown, cx| dropdown.focus_and_open(window, cx));
    }

    /// Closes the table's menu, hands the keyboard back to the grid and runs
    /// `action`, which may move the keyboard on (the export menu, a dropdown).
    pub(super) fn run_toolbar_action_from_menu(
        &mut self,
        action: ToolbarAction,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let is_document_view = self
            .context_menu
            .take()
            .is_some_and(|menu| menu.is_document_view);

        self.restore_focus_after_context_menu(is_document_view, window, cx);
        self.run_toolbar_action(action, window, cx);
    }

    /// Renders the separator and the Toolbar trigger at the end of the table's
    /// menu and, while it is open, the flyout listing `actions`.
    pub(super) fn render_toolbar_submenu_section(
        &self,
        menu: &super::TableContextMenu,
        actions: &[ToolbarAction],
        submenus_open_left: bool,
        cursor: MenuRowCursor<'_>,
        cx: &mut Context<Self>,
    ) {
        if actions.is_empty() {
            return;
        }

        let MenuRowCursor {
            rows: menu_items,
            visual_index,
            selected_index,
        } = cursor;

        menu_items.push(render_separator(cx).into_any_element());
        *visual_index += 1;

        let toolbar_index = *visual_index;
        let submenu_open = menu.toolbar_submenu_open;
        let submenu_selected_index = menu.submenu_selected_index;

        let trigger = MenuItem::new(dbflux_i18n::t!("document.data.context_menu.toolbar.title"))
            .icon(AppIcon::Settings)
            .submenu();

        let flyout = submenu_open.then(|| {
            let rows: Vec<AnyElement> = actions
                .iter()
                .enumerate()
                .map(|(index, &action)| {
                    self.render_toolbar_action_row(
                        index,
                        action,
                        index == submenu_selected_index,
                        cx,
                    )
                })
                .collect();

            submenu_flyout(TOOLBAR_SUBMENU_WIDTH, cx).children(rows)
        });

        menu_items.push(
            menu_row(
                "toolbar-trigger",
                &trigger,
                selected_index == toolbar_index || submenu_open,
                cx,
            )
            .on_mouse_move(cx.listener(move |this, _, _, cx| {
                // Hovering opens the submenu, as native menus do; the flyout
                // is a child of this row, so moving into it keeps bubbling
                // here and the guard leaves it open.
                if let Some(ref mut menu) = this.context_menu
                    && !(menu.selected_index == toolbar_index && menu.toolbar_submenu_open)
                {
                    menu.selected_index = toolbar_index;
                    menu.close_submenus();
                    menu.toolbar_submenu_open = true;
                    menu.submenu_selected_index = 0;
                    cx.notify();
                }
            }))
            .on_click(cx.listener(|this, _, _, cx| {
                if let Some(ref mut menu) = this.context_menu {
                    let open = !menu.toolbar_submenu_open;
                    menu.close_submenus();
                    menu.toolbar_submenu_open = open;
                    menu.submenu_selected_index = 0;
                    cx.notify();
                }
            }))
            .when_some(flyout, |row: Stateful<Div>, flyout| {
                row.child(submenu_frame(submenus_open_left, flyout))
            })
            .into_any_element(),
        );
        *visual_index += 1;
    }

    fn render_toolbar_action_row(
        &self,
        index: usize,
        action: ToolbarAction,
        selected: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let mut item = MenuItem::new(self.toolbar_action_label(action)).icon(action.icon());
        if let Some(shortcut) = action.shortcut() {
            item = item.shortcut(shortcut);
        }

        menu_row(
            SharedString::from(format!("toolbar-action-{}", action.id())),
            &item,
            selected,
            cx,
        )
        .on_mouse_move(cx.listener(move |this, _, _, cx| {
            if let Some(ref mut menu) = this.context_menu
                && menu.submenu_selected_index != index
            {
                menu.submenu_selected_index = index;
                cx.notify();
            }
        }))
        .on_click(cx.listener(move |this, _, window, cx| {
            this.run_toolbar_action_from_menu(action, window, cx);
        }))
        .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    // Explicit imports rather than a glob: combining one with `#[gpui::test]`
    // sends the macro expansion into unbounded recursion.
    use crate::data_grid_panel::{DataGridPanel, DataSource};
    use crate::keyboard_test_support::{KeymapHost, host_document, init_keyboard_runtime};
    use dbflux_app::keymap::{Command, ContextId};
    use dbflux_core::{ColumnKind, ColumnMeta, Pagination, QueryResult, TableRef, Value};
    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_ui_base::AppStateEntity;
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
    use std::time::Duration;
    use uuid::Uuid;

    /// A three-row table under the app keymap, with the WHERE filter
    /// `filter` typed in and the table focused.
    fn host_filtered_table<'a>(
        cx: &'a mut TestAppContext,
        filter: &'static str,
    ) -> (
        Entity<KeymapHost<DataGridPanel>>,
        Entity<DataGridPanel>,
        &'a mut VisualTestContext,
    ) {
        init_keyboard_runtime(cx);

        let app_state = cx.update(|cx| {
            cx.new(|_| {
                AppStateEntity::new_with_storage_runtime(
                    StorageRuntime::in_memory().expect("isolated storage runtime"),
                )
                .expect("test storage setup")
            })
        });

        let (host, window) = host_document(
            cx,
            move |window, cx| {
                cx.new(|cx| {
                    let source = DataSource::Table {
                        profile_id: Uuid::nil(),
                        database: Some("app".to_string()),
                        table: TableRef::with_schema("public", "users"),
                        pagination: Pagination::default(),
                        order_by: Vec::new(),
                        total_rows: Some(3),
                    };

                    let mut panel = DataGridPanel::new_internal(
                        source,
                        app_state.clone(),
                        vec!["id".to_string()],
                        window,
                        cx,
                    );
                    panel.set_result(
                        QueryResult::table(
                            vec![ColumnMeta {
                                name: "id".to_string(),
                                type_name: "int4".to_string(),
                                kind: ColumnKind::Integer,
                                nullable: false,
                                is_primary_key: true,
                            }],
                            (1..=3).map(|id| vec![Value::Int(id)]).collect(),
                            None,
                            Duration::ZERO,
                        ),
                        cx,
                    );
                    panel.filter_bar.filter_input.update(cx, |input, cx| {
                        input.set_value(filter, window, cx);
                    });
                    panel
                })
            },
            |panel, cx| panel.active_context(cx),
            DataGridPanel::dispatch_command,
        );
        let panel = window.update(|_, cx| host.read(cx).document.clone());

        window.update(|window, cx| {
            let table_state = panel
                .read(cx)
                .grid_table
                .table_state
                .clone()
                .expect("the result builds a table");
            let focus_handle = table_state.read(cx).focus_handle().clone();
            focus_handle.focus(window, cx);
        });
        window.run_until_parked();

        (host, panel, window)
    }

    fn filter_text(panel: &Entity<DataGridPanel>, window: &mut VisualTestContext) -> String {
        window.update(|_, cx| {
            panel
                .read(cx)
                .filter_bar
                .filter_input
                .read(cx)
                .value()
                .to_string()
        })
    }

    /// The last row of the table's menu opens the Toolbar submenu, which
    /// runs the toolbar's buttons: Export opens the export menu.
    #[gpui::test]
    fn the_table_menu_ends_with_the_toolbar_and_runs_its_export(cx: &mut TestAppContext) {
        let (_host, panel, window) = host_filtered_table(cx, "id = 1");

        // Up from the first row wraps to the Toolbar trigger; `l` opens it.
        for keys in ["m", "k", "l", "enter"] {
            window.simulate_keystrokes(keys);
            window.run_until_parked();
        }

        assert!(
            window.update(|_, cx| panel.read(cx).chrome.export_menu_open),
            "the first toolbar entry opens the export menu"
        );
        assert!(
            window.update(|_, cx| panel.read(cx).context_menu.is_none()),
            "running an entry closes the table's menu"
        );
        assert_eq!(
            window.update(|_, cx| panel.read(cx).active_context(cx)),
            ContextId::ContextMenu,
            "the export menu holds the keyboard"
        );
    }

    /// Shift+F clears the WHERE filter, and the Toolbar submenu offers the
    /// same thing next to Export.
    #[gpui::test]
    fn shift_f_and_the_toolbar_entry_clear_the_filter(cx: &mut TestAppContext) {
        let (host, panel, window) = host_filtered_table(cx, "id = 1");

        window.simulate_keystrokes("shift-f");
        window.run_until_parked();

        assert!(
            window
                .update(|_, cx| host.read(cx).commands.clone())
                .contains(&Command::ClearFilter),
            "Shift+F reaches the grid as the clear-filter command"
        );
        assert_eq!(filter_text(&panel, window), "", "Shift+F clears the filter");

        window.update(|window, cx| {
            panel.update(cx, |panel, cx| {
                panel.filter_bar.filter_input.update(cx, |input, cx| {
                    input.set_value("id = 2", window, cx);
                });
            });
        });
        window.run_until_parked();

        for keys in ["m", "k", "l", "j", "enter"] {
            window.simulate_keystrokes(keys);
            window.run_until_parked();
        }

        assert_eq!(
            filter_text(&panel, window),
            "",
            "the entry after Export clears the filter"
        );
    }

    #[test]
    fn toolbar_labels_resolve_in_every_locale() {
        let keys = [
            "title",
            "export",
            "clear_filter",
            "open_builder",
            "reset_builder",
            "save_changes",
            "revert_changes",
            "auto_refresh",
            "maximize",
            "restore",
            "hide",
            "query_history",
            "step_to_root",
            "view_documents",
            "view_schema",
            "view_aggregate",
        ];

        for key in keys {
            let key = format!("document.data.context_menu.toolbar.{key}");

            for locale in ["en", "es", "ko", "zh_Hans"] {
                let value = dbflux_i18n::t!(&key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} missing from {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale}"
                );
            }
        }
    }

    /// A table of two numeric series over a time column, shown as its chart
    /// only, under the app keymap with the table focused.
    fn host_chart(
        cx: &mut TestAppContext,
    ) -> (
        Entity<KeymapHost<DataGridPanel>>,
        Entity<DataGridPanel>,
        &mut VisualTestContext,
    ) {
        host_chart_for(
            cx,
            DataSource::Table {
                profile_id: Uuid::nil(),
                database: Some("app".to_string()),
                table: TableRef::with_schema("public", "samples"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: Some(3),
            },
        )
    }

    /// The chart of `host_chart`, read from `source`.
    fn host_chart_for(
        cx: &mut TestAppContext,
        source: DataSource,
    ) -> (
        Entity<KeymapHost<DataGridPanel>>,
        Entity<DataGridPanel>,
        &mut VisualTestContext,
    ) {
        init_keyboard_runtime(cx);

        let app_state = cx.update(|cx| {
            cx.new(|_| {
                AppStateEntity::new_with_storage_runtime(
                    StorageRuntime::in_memory().expect("isolated storage runtime"),
                )
                .expect("test storage setup")
            })
        });

        let column = |name: &str, kind: ColumnKind| ColumnMeta {
            name: name.to_string(),
            type_name: String::new(),
            kind,
            nullable: true,
            is_primary_key: false,
        };

        let (host, window) = host_document(
            cx,
            move |window, cx| {
                cx.new(|cx| {
                    let mut panel = DataGridPanel::new_internal(
                        source,
                        app_state.clone(),
                        Vec::new(),
                        window,
                        cx,
                    );
                    panel.set_result(
                        QueryResult::table(
                            vec![
                                column("ts", ColumnKind::Timestamp),
                                column("a", ColumnKind::Float),
                                column("b", ColumnKind::Float),
                            ],
                            [(0, 1.0, 10.0), (1_000, 2.0, 20.0), (2_000, 3.0, 15.0)]
                                .into_iter()
                                .map(|(ts, a, b)| {
                                    vec![Value::Int(ts), Value::Float(a), Value::Float(b)]
                                })
                                .collect(),
                            None,
                            Duration::ZERO,
                        ),
                        cx,
                    );
                    panel.set_result_view_mode(
                        dbflux_components::result_view::ResultViewMode::Chart,
                        cx,
                    );
                    panel
                })
            },
            |panel, cx| panel.active_context(cx),
            DataGridPanel::dispatch_command,
        );
        let panel = window.update(|_, cx| host.read(cx).document.clone());

        // The chart view draws no table, so the keyboard sits on the grid.
        window.update(|window, cx| {
            let focus_handle = panel.read(cx).focus_handle.clone();
            focus_handle.focus(window, cx);
        });
        window.run_until_parked();

        (host, panel, window)
    }

    /// In the chart view of a result, H and L move a highlighted point that
    /// the chart reports as the hovered one, which is what the point
    /// inspector and its "Show in tree" entry follow, and J moves it to the
    /// other series.
    #[gpui::test]
    fn chart_view_keys_move_the_hovered_point(cx: &mut TestAppContext) {
        let (_host, panel, window) = host_chart(cx);

        let hovered = |window: &mut VisualTestContext| {
            window.update(|_, cx| {
                let shell = panel.read(cx).chart.chart_shell.clone()?;
                let point = shell.read(cx).hovered_data_point(cx)?;
                Some((point.series_idx, point.point_idx_in_series))
            })
        };
        assert_eq!(hovered(window), None);

        window.simulate_keystrokes("l l");
        assert_eq!(hovered(window), Some((0, 1)));

        window.simulate_keystrokes("j");
        assert_eq!(hovered(window), Some((1, 1)));

        window.simulate_keystrokes("escape");
        assert_eq!(hovered(window), None);
    }

    /// Keeps the latest frame drawn in the window it observes.
    #[derive(Default)]
    struct FrameCapture(std::sync::Mutex<Option<gpui::AccessibilityFrame>>);

    impl gpui::FrameObserver for FrameCapture {
        fn accessibility_updated(&self, frame: &gpui::AccessibilityFrame) {
            *self.0.lock().expect("frame capture lock") = Some(frame.clone());
        }
    }

    impl FrameCapture {
        fn bounds_of(&self, id: &str) -> Option<gpui::Bounds<gpui::Pixels>> {
            let frame = self.0.lock().expect("frame capture lock").clone()?;
            frame
                .nodes()
                .find(|(_, node)| node.id() == id)
                .map(|(_, node)| node.bounds())
        }
    }

    fn capture_frames(window: &mut VisualTestContext) -> std::sync::Arc<FrameCapture> {
        let capture = std::sync::Arc::new(FrameCapture::default());
        window.update(|window, _| {
            window.observe_frames(&capture);
            window.refresh();
        });
        window.run_until_parked();
        capture
    }

    /// The chart of a table tracks the row behind each point: a point picked
    /// with the keyboard opens the point inspector, and its "Show in tree"
    /// leaves the chart for the table with that row selected.
    #[gpui::test]
    fn a_table_chart_inspects_the_keyboard_point_and_shows_its_row(cx: &mut TestAppContext) {
        use super::ToolbarAction;
        use dbflux_components::result_view::ResultViewMode;

        let (_host, panel, window) = host_chart(cx);
        let frames = capture_frames(window);

        window.simulate_keystrokes("l l");
        window.update(|window, _| window.refresh());
        window.run_until_parked();

        let actions = window.update(|_, cx| panel.read(cx).toolbar_actions(cx));
        assert!(
            actions.contains(&ToolbarAction::ShowPointInTable(1)),
            "{actions:?}"
        );

        let show_in_tree = frames
            .bounds_of("inspector-action-show-in-tree-row-1")
            .expect("the point inspector shows the row behind the point");
        window.simulate_click(show_in_tree.center(), gpui::Modifiers::default());
        window.run_until_parked();

        let (mode, active_row) = window.update(|_, cx| {
            let panel = panel.read(cx);
            let active_row = panel
                .grid_table
                .table_state
                .as_ref()
                .and_then(|state| state.read(cx).selection().active)
                .map(|cell| cell.row);
            (panel.chrome.result_view_mode, active_row)
        });
        assert_eq!(mode, ResultViewMode::Table);
        assert_eq!(active_row, Some(1));
    }

    /// A chart of a query result keeps no row per point, so a keyboard point
    /// opens no point inspector and offers no "Show in tree".
    #[gpui::test]
    fn a_query_result_chart_opens_no_point_inspector(cx: &mut TestAppContext) {
        use super::ToolbarAction;

        let (_host, panel, window) = host_chart_for(
            cx,
            DataSource::QueryResult {
                result: std::sync::Arc::new(QueryResult::empty()),
                original_query: "SELECT * FROM samples".to_string(),
                profile_id: None,
            },
        );
        let frames = capture_frames(window);

        window.simulate_keystrokes("l");
        window.update(|window, _| window.refresh());
        window.run_until_parked();

        let (hovered, actions) = window.update(|_, cx| {
            let panel = panel.read(cx);
            let hovered = panel
                .chart
                .chart_shell
                .as_ref()
                .and_then(|shell| shell.read(cx).hovered_data_point(cx));
            (hovered.is_some(), panel.toolbar_actions(cx))
        });
        assert!(hovered, "the keyboard point is on the chart");
        assert!(
            !actions
                .iter()
                .any(|action| matches!(action, ToolbarAction::ShowPointInTable(_))),
            "{actions:?}"
        );
        assert!(
            frames
                .bounds_of("inspector-action-show-in-tree-row-1")
                .is_none()
        );
    }

    /// The Toolbar submenu of a chart view offers the chart kind and the
    /// axis pickers; an axis picker it opens is driven by J and Enter.
    #[gpui::test]
    fn the_chart_toolbar_entries_open_a_keyboard_axis_picker(cx: &mut TestAppContext) {
        use super::ToolbarAction;
        use dbflux_components::chart::{AxisPill, ChartKind};

        let (_host, panel, window) = host_chart(cx);

        let actions = window.update(|_, cx| panel.read(cx).toolbar_actions(cx));
        for action in [
            ToolbarAction::NextChartKind,
            ToolbarAction::AxisPicker(AxisPill::X),
            ToolbarAction::AxisPicker(AxisPill::Y),
        ] {
            assert!(actions.contains(&action), "{action:?} in {actions:?}");
        }

        let shell = window
            .update(|_, cx| panel.read(cx).chart.chart_shell.clone())
            .expect("a chartable result has a chart");

        window.update(|window, cx| {
            panel.update(cx, |panel, cx| {
                panel.run_toolbar_action(ToolbarAction::NextChartKind, window, cx);
                panel.run_toolbar_action(ToolbarAction::AxisPicker(AxisPill::X), window, cx);
            })
        });
        window.run_until_parked();
        assert_eq!(
            window.update(|_, cx| shell.read(cx).chart_kind()),
            ChartKind::Bar
        );

        window.simulate_keystrokes("j enter");
        assert_eq!(
            window.update(|_, cx| {
                let shell = shell.read(cx);
                (shell.axis_open_pill, shell.active_bindings().x)
            }),
            (None, 1),
            "J then Enter binds column `a` to X and closes the picker"
        );
    }

    /// The entries a key binding reaches come first, in the grid's order.
    #[gpui::test]
    fn a_filtered_table_offers_export_then_clear_filter(cx: &mut TestAppContext) {
        use super::ToolbarAction;

        let (_host, panel, window) = host_filtered_table(cx, "id = 1");

        let actions = window.update(|_, cx| panel.read(cx).toolbar_actions(cx));
        assert_eq!(
            actions.get(..2),
            Some([ToolbarAction::Export, ToolbarAction::ClearFilter].as_slice())
        );
    }
}
