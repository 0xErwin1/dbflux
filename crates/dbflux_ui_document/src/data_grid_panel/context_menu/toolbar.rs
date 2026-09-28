//! The results toolbar and header buttons, offered from the table's context
//! menu (`m`) so the keyboard reaches every one of them.
//!
//! The menu ends with a Toolbar submenu listing the buttons the grid shows at
//! that moment. Each entry runs exactly what its button runs; the entries a
//! key binding also reaches show that shortcut.

use super::sections::{MenuRowCursor, submenu_flyout, submenu_frame};
use crate::DataViewMode;
use crate::data_grid_panel::{ChartRailTab, DataGridPanel, DataSource};
use dbflux_app::keymap::{Command, ContextId};
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
    /// Maximize or restore in the embedded panel's header.
    ToggleMaximize,
    /// Hide in the embedded panel's header.
    HidePanel,
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

        if self.has_pending_edits(cx) {
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
        }

        if self.chrome.show_panel_controls {
            actions.push(ToolbarAction::ToggleMaximize);
            actions.push(ToolbarAction::HidePanel);
        }

        actions
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

    /// Whether the WHERE field shows a value its × would clear.
    fn filter_is_clearable(&self, cx: &App) -> bool {
        let document_collection = self.collection.raw.is_some() || self.is_document_collection(cx);

        matches!(
            self.source,
            DataSource::Table { .. } | DataSource::Collection { .. }
        ) && !document_collection
            && self.filter_input_visible()
            && !self.filter_bar.filter_input.read(cx).value().is_empty()
    }

    /// Clears the WHERE filter and reloads the rows (`Command::ClearFilter`,
    /// the × in the field). Returns false when there is no filter to clear.
    pub(in crate::data_grid_panel) fn clear_filter(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if !self.filter_is_clearable(cx) {
            return false;
        }

        self.replace_filter_and_reload("", window, cx);
        true
    }

    /// Runs what the button behind `action` runs.
    pub(super) fn run_toolbar_action(
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
            ToolbarAction::OpenBuilder => self.open_query_builder(window, cx),
            ToolbarAction::ResetBuilder => {
                self.reset_builder_query(window, cx);
                cx.notify();
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
            ToolbarAction::ToggleMaximize => self.request_toggle_maximize(cx),
            ToolbarAction::HidePanel => self.request_hide(cx),
        }

        cx.notify();
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
