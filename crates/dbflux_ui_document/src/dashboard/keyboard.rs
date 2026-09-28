//! Keyboard control of a dashboard.
//!
//! The dashboard reports the `Dashboard` key context and answers its keymap
//! commands here:
//!
//! - hjkl and the arrows select a panel (H / L in reading order, J / K the
//!   nearest panel on the next row), G and Shift+G the first and last one.
//!   Panels folded away under a collapsed divider are skipped.
//! - Enter or I open the selected panel: a chart panel then takes the chart
//!   keys and a table panel (an instance inspector) the table keys, until
//!   Escape brings the keyboard back to the dashboard. On a divider it folds
//!   or unfolds its section, as Space does.
//! - C configures the panel, R or F2 renames it, X or Delete removes it,
//!   A adds a panel, Shift+hjkl moves it and Alt+Shift+hjkl resizes it (Edit
//!   mode), Alt+H / Alt+L switch View and Edit, ] and [ step the shared time
//!   range, F5 refreshes, M lists all of it.
//! - While the Configure popover is open, H and L open the neighboring axis
//!   picker, J and K move through it, Enter or Space pick, Alt+H / Alt+L
//!   switch the chart type, Enter without a picker applies and Escape closes
//!   the picker and then the popover.

use super::{DASHBOARD_GRID_COLUMNS, DashboardDocument, DashboardPanelSlot, GridRect};
use crate::chart::keyboard::{ChartKeyOutcome, step_time_range, time_range_pane_actions};
use crate::pane::PaneAction;
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::chart::AxisPill;
use dbflux_components::icons::AppIcon;
use dbflux_ui_base::toast::Toast;
use gpui::{App, Context, Entity, Window};

/// Largest panel height, in grid rows, a resize accepts.
const MAX_PANEL_ROWS: u32 = 12;

impl DashboardDocument {
    /// The key context of the dashboard: the entered panel's while one is
    /// open, otherwise the dashboard's own.
    pub fn active_context(&self, cx: &App) -> ContextId {
        match self
            .entered_panel
            .and_then(|index| self.panel_slots.get(index as usize))
        {
            Some(DashboardPanelSlot::Loaded { panel, .. }) => panel.read(cx).active_context(),
            Some(DashboardPanelSlot::Inspector { entity, .. }) => {
                entity.read(cx).active_context(cx)
            }
            _ => ContextId::Dashboard,
        }
    }

    /// Runs a keymap command. See the module documentation for the keys.
    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if self.pending_configure_panel_index.is_some() {
            return self.configure_popover_command(cmd, cx);
        }

        if let Command::Cancel = cmd {
            if self.panel_context_menu.is_some() {
                self.close_panel_context_menu(cx);
                return true;
            } else if self.editing_dashboard_name {
                self.cancel_dashboard_name_edit(cx);
                return true;
            } else if self.editing_title_panel_index.is_some() {
                self.cancel_panel_title_edit(cx);
                return true;
            }
        }

        if self.entered_panel.is_some() {
            return self.entered_panel_command(cmd, window, cx);
        }

        match cmd {
            Command::ColumnLeft => self.select_in_reading_order(-1, cx),
            Command::ColumnRight => self.select_in_reading_order(1, cx),
            Command::SelectNext => self.select_on_next_row(true, cx),
            Command::SelectPrev => self.select_on_next_row(false, cx),
            Command::SelectFirst => self.select_edge(false, cx),
            Command::SelectLast => self.select_edge(true, cx),
            Command::Execute => self.open_selected_panel(window, cx),
            Command::ExpandCollapse => self.fold_selected_divider(cx),
            Command::ConfigurePanel => self.configure_selected_panel(cx),
            Command::Rename => self.rename_selected_panel(window, cx),
            Command::Delete => self.remove_selected_panel(cx),
            Command::AddItem if !self.read_only => {
                self.request_add_panel(cx);
                true
            }
            Command::MovePanelLeft => self.move_selected_panel(-1, 0, cx),
            Command::MovePanelRight => self.move_selected_panel(1, 0, cx),
            Command::MovePanelUp => self.move_selected_panel(0, -1, cx),
            Command::MovePanelDown => self.move_selected_panel(0, 1, cx),
            Command::ResizePanelNarrower => self.resize_selected_panel(-1, 0, cx),
            Command::ResizePanelWider => self.resize_selected_panel(1, 0, cx),
            Command::ResizePanelShorter => self.resize_selected_panel(0, -1, cx),
            Command::ResizePanelTaller => self.resize_selected_panel(0, 1, cx),
            Command::NextPanelTab | Command::PrevPanelTab if !self.read_only => {
                self.toggle_mode(cx);
                true
            }
            _ => self.shared_command(cmd, cx),
        }
    }

    /// Commands the dashboard answers whether or not a panel is open: the
    /// shared time range and the refresh.
    fn shared_command(&mut self, cmd: Command, cx: &mut Context<Self>) -> bool {
        match cmd {
            Command::NextTimeRange | Command::PrevTimeRange => {
                let delta = if cmd == Command::NextTimeRange { 1 } else { -1 };
                let panel = self.shared_time_range.clone();
                step_time_range(&panel, delta, cx);
                true
            }
            Command::RefreshSchema => {
                match self.entered_panel {
                    Some(index) => self.request_reexec_for_slot(index as usize, cx),
                    None => self.refresh_all_loaded_panels(cx),
                }
                true
            }
            _ => false,
        }
    }

    /// Hands `cmd` to the open panel; Escape it does not use brings the
    /// keyboard back to the dashboard.
    fn entered_panel_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let handled = match self
            .entered_panel
            .and_then(|index| self.panel_slots.get(index as usize))
        {
            Some(DashboardPanelSlot::Loaded { panel, .. }) => {
                let panel = panel.clone();
                panel.update(cx, |chart, cx| chart.dispatch_command(cmd, window, cx))
            }
            Some(DashboardPanelSlot::Inspector { entity, .. }) => {
                let entity = entity.clone();
                entity.update(cx, |inspector, cx| {
                    inspector.dispatch_command(cmd, window, cx)
                })
            }
            _ => {
                self.entered_panel = None;
                false
            }
        };

        if handled {
            return true;
        }

        if cmd == Command::Cancel {
            self.leave_panel(window, cx);
            return true;
        }

        self.shared_command(cmd, cx)
    }

    /// Opens the selected panel: a chart or table panel takes the keyboard,
    /// a divider folds or unfolds its section.
    fn open_selected_panel(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        let Some(index) = self.focused_panel_index else {
            return self.select_edge(false, cx);
        };

        match self.panel_slots.get(index as usize) {
            Some(DashboardPanelSlot::Loaded { panel, .. }) => {
                let panel = panel.clone();
                panel.update(cx, |chart, cx| chart.focus(window, cx));
            }
            Some(DashboardPanelSlot::Inspector { entity, .. }) => {
                let entity = entity.clone();
                entity.update(cx, |inspector, cx| inspector.focus(window, cx));
            }
            Some(DashboardPanelSlot::Divider { .. }) => return self.fold_selected_divider(cx),
            Some(DashboardPanelSlot::Orphan { .. }) | None => return false,
        }

        self.entered_panel = Some(index);
        cx.notify();
        true
    }

    /// Gives the keyboard back to the dashboard from the open panel.
    pub(crate) fn leave_panel(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.entered_panel = None;
        self.focus_handle.focus(window, cx);
        cx.notify();
    }

    /// The panel the keyboard is inside, if any.
    pub fn entered_panel(&self) -> Option<u32> {
        self.entered_panel
    }

    // ---- selection ----

    /// The panels the keyboard can select, with their on-screen `(row,
    /// column)` once folded sections have closed up, in reading order.
    fn selectable_panels(&self) -> Vec<(u32, u32, u32)> {
        let (hidden, row_shifts) = self.collapse_view();

        let mut panels: Vec<(u32, u32, u32)> = self
            .panel_slots
            .iter()
            .enumerate()
            .filter(|(index, _)| !hidden.contains(index))
            .map(|(index, slot)| {
                let pos = slot.grid_pos();
                let shift = row_shifts.get(index).copied().unwrap_or(0);
                (
                    index as u32,
                    pos.grid_row.saturating_sub(shift),
                    pos.grid_column,
                )
            })
            .collect();

        panels.sort_by_key(|(_, row, column)| (*row, *column));
        panels
    }

    fn select(&mut self, index: u32, cx: &mut Context<Self>) -> bool {
        self.focused_panel_index = Some(index);
        cx.notify();
        true
    }

    /// Selects the panel `delta` places away in reading order, wrapping.
    /// Without a selection the first key selects the first (or last) panel.
    fn select_in_reading_order(&mut self, delta: isize, cx: &mut Context<Self>) -> bool {
        let panels = self.selectable_panels();
        if panels.is_empty() {
            return false;
        }

        let current = self
            .focused_panel_index
            .and_then(|focused| panels.iter().position(|(index, ..)| *index == focused));

        let next = match current {
            Some(position) => {
                (position as isize + delta).rem_euclid(panels.len() as isize) as usize
            }
            None if delta < 0 => panels.len() - 1,
            None => 0,
        };

        self.select(panels[next].0, cx)
    }

    /// Selects the panel on the nearest row below (or above) the selected
    /// one, preferring the closest column. Stays put at the last row.
    fn select_on_next_row(&mut self, down: bool, cx: &mut Context<Self>) -> bool {
        let panels = self.selectable_panels();

        let Some(&(_, row, column)) = self
            .focused_panel_index
            .and_then(|focused| panels.iter().find(|(index, ..)| *index == focused))
        else {
            return self.select_edge(!down, cx);
        };

        let target_row = panels
            .iter()
            .map(|(_, candidate_row, _)| *candidate_row)
            .filter(|candidate_row| {
                if down {
                    *candidate_row > row
                } else {
                    *candidate_row < row
                }
            })
            .reduce(|a, b| if down { a.min(b) } else { a.max(b) });

        let Some(target_row) = target_row else {
            return true;
        };

        let nearest = panels
            .iter()
            .filter(|(_, candidate_row, _)| *candidate_row == target_row)
            .min_by_key(|(_, _, candidate_column)| candidate_column.abs_diff(column))
            .map(|(index, ..)| *index);

        match nearest {
            Some(index) => self.select(index, cx),
            None => true,
        }
    }

    fn select_edge(&mut self, last: bool, cx: &mut Context<Self>) -> bool {
        let panels = self.selectable_panels();
        let edge = if last { panels.last() } else { panels.first() };

        match edge {
            Some(&(index, ..)) => self.select(index, cx),
            None => false,
        }
    }

    // ---- panel actions ----

    fn selected_slot(&self) -> Option<(u32, &DashboardPanelSlot)> {
        let index = self.focused_panel_index?;
        Some((index, self.panel_slots.get(index as usize)?))
    }

    fn fold_selected_divider(&mut self, cx: &mut Context<Self>) -> bool {
        match self.selected_slot() {
            Some((index, DashboardPanelSlot::Divider { .. })) => {
                self.toggle_divider_collapse(index, cx);
                true
            }
            _ => false,
        }
    }

    fn configure_selected_panel(&mut self, cx: &mut Context<Self>) -> bool {
        match self.selected_slot() {
            Some((index, DashboardPanelSlot::Loaded { .. })) => {
                self.start_configure_panel(index as usize, cx);
                true
            }
            _ => false,
        }
    }

    fn rename_selected_panel(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        if self.read_only {
            return false;
        }

        match self.selected_slot() {
            Some((index, DashboardPanelSlot::Loaded { .. })) => {
                self.start_panel_title_edit(index, window, cx);
                true
            }
            _ => false,
        }
    }

    fn remove_selected_panel(&mut self, cx: &mut Context<Self>) -> bool {
        if self.read_only {
            return false;
        }

        let Some((index, _)) = self.selected_slot() else {
            return false;
        };

        self.remove_panel(index, cx);
        self.focused_panel_index = None;
        true
    }

    /// Moves the selected panel `columns` / `rows` grid cells, in Edit mode,
    /// as dragging it does: a move off the grid or onto another panel is
    /// refused with the same notice.
    fn move_selected_panel(&mut self, columns: i32, rows: i32, cx: &mut Context<Self>) -> bool {
        self.change_selected_panel(cx, |pos| {
            let column = pos.column.checked_add_signed(columns)?;
            let row = pos.row.checked_add_signed(rows)?;

            (column + pos.width <= DASHBOARD_GRID_COLUMNS).then_some(GridRect {
                column,
                row,
                ..pos
            })
        })
    }

    /// Resizes the selected panel by `columns` / `rows` grid cells, in Edit
    /// mode, as dragging its edge does.
    fn resize_selected_panel(&mut self, columns: i32, rows: i32, cx: &mut Context<Self>) -> bool {
        self.change_selected_panel(cx, |pos| {
            let width = pos.width.checked_add_signed(columns)?;
            let height = pos.height.checked_add_signed(rows)?;

            (width >= 1
                && (1..=MAX_PANEL_ROWS).contains(&height)
                && pos.column + width <= DASHBOARD_GRID_COLUMNS)
                .then_some(GridRect {
                    width,
                    height,
                    ..pos
                })
        })
    }

    /// Applies `change` to the selected panel's rectangle and commits it when
    /// the result stays on the grid and clear of the other panels. Only in
    /// Edit mode, where the pointer can move and resize panels too.
    fn change_selected_panel(
        &mut self,
        cx: &mut Context<Self>,
        change: impl FnOnce(GridRect) -> Option<GridRect>,
    ) -> bool {
        if !self.is_edit_mode() {
            return false;
        }

        let Some((index, slot)) = self.selected_slot() else {
            return false;
        };

        let pos = slot.grid_pos();
        let current = GridRect {
            column: pos.grid_column,
            row: pos.grid_row,
            width: pos.grid_width,
            height: pos.grid_height,
        };

        let Some(proposed) = change(current) else {
            return true;
        };

        if self.collides_with_other_panels(index as usize, &proposed) {
            Toast::info(dbflux_i18n::t!("document.dashboard.toast.position_overlap")).push(cx);
            return true;
        }

        self.commit_panel_position(index as usize, proposed, cx);
        true
    }

    // ---- configure popover ----

    /// Keys of the open Configure popover: the axis pickers and chart type
    /// of the panel it configures, Enter to apply, Escape to close.
    pub(super) fn configure_popover_command(
        &mut self,
        cmd: Command,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(index) = self.pending_configure_panel_index else {
            return false;
        };
        let Some(DashboardPanelSlot::Loaded { panel, .. }) = self.panel_slots.get(index) else {
            if cmd == Command::Cancel {
                self.close_configure_panel(cx);
            }
            return true;
        };
        let panel = panel.clone();

        let picker_open = panel.read(cx).axis_open_pill(cx).is_some();

        match cmd {
            Command::Cancel if !picker_open => {
                self.close_configure_panel(cx);
                true
            }
            Command::Execute if !picker_open => {
                self.configure_apply_and_persist(index, cx);
                true
            }
            Command::ColumnLeft | Command::ColumnRight if !picker_open => {
                let pill = if cmd == Command::ColumnRight {
                    AxisPill::X
                } else {
                    AxisPill::Agg
                };
                panel.update(cx, |chart, cx| chart.open_axis_picker(pill, cx));
                true
            }
            Command::NextPanelTab
            | Command::PrevPanelTab
            | Command::ColumnLeft
            | Command::ColumnRight
            | Command::SelectNext
            | Command::SelectPrev
            | Command::SelectFirst
            | Command::SelectLast
            | Command::Execute
            | Command::ExpandCollapse
            | Command::Cancel => {
                let outcome = panel.update(cx, |chart, cx| chart.chart_key(cmd, cx));
                if outcome != ChartKeyOutcome::Unhandled {
                    cx.notify();
                }
                true
            }
            // The popover owns the keyboard: nothing behind it answers.
            _ => true,
        }
    }

    // ---- pane actions ----

    /// The dashboard's toolbar, panel menu and keys, for the pane-actions
    /// menu. While a chart panel is open, its own chart actions.
    pub(crate) fn pane_actions(&self, entity: &Entity<Self>, cx: &App) -> Vec<PaneAction> {
        if let Some(DashboardPanelSlot::Loaded { panel, .. }) = self
            .entered_panel
            .and_then(|index| self.panel_slots.get(index as usize))
        {
            return panel.read(cx).embedded_pane_actions(cx);
        }

        let context = ContextId::Dashboard;
        let mut actions = self.selected_panel_actions(context);

        if !self.read_only {
            actions.push(
                PaneAction::command(
                    "dashboard-add-panel",
                    dbflux_i18n::t!("document.dashboard.toolbar.add_panel"),
                    Command::AddItem,
                    context,
                )
                .icon(AppIcon::Plus),
            );
        }

        actions.push(
            PaneAction::command(
                "dashboard-refresh",
                dbflux_i18n::t!("document.chart.pane_actions.refresh"),
                Command::RefreshSchema,
                context,
            )
            .icon(AppIcon::RefreshCcw),
        );

        let refresh_dropdown = self.refresh_dropdown.clone();
        actions.push(
            PaneAction::callback(
                "dashboard-auto-refresh",
                dbflux_i18n::t!("document.chart.pane_actions.auto_refresh"),
                move |window, cx| {
                    refresh_dropdown.update(cx, |dropdown, cx| dropdown.focus_and_open(window, cx));
                },
            )
            .icon(AppIcon::Clock),
        );

        let time_range = self.shared_time_range.clone();
        actions.extend(time_range_pane_actions(
            &self.shared_time_range,
            "dashboard",
            context,
            move |_window, cx| {
                time_range.update(cx, |panel, cx| {
                    if let Err(error) = panel.apply_custom_range(cx) {
                        log::debug!("dashboard custom range not applied: {error}");
                    }
                });
            },
            cx,
        ));

        if self.read_only {
            let entity = entity.clone();
            actions.push(
                PaneAction::callback(
                    "dashboard-save-as-editable",
                    dbflux_i18n::t!("document.dashboard.toolbar.save_as_editable"),
                    move |_window, cx| {
                        entity.update(cx, |this, cx| this.request_save_as_editable(cx));
                    },
                )
                .icon(AppIcon::Save),
            );
        } else {
            let label = if self.is_edit_mode() {
                dbflux_i18n::t!("document.dashboard.pane_actions.switch_to_view")
            } else {
                dbflux_i18n::t!("document.dashboard.pane_actions.switch_to_edit")
            };
            actions.push(
                PaneAction::command("dashboard-mode", label, Command::NextPanelTab, context).icon(
                    if self.is_edit_mode() {
                        AppIcon::Eye
                    } else {
                        AppIcon::Pencil
                    },
                ),
            );
        }

        actions
    }

    /// The actions of the selected panel, as its menu and keys offer them.
    fn selected_panel_actions(&self, context: ContextId) -> Vec<PaneAction> {
        let Some((_, slot)) = self.selected_slot() else {
            return Vec::new();
        };

        let mut actions = Vec::new();
        let command = |id: &str, label_key: &str, command: Command| {
            PaneAction::command(
                format!("dashboard-panel-{id}"),
                dbflux_i18n::t!(label_key),
                command,
                context,
            )
        };

        match slot {
            DashboardPanelSlot::Loaded { .. } | DashboardPanelSlot::Inspector { .. } => {
                actions.push(command(
                    "open",
                    "document.dashboard.pane_actions.open_panel",
                    Command::Execute,
                ));
            }
            DashboardPanelSlot::Divider { .. } => {
                actions.push(command(
                    "fold",
                    "document.dashboard.pane_actions.fold_section",
                    Command::ExpandCollapse,
                ));
            }
            DashboardPanelSlot::Orphan { .. } => {}
        }

        if matches!(slot, DashboardPanelSlot::Loaded { .. }) {
            actions.push(
                command(
                    "configure",
                    "document.dashboard.panel.menu.configure",
                    Command::ConfigurePanel,
                )
                .icon(AppIcon::Settings),
            );

            if !self.read_only {
                actions.push(
                    command(
                        "rename",
                        "document.dashboard.panel.menu.edit_title",
                        Command::Rename,
                    )
                    .icon(AppIcon::Pencil),
                );
            }
        }

        if self.read_only {
            return actions;
        }

        actions.push(
            command(
                "remove",
                "document.dashboard.panel.menu.remove",
                Command::Delete,
            )
            .icon(AppIcon::Delete),
        );

        if self.is_edit_mode() {
            for (id, label_key, move_command) in [
                (
                    "move-left",
                    "settings.keybindings.command.move_panel_left",
                    Command::MovePanelLeft,
                ),
                (
                    "move-right",
                    "settings.keybindings.command.move_panel_right",
                    Command::MovePanelRight,
                ),
                (
                    "move-up",
                    "settings.keybindings.command.move_panel_up",
                    Command::MovePanelUp,
                ),
                (
                    "move-down",
                    "settings.keybindings.command.move_panel_down",
                    Command::MovePanelDown,
                ),
                (
                    "narrower",
                    "settings.keybindings.command.resize_panel_narrower",
                    Command::ResizePanelNarrower,
                ),
                (
                    "wider",
                    "settings.keybindings.command.resize_panel_wider",
                    Command::ResizePanelWider,
                ),
                (
                    "shorter",
                    "settings.keybindings.command.resize_panel_shorter",
                    Command::ResizePanelShorter,
                ),
                (
                    "taller",
                    "settings.keybindings.command.resize_panel_taller",
                    Command::ResizePanelTaller,
                ),
            ] {
                actions.push(command(id, label_key, move_command));
            }
        }

        actions
    }
}
