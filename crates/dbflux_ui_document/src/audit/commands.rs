//! Command dispatch and keyboard navigation for `AuditDocument`.
//!
//! Row cursor movement, context menu lifecycle, and the main
//! `dispatch_command` entry point live here so that `mod.rs` can focus
//! on document construction, data loading, and filter state.

use super::{AuditContextMenuAction, AuditDocument, AuditMenuItem, DEFAULT_PAGE_SIZE};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::icons::AppIcon;
use dbflux_storage::repositories::audit::AuditEventDto;
use gpui::prelude::*;
use gpui::*;

impl AuditDocument {
    /// Returns the active `ContextId` for keyboard dispatch.
    ///
    /// Priority (highest first):
    /// - `ContextMenu` — while the context menu is open
    /// - `TextInput`   — while the search input has keyboard focus (Editing)
    /// - `Audit`       — row list or toolbar focus-ring navigation
    pub fn active_context(&self) -> ContextId {
        if self.context_menu.is_some() || self.export_menu_open {
            return ContextId::ContextMenu;
        }

        if self.filter_bar.is_editing() {
            return ContextId::TextInput;
        }

        ContextId::Audit
    }

    // ── Row cursor navigation ─────────────────────────────────────────────

    #[allow(dead_code)]
    fn row_count(&self) -> usize {
        self.events.len()
    }

    pub(super) fn select_row(&mut self, row: usize, cx: &mut Context<Self>) {
        if self.events.is_empty() {
            return;
        }

        let row = row.min(self.events.len().saturating_sub(1));
        self.selected_row = Some(row);
        cx.notify();
    }

    fn select_next_row(&mut self, cx: &mut Context<Self>) {
        let next = match self.selected_row {
            None => 0,
            Some(r) => (r + 1).min(self.events.len().saturating_sub(1)),
        };
        self.selected_row = Some(next);
        cx.notify();
    }

    fn select_prev_row(&mut self, cx: &mut Context<Self>) {
        let prev = match self.selected_row {
            None => 0,
            Some(0) => 0,
            Some(r) => r - 1,
        };
        self.selected_row = Some(prev);
        cx.notify();
    }

    fn select_first_row(&mut self, cx: &mut Context<Self>) {
        if !self.events.is_empty() {
            self.selected_row = Some(0);
            cx.notify();
        }
    }

    fn select_last_row(&mut self, cx: &mut Context<Self>) {
        if !self.events.is_empty() {
            self.selected_row = Some(self.events.len() - 1);
            cx.notify();
        }
    }

    /// Jump down by a partial page (same feel as Ctrl+D in Results).
    fn page_down_rows(&mut self, cx: &mut Context<Self>) {
        let step = (DEFAULT_PAGE_SIZE / 4) as usize;
        let next = match self.selected_row {
            None => step.min(self.events.len().saturating_sub(1)),
            Some(r) => (r + step).min(self.events.len().saturating_sub(1)),
        };
        self.selected_row = Some(next);
        cx.notify();
    }

    /// Jump up by a partial page.
    fn page_up_rows(&mut self, cx: &mut Context<Self>) {
        let step = (DEFAULT_PAGE_SIZE / 4) as usize;
        let prev = match self.selected_row {
            None => 0,
            Some(r) => r.saturating_sub(step),
        };
        self.selected_row = Some(prev);
        cx.notify();
    }

    /// Toggle expand/collapse for the selected row (Execute / Space).
    fn toggle_selected_row_expanded(&mut self, cx: &mut Context<Self>) {
        if let Some(row) = self.selected_row
            && let Some(event) = self.events.get(row)
        {
            self.toggle_event_expanded(event.id, cx);
        }
    }

    // ── Context menu ──────────────────────────────────────────────────────

    /// Static menu item table — separators have `action: None`. The row
    /// menu offers what the row's detail offers: the copies, filtering by
    /// its correlation id, and opening a pending approval.
    pub(super) fn context_menu_items(
        has_correlation: bool,
        can_open_approval: bool,
    ) -> Vec<AuditMenuItem> {
        let mut items = vec![
            AuditMenuItem::item(
                dbflux_i18n::t!("document.audit.menu.copy_row_as_csv"),
                AuditContextMenuAction::CopyRowAsCsv,
                AppIcon::Layers,
            ),
            AuditMenuItem::item(
                dbflux_i18n::t!("document.audit.menu.copy_summary"),
                AuditContextMenuAction::CopySummary,
                AppIcon::Layers,
            ),
            AuditMenuItem::item(
                dbflux_i18n::t!("document.audit.action.copy_json"),
                AuditContextMenuAction::CopyJson,
                AppIcon::Copy,
            ),
        ];

        if has_correlation {
            items.push(AuditMenuItem::separator());
            items.push(AuditMenuItem::item(
                dbflux_i18n::t!("document.audit.menu.filter_by_correlation"),
                AuditContextMenuAction::FilterByCorrelation,
                AppIcon::ListFilter,
            ));
        }

        if can_open_approval {
            items.push(AuditMenuItem::separator());
            items.push(AuditMenuItem::item(
                dbflux_i18n::t!("document.audit.action.open_approval"),
                AuditContextMenuAction::OpenApproval,
                AppIcon::Bot,
            ));
        }

        items
    }

    /// The menu items for the row `row`, or none when it no longer exists.
    pub(super) fn menu_items_for_row(&self, row: usize) -> Vec<AuditMenuItem> {
        let Some(event) = self.events.get(row) else {
            return Vec::new();
        };

        let has_correlation = event
            .correlation_id
            .as_deref()
            .is_some_and(|correlation| !correlation.is_empty());
        let can_open_approval = cfg!(feature = "mcp") && Self::is_pending_approval(event);

        Self::context_menu_items(has_correlation, can_open_approval)
    }

    /// Runs a row menu action on `event`, from a key or a click.
    pub(super) fn run_menu_action(
        &mut self,
        action: AuditContextMenuAction,
        event: AuditEventDto,
        cx: &mut Context<Self>,
    ) {
        match action {
            AuditContextMenuAction::CopyRowAsCsv => {
                let csv = Self::event_to_csv_row(&event);
                cx.write_to_clipboard(ClipboardItem::new_string(csv));
            }
            AuditContextMenuAction::CopySummary => {
                let summary = event.summary.clone().unwrap_or_default();
                cx.write_to_clipboard(ClipboardItem::new_string(summary));
            }
            AuditContextMenuAction::CopyJson => match serde_json::to_string_pretty(&event) {
                Ok(json) => cx.write_to_clipboard(ClipboardItem::new_string(json)),
                Err(error) => log::warn!("audit event could not be serialized: {error}"),
            },
            AuditContextMenuAction::FilterByCorrelation => {
                if let Some(correlation_id) = event.correlation_id.filter(|c| !c.is_empty()) {
                    self.filter_by_correlation(correlation_id, cx);
                }
            }
            AuditContextMenuAction::OpenApproval => {
                cx.emit(super::super::handle::DocumentEvent::RequestOpenApprovals);
            }
        }
    }

    pub(super) fn open_context_menu_at_selection(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(row) = self.selected_row else {
            return;
        };

        if row >= self.events.len() {
            return;
        }

        // Keyboard-triggered: approximate position from row index.
        const AUDIT_ROW_HEIGHT: f32 = 30.0;
        let y = row as f32 * AUDIT_ROW_HEIGHT + AUDIT_ROW_HEIGHT;
        let position = Point::new(px(8.0), px(y)); // guardrail-allow: positional offset, not a spacing/layout token

        self.context_menu = Some(super::AuditContextMenuState {
            row,
            selected_index: 0,
            position,
        });
        // Keep focus on the document's own handle so on_key_down continues
        // to receive events while the context menu is open.
        self.focus_handle.focus(window, cx);
        cx.notify();
    }

    pub(super) fn open_context_menu_at_mouse(
        &mut self,
        row: usize,
        mouse_position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if row >= self.events.len() {
            return;
        }

        self.select_row(row, cx);

        // Convert from window-absolute coordinates to panel-local coordinates,
        // exactly as DataGridPanel does: `menu_x = position.x - panel_origin.x`.
        let local_position = Point::new(
            mouse_position.x - self.panel_origin.x,
            mouse_position.y - self.panel_origin.y,
        );

        self.context_menu = Some(super::AuditContextMenuState {
            row,
            selected_index: 0,
            position: local_position,
        });
        self.focus_handle.focus(window, cx);
        cx.notify();
    }

    pub(super) fn close_context_menu(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.context_menu.is_some() {
            self.context_menu = None;
            self.focus_handle.focus(window, cx);
            cx.notify();
        }
    }

    fn navigate_menu_down(&mut self, cx: &mut Context<Self>) {
        let Some(row) = self.context_menu.as_ref().map(|menu| menu.row) else {
            return;
        };
        let items = self.menu_items_for_row(row);
        let Some(ref mut menu) = self.context_menu else {
            return;
        };

        let navigable: Vec<usize> = items
            .iter()
            .enumerate()
            .filter(|(_, i)| !i.is_separator())
            .map(|(idx, _)| idx)
            .collect();

        if navigable.is_empty() {
            return;
        }

        let current_pos = navigable
            .iter()
            .position(|&idx| idx == menu.selected_index)
            .unwrap_or(0);

        let next_pos = (current_pos + 1) % navigable.len();
        menu.selected_index = navigable[next_pos];
        cx.notify();
    }

    fn navigate_menu_up(&mut self, cx: &mut Context<Self>) {
        let Some(row) = self.context_menu.as_ref().map(|menu| menu.row) else {
            return;
        };
        let items = self.menu_items_for_row(row);
        let Some(ref mut menu) = self.context_menu else {
            return;
        };

        let navigable: Vec<usize> = items
            .iter()
            .enumerate()
            .filter(|(_, i)| !i.is_separator())
            .map(|(idx, _)| idx)
            .collect();

        if navigable.is_empty() {
            return;
        }

        let current_pos = navigable
            .iter()
            .position(|&idx| idx == menu.selected_index)
            .unwrap_or(0);

        let prev_pos = if current_pos == 0 {
            navigable.len() - 1
        } else {
            current_pos - 1
        };

        menu.selected_index = navigable[prev_pos];
        cx.notify();
    }

    fn execute_selected_menu_item(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(menu) = self.context_menu.clone() else {
            return;
        };

        self.run_menu_item_at(menu.row, menu.selected_index, window, cx);
    }

    /// Closes the row menu and runs its item at `index`, if that item still
    /// is an action for the row `row`.
    pub(super) fn run_menu_item_at(
        &mut self,
        row: usize,
        index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let action = self
            .menu_items_for_row(row)
            .get(index)
            .and_then(|item| item.action);
        let Some(action) = action else {
            return;
        };
        let Some(event) = self.events.get(row).cloned() else {
            return;
        };

        self.close_context_menu(window, cx);
        self.run_menu_action(action, event, cx);
    }

    /// Moves the export menu highlight, wrapping at either end.
    fn step_export_menu(&mut self, forward: bool, cx: &mut Context<Self>) {
        let count = Self::EXPORT_FORMATS.len();
        self.export_menu_selected = if forward {
            (self.export_menu_selected + 1) % count
        } else {
            (self.export_menu_selected + count - 1) % count
        };
        cx.notify();
    }

    /// The keys of the open export menu: the menu keys move and pick a
    /// format, Escape or the export shortcut close it.
    fn dispatch_export_menu_command(&mut self, cmd: Command, cx: &mut Context<Self>) -> bool {
        match cmd {
            Command::MenuDown | Command::SelectNext => self.step_export_menu(true, cx),
            Command::MenuUp | Command::SelectPrev => self.step_export_menu(false, cx),
            Command::MenuSelect | Command::Execute => {
                let format = Self::EXPORT_FORMATS[self.export_menu_selected];
                self.export_with_format(format, cx);
            }
            Command::MenuBack | Command::Cancel | Command::ExportResults => {
                self.export_menu_open = false;
                cx.notify();
            }
            _ => return false,
        }

        true
    }

    /// Left and Right on the time presets move the selected preset; at
    /// either end they leave the presets for the neighbouring toolbar item.
    /// Returns whether the step stayed inside the presets.
    fn step_time_preset(&mut self, step: isize, cx: &mut Context<Self>) -> bool {
        if self.toolbar_index(ToolbarSlot::Time) != Some(self.filter_bar.focused_index()) {
            return false;
        }

        let Some(current) = self
            .selected_time_range
            .map(crate::chrome::time_preset_index)
        else {
            self.select_time_preset(0, cx);
            return true;
        };

        match current.checked_add_signed(step).filter(|next| *next <= 5) {
            Some(next) => {
                self.select_time_preset(next, cx);
                true
            }
            None => false,
        }
    }

    /// Execute the button action for the currently focused FilterBar item.
    /// Only called when `activate_input` returned `false` (Button variant).
    pub(super) fn execute_filter_bar_button(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let focused_index = self.filter_bar.focused_index();

        if self.toolbar_index(ToolbarSlot::Time) == Some(focused_index) {
            self.cycle_time_preset(cx);
        } else if self.toolbar_index(ToolbarSlot::Refresh) == Some(focused_index) {
            self.refresh(cx);
            self.filter_bar.deactivate();
            self.focus_handle.focus(window, cx);
        } else if self.toolbar_index(ToolbarSlot::Clear) == Some(focused_index) {
            self.clear_filters(window, cx);
            self.filter_bar.deactivate();
            self.focus_handle.focus(window, cx);
        } else if self.toolbar_index(ToolbarSlot::CustomApply) == Some(focused_index) {
            if self.can_apply_custom_time_range(cx) {
                self.apply_custom_time_range(cx);
            }
        } else if self.toolbar_index(ToolbarSlot::Level) == Some(focused_index) {
            self.multi_select_level
                .update(cx, |ms, cx| ms.toggle_open(cx));
        } else if self.toolbar_index(ToolbarSlot::Category) == Some(focused_index) {
            self.multi_select_category
                .update(cx, |ms, cx| ms.toggle_open(cx));
        } else if self.toolbar_index(ToolbarSlot::Outcome) == Some(focused_index) {
            self.multi_select_outcome
                .update(cx, |ms, cx| ms.toggle_open(cx));
        }
    }

    /// Dispatches a keyboard command to the document.
    ///
    /// Called by the workspace on every key event when this document is active.
    /// Returns `true` if the command was consumed, `false` if it should fall
    /// through to the workspace.
    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        // While the context menu is open, all commands go to the menu.
        if self.context_menu.is_some() {
            return self.dispatch_menu_command(cmd, window, cx);
        }

        if self.export_menu_open {
            return self.dispatch_export_menu_command(cmd, cx);
        }

        // ── Open dropdown in toolbar ──────────────────────────────────────
        // When the focused filter bar item is a Dropdown and it is open,
        // route navigation commands directly to that dropdown. This mirrors
        // how other list-based overlays (context menu, command palette) own
        // the keyboard while they are visible.
        if let Some(entity) = self.filter_bar.focused_dropdown_entity()
            && entity.read(cx).is_open()
        {
            return match cmd {
                Command::SelectNext => {
                    entity.update(cx, |d, cx| d.select_next_item(cx));
                    true
                }
                Command::SelectPrev => {
                    entity.update(cx, |d, cx| d.select_prev_item(cx));
                    true
                }
                Command::Execute => {
                    entity.update(cx, |d, cx| d.accept_selection(cx));
                    true
                }
                Command::Cancel => {
                    entity.update(cx, |d, cx| d.close(cx));
                    true
                }
                // Consume everything else so the list doesn't react while the
                // dropdown is open.
                _ => true,
            };
        }

        // ── Toolbar mode (Navigating or Editing) ─────────────────────────
        // This block mirrors the `if self.focus_mode == GridFocusMode::Toolbar`
        // block in DataGridPanel. When the filter bar is active:
        //   - Navigation commands (h/l, ←/→) move the ring between items.
        //   - Enter activates the focused item.
        //   - Escape / FocusUp exits toolbar and returns to the list.
        //   - All list commands (j/k, g/G, etc.) are consumed without effect
        //     so the list does not move while the toolbar is focused.
        if self.filter_bar.is_active() {
            if self.filter_bar.is_editing() {
                // The input has GPUI focus; only Cancel/Escape is intercepted
                // here to exit editing mode. Everything else goes to the input.
                if cmd == Command::Cancel {
                    self.filter_bar.exit_editing();
                    self.focus_handle.focus(window, cx);
                    cx.notify();
                    return true;
                }
                return false;
            }

            // Navigating mode: ring is visible, no input has GPUI focus.
            return match cmd {
                Command::ColumnLeft | Command::FocusLeft => {
                    if !self.step_time_preset(-1, cx) {
                        self.filter_bar.move_left();
                    }
                    cx.notify();
                    true
                }
                Command::ColumnRight | Command::FocusRight => {
                    if !self.step_time_preset(1, cx) {
                        self.filter_bar.move_right();
                    }
                    cx.notify();
                    true
                }
                Command::Execute => {
                    let activated = self.filter_bar.activate_input(window, cx);
                    if !activated {
                        // Button item: execute the action for this index.
                        self.execute_filter_bar_button(window, cx);
                    }
                    cx.notify();
                    true
                }
                Command::Cancel | Command::FocusUp => {
                    self.filter_bar.deactivate();
                    self.focus_handle.focus(window, cx);
                    cx.notify();
                    true
                }
                // Consume all other list-navigation commands so the list
                // does not respond while the toolbar ring is active.
                _ => true,
            };
        }

        // ── List mode ────────────────────────────────────────────────────
        match cmd {
            Command::SelectNext => {
                self.select_next_row(cx);
                true
            }
            Command::SelectPrev => {
                self.select_prev_row(cx);
                true
            }
            Command::SelectFirst => {
                self.select_first_row(cx);
                true
            }
            Command::SelectLast => {
                self.select_last_row(cx);
                true
            }
            Command::PageDown => {
                self.page_down_rows(cx);
                true
            }
            Command::PageUp => {
                self.page_up_rows(cx);
                true
            }
            Command::ResultsNextPage => {
                self.go_to_next_page(cx);
                true
            }
            Command::ResultsPrevPage => {
                self.go_to_prev_page(cx);
                true
            }
            Command::ExpandCollapse | Command::Execute => {
                self.toggle_selected_row_expanded(cx);
                true
            }
            Command::OpenContextMenu => {
                self.open_context_menu_at_selection(window, cx);
                true
            }
            Command::RefreshSchema => {
                self.refresh(cx);
                true
            }
            Command::ExportResults => {
                self.toggle_export_menu(cx);
                true
            }
            // Alt+L / Alt+H switch the view of the internal audit log
            // between the event table and the chart.
            Command::NextPanelTab | Command::PrevPanelTab if !self.is_external_event_stream() => {
                let to_chart = matches!(self.view_mode, super::chart_view::AuditViewMode::Table);
                self.set_view_mode(to_chart, cx);
                true
            }
            Command::FocusToolbar | Command::FocusSearch => {
                self.filter_bar.enter(0);
                cx.notify();
                true
            }
            _ => false,
        }
    }

    fn dispatch_menu_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match cmd {
            Command::MenuDown | Command::SelectNext => {
                self.navigate_menu_down(cx);
                true
            }
            Command::MenuUp | Command::SelectPrev => {
                self.navigate_menu_up(cx);
                true
            }
            Command::MenuSelect | Command::Execute => {
                self.execute_selected_menu_item(window, cx);
                true
            }
            Command::MenuBack | Command::Cancel => {
                self.close_context_menu(window, cx);
                true
            }
            _ => false,
        }
    }
}

// Suppress unused import warnings from items used only in commands.rs's
// `use super::` that come from mod.rs private items.
use super::ToolbarSlot;

#[cfg(test)]
mod tests {
    use super::{AuditContextMenuAction, AuditDocument, AuditMenuItem};

    const MENU_KEYS: &[&str] = &[
        "document.audit.menu.copy_row_as_csv",
        "document.audit.menu.copy_summary",
        "document.audit.menu.export",
        "document.audit.menu.export_format.csv",
        "document.audit.menu.export_format.json",
        "document.audit.menu.filter_by_correlation",
    ];

    #[test]
    fn audit_context_menu_keys_resolve_in_both_locales() {
        for key in MENU_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, *key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn audit_context_menu_copy_row_as_csv_label_differs_between_locales() {
        let en = dbflux_i18n::t!("document.audit.menu.copy_row_as_csv", locale = "en");
        let es = dbflux_i18n::t!("document.audit.menu.copy_row_as_csv", locale = "es");

        assert_eq!(en, "Copy row as CSV");
        assert_ne!(en, es);
    }

    #[test]
    fn audit_context_menu_label_round_trips_translated_value() {
        let item = AuditMenuItem::item(
            dbflux_i18n::t!("document.audit.menu.copy_row_as_csv"),
            AuditContextMenuAction::CopyRowAsCsv,
            dbflux_components::icons::AppIcon::Layers,
        );

        assert_eq!(
            item.label.to_string(),
            dbflux_i18n::t!("document.audit.menu.copy_row_as_csv")
        );
    }

    #[test]
    fn context_menu_items_includes_correlation_entry_only_when_present() {
        let without_correlation = AuditDocument::context_menu_items(false, false);
        let with_correlation = AuditDocument::context_menu_items(true, false);

        assert_eq!(without_correlation.len(), 3);
        assert!(
            !without_correlation
                .iter()
                .any(|item| item.action == Some(AuditContextMenuAction::FilterByCorrelation))
        );

        assert_eq!(with_correlation.len(), 5);

        let pending_approval = AuditDocument::context_menu_items(false, true);
        assert_eq!(
            pending_approval.last().and_then(|item| item.action),
            Some(AuditContextMenuAction::OpenApproval),
            "a pending approval row offers to open it"
        );
        assert!(
            without_correlation
                .iter()
                .any(|item| item.action == Some(AuditContextMenuAction::CopyJson)),
            "every row offers Copy row as JSON, like its detail"
        );
        assert!(
            with_correlation
                .iter()
                .any(|item| item.action == Some(AuditContextMenuAction::FilterByCorrelation))
        );
    }
}
