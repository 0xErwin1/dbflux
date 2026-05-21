//! Command dispatch and keyboard navigation for `AuditDocument`.
//!
//! Row cursor movement, context menu lifecycle, and the main
//! `dispatch_command` entry point live here so that `mod.rs` can focus
//! on document construction, data loading, and filter state.

use super::{AuditContextMenuAction, AuditDocument, AuditMenuItem, DEFAULT_PAGE_SIZE};
use crate::keymap::{Command, ContextId};
use crate::ui::icons::AppIcon;
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
        if self.context_menu.is_some() {
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

    /// Static menu item table — separators have `action: None`.
    pub(super) fn context_menu_items(has_correlation: bool) -> Vec<AuditMenuItem> {
        let mut items = vec![
            AuditMenuItem::item(
                "Copy Row as CSV",
                AuditContextMenuAction::CopyRowAsCsv,
                AppIcon::Layers,
            ),
            AuditMenuItem::item(
                "Copy Summary",
                AuditContextMenuAction::CopySummary,
                AppIcon::Layers,
            ),
        ];

        if has_correlation {
            items.push(AuditMenuItem::separator());
            items.push(AuditMenuItem::item(
                "Filter by Correlation",
                AuditContextMenuAction::FilterByCorrelation,
                AppIcon::ListFilter,
            ));
        }

        items
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
        let position = Point::new(px(8.0), px(y));

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

    #[allow(dead_code)]
    fn context_menu_item_count(&self) -> usize {
        let Some(menu) = &self.context_menu else {
            return 0;
        };

        let event = self.events.get(menu.row);
        let has_correlation = event
            .and_then(|e| e.correlation_id.as_deref())
            .map(|c| !c.is_empty())
            .unwrap_or(false);

        Self::context_menu_items(has_correlation)
            .iter()
            .filter(|i| !i.is_separator())
            .count()
    }

    fn navigate_menu_down(&mut self, cx: &mut Context<Self>) {
        let Some(ref mut menu) = self.context_menu else {
            return;
        };

        let event = self.events.get(menu.row);
        let has_correlation = event
            .and_then(|e| e.correlation_id.as_deref())
            .map(|c| !c.is_empty())
            .unwrap_or(false);

        let items = Self::context_menu_items(has_correlation);
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
        let Some(ref mut menu) = self.context_menu else {
            return;
        };

        let event = self.events.get(menu.row);
        let has_correlation = event
            .and_then(|e| e.correlation_id.as_deref())
            .map(|c| !c.is_empty())
            .unwrap_or(false);

        let items = Self::context_menu_items(has_correlation);
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

        let event = self.events.get(menu.row).cloned();
        let has_correlation = event
            .as_ref()
            .and_then(|e| e.correlation_id.as_deref())
            .map(|c| !c.is_empty())
            .unwrap_or(false);

        let items = Self::context_menu_items(has_correlation);
        let Some(item) = items.get(menu.selected_index) else {
            return;
        };

        let Some(action) = item.action else {
            return;
        };

        self.close_context_menu(window, cx);

        match action {
            AuditContextMenuAction::CopyRowAsCsv => {
                if let Some(event) = event {
                    let csv = Self::event_to_csv_row(&event);
                    cx.write_to_clipboard(ClipboardItem::new_string(csv));
                }
            }
            AuditContextMenuAction::CopySummary => {
                if let Some(event) = event {
                    let summary = event.summary.clone().unwrap_or_default();
                    cx.write_to_clipboard(ClipboardItem::new_string(summary));
                }
            }
            AuditContextMenuAction::FilterByCorrelation => {
                if let Some(event) = event
                    && let Some(correlation_id) =
                        event.correlation_id.clone().filter(|c| !c.is_empty())
                {
                    self.filter_by_correlation(correlation_id, cx);
                }
            }
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

        if self.toolbar_index(ToolbarSlot::Refresh) == Some(focused_index) {
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
                    self.filter_bar.move_left();
                    cx.notify();
                    true
                }
                Command::ColumnRight | Command::FocusRight => {
                    self.filter_bar.move_right();
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
