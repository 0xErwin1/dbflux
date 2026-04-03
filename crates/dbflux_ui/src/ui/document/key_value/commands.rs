use crate::keymap::Command;
use gpui::*;

use super::context_menu::KvMenuTarget;
use super::{KeyValueDocumentEvent, KeyValueFocusMode, KvValueViewMode};

impl super::KeyValueDocument {
    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if self.new_key_modal.read(cx).is_visible() {
            let handled = self
                .new_key_modal
                .update(cx, |modal, cx| modal.dispatch_command(cmd, window, cx));

            if !self.new_key_modal.read(cx).is_visible() {
                self.focus_mode = KeyValueFocusMode::List;
                self.focus_handle.focus(window);
                cx.notify();
            }

            return handled;
        }

        if self.add_member_modal.read(cx).is_visible() {
            let handled = self
                .add_member_modal
                .update(cx, |modal, cx| modal.dispatch_command(cmd, window, cx));

            if !self.add_member_modal.read(cx).is_visible() {
                self.focus_mode = KeyValueFocusMode::ValuePanel;
                self.focus_handle.focus(window);
                cx.notify();
            }

            return handled;
        }

        if self.context_menu.is_some() {
            return self.dispatch_menu_command(cmd, window, cx);
        }

        match cmd {
            // -- Context menu --
            Command::OpenContextMenu => {
                let target = match self.focus_mode {
                    KeyValueFocusMode::ValuePanel => KvMenuTarget::Value,
                    _ => KvMenuTarget::Key,
                };
                let position = self.keyboard_menu_position(target);
                self.open_context_menu(target, position, window, cx);
                true
            }

            // -- Panel switching (h/l) --
            Command::ColumnLeft => {
                if self.focus_mode == KeyValueFocusMode::ValuePanel {
                    if self.is_document_view_active()
                        && let Some(ts) = &self.document_tree_state
                    {
                        let cursor = ts.read(cx).cursor().cloned();
                        let is_expanded = cursor
                            .as_ref()
                            .map(|id| ts.read(cx).is_expanded(id))
                            .unwrap_or(false);
                        let is_root = cursor
                            .as_ref()
                            .map(|id| id.parent().is_none())
                            .unwrap_or(true);

                        if is_expanded || !is_root {
                            ts.update(cx, |s, cx| s.handle_left(cx));
                            return true;
                        }
                    }

                    self.focus_mode = KeyValueFocusMode::List;
                    cx.notify();
                }
                true
            }
            Command::ColumnRight => {
                if self.focus_mode == KeyValueFocusMode::List && self.selected_value.is_some() {
                    self.focus_mode = KeyValueFocusMode::ValuePanel;

                    if self.is_document_view_active()
                        && let Some(ts) = &self.document_tree_state
                        && ts.read(cx).cursor().is_none()
                    {
                        ts.update(cx, |s, cx| s.move_to_first(cx));
                    } else if self.selected_member_index.is_none()
                        && !self.cached_members.is_empty()
                    {
                        self.selected_member_index = Some(0);
                    }

                    cx.notify();
                } else if self.focus_mode == KeyValueFocusMode::ValuePanel
                    && self.is_document_view_active()
                    && let Some(ts) = &self.document_tree_state
                {
                    ts.update(cx, |s, cx| s.handle_right(cx));
                }
                true
            }

            // -- Vertical navigation (j/k) --
            Command::SelectNext => {
                match self.focus_mode {
                    KeyValueFocusMode::ValuePanel => {
                        if self.is_document_view_active() {
                            if let Some(ts) = &self.document_tree_state {
                                ts.update(cx, |s, cx| {
                                    s.move_cursor(
                                        crate::ui::components::document_tree::TreeDirection::Down,
                                        cx,
                                    );
                                });
                            }
                        } else {
                            self.move_member_selection(1, cx);
                        }
                    }
                    _ => {
                        self.focus_mode = KeyValueFocusMode::List;
                        self.move_selection(1, cx);
                    }
                }
                true
            }
            Command::SelectPrev => {
                match self.focus_mode {
                    KeyValueFocusMode::ValuePanel => {
                        if self.is_document_view_active() {
                            if let Some(ts) = &self.document_tree_state {
                                ts.update(cx, |s, cx| {
                                    s.move_cursor(
                                        crate::ui::components::document_tree::TreeDirection::Up,
                                        cx,
                                    );
                                });
                            }
                        } else {
                            self.move_member_selection(-1, cx);
                        }
                    }
                    _ => {
                        self.focus_mode = KeyValueFocusMode::List;
                        self.move_selection(-1, cx);
                    }
                }
                true
            }
            Command::SelectFirst => {
                match self.focus_mode {
                    KeyValueFocusMode::ValuePanel => {
                        if self.is_document_view_active() {
                            if let Some(ts) = &self.document_tree_state {
                                ts.update(cx, |s, cx| s.move_to_first(cx));
                            }
                        } else if !self.cached_members.is_empty() {
                            self.selected_member_index = Some(0);
                            cx.notify();
                        }
                    }
                    _ => {
                        if !self.keys.is_empty() {
                            self.focus_mode = KeyValueFocusMode::List;
                            self.select_index(0, cx);
                        }
                    }
                }
                true
            }
            Command::SelectLast => {
                match self.focus_mode {
                    KeyValueFocusMode::ValuePanel => {
                        if self.is_document_view_active() {
                            if let Some(ts) = &self.document_tree_state {
                                ts.update(cx, |s, cx| s.move_to_last(cx));
                            }
                        } else if !self.cached_members.is_empty() {
                            self.selected_member_index = Some(self.cached_members.len() - 1);
                            cx.notify();
                        }
                    }
                    _ => {
                        if !self.keys.is_empty() {
                            self.focus_mode = KeyValueFocusMode::List;
                            self.select_index(self.keys.len() - 1, cx);
                        }
                    }
                }
                true
            }
            Command::ExpandCollapse => {
                if self.focus_mode == KeyValueFocusMode::ValuePanel
                    && self.is_document_view_active()
                    && let Some(ts) = &self.document_tree_state
                {
                    let cursor = ts.read(cx).cursor().cloned();
                    if let Some(id) = cursor {
                        ts.update(cx, |s, cx| s.toggle_expand(&id, cx));
                    }
                }
                true
            }

            // -- Actions --
            Command::Delete => {
                if self.focus_mode == KeyValueFocusMode::ValuePanel {
                    if let Some(idx) = self.selected_member_index {
                        self.request_delete_member(idx, cx);
                    }
                } else {
                    self.request_delete_key(cx);
                }
                true
            }
            Command::Rename => {
                self.start_rename(window, cx);
                true
            }
            Command::FocusSearch | Command::FocusToolbar => {
                let target_input = if self.focus_mode == KeyValueFocusMode::ValuePanel {
                    &self.members_filter_input
                } else {
                    &self.filter_input
                };
                target_input.update(cx, |input, cx| input.focus(window, cx));
                self.focus_mode = KeyValueFocusMode::TextInput;
                cx.notify();
                true
            }
            Command::FocusUp => {
                self.focus_mode = KeyValueFocusMode::TextInput;
                self.filter_input
                    .update(cx, |input, cx| input.focus(window, cx));
                cx.notify();
                true
            }
            Command::FocusDown => {
                self.focus_mode = KeyValueFocusMode::List;
                self.focus_handle.focus(window);
                cx.notify();
                true
            }
            Command::Cancel => {
                if self.pending_key_delete.is_some() {
                    self.cancel_delete_key(cx);
                } else if self.pending_member_delete.is_some() {
                    self.cancel_delete_member(cx);
                } else if self.string_edit_input.is_some() {
                    self.cancel_string_edit(cx);
                    self.focus_handle.focus(window);
                } else if self.renaming_index.is_some() {
                    self.cancel_rename(cx);
                    self.focus_handle.focus(window);
                } else if self.editing_member_index.is_some() {
                    self.cancel_member_edit(cx);
                    self.focus_handle.focus(window);
                } else if self.runner.cancel_primary(cx) {
                    self.last_error = None;
                } else {
                    self.focus_mode = KeyValueFocusMode::List;
                    self.focus_handle.focus(window);
                }
                cx.notify();
                true
            }
            Command::Execute => {
                if self.pending_key_delete.is_some() {
                    self.confirm_delete_key(cx);
                    return true;
                }
                if self.pending_member_delete.is_some() {
                    self.confirm_delete_member(cx);
                    return true;
                }
                if self.focus_mode == KeyValueFocusMode::ValuePanel {
                    if self.is_document_view_active() {
                        if let Some(ts) = &self.document_tree_state {
                            ts.update(cx, |s, cx| s.start_edit_at_cursor(window, cx));
                        }
                        return true;
                    }

                    if self.is_stream_type() {
                        return true;
                    }
                    if self.is_structured_type() {
                        if let Some(idx) = self.selected_member_index {
                            self.start_member_edit(idx, window, cx);
                        }
                    } else {
                        self.start_string_edit(window, cx);
                    }
                    return true;
                }
                self.start_string_edit(window, cx);
                true
            }
            Command::ResultsNextPage | Command::PageDown => {
                if self.focus_mode == KeyValueFocusMode::ValuePanel
                    && self.is_document_view_active()
                {
                    if let Some(ts) = &self.document_tree_state {
                        ts.update(cx, |s, cx| s.page_down(20, cx));
                    }
                } else {
                    self.go_next_page(cx);
                }
                true
            }
            Command::ResultsPrevPage | Command::PageUp => {
                if self.focus_mode == KeyValueFocusMode::ValuePanel
                    && self.is_document_view_active()
                {
                    if let Some(ts) = &self.document_tree_state {
                        ts.update(cx, |s, cx| s.page_up(20, cx));
                    }
                } else {
                    self.go_prev_page(cx);
                }
                true
            }
            Command::ResultsAddRow => {
                if self.focus_mode == KeyValueFocusMode::ValuePanel && self.is_structured_type() {
                    if let Some(key_type) = self.selected_key_type() {
                        self.pending_open_add_member_modal = Some(key_type);
                    }
                } else {
                    self.pending_open_new_key_modal = true;
                }
                cx.notify();
                true
            }
            Command::ResultsCopyRow => {
                let text = match self.focus_mode {
                    KeyValueFocusMode::ValuePanel => self
                        .selected_member_index
                        .and_then(|idx| self.cached_members.get(idx))
                        .map(|m| m.display.clone()),
                    _ => self
                        .selected_index
                        .and_then(|idx| self.keys.get(idx))
                        .map(|entry| entry.key.clone()),
                };
                if let Some(text) = text {
                    cx.write_to_clipboard(ClipboardItem::new_string(text));
                }
                true
            }
            Command::RefreshSchema => {
                self.reload_keys(cx);
                true
            }
            _ => false,
        }
    }
}
