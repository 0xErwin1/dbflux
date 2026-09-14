use dbflux_app::keymap::Command;
use dbflux_components::icons::AppIcon;
use dbflux_components::tokens::Heights;
use gpui::*;

use super::KeyValueFocusMode;

pub(super) struct KvContextMenu {
    pub target: KvMenuTarget,
    pub position: Point<Pixels>,
    pub items: Vec<KvMenuItem>,
    pub selected_index: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum KvMenuTarget {
    Key,
    Value,
}

pub(super) struct KvMenuItem {
    pub label: SharedString,
    pub action: KvMenuAction,
    pub icon: AppIcon,
    pub is_danger: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum KvMenuAction {
    CopyKey,
    RenameKey,
    NewKey,
    DeleteKey,
    CopyMember,
    EditMember,
    AddMember,
    DeleteMember,
    CopyValue,
    EditValue,
    CopyAsCommand,
}

impl super::KeyValueDocument {
    pub(super) fn build_key_menu_items(&self) -> Vec<KvMenuItem> {
        let mut items = vec![
            KvMenuItem {
                label: dbflux_i18n::t!("document.key_value.context_menu.copy_key").into(),
                action: KvMenuAction::CopyKey,
                icon: AppIcon::Columns,
                is_danger: false,
            },
            KvMenuItem {
                label: dbflux_i18n::t!("document.key_value.context_menu.copy_as_command").into(),
                action: KvMenuAction::CopyAsCommand,
                icon: AppIcon::Code,
                is_danger: false,
            },
            KvMenuItem {
                label: dbflux_i18n::t!("document.key_value.context_menu.rename").into(),
                action: KvMenuAction::RenameKey,
                icon: AppIcon::Pencil,
                is_danger: false,
            },
            KvMenuItem {
                label: dbflux_i18n::t!("document.key_value.context_menu.new_key").into(),
                action: KvMenuAction::NewKey,
                icon: AppIcon::Plus,
                is_danger: false,
            },
            KvMenuItem {
                label: dbflux_i18n::t!("document.key_value.context_menu.delete_key").into(),
                action: KvMenuAction::DeleteKey,
                icon: AppIcon::Delete,
                is_danger: true,
            },
        ];

        if self.selected_value.is_none() {
            items.retain(|item| item.action != KvMenuAction::CopyAsCommand);
        }

        items
    }

    pub(super) fn build_value_menu_items(&self) -> Vec<KvMenuItem> {
        if self.is_stream_type() {
            vec![
                KvMenuItem {
                    label: dbflux_i18n::t!("document.key_value.context_menu.copy_entry").into(),
                    action: KvMenuAction::CopyMember,
                    icon: AppIcon::Columns,
                    is_danger: false,
                },
                KvMenuItem {
                    label: dbflux_i18n::t!("document.key_value.context_menu.copy_as_command")
                        .into(),
                    action: KvMenuAction::CopyAsCommand,
                    icon: AppIcon::Code,
                    is_danger: false,
                },
                KvMenuItem {
                    label: dbflux_i18n::t!("document.key_value.context_menu.add_entry").into(),
                    action: KvMenuAction::AddMember,
                    icon: AppIcon::Plus,
                    is_danger: false,
                },
                KvMenuItem {
                    label: dbflux_i18n::t!("document.key_value.context_menu.delete_entry").into(),
                    action: KvMenuAction::DeleteMember,
                    icon: AppIcon::Delete,
                    is_danger: true,
                },
            ]
        } else if self.is_structured_type() {
            vec![
                KvMenuItem {
                    label: dbflux_i18n::t!("document.key_value.context_menu.copy_member").into(),
                    action: KvMenuAction::CopyMember,
                    icon: AppIcon::Columns,
                    is_danger: false,
                },
                KvMenuItem {
                    label: dbflux_i18n::t!("document.key_value.context_menu.copy_as_command")
                        .into(),
                    action: KvMenuAction::CopyAsCommand,
                    icon: AppIcon::Code,
                    is_danger: false,
                },
                KvMenuItem {
                    label: dbflux_i18n::t!("document.key_value.context_menu.edit_member").into(),
                    action: KvMenuAction::EditMember,
                    icon: AppIcon::Pencil,
                    is_danger: false,
                },
                KvMenuItem {
                    label: dbflux_i18n::t!("document.key_value.context_menu.add_member").into(),
                    action: KvMenuAction::AddMember,
                    icon: AppIcon::Plus,
                    is_danger: false,
                },
                KvMenuItem {
                    label: dbflux_i18n::t!("document.key_value.context_menu.delete_member").into(),
                    action: KvMenuAction::DeleteMember,
                    icon: AppIcon::Delete,
                    is_danger: true,
                },
            ]
        } else {
            vec![
                KvMenuItem {
                    label: dbflux_i18n::t!("document.key_value.context_menu.copy_value").into(),
                    action: KvMenuAction::CopyValue,
                    icon: AppIcon::Columns,
                    is_danger: false,
                },
                KvMenuItem {
                    label: dbflux_i18n::t!("document.key_value.context_menu.copy_as_command")
                        .into(),
                    action: KvMenuAction::CopyAsCommand,
                    icon: AppIcon::Code,
                    is_danger: false,
                },
                KvMenuItem {
                    label: dbflux_i18n::t!("document.key_value.context_menu.edit_value").into(),
                    action: KvMenuAction::EditValue,
                    icon: AppIcon::Pencil,
                    is_danger: false,
                },
                KvMenuItem {
                    label: dbflux_i18n::t!("document.key_value.context_menu.delete_key").into(),
                    action: KvMenuAction::DeleteKey,
                    icon: AppIcon::Delete,
                    is_danger: true,
                },
            ]
        }
    }

    /// Computes a window-coordinate position for keyboard-triggered menus,
    /// aligned vertically with the selected row in the active panel.
    pub(super) fn keyboard_menu_position(&self, target: KvMenuTarget) -> Point<Pixels> {
        let left_header = Heights::TOOLBAR + Heights::ROW_COMPACT;
        let right_header =
            Heights::TOOLBAR + Heights::ROW_COMPACT + px(30.0) + Heights::ROW_COMPACT;

        match target {
            KvMenuTarget::Key => {
                let row_index = self.selected_index.unwrap_or(0) as f32;
                Point {
                    x: self.panel_origin.x + px(12.0), // guardrail-allow: positional offset, not a spacing/layout token
                    y: self.panel_origin.y + left_header + Heights::ROW * row_index,
                }
            }
            KvMenuTarget::Value => {
                let row_index = self.selected_member_index.unwrap_or(0) as f32;
                Point {
                    x: self.panel_origin.x + px(240.0) + px(12.0), // guardrail-allow: positional offset, not a spacing/layout token
                    y: self.panel_origin.y + right_header + Heights::ROW * row_index,
                }
            }
        }
    }

    pub(super) fn open_context_menu(
        &mut self,
        target: KvMenuTarget,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let items = match target {
            KvMenuTarget::Key => self.build_key_menu_items(),
            KvMenuTarget::Value => self.build_value_menu_items(),
        };

        self.context_menu = Some(KvContextMenu {
            target,
            position,
            items,
            selected_index: 0,
        });

        self.context_menu_focus.focus(window);
        cx.notify();
    }

    pub(super) fn close_context_menu(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(menu) = self.context_menu.take() {
            match menu.target {
                KvMenuTarget::Key => self.focus_mode = KeyValueFocusMode::List,
                KvMenuTarget::Value => self.focus_mode = KeyValueFocusMode::ValuePanel,
            }
        }

        self.focus_handle.focus(window);
        cx.notify();
    }

    pub(super) fn dispatch_menu_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let item_count = match &self.context_menu {
            Some(menu) => menu.items.len(),
            None => return false,
        };

        match cmd {
            Command::MenuDown => {
                if let Some(ref mut menu) = self.context_menu {
                    menu.selected_index = (menu.selected_index + 1) % item_count;
                    cx.notify();
                }
                true
            }
            Command::MenuUp => {
                if let Some(ref mut menu) = self.context_menu {
                    menu.selected_index = if menu.selected_index == 0 {
                        item_count - 1
                    } else {
                        menu.selected_index - 1
                    };
                    cx.notify();
                }
                true
            }
            Command::MenuSelect => {
                if let Some(menu) = self.context_menu.take() {
                    let action = menu.items[menu.selected_index].action;
                    let target = menu.target;
                    self.execute_menu_action(action, target, window, cx);
                }
                true
            }
            Command::MenuBack | Command::Cancel => {
                self.close_context_menu(window, cx);
                true
            }
            _ => false,
        }
    }

    pub(super) fn execute_menu_action(
        &mut self,
        action: KvMenuAction,
        target: KvMenuTarget,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match target {
            KvMenuTarget::Key => self.focus_mode = KeyValueFocusMode::List,
            KvMenuTarget::Value => self.focus_mode = KeyValueFocusMode::ValuePanel,
        }
        self.focus_handle.focus(window);

        match action {
            KvMenuAction::CopyKey => {
                if let Some(key) = self.selected_key() {
                    cx.write_to_clipboard(ClipboardItem::new_string(key));
                }
            }
            KvMenuAction::RenameKey => {
                self.start_rename(window, cx);
            }
            KvMenuAction::NewKey => {
                self.pending_open_new_key_modal = true;
            }
            KvMenuAction::DeleteKey => {
                self.request_delete_key(cx);
            }
            KvMenuAction::CopyMember => {
                if let Some(idx) = self.selected_member_index
                    && let Some(member) = self.cached_members.get(idx)
                {
                    cx.write_to_clipboard(ClipboardItem::new_string(member.display.clone()));
                }
            }
            KvMenuAction::EditMember => {
                if let Some(idx) = self.selected_member_index {
                    self.start_member_edit(idx, window, cx);
                }
            }
            KvMenuAction::AddMember => {
                if let Some(key_type) = self.selected_key_type() {
                    self.pending_open_add_member_modal = Some(key_type);
                }
            }
            KvMenuAction::DeleteMember => {
                if let Some(idx) = self.selected_member_index {
                    self.request_delete_member(idx, cx);
                }
            }
            KvMenuAction::CopyValue => {
                if let Some(value) = &self.selected_value {
                    let text = String::from_utf8_lossy(&value.value).to_string();
                    cx.write_to_clipboard(ClipboardItem::new_string(text));
                }
            }
            KvMenuAction::EditValue => {
                self.start_string_edit(window, cx);
            }
            KvMenuAction::CopyAsCommand => {
                self.handle_copy_as_command(target, cx);
            }
        }

        cx.notify();
    }
}

#[cfg(test)]
mod tests {
    use super::{KvMenuAction, KvMenuItem};
    use dbflux_components::icons::AppIcon;

    const CONTEXT_MENU_KEYS: &[&str] = &[
        "document.key_value.context_menu.add_entry",
        "document.key_value.context_menu.add_member",
        "document.key_value.context_menu.copy_as_command",
        "document.key_value.context_menu.copy_entry",
        "document.key_value.context_menu.copy_key",
        "document.key_value.context_menu.copy_member",
        "document.key_value.context_menu.copy_value",
        "document.key_value.context_menu.delete_entry",
        "document.key_value.context_menu.delete_key",
        "document.key_value.context_menu.delete_member",
        "document.key_value.context_menu.edit_member",
        "document.key_value.context_menu.edit_value",
        "document.key_value.context_menu.new_key",
        "document.key_value.context_menu.rename",
    ];

    #[test]
    fn key_value_context_menu_keys_resolve_in_both_locales() {
        for key in CONTEXT_MENU_KEYS {
            let english = dbflux_i18n::t!(key, locale = "en");
            let spanish = dbflux_i18n::t!(key, locale = "es");

            assert!(!english.is_empty(), "empty English translation for {key}");
            assert!(!spanish.is_empty(), "empty Spanish translation for {key}");
            assert_ne!(english, *key, "English translation missing for {key}");
            assert_ne!(spanish, *key, "Spanish translation missing for {key}");
            assert_ne!(
                english,
                format!("en.{key}"),
                "English translation missing for {key}"
            );
            assert_ne!(
                spanish,
                format!("es.{key}"),
                "Spanish translation missing for {key}"
            );
        }
    }

    #[test]
    fn key_value_context_menu_label_round_trips_translated_value() {
        let item = KvMenuItem {
            label: dbflux_i18n::t!("document.key_value.context_menu.copy_key").into(),
            action: KvMenuAction::CopyKey,
            icon: AppIcon::Columns,
            is_danger: false,
        };

        assert_eq!(
            item.label.to_string(),
            dbflux_i18n::t!("document.key_value.context_menu.copy_key")
        );
    }
}
