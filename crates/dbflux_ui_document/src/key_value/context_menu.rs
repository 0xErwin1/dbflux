use dbflux_app::keymap::Command;
use dbflux_components::icons::AppIcon;
use dbflux_components::tokens::KeyValueMetrics;
use gpui::*;

use super::KeyValueFocusMode;
use super::collection_panes::busiest_group;
use super::decode::ViewAs;
use super::key_tree::KeyListLayout;
use dbflux_core::{KeyType, KeyValueFeatures};

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    // Document-wide actions the toolbar and list offer to the pointer.
    ToggleListLayout,
    BulkDelete,
    SearchWholeKeyspace,
    StopScan,
    AutoRefresh,
    // Value panel actions the header, the large-value gate and the stream
    // callout offer to the pointer.
    ReloadValue,
    PreviewValuePrefix,
    LoadValueWithoutLimit,
    ViewPendingEntries,
    OpenClaimForm,
    ConfirmClaim,
    CloseClaimForm,
    /// A choice of the string value's View as switch.
    ViewAs(ViewAs),
    /// The string value's decompression list, opened for the keyboard.
    Decompression,
}

impl super::KeyValueDocument {
    pub(super) fn build_key_menu_items(&self, cx: &App) -> Vec<KvMenuItem> {
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

        items.extend(self.document_menu_items(cx));
        items
    }

    /// The toolbar's and the key list's own buttons: the list / tree
    /// layout, bulk delete, the auto-refresh interval and, while a filtered
    /// scan reads page by page, Search whole keyspace and Stop.
    fn document_menu_items(&self, cx: &App) -> Vec<KvMenuItem> {
        let item = |key: &str, action, icon, is_danger| KvMenuItem {
            label: dbflux_i18n::t!(key).into(),
            action,
            icon,
            is_danger,
        };

        let mut items = Vec::new();

        if self.is_filtered_scan(cx) && self.is_scanning() && !self.scan_complete() {
            items.push(item(
                "document.key_value.search.stop",
                KvMenuAction::StopScan,
                AppIcon::CircleX,
                false,
            ));

            if self.scan_mode != super::pagination::ScanMode::WholeKeyspace {
                items.push(item(
                    "document.key_value.search.whole_keyspace",
                    KvMenuAction::SearchWholeKeyspace,
                    AppIcon::Layers,
                    false,
                ));
            }
        }

        let layout_key = match self.list_layout {
            KeyListLayout::List => "document.key_value.context_menu.show_as_tree",
            KeyListLayout::Tree => "document.key_value.context_menu.show_as_list",
        };
        items.push(item(
            layout_key,
            KvMenuAction::ToggleListLayout,
            AppIcon::Rows3,
            false,
        ));

        items.push(item(
            "document.data.context_menu.toolbar.auto_refresh",
            KvMenuAction::AutoRefresh,
            AppIcon::Clock,
            false,
        ));

        if self.supports_bulk_delete(cx) {
            items.push(item(
                "document.key_value.bulk_delete.menu_item",
                KvMenuAction::BulkDelete,
                AppIcon::Delete,
                true,
            ));
        }

        items
    }

    /// The value panel's own buttons: reload, the large-value gate's preview
    /// and load anyway, and the stream callout's pending entries and claim.
    fn value_panel_menu_items(&self) -> Vec<KvMenuItem> {
        let item = |label: String, action, icon| KvMenuItem {
            label: label.into(),
            action,
            icon,
            is_danger: false,
        };

        let mut items = Vec::new();

        let Some(value) = &self.selected_value else {
            return items;
        };

        items.push(item(
            dbflux_i18n::t!("document.key_value.value.reload"),
            KvMenuAction::ReloadValue,
            AppIcon::RefreshCcw,
        ));

        if self.shows_string_body() {
            for view in ViewAs::ALL
                .into_iter()
                .filter(|view| *view != self.value_view_as)
            {
                items.push(item(
                    dbflux_i18n::t!(
                        "document.key_value.context_menu.view_as",
                        view = view.label()
                    ),
                    KvMenuAction::ViewAs(view),
                    AppIcon::Eye,
                ));
            }

            items.push(item(
                dbflux_i18n::t!("document.key_value.context_menu.decompression"),
                KvMenuAction::Decompression,
                AppIcon::Boxes,
            ));
        }

        if matches!(value.load_state, dbflux_core::KeyLoadState::TooLarge { .. }) {
            let is_string = matches!(
                self.selected_key_type(),
                Some(KeyType::String | KeyType::Bytes | KeyType::Json) | None
            );
            if is_string && self.key_features.contains(KeyValueFeatures::VALUE_PREFIX) {
                items.push(item(
                    dbflux_i18n::t!(
                        "document.key_value.gate.preview",
                        size = super::metadata::format_size(super::decode::VALUE_PREVIEW_BYTES)
                    ),
                    KvMenuAction::PreviewValuePrefix,
                    AppIcon::Eye,
                ));
            }

            items.push(item(
                dbflux_i18n::t!("document.key_value.render.gate.load_anyway"),
                KvMenuAction::LoadValueWithoutLimit,
                AppIcon::Download,
            ));
        }

        if let Some(pane) = &self.stream_pane
            && let Some(group) = busiest_group(&pane.groups)
        {
            items.push(item(
                dbflux_i18n::t!("document.key_value.stream.view_pending"),
                KvMenuAction::ViewPendingEntries,
                AppIcon::Eye,
            ));

            let claim_open = pane
                .claim
                .as_ref()
                .is_some_and(|claim| claim.group == group.name);

            if claim_open {
                items.push(item(
                    dbflux_i18n::t!("document.key_value.stream.claim_confirm"),
                    KvMenuAction::ConfirmClaim,
                    AppIcon::ArrowLeftRight,
                ));
                items.push(item(
                    dbflux_i18n::t!("document.key_value.context_menu.close_claim"),
                    KvMenuAction::CloseClaimForm,
                    AppIcon::X,
                ));
            } else {
                items.push(item(
                    dbflux_i18n::t!("document.key_value.stream.claim"),
                    KvMenuAction::OpenClaimForm,
                    AppIcon::ArrowLeftRight,
                ));
            }
        }

        items
    }

    /// Whether the value panel shows a string body, whose toolbar carries
    /// the View as switch and the decompression list: a loaded value that is
    /// neither a collection nor a sorted set or stream.
    fn shows_string_body(&self) -> bool {
        self.selected_value.as_ref().is_some_and(|value| {
            !matches!(value.load_state, dbflux_core::KeyLoadState::TooLarge { .. })
        }) && self.zset_pane.is_none()
            && self.stream_pane.is_none()
            && !self.is_structured_type()
    }

    /// The group the stream callout shows, which its buttons act on.
    fn callout_group(&self) -> Option<String> {
        let pane = self.stream_pane.as_ref()?;
        busiest_group(&pane.groups).map(|group| group.name.clone())
    }

    pub(super) fn build_value_menu_items(&self, cx: &App) -> Vec<KvMenuItem> {
        let mut items = self.member_menu_items();
        items.extend(self.value_panel_menu_items());
        items.extend(self.document_menu_items(cx));
        items
    }

    fn member_menu_items(&self) -> Vec<KvMenuItem> {
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
    pub(super) fn keyboard_menu_position(
        &self,
        target: KvMenuTarget,
        window: &Window,
    ) -> Point<Pixels> {
        let rem_size = window.rem_size();
        let toolbars = KeyValueMetrics::TOOLBAR_HEIGHT.to_pixels(rem_size) * 2.0;

        match target {
            KvMenuTarget::Key => {
                let row_index = self.list_cursor.unwrap_or(0) as f32;
                Point {
                    x: self.panel_origin.x + KeyValueMetrics::LIST_PADDING_LEFT,
                    y: self.panel_origin.y
                        + toolbars
                        + KeyValueMetrics::LIST_HEADER_HEIGHT.to_pixels(rem_size)
                        + KeyValueMetrics::LIST_ROW_HEIGHT.to_pixels(rem_size) * row_index,
                }
            }
            KvMenuTarget::Value => {
                let row_index = self.selected_member_index.unwrap_or(0) as f32;
                Point {
                    x: self.panel_origin.x
                        + KeyValueMetrics::KEY_LIST_WIDTH
                        + KeyValueMetrics::VALUE_PADDING_X,
                    y: self.panel_origin.y
                        + toolbars
                        + KeyValueMetrics::VALUE_HEADER_HEIGHT.to_pixels(rem_size)
                        + KeyValueMetrics::META_ROW_HEIGHT.to_pixels(rem_size)
                        + KeyValueMetrics::VALUE_TOOLBAR_HEIGHT.to_pixels(rem_size)
                        + KeyValueMetrics::MEMBER_HEADER_HEIGHT.to_pixels(rem_size)
                        + KeyValueMetrics::MEMBER_ROW_HEIGHT.to_pixels(rem_size) * row_index,
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
            KvMenuTarget::Key => self.build_key_menu_items(cx),
            KvMenuTarget::Value => self.build_value_menu_items(cx),
        };

        self.context_menu = Some(KvContextMenu {
            target,
            position,
            items,
            selected_index: 0,
        });

        self.context_menu_focus.focus(window, cx);
        cx.notify();
    }

    pub(super) fn close_context_menu(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(menu) = self.context_menu.take() {
            match menu.target {
                KvMenuTarget::Key => self.focus_mode = KeyValueFocusMode::List,
                KvMenuTarget::Value => self.focus_mode = KeyValueFocusMode::ValuePanel,
            }
        }

        self.focus_handle.focus(window, cx);
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
        self.focus_handle.focus(window, cx);

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
            KvMenuAction::ToggleListLayout => {
                let layout = match self.list_layout {
                    KeyListLayout::List => KeyListLayout::Tree,
                    KeyListLayout::Tree => KeyListLayout::List,
                };
                self.set_list_layout(layout, cx);
            }
            KvMenuAction::BulkDelete => self.open_bulk_delete(cx),
            KvMenuAction::SearchWholeKeyspace => self.search_whole_keyspace(cx),
            KvMenuAction::StopScan => self.stop_scan(cx),
            KvMenuAction::AutoRefresh => {
                self.refresh_dropdown
                    .update(cx, |dropdown, cx| dropdown.focus_and_open(window, cx));
            }
            KvMenuAction::ReloadValue => self.reload_selected_value(cx),
            KvMenuAction::PreviewValuePrefix => self.preview_value_prefix(cx),
            KvMenuAction::LoadValueWithoutLimit => self.load_selected_value_without_limit(cx),
            KvMenuAction::ViewPendingEntries => {
                if let Some(group) = self.callout_group() {
                    self.view_pending_entries(group, cx);
                }
            }
            KvMenuAction::OpenClaimForm => {
                if let Some(group) = self.callout_group() {
                    self.open_claim_form(group, window, cx);
                }
            }
            KvMenuAction::ConfirmClaim => self.claim_pending_entries(cx),
            KvMenuAction::CloseClaimForm => self.close_claim_form(cx),
            KvMenuAction::ViewAs(view) => self.set_value_view_as(view, cx),
            KvMenuAction::Decompression => {
                self.compression_dropdown
                    .update(cx, |dropdown, cx| dropdown.focus_and_open(window, cx));
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
        "document.key_value.context_menu.show_as_tree",
        "document.key_value.context_menu.show_as_list",
        "document.key_value.context_menu.close_claim",
        "document.key_value.context_menu.view_as",
        "document.key_value.context_menu.decompression",
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
