//! Right-click menu of the secret store's listing rows, mirroring the
//! key-value document's menu (`key_value/context_menu.rs`): the right-clicked
//! row becomes the selection, the panel anchors at the pointer, and every
//! entry dispatches through the actions the toolbar, the footer keys and the
//! pane actions already use — the menu adds no logic of its own.
//!
//! The menu lists read-only actions only. Any write action (Edit value,
//! Rotate now, Edit tags, Delete) waits until the write path exists
//! (issue #1270), and "Open in tab" waits for a per-secret document target:
//! the value pane is deliberately scoped to this tab.

use super::{DetailsState, SecretRowKey, SecretStoreDocument, copied_toast};
use dbflux_app::keymap::Command;
use dbflux_components::composites::{MenuItem, render_menu_items};
use dbflux_components::icons::AppIcon;
use dbflux_core::chrono::{DateTime, Local, Utc};
use gpui::*;
use gpui_component::ActiveTheme;

/// One open menu: the row it acts on, where it opened, its entries and the
/// keyboard highlight. Entries are rebuilt on every render, so an item
/// disabled by an in-flight describe re-enables without reopening the menu.
pub(super) struct SecretContextMenu {
    pub target: SecretRowKey,
    /// The secondary click's position in window coordinates; converted to
    /// document coordinates at render time via `panel_origin`.
    pub position: Point<Pixels>,
    pub items: Vec<SecretMenuItem>,
    pub selected_index: usize,
}

pub(super) struct SecretMenuItem {
    pub action: SecretMenuAction,
    pub label: SharedString,
    pub icon: AppIcon,
    /// Why the entry cannot run, drawn under the label. A disabled entry
    /// never copies or fetches nothing silently.
    pub detail: Option<SharedString>,
    pub disabled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SecretMenuAction {
    RetrieveValue,
    CopyName,
    CopyArn,
    CopyPath,
    Refresh,
}

/// A deletion date the way the listing's badge spells it.
fn short_date(date: DateTime<Utc>) -> String {
    date.with_timezone(&Local).format("%Y-%m-%d").to_string()
}

impl SecretMenuItem {
    fn new(action: SecretMenuAction, label: SharedString, icon: AppIcon) -> Self {
        Self {
            action,
            label,
            icon,
            detail: None,
            disabled: false,
        }
    }

    fn disabled_with_reason(mut self, reason: SharedString) -> Self {
        self.disabled = true;
        self.detail = Some(reason);
        self
    }
}

impl SecretStoreDocument {
    /// The entries of the menu for `target`. Parameter rows (the entries of
    /// a hierarchical store) offer no ARN item: their entries carry no
    /// resource identifier the document copies.
    pub(super) fn build_menu_items(&self, target: &SecretRowKey) -> Vec<SecretMenuItem> {
        match target {
            SecretRowKey::Secret(name) => {
                let mut items = vec![self.retrieve_item(name), self.copy_name_item()];

                if !self.hierarchical() {
                    items.push(self.copy_arn_item());
                }

                items
            }
            SecretRowKey::Path(_) => vec![self.copy_path_item(), self.refresh_item()],
            SecretRowKey::LoadMore => Vec::new(),
        }
    }

    fn retrieve_item(&self, name: &str) -> SecretMenuItem {
        let deletion_date = self
            .listing
            .find(name)
            .and_then(|summary| summary.deletion_scheduled_for);

        // A scheduled deletion keeps the value unreachable: the retrieval is
        // already blocked by the document, so the entry says why instead of
        // running into that block.
        let (detail, disabled) = match deletion_date {
            Some(date) => (
                Some(
                    dbflux_i18n::t!(
                        "document.secret_store.value.deletion_scheduled",
                        date = short_date(date)
                    )
                    .into(),
                ),
                true,
            ),
            None => (None, !self.can_retrieve_value()),
        };

        SecretMenuItem {
            action: SecretMenuAction::RetrieveValue,
            label: dbflux_i18n::t!("document.secret_store.value.retrieve").into(),
            icon: AppIcon::Eye,
            detail,
            disabled,
        }
    }

    fn copy_name_item(&self) -> SecretMenuItem {
        SecretMenuItem::new(
            SecretMenuAction::CopyName,
            dbflux_i18n::t!("document.secret_store.context_menu.copy_name").into(),
            AppIcon::Copy,
        )
    }

    /// Copy ARN is only meaningful once the describe delivered the resource
    /// identifier; otherwise it stays disabled and names what is missing.
    fn copy_arn_item(&self) -> SecretMenuItem {
        let known = matches!(
            &self.details,
            DetailsState::Loaded(details) if details.resource_id.is_some()
        );

        if known {
            return SecretMenuItem::new(
                SecretMenuAction::CopyArn,
                dbflux_i18n::t!("document.secret_store.action.copy_resource_id").into(),
                AppIcon::Copy,
            );
        }

        let reason = match &self.details {
            DetailsState::Loaded(_) => {
                dbflux_i18n::t!("document.secret_store.context_menu.arn_missing")
            }
            _ => dbflux_i18n::t!("document.secret_store.context_menu.arn_loading"),
        };

        SecretMenuItem::new(
            SecretMenuAction::CopyArn,
            dbflux_i18n::t!("document.secret_store.action.copy_resource_id").into(),
            AppIcon::Copy,
        )
        .disabled_with_reason(reason.into())
    }

    fn copy_path_item(&self) -> SecretMenuItem {
        SecretMenuItem::new(
            SecretMenuAction::CopyPath,
            dbflux_i18n::t!("document.secret_store.context_menu.copy_path").into(),
            AppIcon::Copy,
        )
    }

    fn refresh_item(&self) -> SecretMenuItem {
        SecretMenuItem::new(
            SecretMenuAction::Refresh,
            dbflux_i18n::t!("document.secret_store.toolbar.refresh").into(),
            AppIcon::RefreshCcw,
        )
    }

    /// Opens the menu for the right-clicked row. The row is selected first,
    /// so the entries (and the keyboard cursor left behind when the menu
    /// closes) agree with what was right-clicked.
    pub(super) fn open_context_menu(
        &mut self,
        row: SecretRowKey,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.select_row(row.clone(), cx);

        let items = self.build_menu_items(&row);

        if items.is_empty() {
            return;
        }

        self.context_menu = Some(SecretContextMenu {
            target: row,
            position,
            items,
            selected_index: 0,
        });

        self.context_menu_focus.focus(window, cx);
        cx.notify();
    }

    pub(super) fn close_context_menu(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.context_menu.take().is_some() {
            self.focus_mode = super::SecretFocusMode::Table;
            self.focus_handle.focus(window, cx);
            cx.notify();
        }
    }

    /// The menu owns the keyboard while it is up: a command it does not bind
    /// must not move the listing under a target the user picked with a right
    /// click.
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
                if let Some(menu) = self.context_menu.as_mut() {
                    menu.selected_index = (menu.selected_index + 1) % item_count;
                    cx.notify();
                }

                true
            }
            Command::MenuUp => {
                if let Some(menu) = self.context_menu.as_mut() {
                    menu.selected_index =
                        menu.selected_index.checked_sub(1).unwrap_or(item_count - 1);
                    cx.notify();
                }

                true
            }
            Command::MenuSelect | Command::Execute => {
                // selected_index stays within `items`: it starts at 0 on
                // open, the MenuUp/MenuDown arms keep it modulo the count,
                // and rebuilds keep the entry order.
                #[expect(
                    clippy::indexing_slicing,
                    reason = "selected_index starts at 0 on open and the MenuUp/MenuDown arms keep it modulo menu.items.len(), which this dispatch does not re-check"
                )]
                if let Some(menu) = self.context_menu.take() {
                    let action = menu.items[menu.selected_index].action;
                    self.run_menu_action(action, menu.target, window, cx);
                }

                true
            }
            Command::MenuBack | Command::Cancel => {
                self.close_context_menu(window, cx);
                true
            }
            _ => true,
        }
    }

    /// Runs a menu entry and closes the menu. Every action already exists as
    /// a toolbar or footer-key action; the menu only dispatches to it.
    pub(super) fn run_menu_action(
        &mut self,
        action: SecretMenuAction,
        target: SecretRowKey,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.context_menu = None;

        match action {
            SecretMenuAction::RetrieveValue => self.retrieve_selected_value(cx),
            SecretMenuAction::CopyName => {
                if let SecretRowKey::Secret(name) = target {
                    cx.write_to_clipboard(ClipboardItem::new_string(name));
                    copied_toast(
                        dbflux_i18n::t!("document.secret_store.toast.name_copied"),
                        cx,
                    );
                }
            }
            SecretMenuAction::CopyArn => self.copy_resource_id(cx),
            SecretMenuAction::CopyPath => {
                if let SecretRowKey::Path(path) = target {
                    cx.write_to_clipboard(ClipboardItem::new_string(path));
                    copied_toast(
                        dbflux_i18n::t!("document.secret_store.toast.path_copied"),
                        cx,
                    );
                }
            }
            SecretMenuAction::Refresh => self.refresh(cx),
        }

        self.focus_mode = super::SecretFocusMode::Table;
        self.focus_handle.focus(window, cx);
        cx.notify();
    }
}

use dbflux_ui_base::keymap;

/// The menu's entries as the shared chrome draws them: label, icon, the
/// reason line of a disabled entry, and the disabled state itself.
fn shared_menu_items(menu: &SecretContextMenu) -> Vec<MenuItem> {
    menu.items
        .iter()
        .map(|item| {
            let entry = MenuItem::new(item.label.clone()).icon(item.icon);

            let entry = match &item.detail {
                Some(detail) => entry.detail(detail.clone()),
                None => entry,
            };

            if item.disabled {
                entry.disabled()
            } else {
                entry
            }
        })
        .collect()
}

/// The deferred menu panel at the secondary click's position, and the
/// overlay that dismisses it on any click outside. Mirrors the key-value
/// document's menu: the overlay takes the keyboard while the menu is up, so
/// the keymap's menu keys (and Escape) arrive here first, and a chosen entry
/// is queued and run on the next render, which has the window the action
/// needs.
pub(super) fn render_context_menu(
    menu: &SecretContextMenu,
    menu_focus: &FocusHandle,
    panel_origin: Point<Pixels>,
    cx: &mut Context<SecretStoreDocument>,
) -> impl IntoElement {
    let menu_x = menu.position.x - panel_origin.x;
    let menu_y = menu.position.y - panel_origin.y;

    deferred(
        div()
            .id("secret-context-menu-overlay")
            .absolute()
            .top_0()
            .left_0()
            .size_full()
            .track_focus(menu_focus)
            // The document reports the ContextMenu context while the menu is
            // open, so the keymap's menu keys arrive here first.
            .on_action(
                cx.listener(|this, action: &keymap::RunCommand, window, cx| {
                    let handled = keymap::run_command(action)
                        .is_some_and(|command| this.dispatch_menu_command(command, window, cx));

                    if !handled {
                        cx.propagate();
                    }
                }),
            )
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, window, cx| {
                    this.close_context_menu(window, cx);
                }),
            )
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(|this, _, window, cx| {
                    this.close_context_menu(window, cx);
                }),
            )
            .child(
                div()
                    .absolute()
                    .left(menu_x)
                    .top(menu_y)
                    .occlude()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| {
                        cx.stop_propagation();
                    })
                    .on_mouse_down(MouseButton::Right, |_, _, cx| {
                        cx.stop_propagation();
                    })
                    .child(menu_panel(menu, cx)),
            ),
    )
    .with_priority(1)
}

/// The menu's rows, wired to the queue-and-run click and the hover cursor.
fn menu_panel(menu: &SecretContextMenu, cx: &mut Context<SecretStoreDocument>) -> Div {
    let actions: Vec<SecretMenuAction> = menu.items.iter().map(|item| item.action).collect();
    let target = menu.target.clone();
    let click_entity = cx.entity().downgrade();
    let hover_entity = cx.entity().downgrade();

    render_menu_items(
        "secret-context-menu",
        &shared_menu_items(menu),
        Some(menu.selected_index),
        move |index, cx| {
            let Some(action) = actions.get(index).copied() else {
                return;
            };

            if let Some(doc) = click_entity.upgrade() {
                doc.update(cx, |this, cx| {
                    this.context_menu = None;
                    this.pending_menu_action = Some((action, target.clone()));
                    cx.notify();
                });
            }
        },
        move |index, cx| {
            if let Some(doc) = hover_entity.upgrade() {
                doc.update(cx, |this, cx| {
                    if let Some(menu) = this.context_menu.as_mut()
                        && menu.selected_index != index
                    {
                        menu.selected_index = index;
                        cx.notify();
                    }
                });
            }
        },
        cx,
    )
}
