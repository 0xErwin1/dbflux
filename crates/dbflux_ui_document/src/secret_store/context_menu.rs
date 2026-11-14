//! Right-click menu of the secret store's listing rows, mirroring the
//! key-value document's menu (`key_value/context_menu.rs`): the right-clicked
//! row becomes the selection, the panel anchors at the pointer, and every
//! entry dispatches through the actions the toolbar, the footer keys and the
//! pane actions already use — the menu adds no logic of its own.
//!
//! The menu entries dispatch through the actions the details pane and the
//! pane actions already use — the menu adds no logic of its own. The write
//! entries (Edit value, Restore, Delete, and the ones the store offers
//! beyond them) open the same editor and dialogs the pane's buttons do, and
//! "Open in tab" hands the secret to the workspace, which opens it in its
//! own document.

use super::{SecretRowKey, SecretStoreDocument};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::composites::{MenuItem, render_menu_container, render_menu_item};
use dbflux_components::icons::AppIcon;
use gpui::*;

/// One open menu: the row it acts on, where it opened, its entries and the
/// keyboard highlight. Entries are rebuilt on every render, so an item
/// disabled by an in-flight describe re-enables without reopening the menu.
pub(super) struct SecretContextMenu {
    pub target: SecretRowKey,
    /// The secondary click's position in window coordinates.
    pub position: Point<Pixels>,
    pub items: Vec<SecretMenuItem>,
    pub selected_index: usize,
}

pub(super) struct SecretMenuItem {
    pub action: SecretMenuAction,
    pub label: SharedString,
    pub icon: AppIcon,
    /// The hint the shared chrome draws, resolved from the keymap; `None`
    /// for actions a chord does not reach.
    pub shortcut: Option<SharedString>,
    /// Why the entry cannot run, drawn under the label. A disabled entry
    /// never copies or fetches nothing silently.
    pub detail: Option<SharedString>,
    pub disabled: bool,
    /// A destructive entry, drawn in the danger tone.
    pub danger: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SecretMenuAction {
    OpenInTab,
    RetrieveValue,
    EditValue,
    RotateNow,
    EditTags,
    Restore,
    Delete,
    CopyName,
    CopyArn,
    CopyPath,
    Refresh,
}

impl SecretMenuAction {
    /// The panel id that names the entry's element, one action to one id, so
    /// the coverage registry can give each row its own keyboard path.
    fn panel_id(self) -> &'static str {
        match self {
            SecretMenuAction::OpenInTab => "secret-menu-open-in-tab",
            SecretMenuAction::RetrieveValue => "secret-menu-retrieve-value",
            SecretMenuAction::EditValue => "secret-menu-edit-value",
            SecretMenuAction::RotateNow => "secret-menu-rotate-now",
            SecretMenuAction::EditTags => "secret-menu-edit-tags",
            SecretMenuAction::Restore => "secret-menu-restore",
            SecretMenuAction::Delete => "secret-menu-delete",
            SecretMenuAction::CopyName => "secret-menu-copy-name",
            SecretMenuAction::CopyArn => "secret-menu-copy-arn",
            SecretMenuAction::CopyPath => "secret-menu-copy-path",
            SecretMenuAction::Refresh => "secret-menu-refresh",
        }
    }
}

/// The hint a menu row shows, formatted like the other keycaps in the app,
/// resolved from the effective keymap the way the data table's rows do it.
pub(super) fn menu_shortcut(command: Command) -> Option<SharedString> {
    dbflux_ui_base::keymap::effective_keymap()
        .chord_for_command(ContextId::Results, command)
        .map(|chord| {
            dbflux_ui_base::keymap::chord_display_parts(chord)
                .join(" ")
                .into()
        })
}

impl SecretMenuItem {
    pub(super) fn new(action: SecretMenuAction, label: SharedString, icon: AppIcon) -> Self {
        Self {
            action,
            label,
            icon,
            shortcut: None,
            detail: None,
            disabled: false,
            danger: false,
        }
    }

    pub(super) fn danger(mut self) -> Self {
        self.danger = true;
        self
    }

    pub(super) fn with_shortcut(mut self, command: Command) -> Self {
        self.shortcut = menu_shortcut(command);
        self
    }

    pub(super) fn disabled_with_reason(mut self, reason: SharedString) -> Self {
        self.disabled = true;
        self.detail = Some(reason);
        self
    }
}

impl SecretStoreDocument {
    /// The entries of the menu for `target`. Parameter rows (the entries of
    /// a hierarchical store) offer no ARN item: their entries carry no
    /// resource identifier the document copies.
    pub(super) fn build_menu_items(&self, target: &SecretRowKey, cx: &App) -> Vec<SecretMenuItem> {
        match target {
            SecretRowKey::Secret(name) => self.view.read(cx).secret_menu_items(name),
            SecretRowKey::Path(_) => vec![
                SecretMenuItem::new(
                    SecretMenuAction::CopyPath,
                    dbflux_i18n::t!("document.secret_store.context_menu.copy_path").into(),
                    AppIcon::Copy,
                ),
                SecretMenuItem::new(
                    SecretMenuAction::Refresh,
                    dbflux_i18n::t!("document.secret_store.toolbar.refresh").into(),
                    AppIcon::RefreshCcw,
                )
                .with_shortcut(Command::RefreshSchema),
            ],
            SecretRowKey::LoadMore => Vec::new(),
        }
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
        // A menu opened over another row would first select it, which is a
        // move the unsaved-edits guard refuses: opening the menu on a target
        // it cannot act on would offer entries for a secret the user is
        // leaving behind.
        if self.selected.as_ref() != Some(&row) && self.block_unsaved_value_edits(cx) {
            return;
        }

        self.select_row(row.clone(), cx);

        let items = self.build_menu_items(&row, cx);

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

    /// Drops the menu and a queued entry without a window at hand: the
    /// places that replay the listing (a refresh, a region switch, a tab
    /// that stopped being active) clear the state they made stale, and the
    /// next activation of the document gives the keyboard back.
    pub(super) fn drop_context_menu(&mut self) {
        if self.context_menu.take().is_some() {
            self.focus_mode = super::SecretFocusMode::Table;
            self.pending_menu_action = None;
        }

        self.pending_menu_action = None;
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
                let selected = self.context_menu.as_ref().and_then(|menu| {
                    menu.items
                        .get(menu.selected_index)
                        .map(|item| (item.action, item.disabled))
                });

                match selected {
                    // A disabled entry stays put: the menu keeps the
                    // keyboard and runs nothing, so an action the document
                    // blocks never runs from the menu.
                    Some((_, true)) => {}
                    Some((action, false)) => {
                        self.run_menu_action(action, window, cx);
                    }
                    None => {}
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
    /// a toolbar, footer-key or pane action; the menu only dispatches to it,
    /// so the copy actions the pane menu reaches have exactly one
    /// implementation.
    pub(super) fn run_menu_action(
        &mut self,
        action: SecretMenuAction,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.context_menu = None;

        match action {
            // These open an editor or a dialog that takes the keyboard; the
            // common refocus below would take it straight back. An opener that
            // refuses (unsaved edits, a stale selection) opens nothing, and
            // then the document gets the keyboard back like any other entry.
            SecretMenuAction::EditValue => {
                self.view
                    .update(cx, |view, cx| view.request_edit_value(window, cx));

                if self.view.read(cx).is_editing_value() {
                    return;
                }
            }
            SecretMenuAction::Delete => {
                self.view
                    .update(cx, |view, cx| view.open_delete(window, cx));

                if self.view.read(cx).dialog_is_open() {
                    return;
                }
            }
            SecretMenuAction::RotateNow => {
                self.view
                    .update(cx, |view, cx| view.rotate_selected_now(window, cx));

                if self.view.read(cx).dialog_is_open() {
                    return;
                }
            }
            SecretMenuAction::EditTags => {
                self.view
                    .update(cx, |view, cx| view.open_edit_tags(window, cx));

                if self.view.read(cx).dialog_is_open() {
                    return;
                }
            }
            SecretMenuAction::OpenInTab => self.open_selected_in_tab(cx),
            SecretMenuAction::Restore => self.view.update(cx, |view, cx| view.restore_selected(cx)),
            SecretMenuAction::RetrieveValue => self
                .view
                .update(cx, |view, cx| view.retrieve_selected_value(cx)),
            SecretMenuAction::CopyName => {
                self.view.update(cx, |view, cx| view.copy_selected_name(cx))
            }
            SecretMenuAction::CopyArn => self.view.update(cx, |view, cx| view.copy_resource_id(cx)),
            SecretMenuAction::CopyPath => self.copy_selected_path(cx),
            SecretMenuAction::Refresh => self.refresh(cx),
        }

        self.focus_mode = super::SecretFocusMode::Table;
        self.focus_handle.focus(window, cx);
        cx.notify();
    }
}

/// The menu's entries as the shared chrome draws them: label, icon, the
/// shortcut hint, the reason line of a disabled entry, and the disabled
/// state itself.
fn shared_menu_item(item: &SecretMenuItem) -> MenuItem {
    let mut entry = MenuItem::new(item.label.clone()).icon(item.icon);

    if let Some(shortcut) = &item.shortcut {
        entry = entry.shortcut(shortcut.clone());
    }

    if let Some(detail) = &item.detail {
        entry = entry.detail(detail.clone());
    }

    if item.danger {
        entry = entry.danger();
    }

    if item.disabled {
        entry.disabled()
    } else {
        entry
    }
}

/// The menu's rows, each under a per-action element id so the coverage
/// registry can name the keyboard path of that one entry, wired to the
/// queue-and-run click and the hover cursor.
fn menu_panel(menu: &SecretContextMenu, cx: &mut Context<SecretStoreDocument>) -> Div {
    let click_entity = cx.entity().downgrade();
    let hover_entity = cx.entity().downgrade();

    let rows: Vec<_> = menu
        .items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let action = item.action;
            let shared = shared_menu_item(item);
            let click_entity = click_entity.clone();
            let hover_entity = hover_entity.clone();

            render_menu_item(
                action.panel_id(),
                &shared,
                index,
                menu.selected_index == index,
                move |_, _, cx| {
                    if let Some(doc) = click_entity.upgrade() {
                        doc.update(cx, |this, cx| {
                            this.context_menu = None;
                            this.pending_menu_action = Some(action);
                            cx.notify();
                        });
                    }
                },
                move |cx| {
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
        })
        .collect();

    render_menu_container(rows, cx)
}

/// The menu panel inside the window-fitted anchor: the panel positions
/// itself at the secondary click's window coordinates and stays inside the
/// window near the edges.
pub(super) fn render_context_menu(
    menu: &SecretContextMenu,
    menu_focus: &FocusHandle,
    cx: &mut Context<SecretStoreDocument>,
) -> impl IntoElement {
    use dbflux_ui_base::keymap;

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
            // The panel anchors at the secondary click's window coordinates
            // and stays inside the window near the edges.
            .child(
                anchored().position(menu.position).snap_to_window().child(
                    div()
                        .id("secret-context-menu")
                        .occlude()
                        .on_mouse_down(MouseButton::Left, |_, _, cx| {
                            cx.stop_propagation();
                        })
                        .on_mouse_down(MouseButton::Right, |_, _, cx| {
                            cx.stop_propagation();
                        })
                        .child(menu_panel(menu, cx)),
                ),
            ),
    )
    .with_priority(1)
}
