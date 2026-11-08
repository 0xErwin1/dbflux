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
//! "Open in tab" waits for a per-secret document target: the value pane is
//! deliberately scoped to this tab.

use super::{DetailsState, SecretRowKey, SecretStoreDocument, copied_toast};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::composites::{MenuItem, render_menu_container, render_menu_item};
use dbflux_components::icons::AppIcon;
use dbflux_core::chrono::{DateTime, Local, Utc};
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

/// A deletion date the way the listing's badge spells it.
fn short_date(date: DateTime<Utc>) -> String {
    date.with_timezone(&Local).format("%Y-%m-%d").to_string()
}

/// The hint a menu row shows, formatted like the other keycaps in the app,
/// resolved from the effective keymap the way the data table's rows do it.
fn menu_shortcut(command: Command) -> Option<SharedString> {
    dbflux_ui_base::keymap::effective_keymap()
        .chord_for_command(ContextId::Results, command)
        .map(|chord| {
            dbflux_ui_base::keymap::chord_display_parts(chord)
                .join(" ")
                .into()
        })
}

impl SecretMenuItem {
    fn new(action: SecretMenuAction, label: SharedString, icon: AppIcon) -> Self {
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

    fn danger(mut self) -> Self {
        self.danger = true;
        self
    }

    fn with_shortcut(mut self, command: Command) -> Self {
        self.shortcut = menu_shortcut(command);
        self
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
                let mut items = vec![
                    self.retrieve_item(name),
                    SecretMenuItem::new(
                        SecretMenuAction::CopyName,
                        dbflux_i18n::t!("document.secret_store.context_menu.copy_name").into(),
                        AppIcon::Copy,
                    ),
                ];

                if !self.hierarchical() {
                    items.push(self.copy_arn_item());
                }

                // The design's Edit value entry: offered for a value that
                // can be edited, dispatching to the editor the details pane
                // already opens. The menu is built from the selected row's
                // state, which is the state the editor opens on.
                if self.can_edit_value() {
                    items.push(
                        SecretMenuItem::new(
                            SecretMenuAction::EditValue,
                            dbflux_i18n::t!("document.secret_store.edit.open").into(),
                            AppIcon::Pencil,
                        )
                        .with_shortcut(Command::EditValue),
                    );
                }

                // Rotate now starts a rotation of a secret that rotates.
                if self.supports_rotation() {
                    items.push(self.write_item(
                        SecretMenuItem::new(
                            SecretMenuAction::RotateNow,
                            dbflux_i18n::t!("document.secret_store.rotate.button").into(),
                            AppIcon::Zap,
                        ),
                        self.can_start_rotation_action(),
                        true,
                    ));
                }

                // Edit tags opens the dialog the Tags section's button opens.
                if self.supports_tags() {
                    items.push(self.write_item(
                        SecretMenuItem::new(
                            SecretMenuAction::EditTags,
                            dbflux_i18n::t!("document.secret_store.tags_edit.button").into(),
                            AppIcon::Tag,
                        ),
                        self.can_edit_tags(),
                        true,
                    ));
                }

                // Restore cancels a scheduled deletion; Delete schedules or
                // makes one. Each is drawn disabled, with the reason, when
                // the secret's own state refuses it.
                if self.supports_restore()
                    && self
                        .selected_summary()
                        .is_some_and(|summary| summary.is_pending_deletion())
                {
                    items.push(self.write_item(
                        SecretMenuItem::new(
                            SecretMenuAction::Restore,
                            dbflux_i18n::t!("document.secret_store.restore.button").into(),
                            AppIcon::RotateCcw,
                        ),
                        self.can_restore_selected(),
                        false,
                    ));
                }

                if self.supports_delete() {
                    items.push(
                        self.write_item(
                            SecretMenuItem::new(
                                SecretMenuAction::Delete,
                                dbflux_i18n::t!("document.secret_store.delete.button").into(),
                                AppIcon::Delete,
                            )
                            .danger()
                            .with_shortcut(Command::Delete),
                            self.can_delete_selected(),
                            true,
                        ),
                    );
                }

                items
            }
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

    /// A write entry: enabled when `allowed`; otherwise disabled with the
    /// reason the secret's own state gives, when it has one. `refuse_state`
    /// asks for that reason (a restore is for a secret whose deletion is
    /// scheduled, so that state is no reason to refuse it).
    fn write_item(
        &self,
        item: SecretMenuItem,
        allowed: bool,
        refuse_state: bool,
    ) -> SecretMenuItem {
        if allowed {
            return item;
        }

        match refuse_state.then(|| self.write_refusal_reason()).flatten() {
            Some(reason) => item.disabled_with_reason(reason.into()),
            None => SecretMenuItem {
                disabled: true,
                ..item
            },
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
            shortcut: menu_shortcut(Command::ToggleValuePanel),
            detail,
            disabled,
            danger: false,
        }
    }

    /// Why the resource identifier cannot be copied now: still arriving,
    /// failed to load, or not reported by the store.
    pub(super) fn resource_id_unavailable_reason(&self) -> String {
        match &self.details {
            DetailsState::Loaded(_) => {
                dbflux_i18n::t!("document.secret_store.context_menu.arn_missing")
            }
            DetailsState::Failed(error) => dbflux_i18n::t!(
                "document.secret_store.context_menu.arn_failed",
                error = error.as_str()
            ),
            _ => dbflux_i18n::t!("document.secret_store.context_menu.arn_loading"),
        }
        .to_string()
    }

    /// Copy ARN is only meaningful once the describe delivered the resource
    /// identifier. Until then it stays disabled and names what is missing —
    /// an identifier still arriving is not the same as a describe that
    /// failed, and a failure is said the way the value section says its own.
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

        let reason: SharedString = self.resource_id_unavailable_reason().into();

        SecretMenuItem::new(
            SecretMenuAction::CopyArn,
            dbflux_i18n::t!("document.secret_store.action.copy_resource_id").into(),
            AppIcon::Copy,
        )
        .disabled_with_reason(reason)
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
                #[expect(
                    clippy::indexing_slicing,
                    reason = "selected_index starts at 0 on open and the MenuUp/MenuDown arms keep it modulo menu.items.len(), which this dispatch does not re-check"
                )]
                let selected = self.context_menu.as_ref().map(|menu| {
                    (
                        menu.items[menu.selected_index].action,
                        menu.items[menu.selected_index].disabled,
                    )
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
            SecretMenuAction::EditValue => {
                // The editor takes the keyboard when it opens; the common
                // refocus below would take it straight back.
                self.start_value_edit(window, cx);
                return;
            }
            SecretMenuAction::Delete => {
                // The dialog takes the keyboard when it opens; the common
                // refocus below would take it straight back.
                self.open_delete(window, cx);
                return;
            }
            SecretMenuAction::RotateNow => self.rotate_selected_now(cx),
            SecretMenuAction::EditTags => {
                // The dialog takes the keyboard when it opens; the common
                // refocus below would take it straight back.
                self.open_edit_tags(window, cx);
                return;
            }
            SecretMenuAction::Restore => self.restore_selected(cx),
            SecretMenuAction::RetrieveValue => self.retrieve_selected_value(cx),
            SecretMenuAction::CopyName => self.copy_selected_name(cx),
            SecretMenuAction::CopyArn => self.copy_resource_id(cx),
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
