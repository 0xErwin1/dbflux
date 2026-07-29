//! Right-click context menu for the object-browser listing.
//!
//! Every entry dispatches through the flows the toolbar, the preview action
//! bar and the keymap already use — the menu adds no mutation logic of its
//! own, so the unsaved-edits guard, the confirmations and the audit trail all
//! stay on their existing paths. The menu acts on the row that was
//! right-clicked, which is also selected on open, so the keyboard cursor and
//! the menu never disagree about the target.
//!
//! Shape follows the key-value document's menu (`key_value/context_menu.rs`
//! and `key_value/view.rs`): a deferred overlay that dismisses on an outside
//! click and positions the panel at the click, in document coordinates.

use super::tree::ObjectTreeNodeId;
use super::{ObjectAction, ObjectBrowserDocument, ObjectBrowserFocusMode};
use dbflux_app::keymap::Command;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text, surface_raised};
use dbflux_components::tokens::{FontSizes, Heights, Radii, Spacing};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Width of the popup panel. Wide enough for the longest entry ("New folder
/// inside") without wrapping.
const MENU_WIDTH: Pixels = px(200.0);

pub(super) struct ObjectContextMenu {
    pub target: ObjectTreeNodeId,
    /// Click position in window coordinates; converted to document
    /// coordinates at render time via `panel_origin`.
    pub position: Point<Pixels>,
    pub items: Vec<ObjectMenuItem>,
    pub selected_index: usize,
}

pub(super) struct ObjectMenuItem {
    pub label: SharedString,
    pub action: ObjectMenuAction,
    pub icon: AppIcon,
    pub is_danger: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ObjectMenuAction {
    Preview,
    Download,
    Rename,
    Presign,
    CopyUri,
    DeleteObject,
    /// Tree mode: flip the folder's disclosure in place.
    ToggleNode,
    /// Per-level mode: move the listing into the folder.
    OpenPrefix,
    NewFolderInside,
    DeletePrefix,
}

impl ObjectBrowserDocument {
    fn build_object_menu_items(&self) -> Vec<ObjectMenuItem> {
        vec![
            ObjectMenuItem {
                label: "Preview".into(),
                action: ObjectMenuAction::Preview,
                icon: AppIcon::Eye,
                is_danger: false,
            },
            ObjectMenuItem {
                label: "Download".into(),
                action: ObjectMenuAction::Download,
                icon: AppIcon::Download,
                is_danger: false,
            },
            ObjectMenuItem {
                label: "Rename".into(),
                action: ObjectMenuAction::Rename,
                icon: AppIcon::Pencil,
                is_danger: false,
            },
            ObjectMenuItem {
                label: "Presign".into(),
                action: ObjectMenuAction::Presign,
                icon: AppIcon::Link2,
                is_danger: false,
            },
            ObjectMenuItem {
                label: "Copy S3 URI".into(),
                action: ObjectMenuAction::CopyUri,
                icon: AppIcon::Copy,
                is_danger: false,
            },
            ObjectMenuItem {
                label: "Delete".into(),
                action: ObjectMenuAction::DeleteObject,
                icon: AppIcon::Delete,
                is_danger: true,
            },
        ]
    }

    /// Folder entries. The first one mirrors what a click on the row does in
    /// the current mode, so the menu never offers an action the listing does
    /// not support.
    fn build_prefix_menu_items(&self, prefix: &str) -> Vec<ObjectMenuItem> {
        let disclosure = if self.tree.is_tree_mode() {
            ObjectMenuItem {
                label: if self.tree.is_expanded(prefix) {
                    "Collapse".into()
                } else {
                    "Expand".into()
                },
                action: ObjectMenuAction::ToggleNode,
                icon: if self.tree.is_expanded(prefix) {
                    AppIcon::ChevronDown
                } else {
                    AppIcon::ChevronRight
                },
                is_danger: false,
            }
        } else {
            ObjectMenuItem {
                label: "Open".into(),
                action: ObjectMenuAction::OpenPrefix,
                icon: AppIcon::Folder,
                is_danger: false,
            }
        };

        vec![
            disclosure,
            ObjectMenuItem {
                label: "New folder inside".into(),
                action: ObjectMenuAction::NewFolderInside,
                icon: AppIcon::Plus,
                is_danger: false,
            },
            ObjectMenuItem {
                label: "Copy S3 URI".into(),
                action: ObjectMenuAction::CopyUri,
                icon: AppIcon::Copy,
                is_danger: false,
            },
            ObjectMenuItem {
                label: "Delete folder".into(),
                action: ObjectMenuAction::DeletePrefix,
                icon: AppIcon::Delete,
                is_danger: true,
            },
        ]
    }

    /// Opens the menu for the right-clicked row. The row is selected first so
    /// the entries that read the selection (and the keyboard cursor the user
    /// is left with afterwards) agree with what was right-clicked.
    pub(super) fn open_context_menu(
        &mut self,
        target: ObjectTreeNodeId,
        position: Point<Pixels>,
        cx: &mut Context<Self>,
    ) {
        let items = match &target {
            ObjectTreeNodeId::Object(_) => self.build_object_menu_items(),
            ObjectTreeNodeId::Prefix(prefix) => self.build_prefix_menu_items(prefix),
        };

        self.select_node(target.clone(), cx);
        self.focus_mode = ObjectBrowserFocusMode::Listing;

        self.context_menu = Some(ObjectContextMenu {
            target,
            position,
            items,
            selected_index: 0,
        });

        cx.notify();
    }

    pub(super) fn close_context_menu(&mut self, cx: &mut Context<Self>) {
        self.context_menu = None;
        cx.notify();
    }

    pub(super) fn dispatch_menu_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(item_count) = self.context_menu.as_ref().map(|menu| menu.items.len()) else {
            return false;
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
                if let Some(menu) = self.context_menu.take() {
                    let action = menu.items[menu.selected_index].action;
                    self.execute_menu_action(action, menu.target, window, cx);
                }
                true
            }
            Command::MenuBack | Command::Cancel => {
                self.close_context_menu(cx);
                true
            }
            // The menu owns the keyboard while it is up: nothing may move the
            // listing under a target the user picked with a right click.
            _ => true,
        }
    }

    pub(super) fn execute_menu_action(
        &mut self,
        action: ObjectMenuAction,
        target: ObjectTreeNodeId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.context_menu = None;

        match (action, target) {
            (ObjectMenuAction::Preview, ObjectTreeNodeId::Object(key)) => {
                self.open_preview(key, cx)
            }
            (ObjectMenuAction::Download, ObjectTreeNodeId::Object(key)) => {
                self.download_object(key, cx)
            }
            (ObjectMenuAction::Rename, ObjectTreeNodeId::Object(key)) => {
                self.request_rename_object(key, window, cx)
            }
            (ObjectMenuAction::Presign, ObjectTreeNodeId::Object(key)) => {
                self.request_object_action(ObjectAction::Presign { key }, cx)
            }
            (ObjectMenuAction::DeleteObject, ObjectTreeNodeId::Object(key)) => {
                self.request_delete_object(key, cx)
            }
            (ObjectMenuAction::ToggleNode, ObjectTreeNodeId::Prefix(prefix)) => {
                self.toggle_tree_node(prefix, cx)
            }
            (ObjectMenuAction::OpenPrefix, ObjectTreeNodeId::Prefix(prefix)) => {
                self.navigate_to_prefix(prefix, window, cx)
            }
            (ObjectMenuAction::NewFolderInside, ObjectTreeNodeId::Prefix(prefix)) => {
                self.request_new_folder_in(prefix, cx)
            }
            (ObjectMenuAction::DeletePrefix, ObjectTreeNodeId::Prefix(prefix)) => {
                self.request_delete_prefix(prefix, cx)
            }
            // A key-only action on a folder (or the reverse) is unreachable:
            // the item lists are built per target kind.
            (ObjectMenuAction::CopyUri, ObjectTreeNodeId::Object(key)) => {
                self.copy_object_uri(&key, cx)
            }
            (ObjectMenuAction::CopyUri, ObjectTreeNodeId::Prefix(prefix)) => {
                self.copy_object_uri(&prefix, cx)
            }
            _ => {}
        }

        cx.notify();
    }

    /// Deferred popup panel, positioned at the click and dismissed by any
    /// click outside it.
    pub(super) fn render_context_menu(
        &self,
        menu: &ObjectContextMenu,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let selected_index = menu.selected_index;
        let menu_x = menu.position.x - self.panel_origin.x;
        let menu_y = menu.position.y - self.panel_origin.y;

        let entries: Vec<AnyElement> = menu
            .items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let is_selected = index == selected_index;
                let is_danger = item.is_danger;

                let foreground = if is_danger {
                    theme.danger
                } else if is_selected {
                    theme.accent_foreground
                } else {
                    theme.foreground
                };

                let action = item.action;
                let target = menu.target.clone();

                div()
                    .id(SharedString::from(format!(
                        "object-browser-menu-item-{index}"
                    )))
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .h(Heights::ROW_COMPACT)
                    .px(Spacing::SM)
                    .mx(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .text_size(FontSizes::SM)
                    .when(is_selected, |d| {
                        d.bg(if is_danger {
                            theme.danger.opacity(0.1)
                        } else {
                            theme.accent
                        })
                    })
                    .when(!is_selected, |d| {
                        d.hover(|d| {
                            d.bg(if is_danger {
                                theme.danger.opacity(0.1)
                            } else {
                                theme.secondary
                            })
                        })
                    })
                    .on_mouse_move(cx.listener(move |this, _, _, cx| {
                        if let Some(menu) = this.context_menu.as_mut()
                            && menu.selected_index != index
                        {
                            menu.selected_index = index;
                            cx.notify();
                        }
                    }))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.execute_menu_action(action, target.clone(), window, cx);
                    }))
                    .child(
                        Icon::new(item.icon)
                            .size(Heights::ICON_SM)
                            .color(if is_danger {
                                theme.danger
                            } else if is_selected {
                                theme.accent_foreground
                            } else {
                                theme.muted_foreground
                            }),
                    )
                    .child(Text::caption(item.label.clone()).color(foreground))
                    .into_any_element()
            })
            .collect();

        deferred(
            div()
                .id("object-browser-context-menu-overlay")
                .absolute()
                .top_0()
                .left_0()
                .size_full()
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(|this, _, _, cx| {
                        this.close_context_menu(cx);
                    }),
                )
                .on_mouse_down(
                    MouseButton::Right,
                    cx.listener(|this, _, _, cx| {
                        this.close_context_menu(cx);
                    }),
                )
                .child(
                    surface_raised(cx)
                        .id("object-browser-context-menu")
                        .absolute()
                        .left(menu_x)
                        .top(menu_y)
                        .w(MENU_WIDTH)
                        .shadow_lg()
                        .py(Spacing::XS)
                        .occlude()
                        .on_mouse_down(MouseButton::Left, |_, _, cx| {
                            cx.stop_propagation();
                        })
                        .children(entries),
                ),
        )
        .with_priority(1)
    }
}
