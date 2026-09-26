//! `KeyValueView` — view-layer helpers for `KeyValueDocument`.
//!
//! This module owns the pure rendering utilities that are used exclusively
//! by `render.rs` but carry no document state:
//!
//! - `render_delete_confirm_modal` — overlay confirming key/member deletion.
//! - `render_kv_context_menu` — deferred right-click context menu overlay.
//!
//! `KeyValueDocument` self-renders via `impl Render` in `render.rs`.
//! `KeyValueView` holds the document entity reference and is the named
//! view-layer boundary; additional view-only state (selection animations,
//! scroll position cache, per-view overrides) can be absorbed here in
//! future arcs without touching the data model.

use super::KeyValueDocument;
use super::context_menu::{KvContextMenu, KvMenuAction};
use super::render::update_document;
use dbflux_components::composites::{MenuItem, render_menu_items};
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::Modal;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::Spacing;
use gpui::prelude::*;
use gpui::*;

// ---------------------------------------------------------------------------
// View-layer boundary
// ---------------------------------------------------------------------------

/// View-layer entity shell for `KeyValueDocument`.
///
/// `KeyValueDocument` self-renders through its own `impl Render`; this struct
/// holds the document entity reference and is reserved for future extraction
/// of view-only state (selection, animations, per-view overrides) without
/// coupling them to the data model.
#[allow(dead_code)]
pub struct KeyValueView {
    pub(super) document: Entity<KeyValueDocument>,
}

// ---------------------------------------------------------------------------
// Render helpers (called from render.rs)
// ---------------------------------------------------------------------------

/// Renders a floating delete confirmation modal for key or member deletion.
///
/// The caller must ensure either `pending_key_delete` or `pending_member_delete`
/// is `Some` before constructing the title/message strings.
pub(super) fn render_delete_confirm_modal(
    title: &str,
    message: &str,
    cx: &mut Context<KeyValueDocument>,
) -> impl IntoElement {
    let footer = div()
        .flex()
        .gap(Spacing::SM)
        .child(
            Button::new(
                "kv-delete-cancel-btn",
                dbflux_i18n::t!("document.key_value.render.delete_confirm.cancel"),
            )
            .icon(AppIcon::X)
            .on_click(cx.listener(|this, _, _, cx| {
                this.pending_key_delete = None;
                this.pending_member_delete = None;
                cx.notify();
            })),
        )
        .child(
            Button::new(
                "kv-delete-confirm-btn",
                dbflux_i18n::t!("document.key_value.render.delete_confirm.delete"),
            )
            .danger()
            .icon(AppIcon::Delete)
            .on_click(cx.listener(|this, _, _, cx| {
                if this.pending_key_delete.is_some() {
                    this.confirm_delete_key(cx);
                } else if this.pending_member_delete.is_some() {
                    this.confirm_delete_member(cx);
                }
            })),
        );

    Modal::new(title.to_string())
        .id("kv-delete-modal-overlay")
        .danger()
        .icon(AppIcon::TriangleAlert)
        .width(px(420.0))
        .body(Text::body(message.to_string()))
        .footer(footer)
}

/// Renders the deferred right-click context menu overlay.
///
/// The overlay intercepts all mouse and keyboard input until the menu is
/// dismissed. `panel_origin` is used to convert absolute screen coordinates
/// to coordinates relative to the document panel. A chosen item is queued
/// and run on the next render, which has the window the action needs.
pub(super) fn render_kv_context_menu(
    menu: &KvContextMenu,
    menu_focus: &FocusHandle,
    panel_origin: Point<Pixels>,
    cx: &mut Context<KeyValueDocument>,
) -> impl IntoElement {
    let menu_x = menu.position.x - panel_origin.x;
    let menu_y = menu.position.y - panel_origin.y;
    let target = menu.target;

    let items: Vec<MenuItem> = menu
        .items
        .iter()
        .map(|item| {
            let entry = MenuItem::new(item.label.clone()).icon(item.icon);
            if item.is_danger {
                entry.danger()
            } else {
                entry
            }
        })
        .collect();
    let actions: Vec<KvMenuAction> = menu.items.iter().map(|item| item.action).collect();

    let click_entity = cx.entity().downgrade();
    let hover_entity = cx.entity().downgrade();

    let menu_element = render_menu_items(
        "kv-context-menu",
        &items,
        Some(menu.selected_index),
        move |index, cx| {
            let Some(action) = actions.get(index).copied() else {
                return;
            };

            update_document(&click_entity, cx, |this, cx| {
                this.context_menu = None;
                this.pending_menu_action = Some((action, target));
                cx.notify();
            });
        },
        move |index, cx| {
            update_document(&hover_entity, cx, |this, cx| {
                if let Some(menu) = this.context_menu.as_mut()
                    && menu.selected_index != index
                {
                    menu.selected_index = index;
                    cx.notify();
                }
            });
        },
        cx,
    );

    deferred(
        div()
            .id("kv-context-menu-overlay")
            .absolute()
            .top_0()
            .left_0()
            .size_full()
            .track_focus(menu_focus)
            // The document reports the ContextMenu context while the menu is
            // open, so the keymap's menu keys arrive here first.
            .on_action(cx.listener(
                |this, action: &dbflux_ui_base::keymap::RunCommand, window, cx| {
                    let handled = dbflux_ui_base::keymap::run_command(action)
                        .is_some_and(|command| this.dispatch_menu_command(command, window, cx));

                    if !handled {
                        cx.propagate();
                    }
                },
            ))
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
                    .child(menu_element),
            ),
    )
    .with_priority(1)
}
