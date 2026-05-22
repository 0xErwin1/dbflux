//! `KeyValueView` — view-layer helpers for `KeyValueDocument`.
//!
//! This module owns the pure rendering utilities that are used exclusively
//! by `render.rs` but carry no document state:
//!
//! - `icon_button_base` — compact icon-only button primitive.
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
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text, overlay_bg, surface_panel, surface_raised};
use dbflux_components::tokens::{FontSizes, Heights, Radii, Spacing};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

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
    let theme = cx.theme();
    let btn_hover = theme.muted;

    div()
        .id("kv-delete-modal-overlay")
        .absolute()
        .inset_0()
        .bg(overlay_bg(theme))
        .flex()
        .items_center()
        .justify_center()
        .on_mouse_down(MouseButton::Left, |_, _, cx| {
            cx.stop_propagation();
        })
        .child(
            surface_panel(cx)
                .rounded(Radii::MD)
                .min_w(px(300.0))
                .flex()
                .flex_col()
                .gap(Spacing::MD)
                .p(Spacing::MD)
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .child(Icon::new(AppIcon::TriangleAlert).size(px(20.0)).warning())
                        .child(Text::heading(title.to_string())),
                )
                .child(Text::muted(message.to_string()))
                .child(
                    div()
                        .flex()
                        .justify_end()
                        .gap(Spacing::SM)
                        .child(
                            div()
                                .id("kv-delete-cancel-btn")
                                .flex()
                                .items_center()
                                .gap_1()
                                .px(Spacing::SM)
                                .py(Spacing::XS)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .bg(theme.secondary)
                                .hover(move |d| d.bg(btn_hover))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.pending_key_delete = None;
                                    this.pending_member_delete = None;
                                    cx.notify();
                                }))
                                .child(Icon::new(AppIcon::X).size(Heights::ICON_SM).muted())
                                .child(Text::caption("Cancel")),
                        )
                        .child(
                            div()
                                .id("kv-delete-confirm-btn")
                                .flex()
                                .items_center()
                                .gap_1()
                                .px(Spacing::SM)
                                .py(Spacing::XS)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .bg(theme.danger)
                                .hover(|d| d.opacity(0.9))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    if this.pending_key_delete.is_some() {
                                        this.confirm_delete_key(cx);
                                    } else if this.pending_member_delete.is_some() {
                                        this.confirm_delete_member(cx);
                                    }
                                }))
                                .child(
                                    Icon::new(AppIcon::Delete)
                                        .size(Heights::ICON_SM)
                                        .color(theme.background),
                                )
                                .child(Text::caption("Delete").color(theme.background)),
                        ),
                ),
        )
}

/// Renders the deferred right-click context menu overlay.
///
/// The overlay intercepts all mouse and keyboard input until the menu is
/// dismissed. `panel_origin` is used to convert absolute screen coordinates
/// to coordinates relative to the document panel.
pub(super) fn render_kv_context_menu(
    menu: &KvContextMenu,
    menu_focus: &FocusHandle,
    panel_origin: Point<Pixels>,
    cx: &mut Context<KeyValueDocument>,
) -> impl IntoElement {
    let theme = cx.theme();
    let menu_width = px(180.0);
    let menu_x = menu.position.x - panel_origin.x;
    let menu_y = menu.position.y - panel_origin.y;
    let selected_index = menu.selected_index;

    let menu_items: Vec<AnyElement> = menu
        .items
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            let is_selected = idx == selected_index;
            let is_danger = item.is_danger;
            let label = item.label;
            let icon = item.icon;
            let action = item.action;

            let label_color = if is_danger {
                theme.danger
            } else if is_selected {
                theme.accent_foreground
            } else {
                theme.foreground
            };

            div()
                .id(SharedString::from(label))
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
                    if let Some(ref mut m) = this.context_menu
                        && m.selected_index != idx
                    {
                        m.selected_index = idx;
                        cx.notify();
                    }
                }))
                .on_click(cx.listener(move |this, _, window, cx| {
                    if let Some(m) = this.context_menu.take() {
                        let target = m.target;
                        this.execute_menu_action(action, target, window, cx);
                    }
                }))
                .child(Icon::new(icon).size(Heights::ICON_SM).color(if is_danger {
                    theme.danger
                } else if is_selected {
                    theme.accent_foreground
                } else {
                    theme.muted_foreground
                }))
                .child(Text::caption(label).color(label_color))
                .into_any_element()
        })
        .collect();

    deferred(
        div()
            .id("kv-context-menu-overlay")
            .absolute()
            .top_0()
            .left_0()
            .size_full()
            .track_focus(menu_focus)
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                use dbflux_app::keymap::KeyChord;
                use dbflux_ui_base::keymap::{default_keymap, key_chord_from_gpui};

                let chord = key_chord_from_gpui(&event.keystroke);
                let keymap = default_keymap();

                if let Some(cmd) =
                    keymap.resolve(dbflux_app::keymap::ContextId::ContextMenu, &chord)
                    && this.dispatch_menu_command(cmd, window, cx)
                {
                    cx.stop_propagation();
                }
            }))
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
                surface_raised(cx)
                    .id("kv-context-menu")
                    .absolute()
                    .left(menu_x)
                    .top(menu_y)
                    .w(menu_width)
                    .shadow_lg()
                    .py(Spacing::XS)
                    .occlude()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| {
                        cx.stop_propagation();
                    })
                    .children(menu_items),
            ),
    )
    .with_priority(1)
}

/// Compact icon-only button used in the KV document toolbar and value panel header.
///
/// Returns a `Stateful<Div>` so the caller can attach `.on_mouse_down` handlers.
pub(super) fn icon_button_base(
    id: impl Into<ElementId>,
    icon: AppIcon,
    theme: &gpui_component::Theme,
) -> Stateful<Div> {
    let foreground = theme.muted_foreground;
    let hover_bg = theme.secondary;

    div()
        .id(id.into())
        .w(Heights::ICON_MD)
        .h(Heights::ICON_MD)
        .flex()
        .items_center()
        .justify_center()
        .rounded(Radii::SM)
        .cursor_pointer()
        .hover(move |d| d.bg(hover_bg))
        .child(Icon::new(icon).small().color(foreground))
}
