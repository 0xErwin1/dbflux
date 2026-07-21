//! Shared popup-menu helpers for floating context/kebab menus.
//!
//! This module hosts the two pieces every floating menu in the app needs:
//!
//! 1. A full-screen transparent overlay that dismisses the menu on any outside
//!    click (`render_menu_overlay`).
//! 2. A higher-level container that takes a slice of [`MenuItem`]s and wires
//!    per-item click/hover callbacks (`render_menu_items`).
//!
//! Previously these wrappers lived in `dbflux_ui::ui::components::context_menu`,
//! which made them unreachable from the document/sidebar crates that need the
//! same style (sidebar's `⋯` menu, dashboard panel kebab, tab context menu).
//! Moving them down to `dbflux_components` lets every UI crate reuse the exact
//! same chrome — see CLAUDE.md "Generic Deduplication Patterns".

use crate::composites::menu_item::{
    MenuItem, render_menu_container, render_menu_item, render_separator,
};
use gpui::prelude::*;
use gpui::{AnyElement, App, Div, ElementId, MouseButton, MouseDownEvent, Stateful, div};

/// Full-screen transparent overlay that dismisses the menu on any click outside.
///
/// Mount this as a sibling of the menu container (typically inside `deferred()`)
/// so left/right mouse-down events anywhere on the window invoke `on_dismiss`.
pub fn render_menu_overlay(
    id: impl Into<ElementId>,
    on_dismiss: impl Fn(&MouseDownEvent, &mut App) + 'static,
) -> Stateful<Div> {
    let on_dismiss = std::rc::Rc::new(on_dismiss);
    let on_dismiss_right = on_dismiss.clone();

    div()
        .id(id)
        .absolute()
        .top_0()
        .left_0()
        .size_full()
        .on_mouse_down(MouseButton::Left, move |event, _, cx| {
            on_dismiss(event, cx);
        })
        .on_mouse_down(MouseButton::Right, move |event, _, cx| {
            on_dismiss_right(event, cx);
        })
}

/// Render the popup menu container for a slice of [`MenuItem`]s.
///
/// `panel_id` is a stable identifier used to construct per-item element IDs.
/// `selected_index` highlights one entry; pass `None` to leave nothing
/// highlighted. `on_click` / `on_hover` are invoked with the item index.
pub fn render_menu_items(
    panel_id: &str,
    items: &[MenuItem],
    selected_index: Option<usize>,
    on_click: impl Fn(usize, &mut App) + 'static,
    on_hover: impl Fn(usize, &mut App) + 'static,
    cx: &mut App,
) -> Div {
    let on_click = std::rc::Rc::new(on_click);
    let on_hover = std::rc::Rc::new(on_hover);

    let children: Vec<AnyElement> = items
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            if item.is_separator {
                let separator_selector = format!("{}-separator-{}", panel_id, idx);
                return render_separator(cx)
                    .debug_selector(move || separator_selector.clone())
                    .into_any_element();
            }

            let on_click = on_click.clone();
            let on_hover = on_hover.clone();

            render_menu_item(
                panel_id,
                item,
                idx,
                selected_index == Some(idx),
                move |_, _, cx| on_click(idx, cx),
                move |cx| on_hover(idx, cx),
                cx,
            )
            .into_any_element()
        })
        .collect();

    render_menu_container(children, cx)
}
