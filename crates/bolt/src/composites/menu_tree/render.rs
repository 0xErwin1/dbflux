//! Drawing a cascading menu.
//!
//! The cascade is one absolutely positioned panel per open level, positioned by
//! [`place_flyout`] rather than by an anchoring helper, because a submenu has to
//! flip horizontally and scroll internally and the anchor helper only snaps.
//! Panels are siblings, so a scrolling parent never clips the flyout that hangs
//! off it, and the child always paints over the parent.
//!
//! The surface that owns the menu supplies the state ([`MenuNav`]), the layout
//! and one event callback. It keeps no closure in the model: a row reports its
//! own `id` back, and the surface maps that to its action.

use std::rc::Rc;

use gpui::prelude::*;
use gpui::{AnyElement, App, Bounds, ElementId, Pixels, Point, SharedString, Size, div, px, size};

use crate::composites::menu_item::{
    menu_frame, menu_row_with_ring, render_menu_header, render_separator,
};
use crate::composites::menu_tree::model::{MenuNode, level};
use crate::composites::menu_tree::nav::MenuNav;
use crate::composites::menu_tree::placement::{
    FlyoutSpec, LevelMetrics, level_height, place_flyout, row_height,
};
use crate::fonts;
use crate::tokens::{Borders, MenuMetrics};

/// Where a cascade starts and the rect it stays inside.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MenuTreeLayout {
    /// Top-left of the root panel, in the coordinate space of the layer the
    /// cascade is mounted in.
    pub origin: Point<Pixels>,
    /// Width of the root panel. A submenu row sets the width of its own flyout.
    pub width: Pixels,
    /// The rect flyouts are flipped and clamped against: the surface hosting the
    /// menu.
    pub viewport: Size<Pixels>,
    /// Whether the highlight follows the keyboard, which strokes the focus ring
    /// on it.
    pub keyboard_focus: bool,
}

/// What a cascade reports to the surface that owns it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MenuTreeEvent {
    /// A leaf row was activated. `id` is the caller's own value; a row without
    /// one reports its `path`, so the caller can map either way.
    Activated {
        id: Option<SharedString>,
        path: Vec<usize>,
    },
    /// A submenu row was clicked: open its flyout.
    Opened { level: usize, index: usize },
    /// The pointer entered a row: the surface starts its hover timer.
    Hovered { level: usize, index: usize },
}

/// Draws every open level of a cascade.
///
/// Mount the result inside a full-size overlay, behind
/// [`crate::composites::render_menu_overlay`] so an outside click dismisses it.
pub fn render_menu_tree(
    panel_id: &str,
    nodes: &[MenuNode],
    nav: &MenuNav,
    layout: MenuTreeLayout,
    on_event: impl Fn(MenuTreeEvent, &mut App) + 'static,
    cx: &App,
) -> AnyElement {
    let on_event: Rc<dyn Fn(MenuTreeEvent, &mut App)> = Rc::new(on_event);
    let depth = nav.cursor().depth();

    if depth == 0 {
        return div().into_any_element();
    }

    let metrics = level_metrics(cx);
    let mut panels: Vec<AnyElement> = Vec::new();
    let mut path: Vec<usize> = Vec::new();
    let mut parent: Option<Bounds<Pixels>> = None;
    let mut parent_rows: &[MenuNode] = &[];

    for level_index in 0..depth {
        let rows = level(nodes, &path);

        if rows.is_empty() {
            break;
        }

        let (frame, scroll_cap) = place_level(
            rows,
            parent,
            parent_rows,
            nav,
            &metrics,
            layout,
            level_index,
        );
        let highlighted = nav.cursor().highlight(level_index);
        let keyboard_cursor = layout.keyboard_focus && level_index + 1 == depth;

        let elements = rows
            .iter()
            .enumerate()
            .map(|(index, node)| {
                render_row(RowContext {
                    panel_id,
                    level: level_index,
                    level_path: &path,
                    index,
                    node,
                    selected: highlighted == Some(index),
                    focus_ring: keyboard_cursor && highlighted == Some(index),
                    on_event: &on_event,
                    cx,
                })
            })
            .collect::<Vec<AnyElement>>();

        let level_selector = format!("{panel_id}-level-{level_index}");

        panels.push(
            menu_frame(cx)
                .debug_selector(move || level_selector.clone())
                .id(ElementId::Name(
                    format!("{panel_id}-level-{level_index}").into(),
                ))
                .absolute()
                .left(frame.origin.x)
                .top(frame.origin.y)
                .w(frame.size.width)
                .when_some(scroll_cap, |panel, cap| {
                    panel.max_h(cap).overflow_y_scroll()
                })
                .occlude()
                .children(elements)
                .into_any_element(),
        );

        let Some(index) = highlighted else {
            break;
        };

        path.push(index);
        parent = Some(frame);
        parent_rows = rows;
    }

    div()
        .absolute()
        .top_0()
        .left_0()
        .size_full()
        .children(panels)
        .into_any_element()
}

/// The frame of one level, and the height cap it scrolls inside when it does not
/// fit in the surface.
fn place_level(
    rows: &[MenuNode],
    parent: Option<Bounds<Pixels>>,
    parent_rows: &[MenuNode],
    nav: &MenuNav,
    metrics: &LevelMetrics,
    layout: MenuTreeLayout,
    level_index: usize,
) -> (Bounds<Pixels>, Option<Pixels>) {
    let height = level_height(rows, metrics);

    let Some(parent_panel) = parent else {
        let cap = layout.viewport.height - MenuMetrics::FLYOUT_MARGIN * 2.0;
        let frame = Bounds {
            origin: layout.origin,
            size: size(layout.width, height),
        };

        return (frame, (height > cap).then_some(cap));
    };

    let anchor = nav.cursor().highlight(level_index - 1).unwrap_or(0);
    let width = parent_rows
        .get(anchor)
        .and_then(|node| node.children_width)
        .map_or(MenuMetrics::SUBMENU_WIDTH, px);
    let row_top =
        parent_panel.origin.y + metrics.padding_y + height_above(parent_rows, anchor, metrics);

    let placement = place_flyout(
        parent_panel,
        row_top,
        metrics.padding_y,
        FlyoutSpec {
            size: size(width, height),
            viewport: layout.viewport,
            margin: MenuMetrics::FLYOUT_MARGIN,
            overlap: MenuMetrics::SUBMENU_OVERLAP,
        },
    );
    let frame = Bounds {
        origin: placement.origin,
        size: size(width, height),
    };

    (frame, placement.scrolls.then_some(placement.max_height))
}

/// Everything one row needs to draw itself and report back.
struct RowContext<'a> {
    panel_id: &'a str,
    level: usize,
    level_path: &'a [usize],
    index: usize,
    node: &'a MenuNode,
    selected: bool,
    focus_ring: bool,
    on_event: &'a Rc<dyn Fn(MenuTreeEvent, &mut App)>,
    cx: &'a App,
}

fn render_row(context: RowContext<'_>) -> AnyElement {
    let RowContext {
        panel_id,
        level: level_index,
        level_path,
        index,
        node,
        selected,
        focus_ring,
        on_event,
        cx,
    } = context;

    let row_id = node
        .element_id
        .clone()
        .unwrap_or_else(|| SharedString::from(format!("{panel_id}-{level_index}-{index}")));

    if node.item.is_separator {
        let selector = format!("{row_id}-separator");

        return render_separator(cx)
            .debug_selector(move || selector.clone())
            .into_any_element();
    }

    if node.item.is_header {
        let selector = format!("{row_id}-header");

        return render_menu_header(&node.item, cx)
            .debug_selector(move || selector.clone())
            .into_any_element();
    }

    let mut path = level_path.to_vec();
    path.push(index);

    let opens_flyout = node.opens_flyout();
    let id = node.id.clone();
    let on_click = on_event.clone();
    let on_hover = on_event.clone();

    menu_row_with_ring(row_id, &node.item, selected, focus_ring, cx)
        .when(node.is_selectable(), |row| {
            row.on_click(move |_, _, cx| {
                let event = if opens_flyout {
                    MenuTreeEvent::Opened {
                        level: level_index,
                        index,
                    }
                } else {
                    MenuTreeEvent::Activated {
                        id: id.clone(),
                        path: path.clone(),
                    }
                };

                on_click(event, cx);
            })
            .on_mouse_move(move |_, _, cx| {
                on_hover(
                    MenuTreeEvent::Hovered {
                        level: level_index,
                        index,
                    },
                    cx,
                );
            })
        })
        .into_any_element()
}

/// The space the rows above `index` take, which is where the flyout's top edge
/// has to land.
fn height_above(rows: &[MenuNode], index: usize, metrics: &LevelMetrics) -> Pixels {
    rows.iter()
        .take(index)
        .fold(px(0.0), |total, node| total + row_height(node, metrics))
}

/// The row heights of the current theme, resolved from the menu tokens.
fn level_metrics(cx: &App) -> LevelMetrics {
    LevelMetrics {
        row_height: fonts::ui_px(cx, MenuMetrics::ROW_HEIGHT),
        detail_row_height: fonts::ui_px(cx, MenuMetrics::DETAIL_ROW_HEIGHT),
        header_height: fonts::ui_px(cx, MenuMetrics::HEADER_ICON)
            + MenuMetrics::HEADER_PADDING_TOP
            + MenuMetrics::HEADER_PADDING_BOTTOM,
        separator_height: Borders::THIN + MenuMetrics::SEPARATOR_MARGIN_Y * 2.0,
        padding_y: MenuMetrics::PADDING_Y,
    }
}
