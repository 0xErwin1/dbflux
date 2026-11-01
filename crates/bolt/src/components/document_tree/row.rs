//! The row painter of a document tree: one expanded or collapsed node of a
//! document collection, drawn over the display facts the data owner
//! formatted ([`DocumentTreeRowView`]) and the interaction the host owns
//! ([`DocumentTreeRowHost`]).
//!
//! The paint decisions this module pins, asserted in `tests.rs`: the
//! background precedence (cursor, then current match, then match) and its
//! hover variants, and the value's type color per [`ValueTypeClass`].

use std::sync::Arc;

use gpui::{
    App, ClickEvent, Div, ElementId, Entity, Hsla, InteractiveElement, MouseButton, MouseDownEvent,
    ParentElement, Pixels, SharedString, Stateful, StatefulInteractiveElement, Styled, Window, div,
};
use gpui_component::input::InputState;
use gpui_component::{ActiveTheme, Sizable, Theme};

use crate::controls::GpuiInput as Input;
use crate::fonts;
use crate::icon::IconSource;
use crate::primitives::{Icon, Text};
use crate::tokens::{
    ChromeColors, DocumentTreeMetrics, FontSizes, Heights, Radii, Spacing, TreeMetrics,
};

use super::{NodeId, ValueTypeClass, value_type_color};

/// Sizes of a tree row. The tree is the grid surface of document
/// collections, so its rows follow the grid font size rather than the
/// interface size.
#[derive(Clone, Copy)]
pub struct TreeRowMetrics {
    pub row_height: Pixels,
    pub indent: Pixels,
    pub chevron_box: Pixels,
    pub chevron: Pixels,
    pub label_font: Pixels,
    pub value_font: Pixels,
}

impl TreeRowMetrics {
    pub fn for_grid(cx: &App) -> Self {
        Self {
            row_height: fonts::grid_scaled(cx, DocumentTreeMetrics::ROW_HEIGHT),
            indent: fonts::grid_scaled(cx, DocumentTreeMetrics::INDENT),
            chevron_box: fonts::grid_px(cx, Heights::ICON_SM),
            chevron: fonts::grid_px(cx, TreeMetrics::CHEVRON),
            label_font: fonts::grid_px(cx, FontSizes::XS),
            value_font: fonts::grid_px(cx, FontSizes::SM),
        }
    }
}

/// The paint decisions one row derives from its state flags, split from the
/// element builder so every precedence the painter takes is assertable
/// without pixels.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RowPaint {
    /// The row's background: the keyboard cursor, then the current search
    /// match, then any search match, then nothing.
    pub background: Hsla,
    /// The hovered background. A cursor row keeps its own background; the
    /// match washes sit one step above their resting values; an ordinary row
    /// hovers with the secondary wash.
    pub hover_background: Hsla,
    /// The color of the value preview and the type badge: the value's type
    /// color.
    pub value_color: Hsla,
    /// The color of the key text: the chrome tint.
    pub key_color: Hsla,
    /// The color of the key/value separator.
    pub muted_color: Hsla,
}

/// Derive one row's paint decisions from its state flags and its value's
/// class, against the live theme.
pub fn row_paint(
    is_cursor: bool,
    is_current_match: bool,
    is_search_match: bool,
    value_class: ValueTypeClass,
    theme: &Theme,
) -> RowPaint {
    let background = if is_cursor {
        theme.selection
    } else if is_current_match {
        theme.warning.opacity(0.4)
    } else if is_search_match {
        theme.warning.opacity(0.2)
    } else {
        Hsla::transparent_black()
    };

    let hover_background = if is_cursor {
        theme.selection
    } else if is_current_match {
        theme.warning.opacity(0.5)
    } else if is_search_match {
        theme.warning.opacity(0.3)
    } else {
        theme.secondary.opacity(0.5)
    };

    RowPaint {
        background,
        hover_background,
        value_color: value_type_color(value_class, theme),
        key_color: ChromeColors::tint(theme),
        muted_color: theme.muted_foreground,
    }
}

/// What one row draws, already composed and formatted by the data owner: the
/// key, the value text (the collapsed summary, the truncated preview or the
/// full preview — the owner picks), and the state flags the paint reads.
pub struct DocumentTreeRowView {
    /// The node the row draws; the painter derives the element ids from its
    /// path.
    pub id: NodeId,
    /// Depth in the tree; the row indents by one step per level.
    pub depth: usize,
    /// The field name or index label the row leads with.
    pub key: SharedString,
    /// The text the value draws.
    pub value_text: SharedString,
    /// The value's class; decides the value's and the badge's color.
    pub value_class: ValueTypeClass,
    /// The type label the badge draws (`object`, `array`, `string`, …).
    pub type_label: &'static str,
    /// Whether the row can expand (its value is a document or an array).
    pub is_expandable: bool,
    /// Whether the row is expanded.
    pub is_expanded: bool,
    /// Whether the row's scalar value is expanded inline. A fact the row
    /// carries today without a rendering decision of its own.
    pub is_value_expanded: bool,
    /// Whether the row is the keyboard cursor.
    pub is_cursor: bool,
    /// Whether the row is the search match under the cursor.
    pub is_current_match: bool,
    /// Whether the row matches the active search.
    pub is_search_match: bool,
    /// Whether the row's value hosts the inline editor.
    pub is_editing: bool,
    /// The inline editor's input, while the row hosts it.
    pub inline_edit_input: Option<Entity<InputState>>,
}

/// The interaction one row defers to its host: the icons a host supplies and
/// the callbacks the painter must not own. Every callback receives the row's
/// node id and the raw event, so the host keeps the interaction policy.
pub struct DocumentTreeRowHost {
    /// Chevron drawn on expanded rows.
    pub chevron_expanded: IconSource,
    /// Chevron drawn on collapsed rows.
    pub chevron_collapsed: IconSource,
    /// A row was clicked. The event carries the click count, so the host
    /// distinguishes a single click (select) from a double one (activate).
    pub on_row_click: Arc<dyn Fn(&NodeId, &ClickEvent, &mut Window, &mut App)>,
    /// A row was right-clicked; the host opens its context menu at the
    /// event's position.
    pub on_row_context: Arc<dyn Fn(&NodeId, &MouseDownEvent, &mut Window, &mut App)>,
    /// A row's chevron was clicked; the host toggles that node. The click
    /// never reaches the row.
    pub on_chevron_click: Arc<dyn Fn(&NodeId, &ClickEvent, &mut Window, &mut App)>,
    /// A row's value was clicked. One click selects, a double click opens
    /// the inline editor; the click never reaches the row.
    pub on_value_click: Arc<dyn Fn(&NodeId, &ClickEvent, &mut Window, &mut App)>,
}

/// Draw one document tree row.
pub fn render_row(
    view: DocumentTreeRowView,
    host: &DocumentTreeRowHost,
    cx: &App,
) -> Stateful<Div> {
    let theme = cx.theme().clone();
    let metrics = TreeRowMetrics::for_grid(cx);
    let paint = row_paint(
        view.is_cursor,
        view.is_current_match,
        view.is_search_match,
        view.value_class,
        &theme,
    );

    let indent = metrics.indent * view.depth as f32;

    let row_click = host.on_row_click.clone();
    let row_context = host.on_row_context.clone();
    let value_click = host.on_value_click.clone();
    let row_id = view.id.clone();
    let context_id = view.id.clone();
    let value_id = view.id.clone();

    let mut value = div()
        .id(ElementId::Name(format!("value-{:?}", view.id.path).into()))
        .flex_1()
        .overflow_x_hidden()
        .ml(Spacing::XS)
        .text_size(metrics.value_font)
        .text_color(paint.value_color);

    value = if view.is_editing
        && let Some(input) = view.inline_edit_input.clone()
    {
        value
            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                cx.stop_propagation();
            })
            .child(Input::new(&input).small().w_full())
    } else if view.is_expanded {
        // An expanded long value may exceed the row: it draws in a
        // bounded, scrollable block instead of ellipsizing.
        value
            .max_h(DocumentTreeMetrics::EXPANDED_VALUE_MAX_HEIGHT)
            .overflow_y_scroll()
            .cursor_pointer()
            .child(view.value_text.clone())
            .on_click(move |event, window, cx| {
                cx.stop_propagation();
                value_click(&value_id, event, window, cx);
            })
    } else {
        value
            .text_ellipsis()
            .whitespace_nowrap()
            .cursor_pointer()
            .child(view.value_text.clone())
            .on_click(move |event, window, cx| {
                cx.stop_propagation();
                value_click(&value_id, event, window, cx);
            })
    };

    div()
        .id(ElementId::Name(
            format!("tree-row-{:?}", view.id.path).into(),
        ))
        .h(metrics.row_height)
        .w_full()
        .flex()
        .items_center()
        .pl(indent)
        .pr(Spacing::SM)
        .bg(paint.background)
        .hover(move |d| d.bg(paint.hover_background))
        .cursor_pointer()
        .on_click(move |event, window, cx| row_click(&row_id, event, window, cx))
        .on_mouse_down(
            MouseButton::Right,
            move |event: &MouseDownEvent, window, cx| {
                row_context(&context_id, event, window, cx);
            },
        )
        .child(render_chevron(&view, host, paint.muted_color, &metrics))
        // Key
        .child(
            div()
                .flex()
                .items_center()
                .gap(Spacing::XS)
                .child(
                    Text::body_sm(view.key.clone())
                        .font_size(metrics.label_font)
                        .color(paint.key_color),
                )
                .child(
                    Text::caption(":")
                        .font_size(metrics.label_font)
                        .color(paint.muted_color),
                ),
        )
        // Value preview
        .child(value)
        // Type badge with type-colored background
        .child(
            div()
                .px(Spacing::XS)
                .rounded(Radii::SM)
                .bg(paint.value_color.opacity(0.15))
                .child(
                    Text::caption(view.type_label)
                        .font_size(metrics.label_font)
                        .color(paint.value_color),
                ),
        )
}

fn render_chevron(
    view: &DocumentTreeRowView,
    host: &DocumentTreeRowHost,
    muted_color: Hsla,
    metrics: &TreeRowMetrics,
) -> Stateful<Div> {
    let chevron = div()
        .id(ElementId::Name(
            format!("tree-chevron-{:?}", view.id.path).into(),
        ))
        .w(metrics.chevron_box)
        .h(metrics.chevron_box)
        .flex()
        .items_center()
        .justify_center();

    if !view.is_expandable {
        return chevron;
    }

    let icon = if view.is_expanded {
        host.chevron_expanded.clone()
    } else {
        host.chevron_collapsed.clone()
    };

    let click = host.on_chevron_click.clone();
    let node_id = view.id.clone();

    chevron
        .child(Icon::new(icon).size(metrics.chevron).color(muted_color))
        .cursor_pointer()
        .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
        .on_click(move |event, window, cx| {
            cx.stop_propagation();
            click(&node_id, event, window, cx);
        })
}
