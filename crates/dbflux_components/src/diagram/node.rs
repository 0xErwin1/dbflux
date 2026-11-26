//! Node chrome: the card a diagram draws for each node.
//!
//! The card owns what every node shares: its position, border, selection
//! treatment and the header / body / footer slots. What goes in the slots, and
//! which mouse and key handlers the card carries, is the consumer's: [`NodeCard::build`]
//! returns the card as a [`Div`] to chain handlers on.

use gpui::prelude::*;
use gpui::{AnyElement, App, CursorStyle, Div, SharedString, div, px};
use gpui_component::theme::Theme;

use crate::icons::AppIcon;
use crate::primitives::{Chamfer, ChamferCorners, Icon};
use crate::tokens::{ChamferCut, ChromeColors, SchemaMetrics};

/// A node card at a graph-space position.
pub struct NodeCard {
    x: f32,
    y: f32,
    width: f32,
    selected: bool,
    dragging: bool,
    header: Option<AnyElement>,
    body: Option<AnyElement>,
    footer: Option<AnyElement>,
}

impl NodeCard {
    /// A card with its top-left corner at `(x, y)` in graph space.
    pub fn new(x: f32, y: f32, width: f32) -> Self {
        Self {
            x,
            y,
            width,
            selected: false,
            dragging: false,
            header: None,
            body: None,
            footer: None,
        }
    }

    /// A selected card takes the tint on its edge and a shadow. Focus rings stay
    /// reserved for keyboard focus.
    pub fn selected(mut self, selected: bool) -> Self {
        self.selected = selected;
        self
    }

    pub fn dragging(mut self, dragging: bool) -> Self {
        self.dragging = dragging;
        self
    }

    pub fn header(mut self, header: impl IntoElement) -> Self {
        self.header = Some(header.into_any_element());
        self
    }

    pub fn body(mut self, body: impl IntoElement) -> Self {
        self.body = Some(body.into_any_element());
        self
    }

    pub fn footer(mut self, footer: impl IntoElement) -> Self {
        self.footer = Some(footer.into_any_element());
        self
    }

    /// The card as an absolutely positioned column: border, header, body, footer.
    pub fn build(self, theme: &Theme) -> Div {
        let tint = ChromeColors::tint(theme);
        let cursor_style = if self.dragging {
            CursorStyle::PointingHand
        } else {
            CursorStyle::Arrow
        };

        let card_shape = Chamfer::new(ChamferCut::INPUT)
            .fill(theme.popover)
            .border(if self.selected { tint } else { theme.input });

        div()
            .absolute()
            .left(px(self.x))
            .top(px(self.y))
            .w(px(self.width))
            .when(self.selected, |card| card.shadow_lg())
            .cursor(cursor_style)
            .child(card_shape)
            .flex()
            .flex_col()
            .children(self.header)
            .children(self.body)
            .children(self.footer)
    }
}

/// The standard card header: a tint icon and a bold mono title on a fill that
/// takes the tint when the card is selected.
///
/// `height` is the whole header height in pixels. The header keeps a 1 px inset
/// so its fill never covers the card's border, and cuts only its top-left corner
/// to follow the card.
pub fn node_header(
    icon: AppIcon,
    title: impl Into<SharedString>,
    selected: bool,
    height: f32,
    theme: &Theme,
    cx: &App,
) -> Div {
    let tint = ChromeColors::tint(theme);
    let header_fill = if selected {
        tint.opacity(SchemaMetrics::SELECTED_HEADER_ALPHA)
    } else {
        theme.secondary
    };

    div()
        .relative()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(SchemaMetrics::HEADER_GAP)
        .mt(px(1.0))
        .mx(px(1.0))
        .px(SchemaMetrics::CARD_PADDING_X)
        .h(px(height - 1.0))
        .border_b_1()
        .border_color(theme.border)
        .child(
            Chamfer::new(ChamferCut::INPUT)
                .corners(ChamferCorners::TopLeft)
                .fill(header_fill),
        )
        .child(Icon::new(icon).size(SchemaMetrics::HEADER_ICON).color(tint))
        .child(
            div()
                .flex_1()
                .min_w_0()
                .truncate()
                .font_family(crate::fonts::editor_family(cx))
                .text_size(SchemaMetrics::HEADER_FONT)
                .font_weight(gpui::FontWeight::BOLD)
                .text_color(ChromeColors::strong(theme))
                .child(title.into()),
        )
}

/// The standard card body: a column of rows with the card's side padding.
pub fn node_body(rows: impl IntoIterator<Item = impl IntoElement>) -> Div {
    div()
        .flex()
        .flex_col()
        .px(SchemaMetrics::CARD_PADDING_X)
        .children(rows)
}
