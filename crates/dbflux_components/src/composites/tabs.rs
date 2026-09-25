//! The three tab kinds of the design system (DSAppPlan "Tabs").
//!
//! - Document tabs (AppByzTable, AppByzEditor): 36 px tabs at the bottom of a
//!   42 px bar. The active tab has the panel fill, a top-left cut of 8 and a
//!   2 px byzantine edge along its top; inactive tabs are muted.
//! - Result tabs (AppByzEditor results): 34 px tabs at the bottom of a 40 px
//!   bar, with a row count and statement range after the label. The active
//!   tab is drawn like an active document tab.
//! - Inline tabs (P1ConnForm): 42 px tabs over a line, the active one in
//!   strong text with a 2 px byzantine edge along its bottom.
//!
//! Each builder returns the styled, identified shell; the caller adds click
//! handlers and children, so the tab keeps whatever behavior its owner needs.

use gpui::prelude::*;
use gpui::{App, ElementId, FontWeight, SharedString, Stateful, div};
use gpui_component::ActiveTheme;

use crate::primitives::{Chamfer, ChamferCorners, Text};
use crate::tokens::{ChamferCut, ChromeColors, TabMetrics};

/// The document tab strip: 42 px, laid out inside the title bar, which owns
/// the ground and the line along the bottom. Add `document_tab`s and the
/// new-tab button as children.
pub fn document_tab_bar(_cx: &App) -> gpui::Div {
    div()
        .flex()
        .flex_shrink_0()
        .items_stretch()
        .h(TabMetrics::DOCUMENT_BAR_HEIGHT)
        .pl(TabMetrics::DOCUMENT_BAR_PADDING_LEFT)
        .gap(TabMetrics::BAR_GAP)
}

/// One document tab. Add the icon, `document_tab_title`, the dirty diamond
/// and the close button as children.
pub fn document_tab(id: impl Into<ElementId>, active: bool, cx: &App) -> Stateful<gpui::Div> {
    div()
        .id(id)
        .relative()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(TabMetrics::DOCUMENT_TAB_GAP)
        .h(TabMetrics::DOCUMENT_TAB_HEIGHT)
        .mt(TabMetrics::DOCUMENT_TAB_TOP)
        .pr(TabMetrics::DOCUMENT_TAB_PADDING_X)
        .pl(if active {
            TabMetrics::DOCUMENT_TAB_ACTIVE_PADDING_LEFT
        } else {
            TabMetrics::DOCUMENT_TAB_PADDING_X
        })
        .cursor_pointer()
        .child(tab_shape(active, cx))
}

/// Title of a document or result tab: 13 px, strong and 600 when active,
/// muted otherwise.
pub fn document_tab_title(title: impl Into<SharedString>, active: bool, cx: &App) -> Text {
    let theme = cx.theme();

    if active {
        Text::body(title)
            .color(ChromeColors::strong(theme))
            .font_weight(FontWeight::SEMIBOLD)
    } else {
        Text::body(title).muted_foreground()
    }
}

/// The result tab bar: 40 px on the window ground with a line along its
/// bottom, tabs aligned to that line. Actions added after a `flex_1` spacer
/// sit on the right.
pub fn result_tab_bar(cx: &App) -> gpui::Div {
    let theme = cx.theme();

    div()
        .flex()
        .flex_shrink_0()
        .items_end()
        .h(TabMetrics::RESULT_BAR_HEIGHT)
        .px(TabMetrics::RESULT_BAR_PADDING_X)
        .gap(TabMetrics::BAR_GAP)
        .bg(theme.background)
        .border_b_1()
        .border_color(theme.border)
}

/// One result tab: the label, then `meta` (row count, statement range) in
/// small mono. Add a close button or other trailing items as children.
pub fn result_tab(
    id: impl Into<ElementId>,
    label: impl Into<SharedString>,
    meta: Option<SharedString>,
    active: bool,
    cx: &App,
) -> Stateful<gpui::Div> {
    div()
        .id(id)
        .relative()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(TabMetrics::RESULT_TAB_GAP)
        .h(TabMetrics::RESULT_TAB_HEIGHT)
        .px(TabMetrics::RESULT_TAB_PADDING_X)
        .cursor_pointer()
        .child(tab_shape(active, cx))
        .child(document_tab_title(label, active, cx))
        .when_some(meta, |tab, meta| {
            tab.child(
                Text::code(meta)
                    .font_size(TabMetrics::RESULT_META_SIZE)
                    .font_weight(FontWeight::NORMAL)
                    .muted_foreground(),
            )
        })
}

/// The inline tab bar: tabs over a line, as in the connection manager.
pub fn inline_tab_bar(cx: &App) -> gpui::Div {
    div()
        .flex()
        .flex_shrink_0()
        .items_center()
        .px(TabMetrics::INLINE_BAR_PADDING_X)
        .border_b_1()
        .border_color(cx.theme().border)
}

/// One inline tab. Strong text with a byzantine bottom edge when active,
/// muted otherwise; add the icon, label and any badge as children.
pub fn inline_tab(id: impl Into<ElementId>, active: bool, cx: &App) -> Stateful<gpui::Div> {
    let theme = cx.theme();
    let hover = ChromeColors::strong(theme);

    div()
        .id(id)
        .relative()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(TabMetrics::RESULT_TAB_GAP)
        .h(TabMetrics::INLINE_TAB_HEIGHT)
        .px(TabMetrics::INLINE_TAB_PADDING_X)
        .cursor_pointer()
        .when(active, |tab| {
            tab.text_color(ChromeColors::strong(theme)).child(
                div()
                    .absolute()
                    .left_0()
                    .right_0()
                    .bottom_0()
                    .h(TabMetrics::ACTIVE_EDGE)
                    .bg(theme.primary),
            )
        })
        .when(!active, |tab| {
            tab.text_color(theme.muted_foreground)
                .hover(move |style| style.text_color(hover))
        })
}

/// Shape behind a document or result tab: panel fill, top-left cut and
/// byzantine top edge when active; a raised hover fill otherwise.
fn tab_shape(active: bool, cx: &App) -> Chamfer {
    let theme = cx.theme();
    let shape = Chamfer::new(ChamferCut::INPUT).corners(ChamferCorners::TopLeft);

    if active {
        shape
            .fill(theme.popover)
            .top_edge(theme.primary, TabMetrics::ACTIVE_EDGE)
    } else {
        shape
            .fill(gpui::transparent_black())
            .fill_hover(theme.secondary)
            .interactive("tab-shape")
    }
}
