//! The bordered card of a secret's tab: a title row (the uppercase label on
//! the left, the section's actions on the right) over the section's body.
//! The details pane draws the same sections without the frame, so one
//! builder serves both hosts.

use super::{SecretView, section_title};
use dbflux_components::primitives::{SurfaceRole, surface};
use dbflux_components::tokens::{DocumentMetrics, SecretStoreMetrics};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

impl SecretView {
    /// One section of the details. In the pane it is the title row over the
    /// rows; in the tab it is a card with `tab_id` as its element id.
    pub(super) fn framed_section(
        &self,
        tab_id: &'static str,
        title: String,
        badge: Option<AnyElement>,
        actions: Vec<AnyElement>,
        rows: Vec<AnyElement>,
        cx: &App,
    ) -> AnyElement {
        let header = div()
            .flex()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .child(section_title(title, cx))
            .children(badge)
            .child(div().flex_1())
            .children(actions);

        if !self.in_tab() {
            return div()
                .flex()
                .flex_col()
                .gap(SecretStoreMetrics::ROW_GAP)
                .child(header)
                .children(rows)
                .into_any_element();
        }

        card(tab_id, header, rows, cx).into_any_element()
    }
}

/// A card: the chamfered frame, `header` over a rule, then the rows.
pub(super) fn card(
    id: &'static str,
    header: impl IntoElement,
    rows: Vec<AnyElement>,
    cx: &App,
) -> Stateful<Div> {
    let border = cx.theme().border;

    // The frame is the design system's Card surface; the title row and the
    // body are this section's own.
    surface(SurfaceRole::Card, cx)
        .id(id)
        .flex()
        .flex_col()
        .child(
            div()
                .px(DocumentMetrics::DETAIL_BLOCK_PADDING_X)
                .py(DocumentMetrics::DETAIL_BLOCK_PADDING_Y)
                .border_b_1()
                .border_color(border)
                .child(header),
        )
        .child(
            div()
                .flex()
                .flex_col()
                .gap(SecretStoreMetrics::ROW_GAP)
                .px(DocumentMetrics::DETAIL_BLOCK_PADDING_X)
                .py(DocumentMetrics::DETAIL_BLOCK_PADDING_Y)
                .children(rows),
        )
}
