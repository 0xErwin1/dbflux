//! Rendering of `DelimitedDocument`.
//!
//! Layout, top to bottom: the warnings of the opened file, the table of the
//! loaded records, and a footer with the delimiter, the encoding and the
//! record count. While the first page is read, and when opening failed, a
//! centered notice takes the place of all three.

use dbflux_components::components::data_table::DataTable;
use dbflux_components::composites::EmptyState;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::DocumentMetrics;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

use super::document::DelimitedDocument;
use crate::chrome::document_footer;

impl DelimitedDocument {
    fn render_notice(&self, notice: EmptyState, cx: &Context<Self>) -> AnyElement {
        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .p(DocumentMetrics::PADDING_X)
            .bg(cx.theme().background)
            .child(notice)
            .into_any_element()
    }

    fn render_warnings(&self, cx: &Context<Self>) -> Option<AnyElement> {
        let warnings = self.warning_items();

        if warnings.is_empty() {
            return None;
        }

        Some(
            div()
                .flex()
                .flex_col()
                .flex_shrink_0()
                .gap(DocumentMetrics::GAP)
                .px(DocumentMetrics::PADDING_X)
                .py(DocumentMetrics::GAP)
                .border_b_1()
                .border_color(cx.theme().border)
                .children(
                    warnings
                        .iter()
                        .map(|warning| Text::caption(warning.clone()).warning()),
                )
                .into_any_element(),
        )
    }

    fn render_loaded(&self, table: Entity<DataTable>, cx: &Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .flex_1()
            .min_h_0()
            .children(self.render_warnings(cx))
            .child(div().flex_1().min_h_0().child(table))
            .child(
                document_footer(cx).children(
                    self.status_items()
                        .iter()
                        .map(|item| div().flex_shrink_0().child(item.clone())),
                ),
            )
            .into_any_element()
    }
}

impl Render for DelimitedDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // The loading notice held the keyboard while the file was read; the
        // table takes it over, which needs the window the load did not have.
        if self.take_pending_table_focus() && self.focus_handle().is_focused(window) {
            self.focus(window, cx);
        }

        let body = match (self.table().cloned(), self.failure()) {
            (Some(table), _) => self.render_loaded(table, cx),

            (None, Some(cause)) => self.render_notice(
                EmptyState::new(AppIcon::TriangleAlert, cause.to_string())
                    .title(crate::labels::delimited_open_failed_message(&self.title()))
                    .danger(),
                cx,
            ),

            (None, None) => self.render_notice(
                EmptyState::new(
                    AppIcon::Loader,
                    crate::labels::delimited_loading_label(&self.title()),
                ),
                cx,
            ),
        };

        div()
            .id("delimited-document")
            .track_focus(self.focus_handle())
            .flex()
            .flex_col()
            .size_full()
            .bg(cx.theme().background)
            .child(body)
    }
}
