//! Rendering of `ParquetDocument`.
//!
//! Layout, top to bottom: the table of the loaded rows and a footer with the
//! rows loaded of the file's total, the columns shown when some are left out,
//! the control that reads the file again and, while the file has more rows,
//! the control that loads the next window. While the file is opened, when
//! opening failed and when the file has no rows, a centered notice takes the
//! place of both.

use dbflux_components::components::data_table::DataTable;
use dbflux_components::composites::EmptyState;
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::tokens::DocumentMetrics;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

use super::document::{ParquetDocument, ParquetPhase};
use crate::chrome::document_footer;

impl ParquetDocument {
    fn render_notice(
        &self,
        id: &'static str,
        notice: EmptyState,
        cx: &Context<Self>,
    ) -> AnyElement {
        div()
            .id(id)
            .debug_selector(move || id.to_string())
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

    /// The control that loads the next window. `None` once every row is
    /// loaded. While a window is being read it says so and takes no click, and
    /// it takes none once the file was found changed.
    fn render_load_more(&self, cx: &Context<Self>) -> Option<Button> {
        if !self.has_more_rows() {
            return None;
        }

        let label = if self.is_loading_more() {
            dbflux_i18n::t!("document.parquet.footer.loading_more")
        } else {
            dbflux_i18n::t!("document.parquet.footer.load_more")
        };

        Some(
            Button::new("parquet-load-more", label)
                .inline()
                .icon(AppIcon::ChevronDown)
                .when_some(
                    dbflux_ui_base::keymap::shortcut_label(
                        dbflux_app::keymap::ContextId::Results,
                        dbflux_app::keymap::Command::ResultsNextPage,
                    ),
                    Button::kbd,
                )
                .disabled(!self.can_load_more())
                .on_click(cx.listener(|this, _, _, cx| {
                    this.load_more(cx);
                })),
        )
    }

    /// The control that opens the file again, which is how rows of a file
    /// changed elsewhere are seen.
    fn render_reload(&self, cx: &Context<Self>) -> Button {
        Button::new(
            "parquet-reload",
            dbflux_i18n::t!("document.parquet.action.reload"),
        )
        .inline()
        .icon(AppIcon::RefreshCcw)
        .when_some(
            dbflux_ui_base::keymap::shortcut_label(
                dbflux_app::keymap::ContextId::Results,
                dbflux_app::keymap::Command::RefreshSchema,
            ),
            Button::kbd,
        )
        .disabled(!self.can_reload())
        .on_click(cx.listener(|this, _, _, cx| {
            this.reload(cx);
        }))
    }

    fn render_loaded(&self, table: Entity<DataTable>, cx: &mut Context<Self>) -> AnyElement {
        let source_changed = self
            .source_changed()
            .then(|| dbflux_i18n::t!("document.parquet.footer.source_changed"));

        div()
            .flex()
            .flex_col()
            .flex_1()
            .min_h_0()
            .child(div().flex_1().min_h_0().child(table))
            .child(
                document_footer(cx)
                    .id("parquet-footer")
                    .overflow_hidden()
                    .child(
                        // The text gives way, truncated, so the controls after it
                        // stay inside a narrow pane.
                        div()
                            .flex()
                            .flex_1()
                            .min_w_0()
                            .overflow_hidden()
                            .items_center()
                            .gap(DocumentMetrics::GAP)
                            .children(
                                self.status_items()
                                    .into_iter()
                                    .map(|item| div().min_w_0().truncate().child(item)),
                            )
                            .children(
                                source_changed.map(|item| div().min_w_0().truncate().child(item)),
                            ),
                    )
                    .child(self.render_reload(cx))
                    .children(self.render_load_more(cx)),
            )
            .into_any_element()
    }
}

impl Render for ParquetDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // The loading notice held the keyboard while the file was read; the
        // table takes it over, which needs the window the load did not have.
        if self.take_pending_table_focus() && self.focus_handle().is_focused(window) {
            self.focus(window, cx);
        }

        let title = self.title();

        let body = match (self.table().cloned(), self.phase()) {
            (Some(table), _) => self.render_loaded(table, cx),

            (None, ParquetPhase::Failed(cause)) => self.render_notice(
                "parquet-failed",
                EmptyState::new(AppIcon::TriangleAlert, cause.clone())
                    .title(crate::labels::parquet_open_failed_message(&title))
                    .danger(),
                cx,
            ),

            (None, ParquetPhase::Empty) => self.render_notice(
                "parquet-empty",
                EmptyState::new(
                    AppIcon::Table,
                    dbflux_i18n::t!("document.parquet.empty.description"),
                )
                .title(crate::labels::parquet_empty_title(&title)),
                cx,
            ),

            (None, ParquetPhase::Loading | ParquetPhase::Loaded(_)) => self.render_notice(
                "parquet-loading",
                EmptyState::new(
                    AppIcon::Loader,
                    crate::labels::parquet_loading_label(&title),
                ),
                cx,
            ),
        };

        div()
            .id("parquet-document")
            .track_focus(self.focus_handle())
            .relative()
            .flex()
            .flex_col()
            .size_full()
            .bg(cx.theme().background)
            .child(body)
    }
}
