//! Rendering of `ParquetDocument`.
//!
//! Layout of a loaded file, top to bottom, after the columnar artboards: a
//! header with the file's summary ("M columns · R rows · size") and the
//! Data | Columns switch at its end, then the view shown.
//!
//! Data: a toolbar with the column picker ("N of M columns"), the strip that
//! says what the next window reads, the table of the loaded rows, and a
//! footer with the rows loaded of the file's total, the columns shown when
//! some are left out, the control that reads the file again and, while the
//! file has more rows, the control that loads the next window.
//!
//! Columns: the Columns view, one row per column of the file.
//!
//! While the file is opened, when opening failed and when the file has no
//! rows, a centered notice takes the place of all of it.
//!
//! An object that is only read whole is downloaded after a prompt over the
//! document says how large it is. Enter downloads and Escape declines.

use dbflux_components::components::data_table::DataTable;
use dbflux_components::composites::EmptyState;
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::Modal;
use dbflux_components::primitives::{SegmentedControl, SegmentedItem, Text};
use dbflux_components::tokens::{DocumentMetrics, FontSizes};
use dbflux_core::LogErr;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

use super::document::{ParquetDocument, ParquetPhase, ParquetView};
use crate::chrome::{document_bar, document_footer};

const DOWNLOAD_PROMPT_WIDTH: Pixels = px(440.0);

impl ParquetDocument {
    /// The open prompt before a whole download: the object's size, and
    /// Cancel and Download.
    fn render_download_prompt(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        let message = self.download_prompt_message()?;

        let prompt = self.download_prompt_mut()?;
        prompt.focus_mut().apply_pending(window, cx);
        let focus_handle = prompt.focus_mut().handle().clone();

        let footer = div()
            .flex()
            .gap(DocumentMetrics::GAP)
            .child(
                Button::new(
                    "parquet-download-cancel",
                    dbflux_i18n::t!("document.parquet.download.cancel"),
                )
                .on_click(cx.listener(|this, _, _, cx| {
                    this.dismiss_download_prompt(cx);
                })),
            )
            .child(
                Button::new(
                    "parquet-download-confirm",
                    dbflux_i18n::t!("document.parquet.download.confirm"),
                )
                .primary()
                .icon(AppIcon::Download)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.confirm_download(cx);
                })),
            );

        let document = cx.weak_entity();
        let confirm_target = document.clone();

        Some(
            Modal::new(dbflux_i18n::t!("document.parquet.download.title"))
                .id("parquet-download-prompt")
                .icon(AppIcon::Download)
                .width(DOWNLOAD_PROMPT_WIDTH)
                .focus_handle(&focus_handle)
                .on_close(move |_window, cx| {
                    document
                        .update(cx, |this, cx| this.dismiss_download_prompt(cx))
                        .log_err();
                })
                .on_confirm(move |_window, cx| {
                    confirm_target
                        .update(cx, |this, cx| this.confirm_download(cx))
                        .log_err();
                })
                .body(Text::body(message))
                .footer(footer)
                .into_any_element(),
        )
    }

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

    /// The file's summary, and the switch between Data and Columns at the
    /// end of the row.
    fn render_header(&self, cx: &Context<Self>) -> AnyElement {
        let document = cx.entity().downgrade();

        let switch = SegmentedControl::new(
            vec![
                SegmentedItem::new("data", dbflux_i18n::t!("document.parquet.view.data"))
                    .icon(AppIcon::Table),
                SegmentedItem::new("columns", dbflux_i18n::t!("document.parquet.view.columns"))
                    .icon(AppIcon::Columns),
            ],
            match self.view() {
                ParquetView::Data => "data",
                ParquetView::Columns => "columns",
            },
            move |id, window, cx| {
                let view = if id.as_ref() == "columns" {
                    ParquetView::Columns
                } else {
                    ParquetView::Data
                };

                if let Some(document) = document.upgrade() {
                    document.update(cx, |document, cx| document.show_view(view, window, cx));
                }
            },
        )
        .group("parquet-view");

        document_bar(DocumentMetrics::HEADER_HEIGHT, cx)
            .id("parquet-header")
            .child(
                div()
                    .min_w_0()
                    .truncate()
                    .font_family(dbflux_components::fonts::editor_family(cx))
                    .text_size(FontSizes::XS)
                    .text_color(cx.theme().muted_foreground)
                    .children(self.summary().cloned()),
            )
            .child(div().flex_1())
            .child(switch)
            .into_any_element()
    }

    /// The column picker, and under it the strip that says what the next
    /// window reads while the file has more rows.
    fn render_data_toolbar(&self, cx: &Context<Self>) -> impl IntoElement {
        let estimate = self.estimate_bar().cloned().map(|bar| {
            div()
                .flex_shrink_0()
                .px(DocumentMetrics::PADDING_X)
                .py(DocumentMetrics::GAP)
                .border_b_1()
                .border_color(cx.theme().border)
                .child(bar)
        });

        div()
            .flex()
            .flex_col()
            .flex_shrink_0()
            .child(
                document_bar(DocumentMetrics::TOOLBAR_HEIGHT, cx)
                    .id("parquet-toolbar")
                    .children(self.projection_picker().cloned()),
            )
            .children(estimate)
    }

    fn render_loaded(&self, table: Entity<DataTable>, cx: &mut Context<Self>) -> AnyElement {
        let body = match self.view() {
            ParquetView::Data => self.render_data(table, cx),
            ParquetView::Columns => div()
                .flex()
                .flex_col()
                .flex_1()
                .min_h_0()
                .children(self.column_profile_view().cloned())
                .into_any_element(),
        };

        div()
            .flex()
            .flex_col()
            .flex_1()
            .min_h_0()
            .child(self.render_header(cx))
            .child(body)
            .into_any_element()
    }

    fn render_data(&self, table: Entity<DataTable>, cx: &mut Context<Self>) -> AnyElement {
        let source_changed = self
            .source_changed()
            .then(|| dbflux_i18n::t!("document.parquet.footer.source_changed"));

        div()
            .flex()
            .flex_col()
            .flex_1()
            .min_h_0()
            .child(self.render_data_toolbar(cx))
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
        self.ensure_projection_controls(window, cx);

        // The loading notice held the keyboard while the file was read; the
        // table takes it over, which needs the window the load did not have.
        if self.take_pending_table_focus() && self.focus_handle().is_focused(window) {
            self.focus(window, cx);
        }

        let title = self.title();
        let download_prompt = self.render_download_prompt(window, cx);

        let body = match (self.table().cloned(), self.phase()) {
            (Some(table), _) => self.render_loaded(table, cx),

            (None, ParquetPhase::Failed(cause)) => self.render_notice(
                "parquet-failed",
                EmptyState::new(AppIcon::TriangleAlert, cause.clone())
                    .title(crate::labels::parquet_open_failed_message(&title))
                    .danger(),
                cx,
            ),

            (None, ParquetPhase::AwaitingDownload) => self.render_notice(
                "parquet-awaiting-download",
                EmptyState::new(
                    AppIcon::Download,
                    dbflux_i18n::t!("document.parquet.download.title"),
                )
                .title(title.clone()),
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
            .children(download_prompt)
    }
}
