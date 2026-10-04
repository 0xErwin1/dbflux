//! Rendering of `DelimitedDocument`.
//!
//! Layout, top to bottom: the dialect toolbar, the warnings of the opened
//! file, the table of the loaded records, and a footer with the delimiter,
//! the encoding, the record count and, while the file has more records, the
//! control that loads the next page. While the file is read again under an
//! override the footer says so and that control takes no click. While the
//! first page is read, and when opening failed, a centered notice takes the
//! place of all four.

use dbflux_components::components::data_table::DataTable;
use dbflux_components::composites::EmptyState;
use dbflux_components::controls::{Button, Checkbox, Dropdown};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::DocumentMetrics;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

use super::document::DelimitedDocument;
use crate::chrome::document_footer;

/// The width of a select of the dialect toolbar: the longest item, a value
/// marked as detected, fits without being cut.
const DIALECT_SELECT_WIDTH: Pixels = px(190.0);

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

    /// The dialect toolbar: the delimiter, quote and encoding selects, the
    /// header checkbox and the control that drops every override. `None`
    /// until the file is loaded. The row wraps when the tab is narrow.
    fn render_dialect_toolbar(&self, cx: &Context<Self>) -> Option<AnyElement> {
        let controls = self.dialect_controls()?;
        let requested = self.requested_dialect()?;
        let theme = cx.theme();

        let select = |label: String, dropdown: &Entity<Dropdown>| {
            div()
                .flex()
                .flex_shrink_0()
                .items_center()
                .gap(DocumentMetrics::GAP)
                .child(Text::caption(label))
                .child(div().w(DIALECT_SELECT_WIDTH).child(dropdown.clone()))
        };

        let document = cx.entity().downgrade();

        let header = Checkbox::new("delimited-header")
            .checked(requested.has_header)
            .label(self.header_label())
            .on_click(move |has_header, _window, cx| {
                if let Some(document) = document.upgrade() {
                    document.update(cx, |document, cx| {
                        document.override_has_header(*has_header, cx);
                    });
                }
            });

        let reset = Button::new(
            "delimited-dialect-reset",
            dbflux_i18n::t!("document.delimited.toolbar.reset"),
        )
        .inline()
        .icon(AppIcon::RotateCcw)
        .disabled(!self.has_dialect_overrides())
        .on_click(cx.listener(|this, _, _, cx| {
            this.reset_dialect(cx);
        }));

        Some(
            div()
                .flex()
                .flex_wrap()
                .flex_shrink_0()
                .items_center()
                .gap(DocumentMetrics::GAP)
                .px(DocumentMetrics::PADDING_X)
                .py(DocumentMetrics::GAP)
                .border_b_1()
                .border_color(theme.border)
                .child(select(
                    dbflux_i18n::t!("document.delimited.toolbar.delimiter"),
                    &controls.delimiter,
                ))
                .child(select(
                    dbflux_i18n::t!("document.delimited.toolbar.quote"),
                    &controls.quote,
                ))
                .child(div().flex_shrink_0().child(header))
                .child(select(
                    dbflux_i18n::t!("document.delimited.toolbar.encoding"),
                    &controls.encoding,
                ))
                .child(reset)
                .into_any_element(),
        )
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

    /// The control that loads the next page. `None` once every record is
    /// loaded. While a page is being read it says so and takes no click, and
    /// it takes none while the file is read again under an override.
    fn render_load_more(&self, cx: &Context<Self>) -> Option<Button> {
        if !self.has_more_records() {
            return None;
        }

        let is_loading = self.is_loading_more();

        let label = if is_loading {
            dbflux_i18n::t!("document.delimited.footer.loading_more")
        } else {
            dbflux_i18n::t!("document.delimited.footer.load_more")
        };

        Some(
            Button::new("delimited-load-more", label)
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

    fn render_loaded(&self, table: Entity<DataTable>, cx: &Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .flex_1()
            .min_h_0()
            .children(self.render_dialect_toolbar(cx))
            .children(self.render_warnings(cx))
            .child(div().flex_1().min_h_0().child(table))
            .child(
                document_footer(cx)
                    .children(
                        self.status_items()
                            .iter()
                            .map(|item| div().flex_shrink_0().child(item.clone())),
                    )
                    .children(
                        self.progress_item()
                            .map(|item| div().flex_shrink_0().child(item)),
                    )
                    .children(self.render_load_more(cx)),
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
