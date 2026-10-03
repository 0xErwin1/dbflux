//! Rendering of `DelimitedDocument`.
//!
//! Layout, top to bottom: the toolbar, the warnings of the opened file, the
//! table of the loaded records, and a footer with the delimiter, the
//! encoding, the record count and, while the file has more records, the
//! control that loads the next page. The toolbar holds the dialect controls,
//! the control that reads the file again from its source and, for a file
//! that can be saved in place, the controls that insert a row above the
//! cursor, discard the changes and save them. The toolbar wraps when the tab
//! is narrow; the footer is one fixed row. While the file is read again under an
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

    /// The toolbar: the delimiter, quote and encoding selects, the header
    /// checkbox, the control that drops every override, the reload and the
    /// edit controls. `None` until the file is loaded. The row wraps when the
    /// tab is narrow.
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
                .id("delimited-toolbar")
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
                .child(self.render_reload(cx))
                .child(div().flex_1())
                .children(self.render_edit_controls(cx))
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

    /// The control that reads the file again from its source. It takes a
    /// click while there are changes too, and then says why it refuses.
    fn render_reload(&self, cx: &Context<Self>) -> Button {
        Button::new(
            "delimited-reload",
            dbflux_i18n::t!("document.delimited.action.reload"),
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
        .on_click(cx.listener(|this, _, _, cx| {
            this.reload(cx);
        }))
    }

    /// The controls that insert a row above the cursor, discard the changes
    /// and save them. Empty for a file that cannot be saved in place, whose
    /// table is read-only. Discard and save take a click only while there
    /// are changes and no save runs, and insert only while the rows can be
    /// edited.
    fn render_edit_controls(&self, cx: &Context<Self>) -> Vec<Button> {
        if !self.shows_edit_controls() {
            return Vec::new();
        }

        let insert_above = Button::new(
            "delimited-insert-above",
            dbflux_i18n::t!("document.delimited.action.insert_above"),
        )
        .inline()
        .icon(AppIcon::Plus)
        .disabled(!self.can_edit())
        .on_click(cx.listener(|this, _, _, cx| {
            this.insert_row_above(cx);
        }));

        let discard = Button::new(
            "delimited-discard",
            dbflux_i18n::t!("document.delimited.action.discard"),
        )
        .inline()
        .icon(AppIcon::RotateCcw)
        .disabled(!self.can_save_or_discard())
        .on_click(cx.listener(|this, _, _, cx| {
            this.discard_changes(cx);
        }));

        let (save_label, save_icon) = if self.is_saving() {
            (
                dbflux_i18n::t!("document.delimited.action.saving"),
                AppIcon::Loader,
            )
        } else {
            (
                dbflux_i18n::t!("document.delimited.action.save"),
                AppIcon::Save,
            )
        };

        let save = Button::new("delimited-save", save_label)
            .inline()
            .primary()
            .icon(save_icon)
            .when_some(
                dbflux_ui_base::keymap::shortcut_label(
                    dbflux_app::keymap::ContextId::DataTable,
                    dbflux_app::keymap::Command::SaveRow,
                ),
                Button::kbd,
            )
            .disabled(!self.can_save_or_discard())
            .on_click(cx.listener(|this, _, _, cx| {
                this.save(cx);
            }));

        vec![insert_above, discard, save]
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
                    .id("delimited-footer")
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
