//! Rendering of `SpreadsheetDocument`.
//!
//! Layout of an opened workbook, top to bottom: a header with the summary
//! ("N sheets · R rows × C columns · the whole sheet is loaded in memory"),
//! the formula readout of the selected cell, the table of the shown sheet,
//! and the sheet tabs. A hidden sheet's tab says so; a chart sheet's tab is
//! listed but takes no click, and its tooltip says why.
//!
//! While the file is opened and when opening failed, a centered notice takes
//! the place of all of it. While a sheet is read, when reading it failed and
//! when it is empty, a notice takes the place of the readout and the table.

use dbflux_components::composites::{EmptyState, result_tab, result_tab_bar};
use dbflux_components::icons::AppIcon;
use dbflux_components::tokens::{DocumentMetrics, FontSizes};
use dbflux_spreadsheet::SheetKind;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::tooltip::Tooltip;

use super::document::{SheetPhase, SpreadsheetDocument, SpreadsheetPhase};
use super::grid_model::FormulaReadout;
use crate::chrome::document_bar;

impl SpreadsheetDocument {
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

    fn render_header(&self, cx: &Context<Self>) -> AnyElement {
        document_bar(DocumentMetrics::HEADER_HEIGHT, cx)
            .id("spreadsheet-header")
            .child(
                div()
                    .min_w_0()
                    .truncate()
                    .font_family(dbflux_components::fonts::editor_family(cx))
                    .text_size(FontSizes::XS)
                    .text_color(cx.theme().muted_foreground)
                    .children(self.summary()),
            )
            .into_any_element()
    }

    /// The address of the selected cell and its formula, or why there is
    /// none to show.
    fn render_formula_readout(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme();

        let (address, text, is_formula) = match self.selected_formula(cx) {
            Some((address, FormulaReadout::Formula(formula))) => {
                (Some(address), formula.to_string(), true)
            }
            Some((address, FormulaReadout::NoFormula)) => (
                Some(address),
                dbflux_i18n::t!("document.spreadsheet.formula.none"),
                false,
            ),
            Some((address, FormulaReadout::Unavailable)) => (
                Some(address),
                dbflux_i18n::t!("document.spreadsheet.formula.unavailable"),
                false,
            ),
            None => (
                None,
                dbflux_i18n::t!("document.spreadsheet.formula.no_selection"),
                false,
            ),
        };

        document_bar(DocumentMetrics::TOOLBAR_HEIGHT, cx)
            .id("spreadsheet-formula")
            .debug_selector(|| "spreadsheet-formula".to_string())
            .text_size(FontSizes::XS)
            .children(address.map(|address| {
                div()
                    .flex_shrink_0()
                    .font_family(dbflux_components::fonts::editor_family(cx))
                    .text_color(theme.foreground)
                    .child(address)
            }))
            .child(
                div()
                    .min_w_0()
                    .truncate()
                    .when(is_formula, |text| {
                        text.font_family(dbflux_components::fonts::editor_family(cx))
                            .text_color(theme.foreground)
                    })
                    .when(!is_formula, |text| text.text_color(theme.muted_foreground))
                    .child(text),
            )
            .into_any_element()
    }

    /// One tab per sheet. Clicking a worksheet's tab shows it; Alt+L and
    /// Alt+H step through the worksheets.
    fn render_sheet_tabs(&self, cx: &Context<Self>) -> AnyElement {
        let active = self.active_sheet();

        let tabs = self.sheets().iter().enumerate().map(|(index, sheet)| {
            let meta = match (sheet.kind, sheet.visible) {
                (SheetKind::Worksheet, true) => None,
                (SheetKind::Worksheet, false) => {
                    Some(dbflux_i18n::t!("document.spreadsheet.tab.hidden").into())
                }
                (SheetKind::ChartSheet | SheetKind::Other, _) => {
                    Some(dbflux_i18n::t!("document.spreadsheet.tab.chart").into())
                }
            };

            let tab = result_tab(
                ("spreadsheet-sheet", index),
                sheet.name.clone(),
                meta,
                active == Some(index),
                cx,
            );

            if sheet.kind == SheetKind::Worksheet {
                tab.on_click(cx.listener(move |this, _, window, cx| {
                    this.select_sheet(index, window, cx);
                }))
                .into_any_element()
            } else {
                tab.cursor_default()
                    .opacity(0.5)
                    .tooltip(|window, cx| {
                        Tooltip::new(dbflux_i18n::t!("document.spreadsheet.tab.chart_tooltip"))
                            .build(window, cx)
                    })
                    .into_any_element()
            }
        });

        result_tab_bar(cx)
            .id("spreadsheet-sheet-tabs")
            .overflow_x_scroll()
            .children(tabs)
            .into_any_element()
    }

    /// The part between the header and the sheet tabs: the readout and the
    /// table, or a notice about the selected sheet.
    fn render_sheet_body(&self, cx: &Context<Self>) -> AnyElement {
        let sheet_name = self.active_sheet_name().unwrap_or_default().to_string();

        match (self.table().cloned(), self.sheet_phase()) {
            (Some(table), _) => div()
                .flex()
                .flex_col()
                .flex_1()
                .min_h_0()
                .child(self.render_formula_readout(cx))
                .child(div().flex_1().min_h_0().child(table))
                .into_any_element(),

            (None, Some(SheetPhase::Failed(cause))) => self.render_notice(
                "spreadsheet-sheet-failed",
                EmptyState::new(AppIcon::TriangleAlert, cause.clone())
                    .title(crate::labels::spreadsheet_sheet_failed_message(
                        &self.title(),
                        &sheet_name,
                    ))
                    .danger(),
                cx,
            ),

            (None, Some(SheetPhase::Empty)) => self.render_notice(
                "spreadsheet-empty",
                EmptyState::new(
                    AppIcon::Table,
                    dbflux_i18n::t!("document.spreadsheet.empty.description"),
                )
                .title(crate::labels::spreadsheet_empty_sheet_title(&sheet_name)),
                cx,
            ),

            (None, Some(SheetPhase::NoWorksheet)) => self.render_notice(
                "spreadsheet-no-worksheet",
                EmptyState::new(
                    AppIcon::Table,
                    dbflux_i18n::t!("document.spreadsheet.no_worksheet.description"),
                )
                .title(crate::labels::spreadsheet_no_worksheet_title(&self.title())),
                cx,
            ),

            (None, _) => self.render_notice(
                "spreadsheet-reading",
                EmptyState::new(
                    AppIcon::Loader,
                    crate::labels::spreadsheet_reading_sheet_label(&sheet_name),
                ),
                cx,
            ),
        }
    }
}

impl Render for SpreadsheetDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // A notice held the keyboard while the sheet was read; the table
        // takes it over, which needs the window the read did not have.
        if self.take_pending_table_focus() && self.focus_handle().is_focused(window) {
            self.focus(window, cx);
        }

        let title = self.title();

        let body = match self.phase() {
            SpreadsheetPhase::Loaded(_) => div()
                .flex()
                .flex_col()
                .flex_1()
                .min_h_0()
                .child(self.render_header(cx))
                .child(self.render_sheet_body(cx))
                .child(self.render_sheet_tabs(cx))
                .into_any_element(),

            SpreadsheetPhase::Failed(cause) => self.render_notice(
                "spreadsheet-failed",
                EmptyState::new(AppIcon::TriangleAlert, cause.clone())
                    .title(crate::labels::spreadsheet_open_failed_message(&title))
                    .danger(),
                cx,
            ),

            SpreadsheetPhase::Loading => self.render_notice(
                "spreadsheet-loading",
                EmptyState::new(
                    AppIcon::Loader,
                    crate::labels::spreadsheet_loading_label(&title),
                ),
                cx,
            ),
        };

        div()
            .id("spreadsheet-document")
            .track_focus(self.focus_handle())
            .flex()
            .flex_col()
            .size_full()
            .bg(cx.theme().background)
            .child(body)
    }
}
