//! Rendering of `SpreadsheetDocument`.
//!
//! Layout of an opened workbook, top to bottom: a header with the summary
//! ("N sheets · R rows × C columns · the whole sheet is loaded in memory"),
//! the edit bar (the formula replacement warning, Append row and Save) or,
//! for xls, a banner saying the file is read-only, the formula readout of
//! the selected cell, the table of the shown sheet, and the sheet tabs. A
//! hidden sheet's tab says so; a sheet with pending edits ends its name with
//! a dot; a chart sheet's tab is listed but takes no click, and its tooltip
//! says why.
//!
//! While the file is opened and when opening failed, a centered notice takes
//! the place of all of it. While a sheet is read, when reading it failed and
//! when it is empty and cannot be edited, a notice takes the place of the
//! readout and the table.
//!
//! An object that is only read whole is downloaded after a prompt over the
//! document says how large it is. Enter downloads and Escape declines.

use dbflux_components::composites::{EmptyState, result_tab, result_tab_bar};
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::Modal;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::{DocumentMetrics, FontSizes};
use dbflux_core::LogErr;
use dbflux_spreadsheet::SheetKind;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::tooltip::Tooltip;

use super::document::{SheetPhase, SpreadsheetDocument, SpreadsheetPhase};
use super::grid_model::FormulaReadout;
use crate::chrome::document_bar;

const DOWNLOAD_PROMPT_WIDTH: Pixels = px(440.0);

impl SpreadsheetDocument {
    /// The open prompt before a whole download: the object's size, and
    /// Cancel and Download. It asks what the Parquet document asks, in the
    /// same words.
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
                    "spreadsheet-download-cancel",
                    dbflux_i18n::t!("document.parquet.download.cancel"),
                )
                .on_click(cx.listener(|this, _, _, cx| {
                    this.dismiss_download_prompt(cx);
                })),
            )
            .child(
                Button::new(
                    "spreadsheet-download-confirm",
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
                .id("spreadsheet-download-prompt")
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

    /// Append row and Save, after the warning that pending edits replace
    /// formula cells with values. For xls, whose format has no writer, a
    /// banner saying the file is read-only takes its place.
    fn render_edit_bar(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme();

        if !self.is_editable_format() {
            return document_bar(DocumentMetrics::TOOLBAR_HEIGHT, cx)
                .id("spreadsheet-read-only")
                .debug_selector(|| "spreadsheet-read-only".to_string())
                .text_size(FontSizes::XS)
                .text_color(theme.muted_foreground)
                .child(
                    div()
                        .min_w_0()
                        .truncate()
                        .child(dbflux_i18n::t!("document.spreadsheet.read_only.xls")),
                )
                .into_any_element();
        }

        let replaced = self.formula_replacement_count(cx);

        let warning = (replaced > 0).then(|| {
            div()
                .id("spreadsheet-formula-warning")
                .debug_selector(|| "spreadsheet-formula-warning".to_string())
                .min_w_0()
                .truncate()
                .text_color(theme.warning)
                .child(crate::labels::spreadsheet_formula_warning(replaced))
        });

        let append_row = Button::new(
            "spreadsheet-append-row",
            dbflux_i18n::t!("document.spreadsheet.action.append_row"),
        )
        .inline()
        .icon(AppIcon::Plus)
        .when_some(
            dbflux_ui_base::keymap::shortcut_label(
                dbflux_app::keymap::ContextId::DataTable,
                dbflux_app::keymap::Command::ResultsAddRow,
            ),
            Button::kbd,
        )
        .disabled(!self.can_append_row())
        .on_click(cx.listener(|this, _, _, cx| {
            this.append_row(cx);
        }));

        let (save_label, save_icon) = if self.is_saving() {
            (
                dbflux_i18n::t!("document.spreadsheet.action.saving"),
                AppIcon::Loader,
            )
        } else {
            (
                dbflux_i18n::t!("document.spreadsheet.action.save"),
                AppIcon::Save,
            )
        };

        // A disabled primary button keeps its fill, which reads as a live
        // control, so Save takes the primary style only while it can save.
        let can_save = self.can_save();

        let save = Button::new("spreadsheet-save", save_label)
            .inline()
            .when(can_save, Button::primary)
            .icon(save_icon)
            .when_some(
                dbflux_ui_base::keymap::shortcut_label(
                    dbflux_app::keymap::ContextId::DataTable,
                    dbflux_app::keymap::Command::SaveRow,
                ),
                Button::kbd,
            )
            .disabled(!can_save)
            .on_click(cx.listener(|this, _, _, cx| {
                this.save(cx);
            }));

        document_bar(DocumentMetrics::TOOLBAR_HEIGHT, cx)
            .id("spreadsheet-edit-bar")
            .text_size(FontSizes::XS)
            .child(div().flex_1().min_w_0().children(warning))
            .child(append_row)
            .child(save)
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
            let label = if self.sheet_has_pending_edits(index, cx) {
                format!("{} •", sheet.name)
            } else {
                sheet.name.clone()
            };

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
                label,
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
        let download_prompt = self.render_download_prompt(window, cx);

        let body = match self.phase() {
            SpreadsheetPhase::Loaded(_) => div()
                .flex()
                .flex_col()
                .flex_1()
                .min_h_0()
                .child(self.render_header(cx))
                .child(self.render_edit_bar(cx))
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

            SpreadsheetPhase::AwaitingDownload => self.render_notice(
                "spreadsheet-awaiting-download",
                EmptyState::new(
                    AppIcon::Download,
                    dbflux_i18n::t!("document.parquet.download.title"),
                )
                .title(title.clone()),
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
            .capture_action(cx.listener(Self::take_save_key))
            .relative()
            .flex()
            .flex_col()
            .size_full()
            .bg(cx.theme().background)
            .child(body)
            .children(download_prompt)
    }
}
