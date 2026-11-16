//! The import wizard's chrome and its steps: the top stepper, the body of each
//! step and the shared footer (Cancel, Back, Continue) with the reason a
//! disabled Continue gives. The state machine and the run live in the parent
//! module.

use dbflux_components::composites::{
    render_wizard_progress_bar, render_wizard_stepper, wizard_progress_fraction,
};
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::{modal_hint, modal_lead};
use dbflux_components::primitives::{BannerBlock, BannerVariant, Chamfer, Icon};
use dbflux_components::tokens::{ChamferCut, ChromeColors, Feedback, ModalMetrics, Spacing, ui};
use dbflux_core::CancelToken;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

use super::errors::ErrorBlock;
use super::keyboard::{ConfigureField, with_cursor_ring};
use super::{ImportWizard, TableImportRow, WizardStep, import_rail_items};
use crate::export_wizard::notes::{ResultKind, footer_reason, info_line, render_result_rows};

/// Width of the import dialog. (620 px)
pub(super) const IMPORT_WIZARD_WIDTH: Pixels = px(620.0);

/// Tallest the mapping list grows before it scrolls.
const TABLE_LIST_MAX_HEIGHT: Pixels = px(320.0);

/// Widths of the mapping card's dropdowns. (196 and 124 px)
const MODE_DROPDOWN_WIDTH: Rems = ui(196.0);
const COLUMN_DROPDOWN_WIDTH: Rems = ui(124.0);

/// A named element a test or the UI automation can find.
fn selectable(selector: &'static str, element: impl IntoElement) -> Div {
    div().debug_selector(|| selector.to_string()).child(element)
}

/// The danger block of a failed step: the message, then what to do next.
pub(super) fn error_block(error: &ErrorBlock, selector: &'static str, cx: &App) -> AnyElement {
    let danger = cx.theme().danger;
    selectable(
        selector,
        BannerBlock::new(BannerVariant::Danger, error.message.clone())
            .with_icon(
                Icon::new(AppIcon::CircleAlert)
                    .size(Feedback::BANNER_ICON)
                    .color(danger),
            )
            .with_body(error.next_step.clone()),
    )
    .into_any_element()
}

impl ImportWizard {
    /// The top stepper over the current step's body.
    pub(super) fn render_body(&self, cx: &mut Context<Self>) -> AnyElement {
        let phase = match self.step {
            WizardStep::PickFolder => self.render_pick_folder(cx),
            WizardStep::Configure => self.render_configure(cx),
            WizardStep::Confirm => self.render_confirm(cx),
            WizardStep::Running => self.render_running(cx),
            WizardStep::Done => self.render_done(cx),
        };

        div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(selectable(
                "import-wizard-stepper",
                render_wizard_stepper(&import_rail_items(self.step), cx),
            ))
            .child(phase)
            .into_any_element()
    }

    fn render_pick_folder(&self, cx: &mut Context<Self>) -> AnyElement {
        let reading_folder = self.loading && self.loading_folder;
        let reading_file = self.loading && !self.loading_folder;

        div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .when_some(self.manifest_error.as_ref(), |body, error| {
                body.child(error_block(error, "import-wizard-error", cx))
            })
            .child(modal_lead(
                dbflux_i18n::t!("document.import_wizard.pick_folder.description"),
                cx,
            ))
            .child(
                div().flex().child(
                    Button::new(
                        "import-wizard-choose-folder",
                        if reading_folder {
                            dbflux_i18n::t!("document.import_wizard.pick_folder.reading_manifest")
                        } else {
                            dbflux_i18n::t!("document.import_wizard.pick_folder.choose_folder")
                        },
                    )
                    .disabled(self.loading)
                    .on_click(cx.listener(|this, _, _, cx| this.choose_folder(cx))),
                ),
            )
            .child(modal_lead(
                dbflux_i18n::t!("document.import_wizard.pick_file.description"),
                cx,
            ))
            .child(
                selectable(
                    "import-wizard-choose-file",
                    Button::new(
                        "import-wizard-choose-file",
                        if reading_file {
                            dbflux_i18n::t!("document.import_wizard.pick_file.reading_file")
                        } else {
                            dbflux_i18n::t!("document.import_wizard.pick_file.choose_file")
                        },
                    )
                    .disabled(self.loading)
                    .on_click(cx.listener(|this, _, _, cx| this.choose_file(cx))),
                )
                .flex(),
            )
            .into_any_element()
    }

    /// One table's mapping card: its source and target names over the mode,
    /// target-column and source-column dropdowns and the Apply Mapping button.
    fn render_mapping_card(
        &self,
        table_index: usize,
        row: &TableImportRow,
        active: Option<ConfigureField>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();
        let unmatched = row.config.unmatched_source_names();
        let strong = ChromeColors::strong(theme);
        let editor_family = dbflux_components::fonts::editor_family(cx);

        div()
            .relative()
            .flex()
            .flex_col()
            .gap(ModalMetrics::DEPENDENT_ROW_GAP)
            .p(Spacing::MD)
            .child(
                Chamfer::new(ChamferCut::INPUT)
                    .fill(theme.background)
                    .border(theme.border),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(ModalMetrics::DEPENDENT_ROW_GAP)
                    .font_family(editor_family)
                    .text_size(ModalMetrics::CODE_FONT)
                    .text_color(strong)
                    .child(row.config.source_table.clone())
                    .child(
                        Icon::new(AppIcon::ChevronRight)
                            .size(ModalMetrics::LIST_ICON)
                            .color(theme.muted_foreground),
                    )
                    .child(row.config.target_table.clone()),
            )
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .items_center()
                    .gap(ModalMetrics::DEPENDENT_ROW_GAP)
                    .child(div().w(MODE_DROPDOWN_WIDTH).child(with_cursor_ring(
                        active,
                        ConfigureField::Mode(table_index),
                        row.mapping_mode_dropdown.clone(),
                        cx,
                    )))
                    .child(div().w(COLUMN_DROPDOWN_WIDTH).child(with_cursor_ring(
                        active,
                        ConfigureField::Target(table_index),
                        row.rebind_target_dropdown.clone(),
                        cx,
                    )))
                    .child(div().w(COLUMN_DROPDOWN_WIDTH).child(with_cursor_ring(
                        active,
                        ConfigureField::Source(table_index),
                        row.rebind_source_dropdown.clone(),
                        cx,
                    )))
                    .child(
                        Button::new(
                            SharedString::from(format!("import-apply-mapping-{table_index}")),
                            dbflux_i18n::t!("document.import_wizard.configure.apply_mapping"),
                        )
                        .ghost()
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.apply_rebind(table_index, cx);
                        })),
                    ),
            )
            .when(!unmatched.is_empty(), |card| {
                card.child(modal_hint(
                    dbflux_i18n::t!(
                        "document.import_wizard.configure.unmatched_source",
                        names = unmatched.join(", ")
                    ),
                    cx,
                ))
            })
            .into_any_element()
    }

    pub(super) fn render_configure(&self, cx: &mut Context<Self>) -> AnyElement {
        let active = self.cursor_field();
        let file_options = self.render_file_options(active, cx);
        let cards: Vec<AnyElement> = self
            .rows
            .iter()
            .enumerate()
            .map(|(table_index, row)| self.render_mapping_card(table_index, row, active, cx))
            .collect();

        div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .when_some(file_options, |body, options| body.child(options))
            .child(
                div()
                    .id("import-wizard-tables")
                    .debug_selector(|| "import-wizard-tables".to_string())
                    .max_h(TABLE_LIST_MAX_HEIGHT)
                    .overflow_y_scroll()
                    .flex()
                    .flex_col()
                    .gap(ModalMetrics::BODY_GAP)
                    .children(cards),
            )
            .when(!self.rows.is_empty() && !self.supports_truncate, |body| {
                body.child(info_line(
                    dbflux_i18n::t!("document.import_wizard.configure.truncate_unavailable"),
                    "import-wizard-truncate-unavailable",
                    cx,
                ))
            })
            .into_any_element()
    }

    /// One "table — mode" line per table the confirmation covers.
    pub(super) fn destructive_lines(&self) -> Vec<String> {
        self.rows
            .iter()
            .filter(|row| row.config.is_destructive())
            .map(|row| {
                dbflux_i18n::t!(
                    "document.import_wizard.confirm.table_line",
                    table = row.config.target_table.as_str(),
                    mode = crate::labels::import_mapping_mode_label(row.config.mapping_mode)
                )
            })
            .collect()
    }

    pub(super) fn render_confirm(&self, cx: &mut Context<Self>) -> AnyElement {
        let danger = cx.theme().danger;

        div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(modal_lead(
                dbflux_i18n::t!("document.import_wizard.confirm.body"),
                cx,
            ))
            .child(selectable(
                "import-wizard-confirm-block",
                BannerBlock::new(
                    BannerVariant::Danger,
                    dbflux_i18n::t!("document.import_wizard.confirm.warning"),
                )
                .with_icon(
                    Icon::new(AppIcon::CircleAlert)
                        .size(Feedback::BANNER_ICON)
                        .color(danger),
                )
                .with_body(self.destructive_lines().join("\n")),
            ))
            .into_any_element()
    }

    pub(super) fn render_running(&self, cx: &mut Context<Self>) -> AnyElement {
        let (rows_done, estimated_total) = *self.progress.lock().unwrap_or_else(|p| p.into_inner());
        let fraction = wizard_progress_fraction(rows_done, estimated_total);
        let label = match estimated_total {
            Some(total) if total > 0 => dbflux_i18n::t!(
                "document.import_wizard.running.progress.of_total",
                done = rows_done,
                total = total
            ),
            _ => dbflux_i18n::t!(
                "document.import_wizard.running.progress.only",
                done = rows_done
            ),
        };

        div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(modal_lead(
                dbflux_i18n::t!("document.import_wizard.running.title"),
                cx,
            ))
            .when_some(fraction, |body, fraction| {
                body.child(render_wizard_progress_bar(fraction, cx))
            })
            .child(modal_hint(label, cx))
            .into_any_element()
    }

    pub(super) fn render_done(&self, cx: &mut Context<Self>) -> AnyElement {
        let failed = self
            .result_rows
            .iter()
            .any(|row| row.kind == ResultKind::Failed);

        div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .when_some(self.result_summary.clone(), |body, summary| {
                body.child(modal_lead(summary, cx))
            })
            .when(!self.result_rows.is_empty(), |body| {
                body.child(render_result_rows(
                    &self.result_rows,
                    "import-wizard-result",
                    cx,
                ))
            })
            .when(!self.result_notes.is_empty(), |body| {
                body.child(modal_hint(self.result_notes.join("; "), cx))
            })
            .when(failed, |body| {
                body.child(info_line(
                    dbflux_i18n::t!("document.import_wizard.done.closing_failed"),
                    "import-wizard-closing-line",
                    cx,
                ))
            })
            .into_any_element()
    }

    /// Why the footer's Continue is off on this step, or `None`.
    fn footer_reason_text(&self) -> Option<String> {
        match self.step {
            WizardStep::PickFolder if self.loading && self.loading_folder => Some(dbflux_i18n::t!(
                "document.import_wizard.pick_folder.reading_hint"
            )),
            WizardStep::PickFolder if self.loading => Some(dbflux_i18n::t!(
                "document.import_wizard.pick_file.reading_hint"
            )),
            WizardStep::Configure if self.file_import_blocked() => Some(dbflux_i18n::t!(
                "document.import_wizard.footer.reason.file_read"
            )),
            _ => None,
        }
    }

    fn footer_cancel(&self, cx: &mut Context<Self>) -> Div {
        selectable(
            "import-wizard-cancel",
            Button::new(
                "import-wizard-cancel",
                dbflux_i18n::t!("document.import_wizard.footer.cancel"),
            )
            .on_click(cx.listener(|this, _, _, cx| this.close(cx))),
        )
    }

    /// The step's buttons, the primary one last. The group never shrinks.
    fn footer_buttons(&self, cancel_requested: bool, cx: &mut Context<Self>) -> Div {
        let step = self.step;

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(ModalMetrics::FOOTER_GAP)
            .when(step == WizardStep::Confirm, |footer| {
                footer.child(selectable(
                    "import-wizard-cancel-confirm",
                    Button::new(
                        "import-wizard-cancel-confirm",
                        dbflux_i18n::t!("document.import_wizard.confirm.back"),
                    )
                    .icon(AppIcon::ChevronLeft)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.step = WizardStep::Configure;
                        this.confirmed_destructive = false;
                        cx.notify();
                    })),
                ))
            })
            .when(
                matches!(
                    step,
                    WizardStep::PickFolder | WizardStep::Configure | WizardStep::Confirm
                ),
                |footer| footer.child(self.footer_cancel(cx)),
            )
            .when(step == WizardStep::Configure, |footer| {
                footer.child(selectable(
                    "import-wizard-continue",
                    Button::new(
                        "import-wizard-continue",
                        dbflux_i18n::t!("document.import_wizard.configure.continue"),
                    )
                    .primary()
                    .icon(AppIcon::ChevronRight)
                    .disabled(self.file_import_blocked())
                    .on_click(cx.listener(|this, _, _, cx| this.continue_from_configure(cx))),
                ))
            })
            .when(step == WizardStep::Confirm, |footer| {
                footer.child(selectable(
                    "import-wizard-confirm-destructive",
                    Button::new(
                        "import-wizard-confirm-destructive",
                        dbflux_i18n::t!("document.import_wizard.confirm.proceed"),
                    )
                    .primary()
                    .on_click(cx.listener(|this, _, _, cx| this.confirm_destructive_and_run(cx))),
                ))
            })
            .when(step == WizardStep::Running, |footer| {
                footer.child(selectable(
                    "import-wizard-cancel-run",
                    Button::new(
                        "import-wizard-cancel-run",
                        dbflux_i18n::t!("document.import_wizard.running.cancel"),
                    )
                    .danger()
                    .icon(AppIcon::CircleX)
                    .disabled(cancel_requested)
                    .on_click(cx.listener(|this, _, _, cx| this.cancel_run(cx))),
                ))
            })
            .when(step == WizardStep::Done, |footer| {
                footer.child(selectable(
                    "import-wizard-close",
                    Button::new(
                        "import-wizard-close",
                        dbflux_i18n::t!("document.import_wizard.done.close"),
                    )
                    .primary()
                    .on_click(cx.listener(|this, _, _, cx| this.close(cx))),
                ))
            })
    }

    /// The shared footer: the reason a button is off at the start, then the
    /// step's buttons, the primary one last.
    pub(super) fn render_footer(&self, cx: &mut Context<Self>) -> AnyElement {
        let cancel_requested = self
            .cancel_token
            .as_ref()
            .is_none_or(CancelToken::is_cancelled);
        let reason = self.footer_reason_text();

        div()
            .debug_selector(|| "import-wizard-footer".to_string())
            .flex()
            .flex_1()
            .min_w_0()
            .items_center()
            .justify_end()
            .gap(ModalMetrics::FOOTER_GAP)
            .when_some(reason, |footer, reason| {
                footer.child(footer_reason(reason, "import-wizard-footer-reason", cx))
            })
            .child(self.footer_buttons(cancel_requested, cx))
            .into_any_element()
    }
}

#[cfg(test)]
mod tests;
