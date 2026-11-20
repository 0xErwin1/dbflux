//! The export wizard's Tables, Confirm and Run steps and its footer.

use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::{modal_frame, modal_hint, modal_lead};
use dbflux_components::primitives::Icon;
use dbflux_components::tokens::{ChromeColors, ModalMetrics};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

use super::notes::{ResultKind, alert_hint, footer_reason, info_line, render_result_rows};
use super::phases::{ExportPhase, RunState, next_phase, prev_phase};
use super::{ExportWizard, TABLE_LIST_MAX_HEIGHT, TABLE_ROW_HEIGHT};
use dbflux_components::composites::{render_wizard_progress_bar, wizard_progress_fraction};

/// The field that keeps the footer's Continue off.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ContinueBlock {
    Tables,
    BatchSize,
    SegmentSize,
    Folder,
}

impl ContinueBlock {
    pub(super) fn text(self) -> String {
        match self {
            Self::Tables => dbflux_i18n::t!("document.export_wizard.footer.reason.tables"),
            Self::BatchSize => dbflux_i18n::t!("document.export_wizard.footer.reason.batch_size"),
            Self::SegmentSize => {
                dbflux_i18n::t!("document.export_wizard.footer.reason.segment_size")
            }
            Self::Folder => dbflux_i18n::t!("document.export_wizard.footer.reason.folder"),
        }
    }
}

impl ExportWizard {
    pub(super) fn render_tables(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let last_row = self.tables.len().saturating_sub(1);

        let rows = self.tables.iter().enumerate().map(|(index, table)| {
            div()
                .flex()
                .flex_shrink_0()
                .items_center()
                .gap(ModalMetrics::LIST_ROW_GAP)
                .h(TABLE_ROW_HEIGHT)
                .px(ModalMetrics::LIST_ROW_PADDING_X)
                .when(index < last_row, |row| {
                    row.border_b_1().border_color(theme.table_row_border)
                })
                .child(
                    Icon::new(AppIcon::Table)
                        .size(ModalMetrics::LIST_ICON)
                        .color(theme.muted_foreground),
                )
                .child(
                    div()
                        .min_w_0()
                        .truncate()
                        .font_family(dbflux_components::fonts::editor_family(cx))
                        .text_size(ModalMetrics::CODE_FONT)
                        .text_color(ChromeColors::strong(theme))
                        .child(table.qualified_name()),
                )
        });

        div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(modal_lead(
                dbflux_i18n::t!(
                    "document.export_wizard.tables.selected_count",
                    count = self.tables.len()
                ),
                cx,
            ))
            .child(
                modal_frame(cx).child(
                    div()
                        .id("export-wizard-tables")
                        .flex()
                        .flex_col()
                        .max_h(TABLE_LIST_MAX_HEIGHT)
                        .overflow_y_scroll()
                        .children(rows),
                ),
            )
            .when(self.tables.is_empty(), |body| {
                body.child(alert_hint(
                    dbflux_i18n::t!("document.export_wizard.tables.required"),
                    "export-wizard-tables-required",
                    cx,
                ))
            })
            .into_any_element()
    }

    pub(super) fn render_confirm(&self, cx: &mut Context<Self>) -> AnyElement {
        let folder_label = self
            .output_dir
            .as_ref()
            .map(|dir| dir.display().to_string())
            .unwrap_or_default();

        div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(modal_lead(
                dbflux_i18n::t!(
                    "document.export_wizard.confirm.summary",
                    count = self.tables.len(),
                    format = self.selected_format().label(),
                    folder = folder_label
                ),
                cx,
            ))
            .child(modal_hint(
                dbflux_i18n::t!(
                    "document.export_wizard.confirm.segment_size",
                    size = self.segment_size
                ),
                cx,
            ))
            .into_any_element()
    }

    pub(super) fn render_run(&self, cx: &mut Context<Self>) -> AnyElement {
        match self.run_state {
            RunState::Idle => div().into_any_element(),
            RunState::Running => self.render_running(cx),
            RunState::Done => self.render_done(cx),
        }
    }

    fn render_running(&self, cx: &mut Context<Self>) -> AnyElement {
        let progress = *self.progress.lock().unwrap_or_else(|p| p.into_inner());
        let names: Vec<String> = self.tables.iter().map(|t| t.qualified_name()).collect();
        let total_tables = names.len();
        let current_index = progress.table_index.min(total_tables.saturating_sub(1));
        let current_table = names.get(current_index).cloned().unwrap_or_default();

        let rows_label =
            crate::labels::export_running_rows_label(progress.rows_done, progress.estimated_total);
        let position_label =
            crate::labels::export_running_position_label(current_index, total_tables);
        let fraction = wizard_progress_fraction(progress.rows_done, progress.estimated_total);

        div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(modal_lead(format!("{position_label}: {current_table}"), cx))
            .when_some(fraction, |el, fraction| {
                el.child(render_wizard_progress_bar(fraction, cx))
            })
            .child(modal_hint(rows_label, cx))
            .into_any_element()
    }

    fn render_done(&self, cx: &mut Context<Self>) -> AnyElement {
        let failed = self
            .result_rows
            .iter()
            .any(|row| row.kind == ResultKind::Failed);

        div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .when_some(self.result_summary.clone(), |el, summary| {
                el.child(modal_lead(summary, cx))
            })
            .when(!self.result_rows.is_empty(), |el| {
                el.child(render_result_rows(
                    &self.result_rows,
                    "export-wizard-result",
                    cx,
                ))
            })
            .when(!self.result_notes.is_empty(), |el| {
                el.child(modal_hint(self.result_notes.join("; "), cx))
            })
            .when(failed, |el| {
                el.child(info_line(
                    dbflux_i18n::t!("document.export_wizard.done.closing_failed"),
                    "export-wizard-closing-line",
                    cx,
                ))
            })
            .into_any_element()
    }

    /// Why the footer's Continue is off, or `None` while it is on. The first
    /// blocking field wins; the footer names it so the reason stays visible
    /// when the field is scrolled out of view.
    pub(super) fn continue_block_reason(&self) -> Option<ContinueBlock> {
        match self.phase {
            ExportPhase::Tables => self.tables.is_empty().then_some(ContinueBlock::Tables),
            ExportPhase::FormatOptions => {
                if self.selected_format.uses_sql_batch_size() && self.sql_batch_size_invalid {
                    Some(ContinueBlock::BatchSize)
                } else if self.segment_size_invalid {
                    Some(ContinueBlock::SegmentSize)
                } else if self.output_dir.is_none() {
                    Some(ContinueBlock::Folder)
                } else {
                    None
                }
            }
            ExportPhase::Confirm | ExportPhase::Run => None,
        }
    }

    pub(super) fn render_footer(&self, cx: &mut Context<Self>) -> AnyElement {
        let running = self.run_state == RunState::Running;
        let done = self.run_state == RunState::Done;

        let shows_back = prev_phase(self.phase).is_some() && !running && !done;
        let shows_continue = next_phase(self.phase).is_some();
        let shows_start = self.phase == ExportPhase::Confirm;
        let continue_enabled = self.continue_enabled();
        let reason = self.continue_block_reason();

        div()
            .debug_selector(|| "export-wizard-footer".to_string())
            .flex()
            .flex_1()
            .min_w_0()
            .items_center()
            .justify_end()
            .gap(ModalMetrics::FOOTER_GAP)
            .when_some(reason, |footer, reason| {
                footer.child(footer_reason(
                    reason.text(),
                    "export-wizard-footer-reason",
                    cx,
                ))
            })
            .child(
                div()
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .gap(ModalMetrics::FOOTER_GAP)
                    .when(shows_back, |footer| {
                        footer.child(
                            Button::new(
                                "export-wizard-back",
                                dbflux_i18n::t!("document.export_wizard.footer.back"),
                            )
                            .icon(AppIcon::ChevronLeft)
                            .on_click(cx.listener(|this, _event, _window, cx| this.go_back(cx))),
                        )
                    })
                    .when(
                        matches!(self.phase, ExportPhase::Tables | ExportPhase::Confirm),
                        |footer| {
                            footer.child(
                                Button::new(
                                    "export-wizard-dismiss",
                                    dbflux_i18n::t!("document.export_wizard.running.cancel"),
                                )
                                .on_click(cx.listener(|this, _event, _window, cx| this.close(cx))),
                            )
                        },
                    )
                    .when(shows_continue, |footer| {
                        footer.child(
                            Button::new(
                                "export-wizard-continue",
                                dbflux_i18n::t!("document.export_wizard.footer.continue"),
                            )
                            .primary()
                            .icon(AppIcon::ChevronRight)
                            .disabled(!continue_enabled)
                            .on_click(cx.listener(|this, _event, _window, cx| this.advance(cx))),
                        )
                    })
                    .when(shows_start, |footer| {
                        footer.child(
                            Button::new(
                                "export-wizard-start",
                                dbflux_i18n::t!("document.export_wizard.confirm.start_export"),
                            )
                            .primary()
                            .icon(AppIcon::Play)
                            .on_click(
                                cx.listener(|this, _event, _window, cx| this.start_export(cx)),
                            ),
                        )
                    })
                    .when(running, |footer| {
                        footer.child(
                            Button::new(
                                "export-wizard-cancel",
                                dbflux_i18n::t!("document.export_wizard.running.cancel"),
                            )
                            .danger()
                            .icon(AppIcon::CircleX)
                            .on_click(cx.listener(|this, _event, _window, cx| this.cancel_run(cx))),
                        )
                    })
                    .when(done, |footer| {
                        footer.child(
                            Button::new(
                                "export-wizard-close",
                                dbflux_i18n::t!("document.export_wizard.footer.close"),
                            )
                            .primary()
                            .on_click(cx.listener(|this, _event, _window, cx| this.close(cx))),
                        )
                    }),
            )
            .into_any_element()
    }
}
