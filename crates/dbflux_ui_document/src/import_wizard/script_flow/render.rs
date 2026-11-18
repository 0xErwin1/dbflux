//! The steps of the "Run SQL script" flow, drawn from the shared wizard
//! blocks: the file and its contents on Configure, the summary on Confirm,
//! the progress on Running and what happened on Done, with the footer each
//! step needs.

use dbflux_components::composites::{render_wizard_progress_bar, wizard_progress_fraction};
use dbflux_components::controls::{Button, Checkbox};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::{modal_code, modal_frame, modal_hint, modal_lead};
use dbflux_components::primitives::{
    Badge, BadgeTone, BannerBlock, BannerVariant, Icon, SegmentedControl, SegmentedItem, Text,
};
use dbflux_components::tokens::{Feedback, ModalMetrics, Spacing};
use dbflux_core::{DangerousQueryKind, ExecutionClassification};
use dbflux_transfer::sql_script::{OnError, StatementInfo};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

use super::messages::{DoneTone, DoneView};
use super::options::{TransactionAvailability, counted, grouped, size_label};
use super::{DESTRUCTIVE_SHOWN, ScriptFlow};
use crate::export_wizard::notes::{ResultKind, ResultRow, form_row, info_line, render_result_rows};
use crate::import_wizard::chrome::{error_block, selectable};
use crate::import_wizard::keyboard::{ConfigureField, with_cursor_ring};
use crate::import_wizard::{ImportWizard, WizardStep};

const SEGMENT_STOP: &str = "stop";
const SEGMENT_CONTINUE: &str = "continue";

/// The classification's name, as the editor and the audit viewer show it.
pub(in crate::import_wizard) fn classification_label(
    classification: ExecutionClassification,
) -> String {
    match classification {
        ExecutionClassification::Metadata => {
            dbflux_i18n::t!("document.import_wizard.script.classification.metadata")
        }
        ExecutionClassification::Read => {
            dbflux_i18n::t!("document.import_wizard.script.classification.read")
        }
        ExecutionClassification::Write => {
            dbflux_i18n::t!("document.import_wizard.script.classification.write")
        }
        ExecutionClassification::Destructive => {
            dbflux_i18n::t!("document.import_wizard.script.classification.destructive")
        }
        ExecutionClassification::AdminSafe => {
            dbflux_i18n::t!("document.import_wizard.script.classification.admin_safe")
        }
        ExecutionClassification::Admin => {
            dbflux_i18n::t!("document.import_wizard.script.classification.admin")
        }
        ExecutionClassification::AdminDestructive => {
            dbflux_i18n::t!("document.import_wizard.script.classification.admin_destructive")
        }
    }
}

fn classification_tone(classification: ExecutionClassification) -> BadgeTone {
    match classification {
        ExecutionClassification::Metadata => BadgeTone::Neutral,
        ExecutionClassification::Read => BadgeTone::Info,
        ExecutionClassification::Write | ExecutionClassification::AdminSafe => BadgeTone::Warning,
        ExecutionClassification::Destructive
        | ExecutionClassification::Admin
        | ExecutionClassification::AdminDestructive => BadgeTone::Danger,
    }
}

/// Whether the statement is a `DELETE` or `UPDATE` with no `WHERE`, which the
/// preview marks.
fn lacks_where(statement: &StatementInfo) -> bool {
    matches!(
        statement.danger,
        Some(DangerousQueryKind::DeleteNoWhere | DangerousQueryKind::UpdateNoWhere)
    )
}

impl ImportWizard {
    pub(in crate::import_wizard) fn render_script_step(
        &self,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let Some(flow) = self.script.as_ref() else {
            return div().into_any_element();
        };

        match self.step {
            WizardStep::Confirm => self.render_script_confirm(flow, cx),
            WizardStep::Running => self.render_script_running(flow, cx),
            WizardStep::Done => Self::render_script_done(flow, cx),
            WizardStep::PickFolder | WizardStep::Configure => {
                self.render_script_configure(flow, cx)
            }
        }
    }

    fn render_script_configure(&self, flow: &ScriptFlow, cx: &mut Context<Self>) -> AnyElement {
        let active = self.cursor_field();

        div()
            .id("import-wizard-script-configure")
            .debug_selector(|| "import-wizard-script-configure".to_string())
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(Text::caption(dbflux_i18n::t!(
                "document.import_wizard.script.configure.file",
                file = flow.path.display().to_string()
            )))
            .child(script_summary_row(flow, cx))
            .child(preview_row(flow, cx))
            .child(self.on_error_row(flow, active, cx))
            .child(self.transaction_row(flow, active, cx))
            .when_some(self.script_read_only_block(), |body, block| {
                body.child(error_block(&block, "import-wizard-script-read-only", cx))
            })
            .into_any_element()
    }

    fn on_error_row(
        &self,
        flow: &ScriptFlow,
        active: Option<ConfigureField>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let entity = cx.entity().downgrade();
        let selected = match flow.options.on_error {
            OnError::Stop => SEGMENT_STOP,
            OnError::Continue => SEGMENT_CONTINUE,
        };
        let picker = SegmentedControl::new(
            vec![
                SegmentedItem::new(
                    SEGMENT_STOP,
                    dbflux_i18n::t!("document.import_wizard.script.configure.on_error_stop"),
                ),
                SegmentedItem::new(
                    SEGMENT_CONTINUE,
                    dbflux_i18n::t!("document.import_wizard.script.configure.on_error_continue"),
                ),
            ],
            selected,
            move |segment_id, _window, cx| {
                let on_error = if segment_id.as_ref() == SEGMENT_CONTINUE {
                    OnError::Continue
                } else {
                    OnError::Stop
                };
                entity
                    .update(cx, |this, cx| this.set_script_on_error(on_error, cx))
                    .ok();
            },
        )
        .group("import-script-on-error");
        let hint = match flow.options.on_error {
            OnError::Stop => {
                dbflux_i18n::t!("document.import_wizard.script.configure.on_error_stop_hint")
            }
            OnError::Continue => {
                dbflux_i18n::t!("document.import_wizard.script.configure.on_error_continue_hint")
            }
        };

        form_row(
            dbflux_i18n::t!("document.import_wizard.script.configure.on_error_label"),
            div()
                .debug_selector(|| "import-wizard-script-on-error".to_string())
                .flex()
                .child(with_cursor_ring(
                    active,
                    ConfigureField::OnError,
                    picker,
                    cx,
                )),
            Some(modal_hint(hint, cx)),
            cx,
        )
        .into_any_element()
    }

    /// The transaction option: the checkbox when the user can choose, else
    /// the reason it is off or missing. A connection that commits schema
    /// statements on its own says so next to it.
    fn transaction_row(
        &self,
        flow: &ScriptFlow,
        active: Option<ConfigureField>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let availability = flow.options.availability(&flow.rules);
        let label = dbflux_i18n::t!("document.import_wizard.script.configure.transaction_label");

        let control = match availability {
            TransactionAvailability::Unavailable | TransactionAvailability::SelfManaged => {
                info_line(
                    transaction_reason(flow).unwrap_or_default(),
                    "import-wizard-script-transaction-why",
                    cx,
                )
            }
            _ => self.transaction_checkbox(flow, availability, active, cx),
        };
        let mut notes: Vec<AnyElement> = Vec::new();

        match availability {
            TransactionAvailability::OffForContinue => notes.push(info_line(
                dbflux_i18n::t!("document.import_wizard.script.configure.transaction_off_continue"),
                "import-wizard-script-transaction-why",
                cx,
            )),
            TransactionAvailability::Available => notes.push(modal_hint(
                if flow.options.transaction {
                    dbflux_i18n::t!("document.import_wizard.script.configure.transaction_hint")
                } else {
                    dbflux_i18n::t!(
                        "document.import_wizard.script.configure.transaction_unchecked_hint"
                    )
                },
                cx,
            )),
            TransactionAvailability::Unavailable | TransactionAvailability::SelfManaged => {}
        }
        if flow.summary.ddl_commits_on_its_own && flow.rules.transactions_supported {
            notes.push(info_line(
                dbflux_i18n::t!("document.import_wizard.script.configure.ddl_commits"),
                "import-wizard-script-ddl-commits",
                cx,
            ));
        }

        form_row(
            label,
            div()
                .debug_selector(|| "import-wizard-script-transaction-row".to_string())
                .flex()
                .flex_col()
                .gap(ModalMetrics::FIELD_GAP)
                .child(control)
                .children(notes),
            None,
            cx,
        )
        .into_any_element()
    }

    fn transaction_checkbox(
        &self,
        flow: &ScriptFlow,
        availability: TransactionAvailability,
        active: Option<ConfigureField>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let available = availability == TransactionAvailability::Available;
        let checkbox = Checkbox::new("import-wizard-script-transaction")
            .checked(available && flow.options.transaction)
            .disabled(!available)
            .label(dbflux_i18n::t!(
                "document.import_wizard.script.configure.transaction_checkbox"
            ))
            .on_click(cx.listener(|this, checked: &bool, _, cx| {
                this.set_script_transaction(*checked, cx);
            }));

        div()
            .flex()
            .child(with_cursor_ring(
                active,
                ConfigureField::Transaction,
                checkbox,
                cx,
            ))
            .into_any_element()
    }

    fn render_script_confirm(&self, flow: &ScriptFlow, cx: &mut Context<Self>) -> AnyElement {
        let summary = &flow.summary;
        let how = how_it_runs(flow);
        let body = if summary.destructive_count > 0 {
            destructive_confirm(flow, cx)
        } else {
            plain_confirm(flow, cx)
        };

        div()
            .id("import-wizard-script-confirm")
            .debug_selector(|| "import-wizard-script-confirm".to_string())
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(body)
            .child(modal_hint(how, cx))
            .into_any_element()
    }

    fn render_script_running(&self, flow: &ScriptFlow, cx: &mut Context<Self>) -> AnyElement {
        let (index, total) = *self.progress.lock().unwrap_or_else(|p| p.into_inner());
        let total = total.unwrap_or(flow.summary.statement_count as u64);
        let snippet = flow
            .snippet
            .lock()
            .map(|snippet| snippet.clone())
            .unwrap_or_default();
        let transaction = flow.options.runs_in_transaction(&flow.rules);
        let fraction = wizard_progress_fraction(index, Some(total));

        div()
            .id("import-wizard-script-running")
            .debug_selector(|| "import-wizard-script-running".to_string())
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(Text::caption(dbflux_i18n::t!(
                "document.import_wizard.script.configure.file",
                file = flow.path.display().to_string()
            )))
            .child(modal_lead(
                dbflux_i18n::t!(
                    "document.import_wizard.script.running.title",
                    index = grouped(index as usize),
                    total = grouped(total as usize)
                ),
                cx,
            ))
            .when_some(fraction, |body, fraction| {
                body.child(render_wizard_progress_bar(fraction, cx))
            })
            .child(modal_hint(
                dbflux_i18n::t!("document.import_wizard.script.running.current"),
                cx,
            ))
            .child(selectable(
                "import-wizard-script-snippet",
                modal_code(
                    if snippet.is_empty() {
                        dbflux_i18n::t!("document.import_wizard.script.running.starting")
                    } else {
                        snippet
                    },
                    cx,
                ),
            ))
            .child(info_line(
                if transaction {
                    dbflux_i18n::t!("document.import_wizard.script.running.transaction_line")
                } else {
                    dbflux_i18n::t!("document.import_wizard.script.running.no_transaction_line")
                },
                "import-wizard-script-running-mode",
                cx,
            ))
            .into_any_element()
    }

    fn render_script_done(flow: &ScriptFlow, cx: &mut Context<Self>) -> AnyElement {
        let Some(view) = flow.done.as_ref() else {
            return div().into_any_element();
        };
        let danger = cx.theme().danger;

        div()
            .id("import-wizard-script-done")
            .debug_selector(|| "import-wizard-script-done".to_string())
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(selectable(
                "import-wizard-script-headline",
                modal_lead(view.headline.clone(), cx),
            ))
            .when_some(view.block.as_ref(), |body, block| {
                body.child(selectable(
                    "import-wizard-script-done-block",
                    BannerBlock::new(BannerVariant::Danger, block.message.clone())
                        .with_icon(
                            Icon::new(AppIcon::CircleAlert)
                                .size(Feedback::BANNER_ICON)
                                .color(danger),
                        )
                        .with_body(block.next_step.clone()),
                ))
            })
            .when_some(view.failed_statement.as_ref(), |body, failed| {
                body.child(modal_hint(failed.label.clone(), cx))
                    .child(selectable(
                        "import-wizard-script-failed-statement",
                        modal_code(failed.snippet.clone(), cx),
                    ))
            })
            .child(done_notes(view, cx))
            .when(!view.failures.is_empty(), |body| {
                body.child(render_result_rows(
                    &failure_rows(view),
                    "import-wizard-script-failure",
                    cx,
                ))
            })
            .when_some(
                view.next_step
                    .as_ref()
                    .filter(|_| view.error_text.is_none()),
                |body, next| {
                    body.child(info_line(
                        next.clone(),
                        "import-wizard-script-next-step",
                        cx,
                    ))
                },
            )
            .into_any_element()
    }

    /// Why the footer's primary button is off, or what the user is waiting
    /// for on a run.
    pub(in crate::import_wizard) fn script_footer_reason(&self) -> Option<String> {
        let flow = self.script.as_ref()?;

        match self.step {
            WizardStep::Configure if flow.read_only_conflict().is_some() => Some(dbflux_i18n::t!(
                "document.import_wizard.script.footer.read_only"
            )),
            WizardStep::Running => {
                let transaction = flow.options.runs_in_transaction(&flow.rules);
                let cancelling = self
                    .cancel_token
                    .as_ref()
                    .is_some_and(dbflux_core::CancelToken::is_cancelled);

                Some(match (cancelling, transaction) {
                    (false, true) => {
                        dbflux_i18n::t!("document.import_wizard.script.footer.running_transaction")
                    }
                    (false, false) => {
                        dbflux_i18n::t!("document.import_wizard.script.footer.running_kept")
                    }
                    (true, true) => {
                        dbflux_i18n::t!(
                            "document.import_wizard.script.footer.cancelling_transaction"
                        )
                    }
                    (true, false) => {
                        dbflux_i18n::t!("document.import_wizard.script.footer.cancelling_kept")
                    }
                })
            }
            _ => None,
        }
    }

    /// The Confirm step's primary button: "Yes, run script" for a script that
    /// can destroy data, "Run script" for the rest.
    fn confirm_primary(destructive: bool, cx: &mut Context<Self>) -> Div {
        let (id, label) = if destructive {
            (
                "import-wizard-confirm-destructive",
                dbflux_i18n::t!("document.import_wizard.script.confirm.proceed"),
            )
        } else {
            (
                "import-wizard-run-script",
                dbflux_i18n::t!("document.import_wizard.script.confirm.run"),
            )
        };

        selectable(
            id,
            Button::new(id, label)
                .primary()
                .on_click(cx.listener(|this, _, _, cx| this.run_script_confirmed(cx))),
        )
    }

    /// The step's buttons, the primary one last.
    pub(in crate::import_wizard) fn script_footer_buttons(
        &self,
        cancel_requested: bool,
        cx: &mut Context<Self>,
    ) -> Div {
        let step = self.step;
        let destructive = self
            .script
            .as_ref()
            .is_some_and(|flow| flow.summary.destructive_count > 0);
        let blocked = self.loading
            || self
                .script
                .as_ref()
                .is_some_and(|flow| flow.read_only_conflict().is_some());

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
                matches!(step, WizardStep::Configure | WizardStep::Confirm),
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
                    .disabled(blocked)
                    .on_click(cx.listener(|this, _, _, cx| this.continue_script(cx))),
                ))
            })
            .when(step == WizardStep::Confirm, |footer| {
                footer.child(Self::confirm_primary(destructive, cx))
            })
            .when(step == WizardStep::Running, |footer| {
                footer.child(selectable(
                    "import-wizard-cancel-run",
                    Button::new(
                        "import-wizard-cancel-run",
                        if cancel_requested {
                            dbflux_i18n::t!("document.import_wizard.script.running.cancelling")
                        } else {
                            dbflux_i18n::t!("document.import_wizard.script.running.cancel")
                        },
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
}

/// The "Script" row: the statement count and size, over a chip per class.
fn script_summary_row(flow: &ScriptFlow, cx: &App) -> AnyElement {
    let summary = &flow.summary;
    let chips: Vec<AnyElement> = summary
        .by_classification
        .iter()
        .map(|(classification, count)| {
            Badge::new(
                format!(
                    "{} {}",
                    classification_label(*classification),
                    grouped(*count)
                ),
                classification_tone(*classification),
            )
            .into_any_element()
        })
        .collect();

    form_row(
        dbflux_i18n::t!("document.import_wizard.script.configure.script_label"),
        div()
            .debug_selector(|| "import-wizard-script-summary".to_string())
            .flex()
            .flex_col()
            .gap(ModalMetrics::FIELD_GAP)
            .child(counted(
                "document.import_wizard.script.configure.script_summary",
                summary.statement_count,
                &[("size", size_label(summary.bytes))],
            ))
            .child(
                div()
                    .debug_selector(|| "import-wizard-script-chips".to_string())
                    .flex()
                    .flex_wrap()
                    .gap(Spacing::XS)
                    .children(chips),
            ),
        None,
        cx,
    )
    .into_any_element()
}

/// The first statements of the script with their classification.
fn preview_row(flow: &ScriptFlow, cx: &App) -> AnyElement {
    let summary = &flow.summary;
    let rows: Vec<AnyElement> = summary
        .preview
        .iter()
        .map(|statement| statement_row(statement, cx))
        .collect();
    let more = summary
        .statement_count
        .saturating_sub(summary.preview.len());

    form_row(
        dbflux_i18n::t!("document.import_wizard.script.configure.preview_label"),
        div()
            .debug_selector(|| "import-wizard-script-preview".to_string())
            .flex()
            .flex_col()
            .gap(ModalMetrics::FIELD_GAP)
            .child(
                modal_frame(cx)
                    .flex()
                    .flex_col()
                    .children(rows)
                    .when(more > 0, |list| {
                        list.child(div().px(Spacing::SM).py(Spacing::XS).child(modal_hint(
                            counted(
                                "document.import_wizard.script.configure.preview_more",
                                more,
                                &[],
                            ),
                            cx,
                        )))
                    }),
            )
            .child(modal_hint(
                dbflux_i18n::t!("document.import_wizard.script.configure.preview_hint"),
                cx,
            ))
            .child(modal_hint(
                dbflux_i18n::t!("document.import_wizard.script.configure.preview_sequence_hint"),
                cx,
            )),
        None,
        cx,
    )
    .into_any_element()
}

fn statement_row(statement: &StatementInfo, cx: &App) -> AnyElement {
    let theme = cx.theme();
    let selector = format!("import-wizard-script-statement-{}", statement.index);

    div()
        .debug_selector(move || selector)
        .flex()
        .items_center()
        .gap(Spacing::SM)
        .px(Spacing::SM)
        .py(Spacing::XS)
        .font_family(dbflux_components::fonts::editor_family(cx))
        .text_size(ModalMetrics::CODE_FONT)
        .child(
            div()
                .flex_shrink_0()
                .text_color(theme.muted_foreground)
                .child(grouped(statement.index)),
        )
        .child(div().flex_shrink_0().child(Badge::new(
            classification_label(statement.classification),
            classification_tone(statement.classification),
        )))
        .child(
            div()
                .flex_1()
                .min_w_0()
                .overflow_hidden()
                .whitespace_nowrap()
                .text_ellipsis()
                .text_color(theme.foreground)
                .child(statement.snippet.clone()),
        )
        .when(lacks_where(statement), |row| {
            row.child(
                div()
                    .flex_shrink_0()
                    .text_color(theme.danger)
                    .child(dbflux_i18n::t!(
                        "document.import_wizard.script.configure.no_where"
                    )),
            )
        })
        .into_any_element()
}

/// The confirmation of a script with destructive statements: the danger block
/// with the counts by kind and the first few statements.
fn destructive_confirm(flow: &ScriptFlow, cx: &App) -> AnyElement {
    let summary = &flow.summary;
    let danger = cx.theme().danger;
    let kinds: Vec<String> = summary
        .by_danger
        .iter()
        .map(|(kind, count)| {
            dbflux_i18n::t!(
                "document.import_wizard.script.confirm.danger_line",
                count = grouped(*count),
                message = crate::labels::dangerous_query_body(*kind)
            )
        })
        .collect();
    let shown = summary.first_destructive.len().min(DESTRUCTIVE_SHOWN);
    let first: Vec<String> = summary
        .first_destructive
        .iter()
        .take(shown)
        .map(|statement| {
            dbflux_i18n::t!(
                "document.import_wizard.script.confirm.first_line",
                index = grouped(statement.index),
                snippet = statement.snippet
            )
        })
        .collect();
    let mut lines = kinds;
    lines.push(dbflux_i18n::t!(
        "document.import_wizard.script.confirm.first_label",
        shown = grouped(shown),
        total = grouped(summary.destructive_count)
    ));
    lines.extend(first);

    div()
        .flex()
        .flex_col()
        .gap(ModalMetrics::BODY_GAP)
        .child(modal_lead(
            counted(
                "document.import_wizard.script.confirm.destructive_lead",
                summary.destructive_count,
                &[("total", grouped(summary.statement_count))],
            ),
            cx,
        ))
        .child(selectable(
            "import-wizard-script-confirm-danger",
            BannerBlock::new(
                BannerVariant::Danger,
                dbflux_i18n::t!("document.import_wizard.script.confirm.warning"),
            )
            .with_icon(
                Icon::new(AppIcon::CircleAlert)
                    .size(Feedback::BANNER_ICON)
                    .color(danger),
            )
            .with_body(lines.join("\n")),
        ))
        .into_any_element()
}

/// The confirmation of a script with nothing destructive: the counts by
/// classification.
fn plain_confirm(flow: &ScriptFlow, cx: &App) -> AnyElement {
    let summary = &flow.summary;
    let rows: Vec<AnyElement> = summary
        .by_classification
        .iter()
        .map(|(classification, count)| {
            div()
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .px(Spacing::SM)
                .py(Spacing::XS)
                .child(Badge::new(
                    classification_label(*classification),
                    classification_tone(*classification),
                ))
                .child(grouped(*count))
                .into_any_element()
        })
        .collect();

    div()
        .flex()
        .flex_col()
        .gap(ModalMetrics::BODY_GAP)
        .child(modal_lead(
            counted(
                "document.import_wizard.script.confirm.plain_lead",
                summary.statement_count,
                &[],
            ),
            cx,
        ))
        .child(selectable(
            "import-wizard-script-confirm-plain",
            modal_frame(cx).flex().flex_col().children(rows),
        ))
        .into_any_element()
}

/// One sentence on how the run goes: in one transaction or statement by
/// statement, and what a failed statement does.
pub(super) fn how_it_runs(flow: &ScriptFlow) -> String {
    let transaction = flow.options.runs_in_transaction(&flow.rules);

    if flow.options.availability(&flow.rules) == TransactionAvailability::SelfManaged {
        return match flow.options.on_error {
            OnError::Stop => {
                dbflux_i18n::t!("document.import_wizard.script.confirm.how_self_stop")
            }
            OnError::Continue => {
                dbflux_i18n::t!("document.import_wizard.script.confirm.how_self_continue")
            }
        };
    }

    match (transaction, flow.options.on_error) {
        (true, _) => dbflux_i18n::t!("document.import_wizard.script.confirm.how_transaction"),
        (false, OnError::Stop) => {
            dbflux_i18n::t!("document.import_wizard.script.confirm.how_each_stop")
        }
        (false, OnError::Continue) => {
            dbflux_i18n::t!("document.import_wizard.script.confirm.how_each_continue")
        }
    }
}

/// The database's error and what happened to the changes, or the outcome line
/// of a run that did not fail.
fn done_notes(view: &DoneView, cx: &App) -> AnyElement {
    let danger = cx.theme().danger;

    if let Some(error) = view.error_text.as_ref() {
        let mut details = Vec::new();
        details.extend(view.outcome.clone());
        details.extend(view.next_step.clone());

        return selectable(
            "import-wizard-script-error",
            BannerBlock::new(BannerVariant::Danger, error.clone())
                .with_icon(
                    Icon::new(AppIcon::CircleAlert)
                        .size(Feedback::BANNER_ICON)
                        .color(danger),
                )
                .with_body(details.join("\n")),
        )
        .into_any_element();
    }

    match view.outcome.as_ref() {
        Some(outcome) => {
            let kind = match view.tone {
                DoneTone::Success => ResultKind::Completed,
                DoneTone::Warning => ResultKind::Cancelled,
                DoneTone::Danger => ResultKind::Failed,
            };

            render_result_rows(
                &[ResultRow {
                    kind,
                    text: outcome.clone(),
                }],
                "import-wizard-script-outcome",
                cx,
            )
            .into_any_element()
        }
        None => div().into_any_element(),
    }
}

fn failure_rows(view: &DoneView) -> Vec<ResultRow> {
    view.failures
        .iter()
        .map(|failure| ResultRow {
            kind: ResultKind::Failed,
            text: format!("{} {}: {}", failure.label, failure.snippet, failure.error),
        })
        .collect()
}

/// The reason line of a transaction that is off or missing, if any.
pub(super) fn transaction_reason(flow: &ScriptFlow) -> Option<String> {
    match flow.options.availability(&flow.rules) {
        TransactionAvailability::Available => None,
        TransactionAvailability::Unavailable => Some(dbflux_i18n::t!(
            "document.import_wizard.script.configure.transaction_unavailable"
        )),
        TransactionAvailability::OffForContinue => Some(dbflux_i18n::t!(
            "document.import_wizard.script.configure.transaction_off_continue"
        )),
        TransactionAvailability::SelfManaged => {
            let (keyword, line) = flow.rules.self_managed?;
            let key = if matches!(keyword, "COMMIT" | "END") {
                "document.import_wizard.script.configure.transaction_self_managed_commit"
            } else {
                "document.import_wizard.script.configure.transaction_self_managed"
            };

            Some(dbflux_i18n::t!(
                key,
                keyword = keyword,
                line = grouped(line)
            ))
        }
    }
}
