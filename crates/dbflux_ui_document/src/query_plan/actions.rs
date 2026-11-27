//! The pane actions of the Plan tab: the keyboard path of every control the
//! tab draws that no key binds directly. They join the code document's pane
//! actions menu (`m` in the context bar, Shift+F10 in the editor) while the
//! Plan tab is shown.

use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::icons::AppIcon;
use gpui::{App, Entity};

use super::{PlanRunMode, PlanState, PlanView, PlanViewMode};
use crate::pane::PaneAction;

/// The menu entries for the plan `view` holds, by what the tab shows.
pub(crate) fn pane_actions(view: &Entity<PlanView>, cx: &App) -> Vec<PaneAction> {
    let plan = view.read(cx);
    let mut actions = Vec::new();

    if plan.analyze_offer().is_some() && !plan.is_loading() {
        actions.extend(run_mode_entries(view, plan));
    }

    match plan.state() {
        PlanState::Loading => {
            actions.push(
                PaneAction::command(
                    "plan-cancel",
                    dbflux_i18n::t!("document.query_plan.loading.cancel"),
                    Command::CancelQuery,
                    ContextId::Editor,
                )
                .icon(AppIcon::CircleX),
            );
        }
        PlanState::Ready(_) => {
            actions.extend(view_entries(view, plan.mode()));

            if plan.mode() == PlanViewMode::Diagram {
                actions.extend(zoom_entries(view));
            }
            if plan.mode() == PlanViewMode::Raw {
                let view = view.clone();
                actions.push(
                    PaneAction::callback(
                        "plan-copy-raw",
                        dbflux_i18n::t!("document.query_plan.raw.copy"),
                        move |_, cx| view.update(cx, |plan, cx| plan.copy_raw(cx)),
                    )
                    .icon(AppIcon::Copy),
                );
            }
        }
        PlanState::Failed(_) => {
            actions.push(retry_entry(view));

            let copy = view.clone();
            actions.push(
                PaneAction::callback(
                    "plan-copy-error",
                    dbflux_i18n::t!("document.code.execution.copy_error"),
                    move |_, cx| copy.update(cx, |plan, cx| plan.copy_error(cx)),
                )
                .icon(AppIcon::Copy),
            );

            let audit = view.clone();
            actions.push(
                PaneAction::callback(
                    "plan-view-in-audit",
                    dbflux_i18n::t!("errors.action.view_in_audit"),
                    move |_, cx| audit.update(cx, |plan, cx| plan.view_error_in_audit(cx)),
                )
                .icon(AppIcon::FingerprintPattern),
            );
        }
        PlanState::Cancelled => actions.push(retry_entry(view)),
        PlanState::AnalyzeGate => {
            let run = view.clone();
            actions.push(
                PaneAction::callback(
                    "plan-analyze-run",
                    dbflux_i18n::t!("document.query_plan.analyze.gate.run"),
                    move |_, cx| run.update(cx, |plan, cx| plan.request_analysis(cx)),
                )
                .icon(AppIcon::Play),
            );
        }
        PlanState::Unsupported { .. } | PlanState::MultiStatement { .. } => {}
    }

    actions
}

/// One entry per run mode. Analyze is listed but disabled when the statement
/// may not run, as the switch dims it.
fn run_mode_entries(view: &Entity<PlanView>, plan: &PlanView) -> Vec<PaneAction> {
    PlanRunMode::ALL
        .iter()
        .map(|mode| {
            let mode = *mode;
            let target = view.clone();
            let label = match mode {
                PlanRunMode::Estimate => dbflux_i18n::t!("document.query_plan.run_mode.estimate"),
                PlanRunMode::Analyze => dbflux_i18n::t!("document.query_plan.run_mode.analyze"),
            };
            let allowed = mode == PlanRunMode::Estimate || plan.can_analyze();

            PaneAction::callback(
                format!("plan-run-mode-{}", mode.key()),
                label,
                move |_, cx| target.update(cx, |plan, cx| plan.choose_run_mode(mode, cx)),
            )
            .icon(match mode {
                PlanRunMode::Estimate => AppIcon::Search,
                PlanRunMode::Analyze => AppIcon::Play,
            })
            .enabled(allowed && mode != plan.run_mode())
        })
        .collect()
}

fn retry_entry(view: &Entity<PlanView>) -> PaneAction {
    let view = view.clone();
    PaneAction::callback(
        "plan-retry",
        dbflux_i18n::t!("document.query_plan.state.retry"),
        move |_, cx| {
            view.update(cx, |_, cx| cx.emit(super::PlanViewEvent::RetryRequested));
        },
    )
    .icon(AppIcon::RefreshCcw)
}

/// One entry per view, so a view is chosen directly rather than by cycling.
fn view_entries(view: &Entity<PlanView>, current: PlanViewMode) -> Vec<PaneAction> {
    PlanViewMode::ALL
        .iter()
        .map(|mode| {
            let mode = *mode;
            let view = view.clone();
            let (label, icon) = match mode {
                PlanViewMode::Diagram => (
                    dbflux_i18n::t!("document.query_plan.view.diagram"),
                    AppIcon::ChartNetwork,
                ),
                PlanViewMode::Tree => (
                    dbflux_i18n::t!("document.query_plan.view.tree"),
                    AppIcon::Rows3,
                ),
                PlanViewMode::Raw => (
                    dbflux_i18n::t!("document.query_plan.view.raw"),
                    AppIcon::Code,
                ),
            };

            PaneAction::callback(format!("plan-view-{}", mode.key()), label, move |_, cx| {
                view.update(cx, |plan, cx| plan.set_mode(mode, cx))
            })
            .icon(icon)
            .enabled(mode != current)
        })
        .collect()
}

fn zoom_entries(view: &Entity<PlanView>) -> Vec<PaneAction> {
    let zoom_in = view.clone();
    let zoom_out = view.clone();
    let reset = view.clone();
    let fit = view.clone();

    vec![
        PaneAction::callback(
            "plan-zoom-in",
            dbflux_i18n::t!("document.query_plan.controls.zoom_in"),
            move |_, cx| zoom_in.update(cx, |plan, cx| plan.zoom_in(cx)),
        )
        .icon(AppIcon::ZoomIn),
        PaneAction::callback(
            "plan-zoom-out",
            dbflux_i18n::t!("document.query_plan.controls.zoom_out"),
            move |_, cx| zoom_out.update(cx, |plan, cx| plan.zoom_out(cx)),
        )
        .icon(AppIcon::ZoomOut),
        PaneAction::callback(
            "plan-zoom-reset",
            dbflux_i18n::t!("document.query_plan.controls.zoom_reset"),
            move |_, cx| reset.update(cx, |plan, cx| plan.reset_view(cx)),
        )
        .icon(AppIcon::Search),
        PaneAction::callback(
            "plan-fit",
            dbflux_i18n::t!("document.query_plan.controls.fit"),
            move |_, cx| fit.update(cx, |plan, cx| plan.fit(cx)),
        )
        .icon(AppIcon::Maximize2),
    ]
}
