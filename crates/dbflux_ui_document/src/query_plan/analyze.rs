//! Analyze: running the statement to measure it.
//!
//! An estimate only plans the statement. An analyzed plan runs it, so the
//! tab never does that on its own: choosing Analyze shows a gate that says
//! what is about to happen, and only its Run analyze button asks the
//! document to send the request. A statement the shared classifier does not
//! mark as a read cannot be analyzed at all; the switch says why.

use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Icon;
use dbflux_components::tokens::{FontSizes, ResultMetrics, Spacing};
use dbflux_core::{ConnectedProfile, ConnectionEnvironment, QueryLanguage};
use gpui::prelude::*;
use gpui::{AnyElement, Context, FontWeight, div};
use gpui_component::ActiveTheme;

use super::ids::tagged;
use super::{PlanView, PlanViewEvent};

/// Why a statement cannot be analyzed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AnalyzeRefusal {
    /// The shared classifier does not mark the statement as a read.
    ChangesData,
    /// The text holds none or several statements; a plan covers one.
    SeveralStatements,
}

impl AnalyzeRefusal {
    pub(crate) fn title(self) -> String {
        dbflux_i18n::t!("document.query_plan.analyze.refusal.title")
    }

    pub(crate) fn reason(self) -> String {
        match self {
            Self::ChangesData => {
                dbflux_i18n::t!("document.query_plan.analyze.refusal.changes_data")
            }
            Self::SeveralStatements => {
                dbflux_i18n::t!("document.query_plan.analyze.refusal.several")
            }
        }
    }
}

/// What the Estimate | Analyze switch offers for the statement a plan covers.
/// A plan without an offer is on a connection that cannot analyze, and has no
/// switch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AnalyzeOffer {
    /// The connection the statement would run on.
    pub connection: String,
    pub production: bool,
    /// Why Analyze is off for the statement, when it is.
    pub refusal: Option<AnalyzeRefusal>,
}

/// The offer for `text` on `connected`, from what the connection's driver
/// declares and what the shared guard says about the statement. `None` when
/// the driver cannot analyze.
pub(crate) fn offer_for(
    connected: &ConnectedProfile,
    language: &QueryLanguage,
    text: &str,
) -> Option<AnalyzeOffer> {
    let supported = connected
        .connection
        .metadata()
        .query
        .as_ref()
        .is_some_and(|query| query.supports_explain_analyze);
    if !supported {
        return None;
    }

    let refusal = match dbflux_core::single_plan_statement(text) {
        Err(_) => Some(AnalyzeRefusal::SeveralStatements),
        Ok(statement) => dbflux_core::analyze_allowed(language, &statement)
            .err()
            .map(|_| AnalyzeRefusal::ChangesData),
    };

    Some(AnalyzeOffer {
        connection: connected.profile.name.clone(),
        production: connected.profile.environment() == Some(ConnectionEnvironment::Production),
        refusal,
    })
}

pub(super) fn gate_title(offer: &AnalyzeOffer) -> String {
    dbflux_i18n::t!(
        "document.query_plan.analyze.gate.title",
        connection = offer.connection
    )
}

/// One sentence that the statement really runs and what it does outside the
/// transaction still happens, and on a production connection one more that
/// names it.
pub(super) fn gate_body(offer: &AnalyzeOffer) -> String {
    let body = dbflux_i18n::t!("document.query_plan.analyze.gate.body");

    if offer.production {
        format!(
            "{body} {}",
            dbflux_i18n::t!(
                "document.query_plan.analyze.gate.production",
                connection = offer.connection
            )
        )
    } else {
        body
    }
}

impl PlanView {
    /// The card the Analyze segment opens: what is about to run, where, and
    /// the two buttons. Nothing has been sent yet.
    pub(super) fn render_analyze_gate(&self, cx: &mut Context<Self>) -> AnyElement {
        let Some(offer) = self.offer.as_ref() else {
            return div().into_any_element();
        };
        let theme = cx.theme();
        let (title, body) = (gate_title(offer), gate_body(offer));

        tagged("plan-analyze-gate")
            .flex()
            .flex_none()
            .items_center()
            .gap(Spacing::LG)
            .m(Spacing::LG)
            .px(Spacing::LG)
            .py(Spacing::MD)
            .bg(theme.warning.opacity(0.08))
            .border_l_4()
            .border_color(theme.warning)
            .child(
                Icon::new(AppIcon::TriangleAlert)
                    .size(ResultMetrics::FOOTER_ICON)
                    .color(theme.warning),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_w_0()
                    .gap(Spacing::XS)
                    .child(
                        div()
                            .text_size(FontSizes::BASE)
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(dbflux_components::tokens::ChromeColors::strong(theme))
                            .child(title),
                    )
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground)
                            .child(body),
                    ),
            )
            .child(
                Button::new(
                    "plan-analyze-cancel",
                    dbflux_i18n::t!("document.query_plan.analyze.gate.cancel"),
                )
                .when_some(
                    dbflux_ui_base::keymap::shortcut_label(ContextId::Results, Command::Cancel),
                    Button::kbd,
                )
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| this.cancel_gate(cx))),
            )
            .child(
                Button::new(
                    "plan-analyze-run",
                    dbflux_i18n::t!("document.query_plan.analyze.gate.run"),
                )
                .primary()
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| this.request_analysis(cx))),
            )
            .into_any_element()
    }

    /// The band under the switch row that says Analyze is off for this
    /// statement and why, with the estimate still showing under it.
    pub(super) fn render_analyze_refusal(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        let refusal = self.offer.as_ref()?.refusal?;
        self.model()?;
        let theme = cx.theme();

        Some(
            tagged("plan-analyze-refusal")
                .flex()
                .flex_none()
                .items_center()
                .gap(Spacing::SM)
                .px(ResultMetrics::FOOTER_PADDING_X)
                .py(Spacing::SM)
                .border_b_1()
                .text_size(FontSizes::BASE)
                .border_color(theme.warning.opacity(0.25))
                .bg(theme.warning.opacity(0.08))
                .child(
                    Icon::new(AppIcon::TriangleAlert)
                        .size(ResultMetrics::FOOTER_ICON)
                        .color(theme.warning),
                )
                .child(
                    div()
                        .font_weight(FontWeight::BOLD)
                        .text_color(theme.warning)
                        .child(refusal.title()),
                )
                .child(
                    div()
                        .flex_1()
                        .min_w_0()
                        .truncate()
                        .text_color(theme.muted_foreground)
                        .child(refusal.reason()),
                )
                .into_any_element(),
        )
    }

    /// Run analyze: the document sends the analyzed request.
    pub(crate) fn request_analysis(&mut self, cx: &mut Context<Self>) {
        if matches!(self.state, super::PlanState::AnalyzeGate) {
            cx.emit(PlanViewEvent::AnalyzeRequested);
        }
    }

    /// Cancel on the gate: back to the estimate, nothing ran.
    pub(crate) fn cancel_gate(&mut self, cx: &mut Context<Self>) {
        if matches!(self.state, super::PlanState::AnalyzeGate) {
            self.choose_run_mode(super::PlanRunMode::Estimate, cx);
        }
    }
}
