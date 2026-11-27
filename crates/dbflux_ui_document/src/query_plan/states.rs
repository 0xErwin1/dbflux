//! The Plan tab when there is no plan to draw: the driver has none to give,
//! the text held more than one statement, the driver refused, the run was
//! cancelled, or the plan is still on its way.

use dbflux_components::composites::EmptyState;
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{BannerBlock, BannerVariant, Icon};
use dbflux_components::tokens::{Heights, Radii, Spacing, ui};
use gpui::prelude::*;
use gpui::{AnyElement, Context, Rems, div};
use gpui_component::ActiveTheme;

use super::ids::tagged;
use super::{PlanState, PlanView, PlanViewEvent};

/// The placeholder rows of a plan on its way.
const SKELETON_ROWS: [(usize, f32); 5] = [(0, 110.0), (1, 60.0), (2, 140.0), (3, 96.0), (4, 120.0)];

impl PlanView {
    pub(super) fn render_state(&self, cx: &mut Context<Self>) -> AnyElement {
        match &self.state {
            PlanState::Loading => self.render_skeleton(cx),
            PlanState::Unsupported { connection } => tagged("plan-state-unsupported")
                .size_full()
                .child(
                    EmptyState::new(
                        AppIcon::Plug,
                        dbflux_i18n::t!(
                            "document.query_plan.state.unsupported.body",
                            connection = connection
                        ),
                    )
                    .title(dbflux_i18n::t!(
                        "document.query_plan.state.unsupported.title"
                    )),
                )
                .into_any_element(),
            PlanState::MultiStatement { message } => padded(
                "plan-state-multi-statement",
                BannerBlock::new(
                    BannerVariant::Warning,
                    dbflux_i18n::t!("document.query_plan.state.multi_statement.title"),
                )
                .with_icon(Icon::new(AppIcon::TriangleAlert).size(Heights::ICON_SM))
                .with_body(message.clone()),
            ),
            PlanState::Failed(failure) => padded(
                "plan-state-failed",
                BannerBlock::new(
                    BannerVariant::Danger,
                    dbflux_i18n::t!("document.query_plan.state.failed.title"),
                )
                .with_icon(Icon::new(AppIcon::CircleX).size(Heights::ICON_SM))
                .with_body(failure.summary.clone())
                .with_actions(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .child(self.retry_button(cx))
                        .child(
                            Button::new(
                                "plan-copy-error",
                                dbflux_i18n::t!("document.code.execution.copy_error"),
                            )
                            .ghost()
                            .icon(AppIcon::Copy)
                            .tab_stop(false)
                            .on_click(cx.listener(|this, _, _, cx| this.copy_error(cx))),
                        )
                        .child(
                            Button::new(
                                "plan-view-in-audit",
                                dbflux_i18n::t!("errors.action.view_in_audit"),
                            )
                            .ghost()
                            .icon(AppIcon::FingerprintPattern)
                            .tab_stop(false)
                            .on_click(cx.listener(|this, _, _, cx| this.view_error_in_audit(cx))),
                        ),
                ),
            ),
            PlanState::Cancelled => padded(
                "plan-state-cancelled",
                BannerBlock::new(
                    BannerVariant::Info,
                    dbflux_i18n::t!("document.query_plan.state.cancelled.title"),
                )
                .with_icon(Icon::new(AppIcon::Info).size(Heights::ICON_SM))
                .with_actions(self.retry_button(cx)),
            ),
            PlanState::AnalyzeGate => self.render_analyze_gate(cx),
            PlanState::Ready(_) => div().into_any_element(),
        }
    }

    fn retry_button(&self, cx: &mut Context<Self>) -> Button {
        Button::new(
            "plan-retry",
            dbflux_i18n::t!("document.query_plan.state.retry"),
        )
        .ghost()
        .icon(AppIcon::RefreshCcw)
        .tab_stop(false)
        .on_click(cx.listener(|_, _, _, cx| cx.emit(PlanViewEvent::RetryRequested)))
    }

    /// The shape of the Tree view in grey, while the plan is on its way.
    fn render_skeleton(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let bar = |width: f32| {
            div()
                .h(ui(10.0))
                .w(ui(width))
                .rounded(Radii::SM)
                .bg(theme.secondary)
        };

        let rows: Vec<AnyElement> = SKELETON_ROWS
            .iter()
            .map(|(depth, width)| {
                let indent: Rems = ui(16.0 * *depth as f32);
                div()
                    .flex()
                    .items_center()
                    .h(ui(28.0))
                    .px(Spacing::MD)
                    .gap(Spacing::MD)
                    .child(div().flex_1().pl(indent).child(bar(*width)))
                    .child(bar(90.0))
                    .child(bar(56.0))
                    .child(bar(44.0))
                    .into_any_element()
            })
            .collect();

        tagged("plan-state-loading")
            .size_full()
            .flex()
            .flex_col()
            .child(
                div()
                    .flex()
                    .items_center()
                    .h(ui(30.0))
                    .px(Spacing::MD)
                    .border_b_1()
                    .border_color(theme.border)
                    .child(bar(44.0)),
            )
            .children(rows)
            .into_any_element()
    }
}

fn padded(id: &'static str, banner: BannerBlock) -> AnyElement {
    tagged(id)
        .size_full()
        .p(Spacing::MD)
        .child(banner)
        .into_any_element()
}
