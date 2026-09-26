use super::*;
use dbflux_components::primitives::{Status, StatusIndicator};
use dbflux_components::tokens::ShellMetrics;

/// What the sidebar footer says about the saved connections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FooterSummary {
    /// No connection profile exists yet.
    Empty,
    Counts {
        connected: usize,
        idle: usize,
    },
}

impl FooterSummary {
    pub fn new(connected: usize, total_profiles: usize) -> Self {
        if total_profiles == 0 {
            return Self::Empty;
        }

        Self::Counts {
            connected,
            idle: total_profiles.saturating_sub(connected),
        }
    }
}

impl Sidebar {
    /// Footer (AppByzTable): "◆ 2 connected · 37 idle", the connected count
    /// in the success color while any connection is open.
    pub(super) fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let state = self.app_state.read(cx);
        let summary = FooterSummary::new(state.connections().len(), state.profiles().len());

        let content = match summary {
            FooterSummary::Empty => div()
                .flex()
                .items_center()
                .child(
                    StatusIndicator::new(Status::Idle)
                        .label(dbflux_i18n::t!("sidebar.status.no_connections")),
                )
                .into_any_element(),
            FooterSummary::Counts { connected, idle } => {
                let status = if connected > 0 {
                    Status::Connected
                } else {
                    Status::Idle
                };

                div()
                    .flex()
                    .items_center()
                    .gap(ShellMetrics::SIDEBAR_FOOTER_GAP)
                    .child(StatusIndicator::new(status).label(dbflux_i18n::t!(
                        "sidebar.status.connected",
                        count = connected
                    )))
                    .child("\u{b7}")
                    .child(dbflux_i18n::t!("sidebar.status.idle", count = idle))
                    .into_any_element()
            }
        };

        div()
            .id("sidebar-footer")
            .w_full()
            .flex()
            .flex_shrink_0()
            .items_center()
            .h(ShellMetrics::SIDEBAR_FOOTER_HEIGHT)
            .px(ShellMetrics::SIDEBAR_FOOTER_PADDING_X)
            .text_size(ShellMetrics::SIDEBAR_FOOTER_FONT)
            .text_color(theme.muted_foreground)
            .child(content)
    }
}

#[cfg(test)]
mod tests {
    use super::FooterSummary;

    #[test]
    fn footer_counts_idle_profiles_and_names_an_empty_tree() {
        assert_eq!(FooterSummary::new(0, 0), FooterSummary::Empty);
        assert_eq!(
            FooterSummary::new(2, 39),
            FooterSummary::Counts {
                connected: 2,
                idle: 37,
            }
        );
        assert_eq!(
            FooterSummary::new(0, 4),
            FooterSummary::Counts {
                connected: 0,
                idle: 4,
            }
        );
    }
}
