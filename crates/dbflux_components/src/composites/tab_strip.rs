use gpui::prelude::*;
use gpui::{App, Pixels, div};
use gpui_component::ActiveTheme;

use crate::tokens::{ChromeEdgeRole, Heights, Radii, Spacing};

pub(crate) const TAB_STRIP_HEIGHT: Pixels = Heights::TAB;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct TabStripMetrics {
    pub min_height: Pixels,
    pub horizontal_padding: Pixels,
}

pub(crate) fn tab_strip_metrics() -> TabStripMetrics {
    TabStripMetrics {
        min_height: TAB_STRIP_HEIGHT,
        horizontal_padding: Spacing::MD,
    }
}

pub(crate) struct TabStripChromeInspection {
    pub separator_edge: ChromeEdgeRole,
    pub radius: Pixels,
}

pub(crate) fn inspect_tab_strip_chrome() -> TabStripChromeInspection {
    TabStripChromeInspection {
        separator_edge: ChromeEdgeRole::Separator,
        radius: Radii::MD,
    }
}

pub fn tab_strip(children: Vec<gpui::AnyElement>, cx: &App) -> gpui::Div {
    let theme = cx.theme();
    let metrics = tab_strip_metrics();
    let chrome = inspect_tab_strip_chrome();

    div()
        .w_full()
        .flex()
        .items_center()
        .min_h(metrics.min_height)
        .px(metrics.horizontal_padding)
        .gap(Spacing::XS)
        .border_b_1()
        .border_color(chrome.separator_edge.resolve(theme))
        .rounded_t(chrome.radius)
        .children(children)
}

#[cfg(test)]
mod tests {
    use super::{TAB_STRIP_HEIGHT, inspect_tab_strip_chrome, tab_strip_metrics};
    use crate::tokens::{ChromeEdgeRole, Heights, Radii, Spacing};

    #[test]
    fn tab_strip_matches_shared_tab_bar_metrics() {
        let metrics = tab_strip_metrics();

        assert_eq!(metrics.min_height, Heights::TAB);
        assert_eq!(metrics.horizontal_padding, Spacing::MD);
        assert_eq!(TAB_STRIP_HEIGHT, Heights::TAB);
    }

    #[test]
    fn tab_strip_uses_section_separator_instead_of_hard_divider_contract() {
        let chrome = inspect_tab_strip_chrome();

        assert_eq!(chrome.separator_edge, ChromeEdgeRole::Separator);
        assert_eq!(chrome.radius, Radii::MD);
    }
}
