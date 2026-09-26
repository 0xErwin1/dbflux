//! Legend element factory for line charts.
//!
//! `legend_element` renders a row of series swatches below the canvas, one
//! chip per series. Clicking a chip toggles its visibility via
//! `on_toggle_hidden`; the chip's tooltip says so.

use std::collections::HashSet;

use gpui::prelude::*;
use gpui::{AnyElement, Hsla, IntoElement, SharedString, div};
use gpui_component::tooltip::Tooltip;

use crate::chart::spec::SeriesSpec;
use crate::chart::stats::SeriesStats;
use crate::semantic::ChartColors;
use crate::tokens::{ChartGeometry, Spacing};

/// Build the legend element for a chart.
///
/// # Parameters
/// - `series`: series specifications, one chip per entry.
/// - `palette`: resolved `Hsla` colours indexed by `SeriesSpec::color_slot`.
/// - `stats`: per-series statistics (parallel to `series`); may be `None` for empty series.
/// - `hidden`: set of hidden series indices; chips for hidden indices are rendered at 40% opacity.
/// - `focused_series_idx`: currently focused series (chip highlighted with a border).
/// - `colors`: semantic chart colors for the active theme.
/// - `on_toggle_hidden`: called with the series index when the chip is clicked.
pub fn legend_element<F>(
    series: &[SeriesSpec],
    palette: &[Hsla],
    stats: &[Option<SeriesStats>],
    hidden: &HashSet<usize>,
    focused_series_idx: usize,
    colors: &ChartColors,
    on_toggle_hidden: Option<F>,
) -> impl IntoElement
where
    F: Fn(usize, &mut gpui::Window, &mut gpui::App) + Clone + Send + Sync + 'static,
{
    let chips: Vec<AnyElement> = series
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let color = palette
                .get(s.color_slot as usize % palette.len().max(1))
                .copied()
                .unwrap_or(gpui::hsla(0.6, 0.6, 0.5, 1.0)); // guardrail-allow: OOB palette slot neutral fallback, no semantic token for this case

            let label: SharedString = s.label.clone().into();
            let is_focused = i == focused_series_idx;
            let is_hidden = hidden.contains(&i);

            // Optional avg and last values from stats.
            let stat_str: Option<SharedString> = stats.get(i).and_then(|opt| {
                opt.map(|st| format!("avg {:.2} · last {:.2}", st.avg, st.last).into())
            });

            let mut chip = div()
                .flex()
                .flex_row()
                .items_center()
                .gap(Spacing::XS)
                .px(Spacing::XS)
                .py(ChartGeometry::HAIRLINE)
                .text_size(ChartGeometry::FONT_LABEL)
                .when(is_focused, |d| d.font_weight(gpui::FontWeight::SEMIBOLD))
                .when(is_hidden, |d| d.opacity(0.4))
                .child(
                    div()
                        .w(Spacing::XXS)
                        .h(Spacing::XXS)
                        .rounded_full()
                        .bg(color),
                )
                .child(div().child(label));

            if let Some(stat) = stat_str {
                chip = chip.child(div().text_color(colors.label_fg).child(stat));
            }

            let Some(ref handler) = on_toggle_hidden else {
                return chip.into_any_element();
            };

            let handler = handler.clone();
            let hint = toggle_hint(is_hidden);

            chip.id(("chart-legend-series", i))
                .cursor_pointer()
                .tooltip(move |window, cx| Tooltip::new(hint.clone()).build(window, cx))
                .on_mouse_down(gpui::MouseButton::Left, move |_ev, window, cx| {
                    handler(i, window, cx);
                })
                .into_any_element()
        })
        .collect();

    div()
        .flex()
        .flex_row()
        .flex_wrap()
        .items_center()
        .gap_x(Spacing::MD)
        .gap_y(ChartGeometry::ACCENT_STRIPE)
        .px(Spacing::MD)
        .py(Spacing::XS)
        .border_t_1()
        .border_color(colors.pill_border)
        .children(chips)
}

/// Tooltip of a clickable legend chip: what a click does to its series.
fn toggle_hint(is_hidden: bool) -> SharedString {
    if is_hidden {
        dbflux_i18n::t!("chart.legend.show_series").into()
    } else {
        dbflux_i18n::t!("chart.legend.hide_series").into()
    }
}

#[cfg(test)]
mod tests {
    use super::toggle_hint;

    #[test]
    fn toggle_hint_names_the_action_a_click_performs() {
        assert_eq!(
            toggle_hint(false).as_ref(),
            dbflux_i18n::t!("chart.legend.hide_series", locale = "en")
        );
        assert_eq!(
            toggle_hint(true).as_ref(),
            dbflux_i18n::t!("chart.legend.show_series", locale = "en")
        );
        assert_ne!(
            dbflux_i18n::t!("chart.legend.hide_series", locale = "en"),
            dbflux_i18n::t!("chart.legend.hide_series", locale = "es")
        );
    }
}
