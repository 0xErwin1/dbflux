//! Legend element factory for line charts.
//!
//! `legend_element` renders a row of clickable chips below the canvas, one per
//! series. Clicking a chip toggles its visibility via `on_toggle_hidden`.

use std::collections::HashSet;

use gpui::prelude::*;
use gpui::{AnyElement, Hsla, IntoElement, SharedString, div};

use crate::chart::spec::SeriesSpec;
use crate::chart::stats::SeriesStats;
use crate::semantic::ChartColors;

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
    let total = series.len();
    let visible = total - hidden.len();
    let counter: SharedString = format!("{} of {} visible · click to hide", visible, total).into();

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
                .gap(gpui::px(4.0))
                .px(gpui::px(4.0))
                .py(gpui::px(1.0))
                .text_size(gpui::px(11.0))
                .when(is_focused, |d| d.font_weight(gpui::FontWeight::SEMIBOLD))
                .when(is_hidden, |d| d.opacity(0.4))
                .child(
                    div()
                        .w(gpui::px(6.0))
                        .h(gpui::px(6.0))
                        .rounded_full()
                        .bg(color),
                )
                .child(div().child(label));

            if let Some(stat) = stat_str {
                chip = chip.child(div().text_color(colors.label_fg).child(stat));
            }

            if let Some(ref handler) = on_toggle_hidden {
                let handler = handler.clone();
                chip = chip.cursor_pointer().on_mouse_down(
                    gpui::MouseButton::Left,
                    move |_ev, window, cx| {
                        handler(i, window, cx);
                    },
                );
            }

            chip.into_any_element()
        })
        .collect();

    div()
        .flex()
        .flex_row()
        .flex_wrap()
        .items_center()
        .gap_x(gpui::px(12.0))
        .gap_y(gpui::px(2.0))
        .px(gpui::px(12.0))
        .py(gpui::px(4.0))
        .border_t_1()
        .border_color(colors.pill_border)
        .children(chips)
        .child(
            div()
                .flex_1()
                .flex()
                .justify_end()
                .text_color(colors.muted_fg)
                .text_size(gpui::px(10.0))
                .child(counter),
        )
}
