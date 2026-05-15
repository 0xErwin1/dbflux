//! Legend element factory for line charts.
//!
//! `legend_element` returns a flex row of colour-swatch pills. It is a pure
//! element factory — not a GPUI `Entity`. The caller decides whether to include
//! it in the render tree.

use gpui::prelude::*;
use gpui::{AnyElement, Hsla, IntoElement, SharedString, div};

use crate::chart::spec::SeriesSpec;

/// Build the legend element for a chart.
///
/// # Parameters
/// - `series`: the series specifications, one pill per entry.
/// - `palette`: resolved `Hsla` colours indexed by `SeriesSpec::color_slot`.
/// - `focused_series_idx`: the index of the currently focused series; that
///   pill is rendered with a distinct border.
/// - `on_pill_click`: called with the series index, window, and app context
///   when a pill is clicked. Pass `None` to render pills without click handling.
pub fn legend_element(
    series: &[SeriesSpec],
    palette: &[Hsla],
    focused_series_idx: usize,
    on_pill_click: Option<impl Fn(usize, &mut gpui::Window, &mut gpui::App) + Clone + 'static>,
) -> impl IntoElement {
    let pills: Vec<AnyElement> = series
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let color = palette
                .get(s.color_slot as usize % palette.len().max(1))
                .copied()
                .unwrap_or(gpui::hsla(0.6, 0.6, 0.5, 1.0));

            let label: SharedString = s.label.clone().into();
            let is_focused = i == focused_series_idx;

            let mut pill = div()
                .flex()
                .flex_row()
                .items_center()
                .gap(gpui::px(4.0))
                .px(gpui::px(8.0))
                .py(gpui::px(2.0))
                .rounded(gpui::px(4.0))
                .border_1()
                .border_color(if is_focused {
                    color
                } else {
                    gpui::hsla(0.0, 0.0, 0.5, 0.3)
                })
                // Colour swatch.
                .child(
                    div()
                        .w(gpui::px(10.0))
                        .h(gpui::px(10.0))
                        .rounded_full()
                        .bg(color),
                )
                // Series label text.
                .child(div().text_sm().child(label));

            if let Some(ref handler) = on_pill_click {
                let handler = handler.clone();
                pill = pill.cursor_pointer().on_mouse_down(
                    gpui::MouseButton::Left,
                    move |_ev, window, cx| {
                        handler(i, window, cx);
                    },
                );
            }

            pill.into_any_element()
        })
        .collect();

    div()
        .flex()
        .flex_row()
        .flex_wrap()
        .gap(gpui::px(6.0))
        .py(gpui::px(4.0))
        .children(pills)
}
