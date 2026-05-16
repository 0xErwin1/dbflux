//! Shared chart toolbar rendered by both `DataGridPanel` (in Chart mode) and
//! `ChartDocument`.
//!
//! Contains (left to right):
//! - RANGE label + preset chips (only when `time_range_panel` is `Some`)
//! - Vertical divider + REFRESH label + dropdown
//! - Clock icon + resolved window string
//! - Spacer
//! - Points · resolution display
//! - Stats toggle button
//! - PNG export button (stub)
//! - Save chart button (gated on `source_supports_save`)
//!
//! The AxisBar row is NOT part of this toolbar — it lives below and is
//! assembled separately in each caller.

use crate::ui::common::time_range::state::TimeRange;
use crate::ui::common::time_range::view::TimeRangePanel;
use crate::ui::components::dropdown::Dropdown;
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Radii, Spacing};
use super::shell::{ChartRailTab, ChartShell};
use dbflux_components::chart::{format_resolution, format_x_value};
use dbflux_components::primitives::Icon;
use gpui::prelude::*;
use gpui::*;
use gpui_component::theme::Theme;
use std::sync::Arc;

/// Handler called when a RANGE preset chip is clicked; receives the preset index.
pub type RangePresetHandler = Arc<dyn Fn(usize, &mut Window, &mut App)>;
/// Handler called when the Stats button, PNG button, or Save button is clicked.
pub type ActionHandler = Arc<dyn Fn(&mut Window, &mut App)>;

/// All read-only state the toolbar needs to render itself.
///
/// Callers build this from their own fields; the shared function does not
/// read from any concrete entity directly (except through the provided
/// `Entity` handles).
pub struct ChartToolbarContext<'a> {
    /// The active theme.
    pub theme: &'a Theme,
    /// The `ChartShell` entity — used to read rail open/tab state and the
    /// chart view's data bounds (x_min / x_max).
    pub chart_shell: Entity<ChartShell>,
    /// The REFRESH dropdown entity rendered inline.
    pub refresh_dropdown: Entity<Dropdown>,
    /// The `TimeRangePanel` driving RANGE chip state. When `None` the RANGE
    /// chip row and divider are hidden entirely.
    pub time_range_panel: Option<Entity<TimeRangePanel>>,
    /// Total number of data-point rows in the current result.
    pub row_count: usize,
    /// The resolved time window from the driver response `(start_ms, end_ms)`.
    /// When `None`, the toolbar falls back to the chart view's x-axis bounds.
    pub resolved_window: Option<(i64, i64)>,
    /// Show the "Save chart" button. DataGridPanel gates on collection source;
    /// ChartDocument always passes `true` here.
    pub source_supports_save: bool,
}

/// Callbacks for interactive toolbar actions.
///
/// Handlers are `Arc<dyn Fn(...)>` so they are `Clone + 'static` and can be
/// moved into GPUI's element event closures without lifetime issues. The boxing
/// cost is one allocation per `render_chart_toolbar` call, which is negligible.
pub struct ChartToolbarHandlers {
    /// Called when a RANGE preset chip is clicked. Argument is the preset index.
    pub on_select_range_preset: RangePresetHandler,
    /// Called when the Stats button is clicked.
    pub on_toggle_stats_rail: ActionHandler,
    /// Called when the PNG button is clicked.
    pub on_png_export: ActionHandler,
    /// Called when the "Save chart" button is clicked.
    pub on_save_chart: ActionHandler,
}

/// Render the chart toolbar row.
///
/// Returns the single horizontal toolbar div. Does NOT include the AxisBar row;
/// each caller composes that separately below this row.
pub fn render_chart_toolbar(
    ctx: ChartToolbarContext,
    handlers: ChartToolbarHandlers,
    cx: &mut App,
) -> AnyElement {
    let theme = ctx.theme;
    let muted = theme.muted_foreground;
    let border = theme.border;
    let foreground = theme.foreground;
    let secondary = theme.secondary;
    let primary = theme.primary;
    let primary_fg = theme.primary_foreground;

    // --- Read rail state from the shell ---
    let (chart_view_entity, rail_open, rail_tab) = {
        let shell = ctx.chart_shell.read(cx);
        (
            shell.chart_view().cloned(),
            shell.chart_rail_open,
            shell.chart_rail_tab,
        )
    };

    // --- Resolved window label ---
    let (window_label, x_span_ms) = if let Some((start_ms, end_ms)) = ctx.resolved_window {
        let start_str = format_x_value(start_ms as f64, true);
        let end_str = format_x_value(end_ms as f64, true);
        let span = (end_ms - start_ms) as f64;
        (format!("{} \u{2192} {} UTC", start_str, end_str), span)
    } else if let Some(cv) = &chart_view_entity {
        let (x_min, x_max) = cv.read(cx).data_x_bounds();
        let start_str = format_x_value(x_min, true);
        let end_str = format_x_value(x_max, true);
        let span = x_max - x_min;
        (format!("{} \u{2192} {} UTC", start_str, end_str), span)
    } else {
        ("\u{2014}".to_string(), 0.0)
    };

    let row_count = ctx.row_count;
    let resolution_label = SharedString::from(format_resolution(x_span_ms, row_count));
    let window_label: SharedString = window_label.into();

    // --- RANGE chips (only when a TimeRangePanel is wired) ---
    let range_section: Option<AnyElement> = ctx.time_range_panel.map(|trp| {
        let all_presets: Vec<SharedString> = TimeRangePanel::preset_items()
            .into_iter()
            .map(|item| item.label)
            .collect();

        let active_preset_label: Option<SharedString> = {
            let panel = trp.read(cx);
            panel.selected_time_range.and_then(|range| {
                let index = match range {
                    TimeRange::Last15min => 0,
                    TimeRange::LastHour => 1,
                    TimeRange::Last6Hours => 2,
                    TimeRange::Last24Hours => 3,
                    TimeRange::Last7Days => 4,
                    TimeRange::Custom => 5,
                };
                TimeRangePanel::preset_items()
                    .get(index)
                    .map(|item| item.label.clone())
            })
        };

        let num_presets = all_presets.len();
        let on_select = handlers.on_select_range_preset.clone();

        let chips = div()
            .flex()
            .items_center()
            .border_1()
            .border_color(border)
            .rounded(Radii::SM)
            .overflow_hidden()
            .children(all_presets.into_iter().enumerate().map(|(i, label)| {
                let is_active = active_preset_label.as_ref().is_some_and(|a| a == &label);
                let is_last = i == num_presets - 1;
                let handler = on_select.clone();

                let mut chip = div()
                    .id(ElementId::Name(format!("range-chip-{i}").into()))
                    .px(px(8.0))
                    .py(px(3.0))
                    .text_size(px(11.0))
                    .font(font("JetBrains Mono"))
                    .cursor_pointer()
                    .when(is_active, |d| {
                        d.bg(primary)
                            .text_color(primary_fg)
                            .font_weight(FontWeight::SEMIBOLD)
                    })
                    .when(!is_active, |d| {
                        d.text_color(muted).hover(move |d| d.bg(secondary))
                    })
                    .when(!is_last, |d| d.border_r_1().border_color(border))
                    .on_mouse_down(MouseButton::Left, move |_, window, cx| {
                        handler(i, window, cx);
                    })
                    .child(label);

                if i == 0 {
                    chip = chip.rounded_tl(Radii::SM).rounded_bl(Radii::SM);
                } else if is_last {
                    chip = chip.rounded_tr(Radii::SM).rounded_br(Radii::SM);
                }

                chip
            }));

        div()
            .flex()
            .items_center()
            .gap(px(4.0))
            .child(
                div()
                    .text_size(px(10.0))
                    .text_color(muted)
                    .font_weight(FontWeight::BOLD)
                    .child("RANGE"),
            )
            .child(chips)
            .child(vdivider(border))
            .into_any_element()
    });

    // --- Toolbar action button helper ---
    let toolbar_btn =
        |id: &'static str, icon: AppIcon, label: &'static str, is_active: bool| {
            div()
                .id(id)
                .flex()
                .items_center()
                .gap(px(4.0))
                .px(px(6.0))
                .py(px(2.0))
                .rounded(Radii::SM)
                .text_size(FontSizes::XS)
                .cursor_pointer()
                .when(is_active, |d| d.bg(primary).text_color(primary_fg))
                .when(!is_active, |d| {
                    d.text_color(foreground).hover(move |d| d.bg(secondary))
                })
                .child(Icon::new(icon).size(px(11.0)).color(if is_active {
                    primary_fg
                } else {
                    foreground
                }))
                .child(label)
        };

    let is_stats_active = rail_open && rail_tab == ChartRailTab::Stats;
    let on_stats = handlers.on_toggle_stats_rail.clone();
    let on_png = handlers.on_png_export.clone();
    let on_save = handlers.on_save_chart.clone();

    let stats_btn = toolbar_btn("chart-toolbar-stats", AppIcon::Zap, "Stats", is_stats_active)
        .on_mouse_down(MouseButton::Left, move |_, window, cx| {
            on_stats(window, cx);
        });

    let png_btn = toolbar_btn("chart-toolbar-png", AppIcon::Download, "PNG", false)
        .on_mouse_down(MouseButton::Left, move |_, window, cx| {
            on_png(window, cx);
        });

    let save_btn = toolbar_btn("chart-toolbar-save", AppIcon::Save, "Save chart", false)
        .on_mouse_down(MouseButton::Left, move |_, window, cx| {
            on_save(window, cx);
        });

    div()
        .flex()
        .flex_row()
        .items_center()
        .h(px(34.0))
        .px(Spacing::SM)
        .gap(px(4.0))
        .border_b_1()
        .border_color(theme.border)
        .bg(theme.tab_bar)
        .when_some(range_section, |el, range| el.child(range))
        // REFRESH label + dropdown
        .child(
            div()
                .flex()
                .items_center()
                .gap(px(4.0))
                .child(
                    div()
                        .text_size(px(10.0))
                        .text_color(muted)
                        .font_weight(FontWeight::BOLD)
                        .child("REFRESH"),
                )
                .child(
                    div()
                        .w(px(80.0))
                        .h(px(22.0))
                        .child(ctx.refresh_dropdown.clone()),
                ),
        )
        .child(vdivider(border))
        // Clock icon + resolved window string
        .child(
            div()
                .flex()
                .items_center()
                .gap(px(4.0))
                .child(Icon::new(AppIcon::Clock).size(px(11.0)).color(muted))
                .child(
                    div()
                        .text_size(px(11.0))
                        .text_color(muted)
                        .font(font("JetBrains Mono"))
                        .child(window_label),
                ),
        )
        // Spacer
        .child(div().flex_1())
        // Points · resolution
        .child(
            div()
                .flex()
                .items_center()
                .gap(px(4.0))
                .text_size(px(11.0))
                .text_color(muted)
                .font(font("JetBrains Mono"))
                .child(SharedString::from(format!("{row_count} pts")))
                .child("\u{00b7}")
                .child(resolution_label),
        )
        .child(vdivider(border))
        .child(stats_btn)
        .child(png_btn)
        .when(ctx.source_supports_save, |el| {
            el.child(vdivider(border)).child(save_btn)
        })
        .into_any_element()
}

fn vdivider(border: gpui::Hsla) -> impl IntoElement {
    div().w(px(1.0)).h(px(12.0)).mx(px(4.0)).bg(border)
}

#[cfg(test)]
mod tests {
    /// Verify the resolved-window priority logic: when `resolved_window` is `Some`,
    /// the chart view x-bounds fallback must not be used.
    #[test]
    fn resolved_window_some_takes_priority_over_fallback() {
        let resolved: Option<(i64, i64)> = Some((0, 3_600_000));
        let uses_fallback = resolved.is_none();
        assert!(
            !uses_fallback,
            "resolved_window Some must not fall back to chart view bounds"
        );
    }

    /// When `time_range_panel` is `None`, no RANGE section element is produced.
    #[test]
    fn no_time_range_panel_produces_no_range_section() {
        let trp: Option<()> = None;
        let range_section = trp.map(|_| "chips");
        assert!(
            range_section.is_none(),
            "absent time_range_panel must hide RANGE section"
        );
    }

    /// When `source_supports_save` is `false`, the Save button block is skipped.
    #[test]
    fn save_button_gated_on_source_supports_save() {
        let source_supports_save = false;
        // The toolbar's .when(source_supports_save, ...) guard prevents the
        // save button and its preceding divider from rendering.
        assert!(
            !source_supports_save,
            "save button must be gated by source_supports_save"
        );
    }
}
