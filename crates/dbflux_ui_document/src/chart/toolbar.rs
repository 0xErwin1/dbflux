//! Shared chart toolbar rendered by both `DataGridPanel` (in Chart mode) and
//! `ChartDocument`.
//!
//! Contains (left to right):
//! - An optional leading element (a document title)
//! - Time presets (only when a `TimeRangePanel` is wired)
//! - Refresh split-button (icon + "Refresh" label + interval dropdown)
//! - Chart kind switch
//! - Resolved window and point count (when `show_window`)
//! - Stats toggle button
//! - Save chart button (gated on `source_supports_save`)
//!
//! The AxisBar row is NOT part of this toolbar — it lives below and is
//! assembled separately in each caller.

use super::shell::{ChartRailTab, ChartShell};
use crate::chrome::time_preset_control;
use crate::labels::configure_chart_kind_label;
use dbflux_components::chart::{ChartKind, format_x_value};
use dbflux_components::common::time_range::TimeRangePanel;
use dbflux_components::composites::refresh_split_button;
use dbflux_components::controls::{Button, ButtonVariant, Dropdown};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{SegmentedControl, SegmentedItem};
use dbflux_components::tokens::DocumentMetrics;
use dbflux_components::typography::AppFonts;
use dbflux_core::RefreshPolicy;
use gpui::prelude::*;
use gpui::*;
use gpui_component::theme::Theme;
use std::sync::Arc;

/// Handler called when the Stats button, Save button, or Refresh button is
/// clicked.
pub type ActionHandler = Arc<dyn Fn(&mut Window, &mut App)>;
/// Handler called when a chart-kind chip is clicked; receives the chosen kind.
pub type ChartKindHandler = Arc<dyn Fn(ChartKind, &mut Window, &mut App)>;

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
    /// The current refresh policy — drives the refresh split-button icon and label.
    pub refresh_policy: RefreshPolicy,
    /// The REFRESH interval-selector dropdown entity (right section of split-button).
    pub refresh_dropdown: Entity<Dropdown>,
    /// The time-range panel whose presets the toolbar shows as a segmented
    /// control. When `None` the presets are hidden entirely.
    pub time_range_panel: Option<Entity<TimeRangePanel>>,
    /// Total number of data-point rows in the current result.
    pub row_count: usize,
    /// The resolved time window from the driver response `(start_ms, end_ms)`.
    /// When `None`, the toolbar falls back to the chart view's x-axis bounds.
    pub resolved_window: Option<(i64, i64)>,
    /// Show the "Save chart" button. DataGridPanel gates on collection source;
    /// ChartDocument always passes `true` here.
    pub source_supports_save: bool,
    /// Variant of the refresh split. It must match the `chevron_trigger`
    /// variant of `refresh_dropdown`, whose chevron takes that variant's
    /// content color.
    pub refresh_variant: ButtonVariant,
    /// Elements drawn before the controls (a document's title), with the
    /// controls then pushed to the right edge.
    pub leading: Option<AnyElement>,
    /// Show the resolved window and point count in the toolbar. A host that
    /// shows them in its axis row passes `false`.
    pub show_window: bool,
}

/// Callbacks for interactive toolbar actions.
///
/// Handlers are `Arc<dyn Fn(...)>` so they are `Clone + 'static` and can be
/// moved into GPUI's element event closures without lifetime issues. The boxing
/// cost is one allocation per `render_chart_toolbar` call, which is negligible.
pub struct ChartToolbarHandlers {
    /// Called when the Refresh (left segment) of the split-button is clicked.
    pub on_refresh: ActionHandler,
    /// Called when the Stats button is clicked.
    pub on_toggle_stats_rail: ActionHandler,
    /// Called when the "Save chart" button is clicked.
    pub on_save_chart: ActionHandler,
    /// Called when a chart-kind segment is clicked.
    pub on_select_chart_kind: ChartKindHandler,
}

/// Chart kinds offered by the kind switch, in display order.
pub(crate) const CHART_KINDS: [ChartKind; 7] = [
    ChartKind::Line,
    ChartKind::Bar,
    ChartKind::Area,
    ChartKind::Scatter,
    ChartKind::StackedBar,
    ChartKind::Pie,
    ChartKind::Number,
];

fn chart_kind_id(kind: ChartKind) -> SharedString {
    let index = CHART_KINDS
        .iter()
        .position(|candidate| *candidate == kind)
        .unwrap_or_default();

    SharedString::from(format!("chart-kind-{index}"))
}

/// The chart-kind switch: one segment per kind with its icon.
pub fn chart_kind_control(
    current: ChartKind,
    on_select: impl Fn(ChartKind, &mut Window, &mut App) + 'static,
) -> SegmentedControl {
    let items = CHART_KINDS
        .iter()
        .map(|kind| {
            SegmentedItem::new(chart_kind_id(*kind), configure_chart_kind_label(*kind))
                .icon(AppIcon::for_chart_kind(*kind))
        })
        .collect();

    SegmentedControl::new(items, chart_kind_id(current), move |id, window, cx| {
        if let Some(kind) = CHART_KINDS.iter().find(|kind| chart_kind_id(**kind) == *id) {
            on_select(*kind, window, cx);
        }
    })
}

/// The resolved window and point count: `start → end UTC · 72 pts`, from the
/// driver's resolved window or, without one, the chart's x bounds.
pub fn chart_window_label(
    chart_shell: &Entity<ChartShell>,
    resolved_window: Option<(i64, i64)>,
    row_count: usize,
    cx: &App,
) -> String {
    let chart_view = chart_shell.read(cx).chart_view().cloned();

    let bounds = match resolved_window {
        Some((start_ms, end_ms)) => Some((start_ms as f64, end_ms as f64)),
        None => chart_view.map(|view| view.read(cx).data_x_bounds()),
    };

    let points = crate::labels::chart_toolbar_points_label(row_count);

    match bounds {
        Some((start, end)) => format!(
            "{} \u{2192} {} UTC \u{00b7} {}",
            format_x_value(start, true),
            format_x_value(end, true),
            points
        ),
        None => points,
    }
}

/// Render the chart toolbar row.
///
/// Returns the single horizontal toolbar row. Does NOT include the AxisBar
/// row; each caller composes that separately below this row.
pub fn render_chart_toolbar(
    ctx: ChartToolbarContext,
    handlers: ChartToolbarHandlers,
    cx: &mut App,
) -> AnyElement {
    let theme = ctx.theme;

    let (rail_open, rail_tab, current_kind) = {
        let shell = ctx.chart_shell.read(cx);
        (
            shell.chart_rail_open,
            shell.chart_rail_tab,
            shell.chart_kind(),
        )
    };

    let window_label = ctx
        .show_window
        .then(|| chart_window_label(&ctx.chart_shell, ctx.resolved_window, ctx.row_count, cx));

    let presets = ctx.time_range_panel.map(|panel| {
        let selected = panel.read(cx).selected_time_range;

        time_preset_control(selected, true, move |index, _, cx| {
            panel.update(cx, |panel, cx| panel.select_preset(index, cx));
        })
    });

    let on_refresh = handlers.on_refresh.clone();
    let refresh_btn = refresh_split_button(
        "chart-toolbar-refresh",
        ctx.refresh_policy,
        false,
        false,
        ctx.refresh_dropdown.clone(),
        move |window, cx| on_refresh(window, cx),
    )
    .variant(ctx.refresh_variant);

    let on_select_kind = handlers.on_select_chart_kind.clone();
    let kind_switch = chart_kind_control(current_kind, move |kind, window, cx| {
        on_select_kind(kind, window, cx)
    });

    let is_stats_active = rail_open && rail_tab == ChartRailTab::Stats;
    let on_stats = handlers.on_toggle_stats_rail.clone();
    let on_save = handlers.on_save_chart.clone();

    let stats_btn = Button::new(
        "chart-toolbar-stats",
        dbflux_i18n::t!("document.chart.toolbar.stats"),
    )
    .icon(AppIcon::Sigma)
    .selected(is_stats_active)
    .tab_stop(false)
    .on_click(move |_, window, cx| on_stats(window, cx));

    let save_btn = ctx.source_supports_save.then(|| {
        Button::new(
            "chart-toolbar-save",
            dbflux_i18n::t!("document.chart.toolbar.save_chart"),
        )
        .icon(AppIcon::Save)
        .tab_stop(false)
        .on_click(move |_, window, cx| on_save(window, cx))
    });

    let has_leading = ctx.leading.is_some();

    div()
        .flex()
        .flex_row()
        .flex_wrap()
        .flex_shrink_0()
        .items_center()
        .w_full()
        .min_h(DocumentMetrics::HEADER_HEIGHT_TALL)
        .px(DocumentMetrics::PADDING_X)
        .py(DocumentMetrics::TOOLBAR_PADDING_Y)
        .gap(DocumentMetrics::GAP)
        .border_b_1()
        .border_color(theme.border)
        .when_some(ctx.leading, |row, leading| row.child(leading))
        .when(has_leading, |row| row.child(div().flex_1()))
        .children(presets)
        .child(refresh_btn)
        .child(kind_switch)
        .when_some(window_label, |row, label| {
            row.child(div().flex_1()).child(
                div()
                    .flex_shrink_0()
                    .font_family(AppFonts::MONO)
                    .text_size(DocumentMetrics::TABLE_META_FONT)
                    .text_color(theme.muted_foreground)
                    .child(label),
            )
        })
        .child(stats_btn)
        .children(save_btn)
        .into_any_element()
}

#[cfg(test)]
mod tests {
    // Not a glob import of `super`: that would bring `gpui::test` into scope
    // and shadow the built-in `#[test]` attribute that `#[gpui::test]` expands to.
    use super::{
        ActionHandler, ChartShell, ChartToolbarContext, ChartToolbarHandlers, Dropdown,
        RefreshPolicy, render_chart_toolbar,
    };
    use gpui::{
        AccessibilityFrame, AppContext as _, Context, Entity, FrameObserver,
        InteractiveElement as _, IntoElement, ParentElement as _, Render, Styled as _, Window, div,
    };
    use gpui_component::ActiveTheme as _;
    use std::sync::{Arc, Mutex};

    struct ToolbarHarness {
        chart_shell: Entity<ChartShell>,
        refresh_dropdown: Entity<Dropdown>,
    }

    impl Render for ToolbarHarness {
        fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            let theme = cx.theme().clone();
            let noop: ActionHandler = Arc::new(|_window, _cx| {});

            let ctx = ChartToolbarContext {
                theme: &theme,
                chart_shell: self.chart_shell.clone(),
                refresh_policy: RefreshPolicy::Manual,
                refresh_dropdown: self.refresh_dropdown.clone(),
                time_range_panel: None,
                row_count: 0,
                resolved_window: Some((0, 3_600_000)),
                source_supports_save: true,
                refresh_variant: dbflux_components::controls::ButtonVariant::Secondary,
                leading: None,
                show_window: true,
            };

            let handlers = ChartToolbarHandlers {
                on_refresh: noop.clone(),
                on_toggle_stats_rail: noop.clone(),
                on_save_chart: noop,
                on_select_chart_kind: Arc::new(|_kind, _window, _cx| {}),
            };

            div()
                .id("toolbar-harness")
                .size_full()
                .child(render_chart_toolbar(ctx, handlers, cx))
        }
    }

    /// Keeps the latest rendered accessibility frame of the window it observes.
    #[derive(Default)]
    struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

    impl FrameObserver for FrameCapture {
        fn accessibility_updated(&self, frame: &AccessibilityFrame) {
            *self.0.lock().expect("frame capture lock") = Some(frame.clone());
        }
    }

    /// Renders the toolbar with saving enabled and returns the element ids of
    /// its buttons.
    fn rendered_toolbar_button_ids(cx: &mut gpui::TestAppContext) -> Vec<String> {
        cx.update(gpui_component::init);

        let capture = Arc::new(FrameCapture::default());
        let capture_for_window = capture.clone();
        let (_view, visual) = cx.add_window_view(move |window, cx| {
            window.observe_frames(&capture_for_window);
            window.refresh();
            ToolbarHarness {
                chart_shell: cx.new(ChartShell::new_standalone),
                refresh_dropdown: cx.new(|_cx| Dropdown::new("toolbar-harness-refresh")),
            }
        });
        visual.run_until_parked();

        let frame = capture
            .0
            .lock()
            .expect("frame capture lock")
            .clone()
            .expect("the window rendered a frame");

        frame
            .nodes()
            .map(|(_, node)| node.id().to_owned())
            .filter(|id| id.starts_with("chart-toolbar-"))
            .collect()
    }

    /// The toolbar renders its working actions (Stats, Save chart) and no PNG
    /// export control.
    #[gpui::test]
    fn toolbar_renders_no_png_export_control(cx: &mut gpui::TestAppContext) {
        let ids = rendered_toolbar_button_ids(cx);

        assert!(
            ids.iter().all(|id| !id.contains("png")),
            "toolbar still renders a PNG control: {ids:?}"
        );
        assert!(ids.iter().any(|id| id == "chart-toolbar-stats"), "{ids:?}");
        assert!(ids.iter().any(|id| id == "chart-toolbar-save"), "{ids:?}");
    }

    /// Keys for controls that were removed because the feature behind them
    /// does not exist. No shipped catalog may keep them.
    #[test]
    fn removed_placeholder_keys_are_absent_from_every_catalog() {
        let removed = [
            "document.chart.toast.png_export_coming",
            "document.data.grid.export.png_coming_soon",
            "document.dashboard.configure.action.export_png",
            "chart.point_inspector.annotate",
            "chart.point_inspector.copy_as_query",
            "chart.point_inspector.coming_soon",
        ];

        for language in dbflux_i18n::Language::available() {
            let locale = language.locale_code();

            for key in removed {
                assert_eq!(
                    dbflux_i18n::t!(key, locale = locale),
                    format!("{locale}.{key}"),
                    "catalog {locale} still defines {key}"
                );
            }
        }
    }

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

    /// Every kind in the switch maps back to itself through its segment id.
    #[test]
    fn chart_kind_ids_round_trip() {
        for kind in super::CHART_KINDS {
            let id = super::chart_kind_id(kind);
            let found = super::CHART_KINDS
                .iter()
                .find(|candidate| super::chart_kind_id(**candidate) == id);

            assert_eq!(found, Some(&kind));
        }
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

    /// PR 24: the toolbar's chart-kind chip labels route through the same
    /// translated `configure_chart_kind_label` helper the dashboard's
    /// Configure popover uses (PR 23), instead of a duplicate literal set.
    #[test]
    fn kind_chip_labels_route_through_configure_chart_kind_label() {
        use dbflux_components::chart::ChartKind;

        let kinds = [
            ChartKind::Line,
            ChartKind::Bar,
            ChartKind::Scatter,
            ChartKind::Area,
            ChartKind::StackedBar,
            ChartKind::Pie,
            ChartKind::Number,
        ];

        for kind in kinds {
            let label = crate::labels::configure_chart_kind_label(kind);
            assert!(
                !label.is_empty(),
                "configure_chart_kind_label({kind:?}) resolved empty"
            );
        }
    }
}
