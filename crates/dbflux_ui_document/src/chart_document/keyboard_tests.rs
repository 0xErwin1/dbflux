//! Keyboard tests of the chart document: the keymap reaches its commands
//! (X6) and every toolbar control is a key or a pane action.

use super::ChartDocument;
use crate::keyboard_test_support::{KeymapHost, host_document, init_keyboard_runtime};
use crate::pane::PaneActionRun;
use dbflux_app::keymap::Command;
use dbflux_components::chart::{AxisPill, ChartKind};
use dbflux_core::{ColumnKind, ColumnMeta, QueryResult, Value};
use dbflux_ui_base::AppStateEntity;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use std::time::Duration;

fn column(name: &str, kind: ColumnKind) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        type_name: String::new(),
        kind,
        nullable: true,
        is_primary_key: false,
    }
}

/// Three samples of two numeric series over a time column.
fn two_series_result() -> QueryResult {
    let rows = [(0, 1.0, 10.0), (1_000, 2.0, 20.0), (2_000, 3.0, 15.0)]
        .into_iter()
        .map(|(ts, a, b)| vec![Value::Int(ts), Value::Float(a), Value::Float(b)])
        .collect();

    QueryResult::table(
        vec![
            column("ts", ColumnKind::Timestamp),
            column("a", ColumnKind::Float),
            column("b", ColumnKind::Float),
        ],
        rows,
        None,
        Duration::ZERO,
    )
}

fn app_state(cx: &mut TestAppContext) -> Entity<AppStateEntity> {
    cx.update(|cx| {
        cx.new(|_| {
            let storage_runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                .expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage_runtime).expect("test storage setup")
        })
    })
}

/// A chart document showing [`two_series_result`], hosted under the app
/// keymap with the keyboard on the chart.
fn chart_with_keyboard(
    cx: &mut TestAppContext,
) -> (
    Entity<KeymapHost<ChartDocument>>,
    Entity<ChartDocument>,
    &mut VisualTestContext,
) {
    init_keyboard_runtime(cx);
    let app_state = app_state(cx);

    let (host, window) = host_document(
        cx,
        move |window, cx| {
            cx.new(|cx| ChartDocument::new(None, String::new(), app_state, window, cx))
        },
        |chart, _| chart.active_context(),
        ChartDocument::dispatch_command,
    );
    let chart = window.update(|_, cx| host.read(cx).document.clone());

    window.update(|window, cx| {
        chart.update(cx, |chart, cx| {
            chart.show_result_for_test(two_series_result(), cx);
            chart.focus(window, cx);
        });
    });
    window.run_until_parked();

    (host, chart, window)
}

fn chart_kind(chart: &Entity<ChartDocument>, window: &mut VisualTestContext) -> ChartKind {
    window.update(|_, cx| chart.read(cx).chart_kind(cx))
}

/// The point the keyboard highlights: `(series, point, data x)`.
fn keyboard_point(
    chart: &Entity<ChartDocument>,
    window: &mut VisualTestContext,
) -> Option<(usize, usize, f64)> {
    window.update(|_, cx| {
        let view = chart
            .read(cx)
            .chart_shell_for_test()
            .read(cx)
            .chart_view()
            .cloned()?;
        let view = view.read(cx);
        let point = view.keyboard_point()?;

        Some((
            point.series_idx,
            point.point_idx_in_series,
            view.hover_data_x()?,
        ))
    })
}

#[gpui::test]
fn alt_l_and_alt_h_switch_the_chart_kind(cx: &mut TestAppContext) {
    let (host, chart, window) = chart_with_keyboard(cx);
    assert_eq!(chart_kind(&chart, window), ChartKind::Line);

    window.simulate_keystrokes("alt-l");
    assert_eq!(chart_kind(&chart, window), ChartKind::Bar);

    window.simulate_keystrokes("alt-h alt-h");
    assert_eq!(
        chart_kind(&chart, window),
        ChartKind::Number,
        "the kind wraps around"
    );
    assert_eq!(
        window.update(|_, cx| host.read(cx).commands.clone()),
        vec![
            Command::NextPanelTab,
            Command::PrevPanelTab,
            Command::PrevPanelTab
        ]
    );
}

/// H and L walk the highlighted point, J moves it to the other series and
/// Escape clears it; the point stands in for the pointer, so the readout
/// shows it (`hover_data_x`).
#[gpui::test]
fn hjkl_move_the_highlighted_point_and_escape_clears_it(cx: &mut TestAppContext) {
    let (_host, chart, window) = chart_with_keyboard(cx);
    assert_eq!(keyboard_point(&chart, window), None);

    window.simulate_keystrokes("l l");
    assert_eq!(keyboard_point(&chart, window), Some((0, 1, 1_000.0)));

    window.simulate_keystrokes("j");
    assert_eq!(
        keyboard_point(&chart, window),
        Some((1, 1, 1_000.0)),
        "J keeps the X and moves to series b"
    );

    window.simulate_keystrokes("shift-g");
    assert_eq!(keyboard_point(&chart, window), Some((1, 2, 2_000.0)));

    window.simulate_keystrokes("escape");
    assert_eq!(keyboard_point(&chart, window), None);
}

/// The X axis picker opens from the pane actions with the keyboard on the
/// bound column; J moves to the next candidate and Enter binds it.
#[gpui::test]
fn the_x_axis_picker_is_driven_by_the_keyboard(cx: &mut TestAppContext) {
    let (_host, chart, window) = chart_with_keyboard(cx);

    let actions = window.update(|_, cx| chart.read(cx).pane_actions(&chart, cx));
    let x_axis = actions
        .iter()
        .find(|action| action.id.as_ref() == "chart-axis-x")
        .expect("the X axis picker is a pane action");
    let PaneActionRun::Callback(open_x) = x_axis.run.clone() else {
        panic!("the X axis entry opens the picker");
    };
    window.update(|window, cx| open_x(window, cx));
    window.run_until_parked();

    let shell = window.update(|_, cx| chart.read(cx).chart_shell_for_test().clone());
    let picker = |window: &mut VisualTestContext| {
        window.update(|_, cx| {
            let shell = shell.read(cx);
            (shell.axis_open_pill, shell.axis_picker_cursor())
        })
    };
    assert_eq!(picker(window), (Some(AxisPill::X), Some(0)));

    window.simulate_keystrokes("j");
    assert_eq!(picker(window), (Some(AxisPill::X), Some(1)));

    window.simulate_keystrokes("enter");
    assert_eq!(picker(window), (None, None), "Enter closes the picker");
    assert_eq!(
        window.update(|_, cx| chart.read(cx).active_bindings(cx).x),
        1,
        "column `a` is the X axis now"
    );
}

/// Every toolbar control of the chart is a pane action.
#[gpui::test]
fn the_pane_actions_list_the_chart_toolbar(cx: &mut TestAppContext) {
    let (_host, chart, window) = chart_with_keyboard(cx);

    let ids: Vec<String> = window.update(|_, cx| {
        chart
            .read(cx)
            .pane_actions(&chart, cx)
            .into_iter()
            .map(|action| action.id.to_string())
            .collect()
    });

    for id in [
        "chart-refresh",
        "chart-auto-refresh",
        "chart-next-time-range",
        "chart-prev-time-range",
        "chart-next-kind",
        "chart-axis-x",
        "chart-axis-y",
        "chart-axis-group",
        "chart-axis-agg",
        "chart-stats",
        "chart-save",
    ] {
        assert!(ids.iter().any(|entry| entry == id), "{id} in {ids:?}");
    }
}

/// Ctrl/Cmd+S opens the save prompt with the keyboard in its name field;
/// Escape closes it.
#[gpui::test]
fn the_save_prompt_opens_and_closes_by_keys(cx: &mut TestAppContext) {
    let (_host, chart, window) = chart_with_keyboard(cx);

    let save = if cfg!(target_os = "macos") {
        "cmd-s"
    } else {
        "ctrl-s"
    };
    window.simulate_keystrokes(save);
    assert!(window.update(|_, cx| chart.read(cx).is_name_prompt_open()));

    window.simulate_keystrokes("escape");
    assert!(!window.update(|_, cx| chart.read(cx).is_name_prompt_open()));
}

/// ] selects the next time-range preset.
#[gpui::test]
fn brackets_step_the_time_range(cx: &mut TestAppContext) {
    use dbflux_components::common::time_range::state::TimeRange;

    let (_host, chart, window) = chart_with_keyboard(cx);
    let range = |window: &mut VisualTestContext| {
        window.update(|_, cx| {
            chart
                .read(cx)
                .time_range_panel
                .as_ref()
                .and_then(|panel| panel.read(cx).selected_time_range)
        })
    };
    assert_eq!(range(window), Some(TimeRange::Last24Hours));

    window.simulate_keystrokes("]");
    assert_eq!(range(window), Some(TimeRange::Last7Days));

    window.simulate_keystrokes("[ [");
    assert_eq!(range(window), Some(TimeRange::Last6Hours));
}

#[test]
fn chart_pane_action_labels_resolve_in_every_locale() {
    let keys = [
        "refresh",
        "auto_refresh",
        "next_time_range",
        "prev_time_range",
        "date_range",
        "start_hour",
        "start_minute",
        "end_hour",
        "end_minute",
        "next_kind",
        "prev_kind",
        "x_axis",
        "y_axis",
        "group_by",
        "aggregation",
        "metric_rail",
        "next_dimension",
        "prev_dimension",
        "period",
        "statistic",
    ];

    for key in keys {
        let key = format!("document.chart.pane_actions.{key}");

        for locale in ["en", "es", "ko", "zh_Hans"] {
            let value = dbflux_i18n::t!(&key, locale = locale);

            assert!(!value.is_empty(), "{key} resolved empty in {locale}");
            assert_ne!(
                value,
                format!("{locale}.{key}"),
                "{key} missing from {locale}"
            );
        }
    }
}

/// The chart tab's toolbar and axis bar, its save prompt, an axis picker
/// and the stats rail. The keyboard tests above prove the chart keys, and
/// `m` opens the pane actions.
#[gpui::test]
fn the_chart_tab_is_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::CHART;
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture, MODAL_CHROME};

    let (_host, chart, window) = chart_with_keyboard(cx);
    let actions = window.update(|_, cx| chart.read(cx).pane_actions(&chart, cx));
    let menu: Vec<String> = actions.iter().map(|action| action.id.to_string()).collect();
    let run = |window: &mut VisualTestContext, id: &str| {
        let action = actions
            .iter()
            .find(|action| action.id == id)
            .unwrap_or_else(|| panic!("no pane action {id}"));
        match &action.run {
            PaneActionRun::Callback(callback) => {
                let callback = callback.clone();
                window.update(|window, cx| callback(window, cx));
            }
            PaneActionRun::Command(command) => {
                let command = *command;
                window.update(|window, cx| {
                    chart.update(cx, |chart, cx| chart.dispatch_command(command, window, cx))
                });
            }
        }
        window.run_until_parked();
    };

    let capture = FrameCapture::observe(window);
    let coverage = || {
        Coverage::new(CHART)
            .with_surface(MODAL_CHROME)
            .with_menu_entries(menu.iter().cloned())
    };

    let checked = coverage().assert_covered(&capture.frame(window));
    assert!(checked.iter().any(|id| id == "axis-pill-x"), "{checked:?}");

    let save = if cfg!(target_os = "macos") {
        "cmd-s"
    } else {
        "ctrl-s"
    };
    window.simulate_keystrokes(save);
    coverage().assert_covered(&capture.frame(window));
    window.simulate_keystrokes("escape");

    run(window, "chart-axis-y");
    coverage().assert_covered(&capture.frame(window));
    window.simulate_keystrokes("escape");

    run(window, "chart-stats");
    coverage().assert_covered(&capture.frame(window));
}
