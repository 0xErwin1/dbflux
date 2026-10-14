use super::isolated_test_app_state;
use crate::data_grid_panel::DataGridPanel;
use dbflux_components::theme;
use dbflux_core::QueryResult;
use gpui::{AppContext, TestAppContext, VisualTestContext};
use std::sync::Arc;
use std::time::Duration;

/// A query-result grid showing `result`.
fn result_grid(
    cx: &mut TestAppContext,
    result: QueryResult,
) -> (gpui::Entity<DataGridPanel>, &mut VisualTestContext) {
    cx.update(theme::init);
    let app_state = isolated_test_app_state(cx);

    let (panel, window) = cx.add_window_view(move |window, cx| {
        DataGridPanel::new_for_result(
            Arc::new(result),
            "UPDATE device SET is_linked = false".to_string(),
            None,
            app_state,
            window,
            cx,
        )
    });
    window.run_until_parked();

    (panel, window)
}

fn no_column_result(affected_rows: Option<u64>) -> QueryResult {
    QueryResult::table(Vec::new(), Vec::new(), affected_rows, Duration::ZERO)
}

#[gpui::test]
fn a_statement_that_affected_rows_says_so(cx: &mut TestAppContext) {
    let (panel, window) = result_grid(cx, no_column_result(Some(3)));

    window.update(|_, cx| {
        let panel = panel.read(cx);
        assert_eq!(
            panel.statement_outcome().as_deref(),
            Some("Statement executed · 3 rows affected")
        );
        assert_eq!(panel.rows_footer(0), "3 rows affected");
    });
}

#[gpui::test]
fn a_statement_that_affected_one_row_says_so(cx: &mut TestAppContext) {
    let (panel, window) = result_grid(cx, no_column_result(Some(1)));

    window.update(|_, cx| {
        let panel = panel.read(cx);
        assert_eq!(
            panel.statement_outcome().as_deref(),
            Some("Statement executed · 1 row affected")
        );
        assert_eq!(panel.rows_footer(0), "1 row affected");
    });
}

#[gpui::test]
fn a_statement_without_a_row_count_says_it_ran(cx: &mut TestAppContext) {
    let (panel, window) = result_grid(cx, no_column_result(None));

    window.update(|_, cx| {
        let panel = panel.read(cx);
        assert_eq!(
            panel.statement_outcome().as_deref(),
            Some("Statement executed")
        );
        assert_eq!(panel.rows_footer(0), "0 rows");
    });
}

#[gpui::test]
fn a_result_with_columns_keeps_the_row_count(cx: &mut TestAppContext) {
    let (panel, window) = result_grid(
        cx,
        QueryResult::table(super::zero_row_columns(), Vec::new(), None, Duration::ZERO),
    );

    window.update(|_, cx| {
        let panel = panel.read(cx);
        assert_eq!(panel.statement_outcome(), None);
        assert_eq!(panel.rows_footer(0), "0 rows");
    });
}

fn with_notices(mut result: QueryResult, notices: &[(&str, &str)]) -> QueryResult {
    result.set_server_notices(notices.iter().map(|(severity, message)| {
        dbflux_core::ServerNotice {
            severity: severity.to_string(),
            message: message.to_string(),
        }
    }));
    result
}

#[gpui::test]
fn a_bare_commit_shows_the_server_warning(cx: &mut TestAppContext) {
    let (panel, window) = result_grid(
        cx,
        with_notices(
            no_column_result(Some(0)),
            &[("WARNING", "there is no transaction in progress")],
        ),
    );

    window.update(|_, cx| {
        let panel = panel.read(cx);
        assert_eq!(
            panel.server_notice_lines(),
            vec!["WARNING: there is no transaction in progress".to_string()]
        );
        assert_eq!(panel.body_notice_lines(), panel.server_notice_lines());
    });
}

#[gpui::test]
fn every_notice_of_a_row_result_is_listed_in_order(cx: &mut TestAppContext) {
    let (panel, window) = result_grid(
        cx,
        with_notices(
            QueryResult::table(super::zero_row_columns(), Vec::new(), None, Duration::ZERO),
            &[("NOTICE", "first"), ("INFO", "second")],
        ),
    );

    window.update(|_, cx| {
        let panel = panel.read(cx);
        assert_eq!(
            panel.server_notice_lines(),
            vec!["NOTICE: first".to_string(), "INFO: second".to_string()]
        );
        assert!(
            panel.body_notice_lines().is_empty(),
            "a result with columns carries its notices in the footer only"
        );
    });
}

#[gpui::test]
fn a_result_without_notices_lists_none(cx: &mut TestAppContext) {
    let (panel, window) = result_grid(cx, no_column_result(Some(1)));

    window.update(|_, cx| {
        assert!(panel.read(cx).server_notice_lines().is_empty());
    });
}
