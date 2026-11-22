use super::{marker_for, token_range, verified_start};
use crate::code::CodeDocument;
use dbflux_app::keymap::Command;
use dbflux_components::controls::{InputPosition, RopeExt as _};
use dbflux_components::theme;
use dbflux_core::{ConnectionProfile, DbConfig, DbKind, QueryLanguage, WritePrivilege};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_test_support::FakeDriver;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::modals::test_host::host_modal;
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{
    AppContext as _, Entity, EntityInputHandler as _, Focusable as _, TestAppContext,
    VisualTestContext,
};
use std::ops::Range;

#[test]
fn a_token_is_taken_whole() {
    let text = "select a_1 from t where = 1";
    assert_eq!(token_range(text, 7), 7..10);
    assert_eq!(&text[token_range(text, 0)], "select");
}

#[test]
fn punctuation_is_a_token_of_one_character() {
    let text = "select 1 from t where = 1";
    let offset = text.find('=').expect("operator");
    assert_eq!(token_range(text, offset), offset..offset + 1);
}

#[test]
fn a_quoted_token_runs_through_its_closing_quote() {
    let text = "select 'ab c' from t";
    assert_eq!(&text[token_range(text, 7)], "'ab c'");
    let unterminated = "select 'ab";
    assert_eq!(&unterminated[token_range(unterminated, 7)], "'");
}

#[test]
fn the_end_of_the_text_is_an_empty_token() {
    assert_eq!(token_range("select", 6), 6..6);
    assert_eq!(token_range("select", 40), 6..6);
}

#[test]
fn a_multibyte_token_is_not_split() {
    let text = "select é, 日本 from t";
    let offset = text.find('日').expect("token");
    assert_eq!(&text[token_range(text, offset)], "日本");
}

#[test]
fn the_sent_text_must_match_the_buffer_where_it_started() {
    let buffer = "select 1;\nselect 2;";
    assert_eq!(verified_start(buffer, "select 2;", Some(10)), Some(10));
    assert_eq!(verified_start(buffer, "select 2;", Some(0)), None);
    assert_eq!(verified_start(buffer, "select 2;", None), None);
    assert_eq!(verified_start(buffer, "select 2;", Some(500)), None);
}

#[test]
fn a_marker_needs_text_that_still_matches_and_a_position_inside_it() {
    let buffer = "select 1;\nselect from t;";
    let sent = "select from t;";
    let marker = marker_for(buffer, sent, Some(10), 7, "syntax error").expect("marker");
    assert_eq!(&buffer[marker.range], "from");

    assert!(marker_for("select 1;\nselect  from t;", sent, Some(10), 7, "x").is_none());
    assert!(marker_for(buffer, sent, Some(10), 99, "x").is_none());
}

const SCRIPT: &str = "select 1;\nselect 'é' from where;\nselect 3;";

fn open_document<'a>(
    cx: &'a mut TestAppContext,
    driver: &FakeDriver,
    content: &str,
) -> (
    Entity<CodeDocument>,
    Entity<ToastHost>,
    &'a mut VisualTestContext,
) {
    cx.update(theme::init);
    cx.update(dbflux_ui_base::keymap::init_keymap);
    let toasts = cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host: host.clone() });
        host
    });
    let app_state = cx.new(|_| {
        AppStateEntity::new_with_storage_runtime(
            StorageRuntime::in_memory().expect("in-memory storage"),
        )
        .expect("app state")
    });

    let profile = ConnectionProfile::new(
        "fake",
        DbConfig::SQLite {
            path: ":memory:".into(),
            connection_id: None,
        },
    );
    let profile_id = profile.id;
    let connection = driver.connect_arc(&profile).expect("fake connection");
    cx.update(|cx| {
        app_state.update(cx, |app, _| {
            app.apply_connect_profile(
                profile,
                connection,
                None,
                None,
                false,
                WritePrivilege::Unknown,
            );
        });
    });

    let content = content.to_string();
    let (document, _outside, window) = host_modal(cx, move |window, cx| {
        let mut document =
            CodeDocument::new_with_language(app_state, None, QueryLanguage::Sql, window, cx);
        document.connection_id = Some(profile_id);
        document.set_content(&content, window, cx);
        document
    });
    window.run_until_parked();

    (document, toasts, window)
}

fn settle(window: &mut VisualTestContext) {
    for _ in 0..3 {
        window.run_until_parked();
        window.update(|window, _| window.refresh());
    }
    window.run_until_parked();
}

fn set_cursor(window: &mut VisualTestContext, document: &Entity<CodeDocument>, offset: usize) {
    select(window, document, offset..offset);
}

fn select(window: &mut VisualTestContext, document: &Entity<CodeDocument>, range: Range<usize>) {
    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document
                .editor
                .input_state
                .update(cx, |state, cx| state.set_selected_range(range, cx));
        });
    });
}

fn run(window: &mut VisualTestContext, document: &Entity<CodeDocument>) {
    window.update(|window, cx| {
        document.update(cx, |document, cx| document.run_query(window, cx));
    });
    settle(window);
}

/// The byte range the marker covers, when there is one.
fn marked(window: &mut VisualTestContext, document: &Entity<CodeDocument>) -> Option<Range<usize>> {
    window.update(|_, cx| {
        document
            .read(cx)
            .editor
            .query_error
            .as_ref()
            .map(|marker| marker.range.clone())
    })
}

/// The editor's error diagnostics as byte ranges in the buffer.
fn diagnostic_ranges(
    window: &mut VisualTestContext,
    document: &Entity<CodeDocument>,
) -> Vec<Range<usize>> {
    window.update(|_, cx| {
        document
            .read(cx)
            .editor
            .input_state
            .read(cx)
            .diagnostics()
            .map(|set| set.iter().map(|entry| entry.range.clone()).collect())
            .unwrap_or_default()
    })
}

fn cursor(window: &mut VisualTestContext, document: &Entity<CodeDocument>) -> usize {
    window.update(|_, cx| document.read(cx).editor.input_state.read(cx).cursor())
}

fn type_at_end(window: &mut VisualTestContext, document: &Entity<CodeDocument>, text: &str) {
    window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.editor.input_state.update(cx, |state, cx| {
                let end = state.value().len();
                state.set_cursor_position(state.text().offset_to_position(end), window, cx);
                state.replace_text_in_range(None, text, window, cx);
            });
        });
    });
    settle(window);
}

fn go_to_error(window: &mut VisualTestContext, document: &Entity<CodeDocument>) -> bool {
    let handled = window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.dispatch_command(Command::GoToQueryError, window, cx)
        })
    });
    settle(window);
    handled
}

fn token_start(buffer: &str, token: &str) -> Range<usize> {
    let start = buffer.find(token).expect("token in buffer");
    start..start + token.len()
}

#[gpui::test]
fn a_statement_under_the_cursor_marks_the_token_in_the_buffer(cx: &mut TestAppContext) {
    let statement = "select 'é' from where";
    let position = statement.find("where").expect("token");
    let driver = FakeDriver::new(DbKind::SQLite).with_query_error_at(
        statement,
        "syntax error at or near \"where\"",
        position,
    );
    let (document, _toasts, window) = open_document(cx, &driver, SCRIPT);
    set_cursor(
        window,
        &document,
        SCRIPT.find("from where").expect("cursor"),
    );

    run(window, &document);

    assert_eq!(
        marked(window, &document),
        Some(token_start(SCRIPT, "where"))
    );
    assert_eq!(
        diagnostic_ranges(window, &document),
        vec![token_start(SCRIPT, "where")]
    );
}

#[gpui::test]
fn a_whole_buffer_run_marks_the_token_in_the_buffer(cx: &mut TestAppContext) {
    let buffer = "select 'é' from where";
    let position = buffer.find("where").expect("token");
    let driver =
        FakeDriver::new(DbKind::SQLite).with_query_error_at(buffer, "syntax error", position);
    let (document, _toasts, window) = open_document(cx, &driver, buffer);

    run(window, &document);

    assert_eq!(
        marked(window, &document),
        Some(token_start(buffer, "where"))
    );
}

#[gpui::test]
fn a_selection_run_marks_the_token_relative_to_the_selection_start(cx: &mut TestAppContext) {
    let selected = "select 'é' from where";
    let position = selected.find("where").expect("token");
    let driver =
        FakeDriver::new(DbKind::SQLite).with_query_error_at(selected, "syntax error", position);
    let buffer = format!("select 1;\n  {selected}  \nselect 3;");
    let (document, _toasts, window) = open_document(cx, &driver, &buffer);
    // The selection carries surrounding whitespace, which the run trims.
    let start = buffer.find("  select 'é'").expect("selection");
    select(window, &document, start..start + selected.len() + 4);

    run(window, &document);

    assert_eq!(
        marked(window, &document),
        Some(token_start(&buffer, "where"))
    );
}

#[gpui::test]
fn nothing_moves_when_the_run_fails(cx: &mut TestAppContext) {
    let buffer = "select 1;\nselect from t;";
    let position = "select ".len();
    let driver = FakeDriver::new(DbKind::SQLite).with_query_error_at(
        "select from t",
        "syntax error",
        position,
    );
    let (document, _toasts, window) = open_document(cx, &driver, buffer);
    let before = buffer.find("select from").expect("cursor");
    set_cursor(window, &document, before);

    run(window, &document);

    assert!(marked(window, &document).is_some());
    assert_eq!(cursor(window, &document), before);
}

#[gpui::test]
fn go_to_error_moves_the_cursor_onto_the_marked_token(cx: &mut TestAppContext) {
    let buffer = "select 1;\nselect from t;";
    let driver = FakeDriver::new(DbKind::SQLite).with_query_error_at(
        "select from t",
        "syntax error",
        "select ".len(),
    );
    let (document, _toasts, window) = open_document(cx, &driver, buffer);
    set_cursor(
        window,
        &document,
        buffer.find("select from").expect("cursor"),
    );
    run(window, &document);

    assert!(go_to_error(window, &document));

    assert_eq!(
        cursor(window, &document),
        buffer.find("from t").expect("token")
    );
}

#[gpui::test]
fn the_toast_action_moves_the_cursor_on_the_next_render(cx: &mut TestAppContext) {
    let buffer = "select 1;\nselect from t;";
    let driver = FakeDriver::new(DbKind::SQLite).with_query_error_at(
        "select from t",
        "syntax error",
        "select ".len(),
    );
    let (document, toasts, window) = open_document(cx, &driver, buffer);
    let before = buffer.find("select from").expect("cursor");
    set_cursor(window, &document, before);
    run(window, &document);

    let (toast_id, labels) = window.update(|_, cx| {
        let host = toasts.read(cx);
        let id = host.latest_toast_id().expect("the failure shows a toast");
        let actions = host.toast_controls(id).expect("controls").actions;
        (
            id,
            actions
                .into_iter()
                .map(|action| action.id.to_string())
                .collect::<Vec<_>>(),
        )
    });
    assert_eq!(labels, vec!["copy-error", "go-to-query-error"]);
    assert_eq!(
        cursor(window, &document),
        before,
        "the toast alone moves nothing"
    );

    window.update(|_, cx| ToastHost::run_action(&toasts, toast_id, 1, cx));
    settle(window);

    assert_eq!(
        cursor(window, &document),
        buffer.find("from t").expect("token")
    );
}

#[gpui::test]
fn an_edit_clears_the_marker(cx: &mut TestAppContext) {
    let buffer = "select from t";
    let driver = FakeDriver::new(DbKind::SQLite).with_query_error_at(buffer, "syntax error", 7);
    let (document, _toasts, window) = open_document(cx, &driver, buffer);
    run(window, &document);
    assert!(marked(window, &document).is_some());
    assert!(!diagnostic_ranges(window, &document).is_empty());

    type_at_end(window, &document, " ");

    assert_eq!(marked(window, &document), None);
    assert!(diagnostic_ranges(window, &document).is_empty());
}

#[gpui::test]
fn the_next_run_clears_the_marker(cx: &mut TestAppContext) {
    let buffer = "select from t";
    let driver = FakeDriver::new(DbKind::SQLite).with_query_error_at(buffer, "syntax error", 7);
    let (document, _toasts, window) = open_document(cx, &driver, buffer);
    run(window, &document);
    assert!(marked(window, &document).is_some());

    // The statement now succeeds on the fake server.
    window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.set_content("select 1", window, cx);
        });
    });
    settle(window);
    run(window, &document);

    assert_eq!(marked(window, &document), None);
}

#[gpui::test]
fn a_failure_without_a_position_leaves_no_marker(cx: &mut TestAppContext) {
    let buffer = "select from t";
    let driver = FakeDriver::new(DbKind::SQLite).with_query_error(buffer, "boom");
    let (document, _toasts, window) = open_document(cx, &driver, buffer);

    run(window, &document);

    assert_eq!(marked(window, &document), None);
}

#[gpui::test]
fn a_buffer_edited_since_the_run_started_gets_no_marker(cx: &mut TestAppContext) {
    let driver = FakeDriver::new(DbKind::SQLite);
    let (document, _toasts, window) = open_document(cx, &driver, "select  from t");
    let error = dbflux_core::DbError::QueryFailed(
        dbflux_core::FormattedError::new("syntax error").with_position(7),
    );

    // The run sent `select from t`, which the buffer no longer holds.
    let marked_after_edit = window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.mark_failed_query("select from t", Some(0), &error, cx)
        })
    });

    assert!(!marked_after_edit);
    assert_eq!(marked(window, &document), None);
}

#[gpui::test]
fn go_to_error_does_nothing_without_a_marker(cx: &mut TestAppContext) {
    let driver = FakeDriver::new(DbKind::SQLite);
    let (document, _toasts, window) = open_document(cx, &driver, "select 1");
    set_cursor(window, &document, 3);

    assert!(!go_to_error(window, &document));
    assert_eq!(cursor(window, &document), 3);

    let enabled = window.update(|_, cx| {
        let entity = document.clone();
        document
            .read(cx)
            .pane_actions(&entity, cx)
            .into_iter()
            .find(|action| action.id == "go-to-query-error")
            .map(|action| action.enabled)
    });
    assert_eq!(enabled, Some(false));
}

#[gpui::test]
fn the_pane_action_is_enabled_while_a_marker_exists(cx: &mut TestAppContext) {
    let buffer = "select from t";
    let driver = FakeDriver::new(DbKind::SQLite).with_query_error_at(buffer, "syntax error", 7);
    let (document, _toasts, window) = open_document(cx, &driver, buffer);
    run(window, &document);

    let enabled = window.update(|_, cx| {
        let entity = document.clone();
        document
            .read(cx)
            .pane_actions(&entity, cx)
            .into_iter()
            .find(|action| action.id == "go-to-query-error")
            .map(|action| action.enabled)
    });

    assert_eq!(enabled, Some(true));
}

#[test]
fn input_positions_are_zero_based_lines_and_character_columns() {
    let position = dbflux_core::byte_offset_to_line_column("select 'é'\nfrom", 12);
    assert_eq!(position, Some((1, 0)));
    assert_eq!(InputPosition::new(1, 0).line, 1);
}

#[gpui::test]
fn a_selection_marker_never_extends_into_text_that_did_not_run(cx: &mut TestAppContext) {
    let driver = FakeDriver::new(DbKind::SQLite).with_query_error_at("select foo", "boom", 7);
    let (document, _toasts, window) = open_document(cx, &driver, "select foobar;");
    select(window, &document, 0.."select foo".len());

    run(window, &document);

    assert_eq!(marked(window, &document), Some(7..10));
}

fn editor_focused(window: &mut VisualTestContext, document: &Entity<CodeDocument>) -> bool {
    window.update(|window, cx| {
        document
            .read(cx)
            .editor
            .input_state
            .read(cx)
            .focus_handle(cx)
            .is_focused(window)
    })
}

#[gpui::test]
fn go_to_error_from_results_focus_focuses_the_editor(cx: &mut TestAppContext) {
    let buffer = "select 1;\nselect from t;";
    let driver = FakeDriver::new(DbKind::SQLite).with_query_error_at("select from t", "e", 7);
    let (document, _toasts, window) = open_document(cx, &driver, buffer);
    set_cursor(
        window,
        &document,
        buffer.find("select from").expect("cursor"),
    );
    run(window, &document);
    window.update(|_, cx| {
        document.update(cx, |document, _| {
            document.focus_mode = crate::code::SqlQueryFocus::Results;
        });
    });

    assert!(go_to_error(window, &document));

    assert!(editor_focused(window, &document));
    assert_eq!(
        cursor(window, &document),
        buffer.find("from t").expect("token")
    );
}

#[gpui::test]
fn go_to_error_reveals_an_editor_hidden_by_maximized_results(cx: &mut TestAppContext) {
    let buffer = "select 1;\nselect from t;";
    let driver = FakeDriver::new(DbKind::SQLite).with_query_error_at("select from t", "e", 7);
    let (document, _toasts, window) = open_document(cx, &driver, buffer);
    set_cursor(
        window,
        &document,
        buffer.find("select from").expect("cursor"),
    );
    run(window, &document);
    window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.dispatch_command(Command::ToggleResults, window, cx);
        });
    });
    settle(window);
    let hidden =
        window.update(|_, cx| document.read(cx).layout == crate::code::SqlQueryLayout::ResultsOnly);
    assert!(hidden, "the editor starts hidden");

    assert!(go_to_error(window, &document));

    let layout = window.update(|_, cx| document.read(cx).layout);
    assert!(layout != crate::code::SqlQueryLayout::ResultsOnly);
    assert!(editor_focused(window, &document));
    assert_eq!(
        cursor(window, &document),
        buffer.find("from t").expect("token")
    );
}
