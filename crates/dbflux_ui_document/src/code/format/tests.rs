use super::{format_target, formatted_region};
use crate::code::CodeDocument;
use dbflux_app::keymap::Command;
use dbflux_components::theme;
use dbflux_core::{ConnectionProfile, DbConfig, DbKind, QueryLanguage, WritePrivilege};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_test_support::FakeDriver;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::modals::test_host::host_modal;
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use std::ops::Range;

const TWO_STATEMENTS: &str = "select a from t;\nselect b from u where id=@id;";

fn upper(text: &str) -> Option<String> {
    Some(text.to_uppercase())
}

#[test]
fn a_selection_is_the_target() {
    assert_eq!(
        format_target(&QueryLanguage::Sql, TWO_STATEMENTS, 7..13, 13),
        7..13
    );
    assert_eq!(
        format_target(&QueryLanguage::Sql, TWO_STATEMENTS, 13..7, 7),
        7..13
    );
}

#[test]
fn without_a_selection_the_statement_under_the_cursor_is_the_target() {
    let second = TWO_STATEMENTS.find("select b").expect("second statement");
    let target = format_target(
        &QueryLanguage::Sql,
        TWO_STATEMENTS,
        second + 3..second + 3,
        second + 3,
    );
    assert!(TWO_STATEMENTS[target].starts_with("select b from u"));

    let target = format_target(&QueryLanguage::Sql, TWO_STATEMENTS, 2..2, 2);
    assert!(TWO_STATEMENTS[target].starts_with("select a from t"));
}

#[test]
fn a_single_statement_formats_the_whole_buffer() {
    let buffer = "select a from t";
    assert_eq!(
        format_target(&QueryLanguage::Sql, buffer, 3..3, 3),
        0..buffer.len()
    );
}

#[test]
fn the_whitespace_around_the_region_is_kept() {
    let buffer = "\n  select a\n";
    assert_eq!(
        formatted_region(buffer, &(0..buffer.len()), upper).as_deref(),
        Some("\n  SELECT A\n")
    );
}

#[test]
fn an_unchanged_or_blank_region_is_not_rewritten() {
    assert_eq!(formatted_region("SELECT A", &(0..8), upper), None);
    assert_eq!(formatted_region("select   ", &(6..9), upper), None);
    assert_eq!(formatted_region("select", &(0..6), |_| None), None);
}

fn open_document<'a>(
    cx: &'a mut TestAppContext,
    content: &str,
    connected: bool,
    read_only: bool,
) -> (Entity<CodeDocument>, &'a mut VisualTestContext) {
    cx.update(theme::init);
    cx.update(dbflux_ui_base::keymap::init_keymap);
    cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host });
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
    if connected {
        let connection = FakeDriver::new(DbKind::SQLite)
            .connect_arc(&profile)
            .expect("fake connection");
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
    }

    let content = content.to_string();
    let (document, _outside, window) = host_modal(cx, move |window, cx| {
        let mut document =
            CodeDocument::new_with_language(app_state, None, QueryLanguage::Sql, window, cx);
        if connected {
            document.connection_id = Some(profile_id);
        }
        document.set_content(&content, window, cx);
        document.read_only = read_only;
        document
    });
    window.run_until_parked();

    (document, window)
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

fn format(window: &mut VisualTestContext, document: &Entity<CodeDocument>) -> bool {
    let handled = window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.dispatch_command(Command::FormatQuery, window, cx)
        })
    });
    window.run_until_parked();
    handled
}

fn text(window: &mut VisualTestContext, document: &Entity<CodeDocument>) -> String {
    window.update(|_, cx| {
        document
            .read(cx)
            .editor
            .input_state
            .read(cx)
            .value()
            .to_string()
    })
}

#[gpui::test]
fn format_rewrites_only_the_statement_under_the_cursor(cx: &mut TestAppContext) {
    let (document, window) = open_document(cx, TWO_STATEMENTS, true, false);
    let cursor = TWO_STATEMENTS.find("from u").expect("second statement");
    select(window, &document, cursor..cursor);

    assert!(format(window, &document));

    assert_eq!(
        text(window, &document),
        "select a from t;\nSELECT\n  b\nFROM\n  u\nWHERE\n  id = @id;"
    );
}

#[gpui::test]
fn format_rewrites_the_selection_and_keeps_it_selected(cx: &mut TestAppContext) {
    let (document, window) = open_document(cx, TWO_STATEMENTS, true, false);
    select(window, &document, 0..TWO_STATEMENTS.len());

    assert!(format(window, &document));

    let formatted = "SELECT\n  a\nFROM\n  t;\nSELECT\n  b\nFROM\n  u\nWHERE\n  id = @id;";
    assert_eq!(text(window, &document), formatted);
    let selection = window.update(|_, cx| {
        document
            .read(cx)
            .editor
            .input_state
            .read(cx)
            .selected_range()
    });
    assert_eq!(selection, 0..formatted.len());
}

#[gpui::test]
fn a_document_without_a_connection_declines_and_keeps_its_text(cx: &mut TestAppContext) {
    let (document, window) = open_document(cx, "select a from t", false, false);

    assert!(!format(window, &document));
    assert_eq!(text(window, &document), "select a from t");
}

#[gpui::test]
fn a_read_only_document_declines_and_keeps_its_text(cx: &mut TestAppContext) {
    let (document, window) = open_document(cx, "select a from t", true, true);

    assert!(!format(window, &document));
    assert_eq!(text(window, &document), "select a from t");
}

#[gpui::test]
fn the_format_pane_action_runs_the_command_when_the_service_can_format(cx: &mut TestAppContext) {
    let (document, window) = open_document(cx, "select a from t", true, false);

    let format = window.update(|_, cx| {
        document
            .read(cx)
            .pane_actions(&document, cx)
            .into_iter()
            .find(|action| action.id == "format")
            .map(|action| {
                (
                    action.enabled,
                    action.label.to_string(),
                    action.shortcut.is_some(),
                )
            })
    });

    assert_eq!(
        format,
        Some((true, dbflux_i18n::t!("document.code.toolbar.format"), true))
    );
}

#[gpui::test]
fn format_keeps_offsets_right_after_multibyte_text(cx: &mut TestAppContext) {
    let buffer = "select 'ñandú 😀' from t;\nselect b from u where id=@id;";
    let (document, window) = open_document(cx, buffer, true, false);
    let cursor = buffer.find("from u").expect("second statement");
    select(window, &document, cursor..cursor);

    assert!(format(window, &document));

    let expected = "select 'ñandú 😀' from t;\nSELECT\n  b\nFROM\n  u\nWHERE\n  id = @id;";
    assert_eq!(text(window, &document), expected);
    let selection = window.update(|_, cx| {
        document
            .read(cx)
            .editor
            .input_state
            .read(cx)
            .selected_range()
    });
    // The statement range stops before its `;`, and so does the caret.
    let end = expected.rfind(';').expect("terminator");
    assert_eq!(selection, end..end);
}
