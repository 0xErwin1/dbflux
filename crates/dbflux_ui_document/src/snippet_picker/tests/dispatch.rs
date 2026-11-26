//! The picker inside a real `CodeDocument`, driven through the app keymap the
//! way the workspace drives it: Ctrl+Alt+J opens it, typing and Enter go to
//! the focused search input.

use crate::code::{CodeDocument, VimMode};
use crate::keyboard_test_support::{KeymapHost, host_document, init_keyboard_runtime};
use dbflux_app::keymap::Command;
use dbflux_core::QueryLanguage;
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::AppStateEntity;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};

struct Fixture<'a> {
    document: Entity<CodeDocument>,
    window: &'a mut VisualTestContext,
    _host: Entity<KeymapHost<CodeDocument>>,
}

fn open(cx: &mut TestAppContext) -> Fixture<'_> {
    init_keyboard_runtime(cx);
    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let storage = StorageRuntime::in_memory().expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage).expect("test storage setup")
        })
    });

    let (host, window) = host_document(
        cx,
        move |window, cx| {
            cx.new(|cx| {
                CodeDocument::new_with_language(app_state, None, QueryLanguage::Sql, window, cx)
            })
        },
        |document, cx| document.active_context(cx),
        // The workspace answers Execute, SelectNext and SelectPrev itself
        // whenever a document has focus (`dispatch/navigation.rs`), so those
        // keys never reach an element listener of the picker; the document
        // must take them from `dispatch_command`.
        |document, command, window, cx| {
            let handled = document.dispatch_command(command, window, cx);
            handled
                || matches!(
                    command,
                    Command::Execute | Command::SelectNext | Command::SelectPrev
                )
        },
    );
    let document = window.update(|_, cx| host.read(cx).document.clone());
    window.update(|window, cx| document.update(cx, |document, cx| document.focus(window, cx)));
    window.run_until_parked();

    Fixture {
        document,
        window,
        _host: host,
    }
}

impl Fixture<'_> {
    fn keys(&mut self, keystrokes: &str) {
        self.window.simulate_keystrokes(keystrokes);
        self.window.run_until_parked();
    }

    fn type_text(&mut self, input: &str) {
        self.window.simulate_input(input);
        self.window.run_until_parked();
    }

    fn picker_open(&mut self) -> bool {
        let document = self.document.clone();
        self.window
            .update(|_, cx| document.read(cx).snippet_picker_open())
    }

    fn text_and_selection(&mut self) -> (String, String) {
        let document = self.document.clone();
        self.window.update(|_, cx| {
            let (text, range) = document.read(cx).editor_text_and_selection(cx);
            let selected = text.get(range).unwrap_or_default().to_string();
            (text, selected)
        })
    }

    fn session_active(&mut self) -> bool {
        let document = self.document.clone();
        self.window
            .update(|_, cx| document.read(cx).snippet_session_active())
    }

    fn vim_mode(&mut self) -> Option<VimMode> {
        let document = self.document.clone();
        self.window.update(|_, cx| document.read(cx).vim_mode())
    }

    fn highlighted_name(&mut self) -> Option<String> {
        let document = self.document.clone();
        self.window
            .update(|_, cx| document.read(cx).snippet_picker_highlighted_name(cx))
    }

    fn open_filtered(&mut self) {
        self.keys("ctrl-alt-j");
        assert!(self.picker_open(), "Ctrl+Alt+J opens the picker");
        self.type_text("cte");
    }
}

#[gpui::test]
fn enter_in_the_search_inserts_the_snippet_and_closes(cx: &mut TestAppContext) {
    let mut fixture = open(cx);
    fixture.open_filtered();

    fixture.keys("enter");

    let (text, selected) = fixture.text_and_selection();
    assert!(text.contains("WITH name AS ("), "{text:?}");
    assert_eq!(selected, "name");
    assert!(fixture.session_active());
    assert!(!fixture.picker_open());
}

#[gpui::test]
fn escape_closes_without_inserting(cx: &mut TestAppContext) {
    let mut fixture = open(cx);
    fixture.open_filtered();

    fixture.keys("escape");

    assert!(!fixture.picker_open());
    assert_eq!(fixture.text_and_selection().0, "");
    assert!(!fixture.session_active());
}

#[gpui::test]
fn enter_with_no_match_keeps_the_picker_open(cx: &mut TestAppContext) {
    let mut fixture = open(cx);
    fixture.keys("ctrl-alt-j");
    fixture.type_text("zzzzqq");

    fixture.keys("enter");

    assert!(fixture.picker_open());
    assert_eq!(fixture.text_and_selection().0, "");
}

#[gpui::test]
fn clicking_a_row_inserts_the_snippet(cx: &mut TestAppContext) {
    use dbflux_ui_base::keyboard_coverage::FrameCapture;

    let mut fixture = open(cx);
    fixture.open_filtered();

    let capture = FrameCapture::observe(fixture.window);
    let frame = capture.frame(fixture.window);
    let row = frame
        .nodes()
        .find(|(_, node)| node.id() == "snippet-picker-row-0")
        .map(|(_, node)| node.bounds())
        .expect("the best match is listed first");
    fixture
        .window
        .simulate_click(row.center(), gpui::Modifiers::default());
    fixture.window.run_until_parked();

    let (text, _) = fixture.text_and_selection();
    assert!(text.contains("WITH name AS ("), "{text:?}");
    assert!(!fixture.picker_open());
}

#[gpui::test]
fn enter_in_vim_normal_mode_switches_to_insert_and_inserts(cx: &mut TestAppContext) {
    let mut fixture = open(cx);
    let document = fixture.document.clone();
    fixture
        .window
        .update(|_, cx| document.update(cx, |document, cx| document.set_vim_enabled(true, cx)));
    fixture.window.run_until_parked();
    assert_eq!(fixture.vim_mode(), Some(VimMode::Normal));
    fixture.open_filtered();

    fixture.keys("enter");

    let (text, selected) = fixture.text_and_selection();
    assert!(text.contains("WITH name AS ("), "{text:?}");
    assert_eq!(selected, "name");
    assert_eq!(fixture.vim_mode(), Some(VimMode::Insert));
    assert!(fixture.session_active());
    assert!(!fixture.picker_open());
}

#[gpui::test]
fn each_arrow_press_moves_the_highlight_one_row(cx: &mut TestAppContext) {
    let mut fixture = open(cx);
    fixture.keys("ctrl-alt-j");
    assert!(fixture.picker_open(), "Ctrl+Alt+J opens the picker");

    fixture.keys("down");
    assert_eq!(
        fixture.highlighted_name().as_deref(),
        Some("Common table expression")
    );

    fixture.keys("down");
    assert_eq!(fixture.highlighted_name().as_deref(), Some("Delete rows"));

    fixture.keys("up");
    assert_eq!(
        fixture.highlighted_name().as_deref(),
        Some("Common table expression")
    );

    fixture.keys("enter");
    let (text, _) = fixture.text_and_selection();
    assert!(text.contains("WITH name AS ("), "{text:?}");
    assert!(!fixture.picker_open());
}
