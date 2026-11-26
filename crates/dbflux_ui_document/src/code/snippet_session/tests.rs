//! Snippet sessions against a real `CodeDocument` editor. Keys go through the
//! same dispatch path as typing: key bindings, then the input handler.

use crate::code::{CodeDocument, VimMode};
use dbflux_components::theme;
use dbflux_core::{ParsedSnippet, QueryLanguage, parse_snippet};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::keymap::init_keymap;
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{AppContext as _, Entity, EntityInputHandler as _, TestAppContext, VisualTestContext};
use gpui_component::Root;
use lsp_types::CompletionItem;
use std::cell::RefCell;
use std::ops::Range;
use std::rc::Rc;

/// The editor binds Undo to Cmd+Z on macOS and Ctrl+Z elsewhere.
#[cfg(target_os = "macos")]
const UNDO_KEYS: &str = "cmd-z";
#[cfg(not(target_os = "macos"))]
const UNDO_KEYS: &str = "ctrl-z";

const CTE: &str = "WITH ${1:name} AS (\n  ${2:SELECT 1}\n)\nSELECT *\nFROM $1;$0";
const SELECT: &str = "SELECT ${2:*}\nFROM ${1:table}\nWHERE ${3:condition};$0";

struct Fixture<'a> {
    document: Entity<CodeDocument>,
    window: &'a mut VisualTestContext,
}

fn open<'a>(cx: &'a mut TestAppContext, content: &str, vim: bool) -> Fixture<'a> {
    cx.update(gpui_component::init);
    cx.update(theme::init);
    cx.update(|cx| {
        let host = cx.new(|_cx| ToastHost::new());
        cx.set_global(ToastGlobal { host });
        init_keymap(cx);
    });

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let storage = StorageRuntime::in_memory().expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage).expect("test storage setup")
        })
    });
    cx.update(|cx| {
        app_state.update(cx, |state, _cx| {
            let mut settings = state.general_settings().clone();
            settings.vim_mode = vim;
            state.update_general_settings(settings);
        });
    });

    let slot: Rc<RefCell<Option<Entity<CodeDocument>>>> = Rc::new(RefCell::new(None));
    let writer = slot.clone();
    let content = content.to_string();
    let (_, window) = cx.add_window_view(|window, cx| {
        let document = cx.new(|cx| {
            let mut document = CodeDocument::new_with_language(
                app_state.clone(),
                None,
                QueryLanguage::Lua,
                window,
                cx,
            );
            document.set_content(&content, window, cx);
            document
        });
        writer.replace(Some(document.clone()));
        Root::new(document, window, cx)
    });
    let document = slot.borrow().clone().expect("document");
    window.update(|window, cx| document.update(cx, |document, cx| document.focus(window, cx)));
    window.run_until_parked();

    Fixture { document, window }
}

impl Fixture<'_> {
    fn insert(&mut self, body: &str, range: Range<usize>) -> ParsedSnippet {
        let parsed = parse_snippet(body).expect("snippet parses");
        let document = self.document.clone();
        let snippet = parsed.clone();
        self.window.update(|window, cx| {
            document.update(cx, |document, cx| {
                document.insert_snippet(&snippet, range, window, cx)
            })
        });
        self.window.run_until_parked();
        parsed
    }

    fn keys(&mut self, keystrokes: &str) {
        self.window.simulate_keystrokes(keystrokes);
        self.window.run_until_parked();
    }

    fn type_text(&mut self, input: &str) {
        self.window.simulate_input(input);
        self.window.run_until_parked();
    }

    fn text(&mut self) -> String {
        let document = self.document.clone();
        self.window.update(|_, cx| {
            document
                .read(cx)
                .editor
                .input_state
                .read(cx)
                .value()
                .to_string()
        })
    }

    fn selection(&mut self) -> Range<usize> {
        let document = self.document.clone();
        self.window.update(|_, cx| {
            document
                .read(cx)
                .editor
                .input_state
                .read(cx)
                .selected_range()
        })
    }

    fn selected_text(&mut self) -> String {
        let text = self.text();
        let selection = self.selection();
        text[selection].to_string()
    }

    fn active(&mut self) -> bool {
        let document = self.document.clone();
        self.window
            .update(|_, cx| document.read(cx).snippet_session_active())
    }

    fn mode(&mut self) -> Option<VimMode> {
        let document = self.document.clone();
        self.window.update(|_, cx| document.read(cx).vim_mode())
    }
}

#[gpui::test]
fn insert_replaces_the_range_and_selects_the_first_default(cx: &mut TestAppContext) {
    let mut editor = open(cx, "-- note\nsel", false);
    editor.insert(SELECT, 8..11);

    assert_eq!(
        editor.text(),
        "-- note\nSELECT *\nFROM table\nWHERE condition;"
    );
    assert_eq!(editor.selected_text(), "table");
    assert!(editor.active());
}

#[gpui::test]
fn tab_walks_the_fields_and_the_final_stop_ends_the_session(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    editor.insert(SELECT, 0..0);

    editor.keys("tab");
    assert_eq!(editor.selected_text(), "*");
    editor.keys("tab");
    assert_eq!(editor.selected_text(), "condition");
    assert!(editor.active());

    editor.keys("tab");
    let text = editor.text();
    assert_eq!(editor.selection(), text.len()..text.len());
    assert!(!editor.active(), "the final stop ends the session");
    assert_eq!(text, "SELECT *\nFROM table\nWHERE condition;");
}

#[gpui::test]
fn explicit_final_stop_in_the_middle_receives_the_cursor(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    editor.insert("a(${1:x})$0 tail", 0..0);

    editor.keys("tab");
    assert_eq!(editor.selection(), 4..4);
    assert!(!editor.active());
    assert_eq!(editor.text(), "a(x) tail");
}

#[gpui::test]
fn shift_tab_goes_back_and_stays_on_the_first_field(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    editor.insert(SELECT, 0..0);

    editor.keys("tab tab");
    assert_eq!(editor.selected_text(), "condition");
    editor.keys("shift-tab");
    assert_eq!(editor.selected_text(), "*");
    editor.keys("shift-tab shift-tab");
    assert_eq!(editor.selected_text(), "table");
    assert!(editor.active());
}

#[gpui::test]
fn typing_in_a_field_updates_its_mirrors(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    editor.insert(CTE, 0..0);

    editor.type_text("orders");
    assert_eq!(
        editor.text(),
        "WITH orders AS (\n  SELECT 1\n)\nSELECT *\nFROM orders;"
    );
    assert!(editor.active());
    let cursor = editor.selection();
    assert_eq!(cursor, 11..11, "the caret stays after the typed text");

    editor.type_text("_2");
    assert_eq!(
        editor.text(),
        "WITH orders_2 AS (\n  SELECT 1\n)\nSELECT *\nFROM orders_2;"
    );

    editor.keys("tab");
    assert_eq!(editor.selected_text(), "SELECT 1");
    editor.keys("backspace");
    editor.type_text("2");
    editor.keys("tab");
    assert!(!editor.active());
    assert_eq!(
        editor.text(),
        "WITH orders_2 AS (\n  2\n)\nSELECT *\nFROM orders_2;"
    );
}

#[gpui::test]
fn deleting_a_mirrored_default_empties_every_mirror(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    editor.insert(CTE, 0..0);

    editor.keys("backspace");
    assert_eq!(editor.text(), "WITH  AS (\n  SELECT 1\n)\nSELECT *\nFROM ;");
    editor.keys("tab");
    assert_eq!(editor.selected_text(), "SELECT 1");
}

#[gpui::test]
fn escape_ends_the_session_and_tab_indents_again(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    editor.insert(SELECT, 0..0);
    let before = editor.text();

    editor.keys("escape");
    assert!(!editor.active());
    assert_eq!(editor.text(), before);

    editor.keys("tab");
    assert_ne!(editor.text(), before, "Tab indents once the session ended");
}

#[gpui::test]
fn clicking_outside_the_field_ends_the_session(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    editor.insert(SELECT, 0..0);

    let document = editor.document.clone();
    editor.window.update(|_, cx| {
        let input = document.read(cx).editor.input_state.clone();
        input.update(cx, |state, cx| {
            state.set_selected_range(0..0, cx);
            cx.emit(gpui_base::input::InputPointerEvent::CaretPlaced);
        });
    });
    editor.window.run_until_parked();
    assert!(!editor.active());
}

#[gpui::test]
fn editing_outside_every_field_ends_the_session(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    editor.insert(SELECT, 0..0);

    let document = editor.document.clone();
    editor.window.update(|window, cx| {
        let input = document.read(cx).editor.input_state.clone();
        input.update(cx, |state, cx| {
            state.set_selected_range(0..0, cx);
            state.replace_text_in_range(None, "x", window, cx);
        });
    });
    editor.window.run_until_parked();
    assert!(!editor.active());
    assert!(editor.text().starts_with("xSELECT"));
}

#[gpui::test]
fn undo_reverts_the_insertion_and_ends_the_session(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    editor.insert(CTE, 0..0);
    editor.type_text("orders");

    editor.keys(UNDO_KEYS);
    assert!(!editor.active());
    assert_eq!(
        editor.text(),
        "",
        "insertion and its field edits undo together"
    );
}

#[gpui::test]
fn only_a_final_stop_places_the_cursor_without_a_session(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    editor.insert("SELECT 1;", 0..0);
    assert!(!editor.active());
    assert_eq!(editor.selection(), 9..9);
}

#[gpui::test]
fn a_new_insertion_replaces_the_running_session(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    editor.insert(SELECT, 0..0);
    let len = editor.text().len();
    editor.insert("DELETE FROM ${1:t};$0", len..len);
    assert_eq!(editor.selected_text(), "t");
    editor.keys("tab");
    assert!(!editor.active());
}

#[gpui::test]
fn a_menu_open_keeps_tab_cycling_completions(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    editor.insert(SELECT, 0..0);
    let table = editor.selection();
    assert_eq!(table, 14..19);

    let items = vec![completion("orders"), completion("users")];
    let document = editor.document.clone();
    editor.window.update(|_, cx| {
        let input = document.read(cx).editor.input_state.clone();
        input.update(cx, |state, cx| {
            let highlighted = Rc::new(std::cell::Cell::new(0usize));
            state.set_overlay_action_handler({
                let highlighted = highlighted.clone();
                move |_, _, _, _| {
                    highlighted.set(highlighted.get() + 1);
                    true
                }
            });
            state.set_completion_selection_reader({
                let items = items.clone();
                let highlighted = highlighted.clone();
                move |_| items.get(highlighted.get() % 2).cloned()
            });
            state.present_completion_items(14, "table", items.clone(), cx);
        });
    });

    editor.keys("tab");
    assert!(editor.text().contains("FROM orders"), "{}", editor.text());
    assert!(editor.active(), "cycling a completion keeps the session");
    assert_ne!(
        editor.selected_text(),
        "*",
        "Tab did not move to the next field"
    );

    editor.keys("escape");
}

fn completion(label: &str) -> CompletionItem {
    CompletionItem {
        label: label.to_string(),
        ..CompletionItem::default()
    }
}

#[gpui::test]
fn leaving_vim_insert_mode_ends_the_session(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", true);
    editor.keys("i");
    assert_eq!(editor.mode(), Some(VimMode::Insert));
    editor.insert(SELECT, 0..0);
    editor.keys("tab");
    assert_eq!(
        editor.selected_text(),
        "*",
        "Tab moves fields in Insert mode"
    );

    editor.keys("escape");
    assert_eq!(editor.mode(), Some(VimMode::Normal));
    assert!(!editor.active());
}

struct PlainEditor {
    input: Entity<gpui_component::input::EditorState>,
}

impl gpui::Render for PlainEditor {
    fn render(
        &mut self,
        _window: &mut gpui::Window,
        _cx: &mut gpui::Context<Self>,
    ) -> impl gpui::IntoElement {
        gpui_component::input::Editor::new(&self.input)
    }
}

#[gpui::test]
fn an_editor_without_the_hook_still_indents_on_tab(cx: &mut TestAppContext) {
    cx.update(gpui_component::init);
    cx.update(theme::init);

    let slot: Rc<RefCell<Option<Entity<gpui_component::input::EditorState>>>> =
        Rc::new(RefCell::new(None));
    let writer = slot.clone();
    let (_, window) = cx.add_window_view(|window, cx| {
        let input = cx.new(|cx| gpui_component::input::EditorState::new(window, cx));
        writer.replace(Some(input.clone()));
        Root::new(cx.new(|_| PlainEditor { input }), window, cx)
    });
    let input = slot.borrow().clone().expect("input");
    window.update(|window, cx| {
        input.update(cx, |state, cx| {
            state.set_value("a", window, cx);
            state.focus(window, cx);
            state.set_selected_range(0..0, cx);
        });
    });
    window.run_until_parked();

    window.simulate_keystrokes("tab");
    window.run_until_parked();
    let text = window.update(|_, cx| input.read(cx).value().to_string());
    assert_ne!(text, "a", "Tab indents an editor with no interceptor");
    assert!(text.ends_with('a'));
}

#[gpui::test]
fn replace_ranges_ignores_overlapping_or_out_of_bounds_edits(cx: &mut TestAppContext) {
    let mut editor = open(cx, "abcdef", false);
    let document = editor.document.clone();
    editor.window.update(|window, cx| {
        let input = document.read(cx).editor.input_state.clone();
        input.update(cx, |state, cx| {
            state.replace_ranges(&[(0..3, "x".into()), (2..4, "y".into())], window, cx);
            state.replace_ranges(&[(4..99, "z".into())], window, cx);
        });
    });
    assert_eq!(editor.text(), "abcdef");

    editor.window.update(|window, cx| {
        let input = document.read(cx).editor.input_state.clone();
        input.update(cx, |state, cx| {
            state.replace_ranges(&[(4..6, "Z".into()), (0..1, "A".into())], window, cx);
        });
    });
    assert_eq!(editor.text(), "AbcdZ");
}

#[gpui::test]
fn replacing_a_selection_that_reaches_into_a_field_ends_the_session(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    editor.insert("L${1:x} / $1$0", 0..0);
    assert_eq!(editor.selected_text(), "x");

    let document = editor.document.clone();
    editor.window.update(|_, cx| {
        let input = document.read(cx).editor.input_state.clone();
        input.update(cx, |state, cx| state.set_selected_range(0..2, cx));
    });
    editor.type_text("z");

    assert_eq!(
        editor.text(),
        "z / x",
        "the untouched mirror is not rewritten"
    );
    assert!(!editor.active());
}

#[gpui::test]
fn undoing_a_snippet_whose_fields_start_together_ends_the_session(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    editor.insert("$1$0 tail", 0..0);
    assert!(editor.active());

    editor.keys(UNDO_KEYS);

    assert_eq!(editor.text(), "");
    assert!(!editor.active());
}

fn clearing_the_only_field_keeps_the_session(editor: &mut Fixture<'_>, key: &str) {
    editor.insert("SELECT ${1:cols}$0", 0..0);
    assert_eq!(editor.selected_text(), "cols");

    editor.keys(key);
    assert!(editor.active(), "clearing the field keeps the session");
    assert_eq!(editor.text(), "SELECT ");

    editor.type_text("id");
    editor.keys("tab");
    assert_eq!(editor.text(), "SELECT id");
    assert_eq!(editor.selection(), 9..9, "Tab jumps to the final stop");
    assert!(!editor.active());
}

#[gpui::test]
fn clearing_the_only_field_with_backspace_keeps_the_session(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    clearing_the_only_field_keeps_the_session(&mut editor, "backspace");
}

#[gpui::test]
fn clearing_the_only_field_with_delete_keeps_the_session(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    clearing_the_only_field_keeps_the_session(&mut editor, "delete");
}

#[gpui::test]
fn replacing_the_buffer_ends_the_session(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    editor.insert(CTE, 0..0);
    assert!(editor.active());

    let document = editor.document.clone();
    editor.window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.set_content("SELECT 1", window, cx)
        })
    });
    editor.window.run_until_parked();

    assert_eq!(editor.text(), "SELECT 1");
    assert!(!editor.active());
}

struct OffersTables;

impl gpui_base::input::CompletionProvider for OffersTables {
    fn completions(
        &self,
        _text: &gpui_base::input::Rope,
        _offset: usize,
        _trigger: lsp_types::CompletionContext,
        _window: &mut gpui::Window,
        _cx: &mut gpui::App,
    ) -> gpui::Task<anyhow::Result<lsp_types::CompletionResponse>> {
        gpui::Task::ready(Ok(lsp_types::CompletionResponse::Array(vec![
            completion("orders"),
            completion("users"),
        ])))
    }

    fn is_completion_trigger(&self, _offset: usize, new_text: &str, _cx: &mut gpui::App) -> bool {
        !new_text.is_empty()
    }
}

#[gpui::test]
fn typing_in_a_mirrored_field_keeps_the_completion_menu(cx: &mut TestAppContext) {
    let mut editor = open(cx, "", false);
    let document = editor.document.clone();
    editor.window.update(|_, cx| {
        let input = document.read(cx).editor.input_state.clone();
        input.update(cx, |state, _| {
            state.lsp_mut().completion_provider = Some(Rc::new(OffersTables));
        });
    });
    editor.insert("SELECT * FROM ${1:t}; SELECT * FROM $1;$0", 0..0);

    editor.type_text("o");

    assert_eq!(editor.text(), "SELECT * FROM o; SELECT * FROM o;");
    assert!(editor.active());
    let open = editor.window.update(|_, cx| {
        let input = document.read(cx).editor.input_state.read(cx);
        input.completion_menu_state().open
    });
    assert!(open, "the mirror rewrite did not drop the completion menu");
}
