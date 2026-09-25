//! Window-level tests for Vim mode against a real `CodeDocument` editor.
//!
//! Keys go through the real dispatch path (`simulate_keystrokes` /
//! `simulate_input`): key bindings, key listeners and then the platform input
//! handler, in the same order as on a desktop platform. The harness view stands in
//! for the workspace: its bubble-phase key listener resolves chords against the
//! default `KeymapStack`, which is how app shortcuts reach documents.

use super::VimMode;
use crate::code::CodeDocument;
use dbflux_app::keymap::Command;
use dbflux_components::controls::register_input_overrides;
use dbflux_components::controls::{GpuiInput, InputState};
use dbflux_components::theme;
use dbflux_core::{ConnectionProfile, DbConfig, DbKind, QueryLanguage, WritePrivilege};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_test_support::fake_driver::FakeDriver;
use dbflux_ui_base::keymap::{default_keymap, key_chord_from_gpui};
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use dbflux_ui_base::{AppStateChanged, AppStateEntity};
use gpui::{
    AppContext as _, ClipboardItem, Context, Entity, EntityInputHandler, Focusable as _,
    InteractiveElement as _, IntoElement, KeyBinding, KeyDownEvent, ParentElement as _, Render,
    Styled as _, TestAppContext, VisualTestContext, Window, actions, div,
};
use gpui_base::input::InputCursorShape;
use gpui_component::Root;
use gpui_component::input::Paste;
use std::cell::RefCell;
use std::rc::Rc;

actions!(vim_mode_test, [HarnessRunQuery]);

struct Harness {
    document: Entity<CodeDocument>,
    /// A second document, standing in for another open tab.
    second: Option<Entity<CodeDocument>>,
    /// An ordinary input outside the editor, to prove Vim mode stays scoped.
    other_input: Entity<InputState>,
    commands: Vec<Command>,
    run_query_actions: usize,
}

impl Render for Harness {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .on_action(cx.listener(|this, _: &HarnessRunQuery, window, cx| {
                this.run_query_actions += 1;
                this.document
                    .update(cx, |document, cx| document.run_query(window, cx));
            }))
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, _window, cx| {
                let context = this.document.read(cx).active_context(cx);
                let chord = key_chord_from_gpui(&event.keystroke);

                if let Some(command) = default_keymap().resolve(context, &chord) {
                    this.commands.push(command);
                }
            }))
            .child(self.document.clone())
            .children(self.second.clone())
            .child(GpuiInput::new(&self.other_input))
    }
}

fn init_runtime(cx: &mut TestAppContext) {
    cx.update(gpui_component::init);
    cx.update(theme::init);
    cx.update(|cx| {
        let host = cx.new(|_cx| ToastHost::new());
        cx.set_global(ToastGlobal { host });

        register_input_overrides(cx);
        cx.bind_keys([KeyBinding::new(
            "ctrl-enter",
            HarnessRunQuery,
            Some("Input"),
        )]);
    });
}

type FixtureSlot = Rc<RefCell<Option<(Entity<CodeDocument>, Entity<Harness>)>>>;

struct Fixture<'a> {
    app_state: Entity<AppStateEntity>,
    document: Entity<CodeDocument>,
    harness: Entity<Harness>,
    window: &'a mut VisualTestContext,
}

impl Fixture<'_> {
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

    fn clipboard_text(&mut self) -> Option<String> {
        self.window
            .update(|_, cx| cx.read_from_clipboard().and_then(|item| item.text()))
    }

    fn motion_offset(&mut self) -> usize {
        let document = self.document.clone();
        self.window.update(|_, cx| {
            let document = document.read(cx);
            document
                .vim
                .visual_cursor
                .unwrap_or_else(|| document.editor.input_state.read(cx).cursor())
        })
    }

    fn visual_caret(&mut self) -> Option<usize> {
        let document = self.document.clone();
        self.window.update(|_, cx| {
            document
                .read(cx)
                .editor
                .input_state
                .read(cx)
                .visual_caret_offset()
        })
    }

    fn cursor(&mut self) -> usize {
        let document = self.document.clone();
        self.window
            .update(|_, cx| document.read(cx).editor.input_state.read(cx).cursor())
    }

    fn mode(&mut self) -> Option<VimMode> {
        let document = self.document.clone();
        self.window.update(|_, cx| document.read(cx).vim_mode())
    }

    fn selection(&mut self) -> std::ops::Range<usize> {
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

    fn selected_query(&mut self) -> Option<String> {
        let document = self.document.clone();
        self.window.update(|window, cx| {
            document.update(cx, |document, cx| document.selected_query(window, cx))
        })
    }

    fn cursor_shape(&mut self) -> InputCursorShape {
        let document = self.document.clone();
        self.window
            .update(|_, cx| document.read(cx).editor.input_state.read(cx).cursor_shape())
    }

    fn editor_focused(&mut self) -> bool {
        let document = self.document.clone();
        self.window.update(|window, cx| {
            document
                .read(cx)
                .editor
                .input_state
                .read(cx)
                .focus_handle(cx)
                .is_focused(window)
        })
    }

    fn commands(&mut self) -> Vec<Command> {
        let harness = self.harness.clone();
        self.window
            .update(|_, cx| harness.read(cx).commands.clone())
    }

    fn run_query_actions(&mut self) -> usize {
        let harness = self.harness.clone();
        self.window
            .update(|_, cx| harness.read(cx).run_query_actions)
    }

    fn set_cursor(&mut self, offset: usize) {
        let document = self.document.clone();
        self.window.update(|_, cx| {
            document.update(cx, |document, cx| {
                document
                    .editor
                    .input_state
                    .update(cx, |state, cx| state.set_selected_range(offset..offset, cx));
            });
        });
    }

    /// Saves the Vim mode setting the way Settings > General does: the new
    /// settings, then the app-state event open editors listen to.
    fn set_vim(&mut self, enabled: bool) {
        let app_state = self.app_state.clone();
        self.window.update(|_, cx| {
            app_state.update(cx, |state, cx| {
                let mut settings = state.general_settings().clone();
                settings.vim_mode = enabled;
                state.update_general_settings(settings);
                cx.emit(AppStateChanged);
            });
        });
        self.window.run_until_parked();
    }

    fn emit_unrelated_app_state_change(&mut self) {
        let app_state = self.app_state.clone();
        self.window
            .update(|_, cx| app_state.update(cx, |_, cx| cx.emit(AppStateChanged)));
        self.window.run_until_parked();
    }

    fn focus_document(&mut self, document: &Entity<CodeDocument>) {
        let document = document.clone();
        self.window.update(|window, cx| {
            document.update(cx, |document, cx| document.focus(window, cx));
        });
        self.window.run_until_parked();
    }

    fn focus_other_input(&mut self) {
        let harness = self.harness.clone();
        self.window.update(|window, cx| {
            let other_input = harness.read(cx).other_input.clone();
            other_input.update(cx, |state, cx| state.focus(window, cx));
        });
        self.window.run_until_parked();
    }

    fn other_input_text(&mut self) -> String {
        let harness = self.harness.clone();
        self.window
            .update(|_, cx| harness.read(cx).other_input.read(cx).value().to_string())
    }

    /// Opens a second document next to the first, as another tab would be.
    fn open_second_document(&mut self, content: &str) -> Entity<CodeDocument> {
        let app_state = self.app_state.clone();
        let harness = self.harness.clone();
        let content = content.to_string();

        let document = self.window.update(|window, cx| {
            let document = cx.new(|cx| {
                let mut document = CodeDocument::new_with_language(
                    app_state,
                    None,
                    QueryLanguage::Lua,
                    window,
                    cx,
                );
                document.set_content(&content, window, cx);
                document
            });
            harness.update(cx, |harness, cx| {
                harness.second = Some(document.clone());
                cx.notify();
            });
            document
        });
        self.window.run_until_parked();

        document
    }

    fn close_second_document(&mut self) {
        let harness = self.harness.clone();
        self.window.update(|_, cx| {
            harness.update(cx, |harness, cx| {
                harness.second = None;
                cx.notify();
            });
        });
        self.window.run_until_parked();
    }

    fn text_of(&mut self, document: &Entity<CodeDocument>) -> String {
        let document = document.clone();
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

    fn mode_of(&mut self, document: &Entity<CodeDocument>) -> Option<VimMode> {
        let document = document.clone();
        self.window.update(|_, cx| document.read(cx).vim_mode())
    }

    fn keys(&mut self, keystrokes: &str) {
        self.window.simulate_keystrokes(keystrokes);
        self.window.run_until_parked();
    }

    fn type_text(&mut self, input: &str) {
        self.window.simulate_input(input);
        self.window.run_until_parked();
    }

    /// Delivers text the way an IME does: a marked composition, then a commit,
    /// both straight to the input handler without a key event.
    fn ime_compose(&mut self, marked: &str, committed: &str) {
        let document = self.document.clone();
        self.window.update(|window, cx| {
            let input = document.read(cx).editor.input_state.clone();
            input.update(cx, |state, cx| {
                state.replace_and_mark_text_in_range(None, marked, None, window, cx);
                state.replace_text_in_range(None, committed, window, cx);
            });
        });
        self.window.run_until_parked();
    }
}

#[gpui::test]
fn absolute_jumps_clamp_counts_and_preserve_buffer(cx: &mut TestAppContext) {
    for (content, last) in [("", 0), ("a\n", 2), ("a\r\n", 3), ("é\n  中\n z", 10)] {
        let mut editor = open_editor(cx, content, true);
        editor.keys("g g");
        assert_eq!(editor.cursor(), 0);
        editor.keys("shift-g");
        assert_eq!(editor.cursor(), last);
        editor.keys("0 g g");
        assert_eq!(editor.cursor(), 0);
        editor.keys("9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 g g");
        assert_eq!(editor.cursor(), last);
        editor.keys("1 shift-g");
        assert_eq!(editor.cursor(), 0);
        editor.keys("0 shift-g");
        assert_eq!(editor.cursor(), last);
        editor.keys("9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 shift-g");
        assert_eq!(editor.cursor(), last);
        assert_eq!(editor.text(), content);
    }
}

#[gpui::test]
fn pending_absolute_jump_is_discarded_at_dispatch_boundaries(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "alpha\nbeta\ngamma", true);
    editor.keys("shift-g g escape g");
    assert_eq!(editor.cursor(), 11);
    editor.keys("g");
    assert_eq!(editor.cursor(), 0);
    editor.keys("shift-g g z g");
    assert_eq!(editor.cursor(), 11);
    editor.keys("g");
    assert_eq!(editor.cursor(), 0);
    editor.keys("shift-g g ctrl-z g");
    assert_eq!(editor.cursor(), 11);
    editor.keys("g");
    assert_eq!(editor.cursor(), 0);
    editor.keys("shift-g g");
    editor.focus_other_input();
    editor.focus_document(&editor.document.clone());
    editor.keys("g");
    assert_eq!(editor.cursor(), 11);
    editor.keys("g");
    assert_eq!(editor.cursor(), 0);
    assert_eq!(editor.text(), "alpha\nbeta\ngamma");
    assert_eq!(editor.other_input_text(), "");
}

#[gpui::test]
fn absolute_line_motions_and_visual_payload(cx: &mut TestAppContext) {
    for mode_key in ["", "v", "shift-v", "ctrl-v"] {
        let mut editor = open_editor(cx, "  é\r\n  中\r\n  last", true);
        editor.keys("shift-g");
        assert_eq!(editor.cursor(), 15);
        editor.keys(mode_key);
        editor.keys("g g");
        assert_eq!(editor.motion_offset(), 2);
        match mode_key {
            "v" => {
                assert_eq!(editor.selection(), 2..16);
                assert_eq!(editor.selected_query().as_deref(), Some("é\r\n  中\r\n  l"));
                assert_eq!(editor.visual_caret(), Some(2));
            }
            "shift-v" => {
                assert_eq!(editor.selection(), 0..19);
                assert_eq!(
                    editor.selected_query().as_deref(),
                    Some("é\r\n  中\r\n  last")
                );
                assert_eq!(editor.visual_caret(), Some(2));
            }
            "ctrl-v" => {
                assert_eq!(editor.selection(), 2..4);
                assert_eq!(editor.selected_query().as_deref(), Some("é\n中\nl"));
                assert_eq!(editor.visual_caret(), None);
            }
            _ => assert_eq!(editor.selected_query(), None),
        }
        editor.keys("2 shift-g");
        assert_eq!(editor.motion_offset(), 8);
        match mode_key {
            "v" => {
                assert_eq!(editor.selection(), 8..16);
                assert_eq!(editor.selected_query().as_deref(), Some("中\r\n  l"));
                assert_eq!(editor.visual_caret(), Some(8));
            }
            "shift-v" => {
                assert_eq!(editor.selection(), 6..19);
                assert_eq!(editor.selected_query().as_deref(), Some("中\r\n  last"));
                assert_eq!(editor.visual_caret(), Some(8));
            }
            "ctrl-v" => {
                assert_eq!(editor.selection(), 8..11);
                assert_eq!(editor.selected_query().as_deref(), Some("中\nl"));
                assert_eq!(editor.visual_caret(), None);
            }
            _ => assert_eq!(editor.selected_query(), None),
        }
        editor.keys("escape");
    }
}

#[gpui::test]
fn pending_g_escape_exits_visual_and_resets_prefix(cx: &mut TestAppContext) {
    for mode_key in ["v", "shift-v", "ctrl-v"] {
        let mut editor = open_editor(cx, "first\nsecond\nlast", true);
        editor.keys("shift-g");
        editor.keys(mode_key);
        editor.keys("g escape");
        assert_eq!(editor.mode(), Some(VimMode::Normal));
        assert_eq!(editor.visual_caret(), None);
        editor.keys("g");
        assert_eq!(editor.cursor(), 13);
        editor.keys("g");
        assert_eq!(editor.cursor(), 0);
        assert_eq!(editor.text(), "first\nsecond\nlast");
    }
}

fn open_editor<'a>(cx: &'a mut TestAppContext, content: &str, vim_enabled: bool) -> Fixture<'a> {
    open_editor_with(
        cx,
        EditorSetup {
            content,
            vim_enabled,
            language: QueryLanguage::Lua,
            read_only: false,
        },
    )
}

struct EditorSetup<'a> {
    content: &'a str,
    vim_enabled: bool,
    language: QueryLanguage,
    read_only: bool,
}

fn open_editor_with<'a>(cx: &'a mut TestAppContext, setup: EditorSetup<'_>) -> Fixture<'a> {
    init_runtime(cx);

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let storage_runtime = StorageRuntime::in_memory().expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage_runtime).expect("test storage setup")
        })
    });

    cx.update(|cx| {
        app_state.update(cx, |state, _cx| {
            let mut settings = state.general_settings().clone();
            settings.vim_mode = setup.vim_enabled;
            state.update_general_settings(settings);
        });
    });

    let slot: FixtureSlot = Rc::new(RefCell::new(None));
    let slot_writer = slot.clone();
    let content = setup.content.to_string();
    let language = setup.language.clone();
    let read_only = setup.read_only;

    let (_, window) = cx.add_window_view(|window, cx| {
        let document = cx.new(|cx| {
            let mut document = CodeDocument::new_with_language(
                app_state.clone(),
                None,
                language.clone(),
                window,
                cx,
            );
            if read_only {
                document = document.with_read_only(cx);
            }
            document.set_content(&content, window, cx);
            document
        });

        let other_input = cx.new(|cx| InputState::new(window, cx));

        let harness = cx.new(|_cx| Harness {
            document: document.clone(),
            second: None,
            other_input,
            commands: Vec::new(),
            run_query_actions: 0,
        });

        slot_writer.replace(Some((document, harness.clone())));
        Root::new(harness, window, cx)
    });

    let (document, harness) = slot.borrow().clone().expect("editor fixture");

    window.update(|window, cx| {
        document.update(cx, |document, cx| document.focus(window, cx));
    });
    window.run_until_parked();

    Fixture {
        app_state,
        document,
        harness,
        window,
    }
}

#[gpui::test]
fn word_operators_cover_classes_directions_counts_and_undo(cx: &mut TestAppContext) {
    for (keys, expected) in [
        ("d w", ", bar"),
        ("d e", ", bar"),
        ("d shift-w", "bar"),
        ("d shift-e", " bar"),
    ] {
        let mut editor = open_editor(cx, "é_foo, bar", true);
        editor.keys(keys);
        assert_eq!(editor.text(), expected, "{keys}");
        assert_eq!(
            editor.clipboard_text().as_deref(),
            Some(&"é_foo, bar"[.."é_foo, bar".len() - expected.len()]),
            "{keys}"
        );
        editor.keys("u");
        assert_eq!(editor.text(), "é_foo, bar");
    }
    let mut editor = open_editor(cx, "one two three four five six seven", true);
    editor.keys("2 d 3 w");
    assert_eq!(editor.text(), "seven");
    editor.keys("u");
    assert_eq!(editor.text(), "one two three four five six seven");
    editor.set_cursor(8);
    editor.keys("d b");
    assert_eq!(editor.text(), "one three four five six seven");
    editor.keys("u");
    editor.set_cursor(8);
    editor.keys("y shift-b");
    assert_eq!(editor.clipboard_text().as_deref(), Some("two "));
    assert_eq!(editor.text(), "one two three four five six seven");
}

#[gpui::test]
fn word_operators_preserve_unicode_crlf_eof_and_readonly(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "é!\r\n中 x", true);
    editor.keys("d e");
    assert_eq!(editor.text(), "\r\n中 x");
    editor.keys("u");
    editor.set_cursor(2);
    editor.keys("d w");
    assert_eq!(editor.text(), "é中 x");
    editor.keys("u");
    editor.set_cursor(9);
    editor.keys("d w");
    assert_eq!(editor.text(), "é!\r\n中 ");
    editor.keys("u");
    assert_eq!(editor.text(), "é!\r\n中 x");

    let mut editor = open_editor_with(
        cx,
        EditorSetup {
            content: "one two",
            vim_enabled: true,
            language: QueryLanguage::Lua,
            read_only: true,
        },
    );
    editor.keys("d w");
    assert_eq!(editor.text(), "one two");
    editor.keys("y e");
    assert_eq!(editor.clipboard_text().as_deref(), Some("one"));
}

#[gpui::test]
fn terminal_word_delete_and_yank_cover_final_scalar(cx: &mut TestAppContext) {
    for motion in ["w", "shift-w"] {
        for (content, expected) in [
            ("abc", "abc"),
            ("ab中", "ab中"),
            ("abc   ", "abc   "),
            ("abc\r\n", "abc"),
        ] {
            let mut editor = open_editor(cx, content, true);
            editor.keys(&format!("y {motion}"));
            assert_eq!(editor.clipboard_text().as_deref(), Some(expected));
            assert_eq!(editor.text(), content);
            editor.keys(&format!("d {motion}"));
            assert_eq!(editor.clipboard_text().as_deref(), Some(expected));
            assert_eq!(editor.text(), &content[expected.len()..]);
            editor.keys("u");
            assert_eq!(editor.text(), content);
        }
        let mut editor = open_editor(cx, "abc", true);
        editor.keys(&format!("2 d 3 {motion}"));
        assert_eq!(editor.text(), "");
        editor.keys("u");
        assert_eq!(editor.text(), "abc");
    }
}

#[gpui::test]
fn terminal_word_end_delete_and_yank_exclude_lf_and_crlf(cx: &mut TestAppContext) {
    for terminator in ["\n", "\r\n"] {
        for final_glyph in ["c", "中"] {
            let content = format!("ab{final_glyph}{terminator}");
            let last = "ab".len();
            for motion in ["e", "shift-e"] {
                let mut editor = open_editor(cx, &content, true);
                editor.set_cursor(last);
                editor.keys(&format!("y {motion}"));
                assert_eq!(editor.clipboard_text().as_deref(), Some(final_glyph));
                editor.keys(&format!("d {motion}"));
                assert_eq!(editor.text(), format!("ab{terminator}"));
                assert_eq!(editor.clipboard_text().as_deref(), Some(final_glyph));
                editor.keys("u");
                assert_eq!(editor.text(), content);

                let mut editor = open_editor(cx, &content, true);
                editor.keys(&format!("d 2 {motion}"));
                assert_eq!(editor.text(), terminator);
                assert_eq!(
                    editor.clipboard_text().as_deref(),
                    Some(format!("ab{final_glyph}").as_str())
                );
                editor.keys("u");
                assert_eq!(editor.text(), content);
            }
        }
    }
}

#[gpui::test]
fn word_operator_interruptions_do_not_carry_or_insert(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "one two", true);
    editor.keys("d ctrl-s w");
    assert_eq!(editor.text(), "one two");
    assert_eq!(editor.cursor(), 4);
    editor.keys("y q w");
    assert_eq!(editor.text(), "one two");
    editor.keys("d");
    editor.focus_other_input();
    editor.focus_document(&editor.document.clone());
    editor.keys("w");
    assert_eq!(editor.text(), "one two");
}

#[gpui::test]
fn line_operators_preserve_crlf_unicode_counts_and_undo(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "é\r\n中\r\nlast", true);
    editor.keys("2 y y");
    assert_eq!(editor.clipboard_text().as_deref(), Some("é\r\n中\r\n"));
    assert_eq!(editor.text(), "é\r\n中\r\nlast");
    editor.keys("2 d d");
    assert_eq!(editor.text(), "last");
    assert_eq!(editor.cursor(), 0);
    editor.keys("u");
    assert_eq!(editor.text(), "é\r\n中\r\nlast");
    editor.set_cursor(4);
    editor.keys("2 d d");
    assert_eq!(editor.text(), "é");
}

#[gpui::test]
fn absolute_line_operators_respect_explicit_counts_and_interruptions(cx: &mut TestAppContext) {
    for separator in ["\n", "\r\n"] {
        let content = ["é", "二", "three", "four", "five", "six"].join(separator);
        let first_two = format!("é{separator}二{separator}");
        for (keys, cursor_row, target_row) in [
            ("d g g", 3, 0),
            ("d shift-g", 0, 5),
            ("1 d shift-g", 3, 0),
            ("2 d shift-g", 3, 1),
            ("2 d g g", 3, 1),
            ("2 d 2 g g", 0, 3),
            ("3 d 2 shift-g", 0, 5),
            ("d 2 g g", 3, 1),
            ("d 2 shift-g", 3, 1),
        ] {
            let mut editor = open_editor(cx, &content, true);
            let starts: Vec<_> = content
                .match_indices(separator)
                .map(|(i, _)| i + separator.len())
                .collect();
            let cursor = if cursor_row == 0 {
                0
            } else {
                starts[cursor_row - 1]
            };
            editor.set_cursor(cursor);
            let first = cursor_row.min(target_row);
            let last = cursor_row.max(target_row);
            let start = if first == 0 { 0 } else { starts[first - 1] };
            let end = if last == 5 {
                content.len()
            } else {
                starts[last]
            };
            editor.keys(&keys.replacen('d', "y", 1));
            assert_eq!(
                editor.clipboard_text().as_deref(),
                Some(&content[start..end]),
                "{keys}"
            );
            editor.keys(keys);
            assert_eq!(
                editor.text(),
                format!("{}{}", &content[..start], &content[end..]),
                "{keys}"
            );
            editor.keys("u");
            assert_eq!(editor.text(), content);
        }
        let mut editor = open_editor(cx, &content, true);
        editor.set_cursor(first_two.len());
        editor.keys("d g escape g");
        assert_eq!(editor.text(), content);
        editor.keys("d g ctrl-s g");
        assert_eq!(editor.text(), content);
    }
}

#[gpui::test]
fn absolute_operators_handle_empty_and_trailing_lines(cx: &mut TestAppContext) {
    for (content, cursor, keys, expected_text, expected_yank) in [
        ("", 0, "d g g", "", None),
        ("", 0, "y shift-g", "", None),
        ("a\n", 2, "d g g", "", Some("a\n")),
        ("a\r\n", 3, "d g g", "", Some("a\r\n")),
        ("a\n", 2, "y shift-g", "a\n", Some("\n")),
        ("a\r\n", 3, "y shift-g", "a\r\n", Some("\r\n")),
        ("a\n", 0, "d shift-g", "", Some("a\n")),
        ("a\r\n", 0, "d shift-g", "", Some("a\r\n")),
    ] {
        let mut editor = open_editor(cx, content, true);
        editor.set_cursor(cursor);
        editor.window.update(|_, cx| {
            cx.write_to_clipboard(ClipboardItem::new_string("sentinel".into()));
        });
        editor.keys(keys);
        assert_eq!(editor.text(), expected_text, "{content:?} {keys}");
        assert_eq!(
            editor.clipboard_text().as_deref(),
            expected_yank,
            "{content:?} {keys}"
        );
        if keys.starts_with('d') && expected_text != content {
            editor.keys("u");
            assert_eq!(editor.text(), content);
        }
    }
}

#[gpui::test]
fn absolute_operators_readonly_and_pending_y_g_interruptions(cx: &mut TestAppContext) {
    for keys in ["d g g", "d shift-g"] {
        let mut editor = open_editor_with(
            cx,
            EditorSetup {
                content: "alpha\nbeta",
                vim_enabled: true,
                language: QueryLanguage::Lua,
                read_only: true,
            },
        );
        editor.set_cursor(6);
        editor.window.update(|_, cx| {
            cx.write_to_clipboard(ClipboardItem::new_string("sentinel".into()));
        });
        editor.keys(keys);
        assert_eq!(editor.text(), "alpha\nbeta");
        assert_eq!(editor.clipboard_text().as_deref(), Some("sentinel"));
        editor.keys(&keys.replacen('d', "y", 1));
        assert_eq!(
            editor.clipboard_text().as_deref(),
            Some(if keys == "d g g" {
                "alpha\nbeta"
            } else {
                "beta"
            })
        );
    }

    for interruption in ["escape", "q", "ctrl-s"] {
        let mut editor = open_editor(cx, "alpha\nbeta", true);
        editor.set_cursor(6);
        editor.keys(&format!("y g {interruption}"));
        editor.keys("d shift-g");
        assert_eq!(editor.text(), "alpha", "{interruption}");
    }
    let mut editor = open_editor(cx, "alpha\nbeta", true);
    editor.set_cursor(6);
    editor.keys("y g");
    editor.focus_other_input();
    let document = editor.document.clone();
    editor.focus_document(&document);
    editor.keys("d shift-g");
    assert_eq!(editor.text(), "alpha");
}

#[gpui::test]
fn line_delete_at_eof_removes_preceding_separator(cx: &mut TestAppContext) {
    for (content, cursor, keys, expected, yank) in [
        ("a\nb", 2, "d d", "a", "b"),
        ("a\r\nb", 3, "d d", "a", "b"),
        ("a\n", 2, "d d", "a", ""),
        ("a\r\n", 3, "d d", "a", ""),
        ("a", 0, "d d", "", "a"),
        ("a\nb\nc", 2, "2 d d", "a", "b\nc"),
    ] {
        let mut editor = open_editor(cx, content, true);
        editor.set_cursor(cursor);
        editor.keys(keys);
        assert_eq!(editor.text(), expected, "{content:?}");
        if !yank.is_empty() {
            assert_eq!(editor.clipboard_text().as_deref(), Some(yank));
        }
        editor.keys("u");
        assert_eq!(editor.text(), content);
    }
}

#[gpui::test]
fn pending_operator_is_interrupted_and_readonly_yank_does_not_edit(cx: &mut TestAppContext) {
    let mut editor = open_editor_with(
        cx,
        EditorSetup {
            content: "alpha\nbeta",
            vim_enabled: true,
            language: QueryLanguage::Lua,
            read_only: true,
        },
    );
    editor.window.update(|_, cx| {
        cx.write_to_clipboard(ClipboardItem::new_string("sentinel".to_string()));
    });
    editor.keys("d d");
    assert_eq!(editor.clipboard_text().as_deref(), Some("sentinel"));
    assert_eq!(editor.text(), "alpha\nbeta");
    editor.keys("d q");
    assert_eq!(editor.text(), "alpha\nbeta");
    editor.keys("y y");
    assert_eq!(editor.clipboard_text().as_deref(), Some("alpha\n"));
    assert_eq!(editor.text(), "alpha\nbeta");
    editor.keys("d escape d");
    assert_eq!(editor.text(), "alpha\nbeta");
}

#[gpui::test]
fn horizontal_and_vertical_operators_yank_delete_and_undo(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "é中x\r\nlast", true);
    editor.keys("y 2 l");
    assert_eq!(editor.clipboard_text().as_deref(), Some("é中x"));
    editor.keys("d 2 l");
    assert_eq!(editor.text(), "\r\nlast");
    editor.keys("u");
    assert_eq!(editor.text(), "é中x\r\nlast");

    editor.set_cursor(5);
    editor.keys("d 2 h");
    assert_eq!(editor.text(), "x\r\nlast");
    editor.keys("u");
    assert_eq!(editor.text(), "é中x\r\nlast");

    editor.keys("y j");
    assert_eq!(editor.clipboard_text().as_deref(), Some("é中x\r\nlast"));
    editor.set_cursor(8);
    editor.keys("d k");
    assert_eq!(editor.text(), "");
    editor.keys("u");
    assert_eq!(editor.text(), "é中x\r\nlast");
}

#[gpui::test]
fn directional_operator_counts_multiply_and_clamp(cx: &mut TestAppContext) {
    let mut editor = open_editor(
        cx,
        "abcdefghi\nsecond\nthird\nfourth\nfifth\nsixth\nseventh",
        true,
    );
    editor.keys("2 d 3 l");
    assert_eq!(editor.clipboard_text().as_deref(), Some("abcdefg"));
    assert_eq!(
        editor.text(),
        "hi\nsecond\nthird\nfourth\nfifth\nsixth\nseventh"
    );
    editor.keys("u");
    editor.keys("2 y 3 j");
    assert_eq!(
        editor.clipboard_text().as_deref(),
        Some("abcdefghi\nsecond\nthird\nfourth\nfifth\nsixth\nseventh")
    );
    assert_eq!(
        editor.text(),
        "abcdefghi\nsecond\nthird\nfourth\nfifth\nsixth\nseventh"
    );
}

#[gpui::test]
fn vertical_operators_handle_edges_and_eof_separator(cx: &mut TestAppContext) {
    for (content, cursor, keys, yank, result) in [
        ("first\nlast", 0, "y k", "first\n", "first\nlast"),
        ("first\nlast", 6, "d j", "last", "first"),
        ("first\n", 6, "d j", "", "first"),
        ("first\n\nlast", 6, "y j", "\nlast", "first\n\nlast"),
        ("", 0, "d j", "", ""),
    ] {
        let mut editor = open_editor(cx, content, true);
        editor.set_cursor(cursor);
        editor.keys(keys);
        assert_eq!(
            editor.clipboard_text().as_deref(),
            (!yank.is_empty()).then_some(yank),
            "{content:?} {keys}"
        );
        assert_eq!(editor.text(), result, "{content:?} {keys}");
    }
}

#[gpui::test]
fn read_only_directional_delete_preserves_clipboard_but_yank_is_allowed(cx: &mut TestAppContext) {
    let mut editor = open_editor_with(
        cx,
        EditorSetup {
            content: "alpha\nbeta",
            vim_enabled: true,
            language: QueryLanguage::Lua,
            read_only: true,
        },
    );
    editor
        .window
        .update(|_, cx| cx.write_to_clipboard(ClipboardItem::new_string("sentinel".into())));
    editor.keys("d l d j");
    assert_eq!(editor.clipboard_text().as_deref(), Some("sentinel"));
    assert_eq!(editor.text(), "alpha\nbeta");
    editor.keys("y j");
    assert_eq!(editor.clipboard_text().as_deref(), Some("alpha\nbeta"));
    assert_eq!(editor.text(), "alpha\nbeta");
}

#[gpui::test]
fn interrupted_operator_does_not_capture_directional_key(cx: &mut TestAppContext) {
    for operator in ["d", "y"] {
        let mut editor = open_editor(cx, "alpha\nbeta", true);
        editor.keys(&format!("{operator} escape j"));
        assert_eq!(editor.text(), "alpha\nbeta");
        assert_eq!(editor.cursor(), 6);
        editor.keys("l");
        assert_eq!(editor.cursor(), 7);
    }
}

#[gpui::test]
fn empty_buffer_line_operators_do_not_mutate_or_panic(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "", true);
    editor.keys("d d");
    assert_eq!(editor.text(), "");
    editor.keys("y y");
    assert_eq!(editor.text(), "");
}

#[gpui::test]
fn yank_trailing_empty_line_copies_its_line_ending(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "alpha\n", true);
    editor.set_cursor(6);
    editor.window.update(|_, cx| {
        cx.write_to_clipboard(ClipboardItem::new_string("sentinel".to_string()));
    });
    editor.keys("y y");
    assert_eq!(editor.text(), "alpha\n");
    assert_ne!(editor.clipboard_text().as_deref(), Some("sentinel"));
    assert_eq!(editor.clipboard_text().as_deref(), Some("\n"));
}

#[gpui::test]
fn linewise_yank_at_eof_preserves_only_existing_separators(cx: &mut TestAppContext) {
    for (content, cursor, keys, expected) in [
        ("a\n", 2, "y y", Some("\n")),
        ("a\r\n", 3, "3 y y", Some("\r\n")),
        ("a\n", 2, "y j", Some("\n")),
        ("a\r\n", 3, "2 y j", Some("\r\n")),
        ("a\n", 2, "y k", Some("a\n")),
        ("a\r\n", 3, "2 y k", Some("a\r\n")),
        ("a\n", 0, "y j", Some("a\n")),
        ("a\r\n", 0, "y j", Some("a\r\n")),
        ("a", 0, "y y", Some("a")),
        ("", 0, "y y", None),
        ("", 0, "y j", None),
    ] {
        let mut editor = open_editor(cx, content, true);
        editor.set_cursor(cursor);
        editor.keys(keys);
        assert_eq!(
            editor.clipboard_text().as_deref(),
            expected,
            "{content:?} {keys}"
        );
        assert_eq!(editor.text(), content);
    }

    let mut editor = open_editor_with(
        cx,
        EditorSetup {
            content: "a\r\n",
            vim_enabled: true,
            language: QueryLanguage::Lua,
            read_only: true,
        },
    );
    editor.set_cursor(3);
    editor.keys("y j");
    assert_eq!(editor.clipboard_text().as_deref(), Some("\r\n"));
    assert_eq!(editor.text(), "a\r\n");
}

#[gpui::test]
fn interrupted_operators_do_not_capture_a_later_motion(cx: &mut TestAppContext) {
    for operator in ["d", "y"] {
        for interruption in ["escape", "ctrl-z", "ctrl-y", "tab"] {
            let mut editor = open_editor(cx, "alpha\nbeta", true);
            editor.keys(&format!("{operator} {interruption}"));
            editor.keys("d");
            assert_eq!(editor.text(), "alpha\nbeta", "{operator} {interruption}");
            editor.keys("q");
            assert_eq!(editor.text(), "alpha\nbeta", "{operator} {interruption}");
            assert_eq!(editor.cursor(), 0, "{operator} {interruption}");
            editor.keys("j");
            assert_eq!(editor.cursor(), 6, "{operator} {interruption}");
        }

        let mut editor = open_editor(cx, "alpha\nbeta", true);
        editor.keys(operator);
        editor.focus_other_input();
        let document = editor.document.clone();
        editor.focus_document(&document);
        editor.keys("d");
        assert_eq!(editor.text(), "alpha\nbeta", "{operator} focus");
        editor.keys("q");
        assert_eq!(editor.text(), "alpha\nbeta", "{operator} focus");
        assert_eq!(editor.cursor(), 0, "{operator} focus");
        editor.keys("j");
        assert_eq!(editor.cursor(), 6, "{operator} focus");
    }
}

#[gpui::test]
fn visual_run_query_executes_selected_sql_and_whitespace_falls_back_to_buffer(
    cx: &mut TestAppContext,
) {
    let mut editor = open_editor_with(
        cx,
        EditorSetup {
            content: "  SELECT 1;  \nSELECT 2;",
            vim_enabled: true,
            language: QueryLanguage::Sql,
            read_only: false,
        },
    );
    let driver = FakeDriver::new(DbKind::SQLite);
    let profile = ConnectionProfile::new(
        "test",
        DbConfig::SQLite {
            path: ":memory:".into(),
            connection_id: None,
        },
    );
    let connection = driver.connect_arc(&profile).expect("fake connection");
    let profile_id = profile.id;
    let app_state = editor.app_state.clone();
    let document = editor.document.clone();
    editor.window.update(|_, cx| {
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
        document.update(cx, |document, _| document.connection_id = Some(profile_id));
    });
    editor.keys("v 1 2 l");
    assert_eq!(editor.selected_query().as_deref(), Some("SELECT 1;"));
    editor.keys("ctrl-enter");
    editor.window.run_until_parked();
    editor.window.update(|window, _| window.refresh());
    editor.window.run_until_parked();
    assert_eq!(editor.run_query_actions(), 1);
    assert_eq!(
        editor
            .window
            .update(|_, cx| document.read(cx).execution.execution_history.len()),
        1,
        "query did not start"
    );
    assert_eq!(
        driver
            .stats()
            .executed_requests
            .iter()
            .map(|request| request.sql.as_str())
            .collect::<Vec<_>>(),
        vec!["SELECT 1;"],
        "execution error: {:?}",
        editor.window.update(|_, cx| document
            .read(cx)
            .execution
            .execution_history
            .last()
            .and_then(|record| record.error.clone()))
    );

    editor.keys("escape");
    editor.set_cursor(0);
    editor.keys("v l ctrl-enter");
    editor.window.run_until_parked();
    editor.window.update(|window, _| window.refresh());
    editor.window.run_until_parked();
    assert_eq!(editor.run_query_actions(), 2);
    assert_eq!(
        driver
            .stats()
            .executed_requests
            .iter()
            .map(|request| request.sql.as_str())
            .collect::<Vec<_>>(),
        vec!["SELECT 1;", "  SELECT 1;  \nSELECT 2;"]
    );
}

#[gpui::test]
fn visual_character_selection_tracks_reverse_unicode_and_escape(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "é中🎉x\r\nlast", true);
    editor.keys("v");
    assert_eq!(editor.mode(), Some(VimMode::Visual));
    assert_eq!(editor.selection(), 0..2);
    editor.keys("2 l");
    assert_eq!(editor.selection(), 0..9);
    assert_eq!(editor.selected_query().as_deref(), Some("é中🎉"));
    editor.keys("h h");
    assert_eq!(editor.selection(), 0..2);
    editor.keys("escape");
    assert_eq!(editor.mode(), Some(VimMode::Normal));
    assert_eq!(editor.selection(), 0..0);
    assert!(editor.editor_focused());
    assert_eq!(editor.text(), "é中🎉x\r\nlast");
}

#[gpui::test]
fn visual_caret_tracks_active_row_without_changing_selected_query(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "éx\r\nsecond\nlast", true);
    editor.keys("shift-v j");
    assert_eq!(editor.selection(), 0..12);
    assert_eq!(editor.cursor(), 12);
    assert_eq!(editor.visual_caret(), Some(5));
    assert_eq!(editor.selected_query().as_deref(), Some("éx\r\nsecond"));
    editor.keys("k");
    assert_eq!(editor.visual_caret(), Some(0));
    editor.keys("escape");
    assert_eq!(editor.visual_caret(), None);

    editor.set_cursor(5);
    editor.keys("v k");
    assert_eq!(editor.visual_caret(), Some(0));
    editor.keys("j");
    assert_eq!(editor.visual_caret(), Some(5));
    editor.keys("escape");
    assert_eq!(editor.visual_caret(), None);
}

#[gpui::test]
fn visual_block_selects_rows_and_executes_fragments(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abXX\na\nabYY", true);
    editor.set_cursor(1);
    editor.keys("ctrl-v j j 2 l");
    assert_eq!(editor.mode(), Some(VimMode::VisualBlock));
    assert_eq!(editor.selected_query().as_deref(), Some("bXX\nbYY"));
    editor.keys("escape");
    assert_eq!(editor.mode(), Some(VimMode::Normal));
    assert_eq!(editor.selected_query(), None);
    assert_eq!(editor.text(), "abXX\na\nabYY");
}

#[gpui::test]
fn visual_edit_uses_raw_reversed_character_bytes_and_undo(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "aé中z", true);
    editor.set_cursor(6);
    editor.keys("v h h");
    assert_eq!(editor.selected_query().as_deref(), Some("é中z"));
    editor.keys("d");
    assert_eq!(editor.clipboard_text().as_deref(), Some("é中z"));
    assert_eq!(editor.text(), "a");
    assert_eq!(editor.mode(), Some(VimMode::Normal));
    editor.keys("u");
    assert_eq!(editor.text(), "aé中z");
}

#[gpui::test]
fn visual_block_delete_is_disjoint_and_atomic(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abXX\na\nabYY", true);
    editor.set_cursor(1);
    editor.keys("ctrl-v j j 2 l");
    assert_eq!(editor.selected_query().as_deref(), Some("bXX\nbYY"));
    editor.keys("x");
    assert_eq!(editor.clipboard_text().as_deref(), Some("bXX\nbYY"));
    assert_eq!(editor.text(), "a\na\na");
    editor.keys("u");
    assert_eq!(editor.text(), "abXX\na\nabYY");
}

#[gpui::test]
fn empty_visual_operators_exit_without_replacing_clipboard(cx: &mut TestAppContext) {
    for entry in ["v", "shift-v", "ctrl-v"] {
        for operator in ["d", "x", "y"] {
            let mut editor = open_editor(cx, "", true);
            editor.window.update(|_, cx| {
                cx.write_to_clipboard(ClipboardItem::new_string("sentinel".into()))
            });
            editor.keys(&format!("{entry} {operator}"));
            assert_eq!(editor.mode(), Some(VimMode::Normal), "{entry} {operator}");
            assert_eq!(editor.text(), "");
            assert_eq!(editor.clipboard_text().as_deref(), Some("sentinel"));
            editor.keys("u");
            assert_eq!(editor.text(), "");
        }
    }
}

#[gpui::test]
fn read_only_visual_delete_keeps_selection_and_clipboard_but_yank_exits(cx: &mut TestAppContext) {
    for operator in ["d", "x"] {
        let mut editor = open_editor_with(
            cx,
            EditorSetup {
                content: "alpha",
                vim_enabled: true,
                language: QueryLanguage::Sql,
                read_only: true,
            },
        );
        editor
            .window
            .update(|_, cx| cx.write_to_clipboard(ClipboardItem::new_string("sentinel".into())));
        editor.keys(&format!("v l {operator}"));
        assert_eq!(editor.mode(), Some(VimMode::Visual));
        assert_eq!(editor.text(), "alpha");
        assert_eq!(editor.clipboard_text().as_deref(), Some("sentinel"));
        editor.keys("y");
        assert_eq!(editor.mode(), Some(VimMode::Normal));
        assert_eq!(editor.clipboard_text().as_deref(), Some("al"));
    }
}

#[gpui::test]
fn visual_line_eof_delete_preserves_yank_bytes(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "first\r\nlast", true);
    editor.set_cursor(7);
    editor.keys("shift-v d");
    assert_eq!(editor.clipboard_text().as_deref(), Some("last"));
    assert_eq!(editor.text(), "first");
    editor.keys("u");
    assert_eq!(editor.text(), "first\r\nlast");
}

#[gpui::test]
fn visual_block_nonzero_utf8_column_selects_matching_scalars(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "éx\nax", true);
    editor.set_cursor(2);
    editor.keys("ctrl-v j");
    assert_eq!(editor.selected_query().as_deref(), Some("x\nx"));
}

#[gpui::test]
fn visual_block_reverse_unicode_and_whitespace_fallback(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "é中x\néx\né中x", true);
    editor.set_cursor(11);
    editor.keys("ctrl-v k k");
    assert_eq!(editor.mode(), Some(VimMode::VisualBlock));
    assert_eq!(editor.selected_query().as_deref(), Some("é\né\né"));
    editor.keys("escape");
    assert_eq!(editor.selected_query(), None);

    let mut editor = open_editor(cx, "  \n  ", true);
    editor.keys("ctrl-v j");
    assert_eq!(editor.selected_query(), None);
}

#[gpui::test]
fn visual_block_includes_last_glyph_and_single_glyph_rows(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "x\nx\nx", true);
    editor.keys("ctrl-v j j");
    assert_eq!(editor.selected_query().as_deref(), Some("x\nx\nx"));
    editor.keys("escape");

    let mut editor = open_editor(cx, "abc\na\nabc", true);
    editor.set_cursor(2);
    editor.keys("ctrl-v j j");
    assert_eq!(editor.selected_query().as_deref(), Some("c\nc"));
}

#[gpui::test]
fn visual_line_selection_includes_terminators_and_reverses(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "first\r\nsecond\nlast", true);
    editor.keys("shift-v j");
    assert_eq!(editor.mode(), Some(VimMode::VisualLine));
    assert_eq!(editor.selection(), 0..14);
    assert_eq!(editor.selected_query().as_deref(), Some("first\r\nsecond"));
    editor.keys("j");
    assert_eq!(editor.selection(), 0..18);
    editor.keys("k k");
    assert_eq!(editor.selection(), 0..7);
    editor.keys("tab shift-tab escape");
    assert_eq!(editor.selection(), 0..0);
    assert_eq!(editor.mode(), Some(VimMode::Normal));
    assert!(editor.editor_focused());
}

#[gpui::test]
fn visual_reverse_word_motion_and_mode_switch_keep_anchor(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "one two three", true);
    editor.set_cursor(8);
    editor.keys("v b");
    assert_eq!(editor.selection(), 4..9);
    assert_eq!(editor.selected_query().as_deref(), Some("two t"));
    editor.keys("shift-v");
    assert_eq!(editor.selection(), 0..13);
    editor.keys("v");
    assert_eq!(editor.selection(), 4..9);
    editor.keys("v");
    assert_eq!(editor.selection(), 4..4);
    assert_eq!(editor.mode(), Some(VimMode::Normal));
}

#[gpui::test]
fn disabling_visual_clears_selection_without_changing_text_or_focus(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "  one  two  ", true);
    editor.keys("v l");
    assert_eq!(editor.selection(), 0..2);
    editor.set_vim(false);
    assert_eq!(editor.mode(), None);
    assert_eq!(editor.selection(), 1..1);
    assert_eq!(editor.selected_query(), None);
    assert_eq!(editor.text(), "  one  two  ");
    assert!(editor.editor_focused());
    editor.set_vim(true);
    assert_eq!(editor.mode(), Some(VimMode::Normal));
    assert_eq!(editor.selection(), 1..1);
}

#[gpui::test]
fn selected_query_trims_visual_and_non_visual_selections(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "  one  two  ", true);
    editor.keys("v 5 l");
    assert_eq!(editor.selected_query().as_deref(), Some("one"));
    editor.set_vim(false);
    editor.set_cursor(0);
    let document = editor.document.clone();
    editor.window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.editor.input_state.update(cx, |state, cx| {
                state.set_selected_range(0..7, cx);
            });
        });
    });
    assert_eq!(editor.selected_query().as_deref(), Some("one"));
    editor.set_cursor(0);
    assert_eq!(editor.selected_query(), None);
}

#[gpui::test]
fn insert_ctrl_v_pastes_while_normal_ctrl_v_enters_visual_block(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "ab", true);
    editor.keys("i");
    editor.window.update(|_, cx| {
        cx.write_to_clipboard(ClipboardItem::new_string("PASTED".to_string()));
    });
    editor.keys("ctrl-v");
    assert_eq!(editor.mode(), Some(VimMode::Insert));
    assert_eq!(editor.text(), "PASTEDab");
    editor.keys("escape ctrl-v");
    assert_eq!(editor.mode(), Some(VimMode::VisualBlock));
    assert_eq!(editor.text(), "PASTEDab");
}

#[gpui::test]
fn selected_query_joins_mouse_style_ranges_in_document_order(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "one\ntwo\nthree", true);
    let document = editor.document.clone();
    editor.window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.editor.input_state.update(cx, |state, cx| {
                state.set_columnar_selection(8, 0, cx);
                assert_eq!(state.selected_nonempty_ranges(), vec![0..1, 4..5, 8..9]);
            });
        });
    });
    assert_eq!(editor.selected_query().as_deref(), Some("o\nt\nt"));
}

#[gpui::test]
fn selected_query_uses_block_fragment_when_active_row_is_empty(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "ab\n\n", true);
    let document = editor.document.clone();
    editor.window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.editor.input_state.update(cx, |state, cx| {
                state.set_columnar_selection(1, 3, cx);
                assert_eq!(state.selected_nonempty_ranges(), vec![0..2]);
                assert!(state.selected_range().is_empty());
            });
        });
    });
    assert_eq!(editor.selected_query().as_deref(), Some("ab"));
}

#[gpui::test]
fn visual_selection_is_scoped_to_focused_document(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "one two", true);
    editor.keys("v l");
    let first_selection = editor.selection();
    editor.focus_other_input();
    editor.type_text("vjl");
    assert_eq!(editor.other_input_text(), "vjl");
    assert_eq!(editor.selection(), first_selection);
    editor.focus_document(&editor.document.clone());
    editor.keys("l");
    assert_eq!(editor.selection(), 0..3);
}

#[gpui::test]
fn visual_empty_and_read_only_keep_text_and_shortcuts(cx: &mut TestAppContext) {
    let mut editor = open_editor_with(
        cx,
        EditorSetup {
            content: "",
            vim_enabled: true,
            language: QueryLanguage::Sql,
            read_only: true,
        },
    );
    editor.keys("v h j k l");
    assert_eq!(editor.selection(), 0..0);
    assert_eq!(editor.selected_query(), None);
    editor.keys("ctrl-s ctrl-enter tab escape");
    assert!(editor.commands().contains(&Command::SaveQuery));
    assert_eq!(editor.run_query_actions(), 1);
    assert_eq!(editor.text(), "");
    assert!(editor.editor_focused());
}

#[gpui::test]
fn normal_mode_inserts_no_text_for_unbound_keys(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abc", true);
    assert_eq!(editor.mode(), Some(VimMode::Normal));

    editor.keys("b c d 1 2 9 0 ; , . / ? space shift-z");
    editor.type_text("é中🎉ñ");

    assert_eq!(editor.text(), "abc");
    assert!(editor.editor_focused());
}

#[gpui::test]
fn entry_positions_and_counted_word_motions(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "é_foo, bar\nlast", true);
    editor.keys("2 w");
    assert_eq!(editor.cursor(), 8);
    editor.keys("b");
    assert_eq!(editor.cursor(), 6);
    editor.keys("shift-w");
    assert_eq!(editor.cursor(), 8);
    editor.keys("shift-e");
    assert_eq!(editor.cursor(), 10);
    editor.keys("0 shift-a");
    assert_eq!(editor.mode(), Some(VimMode::Insert));
    assert_eq!(editor.cursor(), 11);
    editor.type_text("!");
    editor.keys("escape shift-i");
    assert_eq!(editor.cursor(), 0);
    editor.type_text("X");
    assert_eq!(editor.text(), "Xé_foo, bar!\nlast");
}

#[gpui::test]
fn insert_line_uses_first_nonblank_and_zero_uses_line_start(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "  \t\u{2003}éx\n \t\nlast", true);
    editor.keys("shift-i");
    assert_eq!(editor.cursor(), 6);
    editor.keys("escape 0");
    assert_eq!(editor.cursor(), 0);
    editor.keys("j shift-i");
    assert_eq!(editor.cursor(), 10, "blank lines insert at their start");
    editor.keys("escape 0 j 2 0 w");
    assert_eq!(editor.cursor(), 16, "zero remains part of a count prefix");
}

#[gpui::test]
fn interrupted_count_does_not_reach_next_motion(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abcdef", true);
    editor.keys("3 ctrl-s l");
    assert_eq!(editor.cursor(), 1);
    assert!(editor.commands().contains(&Command::SaveQuery));
    editor.keys("4 z l");
    assert_eq!(editor.cursor(), 2);
    assert_eq!(editor.text(), "abcdef");
}

#[gpui::test]
fn counted_delete_on_long_unicode_line_stops_before_newline(cx: &mut TestAppContext) {
    let line = "é".repeat(8_192);
    let mut editor = open_editor(cx, &format!("{line}\nnext"), true);
    editor.keys("8 1 9 2 x");
    assert_eq!(editor.text(), "\nnext");
    editor.keys("u");
    assert_eq!(editor.text(), format!("{line}\nnext"));
    editor.keys("x");
    assert_eq!(editor.text(), format!("{}\nnext", "é".repeat(8_191)));
}

#[gpui::test]
fn action_and_focus_boundaries_discard_pending_count(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abcdef", true);
    editor.keys("3 tab l");
    assert_eq!(editor.cursor(), 1);
    editor.keys("4 ctrl-z l");
    assert_eq!(editor.cursor(), 2);
    editor.keys("5");
    editor.focus_other_input();
    editor.focus_document(&editor.document.clone());
    editor.keys("l");
    assert_eq!(editor.cursor(), 3);
}

#[gpui::test]
fn pending_display_clears_on_focus_and_tab_action(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abcdef", true);
    let pending = |editor: &mut Fixture<'_>| {
        let document = editor.document.clone();
        editor
            .window
            .update(|_, cx| document.read(cx).vim.pending_keys.clone())
    };

    editor.keys("3");
    assert_eq!(pending(&mut editor), "3");
    editor.focus_document(&editor.document.clone());
    assert_eq!(pending(&mut editor), "");
    assert!(editor.editor_focused());

    editor.keys("4 tab");
    assert_eq!(pending(&mut editor), "");
    assert!(editor.editor_focused());
    editor.keys("l");
    assert_eq!(editor.cursor(), 1);
}

#[gpui::test]
fn counted_delete_and_undo_keep_normal_lock(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abcdef\nnext", true);
    editor.keys("2 x");
    assert_eq!(editor.text(), "cdef\nnext");
    editor.keys("x");
    assert_eq!(editor.text(), "def\nnext");
    editor.keys("2 u");
    assert_eq!(editor.text(), "abcdef\nnext");
    editor.type_text("z");
    assert_eq!(editor.text(), "abcdef\nnext");
}

#[gpui::test]
fn counted_undo_can_restore_from_an_empty_buffer(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "ab", true);
    editor.keys("x x");
    assert_eq!(editor.text(), "");
    editor.keys("2 u");
    assert_eq!(editor.text(), "ab");
    assert_eq!(editor.mode(), Some(VimMode::Normal));
}

#[gpui::test]
fn normal_mode_blocks_enter_tab_paste_and_deletion_keys(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abc\ndef", true);
    editor
        .window
        .write_to_clipboard(ClipboardItem::new_string("PASTED".to_string()));

    editor.keys("tab shift-tab ctrl-v backspace delete");
    assert_eq!(editor.mode(), Some(VimMode::VisualBlock));
    editor.keys("escape");

    // Context-menu paste dispatches the same action to the focused editor.
    let document = editor.document.clone();
    editor.window.update(|window, cx| {
        let focus_handle = document
            .read(cx)
            .editor
            .input_state
            .read(cx)
            .focus_handle(cx);
        focus_handle.dispatch_action(&Paste, window, cx);
    });
    editor.window.run_until_parked();

    assert_eq!(editor.text(), "abc\ndef");
    assert!(
        !editor.commands().contains(&Command::CycleFocusForward)
            && !editor.commands().contains(&Command::CycleFocusBackward),
        "Tab must not reach the workspace keymap in Normal mode: {:?}",
        editor.commands()
    );
    assert!(editor.editor_focused());

    editor.keys("enter");
    assert_eq!(editor.text(), "abc\ndef", "Enter must not insert a newline");
    assert_eq!(editor.cursor(), 4, "Enter moves down one line");
}

#[gpui::test]
fn normal_mode_drops_ime_composition_and_commit(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abc", true);

    editor.ime_compose("に", "日本");

    assert_eq!(editor.text(), "abc");
}

#[gpui::test]
fn insert_mode_accepts_text_like_a_plain_editor(
    cx: &mut TestAppContext,
    plain_cx: &mut TestAppContext,
) {
    fn drive(editor: &mut Fixture<'_>) -> String {
        editor.type_text("hé中🎉");
        editor.keys("enter");
        editor.type_text("x");
        editor.ime_compose("に", "日本");
        editor.text()
    }

    let with_vim = {
        let mut editor = open_editor(cx, "", true);
        editor.keys("i");
        assert_eq!(editor.mode(), Some(VimMode::Insert));
        drive(&mut editor)
    };

    let without_vim = {
        let mut editor = open_editor(plain_cx, "", false);
        drive(&mut editor)
    };

    assert_eq!(with_vim, without_vim);
    assert_eq!(with_vim, "hé中🎉\nx日本");
}

#[gpui::test]
fn motions_stay_inside_the_buffer_and_on_characters(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "ab\ncd\nef", true);

    editor.keys("l");
    assert_eq!(editor.cursor(), 1);
    editor.keys("l");
    assert_eq!(editor.cursor(), 1, "l stops on the last character");
    editor.keys("j");
    assert_eq!(editor.cursor(), 4);
    editor.keys("j j");
    assert_eq!(editor.cursor(), 7, "j stops on the last line");
    editor.keys("k");
    assert_eq!(editor.cursor(), 4);
    editor.keys("h h");
    assert_eq!(editor.cursor(), 3, "h stops at the line start");
    editor.keys("k k");
    assert_eq!(editor.cursor(), 0, "k stops on the first line");
    assert_eq!(editor.text(), "ab\ncd\nef");
}

#[gpui::test]
fn motions_on_an_empty_buffer_are_no_ops(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "", true);

    editor.keys("h j k l x enter");

    assert_eq!(editor.cursor(), 0);
    assert_eq!(editor.text(), "");
}

#[gpui::test]
fn motions_step_over_whole_unicode_characters(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "é中🎉x\nñ", true);

    editor.keys("l");
    assert_eq!(editor.cursor(), 2);
    editor.keys("l");
    assert_eq!(editor.cursor(), 5);
    editor.keys("l");
    assert_eq!(editor.cursor(), 9);
    editor.keys("l");
    assert_eq!(editor.cursor(), 9);
    editor.keys("h");
    assert_eq!(editor.cursor(), 5);
    editor.keys("j");
    assert_eq!(
        editor.cursor(),
        11,
        "j clamps to the last character of a shorter line"
    );
}

#[gpui::test]
fn enabling_clamps_a_cursor_past_the_line_end(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abc\r\nde", false);
    editor.set_cursor(3);

    editor.set_vim(true);
    assert_eq!(editor.cursor(), 2);

    editor.keys("l");
    assert_eq!(
        editor.cursor(),
        2,
        "l never lands on the \\r of a CRLF line"
    );
    editor.keys("j");
    assert_eq!(editor.cursor(), 6);
}

#[gpui::test]
fn x_deletes_one_character_per_press_and_u_undoes_each(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abc\n\nd", true);

    editor.keys("x");
    assert_eq!(editor.text(), "bc\n\nd");
    editor.keys("x");
    assert_eq!(editor.text(), "c\n\nd");

    editor.keys("u");
    assert_eq!(editor.text(), "bc\n\nd", "each x is one undo step");
    assert_eq!(editor.mode(), Some(VimMode::Normal));
    editor.keys("u");
    assert_eq!(editor.text(), "abc\n\nd");

    editor.keys("j x");
    assert_eq!(
        editor.text(),
        "abc\n\nd",
        "x on an empty line does not join lines"
    );

    editor.keys("k l l x");
    assert_eq!(editor.text(), "ab\n\nd");
    assert_eq!(
        editor.cursor(),
        1,
        "x on the last character leaves the cursor on the new last one"
    );

    editor.keys("z");
    assert_eq!(editor.text(), "ab\n\nd", "u restored the Normal-mode lock");
}

/// Records the component's grouping: continuous typing in one Insert session,
/// spaces included, is a single undo step, and each Insert session is its own.
#[gpui::test]
fn undo_groups_an_insert_session_as_the_component_does(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "", true);

    editor.keys("i");
    editor.type_text("one");
    editor.keys("space");
    editor.type_text("two");
    editor.keys("escape");
    assert_eq!(editor.text(), "one two");
    assert_eq!(editor.mode(), Some(VimMode::Normal));

    editor.keys("i");
    editor.type_text("X");
    editor.keys("escape");
    assert_eq!(editor.text(), "one twXo");

    editor.keys("u");
    assert_eq!(
        editor.text(),
        "one two",
        "the second session is its own step"
    );

    editor.keys("u");
    assert_eq!(editor.text(), "", "the first session undoes as one step");
    assert_eq!(editor.mode(), Some(VimMode::Normal));
}

#[gpui::test]
fn escape_leaves_insert_mode_and_keeps_focus(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abc", true);

    editor.keys("l l i");
    assert_eq!(editor.mode(), Some(VimMode::Insert));
    editor.type_text("z");
    assert_eq!(editor.text(), "abzc");

    editor.keys("escape");

    assert_eq!(editor.mode(), Some(VimMode::Normal));
    assert_eq!(
        editor.cursor(),
        2,
        "Esc steps back onto the last inserted character"
    );
    assert!(editor.editor_focused(), "Esc must keep focus on the editor");
    assert!(
        !editor.commands().contains(&Command::Cancel),
        "Esc from Insert must not reach the workspace keymap"
    );

    editor.keys("escape");
    assert_eq!(
        editor.commands(),
        vec![Command::Cancel],
        "Esc in Normal mode keeps its existing meaning"
    );
}

#[gpui::test]
fn escape_with_an_open_completion_menu_closes_it_and_stays_in_insert(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "sel", true);
    editor.keys("i");

    let document = editor.document.clone();
    editor.window.update(|_, cx| {
        let input = document.read(cx).editor.input_state.clone();
        input.update(cx, |state, cx| {
            state.present_completion_items(
                0,
                "sel",
                vec![lsp_types::CompletionItem {
                    label: "select".to_string(),
                    ..Default::default()
                }],
                cx,
            );
        });
    });
    editor.window.run_until_parked();

    let menu_open = |editor: &mut Fixture<'_>| {
        let document = editor.document.clone();
        editor.window.update(|_, cx| {
            document
                .read(cx)
                .editor
                .input_state
                .read(cx)
                .completion_menu_state()
                .open
        })
    };
    assert!(menu_open(&mut editor));

    editor.keys("escape");

    assert!(!menu_open(&mut editor), "Esc closes the completion menu");
    assert_eq!(editor.mode(), Some(VimMode::Insert));
    assert!(editor.editor_focused());

    editor.keys("escape");
    assert_eq!(editor.mode(), Some(VimMode::Normal));
}

#[gpui::test]
fn app_shortcuts_dispatch_the_same_in_every_mode(
    disabled_cx: &mut TestAppContext,
    normal_cx: &mut TestAppContext,
    insert_cx: &mut TestAppContext,
) {
    const SHORTCUTS: &str = "ctrl-h ctrl-j ctrl-k ctrl-l ctrl-s alt-h ctrl-enter ctrl-shift-s";

    let mut outcomes = Vec::new();

    for (setup, app_cx) in [
        ("disabled", disabled_cx),
        ("normal", normal_cx),
        ("insert", insert_cx),
    ] {
        let mut editor = open_editor(app_cx, "abc", setup != "disabled");
        if setup == "insert" {
            editor.keys("i");
        }

        editor.keys(SHORTCUTS);

        outcomes.push((
            setup,
            editor.commands(),
            editor.run_query_actions(),
            editor.text(),
        ));
    }

    let (_, reference_commands, reference_runs, _) = outcomes[0].clone();
    assert!(
        reference_commands.contains(&Command::SaveQuery) && reference_runs == 1,
        "the reference run must exercise the shortcuts: {reference_commands:?}"
    );

    for (setup, commands, runs, text) in &outcomes {
        assert_eq!(commands, &reference_commands, "{setup}");
        assert_eq!(*runs, reference_runs, "{setup}");
        assert_eq!(text, "abc", "{setup}");
    }
}

#[gpui::test]
fn disabled_vim_mode_leaves_typing_unchanged(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "", false);
    assert_eq!(editor.mode(), None);

    editor.type_text("hjklixu");
    editor.keys("enter tab");
    editor.type_text("é");
    editor.keys("escape");

    assert_eq!(editor.text(), "hjklixu\n  é");
    assert_eq!(editor.commands(), vec![Command::Cancel]);
}

#[gpui::test]
fn a_read_only_document_moves_but_never_edits(cx: &mut TestAppContext) {
    let mut editor = open_editor_with(
        cx,
        EditorSetup {
            content: "abc\ndef",
            vim_enabled: true,
            language: QueryLanguage::Sql,
            read_only: true,
        },
    );

    editor.keys("l j");
    assert_eq!(editor.cursor(), 5, "motions work in a read-only document");

    editor.keys("x u");
    assert_eq!(editor.text(), "abc\ndef", "x and u do nothing");

    editor.keys("i");
    editor.type_text("zz");
    assert_eq!(
        editor.text(),
        "abc\ndef",
        "Insert mode cannot edit a read-only document"
    );
}

#[gpui::test]
fn every_code_language_gets_the_same_modal_editing(
    sql_cx: &mut TestAppContext,
    lua_cx: &mut TestAppContext,
    python_cx: &mut TestAppContext,
    bash_cx: &mut TestAppContext,
    mongo_cx: &mut TestAppContext,
) {
    let languages = [
        (sql_cx, QueryLanguage::Sql),
        (lua_cx, QueryLanguage::Lua),
        (python_cx, QueryLanguage::Python),
        (bash_cx, QueryLanguage::Bash),
        (mongo_cx, QueryLanguage::MongoQuery),
    ];

    for (cx, language) in languages {
        let label = format!("{language:?}");
        let mut editor = open_editor_with(
            cx,
            EditorSetup {
                content: "ab\ncd",
                vim_enabled: true,
                language,
                read_only: false,
            },
        );

        editor.type_text("zz");
        editor.keys("enter tab shift-tab");
        assert!(
            editor.editor_focused(),
            "{label}: Tab keeps focus in the editor"
        );
        assert_eq!(
            editor.text(),
            "ab\ncd",
            "{label}: Normal mode inserts nothing"
        );
        assert_eq!(editor.cursor(), 3, "{label}: Enter moved down");

        editor.keys("x");
        assert_eq!(editor.text(), "ab\nd", "{label}: x deletes");

        editor.keys("i");
        editor.type_text("q");
        assert_eq!(editor.text(), "ab\nqd", "{label}: Insert mode types");

        // Languages with completion open the menu while typing; the first Esc
        // then closes it and the second leaves Insert mode.
        editor.keys("escape");
        if editor.mode() == Some(VimMode::Insert) {
            editor.keys("escape");
        }
        assert_eq!(editor.mode(), Some(VimMode::Normal), "{label}");
    }
}

#[gpui::test]
fn other_inputs_are_untouched_while_the_editor_is_in_normal_mode(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abc", true);
    let harness = editor.harness.clone();

    editor.window.update(|window, cx| {
        let other_input = harness.read(cx).other_input.clone();
        other_input.update(cx, |state, cx| state.focus(window, cx));
    });
    editor.window.run_until_parked();

    editor.type_text("hjklxui");

    let other_value = editor
        .window
        .update(|_, cx| harness.read(cx).other_input.read(cx).value().to_string());
    assert_eq!(other_value, "hjklxui");
    assert_eq!(editor.text(), "abc");
    assert_eq!(editor.mode(), Some(VimMode::Normal));
}

#[gpui::test]
fn programmatic_edits_still_apply_in_normal_mode(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abc", true);
    let document = editor.document.clone();

    editor.window.update(|window, cx| {
        let input = document.read(cx).editor.input_state.clone();
        input.update(cx, |state, cx| state.insert("Z", window, cx));
    });
    assert_eq!(
        editor.text(),
        "Zabc",
        "an inserted completion or snippet applies"
    );

    editor.window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.set_content("formatted", window, cx)
        });
    });
    assert_eq!(
        editor.text(),
        "formatted",
        "a whole-buffer replacement applies"
    );
    assert_eq!(editor.mode(), Some(VimMode::Normal));
}

#[gpui::test]
fn vim_mode_is_off_unless_the_setting_enables_it(
    default_cx: &mut TestAppContext,
    enabled_cx: &mut TestAppContext,
) {
    let mut editor = open_editor(default_cx, "", false);
    assert_eq!(editor.mode(), None);

    let mut editor = open_editor(enabled_cx, "", true);
    assert_eq!(
        editor.mode(),
        Some(VimMode::Normal),
        "a document opens in Normal mode"
    );
}

#[gpui::test]
fn vim_cursor_shape_follows_mode_and_disabled_setting(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abc", false);
    assert_eq!(editor.cursor_shape(), InputCursorShape::Bar);
    editor.set_vim(true);
    assert_eq!(editor.cursor_shape(), InputCursorShape::Block);
    editor.keys("i");
    assert_eq!(editor.cursor_shape(), InputCursorShape::Bar);
    editor.keys("escape");
    assert_eq!(editor.cursor_shape(), InputCursorShape::Block);
    editor.set_vim(false);
    assert_eq!(editor.cursor_shape(), InputCursorShape::Bar);
}

#[gpui::test]
fn changing_the_setting_applies_to_open_editors(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "", false);

    editor.type_text("a");
    assert_eq!(editor.text(), "a");

    editor.set_vim(true);
    assert_eq!(editor.mode(), Some(VimMode::Normal));
    editor.type_text("b");
    assert_eq!(
        editor.text(),
        "a",
        "turning Vim mode on starts in Normal mode"
    );

    editor.keys("i");
    assert_eq!(editor.mode(), Some(VimMode::Insert));

    editor.set_vim(false);
    assert_eq!(editor.mode(), None);
    editor.keys("escape");
    assert_eq!(
        editor.commands(),
        vec![Command::Cancel],
        "with Vim mode off, Esc reaches the workspace again"
    );
    editor.focus_document(&editor.document.clone());
    editor.type_text("c");
    assert_eq!(
        editor.text(),
        "ca",
        "turning Vim mode off drops the lock; enabling had moved the cursor onto the `a`"
    );

    editor.set_vim(true);
    assert_eq!(
        editor.mode(),
        Some(VimMode::Normal),
        "turning it back on starts in Normal mode again"
    );
}

#[gpui::test]
fn unrelated_app_state_changes_keep_the_current_mode(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "", true);
    editor.keys("i");

    editor.emit_unrelated_app_state_change();

    assert_eq!(editor.mode(), Some(VimMode::Insert));
}

#[gpui::test]
fn each_document_keeps_its_own_mode_across_focus_changes(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "first", true);
    let first = editor.document.clone();
    let second = editor.open_second_document("second");

    editor.keys("i");
    assert_eq!(editor.mode_of(&first), Some(VimMode::Insert));

    editor.focus_document(&second);
    assert_eq!(editor.mode_of(&second), Some(VimMode::Normal));
    editor.keys("x");
    editor.type_text("zz");
    assert_eq!(editor.text_of(&second), "econd");

    editor.focus_other_input();
    editor.type_text("q");
    assert_eq!(editor.other_input_text(), "q");

    editor.focus_document(&first);
    assert_eq!(
        editor.mode_of(&first),
        Some(VimMode::Insert),
        "switching away and back keeps the mode"
    );
    editor.type_text("y");
    assert_eq!(editor.text_of(&first), "yfirst");
    assert_eq!(editor.text_of(&second), "econd");
    assert_eq!(editor.mode_of(&second), Some(VimMode::Normal));
}

#[gpui::test]
fn closing_a_document_leaves_nothing_behind(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "first", true);
    let second = editor.open_second_document("second");
    editor.focus_document(&second);
    editor.keys("l j i escape x u");
    let released = second.downgrade();
    drop(second);

    editor.close_second_document();
    // Let the document's own edit debounces (auto-save, diagnostics) run out.
    editor
        .window
        .executor()
        .advance_clock(std::time::Duration::from_secs(60));
    editor.window.run_until_parked();

    assert!(
        released.upgrade().is_none(),
        "no Vim callback keeps a closed document alive"
    );

    editor.focus_other_input();
    editor.type_text("hjkl");
    assert_eq!(editor.other_input_text(), "hjkl");
}

#[gpui::test]
fn another_window_is_unaffected_by_a_normal_mode_editor(cx: &mut TestAppContext) {
    struct PlainInput {
        input: Entity<InputState>,
    }

    impl Render for PlainInput {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().size_full().child(GpuiInput::new(&self.input))
        }
    }

    let mut editor = open_editor(cx, "abc", true);

    let slot: Rc<RefCell<Option<Entity<InputState>>>> = Rc::new(RefCell::new(None));
    let slot_writer = slot.clone();
    let other_window = editor.window.add_window(|window, cx| {
        let input = cx.new(|cx| InputState::new(window, cx));
        slot_writer.replace(Some(input.clone()));
        let view = cx.new(|_cx| PlainInput { input });
        Root::new(view, window, cx)
    });
    let input = slot.borrow().clone().expect("plain input");

    other_window
        .update(&mut **editor.window, |_, window, cx| {
            input.update(cx, |state, cx| state.focus(window, cx));
        })
        .expect("focus the other window's input");

    TestAppContext::simulate_input(editor.window, other_window.into(), "hjklxu");

    let value = other_window
        .read_with(&**editor.window, |_, cx| input.read(cx).value().to_string())
        .expect("read the other window's input");
    assert_eq!(value, "hjklxu");
    assert_eq!(editor.text(), "abc");
    assert_eq!(editor.mode(), Some(VimMode::Normal));
}

#[gpui::test]
fn the_editor_undo_and_redo_shortcuts_keep_working_in_normal_mode(cx: &mut TestAppContext) {
    #[cfg(target_os = "macos")]
    const UNDO_REDO: (&str, &str) = ("cmd-z", "cmd-shift-z");
    #[cfg(not(target_os = "macos"))]
    const UNDO_REDO: (&str, &str) = ("ctrl-z", "ctrl-y");

    let mut editor = open_editor(cx, "abc", true);

    editor.keys("x");
    assert_eq!(editor.text(), "bc");

    editor.keys(UNDO_REDO.0);
    assert_eq!(editor.text(), "abc");

    editor.keys(UNDO_REDO.1);
    assert_eq!(editor.text(), "bc");

    editor.type_text("z");
    assert_eq!(editor.text(), "bc", "the lock is back after the shortcut");
    assert_eq!(editor.mode(), Some(VimMode::Normal));
}

#[gpui::test]
fn pending_command_tracks_raw_keys_and_clears_on_completion(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "alpha\nbeta\ngamma", true);
    let pending = |editor: &mut Fixture<'_>| {
        let document = editor.document.clone();
        editor
            .window
            .update(|_, cx| document.read(cx).vim.pending_keys.clone())
    };
    editor.keys("2 d 3");
    assert_eq!(pending(&mut editor), "2d3");
    editor.keys("j");
    assert_eq!(pending(&mut editor), "");
    editor.keys("4 g");
    assert_eq!(pending(&mut editor), "4g");
    editor.keys("g");
    assert_eq!(pending(&mut editor), "");
    editor.keys("g escape");
    assert_eq!(pending(&mut editor), "");
    editor.keys("2 ctrl-z");
    assert_eq!(pending(&mut editor), "");
}
