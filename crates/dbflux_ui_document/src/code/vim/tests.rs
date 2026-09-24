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
use dbflux_components::theme;
use dbflux_core::QueryLanguage;
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::keymap::{default_keymap, key_chord_from_gpui};
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{
    AppContext as _, ClipboardItem, Context, Entity, EntityInputHandler, Focusable as _,
    InteractiveElement as _, IntoElement, KeyBinding, KeyDownEvent, ParentElement as _, Render,
    Styled as _, TestAppContext, VisualTestContext, Window, actions, div,
};
use gpui_component::Root;
use gpui_component::input::Paste;
use std::cell::RefCell;
use std::rc::Rc;

actions!(vim_mode_test, [HarnessRunQuery]);

struct Harness {
    document: Entity<CodeDocument>,
    commands: Vec<Command>,
    run_query_actions: usize,
}

impl Render for Harness {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .on_action(cx.listener(|this, _: &HarnessRunQuery, _window, _cx| {
                this.run_query_actions += 1;
            }))
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, _window, cx| {
                let context = this.document.read(cx).active_context(cx);
                let chord = key_chord_from_gpui(&event.keystroke);

                if let Some(command) = default_keymap().resolve(context, &chord) {
                    this.commands.push(command);
                }
            }))
            .child(self.document.clone())
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

    fn cursor(&mut self) -> usize {
        let document = self.document.clone();
        self.window
            .update(|_, cx| document.read(cx).editor.input_state.read(cx).cursor())
    }

    fn mode(&mut self) -> Option<VimMode> {
        let document = self.document.clone();
        self.window.update(|_, cx| document.read(cx).vim_mode())
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

    fn set_vim(&mut self, enabled: bool) {
        let document = self.document.clone();
        self.window.update(|_, cx| {
            document.update(cx, |document, cx| document.set_vim_enabled(enabled, cx));
        });
        self.window.run_until_parked();
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

fn open_editor<'a>(cx: &'a mut TestAppContext, content: &str, vim_enabled: bool) -> Fixture<'a> {
    init_runtime(cx);

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let storage_runtime = StorageRuntime::in_memory().expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage_runtime).expect("test storage setup")
        })
    });

    let slot: FixtureSlot = Rc::new(RefCell::new(None));
    let slot_writer = slot.clone();
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

        let harness = cx.new(|_cx| Harness {
            document: document.clone(),
            commands: Vec::new(),
            run_query_actions: 0,
        });

        slot_writer.replace(Some((document, harness.clone())));
        Root::new(harness, window, cx)
    });

    let (document, harness) = slot.borrow().clone().expect("editor fixture");

    window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.set_vim_enabled(vim_enabled, cx);
            document.focus(window, cx);
        });
    });
    window.run_until_parked();

    Fixture {
        document,
        harness,
        window,
    }
}

#[gpui::test]
fn normal_mode_inserts_no_text_for_unbound_keys(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abc", true);
    assert_eq!(editor.mode(), Some(VimMode::Normal));

    editor.keys("a b c d 1 2 9 0 ; , . / ? space shift-a shift-z");
    editor.type_text("é中🎉ñ");

    assert_eq!(editor.text(), "abc");
    assert!(editor.editor_focused());
}

#[gpui::test]
fn normal_mode_blocks_enter_tab_paste_and_deletion_keys(cx: &mut TestAppContext) {
    let mut editor = open_editor(cx, "abc\ndef", true);
    editor
        .window
        .write_to_clipboard(ClipboardItem::new_string("PASTED".to_string()));

    editor.keys("tab shift-tab ctrl-v backspace delete");

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

    editor.keys("a z");
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
