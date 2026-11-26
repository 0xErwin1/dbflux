//! The Insert snippet picker as the code editor hosts it: opening it with the
//! editor's language, and inserting the chosen snippet at the selection.

use super::*;
use crate::snippet_picker::{SnippetPicker, SnippetPickerEvent, snippet_language_id};
use dbflux_components::vim::VimBinding;
use dbflux_core::parse_snippet;
use dbflux_storage::SnippetRecord;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};

/// The open picker and the subscription that carries its choice back.
pub(super) struct OpenSnippetPicker {
    pub(super) picker: Entity<SnippetPicker>,
    _subscription: Subscription,
}

impl CodeDocument {
    /// Opens the picker over the editor. Returns whether the command was
    /// consumed: a read-only document takes no snippets, so it leaves the key
    /// to whatever binds it next.
    pub(super) fn open_snippet_picker(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if self.read_only {
            return false;
        }
        if self.snippet_picker.is_some() {
            return true;
        }

        let language = self.effective_query_language(cx);
        let language_id = snippet_language_id(&language);
        let listed = self.app_state.update(cx, |state, _| {
            state.snippets.snippets_for_language(&language_id)
        });
        let snippets = match listed {
            Ok(snippets) => snippets,
            Err(error) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Storage,
                        dbflux_i18n::t!("document.code.snippets.picker.load_error"),
                    )
                    .with_cause(error.to_string()),
                    cx,
                );
                return true;
            }
        };

        let language_name = language.display_name().to_string();
        let picker = cx.new(|cx| SnippetPicker::new(language_name, snippets, window, cx));
        let subscription = cx.subscribe_in(
            &picker,
            window,
            |this, _picker, event: &SnippetPickerEvent, window, cx| match event {
                SnippetPickerEvent::Chosen(snippet) => {
                    this.insert_picked_snippet(snippet, window, cx)
                }
                SnippetPickerEvent::Closed => this.close_snippet_picker(window, cx),
            },
        );
        self.snippet_picker = Some(OpenSnippetPicker {
            picker,
            _subscription: subscription,
        });
        cx.notify();
        true
    }

    /// Routes the modal keys to the open picker. The keymap runs Enter, Escape
    /// and the arrows in the picker's context as `RunCommand`s that reach the
    /// document through the workspace before any element listener of the
    /// picker, so the document hands them over itself. Returns whether the
    /// command was consumed.
    pub(super) fn dispatch_to_snippet_picker(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(open) = self.snippet_picker.as_ref() else {
            return false;
        };
        let picker = open.picker.clone();

        match cmd {
            Command::Execute => picker.update(cx, |picker, cx| picker.choose(window, cx)),
            Command::Cancel => picker.update(cx, |picker, cx| picker.close(cx)),
            Command::SelectNext => picker.update(cx, |picker, cx| picker.select_next(cx)),
            Command::SelectPrev => picker.update(cx, |picker, cx| picker.select_prev(cx)),
            // The editor behind the picker must not run while the picker owns
            // the keyboard.
            Command::RunQuery | Command::RunQueryInNewTab => {}
            _ => return false,
        }
        true
    }

    fn close_snippet_picker(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.snippet_picker.take().is_some() {
            self.focus(window, cx);
            cx.notify();
        }
    }

    /// Replaces the selection (or inserts at the caret) with `snippet`.
    fn insert_picked_snippet(
        &mut self,
        snippet: &SnippetRecord,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.close_snippet_picker(window, cx);

        let parsed = match parse_snippet(&snippet.body) {
            Ok(parsed) => parsed,
            Err(error) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Config,
                        dbflux_i18n::t!(
                            "document.code.snippets.picker.invalid_body",
                            prefix = snippet.prefix
                        ),
                    )
                    .with_cause(error.to_string()),
                    cx,
                );
                return;
            }
        };

        self.enter_vim_insert_for_snippet(window, cx);
        let range = self.editor.input_state.read(cx).selected_range();
        self.insert_snippet(&parsed, range, window, cx);
    }

    /// Vim Normal mode keeps the input read-only, so a snippet could not be
    /// written into it. Enter Insert mode the way the user would, by typing
    /// `i`; the session then lives and ends with Insert mode like any other.
    fn enter_vim_insert_for_snippet(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.read_only || self.vim_mode() != Some(VimMode::Normal) {
            return;
        }

        let Ok(keystroke) = Keystroke::parse("i") else {
            return;
        };
        let input = self.vim.input_id();
        let event = KeyDownEvent {
            keystroke,
            is_held: false,
            prefer_character_input: false,
        };
        VimBinding::key_down(self, input, &event, window, cx);
    }
}

#[cfg(test)]
impl CodeDocument {
    pub(crate) fn snippet_picker_open(&self) -> bool {
        self.snippet_picker.is_some()
    }

    pub(crate) fn snippet_picker_highlighted_name(&self, cx: &App) -> Option<String> {
        let open = self.snippet_picker.as_ref()?;
        open.picker.read(cx).highlighted_name()
    }

    pub(crate) fn editor_text_and_selection(&self, cx: &App) -> (String, std::ops::Range<usize>) {
        let state = self.editor.input_state.read(cx);
        (state.value().to_string(), state.selected_range())
    }
}
