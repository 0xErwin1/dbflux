//! The Insert snippet picker as the code editor hosts it: opening it with the
//! editor's language, and inserting the chosen snippet at the selection.

use super::*;
use crate::snippet_picker::{SnippetPicker, SnippetPickerEvent, snippet_language_id};
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

        let range = self.editor.input_state.read(cx).selected_range();
        self.insert_snippet(&parsed, range, window, cx);
    }
}
