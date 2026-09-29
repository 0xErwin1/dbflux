use crate::actions::SaveEdit;
use crate::components::json_editor_view::{self, JsonEditorView};
use crate::icons::AppIcon;
use crate::modals::Modal;
use crate::vim::{VimBinding, VimHost};
use dbflux_core::keymap_types::{Command, ContextId};
use gpui::*;
use gpui_component::input::EditorState;

/// Sentinel value for `doc_index` when opening the modal in insert (new document) mode.
/// When the modal saves with this index, the handler should call `insert_document` instead
/// of `update_document`.
pub const DOC_INDEX_NEW: usize = usize::MAX;

/// Event emitted when the modal editor saves a document.
#[derive(Clone)]
pub struct DocumentPreviewSaveEvent {
    pub doc_index: usize,
    pub document_json: String,
}

/// Event emitted when the document preview modal is closed.
#[derive(Clone)]
pub struct DocumentPreviewClosedEvent;

/// Modal editor for viewing and editing full MongoDB documents.
pub struct DocumentPreviewModal {
    visible: bool,
    doc_index: usize,
    input: Entity<EditorState>,
    focus_handle: FocusHandle,
    validation_error: Option<String>,
    vim: VimBinding,
    /// Re-renders on every edit so the JSON status line follows the text.
    _input_observation: Subscription,
}

impl DocumentPreviewModal {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        // soft_wrap defaults to true in 0.6.1, so the old explicit builder is gone.
        let input = cx.new(|cx| {
            EditorState::new(window, cx)
                .language("json")
                .line_number(true)
        });
        let input_observation = cx.observe(&input, |_, _, cx| cx.notify());
        let vim = VimBinding::new(input.clone(), window, cx);

        let mut modal = Self {
            visible: false,
            doc_index: 0,
            input,
            focus_handle: cx.focus_handle(),
            validation_error: None,
            vim,
            _input_observation: input_observation,
        };

        let input = modal.vim.input_id();
        VimBinding::follow_setting(&mut modal, input, cx);
        modal
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(
        &mut self,
        doc_index: usize,
        document_json: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.doc_index = doc_index;
        self.visible = true;
        self.validation_error = None;

        let formatted = json_editor_view::format_json(&document_json).unwrap_or(document_json);

        self.input.update(cx, |state, cx| {
            state.set_value(&formatted, window, cx);
            state.focus(window, cx);
        });

        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        let was_visible = self.visible;
        self.visible = false;
        self.validation_error = None;

        if was_visible {
            cx.emit(DocumentPreviewClosedEvent);
        }

        cx.notify();
    }

    fn save(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let value = self.input.read(cx).value().to_string();

        if let Err(e) = json_editor_view::validate_json(&value, false) {
            self.validation_error = Some(e);
            cx.notify();
            return;
        }

        cx.emit(DocumentPreviewSaveEvent {
            doc_index: self.doc_index,
            document_json: value,
        });

        self.close(cx);
    }

    fn compact_json(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let value = self.input.read(cx).value().to_string();
        if let Some(compact) = json_editor_view::compact_json(&value) {
            self.input.update(cx, |state, cx| {
                state.set_value(&compact, window, cx);
            });
            self.validation_error = None;
        }
    }

    fn format(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let value = self.input.read(cx).value().to_string();
        if let Some(formatted) = json_editor_view::format_json(&value) {
            self.input.update(cx, |state, cx| {
                state.set_value(&formatted, window, cx);
            });
            self.validation_error = None;
        }
    }
}

impl VimHost for DocumentPreviewModal {
    fn vim(&self, input: EntityId) -> Option<&VimBinding> {
        self.vim.for_input(input)
    }

    fn vim_mut(&mut self, input: EntityId) -> Option<&mut VimBinding> {
        self.vim.for_input_mut(input)
    }

    /// `<leader> s` runs the dialog's own save, as its primary button does.
    fn vim_dialog_command(
        &mut self,
        _input: EntityId,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if command == Command::SaveQuery {
            self.save(window, cx);
        }
    }
}

impl EventEmitter<DocumentPreviewSaveEvent> for DocumentPreviewModal {}
impl EventEmitter<DocumentPreviewClosedEvent> for DocumentPreviewModal {}

/// Vim's listeners on the editor's container. In Insert mode the modal's
/// Escape (`Cancel`) leaves Insert mode instead of closing, and in Normal
/// mode Enter (`Execute`) moves down instead of reaching the modal.
fn vim_wrapper(element: Div, input: EntityId, cx: &mut Context<DocumentPreviewModal>) -> Div {
    let element = VimBinding::wire(element, input, cx);
    let element = VimBinding::capture_action::<crate::actions::Cancel, _>(element, input, cx);
    VimBinding::capture_action::<crate::actions::Execute, _>(element, input, cx)
}

impl Render for DocumentPreviewModal {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let entity = cx.entity().downgrade();

        let close = move |_window: &mut Window, cx: &mut App| {
            entity.update(cx, |this, cx| this.close(cx)).ok();
        };

        let editor = JsonEditorView::new(
            "doc-preview",
            &self.input,
            cx.listener(|this, _, window, cx| this.save(window, cx)),
            cx.listener(|this, _, _, cx| this.close(cx)),
        )
        .validation_error(self.validation_error.clone())
        .readonly(self.vim.locked(false))
        .below_editor(self.vim.render_indicator(cx))
        .min_editor_height(px(400.0))
        .show_format_buttons(
            cx.listener(|this, _, window, cx| this.format(window, cx)),
            cx.listener(|this, _, window, cx| this.compact_json(window, cx)),
        );

        Modal::new(dbflux_i18n::t!("modals.document_preview.title"))
            .id("document-preview-modal")
            .focus_handle(&self.focus_handle)
            .on_close(close)
            .key_context(ContextId::DocumentPreviewModal.as_gpui_context())
            .icon(AppIcon::Braces)
            .width(px(1000.0))
            .height(px(700.0))
            .top_offset(px(60.0))
            .block_scroll()
            .child(
                vim_wrapper(
                    self.vim
                        .leader_scope(div(), cx)
                        .flex_1()
                        .min_h_0()
                        .flex()
                        .flex_col()
                        .on_action(cx.listener(|this, _: &SaveEdit, window, cx| {
                            this.save(window, cx);
                        })),
                    self.vim.input_id(),
                    cx,
                )
                .child(editor.render(cx)),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn document_preview_keys_resolve_in_both_locales() {
        let keys = ["modals.document_preview.title"];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(!en.is_empty() && en != key, "en missing for {key}");
            assert!(!es.is_empty() && es != key, "es missing for {key}");
        }
    }

    #[test]
    fn document_preview_title_diverges_between_locales() {
        let en = dbflux_i18n::t!("modals.document_preview.title", locale = "en");
        let es = dbflux_i18n::t!("modals.document_preview.title", locale = "es");
        assert_ne!(en, es);
    }
}

#[cfg(test)]
mod keyboard_tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{DocumentPreviewModal, DocumentPreviewSaveEvent};
    use crate::actions::RunCommand;
    use gpui::{
        AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _,
        Render, SharedString, Styled as _, TestAppContext, Window, div,
    };

    /// Stands in for the document behind the dialog: records every keymap
    /// command that bubbles out of it and every saved document.
    struct Host {
        modal: Entity<DocumentPreviewModal>,
        commands: Vec<SharedString>,
        saved: Vec<String>,
    }

    impl Render for Host {
        fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .size_full()
                .on_action(cx.listener(|this, action: &RunCommand, _, _| {
                    this.commands.push(action.command.clone());
                }))
                .child(self.modal.clone())
        }
    }

    fn host(window: &mut Window, cx: &mut Context<Host>) -> Host {
        let modal = cx.new(|cx| DocumentPreviewModal::new(window, cx));
        cx.subscribe(&modal, |host, _, event: &DocumentPreviewSaveEvent, _| {
            host.saved.push(event.document_json.clone());
        })
        .detach();

        Host {
            modal,
            commands: Vec::new(),
            saved: Vec::new(),
        }
    }

    /// Opening the preview puts the keyboard in its text buffer: text typed
    /// through the window reaches the editor without a click first, and
    /// Escape still closes the dialog from there.
    #[gpui::test]
    fn typing_right_after_opening_reaches_the_editor(cx: &mut TestAppContext) {
        cx.update(|cx| {
            gpui_component::init(cx);
            cx.bind_keys([gpui::KeyBinding::new(
                "escape",
                crate::actions::Cancel,
                Some(dbflux_core::keymap_types::ContextId::DocumentPreviewModal.as_gpui_context()),
            )]);
        });

        let (host, window) = cx.add_window_view(host);
        let modal = window.update(|_, cx| host.read(cx).modal.clone());

        window.update(|window, cx| {
            modal.update(cx, |modal, cx| {
                modal.open(0, String::new(), window, cx);
            });
        });
        window.update(|window, _| window.refresh());
        window.run_until_parked();

        window.simulate_input("typed");
        window.run_until_parked();

        let value = window.update(|_, cx| modal.read(cx).input.read(cx).value().to_string());
        assert_eq!(value, "typed", "typing reaches the editor without a click");

        window.simulate_keystrokes("escape");

        assert!(
            !window.update(|_, cx| modal.read(cx).is_visible()),
            "Escape closes the preview while its buffer has focus"
        );
    }

    /// With Vim mode on, the first Escape leaves Insert mode and keeps the
    /// document open; the second closes it.
    #[gpui::test]
    fn vim_mode_escape_leaves_insert_before_closing(cx: &mut TestAppContext) {
        cx.update(|cx| {
            gpui_component::init(cx);
            crate::actions::record_last_keystroke(cx);
            crate::vim::set_vim_enabled(cx, true);
            let context =
                dbflux_core::keymap_types::ContextId::DocumentPreviewModal.as_gpui_context();
            cx.bind_keys([gpui::KeyBinding::new(
                "escape",
                crate::actions::Cancel,
                Some(context),
            )]);
        });

        let (host, window) = cx.add_window_view(host);
        let modal = window.update(|_, cx| host.read(cx).modal.clone());

        window.update(|window, cx| {
            modal.update(cx, |modal, cx| {
                modal.open(0, "{}".to_string(), window, cx);
            });
        });
        window.run_until_parked();

        window.simulate_keystrokes("a");
        window.simulate_input("1");
        window.simulate_keystrokes("escape");
        window.run_until_parked();
        assert_eq!(
            window.update(|_, cx| modal.read(cx).input.read(cx).value().to_string()),
            "{1}"
        );
        assert!(window.update(|_, cx| modal.read(cx).is_visible()));

        window.simulate_keystrokes("escape");
        window.run_until_parked();
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
    }

    /// `<leader> s` saves the document through the dialog's own save and
    /// closes it; nothing reaches the document behind the dialog.
    #[gpui::test]
    fn leader_s_saves_the_document(cx: &mut TestAppContext) {
        cx.update(|cx| {
            gpui_component::init(cx);
            crate::actions::record_last_keystroke(cx);
            crate::vim::set_vim_enabled(cx, true);
            let leader = dbflux_core::keymap_types::ContextId::VimNormal.default_predicate();
            cx.bind_keys([gpui::KeyBinding::new(
                "space s",
                crate::vim::LeaderCommand::new("save_query"),
                Some(leader),
            )]);
        });

        let (host, window) = cx.add_window_view(host);
        let modal = window.update(|_, cx| host.read(cx).modal.clone());

        window.update(|window, cx| {
            modal.update(cx, |modal, cx| {
                modal.open(0, "{}".to_string(), window, cx);
                modal.input.update(cx, |state, cx| state.focus(window, cx));
            });
        });
        window.run_until_parked();

        window.simulate_keystrokes("space s");
        window.run_until_parked();

        assert_eq!(
            window.update(|_, cx| host.read(cx).saved.clone()),
            vec!["{}".to_string()]
        );
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
        assert!(
            window.update(|_, cx| host.read(cx).commands.is_empty()),
            "Save never reaches the document behind the dialog"
        );
    }
}
