//! Inline text editor for the preview pane.
//!
//! Text-like objects the preview gate allows are decoded into an editable
//! buffer (`ObjectEditor`) instead of falling back to download/open-externally.
//! The buffer is a standalone `InputState` in code-editor mode — the same
//! component `CodeDocument` uses — with the loaded content kept as a baseline
//! so "modified" is a plain comparison rather than a change counter.
//!
//! Decoding, the highlighter gate, buffer construction and the save audit
//! record live in `crate::object_text`, shared with the standalone editor tab
//! (`crate::object_editor`) so the two surfaces read and write an object the
//! same way.
//!
//! Saving writes the buffer back with `put_object`, preserving the object's
//! content type and its original line-ending convention. Anything that would
//! move away from a dirty buffer — selecting another object, navigating to
//! another prefix, closing the preview — routes through
//! `guard_navigation`, which parks the request behind a Save / Discard /
//! Cancel confirmation. Edits are never dropped silently.

use super::preview_content::{EncodingChoice, PreviewContentState, TextSource, decode_label};
use super::{ObjectBrowserDocument, ObjectBrowserFocusMode};
use crate::handle::DocumentEvent;
use crate::object_text::{
    FIND_SHORTCUT_HINT, LineEnding, SAVE_SHORTCUT_HINT, TextBody, body_meta_line, build_text_input,
    cursor_label, db_error_to_user_facing, keycap_text, open_find_panel, record_save_audit,
};
// The raw `GpuiInput` (not the app's single-line `Input` wrapper) is what
// `CodeDocument` renders its editor with: only it supports the full-height,
// line-numbered code-editor layout.
use dbflux_app::keymap::Modifiers;
use dbflux_components::controls::Button;
use dbflux_components::controls::{GpuiInput, InputEvent, ReadOnlyEditor};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::{Modal, ModalFocus};
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{Heights, Radii, Spacing};
use dbflux_components::vim::{VimBinding, VimHost};
use dbflux_core::DbError;
use dbflux_ui_base::keymap::modifiers_from_gpui;
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::EditorState;

/// Diameter of the dirty indicator inside the "modified" pill.
const DIRTY_DOT: Pixels = px(7.0);

/// A decoded text body ready to be installed into an editor, handed from the
/// background fetch to the next render — building the `InputState` and seeding
/// its value both need a `Window`, which the fetch continuation does not have.
pub(super) struct PendingTextBody {
    pub(super) key: String,
    pub(super) body: TextBody,
    pub(super) content_type: Option<String>,
    /// What produced this text — the object's raw bytes, or a value decoded
    /// from them. Drives whether the installed buffer is editable.
    pub(super) source: TextSource,
}

impl VimHost for ObjectBrowserDocument {
    fn vim(&self, input: EntityId) -> Option<&VimBinding> {
        self.editor
            .as_ref()
            .and_then(|editor| editor.vim.for_input(input))
    }

    fn vim_mut(&mut self, input: EntityId) -> Option<&mut VimBinding> {
        self.editor
            .as_mut()
            .and_then(|editor| editor.vim.for_input_mut(input))
    }

    /// A decoded view renders through `ReadOnlyEditor`, so typing never
    /// changes it; Vim's own edits (`x`, `dd`, `c`) must not either. An
    /// object's own text takes every Vim edit.
    fn vim_read_only(&self, _input: EntityId, _cx: &App) -> bool {
        self.editor
            .as_ref()
            .is_none_or(|editor| !editor.is_editable())
    }

    fn vim_accepts_focus(&self, _input: EntityId) -> bool {
        self.focus_mode == ObjectBrowserFocusMode::Editor
    }
}

/// The editable buffer for one object.
pub(super) struct ObjectEditor {
    pub(super) key: String,
    pub(super) input: Entity<EditorState>,
    /// Content as last loaded or last saved. `dirty` is `buffer != baseline`.
    pub(super) baseline: String,
    pub(super) line_ending: LineEnding,
    pub(super) content_type: Option<String>,
    pub(super) byte_len: u64,
    pub(super) dirty: bool,
    pub(super) saving: bool,
    /// What produced the buffer's text. Only `TextSource::Raw` may be saved
    /// back — a decoded view never writes its re-encoded form over the
    /// object's real bytes.
    pub(super) source: TextSource,
    /// Vim mode for this buffer; a new buffer gets a new binding.
    pub(super) vim: VimBinding,
    _subscription: Subscription,
}

impl ObjectEditor {
    /// Meta line under the header: what the object is, how big it is, and how
    /// its text is encoded.
    pub(super) fn meta_line(&self) -> String {
        body_meta_line(
            self.content_type.as_deref(),
            self.byte_len,
            self.line_ending,
        )
    }

    /// "gzip → JSON"-style label for a decoded buffer, or `None` for the
    /// object's own raw text.
    pub(super) fn decode_label(&self) -> Option<String> {
        decode_label(&self.baseline, self.source)
    }

    pub(super) fn is_editable(&self) -> bool {
        self.source.is_editable()
    }
}

/// Keyboard focus of the unsaved-edits confirmation: the dialog, which traps
/// Tab among its buttons, and the buttons themselves. The dialog opens with
/// Save focused, so Enter saves unless the user moved to another button.
pub(super) struct UnsavedConfirmFocus {
    modal: ModalFocus,
    pub(super) save: FocusHandle,
    pub(super) discard: FocusHandle,
    pub(super) cancel: FocusHandle,
    open_requested: bool,
}

impl UnsavedConfirmFocus {
    pub(super) fn new(cx: &mut App) -> Self {
        Self {
            modal: ModalFocus::new(cx),
            save: cx.focus_handle(),
            discard: cx.focus_handle(),
            cancel: cx.focus_handle(),
            open_requested: false,
        }
    }

    /// Moves focus to Save on the dialog's next render. The guard parks a
    /// navigation without a window at hand.
    fn request_open(&mut self) {
        self.open_requested = true;
    }

    /// Applies [`Self::request_open`]; call it while rendering the dialog.
    pub(super) fn apply_pending(&mut self, window: &mut Window, cx: &mut App) {
        if std::mem::take(&mut self.open_requested) {
            self.modal.focus(Some(&self.save), window, cx);
        }
    }

    /// Gives focus back to what had it before the dialog opened.
    pub(super) fn restore(&mut self, cx: &mut App) {
        self.open_requested = false;
        self.modal.restore(cx);
    }

    pub(super) fn modal_handle(&self) -> &FocusHandle {
        self.modal.handle()
    }
}

/// A navigation request parked behind the unsaved-edits confirmation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum GuardedNavigation {
    OpenPreview(String),
    NavigateToPrefix(String),
    ClosePreview,
    /// Deleting `key` while its editor is open and dirty — always a
    /// navigate-away, even when it is the same object being edited.
    DeleteObject(String),
    /// Renaming `key` while its editor is open and dirty — same rationale as
    /// `DeleteObject`: the key is about to change under the open buffer.
    RenameObject(String),
    /// Switching the encoding override while the current (raw) buffer is
    /// dirty — the switch replaces the buffer's content with a fresh
    /// resolve, exactly like navigating away from it.
    SetEncodingOverride(Option<EncodingChoice>),
}

impl GuardedNavigation {
    fn description(&self) -> String {
        match self {
            GuardedNavigation::OpenPreview(key) => dbflux_i18n::t!(
                "document.object_browser.editor.nav.open",
                key = key.as_str()
            ),
            GuardedNavigation::NavigateToPrefix(prefix) if prefix.is_empty() => {
                dbflux_i18n::t!("document.object_browser.editor.nav.leave_bucket_root")
            }
            GuardedNavigation::NavigateToPrefix(prefix) => dbflux_i18n::t!(
                "document.object_browser.editor.nav.leave_for",
                prefix = prefix.as_str()
            ),
            GuardedNavigation::ClosePreview => {
                dbflux_i18n::t!("document.object_browser.editor.nav.close_preview")
            }
            GuardedNavigation::DeleteObject(key) => dbflux_i18n::t!(
                "document.object_browser.editor.nav.delete",
                key = key.as_str()
            ),
            GuardedNavigation::RenameObject(key) => dbflux_i18n::t!(
                "document.object_browser.editor.nav.rename",
                key = key.as_str()
            ),
            GuardedNavigation::SetEncodingOverride(_) => {
                dbflux_i18n::t!("document.object_browser.editor.nav.reinterpret")
            }
        }
    }
}

impl ObjectBrowserDocument {
    // -- Buffer lifecycle ----------------------------------------------------

    pub(super) fn editor_for(&self, key: &str) -> Option<&ObjectEditor> {
        self.editor.as_ref().filter(|editor| editor.key == key)
    }

    /// Whether the buffer differs from the content last loaded or saved.
    pub(super) fn editor_is_dirty(&self) -> bool {
        self.editor.as_ref().is_some_and(|editor| editor.dirty)
    }

    /// Short summary of the pending edit for the tab's dirty-dot tooltip and
    /// the workspace's unsaved-changes modal.
    pub fn change_summary(&self) -> Option<String> {
        let editor = self.editor.as_ref()?;

        editor.dirty.then(|| {
            dbflux_i18n::t!(
                "document.object_browser.editor.unsaved_summary",
                key = editor.key.as_str()
            )
        })
    }

    /// Builds the buffer for a freshly fetched body. Called from `render`,
    /// where a `Window` is available.
    pub(super) fn install_text_editor(
        &mut self,
        pending: PendingTextBody,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.preview_key.as_deref() != Some(pending.key.as_str()) {
            return;
        }

        let input = build_text_input(&pending.key, &pending.body.text, window, cx);
        let vim = VimBinding::new(input.clone(), window, cx);

        let subscription = cx.subscribe_in(
            &input,
            window,
            |this, input, event: &InputEvent, _window, cx| {
                if !matches!(event, InputEvent::Change) {
                    return;
                }

                let value = input.read(cx).value().to_string();

                if let Some(editor) = this.editor.as_mut() {
                    let dirty = value != editor.baseline;

                    if editor.dirty != dirty {
                        editor.dirty = dirty;
                        cx.notify();
                    }
                }
            },
        );

        self.editor = Some(ObjectEditor {
            key: pending.key.clone(),
            input: input.clone(),
            baseline: pending.body.text.clone(),
            line_ending: pending.body.line_ending,
            content_type: pending.content_type,
            byte_len: pending.body.byte_len,
            dirty: false,
            saving: false,
            source: pending.source,
            vim,
            _subscription: subscription,
        });

        input.update(cx, |state, cx| {
            state.set_value(&pending.body.text, window, cx);
        });

        let input_id = input.entity_id();
        VimBinding::follow_setting(self, input_id, cx);

        self.preview_content = PreviewContentState::Text;
        cx.notify();
    }

    /// Drops the buffer without touching the object. Used when the preview
    /// moves to another object and there is nothing to preserve.
    pub(super) fn drop_editor(&mut self) {
        self.editor = None;
    }

    /// Restores the buffer to the content last loaded or saved.
    pub(super) fn discard_object_edits(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(editor) = self.editor.as_ref() else {
            return;
        };

        let baseline = editor.baseline.clone();
        let input = editor.input.clone();

        input.update(cx, |state, cx| {
            state.set_value(&baseline, window, cx);
        });

        if let Some(editor) = self.editor.as_mut() {
            editor.dirty = false;
        }

        cx.notify();
    }

    pub(super) fn focus_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(editor) = self.editor.as_ref() else {
            return;
        };

        self.focus_mode = ObjectBrowserFocusMode::Editor;
        editor
            .input
            .clone()
            .update(cx, |state, cx| state.focus(window, cx));
        cx.notify();
    }

    // -- Save ----------------------------------------------------------------

    /// Saves as part of an interrupted close: the tab closes only once the
    /// `put_object` lands, and keeps its changes otherwise.
    ///
    /// Repeating the request while a save is in flight is intentional: that
    /// save then reports to the close the second request asked for.
    pub(super) fn save_for_close(&mut self, cx: &mut Context<Self>) -> bool {
        let Some(editor) = self.editor.as_ref() else {
            return false;
        };

        if !editor.is_editable() {
            // The same refusal `save_object_edits` reports: no write can start,
            // so there is nothing to arm.
            return false;
        }

        self.close_after_save = true;
        self.save_object_edits(cx);
        true
    }

    /// Reports a finished save to the workspace.
    ///
    /// Only a save the interrupted-close flow started, and only one that
    /// actually landed, asks for the tab to close; every other outcome drops
    /// that intent so a later manual save cannot close a tab the user kept.
    fn report_save_outcome(&mut self, succeeded: bool, cx: &mut Context<Self>) {
        let close_after_save = std::mem::take(&mut self.close_after_save);

        cx.emit(DocumentEvent::SaveFinished { succeeded });

        if succeeded && close_after_save {
            cx.emit(DocumentEvent::RequestClose);
        }
    }

    /// Writes the buffer back to the object with `put_object`, preserving the
    /// detected content type and line-ending convention.
    pub(super) fn save_object_edits(&mut self, cx: &mut Context<Self>) {
        if self.editor.is_none() {
            return;
        }

        if self.editor.as_ref().is_some_and(|editor| editor.saving) {
            // The save already in flight reports its own outcome.
            return;
        }

        // A decoded view is never the object's real bytes — writing it back
        // would silently replace the object's actual content with a
        // re-encoding of its decoded form. The footer never offers Save for
        // this state, but the guard stays here too since it is reachable
        // from the Ctrl/Cmd+S shortcut regardless of what is rendered.
        if !self
            .editor
            .as_ref()
            .is_some_and(|editor| editor.is_editable())
        {
            // A refused save must report now, or a tab close waiting on it
            // never resolves. The parked navigation goes with it: the prompt it
            // waits on cannot be resolved by a save that will never start.
            self.pending_navigation = None;
            self.unsaved_confirm_focus.restore(cx);
            self.report_save_outcome(false, cx);
            return;
        }

        let Some(editor) = self.editor.as_ref() else {
            return;
        };

        let key = editor.key.clone();
        let content_type = editor.content_type.clone();
        let text = editor.input.read(cx).value().to_string();
        let bytes = editor.line_ending.apply(&text).into_bytes();
        let byte_len = bytes.len() as u64;

        let Some(connection) = self.get_connection(cx) else {
            self.pending_navigation = None;
            self.unsaved_confirm_focus.restore(cx);
            report_error(
                UserFacingError::new(
                    ErrorKind::Driver,
                    dbflux_i18n::t!("document.object_browser.error.connection_unavailable"),
                ),
                cx,
            );
            self.report_save_outcome(false, cx);
            return;
        };

        if let Some(editor) = self.editor.as_mut() {
            editor.saving = true;
        }
        cx.notify();

        let audit_service = self.app_state.read(cx).audit_service().clone();
        let entity = cx.entity().clone();
        let bucket = self.bucket.clone();
        let profile_id = self.profile_id;
        let key_for_task = key.clone();
        let bucket_for_task = bucket.clone();

        let task = cx.background_executor().spawn(async move {
            let started = std::time::Instant::now();

            let result = match connection.object_store_api() {
                Some(api) => api.put_object(
                    &bucket_for_task,
                    &key_for_task,
                    bytes,
                    content_type.as_deref(),
                ),
                None => Err(DbError::NotSupported(dbflux_i18n::t!(
                    "document.object_browser.error.api_unavailable"
                ))),
            };

            (result, started.elapsed().as_millis())
        });

        cx.spawn(async move |_this, cx| {
            let (result, elapsed_millis) = task.await;

            record_save_audit(
                &audit_service,
                profile_id,
                &bucket,
                &key,
                result.as_ref().err().map(|err| err.to_string()).as_deref(),
            );

            if let Err(ref err) = result {
                report_error_async(db_error_to_user_facing(err), cx);
            }

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.apply_save_outcome(key, text, byte_len, result.is_ok(), elapsed_millis, cx);
                });
            });
        })
        .detach();
    }

    fn apply_save_outcome(
        &mut self,
        key: String,
        saved_text: String,
        byte_len: u64,
        succeeded: bool,
        elapsed_millis: u128,
        cx: &mut Context<Self>,
    ) {
        self.last_operation = Some(crate::buckets_table::OperationTiming {
            label: "PutObject",
            millis: elapsed_millis,
        });

        // No buffer, or an outcome that belongs to an object the buffer no
        // longer shows: the waiting tab keeps its changes rather than closing
        // on a stale save.
        if self.editor.as_ref().is_none_or(|editor| editor.key != key) {
            self.report_save_outcome(false, cx);
            return;
        }

        if let Some(editor) = self.editor.as_mut() {
            editor.saving = false;
        }

        if !succeeded {
            // The failure was already reported; the buffer stays dirty so the
            // user can retry, and any parked navigation is dropped rather than
            // silently carrying the unsaved edits away.
            self.pending_navigation = None;
            self.unsaved_confirm_focus.restore(cx);
            self.report_save_outcome(false, cx);
            cx.notify();
            return;
        }

        // The user may have typed while the write was in flight: what landed is
        // the new baseline, but those newer edits are still pending, and a close
        // waiting on this save must not discard them.
        let landed = self
            .editor
            .as_ref()
            .is_some_and(|editor| editor.input.read(cx).value() == saved_text);

        if let Some(editor) = self.editor.as_mut() {
            editor.baseline = saved_text;
            editor.dirty = !landed;
            editor.byte_len = byte_len;
        }

        Toast::success(dbflux_i18n::t!(
            "document.object_browser.editor.toast.saved",
            uri = format!("s3://{}/{key}", self.bucket).as_str()
        ))
        .meta_right(now_hms())
        .push(cx);

        // Size, last-modified, and ETag all changed server-side.
        self.load_object_metadata(key, cx);

        if landed {
            self.resume_navigation = self.pending_navigation.take();
            self.unsaved_confirm_focus.restore(cx);
        }

        self.report_save_outcome(landed, cx);
        cx.notify();
    }

    // -- Navigate-away guard -------------------------------------------------

    /// Parks `navigation` behind the confirmation when the buffer is dirty.
    /// Returns `true` when the caller must stop — the navigation will run once
    /// the user resolves the prompt.
    pub(super) fn guard_navigation(
        &mut self,
        navigation: GuardedNavigation,
        cx: &mut Context<Self>,
    ) -> bool {
        if !self.editor_is_dirty() {
            return false;
        }

        // Re-selecting the object being edited is not navigating away.
        if let (GuardedNavigation::OpenPreview(key), Some(editor)) =
            (&navigation, self.editor.as_ref())
            && *key == editor.key
        {
            return false;
        }

        self.pending_navigation = Some(navigation);
        self.unsaved_confirm_focus.request_open();
        cx.notify();
        true
    }

    pub(super) fn cancel_guarded_navigation(&mut self, cx: &mut Context<Self>) {
        self.pending_navigation = None;
        self.unsaved_confirm_focus.restore(cx);
        cx.notify();
    }

    /// Discards the edits and lets the parked navigation through.
    pub(super) fn discard_and_navigate(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(navigation) = self.pending_navigation.take() else {
            return;
        };

        self.unsaved_confirm_focus.restore(cx);
        self.discard_object_edits(window, cx);
        self.run_navigation(navigation, window, cx);
    }

    /// Saves, then lets the parked navigation through once the write lands
    /// (`apply_save_outcome` moves it to `resume_navigation`).
    pub(super) fn save_and_navigate(&mut self, cx: &mut Context<Self>) {
        if self.pending_navigation.is_none() {
            return;
        }

        self.save_object_edits(cx);
    }

    /// Runs a navigation that the guard already cleared.
    pub(super) fn run_navigation(
        &mut self,
        navigation: GuardedNavigation,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.drop_editor();

        match navigation {
            GuardedNavigation::OpenPreview(key) => self.open_preview_now(key, cx),
            GuardedNavigation::NavigateToPrefix(prefix) => {
                self.navigate_to_prefix_now(prefix, window, cx)
            }
            GuardedNavigation::ClosePreview => self.close_preview_now(cx),
            GuardedNavigation::DeleteObject(key) => self.open_delete_confirm_now(key, cx),
            GuardedNavigation::RenameObject(key) => self.open_rename_confirm_now(key, window, cx),
            GuardedNavigation::SetEncodingOverride(choice) => {
                self.set_encoding_override_now(choice, cx)
            }
        }
    }

    // -- Rendering -----------------------------------------------------------

    /// The S3-4 editor block: the buffer itself over the save/discard footer.
    pub(super) fn render_text_editor(&self, key: &str, cx: &mut Context<Self>) -> AnyElement {
        let Some(editor) = self.editor_for(key) else {
            return div().into_any_element();
        };

        let theme = cx.theme();
        let position = editor.input.read(cx).cursor_position();
        let is_saving = editor.saving;
        let is_dirty = editor.dirty;
        let is_editable = editor.is_editable();

        // The editor element re-applies its read-only flag to the buffer on
        // every render, so the flag must follow the buffer's editability and
        // the Vim mode. Only a decoded view goes through `ReadOnlyEditor`,
        // which also reports it read-only to accessibility and UI automation;
        // it stays enabled so Vim motions and yanks still move through it.
        let text = if is_editable {
            editor
                .vim
                .editor(false)
                .appearance(false)
                .w_full()
                .h_full()
                .into_any_element()
        } else {
            ReadOnlyEditor::new(&editor.input)
                .appearance(false)
                .w_full()
                .h_full()
                .into_any_element()
        };

        let buffer = div()
            .flex_1()
            .min_h_0()
            .overflow_hidden()
            .bg(theme.background)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, window, cx| {
                    this.focus_editor(window, cx);
                    cx.stop_propagation();
                }),
            )
            .child(text);
        let indicator = editor.vim.render_indicator(cx);
        let wrapper = div().flex_1().flex().flex_col().min_h_0();

        let input = editor.vim.input_id();

        VimBinding::capture_run_command(VimBinding::wire(wrapper, input, cx), input, cx)
            .child(buffer)
            .children(indicator)
            .child(self.render_editor_footer(is_dirty, is_saving, is_editable, position, cx))
            .into_any_element()
    }

    /// Footer: Save (with its shortcut), Discard, and the cursor position —
    /// or, for a decoded read-only buffer, a hint to switch back to Raw.
    fn render_editor_footer(
        &self,
        is_dirty: bool,
        is_saving: bool,
        is_editable: bool,
        position: dbflux_components::controls::InputPosition,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let can_act = is_editable && is_dirty && !is_saving;

        div()
            .flex()
            .items_center()
            .justify_between()
            .gap(Spacing::SM)
            .h(Heights::TOOLBAR)
            .px(Spacing::SM)
            .border_t_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .when(!is_editable, |this| {
                        this.child(
                            Text::caption(dbflux_i18n::t!(
                                "document.object_browser.preview.body.decoded_read_only"
                            ))
                            .muted_foreground(),
                        )
                    })
                    .when(is_editable, |this| {
                        this.child(
                            Button::new(
                                "object-browser-editor-save",
                                if is_saving {
                                    dbflux_i18n::t!("document.object_browser.editor.footer.saving")
                                } else {
                                    dbflux_i18n::t!("document.object_browser.editor.footer.save")
                                },
                            )
                            .primary()
                            .icon(if is_saving {
                                AppIcon::Loader
                            } else {
                                AppIcon::Save
                            })
                            .kbd(keycap_text(SAVE_SHORTCUT_HINT))
                            .disabled(!can_act)
                            .tab_stop(false)
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.save_object_edits(cx);
                            })),
                        )
                        .child(
                            Button::new(
                                "object-browser-editor-discard",
                                dbflux_i18n::t!("document.object_browser.editor.footer.discard"),
                            )
                            .ghost()
                            .icon(AppIcon::RotateCcw)
                            .disabled(!can_act)
                            .tab_stop(false)
                            .on_click(cx.listener(
                                |this, _, window, cx| {
                                    this.discard_object_edits(window, cx);
                                },
                            )),
                        )
                    })
                    .child(
                        Button::new(
                            "object-browser-editor-find",
                            dbflux_i18n::t!("document.object_browser.editor.footer.find"),
                        )
                        .ghost()
                        .icon(AppIcon::Search)
                        .kbd(keycap_text(FIND_SHORTCUT_HINT))
                        .tab_stop(false)
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.open_editor_find(window, cx);
                        })),
                    ),
            )
            .child(Text::caption(cursor_label(position)).muted_foreground())
    }

    /// Whether the buffer itself — rather than one of the editor component's
    /// child inputs, such as the find panel — currently holds focus.
    pub(super) fn editor_input_is_focused(&self, window: &Window, cx: &App) -> bool {
        self.editor
            .as_ref()
            .is_some_and(|editor| editor.input.read(cx).focus_handle(cx).is_focused(window))
    }

    /// Opens the editor component's find panel over the open buffer.
    pub(super) fn open_editor_find(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(input) = self.editor.as_ref().map(|editor| editor.input.clone()) else {
            return;
        };

        self.focus_mode = ObjectBrowserFocusMode::Editor;
        open_find_panel(&input, window, cx);
        cx.notify();
    }

    /// The "modified" pill shown in the preview header while the buffer differs
    /// from the saved content.
    pub(super) fn render_dirty_badge(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme();

        div()
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .px(Spacing::XS)
            .rounded(Radii::SM)
            .border_1()
            .border_color(theme.warning)
            .child(div().size(DIRTY_DOT).rounded(Radii::FULL).bg(theme.warning))
            .child(
                Text::caption(dbflux_i18n::t!(
                    "document.object_browser.editor.dirty_badge"
                ))
                .warning(),
            )
            .into_any_element()
    }

    /// Unsaved-edits confirmation, shown before any navigation that would
    /// leave the buffer behind.
    pub(super) fn render_unsaved_edits_confirm(
        &self,
        navigation: &GuardedNavigation,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let key = self
            .editor
            .as_ref()
            .map(|editor| editor.key.clone())
            .unwrap_or_default();

        let footer = div()
            .flex()
            .gap(Spacing::SM)
            .child(
                Button::new(
                    "object-browser-unsaved-cancel",
                    dbflux_i18n::t!("document.object_browser.editor.unsaved_confirm.cancel"),
                )
                .ghost()
                .focus_handle(&self.unsaved_confirm_focus.cancel)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.cancel_guarded_navigation(cx);
                })),
            )
            .child(
                Button::new(
                    "object-browser-unsaved-discard",
                    dbflux_i18n::t!("document.object_browser.editor.footer.discard"),
                )
                .icon(AppIcon::RotateCcw)
                .focus_handle(&self.unsaved_confirm_focus.discard)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.discard_and_navigate(window, cx);
                })),
            )
            .child(
                Button::new(
                    "object-browser-unsaved-save",
                    dbflux_i18n::t!("document.object_browser.editor.footer.save"),
                )
                .primary()
                .icon(AppIcon::Save)
                .focus_handle(&self.unsaved_confirm_focus.save)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.save_and_navigate(cx);
                })),
            );

        Modal::new(dbflux_i18n::t!(
            "document.object_browser.editor.unsaved_confirm.title"
        ))
        .id("object-browser-unsaved-overlay")
        .icon(AppIcon::TriangleAlert)
        .icon_color(cx.theme().warning)
        .width(px(460.0))
        .body(Text::body(dbflux_i18n::t!(
            "document.object_browser.editor.unsaved_confirm.body",
            key = key.as_str(),
            action = navigation.description().as_str()
        )))
        .footer(footer)
        .focus_handle(self.unsaved_confirm_focus.modal_handle())
        .defer_keys_to_owner()
    }
}

#[cfg(test)]
mod tests {
    // Deliberately narrow imports: `use super::*` would pull in the module's
    // `gpui::*` glob, whose `test` attribute macro would shadow the standard
    // `#[test]` attribute below.
    use super::GuardedNavigation;

    /// T31: the confirmation names what the user was about to do.
    #[test]
    fn guard_describes_the_parked_navigation() {
        assert_eq!(
            GuardedNavigation::OpenPreview("a.txt".to_string()).description(),
            "open a.txt"
        );
        assert_eq!(
            GuardedNavigation::NavigateToPrefix(String::new()).description(),
            "leave for the bucket root"
        );
        assert_eq!(
            GuardedNavigation::ClosePreview.description(),
            "close this preview"
        );
    }

    /// Keeps the latest rendered accessibility frame of the window it observes.
    #[derive(Default)]
    struct FrameCapture(std::sync::Mutex<Option<gpui::AccessibilityFrame>>);

    impl gpui::FrameObserver for FrameCapture {
        fn accessibility_updated(&self, frame: &gpui::AccessibilityFrame) {
            *self.0.lock().expect("frame capture lock") = Some(frame.clone());
        }
    }

    /// Whether the rendered preview editor is reported read-only.
    fn editor_is_reported_read_only(capture: &FrameCapture) -> bool {
        let guard = capture.0.lock().expect("frame capture lock");
        let frame = guard.as_ref().expect("the window rendered a frame");

        frame
            .nodes()
            .find_map(|(_, node)| {
                let accessible = frame.accessibility_node(node)?;
                (accessible.role() == gpui::Role::MultilineTextInput)
                    .then(|| accessible.is_read_only())
            })
            .expect("the preview editor is rendered as a multi-line text input")
    }

    /// Draws the document's preview side panel the way the workspace does:
    /// the preview is not part of the document's own render.
    struct PreviewHost {
        doc: gpui::Entity<super::ObjectBrowserDocument>,
    }

    impl gpui::Render for PreviewHost {
        fn render(
            &mut self,
            window: &mut gpui::Window,
            cx: &mut gpui::Context<Self>,
        ) -> impl gpui::IntoElement {
            use gpui::{ParentElement as _, Styled as _};

            let panel = self
                .doc
                .update(cx, |doc, cx| doc.preview_side_panel(window, cx));

            gpui::div()
                .size_full()
                .flex()
                .children(panel.map(|panel| panel.content))
        }
    }

    /// Opens `key` in the preview with a buffer of `text` from `source`,
    /// renders it, and gives the buffer keyboard focus the way a click does.
    fn open_rendered_editor<'a>(
        cx: &'a mut gpui::TestAppContext,
        key: &str,
        text: &str,
        source: super::TextSource,
    ) -> (
        gpui::Entity<super::ObjectBrowserDocument>,
        &'a mut gpui::VisualTestContext,
        std::sync::Arc<FrameCapture>,
    ) {
        use dbflux_storage::bootstrap::StorageRuntime;
        use gpui::AppContext as _;

        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);
        cx.update(|cx| {
            let host = cx.new(|_cx| dbflux_ui_base::toast::ToastHost::new());
            cx.set_global(dbflux_ui_base::toast::ToastGlobal { host });
        });

        let app_state: gpui::Entity<dbflux_ui_base::AppStateEntity> = cx.update(|cx| {
            cx.new(|_| {
                let runtime = StorageRuntime::in_memory().expect("in-memory storage");
                dbflux_ui_base::AppStateEntity::new_with_storage_runtime(runtime)
                    .expect("test storage setup")
            })
        });

        let capture = std::sync::Arc::new(FrameCapture::default());
        let capture_for_window = capture.clone();

        let (host, window) = cx.add_window_view(move |window, cx| {
            window.observe_frames(&capture_for_window);

            let doc = cx.new(|cx| {
                super::ObjectBrowserDocument::new(
                    uuid::Uuid::new_v4(),
                    "my-bucket".to_string(),
                    app_state,
                    window,
                    cx,
                )
            });

            PreviewHost { doc }
        });

        let doc = window.update(|_window, cx| host.read(cx).doc.clone());

        doc.update_in(window, |doc, window, cx| {
            doc.open_preview(key.to_string(), cx);
            doc.install_text_editor(
                super::PendingTextBody {
                    key: key.to_string(),
                    body: crate::object_text::TextBody {
                        text: text.to_string(),
                        line_ending: crate::object_text::LineEnding::Lf,
                        byte_len: text.len() as u64,
                    },
                    content_type: Some("text/plain".to_string()),
                    source,
                },
                window,
                cx,
            );
        });

        settle_frame(window);

        doc.update_in(window, |doc, window, cx| doc.focus_editor(window, cx));

        settle_frame(window);

        (doc, window, capture)
    }

    fn settle_frame(window: &mut gpui::VisualTestContext) {
        window.update(|window, _cx| window.refresh());
        window.run_until_parked();
    }

    /// Typing into the rendered preview of an object's own text changes the
    /// buffer, marks it dirty, and a landed save makes the typed text the new
    /// baseline.
    #[gpui::test]
    fn typing_into_an_editable_preview_edits_and_saves_the_buffer(cx: &mut gpui::TestAppContext) {
        let (doc, window, capture) =
            open_rendered_editor(cx, "logs/app.log", "before", super::TextSource::Raw);

        assert!(!editor_is_reported_read_only(&capture));

        window.simulate_input("edited ");
        settle_frame(window);

        let typed = doc.update(window, |doc, cx| {
            let typed = doc
                .editor_text_for_test(cx)
                .expect("the preview must have a buffer");

            assert!(typed.contains("edited "), "typing was refused: {typed:?}");
            assert!(doc.editor_is_dirty());

            typed
        });

        doc.update(window, |doc, cx| {
            let byte_len = typed.len() as u64;
            doc.apply_save_outcome(
                "logs/app.log".to_string(),
                typed.clone(),
                byte_len,
                true,
                0,
                cx,
            );

            let editor = doc.editor.as_ref().expect("the buffer survives a save");
            assert_eq!(editor.baseline, typed);
            assert!(!doc.editor_is_dirty());
        });
    }

    /// A decoded view stays read-only in the rendered preview: typing does not
    /// reach the buffer, and it is reported read-only to accessibility and UI
    /// automation.
    #[gpui::test]
    fn a_decoded_preview_stays_read_only(cx: &mut gpui::TestAppContext) {
        let (doc, window, capture) = open_rendered_editor(
            cx,
            "logs/app.log.gz",
            "decoded",
            super::TextSource::Decoded(dbflux_core::Encoding::Gzip),
        );

        assert!(editor_is_reported_read_only(&capture));

        window.simulate_input("edited ");
        settle_frame(window);

        doc.update(window, |doc, cx| {
            assert_eq!(doc.editor_text_for_test(cx).as_deref(), Some("decoded"));
            assert!(!doc.editor_is_dirty());
        });
    }
}
