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

use super::preview_content::PreviewContentState;
use super::{ObjectBrowserDocument, ObjectBrowserFocusMode};
use crate::object_text::{
    FIND_SHORTCUT_HINT, LineEnding, SAVE_SHORTCUT_HINT, TextBody, body_meta_line, build_text_input,
    cursor_label, db_error_to_user_facing, open_find_panel, record_save_audit,
};
// The raw `GpuiInput` (not the app's single-line `Input` wrapper) is what
// `CodeDocument` renders its editor with: only it supports the full-height,
// line-numbered code-editor layout.
use dbflux_app::keymap::Modifiers;
use dbflux_components::controls::{GpuiInput, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text, overlay_bg, surface_panel};
use dbflux_components::tokens::{Heights, Radii, Spacing};
use dbflux_core::DbError;
use dbflux_ui_base::keymap::modifiers_from_gpui;
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Diameter of the dirty indicator inside the "modified" pill.
const DIRTY_DOT: Pixels = px(7.0);

/// A decoded text body ready to be installed into an editor, handed from the
/// background fetch to the next render — building the `InputState` and seeding
/// its value both need a `Window`, which the fetch continuation does not have.
pub(super) struct PendingTextBody {
    pub(super) key: String,
    pub(super) body: TextBody,
    pub(super) content_type: Option<String>,
}

/// The editable buffer for one object.
pub(super) struct ObjectEditor {
    pub(super) key: String,
    pub(super) input: Entity<InputState>,
    /// Content as last loaded or last saved. `dirty` is `buffer != baseline`.
    pub(super) baseline: String,
    pub(super) line_ending: LineEnding,
    pub(super) content_type: Option<String>,
    pub(super) byte_len: u64,
    pub(super) dirty: bool,
    pub(super) saving: bool,
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
}

impl GuardedNavigation {
    fn description(&self) -> String {
        match self {
            GuardedNavigation::OpenPreview(key) => format!("open {key}"),
            GuardedNavigation::NavigateToPrefix(prefix) if prefix.is_empty() => {
                "leave for the bucket root".to_string()
            }
            GuardedNavigation::NavigateToPrefix(prefix) => format!("leave for {prefix}"),
            GuardedNavigation::ClosePreview => "close this preview".to_string(),
            GuardedNavigation::DeleteObject(key) => format!("delete {key}"),
            GuardedNavigation::RenameObject(key) => format!("rename {key}"),
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

        editor
            .dirty
            .then(|| format!("Unsaved edits to {}", editor.key))
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
            _subscription: subscription,
        });

        input.update(cx, |state, cx| {
            state.set_value(&pending.body.text, window, cx);
        });

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

    /// Writes the buffer back to the object with `put_object`, preserving the
    /// detected content type and line-ending convention.
    pub(super) fn save_object_edits(&mut self, cx: &mut Context<Self>) {
        let Some(editor) = self.editor.as_ref() else {
            return;
        };

        if editor.saving {
            return;
        }

        let key = editor.key.clone();
        let content_type = editor.content_type.clone();
        let text = editor.input.read(cx).value().to_string();
        let bytes = editor.line_ending.apply(&text).into_bytes();
        let byte_len = bytes.len() as u64;

        let Some(connection) = self.get_connection(cx) else {
            self.pending_navigation = None;
            report_error(
                UserFacingError::new(
                    ErrorKind::Driver,
                    "Connection is no longer active".to_string(),
                ),
                cx,
            );
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
                None => Err(DbError::NotSupported(
                    "Object-store API unavailable".to_string(),
                )),
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
            })
            .ok();
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

        let Some(editor) = self.editor.as_mut() else {
            return;
        };

        if editor.key != key {
            return;
        }

        editor.saving = false;

        if !succeeded {
            // The failure was already reported; the buffer stays dirty so the
            // user can retry, and any parked navigation is dropped rather than
            // silently carrying the unsaved edits away.
            self.pending_navigation = None;
            cx.notify();
            return;
        }

        editor.baseline = saved_text;
        editor.dirty = false;
        editor.byte_len = byte_len;

        Toast::success(format!("Saved s3://{}/{key}", self.bucket))
            .meta_right(now_hms())
            .push(cx);

        // Size, last-modified, and ETag all changed server-side.
        self.load_object_metadata(key, cx);

        self.resume_navigation = self.pending_navigation.take();

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
        cx.notify();
        true
    }

    pub(super) fn cancel_guarded_navigation(&mut self, cx: &mut Context<Self>) {
        self.pending_navigation = None;
        cx.notify();
    }

    /// Discards the edits and lets the parked navigation through.
    pub(super) fn discard_and_navigate(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(navigation) = self.pending_navigation.take() else {
            return;
        };

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

        div()
            .flex_1()
            .flex()
            .flex_col()
            .min_h_0()
            .child(
                div()
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
                    .on_key_down(cx.listener(|this, event: &KeyDownEvent, _window, cx| {
                        let modifiers = modifiers_from_gpui(&event.keystroke.modifiers);

                        if event.keystroke.key == "s" && modifiers == Modifiers::primary() {
                            this.save_object_edits(cx);
                            cx.stop_propagation();
                        }
                    }))
                    .child(
                        GpuiInput::new(&editor.input)
                            .appearance(false)
                            .w_full()
                            .h_full(),
                    ),
            )
            .child(self.render_editor_footer(is_dirty, is_saving, position, cx))
            .into_any_element()
    }

    /// Footer: Save (with its shortcut), Discard, and the cursor position.
    fn render_editor_footer(
        &self,
        is_dirty: bool,
        is_saving: bool,
        position: dbflux_components::controls::InputPosition,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let can_act = is_dirty && !is_saving;

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
                    .child(
                        div()
                            .id("object-browser-editor-save")
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .h(Heights::CONTROL)
                            .px(Spacing::SM)
                            .rounded(Radii::SM)
                            .bg(theme.primary)
                            .when(!can_act, |d| d.opacity(0.5))
                            .when(can_act, |d| {
                                d.cursor_pointer()
                                    .hover(|d| d.opacity(0.9))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.save_object_edits(cx);
                                    }))
                            })
                            .child(
                                Icon::new(if is_saving {
                                    AppIcon::Loader
                                } else {
                                    AppIcon::Save
                                })
                                .small()
                                .color(theme.primary_foreground),
                            )
                            .child(
                                Text::caption(if is_saving { "Saving…" } else { "Save" })
                                    .color(theme.primary_foreground),
                            )
                            .child(
                                Text::key_hint(SAVE_SHORTCUT_HINT).color(theme.primary_foreground),
                            ),
                    )
                    .child(
                        div()
                            .id("object-browser-editor-discard")
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .h(Heights::CONTROL)
                            .px(Spacing::SM)
                            .rounded(Radii::SM)
                            .when(!can_act, |d| d.opacity(0.5))
                            .when(can_act, |d| {
                                d.cursor_pointer()
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.discard_object_edits(window, cx);
                                    }))
                            })
                            .child(Icon::new(AppIcon::RotateCcw).small().muted())
                            .child(Text::caption("Discard")),
                    )
                    .child(
                        div()
                            .id("object-browser-editor-find")
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .h(Heights::CONTROL)
                            .px(Spacing::SM)
                            .rounded(Radii::SM)
                            .cursor_pointer()
                            .hover(|d| d.bg(theme.secondary))
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.open_editor_find(window, cx);
                            }))
                            .child(Icon::new(AppIcon::Search).small().muted())
                            .child(Text::caption("Find"))
                            .child(Text::key_hint(FIND_SHORTCUT_HINT)),
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
            .child(Text::caption("modified").warning())
            .into_any_element()
    }

    /// Unsaved-edits confirmation, shown before any navigation that would
    /// leave the buffer behind.
    pub(super) fn render_unsaved_edits_confirm(
        &self,
        navigation: &GuardedNavigation,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let key = self
            .editor
            .as_ref()
            .map(|editor| editor.key.clone())
            .unwrap_or_default();

        div()
            .id("object-browser-unsaved-overlay")
            .absolute()
            .inset_0()
            .bg(overlay_bg(theme))
            .flex()
            .items_center()
            .justify_center()
            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                cx.stop_propagation();
            })
            .child(
                surface_panel(cx)
                    .rounded(Radii::MD)
                    .min_w(px(380.0))
                    .flex()
                    .flex_col()
                    .gap(Spacing::MD)
                    .p(Spacing::MD)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .child(
                                Icon::new(AppIcon::TriangleAlert)
                                    .size(Heights::ICON_MD)
                                    .warning(),
                            )
                            .child(Text::heading("Unsaved edits")),
                    )
                    .child(Text::muted(format!(
                        "\"{key}\" has unsaved edits. Save them before you {}?",
                        navigation.description()
                    )))
                    .child(
                        div()
                            .flex()
                            .justify_end()
                            .gap(Spacing::SM)
                            .child(
                                div()
                                    .id("object-browser-unsaved-cancel")
                                    .flex()
                                    .items_center()
                                    .h(Heights::CONTROL)
                                    .px(Spacing::SM)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .bg(theme.secondary)
                                    .hover(|d| d.bg(theme.muted))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.cancel_guarded_navigation(cx);
                                    }))
                                    .child(Text::caption("Cancel")),
                            )
                            .child(
                                div()
                                    .id("object-browser-unsaved-discard")
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::XS)
                                    .h(Heights::CONTROL)
                                    .px(Spacing::SM)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .bg(theme.secondary)
                                    .hover(|d| d.bg(theme.muted))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.discard_and_navigate(window, cx);
                                    }))
                                    .child(Icon::new(AppIcon::RotateCcw).small().muted())
                                    .child(Text::caption("Discard")),
                            )
                            .child(
                                div()
                                    .id("object-browser-unsaved-save")
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::XS)
                                    .h(Heights::CONTROL)
                                    .px(Spacing::SM)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .bg(theme.primary)
                                    .hover(|d| d.opacity(0.9))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.save_and_navigate(cx);
                                    }))
                                    .child(
                                        Icon::new(AppIcon::Save)
                                            .small()
                                            .color(theme.primary_foreground),
                                    )
                                    .child(Text::caption("Save").color(theme.primary_foreground)),
                            ),
                    ),
            )
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
}
