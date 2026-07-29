//! Inline text editor for the preview pane.
//!
//! Text-like objects the preview gate allows are decoded into an editable
//! buffer (`ObjectEditor`) instead of falling back to download/open-externally.
//! The buffer is a standalone `InputState` in code-editor mode — the same
//! component `CodeDocument` uses — with the loaded content kept as a baseline
//! so "modified" is a plain comparison rather than a change counter.
//!
//! Saving writes the buffer back with `put_object`, preserving the object's
//! content type and its original line-ending convention. Anything that would
//! move away from a dirty buffer — selecting another object, navigating to
//! another prefix, closing the preview — routes through
//! `guard_navigation`, which parks the request behind a Save / Discard /
//! Cancel confirmation. Edits are never dropped silently.

use super::preview_content::PreviewContentState;
use super::{ObjectBrowserDocument, ObjectBrowserFocusMode};
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
use uuid::Uuid;

/// Save shortcut label, matching the `SaveQuery` binding (Cmd+S on macOS,
/// Ctrl+S elsewhere) that the editor also answers to.
#[cfg(target_os = "macos")]
pub(super) const SAVE_SHORTCUT_HINT: &str = "Cmd+S";
#[cfg(not(target_os = "macos"))]
pub(super) const SAVE_SHORTCUT_HINT: &str = "Ctrl+S";

/// Diameter of the dirty indicator inside the "modified" pill.
const DIRTY_DOT: Pixels = px(7.0);

/// Line-ending convention of a loaded object.
///
/// The buffer always holds LF internally — the editor component normalises
/// input — so the original convention is recorded on load and restored on
/// save, otherwise editing a CRLF object would silently rewrite every line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LineEnding {
    Lf,
    Crlf,
}

impl LineEnding {
    /// CRLF only when the body actually uses it; a body with no line break at
    /// all is LF, which is what a new line typed into it will produce.
    pub fn detect(text: &str) -> Self {
        if text.contains("\r\n") {
            LineEnding::Crlf
        } else {
            LineEnding::Lf
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            LineEnding::Lf => "LF",
            LineEnding::Crlf => "CRLF",
        }
    }

    /// Rewrites `text` (held with LF) in this convention.
    pub fn apply(self, text: &str) -> String {
        match self {
            LineEnding::Lf => text.to_string(),
            LineEnding::Crlf => text.replace('\n', "\r\n"),
        }
    }
}

/// A text object's body, decoded and normalised for editing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextBody {
    pub text: String,
    pub line_ending: LineEnding,
    pub byte_len: u64,
}

/// Decodes an object body for the editor. Only UTF-8 is accepted: a lossy
/// decode would let the user save back a file whose undecodable bytes had been
/// replaced by placeholders.
pub fn decode_text_body(bytes: Vec<u8>) -> Result<TextBody, String> {
    let byte_len = bytes.len() as u64;

    let text = String::from_utf8(bytes)
        .map_err(|_| "This object is not valid UTF-8 text and cannot be edited in-app.")?;

    let line_ending = LineEnding::detect(&text);
    let normalised = text.replace("\r\n", "\n");

    Ok(TextBody {
        text: normalised,
        line_ending,
        byte_len,
    })
}

/// Highlighter language for the buffer, from the key's extension. Unknown
/// extensions resolve to the plain highlighter inside the editor component.
pub fn editor_language(key: &str) -> String {
    let name = key.rsplit_once('/').map(|(_, name)| name).unwrap_or(key);

    name.rsplit_once('.')
        .map(|(_, extension)| extension.to_lowercase())
        .unwrap_or_else(|| "text".to_string())
}

const MAX_HIGHLIGHT_BYTES: usize = 1024 * 1024;
const MAX_HIGHLIGHT_LINE_CHARS: usize = 10_000;

/// Language for the syntax-highlighting editor, or `None` to open a plain
/// buffer. Tree-sitter parsing and per-line layout run on the UI thread, so a
/// large body or a minified single-line file (typical for html/js/css assets)
/// must skip highlighting entirely or the app freezes on open.
pub fn highlight_language(key: &str, body: &str) -> Option<String> {
    if body.len() > MAX_HIGHLIGHT_BYTES {
        return None;
    }

    if body
        .lines()
        .any(|line| line.len() > MAX_HIGHLIGHT_LINE_CHARS)
    {
        return None;
    }

    Some(editor_language(key))
}

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
        format!(
            "{} · {} · UTF-8 · {}",
            self.content_type.as_deref().unwrap_or("text/plain"),
            crate::buckets_table::format_bytes(self.byte_len),
            self.line_ending.label()
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

        let language = highlight_language(&pending.key, &pending.body.text);

        // Plain buffers exist because the body tripped the highlight gate —
        // usually one enormous minified line. Without wrapping, that line
        // makes click positioning and horizontal navigation unusable.
        let input = cx.new(|cx| {
            let state = InputState::new(window, cx);

            match language {
                Some(language) => state
                    .code_editor(language)
                    .line_number(true)
                    .soft_wrap(false),
                None => state.multi_line(true).line_number(true).soft_wrap(true),
            }
        });

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
                    ),
            )
            .child(Text::caption(cursor_label(position)).muted_foreground())
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

/// `Ln n, Col n` from the editor's 0-based cursor position.
fn cursor_label(position: dbflux_components::controls::InputPosition) -> String {
    format!("Ln {}, Col {}", position.line + 1, position.character + 1)
}

fn db_error_to_user_facing(err: &DbError) -> UserFacingError {
    match err.formatted() {
        Some(formatted) => UserFacingError::from_formatted(ErrorKind::Driver, formatted.clone()),
        None => UserFacingError::new(ErrorKind::Driver, err.to_string()),
    }
}

/// Audits a save-back. Only the bucket, key, and outcome are recorded — never
/// the object's content.
fn record_save_audit(
    audit_service: &dbflux_audit::AuditService,
    profile_id: Uuid,
    bucket: &str,
    key: &str,
    error: Option<&str>,
) {
    use dbflux_core::chrono::Utc;
    use dbflux_core::observability::{
        EventCategory, EventOutcome, EventRecord, EventSeverity, EventSink,
    };

    let (severity, outcome, action) = match error {
        Some(_) => (
            EventSeverity::Error,
            EventOutcome::Failure,
            "object_edit_save_failed",
        ),
        None => (
            EventSeverity::Info,
            EventOutcome::Success,
            "object_edit_save",
        ),
    };

    let mut summary = format!("Saved edits to s3://{bucket}/{key}");
    if let Some(error) = error {
        summary.push_str(&format!(": {error}"));
    }

    let event = EventRecord::new(
        Utc::now().timestamp_millis(),
        severity,
        EventCategory::ObjectStorage,
        outcome,
    )
    .with_action(action.to_string())
    .with_summary(summary)
    .with_actor_id("ui:user")
    .with_object_ref("object", format!("{bucket}/{key}"))
    .with_connection_context(profile_id.to_string(), bucket.to_string(), String::new());

    if let Err(e) = audit_service.record(event) {
        log::warn!("[object browser] failed to record object-edit audit event: {e}");
    }
}

#[cfg(test)]
mod tests {
    // Deliberately narrow imports: `use super::*` would pull in the module's
    // `gpui::*` glob, whose `test` attribute macro would shadow the standard
    // `#[test]` attribute below.
    use super::{GuardedNavigation, LineEnding, cursor_label, decode_text_body, editor_language};

    /// T30: a CRLF body is recognised so the convention survives a round-trip.
    #[test]
    fn line_endings_round_trip() {
        assert_eq!(LineEnding::detect("a\r\nb"), LineEnding::Crlf);
        assert_eq!(LineEnding::detect("a\nb"), LineEnding::Lf);
        assert_eq!(LineEnding::detect("single line"), LineEnding::Lf);

        assert_eq!(LineEnding::Crlf.apply("a\nb"), "a\r\nb");
        assert_eq!(LineEnding::Lf.apply("a\nb"), "a\nb");
    }

    /// T30: the buffer always holds LF, and the original byte length is kept
    /// for the meta line.
    #[test]
    fn decoding_normalises_to_lf() {
        let body = decode_text_body(b"first\r\nsecond".to_vec()).expect("valid UTF-8");

        assert_eq!(body.text, "first\nsecond");
        assert_eq!(body.line_ending, LineEnding::Crlf);
        assert_eq!(body.byte_len, 13);
    }

    /// T30: a body that is not UTF-8 is refused rather than lossily decoded —
    /// saving a placeholder-mangled buffer would corrupt the object.
    #[test]
    fn decoding_refuses_non_utf8_bodies() {
        assert!(decode_text_body(vec![0xff, 0xfe, 0x00]).is_err());
    }

    /// T30: the highlighter language comes from the extension, with a plain
    /// fallback for keys that have none.
    #[test]
    fn editor_language_follows_the_extension() {
        assert_eq!(editor_language("logs/app.JSON"), "json");
        assert_eq!(editor_language("notes.md"), "md");
        assert_eq!(editor_language("data/dump"), "text");
    }

    /// T30: the cursor readout is 1-based, like every other editor.
    #[test]
    fn cursor_label_is_one_based() {
        let position = dbflux_components::controls::InputPosition {
            line: 0,
            character: 0,
        };

        assert_eq!(cursor_label(position), "Ln 1, Col 1");
    }

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

#[cfg(test)]
mod highlight_gate_tests {
    use super::highlight_language;

    #[test]
    fn small_multi_line_files_keep_their_language() {
        let body = "<html>\n<body>hello</body>\n</html>\n";
        assert_eq!(
            highlight_language("site/index.html", body),
            Some("html".to_string())
        );
    }

    #[test]
    fn oversized_bodies_open_plain() {
        let body = "a\n".repeat(600_000);
        assert_eq!(highlight_language("big.html", &body), None);
    }

    #[test]
    fn minified_single_line_files_open_plain() {
        let body = "x".repeat(20_000);
        assert_eq!(highlight_language("app.min.html", &body), None);
    }
}
