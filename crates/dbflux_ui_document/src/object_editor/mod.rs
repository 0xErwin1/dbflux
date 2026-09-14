//! Standalone editor tab for a single object-store text object.
//!
//! The object browser's preview pane can already edit a text object, but it is
//! a narrow side pane. This document takes the same object to a full tab: one
//! buffer, the whole width, nothing else on screen.
//!
//! Everything about reading and writing the object is shared with the preview
//! editor through `crate::object_text` — the UTF-8 decode, the line-ending
//! round-trip, the highlighter gate, the buffer construction and the save
//! audit record — so a save from here is indistinguishable from a save there.
//!
//! The tab is opened by the workspace, never by the browser: the browser only
//! stages an intent, which the workspace drains through the generic
//! `PaneHandle::take_pending_open_object_editor` helper.

pub mod pane;
mod render;

use crate::handle::DocumentEvent;
use crate::object_browser::{
    EncodingChoice, PreparedPreview, PreviewGate, TextSource, prepare_preview,
};
use crate::object_text::{
    LineEnding, TextBody, build_text_input, db_error_to_user_facing, record_save_audit,
};
use crate::pane::ObjectSavedCallback;
use crate::types::{DocumentId, DocumentState};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_core::{DbError, ObjectMetadata, RefreshPolicy};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::*;
use std::time::Instant;
use uuid::Uuid;

/// Why an object could not be opened for editing. Carries the message the tab
/// shows in place of the buffer, and whether it is specifically the size gate
/// — the only refusal the tab lets the user bypass with "Load anyway".
enum LoadRefusal {
    TooLarge(String),
    Other(String),
}

impl LoadRefusal {
    fn message(&self) -> &str {
        match self {
            LoadRefusal::TooLarge(message) | LoadRefusal::Other(message) => message,
        }
    }

    fn is_too_large(&self) -> bool {
        matches!(self, LoadRefusal::TooLarge(_))
    }
}

/// State of the object's body.
enum LoadState {
    Loading,
    Ready,
    Failed(LoadRefusal),
}

/// A decoded body waiting for a render pass to become a buffer — building the
/// `InputState` needs a `Window`, which the fetch continuation does not have.
struct PendingBody {
    body: TextBody,
    content_type: Option<String>,
    /// What produced this text — the object's raw bytes, or a value decoded
    /// from them. Drives whether the installed buffer is editable.
    source: TextSource,
}

/// The editable buffer, with the content last loaded or saved as the baseline
/// so "modified" is a plain comparison rather than a change counter.
struct Buffer {
    input: Entity<InputState>,
    baseline: String,
    line_ending: LineEnding,
    content_type: Option<String>,
    byte_len: u64,
    dirty: bool,
    /// What produced the buffer's text. Only `TextSource::Raw` may be saved
    /// back — a decoded view never writes its re-encoded form over the
    /// object's real bytes.
    source: TextSource,
    _subscription: Subscription,
}

impl Buffer {
    fn is_editable(&self) -> bool {
        self.source.is_editable()
    }
}

/// One object-store text object, open in its own tab.
pub struct ObjectEditorDocument {
    id: DocumentId,
    profile_id: Uuid,
    bucket: String,
    key: String,
    app_state: Entity<AppStateEntity>,
    focus_handle: FocusHandle,
    is_active_tab: bool,
    refresh_policy: RefreshPolicy,
    load: LoadState,
    pending_body: Option<PendingBody>,
    buffer: Option<Buffer>,
    saving: bool,
    /// Set once the user accepts "Load anyway" on a size-gate refusal. The
    /// tab has exactly one object, so this stays sticky for its lifetime
    /// rather than resetting per selection like the browser's preview pane.
    size_gate_override: bool,
    /// User's explicit override of the auto-detected encoding, or `None` to
    /// use magic-byte detection. The tab only ever offers "Raw" — a full
    /// per-format picker belongs to the browser's preview toolbar.
    encoding_override: Option<EncodingChoice>,
    /// Invoked with the key after every successful save so the document that
    /// asked for this tab can refresh its own view of the object.
    on_saved: ObjectSavedCallback,
}

impl EventEmitter<DocumentEvent> for ObjectEditorDocument {}

impl ObjectEditorDocument {
    pub fn new(
        profile_id: Uuid,
        bucket: String,
        key: String,
        on_saved: ObjectSavedCallback,
        app_state: Entity<AppStateEntity>,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut doc = Self {
            id: DocumentId::new(),
            profile_id,
            bucket,
            key,
            app_state,
            focus_handle: cx.focus_handle(),
            is_active_tab: true,
            refresh_policy: RefreshPolicy::Manual,
            load: LoadState::Loading,
            pending_body: None,
            buffer: None,
            saving: false,
            size_gate_override: false,
            encoding_override: None,
            on_saved,
        };

        doc.load_object(cx);
        doc
    }

    pub fn id(&self) -> DocumentId {
        self.id
    }

    /// The key's leaf name — the full key lives in the status bar, where there
    /// is room for it.
    pub fn title(&self) -> String {
        self.key
            .rsplit_once('/')
            .map(|(_, name)| name)
            .unwrap_or(&self.key)
            .to_string()
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    /// Size of the object as last loaded or saved, once a buffer exists.
    pub fn byte_len(&self) -> Option<u64> {
        self.buffer.as_ref().map(|buffer| buffer.byte_len)
    }

    pub fn profile_id(&self) -> Uuid {
        self.profile_id
    }

    pub fn connection_id(&self) -> Option<Uuid> {
        Some(self.profile_id)
    }

    /// Unsaved edits win over every other state: the dirty dot is what routes
    /// a tab close through the workspace's unsaved-changes modal.
    pub fn state(&self) -> DocumentState {
        if self.is_dirty() {
            return DocumentState::Modified;
        }

        match self.load {
            LoadState::Loading => DocumentState::Loading,
            LoadState::Failed(_) => DocumentState::Error,
            LoadState::Ready => DocumentState::Clean,
        }
    }

    /// Always closable: unsaved edits route the close through the workspace's
    /// unsaved-changes modal rather than blocking it.
    pub fn can_close(&self) -> bool {
        true
    }

    pub fn is_dirty(&self) -> bool {
        self.buffer.as_ref().is_some_and(|buffer| buffer.dirty)
    }

    /// Summary for the tab's dirty-dot tooltip and the unsaved-changes modal.
    pub fn change_summary(&self) -> Option<String> {
        self.is_dirty().then(|| {
            dbflux_i18n::t!(
                "document.object_browser.editor.unsaved_summary",
                key = self.key.as_str()
            )
        })
    }

    pub fn refresh_policy(&self) -> RefreshPolicy {
        self.refresh_policy
    }

    pub fn set_refresh_policy(&mut self, policy: RefreshPolicy, cx: &mut Context<Self>) {
        self.refresh_policy = policy;
        cx.notify();
    }

    pub fn set_active_tab(&mut self, active: bool) {
        self.is_active_tab = active;
    }

    /// The buffer is a text input and owns every letter it is given.
    pub fn active_context(&self) -> ContextId {
        ContextId::TextInput
    }

    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.buffer.as_ref() {
            Some(buffer) => buffer
                .input
                .clone()
                .update(cx, |state, cx| state.focus(window, cx)),
            None => self.focus_handle.focus(window),
        }

        cx.notify();
    }

    // -- Loading -------------------------------------------------------------

    fn get_connection(
        &self,
        cx: &Context<Self>,
    ) -> Option<std::sync::Arc<dyn dbflux_core::Connection>> {
        self.app_state
            .read(cx)
            .connections()
            .get(&self.profile_id)
            .map(|connected| connected.connection.clone())
    }

    /// Fetches the object's metadata and body through the same shared
    /// decoder as the browser preview pane: the gate (size limit, archived
    /// tiers) decides whether the body may be fetched at all, magic-byte
    /// detection (or `encoding_override`) decides how to present it, and only
    /// a text result is accepted — this tab edits text, nothing else.
    fn load_object(&mut self, cx: &mut Context<Self>) {
        self.load = LoadState::Loading;
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            self.load = LoadState::Failed(LoadRefusal::Other(dbflux_i18n::t!(
                "document.object_browser.error.connection_unavailable"
            )));
            cx.notify();
            return;
        };

        let limit_bytes = self
            .app_state
            .read(cx)
            .general_settings()
            .object_preview_size_limit_bytes();
        let bypass_size_gate = self.size_gate_override;
        let override_choice = self.encoding_override;

        let entity = cx.entity().clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();

        let task = cx.background_executor().spawn(async move {
            let started = Instant::now();

            let result = load_editable_body(
                &*connection,
                &bucket,
                &key,
                limit_bytes,
                bypass_size_gate,
                override_choice,
            );

            (result, started.elapsed().as_millis())
        });

        cx.spawn(async move |_this, cx| {
            let (result, _elapsed_millis) = task.await;

            if let Err(ref err) = result {
                report_error_async(db_error_to_user_facing(err), cx);
            }

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.apply_load_outcome(result, cx);
                });
            })
            .ok();
        })
        .detach();
    }

    /// Bypasses the size gate at the user's explicit "Load anyway" request
    /// and reloads the object under the override.
    pub fn load_anyway(&mut self, cx: &mut Context<Self>) {
        self.size_gate_override = true;
        self.load_object(cx);
    }

    /// Toggles between magic-byte auto-detection and forcing the object's raw
    /// bytes, then reloads to apply the change.
    ///
    /// Refuses while the buffer is dirty: reloading under the new override
    /// replaces the buffer's content outright, and this tab has no
    /// unsaved-edits confirmation of its own to park the request behind.
    pub fn set_raw_override(&mut self, raw: bool, cx: &mut Context<Self>) {
        if self.is_dirty() {
            return;
        }

        self.encoding_override = raw.then_some(EncodingChoice::Raw);
        self.buffer = None;
        self.load_object(cx);
    }

    /// Whether the currently loaded body is the object's own raw bytes
    /// (editable) or a decoded view of them (read-only).
    pub fn is_editable(&self) -> bool {
        self.buffer.as_ref().is_some_and(Buffer::is_editable)
    }

    pub fn has_raw_override(&self) -> bool {
        self.encoding_override == Some(EncodingChoice::Raw)
    }

    #[allow(clippy::result_large_err)]
    fn apply_load_outcome(
        &mut self,
        result: Result<Result<LoadedBody, LoadRefusal>, DbError>,
        cx: &mut Context<Self>,
    ) {
        match result {
            Ok(Ok(loaded)) => {
                self.pending_body = Some(PendingBody {
                    body: loaded.body,
                    content_type: loaded.content_type,
                    source: loaded.source,
                });
                self.load = LoadState::Ready;
            }
            Ok(Err(refusal)) => self.load = LoadState::Failed(refusal),
            Err(err) => self.load = LoadState::Failed(LoadRefusal::Other(err.to_string())),
        }

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    /// Turns a decoded body into the buffer. Called from `render`, where a
    /// `Window` is available.
    fn install_buffer(
        &mut self,
        pending: PendingBody,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let input = build_text_input(&self.key, &pending.body.text, window, cx);

        let subscription = cx.subscribe_in(
            &input,
            window,
            |this, input, event: &InputEvent, _window, cx| {
                if !matches!(event, InputEvent::Change) {
                    return;
                }

                let value = input.read(cx).value().to_string();

                if let Some(buffer) = this.buffer.as_mut() {
                    let dirty = value != buffer.baseline;

                    if buffer.dirty != dirty {
                        buffer.dirty = dirty;
                        cx.emit(DocumentEvent::MetaChanged);
                        cx.notify();
                    }
                }
            },
        );

        self.buffer = Some(Buffer {
            input: input.clone(),
            baseline: pending.body.text.clone(),
            line_ending: pending.body.line_ending,
            content_type: pending.content_type,
            byte_len: pending.body.byte_len,
            dirty: false,
            source: pending.source,
            _subscription: subscription,
        });

        input.update(cx, |state, cx| {
            state.set_value(&pending.body.text, window, cx);
        });

        cx.notify();
    }

    /// Restores the buffer to the content last loaded or saved.
    fn discard_edits(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(buffer) = self.buffer.as_ref() else {
            return;
        };

        let baseline = buffer.baseline.clone();
        let input = buffer.input.clone();

        input.update(cx, |state, cx| {
            state.set_value(&baseline, window, cx);
        });

        if let Some(buffer) = self.buffer.as_mut() {
            buffer.dirty = false;
        }

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    // -- Save ----------------------------------------------------------------

    /// Writes the buffer back with `put_object`, preserving the object's
    /// content type and its original line-ending convention.
    pub fn save(&mut self, cx: &mut Context<Self>) {
        let Some(buffer) = self.buffer.as_ref() else {
            return;
        };

        // A decoded view is never the object's real bytes — writing it back
        // would silently replace the object's actual content with a
        // re-encoding of its decoded form. The footer never offers Save for
        // this state, but the guard stays here too since it is reachable
        // from the Ctrl/Cmd+S shortcut regardless of what is rendered.
        if !buffer.is_editable() || self.saving {
            return;
        }

        let content_type = buffer.content_type.clone();
        let text = buffer.input.read(cx).value().to_string();
        let bytes = buffer.line_ending.apply(&text).into_bytes();
        let byte_len = bytes.len() as u64;

        let Some(connection) = self.get_connection(cx) else {
            report_error(
                UserFacingError::new(
                    ErrorKind::Driver,
                    dbflux_i18n::t!("document.object_browser.error.connection_unavailable"),
                ),
                cx,
            );
            return;
        };

        self.saving = true;
        cx.notify();

        let audit_service = self.app_state.read(cx).audit_service().clone();
        let entity = cx.entity().clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let profile_id = self.profile_id;
        let bucket_for_task = bucket.clone();
        let key_for_task = key.clone();

        let task = cx.background_executor().spawn(async move {
            match connection.object_store_api() {
                Some(api) => api.put_object(
                    &bucket_for_task,
                    &key_for_task,
                    bytes,
                    content_type.as_deref(),
                ),
                None => Err(DbError::NotSupported(dbflux_i18n::t!(
                    "document.object_browser.error.api_unavailable"
                ))),
            }
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

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
                    doc.apply_save_outcome(text, byte_len, result.is_ok(), cx);
                });
            })
            .ok();
        })
        .detach();
    }

    fn apply_save_outcome(
        &mut self,
        saved_text: String,
        byte_len: u64,
        succeeded: bool,
        cx: &mut Context<Self>,
    ) {
        self.saving = false;

        // The failure was already reported; the buffer stays dirty so the user
        // can retry.
        if !succeeded {
            cx.notify();
            return;
        }

        if let Some(buffer) = self.buffer.as_mut() {
            buffer.baseline = saved_text;
            buffer.dirty = false;
            buffer.byte_len = byte_len;
        }

        Toast::success(dbflux_i18n::t!(
            "document.object_browser.editor.toast.saved",
            uri = format!("s3://{}/{}", self.bucket, self.key).as_str()
        ))
        .meta_right(now_hms())
        .push(cx);

        let notify_opener = self.on_saved.clone();
        let key = self.key.clone();
        notify_opener(&key, cx);

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    // -- Commands ------------------------------------------------------------

    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match cmd {
            // Both save paths land here: Ctrl/Cmd+S in the buffer, and the
            // unsaved-changes modal's save on tab close (`SaveFileAs`).
            Command::SaveQuery | Command::SaveFileAs => {
                self.save(cx);
                true
            }
            Command::RefreshSchema => {
                if !self.is_dirty() {
                    self.buffer = None;
                    self.load_object(cx);
                }
                true
            }
            Command::FocusSearch => {
                self.open_find(window, cx);
                true
            }
            _ => false,
        }
    }

    /// Opens the editor component's find panel over the buffer.
    pub fn open_find(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(input) = self.buffer.as_ref().map(|buffer| buffer.input.clone()) else {
            return;
        };

        crate::object_text::open_find_panel(&input, window, cx);
        cx.notify();
    }

    #[cfg(test)]
    pub(crate) fn install_buffer_for_test(
        &mut self,
        text: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.load = LoadState::Ready;
        self.install_buffer(
            PendingBody {
                body: TextBody {
                    text: text.to_string(),
                    line_ending: LineEnding::Lf,
                    byte_len: text.len() as u64,
                },
                content_type: Some("text/plain".to_string()),
                source: TextSource::Raw,
            },
            window,
            cx,
        );
    }

    #[cfg(test)]
    pub(crate) fn fail_load_for_test(&mut self, message: &str) {
        self.load = LoadState::Failed(LoadRefusal::Other(message.to_string()));
    }

    #[cfg(test)]
    pub(crate) fn type_for_test(
        &mut self,
        text: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(input) = self.buffer.as_ref().map(|buffer| buffer.input.clone()) else {
            return;
        };

        // A real edit, not `set_value`: the component replaces text silently in
        // `set_value`, so only this path emits the `Change` the dirty tracking
        // listens for.
        let text = text.to_string();
        input.update(cx, |state, cx| {
            state.replace_text_in_range(None, &text, window, cx)
        });
    }
}

/// A body that resolved to text, whether the object's own raw bytes or a
/// decoded view of them.
struct LoadedBody {
    body: TextBody,
    content_type: Option<String>,
    source: TextSource,
}

/// Decides whether the shared preview gate lets `metadata`'s bytes be
/// fetched at all — the same size limit and archived-storage-tier check the
/// object browser preview pane applies — before any network call.
/// `bypass_size_gate` is the tab's "Load anyway" override.
fn gate_refusal(
    metadata: &ObjectMetadata,
    limit_bytes: u64,
    bypass_size_gate: bool,
) -> Option<LoadRefusal> {
    let gate = crate::object_browser::evaluate_preview_gate(metadata, limit_bytes);

    match gate {
        PreviewGate::Allowed => None,
        PreviewGate::TooLarge { .. } if bypass_size_gate => None,
        PreviewGate::TooLarge { .. } => {
            Some(LoadRefusal::TooLarge(gate.message().unwrap_or_default()))
        }
        PreviewGate::Archived => Some(LoadRefusal::Other(gate.message().unwrap_or_default())),
    }
}

/// Reads `key`'s metadata, applies the shared preview gate, and only then
/// fetches the body and resolves it through the shared decoder
/// (`prepare_preview`). Only a text result is accepted — this tab edits text,
/// nothing else — with every other resolved kind refused by message.
///
/// Runs entirely on the background executor: every call here is a blocking
/// driver call.
#[allow(clippy::result_large_err)]
fn load_editable_body(
    connection: &dyn dbflux_core::Connection,
    bucket: &str,
    key: &str,
    limit_bytes: u64,
    bypass_size_gate: bool,
    override_choice: Option<EncodingChoice>,
) -> Result<Result<LoadedBody, LoadRefusal>, DbError> {
    let Some(api) = connection.object_store_api() else {
        return Err(DbError::NotSupported(dbflux_i18n::t!(
            "document.object_browser.error.api_unavailable"
        )));
    };

    let metadata: ObjectMetadata = api.head_object(bucket, key)?;

    if let Some(refusal) = gate_refusal(&metadata, limit_bytes, bypass_size_gate) {
        return Ok(Err(refusal));
    }

    let content_type = metadata.content_type.clone();
    let bytes = api.get_object(bucket, key)?;
    let prepared = prepare_preview(
        &bytes,
        content_type.as_deref(),
        key,
        limit_bytes as usize,
        override_choice,
    );

    let outcome = match prepared {
        PreparedPreview::Text { text, source } => Ok(LoadedBody {
            body: TextBody {
                line_ending: LineEnding::detect(&text),
                byte_len: bytes.len() as u64,
                text,
            },
            content_type,
            source,
        }),
        PreparedPreview::Image(_) | PreparedPreview::Pdf | PreparedPreview::Binary => {
            Err(dbflux_i18n::t!("document.object_editor.error.not_text"))
        }
        PreparedPreview::DecodeFailed { reason, .. } => Err(reason),
        PreparedPreview::DecodeTooLarge {
            limit_bytes: decode_limit,
            ..
        } => Err(dbflux_i18n::t!(
            "document.object_editor.error.decode_too_large",
            limit = crate::buckets_table::format_bytes(decode_limit as u64).as_str()
        )),
    };

    Ok(outcome.map_err(LoadRefusal::Other))
}

#[cfg(test)]
mod tests {
    // Deliberately narrow imports: `use super::*` would pull in the module's
    // `gpui::*` glob, whose `test` attribute macro would shadow the plain
    // `#[test]` attribute below.
    use super::{ObjectEditorDocument, gate_refusal};
    use crate::types::DocumentState;
    use dbflux_core::ObjectMetadata;
    use std::cell::RefCell;
    use std::rc::Rc;

    fn metadata(key: &str, size_bytes: u64, content_type: Option<&str>) -> ObjectMetadata {
        ObjectMetadata {
            key: key.to_string(),
            size_bytes,
            content_type: content_type.map(|value| value.to_string()),
            last_modified: None,
            etag: None,
            storage_class: Some("STANDARD".to_string()),
            encryption: None,
            version_count: None,
        }
    }

    /// An object under the limit clears the gate — the body may be fetched.
    #[test]
    fn objects_within_the_limit_clear_the_gate() {
        assert!(gate_refusal(&metadata("a.txt", 10, Some("text/plain")), 1024, false).is_none());
    }

    /// Past the limit the tab refuses with the gate's own explanation rather
    /// than fetching a body it would not render — unless the size gate was
    /// explicitly bypassed via "Load anyway".
    #[test]
    fn oversized_objects_are_refused_with_the_gate_message_unless_bypassed() {
        let refusal = gate_refusal(&metadata("a.txt", 4096, Some("text/plain")), 1024, false)
            .expect("over the limit");

        assert!(refusal.is_too_large());
        assert!(refusal.message().contains("preview limit"));

        assert!(gate_refusal(&metadata("a.txt", 4096, Some("text/plain")), 1024, true).is_none());
    }

    /// The size-refusal message reuses `PreviewGate::message()` exactly as
    /// the object browser preview pane does — the editor never carries its
    /// own copy of the gate's explanation.
    #[test]
    fn oversized_refusal_message_matches_the_shared_gate_mapping() {
        let metadata = metadata("a.txt", 4096, Some("text/plain"));
        let gate = crate::object_browser::evaluate_preview_gate(&metadata, 1024);

        let refusal =
            gate_refusal(&metadata, 1024, false).expect("over the limit refuses without bypass");

        assert_eq!(Some(refusal.message().to_string()), gate.message());
    }

    /// An archived object is refused even when it would fit under the limit,
    /// and bypassing the size gate does not lift it — archival is a
    /// different refusal entirely.
    #[test]
    fn archived_objects_are_refused_regardless_of_the_size_bypass() {
        let mut archived = metadata("a.txt", 10, Some("text/plain"));
        archived.storage_class = Some("GLACIER".to_string());

        let refusal = gate_refusal(&archived, 1024, true).expect("archived objects are refused");
        assert!(!refusal.is_too_large());
    }

    /// Records every key the document reports as saved, so a test can prove
    /// the requesting document is notified.
    type SavedKeys = Rc<RefCell<Vec<String>>>;

    fn new_test_document<'a>(
        cx: &'a mut gpui::TestAppContext,
        key: &str,
    ) -> (
        gpui::Entity<ObjectEditorDocument>,
        &'a mut gpui::VisualTestContext,
        SavedKeys,
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

        let saved: SavedKeys = Rc::new(RefCell::new(Vec::new()));
        let on_saved = {
            let saved = saved.clone();
            Rc::new(move |key: &str, _cx: &mut gpui::App| {
                saved.borrow_mut().push(key.to_string());
            })
        };

        let key = key.to_string();
        let (entity, window_cx) = cx.add_window_view(|_window, cx| {
            ObjectEditorDocument::new(
                uuid::Uuid::new_v4(),
                "my-bucket".to_string(),
                key,
                on_saved,
                app_state,
                cx,
            )
        });

        (entity, window_cx, saved)
    }

    /// The tab title is the key's leaf name — the full key would not fit, and
    /// it is already carried by the status bar.
    #[gpui::test]
    fn title_is_the_object_leaf_name(cx: &mut gpui::TestAppContext) {
        let (doc, window_cx, _saved) = new_test_document(cx, "logs/2026/app.log");

        window_cx.update(|_window, cx| {
            assert_eq!(doc.read(cx).title(), "app.log");
        });
    }

    /// Typing into the buffer makes the document report `Modified`, which is
    /// what routes a tab close through the unsaved-changes modal.
    #[gpui::test]
    fn editing_marks_the_document_modified(cx: &mut gpui::TestAppContext) {
        let (doc, window_cx, _saved) = new_test_document(cx, "notes.md");

        window_cx.update(|window, cx| {
            doc.update(cx, |doc, cx| {
                doc.install_buffer_for_test("hello", window, cx);
            });

            assert_eq!(doc.read(cx).state(), DocumentState::Clean);
            assert!(doc.read(cx).change_summary().is_none());

            doc.update(cx, |doc, cx| {
                doc.type_for_test(" world", window, cx);
            });
        });

        // The dirty flag is driven by the buffer's `Change` event, which the
        // effect cycle delivers rather than the `update` above.
        window_cx.run_until_parked();

        window_cx.update(|_window, cx| {
            let doc = doc.read(cx);
            assert_eq!(doc.state(), DocumentState::Modified);
            assert_eq!(
                doc.change_summary(),
                Some("Unsaved edits to notes.md".to_string())
            );
            // Closing is never blocked; the modal handles the decision.
            assert!(doc.can_close());
        });
    }

    /// Discarding restores the baseline and clears the dirty state, so the tab
    /// stops advertising an edit that no longer exists.
    #[gpui::test]
    fn discarding_restores_the_baseline(cx: &mut gpui::TestAppContext) {
        let (doc, window_cx, _saved) = new_test_document(cx, "notes.md");

        window_cx.update(|window, cx| {
            doc.update(cx, |doc, cx| {
                doc.install_buffer_for_test("baseline", window, cx);
                doc.type_for_test("edited", window, cx);
            });
        });

        window_cx.run_until_parked();

        window_cx.update(|window, cx| {
            assert!(doc.read(cx).is_dirty());

            doc.update(cx, |doc, cx| {
                doc.discard_edits(window, cx);
            });

            assert!(!doc.read(cx).is_dirty());
        });
    }

    /// A successful save clears the dirty state and tells the document that
    /// asked for the tab, so a browser previewing the same object can refresh
    /// its metadata panel.
    #[gpui::test]
    fn a_successful_save_notifies_the_opener(cx: &mut gpui::TestAppContext) {
        let (doc, window_cx, saved) = new_test_document(cx, "logs/app.log");

        window_cx.update(|window, cx| {
            doc.update(cx, |doc, cx| {
                doc.install_buffer_for_test("one", window, cx);
                doc.type_for_test("two", window, cx);
            });
        });

        window_cx.run_until_parked();

        window_cx.update(|_window, cx| {
            assert!(doc.read(cx).is_dirty());

            doc.update(cx, |doc, cx| {
                doc.apply_save_outcome("onetwo".to_string(), 6, true, cx);
            });

            let doc = doc.read(cx);
            assert!(!doc.is_dirty());
            assert_eq!(doc.byte_len(), Some(6));
        });

        assert_eq!(saved.borrow().as_slice(), ["logs/app.log".to_string()]);
    }

    /// A failed save keeps the buffer dirty so the edit can be retried, and
    /// never claims the object was written.
    #[gpui::test]
    fn a_failed_save_keeps_the_edit(cx: &mut gpui::TestAppContext) {
        let (doc, window_cx, saved) = new_test_document(cx, "logs/app.log");

        window_cx.update(|window, cx| {
            doc.update(cx, |doc, cx| {
                doc.install_buffer_for_test("one", window, cx);
                doc.type_for_test("two", window, cx);
            });
        });

        window_cx.run_until_parked();

        window_cx.update(|_window, cx| {
            doc.update(cx, |doc, cx| {
                doc.apply_save_outcome("onetwo".to_string(), 6, false, cx);
            });

            assert!(doc.read(cx).is_dirty());
        });

        assert!(saved.borrow().is_empty());
    }

    /// The dirty-tab summary reuses the object browser preview editor's
    /// `editor.unsaved_summary` catalog entry — the two editing surfaces
    /// describe an unsaved buffer with the same translated sentence.
    ///
    /// The `t!` macro has no arm combining named interpolation with an
    /// explicit `locale =` override (only `(key)` / `(key, locale=)` /
    /// `(key, name=value+)`), so the interpolated-value check and the
    /// locale-divergence check stay two separate assertions, matching the
    /// schema_diff PR 17 precedent.
    #[test]
    fn change_summary_reuses_the_shared_editor_catalog_entry() {
        let en = dbflux_i18n::t!(
            "document.object_browser.editor.unsaved_summary",
            key = "notes.md"
        );
        assert_eq!(en, "Unsaved edits to notes.md");

        let template_en = dbflux_i18n::t!(
            "document.object_browser.editor.unsaved_summary",
            locale = "en"
        );
        let template_es = dbflux_i18n::t!(
            "document.object_browser.editor.unsaved_summary",
            locale = "es"
        );
        assert_ne!(template_en, template_es);
    }

    /// The save toast reuses the shared `editor.toast.saved` catalog entry.
    #[test]
    fn save_toast_reuses_the_shared_editor_catalog_entry() {
        let en = dbflux_i18n::t!(
            "document.object_browser.editor.toast.saved",
            uri = "s3://my-bucket/notes.md"
        );
        assert_eq!(en, "Saved s3://my-bucket/notes.md");

        let template_en =
            dbflux_i18n::t!("document.object_browser.editor.toast.saved", locale = "en");
        let template_es =
            dbflux_i18n::t!("document.object_browser.editor.toast.saved", locale = "es");
        assert_ne!(template_en, template_es);
    }

    /// A refused object reports `Error` and keeps no buffer, so the tab shows
    /// the reason instead of an empty editor.
    #[gpui::test]
    fn a_refused_object_reports_an_error_state(cx: &mut gpui::TestAppContext) {
        let (doc, window_cx, _saved) = new_test_document(cx, "big.bin");

        window_cx.update(|_window, cx| {
            doc.update(cx, |doc, _cx| {
                doc.fail_load_for_test("This object is not text and cannot be edited in-app.");
            });

            let doc = doc.read(cx);
            assert_eq!(doc.state(), DocumentState::Error);
            assert!(doc.byte_len().is_none());
        });
    }
}
