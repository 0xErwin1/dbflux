//! Shared building blocks for editing an object-store text object.
//!
//! Both editing surfaces use these: the object browser's pinned preview-pane
//! editor (`object_browser/editor.rs`) and the standalone editor tab
//! (`object_editor/`). Everything here is host-agnostic — decoding, the
//! highlighter gate, buffer construction, the save audit record, and the
//! error mapping — so the two surfaces cannot drift apart in how they read,
//! render, or write an object.

use dbflux_components::controls::{InputSearch, InputState};
use dbflux_core::DbError;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError};
use gpui::{AppContext, Context, Entity, Window};
use uuid::Uuid;

/// Save shortcut label, matching the `SaveQuery` binding (Cmd+S on macOS,
/// Ctrl+S elsewhere) that both editors answer to.
#[cfg(target_os = "macos")]
pub const SAVE_SHORTCUT_HINT: &str = "Cmd+S";
#[cfg(not(target_os = "macos"))]
pub const SAVE_SHORTCUT_HINT: &str = "Ctrl+S";

/// Find shortcut label. The binding itself belongs to `gpui-component`'s
/// input, which owns `cmd-f` / `ctrl-f` inside its own `Input` key context.
#[cfg(target_os = "macos")]
pub const FIND_SHORTCUT_HINT: &str = "Cmd+F";
#[cfg(not(target_os = "macos"))]
pub const FIND_SHORTCUT_HINT: &str = "Ctrl+F";

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
///
/// Dotenv files (`.env`, `.env.local`, `production.env`, ...) have no
/// registered grammar of their own, but their `KEY=value` + `#` comment shape
/// is shell syntax, so they open with the bash highlighter.
pub fn editor_language(key: &str) -> String {
    let name = key.rsplit_once('/').map(|(_, name)| name).unwrap_or(key);
    let lowercase_name = name.to_lowercase();

    if lowercase_name == ".env"
        || lowercase_name.starts_with(".env.")
        || lowercase_name.ends_with(".env")
    {
        return "bash".to_string();
    }

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

/// Builds the editable buffer for `key`'s decoded `body`.
///
/// Plain buffers exist because the body tripped the highlight gate — usually
/// one enormous minified line. Without wrapping, that line makes click
/// positioning and horizontal navigation unusable. Both shapes are marked
/// searchable so the component's find panel (`cmd-f` / `ctrl-f`) is available
/// in either; `code_editor` already opts in, the plain buffer does not.
pub fn build_text_input<T: 'static>(
    key: &str,
    body: &str,
    window: &mut Window,
    cx: &mut Context<T>,
) -> Entity<InputState> {
    let language = highlight_language(key, body);

    cx.new(|cx| {
        let state = InputState::new(window, cx);

        match language {
            Some(language) => state
                .code_editor(language)
                .line_number(true)
                .soft_wrap(false),
            None => state
                .multi_line(true)
                .searchable(true)
                .line_number(true)
                .soft_wrap(true),
        }
    })
}

/// Opens the editor component's find panel for `input`.
///
/// The panel is owned by `gpui-component` and is only reachable through its
/// `Search` action, so the button path focuses the buffer first and then
/// dispatches the same action its `cmd-f` / `ctrl-f` binding raises.
pub fn open_find_panel(input: &Entity<InputState>, window: &mut Window, cx: &mut gpui::App) {
    input.update(cx, |state, cx| state.focus(window, cx));
    window.dispatch_action(Box::new(InputSearch), cx);
}

/// Whether `metadata`'s object may be opened in an in-app text editor.
///
/// Returns the reason it may not, ready to be shown in place of the buffer.
/// This is the preview gate — the configured size limit and the archived
/// storage tiers — plus the text-kind check, so an editor never fetches a body
/// the preview pane would have refused.
pub fn detect_editable_text(
    metadata: &dbflux_core::ObjectMetadata,
    limit_bytes: u64,
) -> Result<(), String> {
    use crate::object_browser::{PreviewGate, PreviewKind, detect_preview_kind};

    let gate = crate::object_browser::evaluate_preview_gate(metadata, limit_bytes);

    if gate != PreviewGate::Allowed {
        return Err(gate
            .message()
            .unwrap_or_else(|| "This object cannot be opened in-app.".to_string()));
    }

    if detect_preview_kind(metadata.content_type.as_deref(), &metadata.key) != PreviewKind::Text {
        return Err("This object is not text and cannot be edited in-app.".to_string());
    }

    Ok(())
}

/// Meta line describing a loaded object body: what it is, how big it is, and
/// how its text is encoded.
pub fn body_meta_line(
    content_type: Option<&str>,
    byte_len: u64,
    line_ending: LineEnding,
) -> String {
    format!(
        "{} · {} · UTF-8 · {}",
        content_type.unwrap_or("text/plain"),
        crate::buckets_table::format_bytes(byte_len),
        line_ending.label()
    )
}

/// `Ln n, Col n` from the editor's 0-based cursor position.
pub fn cursor_label(position: dbflux_components::controls::InputPosition) -> String {
    format!("Ln {}, Col {}", position.line + 1, position.character + 1)
}

pub fn db_error_to_user_facing(err: &DbError) -> UserFacingError {
    match err.formatted() {
        Some(formatted) => UserFacingError::from_formatted(ErrorKind::Driver, formatted.clone()),
        None => UserFacingError::new(ErrorKind::Driver, err.to_string()),
    }
}

/// Audits a save-back. Only the bucket, key, and outcome are recorded — never
/// the object's content. Both editing surfaces record through here so a save
/// is indistinguishable in the audit trail regardless of where it came from.
pub fn record_save_audit(
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
        log::warn!("[object text] failed to record object-edit audit event: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LineEnding, body_meta_line, cursor_label, decode_text_body, editor_language,
        highlight_language,
    };

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

    /// Dotenv files open with the bash highlighter in every naming shape:
    /// bare `.env`, suffixed `.env.<stage>`, and prefixed `<stage>.env`.
    #[test]
    fn dotenv_files_highlight_as_bash() {
        assert_eq!(editor_language("config/.env"), "bash");
        assert_eq!(editor_language("config/.env.production"), "bash");
        assert_eq!(editor_language("config/staging.env"), "bash");
        assert_eq!(editor_language(".ENV"), "bash");
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

    /// The meta line falls back to `text/plain` when the object reported no
    /// content type, so the strip never shows an empty field.
    #[test]
    fn meta_line_falls_back_to_text_plain() {
        assert_eq!(
            body_meta_line(None, 1024, LineEnding::Crlf),
            "text/plain · 1.0 KiB · UTF-8 · CRLF"
        );
        assert_eq!(
            body_meta_line(Some("application/json"), 512, LineEnding::Lf),
            "application/json · 512 B · UTF-8 · LF"
        );
    }
}
