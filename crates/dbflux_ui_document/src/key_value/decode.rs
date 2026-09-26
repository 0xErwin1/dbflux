//! How a string value is shown: the "View as" choice (Auto, JSON, Text,
//! MsgPack, Hex) on top of an optional decompression step.
//!
//! Detection only ever runs on the value that is open in the value pane,
//! never on the key list. Everything here is free of GPUI so the decoders,
//! the detection and the "may I edit this?" rule stay unit-testable.

use dbflux_core::{
    DecodeOutcome, DecodedPayload, Encoding, KeyGetResult, KeyLoadState, KeyType, ValueRepr,
};
use gpui::Context;
use std::borrow::Cow;

/// Values at or under this size are rendered on the foreground; larger ones
/// are decoded and laid out on the background executor.
pub(super) const INLINE_RENDER_THRESHOLD_BYTES: usize = 256 * 1024;

/// Most lines the value pane lays out for one value.
pub(super) const MAX_RENDERED_LINES: usize = 5_000;

/// Bytes shown per line of the hex view.
const HEX_BYTES_PER_LINE: usize = 16;

/// Size of the prefix read by "Preview first 64 KB".
pub(super) const VALUE_PREVIEW_BYTES: u64 = 64 * 1024;

/// How the (decompressed) bytes are shown.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(super) enum ViewAs {
    /// JSON when the bytes parse as JSON, MessagePack when they decode as
    /// MessagePack, text when they are UTF-8, hex otherwise.
    #[default]
    Auto,
    Json,
    Text,
    MsgPack,
    Hex,
}

impl ViewAs {
    pub(super) const ALL: [ViewAs; 5] = [
        ViewAs::Auto,
        ViewAs::Json,
        ViewAs::Text,
        ViewAs::MsgPack,
        ViewAs::Hex,
    ];

    pub(super) fn id(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Json => "json",
            Self::Text => "text",
            Self::MsgPack => "msgpack",
            Self::Hex => "hex",
        }
    }

    pub(super) fn from_id(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|view| view.id() == id)
    }

    pub(super) fn label(self) -> String {
        match self {
            Self::Auto => dbflux_i18n::t!("document.key_value.view_as.auto"),
            Self::Json => "JSON".to_string(),
            Self::Text => dbflux_i18n::t!("document.key_value.view_as.text"),
            Self::MsgPack => "MsgPack".to_string(),
            Self::Hex => "Hex".to_string(),
        }
    }
}

/// Decompression applied before the bytes are shown.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(super) enum Compression {
    #[default]
    None,
    Gzip,
    Zstd,
    Snappy,
    Lz4,
}

impl Compression {
    pub(super) const ALL: [Compression; 5] = [
        Compression::None,
        Compression::Gzip,
        Compression::Zstd,
        Compression::Snappy,
        Compression::Lz4,
    ];

    pub(super) fn label(self) -> String {
        match self {
            Self::None => dbflux_i18n::t!("document.key_value.view_as.compression_none"),
            Self::Gzip => "gzip".to_string(),
            Self::Zstd => "zstd".to_string(),
            Self::Snappy => "snappy".to_string(),
            Self::Lz4 => "lz4".to_string(),
        }
    }

    pub(super) fn index(self) -> usize {
        Self::ALL
            .iter()
            .position(|candidate| *candidate == self)
            .unwrap_or(0)
    }

    pub(super) fn from_index(index: usize) -> Self {
        Self::ALL.get(index).copied().unwrap_or_default()
    }

    fn encoding(self) -> Option<Encoding> {
        match self {
            Self::None => None,
            Self::Gzip => Some(Encoding::Gzip),
            Self::Zstd => Some(Encoding::Zstd),
            Self::Snappy => Some(Encoding::SnappyFrame),
            Self::Lz4 => Some(Encoding::Lz4Frame),
        }
    }

    fn for_encoding(encoding: Encoding) -> Option<Self> {
        match encoding {
            Encoding::Gzip => Some(Self::Gzip),
            Encoding::Zstd => Some(Self::Zstd),
            Encoding::SnappyFrame => Some(Self::Snappy),
            Encoding::Lz4Frame => Some(Self::Lz4),
            _ => None,
        }
    }
}

/// What detection found in the opened value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(super) struct ValueDetection {
    pub compression: Compression,
    pub format: DetectedFormat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(super) enum DetectedFormat {
    Json,
    MsgPack,
    #[default]
    Text,
    Binary,
    Image(Encoding),
}

impl DetectedFormat {
    /// Label of the "… detected" chip in the value header, for the formats
    /// worth calling out.
    pub(super) fn detected_label(self) -> Option<String> {
        match self {
            Self::Json => Some(dbflux_i18n::t!("document.key_value.view_as.detected.json")),
            Self::MsgPack => Some(dbflux_i18n::t!(
                "document.key_value.view_as.detected.msgpack"
            )),
            Self::Image(encoding) => Some(dbflux_i18n::t!(
                "document.key_value.view_as.detected.image",
                format = encoding_name(encoding)
            )),
            Self::Text | Self::Binary => None,
        }
    }
}

/// Detects the compression and format of a fully loaded value.
///
/// A partially transferred value is never decompressed: a truncated stream
/// would decode into garbage.
pub(super) fn detect_value(raw: &[u8], load_state: KeyLoadState, cap: usize) -> ValueDetection {
    let fully_loaded = matches!(load_state, KeyLoadState::Loaded);

    let compression = match dbflux_core::detect(raw) {
        Some(encoding) if fully_loaded => Compression::for_encoding(encoding),
        _ => None,
    };

    if let Some(compression) = compression {
        let format = match decompress(raw, compression, cap) {
            Ok(bytes) => classify_bytes(&bytes),
            Err(_) => DetectedFormat::Binary,
        };

        return ValueDetection {
            compression,
            format,
        };
    }

    let format = match dbflux_core::detect(raw) {
        Some(encoding) if encoding.is_image() => DetectedFormat::Image(encoding),
        _ => classify_bytes(raw),
    };

    ValueDetection {
        compression: Compression::None,
        format,
    }
}

fn classify_bytes(bytes: &[u8]) -> DetectedFormat {
    match std::str::from_utf8(bytes) {
        Ok(text) if parses_as_json_document(text) => DetectedFormat::Json,
        Ok(_) => DetectedFormat::Text,
        Err(_) if dbflux_core::probe_message_pack(bytes) => DetectedFormat::MsgPack,
        Err(_) => DetectedFormat::Binary,
    }
}

/// Whether `text` is a JSON object or array. Bare scalars (`12`, `"x"`,
/// `true`) are plain text for display purposes.
pub(super) fn parses_as_json_document(text: &str) -> bool {
    let trimmed = text.trim_start();

    (trimmed.starts_with('{') || trimmed.starts_with('['))
        && serde_json::from_str::<serde_json::Value>(text).is_ok()
}

fn decompress(raw: &[u8], compression: Compression, cap: usize) -> Result<Cow<'_, [u8]>, String> {
    let Some(encoding) = compression.encoding() else {
        return Ok(Cow::Borrowed(raw));
    };

    match dbflux_core::decode_as(raw, encoding, cap) {
        DecodeOutcome::Decoded(decoded) => match decoded.payload {
            DecodedPayload::Bytes(bytes) => Ok(Cow::Owned(bytes)),
            DecodedPayload::Text(text) => Ok(Cow::Owned(text.into_bytes())),
            DecodedPayload::PassThrough => Ok(Cow::Borrowed(raw)),
        },
        DecodeOutcome::DetectedButFailed { reason, .. } => Err(dbflux_i18n::t!(
            "document.key_value.view_as.error.decompress",
            compression = compression.label(),
            reason = reason
        )),
        DecodeOutcome::TooLarge { limit_bytes, .. } => Err(dbflux_i18n::t!(
            "document.key_value.view_as.error.too_large",
            limit = super::metadata::format_size(limit_bytes as u64)
        )),
        DecodeOutcome::Undetected => Ok(Cow::Borrowed(raw)),
    }
}

/// Role of a piece of a rendered line, which picks its color.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SpanKind {
    Plain,
    /// An object key.
    Key,
    /// `true`, `false` and `null`.
    Literal,
    /// Hex view offsets and the ASCII column.
    Dim,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ValueSpan {
    pub text: String,
    pub kind: SpanKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ValueLine {
    pub indent: usize,
    pub spans: Vec<ValueSpan>,
}

impl ValueLine {
    fn plain(indent: usize, text: impl Into<String>) -> Self {
        Self {
            indent,
            spans: vec![ValueSpan {
                text: text.into(),
                kind: SpanKind::Plain,
            }],
        }
    }
}

/// The value laid out for the value pane.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(super) struct RenderedValue {
    pub lines: Vec<ValueLine>,
    /// A message shown above the lines: a decoding error, or the fact that
    /// the output was cut at [`MAX_RENDERED_LINES`].
    pub notice: Option<String>,
}

/// Lays out `raw` for display under the chosen view and compression.
pub(super) fn render_value(
    raw: &[u8],
    view: ViewAs,
    compression: Compression,
    cap: usize,
) -> RenderedValue {
    let bytes = match decompress(raw, compression, cap) {
        Ok(bytes) => bytes,
        Err(message) => {
            let mut rendered = hex_lines(raw);
            rendered.notice = Some(message);
            return rendered;
        }
    };

    match view {
        ViewAs::Auto => match classify_bytes(&bytes) {
            DetectedFormat::Json => json_lines_from_text(&bytes),
            DetectedFormat::MsgPack => msgpack_lines(&bytes),
            DetectedFormat::Text => text_lines(&bytes),
            DetectedFormat::Binary | DetectedFormat::Image(_) => hex_lines(&bytes),
        },
        ViewAs::Json => json_lines_from_text(&bytes),
        ViewAs::Text => text_lines(&bytes),
        ViewAs::MsgPack => msgpack_lines(&bytes),
        ViewAs::Hex => hex_lines(&bytes),
    }
}

fn json_lines_from_text(bytes: &[u8]) -> RenderedValue {
    let parsed = std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok());

    match parsed {
        Some(value) => json_lines(&value),
        None => {
            let mut rendered = text_lines(bytes);
            rendered.notice = Some(dbflux_i18n::t!("document.key_value.view_as.error.not_json"));
            rendered
        }
    }
}

fn msgpack_lines(bytes: &[u8]) -> RenderedValue {
    match dbflux_core::decode_as(bytes, Encoding::MessagePack, usize::MAX) {
        DecodeOutcome::Decoded(decoded) => match decoded.payload {
            DecodedPayload::Text(text) => json_lines_from_text(text.as_bytes()),
            DecodedPayload::Bytes(bytes) => text_lines(&bytes),
            DecodedPayload::PassThrough => hex_lines(bytes),
        },
        _ => {
            let mut rendered = hex_lines(bytes);
            rendered.notice = Some(dbflux_i18n::t!(
                "document.key_value.view_as.error.not_msgpack"
            ));
            rendered
        }
    }
}

fn text_lines(bytes: &[u8]) -> RenderedValue {
    let text = String::from_utf8_lossy(bytes);
    let mut lines: Vec<ValueLine> = text
        .lines()
        .take(MAX_RENDERED_LINES + 1)
        .map(|line| ValueLine::plain(0, line))
        .collect();

    if lines.is_empty() {
        lines.push(ValueLine::plain(0, ""));
    }

    cap_lines(RenderedValue {
        lines,
        notice: None,
    })
}

/// Classic 16-bytes-per-line hex dump: offset, byte pairs, ASCII column.
fn hex_lines(bytes: &[u8]) -> RenderedValue {
    let lines = bytes
        .chunks(HEX_BYTES_PER_LINE)
        .take(MAX_RENDERED_LINES + 1)
        .enumerate()
        .map(|(line_index, chunk)| {
            let offset = format!("{:08x}", line_index * HEX_BYTES_PER_LINE);

            let hex: Vec<String> = chunk.iter().map(|byte| format!("{byte:02x}")).collect();
            let padded_hex = format!("{:<width$}", hex.join(" "), width = HEX_BYTES_PER_LINE * 3);

            let ascii: String = chunk
                .iter()
                .map(|byte| {
                    if byte.is_ascii_graphic() || *byte == b' ' {
                        *byte as char
                    } else {
                        '.'
                    }
                })
                .collect();

            ValueLine {
                indent: 0,
                spans: vec![
                    ValueSpan {
                        text: format!("{offset}  "),
                        kind: SpanKind::Dim,
                    },
                    ValueSpan {
                        text: padded_hex,
                        kind: SpanKind::Plain,
                    },
                    ValueSpan {
                        text: format!(" {ascii}"),
                        kind: SpanKind::Dim,
                    },
                ],
            }
        })
        .collect();

    cap_lines(RenderedValue {
        lines,
        notice: None,
    })
}

/// Pretty-prints a JSON value as indented lines with keys and literals
/// marked for coloring.
pub(super) fn json_lines(value: &serde_json::Value) -> RenderedValue {
    let mut lines = Vec::new();
    push_json(value, None, 0, false, &mut lines);

    cap_lines(RenderedValue {
        lines,
        notice: None,
    })
}

fn push_json(
    value: &serde_json::Value,
    key: Option<&str>,
    indent: usize,
    trailing_comma: bool,
    lines: &mut Vec<ValueLine>,
) {
    if lines.len() > MAX_RENDERED_LINES {
        return;
    }

    let comma = if trailing_comma { "," } else { "" };
    let mut spans = Vec::new();

    if let Some(key) = key {
        spans.push(ValueSpan {
            text: serde_json::Value::String(key.to_string()).to_string(),
            kind: SpanKind::Key,
        });
        spans.push(ValueSpan {
            text: ": ".to_string(),
            kind: SpanKind::Plain,
        });
    }

    match value {
        serde_json::Value::Object(map) if !map.is_empty() => {
            spans.push(ValueSpan {
                text: "{".to_string(),
                kind: SpanKind::Plain,
            });
            lines.push(ValueLine { indent, spans });

            let last = map.len().saturating_sub(1);
            for (index, (child_key, child)) in map.iter().enumerate() {
                push_json(child, Some(child_key), indent + 1, index < last, lines);
            }

            lines.push(ValueLine::plain(indent, format!("}}{comma}")));
        }
        serde_json::Value::Array(items) if !items.is_empty() => {
            spans.push(ValueSpan {
                text: "[".to_string(),
                kind: SpanKind::Plain,
            });
            lines.push(ValueLine { indent, spans });

            let last = items.len().saturating_sub(1);
            for (index, item) in items.iter().enumerate() {
                push_json(item, None, indent + 1, index < last, lines);
            }

            lines.push(ValueLine::plain(indent, format!("]{comma}")));
        }
        scalar => {
            let kind = match scalar {
                serde_json::Value::Bool(_) | serde_json::Value::Null => SpanKind::Literal,
                _ => SpanKind::Plain,
            };

            spans.push(ValueSpan {
                text: scalar.to_string(),
                kind,
            });

            if !comma.is_empty() {
                spans.push(ValueSpan {
                    text: comma.to_string(),
                    kind: SpanKind::Plain,
                });
            }

            lines.push(ValueLine { indent, spans });
        }
    }
}

/// Splits a single-line JSON value into colored spans for a table cell:
/// object keys, literals and everything else.
pub(super) fn inline_json_spans(text: &str) -> Vec<ValueSpan> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        return vec![ValueSpan {
            text: text.to_string(),
            kind: SpanKind::Plain,
        }];
    };

    let mut spans = Vec::new();
    push_inline_json(&value, &mut spans);
    spans
}

fn push_inline_json(value: &serde_json::Value, spans: &mut Vec<ValueSpan>) {
    let plain = |text: &str| ValueSpan {
        text: text.to_string(),
        kind: SpanKind::Plain,
    };

    match value {
        serde_json::Value::Object(map) => {
            spans.push(plain("{"));
            for (index, (key, child)) in map.iter().enumerate() {
                if index > 0 {
                    spans.push(plain(", "));
                }
                spans.push(ValueSpan {
                    text: serde_json::Value::String(key.clone()).to_string(),
                    kind: SpanKind::Key,
                });
                spans.push(plain(": "));
                push_inline_json(child, spans);
            }
            spans.push(plain("}"));
        }
        serde_json::Value::Array(items) => {
            spans.push(plain("["));
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    spans.push(plain(", "));
                }
                push_inline_json(item, spans);
            }
            spans.push(plain("]"));
        }
        serde_json::Value::Bool(_) | serde_json::Value::Null => spans.push(ValueSpan {
            text: value.to_string(),
            kind: SpanKind::Literal,
        }),
        scalar => spans.push(plain(&scalar.to_string())),
    }
}

fn cap_lines(mut rendered: RenderedValue) -> RenderedValue {
    if rendered.lines.len() > MAX_RENDERED_LINES {
        rendered.lines.truncate(MAX_RENDERED_LINES);
        rendered.notice = Some(dbflux_i18n::t!(
            "document.key_value.view_as.truncated_lines",
            count = MAX_RENDERED_LINES
        ));
    }

    rendered
}

/// Whether the value editor may open.
///
/// Only the raw bytes are ever written back, so editing is allowed only
/// while the pane shows exactly those bytes as text: no decompression, no
/// MessagePack or hex rendering, and valid UTF-8.
pub(super) fn may_edit_value(
    key_type: KeyType,
    value: &KeyGetResult,
    view: ViewAs,
    compression: Compression,
) -> bool {
    matches!(key_type, KeyType::String | KeyType::Json)
        && matches!(value.load_state, KeyLoadState::Loaded)
        && compression == Compression::None
        && matches!(view, ViewAs::Auto | ViewAs::Json | ViewAs::Text)
        && value.repr != ValueRepr::Binary
        && std::str::from_utf8(&value.value).is_ok()
}

pub(super) fn encoding_name(encoding: Encoding) -> &'static str {
    match encoding {
        Encoding::Gzip => "gzip",
        Encoding::Zstd => "zstd",
        Encoding::SnappyFrame => "snappy",
        Encoding::Lz4Frame => "lz4",
        Encoding::MessagePack => "msgpack",
        Encoding::Png => "png",
        Encoding::Jpeg => "jpeg",
        Encoding::Gif => "gif",
        Encoding::WebP => "webp",
        Encoding::Bmp => "bmp",
    }
}

impl super::KeyValueDocument {
    /// Byte cap for the value fetch (the size gate) and for decompressed
    /// output (the decompression bomb guard).
    pub(super) fn kv_size_limit_bytes(&self, cx: &gpui::App) -> u64 {
        self.app_state
            .read(cx)
            .general_settings()
            .key_value_size_limit_bytes()
    }

    /// Requests the selected key again without the size limit, once.
    pub(super) fn load_selected_value_without_limit(&mut self, cx: &mut Context<Self>) {
        self.kv_load_anyway = true;
        self.reload_selected_value(cx);
    }

    /// Detects the opened value's compression and format, resets the view
    /// to Auto with the detected compression, and lays it out.
    pub(super) fn reset_value_view_for_new_value(&mut self, cx: &mut Context<Self>) {
        let cap = self.value_decode_cap(cx);

        self.value_detection = self
            .selected_value
            .as_ref()
            .filter(|value| !is_structured_repr(value.repr))
            .map(|value| detect_value(&value.value, value.load_state, cap))
            .unwrap_or_default();

        self.value_view_as = ViewAs::Auto;
        self.value_compression = self.value_detection.compression;

        let compression_index = self.value_compression.index();
        self.compression_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(Some(compression_index), cx);
        });

        self.recompute_rendered_value(cx);
    }

    pub(super) fn set_value_view_as(&mut self, view: ViewAs, cx: &mut Context<Self>) {
        if self.value_view_as == view {
            return;
        }

        self.value_view_as = view;
        self.recompute_rendered_value(cx);
        cx.notify();
    }

    pub(super) fn set_value_compression(
        &mut self,
        compression: Compression,
        cx: &mut Context<Self>,
    ) {
        if self.value_compression == compression {
            return;
        }

        self.value_compression = compression;
        self.recompute_rendered_value(cx);
        cx.notify();
    }

    fn value_decode_cap(&self, cx: &gpui::App) -> usize {
        self.kv_size_limit_bytes(cx).min(usize::MAX as u64) as usize
    }

    /// Lays out the opened value again. Large values are processed on the
    /// background executor; a result that lands after the user moved on is
    /// dropped by the generation check.
    pub(super) fn recompute_rendered_value(&mut self, cx: &mut Context<Self>) {
        self.value_render_generation = self.value_render_generation.wrapping_add(1);
        let generation = self.value_render_generation;

        let Some(value) = self
            .selected_value
            .as_ref()
            .filter(|value| !is_structured_repr(value.repr))
            .filter(|value| !matches!(value.load_state, KeyLoadState::TooLarge { .. }))
        else {
            self.rendered_value = None;
            return;
        };

        let view = self.value_view_as;
        let compression = self.value_compression;
        let cap = self.value_decode_cap(cx);

        if value.value.len() <= INLINE_RENDER_THRESHOLD_BYTES {
            self.rendered_value = Some(render_value(&value.value, view, compression, cap));
            return;
        }

        self.rendered_value = None;
        let bytes = value.value.clone();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let rendered = cx
                .background_executor()
                .spawn(async move { render_value(&bytes, view, compression, cap) })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    if this.value_render_generation != generation {
                        return;
                    }
                    this.rendered_value = Some(rendered);
                    cx.notify();
                });
            });
        })
        .detach();
    }
}

fn is_structured_repr(repr: ValueRepr) -> bool {
    matches!(repr, ValueRepr::Structured | ValueRepr::Stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn line_text(line: &ValueLine) -> String {
        line.spans.iter().map(|span| span.text.as_str()).collect()
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(bytes).expect("gzip write");
        encoder.finish().expect("gzip finish")
    }

    fn string_value(bytes: &[u8], repr: ValueRepr) -> KeyGetResult {
        KeyGetResult {
            entry: dbflux_core::KeyEntry::new("k"),
            value: bytes.to_vec(),
            repr,
            load_state: KeyLoadState::Loaded,
        }
    }

    #[test]
    fn detection_finds_json_documents_but_not_bare_scalars() {
        let json = detect_value(br#"{"enabled": true}"#, KeyLoadState::Loaded, 1024);
        let scalar = detect_value(b"12", KeyLoadState::Loaded, 1024);

        assert_eq!(json.format, DetectedFormat::Json);
        assert_eq!(json.compression, Compression::None);
        assert_eq!(scalar.format, DetectedFormat::Text);
    }

    #[test]
    fn detection_sees_through_gzip() {
        let compressed = gzip(br#"{"rollout": 0.25, "enabled": true, "regions": ["eu"]}"#);

        let detection = detect_value(&compressed, KeyLoadState::Loaded, 1 << 20);

        assert_eq!(detection.compression, Compression::Gzip);
        assert_eq!(detection.format, DetectedFormat::Json);
    }

    #[test]
    fn detection_never_decompresses_a_partial_value() {
        let compressed = gzip(b"hello hello hello hello hello");
        let partial = KeyLoadState::Truncated {
            returned_bytes: 10,
            total_bytes: Some(100),
        };

        let detection = detect_value(&compressed, partial, 1 << 20);

        assert_eq!(detection.compression, Compression::None);
    }

    #[test]
    fn detection_recognises_message_pack() {
        let packed = rmp_serde::to_vec_named(&serde_json::json!({"a": [1, 2, 3], "b": "x"}))
            .expect("encode msgpack");

        let detection = detect_value(&packed, KeyLoadState::Loaded, 1024);

        assert_eq!(detection.format, DetectedFormat::MsgPack);
    }

    #[test]
    fn json_view_pretty_prints_with_keys_and_literals_marked() {
        let rendered = render_value(
            br#"{"b":{"enabled":true,"rollout":0.25},"a":[1,null]}"#,
            ViewAs::Json,
            Compression::None,
            1024,
        );

        let texts: Vec<(usize, String)> = rendered
            .lines
            .iter()
            .map(|line| (line.indent, line_text(line)))
            .collect();

        assert_eq!(
            texts,
            vec![
                (0, "{".to_string()),
                (1, "\"b\": {".to_string()),
                (2, "\"enabled\": true,".to_string()),
                (2, "\"rollout\": 0.25".to_string()),
                (1, "},".to_string()),
                (1, "\"a\": [".to_string()),
                (2, "1,".to_string()),
                (2, "null".to_string()),
                (1, "]".to_string()),
                (0, "}".to_string()),
            ]
        );

        let enabled_line = &rendered.lines[2];
        assert_eq!(enabled_line.spans[0].kind, SpanKind::Key);
        assert_eq!(enabled_line.spans[2].kind, SpanKind::Literal);
        assert_eq!(rendered.notice, None);
    }

    #[test]
    fn json_view_falls_back_to_text_with_a_notice() {
        let rendered = render_value(b"not json", ViewAs::Json, Compression::None, 1024);

        assert_eq!(line_text(&rendered.lines[0]), "not json");
        assert!(rendered.notice.is_some());
    }

    #[test]
    fn hex_view_dumps_sixteen_bytes_per_line_with_ascii() {
        let rendered = render_value(b"0123456789abcdefXY", ViewAs::Hex, Compression::None, 1024);

        assert_eq!(rendered.lines.len(), 2);
        assert!(line_text(&rendered.lines[0]).starts_with("00000000  30 31 32"));
        assert!(line_text(&rendered.lines[0]).ends_with(" 0123456789abcdef"));
        assert!(line_text(&rendered.lines[1]).starts_with("00000010  58 59"));
    }

    #[test]
    fn auto_view_decompresses_then_shows_json() {
        let compressed = gzip(br#"{"k": 1}"#);

        let rendered = render_value(&compressed, ViewAs::Auto, Compression::Gzip, 1 << 20);

        assert_eq!(line_text(&rendered.lines[1]), "\"k\": 1");
    }

    #[test]
    fn a_wrong_compression_choice_shows_the_raw_bytes_with_an_error() {
        let rendered = render_value(b"plain text value", ViewAs::Auto, Compression::Zstd, 1024);

        assert!(rendered.notice.is_some());
        assert!(line_text(&rendered.lines[0]).starts_with("00000000"));
    }

    #[test]
    fn msgpack_view_decodes_to_json_lines() {
        let packed =
            rmp_serde::to_vec_named(&serde_json::json!({"name": "kenji"})).expect("encode");

        let rendered = render_value(&packed, ViewAs::MsgPack, Compression::None, 1024);

        assert_eq!(line_text(&rendered.lines[1]), "\"name\": \"kenji\"");
    }

    #[test]
    fn text_view_caps_the_number_of_lines() {
        let long = "line\n".repeat(MAX_RENDERED_LINES + 10);

        let rendered = render_value(long.as_bytes(), ViewAs::Text, Compression::None, 1 << 30);

        assert_eq!(rendered.lines.len(), MAX_RENDERED_LINES);
        assert!(rendered.notice.is_some());
    }

    #[test]
    fn inline_json_spans_color_keys_and_literals() {
        let spans = inline_json_spans(r#"{"theme":"dark","digest":null}"#);

        let keys: Vec<&str> = spans
            .iter()
            .filter(|span| span.kind == SpanKind::Key)
            .map(|span| span.text.as_str())
            .collect();

        assert_eq!(keys, vec!["\"theme\"", "\"digest\""]);
        assert!(spans.iter().any(|span| span.kind == SpanKind::Literal));
    }

    #[test]
    fn editing_is_only_allowed_while_the_raw_text_is_on_screen() {
        let text = string_value(b"{\"a\":1}", ValueRepr::Json);
        let binary = string_value(&[0xff, 0xfe], ValueRepr::Binary);

        assert!(may_edit_value(
            KeyType::String,
            &text,
            ViewAs::Json,
            Compression::None
        ));
        assert!(!may_edit_value(
            KeyType::String,
            &text,
            ViewAs::Hex,
            Compression::None
        ));
        assert!(!may_edit_value(
            KeyType::String,
            &text,
            ViewAs::Auto,
            Compression::Gzip
        ));
        assert!(!may_edit_value(
            KeyType::String,
            &binary,
            ViewAs::Text,
            Compression::None
        ));
        assert!(!may_edit_value(
            KeyType::Hash,
            &text,
            ViewAs::Auto,
            Compression::None
        ));
    }

    #[test]
    fn view_as_ids_round_trip() {
        for view in ViewAs::ALL {
            assert_eq!(ViewAs::from_id(view.id()), Some(view));
        }
    }

    #[test]
    fn compression_indices_round_trip() {
        for compression in Compression::ALL {
            assert_eq!(Compression::from_index(compression.index()), compression);
        }
        assert_eq!(Compression::from_index(99), Compression::None);
    }
}
