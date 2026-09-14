//! What an object's body is, and the state of the bytes fetched for it.
//!
//! Pure model + formatting. The fetch/decode plumbing lives in `data.rs` and
//! the rendering in `preview.rs`. The preview *gate* (`metadata.rs`) decides
//! whether bytes may be fetched at all; this module decides what to do with
//! them once they are allowed.

use crate::buckets_table::format_bytes;
use dbflux_core::{DecodeOutcome, DecodedPayload, Encoding, decode_as, detect};
use gpui::{Image, ImageFormat};
use std::sync::Arc;

/// How a previewable object is presented.
///
/// SVG rides the `Image` path too: gpui renders it natively, but it skips the
/// raster validation decode — `dimensions` stays `None` for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreviewKind {
    Image(ImageFormat),
    Text,
    Pdf,
    Binary,
}

impl PreviewKind {
    pub fn image_format(self) -> Option<ImageFormat> {
        match self {
            PreviewKind::Image(format) => Some(format),
            _ => None,
        }
    }
}

/// Decides how to present an object from its reported content type, falling
/// back to its extension.
///
/// The content type wins when it says something meaningful, but S3 objects are
/// routinely stored as `application/octet-stream`, so a generic type defers to
/// the key's extension rather than condemning the object to the binary path.
pub fn detect_preview_kind(content_type: Option<&str>, key: &str) -> PreviewKind {
    kind_from_content_type(content_type)
        .or_else(|| kind_from_extension(key))
        .unwrap_or(PreviewKind::Binary)
}

/// A user's explicit choice of how to interpret a fetched body, overriding
/// magic-byte auto-detection. `Raw` forces the extension/content-type
/// fallback even when the bytes carry a recognized magic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodingChoice {
    Raw,
    Encoding(Encoding),
}

/// What produced a text preview: the object's own raw bytes, or a value
/// decoded from them.
///
/// Only `Raw` text may be edited and saved back — writing back a decoded view
/// would silently replace the object's real bytes (compressed, MessagePack,
/// ...) with a re-encoding of its decoded form, which `decode`/`decode_as`
/// never attempt in the other direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextSource {
    Raw,
    Decoded(Encoding),
}

impl TextSource {
    pub fn is_editable(self) -> bool {
        matches!(self, TextSource::Raw)
    }
}

/// The fully resolved presentation of a fetched object body: what
/// [`dbflux_core::detect`]/[`dbflux_core::decode_as`] (or the extension/
/// content-type fallback) says the bytes are, and how to show them.
#[derive(Clone, Debug, PartialEq)]
pub enum ResolvedBody {
    Image(ImageFormat),
    Text {
        text: String,
        source: TextSource,
    },
    Pdf,
    Binary,
    /// A magic byte was recognized but decoding it failed. Informative, not
    /// an error: the raw bytes are still there to download.
    DecodeFailed {
        encoding: Encoding,
        reason: String,
    },
    /// A magic byte was recognized but the decoded size exceeds the preview
    /// limit.
    DecodeTooLarge {
        encoding: Encoding,
        limit_bytes: usize,
    },
}

/// Display name for an [`Encoding`], used in the "gzip → JSON"-style label
/// and the encoding-override picker.
pub fn encoding_label(encoding: Encoding) -> &'static str {
    match encoding {
        Encoding::Gzip => "gzip",
        Encoding::Zstd => "zstd",
        Encoding::SnappyFrame => "snappy",
        Encoding::Lz4Frame => "lz4",
        Encoding::MessagePack => "MessagePack",
        Encoding::Png => "PNG",
        Encoding::Jpeg => "JPEG",
        Encoding::Gif => "GIF",
        Encoding::WebP => "WebP",
        Encoding::Bmp => "BMP",
    }
}

/// Every non-image encoding a user may pick from the override control. Image
/// encodings are excluded: they carry no separate decoded form to choose.
pub const OVERRIDABLE_ENCODINGS: [Encoding; 5] = [
    Encoding::Gzip,
    Encoding::Zstd,
    Encoding::SnappyFrame,
    Encoding::Lz4Frame,
    Encoding::MessagePack,
];

/// Coarse label for decoded text: `JSON` when it looks like a JSON document,
/// `Text` otherwise. A heuristic, not a parse — good enough for the "gzip →
/// JSON" header label, not load-bearing for anything else.
fn text_kind_label(text: &str) -> &'static str {
    let trimmed = text.trim_start();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        "JSON"
    } else {
        "Text"
    }
}

/// The "gzip → JSON"-style label for a resolved text preview, or `None` when
/// the text is the object's own raw bytes.
pub fn decode_label(text: &str, source: TextSource) -> Option<String> {
    match source {
        TextSource::Raw => None,
        TextSource::Decoded(encoding) => Some(format!(
            "{} → {}",
            encoding_label(encoding),
            text_kind_label(text)
        )),
    }
}

fn image_format_for_encoding(encoding: Encoding) -> Option<ImageFormat> {
    match encoding {
        Encoding::Png => Some(ImageFormat::Png),
        Encoding::Jpeg => Some(ImageFormat::Jpeg),
        Encoding::Gif => Some(ImageFormat::Gif),
        Encoding::WebP => Some(ImageFormat::Webp),
        Encoding::Bmp => Some(ImageFormat::Bmp),
        _ => None,
    }
}

/// Resolves how to present a fetched object body: magic-byte detection first
/// (or the caller's explicit `override_choice`), falling back to the
/// extension/content-type inference (`detect_preview_kind`) only when nothing
/// is detected.
///
/// `max_output_bytes` bounds a compressed/MessagePack decode exactly like the
/// preview size gate bounds the transfer itself, so decoding can never turn a
/// small allowed object into an unbounded in-memory blow-up.
pub fn resolve_body(
    bytes: &[u8],
    content_type: Option<&str>,
    key: &str,
    max_output_bytes: usize,
    override_choice: Option<EncodingChoice>,
) -> ResolvedBody {
    match override_choice {
        Some(EncodingChoice::Raw) => resolve_fallback(bytes, content_type, key),
        Some(EncodingChoice::Encoding(encoding)) => resolve_decode_outcome(
            decode_as(bytes, encoding, max_output_bytes),
            bytes,
            content_type,
            key,
        ),
        None => match detect(bytes) {
            Some(encoding) => resolve_decode_outcome(
                decode_as(bytes, encoding, max_output_bytes),
                bytes,
                content_type,
                key,
            ),
            None => resolve_fallback(bytes, content_type, key),
        },
    }
}

fn resolve_decode_outcome(
    outcome: DecodeOutcome,
    bytes: &[u8],
    content_type: Option<&str>,
    key: &str,
) -> ResolvedBody {
    match outcome {
        DecodeOutcome::Decoded(value) => match value.payload {
            DecodedPayload::PassThrough => image_format_for_encoding(value.encoding)
                .map(ResolvedBody::Image)
                .unwrap_or(ResolvedBody::Binary),
            DecodedPayload::Bytes(decoded) => match String::from_utf8(decoded) {
                Ok(text) => ResolvedBody::Text {
                    text,
                    source: TextSource::Decoded(value.encoding),
                },
                Err(_) => ResolvedBody::DecodeFailed {
                    encoding: value.encoding,
                    reason: dbflux_i18n::t!(
                        "document.object_browser.preview.body.decoded_not_utf8"
                    ),
                },
            },
            DecodedPayload::Text(text) => ResolvedBody::Text {
                text,
                source: TextSource::Decoded(value.encoding),
            },
        },
        DecodeOutcome::DetectedButFailed { encoding, reason } => {
            ResolvedBody::DecodeFailed { encoding, reason }
        }
        DecodeOutcome::TooLarge {
            encoding,
            limit_bytes,
        } => ResolvedBody::DecodeTooLarge {
            encoding,
            limit_bytes,
        },
        DecodeOutcome::Undetected => resolve_fallback(bytes, content_type, key),
    }
}

fn resolve_fallback(bytes: &[u8], content_type: Option<&str>, key: &str) -> ResolvedBody {
    match detect_preview_kind(content_type, key) {
        PreviewKind::Image(format) => ResolvedBody::Image(format),
        // Normalized to LF here, exactly like the pre-decoder `decode_text_body`
        // did: the buffer always holds LF internally, and the original
        // convention is restored on save from a separately detected
        // `LineEnding` — a raw CRLF body must never reach the buffer as-is,
        // or a later CRLF save-back would double up every line ending.
        PreviewKind::Text => match std::str::from_utf8(bytes) {
            Ok(text) => ResolvedBody::Text {
                text: text.replace("\r\n", "\n"),
                source: TextSource::Raw,
            },
            Err(_) => ResolvedBody::Binary,
        },
        PreviewKind::Pdf => ResolvedBody::Pdf,
        PreviewKind::Binary => ResolvedBody::Binary,
    }
}

fn kind_from_content_type(content_type: Option<&str>) -> Option<PreviewKind> {
    let content_type = content_type?
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_lowercase();

    if let Some(format) = raster_format_from_mime(&content_type) {
        return Some(PreviewKind::Image(format));
    }

    if content_type == "application/pdf" {
        return Some(PreviewKind::Pdf);
    }

    if content_type.starts_with("text/")
        || matches!(
            content_type.as_str(),
            "application/json"
                | "application/x-ndjson"
                | "application/xml"
                | "application/yaml"
                | "application/x-yaml"
                | "application/toml"
                | "application/sql"
        )
    {
        return Some(PreviewKind::Text);
    }

    None
}

fn raster_format_from_mime(content_type: &str) -> Option<ImageFormat> {
    match content_type {
        "image/png" => Some(ImageFormat::Png),
        "image/jpeg" | "image/jpg" => Some(ImageFormat::Jpeg),
        "image/webp" => Some(ImageFormat::Webp),
        "image/gif" => Some(ImageFormat::Gif),
        "image/bmp" | "image/x-ms-bmp" => Some(ImageFormat::Bmp),
        "image/svg+xml" => Some(ImageFormat::Svg),
        _ => None,
    }
}

fn kind_from_extension(key: &str) -> Option<PreviewKind> {
    let name = key.rsplit_once('/').map(|(_, name)| name).unwrap_or(key);
    let extension = name.rsplit_once('.')?.1.to_lowercase();

    let kind = match extension.as_str() {
        "svg" => PreviewKind::Image(ImageFormat::Svg),
        "png" => PreviewKind::Image(ImageFormat::Png),
        "jpg" | "jpeg" => PreviewKind::Image(ImageFormat::Jpeg),
        "webp" => PreviewKind::Image(ImageFormat::Webp),
        "gif" => PreviewKind::Image(ImageFormat::Gif),
        "bmp" => PreviewKind::Image(ImageFormat::Bmp),
        "pdf" => PreviewKind::Pdf,
        "txt" | "text" | "md" | "log" | "json" | "ndjson" | "csv" | "tsv" | "xml" | "yaml"
        | "yml" | "toml" | "ini" | "sql" | "sh" | "conf" | "env" | "properties" => {
            PreviewKind::Text
        }
        _ => return None,
    };

    Some(kind)
}

/// A decoded image held for the currently previewed object. Dropped whenever
/// the selection changes, so at most one object's bytes are resident.
#[derive(Clone, Debug, PartialEq)]
pub struct ImagePreview {
    pub image: Arc<Image>,
    /// Pixel dimensions from the validation decode; `None` for vector formats.
    pub dimensions: Option<(u32, u32)>,
    pub byte_len: u64,
}

impl ImagePreview {
    /// Meta strip under the image: pixel dimensions (when known), format,
    /// transferred size.
    pub fn meta_line(&self) -> String {
        let format = format_label(self.image.format);
        let size = format_bytes(self.byte_len);

        match self.dimensions {
            Some((width, height)) => format!("{width} × {height} · {format} · {size}"),
            None => format!("{format} · {size}"),
        }
    }
}

/// State of the body fetch for the previewed object.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum PreviewContentState {
    /// No body was requested — the object is not previewable in-app, or the
    /// gate refused the fetch.
    #[default]
    Unavailable,
    Loading,
    Image(Box<ImagePreview>),
    /// The body is presented as an editable text buffer. The buffer, its
    /// baseline, and its dirty state live in `editor.rs`'s `ObjectEditor`,
    /// which owns the `InputState` entity; this variant only records that the
    /// pane is showing it.
    Text,
    /// A magic byte was recognized but decoding it failed. Shown as an
    /// informative notice, not an error — the raw bytes are still available
    /// through the download/open actions.
    DecodeFailed {
        encoding: Encoding,
        reason: String,
    },
    /// A magic byte was recognized but its decoded size exceeds the preview
    /// limit.
    DecodeTooLarge {
        encoding: Encoding,
        limit_bytes: usize,
    },
    /// The bytes arrived but could not be turned into an image. The preview
    /// degrades to the same metadata + actions view as a binary object.
    Failed(String),
}

pub fn format_label(format: ImageFormat) -> &'static str {
    match format {
        ImageFormat::Png => "PNG",
        ImageFormat::Jpeg => "JPEG",
        ImageFormat::Webp => "WEBP",
        ImageFormat::Gif => "GIF",
        ImageFormat::Svg => "SVG",
        ImageFormat::Bmp => "BMP",
        ImageFormat::Tiff => "TIFF",
    }
}

/// Fully decodes `bytes` to prove they render and to read their true pixel
/// size. Header-only probing would accept a truncated body that then fails
/// silently inside the renderer, leaving an empty preview with no explanation.
pub fn decode_image_dimensions(bytes: &[u8]) -> Result<(u32, u32), String> {
    let reader = image::ImageReader::new(std::io::Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|e| crate::labels::image_header_error(&e.to_string()))?;

    let decoded = reader
        .decode()
        .map_err(|e| crate::labels::image_decode_error(&e.to_string()))?;

    Ok((decoded.width(), decoded.height()))
}

/// Sanity check for an SVG body: it must be UTF-8 and actually contain an
/// `<svg` root. gpui's renderer fails silently at paint time, so an obviously
/// broken body has to be caught here to reach the decode-failure fallback.
pub fn validate_svg_body(bytes: &[u8]) -> Result<(), String> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| dbflux_i18n::t!("document.object_browser.preview.body.svg_invalid_utf8"))?;

    if text.to_ascii_lowercase().contains("<svg") {
        Ok(())
    } else {
        Err(dbflux_i18n::t!(
            "document.object_browser.preview.body.svg_missing_root"
        ))
    }
}

/// A fetched object body, fully prepared for the preview pane: everything
/// [`resolve_body`] decided, plus the image-specific validation decode that
/// only makes sense once a body is actually confirmed to be an image.
///
/// Built entirely off the background executor (`data.rs`), since decoding and
/// image validation are both potentially expensive.
#[derive(Debug)]
pub enum PreparedPreview {
    Image(Result<ImagePreview, String>),
    Text {
        text: String,
        source: TextSource,
    },
    Pdf,
    Binary,
    DecodeFailed {
        encoding: Encoding,
        reason: String,
    },
    DecodeTooLarge {
        encoding: Encoding,
        limit_bytes: usize,
    },
}

/// Resolves `bytes` and, for an image verdict, validates it decodes — the
/// single entry point `data.rs` uses for every body fetch and every
/// encoding-override recompute.
pub fn prepare_preview(
    bytes: &[u8],
    content_type: Option<&str>,
    key: &str,
    max_output_bytes: usize,
    override_choice: Option<EncodingChoice>,
) -> PreparedPreview {
    match resolve_body(bytes, content_type, key, max_output_bytes, override_choice) {
        ResolvedBody::Image(format) => {
            let dimensions = if format == ImageFormat::Svg {
                validate_svg_body(bytes).map(|_| None)
            } else {
                decode_image_dimensions(bytes).map(Some)
            };

            PreparedPreview::Image(dimensions.map(|dimensions| ImagePreview {
                byte_len: bytes.len() as u64,
                image: Arc::new(Image::from_bytes(format, bytes.to_vec())),
                dimensions,
            }))
        }
        ResolvedBody::Text { text, source } => PreparedPreview::Text { text, source },
        ResolvedBody::Pdf => PreparedPreview::Pdf,
        ResolvedBody::Binary => PreparedPreview::Binary,
        ResolvedBody::DecodeFailed { encoding, reason } => {
            PreparedPreview::DecodeFailed { encoding, reason }
        }
        ResolvedBody::DecodeTooLarge {
            encoding,
            limit_bytes,
        } => PreparedPreview::DecodeTooLarge {
            encoding,
            limit_bytes,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EncodingChoice, ImagePreview, PreparedPreview, PreviewContentState, PreviewKind,
        ResolvedBody, TextSource, decode_image_dimensions, decode_label, detect_preview_kind,
        prepare_preview, resolve_body,
    };
    use dbflux_core::Encoding;
    use gpui::{Image, ImageFormat};
    use std::sync::Arc;

    /// T29: a meaningful content type decides the presentation on its own.
    #[test]
    fn content_type_drives_the_preview_kind() {
        assert_eq!(
            detect_preview_kind(Some("image/png"), "logo"),
            PreviewKind::Image(ImageFormat::Png)
        );
        assert_eq!(
            detect_preview_kind(Some("image/jpeg; charset=binary"), "photo"),
            PreviewKind::Image(ImageFormat::Jpeg)
        );
        assert_eq!(
            detect_preview_kind(Some("application/pdf"), "invoice"),
            PreviewKind::Pdf
        );
        assert_eq!(
            detect_preview_kind(Some("text/plain"), "notes"),
            PreviewKind::Text
        );
    }

    /// T29: `application/octet-stream` is the S3 default for anything uploaded
    /// without an explicit type, so it must not mask a recognisable extension.
    #[test]
    fn generic_content_types_defer_to_the_extension() {
        assert_eq!(
            detect_preview_kind(Some("application/octet-stream"), "shots/hero.PNG"),
            PreviewKind::Image(ImageFormat::Png)
        );
        assert_eq!(
            detect_preview_kind(None, "reports/summary.pdf"),
            PreviewKind::Pdf
        );
        assert_eq!(detect_preview_kind(None, "logs/app.log"), PreviewKind::Text);
    }

    /// T32: anything unrecognised — including SVG, which the raster decoder
    /// cannot read — lands on the download/open-externally path.
    #[test]
    fn unrecognised_objects_fall_back_to_binary() {
        assert_eq!(
            detect_preview_kind(None, "backup.tar.zst"),
            PreviewKind::Binary
        );
        assert_eq!(detect_preview_kind(None, "dataset"), PreviewKind::Binary);
    }

    #[test]
    fn svg_objects_route_to_the_image_path() {
        assert_eq!(
            detect_preview_kind(Some("image/svg+xml"), "icon.svg"),
            PreviewKind::Image(ImageFormat::Svg)
        );
        assert_eq!(
            detect_preview_kind(None, "logo.SVG"),
            PreviewKind::Image(ImageFormat::Svg)
        );
    }

    #[test]
    fn svg_body_validation_requires_an_svg_root() {
        assert_eq!(
            super::validate_svg_body(b"<svg viewBox=\"0 0 1 1\"/>"),
            Ok(())
        );
        assert!(super::validate_svg_body(b"<html></html>").is_err());
        assert!(super::validate_svg_body(&[0xFF, 0xFE, 0x00]).is_err());
    }

    /// T19: the SVG body-validation refusals route through the catalog
    /// instead of hardcoding the English prose.
    #[test]
    fn svg_body_validation_errors_are_translated() {
        assert_eq!(
            super::validate_svg_body(&[0xFF, 0xFE, 0x00]).unwrap_err(),
            dbflux_i18n::t!("document.object_browser.preview.body.svg_invalid_utf8")
        );
        assert_eq!(
            super::validate_svg_body(b"<html></html>").unwrap_err(),
            dbflux_i18n::t!("document.object_browser.preview.body.svg_missing_root")
        );
    }

    /// T29: garbage bytes are rejected by the decode probe instead of reaching
    /// the renderer as an empty image.
    #[test]
    fn decode_rejects_bytes_that_are_not_an_image() {
        assert!(decode_image_dimensions(b"not an image at all").is_err());
    }

    /// T19: garbage bytes with a guessable-but-wrong header fail the decode
    /// step, and the resulting error routes through the translated
    /// `image_decode_error` helper rather than a hardcoded English prefix,
    /// with the underlying decoder cause interpolated verbatim.
    #[test]
    fn decode_failure_is_translated() {
        let bytes: &[u8] = b"not an image at all";
        let underlying = image::ImageReader::new(std::io::Cursor::new(bytes))
            .with_guessed_format()
            .expect("heuristic format guess still succeeds on this input")
            .decode()
            .map(|_| ())
            .map_err(|e| e.to_string())
            .unwrap_err();

        let message = decode_image_dimensions(bytes).unwrap_err();
        assert_eq!(message, crate::labels::image_decode_error(&underlying));
    }

    /// T29: a real image reports its pixel size, which the meta strip shows
    /// alongside the format and transferred size.
    #[test]
    fn decode_reports_dimensions_for_a_real_image() {
        let png = one_by_one_png();

        assert_eq!(decode_image_dimensions(&png), Ok((1, 1)));

        let preview = ImagePreview {
            byte_len: png.len() as u64,
            image: Arc::new(Image::from_bytes(ImageFormat::Png, png)),
            dimensions: Some((1, 1)),
        };

        assert!(preview.meta_line().starts_with("1 × 1 · PNG · "));
    }

    /// T29: nothing is fetched until an object that can be previewed is
    /// selected.
    #[test]
    fn preview_content_starts_unavailable() {
        assert_eq!(
            PreviewContentState::default(),
            PreviewContentState::Unavailable
        );
    }

    /// Smallest valid PNG: a single opaque pixel.
    fn one_by_one_png() -> Vec<u8> {
        const PIXEL: &[u8] = &[
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48,
            0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00,
            0x00, 0x1F, 0x15, 0xC4, 0x89, 0x00, 0x00, 0x00, 0x0A, 0x49, 0x44, 0x41, 0x54, 0x78,
            0x9C, 0x63, 0x00, 0x01, 0x00, 0x00, 0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00,
            0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
        ];

        PIXEL.to_vec()
    }

    fn gzip_compress(data: &[u8]) -> Vec<u8> {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).expect("gzip write");
        encoder.finish().expect("gzip finish")
    }

    fn msgpack_encode(value: &serde_json::Value) -> Vec<u8> {
        use serde::Serialize;

        let mut buffer = Vec::new();
        value
            .serialize(&mut rmp_serde::Serializer::new(&mut buffer))
            .expect("msgpack encode");
        buffer
    }

    /// A gzip magic wins over a generic content type, and the decompressed
    /// JSON is labeled accordingly.
    #[test]
    fn gzip_bytes_decode_regardless_of_content_type() {
        let compressed = gzip_compress(br#"{"hello":"world"}"#);

        match resolve_body(
            &compressed,
            Some("application/octet-stream"),
            "payload.bin",
            1024 * 1024,
            None,
        ) {
            ResolvedBody::Text { text, source } => {
                assert_eq!(text, r#"{"hello":"world"}"#);
                assert_eq!(source, TextSource::Decoded(Encoding::Gzip));
                assert_eq!(decode_label(&text, source), Some("gzip → JSON".to_string()));
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    /// MessagePack has no magic bytes but is still detected structurally, and
    /// renders as its JSON text.
    #[test]
    fn messagepack_bytes_decode_to_json_text() {
        let value = serde_json::json!({ "count": 3 });
        let encoded = msgpack_encode(&value);

        match resolve_body(&encoded, None, "record", 1024 * 1024, None) {
            ResolvedBody::Text { text, source } => {
                assert_eq!(source, TextSource::Decoded(Encoding::MessagePack));
                let round_tripped: serde_json::Value =
                    serde_json::from_str(&text).expect("valid JSON output");
                assert_eq!(round_tripped, value);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    /// Magic bytes win over a misleading extension: a PNG stored as `.bin`
    /// still previews as an image.
    #[test]
    fn image_magic_overrides_a_misleading_extension() {
        let png = one_by_one_png();

        assert_eq!(
            resolve_body(
                &png,
                Some("application/octet-stream"),
                "asset.bin",
                1024,
                None
            ),
            ResolvedBody::Image(ImageFormat::Png)
        );
    }

    /// Corrupted bytes behind a valid magic surface as an informative
    /// decode-failed state, not a hard error hiding the object entirely.
    #[test]
    fn corrupt_bytes_after_a_valid_magic_surface_as_decode_failed() {
        let mut corrupt = gzip_compress(b"the quick brown fox jumps over the lazy dog");
        for byte in corrupt.iter_mut().skip(2) {
            *byte ^= 0xff;
        }

        match resolve_body(&corrupt, None, "log.gz", 1024 * 1024, None) {
            ResolvedBody::DecodeFailed {
                encoding: Encoding::Gzip,
                ..
            } => {}
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    /// A decode whose output would exceed the preview limit reports the
    /// limit instead of allocating an unbounded buffer.
    #[test]
    fn oversized_decoded_output_is_reported_as_too_large() {
        let compressed = gzip_compress(b"the quick brown fox jumps over the lazy dog, repeated");

        assert_eq!(
            resolve_body(&compressed, None, "log.gz", 4, None),
            ResolvedBody::DecodeTooLarge {
                encoding: Encoding::Gzip,
                limit_bytes: 4,
            }
        );
    }

    /// Undetectable bytes fall back to the extension/content-type
    /// inference, exactly as before magic-byte detection existed.
    #[test]
    fn undetected_bytes_fall_back_to_the_extension_guess() {
        assert_eq!(
            resolve_body(
                b"plain text body",
                Some("text/plain"),
                "notes.txt",
                1024,
                None
            ),
            ResolvedBody::Text {
                text: "plain text body".to_string(),
                source: TextSource::Raw,
            }
        );
    }

    /// A raw CRLF body is normalized to LF, exactly like the previous
    /// dedicated `decode_text_body` did: the buffer always holds LF, with
    /// the original convention restored on save from a separately detected
    /// `LineEnding`.
    #[test]
    fn raw_fallback_text_normalizes_crlf_to_lf() {
        assert_eq!(
            resolve_body(
                b"first\r\nsecond",
                Some("text/plain"),
                "notes.txt",
                1024,
                None
            ),
            ResolvedBody::Text {
                text: "first\nsecond".to_string(),
                source: TextSource::Raw,
            }
        );
    }

    /// A raw body that is not valid UTF-8 falls back to `Binary` rather than
    /// a lossy decode -- saving back a placeholder-mangled buffer would
    /// corrupt the object.
    #[test]
    fn raw_fallback_refuses_non_utf8_bodies() {
        assert_eq!(
            resolve_body(
                &[0xff, 0xfe, 0x00],
                Some("text/plain"),
                "notes.txt",
                1024,
                None
            ),
            ResolvedBody::Binary
        );
    }

    /// `EncodingChoice::Raw` forces the fallback even when the bytes carry a
    /// recognized magic — the user's explicit override wins.
    #[test]
    fn raw_override_forces_the_fallback_presentation() {
        let compressed = gzip_compress(b"hidden text");

        assert_eq!(
            resolve_body(
                &compressed,
                Some("application/octet-stream"),
                "archive.gz",
                1024,
                Some(EncodingChoice::Raw),
            ),
            ResolvedBody::Binary
        );
    }

    /// An explicit encoding choice decodes even when auto-detection alone
    /// would not have picked that encoding — MessagePack has no magic bytes,
    /// so the plain-text probe finds nothing, but a user-forced choice still
    /// takes the corrupt-decode path instead of being ignored.
    #[test]
    fn explicit_override_is_attempted_even_without_a_matching_magic() {
        match resolve_body(
            b"just a plain ordinary string, not compressed at all",
            None,
            "notes",
            1024,
            Some(EncodingChoice::Encoding(Encoding::Gzip)),
        ) {
            ResolvedBody::DecodeFailed {
                encoding: Encoding::Gzip,
                ..
            } => {}
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    /// `prepare_preview` adds the image validation decode on top of
    /// `resolve_body`'s verdict, exactly like the pre-existing image path did.
    #[test]
    fn prepare_preview_validates_a_resolved_image() {
        let png = one_by_one_png();

        match prepare_preview(&png, None, "logo.png", 1024, None) {
            PreparedPreview::Image(Ok(preview)) => {
                assert_eq!(preview.dimensions, Some((1, 1)));
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    /// `prepare_preview` passes decoded text straight through without
    /// attempting an image decode on it.
    #[test]
    fn prepare_preview_passes_decoded_text_through() {
        let compressed = gzip_compress(b"hello");

        match prepare_preview(&compressed, None, "greeting.gz", 1024, None) {
            PreparedPreview::Text { text, source } => {
                assert_eq!(text, "hello");
                assert_eq!(source, TextSource::Decoded(Encoding::Gzip));
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }
}
