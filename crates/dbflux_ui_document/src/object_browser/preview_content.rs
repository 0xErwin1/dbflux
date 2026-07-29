//! What an object's body is, and the state of the bytes fetched for it.
//!
//! Pure model + formatting. The fetch/decode plumbing lives in `data.rs` and
//! the rendering in `preview.rs`. The preview *gate* (`metadata.rs`) decides
//! whether bytes may be fetched at all; this module decides what to do with
//! them once they are allowed.

use crate::buckets_table::format_bytes;
use gpui::{Image, ImageFormat};
use std::sync::Arc;

/// How a previewable object is presented.
///
/// SVG is deliberately absent from `Image`: it is a vector document, not a
/// raster payload, and the decoder used to validate raster bytes cannot read
/// it — it falls through to the download/open-externally path instead.
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
        _ => None,
    }
}

fn kind_from_extension(key: &str) -> Option<PreviewKind> {
    let name = key.rsplit_once('/').map(|(_, name)| name).unwrap_or(key);
    let extension = name.rsplit_once('.')?.1.to_lowercase();

    let kind = match extension.as_str() {
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
    pub width: u32,
    pub height: u32,
    pub byte_len: u64,
}

impl ImagePreview {
    /// Meta strip under the image: pixel dimensions, format, transferred size.
    pub fn meta_line(&self) -> String {
        format!(
            "{} × {} · {} · {}",
            self.width,
            self.height,
            format_label(self.image.format),
            format_bytes(self.byte_len)
        )
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
        .map_err(|e| format!("Could not read the image header: {e}"))?;

    let decoded = reader
        .decode()
        .map_err(|e| format!("Could not decode the image: {e}"))?;

    Ok((decoded.width(), decoded.height()))
}

#[cfg(test)]
mod tests {
    use super::{
        ImagePreview, PreviewContentState, PreviewKind, decode_image_dimensions,
        detect_preview_kind,
    };
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
        assert_eq!(
            detect_preview_kind(Some("image/svg+xml"), "icon.svg"),
            PreviewKind::Binary
        );
    }

    /// T29: garbage bytes are rejected by the decode probe instead of reaching
    /// the renderer as an empty image.
    #[test]
    fn decode_rejects_bytes_that_are_not_an_image() {
        assert!(decode_image_dimensions(b"not an image at all").is_err());
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
            width: 1,
            height: 1,
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
}
