use std::path::{Path, PathBuf};

use gpui::{ClipboardEntry, ClipboardItem, FileDragPaths, FileOperation, ImageFormat};
use gpui_util::ResultExt as _;
use http_client::Url;

/// A clipboard format that carries files, with the name it has on Wayland and as an X11 target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FileFormat {
    /// `text/uri-list`: one `file://` URI per line, CRLF terminated.
    UriList,
    /// `x-special/gnome-copied-files`: the operation, then the URIs, newline separated.
    GnomeCopiedFiles,
    /// `application/x-kde-cutselection`: `1` when the files were cut.
    KdeCutSelection,
}

/// The mime types text is offered under.
pub(crate) const TEXT_MIME_TYPES: [&str; 3] =
    ["text/plain;charset=utf-8", "UTF8_STRING", "text/plain"];

impl FileFormat {
    /// The mime type this format is offered under.
    pub(crate) fn mime(self) -> &'static str {
        match self {
            Self::UriList => "text/uri-list",
            Self::GnomeCopiedFiles => "x-special/gnome-copied-files",
            Self::KdeCutSelection => "application/x-kde-cutselection",
        }
    }
}

fn uris<P: AsRef<Path>>(paths: impl IntoIterator<Item = P>) -> impl Iterator<Item = String> {
    // Relative paths have no file URL and are skipped.
    paths
        .into_iter()
        .filter_map(|path| Url::from_file_path(path.as_ref()).ok())
        .map(String::from)
}

/// The `text/uri-list` body for `paths`: one `file://` URI per path that has one, each
/// terminated by CRLF as RFC 2483 requires.
pub(crate) fn file_uri_list<P: AsRef<Path>>(paths: impl IntoIterator<Item = P>) -> String {
    uris(paths).fold(String::new(), |mut list, uri| {
        list.push_str(&uri);
        list.push_str("\r\n");
        list
    })
}

/// The `text/uri-list` body handed to other applications for a native file drag.
pub(crate) fn drag_uri_list(paths: &FileDragPaths) -> String {
    file_uri_list(paths.entries().iter().map(|(path, _)| path))
}

/// The `x-special/gnome-copied-files` body: `copy` or `cut`, then one URI per line, with no
/// trailing newline.
pub(crate) fn gnome_copied_files<P: AsRef<Path>>(
    paths: impl IntoIterator<Item = P>,
    operation: FileOperation,
) -> String {
    let mut body = match operation {
        FileOperation::Copy => String::from("copy"),
        FileOperation::Cut => String::from("cut"),
    };
    for uri in uris(paths) {
        body.push('\n');
        body.push_str(&uri);
    }
    body
}

/// Every file format a file item is offered in, with its bytes. Empty when no path has a file
/// URL, so the caller can fall back to offering the item as text.
///
/// Files are not offered as text: an application that finds the paths as text pastes them as
/// text, while chat applications and browsers attach the files from `text/uri-list` when that is
/// all there is, as GNOME Files offers them.
pub(crate) fn file_clipboard_formats(
    paths: &[PathBuf],
    operation: FileOperation,
) -> Vec<(FileFormat, Vec<u8>)> {
    let uri_list = file_uri_list(paths);
    if uri_list.is_empty() {
        return Vec::new();
    }
    let mut formats = vec![
        (FileFormat::UriList, uri_list.into_bytes()),
        (
            FileFormat::GnomeCopiedFiles,
            gnome_copied_files(paths, operation).into_bytes(),
        ),
    ];
    if operation == FileOperation::Cut {
        formats.push((FileFormat::KdeCutSelection, b"1".to_vec()));
    }
    formats
}

/// One way a clipboard item is offered to other applications, with the bytes it sends.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Offer {
    /// The item's text, under every text mime type.
    Text(Vec<u8>),
    /// A file format of the item's files.
    File(FileFormat, Vec<u8>),
    /// An image entry, under its image mime type.
    Image(ImageFormat, Vec<u8>),
}

/// Every way `item` is offered, in order: its files in their file formats, or its text when it
/// has no file with a file URL; then each image format of its image entries, the first entry of
/// a format winning. An item of only images is not offered as text.
pub(crate) fn clipboard_offers(item: &ClipboardItem) -> Vec<Offer> {
    let mut offers: Vec<Offer> = item
        .file_paths()
        .zip(item.file_operation())
        .map(|(paths, operation)| file_clipboard_formats(&paths, operation))
        .unwrap_or_default()
        .into_iter()
        .map(|(format, bytes)| Offer::File(format, bytes))
        .collect();
    let mut images = Vec::new();
    for entry in item.entries() {
        if let ClipboardEntry::Image(image) = entry
            && !images
                .iter()
                .any(|offer| matches!(offer, Offer::Image(format, _) if *format == image.format))
        {
            images.push(Offer::Image(image.format, image.bytes.clone()));
        }
    }
    let has_string = item
        .entries()
        .iter()
        .any(|entry| matches!(entry, ClipboardEntry::String(_)));
    if offers.is_empty() && (has_string || images.is_empty()) {
        offers.push(Offer::Text(item.text().unwrap_or_default().into_bytes()));
    }
    offers.extend(images);
    offers
}

/// The paths of the `file://` URIs in a `text/uri-list` body. Comments, blank lines, URIs that
/// are not files and lines that are not URIs are skipped.
pub(crate) fn parse_file_uri_list(text: &str) -> Vec<PathBuf> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| Url::parse(line).log_err())
        .filter_map(|url| match url.to_file_path() {
            Ok(path) => Some(path),
            Err(()) => {
                log::error!("Failed turn {url:?} into a file path");
                None
            }
        })
        .collect()
}

/// Splits an `x-special/gnome-copied-files` body into its operation and paths. `None` when the
/// first line is not an operation or no path follows it.
pub(crate) fn parse_gnome_copied_files(text: &str) -> Option<(FileOperation, Vec<PathBuf>)> {
    let (header, uris) = text.split_once('\n').unwrap_or((text, ""));
    let operation = match header.trim() {
        "cut" => FileOperation::Cut,
        "copy" | "link" => FileOperation::Copy,
        _ => return None,
    };
    let paths = parse_file_uri_list(uris);
    (!paths.is_empty()).then_some((operation, paths))
}

/// The file item described by the file formats a clipboard owner offered, or `None` when they
/// name no files. The gnome format is preferred; otherwise the URI list is used and cut when
/// the KDE cut selection says `1`.
pub(crate) fn clipboard_item_from_files(
    gnome_copied_files: Option<&str>,
    uri_list: Option<&str>,
    kde_cut_selection: Option<&str>,
) -> Option<ClipboardItem> {
    if let Some((operation, paths)) = gnome_copied_files.and_then(parse_gnome_copied_files) {
        return Some(ClipboardItem::new_files(paths, operation));
    }
    let paths = parse_file_uri_list(uri_list?);
    if paths.is_empty() {
        return None;
    }
    let operation = if kde_cut_selection.is_some_and(|value| value.trim() == "1") {
        FileOperation::Cut
    } else {
        FileOperation::Copy
    };
    Some(ClipboardItem::new_files(paths, operation))
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt as _, path::PathBuf};

    use super::*;

    fn paths(list: &[&str]) -> Vec<PathBuf> {
        list.iter().map(PathBuf::from).collect()
    }

    #[test]
    fn builds_terminated_uri_list_for_each_dragged_path() {
        let paths = FileDragPaths::new([
            (PathBuf::from("/tmp/first"), false),
            (PathBuf::from("/tmp/nested directory"), true),
        ]);

        assert_eq!(
            drag_uri_list(&paths),
            "file:///tmp/first\r\nfile:///tmp/nested%20directory\r\n"
        );
    }

    #[test]
    fn skips_paths_that_have_no_file_url() {
        let paths = FileDragPaths::new([
            (PathBuf::from("relative/path"), false),
            (PathBuf::from("/tmp/absolute"), false),
        ]);

        assert_eq!(drag_uri_list(&paths), "file:///tmp/absolute\r\n");
    }

    #[test]
    fn builds_empty_uri_list_without_convertible_paths() {
        assert!(drag_uri_list(&FileDragPaths::default()).is_empty());
        assert!(
            drag_uri_list(&FileDragPaths::new([(PathBuf::from("relative"), false)])).is_empty()
        );
    }

    #[test]
    fn uri_list_percent_encodes_and_terminates_with_crlf() {
        assert_eq!(
            file_uri_list(paths(&["/tmp/a b", "/tmp/ünï#%.txt"])),
            "file:///tmp/a%20b\r\nfile:///tmp/%C3%BCn%C3%AF%23%25.txt\r\n"
        );
    }

    #[test]
    fn uri_list_encodes_bytes_that_are_not_utf8() {
        let path = PathBuf::from(OsString::from_vec(b"/tmp/bad\xff.txt".to_vec()));
        let list = file_uri_list([&path]);
        assert_eq!(list, "file:///tmp/bad%FF.txt\r\n");
        assert_eq!(parse_file_uri_list(&list), vec![path]);
    }

    #[test]
    fn gnome_copied_files_has_the_operation_first_and_no_trailing_newline() {
        let files = paths(&["/tmp/a", "/tmp/b c"]);
        assert_eq!(
            gnome_copied_files(&files, FileOperation::Copy),
            "copy\nfile:///tmp/a\nfile:///tmp/b%20c"
        );
        assert_eq!(
            gnome_copied_files(&files, FileOperation::Cut),
            "cut\nfile:///tmp/a\nfile:///tmp/b%20c"
        );
    }

    #[test]
    fn copy_formats_are_uri_list_and_gnome_without_text() {
        let formats = file_clipboard_formats(&paths(&["/tmp/a", "/tmp/b c"]), FileOperation::Copy);
        assert_eq!(
            formats,
            vec![
                (
                    FileFormat::UriList,
                    b"file:///tmp/a\r\nfile:///tmp/b%20c\r\n".to_vec()
                ),
                (
                    FileFormat::GnomeCopiedFiles,
                    b"copy\nfile:///tmp/a\nfile:///tmp/b%20c".to_vec()
                ),
            ]
        );
    }

    #[test]
    fn cut_formats_add_the_kde_cut_selection() {
        let formats = file_clipboard_formats(&paths(&["/tmp/a"]), FileOperation::Cut);
        assert_eq!(
            formats,
            vec![
                (FileFormat::UriList, b"file:///tmp/a\r\n".to_vec()),
                (FileFormat::GnomeCopiedFiles, b"cut\nfile:///tmp/a".to_vec()),
                (FileFormat::KdeCutSelection, b"1".to_vec()),
            ]
        );
    }

    fn png(bytes: &[u8]) -> gpui::Image {
        gpui::Image::from_bytes(gpui::ImageFormat::Png, bytes.to_vec())
    }

    #[test]
    fn files_are_offered_only_as_files() {
        let item = ClipboardItem::new_files(paths(&["/tmp/a"]), FileOperation::Copy);
        assert_eq!(
            clipboard_offers(&item),
            vec![
                Offer::File(FileFormat::UriList, b"file:///tmp/a\r\n".to_vec()),
                Offer::File(
                    FileFormat::GnomeCopiedFiles,
                    b"copy\nfile:///tmp/a".to_vec()
                ),
            ]
        );
    }

    #[test]
    fn files_with_an_image_are_offered_as_files_and_as_the_image() {
        let mut item = ClipboardItem::new_files(paths(&["/tmp/a.png"]), FileOperation::Copy);
        item.entries.push(gpui::ClipboardEntry::Image(png(b"PNG")));
        assert_eq!(
            clipboard_offers(&item),
            vec![
                Offer::File(FileFormat::UriList, b"file:///tmp/a.png\r\n".to_vec()),
                Offer::File(
                    FileFormat::GnomeCopiedFiles,
                    b"copy\nfile:///tmp/a.png".to_vec()
                ),
                Offer::Image(gpui::ImageFormat::Png, b"PNG".to_vec()),
            ]
        );
    }

    #[test]
    fn an_image_is_offered_once_per_format_and_without_text() {
        let mut item = ClipboardItem::new_image(&png(b"first"));
        item.entries
            .push(gpui::ClipboardEntry::Image(png(b"second")));
        assert_eq!(
            clipboard_offers(&item),
            vec![Offer::Image(gpui::ImageFormat::Png, b"first".to_vec())]
        );
    }

    #[test]
    fn text_and_files_without_file_urls_are_offered_as_text() {
        assert_eq!(
            clipboard_offers(&ClipboardItem::new_string("hello".into())),
            vec![Offer::Text(b"hello".to_vec())]
        );
        let relative = ClipboardItem::new_files(paths(&["relative"]), FileOperation::Copy);
        assert_eq!(
            clipboard_offers(&relative),
            vec![Offer::Text(b"relative".to_vec())]
        );
    }

    #[test]
    fn no_formats_are_offered_when_no_path_has_a_uri() {
        assert!(file_clipboard_formats(&paths(&["relative"]), FileOperation::Copy).is_empty());
        assert!(file_clipboard_formats(&[], FileOperation::Copy).is_empty());
    }

    #[test]
    fn formats_are_named_by_their_mime_types() {
        assert_eq!(FileFormat::UriList.mime(), "text/uri-list");
        assert_eq!(
            FileFormat::GnomeCopiedFiles.mime(),
            "x-special/gnome-copied-files"
        );
        assert_eq!(
            FileFormat::KdeCutSelection.mime(),
            "application/x-kde-cutselection"
        );
    }

    #[test]
    fn parses_crlf_and_lf_lists_decoding_escapes() {
        let expected = paths(&["/tmp/a b", "/tmp/ü"]);
        assert_eq!(
            parse_file_uri_list("file:///tmp/a%20b\r\nfile:///tmp/%C3%BC\r\n"),
            expected
        );
        assert_eq!(
            parse_file_uri_list("file:///tmp/a%20b\nfile:///tmp/%C3%BC"),
            expected
        );
    }

    #[test]
    fn parsing_skips_comments_blank_lines_and_non_file_uris() {
        assert_eq!(
            parse_file_uri_list(
                "# a comment\r\n\r\nhttps://example.com/x\r\nnot a uri\r\nfile:///tmp/kept\r\n"
            ),
            paths(&["/tmp/kept"])
        );
        assert!(parse_file_uri_list("https://example.com/").is_empty());
    }

    #[test]
    fn parses_gnome_copied_files_operation_and_paths() {
        assert_eq!(
            parse_gnome_copied_files("cut\nfile:///tmp/a%20b\nfile:///tmp/c"),
            Some((FileOperation::Cut, paths(&["/tmp/a b", "/tmp/c"])))
        );
        assert_eq!(
            parse_gnome_copied_files("copy\r\nfile:///tmp/a\r\n"),
            Some((FileOperation::Copy, paths(&["/tmp/a"])))
        );
        // `link` is not supported by this API; treat it as a copy.
        assert_eq!(
            parse_gnome_copied_files("link\nfile:///tmp/a"),
            Some((FileOperation::Copy, paths(&["/tmp/a"])))
        );
    }

    #[test]
    fn gnome_copied_files_without_a_valid_header_or_paths_is_rejected() {
        assert_eq!(parse_gnome_copied_files(""), None);
        assert_eq!(parse_gnome_copied_files("file:///tmp/a"), None);
        assert_eq!(parse_gnome_copied_files("cut\n"), None);
    }

    #[test]
    fn gnome_format_decides_the_operation_and_wins_over_uri_list() {
        let item =
            clipboard_item_from_files(Some("cut\nfile:///tmp/g"), Some("file:///tmp/u\r\n"), None)
                .expect("files");
        assert_eq!(item.file_paths(), Some(paths(&["/tmp/g"])));
        assert_eq!(item.file_operation(), Some(FileOperation::Cut));
    }

    #[test]
    fn uri_list_with_kde_cut_selection_is_a_cut() {
        let item =
            clipboard_item_from_files(None, Some("file:///tmp/u\r\n"), Some("1")).expect("files");
        assert_eq!(item.file_paths(), Some(paths(&["/tmp/u"])));
        assert_eq!(item.file_operation(), Some(FileOperation::Cut));
    }

    #[test]
    fn uri_list_alone_or_with_kde_zero_is_a_copy() {
        for kde in [None, Some("0"), Some("")] {
            let item =
                clipboard_item_from_files(None, Some("file:///tmp/u\r\n"), kde).expect("files");
            assert_eq!(item.file_operation(), Some(FileOperation::Copy));
        }
    }

    #[test]
    fn clipboards_without_file_paths_are_not_file_items() {
        assert!(clipboard_item_from_files(None, None, None).is_none());
        assert!(clipboard_item_from_files(None, Some("https://example.com/\r\n"), None).is_none());
        assert!(clipboard_item_from_files(Some("copy\n"), None, Some("1")).is_none());
    }

    #[test]
    fn a_broken_gnome_payload_falls_back_to_the_uri_list() {
        let item = clipboard_item_from_files(Some("garbage"), Some("file:///tmp/u\r\n"), None)
            .expect("files");
        assert_eq!(item.file_paths(), Some(paths(&["/tmp/u"])));
    }

    #[test]
    fn file_items_carry_their_paths_and_operation() {
        let cut = gpui::ClipboardItem::new_files(
            [PathBuf::from("/tmp/a"), PathBuf::from("/tmp/b c")],
            gpui::FileOperation::Cut,
        );
        assert_eq!(cut.file_operation(), Some(gpui::FileOperation::Cut));
        assert_eq!(
            cut.file_paths(),
            Some(vec![PathBuf::from("/tmp/a"), PathBuf::from("/tmp/b c")])
        );
        let copy =
            gpui::ClipboardItem::new_files([PathBuf::from("/tmp/a")], gpui::FileOperation::Copy);
        assert_eq!(copy.file_operation(), Some(gpui::FileOperation::Copy));
    }

    #[test]
    fn files_without_an_operation_count_as_copied_and_text_has_none() {
        let bare = gpui::ClipboardItem {
            entries: vec![gpui::ClipboardEntry::ExternalPaths(gpui::ExternalPaths(
                [PathBuf::from("/tmp/a")].into_iter().collect(),
            ))],
        };
        assert_eq!(bare.file_operation(), Some(gpui::FileOperation::Copy));

        let text = gpui::ClipboardItem::new_string("hello".into());
        assert_eq!(text.file_operation(), None);
        assert_eq!(text.file_paths(), None);
        assert_eq!(text.text(), Some("hello".into()));
    }

    #[test]
    fn file_item_text_is_one_path_per_line() {
        let item = gpui::ClipboardItem::new_files(
            [PathBuf::from("/tmp/a"), PathBuf::from("/tmp/b")],
            gpui::FileOperation::Copy,
        );
        assert_eq!(item.text(), Some("/tmp/a\n/tmp/b".into()));
    }

    #[test]
    fn file_items_use_only_the_public_entry_kinds() {
        let cut = gpui::ClipboardItem::new_files(
            [PathBuf::from("/tmp/a"), PathBuf::from("/tmp/b")],
            gpui::FileOperation::Cut,
        );
        let entries = cut.entries();
        assert_eq!(entries.len(), 2);
        assert!(matches!(entries[0], gpui::ClipboardEntry::ExternalPaths(_)));
        let gpui::ClipboardEntry::String(text) = &entries[1] else {
            panic!("expected the operation to ride on a string entry");
        };
        assert_eq!(text.text, "/tmp/a\n/tmp/b");
        assert!(text.metadata.is_some());
        assert_eq!(cut.text(), Some("/tmp/a\n/tmp/b".into()));
    }

    #[test]
    fn a_text_item_cannot_pose_as_a_file_item() {
        let text = gpui::ClipboardItem::new_string_with_metadata("x".into(), "other".into());
        assert_eq!(text.file_operation(), None);
        let cut =
            gpui::ClipboardItem::new_files([PathBuf::from("/tmp/a")], gpui::FileOperation::Cut);
        let marker = match &cut.entries()[1] {
            gpui::ClipboardEntry::String(text) => text.metadata.clone().unwrap(),
            _ => unreachable!(),
        };
        let forged = gpui::ClipboardItem::new_string_with_metadata("x".into(), marker);
        assert_eq!(forged.file_operation(), None);
        assert_eq!(forged.file_paths(), None);
    }
}
