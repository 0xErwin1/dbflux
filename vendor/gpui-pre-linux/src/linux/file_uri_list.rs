use gpui::FileDragPaths;
use http_client::Url;

/// The `text/uri-list` body handed to other applications for a native file drag: one `file://`
/// URI per path that has one, each terminated by CRLF as RFC 2483 requires. Relative paths have
/// no file URL and are skipped.
pub(crate) fn file_uri_list(paths: &FileDragPaths) -> String {
    paths
        .entries()
        .iter()
        .filter_map(|(path, _)| Url::from_file_path(path).ok())
        .fold(String::new(), |mut list, url| {
            list.push_str(url.as_str());
            list.push_str("\r\n");
            list
        })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn builds_terminated_uri_list_for_each_dragged_path() {
        let paths = FileDragPaths::new([
            (PathBuf::from("/tmp/first"), false),
            (PathBuf::from("/tmp/nested directory"), true),
        ]);

        assert_eq!(
            file_uri_list(&paths),
            "file:///tmp/first\r\nfile:///tmp/nested%20directory\r\n"
        );
    }

    #[test]
    fn skips_paths_that_have_no_file_url() {
        let paths = FileDragPaths::new([
            (PathBuf::from("relative/path"), false),
            (PathBuf::from("/tmp/absolute"), false),
        ]);

        assert_eq!(file_uri_list(&paths), "file:///tmp/absolute\r\n");
    }

    #[test]
    fn builds_empty_uri_list_without_convertible_paths() {
        assert!(file_uri_list(&FileDragPaths::default()).is_empty());
        assert!(
            file_uri_list(&FileDragPaths::new([(PathBuf::from("relative"), false)])).is_empty()
        );
    }
}
