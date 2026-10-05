//! Which document opens a data file, decided once from its name.
//!
//! Every entry point that opens a file or object as a data document (the file
//! dialog, the scripts sidebar, recent files, IPC, the object browser) asks
//! [`file_document_format`] and hands the result to a single dispatch, so a
//! new format is one more variant here rather than one more check per caller.

use std::path::Path;

/// A file format that opens in a file-backed data document.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileDocumentFormat {
    /// Comma- or tab-separated text, opened in the delimited document.
    Delimited,

    /// An Apache Parquet file, opened read-only in the Parquet document.
    Parquet,
}

impl FileDocumentFormat {
    /// The extensions, without the leading dot, of the files this format
    /// opens, as the open-file dialog offers them.
    pub fn extensions(self) -> &'static [&'static str] {
        match self {
            Self::Delimited => DELIMITED_EXTENSIONS,
            Self::Parquet => PARQUET_EXTENSIONS,
        }
    }
}

/// The format of the file or object at `path`, or `None` when it does not open
/// as a data document.
///
/// The decision is made on the extension alone, in any letter case. An object
/// key is passed as a path, so its `/`-separated last component is the name.
pub fn file_document_format(path: &Path) -> Option<FileDocumentFormat> {
    let extension = path.extension()?;

    [FileDocumentFormat::Delimited, FileDocumentFormat::Parquet]
        .into_iter()
        .find(|format| {
            format
                .extensions()
                .iter()
                .any(|candidate| extension.eq_ignore_ascii_case(candidate))
        })
}

const DELIMITED_EXTENSIONS: &[&str] = &["csv", "tsv"];

const PARQUET_EXTENSIONS: &[&str] = &["parquet"];

#[cfg(test)]
mod tests {
    use super::{FileDocumentFormat, file_document_format};
    use std::path::Path;

    #[test]
    fn file_document_format_matches_csv_and_tsv_in_any_case_and_rejects_others() {
        for name in [
            "a.csv",
            "B.CSV",
            "c.tsv",
            "D.Tsv",
            "/home/ana/Reports.Csv",
            "2026/q1/cities.tsv",
        ] {
            assert_eq!(
                file_document_format(Path::new(name)),
                Some(FileDocumentFormat::Delimited),
                "{name}"
            );
        }

        for name in [
            "query.sql",
            "notes.txt",
            "cities.csv.gz",
            "csv",
            ".csv",
            "reports/csv/",
        ] {
            assert_eq!(file_document_format(Path::new(name)), None, "{name}");
        }
    }

    #[test]
    fn file_document_format_recognizes_parquet_in_any_case() {
        for name in [
            "cities.parquet",
            "CITIES.PARQUET",
            "Cities.Parquet",
            "/home/ana/exports/trips.parquet",
            "2026/q1/trips.PARQUET",
        ] {
            assert_eq!(
                file_document_format(Path::new(name)),
                Some(FileDocumentFormat::Parquet),
                "{name}"
            );
        }

        for name in ["trips.parquet.gz", "parquet", ".parquet", "trips.pq"] {
            assert_eq!(file_document_format(Path::new(name)), None, "{name}");
        }
    }
}
