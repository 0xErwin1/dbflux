use super::*;

#[test]
fn csv_shows_delimiter_encoding_and_null_text() {
    assert_eq!(
        visible_file_options(FileFormat::Csv),
        VisibleFileOptions {
            delimiter_and_encoding: true,
            null_text: true
        }
    );
}

#[test]
fn json_json_lines_and_xml_show_no_options() {
    for format in [FileFormat::Json, FileFormat::JsonLines, FileFormat::Xml] {
        assert_eq!(
            visible_file_options(format),
            VisibleFileOptions {
                delimiter_and_encoding: false,
                null_text: false
            },
            "{format:?}"
        );
    }
}

#[test]
fn formats_that_cannot_be_imported_show_no_options() {
    for format in [FileFormat::Html, FileFormat::Markdown, FileFormat::Sql] {
        assert_eq!(
            visible_file_options(format),
            VisibleFileOptions {
                delimiter_and_encoding: false,
                null_text: false
            },
            "{format:?}"
        );
    }
}

#[test]
fn the_picker_offers_only_importable_extensions() {
    assert_eq!(picker_extensions(), ["csv", "json", "jsonl", "xml"]);
}

#[test]
fn format_is_detected_from_the_extension_of_a_picked_file() {
    assert_eq!(
        detect_format(Path::new("/data/people.csv")),
        Some(FileFormat::Csv)
    );
    assert_eq!(
        detect_format(Path::new("/data/People.JSON")),
        Some(FileFormat::Json)
    );
    assert_eq!(
        detect_format(Path::new("events.jsonl")),
        Some(FileFormat::JsonLines)
    );
    assert_eq!(detect_format(Path::new("feed.XML")), Some(FileFormat::Xml));
    for rejected in ["page.html", "readme.md", "dump.sql", "notes.txt", "noext"] {
        assert_eq!(detect_format(Path::new(rejected)), None, "{rejected}");
    }
}

#[test]
fn every_choice_round_trips_through_its_segment_id() {
    for delimiter in CsvDelimiter::ALL {
        assert_eq!(
            delimiter_from_segment_id(delimiter_segment_id(delimiter)),
            Some(delimiter)
        );
    }
    for encoding in TextEncoding::ALL {
        assert_eq!(
            encoding_from_segment_id(encoding_segment_id(encoding)),
            Some(encoding)
        );
    }
    assert_eq!(delimiter_from_segment_id("nope"), None);
    assert_eq!(encoding_from_segment_id("nope"), None);
}

#[test]
fn every_choice_has_a_label() {
    for delimiter in CsvDelimiter::ALL {
        assert!(
            !delimiter_label(delimiter).starts_with("document."),
            "{delimiter:?}"
        );
    }
    for encoding in TextEncoding::ALL {
        assert!(
            !encoding_label(encoding).starts_with("document."),
            "{encoding:?}"
        );
    }
}
