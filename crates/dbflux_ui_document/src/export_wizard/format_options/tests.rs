use super::*;

#[test]
fn options_a_format_does_not_use_are_hidden() {
    assert_eq!(
        visible_options(FileFormat::Csv),
        VisibleOptions {
            delimiter_and_encoding: true,
            null_text: true,
            date_format: true,
            sql_batch_size: false
        }
    );
    for format in [FileFormat::Html, FileFormat::Markdown] {
        assert_eq!(
            visible_options(format),
            VisibleOptions {
                delimiter_and_encoding: false,
                null_text: true,
                date_format: true,
                sql_batch_size: false
            },
            "{format:?}"
        );
    }
    for format in [FileFormat::Json, FileFormat::JsonLines, FileFormat::Xml] {
        assert_eq!(
            visible_options(format),
            VisibleOptions {
                delimiter_and_encoding: false,
                null_text: false,
                date_format: true,
                sql_batch_size: false
            },
            "{format:?}"
        );
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
    for date_format in DateFormat::ALL {
        assert_eq!(
            date_format_from_segment_id(date_format_segment_id(date_format)),
            Some(date_format)
        );
    }
    assert_eq!(delimiter_from_segment_id("nope"), None);
}

#[test]
fn every_choice_label_resolves_in_every_locale() {
    let keys = [
        "delimiter.comma",
        "delimiter.semicolon",
        "delimiter.tab",
        "delimiter.pipe",
        "encoding.utf8",
        "encoding.utf8_bom",
        "encoding.utf16le",
        "encoding.windows1252",
        "date_format.rfc3339",
        "date_format.date_time",
        "date_format.epoch",
        "delimiter_label",
        "encoding_label",
        "null_text_label",
        "null_text_placeholder",
        "null_text_hint",
        "date_format_label",
        "sql_batch_size_label",
        "sql_batch_size_placeholder",
        "sql_batch_size_hint",
        "sql_batch_size_invalid",
    ];
    for locale in ["en", "es", "ko", "pt_BR", "zh_Hans"] {
        for key in keys {
            let full = format!("document.export_wizard.format_options.{key}");
            let value = dbflux_i18n::t!(full.as_str(), locale = locale);
            assert!(!value.is_empty(), "{full} empty in {locale}");
            assert_ne!(value, full, "{full} missing in {locale}");
            assert_ne!(
                value,
                format!("{locale}.{full}"),
                "{full} missing in {locale}"
            );
        }
    }
}

#[test]
fn sql_shows_only_the_batch_size() {
    assert_eq!(
        visible_options(FileFormat::Sql),
        VisibleOptions {
            delimiter_and_encoding: false,
            null_text: false,
            date_format: false,
            sql_batch_size: true
        }
    );
}

#[test]
fn sql_is_offered_only_when_the_source_connection_supports_it() {
    assert_eq!(available_formats(true), FileFormat::WITH_SQL.to_vec());
    assert_eq!(available_formats(false), FileFormat::ALL.to_vec());
    assert!(!available_formats(false).contains(&FileFormat::Sql));
}

#[test]
fn the_sql_segment_id_is_its_extension_and_the_label_resolves_in_every_locale() {
    assert_eq!(FileFormat::Sql.extension(), "sql");
    for locale in ["en", "es", "ko", "pt_BR", "zh_Hans"] {
        let key = "document.data.grid.export.format.sql";
        let value = dbflux_i18n::t!(key, locale = locale);
        assert_eq!(value, "SQL", "{locale}");
    }
}
