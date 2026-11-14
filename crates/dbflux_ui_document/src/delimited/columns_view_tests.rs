//! The Columns view of a delimited file: the profile of the loaded records,
//! the cycle of the three views, the full scan and what stops it.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use dbflux_app::keymap::Command;
use dbflux_components::components::column_profile_view::{
    ColumnProfileView, ColumnSort, ProfileAction,
};
use dbflux_core::{ProfileSource, TableProfile};
use dbflux_delimited::{DialectOverrides, ReaderOptions};
use gpui::{Entity, TestAppContext, VisualTestContext};

use super::columns_view::INLINE_PROFILE_RECORDS;
use super::document::DelimitedDocument;
use super::document_tests::{
    CITIES, SMALL_PAGES, covered_ids, open, open_local, open_local_in_small_pages, pane_action_ids,
    run_pane_action, set_overrides,
};
use super::editing_tests::{press, table_state, type_into_cell};
use super::tests::TestDirectory;
use super::text_view::DelimitedView;
use super::text_view_tests::{show, view};

/// A file whose `note` column is empty in every even record: `total`
/// records, `total / 2 + total % 2` of them with an empty note.
fn notes_csv(total: usize) -> Vec<u8> {
    let mut bytes = b"id,note\n".to_vec();

    for record in 0..total {
        let note = if record % 2 == 0 { "" } else { "seen" };
        bytes.extend_from_slice(format!("{record},{note}\n").as_bytes());
    }

    bytes
}

fn profile_view(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Entity<ColumnProfileView> {
    window.update(|_, cx| {
        document
            .read(cx)
            .column_profile_view()
            .cloned()
            .expect("the Columns view was drawn")
    })
}

fn profile(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Arc<TableProfile> {
    window.update(|_, cx| {
        document
            .read(cx)
            .column_profile()
            .cloned()
            .expect("the Columns view shows a profile")
    })
}

fn source_text(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> String {
    let view = profile_view(document, window);
    window.update(|_, cx| view.read(cx).source_text().to_string())
}

fn profile_whole_file(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) {
    let view = profile_view(document, window);

    window.update(|_, cx| {
        view.update(cx, |view, cx| {
            view.request_action(ProfileAction::FullScan, cx)
        });
    });
    window.run_until_parked();
}

fn dispatch(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    command: Command,
) {
    window.update(|window, cx| {
        document.update(cx, |document, cx| {
            assert!(
                document.dispatch_command(command, window, cx),
                "{command:?} is handled"
            );
        })
    });
    window.run_until_parked();
}

// -- The loaded-rows profile ----------------------------------------------------

#[gpui::test]
fn the_columns_view_profiles_the_loaded_rows(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("columns-loaded");
    let (path, _) = directory.file("notes.csv", &notes_csv(5));

    let (document, window) = open_local(cx, path);
    show(&document, window, DelimitedView::Columns);

    assert_eq!(view(&document, window), DelimitedView::Columns);
    assert_eq!(source_text(&document, window), "From the 5 loaded rows");

    let profile = profile(&document, window);

    assert_eq!(profile.source_label, ProfileSource::LoadedRows { rows: 5 });
    assert_eq!(profile.row_count, Some(5));

    let names: Vec<&str> = profile.columns.iter().map(|c| c.name.as_ref()).collect();
    assert_eq!(names, ["id", "note"]);

    let id = &profile.columns[0];
    assert_eq!(id.type_name.as_ref(), "Integer");
    assert_eq!(id.null_count, Some(0));
    assert_eq!(id.distinct_count, Some(5));
    assert!(id.distinct_exact);

    let range = id.range.as_ref().expect("a range");
    assert_eq!((range.min.as_ref(), range.max.as_ref()), ("0", "4"));
    assert!(range.exact);

    let note = &profile.columns[1];
    assert_eq!(note.type_name.as_ref(), "Text");
    assert_eq!(note.null_count, Some(3));
    assert_eq!(note.top_values.len(), 1);
    assert_eq!(note.top_values[0].value.as_ref(), "seen");
    assert_eq!(note.top_values[0].count, 2);
}

#[gpui::test]
fn the_profile_follows_the_pages_loaded_while_it_is_shown(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("columns-load-more");
    let (path, _) = directory.file("notes.csv", &notes_csv(5));

    let (document, window) = open_local_in_small_pages(cx, path);
    show(&document, window, DelimitedView::Columns);

    assert_eq!(
        profile(&document, window).source_label,
        ProfileSource::LoadedRows { rows: 2 }
    );

    dispatch(&document, window, Command::ResultsNextPage);

    assert_eq!(
        profile(&document, window).source_label,
        ProfileSource::LoadedRows { rows: 4 }
    );
    assert_eq!(source_text(&document, window), "From the 4 loaded rows");
}

#[gpui::test]
fn a_large_backlog_is_profiled_in_the_background(cx: &mut TestAppContext) {
    let total = INLINE_PROFILE_RECORDS + 1_000;
    let directory = TestDirectory::new("columns-backlog");
    let (path, _) = directory.file("notes.csv", &notes_csv(total));

    let one_page = ReaderOptions {
        page_size: NonZeroUsize::new(total + 1).unwrap(),
        window_size: NonZeroU64::new(1024 * 1024).unwrap(),
    };

    let (document, window) = open(cx, move |cx| {
        DelimitedDocument::open_local_with(path, one_page, cx)
    });
    show(&document, window, DelimitedView::Columns);

    let profile = profile(&document, window);

    assert_eq!(
        profile.source_label,
        ProfileSource::LoadedRows { rows: total as u64 }
    );
    assert_eq!(
        profile.columns[1].null_count,
        Some(total.div_ceil(2) as u64)
    );
}

#[gpui::test]
fn the_profile_describes_the_file_as_read_not_the_pending_edits(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("columns-pending");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);
    type_into_cell(&document, window, 0, 1, "Cusco");
    show(&document, window, DelimitedView::Columns);

    let city = &profile(&document, window).columns[1];
    let range = city.range.as_ref().expect("a range");

    assert_eq!((range.min.as_ref(), range.max.as_ref()), ("Lima", "Quito"));
}

#[gpui::test]
fn the_eye_column_and_the_storage_columns_are_hidden(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("columns-eye");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);
    show(&document, window, DelimitedView::Columns);

    let view = profile_view(&document, window);

    window.update(|_, cx| {
        let view = view.read(cx);

        assert!(!view.projection_enabled());
        assert!(!view.shows_codec() && !view.shows_sizes());
        assert_eq!(view.applied().selected_count(), 2);
    });
    assert!(!window.update(|_, cx| view.update(cx, |view, cx| view.toggle_column(0, cx))));
}

// -- Switching ---------------------------------------------------------------------

#[gpui::test]
fn t_cycles_the_table_the_text_and_the_columns(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("columns-cycle");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);
    assert_eq!(view(&document, window), DelimitedView::Table);

    press(window, "t");
    assert_eq!(view(&document, window), DelimitedView::Text);

    // The text editor holds the keyboard, so Escape hands it to the tab.
    press(window, "escape");
    press(window, "t");
    assert_eq!(view(&document, window), DelimitedView::Columns);

    let table_state = table_state(&document, window);
    assert!(
        !window.update(|window, cx| table_state.read(cx).focus_handle().is_focused(window)),
        "the Columns view takes the keyboard from the table"
    );

    press(window, "t");
    assert_eq!(view(&document, window), DelimitedView::Table);
}

#[gpui::test]
fn the_columns_segment_switches_by_mouse(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("columns-segment");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    super::columns_tests::click(window, "segmented-delimited-view-columns");
    assert_eq!(view(&document, window), DelimitedView::Columns);

    super::columns_tests::click(window, "segmented-delimited-view-table");
    assert_eq!(view(&document, window), DelimitedView::Table);
}

#[gpui::test]
fn shift_t_sorts_and_slash_filters_in_the_columns_view(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("columns-commands");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);
    show(&document, window, DelimitedView::Columns);

    let view = profile_view(&document, window);

    press(window, "shift-t");
    assert_eq!(
        window.update(|_, cx| view.read(cx).sort()),
        ColumnSort::Size
    );

    press(window, "/ c i t y");
    assert_eq!(
        window.update(|_, cx| view.read(cx).visible_columns().to_vec()),
        vec![1]
    );
}

#[gpui::test]
fn the_text_mode_keys_still_work_in_the_text_view(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("columns-text-mode");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);
    show(&document, window, DelimitedView::Text);
    press(window, "escape");

    dispatch(&document, window, Command::CycleResultView);

    assert_eq!(
        window.update(|_, cx| document.read(cx).text_mode()),
        super::text_view::TextMode::Aligned
    );
}

// -- Full scan ---------------------------------------------------------------------

#[gpui::test]
fn a_full_scan_profiles_every_record_of_a_file_larger_than_a_page(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("columns-full-scan");
    let (path, _) = directory.file("notes.csv", &notes_csv(7));

    let (document, window) = open_local_in_small_pages(cx, path);
    show(&document, window, DelimitedView::Columns);

    assert_eq!(
        profile(&document, window).source_label,
        ProfileSource::LoadedRows { rows: 2 }
    );

    profile_whole_file(&document, window);

    let profile = profile(&document, window);

    assert_eq!(profile.source_label, ProfileSource::FullScan { rows: 7 });
    assert_eq!(profile.row_count, Some(7));
    assert!(profile.captured_at.is_some());
    assert_eq!(profile.columns[1].null_count, Some(4));
    assert_eq!(profile.columns[0].distinct_count, Some(7));
    assert!(source_text(&document, window).starts_with("From all 7 rows"));

    assert!(!window.update(|_, cx| document.read(cx).is_profiling_whole_file()));
    let view = profile_view(&document, window);
    assert_eq!(window.update(|_, cx| view.read(cx).running()), None);

    // A page loaded afterwards keeps the full scan on screen.
    dispatch(&document, window, Command::ResultsNextPage);
    assert_eq!(
        self::profile(&document, window).source_label,
        ProfileSource::FullScan { rows: 7 }
    );
}

#[gpui::test]
fn the_pane_action_profiles_the_whole_file_from_the_table(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("columns-pane-action");
    let (path, _) = directory.file("notes.csv", &notes_csv(5));

    let (document, window) = open_local_in_small_pages(cx, path);

    assert!(
        pane_action_ids(&document, window)
            .iter()
            .any(|id| id == "delimited-profile-full-scan")
    );

    run_pane_action(&document, window, "delimited-profile-full-scan");

    assert_eq!(view(&document, window), DelimitedView::Columns);
    assert_eq!(
        profile(&document, window).source_label,
        ProfileSource::FullScan { rows: 5 }
    );
}

#[gpui::test]
fn a_dialect_change_returns_the_profile_to_the_loaded_rows(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("columns-dialect");
    let (path, _) = directory.file("notes.csv", &notes_csv(5));

    let (document, window) = open_local_in_small_pages(cx, path);
    show(&document, window, DelimitedView::Columns);
    profile_whole_file(&document, window);

    assert_eq!(
        profile(&document, window).source_label,
        ProfileSource::FullScan { rows: 5 }
    );

    set_overrides(
        &document,
        window,
        DialectOverrides {
            has_header: Some(false),
            ..DialectOverrides::default()
        },
    );

    let profile = profile(&document, window);

    assert_eq!(profile.source_label, ProfileSource::LoadedRows { rows: 2 });
    assert_eq!(profile.columns[0].name.as_ref(), "column_1");
}

#[gpui::test]
fn a_reload_stops_a_running_scan(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("columns-reload");
    let (path, _) = directory.file("notes.csv", &notes_csv(5));

    let (document, window) = open_local_in_small_pages(cx, path);
    show(&document, window, DelimitedView::Columns);

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.profile_whole_file(cx);
            assert!(document.is_profiling_whole_file());

            document.reload(cx);
            assert!(!document.is_profiling_whole_file());
        })
    });
    window.run_until_parked();

    let view = profile_view(&document, window);
    assert_eq!(window.update(|_, cx| view.read(cx).running()), None);
    assert_eq!(
        profile(&document, window).source_label,
        ProfileSource::LoadedRows { rows: 2 }
    );
}

// -- Keyboard coverage and strings ------------------------------------------------

#[gpui::test]
fn the_columns_view_controls_are_covered(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("columns-coverage");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);
    show(&document, window, DelimitedView::Columns);

    let checked = covered_ids(&document, window);

    for control in [
        "segmented-delimited-view-columns",
        "column-profile-sort",
        "column-profile-full-scan",
    ] {
        assert!(checked.iter().any(|id| id == control), "{checked:?}");
    }
}

#[test]
fn the_columns_view_strings_resolve_in_every_locale() {
    let keys = [
        "document.delimited.view.columns",
        "document.delimited.columns.type.integer",
        "document.delimited.columns.type.decimal",
        "document.delimited.columns.type.boolean",
        "document.delimited.columns.type.date",
        "document.delimited.columns.type.timestamp",
        "document.delimited.columns.type.text",
        "document.delimited.columns.progress",
        "document.delimited.columns.error.scan_failed",
    ];

    for key in keys {
        for locale in ["en", "es", "ko", "pt_BR", "zh_Hans"] {
            let text = dbflux_i18n::t!(key, locale = locale);

            assert!(!text.is_empty());
            assert_ne!(text, format!("{locale}.{key}"), "{locale} lacks {key}");
        }
    }
}

#[gpui::test]
fn clicking_profile_whole_file_scans_a_file_of_many_pages(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("columns-click-full-scan");
    let (path, _) = directory.file("notes.csv", &notes_csv(20_000));

    let (document, window) = open_local(cx, path);
    super::columns_tests::click(window, "segmented-delimited-view-columns");

    assert_eq!(
        profile(&document, window).source_label,
        ProfileSource::LoadedRows { rows: 500 }
    );

    super::columns_tests::click(window, "column-profile-full-scan");

    let profile = profile(&document, window);

    assert_eq!(
        profile.source_label,
        ProfileSource::FullScan { rows: 20_000 }
    );
    assert_eq!(profile.columns[1].null_count, Some(10_000));
}
