//! The Columns view of `DelimitedDocument`: a profile of every column of the
//! file, shown in the shared `ColumnProfileView`.
//!
//! The profile describes the file as it was read, not the pending changes:
//! the column names are the header's own fields, and edits, inserted rows
//! and appended columns are left out.
//!
//! By default the profile covers the records loaded so far
//! (`ProfileSource::LoadedRows`). It is worked out when the view is drawn
//! and is fed only the records loaded since the last time, so loading a page
//! costs one page of profiling. A backlog larger than
//! [`INLINE_PROFILE_RECORDS`], such as the rest of a file loaded at once, is
//! profiled on the background executor while the previous profile stays on
//! screen.
//!
//! "Profile whole file" reads every record again through a reader of its
//! own, with the dialect the table shows, on the background executor, and
//! reports the bytes read so far. Its result (`ProfileSource::FullScan`)
//! replaces the loaded-rows profile until the file is read again: a reload,
//! a save or a dialect change gives the reader a new epoch, which stops a
//! running scan and returns the view to the loaded rows. Dropping the
//! document drops the scan's task, which stops it after the step it is in.
//!
//! A CSV file has no column projection, so the view's eye column is turned
//! off and every column counts as shown.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use dbflux_components::components::column_facts::format_bytes;
use dbflux_components::components::column_profile_view::{
    ColumnProfileActionRequested, ColumnProfileView, ProfileAction, ProfileActions,
};
use dbflux_core::{
    ColumnProfile, ColumnProjection, ProfileSource, TableProfile, ValueCount, ValueRange,
};
use dbflux_delimited::{
    ColumnSummary, Dialect, PagedReader, ProfileAccumulator, ReaderOptions, Record, ValueKind,
};
use dbflux_ui_base::user_error::report_error;
use gpui::*;

use super::document::{DelimitedDocument, DelimitedPhase, OpenError, open_error_to_user_facing};
use super::page_model::positional_name;
use super::text_view::DelimitedView;
use crate::file_source::{FileLocation, LocationSource, open_source};

/// The most loaded records profiled on the foreground in one go. A larger
/// backlog is profiled on the background executor.
pub(super) const INLINE_PROFILE_RECORDS: usize = 5_000;

/// How long one background step of a full scan reads before it hands its
/// progress to the foreground.
const SCAN_STEP: Duration = Duration::from_millis(100);

/// The profile of the loaded records and the full scan, and the view that
/// shows them.
#[derive(Default)]
pub(super) struct ColumnsViewState {
    /// Built the first time the Columns view is drawn, because it needs the
    /// window.
    pub(super) view: Option<Entity<ColumnProfileView>>,

    /// The reader epoch the profiles below belong to. A different epoch
    /// means the file was read again, and both are dropped.
    epoch: u64,

    /// The profile of the loaded records. `None` while a background step
    /// holds it.
    loaded: Option<ProfileAccumulator>,

    /// How many loaded records `loaded` was fed.
    fed: usize,

    /// Whether the view shows what `loaded` holds now.
    published: bool,

    /// The background profiling of a large backlog. Dropping it drops the
    /// step's result.
    catching_up: Option<Task<()>>,

    /// The profile of the whole file, once a full scan finished.
    full_scan: Option<Arc<TableProfile>>,

    /// The running full scan. Dropping it stops the scan.
    scan: Option<Task<()>>,

    /// The profile the view was last given.
    shown: Option<Arc<TableProfile>>,

    _subscription: Option<Subscription>,
}

/// A full scan between two background steps.
struct Scan {
    reader: PagedReader<LocationSource>,
    page_size: NonZeroUsize,
    names: Vec<String>,
    accumulator: ProfileAccumulator,
    next_page: usize,
    scanned_bytes: u64,
    total_bytes: u64,
    finished: bool,
}

impl Scan {
    /// Opens a reader of its own over the file at `location` with `dialect`.
    /// Blocks on file or network I/O.
    fn open(
        location: &FileLocation,
        dialect: Dialect,
        reader_options: ReaderOptions,
    ) -> Result<Self, OpenError> {
        let (source, _version) = open_source(location)?;
        let reader = PagedReader::open(source, dialect, reader_options)?;

        let mut accumulator = ProfileAccumulator::new();
        let names = header_names(reader.header());
        accumulator.set_column_count(names.len());

        Ok(Self {
            total_bytes: reader.source_length(),
            scanned_bytes: reader.byte_order_mark_length(),
            reader,
            page_size: reader_options.page_size,
            names,
            accumulator,
            next_page: 0,
            finished: false,
        })
    }

    /// Reads pages until [`SCAN_STEP`] has passed or the file ends. Blocks on
    /// file or network I/O.
    fn step(&mut self) -> Result<(), OpenError> {
        let started = Instant::now();

        while !self.finished && started.elapsed() < SCAN_STEP {
            let page = self.reader.read_page(self.next_page)?;
            self.next_page += 1;

            for record in &page.records {
                self.accumulator.push_record(&record.fields);
            }

            if let Some(last) = page.records.last() {
                self.scanned_bytes = last.byte_range.end;
            }

            if page.records.len() < self.page_size.get() {
                self.finished = true;
                self.scanned_bytes = self.total_bytes;
            }
        }

        Ok(())
    }
}

impl DelimitedDocument {
    /// The Columns view, once it was drawn.
    pub(super) fn column_profile_view(&self) -> Option<&Entity<ColumnProfileView>> {
        self.columns.view.as_ref()
    }

    /// The epoch of the reader the loaded records came from.
    fn reader_epoch(&self) -> Option<u64> {
        self.loaded().map(|loaded| loaded.reader_epoch)
    }

    /// The profile the Columns view shows, once it was drawn.
    pub fn column_profile(&self) -> Option<&Arc<TableProfile>> {
        self.columns.shown.as_ref()
    }

    /// Whether a full scan of the file is running.
    pub fn is_profiling_whole_file(&self) -> bool {
        self.columns.scan.is_some()
    }

    /// Whether "Profile whole file" can start now: the file is loaded, no
    /// scan runs, the file is not being read again and no dialog is open.
    pub(super) fn can_profile_whole_file(&self) -> bool {
        self.loaded().is_some()
            && !self.is_profiling_whole_file()
            && !self.is_rereading()
            && !self.has_open_dialog()
    }

    /// Builds the Columns view, brings its profile up to date and hands it
    /// the keyboard when it was just shown. Called from `prepare_view`,
    /// which has the window.
    pub(super) fn prepare_columns_view(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.ensure_columns_view(window, cx);
        self.refresh_columns_profile(cx);

        if std::mem::take(&mut self.text.pending_focus)
            && let Some(view) = self.columns.view.clone()
        {
            view.update(cx, |view, cx| view.focus(window, cx));
        }
    }

    fn ensure_columns_view(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.columns.view.is_some() {
            return;
        }

        // A scan started from the pane actions menu before the view was
        // drawn shows as running once it is.
        let scanning = self.columns.scan.is_some();

        let view = cx.new(|cx| {
            let mut view = ColumnProfileView::new(window, cx);
            view.set_projection_enabled(false, cx);
            if scanning {
                view.set_running(Some(ProfileAction::FullScan), cx);
            }
            view.set_actions(
                ProfileActions {
                    refresh: false,
                    sample_rows: None,
                    full_scan: true,
                },
                cx,
            );
            view
        });

        let subscription = cx.subscribe(
            &view,
            |this, _view, event: &ColumnProfileActionRequested, cx| {
                if event.0 == ProfileAction::FullScan {
                    this.profile_whole_file(cx);
                }
            },
        );

        self.columns.view = Some(view);
        self.columns._subscription = Some(subscription);
        self.columns.published = false;
    }

    /// Drops both profiles and stops a running scan when the file was read
    /// again since they were worked out.
    fn sync_columns_epoch(&mut self, cx: &mut Context<Self>) {
        // The phase is matched directly, not through `loaded()`, so that the
        // columns state can be borrowed mutably next to it.
        let DelimitedPhase::Loaded(loaded) = &self.phase else {
            return;
        };

        let epoch = loaded.reader_epoch;
        let records = loaded.page_model.records().len();
        let state = &mut self.columns;

        if state.epoch == epoch && state.loaded.is_some() && state.fed <= records {
            return;
        }

        if state.epoch == epoch && state.catching_up.is_some() {
            return;
        }

        let names = header_names(loaded.page_model.header());
        let mut accumulator = ProfileAccumulator::new();
        accumulator.set_column_count(names.len());

        let stopped_scan = state.scan.take().is_some();

        state.epoch = epoch;
        state.loaded = Some(accumulator);
        state.fed = 0;
        state.published = false;
        state.catching_up = None;
        state.full_scan = None;

        if stopped_scan && let Some(view) = state.view.clone() {
            view.update(cx, |view, cx| view.set_running(None, cx));
        }
    }

    /// Feeds the profile the records loaded since it was last fed and shows
    /// it, unless a full scan's profile is shown.
    fn refresh_columns_profile(&mut self, cx: &mut Context<Self>) {
        self.sync_columns_epoch(cx);

        if self.columns.full_scan.is_some() || self.columns.catching_up.is_some() {
            return;
        }

        let DelimitedPhase::Loaded(loaded) = &self.phase else {
            return;
        };

        let records = loaded.page_model.records();
        let backlog = records.get(self.columns.fed..).unwrap_or_default();

        if backlog.len() > INLINE_PROFILE_RECORDS {
            let fields: Vec<Vec<String>> =
                backlog.iter().map(|record| record.fields.clone()).collect();

            self.catch_up_in_background(fields, cx);
            return;
        }

        if !backlog.is_empty() {
            let fed = records.len();

            if let Some(accumulator) = self.columns.loaded.as_mut() {
                for record in backlog {
                    accumulator.push_record(&record.fields);
                }
            }

            self.columns.fed = fed;
            self.columns.published = false;
        }

        if !self.columns.published {
            self.publish_loaded_profile(cx);
        }
    }

    /// Profiles `fields`, the records after the ones already fed, on the
    /// background executor. The previous profile stays on screen meanwhile.
    fn catch_up_in_background(&mut self, fields: Vec<Vec<String>>, cx: &mut Context<Self>) {
        let Some(mut accumulator) = self.columns.loaded.take() else {
            return;
        };

        let epoch = self.columns.epoch;
        let count = fields.len();

        let step = cx.background_executor().spawn(async move {
            for record in &fields {
                accumulator.push_record(record);
            }

            accumulator
        });

        self.columns.catching_up = Some(cx.spawn(async move |this, cx| {
            let accumulator = step.await;

            cx.update(|cx| {
                this.update(cx, |document, cx| {
                    let state = &mut document.columns;

                    if state.epoch != epoch {
                        return;
                    }

                    state.loaded = Some(accumulator);
                    state.fed += count;
                    state.published = false;
                    state.catching_up = None;

                    cx.notify();
                })
                .ok();
            });
        }));
    }

    fn publish_loaded_profile(&mut self, cx: &mut Context<Self>) {
        let Some(loaded) = self.loaded() else {
            return;
        };
        let Some(accumulator) = self.columns.loaded.as_ref() else {
            return;
        };
        let Some(view) = self.columns.view.clone() else {
            return;
        };

        let names = header_names(loaded.page_model.header());
        let rows = accumulator.rows();
        let profile = Arc::new(table_profile(
            &names,
            accumulator,
            ProfileSource::LoadedRows { rows },
        ));

        self.columns.published = true;
        self.columns.shown = Some(profile.clone());
        show_profile(&view, profile, cx);
    }

    /// Reads every record of the file again on the background executor and
    /// shows the profile of all of them. Shows the Columns view first. Does
    /// nothing when [`Self::can_profile_whole_file`] is false.
    ///
    /// An object is read through the live connection of its profile. A
    /// failure is reported here, the first place it is caught, and the view
    /// keeps the loaded-rows profile.
    pub fn profile_whole_file(&mut self, cx: &mut Context<Self>) {
        if !self.can_profile_whole_file() {
            return;
        }

        let title = self.title();
        let summary = scan_failed_message(&title);

        if self.use_live_connection(summary, cx).is_err() {
            return;
        }

        self.show_view(DelimitedView::Columns, cx);

        let Some(loaded) = self.loaded() else {
            return;
        };

        let location = self.location.clone();
        let dialect = loaded.dialect;
        let reader_options = self.reader_options;
        let epoch = loaded.reader_epoch;

        let open = cx
            .background_executor()
            .spawn(async move { Scan::open(&location, dialect, reader_options) });

        let task = cx.spawn(async move |this, cx| {
            let mut scan = match open.await {
                Ok(scan) => scan,
                Err(error) => {
                    finish_scan(&this, epoch, Err(error), cx);
                    return;
                }
            };

            loop {
                let step = cx.background_executor().spawn(async move {
                    let result = scan.step();
                    (scan, result)
                });

                let (stepped, result) = step.await;
                scan = stepped;

                if let Err(error) = result {
                    finish_scan(&this, epoch, Err(error), cx);
                    return;
                }

                if scan.finished {
                    finish_scan(&this, epoch, Ok(scan), cx);
                    return;
                }

                let progress = scan_progress(scan.scanned_bytes, scan.total_bytes);
                let still_current = cx.update(|cx| {
                    this.update(cx, |document, cx| {
                        if document.reader_epoch() != Some(epoch) {
                            document.stop_profiling_whole_file(cx);
                            cx.notify();
                            return false;
                        }

                        if let Some(view) = document.columns.view.clone() {
                            view.update(cx, |view, cx| view.set_progress(progress, cx));
                        }

                        true
                    })
                    .unwrap_or(false)
                });

                if !still_current {
                    return;
                }
            }
        });

        self.sync_columns_epoch(cx);
        self.columns.scan = Some(task);

        if let Some(view) = self.columns.view.clone() {
            let total = self.loaded().map_or(0, |loaded| loaded.source_length);

            view.update(cx, |view, cx| {
                view.set_running(Some(ProfileAction::FullScan), cx);
                view.set_progress(scan_progress(0, total), cx);
            });
        }

        cx.notify();
    }

    /// Stores the outcome of a full scan started at `epoch`. When the file
    /// was read again meanwhile, the outcome is stale: the scan is only
    /// marked stopped, so it can be started again.
    fn apply_scan_outcome(
        &mut self,
        epoch: u64,
        outcome: Result<Scan, OpenError>,
        cx: &mut Context<Self>,
    ) {
        if self.columns.scan.is_none() {
            return;
        }

        if self.reader_epoch() != Some(epoch) {
            self.stop_profiling_whole_file(cx);
            cx.notify();
            return;
        }

        self.columns.scan = None;

        if let Some(view) = self.columns.view.clone() {
            view.update(cx, |view, cx| view.set_running(None, cx));
        }

        match outcome {
            Ok(scan) => {
                let rows = scan.accumulator.rows();
                let mut profile = table_profile(
                    &scan.names,
                    &scan.accumulator,
                    ProfileSource::FullScan { rows },
                );
                profile.captured_at = Some(Utc::now());

                let profile = Arc::new(profile);
                self.columns.full_scan = Some(profile.clone());
                self.columns.shown = Some(profile.clone());

                if let Some(view) = self.columns.view.clone() {
                    show_profile(&view, profile, cx);
                }
            }

            Err(error) => {
                let summary = scan_failed_message(&self.title());
                report_error(open_error_to_user_facing(&error, summary), cx);
            }
        }

        cx.notify();
    }

    /// Stops a running full scan. Called when the file starts being read
    /// again, whose new reader makes the scan's dialect or bytes stale.
    pub(super) fn stop_profiling_whole_file(&mut self, cx: &mut Context<Self>) {
        if self.columns.scan.take().is_none() {
            return;
        }

        if let Some(view) = self.columns.view.clone() {
            view.update(cx, |view, cx| view.set_running(None, cx));
        }
    }
}

/// Hands the outcome of a scan to the document, from the scan's task.
fn finish_scan(
    this: &WeakEntity<DelimitedDocument>,
    epoch: u64,
    outcome: Result<Scan, OpenError>,
    cx: &mut AsyncApp,
) {
    cx.update(|cx| {
        this.update(cx, |document, cx| {
            document.apply_scan_outcome(epoch, outcome, cx);
        })
        .ok();
    });
}

fn show_profile(view: &Entity<ColumnProfileView>, profile: Arc<TableProfile>, cx: &mut App) {
    let columns = profile.columns.len();

    view.update(cx, |view, cx| {
        view.set_profile(profile, ColumnProjection::all(columns), cx);
    });
}

fn scan_failed_message(file_name: &str) -> String {
    dbflux_i18n::t!(
        "document.delimited.columns.error.scan_failed",
        file = file_name
    )
}

/// What a running scan has read, as the view's progress text.
fn scan_progress(scanned: u64, total: u64) -> String {
    dbflux_i18n::t!(
        "document.delimited.columns.progress",
        scanned = format_bytes(scanned),
        total = format_bytes(total)
    )
}

/// The column names of a file whose header record is `header`: the header's
/// own fields, and a positional name for an empty one. No header has no
/// names; the profile then names its columns by position.
pub(super) fn header_names(header: Option<&Record>) -> Vec<String> {
    header.map_or_else(Vec::new, |record| {
        record
            .fields
            .iter()
            .enumerate()
            .map(|(column, name)| {
                if name.is_empty() {
                    positional_name(column)
                } else {
                    name.clone()
                }
            })
            .collect()
    })
}

/// The profile of the columns `accumulator` holds, named by `names` (and by
/// position past them), from `source`.
pub(super) fn table_profile(
    names: &[String],
    accumulator: &ProfileAccumulator,
    source: ProfileSource,
) -> TableProfile {
    let columns = accumulator
        .summaries()
        .into_iter()
        .enumerate()
        .map(|(column, summary)| {
            let name = names
                .get(column)
                .cloned()
                .unwrap_or_else(|| positional_name(column));

            column_profile(name, summary)
        })
        .collect();

    let mut profile = TableProfile::new(columns, source);
    profile.row_count = Some(accumulator.rows());
    profile
}

fn column_profile(name: String, summary: ColumnSummary) -> ColumnProfile {
    let ColumnSummary {
        kind,
        null_count,
        distinct_count,
        distinct_exact,
        range,
        top_values,
    } = summary;

    ColumnProfile {
        null_count: Some(null_count),
        distinct_count: Some(distinct_count),
        distinct_exact,
        range: range.map(|bounds| ValueRange {
            min: bounds.min.into(),
            max: bounds.max.into(),
            exact: true,
        }),
        top_values: top_values
            .into_iter()
            .map(|(value, count)| ValueCount {
                value: value.into(),
                count,
            })
            .collect(),
        ..ColumnProfile::new(name, kind_label(kind))
    }
}

/// The translated name of an inferred value kind.
pub(super) fn kind_label(kind: ValueKind) -> String {
    let key = match kind {
        ValueKind::Integer => "integer",
        ValueKind::Decimal => "decimal",
        ValueKind::Boolean => "boolean",
        ValueKind::Date => "date",
        ValueKind::Timestamp => "timestamp",
        ValueKind::Text => "text",
    };

    dbflux_i18n::t!(&format!("document.delimited.columns.type.{key}"))
}
