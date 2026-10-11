//! `ColumnProfileView`: the Columns view of a table, one row per column of a
//! [`TableProfile`] with what the source knows about it (codec, size on
//! disk, compression ratio, share of the table, distinct values, nulls and
//! value range) and an eye that adds the column to the Data projection or
//! drops it.
//!
//! An eye toggle changes the applied projection at once and emits
//! [`ProjectionChanged`]; dropping the last shown column is refused, so the
//! Data view always has a column to read.
//!
//! Above the columns the view shows the table's storage facts (engine, keys,
//! rows and sizes) and a line naming where the statistics came from and when
//! they were read. The host decides which profile actions it offers (refresh,
//! profile a sample, profile the whole file); a click emits
//! [`ColumnProfileActionRequested`] and the host runs the work. Columns about
//! storage (codec, size, ratio, share) are left out when no column of the
//! profile knows any of them, instead of showing a column of dashes.

use std::ops::Range;
use std::sync::Arc;

use chrono::{DateTime, Local, Utc};
use dbflux_core::keymap_types::{Command, ContextId};
use dbflux_core::{
    ColumnBadge, ColumnProfile, ColumnProjection, ProfileSource, TableFact, TableFactKind,
    TableProfile,
};
use gpui::prelude::*;
use gpui::{
    Entity, EventEmitter, FocusHandle, Focusable, FontWeight, Hsla, IntoElement, ParentElement,
    Pixels, Render, Role, ScrollHandle, ScrollStrategy, SharedString, Styled, Subscription,
    Toggled, UniformListScrollHandle, Window, div, px, uniform_list,
};
use gpui_component::ActiveTheme;
use gpui_component::scroll::Scrollbar;

use crate::actions::RunCommand;
use crate::components::column_facts::{
    UNKNOWN_FACT, format_bytes, format_count, format_optional_bytes, format_percent, format_range,
    format_ratio, group_thousands,
};
use crate::components::column_projection::ProjectionChanged;
use crate::composites::EmptyState;
use crate::controls::{Button, Input, InputEvent, InputMoveDown, InputState};
use crate::icons::AppIcon;
use crate::primitives::{Badge, BadgeTone, Icon, LoadingState, Spinner};
use crate::tokens::{ChromeColors, Fields, FontSizes, GridMetrics, Spacing};

/// Height of the filter and sort row, and of the footer.
const BAR_HEIGHT: Pixels = px(46.0);

/// Height of the column header row.
const HEADER_HEIGHT: Pixels = px(34.0);

/// Height of one column row: name over type.
const ROW_HEIGHT: Pixels = px(44.0);

/// Width of the filter field.
const FILTER_WIDTH: Pixels = px(280.0);

/// Fixed widths of the table's columns, in display order. The distribution
/// column takes the rest of the row.
const EYE_WIDTH: Pixels = px(34.0);
const NAME_WIDTH: Pixels = px(260.0);
const CODEC_WIDTH: Pixels = px(108.0);
const ON_DISK_WIDTH: Pixels = px(96.0);
const RATIO_WIDTH: Pixels = px(70.0);
const SHARE_WIDTH: Pixels = px(150.0);
const DISTINCT_WIDTH: Pixels = px(104.0);
const NULLS_WIDTH: Pixels = px(64.0);

/// The narrowest the distribution column gets before the table scrolls
/// sideways.
const DISTRIBUTION_MIN_WIDTH: Pixels = px(200.0);

/// The narrowest the table gets: every fixed column plus the narrowest
/// distribution. A narrower panel scrolls the header and rows sideways
/// together. The storage columns count only when they are shown.
fn table_min_width(storage: StorageColumns) -> Pixels {
    let codec = if storage.codec { CODEC_WIDTH } else { px(0.0) };
    let sizes = if storage.sizes {
        ON_DISK_WIDTH + RATIO_WIDTH + SHARE_WIDTH
    } else {
        px(0.0)
    };

    EYE_WIDTH + NAME_WIDTH + codec + sizes + DISTINCT_WIDTH + NULLS_WIDTH + DISTRIBUTION_MIN_WIDTH
}

/// Which storage columns the table shows: each only when some column of
/// the profile knows it, so a source without sizes shows no column of dashes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct StorageColumns {
    codec: bool,
    /// On disk, ratio and share of table.
    sizes: bool,
}

/// How many of the most common values the distribution cell lists.
const TOP_VALUES_SHOWN: usize = 3;

/// A profile action a host can offer above the columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProfileAction {
    /// Read the metadata profile again.
    Refresh,
    /// Compute distributions from a bounded sample of rows.
    Sample,
    /// Compute distributions from every row.
    FullScan,
}

/// The profile actions a host offers. Each one shows a button only when set.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProfileActions {
    pub refresh: bool,
    /// The rows a sample reads, shown on the button. `None` hides it.
    pub sample_rows: Option<u64>,
    pub full_scan: bool,
}

/// Emitted when the user asks for a [`ProfileAction`]; the host runs it and
/// reports back with [`ColumnProfileView::set_running`] and `set_profile`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnProfileActionRequested(pub ProfileAction);

/// Height of the horizontal scrollbar strip over the bottom of the rows.
const SCROLLBAR_HEIGHT: Pixels = px(12.0); // guardrail-allow: matches the data table scrollbar width

/// Share bar: 6 px tall, its label 30 px wide.
const SHARE_BAR_HEIGHT: Pixels = px(6.0); // guardrail-allow: share bar height from the artboard
const SHARE_LABEL_WIDTH: Pixels = px(30.0);

/// The order of the rows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ColumnSort {
    /// Schema order.
    #[default]
    TableOrder,
    /// Largest size on disk first; columns of unknown size last.
    Size,
    /// By name, ignoring case.
    Name,
}

impl ColumnSort {
    /// The next order of the sort button's cycle.
    pub fn next(self) -> Self {
        match self {
            Self::TableOrder => Self::Size,
            Self::Size => Self::Name,
            Self::Name => Self::TableOrder,
        }
    }

    fn label(self) -> String {
        match self {
            Self::TableOrder => dbflux_i18n::t!("components.column_profile.sort.table_order"),
            Self::Size => dbflux_i18n::t!("components.column_profile.sort.size"),
            Self::Name => dbflux_i18n::t!("components.column_profile.sort.name"),
        }
    }
}

/// The text of one column row, formatted once when the profile is set.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProfileRow {
    pub(crate) name: SharedString,
    pub(crate) type_name: SharedString,
    pub(crate) badges: Vec<SharedString>,
    pub(crate) codec: SharedString,
    pub(crate) on_disk: SharedString,
    pub(crate) ratio: SharedString,
    /// Share of the table's bytes, from 0.0 to 1.0, when known.
    pub(crate) share: Option<f32>,
    pub(crate) share_label: SharedString,
    pub(crate) distinct: SharedString,
    pub(crate) nulls: SharedString,
    pub(crate) distribution: SharedString,
    compressed_bytes: Option<u64>,
    sort_name: String,
}

impl ProfileRow {
    pub(crate) fn new(column: &ColumnProfile, table: &TableProfile) -> Self {
        let share = column.share_of(table.total_compressed_bytes);

        let distribution = match (&column.range, shows_top_values(column)) {
            (_, true) => format_top_values(column, profiled_rows(table)),
            (Some(range), false) => format_range(&range.min, &range.max, range.exact),
            (None, false) => UNKNOWN_FACT.to_string(),
        };

        let distinct = match column.distinct_count {
            Some(count) if !column.distinct_exact => format!("≈ {}", group_thousands(count)),
            Some(count) => group_thousands(count),
            None => format_count(None),
        };

        Self {
            name: SharedString::from(column.name.to_string()),
            type_name: SharedString::from(column.type_name.to_string()),
            badges: column
                .badges
                .iter()
                .map(|badge| badge_label(*badge).into())
                .collect(),
            codec: column
                .codec
                .as_deref()
                .unwrap_or(UNKNOWN_FACT)
                .to_string()
                .into(),
            on_disk: format_optional_bytes(column.compressed_bytes).into(),
            ratio: format_ratio(column.compression_ratio()).into(),
            share: share.map(|share| share as f32),
            share_label: format_percent(share).into(),
            distinct: distinct.into(),
            nulls: format_percent(column.null_fraction(table.row_count)).into(),
            distribution: distribution.into(),
            compressed_bytes: column.compressed_bytes,
            sort_name: column.name.to_lowercase(),
        }
    }
}

/// The rows the distribution facts were computed over, used to turn top
/// value counts into shares.
fn profiled_rows(table: &TableProfile) -> Option<u64> {
    match table.distribution_source.unwrap_or(table.source_label) {
        ProfileSource::LoadedRows { rows }
        | ProfileSource::FullScan { rows }
        | ProfileSource::Sample { rows } => Some(rows),
        ProfileSource::FileFooter
        | ProfileSource::Catalog(_)
        | ProfileSource::PlannerStatistics(_) => table.row_count,
    }
}

/// Most distinct values a column may have for its most common values to say
/// more than its range.
const TOP_VALUES_MAX_DISTINCT: u64 = 50;

/// Whether the distribution cell lists the most common values instead of the
/// range: only when the most common value repeats and the column has few
/// distinct values. In a column of nearly unique values every top value is a
/// fraction of a percent, which says nothing the range does not.
fn shows_top_values(column: &ColumnProfile) -> bool {
    let Some(first) = column.top_values.first() else {
        return false;
    };

    if column.range.is_none() {
        return true;
    }

    first.count > 1
        && column
            .distinct_count
            .is_some_and(|distinct| distinct <= TOP_VALUES_MAX_DISTINCT)
}

/// The most common values with their share of the profiled rows
/// (`US 31% · DE 12% · IN 9%`), or with their counts when the rows are
/// unknown.
fn format_top_values(column: &ColumnProfile, rows: Option<u64>) -> String {
    column
        .top_values
        .iter()
        .take(TOP_VALUES_SHOWN)
        .map(|value| {
            let share = rows
                .filter(|rows| *rows > 0)
                .map(|rows| value.count as f64 / rows as f64);

            match share {
                Some(share) => format!("{} {}", value.value, format_percent(Some(share))),
                None => format!("{} ×{}", value.value, group_thousands(value.count)),
            }
        })
        .collect::<Vec<_>>()
        .join(" · ")
}

pub(crate) fn badge_label(badge: ColumnBadge) -> String {
    match badge {
        ColumnBadge::SortKey { position: None } => {
            dbflux_i18n::t!("components.column_profile.badge.sort_key")
        }
        ColumnBadge::SortKey {
            position: Some(position),
        } => dbflux_i18n::t!(
            "components.column_profile.badge.sort_key_position",
            position = position
        ),
        ColumnBadge::PartitionKey => {
            dbflux_i18n::t!("components.column_profile.badge.partition_key")
        }
        ColumnBadge::DistributionKey => {
            dbflux_i18n::t!("components.column_profile.badge.distribution_key")
        }
    }
}

/// What `source` is, as the lead of the source line.
pub fn source_label(source: ProfileSource) -> String {
    match source {
        ProfileSource::FileFooter => {
            dbflux_i18n::t!("components.column_profile.source.file_footer")
        }
        ProfileSource::Catalog(name) => {
            dbflux_i18n::t!("components.column_profile.source.catalog", name = name)
        }
        ProfileSource::PlannerStatistics(name) => dbflux_i18n::t!(
            "components.column_profile.source.planner_statistics",
            name = name
        ),
        ProfileSource::LoadedRows { rows } => dbflux_i18n::t!(
            "components.column_profile.source.loaded_rows",
            rows = group_thousands(rows)
        ),
        ProfileSource::FullScan { rows } => dbflux_i18n::t!(
            "components.column_profile.source.full_scan",
            rows = group_thousands(rows)
        ),
        ProfileSource::Sample { rows } => dbflux_i18n::t!(
            "components.column_profile.source.sample",
            rows = group_thousands(rows)
        ),
    }
}

/// When a profile was read, in local time: the time alone for today, the
/// date and time otherwise.
pub fn read_at_label(captured_at: DateTime<Utc>, now: DateTime<Local>) -> String {
    let local = captured_at.with_timezone(&Local);

    let time = if local.date_naive() == now.date_naive() {
        local.format("%H:%M:%S").to_string()
    } else {
        local.format("%Y-%m-%d %H:%M").to_string()
    };

    dbflux_i18n::t!("components.column_profile.source.read_at", time = time)
}

/// When a file was last modified, as the source line of its footer says it.
fn file_modified_label(modified: DateTime<Utc>, now: DateTime<Local>) -> String {
    let local = modified.with_timezone(&Local);

    let time = if local.date_naive() == now.date_naive() {
        local.format("%H:%M:%S").to_string()
    } else {
        local.format("%Y-%m-%d %H:%M").to_string()
    };

    dbflux_i18n::t!(
        "components.column_profile.source.file_modified",
        time = time
    )
}

/// The source lines: where the sizes came from and when, then, on a second
/// line, where the distributions came from and when, when a sample filled
/// them.
fn source_lines(profile: &TableProfile, now: DateTime<Local>) -> Vec<String> {
    let with_time = |source: ProfileSource, captured_at: Option<DateTime<Utc>>| {
        let lead = source_label(source);

        match (source, captured_at) {
            // A footer is as old as its file, not as the moment it was read.
            (ProfileSource::FileFooter, Some(modified)) => {
                format!("{lead} · {}", file_modified_label(modified, now))
            }
            (_, Some(captured_at)) => format!("{lead} · {}", read_at_label(captured_at, now)),
            (_, None) => lead,
        }
    };

    let mut lines = vec![with_time(profile.source_label, profile.captured_at)];

    if let Some(source) = profile.distribution_source {
        lines.push(with_time(source, profile.distribution_captured_at));
    }

    lines
}

fn fact_label(kind: &TableFactKind) -> String {
    let key = match kind {
        TableFactKind::Engine => "engine",
        TableFactKind::SortKey => "sort_key",
        TableFactKind::PartitionKey => "partition_key",
        TableFactKind::PrimaryKey => "primary_key",
        TableFactKind::SamplingKey => "sampling_key",
        TableFactKind::Ttl => "ttl",
        TableFactKind::DistributionStyle => "distribution_style",
        TableFactKind::ActiveParts => "active_parts",
        TableFactKind::RowGroups => "row_groups",
        TableFactKind::Unsorted => "unsorted",
        TableFactKind::CreatedBy => "created_by",
        TableFactKind::StatisticsUpdated => "statistics_updated",
        TableFactKind::Other(label) => return label.to_string(),
    };

    dbflux_i18n::t!(&format!("components.column_profile.fact.{key}"))
}

/// The facts strip: the source's facts, then the row count and the sizes
/// the profile knows.
fn facts_strip(profile: &TableProfile) -> Vec<(SharedString, SharedString)> {
    let mut facts: Vec<(SharedString, SharedString)> = profile
        .facts
        .iter()
        .map(|TableFact { kind, value }| (fact_label(kind).into(), value.to_string().into()))
        .collect();

    let fact = |key: &str| dbflux_i18n::t!(&format!("components.column_profile.fact.{key}"));

    if let Some(rows) = profile.row_count {
        let rows = if profile.row_count_exact {
            group_thousands(rows)
        } else {
            format!("≈ {}", group_thousands(rows))
        };

        facts.push((fact("rows").into(), rows.into()));
    }

    if let Some(bytes) = profile.total_compressed_bytes {
        facts.push((fact("on_disk").into(), format_bytes(bytes).into()));
    }

    if let Some(bytes) = profile.total_uncompressed_bytes {
        facts.push((fact("raw").into(), format_bytes(bytes).into()));
    }

    let ratio = profile
        .total_compressed_bytes
        .filter(|bytes| *bytes > 0)
        .zip(profile.total_uncompressed_bytes)
        .map(|(compressed, raw)| raw as f64 / compressed as f64);

    if ratio.is_some() {
        facts.push((fact("ratio").into(), format_ratio(ratio).into()));
    }

    facts
}

/// The debug selectors of the action buttons, in the order they render.
fn action_selectors(actions: &ProfileActions) -> Vec<&'static str> {
    let mut selectors = Vec::new();

    if actions.sample_rows.is_some() {
        selectors.push("column-profile-sample");
    }

    if actions.full_scan {
        selectors.push("column-profile-full-scan");
    }

    if actions.refresh {
        selectors.push("column-profile-refresh");
    }

    selectors
}

/// The storage columns `profile` has anything to show in.
fn storage_columns(profile: &TableProfile) -> StorageColumns {
    StorageColumns {
        codec: profile.columns.iter().any(|column| column.codec.is_some()),
        sizes: profile
            .columns
            .iter()
            .any(|column| column.compressed_bytes.is_some() || column.uncompressed_bytes.is_some()),
    }
}

pub struct ColumnProfileView {
    content: LoadingState<Arc<TableProfile>>,
    rows: Vec<ProfileRow>,
    applied: ColumnProjection,
    filter: Entity<InputState>,
    query: String,
    sort: ColumnSort,
    /// Column indices the filter shows, in the current sort order.
    visible: Vec<usize>,
    /// Position in `visible` the keyboard points at.
    cursor: Option<usize>,
    table_focus: FocusHandle,
    table_scroll: UniformListScrollHandle,
    table_hscroll: ScrollHandle,
    sort_label: SharedString,
    /// The source line, joined, for tests and screen readers.
    source_label: SharedString,
    /// The source lines as drawn, one per source.
    source_lines: Vec<SharedString>,
    facts: Vec<(SharedString, SharedString)>,
    storage: StorageColumns,
    /// Whether the eye column and the footer's projection text are shown.
    /// A source the Data view cannot project (a CSV file) turns them off.
    projection_enabled: bool,
    actions: ProfileActions,
    /// The action the host is running, which disables every action button.
    running: Option<ProfileAction>,
    /// What the running action has done so far, such as bytes scanned.
    progress: Option<SharedString>,
    footer_counts: SharedString,
    _subscriptions: Vec<Subscription>,
}

impl ColumnProfileView {
    /// A view that waits for its profile.
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let filter = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "components.column_profile.filter_placeholder"
            ))
        });

        let subscription = cx.subscribe(&filter, |this, _filter, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Change) {
                this.query = this.filter.read(cx).value().to_string();
                this.refresh_visible(cx);
            }
        });

        Self {
            content: LoadingState::Loading,
            rows: Vec::new(),
            applied: ColumnProjection::none(0),
            filter,
            query: String::new(),
            sort: ColumnSort::default(),
            visible: Vec::new(),
            cursor: None,
            table_focus: cx.focus_handle(),
            table_scroll: UniformListScrollHandle::new(),
            table_hscroll: ScrollHandle::new(),
            sort_label: ColumnSort::default().label().into(),
            source_label: SharedString::default(),
            source_lines: Vec::new(),
            facts: Vec::new(),
            storage: StorageColumns {
                codec: true,
                sizes: true,
            },
            projection_enabled: true,
            actions: ProfileActions::default(),
            running: None,
            progress: None,
            footer_counts: SharedString::default(),
            _subscriptions: vec![subscription],
        }
    }

    /// Shows or hides the eye column and the footer's projection text. With
    /// them hidden the rows cannot change what the Data view reads.
    pub fn set_projection_enabled(&mut self, enabled: bool, cx: &mut Context<Self>) {
        self.projection_enabled = enabled;
        self.refresh_footer();
        cx.notify();
    }

    pub fn projection_enabled(&self) -> bool {
        self.projection_enabled
    }

    /// Sets the profile actions offered above the columns.
    pub fn set_actions(&mut self, actions: ProfileActions, cx: &mut Context<Self>) {
        self.actions = actions;
        cx.notify();
    }

    /// Marks `action` as running (or none), which disables the action
    /// buttons, and clears the progress text.
    pub fn set_running(&mut self, action: Option<ProfileAction>, cx: &mut Context<Self>) {
        self.running = action;
        self.progress = None;
        cx.notify();
    }

    /// Shows what the running action has done so far.
    pub fn set_progress(&mut self, progress: impl Into<SharedString>, cx: &mut Context<Self>) {
        self.progress = Some(progress.into());
        cx.notify();
    }

    pub fn running(&self) -> Option<ProfileAction> {
        self.running
    }

    /// The source line as shown, for tests and screen readers.
    pub fn source_text(&self) -> &SharedString {
        &self.source_label
    }

    /// The facts strip as shown: label and value pairs.
    pub fn facts(&self) -> &[(SharedString, SharedString)] {
        &self.facts
    }

    /// Whether the codec column is shown.
    pub fn shows_codec(&self) -> bool {
        self.storage.codec
    }

    /// Whether the size columns (on disk, ratio, share) are shown.
    pub fn shows_sizes(&self) -> bool {
        self.storage.sizes
    }

    /// Asks the host to run `action`, unless another one is running or the
    /// host does not offer it.
    pub fn request_action(&mut self, action: ProfileAction, cx: &mut Context<Self>) {
        let offered = match action {
            ProfileAction::Refresh => self.actions.refresh,
            ProfileAction::Sample => self.actions.sample_rows.is_some(),
            ProfileAction::FullScan => self.actions.full_scan,
        };

        if !offered || self.running.is_some() {
            return;
        }

        cx.emit(ColumnProfileActionRequested(action));
    }

    /// Shows the loading state until a profile or an error arrives.
    pub fn set_loading(&mut self, cx: &mut Context<Self>) {
        self.content = LoadingState::Loading;
        self.rows.clear();
        self.visible.clear();
        self.cursor = None;
        cx.notify();
    }

    /// Shows why the profile could not be read.
    pub fn set_failed(&mut self, message: impl Into<SharedString>, cx: &mut Context<Self>) {
        self.content = LoadingState::Failed {
            message: message.into(),
        };
        self.rows.clear();
        self.visible.clear();
        self.cursor = None;
        cx.notify();
    }

    /// Shows `profile` with the columns of `applied` marked as shown in Data.
    pub fn set_profile(
        &mut self,
        profile: Arc<TableProfile>,
        applied: ColumnProjection,
        cx: &mut Context<Self>,
    ) {
        self.rows = profile
            .columns
            .iter()
            .map(|column| ProfileRow::new(column, &profile))
            .collect();
        let lines = source_lines(&profile, Local::now());
        self.source_label = lines.join(" · ").into();
        self.source_lines = lines.into_iter().map(SharedString::from).collect();
        self.facts = facts_strip(&profile);
        self.storage = storage_columns(&profile);
        self.content = LoadingState::Loaded(profile);
        self.applied = applied;
        self.cursor = None;
        self.refresh_footer();
        self.refresh_visible(cx);
    }

    /// Replaces the applied projection after the host changed it elsewhere,
    /// such as from the projection picker.
    pub fn set_applied(&mut self, applied: ColumnProjection, cx: &mut Context<Self>) {
        self.applied = applied;
        self.refresh_footer();
        cx.notify();
    }

    pub fn applied(&self) -> &ColumnProjection {
        &self.applied
    }

    pub fn sort(&self) -> ColumnSort {
        self.sort
    }

    pub fn set_sort(&mut self, sort: ColumnSort, cx: &mut Context<Self>) {
        self.sort = sort;
        self.sort_label = sort.label().into();
        self.refresh_visible(cx);
    }

    /// Moves to the next order of the sort button's cycle: table order,
    /// size, name.
    pub fn cycle_sort(&mut self, cx: &mut Context<Self>) {
        self.set_sort(self.sort.next(), cx);
    }

    /// Replaces the filter text and narrows the rows to match it.
    pub fn set_filter_query(&mut self, query: &str, window: &mut Window, cx: &mut Context<Self>) {
        self.filter.update(cx, |filter, cx| {
            filter.set_value(query.to_string(), window, cx)
        });
        self.query = query.to_string();
        self.refresh_visible(cx);
    }

    /// The column indices the filter shows, in the current sort order.
    pub fn visible_columns(&self) -> &[usize] {
        &self.visible
    }

    /// The footer's column counts: every column of the profile and how many
    /// are shown in Data, whatever the filter shows.
    pub fn footer_counts(&self) -> &SharedString {
        &self.footer_counts
    }

    /// Moves keyboard focus to the column rows.
    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.cursor.is_none() && !self.visible.is_empty() {
            self.cursor = Some(0);
        }

        self.table_focus.focus(window, cx);
        cx.notify();
    }

    /// Moves keyboard focus to the filter field.
    pub fn focus_filter(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let handle = self.filter.read(cx).focus_handle(cx);
        handle.focus(window, cx);
        cx.notify();
    }

    /// Adds the column at `index` to the Data projection or drops it, and
    /// emits the new projection. Returns false, emitting nothing, when the
    /// toggle would leave the projection without columns.
    pub fn toggle_column(&mut self, index: usize, cx: &mut Context<Self>) -> bool {
        if !self.projection_enabled || index >= self.applied.total_count() {
            return false;
        }

        let mut next = self.applied.clone();
        next.toggle(index);

        if !next.is_applicable() {
            return false;
        }

        self.applied = next.clone();
        self.refresh_footer();
        cx.emit(ProjectionChanged(next));
        cx.notify();
        true
    }

    fn refresh_footer(&mut self) {
        self.footer_counts = if self.projection_enabled {
            dbflux_i18n::t!(
                "components.column_profile.footer.counts",
                total = self.rows.len(),
                shown = self.applied.selected_count()
            )
        } else {
            dbflux_i18n::t!(
                "components.column_profile.footer.counts_only",
                total = self.rows.len()
            )
        }
        .into();
    }

    /// Recomputes the rows the filter shows, in sort order, keeping the
    /// cursor on the same column when it is still shown.
    fn refresh_visible(&mut self, cx: &mut Context<Self>) {
        let cursor_column = self
            .cursor
            .and_then(|position| self.visible.get(position).copied());

        let mut visible = match self.content.loaded() {
            Some(profile) => self.applied.filter(profile, &self.query),
            None => Vec::new(),
        };

        match self.sort {
            ColumnSort::TableOrder => {}
            ColumnSort::Size => visible.sort_by_key(|index| {
                let bytes = self.rows.get(*index).and_then(|row| row.compressed_bytes);
                (bytes.is_none(), std::cmp::Reverse(bytes), *index)
            }),
            ColumnSort::Name => visible.sort_by(|left, right| {
                let name = |index: &usize| self.rows.get(*index).map(|row| row.sort_name.as_str());
                name(left).cmp(&name(right)).then(left.cmp(right))
            }),
        }

        self.visible = visible;

        self.cursor = match cursor_column {
            Some(column) => self.visible.iter().position(|index| *index == column),
            None => None,
        }
        .or_else(|| self.cursor.filter(|_| !self.visible.is_empty()).map(|_| 0));

        cx.notify();
    }

    fn move_cursor(&mut self, forward: bool, cx: &mut Context<Self>) {
        let count = self.visible.len();

        if count == 0 {
            self.cursor = None;
            return;
        }

        let next = match (self.cursor, forward) {
            (Some(position), true) => (position + 1).min(count - 1),
            (Some(position), false) => position.saturating_sub(1),
            (None, _) => 0,
        };

        self.cursor = Some(next);
        self.table_scroll
            .scroll_to_item(next, ScrollStrategy::Nearest);
        cx.notify();
    }

    /// Keys of the column rows: the `Dropdown` keys move the cursor and
    /// Space toggles the eye of the row under it. The `Dropdown` context's
    /// `s` (`SaveQuery`) stops here, so a host that saves on it does not save
    /// from the rows. Other keys propagate.
    fn handle_run_command(
        &mut self,
        action: &RunCommand,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match Command::from_action_id(&action.command) {
            Some(Command::SelectNext) => self.move_cursor(true, cx),
            Some(Command::SelectPrev) => self.move_cursor(false, cx),
            Some(Command::ExpandCollapse) => {
                if let Some(index) = self
                    .cursor
                    .and_then(|position| self.visible.get(position).copied())
                {
                    self.toggle_column(index, cx);
                }
            }
            Some(Command::SaveQuery) => {}
            _ => cx.propagate(),
        }
    }

    fn render_toolbar(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let muted = theme.muted_foreground;

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(Spacing::SM)
            .h(BAR_HEIGHT)
            .px(Spacing::MD)
            .child(
                div()
                    .id("column-profile-filter-row")
                    .w(FILTER_WIDTH)
                    .capture_action(cx.listener(|this, _: &InputMoveDown, window, cx| {
                        this.focus(window, cx);
                        cx.stop_propagation();
                    }))
                    .child(
                        Input::new(&self.filter)
                            .id("column-profile-filter")
                            .w_full()
                            .prefix(
                                Icon::new(AppIcon::Search)
                                    .size(Fields::LEADING_ICON)
                                    .color(muted),
                            ),
                    ),
            )
            .child(
                Button::new("column-profile-sort", self.sort_label.clone())
                    .icon(AppIcon::ArrowUpDown)
                    .on_click(cx.listener(|this, _event, _window, cx| this.cycle_sort(cx))),
            )
            .child(div().flex_1())
            .when(!self.source_label.is_empty(), |toolbar| {
                toolbar.child(
                    div()
                        .id("column-profile-source")
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .min_w_0()
                        .text_size(FontSizes::XS)
                        .text_color(muted)
                        .child(
                            Icon::new(AppIcon::HardDrive)
                                .size(Fields::LEADING_ICON)
                                .color(muted),
                        )
                        .child(
                            div().flex().flex_col().min_w_0().children(
                                self.source_lines
                                    .iter()
                                    .map(|line| div().truncate().child(line.clone())),
                            ),
                        ),
                )
            })
            .when_some(self.progress.clone(), |toolbar, progress| {
                toolbar.child(
                    div()
                        .id("column-profile-progress")
                        .flex()
                        .items_center()
                        .gap(Spacing::XS)
                        .whitespace_nowrap()
                        .text_size(FontSizes::XS)
                        .text_color(muted)
                        .child(Spinner::new(1))
                        .child(progress),
                )
            })
            .children(self.action_buttons(cx))
    }

    fn action_buttons(&self, cx: &Context<Self>) -> Vec<gpui::AnyElement> {
        let busy = self.running.is_some();
        let mut buttons = Vec::new();

        let label = |action: ProfileAction, idle: String| {
            if self.running == Some(action) {
                dbflux_i18n::t!("components.column_profile.action.running")
            } else {
                idle
            }
        };

        if let Some(rows) = self.actions.sample_rows {
            buttons.push(
                Button::new(
                    "column-profile-sample",
                    label(
                        ProfileAction::Sample,
                        dbflux_i18n::t!(
                            "components.column_profile.action.sample",
                            rows = group_thousands(rows)
                        ),
                    ),
                )
                .icon(AppIcon::ChartBar)
                .disabled(busy)
                .on_click(cx.listener(|this, _event, _window, cx| {
                    this.request_action(ProfileAction::Sample, cx)
                }))
                .into_any_element(),
            );
        }

        if self.actions.full_scan {
            buttons.push(
                Button::new(
                    "column-profile-full-scan",
                    label(
                        ProfileAction::FullScan,
                        dbflux_i18n::t!("components.column_profile.action.full_scan"),
                    ),
                )
                .icon(AppIcon::ChartBar)
                .disabled(busy)
                .on_click(cx.listener(|this, _event, _window, cx| {
                    this.request_action(ProfileAction::FullScan, cx)
                }))
                .into_any_element(),
            );
        }

        if self.actions.refresh {
            buttons.push(
                Button::new(
                    "column-profile-refresh",
                    dbflux_i18n::t!("components.column_profile.action.refresh"),
                )
                .icon(AppIcon::RefreshCcw)
                .icon_only()
                .ghost()
                .disabled(busy)
                .on_click(cx.listener(|this, _event, _window, cx| {
                    this.request_action(ProfileAction::Refresh, cx)
                }))
                .into_any_element(),
            );
        }

        buttons
            .into_iter()
            .zip(action_selectors(&self.actions))
            .map(|(button, selector)| {
                div()
                    .debug_selector(move || selector.to_string())
                    .child(button)
                    .into_any_element()
            })
            .collect()
    }

    /// The table's storage facts as label and value pairs on one wrapping
    /// row. Nothing when the profile has no facts.
    fn render_facts(&self, cx: &Context<Self>) -> Option<gpui::AnyElement> {
        if self.facts.is_empty() {
            return None;
        }

        let theme = cx.theme();

        Some(
            div()
                .id("column-profile-facts")
                .flex()
                .flex_wrap()
                .flex_shrink_0()
                .items_center()
                .gap_x(Spacing::LG)
                .gap_y(Spacing::XS)
                .px(Spacing::MD)
                .py(Spacing::SM)
                .border_b_1()
                .border_color(theme.border)
                .text_size(FontSizes::XS)
                .children(
                    self.facts
                        .iter()
                        .enumerate()
                        .map(|(index, (label, value))| {
                            div()
                                .debug_selector(move || format!("column-profile-fact-{index}"))
                                .flex()
                                .items_center()
                                .gap(Spacing::XS)
                                .min_w_0()
                                .child(
                                    div()
                                        .whitespace_nowrap()
                                        .text_color(theme.muted_foreground)
                                        .child(label.clone()),
                                )
                                .child(
                                    div()
                                        .truncate()
                                        .font_family(crate::fonts::editor_family(cx))
                                        .text_color(theme.foreground)
                                        .child(value.clone()),
                                )
                        }),
                )
                .into_any_element(),
        )
    }

    fn render_header(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let cell = |selector: &'static str, width: Option<Pixels>, label: String, right: bool| {
            div()
                .debug_selector(move || selector.to_string())
                .px(GridMetrics::CELL_PADDING_X)
                .when_some(width, |cell, width| cell.w(width).flex_shrink_0())
                .when(width.is_none(), |cell| cell.flex_1().min_w_0())
                .when(right, |cell| cell.flex().justify_end())
                .whitespace_nowrap()
                .child(label)
        };

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .h(HEADER_HEIGHT)
            .bg(theme.table_head)
            .text_size(GridMetrics::TYPE_FONT)
            .text_color(theme.muted_foreground)
            .when(self.projection_enabled, |header| {
                header.child(div().w(EYE_WIDTH).flex_shrink_0())
            })
            .child(cell(
                "column-profile-header-column",
                Some(NAME_WIDTH),
                header_label("column"),
                false,
            ))
            .when(self.storage.codec, |header| {
                header.child(cell(
                    "column-profile-header-codec",
                    Some(CODEC_WIDTH),
                    header_label("codec"),
                    false,
                ))
            })
            .when(self.storage.sizes, |header| {
                header
                    .child(cell(
                        "column-profile-header-on_disk",
                        Some(ON_DISK_WIDTH),
                        header_label("on_disk"),
                        true,
                    ))
                    .child(cell(
                        "column-profile-header-ratio",
                        Some(RATIO_WIDTH),
                        header_label("ratio"),
                        true,
                    ))
                    .child(cell(
                        "column-profile-header-share",
                        Some(SHARE_WIDTH),
                        header_label("share"),
                        false,
                    ))
            })
            .child(cell(
                "column-profile-header-distinct",
                Some(DISTINCT_WIDTH),
                header_label("distinct"),
                true,
            ))
            .child(cell(
                "column-profile-header-nulls",
                Some(NULLS_WIDTH),
                header_label("nulls"),
                true,
            ))
            .child(cell(
                "column-profile-header-distribution",
                None,
                header_label("distribution"),
                false,
            ))
    }

    fn render_rows(
        &mut self,
        range: Range<usize>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Vec<gpui::AnyElement> {
        let palette = RowPalette::of(cx.theme());
        let focused = self.table_focus.is_focused(window);
        let storage = self.storage;
        let projection_enabled = self.projection_enabled;

        range
            .filter_map(|position| {
                let index = *self.visible.get(position)?;
                let row = self.rows.get(index)?;
                let shown = self.applied.is_selected(index);
                let at_cursor = focused && self.cursor == Some(position);

                let eye = projection_enabled
                    .then(|| Self::eye_cell(row, index, position, shown, &palette, cx));

                Some(
                    div()
                        .id(("column-profile-row", index))
                        .debug_selector(move || format!("column-profile-row-{index}"))
                        .flex()
                        .items_center()
                        // The list lays each row out on its own, so without a
                        // full width a row sizes to its text and drifts from
                        // the header and the scrollable width.
                        .w_full()
                        .h(ROW_HEIGHT)
                        .border_b_1()
                        .border_color(palette.border)
                        .font_family(crate::fonts::editor_family(cx))
                        .text_size(GridMetrics::FONT)
                        .when(at_cursor, |row| row.bg(palette.cursor_wash))
                        .children(eye)
                        .child(name_cell(row, index, shown, &palette))
                        .when(storage.codec, |cells| {
                            cells.child(
                                fixed_cell(CODEC_WIDTH, row.codec.clone(), palette.muted, false)
                                    .text_size(FontSizes::LABEL),
                            )
                        })
                        .when(storage.sizes, |cells| {
                            cells
                                .child(fixed_cell(
                                    ON_DISK_WIDTH,
                                    row.on_disk.clone(),
                                    palette.strong,
                                    true,
                                ))
                                .child(fixed_cell(
                                    RATIO_WIDTH,
                                    row.ratio.clone(),
                                    palette.secondary_text,
                                    true,
                                ))
                                .child(share_cell(row, &palette))
                        })
                        .child(fixed_cell(
                            DISTINCT_WIDTH,
                            row.distinct.clone(),
                            palette.secondary_text,
                            true,
                        ))
                        .child(fixed_cell(
                            NULLS_WIDTH,
                            row.nulls.clone(),
                            palette.secondary_text,
                            true,
                        ))
                        .child(distribution_cell(row, index, &palette))
                        .into_any_element(),
                )
            })
            .collect()
    }

    /// The eye of the row at `position`, which shows whether the column at
    /// `index` is in the projection and toggles it on click.
    fn eye_cell(
        row: &ProfileRow,
        index: usize,
        position: usize,
        shown: bool,
        palette: &RowPalette,
        cx: &Context<Self>,
    ) -> impl IntoElement {
        div()
            .id(("column-profile-eye", index))
            .flex()
            .flex_shrink_0()
            .items_center()
            .justify_center()
            .w(EYE_WIDTH)
            .h_full()
            .cursor_pointer()
            .role(Role::CheckBox)
            .aria_toggled(if shown { Toggled::True } else { Toggled::False })
            .aria_label(row.name.clone())
            .on_click(cx.listener(move |this, _event, _window, cx| {
                this.cursor = Some(position);
                this.toggle_column(index, cx);
            }))
            .child(
                Icon::new(if shown { AppIcon::Eye } else { AppIcon::EyeOff })
                    .size(Fields::LEADING_ICON)
                    .color(if shown { palette.tint } else { palette.muted }),
            )
    }

    fn render_table(&self, cx: &Context<Self>) -> gpui::AnyElement {
        let theme = cx.theme();

        let rows = if self.visible.is_empty() {
            div()
                .id("column-profile-no-match")
                .debug_selector(|| "column-profile-no-match".to_string())
                .flex()
                .flex_1()
                .items_center()
                .justify_center()
                .text_size(FontSizes::XS)
                .text_color(theme.muted_foreground)
                .child(dbflux_i18n::t!("components.column_profile.no_match"))
                .into_any_element()
        } else {
            let mut list = uniform_list(
                "column-profile-rows",
                self.visible.len(),
                cx.processor(Self::render_rows),
            )
            .track_scroll(&self.table_scroll)
            .flex_1();

            // Keeps a sideways wheel from scrolling the rows vertically; the
            // horizontal scroller around them takes it instead.
            list.style().restrict_scroll_to_axis = Some(true);
            list.into_any_element()
        };

        // Header and rows share one horizontal scroller so they stay aligned;
        // the rows keep their own vertical, virtualized scroll inside it.
        let scroller = div()
            .id("column-profile-hscroll")
            .size_full()
            .overflow_x_scroll()
            .restrict_scroll_to_axis()
            .track_scroll(&self.table_hscroll)
            .child(
                div()
                    .debug_selector(|| "column-profile-content".to_string())
                    .flex()
                    .flex_col()
                    .h_full()
                    .w_full()
                    .min_w(table_min_width(self.storage))
                    .child(self.render_header(cx))
                    .child(rows),
            );

        div()
            .id("column-profile-table")
            .debug_selector(|| "column-profile-table".to_string())
            .key_context(ContextId::Dropdown.as_gpui_context())
            .track_focus(&self.table_focus)
            .on_action(cx.listener(Self::handle_run_command))
            .relative()
            .flex_1()
            .min_h_0()
            .child(scroller)
            .child(
                div()
                    .absolute()
                    .left_0()
                    .right_0()
                    .bottom_0()
                    .h(SCROLLBAR_HEIGHT)
                    .child(Scrollbar::horizontal(&self.table_hscroll)),
            )
            .into_any_element()
    }

    fn render_footer(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let muted = theme.muted_foreground;

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(Spacing::LG)
            .h(BAR_HEIGHT)
            .px(Spacing::LG)
            .border_t_1()
            .border_color(theme.input)
            .text_size(FontSizes::XS)
            .text_color(muted)
            .child(
                div()
                    .whitespace_nowrap()
                    .text_color(theme.foreground)
                    .child(self.footer_counts.clone()),
            )
            .when(self.projection_enabled, |footer| {
                footer.child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::XXS)
                        .min_w_0()
                        .child(
                            Icon::new(AppIcon::Info)
                                .size(Fields::LEADING_ICON)
                                .color(muted),
                        )
                        .child(
                            div()
                                .truncate()
                                .child(dbflux_i18n::t!("components.column_profile.footer.hint")),
                        ),
                )
            })
    }

    fn render_body(&self, cx: &Context<Self>) -> gpui::AnyElement {
        match &self.content {
            LoadingState::Idle | LoadingState::Loading => div()
                .id("column-profile-loading")
                .debug_selector(|| "column-profile-loading".to_string())
                .flex()
                .flex_1()
                .items_center()
                .justify_center()
                .gap(Spacing::SM)
                .text_size(FontSizes::XS)
                .text_color(cx.theme().muted_foreground)
                .child(Spinner::new(0))
                .child(dbflux_i18n::t!("components.column_profile.loading"))
                .into_any_element(),
            LoadingState::Failed { message } => div()
                .id("column-profile-error")
                .debug_selector(|| "column-profile-error".to_string())
                .flex()
                .flex_1()
                .items_center()
                .justify_center()
                .child(
                    EmptyState::new(AppIcon::TriangleAlert, message.clone())
                        .title(dbflux_i18n::t!("components.column_profile.error_title"))
                        .danger(),
                )
                .into_any_element(),
            LoadingState::Loaded(profile) if profile.columns.is_empty() => div()
                .id("column-profile-empty")
                .debug_selector(|| "column-profile-empty".to_string())
                .flex()
                .flex_1()
                .items_center()
                .justify_center()
                .child(EmptyState::new(
                    AppIcon::Columns,
                    dbflux_i18n::t!("components.column_profile.empty"),
                ))
                .into_any_element(),
            LoadingState::Loaded(_) => div()
                .flex()
                .flex_col()
                .flex_1()
                .min_h_0()
                .children(self.render_facts(cx))
                .child(self.render_toolbar(cx))
                .child(self.render_table(cx))
                .child(self.render_footer(cx))
                .into_any_element(),
        }
    }
}

/// The theme colors a column row uses, read once per batch of rows.
struct RowPalette {
    strong: Hsla,
    tint: Hsla,
    muted: Hsla,
    secondary_text: Hsla,
    border: Hsla,
    track: Hsla,
    info: Hsla,
    cursor_wash: Hsla,
}

impl RowPalette {
    fn of(theme: &gpui_component::Theme) -> Self {
        let tint = ChromeColors::tint(theme);

        Self {
            strong: ChromeColors::strong(theme),
            tint,
            muted: theme.muted_foreground,
            secondary_text: theme.foreground,
            border: theme.border,
            track: theme.secondary,
            info: theme.info,
            cursor_wash: tint.opacity(GridMetrics::CELL_SELECTED_ALPHA),
        }
    }
}

/// The column's name and badges over its type.
fn name_cell(row: &ProfileRow, index: usize, shown: bool, palette: &RowPalette) -> gpui::Div {
    div()
        .debug_selector(move || format!("column-profile-name-{index}"))
        .flex()
        .flex_col()
        .flex_shrink_0()
        .gap(px(2.0))
        .w(NAME_WIDTH)
        .min_w_0()
        .px(GridMetrics::CELL_PADDING_X)
        .child(
            div()
                .flex()
                .items_center()
                .gap(Spacing::XXS)
                .min_w_0()
                .child(
                    div()
                        .min_w_0()
                        .truncate()
                        .font_weight(FontWeight::SEMIBOLD)
                        .text_color(if shown { palette.strong } else { palette.muted })
                        .child(row.name.clone()),
                )
                .children(
                    row.badges
                        .iter()
                        .map(|badge| Badge::new(badge.clone(), BadgeTone::Accent)),
                ),
        )
        .child(
            div()
                .truncate()
                .text_size(GridMetrics::TYPE_FONT)
                .text_color(palette.muted)
                .child(row.type_name.clone()),
        )
}

/// A cell of fixed `width` holding one line of text.
fn fixed_cell(width: Pixels, text: SharedString, color: Hsla, right: bool) -> gpui::Div {
    div()
        .flex_shrink_0()
        .w(width)
        .px(GridMetrics::CELL_PADDING_X)
        .whitespace_nowrap()
        .truncate()
        .when(right, |cell| cell.flex().justify_end())
        .text_color(color)
        .child(text)
}

/// The column's share of the table as a bar and a percentage.
fn share_cell(row: &ProfileRow, palette: &RowPalette) -> gpui::Div {
    div()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(Spacing::SM)
        .w(SHARE_WIDTH)
        .px(GridMetrics::CELL_PADDING_X)
        .child(
            div()
                .flex_1()
                .h(SHARE_BAR_HEIGHT)
                .bg(palette.track)
                .when_some(row.share, |bar, share| {
                    bar.child(
                        div()
                            .h_full()
                            .w(gpui::relative(share.clamp(0.0, 1.0)))
                            .bg(palette.info),
                    )
                }),
        )
        .child(
            div()
                .flex()
                .flex_shrink_0()
                .justify_end()
                .w(SHARE_LABEL_WIDTH)
                .text_size(FontSizes::LABEL)
                .text_color(palette.muted)
                .child(row.share_label.clone()),
        )
}

fn distribution_cell(row: &ProfileRow, index: usize, palette: &RowPalette) -> gpui::Div {
    div()
        .debug_selector(move || format!("column-profile-distribution-{index}"))
        .flex_1()
        .min_w_0()
        .px(GridMetrics::CELL_PADDING_X)
        .truncate()
        .text_size(FontSizes::LABEL)
        .text_color(palette.muted)
        .child(row.distribution.clone())
}

fn header_label(key: &str) -> String {
    dbflux_i18n::t!(&format!("components.column_profile.header.{key}"))
}

impl Render for ColumnProfileView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("column-profile-view")
            .flex()
            .flex_col()
            .size_full()
            .font_family(crate::fonts::ui_family(cx))
            .text_color(cx.theme().foreground)
            .child(self.render_body(cx))
    }
}

impl EventEmitter<ProjectionChanged> for ColumnProfileView {}
impl EventEmitter<ColumnProfileActionRequested> for ColumnProfileView {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use dbflux_core::{
        ColumnBadge, ColumnProfile, ColumnProjection, ProfileSource, TableProfile, ValueRange,
    };
    use gpui::{
        AppContext as _, Context, Entity, IntoElement, ParentElement as _, Render, Styled as _,
        TestAppContext, VisualTestContext, Window, div,
    };

    use super::{ColumnProfileView, ColumnSort, ProfileRow};
    use crate::components::column_facts::UNKNOWN_FACT;
    use crate::components::column_projection::ProjectionChanged;
    use crate::controls::bind_dropdown_keys_for_tests;

    struct Owner {
        view: Entity<ColumnProfileView>,
        changes: Vec<ColumnProjection>,
        _subscription: gpui::Subscription,
    }

    impl Render for Owner {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().size_full().child(self.view.clone())
        }
    }

    fn unknown(name: &str, type_name: &str) -> ColumnProfile {
        ColumnProfile::new(name, type_name)
    }

    fn known(name: &str, type_name: &str, compressed_bytes: u64) -> ColumnProfile {
        ColumnProfile {
            codec: Some("ZSTD".into()),
            compressed_bytes: Some(compressed_bytes),
            uncompressed_bytes: Some(compressed_bytes * 4),
            null_count: Some(0),
            ..unknown(name, type_name)
        }
    }

    fn table(columns: Vec<ColumnProfile>) -> TableProfile {
        let total = columns
            .iter()
            .filter_map(|column| column.compressed_bytes)
            .sum::<u64>();

        TableProfile {
            row_count: Some(1000),
            total_compressed_bytes: Some(total),
            ..TableProfile::new(columns, ProfileSource::FileFooter)
        }
    }

    fn four_columns() -> Arc<TableProfile> {
        Arc::new(table(vec![
            known("zone_id", "INT64", 4096),
            known("name", "UTF8", 2048),
            unknown("note", "UTF8"),
            known("created_at", "TIMESTAMP(MICROS)", 8192),
        ]))
    }

    fn setup(
        profile: Option<Arc<TableProfile>>,
        cx: &mut TestAppContext,
    ) -> (Entity<Owner>, &mut VisualTestContext) {
        cx.update(gpui_component::init);
        bind_dropdown_keys_for_tests(cx);

        let (owner, window) = cx.add_window_view(|window, cx| {
            let view = cx.new(|cx| {
                let mut view = ColumnProfileView::new(window, cx);

                if let Some(profile) = profile {
                    let total = profile.columns.len();
                    view.set_profile(profile, ColumnProjection::from_indices(total, &[0, 1]), cx);
                }

                view
            });

            let subscription = cx.subscribe_in(
                &view,
                window,
                |this: &mut Owner, _, event: &ProjectionChanged, _window, _cx| {
                    this.changes.push(event.0.clone());
                },
            );

            Owner {
                view,
                changes: Vec::new(),
                _subscription: subscription,
            }
        });
        window.run_until_parked();

        (owner, window)
    }

    fn view(owner: &Entity<Owner>, window: &mut VisualTestContext) -> Entity<ColumnProfileView> {
        window.update(|_, cx| owner.read(cx).view.clone())
    }

    #[gpui::test]
    fn eye_toggle_emits_projection_change(cx: &mut TestAppContext) {
        let (owner, window) = setup(Some(four_columns()), cx);
        let view = view(&owner, window);

        let added = window.update(|_, cx| view.update(cx, |view, cx| view.toggle_column(3, cx)));
        assert!(added);

        // From the keyboard: the cursor starts on `zone_id`; Space drops it.
        window.update(|window, cx| view.update(cx, |view, cx| view.focus(window, cx)));
        window.simulate_keystrokes("space");
        window.run_until_parked();

        // Dropping `name` and then `created_at` would leave nothing shown.
        window.update(|_, cx| view.update(cx, |view, cx| view.toggle_column(1, cx)));
        let refused = window.update(|_, cx| view.update(cx, |view, cx| view.toggle_column(3, cx)));
        assert!(!refused, "the last shown column cannot be dropped");

        let (changes, applied) = window.update(|_, cx| {
            let owner = owner.read(cx);
            (
                owner
                    .changes
                    .iter()
                    .map(ColumnProjection::selected_indices)
                    .collect::<Vec<_>>(),
                owner.view.read(cx).applied().selected_indices(),
            )
        });
        assert_eq!(
            changes,
            vec![vec![0, 1, 3], vec![1, 3], vec![3]],
            "each accepted toggle emits the new projection, the refused one nothing"
        );
        assert_eq!(applied, vec![3]);
    }

    #[gpui::test]
    fn filter_keeps_footer_counts(cx: &mut TestAppContext) {
        let (owner, window) = setup(Some(four_columns()), cx);
        let view = view(&owner, window);

        let before = window.update(|_, cx| view.read(cx).footer_counts().clone());
        assert_eq!(before.as_ref(), "4 columns · 2 shown in Data");

        window.update(|window, cx| {
            view.update(cx, |view, cx| view.set_filter_query("utf", window, cx))
        });
        window.run_until_parked();

        let (visible, after) = window.update(|_, cx| {
            let view = view.read(cx);
            (
                view.visible_columns().to_vec(),
                view.footer_counts().clone(),
            )
        });
        assert_eq!(visible, vec![1, 2]);
        assert_eq!(
            after, before,
            "the footer counts the profile, not the filter"
        );

        window.update(|window, cx| {
            view.update(cx, |view, cx| view.set_filter_query("missing", window, cx))
        });
        window.run_until_parked();
        assert!(window.debug_bounds("column-profile-no-match").is_some());
    }

    #[gpui::test]
    fn empty_profile_shows_empty_state(cx: &mut TestAppContext) {
        let (owner, window) = setup(Some(Arc::new(table(Vec::new()))), cx);

        assert!(
            window.debug_bounds("column-profile-empty").is_some(),
            "a profile without columns shows the empty state"
        );
        assert!(window.debug_bounds("column-profile-no-match").is_none());

        let view = view(&owner, window);
        window.update(|_, cx| view.update(cx, |view, cx| view.set_loading(cx)));
        window.run_until_parked();
        assert!(window.debug_bounds("column-profile-loading").is_some());

        window
            .update(|_, cx| view.update(cx, |view, cx| view.set_failed("footer is truncated", cx)));
        window.run_until_parked();
        assert!(window.debug_bounds("column-profile-error").is_some());
    }

    #[test]
    fn unknown_values_show_a_dash() {
        let column = unknown("payload", "BYTE_ARRAY");
        let row = ProfileRow::new(&column, &table(vec![column.clone()]));

        for (field, text) in [
            ("codec", &row.codec),
            ("on disk", &row.on_disk),
            ("ratio", &row.ratio),
            ("share", &row.share_label),
            ("distinct", &row.distinct),
            ("nulls", &row.nulls),
            ("distribution", &row.distribution),
        ] {
            assert_eq!(text.as_ref(), UNKNOWN_FACT, "unknown {field} shows a dash");
        }
        assert_eq!(row.share, None);

        let zero = ColumnProfile {
            codec: Some("SNAPPY".into()),
            compressed_bytes: Some(0),
            uncompressed_bytes: Some(0),
            null_count: Some(0),
            distinct_count: Some(0),
            range: Some(ValueRange {
                min: "a".into(),
                max: "zz".into(),
                exact: false,
            }),
            badges: vec![ColumnBadge::SortKey { position: None }],
            ..unknown("empty", "UTF8")
        };
        let row = ProfileRow::new(&zero, &table(vec![zero.clone()]));

        assert_eq!(row.codec.as_ref(), "SNAPPY");
        assert_eq!(row.on_disk.as_ref(), "0 B", "a known zero is not a dash");
        assert_eq!(
            row.ratio.as_ref(),
            UNKNOWN_FACT,
            "a ratio over zero bytes is unknown"
        );
        assert_eq!(row.distinct.as_ref(), "0");
        assert_eq!(row.nulls.as_ref(), "0%");
        assert_eq!(row.distribution.as_ref(), "≈ a → zz");
        assert_eq!(row.badges.len(), 1);
    }

    #[gpui::test]
    fn sort_orders_by_size_and_name(cx: &mut TestAppContext) {
        let (owner, window) = setup(Some(four_columns()), cx);
        let view = view(&owner, window);

        window.update(|_, cx| view.update(cx, |view, cx| view.set_sort(ColumnSort::Size, cx)));
        let by_size = window.update(|_, cx| view.read(cx).visible_columns().to_vec());
        assert_eq!(by_size, vec![3, 0, 1, 2], "unknown size sorts last");

        window.update(|_, cx| view.update(cx, |view, cx| view.cycle_sort(cx)));
        let by_name = window.update(|_, cx| view.read(cx).visible_columns().to_vec());
        assert_eq!(by_name, vec![3, 1, 2, 0]);

        window.update(|_, cx| view.update(cx, |view, cx| view.cycle_sort(cx)));
        let table_order = window.update(|_, cx| view.read(cx).visible_columns().to_vec());
        assert_eq!(table_order, vec![0, 1, 2, 3]);
    }

    #[test]
    fn profile_view_keys_resolve_in_every_locale() {
        let keys = [
            "components.column_profile.filter_placeholder",
            "components.column_profile.sort.table_order",
            "components.column_profile.sort.size",
            "components.column_profile.sort.name",
            "components.column_profile.source.file_footer",
            "components.column_profile.header.column",
            "components.column_profile.header.on_disk",
            "components.column_profile.header.share",
            "components.column_profile.header.distinct",
            "components.column_profile.header.nulls",
            "components.column_profile.header.distribution",
            "components.column_profile.footer.counts",
            "components.column_profile.footer.hint",
            "components.column_profile.loading",
            "components.column_profile.empty",
            "components.column_profile.no_match",
            "components.column_profile.error_title",
            "components.column_profile.badge.sort_key",
            "components.column_profile.badge.partition_key",
        ];

        // "Codec" and "Ratio" read the same as English in some catalogs, so
        // only their English text is checked.
        let english_only_keys = [
            "components.column_profile.header.codec",
            "components.column_profile.header.ratio",
        ];

        for key in keys.iter().chain(&english_only_keys) {
            let english = dbflux_i18n::t!(key, locale = "en");
            assert!(
                !english.is_empty() && !english.ends_with(key),
                "en misses {key}"
            );
        }

        for key in keys {
            let english = dbflux_i18n::t!(key, locale = "en");

            // A key missing from a catalog falls back to English.
            for locale in ["es", "ko", "pt_BR", "zh_Hans"] {
                let text = dbflux_i18n::t!(key, locale = locale);
                assert_ne!(text, english, "{locale} misses {key}");
            }
        }
    }

    #[gpui::test]
    fn a_narrow_columns_view_scrolls_to_the_last_column(cx: &mut TestAppContext) {
        let (_owner, window) = setup(Some(four_columns()), cx);
        window.simulate_resize(gpui::size(gpui::px(585.0), gpui::px(600.0)));
        window.run_until_parked();

        let table = window
            .debug_bounds("column-profile-table")
            .expect("the table renders");
        let before = window
            .debug_bounds("column-profile-header-distribution")
            .expect("the distribution header renders");
        assert!(
            before.right() > table.right(),
            "the fixture must overflow: header ends at {:?}, table at {:?}",
            before.right(),
            table.right()
        );

        window.simulate_event(gpui::ScrollWheelEvent {
            position: table.center(),
            delta: gpui::ScrollDelta::Pixels(gpui::point(gpui::px(-2000.0), gpui::px(0.0))),
            ..Default::default()
        });
        window.run_until_parked();

        let header = window
            .debug_bounds("column-profile-header-distribution")
            .expect("the distribution header renders");
        assert!(
            header.left() < before.left(),
            "a sideways scroll moves the columns: {:?} then {:?}",
            before.left(),
            header.left()
        );
        assert!(
            header.right() <= table.right() + gpui::px(0.5) && header.left() >= table.left(),
            "the last column is in view: header {header:?}, table {table:?}"
        );

        let cell = window
            .debug_bounds("column-profile-distribution-0")
            .expect("the first row's distribution renders");
        assert_eq!(
            header.left(),
            cell.left(),
            "header and rows scroll together"
        );

        let name_header = window
            .debug_bounds("column-profile-header-column")
            .expect("the name header renders");
        let name_cell = window
            .debug_bounds("column-profile-name-0")
            .expect("the first row's name renders");
        assert_eq!(name_header.left(), name_cell.left());
    }

    #[gpui::test]
    fn a_full_sideways_scroll_shows_the_end_of_every_row(cx: &mut TestAppContext) {
        let long_range = ColumnProfile {
            range: Some(ValueRange {
                min: "2019-01-01T00:00:00.000000 Europe/Amsterdam".into(),
                max: "2024-12-31T23:59:59.999999 Europe/Amsterdam".into(),
                exact: true,
            }),
            ..known("created_at", "TIMESTAMP(MICROS)", 8192)
        };
        let profile = table(vec![long_range, known("zone_id", "INT64", 4096)]);
        let (_owner, window) = setup(Some(Arc::new(profile)), cx);
        window.simulate_resize(gpui::size(gpui::px(585.0), gpui::px(600.0)));
        window.run_until_parked();

        let table = window
            .debug_bounds("column-profile-table")
            .expect("the table renders");

        window.simulate_event(gpui::ScrollWheelEvent {
            position: table.center(),
            delta: gpui::ScrollDelta::Pixels(gpui::point(gpui::px(-5000.0), gpui::px(0.0))),
            ..Default::default()
        });
        window.run_until_parked();

        let content = window
            .debug_bounds("column-profile-content")
            .expect("the scrolled content renders");
        let header = window
            .debug_bounds("column-profile-header-distribution")
            .expect("the distribution header renders");
        assert!(
            header.right() <= table.right() + gpui::px(1.0),
            "the header's last column ends in view: header {header:?}, table {table:?}"
        );

        let rows = [
            (0, "column-profile-row-0", "column-profile-distribution-0"),
            (1, "column-profile-row-1", "column-profile-distribution-1"),
        ];

        for (index, row_selector, cell_selector) in rows {
            let row = window.debug_bounds(row_selector).expect("the row renders");
            let cell = window
                .debug_bounds(cell_selector)
                .expect("the row's distribution renders");

            assert!(
                (row.size.width - content.size.width).abs() <= gpui::px(1.0),
                "row {index} is as wide as the scrolled content: row {:?}, content {:?}",
                row.size.width,
                content.size.width
            );
            assert!(
                cell.right() <= table.right() + gpui::px(1.0),
                "row {index} ends in view: cell {cell:?}, table {table:?}"
            );
            assert!(
                (cell.right() - header.right()).abs() <= gpui::px(1.0),
                "row {index} ends with the header: cell {cell:?}, header {header:?}"
            );
        }
    }

    #[gpui::test]
    fn a_vertical_wheel_scrolls_rows_not_columns(cx: &mut TestAppContext) {
        let columns = (0..80)
            .map(|index| known(&format!("col_{index:03}"), "INT64", 1024))
            .collect();
        let (_owner, window) = setup(Some(Arc::new(table(columns))), cx);
        window.simulate_resize(gpui::size(gpui::px(585.0), gpui::px(600.0)));
        window.run_until_parked();

        assert!(
            window.debug_bounds("column-profile-name-79").is_none(),
            "rows out of view are not rendered"
        );

        let table = window
            .debug_bounds("column-profile-table")
            .expect("the table renders");
        let header_before = window
            .debug_bounds("column-profile-header-column")
            .expect("the name header renders");
        let first_before = window
            .debug_bounds("column-profile-name-0")
            .expect("the first row renders");

        window.simulate_event(gpui::ScrollWheelEvent {
            position: table.center(),
            delta: gpui::ScrollDelta::Pixels(gpui::point(gpui::px(0.0), gpui::px(-200.0))),
            ..Default::default()
        });
        window.run_until_parked();

        let header_after = window
            .debug_bounds("column-profile-header-column")
            .expect("the name header renders");
        assert_eq!(header_before, header_after, "the header stays put");

        let first_after = window.debug_bounds("column-profile-name-0");
        assert!(
            first_after.is_none_or(|bounds| bounds.top() < first_before.top()),
            "the rows scroll up"
        );
    }

    #[gpui::test]
    fn a_catalog_profile_shows_its_facts_and_names_its_source(cx: &mut TestAppContext) {
        use chrono::{TimeZone, Utc};
        use dbflux_core::{TableFact, TableFactKind};

        let captured_at = Utc.with_ymd_and_hms(2026, 9, 30, 14, 22, 5).unwrap();
        let profile = Arc::new(TableProfile {
            row_count: Some(1_840_000_000),
            row_count_exact: false,
            total_compressed_bytes: Some(400),
            total_uncompressed_bytes: Some(4_320),
            engine: Some("MergeTree".into()),
            facts: vec![
                TableFact::new(TableFactKind::Engine, "MergeTree"),
                TableFact::new(TableFactKind::SortKey, "(site_id, ts)"),
            ],
            captured_at: Some(captured_at),
            ..TableProfile::new(
                vec![
                    known("site_id", "UInt32", 100),
                    known("ts", "DateTime", 300),
                ],
                ProfileSource::Catalog("system.columns"),
            )
        });
        let (owner, window) = setup(Some(profile), cx);
        let view = view(&owner, window);

        let (facts, source) = window.update(|_, cx| {
            let view = view.read(cx);
            (
                view.facts()
                    .iter()
                    .map(|(label, value)| format!("{label}={value}"))
                    .collect::<Vec<_>>(),
                view.source_text().to_string(),
            )
        });

        assert_eq!(
            facts,
            vec![
                "Engine=MergeTree",
                "Sort key=(site_id, ts)",
                "Rows=≈ 1\u{202F}840\u{202F}000\u{202F}000",
                "On disk=400 B",
                "Raw=4.2 KiB",
                "Ratio=11×",
            ]
        );
        assert!(
            source.starts_with("Sizes from system.columns · read at "),
            "{source}"
        );
    }

    #[gpui::test]
    fn a_profile_without_storage_facts_hides_the_storage_columns(cx: &mut TestAppContext) {
        let profile = Arc::new(TableProfile::new(
            vec![
                ColumnProfile {
                    null_count: Some(2),
                    distinct_count: Some(3),
                    top_values: vec![
                        dbflux_core::ValueCount {
                            value: "US".into(),
                            count: 250,
                        },
                        dbflux_core::ValueCount {
                            value: "DE".into(),
                            count: 125,
                        },
                    ],
                    ..unknown("country", "Text")
                },
                unknown("note", "Text"),
            ],
            ProfileSource::LoadedRows { rows: 500 },
        ));
        let (owner, window) = setup(Some(profile.clone()), cx);
        let view = view(&owner, window);

        let (shows_codec, shows_sizes, source) = window.update(|_, cx| {
            let view = view.read(cx);
            (
                view.shows_codec(),
                view.shows_sizes(),
                view.source_text().to_string(),
            )
        });
        assert!(!shows_codec && !shows_sizes);
        assert_eq!(source, "From the 500 loaded rows");
        assert!(window.debug_bounds("column-profile-header-codec").is_none());
        assert!(
            window
                .debug_bounds("column-profile-header-distinct")
                .is_some()
        );

        let row = ProfileRow::new(&profile.columns[0], &profile);
        assert_eq!(row.distribution.as_ref(), "US 50% · DE 25%");
        assert_eq!(row.distinct.as_ref(), "3");

        let (owner, window) = setup(Some(four_columns()), cx);
        let stored = super::tests::view(&owner, window);
        assert!(window.update(|_, cx| stored.read(cx).shows_sizes()));
        assert!(window.debug_bounds("column-profile-header-codec").is_some());
    }

    #[gpui::test]
    fn actions_emit_only_when_offered_and_idle(cx: &mut TestAppContext) {
        use super::{ColumnProfileActionRequested, ProfileAction, ProfileActions};

        let (owner, window) = setup(Some(four_columns()), cx);
        let view = view(&owner, window);
        let requested = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));

        let _subscription = window.update(|_, cx| {
            let requested = requested.clone();
            cx.subscribe(&view, move |_, event: &ColumnProfileActionRequested, _| {
                requested.borrow_mut().push(event.0);
            })
        });

        window.update(|_, cx| {
            view.update(cx, |view, cx| {
                view.request_action(ProfileAction::Sample, cx)
            })
        });
        assert!(
            requested.borrow().is_empty(),
            "an action not offered emits nothing"
        );

        window.update(|_, cx| {
            view.update(cx, |view, cx| {
                view.set_actions(
                    ProfileActions {
                        refresh: true,
                        sample_rows: Some(1_000_000),
                        full_scan: false,
                    },
                    cx,
                );
                view.request_action(ProfileAction::Sample, cx);
                view.request_action(ProfileAction::FullScan, cx);
                view.set_running(Some(ProfileAction::Sample), cx);
                view.request_action(ProfileAction::Refresh, cx);
                view.set_running(None, cx);
                view.request_action(ProfileAction::Refresh, cx);
            })
        });
        window.run_until_parked();

        assert_eq!(
            *requested.borrow(),
            vec![ProfileAction::Sample, ProfileAction::Refresh],
            "the full scan is not offered and nothing starts while an action runs"
        );
        assert!(window.debug_bounds("column-profile-sample").is_some());
        assert!(window.debug_bounds("column-profile-full-scan").is_none());
    }

    #[test]
    fn badges_name_their_sort_position() {
        use super::badge_label;

        assert_eq!(
            badge_label(ColumnBadge::SortKey { position: None }),
            "Sort key"
        );
        assert_eq!(
            badge_label(ColumnBadge::SortKey { position: Some(2) }),
            "Sort key 2"
        );
        assert_eq!(
            badge_label(ColumnBadge::DistributionKey),
            "Distribution key"
        );
    }

    #[test]
    fn an_estimated_distinct_count_is_marked() {
        let column = ColumnProfile {
            distinct_count: Some(9_800_000),
            distinct_exact: false,
            ..unknown("ts", "DateTime")
        };
        let profile = table(vec![column.clone()]);

        assert_eq!(
            ProfileRow::new(&column, &profile).distinct.as_ref(),
            "≈ 9\u{202F}800\u{202F}000"
        );
    }

    #[gpui::test]
    fn a_view_without_projection_hides_the_eyes_and_refuses_toggles(cx: &mut TestAppContext) {
        let (owner, window) = setup(Some(four_columns()), cx);
        let view = view(&owner, window);

        let toggled = window.update(|_, cx| {
            view.update(cx, |view, cx| {
                view.set_projection_enabled(false, cx);
                view.toggle_column(2, cx)
            })
        });
        window.run_until_parked();

        assert!(!toggled);
        assert_eq!(
            window.update(|_, cx| view.read(cx).footer_counts().to_string()),
            "4 columns"
        );
        assert!(window.debug_bounds("column-profile-name-0").is_some());
        assert!(
            window.update(|_, cx| owner.read(cx).changes.is_empty()),
            "no projection change is emitted"
        );
    }

    #[test]
    fn nearly_unique_columns_show_their_range_instead_of_top_values() {
        use dbflux_core::ValueCount;

        let top = |value: &str, count| ValueCount {
            value: value.into(),
            count,
        };
        let range = Some(ValueRange {
            min: "0".into(),
            max: "499".into(),
            exact: true,
        });
        let profile = table(Vec::new());

        let unique = ColumnProfile {
            distinct_count: Some(500),
            range: range.clone(),
            top_values: vec![top("0", 1), top("1", 1)],
            ..unknown("id", "Integer")
        };
        assert_eq!(
            ProfileRow::new(&unique, &profile).distribution.as_ref(),
            "0 → 499"
        );

        let categorical = ColumnProfile {
            distinct_count: Some(5),
            range,
            top_values: vec![top("AR", 100), top("BR", 100)],
            ..unknown("country", "Text")
        };
        assert_eq!(
            ProfileRow::new(&categorical, &profile)
                .distribution
                .as_ref(),
            "AR 10% · BR 10%"
        );

        let no_range = ColumnProfile {
            top_values: vec![top("x", 1)],
            ..unknown("note", "Text")
        };
        assert_eq!(
            ProfileRow::new(&no_range, &profile).distribution.as_ref(),
            "x 0.1%"
        );
    }

    #[gpui::test]
    fn codecs_without_sizes_show_the_codec_column_only(cx: &mut TestAppContext) {
        let profile = Arc::new(TableProfile::new(
            vec![ColumnProfile {
                codec: Some("BitPacking".into()),
                ..unknown("id", "BIGINT")
            }],
            ProfileSource::Catalog("pragma_storage_info"),
        ));
        let (owner, window) = setup(Some(profile), cx);
        let view = view(&owner, window);

        assert!(window.update(|_, cx| view.read(cx).shows_codec()));
        assert!(!window.update(|_, cx| view.read(cx).shows_sizes()));
        assert!(window.debug_bounds("column-profile-header-codec").is_some());
        assert!(
            window
                .debug_bounds("column-profile-header-on_disk")
                .is_none()
        );
    }
}
