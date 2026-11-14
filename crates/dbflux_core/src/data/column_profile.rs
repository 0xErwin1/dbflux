//! Per-column profile of a table and the column projection chosen over it.
//!
//! A source fills a [`TableProfile`] from whatever it knows about its columns
//! (a file footer, a server catalog, planner statistics or a sample of rows),
//! and the UI renders the profile, the projection picker, the partitions and
//! the read estimate from these types alone, without knowing which format or
//! driver produced them.

use std::collections::HashMap;
use std::sync::Arc;

use bitflags::bitflags;
use chrono::{DateTime, Utc};

/// The rows one page of a paged table view reads.
pub const DEFAULT_PAGE_ROWS: u64 = 500;

/// The most bytes the default projection lets one page of
/// [`DEFAULT_PAGE_ROWS`] rows read.
pub const DEFAULT_PROJECTION_BUDGET_BYTES: u64 = 16 * 1024 * 1024;

/// The most columns the default projection selects.
pub const DEFAULT_PROJECTION_MAX_COLUMNS: usize = 32;

/// Where the facts of a [`TableProfile`] or [`TablePartitions`] come from.
///
/// The UI names the source next to every statistic, so a reader can tell a
/// figure the server keeps in its catalog from one computed over a sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileSource {
    /// The metadata a file stores about itself, such as a Parquet footer.
    FileFooter,
    /// A catalog the server keeps about its tables, named as a user would
    /// query it (for example `system.columns`). Reading it reads no table data.
    Catalog(&'static str),
    /// Statistics the server's planner keeps, named as a user would query
    /// them (for example `pg_stats`). They are as fresh as the last analyze.
    PlannerStatistics(&'static str),
    /// Computed from the rows the view has loaded so far.
    LoadedRows { rows: u64 },
    /// Computed by reading every one of `rows` rows.
    FullScan { rows: u64 },
    /// Computed from a sample of `rows` rows read from the table.
    Sample { rows: u64 },
}

impl ProfileSource {
    /// Whether producing this profile read table data rather than metadata.
    pub fn reads_table_data(&self) -> bool {
        matches!(
            self,
            Self::LoadedRows { .. } | Self::FullScan { .. } | Self::Sample { .. }
        )
    }
}

/// A role a column plays in how its table is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnBadge {
    /// The column is part of the key the rows are sorted by. `position` is its
    /// one-based place in that key, when the source knows it.
    SortKey { position: Option<u16> },
    /// The column is part of the key the rows are partitioned by.
    PartitionKey,
    /// The column decides which node stores each row (a distribution key).
    DistributionKey,
}

/// One value of a column and how many of the profiled rows hold it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueCount {
    pub value: Arc<str>,
    pub count: u64,
}

/// The smallest and largest value of a column, as display text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueRange {
    pub min: Arc<str>,
    pub max: Arc<str>,
    /// False when a bound is not itself a value of the column, such as a
    /// string cut to a statistics length limit.
    pub exact: bool,
}

/// What is known about one column. Every `None` means the source does not
/// know the value; it never stands for zero.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ColumnProfile {
    pub name: Arc<str>,
    pub type_name: Arc<str>,
    pub badges: Vec<ColumnBadge>,
    pub codec: Option<Arc<str>>,
    pub compressed_bytes: Option<u64>,
    pub uncompressed_bytes: Option<u64>,
    pub null_count: Option<u64>,
    pub distinct_count: Option<u64>,
    /// False when `distinct_count` is an estimate, such as a HyperLogLog
    /// count or a planner statistic.
    pub distinct_exact: bool,
    pub range: Option<ValueRange>,
    /// The most common values, most frequent first. Empty when unknown.
    pub top_values: Vec<ValueCount>,
}

impl ColumnProfile {
    /// A column known only by its name and type.
    pub fn new(name: impl Into<Arc<str>>, type_name: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            distinct_exact: true,
            ..Self::default()
        }
    }

    /// The one-based position of this column in the sort key, when it is part
    /// of it and the source knows where.
    pub fn sort_key_position(&self) -> Option<u16> {
        self.badges.iter().find_map(|badge| match badge {
            ColumnBadge::SortKey { position } => *position,
            _ => None,
        })
    }

    /// Uncompressed bytes per compressed byte. `None` when either size is
    /// unknown or the compressed size is zero.
    pub fn compression_ratio(&self) -> Option<f64> {
        let compressed = self.compressed_bytes.filter(|bytes| *bytes > 0)?;
        let uncompressed = self.uncompressed_bytes?;

        Some(uncompressed as f64 / compressed as f64)
    }

    /// The fraction of `total` bytes this column's compressed bytes take.
    /// `None` when either size is unknown or `total` is zero.
    pub fn share_of(&self, total: Option<u64>) -> Option<f64> {
        let total = total.filter(|bytes| *bytes > 0)?;
        let compressed = self.compressed_bytes?;

        Some(compressed as f64 / total as f64)
    }

    /// The fraction of `row_count` rows that are null in this column. `None`
    /// when either count is unknown or `row_count` is zero.
    pub fn null_fraction(&self, row_count: Option<u64>) -> Option<f64> {
        let row_count = row_count.filter(|rows| *rows > 0)?;
        let null_count = self.null_count?;

        Some(null_count as f64 / row_count as f64)
    }
}

/// What a [`TableFact`] describes. The UI owns the label of every kind but
/// [`TableFactKind::Other`], whose label the source supplies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableFactKind {
    /// The storage engine or table type, such as `MergeTree`.
    Engine,
    /// The expression the rows are sorted by.
    SortKey,
    /// The expression the rows are partitioned by.
    PartitionKey,
    /// The primary key expression, when it differs from the sort key.
    PrimaryKey,
    /// The expression rows are sampled by.
    SamplingKey,
    /// The rule that expires rows.
    Ttl,
    /// How rows are spread over nodes, such as `KEY(user_id)`.
    DistributionStyle,
    /// How many storage parts are active.
    ActiveParts,
    /// How many row groups the table is stored in.
    RowGroups,
    /// The share of rows not yet in sort-key order.
    Unsorted,
    /// The program that wrote a file.
    CreatedBy,
    /// When the statistics behind the profile were last refreshed on the
    /// server, as the server reports it.
    StatisticsUpdated,
    /// A fact without a dedicated kind, labelled by the source.
    Other(Arc<str>),
}

/// One storage fact about a table, such as its engine or its sort key, as
/// display text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableFact {
    pub kind: TableFactKind,
    pub value: Arc<str>,
}

impl TableFact {
    pub fn new(kind: TableFactKind, value: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            value: value.into(),
        }
    }
}

/// What is known about a table and each of its columns, in schema order.
#[derive(Debug, Clone, PartialEq)]
pub struct TableProfile {
    pub columns: Vec<ColumnProfile>,
    pub row_count: Option<u64>,
    /// False when `row_count` is an estimate, such as a planner statistic.
    pub row_count_exact: bool,
    pub total_compressed_bytes: Option<u64>,
    pub total_uncompressed_bytes: Option<u64>,
    pub source_label: ProfileSource,
    /// The engine or table type shown next to the table name.
    pub engine: Option<Arc<str>>,
    /// Storage facts shown above the columns, in the order the source gives.
    pub facts: Vec<TableFact>,
    /// When the profile was read. `None` when the source cannot tell.
    pub captured_at: Option<DateTime<Utc>>,
    /// Where the null counts, distinct counts, ranges and top values came
    /// from, when a sample filled them over this profile (see
    /// [`TableProfile::merge_sample`]). `None` means `source_label` covers them.
    pub distribution_source: Option<ProfileSource>,
    /// When the distribution facts were computed.
    pub distribution_captured_at: Option<DateTime<Utc>>,
}

impl TableProfile {
    /// A profile of `columns` from `source` with nothing else known.
    pub fn new(columns: Vec<ColumnProfile>, source: ProfileSource) -> Self {
        Self {
            columns,
            row_count: None,
            row_count_exact: true,
            total_compressed_bytes: None,
            total_uncompressed_bytes: None,
            source_label: source,
            engine: None,
            facts: Vec::new(),
            captured_at: None,
            distribution_source: None,
            distribution_captured_at: None,
        }
    }

    /// Fills the distribution facts of this profile (null count, distinct
    /// count, range and top values) from `sample`, matching columns by name.
    ///
    /// Sizes, codecs and badges are kept: a sample knows nothing about how a
    /// column is stored. A fact the sample does not know keeps its current
    /// value. The sample's source and time become this profile's distribution
    /// source. Returns the names of the columns the sample filled.
    pub fn merge_sample(&mut self, sample: &TableProfile) -> Vec<Arc<str>> {
        let sampled: HashMap<&str, &ColumnProfile> = sample
            .columns
            .iter()
            .map(|column| (column.name.as_ref(), column))
            .collect();
        let mut filled = Vec::new();

        for column in &mut self.columns {
            let Some(found) = sampled.get(column.name.as_ref()) else {
                continue;
            };

            if found.null_count.is_some() {
                column.null_count = found.null_count;
            }

            if found.distinct_count.is_some() {
                column.distinct_count = found.distinct_count;
                column.distinct_exact = found.distinct_exact;
            }

            if found.range.is_some() {
                column.range = found.range.clone();
            }

            if !found.top_values.is_empty() {
                column.top_values = found.top_values.clone();
            }

            filled.push(column.name.clone());
        }

        self.distribution_source = Some(sample.source_label);
        self.distribution_captured_at = sample.captured_at;

        filled
    }
}

/// One partition of a table and what its storage holds.
#[derive(Debug, Clone, PartialEq)]
pub struct TablePartition {
    /// The partition's value or identifier, as display text.
    pub name: Arc<str>,
    /// Storage parts currently active in the partition.
    pub parts: Option<u64>,
    pub rows: Option<u64>,
    pub compressed_bytes: Option<u64>,
    pub uncompressed_bytes: Option<u64>,
    /// The smallest and largest value of the partition key column.
    pub range: Option<ValueRange>,
    pub modified_at: Option<DateTime<Utc>>,
}

impl TablePartition {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            parts: None,
            rows: None,
            compressed_bytes: None,
            uncompressed_bytes: None,
            range: None,
            modified_at: None,
        }
    }

    /// Uncompressed bytes per compressed byte, when both are known.
    pub fn compression_ratio(&self) -> Option<f64> {
        let compressed = self.compressed_bytes.filter(|bytes| *bytes > 0)?;
        let uncompressed = self.uncompressed_bytes?;

        Some(uncompressed as f64 / compressed as f64)
    }
}

/// The partitions of a table, newest first when the source can order them.
#[derive(Debug, Clone, PartialEq)]
pub struct TablePartitions {
    /// The partition key expression, when the table declares one.
    pub key: Option<Arc<str>>,
    pub partitions: Vec<TablePartition>,
    pub source: ProfileSource,
    pub captured_at: Option<DateTime<Utc>>,
}

bitflags! {
    /// The table views a connection can fill, beyond the data grid itself.
    ///
    /// Kept apart from `DriverCapabilities` (whose bits are all taken) and
    /// reported per connection, like `DocumentFeatures`, so a driver can
    /// depend on the server version. The UI shows a tab or control only when
    /// its feature is reported; it never checks a driver id.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct TableViewFeatures: u32 {
        /// `Connection::table_profile` returns per-column storage facts from
        /// metadata, without reading table data. Shows the Columns tab.
        const COLUMN_PROFILE = 1 << 0;
        /// `Connection::table_partitions` lists the table's partitions.
        /// Shows the Partitions tab.
        const PARTITIONS = 1 << 1;
        /// `Connection::estimate_table_read` estimates what a browse reads
        /// before it runs. Shows the read estimate in the Data tab.
        const READ_ESTIMATE = 1 << 2;
        /// `Connection::sample_table_profile` computes nulls, distinct counts,
        /// ranges and top values from a bounded sample of rows. Shows the
        /// "Profile a sample" action.
        const SAMPLE_PROFILE = 1 << 3;
        /// `Connection::browse_table` honours `TableBrowseRequest::columns`.
        /// Shows the column projection picker in the Data tab.
        const COLUMN_PROJECTION = 1 << 4;
    }
}

/// The columns of a profile selected for reading, kept in schema order.
///
/// A projection may be empty while the user edits it: the picker applies its
/// selection explicitly, so an empty draft is allowed and reported by
/// [`ColumnProjection::is_applicable`] instead of being prevented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnProjection {
    selected: Vec<bool>,
}

impl ColumnProjection {
    /// Every one of `total_count` columns selected.
    pub fn all(total_count: usize) -> Self {
        Self {
            selected: vec![true; total_count],
        }
    }

    /// None of `total_count` columns selected.
    pub fn none(total_count: usize) -> Self {
        Self {
            selected: vec![false; total_count],
        }
    }

    /// The columns at `indices` selected out of `total_count`. Indices at or
    /// past `total_count` are ignored.
    pub fn from_indices(total_count: usize, indices: &[usize]) -> Self {
        let mut projection = Self::none(total_count);

        for &index in indices {
            if let Some(selected) = projection.selected.get_mut(index) {
                *selected = true;
            }
        }

        projection
    }

    /// Flips the selection of the column at `index`. Does nothing for an
    /// index past the last column.
    pub fn toggle(&mut self, index: usize) {
        if let Some(selected) = self.selected.get_mut(index) {
            *selected = !*selected;
        }
    }

    pub fn select_all(&mut self) {
        self.selected.fill(true);
    }

    pub fn select_none(&mut self) {
        self.selected.fill(false);
    }

    pub fn is_selected(&self, index: usize) -> bool {
        self.selected.get(index).copied().unwrap_or(false)
    }

    pub fn selected_count(&self) -> usize {
        self.selected.iter().filter(|selected| **selected).count()
    }

    pub fn total_count(&self) -> usize {
        self.selected.len()
    }

    /// Whether the selection can be applied: at least one column is selected.
    pub fn is_applicable(&self) -> bool {
        self.selected.contains(&true)
    }

    /// The selected column indices in schema order.
    pub fn selected_indices(&self) -> Vec<usize> {
        self.selected
            .iter()
            .enumerate()
            .filter(|(_, selected)| **selected)
            .map(|(index, _)| index)
            .collect()
    }

    /// The indices of the columns of `profile` whose name or type name
    /// contains `query`, ignoring case, in schema order. An empty query
    /// matches every column. The selection is not changed.
    pub fn filter(&self, profile: &TableProfile, query: &str) -> Vec<usize> {
        let query = query.to_lowercase();

        profile
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| {
                column.name.to_lowercase().contains(&query)
                    || column.type_name.to_lowercase().contains(&query)
            })
            .map(|(index, _)| index)
            .collect()
    }
}

/// The unit a read is counted in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartUnit {
    /// Parquet row groups.
    RowGroups,
    /// Storage parts of a table, such as ClickHouse data parts.
    Parts,
    /// Index granules (marks): the smallest block an index can skip.
    Granules,
}

/// How much of the table a read touches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EstimateScope {
    /// The read touches `touched` of the table's `total` parts.
    Parts {
        touched: usize,
        total: usize,
        unit: PartUnit,
    },
}

/// The bytes a read costs, worked out before reading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadEstimate {
    pub total_bytes: u64,
    /// The bytes of each read column, by column index in schema order.
    pub per_column: Vec<(usize, u64)>,
    /// How much of the table the read touches, coarsest unit first.
    pub scopes: Vec<EstimateScope>,
    /// The rows the read scans, when the source knows.
    pub rows: Option<u64>,
    /// False when the bytes are scaled from table totals instead of worked out
    /// from the exact parts the read touches.
    pub exact: bool,
}

/// The projection a table opens with: its leading columns in schema order,
/// while the bytes one page reads stay within `budget_bytes`, and at most
/// `max_columns` of them.
///
/// `per_column_window_bytes[i]` is what one page of column `i` reads; a
/// missing entry or `None` means unknown. The first column is always selected,
/// even over the budget, so a table never opens with nothing to show. The walk
/// stops at the first column whose size is unknown: counting it as zero could
/// select any number of columns of unknown cost and break the budget.
pub fn default_projection(
    profile: &TableProfile,
    per_column_window_bytes: &[Option<u64>],
    budget_bytes: u64,
    max_columns: usize,
) -> ColumnProjection {
    let column_count = profile.columns.len();
    let mut projection = ColumnProjection::none(column_count);

    if column_count == 0 {
        return projection;
    }

    projection.toggle(0);

    let Some(mut running_bytes) = window_bytes(per_column_window_bytes, 0) else {
        return projection;
    };

    for index in 1..column_count.min(max_columns) {
        let Some(bytes) = window_bytes(per_column_window_bytes, index) else {
            break;
        };

        let next_bytes = running_bytes.saturating_add(bytes);

        if next_bytes > budget_bytes {
            break;
        }

        projection.toggle(index);
        running_bytes = next_bytes;
    }

    projection
}

fn window_bytes(per_column_window_bytes: &[Option<u64>], index: usize) -> Option<u64> {
    per_column_window_bytes.get(index).copied().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, type_name: &str) -> ColumnProfile {
        ColumnProfile::new(name, type_name)
    }

    fn profile(columns: Vec<ColumnProfile>) -> TableProfile {
        TableProfile::new(columns, ProfileSource::FileFooter)
    }

    fn numbered_profile(count: usize) -> TableProfile {
        profile(
            (0..count)
                .map(|index| column(&format!("column_{index}"), "INT64"))
                .collect(),
        )
    }

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn default_projection_fits_budget_in_schema_order() {
        let table = numbered_profile(6);
        let window_bytes = [
            Some(4 * MIB),
            Some(5 * MIB),
            Some(6 * MIB),
            Some(2 * MIB),
            Some(MIB),
            Some(MIB),
        ];

        let projection = default_projection(
            &table,
            &window_bytes,
            DEFAULT_PROJECTION_BUDGET_BYTES,
            DEFAULT_PROJECTION_MAX_COLUMNS,
        );

        // 4 + 5 + 6 = 15 MiB fits; adding 2 MiB makes 17 MiB, so the walk
        // stops there even though later columns would still fit.
        assert_eq!(projection.selected_indices(), vec![0, 1, 2]);
        assert_eq!(projection.total_count(), 6);

        let unknown_size = [Some(MIB), None, Some(MIB)];
        let stopped = default_projection(
            &numbered_profile(3),
            &unknown_size,
            DEFAULT_PROJECTION_BUDGET_BYTES,
            DEFAULT_PROJECTION_MAX_COLUMNS,
        );
        assert_eq!(stopped.selected_indices(), vec![0]);
    }

    #[test]
    fn default_projection_caps_at_max_columns() {
        let table = numbered_profile(40);
        let window_bytes = vec![Some(1024); 40];

        let projection = default_projection(
            &table,
            &window_bytes,
            DEFAULT_PROJECTION_BUDGET_BYTES,
            DEFAULT_PROJECTION_MAX_COLUMNS,
        );

        assert_eq!(projection.selected_count(), DEFAULT_PROJECTION_MAX_COLUMNS);
        assert_eq!(
            projection.selected_indices(),
            (0..DEFAULT_PROJECTION_MAX_COLUMNS).collect::<Vec<_>>()
        );
    }

    #[test]
    fn default_projection_keeps_the_first_column_even_over_budget() {
        let table = numbered_profile(3);
        let window_bytes = [Some(64 * MIB), Some(MIB), Some(MIB)];

        let projection = default_projection(
            &table,
            &window_bytes,
            DEFAULT_PROJECTION_BUDGET_BYTES,
            DEFAULT_PROJECTION_MAX_COLUMNS,
        );
        assert_eq!(projection.selected_indices(), vec![0]);

        let unknown_first = default_projection(&table, &[], DEFAULT_PROJECTION_BUDGET_BYTES, 0);
        assert_eq!(unknown_first.selected_indices(), vec![0]);

        let empty = default_projection(
            &profile(Vec::new()),
            &[],
            DEFAULT_PROJECTION_BUDGET_BYTES,
            DEFAULT_PROJECTION_MAX_COLUMNS,
        );
        assert_eq!(empty.selected_count(), 0);
        assert!(!empty.is_applicable());
    }

    #[test]
    fn an_empty_draft_is_not_applicable() {
        let mut projection = ColumnProjection::from_indices(3, &[1]);
        assert!(projection.is_applicable());

        projection.toggle(1);

        assert_eq!(projection.selected_count(), 0);
        assert!(!projection.is_applicable());

        projection.toggle(2);
        assert!(projection.is_applicable());
        assert_eq!(projection.selected_indices(), vec![2]);

        projection.toggle(7);
        assert_eq!(projection.selected_indices(), vec![2]);
    }

    #[test]
    fn filter_is_case_insensitive_substring_over_name_and_type() {
        let table = profile(vec![
            column("UserId", "INT64"),
            column("created_at", "TIMESTAMP(MICROS)"),
            column("payload", "Utf8"),
            column("email", "UTF8"),
        ]);
        let projection = ColumnProjection::all(table.columns.len());

        assert_eq!(projection.filter(&table, "userid"), vec![0]);
        assert_eq!(projection.filter(&table, "utf"), vec![2, 3]);
        assert_eq!(projection.filter(&table, "TIME"), vec![1]);
        assert_eq!(projection.filter(&table, "a"), vec![1, 2, 3]);
        assert_eq!(projection.filter(&table, ""), vec![0, 1, 2, 3]);
        assert!(projection.filter(&table, "missing").is_empty());
    }

    #[test]
    fn filter_does_not_change_the_selection() {
        let table = profile(vec![
            column("id", "INT64"),
            column("name", "UTF8"),
            column("note", "UTF8"),
        ]);
        let projection = ColumnProjection::from_indices(3, &[0, 2]);
        let before = projection.clone();

        let matches = projection.filter(&table, "utf");

        assert_eq!(matches, vec![1, 2]);
        assert_eq!(projection, before);
        assert_eq!(projection.selected_indices(), vec![0, 2]);
    }

    #[test]
    fn ratio_share_and_null_fraction_are_none_when_data_is_missing() {
        let unknown = column("a", "INT64");
        assert_eq!(unknown.compression_ratio(), None);
        assert_eq!(unknown.share_of(Some(100)), None);
        assert_eq!(unknown.null_fraction(Some(10)), None);

        let known = ColumnProfile {
            compressed_bytes: Some(25),
            uncompressed_bytes: Some(100),
            null_count: Some(3),
            ..column("b", "INT64")
        };
        assert_eq!(known.compression_ratio(), Some(4.0));
        assert_eq!(known.share_of(Some(100)), Some(0.25));
        assert_eq!(known.null_fraction(Some(12)), Some(0.25));

        assert_eq!(known.share_of(None), None);
        assert_eq!(known.share_of(Some(0)), None);
        assert_eq!(known.null_fraction(None), None);
        assert_eq!(known.null_fraction(Some(0)), None);

        let empty_chunk = ColumnProfile {
            compressed_bytes: Some(0),
            uncompressed_bytes: Some(0),
            null_count: Some(0),
            ..column("c", "INT64")
        };
        assert_eq!(empty_chunk.compression_ratio(), None);
        assert_eq!(empty_chunk.null_fraction(Some(5)), Some(0.0));
    }

    #[test]
    fn select_all_and_none_report_counts() {
        let mut projection = ColumnProjection::from_indices(4, &[1, 3, 9]);
        assert_eq!(projection.selected_count(), 2);
        assert_eq!(projection.total_count(), 4);
        assert!(projection.is_selected(3));
        assert!(!projection.is_selected(0));
        assert!(!projection.is_selected(9));

        projection.select_all();
        assert_eq!(projection.selected_count(), 4);
        assert_eq!(projection.selected_indices(), vec![0, 1, 2, 3]);
        assert!(projection.is_applicable());

        projection.select_none();
        assert_eq!(projection.selected_count(), 0);
        assert_eq!(projection.total_count(), 4);
        assert!(projection.selected_indices().is_empty());
        assert!(!projection.is_applicable());

        assert_eq!(ColumnProjection::all(2).selected_count(), 2);
        assert_eq!(ColumnProjection::none(2).selected_count(), 0);
    }

    #[test]
    fn merge_sample_fills_distribution_facts_by_name_and_keeps_storage_facts() {
        let mut metadata = profile(vec![
            ColumnProfile {
                codec: Some("LZ4".into()),
                compressed_bytes: Some(10),
                badges: vec![ColumnBadge::SortKey { position: Some(1) }],
                ..column("site_id", "UInt32")
            },
            ColumnProfile {
                null_count: Some(4),
                ..column("path", "String")
            },
            column("untouched", "String"),
        ]);
        let sample = TableProfile::new(
            vec![
                ColumnProfile {
                    null_count: Some(0),
                    distinct_count: Some(38),
                    distinct_exact: false,
                    range: Some(ValueRange {
                        min: "1".into(),
                        max: "211".into(),
                        exact: true,
                    }),
                    top_values: vec![ValueCount {
                        value: "12".into(),
                        count: 900,
                    }],
                    ..column("site_id", "UInt32")
                },
                ColumnProfile {
                    distinct_count: Some(7),
                    ..column("path", "String")
                },
                column("not_in_metadata", "String"),
            ],
            ProfileSource::Sample { rows: 1_000 },
        );

        let filled = metadata.merge_sample(&sample);

        assert_eq!(filled, vec![Arc::<str>::from("site_id"), Arc::from("path")]);

        let site_id = &metadata.columns[0];
        assert_eq!(site_id.codec.as_deref(), Some("LZ4"));
        assert_eq!(site_id.compressed_bytes, Some(10));
        assert_eq!(site_id.sort_key_position(), Some(1));
        assert_eq!(site_id.null_count, Some(0));
        assert_eq!(site_id.distinct_count, Some(38));
        assert!(!site_id.distinct_exact);
        assert_eq!(site_id.top_values.len(), 1);

        let path = &metadata.columns[1];
        assert_eq!(
            path.null_count,
            Some(4),
            "an unknown sample fact keeps the old value"
        );
        assert_eq!(path.distinct_count, Some(7));

        assert_eq!(metadata.columns[2], column("untouched", "String"));
        assert_eq!(metadata.source_label, ProfileSource::FileFooter);
        assert_eq!(
            metadata.distribution_source,
            Some(ProfileSource::Sample { rows: 1_000 })
        );
    }

    #[test]
    fn only_row_reading_sources_report_reading_table_data() {
        assert!(!ProfileSource::FileFooter.reads_table_data());
        assert!(!ProfileSource::Catalog("system.columns").reads_table_data());
        assert!(!ProfileSource::PlannerStatistics("pg_stats").reads_table_data());
        assert!(ProfileSource::LoadedRows { rows: 500 }.reads_table_data());
        assert!(ProfileSource::FullScan { rows: 9 }.reads_table_data());
        assert!(ProfileSource::Sample { rows: 9 }.reads_table_data());
    }

    #[test]
    fn sort_key_position_reads_the_sort_badge_only() {
        let partition_only = ColumnProfile {
            badges: vec![ColumnBadge::PartitionKey],
            ..column("ts", "DateTime")
        };
        assert_eq!(partition_only.sort_key_position(), None);

        let both = ColumnProfile {
            badges: vec![
                ColumnBadge::PartitionKey,
                ColumnBadge::SortKey { position: Some(2) },
            ],
            ..column("ts", "DateTime")
        };
        assert_eq!(both.sort_key_position(), Some(2));

        let unknown_position = ColumnProfile {
            badges: vec![ColumnBadge::SortKey { position: None }],
            ..column("ts", "DateTime")
        };
        assert_eq!(unknown_position.sort_key_position(), None);
    }

    #[test]
    fn partition_ratio_needs_both_sizes() {
        let mut partition = TablePartition::new("202609");
        assert_eq!(partition.compression_ratio(), None);

        partition.compressed_bytes = Some(10);
        partition.uncompressed_bytes = Some(65);
        assert_eq!(partition.compression_ratio(), Some(6.5));

        partition.compressed_bytes = Some(0);
        assert_eq!(partition.compression_ratio(), None);
    }
}
