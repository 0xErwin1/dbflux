//! The rows of a Parquet file loaded so far, as the data table shows them.
//!
//! The reader hands over cells whose display text is already built, so the
//! table shows exactly that text: a binary value keeps its hex preview and a
//! nested value its truncated JSON. Nothing here does I/O.

use std::sync::Arc;

use dbflux_components::components::column_facts::{
    format_optional_bytes, format_percent, format_ratio,
};
use dbflux_components::components::data_table::HeaderAnnotation;
use dbflux_components::components::data_table::TableModel;
use dbflux_components::components::data_table::model::{
    CellValue, ColumnKind, ColumnSpec, RowData,
};
use dbflux_core::ColumnProfile;
use dbflux_parquet::{Cell, CellKind, CellPage, ColumnDisplay, ColumnStatistics};
use gpui::TextAlign;

/// The shown columns of a Parquet file and the rows read of them so far.
///
/// The rows live in the table model the data table shows, shared rather than
/// copied, so the loaded rows are held once.
pub(super) struct ParquetPageModel {
    table: Arc<TableModel>,
    total_rows: u64,
}

impl ParquetPageModel {
    /// A model of the columns of `first_page`, holding its rows, for a file
    /// of `total_rows` rows.
    pub(super) fn new(first_page: CellPage, total_rows: u64) -> Self {
        let columns = first_page
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| column_spec(index, column))
            .collect();

        let mut model = Self {
            table: Arc::new(TableModel::new(columns, Vec::new())),
            total_rows,
        };

        model.append(first_page);
        model
    }

    /// Appends the rows of `page`, a later window of the same columns. While
    /// the data table still shows the previous model, the rows are copied
    /// once into a new one; the previous copy is freed when the table takes
    /// the new model.
    pub(super) fn append(&mut self, page: CellPage) {
        Arc::make_mut(&mut self.table)
            .rows
            .extend(page.rows.iter().map(|row| RowData {
                cells: row.iter().map(cell_value).collect(),
            }));
    }

    pub(super) fn loaded_rows(&self) -> u64 {
        self.table.rows.len() as u64
    }

    pub(super) fn total_rows(&self) -> u64 {
        self.total_rows
    }

    pub(super) fn is_fully_loaded(&self) -> bool {
        self.loaded_rows() >= self.total_rows
    }

    pub(super) fn table_model(&self) -> Arc<TableModel> {
        self.table.clone()
    }
}

/// The header of one column. The kind decides alignment only: the rows are
/// never sorted or edited, so the cell kinds below carry no other meaning.
fn column_spec(index: usize, column: &ColumnDisplay) -> ColumnSpec {
    let kind = match column.kind {
        dbflux_parquet::ColumnKind::Integer => ColumnKind::Integer,
        dbflux_parquet::ColumnKind::Float => ColumnKind::Float,
        dbflux_parquet::ColumnKind::Text | dbflux_parquet::ColumnKind::Timestamp => {
            ColumnKind::Text
        }
        dbflux_parquet::ColumnKind::Unknown => ColumnKind::Unknown,
    };

    let align = match kind {
        ColumnKind::Integer | ColumnKind::Float => TextAlign::Right,
        _ => TextAlign::Left,
    };

    ColumnSpec {
        id: Arc::from(index.to_string()),
        title: column.name.clone(),
        kind,
        align,
        type_name: column.type_name.clone(),
    }
}

/// The table cell of one value, showing the reader's display text.
///
/// Floats are text cells: the reader prints a Float32 in its own width, which
/// a float cell would widen to the digits of an `f64`. Binary and nested
/// values are text too, because a bytes cell shows only a length and a JSON
/// cell expects a whole document where the reader may have cut it short.
fn cell_value(cell: &Cell) -> CellValue {
    match cell.kind {
        CellKind::Null => CellValue::null(),
        CellKind::Bool(value) => CellValue::bool(value),
        CellKind::Integer(value) => CellValue::int(value),
        CellKind::Float(_) | CellKind::Text | CellKind::Binary { .. } | CellKind::Nested => {
            CellValue::text(&cell.display)
        }
    }
}

/// The profile of one column from what its footer says.
pub(super) fn column_profile(statistics: &ColumnStatistics) -> ColumnProfile {
    ColumnProfile {
        name: statistics.name.clone(),
        type_name: statistics.type_name.clone(),
        badges: Vec::new(),
        codec: statistics.codec.clone(),
        compressed_bytes: Some(statistics.compressed_bytes),
        uncompressed_bytes: Some(statistics.uncompressed_bytes),
        null_count: statistics.null_count,
        distinct_count: statistics.distinct_count,
        range: statistics
            .range
            .as_ref()
            .map(|range| dbflux_core::ValueRange {
                min: range.min.clone(),
                max: range.max.clone(),
                exact: range.exact,
            }),
    }
}

/// The second header line of a column: its compression ratio and size on
/// disk, and its share of nulls. The type is already on the first line.
pub(super) fn header_annotation(profile: &ColumnProfile, row_count: u64) -> HeaderAnnotation {
    let leading = format!(
        "{}  ·  {}",
        format_ratio(profile.compression_ratio()),
        format_optional_bytes(profile.compressed_bytes)
    );

    let nulls = format_percent(profile.null_fraction(Some(row_count)));

    HeaderAnnotation::new(leading).trailing(dbflux_i18n::t!(
        "document.parquet.header.nulls",
        percent = nulls
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use dbflux_parquet::{Cell, CellKind, CellPage, ColumnDisplay, ColumnKind};

    use super::ParquetPageModel;

    fn page(first: i64, rows: i64) -> CellPage {
        CellPage {
            columns: vec![ColumnDisplay {
                name: Arc::from("id"),
                type_name: Arc::from("INT64"),
                kind: ColumnKind::Integer,
            }],
            rows: (first..first + rows)
                .map(|value| {
                    vec![Cell {
                        kind: CellKind::Integer(value),
                        display: Arc::from(value.to_string()),
                    }]
                })
                .collect(),
        }
    }

    #[test]
    fn the_table_shares_the_loaded_rows_instead_of_copying_them() {
        let mut model = ParquetPageModel::new(page(0, 3), 6);

        let shown = model.table_model();
        assert!(Arc::ptr_eq(&shown, &model.table_model()));

        drop(shown);
        model.append(page(3, 3));

        let shown = model.table_model();
        assert_eq!(shown.rows.len(), 6);
        assert_eq!(Arc::strong_count(&shown), 2, "the model and this handle");
        assert!(model.is_fully_loaded());
    }
}
