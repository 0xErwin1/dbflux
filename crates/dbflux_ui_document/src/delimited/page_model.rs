//! The page model of the delimited document: the records loaded so far, the
//! columns they form, the table model built from them, and the translation
//! of the table's pending edits into an [`EditSet`].
//!
//! Table columns are the base columns followed by the columns appended this
//! session. The base columns are as many as the widest loaded record or the
//! header, whichever is wider. A column is named by its header field, and by
//! `column_<n>` (`n` counted from one) when the dialect has no header or the
//! header field is missing or empty.
//!
//! Nothing here reads a file or touches GPUI state. The byte ranges it holds
//! belong to one version of the source, so the model is reset after a save.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use dbflux_components::components::data_table::TableModel;
use dbflux_components::components::data_table::model::{
    CellValue, ColumnKind, ColumnSpec, EditBuffer, InsertAnchor, RowData,
};
use dbflux_delimited::{
    AppendedColumn, EditSet, InsertPosition, Insertion, Page, Record, RecordCount, Replacement,
};
use gpui::TextAlign;

/// Why the page model refused an operation. Nothing was changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageModelError {
    /// The page does not start at the record that follows the loaded ones.
    PageOutOfOrder { expected: u64, found: u64 },

    /// A row was inserted after the last loaded record of a file that is not
    /// fully loaded, so the record it goes before is not known yet. Loading
    /// the next page settles it.
    NextPageRequired,

    /// The dialect has no header record, so there is no column name to write.
    NoHeader,

    /// The table has no column at this index.
    ColumnOutOfRange { column: usize },

    /// A column can be appended only when every record of the file is
    /// loaded. A record that is not loaded can be wider than the loaded
    /// ones, and the appended column would then share a position with one of
    /// its fields.
    FullLoadRequired,
}

impl fmt::Display for PageModelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PageOutOfOrder { expected, found } => write!(
                formatter,
                "the page starts at record {found}, and the next record to load is {expected}"
            ),

            Self::NextPageRequired => formatter.write_str(
                "a row was inserted after the last loaded record: load the next page first",
            ),

            Self::NoHeader => {
                formatter.write_str("the file has no header record, so its columns have no names")
            }

            Self::ColumnOutOfRange { column } => {
                write!(formatter, "there is no column at index {column}")
            }

            Self::FullLoadRequired => formatter
                .write_str("a column can be added only after every record of the file is loaded"),
        }
    }
}

impl std::error::Error for PageModelError {}

/// The loaded part of a delimited file and the column changes made to it.
///
/// See the module documentation for how columns are counted and named.
#[derive(Debug, Clone)]
pub struct PageModel {
    header: Option<Record>,

    /// The data records of every appended page, in file order.
    records: Vec<Record>,

    record_count: RecordCount,
    pages_loaded: usize,

    /// The widest of the header and the loaded records.
    base_column_count: usize,

    had_replacements: bool,

    /// New names of base columns, by column index. A name equal to the
    /// header's own field is never kept here.
    header_renames: BTreeMap<usize, String>,

    /// The names of the columns appended this session, in table order.
    appended_columns: Vec<String>,
}

impl Default for PageModel {
    fn default() -> Self {
        Self::new(None)
    }
}

impl PageModel {
    /// A model without records for a reader whose header record is `header`
    /// ([`dbflux_delimited::PagedReader::header`]).
    pub fn new(header: Option<Record>) -> Self {
        let base_column_count = header.as_ref().map_or(0, |record| record.fields.len());
        let had_replacements = header
            .as_ref()
            .is_some_and(|record| record.had_replacements);

        Self {
            header,
            records: Vec::new(),
            record_count: RecordCount::IndexedSoFar(0),
            pages_loaded: 0,
            base_column_count,
            had_replacements,
            header_renames: BTreeMap::new(),
            appended_columns: Vec::new(),
        }
    }

    /// Drops the header, every record and every column change. Called after a
    /// save, which makes every stored byte range stale. The reloaded file
    /// starts from [`PageModel::new`].
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Adds the next page after the records already loaded and stores
    /// `record_count`, the reader's count after it read the page.
    ///
    /// Returns how many base columns the page added. A page cannot widen the
    /// columns under an appended one: [`PageModel::append_column`] needs a
    /// fully loaded file, which has no further records.
    pub fn append_page(
        &mut self,
        page: Page,
        record_count: RecordCount,
    ) -> Result<usize, PageModelError> {
        let expected = self.records.len() as u64;

        if page.first_record != expected {
            return Err(PageModelError::PageOutOfOrder {
                expected,
                found: page.first_record,
            });
        }

        let previous_column_count = self.base_column_count;

        for record in &page.records {
            self.base_column_count = self.base_column_count.max(record.fields.len());
            self.had_replacements |= record.had_replacements;
        }

        self.records.extend(page.records);
        self.record_count = record_count;
        self.pages_loaded += 1;

        Ok(self.base_column_count - previous_column_count)
    }

    pub fn header(&self) -> Option<&Record> {
        self.header.as_ref()
    }

    /// The loaded data records in file order. Table row `n` is record `n`.
    pub fn records(&self) -> &[Record] {
        &self.records
    }

    /// The reader's count as of the last appended page.
    pub fn record_count(&self) -> RecordCount {
        self.record_count
    }

    /// The index of the page to read next.
    pub fn next_page(&self) -> usize {
        self.pages_loaded
    }

    /// Whether every data record of the file is loaded. False until the
    /// reader reports a total, which can take one more page read after the
    /// last record.
    pub fn is_fully_loaded(&self) -> bool {
        matches!(self.record_count, RecordCount::Total(total) if total == self.records.len() as u64)
    }

    /// Whether decoding replaced a malformed sequence in the header or in any
    /// loaded record, which means the encoding is probably wrong.
    pub fn had_replacements(&self) -> bool {
        self.had_replacements
    }

    /// The number of table columns: the base columns and the appended ones.
    pub fn column_count(&self) -> usize {
        self.base_column_count + self.appended_columns.len()
    }

    /// The name of every table column, in table order.
    pub fn column_names(&self) -> Vec<String> {
        let header_fields = self.renamed_header_fields().unwrap_or_default();
        let appended_names = self.header.is_some().then_some(&self.appended_columns);

        (0..self.column_count())
            .map(|column| {
                let name = match column.checked_sub(self.base_column_count) {
                    None => header_fields.get(column),
                    Some(appended) => appended_names.and_then(|names| names.get(appended)),
                };

                match name {
                    Some(name) if !name.is_empty() => name.clone(),
                    _ => positional_name(column),
                }
            })
            .collect()
    }

    /// The table model of the loaded records. Every cell is text, an empty
    /// field is an empty string, and a record shorter than the table is
    /// padded with empty cells. Display strings are computed here, once per
    /// cell.
    pub fn table_model(&self) -> TableModel {
        let columns = self
            .column_names()
            .into_iter()
            .enumerate()
            .map(|(column, name)| ColumnSpec {
                id: Arc::from(column.to_string()),
                title: Arc::from(name),
                kind: ColumnKind::Text,
                align: TextAlign::Left,
                type_name: Arc::from(""),
            })
            .collect();

        let column_count = self.column_count();
        let empty_cell = CellValue::text("");

        let rows = self
            .records
            .iter()
            .map(|record| {
                let mut cells: Vec<CellValue> = record
                    .fields
                    .iter()
                    .map(|field| CellValue::text(field))
                    .collect();

                cells.resize(column_count, empty_cell.clone());

                RowData { cells }
            })
            .collect();

        TableModel::new(columns, rows)
    }

    /// The names of the columns appended this session, in table order.
    pub fn appended_columns(&self) -> &[String] {
        &self.appended_columns
    }

    /// Whether a column was appended or renamed since the file was loaded.
    pub fn has_column_changes(&self) -> bool {
        !self.appended_columns.is_empty() || !self.header_renames.is_empty()
    }

    /// Drops every rename and every appended column, which leaves the
    /// columns as the loaded records form them.
    pub fn discard_column_changes(&mut self) {
        self.header_renames.clear();
        self.appended_columns.clear();
    }

    /// Adds a column after the last one. Refused with
    /// [`PageModelError::FullLoadRequired`] unless every record is loaded,
    /// because the column's position is the width of the widest record and
    /// only a fully loaded file knows it.
    ///
    /// `name` goes to the header record. It is not written when the model has
    /// no header record, which is a dialect without a header or a file
    /// without any record: the column then has a positional name, and a row
    /// inserted into such an empty file is its first record, which a dialect
    /// with a header reads back as the header.
    pub fn append_column(&mut self, name: String) -> Result<(), PageModelError> {
        if !self.is_fully_loaded() {
            return Err(PageModelError::FullLoadRequired);
        }

        self.appended_columns.push(name);

        Ok(())
    }

    /// Renames the table column at `column`, a base column or an appended
    /// one. An empty name shows as the positional name.
    pub fn rename_column(&mut self, column: usize, name: String) -> Result<(), PageModelError> {
        let Some(header) = &self.header else {
            return Err(PageModelError::NoHeader);
        };

        if column >= self.column_count() {
            return Err(PageModelError::ColumnOutOfRange { column });
        }

        if let Some(appended) = column.checked_sub(self.base_column_count) {
            if let Some(appended_name) = self.appended_columns.get_mut(appended) {
                *appended_name = name;
            }

            return Ok(());
        }

        let original_name = header.fields.get(column).map_or("", String::as_str);

        if name == original_name {
            self.header_renames.remove(&column);
        } else {
            self.header_renames.insert(column, name);
        }

        Ok(())
    }

    /// Translates the table's pending edits and this model's column changes
    /// into the edit set of a save. `source_length` is the
    /// [`dbflux_delimited::PagedReader::source_length`] of the reader that
    /// returned the loaded records, and `edits` is the edit buffer of the
    /// table built from [`PageModel::table_model`].
    ///
    /// - A row marked for deletion is a deletion, whatever was edited in it
    ///   before. A cell edit made after the mark clears the mark in the edit
    ///   buffer, so that row is a replacement, as the table shows it.
    /// - A row whose cells differ from the record is a replacement with the
    ///   record's full fields. A cell set to null is an empty string. A
    ///   record that ends up as it was read is left out, so its bytes are
    ///   copied.
    /// - An inserted row goes before the record that follows it, or at the
    ///   end of a fully loaded file. A row inserted above the first row goes
    ///   before the first loaded record. It is padded to every column. A row
    ///   inserted into a file without columns has no field, and the writer
    ///   renders that as one empty field, a pair of quotes, which a dialect
    ///   without a quote character refuses.
    /// - A renamed column is a replacement of the header record.
    /// - Each appended column carries the values of the records that are
    ///   copied. Replaced and inserted records carry their own.
    ///
    /// A record shorter than the base columns that has a value in an appended
    /// column is replaced and padded, because the writer places appended
    /// fields right after a copied record's last field. A header shorter than
    /// the base columns is replaced and padded the same way when an appended
    /// column has a name.
    ///
    /// Appended columns need every record loaded, so that no record is wider
    /// than the base columns. With appended columns on a model that is not
    /// fully loaded this returns [`PageModelError::FullLoadRequired`].
    pub fn edit_set(
        &self,
        source_length: u64,
        edits: &EditBuffer,
    ) -> Result<EditSet, PageModelError> {
        if !self.appended_columns.is_empty() && !self.is_fully_loaded() {
            return Err(PageModelError::FullLoadRequired);
        }

        let mut edit_set = EditSet::new(source_length);

        if let Some(header) = &self.header
            && let Some(edited) = self.renamed_header_fields()
            && let Some(fields) =
                self.replacement_fields(&header.fields, edited, &self.appended_columns)
        {
            edit_set.replacements.push(Replacement {
                byte_range: header.byte_range.clone(),
                fields,
            });
        }

        let mut appended_columns: Vec<AppendedColumn> = self
            .appended_columns
            .iter()
            .map(|name| AppendedColumn {
                header: name.clone(),
                default_value: String::new(),
                values: Vec::new(),
            })
            .collect();

        for (row, record) in self.records.iter().enumerate() {
            if edits.is_pending_delete(row) {
                edit_set.deletions.push(record.byte_range.clone());
                continue;
            }

            let Some((edited, appended_values)) = self.edited_row(row, record, edits) else {
                continue;
            };

            match self.replacement_fields(&record.fields, edited, &appended_values) {
                Some(fields) => edit_set.replacements.push(Replacement {
                    byte_range: record.byte_range.clone(),
                    fields,
                }),

                None => {
                    for (column, value) in appended_columns.iter_mut().zip(appended_values) {
                        if !value.is_empty() {
                            column.values.push((record.byte_range.clone(), value));
                        }
                    }
                }
            }
        }

        for insert in edits.pending_inserts() {
            let following_row = match insert.anchor {
                InsertAnchor::BeforeFirst => Some(0),
                InsertAnchor::After(row) => row.checked_add(1),
                InsertAnchor::End => None,
            };

            let following_record = following_row.and_then(|row| self.records.get(row));

            let position = match following_record {
                Some(record) => InsertPosition::Before(record.byte_range.clone()),
                None if self.is_fully_loaded() => InsertPosition::End,
                None => return Err(PageModelError::NextPageRequired),
            };

            let mut fields: Vec<String> = insert.data.iter().map(CellValue::edit_text).collect();

            if fields.len() < self.column_count() {
                fields.resize(self.column_count(), String::new());
            }

            edit_set.insertions.push(Insertion { position, fields });
        }

        edit_set.appended_columns = appended_columns;

        Ok(edit_set)
    }

    /// The header's fields with the renames applied, padded up to the last
    /// renamed column. `None` without a header.
    fn renamed_header_fields(&self) -> Option<Vec<String>> {
        let mut fields = self.header.as_ref()?.fields.clone();

        for (&column, name) in &self.header_renames {
            set_field(&mut fields, column, name.clone());
        }

        Some(fields)
    }

    /// The base fields of `record` with the pending cell edits of table row
    /// `row` applied, and the row's value for each appended column. `None`
    /// when the row has no edited cell.
    fn edited_row(
        &self,
        row: usize,
        record: &Record,
        edits: &EditBuffer,
    ) -> Option<(Vec<String>, Vec<String>)> {
        let unedited_cell = CellValue::text("");

        let edited_cells: Vec<(usize, String)> = (0..self.column_count())
            .filter(|&column| edits.is_cell_dirty(row, column))
            .map(|column| {
                let text = edits.get_cell(row, column, &unedited_cell).edit_text();
                (column, text)
            })
            .collect();

        if edited_cells.is_empty() {
            return None;
        }

        let mut base_fields = record.fields.clone();
        let mut appended_values = vec![String::new(); self.appended_columns.len()];

        for (column, text) in edited_cells {
            match column.checked_sub(self.base_column_count) {
                None => set_field(&mut base_fields, column, text),

                Some(appended) => {
                    if let Some(value) = appended_values.get_mut(appended) {
                        *value = text;
                    }
                }
            }
        }

        Some((base_fields, appended_values))
    }

    /// The fields to write for a record read as `original`, whose base fields
    /// are now `edited` and whose values for the appended columns are
    /// `appended_values`. `None` when the record can be copied as it is, with
    /// its appended values left to the writer.
    fn replacement_fields(
        &self,
        original: &[String],
        mut edited: Vec<String>,
        appended_values: &[String],
    ) -> Option<Vec<String>> {
        while edited.len() > original.len() && edited.last().is_some_and(String::is_empty) {
            edited.pop();
        }

        let is_misplaced_when_copied = original.len() < self.base_column_count
            && appended_values.iter().any(|value| !value.is_empty());

        if edited == original && !is_misplaced_when_copied {
            return None;
        }

        if !appended_values.is_empty() {
            if edited.len() < self.base_column_count {
                edited.resize(self.base_column_count, String::new());
            }

            edited.extend_from_slice(appended_values);
        }

        Some(edited)
    }
}

/// The name of a column that has none of its own.
pub(super) fn positional_name(column: usize) -> String {
    format!("column_{}", column + 1)
}

/// Sets the field at `column`, padding with empty fields up to it.
fn set_field(fields: &mut Vec<String>, column: usize, value: String) {
    if fields.len() <= column {
        fields.resize(column + 1, String::new());
    }

    if let Some(field) = fields.get_mut(column) {
        *field = value;
    }
}
