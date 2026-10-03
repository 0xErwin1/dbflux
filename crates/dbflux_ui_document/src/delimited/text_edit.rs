//! Turning an edit of the raw text into changes of the pending state. Pure:
//! nothing here reads a file or touches GPUI state.
//!
//! The raw text was rendered from the page model and the table's edit
//! buffer (`text.rs`), and the layout says which row of the table each of
//! its records is, so the fields each record of it shows are known from the
//! pending state without reading the text again. The edited text is read
//! with the reader's own scanner under the dialect in effect, and its
//! records are compared with the rendered ones as a sequence, a record equal
//! to another when its fields are.
//!
//! The comparison runs in three passes, each over records replaced by
//! numbers: first by their exact text without the line terminator, so that
//! a line the user did not touch keeps its row and its bytes; then, in each run of removed and added
//! records between two kept ones, by their fields, so that a line whose
//! quoting or line break alone changed keeps its row; and what is left of a
//! run is paired up in order as changed records, so that editing a line
//! changes its row instead of deleting and inserting it.
//!
//! Each of the first two passes is a patience diff: the records that appear
//! once in both texts anchor the comparison, the longest run of them in the
//! same order is kept, and the sections between them are split the same
//! way, down to sections without such a record, which Myers' linear-space
//! search (divide and conquer on the middle snake) compares. That search has
//! no limit past which it gives up, finds a shortest edit script for its
//! section, so it never leaves two equal records in a run of removed and
//! added ones, and takes time in the size of the section times the number of
//! changes in it and memory in the size of the section. Anchoring first
//! keeps a line that appears once from being traded for copies of a line
//! that appears more often.
//!
//! What the comparison gives becomes the operations the table itself makes:
//!
//! - A record whose fields did not change gives nothing, so its bytes are
//!   kept by a save: a record whose line break or quoting alone changed is
//!   one of them.
//! - A changed record gives a cell edit for every field whose value
//!   changed, on its row, a loaded record or a pending insert. A missing
//!   field is an empty one.
//! - A new record is a pending insert where the text puts it: after the row
//!   before it, as the table anchors a row added below one, or above the
//!   first row. The table orders the rows of one anchor by their place in its
//!   list of pending inserts, so a new record that the text puts before a
//!   pending insert of the same anchor is placed before it in that list, and
//!   that insert is left as it is, in the table and in its undo history.
//! - A removed record marks its loaded record for deletion, and removes a
//!   pending insert.
//! - The header is the first record, when the text has one. A changed field
//!   renames its column, and a field after the last column appends a column,
//!   which only a fully loaded file takes. A header with fewer fields, or
//!   with its fields reordered, is refused: columns are never removed or
//!   moved.
//! - A data record with more fields than the columns is refused, unless the
//!   header gained the columns in the same text. Fewer fields are allowed.

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;

use dbflux_components::components::data_table::model::{CellValue, EditBuffer, InsertAnchor};
use dbflux_delimited::{Dialect, ParseTextError, parse_text};

use super::editing::pad_pending_inserts;
use super::page_model::{PageModel, PageModelError};
use super::text::{RawPlan, TextLayout, TextRow};

/// The changes an edited text makes to the pending state, in the terms the
/// table and the page model take them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct TextEdits {
    /// New names of table columns, by column index.
    pub(super) renames: Vec<(usize, String)>,

    /// Names of columns to append after the last one, in order.
    pub(super) appended_columns: Vec<String>,

    /// New values of cells of loaded records: row, column, value.
    pub(super) base_cells: Vec<(usize, usize, String)>,

    /// New values of cells of pending inserts: insert index, column, value.
    pub(super) insert_cells: Vec<(usize, usize, String)>,

    /// Loaded records to mark for deletion.
    pub(super) deleted_rows: Vec<usize>,

    /// Pending inserts to remove, by their index before any is removed.
    pub(super) removed_inserts: Vec<usize>,

    /// Rows to insert, in the order they are added.
    pub(super) added_inserts: Vec<AddedInsert>,
}

/// A row the edited text adds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AddedInsert {
    pub(super) anchor: InsertAnchor,
    pub(super) fields: Vec<String>,

    /// The pending insert of the same anchor the text puts right after it,
    /// by its index before any change: the row goes before that insert,
    /// which keeps its place, its values and its undo history. `None` puts
    /// it after the inserts of its anchor.
    pub(super) before: Option<usize>,
}

impl TextEdits {
    pub(super) fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// Why an edited text cannot be applied. Lines count from one, by line
/// feeds, as the editor numbers them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TextEditError {
    /// The text ends inside a quoted field that is never closed.
    UnclosedQuote { line: usize },

    /// The text holds a character the file's encoding cannot represent.
    Unencodable {
        line: usize,
        character: char,
        encoding: &'static str,
    },

    /// The reader refuses the dialect.
    Dialect(String),

    /// The header has fewer fields than it had.
    HeaderFieldRemoved,

    /// The header's fields were reordered.
    HeaderReordered,

    /// The header has fields after the last column, and the file is not
    /// fully loaded.
    ColumnsNeedFullLoad,

    /// A data record has more fields than the columns.
    TooManyFields {
        line: usize,
        fields: usize,
        columns: usize,
        has_header: bool,
    },

    /// The text the editor started from does not read as the rows it was
    /// rendered from.
    Unmapped,
}

impl TextEditError {
    /// What the user is told.
    pub(super) fn cause(&self) -> String {
        match self {
            Self::UnclosedQuote { line } => {
                dbflux_i18n::t!("document.delimited.error.text_unclosed_quote", line = line)
            }

            Self::Unencodable {
                line,
                character,
                encoding,
            } => dbflux_i18n::t!(
                "document.delimited.error.text_unencodable",
                line = line,
                character = format!("{character:?}"),
                encoding = encoding
            ),

            Self::Dialect(cause) => cause.clone(),

            Self::HeaderFieldRemoved => {
                dbflux_i18n::t!("document.delimited.error.text_header_field_removed")
            }

            Self::HeaderReordered => {
                dbflux_i18n::t!("document.delimited.error.text_header_reordered")
            }

            Self::ColumnsNeedFullLoad => {
                dbflux_i18n::t!("document.delimited.error.text_columns_need_full_load")
            }

            Self::TooManyFields {
                line,
                fields,
                columns,
                has_header: true,
            } => dbflux_i18n::t!(
                "document.delimited.error.text_too_many_fields",
                line = line,
                fields = fields,
                columns = columns
            ),

            Self::TooManyFields {
                line,
                fields,
                columns,
                has_header: false,
            } => dbflux_i18n::t!(
                "document.delimited.error.text_too_many_fields_no_header",
                line = line,
                fields = fields,
                columns = columns
            ),

            Self::Unmapped => dbflux_i18n::t!("document.delimited.error.text_unmapped"),
        }
    }
}

/// The changes that make the pending state show `edited`, the editor's text
/// after the user edited `rendered`. `layout` is where each row is in
/// `rendered`, which was rendered from `model` and `buffer`, the edit buffer
/// of the table built from it, under `dialect`. See the module
/// documentation.
///
/// # Errors
///
/// A text the reader would not read as `edited` says, or a change the table
/// cannot make: see [`TextEditError`].
pub(super) fn text_edits(
    layout: &TextLayout,
    rendered: &str,
    edited: &str,
    dialect: &Dialect,
    model: &PageModel,
    buffer: &EditBuffer,
) -> Result<TextEdits, TextEditError> {
    if edited == rendered {
        return Ok(TextEdits::default());
    }

    // The fields each row of the rendered text shows, from the pending
    // state it was rendered from. The source length only matters to a save.
    let edit_set = model
        .loaded_edit_set(0, buffer)
        .map_err(|_| TextEditError::Unmapped)?;
    let before = rendered_fields(layout, model, &edit_set).ok_or(TextEditError::Unmapped)?;

    let EditedRecords {
        records: after,
        kept_start,
        kept_end,
    } = edited_records(layout, rendered, edited, dialect, &before)?;

    check_unclosed_quote(layout, rendered, &after, edited)?;

    let has_header = layout
        .records
        .first()
        .is_some_and(|record| record.row == TextRow::Header);

    let data_start = usize::from(has_header);
    let mut edits = TextEdits::default();

    let column_count = if has_header {
        let old_header = before.first().map_or(&[][..], |fields| fields.as_ref());
        let new_header = after
            .first()
            .map_or(&[][..], |record| record.fields.as_ref());

        header_edits(old_header, new_header, model, &mut edits)?
    } else {
        model.column_count()
    };

    let old_rows: Vec<OldRow<'_>> = layout
        .records
        .iter()
        .zip(&before)
        .skip(data_start)
        .map(|(record, fields)| OldRow {
            row: record.row,
            fields: fields.as_ref(),
            text: rendered.get(record.text.clone()).unwrap_or_default(),
        })
        .collect();

    let new_rows: &[EditedRecord<'_>] = after.get(data_start..).unwrap_or_default();

    // The rows the edit left alone at the start and at the end of the text.
    let kept_rows = kept_start.saturating_sub(data_start);
    let kept_rows = kept_rows.min(old_rows.len()).min(new_rows.len());
    let kept_rows_at_end = kept_end
        .min(old_rows.len() - kept_rows)
        .min(new_rows.len() - kept_rows);

    let rows = RowEdits {
        old_rows: &old_rows,
        new_rows,
        kept_rows,
        kept_rows_at_end,
        edited,
        buffer,
        column_count,
        has_header,
    };

    rows.collect(&mut edits)?;

    Ok(edits)
}

/// The fields of every row of a rendered text, in its order, as the text
/// shows them: those `render_raw` rendered each row from. `None` when a row
/// of the layout is not in the pending state.
fn rendered_fields<'a>(
    layout: &TextLayout,
    model: &'a PageModel,
    edit_set: &'a dbflux_delimited::EditSet,
) -> Option<Vec<Cow<'a, [String]>>> {
    let plan = RawPlan::new(edit_set);

    layout
        .records
        .iter()
        .map(|record| match record.row {
            TextRow::Header => Some(plan.shown_fields(model.header()?, true)),
            TextRow::Base(row) => Some(plan.shown_fields(model.records().get(row)?, false)),

            TextRow::Insert(index) => edit_set
                .insertions
                .get(index)
                .map(|insertion| Cow::Borrowed(insertion.fields.as_slice())),
        })
        .collect()
}

/// A record of the edited text: its fields, its place in the text, and
/// whether it ends inside a quoted field that is never closed.
struct EditedRecord<'a> {
    fields: Cow<'a, [String]>,
    text_range: Range<usize>,
    ends_inside_quotes: bool,
}

/// The records of the edited text, and how many of them at its start and at
/// its end are rendered records the edit left as they were.
struct EditedRecords<'a> {
    records: Vec<EditedRecord<'a>>,
    kept_start: usize,
    kept_end: usize,
}

/// The records of `edited`. Only the part of it that differs from
/// `rendered` is read: the records before and after that part are the
/// rendered ones, whose fields `before` holds. When that part does not read
/// as whole records, so that a quote or a line break in it could change how
/// the records after it read, the whole text is read.
fn edited_records<'a>(
    layout: &TextLayout,
    rendered: &str,
    edited: &str,
    dialect: &Dialect,
    before: &'a [Cow<'a, [String]>],
) -> Result<EditedRecords<'a>, TextEditError> {
    if let Some(records) = edited_middle(layout, rendered, edited, dialect, before)? {
        return Ok(records);
    }

    let parsed = parse_text(edited, dialect).map_err(|error| parse_error(edited, 0, error))?;

    Ok(EditedRecords {
        records: parsed
            .into_iter()
            .map(|record| EditedRecord {
                fields: Cow::Owned(record.fields),
                text_range: record.text_range,
                ends_inside_quotes: record.ends_inside_quotes,
            })
            .collect(),
        kept_start: 0,
        kept_end: 0,
    })
}

/// The records of `edited` read from the part of it that differs from
/// `rendered` only, or `None` when that part cannot be read on its own.
///
/// A rendered record is kept at the start when it ends inside the bytes both
/// texts start with and ends with a line feed, or with a carriage return
/// that is not the last byte they share, which a line feed typed after it
/// would join. A rendered record is kept at the end when it starts inside
/// the bytes both texts end with. The part between is read when it ends
/// with a line break that is not a carriage return before a line feed of
/// the kept records, and outside a quoted field: the kept records then read
/// in the edited text as they do in the rendered one.
fn edited_middle<'a>(
    layout: &TextLayout,
    rendered: &str,
    edited: &str,
    dialect: &Dialect,
    before: &'a [Cow<'a, [String]>],
) -> Result<Option<EditedRecords<'a>>, TextEditError> {
    let records = &layout.records;

    let common_prefix = rendered
        .bytes()
        .zip(edited.bytes())
        .take_while(|(old, new)| old == new)
        .count();
    let longest_suffix = rendered.len().min(edited.len()) - common_prefix;
    let common_suffix = rendered
        .bytes()
        .rev()
        .zip(edited.bytes().rev())
        .take(longest_suffix)
        .take_while(|(old, new)| old == new)
        .count();

    let kept_start = records
        .iter()
        .take_while(|record| {
            let text = rendered.get(record.text.clone()).unwrap_or_default();

            text.ends_with('\n') || (text.ends_with('\r') && record.text.end < common_prefix)
        })
        .take_while(|record| record.text.end <= common_prefix)
        .count();

    let suffix_start = rendered.len() - common_suffix;
    let kept_end = records
        .iter()
        .rev()
        .take_while(|record| record.text.start >= suffix_start)
        .count()
        .min(records.len() - kept_start);

    let middle_start = kept_start
        .checked_sub(1)
        .and_then(|last| records.get(last))
        .map_or(0, |record| record.text.end);
    let old_middle_end = records
        .get(records.len() - kept_end)
        .map_or(rendered.len(), |record| record.text.start);
    let new_middle_end = edited.len() - (rendered.len() - old_middle_end);

    let Some(middle) = edited.get(middle_start..new_middle_end) else {
        return Ok(None);
    };

    let parsed =
        parse_text(middle, dialect).map_err(|error| parse_error(edited, middle_start, error))?;

    let suffix = rendered.get(old_middle_end..).unwrap_or_default();

    let reads_on_its_own = match parsed.last() {
        _ if kept_end == 0 => true,

        Some(last) => {
            let text = middle.get(last.text_range.clone()).unwrap_or_default();

            !last.ends_inside_quotes
                && (text.ends_with('\n') || (text.ends_with('\r') && !suffix.starts_with('\n')))
        }

        None => {
            let prefix = rendered.get(..middle_start).unwrap_or_default();
            !(prefix.ends_with('\r') && suffix.starts_with('\n'))
        }
    };

    if !reads_on_its_own {
        return Ok(None);
    }

    let kept = |index: usize, shift: isize| {
        let record = records.get(index)?;
        let fields = before.get(index)?;

        let start = record.text.start.checked_add_signed(shift)?;
        let end = record.text.end.checked_add_signed(shift)?;

        Some(EditedRecord {
            fields: Cow::Borrowed(fields.as_ref()),
            text_range: start..end,
            ends_inside_quotes: false,
        })
    };

    let shift = isize::try_from(new_middle_end).unwrap_or(isize::MAX)
        - isize::try_from(old_middle_end).unwrap_or(isize::MAX);

    let mut all = Vec::with_capacity(kept_start + parsed.len() + kept_end);

    for index in 0..kept_start {
        all.extend(kept(index, 0));
    }

    all.extend(parsed.into_iter().map(|record| EditedRecord {
        fields: Cow::Owned(record.fields),
        text_range: record.text_range.start + middle_start..record.text_range.end + middle_start,
        ends_inside_quotes: record.ends_inside_quotes,
    }));

    for index in records.len() - kept_end..records.len() {
        all.extend(kept(index, shift));
    }

    Ok(Some(EditedRecords {
        records: all,
        kept_start,
        kept_end,
    }))
}

/// The error of a text the reader cannot read: `text` from byte `start` of
/// the edited text on.
fn parse_error(edited: &str, start: usize, error: ParseTextError) -> TextEditError {
    match error {
        ParseTextError::UnencodableCharacter {
            offset,
            character,
            encoding,
        } => TextEditError::Unencodable {
            line: line_at(edited, start + offset),
            character,
            encoding,
        },

        ParseTextError::Dialect(error) => TextEditError::Dialect(error.to_string()),
    }
}

/// The line of `text` that byte `offset` is on, counted from one by line
/// feeds, as the editor numbers its lines.
fn line_at(text: &str, offset: usize) -> usize {
    let before = text.get(..offset).unwrap_or(text);

    before.bytes().filter(|byte| *byte == b'\n').count() + 1
}

/// Refuses an edited text that ends inside a quoted field that is never
/// closed, unless the user left its last record as the rendered text ended:
/// a file that already ends that way can still be edited above its last
/// record.
fn check_unclosed_quote(
    layout: &TextLayout,
    rendered: &str,
    after: &[EditedRecord<'_>],
    edited: &str,
) -> Result<(), TextEditError> {
    let Some(last) = after.last().filter(|record| record.ends_inside_quotes) else {
        return Ok(());
    };

    let kept_as_it_was = layout.records.last().is_some_and(|original| {
        rendered.get(original.text.clone()) == edited.get(last.text_range.clone())
    });

    if kept_as_it_was {
        return Ok(());
    }

    Err(TextEditError::UnclosedQuote {
        line: line_at(edited, last.text_range.start),
    })
}

/// Adds the renames and appended columns of a header edited from `old` into
/// `new`, and returns how many columns the table has with them.
fn header_edits(
    old: &[String],
    new: &[String],
    model: &PageModel,
    edits: &mut TextEdits,
) -> Result<usize, TextEditError> {
    if new.len() < old.len() {
        return Err(TextEditError::HeaderFieldRemoved);
    }

    if is_reordered(old, new.get(..old.len()).unwrap_or_default()) {
        return Err(TextEditError::HeaderReordered);
    }

    let column_count = model.column_count();

    for (column, name) in new.iter().enumerate().take(column_count) {
        let before = old.get(column).map_or("", String::as_str);

        if name != before {
            edits.renames.push((column, name.clone()));
        }
    }

    let appended: Vec<String> = new.iter().skip(column_count).cloned().collect();

    if !appended.is_empty() && !model.is_fully_loaded() {
        return Err(TextEditError::ColumnsNeedFullLoad);
    }

    edits.appended_columns = appended;

    Ok(column_count + edits.appended_columns.len())
}

/// Whether `new` holds the fields of `old` in another order: two or more
/// fields changed, and the changed ones hold the same names between them.
fn is_reordered(old: &[String], new: &[String]) -> bool {
    let changed: Vec<usize> = (0..old.len().min(new.len()))
        .filter(|&index| old.get(index) != new.get(index))
        .collect();

    if changed.len() < 2 {
        return false;
    }

    let mut old_names: Vec<&String> = changed.iter().filter_map(|&index| old.get(index)).collect();
    let mut new_names: Vec<&String> = changed.iter().filter_map(|&index| new.get(index)).collect();

    old_names.sort_unstable();
    new_names.sort_unstable();

    old_names == new_names
}

/// Which of the table's rows each record of the edited text is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pairing {
    /// The old row at the first index is the new record at the second, with
    /// the same fields.
    Same(usize, usize),

    /// The old row at the first index is the new record at the second, with
    /// other fields.
    Changed(usize, usize),

    /// The old row at this index is gone.
    Removed(usize),

    /// The new record at this index is a new row.
    Added(usize),
}

/// The pending inserts that one anchor groups together: the table shows the
/// inserts of a group in the order they were added.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum InsertGroup {
    BeforeFirst,
    After(usize),

    /// At the end of a table without loaded rows.
    End,
}

/// A row of the rendered text: which row of the table it is, the fields the
/// text showed for it, and its text.
struct OldRow<'a> {
    row: TextRow,
    fields: &'a [String],
    text: &'a str,
}

/// The data rows of an edited text, with what turning them into operations
/// needs.
struct RowEdits<'a> {
    /// The rows the text was rendered from, in text order.
    old_rows: &'a [OldRow<'a>],
    new_rows: &'a [EditedRecord<'a>],

    /// How many rows at the start and at the end of both the edit left
    /// alone: they are the same rows, and are not compared.
    kept_rows: usize,
    kept_rows_at_end: usize,

    edited: &'a str,
    buffer: &'a EditBuffer,

    /// The columns of the table once the header's changes are made.
    column_count: usize,
    has_header: bool,
}

impl RowEdits<'_> {
    /// Adds the operations of the data rows to `edits`. See the module
    /// documentation.
    fn collect(&self, edits: &mut TextEdits) -> Result<(), TextEditError> {
        let base_row_count = self.buffer.base_row_count();

        // Where a new record goes: after the row before it in the text.
        let mut anchor = if base_row_count > 0 {
            InsertAnchor::BeforeFirst
        } else {
            InsertAnchor::End
        };

        // The new rows that go before the next kept pending insert of their
        // group, by their index among the added rows.
        let mut waiting: Vec<(usize, InsertGroup)> = Vec::new();

        for pairing in self.pairings() {
            match pairing {
                Pairing::Removed(old) => match self.old_rows.get(old).map(|old_row| old_row.row) {
                    Some(TextRow::Base(row)) => edits.deleted_rows.push(row),
                    Some(TextRow::Insert(insert)) => edits.removed_inserts.push(insert),
                    Some(TextRow::Header) | None => {}
                },

                Pairing::Added(new) => {
                    let fields = self.checked_fields(new)?;

                    waiting.push((
                        edits.added_inserts.len(),
                        insert_group(anchor, base_row_count),
                    ));
                    edits.added_inserts.push(AddedInsert {
                        anchor,
                        fields: fields.to_vec(),
                        before: None,
                    });
                }

                Pairing::Same(old, new) | Pairing::Changed(old, new) => {
                    let is_changed = matches!(pairing, Pairing::Changed(..));
                    let fields = if is_changed {
                        self.checked_fields(new)?
                    } else {
                        self.new_rows
                            .get(new)
                            .map_or(&[][..], |record| record.fields.as_ref())
                    };

                    let Some(old_row) = self.old_rows.get(old) else {
                        continue;
                    };

                    let old_values = old_row.fields;

                    match old_row.row {
                        TextRow::Base(row) => {
                            anchor = InsertAnchor::After(row);

                            for (column, value) in changed_cells(old_values, fields) {
                                edits.base_cells.push((row, column, value));
                            }
                        }

                        TextRow::Insert(insert) => {
                            let Some(pending) = self.buffer.pending_inserts().get(insert) else {
                                continue;
                            };

                            anchor = pending.anchor;

                            // The new rows of its group so far go before it.
                            let group = insert_group(anchor, base_row_count);

                            waiting.retain(|(added, waiting_group)| {
                                if *waiting_group != group {
                                    return true;
                                }

                                if let Some(added) = edits.added_inserts.get_mut(*added) {
                                    added.before = Some(insert);
                                }

                                false
                            });

                            for (column, value) in changed_cells(old_values, fields) {
                                edits.insert_cells.push((insert, column, value));
                            }
                        }

                        TextRow::Header => {}
                    }
                }
            }
        }

        Ok(())
    }

    /// Which old row each new record is, in the order of the new records:
    /// the rows the edit left alone at the start and at the end are the
    /// same, and the ones between are compared by [`pair_rows`].
    fn pairings(&self) -> Vec<Pairing> {
        let old_end = self.old_rows.len() - self.kept_rows_at_end;
        let new_end = self.new_rows.len() - self.kept_rows_at_end;

        let old_middle = self
            .old_rows
            .get(self.kept_rows..old_end)
            .unwrap_or_default();
        let new_middle = self
            .new_rows
            .get(self.kept_rows..new_end)
            .unwrap_or_default();

        let old_fields: Vec<&[String]> = old_middle.iter().map(|row| row.fields).collect();
        let new_fields: Vec<&[String]> = new_middle
            .iter()
            .map(|record| record.fields.as_ref())
            .collect();

        let old_texts: Vec<&str> = old_middle
            .iter()
            .map(|row| without_terminator(row.text))
            .collect();
        let new_texts: Vec<&str> = new_middle
            .iter()
            .map(|record| {
                without_terminator(
                    self.edited
                        .get(record.text_range.clone())
                        .unwrap_or_default(),
                )
            })
            .collect();

        let (old_text_ids, new_text_ids) = intern(&old_texts, &new_texts);
        let (old_field_ids, new_field_ids) = intern(&old_fields, &new_fields);

        let records = Records {
            old_texts: &old_text_ids,
            new_texts: &new_text_ids,
            old_fields: &old_field_ids,
            new_fields: &new_field_ids,
        };

        let start = self.kept_rows;

        let middle = pair_rows(&records)
            .into_iter()
            .map(|pairing| match pairing {
                Pairing::Same(old, new) => Pairing::Same(old + start, new + start),
                Pairing::Changed(old, new) => Pairing::Changed(old + start, new + start),
                Pairing::Removed(old) => Pairing::Removed(old + start),
                Pairing::Added(new) => Pairing::Added(new + start),
            });

        (0..start)
            .map(|index| Pairing::Same(index, index))
            .chain(middle)
            .chain(
                (0..self.kept_rows_at_end)
                    .map(|index| Pairing::Same(old_end + index, new_end + index)),
            )
            .collect()
    }

    /// The fields of new record `new`, refused when it has more of them than
    /// the columns.
    fn checked_fields(&self, new: usize) -> Result<&[String], TextEditError> {
        let Some(record) = self.new_rows.get(new) else {
            return Ok(&[]);
        };

        if record.fields.len() > self.column_count {
            return Err(TextEditError::TooManyFields {
                line: line_at(self.edited, record.text_range.start),
                fields: record.fields.len(),
                columns: self.column_count,
                has_header: self.has_header,
            });
        }

        Ok(&record.fields)
    }
}

/// `text` without the line terminator it ends with: a record kept with its
/// text keeps the terminator it has in the file, whatever the edited text
/// ends it with.
fn without_terminator(text: &str) -> &str {
    text.strip_suffix("\r\n")
        .or_else(|| text.strip_suffix('\n'))
        .or_else(|| text.strip_suffix('\r'))
        .unwrap_or(text)
}

/// The group of a pending insert anchored at `anchor` in a table of
/// `base_row_count` loaded rows: one at the end of a table with rows is
/// shown with the ones after its last row.
fn insert_group(anchor: InsertAnchor, base_row_count: usize) -> InsertGroup {
    match anchor {
        InsertAnchor::BeforeFirst => InsertGroup::BeforeFirst,
        InsertAnchor::After(row) => InsertGroup::After(row),

        InsertAnchor::End => match base_row_count.checked_sub(1) {
            Some(last) => InsertGroup::After(last),
            None => InsertGroup::End,
        },
    }
}

/// The columns whose value differs between `old` and `new`, with the new
/// value. A missing field is an empty one.
fn changed_cells<'a>(
    old: &'a [String],
    new: &'a [String],
) -> impl Iterator<Item = (usize, String)> + 'a {
    let width = old.len().max(new.len());
    let value = |fields: &'a [String], column: usize| fields.get(column).map_or("", String::as_str);

    (0..width)
        .filter(move |&column| value(old, column) != value(new, column))
        .map(move |column| (column, value(new, column).to_string()))
}

// -- The diff --------------------------------------------------------------------

/// A number for each item of `old` and of `new`, the same for two items
/// exactly when they are equal.
fn intern<T: Eq + Hash + Copy>(old: &[T], new: &[T]) -> (Vec<u32>, Vec<u32>) {
    let mut numbers: HashMap<T, u32> = HashMap::new();

    let mut number_of = |item: T| -> u32 {
        let next = u32::try_from(numbers.len()).unwrap_or(u32::MAX);
        *numbers.entry(item).or_insert(next)
    };

    let old_ids = old.iter().map(|item| number_of(*item)).collect();
    let new_ids = new.iter().map(|item| number_of(*item)).collect();

    (old_ids, new_ids)
}

/// The records of the rendered and the edited text as numbers: by their
/// exact text, and by their fields.
struct Records<'a> {
    old_texts: &'a [u32],
    new_texts: &'a [u32],
    old_fields: &'a [u32],
    new_fields: &'a [u32],
}

/// One step of an edit script from `old` to `new`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Step {
    Keep(usize, usize),
    Remove(usize),
    Add(usize),
}

/// Pairs the rendered records with the edited ones, in three passes:
///
/// 1. The records an edit script over their exact text, without the line
///    terminator, keeps are the same: a line the user did not touch stays on
///    its row, bytes included.
/// 2. In each run of removed and added records between two kept ones, the
///    records a shortest edit script over their fields keeps are paired as
///    changed records whose fields did not change: a line whose quoting or
///    line break alone changed stays on its row.
/// 3. What is left of each run is paired in order as changed records, and
///    the rest are removed or added.
///
/// Listed in the order of the edited records, with each removed record
/// among them.
fn pair_rows(records: &Records<'_>) -> Vec<Pairing> {
    let mut pairings = Vec::with_capacity(records.old_texts.len().max(records.new_texts.len()));

    let mut removed = Vec::new();
    let mut added = Vec::new();

    for step in edit_script(records.old_texts, records.new_texts) {
        match step {
            Step::Remove(old) => removed.push(old),
            Step::Add(new) => added.push(new),

            Step::Keep(old, new) => {
                pair_gap(records, &mut pairings, &mut removed, &mut added);
                pairings.push(Pairing::Same(old, new));
            }
        }
    }

    pair_gap(records, &mut pairings, &mut removed, &mut added);

    pairings
}

/// Pairs a run of `removed` and `added` records between two records kept
/// with their text: first those a shortest edit script over their fields
/// keeps, then the rest in order. Empties both.
fn pair_gap(
    records: &Records<'_>,
    pairings: &mut Vec<Pairing>,
    removed: &mut Vec<usize>,
    added: &mut Vec<usize>,
) {
    let field_of = |ids: &[u32], indices: &[usize]| -> Vec<u32> {
        indices
            .iter()
            .map(|index| ids.get(*index).copied().unwrap_or_default())
            .collect()
    };

    let old_fields = field_of(records.old_fields, removed);
    let new_fields = field_of(records.new_fields, added);

    let mut left_removed = Vec::new();
    let mut left_added = Vec::new();

    for step in edit_script(&old_fields, &new_fields) {
        match step {
            Step::Remove(index) => left_removed.extend(removed.get(index)),
            Step::Add(index) => left_added.extend(added.get(index)),

            Step::Keep(old, new) => {
                pair_run(pairings, &mut left_removed, &mut left_added);

                if let (Some(old), Some(new)) = (removed.get(old), added.get(new)) {
                    pairings.push(Pairing::Changed(*old, *new));
                }
            }
        }
    }

    pair_run(pairings, &mut left_removed, &mut left_added);

    removed.clear();
    added.clear();
}

/// Pairs a run of `removed` and `added` records in order and empties both.
fn pair_run(pairings: &mut Vec<Pairing>, removed: &mut Vec<usize>, added: &mut Vec<usize>) {
    let paired = removed.len().min(added.len());

    pairings.extend(
        removed
            .iter()
            .zip(added.iter())
            .map(|(old, new)| Pairing::Changed(*old, *new)),
    );
    pairings.extend(
        removed
            .iter()
            .skip(paired)
            .map(|old| Pairing::Removed(*old)),
    );
    pairings.extend(added.iter().skip(paired).map(|new| Pairing::Added(*new)));

    removed.clear();
    added.clear();
}

/// How many times the anchoring on unique records splits a section before
/// the rest is left to Myers' search. Each level costs a pass over the
/// records, and a level that finds no anchor ends it anyway.
const ANCHOR_DEPTH: usize = 16;

/// An edit script from `old` to `new`, in order: a patience diff. The records
/// that appear exactly once in `old` and once in `new` are anchors, and the
/// longest run of them in the same order in both is kept. The sections
/// between anchors are split the same way, and what has no anchor left is
/// searched with Myers' algorithm, which finds a shortest script there.
///
/// Keeping unique records first means a line that appears once is not
/// traded for copies of a line that appears more often, which a shortest
/// script over the whole sequence may do. Within each searched section the
/// script is a shortest one, so a run of removed and added records between
/// two kept ones never holds two equal records.
pub(super) fn edit_script(old: &[u32], new: &[u32]) -> Vec<Step> {
    let max_distance = (old.len() + new.len()).div_ceil(2) + 1;

    let mut search = Search {
        old,
        new,
        forward: Diagonals::new(max_distance),
        backward: Diagonals::new(max_distance),
        steps: Vec::with_capacity(old.len().max(new.len())),
    };

    search.anchored(0..old.len(), 0..new.len(), ANCHOR_DEPTH);
    search.steps
}

/// A shortest edit script from `old` to `new`, by Myers' search alone,
/// without anchoring on unique records.
#[cfg(test)]
pub(super) fn shortest_edit_script(old: &[u32], new: &[u32]) -> Vec<Step> {
    let max_distance = (old.len() + new.len()).div_ceil(2) + 1;

    let mut search = Search {
        old,
        new,
        forward: Diagonals::new(max_distance),
        backward: Diagonals::new(max_distance),
        steps: Vec::with_capacity(old.len().max(new.len())),
    };

    search.conquer(0..old.len(), 0..new.len());
    search.steps
}

/// The records that appear exactly once in `old` and once in `new`, as
/// pairs of their indices, the longest run of them that is in the same order
/// in both, ordered.
fn unique_anchors(old: &[u32], new: &[u32]) -> Vec<(usize, usize)> {
    // For each record: how often it is in `old`, where, and the same in `new`.
    let mut seen: HashMap<u32, (usize, usize, usize, usize)> = HashMap::new();

    for (index, record) in old.iter().enumerate() {
        let entry = seen.entry(*record).or_insert((0, index, 0, 0));
        entry.0 += 1;
    }

    for (index, record) in new.iter().enumerate() {
        if let Some(entry) = seen.get_mut(record) {
            entry.2 += 1;
            entry.3 = index;
        }
    }

    let mut pairs: Vec<(usize, usize)> = seen
        .values()
        .filter(|(old_count, _, new_count, _)| *old_count == 1 && *new_count == 1)
        .map(|(_, old_index, _, new_index)| (*old_index, *new_index))
        .collect();

    pairs.sort_unstable_by_key(|(_, new_index)| *new_index);

    longest_increasing_run(&pairs)
}

/// The longest subsequence of `pairs`, which are ordered by their second
/// index, whose first indices increase too: patience sorting, with a link
/// from each pair to the pair before it in the run that ends with it.
fn longest_increasing_run(pairs: &[(usize, usize)]) -> Vec<(usize, usize)> {
    // `tops[k]` is the pair, by index, that ends the run of length `k + 1`
    // whose last first index is the smallest.
    let mut tops: Vec<usize> = Vec::new();
    let mut previous: Vec<Option<usize>> = Vec::with_capacity(pairs.len());

    for (index, (old_index, _)) in pairs.iter().enumerate() {
        let length = tops.partition_point(|top| {
            pairs
                .get(*top)
                .is_some_and(|(top_old, _)| top_old < old_index)
        });

        previous.push(
            length
                .checked_sub(1)
                .and_then(|before| tops.get(before).copied()),
        );

        match tops.get_mut(length) {
            Some(top) => *top = index,
            None => tops.push(index),
        }
    }

    let mut run = Vec::with_capacity(tops.len());
    let mut current = tops.last().copied();

    while let Some(index) = current {
        run.extend(pairs.get(index).copied());
        current = previous.get(index).copied().flatten();
    }

    run.reverse();
    run
}

/// The furthest point reached on each diagonal `k`, by `k` from
/// `-max - 1` to `max + 1`.
struct Diagonals {
    offset: isize,
    furthest: Vec<isize>,
}

impl Diagonals {
    fn new(max_distance: usize) -> Self {
        let offset = isize::try_from(max_distance).unwrap_or(isize::MAX - 1) + 1;

        Self {
            offset,
            furthest: vec![0; max_distance * 2 + 3],
        }
    }

    fn get(&self, diagonal: isize) -> isize {
        usize::try_from(diagonal + self.offset)
            .ok()
            .and_then(|index| self.furthest.get(index))
            .copied()
            .unwrap_or_default()
    }

    fn set(&mut self, diagonal: isize, value: isize) {
        let slot = usize::try_from(diagonal + self.offset)
            .ok()
            .and_then(|index| self.furthest.get_mut(index));

        if let Some(slot) = slot {
            *slot = value;
        }
    }
}

/// Myers' linear-space search for a shortest edit script: the records both
/// ranges start and end with alike are kept, and what is left is split at a
/// middle snake, the overlap of a forward and a backward search, and each
/// half is searched the same way.
struct Search<'a> {
    old: &'a [u32],
    new: &'a [u32],
    forward: Diagonals,
    backward: Diagonals,
    steps: Vec<Step>,
}

impl Search<'_> {
    /// Splits the two ranges at the unique records they share, up to `depth`
    /// times, and searches what has no anchor with [`Search::conquer`].
    fn anchored(&mut self, mut old: Range<usize>, mut new: Range<usize>, depth: usize) {
        let prefix = self.common_prefix(old.clone(), new.clone());

        self.steps
            .extend((0..prefix).map(|index| Step::Keep(old.start + index, new.start + index)));
        old.start += prefix;
        new.start += prefix;

        let suffix = self.common_suffix(old.clone(), new.clone());
        old.end -= suffix;
        new.end -= suffix;

        let anchors = if depth == 0 || old.is_empty() || new.is_empty() {
            Vec::new()
        } else {
            unique_anchors(
                self.old.get(old.clone()).unwrap_or_default(),
                self.new.get(new.clone()).unwrap_or_default(),
            )
        };

        if anchors.is_empty() {
            self.conquer(old.clone(), new.clone());
        } else {
            let (mut old_start, mut new_start) = (old.start, new.start);

            for (old_anchor, new_anchor) in anchors {
                let (old_anchor, new_anchor) = (old.start + old_anchor, new.start + new_anchor);

                self.anchored(old_start..old_anchor, new_start..new_anchor, depth - 1);
                self.steps.push(Step::Keep(old_anchor, new_anchor));

                old_start = old_anchor + 1;
                new_start = new_anchor + 1;
            }

            self.anchored(old_start..old.end, new_start..new.end, depth - 1);
        }

        self.steps
            .extend((0..suffix).map(|index| Step::Keep(old.end + index, new.end + index)));
    }

    fn conquer(&mut self, mut old: Range<usize>, mut new: Range<usize>) {
        let prefix = self.common_prefix(old.clone(), new.clone());

        self.steps
            .extend((0..prefix).map(|index| Step::Keep(old.start + index, new.start + index)));
        old.start += prefix;
        new.start += prefix;

        let suffix = self.common_suffix(old.clone(), new.clone());
        old.end -= suffix;
        new.end -= suffix;

        if old.is_empty() {
            self.steps.extend(new.clone().map(Step::Add));
        } else if new.is_empty() {
            self.steps.extend(old.clone().map(Step::Remove));
        } else {
            match self.middle_snake(old.clone(), new.clone()) {
                Some((x, y))
                    if (x, y) != (old.start, new.start) && (x, y) != (old.end, new.end) =>
                {
                    self.conquer(old.start..x, new.start..y);
                    self.conquer(x..old.end, y..new.end);
                }

                // A middle snake always splits two ranges that differ at
                // both ends, so this is never reached.
                _ => {
                    self.steps.extend(old.clone().map(Step::Remove));
                    self.steps.extend(new.clone().map(Step::Add));
                }
            }
        }

        self.steps
            .extend((0..suffix).map(|index| Step::Keep(old.end + index, new.end + index)));
    }

    /// How many records `old` and `new` start with alike.
    fn common_prefix(&self, old: Range<usize>, new: Range<usize>) -> usize {
        let old = self.old.get(old).unwrap_or_default();
        let new = self.new.get(new).unwrap_or_default();

        old.iter()
            .zip(new)
            .take_while(|(old, new)| old == new)
            .count()
    }

    /// How many records `old` and `new` end with alike.
    fn common_suffix(&self, old: Range<usize>, new: Range<usize>) -> usize {
        let old = self.old.get(old).unwrap_or_default();
        let new = self.new.get(new).unwrap_or_default();

        old.iter()
            .rev()
            .zip(new.iter().rev())
            .take_while(|(old, new)| old == new)
            .count()
    }

    /// Where a shortest path from the start of the two ranges to their end
    /// crosses its middle, found by searching from both ends at once.
    fn middle_snake(&mut self, old: Range<usize>, new: Range<usize>) -> Option<(usize, usize)> {
        let n = isize::try_from(old.len()).ok()?;
        let m = isize::try_from(new.len()).ok()?;
        let old_start = isize::try_from(old.start).ok()?;
        let new_start = isize::try_from(new.start).ok()?;

        let delta = n - m;
        let odd = delta & 1 == 1;
        let max_distance = (n + m + 1) / 2 + 1;

        let to_point = |x: isize, y: isize| {
            Some((
                usize::try_from(old_start + x).ok()?,
                usize::try_from(new_start + y).ok()?,
            ))
        };

        self.forward.set(1, 0);
        self.backward.set(1, 0);

        for distance in 0..max_distance {
            for diagonal in (-distance..=distance).rev().step_by(2) {
                let mut x = if diagonal == -distance
                    || (diagonal != distance
                        && self.forward.get(diagonal - 1) < self.forward.get(diagonal + 1))
                {
                    self.forward.get(diagonal + 1)
                } else {
                    self.forward.get(diagonal - 1) + 1
                };
                let y = x - diagonal;
                let (start_x, start_y) = (x, y);

                if (0..n).contains(&x) && (0..m).contains(&y) {
                    let advance = self.common_prefix(to_range(&old, x, n)?, to_range(&new, y, m)?);
                    x += isize::try_from(advance).ok()?;
                }

                self.forward.set(diagonal, x);

                if odd
                    && (diagonal - delta).abs() < distance
                    && x + self.backward.get(delta - diagonal) >= n
                {
                    return to_point(start_x, start_y);
                }
            }

            for diagonal in (-distance..=distance).rev().step_by(2) {
                let mut x = if diagonal == -distance
                    || (diagonal != distance
                        && self.backward.get(diagonal - 1) < self.backward.get(diagonal + 1))
                {
                    self.backward.get(diagonal + 1)
                } else {
                    self.backward.get(diagonal - 1) + 1
                };
                let mut y = x - diagonal;

                if (0..n).contains(&x) && (0..m).contains(&y) {
                    let advance =
                        self.common_suffix(to_range(&old, 0, n - x)?, to_range(&new, 0, m - y)?);
                    x += isize::try_from(advance).ok()?;
                    y += isize::try_from(advance).ok()?;
                }

                self.backward.set(diagonal, x);

                if !odd
                    && (diagonal - delta).abs() <= distance
                    && x + self.forward.get(delta - diagonal) >= n
                {
                    return to_point(n - x, m - y);
                }
            }
        }

        None
    }
}

/// The part of `range` from `start` to `end`, counted from its start.
fn to_range(range: &Range<usize>, start: isize, end: isize) -> Option<Range<usize>> {
    let start = usize::try_from(start).ok()?;
    let end = usize::try_from(end).ok()?;

    Some(range.start + start..range.start + end)
}

// -- What the pending state becomes ------------------------------------------------

/// Makes `edits` on `model` and `buffer` as the document's applier makes
/// them on the table, without GPUI, so that what the pending state becomes
/// can be checked before anything is applied: a cell set to the value its
/// record holds drops its staged value, as the table stages a value. The
/// page model is copied only when the edits change its columns, because a
/// copy holds every loaded record.
///
/// # Errors
///
/// A column change the page model refuses.
pub(super) fn apply_to_pending(
    edits: &TextEdits,
    model: &mut Cow<'_, PageModel>,
    buffer: &mut EditBuffer,
) -> Result<(), PageModelError> {
    if !edits.renames.is_empty() || !edits.appended_columns.is_empty() {
        let model = model.to_mut();

        for (column, name) in &edits.renames {
            model.rename_column(*column, name.clone())?;
        }

        for name in &edits.appended_columns {
            model.append_column(name.clone())?;
        }

        pad_pending_inserts(buffer, model.column_count());
    }

    let model: &PageModel = model;
    let column_count = model.column_count();

    for (row, column, value) in &edits.base_cells {
        let own = model
            .records()
            .get(*row)
            .and_then(|record| record.fields.get(*column))
            .map_or("", String::as_str);

        if own == value {
            buffer.clear_cell(*row, *column);
        } else {
            buffer.set_cell(*row, *column, CellValue::text(value));
        }
    }

    for (insert, column, value) in &edits.insert_cells {
        buffer.set_insert_cell(*insert, *column, CellValue::text(value));
    }

    for row in &edits.deleted_rows {
        buffer.mark_for_delete(*row);
    }

    change_inserts(edits, buffer, column_count);

    Ok(())
}

/// Removes and adds the pending inserts of `edits` in `buffer`, as both
/// appliers do: the removed ones from the last, then each new one at its
/// anchor, before the kept insert the text puts after it, or after the
/// inserts of its anchor, padded to `column_count` fields. A new row placed
/// before a kept insert takes that insert's place in the list, which moves
/// the kept insert and the ones after it up by one: no kept insert is
/// removed or changed, so no step of the undo history touches a row the
/// user did not edit.
pub(super) fn change_inserts(edits: &TextEdits, buffer: &mut EditBuffer, column_count: usize) {
    // Which insert, by its index before any change, is at each place now.
    let mut original: Vec<Option<usize>> = (0..buffer.pending_inserts().len()).map(Some).collect();

    let mut removed = edits.removed_inserts.clone();
    removed.sort_unstable();
    removed.dedup();

    for insert in removed.into_iter().rev() {
        buffer.remove_pending_insert_by_idx(insert);

        if insert < original.len() {
            original.remove(insert);
        }
    }

    for added in &edits.added_inserts {
        let mut row: Vec<CellValue> = added
            .fields
            .iter()
            .map(|field| CellValue::text(field))
            .collect();

        if row.len() < column_count {
            row.resize(column_count, CellValue::text(""));
        }

        let place = added
            .before
            .and_then(|before| original.iter().position(|insert| *insert == Some(before)));

        match place {
            Some(place) => {
                buffer.add_pending_insert_at_index(place, added.anchor, row);
                original.insert(place, None);
            }

            None => {
                buffer.add_pending_insert_at(added.anchor, row);
                original.push(None);
            }
        }
    }
}
