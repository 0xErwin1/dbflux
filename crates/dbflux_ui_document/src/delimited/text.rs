//! The text of the loaded records, as the text view shows it. Pure: nothing
//! here reads a file or touches GPUI state.
//!
//! **Raw** is the exact text of the loaded part of the file as a save would
//! write it. The header and every record that is not edited come from their
//! own bytes in the source, decoded under the dialect's encoding, line
//! terminators included. Replaced and inserted records, deleted records, and
//! appended or renamed columns come from the pending state: the page model
//! turns it into an edit set, as for a save, and each edited record is
//! rendered by the writer's own rules (`dbflux_delimited::render_record`).
//! The terminators of inserted records, the terminator a final record gains
//! when a record is inserted after it, and the quoted empty record that keeps
//! an empty line from fusing with a bare carriage return follow the writer's
//! rules too. A byte-order mark is not part of the text. A row inserted after
//! the last loaded record of a file that is not fully loaded, which a save
//! refuses, is shown at the end of the loaded text.
//!
//! **Aligned** shows the same rows, one per line, with every field padded to
//! the width of its column and the columns separated by ` | `. The first line
//! holds the column names as the table shows them. A short record is padded
//! with empty cells, as the table pads it. A line break inside a value is
//! shown as `↵` and a tab as `⇥`, so that every record stays on one line, and
//! a value longer than [`ALIGNED_MAX_WIDTH`] characters is cut, ending with
//! `…`. Widths count characters, so a wide character takes one column.
//!
//! Both leave deleted rows out, as a save does, and keep for every row they
//! show its identity in the table and its place in the text, which is what a
//! text edit is mapped back through.
//!
//! The text stops at a record boundary once it would pass its limits, and
//! the raw text also stops at the first record whose bytes were not kept.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::Range;

use dbflux_components::components::data_table::model::{CellValue, EditBuffer, VisualRowSource};
use dbflux_delimited::{
    AppendedColumn, ByteSource, Dialect, EditLocation, EditSet, InsertPosition, Record,
    SourceError, WriteError, render_appended_fields, render_record, write_edited,
};

use super::page_model::{PageModel, PageModelError};

/// How many characters of a value the aligned text shows. A longer value is
/// cut to one less and ends with `…`.
pub(super) const ALIGNED_MAX_WIDTH: usize = 40;

/// A line longer than this many bytes makes the text view wrap its lines,
/// as the object editors do for a body with such a line: without wrapping,
/// clicking into it and moving along it is unusable.
pub(super) const WRAP_LINE_BYTES: usize = 10_000;

/// The most bytes of text the text view shows, and the most bytes of the
/// file the document keeps for it.
pub(super) const MAX_TEXT_BYTES: usize = 8 * 1024 * 1024;

/// The most lines the text view shows: the editor component is built for
/// about this many.
pub(super) const MAX_TEXT_LINES: usize = 50_000;

/// What the text holds at most. Both are checked before a row is added, so
/// the text always ends at a record boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TextLimits {
    pub(super) max_bytes: usize,
    pub(super) max_lines: usize,
}

/// The limits of the text view.
pub(super) const TEXT_LIMITS: TextLimits = TextLimits {
    max_bytes: MAX_TEXT_BYTES,
    max_lines: MAX_TEXT_LINES,
};

/// The bytes of the loaded part of a file from its first record on (the
/// header included), kept for the raw text as far as they were read.
///
/// The bytes are kept while they follow each other. A page whose bytes were
/// not kept, or bytes that do not start where the kept ones end, close the
/// span: nothing after them is kept, so the raw text never skips a record.
#[derive(Debug, Clone, Default)]
pub(super) struct SourceSpan {
    start: u64,
    bytes: Vec<u8>,
    closed: bool,
}

impl SourceSpan {
    /// The span of `bytes`, which start at offset `start` of the source.
    pub(super) fn new(start: u64, bytes: Vec<u8>) -> Self {
        Self {
            start,
            bytes,
            closed: false,
        }
    }

    /// The offset of the first kept byte, which is where the records start.
    pub(super) fn start(&self) -> u64 {
        self.start
    }

    /// The offset after the last kept byte.
    pub(super) fn end(&self) -> u64 {
        self.start + self.bytes.len() as u64
    }

    /// How many more bytes of the records that follow should be kept so
    /// that the span holds at most `limit`: none once it is closed.
    pub(super) fn budget(&self, limit: usize) -> usize {
        if self.closed {
            return 0;
        }

        limit.saturating_sub(self.bytes.len())
    }

    /// Keeps nothing more.
    pub(super) fn close(&mut self) {
        self.closed = true;
    }

    /// Adds the bytes of the next records, read from offset `start`. `None`
    /// is a page whose bytes were not kept. Either that or bytes that do not
    /// start at [`SourceSpan::end`] close the span.
    pub(super) fn extend(&mut self, start: u64, bytes: Option<Vec<u8>>) {
        if self.closed {
            return;
        }

        match bytes {
            Some(bytes) if start == self.end() => self.bytes.extend_from_slice(&bytes),
            _ => self.closed = true,
        }
    }

    /// Adds `bytes`, the kept bytes of the leading `records` of a page that
    /// was just loaded. The span closes unless they are the bytes of every
    /// one of them. A page without records changes nothing.
    pub(super) fn extend_with_records(&mut self, records: &[Record], bytes: Option<Vec<u8>>) {
        let (Some(first), Some(last)) = (records.first(), records.last()) else {
            return;
        };

        let start = first.byte_range.start;
        let is_whole = bytes
            .as_ref()
            .is_some_and(|bytes| start + bytes.len() as u64 == last.byte_range.end);

        self.extend(start, bytes);

        if !is_whole {
            self.close();
        }
    }

    /// The bytes of the record at `range`, or of any other range of the
    /// source, when all of them were kept.
    pub(super) fn record(&self, range: &Range<u64>) -> Option<&[u8]> {
        let start = usize::try_from(range.start.checked_sub(self.start)?).ok()?;
        let end = usize::try_from(range.end.checked_sub(self.start)?).ok()?;

        self.bytes.get(start..end)
    }
}

/// The row of the table a record of the text belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TextRow {
    /// The header record in the raw text, and the line of column names in
    /// the aligned text.
    Header,

    /// A loaded record, by its index among the records.
    Base(usize),

    /// A pending insert, by its index in the edit buffer.
    Insert(usize),
}

/// One row of the text: which row of the table it is, and where it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TextRecord {
    pub(super) row: TextRow,

    /// Its bytes in the text, its terminator included.
    pub(super) text: Range<usize>,

    /// The lines of the text it is on, counted by line feeds as the editor
    /// counts them. A record that ends in a line feed ends before the line
    /// after it.
    ///
    /// Not unique: the editor keeps a bare carriage return as a character,
    /// so in text whose records end with one, several records are on the
    /// same line and their ranges overlap. `text` is what identifies a
    /// record's place.
    pub(super) lines: Range<usize>,
}

/// How the text ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TextEnd {
    /// Every row of the file is in the text.
    Complete,

    /// Every loaded row is in the text, and the file has records that are
    /// not loaded.
    MoreInFile,

    /// The text stopped before the last loaded row, at a limit or at a
    /// record whose bytes were not kept. `shown` data rows are in it.
    Capped { shown: usize },
}

/// The text of the loaded records and where each row of it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RenderedText {
    pub(super) text: String,
    pub(super) layout: TextLayout,
}

/// Where each row is in a rendered text, and how the text ends. Kept apart
/// from the text, which the text view hands to the editor, so that the
/// document holds the text once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TextLayout {
    /// The rows of the text in order. Their `text` ranges tile it: every
    /// byte of the text belongs to exactly one of them.
    pub(super) records: Vec<TextRecord>,
    pub(super) end: TextEnd,

    /// Whether a line is longer than [`WRAP_LINE_BYTES`].
    pub(super) wraps: bool,
}

impl TextLayout {
    /// The first line of `row`, when it is in the text. Lines are counted by
    /// line feeds, so in text whose records end with a bare carriage return
    /// several rows share a line: map by [`TextRecord::text`] there.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "a seam for mapping text edits back to rows")
    )]
    pub(super) fn line_of(&self, row: TextRow) -> Option<usize> {
        self.records
            .iter()
            .find(|record| record.row == row)
            .map(|record| record.lines.start)
    }

    /// The row whose text holds byte `offset`, and how far into that text
    /// it is. The end of the text belongs to the last row.
    pub(super) fn row_at(&self, offset: usize) -> Option<(TextRow, usize)> {
        let record = self
            .records
            .iter()
            .find(|record| record.text.contains(&offset))
            .or_else(|| self.records.last().filter(|last| last.text.end == offset))?;

        Some((record.row, offset - record.text.start))
    }

    /// The byte offset `within` bytes into the text of `row`, kept inside
    /// it, when `row` is in the text.
    pub(super) fn offset_in(&self, row: TextRow, within: usize) -> Option<usize> {
        let record = self.records.iter().find(|record| record.row == row)?;
        let last = record.text.end.saturating_sub(1).max(record.text.start);

        Some(record.text.start.saturating_add(within).min(last))
    }
}

/// Why the raw text could not be rendered: the same reasons a save of the
/// pending changes would give.
#[derive(Debug)]
pub(super) enum RawTextError {
    PageModel(PageModelError),
    Write(WriteError),
}

impl fmt::Display for RawTextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PageModel(error) => fmt::Display::fmt(error, formatter),
            Self::Write(error) => fmt::Display::fmt(error, formatter),
        }
    }
}

impl From<PageModelError> for RawTextError {
    fn from(error: PageModelError) -> Self {
        Self::PageModel(error)
    }
}

impl From<WriteError> for RawTextError {
    fn from(error: WriteError) -> Self {
        Self::Write(error)
    }
}

/// Collects the text row by row, with each row's place in it.
struct TextBuilder {
    text: String,
    records: Vec<TextRecord>,
    line_feeds: usize,
    limits: TextLimits,
    shown: usize,
}

impl TextBuilder {
    fn new(limits: TextLimits) -> Self {
        Self {
            text: String::new(),
            records: Vec::new(),
            line_feeds: 0,
            limits,
            shown: 0,
        }
    }

    /// Whether `piece` fits in the limits after the text so far.
    fn fits(&self, piece: &str) -> bool {
        let bytes = self.text.len().saturating_add(piece.len());
        let lines = self
            .line_feeds
            .saturating_add(line_feeds(piece))
            .saturating_add(usize::from(!piece.ends_with('\n') && !piece.is_empty()));

        bytes <= self.limits.max_bytes && lines <= self.limits.max_lines
    }

    /// Adds `piece` as the text of `row`. A data row counts as shown.
    fn push(&mut self, row: TextRow, piece: &str) {
        let start = self.text.len();
        let first_line = self.line_feeds;

        self.text.push_str(piece);
        self.line_feeds += line_feeds(piece);

        let last_line = self.line_feeds + usize::from(!piece.ends_with('\n'));

        self.records.push(TextRecord {
            row,
            text: start..self.text.len(),
            lines: first_line..last_line.max(first_line + 1),
        });

        if row != TextRow::Header {
            self.shown += 1;
        }
    }

    /// Adds `piece` as the text of data row `row` when it fits. Returns
    /// whether it did.
    fn push_row(&mut self, row: TextRow, piece: &str) -> bool {
        if !self.fits(piece) {
            return false;
        }

        self.push(row, piece);
        true
    }

    fn finish(self, end: TextEnd) -> RenderedText {
        let end = match end {
            TextEnd::Capped { .. } => TextEnd::Capped { shown: self.shown },
            other => other,
        };

        let wraps = self
            .text
            .split('\n')
            .any(|line| line.len() > WRAP_LINE_BYTES);

        RenderedText {
            text: self.text,
            layout: TextLayout {
                records: self.records,
                end,
                wraps,
            },
        }
    }
}

fn line_feeds(text: &str) -> usize {
    text.bytes().filter(|byte| *byte == b'\n').count()
}

/// How the text of a fully shown page model ends.
fn complete_end(model: &PageModel) -> TextEnd {
    if model.is_fully_loaded() {
        TextEnd::Complete
    } else {
        TextEnd::MoreInFile
    }
}

// -- Raw ---------------------------------------------------------------------

/// The pending changes of an edit set, by the start of the record they name.
pub(super) struct RawPlan<'a> {
    replacements: HashMap<u64, &'a [String]>,
    deletions: HashSet<u64>,

    /// The inserts placed before each record, as pending insert index and
    /// fields, in the order they are written.
    inserted_before: HashMap<u64, Vec<(usize, &'a [String])>>,
    inserted_at_end: Vec<(usize, &'a [String])>,

    /// The fields appended columns add, `None` when no column is appended.
    appended: Option<AppendedValues>,
}

/// What appended columns add: the names to the header, and to each copied
/// record its own values or the defaults.
struct AppendedValues {
    header: Vec<String>,
    defaults: Vec<String>,
    by_record: HashMap<u64, Vec<String>>,
}

impl<'a> RawPlan<'a> {
    /// The fields the raw text shows for `record`, the header when
    /// `is_header`: the fields it is replaced with, or its own followed by
    /// those of the appended columns. These are the fields the text of the
    /// record reads as, so nothing needs to read the text again for them.
    pub(super) fn shown_fields(&self, record: &'a Record, is_header: bool) -> Cow<'a, [String]> {
        if let Some(fields) = self.replacements.get(&record.byte_range.start) {
            return Cow::Borrowed(fields);
        }

        let Some(appended) = &self.appended else {
            return Cow::Borrowed(&record.fields);
        };

        let values = match appended.by_record.get(&record.byte_range.start) {
            Some(values) => values,
            None if is_header => &appended.header,
            None => &appended.defaults,
        };

        Cow::Owned(record.fields.iter().chain(values).cloned().collect())
    }

    pub(super) fn new(edit_set: &'a EditSet) -> Self {
        let replacements = edit_set
            .replacements
            .iter()
            .map(|replacement| (replacement.byte_range.start, replacement.fields.as_slice()))
            .collect();

        let deletions = edit_set.deletions.iter().map(|range| range.start).collect();

        let mut inserted_before: HashMap<u64, Vec<(usize, &[String])>> = HashMap::new();
        let mut inserted_at_end = Vec::new();

        for (index, insertion) in edit_set.insertions.iter().enumerate() {
            let insert = (index, insertion.fields.as_slice());

            match &insertion.position {
                InsertPosition::Before(range) => {
                    inserted_before.entry(range.start).or_default().push(insert);
                }

                InsertPosition::End => inserted_at_end.push(insert),
            }
        }

        let appended = (!edit_set.appended_columns.is_empty()).then(|| {
            let columns = &edit_set.appended_columns;
            let defaults: Vec<String> = columns
                .iter()
                .map(|column| column.default_value.clone())
                .collect();

            let mut by_record: HashMap<u64, Vec<String>> = HashMap::new();

            for (index, column) in columns.iter().enumerate() {
                for (range, value) in &column.values {
                    let values = by_record
                        .entry(range.start)
                        .or_insert_with(|| defaults.clone());

                    if let Some(slot) = values.get_mut(index) {
                        slot.clone_from(value);
                    }
                }
            }

            AppendedValues {
                header: columns.iter().map(|column| column.header.clone()).collect(),
                defaults,
                by_record,
            }
        });

        Self {
            replacements,
            deletions,
            inserted_before,
            inserted_at_end,
            appended,
        }
    }
}

/// Renders the raw text of the loaded records with the pending changes of
/// `edits`, the edit buffer of the table built from `model`. `span` holds
/// the kept bytes of the file and `dialect` is the one the records were read
/// with. See the module documentation.
///
/// # Errors
///
/// What a save of the same changes would refuse before writing: a value the
/// encoding cannot represent, or a field the dialect cannot quote.
pub(super) fn render_raw(
    model: &PageModel,
    span: &SourceSpan,
    edits: &EditBuffer,
    dialect: &Dialect,
    limits: TextLimits,
) -> Result<RenderedText, RawTextError> {
    let edit_set = model.loaded_edit_set(span.end(), edits)?;

    if !edit_set.is_empty() {
        check_save(model, span, &edit_set, dialect)?;
    }

    let plan = RawPlan::new(&edit_set);

    let mut raw = RawText {
        builder: TextBuilder::new(limits),
        dialect,
        plan: &plan,
    };

    let first_record = model.header().or(model.records().first());
    let fallback = first_record
        .and_then(|record| span.record(&record.byte_range))
        .map(|bytes| split_terminator(&decode_text(bytes, dialect)).1.to_string())
        .filter(|terminator| !terminator.is_empty())
        .unwrap_or_else(|| "\n".to_string());

    // Nothing is shown without the header that comes first in the file.
    if let Some(header) = model.header() {
        let Some(bytes) = span.record(&header.byte_range) else {
            return Ok(raw.builder.finish(TextEnd::Capped { shown: 0 }));
        };

        let text = raw.record_text(header, bytes, true)?;

        if !raw.builder.push_row(TextRow::Header, &text) {
            return Ok(raw.builder.finish(TextEnd::Capped { shown: 0 }));
        }
    }

    for (row, record) in model.records().iter().enumerate() {
        let Some(bytes) = span.record(&record.byte_range) else {
            return Ok(raw.builder.finish(TextEnd::Capped { shown: 0 }));
        };

        if !raw.push_inserted(record, bytes, &fallback)? {
            return Ok(raw.builder.finish(TextEnd::Capped { shown: 0 }));
        }

        if plan.deletions.contains(&record.byte_range.start) {
            continue;
        }

        let text = raw.record_text(record, bytes, false)?;

        if !raw.builder.push_row(TextRow::Base(row), &text) {
            return Ok(raw.builder.finish(TextEnd::Capped { shown: 0 }));
        }
    }

    if !raw.push_inserted_at_end(model, span, &fallback)? {
        return Ok(raw.builder.finish(TextEnd::Capped { shown: 0 }));
    }

    Ok(raw.builder.finish(complete_end(model)))
}

/// The raw text being built.
struct RawText<'a> {
    builder: TextBuilder,
    dialect: &'a Dialect,
    plan: &'a RawPlan<'a>,
}

impl RawText<'_> {
    /// The text of `record`, whose bytes in the source are `bytes`: rendered
    /// from its fields when it is replaced, its own text otherwise, with the
    /// fields of appended columns before its terminator.
    fn record_text(
        &self,
        record: &Record,
        bytes: &[u8],
        is_header: bool,
    ) -> Result<String, RawTextError> {
        let start = record.byte_range.start;
        let own_text = decode_text(bytes, self.dialect);
        let (content, terminator) = split_terminator(&own_text);

        if let Some(fields) = self.plan.replacements.get(&start) {
            let location = EditLocation::Existing(record.byte_range.clone());
            let rendered = render_record(fields, self.dialect, &location)?;

            return Ok(format!(
                "{}{terminator}",
                decode_text(&rendered, self.dialect)
            ));
        }

        let Some(appended) = &self.plan.appended else {
            return self.unfused(start, own_text).map_err(RawTextError::from);
        };

        let values = match appended.by_record.get(&start) {
            Some(values) => values,
            None if is_header => &appended.header,
            None => &appended.defaults,
        };

        let location = EditLocation::Existing(record.byte_range.clone());
        let fields = render_appended_fields(values, self.dialect, &location)?;

        Ok(format!(
            "{content}{}{terminator}",
            decode_text(&fields, self.dialect)
        ))
    }

    /// `text`, the own text of the record at `start`, with the quoted empty
    /// record the writer puts before it when the text so far ends with a
    /// carriage return and `text` starts with a line feed: the records
    /// between them were deleted, and the two would read as one terminator.
    fn unfused(&self, start: u64, text: String) -> Result<String, WriteError> {
        if !(self.builder.text.ends_with('\r') && text.starts_with('\n')) {
            return Ok(text);
        }

        let Some(quote) = self.dialect.quote.map(char::from) else {
            return Err(WriteError::FusedLineBreak { offset: start });
        };

        Ok(format!("{quote}{quote}{text}"))
    }

    /// Adds the rows inserted before `record`, each with the terminator of
    /// `record` or `fallback` when it has none. Returns false when one did
    /// not fit.
    fn push_inserted(
        &mut self,
        record: &Record,
        bytes: &[u8],
        fallback: &str,
    ) -> Result<bool, RawTextError> {
        let plan = self.plan;

        let Some(inserts) = plan.inserted_before.get(&record.byte_range.start) else {
            return Ok(true);
        };

        let own_text = decode_text(bytes, self.dialect);
        let terminator = match split_terminator(&own_text).1 {
            "" => fallback,
            terminator => terminator,
        };

        self.push_inserts(inserts, terminator)
    }

    /// Adds the rows inserted at the end. They take the terminator the
    /// loaded text ends with. When it ends without one, they take
    /// `fallback`, which the last record also gains unless it was deleted.
    fn push_inserted_at_end(
        &mut self,
        model: &PageModel,
        span: &SourceSpan,
        fallback: &str,
    ) -> Result<bool, RawTextError> {
        let plan = self.plan;

        if plan.inserted_at_end.is_empty() {
            return Ok(true);
        }

        let last_record = model.records().last().or(model.header());

        let last_text = last_record
            .and_then(|record| span.record(&record.byte_range))
            .map(|bytes| decode_text(bytes, self.dialect))
            .unwrap_or_default();

        let mut terminator = split_terminator(&last_text).1.to_string();

        if terminator.is_empty() {
            terminator = fallback.to_string();

            let last_is_deleted = model
                .records()
                .last()
                .is_some_and(|record| plan.deletions.contains(&record.byte_range.start));

            // The terminator goes on the last record the text shows, which
            // is the last record of the file: the text holds every loaded
            // record by now.
            if last_record.is_some() && !last_is_deleted && !self.builder.records.is_empty() {
                append_to_last(&mut self.builder, &terminator);
            }
        }

        self.push_inserts(&plan.inserted_at_end, &terminator)
    }

    fn push_inserts(
        &mut self,
        inserts: &[(usize, &[String])],
        terminator: &str,
    ) -> Result<bool, RawTextError> {
        for (index, fields) in inserts {
            let rendered = render_record(fields, self.dialect, &EditLocation::Inserted(*index))?;
            let text = format!("{}{terminator}", decode_text(&rendered, self.dialect));

            if !self.builder.push_row(TextRow::Insert(*index), &text) {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

/// Adds `terminator` to the text and to the last record of it, which ended
/// without one.
fn append_to_last(builder: &mut TextBuilder, terminator: &str) {
    builder.text.push_str(terminator);
    builder.line_feeds += line_feeds(terminator);

    if let Some(last) = builder.records.last_mut() {
        last.text.end = builder.text.len();
        last.lines.end = last.lines.end.max(builder.line_feeds);
    }
}

/// Checks that a save of the pending changes of `edits`, the edit buffer of
/// the table built from `model`, would be written, as [`render_raw`] checks
/// before it renders: the same refusals, without rendering the text.
///
/// # Errors
///
/// What [`render_raw`] returns for the same pending changes.
pub(super) fn check_pending(
    model: &PageModel,
    span: &SourceSpan,
    edits: &EditBuffer,
    dialect: &Dialect,
) -> Result<(), RawTextError> {
    let edit_set = model.loaded_edit_set(span.end(), edits)?;

    if !edit_set.is_empty() {
        check_save(model, span, &edit_set, dialect)?;
    }

    Ok(())
}

/// Runs the writer over the kept bytes with `edit_set`, the loaded edit set
/// of `model`, so that the raw text never shows changes a save of them would
/// refuse: whatever the writer refuses, before or while writing, is returned.
///
/// Only the edits of records whose bytes were kept take part, and the edits
/// at the end only when every loaded record was kept.
fn check_save(
    model: &PageModel,
    span: &SourceSpan,
    edit_set: &EditSet,
    dialect: &Dialect,
) -> Result<(), WriteError> {
    let loaded_end = model
        .records()
        .last()
        .or(model.header())
        .map_or(span.start(), |record| record.byte_range.end);

    let source = SpanSource {
        mark: byte_order_mark(dialect, span.start()),
        span,
    };

    let edits = edits_within(edit_set, span.end(), span.end() >= loaded_end);
    let window = std::num::NonZeroU64::new(1024 * 1024).unwrap_or(std::num::NonZeroU64::MIN);

    write_edited(&source, dialect, &edits, window, &mut std::io::sink())
}

/// The edits of `edit_set` that name records ending at or before `end`, and
/// its edits at the end when `with_end` is set. The source length is `end`.
fn edits_within(edit_set: &EditSet, end: u64, with_end: bool) -> EditSet {
    let inside = |range: &Range<u64>| range.end <= end;

    EditSet {
        source_length: end,
        replacements: edit_set
            .replacements
            .iter()
            .filter(|replacement| inside(&replacement.byte_range))
            .cloned()
            .collect(),
        deletions: edit_set
            .deletions
            .iter()
            .filter(|range| inside(range))
            .cloned()
            .collect(),
        insertions: edit_set
            .insertions
            .iter()
            .filter(|insertion| match &insertion.position {
                InsertPosition::Before(range) => inside(range),
                InsertPosition::End => with_end,
            })
            .cloned()
            .collect(),
        appended_columns: edit_set
            .appended_columns
            .iter()
            .map(|column| AppendedColumn {
                values: column
                    .values
                    .iter()
                    .filter(|(range, _)| inside(range))
                    .cloned()
                    .collect(),
                ..column.clone()
            })
            .collect(),
    }
}

/// The byte-order mark the kept bytes started after, which is `length`
/// bytes long: the mark of the dialect's encoding, or none.
fn byte_order_mark(dialect: &Dialect, length: u64) -> &'static [u8] {
    let mark: &'static [u8] = match dialect.encoding.name() {
        "UTF-8" => b"\xEF\xBB\xBF",
        "UTF-16LE" => b"\xFF\xFE",
        "UTF-16BE" => b"\xFE\xFF",
        _ => b"",
    };

    if mark.len() as u64 == length {
        mark
    } else {
        b""
    }
}

/// The start of the source as far as its bytes were kept: the byte-order
/// mark and the kept records.
struct SpanSource<'a> {
    mark: &'static [u8],
    span: &'a SourceSpan,
}

impl ByteSource for SpanSource<'_> {
    fn byte_length(&self) -> Result<u64, SourceError> {
        Ok(self.span.end())
    }

    /// Reads what of `range` the mark and the kept bytes hold. The writer
    /// asks only for ranges inside [`ByteSource::byte_length`].
    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        let mark_end = self.mark.len() as u64;
        let mut bytes = Vec::new();

        if range.start < mark_end {
            let start = usize::try_from(range.start).unwrap_or(usize::MAX);
            let end = usize::try_from(range.end.min(mark_end)).unwrap_or(usize::MAX);

            bytes.extend_from_slice(self.mark.get(start..end).unwrap_or_default());
        }

        let kept_start = range.start.max(mark_end);

        if kept_start < range.end
            && let Some(kept) = self.span.record(&(kept_start..range.end))
        {
            bytes.extend_from_slice(kept);
        }

        Ok(bytes)
    }
}

/// `bytes` decoded under the dialect's encoding, as they are: a byte-order
/// mark is never inside a record.
fn decode_text(bytes: &[u8], dialect: &Dialect) -> String {
    dialect
        .encoding
        .decode_without_bom_handling(bytes)
        .0
        .into_owned()
}

/// `text` split into its content and the line terminator it ends with:
/// CRLF, LF, a bare CR, or nothing.
fn split_terminator(text: &str) -> (&str, &str) {
    for terminator in ["\r\n", "\n", "\r"] {
        if let Some(content) = text.strip_suffix(terminator) {
            return (content, &text[content.len()..]);
        }
    }

    (text, "")
}

// -- Aligned -----------------------------------------------------------------

/// Renders the aligned text of the loaded records with the pending changes
/// of `edits`, the edit buffer of the table built from `model`. See the
/// module documentation.
pub(super) fn render_aligned(
    model: &PageModel,
    edits: &EditBuffer,
    limits: TextLimits,
) -> RenderedText {
    let names = model.column_names();
    let rows = aligned_rows(model, edits);

    let row_budget = limits.max_lines.saturating_sub(1);
    let measured = rows.len().min(row_budget);

    let mut widths: Vec<usize> = names.iter().map(|name| cell_width(name)).collect();

    for row in rows.iter().take(measured) {
        for (column, width) in widths.iter_mut().enumerate() {
            *width = (*width).max(cell_width(&row_value(model, edits, *row, column)));
        }
    }

    let mut line = String::new();

    let mut builder = TextBuilder::new(limits);

    if !names.is_empty() || !rows.is_empty() {
        let cells = names.iter().map(|name| Cow::Borrowed(name.as_str()));
        aligned_line(&mut line, cells, &widths);

        if !builder.push_row(TextRow::Header, &line) {
            return builder.finish(TextEnd::Capped { shown: 0 });
        }
    }

    for row in &rows {
        let cells = (0..widths.len()).map(|column| row_value(model, edits, *row, column));
        aligned_line(&mut line, cells, &widths);

        if !builder.push_row(*row, &line) {
            return builder.finish(TextEnd::Capped { shown: 0 });
        }
    }

    builder.finish(complete_end(model))
}

/// The rows the aligned text shows, in table order: every row the table
/// shows except the deleted ones.
fn aligned_rows(model: &PageModel, edits: &EditBuffer) -> Vec<TextRow> {
    edits
        .compute_visual_order()
        .into_iter()
        .filter_map(|source| match source {
            VisualRowSource::Base(row) if row < model.records().len() => {
                (!edits.is_pending_delete(row)).then_some(TextRow::Base(row))
            }

            VisualRowSource::Base(_) => None,
            VisualRowSource::Insert(index) => Some(TextRow::Insert(index)),
        })
        .collect()
}

/// The value of `column` in `row`, with its pending edit. Borrowed from the
/// loaded record unless the cell was edited.
fn row_value<'a>(
    model: &'a PageModel,
    edits: &EditBuffer,
    row: TextRow,
    column: usize,
) -> Cow<'a, str> {
    match row {
        TextRow::Header => Cow::Borrowed(""),

        TextRow::Base(row) => {
            // `has_changes` spares a lookup per cell while nothing is edited.
            if edits.has_changes() && edits.is_cell_dirty(row, column) {
                let absent = CellValue::text("");
                return Cow::Owned(edits.get_cell(row, column, &absent).edit_text());
            }

            model
                .records()
                .get(row)
                .and_then(|record| record.fields.get(column))
                .map_or(Cow::Borrowed(""), |field| Cow::Borrowed(field.as_str()))
        }

        TextRow::Insert(index) => Cow::Owned(
            edits
                .pending_inserts()
                .get(index)
                .and_then(|insert| insert.data.get(column))
                .map(CellValue::edit_text)
                .unwrap_or_default(),
        ),
    }
}

/// `value` as one cell of the aligned text: line breaks and tabs shown as
/// marks, and cut to [`ALIGNED_MAX_WIDTH`] characters. Borrowed when it
/// needs neither.
fn aligned_cell(value: &str) -> Cow<'_, str> {
    let shown = if value.contains(['\n', '\r', '\t']) {
        Cow::Owned(
            value
                .replace("\r\n", "\u{21b5}")
                .replace(['\n', '\r'], "\u{21b5}")
                .replace('\t', "\u{21e5}"),
        )
    } else {
        Cow::Borrowed(value)
    };

    // A value of at most the width in bytes is at most the width in
    // characters, which spares counting them.
    if shown.len() <= ALIGNED_MAX_WIDTH || shown.chars().count() <= ALIGNED_MAX_WIDTH {
        return shown;
    }

    let mut cut: String = shown.chars().take(ALIGNED_MAX_WIDTH - 1).collect();
    cut.push('\u{2026}');
    Cow::Owned(cut)
}

fn cell_width(value: &str) -> usize {
    aligned_cell(value).chars().count()
}

/// Writes one line of the aligned text into `line`, replacing what it held:
/// every cell padded to its column's width, the columns separated by ` | `,
/// without trailing spaces, and a line feed.
fn aligned_line<'a>(
    line: &mut String,
    cells: impl Iterator<Item = Cow<'a, str>>,
    widths: &[usize],
) {
    line.clear();

    for (column, (cell, width)) in cells.zip(widths).enumerate() {
        if column > 0 {
            line.push_str(" | ");
        }

        let cell = aligned_cell(&cell);
        let padding = width.saturating_sub(cell.chars().count());

        line.push_str(&cell);
        line.extend(std::iter::repeat_n(' ', padding));
    }

    line.truncate(line.trim_end_matches(' ').len());
    line.push('\n');
}
