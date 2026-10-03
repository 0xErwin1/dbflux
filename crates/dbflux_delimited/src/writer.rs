//! The byte-preserving writer: rewrites a delimited source with a set of
//! pending edits, copying every record that was not edited from the source
//! bytes and rendering only the records that were.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Write;
use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::Range;

use encoding_rs::{EncoderResult, Encoding, UTF_8, UTF_16BE, UTF_16LE};

use crate::Dialect;
use crate::reader::{
    CARRIAGE_RETURN, LINE_FEED, Layout, PagedReader, ReadError, ReaderOptions, byte_order_mark,
    ends_inside_quotes, record_length, to_u64,
};
use crate::source::{ByteSource, SourceError};

/// Where a new record goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertPosition {
    /// Immediately before the existing record with this `byte_range`.
    Before(Range<u64>),

    /// After the last record of the file.
    End,
}

/// New fields for an existing record, the header record included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replacement {
    /// The `byte_range` the reader returned for the record.
    pub byte_range: Range<u64>,

    pub fields: Vec<String>,
}

/// A new record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Insertion {
    pub position: InsertPosition,

    pub fields: Vec<String>,
}

/// A column added after the last field of every record.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AppendedColumn {
    /// The column's name, written to the header record. Ignored when the
    /// dialect has no header.
    pub header: String,

    /// The value written to every record that has no entry in `values`.
    pub default_value: String,

    /// The value for single records, each named by the `byte_range` the
    /// reader returned for it. A value for a record that is also replaced or
    /// deleted is ignored, and its range is still checked.
    pub values: Vec<(Range<u64>, String)>,
}

/// The pending changes to a delimited file.
///
/// Existing records are named by the `byte_range` the reader returned for
/// them. Renaming a column is a replacement of the header record.
///
/// Replaced and inserted records are written exactly from the fields they
/// carry. [`EditSet::appended_columns`] apply only to records that are
/// otherwise copied, so a replaced or inserted record must already include
/// its values for the new columns, and so must a replaced header.
///
/// There is no `Default`: an edit set always states which source its ranges
/// were read from, through [`EditSet::new`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EditSet {
    /// The byte length of the source when the ranges in this edit set were
    /// read. [`write_edited`] refuses a source of any other length.
    pub source_length: u64,

    pub replacements: Vec<Replacement>,

    /// The `byte_range` of every record to remove.
    pub deletions: Vec<Range<u64>>,

    /// Records inserted at the same position are written in the order they
    /// have here.
    pub insertions: Vec<Insertion>,

    /// Columns added after the last field, in this order.
    pub appended_columns: Vec<AppendedColumn>,
}

impl EditSet {
    /// An edit set without edits for a source of `source_length` bytes: the
    /// [`PagedReader::source_length`] of the reader that returned the records
    /// the edits will name.
    pub fn new(source_length: u64) -> Self {
        Self {
            source_length,
            replacements: Vec::new(),
            deletions: Vec::new(),
            insertions: Vec::new(),
            appended_columns: Vec::new(),
        }
    }

    /// Whether there is nothing to change, in which case the output is the
    /// source byte for byte.
    pub fn is_empty(&self) -> bool {
        self.replacements.is_empty()
            && self.deletions.is_empty()
            && self.insertions.is_empty()
            && self.appended_columns.is_empty()
    }
}

/// Which part of an [`EditSet`] a rendering error is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditLocation {
    /// The existing record with this `byte_range`: a replacement, or a value
    /// of an appended column.
    Existing(Range<u64>),

    /// The insertion at this index of [`EditSet::insertions`].
    Inserted(usize),

    /// The header or the default value of the column at this index of
    /// [`EditSet::appended_columns`].
    AppendedColumn(usize),
}

impl fmt::Display for EditLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Existing(range) => {
                write!(
                    formatter,
                    "the record at bytes {}..{}",
                    range.start, range.end
                )
            }

            Self::Inserted(index) => write!(formatter, "new record {}", index + 1),

            Self::AppendedColumn(index) => write!(formatter, "new column {}", index + 1),
        }
    }
}

/// A failure to write an edited delimited file.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    /// The source failed. The message is the source's own.
    #[error(transparent)]
    Source(SourceError),

    /// The sink refused bytes.
    #[error("could not write the output: {0}")]
    Sink(#[source] std::io::Error),

    /// The source could not be read as a file of the dialect.
    #[error(transparent)]
    Read(ReadError),

    /// The source is not as long as it was when the edit set's ranges were
    /// read, so those ranges no longer name its records.
    #[error(
        "the file changed since it was loaded (it was {expected} bytes and is now {actual}): reload the file and edit it again"
    )]
    SourceChanged { expected: u64, actual: u64 },

    /// An edit names a range that is empty or ends past the end of the
    /// source.
    #[error(
        "an edit names bytes {}..{}, which are not inside the {source_length}-byte file: reload the file and edit it again",
        .range.start,
        .range.end
    )]
    RangeOutsideSource {
        range: Range<u64>,
        source_length: u64,
    },

    /// An edit names a range that is not exactly one record: it starts
    /// inside the byte-order mark, does not start right after a line break,
    /// or does not end where the record that starts there ends. Reported
    /// before anything is written, except for a range that starts after a
    /// line break inside a quoted field, which is only found, after output
    /// has started, when the edit set appends a column.
    #[error(
        "bytes {}..{} are not one record of the file: reload the file and edit it again",
        .range.start,
        .range.end
    )]
    NotARecord { range: Range<u64> },

    /// Two edits name ranges that overlap without being the same record.
    #[error(
        "the edits of bytes {}..{} and bytes {}..{} overlap",
        .first.start,
        .first.end,
        .second.start,
        .second.end
    )]
    OverlappingEdits {
        first: Range<u64>,
        second: Range<u64>,
    },

    /// The same record is replaced twice, deleted twice, or given two values
    /// for the same appended column.
    #[error("the record at bytes {}..{} is edited twice", .range.start, .range.end)]
    DuplicateEdit { range: Range<u64> },

    /// The same record is both replaced and deleted.
    #[error(
        "the record at bytes {}..{} is both replaced and deleted",
        .range.start,
        .range.end
    )]
    ReplacedAndDeleted { range: Range<u64> },

    /// A field holds a character the dialect's encoding cannot represent.
    #[error(
        "{record} contains {character:?} (U+{:04X}), which {encoding} cannot represent: remove the character or change the file's encoding",
        u32::from(*character)
    )]
    UnencodableCharacter {
        record: EditLocation,
        character: char,
        encoding: &'static str,
    },

    /// A field needs quoting and the dialect has no quote character. `field`
    /// is the zero-based index of the field within the fields given for the
    /// record, and zero for an appended column.
    #[error(
        "field {} of {record} contains the delimiter or a line break, starts with U+FEFF, or is the only and empty field of its record, and the file has no quote character to enclose it: change the value or choose a quote character",
        field + 1
    )]
    UnquotableField { record: EditLocation, field: usize },

    /// The record at `offset` would become the first of a file without a
    /// byte-order mark, and its bytes start with one, so its first character
    /// would be read as the mark and lost.
    #[error(
        "the record at byte {offset} would become the first of the file, and its leading U+FEFF would then be read as a byte-order mark and lost: edit the first field of that record, or keep a record before it"
    )]
    LeadingByteOrderMark { offset: u64 },

    /// Removing the records before the empty line at `offset` would join its
    /// line feed to the carriage return before it, and the dialect has no
    /// quote character to keep the empty record apart.
    #[error(
        "removing the records before the empty line at byte {offset} would merge it into the line break before it, and the file has no quote character to keep it: delete that empty line too, or choose a quote character"
    )]
    FusedLineBreak { offset: u64 },

    /// The source ends in the middle of a code unit, and the edit set is not
    /// empty and leaves the final record, which holds the stray byte, in the
    /// output.
    #[error(
        "the {source_length}-byte file ends in the middle of a character, so it cannot be edited as it is: replace or delete its last record in the same save"
    )]
    TruncatedCodeUnit { source_length: u64 },

    /// A column cannot be appended to a record that ends inside a quoted
    /// field that was never closed.
    #[error(
        "the record at bytes {}..{} ends inside a quoted field that is never closed, so a column cannot be added after it: edit that record first",
        .byte_range.start,
        .byte_range.end
    )]
    UnclosedQuote { byte_range: Range<u64> },
}

impl From<SourceError> for WriteError {
    fn from(error: SourceError) -> Self {
        Self::Source(error)
    }
}

impl From<ReadError> for WriteError {
    fn from(error: ReadError) -> Self {
        match error {
            ReadError::Source(source) => Self::Source(source),
            other => Self::Read(other),
        }
    }
}

/// Writes `source` to `sink` with `edits` applied.
///
/// `source` is a file of `dialect` and is read in ranges of at most
/// `window_size` bytes, apart from single records: each edited record is read
/// whole once to check its range, and every record is when a column is
/// appended. The sink receives the output in pieces of about that size and is
/// flushed at the end. Neither the source nor the output is held whole in
/// memory, and the rendered form of every edited record is.
///
/// The sink must be a temporary destination that the caller discards on any
/// error. See the errors section for which errors can follow partial output.
///
/// # Byte preservation
///
/// Every byte of the source that is not inside a replaced or deleted record
/// is copied as it is, the byte-order mark included, so an empty edit set
/// writes the source byte for byte. Stretches between edited records are
/// copied by range without being scanned. A deleted record removes exactly
/// its range. A replaced record keeps its own terminator bytes, and a final
/// record without a terminator stays without one.
///
/// Appending a column scans every record through the reader. A record that is
/// not replaced keeps its bytes, and the new fields go between its last field
/// and its terminator. A source without records gets no header for the new
/// column: insert the header record instead.
///
/// A record that was not edited is changed in exactly two cases:
///
/// - When a record is inserted at the end and the final record of the source
///   has no terminator and is not deleted, that final record is given one.
/// - When the output would put a carriage return directly before the line
///   feed of an empty line that did not follow it in the source, because the
///   records between them were deleted, the two would read as one terminator
///   and the empty record would disappear. That empty record is written as a
///   pair of quotes before its line feed. In a dialect without a quote
///   character this is [`WriteError::FusedLineBreak`].
///
/// # Rendering
///
/// A replaced or inserted record is its fields joined by the delimiter. Each
/// field is encoded in the dialect's encoding and written as it is, unless one
/// of these holds, in which case it is enclosed in the quote character with
/// every quote inside it doubled:
///
/// - it contains the delimiter, the quote character, a carriage return or a
///   line feed;
/// - its first or last character is whitespace ([`char::is_whitespace`]);
/// - its first character is U+FEFF, which unquoted at the start of a file
///   would be read as a byte-order mark;
/// - it is empty and is the only field of its record, because a record of no
///   bytes is not a record once it is the last one of the file. A record given
///   no fields at all is written the same way.
///
/// Any other empty field is written as nothing between two delimiters.
///
/// In a dialect without a quote character, a field that contains the
/// delimiter or a line break, starts with U+FEFF, or is the empty only field
/// of its record, is [`WriteError::UnquotableField`]. Leading and trailing
/// whitespace is written as it is there, because the reader returns it
/// unchanged.
///
/// # Terminators of inserted records
///
/// Every inserted record is written with a terminator. A record inserted
/// before another takes that record's terminator, and a record inserted at
/// the end takes the terminator of the source's last record. When that record
/// has none, the terminator is the one of the first record of the source, the
/// header included, or a line feed when that has none either or the source has
/// no records.
///
/// # Errors
///
/// These are reported before the first byte is written and leave the sink
/// untouched:
///
/// - [`WriteError::Read`] for a dialect the reader refuses;
/// - [`WriteError::SourceChanged`] when the source's length is not
///   [`EditSet::source_length`];
/// - [`WriteError::RangeOutsideSource`], [`WriteError::OverlappingEdits`],
///   [`WriteError::DuplicateEdit`] and [`WriteError::ReplacedAndDeleted`];
/// - [`WriteError::UnencodableCharacter`] and [`WriteError::UnquotableField`];
/// - [`WriteError::NotARecord`] for every range the boundary check can
///   catch. Each range named by a replacement, a deletion, an
///   [`InsertPosition::Before`] or a value of an appended column must start
///   at the start of the data or right after a line break that is not a
///   carriage return whose line feed starts the range, and must be as long as
///   the record the reader's scanner finds there;
/// - [`WriteError::TruncatedCodeUnit`] when a UTF-16 source ends inside a
///   code unit and the edit set is not empty, unless it replaces or deletes
///   the final record, which removes the stray byte from the output. An
///   empty edit set still writes such a source byte for byte;
/// - [`WriteError::LeadingByteOrderMark`] when deleting the leading records
///   of a source without a byte-order mark would leave a copied record whose
///   bytes start with one at the start of the output.
///
/// These can be returned after part of the output was written:
///
/// - [`WriteError::Source`] and [`WriteError::Sink`], and
///   [`WriteError::Read`] for a source that returns fewer bytes than asked;
/// - [`WriteError::FusedLineBreak`];
/// - [`WriteError::UnclosedQuote`];
/// - [`WriteError::NotARecord`] for a range the boundary check cannot
///   catch, when the edit set appends a column.
///
/// # What the checks cannot catch
///
/// A range that starts right after a line break inside a quoted field and
/// ends at a line break looks like a record from its own bytes. It is found
/// only when a column is appended, which scans from the start. Any other save
/// writes it as given.
///
/// A range read from an older version of the source can line up with another
/// record of the current one. The length check catches that only when the
/// length changed. The caller owns the rest: it keeps a version of the source
/// with the ranges it read and does not pass ranges from another version.
pub fn write_edited<S: ByteSource, W: Write>(
    source: &S,
    dialect: &Dialect,
    edits: &EditSet,
    window_size: NonZeroU64,
    sink: &mut W,
) -> Result<(), WriteError> {
    let options = ReaderOptions {
        page_size: NonZeroUsize::MIN,
        window_size,
    };

    let mut reader = PagedReader::open(Borrowed(source), *dialect, options)?;

    let layout = *reader.layout();
    let bounds = reader.byte_order_mark_length()..reader.source_length();
    let header_start = reader.header().map(|header| header.byte_range.start);

    let renderer = Renderer {
        layout,
        encoding: dialect.encoding,
    };

    if edits.source_length != bounds.end {
        return Err(WriteError::SourceChanged {
            expected: edits.source_length,
            actual: bounds.end,
        });
    }

    let mut plan = Plan::build(edits, &renderer, &bounds, header_start)?;

    let final_record_deleted =
        plan.final_record_is(&bounds, |action| matches!(action, Action::Delete));

    let mut output = Output {
        source,
        sink,
        layout,
        window_size: window_size.get(),
        last_bytes: Vec::new(),
        written: 0,
    };

    output.check_before_writing(&mut plan, &bounds, byte_order_mark(dialect.encoding))?;

    let fallback_terminator = if edits.insertions.is_empty() {
        Vec::new()
    } else {
        first_terminator(&mut reader, &output)?
    };

    match &plan.appended {
        Some(appended) => {
            output.copy(0..bounds.start)?;

            output.write_by_record(
                &reader,
                plan.records,
                appended,
                header_start,
                &fallback_terminator,
            )?;
        }

        None => output.write_by_range(plan.records, bounds.end, &fallback_terminator)?,
    }

    if !plan.inserted_at_end.is_empty() {
        let mut terminator = output.terminator_of(bounds.clone())?;

        if terminator.is_empty() {
            terminator = fallback_terminator;

            if !bounds.is_empty() && !final_record_deleted {
                output.write(&terminator)?;
            }
        }

        output.write_inserted(&plan.inserted_at_end, &terminator, &terminator)?;
    }

    output.sink.flush().map_err(WriteError::Sink)
}

/// Renders `fields` as one record of `dialect`, without its terminator and in
/// the dialect's encoding, exactly as [`write_edited`] renders a replaced or
/// inserted record (see its rendering rules). `location` names the record in
/// a returned error.
///
/// # Errors
///
/// [`WriteError::Read`] for a dialect the reader refuses,
/// [`WriteError::UnencodableCharacter`] and [`WriteError::UnquotableField`].
pub fn render_record(
    fields: &[String],
    dialect: &Dialect,
    location: &EditLocation,
) -> Result<Vec<u8>, WriteError> {
    Renderer::for_dialect(dialect)?.record(fields, location)
}

/// Renders `values` as the fields that appended columns add after the last
/// field of a record that is otherwise copied, each led by the delimiter and
/// in the dialect's encoding, exactly as [`write_edited`] renders them.
/// `location` names the record or column in a returned error.
///
/// # Errors
///
/// The errors of [`render_record`].
pub fn render_appended_fields(
    values: &[String],
    dialect: &Dialect,
    location: &EditLocation,
) -> Result<Vec<u8>, WriteError> {
    let renderer = Renderer::for_dialect(dialect)?;
    let mut rendered = Vec::new();

    for value in values {
        renderer.appended_field(&mut rendered, value, location)?;
    }

    Ok(rendered)
}

/// Lends a source to a reader without giving it away.
struct Borrowed<'a, S>(&'a S);

impl<S: ByteSource> ByteSource for Borrowed<'_, S> {
    fn byte_length(&self) -> Result<u64, SourceError> {
        self.0.byte_length()
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        self.0.read_range(range)
    }
}

/// What happens to an existing record.
#[derive(Debug)]
enum Action {
    /// Its bytes are copied.
    Keep,

    /// These rendered bytes, without a terminator, take its place.
    Replace(Vec<u8>),

    Delete,
}

/// Everything an edit set asks of one existing record.
#[derive(Debug)]
struct RecordPlan {
    /// The end of the record's byte range.
    end: u64,

    /// The rendered records to write before it, without their terminators.
    inserted_before: Vec<Vec<u8>>,

    action: Action,

    /// The rendered fields to add after its last field, each led by a
    /// delimiter, when they differ from the ones every other record gets.
    appended: Option<Vec<u8>>,

    /// The terminator its bytes in the source end with, taken from the read
    /// that checks its boundaries before anything is written.
    terminator: Vec<u8>,
}

/// The rendered fields that appended columns add to a record, each led by a
/// delimiter.
#[derive(Debug)]
struct AppendedFields {
    header: Vec<u8>,
    default: Vec<u8>,
}

/// A checked edit set with every edited record rendered.
#[derive(Debug)]
struct Plan {
    /// The edited records by the start of their byte range.
    records: BTreeMap<u64, RecordPlan>,

    /// The rendered records to write after the last one, without their
    /// terminators.
    inserted_at_end: Vec<Vec<u8>>,

    /// `None` when the edit set appends no column.
    appended: Option<AppendedFields>,
}

impl Plan {
    /// Whether the record that ends where the source does is planned with an
    /// action that `matches`.
    fn final_record_is(&self, bounds: &Range<u64>, matches: impl Fn(&Action) -> bool) -> bool {
        self.records
            .values()
            .any(|record| record.end == bounds.end && matches(&record.action))
    }

    /// Checks `edits` against a source whose records span `bounds` and whose
    /// header, if any, starts at `header_start`, and renders every record and
    /// field the edits carry.
    fn build(
        edits: &EditSet,
        renderer: &Renderer,
        bounds: &Range<u64>,
        header_start: Option<u64>,
    ) -> Result<Self, WriteError> {
        let mut records = BTreeMap::new();
        let mut inserted_at_end = Vec::new();

        for replacement in &edits.replacements {
            let range = &replacement.byte_range;
            let record = planned_record(&mut records, bounds, range)?;

            if !matches!(record.action, Action::Keep) {
                return Err(WriteError::DuplicateEdit {
                    range: range.clone(),
                });
            }

            let location = EditLocation::Existing(range.clone());
            record.action = Action::Replace(renderer.record(&replacement.fields, &location)?);
        }

        for range in &edits.deletions {
            let record = planned_record(&mut records, bounds, range)?;

            match record.action {
                Action::Keep => record.action = Action::Delete,

                Action::Replace(_) => {
                    return Err(WriteError::ReplacedAndDeleted {
                        range: range.clone(),
                    });
                }

                Action::Delete => {
                    return Err(WriteError::DuplicateEdit {
                        range: range.clone(),
                    });
                }
            }
        }

        for (index, insertion) in edits.insertions.iter().enumerate() {
            let rendered = renderer.record(&insertion.fields, &EditLocation::Inserted(index))?;

            match &insertion.position {
                InsertPosition::Before(range) => planned_record(&mut records, bounds, range)?
                    .inserted_before
                    .push(rendered),

                InsertPosition::End => inserted_at_end.push(rendered),
            }
        }

        let appended = plan_appended_columns(
            &edits.appended_columns,
            renderer,
            &mut records,
            bounds,
            header_start,
        )?;

        let mut previous: Option<Range<u64>> = None;

        for (start, record) in &records {
            let current = *start..record.end;

            if let Some(previous) = previous
                && previous.end > current.start
            {
                return Err(WriteError::OverlappingEdits {
                    first: previous,
                    second: current,
                });
            }

            previous = Some(current);
        }

        Ok(Self {
            records,
            inserted_at_end,
            appended,
        })
    }
}

/// Returns the plan of the record at `range`, creating it when this is the
/// first edit to name the record. Refuses a range outside `bounds` and a
/// range that starts where another planned record does but ends elsewhere.
fn planned_record<'a>(
    records: &'a mut BTreeMap<u64, RecordPlan>,
    bounds: &Range<u64>,
    range: &Range<u64>,
) -> Result<&'a mut RecordPlan, WriteError> {
    if range.start >= range.end || range.end > bounds.end {
        return Err(WriteError::RangeOutsideSource {
            range: range.clone(),
            source_length: bounds.end,
        });
    }

    if range.start < bounds.start {
        return Err(WriteError::NotARecord {
            range: range.clone(),
        });
    }

    let record = records.entry(range.start).or_insert_with(|| RecordPlan {
        end: range.end,
        inserted_before: Vec::new(),
        action: Action::Keep,
        appended: None,
        terminator: Vec::new(),
    });

    if record.end != range.end {
        return Err(WriteError::OverlappingEdits {
            first: range.start..record.end,
            second: range.clone(),
        });
    }

    Ok(record)
}

/// Renders the fields that `columns` add: the ones for the header when the
/// source has one, the ones for a record without a value of its own, and into
/// `records` the ones for each copied record that has a value of its own in at
/// least one column. A value for a replaced or deleted record is not rendered.
fn plan_appended_columns(
    columns: &[AppendedColumn],
    renderer: &Renderer,
    records: &mut BTreeMap<u64, RecordPlan>,
    bounds: &Range<u64>,
    header_start: Option<u64>,
) -> Result<Option<AppendedFields>, WriteError> {
    if columns.is_empty() {
        return Ok(None);
    }

    let mut fields = AppendedFields {
        header: Vec::new(),
        default: Vec::new(),
    };

    let mut own_values: BTreeMap<u64, (Range<u64>, Vec<Option<&str>>)> = BTreeMap::new();

    for (index, column) in columns.iter().enumerate() {
        let location = EditLocation::AppendedColumn(index);

        if header_start.is_some() {
            renderer.appended_field(&mut fields.header, &column.header, &location)?;
        }

        renderer.appended_field(&mut fields.default, &column.default_value, &location)?;

        for (range, value) in &column.values {
            planned_record(records, bounds, range)?;

            let (_, values) = own_values
                .entry(range.start)
                .or_insert_with(|| (range.clone(), vec![None; columns.len()]));

            match values.get_mut(index) {
                Some(slot @ None) => *slot = Some(value),

                _ => {
                    return Err(WriteError::DuplicateEdit {
                        range: range.clone(),
                    });
                }
            }
        }
    }

    for (range, values) in own_values.into_values() {
        let record = planned_record(records, bounds, &range)?;

        if !matches!(record.action, Action::Keep) {
            continue;
        }

        let is_header = header_start == Some(range.start);
        let mut rendered = Vec::new();

        for ((index, column), value) in columns.iter().enumerate().zip(values) {
            match value {
                Some(value) => {
                    let location = EditLocation::Existing(range.clone());
                    renderer.appended_field(&mut rendered, value, &location)?;
                }

                None => {
                    let inherited = if is_header {
                        &column.header
                    } else {
                        &column.default_value
                    };

                    let location = EditLocation::AppendedColumn(index);
                    renderer.appended_field(&mut rendered, inherited, &location)?;
                }
            }
        }

        record.appended = Some(rendered);
    }

    Ok(Some(fields))
}

/// Turns fields into the bytes of a record of the dialect.
struct Renderer {
    layout: Layout,
    encoding: &'static Encoding,
}

impl Renderer {
    /// The renderer of `dialect`, or the reader's refusal of it.
    fn for_dialect(dialect: &Dialect) -> Result<Self, WriteError> {
        Ok(Self {
            layout: Layout::for_dialect(dialect)?,
            encoding: dialect.encoding,
        })
    }

    /// Renders `fields` as one record without its terminator.
    fn record(&self, fields: &[String], location: &EditLocation) -> Result<Vec<u8>, WriteError> {
        let mut rendered = Vec::new();

        if fields.is_empty() {
            self.field(&mut rendered, "", true, location, 0)?;
        }

        for (index, field) in fields.iter().enumerate() {
            if index > 0 {
                self.layout.push_unit(&mut rendered, self.layout.delimiter);
            }

            self.field(&mut rendered, field, fields.len() == 1, location, index)?;
        }

        Ok(rendered)
    }

    /// Renders a delimiter and then `text` as a field that follows others.
    fn appended_field(
        &self,
        rendered: &mut Vec<u8>,
        text: &str,
        location: &EditLocation,
    ) -> Result<(), WriteError> {
        self.layout.push_unit(rendered, self.layout.delimiter);

        self.field(rendered, text, false, location, 0)
    }

    /// Appends `text` to `rendered` as one field, quoted when the rule on
    /// [`write_edited`] says so.
    fn field(
        &self,
        rendered: &mut Vec<u8>,
        text: &str,
        is_only_field: bool,
        location: &EditLocation,
        index: usize,
    ) -> Result<(), WriteError> {
        let encoded = self
            .encode(text)
            .map_err(|character| WriteError::UnencodableCharacter {
                record: location.clone(),
                character,
                encoding: self.encoding.name(),
            })?;

        let layout = &self.layout;

        let units = || {
            (0..encoded.len())
                .step_by(layout.unit_size())
                .filter_map(|position| layout.unit_at(&encoded, position))
        };

        let has_structural_unit = units().any(|unit| {
            unit == layout.delimiter
                || Some(unit) == layout.quote
                || unit == LINE_FEED
                || unit == CARRIAGE_RETURN
        });

        let is_padded =
            text.starts_with(char::is_whitespace) || text.ends_with(char::is_whitespace);

        let is_only_and_empty = is_only_field && text.is_empty();

        // Unquoted at the start of a file, it would be read as the mark.
        let starts_like_a_mark = text.starts_with('\u{feff}');

        let Some(quote) = layout.quote else {
            if has_structural_unit || is_only_and_empty || starts_like_a_mark {
                return Err(WriteError::UnquotableField {
                    record: location.clone(),
                    field: index,
                });
            }

            rendered.extend_from_slice(&encoded);

            return Ok(());
        };

        if !(has_structural_unit || is_padded || is_only_and_empty || starts_like_a_mark) {
            rendered.extend_from_slice(&encoded);

            return Ok(());
        }

        layout.push_unit(rendered, quote);

        for unit in units() {
            if unit == quote {
                layout.push_unit(rendered, quote);
            }

            layout.push_unit(rendered, unit);
        }

        layout.push_unit(rendered, quote);

        Ok(())
    }

    /// Encodes `text`, or returns the first character the encoding cannot
    /// represent.
    ///
    /// UTF-16 is encoded here because `encoding_rs` has no UTF-16 encoder:
    /// its encoder for a UTF-16 encoding writes UTF-8.
    fn encode(&self, text: &str) -> Result<Vec<u8>, char> {
        if self.encoding == UTF_8 {
            return Ok(text.as_bytes().to_vec());
        }

        if self.encoding == UTF_16LE {
            return Ok(text.encode_utf16().flat_map(u16::to_le_bytes).collect());
        }

        if self.encoding == UTF_16BE {
            return Ok(text.encode_utf16().flat_map(u16::to_be_bytes).collect());
        }

        let mut encoder = self.encoding.new_encoder();
        let mut encoded = Vec::new();
        let mut remaining = text;

        loop {
            let needed = encoder
                .max_buffer_length_from_utf8_without_replacement(remaining.len())
                .unwrap_or(remaining.len());

            encoded.reserve(needed.max(1));

            let (result, read) =
                encoder.encode_from_utf8_to_vec_without_replacement(remaining, &mut encoded, true);

            match result {
                EncoderResult::InputEmpty => return Ok(encoded),
                EncoderResult::Unmappable(character) => return Err(character),
                EncoderResult::OutputFull => remaining = remaining.get(read..).unwrap_or_default(),
            }
        }
    }
}

/// Returns the length in bytes of the record terminator that `bytes` ends
/// with, or zero. `bytes` must start on a code unit boundary.
fn terminator_length(bytes: &[u8], layout: &Layout) -> usize {
    let unit_size = layout.unit_size();

    if !bytes.len().is_multiple_of(unit_size) {
        return 0;
    }

    let unit_from_end = |count: usize| {
        bytes
            .len()
            .checked_sub(count * unit_size)
            .and_then(|position| layout.unit_at(bytes, position))
    };

    match unit_from_end(1) {
        Some(LINE_FEED) if unit_from_end(2) == Some(CARRIAGE_RETURN) => 2 * unit_size,
        Some(LINE_FEED | CARRIAGE_RETURN) => unit_size,
        _ => 0,
    }
}

/// Returns the terminator of the first record of the source, the header
/// included, or a line feed when that record has none or there is no record.
fn first_terminator<S: ByteSource, W>(
    reader: &mut PagedReader<Borrowed<'_, S>>,
    output: &Output<'_, S, W>,
) -> Result<Vec<u8>, WriteError> {
    let first_record = match reader.header() {
        Some(header) => Some(header.byte_range.clone()),

        None => reader
            .read_page(0)?
            .records
            .into_iter()
            .next()
            .map(|record| record.byte_range),
    };

    let mut terminator = match first_record {
        Some(range) => output.terminator_of(range)?,
        None => Vec::new(),
    };

    if terminator.is_empty() {
        output.layout.push_unit(&mut terminator, LINE_FEED);
    }

    Ok(terminator)
}

/// The source being copied from and the sink being written to.
struct Output<'a, S, W> {
    source: &'a S,
    sink: &'a mut W,
    layout: Layout,
    window_size: u64,

    /// The last bytes written to the sink, at most one code unit of them.
    last_bytes: Vec<u8>,

    /// How many bytes were written to the sink.
    written: u64,
}

impl<S: ByteSource, W: Write> Output<'_, S, W> {
    fn write(&mut self, bytes: &[u8]) -> Result<(), WriteError> {
        self.sink.write_all(bytes).map_err(WriteError::Sink)?;

        self.written = self.written.saturating_add(to_u64(bytes.len()));
        self.last_bytes.extend_from_slice(bytes);

        let surplus = self
            .last_bytes
            .len()
            .saturating_sub(self.layout.unit_size());
        self.last_bytes.drain(..surplus);

        Ok(())
    }

    /// Whether the last code unit written is a carriage return.
    fn ends_with_carriage_return(&self) -> bool {
        self.written.is_multiple_of(to_u64(self.layout.unit_size()))
            && self.layout.unit_at(&self.last_bytes, 0) == Some(CARRIAGE_RETURN)
    }

    /// Copies the bytes of `range` from the source to the sink, one window at
    /// a time. `range` starts at a record or at the start of the source.
    ///
    /// When the output so far ends with a carriage return and `range` starts
    /// with a line feed, the two were not next to each other in the source:
    /// the line feed is an empty record whose predecessors were removed, and
    /// copied as it is the pair would read as one terminator. The empty
    /// record is written as a pair of quotes before its line feed instead.
    fn copy(&mut self, range: Range<u64>) -> Result<(), WriteError> {
        if !range.is_empty() && self.ends_with_carriage_return() {
            let unit_size = to_u64(self.layout.unit_size());
            let first_unit_end = range.start.saturating_add(unit_size).min(range.end);

            let first_bytes = self.read(range.start..first_unit_end)?;

            if self.layout.unit_at(&first_bytes, 0) == Some(LINE_FEED) {
                let Some(quote) = self.layout.quote else {
                    return Err(WriteError::FusedLineBreak {
                        offset: range.start,
                    });
                };

                let mut empty_field = Vec::new();
                self.layout.push_unit(&mut empty_field, quote);
                self.layout.push_unit(&mut empty_field, quote);

                self.write(&empty_field)?;
            }
        }

        let mut position = range.start;

        while position < range.end {
            let end = position.saturating_add(self.window_size).min(range.end);

            let bytes = self.read(position..end)?;
            self.write(&bytes)?;

            position = end;
        }

        Ok(())
    }

    /// Writes each record of `inserted` followed by `terminator`, or by
    /// `fallback` when `terminator` is empty.
    fn write_inserted(
        &mut self,
        inserted: &[Vec<u8>],
        terminator: &[u8],
        fallback: &[u8],
    ) -> Result<(), WriteError> {
        let terminator = if terminator.is_empty() {
            fallback
        } else {
            terminator
        };

        for record in inserted {
            self.write(record)?;
            self.write(terminator)?;
        }

        Ok(())
    }

    /// Applies `records` without scanning the source: everything between two
    /// edited records is copied by range.
    fn write_by_range(
        &mut self,
        records: BTreeMap<u64, RecordPlan>,
        source_length: u64,
        fallback_terminator: &[u8],
    ) -> Result<(), WriteError> {
        let mut position = 0;

        for (start, record) in records {
            self.copy(position..start)?;

            self.write_inserted(
                &record.inserted_before,
                &record.terminator,
                fallback_terminator,
            )?;

            position = match record.action {
                Action::Keep => start,

                Action::Replace(rendered) => {
                    self.write(&rendered)?;
                    self.write(&record.terminator)?;

                    record.end
                }

                Action::Delete => record.end,
            };
        }

        self.copy(position..source_length)
    }

    /// Applies `records` while scanning every record of the source, adding
    /// the `appended` fields to each record that is copied.
    fn write_by_record(
        &mut self,
        reader: &PagedReader<Borrowed<'_, S>>,
        mut records: BTreeMap<u64, RecordPlan>,
        appended: &AppendedFields,
        header_start: Option<u64>,
        fallback_terminator: &[u8],
    ) -> Result<(), WriteError> {
        let layout = self.layout;

        reader.visit_raw_records(|start, bytes| {
            let byte_range = start..start + to_u64(bytes.len());

            let content_length = bytes.len() - terminator_length(bytes, &layout);
            let (content, terminator) = bytes
                .split_at_checked(content_length)
                .unwrap_or((bytes, &[]));

            let inherited = if header_start == Some(start) {
                &appended.header
            } else {
                &appended.default
            };

            let Some(record) = records.remove(&start) else {
                return self.write_extended(&byte_range, bytes, content, inherited, terminator);
            };

            if record.end != byte_range.end {
                return Err(WriteError::NotARecord {
                    range: start..record.end,
                });
            }

            self.write_inserted(&record.inserted_before, terminator, fallback_terminator)?;

            match record.action {
                Action::Keep => {
                    let fields = record.appended.as_ref().unwrap_or(inherited);

                    self.write_extended(&byte_range, bytes, content, fields, terminator)
                }

                Action::Replace(rendered) => {
                    self.write(&rendered)?;
                    self.write(terminator)
                }

                Action::Delete => Ok(()),
            }
        })?;

        match records.into_iter().next() {
            Some((start, record)) => Err(WriteError::NotARecord {
                range: start..record.end,
            }),

            None => Ok(()),
        }
    }

    /// Writes a copied record with `fields` between its `content` and its
    /// `terminator`. `bytes` is the whole record.
    fn write_extended(
        &mut self,
        byte_range: &Range<u64>,
        bytes: &[u8],
        content: &[u8],
        fields: &[u8],
        terminator: &[u8],
    ) -> Result<(), WriteError> {
        if ends_inside_quotes(bytes, &self.layout) {
            return Err(WriteError::UnclosedQuote {
                byte_range: byte_range.clone(),
            });
        }

        self.write(content)?;
        self.write(fields)?;
        self.write(terminator)
    }
}

impl<S: ByteSource, W> Output<'_, S, W> {
    /// Runs every check that needs the source and can be made before the
    /// first byte is written, and keeps the terminator of each edited record
    /// from the read that checks it.
    fn check_before_writing(
        &self,
        plan: &mut Plan,
        bounds: &Range<u64>,
        mark: &[u8],
    ) -> Result<(), WriteError> {
        let unit_size = to_u64(self.layout.unit_size());
        let ends_inside_a_unit = !(bounds.end - bounds.start).is_multiple_of(unit_size);

        let has_edits =
            !plan.records.is_empty() || !plan.inserted_at_end.is_empty() || plan.appended.is_some();

        let final_record_is_rewritten =
            plan.final_record_is(bounds, |action| !matches!(action, Action::Keep));

        if ends_inside_a_unit && has_edits && !final_record_is_rewritten {
            return Err(WriteError::TruncatedCodeUnit {
                source_length: bounds.end,
            });
        }

        for (start, record) in &mut plan.records {
            record.terminator = self.check_record_boundaries(&(*start..record.end), bounds)?;
        }

        self.check_first_record(plan, bounds, mark)
    }

    /// Checks that `range`, which is inside `bounds`, starts where a record
    /// starts and holds exactly one record, with one read of the range and
    /// one code unit on each side of it, and returns the terminator the
    /// record ends with, empty when it has none.
    ///
    /// The start is accepted after a line break that does not split a
    /// carriage return from its line feed, and at the start of the data. The
    /// length must be the one the reader's scanner finds from there. A range
    /// that starts after a line break inside a quoted field passes both.
    fn check_record_boundaries(
        &self,
        range: &Range<u64>,
        bounds: &Range<u64>,
    ) -> Result<Vec<u8>, WriteError> {
        let not_a_record = || WriteError::NotARecord {
            range: range.clone(),
        };

        let unit_size = self.layout.unit_size();

        if !(range.start - bounds.start).is_multiple_of(to_u64(unit_size)) {
            return Err(not_a_record());
        }

        let lead = if range.start == bounds.start {
            0
        } else {
            unit_size
        };

        let read_end = range.end.saturating_add(to_u64(unit_size)).min(bounds.end);

        let bytes = self.read(range.start - to_u64(lead)..read_end)?;
        let (before, record) = bytes.split_at_checked(lead).ok_or_else(not_a_record)?;

        if lead > 0 {
            let previous = self.layout.unit_at(before, 0);
            let first = self.layout.unit_at(record, 0);

            let follows_a_line_break = matches!(previous, Some(LINE_FEED | CARRIAGE_RETURN));
            let splits_a_pair = previous == Some(CARRIAGE_RETURN) && first == Some(LINE_FEED);

            if !follows_a_line_break || splits_a_pair {
                return Err(not_a_record());
            }
        }

        let length = record_length(record, &self.layout, read_end >= bounds.end)
            .filter(|length| to_u64(*length) == range.end - range.start)
            .ok_or_else(not_a_record)?;

        let record = record.get(..length).ok_or_else(not_a_record)?;
        let content_length = record.len() - terminator_length(record, &self.layout);

        Ok(record.get(content_length..).unwrap_or_default().to_vec())
    }

    /// Refuses a save after which a source without a byte-order mark would
    /// start with a copied record whose own bytes start with `mark`.
    fn check_first_record(
        &self,
        plan: &Plan,
        bounds: &Range<u64>,
        mark: &[u8],
    ) -> Result<(), WriteError> {
        if bounds.start != 0 || mark.is_empty() {
            return Ok(());
        }

        let mut first_copied = 0;

        while let Some(record) = plan.records.get(&first_copied) {
            if !record.inserted_before.is_empty() {
                return Ok(());
            }

            match record.action {
                Action::Delete => first_copied = record.end,
                Action::Keep => break,
                Action::Replace(_) => return Ok(()),
            }
        }

        if first_copied == 0 || first_copied >= bounds.end {
            return Ok(());
        }

        let mark_end = first_copied
            .saturating_add(to_u64(mark.len()))
            .min(bounds.end);

        if self.read(first_copied..mark_end)? == mark {
            return Err(WriteError::LeadingByteOrderMark {
                offset: first_copied,
            });
        }

        Ok(())
    }

    /// Reads exactly `range`, which must be inside the source.
    fn read(&self, range: Range<u64>) -> Result<Vec<u8>, WriteError> {
        let bytes = self.source.read_range(range.clone())?;

        if to_u64(bytes.len()) != range.end.saturating_sub(range.start) {
            return Err(WriteError::Read(ReadError::UnexpectedReadLength {
                start: range.start,
                end: range.end,
                received: bytes.len(),
            }));
        }

        Ok(bytes)
    }

    /// Returns the terminator bytes that the record, or run of records, at
    /// `range` ends with, reading only its last two code units. Empty when it
    /// ends without a terminator, is empty, or is not a whole number of code
    /// units.
    fn terminator_of(&self, range: Range<u64>) -> Result<Vec<u8>, WriteError> {
        let unit_size = to_u64(self.layout.unit_size());

        if range.is_empty() || !(range.end - range.start).is_multiple_of(unit_size) {
            return Ok(Vec::new());
        }

        let tail_start = range.end.saturating_sub(2 * unit_size).max(range.start);

        let mut tail = self.read(tail_start..range.end)?;
        let content_length = tail.len() - terminator_length(&tail, &self.layout);

        Ok(tail.split_off(content_length))
    }
}
