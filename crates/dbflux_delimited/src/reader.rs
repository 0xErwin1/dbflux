//! Record scanning, the page index and the paged reader.
//!
//! One state machine ([`step`]) decides what every code unit of a file means.
//! Finding where a record ends and splitting a record into fields both run it,
//! so the two cannot disagree about quoting.

use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::Range;

use encoding_rs::{EUC_JP, Encoding, ISO_2022_JP, UTF_8, UTF_16BE, UTF_16LE};

use crate::Dialect;
use crate::source::{ByteSource, SourceError};

pub(crate) const LINE_FEED: u16 = 0x0A;
pub(crate) const CARRIAGE_RETURN: u16 = 0x0D;

/// How a [`PagedReader`] pages and fetches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderOptions {
    /// The number of data records in every page except possibly the last.
    pub page_size: NonZeroUsize,

    /// The number of bytes asked from the source in one read. A record that
    /// does not end within the bytes read so far is continued with one more
    /// window, and each further read for that same record doubles in size.
    pub window_size: NonZeroU64,
}

/// A failure to open or read a delimited source.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// The source failed. The message is the source's own.
    #[error(transparent)]
    Source(#[from] SourceError),

    /// The source returned a different number of bytes than the clamped range
    /// it was asked for, which breaks the [`ByteSource`] contract.
    #[error("the source returned {received} bytes for the byte range {start}..{end}")]
    UnexpectedReadLength {
        start: u64,
        end: u64,
        received: usize,
    },

    /// The delimiter or quote `byte` can occur inside a multi-byte character
    /// of `encoding`, so scanning the file by byte could split a character.
    #[error("a {encoding} file cannot be read with byte {byte:#04x} as its delimiter or quote")]
    UnsupportedDialect { encoding: &'static str, byte: u8 },

    /// The delimiter or the quote is the line feed or carriage return `byte`.
    /// Those always end a record, so they cannot also separate or enclose
    /// fields.
    #[error(
        "the delimiter and the quote cannot be a line break (byte {byte:#04x}): choose a different character"
    )]
    LineBreakInDialect { byte: u8 },

    /// The quote and the delimiter are the same `byte`, so a field could
    /// never be told apart from its separator.
    #[error(
        "the quote and the delimiter are both {:?}: choose a different character for one of them",
        char::from(*byte)
    )]
    QuoteEqualsDelimiter { byte: u8 },
}

/// One record of a delimited source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// The bytes of the record in the source, including its terminator. The
    /// ranges of the header and of all data records are contiguous and,
    /// together with the byte-order mark, cover the whole source.
    pub byte_range: Range<u64>,

    /// The decoded fields, without their enclosing quotes and with every
    /// doubled quote reduced to one. An empty line is one empty field.
    pub fields: Vec<String>,

    /// Whether decoding replaced at least one malformed sequence in this
    /// record with U+FFFD, which means the fields do not round-trip to the
    /// record's bytes and the encoding is probably wrong.
    pub had_replacements: bool,
}

/// One page of data records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    /// The zero-based index, among data records, of the first record this
    /// page holds or would hold. The header is not a data record.
    pub first_record: u64,

    /// The records of the page. Fewer than the page size on the last page,
    /// and none for a page past the end.
    pub records: Vec<Record>,
}

/// How many data records a source has, as far as the reader knows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordCount {
    /// A scan reached the end of the source: this is the exact total.
    Total(u64),

    /// No scan has reached the end yet. This many records have been scanned,
    /// and the source holds at least that many.
    IndexedSoFar(u64),
}

/// The size of the code units a file is scanned in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnitWidth {
    Byte,
    Utf16LittleEndian,
    Utf16BigEndian,
}

/// The dialect as the scanner needs it: the unit width, and the delimiter and
/// quote as code units.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Layout {
    width: UnitWidth,
    pub(crate) delimiter: u16,
    pub(crate) quote: Option<u16>,
}

impl Layout {
    /// Resolves how to scan a file of `dialect`. Refuses a dialect whose
    /// delimiter or quote is a line break, whose quote is its delimiter, or
    /// whose delimiter or quote byte could be part of a multi-byte character.
    pub(crate) fn for_dialect(dialect: &Dialect) -> Result<Self, ReadError> {
        let encoding = dialect.encoding;
        let special_bytes = [Some(dialect.delimiter), dialect.quote];

        let line_break = special_bytes
            .into_iter()
            .flatten()
            .find(|byte| matches!(byte, b'\n' | b'\r'));

        if let Some(byte) = line_break {
            return Err(ReadError::LineBreakInDialect { byte });
        }

        if dialect.quote == Some(dialect.delimiter) {
            return Err(ReadError::QuoteEqualsDelimiter {
                byte: dialect.delimiter,
            });
        }

        let width = if encoding == UTF_16LE {
            UnitWidth::Utf16LittleEndian
        } else if encoding == UTF_16BE {
            UnitWidth::Utf16BigEndian
        } else {
            UnitWidth::Byte
        };

        if width == UnitWidth::Byte {
            let collision = special_bytes
                .into_iter()
                .flatten()
                .find(|byte| !is_scannable_by_byte(encoding, *byte));

            if let Some(byte) = collision {
                return Err(ReadError::UnsupportedDialect {
                    encoding: encoding.name(),
                    byte,
                });
            }
        }

        Ok(Self {
            width,
            delimiter: u16::from(dialect.delimiter),
            quote: dialect.quote.map(u16::from),
        })
    }

    pub(crate) fn unit_size(&self) -> usize {
        match self.width {
            UnitWidth::Byte => 1,
            UnitWidth::Utf16LittleEndian | UnitWidth::Utf16BigEndian => 2,
        }
    }

    /// Returns the code unit that starts at `position`, or `None` when fewer
    /// bytes than one unit are left.
    pub(crate) fn unit_at(&self, bytes: &[u8], position: usize) -> Option<u16> {
        match self.width {
            UnitWidth::Byte => bytes.get(position).copied().map(u16::from),

            UnitWidth::Utf16LittleEndian => match bytes.get(position..position.checked_add(2)?) {
                Some(&[low, high]) => Some(u16::from_le_bytes([low, high])),
                _ => None,
            },

            UnitWidth::Utf16BigEndian => match bytes.get(position..position.checked_add(2)?) {
                Some(&[high, low]) => Some(u16::from_be_bytes([high, low])),
                _ => None,
            },
        }
    }

    /// Appends the bytes of code unit `unit` to `bytes`. With one-byte units,
    /// a unit above 0xFF has no encoding and appends nothing.
    pub(crate) fn push_unit(&self, bytes: &mut Vec<u8>, unit: u16) {
        match self.width {
            UnitWidth::Byte => bytes.extend(u8::try_from(unit).ok()),
            UnitWidth::Utf16LittleEndian => bytes.extend_from_slice(&unit.to_le_bytes()),
            UnitWidth::Utf16BigEndian => bytes.extend_from_slice(&unit.to_be_bytes()),
        }
    }
}

/// Whether `byte`, used as a delimiter or quote, can be matched byte by byte
/// in a file of `encoding` without ever matching part of a longer character.
///
/// Line feed and carriage return pass this test in every encoding it accepts,
/// so only the delimiter and the quote need checking.
fn is_scannable_by_byte(encoding: &'static Encoding, byte: u8) -> bool {
    if encoding.is_single_byte() {
        true
    } else if encoding == UTF_8 || encoding == EUC_JP {
        byte.is_ascii()
    } else if encoding == ISO_2022_JP {
        // Its two-byte characters are made of printable ASCII bytes, and its
        // decoder keeps state that does not survive decoding field by field.
        false
    } else {
        never_collides_with_a_trail_byte(byte)
    }
}

/// Whether `byte` never occurs inside a multi-byte character of Shift_JIS,
/// GBK, gb18030, Big5 or EUC-KR.
///
/// The trail bytes of those encodings start at 0x40, except for the digits
/// 0x30 to 0x39 that gb18030 uses in its four-byte form.
pub(crate) fn never_collides_with_a_trail_byte(byte: u8) -> bool {
    byte < 0x30 || (0x3A..=0x3F).contains(&byte)
}

/// Where the scanner is within a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// At the start of a field: a quote here opens a quoted field.
    FieldStart,

    /// Inside a field that is not quoted, or after the closing quote of one
    /// that was. A quote here is ordinary text.
    Unquoted,

    /// Inside a quoted field. Only a quote is special.
    Quoted,

    /// Just after a quote inside a quoted field. A second quote makes it an
    /// escaped quote, anything else makes it the closing quote.
    QuoteInQuoted,

    /// Just after a carriage return that ended the record. A line feed here
    /// belongs to that same terminator.
    AfterCarriageReturn,
}

/// What one code unit turned out to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// Part of the current field's value.
    Literal,

    /// A quote that encloses a field, or a carriage return that ends the
    /// record. Not part of any value.
    Structural,

    /// The delimiter that ends the current field.
    FieldEnd,

    /// The last unit of the record.
    RecordEnd,

    /// The first unit of the next record: the record ended just before it.
    NextRecord,
}

/// Advances the scanner over one code unit.
fn step(layout: &Layout, state: &mut State, unit: u16) -> Step {
    let is_quote = layout.quote == Some(unit);

    match *state {
        State::AfterCarriageReturn => {
            if unit == LINE_FEED {
                Step::RecordEnd
            } else {
                Step::NextRecord
            }
        }

        State::Quoted => {
            if is_quote {
                *state = State::QuoteInQuoted;
                Step::Structural
            } else {
                Step::Literal
            }
        }

        State::FieldStart | State::Unquoted | State::QuoteInQuoted => {
            if unit == LINE_FEED {
                Step::RecordEnd
            } else if unit == CARRIAGE_RETURN {
                *state = State::AfterCarriageReturn;
                Step::Structural
            } else if unit == layout.delimiter {
                *state = State::FieldStart;
                Step::FieldEnd
            } else if is_quote && *state == State::FieldStart {
                *state = State::Quoted;
                Step::Structural
            } else if is_quote && *state == State::QuoteInQuoted {
                *state = State::Quoted;
                Step::Literal
            } else {
                *state = State::Unquoted;
                Step::Literal
            }
        }
    }
}

/// Returns the length in bytes, terminator included, of the record that starts
/// at the beginning of `bytes`.
///
/// `at_end_of_source` says whether `bytes` runs to the end of the source. When
/// it does not and the record's end is not settled within `bytes` (no
/// terminator yet, a carriage return that a line feed may still follow, or
/// half a code unit), the result is `None` and the caller must supply more
/// bytes. At the end of the source, whatever is left is the final record.
pub(crate) fn record_length(
    bytes: &[u8],
    layout: &Layout,
    at_end_of_source: bool,
) -> Option<usize> {
    let unit_size = layout.unit_size();

    let mut state = State::FieldStart;
    let mut position = 0;

    while let Some(unit) = layout.unit_at(bytes, position) {
        match step(layout, &mut state, unit) {
            Step::RecordEnd => return Some(position + unit_size),
            Step::NextRecord => return Some(position),
            Step::Literal | Step::Structural | Step::FieldEnd => {}
        }

        position += unit_size;
    }

    (at_end_of_source && !bytes.is_empty()).then_some(bytes.len())
}

/// Splits the bytes of exactly one record into the encoded bytes of each
/// field's value, without enclosing quotes or the terminator.
fn split_fields(record: &[u8], layout: &Layout) -> Vec<Vec<u8>> {
    let unit_size = layout.unit_size();

    let mut fields = Vec::new();
    let mut current = Vec::new();

    let mut state = State::FieldStart;
    let mut position = 0;

    while let Some(unit) = layout.unit_at(record, position) {
        match step(layout, &mut state, unit) {
            Step::Literal => {
                current.extend_from_slice(
                    record
                        .get(position..position + unit_size)
                        .unwrap_or_default(),
                );
            }

            Step::Structural => {}

            Step::FieldEnd => fields.push(std::mem::take(&mut current)),

            Step::RecordEnd | Step::NextRecord => {
                position = record.len();
                break;
            }
        }

        position += unit_size;
    }

    // Half a UTF-16 code unit at the very end of the source is kept, so that
    // decoding reports it instead of dropping it.
    current.extend_from_slice(record.get(position..).unwrap_or_default());
    fields.push(current);

    fields
}

/// Whether the bytes of exactly one record end inside a quoted field that was
/// never closed, which only the final record of a source can do.
pub(crate) fn ends_inside_quotes(record: &[u8], layout: &Layout) -> bool {
    let unit_size = layout.unit_size();

    let mut state = State::FieldStart;
    let mut position = 0;

    while let Some(unit) = layout.unit_at(record, position) {
        step(layout, &mut state, unit);
        position += unit_size;
    }

    state == State::Quoted
}

/// Returns the byte-order mark that `encoding` defines, or an empty slice.
pub(crate) fn byte_order_mark(encoding: &'static Encoding) -> &'static [u8] {
    if encoding == UTF_8 {
        b"\xEF\xBB\xBF"
    } else if encoding == UTF_16LE {
        b"\xFF\xFE"
    } else if encoding == UTF_16BE {
        b"\xFE\xFF"
    } else {
        b""
    }
}

pub(crate) fn to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// A read position in the source with the bytes fetched from there on.
#[derive(Debug, Default)]
struct Cursor {
    /// The source offset of the first byte of `buffer`.
    buffer_start: u64,

    buffer: Vec<u8>,

    /// How many bytes at the front of `buffer` belong to records already
    /// handed out.
    consumed: usize,
}

impl Cursor {
    fn at(offset: u64) -> Self {
        Self {
            buffer_start: offset,
            buffer: Vec::new(),
            consumed: 0,
        }
    }

    /// The source offset where the next record starts.
    fn offset(&self) -> u64 {
        self.buffer_start + to_u64(self.consumed)
    }

    /// The source offset just past the last fetched byte.
    fn buffer_end(&self) -> u64 {
        self.buffer_start + to_u64(self.buffer.len())
    }

    /// The fetched bytes from the next record on.
    fn remaining(&self) -> &[u8] {
        self.buffer.get(self.consumed..).unwrap_or_default()
    }

    fn consume(&mut self, length: usize) {
        self.consumed = (self.consumed + length).min(self.buffer.len());
    }

    /// Drops the consumed bytes and adds `bytes`, which must start at
    /// [`Cursor::buffer_end`].
    fn append(&mut self, bytes: &[u8]) {
        self.buffer_start = self.offset();
        self.buffer.drain(..self.consumed);
        self.consumed = 0;

        self.buffer.extend_from_slice(bytes);
    }
}

/// The bytes of a page's leading records, kept while they fit in a budget.
struct KeptBytes {
    budget: usize,
    bytes: Vec<u8>,

    /// Set once a record did not fit: no later record is kept, so the bytes
    /// never skip one.
    full: bool,
}

impl KeptBytes {
    /// Keeps `record`, the bytes of the next record, when it fits.
    fn offer(&mut self, record: &[u8]) {
        if self.full || self.bytes.len().saturating_add(record.len()) > self.budget {
            self.full = true;
            return;
        }

        self.bytes.extend_from_slice(record);
    }
}

/// Reads a delimited source one page of records at a time.
///
/// The reader keeps one byte offset per page it has scanned, not one per
/// record, so its memory does not grow with the size of a page's records and
/// grows by eight bytes per page. Offsets are found by scanning forward:
/// reading page N scans from the last page already indexed up to N and
/// remembers every page start on the way, and reading any page at or before
/// that point afterwards goes straight to its offset.
///
/// All methods block on the source.
#[derive(Debug)]
pub struct PagedReader<S> {
    source: S,
    dialect: Dialect,
    layout: Layout,
    options: ReaderOptions,

    /// The length of the source in bytes, as of opening or the last
    /// invalidation.
    length: u64,

    byte_order_mark_length: u64,
    header: Option<Record>,

    /// The source bytes of `header`, terminator included.
    header_bytes: Option<Vec<u8>>,

    /// `page_starts[n]` is the source offset of the first record of page `n`.
    /// Never empty: entry 0 is where the data starts, after the byte-order
    /// mark and the header.
    page_starts: Vec<u64>,

    indexed_records: u64,
    total_records: Option<u64>,

    /// Where the last read stopped, with the bytes it fetched past that
    /// point, so that reading the following page does not fetch them again.
    cursor: Cursor,
}

impl<S: ByteSource> PagedReader<S> {
    /// Opens `source` as a file of `dialect`.
    ///
    /// This reads the source's length and, when the encoding defines a
    /// byte-order mark or the dialect has a header, the first window of the
    /// source. A leading byte-order mark of the dialect's own encoding is
    /// skipped, and with `has_header` the first record becomes the header.
    ///
    /// A dialect change needs a new reader: take the source back with
    /// [`PagedReader::into_source`] and open it again.
    ///
    /// # Errors
    ///
    /// [`ReadError::UnsupportedDialect`] when the delimiter or the quote
    /// cannot be told apart from part of a multi-byte character: any
    /// non-ASCII byte in UTF-8 or EUC-JP, a byte from 0x30 to 0x39 or from
    /// 0x40 up in Shift_JIS, GBK, gb18030, Big5 and EUC-KR, and every byte in
    /// ISO-2022-JP. Single-byte encodings and UTF-16 accept any byte.
    ///
    /// [`ReadError::LineBreakInDialect`] when the delimiter or the quote is a
    /// line feed or a carriage return, and [`ReadError::QuoteEqualsDelimiter`]
    /// when the quote is the delimiter, in every encoding.
    pub fn open(source: S, dialect: Dialect, options: ReaderOptions) -> Result<Self, ReadError> {
        let layout = Layout::for_dialect(&dialect)?;

        let mut reader = Self {
            source,
            dialect,
            layout,
            options,
            length: 0,
            byte_order_mark_length: 0,
            header: None,
            header_bytes: None,
            page_starts: vec![0],
            indexed_records: 0,
            total_records: None,
            cursor: Cursor::default(),
        };

        reader.prepare()?;

        Ok(reader)
    }

    /// The header record, when the dialect has a header and the source is not
    /// empty. It is never part of a page.
    pub fn header(&self) -> Option<&Record> {
        self.header.as_ref()
    }

    /// The source bytes of the header record, terminator included, when there
    /// is one. They were fetched to read the header, so this reads nothing.
    pub fn header_bytes(&self) -> Option<&[u8]> {
        self.header_bytes.as_deref()
    }

    /// The number of data records, exact once a scan has reached the end of
    /// the source and a lower bound until then. The header is not counted.
    pub fn record_count(&self) -> RecordCount {
        match self.total_records {
            Some(total) => RecordCount::Total(total),
            None => RecordCount::IndexedSoFar(self.indexed_records),
        }
    }

    /// The length of the byte-order mark at the start of the source, or zero.
    /// Those bytes belong to no record.
    pub fn byte_order_mark_length(&self) -> u64 {
        self.byte_order_mark_length
    }

    pub fn dialect(&self) -> &Dialect {
        &self.dialect
    }

    pub(crate) fn layout(&self) -> &Layout {
        &self.layout
    }

    /// The length of the source in bytes, as of opening or the last
    /// invalidation. Every byte range this reader returns was read against a
    /// source of this length, which is what [`crate::EditSet::new`] takes.
    pub fn source_length(&self) -> u64 {
        self.length
    }

    /// Calls `visit` with the source offset and the raw bytes, terminator
    /// included, of every record in source order. The header is the first
    /// record visited. The index and the read position are left untouched.
    pub(crate) fn visit_raw_records<E: From<ReadError>>(
        &self,
        mut visit: impl FnMut(u64, &[u8]) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut cursor = Cursor::at(self.byte_order_mark_length);

        while let Some(length) = self.next_record(&mut cursor)? {
            let record = cursor.remaining().get(..length).unwrap_or_default();

            visit(cursor.offset(), record)?;
            cursor.consume(length);
        }

        Ok(())
    }

    pub fn source(&self) -> &S {
        &self.source
    }

    /// Gives mutable access to the source. After changing its content, call
    /// [`PagedReader::invalidate_from_offset`] before reading again.
    pub fn source_mut(&mut self) -> &mut S {
        &mut self.source
    }

    pub fn into_source(self) -> S {
        self.source
    }

    /// Reads data page `page`, counting from zero.
    ///
    /// A page that is not indexed yet is reached by scanning forward from the
    /// last indexed page, which fetches every byte in between once. A page
    /// past the end of the source has no records, and finding that out
    /// settles the total [`PagedReader::record_count`].
    pub fn read_page(&mut self, page: usize) -> Result<Page, ReadError> {
        self.read_page_keeping(page, 0).map(|(page, _)| page)
    }

    /// Reads data page `page` as [`PagedReader::read_page`] does, and returns
    /// with it the source bytes, terminators included, of the page's leading
    /// records whose bytes together fit in `budget` bytes: the bytes from the
    /// start of its first record to the end of the last one kept. They are
    /// taken from the bytes the read fetched anyway, so this reads exactly
    /// what [`PagedReader::read_page`] reads. Empty for a page without
    /// records, and when its first record alone is longer than `budget`.
    pub fn read_page_with_bytes(
        &mut self,
        page: usize,
        budget: usize,
    ) -> Result<(Page, Vec<u8>), ReadError> {
        self.read_page_keeping(page, budget)
    }

    fn read_page_keeping(
        &mut self,
        page: usize,
        budget: usize,
    ) -> Result<(Page, Vec<u8>), ReadError> {
        let first_record = self.first_record_of(page);

        if page >= self.page_starts.len() && self.total_records.is_some() {
            return Ok((
                Page {
                    first_record,
                    records: Vec::new(),
                },
                Vec::new(),
            ));
        }

        let nearest = self.page_starts.len().saturating_sub(1).min(page);
        let start = self.page_starts.get(nearest).copied().unwrap_or_default();

        let mut cursor = std::mem::take(&mut self.cursor);

        if cursor.offset() != start {
            cursor = Cursor::at(start);
        }

        for skipped in nearest..page {
            if self.page_starts.len() <= skipped {
                break;
            }

            self.scan_page(skipped, &mut cursor, None)?;
        }

        let mut kept = KeptBytes {
            budget,
            bytes: Vec::new(),
            full: budget == 0,
        };

        let records = if page < self.page_starts.len() {
            self.scan_page(page, &mut cursor, Some(&mut kept))?
        } else {
            Vec::new()
        };

        self.cursor = cursor;

        Ok((
            Page {
                first_record,
                records,
            },
            kept.bytes,
        ))
    }

    /// Forgets everything indexed after byte `offset` of the source, and
    /// reads the source's length again.
    ///
    /// Call it after the source's content changed from `offset` on, for
    /// example with the start of the first record whose length changed. Pages
    /// that start at or before `offset` keep their place, so the next read
    /// rescans from the page that contains `offset`. The total becomes
    /// unknown again.
    ///
    /// Offset zero invalidates everything: the byte-order mark and the header
    /// are always read again, even when the source had neither before. So
    /// does any other `offset` inside the byte-order mark or the header, and
    /// a source that now ends at or before the first data byte.
    pub fn invalidate_from_offset(&mut self, offset: u64) -> Result<(), ReadError> {
        let data_start = self.page_starts.first().copied().unwrap_or_default();

        if offset == 0 || offset < data_start {
            return self.prepare();
        }

        self.length = self.source.byte_length()?;

        if data_start >= self.length {
            return self.prepare();
        }

        let kept_pages = self.page_starts.partition_point(|start| *start <= offset);
        self.page_starts.truncate(kept_pages);

        self.indexed_records = self.first_record_of(kept_pages.saturating_sub(1));
        self.total_records = None;
        self.cursor = Cursor::default();

        Ok(())
    }

    /// Forgets everything indexed after the start of page `page`, as
    /// [`PagedReader::invalidate_from_offset`] does for that page's first
    /// byte. A page that is not indexed yet only resets the total.
    ///
    /// Page zero invalidates everything, exactly as offset zero does: the
    /// byte-order mark and the header are read again.
    pub fn invalidate_from_page(&mut self, page: usize) -> Result<(), ReadError> {
        if page == 0 {
            return self.prepare();
        }

        let offset = self
            .page_starts
            .get(page)
            .or(self.page_starts.last())
            .copied()
            .unwrap_or_default();

        self.invalidate_from_offset(offset)
    }

    /// Reads the length, the byte-order mark and the header, and resets the
    /// index to an unscanned source.
    fn prepare(&mut self) -> Result<(), ReadError> {
        self.length = self.source.byte_length()?;

        let mut cursor = Cursor::default();
        let mark = byte_order_mark(self.dialect.encoding);

        while cursor.remaining().len() < mark.len() && cursor.buffer_end() < self.length {
            self.extend(&mut cursor, self.options.window_size.get())?;
        }

        if !mark.is_empty() && cursor.remaining().starts_with(mark) {
            cursor.consume(mark.len());
        }

        self.byte_order_mark_length = cursor.offset();
        self.header = None;
        self.header_bytes = None;

        if self.dialect.has_header
            && let Some(length) = self.next_record(&mut cursor)?
        {
            self.header = Some(self.build_record(&cursor, length));
            self.header_bytes = Some(
                cursor
                    .remaining()
                    .get(..length)
                    .unwrap_or_default()
                    .to_vec(),
            );
            cursor.consume(length);
        }

        let data_start = cursor.offset();

        self.page_starts = vec![data_start];
        self.indexed_records = 0;
        self.total_records = (data_start >= self.length).then_some(0);
        self.cursor = cursor;

        Ok(())
    }

    fn first_record_of(&self, page: usize) -> u64 {
        to_u64(page).saturating_mul(to_u64(self.options.page_size.get()))
    }

    /// Scans the records of `page` from `cursor`, which must be at the page's
    /// start, and records what the scan learned in the index. The records are
    /// built, and the leading ones' bytes kept in `kept`, only when `kept` is
    /// given, and skipped over otherwise.
    fn scan_page(
        &mut self,
        page: usize,
        cursor: &mut Cursor,
        mut kept: Option<&mut KeptBytes>,
    ) -> Result<Vec<Record>, ReadError> {
        let mut records = Vec::new();
        let mut scanned: u64 = 0;

        for _ in 0..self.options.page_size.get() {
            let Some(length) = self.next_record(cursor)? else {
                break;
            };

            if let Some(kept) = kept.as_deref_mut() {
                records.push(self.build_record(cursor, length));
                kept.offer(cursor.remaining().get(..length).unwrap_or_default());
            }

            cursor.consume(length);
            scanned += 1;
        }

        let records_through_page = self.first_record_of(page).saturating_add(scanned);
        self.indexed_records = self.indexed_records.max(records_through_page);

        if cursor.offset() >= self.length {
            self.total_records = Some(records_through_page);
        } else if self.page_starts.len() == page + 1 {
            self.page_starts.push(cursor.offset());
        }

        Ok(records)
    }

    /// Returns the length of the record at `cursor`, fetching more of the
    /// source until the record's end is settled, or `None` at the end of the
    /// source.
    fn next_record(&self, cursor: &mut Cursor) -> Result<Option<usize>, ReadError> {
        let mut request = self.options.window_size.get();

        loop {
            let at_end_of_source = cursor.buffer_end() >= self.length;

            let length = record_length(cursor.remaining(), &self.layout, at_end_of_source);

            if length.is_some() || at_end_of_source {
                return Ok(length);
            }

            self.extend(cursor, request)?;
            request = request.saturating_mul(2);
        }
    }

    /// Fetches up to `request` more bytes after the ones `cursor` holds.
    fn extend(&self, cursor: &mut Cursor, request: u64) -> Result<(), ReadError> {
        let start = cursor.buffer_end();
        let end = start.saturating_add(request).min(self.length);

        let bytes = self.source.read_range(start..end)?;

        if to_u64(bytes.len()) != end.saturating_sub(start) {
            return Err(ReadError::UnexpectedReadLength {
                start,
                end,
                received: bytes.len(),
            });
        }

        cursor.append(&bytes);

        Ok(())
    }

    /// Builds the record made of the next `length` bytes at `cursor`.
    fn build_record(&self, cursor: &Cursor, length: usize) -> Record {
        let start = cursor.offset();
        let bytes = cursor.remaining().get(..length).unwrap_or_default();

        let mut had_replacements = false;

        let fields = split_fields(bytes, &self.layout)
            .into_iter()
            .map(|field| {
                let (text, replaced) = self.dialect.encoding.decode_without_bom_handling(&field);
                had_replacements |= replaced;

                text.into_owned()
            })
            .collect();

        Record {
            byte_range: start..start + to_u64(length),
            fields,
            had_replacements,
        }
    }
}
