//! Streaming RDB (Redis dump file) parser and `DumpAnalyzer` implementation.
//!
//! The parser deliberately never decodes value payloads: it only reads
//! opcodes and key names, and skips every value byte-for-byte using the
//! length encoding embedded in the format. This keeps memory flat and lets a
//! multi-gigabyte dump be analyzed at I/O speed through a `BufReader`.
//!
//! Key names are the one thing materialized, including decompressing an
//! LZF-compressed key name — everything else (aggregate counts, ziplists,
//! listpacks, scores, stream payloads) is skipped without allocation beyond
//! a bounded scratch buffer.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

use dbflux_core::{
    DumpAnalysisError, DumpAnalysisReport, DumpAnalyzer, DumpKeyEntry, DumpPrefixEntry,
};

const TOP_N_LARGEST_KEYS: usize = 500;
const MAX_PREFIX_BUCKETS: usize = 10_000;
const MAX_SHORT_KEY_AS_OWN_PREFIX: usize = 32;
const MAX_KEY_NAME_BYTES: usize = 16 * 1024 * 1024;
const PROGRESS_EVERY_ENTRIES: u64 = 4096;
const PROGRESS_EVERY_BYTES: u64 = 4 * 1024 * 1024;

const OPCODE_SLOT_INFO: u8 = 0xF4;
const OPCODE_FUNCTION2: u8 = 0xF5;
const OPCODE_FUNCTION_PRE_GA: u8 = 0xF6;
const OPCODE_MODULE_AUX: u8 = 0xF7;
const OPCODE_IDLE: u8 = 0xF8;
const OPCODE_FREQ: u8 = 0xF9;
const OPCODE_AUX: u8 = 0xFA;
const OPCODE_RESIZEDB: u8 = 0xFB;
const OPCODE_EXPIRETIME_MS: u8 = 0xFC;
const OPCODE_EXPIRETIME: u8 = 0xFD;
const OPCODE_SELECTDB: u8 = 0xFE;
const OPCODE_EOF: u8 = 0xFF;

/// Analyzes Redis RDB dump files without connecting to a live server.
pub struct RdbAnalyzer;

impl DumpAnalyzer for RdbAnalyzer {
    fn display_name(&self) -> &'static str {
        "Redis RDB"
    }

    fn file_extensions(&self) -> &'static [&'static str] {
        &["rdb"]
    }

    fn size_caveat(&self) -> &'static str {
        "Sizes reflect each key's serialized footprint in the RDB file, not its footprint in \
         live Redis memory. Allocator overhead and Redis's in-memory encodings (ziplists, \
         listpacks, hash tables) mean the two numbers diverge, sometimes significantly."
    }

    fn analyze(
        &self,
        path: &Path,
        progress: &(dyn Fn(u64, Option<u64>) + Sync),
        cancelled: &(dyn Fn() -> bool + Sync),
    ) -> Result<DumpAnalysisReport, DumpAnalysisError> {
        let file = File::open(path).map_err(|error| DumpAnalysisError::Io(error.to_string()))?;
        let total_bytes = file.metadata().ok().map(|metadata| metadata.len());
        let reader = BufReader::new(file);

        analyze_reader(reader, total_bytes, progress, cancelled)
    }
}

/// Parses an RDB byte stream, aggregating results with bounded memory.
///
/// Split out from `RdbAnalyzer::analyze` so tests can feed hand-built
/// in-memory fixtures (`std::io::Cursor<Vec<u8>>`) without touching the
/// filesystem.
fn analyze_reader<R: Read>(
    reader: R,
    total_bytes: Option<u64>,
    progress: &(dyn Fn(u64, Option<u64>) + Sync),
    cancelled: &(dyn Fn() -> bool + Sync),
) -> Result<DumpAnalysisReport, DumpAnalysisError> {
    let mut reader = RdbReader::new(reader);
    parse_header(&mut reader)?;

    let mut aggregator = Aggregator::new();
    let mut current_db: u32 = 0;
    let mut pending_expire_ms: Option<i64> = None;
    let mut entry_count: u64 = 0;
    let mut last_progress_offset: u64 = 0;

    loop {
        if cancelled() {
            return Err(DumpAnalysisError::Cancelled);
        }

        let entry_start = reader.offset();
        let opcode = reader.read_u8()?;

        match opcode {
            OPCODE_EOF => {
                reader.try_skip_checksum()?;
                progress(reader.offset(), total_bytes);
                return Ok(aggregator.into_report());
            }
            OPCODE_SELECTDB => {
                current_db = reader.read_length()? as u32;
            }
            OPCODE_RESIZEDB => {
                reader.read_length()?;
                reader.read_length()?;
            }
            OPCODE_AUX => {
                reader.skip_string()?;
                reader.skip_string()?;
            }
            OPCODE_EXPIRETIME => {
                let seconds = u32::from_le_bytes(reader.read_array::<4>()?);
                pending_expire_ms = Some(i64::from(seconds) * 1000);
            }
            OPCODE_EXPIRETIME_MS => {
                let millis = u64::from_le_bytes(reader.read_array::<8>()?);
                pending_expire_ms = Some(millis as i64);
            }
            OPCODE_IDLE => {
                reader.read_length()?;
            }
            OPCODE_FREQ => {
                reader.read_u8()?;
            }
            OPCODE_MODULE_AUX | OPCODE_FUNCTION_PRE_GA | OPCODE_FUNCTION2 | OPCODE_SLOT_INFO => {
                return Err(DumpAnalysisError::Format {
                    offset: entry_start,
                    message: format!("unsupported opcode 0x{opcode:02X} is not implemented"),
                });
            }
            type_byte => {
                let key_bytes = reader.read_string(MAX_KEY_NAME_BYTES)?;
                skip_value(&mut reader, type_byte, entry_start)?;

                let serialized_bytes = reader.offset() - entry_start;
                aggregator.record(DumpKeyEntry {
                    key: String::from_utf8_lossy(&key_bytes).into_owned(),
                    type_name: type_name_for(type_byte),
                    serialized_bytes,
                    expires_at_ms: pending_expire_ms.take(),
                    database: current_db,
                });
            }
        }

        entry_count += 1;
        let should_report_by_count = entry_count.is_multiple_of(PROGRESS_EVERY_ENTRIES);
        let should_report_by_bytes = reader.offset() - last_progress_offset >= PROGRESS_EVERY_BYTES;
        if should_report_by_count || should_report_by_bytes {
            progress(reader.offset(), total_bytes);
            last_progress_offset = reader.offset();
        }
    }
}

fn parse_header<R: Read>(reader: &mut RdbReader<R>) -> Result<(), DumpAnalysisError> {
    let header = reader.read_array::<9>()?;

    if &header[..5] != b"REDIS" {
        return Err(DumpAnalysisError::Format {
            offset: 0,
            message: "missing REDIS magic header".to_string(),
        });
    }

    if !header[5..].iter().all(u8::is_ascii_digit) {
        return Err(DumpAnalysisError::Format {
            offset: 5,
            message: "RDB version field is not ASCII digits".to_string(),
        });
    }

    Ok(())
}

/// Maps an RDB value-type byte to the coarse type name shown in the report.
fn type_name_for(type_byte: u8) -> String {
    match type_byte {
        0 => "string",
        1 | 10 | 14 | 18 => "list",
        2 | 11 | 20 => "set",
        3 | 5 | 12 | 17 => "zset",
        4 | 9 | 13 | 16 => "hash",
        15 | 19 | 21 => "stream",
        other => return format!("unknown-0x{other:02X}"),
    }
    .to_string()
}

/// Skips the value payload for `type_byte`, advancing the reader without
/// materializing any of it. `entry_start` is only used to point a `Format`
/// error at the type byte that introduced the entry.
fn skip_value<R: Read>(
    reader: &mut RdbReader<R>,
    type_byte: u8,
    entry_start: u64,
) -> Result<(), DumpAnalysisError> {
    match type_byte {
        0 => reader.skip_string()?,

        1 | 2 => skip_string_list(reader, 1)?,
        4 => skip_string_list(reader, 2)?,

        3 => {
            let count = reader.read_length()?;
            for _ in 0..count {
                reader.skip_string()?;
                reader.skip_ascii_double()?;
            }
        }
        5 => {
            let count = reader.read_length()?;
            for _ in 0..count {
                reader.skip_string()?;
                reader.skip(8)?;
            }
        }

        9 | 10 | 11 | 12 | 13 | 16 | 17 | 20 => reader.skip_string()?,

        14 => {
            let node_count = reader.read_length()?;
            for _ in 0..node_count {
                reader.skip_string()?;
            }
        }
        18 => {
            let node_count = reader.read_length()?;
            for _ in 0..node_count {
                reader.read_length()?;
                reader.skip_string()?;
            }
        }

        15 | 19 | 21 => skip_stream(reader, type_byte)?,

        other => {
            return Err(DumpAnalysisError::Format {
                offset: entry_start,
                message: format!("unknown value type 0x{other:02X}"),
            });
        }
    }

    Ok(())
}

fn skip_string_list<R: Read>(
    reader: &mut RdbReader<R>,
    strings_per_element: u64,
) -> Result<(), DumpAnalysisError> {
    let count = reader.read_length()?;
    for _ in 0..(count * strings_per_element) {
        reader.skip_string()?;
    }
    Ok(())
}

/// Skips an RDB stream value (types 15/19/21), which is otherwise-unrelated
/// structural data rather than a single blob: listpack entries, stream
/// metadata, consumer groups, and two flavors of PEL (pending entries list).
///
/// Types 19 and 21 (STREAM_LISTPACKS_2/_3) add extra length-encoded metadata
/// fields over type 15, and type 21 additionally stores a per-consumer
/// "active time" timestamp. None of this data is decoded — only its shape is
/// walked so the reader ends up exactly at the start of the next opcode.
fn skip_stream<R: Read>(reader: &mut RdbReader<R>, type_byte: u8) -> Result<(), DumpAnalysisError> {
    let is_v2_or_v3 = type_byte != 15;

    let listpack_count = reader.read_length()?;
    for _ in 0..listpack_count {
        reader.skip_string()?; // master stream ID, packed as a 16-byte string
        reader.skip_string()?; // listpack blob
    }

    reader.read_length()?; // items count
    reader.read_length()?; // last_id.ms
    reader.read_length()?; // last_id.seq

    if is_v2_or_v3 {
        reader.read_length()?; // first_id.ms
        reader.read_length()?; // first_id.seq
        reader.read_length()?; // max_deleted_entry_id.ms
        reader.read_length()?; // max_deleted_entry_id.seq
        reader.read_length()?; // entries_added
    }

    let group_count = reader.read_length()?;
    for _ in 0..group_count {
        skip_stream_group(reader, type_byte, is_v2_or_v3)?;
    }

    Ok(())
}

fn skip_stream_group<R: Read>(
    reader: &mut RdbReader<R>,
    type_byte: u8,
    is_v2_or_v3: bool,
) -> Result<(), DumpAnalysisError> {
    reader.skip_string()?; // group name
    reader.read_length()?; // g_ms
    reader.read_length()?; // g_seq
    if is_v2_or_v3 {
        reader.read_length()?; // entries_read
    }

    let global_pel_count = reader.read_length()?;
    for _ in 0..global_pel_count {
        reader.skip(16)?; // stream ID, raw (not length-encoded)
        reader.skip(8)?; // delivery time ms, raw little-endian
        reader.read_length()?; // delivery count
    }

    let consumer_count = reader.read_length()?;
    for _ in 0..consumer_count {
        reader.skip_string()?; // consumer name
        reader.skip(8)?; // seen time ms, raw little-endian
        if type_byte == 21 {
            reader.skip(8)?; // active time ms, raw little-endian (v3 only)
        }

        let consumer_pel_count = reader.read_length()?;
        for _ in 0..consumer_pel_count {
            reader.skip(16)?; // stream ID only, raw (delivery info lives in the global PEL)
        }
    }

    Ok(())
}

/// A length-or-encoding value read from the RDB length prefix.
enum StringEncoding {
    /// A plain byte length: the string that follows is exactly this many bytes.
    Len(u64),
    /// An 8-bit signed integer stored as a string.
    Int8,
    /// A 16-bit little-endian signed integer stored as a string.
    Int16,
    /// A 32-bit little-endian signed integer stored as a string.
    Int32,
    /// An LZF-compressed string.
    Lzf,
}

/// Wraps a `Read` implementation with absolute byte-offset tracking.
///
/// Offset tracking uses only forward reads (never `Seek`), so it works
/// uniformly over files, pipes, and in-memory cursors alike.
struct RdbReader<R> {
    inner: R,
    offset: u64,
}

impl<R: Read> RdbReader<R> {
    fn new(inner: R) -> Self {
        Self { inner, offset: 0 }
    }

    fn offset(&self) -> u64 {
        self.offset
    }

    fn map_io_error(&self, error: io::Error) -> DumpAnalysisError {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            DumpAnalysisError::Format {
                offset: self.offset,
                message: "unexpected end of file".to_string(),
            }
        } else {
            DumpAnalysisError::Io(error.to_string())
        }
    }

    fn read_u8(&mut self) -> Result<u8, DumpAnalysisError> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], DumpAnalysisError> {
        let mut buf = [0u8; N];
        self.inner
            .read_exact(&mut buf)
            .map_err(|error| self.map_io_error(error))?;
        self.offset += N as u64;
        Ok(buf)
    }

    /// Discards `n` bytes from the stream without allocating a buffer of
    /// that size, tracking the offset as it goes.
    fn skip(&mut self, n: u64) -> Result<(), DumpAnalysisError> {
        let copied = io::copy(&mut (&mut self.inner).take(n), &mut io::sink())
            .map_err(|error| self.map_io_error(error))?;
        self.offset += copied;

        if copied != n {
            return Err(DumpAnalysisError::Format {
                offset: self.offset,
                message: "unexpected end of file while skipping a value".to_string(),
            });
        }

        Ok(())
    }

    fn try_skip_checksum(&mut self) -> Result<(), DumpAnalysisError> {
        let mut buf = [0u8; 8];
        match self.inner.read_exact(&mut buf) {
            Ok(()) => {
                self.offset += 8;
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(()),
            Err(error) => Err(DumpAnalysisError::Io(error.to_string())),
        }
    }

    fn read_length_or_encoding(&mut self) -> Result<StringEncoding, DumpAnalysisError> {
        let byte = self.read_u8()?;

        match byte >> 6 {
            0b00 => Ok(StringEncoding::Len(u64::from(byte & 0x3F))),
            0b01 => {
                let low = self.read_u8()?;
                Ok(StringEncoding::Len(
                    (u64::from(byte & 0x3F) << 8) | u64::from(low),
                ))
            }
            0b10 if byte == 0x80 => {
                let bytes = self.read_array::<4>()?;
                Ok(StringEncoding::Len(u64::from(u32::from_be_bytes(bytes))))
            }
            0b10 if byte == 0x81 => {
                let bytes = self.read_array::<8>()?;
                Ok(StringEncoding::Len(u64::from_be_bytes(bytes)))
            }
            0b10 => Err(DumpAnalysisError::Format {
                offset: self.offset - 1,
                message: format!("unsupported length prefix 0x{byte:02X}"),
            }),
            _ => match byte & 0x3F {
                0 => Ok(StringEncoding::Int8),
                1 => Ok(StringEncoding::Int16),
                2 => Ok(StringEncoding::Int32),
                3 => Ok(StringEncoding::Lzf),
                other => Err(DumpAnalysisError::Format {
                    offset: self.offset - 1,
                    message: format!("unsupported special string encoding {other}"),
                }),
            },
        }
    }

    /// Reads a plain length prefix, used for counts, database numbers, and
    /// other non-string integers. Returns an error if the prefix turns out
    /// to be a special string encoding, which is never valid in these contexts.
    fn read_length(&mut self) -> Result<u64, DumpAnalysisError> {
        match self.read_length_or_encoding()? {
            StringEncoding::Len(len) => Ok(len),
            _ => Err(DumpAnalysisError::Format {
                offset: self.offset,
                message: "expected a plain length, found a special string encoding".to_string(),
            }),
        }
    }

    /// Advances past a string without materializing it. Never allocates a
    /// buffer proportional to the string's length, so no length cap applies.
    fn skip_string(&mut self) -> Result<(), DumpAnalysisError> {
        match self.read_length_or_encoding()? {
            StringEncoding::Len(len) => self.skip(len),
            StringEncoding::Int8 => {
                self.read_u8()?;
                Ok(())
            }
            StringEncoding::Int16 => {
                self.read_array::<2>()?;
                Ok(())
            }
            StringEncoding::Int32 => {
                self.read_array::<4>()?;
                Ok(())
            }
            StringEncoding::Lzf => {
                let compressed_len = self.read_length()?;
                self.read_length()?;
                self.skip(compressed_len)
            }
        }
    }

    fn read_string(&mut self, cap: usize) -> Result<Vec<u8>, DumpAnalysisError> {
        match self.read_length_or_encoding()? {
            StringEncoding::Len(len) => self.read_raw_bytes(len, cap),
            StringEncoding::Int8 => {
                let byte = self.read_u8()?;
                Ok((byte as i8).to_string().into_bytes())
            }
            StringEncoding::Int16 => {
                let bytes = self.read_array::<2>()?;
                Ok(i16::from_le_bytes(bytes).to_string().into_bytes())
            }
            StringEncoding::Int32 => {
                let bytes = self.read_array::<4>()?;
                Ok(i32::from_le_bytes(bytes).to_string().into_bytes())
            }
            StringEncoding::Lzf => self.read_lzf_string(cap),
        }
    }

    fn read_raw_bytes(&mut self, len: u64, cap: usize) -> Result<Vec<u8>, DumpAnalysisError> {
        if len as usize > cap {
            return Err(DumpAnalysisError::Format {
                offset: self.offset,
                message: format!("string length {len} exceeds the {cap}-byte cap"),
            });
        }

        let mut buf = vec![0u8; len as usize];
        self.inner
            .read_exact(&mut buf)
            .map_err(|error| self.map_io_error(error))?;
        self.offset += len;
        Ok(buf)
    }

    fn read_lzf_string(&mut self, cap: usize) -> Result<Vec<u8>, DumpAnalysisError> {
        let compressed_len = self.read_length()?;
        let uncompressed_len = self.read_length()?;

        if uncompressed_len as usize > cap {
            return Err(DumpAnalysisError::Format {
                offset: self.offset,
                message: format!(
                    "LZF uncompressed length {uncompressed_len} exceeds the {cap}-byte cap"
                ),
            });
        }

        let compressed = self.read_raw_bytes(compressed_len, cap)?;

        lzf_decompress(&compressed, uncompressed_len as usize, cap).map_err(|message| {
            DumpAnalysisError::Format {
                offset: self.offset,
                message,
            }
        })
    }

    /// Skips one RDB-encoded double, used by the old (type 3) zset format.
    ///
    /// A leading length byte of 253/254/255 means NaN/+Infinity/-Infinity
    /// with no further bytes; otherwise that many ASCII bytes follow.
    fn skip_ascii_double(&mut self) -> Result<(), DumpAnalysisError> {
        let len = self.read_u8()?;
        match len {
            253..=255 => Ok(()),
            len => self.skip(u64::from(len)),
        }
    }
}

/// Decompresses an LZF-compressed byte stream.
///
/// Implements the liblzf algorithm: a control byte either starts a literal
/// run (`ctrl < 32`, copy `ctrl + 1` raw bytes) or a back-reference
/// (`ctrl >= 32`, copy `len + 2` bytes from `offset` bytes behind the
/// current output position, where `len` and `offset` are packed across the
/// control byte and the one or two bytes that follow it).
fn lzf_decompress(input: &[u8], expected_len: usize, cap: usize) -> Result<Vec<u8>, String> {
    if expected_len > cap {
        return Err(format!(
            "declared LZF length {expected_len} exceeds the {cap}-byte cap"
        ));
    }

    let mut out: Vec<u8> = Vec::with_capacity(expected_len);
    let mut pos = 0usize;

    while pos < input.len() {
        let ctrl = usize::from(*input.get(pos).ok_or("LZF control byte missing")?);
        pos += 1;

        if ctrl < 32 {
            let len = ctrl + 1;
            let end = pos
                .checked_add(len)
                .ok_or("LZF literal run length overflow")?;
            let literal = input
                .get(pos..end)
                .ok_or("LZF literal run reads past end of compressed input")?;
            if out.len() + literal.len() > cap {
                return Err(format!("LZF output exceeds the {cap}-byte cap"));
            }
            out.extend_from_slice(literal);
            pos = end;
            continue;
        }

        let mut len = ctrl >> 5;
        if len == 7 {
            let extra = *input
                .get(pos)
                .ok_or("LZF back-reference length byte missing")?;
            len += usize::from(extra);
            pos += 1;
        }

        let offset_low = *input
            .get(pos)
            .ok_or("LZF back-reference offset byte missing")?;
        pos += 1;

        let offset = ((ctrl & 0x1F) << 8) + usize::from(offset_low) + 1;
        if offset > out.len() {
            return Err("LZF back-reference points before the start of the output".to_string());
        }

        let total_len = len + 2;
        if out.len() + total_len > cap {
            return Err(format!("LZF output exceeds the {cap}-byte cap"));
        }

        // Copies byte-by-byte (never a slice-range copy) because a back-reference
        // may overlap the bytes it is still writing, which is how LZF encodes runs.
        let start = out.len() - offset;
        for ref_pos in start..start + total_len {
            let byte = *out
                .get(ref_pos)
                .ok_or("LZF back-reference read out of bounds")?;
            out.push(byte);
        }
    }

    if out.len() != expected_len {
        return Err(format!(
            "LZF decompressed length {} does not match the declared length {expected_len}",
            out.len()
        ));
    }

    Ok(out)
}

/// Streaming aggregation state, bounded regardless of dump size.
struct Aggregator {
    total_keys: u64,
    total_serialized_bytes: u64,
    by_type: HashMap<String, (u64, u64)>,
    largest_keys: BinaryHeap<Reverse<HeapEntry>>,
    largest_keys_seq: u64,
    prefixes: HashMap<String, (u64, u64)>,
}

/// Wraps a `DumpKeyEntry` with a monotonic sequence number so the bounded
/// top-N heap has a total order even when two entries tie on size.
struct HeapEntry {
    serialized_bytes: u64,
    sequence: u64,
    entry: DumpKeyEntry,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.serialized_bytes == other.serialized_bytes && self.sequence == other.sequence
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.serialized_bytes
            .cmp(&other.serialized_bytes)
            .then(self.sequence.cmp(&other.sequence))
    }
}

impl Aggregator {
    fn new() -> Self {
        Self {
            total_keys: 0,
            total_serialized_bytes: 0,
            by_type: HashMap::new(),
            largest_keys: BinaryHeap::new(),
            largest_keys_seq: 0,
            prefixes: HashMap::new(),
        }
    }

    fn record(&mut self, entry: DumpKeyEntry) {
        self.total_keys += 1;
        self.total_serialized_bytes += entry.serialized_bytes;

        let type_totals = self
            .by_type
            .entry(entry.type_name.clone())
            .or_insert((0, 0));
        type_totals.0 += 1;
        type_totals.1 += entry.serialized_bytes;

        self.record_prefix(&entry.key, entry.serialized_bytes);
        self.record_top_n(entry);
    }

    fn record_prefix(&mut self, key: &str, serialized_bytes: u64) {
        let prefix = prefix_bucket_for(key);
        let overflowed =
            self.prefixes.len() >= MAX_PREFIX_BUCKETS && !self.prefixes.contains_key(&prefix);
        let bucket = if overflowed {
            "(other)".to_string()
        } else {
            prefix
        };

        let totals = self.prefixes.entry(bucket).or_insert((0, 0));
        totals.0 += 1;
        totals.1 += serialized_bytes;
    }

    fn record_top_n(&mut self, entry: DumpKeyEntry) {
        self.largest_keys_seq += 1;
        let candidate = HeapEntry {
            serialized_bytes: entry.serialized_bytes,
            sequence: self.largest_keys_seq,
            entry,
        };

        if self.largest_keys.len() < TOP_N_LARGEST_KEYS {
            self.largest_keys.push(Reverse(candidate));
            return;
        }

        let should_replace = self.largest_keys.peek().is_some_and(|Reverse(smallest)| {
            candidate.serialized_bytes > smallest.serialized_bytes
        });

        if should_replace {
            self.largest_keys.pop();
            self.largest_keys.push(Reverse(candidate));
        }
    }

    /// Consumes the aggregator into the final bounded report.
    ///
    /// `BinaryHeap::into_sorted_vec` sorts `Reverse<HeapEntry>` ascending,
    /// which is descending order for the wrapped `HeapEntry` — exactly the
    /// "largest first" order the report requires.
    fn into_report(self) -> DumpAnalysisReport {
        let mut keys_by_type: Vec<(String, u64, u64)> = self
            .by_type
            .into_iter()
            .map(|(type_name, (count, bytes))| (type_name, count, bytes))
            .collect();
        keys_by_type.sort_by_key(|(_, _, bytes)| Reverse(*bytes));

        let largest_keys: Vec<DumpKeyEntry> = self
            .largest_keys
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse(heap_entry)| heap_entry.entry)
            .collect();

        let mut prefix_rollup: Vec<DumpPrefixEntry> = self
            .prefixes
            .into_iter()
            .map(|(prefix, (key_count, serialized_bytes))| DumpPrefixEntry {
                prefix,
                key_count,
                serialized_bytes,
            })
            .collect();
        prefix_rollup.sort_by_key(|entry| Reverse(entry.serialized_bytes));

        DumpAnalysisReport {
            total_keys: self.total_keys,
            total_serialized_bytes: self.total_serialized_bytes,
            keys_by_type,
            largest_keys,
            prefix_rollup,
        }
    }
}

/// Computes the prefix bucket for a key name.
///
/// Splits on the first occurrence of any of `: / . |`, keeping the
/// separator as part of the bucket. Keys with no separator bucket under
/// their own full name when short (`<= 32` bytes), otherwise under
/// `"(no prefix)"`.
fn prefix_bucket_for(key: &str) -> String {
    match key.find([':', '/', '.', '|']) {
        Some(index) => key[..=index].to_string(),
        None if key.len() <= MAX_SHORT_KEY_AS_OWN_PREFIX => key.to_string(),
        None => "(no prefix)".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn no_progress(_bytes_read: u64, _total: Option<u64>) {}
    fn never_cancelled() -> bool {
        false
    }

    fn analyze_bytes(bytes: Vec<u8>) -> Result<DumpAnalysisReport, DumpAnalysisError> {
        analyze_reader(Cursor::new(bytes), None, &no_progress, &never_cancelled)
    }

    fn header() -> Vec<u8> {
        b"REDIS0011".to_vec()
    }

    /// Plain (non-special) length encoding for values `0..=0x3FFF`.
    fn encode_length(len: u64) -> Vec<u8> {
        if len < 64 {
            vec![len as u8]
        } else if len < 16384 {
            vec![0x40 | ((len >> 8) as u8), (len & 0xFF) as u8]
        } else {
            let mut out = vec![0x80];
            out.extend_from_slice(&(len as u32).to_be_bytes());
            out
        }
    }

    fn encode_raw_string(bytes: &[u8]) -> Vec<u8> {
        let mut out = encode_length(bytes.len() as u64);
        out.extend_from_slice(bytes);
        out
    }

    #[test]
    fn parses_a_minimal_dump_with_one_string_key() {
        let mut bytes = header();
        bytes.push(0); // string type
        bytes.extend(encode_raw_string(b"greeting"));
        bytes.extend(encode_raw_string(b"hello"));
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("valid dump must parse");

        assert_eq!(report.total_keys, 1);
        assert_eq!(
            report.keys_by_type,
            vec![("string".to_string(), 1, report.total_serialized_bytes)]
        );
        assert_eq!(report.largest_keys.len(), 1);
        assert_eq!(report.largest_keys[0].key, "greeting");
        assert_eq!(report.largest_keys[0].type_name, "string");
        assert_eq!(report.largest_keys[0].database, 0);
        assert_eq!(report.largest_keys[0].expires_at_ms, None);
    }

    #[test]
    fn parses_old_list_type() {
        let mut bytes = header();
        bytes.push(1); // list
        bytes.extend(encode_raw_string(b"mylist"));
        bytes.extend(encode_length(2)); // 2 elements
        bytes.extend(encode_raw_string(b"a"));
        bytes.extend(encode_raw_string(b"b"));
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("list dump must parse");
        assert_eq!(report.total_keys, 1);
        assert_eq!(report.largest_keys[0].type_name, "list");
    }

    #[test]
    fn parses_old_set_type() {
        let mut bytes = header();
        bytes.push(2); // set
        bytes.extend(encode_raw_string(b"myset"));
        bytes.extend(encode_length(2));
        bytes.extend(encode_raw_string(b"a"));
        bytes.extend(encode_raw_string(b"b"));
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("set dump must parse");
        assert_eq!(report.largest_keys[0].type_name, "set");
    }

    #[test]
    fn parses_old_hash_type() {
        let mut bytes = header();
        bytes.push(4); // hash
        bytes.extend(encode_raw_string(b"myhash"));
        bytes.extend(encode_length(1)); // 1 field/value pair -> 2 strings
        bytes.extend(encode_raw_string(b"field"));
        bytes.extend(encode_raw_string(b"value"));
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("hash dump must parse");
        assert_eq!(report.largest_keys[0].type_name, "hash");
    }

    #[test]
    fn parses_old_zset_type_with_ascii_scores() {
        let mut bytes = header();
        bytes.push(3); // zset (old)
        bytes.extend(encode_raw_string(b"myzset"));
        bytes.extend(encode_length(1));
        bytes.extend(encode_raw_string(b"member"));
        let score = b"1.5";
        bytes.push(score.len() as u8);
        bytes.extend_from_slice(score);
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("zset dump must parse");
        assert_eq!(report.largest_keys[0].type_name, "zset");
    }

    #[test]
    fn parses_zset2_type_with_binary_double() {
        let mut bytes = header();
        bytes.push(5); // zset_2
        bytes.extend(encode_raw_string(b"myzset2"));
        bytes.extend(encode_length(1));
        bytes.extend(encode_raw_string(b"member"));
        bytes.extend_from_slice(&1.5f64.to_le_bytes());
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("zset_2 dump must parse");
        assert_eq!(report.largest_keys[0].type_name, "zset");
    }

    #[test]
    fn parses_a_ziplist_blob_variant_as_a_single_string() {
        let mut bytes = header();
        bytes.push(10); // list encoded as ziplist blob
        bytes.extend(encode_raw_string(b"mylist_zl"));
        bytes.extend(encode_raw_string(b"fake-ziplist-bytes"));
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("ziplist-blob dump must parse");
        assert_eq!(report.largest_keys[0].type_name, "list");
    }

    #[test]
    fn parses_quicklist_as_n_string_nodes() {
        let mut bytes = header();
        bytes.push(14); // quicklist
        bytes.extend(encode_raw_string(b"myquicklist"));
        bytes.extend(encode_length(2)); // 2 nodes
        bytes.extend(encode_raw_string(b"node-ziplist-1"));
        bytes.extend(encode_raw_string(b"node-ziplist-2"));
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("quicklist dump must parse");
        assert_eq!(report.largest_keys[0].type_name, "list");
    }

    #[test]
    fn parses_quicklist2_with_per_node_container_flag() {
        let mut bytes = header();
        bytes.push(18); // quicklist2
        bytes.extend(encode_raw_string(b"myquicklist2"));
        bytes.extend(encode_length(1)); // 1 node
        bytes.extend(encode_length(2)); // container flag (PACKED)
        bytes.extend(encode_raw_string(b"node-listpack"));
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("quicklist2 dump must parse");
        assert_eq!(report.largest_keys[0].type_name, "list");
    }

    #[test]
    fn records_expiretime_ms_on_the_following_key() {
        let mut bytes = header();
        bytes.push(OPCODE_EXPIRETIME_MS);
        bytes.extend_from_slice(&1_700_000_000_000u64.to_le_bytes());
        bytes.push(0); // string
        bytes.extend(encode_raw_string(b"expiring"));
        bytes.extend(encode_raw_string(b"value"));
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("dump with expiry must parse");
        assert_eq!(
            report.largest_keys[0].expires_at_ms,
            Some(1_700_000_000_000)
        );
    }

    #[test]
    fn records_expiretime_seconds_on_the_following_key() {
        let mut bytes = header();
        bytes.push(OPCODE_EXPIRETIME);
        bytes.extend_from_slice(&1_700_000_000u32.to_le_bytes());
        bytes.push(0);
        bytes.extend(encode_raw_string(b"expiring"));
        bytes.extend(encode_raw_string(b"value"));
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("dump with expiry must parse");
        assert_eq!(
            report.largest_keys[0].expires_at_ms,
            Some(1_700_000_000_000)
        );
    }

    #[test]
    fn decodes_an_lzf_compressed_key_name() {
        // "aaaaaaaaaa" (10 bytes) encoded as: literal 'a' + back-reference of 9 more.
        let compressed: Vec<u8> = vec![0x00, b'a', 0xE0, 0x00, 0x00];
        let uncompressed = b"aaaaaaaaaa";
        assert_eq!(
            lzf_decompress(&compressed, uncompressed.len(), 1024).expect("must decompress"),
            uncompressed
        );

        let mut bytes = header();
        bytes.push(0); // string
        // Special-encoding LZF string: ctrl byte 0xC3 (0b11 000011 -> encoding 3 = LZF).
        bytes.push(0xC3);
        bytes.extend(encode_length(compressed.len() as u64)); // compressed length
        bytes.extend(encode_length(uncompressed.len() as u64)); // uncompressed length
        bytes.extend_from_slice(&compressed);
        bytes.extend(encode_raw_string(b"value"));
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("dump with LZF key name must parse");
        assert_eq!(report.largest_keys[0].key, "aaaaaaaaaa");
    }

    #[test]
    fn switches_database_on_selectdb() {
        let mut bytes = header();
        bytes.push(OPCODE_SELECTDB);
        bytes.extend(encode_length(3));
        bytes.push(0);
        bytes.extend(encode_raw_string(b"key-in-db-3"));
        bytes.extend(encode_raw_string(b"value"));
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("dump with SELECTDB must parse");
        assert_eq!(report.largest_keys[0].database, 3);
    }

    #[test]
    fn skips_aux_fields() {
        let mut bytes = header();
        bytes.push(OPCODE_AUX);
        bytes.extend(encode_raw_string(b"redis-ver"));
        bytes.extend(encode_raw_string(b"7.2.0"));
        bytes.push(0);
        bytes.extend(encode_raw_string(b"key"));
        bytes.extend(encode_raw_string(b"value"));
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("dump with AUX must parse");
        assert_eq!(report.total_keys, 1);
    }

    #[test]
    fn unknown_value_type_returns_format_error_with_offset() {
        let mut bytes = header();
        let type_byte_offset = bytes.len() as u64;
        bytes.push(200); // not a recognized value type
        bytes.extend(encode_raw_string(b"key"));
        bytes.push(OPCODE_EOF);

        match analyze_bytes(bytes) {
            Err(DumpAnalysisError::Format { offset, message }) => {
                assert_eq!(offset, type_byte_offset);
                assert!(message.contains("0xC8"), "message was: {message}");
            }
            other => panic!("expected a Format error, got {other:?}"),
        }
    }

    #[test]
    fn parses_a_stream_v1_with_one_listpack_and_no_groups() {
        let mut bytes = header();
        bytes.push(15); // STREAM_LISTPACKS
        bytes.extend(encode_raw_string(b"mystream-v1"));
        bytes.extend(encode_length(1)); // one listpack
        bytes.extend(encode_raw_string(&[0xAA; 16])); // master stream ID
        bytes.extend(encode_raw_string(b"fake-listpack-blob"));
        bytes.extend(encode_length(1)); // items count
        bytes.extend(encode_length(1000)); // last_id.ms
        bytes.extend(encode_length(0)); // last_id.seq
        bytes.extend(encode_length(0)); // no consumer groups
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("stream v1 dump must parse");
        assert_eq!(report.total_keys, 1);
        assert_eq!(report.largest_keys[0].type_name, "stream");
        assert_eq!(report.largest_keys[0].key, "mystream-v1");
    }

    #[test]
    fn parses_a_stream_v2_with_a_group_and_pending_entries() {
        let mut bytes = header();
        bytes.push(19); // STREAM_LISTPACKS_2
        bytes.extend(encode_raw_string(b"mystream-v2"));
        bytes.extend(encode_length(1)); // one listpack
        bytes.extend(encode_raw_string(&[0xAA; 16]));
        bytes.extend(encode_raw_string(b"fake-listpack-blob"));
        bytes.extend(encode_length(1)); // items count
        bytes.extend(encode_length(1000)); // last_id.ms
        bytes.extend(encode_length(0)); // last_id.seq
        bytes.extend(encode_length(500)); // first_id.ms
        bytes.extend(encode_length(0)); // first_id.seq
        bytes.extend(encode_length(0)); // max_deleted_entry_id.ms
        bytes.extend(encode_length(0)); // max_deleted_entry_id.seq
        bytes.extend(encode_length(1)); // entries_added
        bytes.extend(encode_length(1)); // one consumer group
        bytes.extend(encode_raw_string(b"mygroup"));
        bytes.extend(encode_length(1000)); // g_ms
        bytes.extend(encode_length(0)); // g_seq
        bytes.extend(encode_length(1)); // entries_read (v2+)
        bytes.extend(encode_length(1)); // one global PEL entry
        bytes.extend_from_slice(&[0xBB; 16]); // stream ID, raw
        bytes.extend_from_slice(&1_700_000_000_000u64.to_le_bytes()); // delivery time, raw
        bytes.extend(encode_length(1)); // delivery count
        bytes.extend(encode_length(1)); // one consumer
        bytes.extend(encode_raw_string(b"consumer-1"));
        bytes.extend_from_slice(&1_700_000_000_000u64.to_le_bytes()); // seen time, raw
        bytes.extend(encode_length(1)); // one consumer PEL entry
        bytes.extend_from_slice(&[0xCC; 16]); // stream ID only, raw
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("stream v2 dump must parse");
        assert_eq!(report.total_keys, 1);
        assert_eq!(report.largest_keys[0].type_name, "stream");
        assert_eq!(report.largest_keys[0].key, "mystream-v2");
    }

    #[test]
    fn parses_a_stream_v3_with_consumer_active_time() {
        let mut bytes = header();
        bytes.push(21); // STREAM_LISTPACKS_3
        bytes.extend(encode_raw_string(b"mystream-v3"));
        bytes.extend(encode_length(1));
        bytes.extend(encode_raw_string(&[0xAA; 16]));
        bytes.extend(encode_raw_string(b"fake-listpack-blob"));
        bytes.extend(encode_length(1)); // items count
        bytes.extend(encode_length(1000)); // last_id.ms
        bytes.extend(encode_length(0)); // last_id.seq
        bytes.extend(encode_length(500)); // first_id.ms
        bytes.extend(encode_length(0)); // first_id.seq
        bytes.extend(encode_length(0)); // max_deleted_entry_id.ms
        bytes.extend(encode_length(0)); // max_deleted_entry_id.seq
        bytes.extend(encode_length(1)); // entries_added
        bytes.extend(encode_length(1)); // one consumer group
        bytes.extend(encode_raw_string(b"mygroup"));
        bytes.extend(encode_length(1000)); // g_ms
        bytes.extend(encode_length(0)); // g_seq
        bytes.extend(encode_length(1)); // entries_read
        bytes.extend(encode_length(1)); // one global PEL entry
        bytes.extend_from_slice(&[0xBB; 16]);
        bytes.extend_from_slice(&1_700_000_000_000u64.to_le_bytes());
        bytes.extend(encode_length(1)); // delivery count
        bytes.extend(encode_length(1)); // one consumer
        bytes.extend(encode_raw_string(b"consumer-1"));
        bytes.extend_from_slice(&1_700_000_000_000u64.to_le_bytes()); // seen time, raw
        bytes.extend_from_slice(&1_700_000_001_000u64.to_le_bytes()); // active time, raw (v3 only)
        bytes.extend(encode_length(1)); // one consumer PEL entry
        bytes.extend_from_slice(&[0xCC; 16]);
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("stream v3 dump must parse");
        assert_eq!(report.total_keys, 1);
        assert_eq!(report.largest_keys[0].type_name, "stream");
        assert_eq!(report.largest_keys[0].key, "mystream-v3");
    }

    #[test]
    fn truncated_stream_mid_pel_returns_an_error_not_a_panic() {
        let mut bytes = header();
        bytes.push(15); // STREAM_LISTPACKS
        bytes.extend(encode_raw_string(b"mystream-truncated"));
        bytes.extend(encode_length(0)); // no listpacks
        bytes.extend(encode_length(0)); // items count
        bytes.extend(encode_length(0)); // last_id.ms
        bytes.extend(encode_length(0)); // last_id.seq
        bytes.extend(encode_length(1)); // one consumer group
        bytes.extend(encode_raw_string(b"mygroup"));
        bytes.extend(encode_length(0)); // g_ms
        bytes.extend(encode_length(0)); // g_seq
        bytes.extend(encode_length(1)); // one global PEL entry
        bytes.extend_from_slice(&[0xBB; 10]); // truncated: only 10 of the 16 raw ID bytes

        let result = analyze_bytes(bytes);
        assert!(
            result.is_err(),
            "truncated stream PEL must error, not panic"
        );
    }

    #[test]
    fn mixed_dump_aggregates_stream_and_string_keys() {
        let mut bytes = header();
        bytes.push(15); // STREAM_LISTPACKS
        bytes.extend(encode_raw_string(b"mystream"));
        bytes.extend(encode_length(0)); // no listpacks
        bytes.extend(encode_length(0)); // items count
        bytes.extend(encode_length(0)); // last_id.ms
        bytes.extend(encode_length(0)); // last_id.seq
        bytes.extend(encode_length(0)); // no consumer groups

        bytes.push(0); // string
        bytes.extend(encode_raw_string(b"mystring"));
        bytes.extend(encode_raw_string(b"value"));
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("mixed dump must parse");
        assert_eq!(report.total_keys, 2);

        let type_names: Vec<&str> = report
            .keys_by_type
            .iter()
            .map(|(type_name, _, _)| type_name.as_str())
            .collect();
        assert!(type_names.contains(&"stream"));
        assert!(type_names.contains(&"string"));
    }

    #[test]
    fn truncated_file_mid_value_returns_an_error_not_a_panic() {
        let mut bytes = header();
        bytes.push(0); // string
        bytes.extend(encode_raw_string(b"key"));
        bytes.extend(encode_length(100)); // claims 100 bytes but the file ends here

        let result = analyze_bytes(bytes);
        assert!(result.is_err(), "truncated file must error, not panic");
    }

    #[test]
    fn invalid_magic_header_returns_format_error_at_offset_zero() {
        let bytes = b"NOTREDIS1".to_vec();
        match analyze_bytes(bytes) {
            Err(DumpAnalysisError::Format { offset, .. }) => assert_eq!(offset, 0),
            other => panic!("expected a Format error, got {other:?}"),
        }
    }

    #[test]
    fn prefix_rollup_groups_by_first_separator() {
        let mut bytes = header();
        for key in ["user:1", "user:2", "session/abc", "no-separator-but-short"] {
            bytes.push(0);
            bytes.extend(encode_raw_string(key.as_bytes()));
            bytes.extend(encode_raw_string(b"v"));
        }
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("dump must parse");
        let user_bucket = report
            .prefix_rollup
            .iter()
            .find(|entry| entry.prefix == "user:")
            .expect("user: bucket must exist");
        assert_eq!(user_bucket.key_count, 2);

        assert!(
            report
                .prefix_rollup
                .iter()
                .any(|entry| entry.prefix == "session/")
        );
        assert!(
            report
                .prefix_rollup
                .iter()
                .any(|entry| entry.prefix == "no-separator-but-short")
        );
    }

    #[test]
    fn long_key_without_separator_buckets_under_no_prefix() {
        let long_key = "x".repeat(64);
        assert_eq!(prefix_bucket_for(&long_key), "(no prefix)");
    }

    #[test]
    fn top_n_largest_keys_is_bounded_and_sorted_descending() {
        let mut bytes = header();
        // One more key than the cap, with strictly increasing value sizes so
        // the smallest one must be evicted from the bounded heap.
        for i in 0..(TOP_N_LARGEST_KEYS + 1) {
            bytes.push(0);
            bytes.extend(encode_raw_string(format!("key-{i}").as_bytes()));
            bytes.extend(encode_raw_string(&vec![b'v'; i + 1]));
        }
        bytes.push(OPCODE_EOF);

        let report = analyze_bytes(bytes).expect("dump must parse");
        assert_eq!(report.largest_keys.len(), TOP_N_LARGEST_KEYS);
        assert_eq!(report.total_keys, (TOP_N_LARGEST_KEYS + 1) as u64);

        let sizes: Vec<u64> = report
            .largest_keys
            .iter()
            .map(|entry| entry.serialized_bytes)
            .collect();
        let mut sorted = sizes.clone();
        sorted.sort_unstable_by(|a, b| b.cmp(a));
        assert_eq!(sizes, sorted, "largest_keys must be sorted descending");

        // The very first key (smallest value) must have been evicted.
        assert!(!report.largest_keys.iter().any(|entry| entry.key == "key-0"));
    }

    #[test]
    fn cancellation_mid_parse_returns_cancelled() {
        let mut bytes = header();
        for i in 0..10 {
            bytes.push(0);
            bytes.extend(encode_raw_string(format!("key-{i}").as_bytes()));
            bytes.extend(encode_raw_string(b"v"));
        }
        bytes.push(OPCODE_EOF);

        let calls = AtomicU32::new(0);
        let cancelled = || calls.fetch_add(1, Ordering::SeqCst) > 2;

        let result = analyze_reader(Cursor::new(bytes), None, &no_progress, &cancelled);
        assert!(matches!(result, Err(DumpAnalysisError::Cancelled)));
    }

    mod lzf {
        use super::lzf_decompress;

        #[test]
        fn decompresses_pure_literal_runs() {
            // Two literal runs: "hello" (5 bytes) and "world" (5 bytes).
            let compressed = vec![
                4, b'h', b'e', b'l', b'l', b'o', 4, b'w', b'o', b'r', b'l', b'd',
            ];
            let out = lzf_decompress(&compressed, 10, 1024).expect("must decompress");
            assert_eq!(out, b"helloworld");
        }

        #[test]
        fn decompresses_a_back_reference() {
            // literal 'a', then a back-reference copying 9 more 'a's.
            let compressed = vec![0x00, b'a', 0xE0, 0x00, 0x00];
            let out = lzf_decompress(&compressed, 10, 1024).expect("must decompress");
            assert_eq!(out, b"aaaaaaaaaa");
        }

        #[test]
        fn rejects_declared_length_larger_than_cap() {
            let compressed = vec![0x00, b'a'];
            let result = lzf_decompress(&compressed, 1, 0);
            assert!(result.is_err());
        }

        #[test]
        fn rejects_back_reference_before_start_of_output() {
            // A back-reference control byte with no prior literal output.
            let compressed = vec![0xE0, 0x00, 0x00];
            let result = lzf_decompress(&compressed, 9, 1024);
            assert!(result.is_err());
        }

        #[test]
        fn rejects_mismatched_declared_length() {
            let compressed = vec![4, b'h', b'e', b'l', b'l'];
            let result = lzf_decompress(&compressed, 100, 1024);
            assert!(result.is_err());
        }
    }
}
