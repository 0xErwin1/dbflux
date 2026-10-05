use std::ops::Range;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use bytes::Bytes;
use dbflux_byte_source::ByteSource;
use parquet::DecodeResult;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::basic::CompressionCodec;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataPushDecoder};

use crate::ParquetError;

/// Bytes read from the end of the file in the first request. Small and
/// medium files hold their footer and page index inside it, so they open with
/// one request.
pub const TAIL_READ_BYTES: u64 = 64 * 1024;

/// Largest single footer or page-index read accepted. Footers of files with
/// thousands of columns stay in the low megabytes, so a larger length is a
/// corrupt or hostile file that would otherwise make the reader allocate it.
pub const MAX_FOOTER_BYTES: u64 = 16 * 1024 * 1024;

/// Length of the trailing metadata length plus magic number.
const FOOTER_TAIL_BYTES: u64 = 8;

const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

/// Magic number of a file whose footer is encrypted.
const ENCRYPTED_FOOTER_MAGIC: &[u8; 4] = b"PARE";

/// The decoded footer of a Parquet file: its metadata, page index and Arrow
/// schema.
#[derive(Debug, Clone)]
pub struct ParquetFile {
    metadata: Arc<ParquetMetaData>,
    schema: SchemaRef,
    row_count: u64,
    has_offset_index: bool,
}

impl ParquetFile {
    /// The footer metadata, with the column index and offset index loaded when
    /// the file has them.
    pub fn metadata(&self) -> &Arc<ParquetMetaData> {
        &self.metadata
    }

    /// The Arrow schema the file decodes to, honoring the Arrow schema a
    /// writer embedded in the footer.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn row_group_count(&self) -> usize {
        self.metadata.num_row_groups()
    }

    /// Whether every column chunk of every row group has an offset index, so
    /// a reader can locate the pages of any row window. True for a file with
    /// no row groups, which has nothing to locate.
    pub fn has_offset_index(&self) -> bool {
        self.has_offset_index
    }
}

/// Reads the footer and page index of the Parquet file in `source`.
///
/// The first request reads up to [`TAIL_READ_BYTES`] from the end of the
/// file; any further request is a range the metadata decoder still needs,
/// each at most [`MAX_FOOTER_BYTES`] long. Encrypted files and columns
/// compressed with a codec this build cannot decode are refused here, before
/// any page is read.
pub fn open<S: ByteSource + ?Sized>(source: &S) -> Result<ParquetFile, ParquetError> {
    let file_length = source.byte_length()?;

    if file_length < FOOTER_TAIL_BYTES {
        return Err(ParquetError::NotParquet {
            reason: format!("the file is {file_length} bytes, shorter than a Parquet footer"),
        });
    }

    let tail_range = file_length.saturating_sub(TAIL_READ_BYTES)..file_length;
    let tail = read_exact(source, tail_range.clone())?;

    check_footer_tail(&tail, file_length)?;

    let mut decoder = ParquetMetaDataPushDecoder::try_new(file_length)?
        .with_page_index_policy(PageIndexPolicy::Optional);

    decoder.push_range(tail_range, Bytes::from(tail))?;

    let metadata = loop {
        match decoder.try_decode()? {
            DecodeResult::NeedsData(ranges) => {
                let mut buffers = Vec::with_capacity(ranges.len());

                for range in &ranges {
                    check_requested_range(range, file_length)?;
                    buffers.push(Bytes::from(read_exact(source, range.clone())?));
                }

                decoder.push_ranges(ranges, buffers)?;
            }

            DecodeResult::Data(metadata) => break metadata,

            DecodeResult::Finished => {
                return Err(ParquetError::malformed(
                    "the metadata decoder finished without producing metadata",
                ));
            }
        }
    };

    check_codecs(&metadata)?;

    let file_metadata = metadata.file_metadata();

    let schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;

    let row_count = u64::try_from(file_metadata.num_rows()).map_err(|_| {
        ParquetError::malformed(format!(
            "the footer declares a negative row count ({})",
            file_metadata.num_rows()
        ))
    })?;

    let covered_rows = row_group_rows(&metadata)?;

    if row_count > covered_rows {
        return Err(ParquetError::malformed(format!(
            "the footer declares {row_count} rows but its row groups hold {covered_rows}"
        )));
    }

    let has_offset_index = every_row_group_has_offset_index(&metadata);

    Ok(ParquetFile {
        metadata: Arc::new(metadata),
        schema: Arc::new(schema),
        row_count,
        has_offset_index,
    })
}

/// The rows the row groups of `metadata` hold together. A footer total above
/// it would promise rows no window can read.
fn row_group_rows(metadata: &ParquetMetaData) -> Result<u64, ParquetError> {
    metadata.row_groups().iter().enumerate().try_fold(
        0_u64,
        |total, (row_group, row_group_metadata)| {
            let rows = u64::try_from(row_group_metadata.num_rows()).map_err(|_| {
                ParquetError::malformed(format!(
                    "row group {row_group} declares a negative row count ({})",
                    row_group_metadata.num_rows()
                ))
            })?;

            total.checked_add(rows).ok_or_else(|| {
                ParquetError::malformed("the row groups declare more rows than fit in 64 bits")
            })
        },
    )
}

/// Reads `range`, which must lie inside the file, and refuses a result that is
/// shorter than the range.
pub(crate) fn read_exact<S: ByteSource + ?Sized>(
    source: &S,
    range: Range<u64>,
) -> Result<Vec<u8>, ParquetError> {
    let bytes = source.read_range(range.clone())?;

    let expected = range.end.saturating_sub(range.start);
    let returned = u64::try_from(bytes.len()).unwrap_or(u64::MAX);

    if returned != expected {
        return Err(ParquetError::ShortRead {
            offset: range.start,
            expected,
            returned,
        });
    }

    Ok(bytes)
}

/// Validates the last eight bytes of the file before the decoder sees them.
///
/// The decoder computes the metadata start as `file length - 8 - metadata
/// length` without checking, so a length larger than the file would underflow
/// there.
fn check_footer_tail(tail: &[u8], file_length: u64) -> Result<(), ParquetError> {
    let footer_tail = tail
        .len()
        .checked_sub(8)
        .and_then(|start| tail.get(start..))
        .and_then(|bytes| <&[u8; 8]>::try_from(bytes).ok())
        .ok_or_else(|| ParquetError::NotParquet {
            reason: "the file is shorter than a Parquet footer".to_string(),
        })?;

    let (length_bytes, magic) = footer_tail.split_at(4);

    if magic == ENCRYPTED_FOOTER_MAGIC {
        return Err(ParquetError::Encrypted);
    }

    if magic != PARQUET_MAGIC {
        return Err(ParquetError::NotParquet {
            reason: "the file does not end with the Parquet magic number".to_string(),
        });
    }

    let metadata_length = length_bytes
        .try_into()
        .map(u32::from_le_bytes)
        .map(u64::from)
        .map_err(|_| ParquetError::malformed("the footer length field is incomplete"))?;

    if metadata_length > MAX_FOOTER_BYTES {
        return Err(ParquetError::FooterTooLarge {
            length: metadata_length,
            limit: MAX_FOOTER_BYTES,
        });
    }

    if metadata_length > file_length - FOOTER_TAIL_BYTES {
        return Err(ParquetError::malformed(format!(
            "the footer declares {metadata_length} bytes of metadata, more than the {file_length}-byte file holds"
        )));
    }

    Ok(())
}

/// Refuses a range the decoder asks for that is too large to read or that
/// points outside the file, so neither reaches the source.
fn check_requested_range(range: &Range<u64>, file_length: u64) -> Result<(), ParquetError> {
    if range.start > range.end || range.end > file_length {
        return Err(ParquetError::malformed(format!(
            "the metadata points at bytes {}..{}, outside the {file_length}-byte file",
            range.start, range.end
        )));
    }

    let length = range.end - range.start;

    if length > MAX_FOOTER_BYTES {
        return Err(ParquetError::FooterTooLarge {
            length,
            limit: MAX_FOOTER_BYTES,
        });
    }

    Ok(())
}

/// Refuses the first column chunk compressed with a codec this build was not
/// compiled with, so the error names it when the file opens instead of at the
/// first page read.
fn check_codecs(metadata: &ParquetMetaData) -> Result<(), ParquetError> {
    let unsupported = metadata
        .row_groups()
        .iter()
        .flat_map(|row_group| row_group.columns())
        .find(|column| !is_codec_supported(column.compression_codec()));

    match unsupported {
        Some(column) => Err(ParquetError::UnsupportedCodec {
            codec: column.compression_codec().to_string(),
            column: column.column_path().string(),
        }),
        None => Ok(()),
    }
}

/// Mirrors the codec features enabled on the `parquet` dependency in the
/// workspace manifest; keep the two in step.
fn is_codec_supported(codec: CompressionCodec) -> bool {
    match codec {
        CompressionCodec::UNCOMPRESSED
        | CompressionCodec::SNAPPY
        | CompressionCodec::GZIP
        | CompressionCodec::LZ4
        | CompressionCodec::ZSTD
        | CompressionCodec::LZ4_RAW => true,

        CompressionCodec::BROTLI | CompressionCodec::LZO => false,
    }
}

fn every_row_group_has_offset_index(metadata: &ParquetMetaData) -> bool {
    if metadata.num_row_groups() == 0 {
        return true;
    }

    let Some(offset_index) = metadata.offset_index() else {
        return false;
    };

    offset_index.len() == metadata.num_row_groups()
        && metadata
            .row_groups()
            .iter()
            .zip(offset_index)
            .all(|(row_group, columns)| columns.len() == row_group.num_columns())
}
