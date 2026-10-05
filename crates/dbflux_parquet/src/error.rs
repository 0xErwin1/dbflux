use dbflux_byte_source::SourceError;

/// A failure to open or read a Parquet file.
#[derive(Debug, thiserror::Error)]
pub enum ParquetError {
    /// The byte source failed; the message is the source's own.
    #[error(transparent)]
    Source(#[from] SourceError),

    /// The byte source returned fewer bytes than a range that lies inside the
    /// file, usually because the file shrank while it was being read.
    #[error(
        "the file returned {returned} of the {expected} bytes requested at offset {offset}; it may have changed while it was being read"
    )]
    ShortRead {
        offset: u64,
        expected: u64,
        returned: u64,
    },

    /// The bytes do not end with the Parquet magic number.
    #[error("not a Parquet file: {reason}")]
    NotParquet { reason: String },

    /// A footer or page-index read is larger than [`crate::MAX_FOOTER_BYTES`].
    #[error("the Parquet footer needs {length} bytes, more than the {limit}-byte limit")]
    FooterTooLarge { length: u64, limit: u64 },

    /// A column chunk is compressed with a codec this build cannot decode.
    #[error("column `{column}` is compressed with {codec}, which DBFlux cannot read")]
    UnsupportedCodec { codec: String, column: String },

    /// The file uses Parquet modular encryption.
    #[error("the Parquet file is encrypted, which DBFlux cannot read")]
    Encrypted,

    /// The footer, page index or column data does not decode as valid
    /// Parquet; the message is the decoder's own.
    #[error("malformed Parquet file: {message}")]
    Malformed { message: String },

    /// A row window needs a whole column chunk, because its row group has no
    /// offset index, and that chunk is larger than the read budget.
    #[error(
        "column `{column}` has no page index, so reading any of its rows needs the whole {size}-byte chunk, more than the {limit}-byte limit"
    )]
    UnindexedChunkTooLarge {
        column: String,
        size: u64,
        limit: u64,
    },

    /// A row window was asked for without any column.
    #[error("no columns were selected to read")]
    NoColumnsSelected,

    /// A selected column index is not a top-level field of the file.
    #[error("column {index} does not exist; the file has {column_count} top-level columns")]
    ColumnOutOfRange { index: usize, column_count: usize },
}

impl ParquetError {
    pub(crate) fn malformed(message: impl Into<String>) -> Self {
        Self::Malformed {
            message: message.into(),
        }
    }
}

impl From<parquet::errors::ParquetError> for ParquetError {
    fn from(error: parquet::errors::ParquetError) -> Self {
        Self::malformed(error.to_string())
    }
}
