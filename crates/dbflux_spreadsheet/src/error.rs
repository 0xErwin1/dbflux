use dbflux_byte_source::SourceError;

/// A failure to open a spreadsheet or read one of its sheets.
#[derive(Debug, thiserror::Error)]
pub enum SpreadsheetError {
    /// The byte source failed; the message is the source's own.
    #[error(transparent)]
    Source(#[from] SourceError),

    /// The bytes are not an xlsx, xlsm, xls or ods workbook.
    #[error("not a spreadsheet: {reason}")]
    NotASpreadsheet { reason: String },

    /// The workbook is encrypted or protected with a password to open.
    #[error("the spreadsheet is encrypted or password protected, which DBFlux cannot read")]
    Encrypted,

    /// The sheet is a chart sheet, which holds a chart and no cells.
    #[error("sheet `{name}` is a chart sheet and has no cells to show")]
    ChartSheet { name: String },

    /// A sheet index past the last sheet of the workbook.
    #[error("sheet {index} does not exist; the workbook has {sheet_count} sheets")]
    SheetOutOfRange { index: usize, sheet_count: usize },

    /// The sheet's grid, padded to A1, has more than
    /// [`crate::MAX_GRID_CELLS`] cells.
    #[error("the sheet spans {rows} rows by {columns} columns, more than the {limit}-cell limit")]
    SheetTooLarge {
        rows: usize,
        columns: usize,
        limit: usize,
    },

    /// The workbook does not decode as its format; the message is the
    /// reader's own.
    #[error("malformed spreadsheet: {message}")]
    Malformed { message: String },
}

impl SpreadsheetError {
    pub(crate) fn malformed(message: impl Into<String>) -> Self {
        Self::Malformed {
            message: message.into(),
        }
    }

    pub(crate) fn not_a_spreadsheet(reason: impl Into<String>) -> Self {
        Self::NotASpreadsheet {
            reason: reason.into(),
        }
    }
}

/// Keeps a failed read of the byte source as [`SpreadsheetError::Source`]
/// instead of reporting the file as malformed.
///
/// The reader hands calamine and zip an [`std::io::Error`] built from the
/// [`SourceError`], so the I/O error is the only trace of a source failure
/// once it comes back out of them. Decompressors report corrupt data as
/// [`std::io::ErrorKind::InvalidData`], which is the file's fault, not the
/// source's.
pub(crate) fn from_io(error: std::io::Error) -> SpreadsheetError {
    if error.kind() == std::io::ErrorKind::InvalidData {
        return SpreadsheetError::malformed(error.to_string());
    }

    SpreadsheetError::Source(SourceError::new(error))
}

impl From<calamine::XlsxError> for SpreadsheetError {
    fn from(error: calamine::XlsxError) -> Self {
        match error {
            calamine::XlsxError::Io(io_error) => from_io(io_error),
            calamine::XlsxError::Zip(zip::result::ZipError::Io(io_error)) => from_io(io_error),
            calamine::XlsxError::Password => Self::Encrypted,
            other => Self::malformed(other.to_string()),
        }
    }
}

impl From<calamine::XlsError> for SpreadsheetError {
    fn from(error: calamine::XlsError) -> Self {
        match error {
            calamine::XlsError::Io(io_error) => from_io(io_error),
            calamine::XlsError::Password => Self::Encrypted,
            other => Self::malformed(other.to_string()),
        }
    }
}

impl From<calamine::OdsError> for SpreadsheetError {
    fn from(error: calamine::OdsError) -> Self {
        match error {
            calamine::OdsError::Io(io_error) => from_io(io_error),
            calamine::OdsError::Zip(zip::result::ZipError::Io(io_error)) => from_io(io_error),
            calamine::OdsError::Password => Self::Encrypted,
            other => Self::malformed(other.to_string()),
        }
    }
}

impl From<zip::result::ZipError> for SpreadsheetError {
    fn from(error: zip::result::ZipError) -> Self {
        match error {
            zip::result::ZipError::Io(io_error) => from_io(io_error),
            other => Self::malformed(other.to_string()),
        }
    }
}
