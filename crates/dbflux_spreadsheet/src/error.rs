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

/// A failure to write edits into an xlsx or xlsm package.
///
/// Every refusal of an edit names the sheet and the cell, so the caller can
/// point the user at it. Refusals come before anything is written; after a
/// [`SheetWriteError::Source`] or [`SheetWriteError::Sink`] failure the sink
/// may hold a partial package.
#[derive(Debug, thiserror::Error)]
pub enum SheetWriteError {
    /// Reading the original package failed; the message is the source's own.
    #[error(transparent)]
    Source(#[from] SourceError),

    /// Writing the patched package to the sink failed.
    #[error("cannot write the patched spreadsheet: {0}")]
    Sink(#[source] std::io::Error),

    /// The package is not an xlsx package DBFlux can patch.
    #[error("malformed spreadsheet: {message}")]
    Malformed { message: String },

    /// A sheet index past the last sheet of the workbook.
    #[error("sheet {index} does not exist; the workbook has {sheet_count} sheets")]
    SheetOutOfRange { index: usize, sheet_count: usize },

    /// The sheet is a chart sheet or another sheet type without cells.
    #[error("sheet `{sheet}` is not a worksheet, so it has no cells to edit")]
    NotAWorksheet { sheet: String },

    /// A zero-based position past `XFD1048576`, the last cell of a sheet.
    #[error("sheet `{sheet}`: row {row} and column {column} are past the last cell of a sheet")]
    CellOutOfRange {
        sheet: String,
        row: usize,
        column: usize,
    },

    /// Text longer than the 32,767 UTF-16 units a cell holds.
    #[error("{sheet}!{cell}: the text has {length} characters, more than the 32767 a cell holds")]
    TextTooLong {
        sheet: String,
        cell: String,
        length: usize,
    },

    /// A character XML 1.0 cannot represent, such as most control characters.
    #[error("{sheet}!{cell}: the character U+{code:04X} cannot be stored in an xlsx file", code = u32::from(*character))]
    InvalidCharacter {
        sheet: String,
        cell: String,
        character: char,
    },

    /// NaN or an infinity, which a cell cannot store as a number.
    #[error("{sheet}!{cell}: {value} is not a number a cell can store")]
    NonFiniteNumber {
        sheet: String,
        cell: String,
        value: f64,
    },

    /// A date before the workbook's epoch or after 9999-12-31.
    #[error("{sheet}!{cell}: {date} is outside the dates this workbook can store")]
    DateOutOfRange {
        sheet: String,
        cell: String,
        date: chrono::NaiveDateTime,
    },

    /// The cell holds the text of a shared formula that other cells reuse.
    #[error(
        "{sheet}!{cell} holds a formula shared with the cells of {range}; editing it would change them too"
    )]
    SharedFormulaMaster {
        sheet: String,
        cell: String,
        range: String,
    },

    /// The cell is part of an array or data-table formula, which only the
    /// spreadsheet application can edit as a whole.
    #[error(
        "{sheet}!{cell} is inside the {kind} formula range {range}, which is edited as a whole"
    )]
    InsideFormulaRange {
        sheet: String,
        cell: String,
        range: String,
        kind: FormulaRangeKind,
    },
}

/// A formula whose result fills a range of cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormulaRangeKind {
    Array,
    DataTable,
}

impl std::fmt::Display for FormulaRangeKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Array => formatter.write_str("array"),
            Self::DataTable => formatter.write_str("data table"),
        }
    }
}

impl SheetWriteError {
    pub(crate) fn malformed(message: impl Into<String>) -> Self {
        Self::Malformed {
            message: message.into(),
        }
    }

    /// Maps a failure reading the original package, keeping a source failure
    /// apart from a damaged file the way [`from_io`] does for the reader.
    pub(crate) fn from_read(error: zip::result::ZipError) -> Self {
        match SpreadsheetError::from(error) {
            SpreadsheetError::Source(source) => Self::Source(source),
            SpreadsheetError::Malformed { message } => Self::Malformed { message },
            other => Self::malformed(other.to_string()),
        }
    }

    pub(crate) fn from_read_io(error: std::io::Error) -> Self {
        Self::from_read(zip::result::ZipError::Io(error))
    }

    /// Maps a failure writing the patched package.
    pub(crate) fn from_write(error: zip::result::ZipError) -> Self {
        match error {
            zip::result::ZipError::Io(io_error) => Self::Sink(io_error),
            other => Self::Sink(std::io::Error::other(other)),
        }
    }
}

/// Reports a failure to scan an xlsx package while reading it, such as when
/// looking for the row where appended rows go.
impl From<SheetWriteError> for SpreadsheetError {
    fn from(error: SheetWriteError) -> Self {
        match error {
            SheetWriteError::Source(source) => Self::Source(source),
            SheetWriteError::Malformed { message } => Self::Malformed { message },
            other => Self::malformed(other.to_string()),
        }
    }
}

impl From<quick_xml::Error> for SheetWriteError {
    fn from(error: quick_xml::Error) -> Self {
        Self::malformed(format!("invalid XML: {error}"))
    }
}

impl From<quick_xml::events::attributes::AttrError> for SheetWriteError {
    fn from(error: quick_xml::events::attributes::AttrError) -> Self {
        Self::malformed(format!("invalid XML attribute: {error}"))
    }
}
