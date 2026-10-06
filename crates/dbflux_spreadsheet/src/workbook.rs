use std::fmt;
use std::io::Read;

use calamine::{Data, Ods, Range, Reader, SheetType, SheetVisible, Xls, XlsError, Xlsx, XlsxError};
use dbflux_byte_source::{ByteSource, ByteSourceReader};

use crate::error::SpreadsheetError;
use crate::grid::{FormulaSource, SheetGrid, build_grid};
use crate::xlsx_patch::sheet_append_row;

const ZIP_LOCAL_HEADER: &[u8] = b"PK\x03\x04";
const ZIP_EMPTY_ARCHIVE: &[u8] = b"PK\x05\x06";
const COMPOUND_FILE_HEADER: &[u8] = &[0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];

const ODS_MIMETYPE: &[u8] = b"application/vnd.oasis.opendocument.spreadsheet";
const XLSX_WORKBOOK_PART: &str = "xl/workbook.xml";
const XLSM_VBA_PART: &str = "xl/vbaProject.bin";

/// The bytes read to tell the formats apart, and the most of an ods
/// `mimetype` entry read to recognize it.
const HEADER_BYTES: u64 = 8;
const MIMETYPE_READ_LIMIT: u64 = 128;

/// A spreadsheet format, recognized from the file's content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpreadsheetFormat {
    Xlsx,
    /// An xlsx package that carries a VBA project.
    Xlsm,
    Xls,
    Ods,
}

/// What a sheet of a workbook holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SheetKind {
    Worksheet,
    /// A sheet that holds a single chart and no cells.
    ChartSheet,
    /// A dialog sheet, a macro sheet or a VBA module.
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SheetInfo {
    pub name: String,
    pub kind: SheetKind,
    /// `false` for hidden and very hidden sheets.
    pub visible: bool,
}

/// An open workbook: its format, its sheets, and a reader that loads one
/// sheet at a time with [`Workbook::read_sheet`].
pub struct Workbook<S> {
    format: SpreadsheetFormat,
    sheets: Vec<SheetInfo>,
    date_1904: bool,
    reader: FormatReader<S>,
    /// The source of an xlsx or xlsm package, read again to find where
    /// appended rows go, which calamine does not report.
    package_source: Option<S>,
}

enum FormatReader<S> {
    Xlsx(Box<Xlsx<ByteSourceReader<S>>>),
    Xls(Box<Xls<ByteSourceReader<S>>>),
    Ods(Box<Ods<ByteSourceReader<S>>>),
}

impl<S> fmt::Debug for Workbook<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Workbook")
            .field("format", &self.format)
            .field("sheets", &self.sheets)
            .field("date_1904", &self.date_1904)
            .finish_non_exhaustive()
    }
}

/// Opens a workbook, recognizing its format from its content rather than
/// from a file name.
///
/// - A zip archive whose `mimetype` entry is
///   `application/vnd.oasis.opendocument.spreadsheet` is ods.
/// - A zip archive with an `xl/workbook.xml` part is xlsx, or xlsm when it
///   also has an `xl/vbaProject.bin` part.
/// - A compound file (the OLE container) is an encrypted xlsx when it holds
///   an `EncryptedPackage` stream, and xls otherwise.
/// - Anything else is [`SpreadsheetError::NotASpreadsheet`].
///
/// The sheet list is read here; cell data is read by
/// [`Workbook::read_sheet`]. Reads block, so a caller with a slow source runs
/// this off its UI thread.
pub fn open<S: ByteSource + Clone>(source: S) -> Result<Workbook<S>, SpreadsheetError> {
    let header = source.read_range(0..HEADER_BYTES)?;

    if header.starts_with(ZIP_LOCAL_HEADER) || header.starts_with(ZIP_EMPTY_ARCHIVE) {
        return open_zip_package(source);
    }

    if header.starts_with(COMPOUND_FILE_HEADER) {
        return open_compound_file(source);
    }

    Err(SpreadsheetError::not_a_spreadsheet(
        "the content does not start with a zip or compound-file header",
    ))
}

fn open_zip_package<S: ByteSource + Clone>(source: S) -> Result<Workbook<S>, SpreadsheetError> {
    let format = zip_package_format(source.clone())?;
    let package_source =
        matches!(format, SpreadsheetFormat::Xlsx | SpreadsheetFormat::Xlsm).then(|| source.clone());

    let reader = ByteSourceReader::new(source);
    let reader = match format {
        SpreadsheetFormat::Ods => FormatReader::Ods(Box::new(Ods::new(reader)?)),
        SpreadsheetFormat::Xlsx | SpreadsheetFormat::Xlsm => {
            FormatReader::Xlsx(Box::new(Xlsx::new(reader)?))
        }
        SpreadsheetFormat::Xls => {
            return Err(SpreadsheetError::malformed(
                "a zip archive cannot hold an xls workbook",
            ));
        }
    };

    Ok(Workbook::new(format, reader, package_source))
}

fn zip_package_format<S: ByteSource>(source: S) -> Result<SpreadsheetFormat, SpreadsheetError> {
    let mut archive = zip::ZipArchive::new(ByteSourceReader::new(source))?;

    if archive.index_for_name("mimetype").is_some() {
        let mut mimetype = Vec::new();
        archive
            .by_name("mimetype")?
            .take(MIMETYPE_READ_LIMIT)
            .read_to_end(&mut mimetype)
            .map_err(crate::error::from_io)?;

        if mimetype.trim_ascii() == ODS_MIMETYPE {
            return Ok(SpreadsheetFormat::Ods);
        }

        return Err(SpreadsheetError::not_a_spreadsheet(format!(
            "an OpenDocument file of type `{}`, not a spreadsheet",
            String::from_utf8_lossy(&mimetype).trim()
        )));
    }

    if archive.index_for_name(XLSX_WORKBOOK_PART).is_some() {
        if archive.index_for_name(XLSM_VBA_PART).is_some() {
            return Ok(SpreadsheetFormat::Xlsm);
        }

        return Ok(SpreadsheetFormat::Xlsx);
    }

    Err(SpreadsheetError::not_a_spreadsheet(
        "a zip archive without an ods mimetype or an xlsx workbook part",
    ))
}

fn open_compound_file<S: ByteSource + Clone>(source: S) -> Result<Workbook<S>, SpreadsheetError> {
    // An encrypted xlsx is a compound file too. calamine's xlsx reader is the
    // one that looks for its `EncryptedPackage` stream, and it fails on any
    // other compound file because that is not a zip archive. A read of the
    // source that failed meanwhile is the source's failure, which the xls
    // reader would report as a compound file without a workbook.
    match Xlsx::new(ByteSourceReader::new(source.clone())) {
        Err(XlsxError::Password) => return Err(SpreadsheetError::Encrypted),
        Err(error @ (XlsxError::Io(_) | XlsxError::Zip(zip::result::ZipError::Io(_)))) => {
            return Err(error.into());
        }
        _ => {}
    }

    let xls = match Xls::new(ByteSourceReader::new(source)) {
        Ok(xls) => xls,
        // calamine keeps the compound-file error type private, so a missing
        // workbook stream (a Word or Outlook file) cannot be told apart from
        // a damaged container; both mean there is no workbook to read.
        Err(XlsError::Cfb(error)) => {
            return Err(SpreadsheetError::not_a_spreadsheet(format!(
                "a compound file without a readable workbook stream ({error})"
            )));
        }
        Err(error) => return Err(error.into()),
    };

    Ok(Workbook::new(
        SpreadsheetFormat::Xls,
        FormatReader::Xls(Box::new(xls)),
        None,
    ))
}

impl<S: ByteSource> Workbook<S> {
    fn new(format: SpreadsheetFormat, reader: FormatReader<S>, package_source: Option<S>) -> Self {
        let (metadata, date_1904) = match &reader {
            FormatReader::Xlsx(xlsx) => (xlsx.sheets_metadata(), xlsx.has_1904_epoch()),
            FormatReader::Xls(xls) => (xls.sheets_metadata(), xls.has_1904_epoch()),
            FormatReader::Ods(ods) => (ods.sheets_metadata(), false),
        };

        let sheets = metadata
            .iter()
            .map(|sheet| SheetInfo {
                name: sheet.name.clone(),
                kind: match sheet.typ {
                    SheetType::WorkSheet => SheetKind::Worksheet,
                    SheetType::ChartSheet => SheetKind::ChartSheet,
                    SheetType::DialogSheet | SheetType::MacroSheet | SheetType::Vba => {
                        SheetKind::Other
                    }
                },
                visible: sheet.visible == SheetVisible::Visible,
            })
            .collect();

        Self {
            format,
            sheets,
            date_1904,
            reader,
            package_source,
        }
    }
}

impl<S> Workbook<S> {
    pub fn format(&self) -> SpreadsheetFormat {
        self.format
    }

    /// Returns the sheets in workbook order, hidden ones included.
    pub fn sheets(&self) -> &[SheetInfo] {
        &self.sheets
    }

    /// Whether the workbook counts dates from 1904 instead of 1900. Always
    /// `false` for ods, whose dates are stored as calendar dates.
    pub fn date_1904(&self) -> bool {
        self.date_1904
    }
}

impl<S: ByteSource> Workbook<S> {
    /// Reads the sheet at `index` (in [`Workbook::sheets`] order) into a grid
    /// padded to A1.
    ///
    /// The whole sheet is decoded into memory; calamine has no row stream.
    /// xls formula text is never read, because calamine decodes it with wrong
    /// references and loses shared formulas; see [`crate::CellFormula`].
    /// For xlsx and xlsm the sheet's part is scanned once more to find
    /// [`SheetGrid::append_row`], which calamine does not report. That scan
    /// is stricter than calamine, which reads cells without the table parts,
    /// so a scan that fails still returns the cells, without an append row:
    /// the patcher scans again before it writes.
    pub fn read_sheet(&mut self, index: usize) -> Result<SheetGrid, SpreadsheetError> {
        let sheet = self
            .sheets
            .get(index)
            .ok_or(SpreadsheetError::SheetOutOfRange {
                index,
                sheet_count: self.sheets.len(),
            })?;

        if sheet.kind == SheetKind::ChartSheet {
            return Err(SpreadsheetError::ChartSheet {
                name: sheet.name.clone(),
            });
        }

        let (values, formulas) = self.reader.ranges(&sheet.name)?;
        let grid = build_grid(values, formulas)?;

        let Some(source) = &self.package_source else {
            return Ok(grid);
        };

        match sheet_append_row(source, index) {
            Ok(Some(append_row)) => grid.with_append_row(append_row),
            Ok(None) | Err(_) => Ok(grid),
        }
    }
}

impl<S: ByteSource> FormatReader<S> {
    fn ranges(&mut self, name: &str) -> Result<(Range<Data>, FormulaSource), SpreadsheetError> {
        match self {
            Self::Xlsx(xlsx) => {
                let values = xlsx.worksheet_range(name)?;
                let formulas = xlsx.worksheet_formula(name)?;

                Ok((values, FormulaSource::PrefixEquals(formulas)))
            }
            Self::Ods(ods) => {
                let values = ods.worksheet_range(name)?;
                let formulas = ods.worksheet_formula(name)?;

                Ok((values, FormulaSource::Verbatim(formulas)))
            }
            Self::Xls(xls) => Ok((xls.worksheet_range(name)?, FormulaSource::Unavailable)),
        }
    }
}
