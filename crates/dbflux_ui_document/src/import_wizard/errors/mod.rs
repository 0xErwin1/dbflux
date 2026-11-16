//! What the import wizard tells the user when a step cannot go on: the
//! message and, always, the next step to take. Rendered as one danger block
//! on both the Pick Source and the Configure step.

mod sheet;

use dbflux_transfer::import_file::SheetIssue;
use dbflux_transfer::{FileFormat, TransferError};

/// An error shown to the user: what went wrong and what to do next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ErrorBlock {
    pub(super) message: String,
    pub(super) next_step: String,
    /// Why Continue is off while this block shows, when the generic "the file
    /// has not been read" line would not say it.
    pub(super) footer_reason: Option<String>,
}

/// Why a picked file could not be turned into a table plan.
#[derive(Debug)]
pub(super) enum FileLoadError {
    /// The file itself could not be read with the current options.
    Read(TransferError),
    /// The chosen sheet of a workbook cannot be imported; another sheet can
    /// be chosen on the Configure step.
    Sheet(SheetIssue),
    /// The target table could not be looked up, so whether it exists is
    /// unknown. The text is the lookup's own error.
    Lookup(String),
}

impl ErrorBlock {
    pub(super) fn no_connection() -> Self {
        Self {
            message: dbflux_i18n::t!("document.import_wizard.pick_folder.error.no_connection"),
            next_step: dbflux_i18n::t!(
                "document.import_wizard.pick_folder.error.no_connection_next"
            ),
            footer_reason: None,
        }
    }

    pub(super) fn no_dialog() -> Self {
        Self {
            message: dbflux_i18n::t!("document.import_wizard.pick_folder.error.no_dialog"),
            next_step: dbflux_i18n::t!("document.import_wizard.pick_folder.error.no_dialog_next"),
            footer_reason: None,
        }
    }

    pub(super) fn invalid_bundle(error: &str) -> Self {
        Self {
            message: dbflux_i18n::t!(
                "document.import_wizard.pick_folder.error.invalid_bundle",
                error = error
            ),
            next_step: dbflux_i18n::t!(
                "document.import_wizard.pick_folder.error.invalid_bundle_next"
            ),
            footer_reason: None,
        }
    }

    pub(super) fn unsupported_file() -> Self {
        Self {
            message: dbflux_i18n::t!("document.import_wizard.pick_file.error.unsupported"),
            next_step: dbflux_i18n::t!("document.import_wizard.pick_file.error.unsupported_next"),
            footer_reason: None,
        }
    }

    /// The block for a failed file read. A CSV can be fixed on the Configure
    /// step by another delimiter or encoding; a JSON file has to be fixed or
    /// replaced; a failed table lookup is retried with Use Table.
    pub(super) fn file_load_failure(failure: &FileLoadError, format: FileFormat) -> Self {
        match failure {
            FileLoadError::Lookup(error) => Self {
                message: dbflux_i18n::t!(
                    "document.import_wizard.pick_file.error.lookup_failed",
                    error = error.as_str()
                ),
                next_step: dbflux_i18n::t!("document.import_wizard.pick_file.error.next_lookup"),
                footer_reason: None,
            },
            FileLoadError::Sheet(issue) => Self::sheet_issue(issue, true),
            FileLoadError::Read(error) => Self {
                message: dbflux_i18n::t!(
                    "document.import_wizard.pick_file.error.invalid_file",
                    error = error.to_string()
                ),
                next_step: match format {
                    FileFormat::Csv => {
                        dbflux_i18n::t!("document.import_wizard.pick_file.error.next_csv")
                    }
                    FileFormat::Xml => {
                        dbflux_i18n::t!("document.import_wizard.pick_file.error.next_xml")
                    }
                    FileFormat::Spreadsheet => {
                        dbflux_i18n::t!("document.import_wizard.pick_file.error.next_spreadsheet")
                    }
                    _ => dbflux_i18n::t!("document.import_wizard.pick_file.error.next_json"),
                },
                footer_reason: None,
            },
        }
    }
}

#[cfg(test)]
mod tests;
