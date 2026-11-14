//! Save as .xlsx: an xls workbook, which DBFlux cannot write, written as a
//! new xlsx file that can be edited.
//!
//! A prompt first says what the new file does not keep. Confirming opens the
//! file dialog, which proposes the file's name with `.xlsx` next to the
//! original; an object is written to this computer only. The original file
//! is never the target: choosing it is refused. The values of every
//! worksheet are then read one sheet at a time and written on the background
//! executor, into a staging file next to the target that is renamed over it
//! once complete. The new file is reported and handed to the callback that
//! opens it.

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use dbflux_components::modals::ModalFocus;
use dbflux_spreadsheet::{SheetKind, SpreadsheetError, SpreadsheetFormat, ValuesWriteError};
use dbflux_ui_base::toast::Toast;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use dbflux_ui_base::{SaveTargetOutcome, SaveTargetProvider, SaveTargetRequest};
use gpui::*;

use super::document::{SheetSource, SpreadsheetDocument, spreadsheet_error_to_user_facing};
use crate::dedup::FileDocumentKey;

const XLSX_EXTENSION: &str = "xlsx";

/// Told the path of a file Save as .xlsx wrote.
pub(super) type SavedAsCallback = Rc<dyn Fn(PathBuf, &mut App)>;

/// The open prompt that says what Save as .xlsx does not keep.
pub(super) struct SaveAsPrompt {
    focus: ModalFocus,
}

impl SaveAsPrompt {
    pub(super) fn focus_mut(&mut self) -> &mut ModalFocus {
        &mut self.focus
    }
}

/// Why Save as .xlsx wrote no new file.
#[derive(Debug)]
enum SaveAsError {
    /// The original workbook could not be opened again.
    Open(SpreadsheetError),

    /// The values could not be read or written.
    Values(ValuesWriteError),

    /// The staging file or its rename over the target failed.
    Local {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl SpreadsheetDocument {
    /// Makes Save as .xlsx ask `provider` where to write instead of opening
    /// the file dialog. `None` restores the dialog.
    pub fn set_save_target_override(&mut self, provider: Option<SaveTargetProvider>) {
        self.save_target_override = provider;
    }

    /// Sets what is told the path of each file Save as .xlsx wrote, so the
    /// caller opens it.
    pub fn set_on_saved_as(&mut self, on_saved_as: impl Fn(PathBuf, &mut App) + 'static) {
        self.on_saved_as = Some(Rc::new(on_saved_as));
    }

    /// Whether the workbook is an xls file, the only format Save as .xlsx
    /// is for.
    pub(super) fn offers_save_as(&self) -> bool {
        self.loaded()
            .is_some_and(|loaded| loaded.format == SpreadsheetFormat::Xls)
    }

    /// Whether Save as .xlsx can start now: the xls workbook has a worksheet
    /// to write, and no Save as .xlsx is open or running.
    pub(super) fn can_save_as_xlsx(&self) -> bool {
        self.offers_save_as()
            && !self.saving_as
            && self.save_as_prompt.is_none()
            && self
                .sheets()
                .iter()
                .any(|sheet| sheet.kind == SheetKind::Worksheet)
    }

    /// Opens the prompt that says what the new .xlsx file does not keep.
    /// Does nothing when Save as .xlsx cannot start.
    pub fn save_as_xlsx(&mut self, cx: &mut Context<Self>) {
        if !self.can_save_as_xlsx() {
            return;
        }

        let mut focus = ModalFocus::new(cx);
        focus.focus_on_next_render();

        self.save_as_prompt = Some(SaveAsPrompt { focus });
        cx.notify();
    }

    /// What the open Save as .xlsx prompt says, and `None` while it is
    /// closed.
    pub fn save_as_prompt_message(&self) -> Option<String> {
        self.save_as_prompt.as_ref()?;

        let chart_sheets: Vec<String> = self
            .sheets()
            .iter()
            .filter(|sheet| sheet.kind != SheetKind::Worksheet)
            .map(|sheet| sheet.name.clone())
            .collect();

        let object = matches!(self.file(), FileDocumentKey::Object { .. });

        Some(crate::labels::spreadsheet_save_as_body(
            &self.title(),
            &chart_sheets,
            object,
        ))
    }

    /// Closes the Save as .xlsx prompt without writing anything.
    pub fn dismiss_save_as_prompt(&mut self, cx: &mut Context<Self>) {
        let Some(mut prompt) = self.save_as_prompt.take() else {
            return;
        };

        prompt.focus.restore(cx);
        cx.notify();
    }

    /// Closes the prompt, asks where to write the new .xlsx file, and writes
    /// it there on the background executor. Does nothing when no prompt is
    /// open.
    pub fn confirm_save_as(&mut self, cx: &mut Context<Self>) {
        let Some(mut prompt) = self.save_as_prompt.take() else {
            return;
        };

        prompt.focus.restore(cx);

        let Some(source) = self.loaded().map(|loaded| loaded.source.clone()) else {
            cx.notify();
            return;
        };

        let original = match self.file() {
            FileDocumentKey::Local { path } => Some(path.clone()),
            FileDocumentKey::Object { .. } => None,
        };

        let title = self.title();
        let suggested_name = format!("{}.{XLSX_EXTENSION}", file_stem(&title));
        let provider = self.save_target_override.clone();
        let on_saved_as = self.on_saved_as.clone();

        self.saving_as = true;
        cx.notify();

        cx.spawn(async move |this, cx| {
            let target =
                choose_target(provider, &suggested_name, original.as_deref(), &title, cx).await;

            let result = match target {
                Some(target) => {
                    let write = cx.background_executor().spawn({
                        let target = target.clone();
                        async move { write_new_xlsx(&source, &target) }
                    });

                    Some((target, write.await))
                }

                None => None,
            };

            cx.update(|cx| {
                this.update(cx, |document, cx| {
                    document.saving_as = false;
                    cx.notify();
                })
                .ok();

                let Some((target, result)) = result else {
                    return;
                };

                match result {
                    Ok(()) => {
                        let name = target
                            .file_name()
                            .map(|name| name.to_string_lossy().into_owned())
                            .unwrap_or_else(|| target.display().to_string());

                        Toast::success(crate::labels::spreadsheet_save_as_saved_message(&name))
                            .push(cx);

                        if let Some(on_saved_as) = on_saved_as {
                            on_saved_as(target, cx);
                        }
                    }

                    Err(error) => report_error(save_as_error_to_user_facing(&error, &title), cx),
                }
            });
        })
        .detach();
    }
}

/// Asks the file dialog, or the override, where to write. `None` when the
/// dialog was cancelled, when no target could be chosen, or when the target
/// is the original file; the last two are reported here.
async fn choose_target(
    provider: Option<SaveTargetProvider>,
    suggested_name: &str,
    original: Option<&Path>,
    title: &str,
    cx: &mut AsyncApp,
) -> Option<PathBuf> {
    let filter = dbflux_i18n::t!("document.spreadsheet.save_as.dialog_filter");
    let dialog_title = match original {
        Some(_) => dbflux_i18n::t!("document.spreadsheet.save_as.dialog_title"),
        None => dbflux_i18n::t!("document.spreadsheet.save_as.dialog_title_object"),
    };
    let directory = original.and_then(Path::parent).map(Path::to_path_buf);

    let outcome = dbflux_ui_base::file_dialog::resolve_save_target(
        provider,
        SaveTargetRequest {
            suggested_name,
            language_name: &filter,
            default_extension: XLSX_EXTENSION,
        },
        async {
            let mut dialog = rfd::AsyncFileDialog::new()
                .set_title(dialog_title)
                .set_file_name(suggested_name)
                .add_filter(&filter, &[XLSX_EXTENSION]);

            if let Some(directory) = &directory {
                dialog = dialog.set_directory(directory);
            }

            dialog
                .save_file()
                .await
                .map(|handle| handle.path().to_path_buf())
        },
    )
    .await;

    let summary = crate::labels::spreadsheet_save_as_failed_message(title);

    let target = match outcome {
        SaveTargetOutcome::Selected { path, .. } => path,
        SaveTargetOutcome::Cancelled => return None,
        SaveTargetOutcome::Failed(error) => {
            cx.update(|cx| {
                report_error(
                    UserFacingError::new(ErrorKind::Storage, summary).with_cause(dbflux_i18n::t!(
                        "document.spreadsheet.save_as.error.dialog_unavailable",
                        error = error
                    )),
                    cx,
                );
            });
            return None;
        }
    };

    let target = with_resolved_directory(&target);

    if original.is_some_and(|original| is_same_file(original, &target)) {
        cx.update(|cx| {
            report_error(
                UserFacingError::new(ErrorKind::User, summary).with_cause(dbflux_i18n::t!(
                    "document.spreadsheet.save_as.error.replaces_source"
                )),
                cx,
            );
        });
        return None;
    }

    Some(target)
}

/// The file name without its last extension: `book` for `book.xls`.
fn file_stem(file_name: &str) -> &str {
    match file_name.rsplit_once('.') {
        Some((stem, _)) if !stem.is_empty() => stem,
        _ => file_name,
    }
}

/// `target` with its directory resolved to a path without links, so the file
/// written is the one checked against the original even when a link in the
/// chosen path is changed between the check and the write. `target` as given
/// when its directory cannot be resolved; writing there then fails.
fn with_resolved_directory(target: &Path) -> PathBuf {
    let (Some(directory), Some(name)) = (target.parent(), target.file_name()) else {
        return target.to_path_buf();
    };

    let directory = if directory.as_os_str().is_empty() {
        Path::new(".")
    } else {
        directory
    };

    match std::fs::canonicalize(directory) {
        Ok(resolved) => resolved.join(name),
        Err(_) => target.to_path_buf(),
    }
}

/// Whether `target` names the file at `original`, however it is spelled.
fn is_same_file(original: &Path, target: &Path) -> bool {
    if original == target {
        return true;
    }

    match (
        std::fs::canonicalize(original),
        std::fs::canonicalize(target),
    ) {
        (Ok(original), Ok(target)) => original == target,
        _ => false,
    }
}

/// Opens the workbook `source` reads again and writes the values of its
/// worksheets into a new xlsx file at `target`, replacing what is there only
/// once the file is complete. Blocks on reading and writing.
fn write_new_xlsx(source: &SheetSource, target: &Path) -> Result<(), SaveAsError> {
    let mut workbook = dbflux_spreadsheet::open(source.clone()).map_err(SaveAsError::Open)?;

    let directory = match target.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let staged_path = directory.join(format!(".dbflux-stage-{}.tmp", uuid::Uuid::new_v4()));

    let local_error = |source| SaveAsError::Local {
        path: target.to_path_buf(),
        source,
    };

    let staged = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staged_path)
        .map_err(local_error)?;

    let written = write_staged(&mut workbook, &staged).and_then(|()| {
        staged.sync_all().map_err(local_error)?;
        drop(staged);

        std::fs::rename(&staged_path, target).map_err(local_error)
    });

    if written.is_err()
        && let Err(error) = std::fs::remove_file(&staged_path)
    {
        log::warn!(
            "Failed to remove staging file {}: {error}",
            staged_path.display()
        );
    }

    written
}

fn write_staged(
    workbook: &mut super::document::OpenWorkbook,
    staged: &std::fs::File,
) -> Result<(), SaveAsError> {
    let mut sink = BufWriter::new(staged);

    dbflux_spreadsheet::write_values_xlsx(workbook, &mut sink).map_err(SaveAsError::Values)?;

    sink.flush().map_err(|source| {
        SaveAsError::Values(ValuesWriteError::Write {
            message: source.to_string(),
        })
    })
}

/// The user-facing error of a Save as .xlsx that wrote no file. A value or
/// sheet name the xlsx format refuses is the user's to change; everything
/// else is a file that could not be read or written.
fn save_as_error_to_user_facing(error: &SaveAsError, title: &str) -> UserFacingError {
    let summary = crate::labels::spreadsheet_save_as_failed_message(title);

    match error {
        SaveAsError::Open(source) => spreadsheet_error_to_user_facing(
            source,
            summary,
            crate::labels::spreadsheet_error_cause(source, None),
        ),

        SaveAsError::Values(values) => {
            let cause = crate::labels::spreadsheet_values_write_error_cause(values);

            match values {
                ValuesWriteError::Read { source, .. } => {
                    spreadsheet_error_to_user_facing(source, summary, cause)
                }

                ValuesWriteError::Sheet { .. } | ValuesWriteError::Cell { .. } => {
                    UserFacingError::new(ErrorKind::User, summary).with_cause(cause)
                }

                ValuesWriteError::Write { .. } => {
                    UserFacingError::new(ErrorKind::Storage, summary).with_cause(cause)
                }
            }
        }

        SaveAsError::Local { path, source } => UserFacingError::new(ErrorKind::Storage, summary)
            .with_cause(dbflux_i18n::t!(
                "document.file.error.storage.local_io",
                path = path.display().to_string(),
                cause = source
            )),
    }
}
