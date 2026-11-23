//! The Sheet row of the Configure step for a spreadsheet: which worksheet of
//! the workbook the import reads. Only visible worksheets are offered. The
//! first one is chosen at the start and choosing another reads the file again.

use std::path::Path;

use dbflux_components::controls::{Dropdown, DropdownItem, DropdownSelectionChanged};
use dbflux_components::modals::modal_hint;
use dbflux_components::tokens::ui;
use dbflux_transfer::import_file::{ImportSheet, list_sheets};
use dbflux_transfer::{FileFormat, TransferError};
use gpui::*;

use super::file_flow::FileImportState;
use super::keyboard::{ConfigureField, with_cursor_ring};
use super::{FileInputs, ImportWizard};
use crate::export_wizard::notes::{form_row, info_line};

/// Width of the Sheet dropdown. (220 px at the base font size)
const SHEET_DROPDOWN_WIDTH: Rems = ui(220.0);

/// Whether a file of this format is imported one sheet at a time.
pub(super) fn uses_sheet_picker(format: FileFormat) -> bool {
    format == FileFormat::Spreadsheet
}

/// The visible worksheets of a workbook and the one the import reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SheetChoice {
    pub(super) sheets: Vec<String>,
    pub(super) selected: String,
}

/// The names of the worksheets a person can see, in workbook order.
pub(super) fn visible_sheet_names(sheets: Vec<ImportSheet>) -> Vec<String> {
    sheets
        .into_iter()
        .filter(|sheet| sheet.visible)
        .map(|sheet| sheet.name)
        .collect()
}

/// Lists the visible worksheets of the workbook at `path` and picks
/// `requested` when it is one of them, else the first.
pub(super) fn choose_sheet(
    path: &Path,
    requested: Option<&str>,
) -> Result<SheetChoice, TransferError> {
    let sheets = visible_sheet_names(list_sheets(path)?);
    let selected = requested
        .filter(|name| sheets.iter().any(|sheet| sheet == name))
        .or_else(|| sheets.first().map(String::as_str))
        .map(str::to_string)
        .ok_or_else(|| {
            TransferError::Source(format!(
                "{}: the workbook has no visible worksheet to import",
                path.display()
            ))
        })?;

    Ok(SheetChoice { sheets, selected })
}

impl FileInputs {
    /// The Sheet dropdown. It takes no focus on its own: the Configure
    /// cursor opens it, like the mapping dropdowns.
    pub(super) fn new_sheet_dropdown(
        cx: &mut Context<ImportWizard>,
    ) -> (Entity<Dropdown>, Subscription) {
        let dropdown = cx.new(|_cx| Dropdown::new("import-sheet"));
        let subscription = cx.subscribe(
            &dropdown,
            |this, _dropdown, event: &DropdownSelectionChanged, cx| {
                this.select_sheet(event.index, cx);
            },
        );

        (dropdown, subscription)
    }
}

impl ImportWizard {
    /// Keeps the dropdown's list, selection and enabled state in step with
    /// the file state: a workbook with one visible sheet has nothing to
    /// choose, so its dropdown is off.
    pub(super) fn sync_sheet_dropdown(&self, cx: &mut Context<Self>) {
        let (Some(state), Some(inputs)) = (self.file_import.as_ref(), self.file_inputs.as_ref())
        else {
            return;
        };
        let items: Vec<DropdownItem> = state
            .sheets
            .iter()
            .map(|name| DropdownItem::new(name.clone()))
            .collect();
        let selected = state
            .sheet
            .as_ref()
            .and_then(|name| state.sheets.iter().position(|sheet| sheet == name));
        let single = state.sheets.len() <= 1;

        inputs.sheet_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(items, cx);
            dropdown.set_selected_index(selected, cx);
            dropdown.set_disabled(single, cx);
        });
    }

    /// Applies the sheets a read found. A read that is no longer the latest
    /// is ignored.
    pub(super) fn apply_sheet_choice(
        &mut self,
        generation: u64,
        choice: Option<SheetChoice>,
        cx: &mut Context<Self>,
    ) {
        let Some(state) = self
            .file_import
            .as_mut()
            .filter(|state| state.generation == generation)
        else {
            return;
        };

        if let Some(choice) = choice {
            state.sheets = choice.sheets;
            state.sheet = Some(choice.selected);
        }
        self.sync_sheet_dropdown(cx);
    }

    /// Reads the file again for the sheet at `index` of the list.
    pub(super) fn select_sheet(&mut self, index: usize, cx: &mut Context<Self>) {
        let Some(state) = self.file_import.as_mut() else {
            return;
        };
        let Some(name) = state.sheets.get(index).cloned() else {
            return;
        };
        if state.sheet.as_deref() == Some(name.as_str()) {
            return;
        }

        state.sheet = Some(name);
        self.reload_file(cx);
    }

    /// Whether the Sheet dropdown can be chosen from, and so joins the
    /// Configure cursor.
    pub(super) fn sheet_is_choosable(state: &FileImportState) -> bool {
        uses_sheet_picker(state.format) && state.sheets.len() > 1
    }

    /// The Sheet row: the dropdown and what it does, or why there is nothing
    /// to choose.
    pub(super) fn render_sheet_row(
        &self,
        state: &FileImportState,
        inputs: &FileInputs,
        active: Option<ConfigureField>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let hint = if Self::sheet_is_choosable(state) {
            div()
                .debug_selector(|| "import-wizard-sheet-hint".to_string())
                .child(modal_hint(
                    dbflux_i18n::t!("document.import_wizard.file.sheet_hint"),
                    cx,
                ))
                .into_any_element()
        } else {
            info_line(
                dbflux_i18n::t!("document.import_wizard.file.sheet_single"),
                "import-wizard-sheet-single",
                cx,
            )
        };

        form_row(
            dbflux_i18n::t!("document.import_wizard.file.sheet_label"),
            div()
                .debug_selector(|| "import-wizard-sheet".to_string())
                .w(SHEET_DROPDOWN_WIDTH)
                .child(with_cursor_ring(
                    active,
                    ConfigureField::Sheet,
                    inputs.sheet_dropdown.clone(),
                    cx,
                )),
            Some(hint),
            cx,
        )
        .into_any_element()
    }
}

#[cfg(test)]
pub(super) mod tests;
