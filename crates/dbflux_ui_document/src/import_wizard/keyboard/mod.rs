//! Keyboard paths of the import wizard's configure step.
//!
//! The CSV delimiter and encoding segments and each table's mapping-mode,
//! target-column and source-column dropdowns take no focus inside the dialog,
//! so the keymap's `ImportWizard` context reaches them through a cursor over
//! the step's choices: J and K move it, H and L pick the previous or next
//! delimiter or encoding, and Enter (which the dialog hands to its owner)
//! picks the next one or opens the dropdown under the cursor. Every path runs
//! the action of the mouse control.

use dbflux_components::primitives::{FocusShape, focus_ring};
use dbflux_components::tokens::ChamferCut;
use dbflux_core::keymap_types::Command;
use dbflux_export::{CsvDelimiter, TextEncoding};
use gpui::*;

use super::file_flow::FileImportState;
use super::file_options::{delimiter_segment_id, encoding_segment_id, visible_file_options};
use super::{ImportWizard, WizardStep};

/// A choice of the configure step the keyboard cursor can rest on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ConfigureField {
    Delimiter,
    Encoding,
    Mode(usize),
    Target(usize),
    Source(usize),
}

/// The next (`forward`) or previous entry of `all` after `current`, wrapping.
fn cycled<T: Copy + PartialEq>(all: &[T], current: T, forward: bool) -> Option<T> {
    let position = all.iter().position(|item| *item == current)?;
    let count = all.len();
    let next = if forward {
        (position + 1) % count
    } else {
        (position + count - 1) % count
    };
    all.get(next).copied()
}

/// Wraps `control` in the keyboard cursor's ring when `active` is `field`.
pub(super) fn with_cursor_ring(
    active: Option<ConfigureField>,
    field: ConfigureField,
    control: impl IntoElement,
    cx: &App,
) -> Div {
    focus_ring(
        active == Some(field),
        FocusShape::Chamfer(ChamferCut::CONTROL),
        None,
        control,
        cx,
    )
}

impl ImportWizard {
    /// How many choices the cursor visits for a file and a number of rows.
    pub(super) fn field_count(file: Option<&FileImportState>, rows: usize) -> usize {
        let file_fields = match file {
            Some(state) if visible_file_options(state.format).delimiter_and_encoding => 2,
            _ => 0,
        };

        file_fields + rows * 3
    }

    /// The choices the cursor visits, in reading order: the CSV delimiter and
    /// encoding when the picked file has them, then each row's mapping mode,
    /// target column and source column.
    pub(super) fn configure_fields(&self) -> Vec<ConfigureField> {
        let mut fields = Vec::new();

        if let Some(state) = &self.file_import
            && visible_file_options(state.format).delimiter_and_encoding
        {
            fields.push(ConfigureField::Delimiter);
            fields.push(ConfigureField::Encoding);
        }

        for table_index in 0..self.rows.len() {
            fields.push(ConfigureField::Mode(table_index));
            fields.push(ConfigureField::Target(table_index));
            fields.push(ConfigureField::Source(table_index));
        }

        fields
    }

    /// The choice under the cursor, when the configure step shows it.
    pub(super) fn cursor_field(&self) -> Option<ConfigureField> {
        if self.step != WizardStep::Configure {
            return None;
        }

        self.configure_fields().get(self.field_cursor).copied()
    }

    fn move_cursor(&mut self, forward: bool, cx: &mut Context<Self>) {
        if self.step != WizardStep::Configure {
            return;
        }

        let count = self.configure_fields().len();
        if count == 0 {
            return;
        }

        self.field_cursor = if forward {
            (self.field_cursor + 1).min(count - 1)
        } else {
            self.field_cursor.saturating_sub(1)
        };
        cx.notify();
    }

    /// Picks the next or previous delimiter or encoding through the same
    /// handler the segments call, so the file is read again with it.
    fn cycle_file_option(&mut self, field: ConfigureField, forward: bool, cx: &mut Context<Self>) {
        let Some(options) = self.file_import.as_ref().map(|state| state.options.clone()) else {
            return;
        };

        match field {
            ConfigureField::Delimiter => {
                if let Some(next) = cycled(&CsvDelimiter::ALL, options.delimiter, forward) {
                    self.select_delimiter(delimiter_segment_id(next), cx);
                }
            }
            ConfigureField::Encoding => {
                if let Some(next) = cycled(&TextEncoding::ALL, options.encoding, forward) {
                    self.select_encoding(encoding_segment_id(next), cx);
                }
            }
            ConfigureField::Mode(_) | ConfigureField::Target(_) | ConfigureField::Source(_) => {}
        }
    }

    /// Opens the dropdown of `field`, which then answers the menu keys
    /// itself and hands focus back to the dialog when it closes.
    fn open_field_dropdown(&mut self, field: ConfigureField, window: &mut Window, cx: &mut App) {
        let dropdown = match field {
            ConfigureField::Mode(index) => {
                self.rows.get(index).map(|row| &row.mapping_mode_dropdown)
            }
            ConfigureField::Target(index) => {
                self.rows.get(index).map(|row| &row.rebind_target_dropdown)
            }
            ConfigureField::Source(index) => {
                self.rows.get(index).map(|row| &row.rebind_source_dropdown)
            }
            ConfigureField::Delimiter | ConfigureField::Encoding => None,
        };

        if let Some(dropdown) = dropdown {
            dropdown.update(cx, |dropdown, cx| dropdown.focus_and_open(window, cx));
        }
    }

    /// Whether the target table or NULL text field holds the keyboard focus.
    fn text_field_has_focus(&self, window: &Window, cx: &App) -> bool {
        self.file_inputs.as_ref().is_some_and(|inputs| {
            [&inputs.target_table, &inputs.null_text]
                .into_iter()
                .any(|input| input.read(cx).focus_handle(cx).contains_focused(window, cx))
        })
    }

    /// Runs a keymap command the dialog's `ImportWizard` context bound.
    /// Every command is consumed: none may reach the document behind the
    /// dialog.
    pub(super) fn run_import_command(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Escape inside a text field only leaves the field: Tab never lands on
        // the dialog itself, so without this the cursor keys would stay out of
        // reach. The next Escape, now on the dialog, closes the wizard.
        if command == Command::Cancel && self.text_field_has_focus(window, cx) {
            self.focus_handle.focus(window, cx);
            cx.notify();
            return;
        }

        // Enter and the cursor keys reach here from a focused text field or an
        // open dropdown too (the dialog hands them to its owner); they act on
        // that control, never on the cursor.
        if command != Command::Cancel && !self.focus_handle.is_focused(window) {
            return;
        }

        match command {
            Command::Cancel => self.close(cx),
            Command::SelectNext => self.move_cursor(true, cx),
            Command::SelectPrev => self.move_cursor(false, cx),
            Command::ColumnRight | Command::ColumnLeft | Command::Execute => {
                let Some(field) = self.cursor_field() else {
                    return;
                };

                match field {
                    ConfigureField::Delimiter | ConfigureField::Encoding => {
                        self.cycle_file_option(field, command != Command::ColumnLeft, cx);
                    }
                    _ if command == Command::Execute => {
                        self.open_field_dropdown(field, window, cx);
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests;
