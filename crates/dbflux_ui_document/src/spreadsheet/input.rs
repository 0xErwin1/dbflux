//! What a value typed into a spreadsheet cell is written as, and the
//! pending edits of a sheet as the patchers take them.
//!
//! The table stages every typed value as text. A save turns each one into a
//! [`CellEdit`] by these rules, in order:
//!
//! 1. An empty value clears the cell.
//! 2. A value starting with `'` is text, without the `'`.
//! 3. A value starting with `=` is a formula. An xlsx cell takes the text
//!    after `=`. An ods cell takes it as OpenFormula after `of:=`, or whole
//!    when it already starts with `of:=`; an A1-style reference outside
//!    brackets (`B2`, `$A$1`, `A1:B3`) is refused, because OpenFormula writes
//!    references as `[.B2]` and DBFlux does not translate them.
//! 4. `true` or `false`, in any case, typed into a cell that holds a
//!    boolean is a boolean.
//! 5. An ISO date (`2025-03-04`, `2025-03-04T10:30:00` or
//!    `2025-03-04 10:30:00`) typed into a cell whose number format is a date
//!    format is a date.
//! 6. A decimal number, such as `42`, `-1.5` or `1e3`, is a number.
//! 7. Anything else is text.

use std::collections::BTreeMap;

use chrono::{NaiveDate, NaiveDateTime};
use dbflux_components::components::data_table::model::{EditBuffer, VisualRowSource};
use dbflux_spreadsheet::CellEdit;

use super::grid_model::{CellTarget, SheetModel, cell_address};

/// The formula syntax of the workbook's format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FormulaSyntax {
    /// xlsx and xlsm: A1 references, written without the leading `=`.
    Excel,
    /// ods: OpenFormula, written after `of:=`.
    OpenFormula,
}

/// A typed value no rule can write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum InputRefusal {
    /// An ods formula holding an A1-style reference outside brackets.
    A1ReferenceInOpenFormula,
}

/// A refused value and the cell it was typed into, in A1 notation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CellRefusal {
    pub(super) cell: String,
    pub(super) refusal: InputRefusal,
}

/// One value staged in the table, at its position in the file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PendingCell {
    pub(super) row: usize,
    pub(super) column: usize,
    pub(super) text: String,

    /// Whether the cell belongs to an appended row.
    pub(super) appended: bool,
}

/// The edits of one sheet, ready for the patcher, and how many formula cells
/// they replace with a value.
#[derive(Clone, Debug)]
pub(super) struct SheetChanges {
    pub(super) cells: Result<BTreeMap<(usize, usize), CellEdit>, CellRefusal>,
    pub(super) replaced_formulas: usize,
}

/// The [`CellEdit`] a value typed into a cell is written as. See the module
/// documentation for the rules.
pub(super) fn cell_edit(
    input: &str,
    target: CellTarget,
    syntax: FormulaSyntax,
) -> Result<CellEdit, InputRefusal> {
    if input.is_empty() {
        return Ok(CellEdit::Clear);
    }

    if let Some(text) = input.strip_prefix('\'') {
        return Ok(CellEdit::Text(text.to_string()));
    }

    if let Some(formula) = input.strip_prefix('=') {
        return formula_edit(formula, syntax);
    }

    if target.holds_boolean {
        if input.eq_ignore_ascii_case("true") {
            return Ok(CellEdit::Bool(true));
        }

        if input.eq_ignore_ascii_case("false") {
            return Ok(CellEdit::Bool(false));
        }
    }

    if target.date_format
        && let Some(date) = iso_date(input)
    {
        return Ok(CellEdit::Date(date));
    }

    if let Some(number) = decimal_number(input) {
        return Ok(CellEdit::Number(number));
    }

    Ok(CellEdit::Text(input.to_string()))
}

fn formula_edit(formula: &str, syntax: FormulaSyntax) -> Result<CellEdit, InputRefusal> {
    match syntax {
        FormulaSyntax::Excel => Ok(CellEdit::Formula(formula.to_string())),

        FormulaSyntax::OpenFormula => {
            let body = formula.strip_prefix("of:=").unwrap_or(formula);

            if has_a1_reference(body) {
                return Err(InputRefusal::A1ReferenceInOpenFormula);
            }

            Ok(CellEdit::Formula(formula.to_string()))
        }
    }
}

/// Whether an OpenFormula expression holds an A1-style reference outside a
/// bracketed reference and outside a string literal: one to three letters
/// and digits, each optionally after `$`, as a whole token that is not a
/// function name (one followed by `(`).
#[expect(
    clippy::indexing_slicing,
    reason = "characters[index] is guarded by the loop condition, characters[index - 1] by the index == 0 short-circuit, and characters[index..end] holds because token_end returns index..=characters.len()"
)]
fn has_a1_reference(formula: &str) -> bool {
    let characters: Vec<char> = formula.chars().collect();
    let is_word = |character: char| character.is_alphanumeric() || matches!(character, '_' | '.');

    let mut index = 0;
    let mut bracket_depth = 0usize;
    let mut in_string = false;

    while index < characters.len() {
        let character = characters[index];

        if in_string {
            in_string = character != '"';
            index += 1;
            continue;
        }

        match character {
            '"' => in_string = true,
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            _ => {}
        }

        let starts_token = index == 0 || !is_word(characters[index - 1]);

        if bracket_depth == 0
            && starts_token
            && (character == '$' || character.is_ascii_alphabetic())
        {
            let end = token_end(&characters, index, is_word);
            let token: String = characters[index..end].iter().collect();
            let calls_a_function = characters.get(end) == Some(&'(');

            if !calls_a_function && is_a1_cell(&token) {
                return true;
            }

            index = end.max(index + 1);
            continue;
        }

        index += 1;
    }

    false
}

/// The index past the token starting at `start`: word characters and `$`.
#[expect(
    clippy::indexing_slicing,
    reason = "characters[end] is guarded by the loop condition end < characters.len()"
)]
fn token_end(characters: &[char], start: usize, is_word: impl Fn(char) -> bool) -> usize {
    let mut end = start;

    while end < characters.len() && (is_word(characters[end]) || characters[end] == '$') {
        end += 1;
    }

    end
}

/// Whether `token` is a cell in A1 notation, such as `B2`, `$A$1` or `XFD9`.
fn is_a1_cell(token: &str) -> bool {
    let token = token.strip_prefix('$').unwrap_or(token);
    let letters = token
        .chars()
        .take_while(|character| character.is_ascii_alphabetic())
        .count();

    if !(1..=3).contains(&letters) {
        return false;
    }

    let rest = &token[letters..];
    let digits = rest.strip_prefix('$').unwrap_or(rest);

    !digits.is_empty() && digits.chars().all(|character| character.is_ascii_digit())
}

/// `input` as a finite decimal number. Only digits, signs, a decimal point
/// and an exponent are accepted, so `inf`, `NaN` and `1,5` stay text.
fn decimal_number(input: &str) -> Option<f64> {
    let is_decimal = input.chars().any(|character| character.is_ascii_digit())
        && input.chars().all(|character| {
            character.is_ascii_digit() || matches!(character, '+' | '-' | '.' | 'e' | 'E')
        });

    if !is_decimal {
        return None;
    }

    input
        .parse::<f64>()
        .ok()
        .filter(|number| number.is_finite())
}

/// `input` as an ISO 8601 date, with an optional time.
fn iso_date(input: &str) -> Option<NaiveDateTime> {
    if let Ok(date) = NaiveDate::parse_from_str(input, "%Y-%m-%d") {
        return date.and_hms_opt(0, 0, 0);
    }

    ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"]
        .iter()
        .find_map(|format| NaiveDateTime::parse_from_str(input, format).ok())
}

/// Every value staged in the table of the sheet `model` shows: the edited
/// cells of its rows, and the non-empty cells of its appended rows at the
/// file rows they are written at.
pub(super) fn pending_cells(model: &SheetModel, buffer: &EditBuffer) -> Vec<PendingCell> {
    let mut cells = Vec::new();

    for row in buffer.dirty_rows() {
        for (column, value) in buffer.row_changes(row) {
            cells.push(PendingCell {
                row,
                column,
                text: value.edit_text(),
                appended: false,
            });
        }
    }

    if let Some(append_row) = model.append_row() {
        for (index, insert) in buffer.pending_inserts().iter().enumerate() {
            for (column, value) in insert.data.iter().enumerate() {
                let text = value.edit_text();

                if !text.is_empty() {
                    cells.push(PendingCell {
                        row: append_row + index,
                        column,
                        text,
                        appended: true,
                    });
                }
            }
        }
    }

    cells.sort_by_key(|cell| (cell.row, cell.column));
    cells
}

/// The edits of the sheet `model` shows, from the values staged in its
/// table. The first value no rule can write refuses the whole sheet.
pub(super) fn sheet_changes(
    model: &SheetModel,
    buffer: &EditBuffer,
    syntax: FormulaSyntax,
) -> SheetChanges {
    let pending = pending_cells(model, buffer);

    let replaced_formulas = pending
        .iter()
        .filter(|cell| {
            !cell.appended
                && model.has_formula(cell.row, cell.column)
                && !cell.text.starts_with('=')
        })
        .count();

    let cells = pending
        .iter()
        .map(|cell| {
            let row = (!cell.appended).then_some(cell.row);
            let target = model.target(row, cell.column);

            cell_edit(&cell.text, target, syntax)
                .map(|edit| ((cell.row, cell.column), edit))
                .map_err(|refusal| CellRefusal {
                    cell: cell_address(cell.row, cell.column),
                    refusal,
                })
        })
        .collect();

    SheetChanges {
        cells,
        replaced_formulas,
    }
}

/// The text staged for the cell shown at `visual_row` and `column`, when one
/// is: an edit of a sheet row, or a cell of an appended row.
pub(super) fn staged_text(buffer: &EditBuffer, visual_row: usize, column: usize) -> Option<String> {
    match buffer.visual_row_source(visual_row)? {
        VisualRowSource::Base(row) => buffer
            .row_changes(row)
            .into_iter()
            .find(|(changed, _)| *changed == column)
            .map(|(_, value)| value.edit_text()),

        VisualRowSource::Insert(index) => buffer
            .get_pending_insert_by_idx(index)?
            .get(column)
            .map(|value| value.edit_text()),
    }
}

#[cfg(test)]
mod tests {
    use dbflux_spreadsheet::CellEdit;

    use super::{FormulaSyntax, InputRefusal, cell_edit, has_a1_reference};
    use crate::spreadsheet::grid_model::CellTarget;

    const PLAIN: CellTarget = CellTarget {
        holds_boolean: false,
        date_format: false,
    };

    #[test]
    fn typed_values_follow_the_rules() {
        let excel = FormulaSyntax::Excel;

        assert_eq!(cell_edit("", PLAIN, excel), Ok(CellEdit::Clear));
        assert_eq!(
            cell_edit("'007", PLAIN, excel),
            Ok(CellEdit::Text("007".into()))
        );
        assert_eq!(
            cell_edit("=A1+1", PLAIN, excel),
            Ok(CellEdit::Formula("A1+1".into()))
        );
        assert_eq!(
            cell_edit("-1.5e2", PLAIN, excel),
            Ok(CellEdit::Number(-150.0))
        );
        assert_eq!(
            cell_edit("inf", PLAIN, excel),
            Ok(CellEdit::Text("inf".into()))
        );
        assert_eq!(
            cell_edit("NaN", PLAIN, excel),
            Ok(CellEdit::Text("NaN".into()))
        );
        assert_eq!(
            cell_edit("1e999", PLAIN, excel),
            Ok(CellEdit::Text("1e999".into()))
        );
        assert_eq!(
            cell_edit("true", PLAIN, excel),
            Ok(CellEdit::Text("true".into()))
        );

        let boolean = CellTarget {
            holds_boolean: true,
            ..PLAIN
        };
        assert_eq!(cell_edit("TRUE", boolean, excel), Ok(CellEdit::Bool(true)));

        let date = CellTarget {
            date_format: true,
            ..PLAIN
        };
        assert!(matches!(
            cell_edit("2025-03-04 10:30:00", date, excel),
            Ok(CellEdit::Date(_))
        ));
        assert_eq!(
            cell_edit("2025-03-04", PLAIN, excel),
            Ok(CellEdit::Text("2025-03-04".into()))
        );
    }

    #[test]
    fn ods_formulas_refuse_a1_references() {
        let ods = FormulaSyntax::OpenFormula;

        assert_eq!(
            cell_edit("=[.B2]*3", PLAIN, ods),
            Ok(CellEdit::Formula("[.B2]*3".into()))
        );
        assert_eq!(
            cell_edit("=of:=SUM([.A1:.A3])", PLAIN, ods),
            Ok(CellEdit::Formula("of:=SUM([.A1:.A3])".into()))
        );
        assert_eq!(
            cell_edit("=B2*3", PLAIN, ods),
            Err(InputRefusal::A1ReferenceInOpenFormula)
        );

        assert!(has_a1_reference("SUM(A1:B3)"));
        assert!(has_a1_reference("$A$1+1"));
        assert!(!has_a1_reference("LOG10(100)+[.XFD9]"));
        assert!(!has_a1_reference("CONCATENATE(\"A1\";[.B2])"));
        assert!(!has_a1_reference("PI()*2"));
    }
}
