//! The wording of the problems a workbook's sheet can have, each as a danger
//! block that names the sheet, the cell or the size, then says what to do,
//! with the line the footer gives for the disabled Continue.

use dbflux_transfer::import_file::{DuplicateHeader, SheetIssue};

use super::ErrorBlock;

/// Columns named in one message; the rest are counted.
const LISTED_COLUMNS: usize = 6;

/// The letters a spreadsheet application gives a zero-based column: A to Z,
/// then AA, AB and so on.
pub(in crate::import_wizard) fn column_letters(index: usize) -> String {
    let mut remaining = index + 1;
    let mut letters = Vec::new();

    while remaining > 0 {
        let digit = (remaining - 1) % 26;
        letters.push(char::from(b'A' + digit as u8));
        remaining = (remaining - 1) / 26;
    }

    letters.iter().rev().collect()
}

/// `value` with thousands separated by commas (`14,200,000`).
fn group_digits(value: usize) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);

    for (position, digit) in digits.chars().enumerate() {
        if position > 0 && (digits.len() - position).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }

    grouped
}

/// `a`, `a and b`, `a, b and c`.
fn join_list(items: &[String]) -> String {
    match items {
        [] => String::new(),
        [only] => only.clone(),
        [head @ .., last] => dbflux_i18n::t!(
            "document.import_wizard.sheet.list_and",
            head = head.join(", "),
            last = last.as_str()
        ),
    }
}

/// The letters of the columns, the first few and a count of the rest.
fn columns_list(columns: &[usize]) -> String {
    let letters: Vec<String> = columns
        .iter()
        .take(LISTED_COLUMNS)
        .map(|column| column_letters(*column))
        .collect();
    let list = join_list(&letters);

    match columns.len().checked_sub(LISTED_COLUMNS) {
        Some(more) if more > 0 => dbflux_i18n::t!(
            "document.import_wizard.sheet.list_more",
            list = list,
            count = more
        ),
        _ => list,
    }
}

fn header_problems(blank_columns: &[usize], duplicates: &[DuplicateHeader]) -> String {
    let mut problems = Vec::new();

    if !blank_columns.is_empty() {
        let columns = columns_list(blank_columns);
        problems.push(if blank_columns.len() == 1 {
            dbflux_i18n::t!(
                "document.import_wizard.sheet.header.blank_one",
                columns = columns
            )
        } else {
            dbflux_i18n::t!(
                "document.import_wizard.sheet.header.blank_many",
                columns = columns
            )
        });
    }

    for duplicate in duplicates {
        let columns = columns_list(&duplicate.columns);
        problems.push(if duplicate.columns.len() == 2 {
            dbflux_i18n::t!(
                "document.import_wizard.sheet.header.duplicate_two",
                columns = columns,
                name = duplicate.name.as_str()
            )
        } else {
            dbflux_i18n::t!(
                "document.import_wizard.sheet.header.duplicate_many",
                columns = columns,
                name = duplicate.name.as_str()
            )
        });
    }

    join_list(&problems)
}

impl ErrorBlock {
    /// The block for a sheet that cannot be imported. It points at the Sheet
    /// dropdown when `other_sheets` says there is another sheet to choose,
    /// and otherwise only at fixing the file.
    pub(in crate::import_wizard) fn sheet_issue(issue: &SheetIssue, other_sheets: bool) -> Self {
        let mut block = Self::sheet_issue_with_others(issue);
        if !other_sheets {
            block.next_step = Self::single_sheet_next_step(issue);
        }

        block
    }

    fn single_sheet_next_step(issue: &SheetIssue) -> String {
        match issue {
            SheetIssue::Empty { .. } => {
                dbflux_i18n::t!("document.import_wizard.sheet.empty.next_single")
            }
            SheetIssue::InvalidHeader { .. } => {
                dbflux_i18n::t!("document.import_wizard.sheet.header.next_single")
            }
            SheetIssue::ErrorCell { .. } => {
                dbflux_i18n::t!("document.import_wizard.sheet.cell.next")
            }
            SheetIssue::TooLarge { .. } => {
                dbflux_i18n::t!("document.import_wizard.sheet.large.next_single")
            }
        }
    }

    fn sheet_issue_with_others(issue: &SheetIssue) -> Self {
        match issue {
            SheetIssue::Empty { sheet } => Self {
                message: dbflux_i18n::t!(
                    "document.import_wizard.sheet.empty.message",
                    sheet = sheet.as_str()
                ),
                next_step: dbflux_i18n::t!("document.import_wizard.sheet.empty.next"),
                footer_reason: Some(dbflux_i18n::t!(
                    "document.import_wizard.footer.reason.sheet_empty",
                    sheet = sheet.as_str()
                )),
            },
            SheetIssue::InvalidHeader {
                sheet,
                blank_columns,
                duplicate_names,
            } => Self {
                message: dbflux_i18n::t!(
                    "document.import_wizard.sheet.header.message",
                    sheet = sheet.as_str(),
                    problems = header_problems(blank_columns, duplicate_names)
                ),
                next_step: dbflux_i18n::t!("document.import_wizard.sheet.header.next"),
                footer_reason: Some(dbflux_i18n::t!(
                    "document.import_wizard.footer.reason.sheet_header"
                )),
            },
            SheetIssue::ErrorCell {
                sheet,
                row,
                column,
                column_name,
                code,
            } => Self {
                message: dbflux_i18n::t!(
                    "document.import_wizard.sheet.cell.message",
                    sheet = sheet.as_str(),
                    row = *row,
                    column = column_letters(*column),
                    name = column_name.as_str(),
                    code = *code
                ),
                next_step: dbflux_i18n::t!("document.import_wizard.sheet.cell.next"),
                footer_reason: Some(dbflux_i18n::t!(
                    "document.import_wizard.footer.reason.sheet_cell"
                )),
            },
            SheetIssue::TooLarge {
                sheet,
                cells,
                limit,
            } => Self {
                message: dbflux_i18n::t!(
                    "document.import_wizard.sheet.large.message",
                    sheet = sheet.as_str(),
                    cells = group_digits(*cells),
                    limit = group_digits(*limit)
                ),
                next_step: dbflux_i18n::t!("document.import_wizard.sheet.large.next"),
                footer_reason: Some(dbflux_i18n::t!(
                    "document.import_wizard.footer.reason.sheet_large",
                    limit = group_digits(*limit)
                )),
            },
        }
    }
}
