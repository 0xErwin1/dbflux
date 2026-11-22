//! Conversions between the positions a database reports for a failed
//! statement and the byte offsets and line/column pairs the editor uses.

/// Converts a 1-based character position, as PostgreSQL reports it, into the
/// byte offset of that character in `text`.
///
/// A position one past the last character is the end of the input, which the
/// server reports for a statement that stops too early. Returns `None` for
/// position 0 and for a position beyond the end.
pub fn char_position_to_byte_offset(text: &str, position: usize) -> Option<usize> {
    let index = position.checked_sub(1)?;

    match text.char_indices().nth(index) {
        Some((offset, _)) => Some(offset),
        None => (text.chars().count() == index).then_some(text.len()),
    }
}

/// Converts a byte offset in `text` into a 0-based line and a 0-based column
/// counted in characters, the unit the editor positions the cursor in.
///
/// Lines end at `\n`, so a `\r` of a CRLF pair counts as the last column of
/// its line. Returns `None` when `offset` is beyond the end of `text` or
/// falls inside a multi-byte character.
pub fn byte_offset_to_line_column(text: &str, offset: usize) -> Option<(usize, usize)> {
    let before = text.get(..offset)?;
    let line = before.matches('\n').count();
    let line_start = before.rfind('\n').map_or(0, |index| index + 1);
    let column = before.get(line_start..)?.chars().count();

    Some((line, column))
}

/// Finds where each statement of a script starts in the request, in order.
///
/// A statement the splitter returns is a trimmed slice of the request, so the
/// next occurrence at or after the end of the previous one is its start.
pub struct StatementLocator<'a> {
    request: &'a str,
    cursor: usize,
}

impl<'a> StatementLocator<'a> {
    pub fn new(request: &'a str) -> Self {
        Self { request, cursor: 0 }
    }

    pub fn locate(&mut self, statement: &str) -> Option<usize> {
        let found = self.cursor + self.request.get(self.cursor..)?.find(statement)?;
        self.cursor = found + statement.len();

        Some(found)
    }

    /// Runs `error` through the position of the statement it came from.
    pub fn place(error: crate::DbError, start: Option<usize>) -> crate::DbError {
        match start {
            Some(start) => error.with_position_shifted(start),
            None => error.without_position(),
        }
    }
}

#[cfg(test)]
mod tests;
