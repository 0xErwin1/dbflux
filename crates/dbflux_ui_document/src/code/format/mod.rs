//! The editor's Format command: reformats the selection, or the statement
//! under the cursor, with the connection's language service.

use super::*;
use std::ops::Range;

/// The byte range Format rewrites: the selection when there is one, else the
/// statement under the cursor with Run Statement's boundaries, else the whole
/// buffer (a single statement, or a language without a statement splitter).
pub(super) fn format_target(
    language: &QueryLanguage,
    buffer: &str,
    selection: Range<usize>,
    cursor: usize,
) -> Range<usize> {
    let start = selection.start.min(selection.end).min(buffer.len());
    let end = selection.start.max(selection.end).min(buffer.len());

    if start < end {
        return start..end;
    }

    statements::cursor_statement(language, buffer, cursor).unwrap_or(0..buffer.len())
}

/// The formatted text for `buffer[range]`, keeping the whitespace around it
/// so a selection that ends in a newline still does. `None` when the range
/// is blank, the formatter declines it, or nothing would change.
pub(super) fn formatted_region(
    buffer: &str,
    range: &Range<usize>,
    format: impl Fn(&str) -> Option<String>,
) -> Option<String> {
    let region = buffer.get(range.clone())?;
    let body = region.trim();
    if body.is_empty() {
        return None;
    }

    let leading = &region[..region.len() - region.trim_start().len()];
    let trailing = &region[region.trim_end().len()..];
    let formatted = format(body)?;
    let replacement = format!("{leading}{}{trailing}", formatted.trim());

    (replacement != region).then_some(replacement)
}

impl CodeDocument {
    /// Whether the connection's language service can format this document.
    /// A document without a connection, such as a script, cannot. The
    /// document must also be in SQL: connections that do not provide their own
    /// language service fall back to the SQL one, whatever their language.
    pub(super) fn can_format(&self, cx: &App) -> bool {
        let Some(connection_id) = self.connection_id else {
            return false;
        };

        if !matches!(self.effective_language(), QueryLanguage::Sql) {
            return false;
        }

        self.app_state
            .read(cx)
            .connections()
            .get(&connection_id)
            .is_some_and(|connected| connected.connection.language_service().can_format())
    }

    /// Reformats the selection, or the statement under the cursor, in place.
    /// Returns `false` when the document is read-only or cannot format, so
    /// the key falls through.
    pub(super) fn format_query(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        if self.read_only || !self.can_format(cx) {
            return false;
        }

        let (buffer, selection, cursor) = {
            let state = self.editor.input_state.read(cx);
            (
                state.value().to_string(),
                state.selected_range(),
                state.cursor(),
            )
        };
        let had_selection = selection.start != selection.end;
        let range = format_target(self.effective_language(), &buffer, selection, cursor);

        let replacement = {
            let Some(connection_id) = self.connection_id else {
                return false;
            };
            let app_state = self.app_state.read(cx);
            let Some(connected) = app_state.connections().get(&connection_id) else {
                return false;
            };
            let service = connected.connection.language_service();
            formatted_region(&buffer, &range, |text| service.format(text))
        };

        // Formatted already: the command is handled, there is just nothing to do.
        let Some(replacement) = replacement else {
            return true;
        };

        let region = super::comment::utf16_range(&buffer, &range);
        let next_selection = if had_selection {
            range.start..range.start + replacement.len()
        } else {
            let end = range.start + replacement.len();
            end..end
        };

        // Same as a comment toggle: a programmatic rewrite, not a keystroke.
        self.editor.programmatic_edit = true;
        self.editor.input_state.update(cx, |state, cx| {
            let scroll_offset = state.scroll_offset();
            let cursor = state.cursor_position();
            state.set_cursor_position(cursor, window, cx);
            state.replace_text_in_range(Some(region), &replacement, window, cx);
            state.set_selected_range(next_selection, cx);
            state.set_scroll_offset(scroll_offset, cx);
        });

        true
    }
}

#[cfg(test)]
mod tests;
