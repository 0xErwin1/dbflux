//! The marker a failed run leaves on the token the server reported.
//!
//! A driver that knows where its server rejected the SQL reports it as
//! `FormattedError::position`, a byte offset into the text it was given. Each
//! run remembers where that text started in the buffer, so a failure can be
//! tied back to the token. The marker is an error diagnostic that lives until
//! the next edit or run, and "Go to error" moves the cursor onto it on
//! request. Nothing moves on its own.

use super::*;
use std::ops::Range;

/// The token the server named in the last failed run.
pub(crate) struct QueryErrorMarker {
    /// Byte range of the token in the buffer the run was started from.
    pub(crate) range: Range<usize>,
    pub(crate) message: String,
}

/// Where `sent` starts in `buffer`, when `candidate` is that place.
///
/// The text a run sends is not always the buffer slice it came from: time
/// macros are substituted first, and the buffer may change while the query
/// runs. Only an exact match ties a reported position to the buffer.
pub(super) fn verified_start(buffer: &str, sent: &str, candidate: Option<usize>) -> Option<usize> {
    let start = candidate?;
    (buffer.get(start..start.checked_add(sent.len())?)? == sent).then_some(start)
}

/// The extent of the token at `offset`: an identifier, keyword or number is
/// taken whole, a quoted string or identifier through its closing quote, and
/// any other character on its own. At the end of the text the range is empty.
pub(super) fn token_range(text: &str, offset: usize) -> Range<usize> {
    let Some(rest) = text.get(offset..) else {
        return text.len()..text.len();
    };
    let Some(first) = rest.chars().next() else {
        return offset..offset;
    };

    let length = if first.is_alphanumeric() || first == '_' {
        rest.find(|character: char| !(character.is_alphanumeric() || character == '_'))
            .unwrap_or(rest.len())
    } else if matches!(first, '\'' | '"' | '`') {
        rest[first.len_utf8()..]
            .find(first)
            .map_or(first.len_utf8(), |closing| {
                first.len_utf8() + closing + first.len_utf8()
            })
    } else {
        first.len_utf8()
    };

    offset..offset + length
}

/// The marker for a failure at `position` of the text sent from
/// `buffer_start`, or `None` when the sent text is no longer what the buffer
/// holds there.
pub(super) fn marker_for(
    buffer: &str,
    sent: &str,
    buffer_start: Option<usize>,
    position: usize,
    message: &str,
) -> Option<QueryErrorMarker> {
    let start = verified_start(buffer, sent, buffer_start)?;
    if position > sent.len() {
        return None;
    }

    let token = token_range(buffer, start + position);

    Some(QueryErrorMarker {
        range: token,
        message: message.to_string(),
    })
}

impl CodeDocument {
    /// Where the text of the run being started sits in the buffer: the
    /// statement or buffer start `query_origin` recorded, or the start of the
    /// selection, trimmed the way the run trims it. `None` when the sent text
    /// is not a slice of the buffer.
    pub(super) fn sent_text_start(&self, sent: &str, cx: &App) -> Option<usize> {
        let state = self.editor.input_state.read(cx);
        let buffer = state.value();

        let candidate = self.execution.query_origin.or_else(|| {
            let ranges = state.selected_nonempty_ranges();
            let [range] = ranges.as_slice() else {
                return None;
            };
            let raw = buffer.get(range.clone())?;

            Some(range.start + (raw.len() - raw.trim_start().len()))
        });

        verified_start(&buffer, sent, candidate)
    }

    /// Replaces the marker with the one for a failed run, or removes it when
    /// the failure cannot be tied to the buffer.
    pub(super) fn set_query_error(
        &mut self,
        marker: Option<QueryErrorMarker>,
        cx: &mut Context<Self>,
    ) {
        let changed = marker.is_some() || self.editor.query_error.is_some();
        self.editor.query_error = marker;

        if changed {
            self.run_diagnostics(cx);
        }
    }

    pub(super) fn clear_query_error(&mut self, cx: &mut Context<Self>) {
        if self.editor.query_error.is_some() {
            self.set_query_error(None, cx);
        }
    }

    /// The marker as an editor diagnostic, positioned in `buffer`.
    pub(super) fn query_error_diagnostic(&self, buffer: &str) -> Option<InputDiagnostic> {
        let marker = self.editor.query_error.as_ref()?;
        let (start_line, start_column) =
            dbflux_core::byte_offset_to_line_column(buffer, marker.range.start)?;
        let (end_line, end_column) =
            dbflux_core::byte_offset_to_line_column(buffer, marker.range.end)?;

        let start = InputPosition::new(start_line as u32, start_column as u32);
        let mut end = InputPosition::new(end_line as u32, end_column as u32);
        if end == start {
            end.character = start.character.saturating_add(1);
        }

        Some(
            InputDiagnostic::new(start..end, marker.message.clone())
                .with_severity(InputDiagnosticSeverity::Error),
        )
    }

    /// Marks the token a failed run's error names, when it names one and the
    /// sent text is still in the buffer. Returns whether a marker was set.
    pub(super) fn mark_failed_query(
        &mut self,
        sent: &str,
        sent_buffer_start: Option<usize>,
        error: &DbError,
        cx: &mut Context<Self>,
    ) -> bool {
        let marker = error
            .formatted()
            .and_then(|formatted| Some((formatted.position?, &formatted.message)))
            .and_then(|(position, message)| {
                let buffer = self.editor.input_state.read(cx).value();
                marker_for(&buffer, sent, sent_buffer_start, position, message)
            });
        let marked = marker.is_some();
        self.set_query_error(marker, cx);

        marked
    }

    /// Whether a failed run left a marker the cursor can jump to.
    pub(super) fn has_query_error(&self) -> bool {
        self.editor.query_error.is_some()
    }

    /// Moves the cursor onto the marked token and focuses the editor. Returns
    /// `false`, moving nothing, when no marker exists.
    pub(super) fn go_to_query_error(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(marker) = self.editor.query_error.as_ref() else {
            return false;
        };
        let Some((line, column)) = dbflux_core::byte_offset_to_line_column(
            self.editor.input_state.read(cx).value().as_ref(),
            marker.range.start,
        ) else {
            return false;
        };

        self.editor.input_state.update(cx, |state, cx| {
            state.set_cursor_position(InputPosition::new(line as u32, column as u32), window, cx);
        });
        self.focus(window, cx);
        cx.notify();

        true
    }

    /// Runs a "Go to error" request a toast action queued, which has no
    /// window to move the cursor in.
    pub(super) fn process_pending_go_to_query_error(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if std::mem::take(&mut self.pending.go_to_query_error) {
            self.go_to_query_error(window, cx);
        }
    }

    /// The error toast's "Go to error" action. Toast callbacks only get the
    /// app, so the click queues the request and the next render runs it.
    pub(super) fn go_to_error_action(
        &self,
        cx: &Context<Self>,
    ) -> dbflux_ui_base::toast::ToastAction {
        let document = cx.entity().downgrade();

        dbflux_ui_base::toast::ToastAction::new(
            "go-to-query-error",
            dbflux_i18n::t!("document.code.execution.go_to_error"),
        )
        .on_click(move |cx: &mut App| {
            if let Some(document) = document.upgrade() {
                document.update(cx, |document, cx| {
                    document.pending.go_to_query_error = true;
                    cx.notify();
                });
            }
        })
    }
}

#[cfg(test)]
mod tests;
