use super::*;
use std::ops::Range;

/// Result of a line-comment toggle: the rewritten block of lines, the byte range
/// of the buffer it replaces, and the byte range the editor should leave
/// selected afterwards.
pub(super) struct CommentToggle {
    /// Byte range of the original text that `region` replaces.
    pub range: Range<usize>,
    /// The rewritten block, built from the lines in `range`.
    pub region: String,
    pub selection: Range<usize>,
}

/// One line of the toggled region, with everything the caret remap needs.
struct LineEdit {
    /// Absolute byte offset of the line start in the original text.
    abs_start: usize,
    /// Byte length of the original line (without its `\n`).
    len: usize,
    /// Byte offset of the first non-whitespace character, relative to the line.
    first_non_ws: usize,
}

/// Toggle line comments on every non-blank line the selection touches.
///
/// `selection` is a byte range into `text`, matching
/// `InputBaseState::selected_range()`. An empty selection toggles the line the
/// caret sits on; a non-empty one expands to whole lines. Blank lines are left
/// untouched and do not affect the comment/uncomment decision.
///
/// When every non-blank line already carries `prefix`, the prefix (and one
/// following space, if present) is removed; otherwise `prefix` plus a space is
/// inserted after each line's indentation.
///
/// Returns `None` when there is nothing to change.
pub(super) fn toggle_line_comments(
    text: &str,
    selection: Range<usize>,
    prefix: &str,
) -> Option<CommentToggle> {
    if prefix.is_empty() {
        return None;
    }

    let (start, end) = normalize_selection(selection, text.len());

    let line_start = text[..start].rfind('\n').map_or(0, |index| index + 1);
    let last_line_anchor = if end > start && text.as_bytes().get(end - 1) == Some(&b'\n') {
        end - 1
    } else {
        end
    };
    let line_end = text[last_line_anchor..]
        .find('\n')
        .map_or(text.len(), |index| last_line_anchor + index);

    let region = &text[line_start..line_end];
    let lines = line_edits(region, line_start);

    let commentable: Vec<&LineEdit> = lines
        .iter()
        .filter(|line| line.first_non_ws < line.len)
        .collect();
    if commentable.is_empty() {
        return None;
    }

    let uncomment = commentable.iter().all(|line| {
        let relative_start = line.abs_start - line_start;
        region[relative_start + line.first_non_ws..relative_start + line.len].starts_with(prefix)
    });

    let mut new_region =
        String::with_capacity(region.len() + commentable.len() * (prefix.len() + 1));
    let mut deltas: Vec<usize> = Vec::with_capacity(lines.len());

    for (index, line) in lines.iter().enumerate() {
        if index > 0 {
            new_region.push('\n');
        }

        let relative_start = line.abs_start - line_start;
        let line_text = &region[relative_start..relative_start + line.len];

        if line.first_non_ws >= line.len {
            new_region.push_str(line_text);
            deltas.push(0);
            continue;
        }

        let tail = &line_text[line.first_non_ws..];
        if uncomment {
            let removed = if tail[prefix.len()..].starts_with(' ') {
                prefix.len() + 1
            } else {
                prefix.len()
            };
            new_region.push_str(&line_text[..line.first_non_ws]);
            new_region.push_str(&line_text[line.first_non_ws + removed..]);
            deltas.push(removed);
        } else {
            new_region.push_str(&line_text[..line.first_non_ws]);
            new_region.push_str(prefix);
            new_region.push(' ');
            new_region.push_str(tail);
            deltas.push(prefix.len() + 1);
        }
    }

    let next_line = text[line_end..]
        .strip_prefix('\n')
        .map(|rest| rest.split('\n').next().unwrap_or_default());

    let selection = if end > start {
        line_start..line_start + new_region.len()
    } else {
        let caret = remap_caret(start, &lines, &deltas, uncomment);
        let offset = content_offset(start, &lines);
        let caret = caret_on_next_line(&new_region, next_line, line_start, caret, offset);
        caret..caret
    };

    Some(CommentToggle {
        range: line_start..line_end,
        region: new_region,
        selection,
    })
}

/// Byte offset of `caret` from the first non-whitespace character of its line,
/// which is the column the caret keeps when the toggle walks it down.
fn content_offset(caret: usize, lines: &[LineEdit]) -> usize {
    let Some(line) = lines
        .iter()
        .find(|line| caret >= line.abs_start && caret <= line.abs_start + line.len)
    else {
        return 0;
    };

    caret.saturating_sub(line.abs_start + line.first_non_ws)
}

/// Move the caret one line down, keeping its column within the line's text,
/// which is what toggling a comment on a bare caret does. `next_line` is the
/// line that follows the rewritten block in the original text; it is `None`
/// when the block ends the buffer, where the caret has nowhere to go.
fn caret_on_next_line(
    new_region: &str,
    next_line: Option<&str>,
    line_start: usize,
    caret: usize,
    content_offset: usize,
) -> usize {
    let Some(next_line) = next_line else {
        return caret;
    };

    let indentation = next_line.len() - next_line.trim_start().len();
    let column = char_boundary(next_line, indentation + content_offset);

    line_start + new_region.len() + 1 + column
}

/// Clamp a byte column to the line, never splitting a character.
fn char_boundary(line: &str, column: usize) -> usize {
    let mut column = column.min(line.len());
    while !line.is_char_boundary(column) {
        column -= 1;
    }
    column
}

/// Convert a byte range into the UTF-16 offsets the editor's text APIs use.
fn utf16_range(text: &str, range: &Range<usize>) -> Range<usize> {
    let start = text[..range.start].chars().map(char::len_utf16).sum();
    let end = start
        + text[range.start..range.end]
            .chars()
            .map(char::len_utf16)
            .sum::<usize>();
    start..end
}

fn normalize_selection(selection: Range<usize>, len: usize) -> (usize, usize) {
    let start = selection.start.min(len);
    let end = selection.end.min(len);
    if start <= end {
        (start, end)
    } else {
        (end, start)
    }
}

/// Split the toggled region into lines, recording each line's start, length and
/// indentation width.
fn line_edits(region: &str, region_start: usize) -> Vec<LineEdit> {
    let mut edits = Vec::new();
    let mut line_start = 0;

    for line in region.split('\n') {
        let first_non_ws = line.len() - line.trim_start().len();
        edits.push(LineEdit {
            abs_start: region_start + line_start,
            len: line.len(),
            first_non_ws,
        });
        line_start += line.len() + 1;
    }

    edits
}

/// Shift the caret by the edit that lands at or before it on its own line.
fn remap_caret(caret: usize, lines: &[LineEdit], deltas: &[usize], uncomment: bool) -> usize {
    let Some(index) = lines
        .iter()
        .position(|line| caret >= line.abs_start && caret <= line.abs_start + line.len)
    else {
        return caret;
    };

    let line = &lines[index];
    let local = caret - line.abs_start;
    let delta = deltas[index];
    if line.first_non_ws >= line.len || local < line.first_non_ws || delta == 0 {
        return caret;
    }

    if uncomment {
        if local >= line.first_non_ws + delta {
            caret - delta
        } else {
            line.abs_start + line.first_non_ws
        }
    } else {
        caret + delta
    }
}

impl CodeDocument {
    /// Comment or uncomment the lines touched by the editor selection.
    ///
    /// Returns `false` when the document is read-only or the selection covers
    /// nothing that can carry a comment.
    ///
    /// The active selection is the one gpui-base exposes; additional cursors are
    /// collapsed by `set_selected_range`, so a multi-cursor toggle acts on the
    /// active one only.
    ///
    /// Only the block of touched lines is replaced, so the editor's syntax
    /// highlighter stays on its incremental path. `replace_all` would drop and
    /// rebuild it instead, which can leave the buffer painted without any
    /// capture. A ranged edit still emits `InputEvent::Change`, so the
    /// dirty/autosave/diagnostics bookkeeping stays with the Change handler in
    /// `code/mod.rs`.
    pub(super) fn toggle_comment(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        if self.read_only {
            return false;
        }

        let (selection, current) = {
            let state = self.editor.input_state.read(cx);
            (state.selected_range(), state.value().to_string())
        };

        let Some(toggle) = toggle_line_comments(&current, selection, self.comment_prefix()) else {
            return false;
        };

        let next_selection = toggle.selection;
        let region = utf16_range(&current, &toggle.range);
        // A toggle is not a keystroke: the Change handler's deletion path moves
        // the cursor and reopens the completion menu, which costs the selection
        // the toggle has just restored. The flag stays set until that handler
        // consumes it — GPUI delivers `emit` after the outermost update, so
        // clearing it here would be too early. Closing an open menu must not be
        // skipped with it, so that half of the cursor move happens here, before
        // the edit; only the reopening stays suppressed.
        self.editor.toggling_comment = true;
        self.editor.input_state.update(cx, |state, cx| {
            let scroll_offset = state.scroll_offset();
            let cursor = state.cursor_position();
            state.set_cursor_position(cursor, window, cx);
            state.replace_text_in_range(Some(region), &toggle.region, window, cx);
            state.set_selected_range(next_selection, cx);
            state.set_scroll_offset(scroll_offset, cx);
        });

        true
    }
}

#[cfg(test)]
mod tests {
    use super::toggle_line_comments;

    fn toggle_range(
        text: &str,
        selection: std::ops::Range<usize>,
        prefix: &str,
    ) -> (String, std::ops::Range<usize>, std::ops::Range<usize>) {
        let result = toggle_line_comments(text, selection, prefix).expect("toggle must apply");
        let mut toggled = text.to_string();
        toggled.replace_range(result.range.clone(), &result.region);
        (toggled, result.selection, result.range)
    }

    fn toggle(
        text: &str,
        selection: std::ops::Range<usize>,
        prefix: &str,
    ) -> (String, std::ops::Range<usize>) {
        let (toggled, selection, _) = toggle_range(text, selection, prefix);
        (toggled, selection)
    }

    #[test]
    fn only_the_touched_block_is_replaced() {
        let text = "SELECT 1;\nSELECT 2;\nSELECT 3;\nSELECT 4;";
        let caret = "SELECT 1;\nSELECT 2;\n".len();

        let (toggled, selection, range) = toggle_range(text, caret..caret, "--");

        assert_eq!(range, caret..caret + "SELECT 3;".len());
        assert_eq!(toggled, "SELECT 1;\nSELECT 2;\n-- SELECT 3;\nSELECT 4;");
        let next_line = toggled.find("SELECT 4;").expect("the next line must exist");
        assert_eq!(
            selection,
            next_line..next_line,
            "the caret walks down to the next line"
        );
    }

    #[test]
    fn caret_moves_to_the_next_line_keeping_its_column() {
        let text = "SELECT 1;\nSELECT 2;\nSELECT 3;";

        let (commented, selection) = toggle(text, 4..4, "--");

        assert_eq!(commented, "-- SELECT 1;\nSELECT 2;\nSELECT 3;");
        let next_line = commented.find("SELECT 2;").expect("the next line") + 4;
        assert_eq!(selection, next_line..next_line);

        let (restored, selection) = toggle(&commented, selection, "--");

        assert_eq!(restored, "-- SELECT 1;\n-- SELECT 2;\nSELECT 3;");
        let next_line = restored.find("SELECT 3;").expect("the next line") + 4;
        assert_eq!(selection, next_line..next_line);
    }

    #[test]
    fn caret_on_the_last_line_stays_put() {
        let text = "SELECT 1;\nSELECT 2;";
        let caret = "SELECT 1;\n".len() + 3;

        let (commented, selection) = toggle(text, caret..caret, "--");

        assert_eq!(commented, "SELECT 1;\n-- SELECT 2;");
        assert_eq!(selection, caret + 3..caret + 3);
    }

    #[test]
    fn caret_comments_and_uncomments_a_single_line() {
        let (commented, _) = toggle("SELECT 1;", 0..0, "--");
        assert_eq!(commented, "-- SELECT 1;");

        let (restored, _) = toggle(&commented, 0..0, "--");
        assert_eq!(restored, "SELECT 1;");
    }

    #[test]
    fn selection_comments_every_touched_line() {
        let text = "SELECT 1;\nSELECT 2;\nSELECT 3;";
        let (commented, selection) = toggle(text, 0..19, "--");

        assert_eq!(commented, "-- SELECT 1;\n-- SELECT 2;\nSELECT 3;");
        assert_eq!(selection, 0..25);

        let (restored, _) = toggle(&commented, selection, "--");
        assert_eq!(restored, text);
    }

    #[test]
    fn partially_commented_selection_comments_all_lines() {
        let text = "-- SELECT 1;\nSELECT 2;";
        let (commented, _) = toggle(text, 0..text.len(), "--");

        assert_eq!(commented, "-- -- SELECT 1;\n-- SELECT 2;");
    }

    #[test]
    fn uncomment_strips_prefix_and_one_space() {
        let text = "-- SELECT 1;\n--SELECT 2;";
        let (uncommented, _) = toggle(text, 0..text.len(), "--");

        assert_eq!(uncommented, "SELECT 1;\nSELECT 2;");
    }

    #[test]
    fn indentation_and_blank_lines_are_preserved() {
        let text = "    SELECT 1;\n\n\tSELECT 2;";
        let (commented, _) = toggle(text, 0..text.len(), "#");
        assert_eq!(commented, "    # SELECT 1;\n\n\t# SELECT 2;");

        let (restored, _) = toggle(&commented, 0..commented.len(), "#");
        assert_eq!(restored, text);
    }

    #[test]
    fn crlf_line_endings_survive_the_round_trip() {
        let text = "SELECT 1;\r\nSELECT 2;";
        let (commented, _) = toggle(text, 0..text.len(), "--");
        assert_eq!(commented, "-- SELECT 1;\r\n-- SELECT 2;");

        let (restored, _) = toggle(&commented, 0..commented.len(), "--");
        assert_eq!(restored, text);
    }

    #[test]
    fn selection_ending_after_a_newline_skips_the_next_line() {
        let text = "SELECT 1;\nSELECT 2;";
        let (commented, _) = toggle(text, 0..10, "--");

        assert_eq!(commented, "-- SELECT 1;\nSELECT 2;");
    }

    #[test]
    fn caret_keeps_its_place_after_the_inserted_prefix() {
        let text = "SELECT 1;";
        let (commented, selection) = toggle(text, 5..5, "--");

        assert_eq!(commented, "-- SELECT 1;");
        assert_eq!(selection, 8..8);

        let (restored, selection) = toggle(&commented, selection, "--");
        assert_eq!(restored, text);
        assert_eq!(selection, 5..5);
    }

    #[test]
    fn selection_starting_mid_line_comments_the_whole_line() {
        let text = "SELECT 1;\nSELECT 2;";
        let (commented, selection) = toggle(text, 4..10, "--");

        assert_eq!(commented, "-- SELECT 1;\nSELECT 2;");
        assert_eq!(selection, 0.."-- SELECT 1;".len());

        let (restored, selection) = toggle(&commented, selection, "--");
        assert_eq!(restored, text);
        assert_eq!(selection, 0.."SELECT 1;".len());
    }

    #[test]
    fn uncommenting_keeps_a_caret_inside_multibyte_text() {
        let text = "-- SELECT 'привет';";
        let caret = "-- SELECT 'при".len();

        let (restored, selection) = toggle(text, caret..caret, "--");

        assert_eq!(restored, "SELECT 'привет';");
        assert_eq!(selection, caret - 3..caret - 3, "the prefix is dropped");
    }

    #[test]
    fn caret_inside_a_removed_prefix_moves_to_the_text() {
        let text = "-- SELECT 'привет';";
        let (restored, selection) = toggle(text, 1..1, "--");

        assert_eq!(restored, "SELECT 'привет';");
        assert_eq!(selection, 0..0);
    }

    #[test]
    fn nothing_to_do_returns_none() {
        assert!(toggle_line_comments("", 0..0, "--").is_none());
        assert!(toggle_line_comments("   \n\t", 0..0, "--").is_none());
        assert!(toggle_line_comments("SELECT 1;", 0..0, "").is_none());
    }

    #[test]
    fn lua_and_python_prefixes_are_language_specific() {
        let (lua, _) = toggle("print(1)", 0..0, "--");
        assert_eq!(lua, "-- print(1)");

        let (python, _) = toggle("print(1)", 0..0, "#");
        assert_eq!(python, "# print(1)");
    }
}
