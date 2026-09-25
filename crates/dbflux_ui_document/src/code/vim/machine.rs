//! Window-free core of Vim mode: which command a key means in each mode, and
//! where each command puts the cursor.
//!
//! Positions are UTF-8 byte offsets into the editor's rope, the unit
//! gpui-component's `InputState` uses. The cursor moves one Unicode scalar value
//! at a time, the same step the editor's own arrow keys take. In Normal mode the
//! cursor sits on a character, so it never rests on a line terminator (`\n`, or
//! the `\r` of a CRLF pair) unless the line is empty.

use dbflux_components::controls::{Rope, RopeExt};
use std::ops::Range;

/// Editing mode of a code editor while Vim mode is enabled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum VimMode {
    #[default]
    Normal,
    Insert,
    Visual,
    VisualLine,
    VisualBlock,
}

/// What a key does in the current mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VimCommand {
    MoveLeft,
    MoveRight,
    MoveUp,
    MoveDown,
    PendingG,
    PendingMark(char),
    FirstLine,
    LastLine,
    EnterInsert,
    EnterVisual,
    EnterVisualLine,
    EnterVisualBlock,
    LeaveVisual,
    Append,
    AppendLine,
    InsertLine,
    WordEnd(bool),
    WordForward(bool),
    WordBackward(bool),
    Digit(u8),
    LeaveInsert,
    DeleteChar,
    VisualDelete,
    VisualYank,
    Operator(char),
    Undo,
    OpenSearch,
    RepeatSearch(bool),
    /// Consumed without effect, so the key neither edits nor reaches other handlers.
    Swallow,
}

/// The parts of a keystroke the machine needs.
#[derive(Clone, Copy, Debug)]
pub(crate) struct VimKey<'a> {
    /// GPUI key name, such as `"h"`, `"enter"` or `"escape"`.
    pub key: &'a str,
    pub shift: bool,
    /// Control, Alt, Cmd/Super or Fn is held. Such keys are application shortcuts
    /// and always pass through.
    pub command_modifier: bool,
}

/// Maps a key to its command in `mode`. `None` means the key is not Vim's to
/// handle and continues to the editor and the application keymap.
pub(crate) fn command_for(mode: VimMode, key: VimKey<'_>) -> Option<VimCommand> {
    if key.command_modifier {
        return None;
    }

    match mode {
        VimMode::Insert => (key.key == "escape" && !key.shift).then_some(VimCommand::LeaveInsert),
        VimMode::Normal | VimMode::Visual | VimMode::VisualLine | VimMode::VisualBlock => {
            let visual = matches!(
                mode,
                VimMode::Visual | VimMode::VisualLine | VimMode::VisualBlock
            );
            if key.key == "escape" && visual {
                return Some(VimCommand::LeaveVisual);
            }
            if key.key == "v" && !key.shift {
                return Some(if mode == VimMode::Visual {
                    VimCommand::LeaveVisual
                } else {
                    VimCommand::EnterVisual
                });
            }
            if key.key == "v" && key.shift {
                return Some(if mode == VimMode::VisualLine {
                    VimCommand::LeaveVisual
                } else {
                    VimCommand::EnterVisualLine
                });
            }
            // Tab and Shift+Tab would otherwise indent or move focus out of the editor.
            if key.key == "tab" {
                return Some(VimCommand::Swallow);
            }

            if key.shift {
                return match key.key {
                    "a" if !visual => Some(VimCommand::AppendLine),
                    "i" if !visual => Some(VimCommand::InsertLine),
                    "e" => Some(VimCommand::WordEnd(true)),
                    "w" => Some(VimCommand::WordForward(true)),
                    "b" => Some(VimCommand::WordBackward(true)),
                    "g" => Some(VimCommand::LastLine),
                    "n" if !visual => Some(VimCommand::RepeatSearch(true)),
                    _ => None,
                };
            }

            match key.key {
                "g" => Some(VimCommand::PendingG),
                "m" | "'" | "`" if !visual => {
                    Some(VimCommand::PendingMark(key.key.chars().next()?))
                }
                "/" if !visual => Some(VimCommand::OpenSearch),
                "n" if !visual => Some(VimCommand::RepeatSearch(false)),
                "h" => Some(VimCommand::MoveLeft),
                "l" => Some(VimCommand::MoveRight),
                "j" | "enter" => Some(VimCommand::MoveDown),
                "k" => Some(VimCommand::MoveUp),
                "i" if !visual => Some(VimCommand::EnterInsert),
                "a" if !visual => Some(VimCommand::Append),
                "e" => Some(VimCommand::WordEnd(false)),
                "w" => Some(VimCommand::WordForward(false)),
                "b" => Some(VimCommand::WordBackward(false)),
                "0" => Some(VimCommand::Digit(0)),
                digit if digit.len() == 1 && digit.as_bytes()[0].is_ascii_digit() => {
                    Some(VimCommand::Digit(digit.as_bytes()[0] - b'0'))
                }
                "x" | "d" if visual => Some(VimCommand::VisualDelete),
                "y" if visual => Some(VimCommand::VisualYank),
                "x" if !visual => Some(VimCommand::DeleteChar),
                "d" if !visual => Some(VimCommand::Operator('d')),
                "y" if !visual => Some(VimCommand::Operator('y')),
                "u" if !visual => Some(VimCommand::Undo),
                _ => None,
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SearchDirection {
    Forward,
    Backward,
}

/// Selects a literal match by its UTF-8 byte start, excluding the cursor's start.
/// Ranges must be sorted and nonoverlapping.
pub(crate) fn cursor_relative_match(
    matches: &[Range<usize>],
    cursor: usize,
    direction: SearchDirection,
) -> Option<Range<usize>> {
    if matches.is_empty() {
        return None;
    }

    let index = match direction {
        SearchDirection::Forward => {
            matches.partition_point(|range| range.start <= cursor) % matches.len()
        }
        SearchDirection::Backward => matches
            .partition_point(|range| range.start < cursor)
            .checked_sub(1)
            .unwrap_or(matches.len() - 1),
    };
    Some(matches[index].clone())
}

/// The mode a command leaves the editor in.
pub(crate) fn mode_after(mode: VimMode, command: VimCommand) -> VimMode {
    match command {
        VimCommand::EnterInsert => VimMode::Insert,
        VimCommand::LeaveInsert | VimCommand::LeaveVisual => VimMode::Normal,
        VimCommand::EnterVisual => VimMode::Visual,
        VimCommand::EnterVisualLine => VimMode::VisualLine,
        VimCommand::EnterVisualBlock => VimMode::VisualBlock,
        _ => mode,
    }
}

/// One line of the buffer, without its line terminator.
struct Line {
    row: usize,
    start: usize,
    content: String,
}

impl Line {
    fn at_row(text: &Rope, row: usize) -> Self {
        let line = text.slice_line(row).to_string();
        let content = line.strip_suffix('\r').unwrap_or(&line).to_string();

        Self {
            row,
            start: text.line_start_offset(row),
            content,
        }
    }

    fn containing(text: &Rope, offset: usize) -> Self {
        Self::at_row(text, text.offset_to_point(offset).row)
    }

    /// Byte column of `offset` within the line, moved back onto a character boundary.
    fn column_of(&self, offset: usize) -> usize {
        let mut column = offset.saturating_sub(self.start).min(self.content.len());
        while !self.content.is_char_boundary(column) {
            column -= 1;
        }
        column
    }

    /// Byte column of the last character: the right-most Normal-mode position.
    fn last_column(&self) -> usize {
        self.content
            .char_indices()
            .next_back()
            .map(|(column, _)| column)
            .unwrap_or(0)
    }

    fn char_count_before(&self, column: usize) -> usize {
        self.content[..column].chars().count()
    }

    /// Byte column of the character at `index`, clamped to the last character.
    fn column_of_char(&self, index: usize) -> usize {
        self.content
            .char_indices()
            .nth(index)
            .map(|(column, _)| column)
            .unwrap_or(self.last_column())
            .min(self.last_column())
    }
}

/// Moves `offset` onto a character of its line: a cursor past the last
/// character comes back onto it.
pub(crate) fn clamp_to_character(text: &Rope, offset: usize) -> usize {
    let line = Line::containing(text, offset);
    line.start + line.column_of(offset).min(line.last_column())
}

/// One character left, stopping at the line start.
pub(crate) fn step_left(text: &Rope, offset: usize) -> usize {
    let line = Line::containing(text, offset);
    let column = line.column_of(offset);

    let target = line.content[..column]
        .chars()
        .next_back()
        .map(|character| column - character.len_utf8())
        .unwrap_or(column);

    line.start + target
}

/// One character right, stopping on the last character of the line.
pub(crate) fn step_right(text: &Rope, offset: usize) -> usize {
    let line = Line::containing(text, offset);
    let column = line.column_of(offset);

    let target = line.content[column..]
        .chars()
        .next()
        .map(|character| column + character.len_utf8())
        .unwrap_or(column);

    line.start + target.min(line.last_column())
}

/// Absolute logical line motion, using Vim's first nonblank column.
pub(crate) fn absolute_line(text: &Rope, row: usize) -> usize {
    let line = Line::at_row(text, row.min(text.lines_len().saturating_sub(1)));
    line.start
        + line
            .content
            .char_indices()
            .find(|(_, character)| !character.is_whitespace())
            .map_or(0, |(column, _)| column)
}

pub(crate) fn line_start(text: &Rope, offset: usize) -> usize {
    Line::containing(text, offset).start
}

pub(crate) fn line_first_nonblank(text: &Rope, offset: usize) -> usize {
    let line = Line::containing(text, offset);
    line.start
        + line
            .content
            .char_indices()
            .find(|(_, character)| !character.is_whitespace())
            .map_or(0, |(column, _)| column)
}

pub(crate) fn line_end(text: &Rope, offset: usize) -> usize {
    let line = Line::containing(text, offset);
    line.start + line.content.len()
}

pub(crate) fn append_after(text: &Rope, offset: usize) -> usize {
    let line = Line::containing(text, offset);
    let column = line.column_of(offset);
    line.start
        + column
        + line.content[column..]
            .chars()
            .next()
            .map_or(0, char::len_utf8)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WordClass {
    Space,
    Keyword,
    Punctuation,
}

fn word_class(character: char, big: bool) -> WordClass {
    if character.is_whitespace() {
        WordClass::Space
    } else if big || character.is_alphanumeric() || character == '_' {
        WordClass::Keyword
    } else {
        WordClass::Punctuation
    }
}

#[derive(Clone, Copy)]
pub(crate) enum WordMotion {
    End,
    Forward,
    Backward,
}

pub(crate) fn word_offsets(text: &Rope) -> Vec<(usize, char)> {
    text.to_string().char_indices().collect()
}

pub(crate) fn step_word(
    text: &Rope,
    chars: &[(usize, char)],
    offset: usize,
    motion: WordMotion,
    big: bool,
) -> usize {
    if chars.is_empty() {
        return 0;
    }
    let mut index = chars.partition_point(|(start, _)| *start < offset);
    if index == chars.len() || chars[index].0 != offset {
        index = index.saturating_sub(1);
    }
    let class = |at: usize| word_class(chars[at].1, big);
    match motion {
        WordMotion::Forward => {
            let current = class(index);
            if current != WordClass::Space {
                while index < chars.len() && class(index) == current {
                    index += 1;
                }
            }
            while index < chars.len() && class(index) == WordClass::Space {
                index += 1;
            }
            chars
                .get(index)
                .map_or_else(|| clamp_to_character(text, text.len()), |item| item.0)
        }
        WordMotion::Backward => {
            index = index.saturating_sub(1);
            while index > 0 && class(index) == WordClass::Space {
                index -= 1;
            }
            let current = class(index);
            while index > 0 && class(index - 1) == current {
                index -= 1;
            }
            chars[index].0
        }
        WordMotion::End => {
            if class(index) != WordClass::Space {
                let current = class(index);
                if index + 1 < chars.len() && class(index + 1) == current {
                    index += 1;
                } else {
                    index += 1;
                    while index < chars.len() && class(index) == WordClass::Space {
                        index += 1;
                    }
                }
            } else {
                while index < chars.len() && class(index) == WordClass::Space {
                    index += 1;
                }
            }
            if index >= chars.len() {
                return clamp_to_character(text, text.len());
            }
            let current = class(index);
            while index + 1 < chars.len() && class(index + 1) == current {
                index += 1;
            }
            clamp_to_character(text, chars[index].0)
        }
    }
}

/// Characterwise operator range. Word starts are exclusive; word ends include
/// the entire character under the destination cursor.
pub(crate) fn word_operator_range(
    text: &Rope,
    offset: usize,
    motion: WordMotion,
    big: bool,
    count: usize,
) -> Option<Range<usize>> {
    let chars = word_offsets(text);
    let mut target = offset;
    for _ in 0..count.min(chars.len().saturating_add(1)) {
        let next = step_word(text, &chars, target, motion, big);
        if next == target {
            break;
        }
        target = next;
    }
    let range = match motion {
        WordMotion::End => {
            let end = counted_character_range(text, target, 1)
                .map(|range| range.end)
                .or_else(|| {
                    let content = text.to_string();
                    let before_eof = content.trim_end_matches(['\r', '\n']);
                    (target == content.len() && before_eof.len() > offset)
                        .then_some(before_eof.len())
                })?;
            offset..end
        }
        WordMotion::Forward
            if target == clamp_to_character(text, text.len()) && !chars.is_empty() =>
        {
            let content = text.to_string();
            offset..content.trim_end_matches(['\r', '\n']).len()
        }
        WordMotion::Forward => offset..target,
        WordMotion::Backward => target..offset,
    };
    (!range.is_empty()).then_some(range)
}

/// Horizontal operator motions exclude the destination for `h` and include it
/// for `l`, without crossing a logical line or including its separator.
pub(crate) fn horizontal_operator_range(
    text: &Rope,
    offset: usize,
    right: bool,
    count: usize,
) -> Option<Range<usize>> {
    let line = Line::containing(text, offset);
    let column = line.column_of(offset).min(line.last_column());
    if line.content.is_empty() {
        return None;
    }
    let index = line.char_count_before(column);
    let columns: Vec<usize> = line.content.char_indices().map(|(at, _)| at).collect();
    let (start, end) = if right {
        let last = index.saturating_add(count).min(columns.len() - 1);
        (
            column,
            columns[last] + line.content[columns[last]..].chars().next()?.len_utf8(),
        )
    } else {
        (columns[index.saturating_sub(count)], column)
    };
    (start < end).then_some(line.start + start..line.start + end)
}

/// Whole current and destination logical lines, clamping at either edge.
pub(crate) fn vertical_operator_range(
    text: &Rope,
    offset: usize,
    down: bool,
    count: usize,
) -> Range<usize> {
    let row = text.offset_to_point(offset).row;
    let last = text.lines_len().saturating_sub(1);
    let target = if down {
        row.saturating_add(count).min(last)
    } else {
        row.saturating_sub(count)
    };
    let first = row.min(target);
    let end = row.max(target).saturating_add(1);
    text.line_start_offset(first)..if end < text.lines_len() {
        text.line_start_offset(end)
    } else {
        text.len()
    }
}

/// Inclusive logical lines between the cursor and an absolute, clamped row.
pub(crate) fn absolute_operator_range(
    text: &Rope,
    offset: usize,
    target_row: usize,
) -> Range<usize> {
    let row = text.offset_to_point(offset).row;
    let target = target_row.min(text.lines_len().saturating_sub(1));
    let first = row.min(target);
    let end = row.max(target).saturating_add(1);
    text.line_start_offset(first)..if end < text.lines_len() {
        text.line_start_offset(end)
    } else {
        text.len()
    }
}

/// Result of a vertical move.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct VerticalStep {
    pub offset: usize,
    /// Character column the move aimed for. Passing it to the next vertical move
    /// keeps the column across shorter lines, as Vim does.
    pub goal_column: usize,
}

/// Moves `delta` lines up (negative) or down, aiming for `goal_column` (in
/// characters) or, when `None`, for the cursor's current column. Returns `None`
/// when the target line is outside the buffer.
pub(crate) fn step_vertical(
    text: &Rope,
    offset: usize,
    delta: isize,
    goal_column: Option<usize>,
) -> Option<VerticalStep> {
    let line = Line::containing(text, offset);
    let target_row = line
        .row
        .checked_add_signed(delta)
        .filter(|row| *row < text.lines_len())?;

    let goal_column = goal_column.unwrap_or_else(|| line.char_count_before(line.column_of(offset)));
    let target = Line::at_row(text, target_row);

    Some(VerticalStep {
        offset: target.start + target.column_of_char(goal_column),
        goal_column,
    })
}

/// Byte range of the character under the cursor, or `None` on an empty line.
/// Never includes a line terminator, so `x` cannot join lines.
pub(crate) fn visual_range(
    text: &Rope,
    anchor: usize,
    cursor: usize,
    linewise: bool,
) -> Range<usize> {
    if linewise {
        let start = line_start(text, anchor.min(cursor));
        let end_line = Line::containing(text, anchor.max(cursor));
        let end = if end_line.row + 1 < text.lines_len() {
            text.line_start_offset(end_line.row + 1)
        } else {
            text.len()
        };
        start..end
    } else {
        let start = anchor.min(cursor);
        let end = anchor.max(cursor);
        let width = counted_character_range(text, end, 1).map_or(0, |range| range.len());
        start..end + width
    }
}

/// Whole logical lines, including their terminators when present. The final
/// unterminated line has no invented newline in the returned range.
pub(crate) fn counted_line_range(text: &Rope, offset: usize, count: usize) -> Range<usize> {
    let row = text.offset_to_point(offset).row;
    let end_row = row.saturating_add(count).min(text.lines_len());
    text.line_start_offset(row)..if end_row < text.lines_len() {
        text.line_start_offset(end_row)
    } else {
        text.len()
    }
}

/// An empty trailing logical line yanks the separator that created it.
/// Other line selections retain their original bytes, including an unterminated EOF.
pub(crate) fn line_yank_text<'a>(content: &'a str, range: Range<usize>) -> Option<&'a str> {
    if range.is_empty() && range.start == content.len() {
        let prefix = &content[..range.start];
        if prefix.ends_with("\r\n") {
            return Some(&prefix[prefix.len() - 2..]);
        }
        if prefix.ends_with('\n') {
            return Some(&prefix[prefix.len() - 1..]);
        }
    }
    content.get(range)
}

/// Deleting the last logical line also removes the separator before it.
pub(crate) fn line_delete_range(text: &Rope, range: Range<usize>) -> Range<usize> {
    if range.end != text.len() || range.start == 0 {
        return range;
    }
    let content = text.to_string();
    let before = &content[..range.start];
    let separator_len = if before.ends_with("\r\n") { 2 } else { 1 };
    range.start.saturating_sub(separator_len)..range.end
}

#[cfg(test)]
pub(crate) fn character_range(text: &Rope, offset: usize) -> Option<Range<usize>> {
    counted_character_range(text, offset, 1)
}

/// Selects at most `count` characters from one line without copying the line
/// again for each character. A zero count selects nothing.
pub(crate) fn counted_character_range(
    text: &Rope,
    offset: usize,
    count: usize,
) -> Option<Range<usize>> {
    let line = Line::containing(text, offset);
    let column = line.column_of(offset);
    let width: usize = line.content[column..]
        .chars()
        .take(count)
        .map(char::len_utf8)
        .sum();
    (width > 0).then_some(line.start + column..line.start + column + width)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str) -> VimKey<'_> {
        VimKey {
            key: name,
            shift: false,
            command_modifier: false,
        }
    }

    fn shifted(name: &str) -> VimKey<'_> {
        VimKey {
            shift: true,
            ..key(name)
        }
    }

    fn with_command_modifier(name: &str) -> VimKey<'_> {
        VimKey {
            command_modifier: true,
            ..key(name)
        }
    }

    #[test]
    fn search_selects_strictly_by_start_and_wraps() {
        let matches = [0..2, 5..7, 10..12];
        for (cursor, forward, backward) in [
            (0, 5..7, 10..12),
            (6, 10..12, 5..7),
            (10, 0..2, 5..7),
            (12, 0..2, 10..12),
        ] {
            assert_eq!(
                cursor_relative_match(&matches, cursor, SearchDirection::Forward),
                Some(forward)
            );
            assert_eq!(
                cursor_relative_match(&matches, cursor, SearchDirection::Backward),
                Some(backward)
            );
        }
    }

    #[test]
    fn search_uses_utf8_byte_offsets_without_splitting_matches() {
        let content = "é中é中";
        let matches = [0..2, 5..7];
        assert!(content.is_char_boundary(matches[1].start));
        assert_eq!(
            cursor_relative_match(&matches, 2, SearchDirection::Forward),
            Some(5..7)
        );
        assert_eq!(
            cursor_relative_match(&matches, 6, SearchDirection::Backward),
            Some(5..7)
        );
        for direction in [SearchDirection::Forward, SearchDirection::Backward] {
            assert_eq!(cursor_relative_match(&[], 0, direction), None);
        }
    }

    #[test]
    fn absolute_lines_use_logical_rows_and_first_nonblank() {
        for content in ["", "  é\n\t中\n", "  é\r\n\t中\r\n"] {
            let text = Rope::from(content);
            assert_eq!(
                absolute_line(&text, 0),
                if content.is_empty() { 0 } else { 2 }
            );
            assert_eq!(
                absolute_line(&text, 1),
                if content.is_empty() {
                    0
                } else {
                    content.find('中').unwrap()
                }
            );
            assert_eq!(absolute_line(&text, usize::MAX), text.len());
        }
        for mode in [
            VimMode::Normal,
            VimMode::Visual,
            VimMode::VisualLine,
            VimMode::VisualBlock,
        ] {
            assert_eq!(command_for(mode, key("g")), Some(VimCommand::PendingG));
            assert_eq!(command_for(mode, shifted("g")), Some(VimCommand::LastLine));
            assert_eq!(command_for(mode, with_command_modifier("g")), None);
        }
    }

    #[test]
    fn normal_mode_binds_only_the_supported_commands() {
        let expected = [
            ("h", VimCommand::MoveLeft),
            ("l", VimCommand::MoveRight),
            ("j", VimCommand::MoveDown),
            ("enter", VimCommand::MoveDown),
            ("k", VimCommand::MoveUp),
            ("i", VimCommand::EnterInsert),
            ("x", VimCommand::DeleteChar),
            ("u", VimCommand::Undo),
            ("/", VimCommand::OpenSearch),
            ("n", VimCommand::RepeatSearch(false)),
            ("tab", VimCommand::Swallow),
        ];

        for (name, command) in expected {
            assert_eq!(
                command_for(VimMode::Normal, key(name)),
                Some(command),
                "{name}"
            );
        }

        for name in ["o", "p", "escape", "backspace", "space"] {
            assert_eq!(command_for(VimMode::Normal, key(name)), None, "{name}");
        }
    }

    #[test]
    fn shifted_keys_are_not_their_lowercase_commands() {
        for name in ["h", "j", "k", "l", "x", "u", "enter"] {
            assert_eq!(command_for(VimMode::Normal, shifted(name)), None, "{name}");
        }

        assert_eq!(
            command_for(VimMode::Normal, shifted("tab")),
            Some(VimCommand::Swallow)
        );
    }

    #[test]
    fn keys_with_command_modifiers_always_pass_through() {
        for mode in [VimMode::Normal, VimMode::Insert] {
            for name in ["h", "j", "k", "l", "x", "u", "enter", "tab", "escape", "s"] {
                assert_eq!(
                    command_for(mode, with_command_modifier(name)),
                    None,
                    "{mode:?} {name}"
                );
            }
        }
    }

    #[test]
    fn insert_mode_only_claims_escape() {
        assert_eq!(
            command_for(VimMode::Insert, key("escape")),
            Some(VimCommand::LeaveInsert)
        );

        for name in ["h", "j", "x", "u", "i", "enter", "tab"] {
            assert_eq!(command_for(VimMode::Insert, key(name)), None, "{name}");
        }

        assert_eq!(command_for(VimMode::Insert, shifted("escape")), None);
    }

    #[test]
    fn only_mode_commands_change_the_mode() {
        assert_eq!(
            mode_after(VimMode::Normal, VimCommand::EnterInsert),
            VimMode::Insert
        );
        assert_eq!(
            mode_after(VimMode::Insert, VimCommand::LeaveInsert),
            VimMode::Normal
        );

        for command in [
            VimCommand::MoveLeft,
            VimCommand::MoveDown,
            VimCommand::DeleteChar,
            VimCommand::Undo,
            VimCommand::Swallow,
        ] {
            assert_eq!(mode_after(VimMode::Normal, command), VimMode::Normal);
        }
    }

    #[test]
    fn words_on_empty_and_crlf_lines_remain_on_character_boundaries() {
        let empty = Rope::from("");
        let chars = word_offsets(&empty);
        assert_eq!(step_word(&empty, &chars, 0, WordMotion::Forward, false), 0);
        let text = Rope::from("é!\r\n\r\n中 x");
        let chars = word_offsets(&text);
        assert_eq!(step_word(&text, &chars, 0, WordMotion::Forward, false), 2);
        assert_eq!(step_word(&text, &chars, 2, WordMotion::Forward, false), 7);
        assert_eq!(step_word(&text, &chars, 7, WordMotion::Backward, false), 2);
        assert_eq!(step_word(&text, &chars, 7, WordMotion::End, false), 11);
    }

    #[test]
    fn forward_word_operator_covers_terminal_scalar_without_terminal_newline() {
        for big in [false, true] {
            for (content, expected) in [
                ("abc", Some(0..3)),
                ("ab中", Some(0..5)),
                ("abc   ", Some(0..6)),
                ("abc\r\n", Some(0..3)),
                ("", None),
            ] {
                let text = Rope::from(content);
                assert_eq!(
                    word_operator_range(&text, 0, WordMotion::Forward, big, 1),
                    expected
                );
            }
            let text = Rope::from("abc");
            assert_eq!(
                word_operator_range(&text, 0, WordMotion::Forward, big, 20),
                Some(0..3)
            );
            assert_eq!(
                word_operator_range(&text, 2, WordMotion::Forward, big, 1),
                Some(2..3)
            );
        }
        let text = Rope::from("one two");
        assert_eq!(
            word_operator_range(&text, 4, WordMotion::Backward, false, 1),
            Some(0..4)
        );
        assert_eq!(
            word_operator_range(&text, 4, WordMotion::Backward, true, 1),
            Some(0..4)
        );
    }

    #[test]
    fn terminal_word_end_operator_excludes_line_terminators() {
        for content in ["abc\n", "abc\r\n", "ab中\n", "ab中\r\n"] {
            let text = Rope::from(content);
            let end = content.trim_end_matches(['\r', '\n']).len();
            let last = content[..end].char_indices().last().unwrap().0;
            for big in [false, true] {
                for count in [1, 2, 20] {
                    assert_eq!(
                        word_operator_range(&text, last, WordMotion::End, big, count),
                        Some(last..end)
                    );
                    assert_eq!(
                        word_operator_range(&text, 0, WordMotion::End, big, count),
                        Some(0..end)
                    );
                }
            }
        }
    }

    #[test]
    fn operator_motions_respect_unicode_and_line_boundaries() {
        let text = Rope::from("é中x\r\nlast");
        assert_eq!(horizontal_operator_range(&text, 0, true, 2), Some(0..6));
        assert_eq!(horizontal_operator_range(&text, 5, false, 2), Some(0..5));
        assert_eq!(horizontal_operator_range(&text, 0, false, 1), None);
        assert_eq!(horizontal_operator_range(&text, 5, true, 50), Some(5..6));
        assert_eq!(vertical_operator_range(&text, 0, true, 1), 0..12);
        assert_eq!(vertical_operator_range(&text, 9, false, 1), 0..12);
        assert_eq!(line_delete_range(&text, 0..12), 0..12);
        let empty = Rope::from("");
        assert_eq!(horizontal_operator_range(&empty, 0, true, 1), None);
        assert_eq!(vertical_operator_range(&empty, 0, true, 1), 0..0);
    }

    #[test]
    fn absolute_operator_ranges_include_both_logical_lines() {
        for separator in ["\n", "\r\n"] {
            let content = format!("é{separator}中{separator}last");
            let text = Rope::from(content.as_str());
            let second = text.line_start_offset(1);
            assert_eq!(
                absolute_operator_range(&text, second, 0),
                0..text.line_start_offset(2)
            );
            assert_eq!(absolute_operator_range(&text, 0, usize::MAX), 0..text.len());
            assert_eq!(
                absolute_operator_range(&text, second, 1),
                second..text.line_start_offset(2)
            );
        }
        let empty = Rope::from("");
        assert_eq!(absolute_operator_range(&empty, 0, usize::MAX), 0..0);
    }

    #[test]
    fn counted_lines_preserve_terminators_and_eof() {
        let text = Rope::from("é\r\n\r\n中");
        assert_eq!(counted_line_range(&text, 0, 2), 0..6);
        assert_eq!(counted_line_range(&text, 4, 20), 4..9);
        assert_eq!(counted_line_range(&text, 6, 1), 6..9);
        let terminated = Rope::from("a\n");
        assert_eq!(counted_line_range(&terminated, 2, 1), 2..2);
        assert_eq!(line_delete_range(&terminated, 2..2), 1..2);
        assert_eq!(line_delete_range(&text, 6..9), 4..9);
        assert_eq!(line_delete_range(&Rope::from("a\nb"), 2..3), 1..3);
        assert_eq!(line_delete_range(&Rope::from("a"), 0..1), 0..1);
    }

    #[test]
    fn horizontal_steps_stay_on_the_line_and_on_characters() {
        let text = Rope::from("ab\ncd");

        assert_eq!(step_left(&text, 0), 0);
        assert_eq!(step_right(&text, 0), 1);
        assert_eq!(step_right(&text, 1), 1);
        assert_eq!(
            step_left(&text, 3),
            3,
            "h does not wrap to the previous line"
        );
        assert_eq!(
            step_right(&text, 4),
            4,
            "l stops on the last line's last character"
        );
    }

    #[test]
    fn horizontal_steps_move_one_unicode_character() {
        // é is 2 bytes, 中 is 3, 🎉 is 4.
        let text = Rope::from("é中🎉x");

        assert_eq!(step_right(&text, 0), 2);
        assert_eq!(step_right(&text, 2), 5);
        assert_eq!(step_right(&text, 5), 9);
        assert_eq!(step_right(&text, 9), 9);
        assert_eq!(step_left(&text, 9), 5);
        assert_eq!(step_left(&text, 5), 2);
        assert_eq!(step_left(&text, 2), 0);
    }

    #[test]
    fn crlf_terminators_are_never_a_cursor_position() {
        let text = Rope::from("ab\r\ncd");

        assert_eq!(step_right(&text, 1), 1);
        assert_eq!(clamp_to_character(&text, 2), 1);
        assert_eq!(character_range(&text, 1), Some(1..2));
        assert_eq!(
            step_vertical(&text, 1, 1, None).map(|step| step.offset),
            Some(5)
        );
    }

    #[test]
    fn empty_buffer_and_empty_lines_have_a_single_position() {
        let empty = Rope::from("");

        assert_eq!(step_left(&empty, 0), 0);
        assert_eq!(step_right(&empty, 0), 0);
        assert_eq!(clamp_to_character(&empty, 0), 0);
        assert_eq!(step_vertical(&empty, 0, 1, None), None);
        assert_eq!(step_vertical(&empty, 0, -1, None), None);
        assert_eq!(character_range(&empty, 0), None);

        let with_empty_line = Rope::from("a\n\nb");
        assert_eq!(character_range(&with_empty_line, 2), None);
        assert_eq!(step_right(&with_empty_line, 2), 2);
    }

    #[test]
    fn vertical_steps_stop_at_the_first_and_last_line() {
        let text = Rope::from("ab\ncd\nef");

        assert_eq!(step_vertical(&text, 0, -1, None), None);
        assert_eq!(step_vertical(&text, 7, 1, None), None);
        assert_eq!(
            step_vertical(&text, 1, 1, None),
            Some(VerticalStep {
                offset: 4,
                goal_column: 1
            })
        );
    }

    #[test]
    fn vertical_steps_keep_the_goal_column_across_short_lines() {
        let text = Rope::from("abcdef\nx\nabcdef");

        let down = step_vertical(&text, 4, 1, None).expect("second line");
        assert_eq!(
            down.offset, 7,
            "clamped onto the only character of the short line"
        );
        assert_eq!(down.goal_column, 4);

        let again =
            step_vertical(&text, down.offset, 1, Some(down.goal_column)).expect("third line");
        assert_eq!(again.offset, 9 + 4, "the goal column is restored");
    }

    #[test]
    fn vertical_steps_count_columns_in_characters() {
        let text = Rope::from("é中🎉x\nabcd");

        let down = step_vertical(&text, 9, 1, None).expect("second line");
        assert_eq!(down.goal_column, 3);
        assert_eq!(down.offset, 11 + 3);
    }

    #[test]
    fn a_trailing_newline_leaves_an_empty_last_line() {
        let text = Rope::from("ab\n");

        assert_eq!(
            step_vertical(&text, 1, 1, None).map(|step| step.offset),
            Some(3)
        );
        assert_eq!(character_range(&text, 3), None);
    }

    #[test]
    fn character_range_covers_one_whole_character() {
        let text = Rope::from("a中b");

        assert_eq!(character_range(&text, 0), Some(0..1));
        assert_eq!(character_range(&text, 1), Some(1..4));
        assert_eq!(character_range(&text, 4), Some(4..5));
        assert_eq!(character_range(&text, 5), None, "past the last character");
    }

    #[test]
    fn clamping_pulls_a_line_end_cursor_onto_the_last_character() {
        let text = Rope::from("abc\nde");

        assert_eq!(clamp_to_character(&text, 3), 2);
        assert_eq!(clamp_to_character(&text, 6), 5);
        assert_eq!(clamp_to_character(&text, 1), 1);
    }
}
