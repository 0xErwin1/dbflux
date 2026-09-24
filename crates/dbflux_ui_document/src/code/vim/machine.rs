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
}

/// What a key does in the current mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VimCommand {
    MoveLeft,
    MoveRight,
    MoveUp,
    MoveDown,
    EnterInsert,
    Append,
    AppendLine,
    InsertLine,
    WordEnd(bool),
    WordForward(bool),
    WordBackward(bool),
    Digit(u8),
    LeaveInsert,
    DeleteChar,
    Undo,
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
        VimMode::Normal => {
            // Tab and Shift+Tab would otherwise indent or move focus out of the editor.
            if key.key == "tab" {
                return Some(VimCommand::Swallow);
            }

            if key.shift {
                return match key.key {
                    "a" => Some(VimCommand::AppendLine),
                    "i" => Some(VimCommand::InsertLine),
                    "e" => Some(VimCommand::WordEnd(true)),
                    "w" => Some(VimCommand::WordForward(true)),
                    "b" => Some(VimCommand::WordBackward(true)),
                    _ => None,
                };
            }

            match key.key {
                "h" => Some(VimCommand::MoveLeft),
                "l" => Some(VimCommand::MoveRight),
                "j" | "enter" => Some(VimCommand::MoveDown),
                "k" => Some(VimCommand::MoveUp),
                "i" => Some(VimCommand::EnterInsert),
                "a" => Some(VimCommand::Append),
                "e" => Some(VimCommand::WordEnd(false)),
                "w" => Some(VimCommand::WordForward(false)),
                "b" => Some(VimCommand::WordBackward(false)),
                "0" => Some(VimCommand::Digit(0)),
                digit if digit.len() == 1 && digit.as_bytes()[0].is_ascii_digit() => {
                    Some(VimCommand::Digit(digit.as_bytes()[0] - b'0'))
                }
                "x" => Some(VimCommand::DeleteChar),
                "u" => Some(VimCommand::Undo),
                _ => None,
            }
        }
    }
}

/// The mode a command leaves the editor in.
pub(crate) fn mode_after(mode: VimMode, command: VimCommand) -> VimMode {
    match command {
        VimCommand::EnterInsert => VimMode::Insert,
        VimCommand::LeaveInsert => VimMode::Normal,
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
            ("tab", VimCommand::Swallow),
        ];

        for (name, command) in expected {
            assert_eq!(
                command_for(VimMode::Normal, key(name)),
                Some(command),
                "{name}"
            );
        }

        for name in ["o", "p", "d", "/", "escape", "backspace", "space"] {
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
