//! Window-free core of Vim mode: which command a key means in each mode, and
//! where each command puts the cursor.
//!
//! Positions are UTF-8 byte offsets into the editor's rope, the unit
//! gpui-component's `InputState` uses. The cursor moves one Unicode scalar value
//! at a time, the same step the editor's own arrow keys take. In Normal mode the
//! cursor sits on a character, so it never rests on a line terminator (`\n`, or
//! the `\r` of a CRLF pair) unless the line is empty.

use crate::controls::{Rope, RopeExt};
use std::ops::Range;

/// Editing mode of a code editor while Vim mode is enabled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum VimMode {
    #[default]
    Normal,
    Insert,
    Replace,
    Visual,
    VisualLine,
    VisualBlock,
}

impl VimMode {
    /// The mode's value for the `vim_mode` keymap context key.
    pub fn context_id(self) -> &'static str {
        match self {
            VimMode::Normal => "normal",
            VimMode::Insert => "insert",
            VimMode::Replace => "replace",
            VimMode::Visual => "visual",
            VimMode::VisualLine => "visual_line",
            VimMode::VisualBlock => "visual_block",
        }
    }

    /// Insert and Replace let the native input edit text; the other modes lock it.
    pub fn accepts_text(self) -> bool {
        matches!(self, VimMode::Insert | VimMode::Replace)
    }
}

/// What a key does in the current mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VimCommand {
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
    ReplaceOnce,
    VisualDelete,
    VisualChange,
    VisualYank,
    Operator(char),
    Undo,
    OpenSearch,
    RepeatSearch(bool),
    EnterReplace,
    OpenLineBelow,
    OpenLineAbove,
    PutAfter,
    PutBefore,
    /// `$`: the last character of the line.
    LineEnd,
    /// `^`: the first non-blank character of the line.
    FirstNonBlank,
    /// `f`, `t`, `F` or `T`, waiting for the character to find.
    PendingFind(FindKind),
    /// `;` (`false`) or `,` (`true`, the opposite direction): repeats the last find.
    RepeatFind(bool),
    /// `%`: the bracket matching the one under or after the cursor.
    MatchPair,
    /// `i` (`false`) or `a` (`true`) in Visual mode, waiting for the object.
    PendingTextObject(bool),
    /// `D`: `d$`.
    DeleteToEnd,
    /// `C`: `c$`.
    ChangeToEnd,
    /// `s`: changes the characters under the cursor.
    Substitute,
    /// `S`: `cc`.
    SubstituteLine,
    /// `J`: joins lines with one space.
    JoinLines,
    /// `~`: switches the case of the characters under the cursor.
    ToggleCase,
    /// Visual `p` (`true`, the replaced text goes to the clipboard) or `P`
    /// (`false`, the clipboard is kept): replaces the selection.
    VisualPut(bool),
    /// `Ctrl+R`.
    Redo,
    /// `Ctrl+D`: half a screen down.
    HalfPageDown,
    /// `Ctrl+U`: half a screen up.
    HalfPageUp,
    /// `.`: repeats the last change.
    RepeatChange,
    /// `*` (`false`) or `#` (`true`, backward): searches the word under the
    /// cursor as a whole word.
    SearchWord(bool),
    /// `?`: opens the find panel searching backward.
    OpenSearchBackward,
    /// `}`: the next blank line.
    ParagraphForward,
    /// `{`: the previous blank line.
    ParagraphBackward,
}

/// The object of `iw`, `a"`, `i(` and the like.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextObject {
    Word { big: bool },
    Quote(char),
    Pair { open: char, close: char },
}

impl TextObject {
    /// The object a key names after `i` or `a`.
    pub fn from_char(character: char) -> Option<Self> {
        let pair = |open, close| Some(Self::Pair { open, close });
        match character {
            'w' => Some(Self::Word { big: false }),
            'W' => Some(Self::Word { big: true }),
            '"' | '\'' | '`' => Some(Self::Quote(character)),
            '(' | ')' | 'b' => pair('(', ')'),
            '[' | ']' => pair('[', ']'),
            '{' | '}' | 'B' => pair('{', '}'),
            '<' | '>' => pair('<', '>'),
            _ => None,
        }
    }
}

/// Direction of a character find, and whether it stops next to the character
/// (`t` / `T`) instead of on it (`f` / `F`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FindKind {
    pub forward: bool,
    pub till: bool,
}

impl FindKind {
    /// The same find in the other direction, for `,`.
    pub fn reversed(self) -> Self {
        Self {
            forward: !self.forward,
            ..self
        }
    }

    /// The key that starts this find.
    pub fn key(self) -> char {
        match (self.forward, self.till) {
            (true, false) => 'f',
            (true, true) => 't',
            (false, false) => 'F',
            (false, true) => 'T',
        }
    }
}

/// A motion with a target of its own on the buffer, usable alone or after an
/// operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LineMotion {
    LineStart,
    FirstNonBlank,
    LineEnd,
    /// `repeat` is set for `;` and `,`: a till then skips a target right next
    /// to the cursor, so repeating it moves on.
    Find {
        kind: FindKind,
        target: char,
        repeat: bool,
    },
    MatchPair,
    /// `}` (`true`) or `{`: the next or previous blank line, or the buffer's
    /// end or start when there is none.
    Paragraph(bool),
}

/// The parts of a keystroke the machine needs.
#[derive(Clone, Copy, Debug)]
pub struct VimKey<'a> {
    /// GPUI key name, such as `"h"`, `"enter"` or `"escape"`.
    pub key: &'a str,
    pub shift: bool,
    /// Control, Alt, Cmd/Super or Fn is held. Such keys are application shortcuts
    /// and always pass through.
    pub command_modifier: bool,
}

/// Maps a key to its command in `mode`. `None` means the key is not Vim's to
/// handle and continues to the editor and the application keymap.
// Attribute on the function: the indexing site is the byte access in the digit
// match arm, nested inside the mode and key matches rather than being the
// function's tail expression.
#[expect(
    clippy::indexing_slicing,
    reason = "the digit arm guard requires digit.len() == 1, so byte index 0 always exists"
)]
pub fn command_for(mode: VimMode, key: VimKey<'_>) -> Option<VimCommand> {
    if key.command_modifier {
        return None;
    }

    match mode {
        VimMode::Insert | VimMode::Replace => {
            (key.key == "escape" && !key.shift).then_some(VimCommand::LeaveInsert)
        }
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
            match key.key {
                "$" => return Some(VimCommand::LineEnd),
                "^" => return Some(VimCommand::FirstNonBlank),
                "%" => return Some(VimCommand::MatchPair),
                "~" if !visual => return Some(VimCommand::ToggleCase),
                "*" if !visual => return Some(VimCommand::SearchWord(false)),
                "#" if !visual => return Some(VimCommand::SearchWord(true)),
                "?" if !visual => return Some(VimCommand::OpenSearchBackward),
                "}" => return Some(VimCommand::ParagraphForward),
                "{" => return Some(VimCommand::ParagraphBackward),
                ";" => return Some(VimCommand::RepeatFind(false)),
                "," => return Some(VimCommand::RepeatFind(true)),
                _ => {}
            }
            if key.shift {
                return match key.key {
                    "d" if !visual => Some(VimCommand::DeleteToEnd),
                    "c" if !visual => Some(VimCommand::ChangeToEnd),
                    "s" if !visual => Some(VimCommand::SubstituteLine),
                    "j" if !visual => Some(VimCommand::JoinLines),
                    "p" if mode != VimMode::VisualBlock && visual => {
                        Some(VimCommand::VisualPut(false))
                    }
                    "f" => Some(VimCommand::PendingFind(FindKind {
                        forward: false,
                        till: false,
                    })),
                    "t" => Some(VimCommand::PendingFind(FindKind {
                        forward: false,
                        till: true,
                    })),
                    "a" if !visual => Some(VimCommand::AppendLine),
                    "i" if !visual => Some(VimCommand::InsertLine),
                    "r" if !visual => Some(VimCommand::EnterReplace),
                    "e" => Some(VimCommand::WordEnd(true)),
                    "w" => Some(VimCommand::WordForward(true)),
                    "b" => Some(VimCommand::WordBackward(true)),
                    "g" => Some(VimCommand::LastLine),
                    "n" if !visual => Some(VimCommand::RepeatSearch(true)),
                    "o" if !visual => Some(VimCommand::OpenLineAbove),
                    "p" if !visual => Some(VimCommand::PutBefore),
                    _ => None,
                };
            }

            match key.key {
                "g" => Some(VimCommand::PendingG),
                "m" | "'" | "`" if !visual => {
                    Some(VimCommand::PendingMark(key.key.chars().next()?))
                }
                "/" if !visual => Some(VimCommand::OpenSearch),
                "s" if !visual => Some(VimCommand::Substitute),
                "." if !visual => Some(VimCommand::RepeatChange),
                "p" if mode != VimMode::VisualBlock && visual => Some(VimCommand::VisualPut(true)),
                "f" => Some(VimCommand::PendingFind(FindKind {
                    forward: true,
                    till: false,
                })),
                "t" => Some(VimCommand::PendingFind(FindKind {
                    forward: true,
                    till: true,
                })),
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
                "i" if visual => Some(VimCommand::PendingTextObject(false)),
                "a" if visual => Some(VimCommand::PendingTextObject(true)),
                "x" | "d" if visual => Some(VimCommand::VisualDelete),
                "c" if visual => Some(VimCommand::VisualChange),
                "y" if visual => Some(VimCommand::VisualYank),
                "x" if !visual => Some(VimCommand::DeleteChar),
                "r" if !visual => Some(VimCommand::ReplaceOnce),
                "d" if !visual => Some(VimCommand::Operator('d')),
                "c" if !visual => Some(VimCommand::Operator('c')),
                "y" if !visual => Some(VimCommand::Operator('y')),
                "u" if !visual => Some(VimCommand::Undo),
                "o" if !visual => Some(VimCommand::OpenLineBelow),
                "p" if !visual => Some(VimCommand::PutAfter),
                _ => None,
            }
        }
    }
}

/// Maps a key pressed with Control alone to its command in `mode`. Every
/// other Control key is an application shortcut and passes through.
pub fn control_command(mode: VimMode, key: &str) -> Option<VimCommand> {
    if mode.accepts_text() {
        return None;
    }

    match key {
        "r" if mode == VimMode::Normal => Some(VimCommand::Redo),
        "d" => Some(VimCommand::HalfPageDown),
        "u" => Some(VimCommand::HalfPageUp),
        _ => None,
    }
}

/// The mode a command leaves the editor in.
pub fn mode_after(mode: VimMode, command: VimCommand) -> VimMode {
    match command {
        VimCommand::EnterInsert | VimCommand::OpenLineBelow | VimCommand::OpenLineAbove => {
            VimMode::Insert
        }
        VimCommand::EnterReplace => VimMode::Replace,
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
pub fn clamp_to_character(text: &Rope, offset: usize) -> usize {
    let line = Line::containing(text, offset);
    line.start + line.column_of(offset).min(line.last_column())
}

/// One character left, stopping at the line start.
pub fn step_left(text: &Rope, offset: usize) -> usize {
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
pub fn step_right(text: &Rope, offset: usize) -> usize {
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
pub fn absolute_line(text: &Rope, row: usize) -> usize {
    let line = Line::at_row(text, row.min(text.lines_len().saturating_sub(1)));
    line.start
        + line
            .content
            .char_indices()
            .find(|(_, character)| !character.is_whitespace())
            .map_or(0, |(column, _)| column)
}

pub fn line_start(text: &Rope, offset: usize) -> usize {
    Line::containing(text, offset).start
}

pub fn line_first_nonblank(text: &Rope, offset: usize) -> usize {
    let line = Line::containing(text, offset);
    line.start
        + line
            .content
            .char_indices()
            .find(|(_, character)| !character.is_whitespace())
            .map_or(0, |(column, _)| column)
}

pub fn line_end(text: &Rope, offset: usize) -> usize {
    let line = Line::containing(text, offset);
    line.start + line.content.len()
}

/// Where `motion` puts the cursor, `count` times where a count applies, or
/// `None` when it finds no target.
pub fn line_motion_target(
    text: &Rope,
    offset: usize,
    motion: LineMotion,
    count: usize,
) -> Option<usize> {
    match motion {
        LineMotion::LineStart => Some(line_start(text, offset)),
        LineMotion::FirstNonBlank => Some(line_first_nonblank(text, offset)),
        LineMotion::LineEnd => {
            let end = line_end_offset(text, offset, count);
            Some(clamp_to_character(text, end))
        }
        LineMotion::Find {
            kind,
            target,
            repeat,
        } => find_target(text, offset, kind, target, count, repeat),
        LineMotion::MatchPair => matching_pair(text, offset),
        LineMotion::Paragraph(forward) => Some(match paragraph_row(text, offset, forward, count) {
            Some(row) => text.line_start_offset(row),
            None if forward => clamp_to_character(text, text.len()),
            None => 0,
        }),
    }
}

/// The blank line `count` paragraphs away, or `None` when the buffer ends
/// first. A blank line is an empty one; a line of spaces is part of its
/// paragraph, as in Vim.
fn paragraph_row(text: &Rope, offset: usize, forward: bool, count: usize) -> Option<usize> {
    let last = text.lines_len().saturating_sub(1);
    let blank = |row: usize| Line::at_row(text, row).content.is_empty();
    let step = |row: usize| {
        if forward {
            (row < last).then_some(row + 1)
        } else {
            row.checked_sub(1)
        }
    };
    let mut row = text.offset_to_point(offset).row;

    for _ in 0..count.max(1) {
        // From a blank line, the blank lines next to it are skipped first.
        while blank(row) {
            row = step(row)?;
        }
        while !blank(row) {
            row = step(row)?;
        }
    }
    Some(row)
}

/// The word under the cursor, or the first one after it on the line, for
/// `*` and `#`.
pub fn word_under_cursor(text: &Rope, offset: usize) -> Option<Range<usize>> {
    let line = Line::containing(text, offset);
    let (runs, current) = word_runs(&line, offset, false)?;
    let (start, end, _) = runs
        .iter()
        .skip(current)
        .find(|run| run.2 == WordClass::Keyword)
        .copied()?;
    let byte = |index: usize| {
        line.content
            .char_indices()
            .nth(index)
            .map_or(line.content.len(), |(at, _)| at)
    };
    Some(line.start + byte(start)..line.start + byte(end))
}

/// Whether `range` is a whole word: no letter, digit or `_` right before or
/// after it.
pub fn is_whole_word(text: &Rope, range: Range<usize>) -> bool {
    let content = text.to_string();
    let keyword = |character: char| character.is_alphanumeric() || character == '_';
    let before = content
        .get(..range.start)
        .and_then(|before| before.chars().next_back());
    let after = content
        .get(range.end..)
        .and_then(|after| after.chars().next());
    !before.is_some_and(keyword) && !after.is_some_and(keyword)
}

/// The range an operator acts on for `motion`: `f`, `t`, `$` and `%` include
/// their destination, the backward motions exclude the cursor character.
/// `None` when the motion finds no target; an empty range when it does but
/// covers nothing (`$` on an empty line).
pub fn line_motion_operator_range(
    text: &Rope,
    offset: usize,
    motion: LineMotion,
    count: usize,
) -> Option<Range<usize>> {
    match motion {
        LineMotion::LineEnd => {
            let end = line_end_offset(text, offset, count);
            Some(offset..end.max(offset))
        }
        LineMotion::Find { kind, .. } if !kind.forward => {
            let target = line_motion_target(text, offset, motion, count)?;
            Some(target..offset.max(target))
        }
        LineMotion::Find { .. } | LineMotion::MatchPair => {
            let target = line_motion_target(text, offset, motion, count)?;
            let start = offset.min(target);
            let last = offset.max(target);
            let width = counted_character_range(text, last, 1).map_or(0, |range| range.len());
            Some(start..last + width)
        }
        LineMotion::Paragraph(true) if paragraph_row(text, offset, true, count).is_none() => {
            Some(offset..text.len())
        }
        LineMotion::LineStart | LineMotion::FirstNonBlank | LineMotion::Paragraph(_) => {
            let target = line_motion_target(text, offset, motion, count)?;
            Some(offset.min(target)..offset.max(target))
        }
    }
}

/// End of the content of the line `count - 1` lines below the cursor's,
/// clamped to the last line.
fn line_end_offset(text: &Rope, offset: usize, count: usize) -> usize {
    let row = text
        .offset_to_point(offset)
        .row
        .saturating_add(count.max(1) - 1)
        .min(text.lines_len().saturating_sub(1));
    let line = Line::at_row(text, row);
    line.start + line.content.len()
}

/// The `count`-th `target` on the cursor's line in `kind`'s direction, or the
/// character next to it for a till. Never leaves the line.
fn find_target(
    text: &Rope,
    offset: usize,
    kind: FindKind,
    target: char,
    count: usize,
    repeat: bool,
) -> Option<usize> {
    let line = Line::containing(text, offset);
    let column = line.column_of(offset);
    let chars: Vec<(usize, char)> = line.content.char_indices().collect();
    let index = chars.iter().position(|(at, _)| *at == column)?;
    let skip = usize::from(kind.till && repeat);
    let count = count.max(1);

    let target_index = if kind.forward {
        let start = index.saturating_add(1).saturating_add(skip);
        let found = chars
            .iter()
            .enumerate()
            .skip(start)
            .filter(|(_, (_, character))| *character == target)
            .nth(count - 1)?
            .0;
        if kind.till { found - 1 } else { found }
    } else {
        let end = index.saturating_sub(skip);
        let found = chars
            .iter()
            .enumerate()
            .take(end)
            .rev()
            .filter(|(_, (_, character))| *character == target)
            .nth(count - 1)?
            .0;
        if kind.till { found + 1 } else { found }
    };

    chars.get(target_index).map(|(at, _)| line.start + at)
}

/// The bracket matching the first `()[]{}` bracket at or after the cursor on
/// its line, counting nesting and crossing lines.
pub fn matching_pair(text: &Rope, offset: usize) -> Option<usize> {
    let line = Line::containing(text, offset);
    let column = line.column_of(offset);
    let (relative, bracket) = line.content[column..]
        .char_indices()
        .find(|(_, character)| matches!(character, '(' | ')' | '[' | ']' | '{' | '}'))?;
    let at = line.start + column + relative;

    let (open, close, forward) = match bracket {
        '(' => ('(', ')', true),
        ')' => ('(', ')', false),
        '[' => ('[', ']', true),
        ']' => ('[', ']', false),
        '{' => ('{', '}', true),
        _ => ('{', '}', false),
    };

    let content = text.to_string();
    let mut depth = 0usize;
    if forward {
        for (position, character) in content.get(at..)?.char_indices() {
            if character == open {
                depth += 1;
            } else if character == close {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(at + position);
                }
            }
        }
    } else {
        for (position, character) in content.get(..=at)?.char_indices().rev() {
            if character == close {
                depth += 1;
            } else if character == open {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(position);
                }
            }
        }
    }
    None
}

/// The range of a text object at the cursor, and whether it is whole lines.
/// `around` selects the `a` form. `count` extends a word object over more
/// words, and picks an outer pair of brackets. `None` when there is no object.
pub fn text_object_range(
    text: &Rope,
    offset: usize,
    object: TextObject,
    around: bool,
    count: usize,
) -> Option<(Range<usize>, bool)> {
    let count = count.max(1);
    match object {
        TextObject::Word { big } => {
            word_object_range(text, offset, big, around, count).map(|range| (range, false))
        }
        TextObject::Quote(quote) => {
            quote_object_range(text, offset, quote, around).map(|range| (range, false))
        }
        TextObject::Pair { open, close } => {
            pair_object_range(text, offset, open, close, around, count)
        }
    }
}

/// Runs of characters of one class on the cursor's line, as character index
/// ranges, and the index of the run under the cursor.
fn word_runs(
    line: &Line,
    offset: usize,
    big: bool,
) -> Option<(Vec<(usize, usize, WordClass)>, usize)> {
    let characters: Vec<char> = line.content.chars().collect();
    if characters.is_empty() {
        return None;
    }

    let cursor = line.char_count_before(line.column_of(offset).min(line.last_column()));
    let mut runs: Vec<(usize, usize, WordClass)> = Vec::new();
    for (index, character) in characters.iter().enumerate() {
        let class = word_class(*character, big);
        match runs.last_mut() {
            Some(run) if run.2 == class => run.1 = index + 1,
            _ => runs.push((index, index + 1, class)),
        }
    }

    let current = runs
        .iter()
        .position(|run| run.0 <= cursor && cursor < run.1)?;
    Some((runs, current))
}

/// `iw` / `aw` on the cursor's line. `iw` counts a run of spaces as a word;
/// `aw` adds the spaces after each word, or before the first one when the
/// last word has none after it.
fn word_object_range(
    text: &Rope,
    offset: usize,
    big: bool,
    around: bool,
    count: usize,
) -> Option<Range<usize>> {
    let line = Line::containing(text, offset);
    let (runs, current) = word_runs(&line, offset, big)?;
    let last_run = runs.len() - 1;

    let is_space = |index: usize| runs.get(index).is_some_and(|run| run.2 == WordClass::Space);

    let (first, last) = if !around {
        (current, current.saturating_add(count - 1).min(last_run))
    } else {
        // Each count takes one run and, after it, a run of the other kind:
        // the spaces after a word, or the word after spaces.
        let mut last = current;
        let mut index = current;
        for _ in 0..count {
            if index > last_run {
                break;
            }
            last = index;
            if index < last_run && is_space(index + 1) != is_space(index) {
                last = index + 1;
            }
            index = last + 1;
        }

        let first = if !is_space(current) && !is_space(last) && current > 0 && is_space(current - 1)
        {
            current - 1
        } else {
            current
        };
        (first, last)
    };

    let start_char = runs.get(first)?.0;
    let end_char = runs.get(last)?.1;
    let byte = |index: usize| {
        line.content
            .char_indices()
            .nth(index)
            .map_or(line.content.len(), |(at, _)| at)
    };
    Some(line.start + byte(start_char)..line.start + byte(end_char))
}

/// `i"` / `a"` and the other quotes, on the cursor's line only. On a quote,
/// the quotes are paired from the line start; elsewhere the nearest quotes
/// before and after the cursor are used, or, with none before, the first two
/// after it. A quote after a backslash does not count. `a"` adds the spaces
/// after the closing quote, or before the opening one when there are none.
fn quote_object_range(
    text: &Rope,
    offset: usize,
    quote: char,
    around: bool,
) -> Option<Range<usize>> {
    let line = Line::containing(text, offset);
    let column = line.column_of(offset);
    let mut quotes = Vec::new();
    let mut escaped = false;
    for (at, character) in line.content.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' {
            escaped = true;
        } else if character == quote {
            quotes.push(at);
        }
    }

    let (open, close) = if let Some(index) = quotes.iter().position(|at| *at == column) {
        if index % 2 == 0 {
            (column, *quotes.get(index + 1)?)
        } else {
            (*quotes.get(index - 1)?, column)
        }
    } else {
        let before = quotes.iter().rev().find(|at| **at < column).copied();
        let mut after = quotes.iter().filter(|at| **at > column).copied();
        match before {
            Some(before) => (before, after.next()?),
            None => (after.next()?, after.next()?),
        }
    };

    if !around {
        return Some(line.start + open + quote.len_utf8()..line.start + close);
    }

    let end = close + quote.len_utf8();
    let trailing = line.content[end..]
        .chars()
        .take_while(|character| matches!(character, ' ' | '\t'))
        .count();
    let (start, end) = if trailing > 0 {
        (open, end + trailing)
    } else {
        let leading = line.content[..open]
            .chars()
            .rev()
            .take_while(|character| matches!(character, ' ' | '\t'))
            .count();
        (open - leading, end)
    };
    Some(line.start + start..line.start + end)
}

/// `i(` / `a(` and the other brackets: the `count`-th pair around the cursor,
/// across lines. A cursor on a bracket belongs to that bracket's pair. When
/// the inside starts with a line break and the closing bracket has only
/// spaces before it on its line, `i(` covers the whole lines between the
/// brackets.
fn pair_object_range(
    text: &Rope,
    offset: usize,
    open: char,
    close: char,
    around: bool,
    count: usize,
) -> Option<(Range<usize>, bool)> {
    let content = text.to_string();
    let offset = offset.min(content.len());

    let mut remaining = count;
    let mut depth = 0usize;
    let mut start = None;
    let upto = content
        .get(offset..)
        .and_then(|rest| rest.chars().next())
        .map_or(offset, |character| offset + character.len_utf8());
    for (at, character) in content.get(..upto)?.char_indices().rev() {
        if character == close && at != offset {
            depth += 1;
        } else if character == open {
            if depth == 0 {
                remaining -= 1;
                if remaining == 0 {
                    start = Some(at);
                    break;
                }
            } else {
                depth -= 1;
            }
        }
    }
    let start = start?;

    let mut depth = 0usize;
    let mut end = None;
    for (at, character) in content.get(start..)?.char_indices() {
        if character == open {
            depth += 1;
        } else if character == close {
            depth -= 1;
            if depth == 0 {
                end = Some(start + at);
                break;
            }
        }
    }
    let end = end?;

    if around {
        return Some((start..end + close.len_utf8(), false));
    }

    let inner_start = start + open.len_utf8();
    let after_open = content.get(inner_start..end).unwrap_or_default();
    let line_break = if after_open.starts_with("\r\n") {
        2
    } else if after_open.starts_with('\n') {
        1
    } else {
        0
    };
    if line_break == 0 {
        return Some((inner_start..end, false));
    }

    let inner_start = inner_start + line_break;
    let close_line_start = content
        .get(..end)
        .and_then(|before| before.rfind('\n'))
        .map_or(0, |at| at + 1);
    let blank_before_close = content.get(close_line_start..end).is_some_and(|before| {
        before
            .chars()
            .all(|character| matches!(character, ' ' | '\t'))
    });
    if blank_before_close && close_line_start > inner_start {
        Some((inner_start..close_line_start, true))
    } else if blank_before_close {
        Some((inner_start..inner_start, false))
    } else {
        Some((inner_start..end, false))
    }
}

/// An edit of one range, and where the cursor goes after it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replacement {
    pub range: Range<usize>,
    pub text: String,
    pub cursor: usize,
}

/// Visual `p` / `P`: replaces `selection` (whole lines when
/// `selection_linewise`) with `register`, `count` times. Whole lines over
/// characters go on lines of their own, characters over whole lines keep the
/// last line break, and whole lines put over the unterminated last line add
/// no line break after it. The cursor goes to the first non-blank character
/// of put lines, or to the last put character.
pub fn visual_put(
    text: &Rope,
    selection: Range<usize>,
    selection_linewise: bool,
    register: &str,
    register_linewise: bool,
    count: usize,
) -> Replacement {
    let count = count.clamp(1, MAX_PUT_COUNT);
    let line = Line::containing(text, selection.start);
    let terminator = line_terminator(text, &line);
    let content = text.to_string();
    let selected = content.get(selection.clone()).unwrap_or_default();
    let selected_terminator = if selected.ends_with("\r\n") {
        2
    } else {
        usize::from(selected.ends_with('\n'))
    };

    if !register_linewise {
        let range = if selection_linewise {
            selection.start..selection.end - selected_terminator
        } else {
            selection
        };
        let inserted = register.repeat(count);
        let last_width = inserted.chars().next_back().map_or(0, char::len_utf8);
        return Replacement {
            cursor: range.start + inserted.len() - last_width,
            range,
            text: inserted,
        };
    }

    let body = if register.ends_with('\n') {
        register.to_string()
    } else {
        format!("{register}{terminator}")
    }
    .repeat(count);
    let first_nonblank = body
        .split('\n')
        .next()
        .unwrap_or_default()
        .trim_end_matches('\r')
        .char_indices()
        .find(|(_, character)| !character.is_whitespace())
        .map_or(0, |(column, _)| column);

    let (inserted, first_line) = if !selection_linewise {
        (
            format!("{terminator}{body}"),
            selection.start + terminator.len(),
        )
    } else if selected_terminator == 0 {
        let trimmed = body
            .strip_suffix("\r\n")
            .or_else(|| body.strip_suffix('\n'))
            .unwrap_or(&body)
            .to_string();
        (trimmed, selection.start)
    } else {
        (body, selection.start)
    };

    Replacement {
        cursor: first_line + first_nonblank,
        range: selection,
        text: inserted,
    }
}

/// `J`: joins the cursor's line with the next `count - 1` lines (at least
/// one). Each joined line loses its leading spaces and gets one space before
/// it, except when it is empty or starts with `)`, or when the text before it
/// is empty or ends with a space. The cursor goes to the last join. `None`
/// on the last line.
pub fn join_lines(text: &Rope, offset: usize, count: usize) -> Option<Replacement> {
    let row = text.offset_to_point(offset).row;
    let last_row = row
        .saturating_add(count.max(2) - 1)
        .min(text.lines_len().saturating_sub(1));
    if last_row == row {
        return None;
    }

    let first = Line::at_row(text, row);
    let start = first.start + first.content.len();
    let mut joined = String::new();
    let mut previous = first.content.clone();
    let mut cursor = start;

    for next_row in row + 1..=last_row {
        let line = Line::at_row(text, next_row);
        let trimmed = line.content.trim_start_matches([' ', '\t']);
        let ends_with_space = previous.ends_with([' ', '\t']);
        let separator = if trimmed.is_empty()
            || trimmed.starts_with(')')
            || previous.is_empty()
            || ends_with_space
        {
            ""
        } else {
            " "
        };

        let join_point = start + joined.len();
        cursor = if ends_with_space {
            join_point.saturating_sub(1)
        } else {
            join_point
        };
        joined.push_str(separator);
        joined.push_str(trimmed);
        if !trimmed.is_empty() {
            previous = trimmed.to_string();
        }
    }

    let last = Line::at_row(text, last_row);
    Some(Replacement {
        range: start..last.start + last.content.len(),
        text: joined,
        cursor,
    })
}

/// `~`: the next `count` characters of the line with their case switched,
/// and the cursor after them. `None` on an empty line.
pub fn toggle_case(text: &Rope, offset: usize, count: usize) -> Option<Replacement> {
    let range = counted_character_range(text, offset, count.max(1))?;
    let original = text.slice(range.clone()).to_string();
    let toggled: String = original
        .chars()
        .flat_map(|character| {
            if character.is_lowercase() {
                character.to_uppercase().collect::<Vec<_>>()
            } else if character.is_uppercase() {
                character.to_lowercase().collect::<Vec<_>>()
            } else {
                vec![character]
            }
        })
        .collect();

    Some(Replacement {
        cursor: range.start + toggled.len(),
        range,
        text: toggled,
    })
}

/// Keys that edit or move the cursor through editor actions instead of
/// delivering text.
pub fn is_editing_key(key: &str) -> bool {
    matches!(
        key,
        "backspace"
            | "delete"
            | "enter"
            | "tab"
            | "left"
            | "right"
            | "up"
            | "down"
            | "home"
            | "end"
            | "pageup"
            | "pagedown"
            | "insert"
    )
}

/// The line's own terminator, or on an unterminated last line the buffer's
/// first CRLF or else LF.
fn line_terminator(text: &Rope, line: &Line) -> &'static str {
    let content = text.to_string();
    let after = content
        .get(line.start + line.content.len()..)
        .unwrap_or_default();
    let crlf = if after.is_empty() {
        content.contains("\r\n")
    } else {
        after.starts_with("\r\n")
    };

    if crlf { "\r\n" } else { "\n" }
}

/// The line's leading spaces and tabs, as Vim's autoindent keeps them.
fn line_indent(line: &Line) -> String {
    line.content
        .chars()
        .take_while(|character| matches!(character, ' ' | '\t'))
        .collect()
}

/// The line break `r<CR>` inserts: the line's own terminator (on an unterminated
/// last line, the buffer's first CRLF or else LF), then the line's leading
/// whitespace, as Vim's autoindent keeps it.
pub fn line_break_with_indent(text: &Rope, offset: usize) -> String {
    let line = Line::containing(text, offset);
    format!("{}{}", line_terminator(text, &line), line_indent(&line))
}

/// Text inserted at one offset, and where the cursor goes once it is in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Insertion {
    pub offset: usize,
    pub text: String,
    pub cursor: usize,
}

/// The new line `o` (below) or `O` (above) opens next to the cursor's logical
/// line: the line's terminator and its indent, with the cursor after the indent.
pub fn open_line(text: &Rope, offset: usize, below: bool) -> Insertion {
    let line = Line::containing(text, offset);
    let terminator = line_terminator(text, &line);
    let indent = line_indent(&line);

    if below {
        let at = line.start + line.content.len();
        let inserted = format!("{terminator}{indent}");
        Insertion {
            offset: at,
            cursor: at + inserted.len(),
            text: inserted,
        }
    } else {
        Insertion {
            offset: line.start,
            cursor: line.start + indent.len(),
            text: format!("{indent}{terminator}"),
        }
    }
}

/// Upper bound on a put count, so a mistyped count cannot exhaust memory.
const MAX_PUT_COUNT: usize = 10_000;

/// Whether `p` puts `clipboard` as whole lines. Text Vim itself last wrote to
/// the clipboard keeps the kind it was yanked or deleted with; any other text
/// is linewise when it ends with a line break.
pub fn put_is_linewise(clipboard: &str, remembered: Option<(&str, bool)>) -> bool {
    match remembered {
        Some((text, linewise)) if text == clipboard => linewise,
        _ => clipboard.ends_with('\n'),
    }
}

/// What `p` (`after`) or `P` puts for `register`, repeated `count` times.
///
/// Linewise text goes below or above the cursor's logical line, with the cursor
/// on the first non-blank character of the first put line. Text without a final
/// line break gets the buffer's, and below an unterminated last line the
/// separator moves to the front so no empty line is left behind. Characterwise
/// text goes after or at the cursor, with the cursor on its last character.
/// An empty register puts nothing.
pub fn put(
    text: &Rope,
    offset: usize,
    register: &str,
    linewise: bool,
    after: bool,
    count: usize,
) -> Option<Insertion> {
    if register.is_empty() {
        return None;
    }

    let count = count.clamp(1, MAX_PUT_COUNT);
    if !linewise {
        let at = if after {
            append_after(text, offset)
        } else {
            offset
        };
        let inserted = register.repeat(count);
        let last_width = inserted.chars().next_back().map_or(0, char::len_utf8);

        return Some(Insertion {
            offset: at,
            cursor: at + inserted.len() - last_width,
            text: inserted,
        });
    }

    let line = Line::containing(text, offset);
    let terminator = line_terminator(text, &line);
    let body = if register.ends_with('\n') {
        register.to_string()
    } else {
        format!("{register}{terminator}")
    }
    .repeat(count);

    let first_line = body.split('\n').next().unwrap_or_default();
    let first_nonblank = first_line
        .trim_end_matches('\r')
        .char_indices()
        .find(|(_, character)| !character.is_whitespace())
        .map_or(0, |(column, _)| column);

    let (at, inserted, first_line_start) = if !after {
        (line.start, body, line.start)
    } else if line.row + 1 < text.lines_len() {
        let next = text.line_start_offset(line.row + 1);
        (next, body, next)
    } else {
        let at = line.start + line.content.len();
        let trimmed = body
            .strip_suffix("\r\n")
            .or_else(|| body.strip_suffix('\n'))
            .unwrap_or(&body);
        (at, format!("{terminator}{trimmed}"), at + terminator.len())
    };

    Some(Insertion {
        offset: at,
        text: inserted,
        cursor: first_line_start + first_nonblank,
    })
}

pub fn append_after(text: &Rope, offset: usize) -> usize {
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

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum WordMotion {
    End,
    Forward,
    Backward,
}

pub fn change_word_range_with_class(
    text: &Rope,
    offset: usize,
    count: usize,
    big: bool,
) -> Option<Range<usize>> {
    let content = text.to_string();
    let chars = word_offsets(text);
    let mut index = chars.partition_point(|(start, _)| *start < offset);
    let first_class = word_class(chars.get(index)?.1, big);
    let mut remaining = count.max(1);
    while index < chars.len() && remaining > 0 {
        #[expect(
            clippy::indexing_slicing,
            reason = "the while condition checks index < chars.len() on every pass and chars is not mutated in this function"
        )]
        let class = word_class(chars[index].1, big);
        #[expect(
            clippy::indexing_slicing,
            reason = "the while condition checks index < chars.len() on every pass and chars is not mutated in this function"
        )]
        if matches!(chars[index].1, '\r' | '\n') {
            break;
        }
        #[expect(
            clippy::indexing_slicing,
            reason = "the loop condition checks index < chars.len() on every pass and chars is not mutated in this function"
        )]
        while index < chars.len()
            && !matches!(chars[index].1, '\r' | '\n')
            && word_class(chars[index].1, big) == class
        {
            index += 1;
        }
        remaining -= 1;
        if class != WordClass::Space && remaining > 0 {
            #[expect(
                clippy::indexing_slicing,
                reason = "the loop condition checks index < chars.len() on every pass and chars is not mutated in this function"
            )]
            while index < chars.len()
                && chars[index].1.is_whitespace()
                && !matches!(chars[index].1, '\r' | '\n')
            {
                index += 1;
            }
        }
    }
    let end = chars.get(index).map_or(content.len(), |(at, _)| *at);
    let end = if first_class == WordClass::Space {
        end
    } else {
        content[..end].trim_end_matches([' ', '\t']).len()
    };
    (end > offset).then_some(offset..end)
}

pub fn word_offsets(text: &Rope) -> Vec<(usize, char)> {
    text.to_string().char_indices().collect()
}

// Attribute on the function: the indexing sites span the initial clamp, the
// class closure, and the Backward and End match arms.
#[expect(
    clippy::indexing_slicing,
    reason = "chars is non-empty (early return above) and never mutated; the initial partition_point index is clamped with saturating_sub so it stays below chars.len(), forward and End access is guarded by index < chars.len() checks, and Backward decrements are bounded by index > 0, so chars[index] stays in bounds across all arms"
)]
pub fn step_word(
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
pub fn word_operator_range(
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
pub fn horizontal_operator_range(
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
    #[expect(
        clippy::indexing_slicing,
        reason = "last is clamped to columns.len() - 1 on a non-empty columns vec (the is_empty early return above), so columns[last] is in bounds and columns[last] is a valid byte offset into line.content; index comes from char_count_before(column), which callers pass from a cursor offset inside the line"
    )]
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

/// Change to the right removes the requested characters, not the destination.
pub fn change_horizontal_right_range(
    text: &Rope,
    offset: usize,
    count: usize,
) -> Option<Range<usize>> {
    let range = horizontal_operator_range(text, offset, true, count)?;
    let line = Line::containing(text, offset);
    let length: usize = line.content[range.start - line.start..]
        .chars()
        .take(count)
        .map(char::len_utf8)
        .sum();
    Some(range.start..range.start + length)
}

/// Whole current and destination logical lines, clamping at either edge.
pub fn vertical_operator_range(
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
pub fn absolute_operator_range(text: &Rope, offset: usize, target_row: usize) -> Range<usize> {
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

/// Keep the separator after changed rows when later rows remain; consume it
/// entirely (leaving no dangling empty line) when the range reaches EOF.
pub fn change_line_range(text: &Rope, range: Range<usize>) -> Range<usize> {
    let content = text.to_string();
    let selected = content.get(range.clone()).unwrap_or_default();
    let separator = if selected.ends_with("\r\n") {
        2
    } else if selected.ends_with('\n') {
        1
    } else {
        0
    };
    if range.end == content.len() && separator > 0 {
        range
    } else {
        range.start..range.end.saturating_sub(separator).max(range.start)
    }
}

/// Result of a vertical move.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VerticalStep {
    pub offset: usize,
    /// Character column the move aimed for. Passing it to the next vertical move
    /// keeps the column across shorter lines, as Vim does.
    pub goal_column: usize,
}

/// Moves `delta` lines up (negative) or down, aiming for `goal_column` (in
/// characters) or, when `None`, for the cursor's current column. Returns `None`
/// when the target line is outside the buffer.
pub fn step_vertical(
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
pub fn visual_range(text: &Rope, anchor: usize, cursor: usize, linewise: bool) -> Range<usize> {
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

/// Rows of a Visual Block that reach its left column, each with the byte range
/// of its block columns. Columns count Unicode scalars, as the native columnar
/// selection does; rows shorter than the left column are skipped, like Vim.
pub fn block_rows(text: &Rope, anchor: usize, cursor: usize) -> Vec<(usize, Range<usize>)> {
    let column = |offset: usize| {
        let line = Line::containing(text, offset);
        (line.row, line.char_count_before(line.column_of(offset)))
    };
    let (anchor_row, anchor_column) = column(anchor);
    let (cursor_row, cursor_column) = column(cursor);
    let left = anchor_column.min(cursor_column);
    let right = anchor_column.max(cursor_column);

    (anchor_row.min(cursor_row)..=anchor_row.max(cursor_row))
        .filter_map(|row| {
            let line = Line::at_row(text, row);
            let columns: Vec<usize> = line.content.char_indices().map(|(at, _)| at).collect();
            if columns.len() < left {
                return None;
            }
            let start = columns.get(left).copied().unwrap_or(line.content.len());
            let end = columns
                .get(right.saturating_add(1))
                .copied()
                .unwrap_or(line.content.len());
            Some((row, line.start + start..line.start + end))
        })
        .collect()
}

/// Byte length of a logical line, without its terminator.
pub fn line_content_len(text: &Rope, row: usize) -> usize {
    Line::at_row(text, row).content.len()
}

/// Whole logical lines, including their terminators when present. The final
/// unterminated line has no invented newline in the returned range.
pub fn counted_line_range(text: &Rope, offset: usize, count: usize) -> Range<usize> {
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
pub fn line_yank_text(content: &str, range: Range<usize>) -> Option<&str> {
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
pub fn line_delete_range(text: &Rope, range: Range<usize>) -> Range<usize> {
    if range.end != text.len() || range.start == 0 {
        return range;
    }
    let content = text.to_string();
    let before = &content[..range.start];
    let separator_len = if before.ends_with("\r\n") { 2 } else { 1 };
    range.start.saturating_sub(separator_len)..range.end
}

#[cfg(test)]
pub fn character_range(text: &Rope, offset: usize) -> Option<Range<usize>> {
    counted_character_range(text, offset, 1)
}

/// Selects at most `count` characters from one line without copying the line
/// again for each character. A zero count selects nothing.
pub fn counted_character_range(text: &Rope, offset: usize, count: usize) -> Option<Range<usize>> {
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
        ];

        for (name, command) in expected {
            assert_eq!(
                command_for(VimMode::Normal, key(name)),
                Some(command),
                "{name}"
            );
        }

        for name in ["escape", "backspace", "space", "tab"] {
            assert_eq!(command_for(VimMode::Normal, key(name)), None, "{name}");
        }
    }

    #[test]
    fn open_line_and_put_are_normal_mode_only() {
        for (name, plain, shifted_command) in [
            ("o", VimCommand::OpenLineBelow, VimCommand::OpenLineAbove),
            ("p", VimCommand::PutAfter, VimCommand::PutBefore),
        ] {
            assert_eq!(command_for(VimMode::Normal, key(name)), Some(plain));
            assert_eq!(
                command_for(VimMode::Normal, shifted(name)),
                Some(shifted_command)
            );

            let modes: &[VimMode] = if name == "p" {
                &[VimMode::VisualBlock, VimMode::Insert, VimMode::Replace]
            } else {
                &[
                    VimMode::Visual,
                    VimMode::VisualLine,
                    VimMode::VisualBlock,
                    VimMode::Insert,
                    VimMode::Replace,
                ]
            };
            for mode in modes.iter().copied() {
                assert_eq!(command_for(mode, key(name)), None, "{mode:?} {name}");
                assert_eq!(command_for(mode, shifted(name)), None, "{mode:?} {name}");
            }
        }

        for mode in [VimMode::Visual, VimMode::VisualLine] {
            assert_eq!(
                command_for(mode, key("p")),
                Some(VimCommand::VisualPut(true))
            );
            assert_eq!(
                command_for(mode, shifted("p")),
                Some(VimCommand::VisualPut(false))
            );
        }

        for command in [VimCommand::OpenLineBelow, VimCommand::OpenLineAbove] {
            assert_eq!(mode_after(VimMode::Normal, command), VimMode::Insert);
        }
        for command in [VimCommand::PutAfter, VimCommand::PutBefore] {
            assert_eq!(mode_after(VimMode::Normal, command), VimMode::Normal);
        }
    }

    fn insertion(offset: usize, text: &str, cursor: usize) -> Insertion {
        Insertion {
            offset,
            text: text.to_string(),
            cursor,
        }
    }

    #[test]
    fn open_line_keeps_indent_and_the_line_terminator() {
        let text = Rope::from("  ab\r\ncd");
        assert_eq!(open_line(&text, 2, true), insertion(4, "\r\n  ", 8));
        assert_eq!(open_line(&text, 2, false), insertion(0, "  \r\n", 2));
        assert_eq!(open_line(&text, 7, true), insertion(8, "\r\n", 10));
        assert_eq!(open_line(&text, 7, false), insertion(6, "\r\n", 6));

        let unterminated = Rope::from("\tab");
        assert_eq!(open_line(&unterminated, 1, true), insertion(3, "\n\t", 5));
        assert_eq!(open_line(&unterminated, 1, false), insertion(0, "\t\n", 1));

        let trailing = Rope::from("ab\n");
        assert_eq!(open_line(&trailing, 0, true), insertion(2, "\n", 3));
        assert_eq!(open_line(&trailing, 3, true), insertion(3, "\n", 4));

        let empty = Rope::from("");
        assert_eq!(open_line(&empty, 0, true), insertion(0, "\n", 1));
        assert_eq!(open_line(&empty, 0, false), insertion(0, "\n", 0));
    }

    #[test]
    fn put_kind_prefers_the_remembered_yank() {
        assert!(put_is_linewise("two", Some(("two", true))));
        assert!(!put_is_linewise("ab\n", Some(("ab\n", false))));
        assert!(!put_is_linewise("zz", Some(("two", true))));
        assert!(put_is_linewise("line\n", Some(("other", false))));
        assert!(put_is_linewise("line\r\n", None));
        assert!(!put_is_linewise("word", None));
    }

    #[test]
    fn linewise_put_inserts_whole_lines_below_or_above() {
        let text = Rope::from("one\n  two");
        assert_eq!(
            put(&text, 0, "one\n", true, true, 1),
            Some(insertion(4, "one\n", 4))
        );
        assert_eq!(
            put(&text, 6, "  x\n", true, false, 1),
            Some(insertion(4, "  x\n", 6))
        );
        assert_eq!(
            put(&text, 6, "two", true, true, 2),
            Some(insertion(9, "\ntwo\ntwo", 10))
        );
        assert_eq!(
            put(&text, 0, "two", true, false, 1),
            Some(insertion(0, "two\n", 0))
        );

        let crlf = Rope::from("one\r\ntwo");
        assert_eq!(
            put(&crlf, 5, "two", true, true, 1),
            Some(insertion(8, "\r\ntwo", 10))
        );
        assert_eq!(
            put(&crlf, 5, "x\r\n", true, true, 1),
            Some(insertion(8, "\r\nx", 10))
        );

        let trailing = Rope::from("ab\n");
        assert_eq!(
            put(&trailing, 0, "x\n", true, true, 1),
            Some(insertion(3, "x\n", 3))
        );
    }

    #[test]
    fn characterwise_put_lands_on_the_last_inserted_character() {
        let text = Rope::from("ab\n\ncd");
        assert_eq!(
            put(&text, 0, "ab ", false, true, 1),
            Some(insertion(1, "ab ", 3))
        );
        assert_eq!(
            put(&text, 0, "ab ", false, false, 1),
            Some(insertion(0, "ab ", 2))
        );
        assert_eq!(
            put(&text, 3, "中", false, true, 1),
            Some(insertion(3, "中", 3))
        );
        assert_eq!(
            put(&text, 0, "yz", false, true, 3),
            Some(insertion(1, "yzyzyz", 6))
        );
        assert_eq!(put(&text, 0, "", false, true, 1), None);
        assert_eq!(put(&text, 0, "", true, true, 1), None);
        assert_eq!(
            put(&text, 0, "x", false, true, usize::MAX).map(|insertion| insertion.text.len()),
            Some(MAX_PUT_COUNT)
        );
    }

    #[test]
    fn line_motion_symbols_bind_with_or_without_shift() {
        for mode in [
            VimMode::Normal,
            VimMode::Visual,
            VimMode::VisualLine,
            VimMode::VisualBlock,
        ] {
            for (name, command) in [
                ("$", VimCommand::LineEnd),
                ("^", VimCommand::FirstNonBlank),
                ("%", VimCommand::MatchPair),
                (";", VimCommand::RepeatFind(false)),
                (",", VimCommand::RepeatFind(true)),
            ] {
                assert_eq!(
                    command_for(mode, key(name)),
                    Some(command),
                    "{mode:?} {name}"
                );
                assert_eq!(
                    command_for(mode, shifted(name)),
                    Some(command),
                    "{mode:?} shift-{name}"
                );
                assert_eq!(command_for(mode, with_command_modifier(name)), None);
            }
        }
        assert_eq!(
            command_for(VimMode::Normal, shifted("t")),
            Some(VimCommand::PendingFind(FindKind {
                forward: false,
                till: true,
            }))
        );
    }

    #[test]
    fn paragraph_motions_stop_on_empty_lines() {
        let text = Rope::from("a\nb\n\nc\nd\n\ne");
        let forward = |offset| line_motion_target(&text, offset, LineMotion::Paragraph(true), 1);
        let backward = |offset| line_motion_target(&text, offset, LineMotion::Paragraph(false), 1);
        assert_eq!(forward(0), Some(4));
        assert_eq!(forward(4), Some(9));
        assert_eq!(forward(9), Some(10));
        assert_eq!(backward(10), Some(9));
        assert_eq!(backward(9), Some(4));
        assert_eq!(backward(4), Some(0));
    }

    #[test]
    fn visual_i_and_a_wait_for_a_text_object() {
        for mode in [VimMode::Visual, VimMode::VisualLine, VimMode::VisualBlock] {
            assert_eq!(
                command_for(mode, key("i")),
                Some(VimCommand::PendingTextObject(false))
            );
            assert_eq!(
                command_for(mode, key("a")),
                Some(VimCommand::PendingTextObject(true))
            );
        }
        assert_eq!(
            command_for(VimMode::Normal, key("i")),
            Some(VimCommand::EnterInsert)
        );
        assert_eq!(
            TextObject::from_char('b'),
            Some(TextObject::Pair {
                open: '(',
                close: ')'
            })
        );
        assert_eq!(TextObject::from_char('z'), None);
    }

    #[test]
    fn find_targets_stay_on_the_line_and_on_characters() {
        let text = Rope::from("é,中,x\n,");
        let forward = FindKind {
            forward: true,
            till: false,
        };
        let comma = |offset, kind, count, repeat| {
            line_motion_target(
                &text,
                offset,
                LineMotion::Find {
                    kind,
                    target: ',',
                    repeat,
                },
                count,
            )
        };

        assert_eq!(comma(0, forward, 1, false), Some(2));
        assert_eq!(comma(0, forward, 2, false), Some(6));
        assert_eq!(comma(0, forward, 3, false), None, "never the next line");
        let till = FindKind {
            forward: true,
            till: true,
        };
        assert_eq!(comma(0, till, 1, false), Some(0));
        assert_eq!(comma(0, till, 1, true), Some(3));
        assert_eq!(comma(7, forward.reversed(), 1, false), Some(6));
        assert_eq!(
            line_motion_operator_range(
                &text,
                0,
                LineMotion::Find {
                    kind: forward,
                    target: ',',
                    repeat: false,
                },
                1,
            ),
            Some(0..3),
            "an inclusive find covers its target"
        );
    }

    #[test]
    fn shifted_keys_are_not_their_lowercase_commands() {
        for name in ["h", "k", "l", "x", "u", "enter", "tab"] {
            assert_eq!(command_for(VimMode::Normal, shifted(name)), None, "{name}");
        }
        for (name, command) in [
            ("d", VimCommand::DeleteToEnd),
            ("c", VimCommand::ChangeToEnd),
            ("s", VimCommand::SubstituteLine),
            ("j", VimCommand::JoinLines),
        ] {
            assert_eq!(command_for(VimMode::Normal, shifted(name)), Some(command));
            assert_eq!(command_for(VimMode::Visual, shifted(name)), None, "{name}");
        }
    }

    #[test]
    fn keys_with_command_modifiers_always_pass_through() {
        for mode in [
            VimMode::Normal,
            VimMode::Insert,
            VimMode::Replace,
            VimMode::Visual,
            VimMode::VisualLine,
            VimMode::VisualBlock,
        ] {
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
    fn insert_and_replace_modes_only_claim_escape() {
        for mode in [VimMode::Insert, VimMode::Replace] {
            assert_eq!(
                command_for(mode, key("escape")),
                Some(VimCommand::LeaveInsert)
            );

            for name in ["h", "j", "x", "u", "i", "r", "enter", "tab", "backspace"] {
                assert_eq!(command_for(mode, key(name)), None, "{mode:?} {name}");
            }

            assert_eq!(command_for(mode, shifted("escape")), None);
            assert_eq!(command_for(mode, shifted("r")), None);
            assert!(mode.accepts_text());
        }

        assert_eq!(
            command_for(VimMode::Normal, shifted("r")),
            Some(VimCommand::EnterReplace)
        );
        assert_eq!(command_for(VimMode::Visual, shifted("r")), None);
        assert_eq!(
            mode_after(VimMode::Replace, VimCommand::LeaveInsert),
            VimMode::Normal
        );
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
    fn block_rows_skip_short_rows_and_use_scalar_columns() {
        let text = Rope::from("abcdef\r\nab\r\n\r\na中cdef");
        let rows = block_rows(&text, 2, 19);
        assert_eq!(rows, vec![(0, 2..4), (1, 10..10), (3, 18..20)]);
        assert_eq!(block_rows(&text, 19, 2), rows);
        assert_eq!(line_content_len(&text, 0), 6);
        assert_eq!(line_content_len(&text, 2), 0);
        assert_eq!(block_rows(&Rope::from(""), 0, 0), vec![(0, 0..0)]);
    }

    #[test]
    fn replace_line_break_follows_line_terminator_and_indent() {
        for (content, offset, expected) in [
            ("\t x", 2, "\n\t "),
            ("ab\r\ncd", 0, "\r\n"),
            ("ab\ncd", 4, "\n"),
            ("ab\r\ncd", 5, "\r\n"),
            ("ab", 1, "\n"),
        ] {
            let text = Rope::from(content);
            assert_eq!(
                line_break_with_indent(&text, offset),
                expected,
                "{content:?}"
            );
        }
    }

    #[test]
    fn clamping_pulls_a_line_end_cursor_onto_the_last_character() {
        let text = Rope::from("abc\nde");

        assert_eq!(clamp_to_character(&text, 3), 2);
        assert_eq!(clamp_to_character(&text, 6), 5);
        assert_eq!(clamp_to_character(&text, 1), 1);
    }
}
