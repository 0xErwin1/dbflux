//! A line-based diff of the editor text against the text Save compares with,
//! for the gutter's change markers and the footer's changed-line count.
//!
//! Only which lines changed leaves this module: no line text is kept or
//! returned.

/// Tables larger than this many cells fall back to comparing lines by
/// position, so a very large value never stalls a keystroke.
const MAX_TABLE_CELLS: usize = 2_000_000;

/// Which lines of the edited text differ from the baseline.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct LineChanges {
    /// One entry per line of the edited text; `true` when the line is not
    /// part of the baseline.
    pub(crate) marked: Vec<bool>,

    /// Changed lines, counted per block of consecutive differences: a
    /// replaced line counts once, and so does a deleted one, which has no
    /// line left to mark. Separate blocks add up.
    changed: usize,
}

impl LineChanges {
    /// A text with `lines` lines and nothing changed.
    pub(crate) fn unchanged(lines: usize) -> Self {
        Self {
            marked: vec![false; lines],
            changed: 0,
        }
    }

    pub(crate) fn changed(&self) -> usize {
        self.changed
    }
}

/// Compares `text` with `baseline` line by line, keeping the longest run of
/// lines they share, so an inserted line marks itself and not every line
/// after it.
pub(crate) fn changed_lines(baseline: &str, text: &str) -> LineChanges {
    let old: Vec<&str> = baseline.split('\n').collect();
    let new: Vec<&str> = text.split('\n').collect();

    let prefix = old
        .iter()
        .zip(&new)
        .take_while(|(left, right)| left == right)
        .count();
    let suffix = old
        .iter()
        .skip(prefix)
        .rev()
        .zip(new.iter().skip(prefix).rev())
        .take_while(|(left, right)| left == right)
        .count();

    let old_middle: Vec<&str> = old
        .iter()
        .skip(prefix)
        .take(old.len().saturating_sub(prefix + suffix))
        .copied()
        .collect();
    let new_middle: Vec<&str> = new
        .iter()
        .skip(prefix)
        .take(new.len().saturating_sub(prefix + suffix))
        .copied()
        .collect();

    let pairs = if old_middle.len() * new_middle.len() <= MAX_TABLE_CELLS {
        shared_lines(&old_middle, &new_middle)
    } else {
        positional(&old_middle, &new_middle)
    };

    let mut marked = vec![true; new.len()];

    for (index, slot) in marked.iter_mut().enumerate() {
        if index < prefix || index >= new.len() - suffix {
            *slot = false;
        }
    }

    for (_, new_index) in &pairs {
        if let Some(slot) = marked.get_mut(prefix + new_index) {
            *slot = false;
        }
    }

    let mut changed = 0;
    let (mut next_old, mut next_new) = (0, 0);

    for (old_index, new_index) in pairs
        .iter()
        .copied()
        .chain([(old_middle.len(), new_middle.len())])
    {
        changed += (old_index - next_old).max(new_index - next_new);
        next_old = old_index + 1;
        next_new = new_index + 1;
    }

    LineChanges { marked, changed }
}

/// The `(old, new)` index pairs of the longest common subsequence.
fn shared_lines(old: &[&str], new: &[&str]) -> Vec<(usize, usize)> {
    let width = new.len() + 1;
    let mut table = vec![0u32; (old.len() + 1) * width];
    let cell = |table: &[u32], row: usize, column: usize| {
        table.get(row * width + column).copied().unwrap_or(0)
    };

    for (row, old_line) in old.iter().enumerate().rev() {
        for (column, new_line) in new.iter().enumerate().rev() {
            let value = if old_line == new_line {
                cell(&table, row + 1, column + 1) + 1
            } else {
                cell(&table, row + 1, column).max(cell(&table, row, column + 1))
            };

            if let Some(slot) = table.get_mut(row * width + column) {
                *slot = value;
            }
        }
    }

    let mut pairs = Vec::new();
    let (mut row, mut column) = (0, 0);

    while let (Some(old_line), Some(new_line)) = (old.get(row), new.get(column)) {
        if old_line == new_line {
            pairs.push((row, column));
            row += 1;
            column += 1;
        } else if cell(&table, row + 1, column) >= cell(&table, row, column + 1) {
            row += 1;
        } else {
            column += 1;
        }
    }

    pairs
}

fn positional(old: &[&str], new: &[&str]) -> Vec<(usize, usize)> {
    new.iter()
        .enumerate()
        .filter(|(index, line)| old.get(*index) == Some(line))
        .map(|(index, _)| (index, index))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marked(baseline: &str, text: &str) -> Vec<usize> {
        changed_lines(baseline, text)
            .marked
            .iter()
            .enumerate()
            .filter_map(|(index, marked)| marked.then_some(index))
            .collect()
    }

    #[test]
    fn an_untouched_text_has_no_changes() {
        let changes = changed_lines("a\nb", "a\nb");
        assert_eq!(changes.changed(), 0);
        assert_eq!(marked("a\nb", "a\nb"), Vec::<usize>::new());
    }

    #[test]
    fn a_replaced_line_counts_once() {
        assert_eq!(marked("a\nb\nc", "a\nX\nc"), vec![1]);
        assert_eq!(changed_lines("a\nb\nc", "a\nX\nc").changed(), 1);
    }

    #[test]
    fn an_inserted_line_marks_only_itself() {
        assert_eq!(marked("a\nb\nc", "a\nNEW\nb\nc"), vec![1]);
        assert_eq!(changed_lines("a\nb\nc", "a\nNEW\nb\nc").changed(), 1);
    }

    #[test]
    fn a_deleted_line_counts_without_a_marker() {
        let changes = changed_lines("a\nb\nc", "a\nc");
        assert_eq!(changes.marked, vec![false, false]);
        assert_eq!(changes.changed(), 1);
    }

    #[test]
    fn separated_edits_count_independently() {
        let changes = changed_lines("drop\nkeep\nstay", "keep\nstay\nadd");
        assert_eq!(changes.changed(), 2);
        assert_eq!(changes.marked, vec![false, false, true]);
    }

    #[test]
    fn a_replacement_inside_a_block_counts_once() {
        assert_eq!(changed_lines("a\nb\nc\nd", "a\nX\nY\nd").changed(), 2);
        assert_eq!(changed_lines("a\nb\nc\nd", "a\nX\nd").changed(), 2);
        assert_eq!(changed_lines("a\nb\nc\nd", "a\nX\nc\nY").changed(), 2);
    }

    #[test]
    fn a_huge_replacement_falls_back_to_position_without_stalling() {
        let old = (0..3000)
            .map(|i| format!("o{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let new = (0..3000)
            .map(|i| format!("n{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(changed_lines(&old, &new).changed(), 3000);
    }
}
