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

    /// Baseline lines the edit no longer holds.
    pub(crate) removed: usize,
}

impl LineChanges {
    /// The number of lines the footer reports: a replaced line counts once,
    /// and so does a deleted one, which has no line left to mark.
    pub(crate) fn changed(&self) -> usize {
        self.marked
            .iter()
            .filter(|marked| **marked)
            .count()
            .max(self.removed)
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

    let kept = if old_middle.len() * new_middle.len() <= MAX_TABLE_CELLS {
        shared_lines(&old_middle, &new_middle)
    } else {
        positional(&old_middle, &new_middle)
    };

    let mut marked = vec![false; new.len()];

    for (offset, line_kept) in kept.iter().enumerate() {
        if let Some(slot) = marked.get_mut(prefix + offset) {
            *slot = !line_kept;
        }
    }

    let kept_count = kept.iter().filter(|kept| **kept).count();

    LineChanges {
        marked,
        removed: old_middle.len().saturating_sub(kept_count),
    }
}

/// For each line of `new`, whether it belongs to the longest common
/// subsequence with `old`.
fn shared_lines(old: &[&str], new: &[&str]) -> Vec<bool> {
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

    let mut kept = vec![false; new.len()];
    let (mut row, mut column) = (0, 0);

    while let (Some(old_line), Some(new_line)) = (old.get(row), new.get(column)) {
        if old_line == new_line {
            if let Some(slot) = kept.get_mut(column) {
                *slot = true;
            }
            row += 1;
            column += 1;
        } else if cell(&table, row + 1, column) >= cell(&table, row, column + 1) {
            row += 1;
        } else {
            column += 1;
        }
    }

    kept
}

fn positional(old: &[&str], new: &[&str]) -> Vec<bool> {
    new.iter()
        .enumerate()
        .map(|(index, line)| old.get(index) == Some(line))
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
