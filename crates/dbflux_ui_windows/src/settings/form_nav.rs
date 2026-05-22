/// 2D grid navigation for settings forms.
///
/// Holds only the focused field; callers pass `&[Vec<F>]` rows on each
/// operation so the grid can vary dynamically (auth mode, editing state).
#[derive(Clone)]
pub(super) struct FormGridNav<F> {
    pub(super) focused: F,
}

impl<F: Copy + PartialEq> FormGridNav<F> {
    pub(super) fn new(initial: F) -> Self {
        Self { focused: initial }
    }

    fn position(&self, rows: &[Vec<F>]) -> Option<(usize, usize)> {
        for (row_idx, row) in rows.iter().enumerate() {
            if let Some(col_idx) = row.iter().position(|f| *f == self.focused) {
                return Some((row_idx, col_idx));
            }
        }
        None
    }

    #[allow(dead_code)]
    pub(super) fn move_down(&mut self, rows: &[Vec<F>]) {
        if let Some((row_idx, col_idx)) = self.position(rows)
            && row_idx + 1 < rows.len()
        {
            let next_row = &rows[row_idx + 1];
            if next_row.is_empty() {
                return;
            }
            self.focused = next_row[col_idx.min(next_row.len() - 1)];
        }
    }

    #[allow(dead_code)]
    pub(super) fn move_up(&mut self, rows: &[Vec<F>]) {
        if let Some((row_idx, col_idx)) = self.position(rows)
            && row_idx > 0
        {
            let prev_row = &rows[row_idx - 1];
            if prev_row.is_empty() {
                return;
            }
            self.focused = prev_row[col_idx.min(prev_row.len() - 1)];
        }
    }

    #[allow(dead_code)]
    pub(super) fn move_right(&mut self, rows: &[Vec<F>]) {
        if let Some((row_idx, col_idx)) = self.position(rows) {
            let row = &rows[row_idx];
            if col_idx + 1 < row.len() {
                self.focused = row[col_idx + 1];
            }
        }
    }

    #[allow(dead_code)]
    pub(super) fn move_left(&mut self, rows: &[Vec<F>]) {
        if let Some((row_idx, col_idx)) = self.position(rows)
            && col_idx > 0
        {
            self.focused = rows[row_idx][col_idx - 1];
        }
    }

    #[allow(dead_code)]
    pub(super) fn move_first(&mut self, rows: &[Vec<F>]) {
        if let Some(first_row) = rows.first()
            && let Some(first_field) = first_row.first()
        {
            self.focused = *first_field;
        }
    }

    #[allow(dead_code)]
    pub(super) fn move_last(&mut self, rows: &[Vec<F>]) {
        if let Some(last_row) = rows.last()
            && let Some(last_field) = last_row.last()
        {
            self.focused = *last_field;
        }
    }

    pub(super) fn tab_next(&mut self, rows: &[Vec<F>]) {
        if let Some((row_idx, col_idx)) = self.position(rows) {
            let row = &rows[row_idx];
            if col_idx + 1 < row.len() {
                self.focused = row[col_idx + 1];
            } else if row_idx + 1 < rows.len() && !rows[row_idx + 1].is_empty() {
                self.focused = rows[row_idx + 1][0];
            }
        }
    }

    pub(super) fn tab_prev(&mut self, rows: &[Vec<F>]) {
        if let Some((row_idx, col_idx)) = self.position(rows) {
            if col_idx > 0 {
                self.focused = rows[row_idx][col_idx - 1];
            } else if row_idx > 0 {
                let prev_row = &rows[row_idx - 1];
                if let Some(last_field) = prev_row.last() {
                    self.focused = *last_field;
                }
            }
        }
    }

    /// Resets to `fallback` if the current field is not present in `rows`.
    pub(super) fn validate(&mut self, rows: &[Vec<F>], fallback: F) {
        let is_valid = rows.iter().any(|row| row.contains(&self.focused));
        if !is_valid {
            self.focused = fallback;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FormGridNav;

    #[derive(Clone, Copy, PartialEq, Debug)]
    enum F {
        A,
        B,
        C,
        D,
        E,
        G,
    }

    fn grid() -> Vec<Vec<F>> {
        vec![vec![F::A], vec![F::B, F::C], vec![F::D, F::E, F::G]]
    }

    #[test]
    fn move_down_clamps_column() {
        let rows = grid();
        let mut nav = FormGridNav::new(F::G);
        nav.move_down(&rows);
        // Already at last row, no change
        assert_eq!(nav.focused, F::G);

        let mut nav = FormGridNav::new(F::A);
        nav.move_down(&rows);
        assert_eq!(nav.focused, F::B);
    }

    #[test]
    fn move_up_clamps_column() {
        let rows = grid();
        let mut nav = FormGridNav::new(F::G);
        nav.move_up(&rows);
        // col 2 clamped to col 1 in row with 2 items
        assert_eq!(nav.focused, F::C);
    }

    #[test]
    fn move_right_stops_at_end() {
        let rows = grid();
        let mut nav = FormGridNav::new(F::B);
        nav.move_right(&rows);
        assert_eq!(nav.focused, F::C);
        nav.move_right(&rows);
        assert_eq!(nav.focused, F::C);
    }

    #[test]
    fn move_left_stops_at_start() {
        let rows = grid();
        let mut nav = FormGridNav::new(F::C);
        nav.move_left(&rows);
        assert_eq!(nav.focused, F::B);
        nav.move_left(&rows);
        assert_eq!(nav.focused, F::B);
    }

    #[test]
    fn tab_next_crosses_rows() {
        let rows = grid();
        let mut nav = FormGridNav::new(F::C);
        nav.tab_next(&rows);
        assert_eq!(nav.focused, F::D);
    }

    #[test]
    fn tab_prev_crosses_rows() {
        let rows = grid();
        let mut nav = FormGridNav::new(F::D);
        nav.tab_prev(&rows);
        assert_eq!(nav.focused, F::C);
    }

    #[test]
    fn move_first_and_last() {
        let rows = grid();
        let mut nav = FormGridNav::new(F::D);
        nav.move_first(&rows);
        assert_eq!(nav.focused, F::A);
        nav.move_last(&rows);
        assert_eq!(nav.focused, F::G);
    }

    #[test]
    fn validate_resets_orphaned_field() {
        let rows = vec![vec![F::A], vec![F::B]];
        let mut nav = FormGridNav::new(F::G);
        nav.validate(&rows, F::A);
        assert_eq!(nav.focused, F::A);
    }

    #[test]
    fn validate_keeps_valid_field() {
        let rows = vec![vec![F::A], vec![F::B]];
        let mut nav = FormGridNav::new(F::B);
        nav.validate(&rows, F::A);
        assert_eq!(nav.focused, F::B);
    }
}
