use super::*;

impl QueryBuilderPanel {
    /// Enables or disables "all columns (*)" mode.
    ///
    /// Disabling preserves the projection rows that were active before all-
    /// columns was toggled on; if none are saved, it defaults to no columns
    /// (the user must add them).
    pub fn set_all_columns(&mut self, all: bool, cx: &mut Context<Self>) {
        self.projection_mode = if all {
            ProjectionMode::All
        } else {
            ProjectionMode::Explicit
        };
        self.rebuild_spec_and_notify(cx);
    }

    /// Adds a column to the explicit projection list.
    ///
    /// No-op if the same `(source_alias, column)` pair already exists.
    pub fn add_column(&mut self, source_alias: &str, column: &str, cx: &mut Context<Self>) {
        let already_present = self
            .projection_rows
            .iter()
            .any(|r| r.source_alias == source_alias && r.column == column);

        if already_present {
            return;
        }

        self.projection_rows.push(ProjectionRow {
            source_alias: source_alias.to_string(),
            column: column.to_string(),
            alias: None,
        });

        if self.projection_mode == ProjectionMode::All {
            self.projection_mode = ProjectionMode::Explicit;
        }

        self.rebuild_spec_and_notify(cx);
    }

    /// Removes a column from the explicit projection list by its index.
    pub fn remove_column(&mut self, index: usize, cx: &mut Context<Self>) {
        if index < self.projection_rows.len() {
            self.projection_rows.remove(index);
            self.rebuild_spec_and_notify(cx);
        }
    }

    /// Moves the column at `from_index` to `to_index`.
    pub fn reorder_column(&mut self, from_index: usize, to_index: usize, cx: &mut Context<Self>) {
        if from_index < self.projection_rows.len() && to_index < self.projection_rows.len() {
            let row = self.projection_rows.remove(from_index);
            self.projection_rows.insert(to_index, row);
            self.rebuild_spec_and_notify(cx);
        }
    }

    // -----------------------------------------------------------------------
    // Filter mutations
    // -----------------------------------------------------------------------

    /// Returns true when an explicit projection entry exists matching the
    /// given `(alias, column)` pair.
    pub fn is_column_selected(&self, alias: &str, column: &str) -> bool {
        self.projection_rows
            .iter()
            .any(|r| r.source_alias == alias && r.column == column)
    }

    /// Adds or removes a column from the explicit projection. Used by the
    /// per-column checklist so the user can toggle without typing.
    pub fn toggle_column(&mut self, alias: &str, column: &str, cx: &mut Context<Self>) {
        match self
            .projection_rows
            .iter()
            .position(|r| r.source_alias == alias && r.column == column)
        {
            Some(idx) => self.remove_column(idx, cx),
            None => self.add_column(alias, column, cx),
        }
    }
}
