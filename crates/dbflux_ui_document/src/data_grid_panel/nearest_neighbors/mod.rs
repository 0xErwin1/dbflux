//! Nearest-neighbor view of a table with a vector facet: the rows closest to
//! an anchor row by one vector column, shown in the same grid with their
//! distance until the neighbor bar is dismissed.

mod bar;
mod present;

use super::{DataGridPanel, DataSource, TableReload};
use dbflux_components::components::data_table::selection::CellCoord;
use dbflux_components::controls::{
    Dropdown, DropdownItem, DropdownSelectionChanged, InputEvent, InputState,
};
use dbflux_core::{
    NearestNeighborRequest, ObjectFacet, QueryRequest, TaskKind, TaskTarget, Value, VectorDistance,
    VectorFacet,
};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::*;

pub(crate) use present::distance_column_label;

/// Number of neighbors read when a search starts.
pub(crate) const DEFAULT_NEIGHBOR_LIMIT: u32 = 10;

/// Most neighbors one search may read.
pub(crate) const MAX_NEIGHBOR_LIMIT: u32 = 1000;

/// The search behind the rows on screen, kept so the distance, the column
/// or K can change without picking the anchor row again.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NearestNeighborSearch {
    pub(crate) column: String,
    pub(crate) anchor: Value,
    /// Every value of the anchor row by column name, for the pinned row and
    /// for switching to another vector column.
    pub(crate) anchor_row: Vec<(String, Value)>,
    /// How the bar names the anchor: its key (`id = 2`) or its row number.
    pub(crate) anchor_label: String,
    pub(crate) exclude_primary_key: Vec<(String, Value)>,
    pub(crate) distance: VectorDistance,
    pub(crate) limit: u32,
}

/// The neighbor bar's own controls and the state of the search it shows.
pub(crate) struct NearestNeighborsUi {
    /// The search started last, while its answer has not landed.
    pub(crate) running: Option<NearestNeighborSearch>,
    /// Why the last search failed, until another one succeeds.
    pub(crate) error: Option<String>,
    pub(crate) k_input: Entity<InputState>,
    pub(crate) column_dropdown: Entity<Dropdown>,
    /// K the field must show at the next render.
    pending_k: Option<u32>,
}

impl NearestNeighborsUi {
    pub(crate) fn new(window: &mut Window, cx: &mut Context<DataGridPanel>) -> Self {
        let k_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx);
            state.set_value(DEFAULT_NEIGHBOR_LIMIT.to_string(), window, cx);
            state
        });

        cx.subscribe_in(
            &k_input,
            window,
            |this, _, event: &InputEvent, window, cx| match event {
                InputEvent::PressEnter { .. } => {
                    this.apply_neighbor_limit(window, cx);
                    this.focus_table(window, cx);
                }
                InputEvent::Blur => this.exit_edit_mode(window, cx),
                _ => {}
            },
        )
        .detach();

        let column_dropdown =
            cx.new(|_cx| Dropdown::new("nearest-neighbors-column").toolbar_style(true));

        cx.subscribe_in(
            &column_dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, _window, cx| {
                this.switch_neighbor_column(event.index, cx);
            },
        )
        .detach();

        Self {
            running: None,
            error: None,
            k_input,
            column_dropdown,
            pending_k: None,
        }
    }
}

pub(crate) fn distance_label(distance: VectorDistance) -> String {
    match distance {
        VectorDistance::L2 => dbflux_i18n::t!("document.data.grid.nearest.distance.l2"),
        VectorDistance::Cosine => dbflux_i18n::t!("document.data.grid.nearest.distance.cosine"),
        VectorDistance::InnerProduct => {
            dbflux_i18n::t!("document.data.grid.nearest.distance.inner_product")
        }
    }
}

fn next_distance(distance: VectorDistance) -> VectorDistance {
    let position = VectorDistance::ALL
        .iter()
        .position(|candidate| *candidate == distance)
        .unwrap_or(0);
    VectorDistance::ALL
        .get((position + 1) % VectorDistance::ALL.len())
        .copied()
        .unwrap_or_default()
}

/// Why "Find similar rows" cannot run from a row, as the row menu says it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SimilarRowsBlock {
    /// The anchor row has no value in the vector column.
    NoVector { column: String },
    /// Staged edits would be dropped with the rows on screen.
    PendingEdits { count: usize },
}

impl SimilarRowsBlock {
    /// The reason as the row menu's second line shows it, short enough not
    /// to be cut off.
    pub(crate) fn menu_reason(&self) -> String {
        match self {
            SimilarRowsBlock::NoVector { column } => dbflux_i18n::t!(
                "document.data.grid.nearest.menu_blocked.no_vector",
                column = column.as_str()
            ),
            SimilarRowsBlock::PendingEdits { count: 1 } => {
                dbflux_i18n::t!("document.data.grid.nearest.menu_blocked.pending_edits.one")
            }
            SimilarRowsBlock::PendingEdits { count } => dbflux_i18n::t!(
                "document.data.grid.nearest.menu_blocked.pending_edits.many",
                count = count
            ),
        }
    }

    pub(crate) fn reason(&self) -> String {
        match self {
            SimilarRowsBlock::NoVector { column } => dbflux_i18n::t!(
                "document.data.grid.nearest.error.no_vector",
                column = column.as_str()
            ),
            SimilarRowsBlock::PendingEdits { count: 1 } => {
                dbflux_i18n::t!("document.data.grid.nearest.error.pending_edits.one")
            }
            SimilarRowsBlock::PendingEdits { count } => dbflux_i18n::t!(
                "document.data.grid.nearest.error.pending_edits.many",
                count = count
            ),
        }
    }
}

impl DataGridPanel {
    /// Vector facets of the table on screen, from its cached details. Empty
    /// for other sources and until the details arrive (the panel fetches them
    /// on open for the primary key).
    pub(crate) fn vector_facets(&self, cx: &Context<Self>) -> Vec<VectorFacet> {
        let DataSource::Table { profile_id, .. } = &self.source else {
            return Vec::new();
        };
        let Some(details) = self.table_details_for(cx) else {
            return Vec::new();
        };
        let Some(connected) = self.app_state.read(cx).connections().get(profile_id) else {
            return Vec::new();
        };

        connected
            .connection
            .object_facets(details)
            .into_iter()
            .filter_map(|facet| match facet {
                ObjectFacet::Vector(vector) => Some(vector),
                _ => None,
            })
            .collect()
    }

    /// Whether the menus offer actions that build a table query from the row
    /// on screen (Filter, Order, Generate SQL, Copy as SQL).
    pub(crate) fn offers_table_query_actions(&self) -> bool {
        self.nearest_neighbors.is_none()
    }

    /// The search shown in the grid, if the rows are nearest neighbors.
    #[cfg(test)]
    pub(crate) fn nearest_neighbor_search(&self) -> Option<&NearestNeighborSearch> {
        self.nearest_neighbors.as_ref()
    }

    /// The search the neighbor bar describes: the one running, else the one
    /// shown. `None` while the grid shows the table page.
    pub(crate) fn neighbor_bar_search(&self) -> Option<&NearestNeighborSearch> {
        self.nearest
            .running
            .as_ref()
            .filter(|_| self.runner.is_primary_active())
            .or(self.nearest_neighbors.as_ref())
    }

    /// What the neighbor bar says the rows are similar to.
    pub(crate) fn nearest_neighbors_summary(&self) -> Option<String> {
        self.neighbor_bar_search().map(|search| {
            dbflux_i18n::t!(
                "document.data.grid.nearest.summary",
                anchor = search.anchor_label.as_str(),
                column = search.column.as_str()
            )
        })
    }

    /// The cell under the cursor, as (visual row, column).
    fn active_cell(&self, cx: &Context<Self>) -> Option<(usize, usize)> {
        let table_state = self.grid_table.table_state.as_ref()?;
        let active = table_state.read(cx).selection().active?;
        Some((active.row, active.col))
    }

    /// Whether "Find similar rows" applies from column `col`: the table has a
    /// vector column among the rows on screen.
    pub(crate) fn can_find_similar_rows(&self, col: usize, cx: &Context<Self>) -> bool {
        self.similar_rows_column(col, cx).is_some()
    }

    /// The vector columns of the table that the rows on screen carry, in
    /// facet order.
    fn vector_columns_on_screen(&self, cx: &Context<Self>) -> Vec<String> {
        self.vector_facets(cx)
            .into_iter()
            .map(|facet| facet.column)
            .filter(|column| self.result.columns.iter().any(|c| c.name == *column))
            .collect()
    }

    /// The vector column "Find similar rows" reads: the column at `col` when
    /// it is a vector column, the table's first one otherwise.
    pub(crate) fn similar_rows_column(&self, col: usize, cx: &Context<Self>) -> Option<String> {
        let vector_columns = self.vector_columns_on_screen(cx);
        let cursor_column = self
            .result
            .columns
            .get(col)
            .map(|column| column.name.clone());

        cursor_column
            .filter(|name| vector_columns.contains(name))
            .or_else(|| vector_columns.into_iter().next())
    }

    /// Why "Find similar rows" cannot run from `visual_row`, if it cannot.
    pub(crate) fn similar_rows_block(
        &self,
        visual_row: usize,
        col: usize,
        cx: &Context<Self>,
    ) -> Option<SimilarRowsBlock> {
        let column = self.similar_rows_column(col, cx)?;

        let pending = self.pending_change_count(cx);
        if pending > 0 {
            return Some(SimilarRowsBlock::PendingEdits { count: pending });
        }

        let row = self.collect_row_values(visual_row, cx);
        let anchor = self
            .result
            .columns
            .iter()
            .position(|c| c.name == column)
            .and_then(|index| row.get(index));

        match anchor {
            Some(value) if *value != Value::Null => None,
            _ => Some(SimilarRowsBlock::NoVector { column }),
        }
    }

    /// The row-menu line under "Find similar rows": the column and K it reads.
    pub(crate) fn similar_rows_menu_detail(
        &self,
        col: usize,
        cx: &Context<Self>,
    ) -> Option<String> {
        let column = self.similar_rows_column(col, cx)?;

        Some(dbflux_i18n::t!(
            "document.data.grid.nearest.menu_detail",
            column = column.as_str(),
            limit = self.neighbor_limit()
        ))
    }

    /// Changes staged in the grid: edited, inserted and deleted rows.
    fn pending_change_count(&self, cx: &Context<Self>) -> usize {
        self.grid_table
            .table_state
            .as_ref()
            .map(|table_state| {
                let buffer = table_state.read(cx).edit_buffer();
                buffer.dirty_row_count()
                    + buffer.pending_insert_rows().len()
                    + buffer.pending_delete_rows().len()
            })
            .unwrap_or(0)
    }

    /// K of the next search: the shown search's, else the default.
    fn neighbor_limit(&self) -> u32 {
        self.neighbor_bar_search()
            .map(|search| search.limit)
            .unwrap_or(DEFAULT_NEIGHBOR_LIMIT)
    }

    /// Shows the rows nearest to the row under the cursor
    /// (`Command::FindSimilarRows`). Returns false when the action does not
    /// apply.
    pub(crate) fn find_similar_rows(&mut self, cx: &mut Context<Self>) -> bool {
        let Some((row, col)) = self.active_cell(cx) else {
            return false;
        };
        self.find_similar_rows_at(row, col, cx)
    }

    /// Shows the rows nearest to the visual row `visual_row` (the row menu).
    pub(crate) fn find_similar_rows_at(
        &mut self,
        visual_row: usize,
        col: usize,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(column) = self.similar_rows_column(col, cx) else {
            return false;
        };

        // The neighbors replace the rows on screen, which would drop every
        // staged edit; the guard's warning says why nothing happened.
        if self.reload_blocked_by_pending_edits(cx) {
            return true;
        }

        // The visual row goes through the edit buffer's order, so a sorted or
        // edited grid still reads the row under the cursor.
        let row = self.collect_row_values(visual_row, cx);
        let anchor_row: Vec<(String, Value)> = self
            .result
            .columns
            .iter()
            .map(|c| c.name.clone())
            .zip(row)
            .collect();
        let value_of = |name: &str| {
            anchor_row
                .iter()
                .find(|(column, _)| column == name)
                .map(|(_, value)| value.clone())
        };

        let anchor = value_of(&column).unwrap_or(Value::Null);
        if anchor == Value::Null {
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    SimilarRowsBlock::NoVector { column }.reason(),
                ),
                cx,
            );
            return true;
        }

        let exclude_primary_key: Option<Vec<(String, Value)>> = self
            .pk_columns
            .iter()
            .map(|name| value_of(name).map(|value| (name.clone(), value)))
            .collect();
        let exclude_primary_key = exclude_primary_key.unwrap_or_default();

        let anchor_label = if exclude_primary_key.is_empty() {
            dbflux_i18n::t!(
                "document.data.grid.nearest.anchor_row",
                row = visual_row + 1
            )
        } else {
            exclude_primary_key
                .iter()
                .map(|(name, value)| format!("{name} = {}", value.as_display_string_truncated(40)))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let search = NearestNeighborSearch {
            column,
            anchor,
            anchor_row,
            anchor_label,
            exclude_primary_key,
            distance: self
                .nearest_neighbors
                .as_ref()
                .map(|search| search.distance)
                .unwrap_or_default(),
            limit: self.neighbor_limit(),
        };

        self.run_nearest_neighbors(search, cx);
        true
    }

    /// Runs the search again with the next distance function.
    pub(crate) fn cycle_nearest_neighbor_distance(&mut self, cx: &mut Context<Self>) {
        let Some(search) = self.neighbor_bar_search() else {
            return;
        };
        let distance = next_distance(search.distance);
        self.set_nearest_neighbor_distance(distance, cx);
    }

    /// Runs the search again with `distance`.
    pub(crate) fn set_nearest_neighbor_distance(
        &mut self,
        distance: VectorDistance,
        cx: &mut Context<Self>,
    ) {
        let Some(search) = self.neighbor_bar_search().cloned() else {
            return;
        };
        if search.distance == distance {
            return;
        }

        self.run_nearest_neighbors(NearestNeighborSearch { distance, ..search }, cx);
    }

    /// Runs the shown search again (Refresh while the neighbors are shown).
    pub(crate) fn rerun_nearest_neighbors(&mut self, cx: &mut Context<Self>) -> bool {
        let Some(search) = self.nearest_neighbors.clone() else {
            return false;
        };

        self.run_nearest_neighbors(search, cx);
        true
    }

    /// Reads K from the bar and runs the search again with it. A value out of
    /// range is refused and the field goes back to the K shown.
    fn apply_neighbor_limit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(search) = self.neighbor_bar_search().cloned() else {
            return;
        };

        let text = self.nearest.k_input.read(cx).value().trim().to_string();
        let limit = text
            .parse::<u32>()
            .ok()
            .filter(|limit| (1..=MAX_NEIGHBOR_LIMIT).contains(limit));

        let Some(limit) = limit else {
            self.nearest.k_input.update(cx, |input, cx| {
                input.set_value(search.limit.to_string(), window, cx);
            });
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!(
                        "document.data.grid.nearest.error.invalid_k",
                        max = MAX_NEIGHBOR_LIMIT
                    ),
                ),
                cx,
            );
            return;
        };

        if limit != search.limit {
            self.run_nearest_neighbors(NearestNeighborSearch { limit, ..search }, cx);
        }
    }

    /// Runs the search again on the vector column at `index` of the bar's
    /// column list, from the same anchor row.
    fn switch_neighbor_column(&mut self, index: usize, cx: &mut Context<Self>) {
        let Some(search) = self.neighbor_bar_search().cloned() else {
            return;
        };
        let Some(column) = self
            .vector_columns_on_screen(cx)
            .get(index)
            .cloned()
            .or_else(|| {
                self.vector_facets(cx)
                    .get(index)
                    .map(|facet| facet.column.clone())
            })
        else {
            return;
        };
        if column == search.column {
            return;
        }

        let anchor = search
            .anchor_row
            .iter()
            .find(|(name, _)| *name == column)
            .map(|(_, value)| value.clone())
            .unwrap_or(Value::Null);

        if anchor == Value::Null {
            let reason = SimilarRowsBlock::NoVector { column }.reason();
            self.nearest.error = Some(reason.clone());
            self.sync_neighbor_controls(&search, cx);
            report_error(UserFacingError::new(ErrorKind::User, reason), cx);
            cx.notify();
            return;
        }

        self.run_nearest_neighbors(
            NearestNeighborSearch {
                column,
                anchor,
                ..search
            },
            cx,
        );
    }

    /// Leaves the neighbors and reads the table page again, with the filter
    /// and limit it had.
    pub(crate) fn clear_nearest_neighbors(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let shown = self.nearest_neighbors.take().is_some();
        let running = self.nearest.running.take().is_some();
        self.nearest.error = None;

        if !shown && !running {
            return;
        }

        self.grid_table.reload = TableReload::NewColumns;
        self.refresh(window, cx);
        cx.notify();
    }

    /// Points the bar's K field and column list at `search`.
    fn sync_neighbor_controls(&mut self, search: &NearestNeighborSearch, cx: &mut Context<Self>) {
        let columns = {
            let on_screen = self.vector_columns_on_screen(cx);
            if on_screen.is_empty() {
                self.vector_facets(cx)
                    .into_iter()
                    .map(|facet| facet.column)
                    .collect()
            } else {
                on_screen
            }
        };
        let selected = columns.iter().position(|column| *column == search.column);
        let items = columns.into_iter().map(DropdownItem::new).collect();

        self.nearest.column_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(items, cx);
            dropdown.set_selected_index(selected, cx);
        });

        // Setting the field needs the window, so render applies it.
        self.nearest.pending_k = Some(search.limit);
    }

    /// Applies the K that a search set while no window was at hand.
    pub(crate) fn apply_pending_neighbor_controls(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(limit) = self.nearest.pending_k.take() else {
            return;
        };

        let limit = limit.to_string();
        self.nearest.k_input.update(cx, |input, cx| {
            if input.value() != limit.as_str() {
                input.set_value(limit.clone(), window, cx);
            }
        });
    }

    fn run_nearest_neighbors(&mut self, search: NearestNeighborSearch, cx: &mut Context<Self>) {
        let DataSource::Table {
            profile_id,
            database,
            table,
            ..
        } = &self.source
        else {
            return;
        };
        let (profile_id, database, table) = (*profile_id, database.clone(), table.clone());

        let connection = {
            let state = self.app_state.read(cx);
            let Some(connected) = state.connections().get(&profile_id) else {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Network,
                        dbflux_i18n::t!("document.data.grid.error.connection_not_found"),
                    ),
                    cx,
                );
                return;
            };

            match connected.resolve_connection_for_execution(database.as_deref()) {
                Ok(connection) => connection,
                Err(dbflux_core::ConnectionResolutionError::PendingDatabaseConnection {
                    database,
                }) => {
                    report_error(
                        UserFacingError::new(
                            ErrorKind::Network,
                            format!(
                                "No connection to database '{database}'. Please expand it in the sidebar first."
                            ),
                        ),
                        cx,
                    );
                    return;
                }
            }
        };

        let request = NearestNeighborRequest {
            table: table.clone(),
            database: database.clone(),
            column: search.column.clone(),
            anchor: search.anchor.clone(),
            distance: search.distance,
            limit: search.limit,
            exclude_primary_key: search.exclude_primary_key.clone(),
        };

        let generated = match connection.query_generator() {
            Some(generator) => generator
                .generate_nearest_neighbors(&request)
                .map_err(|error| error.to_string()),
            None => Err(dbflux_core::GeneratorError::Unsupported(
                "this connection has no query generator".to_string(),
            )
            .to_string()),
        };
        let generated = match generated {
            Ok(generated) => generated,
            Err(error) => {
                self.nearest.error = Some(error.clone());
                report_error(
                    UserFacingError::new(
                        ErrorKind::Driver,
                        dbflux_i18n::t!("document.data.grid.nearest.error.failed", error = error),
                    ),
                    cx,
                );
                cx.notify();
                return;
            }
        };

        let (task_id, cancel_token) = self.runner.start_primary_for_target(
            TaskKind::Query,
            generated.text.clone(),
            Some(TaskTarget {
                profile_id,
                database: database.clone(),
            }),
            cx,
        );

        self.nearest.running = Some(search.clone());
        self.sync_neighbor_controls(&search, cx);
        cx.notify();

        let query = QueryRequest::new(generated.text).with_database(database);
        let connection_for_cleanup = connection.clone();
        let task = cx
            .background_executor()
            .spawn(async move { connection.execute(&query) });

        let entity = cx.entity().clone();
        cx.spawn(async move |_this, cx| {
            let result = task.await;

            let failure = cx.update(|cx| {
                // A refresh or another search started since: its answer is
                // the one the grid shows.
                if cancel_token.is_cancelled() {
                    log::info!("Nearest-neighbor search was superseded, discarding result");
                    if let Err(error) = connection_for_cleanup.cleanup_after_cancel() {
                        log::warn!("Cleanup after cancel failed: {}", error);
                    }
                    return None;
                }

                entity.update(cx, |panel, cx| match result {
                    Ok(result) => {
                        panel.runner.complete_primary(task_id, cx);
                        panel.show_neighbors(result, search, cx);
                        None
                    }
                    Err(error) => {
                        panel.runner.fail_primary(task_id, error.to_string(), cx);
                        panel.nearest.running = None;
                        panel.nearest.error = Some(error.to_string());
                        if let Some(shown) = panel.nearest_neighbors.clone() {
                            panel.sync_neighbor_controls(&shown, cx);
                        }
                        cx.notify();
                        Some(UserFacingError::new(
                            ErrorKind::Driver,
                            dbflux_i18n::t!(
                                "document.data.grid.nearest.error.failed",
                                error = error.to_string()
                            ),
                        ))
                    }
                })
            });

            if let Some(failure) = failure {
                report_error_async(failure, cx);
            }
        })
        .detach();
    }

    /// Puts the neighbors on screen read-only, the anchor row first, with the
    /// cursor on the anchor.
    fn show_neighbors(
        &mut self,
        result: dbflux_core::QueryResult,
        search: NearestNeighborSearch,
        cx: &mut Context<Self>,
    ) {
        let shown = present::present_neighbors(result, &search.anchor_row, search.distance);
        let column = self
            .grid_table
            .table_state
            .as_ref()
            .and_then(|table_state| table_state.read(cx).selection().active)
            .map(|active| active.col)
            .unwrap_or(0);

        self.grid_table.reload = TableReload::NewColumns;
        self.set_result(shown, cx);
        self.nearest.running = None;
        self.nearest.error = None;
        self.nearest_neighbors = Some(search);
        // `set_result` built the table while no search was recorded, so build
        // it again read-only.
        self.rebuild_table(None, cx);

        if let Some(table_state) = &self.grid_table.table_state {
            let column = column.min(self.result.columns.len().saturating_sub(1));
            table_state.update(cx, |state, cx| {
                state.select_cell(CellCoord::new(0, column), cx);
            });
        }

        cx.notify();
    }
}

#[cfg(test)]
mod tests;
