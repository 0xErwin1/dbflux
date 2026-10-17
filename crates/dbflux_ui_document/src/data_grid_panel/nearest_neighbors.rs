//! Nearest-neighbor view of a table with a vector facet: the rows closest to
//! the active row by one vector column, shown in the same grid with their
//! distance until the notice is dismissed.

use super::{DataGridPanel, DataSource};
use dbflux_core::{
    NearestNeighborRequest, ObjectFacet, QueryRequest, TaskKind, TaskTarget, Value, VectorDistance,
    VectorFacet,
};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::*;

/// Number of neighbors read.
pub(crate) const DEFAULT_NEIGHBOR_LIMIT: u32 = 10;

/// The search behind the rows on screen, kept so the distance can change
/// without picking the anchor row again.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NearestNeighborSearch {
    pub(crate) column: String,
    pub(crate) anchor: Value,
    pub(crate) exclude_primary_key: Vec<(String, Value)>,
    pub(crate) distance: VectorDistance,
    pub(crate) limit: u32,
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

    /// The search shown in the grid, if the rows are nearest neighbors.
    pub(crate) fn nearest_neighbor_search(&self) -> Option<&NearestNeighborSearch> {
        self.nearest_neighbors.as_ref()
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

    /// The vector column "Find similar rows" reads: the column at `col` when
    /// it is a vector column, the table's first one otherwise.
    fn similar_rows_column(&self, col: usize, cx: &Context<Self>) -> Option<String> {
        let vector_columns: Vec<String> = self
            .vector_facets(cx)
            .into_iter()
            .map(|facet| facet.column)
            .filter(|column| self.result.columns.iter().any(|c| c.name == *column))
            .collect();

        let cursor_column = self
            .result
            .columns
            .get(col)
            .map(|column| column.name.clone());
        cursor_column
            .filter(|name| vector_columns.contains(name))
            .or_else(|| vector_columns.into_iter().next())
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

        // The visual row goes through the edit buffer's order, so a sorted or
        // edited grid still reads the row under the cursor.
        let row = self.collect_row_values(visual_row, cx);
        let value_of = |name: &str| {
            self.result
                .columns
                .iter()
                .position(|c| c.name == name)
                .and_then(|index| row.get(index).cloned())
        };

        let anchor = value_of(&column).unwrap_or(Value::Null);
        if anchor == Value::Null {
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!(
                        "document.data.grid.nearest.error.no_vector",
                        column = column
                    ),
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

        let search = NearestNeighborSearch {
            column,
            anchor,
            exclude_primary_key: exclude_primary_key.unwrap_or_default(),
            distance: self
                .nearest_neighbors
                .as_ref()
                .map(|search| search.distance)
                .unwrap_or_default(),
            limit: DEFAULT_NEIGHBOR_LIMIT,
        };

        self.run_nearest_neighbors(search, cx);
        true
    }

    /// Runs the search again with the next distance function.
    pub(crate) fn cycle_nearest_neighbor_distance(&mut self, cx: &mut Context<Self>) {
        let Some(search) = self.nearest_neighbors.clone() else {
            return;
        };

        self.run_nearest_neighbors(
            NearestNeighborSearch {
                distance: next_distance(search.distance),
                ..search
            },
            cx,
        );
    }

    /// Leaves the neighbors and reads the table page again.
    pub(crate) fn clear_nearest_neighbors(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.nearest_neighbors.take().is_none() {
            return;
        }

        self.refresh(window, cx);
        cx.notify();
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
                report_error(
                    UserFacingError::new(
                        ErrorKind::Driver,
                        dbflux_i18n::t!("document.data.grid.nearest.error.failed", error = error),
                    ),
                    cx,
                );
                return;
            }
        };

        let (task_id, _cancel_token) = self.runner.start_primary_for_target(
            TaskKind::Query,
            generated.text.clone(),
            Some(TaskTarget {
                profile_id,
                database: database.clone(),
            }),
            cx,
        );

        let query = QueryRequest::new(generated.text).with_database(database);
        let task = cx
            .background_executor()
            .spawn(async move { connection.execute(&query) });

        let entity = cx.entity().clone();
        cx.spawn(async move |_this, cx| {
            let result = task.await;

            let failure = cx.update(|cx| {
                entity.update(cx, |panel, cx| match result {
                    Ok(result) => {
                        panel.runner.complete_primary(task_id, cx);
                        panel.set_result(result, cx);
                        panel.nearest_neighbors = Some(search);
                        // `set_result` built the table while no search was
                        // recorded, so build it again read-only.
                        panel.rebuild_table(None, cx);
                        cx.notify();
                        None
                    }
                    Err(error) => {
                        panel.runner.fail_primary(task_id, error.to_string(), cx);
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

    /// The notice above the neighbors: what they are nearest to, the
    /// distance (pressing it switches to the next one) and the way back.
    pub(super) fn render_nearest_neighbors_notice(
        &self,
        search: &NearestNeighborSearch,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        use dbflux_components::controls::Button;
        use dbflux_components::icons::AppIcon;
        use dbflux_components::primitives::{Icon, Text};
        use dbflux_components::tokens::{Fields, Spacing};

        div()
            .id("nearest-neighbors-notice")
            .flex()
            .flex_1()
            .min_w(px(0.0))
            .items_center()
            .gap(Spacing::SM)
            .child(
                Icon::new(AppIcon::Search)
                    .size(Fields::FILTER_ICON)
                    .color(theme.muted_foreground),
            )
            .child(
                div().min_w(px(0.0)).truncate().child(
                    Text::body_sm(dbflux_i18n::t!(
                        "document.data.grid.nearest.notice",
                        column = search.column.as_str(),
                        limit = search.limit
                    ))
                    .color(theme.muted_foreground),
                ),
            )
            .child(
                Button::new(
                    "nearest-neighbors-distance",
                    distance_label(search.distance),
                )
                .ghost()
                .inline()
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.cycle_nearest_neighbor_distance(cx);
                })),
            )
            .child(
                Button::new(
                    "nearest-neighbors-clear",
                    dbflux_i18n::t!("document.data.grid.nearest.back_to_table"),
                )
                .ghost()
                .inline()
                .icon(AppIcon::X)
                .tab_stop(false)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.clear_nearest_neighbors(window, cx);
                })),
            )
    }
}

#[cfg(test)]
#[path = "nearest_neighbors_tests.rs"]
mod tests;
