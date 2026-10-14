//! Background resolution of foreign-key cell display values.
//!
//! [`DataGridPanel::resolve_fk_display_values`] runs after a page of a table
//! lands (and again when FK metadata arrives): for every single-column
//! foreign key of the browsed table it fetches the referenced rows through
//! `Connection::fetch_row_by_pk` — [`MAX_CONCURRENT_FK_LOOKUPS`] at a time —
//! and swaps the raw-id text cell for `CellValue::fk_reference`, which draws
//! the referenced row's display column beside the id while editing, copying
//! and mutations keep carrying the id.
//!
//! Ids already resolved live in [`FkDisplayLoader::cache`] and are applied
//! without a query; an id no row answered for is cached as `None` so it is
//! not fetched again. Fetches that land after another page replaced the rows
//! are dropped (see [`FkDisplayLoader::generation`]).

use super::*;
use dbflux_components::components::data_table::model::{CellKind, CellValue};
use dbflux_core::{ColumnInfo, SchemaForeignKeyInfo};

/// Most referenced rows fetched at the same time for one page.
const MAX_CONCURRENT_FK_LOOKUPS: usize = 4;

/// (referenced schema, referenced table, raw id text) of one foreign-key
/// cell.
type FkDisplayKey = (Option<String>, String, String);

/// One `fetch_row_by_pk` answer: the referenced row keyed by its columns,
/// or the error text when the lookup failed.
type FetchOutcome = Result<Option<HashMap<String, Value>>, String>;

/// A single-column foreign key of the browsed table, mapped onto the grid's
/// columns.
struct FkTarget {
    /// Index of the local column in the current result.
    col: usize,
    /// Schema of the referenced table, when the metadata names one.
    schema: Option<String>,
    table: String,
    /// Referenced column the local value matches.
    pk_column: String,
}

/// One referenced row to fetch.
struct FkLookup {
    key: FkDisplayKey,
    pk_column: String,
    value: Value,
}

/// Resolved display values of the grid's foreign-key cells.
#[derive(Default)]
pub(super) struct FkDisplayLoader {
    /// The round in flight; dropped when the next page lands.
    task: Option<Task<()>>,
    /// Advances on every round, so a fetch that lands late is not applied to
    /// the rows that replaced the ones it was started for.
    generation: u64,
    /// Resolved labels by (schema, table, id). `None` marks an id no row
    /// answered for, so it is not fetched again. Entries survive page turns
    /// of the same table: an id keeps naming the same referenced row.
    cache: HashMap<FkDisplayKey, Option<String>>,
}

/// Maps one schema foreign key onto the result's columns when it is
/// single-column and points at a named table and column.
fn single_column_target(fk: &SchemaForeignKeyInfo, columns: &[ColumnMeta]) -> Option<FkTarget> {
    let ([local], [referenced_column]) = (fk.columns.as_slice(), fk.referenced_columns.as_slice())
    else {
        return None;
    };

    if fk.referenced_table.is_empty() || referenced_column.is_empty() {
        return None;
    }

    let col = columns.iter().position(|column| column.name == *local)?;

    Some(FkTarget {
        col,
        schema: fk.referenced_schema.clone(),
        table: fk.referenced_table.clone(),
        pk_column: referenced_column.clone(),
    })
}

/// The id a foreign-key cell holds, as a query value. `None` for null and
/// the kinds an id column does not take.
fn fk_cell_value(cell: &CellValue) -> Option<Value> {
    match &cell.kind {
        CellKind::Int(id) => Some(Value::Int(*id)),
        CellKind::Float(id) => Some(Value::Float(*id)),
        CellKind::Text(id) | CellKind::Json(id) => Some(Value::Text(id.to_string())),
        _ => None,
    }
}

/// The label a referenced row's display value contributes; `None` for null.
fn display_label(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Text(label) => Some(label.clone()),
        other => Some(other.as_display_string_truncated(200)),
    }
}

/// The column a referenced row is named by: its first textual
/// non-primary-key column, else its first non-primary-key column.
fn pick_display_column(columns: &[ColumnInfo]) -> Option<String> {
    let is_textual = |column: &ColumnInfo| {
        let type_name = column.type_name.to_lowercase();
        type_name.contains("text") || type_name.contains("char")
    };

    columns
        .iter()
        .find(|column| !column.is_primary_key && is_textual(column))
        .or_else(|| columns.iter().find(|column| !column.is_primary_key))
        .map(|column| column.name.clone())
}

/// The model with every foreign-key cell whose id the cache resolved
/// showing `CellValue::fk_reference`, and every other cell unchanged.
/// `None` when no cell changed.
fn substitute_fk_references(
    model: &TableModel,
    targets: &[FkTarget],
    cache: &HashMap<FkDisplayKey, Option<String>>,
) -> Option<TableModel> {
    let mut substituted: Option<TableModel> = None;

    for (row_ix, row) in model.rows.iter().enumerate() {
        for target in targets {
            let Some(cell) = row.cells.get(target.col) else {
                continue;
            };

            if cell.fk_spans().is_some() {
                continue;
            }

            let raw_id = cell.edit_text();
            let key = (target.schema.clone(), target.table.clone(), raw_id.clone());
            let Some(label) = cache.get(&key).cloned().flatten() else {
                continue;
            };

            let new_model = substituted.get_or_insert_with(|| model.clone());
            if let Some(cell) = new_model
                .rows
                .get_mut(row_ix)
                .and_then(|row| row.cells.get_mut(target.col))
            {
                *cell = CellValue::fk_reference(&label, &raw_id);
            }
        }
    }

    substituted
}

impl DataGridPanel {
    /// The grid's single-column foreign keys, from the panel's FK cache.
    /// Empty for non-table sources and while the cache is not `Ready`.
    fn fk_display_targets(&self) -> Vec<FkTarget> {
        let DataSource::Table { table, .. } = &self.source else {
            return Vec::new();
        };

        let FkLoadState::Ready(foreign_keys) = &self.builder.fk_cache else {
            return Vec::new();
        };

        let source_table_lower = table.name.to_lowercase();

        foreign_keys
            .iter()
            .filter(|fk| fk.table_name.to_lowercase() == source_table_lower)
            .filter_map(|fk| single_column_target(fk, &self.result.columns))
            .collect()
    }

    /// Resolves the readable values behind the foreign-key cells of the
    /// current page: ids the cache already knows are applied at once, the
    /// distinct unresolved ones are fetched in the background and applied as
    /// they arrive. A no-op for non-table sources, missing FK metadata and
    /// connections without row lookups.
    pub(super) fn resolve_fk_display_values(&mut self, cx: &mut Context<Self>) {
        self.fk_display.generation += 1;
        let generation = self.fk_display.generation;

        let targets = self.fk_display_targets();
        if targets.is_empty() {
            return;
        }

        let Some(table_state) = self.grid_table.table_state.clone() else {
            return;
        };

        let (profile_id, database) = match &self.source {
            DataSource::Table {
                profile_id,
                database,
                ..
            } => (*profile_id, database.clone()),
            _ => return,
        };
        let Some(database) = database else {
            return;
        };

        let (connection, resolved_database) = {
            let Some(connected) = self.app_state.read(cx).connections().get(&profile_id) else {
                return;
            };

            let resolved = Self::table_details_database(connected, Some(database.as_str()));
            (connected.connection_for_database(&resolved), resolved)
        };

        let model = table_state.read(cx).model_arc().clone();

        // Ids the cache already knows land on the cells without a query.
        if let Some(new_model) = substitute_fk_references(&model, &targets, &self.fk_display.cache)
        {
            self.swap_table_model_keep_chrome(new_model, cx);
        }

        // One fetch per distinct unresolved id.
        let mut seen: HashSet<FkDisplayKey> = HashSet::new();
        let mut lookups: Vec<FkLookup> = Vec::new();
        for row in &model.rows {
            for target in &targets {
                let Some(cell) = row.cells.get(target.col) else {
                    continue;
                };

                let Some(value) = fk_cell_value(cell) else {
                    continue;
                };

                let key = (
                    target.schema.clone(),
                    target.table.clone(),
                    cell.edit_text(),
                );
                if self.fk_display.cache.contains_key(&key) || !seen.insert(key.clone()) {
                    continue;
                }

                lookups.push(FkLookup {
                    key,
                    pk_column: target.pk_column.clone(),
                    value,
                });
            }
        }

        if lookups.is_empty() {
            return;
        }

        // The display column of each referenced table, from the cached table
        // details — keyed by the referenced schema first and without one
        // second, the shape MySQL writes when it records the schema on the
        // foreign key but not on the table. Tables the cache cannot answer
        // are fetched below.
        let mut display_columns: HashMap<(Option<String>, String), Option<String>> = HashMap::new();
        let mut unknown_tables: Vec<(Option<String>, String)> = Vec::new();
        {
            let Some(connected) = self.app_state.read(cx).connections().get(&profile_id) else {
                return;
            };

            for target in &targets {
                let table_key = (target.schema.clone(), target.table.clone());
                if display_columns.contains_key(&table_key) {
                    continue;
                }

                let info = connected
                    .table_details
                    .get(&(
                        resolved_database.clone(),
                        target.schema.clone(),
                        target.table.clone(),
                    ))
                    .or_else(|| {
                        connected.table_details.get(&(
                            resolved_database.clone(),
                            None,
                            target.table.clone(),
                        ))
                    });

                let column = info
                    .and_then(|details| details.columns.as_deref())
                    .and_then(pick_display_column);

                if column.is_none() {
                    unknown_tables.push(table_key.clone());
                }

                display_columns.insert(table_key, column);
            }
        }

        let app_state = self.app_state.clone();

        self.fk_display.task = Some(cx.spawn(async move |this, cx| {
            let executor = cx.background_executor().clone();

            for (schema, table) in &unknown_tables {
                let connection = connection.clone();
                let database = resolved_database.clone();
                let schema_for_task = schema.clone();
                let table_for_task = table.clone();

                let details = executor
                    .spawn(async move {
                        connection.table_details(
                            &database,
                            schema_for_task.as_deref(),
                            &table_for_task,
                        )
                    })
                    .await;

                let column = match details {
                    Ok(details) => {
                        let column = details.columns.as_deref().and_then(pick_display_column);
                        cx.update(|cx| {
                            app_state.update(cx, |state, _| {
                                state.set_table_details(
                                    profile_id,
                                    resolved_database.clone(),
                                    schema.clone(),
                                    table.clone(),
                                    details,
                                );
                            });
                        });
                        column
                    }
                    Err(error) => {
                        log::warn!(
                            "fk display: no column metadata for referenced table {table}: {error}"
                        );
                        None
                    }
                };

                display_columns.insert((schema.clone(), table.clone()), column);
            }

            for group in lookups.chunks(MAX_CONCURRENT_FK_LOOKUPS) {
                let still_current = this
                    .update(cx, |panel, _| panel.fk_display.generation == generation)
                    .unwrap_or(false);
                if !still_current {
                    return;
                }

                let pending: Vec<Task<FetchOutcome>> = group
                    .iter()
                    .map(|lookup| {
                        let connection = connection.clone();
                        let database = resolved_database.clone();
                        let schema = lookup.key.0.clone();
                        let table = lookup.key.1.clone();
                        let pk_column = lookup.pk_column.clone();
                        let value = lookup.value.clone();

                        executor.spawn(async move {
                            connection
                                .fetch_row_by_pk(
                                    &database,
                                    schema.as_deref().unwrap_or(""),
                                    &table,
                                    &pk_column,
                                    &value,
                                )
                                .map_err(|error| error.to_string())
                        })
                    })
                    .collect();

                let mut resolved: Vec<(&FkLookup, FetchOutcome)> = Vec::new();
                for (lookup, task) in group.iter().zip(pending) {
                    resolved.push((lookup, task.await));
                }

                this.update(cx, |panel, cx| {
                    panel.apply_fk_display_batch(generation, resolved, &display_columns, cx);
                })
                .ok();
            }
        }));
    }

    /// Records a group of fetched referenced rows in the cache and, when
    /// they belong to the current round, swaps the resolved labels into the
    /// grid's model.
    fn apply_fk_display_batch(
        &mut self,
        generation: u64,
        resolved: Vec<(&FkLookup, FetchOutcome)>,
        display_columns: &HashMap<(Option<String>, String), Option<String>>,
        cx: &mut Context<Self>,
    ) {
        for (lookup, result) in resolved {
            let label = match result {
                Ok(Some(row)) => display_columns
                    .get(&(lookup.key.0.clone(), lookup.key.1.clone()))
                    .cloned()
                    .flatten()
                    .and_then(|column| row.get(&column))
                    .and_then(display_label),
                // No row answers this id: cache the miss so it is not
                // fetched again; the cell keeps showing the raw id.
                Ok(None) => None,
                Err(error) => {
                    // A failed lookup is transient, so the id stays uncached
                    // and the next page retries it.
                    log::debug!(
                        "fk display: could not fetch a row of {} for id {}: {error}",
                        lookup.key.1,
                        lookup.key.2
                    );
                    continue;
                }
            };

            self.fk_display.cache.insert(lookup.key.clone(), label);
        }

        if self.fk_display.generation != generation {
            return;
        }

        let targets = self.fk_display_targets();
        let Some(table_state) = self.grid_table.table_state.clone() else {
            return;
        };

        let model = table_state.read(cx).model_arc().clone();
        if let Some(new_model) = substitute_fk_references(&model, &targets, &self.fk_display.cache)
        {
            self.swap_table_model_keep_chrome(new_model, cx);
        }
    }

    /// Installs `model` in the table state, keeping the chrome a reload
    /// preserves (widths, sort, cursor) and re-arming the enum options
    /// `set_model` clears. Skipped while a cell is being edited or edits are
    /// staged, so a late resolution never drops them; the cached labels
    /// apply on the next round instead.
    fn swap_table_model_keep_chrome(&mut self, model: TableModel, cx: &mut Context<Self>) {
        let Some(table_state) = self.grid_table.table_state.clone() else {
            return;
        };

        if table_state.read(cx).is_editing() || table_state.read(cx).has_pending_operations() {
            return;
        }

        let column_details = self.get_column_details(cx);
        let enum_options = enum_options_for_result(&self.result, column_details.as_deref());

        table_state.update(cx, |state, cx| {
            state.set_model(Arc::new(model), ModelSwap::KeepCursor, cx);
            for (col_ix, options) in enum_options {
                state.set_enum_options(col_ix, options);
            }
        });

        cx.notify();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FkTarget, QueryResult, TableModel, Value, pick_display_column, substitute_fk_references,
    };
    use dbflux_core::{ColumnInfo, ColumnKind, ColumnMeta};
    use std::collections::HashMap;
    use std::time::Duration;

    fn column(name: &str, type_name: &str, is_primary_key: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            type_name: type_name.to_string(),
            nullable: true,
            is_primary_key,
            default_value: None,
            enum_values: None,
        }
    }

    fn result_column(name: &str) -> ColumnMeta {
        ColumnMeta {
            name: name.to_string(),
            type_name: "int4".to_string(),
            kind: ColumnKind::Integer,
            nullable: false,
            is_primary_key: name == "id",
        }
    }

    #[test]
    fn display_column_prefers_the_first_text_column_that_is_not_the_key() {
        let columns = [
            column("id", "int4", true),
            column("active", "bool", false),
            column("email", "varchar(255)", false),
            column("name", "text", false),
        ];

        assert_eq!(pick_display_column(&columns).as_deref(), Some("email"));
    }

    #[test]
    fn display_column_falls_back_to_the_first_non_key_column() {
        let columns = [
            column("id", "int4", true),
            column("amount", "numeric", false),
        ];

        assert_eq!(pick_display_column(&columns).as_deref(), Some("amount"));
    }

    #[test]
    fn display_column_is_none_when_the_table_only_lists_keys() {
        let columns = [column("id", "int4", true)];

        assert_eq!(pick_display_column(&columns), None);
    }

    #[test]
    fn substitute_only_touches_resolved_foreign_key_cells() {
        let result = QueryResult::table(
            vec![result_column("id"), result_column("customer_id")],
            vec![
                vec![Value::Int(1), Value::Int(8)],
                vec![Value::Int(2), Value::Int(8)],
                vec![Value::Int(3), Value::Int(9)],
            ],
            None,
            Duration::ZERO,
        );
        let model = TableModel::from(&result);
        let targets = [FkTarget {
            col: 1,
            schema: None,
            table: "customers".to_string(),
            pk_column: "id".to_string(),
        }];
        let cache = HashMap::from([(
            (None, "customers".to_string(), "8".to_string()),
            Some("María López".to_string()),
        )]);

        let new_model = substitute_fk_references(&model, &targets, &cache).expect("cells resolve");

        let resolved = new_model.cell(0, 1).expect("the cell exists");
        assert_eq!(
            resolved
                .fk_spans()
                .map(|(label, _)| label.to_string())
                .as_deref(),
            Some("María López")
        );
        assert_eq!(resolved.edit_text(), "8", "the editor carries the raw id");

        assert!(
            new_model
                .cell(1, 1)
                .expect("the cell exists")
                .fk_spans()
                .is_some(),
            "the same id on another row resolves too"
        );
        assert!(
            new_model
                .cell(2, 1)
                .expect("the cell exists")
                .fk_spans()
                .is_none(),
            "an id the cache has not resolved stays plain"
        );
        assert!(
            new_model
                .cell(0, 0)
                .expect("the cell exists")
                .fk_spans()
                .is_none(),
            "a column that is not a foreign key stays untouched"
        );
    }

    #[test]
    fn substitute_returns_none_when_nothing_resolves() {
        let result = QueryResult::table(
            vec![result_column("id"), result_column("customer_id")],
            vec![vec![Value::Int(1), Value::Int(8)]],
            None,
            Duration::ZERO,
        );
        let model = TableModel::from(&result);
        let targets = [FkTarget {
            col: 1,
            schema: None,
            table: "customers".to_string(),
            pk_column: "id".to_string(),
        }];

        assert!(
            substitute_fk_references(&model, &targets, &HashMap::new()).is_none(),
            "no cached label leaves the model alone"
        );
    }
}
