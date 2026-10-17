//! Background resolution of foreign-key cell display values.
//!
//! [`DataGridPanel::resolve_fk_display_values`] runs after a page of a table
//! lands (and again when FK metadata arrives): for every single-column
//! foreign key of the browsed table it groups the distinct unresolved ids by
//! referenced table and fetches each group through one
//! `Connection::fetch_rows_by_pks` batch, projected down to the referenced
//! table's pk and display columns. When every group of the round has landed
//! it swaps the raw-id text cells for `CellValue::fk_reference` in a single
//! model swap, which draws the referenced row's display column beside the id
//! while editing, copying and mutations keep carrying the id.
//!
//! The whole pass is gated on the `grid_fk_labels` setting (off by default):
//! with it off the grid shows raw ids and starts no resolution fetches. The
//! row inspector resolves its references regardless of the setting.
//!
//! Ids already resolved live in [`FkDisplayLoader::cache`] and are applied
//! without a query; an id no row answered for is cached as `None` so it is
//! not fetched again. A round that lands after another page replaced the
//! rows still feeds the cache but never swaps the model (see
//! [`FkDisplayLoader::generation`]).

use super::*;
use dbflux_components::components::data_table::model::{CellKind, CellValue};
use dbflux_core::{ColumnInfo, ConnectedProfile, Connection, SchemaForeignKeyInfo};

/// (referenced schema, referenced table, raw id text) of one foreign-key
/// cell.
type FkDisplayKey = (Option<String>, String, String);

/// (referenced schema, referenced table) of a referenced table.
type ReferencedTable = (Option<String>, String);

/// The display column of each referenced table, named by its pk column
/// key first and without one second. `None` marks a table whose column
/// is not known yet, or has none to name rows by.
type DisplayColumns = HashMap<ReferencedTable, Option<String>>;

/// The round's lookups batched per (referenced schema, referenced table,
/// pk column).
type FkLookupGroups = HashMap<(Option<String>, String, String), Vec<FkLookup>>;

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

impl FkDisplayLoader {
    /// The label the cache resolved for the referenced row `id` of `table`,
    /// when the cache holds one. Ids are keyed by the text the grid's cell
    /// edits carry, so only the id kinds ([`Value::Int`], [`Value::Float`],
    /// [`Value::Text`]) can match.
    pub(super) fn cached_label(
        &self,
        schema: Option<&str>,
        table: &str,
        id: &Value,
    ) -> Option<String> {
        let id_text = id_text(id)?;

        self.cache
            .get(&(schema.map(str::to_string), table.to_string(), id_text))
            .cloned()
            .flatten()
    }
}

/// The text an id value keys a lookup by: the one the grid's cell edits
/// carry. `None` for the kinds an id column does not take.
pub(super) fn id_text(value: &Value) -> Option<String> {
    match value {
        Value::Int(number) => Some(number.to_string()),
        Value::Float(number) => Some(number.to_string()),
        Value::Text(text) => Some(text.clone()),
        _ => None,
    }
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
pub(super) fn display_label(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Text(label) => Some(label.clone()),
        other => Some(other.as_display_string_truncated(200)),
    }
}

/// The column a referenced row is named by: its first textual
/// non-primary-key column, else its first non-primary-key column.
pub(super) fn pick_display_column(columns: &[ColumnInfo]) -> Option<String> {
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

/// The page's unresolved lookups: one per distinct foreign-key id the
/// cache does not hold, in row order. Ids the cache already resolved —
/// including `None` misses — are not fetched again.
fn fk_lookups_for_page(
    model: &TableModel,
    targets: &[FkTarget],
    cache: &HashMap<FkDisplayKey, Option<String>>,
) -> Vec<FkLookup> {
    // One fetch per distinct unresolved id.
    let mut seen: HashSet<FkDisplayKey> = HashSet::new();
    let mut lookups: Vec<FkLookup> = Vec::new();

    for row in &model.rows {
        for target in targets {
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
            if cache.contains_key(&key) || !seen.insert(key.clone()) {
                continue;
            }

            lookups.push(FkLookup {
                key,
                pk_column: target.pk_column.clone(),
                value,
            });
        }
    }

    lookups
}

/// The lookups grouped per distinct referenced table and pk column — two
/// foreign keys into the same table share the group. No chunking on top:
/// a group is bounded by one page's distinct ids and the driver already
/// splits the IN list.
fn group_fk_lookups(lookups: Vec<FkLookup>) -> FkLookupGroups {
    let mut groups = FkLookupGroups::default();

    for lookup in lookups {
        let group_key = (
            lookup.key.0.clone(),
            lookup.key.1.clone(),
            lookup.pk_column.clone(),
        );
        groups.entry(group_key).or_default().push(lookup);
    }

    groups
}

/// The display column of each referenced table the connection's cached
/// table details answer, plus the tables they do not — the ones the round
/// fetches metadata for. The cache is keyed by the referenced schema
/// first and without one second, the shape MySQL writes when it records
/// the schema on the foreign key but not on the table.
fn cached_display_columns(
    connected: &ConnectedProfile,
    resolved_database: &str,
    targets: &[FkTarget],
) -> (DisplayColumns, Vec<ReferencedTable>) {
    let mut display_columns = DisplayColumns::default();
    let mut unknown_tables: Vec<ReferencedTable> = Vec::new();

    for target in targets {
        let table_key = (target.schema.clone(), target.table.clone());
        if display_columns.contains_key(&table_key) {
            continue;
        }

        let info = connected
            .table_details
            .get(&(
                resolved_database.to_string(),
                target.schema.clone(),
                target.table.clone(),
            ))
            .or_else(|| {
                connected.table_details.get(&(
                    resolved_database.to_string(),
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

    (display_columns, unknown_tables)
}

/// The label of every row a batch answered, keyed by its pk's text — the
/// same text the cache keys lookups by. A requested id no row answered
/// for stays a `None` miss, so it is not fetched again.
fn labels_by_pk_text(
    rows: &[HashMap<String, Value>],
    pk_column: &str,
    display_column: Option<&str>,
) -> HashMap<String, Option<String>> {
    let mut labels_by_pk: HashMap<String, Option<String>> = HashMap::new();

    for row in rows {
        let Some(pk) = row.get(pk_column).and_then(id_text) else {
            continue;
        };
        let label = display_column
            .and_then(|column| row.get(column))
            .and_then(display_label);
        labels_by_pk.insert(pk, label);
    }

    labels_by_pk
}

/// One round of foreign-key label resolution: everything its background
/// fetches work on, gathered on the foreground before the page can
/// change underneath them.
struct FkDisplayRound {
    /// The round's number, re-checked before every fetch so a late
    /// landing never touches newer rows.
    generation: u64,
    profile_id: Uuid,
    app_state: Entity<AppStateEntity>,
    connection: Arc<dyn Connection>,
    /// The database the table-details cache is keyed by.
    resolved_database: String,
    groups: FkLookupGroups,
    /// Display column per referenced table; the metadata fetch fills the
    /// entries the cache left open.
    display_columns: DisplayColumns,
    /// The referenced tables the cache could not answer; their metadata
    /// is fetched before the group batches run.
    unknown_tables: Vec<ReferencedTable>,
}

impl FkDisplayRound {
    /// Fetches the round's unknown table metadata, then each group's rows,
    /// and lands every label of the round in one model swap through
    /// [`DataGridPanel::apply_fk_display_batch`].
    async fn run(mut self, panel: WeakEntity<DataGridPanel>, cx: &mut AsyncApp) {
        let executor = cx.background_executor().clone();

        // A round a newer page already outran skips its metadata
        // queries, not just its group fetches.
        if !self.still_current(&panel, cx) {
            return;
        }

        self.fetch_unknown_display_columns(&executor, cx).await;

        // One batch per referenced table, in sequence: a page references
        // a handful of tables, and the plain loop keeps the generation
        // check a `break`. The round's labels accumulate here and swap
        // into the model once at the end.
        let mut resolved: Vec<(FkDisplayKey, Option<String>)> = Vec::new();

        for ((schema, table, pk_column), lookups) in &self.groups {
            if !self.still_current(&panel, cx) {
                break;
            }

            let display_column = self
                .display_columns
                .get(&(schema.clone(), table.clone()))
                .cloned()
                .flatten();
            let mut projection = vec![pk_column.clone()];
            if let Some(column) = &display_column {
                projection.push(column.clone());
            }
            let values: Vec<Value> = lookups.iter().map(|lookup| lookup.value.clone()).collect();

            let connection = self.connection.clone();
            let database = self.resolved_database.clone();
            let schema_for_task = schema.clone();
            let table_for_task = table.clone();
            let pk_for_task = pk_column.clone();

            let rows = executor
                .spawn(async move {
                    connection.fetch_rows_by_pks(
                        &database,
                        schema_for_task.as_deref().unwrap_or(""),
                        &table_for_task,
                        &pk_for_task,
                        &values,
                        Some(&projection),
                    )
                })
                .await;

            match rows {
                Ok(rows) => {
                    let labels = labels_by_pk_text(&rows, pk_column, display_column.as_deref());

                    for lookup in lookups {
                        let label = labels.get(&lookup.key.2).cloned().flatten();
                        resolved.push((lookup.key.clone(), label));
                    }
                }
                Err(error) => {
                    // A failed batch is transient: the ids stay uncached
                    // and the next page retries them.
                    log::debug!(
                        "fk display: could not fetch rows of {table} for {} ids: {error}",
                        lookups.len()
                    );
                }
            }
        }

        panel
            .update(cx, |panel, cx| {
                panel.apply_fk_display_batch(self.generation, resolved, cx);
            })
            .ok();
    }

    /// Fetches the metadata of the referenced tables the connection's
    /// cache could not answer, records it in the app's table-details
    /// cache and folds each table's display column into `display_columns`.
    async fn fetch_unknown_display_columns(
        &mut self,
        executor: &BackgroundExecutor,
        cx: &mut AsyncApp,
    ) {
        for (schema, table) in &self.unknown_tables {
            let connection = self.connection.clone();
            let database = self.resolved_database.clone();
            let schema_for_task = schema.clone();
            let table_for_task = table.clone();

            let details = executor
                .spawn(async move {
                    connection.table_details(&database, schema_for_task.as_deref(), &table_for_task)
                })
                .await;

            let column = match details {
                Ok(details) => {
                    let column = details.columns.as_deref().and_then(pick_display_column);
                    cx.update(|cx| {
                        self.app_state.update(cx, |state, _| {
                            state.set_table_details(
                                self.profile_id,
                                self.resolved_database.clone(),
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

            self.display_columns
                .insert((schema.clone(), table.clone()), column);
        }
    }

    /// Whether the round is still the panel's current one; a newer page
    /// landing first means the fetches' results must not be applied.
    fn still_current(&self, panel: &WeakEntity<DataGridPanel>, cx: &mut AsyncApp) -> bool {
        panel
            .update(cx, |panel, _| {
                panel.fk_display.generation == self.generation
            })
            .unwrap_or(false)
    }
}

impl DataGridPanel {
    /// Whether the grid resolves foreign-key cells into readable labels
    /// (`grid_fk_labels`). The row inspector resolves its references
    /// regardless of this setting.
    pub(super) fn fk_labels_enabled(&self, cx: &App) -> bool {
        self.app_state.read(cx).general_settings().grid_fk_labels
    }

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
    /// distinct unresolved ones are fetched in the background — one batch
    /// per referenced table — and every label of the round lands in a single
    /// model swap once the batches finish. A no-op for non-table sources,
    /// missing FK metadata and connections without row lookups.
    pub(super) fn resolve_fk_display_values(&mut self, cx: &mut Context<Self>) {
        self.fk_display.generation += 1;

        // The setting is read per page load, so toggling it applies to the
        // pages loaded afterwards without recreating the panel.
        if !self.fk_labels_enabled(cx) {
            return;
        }

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
            } => (*profile_id, database.as_deref()),
            _ => return,
        };

        let (connection, resolved_database, display_columns, unknown_tables) = {
            let Some(connected) = self.app_state.read(cx).connections().get(&profile_id) else {
                return;
            };

            let resolved_database = Self::table_details_database(connected, database);
            let connection = connected.connection_for_database(&resolved_database);
            let (display_columns, unknown_tables) =
                cached_display_columns(connected, &resolved_database, &targets);

            (
                connection,
                resolved_database,
                display_columns,
                unknown_tables,
            )
        };

        let model = table_state.read(cx).model_arc().clone();
        let lookups = fk_lookups_for_page(&model, &targets, &self.fk_display.cache);

        // Nothing left to fetch: the ids the cache already knows land on the
        // cells without a query, and this swap is the round's only one.
        if lookups.is_empty() {
            if let Some(new_model) =
                substitute_fk_references(&model, &targets, &self.fk_display.cache)
            {
                self.swap_table_model_keep_chrome(new_model, cx);
            }
            return;
        }

        let round = FkDisplayRound {
            generation,
            profile_id,
            app_state: self.app_state.clone(),
            connection,
            resolved_database,
            groups: group_fk_lookups(lookups),
            display_columns,
            unknown_tables,
        };

        self.fk_display.task = Some(cx.spawn(async move |this, cx| {
            round.run(this, cx).await;
        }));
    }

    /// Records the round's resolved labels in the cache and, when the round
    /// is still current, swaps every resolved foreign-key cell into the
    /// grid's model — the round's single swap.
    fn apply_fk_display_batch(
        &mut self,
        generation: u64,
        resolved: Vec<(FkDisplayKey, Option<String>)>,
        cx: &mut Context<Self>,
    ) {
        for (key, label) in resolved {
            self.fk_display.cache.insert(key, label);
        }

        // A round a newer page outran still feeds the cache — the labels
        // name the same referenced rows — but must not touch the rows on
        // screen.
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
