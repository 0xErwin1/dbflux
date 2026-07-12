//! Tables Mapping phase of the migration wizard: a wizard-local flex grid
//! with one row per checked source table and columns
//! **Source / Target / Mapping mode / Transform**. Each row wraps the pure
//! [`TableMigrationConfig`] (the unchanged binding contract) with the live
//! `Input` (target table name) and `Dropdown` (mapping mode) controls the user
//! adjusts it through, plus a per-table column drill-in that edits the
//! source→target column bindings.
//!
//! Typing a target-table name that already exists in the chosen container sets
//! the row to [`TableMappingMode::Existing`] and reseeds its column bindings
//! from the real target schema (fetched through the shared metadata seam);
//! typing an unknown name sets [`TableMappingMode::Create`] and mirrors the
//! source columns as the to-be-created target columns. The column drill-in
//! drives `TableMigrationConfig::set_binding`, so the assembled plan still
//! flows through `to_overrides()` verbatim.
//!
//! `data_table::DataTable` is deliberately not reused here — it is a
//! virtualized text grid that cannot host a `Dropdown`/`Input` entity per cell
//! (design ADR #6). The grid is shaped as its own module so a future review
//! surface (e.g. a deferred Import review) can lift it without a premature
//! `dbflux_components` abstraction.

use dbflux_components::controls::{
    Button, Dropdown, DropdownItem, DropdownSelectionChanged, GpuiInput as Input, InputEvent,
    InputState,
};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{Heights, Spacing};
use dbflux_core::{TableRef, TransferColumn};
use dbflux_transfer::TableMappingMode;
use dbflux_ui_base::app_state_entity::AppStateEntity;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::{ActiveTheme, Sizable};
use uuid::Uuid;

use crate::migrate_wizard::column_mapping::{
    TableMigrationConfig, default_mapping_mode, mapping_mode_options,
};
use crate::migrate_wizard::phases::{
    MappingRowPlan, MappingRowReadiness, can_advance_from_tables_mapping,
    tables_mapping_blocking_errors,
};
use crate::migrate_wizard::tree_model::NodeLoad;

const TARGET_COL_W: Pixels = px(220.0);
const MODE_COL_W: Pixels = px(200.0);
const TRANSFORM_COL_W: Pixels = px(160.0);
const UNSET_LABEL: &str = "(unset)";

/// Whether a typed target-table name refers to a table that already exists in
/// the chosen target container (⇒ [`TargetResolution::Existing`]) or a new one
/// the engine will create (⇒ [`TargetResolution::Create`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetResolution {
    Create,
    Existing,
}

/// Classifies a typed target-table name against the set of tables known to
/// already exist in the container. Exact match after trimming — an empty name
/// is treated as a not-yet-existing (Create) target so the advance guard, not
/// this classifier, is what blocks on a blank name.
pub fn resolve_target_name(name: &str, existing_tables: &[String]) -> TargetResolution {
    let trimmed = name.trim();

    if !trimmed.is_empty() && existing_tables.iter().any(|table| table == trimmed) {
        TargetResolution::Existing
    } else {
        TargetResolution::Create
    }
}

/// Applies a typed target-table name to a row's config: records the trimmed
/// name, sets the mapping mode from whether the target already exists
/// (Existing vs Create), and returns the resolution so the caller can decide
/// how to reseed the column bindings (a background schema fetch for an
/// existing table, or the source columns for a to-be-created one).
pub fn apply_target_name(
    config: &mut TableMigrationConfig,
    name: &str,
    existing_tables: &[String],
) -> TargetResolution {
    let resolution = resolve_target_name(name, existing_tables);

    config.target_table = name.trim().to_string();
    config.target_exists = resolution == TargetResolution::Existing;
    config.mapping_mode = default_mapping_mode(config.target_exists);

    resolution
}

/// Reseeds a config's target columns and re-runs the name-based auto-map while
/// preserving the user's chosen target table name, target schema, and mapping
/// mode. Reuses [`TableMigrationConfig::new`]'s auto-mapping rather than
/// reimplementing the binding model: for an existing table the fetched target
/// schema is passed in; for a Create target the source columns are mirrored.
pub fn reseed_target_columns(
    config: &mut TableMigrationConfig,
    target_columns: Vec<TransferColumn>,
) {
    let target_table = config.target_table.clone();
    let target_schema = config.target_schema.clone();
    let mapping_mode = config.mapping_mode;

    let mut rebuilt = TableMigrationConfig::new(
        config.source_table.clone(),
        config.source_columns.clone(),
        config.target_exists,
        target_columns,
    );

    rebuilt.target_table = target_table;
    rebuilt.target_schema = target_schema;
    rebuilt.mapping_mode = mapping_mode;

    *config = rebuilt;
}

/// Emitted whenever a row's target name, mapping mode, or column binding
/// changes, so the host can re-evaluate the `TablesMapping` advance guard.
#[derive(Debug, Clone)]
pub struct MappingChanged;

/// One grid row: the pure [`TableMigrationConfig`] plus the live controls the
/// user edits it through and the load state of its target-existence lookup.
struct MappingRow {
    config: TableMigrationConfig,
    target_input: Entity<InputState>,
    mode_dropdown: Entity<Dropdown>,
    target_lookup: NodeLoad,
}

/// The open column drill-in: which row it edits and one source-column
/// `Dropdown` per target column (index 0 = `(unset)` = NULL).
struct BindingEditor {
    row_index: usize,
    dropdowns: Vec<Entity<Dropdown>>,
}

pub struct MappingPhase {
    app_state: Entity<AppStateEntity>,
    focus_handle: FocusHandle,
    source_profile_id: Uuid,
    source_database: String,
    target_profile_id: Uuid,
    target_database: String,
    existing_target_tables: Vec<String>,
    supports_truncate: bool,
    rows: Vec<MappingRow>,
    editor: Option<BindingEditor>,
    _subscriptions: Vec<Subscription>,
    _editor_subscriptions: Vec<Subscription>,
}

impl EventEmitter<MappingChanged> for MappingPhase {}

impl MappingPhase {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        app_state: Entity<AppStateEntity>,
        source_profile_id: Uuid,
        source_database: String,
        target_profile_id: Uuid,
        target_database: String,
        existing_target_tables: Vec<String>,
        supports_truncate: bool,
        configs: Vec<TableMigrationConfig>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut phase = Self {
            app_state,
            focus_handle: cx.focus_handle(),
            source_profile_id,
            source_database,
            target_profile_id,
            target_database,
            existing_target_tables,
            supports_truncate,
            rows: Vec::new(),
            editor: None,
            _subscriptions: Vec::new(),
            _editor_subscriptions: Vec::new(),
        };

        phase.build_rows(configs, window, cx);
        phase
    }

    pub fn focus_handle(&self) -> &FocusHandle {
        &self.focus_handle
    }

    /// The assembled configs, in row order, for the host to feed into
    /// `build_migration_table_plans` — the binding contract is untouched.
    pub fn configs(&self) -> impl Iterator<Item = &TableMigrationConfig> {
        self.rows.iter().map(|row| &row.config)
    }

    /// Per-row readiness for the `TablesMapping` → `Options` guard.
    pub fn readiness(&self) -> Vec<MappingRowReadiness<'_>> {
        self.rows
            .iter()
            .map(|row| MappingRowReadiness {
                target_name: row.config.target_table.as_str(),
                target_lookup: row.target_lookup.clone(),
            })
            .collect()
    }

    /// Whether the source and target refer to the same container — the same
    /// connection and database — so a target table can collide with a source
    /// table (see [`tables_mapping_blocking_errors`]).
    fn same_container(&self) -> bool {
        self.source_profile_id == self.target_profile_id
            && self.source_database == self.target_database
    }

    fn row_plans(&self) -> Vec<MappingRowPlan<'_>> {
        self.rows
            .iter()
            .map(|row| MappingRowPlan {
                source_schema: row.config.source_table.schema.as_deref(),
                source_table: row.config.source_table.name.as_str(),
                target_schema: row.config.target_schema.as_deref(),
                target_table: row.config.target_table.as_str(),
                destructive: row.config.is_destructive(),
            })
            .collect()
    }

    /// Cross-row blocking validation errors (duplicate targets, destructive
    /// source-as-target collisions) that must prevent advancing.
    pub fn blocking_errors(&self) -> Vec<String> {
        tables_mapping_blocking_errors(&self.row_plans(), self.same_container())
    }

    /// The single guard the host uses for the `TablesMapping` → `Options`
    /// advance: every row ready AND no blocking cross-row collision.
    pub fn can_advance(&self) -> bool {
        can_advance_from_tables_mapping(&self.readiness()) && self.blocking_errors().is_empty()
    }

    fn build_rows(
        &mut self,
        configs: Vec<TableMigrationConfig>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.rows.clear();
        self._subscriptions.clear();
        self.editor = None;
        self._editor_subscriptions.clear();

        let supports_truncate = self.supports_truncate;

        for (row_index, config) in configs.into_iter().enumerate() {
            let target_input = cx.new(|cx| {
                InputState::new(window, cx)
                    .default_value(config.target_table.clone())
                    .placeholder("Target table")
            });

            let mode_options = mapping_mode_options(supports_truncate);
            let selected_mode = mode_options
                .iter()
                .position(|(_, mode)| *mode == config.mapping_mode);
            let mode_items: Vec<DropdownItem> = mode_options
                .iter()
                .map(|(label, _)| DropdownItem::new(*label))
                .collect();
            let mode_dropdown = cx.new(|_cx| {
                Dropdown::new(SharedString::from(format!("migrate-mode-{row_index}")))
                    .items(mode_items)
                    .selected_index(selected_mode)
                    .placeholder("Mode")
            });

            let input_sub = cx.subscribe_in(
                &target_input,
                window,
                move |this, _entity, event: &InputEvent, window, cx| {
                    if let InputEvent::Change = event {
                        this.on_target_name_changed(row_index, window, cx);
                    }
                },
            );
            let mode_sub = cx.subscribe(
                &mode_dropdown,
                move |this, _entity, event: &DropdownSelectionChanged, cx| {
                    this.on_mode_changed(row_index, event.index, cx);
                },
            );

            self.rows.push(MappingRow {
                config,
                target_input,
                mode_dropdown,
                target_lookup: NodeLoad::Loaded,
            });
            self._subscriptions.push(input_sub);
            self._subscriptions.push(mode_sub);
        }
    }

    fn on_target_name_changed(
        &mut self,
        row_index: usize,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let existing = self.existing_target_tables.clone();
        let supports_truncate = self.supports_truncate;
        let Some(row) = self.rows.get_mut(row_index) else {
            return;
        };

        let typed = row.target_input.read(cx).value().to_string();
        let resolution = apply_target_name(&mut row.config, &typed, &existing);

        // `apply_target_name` may reset the mapping mode (Create vs Existing);
        // keep the row's mode dropdown in lockstep so the grid never shows a
        // mode different from the one that will actually run.
        let mode_options = mapping_mode_options(supports_truncate);
        let selected_mode = mode_options
            .iter()
            .position(|(_, mode)| *mode == row.config.mapping_mode);
        row.mode_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(selected_mode, cx)
        });

        match resolution {
            TargetResolution::Create => {
                let source_columns = row.config.source_columns.clone();
                reseed_target_columns(&mut row.config, source_columns);
                row.target_lookup = NodeLoad::Loaded;
                self.sync_editor_after_reseed(row_index, cx);
                cx.emit(MappingChanged);
                cx.notify();
            }
            TargetResolution::Existing => {
                row.target_lookup = NodeLoad::Loading;
                cx.emit(MappingChanged);
                cx.notify();
                self.reseed_existing(row_index, cx);
            }
        }
    }

    fn on_mode_changed(&mut self, row_index: usize, index: usize, cx: &mut Context<Self>) {
        let mode_options = mapping_mode_options(self.supports_truncate);
        let Some((_, mode)) = mode_options.get(index).copied() else {
            return;
        };
        if let Some(row) = self.rows.get_mut(row_index) {
            row.config.mapping_mode = mode;
            cx.emit(MappingChanged);
            cx.notify();
        }
    }

    /// Fetches the existing target table's real schema through the shared
    /// metadata seam on the background executor and reseeds the row's column
    /// bindings against it (design "existing ⇒ bg target-column fetch").
    fn reseed_existing(&mut self, row_index: usize, cx: &mut Context<Self>) {
        let Some(row) = self.rows.get(row_index) else {
            return;
        };
        let table_ref = TableRef {
            schema: row.config.target_schema.clone(),
            name: row.config.target_table.clone(),
        };

        let app_state = self.app_state.clone();
        let profile_id = self.target_profile_id;
        let database = self.target_database.clone();

        cx.spawn(async move |this, cx| {
            let result = super::fetch_table_details_via_seam(
                &app_state, profile_id, &database, &table_ref, cx,
            )
            .await;

            this.update(cx, |this, cx| {
                // Drop a stale result: if the row's target changed while this
                // fetch was in flight, a newer reseed owns the row and
                // applying this schema would clobber the current state.
                let still_current = this.rows.get(row_index).is_some_and(|row| {
                    row.config.target_schema == table_ref.schema
                        && row.config.target_table == table_ref.name
                });
                if !still_current {
                    return;
                }

                if let Some(row) = this.rows.get_mut(row_index) {
                    match result {
                        Ok(super::TableDetailsFetch::Found(info)) => {
                            let columns =
                                super::to_transfer_columns(info.columns.unwrap_or_default());
                            reseed_target_columns(&mut row.config, columns);
                            row.target_lookup = NodeLoad::Loaded;
                        }
                        Ok(super::TableDetailsFetch::NotFound(error)) | Err(error) => {
                            row.target_lookup = NodeLoad::Failed(error.clone());
                            report_error(
                                UserFacingError::new(
                                    ErrorKind::Driver,
                                    format!("Could not read target table schema: {error}"),
                                ),
                                cx,
                            );
                        }
                    }
                }
                this.sync_editor_after_reseed(row_index, cx);
                cx.emit(MappingChanged);
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn sync_editor_after_reseed(&mut self, row_index: usize, cx: &mut Context<Self>) {
        if self
            .editor
            .as_ref()
            .is_some_and(|editor| editor.row_index == row_index)
        {
            self.open_drilldown(row_index, cx);
        }
    }

    /// Opens (or rebuilds) the column drill-in for `row_index`, creating one
    /// source-column `Dropdown` per target column seeded from the current
    /// bindings and wiring each back to `TableMigrationConfig::set_binding`.
    fn open_drilldown(&mut self, row_index: usize, cx: &mut Context<Self>) {
        self._editor_subscriptions.clear();

        let Some(row) = self.rows.get(row_index) else {
            self.editor = None;
            return;
        };

        let source_names: Vec<SharedString> = row
            .config
            .source_columns
            .iter()
            .map(|column| SharedString::from(column.name.clone()))
            .collect();

        let mut dropdowns = Vec::with_capacity(row.config.target_columns.len());
        let mut subscriptions = Vec::new();

        for (target_index, binding) in row.config.bindings.iter().enumerate() {
            let mut items = vec![DropdownItem::new(UNSET_LABEL)];
            items.extend(source_names.iter().cloned().map(DropdownItem::new));

            let selected = binding.map(|source_index| source_index + 1).or(Some(0));
            let dropdown = cx.new(|_cx| {
                Dropdown::new(SharedString::from(format!(
                    "migrate-bind-{row_index}-{target_index}"
                )))
                .items(items)
                .selected_index(selected)
            });

            let subscription = cx.subscribe(
                &dropdown,
                move |this, _entity, event: &DropdownSelectionChanged, cx| {
                    this.on_binding_changed(row_index, target_index, event.index, cx);
                },
            );

            dropdowns.push(dropdown);
            subscriptions.push(subscription);
        }

        self.editor = Some(BindingEditor {
            row_index,
            dropdowns,
        });
        self._editor_subscriptions = subscriptions;
        cx.notify();
    }

    fn close_drilldown(&mut self, cx: &mut Context<Self>) {
        self.editor = None;
        self._editor_subscriptions.clear();
        cx.notify();
    }

    fn on_binding_changed(
        &mut self,
        row_index: usize,
        target_index: usize,
        item_index: usize,
        cx: &mut Context<Self>,
    ) {
        let source_index = item_index.checked_sub(1);
        if let Some(row) = self.rows.get_mut(row_index) {
            row.config.set_binding(target_index, source_index);
            cx.emit(MappingChanged);
            cx.notify();
        }
    }

    fn set_all_modes(&mut self, mode: TableMappingMode, cx: &mut Context<Self>) {
        let mode_options = mapping_mode_options(self.supports_truncate);
        let selected = mode_options.iter().position(|(_, m)| *m == mode);

        for row in &mut self.rows {
            row.config.mapping_mode = mode;
            row.mode_dropdown
                .update(cx, |dropdown, cx| dropdown.set_selected_index(selected, cx));
        }

        cx.emit(MappingChanged);
        cx.notify();
    }
}

impl Render for MappingPhase {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let body = match self.editor.as_ref().map(|editor| editor.row_index) {
            Some(row_index) => self.render_drilldown(row_index, cx).into_any_element(),
            None => self.render_grid(cx).into_any_element(),
        };

        div()
            .track_focus(&self.focus_handle)
            .key_context("MigrateTablesMapping")
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .p(Spacing::MD)
            .size_full()
            .child(body)
    }
}

impl MappingPhase {
    fn render_grid(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let rows: Vec<AnyElement> = self
            .rows
            .iter()
            .enumerate()
            .map(|(index, row)| self.render_grid_row(index, row, cx))
            .collect();

        let blocking_errors = self.blocking_errors();

        div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .size_full()
            .child(self.render_bulk_toolbar(cx))
            .child(render_grid_header(cx))
            .child(
                div()
                    .id("migrate-mapping-rows")
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_h(px(0.0))
                    .overflow_y_scroll()
                    .children(rows),
            )
            .when(!blocking_errors.is_empty(), |parent| {
                parent.child(
                    div().flex().flex_col().gap(px(2.0)).children(
                        blocking_errors
                            .into_iter()
                            .map(|error| Text::caption(error).danger().into_any_element()),
                    ),
                )
            })
    }

    fn render_bulk_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let supports_truncate = self.supports_truncate;

        div()
            .flex()
            .flex_row()
            .items_center()
            .gap(Spacing::SM)
            .child(Text::caption("Set all:"))
            .child(
                Button::new("migrate-bulk-existing", "Existing")
                    .small()
                    .ghost()
                    .on_click(cx.listener(|this, _event, _window, cx| {
                        this.set_all_modes(TableMappingMode::Existing, cx);
                    })),
            )
            .when(supports_truncate, |parent| {
                parent.child(
                    Button::new("migrate-bulk-truncate", "Truncate")
                        .small()
                        .ghost()
                        .on_click(cx.listener(|this, _event, _window, cx| {
                            this.set_all_modes(TableMappingMode::Truncate, cx);
                        })),
                )
            })
            .child(
                Button::new("migrate-bulk-skip", "Skip")
                    .small()
                    .ghost()
                    .on_click(cx.listener(|this, _event, _window, cx| {
                        this.set_all_modes(TableMappingMode::Skip, cx);
                    })),
            )
    }

    fn render_grid_row(
        &self,
        row_index: usize,
        row: &MappingRow,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();
        let unmatched = row.config.unmatched_source_names();

        let transform_cell = div()
            .w(TRANSFORM_COL_W)
            .flex()
            .flex_col()
            .gap(px(2.0))
            .child(
                Button::new(
                    SharedString::from(format!("migrate-transform-{row_index}")),
                    "Columns…",
                )
                .small()
                .ghost()
                .disabled(matches!(row.target_lookup, NodeLoad::Loading))
                .on_click(cx.listener(move |this, _event, _window, cx| {
                    this.open_drilldown(row_index, cx);
                })),
            )
            .when(!unmatched.is_empty(), |parent| {
                parent.child(
                    Text::caption(SharedString::from(format!("{} unmapped", unmatched.len())))
                        .color(theme.warning),
                )
            });

        div()
            .flex()
            .flex_row()
            .items_center()
            .gap(Spacing::SM)
            .py(Spacing::XS)
            .border_b_1()
            .border_color(theme.border)
            .child(
                div()
                    .flex_1()
                    .min_w(px(0.0))
                    .overflow_hidden()
                    .child(Text::body(row.config.source_table.qualified_name())),
            )
            .child(
                div()
                    .w(TARGET_COL_W)
                    .child(Input::new(&row.target_input).small().w_full()),
            )
            .child(div().w(MODE_COL_W).child(row.mode_dropdown.clone()))
            .child(transform_cell)
            .into_any_element()
    }

    fn render_drilldown(&self, row_index: usize, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let Some(row) = self.rows.get(row_index) else {
            return div().into_any_element();
        };
        let Some(editor) = self.editor.as_ref() else {
            return div().into_any_element();
        };

        let unmatched = row.config.unmatched_source_names();
        let binding_rows: Vec<AnyElement> = row
            .config
            .target_columns
            .iter()
            .zip(editor.dropdowns.iter())
            .map(|(target_column, dropdown)| {
                render_binding_row(&target_column.name, dropdown, &theme)
            })
            .collect();

        div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .size_full()
            .child(
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        Button::new("migrate-drilldown-back", "Back")
                            .small()
                            .ghost()
                            .icon(AppIcon::ChevronLeft)
                            .on_click(cx.listener(|this, _event, _window, cx| {
                                this.close_drilldown(cx);
                            })),
                    )
                    .child(Text::body(SharedString::from(format!(
                        "Column mapping — {}",
                        row.config.source_table.qualified_name()
                    )))),
            )
            .child(
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(div().w(TARGET_COL_W).child(Text::label("Target column")))
                    .child(div().w(TARGET_COL_W).child(Text::label("Source column"))),
            )
            .child(
                div()
                    .id("migrate-binding-rows")
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .flex_1()
                    .min_h(px(0.0))
                    .overflow_y_scroll()
                    .children(binding_rows),
            )
            .when(!unmatched.is_empty(), |parent| {
                parent.child(
                    Text::caption(SharedString::from(format!(
                        "Unmapped source columns: {}",
                        unmatched.join(", ")
                    )))
                    .color(theme.warning),
                )
            })
            .into_any_element()
    }
}

fn render_grid_header(cx: &mut Context<MappingPhase>) -> impl IntoElement {
    let theme = cx.theme().clone();

    div()
        .flex()
        .flex_row()
        .items_center()
        .gap(Spacing::SM)
        .h(Heights::ROW)
        .border_b_1()
        .border_color(theme.border)
        .child(div().flex_1().min_w(px(0.0)).child(Text::label("Source")))
        .child(div().w(TARGET_COL_W).child(Text::label("Target")))
        .child(div().w(MODE_COL_W).child(Text::label("Mapping mode")))
        .child(div().w(TRANSFORM_COL_W).child(Text::label("Transform")))
}

fn render_binding_row(
    target_name: &str,
    dropdown: &Entity<Dropdown>,
    theme: &gpui_component::Theme,
) -> AnyElement {
    div()
        .flex()
        .flex_row()
        .items_center()
        .gap(Spacing::SM)
        .child(
            div()
                .w(TARGET_COL_W)
                .overflow_hidden()
                .child(Text::body(target_name.to_string())),
        )
        .child(
            Icon::new(AppIcon::ChevronRight)
                .small()
                .color(theme.muted_foreground),
        )
        .child(div().w(TARGET_COL_W).child(dropdown.clone()))
        .into_any_element()
}

#[cfg(test)]
mod tests {
    use super::{TargetResolution, apply_target_name, reseed_target_columns, resolve_target_name};
    use crate::migrate_wizard::column_mapping::TableMigrationConfig;
    use dbflux_core::{TableRef, TransferColumn};
    use dbflux_transfer::TableMappingMode;

    fn column(name: &str) -> TransferColumn {
        TransferColumn {
            name: name.to_string(),
            type_name: Some("text".to_string()),
            nullable: true,
            is_primary_key: false,
        }
    }

    fn config_for(source: &[&str]) -> TableMigrationConfig {
        let source_columns = source.iter().map(|name| column(name)).collect();
        TableMigrationConfig::new(TableRef::new("users"), source_columns, false, Vec::new())
    }

    #[test]
    fn resolve_target_name_matches_existing_tables_exactly_after_trimming() {
        let existing = vec!["users".to_string(), "orders".to_string()];

        assert_eq!(
            resolve_target_name("  users  ", &existing),
            TargetResolution::Existing
        );
        assert_eq!(
            resolve_target_name("brand_new", &existing),
            TargetResolution::Create
        );
        assert_eq!(
            resolve_target_name("   ", &existing),
            TargetResolution::Create
        );
        assert_eq!(
            resolve_target_name("Users", &existing),
            TargetResolution::Create
        );
    }

    #[test]
    fn apply_target_name_with_unknown_name_sets_create_mode() {
        let mut config = config_for(&["id", "email"]);
        let existing = vec!["orders".to_string()];

        let resolution = apply_target_name(&mut config, "t_new", &existing);

        assert_eq!(resolution, TargetResolution::Create);
        assert_eq!(config.mapping_mode, TableMappingMode::Create);
        assert!(!config.target_exists);
        assert_eq!(config.target_table, "t_new");
    }

    #[test]
    fn apply_target_name_with_existing_name_sets_existing_mode_and_signals_reseed() {
        let mut config = config_for(&["id", "email"]);
        let existing = vec!["accounts".to_string()];

        let resolution = apply_target_name(&mut config, "accounts", &existing);

        assert_eq!(resolution, TargetResolution::Existing);
        assert_eq!(config.mapping_mode, TableMappingMode::Existing);
        assert!(config.target_exists);
        assert_eq!(config.target_table, "accounts");
    }

    #[test]
    fn reseed_target_columns_reautomaps_and_surfaces_unmatched_source() {
        let mut config = config_for(&["id", "legacy_x"]);
        apply_target_name(&mut config, "accounts", &["accounts".to_string()]);

        reseed_target_columns(&mut config, vec![column("id"), column("y")]);

        assert_eq!(config.target_table, "accounts");
        assert_eq!(config.mapping_mode, TableMappingMode::Existing);
        assert_eq!(
            config.unmatched_source_names(),
            vec!["legacy_x".to_string()]
        );
    }

    #[test]
    fn reseed_as_create_mirrors_source_columns_and_clears_unmatched() {
        let mut config = config_for(&["id", "email"]);
        apply_target_name(&mut config, "accounts", &["accounts".to_string()]);
        reseed_target_columns(&mut config, vec![column("id")]);
        assert_eq!(config.unmatched_source_names(), vec!["email".to_string()]);

        apply_target_name(&mut config, "fresh_table", &["accounts".to_string()]);
        let source_columns = config.source_columns.clone();
        reseed_target_columns(&mut config, source_columns);

        assert_eq!(config.mapping_mode, TableMappingMode::Create);
        assert!(config.unmatched_source_names().is_empty());
    }

    #[test]
    fn drilldown_set_binding_round_trips_through_to_overrides() {
        let mut config = config_for(&["id", "legacy_x"]);
        apply_target_name(&mut config, "accounts", &["accounts".to_string()]);
        reseed_target_columns(&mut config, vec![column("id"), column("y")]);
        assert_eq!(
            config.unmatched_source_names(),
            vec!["legacy_x".to_string()]
        );

        let source_index = Some(1);
        config.set_binding(1, source_index);

        assert!(config.unmatched_source_names().is_empty());
        let overrides = config.to_overrides();
        assert_eq!(overrides[1].target_column, "y");
        assert_eq!(overrides[1].source_column, Some("legacy_x".to_string()));

        config.set_binding(1, None);
        let overrides = config.to_overrides();
        assert_eq!(overrides[1].source_column, None);
    }
}
