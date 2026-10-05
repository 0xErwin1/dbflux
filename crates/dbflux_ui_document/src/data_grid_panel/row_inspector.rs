//! Row Inspector content for the workspace-level inspector rail.
//!
//! # `RowInspectorContent`
//!
//! The row inspector draws its whole panel (IslTable): a header with the row
//! number, its key expression and the copy, pin and close buttons, the
//! scrollable field list and REFERENCES section, and a footer with the row
//! actions. The workspace rail only hosts it and owns the resize grip; it
//! skips its own title bar for this content.
//!
//! # Opening
//!
//! `DataGridPanel::open_row_inspector` builds an `InspectorSnapshot`, creates
//! or updates a `RowInspectorContent` entity, and emits
//! `DataGridEvent::OpenInspector` so the workspace mounts it in the inspector
//! rail. The buttons emit `RowInspectorContentEvent`s that the grid acts on.
//!
//! # Sections
//!
//! - **Fields** — every column of the row: its name and type on one line,
//!   the value in a box under it.
//! - **REFERENCES** — the rows this one points at through its single-column
//!   foreign keys ("customers · id = 2129"), then the tables whose foreign
//!   keys point at it with the number of rows that do ("order_items · 3
//!   rows"). Those counts load in the background, one query per table,
//!   once the cursor rests on the row for `INCOMING_REFERENCES_DEBOUNCE`
//!   (see `IncomingReferencesLoader`).

use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Chamfer, Icon, LoadingState, Text};
use dbflux_components::tokens::{ChamferCut, ChromeColors, InspectorMetrics, SyntaxColors};
use dbflux_components::typography::AppFonts;
use dbflux_core::Value;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

// ---------------------------------------------------------------------------
// Incoming-reference loading
// ---------------------------------------------------------------------------

/// How long the cursor has to rest on a row before its incoming references
/// are looked up and counted.
pub(crate) const INCOMING_REFERENCES_DEBOUNCE: std::time::Duration =
    std::time::Duration::from_millis(250);

/// Most incoming-reference counts that run at the same time for one row.
pub(crate) const MAX_CONCURRENT_REFERENCE_COUNTS: usize = 4;

/// Schedules the incoming-reference lookup of the inspected row.
///
/// Every call replaces the previous lookup. Dropping its task cancels a
/// lookup still waiting out the debounce, so moving the cursor across many
/// rows runs one lookup for the row it stops on. A lookup that already
/// started stops before its next group of counts once another row opened
/// (see `count_incoming_references`).
#[derive(Default)]
pub(crate) struct IncomingReferencesLoader {
    task: Option<Task<()>>,
}

impl IncomingReferencesLoader {
    /// Runs `job` after `INCOMING_REFERENCES_DEBOUNCE`, unless another call
    /// or `cancel` comes first.
    pub(crate) fn schedule<Job>(&mut self, job: Job, cx: &mut App)
    where
        Job: AsyncFnOnce(&mut AsyncApp) + 'static,
    {
        self.task = Some(cx.spawn(async move |cx: &mut AsyncApp| {
            cx.background_executor()
                .timer(INCOMING_REFERENCES_DEBOUNCE)
                .await;
            job(cx).await;
        }));
    }

    /// Drops the pending or running lookup.
    pub(crate) fn cancel(&mut self) {
        self.task = None;
    }
}

/// Counts the rows behind each incoming reference of row `generation`,
/// `MAX_CONCURRENT_REFERENCE_COUNTS` at a time, and records every result at
/// its index (`first` is the index of `references[0]` in the inspector's
/// list). Stops before the next group once another row opened; counts of a
/// group already running still finish and are dropped as stale.
pub(crate) async fn count_incoming_references<Count>(
    content: &Entity<RowInspectorContent>,
    generation: u64,
    first: usize,
    references: &[FkReference],
    count: Count,
    cx: &mut AsyncApp,
) where
    Count: Fn(&FkReference, &BackgroundExecutor) -> Task<Result<u64, String>>,
{
    for (group_index, group) in references
        .chunks(MAX_CONCURRENT_REFERENCE_COUNTS)
        .enumerate()
    {
        let still_open = cx.update(|cx| content.read(cx).generation() == generation);
        if !still_open {
            return;
        }

        let executor = cx.background_executor().clone();
        let pending: Vec<Task<Result<u64, String>>> = group
            .iter()
            .map(|reference| count(reference, &executor))
            .collect();

        for (offset, task) in pending.into_iter().enumerate() {
            let result = task.await;
            let index = first + group_index * MAX_CONCURRENT_REFERENCE_COUNTS + offset;

            cx.update(|cx| {
                content.update(cx, |content, cx| {
                    content.resolve_count(generation, index, result, cx);
                })
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// A single column value pair captured from the selected row.
#[derive(Debug, Clone)]
pub struct InspectorCell {
    pub name: String,
    pub value: Value,
    /// The column type shown at the right of the name ("int8", or
    /// "int8 → customers" for a foreign key). Empty when unknown.
    pub type_label: String,
    pub is_primary_key: bool,
    pub is_foreign_key: bool,
}

/// All data the inspector needs to render without further async calls
/// (except the reference counts, which load lazily).
#[derive(Debug, Clone)]
pub struct InspectorSnapshot {
    /// One-based row number shown in the header.
    pub row_number: usize,
    /// The row's key expression, shown next to the row number.
    pub row_key: Option<String>,
    /// Column values for the row.
    pub cells: Vec<InspectorCell>,
    /// Whether the footer's Edit, Duplicate and Delete actions apply: the
    /// result is editable and the row is not grouped.
    pub can_edit: bool,
}

/// The row's key as an expression over its primary key columns,
/// "orders.id = 2", with the columns of a composite key joined by " · ".
/// `table` qualifies each column when the row comes from a known table.
/// `None` when the row has no primary key column.
pub fn row_key_label(table: Option<&str>, cells: &[InspectorCell]) -> Option<String> {
    let parts: Vec<String> = cells
        .iter()
        .filter(|cell| cell.is_primary_key)
        .map(|cell| {
            let value = cell.value.as_display_string_truncated(60);
            match table {
                Some(table) => format!("{table}.{} = {value}", cell.name),
                None => format!("{} = {value}", cell.name),
            }
        })
        .collect();

    (!parts.is_empty()).then(|| parts.join(" · "))
}

/// The type label of a column: its type, followed by the table a foreign key
/// on it points at.
pub fn column_type_label(type_name: &str, referenced_table: Option<&str>) -> String {
    match referenced_table {
        Some(table) if type_name.is_empty() => format!("→ {table}"),
        Some(table) => format!("{type_name} → {table}"),
        None => type_name.to_string(),
    }
}

/// The inspected row as a JSON object of its columns, for the copy button.
fn row_json(cells: &[InspectorCell]) -> String {
    let object: serde_json::Map<String, serde_json::Value> = cells
        .iter()
        .map(|cell| (cell.name.clone(), Value::to_serde_json(&cell.value)))
        .collect();

    serde_json::to_string_pretty(&serde_json::Value::Object(object))
        .unwrap_or_else(|error| format!("{{\"error\": \"{error}\"}}"))
}

// ---------------------------------------------------------------------------
// References
// ---------------------------------------------------------------------------

/// Which way a reference runs from the inspected row.
#[derive(Debug, Clone, PartialEq)]
pub enum ReferenceKind {
    /// A foreign key of this row: its `column` holds `value`, which is the
    /// `target_column` of one row of the target table.
    Outgoing,
    /// A foreign key of the target table on `column` that points at this
    /// row's `target_column`; `count` is how many of its rows hold `value`.
    Incoming { count: LoadingState<u64> },
}

/// One entry of the REFERENCES section.
#[derive(Debug, Clone)]
pub struct FkReference {
    /// The foreign key column: of this row for an outgoing reference, of the
    /// target table for an incoming one.
    pub column: String,
    /// Schema of the target table, if known.
    pub target_schema: Option<String>,
    /// The table at the other end.
    pub target_table: String,
    /// The referenced key column: of the target table for an outgoing
    /// reference, of this row's table for an incoming one.
    pub target_pk: String,
    /// The key value that links the two rows.
    pub value: Value,
    pub kind: ReferenceKind,
}

impl FkReference {
    /// The referenced table, schema-qualified when the schema is known.
    pub fn qualified_target(&self) -> String {
        match &self.target_schema {
            Some(schema) => format!("{}.{}", schema, self.target_table),
            None => self.target_table.clone(),
        }
    }

    /// The text at the right of the reference: the key an outgoing
    /// reference matches ("id = 2129"), or how many rows an incoming one
    /// has ("3 rows"); `None` while that count loads.
    pub fn detail(&self) -> Option<String> {
        match &self.kind {
            ReferenceKind::Outgoing => Some(format!(
                "{} = {}",
                self.target_pk,
                self.value.as_display_string_truncated(40)
            )),
            ReferenceKind::Incoming {
                count: LoadingState::Loaded(count),
            } => Some(crate::labels::row_count_label(*count as usize)),
            ReferenceKind::Incoming {
                count: LoadingState::Failed { .. },
            } => Some("—".to_string()),
            ReferenceKind::Incoming { .. } => None,
        }
    }
}

/// The incoming references of a row of `table` (in `schema`): one per
/// single-column foreign key in `foreign_keys` that points at the table,
/// linked through the row's value of the referenced column, which must not
/// be null. `values` holds the row's cells by column name. Counts start
/// loading.
pub fn incoming_references(
    foreign_keys: &[dbflux_core::SchemaForeignKeyInfo],
    table: &str,
    schema: Option<&str>,
    values: &std::collections::HashMap<String, Value>,
) -> Vec<FkReference> {
    foreign_keys
        .iter()
        .filter(|fk| fk.referenced_table == table)
        .filter(|fk| {
            fk.referenced_schema
                .as_deref()
                .is_none_or(|referenced| Some(referenced) == schema)
        })
        .filter_map(|fk| {
            let ([column], [referenced_column]) =
                (fk.columns.as_slice(), fk.referenced_columns.as_slice())
            else {
                return None;
            };

            let value = values
                .get(referenced_column)
                .filter(|value| !value.is_null())?;

            Some(FkReference {
                column: column.clone(),
                target_schema: None,
                target_table: fk.table_name.clone(),
                target_pk: referenced_column.clone(),
                value: value.clone(),
                kind: ReferenceKind::Incoming {
                    count: LoadingState::Loading,
                },
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Section helpers
// ---------------------------------------------------------------------------

fn render_section_label(
    label: impl Into<SharedString>,
    padding_top: Pixels,
    padding_bottom: Pixels,
    theme: &gpui_component::theme::Theme,
) -> impl IntoElement {
    div()
        .px(InspectorMetrics::PADDING_X)
        .pt(padding_top)
        .pb(padding_bottom)
        .child(Text::label(label.into()).color(theme.muted_foreground))
}

/// One field (IslTable): the column name with its type at the right end,
/// then the value in a box on the ground, in the data face; NULL in the null
/// colour and italic, as in the grid.
fn render_row_entry(
    cell: &InspectorCell,
    null_color: Hsla,
    theme: &gpui_component::theme::Theme,
    cx: &App,
) -> impl IntoElement {
    let is_null = cell.value.is_null();
    let value_text = cell.value.as_display_string_truncated(200);

    div()
        .flex()
        .flex_col()
        .gap(InspectorMetrics::FIELD_GAP)
        .px(InspectorMetrics::PADDING_X)
        .py(InspectorMetrics::FIELD_PADDING_Y)
        .child(
            div()
                .flex()
                .items_center()
                .justify_between()
                .gap(InspectorMetrics::FIELD_LABEL_GAP)
                .min_w_0()
                .h(InspectorMetrics::FIELD_LABEL_LINE_HEIGHT)
                .text_size(InspectorMetrics::FIELD_LABEL_FONT)
                .line_height(InspectorMetrics::FIELD_LABEL_LINE_HEIGHT)
                .text_color(theme.muted_foreground)
                .child(div().min_w_0().truncate().child(cell.name.clone()))
                .when(!cell.type_label.is_empty(), |line| {
                    line.child(
                        div()
                            .flex_shrink_0()
                            .font_family(dbflux_components::fonts::editor_family(cx))
                            .child(cell.type_label.clone()),
                    )
                }),
        )
        .child(
            div()
                .relative()
                .min_w_0()
                .px(InspectorMetrics::FIELD_VALUE_PADDING_X)
                .py(InspectorMetrics::FIELD_VALUE_PADDING_Y)
                .child(Chamfer::new(ChamferCut::KEYCAP).fill(theme.background))
                .child(
                    div()
                        .min_w_0()
                        .truncate()
                        .font_family(dbflux_components::fonts::editor_family(cx))
                        .text_size(InspectorMetrics::FIELD_VALUE_FONT)
                        .line_height(InspectorMetrics::FIELD_VALUE_LINE_HEIGHT)
                        .text_color(if is_null {
                            null_color
                        } else {
                            ChromeColors::strong(theme)
                        })
                        .when(is_null, |value| value.italic())
                        .child(value_text),
                ),
        )
}

fn render_references_section(
    references: &[FkReference],
    references_ready: bool,
    loading_label: &str,
    theme: &gpui_component::theme::Theme,
    cx: &App,
) -> impl IntoElement {
    div()
        .flex()
        .flex_col()
        .children(
            references
                .iter()
                .enumerate()
                .map(|(index, reference)| render_fk_reference_entry(index, reference, theme, cx)),
        )
        .when(!references_ready, |section| {
            section.child(
                div()
                    .flex()
                    .items_center()
                    .gap(InspectorMetrics::FIELD_LABEL_GAP)
                    .px(InspectorMetrics::PADDING_X)
                    .child(
                        Icon::new(AppIcon::Loader)
                            .size(InspectorMetrics::FIELD_ICON)
                            .color(theme.muted_foreground),
                    )
                    .child(Text::caption(loading_label.to_string()).color(theme.muted_foreground)),
            )
        })
}

/// One reference row (IslTable): the link icon of an outgoing key or the
/// table icon of an incoming one, the target table in mono, its key or row
/// count at the right, and a chevron.
fn render_fk_reference_entry(
    index: usize,
    reference: &FkReference,
    theme: &gpui_component::theme::Theme,
    cx: &App,
) -> impl IntoElement {
    let icon = match reference.kind {
        ReferenceKind::Outgoing => AppIcon::Cable,
        ReferenceKind::Incoming { .. } => AppIcon::Rows3,
    };

    div()
        .id(("row-inspector-reference", index))
        .relative()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(InspectorMetrics::REFERENCE_GAP)
        .h(InspectorMetrics::REFERENCE_HEIGHT)
        .mx(InspectorMetrics::REFERENCE_MARGIN_X)
        .mb(InspectorMetrics::REFERENCE_MARGIN_BOTTOM)
        .px(InspectorMetrics::REFERENCE_PADDING_X)
        .text_size(InspectorMetrics::FIELD_VALUE_FONT)
        .child(Chamfer::new(ChamferCut::CONTROL).fill(theme.secondary))
        .child(
            Icon::new(icon)
                .size(InspectorMetrics::REFERENCE_ICON)
                .color(theme.info),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .truncate()
                .font_family(dbflux_components::fonts::editor_family(cx))
                .text_color(ChromeColors::strong(theme))
                .child(reference.qualified_target()),
        )
        .when_some(reference.detail(), |row, detail| {
            row.child(
                div()
                    .flex_shrink_0()
                    .text_color(theme.muted_foreground)
                    .child(detail),
            )
        })
        .child(
            Icon::new(AppIcon::ChevronRight)
                .size(InspectorMetrics::REFERENCE_CHEVRON)
                .color(theme.muted_foreground),
        )
}

// ---------------------------------------------------------------------------
// RowInspectorContent entity
// ---------------------------------------------------------------------------

/// The row inspector panel mounted in the workspace inspector rail.
pub struct RowInspectorContent {
    snapshot: InspectorSnapshot,
    references: Vec<FkReference>,
    references_ready: bool,
    /// Advances every time another row opens, so a reference count that
    /// arrives for a previous row is dropped instead of landing on this one.
    generation: u64,
    pinned: bool,
    focus_handle: FocusHandle,
    /// Scroll position of the field list, moved by the keys while the
    /// keyboard is in the inspector.
    scroll_handle: ScrollHandle,
}

/// Requests from the inspector's buttons; the owning grid carries them out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowInspectorContentEvent {
    /// The close button: dismiss the inspector.
    Close,
    /// The pin button: stop or resume following the grid selection.
    TogglePin,
    /// Edit the inspected row.
    Edit,
    /// Duplicate the inspected row.
    Duplicate,
    /// Delete the inspected row.
    Delete,
}

impl EventEmitter<RowInspectorContentEvent> for RowInspectorContent {}

impl RowInspectorContent {
    pub fn new(snapshot: InspectorSnapshot, cx: &mut Context<Self>) -> Self {
        Self {
            snapshot,
            references: Vec::new(),
            references_ready: false,
            generation: 0,
            pinned: false,
            focus_handle: cx.focus_handle(),
            scroll_handle: ScrollHandle::new(),
        }
    }

    /// Scroll the field list by a line, a page, or to either end.
    pub fn scroll(
        &self,
        step: crate::data_grid_panel::side_island::IslandScroll,
        cx: &mut Context<Self>,
    ) {
        crate::data_grid_panel::side_island::scroll_by(&self.scroll_handle, step, cx);
        cx.notify();
    }

    /// Replace the snapshot for a new row selection while keeping the entity alive.
    pub fn open(&mut self, snapshot: InspectorSnapshot, cx: &mut Context<Self>) {
        self.snapshot = snapshot;
        self.references = Vec::new();
        self.references_ready = false;
        self.generation += 1;
        cx.notify();
    }

    /// Identifies the row currently open, for references that load later.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Show the pin button as pressed (`true`) or released.
    pub fn set_pinned(&mut self, pinned: bool, cx: &mut Context<Self>) {
        if self.pinned != pinned {
            self.pinned = pinned;
            cx.notify();
        }
    }

    /// Set the complete reference list.
    #[cfg(test)]
    pub fn set_references(&mut self, references: Vec<FkReference>, cx: &mut Context<Self>) {
        self.references = references;
        self.references_ready = true;
        cx.notify();
    }

    /// Show the outgoing references while the incoming ones still load.
    pub fn set_outgoing_references(
        &mut self,
        references: Vec<FkReference>,
        cx: &mut Context<Self>,
    ) {
        self.references = references;
        self.references_ready = false;
        cx.notify();
    }

    /// Append the incoming references found for row `generation` and mark
    /// the list complete. Returns the index of the first one appended, or
    /// `None` when another row opened in the meantime.
    pub fn add_incoming_references(
        &mut self,
        generation: u64,
        references: Vec<FkReference>,
        cx: &mut Context<Self>,
    ) -> Option<usize> {
        if generation != self.generation {
            return None;
        }

        let first = self.references.len();
        self.references.extend(references);
        self.references_ready = true;
        cx.notify();

        Some(first)
    }

    /// Record how many rows point at row `generation` through the incoming
    /// reference at `index`. Ignored for another row or another kind.
    pub fn resolve_count(
        &mut self,
        generation: u64,
        index: usize,
        result: Result<u64, String>,
        cx: &mut Context<Self>,
    ) {
        if generation != self.generation {
            return;
        }

        let Some(reference) = self.references.get_mut(index) else {
            return;
        };

        if let ReferenceKind::Incoming { count } = &mut reference.kind {
            *count = match result {
                Ok(rows) => LoadingState::Loaded(rows),
                Err(message) => LoadingState::Failed {
                    message: message.into(),
                },
            };
            cx.notify();
        }
    }

    /// Whether the references list has been populated (even if empty).
    #[cfg(test)]
    pub fn references_ready(&self) -> bool {
        self.references_ready
    }

    /// Number of FK references.
    #[cfg(test)]
    pub fn references_len(&self) -> usize {
        self.references.len()
    }

    #[cfg(test)]
    pub fn is_pinned(&self) -> bool {
        self.pinned
    }

    fn render_header(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let title = crate::labels::row_inspector_title(self.snapshot.row_number);
        let pin_label = if self.pinned {
            dbflux_i18n::t!("document.data.row_inspector.action.unpin")
        } else {
            dbflux_i18n::t!("document.data.row_inspector.action.pin")
        };

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(InspectorMetrics::HEADER_GAP)
            .h(InspectorMetrics::HEADER_HEIGHT)
            .pl(InspectorMetrics::HEADER_PADDING_LEFT)
            .pr(InspectorMetrics::HEADER_PADDING_RIGHT)
            .child(
                Icon::new(AppIcon::Rows3)
                    .size(InspectorMetrics::HEADER_ICON)
                    .color(ChromeColors::tint(theme)),
            )
            .child(
                div().flex_shrink_0().child(
                    Text::body(title)
                        .color(ChromeColors::strong(theme))
                        .font_weight(FontWeight::BOLD),
                ),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .font_family(dbflux_components::fonts::editor_family(cx))
                    .text_size(InspectorMetrics::KEY_FONT)
                    .text_color(theme.muted_foreground)
                    .when_some(self.snapshot.row_key.clone(), |key, text| key.child(text)),
            )
            .child(
                Button::new(
                    "row-inspector-copy",
                    dbflux_i18n::t!("document.data.row_inspector.action.copy"),
                )
                .icon(AppIcon::Copy)
                .icon_only()
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| {
                    cx.write_to_clipboard(ClipboardItem::new_string(row_json(
                        &this.snapshot.cells,
                    )));
                })),
            )
            .child(
                Button::new("row-inspector-pin", pin_label)
                    .icon(AppIcon::Pin)
                    .icon_only()
                    .selected(self.pinned)
                    .tab_stop(false)
                    .on_click(cx.listener(|_, _, _, cx| {
                        cx.emit(RowInspectorContentEvent::TogglePin);
                    })),
            )
            .child(
                Button::new(
                    "row-inspector-close",
                    dbflux_i18n::t!("document.data.row_inspector.action.close"),
                )
                .icon(AppIcon::CircleX)
                .icon_only()
                .tab_stop(false)
                .on_click(cx.listener(|_, _, _, cx| {
                    cx.emit(RowInspectorContentEvent::Close);
                })),
            )
            .into_any_element()
    }

    /// Edit and Duplicate at the left, Delete at the right, all secondary
    /// (IslTable); Delete still asks before it stages the removal.
    fn render_footer(&self, cx: &mut Context<Self>) -> AnyElement {
        let can_edit = self.snapshot.can_edit;

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(InspectorMetrics::FOOTER_GAP)
            .h(InspectorMetrics::FOOTER_HEIGHT)
            .pl(InspectorMetrics::FOOTER_PADDING_LEFT)
            .pr(InspectorMetrics::FOOTER_PADDING_RIGHT)
            .child(
                Button::new(
                    "row-inspector-edit",
                    dbflux_i18n::t!("document.data.row_inspector.action.edit"),
                )
                .icon(AppIcon::Pencil)
                .disabled(!can_edit)
                .tab_stop(false)
                .on_click(cx.listener(|_, _, _, cx| {
                    cx.emit(RowInspectorContentEvent::Edit);
                })),
            )
            .child(
                Button::new(
                    "row-inspector-duplicate",
                    dbflux_i18n::t!("document.data.row_inspector.action.duplicate"),
                )
                .icon(AppIcon::Copy)
                .disabled(!can_edit)
                .tab_stop(false)
                .on_click(cx.listener(|_, _, _, cx| {
                    cx.emit(RowInspectorContentEvent::Duplicate);
                })),
            )
            .child(div().flex_1())
            .child(
                Button::new(
                    "row-inspector-delete",
                    dbflux_i18n::t!("document.data.row_inspector.action.delete"),
                )
                .icon(AppIcon::Delete)
                .disabled(!can_edit)
                .tab_stop(false)
                .on_click(cx.listener(|_, _, _, cx| {
                    cx.emit(RowInspectorContentEvent::Delete);
                })),
            )
            .into_any_element()
    }
}

impl Focusable for RowInspectorContent {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for RowInspectorContent {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let header = self.render_header(cx);
        let footer = self.render_footer(cx);

        let null_color = SyntaxColors::for_current(cx).number;
        let theme = cx.theme();
        let has_fk = self.snapshot.cells.iter().any(|cell| cell.is_foreign_key);
        let shows_references = !self.references.is_empty() || (!self.references_ready && has_fk);
        let loading_label = dbflux_i18n::t!("document.data.row_inspector.references.loading");

        let body = div()
            .id("row-inspector-body")
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .overflow_y_scroll()
            .track_scroll(&self.scroll_handle)
            .children(
                self.snapshot
                    .cells
                    .iter()
                    .map(|cell| render_row_entry(cell, null_color, theme, cx)),
            )
            .when(shows_references, |body| {
                body.child(render_section_label(
                    dbflux_i18n::t!("document.data.row_inspector.section.references"),
                    InspectorMetrics::REFERENCES_LABEL_PADDING_TOP,
                    InspectorMetrics::REFERENCES_LABEL_PADDING_BOTTOM,
                    theme,
                ))
                .child(render_references_section(
                    &self.references,
                    self.references_ready,
                    &loading_label,
                    theme,
                    cx,
                ))
            });

        div()
            .id("row-inspector-content")
            .size_full()
            .flex()
            .flex_col()
            .bg(theme.popover)
            .track_focus(&self.focus_handle)
            .child(header)
            .child(body)
            .child(footer)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::{
        FkReference, INCOMING_REFERENCES_DEBOUNCE, IncomingReferencesLoader, InspectorCell,
        InspectorSnapshot, MAX_CONCURRENT_REFERENCE_COUNTS, ReferenceKind, RowInspectorContent,
        column_type_label, count_incoming_references, incoming_references, row_json, row_key_label,
    };
    use dbflux_components::primitives::LoadingState;
    use dbflux_core::Value;
    use gpui::{AppContext as _, TestAppContext};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn cell(name: &str, value: Value, is_primary_key: bool) -> InspectorCell {
        InspectorCell {
            name: name.to_string(),
            value,
            type_label: String::new(),
            is_primary_key,
            is_foreign_key: false,
        }
    }

    fn make_snapshot() -> InspectorSnapshot {
        let cells = vec![cell("id", Value::Int(1), true)];

        InspectorSnapshot {
            row_number: 1,
            row_key: row_key_label(None, &cells),
            cells,
            can_edit: true,
        }
    }

    fn incoming(table: &str) -> FkReference {
        FkReference {
            column: "order_id".to_string(),
            target_schema: None,
            target_table: table.to_string(),
            target_pk: "id".to_string(),
            value: Value::Int(2),
            kind: ReferenceKind::Incoming {
                count: LoadingState::Loading,
            },
        }
    }

    #[test]
    fn row_key_label_names_the_key_columns_of_the_table() {
        let cells = vec![
            cell("tenant", Value::Text("acme".to_string()), true),
            cell("name", Value::Text("Alice".to_string()), false),
            cell("id", Value::Int(7), true),
        ];

        assert_eq!(
            row_key_label(Some("orders"), &cells).as_deref(),
            Some("orders.tenant = acme · orders.id = 7")
        );
        assert_eq!(
            row_key_label(None, &cells).as_deref(),
            Some("tenant = acme · id = 7")
        );
    }

    #[test]
    fn row_key_label_is_none_without_a_primary_key() {
        let cells = vec![cell("name", Value::Text("Alice".to_string()), false)];

        assert_eq!(row_key_label(Some("users"), &cells), None);
    }

    #[test]
    fn column_type_label_points_a_foreign_key_at_its_table() {
        assert_eq!(column_type_label("int8", None), "int8");
        assert_eq!(
            column_type_label("int8", Some("customers")),
            "int8 → customers"
        );
        assert_eq!(column_type_label("", Some("customers")), "→ customers");
    }

    #[test]
    fn row_json_keeps_every_column() {
        let cells = vec![
            cell("id", Value::Int(2), true),
            cell("note", Value::Null, false),
        ];

        let json: serde_json::Value =
            serde_json::from_str(&row_json(&cells)).expect("the copied row is valid JSON");

        assert_eq!(json["id"], serde_json::json!(2));
        assert_eq!(json["note"], serde_json::Value::Null);
    }

    #[test]
    fn qualified_target_prefixes_the_schema_when_known() {
        let mut reference = FkReference {
            column: "user_id".to_string(),
            target_schema: Some("public".to_string()),
            target_table: "users".to_string(),
            target_pk: "id".to_string(),
            value: Value::Int(1),
            kind: ReferenceKind::Outgoing,
        };

        assert_eq!(reference.qualified_target(), "public.users");

        reference.target_schema = None;
        assert_eq!(reference.qualified_target(), "users");
    }

    #[test]
    fn reference_detail_shows_the_key_or_the_row_count() {
        let outgoing = FkReference {
            column: "customer_id".to_string(),
            target_schema: None,
            target_table: "customers".to_string(),
            target_pk: "id".to_string(),
            value: Value::Int(2129),
            kind: ReferenceKind::Outgoing,
        };
        assert_eq!(outgoing.detail().as_deref(), Some("id = 2129"));

        let mut reference = incoming("order_items");
        assert_eq!(reference.detail(), None, "no count while it loads");

        reference.kind = ReferenceKind::Incoming {
            count: LoadingState::Loaded(3),
        };
        assert_eq!(reference.detail().as_deref(), Some("3 rows"));

        reference.kind = ReferenceKind::Incoming {
            count: LoadingState::Loaded(1),
        };
        assert_eq!(reference.detail().as_deref(), Some("1 row"));
    }

    fn foreign_key(
        table: &str,
        columns: &[&str],
        referenced_table: &str,
        referenced_columns: &[&str],
    ) -> dbflux_core::SchemaForeignKeyInfo {
        dbflux_core::SchemaForeignKeyInfo {
            name: format!("{table}_fk"),
            table_name: table.to_string(),
            columns: columns.iter().map(|column| column.to_string()).collect(),
            referenced_schema: Some("public".to_string()),
            referenced_table: referenced_table.to_string(),
            referenced_columns: referenced_columns
                .iter()
                .map(|column| column.to_string())
                .collect(),
            on_delete: None,
            on_update: None,
        }
    }

    #[test]
    fn incoming_references_are_the_single_column_keys_pointing_at_the_table() {
        let foreign_keys = vec![
            foreign_key("order_items", &["order_id"], "orders", &["id"]),
            foreign_key("payments", &["order_id"], "orders", &["id"]),
            foreign_key("orders", &["customer_id"], "customers", &["id"]),
            foreign_key(
                "shipments",
                &["order_id", "tenant"],
                "orders",
                &["id", "tenant"],
            ),
        ];
        let values = [("id".to_string(), Value::Int(2))].into_iter().collect();

        let references = incoming_references(&foreign_keys, "orders", Some("public"), &values);

        let tables: Vec<&str> = references
            .iter()
            .map(|reference| reference.target_table.as_str())
            .collect();
        assert_eq!(tables, vec!["order_items", "payments"]);
        assert_eq!(references[0].column, "order_id");
        assert_eq!(references[0].value, Value::Int(2));
    }

    #[test]
    fn a_null_key_has_no_incoming_references() {
        let foreign_keys = vec![foreign_key("order_items", &["order_id"], "orders", &["id"])];
        let values = [("id".to_string(), Value::Null)].into_iter().collect();

        assert!(incoming_references(&foreign_keys, "orders", Some("public"), &values).is_empty());
    }

    #[gpui::test]
    fn row_inspector_content_open_updates_snapshot(cx: &mut TestAppContext) {
        let entity = cx.new(|cx| RowInspectorContent::new(make_snapshot(), cx));

        let new_snap = InspectorSnapshot {
            row_number: 4,
            row_key: None,
            cells: vec![cell("name", Value::Text("Alice".to_string()), false)],
            can_edit: false,
        };

        cx.update(|cx| {
            entity.update(cx, |content, cx| {
                content.open(new_snap, cx);
            });
        });

        cx.read(|cx| {
            let content = entity.read(cx);
            assert_eq!(content.snapshot.cells[0].name, "name");
            assert_eq!(content.snapshot.row_number, 4);
            assert!(!content.references_ready(), "open resets references_ready");
        });
    }

    #[gpui::test]
    fn row_inspector_content_pin_state_survives_open(cx: &mut TestAppContext) {
        let entity = cx.new(|cx| RowInspectorContent::new(make_snapshot(), cx));

        cx.update(|cx| {
            entity.update(cx, |content, cx| {
                content.set_pinned(true, cx);
                content.open(make_snapshot(), cx);
            });
        });

        cx.read(|cx| assert!(entity.read(cx).is_pinned()));
    }

    #[gpui::test]
    fn row_inspector_content_set_references(cx: &mut TestAppContext) {
        let entity = cx.new(|cx| RowInspectorContent::new(make_snapshot(), cx));

        cx.read(|cx| {
            assert!(!entity.read(cx).references_ready());
        });

        cx.update(|cx| {
            entity.update(cx, |content, cx| {
                content.set_references(vec![incoming("order_items")], cx);
            });
        });

        cx.read(|cx| {
            let content = entity.read(cx);
            assert!(content.references_ready());
            assert_eq!(content.references_len(), 1);
        });
    }

    #[gpui::test]
    fn incoming_references_follow_the_outgoing_ones_and_take_their_counts(cx: &mut TestAppContext) {
        let entity = cx.new(|cx| RowInspectorContent::new(make_snapshot(), cx));

        cx.update(|cx| {
            entity.update(cx, |content, cx| {
                let generation = content.generation();
                let first = content
                    .add_incoming_references(
                        generation,
                        vec![incoming("order_items"), incoming("payments")],
                        cx,
                    )
                    .expect("the row is still open");

                content.resolve_count(generation, first + 1, Ok(1), cx);
            });
        });

        cx.read(|cx| {
            let content = entity.read(cx);
            assert!(content.references_ready());
            assert_eq!(content.references[0].detail(), None);
            assert_eq!(content.references[1].detail().as_deref(), Some("1 row"));
        });
    }

    #[gpui::test]
    fn counts_for_a_row_that_is_no_longer_open_are_dropped(cx: &mut TestAppContext) {
        let entity = cx.new(|cx| RowInspectorContent::new(make_snapshot(), cx));

        cx.update(|cx| {
            entity.update(cx, |content, cx| {
                let stale = content.generation();
                content.open(make_snapshot(), cx);

                let current = content.generation();
                assert_eq!(
                    content.add_incoming_references(stale, vec![incoming("orders")], cx),
                    None
                );

                content.add_incoming_references(current, vec![incoming("orders")], cx);
                content.resolve_count(stale, 0, Ok(5), cx);
            });
        });

        cx.read(|cx| {
            let content = entity.read(cx);
            assert_eq!(content.references_len(), 1);
            assert_eq!(content.references[0].detail(), None);
        });
    }

    #[gpui::test]
    fn rapid_cursor_moves_run_one_lookup_for_the_row_the_cursor_stops_on(cx: &mut TestAppContext) {
        let runs: Rc<RefCell<Vec<usize>>> = Rc::default();
        let mut loader = IncomingReferencesLoader::default();

        for row in 0..5 {
            cx.update(|cx| {
                loader.schedule(
                    {
                        let runs = runs.clone();
                        async move |_cx| runs.borrow_mut().push(row)
                    },
                    cx,
                );
            });
            cx.executor()
                .advance_clock(INCOMING_REFERENCES_DEBOUNCE / 2);
            cx.run_until_parked();
        }

        assert!(
            runs.borrow().is_empty(),
            "no lookup starts while the cursor moves"
        );

        cx.executor().advance_clock(INCOMING_REFERENCES_DEBOUNCE);
        cx.run_until_parked();

        assert_eq!(*runs.borrow(), vec![4]);
    }

    #[gpui::test]
    fn cancel_drops_a_pending_lookup(cx: &mut TestAppContext) {
        let runs: Rc<RefCell<usize>> = Rc::default();
        let mut loader = IncomingReferencesLoader::default();

        cx.update(|cx| {
            loader.schedule(
                {
                    let runs = runs.clone();
                    async move |_cx| *runs.borrow_mut() += 1
                },
                cx,
            );
        });
        loader.cancel();

        cx.executor()
            .advance_clock(INCOMING_REFERENCES_DEBOUNCE * 2);
        cx.run_until_parked();

        assert_eq!(*runs.borrow(), 0);
    }

    /// Opens a row with `reference_count` incoming references and starts
    /// counting them; every count takes one second. Returns the content, the
    /// row's generation and how many counts have started.
    fn start_counting(
        reference_count: usize,
        cx: &mut TestAppContext,
    ) -> (gpui::Entity<RowInspectorContent>, u64, Arc<AtomicUsize>) {
        let entity = cx.new(|cx| RowInspectorContent::new(make_snapshot(), cx));
        let references: Vec<FkReference> = (0..reference_count)
            .map(|index| incoming(&format!("table_{index}")))
            .collect();

        let (generation, first) = cx.update(|cx| {
            entity.update(cx, |content, cx| {
                let generation = content.generation();
                let first = content
                    .add_incoming_references(generation, references.clone(), cx)
                    .expect("the row is still open");
                (generation, first)
            })
        });

        let started = Arc::new(AtomicUsize::new(0));

        cx.spawn({
            let entity = entity.clone();
            let started = started.clone();
            move |mut cx| async move {
                count_incoming_references(
                    &entity,
                    generation,
                    first,
                    &references,
                    move |_reference, executor| {
                        started.fetch_add(1, Ordering::SeqCst);
                        let timer = executor.timer(Duration::from_secs(1));
                        executor.spawn(async move {
                            timer.await;
                            Ok(3)
                        })
                    },
                    &mut cx,
                )
                .await;
            }
        })
        .detach();

        cx.run_until_parked();

        (entity, generation, started)
    }

    #[gpui::test]
    fn counts_run_in_capped_groups_until_every_reference_resolves(cx: &mut TestAppContext) {
        let reference_count = MAX_CONCURRENT_REFERENCE_COUNTS + 2;
        let (entity, _generation, started) = start_counting(reference_count, cx);

        assert_eq!(
            started.load(Ordering::SeqCst),
            MAX_CONCURRENT_REFERENCE_COUNTS
        );

        cx.executor().advance_clock(Duration::from_secs(1));
        cx.run_until_parked();
        assert_eq!(started.load(Ordering::SeqCst), reference_count);

        cx.executor().advance_clock(Duration::from_secs(1));
        cx.run_until_parked();

        cx.read(|cx| {
            let content = entity.read(cx);
            for reference in &content.references {
                assert_eq!(reference.detail().as_deref(), Some("3 rows"));
            }
        });
    }

    #[gpui::test]
    fn counting_stops_once_another_row_opens(cx: &mut TestAppContext) {
        let (entity, _generation, started) =
            start_counting(MAX_CONCURRENT_REFERENCE_COUNTS * 3, cx);

        cx.update(|cx| {
            entity.update(cx, |content, cx| content.open(make_snapshot(), cx));
        });

        cx.executor().advance_clock(Duration::from_secs(5));
        cx.run_until_parked();

        assert_eq!(
            started.load(Ordering::SeqCst),
            MAX_CONCURRENT_REFERENCE_COUNTS,
            "no count starts for a row that is no longer open"
        );
    }

    #[test]
    fn row_inspector_keys_resolve_in_both_locales() {
        let keys = [
            "document.data.row_inspector.action.copy",
            "document.data.row_inspector.action.pin",
            "document.data.row_inspector.action.unpin",
            "document.data.row_inspector.action.close",
            "document.data.row_inspector.action.edit",
            "document.data.row_inspector.action.duplicate",
            "document.data.row_inspector.action.delete",
            "document.data.row_inspector.references.empty",
            "document.data.row_inspector.references.loading",
            "document.data.row_inspector.references.not_found",
            "document.data.row_inspector.references.resolving",
            "document.data.row_inspector.section.references",
            "document.data.row_inspector.section.row",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn row_inspector_title_differs_between_locales() {
        let en = dbflux_i18n::t!("document.data.row_inspector.section.row", locale = "en");
        let es = dbflux_i18n::t!("document.data.row_inspector.section.row", locale = "es");

        assert_eq!(en, "ROW");
        assert_ne!(en, es);
    }

    #[test]
    fn row_inspector_pin_labels_differ_between_locales() {
        let en_pin = dbflux_i18n::t!("document.data.row_inspector.action.pin", locale = "en");
        let es_pin = dbflux_i18n::t!("document.data.row_inspector.action.pin", locale = "es");

        assert_eq!(en_pin, "Pin this row");
        assert_ne!(en_pin, es_pin);
    }

    #[test]
    fn row_inspector_render_functions_hoist_translations_out_of_per_row_closures() {
        let source = include_str!("row_inspector.rs");

        for function_name in ["fn render_row_entry(", "fn render_fk_reference_entry("] {
            let start = source
                .find(function_name)
                .unwrap_or_else(|| panic!("{function_name} not found in row_inspector.rs"));
            let after_signature = &source[start + function_name.len()..];
            let end = after_signature
                .find("\n}\n")
                .unwrap_or(after_signature.len());
            let body = &after_signature[..end];

            assert!(
                !body.contains("dbflux_i18n::t!("),
                "{function_name} must not call t! per row; hoist the translated label \
                 before the closure that invokes it"
            );
        }
    }
}
