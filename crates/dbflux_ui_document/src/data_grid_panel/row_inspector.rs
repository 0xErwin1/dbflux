//! Row Inspector content for the workspace-level inspector rail.
//!
//! # `RowInspectorContent`
//!
//! The row inspector draws its whole panel (AppByzTable, DSAppPlan
//! "RowInspector"): a header with the row number, its key and the pin and
//! close buttons, the scrollable ROW and REFERENCES sections, and a footer
//! with the row actions. The workspace rail only hosts it and owns the resize
//! grip; it skips its own title bar for this content.
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
//! - **ROW** — every column name / value pair of the row, with the key icons
//!   the table metadata gives the primary and foreign key columns.
//! - **REFERENCES** — one card per single-column foreign key, naming the
//!   referenced table; the referenced row resolves asynchronously.

use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Chamfer, Icon, LoadingState, Text};
use dbflux_components::tokens::{ChamferCut, ChromeColors, InspectorMetrics, SyntaxColors};
use dbflux_components::typography::AppFonts;
use dbflux_core::Value;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// A single column value pair captured from the selected row.
#[derive(Debug, Clone)]
pub struct InspectorCell {
    pub name: String,
    pub value: Value,
    pub is_primary_key: bool,
    pub is_foreign_key: bool,
}

/// All data the inspector needs to render without further async calls
/// (except FK reference resolution which is done lazily).
#[derive(Debug, Clone)]
pub struct InspectorSnapshot {
    /// One-based row number shown in the header.
    pub row_number: usize,
    /// The row's primary key value(s), shown next to the row number.
    pub row_key: Option<String>,
    /// Column values for the row.
    pub cells: Vec<InspectorCell>,
    /// Whether the footer's Edit, Duplicate and Delete actions apply: the
    /// result is editable and the row is not grouped.
    pub can_edit: bool,
}

/// The primary key value(s) of `cells`, joined in column order, or `None`
/// when the row has no primary key column.
pub fn row_key_label(cells: &[InspectorCell]) -> Option<String> {
    let parts: Vec<String> = cells
        .iter()
        .filter(|cell| cell.is_primary_key)
        .map(|cell| cell.value.as_display_string_truncated(60))
        .collect();

    (!parts.is_empty()).then(|| parts.join(", "))
}

// ---------------------------------------------------------------------------
// FK reference — per-FK async resolution state
// ---------------------------------------------------------------------------

/// Describes one FK reference and its async resolution state.
#[derive(Debug, Clone)]
pub struct FkReference {
    /// FK column name in the current row (e.g. "user_id").
    pub column: String,
    /// Schema of the referenced table (e.g. "public"), if known.
    pub target_schema: Option<String>,
    /// Name of the referenced table (e.g. "users").
    pub target_table: String,
    /// PK column in the referenced table (e.g. "id").
    pub target_pk: String,
    /// FK value from the current row.
    pub value: Value,
    /// Async resolution state for the referenced row.
    pub row: LoadingState<HashMap<String, Value>>,
}

impl FkReference {
    /// The referenced table, schema-qualified when the schema is known.
    pub fn qualified_target(&self) -> String {
        match &self.target_schema {
            Some(schema) => format!("{}.{}", schema, self.target_table),
            None => self.target_table.clone(),
        }
    }
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

/// The key icon of a field label: the warning-toned key for a primary key,
/// the info-toned cable for a foreign key, or an empty slot of the same width
/// so every name lines up.
fn render_key_icon(
    is_primary_key: bool,
    is_foreign_key: bool,
    size: Pixels,
    theme: &gpui_component::theme::Theme,
) -> AnyElement {
    if is_primary_key {
        Icon::new(AppIcon::KeyRound)
            .size(size)
            .color(theme.warning)
            .into_any_element()
    } else if is_foreign_key {
        Icon::new(AppIcon::Cable)
            .size(size)
            .color(theme.info)
            .into_any_element()
    } else {
        div().w(size).flex_shrink_0().into_any_element()
    }
}

fn render_row_entry(
    cell: &InspectorCell,
    null_color: Hsla,
    theme: &gpui_component::theme::Theme,
) -> impl IntoElement {
    let is_null = cell.value.is_null();
    let value_text = cell.value.as_display_string_truncated(200);

    div()
        .flex()
        .flex_col()
        .gap(InspectorMetrics::FIELD_GAP)
        .px(InspectorMetrics::PADDING_X)
        .py(InspectorMetrics::FIELD_PADDING_Y)
        .border_b_1()
        .border_color(theme.table_row_border)
        .child(
            div()
                .flex()
                .items_center()
                .gap(InspectorMetrics::FIELD_LABEL_GAP)
                .min_w_0()
                .text_size(InspectorMetrics::FIELD_LABEL_FONT)
                .text_color(theme.muted_foreground)
                .child(render_key_icon(
                    cell.is_primary_key,
                    cell.is_foreign_key,
                    InspectorMetrics::FIELD_ICON,
                    theme,
                ))
                .child(div().min_w_0().truncate().child(cell.name.clone())),
        )
        .child(
            div()
                .min_w_0()
                .truncate()
                .font_family(AppFonts::MONO)
                .text_size(InspectorMetrics::FIELD_VALUE_FONT)
                .text_color(if is_null {
                    null_color
                } else {
                    ChromeColors::strong(theme)
                })
                .when(is_null, |value| value.italic())
                .child(value_text),
        )
}

fn render_references_section(
    references: &[FkReference],
    references_ready: bool,
    theme: &gpui_component::theme::Theme,
) -> impl IntoElement {
    if !references_ready {
        return div()
            .flex()
            .items_center()
            .gap(InspectorMetrics::FIELD_LABEL_GAP)
            .px(InspectorMetrics::PADDING_X)
            .child(
                Icon::new(AppIcon::Loader)
                    .size(InspectorMetrics::FIELD_ICON)
                    .color(theme.muted_foreground),
            )
            .child(
                Text::caption(dbflux_i18n::t!(
                    "document.data.row_inspector.references.loading"
                ))
                .color(theme.muted_foreground),
            )
            .into_any_element();
    }

    if references.is_empty() {
        return div()
            .px(InspectorMetrics::PADDING_X)
            .child(
                Text::caption(dbflux_i18n::t!(
                    "document.data.row_inspector.references.empty"
                ))
                .color(theme.muted_foreground),
            )
            .into_any_element();
    }

    let resolving_label = dbflux_i18n::t!("document.data.row_inspector.references.resolving");
    let not_found_label = dbflux_i18n::t!("document.data.row_inspector.references.not_found");

    div()
        .flex()
        .flex_col()
        .children(references.iter().map(|fk_ref| {
            render_fk_reference_entry(fk_ref, &resolving_label, &not_found_label, theme)
        }))
        .into_any_element()
}

/// One reference card: the foreign key column and the table it points at,
/// with the referenced row's resolution state underneath.
fn render_fk_reference_entry(
    fk_ref: &FkReference,
    resolving_label: &str,
    not_found_label: &str,
    theme: &gpui_component::theme::Theme,
) -> impl IntoElement {
    let status: Option<(String, Hsla)> = match &fk_ref.row {
        LoadingState::Idle => None,
        LoadingState::Loading => Some((resolving_label.to_string(), theme.muted_foreground)),
        LoadingState::Failed { message } => Some((message.to_string(), theme.danger)),
        LoadingState::Loaded(map) if map.is_empty() => {
            Some((not_found_label.to_string(), theme.muted_foreground))
        }
        LoadingState::Loaded(map) => Some((summarize_row(map), theme.foreground)),
    };

    div()
        .relative()
        .flex()
        .flex_col()
        .gap(InspectorMetrics::FIELD_GAP)
        .mx(InspectorMetrics::REFERENCE_MARGIN_X)
        .mb(InspectorMetrics::REFERENCE_MARGIN_BOTTOM)
        .px(InspectorMetrics::REFERENCE_PADDING_X)
        .py(InspectorMetrics::REFERENCE_PADDING_Y)
        .child(Chamfer::new(ChamferCut::CONTROL).fill(theme.secondary))
        .child(
            div()
                .flex()
                .items_center()
                .gap(InspectorMetrics::REFERENCE_GAP)
                .min_w_0()
                .font_family(AppFonts::MONO)
                .text_size(InspectorMetrics::FIELD_VALUE_FONT)
                .child(
                    Icon::new(AppIcon::Cable)
                        .size(InspectorMetrics::REFERENCE_ICON)
                        .color(theme.info),
                )
                .child(
                    div()
                        .flex_shrink_0()
                        .text_color(theme.foreground)
                        .child(fk_ref.column.clone()),
                )
                .child(
                    Icon::new(AppIcon::ChevronRight)
                        .size(InspectorMetrics::REFERENCE_CHEVRON)
                        .color(theme.input),
                )
                .child(
                    div()
                        .min_w_0()
                        .truncate()
                        .text_color(ChromeColors::strong(theme))
                        .child(fk_ref.qualified_target()),
                ),
        )
        .when_some(status, |card, (text, color)| {
            card.child(
                div()
                    .min_w_0()
                    .truncate()
                    .text_size(InspectorMetrics::FIELD_LABEL_FONT)
                    .text_color(color)
                    .child(text),
            )
        })
}

/// Build a short human-readable summary of a resolved row.
///
/// Prefers well-known display columns (`name`, `title`, `email`, `label`) if
/// present. Falls back to the first non-PK string column, then to a count of
/// fields. At most three values are included in the summary.
pub fn summarize_row(map: &HashMap<String, Value>) -> String {
    const DISPLAY_KEYS: &[&str] = &["name", "title", "email", "label", "username", "slug"];

    let mut parts: Vec<String> = Vec::new();

    // Preferred display columns, in priority order.
    for key in DISPLAY_KEYS {
        if let Some(val) = map.get(*key)
            && !val.is_null()
        {
            parts.push(val.as_display_string_truncated(60));
            if parts.len() >= 2 {
                break;
            }
        }
    }

    // If nothing matched, fall back to the first non-id string column.
    if parts.is_empty() {
        for (key, val) in map.iter() {
            if key == "id" || key.ends_with("_id") || val.is_null() {
                continue;
            }
            if matches!(val, Value::Text(_)) {
                parts.push(val.as_display_string_truncated(60));
                break;
            }
        }
    }

    // Final fallback: field count.
    if parts.is_empty() {
        return format!("{} fields", map.len());
    }

    parts.join(" · ")
}

// ---------------------------------------------------------------------------
// RowInspectorContent entity
// ---------------------------------------------------------------------------

/// The row inspector panel mounted in the workspace inspector rail.
pub struct RowInspectorContent {
    snapshot: InspectorSnapshot,
    references: Vec<FkReference>,
    references_ready: bool,
    pinned: bool,
    focus_handle: FocusHandle,
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
            pinned: false,
            focus_handle: cx.focus_handle(),
        }
    }

    /// Replace the snapshot for a new row selection while keeping the entity alive.
    pub fn open(&mut self, snapshot: InspectorSnapshot, cx: &mut Context<Self>) {
        self.snapshot = snapshot;
        self.references = Vec::new();
        self.references_ready = false;
        cx.notify();
    }

    /// Show the pin button as pressed (`true`) or released.
    pub fn set_pinned(&mut self, pinned: bool, cx: &mut Context<Self>) {
        if self.pinned != pinned {
            self.pinned = pinned;
            cx.notify();
        }
    }

    /// Set the resolved FK references after an async lookup completes.
    pub fn set_references(&mut self, references: Vec<FkReference>, cx: &mut Context<Self>) {
        self.references = references;
        self.references_ready = true;
        cx.notify();
    }

    /// Update the resolution state for a single FK reference by index.
    ///
    /// Out-of-bounds index is silently ignored.
    pub fn resolve_reference(
        &mut self,
        index: usize,
        result: Result<Option<HashMap<String, Value>>, String>,
        cx: &mut Context<Self>,
    ) {
        let Some(fk_ref) = self.references.get_mut(index) else {
            return;
        };

        fk_ref.row = match result {
            Ok(Some(map)) => LoadingState::Loaded(map),
            Ok(None) => LoadingState::Loaded(HashMap::new()),
            Err(msg) => LoadingState::Failed {
                message: msg.into(),
            },
        };

        cx.notify();
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
            .border_b_1()
            .border_color(theme.border)
            .child(
                Icon::new(AppIcon::Info)
                    .size(InspectorMetrics::HEADER_ICON)
                    .color(ChromeColors::tint(theme)),
            )
            .child(
                Text::body(title)
                    .color(ChromeColors::strong(theme))
                    .font_weight(FontWeight::BOLD),
            )
            .when_some(self.snapshot.row_key.clone(), |header, key| {
                header.child(
                    div()
                        .min_w_0()
                        .truncate()
                        .font_family(AppFonts::MONO)
                        .text_size(InspectorMetrics::KEY_FONT)
                        .text_color(theme.muted_foreground)
                        .child(key),
                )
            })
            .child(div().flex_1())
            .child(
                Button::new("row-inspector-pin", pin_label)
                    .small()
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
                .small()
                .icon(AppIcon::CircleX)
                .icon_only()
                .tab_stop(false)
                .on_click(cx.listener(|_, _, _, cx| {
                    cx.emit(RowInspectorContentEvent::Close);
                })),
            )
            .into_any_element()
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let can_edit = self.snapshot.can_edit;

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(InspectorMetrics::FOOTER_GAP)
            .p(InspectorMetrics::FOOTER_PADDING)
            .border_t_1()
            .border_color(theme.border)
            .child(
                Button::new(
                    "row-inspector-edit",
                    dbflux_i18n::t!("document.data.row_inspector.action.edit"),
                )
                .small()
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
                .small()
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
                .small()
                .danger()
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

        let body = div()
            .id("row-inspector-body")
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .overflow_y_scroll()
            .child(render_section_label(
                dbflux_i18n::t!("document.data.row_inspector.section.row"),
                InspectorMetrics::ROW_LABEL_PADDING_TOP,
                InspectorMetrics::ROW_LABEL_PADDING_BOTTOM,
                theme,
            ))
            .children(
                self.snapshot
                    .cells
                    .iter()
                    .map(|cell| render_row_entry(cell, null_color, theme)),
            )
            .when(has_fk, |body| {
                body.child(render_section_label(
                    dbflux_i18n::t!("document.data.row_inspector.section.references"),
                    InspectorMetrics::REFERENCES_LABEL_PADDING_TOP,
                    InspectorMetrics::REFERENCES_LABEL_PADDING_BOTTOM,
                    theme,
                ))
                .child(render_references_section(
                    &self.references,
                    self.references_ready,
                    theme,
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
        FkReference, InspectorCell, InspectorSnapshot, RowInspectorContent, row_key_label,
        summarize_row,
    };
    use dbflux_components::primitives::LoadingState;
    use dbflux_core::Value;
    use gpui::{AppContext as _, TestAppContext};
    use std::collections::HashMap;

    fn cell(name: &str, value: Value, is_primary_key: bool) -> InspectorCell {
        InspectorCell {
            name: name.to_string(),
            value,
            is_primary_key,
            is_foreign_key: false,
        }
    }

    fn make_snapshot() -> InspectorSnapshot {
        let cells = vec![cell("id", Value::Int(1), true)];

        InspectorSnapshot {
            row_number: 1,
            row_key: row_key_label(&cells),
            cells,
            can_edit: true,
        }
    }

    #[test]
    fn row_key_label_joins_primary_key_values_in_column_order() {
        let cells = vec![
            cell("tenant", Value::Text("acme".to_string()), true),
            cell("name", Value::Text("Alice".to_string()), false),
            cell("id", Value::Int(7), true),
        ];

        assert_eq!(row_key_label(&cells).as_deref(), Some("acme, 7"));
    }

    #[test]
    fn row_key_label_is_none_without_a_primary_key() {
        let cells = vec![cell("name", Value::Text("Alice".to_string()), false)];

        assert_eq!(row_key_label(&cells), None);
    }

    #[test]
    fn qualified_target_prefixes_the_schema_when_known() {
        let mut reference = FkReference {
            column: "user_id".to_string(),
            target_schema: Some("public".to_string()),
            target_table: "users".to_string(),
            target_pk: "id".to_string(),
            value: Value::Int(1),
            row: LoadingState::Idle,
        };

        assert_eq!(reference.qualified_target(), "public.users");

        reference.target_schema = None;
        assert_eq!(reference.qualified_target(), "users");
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

        let fk_refs = vec![FkReference {
            column: "user_id".to_string(),
            target_schema: None,
            target_table: "users".to_string(),
            target_pk: "id".to_string(),
            value: Value::Int(42),
            row: LoadingState::Loading,
        }];

        cx.update(|cx| {
            entity.update(cx, |content, cx| {
                content.set_references(fk_refs, cx);
            });
        });

        cx.read(|cx| {
            let content = entity.read(cx);
            assert!(content.references_ready());
            assert_eq!(content.references_len(), 1);
        });
    }

    #[gpui::test]
    fn row_inspector_content_resolve_reference(cx: &mut TestAppContext) {
        let entity = cx.new(|cx| RowInspectorContent::new(make_snapshot(), cx));

        let fk_refs = vec![FkReference {
            column: "user_id".to_string(),
            target_schema: None,
            target_table: "users".to_string(),
            target_pk: "id".to_string(),
            value: Value::Int(1),
            row: LoadingState::Loading,
        }];

        cx.update(|cx| {
            entity.update(cx, |content, cx| {
                content.set_references(fk_refs, cx);
                let mut resolved = HashMap::new();
                resolved.insert("name".to_string(), Value::Text("Alice".to_string()));
                content.resolve_reference(0, Ok(Some(resolved)), cx);
            });
        });

        cx.read(|cx| {
            let content = entity.read(cx);
            match &content.references[0].row {
                LoadingState::Loaded(map) => {
                    assert_eq!(map.get("name"), Some(&Value::Text("Alice".to_string())));
                }
                other => panic!("expected Loaded, got {:?}", other),
            }
        });
    }

    #[gpui::test]
    fn row_inspector_content_resolve_reference_out_of_bounds_is_noop(cx: &mut TestAppContext) {
        let entity = cx.new(|cx| RowInspectorContent::new(make_snapshot(), cx));

        cx.update(|cx| {
            entity.update(cx, |content, cx| {
                content.resolve_reference(99, Ok(None), cx);
            });
        });

        cx.read(|cx| {
            assert_eq!(entity.read(cx).references_len(), 0);
        });
    }

    fn map_from(pairs: &[(&str, &str)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Text(v.to_string())))
            .collect()
    }

    #[test]
    fn summarize_prefers_name_over_other_columns() {
        let map = map_from(&[("id", "1"), ("name", "Alice"), ("email", "a@b.com")]);
        let summary = summarize_row(&map);
        assert!(
            summary.contains("Alice"),
            "should include name: {}",
            summary
        );
    }

    #[test]
    fn summarize_falls_back_to_email_when_no_name() {
        let map = map_from(&[("id", "1"), ("email", "a@b.com")]);
        let summary = summarize_row(&map);
        assert!(
            summary.contains("a@b.com"),
            "should include email: {}",
            summary
        );
    }

    #[test]
    fn summarize_shows_field_count_when_no_useful_columns() {
        let mut map: HashMap<String, Value> = HashMap::new();
        map.insert("id".to_string(), Value::Int(42));
        map.insert("user_id".to_string(), Value::Int(7));
        let summary = summarize_row(&map);
        assert!(
            summary.contains("fields"),
            "should show field count: {}",
            summary
        );
    }

    #[test]
    fn summarize_skips_null_values() {
        let mut map: HashMap<String, Value> = HashMap::new();
        map.insert("name".to_string(), Value::Null);
        map.insert("email".to_string(), Value::Text("x@y.com".to_string()));
        let summary = summarize_row(&map);
        assert!(
            summary.contains("x@y.com"),
            "should skip null name: {}",
            summary
        );
    }

    #[test]
    fn summarize_empty_map_returns_zero_fields() {
        let map = HashMap::new();
        let summary = summarize_row(&map);
        assert_eq!(summary, "0 fields");
    }

    #[test]
    fn row_inspector_keys_resolve_in_both_locales() {
        let keys = [
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
