//! Row Inspector content for the workspace-level inspector rail.
//!
//! # `RowInspectorContent`
//!
//! Content-only entity that renders the scrollable ROW / REFERENCES / COLUMN
//! sections.  All chrome (title bar, close button, resize grip, drag mask) is
//! owned by `WorkspaceInspector` in `workspace/inspector.rs`.
//!
//! # Opening
//!
//! `DataGridPanel::open_row_inspector` builds an `InspectorSnapshot`, creates
//! or updates a `RowInspectorContent` entity, and emits
//! `DataGridEvent::OpenInspector` so the workspace mounts it in the inspector
//! rail.
//!
//! # Sections
//!
//! - **ROW** — all column name / value pairs for the selected row.
//! - **COLUMN** — metadata for the focused column (type, nullable, PK/FK flags).
//! - **REFERENCES** — FK-resolved values; each FK resolves asynchronously.

use crate::ui::icons::AppIcon;
use crate::ui::tokens::Spacing;
use dbflux_components::primitives::{Icon, LoadingState, Text};
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
    pub type_label: String,
    pub nullable: bool,
}

/// All data the inspector needs to render without further async calls
/// (except FK reference resolution which is done lazily).
#[derive(Debug, Clone)]
pub struct InspectorSnapshot {
    /// Column values for the row.
    pub cells: Vec<InspectorCell>,
    /// Index of the column that was focused when the inspector opened.
    pub focused_col: usize,
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

// ---------------------------------------------------------------------------
// Section helpers
// ---------------------------------------------------------------------------

fn render_section_header(
    label: &'static str,
    theme: &gpui_component::theme::Theme,
) -> impl IntoElement {
    div()
        .px(Spacing::SM)
        .py(Spacing::XS)
        .border_b_1()
        .border_color(theme.border)
        .bg(theme.secondary.opacity(0.5))
        .child(
            Text::caption(label)
                .font_size(dbflux_components::tokens::FontSizes::XS)
                .color(theme.muted_foreground),
        )
}

fn render_row_entry(
    cell: &InspectorCell,
    theme: &gpui_component::theme::Theme,
) -> impl IntoElement {
    let value_text = cell.value.as_display_string_truncated(200);
    let is_null = cell.value.is_null();

    div()
        .flex()
        .items_start()
        .justify_between()
        .gap(Spacing::XS)
        .px(Spacing::SM)
        .py(Spacing::XS)
        .border_b_1()
        .border_color(theme.border.opacity(0.5))
        // Column name (left)
        .child(
            div()
                .flex()
                .flex_shrink_0()
                .items_center()
                .gap_1()
                .w(px(120.0))
                .overflow_hidden()
                .when(cell.is_primary_key, |d| {
                    d.child(
                        Text::caption("PK")
                            .font_size(dbflux_components::tokens::FontSizes::XS)
                            .color(theme.accent),
                    )
                })
                .when(cell.is_foreign_key, |d| {
                    d.child(
                        Text::caption("FK")
                            .font_size(dbflux_components::tokens::FontSizes::XS)
                            .color(theme.muted_foreground),
                    )
                })
                .child(
                    div()
                        .overflow_hidden()
                        .text_ellipsis()
                        .child(Text::caption(cell.name.clone()).color(theme.muted_foreground)),
                ),
        )
        // Value (right)
        .child(
            div()
                .flex_1()
                .overflow_hidden()
                .text_ellipsis()
                .when(is_null, |d| d.italic())
                .child(Text::caption(value_text).color(if is_null {
                    theme.muted_foreground
                } else {
                    theme.foreground
                })),
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
            .gap(Spacing::SM)
            .px(Spacing::SM)
            .py(Spacing::XS)
            .child(
                Icon::new(AppIcon::Loader)
                    .size(px(12.0))
                    .color(theme.muted_foreground),
            )
            .child(Text::caption("Loading schema…").color(theme.muted_foreground))
            .into_any_element();
    }

    if references.is_empty() {
        return div()
            .px(Spacing::SM)
            .py(Spacing::XS)
            .child(Text::caption("No references").color(theme.muted_foreground))
            .into_any_element();
    }

    div()
        .flex()
        .flex_col()
        .children(
            references
                .iter()
                .map(|fk_ref| render_fk_reference_entry(fk_ref, theme)),
        )
        .into_any_element()
}

fn render_fk_reference_entry(
    fk_ref: &FkReference,
    theme: &gpui_component::theme::Theme,
) -> impl IntoElement {
    let qualified_table = match &fk_ref.target_schema {
        Some(s) => format!("{}.{}", s, fk_ref.target_table),
        None => fk_ref.target_table.clone(),
    };
    let header_label = format!(
        "{} → {}.{} = {}",
        fk_ref.column,
        qualified_table,
        fk_ref.target_pk,
        fk_ref.value.as_display_string_truncated(40),
    );

    div()
        .flex()
        .flex_col()
        .border_b_1()
        .border_color(theme.border.opacity(0.5))
        // Header line — wraps for long FK target strings.
        .child(
            div()
                .w_full()
                .px(Spacing::SM)
                .py(Spacing::XS)
                .text_color(theme.muted_foreground)
                .text_size(dbflux_components::tokens::FontSizes::XS)
                .child(SharedString::from(header_label)),
        )
        // Body: resolution state
        .child(match &fk_ref.row {
            LoadingState::Idle => div().into_any_element(),

            LoadingState::Loading => div()
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .px(Spacing::SM)
                .py(Spacing::XS)
                .child(
                    Icon::new(AppIcon::Loader)
                        .size(px(12.0))
                        .color(theme.muted_foreground),
                )
                .child(Text::caption("Resolving…").color(theme.muted_foreground))
                .into_any_element(),

            LoadingState::Failed { message } => div()
                .w_full()
                .px(Spacing::SM)
                .py(Spacing::XS)
                .text_color(theme.danger.opacity(0.85))
                .text_size(dbflux_components::tokens::FontSizes::XS)
                // Inline error: muted danger text that wraps. The full
                // BannerBlock variant is overkill inside the narrow
                // inspector and breaks the layout for long server errors.
                .child(SharedString::from(message.to_string()))
                .into_any_element(),

            LoadingState::Loaded(map) if map.is_empty() => div()
                .px(Spacing::SM)
                .py(Spacing::XS)
                .child(Text::caption("— not found").color(theme.muted_foreground))
                .into_any_element(),

            LoadingState::Loaded(map) => {
                let summary = summarize_row(map);
                div()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .child(Text::caption(summary).color(theme.foreground))
                    .into_any_element()
            }
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

fn render_column_metadata(
    cell: &InspectorCell,
    theme: &gpui_component::theme::Theme,
) -> impl IntoElement {
    div()
        .flex()
        .flex_col()
        .gap(Spacing::XS)
        .px(Spacing::SM)
        .py(Spacing::SM)
        .child(render_meta_row("Name", &cell.name, theme))
        .child(render_meta_row("Type", &cell.type_label, theme))
        .child(render_meta_row(
            "Nullable",
            if cell.nullable { "yes" } else { "no" },
            theme,
        ))
        .child(render_meta_row(
            "Primary Key",
            if cell.is_primary_key { "yes" } else { "no" },
            theme,
        ))
        .child(render_meta_row(
            "Foreign Key",
            if cell.is_foreign_key { "yes" } else { "no" },
            theme,
        ))
}

fn render_meta_row(
    label: &str,
    value: &str,
    theme: &gpui_component::theme::Theme,
) -> impl IntoElement {
    div()
        .flex()
        .items_center()
        .justify_between()
        .gap(Spacing::XS)
        .child(Text::caption(label.to_string()).color(theme.muted_foreground))
        .child(Text::caption(value.to_string()).color(theme.foreground))
}

// ---------------------------------------------------------------------------
// RowInspectorContent entity
// ---------------------------------------------------------------------------

/// Content-only inspector for the workspace-level inspector rail.
///
/// Unlike `RowInspector`, `RowInspectorContent` renders ONLY the scrollable
/// body (ROW / REFERENCES / COLUMN sections).  All chrome — title bar, close
/// button, resize grip, and drag mask — is owned by `WorkspaceInspector`.
pub struct RowInspectorContent {
    snapshot: InspectorSnapshot,
    references: Vec<FkReference>,
    references_ready: bool,
    focus_handle: FocusHandle,
}

#[derive(Clone, Debug)]
pub enum RowInspectorContentEvent {
    // Reserved for future use (no Close — workspace owns lifecycle now).
}

impl EventEmitter<RowInspectorContentEvent> for RowInspectorContent {}

impl RowInspectorContent {
    pub fn new(snapshot: InspectorSnapshot, cx: &mut Context<Self>) -> Self {
        Self {
            snapshot,
            references: Vec::new(),
            references_ready: false,
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

    /// Set the resolved FK references after an async lookup completes.
    pub fn set_references(&mut self, references: Vec<FkReference>, cx: &mut Context<Self>) {
        self.references = references;
        self.references_ready = true;
        cx.notify();
    }

    /// Update the resolution state for a single FK reference by index.
    ///
    /// Out-of-bounds index is silently ignored (same behaviour as `RowInspector`).
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
}

impl Focusable for RowInspectorContent {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for RowInspectorContent {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let snapshot = self.snapshot.clone();
        let has_fk = snapshot.cells.iter().any(|c| c.is_foreign_key);

        div()
            .id("row-inspector-content")
            .size_full()
            .flex()
            .flex_col()
            .overflow_y_scroll()
            .track_focus(&self.focus_handle)
            .child(render_section_header("ROW", theme))
            .children(
                snapshot
                    .cells
                    .iter()
                    .map(|cell| render_row_entry(cell, theme)),
            )
            .when(has_fk, |d| {
                d.child(render_section_header("REFERENCES", theme)).child(
                    render_references_section(&self.references, self.references_ready, theme),
                )
            })
            .child(render_section_header("COLUMN", theme))
            .when_some(
                snapshot.cells.get(
                    snapshot
                        .focused_col
                        .min(snapshot.cells.len().saturating_sub(1)),
                ),
                |d, cell| d.child(render_column_metadata(cell, theme)),
            )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::{
        FkReference, InspectorCell, InspectorSnapshot, RowInspectorContent, summarize_row,
    };
    use dbflux_components::primitives::LoadingState;
    use dbflux_core::Value;
    use gpui::{AppContext as _, TestAppContext};
    use std::collections::HashMap;

    fn make_snapshot() -> InspectorSnapshot {
        InspectorSnapshot {
            cells: vec![InspectorCell {
                name: "id".to_string(),
                value: Value::Int(1),
                is_primary_key: true,
                is_foreign_key: false,
                type_label: "integer".to_string(),
                nullable: false,
            }],
            focused_col: 0,
        }
    }

    #[gpui::test]
    fn row_inspector_content_open_updates_snapshot(cx: &mut TestAppContext) {
        let entity = cx.new(|cx| RowInspectorContent::new(make_snapshot(), cx));

        let new_snap = InspectorSnapshot {
            cells: vec![InspectorCell {
                name: "name".to_string(),
                value: Value::Text("Alice".to_string()),
                is_primary_key: false,
                is_foreign_key: false,
                type_label: "text".to_string(),
                nullable: true,
            }],
            focused_col: 0,
        };

        cx.update(|cx| {
            entity.update(cx, |content, cx| {
                content.open(new_snap, cx);
            });
        });

        cx.read(|cx| {
            let content = entity.read(cx);
            assert_eq!(content.snapshot.cells[0].name, "name");
            assert!(!content.references_ready(), "open resets references_ready");
        });
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

        // Should not panic when index is out of bounds
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
}
