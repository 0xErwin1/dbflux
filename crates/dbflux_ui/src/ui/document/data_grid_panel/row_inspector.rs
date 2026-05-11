//! Row Inspector — a 320px slide-in panel showing full row data, column
//! metadata, and FK-referenced values for the selected row.
//!
//! # Opening
//!
//! Call `RowInspector::open` with an `InspectorSnapshot` built from the
//! current selection. The inspector renders itself as an overlay positioned
//! absolutely at the right edge of the grid panel.
//!
//! # Sections
//!
//! - **ROW** — all column name / value pairs for the selected row.
//! - **COLUMN** — metadata for the focused column (type, nullable, PK/FK flags).
//! - **REFERENCES** — FK-resolved values; each FK resolves asynchronously.

use crate::ui::icons::AppIcon;
use crate::ui::tokens::{Heights, Radii, Spacing};
use dbflux_components::primitives::{BannerBlock, BannerVariant, Icon, LoadingState, Text};
use dbflux_components::tokens::Widths;
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
    /// Human-readable row identifier (e.g. "Row 3" or PK value).
    pub row_label: String,
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
// RowInspector entity
// ---------------------------------------------------------------------------

/// Overlay inspector panel for the selected row.
pub struct RowInspector {
    snapshot: InspectorSnapshot,
    /// Per-FK reference entries; each resolves independently.
    references: Vec<FkReference>,
    /// True when the references list has been populated (even if empty).
    references_ready: bool,
    close_requested: bool,
    focus_handle: FocusHandle,
    /// Current panel width. Persists across re-renders within the same
    /// inspector lifetime; reset to the default each time `RowInspector::new`
    /// is called.
    width: Pixels,
    is_resizing: bool,
    resize_start_x: Option<Pixels>,
    resize_start_width: Option<Pixels>,
}

const INSPECTOR_MIN_WIDTH: Pixels = px(240.0);
const INSPECTOR_MAX_WIDTH: Pixels = px(1280.0);

impl EventEmitter<RowInspectorEvent> for RowInspector {}

#[derive(Debug, Clone)]
pub enum RowInspectorEvent {
    CloseRequested,
}

impl RowInspector {
    pub fn new(snapshot: InspectorSnapshot, cx: &mut Context<Self>) -> Self {
        Self {
            snapshot,
            references: Vec::new(),
            references_ready: false,
            close_requested: false,
            focus_handle: cx.focus_handle(),
            width: Widths::INSPECTOR,
            is_resizing: false,
            resize_start_x: None,
            resize_start_width: None,
        }
    }

    /// Update the snapshot for a new row selection while keeping the panel open.
    pub fn open(&mut self, snapshot: InspectorSnapshot, cx: &mut Context<Self>) {
        self.snapshot = snapshot;
        self.references = Vec::new();
        self.references_ready = false;
        self.close_requested = false;
        cx.notify();
    }

    /// Set the resolved FK references after an async lookup completes.
    ///
    /// Called from the DataGridPanel after all per-FK fetches return.
    pub fn set_references(&mut self, references: Vec<FkReference>, cx: &mut Context<Self>) {
        self.references = references;
        self.references_ready = true;
        cx.notify();
    }

    /// Update the resolution state for a single FK reference by index.
    ///
    /// Called when one async fetch completes while others are still in flight.
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

    /// Request to close the panel (emits `CloseRequested`).
    pub fn request_close(&mut self, cx: &mut Context<Self>) {
        self.close_requested = true;
        cx.emit(RowInspectorEvent::CloseRequested);
    }
}

impl Focusable for RowInspector {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

impl Render for RowInspector {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let snapshot = self.snapshot.clone();
        let close_entity = cx.entity().clone();
        let has_fk = snapshot.cells.iter().any(|c| c.is_foreign_key);

        // Resize grip + fullscreen drag mask.
        //
        // The grip itself is a 6 px column on the inspector's left edge that
        // captures mouse_down. While `is_resizing` is true we render a
        // sibling div with `.absolute().inset_0()` and `.size_full()` on top
        // of the data grid (but *behind* the inspector visually because the
        // inspector is rendered AFTER it). That mask owns the move/up
        // handlers, so the drag tracks the cursor everywhere — not just
        // inside the 6 px column — and releases cleanly even if the cursor
        // ends up outside the inspector.
        const GRIP_WIDTH: Pixels = px(6.0);
        let content_width = self.width - GRIP_WIDTH;
        let grip = div()
            .id("inspector-grip")
            .h_full()
            .w(GRIP_WIDTH)
            .flex_shrink_0()
            .cursor_col_resize()
            .hover(|el| el.bg(theme.accent.opacity(0.3)))
            .when(self.is_resizing, |el| el.bg(theme.primary))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, event: &MouseDownEvent, _, cx| {
                    this.is_resizing = true;
                    this.resize_start_x = Some(event.position.x);
                    this.resize_start_width = Some(this.width);
                    cx.notify();
                }),
            );

        // Outer wrapper covers the entire data-grid panel so the drag mask
        // can track the cursor anywhere (not just inside the inspector's
        // own bounds). When not resizing, it has no handlers and clicks
        // pass through to the grid.
        let outer = div().absolute().inset_0().when(self.is_resizing, |el| {
            el.cursor_col_resize()
                .on_mouse_move(cx.listener(|this, event: &MouseMoveEvent, _, cx| {
                    if !this.is_resizing {
                        return;
                    }
                    let Some(start_x) = this.resize_start_x else {
                        return;
                    };
                    let Some(start_width) = this.resize_start_width else {
                        return;
                    };
                    let delta = event.position.x - start_x;
                    let new_width =
                        (start_width - delta).clamp(INSPECTOR_MIN_WIDTH, INSPECTOR_MAX_WIDTH);
                    this.width = new_width;
                    cx.notify();
                }))
                .on_mouse_up(
                    MouseButton::Left,
                    cx.listener(|this, _, _, cx| {
                        this.is_resizing = false;
                        this.resize_start_x = None;
                        this.resize_start_width = None;
                        cx.notify();
                    }),
                )
        });

        let panel = div()
            .absolute()
            .right_0()
            .top_0()
            .bottom_0()
            .w(self.width)
            .flex()
            .flex_row()
            .bg(theme.background)
            .border_l_1()
            .border_color(theme.border)
            .track_focus(&self.focus_handle)
            .on_scroll_wheel(|_, _, cx| {
                cx.stop_propagation();
            })
            .child(grip)
            .child(
                div()
                    .h_full()
                    .w(content_width)
                    .flex()
                    .flex_col()
                    .overflow_hidden()
                    // Header
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .h(Heights::TOOLBAR)
                            .px(Spacing::SM)
                            .border_b_1()
                            .border_color(theme.border)
                            .child(
                                Text::caption(snapshot.row_label.clone())
                                    .color(theme.muted_foreground),
                            )
                            .child(
                                div()
                                    .id("inspector-close")
                                    .w(px(20.0))
                                    .h(px(20.0))
                                    .flex()
                                    .items_center()
                                    .justify_center()
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .text_color(theme.muted_foreground)
                                    .hover(|d| d.bg(theme.secondary).text_color(theme.foreground))
                                    .on_click(move |_, _, cx| {
                                        close_entity.update(cx, |inspector, cx| {
                                            inspector.request_close(cx);
                                        });
                                    })
                                    .child("\u{00d7}"),
                            ),
                    )
                    // Scrollable body
                    .child(
                        div()
                            .id("inspector-body")
                            .flex_1()
                            .overflow_y_scroll()
                            .flex()
                            .flex_col()
                            .child(render_section_header("ROW", theme))
                            .children(
                                snapshot
                                    .cells
                                    .iter()
                                    .map(|cell| render_row_entry(cell, theme)),
                            )
                            .when(has_fk, |d| {
                                d.child(render_section_header("REFERENCES", theme)).child(
                                    render_references_section(
                                        &self.references,
                                        self.references_ready,
                                        theme,
                                    ),
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
                            ),
                    ),
            );

        outer.child(panel)
    }
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::summarize_row;
    use dbflux_core::Value;
    use std::collections::HashMap;

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
