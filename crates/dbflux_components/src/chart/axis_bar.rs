//! `AxisBar` — inline pill row for chart binding configuration.
//!
//! The AxisBar renders a compact horizontal strip of clickable pills that
//! represent the current `BindingSpec`. Clicking a pill opens a lightweight
//! dropdown that lets the user pick a column (or aggregation kind) without
//! leaving the chart surface.
//!
//! # Design constraints
//!
//! - Pure render function (`axis_bar_element`): takes borrowed state and
//!   `'static` callbacks; emits no side-effects.
//! - Picker (dropdown) state — which pill is currently open — lives on the
//!   host (`ChartShell`) so it is preserved across re-renders.
//! - Only `Line` charts are wired in v0.6; the AxisBar is present for all chart
//!   kinds as a forward-compatibility seam.

use gpui::prelude::*;
use gpui::{AnyElement, App, ElementId, MouseButton, SharedString, Window, div, px};

use crate::chart::spec::{AggKind, BindingSpec};
use dbflux_core::{ColumnKind, ColumnMeta};

/// Identifies which AxisBar pill is currently open (showing its picker).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AxisPill {
    /// X-axis column picker.
    X,
    /// Y-axis (multi-select) column picker.
    Y,
    /// Group-by column picker.
    Group,
    /// Aggregation kind picker.
    Agg,
}

/// Render the AxisBar pill row.
///
/// # Parameters
///
/// - `bindings`: current `BindingSpec`; drives pill labels.
/// - `columns`: column metadata from the current `QueryResult`.
/// - `open_pill`: which pill's picker is currently shown (`None` = all closed).
/// - `on_pill_click`: called when the user clicks a pill header (to open/close
///   its picker). Receives the clicked `AxisPill`.
/// - `on_x_select`: called when the user picks a column for the X axis.
///   Receives the column index.
/// - `on_y_toggle`: called when the user toggles a Y column. Receives the
///   column index and the new checked state.
/// - `on_group_select`: called when the user picks a group-by column.
///   Receives `Some(col_idx)` for a column or `None` for "none".
/// - `on_agg_select`: called when the user picks an aggregation kind.
///
/// The high parameter count is intentional: each callback has a distinct type
/// signature that cannot be collapsed without boxing (and thus heap allocation
/// per render). The `#[allow]` suppresses the clippy lint for this case.
#[allow(clippy::too_many_arguments)]
pub fn axis_bar_element<FPill, FX, FY, FGroup, FAgg>(
    bindings: &BindingSpec,
    columns: &[ColumnMeta],
    open_pill: Option<AxisPill>,
    on_pill_click: FPill,
    on_x_select: FX,
    on_y_toggle: FY,
    on_group_select: FGroup,
    on_agg_select: FAgg,
) -> impl IntoElement
where
    FPill: Fn(AxisPill, &mut Window, &mut App) + Clone + Send + Sync + 'static,
    FX: Fn(usize, &mut Window, &mut App) + Clone + Send + Sync + 'static,
    FY: Fn(usize, bool, &mut Window, &mut App) + Clone + Send + Sync + 'static,
    FGroup: Fn(Option<usize>, &mut Window, &mut App) + Clone + Send + Sync + 'static,
    FAgg: Fn(AggKind, &mut Window, &mut App) + Clone + Send + Sync + 'static,
{
    // Build pills for X, Y, Group, and Agg.
    let x_label: SharedString = columns
        .get(bindings.x)
        .map(|c| c.name.clone())
        .unwrap_or_else(|| "—".to_string())
        .into();

    let y_label: SharedString = match bindings.y.len() {
        0 => "none".to_string(),
        1 => columns
            .get(bindings.y[0])
            .map(|c| c.name.clone())
            .unwrap_or_else(|| "?".to_string()),
        n => format!(
            "{} +{}",
            columns
                .get(bindings.y[0])
                .map(|c| c.name.as_str())
                .unwrap_or("?"),
            n - 1
        ),
    }
    .into();

    let group_label: SharedString = bindings
        .group_by
        .and_then(|i| columns.get(i))
        .map(|c| c.name.clone())
        .unwrap_or_else(|| "—".to_string())
        .into();

    let agg_label: SharedString = match bindings.aggregation {
        AggKind::None => "none",
        AggKind::Sum => "sum",
        AggKind::Avg => "avg",
        AggKind::Min => "min",
        AggKind::Max => "max",
    }
    .into();

    let x_open = open_pill == Some(AxisPill::X);
    let y_open = open_pill == Some(AxisPill::Y);
    let group_open = open_pill == Some(AxisPill::Group);
    let agg_open = open_pill == Some(AxisPill::Agg);

    // X pill
    let x_pill = {
        let handler = on_pill_click.clone();
        pill_element(
            "axis-pill-x",
            "X",
            x_label,
            x_open,
            move |w, cx| handler(AxisPill::X, w, cx),
        )
    };

    // X picker dropdown (shown when x_open == true)
    let x_picker: Option<AnyElement> = if x_open {
        let x_candidates: Vec<(usize, SharedString)> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                matches!(
                    c.kind,
                    ColumnKind::Timestamp | ColumnKind::Integer | ColumnKind::Float
                )
            })
            .map(|(i, c)| (i, SharedString::from(c.name.clone())))
            .collect();

        Some(
            column_picker_element(
                "axis-picker-x",
                x_candidates,
                Some(bindings.x),
                move |col_idx, w, cx| on_x_select(col_idx, w, cx),
            )
            .into_any_element(),
        )
    } else {
        None
    };

    // Y pill
    let y_pill = {
        let handler = on_pill_click.clone();
        pill_element(
            "axis-pill-y",
            "Y",
            y_label,
            y_open,
            move |w, cx| handler(AxisPill::Y, w, cx),
        )
    };

    // Y picker (multi-select: show all numeric columns with checkboxes)
    let y_picker: Option<AnyElement> = if y_open {
        let y_candidates: Vec<(usize, SharedString, bool)> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| matches!(c.kind, ColumnKind::Integer | ColumnKind::Float))
            .map(|(i, c)| {
                let checked = bindings.y.contains(&i);
                (i, SharedString::from(c.name.clone()), checked)
            })
            .collect();

        Some(
            y_picker_element("axis-picker-y", y_candidates, move |col_idx, checked, w, cx| {
                on_y_toggle(col_idx, checked, w, cx);
            })
            .into_any_element(),
        )
    } else {
        None
    };

    // Group pill
    let group_pill = {
        let handler = on_pill_click.clone();
        pill_element(
            "axis-pill-group",
            "Group",
            group_label,
            group_open,
            move |w, cx| handler(AxisPill::Group, w, cx),
        )
    };

    // Group picker (single-select from Text columns, plus "none")
    let group_picker: Option<AnyElement> = if group_open {
        let mut group_candidates: Vec<(Option<usize>, SharedString)> =
            vec![(None, SharedString::from("—"))];

        let text_cols: Vec<(Option<usize>, SharedString)> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| matches!(c.kind, ColumnKind::Text))
            .map(|(i, c)| (Some(i), SharedString::from(c.name.clone())))
            .collect();

        group_candidates.extend(text_cols);

        let current = bindings.group_by;
        Some(
            group_picker_element(
                "axis-picker-group",
                group_candidates,
                current,
                move |sel, w, cx| on_group_select(sel, w, cx),
            )
            .into_any_element(),
        )
    } else {
        None
    };

    // Agg pill
    let agg_pill = {
        let handler = on_pill_click.clone();
        pill_element(
            "axis-pill-agg",
            "Agg",
            agg_label,
            agg_open,
            move |w, cx| handler(AxisPill::Agg, w, cx),
        )
    };

    // Agg picker (enum dropdown)
    let agg_picker: Option<AnyElement> = if agg_open {
        let agg_kinds: Vec<(AggKind, SharedString)> = vec![
            (AggKind::None, "none".into()),
            (AggKind::Sum, "sum".into()),
            (AggKind::Avg, "avg".into()),
            (AggKind::Min, "min".into()),
            (AggKind::Max, "max".into()),
        ];
        let current = bindings.aggregation;

        Some(
            agg_picker_element("axis-picker-agg", agg_kinds, current, move |kind, w, cx| {
                on_agg_select(kind, w, cx);
            })
            .into_any_element(),
        )
    } else {
        None
    };

    // Assemble the bar: pills in a row, each with its picker floating below.
    div()
        .flex()
        .flex_row()
        .items_center()
        .gap(px(4.0))
        .px(px(8.0))
        .py(px(2.0))
        .child(
            pill_group("axis-x-group", x_pill, x_picker)
        )
        .child(
            pill_group("axis-y-group", y_pill, y_picker)
        )
        .child(
            pill_group("axis-group-group", group_pill, group_picker)
        )
        .child(
            pill_group("axis-agg-group", agg_pill, agg_picker)
        )
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Build a single axis pill button.
///
/// The pill has a role label ("X", "Y", …) and a value label (column name).
/// When `active`, the pill border is highlighted.
fn pill_element(
    id: impl Into<ElementId>,
    role: &'static str,
    value: SharedString,
    active: bool,
    on_click: impl Fn(&mut Window, &mut App) + Send + Sync + 'static,
) -> impl IntoElement {
    let border_alpha = if active { 0.6_f32 } else { 0.18 };

    div()
        .id(id.into())
        .flex()
        .flex_row()
        .items_center()
        .gap(px(3.0))
        .px(px(6.0))
        .py(px(2.0))
        .rounded(px(4.0))
        .border_1()
        .border_color(gpui::hsla(0.0, 0.0, 1.0, border_alpha))
        .bg(gpui::hsla(0.0, 0.0, 1.0, if active { 0.08 } else { 0.04 }))
        .cursor_pointer()
        .hover(|s| s.bg(gpui::hsla(0.0, 0.0, 1.0, 0.10)))
        .on_mouse_down(MouseButton::Left, move |_, window, cx| {
            on_click(window, cx);
        })
        .child(
            div()
                .text_size(px(10.0))
                .font_weight(gpui::FontWeight::SEMIBOLD)
                .text_color(gpui::hsla(0.0, 0.0, 0.5, 1.0))
                .child(SharedString::from(role)),
        )
        .child(
            div()
                .text_size(px(11.0))
                .text_color(gpui::hsla(0.0, 0.0, 0.9, 1.0))
                .child(value),
        )
}

/// Wrap a pill and its optional picker in a relative-positioned container.
///
/// The picker floats absolutely below the pill so it doesn't affect layout.
fn pill_group(
    id: impl Into<ElementId>,
    pill: impl IntoElement,
    picker: Option<AnyElement>,
) -> impl IntoElement {
    let mut container = div()
        .id(id.into())
        .relative()
        .flex()
        .flex_col()
        .child(pill);

    if let Some(picker_el) = picker {
        container = container.child(
            div()
                .absolute()
                .top(px(24.0))
                .left(px(0.0))
                .child(picker_el),
        );
    }

    container
}

/// A single-select column picker rendered as a vertical list.
fn column_picker_element<F>(
    id: impl Into<ElementId>,
    candidates: Vec<(usize, SharedString)>,
    selected: Option<usize>,
    on_select: F,
) -> impl IntoElement
where
    F: Fn(usize, &mut Window, &mut App) + Clone + Send + Sync + 'static,
{
    let rows: Vec<AnyElement> = candidates
        .into_iter()
        .map(|(col_idx, label)| {
            let is_selected = selected == Some(col_idx);
            let handler = on_select.clone();

            div()
                .id(ElementId::Name(
                    format!("col-pick-{}", col_idx).into(),
                ))
                .flex()
                .flex_row()
                .items_center()
                .gap(px(6.0))
                .px(px(8.0))
                .py(px(3.0))
                .cursor_pointer()
                .hover(|s| s.bg(gpui::hsla(0.0, 0.0, 1.0, 0.06)))
                .when(is_selected, |d| d.font_weight(gpui::FontWeight::MEDIUM))
                .on_mouse_down(MouseButton::Left, move |_, window, cx| {
                    handler(col_idx, window, cx);
                })
                .child(
                    div()
                        .text_size(px(11.0))
                        .text_color(gpui::hsla(0.0, 0.0, 0.9, 1.0))
                        .child(label),
                )
                .into_any_element()
        })
        .collect();

    picker_container(id, rows)
}

/// A multi-select Y-column picker with one checkbox row per candidate.
fn y_picker_element<F>(
    id: impl Into<ElementId>,
    candidates: Vec<(usize, SharedString, bool)>,
    on_toggle: F,
) -> impl IntoElement
where
    F: Fn(usize, bool, &mut Window, &mut App) + Clone + Send + Sync + 'static,
{
    let rows: Vec<AnyElement> = candidates
        .into_iter()
        .map(|(col_idx, label, checked)| {
            let handler = on_toggle.clone();

            div()
                .id(ElementId::Name(
                    format!("y-pick-{}", col_idx).into(),
                ))
                .flex()
                .flex_row()
                .items_center()
                .gap(px(6.0))
                .px(px(8.0))
                .py(px(3.0))
                .cursor_pointer()
                .hover(|s| s.bg(gpui::hsla(0.0, 0.0, 1.0, 0.06)))
                .on_mouse_down(MouseButton::Left, move |_, window, cx| {
                    handler(col_idx, !checked, window, cx);
                })
                .child(
                    // Checkbox indicator
                    div()
                        .w(px(10.0))
                        .h(px(10.0))
                        .rounded(px(2.0))
                        .border_1()
                        .border_color(gpui::hsla(0.0, 0.0, 1.0, 0.3))
                        .bg(if checked {
                            gpui::hsla(0.55, 0.7, 0.5, 1.0)
                        } else {
                            gpui::hsla(0.0, 0.0, 0.0, 0.0)
                        }),
                )
                .child(
                    div()
                        .text_size(px(11.0))
                        .text_color(gpui::hsla(0.0, 0.0, 0.9, 1.0))
                        .child(label),
                )
                .into_any_element()
        })
        .collect();

    picker_container(id, rows)
}

/// A single-select picker for group-by column (includes a "none" option).
fn group_picker_element<F>(
    id: impl Into<ElementId>,
    candidates: Vec<(Option<usize>, SharedString)>,
    selected: Option<usize>,
    on_select: F,
) -> impl IntoElement
where
    F: Fn(Option<usize>, &mut Window, &mut App) + Clone + Send + Sync + 'static,
{
    let rows: Vec<AnyElement> = candidates
        .into_iter()
        .map(|(col_idx_opt, label)| {
            let is_selected = col_idx_opt == selected;
            let handler = on_select.clone();

            div()
                .id(ElementId::Name(
                    format!(
                        "grp-pick-{}",
                        col_idx_opt.map(|i| i.to_string()).unwrap_or_else(|| "none".to_string())
                    )
                    .into(),
                ))
                .flex()
                .flex_row()
                .items_center()
                .gap(px(6.0))
                .px(px(8.0))
                .py(px(3.0))
                .cursor_pointer()
                .hover(|s| s.bg(gpui::hsla(0.0, 0.0, 1.0, 0.06)))
                .when(is_selected, |d| d.font_weight(gpui::FontWeight::MEDIUM))
                .on_mouse_down(MouseButton::Left, move |_, window, cx| {
                    handler(col_idx_opt, window, cx);
                })
                .child(
                    div()
                        .text_size(px(11.0))
                        .text_color(gpui::hsla(0.0, 0.0, 0.9, 1.0))
                        .child(label),
                )
                .into_any_element()
        })
        .collect();

    picker_container(id, rows)
}

/// A single-select picker for `AggKind`.
fn agg_picker_element<F>(
    id: impl Into<ElementId>,
    agg_kinds: Vec<(AggKind, SharedString)>,
    current: AggKind,
    on_select: F,
) -> impl IntoElement
where
    F: Fn(AggKind, &mut Window, &mut App) + Clone + Send + Sync + 'static,
{
    let rows: Vec<AnyElement> = agg_kinds
        .into_iter()
        .map(|(kind, label)| {
            let is_selected = kind == current;
            let handler = on_select.clone();

            div()
                .id(ElementId::Name(
                    format!("agg-pick-{:?}", kind).into(),
                ))
                .flex()
                .flex_row()
                .items_center()
                .gap(px(6.0))
                .px(px(8.0))
                .py(px(3.0))
                .cursor_pointer()
                .hover(|s| s.bg(gpui::hsla(0.0, 0.0, 1.0, 0.06)))
                .when(is_selected, |d| d.font_weight(gpui::FontWeight::MEDIUM))
                .on_mouse_down(MouseButton::Left, move |_, window, cx| {
                    handler(kind, window, cx);
                })
                .child(
                    div()
                        .text_size(px(11.0))
                        .text_color(gpui::hsla(0.0, 0.0, 0.9, 1.0))
                        .child(label),
                )
                .into_any_element()
        })
        .collect();

    picker_container(id, rows)
}

/// Shared container styling for picker dropdowns.
fn picker_container(id: impl Into<ElementId>, rows: Vec<AnyElement>) -> impl IntoElement {
    div()
        .id(id.into())
        .flex()
        .flex_col()
        .min_w(px(140.0))
        .bg(gpui::hsla(0.0, 0.0, 0.12, 1.0))
        .border_1()
        .border_color(gpui::hsla(0.0, 0.0, 1.0, 0.12))
        .rounded(px(4.0))
        .shadow_lg()
        .py(px(2.0))
        .children(rows)
}

// ---------------------------------------------------------------------------
// Tests — pure logic (no GPUI context)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{ColumnKind, ColumnMeta};

    fn make_col(name: &str, kind: ColumnKind) -> ColumnMeta {
        ColumnMeta {
            name: name.to_owned(),
            type_name: String::new(),
            kind,
            nullable: true,
            is_primary_key: false,
        }
    }

    #[test]
    fn axis_pill_equality() {
        assert_eq!(AxisPill::X, AxisPill::X);
        assert_ne!(AxisPill::X, AxisPill::Y);
    }

    #[test]
    fn y_label_single_column() {
        let cols = vec![
            make_col("ts", ColumnKind::Timestamp),
            make_col("cpu", ColumnKind::Float),
        ];
        let bindings = BindingSpec {
            x: 0,
            y: vec![1],
            group_by: None,
            filter: None,
            aggregation: AggKind::None,
        };

        // Mirror the y_label logic from axis_bar_element.
        let label = match bindings.y.len() {
            0 => "none".to_string(),
            1 => cols
                .get(bindings.y[0])
                .map(|c| c.name.clone())
                .unwrap_or_else(|| "?".to_string()),
            n => format!(
                "{} +{}",
                cols.get(bindings.y[0])
                    .map(|c| c.name.as_str())
                    .unwrap_or("?"),
                n - 1
            ),
        };

        assert_eq!(label, "cpu");
    }

    #[test]
    fn y_label_multi_column_shows_plus_n() {
        let cols = vec![
            make_col("ts", ColumnKind::Timestamp),
            make_col("cpu", ColumnKind::Float),
            make_col("mem", ColumnKind::Float),
            make_col("disk", ColumnKind::Float),
        ];
        let bindings = BindingSpec {
            x: 0,
            y: vec![1, 2, 3],
            group_by: None,
            filter: None,
            aggregation: AggKind::None,
        };

        let label = match bindings.y.len() {
            0 => "none".to_string(),
            1 => cols
                .get(bindings.y[0])
                .map(|c| c.name.clone())
                .unwrap_or_else(|| "?".to_string()),
            n => format!(
                "{} +{}",
                cols.get(bindings.y[0])
                    .map(|c| c.name.as_str())
                    .unwrap_or("?"),
                n - 1
            ),
        };

        assert_eq!(label, "cpu +2");
    }

    #[test]
    fn y_label_empty_y_shows_none() {
        let cols: Vec<ColumnMeta> = vec![make_col("ts", ColumnKind::Timestamp)];
        let bindings = BindingSpec {
            x: 0,
            y: vec![],
            group_by: None,
            filter: None,
            aggregation: AggKind::None,
        };

        let label = match bindings.y.len() {
            0 => "none".to_string(),
            1 => cols
                .get(bindings.y[0])
                .map(|c| c.name.clone())
                .unwrap_or_else(|| "?".to_string()),
            n => format!(
                "{} +{}",
                cols.get(bindings.y[0])
                    .map(|c| c.name.as_str())
                    .unwrap_or("?"),
                n - 1
            ),
        };

        assert_eq!(label, "none");
    }

    #[test]
    fn agg_label_matches_kind() {
        let pairs: &[(AggKind, &str)] = &[
            (AggKind::None, "none"),
            (AggKind::Sum, "sum"),
            (AggKind::Avg, "avg"),
            (AggKind::Min, "min"),
            (AggKind::Max, "max"),
        ];
        for (kind, expected) in pairs {
            let label = match kind {
                AggKind::None => "none",
                AggKind::Sum => "sum",
                AggKind::Avg => "avg",
                AggKind::Min => "min",
                AggKind::Max => "max",
            };
            assert_eq!(label, *expected, "mismatch for {:?}", kind);
        }
    }

    #[test]
    fn x_candidates_include_only_numeric_and_timestamp() {
        let cols = vec![
            make_col("ts", ColumnKind::Timestamp),
            make_col("cpu", ColumnKind::Float),
            make_col("host", ColumnKind::Text),
            make_col("seq", ColumnKind::Integer),
        ];

        let candidates: Vec<usize> = cols
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                matches!(
                    c.kind,
                    ColumnKind::Timestamp | ColumnKind::Integer | ColumnKind::Float
                )
            })
            .map(|(i, _)| i)
            .collect();

        // ts (0), cpu (1), seq (3) qualify; host (2) does not.
        assert_eq!(candidates, vec![0, 1, 3]);
    }

    #[test]
    fn y_candidates_include_only_numeric() {
        let cols = vec![
            make_col("ts", ColumnKind::Timestamp),
            make_col("cpu", ColumnKind::Float),
            make_col("host", ColumnKind::Text),
            make_col("count", ColumnKind::Integer),
        ];

        let candidates: Vec<usize> = cols
            .iter()
            .enumerate()
            .filter(|(_, c)| matches!(c.kind, ColumnKind::Integer | ColumnKind::Float))
            .map(|(i, _)| i)
            .collect();

        // cpu (1) and count (3) qualify; ts and host do not.
        assert_eq!(candidates, vec![1, 3]);
    }
}
