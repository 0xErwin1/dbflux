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
use gpui::{
    Anchor, AnyElement, App, ElementId, Pixels, SharedString, Window, anchored, deferred, div,
    point, px,
};

use crate::chart::spec::{AggKind, BindingSpec};
use crate::icons::AppIcon;
use crate::primitives::{Chamfer, Icon};
use crate::semantic::ChartColors;
use crate::tokens::{AxisBarMetrics, ChamferCut, ChartGeometry, Fields, FontSizes};
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

/// Translated label shown on the "Agg" pill for a given aggregation kind.
fn agg_label(kind: AggKind) -> String {
    match kind {
        AggKind::None => dbflux_i18n::t!("chart.axis_bar.aggregation.none"),
        AggKind::Sum => dbflux_i18n::t!("chart.axis_bar.aggregation.sum"),
        AggKind::Avg => dbflux_i18n::t!("chart.axis_bar.aggregation.avg"),
        AggKind::Min => dbflux_i18n::t!("chart.axis_bar.aggregation.min"),
        AggKind::Max => dbflux_i18n::t!("chart.axis_bar.aggregation.max"),
    }
}

/// One row of an axis picker: what choosing it binds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AxisPickerOption {
    /// A column for the X axis.
    X(usize),
    /// A Y column and whether it is plotted now; choosing it toggles it.
    Y { column: usize, checked: bool },
    /// A group-by column, or `None` for no grouping.
    Group(Option<usize>),
    /// An aggregation kind.
    Agg(AggKind),
}

impl AxisPickerOption {
    /// Whether this row is what `bindings` holds now (the bound X column,
    /// a plotted Y column, the group-by column, the aggregation).
    pub fn is_current(&self, bindings: &BindingSpec) -> bool {
        match *self {
            AxisPickerOption::X(column) => bindings.x == column,
            AxisPickerOption::Y { checked, .. } => checked,
            AxisPickerOption::Group(column) => bindings.group_by == column,
            AxisPickerOption::Agg(kind) => bindings.aggregation == kind,
        }
    }

    /// `bindings` with this row chosen, as clicking the row does.
    pub fn apply(&self, bindings: &BindingSpec) -> BindingSpec {
        let mut next = bindings.clone();

        match *self {
            AxisPickerOption::X(column) => next.x = column,
            AxisPickerOption::Y { column, checked } => {
                if checked {
                    next.y.retain(|index| *index != column);
                } else if !next.y.contains(&column) {
                    next.y.push(column);
                }
            }
            AxisPickerOption::Group(column) => next.group_by = column,
            AxisPickerOption::Agg(kind) => next.aggregation = kind,
        }

        next
    }
}

/// The rows the picker of `pill` lists, in display order: X takes time and
/// numeric columns, Y numeric columns, Group "none" then text columns, Agg
/// every aggregation kind. Column roles come from `ColumnKind` only.
pub fn axis_picker_options(
    pill: AxisPill,
    bindings: &BindingSpec,
    columns: &[ColumnMeta],
) -> Vec<AxisPickerOption> {
    match pill {
        AxisPill::X => columns
            .iter()
            .enumerate()
            .filter(|(_, column)| {
                matches!(
                    column.kind,
                    ColumnKind::Timestamp | ColumnKind::Integer | ColumnKind::Float
                )
            })
            .map(|(index, _)| AxisPickerOption::X(index))
            .collect(),
        AxisPill::Y => columns
            .iter()
            .enumerate()
            .filter(|(_, column)| matches!(column.kind, ColumnKind::Integer | ColumnKind::Float))
            .map(|(index, _)| AxisPickerOption::Y {
                column: index,
                checked: bindings.y.contains(&index),
            })
            .collect(),
        AxisPill::Group => std::iter::once(AxisPickerOption::Group(None))
            .chain(
                columns
                    .iter()
                    .enumerate()
                    .filter(|(_, column)| matches!(column.kind, ColumnKind::Text))
                    .map(|(index, _)| AxisPickerOption::Group(Some(index))),
            )
            .collect(),
        AxisPill::Agg => [
            AggKind::None,
            AggKind::Sum,
            AggKind::Avg,
            AggKind::Min,
            AggKind::Max,
        ]
        .into_iter()
        .map(AxisPickerOption::Agg)
        .collect(),
    }
}

/// The untranslated name the aggregation picker lists for `kind`.
fn agg_row(kind: AggKind) -> &'static str {
    match kind {
        AggKind::None => "none",
        AggKind::Sum => "sum",
        AggKind::Avg => "avg",
        AggKind::Min => "min",
        AggKind::Max => "max",
    }
}

/// The name a picker row shows for `column`.
fn column_name(columns: &[ColumnMeta], column: usize) -> SharedString {
    columns
        .get(column)
        .map(|meta| SharedString::from(meta.name.clone()))
        .unwrap_or_default()
}

/// Render the AxisBar pill row.
///
/// # Parameters
///
/// - `bindings`: current `BindingSpec`; drives pill labels.
/// - `columns`: column metadata from the current `QueryResult`.
/// - `open_pill`: which pill's picker is currently shown (`None` = all closed).
/// - `highlighted`: the row of the open picker the keyboard is on, an index
///   into [`axis_picker_options`] for that pill.
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
    highlighted: Option<usize>,
    colors: &ChartColors,
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

    let agg_label: SharedString = agg_label(bindings.aggregation).into();

    let x_icon = match columns.get(bindings.x).map(|column| column.kind) {
        Some(ColumnKind::Timestamp) => AppIcon::Clock,
        _ => AppIcon::Hash,
    };

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
            AxisPillStyle {
                icon: x_icon,
                width: AxisBarMetrics::X_WIDTH,
            },
            x_label,
            x_open,
            colors,
            move |w, cx| handler(AxisPill::X, w, cx),
        )
    };

    // X picker dropdown (shown when x_open == true)
    let x_picker: Option<AnyElement> = if x_open {
        let x_candidates: Vec<(usize, SharedString)> =
            axis_picker_options(AxisPill::X, bindings, columns)
                .into_iter()
                .filter_map(|option| match option {
                    AxisPickerOption::X(index) => Some((index, column_name(columns, index))),
                    _ => None,
                })
                .collect();

        Some(
            column_picker_element(
                "axis-picker-x",
                x_candidates,
                Some(bindings.x),
                highlighted,
                colors,
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
            AxisPillStyle {
                icon: AppIcon::Hash,
                width: AxisBarMetrics::FIELD_WIDTH,
            },
            y_label,
            y_open,
            colors,
            move |w, cx| handler(AxisPill::Y, w, cx),
        )
    };

    // Y picker (multi-select: show all numeric columns with checkboxes)
    let y_picker: Option<AnyElement> = if y_open {
        let y_candidates: Vec<(usize, SharedString, bool)> =
            axis_picker_options(AxisPill::Y, bindings, columns)
                .into_iter()
                .filter_map(|option| match option {
                    AxisPickerOption::Y { column, checked } => {
                        Some((column, column_name(columns, column), checked))
                    }
                    _ => None,
                })
                .collect();

        Some(
            y_picker_element(
                "axis-picker-y",
                y_candidates,
                highlighted,
                colors,
                move |col_idx, checked, w, cx| {
                    on_y_toggle(col_idx, checked, w, cx);
                },
            )
            .into_any_element(),
        )
    } else {
        None
    };

    // Group pill
    let group_role = dbflux_i18n::t!("chart.axis_bar.group");
    let group_pill = {
        let handler = on_pill_click.clone();
        pill_element(
            "axis-pill-group",
            &group_role,
            AxisPillStyle {
                icon: AppIcon::Tag,
                width: AxisBarMetrics::FIELD_WIDTH,
            },
            group_label,
            group_open,
            colors,
            move |w, cx| handler(AxisPill::Group, w, cx),
        )
    };

    // Group picker (single-select from Text columns, plus "none")
    let group_picker: Option<AnyElement> = if group_open {
        let group_candidates: Vec<(Option<usize>, SharedString)> =
            axis_picker_options(AxisPill::Group, bindings, columns)
                .into_iter()
                .filter_map(|option| match option {
                    AxisPickerOption::Group(None) => Some((None, SharedString::from("—"))),
                    AxisPickerOption::Group(Some(index)) => {
                        Some((Some(index), column_name(columns, index)))
                    }
                    _ => None,
                })
                .collect();

        let current = bindings.group_by;
        Some(
            group_picker_element(
                "axis-picker-group",
                group_candidates,
                current,
                highlighted,
                colors,
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
            AxisPillStyle {
                icon: AppIcon::Sigma,
                width: AxisBarMetrics::AGG_WIDTH,
            },
            agg_label,
            agg_open,
            colors,
            move |w, cx| handler(AxisPill::Agg, w, cx),
        )
    };

    // Agg picker (enum dropdown)
    let agg_picker: Option<AnyElement> = if agg_open {
        let agg_kinds: Vec<(AggKind, SharedString)> =
            axis_picker_options(AxisPill::Agg, bindings, columns)
                .into_iter()
                .filter_map(|option| match option {
                    AxisPickerOption::Agg(kind) => Some((kind, SharedString::from(agg_row(kind)))),
                    _ => None,
                })
                .collect();
        let current = bindings.aggregation;

        Some(
            agg_picker_element(
                "axis-picker-agg",
                agg_kinds,
                current,
                highlighted,
                colors,
                move |kind, w, cx| {
                    on_agg_select(kind, w, cx);
                },
            )
            .into_any_element(),
        )
    } else {
        None
    };

    // Assemble the bar: pills in a row, each with its picker floating below.
    div()
        .flex()
        .flex_row()
        .flex_wrap()
        .items_center()
        .gap(AxisBarMetrics::GAP)
        .child(pill_group("axis-x-group", x_pill, x_picker))
        .child(pill_group("axis-y-group", y_pill, y_picker))
        .child(pill_group("axis-group-group", group_pill, group_picker))
        .child(pill_group("axis-agg-group", agg_pill, agg_picker))
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Leading icon and width of an axis field.
struct AxisPillStyle {
    icon: AppIcon,
    width: Pixels,
}

/// Build a single axis field (P1Chart): the role ("X", "Y", "Group") in
/// muted text, then a 30 px chamfered select with a leading icon, the bound
/// column and a chevron. The open field keeps its hover fill.
#[allow(clippy::too_many_arguments)]
fn pill_element(
    id: impl Into<ElementId>,
    role: &str,
    style: AxisPillStyle,
    value: SharedString,
    active: bool,
    colors: &ChartColors,
    on_click: impl Fn(&mut Window, &mut App) + Send + Sync + 'static,
) -> impl IntoElement {
    let id: ElementId = id.into();

    let shape = Chamfer::new(ChamferCut::CONTROL)
        .fill(if active {
            colors.panel_border
        } else {
            colors.pill_bg
        })
        .fill_hover(colors.panel_border)
        .border(colors.panel_border)
        .interactive(ElementId::Name(format!("{id}-shape").into()));

    div()
        .flex()
        .flex_row()
        .items_center()
        .gap(AxisBarMetrics::GAP)
        .child(
            div()
                .text_size(AxisBarMetrics::ROLE_FONT)
                .text_color(colors.label_fg)
                .child(SharedString::from(role.to_string())),
        )
        .child(
            div()
                .id(id)
                .relative()
                .flex()
                .flex_row()
                .items_center()
                .gap(Fields::GAP)
                .w(style.width)
                .h(Fields::HEIGHT)
                .px(Fields::PADDING_X)
                .cursor_pointer()
                .on_click(move |_, window, cx| {
                    on_click(window, cx);
                })
                .child(shape)
                .child(
                    Icon::new(style.icon)
                        .size(Fields::LEADING_ICON)
                        .color(colors.label_fg),
                )
                .child(
                    div()
                        .flex_1()
                        .min_w_0()
                        .truncate()
                        .text_size(Fields::TEXT)
                        .text_color(colors.value_fg)
                        .child(value),
                )
                .child(
                    Icon::new(AppIcon::ChevronDown)
                        .size(Fields::CHEVRON)
                        .color(colors.label_fg),
                ),
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
    let mut container = div().id(id.into()).relative().flex().flex_col().child(pill);

    if let Some(picker_el) = picker {
        container = container.child(
            deferred(
                anchored()
                    .anchor(Anchor::TopLeft)
                    .offset(point(px(0.0), AxisBarMetrics::PICKER_OFFSET))
                    .snap_to_window()
                    .child(picker_el),
            )
            .with_priority(1),
        );
    }

    container
}

/// A single-select column picker rendered as a vertical list.
fn column_picker_element<F>(
    id: impl Into<ElementId>,
    candidates: Vec<(usize, SharedString)>,
    selected: Option<usize>,
    highlighted: Option<usize>,
    colors: &ChartColors,
    on_select: F,
) -> impl IntoElement
where
    F: Fn(usize, &mut Window, &mut App) + Clone + Send + Sync + 'static,
{
    let rows: Vec<AnyElement> = candidates
        .into_iter()
        .enumerate()
        .map(|(row, (col_idx, label))| {
            let is_selected = selected == Some(col_idx);
            let handler = on_select.clone();
            let hover_bg = colors.hover_bg;
            let value_fg = colors.value_fg;

            div()
                .id(ElementId::Name(format!("col-pick-{}", col_idx).into()))
                .when(highlighted == Some(row), |d| d.bg(colors.panel_border))
                .flex()
                .flex_row()
                .items_center()
                .gap(Fields::CHECKBOX_GAP)
                .h(Fields::MENU_ROW_HEIGHT)
                .mx(Fields::MENU_ROW_INSET)
                .px(Fields::PADDING_X)
                .cursor_pointer()
                .hover(move |s| s.bg(hover_bg))
                .when(is_selected, |d| d.font_weight(gpui::FontWeight::MEDIUM))
                .on_click(move |_, window, cx| {
                    handler(col_idx, window, cx);
                })
                .child(
                    div()
                        .text_size(FontSizes::BASE)
                        .text_color(value_fg)
                        .child(label),
                )
                .into_any_element()
        })
        .collect();

    picker_container(id, rows, colors)
}

/// A multi-select Y-column picker with one checkbox row per candidate.
fn y_picker_element<F>(
    id: impl Into<ElementId>,
    candidates: Vec<(usize, SharedString, bool)>,
    highlighted: Option<usize>,
    colors: &ChartColors,
    on_toggle: F,
) -> impl IntoElement
where
    F: Fn(usize, bool, &mut Window, &mut App) + Clone + Send + Sync + 'static,
{
    let rows: Vec<AnyElement> = candidates
        .into_iter()
        .enumerate()
        .map(|(row, (col_idx, label, checked))| {
            let handler = on_toggle.clone();
            let hover_bg = colors.hover_bg;
            let pill_border = colors.pill_border;
            let checkbox_checked = colors.checkbox_checked;
            let value_fg = colors.value_fg;

            div()
                .id(ElementId::Name(format!("y-pick-{}", col_idx).into()))
                .when(highlighted == Some(row), |d| d.bg(colors.panel_border))
                .flex()
                .flex_row()
                .items_center()
                .gap(Fields::CHECKBOX_GAP)
                .h(Fields::MENU_ROW_HEIGHT)
                .mx(Fields::MENU_ROW_INSET)
                .px(Fields::PADDING_X)
                .cursor_pointer()
                .hover(move |s| s.bg(hover_bg))
                .on_click(move |_, window, cx| {
                    handler(col_idx, !checked, window, cx);
                })
                .child(
                    // Checkbox indicator
                    div()
                        .size(Fields::CHECKBOX_SIZE)
                        .border_1()
                        .border_color(pill_border)
                        .bg(if checked {
                            checkbox_checked
                        } else {
                            gpui::transparent_black()
                        }),
                )
                .child(
                    div()
                        .text_size(FontSizes::BASE)
                        .text_color(value_fg)
                        .child(label),
                )
                .into_any_element()
        })
        .collect();

    picker_container(id, rows, colors)
}

/// A single-select picker for group-by column (includes a "none" option).
fn group_picker_element<F>(
    id: impl Into<ElementId>,
    candidates: Vec<(Option<usize>, SharedString)>,
    selected: Option<usize>,
    highlighted: Option<usize>,
    colors: &ChartColors,
    on_select: F,
) -> impl IntoElement
where
    F: Fn(Option<usize>, &mut Window, &mut App) + Clone + Send + Sync + 'static,
{
    let rows: Vec<AnyElement> = candidates
        .into_iter()
        .enumerate()
        .map(|(row, (col_idx_opt, label))| {
            let is_selected = col_idx_opt == selected;
            let handler = on_select.clone();
            let hover_bg = colors.hover_bg;
            let value_fg = colors.value_fg;

            div()
                .id(ElementId::Name(
                    format!(
                        "grp-pick-{}",
                        col_idx_opt
                            .map(|i| i.to_string())
                            .unwrap_or_else(|| "none".to_string())
                    )
                    .into(),
                ))
                .when(highlighted == Some(row), |d| d.bg(colors.panel_border))
                .flex()
                .flex_row()
                .items_center()
                .gap(Fields::CHECKBOX_GAP)
                .h(Fields::MENU_ROW_HEIGHT)
                .mx(Fields::MENU_ROW_INSET)
                .px(Fields::PADDING_X)
                .cursor_pointer()
                .hover(move |s| s.bg(hover_bg))
                .when(is_selected, |d| d.font_weight(gpui::FontWeight::MEDIUM))
                .on_click(move |_, window, cx| {
                    handler(col_idx_opt, window, cx);
                })
                .child(
                    div()
                        .text_size(FontSizes::BASE)
                        .text_color(value_fg)
                        .child(label),
                )
                .into_any_element()
        })
        .collect();

    picker_container(id, rows, colors)
}

/// A single-select picker for `AggKind`.
fn agg_picker_element<F>(
    id: impl Into<ElementId>,
    agg_kinds: Vec<(AggKind, SharedString)>,
    current: AggKind,
    highlighted: Option<usize>,
    colors: &ChartColors,
    on_select: F,
) -> impl IntoElement
where
    F: Fn(AggKind, &mut Window, &mut App) + Clone + Send + Sync + 'static,
{
    let rows: Vec<AnyElement> = agg_kinds
        .into_iter()
        .enumerate()
        .map(|(row, (kind, label))| {
            let is_selected = kind == current;
            let handler = on_select.clone();
            let hover_bg = colors.hover_bg;
            let value_fg = colors.value_fg;

            div()
                .id(ElementId::Name(format!("agg-pick-{:?}", kind).into()))
                .when(highlighted == Some(row), |d| d.bg(colors.panel_border))
                .flex()
                .flex_row()
                .items_center()
                .gap(Fields::CHECKBOX_GAP)
                .h(Fields::MENU_ROW_HEIGHT)
                .mx(Fields::MENU_ROW_INSET)
                .px(Fields::PADDING_X)
                .cursor_pointer()
                .hover(move |s| s.bg(hover_bg))
                .when(is_selected, |d| d.font_weight(gpui::FontWeight::MEDIUM))
                .on_click(move |_, window, cx| {
                    handler(kind, window, cx);
                })
                .child(
                    div()
                        .text_size(FontSizes::BASE)
                        .text_color(value_fg)
                        .child(label),
                )
                .into_any_element()
        })
        .collect();

    picker_container(id, rows, colors)
}

/// Shared container styling for picker dropdowns: the overlay chamfer on
/// the raised fill with the strong line, rows inset like a select menu.
fn picker_container(
    id: impl Into<ElementId>,
    rows: Vec<AnyElement>,
    colors: &ChartColors,
) -> impl IntoElement {
    div()
        .id(id.into())
        .relative()
        .flex()
        .flex_col()
        .min_w(ChartGeometry::DROPDOWN_PANEL)
        .max_h(Fields::MENU_MAX_HEIGHT)
        .overflow_y_scroll()
        .py(Fields::MENU_PADDING_Y)
        .shadow_lg()
        .occlude()
        .child(
            Chamfer::new(ChamferCut::OVERLAY)
                .fill(colors.pill_bg)
                .border(colors.pill_border),
        )
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
    fn picker_options_follow_the_column_kinds() {
        let cols = [
            make_col("ts", ColumnKind::Timestamp),
            make_col("cpu", ColumnKind::Float),
            make_col("host", ColumnKind::Text),
            make_col("seq", ColumnKind::Integer),
        ];
        let bindings = BindingSpec {
            x: 0,
            y: vec![1],
            group_by: None,
            filter: None,
            aggregation: AggKind::None,
        };

        assert_eq!(
            axis_picker_options(AxisPill::X, &bindings, &cols),
            vec![
                AxisPickerOption::X(0),
                AxisPickerOption::X(1),
                AxisPickerOption::X(3)
            ]
        );
        assert_eq!(
            axis_picker_options(AxisPill::Y, &bindings, &cols),
            vec![
                AxisPickerOption::Y {
                    column: 1,
                    checked: true
                },
                AxisPickerOption::Y {
                    column: 3,
                    checked: false
                },
            ]
        );
        assert_eq!(
            axis_picker_options(AxisPill::Group, &bindings, &cols),
            vec![
                AxisPickerOption::Group(None),
                AxisPickerOption::Group(Some(2))
            ]
        );
        assert_eq!(
            axis_picker_options(AxisPill::Agg, &bindings, &cols).len(),
            5
        );
    }

    #[test]
    fn choosing_a_picker_row_binds_it() {
        let bindings = BindingSpec {
            x: 0,
            y: vec![1],
            group_by: None,
            filter: None,
            aggregation: AggKind::None,
        };

        assert_eq!(AxisPickerOption::X(3).apply(&bindings).x, 3);
        assert_eq!(
            AxisPickerOption::Y {
                column: 3,
                checked: false
            }
            .apply(&bindings)
            .y,
            vec![1, 3],
            "an unchecked Y column is added"
        );
        assert!(
            AxisPickerOption::Y {
                column: 1,
                checked: true
            }
            .apply(&bindings)
            .y
            .is_empty(),
            "a checked Y column is removed"
        );
        assert_eq!(
            AxisPickerOption::Group(Some(2)).apply(&bindings).group_by,
            Some(2)
        );
        assert!(AxisPickerOption::X(0).is_current(&bindings));
        assert!(!AxisPickerOption::Group(Some(2)).is_current(&bindings));
    }

    #[test]
    fn axis_pill_equality() {
        assert_eq!(AxisPill::X, AxisPill::X);
        assert_ne!(AxisPill::X, AxisPill::Y);
    }

    #[test]
    fn y_label_single_column() {
        let cols = [
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
        let cols = [
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
            assert_eq!(agg_label(*kind), *expected, "mismatch for {:?}", kind);
        }
    }

    #[test]
    fn chart_axis_bar_aggregation_keys_resolve_in_both_locales() {
        let keys = [
            "chart.axis_bar.aggregation.none",
            "chart.axis_bar.aggregation.sum",
            "chart.axis_bar.aggregation.avg",
            "chart.axis_bar.aggregation.min",
            "chart.axis_bar.aggregation.max",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(
                !en.is_empty() && !en.starts_with("en."),
                "en missing for {key}, got {en:?}"
            );
            assert!(
                !es.is_empty() && !es.starts_with("es."),
                "es missing for {key}, got {es:?}"
            );
        }
    }

    #[test]
    fn axis_role_labels_are_accepted_untranslated_single_tokens() {
        for role in ["X", "Y", "Agg"] {
            assert_eq!(role.split_whitespace().count(), 1);
        }
    }

    #[test]
    fn agg_label_sum_diverges_between_locales() {
        let en = dbflux_i18n::t!("chart.axis_bar.aggregation.sum", locale = "en");
        let es = dbflux_i18n::t!("chart.axis_bar.aggregation.sum", locale = "es");
        assert_eq!(en, "sum");
        assert_eq!(es, "suma");
        assert_ne!(en, es);
    }

    #[test]
    fn x_candidates_include_only_numeric_and_timestamp() {
        let cols = [
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
        let cols = [
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

    #[test]
    fn axis_bar_group_key_resolves() {
        let en = dbflux_i18n::t!("chart.axis_bar.group", locale = "en");
        let es = dbflux_i18n::t!("chart.axis_bar.group", locale = "es");
        assert_eq!(en, "Group");
        assert_eq!(es, "Grupo");
        assert_ne!(en, es);
    }
}
