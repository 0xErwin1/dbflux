//! The grid's row list and cell painter, over [`crate::data`].
//!
//! A row-and-cell renderer that reads only the data contract: it asks a
//! [`RowSource`] for columns, cells and row states and paints them with the
//! design system's tokens and primitives. Everything it needs beyond the
//! data — the active cell, the selection, the inline editor, the reference
//! row, the icons a nested value and a reference row draw — the host
//! supplies through [`GridHost`], and the host's answers to clicks arrive
//! as callbacks, so this module owns no interaction policy and no data
//! ownership of its own.
//!
//! The rendering decisions this module pins, asserted in `tests.rs`: the
//! background order per cell (group wash, dirty mark, selection, transition
//! wash, active chamfer), the color precedence between a transition and its
//! tone, italic and strike per tone and row state, the two-tone span
//! suppression (a secondary span replaces the cell's own display text), and
//! the nested braces plus the array-only chevron.

#[cfg(test)]
mod tests;

use std::ops::Range;
use std::sync::Arc;

use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, App, ClickEvent, Hsla, InteractiveElement, IntoElement, MouseDownEvent,
    ParentElement, Pixels, SharedString, StatefulInteractiveElement, Styled, Window, div, px,
};
use gpui_component::Theme;

use crate::data::{
    CellAlignment, CellContent, CellTone, CellView, NestedMarker, RowSource, RowVisualState,
};
use crate::icon::IconSource;
use crate::primitives::{Chamfer, ChamferRing, Icon};
use crate::tokens::{ChromeColors, CollectionMetrics, GridMetrics, RowColors};

/// Coordinate of one cell in the grid, as the renderer addresses it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GridCellCoord {
    /// Row index in the order the grid paints.
    pub row: usize,
    /// Column index.
    pub col: usize,
}

impl GridCellCoord {
    pub fn new(row: usize, col: usize) -> Self {
        Self { row, col }
    }
}

/// The host state one row render reads: the facts the data contract does
/// not carry and the interaction the renderer must not own.
///
/// The click callbacks are `Arc` because gpui handlers are `'static`; the
/// selection test is borrowed because the renderer calls it synchronously
/// while it paints.
pub struct GridHost<'a> {
    /// Width per column index, as the host has laid the columns out.
    pub column_widths: &'a [f32],
    /// The cell the keyboard cursor sits on. It draws the active wash and
    /// ring, and its row draws the active-row background.
    pub active: Option<GridCellCoord>,
    /// The cell that hosts the inline editor.
    pub editing: Option<GridCellCoord>,
    /// Whether a cell lies inside the host's selection. The selection
    /// semantics (anchor, ranges, whole rows) stay the host's.
    pub is_selected: &'a dyn Fn(GridCellCoord) -> bool,
    /// Whether the grid pins a reference row on top: row 0 then draws as the
    /// reference and the rest are numbered from 1.
    pub reference_row: bool,
    /// A cell was left-clicked. The event carries the click count, so the
    /// host distinguishes a double-click (step into the editor) from a
    /// single one (move the selection).
    pub on_cell_click: Arc<dyn Fn(GridCellCoord, &ClickEvent, &mut Window, &mut App)>,
    /// A cell was right-clicked; the host opens its context menu.
    pub on_cell_context: Arc<dyn Fn(GridCellCoord, &MouseDownEvent, &mut Window, &mut App)>,
    /// Icon drawn before a nested-value summary (the braces marker).
    pub braces_icon: IconSource,
    /// Icon drawn after an array summary, marking the step-in affordance.
    pub chevron_icon: IconSource,
    /// Icon drawn in the gutter of the reference row.
    pub reference_icon: IconSource,
}

/// The paint decisions one cell derives from the [`CellView`] contract plus
/// the host state that is not data (the active cell, the row's pending
/// delete). Split from the element builder so every branch the painter takes
/// is assertable without pixels.
#[derive(Clone)]
pub struct CellPaint {
    /// The color of the cell's leading text.
    pub text_color: Hsla,
    pub italic: bool,
    pub line_through: bool,
    /// The relational dirty mark: a dirty background and a 2px warning
    /// accent. A document grid stages its edits as transitions instead.
    pub dirty_mark: bool,
    /// The document transition: a warning wash and a chamfer, whatever the
    /// tone says.
    pub transition: bool,
    /// The group wash of a grouped document column.
    pub group_wash: bool,
}

/// Derive one cell's paint decisions from its view, its column's group
/// membership, its row's pending-delete state and whether it is active.
pub fn cell_paint(
    view: &CellView,
    group_child: bool,
    row_pending_delete: bool,
    is_active: bool,
    theme: &Theme,
    null_color: Hsla,
) -> CellPaint {
    let transition = matches!(view.content, CellContent::Transition { .. });
    let text_color = if transition {
        // The transition takes the color precedence over the tone.
        theme.warning
    } else if row_pending_delete && !matches!(view.tone, CellTone::Missing | CellTone::Nested) {
        theme.muted_foreground
    } else {
        match view.tone {
            CellTone::Missing => theme.input,
            CellTone::Nested => theme.info,
            CellTone::PendingDelete | CellTone::Muted => theme.muted_foreground,
            CellTone::Null => null_color,
            CellTone::Normal | CellTone::PendingInsert | CellTone::Error => {
                if is_active {
                    ChromeColors::strong(theme)
                } else {
                    theme.foreground
                }
            }
        }
    };

    CellPaint {
        text_color,
        italic: matches!(
            view.tone,
            CellTone::Null | CellTone::Muted | CellTone::Missing
        ),
        line_through: row_pending_delete,
        // The mark follows the edit-state fact, not the tone: a staged null
        // draws the null styling and the mark at once, which a single tone
        // cannot express.
        dirty_mark: !transition && view.dirty,
        transition,
        group_wash: group_child,
    }
}

/// Renders the visible rows for a `uniform_list`.
///
/// `editor` is the inline editor element the host composed for its editing
/// cell (a dropdown when the cell enumerates values, a text input
/// otherwise), or nothing when the cell cannot open an editor yet. It is
/// consumed by the one visible cell that is editing; an editing cell with
/// no editor paints as a plain cell, as before.
#[allow(clippy::too_many_arguments)]
pub fn render_rows(
    visible_range: Range<usize>,
    source: &dyn RowSource,
    host: &GridHost<'_>,
    editor: Option<AnyElement>,
    total_width: f32,
    font_size: Pixels,
    row_height: Pixels,
    nested_icon: Pixels,
    null_color: Hsla,
    theme: &Theme,
) -> Vec<AnyElement> {
    let mut editor = editor;

    visible_range
        .map(|visual_ix| {
            let row_state = source.row_state(visual_ix);

            // Row background based on state
            // - Dirty: cell-level only (no row bg)
            // - Saving: warning background
            // - Error: danger background
            // - PendingInsert: green-ish to indicate new row
            // - PendingDelete: red-ish with visual indication of deletion
            let row_bg = match row_state {
                RowVisualState::Dirty => None, // Cell-level only — see dirty cell highlight below
                RowVisualState::Saving => Some(RowColors::saving(theme)),
                RowVisualState::Error => Some(RowColors::error(theme)),
                RowVisualState::Clean => None,
                RowVisualState::PendingInsert => Some(RowColors::insert(theme)),
                RowVisualState::PendingDelete => Some(RowColors::delete(theme)),
            };

            let is_pending_delete = row_state == RowVisualState::PendingDelete;
            let is_active_row = host.active.is_some_and(|active| active.row == visual_ix);
            let tint = ChromeColors::tint(theme);
            let cell_wash = tint.opacity(GridMetrics::CELL_SELECTED_ALPHA);
            let is_reference_row = host.reference_row && visual_ix == 0;
            // Under a reference row the others are numbered from 1, so the
            // number reads as their rank.
            let row_number = if host.reference_row {
                visual_ix
            } else {
                visual_ix + 1
            };

            // The column facts the cells read, once per row render.
            let group_children: Vec<bool> = (0..source.column_count())
                .map(|col_ix| {
                    source
                        .column(col_ix)
                        .is_some_and(|column| column.group_child)
                })
                .collect();

            let cells: Vec<AnyElement> = (0..source.column_count())
                .map(|col_ix| {
                    let view = source.cell(visual_ix, col_ix).unwrap_or_else(|| {
                        CellView::new(
                            SharedString::default(),
                            CellTone::Normal,
                            CellAlignment::Left,
                        )
                    });
                    let width = column_width_or(host.column_widths, col_ix);
                    let coord = GridCellCoord::new(visual_ix, col_ix);
                    let is_selected = (host.is_selected)(coord);
                    let is_active = host.active == Some(coord);
                    let is_editing = host.editing == Some(coord);

                    if is_editing && let Some(editor_element) = editor.take() {
                        return div()
                            .id(("cell", visual_ix * 10000 + col_ix))
                            .flex()
                            .flex_shrink_0()
                            .items_center()
                            .h_full()
                            .w(px(width))
                            .overflow_hidden()
                            .border_1()
                            .border_color(theme.ring)
                            .bg(theme.background)
                            .child(editor_element)
                            .into_any_element();
                    }

                    let paint = cell_paint(
                        &view,
                        group_children.get(col_ix).copied().unwrap_or(false),
                        is_pending_delete,
                        is_active,
                        theme,
                        null_color,
                    );

                    // The text the cell leads with: the contract's plain text,
                    // or the transition composed for drawing.
                    let content_text: SharedString = match &view.content {
                        CellContent::Plain(text) => text.clone(),
                        CellContent::Transition { from, to } => {
                            format!("{from} \u{2192} {to}").into()
                        }
                    };
                    let secondary = view.secondary.clone();
                    let has_secondary = secondary.is_some();

                    div()
                        .id(("cell", visual_ix * 10000 + col_ix))
                        .relative()
                        .flex()
                        .flex_shrink_0()
                        .items_center()
                        .h_full()
                        .w(px(width))
                        .px(GridMetrics::CELL_PADDING_X)
                        .overflow_hidden()
                        .cursor_pointer()
                        // Highlight individual dirty cells (like DBeaver).
                        // Uses RowColors::dirty for the background and the
                        // theme warning for the 2px left accent stroke.
                        .when(paint.group_wash, |d| {
                            d.bg(tint.opacity(CollectionMetrics::GROUP_CHILD_CELL_ALPHA))
                        })
                        .when(paint.dirty_mark, |d| {
                            d.bg(RowColors::dirty(theme))
                                .border_l_2()
                                .border_color(theme.warning)
                        })
                        .when(is_selected || is_active, |d| d.bg(cell_wash))
                        .when(paint.transition, |d| {
                            d.bg(theme.warning.opacity(CollectionMetrics::EDITED_CELL_ALPHA))
                                .child(
                                    Chamfer::new(Pixels::ZERO)
                                        .ring(ChamferRing::focus(theme.warning)),
                                )
                        })
                        // Focused cell: 1.5 px tint ring inside the cell. No
                        // cut: cells hold data.
                        .when(is_active, |d| {
                            d.child(Chamfer::new(Pixels::ZERO).ring(ChamferRing::focus(tint)))
                        })
                        .when(paint.italic, |d| d.italic())
                        .when(paint.line_through, |d| d.line_through())
                        .on_click({
                            let on_cell_click = Arc::clone(&host.on_cell_click);
                            move |event: &ClickEvent, window, cx| {
                                on_cell_click(coord, event, window, cx)
                            }
                        })
                        .on_mouse_down(gpui::MouseButton::Right, {
                            let on_cell_context = Arc::clone(&host.on_cell_context);
                            move |event: &MouseDownEvent, window, cx| {
                                on_cell_context(coord, event, window, cx)
                            }
                        })
                        .when_some(view.nested, |d, _| {
                            d.gap(GridMetrics::HEADER_GAP).child(
                                div()
                                    .debug_selector(|| "table-cell-braces".to_string())
                                    .child(
                                        Icon::new(host.braces_icon.clone())
                                            .size(nested_icon)
                                            .color(paint.text_color),
                                    ),
                            )
                        })
                        // A resolved foreign key draws its label in the
                        // cell's text color with the raw id muted after it;
                        // the pair replaces the cell's own display text.
                        .when_some(secondary, |d, span| {
                            d.child(
                                div()
                                    .debug_selector(|| "table-cell-label".to_string())
                                    .min_w_0()
                                    .truncate()
                                    .text_size(font_size)
                                    .text_color(paint.text_color)
                                    .child(content_text.clone()),
                            )
                            .child(
                                div()
                                    .debug_selector(|| "table-cell-secondary".to_string())
                                    .flex_shrink_0()
                                    .whitespace_nowrap()
                                    .text_size(font_size)
                                    .text_color(match span.tone {
                                        CellTone::Missing => theme.input,
                                        CellTone::Nested => theme.info,
                                        CellTone::PendingDelete | CellTone::Muted => {
                                            theme.muted_foreground
                                        }
                                        CellTone::Null => null_color,
                                        CellTone::Normal
                                        | CellTone::PendingInsert
                                        | CellTone::Error => theme.foreground,
                                    })
                                    .child(span.text.to_string()),
                            )
                        })
                        .when(!has_secondary, |d| {
                            d.child(
                                div()
                                    .debug_selector(|| "table-cell-text".to_string())
                                    .min_w_0()
                                    .truncate()
                                    .text_size(font_size)
                                    .text_color(paint.text_color)
                                    .child(content_text),
                            )
                        })
                        .when(view.nested == Some(NestedMarker { is_array: true }), |d| {
                            d.child(
                                div()
                                    .debug_selector(|| "table-cell-chevron".to_string())
                                    .child(
                                        Icon::new(host.chevron_icon.clone())
                                            .size(nested_icon)
                                            .color(paint.text_color),
                                    ),
                            )
                        })
                        .into_any_element()
                })
                .collect();

            div()
                .id(("row", visual_ix))
                .flex()
                .flex_shrink_0()
                .w(px(total_width))
                .h(row_height)
                .overflow_hidden()
                .border_b_1()
                .border_color(theme.table_row_border)
                // Row state background (dirty=yellow, error=red)
                .when_some(row_bg, |d, bg| d.bg(bg))
                // The row holding the focused cell gets the selected-row
                // wash; clean rows otherwise alternate.
                .when(row_bg.is_none() && is_reference_row, |d| {
                    d.bg(tint.opacity(GridMetrics::REFERENCE_ROW_ALPHA))
                        .border_color(tint.opacity(GridMetrics::REFERENCE_ROW_BORDER_ALPHA))
                })
                .when(
                    row_bg.is_none() && !is_reference_row && is_active_row,
                    |d| d.bg(theme.table_active),
                )
                .when(
                    row_bg.is_none() && !is_reference_row && !is_active_row && visual_ix % 2 == 1,
                    |d| d.bg(theme.table_even),
                )
                .child(
                    div()
                        .flex()
                        .flex_shrink_0()
                        .items_center()
                        .justify_end()
                        .w(GridMetrics::ROW_NUMBER_WIDTH)
                        .h_full()
                        .pr(GridMetrics::CELL_PADDING_X)
                        .text_size(font_size)
                        .text_color(if is_active_row || is_reference_row {
                            tint
                        } else {
                            theme.input
                        })
                        .map(|gutter| {
                            if is_reference_row {
                                gutter.child(
                                    Icon::new(host.reference_icon.clone())
                                        .size(nested_icon)
                                        .color(tint),
                                )
                            } else {
                                gutter.child(row_number.to_string())
                            }
                        }),
                )
                .children(cells)
                .into_any_element()
        })
        .collect()
}

/// The width column `col_ix` draws at: the host's laid-out width, or the
/// grid's default for a column the host has not sized.
fn column_width_or(column_widths: &[f32], col_ix: usize) -> f32 {
    column_widths
        .get(col_ix)
        .copied()
        .unwrap_or(f32::from(GridMetrics::DEFAULT_COLUMN_WIDTH))
}
