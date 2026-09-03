//! Record mode — the grid transposed onto a single row.
//!
//! The active row is laid out vertically as `name → value` pairs that fill the
//! whole result area, the way DBeaver's record view does. It is not a
//! read-only presentation: every field goes through the same
//! `DataTableState::start_editing` / `EditBuffer` path as a grid cell, so
//! dirty marks, Save Row, and revert behave identically in both modes.
//!
//! Navigation is transposed in `DataTableState` (see `effective_direction`),
//! not here: up/down walk the fields of the current row, left/right walk rows.

use std::ops::Range;

use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, App, ClickEvent, Entity, InteractiveElement, IntoElement, ListSizingBehavior,
    MouseButton, MouseDownEvent, ParentElement, SharedString, StatefulInteractiveElement, Styled,
    Window, div, uniform_list,
};
use gpui_component::scroll::Scrollbar;
use gpui_component::{ActiveTheme, Sizable};

use crate::controls::{GpuiInput as Input, InputState};
use crate::primitives::Text;
use crate::tokens::{FontSizes, RowColors, Spacing};

use super::events::DataTableEvent;
use super::model::{CellValue, EditBuffer, TableModel, VisualRowSource};
use super::selection::CellCoord;
use super::state::DataTableState;
use super::theme::{
    CELL_PADDING_X, HEADER_HEIGHT, RECORD_NAME_WIDTH, RECORD_ROW_HEIGHT, SCROLLBAR_WIDTH,
};

/// The row a record view is currently showing, together with the source it was
/// resolved from. Pending inserts read from the edit buffer, base rows from the
/// model — the same split `render_rows` makes for the grid.
struct RecordRow {
    /// Visual row index, which is what selection coordinates use.
    visual_ix: usize,
    source: Option<VisualRowSource>,
    state: dbflux_core::RowState,
}

fn resolve_record_row(edit_buffer: &EditBuffer, visual_ix: usize) -> RecordRow {
    let source = edit_buffer.compute_visual_order().get(visual_ix).copied();
    let state = match source {
        Some(VisualRowSource::Base(base_idx)) => edit_buffer.row_state(base_idx).clone(),
        Some(VisualRowSource::Insert(_)) => dbflux_core::RowState::PendingInsert,
        None => dbflux_core::RowState::Clean,
    };

    RecordRow {
        visual_ix,
        source,
        state,
    }
}

/// Render the record-mode body: a header strip naming the current row followed
/// by the scrollable field list.
pub(super) fn render_record(
    state_entity: &Entity<DataTableState>,
    state: &DataTableState,
    cx: &App,
) -> AnyElement {
    let row_count = state.row_count();
    let col_count = state.col_count();

    if row_count == 0 || col_count == 0 {
        return div()
            .flex_1()
            .flex()
            .items_center()
            .justify_center()
            .child(Text::muted(dbflux_i18n::t!(
                "components.data_table.record.empty"
            )))
            .into_any_element();
    }

    // Selection drives which row the record shows. Clamped because a refresh
    // can shrink the result under a stale selection.
    let visual_ix = state
        .selection()
        .active
        .map(|coord| coord.row)
        .unwrap_or(0)
        .min(row_count - 1);

    let record_scroll_handle = state.record_scroll_handle().clone();
    let model = std::sync::Arc::clone(state.model_arc());
    let list_entity = state_entity.clone();
    let state_for_empty = state_entity.clone();

    let mut list = uniform_list(
        "record-fields",
        col_count,
        move |visible_range: Range<usize>, _window: &mut Window, cx: &mut App| {
            let state = list_entity.read(cx);
            render_fields(&list_entity, state, &model, visual_ix, visible_range, cx)
        },
    )
    .size_full()
    .with_sizing_behavior(ListSizingBehavior::Auto)
    .track_scroll(record_scroll_handle.clone());

    list.style().restrict_scroll_to_axis = Some(true);

    div()
        .flex_1()
        .flex()
        .flex_col()
        .min_h_0()
        .relative()
        .child(render_record_header(visual_ix, row_count, cx))
        .child(
            div()
                .id("record-body")
                .flex_1()
                .min_h_0()
                .overflow_hidden()
                // A click below the last field lands here, not on a row.
                // Treat it like clicking another cell: commit the open edit
                // and give the table back the keyboard. Rows stop propagation
                // so a click inside the editor never reaches this handler.
                .on_mouse_down(MouseButton::Left, move |_, window, cx| {
                    state_for_empty.update(cx, |state, cx| {
                        if state.is_editing() {
                            state.stop_editing(true, cx);
                        }
                        state.focus(window, cx);
                    });
                })
                .child(list),
        )
        .child(
            div()
                .absolute()
                .top(HEADER_HEIGHT)
                .right_0()
                .bottom_0()
                .w(SCROLLBAR_WIDTH)
                .child(Scrollbar::vertical(&record_scroll_handle)),
        )
        .into_any_element()
}

/// Column captions plus the "Row 22 of 165" position readout.
fn render_record_header(visual_ix: usize, row_count: usize, cx: &App) -> AnyElement {
    let theme = cx.theme();

    div()
        .flex()
        .flex_row()
        .items_center()
        .h(HEADER_HEIGHT)
        .w_full()
        .flex_shrink_0()
        .bg(theme.table_head)
        .border_b_1()
        .border_color(theme.border)
        .child(
            div()
                .flex()
                .items_center()
                .w(RECORD_NAME_WIDTH)
                .flex_shrink_0()
                .h_full()
                .px(CELL_PADDING_X)
                .border_r_1()
                .border_color(theme.border)
                .child(Text::label_sm(dbflux_i18n::t!(
                    "components.data_table.record.name_header"
                ))),
        )
        .child(
            div()
                .flex()
                .flex_1()
                .min_w_0()
                .items_center()
                .justify_between()
                .gap(Spacing::SM)
                .h_full()
                .px(CELL_PADDING_X)
                // The scrollbar floats over the right edge; keep the position
                // readout clear of it.
                .pr(SCROLLBAR_WIDTH)
                .child(Text::label_sm(dbflux_i18n::t!(
                    "components.data_table.record.value_header"
                )))
                .child(
                    Text::body(dbflux_i18n::t!(
                        "components.data_table.record.position",
                        row = visual_ix + 1,
                        total = row_count
                    ))
                    .font_size(FontSizes::XS)
                    .color(theme.muted_foreground),
                ),
        )
        .into_any_element()
}

/// Render the visible slice of fields for the record's uniform list.
fn render_fields(
    state_entity: &Entity<DataTableState>,
    state: &DataTableState,
    model: &TableModel,
    visual_ix: usize,
    visible_range: Range<usize>,
    cx: &App,
) -> Vec<AnyElement> {
    let theme = cx.theme();
    let edit_buffer = state.edit_buffer();
    let record = resolve_record_row(edit_buffer, visual_ix);
    let selection = state.selection();
    let editing_cell = state.editing_cell();
    let pk_columns = state.pk_columns();
    let fk_columns = state.fk_columns();

    let row_bg = match record.state {
        dbflux_core::RowState::Saving => Some(RowColors::saving(theme)),
        dbflux_core::RowState::Error(_) => Some(RowColors::error(theme)),
        dbflux_core::RowState::PendingInsert => Some(RowColors::insert(theme)),
        dbflux_core::RowState::PendingDelete => Some(RowColors::delete(theme)),
        dbflux_core::RowState::Dirty | dbflux_core::RowState::Clean => None,
    };
    let is_pending_delete = record.state.is_pending_delete();
    let null_value = CellValue::null();

    visible_range
        .map(|col_ix| {
            let coord = CellCoord::new(record.visual_ix, col_ix);
            let is_active = selection.active == Some(coord);
            let is_editing = editing_cell == Some(coord);

            let column = model.columns.get(col_ix);
            let name: SharedString = column
                .map(|spec| SharedString::from(spec.title.to_string()))
                .unwrap_or_default();
            let type_label: SharedString = column
                .map(|spec| SharedString::from(spec.type_name.to_string()))
                .unwrap_or_default();

            // Base rows carry cell-level dirty tracking; pending inserts do not
            // (their whole row is new), matching the grid's rules.
            let (display_value, is_cell_dirty) = match record.source {
                Some(VisualRowSource::Base(base_idx)) => {
                    let base = model.cell(base_idx, col_ix).unwrap_or(&null_value);
                    (
                        edit_buffer.get_cell(base_idx, col_ix, base),
                        edit_buffer.is_cell_dirty(base_idx, col_ix),
                    )
                }
                Some(VisualRowSource::Insert(insert_idx)) => (
                    edit_buffer
                        .get_pending_insert_by_idx(insert_idx)
                        .and_then(|data| data.get(col_ix))
                        .unwrap_or(&null_value),
                    false,
                ),
                None => (&null_value, false),
            };

            let is_null = display_value.is_null();
            let is_auto_generated = display_value.is_auto_generated();

            let name_cell = div()
                .flex()
                .flex_row()
                .items_center()
                .gap(Spacing::XS)
                .w(RECORD_NAME_WIDTH)
                .flex_shrink_0()
                .h_full()
                .px(CELL_PADDING_X)
                .overflow_hidden()
                .border_r_1()
                .border_color(theme.border)
                .bg(theme.table_head.opacity(0.4))
                .when(pk_columns.contains(&col_ix), |d| {
                    d.child(
                        Text::body("PK")
                            .font_size(FontSizes::XS)
                            .color(theme.muted_foreground.opacity(0.6)),
                    )
                })
                .when(fk_columns.contains(&col_ix), |d| {
                    d.child(
                        Text::body("FK")
                            .font_size(FontSizes::XS)
                            .color(theme.muted_foreground.opacity(0.6)),
                    )
                })
                .child(
                    div()
                        .flex_shrink_0()
                        .whitespace_nowrap()
                        .child(Text::label_sm(name)),
                )
                .when(!type_label.is_empty(), |d| {
                    d.child(
                        div()
                            .flex()
                            .min_w_0()
                            .flex_1()
                            .justify_end()
                            .overflow_hidden()
                            .text_ellipsis()
                            .whitespace_nowrap()
                            .child(
                                Text::body(type_label)
                                    .font_size(FontSizes::XS)
                                    .color(theme.muted_foreground.opacity(0.6)),
                            ),
                    )
                });

            let value_cell = if is_editing {
                render_editor_cell(state, theme)
            } else {
                let display_text = display_value.display_text().to_string();
                div()
                    .flex()
                    .flex_1()
                    .min_w_0()
                    .items_center()
                    .h_full()
                    .px(CELL_PADDING_X)
                    .overflow_hidden()
                    .when(is_cell_dirty, |d| {
                        d.bg(RowColors::dirty(theme))
                            .border_l_2()
                            .border_color(theme.warning)
                    })
                    .when(is_active, |d| d.border_1().border_color(theme.ring))
                    .when(is_null || is_auto_generated, |d| d.italic())
                    .when(is_pending_delete, |d| d.line_through())
                    .child(Text::body(display_text).font_size(FontSizes::SM).color(
                        if is_pending_delete || is_null || is_auto_generated {
                            theme.muted_foreground
                        } else {
                            theme.foreground
                        },
                    ))
                    .into_any_element()
            };

            let state_for_click = state_entity.clone();
            let state_for_context = state_entity.clone();

            div()
                .id(("record-field", col_ix))
                .flex()
                .flex_row()
                .w_full()
                .h(RECORD_ROW_HEIGHT)
                .border_b_1()
                .border_color(theme.table_row_border)
                .cursor_pointer()
                .when_some(row_bg, |d, bg| d.bg(bg))
                .when(row_bg.is_none() && col_ix % 2 == 1, |d| {
                    d.bg(theme.table_even)
                })
                .when(is_active, |d| d.bg(theme.table_active.opacity(0.45)))
                // Keep row clicks off the record body: its handler commits the
                // open edit, which must only happen for clicks beside the rows.
                .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
                // The editor is a child of this row, so a click inside it
                // bubbles here. While editing, the row must carry no handler
                // that moves focus back to the table — doing so blurs the
                // input, and the blur subscription cancels the edit on the
                // user's first click into the field they just opened.
                .when(!is_editing, |d| {
                    d.cursor_pointer()
                        .on_click(move |event: &ClickEvent, window, cx| {
                            state_for_click.update(cx, |state, cx| {
                                state.focus(window, cx);
                                state.select_cell(coord, cx);
                            });

                            // Whether the field can actually be edited is
                            // `start_editing`'s call — read-only columns,
                            // non-editable results, and unsupported cell kinds
                            // all refuse there.
                            if event.click_count() == 2 {
                                state_for_click.update(cx, |state, cx| {
                                    state.start_editing(coord, window, cx);
                                });
                            }
                        })
                        .on_mouse_down(
                            MouseButton::Right,
                            move |event: &MouseDownEvent, window, cx| {
                                cx.stop_propagation();
                                state_for_context.update(cx, |state, cx| {
                                    state.focus(window, cx);
                                    state.select_cell(coord, cx);
                                    cx.emit(DataTableEvent::ContextMenuRequested {
                                        row: coord.row,
                                        col: coord.col,
                                        position: event.position,
                                    });
                                });
                            },
                        )
                })
                .child(name_cell)
                .child(value_cell)
                .into_any_element()
        })
        .collect()
}

/// The inline editor for the field being edited — the same enum dropdown or
/// text input the grid mounts, so commit/cancel keep going through
/// `DataTableState::stop_editing`.
fn render_editor_cell(state: &DataTableState, theme: &gpui_component::theme::Theme) -> AnyElement {
    let shell = div()
        .flex()
        .flex_1()
        .min_w_0()
        .items_center()
        .h_full()
        .overflow_hidden()
        .border_1()
        .border_color(theme.ring)
        .bg(theme.background);

    if let Some(dropdown) = state.enum_dropdown() {
        return shell.child(dropdown.clone()).into_any_element();
    }

    if let Some(input_state) = state.cell_input() {
        return shell.child(render_input(input_state)).into_any_element();
    }

    shell.into_any_element()
}

fn render_input(input_state: &Entity<InputState>) -> impl IntoElement {
    div()
        .flex_1()
        .min_w_0()
        .child(Input::new(input_state).small())
}
