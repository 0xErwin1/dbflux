use std::ops::Range;
use std::sync::{Arc, Mutex};

use gpui::prelude::FluentBuilder;
use gpui::ElementId;
use gpui::{
    actions, canvas, div, px, uniform_list, AnyElement, App, ClickEvent, Context, Entity,
    InteractiveElement, IntoElement, KeyBinding, ListSizingBehavior, ParentElement,
    StatefulInteractiveElement, Styled, Window,
};
use gpui_component::scroll::Scrollbar;
use gpui_component::ActiveTheme;

use super::events::{Direction, Edge};
use super::model::TableModel;
use super::selection::{CellCoord, SelectionState};
use super::state::DataTableState;
use super::theme::{
    CELL_PADDING_X, CELL_PADDING_Y, HEADER_HEIGHT, ROW_HEIGHT, SCROLLBAR_WIDTH, SORT_INDICATOR_ASC,
    SORT_INDICATOR_DESC,
};
use dbflux_core::SortDirection;

/// Cached scroll state to prevent unnecessary syncs
#[derive(Clone)]
struct ScrollSyncState {
    last_viewport_size: gpui::Size<gpui::Pixels>,
    last_h_offset: gpui::Pixels,
}

impl Default for ScrollSyncState {
    fn default() -> Self {
        Self {
            last_viewport_size: gpui::Size::default(),
            last_h_offset: gpui::px(0.0),
        }
    }
}

actions!(
    data_table,
    [
        MoveUp,
        MoveDown,
        MoveLeft,
        MoveRight,
        SelectUp,
        SelectDown,
        SelectLeft,
        SelectRight,
        MoveToLineStart,
        MoveToLineEnd,
        MoveToTop,
        MoveToBottom,
        SelectToLineStart,
        SelectToLineEnd,
        SelectToTop,
        SelectToBottom,
        SelectAll,
        ClearSelection,
        Copy,
    ]
);

const CONTEXT: &str = "DataTable";

pub fn init(cx: &mut App) {
    cx.bind_keys([
        KeyBinding::new("up", MoveUp, Some(CONTEXT)),
        KeyBinding::new("down", MoveDown, Some(CONTEXT)),
        KeyBinding::new("left", MoveLeft, Some(CONTEXT)),
        KeyBinding::new("right", MoveRight, Some(CONTEXT)),
        KeyBinding::new("shift-up", SelectUp, Some(CONTEXT)),
        KeyBinding::new("shift-down", SelectDown, Some(CONTEXT)),
        KeyBinding::new("shift-left", SelectLeft, Some(CONTEXT)),
        KeyBinding::new("shift-right", SelectRight, Some(CONTEXT)),
        KeyBinding::new("home", MoveToLineStart, Some(CONTEXT)),
        KeyBinding::new("end", MoveToLineEnd, Some(CONTEXT)),
        KeyBinding::new("ctrl-home", MoveToTop, Some(CONTEXT)),
        KeyBinding::new("ctrl-end", MoveToBottom, Some(CONTEXT)),
        KeyBinding::new("shift-home", SelectToLineStart, Some(CONTEXT)),
        KeyBinding::new("shift-end", SelectToLineEnd, Some(CONTEXT)),
        KeyBinding::new("ctrl-shift-home", SelectToTop, Some(CONTEXT)),
        KeyBinding::new("ctrl-shift-end", SelectToBottom, Some(CONTEXT)),
        KeyBinding::new("ctrl-a", SelectAll, Some(CONTEXT)),
        KeyBinding::new("escape", ClearSelection, Some(CONTEXT)),
        KeyBinding::new("ctrl-c", Copy, Some(CONTEXT)),
    ]);
}

/// DataTable component for rendering tabular data with virtualization.
pub struct DataTable {
    id: ElementId,
    state: Entity<DataTableState>,
    scroll_sync: Arc<Mutex<ScrollSyncState>>,
}

impl DataTable {
    pub fn new(
        id: impl Into<ElementId>,
        state: Entity<DataTableState>,
        cx: &mut Context<Self>,
    ) -> Self {
        cx.observe(&state, |_this, _state, cx| cx.notify()).detach();

        Self {
            id: id.into(),
            state,
            scroll_sync: Arc::new(Mutex::new(ScrollSyncState::default())),
        }
    }
}

impl gpui::Render for DataTable {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let state = self.state.read(cx);
        let theme = cx.theme();

        let row_count = state.row_count();

        let vertical_scroll_handle = state.vertical_scroll_handle().clone();
        let horizontal_scroll_handle = state.horizontal_scroll_handle().clone();
        let focus_handle = state.focus_handle().clone();

        let total_width = state.total_content_width();

        // Build header
        let header = self.render_header(state, total_width, theme, cx);

        // Build body using uniform_list for virtualization
        let body = self.render_body(row_count, total_width, cx);

        // Clone state entity for callbacks
        let state_entity = self.state.clone();

        // Create action closures
        let s = self.state.clone();
        let on_move_up = move |_: &MoveUp, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| state.move_active(Direction::Up, false, cx));
        };
        let s = self.state.clone();
        let on_move_down = move |_: &MoveDown, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| {
                state.move_active(Direction::Down, false, cx)
            });
        };
        let s = self.state.clone();
        let on_move_left = move |_: &MoveLeft, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| {
                state.move_active(Direction::Left, false, cx)
            });
        };
        let s = self.state.clone();
        let on_move_right = move |_: &MoveRight, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| {
                state.move_active(Direction::Right, false, cx)
            });
        };
        let s = self.state.clone();
        let on_select_up = move |_: &SelectUp, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| state.move_active(Direction::Up, true, cx));
        };
        let s = self.state.clone();
        let on_select_down = move |_: &SelectDown, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| state.move_active(Direction::Down, true, cx));
        };
        let s = self.state.clone();
        let on_select_left = move |_: &SelectLeft, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| state.move_active(Direction::Left, true, cx));
        };
        let s = self.state.clone();
        let on_select_right = move |_: &SelectRight, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| {
                state.move_active(Direction::Right, true, cx)
            });
        };
        let s = self.state.clone();
        let on_line_start = move |_: &MoveToLineStart, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| state.move_to_edge(Edge::Left, false, cx));
        };
        let s = self.state.clone();
        let on_line_end = move |_: &MoveToLineEnd, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| state.move_to_edge(Edge::Right, false, cx));
        };
        let s = self.state.clone();
        let on_top = move |_: &MoveToTop, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| state.move_to_edge(Edge::Home, false, cx));
        };
        let s = self.state.clone();
        let on_bottom = move |_: &MoveToBottom, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| state.move_to_edge(Edge::End, false, cx));
        };
        let s = self.state.clone();
        let on_select_line_start = move |_: &SelectToLineStart, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| state.move_to_edge(Edge::Left, true, cx));
        };
        let s = self.state.clone();
        let on_select_line_end = move |_: &SelectToLineEnd, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| state.move_to_edge(Edge::Right, true, cx));
        };
        let s = self.state.clone();
        let on_select_top = move |_: &SelectToTop, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| state.move_to_edge(Edge::Home, true, cx));
        };
        let s = self.state.clone();
        let on_select_bottom = move |_: &SelectToBottom, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| state.move_to_edge(Edge::End, true, cx));
        };
        let s = self.state.clone();
        let on_select_all = move |_: &SelectAll, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| state.select_all(cx));
        };
        let s = self.state.clone();
        let on_clear_selection = move |_: &ClearSelection, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| state.clear_selection(cx));
        };
        let s = self.state.clone();
        let on_copy = move |_: &Copy, _: &mut Window, cx: &mut App| {
            let text = s.read(cx).copy_selection();
            if let Some(text) = text {
                cx.write_to_clipboard(gpui::ClipboardItem::new_string(text));
            }
        };

        // Main layout: vertical flex with header and scrollable body.
        // Both header and body share the same horizontal scroll handle.
        let inner_table = div()
            .id("table-inner")
            .flex()
            .flex_col()
            .size_full()
            .child(header)
            .when(row_count > 0, |this| this.child(body));

        div()
            .id(self.id.clone())
            .key_context(CONTEXT)
            .track_focus(&focus_handle)
            .relative()
            .size_full()
            .overflow_hidden()
            .bg(theme.table)
            .border_1()
            .border_color(theme.border)
            // Navigation actions
            .on_action(on_move_up)
            .on_action(on_move_down)
            .on_action(on_move_left)
            .on_action(on_move_right)
            .on_action(on_select_up)
            .on_action(on_select_down)
            .on_action(on_select_left)
            .on_action(on_select_right)
            .on_action(on_line_start)
            .on_action(on_line_end)
            .on_action(on_top)
            .on_action(on_bottom)
            .on_action(on_select_line_start)
            .on_action(on_select_line_end)
            .on_action(on_select_top)
            .on_action(on_select_bottom)
            .on_action(on_select_all)
            .on_action(on_clear_selection)
            .on_action(on_copy)
            .child(inner_table)
            // Measure viewport size and sync horizontal scroll offset using canvas
            .child({
                let scroll_sync = self.scroll_sync.clone();
                canvas(
                    move |bounds, _, cx| {
                        let mut sync = scroll_sync.lock().unwrap();
                        state_entity.update(cx, |state, cx| {
                            let new_size = bounds.size;
                            let viewport_changed = new_size != sync.last_viewport_size;

                            if viewport_changed {
                                sync.last_viewport_size = new_size;
                                if state.viewport_size() != new_size {
                                    state.set_viewport_size(new_size, cx);
                                }
                            }

                            // Only sync horizontal offset if viewport changed or offset actually changed
                            let current_h_offset = state.horizontal_scroll_handle().offset().x;
                            let h_offset_changed =
                                (current_h_offset - sync.last_h_offset).abs() > gpui::px(0.5);

                            if viewport_changed || h_offset_changed {
                                sync.last_h_offset = current_h_offset;
                                // Sync horizontal offset from scroll handle to trigger body re-render
                                state.sync_horizontal_offset(cx);
                            }
                        });
                    },
                    |_, _, _, _| {},
                )
                .absolute()
                .size_full()
            })
            // Phantom scroller: owns the horizontal scroll handle for the scrollbar.
            // It's 1px tall and positioned at the bottom, so it never receives wheel events.
            // The mouse is always over the header or body, which don't capture horizontal wheel.
            .child(
                div()
                    .id("table-hscroll-owner")
                    .absolute()
                    .left_0()
                    .right(SCROLLBAR_WIDTH)
                    .bottom_0()
                    .h(px(1.0))
                    .overflow_x_scroll()
                    .track_scroll(&horizontal_scroll_handle)
                    .child(div().min_w(px(total_width)).h(px(1.0))),
            )
            // Scrollbars as absolute overlays
            .child(
                div()
                    .absolute()
                    .top(HEADER_HEIGHT)
                    .right_0()
                    .bottom_0()
                    .w(SCROLLBAR_WIDTH)
                    .when(row_count > 0, |this| {
                        this.child(Scrollbar::vertical(&vertical_scroll_handle))
                    }),
            )
            .child(
                div()
                    .absolute()
                    .left_0()
                    .right_0()
                    .bottom_0()
                    .h(SCROLLBAR_WIDTH)
                    .child(Scrollbar::horizontal(&horizontal_scroll_handle)),
            )
    }
}

impl DataTable {
    fn render_header(
        &self,
        state: &DataTableState,
        total_width: f32,
        theme: &gpui_component::theme::Theme,
        _cx: &gpui::App,
    ) -> impl IntoElement {
        let model = state.model();
        let sort = state.sort();
        let column_widths = state.column_widths();
        let h_offset = state.horizontal_offset();
        let state_entity = self.state.clone();

        let header_cells: Vec<_> = model
            .columns
            .iter()
            .enumerate()
            .map(|(col_ix, col_spec)| {
                let width = column_widths.get(col_ix).copied().unwrap_or(120.0);
                let is_sorted = sort.map(|s| s.column_ix == col_ix).unwrap_or(false);
                let sort_indicator = if is_sorted {
                    match sort.unwrap().direction {
                        SortDirection::Ascending => SORT_INDICATOR_ASC,
                        SortDirection::Descending => SORT_INDICATOR_DESC,
                    }
                } else {
                    ""
                };

                let state_for_click = state_entity.clone();

                div()
                    .id(("header-col", col_ix))
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .justify_between()
                    .h(HEADER_HEIGHT)
                    .w(px(width))
                    .px(CELL_PADDING_X)
                    .overflow_hidden()
                    .border_r_1()
                    .border_color(theme.border)
                    .bg(theme.table_head)
                    .hover(|s| s.bg(theme.table_hover))
                    .cursor_pointer()
                    .on_click(move |_event: &ClickEvent, _window, cx| {
                        state_for_click.update(cx, |state, cx| {
                            state.cycle_sort(col_ix, cx);
                        });
                    })
                    .child(
                        div()
                            .text_sm()
                            .font_weight(gpui::FontWeight::MEDIUM)
                            .text_color(if is_sorted {
                                theme.primary
                            } else {
                                theme.table_head_foreground
                            })
                            .overflow_hidden()
                            .text_ellipsis()
                            .whitespace_nowrap()
                            .child(col_spec.title.to_string()),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(if is_sorted {
                                theme.primary
                            } else {
                                theme.muted_foreground
                            })
                            .child(sort_indicator),
                    )
            })
            .collect();

        // Header uses overflow_hidden and applies horizontal offset via margin.
        // The phantom scroller owns the scroll handle; header just follows the offset.
        div()
            .id("table-header")
            .flex_shrink_0()
            .h(HEADER_HEIGHT)
            .overflow_hidden()
            .border_b_1()
            .border_color(theme.border)
            .child(
                div()
                    .flex()
                    .min_w(px(total_width))
                    .ml(-h_offset)
                    .bg(theme.table_head)
                    .children(header_cells),
            )
    }

    fn render_body(&self, row_count: usize, total_width: f32, cx: &gpui::App) -> impl IntoElement {
        let state = self.state.read(cx);
        let vertical_scroll_handle = state.vertical_scroll_handle().clone();
        let h_offset = state.horizontal_offset();
        let model = Arc::clone(state.model_arc());

        let state_entity = self.state.clone();

        // Body uses overflow_hidden to prevent wheel capture.
        // Horizontal position is set via margin based on state.horizontal_offset().
        // uniform_list handles vertical scrolling.
        div()
            .id("table-body")
            .flex_1()
            .min_h_0()
            .overflow_hidden()
            .child(
                uniform_list(
                    "table-rows",
                    row_count,
                    move |visible_range: Range<usize>, _window: &mut Window, cx: &mut App| {
                        let theme = cx.theme();
                        // Read state INSIDE closure - only when actually rendering
                        let state = state_entity.read(cx);

                        render_rows(
                            &state_entity,
                            visible_range,
                            &model,
                            state.column_widths(),
                            state.selection(),
                            total_width,
                            theme,
                        )
                    },
                )
                .size_full()
                .min_w(px(total_width))
                .ml(-h_offset)
                .with_sizing_behavior(ListSizingBehavior::Auto)
                .track_scroll(vertical_scroll_handle),
            )
    }
}

/// Renders the visible rows for the uniform_list.
fn render_rows(
    state_entity: &Entity<DataTableState>,
    visible_range: Range<usize>,
    model: &TableModel,
    column_widths: &[f32],
    selection: &SelectionState,
    total_width: f32,
    theme: &gpui_component::theme::Theme,
) -> Vec<AnyElement> {
    // Pre-calculate cumulative column offsets for hit-testing
    let mut col_offsets = vec![0.0f32];
    for width in column_widths {
        col_offsets.push(col_offsets.last().unwrap_or(&0.0) + width);
    }

    visible_range
        .map(|row_ix| {
            let row_data = model.rows.get(row_ix);
            let state_for_click = state_entity.clone();

            // Build cells without individual click handlers
            let cells: Vec<_> = (0..model.col_count())
                .map(|col_ix| {
                    let cell = row_data.and_then(|r| r.cells.get(col_ix));
                    let width = column_widths.get(col_ix).copied().unwrap_or(120.0);
                    let coord = CellCoord::new(row_ix, col_ix);
                    let is_selected = selection.is_selected(coord);
                    let is_active = selection.active == Some(coord);
                    let display_text = cell
                        .map(|c| c.display_text().to_string())
                        .unwrap_or_default();
                    let is_null = cell.map(|c| c.is_null()).unwrap_or(false);

                    // Simplified cell structure - single div instead of nested
                    div()
                        .id(("cell", row_ix * 10000 + col_ix))
                        .flex()
                        .flex_shrink_0()
                        .items_center()
                        .h(ROW_HEIGHT)
                        .w(px(width))
                        .px(CELL_PADDING_X)
                        .overflow_hidden()
                        .border_r_1()
                        .border_color(theme.border)
                        .cursor_pointer()
                        .when(is_selected, |d| {
                            d.bg(theme.table_active)
                                .border_color(theme.table_active_border)
                        })
                        .when(is_active, |d| d.border_1().border_color(theme.ring))
                        .text_sm()
                        .text_color(if is_null {
                            theme.muted_foreground
                        } else {
                            theme.foreground
                        })
                        .when(is_null, |d| d.italic())
                        .child(display_text.to_string())
                })
                .collect();

            // Row-level click handler with hit-testing
            let col_offsets_for_click = col_offsets.clone();
            let col_count_for_click = column_widths.len();
            div()
                .id(("row", row_ix))
                .flex()
                .flex_shrink_0()
                .w(px(total_width))
                .h(ROW_HEIGHT)
                .overflow_hidden()
                .border_b_1()
                .border_color(theme.table_row_border)
                .when(row_ix % 2 == 1, |d| d.bg(theme.table_even))
                .cursor_pointer()
                .on_click(move |event: &ClickEvent, _window, cx| {
                    // Hit-test: find which column was clicked based on X position
                    let click_x: f32 = event.position().x.into();
                    let col_ix = col_offsets_for_click
                        .windows(2)
                        .enumerate()
                        .find(|(_, pair)| click_x >= pair[0] && click_x < pair[1])
                        .map(|(i, _)| i)
                        .unwrap_or(col_count_for_click.saturating_sub(1));

                    let coord = CellCoord::new(row_ix, col_ix);

                    if event.modifiers().shift {
                        state_for_click.update(cx, |state, cx| {
                            state.extend_selection(coord, cx);
                        });
                    } else {
                        state_for_click.update(cx, |state, cx| {
                            state.select_cell(coord, cx);
                        });
                    }
                })
                .children(cells)
                .into_any_element()
        })
        .collect()
}
