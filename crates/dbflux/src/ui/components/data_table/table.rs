use std::ops::Range;
use std::sync::{Arc, Mutex};

use gpui::ElementId;
use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, App, ClickEvent, Context, Entity, InteractiveElement, IntoElement, KeyBinding,
    ListSizingBehavior, MouseButton, MouseDownEvent, ParentElement, StatefulInteractiveElement,
    Styled, Window, actions, canvas, div, px, uniform_list,
};
use gpui_component::input::{Input, InputState};
use gpui_component::scroll::Scrollbar;
use gpui_component::{ActiveTheme, Sizable};

use super::events::{DataTableEvent, Direction, Edge};
use super::model::TableModel;
use super::selection::{CellCoord, SelectionState};
use super::state::DataTableState;
use super::theme::{
    CELL_PADDING_X, HEADER_HEIGHT, ROW_HEIGHT, SCROLLBAR_WIDTH, SORT_INDICATOR_ASC,
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
        CopyRow,
        StartEdit,
        ConfirmEdit,
        CancelEdit,
        SaveRow,
        // Row operations (vim-style)
        DeleteRow,
        AddRow,
        DuplicateRow,
        SetNull,
        // Undo/Redo
        Undo,
        Redo,
    ]
);

/// Key context for DataTable - matches ContextId::Results.as_gpui_context()
const CONTEXT: &str = "Results";

pub fn init(cx: &mut App) {
    cx.bind_keys([
        // Navigation
        KeyBinding::new("up", MoveUp, Some(CONTEXT)),
        KeyBinding::new("down", MoveDown, Some(CONTEXT)),
        KeyBinding::new("left", MoveLeft, Some(CONTEXT)),
        KeyBinding::new("right", MoveRight, Some(CONTEXT)),
        KeyBinding::new("k", MoveUp, Some(CONTEXT)),
        KeyBinding::new("j", MoveDown, Some(CONTEXT)),
        KeyBinding::new("h", MoveLeft, Some(CONTEXT)),
        KeyBinding::new("l", MoveRight, Some(CONTEXT)),
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
        // Copy
        KeyBinding::new("ctrl-c", Copy, Some(CONTEXT)),
        KeyBinding::new("y y", Copy, Some(CONTEXT)),
        KeyBinding::new("shift-y shift-y", CopyRow, Some(CONTEXT)),
        // Edit mode
        KeyBinding::new("enter", StartEdit, Some(CONTEXT)),
        KeyBinding::new("f2", StartEdit, Some(CONTEXT)),
        KeyBinding::new("ctrl-enter", SaveRow, Some(CONTEXT)),
        // Row operations (vim-style)
        KeyBinding::new("d d", DeleteRow, Some(CONTEXT)),
        KeyBinding::new("delete", DeleteRow, Some(CONTEXT)),
        KeyBinding::new("a a", AddRow, Some(CONTEXT)),
        KeyBinding::new("shift-a shift-a", DuplicateRow, Some(CONTEXT)),
        KeyBinding::new("ctrl-n", SetNull, Some(CONTEXT)),
        // Undo/Redo (vim-style + standard)
        KeyBinding::new("u", Undo, Some(CONTEXT)),
        KeyBinding::new("ctrl-z", Undo, Some(CONTEXT)),
        KeyBinding::new("ctrl-r", Redo, Some(CONTEXT)),
        KeyBinding::new("ctrl-shift-z", Redo, Some(CONTEXT)),
    ]);
}

#[derive(Clone)]
struct ResizeDragState {
    col: Option<usize>,
    start_x: gpui::Pixels,
    original_width: f32,
}

impl Default for ResizeDragState {
    fn default() -> Self {
        Self {
            col: None,
            start_x: gpui::px(0.0),
            original_width: 0.0,
        }
    }
}

pub struct DataTable {
    id: ElementId,
    state: Entity<DataTableState>,
    scroll_sync: Arc<Mutex<ScrollSyncState>>,
    resize_drag: Arc<Mutex<ResizeDragState>>,
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
            resize_drag: Arc::new(Mutex::new(ResizeDragState::default())),
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
        let col_count = state.col_count();

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
            s.update(cx, |state, cx| {
                if state.is_editing() {
                    state.stop_editing(false, cx);
                } else {
                    state.clear_selection(cx);
                }
            });
        };
        let s = self.state.clone();
        let on_copy = move |_: &Copy, _: &mut Window, cx: &mut App| {
            let text = s.read(cx).copy_selection();
            if let Some(text) = text {
                cx.write_to_clipboard(gpui::ClipboardItem::new_string(text));
            }
        };

        let s = self.state.clone();
        let on_start_edit = move |_: &StartEdit, window: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| {
                if state.is_editing() {
                    return;
                }
                if let Some(coord) = state.selection().active {
                    state.start_editing(coord, window, cx);
                }
            });
        };

        let s = self.state.clone();
        let on_confirm_edit = move |_: &ConfirmEdit, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| {
                if state.is_editing() {
                    state.stop_editing(true, cx);
                }
            });
        };

        let s = self.state.clone();
        let on_cancel_edit = move |_: &CancelEdit, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| {
                if state.is_editing() {
                    state.stop_editing(false, cx);
                }
            });
        };

        let s = self.state.clone();
        let on_save_row = move |_: &SaveRow, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| {
                state.request_save_row(cx);
            });
        };

        // Row operations (vim-style)
        let s = self.state.clone();
        let on_delete_row = move |_: &DeleteRow, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| {
                if !state.is_editable() {
                    return;
                }
                if let Some(coord) = state.selection().active {
                    cx.emit(DataTableEvent::DeleteRowRequested(coord.row));
                }
            });
        };

        let s = self.state.clone();
        let on_add_row = move |_: &AddRow, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| {
                if !state.is_insertable() {
                    return;
                }
                let row = state.selection().active.map(|c| c.row).unwrap_or(0);
                cx.emit(DataTableEvent::AddRowRequested(row));
            });
        };

        let s = self.state.clone();
        let on_duplicate_row = move |_: &DuplicateRow, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| {
                if !state.is_insertable() {
                    return;
                }
                if let Some(coord) = state.selection().active {
                    cx.emit(DataTableEvent::DuplicateRowRequested(coord.row));
                }
            });
        };

        let s = self.state.clone();
        let on_set_null = move |_: &SetNull, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| {
                if !state.is_editable() {
                    return;
                }
                if let Some(coord) = state.selection().active {
                    cx.emit(DataTableEvent::SetNullRequested {
                        row: coord.row,
                        col: coord.col,
                    });
                }
            });
        };

        let s = self.state.clone();
        let on_copy_row = move |_: &CopyRow, _: &mut Window, cx: &mut App| {
            let row = s.read(cx).selection().active.map(|c| c.row);
            if let Some(row) = row {
                s.update(cx, |_state, cx| {
                    cx.emit(DataTableEvent::CopyRowRequested(row));
                });
            }
        };

        let s = self.state.clone();
        let on_undo = move |_: &Undo, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| {
                // Stop editing before undo to avoid stale visual index references
                if state.is_editing() {
                    state.stop_editing(false, cx);
                }

                if state.edit_buffer_mut().undo() {
                    // Validate selection after undo - indices may have shifted
                    let visual_count = state.edit_buffer().compute_visual_order().len();
                    if let Some(active) = state.selection().active
                        && active.row >= visual_count
                    {
                        state.clear_selection(cx);
                    }
                    cx.notify();
                }
            });
        };

        let s = self.state.clone();
        let on_redo = move |_: &Redo, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| {
                // Stop editing before redo to avoid stale visual index references
                if state.is_editing() {
                    state.stop_editing(false, cx);
                }

                if state.edit_buffer_mut().redo() {
                    // Validate selection after redo - indices may have shifted
                    let visual_count = state.edit_buffer().compute_visual_order().len();
                    if let Some(active) = state.selection().active
                        && active.row >= visual_count
                    {
                        state.clear_selection(cx);
                    }
                    cx.notify();
                }
            });
        };

        // Main layout: vertical flex with header and scrollable body.
        // Both header and body share the same horizontal scroll handle.
        let state_for_empty_context = self.state.clone();
        let focus_for_empty = focus_handle.clone();

        let inner_table = div()
            .id("table-inner")
            .flex()
            .flex_col()
            .size_full()
            .child(header)
            .when(row_count > 0, |this| this.child(body))
            .when(row_count == 0 && col_count > 0, |this| {
                this.child(
                    div()
                        .id("table-empty-body")
                        .flex_1()
                        .size_full()
                        .on_mouse_down(MouseButton::Left, move |_, window, cx| {
                            cx.stop_propagation();
                            window.focus(&focus_for_empty);
                        })
                        .on_mouse_down(MouseButton::Right, move |event, window, cx| {
                            cx.stop_propagation();
                            state_for_empty_context.update(cx, |state, cx| {
                                state.focus(window, cx);
                                cx.emit(DataTableEvent::ContextMenuRequested {
                                    row: 0,
                                    col: 0,
                                    position: event.position,
                                });
                            });
                        }),
                )
            });

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
            .on_action(on_start_edit)
            .on_action(on_confirm_edit)
            .on_action(on_cancel_edit)
            .on_action(on_save_row)
            .on_action(on_copy)
            .on_action(on_copy_row)
            // Row operations (vim-style)
            .on_action(on_delete_row)
            .on_action(on_add_row)
            .on_action(on_duplicate_row)
            .on_action(on_set_null)
            // Undo/Redo
            .on_action(on_undo)
            .on_action(on_redo)
            .child(inner_table)
            // Measure viewport size and sync horizontal scroll offset using canvas
            .child({
                let scroll_sync = self.scroll_sync.clone();
                canvas(
                    move |bounds, _, cx| {
                        let mut sync = match scroll_sync.lock() {
                            Ok(guard) => guard,
                            Err(poison_err) => {
                                log::warn!("Scroll sync mutex poisoned, recovering");
                                poison_err.into_inner()
                            }
                        };
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
        let resize_drag = self.resize_drag.clone();

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
                let state_for_resize = state_entity.clone();
                let resize_drag_for_down = resize_drag.clone();
                let resize_drag_for_move = resize_drag.clone();
                let resize_drag_for_up = resize_drag.clone();

                div()
                    .id(("header-col", col_ix))
                    .relative()
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
                    // Resize handle
                    .child(
                        div()
                            .id(("resize-handle", col_ix))
                            .absolute()
                            .right_0()
                            .top_0()
                            .bottom_0()
                            .w(px(6.0))
                            .cursor_col_resize()
                            .hover(|s| s.bg(theme.primary.opacity(0.3)))
                            .on_mouse_down(
                                MouseButton::Left,
                                move |event: &MouseDownEvent, _window, cx| {
                                    cx.stop_propagation();
                                    if let Ok(mut drag) = resize_drag_for_down.lock() {
                                        drag.col = Some(col_ix);
                                        drag.start_x = event.position.x;
                                        drag.original_width = width;
                                    }
                                },
                            )
                            .on_mouse_move(move |event, _window, cx| {
                                let (col, new_width) = {
                                    let drag = resize_drag_for_move.lock().ok();
                                    if let Some(drag) = drag {
                                        if let Some(col) = drag.col {
                                            let delta = event.position.x - drag.start_x;
                                            let delta_f32: f32 = delta.into();
                                            let new_width = drag.original_width + delta_f32;
                                            (Some(col), new_width.max(40.0))
                                        } else {
                                            (None, 0.0)
                                        }
                                    } else {
                                        (None, 0.0)
                                    }
                                };
                                if let Some(col) = col {
                                    state_for_resize.update(cx, |state, cx| {
                                        state.set_column_width(col, new_width, cx);
                                    });
                                }
                            })
                            .on_mouse_up(MouseButton::Left, move |_event, _window, _cx| {
                                if let Ok(mut drag) = resize_drag_for_up.lock() {
                                    drag.col = None;
                                }
                            }),
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

                        let editing_cell = state.editing_cell();
                        let cell_input = state.cell_input().cloned();
                        let edit_buffer = state.edit_buffer();

                        render_rows(
                            &state_entity,
                            visible_range,
                            &model,
                            state.column_widths(),
                            state.selection(),
                            editing_cell,
                            cell_input.as_ref(),
                            edit_buffer,
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
#[allow(clippy::too_many_arguments)]
fn render_rows(
    state_entity: &Entity<DataTableState>,
    visible_range: Range<usize>,
    model: &TableModel,
    column_widths: &[f32],
    selection: &SelectionState,
    editing_cell: Option<CellCoord>,
    cell_input: Option<&Entity<InputState>>,
    edit_buffer: &super::model::EditBuffer,
    total_width: f32,
    theme: &gpui_component::theme::Theme,
) -> Vec<AnyElement> {
    use super::model::VisualRowSource;

    // Compute visual ordering once for this render pass
    let visual_order = edit_buffer.compute_visual_order();

    visible_range
        .map(|visual_ix| {
            // Map visual index to actual data source
            let source = visual_order.get(visual_ix).copied();

            // Get row data and state based on source type
            let (row_data, pending_insert_data, row_state, data_row_ix) = match source {
                Some(VisualRowSource::Base(base_idx)) => {
                    let row = model.rows.get(base_idx);
                    let state = edit_buffer.row_state(base_idx);
                    (row, None, state.clone(), base_idx)
                }
                Some(VisualRowSource::Insert(insert_idx)) => {
                    let data = edit_buffer.get_pending_insert_by_idx(insert_idx);
                    (None, data, dbflux_core::RowState::PendingInsert, visual_ix)
                }
                None => {
                    // Should not happen, but handle gracefully
                    (None, None, dbflux_core::RowState::Clean, visual_ix)
                }
            };

            let is_pending_insert_row = matches!(source, Some(VisualRowSource::Insert(_)));

            // Use visual_ix for selection/display, but data_row_ix for edit buffer access
            let row_ix = visual_ix;

            // Row background based on state
            // - Dirty: cell-level only (no row bg)
            // - Saving: warning background
            // - Error: danger background
            // - PendingInsert: green-ish to indicate new row
            // - PendingDelete: red-ish with visual indication of deletion
            let row_bg = match row_state {
                dbflux_core::RowState::Dirty => None, // Cell-level only
                dbflux_core::RowState::Saving => Some(theme.warning.opacity(0.1)),
                dbflux_core::RowState::Error(_) => Some(theme.danger.opacity(0.15)),
                dbflux_core::RowState::Clean => None,
                dbflux_core::RowState::PendingInsert => Some(theme.success.opacity(0.15)),
                dbflux_core::RowState::PendingDelete => Some(theme.danger.opacity(0.1)),
            };

            let is_pending_delete = row_state.is_pending_delete();

            let cells: Vec<AnyElement> = (0..model.col_count())
                .map(|col_ix| {
                    // Get cell either from model or from pending insert
                    let cell = if let Some(insert_data) = pending_insert_data {
                        insert_data.get(col_ix)
                    } else {
                        row_data.and_then(|r| r.cells.get(col_ix))
                    };
                    let width = column_widths.get(col_ix).copied().unwrap_or(120.0);
                    let coord = CellCoord::new(row_ix, col_ix);
                    let is_selected = selection.is_selected(coord);
                    let is_active = selection.active == Some(coord);
                    let is_editing = editing_cell == Some(coord);

                    if is_editing && let Some(input_state) = cell_input {
                        return div()
                            .id(("cell", row_ix * 10000 + col_ix))
                            .flex()
                            .flex_shrink_0()
                            .items_center()
                            .h(ROW_HEIGHT)
                            .w(px(width))
                            .overflow_hidden()
                            .border_r_1()
                            .border_1()
                            .border_color(theme.ring)
                            .bg(theme.background)
                            .child(Input::new(input_state).small())
                            .into_any_element();
                    }

                    // For edit buffer access, use the data row index (model index for base rows)
                    let is_cell_dirty = if is_pending_insert_row {
                        false // Pending inserts don't have cell-level dirty tracking
                    } else {
                        edit_buffer.is_cell_dirty(data_row_ix, col_ix)
                    };
                    let null_value = super::model::CellValue::null();
                    let base_value = cell.unwrap_or(&null_value);
                    let display_value = if is_pending_insert_row {
                        base_value // For pending inserts, just use the cell value directly
                    } else {
                        edit_buffer.get_cell(data_row_ix, col_ix, base_value)
                    };
                    let display_text = display_value.display_text();
                    let is_null = display_value.is_null();
                    let is_auto_generated = display_value.is_auto_generated();

                    let state_for_click = state_entity.clone();
                    let state_for_context = state_entity.clone();

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
                        // Highlight individual dirty cells (like DBeaver)
                        .when(is_cell_dirty, |d| {
                            d.bg(theme.warning.opacity(0.2))
                                .border_l_2()
                                .border_color(theme.warning)
                        })
                        .when(is_selected, |d| {
                            d.bg(theme.table_active)
                                .border_color(theme.table_active_border)
                        })
                        .when(is_active, |d| d.border_1().border_color(theme.ring))
                        .text_sm()
                        .text_color(if is_pending_delete || is_null || is_auto_generated {
                            theme.muted_foreground
                        } else {
                            theme.foreground
                        })
                        .when(is_null || is_auto_generated, |d| d.italic())
                        .when(is_pending_delete, |d| d.line_through())
                        .on_click(move |event: &ClickEvent, window, cx| {
                            state_for_click.update(cx, |state, cx| {
                                state.focus(window, cx);
                            });

                            if event.click_count() == 2 {
                                state_for_click.update(cx, |state, cx| {
                                    state.start_editing(coord, window, cx);
                                });
                                return;
                            }

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
                        .child(display_text.to_string())
                        .into_any_element()
                })
                .collect();

            div()
                .id(("row", row_ix))
                .flex()
                .flex_shrink_0()
                .w(px(total_width))
                .h(ROW_HEIGHT)
                .overflow_hidden()
                .border_b_1()
                .border_color(theme.table_row_border)
                // Row state background (dirty=yellow, error=red)
                .when_some(row_bg, |d, bg| d.bg(bg))
                // Alternating row colors only when clean
                .when(row_bg.is_none() && row_ix % 2 == 1, |d| {
                    d.bg(theme.table_even)
                })
                .children(cells)
                .into_any_element()
        })
        .collect()
}
