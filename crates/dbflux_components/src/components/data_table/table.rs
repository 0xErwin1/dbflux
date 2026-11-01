use std::ops::Range;
use std::sync::{Arc, Mutex};

use crate::controls::{GpuiInput as Input, InputState};
use crate::fonts;
use crate::icons::AppIcon;
use crate::primitives::{Chamfer, ChamferRing, Icon};
use crate::tokens::{ChromeColors, CollectionMetrics, GridMetrics, RowColors, Spacing};
use gpui::ElementId;
use gpui::prelude::FluentBuilder;
use gpui::{
    Action, AnyElement, App, ClickEvent, Context, Entity, FontWeight, Hsla, InteractiveElement,
    IntoElement, Keystroke, ListSizingBehavior, MouseButton, MouseDownEvent, ParentElement, Pixels,
    ScrollWheelEvent, SharedString, StatefulInteractiveElement, Styled, Window, canvas, div, px,
    uniform_list,
};
use gpui_component::scroll::{Scrollbar, ScrollbarMode};
use gpui_component::{ActiveTheme, Sizable};

use super::document::DocumentPresentation;
use super::events::{ContextMenuAction, DataTableEvent, Direction, Edge};
use super::selection::{CellCoord, SelectionState};
use super::source::GridRowSource;
use super::state::DataTableState;
use super::theme::{CELL_PADDING_X, ROW_NUMBER_WIDTH, SCROLLBAR_WIDTH};
use bolt::data::{
    CellAlignment, CellContent, CellTone, CellView, NestedMarker, RowSource as _, RowVisualState,
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

/// Actions of the data table. The keymap binds them in the table's
/// [`CONTEXT`] (see `dbflux_ui_base::keymap`).
pub mod actions {
    gpui::actions!(
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
            // Document grids
            ToggleColumnGroup,
            StepOut,
        ]
    );
}

use actions::*;

/// Key context of the data table element. It is its own identifier rather
/// than the `Results` context a window root sets, so the table's keys never
/// match on the window root and a root binding never matches on the table.
pub const CONTEXT: &str = "DataTable";

/// The single-keystroke binding of the table action behind a context-menu
/// action, shown as the shortcut on that menu row.
///
/// Multi-keystroke vim sequences (`y y`, `d d`) are skipped, so an action
/// bound only to a sequence has no shortcut. Among single keystrokes the
/// first binding registered wins.
pub fn context_menu_keystroke(action: ContextMenuAction, cx: &App) -> Option<Keystroke> {
    let table_action: Box<dyn Action> = match action {
        ContextMenuAction::Copy => Box::new(Copy),
        ContextMenuAction::Edit => Box::new(StartEdit),
        ContextMenuAction::DeleteRow => Box::new(DeleteRow),
        ContextMenuAction::SetNull => Box::new(SetNull),
        ContextMenuAction::AddRow => Box::new(AddRow),
        ContextMenuAction::DuplicateRow => Box::new(DuplicateRow),
        _ => return None,
    };

    let keymap = cx.key_bindings();
    let keymap = keymap.borrow();

    keymap
        .bindings_for_action(table_action.as_ref())
        .find(|binding| binding.keystrokes().len() == 1)
        .and_then(|binding| binding.keystrokes().first())
        .map(|keystroke| keystroke.inner().clone())
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
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // A closed inline editor unmounted the focused element, which leaves the
        // window with no focus at all and every action bound to the table's key
        // context inert — Save Row on `secondary-enter` among them. This is the
        // nearest place that owns a `Window`, so the debt is settled here.
        if self
            .state
            .update(cx, |state, _cx| state.take_pending_refocus())
        {
            let focus_handle = self.state.read(cx).focus_handle().clone();
            focus_handle.focus(window, cx);
        }

        self.state
            .update(cx, |state, cx| state.sync_grid_text_metrics(cx));

        let state = self.state.read(cx);
        let theme = cx.theme();

        let row_count = state.row_count();
        let col_count = state.col_count();

        let vertical_scroll_handle = state.vertical_scroll_handle().clone();
        let horizontal_scroll_handle = state.horizontal_scroll_handle().clone();
        let focus_handle = state.focus_handle().clone();

        let total_width = state.total_content_width();
        let record_mode = state.record_mode();

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
                state.request_save_all(cx);
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
                // A host makes the table read-only while its edits must not
                // change, so the history must not change them either.
                if !state.is_editable() && !state.is_insertable() {
                    return;
                }

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
                // A host makes the table read-only while its edits must not
                // change, so the history must not change them either.
                if !state.is_editable() && !state.is_insertable() {
                    return;
                }

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

        let s = self.state.clone();
        let on_toggle_column_group = move |_: &ToggleColumnGroup, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| {
                if state.document_presentation().is_none() || state.is_editing() {
                    return;
                }
                if let Some(coord) = state.selection().active {
                    cx.emit(DataTableEvent::ToggleColumnGroupRequested { col: coord.col });
                }
            });
        };

        let s = self.state.clone();
        let on_step_out = move |_: &StepOut, _: &mut Window, cx: &mut App| {
            s.update(cx, |state, cx| {
                if state.document_presentation().is_some() && !state.is_editing() {
                    cx.emit(DataTableEvent::StepOutRequested);
                }
            });
        };

        let header_height = state.header_height(cx);

        // Main layout: vertical flex with header and scrollable body.
        // Both header and body share the same horizontal scroll handle.
        let state_for_empty_context = self.state.clone();
        let focus_for_empty = focus_handle.clone();

        // Resize drag handlers live on the root div so they keep firing
        // even when the cursor leaves the narrow 6px resize handle.
        let resize_drag_for_move = self.resize_drag.clone();
        let state_for_resize_move = self.state.clone();
        let resize_drag_for_up = self.resize_drag.clone();

        // Forward horizontal wheel/trackpad deltas to the horizontal scroll
        // handle. The handle is owned by a 1px phantom scroller at the bottom
        // (so the gpui-component scrollbar can drive it), which means
        // horizontal wheel events landing on the header or body would
        // otherwise be lost. The body's uniform_list still consumes delta.y
        // natively. We deliberately do not synthesise a horizontal delta
        // from shift+wheel because the list consumes delta.y first and we
        // would end up scrolling both axes at once.
        //
        // After updating the scroll handle we synchronously bump
        // `state.horizontal_offset` so the body shift (`ml(-h_offset)`) and
        // the scrollbar move on the same frame; otherwise the canvas-based
        // sync runs one frame later and the table reads as jittery during
        // trackpad momentum.
        let state_for_wheel = self.state.clone();
        let on_scroll_wheel = move |event: &ScrollWheelEvent, window: &mut Window, cx: &mut App| {
            let delta_x = event.delta.pixel_delta(window.line_height()).x;
            if delta_x == px(0.0) {
                return;
            }
            let consumed = state_for_wheel.update(cx, |state, cx| {
                if state.apply_horizontal_wheel_delta(delta_x) {
                    state.sync_horizontal_offset(cx);
                    true
                } else {
                    false
                }
            });
            if consumed {
                cx.stop_propagation();
            }
        };

        // Record mode replaces the whole grid — header, body, and horizontal
        // scrolling alike — with the transposed single-row view. It shares the
        // root below so focus, key context, and every action binding stay
        // identical in both presentations.
        let inner_table = if record_mode {
            div()
                .id("table-inner")
                .flex()
                .flex_col()
                .size_full()
                .child(super::record::render_record(&self.state, state, cx))
        } else {
            // Build header
            let header = match state.document_presentation() {
                Some(document) => self
                    .render_document_header(state, document, total_width, theme, cx)
                    .into_any_element(),
                None => self
                    .render_header(state, total_width, theme, cx)
                    .into_any_element(),
            };

            // Build body using uniform_list for virtualization
            let body = self.render_body(row_count, total_width, cx);

            div()
                .id("table-inner")
                .flex()
                .flex_col()
                .size_full()
                .on_scroll_wheel(on_scroll_wheel)
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
                                focus_for_empty.focus(window, cx);
                            })
                            .on_mouse_down(MouseButton::Right, move |event, window, cx| {
                                cx.stop_propagation();
                                state_for_empty_context.update(cx, |state, cx| {
                                    state.focus(window, cx);
                                    cx.emit(DataTableEvent::ContextMenuRequested {
                                        row: 0,
                                        col: 0,
                                        position: event.position,
                                        is_column_header: false,
                                    });
                                });
                            }),
                    )
                })
        };

        div()
            .id(self.id.clone())
            .key_context(CONTEXT)
            .track_focus(&focus_handle)
            .relative()
            .size_full()
            .overflow_hidden()
            .font_family(fonts::grid_family(cx))
            .bg(theme.table)
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
            .on_action(on_toggle_column_group)
            .on_action(on_step_out)
            // Column resize: move and up handlers on the root div so the drag
            // continues even when the cursor leaves the 6px handle area.
            .on_mouse_move(move |event, _window, cx| {
                let (col, new_width) = {
                    let drag = resize_drag_for_move.lock().ok();
                    if let Some(drag) = drag {
                        if let Some(col) = drag.col {
                            let delta = event.position.x - drag.start_x;
                            let delta_f32: f32 = delta.into();
                            let new_width = drag.original_width + delta_f32;
                            (Some(col), new_width.max(super::theme::MIN_COLUMN_WIDTH))
                        } else {
                            (None, 0.0)
                        }
                    } else {
                        (None, 0.0)
                    }
                };
                if let Some(col) = col {
                    state_for_resize_move.update(cx, |state, cx| {
                        state.set_column_width(col, new_width, cx);
                    });
                }
            })
            .on_mouse_up(MouseButton::Left, move |_event, _window, _cx| {
                if let Ok(mut drag) = resize_drag_for_up.lock() {
                    drag.col = None;
                }
            })
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
            // Grid-only scroll chrome. Record mode owns a single vertical list
            // and no horizontal axis, so it brings its own scrollbar instead.
            .when(!record_mode, |root| {
                root
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
                    // Scrollbars as absolute overlays.
                    .child(
                        div()
                            .absolute()
                            .top(header_height)
                            .right_0()
                            .bottom_0()
                            .w(SCROLLBAR_WIDTH)
                            .when(row_count > 0, |this| {
                                // The overlay strip below the header is the
                                // visible viewport. The handle's own bounds are
                                // the uniform_list element, which extends past
                                // the visible area when the grid overflows
                                // horizontally — deriving the hitbox from them
                                // makes right-edge track clicks miss. Same
                                // binding the horizontal bar above uses.
                                this.child(
                                    Scrollbar::vertical(&vertical_scroll_handle)
                                        .viewport_from_layout(),
                                )
                            }),
                    )
                    // Horizontal scrollbar uses `ScrollbarMode::Always` because the phantom
                    // scroller that owns the handle is 1px tall and never captures the wheel,
                    // so the bar would otherwise stay transparent until the user navigates
                    // off-screen with the keyboard.
                    //
                    // The scrollbar lays itself out over its handle's viewport by default,
                    // which here is the 1px phantom, so the bar would be clipped to a
                    // single pixel. It takes this overlay strip as its viewport instead;
                    // the strip spans the phantom's width, so the thumb math matches.
                    .child(
                        div()
                            .absolute()
                            .left_0()
                            .right(SCROLLBAR_WIDTH)
                            .bottom_0()
                            .h(SCROLLBAR_WIDTH)
                            .child(
                                Scrollbar::horizontal(&horizontal_scroll_handle)
                                    .mode(ScrollbarMode::Always)
                                    .viewport_from_layout(),
                            ),
                    )
            })
    }
}

impl DataTable {
    fn render_header(
        &self,
        state: &DataTableState,
        total_width: f32,
        theme: &gpui_component::theme::Theme,
        cx: &gpui::App,
    ) -> impl IntoElement {
        let model = state.model();
        let sort = state.sort();
        let font_size = fonts::grid_font_size(cx);
        let type_font_size = fonts::grid_type_font_size(cx);
        let column_widths = state.column_widths();
        let h_offset = state.horizontal_offset();
        let state_entity = self.state.clone();
        let resize_drag = self.resize_drag.clone();

        let pk_cols = state.pk_columns().to_vec();
        let fk_cols = state.fk_columns().clone();
        let annotated = state.has_header_annotations();
        let header_height = state.header_height(cx);

        let header_cells: Vec<_> = model
            .columns
            .iter()
            .enumerate()
            .map(|(col_ix, col_spec)| {
                let width = column_widths.get(col_ix).copied().unwrap_or(120.0);
                let sort_direction = sort
                    .filter(|sort| sort.column_ix == col_ix)
                    .map(|sort| sort.direction);

                let is_pk = pk_cols.contains(&col_ix);
                let is_fk = fk_cols.contains(&col_ix);

                let key_icon = if is_pk {
                    Some((AppIcon::KeyRound, theme.warning))
                } else if is_fk {
                    Some((AppIcon::Cable, theme.info))
                } else {
                    None
                };

                let type_label: SharedString = col_spec.type_name.clone().into();
                let annotation = state.header_annotation(col_ix).cloned();

                let state_for_click = state_entity.clone();
                let resize_drag_for_down = resize_drag.clone();

                div()
                    .id(("header-col", col_ix))
                    .relative()
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .gap(GridMetrics::HEADER_GAP)
                    .h_full()
                    .w(px(width))
                    .px(CELL_PADDING_X)
                    .overflow_hidden()
                    .border_r_1()
                    .border_color(theme.border)
                    .hover(|s| s.bg(theme.table_hover))
                    .cursor_pointer()
                    .on_click(move |_event: &ClickEvent, _window, cx| {
                        state_for_click.update(cx, |state, cx| {
                            state.cycle_sort(col_ix, cx);
                        });
                    })
                    // Right-click opens the menu scoped to this column: its
                    // ordering and filtering in one flat list. Left click keeps
                    // cycling the sort — the fastest interaction in the grid,
                    // not to be spent on opening a menu for the rare case.
                    .on_mouse_down(MouseButton::Right, {
                        let state_for_menu = state_entity.clone();
                        move |event: &MouseDownEvent, window, cx| {
                            cx.stop_propagation();
                            state_for_menu.update(cx, |state, cx| {
                                state.focus(window, cx);
                                let row = state.selection().active.map(|c| c.row).unwrap_or(0);
                                cx.emit(DataTableEvent::ContextMenuRequested {
                                    row,
                                    col: col_ix,
                                    position: event.position,
                                    is_column_header: true,
                                });
                            });
                        }
                    })
                    // PK / FK key icon: amber key for a primary key, blue
                    // cable for a foreign key.
                    .when_some(key_icon, |d, (icon, color)| {
                        d.child(Icon::new(icon).size(GridMetrics::HEADER_ICON).color(color))
                    })
                    // Column name — primary affordance, never shrinks. It
                    // pushes the (secondary) type label out of the cell before
                    // its own characters get truncated.
                    .child(
                        div()
                            .flex_shrink_0()
                            .whitespace_nowrap()
                            .text_size(font_size)
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(ChromeColors::strong(theme))
                            .child(SharedString::from(col_spec.title.clone())),
                    )
                    // Type label — muted metadata. Shrinks and truncates first
                    // when the cell runs out of horizontal space.
                    .when_some(
                        (!type_label.is_empty()).then_some(type_label),
                        |d, label| {
                            d.child(
                                div()
                                    .min_w_0()
                                    .flex_1()
                                    .overflow_hidden()
                                    .text_ellipsis()
                                    .whitespace_nowrap()
                                    .text_size(type_font_size)
                                    .text_color(theme.muted_foreground)
                                    .child(label),
                            )
                        },
                    )
                    .when_some(sort_direction, |d, direction| {
                        let arrow = match direction {
                            SortDirection::Ascending => AppIcon::ArrowUp,
                            SortDirection::Descending => AppIcon::ArrowDown,
                        };

                        d.child(
                            div().flex_shrink_0().ml_auto().child(
                                Icon::new(arrow)
                                    .size(GridMetrics::HEADER_ICON)
                                    .color(ChromeColors::tint(theme)),
                            ),
                        )
                    })
                    // An annotated header pins the name line to the top and
                    // draws the facts along the bottom, so columns with and
                    // without facts keep their names on the same line.
                    .when(annotated, |d| d.items_start().pt(Spacing::SM))
                    .when_some(annotation, |d, annotation| {
                        d.child(
                            div()
                                .debug_selector(|| "table-header-annotation".to_string())
                                .absolute()
                                .left(CELL_PADDING_X)
                                .right(CELL_PADDING_X)
                                .bottom(Spacing::XXS)
                                .flex()
                                .justify_between()
                                .gap(GridMetrics::HEADER_GAP)
                                .overflow_hidden()
                                .whitespace_nowrap()
                                .text_size(type_font_size)
                                .text_color(theme.muted_foreground)
                                .child(
                                    div()
                                        .min_w_0()
                                        .truncate()
                                        .text_color(theme.foreground)
                                        .child(annotation.leading),
                                )
                                .when_some(annotation.trailing, |line, trailing| {
                                    line.child(div().flex_shrink_0().child(trailing))
                                }),
                        )
                    })
                    // Resize handle: mouse-down starts the drag; move/up are
                    // handled on the DataTable root div so the drag survives
                    // the cursor leaving this 6px strip.
                    .child(
                        div()
                            .id(("resize-handle", col_ix))
                            .absolute()
                            .right_0()
                            .top_0()
                            .bottom_0()
                            .w(px(6.0)) // guardrail-allow: resize handle width, not spacing
                            .cursor_col_resize()
                            .hover(|s| s.bg(ChromeColors::tint(theme).opacity(0.3)))
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
                            ),
                    )
            })
            .collect();

        // Header uses overflow_hidden and applies horizontal offset via margin.
        // The phantom scroller owns the scroll handle; header just follows the offset.
        div()
            .id("table-header")
            .debug_selector(|| "table-header".to_string())
            .flex_shrink_0()
            .h(header_height)
            .overflow_hidden()
            .bg(theme.table_head)
            .border_b_1()
            .border_color(theme.input)
            .font_family(fonts::grid_family(cx))
            .child(
                div()
                    .flex()
                    .h_full()
                    .min_w(px(total_width))
                    .ml(-h_offset)
                    .child(div().flex_shrink_0().w(ROW_NUMBER_WIDTH))
                    .children(header_cells),
            )
    }

    /// Header of a document grid: an optional row of column-group labels over
    /// the name row, and a presence bar under each name when the host knows
    /// how often each field appears.
    fn render_document_header(
        &self,
        state: &DataTableState,
        document: &DocumentPresentation,
        total_width: f32,
        theme: &gpui_component::theme::Theme,
        cx: &gpui::App,
    ) -> impl IntoElement {
        let model = state.model();
        let column_widths = state.column_widths();
        let h_offset = state.horizontal_offset();
        let sort = state.sort();
        let tint = ChromeColors::tint(theme);
        let strong = ChromeColors::strong(theme);
        let muted = theme.muted_foreground;
        let has_presence = document.has_presence();
        let name_row_height = document.name_row_height(cx);
        let font_size = fonts::grid_font_size(cx);
        let col_count = model.col_count();

        let width_of = |col: usize| column_widths.get(col).copied().unwrap_or(120.0);

        let group_row = document.has_groups().then(|| {
            let cells = document
                .group_spans(col_count)
                .into_iter()
                .map(|span| {
                    let width: f32 = span.columns.clone().map(width_of).sum();

                    div()
                        .flex()
                        .flex_shrink_0()
                        .items_center()
                        .gap(GridMetrics::HEADER_GAP)
                        .w(px(width))
                        .h_full()
                        .border_r_1()
                        .border_color(theme.border)
                        .when_some(span.group, |cell, group| {
                            cell.px(CELL_PADDING_X)
                                .bg(tint.opacity(CollectionMetrics::GROUP_HEADER_ALPHA))
                                .text_color(strong)
                                .child(
                                    Icon::new(AppIcon::ChevronDown)
                                        .size(GridMetrics::HEADER_ICON)
                                        .color(muted),
                                )
                                .child(SharedString::from(group.label.to_string()))
                                .child(
                                    div()
                                        .text_size(fonts::grid_px(
                                            cx,
                                            CollectionMetrics::HEADER_META_FONT,
                                        ))
                                        .text_color(muted)
                                        .child(SharedString::from(group.type_label.to_string())),
                                )
                        })
                })
                .collect::<Vec<_>>();

            div()
                .flex()
                .flex_shrink_0()
                .h(DocumentPresentation::group_row_height(cx))
                .min_w(px(total_width))
                .ml(-h_offset)
                .border_b_1()
                .border_color(theme.border)
                .text_size(fonts::grid_px(cx, CollectionMetrics::GROUP_FONT))
                .child(div().flex_shrink_0().w(ROW_NUMBER_WIDTH))
                .children(cells)
        });

        let name_cells = model
            .columns
            .iter()
            .enumerate()
            .map(|(col_ix, col_spec)| {
                let width = width_of(col_ix);
                let header = document.header(col_ix);
                let is_child = header.is_some_and(|header| header.group.is_some());
                let presence = header.and_then(|header| header.presence);
                let sort_direction = sort
                    .filter(|sort| sort.column_ix == col_ix)
                    .map(|sort| sort.direction);
                let state_for_click = self.state.clone();
                let state_for_menu = self.state.clone();
                let resize_drag_for_down = self.resize_drag.clone();

                let name_line = div()
                    .flex()
                    .items_center()
                    .gap(GridMetrics::HEADER_GAP)
                    .min_w_0()
                    .child(
                        div()
                            .flex_shrink_0()
                            .whitespace_nowrap()
                            .text_size(font_size)
                            .when(!is_child, |name| name.font_weight(FontWeight::SEMIBOLD))
                            .text_color(strong)
                            .child(SharedString::from(col_spec.title.to_string())),
                    )
                    .child(
                        div()
                            .min_w_0()
                            .overflow_hidden()
                            .whitespace_nowrap()
                            .text_size(fonts::grid_px(cx, CollectionMetrics::HEADER_META_FONT))
                            .text_color(muted)
                            .child(SharedString::from(col_spec.type_name.to_string())),
                    )
                    .when_some(sort_direction, |line, direction| {
                        let arrow = match direction {
                            SortDirection::Ascending => AppIcon::ArrowUp,
                            SortDirection::Descending => AppIcon::ArrowDown,
                        };
                        line.child(
                            div()
                                .flex_shrink_0()
                                .ml_auto()
                                .child(Icon::new(arrow).size(GridMetrics::HEADER_ICON).color(tint)),
                        )
                    });

                let presence_line = presence.map(|ratio| {
                    let clamped = ratio.clamp(0.0, 1.0);
                    let bar_color = if clamped >= 0.999 {
                        theme.success
                    } else {
                        theme.warning
                    };
                    let bar_width: f32 = f32::from(CollectionMetrics::PRESENCE_BAR_WIDTH) * clamped;

                    div()
                        .flex()
                        .items_center()
                        .gap(GridMetrics::HEADER_GAP)
                        .child(
                            div()
                                .flex_shrink_0()
                                .w(CollectionMetrics::PRESENCE_BAR_WIDTH)
                                .h(CollectionMetrics::PRESENCE_BAR_HEIGHT)
                                .bg(theme.secondary)
                                .child(
                                    div()
                                        .w(px(bar_width))
                                        .h(CollectionMetrics::PRESENCE_BAR_HEIGHT)
                                        .bg(bar_color),
                                ),
                        )
                        .child(
                            div()
                                .text_size(fonts::grid_px(cx, CollectionMetrics::HEADER_META_FONT))
                                .text_color(muted)
                                .child(format!("{}%", (clamped * 100.0).round() as u32)),
                        )
                });

                div()
                    .id(("header-col", col_ix))
                    .relative()
                    .flex()
                    .flex_col()
                    .flex_shrink_0()
                    .justify_center()
                    .gap(CollectionMetrics::HEADER_LINE_GAP)
                    .h_full()
                    .w(px(width))
                    .px(CELL_PADDING_X)
                    .overflow_hidden()
                    .border_r_1()
                    .border_color(theme.border)
                    .when(is_child, |cell| {
                        cell.bg(tint.opacity(CollectionMetrics::GROUP_CHILD_HEADER_ALPHA))
                    })
                    .hover(|cell| cell.bg(theme.table_hover))
                    .cursor_pointer()
                    .on_click(move |_event: &ClickEvent, _window, cx| {
                        state_for_click.update(cx, |state, cx| state.cycle_sort(col_ix, cx));
                    })
                    .on_mouse_down(
                        MouseButton::Right,
                        move |event: &MouseDownEvent, window, cx| {
                            cx.stop_propagation();
                            state_for_menu.update(cx, |state, cx| {
                                state.focus(window, cx);
                                let row = state.selection().active.map(|c| c.row).unwrap_or(0);
                                cx.emit(DataTableEvent::ContextMenuRequested {
                                    row,
                                    col: col_ix,
                                    position: event.position,
                                    is_column_header: true,
                                });
                            });
                        },
                    )
                    .child(name_line)
                    .when_some(presence_line, |cell, line| cell.child(line))
                    .child(
                        div()
                            .id(("resize-handle", col_ix))
                            .absolute()
                            .right_0()
                            .top_0()
                            .bottom_0()
                            .w(px(6.0)) // guardrail-allow: resize handle width, not spacing
                            .cursor_col_resize()
                            .hover(|handle| handle.bg(tint.opacity(0.3)))
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
                            ),
                    )
            })
            .collect::<Vec<_>>();

        div()
            .id("table-header")
            .flex()
            .flex_col()
            .flex_shrink_0()
            .overflow_hidden()
            .bg(theme.table_head)
            .font_family(fonts::grid_family(cx))
            .when_some(group_row, |header, row| header.child(row))
            .child(
                div()
                    .flex()
                    .flex_shrink_0()
                    .h(name_row_height)
                    .min_w(px(total_width))
                    .ml(-h_offset)
                    .border_b_1()
                    .border_color(theme.input)
                    .when(!has_presence, |row| row.items_center())
                    .child(div().flex_shrink_0().w(ROW_NUMBER_WIDTH))
                    .children(name_cells),
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
        let mut list = uniform_list(
            "table-rows",
            row_count,
            move |visible_range: Range<usize>, _window: &mut Window, cx: &mut App| {
                let null_color = crate::tokens::SyntaxColors::for_current(cx).number;
                let font_size = fonts::grid_font_size(cx);
                let row_height = fonts::grid_row_height(cx);
                let nested_icon = fonts::grid_px(cx, CollectionMetrics::NESTED_ICON);
                let theme = cx.theme();
                // Read state INSIDE closure - only when actually rendering
                let state = state_entity.read(cx);

                let editing_cell = state.editing_cell();
                let cell_input = state.cell_input().cloned();
                let enum_dropdown = state.enum_dropdown().cloned();
                let source = GridRowSource::with_document(
                    &model,
                    state.edit_buffer(),
                    state.document_presentation(),
                );

                render_rows(
                    &state_entity,
                    visible_range,
                    &source,
                    state.column_widths(),
                    state.selection(),
                    editing_cell,
                    cell_input.as_ref(),
                    enum_dropdown.as_ref(),
                    state.has_reference_row(),
                    total_width,
                    font_size,
                    row_height,
                    nested_icon,
                    null_color,
                    theme,
                )
            },
        )
        .size_full()
        .min_w(px(total_width))
        .ml(-h_offset)
        .with_sizing_behavior(ListSizingBehavior::Auto)
        .track_scroll(&vertical_scroll_handle);

        // Stop GPUI's paint_scroll_listener from translating a non-zero delta.x
        // into delta.y on this vertical-only list. The platform layer maps
        // shift+wheel to delta.x with delta.y == 0, and without this flag the
        // list would scroll vertically using that delta.x while the outer
        // on_scroll_wheel handler also scrolls horizontally — both axes move
        // at once. With restrict_scroll_to_axis set, shift+wheel becomes a
        // pure horizontal scroll driven by the outer handler.
        list.style().restrict_scroll_to_axis = Some(true);

        div()
            .id("table-body")
            .flex_1()
            .min_h_0()
            .overflow_hidden()
            .child(list)
    }
}

/// The paint decisions one cell derives from the [`CellView`] contract plus
/// the host state that is not data (the active cell, the row's pending
/// delete). Split from the element builder so every branch the painter takes
/// is assertable without pixels.
#[derive(Clone)]
struct CellPaint {
    /// The color of the cell's leading text.
    text_color: Hsla,
    italic: bool,
    line_through: bool,
    /// The relational dirty mark: a dirty background and a 2px warning
    /// accent. A document grid stages its edits as transitions instead.
    dirty_mark: bool,
    /// The document transition: a warning wash and a chamfer, whatever the
    /// tone says.
    transition: bool,
    /// The group wash of a grouped document column.
    group_wash: bool,
}

fn cell_paint(
    view: &CellView,
    group_child: bool,
    row_pending_delete: bool,
    is_active: bool,
    theme: &gpui_component::theme::Theme,
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

/// Renders the visible rows for the uniform_list.
#[allow(clippy::too_many_arguments)]
fn render_rows(
    state_entity: &Entity<DataTableState>,
    visible_range: Range<usize>,
    source: &GridRowSource<'_>,
    column_widths: &[f32],
    selection: &SelectionState,
    editing_cell: Option<CellCoord>,
    cell_input: Option<&Entity<InputState>>,
    enum_dropdown: Option<&Entity<crate::controls::Dropdown>>,
    reference_row: bool,
    total_width: f32,
    font_size: Pixels,
    row_height: Pixels,
    nested_icon: Pixels,
    null_color: Hsla,
    theme: &gpui_component::theme::Theme,
) -> Vec<AnyElement> {
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
            let is_active_row = selection
                .active
                .is_some_and(|active| active.row == visual_ix);
            let tint = ChromeColors::tint(theme);
            let cell_wash = tint.opacity(GridMetrics::CELL_SELECTED_ALPHA);
            let is_reference_row = reference_row && visual_ix == 0;
            // Under a reference row the others are numbered from 1, so the
            // number reads as their rank.
            let row_number = if reference_row {
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
                    let width = column_widths.get(col_ix).copied().unwrap_or(120.0);
                    let coord = CellCoord::new(visual_ix, col_ix);
                    let is_selected = selection.is_selected(coord);
                    let is_active = selection.active == Some(coord);
                    let is_editing = editing_cell == Some(coord);

                    if is_editing {
                        if let Some(dropdown) = enum_dropdown {
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
                                .child(dropdown.clone())
                                .into_any_element();
                        }

                        if let Some(input_state) = cell_input {
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
                                .child(Input::new(input_state).small())
                                .into_any_element();
                        }
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

                    let state_for_click = state_entity.clone();
                    let state_for_context = state_entity.clone();

                    div()
                        .id(("cell", visual_ix * 10000 + col_ix))
                        .relative()
                        .flex()
                        .flex_shrink_0()
                        .items_center()
                        .h_full()
                        .w(px(width))
                        .px(CELL_PADDING_X)
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

                            let modifiers = event.modifiers();
                            state_for_click.update(cx, |state, cx| {
                                state.click_cell(coord, modifiers, cx);
                            });
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
                                        is_column_header: false,
                                    });
                                });
                            },
                        )
                        .when_some(view.nested, |d, _| {
                            d.gap(GridMetrics::HEADER_GAP).child(
                                div()
                                    .debug_selector(|| "table-cell-braces".to_string())
                                    .child(
                                        Icon::new(AppIcon::Braces)
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
                                        Icon::new(AppIcon::ChevronRight)
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
                        .w(ROW_NUMBER_WIDTH)
                        .h_full()
                        .pr(CELL_PADDING_X)
                        .text_size(font_size)
                        .text_color(if is_active_row || is_reference_row {
                            tint
                        } else {
                            theme.input
                        })
                        .map(|gutter| {
                            if is_reference_row {
                                gutter.child(
                                    Icon::new(AppIcon::Crosshair).size(nested_icon).color(tint),
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

#[cfg(test)]
mod tests {
    use super::super::model::TableModel;
    use super::*;
    use gpui::{AppContext as _, Render, TestAppContext, VisualTestContext, div};
    use gpui_base::ScrollbarHandle as _;

    struct ScrollHarness {
        table: Entity<DataTable>,
    }

    impl Render for ScrollHarness {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .w(px(600.0))
                .h(px(300.0))
                .debug_selector(|| "scroll-harness".to_string())
                .child(self.table.clone())
        }
    }

    fn overflow_model() -> Arc<TableModel> {
        use super::super::model::{CellValue, ColumnKind, ColumnSpec, RowData};
        use gpui::TextAlign;

        let columns = vec![
            ColumnSpec {
                id: "id".into(),
                title: "id".into(),
                kind: ColumnKind::Integer,
                align: TextAlign::Left,
                type_name: "int8".into(),
            },
            ColumnSpec {
                id: "name".into(),
                title: "name".into(),
                kind: ColumnKind::Text,
                align: TextAlign::Left,
                type_name: "text".into(),
            },
        ];
        let rows = (0..100)
            .map(|i| RowData {
                cells: vec![CellValue::int(i as i64), CellValue::text("row")],
            })
            .collect();
        Arc::new(TableModel::new(columns, rows))
    }

    fn offsets(
        visual: &mut VisualTestContext,
        state: &Entity<DataTableState>,
    ) -> (gpui::Pixels, gpui::Pixels) {
        visual.update(|_, cx| {
            let vertical = state.read(cx).vertical_scroll_handle().offset().y;
            let horizontal = state.read(cx).horizontal_scroll_handle().offset().x;
            (vertical, horizontal)
        })
    }

    /// Clicking the vertical scrollbar track must scroll the body vertically
    /// and leave the horizontal axis untouched, even when the grid overflows
    /// horizontally (columns wider than the viewport).
    ///
    /// The vertical `Scrollbar` overlays a positioned strip below the header.
    /// Without `.viewport_from_layout()` the bar derives its hitbox from the
    /// uniform_list handle's bounds, which extend past the visible viewport
    /// when the list is wider than the table — the right-edge track click
    /// misses and the grid never scrolls. The binding takes the overlay strip
    /// itself as the visible viewport, matching the horizontal bar.
    #[gpui::test]
    fn vertical_scrollbar_track_click_scrolls_vertically_only(cx: &mut TestAppContext) {
        cx.update(crate::theme::init);
        cx.update(|cx| {
            gpui_component::Theme::set_scrollbar_mode(
                gpui_component::scroll::ScrollbarMode::Always,
                cx,
            );
        });

        let state_holder: std::rc::Rc<
            std::cell::RefCell<Option<(Entity<DataTableState>, Entity<DataTable>)>>,
        > = std::rc::Rc::default();
        let holder_for_view = state_holder.clone();

        let (_, visual) = cx.add_window_view(move |_window, cx| {
            let state = cx.new(|cx| DataTableState::new(overflow_model(), cx));
            let table = cx.new(|cx| DataTable::new("scroll-test-table", state.clone(), cx));
            holder_for_view.replace(Some((state.clone(), table.clone())));
            ScrollHarness { table }
        });

        let (state, _table) = state_holder
            .borrow()
            .clone()
            .expect("state and table entities must be created");

        // Force horizontal overflow: column 0 wider than the window.
        visual.update(|_, cx| {
            state.update(cx, |s, cx| s.set_column_width(0, 1000.0, cx));
        });
        visual.run_until_parked();

        let harness = visual
            .debug_bounds("scroll-harness")
            .expect("harness must render");

        // Precondition: the grid must actually overflow horizontally, and the
        // horizontal axis must start unscrolled — otherwise this test exercises
        // nothing.
        let total_width = visual.update(|_, cx| state.read(cx).total_content_width());
        assert!(
            harness.size.width < px(total_width),
            "fixture must overflow horizontally: viewport {:?} vs content {total_width}",
            harness.size.width
        );

        // Track point: rightmost strip, below the header, above the
        // horizontal scrollbar strip.
        let track_x =
            harness.origin.x + harness.size.width - super::super::theme::SCROLLBAR_WIDTH / 2.0;
        let header_height = visual.update(|_, cx| fonts::grid_header_height(cx));
        let track_y = harness.origin.y
            + header_height
            + (harness.size.height - header_height - super::super::theme::SCROLLBAR_WIDTH) / 2.0;

        let (y_before, x_before) = offsets(visual, &state);
        assert_eq!(x_before, gpui::px(0.0), "horizontal must start unscrolled");

        visual.simulate_click(gpui::point(track_x, track_y), gpui::Modifiers::default());
        visual.run_until_parked();

        let (y_after, x_after) = offsets(visual, &state);
        assert!(
            y_after < y_before,
            "clicking the vertical track must scroll the body vertically \
             (before {y_before:?}, after {y_after:?})"
        );
        assert!(
            y_after <= gpui::px(0.0),
            "vertical offset must move into scrolled range (after {y_after:?})"
        );
        assert_eq!(
            x_after, x_before,
            "vertical track click must not scroll horizontally"
        );
    }

    fn dirty_cells(visual: &mut VisualTestContext, state: &Entity<DataTableState>) -> usize {
        visual.update(|_, cx| state.read(cx).edit_buffer().dirty_rows().len())
    }

    fn set_editable(
        visual: &mut VisualTestContext,
        state: &Entity<DataTableState>,
        editable: bool,
    ) {
        visual.update(|_, cx| {
            state.update(cx, |state, _| {
                state.set_positional_editing(editable);
                state.set_insertable(editable);
            });
        });
    }

    /// A host makes a table read-only while its edits must not change (a
    /// save or a reload is running): undo and redo then change nothing.
    #[gpui::test]
    fn undo_and_redo_change_nothing_in_a_read_only_table(cx: &mut TestAppContext) {
        use super::super::model::CellValue;

        cx.update(crate::theme::init);

        let state_holder: std::rc::Rc<std::cell::RefCell<Option<Entity<DataTableState>>>> =
            std::rc::Rc::default();
        let holder_for_view = state_holder.clone();

        let (_, visual) = cx.add_window_view(move |_window, cx| {
            let state = cx.new(|cx| DataTableState::new(overflow_model(), cx));
            let table = cx.new(|cx| DataTable::new("undo-test-table", state.clone(), cx));
            holder_for_view.replace(Some(state));
            ScrollHarness { table }
        });

        let state = state_holder
            .borrow()
            .clone()
            .expect("the state entity must be created");

        visual.update(|window, cx| {
            state.update(cx, |state, cx| {
                state.set_positional_editing(true);
                state
                    .edit_buffer_mut()
                    .set_cell(0, 1, CellValue::text("edited"));
                state.focus(window, cx);
            });
        });
        visual.run_until_parked();

        set_editable(visual, &state, false);
        visual.dispatch_action(actions::Undo);
        assert_eq!(dirty_cells(visual, &state), 1, "undo is refused");

        set_editable(visual, &state, true);
        visual.dispatch_action(actions::Undo);
        assert_eq!(dirty_cells(visual, &state), 0, "undo runs");

        set_editable(visual, &state, false);
        visual.dispatch_action(actions::Redo);
        assert_eq!(dirty_cells(visual, &state), 0, "redo is refused");

        set_editable(visual, &state, true);
        visual.dispatch_action(actions::Redo);
        assert_eq!(dirty_cells(visual, &state), 1, "redo runs");
    }

    fn header_harness(
        cx: &mut TestAppContext,
        annotations: Option<Vec<Option<super::super::HeaderAnnotation>>>,
    ) -> &mut VisualTestContext {
        cx.update(crate::theme::init);

        let (_, visual) = cx.add_window_view(move |_window, cx| {
            let state = cx.new(|cx| {
                let mut state = DataTableState::new(overflow_model(), cx);

                if let Some(annotations) = annotations {
                    state.set_header_annotations(annotations, cx);
                }

                state
            });
            let table = cx.new(|cx| DataTable::new("header-test-table", state, cx));
            ScrollHarness { table }
        });
        visual.run_until_parked();

        visual
    }

    /// A one-column, one-row grid whose single cell holds `cell`, so the
    /// debug selectors below name exactly one element.
    fn single_cell_harness(
        cx: &mut TestAppContext,
        cell: super::super::model::CellValue,
    ) -> &mut VisualTestContext {
        use super::super::model::{ColumnKind, ColumnSpec, RowData};
        use gpui::TextAlign;

        cx.update(crate::theme::init);

        let model = Arc::new(TableModel::new(
            vec![ColumnSpec {
                id: "v".into(),
                title: "v".into(),
                kind: ColumnKind::Text,
                align: TextAlign::Left,
                type_name: "text".into(),
            }],
            vec![RowData { cells: vec![cell] }],
        ));

        let (_, visual) = cx.add_window_view(move |_window, cx| {
            let state = cx.new(|cx| DataTableState::new(model.clone(), cx));
            let table = cx.new(|cx| DataTable::new("cell-test-table", state, cx));
            ScrollHarness { table }
        });
        visual.run_until_parked();

        visual
    }

    #[gpui::test]
    fn an_array_cell_draws_the_braces_and_the_chevron(cx: &mut TestAppContext) {
        use super::super::model::CellValue;

        let visual = single_cell_harness(cx, CellValue::nested(true, 4));

        assert!(
            visual.debug_bounds("table-cell-braces").is_some(),
            "a nested summary draws the braces marker"
        );
        assert!(
            visual.debug_bounds("table-cell-chevron").is_some(),
            "an array draws the step-in chevron"
        );
    }

    #[gpui::test]
    fn a_document_cell_draws_the_braces_but_not_the_chevron(cx: &mut TestAppContext) {
        use super::super::model::CellValue;

        let visual = single_cell_harness(cx, CellValue::nested(false, 3));

        assert!(visual.debug_bounds("table-cell-braces").is_some());
        assert!(
            visual.debug_bounds("table-cell-chevron").is_none(),
            "only an array steps in; a document draws no chevron"
        );
    }

    #[gpui::test]
    fn a_plain_cell_draws_one_text_span(cx: &mut TestAppContext) {
        use super::super::model::CellValue;

        let visual = single_cell_harness(cx, CellValue::text("value"));

        assert!(visual.debug_bounds("table-cell-text").is_some());
        assert!(visual.debug_bounds("table-cell-secondary").is_none());
        assert!(visual.debug_bounds("table-cell-label").is_none());
    }

    #[gpui::test]
    fn a_foreign_key_cell_draws_two_spans_and_suppresses_the_main_text(cx: &mut TestAppContext) {
        use super::super::model::CellValue;

        let visual = single_cell_harness(cx, CellValue::int(8).with_fk_label("Alice"));

        assert!(
            visual.debug_bounds("table-cell-text").is_none(),
            "the two-tone pair replaces the cell's own display text"
        );
        assert!(visual.debug_bounds("table-cell-label").is_some());
        assert!(visual.debug_bounds("table-cell-secondary").is_some());
    }

    #[gpui::test]
    fn a_table_without_annotations_keeps_its_header_height(cx: &mut TestAppContext) {
        let visual = header_harness(cx, None);

        let header = visual
            .debug_bounds("table-header")
            .expect("the header is drawn");
        assert_eq!(
            header.size.height,
            crate::tokens::GridMetrics::HEADER_HEIGHT
        );
        assert!(visual.debug_bounds("table-header-annotation").is_none());
    }

    #[gpui::test]
    fn annotations_add_a_second_header_line(cx: &mut TestAppContext) {
        let visual = header_harness(
            cx,
            Some(vec![
                None,
                Some(super::super::HeaderAnnotation::new("9.7×  ·  9.8 GiB").trailing("0% null")),
            ]),
        );

        let header = visual
            .debug_bounds("table-header")
            .expect("the header is drawn");
        assert_eq!(
            header.size.height,
            super::super::theme::ANNOTATED_HEADER_HEIGHT
        );
        assert!(
            visual.debug_bounds("table-header-annotation").is_some(),
            "the annotated column draws its facts"
        );
    }

    /// The paint decisions of one cell, computed against the live theme.
    /// Returns the decision, the theme it was computed against and the null
    /// color, so assertions compare against the same values.
    fn paint_of(
        cx: &mut TestAppContext,
        view: &CellView,
        group_child: bool,
        row_pending_delete: bool,
        is_active: bool,
    ) -> (CellPaint, gpui_component::theme::Theme, Hsla) {
        cx.update(crate::theme::init);
        let paint = std::rc::Rc::new(std::cell::RefCell::new(None));
        let paint_for_assert = paint.clone();
        cx.update(move |cx| {
            let theme = cx.theme().clone();
            let null_color = crate::tokens::SyntaxColors::for_current(cx).number;
            paint_for_assert.replace(Some((
                cell_paint(
                    view,
                    group_child,
                    row_pending_delete,
                    is_active,
                    &theme,
                    null_color,
                ),
                theme,
                null_color,
            )));
        });
        paint
            .borrow()
            .clone()
            .expect("the paint decision must be computed")
    }

    /// A transition cell takes the transition color whatever its tone says:
    /// the content outranks the tone in the text color.
    #[gpui::test]
    fn a_transition_cell_takes_the_transition_color_over_its_tone(cx: &mut TestAppContext) {
        let (transitioning, theme, _) = paint_of(
            cx,
            &CellView::transition("old", "new", CellTone::Normal, CellAlignment::Left)
                .with_dirty(true),
            false,
            false,
            false,
        );
        assert_eq!(
            transitioning.text_color, theme.warning,
            "the transition beats the cell's own color"
        );
        assert!(transitioning.transition);
        assert!(
            !transitioning.dirty_mark,
            "a transition never draws the dirty mark"
        );

        // Even a null tone under a transition stays in the transition color.
        let (nulled, theme, null_color) = paint_of(
            cx,
            &CellView::transition("old", "NULL", CellTone::Null, CellAlignment::Left),
            false,
            false,
            false,
        );
        assert_eq!(nulled.text_color, theme.warning);
        assert_ne!(nulled.text_color, null_color);

        // And the same tone without the transition draws normally, which is
        // what makes the precedence observable.
        let (plain_dirty, theme, _) = paint_of(
            cx,
            &CellView::new("new", CellTone::Normal, CellAlignment::Left).with_dirty(true),
            false,
            false,
            false,
        );
        assert_eq!(plain_dirty.text_color, theme.foreground);
    }

    /// A plain staged edit draws the dirty mark; a transition draws the
    /// transition wash instead. The two treatments are mutually exclusive.
    #[gpui::test]
    fn a_plain_dirty_cell_draws_the_mark_and_a_transition_does_not(cx: &mut TestAppContext) {
        let (plain, _, _) = paint_of(
            cx,
            &CellView::new("new", CellTone::Normal, CellAlignment::Left).with_dirty(true),
            false,
            false,
            false,
        );
        assert!(plain.dirty_mark, "the relational dirty mark draws");
        assert!(!plain.transition);

        let (transition, _, _) = paint_of(
            cx,
            &CellView::transition("old", "new", CellTone::Normal, CellAlignment::Left)
                .with_dirty(true),
            false,
            false,
            false,
        );
        assert!(transition.transition, "the transition wash draws");
        assert!(!transition.dirty_mark);
    }

    /// The accepted-defect regression: a relational staged null draws the
    /// null styling (null color, italic) and the amber dirty mark at once.
    /// The two facts are orthogonal — the tone says what the value is, the
    /// dirty flag says it holds an uncommitted edit — so the cell needs
    /// neither sacrificed for the other.
    #[gpui::test]
    fn a_staged_null_edit_draws_the_null_styling_and_the_dirty_mark_together(
        cx: &mut TestAppContext,
    ) {
        use super::super::model::{CellValue, EditBuffer};

        cx.update(crate::theme::init);

        let model = TableModel::new(
            vec![super::super::model::ColumnSpec {
                id: "v".into(),
                title: "v".into(),
                kind: super::super::model::ColumnKind::Text,
                align: gpui::TextAlign::Left,
                type_name: "text".into(),
            }],
            vec![super::super::model::RowData {
                cells: vec![CellValue::text("old")],
            }],
        );
        let mut edits = EditBuffer::new();
        edits.set_base_row_count(model.row_count());
        // SetNull stages `CellValue::null` (`handle_set_null`).
        edits.set_cell(0, 0, CellValue::null());
        let source = GridRowSource::new(&model, &edits);

        let (staged_null, _, null_color) =
            paint_of(cx, &source.cell(0, 0).expect("cell"), false, false, false);
        assert_eq!(
            staged_null.text_color, null_color,
            "the staged null draws the null color"
        );
        assert!(staged_null.italic, "the staged null draws the null italic");
        assert!(
            staged_null.dirty_mark,
            "the staged null still draws the dirty mark"
        );
        assert!(!staged_null.transition);
    }

    /// The pending-delete combinations: the row strikes every cell, the
    /// row mutes the color of its ordinary cells, and a null or muted cell
    /// keeps its own italic on top.
    #[gpui::test]
    fn pending_delete_rows_strike_all_cells_and_mute_the_ordinary_ones(cx: &mut TestAppContext) {
        let alignment = CellAlignment::Left;

        let (struck, theme, _) = paint_of(
            cx,
            &CellView::new("x", CellTone::Normal, alignment),
            false,
            true,
            false,
        );
        assert!(struck.line_through);
        assert!(!struck.italic, "a struck ordinary cell does not italicize");
        assert_eq!(
            struck.text_color, theme.muted_foreground,
            "the row's ordinary cells take the pending-delete color"
        );

        let (struck_null, _, null_color) = paint_of(
            cx,
            &CellView::new("NULL", CellTone::Null, alignment),
            false,
            true,
            false,
        );
        assert!(struck_null.line_through);
        assert!(struck_null.italic, "a null cell keeps its own italic");
        assert_ne!(
            struck_null.text_color, null_color,
            "the row mutes the null color, as the old painter drew it"
        );

        let (struck_muted, theme, _) = paint_of(
            cx,
            &CellView::new("auto", CellTone::Muted, alignment),
            false,
            true,
            false,
        );
        assert!(struck_muted.line_through);
        assert!(struck_muted.italic, "a muted cell keeps its own italic");
        assert_eq!(struck_muted.text_color, theme.muted_foreground);
    }

    /// A group-child cell of a document grid draws the wash; the fact comes
    /// from `RowSource::column`, exactly as the painter reads it.
    #[gpui::test]
    fn a_group_child_cell_draws_the_wash(cx: &mut TestAppContext) {
        use super::super::document::{
            ColumnGroupHeader, DocumentColumnHeader, DocumentPresentation,
        };
        use super::super::model::{CellValue, ColumnKind, ColumnSpec, RowData};
        use gpui::TextAlign;

        cx.update(crate::theme::init);

        let model = TableModel::new(
            vec![
                ColumnSpec {
                    id: "a".into(),
                    title: "a".into(),
                    kind: ColumnKind::Text,
                    align: TextAlign::Left,
                    type_name: "text".into(),
                },
                ColumnSpec {
                    id: "b".into(),
                    title: "b".into(),
                    kind: ColumnKind::Text,
                    align: TextAlign::Left,
                    type_name: "text".into(),
                },
            ],
            vec![RowData {
                cells: vec![CellValue::text("x"), CellValue::text("y")],
            }],
        );
        let document = DocumentPresentation {
            headers: vec![
                DocumentColumnHeader::default(),
                DocumentColumnHeader {
                    group: Some(ColumnGroupHeader {
                        key: "price".into(),
                        label: "price".into(),
                        type_label: "obj".into(),
                    }),
                    presence: None,
                },
            ],
        };
        let edits = super::super::model::EditBuffer::new();
        let source = GridRowSource::with_document(&model, &edits, Some(&document));

        let (washed, _, _) = paint_of(
            cx,
            &source.cell(0, 1).expect("cell"),
            source.column(1).expect("column").group_child,
            false,
            false,
        );
        assert!(
            washed.group_wash,
            "the grouped column's cell draws the wash"
        );

        let (unwashed, _, _) = paint_of(
            cx,
            &source.cell(0, 0).expect("cell"),
            source.column(0).expect("column").group_child,
            false,
            false,
        );
        assert!(!unwashed.group_wash);
    }

    /// The italic and strike decisions: null, muted and missing cells
    /// italicize; a pending-delete row strikes all its cells whatever their
    /// tone, and nothing else strikes.
    #[gpui::test]
    fn null_muted_and_missing_italicize_and_pending_delete_strikes(cx: &mut TestAppContext) {
        let alignment = CellAlignment::Left;
        for (view, expect_italic) in [
            (CellView::new("x", CellTone::Null, alignment), true),
            (CellView::new("x", CellTone::Muted, alignment), true),
            (CellView::new("x", CellTone::Missing, alignment), true),
            (CellView::new("x", CellTone::Normal, alignment), false),
            (CellView::new("x", CellTone::Nested, alignment), false),
            // The edit state does not italicize; a staged text edit draws
            // upright, as the old painter drew it.
            (
                CellView::new("x", CellTone::Normal, alignment).with_dirty(true),
                false,
            ),
        ] {
            let (paint, _, _) = paint_of(cx, &view, false, false, false);
            assert_eq!(
                paint.italic, expect_italic,
                "tone {:?} italic decision",
                view.tone
            );
            assert!(!paint.line_through);
        }
    }

    /// The ordinary tone colors: foreground, the active cell's strong tint,
    /// and the null color.
    #[gpui::test]
    fn ordinary_cells_take_the_foreground_and_the_active_cell_the_strong_tint(
        cx: &mut TestAppContext,
    ) {
        let (normal, theme, _) = paint_of(
            cx,
            &CellView::new("x", CellTone::Normal, CellAlignment::Left),
            false,
            false,
            false,
        );
        assert_eq!(normal.text_color, theme.foreground);

        let (active, theme, _) = paint_of(
            cx,
            &CellView::new("x", CellTone::Normal, CellAlignment::Left),
            false,
            false,
            true,
        );
        assert_eq!(
            active.text_color,
            crate::tokens::ChromeColors::strong(&theme)
        );

        let (null_cell, _, null_color) = paint_of(
            cx,
            &CellView::new("NULL", CellTone::Null, CellAlignment::Left),
            false,
            false,
            true,
        );
        assert_eq!(
            null_cell.text_color, null_color,
            "null outranks the active tint, as the painter draws it today"
        );
    }
}
