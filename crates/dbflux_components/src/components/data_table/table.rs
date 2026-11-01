use std::ops::Range;
use std::sync::{Arc, Mutex};

use crate::controls::GpuiInput as Input;
use crate::fonts;
use crate::icons::AppIcon;
use crate::primitives::Icon;
use crate::tokens::{ChromeColors, CollectionMetrics, GridMetrics, Spacing};
use gpui::ElementId;
use gpui::prelude::FluentBuilder;
use gpui::{
    Action, App, ClickEvent, Context, Entity, FontWeight, InteractiveElement, IntoElement,
    Keystroke, ListSizingBehavior, MouseButton, MouseDownEvent, ParentElement, ScrollWheelEvent,
    SharedString, StatefulInteractiveElement, Styled, Window, canvas, div, px, uniform_list,
};
use gpui_component::scroll::{Scrollbar, ScrollbarMode};
use gpui_component::{ActiveTheme, Sizable};

use super::document::DocumentPresentation;
use super::events::{ContextMenuAction, DataTableEvent, Direction, Edge};
use super::selection::CellCoord;
use super::source::GridRowSource;
use super::state::DataTableState;
use super::theme::{CELL_PADDING_X, ROW_NUMBER_WIDTH, SCROLLBAR_WIDTH};
// The row list and cell painter moved to `bolt::components::grid`; the old
// in-file names are the re-exported Bolt API.
use bolt::components::grid::{GridCellCoord, GridHost, render_rows};
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
                let selection = state.selection();

                let source = GridRowSource::with_document(
                    &model,
                    state.edit_buffer(),
                    state.document_presentation(),
                );

                // The host state the Bolt row renderer reads: the grid's
                // selection and editing facts, its interaction policy and
                // its icons, all as plain values the renderer cannot
                // misread. The selection test stays a borrow; the click
                // callbacks are Arcs because gpui handlers are 'static.
                let is_selected = |coord: GridCellCoord| {
                    selection.is_selected(CellCoord::new(coord.row, coord.col))
                };
                let host = GridHost {
                    column_widths: state.column_widths(),
                    active: selection.active.map(|c| GridCellCoord::new(c.row, c.col)),
                    editing: state
                        .editing_cell()
                        .map(|c| GridCellCoord::new(c.row, c.col)),
                    is_selected: &is_selected,
                    reference_row: state.has_reference_row(),
                    on_cell_click: {
                        let state = state_entity.clone();
                        Arc::new(
                            move |coord: GridCellCoord,
                                  event: &ClickEvent,
                                  window: &mut Window,
                                  cx: &mut App| {
                                let coord = CellCoord::new(coord.row, coord.col);
                                state.update(cx, |state, cx| {
                                    state.focus(window, cx);
                                });

                                if event.click_count() == 2 {
                                    state.update(cx, |state, cx| {
                                        state.start_editing(coord, window, cx);
                                    });
                                    return;
                                }

                                let modifiers = event.modifiers();
                                state.update(cx, |state, cx| {
                                    state.click_cell(coord, modifiers, cx);
                                });
                            },
                        )
                    },
                    on_cell_context: {
                        let state = state_entity.clone();
                        Arc::new(
                            move |coord: GridCellCoord,
                                  event: &MouseDownEvent,
                                  window: &mut Window,
                                  cx: &mut App| {
                                let coord = CellCoord::new(coord.row, coord.col);
                                cx.stop_propagation();
                                state.update(cx, |state, cx| {
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
                    },
                    braces_icon: AppIcon::Braces.into(),
                    chevron_icon: AppIcon::ChevronRight.into(),
                    reference_icon: AppIcon::Crosshair.into(),
                };
                // The inline editor the editing cell draws: a dropdown when
                // the cell enumerates values, the text input otherwise.
                let editor = state
                    .enum_dropdown()
                    .map(|dropdown| dropdown.clone().into_any_element())
                    .or_else(|| {
                        state
                            .cell_input()
                            .map(|input| Input::new(input).small().into_any_element())
                    });

                render_rows(
                    visible_range,
                    &source,
                    &host,
                    editor,
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
}
