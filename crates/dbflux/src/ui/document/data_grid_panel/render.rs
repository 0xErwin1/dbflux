use super::{DataGridPanel, DataSource, EditState, GridFocusMode, GridState, ToolbarFocus};
use crate::ui::components::data_table::SortState as TableSortState;
use crate::ui::document::data_view::DataViewMode;
use crate::ui::document::result_view::ResultViewMode;
use crate::ui::icons::AppIcon;
use crate::ui::toast::ToastExt;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::{Pagination, SortDirection, Value};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::input::{Input, InputState};
use gpui_component::{ActiveTheme, Sizable};

impl Render for DataGridPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Process pending state
        if let Some(pending) = self.pending_total_count.take() {
            self.apply_total_count(pending.source_qualified, pending.total, cx);
        }

        crate::ui::toast::flush_pending_toast(self.pending_toast.take(), window, cx);

        if let Some(requery) = self.pending_requery.take() {
            self.run_table_query(
                requery.profile_id,
                requery.database,
                requery.table,
                requery.pagination,
                requery.order_by,
                requery.total_rows,
                window,
                cx,
            );
        }

        if self.pending_rebuild {
            self.pending_rebuild = false;
            let sort = self
                .local_sort_state
                .map(|s| TableSortState::new(s.column_ix, s.direction));
            self.rebuild_table(sort, cx);
        }

        if self.pending_refresh {
            self.pending_refresh = false;
            self.refresh(window, cx);
        }

        if self.context_menu.is_none() {
            self.pending_context_menu_focus = false;
        } else if self.pending_context_menu_focus {
            self.pending_context_menu_focus = false;
            self.context_menu_focus.focus(window);
        }

        if let Some(modal) = self.pending_modal_open.take() {
            self.cell_editor.update(cx, |editor, cx| {
                editor.open(modal.row, modal.col, modal.value, modal.is_json, window, cx);
            });
        }

        if let Some(preview) = self.pending_document_preview.take() {
            self.document_preview_modal.update(cx, |modal, cx| {
                modal.open(preview.doc_index, preview.document_json, window, cx);
            });
        }

        // Clone theme colors to avoid borrow conflicts with cx
        let theme = cx.theme().clone();

        let row_count = self.result.row_count();
        let exec_time = format!("{}ms", self.result.execution_time.as_millis());

        let is_table_view = self.source.is_table();
        let show_data_toolbar = matches!(
            self.source,
            DataSource::Table { .. } | DataSource::Collection { .. }
        );
        let is_paginated = self.source.is_paginated();
        let source_name = match &self.source {
            DataSource::Table { table, .. } => table.qualified_name(),
            DataSource::Collection { collection, .. } => collection.qualified_name(),
            DataSource::QueryResult { .. } => String::new(),
        };
        let source_query_prefix = if self.source.is_collection() {
            "find"
        } else {
            "SELECT * FROM"
        };
        let filter_input = self.filter_input.clone();
        let filter_has_value = !self.filter_input.read(cx).value().is_empty();
        let limit_input = self.limit_input.clone();

        let pagination_info = self.source.pagination().cloned();
        let total_pages = self.total_pages();
        let can_prev = self.can_go_prev();
        let can_next = self.can_go_next();
        let sort_info = self.current_sort_info();

        let focus_mode = self.focus_mode;
        let toolbar_focus = self.toolbar_focus;
        let edit_state = self.edit_state;
        let show_toolbar_focus =
            focus_mode == GridFocusMode::Toolbar && edit_state == EditState::Navigating;
        let focus_handle = self.focus_handle.clone();

        let has_data = !self.result.rows.is_empty()
            || self.result.text_body.is_some()
            || self.result.raw_bytes.is_some();
        let has_columns = !self.result.columns.is_empty();
        let is_loading = self.state == GridState::Loading;
        let muted_fg = theme.muted_foreground;

        let show_panel_controls = self.show_panel_controls;
        let is_maximized = self.is_maximized;
        let uses_result_view = self.uses_result_view();

        // Get edit state from table
        let (is_editable, has_pending_changes, dirty_count, can_undo, can_redo) = self
            .table_state
            .as_ref()
            .map(|ts| {
                let state = ts.read(cx);
                let buffer = state.edit_buffer();

                // Count all pending operations: edits, inserts, deletes
                let edit_count = buffer.dirty_row_count();
                let insert_count = buffer.pending_insert_rows().len();
                let delete_count = buffer.pending_delete_rows().len();
                let total_count = edit_count + insert_count + delete_count;

                (
                    state.is_editable(),
                    total_count > 0,
                    total_count,
                    buffer.can_undo(),
                    buffer.can_redo(),
                )
            })
            .unwrap_or((false, false, 0, false, false));

        let show_pk_warning = is_table_view && has_data && !is_editable;
        let show_edit_toolbar = is_table_view && has_columns && is_editable;

        div()
            .track_focus(&focus_handle)
            .flex()
            .flex_col()
            .size_full()
            // Track panel origin for context menu positioning
            .child({
                let this_entity = cx.entity().clone();
                canvas(
                    move |bounds, _, cx| {
                        this_entity.update(cx, |this, _cx| {
                            this.panel_origin = bounds.origin;
                        });
                    },
                    |_, _, _, _| {},
                )
                .absolute()
                .size_full()
            })
            // Toolbar (Table / Collection sources)
            .when(show_data_toolbar, |d| {
                d.child(self.render_toolbar(
                    source_query_prefix,
                    &source_name,
                    &filter_input,
                    filter_has_value,
                    &limit_input,
                    show_toolbar_focus,
                    toolbar_focus,
                    &theme,
                    cx,
                ))
            })
            // PK warning banner (when table has no PK)
            .when(show_pk_warning, |d| {
                d.child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .h(Heights::ROW_COMPACT)
                        .px(Spacing::SM)
                        .bg(theme.warning.opacity(0.15))
                        .border_b_1()
                        .border_color(theme.warning.opacity(0.3))
                        .child(
                            svg()
                                .path(AppIcon::TriangleAlert.path())
                                .size_4()
                                .text_color(theme.warning),
                        )
                        .child(
                            div()
                                .text_size(FontSizes::SM)
                                .text_color(theme.warning)
                                .child("This table has no primary key - editing is disabled"),
                        ),
                )
            })
            // Edit toolbar (always visible for editable tables)
            .when(show_edit_toolbar, |d| {
                d.child(self.render_edit_toolbar(
                    dirty_count,
                    has_pending_changes,
                    can_undo,
                    can_redo,
                    &theme,
                    cx,
                ))
            })
            // Header bar with panel controls (only when embedded)
            .when(show_panel_controls && has_data, |d| {
                d.child(
                    div()
                        .flex()
                        .items_center()
                        .justify_end()
                        .h(Heights::ROW_COMPACT)
                        .px(Spacing::SM)
                        .border_b_1()
                        .border_color(theme.border)
                        .child(
                            div()
                                .id("toggle-maximize")
                                .flex()
                                .items_center()
                                .justify_center()
                                .w(px(24.0))
                                .h(px(24.0))
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .hover(|d| d.bg(theme.secondary))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.request_toggle_maximize(cx);
                                }))
                                .child(
                                    svg()
                                        .path(if is_maximized {
                                            AppIcon::Minimize2.path()
                                        } else {
                                            AppIcon::Maximize2.path()
                                        })
                                        .size_4()
                                        .text_color(theme.muted_foreground),
                                ),
                        )
                        .child(
                            div()
                                .id("hide-panel")
                                .flex()
                                .items_center()
                                .justify_center()
                                .w(px(24.0))
                                .h(px(24.0))
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .hover(|d| d.bg(theme.secondary))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.request_hide(cx);
                                }))
                                .child(
                                    svg()
                                        .path(AppIcon::PanelBottomClose.path())
                                        .size_4()
                                        .text_color(theme.muted_foreground),
                                ),
                        ),
                )
            })
            // Grid, Document, or Result View
            .child({
                let view_mode = self.view_config.mode;
                let use_document_view = view_mode == DataViewMode::Document && has_data;
                let result_view_mode = self.result_view_mode;

                div()
                    .flex_1()
                    .overflow_hidden()
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, _, window, cx| {
                            if this.focus_mode != GridFocusMode::Table {
                                this.focus_table(window, cx);
                            }
                        }),
                    )
                    .when(!has_data && !uses_result_view, |d| {
                        d.flex().items_center().justify_center().child(
                            div()
                                .text_size(FontSizes::BASE)
                                .text_color(muted_fg)
                                .child(if is_loading { "Loading..." } else { "No data" }),
                        )
                    })
                    .when(uses_result_view, |d| {
                        d.child(self.render_result_view(result_view_mode, &theme, cx))
                    })
                    .when(!uses_result_view && has_data && use_document_view, |d| {
                        d.child(self.render_document_view(&theme, cx))
                    })
                    .when(!uses_result_view && has_data && !use_document_view, |d| {
                        d.when_some(self.data_table.clone(), |d, data_table| d.child(data_table))
                    })
            })
            // Status bar
            .child(self.render_status_bar(
                row_count,
                &exec_time,
                is_paginated,
                pagination_info,
                total_pages,
                can_prev,
                can_next,
                sort_info,
                has_data,
                uses_result_view,
                &theme,
                cx,
            ))
            // Context menu overlay
            .when_some(self.context_menu.as_ref(), |d, menu| {
                d.child(self.render_context_menu(menu, is_editable, &theme, cx))
            })
            // Delete confirmation modal
            .when(self.pending_delete_confirm.is_some(), |d| {
                d.child(self.render_delete_confirm_modal(&theme, cx))
            })
            // Cell editor modal overlay
            .when(self.cell_editor.read(cx).is_visible(), |d| {
                d.child(self.cell_editor.clone())
            })
            // Document preview modal overlay
            .when(self.document_preview_modal.read(cx).is_visible(), |d| {
                d.child(self.document_preview_modal.clone())
            })
    }
}

impl DataGridPanel {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn render_toolbar(
        &self,
        source_query_prefix: &str,
        source_name: &str,
        filter_input: &Entity<InputState>,
        filter_has_value: bool,
        limit_input: &Entity<InputState>,
        show_toolbar_focus: bool,
        toolbar_focus: ToolbarFocus,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let refresh_label = if self.refresh_policy.is_auto() {
            self.refresh_policy.label()
        } else {
            "Refresh"
        };

        div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .h(Heights::TOOLBAR)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.secondary)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .text_color(theme.muted_foreground)
                            .child(source_query_prefix.to_string()),
                    )
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(theme.foreground)
                            .child(source_name.to_string()),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .text_color(theme.muted_foreground)
                            .child("WHERE"),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .w(px(280.0))
                            .rounded(Radii::SM)
                            .when(
                                show_toolbar_focus && toolbar_focus == ToolbarFocus::Filter,
                                |d| d.border_1().border_color(theme.ring),
                            )
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, _, cx| {
                                    this.switching_input = true;
                                    this.focus_mode = GridFocusMode::Toolbar;
                                    this.toolbar_focus = ToolbarFocus::Filter;
                                    this.edit_state = EditState::Editing;
                                    cx.notify();
                                }),
                            )
                            .child(div().flex_1().child(Input::new(filter_input).small()))
                            .when(filter_has_value, |d| {
                                d.child(
                                    div()
                                        .id("clear-filter")
                                        .w(px(20.0))
                                        .h(px(20.0))
                                        .mr(Spacing::XS)
                                        .flex()
                                        .items_center()
                                        .justify_center()
                                        .rounded(Radii::SM)
                                        .text_size(FontSizes::SM)
                                        .text_color(theme.muted_foreground)
                                        .cursor_pointer()
                                        .hover(|d| {
                                            d.bg(theme.secondary).text_color(theme.foreground)
                                        })
                                        .on_click(cx.listener(|this, _, window, cx| {
                                            this.filter_input.update(cx, |input, cx| {
                                                input.set_value("", window, cx);
                                            });
                                            this.refresh(window, cx);
                                        }))
                                        .child("×"),
                                )
                            }),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .text_color(theme.muted_foreground)
                            .child("LIMIT"),
                    )
                    .child(
                        div()
                            .w(px(60.0))
                            .rounded(Radii::SM)
                            .when(
                                show_toolbar_focus && toolbar_focus == ToolbarFocus::Limit,
                                |d| d.border_1().border_color(theme.ring),
                            )
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, _, cx| {
                                    this.switching_input = true;
                                    this.focus_mode = GridFocusMode::Toolbar;
                                    this.toolbar_focus = ToolbarFocus::Limit;
                                    this.edit_state = EditState::Editing;
                                    cx.notify();
                                }),
                            )
                            .child(Input::new(limit_input).small()),
                    ),
            )
            .child(
                div()
                    .id("refresh-control")
                    .h(Heights::BUTTON)
                    .flex()
                    .items_center()
                    .gap_0()
                    .rounded(Radii::SM)
                    .bg(theme.background)
                    .border_1()
                    .border_color(
                        if show_toolbar_focus && toolbar_focus == ToolbarFocus::Refresh {
                            theme.ring
                        } else {
                            theme.input
                        },
                    )
                    .child(
                        div()
                            .id("refresh-action")
                            .h_full()
                            .px(Spacing::SM)
                            .flex()
                            .items_center()
                            .gap_1()
                            .text_size(FontSizes::SM)
                            .text_color(theme.foreground)
                            .cursor_pointer()
                            .hover(|d| d.bg(theme.accent.opacity(0.08)))
                            .on_click(cx.listener(|this, _, window, cx| {
                                if this.runner.is_primary_active() {
                                    this.runner.cancel_primary(cx);
                                    cx.notify();
                                } else {
                                    this.refresh(window, cx);
                                    this.focus_table(window, cx);
                                }
                            }))
                            .child(
                                svg()
                                    .path(if self.runner.is_primary_active() {
                                        AppIcon::Loader.path()
                                    } else if self.refresh_policy.is_auto() {
                                        AppIcon::Clock.path()
                                    } else {
                                        AppIcon::RefreshCcw.path()
                                    })
                                    .size_4()
                                    .text_color(theme.foreground),
                            )
                            .child(refresh_label),
                    )
                    .child(div().w(px(1.0)).h_full().bg(theme.input))
                    .child(
                        div()
                            .w(px(28.0))
                            .h_full()
                            .child(self.refresh_dropdown.clone()),
                    ),
            )
            .when(self.can_toggle_view(), |d| {
                let mode = self.view_config.mode;
                let icon_path = match mode {
                    DataViewMode::Table => AppIcon::Table.path(),
                    DataViewMode::Document => AppIcon::Braces.path(),
                };
                let _tooltip = match mode {
                    DataViewMode::Table => "Switch to Document View",
                    DataViewMode::Document => "Switch to Table View",
                };

                d.child(
                    div()
                        .id("view-toggle-btn")
                        .h_full()
                        .px(Spacing::SM)
                        .flex()
                        .items_center()
                        .gap(Spacing::XS)
                        .rounded(Radii::SM)
                        .text_color(theme.muted_foreground)
                        .cursor_pointer()
                        .hover(|d| d.bg(theme.secondary).text_color(theme.foreground))
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.toggle_view_mode(cx);
                        }))
                        .child(
                            svg()
                                .path(icon_path)
                                .size_4()
                                .text_color(theme.muted_foreground),
                        )
                        .child(
                            div()
                                .text_size(FontSizes::XS)
                                .text_color(theme.muted_foreground)
                                .child(mode.label()),
                        ),
                )
            })
    }

    pub(super) fn render_edit_toolbar(
        &self,
        dirty_count: usize,
        has_changes: bool,
        can_undo: bool,
        can_redo: bool,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .justify_between()
            .h(px(44.0))
            .px(Spacing::MD)
            .border_b_1()
            .border_color(theme.border)
            // Left: status text
            .child(
                div()
                    .text_size(FontSizes::SM)
                    .text_color(if has_changes {
                        theme.warning
                    } else {
                        theme.muted_foreground
                    })
                    .child(if has_changes {
                        format!(
                            "{} unsaved change{}",
                            dirty_count,
                            if dirty_count == 1 { "" } else { "s" }
                        )
                    } else {
                        "No unsaved changes".to_string()
                    }),
            )
            // Right: buttons
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    // Undo button
                    .child(
                        div()
                            .id("undo-btn")
                            .flex()
                            .items_center()
                            .justify_center()
                            .size(px(28.0))
                            .rounded(Radii::MD)
                            .border_1()
                            .when(can_undo, |d| {
                                d.border_color(theme.border)
                                    .cursor_pointer()
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        if let Some(table_state) = &this.table_state {
                                            table_state.update(cx, |state, cx| {
                                                if state.is_editing() {
                                                    state.stop_editing(false, cx);
                                                }
                                                if state.edit_buffer_mut().undo() {
                                                    let visual_count = state
                                                        .edit_buffer()
                                                        .compute_visual_order()
                                                        .len();
                                                    if let Some(active) = state.selection().active
                                                        && active.row >= visual_count
                                                    {
                                                        state.clear_selection(cx);
                                                    }
                                                    cx.notify();
                                                }
                                            });
                                        }
                                        window.focus(&this.focus_handle);
                                    }))
                            })
                            .when(!can_undo, |d| {
                                d.border_color(theme.border)
                                    .text_color(theme.muted_foreground)
                            })
                            .child(svg().path(AppIcon::Undo.path()).size_4().text_color(
                                if can_undo {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            )),
                    )
                    // Redo button
                    .child(
                        div()
                            .id("redo-btn")
                            .flex()
                            .items_center()
                            .justify_center()
                            .size(px(28.0))
                            .rounded(Radii::MD)
                            .border_1()
                            .when(can_redo, |d| {
                                d.border_color(theme.border)
                                    .cursor_pointer()
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        if let Some(table_state) = &this.table_state {
                                            table_state.update(cx, |state, cx| {
                                                if state.is_editing() {
                                                    state.stop_editing(false, cx);
                                                }
                                                if state.edit_buffer_mut().redo() {
                                                    let visual_count = state
                                                        .edit_buffer()
                                                        .compute_visual_order()
                                                        .len();
                                                    if let Some(active) = state.selection().active
                                                        && active.row >= visual_count
                                                    {
                                                        state.clear_selection(cx);
                                                    }
                                                    cx.notify();
                                                }
                                            });
                                        }
                                        window.focus(&this.focus_handle);
                                    }))
                            })
                            .when(!can_redo, |d| d.border_color(theme.border))
                            .child(svg().path(AppIcon::Redo.path()).size_4().text_color(
                                if can_redo {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            )),
                    )
                    // Save button
                    .child(
                        div()
                            .id("save-btn")
                            .flex()
                            .items_center()
                            .gap_1()
                            .px(Spacing::MD)
                            .h(px(28.0))
                            .rounded(Radii::MD)
                            .text_size(FontSizes::SM)
                            .border_1()
                            .when(has_changes, |d| {
                                d.border_color(theme.primary)
                                    .bg(theme.primary)
                                    .text_color(theme.primary_foreground)
                                    .cursor_pointer()
                                    .hover(|d| d.opacity(0.9))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        if let Some(table_state) = &this.table_state {
                                            table_state.update(cx, |state, cx| {
                                                state.request_save_row(cx);
                                            });
                                        }
                                        // Refocus table after button click
                                        window.focus(&this.focus_handle);
                                    }))
                            })
                            .when(!has_changes, |d| {
                                d.border_color(theme.border)
                                    .text_color(theme.muted_foreground)
                            })
                            .child("Save")
                            .child(
                                div()
                                    .text_size(FontSizes::XS)
                                    .text_color(if has_changes {
                                        theme.primary_foreground.opacity(0.7)
                                    } else {
                                        theme.muted_foreground.opacity(0.5)
                                    })
                                    .child("Ctrl+↵"),
                            ),
                    )
                    // Revert button
                    .child(
                        div()
                            .id("revert-btn")
                            .flex()
                            .items_center()
                            .px(Spacing::MD)
                            .h(px(28.0))
                            .rounded(Radii::MD)
                            .text_size(FontSizes::SM)
                            .border_1()
                            .when(has_changes, |d| {
                                d.border_color(theme.border)
                                    .text_color(theme.foreground)
                                    .cursor_pointer()
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        if let Some(table_state) = &this.table_state {
                                            table_state.update(cx, |state, cx| {
                                                state.revert_all(cx);
                                            });
                                        }
                                        // Refocus table after button click
                                        window.focus(&this.focus_handle);
                                    }))
                            })
                            .when(!has_changes, |d| {
                                d.border_color(theme.border)
                                    .text_color(theme.muted_foreground)
                            })
                            .child("Revert"),
                    ),
            )
    }

    pub(super) fn render_document_view(
        &self,
        _theme: &gpui_component::theme::Theme,
        _cx: &mut Context<Self>,
    ) -> impl IntoElement {
        if let Some(tree) = &self.document_tree {
            div()
                .id("document-view-container")
                .size_full()
                .child(tree.clone())
        } else {
            let rows = &self.result.rows;
            let columns = &self.result.columns;

            let cards: Vec<_> = rows
                .iter()
                .enumerate()
                .map(|(row_idx, row)| self.render_document_card(row_idx, row, columns, _theme))
                .collect();

            div()
                .id("document-view-container")
                .flex()
                .flex_col()
                .size_full()
                .p(Spacing::MD)
                .gap(Spacing::MD)
                .children(cards)
        }
    }

    pub(super) fn render_document_card(
        &self,
        row_idx: usize,
        row: &[Value],
        columns: &[dbflux_core::ColumnMeta],
        theme: &gpui_component::theme::Theme,
    ) -> impl IntoElement {
        div()
            .id(ElementId::Name(format!("doc-{}", row_idx).into()))
            .flex()
            .flex_col()
            .w_full()
            .p(Spacing::MD)
            .rounded(Radii::MD)
            .border_1()
            .border_color(theme.border)
            .bg(theme.secondary)
            .gap(Spacing::XS)
            .children(
                columns
                    .iter()
                    .zip(row.iter())
                    .filter(|(_, val)| !matches!(val, Value::Null))
                    .map(|(col, val)| self.render_document_field(&col.name, val, theme, 0)),
            )
    }

    pub(super) fn render_document_field(
        &self,
        name: &str,
        value: &Value,
        theme: &gpui_component::theme::Theme,
        depth: usize,
    ) -> impl IntoElement {
        let indent = px(depth as f32 * 16.0);

        div()
            .flex()
            .pl(indent)
            .gap(Spacing::SM)
            .child(
                div()
                    .text_size(FontSizes::SM)
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(theme.muted_foreground)
                    .child(format!("{}:", name)),
            )
            .child(self.render_value(value, theme, depth))
    }

    pub(super) fn render_value(
        &self,
        value: &Value,
        theme: &gpui_component::theme::Theme,
        depth: usize,
    ) -> impl IntoElement {
        let text_color = match value {
            Value::Null => theme.muted_foreground,
            Value::Bool(_) => theme.chart_1,
            Value::Int(_) | Value::Float(_) => theme.chart_2,
            Value::Text(_) => theme.chart_3,
            Value::ObjectId(_) => theme.chart_4,
            _ => theme.foreground,
        };

        match value {
            Value::Null => div()
                .text_size(FontSizes::SM)
                .text_color(text_color)
                .child("null"),

            Value::Bool(b) => div()
                .text_size(FontSizes::SM)
                .text_color(text_color)
                .child(if *b { "true" } else { "false" }),

            Value::Int(i) => div()
                .text_size(FontSizes::SM)
                .text_color(text_color)
                .child(i.to_string()),

            Value::Float(f) => div()
                .text_size(FontSizes::SM)
                .text_color(text_color)
                .child(f.to_string()),

            Value::Text(s) => {
                let display: String = s.replace('\n', "\\n").replace('\r', "\\r");
                div()
                    .text_size(FontSizes::SM)
                    .text_color(text_color)
                    .child(format!("\"{}\"", display))
            }

            Value::ObjectId(oid) => div()
                .text_size(FontSizes::SM)
                .text_color(text_color)
                .child(format!("ObjectId(\"{}\")", oid)),

            Value::DateTime(dt) => div()
                .text_size(FontSizes::SM)
                .text_color(text_color)
                .child(dt.to_rfc3339()),

            Value::Array(arr) => {
                if arr.is_empty() {
                    div()
                        .text_size(FontSizes::SM)
                        .text_color(theme.muted_foreground)
                        .child("[]")
                } else if arr.len() <= 3 && depth < 2 {
                    div()
                        .flex()
                        .gap(Spacing::XS)
                        .child(
                            div()
                                .text_size(FontSizes::SM)
                                .text_color(theme.muted_foreground)
                                .child("["),
                        )
                        .children(arr.iter().enumerate().map(|(i, v)| {
                            div()
                                .flex()
                                .child(self.render_value(v, theme, depth + 1))
                                .when(i < arr.len() - 1, |d| {
                                    d.child(
                                        div()
                                            .text_size(FontSizes::SM)
                                            .text_color(theme.muted_foreground)
                                            .child(","),
                                    )
                                })
                        }))
                        .child(
                            div()
                                .text_size(FontSizes::SM)
                                .text_color(theme.muted_foreground)
                                .child("]"),
                        )
                } else {
                    div()
                        .text_size(FontSizes::SM)
                        .text_color(theme.muted_foreground)
                        .child(format!("[{} items]", arr.len()))
                }
            }

            Value::Document(doc) => {
                if doc.is_empty() {
                    div()
                        .text_size(FontSizes::SM)
                        .text_color(theme.muted_foreground)
                        .child("{}")
                } else if depth < 2 {
                    div().flex().flex_col().pl(Spacing::MD).children(
                        doc.iter()
                            .map(|(k, v)| self.render_document_field(k, v, theme, depth + 1)),
                    )
                } else {
                    div()
                        .text_size(FontSizes::SM)
                        .text_color(theme.muted_foreground)
                        .child(format!("{{{} fields}}", doc.len()))
                }
            }

            _ => {
                let display = format!("{:?}", value).replace('\n', "\\n").replace('\r', "\\r");
                div()
                    .text_size(FontSizes::SM)
                    .text_color(theme.foreground)
                    .child(display)
            }
        }
    }

    // -- Result View Renderers --

    pub(super) fn render_result_view(
        &mut self,
        mode: ResultViewMode,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let mut container = div().size_full();

        match mode {
            ResultViewMode::Table => {
                container = container.when_some(self.data_table.clone(), |d, dt| d.child(dt));
            }
            ResultViewMode::Text => {
                let text = self.derived_text().to_string();
                container = container.child(self.render_text_view(&text, theme));
            }
            ResultViewMode::Json => {
                let json = self.derived_json().to_string();
                container = container.child(self.render_json_view(&json, theme));
            }
            ResultViewMode::Raw => {
                let bytes = self.result.raw_bytes.clone();
                let text_body = self.result.text_body.clone();
                container = container.child(self.render_raw_view(
                    bytes.as_deref(),
                    text_body.as_deref(),
                    theme,
                    cx,
                ));
            }
        }

        container
    }

    fn render_text_view(
        &self,
        text: &str,
        theme: &gpui_component::theme::Theme,
    ) -> impl IntoElement {
        self.render_line_based_view("result-text-view", text, theme)
    }

    fn render_json_view(
        &self,
        json: &str,
        theme: &gpui_component::theme::Theme,
    ) -> impl IntoElement {
        self.render_line_based_view("result-json-view", json, theme)
    }

    fn render_line_based_view(
        &self,
        id: &'static str,
        content: &str,
        theme: &gpui_component::theme::Theme,
    ) -> impl IntoElement {
        const MAX_LINES: usize = 5000;

        let fg = theme.foreground;

        let line_count = content.lines().count();
        let truncated = line_count > MAX_LINES;

        let display_text: SharedString = if truncated {
            let capped: String = content
                .lines()
                .take(MAX_LINES)
                .collect::<Vec<_>>()
                .join("\n");
            SharedString::from(capped)
        } else {
            SharedString::from(content.to_string())
        };

        div()
            .id(id)
            .size_full()
            .p(Spacing::MD)
            .overflow_y_scroll()
            .overflow_x_scroll()
            .bg(theme.background)
            .child(
                div()
                    .font_family("monospace")
                    .text_size(FontSizes::SM)
                    .text_color(fg)
                    .whitespace_nowrap()
                    .child(display_text),
            )
            .when(truncated, |d| {
                d.child(
                    div()
                        .px(Spacing::MD)
                        .pb(Spacing::SM)
                        .text_size(FontSizes::XS)
                        .text_color(theme.muted_foreground)
                        .child(format!("(truncated at {} lines)", MAX_LINES)),
                )
            })
    }

    fn render_raw_view(
        &self,
        raw_bytes: Option<&[u8]>,
        text_body: Option<&str>,
        theme: &gpui_component::theme::Theme,
        _cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let hex_dump = if let Some(bytes) = raw_bytes {
            format_hex_dump(bytes)
        } else if let Some(text) = text_body {
            text.to_string()
        } else {
            "(empty)".to_string()
        };

        div()
            .id("result-raw-view")
            .size_full()
            .p(Spacing::MD)
            .overflow_y_scroll()
            .bg(theme.background)
            .child(
                div()
                    .font_family("monospace")
                    .text_size(FontSizes::SM)
                    .text_color(theme.foreground)
                    .whitespace_nowrap()
                    .child(hex_dump),
            )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn render_status_bar(
        &self,
        row_count: usize,
        exec_time: &str,
        is_paginated: bool,
        pagination_info: Option<Pagination>,
        total_pages: Option<u64>,
        can_prev: bool,
        can_next: bool,
        sort_info: Option<(String, SortDirection, bool)>,
        has_data: bool,
        uses_result_view: bool,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let result_shape_label = if uses_result_view {
            Some(self.result.shape.clone())
        } else {
            None
        };

        let available_modes = if uses_result_view {
            ResultViewMode::available_for_shape(&self.result.shape)
        } else {
            vec![]
        };
        let current_result_mode = self.result_view_mode;

        div()
            .flex()
            .items_center()
            .justify_between()
            .h(Heights::ROW_COMPACT)
            .px(Spacing::SM)
            .border_t_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            // Left: row count / shape info and sort info
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    // Result view mode selector (for non-Table shapes)
                    .when(available_modes.len() > 1, |d| {
                        d.child(div().flex().items_center().gap_0().children(
                            available_modes.iter().enumerate().map(|(i, mode)| {
                                let mode = *mode;
                                let is_active = mode == current_result_mode;
                                div()
                                    .id(ElementId::Name(format!("result-view-{}", i).into()))
                                    .px(Spacing::SM)
                                    .text_size(FontSizes::XS)
                                    .cursor_pointer()
                                    .rounded(Radii::SM)
                                    .when(is_active, |d| {
                                        d.bg(theme.accent.opacity(0.15))
                                            .text_color(theme.foreground)
                                            .font_weight(FontWeight::MEDIUM)
                                    })
                                    .when(!is_active, |d| {
                                        d.text_color(theme.muted_foreground).hover(|d| {
                                            d.bg(theme.secondary).text_color(theme.foreground)
                                        })
                                    })
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.set_result_view_mode(mode, cx);
                                    }))
                                    .child(mode.label())
                            }),
                        ))
                    })
                    // Shape badge
                    .when_some(result_shape_label, |d, shape| {
                        let label = match &shape {
                            dbflux_core::QueryResultShape::Table => "table",
                            dbflux_core::QueryResultShape::Json => "json",
                            dbflux_core::QueryResultShape::Text => "text",
                            dbflux_core::QueryResultShape::Binary => "binary",
                        };
                        d.child(
                            div()
                                .text_size(FontSizes::XS)
                                .text_color(theme.muted_foreground)
                                .child(label.to_string()),
                        )
                    })
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground)
                            .child(
                                svg()
                                    .path(AppIcon::Rows3.path())
                                    .size_3()
                                    .text_color(theme.muted_foreground),
                            )
                            .child(format!("{} rows", row_count)),
                    )
                    .when_some(sort_info, |d, (col_name, direction, is_server)| {
                        let arrow_icon = match direction {
                            SortDirection::Ascending => AppIcon::ArrowUp,
                            SortDirection::Descending => AppIcon::ArrowDown,
                        };
                        let mode = if is_server { "db" } else { "local" };
                        d.child(
                            div()
                                .flex()
                                .items_center()
                                .gap_1()
                                .text_size(FontSizes::XS)
                                .text_color(theme.muted_foreground)
                                .child(
                                    svg()
                                        .path(arrow_icon.path())
                                        .size_3()
                                        .text_color(theme.muted_foreground),
                                )
                                .child(format!("{} ({})", col_name, mode)),
                        )
                    }),
            )
            // Center: pagination (for Table and Collection sources)
            .child(div().flex().items_center().gap(Spacing::SM).when_some(
                pagination_info.clone().filter(|_| is_paginated),
                |d, pagination| {
                    let page = pagination.current_page();
                    let offset = pagination.offset();
                    let start = offset + 1;
                    let end = offset + row_count as u64;

                    d.child(
                        div()
                            .id("prev-page")
                            .flex()
                            .items_center()
                            .gap_1()
                            .px(Spacing::XS)
                            .rounded(Radii::SM)
                            .text_size(FontSizes::XS)
                            .when(can_prev, |d| {
                                d.cursor_pointer()
                                    .text_color(theme.foreground)
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.go_to_prev_page(window, cx);
                                    }))
                            })
                            .when(!can_prev, |d| {
                                d.text_color(theme.muted_foreground).opacity(0.5)
                            })
                            .child(svg().path(AppIcon::ChevronLeft.path()).size_3().text_color(
                                if can_prev {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            ))
                            .child("Prev"),
                    )
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground)
                            .child(if let Some(total) = total_pages {
                                format!("Page {}/{} ({}-{})", page, total, start, end)
                            } else {
                                format!("Page {} ({}-{})", page, start, end)
                            }),
                    )
                    .child(
                        div()
                            .id("next-page")
                            .flex()
                            .items_center()
                            .gap_1()
                            .px(Spacing::XS)
                            .rounded(Radii::SM)
                            .text_size(FontSizes::XS)
                            .when(can_next, |d| {
                                d.cursor_pointer()
                                    .text_color(theme.foreground)
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.go_to_next_page(window, cx);
                                    }))
                            })
                            .when(!can_next, |d| {
                                d.text_color(theme.muted_foreground).opacity(0.5)
                            })
                            .child("Next")
                            .child(
                                svg()
                                    .path(AppIcon::ChevronRight.path())
                                    .size_3()
                                    .text_color(if can_next {
                                        theme.foreground
                                    } else {
                                        theme.muted_foreground
                                    }),
                            ),
                    )
                },
            ))
            // Right: export and execution time
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .when(has_data, |d| d.child(self.render_export_button(theme, cx)))
                    .child({
                        let mut muted = theme.muted_foreground;
                        muted.a = 0.5;
                        div()
                            .text_size(FontSizes::XS)
                            .text_color(muted)
                            .child(exec_time.to_string())
                    }),
            )
    }

    fn render_export_button(
        &self,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let formats = dbflux_export::available_formats(&self.result.shape);
        let menu_open = self.export_menu_open;

        let label = if formats.len() == 1 {
            format!("Export {}", formats[0].name())
        } else {
            "Export".to_string()
        };

        div()
            .id("export-trigger")
            .relative()
            .flex()
            .items_center()
            .gap_1()
            .px(Spacing::XS)
            .rounded(Radii::SM)
            .text_size(FontSizes::XS)
            .cursor_pointer()
            .text_color(theme.muted_foreground)
            .hover(|d| d.bg(theme.secondary).text_color(theme.foreground))
            .on_click(cx.listener(|this, _, window, cx| {
                this.export_results(window, cx);
            }))
            .child(
                svg()
                    .path(AppIcon::FileSpreadsheet.path())
                    .size_4()
                    .text_color(theme.muted_foreground),
            )
            .child(label)
            .when(formats.len() > 1, |d| {
                d.child(
                    svg()
                        .path(AppIcon::ChevronDown.path())
                        .size_3()
                        .text_color(theme.muted_foreground),
                )
            })
            .when(menu_open, |d| {
                d.child(self.render_export_menu(formats, theme, cx))
            })
    }

    fn render_export_menu(
        &self,
        formats: &[dbflux_export::ExportFormat],
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let items: Vec<AnyElement> = formats
            .iter()
            .enumerate()
            .map(|(idx, &format)| {
                div()
                    .id(SharedString::from(format!("export-{}", idx)))
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .h(Heights::ROW_COMPACT)
                    .px(Spacing::SM)
                    .mx(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .text_size(FontSizes::SM)
                    .text_color(theme.foreground)
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.export_with_format(format, window, cx);
                    }))
                    .child(format.name())
                    .into_any_element()
            })
            .collect();

        deferred(
            div()
                .absolute()
                .bottom_full()
                .right_0()
                .mb(Spacing::XS)
                .w(px(160.0))
                .bg(theme.popover)
                .border_1()
                .border_color(theme.border)
                .rounded(Radii::MD)
                .shadow_lg()
                .py(Spacing::XS)
                .occlude()
                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                    cx.stop_propagation();
                })
                .on_mouse_down_out(cx.listener(|this, _, _, cx| {
                    this.export_menu_open = false;
                    cx.notify();
                }))
                .children(items),
        )
        .with_priority(1)
    }
}

fn format_hex_dump(data: &[u8]) -> String {
    const BYTES_PER_LINE: usize = 16;

    let mut lines = Vec::new();

    for (offset, chunk) in data.chunks(BYTES_PER_LINE).enumerate() {
        let hex_part: String = chunk
            .iter()
            .enumerate()
            .map(|(i, b)| {
                if i == 8 {
                    format!("  {:02x}", b)
                } else {
                    format!(" {:02x}", b)
                }
            })
            .collect();

        let padding = if chunk.len() < BYTES_PER_LINE {
            let missing = BYTES_PER_LINE - chunk.len();
            let extra_gap = if chunk.len() <= 8 { 1 } else { 0 };
            " ".repeat(missing * 3 + extra_gap)
        } else {
            String::new()
        };

        let ascii_part: String = chunk
            .iter()
            .map(|b| {
                if b.is_ascii_graphic() || *b == b' ' {
                    *b as char
                } else {
                    '.'
                }
            })
            .collect();

        lines.push(format!(
            "{:08x} {}{}  |{}|",
            offset * BYTES_PER_LINE,
            hex_part,
            padding,
            ascii_part
        ));
    }

    if lines.is_empty() {
        "(empty)".to_string()
    } else {
        lines.join("\n")
    }
}
