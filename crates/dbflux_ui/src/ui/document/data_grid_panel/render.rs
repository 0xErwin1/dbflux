use super::{DataGridPanel, DataSource, EditState, GridFocusMode, GridState, ToolbarFocus};
use crate::ui::components::data_table::SortState as TableSortState;
use crate::ui::components::toast::{Toast, copy_action, now_hms};
use crate::ui::document::data_view::DataViewMode;
use crate::ui::document::result_view::ResultViewMode;
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_components::chart::{
    CHART_ACCENT_CYAN, CHART_ACCENT_PRIMARY, ChartDetection, ManualChartSelection, SeriesSpec,
    SeriesStats, count_columns_for_why, format_resolution, format_span, format_x_value,
    format_y_value,
};
use dbflux_components::chart::legend::legend_element;
use dbflux_components::controls::{Checkbox, Input, InputState};
use dbflux_components::primitives::{BannerBlock, BannerVariant, Icon, Text, surface_raised};
use dbflux_core::{
    ColumnKind, DatabaseCategory, Pagination, QueryResultShape, SortDirection, Value,
};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

// Save-row shortcut hint: matches the SaveRow binding in the data-table
// component (`secondary-enter` — Cmd+Enter on macOS, Ctrl+Enter elsewhere).
#[cfg(target_os = "macos")]
const SAVE_ROW_SHORTCUT_HINT: &str = "Cmd+↵";
#[cfg(not(target_os = "macos"))]
const SAVE_ROW_SHORTCUT_HINT: &str = "Ctrl+↵";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DataGridContentMode {
    EmptyFallback,
    ResultView,
    Document,
    Table,
}

fn content_mode_for_result(
    uses_result_view: bool,
    view_mode: DataViewMode,
    has_columns: bool,
    has_data: bool,
) -> DataGridContentMode {
    if uses_result_view {
        DataGridContentMode::ResultView
    } else if view_mode == DataViewMode::Document && has_data {
        DataGridContentMode::Document
    } else if view_mode != DataViewMode::Document && has_columns {
        DataGridContentMode::Table
    } else {
        DataGridContentMode::EmptyFallback
    }
}

impl Render for DataGridPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Process pending state
        if let Some(pending) = self.pending_total_count.take() {
            self.apply_total_count(pending.source_qualified, pending.total, cx);
        }

        crate::ui::components::toast::flush_pending_toast(self.pending_toast.take(), window, cx);

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
        let (source_query_prefix, filter_keyword) =
            DataGridPanel::filter_labels_for_source(&self.source, &self.app_state, cx);
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
        let view_mode = self.view_config.mode;

        let show_panel_controls = self.show_panel_controls;
        let is_maximized = self.is_maximized;
        let uses_result_view = self.uses_result_view();
        let content_mode =
            content_mode_for_result(uses_result_view, view_mode, has_columns, has_data);
        let shows_table_content = matches!(content_mode, DataGridContentMode::Table);
        let shows_content_controls = has_data || shows_table_content;

        // Show result-tabs strip (Table | Chart) when the result shape is Table
        // and chart auto-detection succeeded or the driver is TimeSeries.
        let is_time_series_source =
            DataGridPanel::connection_category(&self.source, &self.app_state, cx)
                == Some(DatabaseCategory::TimeSeries);
        let show_chart_tabs_strip = self.result.shape == QueryResultShape::Table
            && (self.chart_available() || is_time_series_source)
            && (shows_table_content || uses_result_view);

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

        let show_pk_warning = is_table_view && shows_table_content && !is_editable;
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
                    filter_keyword,
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
                        .child(Icon::new(AppIcon::TriangleAlert).small().warning())
                        .child(
                            Text::caption("This table has no primary key - editing is disabled")
                                .warning(),
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
            .when(show_panel_controls && shows_content_controls, |d| {
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
                                    Icon::new(if is_maximized {
                                        AppIcon::Minimize2
                                    } else {
                                        AppIcon::Maximize2
                                    })
                                    .small()
                                    .color(theme.muted_foreground),
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
                                    Icon::new(AppIcon::PanelBottomClose)
                                        .small()
                                        .color(theme.muted_foreground),
                                ),
                        ),
                )
            })
            // Grid, Document, or Result View
            .child({
                let result_view_mode = self.result_view_mode;
                let row_count_for_strip = row_count;

                div()
                    .flex_1()
                    .flex()
                    .flex_col()
                    .overflow_hidden()
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, _, window, cx| {
                            if this.focus_mode != GridFocusMode::Table {
                                this.focus_table(window, cx);
                            }
                        }),
                    )
                    // Result-tabs strip (Table | Chart) at the top of the content area.
                    .when(show_chart_tabs_strip, |d| {
                        d.child(
                            self.render_result_tabs_strip(row_count_for_strip, &theme, cx),
                        )
                    })
                    // Content body — fills remaining space below the strip.
                    .child({
                        let content = div().flex_1().overflow_hidden();

                        let content = content.when(
                            matches!(content_mode, DataGridContentMode::EmptyFallback),
                            |d| {
                                d.flex()
                                    .items_center()
                                    .justify_center()
                                    .child(if is_loading {
                                        div()
                                            .flex()
                                            .items_center()
                                            .gap(Spacing::SM)
                                            .child(
                                                Icon::new(AppIcon::Loader)
                                                    .size(px(12.0))
                                                    .color(theme.muted_foreground),
                                            )
                                            .child(Text::muted("Loading…"))
                                            .into_any_element()
                                    } else {
                                        Text::muted("No data").into_any_element()
                                    })
                            },
                        );

                        let content = content.when(
                            matches!(content_mode, DataGridContentMode::ResultView),
                            |d| d.child(self.render_result_view(result_view_mode, &theme, cx)),
                        );

                        let content = content.when(
                            matches!(content_mode, DataGridContentMode::Document),
                            |d| d.child(self.render_document_view(&theme, cx)),
                        );

                        content.when(
                            matches!(content_mode, DataGridContentMode::Table),
                            |d| {
                                d.when_some(self.data_table.clone(), |d, data_table| {
                                    d.child(data_table)
                                })
                            },
                        )
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
                dirty_count,
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
        filter_keyword: &str,
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
            .flex_wrap()
            .items_center()
            .gap(Spacing::SM)
            .min_h(Heights::TOOLBAR)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.secondary)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .child(Text::caption(source_query_prefix.to_string()).primary())
                    .child(Text::label(source_name.to_string())),
            )
            // Filter input: hidden for drivers that don't support collection filtering
            // (e.g. TimeSeries/InfluxDB where browse_collection ignores the filter).
            .when(!filter_keyword.is_empty(), |d| {
                d.child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::XS)
                        .child(Text::caption(filter_keyword.to_string()).primary())
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .w(px(420.0))
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
                                            .child("\u{00d7}"),
                                    )
                                }),
                        ),
                )
            })
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .child(Text::caption("LIMIT").primary())
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
                                Icon::new(if self.runner.is_primary_active() {
                                    AppIcon::Loader
                                } else if self.refresh_policy.is_auto() {
                                    AppIcon::Clock
                                } else {
                                    AppIcon::RefreshCcw
                                })
                                .small()
                                .color(theme.foreground),
                            )
                            .child(Text::body(refresh_label)),
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
                let view_icon: AppIcon = match mode {
                    DataViewMode::Table => AppIcon::Table,
                    DataViewMode::Document => AppIcon::Braces,
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
                        .child(Icon::new(view_icon).small().color(theme.muted_foreground))
                        .child(Text::muted(mode.label())),
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
                Text::caption(if has_changes {
                    format!(
                        "{} unsaved change{}",
                        dirty_count,
                        if dirty_count == 1 { "" } else { "s" }
                    )
                } else {
                    "No unsaved changes".to_string()
                })
                .color(if has_changes {
                    theme.warning
                } else {
                    theme.muted_foreground
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
                            .when(!can_undo, |d| d.border_color(theme.border))
                            .child(Icon::new(AppIcon::Undo).small().color(if can_undo {
                                theme.foreground
                            } else {
                                theme.muted_foreground
                            })),
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
                            .child(Icon::new(AppIcon::Redo).small().color(if can_redo {
                                theme.foreground
                            } else {
                                theme.muted_foreground
                            })),
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
                            .border_1()
                            .when(has_changes, |d| {
                                d.border_color(theme.primary)
                                    .bg(theme.primary)
                                    .cursor_pointer()
                                    .hover(|d| d.opacity(0.9))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        if let Some(table_state) = &this.table_state {
                                            table_state.update(cx, |state, cx| {
                                                state.request_save_all(cx);
                                            });
                                        }
                                        // Refocus table after button click
                                        window.focus(&this.focus_handle);
                                    }))
                            })
                            .when(!has_changes, |d| d.border_color(theme.border))
                            .child(Text::caption("Save").color(if has_changes {
                                theme.primary_foreground
                            } else {
                                theme.muted_foreground
                            }))
                            .child(Text::caption(SAVE_ROW_SHORTCUT_HINT).color(if has_changes {
                                theme.primary_foreground.opacity(0.7)
                            } else {
                                theme.muted_foreground.opacity(0.5)
                            })),
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
                            .border_1()
                            .border_color(theme.border)
                            .when(has_changes, |d| {
                                d.cursor_pointer()
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
                            .child(Text::caption("Revert").color(if has_changes {
                                theme.foreground
                            } else {
                                theme.muted_foreground
                            })),
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
            .child(Text::label_sm(format!("{}:", name)).muted_foreground())
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
            Value::Null => Text::caption("null").color(text_color).into_any_element(),

            Value::Bool(b) => Text::caption(if *b { "true" } else { "false" })
                .color(text_color)
                .into_any_element(),

            Value::Int(i) => Text::caption(i.to_string())
                .color(text_color)
                .into_any_element(),

            Value::Float(f) => Text::caption(f.to_string())
                .color(text_color)
                .into_any_element(),

            Value::Text(s) => {
                let display: String = s.replace('\n', "\\n").replace('\r', "\\r");
                Text::caption(format!("\"{}\"", display))
                    .color(text_color)
                    .into_any_element()
            }

            Value::ObjectId(oid) => Text::caption(format!("ObjectId(\"{}\")", oid))
                .color(text_color)
                .into_any_element(),

            Value::DateTime(dt) => Text::caption(dt.to_rfc3339())
                .color(text_color)
                .into_any_element(),

            Value::Array(arr) => {
                if arr.is_empty() {
                    Text::caption("[]").into_any_element()
                } else if arr.len() <= 3 && depth < 2 {
                    div()
                        .flex()
                        .gap(Spacing::XS)
                        .child(Text::caption("["))
                        .children(arr.iter().enumerate().map(|(i, v)| {
                            div()
                                .flex()
                                .child(self.render_value(v, theme, depth + 1))
                                .when(i < arr.len() - 1, |d| d.child(Text::caption(",")))
                        }))
                        .child(Text::caption("]"))
                        .into_any_element()
                } else {
                    Text::caption(format!("[{} items]", arr.len())).into_any_element()
                }
            }

            Value::Document(doc) => {
                if doc.is_empty() {
                    Text::caption("{}").into_any_element()
                } else if depth < 2 {
                    div()
                        .flex()
                        .flex_col()
                        .pl(Spacing::MD)
                        .children(
                            doc.iter()
                                .map(|(k, v)| self.render_document_field(k, v, theme, depth + 1)),
                        )
                        .into_any_element()
                } else {
                    Text::caption(format!("{{{} fields}}", doc.len())).into_any_element()
                }
            }

            _ => {
                let display = format!("{:?}", value)
                    .replace('\n', "\\n")
                    .replace('\r', "\\r");
                Text::body(display).into_any_element()
            }
        }
    }

    // -- Result View Renderers --

    /// Render the Table / Chart tab strip that appears above the result content
    /// area when the result is Table-shaped and chart detection succeeded (or the
    /// driver is TimeSeries).
    ///
    /// Each tab switches `result_view_mode` on click. The active tab is underlined
    /// with the theme accent and shown in full foreground weight.
    pub(super) fn render_result_tabs_strip(
        &self,
        row_count: usize,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let current_mode = self.result_view_mode;

        let tabs: &[(ResultViewMode, &str)] = &[
            (ResultViewMode::Table, "Data"),
            (ResultViewMode::Chart, "Chart"),
        ];

        div()
            .flex()
            .flex_row()
            .items_center()
            .h(px(30.0))
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .children(tabs.iter().map(|(mode, label)| {
                let mode = *mode;
                let is_active = mode == current_mode;
                let label_text: SharedString = (*label).into();

                div()
                    .id(ElementId::Name(
                        format!("result-tab-{}", label.to_lowercase()).into(),
                    ))
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .h_full()
                    .px(Spacing::SM)
                    .cursor_pointer()
                    .border_b_2()
                    .border_color(if is_active {
                        theme.accent
                    } else {
                        gpui::transparent_black()
                    })
                    .text_color(if is_active {
                        theme.foreground
                    } else {
                        theme.muted_foreground
                    })
                    .when(!is_active, |d| d.hover(|d| d.bg(theme.secondary)))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, _, cx| {
                            this.set_result_view_mode(mode, cx);
                        }),
                    )
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .when(is_active, |d| {
                                d.font_weight(gpui::FontWeight::SEMIBOLD)
                            })
                            .child(label_text),
                    )
                    // Row-count badge on the Data tab only.
                    .when(mode == ResultViewMode::Table, |d| {
                        d.child(
                            div()
                                .text_size(px(10.0))
                                .text_color(theme.muted_foreground)
                                .font(font("JetBrains Mono"))
                                .child(SharedString::from(format!("{}", row_count))),
                        )
                    })
            }))
    }

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
            ResultViewMode::Chart => {
                // Chart mode: vertical stack — chart+rail row, then legend row below.
                let rail_open = self.chart_rail_open;

                // Build chart_view on first render before checking whether it exists.
                let _ = self.ensure_chart_view(cx);
                let has_chart_view = self.chart_view.is_some();

                let chart_area = if let Some(chart_entity) = self.chart_view.clone() {
                    div()
                        .flex_grow()
                        .size_full()
                        .child(chart_entity)
                        .into_any_element()
                } else {
                    div()
                        .flex_grow()
                        .size_full()
                        .child(self.render_chart_degraded(cx))
                        .into_any_element()
                };

                let row = div()
                    .flex()
                    .flex_row()
                    .flex_grow()
                    .child(chart_area)
                    .when(rail_open, |d| d.child(self.render_chart_rail(theme, cx)));

                let col = div()
                    .flex()
                    .flex_col()
                    .size_full()
                    .child(row)
                    // Legend row — always shown when a chart view exists.
                    .when(has_chart_view, |d| {
                        d.child(self.render_chart_legend_row(theme, cx))
                    });

                container = container.child(col);
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

    /// Render the legend row below the chart canvas.
    ///
    /// Reads series specs, palette colours, and stats from the `ChartView` entity,
    /// then delegates to `legend_element` with a toggle callback that calls
    /// `toggle_chart_series_hidden` on this panel.
    pub(super) fn render_chart_legend_row(
        &mut self,
        _theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let Some(chart_entity) = self.chart_view.clone() else {
            return div().into_any_element();
        };

        let cv = chart_entity.read(cx);
        let series = cv.spec_series().to_vec();
        let palette = cv.palette_colors().to_vec();
        let stats = cv.series_stats().to_vec();
        let focused_idx = cv.focused_series_idx();

        let hidden = self.chart_hidden_series.clone();
        let panel_entity = cx.entity().clone();

        let on_toggle = move |idx: usize, _window: &mut Window, cx: &mut App| {
            panel_entity.update(cx, |this, cx| {
                this.toggle_chart_series_hidden(idx, cx);
            });
        };

        legend_element(&series, &palette, &stats, &hidden, focused_idx, Some(on_toggle))
            .into_any_element()
    }

    /// Render the degraded-state chart panel when `ensure_chart_view` returned `None`.
    ///
    /// Shows a detection-specific banner. For `NoTimeColumn` and `NoNumericSeries`
    /// a manual column picker is also shown so the user can override detection.
    fn render_chart_degraded(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let detection = self.chart_detection.clone();

        // Y-candidate columns: Float, Integer, or Unknown.
        // X-candidate columns: Timestamp, Text, or Unknown.
        let y_candidates: Vec<(usize, String)> = self
            .result
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                matches!(
                    c.kind,
                    ColumnKind::Float | ColumnKind::Integer | ColumnKind::Unknown
                )
            })
            .map(|(i, c)| (i, c.name.clone()))
            .collect();

        let x_candidates: Vec<(usize, String)> = self
            .result
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                matches!(
                    c.kind,
                    ColumnKind::Timestamp | ColumnKind::Text | ColumnKind::Unknown
                )
            })
            .map(|(i, c)| (i, c.name.clone()))
            .collect();

        let (banner_text, show_picker) = match &detection {
            Some(ChartDetection::NoTimeColumn) | None => (
                "No time column detected — pick one below to chart this result.",
                true,
            ),
            Some(ChartDetection::NoNumericSeries) => (
                "No numeric series detected — this result has no chartable values.",
                false,
            ),
            Some(ChartDetection::EmptyResult) => (
                "No data to chart yet — run the query to populate this view.",
                false,
            ),
            Some(ChartDetection::Ok { .. }) => {
                // ensure_chart_view handles Ok; this path means build() failed.
                ("Chart could not be built from this result.", false)
            }
        };

        // Current picker selection state (indices into x_candidates / y_candidates).
        let selected_x_col = self.chart_picker_x_col;
        let y_checked = self.chart_picker_y_checked.clone();

        // Find the index into x_candidates that matches selected_x_col, for display.
        let x_selected_candidate_idx = x_candidates
            .iter()
            .position(|(col_idx, _)| *col_idx == selected_x_col)
            .unwrap_or(0);

        let any_y_checked = y_checked.iter().any(|&c| c);

        let mut outer = div()
            .size_full()
            .flex()
            .flex_col()
            .items_center()
            .justify_start()
            .p(Spacing::LG)
            .gap(Spacing::MD);

        outer = outer.child(BannerBlock::new(BannerVariant::Info, banner_text));

        if show_picker && !x_candidates.is_empty() {
            // X-axis column selector: clickable row of candidate column names.
            let x_row =
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .child(Text::body("X axis (time / label column)")),
                    )
                    .child(div().flex().flex_wrap().gap(Spacing::XS).children(
                        x_candidates.iter().enumerate().map(
                            |(candidate_idx, (col_idx, col_name))| {
                                let col_idx = *col_idx;
                                let is_selected = candidate_idx == x_selected_candidate_idx;
                                let label = col_name.clone();
                                div()
                                    .id(ElementId::Name(format!("chart-x-col-{}", col_idx).into()))
                                    .px(Spacing::SM)
                                    .py(Spacing::XS)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .text_size(FontSizes::SM)
                                    .when(is_selected, |d| d.bg(gpui::hsla(0.6, 0.7, 0.55, 0.2)))
                                    .when(!is_selected, |d| {
                                        d.hover(|d| d.bg(gpui::hsla(0.0, 0.0, 0.5, 0.1)))
                                    })
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(move |this, _, _, cx| {
                                            this.chart_picker_x_col = col_idx;
                                            cx.notify();
                                        }),
                                    )
                                    .child(label)
                            },
                        ),
                    ));

            outer = outer.child(x_row);

            // Y-axis column checkboxes.
            if !y_candidates.is_empty() {
                let y_row =
                    div()
                        .flex()
                        .flex_col()
                        .gap(Spacing::XS)
                        .child(
                            div()
                                .text_size(FontSizes::SM)
                                .child(Text::body("Y axis (numeric columns)")),
                        )
                        .child(div().flex().flex_col().gap(Spacing::XS).children(
                            y_candidates.iter().enumerate().map(
                                |(candidate_idx, (_, col_name))| {
                                    let checked =
                                        y_checked.get(candidate_idx).copied().unwrap_or(false);
                                    let label = col_name.clone();
                                    Checkbox::new(ElementId::Name(
                                        format!("chart-y-col-{}", candidate_idx).into(),
                                    ))
                                    .checked(checked)
                                    .label(label)
                                    .on_click(cx.listener(
                                        move |this, &new_checked, _, cx| {
                                            if let Some(slot) =
                                                this.chart_picker_y_checked.get_mut(candidate_idx)
                                            {
                                                *slot = new_checked;
                                            }
                                            cx.notify();
                                        },
                                    ))
                                },
                            ),
                        ));

                outer = outer.child(y_row);
            }

            // Apply button — enabled only when at least one Y column is checked.
            let x_col_snapshot = selected_x_col;
            let y_col_indices: Vec<usize> = y_candidates
                .iter()
                .enumerate()
                .filter_map(|(candidate_idx, (col_idx, _))| {
                    if y_checked.get(candidate_idx).copied().unwrap_or(false) {
                        Some(*col_idx)
                    } else {
                        None
                    }
                })
                .collect();

            let apply_btn = div()
                .id("chart-picker-apply")
                .px(Spacing::MD)
                .py(Spacing::SM)
                .rounded(Radii::SM)
                .text_size(FontSizes::SM)
                .when(any_y_checked, |d| {
                    d.cursor_pointer()
                        .bg(gpui::hsla(0.6, 0.7, 0.55, 1.0))
                        .text_color(gpui::white())
                        .hover(|d| d.bg(gpui::hsla(0.6, 0.7, 0.50, 1.0)))
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(move |this, _, _, cx| {
                                let selection = ManualChartSelection {
                                    x_col: x_col_snapshot,
                                    y_cols: y_col_indices.clone(),
                                };
                                this.chart_manual_selection = Some(selection);
                                this.chart_view = None;
                                this.chart_view_observer = None;
                                cx.notify();
                            }),
                        )
                })
                .when(!any_y_checked, |d| {
                    d.bg(gpui::hsla(0.0, 0.0, 0.5, 0.3))
                        .text_color(gpui::hsla(0.0, 0.0, 0.5, 0.7))
                })
                .child("Apply");

            outer = outer.child(apply_btn);
        }

        outer
    }

    /// Render the 280px Configure rail shown when `chart_rail_open` is true.
    ///
    /// Contains two tabs — Configure (column picker + Apply/Reset) and Stats
    /// (focused-series descriptive statistics + window summary).
    fn render_chart_rail(
        &mut self,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        use super::ChartRailTab;

        let active_tab = self.chart_rail_tab;

        // Tab header
        let tab_header = div()
            .flex()
            .flex_row()
            .gap_1()
            .px_2()
            .pt_2()
            .pb_1()
            .border_b_1()
            .border_color(theme.border)
            .child({
                let is_active = active_tab == ChartRailTab::Configure;
                div()
                    .id("rail-tab-configure")
                    .px(Spacing::SM)
                    .py(gpui::px(2.0))
                    .rounded(Radii::SM)
                    .text_size(FontSizes::XS)
                    .cursor_pointer()
                    .when(is_active, |d| d.bg(gpui::hsla(0.0, 0.0, 0.15, 1.0)))
                    .when(!is_active, |d| {
                        d.border_1()
                            .border_color(theme.border)
                            .hover(|d| d.bg(theme.secondary))
                    })
                    .on_mouse_down(
                        gpui::MouseButton::Left,
                        cx.listener(|this, _, _, cx| {
                            this.chart_rail_tab = ChartRailTab::Configure;
                            cx.notify();
                        }),
                    )
                    .child("Configure")
            })
            .child({
                let is_active = active_tab == ChartRailTab::Stats;
                div()
                    .id("rail-tab-stats")
                    .px(Spacing::SM)
                    .py(gpui::px(2.0))
                    .rounded(Radii::SM)
                    .text_size(FontSizes::XS)
                    .cursor_pointer()
                    .when(is_active, |d| d.bg(gpui::hsla(0.0, 0.0, 0.15, 1.0)))
                    .when(!is_active, |d| {
                        d.border_1()
                            .border_color(theme.border)
                            .hover(|d| d.bg(theme.secondary))
                    })
                    .on_mouse_down(
                        gpui::MouseButton::Left,
                        cx.listener(|this, _, _, cx| {
                            this.chart_rail_tab = ChartRailTab::Stats;
                            cx.notify();
                        }),
                    )
                    .child("Stats")
            });

        let body = match active_tab {
            ChartRailTab::Configure => self.render_rail_configure_tab(theme, cx).into_any_element(),
            ChartRailTab::Stats => self.render_rail_stats_tab(theme, cx).into_any_element(),
        };

        div()
            .w(gpui::px(280.0))
            .flex_shrink_0()
            .flex()
            .flex_col()
            .border_l_1()
            .border_color(theme.border)
            .bg(theme.background)
            .child(tab_header)
            .child(div().flex_grow().child(body))
    }

    /// Configure tab body: X-column selector, Y-column checkboxes with
    /// inline avg/last, and Reset-to-auto + Apply footer buttons.
    fn render_rail_configure_tab(
        &mut self,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        use dbflux_core::ColumnKind;

        let columns = &self.result.columns;

        let x_candidates: Vec<(usize, String)> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                matches!(
                    c.kind,
                    ColumnKind::Timestamp | ColumnKind::Text | ColumnKind::Unknown
                )
            })
            .map(|(i, c)| (i, c.name.clone()))
            .collect();

        let y_candidates: Vec<(usize, String)> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                matches!(
                    c.kind,
                    ColumnKind::Float | ColumnKind::Integer | ColumnKind::Unknown
                )
            })
            .map(|(i, c)| (i, c.name.clone()))
            .collect();

        let selected_x = self.chart_rail_picker_x_col;
        let y_checked = self.chart_rail_picker_y_checked.clone();
        let any_y_checked = y_checked.iter().any(|&c| c);

        // Build inline avg/last labels for Y candidates currently in the spec.
        let stats: Vec<Option<SeriesStats>> = if let Some(cv) = &self.chart_view {
            cv.read(cx).series_stats().to_vec()
        } else {
            vec![]
        };

        // Which column indices are currently charted (in order of series)?
        let active_y_cols: Vec<usize> = if let Some(manual) = &self.chart_manual_selection {
            manual.y_cols.clone()
        } else if let Some(ChartDetection::Ok { numeric_cols, .. }) = &self.chart_detection {
            numeric_cols.clone()
        } else {
            vec![]
        };

        let detection_ok = matches!(&self.chart_detection, Some(ChartDetection::Ok { .. }));
        let has_manual = self.chart_manual_selection.is_some();
        let reset_enabled = detection_ok || has_manual;

        div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .p_2()
            // X-column section
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .child(Text::caption("Time column".to_string()).color(theme.muted_foreground))
                    .children(x_candidates.iter().enumerate().map(
                        |(cand_idx, (col_idx, col_name))| {
                            let col_idx = *col_idx;
                            let is_selected = cand_idx == selected_x;
                            let label = col_name.clone();
                            div()
                                .id(ElementId::Name(format!("rail-x-col-{}", col_idx).into()))
                                .px(Spacing::SM)
                                .py(gpui::px(2.0))
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .text_size(FontSizes::XS)
                                .when(is_selected, |d| d.bg(gpui::hsla(0.6, 0.7, 0.55, 0.2)))
                                .when(!is_selected, |d| d.hover(|d| d.bg(theme.secondary)))
                                .on_mouse_down(
                                    gpui::MouseButton::Left,
                                    cx.listener(move |this, _, _, cx| {
                                        this.chart_rail_picker_x_col = cand_idx;
                                        cx.notify();
                                    }),
                                )
                                .child(label)
                        },
                    )),
            )
            // Y-column section
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .child(Text::caption("Y columns".to_string()).color(theme.muted_foreground))
                    .children(y_candidates.iter().enumerate().map(
                        |(cand_idx, (col_idx, col_name))| {
                            let col_idx = *col_idx;
                            let checked = y_checked.get(cand_idx).copied().unwrap_or(false);
                            let label = col_name.clone();

                            // Find this column's series index in active_y_cols.
                            let series_idx_opt = active_y_cols.iter().position(|&ci| ci == col_idx);
                            let stat_label = series_idx_opt
                                .and_then(|si| stats.get(si).copied().flatten())
                                .map(|s| {
                                    format!(
                                        "avg {} · last {}",
                                        format_y_value(s.avg),
                                        format_y_value(s.last)
                                    )
                                })
                                .unwrap_or_else(|| "—".to_string());

                            div()
                                .flex()
                                .flex_col()
                                .gap(gpui::px(1.0))
                                .child(
                                    Checkbox::new(ElementId::Name(
                                        format!("rail-y-col-{}", cand_idx).into(),
                                    ))
                                    .checked(checked)
                                    .label(label)
                                    .on_click(cx.listener(
                                        move |this, &new_checked, _, cx| {
                                            if let Some(slot) =
                                                this.chart_rail_picker_y_checked.get_mut(cand_idx)
                                            {
                                                *slot = new_checked;
                                            }
                                            cx.notify();
                                        },
                                    )),
                                )
                                .child(
                                    div()
                                        .pl(gpui::px(20.0))
                                        .text_size(FontSizes::XS)
                                        .text_color(theme.muted_foreground)
                                        .child(stat_label),
                                )
                        },
                    )),
            )
            // Footer: Reset + Apply
            .child(
                div()
                    .flex()
                    .flex_row()
                    .justify_between()
                    .pt_1()
                    .border_t_1()
                    .border_color(theme.border)
                    // Reset-to-auto button
                    .child(
                        div()
                            .id("rail-reset-btn")
                            .px(Spacing::SM)
                            .py(gpui::px(3.0))
                            .rounded(Radii::SM)
                            .text_size(FontSizes::XS)
                            .when(reset_enabled, |d| {
                                d.cursor_pointer()
                                    .text_color(theme.foreground)
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_mouse_down(
                                        gpui::MouseButton::Left,
                                        cx.listener(|this, _, _, cx| {
                                            this.reset_chart_rail_to_auto(cx);
                                        }),
                                    )
                            })
                            .when(!reset_enabled, |d| {
                                d.text_color(theme.muted_foreground).opacity(0.4)
                            })
                            .child("Reset"),
                    )
                    // Apply button
                    .child(
                        div()
                            .id("rail-apply-btn")
                            .px(Spacing::SM)
                            .py(gpui::px(3.0))
                            .rounded(Radii::SM)
                            .text_size(FontSizes::XS)
                            .when(any_y_checked, |d| {
                                d.cursor_pointer()
                                    .bg(gpui::hsla(0.6, 0.7, 0.55, 1.0))
                                    .text_color(gpui::white())
                                    .hover(|d| d.bg(gpui::hsla(0.6, 0.7, 0.50, 1.0)))
                                    .on_mouse_down(
                                        gpui::MouseButton::Left,
                                        cx.listener(|this, _, _, cx| {
                                            this.apply_chart_rail_selection(cx);
                                        }),
                                    )
                            })
                            .when(!any_y_checked, |d| {
                                d.bg(gpui::hsla(0.0, 0.0, 0.5, 0.3))
                                    .text_color(gpui::hsla(0.0, 0.0, 0.5, 0.7))
                            })
                            .child("Apply"),
                    ),
            )
    }

    /// Stats tab body: focused-series descriptive statistics and window summary.
    fn render_rail_stats_tab(
        &mut self,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        // Read focus from the live ChartView so hover-driven focus changes
        // (which only mutate the chart entity's state) update the Stats tab
        // on the next render. Falling back to the panel's cached index when
        // the chart entity is not yet built keeps the Reset/rebuild path
        // working without flicker.
        let focused_idx = self
            .chart_view
            .as_ref()
            .map(|cv| cv.read(cx).focused_series_idx())
            .unwrap_or(self.chart_focused_series_idx);

        let (stats_opt, label, color, x_min, x_max, x_is_time) = if let Some(cv) = &self.chart_view
        {
            let view = cv.read(cx);
            let s = view.series_stats().get(focused_idx).copied().flatten();
            let label = view.series_label(focused_idx).to_string();
            let color = view.series_color(focused_idx);
            let (x_min, x_max) = view.data_x_bounds();
            let x_is_time = view.x_is_time();
            (s, label, color, x_min, x_max, x_is_time)
        } else {
            // Rail may briefly be open while chart_view is None (e.g. during
            // rebuild after Apply). Render an empty state.
            return div()
                .p_2()
                .text_size(FontSizes::XS)
                .text_color(theme.muted_foreground)
                .child("Rebuilding chart…")
                .into_any_element();
        };

        let Some(stats) = stats_opt else {
            return div()
                .p_2()
                .text_size(FontSizes::XS)
                .text_color(theme.muted_foreground)
                .child("No stats available for this series.")
                .into_any_element();
        };

        // Compute window span.
        let span_ms = x_max - x_min;
        let start_label = format_x_value(x_min, x_is_time);
        let end_label = format_x_value(x_max, x_is_time);
        let span_label = format_span(span_ms);
        let points_count = self.result.rows.len();

        let stat_row = |name: &str, value: String| -> gpui::AnyElement {
            div()
                .flex()
                .flex_row()
                .justify_between()
                .py(gpui::px(1.0))
                .text_size(FontSizes::XS)
                .child(
                    div()
                        .text_color(theme.muted_foreground)
                        .child(SharedString::from(name.to_string())),
                )
                .child(
                    div()
                        .text_color(theme.foreground)
                        .child(SharedString::from(value)),
                )
                .into_any_element()
        };

        div()
            .flex()
            .flex_col()
            .gap_1()
            .p_2()
            // Series header: swatch + label
            .child(
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .gap_2()
                    .pb_1()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(div().w(gpui::px(8.0)).h(gpui::px(8.0)).bg(color))
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .font_weight(gpui::FontWeight::BOLD)
                            .child(SharedString::from(label)),
                    ),
            )
            // Stats rows
            .child(stat_row("min", format_y_value(stats.min)))
            .child(stat_row("max", format_y_value(stats.max)))
            .child(stat_row("avg", format_y_value(stats.avg)))
            .child(stat_row("p50", format_y_value(stats.p50)))
            .child(stat_row("p95", format_y_value(stats.p95)))
            .child(stat_row("p99", format_y_value(stats.p99)))
            .child(stat_row("last", format_y_value(stats.last)))
            // Window section
            .child(
                div()
                    .pt_1()
                    .mt_1()
                    .border_t_1()
                    .border_color(theme.border)
                    .flex()
                    .flex_col()
                    .gap(gpui::px(1.0))
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground)
                            .pb(gpui::px(1.0))
                            .child("Window"),
                    )
                    .child(stat_row("start", start_label))
                    .child(stat_row("end", end_label))
                    .child(stat_row("span", span_label))
                    .child(stat_row("points", format!("{}", points_count))),
            )
            .into_any_element()
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
            .child(div().whitespace_nowrap().child(Text::code(display_text)))
            .when(truncated, |d| {
                d.child(Text::caption(format!("(truncated at {} lines)", MAX_LINES)))
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
            .child(div().whitespace_nowrap().child(Text::code(hex_dump)))
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
        pending_change_count: usize,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let result_shape_label = if uses_result_view {
            Some(self.result.shape.clone())
        } else {
            None
        };

        // Chart toggling for Table-shaped results is now handled by the
        // result-tabs strip rendered at the top of the content area (T12).
        // The status-bar pill row must NOT include Chart for Table-shaped results
        // to avoid duplicating the control.
        //
        // For non-Table shapes (Json / Text / Binary), the pill row remains the
        // only mode selector and Chart is never eligible there.
        // Chart toggling for Table-shaped results is now handled by the
        // result-tabs strip rendered at the top of the content area (T12).
        // The status-bar pills never include Chart — for non-Table shapes,
        // `available_for_shape` never returns Chart anyway.
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
            // Left: pending-change count (when applicable), row count / shape info, sort info
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    // Pending-change count — visible only when there are unsaved edits
                    .when(pending_change_count > 0, |d| {
                        d.child(
                            Text::caption(format!(
                                "{} pending change{}",
                                pending_change_count,
                                if pending_change_count == 1 { "" } else { "s" }
                            ))
                            .color(theme.warning),
                        )
                    })
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
                                    .when(is_active, |d| d.bg(theme.accent.opacity(0.15)))
                                    .when(!is_active, |d| d.hover(|d| d.bg(theme.secondary)))
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.set_result_view_mode(mode, cx);
                                    }))
                                    .child(Self::result_mode_label(mode.label(), is_active))
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
                        d.child(Text::caption(label.to_string()))
                    })
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .child(
                                Icon::new(AppIcon::Rows3)
                                    .size(px(12.0))
                                    .color(theme.muted_foreground),
                            )
                            .child(Text::caption(format!("{} rows", row_count))),
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
                                .child(
                                    Icon::new(arrow_icon)
                                        .size(px(12.0))
                                        .color(theme.muted_foreground),
                                )
                                .child(Text::caption(format!("{} ({})", col_name, mode))),
                        )
                    }),
            )
            // Center: pagination (for Table and Collection sources).
            // Layout: ‹  N / Total  › using Unicode single-chevrons.
            .child(div().flex().items_center().gap(Spacing::XS).when_some(
                pagination_info.clone().filter(|_| is_paginated),
                |d, pagination| {
                    let page = pagination.current_page();

                    let page_label = if let Some(total) = total_pages {
                        format!("{} / {}", page, total)
                    } else {
                        format!("{}", page)
                    };

                    d.child(
                        div()
                            .id("prev-page")
                            .flex()
                            .items_center()
                            .justify_center()
                            .w(px(20.0))
                            .h(px(20.0))
                            .rounded(Radii::SM)
                            .text_size(FontSizes::SM)
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
                            .child("\u{2039}"),
                    )
                    .child(
                        Text::caption(page_label)
                            .font_size(FontSizes::XS)
                            .color(theme.muted_foreground),
                    )
                    .child(
                        div()
                            .id("next-page")
                            .flex()
                            .items_center()
                            .justify_center()
                            .w(px(20.0))
                            .h(px(20.0))
                            .rounded(Radii::SM)
                            .text_size(FontSizes::SM)
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
                            .child("\u{203a}"),
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
                        Text::caption(exec_time.to_string()).color(muted)
                    }),
            )
    }

    fn result_mode_label(label: &'static str, is_active: bool) -> Text {
        if is_active {
            Text::label_sm(label).font_size(FontSizes::XS)
        } else {
            Text::caption(label).font_size(FontSizes::XS)
        }
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
            .hover(|d| d.bg(theme.secondary))
            .on_click(cx.listener(|this, _, window, cx| {
                this.export_results(window, cx);
            }))
            .child(
                Icon::new(AppIcon::FileSpreadsheet)
                    .small()
                    .color(theme.muted_foreground),
            )
            .child(Text::caption(label).muted_foreground())
            .when(formats.len() > 1, |d| {
                d.child(
                    Icon::new(AppIcon::ChevronDown)
                        .size(px(12.0))
                        .color(theme.muted_foreground),
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
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.export_with_format(format, window, cx);
                    }))
                    .child(Text::body(format.name()))
                    .into_any_element()
            })
            .collect();

        deferred(
            surface_raised(cx)
                .absolute()
                .bottom_full()
                .right_0()
                .mb(Spacing::XS)
                .w(px(160.0))
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

#[cfg(test)]
mod tests {
    use super::DataGridContentMode;
    use crate::ui::document::data_view::DataViewMode;

    #[test]
    fn table_mode_with_columns_and_zero_rows_prefers_table_content() {
        let mode = super::content_mode_for_result(false, DataViewMode::Table, true, false);

        assert_eq!(mode, DataGridContentMode::Table);
    }

    #[test]
    fn table_mode_without_columns_uses_empty_fallback() {
        let mode = super::content_mode_for_result(false, DataViewMode::Table, false, false);

        assert_eq!(mode, DataGridContentMode::EmptyFallback);
    }

    #[test]
    fn document_mode_with_columns_and_zero_rows_keeps_empty_fallback() {
        let mode = super::content_mode_for_result(false, DataViewMode::Document, true, false);

        assert_eq!(mode, DataGridContentMode::EmptyFallback);
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
