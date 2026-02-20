use super::utils::value_to_json;
use super::{
    ContextMenuItem, DataGridEvent, DataGridPanel, DataSource, EditState, PendingDeleteConfirm,
    PendingDocumentPreview, PendingModalOpen, PendingToast, SqlGenerateKind, TableContextMenu,
};
use crate::keymap::{Command, ContextId};
use crate::ui::components::data_table::ContextMenuAction;
use crate::ui::components::data_table::{HEADER_HEIGHT, ROW_HEIGHT};
use crate::ui::icons::AppIcon;
use crate::ui::toast::ToastExt;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::{QueryRequest, Value};
use dbflux_export::{CsvExporter, Exporter};
use gpui::prelude::FluentBuilder;
use gpui::{deferred, *};
use gpui_component::ActiveTheme;
use std::fs::File;
use std::io::BufWriter;

impl DataGridPanel {
    fn restore_focus_after_context_menu(
        &mut self,
        is_document_view: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.focus_mode = super::GridFocusMode::Table;
        self.edit_state = EditState::Navigating;

        if is_document_view {
            if let Some(tree_state) = &self.document_tree_state {
                tree_state.update(cx, |state, _| state.focus(window));
            } else {
                self.focus_handle.focus(window);
            }
        } else {
            self.focus_handle.focus(window);
        }

        cx.emit(DataGridEvent::Focused);
    }

    /// Opens context menu at the current selection.
    pub(super) fn open_context_menu_at_selection(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let (row, col, cell_x, horizontal_offset) = {
            let ts = table_state.read(cx);

            let (row, col) = ts
                .selection()
                .active
                .map(|c| (c.row, c.col))
                .unwrap_or((0, 0));

            let widths = ts.column_widths();

            // Calculate cell x position: sum of column widths up to col
            let cell_x: f32 = widths.iter().take(col).sum();

            (row, col, cell_x, ts.horizontal_offset())
        };

        // Calculate position in window coordinates:
        // x: panel_origin.x + cell_x - horizontal_scroll + some padding
        // y: panel_origin.y + HEADER_HEIGHT + (row * ROW_HEIGHT) + some padding for toolbar
        let toolbar_height = px(36.0); // Approximate toolbar height
        let position = Point {
            x: self.panel_origin.x + px(cell_x) - horizontal_offset + px(20.0),
            y: self.panel_origin.y + toolbar_height + HEADER_HEIGHT + ROW_HEIGHT * row,
        };

        self.context_menu = Some(TableContextMenu {
            row,
            col,
            position,
            sql_submenu_open: false,
            selected_index: 0,
            submenu_selected_index: 0,
            is_document_view: false,
        });

        // Focus the context menu to receive keyboard events
        self.context_menu_focus.focus(window);
        cx.emit(DataGridEvent::Focused);
        cx.notify();
    }

    /// Opens context menu for document view at the specified position.
    #[allow(dead_code)]
    pub(super) fn open_document_context_menu(
        &mut self,
        doc_index: usize,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.context_menu = Some(TableContextMenu {
            row: doc_index,
            col: 0,
            position,
            sql_submenu_open: false,
            selected_index: 0,
            submenu_selected_index: 0,
            is_document_view: true,
        });

        self.context_menu_focus.focus(window);
        cx.emit(DataGridEvent::Focused);
        cx.notify();
    }

    /// Opens context menu for document view at the current cursor position (keyboard triggered).
    pub(super) fn open_document_context_menu_at_cursor(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(tree_state) = &self.document_tree_state else {
            return;
        };

        let cursor_info = tree_state.read(cx).cursor().and_then(|id| id.doc_index());
        let doc_index = cursor_info.unwrap_or(0);

        // Use panel origin with some offset for keyboard-triggered menu
        let position = Point {
            x: self.panel_origin.x + px(100.0),
            y: self.panel_origin.y + px(100.0),
        };

        self.context_menu = Some(TableContextMenu {
            row: doc_index,
            col: 0,
            position,
            sql_submenu_open: false,
            selected_index: 0,
            submenu_selected_index: 0,
            is_document_view: true,
        });

        self.context_menu_focus.focus(window);
        cx.emit(DataGridEvent::Focused);
        cx.notify();
    }

    /// Returns true if the data grid is editable (has primary key info).
    pub(super) fn check_is_editable(&self, cx: &App) -> bool {
        self.table_state
            .as_ref()
            .map(|ts| ts.read(cx).is_editable())
            .unwrap_or(false)
    }

    /// Returns true if the context menu is currently open.
    /// Returns the active context for keyboard handling.
    pub fn active_context(&self, cx: &App) -> ContextId {
        if self.cell_editor.read(cx).is_visible()
            || self.document_preview_modal.read(cx).is_visible()
        {
            return ContextId::TextInput;
        }

        if self.context_menu.is_some() {
            ContextId::ContextMenu
        } else if self.edit_state == EditState::Editing {
            ContextId::TextInput
        } else {
            ContextId::Results
        }
    }

    /// Handles commands when the context menu is open.
    pub(super) fn dispatch_menu_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let is_editable = self.check_is_editable(cx);
        let is_document_view = self
            .context_menu
            .as_ref()
            .map(|m| m.is_document_view)
            .unwrap_or(false);

        // Build the menu items list based on view mode
        let menu_items: Vec<Option<ContextMenuAction>> = if is_document_view {
            // Document view: Copy, View Document, [sep, Delete Document if editable]
            if is_editable {
                vec![
                    Some(ContextMenuAction::Copy),
                    Some(ContextMenuAction::EditInModal),
                    None, // separator
                    Some(ContextMenuAction::DeleteRow),
                ]
            } else {
                vec![
                    Some(ContextMenuAction::Copy),
                    Some(ContextMenuAction::EditInModal),
                ]
            }
        } else if is_editable {
            // Table view (editable): Copy, Paste, Edit, EditModal, sep, SetDefault, SetNull, sep, AddRow, DupRow, DelRow, sep, GenSQL
            vec![
                Some(ContextMenuAction::Copy),
                Some(ContextMenuAction::Paste),
                Some(ContextMenuAction::Edit),
                Some(ContextMenuAction::EditInModal),
                None, // separator
                Some(ContextMenuAction::SetDefault),
                Some(ContextMenuAction::SetNull),
                None, // separator
                Some(ContextMenuAction::AddRow),
                Some(ContextMenuAction::DuplicateRow),
                Some(ContextMenuAction::DeleteRow),
                None, // separator (before Generate SQL)
                None, // Generate SQL trigger (special handling)
            ]
        } else {
            // Table view (read-only): Copy, sep, GenSQL
            vec![
                Some(ContextMenuAction::Copy),
                None, // separator (before Generate SQL)
                None, // Generate SQL trigger
            ]
        };

        let item_count = menu_items.len();
        let submenu_count = 4; // SELECT WHERE, INSERT, UPDATE, DELETE
        let has_generate_sql = !is_document_view;

        match cmd {
            Command::MenuDown => {
                if let Some(ref mut menu) = self.context_menu {
                    if menu.sql_submenu_open && has_generate_sql {
                        menu.submenu_selected_index =
                            (menu.submenu_selected_index + 1) % submenu_count;
                    } else {
                        menu.selected_index = (menu.selected_index + 1) % item_count;
                        // Skip separators
                        while menu.selected_index < item_count
                            && menu_items[menu.selected_index].is_none()
                            && (has_generate_sql || menu.selected_index != item_count - 1)
                        {
                            menu.selected_index = (menu.selected_index + 1) % item_count;
                        }
                    }
                    cx.notify();
                }
                true
            }
            Command::MenuUp => {
                if let Some(ref mut menu) = self.context_menu {
                    if menu.sql_submenu_open && has_generate_sql {
                        menu.submenu_selected_index = if menu.submenu_selected_index == 0 {
                            submenu_count - 1
                        } else {
                            menu.submenu_selected_index - 1
                        };
                    } else {
                        menu.selected_index = if menu.selected_index == 0 {
                            item_count - 1
                        } else {
                            menu.selected_index - 1
                        };
                        // Skip separators (going backwards)
                        while menu.selected_index > 0
                            && menu_items[menu.selected_index].is_none()
                            && (has_generate_sql || menu.selected_index != item_count - 1)
                        {
                            menu.selected_index = if menu.selected_index == 0 {
                                item_count - 1
                            } else {
                                menu.selected_index - 1
                            };
                        }
                    }
                    cx.notify();
                }
                true
            }
            Command::MenuSelect => {
                if let Some(ref mut menu) = self.context_menu {
                    if menu.sql_submenu_open && has_generate_sql {
                        // Execute submenu action
                        let action = match menu.submenu_selected_index {
                            0 => ContextMenuAction::GenerateSelectWhere,
                            1 => ContextMenuAction::GenerateInsert,
                            2 => ContextMenuAction::GenerateUpdate,
                            _ => ContextMenuAction::GenerateDelete,
                        };
                        self.handle_context_menu_action(action, window, cx);
                    } else if has_generate_sql && menu.selected_index == item_count - 1 {
                        // Last item is Generate SQL - open submenu (only for table view)
                        menu.sql_submenu_open = true;
                        menu.submenu_selected_index = 0;
                        cx.notify();
                    } else if let Some(action) =
                        menu_items.get(menu.selected_index).and_then(|a| *a)
                    {
                        self.handle_context_menu_action(action, window, cx);
                    }
                }
                true
            }
            Command::MenuBack | Command::Cancel => {
                if let Some(ref mut menu) = self.context_menu {
                    if menu.sql_submenu_open {
                        // Close submenu
                        menu.sql_submenu_open = false;
                        cx.notify();
                    } else {
                        // Close menu and restore focus to active view
                        let is_document_view = menu.is_document_view;
                        self.context_menu = None;
                        self.restore_focus_after_context_menu(is_document_view, window, cx);
                        cx.notify();
                    }
                }
                true
            }
            _ => false,
        }
    }

    // === Export ===

    pub fn export_results(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.result.rows.is_empty() {
            cx.toast_error("No results to export", window);
            return;
        }

        let result = self.result.clone();
        let suggested_name = match &self.source {
            DataSource::Table { table, .. } => format!("{}.csv", table.name),
            DataSource::Collection { collection, .. } => format!("{}.csv", collection.name),
            DataSource::QueryResult { .. } => {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                format!("result_{}.csv", timestamp)
            }
        };

        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let file_handle = rfd::AsyncFileDialog::new()
                .set_title("Export as CSV")
                .set_file_name(&suggested_name)
                .add_filter("CSV", &["csv"])
                .save_file()
                .await;

            let Some(handle) = file_handle else {
                return;
            };

            let path = handle.path().to_path_buf();

            let export_result = (|| {
                let file = File::create(&path)?;
                let mut writer = BufWriter::new(file);
                CsvExporter.export(&result, &mut writer)?;
                Ok::<_, dbflux_export::ExportError>(())
            })();

            let message = match &export_result {
                Ok(()) => format!("Exported to {}", path.display()),
                Err(e) => format!("Export failed: {}", e),
            };
            let is_error = export_result.is_err();

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    panel.pending_toast = Some(PendingToast { message, is_error });
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn build_context_menu_items(
        is_editable: bool,
        is_document_view: bool,
    ) -> Vec<ContextMenuItem> {
        if is_document_view {
            // Document view menu: Copy, View/Edit Document, Delete Document
            let mut items = vec![
                ContextMenuItem {
                    label: "Copy",
                    action: Some(ContextMenuAction::Copy),
                    icon: Some(AppIcon::Layers),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "View Document",
                    action: Some(ContextMenuAction::EditInModal),
                    icon: Some(AppIcon::Maximize2),
                    is_separator: false,
                    is_danger: false,
                },
            ];

            if is_editable {
                items.extend([
                    ContextMenuItem {
                        label: "",
                        action: None,
                        icon: None,
                        is_separator: true,
                        is_danger: false,
                    },
                    ContextMenuItem {
                        label: "Delete Document",
                        action: Some(ContextMenuAction::DeleteRow),
                        icon: Some(AppIcon::Delete),
                        is_separator: false,
                        is_danger: true,
                    },
                ]);
            }

            return items;
        }

        // Table view menu
        let mut items = vec![ContextMenuItem {
            label: "Copy",
            action: Some(ContextMenuAction::Copy),
            icon: Some(AppIcon::Layers),
            is_separator: false,
            is_danger: false,
        }];

        if is_editable {
            items.extend([
                ContextMenuItem {
                    label: "Paste",
                    action: Some(ContextMenuAction::Paste),
                    icon: Some(AppIcon::Download),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Edit",
                    action: Some(ContextMenuAction::Edit),
                    icon: Some(AppIcon::Pencil),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Edit in Modal",
                    action: Some(ContextMenuAction::EditInModal),
                    icon: Some(AppIcon::Maximize2),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "",
                    action: None,
                    icon: None,
                    is_separator: true,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Set to Default",
                    action: Some(ContextMenuAction::SetDefault),
                    icon: Some(AppIcon::RotateCcw),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Set to NULL",
                    action: Some(ContextMenuAction::SetNull),
                    icon: Some(AppIcon::X),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "",
                    action: None,
                    icon: None,
                    is_separator: true,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Add Row",
                    action: Some(ContextMenuAction::AddRow),
                    icon: Some(AppIcon::Plus),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Duplicate Row",
                    action: Some(ContextMenuAction::DuplicateRow),
                    icon: Some(AppIcon::Layers),
                    is_separator: false,
                    is_danger: false,
                },
                ContextMenuItem {
                    label: "Delete Row",
                    action: Some(ContextMenuAction::DeleteRow),
                    icon: Some(AppIcon::Delete),
                    is_separator: false,
                    is_danger: true,
                },
            ]);
        }

        items
    }

    /// Returns the total number of navigable items in the context menu.
    /// This includes all visible items plus the Generate SQL trigger (for table view).
    #[allow(dead_code)]
    pub(super) fn context_menu_item_count(is_editable: bool, is_document_view: bool) -> usize {
        let base_items = Self::build_context_menu_items(is_editable, is_document_view);
        let base_count = base_items.iter().filter(|i| !i.is_separator).count();
        // Add 1 for Generate SQL only in table view
        if is_document_view {
            base_count
        } else {
            base_count + 1
        }
    }

    pub(super) fn render_delete_confirm_modal(
        &self,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let entity = cx.entity().clone();
        let entity_cancel = cx.entity().clone();

        let btn_hover = theme.muted;

        // Backdrop with centered modal
        div()
            .id("delete-modal-overlay")
            .absolute()
            .inset_0()
            .bg(gpui::hsla(0.0, 0.0, 0.0, 0.5))
            .flex()
            .items_center()
            .justify_center()
            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                cx.stop_propagation();
            })
            .child(
                div()
                    .bg(theme.background)
                    .border_1()
                    .border_color(theme.border)
                    .rounded(Radii::MD)
                    .p(Spacing::MD)
                    .min_w(px(300.0))
                    .flex()
                    .flex_col()
                    .gap(Spacing::MD)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(
                                svg()
                                    .path(AppIcon::TriangleAlert.path())
                                    .size_5()
                                    .text_color(theme.warning),
                            )
                            .child(
                                div()
                                    .text_size(FontSizes::SM)
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(theme.foreground)
                                    .child("Delete row?"),
                            ),
                    )
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .text_color(theme.muted_foreground)
                            .child("This action cannot be undone."),
                    )
                    .child(
                        div()
                            .flex()
                            .justify_end()
                            .gap(Spacing::SM)
                            .child(
                                div()
                                    .id("delete-cancel-btn")
                                    .flex()
                                    .items_center()
                                    .gap_1()
                                    .px(Spacing::SM)
                                    .py(Spacing::XS)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .text_size(FontSizes::SM)
                                    .text_color(theme.muted_foreground)
                                    .bg(theme.secondary)
                                    .hover(|d| d.bg(btn_hover))
                                    .on_click(cx.listener(move |_, _, window, cx| {
                                        entity_cancel.update(cx, |panel, cx| {
                                            panel.cancel_delete(window, cx);
                                        });
                                    }))
                                    .child(
                                        svg()
                                            .path(AppIcon::X.path())
                                            .size_4()
                                            .text_color(theme.muted_foreground),
                                    )
                                    .child("Cancel"),
                            )
                            .child(
                                div()
                                    .id("delete-confirm-btn")
                                    .flex()
                                    .items_center()
                                    .gap_1()
                                    .px(Spacing::SM)
                                    .py(Spacing::XS)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .text_size(FontSizes::SM)
                                    .text_color(theme.background)
                                    .bg(theme.danger)
                                    .hover(|d| d.opacity(0.9))
                                    .on_click(cx.listener(move |_, _, window, cx| {
                                        entity.update(cx, |panel, cx| {
                                            panel.confirm_delete(window, cx);
                                        });
                                    }))
                                    .child(
                                        svg()
                                            .path(AppIcon::Delete.path())
                                            .size_4()
                                            .text_color(theme.background),
                                    )
                                    .child("Delete"),
                            ),
                    ),
            )
    }

    pub(super) fn render_context_menu(
        &self,
        menu: &TableContextMenu,
        is_editable: bool,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let menu_width = px(180.0);

        // Convert window coordinates to panel-relative coordinates
        let menu_x = menu.position.x - self.panel_origin.x;
        let menu_y = menu.position.y - self.panel_origin.y;

        // Build visible menu items list for keyboard navigation
        let visible_items = Self::build_context_menu_items(is_editable, menu.is_document_view);
        let selected_index = menu.selected_index;
        let is_document_view = menu.is_document_view;

        // Build menu items with selection highlighting
        let mut menu_items: Vec<AnyElement> = Vec::new();
        let mut visual_index = 0usize;

        for item in &visible_items {
            if item.is_separator {
                menu_items.push(
                    div()
                        .h(px(1.0))
                        .mx(Spacing::SM)
                        .my(Spacing::XS)
                        .bg(theme.border)
                        .into_any_element(),
                );
                visual_index += 1;
                continue;
            }

            let Some(action) = item.action else {
                visual_index += 1;
                continue;
            };

            let is_selected = visual_index == selected_index;
            let is_danger = item.is_danger;
            let label = item.label;
            let icon = item.icon;
            let current_index = visual_index;

            menu_items.push(
                div()
                    .id(SharedString::from(label))
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .h(Heights::ROW_COMPACT)
                    .px(Spacing::SM)
                    .mx(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .text_size(FontSizes::SM)
                    .text_color(if is_danger {
                        theme.danger
                    } else {
                        theme.foreground
                    })
                    .when(is_selected, |d| {
                        d.bg(if is_danger {
                            theme.danger.opacity(0.1)
                        } else {
                            theme.accent
                        })
                        .text_color(if is_danger {
                            theme.danger
                        } else {
                            theme.accent_foreground
                        })
                    })
                    .when(!is_selected, |d| {
                        d.hover(|d| {
                            d.bg(if is_danger {
                                theme.danger.opacity(0.1)
                            } else {
                                theme.secondary
                            })
                        })
                    })
                    .on_mouse_move(cx.listener(move |this, _, _, cx| {
                        if let Some(ref mut menu) = this.context_menu
                            && menu.selected_index != current_index
                        {
                            menu.selected_index = current_index;
                            cx.notify();
                        }
                    }))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.handle_context_menu_action(action, window, cx);
                    }))
                    .when_some(icon, |d, icon| {
                        d.child(svg().path(icon.path()).size_4().text_color(if is_danger {
                            theme.danger
                        } else if is_selected {
                            theme.accent_foreground
                        } else {
                            theme.muted_foreground
                        }))
                    })
                    .when(icon.is_none(), |d| d.pl(px(20.0)))
                    .child(label)
                    .into_any_element(),
            );

            visual_index += 1;
        }

        // "Generate SQL" submenu (only for table view, not document view)
        if !is_document_view {
            // Add separator before "Generate SQL"
            menu_items.push(
                div()
                    .h(px(1.0))
                    .mx(Spacing::SM)
                    .my(Spacing::XS)
                    .bg(theme.border)
                    .into_any_element(),
            );
            visual_index += 1; // Separator takes an index slot

            // "Generate SQL" submenu trigger
            let sql_submenu_open = menu.sql_submenu_open;
            let submenu_bg = theme.popover;
            let submenu_border = theme.border;
            let submenu_fg = theme.foreground;
            let submenu_hover = theme.secondary;
            let gen_sql_index = visual_index; // Index for Generate SQL item
            let gen_sql_selected = selected_index == gen_sql_index;
            let submenu_selected_index = menu.submenu_selected_index;

            menu_items.push(
                div()
                    .id("generate-sql-trigger")
                    .relative()
                    .flex()
                    .items_center()
                    .justify_between()
                    .h(Heights::ROW_COMPACT)
                    .px(Spacing::SM)
                    .mx(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .text_size(FontSizes::SM)
                    .text_color(if gen_sql_selected && !sql_submenu_open {
                        theme.accent_foreground
                    } else {
                        submenu_fg
                    })
                    .when(sql_submenu_open, |d| d.bg(submenu_hover))
                    .when(gen_sql_selected && !sql_submenu_open, |d| {
                        d.bg(theme.accent)
                    })
                    .when(!gen_sql_selected && !sql_submenu_open, |d| {
                        d.hover(|d| d.bg(submenu_hover))
                    })
                    .on_mouse_move(cx.listener(move |this, _, _, cx| {
                        if let Some(ref mut menu) = this.context_menu
                            && menu.selected_index != gen_sql_index
                            && !menu.sql_submenu_open
                        {
                            menu.selected_index = gen_sql_index;
                            cx.notify();
                        }
                    }))
                    .on_click(cx.listener(|this, _, _, cx| {
                        if let Some(ref mut menu) = this.context_menu {
                            menu.sql_submenu_open = !menu.sql_submenu_open;
                            menu.submenu_selected_index = 0;
                            cx.notify();
                        }
                    }))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .child(svg().path(AppIcon::Code.path()).size_4().text_color(
                                if gen_sql_selected && !sql_submenu_open {
                                    theme.accent_foreground
                                } else {
                                    submenu_fg
                                },
                            ))
                            .child("Generate SQL"),
                    )
                    .child(
                        svg()
                            .path(AppIcon::ChevronRight.path())
                            .size_4()
                            .text_color(if gen_sql_selected && !sql_submenu_open {
                                theme.accent_foreground
                            } else {
                                theme.muted_foreground
                            }),
                    )
                    // Submenu appears to the right
                    .when(sql_submenu_open, |d: Stateful<Div>| {
                        d.child(
                            div()
                                .absolute()
                                .left(px(172.0)) // menu_width - some padding
                                .top(px(-4.0))
                                .w(px(160.0))
                                .bg(submenu_bg)
                                .border_1()
                                .border_color(submenu_border)
                                .rounded(Radii::MD)
                                .shadow_lg()
                                .py(Spacing::XS)
                                // Capture clicks within submenu bounds (prevents overlay from closing menu)
                                .occlude()
                                // Stop click from bubbling to parent "Generate SQL" trigger
                                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                    cx.stop_propagation();
                                })
                                .children(
                                    [
                                        ("SELECT WHERE", ContextMenuAction::GenerateSelectWhere),
                                        ("INSERT", ContextMenuAction::GenerateInsert),
                                        ("UPDATE", ContextMenuAction::GenerateUpdate),
                                        ("DELETE", ContextMenuAction::GenerateDelete),
                                    ]
                                    .into_iter()
                                    .enumerate()
                                    .map(|(idx, (label, action))| {
                                        let is_submenu_selected = idx == submenu_selected_index;
                                        div()
                                            .id(SharedString::from(label))
                                            .flex()
                                            .items_center()
                                            .gap(Spacing::SM)
                                            .h(Heights::ROW_COMPACT)
                                            .px(Spacing::SM)
                                            .mx(Spacing::XS)
                                            .rounded(Radii::SM)
                                            .cursor_pointer()
                                            .text_size(FontSizes::SM)
                                            .text_color(if is_submenu_selected {
                                                theme.accent_foreground
                                            } else {
                                                submenu_fg
                                            })
                                            .when(is_submenu_selected, |d| d.bg(theme.accent))
                                            .when(!is_submenu_selected, |d| {
                                                d.hover(|d| d.bg(submenu_hover))
                                            })
                                            .on_mouse_move(cx.listener(move |this, _, _, cx| {
                                                if let Some(ref mut menu) = this.context_menu
                                                    && menu.submenu_selected_index != idx
                                                {
                                                    menu.submenu_selected_index = idx;
                                                    cx.notify();
                                                }
                                            }))
                                            .on_click(cx.listener(move |this, _, window, cx| {
                                                this.handle_context_menu_action(action, window, cx);
                                            }))
                                            .child(
                                                svg()
                                                    .path(AppIcon::Code.path())
                                                    .size_4()
                                                    .text_color(if is_submenu_selected {
                                                        theme.accent_foreground
                                                    } else {
                                                        theme.muted_foreground
                                                    }),
                                            )
                                            .child(label)
                                    })
                                    .collect::<Vec<_>>(),
                                ),
                        )
                    })
                    .into_any_element(),
            );
        }

        // Use deferred() to render at window level for correct positioning
        deferred(
            div()
                .id("context-menu-overlay")
                .absolute()
                .top_0()
                .left_0()
                .size_full()
                .track_focus(&self.context_menu_focus)
                .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                    use crate::keymap::{KeyChord, default_keymap};

                    let chord = KeyChord::from_gpui(&event.keystroke);
                    let keymap = default_keymap();

                    if let Some(cmd) = keymap.resolve(ContextId::ContextMenu, &chord)
                        && this.dispatch_menu_command(cmd, window, cx)
                    {
                        cx.stop_propagation();
                    }
                }))
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(|this, _, window, cx| {
                        let is_document_view = this
                            .context_menu
                            .as_ref()
                            .map(|menu| menu.is_document_view)
                            .unwrap_or(false);

                        this.context_menu = None;
                        this.restore_focus_after_context_menu(is_document_view, window, cx);
                        cx.notify();
                    }),
                )
                .on_mouse_down(
                    MouseButton::Right,
                    cx.listener(|this, _, window, cx| {
                        let is_document_view = this
                            .context_menu
                            .as_ref()
                            .map(|menu| menu.is_document_view)
                            .unwrap_or(false);

                        this.context_menu = None;
                        this.restore_focus_after_context_menu(is_document_view, window, cx);
                        cx.notify();
                    }),
                )
                .child(
                    div()
                        .id("context-menu")
                        .absolute()
                        .left(menu_x)
                        .top(menu_y)
                        .w(menu_width)
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
                        .children(menu_items),
                ),
        )
        .with_priority(1)
    }

    pub(super) fn handle_context_menu_action(
        &mut self,
        action: ContextMenuAction,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let menu = match self.context_menu.take() {
            Some(m) => m,
            None => return,
        };

        let is_document_view = menu.is_document_view;

        match action {
            ContextMenuAction::Copy => {
                if menu.is_document_view {
                    self.handle_copy_document(menu.row, cx);
                } else {
                    self.handle_copy(window, cx);
                }
            }
            ContextMenuAction::Paste => self.handle_paste(window, cx),
            ContextMenuAction::Edit => self.handle_edit(menu.row, menu.col, window, cx),
            ContextMenuAction::EditInModal => {
                if menu.is_document_view {
                    self.handle_view_document(menu.row, cx);
                } else {
                    self.handle_edit_in_modal(menu.row, menu.col, cx);
                }
            }
            ContextMenuAction::SetDefault => self.handle_set_default(menu.row, menu.col, cx),
            ContextMenuAction::SetNull => self.handle_set_null(menu.row, menu.col, cx),
            ContextMenuAction::AddRow => self.handle_add_row(menu.row, cx),
            ContextMenuAction::DuplicateRow => self.handle_duplicate_row(menu.row, cx),
            ContextMenuAction::DeleteRow => {
                if menu.is_document_view {
                    self.pending_delete_confirm = Some(PendingDeleteConfirm {
                        row_idx: menu.row,
                        is_table: false,
                    });
                    cx.notify();
                } else {
                    self.handle_delete_row(menu.row, cx);
                }
            }
            ContextMenuAction::GenerateSelectWhere => {
                self.handle_generate_sql(menu.row, SqlGenerateKind::SelectWhere, cx)
            }
            ContextMenuAction::GenerateInsert => {
                self.handle_generate_sql(menu.row, SqlGenerateKind::Insert, cx)
            }
            ContextMenuAction::GenerateUpdate => {
                self.handle_generate_sql(menu.row, SqlGenerateKind::Update, cx)
            }
            ContextMenuAction::GenerateDelete => {
                self.handle_generate_sql(menu.row, SqlGenerateKind::Delete, cx)
            }
        }

        // Restore focus to the active view after action
        self.restore_focus_after_context_menu(is_document_view, window, cx);
        cx.notify();
    }

    pub(super) fn handle_copy(&self, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(table_state) = &self.table_state {
            let text = table_state.read(cx).copy_selection();
            if let Some(text) = text {
                cx.write_to_clipboard(ClipboardItem::new_string(text));
            }
        }
    }

    /// Copy entire document as JSON (for document view).
    pub(super) fn handle_copy_document(&self, doc_index: usize, cx: &mut Context<Self>) {
        let Some(tree_state) = &self.document_tree_state else {
            return;
        };

        if let Some(raw_doc) = tree_state.read(cx).get_raw_document(doc_index) {
            let json_value = value_to_json(raw_doc);
            if let Ok(json_str) = serde_json::to_string_pretty(&json_value) {
                cx.write_to_clipboard(ClipboardItem::new_string(json_str));
            }
        }
    }

    /// Open document preview modal for viewing/editing (for document view).
    pub(super) fn handle_view_document(&mut self, doc_index: usize, cx: &mut Context<Self>) {
        let Some(tree_state) = &self.document_tree_state else {
            return;
        };

        if let Some(raw_doc) = tree_state.read(cx).get_raw_document(doc_index) {
            let json_value = value_to_json(raw_doc);
            let json_str =
                serde_json::to_string_pretty(&json_value).unwrap_or_else(|_| "{}".to_string());

            self.pending_document_preview = Some(PendingDocumentPreview {
                doc_index,
                document_json: json_str,
            });
            cx.notify();
        }
    }

    /// Copy entire row as TSV (tab-separated values).
    pub(super) fn handle_copy_row(&self, row: usize, cx: &mut Context<Self>) {
        use crate::ui::components::data_table::model::VisualRowSource;

        let Some(table_state) = &self.table_state else {
            return;
        };

        let state = table_state.read(cx);
        let buffer = state.edit_buffer();
        let visual_order = buffer.compute_visual_order();

        // Get row data based on visual row source
        let row_values: Vec<String> = match visual_order.get(row).copied() {
            Some(VisualRowSource::Base(base_idx)) => self
                .result
                .rows
                .get(base_idx)
                .map(|r| {
                    r.iter()
                        .map(|val| {
                            crate::ui::components::data_table::clipboard::format_cell(
                                &crate::ui::components::data_table::model::CellValue::from(val),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
            Some(VisualRowSource::Insert(insert_idx)) => buffer
                .get_pending_insert_by_idx(insert_idx)
                .map(|cells| {
                    cells
                        .iter()
                        .map(crate::ui::components::data_table::clipboard::format_cell)
                        .collect()
                })
                .unwrap_or_default(),
            None => return,
        };

        if !row_values.is_empty() {
            let text = row_values.join("\t");
            cx.write_to_clipboard(ClipboardItem::new_string(text));
        }
    }

    pub(super) fn handle_paste(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let clipboard_text = cx
            .read_from_clipboard()
            .and_then(|item| item.text().map(|s| s.to_string()));

        let Some(text) = clipboard_text else {
            return;
        };

        table_state.update(cx, |state, cx| {
            if let Some(coord) = state.selection().active {
                let cell_value = crate::ui::components::data_table::model::CellValue::text(&text);
                state
                    .edit_buffer_mut()
                    .set_cell(coord.row, coord.col, cell_value);
                cx.notify();
            }
        });
    }

    pub(super) fn handle_edit(
        &mut self,
        row: usize,
        col: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |state, cx| {
                let coord = crate::ui::components::data_table::selection::CellCoord::new(row, col);
                state.start_editing(coord, window, cx);
            });
        }
    }

    pub(super) fn handle_edit_in_modal(&mut self, row: usize, col: usize, cx: &mut Context<Self>) {
        use crate::ui::components::data_table::model::{ColumnKind, VisualRowSource};

        let Some(table_state) = &self.table_state else {
            return;
        };

        let state = table_state.read(cx);
        if !state.is_editable() {
            return;
        }

        let is_json = state
            .model()
            .columns
            .get(col)
            .map(|c| c.kind == ColumnKind::Json)
            .unwrap_or(false);

        let visual_order = state.edit_buffer().compute_visual_order();
        let null_cell = crate::ui::components::data_table::model::CellValue::null();

        let value = match visual_order.get(row).copied() {
            Some(VisualRowSource::Base(base_idx)) => {
                let base_cell = state.model().cell(base_idx, col);
                let base = base_cell.unwrap_or(&null_cell);
                let cell = state.edit_buffer().get_cell(base_idx, col, base);
                cell.edit_text()
            }
            Some(VisualRowSource::Insert(insert_idx)) => {
                if let Some(insert_data) = state.edit_buffer().get_pending_insert_by_idx(insert_idx)
                {
                    if col < insert_data.len() {
                        insert_data[col].edit_text()
                    } else {
                        String::new()
                    }
                } else {
                    String::new()
                }
            }
            None => return,
        };

        self.pending_modal_open = Some(PendingModalOpen {
            row,
            col,
            value,
            is_json,
        });
        cx.notify();
    }

    pub(super) fn handle_set_default(&mut self, row: usize, col: usize, cx: &mut Context<Self>) {
        use crate::ui::components::data_table::model::VisualRowSource;

        // Get column default value from table details
        let default_value = self.get_column_default(col, cx);

        let Some(table_state) = &self.table_state else {
            return;
        };

        table_state.update(cx, |state, cx| {
            let buffer = state.edit_buffer_mut();
            let visual_order = buffer.compute_visual_order();

            let cell_value = if let Some(default) = default_value {
                crate::ui::components::data_table::model::CellValue::text(&default)
            } else {
                crate::ui::components::data_table::model::CellValue::null()
            };

            match visual_order.get(row).copied() {
                Some(VisualRowSource::Base(base_idx)) => {
                    buffer.set_cell(base_idx, col, cell_value);
                }
                Some(VisualRowSource::Insert(insert_idx)) => {
                    buffer.set_insert_cell(insert_idx, col, cell_value);
                }
                None => {}
            }

            cx.notify();
        });
    }

    pub(super) fn handle_set_null(&mut self, row: usize, col: usize, cx: &mut Context<Self>) {
        use crate::ui::components::data_table::model::VisualRowSource;

        let Some(table_state) = &self.table_state else {
            return;
        };

        table_state.update(cx, |state, cx| {
            let buffer = state.edit_buffer_mut();
            let visual_order = buffer.compute_visual_order();
            let cell_value = crate::ui::components::data_table::model::CellValue::null();

            match visual_order.get(row).copied() {
                Some(VisualRowSource::Base(base_idx)) => {
                    buffer.set_cell(base_idx, col, cell_value);
                }
                Some(VisualRowSource::Insert(insert_idx)) => {
                    buffer.set_insert_cell(insert_idx, col, cell_value);
                }
                None => {}
            }

            cx.notify();
        });
    }

    pub(super) fn handle_cell_editor_save(
        &mut self,
        row: usize,
        col: usize,
        value: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::components::data_table::model::VisualRowSource;

        let Some(table_state) = &self.table_state else {
            return;
        };

        table_state.update(cx, |state, cx| {
            let buffer = state.edit_buffer_mut();
            let visual_order = buffer.compute_visual_order();
            let cell_value = crate::ui::components::data_table::model::CellValue::text(value);

            match visual_order.get(row).copied() {
                Some(VisualRowSource::Base(base_idx)) => {
                    buffer.set_cell(base_idx, col, cell_value);
                }
                Some(VisualRowSource::Insert(insert_idx)) => {
                    buffer.set_insert_cell(insert_idx, col, cell_value);
                }
                None => {}
            }

            cx.notify();
        });

        self.focus_table(window, cx);
    }

    pub(super) fn handle_document_preview_save(
        &mut self,
        _doc_index: usize,
        document_json: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let new_doc: serde_json::Value = match serde_json::from_str(document_json) {
            Ok(v) => v,
            Err(e) => {
                cx.toast_error(format!("Invalid JSON: {}", e), window);
                return;
            }
        };

        let doc_id = match new_doc.get("_id") {
            Some(id) => id.clone(),
            None => {
                cx.toast_error("Document must have an _id field", window);
                return;
            }
        };

        let DataSource::Collection {
            profile_id,
            collection,
            ..
        } = &self.source
        else {
            return;
        };

        let (conn, active_database) = {
            let state = self.app_state.read(cx);
            match state.connections().get(profile_id) {
                Some(c) => (Some(c.connection.clone()), c.active_database.clone()),
                None => (None, None),
            }
        };

        let Some(conn) = conn else {
            cx.toast_error("Connection not available", window);
            return;
        };

        let replace_query = serde_json::json!({
            "database": collection.database,
            "collection": collection.name,
            "replace": {
                "filter": { "_id": doc_id },
                "replacement": new_doc
            }
        });

        let query_request =
            QueryRequest::new(replace_query.to_string()).with_database(active_database);
        let entity = cx.entity().clone();

        let task = cx
            .background_executor()
            .spawn(async move { conn.execute(&query_request) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    match result {
                        Ok(_) => {
                            panel.pending_toast = Some(PendingToast {
                                message: "Document updated".to_string(),
                                is_error: false,
                            });
                            panel.pending_refresh = true;
                        }
                        Err(e) => {
                            panel.pending_toast = Some(PendingToast {
                                message: format!("Failed to update document: {}", e),
                                is_error: true,
                            });
                        }
                    }
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn handle_add_row(&mut self, after_visual_row: usize, cx: &mut Context<Self>) {
        use crate::ui::components::data_table::model::VisualRowSource;

        let is_table = matches!(self.source, DataSource::Table { .. });
        let is_collection = matches!(self.source, DataSource::Collection { .. });

        if !is_table && !is_collection {
            return;
        }

        let Some(table_state) = &self.table_state else {
            return;
        };

        let insert_after_base = {
            let state = table_state.read(cx);
            let buffer = state.edit_buffer();
            let visual_order = buffer.compute_visual_order();

            match visual_order.get(after_visual_row).copied() {
                Some(VisualRowSource::Base(base_idx)) => base_idx,
                Some(VisualRowSource::Insert(insert_idx)) => buffer
                    .pending_inserts()
                    .get(insert_idx)
                    .and_then(|pi| pi.insert_after)
                    .unwrap_or(self.result.rows.len().saturating_sub(1)),
                None => self.result.rows.len().saturating_sub(1),
            }
        };

        let new_row: Vec<crate::ui::components::data_table::model::CellValue> = if is_collection {
            self.result
                .columns
                .iter()
                .map(|col| {
                    if col.name == "_id" {
                        let new_oid =
                            uuid::Uuid::new_v4().to_string().replace("-", "")[..24].to_string();
                        crate::ui::components::data_table::model::CellValue::text(&new_oid)
                    } else {
                        crate::ui::components::data_table::model::CellValue::null()
                    }
                })
                .collect()
        } else {
            let column_defaults = self.get_all_column_defaults(cx);
            self.result
                .columns
                .iter()
                .enumerate()
                .map(|(idx, _)| {
                    if let Some(default_expr) = column_defaults.get(idx).and_then(|d| d.as_ref()) {
                        crate::ui::components::data_table::model::CellValue::auto_generated(
                            default_expr,
                        )
                    } else {
                        crate::ui::components::data_table::model::CellValue::null()
                    }
                })
                .collect()
        };

        table_state.update(cx, |state, cx| {
            let buffer = state.edit_buffer_mut();
            buffer.set_base_row_count(self.result.rows.len());
            buffer.add_pending_insert_after(insert_after_base, new_row);
            cx.notify();
        });
    }

    pub(super) fn handle_duplicate_row(&mut self, visual_row: usize, cx: &mut Context<Self>) {
        use crate::ui::components::data_table::model::VisualRowSource;

        let is_table = matches!(self.source, DataSource::Table { .. });
        let is_collection = matches!(self.source, DataSource::Collection { .. });

        if !is_table && !is_collection {
            return;
        }

        let Some(table_state) = &self.table_state else {
            return;
        };

        let id_column_idx = if is_collection {
            self.result.columns.iter().position(|c| c.name == "_id")
        } else {
            None
        };

        let pk_indices: std::collections::HashSet<usize> = if is_table {
            self.pk_columns
                .iter()
                .filter_map(|pk_name| self.result.columns.iter().position(|c| c.name == *pk_name))
                .collect()
        } else {
            std::collections::HashSet::new()
        };

        let column_defaults = if is_table {
            self.get_all_column_defaults(cx)
        } else {
            vec![]
        };

        // Get source row data and determine insert position
        let base_row_count = self.result.rows.len();
        let state = table_state.read(cx);
        let buffer = state.edit_buffer();
        let visual_order = buffer.compute_visual_order();

        let new_oid = || uuid::Uuid::new_v4().to_string().replace("-", "")[..24].to_string();

        let (source_values, insert_after_base): (
            Vec<crate::ui::components::data_table::model::CellValue>,
            usize,
        ) = match visual_order.get(visual_row).copied() {
            Some(VisualRowSource::Base(base_idx)) => {
                let values = self
                    .result
                    .rows
                    .get(base_idx)
                    .map(|r| {
                        r.iter()
                            .enumerate()
                            .map(|(idx, val)| {
                                if Some(idx) == id_column_idx {
                                    crate::ui::components::data_table::model::CellValue::text(&new_oid())
                                } else if pk_indices.contains(&idx) {
                                    if let Some(default_expr) =
                                        column_defaults.get(idx).and_then(|d| d.as_ref())
                                    {
                                        crate::ui::components::data_table::model::CellValue::auto_generated(default_expr)
                                    } else {
                                        crate::ui::components::data_table::model::CellValue::null()
                                    }
                                } else {
                                    crate::ui::components::data_table::model::CellValue::from(val)
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                (values, base_idx)
            }
            Some(VisualRowSource::Insert(insert_idx)) => {
                let insert_after = buffer
                    .pending_inserts()
                    .get(insert_idx)
                    .and_then(|pi| pi.insert_after)
                    .unwrap_or(base_row_count.saturating_sub(1));

                let values = buffer
                    .get_pending_insert_by_idx(insert_idx)
                    .map(|insert_data| {
                        insert_data
                            .iter()
                            .enumerate()
                            .map(|(idx, val)| {
                                if Some(idx) == id_column_idx {
                                    crate::ui::components::data_table::model::CellValue::text(&new_oid())
                                } else if pk_indices.contains(&idx) {
                                    if let Some(default_expr) =
                                        column_defaults.get(idx).and_then(|d| d.as_ref())
                                    {
                                        crate::ui::components::data_table::model::CellValue::auto_generated(default_expr)
                                    } else {
                                        crate::ui::components::data_table::model::CellValue::null()
                                    }
                                } else {
                                    val.clone()
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                (values, insert_after)
            }
            None => return,
        };

        if source_values.is_empty() {
            return;
        }

        table_state.update(cx, |state, cx| {
            let buffer = state.edit_buffer_mut();
            buffer.set_base_row_count(base_row_count);
            buffer.add_pending_insert_after(insert_after_base, source_values);
            cx.notify();
        });
    }

    pub(super) fn handle_delete_row(&mut self, row: usize, cx: &mut Context<Self>) {
        use crate::ui::components::data_table::model::VisualRowSource;

        let is_table = matches!(self.source, DataSource::Table { .. });
        let is_collection = matches!(self.source, DataSource::Collection { .. });

        if !is_table && !is_collection {
            return;
        }

        let Some(table_state) = &self.table_state else {
            return;
        };

        let base_row_count = self.result.rows.len();

        table_state.update(cx, |state, cx| {
            let buffer = state.edit_buffer_mut();
            buffer.set_base_row_count(base_row_count);

            let visual_order = buffer.compute_visual_order();

            match visual_order.get(row).copied() {
                Some(VisualRowSource::Base(base_idx)) => {
                    buffer.mark_for_delete(base_idx);
                }
                Some(VisualRowSource::Insert(insert_idx)) => {
                    buffer.remove_pending_insert_by_idx(insert_idx);
                }
                None => {}
            }

            cx.notify();
        });
    }

    pub(super) fn handle_generate_sql(
        &mut self,
        visual_row: usize,
        kind: SqlGenerateKind,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::components::data_table::model::VisualRowSource;
        use crate::ui::sql_preview_modal::SqlGenerationType;

        let (profile_id, table_ref) = match &self.source {
            DataSource::Table {
                profile_id, table, ..
            } => (*profile_id, table.clone()),
            DataSource::Collection { .. } => return,
            DataSource::QueryResult { .. } => return,
        };

        let Some(table_state) = &self.table_state else {
            return;
        };

        // Get column info including primary keys
        let state = self.app_state.read(cx);
        let connected = match state.connections().get(&profile_id) {
            Some(c) => c,
            None => return,
        };

        let database = connected.active_database.as_deref().unwrap_or("default");
        let cache_key = (database.to_string(), table_ref.name.clone());
        let table_info = connected.table_details.get(&cache_key);
        let columns_info = table_info.and_then(|t| t.columns.as_deref());

        let col_names: Vec<String> = self.result.columns.iter().map(|c| c.name.clone()).collect();
        let ts = table_state.read(cx);
        let buffer = ts.edit_buffer();
        let visual_order = buffer.compute_visual_order();

        let row_values: Vec<Value> = match visual_order.get(visual_row).copied() {
            Some(VisualRowSource::Base(base_idx)) => {
                self.result.rows.get(base_idx).cloned().unwrap_or_default()
            }
            Some(VisualRowSource::Insert(insert_idx)) => buffer
                .get_pending_insert_by_idx(insert_idx)
                .map(|cells| cells.iter().map(|c| self.cell_value_to_value(c)).collect())
                .unwrap_or_default(),
            None => return,
        };

        if row_values.is_empty() || col_names.len() != row_values.len() {
            return;
        }

        // Find primary key columns
        let pk_indices: Vec<usize> = if let Some(cols) = columns_info {
            col_names
                .iter()
                .enumerate()
                .filter_map(|(idx, name)| {
                    cols.iter()
                        .find(|c| c.name == *name && c.is_primary_key)
                        .map(|_| idx)
                })
                .collect()
        } else {
            vec![]
        };

        // Convert SqlGenerateKind to SqlGenerationType
        let generation_type = match kind {
            SqlGenerateKind::SelectWhere => SqlGenerationType::SelectWhere,
            SqlGenerateKind::Insert => SqlGenerationType::Insert,
            SqlGenerateKind::Update => SqlGenerationType::Update,
            SqlGenerateKind::Delete => SqlGenerationType::Delete,
        };

        // Emit event for SQL preview modal
        cx.emit(DataGridEvent::RequestSqlPreview {
            profile_id,
            schema_name: table_ref.schema.clone(),
            table_name: table_ref.name.clone(),
            column_names: col_names,
            row_values,
            pk_indices,
            generation_type,
        });
    }

    pub(super) fn cell_value_to_value(
        &self,
        cell: &crate::ui::components::data_table::model::CellValue,
    ) -> Value {
        use crate::ui::components::data_table::model::CellKind;

        match &cell.kind {
            CellKind::Null => Value::Null,
            CellKind::Bool(b) => Value::Bool(*b),
            CellKind::Int(i) => Value::Int(*i),
            CellKind::Float(f) => Value::Float(*f),
            CellKind::Text(s) => Value::Text(s.to_string()),
            CellKind::Bytes(len) => Value::Bytes(vec![0u8; *len]),
            CellKind::AutoGenerated(expr) => Value::Text(format!("DEFAULT({})", expr)),
        }
    }
}
