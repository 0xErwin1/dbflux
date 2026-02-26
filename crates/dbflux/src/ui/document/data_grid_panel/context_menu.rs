use super::utils::value_to_json;
use super::{
    ContextMenuItem, DataGridEvent, DataGridPanel, DataSource, EditState, PendingDeleteConfirm,
    PendingDocumentPreview, PendingModalOpen, PendingToast, SqlGenerateKind, TableContextMenu,
};
use crate::keymap::{Command, ContextId};
use crate::ui::components::data_table::{ContextMenuAction, FilterOperator};
use crate::ui::components::data_table::{HEADER_HEIGHT, ROW_HEIGHT};
use crate::ui::icons::AppIcon;
use crate::ui::toast::ToastExt;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::{
    DocumentDelete, DocumentFilter, DocumentInsert, DocumentUpdate, MutationRequest, QueryRequest,
    RowDelete, RowIdentity, RowInsert, RowPatch, Value,
};
use dbflux_export::ExportFormat;
use gpui::prelude::FluentBuilder;
use gpui::{deferred, *};
use gpui_component::ActiveTheme;
use std::fs::File;
use std::io::BufWriter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterBackend {
    Sql,
    Mongo,
}

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
            copy_query_submenu_open: false,
            filter_submenu_open: false,
            order_submenu_open: false,
            selected_index: 0,
            submenu_selected_index: 0,
            is_document_view: false,
            doc_field_path: None,
            doc_field_value: None,
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
            copy_query_submenu_open: false,
            filter_submenu_open: false,
            order_submenu_open: false,
            selected_index: 0,
            submenu_selected_index: 0,
            is_document_view: true,
            doc_field_path: None,
            doc_field_value: None,
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

        let (doc_index, field_path, field_value) = tree_state.update(cx, |ts, _cx| {
            let cursor_id = ts.cursor().cloned();
            let idx = cursor_id
                .as_ref()
                .and_then(|id| id.doc_index())
                .unwrap_or(0);

            let (fp, fv) = cursor_id
                .as_ref()
                .and_then(|cid| {
                    let node = ts.visible_nodes().iter().find(|n| &n.id == cid)?;
                    let path: Vec<String> = cid.path[1..].to_vec();
                    let path_opt = if path.is_empty() { None } else { Some(path) };
                    Some((path_opt, Some(node.value.clone())))
                })
                .unwrap_or((None, None));

            (idx, fp, fv)
        });

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
            copy_query_submenu_open: false,
            filter_submenu_open: false,
            order_submenu_open: false,
            selected_index: 0,
            submenu_selected_index: 0,
            is_document_view: true,
            doc_field_path: field_path,
            doc_field_value: field_value,
        });

        self.context_menu_focus.focus(window);
        cx.emit(DataGridEvent::Focused);
        cx.notify();
    }

    fn filter_backend(&self, cx: &App) -> Option<FilterBackend> {
        match &self.source {
            DataSource::Table { profile_id, .. } => {
                let is_sql = self
                    .app_state
                    .read(cx)
                    .connections()
                    .get(profile_id)
                    .map(|c| {
                        c.connection.metadata().query_language == dbflux_core::QueryLanguage::Sql
                    })
                    .unwrap_or(false);
                is_sql.then_some(FilterBackend::Sql)
            }
            DataSource::Collection { .. } => Some(FilterBackend::Mongo),
            _ => None,
        }
    }

    fn has_filter_submenu(
        &self,
        backend: Option<FilterBackend>,
        is_document_view: bool,
        cx: &App,
    ) -> bool {
        match backend {
            Some(FilterBackend::Sql) => !is_document_view,
            Some(FilterBackend::Mongo) => {
                is_document_view && self.mongo_filter_field_info(cx).is_some()
            }
            None => false,
        }
    }

    fn mongo_filter_field_info(&self, _cx: &App) -> Option<(String, Value)> {
        use crate::ui::components::document_tree::NodeValue;

        let menu = self.context_menu.as_ref()?;
        let path = menu.doc_field_path.as_ref()?;
        if path.is_empty() {
            return None;
        }

        let field = path.join(".");
        let value = match menu.doc_field_value.as_ref()? {
            NodeValue::Scalar(v) => v.clone(),
            _ => return None,
        };

        if matches!(value, Value::Bytes(_)) {
            return None;
        }

        Some((field, value))
    }

    fn sql_filter_operators(type_name: &str, value: &Value) -> Vec<FilterOperator> {
        if !Self::is_value_filterable(value) {
            return Vec::new();
        }

        if Self::is_sql_json_type(type_name, value) {
            return vec![FilterOperator::Eq, FilterOperator::NotEq];
        }

        if Self::is_sql_bool_type(type_name, value) {
            return vec![FilterOperator::Eq, FilterOperator::NotEq];
        }

        if Self::is_sql_comparable_type(type_name, value) {
            return vec![
                FilterOperator::Eq,
                FilterOperator::NotEq,
                FilterOperator::Gt,
                FilterOperator::Gte,
                FilterOperator::Lt,
                FilterOperator::Lte,
            ];
        }

        if Self::is_sql_text_type(type_name, value) {
            return vec![
                FilterOperator::Eq,
                FilterOperator::NotEq,
                FilterOperator::Like,
            ];
        }

        vec![FilterOperator::Eq, FilterOperator::NotEq]
    }

    fn mongo_filter_operators(value: &Value) -> Vec<FilterOperator> {
        if !Self::is_value_filterable(value) {
            return Vec::new();
        }

        if Self::is_mongo_comparable(value) {
            return vec![
                FilterOperator::Eq,
                FilterOperator::NotEq,
                FilterOperator::Gt,
                FilterOperator::Gte,
                FilterOperator::Lt,
                FilterOperator::Lte,
            ];
        }

        vec![FilterOperator::Eq, FilterOperator::NotEq]
    }

    fn is_mongo_comparable(value: &Value) -> bool {
        matches!(
            value,
            Value::Int(_)
                | Value::Float(_)
                | Value::Decimal(_)
                | Value::DateTime(_)
                | Value::Date(_)
                | Value::Time(_)
        )
    }

    fn is_sql_json_type(type_name: &str, value: &Value) -> bool {
        type_name.contains("json") || type_name.contains("bson") || matches!(value, Value::Json(_))
    }

    fn is_sql_bool_type(type_name: &str, value: &Value) -> bool {
        type_name.contains("bool") || matches!(value, Value::Bool(_))
    }

    fn is_sql_comparable_type(type_name: &str, value: &Value) -> bool {
        if matches!(
            value,
            Value::Int(_)
                | Value::Float(_)
                | Value::Decimal(_)
                | Value::DateTime(_)
                | Value::Date(_)
                | Value::Time(_)
        ) {
            return true;
        }

        type_name.contains("int")
            || type_name.contains("serial")
            || type_name.contains("float")
            || type_name.contains("double")
            || type_name.contains("real")
            || type_name.contains("numeric")
            || type_name.contains("decimal")
            || type_name.contains("number")
            || type_name.contains("money")
            || type_name.contains("date")
            || type_name.contains("time")
            || type_name.contains("timestamp")
            || type_name.contains("datetime")
            || type_name.contains("year")
    }

    fn is_sql_text_type(type_name: &str, value: &Value) -> bool {
        if matches!(value, Value::Text(_) | Value::ObjectId(_)) {
            return true;
        }

        type_name.contains("text")
            || type_name.contains("char")
            || type_name.contains("string")
            || type_name.contains("clob")
            || type_name.contains("uuid")
            || type_name.contains("citext")
            || type_name.contains("enum")
            || type_name.contains("set")
    }

    fn sql_operator_symbol(operator: FilterOperator) -> &'static str {
        match operator {
            FilterOperator::Eq => "=",
            FilterOperator::NotEq => "<>",
            FilterOperator::Gt => ">",
            FilterOperator::Gte => ">=",
            FilterOperator::Lt => "<",
            FilterOperator::Lte => "<=",
            FilterOperator::Like => "LIKE",
        }
    }

    fn mongo_operator_symbol(operator: FilterOperator) -> &'static str {
        match operator {
            FilterOperator::Eq => "=",
            FilterOperator::NotEq => "!=",
            FilterOperator::Gt => ">",
            FilterOperator::Gte => ">=",
            FilterOperator::Lt => "<",
            FilterOperator::Lte => "<=",
            FilterOperator::Like => "LIKE",
        }
    }

    fn mongo_value_display_preview(value: &Value) -> String {
        match value {
            Value::Null => "null".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Decimal(d) => d.clone(),
            Value::Text(s) => {
                let sanitized = Self::sanitize_for_label(s);
                format!("\"{}\"", sanitized)
            }
            Value::Json(j) => Self::truncate_for_label(&Self::sanitize_for_label(j), 20),
            Value::ObjectId(oid) => {
                format!("ObjectId(\"{}\")", Self::truncate_for_label(oid, 12))
            }
            Value::DateTime(dt) => format!("\"{}\"", dt.to_rfc3339()),
            Value::Date(d) => format!("\"{}\"", d),
            Value::Time(t) => format!("\"{}\"", t),
            Value::Bytes(b) => format!("[{} bytes]", b.len()),
            Value::Array(_) | Value::Document(_) => "...".to_string(),
        }
    }

    fn build_filter_items(
        &self,
        menu: &TableContextMenu,
        backend: Option<FilterBackend>,
        cx: &App,
    ) -> (String, usize, Vec<(String, ContextMenuAction)>, usize) {
        match backend {
            Some(FilterBackend::Sql) => self.build_sql_filter_items(menu, cx),
            Some(FilterBackend::Mongo) => self.build_mongo_filter_items(cx),
            None => (String::new(), 0, Vec::new(), 0),
        }
    }

    fn build_sql_filter_items(
        &self,
        menu: &TableContextMenu,
        cx: &App,
    ) -> (String, usize, Vec<(String, ContextMenuAction)>, usize) {
        let (col_name, col_type_name) = self
            .result
            .columns
            .get(menu.col)
            .map(|column| (column.name.clone(), column.type_name.to_ascii_lowercase()))
            .unwrap_or_default();

        let cell_value = self.resolve_cell_value(menu.row, menu.col, cx);

        let mut items: Vec<(String, ContextMenuAction)> = Vec::new();
        let mut value_ops_count = 0;

        if let Some(ref value) = cell_value {
            let display = cell_value
                .as_ref()
                .map(Self::value_display_preview)
                .unwrap_or_default();
            let short = Self::truncate_for_label(&display, 20);

            let operators = Self::sql_filter_operators(&col_type_name, value);
            value_ops_count = operators.len();

            for operator in operators {
                let op = Self::sql_operator_symbol(operator);
                items.push((
                    format!("{} {} {}", col_name, op, short),
                    ContextMenuAction::FilterByValue(operator),
                ));
            }
        }

        items.push((
            format!("{} IS NULL", col_name),
            ContextMenuAction::FilterIsNull,
        ));
        items.push((
            format!("{} IS NOT NULL", col_name),
            ContextMenuAction::FilterIsNotNull,
        ));
        items.push(("Remove filter".to_string(), ContextMenuAction::RemoveFilter));

        let count = items.len();
        (col_name, count, items, value_ops_count)
    }

    fn build_mongo_filter_items(
        &self,
        cx: &App,
    ) -> (String, usize, Vec<(String, ContextMenuAction)>, usize) {
        let Some((field, ref val)) = self.mongo_filter_field_info(cx) else {
            return (String::new(), 0, Vec::new(), 0);
        };

        let display = Self::mongo_value_display_preview(val);
        let short = Self::truncate_for_label(&display, 20);

        let mut items: Vec<(String, ContextMenuAction)> = Vec::new();
        let operators = Self::mongo_filter_operators(val);
        let value_ops_count = operators.len();

        for operator in operators {
            let op = Self::mongo_operator_symbol(operator);
            items.push((
                format!("{} {} {}", field, op, short),
                ContextMenuAction::FilterByValue(operator),
            ));
        }

        items.push((
            format!("{} IS NULL", field),
            ContextMenuAction::FilterIsNull,
        ));
        items.push((
            format!("{} IS NOT NULL", field),
            ContextMenuAction::FilterIsNotNull,
        ));
        items.push(("Remove filter".to_string(), ContextMenuAction::RemoveFilter));

        let count = items.len();
        (field, count, items, value_ops_count)
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
        let backend = self.filter_backend(cx);
        let is_document_view = self
            .context_menu
            .as_ref()
            .map(|m| m.is_document_view)
            .unwrap_or(false);

        let has_filter = self.has_filter_submenu(backend, is_document_view, cx);
        let has_order = matches!(backend, Some(FilterBackend::Sql)) && !is_document_view;
        let has_generate_sql = !is_document_view;
        let has_copy_query = self.has_copy_query_support();

        // Layout:
        //   [base items]
        //   [sep + Filter trigger]?   (if has_filter)
        //   [Order trigger]?          (if has_order, shares separator with filter)
        //   [sep + GenSQL trigger]?   (if has_generate_sql)
        //   [sep + CopyQuery trigger]?(if has_copy_query)
        let base_items = Self::build_context_menu_items(is_editable, is_document_view);
        let base_count = base_items.len();

        // Filter: sep(1) + filter(1) = 2; Order adds 1 more
        let filter_slots = if has_filter { 2 } else { 0 };
        let order_slots = if has_order { 1 } else { 0 };
        let after_filter_order = base_count + filter_slots + order_slots;

        // GenSQL: sep(1) + trigger(1) = 2
        let gen_sql_slots = if has_generate_sql { 2 } else { 0 };
        let after_gen_sql = after_filter_order + gen_sql_slots;

        // CopyQuery: sep(1) + trigger(1) = 2
        let copy_query_slots = if has_copy_query { 2 } else { 0 };
        let total_count = after_gen_sql + copy_query_slots;

        let filter_trigger_idx = if has_filter {
            Some(base_count + 1) // after separator
        } else {
            None
        };

        let order_trigger_idx = if has_order {
            Some(base_count + filter_slots) // right after filter trigger
        } else {
            None
        };

        let gen_sql_trigger_idx = if has_generate_sql {
            Some(after_filter_order + 1) // after separator
        } else {
            None
        };

        let copy_query_trigger_idx = if has_copy_query {
            Some(after_gen_sql + 1) // after separator
        } else {
            None
        };

        let any_submenu_open = self
            .context_menu
            .as_ref()
            .map(|m| {
                m.sql_submenu_open
                    || m.copy_query_submenu_open
                    || m.filter_submenu_open
                    || m.order_submenu_open
            })
            .unwrap_or(false);

        let filter_submenu_actions: Vec<ContextMenuAction> = self
            .context_menu
            .as_ref()
            .filter(|m| m.filter_submenu_open)
            .map(|m| {
                let (_, _, items, _) = self.build_filter_items(m, backend, cx);
                items.into_iter().map(|(_, action)| action).collect()
            })
            .unwrap_or_default();

        // Determine count of items in the active submenu
        let active_submenu_count = if let Some(menu) = &self.context_menu {
            if menu.filter_submenu_open {
                filter_submenu_actions.len()
            } else if menu.order_submenu_open {
                3 // ASC, DESC, Remove
            } else if menu.sql_submenu_open {
                4 // SELECT WHERE, INSERT, UPDATE, DELETE
            } else if menu.copy_query_submenu_open {
                3 // INSERT, UPDATE, DELETE
            } else {
                0
            }
        } else {
            0
        };

        let is_separator = |idx: usize| -> bool {
            if idx < base_count {
                return base_items.get(idx).map(|i| i.is_separator).unwrap_or(false);
            }

            // Filter separator
            if has_filter && idx == base_count {
                return true;
            }

            // GenSQL separator
            if has_generate_sql && idx == after_filter_order {
                return true;
            }

            // CopyQuery separator
            if has_copy_query && idx == after_gen_sql {
                return true;
            }

            false
        };

        match cmd {
            Command::MenuDown => {
                if let Some(ref mut menu) = self.context_menu {
                    if any_submenu_open {
                        menu.submenu_selected_index =
                            (menu.submenu_selected_index + 1) % active_submenu_count;
                    } else {
                        menu.selected_index = (menu.selected_index + 1) % total_count;
                        while is_separator(menu.selected_index) {
                            menu.selected_index = (menu.selected_index + 1) % total_count;
                        }
                    }
                    cx.notify();
                }
                true
            }
            Command::MenuUp => {
                if let Some(ref mut menu) = self.context_menu {
                    if any_submenu_open {
                        menu.submenu_selected_index = if menu.submenu_selected_index == 0 {
                            active_submenu_count - 1
                        } else {
                            menu.submenu_selected_index - 1
                        };
                    } else {
                        menu.selected_index = if menu.selected_index == 0 {
                            total_count - 1
                        } else {
                            menu.selected_index - 1
                        };
                        while is_separator(menu.selected_index) && menu.selected_index > 0 {
                            menu.selected_index -= 1;
                        }
                    }
                    cx.notify();
                }
                true
            }
            Command::MenuSelect => {
                if let Some(ref mut menu) = self.context_menu {
                    if menu.filter_submenu_open {
                        if let Some(action) =
                            filter_submenu_actions.get(menu.submenu_selected_index).copied()
                        {
                            self.handle_context_menu_action(action, window, cx);
                        }
                    } else if menu.order_submenu_open {
                        let action = match menu.submenu_selected_index {
                            0 => ContextMenuAction::Order(dbflux_core::SortDirection::Ascending),
                            1 => ContextMenuAction::Order(dbflux_core::SortDirection::Descending),
                            _ => ContextMenuAction::RemoveOrdering,
                        };
                        self.handle_context_menu_action(action, window, cx);
                    } else if menu.sql_submenu_open {
                        let action = match menu.submenu_selected_index {
                            0 => ContextMenuAction::GenerateSelectWhere,
                            1 => ContextMenuAction::GenerateInsert,
                            2 => ContextMenuAction::GenerateUpdate,
                            _ => ContextMenuAction::GenerateDelete,
                        };
                        self.handle_context_menu_action(action, window, cx);
                    } else if menu.copy_query_submenu_open {
                        let action = match menu.submenu_selected_index {
                            0 => ContextMenuAction::CopyAsInsert,
                            1 => ContextMenuAction::CopyAsUpdate,
                            _ => ContextMenuAction::CopyAsDelete,
                        };
                        self.handle_context_menu_action(action, window, cx);
                    } else if filter_trigger_idx == Some(menu.selected_index) {
                        menu.filter_submenu_open = true;
                        menu.order_submenu_open = false;
                        menu.sql_submenu_open = false;
                        menu.copy_query_submenu_open = false;
                        menu.submenu_selected_index = 0;
                        cx.notify();
                    } else if order_trigger_idx == Some(menu.selected_index) {
                        menu.order_submenu_open = true;
                        menu.filter_submenu_open = false;
                        menu.sql_submenu_open = false;
                        menu.copy_query_submenu_open = false;
                        menu.submenu_selected_index = 0;
                        cx.notify();
                    } else if gen_sql_trigger_idx == Some(menu.selected_index) {
                        menu.sql_submenu_open = true;
                        menu.filter_submenu_open = false;
                        menu.order_submenu_open = false;
                        menu.copy_query_submenu_open = false;
                        menu.submenu_selected_index = 0;
                        cx.notify();
                    } else if copy_query_trigger_idx == Some(menu.selected_index) {
                        menu.copy_query_submenu_open = true;
                        menu.filter_submenu_open = false;
                        menu.order_submenu_open = false;
                        menu.sql_submenu_open = false;
                        menu.submenu_selected_index = 0;
                        cx.notify();
                    } else if menu.selected_index < base_count
                        && let Some(item) = base_items.get(menu.selected_index)
                        && let Some(action) = item.action
                    {
                        self.handle_context_menu_action(action, window, cx);
                    }
                }
                true
            }
            Command::MenuBack | Command::Cancel => {
                if let Some(ref mut menu) = self.context_menu {
                    if menu.sql_submenu_open
                        || menu.copy_query_submenu_open
                        || menu.filter_submenu_open
                        || menu.order_submenu_open
                    {
                        menu.sql_submenu_open = false;
                        menu.copy_query_submenu_open = false;
                        menu.filter_submenu_open = false;
                        menu.order_submenu_open = false;
                        cx.notify();
                    } else {
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
        if self.result.rows.is_empty()
            && self.result.text_body.is_none()
            && self.result.raw_bytes.is_none()
        {
            cx.toast_error("No results to export", window);
            return;
        }

        let formats = dbflux_export::available_formats(&self.result.shape);

        if formats.len() == 1 {
            self.export_with_format(formats[0], window, cx);
        } else {
            self.export_menu_open = !self.export_menu_open;
            cx.notify();
        }
    }

    pub fn export_with_format(
        &mut self,
        format: ExportFormat,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.export_menu_open = false;

        let result = self.result.clone();
        let base_name = self.export_base_name();
        let extension = format.extension();
        let suggested_name = format!("{}.{}", base_name, extension);
        let format_name = format.name();

        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let file_handle = rfd::AsyncFileDialog::new()
                .set_title(format!("Export as {}", format_name))
                .set_file_name(&suggested_name)
                .add_filter(format_name, &[extension])
                .save_file()
                .await;

            let Some(handle) = file_handle else {
                return;
            };

            let path = handle.path().to_path_buf();

            let export_result = (|| {
                let file = File::create(&path)?;
                let mut writer = BufWriter::new(file);
                dbflux_export::export(&result, format, &mut writer)?;
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

    fn export_base_name(&self) -> String {
        match &self.source {
            DataSource::Table { table, .. } => table.name.clone(),
            DataSource::Collection { collection, .. } => collection.name.clone(),
            DataSource::QueryResult { .. } => {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                format!("result_{}", timestamp)
            }
        }
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

        // -- Filter submenu --
        let backend = self.filter_backend(cx);
        let has_filter = self.has_filter_submenu(backend, is_document_view, cx);
        let has_order = matches!(backend, Some(FilterBackend::Sql)) && !is_document_view;

        if has_filter {
            menu_items.push(
                div()
                    .h(px(1.0))
                    .mx(Spacing::SM)
                    .my(Spacing::XS)
                    .bg(theme.border)
                    .into_any_element(),
            );
            visual_index += 1;

            let filter_submenu_open = menu.filter_submenu_open;
            let submenu_bg = theme.popover;
            let submenu_border = theme.border;
            let submenu_fg = theme.foreground;
            let submenu_hover = theme.secondary;
            let filter_index = visual_index;
            let filter_selected = selected_index == filter_index;
            let submenu_selected_index = menu.submenu_selected_index;

            let (_col_name_display, filter_submenu_count, filter_items, value_ops_count) =
                self.build_filter_items(menu, backend, cx);

            menu_items.push(
                div()
                    .id("filter-trigger")
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
                    .text_color(if filter_selected && !filter_submenu_open {
                        theme.accent_foreground
                    } else {
                        submenu_fg
                    })
                    .when(filter_submenu_open, |d| d.bg(submenu_hover))
                    .when(filter_selected && !filter_submenu_open, |d| {
                        d.bg(theme.accent)
                    })
                    .when(!filter_selected && !filter_submenu_open, |d| {
                        d.hover(|d| d.bg(submenu_hover))
                    })
                    .on_mouse_move(cx.listener(move |this, _, _, cx| {
                        if let Some(ref mut menu) = this.context_menu
                            && menu.selected_index != filter_index
                            && !menu.filter_submenu_open
                        {
                            menu.selected_index = filter_index;
                            cx.notify();
                        }
                    }))
                    .on_click(cx.listener(|this, _, _, cx| {
                        if let Some(ref mut menu) = this.context_menu {
                            menu.filter_submenu_open = !menu.filter_submenu_open;
                            menu.order_submenu_open = false;
                            menu.sql_submenu_open = false;
                            menu.copy_query_submenu_open = false;
                            menu.submenu_selected_index = 0;
                            cx.notify();
                        }
                    }))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .child(svg().path(AppIcon::ListFilter.path()).size_4().text_color(
                                if filter_selected && !filter_submenu_open {
                                    theme.accent_foreground
                                } else {
                                    submenu_fg
                                },
                            ))
                            .child("Filter"),
                    )
                    .child(
                        svg()
                            .path(AppIcon::ChevronRight.path())
                            .size_4()
                            .text_color(if filter_selected && !filter_submenu_open {
                                theme.accent_foreground
                            } else {
                                theme.muted_foreground
                            }),
                    )
                    .when(filter_submenu_open, |d: Stateful<Div>| {
                        let value_section_separator_idx =
                            (value_ops_count > 0).then_some(value_ops_count);
                        let remove_separator_idx = filter_submenu_count.saturating_sub(1);

                        d.child(
                            div()
                                .absolute()
                                .left(px(172.0))
                                .top(px(-4.0))
                                .w(px(280.0))
                                .bg(submenu_bg)
                                .border_1()
                                .border_color(submenu_border)
                                .rounded(Radii::MD)
                                .shadow_lg()
                                .py(Spacing::XS)
                                .occlude()
                                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                    cx.stop_propagation();
                                })
                                .when(value_ops_count > 0, |d| {
                                    d.child(
                                        div()
                                            .px(Spacing::SM)
                                            .py(Spacing::XS)
                                            .text_size(FontSizes::XS)
                                            .text_color(theme.muted_foreground)
                                            .child("Cell value"),
                                    )
                                })
                                .children(
                                    filter_items
                                        .into_iter()
                                        .enumerate()
                                        .flat_map(|(idx, (label, action))| {
                                            let mut elements: Vec<AnyElement> = Vec::new();

                                            // Add separator between value ops and IS NULL section
                                            if value_section_separator_idx == Some(idx) {
                                                elements.push(
                                                    div()
                                                        .h(px(1.0))
                                                        .mx(Spacing::SM)
                                                        .my(Spacing::XS)
                                                        .bg(submenu_border)
                                                        .into_any_element(),
                                                );
                                            }

                                            // Add separator before "Remove filter"
                                            if idx == remove_separator_idx {
                                                elements.push(
                                                    div()
                                                        .h(px(1.0))
                                                        .mx(Spacing::SM)
                                                        .my(Spacing::XS)
                                                        .bg(submenu_border)
                                                        .into_any_element(),
                                                );
                                            }

                                            let is_submenu_selected = idx == submenu_selected_index;
                                            let is_remove =
                                                matches!(action, ContextMenuAction::RemoveFilter);
                                            let label_shared =
                                                SharedString::from(format!("filter-{}", idx));

                                            elements.push(
                                                div()
                                                    .id(label_shared)
                                                    .flex()
                                                    .items_center()
                                                    .gap(Spacing::SM)
                                                    .h(Heights::ROW_COMPACT)
                                                    .px(Spacing::SM)
                                                    .mx(Spacing::XS)
                                                    .rounded(Radii::SM)
                                                    .cursor_pointer()
                                                    .text_size(FontSizes::SM)
                                                    .text_color(if is_remove {
                                                        theme.danger
                                                    } else if is_submenu_selected {
                                                        theme.accent_foreground
                                                    } else {
                                                        submenu_fg
                                                    })
                                                    .when(is_submenu_selected && !is_remove, |d| {
                                                        d.bg(theme.accent)
                                                    })
                                                    .when(is_submenu_selected && is_remove, |d| {
                                                        d.bg(theme.danger.opacity(0.1))
                                                    })
                                                    .when(!is_submenu_selected, |d| {
                                                        d.hover(|d| d.bg(submenu_hover))
                                                    })
                                                    .on_mouse_move(cx.listener(
                                                        move |this, _, _, cx| {
                                                            if let Some(ref mut menu) =
                                                                this.context_menu
                                                                && menu.submenu_selected_index
                                                                    != idx
                                                            {
                                                                menu.submenu_selected_index = idx;
                                                                cx.notify();
                                                            }
                                                        },
                                                    ))
                                                    .on_click(cx.listener(
                                                        move |this, _, window, cx| {
                                                            this.handle_context_menu_action(
                                                                action, window, cx,
                                                            );
                                                        },
                                                    ))
                                                    .child(label.clone())
                                                    .into_any_element(),
                                            );

                                            elements
                                        })
                                        .collect::<Vec<_>>(),
                                ),
                        )
                    })
                    .into_any_element(),
            );
            visual_index += 1;
        }

        // -- Order submenu (SQL table view only) --
        if has_order {
            let submenu_bg = theme.popover;
            let submenu_border = theme.border;
            let submenu_fg = theme.foreground;
            let submenu_hover = theme.secondary;
            let order_submenu_open = menu.order_submenu_open;
            let order_index = visual_index;
            let order_selected = selected_index == order_index;
            let submenu_selected_index = menu.submenu_selected_index;

            let col_name_for_order = self
                .result
                .columns
                .get(menu.col)
                .map(|c| c.name.clone())
                .unwrap_or_default();

            menu_items.push(
                div()
                    .id("order-trigger")
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
                    .text_color(if order_selected && !order_submenu_open {
                        theme.accent_foreground
                    } else {
                        submenu_fg
                    })
                    .when(order_submenu_open, |d| d.bg(submenu_hover))
                    .when(order_selected && !order_submenu_open, |d| {
                        d.bg(theme.accent)
                    })
                    .when(!order_selected && !order_submenu_open, |d| {
                        d.hover(|d| d.bg(submenu_hover))
                    })
                    .on_mouse_move(cx.listener(move |this, _, _, cx| {
                        if let Some(ref mut menu) = this.context_menu
                            && menu.selected_index != order_index
                            && !menu.order_submenu_open
                        {
                            menu.selected_index = order_index;
                            cx.notify();
                        }
                    }))
                    .on_click(cx.listener(|this, _, _, cx| {
                        if let Some(ref mut menu) = this.context_menu {
                            menu.order_submenu_open = !menu.order_submenu_open;
                            menu.filter_submenu_open = false;
                            menu.sql_submenu_open = false;
                            menu.copy_query_submenu_open = false;
                            menu.submenu_selected_index = 0;
                            cx.notify();
                        }
                    }))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .child(svg().path(AppIcon::ArrowUpDown.path()).size_4().text_color(
                                if order_selected && !order_submenu_open {
                                    theme.accent_foreground
                                } else {
                                    submenu_fg
                                },
                            ))
                            .child("Order"),
                    )
                    .child(
                        svg()
                            .path(AppIcon::ChevronRight.path())
                            .size_4()
                            .text_color(if order_selected && !order_submenu_open {
                                theme.accent_foreground
                            } else {
                                theme.muted_foreground
                            }),
                    )
                    .when(order_submenu_open, |d: Stateful<Div>| {
                        let order_items: Vec<(String, ContextMenuAction, AppIcon)> = vec![
                            (
                                format!("{} ASC", col_name_for_order),
                                ContextMenuAction::Order(dbflux_core::SortDirection::Ascending),
                                AppIcon::ArrowUp,
                            ),
                            (
                                format!("{} DESC", col_name_for_order),
                                ContextMenuAction::Order(dbflux_core::SortDirection::Descending),
                                AppIcon::ArrowDown,
                            ),
                            (
                                "Remove ordering".to_string(),
                                ContextMenuAction::RemoveOrdering,
                                AppIcon::X,
                            ),
                        ];

                        d.child(
                            div()
                                .absolute()
                                .left(px(172.0))
                                .top(px(-4.0))
                                .w(px(200.0))
                                .bg(submenu_bg)
                                .border_1()
                                .border_color(submenu_border)
                                .rounded(Radii::MD)
                                .shadow_lg()
                                .py(Spacing::XS)
                                .occlude()
                                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                    cx.stop_propagation();
                                })
                                .children(
                                    order_items
                                        .into_iter()
                                        .enumerate()
                                        .flat_map(|(idx, (label, action, icon))| {
                                            let mut elements: Vec<AnyElement> = Vec::new();

                                            // Separator before "Remove ordering"
                                            if idx == 2 {
                                                elements.push(
                                                    div()
                                                        .h(px(1.0))
                                                        .mx(Spacing::SM)
                                                        .my(Spacing::XS)
                                                        .bg(submenu_border)
                                                        .into_any_element(),
                                                );
                                            }

                                            let is_submenu_selected = idx == submenu_selected_index;
                                            let is_remove =
                                                matches!(action, ContextMenuAction::RemoveOrdering);

                                            elements.push(
                                                div()
                                                    .id(SharedString::from(format!(
                                                        "order-{}",
                                                        idx
                                                    )))
                                                    .flex()
                                                    .items_center()
                                                    .gap(Spacing::SM)
                                                    .h(Heights::ROW_COMPACT)
                                                    .px(Spacing::SM)
                                                    .mx(Spacing::XS)
                                                    .rounded(Radii::SM)
                                                    .cursor_pointer()
                                                    .text_size(FontSizes::SM)
                                                    .text_color(if is_remove {
                                                        theme.danger
                                                    } else if is_submenu_selected {
                                                        theme.accent_foreground
                                                    } else {
                                                        submenu_fg
                                                    })
                                                    .when(is_submenu_selected && !is_remove, |d| {
                                                        d.bg(theme.accent)
                                                    })
                                                    .when(is_submenu_selected && is_remove, |d| {
                                                        d.bg(theme.danger.opacity(0.1))
                                                    })
                                                    .when(!is_submenu_selected, |d| {
                                                        d.hover(|d| d.bg(submenu_hover))
                                                    })
                                                    .on_mouse_move(cx.listener(
                                                        move |this, _, _, cx| {
                                                            if let Some(ref mut menu) =
                                                                this.context_menu
                                                                && menu.submenu_selected_index
                                                                    != idx
                                                            {
                                                                menu.submenu_selected_index = idx;
                                                                cx.notify();
                                                            }
                                                        },
                                                    ))
                                                    .on_click(cx.listener(
                                                        move |this, _, window, cx| {
                                                            this.handle_context_menu_action(
                                                                action, window, cx,
                                                            );
                                                        },
                                                    ))
                                                    .child(
                                                        svg()
                                                            .path(icon.path())
                                                            .size_4()
                                                            .text_color(if is_remove {
                                                                theme.danger
                                                            } else if is_submenu_selected {
                                                                theme.accent_foreground
                                                            } else {
                                                                theme.muted_foreground
                                                            }),
                                                    )
                                                    .child(label)
                                                    .into_any_element(),
                                            );

                                            elements
                                        })
                                        .collect::<Vec<_>>(),
                                ),
                        )
                    })
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
                            menu.copy_query_submenu_open = false;
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

        // -- Copy as Query submenu --
        if self.has_copy_query_support() {
            menu_items.push(
                div()
                    .h(px(1.0))
                    .mx(Spacing::SM)
                    .my(Spacing::XS)
                    .bg(theme.border)
                    .into_any_element(),
            );
            visual_index += 1;

            let copy_query_label = self.copy_query_submenu_label(cx);
            let copy_submenu_open = menu.copy_query_submenu_open;
            let submenu_bg = theme.popover;
            let submenu_border = theme.border;
            let submenu_fg = theme.foreground;
            let submenu_hover = theme.secondary;
            let copy_query_index = visual_index;
            let copy_query_selected = selected_index == copy_query_index;
            let submenu_selected_index = menu.submenu_selected_index;

            menu_items.push(
                div()
                    .id("copy-query-trigger")
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
                    .text_color(if copy_query_selected && !copy_submenu_open {
                        theme.accent_foreground
                    } else {
                        submenu_fg
                    })
                    .when(copy_submenu_open, |d| d.bg(submenu_hover))
                    .when(copy_query_selected && !copy_submenu_open, |d| {
                        d.bg(theme.accent)
                    })
                    .when(!copy_query_selected && !copy_submenu_open, |d| {
                        d.hover(|d| d.bg(submenu_hover))
                    })
                    .on_mouse_move(cx.listener(move |this, _, _, cx| {
                        if let Some(ref mut menu) = this.context_menu
                            && menu.selected_index != copy_query_index
                            && !menu.copy_query_submenu_open
                        {
                            menu.selected_index = copy_query_index;
                            cx.notify();
                        }
                    }))
                    .on_click(cx.listener(|this, _, _, cx| {
                        if let Some(ref mut menu) = this.context_menu {
                            menu.copy_query_submenu_open = !menu.copy_query_submenu_open;
                            menu.sql_submenu_open = false;
                            menu.submenu_selected_index = 0;
                            cx.notify();
                        }
                    }))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .child(svg().path(AppIcon::Columns.path()).size_4().text_color(
                                if copy_query_selected && !copy_submenu_open {
                                    theme.accent_foreground
                                } else {
                                    submenu_fg
                                },
                            ))
                            .child(copy_query_label),
                    )
                    .child(
                        svg()
                            .path(AppIcon::ChevronRight.path())
                            .size_4()
                            .text_color(if copy_query_selected && !copy_submenu_open {
                                theme.accent_foreground
                            } else {
                                theme.muted_foreground
                            }),
                    )
                    .when(copy_submenu_open, |d: Stateful<Div>| {
                        d.child(
                            div()
                                .absolute()
                                .left(px(172.0))
                                .top(px(-4.0))
                                .w(px(140.0))
                                .bg(submenu_bg)
                                .border_1()
                                .border_color(submenu_border)
                                .rounded(Radii::MD)
                                .shadow_lg()
                                .py(Spacing::XS)
                                .occlude()
                                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                    cx.stop_propagation();
                                })
                                .children(
                                    [
                                        ("INSERT", ContextMenuAction::CopyAsInsert),
                                        ("UPDATE", ContextMenuAction::CopyAsUpdate),
                                        ("DELETE", ContextMenuAction::CopyAsDelete),
                                    ]
                                    .into_iter()
                                    .enumerate()
                                    .map(|(idx, (label, action))| {
                                        let is_submenu_selected = idx == submenu_selected_index;
                                        div()
                                            .id(SharedString::from(format!("copy-{}", label)))
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
                                                    .path(AppIcon::Columns.path())
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
        let backend = self.filter_backend(cx);

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
            ContextMenuAction::CopyAsInsert
            | ContextMenuAction::CopyAsUpdate
            | ContextMenuAction::CopyAsDelete => {
                self.handle_copy_as_query(menu.row, action, cx);
            }
            ContextMenuAction::FilterByValue(op) => match backend {
                Some(FilterBackend::Mongo) => {
                    self.handle_mongo_filter_by_value(
                        &menu.doc_field_path,
                        &menu.doc_field_value,
                        op,
                        window,
                        cx,
                    );
                }
                _ => {
                    self.handle_filter_by_value(menu.row, menu.col, op, window, cx);
                }
            },
            ContextMenuAction::FilterIsNull => match backend {
                Some(FilterBackend::Mongo) => {
                    self.handle_mongo_filter_null(&menu.doc_field_path, false, window, cx);
                }
                _ => {
                    self.handle_filter_is_null(menu.col, false, window, cx);
                }
            },
            ContextMenuAction::FilterIsNotNull => match backend {
                Some(FilterBackend::Mongo) => {
                    self.handle_mongo_filter_null(&menu.doc_field_path, true, window, cx);
                }
                _ => {
                    self.handle_filter_is_null(menu.col, true, window, cx);
                }
            },
            ContextMenuAction::RemoveFilter => {
                self.handle_remove_filter(window, cx);
            }
            ContextMenuAction::Order(direction) => {
                self.handle_sort_request(menu.col, direction, cx);
            }
            ContextMenuAction::RemoveOrdering => {
                self.handle_sort_clear(cx);
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

    // === Filter / Order from context menu ===

    /// Resolves the original `Value` for a cell from the result set.
    fn resolve_cell_value(&self, visual_row: usize, col: usize, cx: &App) -> Option<Value> {
        use crate::ui::components::data_table::model::VisualRowSource;

        let table_state = self.table_state.as_ref()?;
        let ts = table_state.read(cx);
        let buffer = ts.edit_buffer();
        let visual_order = buffer.compute_visual_order();

        match visual_order.get(visual_row).copied() {
            Some(VisualRowSource::Base(base_idx)) => self
                .result
                .rows
                .get(base_idx)
                .and_then(|r| r.get(col).cloned()),
            Some(VisualRowSource::Insert(insert_idx)) => buffer
                .get_pending_insert_by_idx(insert_idx)
                .and_then(|cells| cells.get(col).map(|c| self.cell_value_to_value(c))),
            None => None,
        }
    }

    /// Appends `expr` to the WHERE filter input and refreshes.
    /// Wraps with parentheses — `(old) AND (new)` — to avoid precedence bugs.
    fn apply_filter_expression(&mut self, expr: &str, window: &mut Window, cx: &mut Context<Self>) {
        let current = self.filter_input.read(cx).value().to_string();

        let new_filter = if current.trim().is_empty() {
            expr.to_string()
        } else {
            format!("({}) AND ({})", current.trim(), expr)
        };

        self.filter_input
            .update(cx, |state, cx| state.set_value(&new_filter, window, cx));
        self.refresh(window, cx);
    }

    fn handle_filter_by_value(
        &mut self,
        visual_row: usize,
        col: usize,
        operator: FilterOperator,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let profile_id = match &self.source {
            DataSource::Table { profile_id, .. } => *profile_id,
            _ => return,
        };

        let conn = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.clone());

        let Some(conn) = conn else { return };
        let dialect = conn.dialect();
        let is_postgres = conn.metadata().id == "postgres";

        let (col_name, col_type_name) = match self.result.columns.get(col) {
            Some(c) => (
                dialect.quote_identifier(&c.name),
                c.type_name.to_ascii_lowercase(),
            ),
            None => return,
        };

        let cell_value = match self.resolve_cell_value(visual_row, col, cx) {
            Some(v) => v,
            None => return,
        };

        let literal = dialect.value_to_literal(&cell_value);

        let op_str = Self::sql_operator_symbol(operator);

        let expr = if operator == FilterOperator::Like {
            let raw = match &cell_value {
                Value::Text(text) => text.clone(),
                Value::ObjectId(id) => id.clone(),
                _ => return,
            };

            let needs_escape = raw.contains('\\') || raw.contains('%') || raw.contains('_');

            let pattern_value = if needs_escape {
                let escaped = raw
                    .replace('\\', "\\\\")
                    .replace('%', "\\%")
                    .replace('_', "\\_");
                format!("%{}%", escaped)
            } else {
                format!("%{}%", raw)
            };

            let pattern_literal = dialect.value_to_literal(&Value::Text(pattern_value));

            let like_column = if is_postgres && col_type_name == "uuid" {
                format!("({})::text", col_name)
            } else {
                col_name.clone()
            };

            if needs_escape {
                format!("{} LIKE {} ESCAPE '\\'", like_column, pattern_literal)
            } else {
                format!("{} LIKE {}", like_column, pattern_literal)
            }
        } else if is_postgres
            && matches!(cell_value, Value::Json(_))
            && col_type_name.contains("json")
        {
            format!("({})::jsonb {} ({})", col_name, op_str, literal)
        } else {
            format!("{} {} {}", col_name, op_str, literal)
        };

        self.apply_filter_expression(&expr, window, cx);
    }

    fn handle_filter_is_null(
        &mut self,
        col: usize,
        is_not_null: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let profile_id = match &self.source {
            DataSource::Table { profile_id, .. } => *profile_id,
            _ => return,
        };

        let conn = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.clone());

        let Some(conn) = conn else { return };
        let dialect = conn.dialect();

        let col_name = match self.result.columns.get(col) {
            Some(c) => dialect.quote_identifier(&c.name),
            None => return,
        };

        let expr = if is_not_null {
            format!("{} IS NOT NULL", col_name)
        } else {
            format!("{} IS NULL", col_name)
        };

        self.apply_filter_expression(&expr, window, cx);
    }

    fn handle_remove_filter(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.filter_input
            .update(cx, |state, cx| state.set_value("", window, cx));
        self.refresh(window, cx);
    }

    // === MongoDB filter handlers ===

    fn handle_mongo_filter_by_value(
        &mut self,
        field_path: &Option<Vec<String>>,
        field_value: &Option<crate::ui::components::document_tree::NodeValue>,
        operator: FilterOperator,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::components::document_tree::NodeValue;

        let Some(path) = field_path else { return };
        if path.is_empty() {
            return;
        }
        let field_dot = path.join(".");

        let scalar = match field_value {
            Some(NodeValue::Scalar(v)) => v,
            _ => return,
        };

        let json_val = value_to_json(scalar);

        let filter_obj = match operator {
            FilterOperator::Eq => serde_json::json!({ &field_dot: json_val }),
            FilterOperator::NotEq => serde_json::json!({ &field_dot: { "$ne": json_val } }),
            FilterOperator::Gt => serde_json::json!({ &field_dot: { "$gt": json_val } }),
            FilterOperator::Gte => serde_json::json!({ &field_dot: { "$gte": json_val } }),
            FilterOperator::Lt => serde_json::json!({ &field_dot: { "$lt": json_val } }),
            FilterOperator::Lte => serde_json::json!({ &field_dot: { "$lte": json_val } }),
            FilterOperator::Like => return,
        };

        self.apply_mongo_filter(&filter_obj, window, cx);
    }

    fn handle_mongo_filter_null(
        &mut self,
        field_path: &Option<Vec<String>>,
        is_not_null: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(path) = field_path else { return };
        if path.is_empty() {
            return;
        }
        let field_dot = path.join(".");

        let filter_obj = if is_not_null {
            serde_json::json!({
                "$and": [
                    { &field_dot: { "$ne": null } },
                    { &field_dot: { "$exists": true } }
                ]
            })
        } else {
            serde_json::json!({
                "$and": [
                    { &field_dot: null },
                    { &field_dot: { "$exists": true } }
                ]
            })
        };

        self.apply_mongo_filter(&filter_obj, window, cx);
    }

    fn apply_mongo_filter(
        &mut self,
        new_filter: &serde_json::Value,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let current = self.filter_input.read(cx).value().to_string();
        let current_trimmed = current.trim();

        let composed = if current_trimmed.is_empty() {
            new_filter.clone()
        } else {
            match serde_json::from_str::<serde_json::Value>(current_trimmed) {
                Ok(existing) => Self::compose_mongo_and(&existing, new_filter),
                Err(_) => new_filter.clone(),
            }
        };

        let serialized = serde_json::to_string(&composed).unwrap_or_default();

        self.filter_input
            .update(cx, |state, cx| state.set_value(&serialized, window, cx));
        self.refresh(window, cx);
    }

    fn compose_mongo_and(
        existing: &serde_json::Value,
        new_clause: &serde_json::Value,
    ) -> serde_json::Value {
        if let Some(obj) = existing.as_object()
            && obj.len() == 1
            && let Some(existing_and) = obj.get("$and")
            && let Some(arr) = existing_and.as_array()
        {
            let mut clauses = arr.clone();
            clauses.push(new_clause.clone());
            return serde_json::json!({ "$and": clauses });
        }

        serde_json::json!({ "$and": [existing, new_clause] })
    }

    fn sanitize_for_label(s: &str) -> String {
        s.chars()
            .map(|c| if c.is_ascii_control() { ' ' } else { c })
            .collect()
    }

    fn truncate_for_label(s: &str, max_len: usize) -> String {
        if s.len() <= max_len {
            s.to_string()
        } else {
            let truncated: String = s.chars().take(max_len).collect();
            format!("{}...", truncated)
        }
    }

    fn value_display_preview(value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Decimal(s) => s.clone(),
            Value::Text(s) => format!("'{}'", Self::sanitize_for_label(s)),
            Value::Json(s) => format!("'{}'", Self::sanitize_for_label(s)),
            Value::ObjectId(id) => format!("'{}'", id),
            Value::DateTime(dt) => format!("'{}'", dt.format("%Y-%m-%d %H:%M:%S")),
            Value::Date(d) => format!("'{}'", d.format("%Y-%m-%d")),
            Value::Time(t) => format!("'{}'", t.format("%H:%M:%S")),
            Value::Bytes(b) => format!("[{} bytes]", b.len()),
            Value::Array(_) | Value::Document(_) => "'...'".to_string(),
        }
    }

    /// NULL, bytes, complex structures, NaN, and Infinity don't support value operators;
    /// only IS NULL / IS NOT NULL applies.
    fn is_value_filterable(value: &Value) -> bool {
        match value {
            Value::Null | Value::Bytes(_) | Value::Array(_) | Value::Document(_) => false,
            Value::Float(f) if f.is_nan() || f.is_infinite() => false,
            _ => true,
        }
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

    // -- Copy as Query --

    fn copy_query_submenu_label(&self, cx: &App) -> &'static str {
        let profile_id = match &self.source {
            DataSource::Table { profile_id, .. } => profile_id,
            DataSource::Collection { profile_id, .. } => profile_id,
            DataSource::QueryResult { .. } => return "Copy as Query",
        };

        let language = self
            .app_state
            .read(cx)
            .connections()
            .get(profile_id)
            .map(|c| c.connection.metadata().query_language);

        match language {
            Some(dbflux_core::QueryLanguage::Sql) => "Copy as SQL",
            Some(dbflux_core::QueryLanguage::MongoQuery) => "Copy as Query",
            Some(dbflux_core::QueryLanguage::RedisCommands) => "Copy as Command",
            _ => "Copy as Query",
        }
    }

    fn has_copy_query_support(&self) -> bool {
        matches!(
            self.source,
            DataSource::Table { .. } | DataSource::Collection { .. }
        )
    }

    pub(super) fn handle_copy_as_query(
        &mut self,
        visual_row: usize,
        action: ContextMenuAction,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::components::data_table::model::VisualRowSource;

        let profile_id = match &self.source {
            DataSource::Table { profile_id, .. } => *profile_id,
            DataSource::Collection { profile_id, .. } => *profile_id,
            DataSource::QueryResult { .. } => return,
        };

        let conn = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|c| c.connection.clone());

        let Some(conn) = conn else {
            return;
        };

        let Some(generator) = conn.query_generator() else {
            return;
        };

        let mutation = match &self.source {
            DataSource::Table { table, .. } => {
                self.build_sql_mutation(visual_row, table, action, cx)
            }
            DataSource::Collection { collection, .. } => {
                self.build_document_mutation(visual_row, collection, action, cx)
            }
            DataSource::QueryResult { .. } => None,
        };

        let Some(mutation) = mutation else {
            return;
        };

        if let Some(generated) = generator.generate_mutation(&mutation) {
            cx.write_to_clipboard(ClipboardItem::new_string(generated.text));
        }
    }

    fn build_sql_mutation(
        &self,
        visual_row: usize,
        table: &dbflux_core::TableRef,
        action: ContextMenuAction,
        cx: &App,
    ) -> Option<MutationRequest> {
        use crate::ui::components::data_table::model::VisualRowSource;

        let table_state = self.table_state.as_ref()?;
        let state = table_state.read(cx);
        let model = state.model();
        let buffer = state.edit_buffer();
        let visual_order = buffer.compute_visual_order();

        let col_names: Vec<String> = self.result.columns.iter().map(|c| c.name.clone()).collect();

        let row_values: Vec<Value> = match visual_order.get(visual_row).copied() {
            Some(VisualRowSource::Base(base_idx)) => {
                self.result.rows.get(base_idx).cloned().unwrap_or_default()
            }
            Some(VisualRowSource::Insert(insert_idx)) => buffer
                .get_pending_insert_by_idx(insert_idx)
                .map(|cells| cells.iter().map(|c| self.cell_value_to_value(c)).collect())
                .unwrap_or_default(),
            None => return None,
        };

        if row_values.is_empty() || col_names.len() != row_values.len() {
            return None;
        }

        let pk_indices = state.pk_columns();

        match action {
            ContextMenuAction::CopyAsInsert => {
                let insert = RowInsert::new(
                    table.name.clone(),
                    table.schema.clone(),
                    col_names,
                    row_values,
                );
                Some(MutationRequest::SqlInsert(insert))
            }

            ContextMenuAction::CopyAsUpdate => {
                if pk_indices.is_empty() {
                    return None;
                }

                let pk_columns: Vec<String> = pk_indices
                    .iter()
                    .filter_map(|&idx| model.columns.get(idx).map(|c| c.title.to_string()))
                    .collect();

                let pk_values: Vec<Value> = pk_indices
                    .iter()
                    .filter_map(|&idx| row_values.get(idx).cloned())
                    .collect();

                let identity = RowIdentity::new(pk_columns, pk_values);

                let changes: Vec<(String, Value)> = col_names
                    .into_iter()
                    .zip(row_values)
                    .enumerate()
                    .filter(|(idx, _)| !pk_indices.contains(idx))
                    .map(|(_, pair)| pair)
                    .collect();

                let patch =
                    RowPatch::new(identity, table.name.clone(), table.schema.clone(), changes);
                Some(MutationRequest::SqlUpdate(patch))
            }

            ContextMenuAction::CopyAsDelete => {
                if pk_indices.is_empty() {
                    return None;
                }

                let pk_columns: Vec<String> = pk_indices
                    .iter()
                    .filter_map(|&idx| model.columns.get(idx).map(|c| c.title.to_string()))
                    .collect();

                let pk_values: Vec<Value> = pk_indices
                    .iter()
                    .filter_map(|&idx| row_values.get(idx).cloned())
                    .collect();

                let identity = RowIdentity::new(pk_columns, pk_values);
                let delete = RowDelete::new(identity, table.name.clone(), table.schema.clone());
                Some(MutationRequest::SqlDelete(delete))
            }

            _ => None,
        }
    }

    fn build_document_mutation(
        &self,
        visual_row: usize,
        collection: &dbflux_core::CollectionRef,
        action: ContextMenuAction,
        cx: &App,
    ) -> Option<MutationRequest> {
        use crate::ui::components::data_table::model::VisualRowSource;

        let table_state = self.table_state.as_ref()?;
        let state = table_state.read(cx);
        let buffer = state.edit_buffer();
        let visual_order = buffer.compute_visual_order();

        let row_values: Vec<Value> = match visual_order.get(visual_row).copied() {
            Some(VisualRowSource::Base(base_idx)) => {
                self.result.rows.get(base_idx).cloned().unwrap_or_default()
            }
            Some(VisualRowSource::Insert(insert_idx)) => buffer
                .get_pending_insert_by_idx(insert_idx)
                .map(|cells| cells.iter().map(|c| self.cell_value_to_value(c)).collect())
                .unwrap_or_default(),
            None => return None,
        };

        if row_values.is_empty() {
            return None;
        }

        let id_col_idx = self
            .result
            .columns
            .iter()
            .position(|c| c.name == "_id")
            .unwrap_or(0);

        let id_value = row_values.get(id_col_idx).cloned().unwrap_or(Value::Null);

        let filter = match &id_value {
            Value::ObjectId(oid) => DocumentFilter::new(serde_json::json!({"_id": {"$oid": oid}})),
            Value::Text(s) => DocumentFilter::new(serde_json::json!({"_id": s})),
            _ => return None,
        };

        match action {
            ContextMenuAction::CopyAsInsert => {
                let mut doc = serde_json::Map::new();
                for (col_idx, val) in row_values.iter().enumerate() {
                    if let Some(col) = self.result.columns.get(col_idx)
                        && !matches!(val, Value::Null)
                    {
                        doc.insert(col.name.clone(), value_to_json(val));
                    }
                }

                let insert = DocumentInsert::one(collection.name.clone(), doc.into())
                    .with_database(collection.database.clone());
                Some(MutationRequest::DocumentInsert(insert))
            }

            ContextMenuAction::CopyAsUpdate => {
                let mut set_fields = serde_json::Map::new();
                for (col_idx, val) in row_values.iter().enumerate() {
                    if col_idx == id_col_idx {
                        continue;
                    }
                    if let Some(col) = self.result.columns.get(col_idx) {
                        set_fields.insert(col.name.clone(), value_to_json(val));
                    }
                }

                let update_doc = serde_json::json!({"$set": set_fields});
                let update = DocumentUpdate::new(collection.name.clone(), filter, update_doc)
                    .with_database(collection.database.clone());
                Some(MutationRequest::DocumentUpdate(update))
            }

            ContextMenuAction::CopyAsDelete => {
                let delete = DocumentDelete::new(collection.name.clone(), filter)
                    .with_database(collection.database.clone());
                Some(MutationRequest::DocumentDelete(delete))
            }

            _ => None,
        }
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
            CellKind::Json(s) => Value::Json(s.to_string()),
            CellKind::Bytes(len) => Value::Bytes(vec![0u8; *len]),
            CellKind::AutoGenerated(expr) => Value::Text(format!("DEFAULT({})", expr)),
        }
    }
}
