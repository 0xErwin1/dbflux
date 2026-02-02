use crate::keymap::ContextId;
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::TableInfo;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::checkbox::Checkbox;
use gpui_component::input::{Input, InputState};
use uuid::Uuid;

/// Type of SQL statement to generate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlGenerationType {
    SelectWhere,
    Insert,
    Update,
    Delete,
}

impl SqlGenerationType {
    pub fn label(&self) -> &'static str {
        match self {
            SqlGenerationType::SelectWhere => "SELECT WHERE",
            SqlGenerationType::Insert => "INSERT",
            SqlGenerationType::Update => "UPDATE",
            SqlGenerationType::Delete => "DELETE",
        }
    }

    /// Convert from driver generator_id to SqlGenerationType.
    /// Returns None for generator types we don't support in the preview modal.
    pub fn from_generator_id(id: &str) -> Option<Self> {
        match id {
            "select_star" => Some(SqlGenerationType::SelectWhere),
            "insert" => Some(SqlGenerationType::Insert),
            "update" => Some(SqlGenerationType::Update),
            "delete" => Some(SqlGenerationType::Delete),
            _ => None,
        }
    }
}

/// Settings for SQL generation.
#[derive(Clone)]
pub struct SqlPreviewSettings {
    pub use_fully_qualified_names: bool,
    pub compact_sql: bool,
}

impl Default for SqlPreviewSettings {
    fn default() -> Self {
        Self {
            use_fully_qualified_names: true,
            compact_sql: false,
        }
    }
}

/// Context for SQL generation - where the request came from.
#[derive(Clone)]
#[allow(dead_code)]
pub enum SqlPreviewContext {
    /// From data table: row data with values
    DataTableRow {
        profile_id: Uuid,
        schema_name: Option<String>,
        table_name: String,
        column_names: Vec<String>,
        row_values: Vec<String>,
        pk_indices: Vec<usize>,
    },
    /// From sidebar: table metadata
    SidebarTable {
        profile_id: Uuid,
        table_info: TableInfo,
    },
}

#[allow(dead_code)]
impl SqlPreviewContext {
    pub fn profile_id(&self) -> Uuid {
        match self {
            SqlPreviewContext::DataTableRow { profile_id, .. } => *profile_id,
            SqlPreviewContext::SidebarTable { profile_id, .. } => *profile_id,
        }
    }

    pub fn table_name(&self) -> &str {
        match self {
            SqlPreviewContext::DataTableRow { table_name, .. } => table_name,
            SqlPreviewContext::SidebarTable { table_info, .. } => &table_info.name,
        }
    }

    pub fn schema_name(&self) -> Option<&str> {
        match self {
            SqlPreviewContext::DataTableRow { schema_name, .. } => schema_name.as_deref(),
            SqlPreviewContext::SidebarTable { table_info, .. } => table_info.schema.as_deref(),
        }
    }
}

/// Modal for previewing and copying generated SQL.
pub struct SqlPreviewModal {
    visible: bool,
    context: Option<SqlPreviewContext>,
    generation_type: SqlGenerationType,
    settings: SqlPreviewSettings,
    sql_display: Entity<InputState>,
    generated_sql: String,
    focus_handle: FocusHandle,
}

impl SqlPreviewModal {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let sql_display = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor("sql")
                .line_number(true)
                .soft_wrap(true)
        });

        Self {
            visible: false,
            context: None,
            generation_type: SqlGenerationType::SelectWhere,
            settings: SqlPreviewSettings::default(),
            sql_display,
            generated_sql: String::new(),
            focus_handle: cx.focus_handle(),
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    /// Open the modal with the given context and generation type.
    pub fn open(
        &mut self,
        context: SqlPreviewContext,
        generation_type: SqlGenerationType,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.context = Some(context);
        self.generation_type = generation_type;
        self.visible = true;
        self.regenerate_sql(window, cx);
        self.focus_handle.focus(window);
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.context = None;
        self.generated_sql.clear();
        cx.notify();
    }

    fn regenerate_sql(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(context) = &self.context else {
            return;
        };

        let sql = match context {
            SqlPreviewContext::DataTableRow {
                schema_name,
                table_name,
                column_names,
                row_values,
                pk_indices,
                ..
            } => self.generate_from_row_data(
                schema_name.as_deref(),
                table_name,
                column_names,
                row_values,
                pk_indices,
            ),
            SqlPreviewContext::SidebarTable { table_info, .. } => {
                self.generate_from_table_info(table_info)
            }
        };

        self.generated_sql = sql.clone();
        self.sql_display.update(cx, |state, cx| {
            state.set_value(&sql, window, cx);
        });
    }

    fn generate_from_row_data(
        &self,
        schema_name: Option<&str>,
        table_name: &str,
        column_names: &[String],
        row_values: &[String],
        pk_indices: &[usize],
    ) -> String {
        let table_ref = self.build_table_reference(schema_name, table_name);
        let separator = if self.settings.compact_sql {
            " "
        } else {
            "\n    "
        };
        let newline = if self.settings.compact_sql { " " } else { "\n" };

        match self.generation_type {
            SqlGenerationType::SelectWhere => {
                let where_clause = self.build_where_clause(column_names, row_values, pk_indices);
                if self.settings.compact_sql {
                    format!("SELECT * FROM {} WHERE {};", table_ref, where_clause)
                } else {
                    format!("SELECT *\nFROM {}\nWHERE {};", table_ref, where_clause)
                }
            }

            SqlGenerationType::Insert => {
                let cols_str = column_names.join(", ");
                let vals_str = row_values.join(", ");
                if self.settings.compact_sql {
                    format!(
                        "INSERT INTO {} ({}) VALUES ({});",
                        table_ref, cols_str, vals_str
                    )
                } else {
                    format!(
                        "INSERT INTO {} ({}){}VALUES ({});",
                        table_ref, cols_str, newline, vals_str
                    )
                }
            }

            SqlGenerationType::Update => {
                let set_parts: Vec<String> = column_names
                    .iter()
                    .zip(row_values.iter())
                    .map(|(col, val)| format!("{} = {}", col, val))
                    .collect();
                let set_clause = set_parts.join(&format!(",{}", separator));
                let where_clause = self.build_where_clause(column_names, row_values, pk_indices);

                if self.settings.compact_sql {
                    format!(
                        "UPDATE {} SET {} WHERE {};",
                        table_ref, set_clause, where_clause
                    )
                } else {
                    format!(
                        "UPDATE {}\nSET {}\nWHERE {};",
                        table_ref, set_clause, where_clause
                    )
                }
            }

            SqlGenerationType::Delete => {
                let where_clause = self.build_where_clause(column_names, row_values, pk_indices);
                if self.settings.compact_sql {
                    format!("DELETE FROM {} WHERE {};", table_ref, where_clause)
                } else {
                    format!("DELETE FROM {}\nWHERE {};", table_ref, where_clause)
                }
            }
        }
    }

    fn generate_from_table_info(&self, table_info: &TableInfo) -> String {
        let table_ref = self.build_table_reference(table_info.schema.as_deref(), &table_info.name);

        let columns: Vec<&str> = table_info
            .columns
            .as_ref()
            .map(|cols| cols.iter().map(|c| c.name.as_str()).collect())
            .unwrap_or_default();

        let pk_columns: Vec<&str> = table_info
            .columns
            .as_ref()
            .map(|cols| {
                cols.iter()
                    .filter(|c| c.is_primary_key)
                    .map(|c| c.name.as_str())
                    .collect()
            })
            .unwrap_or_default();

        let separator = if self.settings.compact_sql {
            " "
        } else {
            "\n    "
        };
        let newline = if self.settings.compact_sql { " " } else { "\n" };

        match self.generation_type {
            SqlGenerationType::SelectWhere => {
                let cols_str = if columns.is_empty() {
                    "*".to_string()
                } else if self.settings.compact_sql {
                    columns.join(", ")
                } else {
                    columns.join(&format!(",{}", separator))
                };

                let where_cols = if pk_columns.is_empty() {
                    if columns.is_empty() {
                        vec!["id"]
                    } else {
                        vec![columns[0]]
                    }
                } else {
                    pk_columns.clone()
                };

                let where_clause = where_cols
                    .iter()
                    .map(|c| format!("{} = ?", c))
                    .collect::<Vec<_>>()
                    .join(" AND ");

                if self.settings.compact_sql {
                    format!(
                        "SELECT {} FROM {} WHERE {};",
                        cols_str, table_ref, where_clause
                    )
                } else {
                    format!(
                        "SELECT {}{}\nFROM {}\nWHERE {};",
                        separator, cols_str, table_ref, where_clause
                    )
                }
            }

            SqlGenerationType::Insert => {
                let cols_str = if columns.is_empty() {
                    "column1, column2".to_string()
                } else {
                    columns.join(", ")
                };

                let vals_str = if columns.is_empty() {
                    "?, ?".to_string()
                } else {
                    vec!["?"; columns.len()].join(", ")
                };

                if self.settings.compact_sql {
                    format!(
                        "INSERT INTO {} ({}) VALUES ({});",
                        table_ref, cols_str, vals_str
                    )
                } else {
                    format!(
                        "INSERT INTO {} ({}){}VALUES ({});",
                        table_ref, cols_str, newline, vals_str
                    )
                }
            }

            SqlGenerationType::Update => {
                let set_parts: Vec<String> = if columns.is_empty() {
                    vec!["column1 = ?".to_string(), "column2 = ?".to_string()]
                } else {
                    columns.iter().map(|c| format!("{} = ?", c)).collect()
                };
                let set_clause = set_parts.join(&format!(",{}", separator));

                let where_cols = if pk_columns.is_empty() {
                    if columns.is_empty() {
                        vec!["id"]
                    } else {
                        vec![columns[0]]
                    }
                } else {
                    pk_columns.clone()
                };

                let where_clause = where_cols
                    .iter()
                    .map(|c| format!("{} = ?", c))
                    .collect::<Vec<_>>()
                    .join(" AND ");

                if self.settings.compact_sql {
                    format!(
                        "UPDATE {} SET {} WHERE {};",
                        table_ref, set_clause, where_clause
                    )
                } else {
                    format!(
                        "UPDATE {}\nSET {}{}\nWHERE {};",
                        table_ref, separator, set_clause, where_clause
                    )
                }
            }

            SqlGenerationType::Delete => {
                let where_cols = if pk_columns.is_empty() {
                    if columns.is_empty() {
                        vec!["id"]
                    } else {
                        vec![columns[0]]
                    }
                } else {
                    pk_columns
                };

                let where_clause = where_cols
                    .iter()
                    .map(|c| format!("{} = ?", c))
                    .collect::<Vec<_>>()
                    .join(" AND ");

                if self.settings.compact_sql {
                    format!("DELETE FROM {} WHERE {};", table_ref, where_clause)
                } else {
                    format!("DELETE FROM {}\nWHERE {};", table_ref, where_clause)
                }
            }
        }
    }

    fn build_table_reference(&self, schema: Option<&str>, table: &str) -> String {
        if self.settings.use_fully_qualified_names
            && let Some(schema) = schema
        {
            return format!("{}.{}", schema, table);
        }
        table.to_string()
    }

    fn build_where_clause(
        &self,
        column_names: &[String],
        row_values: &[String],
        pk_indices: &[usize],
    ) -> String {
        let indices: Vec<usize> = if pk_indices.is_empty() {
            (0..column_names.len()).collect()
        } else {
            pk_indices.to_vec()
        };

        let conditions: Vec<String> = indices
            .iter()
            .filter_map(|&idx| {
                let col = column_names.get(idx)?;
                let val = row_values.get(idx)?;
                if val == "NULL" {
                    Some(format!("{} IS NULL", col))
                } else {
                    Some(format!("{} = {}", col, val))
                }
            })
            .collect();

        if conditions.is_empty() {
            "1=1".to_string()
        } else {
            conditions.join(" AND ")
        }
    }

    fn copy_to_clipboard(&self, cx: &mut Context<Self>) {
        if !self.generated_sql.is_empty() {
            cx.write_to_clipboard(ClipboardItem::new_string(self.generated_sql.clone()));
        }
    }

    fn toggle_fully_qualified(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.settings.use_fully_qualified_names = !self.settings.use_fully_qualified_names;
        self.regenerate_sql(window, cx);
    }

    fn toggle_compact(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.settings.compact_sql = !self.settings.compact_sql;
        self.regenerate_sql(window, cx);
    }
}

impl Render for SqlPreviewModal {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let theme = cx.theme();
        let sql_display = self.sql_display.clone();
        let generation_type = self.generation_type;
        let use_fqn = self.settings.use_fully_qualified_names;
        let compact = self.settings.compact_sql;

        div()
            .id("sql-preview-modal")
            .key_context(ContextId::SqlPreviewModal.as_gpui_context())
            .track_focus(&self.focus_handle)
            .absolute()
            .inset_0()
            .bg(gpui::black().opacity(0.5))
            .flex()
            .justify_center()
            .items_start()
            .pt(px(80.0))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.close(cx);
                }),
            )
            .on_action(cx.listener(|this, _: &crate::keymap::Cancel, _, cx| {
                this.close(cx);
            }))
            .child(
                div()
                    .w(px(1000.0))
                    .max_h(px(800.0))
                    .bg(theme.background)
                    .border_1()
                    .border_color(theme.border)
                    .rounded(Radii::LG)
                    .shadow_lg()
                    .overflow_hidden()
                    .flex()
                    .flex_col()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| {
                        cx.stop_propagation();
                    })
                    // Header
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .px(Spacing::MD)
                            .py(Spacing::SM)
                            .border_b_1()
                            .border_color(theme.border)
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::SM)
                                    .child(
                                        svg()
                                            .path(AppIcon::Code.path())
                                            .size_4()
                                            .text_color(theme.primary),
                                    )
                                    .child(
                                        div()
                                            .text_size(FontSizes::SM)
                                            .font_weight(FontWeight::MEDIUM)
                                            .text_color(theme.foreground)
                                            .child("SQL Preview"),
                                    )
                                    .child(
                                        div()
                                            .px(Spacing::SM)
                                            .py(Spacing::XS)
                                            .rounded(Radii::SM)
                                            .bg(theme.secondary)
                                            .text_size(FontSizes::XS)
                                            .text_color(theme.muted_foreground)
                                            .child(generation_type.label()),
                                    ),
                            )
                            .child(
                                div()
                                    .id("close-btn")
                                    .flex()
                                    .items_center()
                                    .justify_center()
                                    .size(Heights::ICON_SM)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.close(cx);
                                    }))
                                    .child(
                                        svg()
                                            .path(AppIcon::X.path())
                                            .size_4()
                                            .text_color(theme.muted_foreground),
                                    ),
                            ),
                    )
                    // SQL Editor
                    .child(
                        div()
                            .flex_1()
                            .p(Spacing::MD)
                            .min_h(px(200.0))
                            .max_h(px(300.0))
                            .overflow_hidden()
                            .child(Input::new(&sql_display).w_full().h_full()),
                    )
                    // Options
                    .child(
                        div()
                            .px(Spacing::MD)
                            .py(Spacing::SM)
                            .border_t_1()
                            .border_color(theme.border)
                            .flex()
                            .items_center()
                            .gap(Spacing::LG)
                            .child(
                                div()
                                    .text_size(FontSizes::XS)
                                    .text_color(theme.muted_foreground)
                                    .font_weight(FontWeight::MEDIUM)
                                    .child("Options"),
                            )
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::SM)
                                    .child(
                                        Checkbox::new("fqn-checkbox")
                                            .checked(use_fqn)
                                            .small()
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.toggle_fully_qualified(window, cx);
                                            })),
                                    )
                                    .child(
                                        div()
                                            .text_size(FontSizes::SM)
                                            .text_color(theme.foreground)
                                            .child("Fully qualified names"),
                                    ),
                            )
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::SM)
                                    .child(
                                        Checkbox::new("compact-checkbox")
                                            .checked(compact)
                                            .small()
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.toggle_compact(window, cx);
                                            })),
                                    )
                                    .child(
                                        div()
                                            .text_size(FontSizes::SM)
                                            .text_color(theme.foreground)
                                            .child("Compact SQL"),
                                    ),
                            ),
                    )
                    // Footer
                    .child(
                        div()
                            .px(Spacing::MD)
                            .py(Spacing::SM)
                            .border_t_1()
                            .border_color(theme.border)
                            .flex()
                            .items_center()
                            .justify_end()
                            .gap(Spacing::SM)
                            .child(
                                div()
                                    .id("refresh-btn")
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::XS)
                                    .px(Spacing::MD)
                                    .py(Spacing::SM)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .bg(theme.secondary)
                                    .hover(|d| d.bg(theme.muted))
                                    .text_size(FontSizes::SM)
                                    .text_color(theme.foreground)
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.regenerate_sql(window, cx);
                                    }))
                                    .child(
                                        svg()
                                            .path(AppIcon::RefreshCcw.path())
                                            .size_4()
                                            .text_color(theme.muted_foreground),
                                    )
                                    .child("Refresh"),
                            )
                            .child(
                                div()
                                    .id("copy-btn")
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::XS)
                                    .px(Spacing::MD)
                                    .py(Spacing::SM)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .bg(theme.primary)
                                    .hover(|d| d.opacity(0.9))
                                    .text_size(FontSizes::SM)
                                    .text_color(theme.primary_foreground)
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.copy_to_clipboard(cx);
                                        this.close(cx);
                                    }))
                                    .child(
                                        svg()
                                            .path(AppIcon::Layers.path())
                                            .size_4()
                                            .text_color(theme.primary_foreground),
                                    )
                                    .child("Copy"),
                            )
                            .child(
                                div()
                                    .id("close-footer-btn")
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::XS)
                                    .px(Spacing::MD)
                                    .py(Spacing::SM)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .bg(theme.secondary)
                                    .hover(|d| d.bg(theme.muted))
                                    .text_size(FontSizes::SM)
                                    .text_color(theme.foreground)
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.close(cx);
                                    }))
                                    .child("Close"),
                            ),
                    ),
            )
            .into_any_element()
    }
}

impl EventEmitter<()> for SqlPreviewModal {}
