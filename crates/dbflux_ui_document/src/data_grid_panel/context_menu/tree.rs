//! The data-grid context menu as a Bolt cascade tree.
//!
//! [`build_menu_tree`] converts the menu `DataGridPanel` draws into the
//! declarative [`MenuNode`] rows the Bolt cascade renders:
//! `render_context_menu` mounts the tree in place of the old flyout
//! renderer, and the keyboard dispatch resolves a row the cascade
//! activated by the stable ids minted here, through [`action_from_key`].
//!
//! The rows, their order and their content mirror the menu row for row.
//! The one structural change is the submenus: what the old renderers drew
//! as absolutely-positioned flyouts, built only while open, hangs here as
//! `children` on its trigger, always present, and each trigger carries the
//! width its flyout is drawn at.

use super::sections::action_shortcut;
use super::toolbar::ToolbarAction;
use super::{ContextMenuItem, DataGridPanel, FilterBackend, SetValueEntry, TableContextMenu};
use dbflux_components::components::data_table::{ContextMenuAction, FilterOperator};
use dbflux_components::composites::{MenuItem, MenuNode};
use dbflux_components::icons::AppIcon;
use gpui::{App, Context, Pixels, px};
use gpui_component::ActiveTheme;
use std::borrow::Cow;

/// Widths of the submenu flyouts, sized to their longest labels.
const FILTER_SUBMENU_WIDTH: Pixels = px(280.0);
const ORDER_SUBMENU_WIDTH: Pixels = px(220.0);
const GENERATE_SQL_SUBMENU_WIDTH: Pixels = px(180.0);
const COPY_QUERY_SUBMENU_WIDTH: Pixels = px(160.0);
const SET_VALUE_SUBMENU_WIDTH: Pixels = px(300.0);
const TOOLBAR_SUBMENU_WIDTH: Pixels = px(240.0);

/// The rows of the data-grid context menu as a Bolt cascade tree.
///
/// Reads the same panel and menu state the menu is drawn and dispatched
/// from, and produces the same rows, in the same order: the header caption,
/// the plain action rows, then the Filter / Order / Generate SQL / Copy as
/// Query / driver row-action / Toolbar sections, each submenu nested under
/// its trigger.
pub(in crate::data_grid_panel) fn build_menu_tree(
    panel: &DataGridPanel,
    menu: &TableContextMenu,
    cx: &Context<DataGridPanel>,
) -> Vec<MenuNode> {
    if menu.is_column_header {
        let backend = panel.filter_backend(cx);
        return build_column_header_menu(panel, menu, backend, cx);
    }

    build_cell_menu(panel, menu, cx)
}

/// The flat menu a column header opens: the fixed copy/order actions, then
/// every filter operator inline.
fn build_column_header_menu(
    panel: &DataGridPanel,
    menu: &TableContextMenu,
    backend: Option<FilterBackend>,
    cx: &Context<DataGridPanel>,
) -> Vec<MenuNode> {
    let column = panel
        .result
        .columns
        .get(menu.col)
        .map(|column| column.name.clone())
        .unwrap_or_default();
    let (_, _, filter_items, value_ops_count) = panel.build_filter_items(menu, backend, cx);

    let mut rows = Vec::new();

    for action in DataGridPanel::column_header_menu_fixed_actions() {
        let (label, icon, is_danger) = match action {
            ContextMenuAction::CopyColumnName => (
                dbflux_i18n::t!("document.data.context_menu.item.copy_column_name"),
                AppIcon::Copy,
                false,
            ),
            ContextMenuAction::Order(dbflux_core::SortDirection::Ascending) => {
                (format!("{column} ASC"), AppIcon::ArrowUp, false)
            }
            ContextMenuAction::Order(dbflux_core::SortDirection::Descending) => {
                (format!("{column} DESC"), AppIcon::ArrowDown, false)
            }
            _ => (
                dbflux_i18n::t!("document.data.context_menu.order.remove"),
                AppIcon::X,
                true,
            ),
        };

        match action {
            ContextMenuAction::Order(dbflux_core::SortDirection::Ascending) => {
                rows.push(MenuNode::separator());
                rows.push(
                    MenuNode::header(dbflux_i18n::t!("document.data.context_menu.order.title"))
                        .icon(AppIcon::ArrowUpDown),
                );
            }
            ContextMenuAction::RemoveOrdering => {
                rows.push(MenuNode::separator());
            }
            _ => {}
        }

        let mut node = MenuNode::row(label)
            .id(action_row_id(&column, action))
            .icon(icon);
        if is_danger {
            node = node.danger();
        }
        rows.push(node);
    }

    rows.push(MenuNode::separator());
    rows.push(
        MenuNode::header(dbflux_i18n::t!("document.data.context_menu.filter.title"))
            .icon(AppIcon::ListFilter),
    );

    rows.extend(build_filter_children(
        &column,
        &filter_items,
        value_ops_count,
        None,
    ));

    // The rows carry the element ids the keyboard-coverage registry names,
    // numbered across the menu's action rows like the old renderer numbered
    // them; separators and captions take no id.
    let mut action_index = 0usize;
    for row in rows.iter_mut() {
        if row.id.is_some() {
            row.element_id = Some(format!("column-menu-action-{action_index}").into());
            action_index += 1;
        }
    }

    rows
}

/// Which member of the Filter / Order / Generate SQL / Copy as Query group
/// carries the separator that opens it. The four read as one group
/// (IslMenu), so only the first one present gets it; the Filter member
/// always opens the group, so its separator is implied by its presence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueryGroupSeparators {
    order: bool,
    generate_sql: bool,
    copy_query: bool,
}

/// Resolves the group's separator carriers from which members are present.
fn query_group_separators(
    has_filter: bool,
    has_order: bool,
    has_generate_sql: bool,
    has_copy_query: bool,
) -> QueryGroupSeparators {
    QueryGroupSeparators {
        order: has_order && !has_filter,
        generate_sql: has_generate_sql && !has_filter && !has_order,
        copy_query: has_copy_query && !has_filter && !has_order && !has_generate_sql,
    }
}

/// The menu a cell opens, as `render_context_menu` assembles it.
fn build_cell_menu(
    panel: &DataGridPanel,
    menu: &TableContextMenu,
    cx: &Context<DataGridPanel>,
) -> Vec<MenuNode> {
    let is_document_view = menu.is_document_view;
    let backend = panel.filter_backend(cx);
    let has_row_target = panel.has_context_menu_row_target(menu.row, is_document_view, cx);
    let is_editable = panel.check_is_editable(cx);

    let mut rows = Vec::new();

    if has_row_target && !is_document_view {
        rows.push(cell_menu_header_node(panel, menu, cx));
    }

    for item in &visible_menu_items(panel, menu, has_row_target, is_editable, cx) {
        push_menu_item_row(panel, item, menu, &mut rows, cx);
    }

    let table_queries = panel.offers_table_query_actions();
    let has_filter = table_queries && panel.has_filter_submenu(backend, is_document_view, cx);
    let has_order =
        table_queries && matches!(backend, Some(FilterBackend::Sql)) && !is_document_view;
    let separators = query_group_separators(
        has_filter,
        has_order,
        table_queries && !is_document_view,
        panel.has_copy_query_support(),
    );

    if has_filter {
        rows.push(MenuNode::separator());
        rows.push(filter_trigger_node(panel, menu, backend, cx));
    }

    if has_order {
        if separators.order {
            rows.push(MenuNode::separator());
        }
        rows.push(order_trigger_node(panel, menu));
    }

    let generate_sql_hidden = is_document_view || !table_queries;
    if !generate_sql_hidden {
        if separators.generate_sql {
            rows.push(MenuNode::separator());
        }
        rows.push(generate_sql_trigger_node());
    }

    if panel.has_copy_query_support() {
        if separators.copy_query {
            rows.push(MenuNode::separator());
        }
        rows.push(copy_query_trigger_node(panel, cx));
    }

    if !menu.row_actions.is_empty() {
        rows.push(MenuNode::separator());
        for (index, action) in menu.row_actions.iter().enumerate() {
            let mut node = MenuNode::row(action.label.clone()).id(format!("row-action:{index}"));
            node = node.icon(if action.is_destructive {
                AppIcon::Power
            } else {
                AppIcon::Zap
            });
            if action.is_destructive {
                node = node.danger();
            }
            rows.push(node);
        }
    }

    let toolbar_actions = panel.toolbar_actions(cx);
    if !toolbar_actions.is_empty() {
        rows.push(MenuNode::separator());
        rows.push(toolbar_trigger_node(panel, &toolbar_actions));
    }

    rows
}

/// The items of the plain action list, built through the adapter chain in
/// the order the menu lists them.
fn visible_menu_items(
    panel: &DataGridPanel,
    menu: &TableContextMenu,
    has_row_target: bool,
    is_editable: bool,
    cx: &Context<DataGridPanel>,
) -> Vec<ContextMenuItem> {
    let can_chart = panel.can_chart_from_context_menu(cx);
    let inspect_row_enabled = !panel.is_grouped_result();
    let base = DataGridPanel::build_context_menu_items(
        is_editable,
        menu.is_document_view,
        has_row_target,
        can_chart,
        inspect_row_enabled,
    );
    let adapted = panel.adapt_menu_items_for_documents(base, menu.is_document_view, cx);
    let with_similar = panel.with_find_similar_rows(adapted, menu, has_row_target, cx);
    panel.with_set_value_menu(with_similar, menu, cx)
}

/// One row of the plain item list: a separator, the "Set value" trigger
/// with its flyout, or a plain action row.
fn push_menu_item_row(
    panel: &DataGridPanel,
    item: &ContextMenuItem,
    menu: &TableContextMenu,
    rows: &mut Vec<MenuNode>,
    cx: &Context<DataGridPanel>,
) {
    if item.is_separator {
        rows.push(MenuNode::separator());
        return;
    }

    let Some(action) = item.action else {
        return;
    };

    if action == ContextMenuAction::SetValueMenu {
        let entries = panel.set_value_entries(menu.col, cx);
        let mut trigger = MenuNode::row(item.label.clone())
            .id("set-value-trigger")
            .element_id("set-value-trigger")
            .children_width(SET_VALUE_SUBMENU_WIDTH.as_f32());
        if let Some(icon) = item.icon {
            trigger = trigger.icon(icon);
        }
        rows.push(trigger.children(build_set_value_children(&entries)));
        return;
    }

    let mut node = MenuNode::row(item.label.clone()).id(action_key(action).into_owned());
    if let Some(icon) = item.icon {
        node = node.icon(icon);
    }
    if item.is_danger {
        node = node.danger();
    }
    if let Some(shortcut) = action_shortcut(action, cx) {
        node = node.shortcut(shortcut);
    }
    if let Some(detail) = &item.detail {
        node = node.detail(detail.clone());
    }
    if item.disabled {
        node = node.disabled();
    }
    rows.push(node);
}

/// The "Set value" trigger's flyout rows: the column default, the type's
/// expressions, NULL and an expression.
fn build_set_value_children(entries: &[SetValueEntry]) -> Vec<MenuNode> {
    let mut rows = Vec::new();

    for (index, entry) in entries.iter().enumerate() {
        if let Some(header) = &entry.header {
            rows.push(MenuNode::separator());
            rows.push(MenuNode::header(header.clone()).icon(AppIcon::Code));
        }

        let mut node = MenuNode::row(entry.label.clone())
            .id(format!("set-value:{index}"))
            .element_id(format!("set-value-entry-{index}"))
            .icon(if entry.is_expression {
                AppIcon::Code
            } else {
                AppIcon::CircleX
            });
        if let Some(detail) = &entry.detail {
            node = node.detail(detail.clone());
        }
        if entry.disabled {
            node = node.disabled();
        }
        rows.push(node);
    }

    rows
}

/// The "Filter" trigger with its flyout of value-based filter operators plus
/// "Remove filter".
fn filter_trigger_node(
    panel: &DataGridPanel,
    menu: &TableContextMenu,
    backend: Option<FilterBackend>,
    cx: &Context<DataGridPanel>,
) -> MenuNode {
    let (column, _count, filter_items, value_ops_count) =
        panel.build_filter_items(menu, backend, cx);
    let cell_value_label = dbflux_i18n::t!("document.data.context_menu.filter.cell_value");
    let children = build_filter_children(
        &column,
        &filter_items,
        value_ops_count,
        Some(&cell_value_label),
    );

    MenuNode::row(dbflux_i18n::t!("document.data.context_menu.filter.title"))
        .id("filter-trigger")
        .icon(AppIcon::ListFilter)
        .children_width(FILTER_SUBMENU_WIDTH.as_f32())
        .children(children)
}

/// The filter rows both menus share: the value operators under the
/// cell-value caption, then IS NULL / IS NOT NULL, then Remove filter.
fn build_filter_children(
    column: &str,
    filter_items: &[(String, ContextMenuAction)],
    value_ops_count: usize,
    cell_value_header: Option<&str>,
) -> Vec<MenuNode> {
    let value_section_separator_idx = (value_ops_count > 0).then_some(value_ops_count);
    let remove_separator_idx = filter_items.len().saturating_sub(1);

    let mut rows = Vec::new();

    if let Some(label) = cell_value_header {
        rows.push(MenuNode::header(label));
    }

    for (idx, (label, action)) in filter_items.iter().enumerate() {
        if value_section_separator_idx == Some(idx) || idx == remove_separator_idx {
            rows.push(MenuNode::separator());
        }
        rows.push(filter_row_node(column, label, *action));
    }

    rows
}

/// One filter entry: the remove action is marked, every other operator gets
/// the filter icon.
fn filter_row_node(column: &str, label: &str, action: ContextMenuAction) -> MenuNode {
    let is_remove = matches!(action, ContextMenuAction::RemoveFilter);
    let mut node = MenuNode::row(label)
        .id(action_row_id(column, action))
        .icon(if is_remove {
            AppIcon::X
        } else {
            AppIcon::ListFilter
        });
    if is_remove {
        node = node.danger();
    }
    node
}

/// The "Order" trigger with its ASC / DESC / Remove ordering flyout.
fn order_trigger_node(panel: &DataGridPanel, menu: &TableContextMenu) -> MenuNode {
    let column = panel
        .result
        .columns
        .get(menu.col)
        .map(|column| column.name.clone())
        .unwrap_or_default();

    let children = vec![
        ordering_row_node(&column, dbflux_core::SortDirection::Ascending),
        ordering_row_node(&column, dbflux_core::SortDirection::Descending),
        MenuNode::separator(),
        MenuNode::row(dbflux_i18n::t!("document.data.context_menu.order.remove"))
            .id("remove-ordering")
            .icon(AppIcon::X)
            .danger(),
    ];

    MenuNode::row(dbflux_i18n::t!("document.data.context_menu.order.title"))
        .id("order-trigger")
        .icon(AppIcon::ArrowUpDown)
        .children_width(ORDER_SUBMENU_WIDTH.as_f32())
        .children(children)
}

/// One `{column} ASC` / `{column} DESC` ordering entry, carrying the column
/// it acts on so dispatch needs no index arithmetic.
fn ordering_row_node(column: &str, direction: dbflux_core::SortDirection) -> MenuNode {
    let label = format!(
        "{column} {}",
        match direction {
            dbflux_core::SortDirection::Ascending => "ASC",
            dbflux_core::SortDirection::Descending => "DESC",
        }
    );

    MenuNode::row(label)
        .id(format!("order:{column}:{}", direction_key(direction)))
        .icon(match direction {
            dbflux_core::SortDirection::Ascending => AppIcon::ArrowUp,
            dbflux_core::SortDirection::Descending => AppIcon::ArrowDown,
        })
}

/// The "Generate SQL" trigger with its SELECT WHERE / INSERT / UPDATE /
/// DELETE flyout.
fn generate_sql_trigger_node() -> MenuNode {
    let children = [
        ("SELECT WHERE", ContextMenuAction::GenerateSelectWhere),
        ("INSERT", ContextMenuAction::GenerateInsert),
        ("UPDATE", ContextMenuAction::GenerateUpdate),
        ("DELETE", ContextMenuAction::GenerateDelete),
    ]
    .into_iter()
    .map(|(label, action)| {
        MenuNode::row(label)
            .id(action_key(action).into_owned())
            .icon(AppIcon::Code)
    })
    .collect();

    MenuNode::row(dbflux_i18n::t!(
        "document.data.context_menu.generate_sql.title"
    ))
    .id("generate-sql-trigger")
    .icon(AppIcon::Code)
    .children_width(GENERATE_SQL_SUBMENU_WIDTH.as_f32())
    .children(children)
}

/// The "Copy as Query" trigger with its INSERT / UPDATE / DELETE flyout.
fn copy_query_trigger_node(panel: &DataGridPanel, cx: &Context<DataGridPanel>) -> MenuNode {
    let children = [
        ("INSERT", ContextMenuAction::CopyAsInsert),
        ("UPDATE", ContextMenuAction::CopyAsUpdate),
        ("DELETE", ContextMenuAction::CopyAsDelete),
    ]
    .into_iter()
    .map(|(label, action)| {
        MenuNode::row(label)
            .id(action_key(action).into_owned())
            .icon(AppIcon::Columns)
    })
    .collect();

    MenuNode::row(panel.copy_query_submenu_label(cx))
        .id("copy-query-trigger")
        .icon(AppIcon::Table)
        .children_width(COPY_QUERY_SUBMENU_WIDTH.as_f32())
        .children(children)
}

/// The "Toolbar" trigger with the toolbar's buttons as its flyout. The rows
/// keep the element ids the keyboard-coverage registry already names.
fn toolbar_trigger_node(panel: &DataGridPanel, actions: &[ToolbarAction]) -> MenuNode {
    let children = actions
        .iter()
        .map(|&action| {
            let id = format!("toolbar-action-{}", action.id());
            let mut node = MenuNode::row(panel.toolbar_action_label(action))
                .id(id.clone())
                .element_id(id)
                .icon(action.icon());
            if let Some(shortcut) = action.shortcut() {
                node = node.shortcut(shortcut);
            }
            node
        })
        .collect();

    MenuNode::row(dbflux_i18n::t!("document.data.context_menu.toolbar.title"))
        .id("toolbar-trigger")
        .icon(AppIcon::Settings)
        .children_width(TOOLBAR_SUBMENU_WIDTH.as_f32())
        .children(children)
}

/// The header caption of the cell menu: the column and row it acts on, with
/// the key icon in the theme's warning or info color when the column is a
/// primary or foreign key.
fn cell_menu_header_node(
    panel: &DataGridPanel,
    menu: &TableContextMenu,
    cx: &Context<DataGridPanel>,
) -> MenuNode {
    let column_name = panel
        .result
        .columns
        .get(menu.col)
        .map(|column| column.name.clone())
        .unwrap_or_default();

    let label = dbflux_i18n::t!(
        "document.data.context_menu.header",
        column = column_name,
        row = menu.row + 1
    );

    let table_state = panel
        .grid_table
        .table_state
        .as_ref()
        .map(|table| table.read(cx));
    let is_primary_key = table_state.is_some_and(|state| state.pk_columns().contains(&menu.col));
    let is_foreign_key = table_state.is_some_and(|state| state.fk_columns().contains(&menu.col));

    let theme = cx.theme();
    let item = if is_primary_key {
        MenuItem::header(label)
            .icon(AppIcon::KeyRound)
            .header_icon_color(theme.warning)
    } else if is_foreign_key {
        MenuItem::header(label)
            .icon(AppIcon::Cable)
            .header_icon_color(theme.info)
    } else {
        MenuItem::header(label).icon(AppIcon::Columns)
    };

    MenuNode::new(item)
}

/// The stable, locale-independent id of a row's action, minted on the row
/// so the keyboard dispatch resolves an activated row without index
/// arithmetic. Filter and ordering rows carry the column they act on.
pub(super) fn action_key(action: ContextMenuAction) -> Cow<'static, str> {
    match action {
        ContextMenuAction::Copy => Cow::Borrowed("copy"),
        ContextMenuAction::Paste => Cow::Borrowed("paste"),
        ContextMenuAction::Edit => Cow::Borrowed("edit"),
        ContextMenuAction::EditInModal => Cow::Borrowed("edit-in-modal"),
        ContextMenuAction::ViewValue => Cow::Borrowed("view-value"),
        ContextMenuAction::SetDefault => Cow::Borrowed("set-default"),
        ContextMenuAction::SetNull => Cow::Borrowed("set-null"),
        ContextMenuAction::SetValueMenu => Cow::Borrowed("set-value-menu"),
        ContextMenuAction::SetTypeValue(index) => Cow::Owned(format!("set-type-value:{index}")),
        ContextMenuAction::SetExpression => Cow::Borrowed("set-expression"),
        ContextMenuAction::AddRow => Cow::Borrowed("add-row"),
        ContextMenuAction::DuplicateRow => Cow::Borrowed("duplicate-row"),
        ContextMenuAction::DeleteRow => Cow::Borrowed("delete-row"),
        ContextMenuAction::GenerateSelectWhere => Cow::Borrowed("generate-select-where"),
        ContextMenuAction::GenerateInsert => Cow::Borrowed("generate-insert"),
        ContextMenuAction::GenerateUpdate => Cow::Borrowed("generate-update"),
        ContextMenuAction::GenerateDelete => Cow::Borrowed("generate-delete"),
        ContextMenuAction::CopyAsInsert => Cow::Borrowed("copy-as-insert"),
        ContextMenuAction::CopyAsUpdate => Cow::Borrowed("copy-as-update"),
        ContextMenuAction::CopyAsDelete => Cow::Borrowed("copy-as-delete"),
        ContextMenuAction::FilterByValue(operator) => {
            Cow::Owned(format!("filter-by-value:{}", operator_key(operator)))
        }
        ContextMenuAction::FilterIsNull => Cow::Borrowed("filter-is-null"),
        ContextMenuAction::FilterIsNotNull => Cow::Borrowed("filter-is-not-null"),
        ContextMenuAction::RemoveFilter => Cow::Borrowed("remove-filter"),
        ContextMenuAction::Order(direction) => Cow::Borrowed(match direction {
            dbflux_core::SortDirection::Ascending => "order:asc",
            dbflux_core::SortDirection::Descending => "order:desc",
        }),
        ContextMenuAction::RemoveOrdering => Cow::Borrowed("remove-ordering"),
        ContextMenuAction::CopyColumnName => Cow::Borrowed("copy-column-name"),
        ContextMenuAction::InspectRow => Cow::Borrowed("inspect-row"),
        ContextMenuAction::ChartThisQuery => Cow::Borrowed("chart-this-query"),
        ContextMenuAction::UnsetField => Cow::Borrowed("unset-field"),
        ContextMenuAction::RevertCell => Cow::Borrowed("revert-cell"),
        ContextMenuAction::FindSimilarRows => Cow::Borrowed("find-similar-rows"),
    }
}

/// Parses an id from [`action_key`] back into its action, and the ids of
/// the filter and ordering rows that carry a column. The matches are
/// exhaustive against the keys above: adding a variant to
/// `ContextMenuAction` breaks this file's build until it gets both halves.
pub(super) fn action_from_key(key: &str) -> Option<ContextMenuAction> {
    match key {
        "copy" => Some(ContextMenuAction::Copy),
        "paste" => Some(ContextMenuAction::Paste),
        "edit" => Some(ContextMenuAction::Edit),
        "edit-in-modal" => Some(ContextMenuAction::EditInModal),
        "view-value" => Some(ContextMenuAction::ViewValue),
        "set-default" => Some(ContextMenuAction::SetDefault),
        "set-null" => Some(ContextMenuAction::SetNull),
        "set-value-menu" => Some(ContextMenuAction::SetValueMenu),
        "set-expression" => Some(ContextMenuAction::SetExpression),
        "add-row" => Some(ContextMenuAction::AddRow),
        "duplicate-row" => Some(ContextMenuAction::DuplicateRow),
        "delete-row" => Some(ContextMenuAction::DeleteRow),
        "generate-select-where" => Some(ContextMenuAction::GenerateSelectWhere),
        "generate-insert" => Some(ContextMenuAction::GenerateInsert),
        "generate-update" => Some(ContextMenuAction::GenerateUpdate),
        "generate-delete" => Some(ContextMenuAction::GenerateDelete),
        "copy-as-insert" => Some(ContextMenuAction::CopyAsInsert),
        "copy-as-update" => Some(ContextMenuAction::CopyAsUpdate),
        "copy-as-delete" => Some(ContextMenuAction::CopyAsDelete),
        "filter-is-null" => Some(ContextMenuAction::FilterIsNull),
        "filter-is-not-null" => Some(ContextMenuAction::FilterIsNotNull),
        "remove-filter" => Some(ContextMenuAction::RemoveFilter),
        "order:asc" => Some(ContextMenuAction::Order(
            dbflux_core::SortDirection::Ascending,
        )),
        "order:desc" => Some(ContextMenuAction::Order(
            dbflux_core::SortDirection::Descending,
        )),
        "remove-ordering" => Some(ContextMenuAction::RemoveOrdering),
        "copy-column-name" => Some(ContextMenuAction::CopyColumnName),
        "inspect-row" => Some(ContextMenuAction::InspectRow),
        "chart-this-query" => Some(ContextMenuAction::ChartThisQuery),
        "unset-field" => Some(ContextMenuAction::UnsetField),
        "revert-cell" => Some(ContextMenuAction::RevertCell),
        "find-similar-rows" => Some(ContextMenuAction::FindSimilarRows),
        other => {
            // The composite ids carry a column name that may itself contain
            // ':', so the payload splits off from the right.
            let (head, payload) = other.rsplit_once(':')?;

            // `set-type-value:{index}` stages the dialect's `index`th
            // expression for the column the menu was opened on.
            if head == "set-type-value" {
                return payload.parse().ok().map(ContextMenuAction::SetTypeValue);
            }

            // `filter-by-value:{op}` (the plain action key) and
            // `filter:{column}:{op}` (a row carrying its column) name the
            // same operator.
            if head == "filter-by-value" || head.strip_prefix("filter:").is_some() {
                return operator_from_key(payload).map(ContextMenuAction::FilterByValue);
            }

            // `order:{asc|desc}` and `order:{column}:{asc|desc}`, likewise.
            if head == "order" || head.strip_prefix("order:").is_some() {
                return direction_from_key(payload).map(ContextMenuAction::Order);
            }

            None
        }
    }
}

/// The id of a row whose action acts on a column: filter and ordering rows
/// carry the column in their id, every other action its plain action key.
fn action_row_id(column: &str, action: ContextMenuAction) -> String {
    match action {
        ContextMenuAction::FilterByValue(operator) => {
            format!("filter:{column}:{}", operator_key(operator))
        }
        ContextMenuAction::Order(direction) => {
            format!("order:{column}:{}", direction_key(direction))
        }
        other => action_key(other).into_owned(),
    }
}

fn direction_key(direction: dbflux_core::SortDirection) -> &'static str {
    match direction {
        dbflux_core::SortDirection::Ascending => "asc",
        dbflux_core::SortDirection::Descending => "desc",
    }
}

fn direction_from_key(key: &str) -> Option<dbflux_core::SortDirection> {
    match key {
        "asc" => Some(dbflux_core::SortDirection::Ascending),
        "desc" => Some(dbflux_core::SortDirection::Descending),
        _ => None,
    }
}

fn operator_key(operator: FilterOperator) -> &'static str {
    match operator {
        FilterOperator::Eq => "eq",
        FilterOperator::NotEq => "not-eq",
        FilterOperator::Gt => "gt",
        FilterOperator::Gte => "gte",
        FilterOperator::Lt => "lt",
        FilterOperator::Lte => "lte",
        FilterOperator::Like => "like",
    }
}

fn operator_from_key(key: &str) -> Option<FilterOperator> {
    match key {
        "eq" => Some(FilterOperator::Eq),
        "not-eq" => Some(FilterOperator::NotEq),
        "gt" => Some(FilterOperator::Gt),
        "gte" => Some(FilterOperator::Gte),
        "lt" => Some(FilterOperator::Lt),
        "lte" => Some(FilterOperator::Lte),
        "like" => Some(FilterOperator::Like),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{ContextMenuAction, QueryGroupSeparators, action_from_key, action_key};
    use super::{build_menu_tree, query_group_separators};
    use crate::data_grid_panel::{DataGridPanel, DataSource, TableContextMenu};
    use crate::keyboard_test_support::{connected_app_state, host_document, init_keyboard_runtime};
    use dbflux_components::components::data_table::{FilterOperator, context_menu_keystroke};
    use dbflux_components::composites::{HoverIntent, MenuNav, MenuNode};
    use dbflux_core::{
        ColumnKind, ColumnMeta, DbKind, InspectorRowAction, Pagination, QueryResult, SortDirection,
        TableRef, Value,
    };
    use dbflux_test_support::fake_driver::FakeDriver;
    use dbflux_ui_base::keymap::{chord_display_parts, key_chord_from_gpui};
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext, px};
    use std::time::Duration;

    /// A table panel for a connected SQL driver, with one `id` column and
    /// three rows, and the window it lives in. The panel focuses its data
    /// table, so a menu on row 0 / column 0 has a row target.
    fn hosted_table(cx: &mut TestAppContext) -> (Entity<DataGridPanel>, &mut VisualTestContext) {
        init_keyboard_runtime(cx);

        let driver = FakeDriver::new(DbKind::Postgres);
        let (app_state, profile_id) = connected_app_state(cx, &driver, "shop");

        let (host, window) = host_document(
            cx,
            move |window, cx| {
                cx.new(|cx| {
                    let source = DataSource::Table {
                        profile_id,
                        database: Some("app".to_string()),
                        table: TableRef::with_schema("public", "users"),
                        pagination: Pagination::default(),
                        order_by: Vec::new(),
                        total_rows: Some(3),
                    };

                    let mut panel = DataGridPanel::new_internal(
                        source,
                        app_state.clone(),
                        vec!["id".to_string()],
                        window,
                        cx,
                    );
                    panel.set_result(
                        QueryResult::table(
                            vec![ColumnMeta {
                                name: "id".to_string(),
                                type_name: "int4".to_string(),
                                kind: ColumnKind::Integer,
                                nullable: false,
                                is_primary_key: true,
                            }],
                            (1..=3).map(|id| vec![Value::Int(id)]).collect(),
                            None,
                            Duration::ZERO,
                        ),
                        cx,
                    );
                    panel
                })
            },
            |panel, cx| panel.active_context(cx),
            DataGridPanel::dispatch_command,
        );

        let panel = window.update(|_, cx| host.read(cx).document.clone());
        (panel, window)
    }

    /// A menu state opened on cell (0, 0) of the table view, with no
    /// flyout open: the nav cursor sits at depth 0, as a menu before its
    /// first frame does.
    fn cell_menu(row_actions: Vec<InspectorRowAction>) -> TableContextMenu {
        TableContextMenu {
            row: 0,
            col: 0,
            position: gpui::Point {
                x: px(100.0),
                y: px(100.0),
            },
            nav: MenuNav::new(),
            hover: HoverIntent::new(),
            is_document_view: false,
            is_column_header: false,
            doc_field_path: None,
            doc_field_value: None,
            row_actions,
        }
    }

    /// The tree of `menu` opened on the hosted panel.
    fn menu_tree(
        panel: &Entity<DataGridPanel>,
        window: &mut VisualTestContext,
        menu: &TableContextMenu,
    ) -> Vec<MenuNode> {
        window.update(|_, cx| panel.update(cx, |panel, cx| build_menu_tree(panel, menu, cx)))
    }

    /// The row with `id`, at the root level or one flyout deep.
    fn find_row<'a>(tree: &'a [MenuNode], id: &str) -> &'a MenuNode {
        tree.iter()
            .find(|node| node.id.as_deref() == Some(id))
            .or_else(|| {
                tree.iter()
                    .flat_map(|node| node.children.iter())
                    .find(|node| node.id.as_deref() == Some(id))
            })
            .unwrap_or_else(|| panic!("the tree has a {id} row"))
    }

    /// The root level carries an id on every activatable row, in the order
    /// the old renderer draws them: the plain actions first, then the submenu
    /// triggers, the driver row actions and the Toolbar trigger.
    #[gpui::test]
    fn the_root_level_lists_its_action_ids_in_order(cx: &mut TestAppContext) {
        let (panel, window) = hosted_table(cx);
        let menu = cell_menu(vec![
            InspectorRowAction {
                id: "kill".to_string(),
                label: "Kill".to_string(),
                description: None,
                is_destructive: true,
            },
            InspectorRowAction {
                id: "cancel".to_string(),
                label: "Cancel".to_string(),
                description: None,
                is_destructive: false,
            },
        ]);

        let tree = menu_tree(&panel, window, &menu);

        let ids: Vec<&str> = tree.iter().filter_map(|node| node.id.as_deref()).collect();
        assert_eq!(
            ids,
            vec![
                "copy",
                "copy-column-name",
                "view-value",
                "paste",
                "edit",
                "edit-in-modal",
                "set-value-trigger",
                "set-default",
                "set-null",
                "add-row",
                "inspect-row",
                "duplicate-row",
                "delete-row",
                "filter-trigger",
                "order-trigger",
                "generate-sql-trigger",
                "copy-query-trigger",
                "row-action:0",
                "row-action:1",
                "toolbar-trigger",
            ]
        );

        // The caption the cell menu opens on is a root row too, without an
        // id, keyed like the primary key it names.
        let header = tree.first().expect("the cell menu opens on a caption");
        assert!(header.item.is_header);
        assert!(!header.opens_flyout());
        assert_eq!(
            header.item.label.to_string(),
            dbflux_i18n::t!("document.data.context_menu.header", column = "id", row = 1)
        );
        assert!(header.item.header_icon_color.is_some());
    }

    /// Each of the six submenus hangs its rows under its trigger, at the
    /// width its flyout used to have, instead of listing them at the root
    /// level.
    #[gpui::test]
    fn every_submenu_nests_under_its_trigger(cx: &mut TestAppContext) {
        let (panel, window) = hosted_table(cx);
        let tree = menu_tree(&panel, window, &cell_menu(Vec::new()));

        let root_ids: Vec<&str> = tree.iter().filter_map(|node| node.id.as_deref()).collect();
        let expected = [
            ("set-value-trigger", 300.0, "set-value:0"),
            ("filter-trigger", 280.0, "filter:id:eq"),
            ("order-trigger", 220.0, "order:id:asc"),
            ("generate-sql-trigger", 180.0, "generate-select-where"),
            ("copy-query-trigger", 160.0, "copy-as-insert"),
            ("toolbar-trigger", 240.0, "toolbar-action-export"),
        ];

        for (trigger, width, known_child) in expected {
            let node = tree
                .iter()
                .find(|node| node.id.as_deref() == Some(trigger))
                .unwrap_or_else(|| panic!("the cell menu has a {trigger} row"));
            assert!(node.opens_flyout(), "{trigger} nests its rows");
            assert_eq!(node.children_width, Some(width));
            assert!(
                node.children
                    .iter()
                    .any(|child| child.id.as_deref() == Some(known_child)),
                "{trigger}'s flyout lists {known_child}"
            );
            for child in &node.children {
                if let Some(id) = child.id.as_deref() {
                    assert!(
                        !root_ids.contains(&id),
                        "{id} sits under {trigger}, not at the root level"
                    );
                }
            }
        }
    }

    /// Every `ContextMenuAction` variant round-trips through `action_key` /
    /// `action_from_key`. The two matches are exhaustive on purpose: adding a
    /// variant without a key fails this file's build, and a key without a
    /// variant fails this test.
    #[test]
    fn every_context_menu_action_round_trips_through_its_key() {
        let actions = [
            ContextMenuAction::Copy,
            ContextMenuAction::Paste,
            ContextMenuAction::Edit,
            ContextMenuAction::EditInModal,
            ContextMenuAction::ViewValue,
            ContextMenuAction::SetDefault,
            ContextMenuAction::SetNull,
            ContextMenuAction::SetValueMenu,
            ContextMenuAction::SetTypeValue(2),
            ContextMenuAction::SetExpression,
            ContextMenuAction::AddRow,
            ContextMenuAction::DuplicateRow,
            ContextMenuAction::DeleteRow,
            ContextMenuAction::GenerateSelectWhere,
            ContextMenuAction::GenerateInsert,
            ContextMenuAction::GenerateUpdate,
            ContextMenuAction::GenerateDelete,
            ContextMenuAction::CopyAsInsert,
            ContextMenuAction::CopyAsUpdate,
            ContextMenuAction::CopyAsDelete,
            ContextMenuAction::FilterByValue(FilterOperator::Like),
            ContextMenuAction::FilterIsNull,
            ContextMenuAction::FilterIsNotNull,
            ContextMenuAction::RemoveFilter,
            ContextMenuAction::Order(SortDirection::Ascending),
            ContextMenuAction::Order(SortDirection::Descending),
            ContextMenuAction::RemoveOrdering,
            ContextMenuAction::CopyColumnName,
            ContextMenuAction::InspectRow,
            ContextMenuAction::ChartThisQuery,
            ContextMenuAction::UnsetField,
            ContextMenuAction::RevertCell,
            ContextMenuAction::FindSimilarRows,
        ];

        for action in actions {
            let key = action_key(action);
            assert_eq!(
                action_from_key(&key),
                Some(action),
                "the key of {action:?} round-trips"
            );
        }

        assert_eq!(action_from_key("no-such-action"), None);
        assert_eq!(action_from_key("set-type-value:not-a-number"), None);
        assert_eq!(action_from_key("filter-by-value:unknown"), None);
    }

    /// A row whose id carries the column it acts on parses back into its
    /// action. Column names may contain ':', so the ids split from the
    /// right: `filter:a:b:eq` filters on column `a:b` with `eq`, not on
    /// column `a` with `b:eq`.
    #[test]
    fn row_ids_carrying_a_column_parse_from_the_right() {
        assert_eq!(
            action_from_key("filter:id:eq"),
            Some(ContextMenuAction::FilterByValue(FilterOperator::Eq))
        );
        assert_eq!(
            action_from_key("filter:a:b:eq"),
            Some(ContextMenuAction::FilterByValue(FilterOperator::Eq))
        );
        assert_eq!(
            action_from_key("order:id:asc"),
            Some(ContextMenuAction::Order(SortDirection::Ascending))
        );
        assert_eq!(
            action_from_key("order:a:b:desc"),
            Some(ContextMenuAction::Order(SortDirection::Descending))
        );
        assert_eq!(action_from_key("filter:a:b:unknown"), None);
        assert_eq!(action_from_key("order:a:b:unknown"), None);
    }

    /// A row keeps the content the old renderer draws: its label, its
    /// shortcut, its danger mark, its disabled state with the detail that
    /// says why, and the element id the coverage registry names.
    #[gpui::test]
    fn a_row_keeps_its_label_shortcut_and_disabled_state(cx: &mut TestAppContext) {
        let (panel, window) = hosted_table(cx);
        let menu = cell_menu(Vec::new());

        let (tree, copy_shortcut) = window.update(|_, cx| {
            let copy_shortcut = context_menu_keystroke(ContextMenuAction::Copy, cx)
                .map(|keystroke| chord_display_parts(&key_chord_from_gpui(&keystroke)).join(" "));
            let tree = panel.update(cx, |panel, cx| build_menu_tree(panel, &menu, cx));
            (tree, copy_shortcut)
        });

        let node = find_row(&tree, "delete-row");
        assert_eq!(
            node.item.label.to_string(),
            dbflux_i18n::t!("document.data.context_menu.item.delete_row")
        );
        assert!(node.item.is_danger);
        assert!(!node.item.disabled);

        // The shortcut comes from the table's own key bindings, as in the old
        // renderer.
        let node = find_row(&tree, "copy");
        assert_eq!(node.item.shortcut.as_deref(), copy_shortcut.as_deref());

        // The test table's column has no default, so the first "Set value"
        // entry is drawn but not activatable, and its detail says why.
        let set_value = find_row(&tree, "set-value-trigger");
        let first = set_value
            .children
            .iter()
            .find(|child| child.id.as_deref() == Some("set-value:0"))
            .expect("the column default is the first entry");
        assert_eq!(
            first.item.label.to_string(),
            dbflux_i18n::t!("document.data.context_menu.item.set_value_column_default")
        );
        assert!(first.item.disabled);
        assert_eq!(
            first.item.detail.as_deref(),
            Some(
                dbflux_i18n::t!(
                    "document.data.context_menu.item.set_value_no_default",
                    column = "id"
                )
                .as_str()
            )
        );

        let export = find_row(&tree, "toolbar-action-export");
        assert_eq!(
            export.item.label.to_string(),
            dbflux_i18n::t!("document.data.context_menu.toolbar.export")
        );
        assert_eq!(export.element_id.as_deref(), Some("toolbar-action-export"));
    }

    /// Which member of the Filter / Order / Generate SQL / Copy as Query
    /// group carries the separator that opens it: the four read as one
    /// group, so only the first one present gets it.
    #[test]
    fn query_group_has_one_separator_before_its_first_member() {
        assert_eq!(
            query_group_separators(true, true, true, true),
            QueryGroupSeparators {
                order: false,
                generate_sql: false,
                copy_query: false,
            }
        );

        assert_eq!(
            query_group_separators(false, false, true, true),
            QueryGroupSeparators {
                order: false,
                generate_sql: true,
                copy_query: false,
            }
        );

        assert_eq!(
            query_group_separators(false, false, false, true),
            QueryGroupSeparators {
                order: false,
                generate_sql: false,
                copy_query: true,
            }
        );

        assert_eq!(
            query_group_separators(false, true, false, false),
            QueryGroupSeparators {
                order: true,
                generate_sql: false,
                copy_query: false,
            }
        );
    }

    /// The plain menu rows must not translate per row: the labels arrive
    /// pre-built on `ContextMenuItem`, so `push_menu_item_row` hoists every
    /// translated string out of the loop it runs per row.
    #[test]
    fn plain_menu_rows_hoist_translations_out_of_the_per_row_loop() {
        let source = include_str!("tree.rs");

        let function_name = "fn push_menu_item_row(";
        let start = source
            .find(function_name)
            .unwrap_or_else(|| panic!("{function_name} not found in tree.rs"));
        let after_signature = &source[start + function_name.len()..];
        let end = after_signature
            .find("\n/// ")
            .unwrap_or(after_signature.len());
        let body = &after_signature[..end];

        assert!(
            !body.contains("dbflux_i18n::t!("),
            "push_menu_item_row must not call t! per row; hoist any translated \n             label onto ContextMenuItem before this loop runs"
        );
    }
}
