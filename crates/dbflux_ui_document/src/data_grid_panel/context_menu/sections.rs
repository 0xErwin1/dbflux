use super::{ContextMenuItem, DataGridEvent, DataGridPanel, FilterBackend, TableContextMenu};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::components::data_table::{ContextMenuAction, context_menu_keystroke};
use dbflux_components::composites::{
    MenuItem, menu_frame, menu_row, render_menu_header, render_separator,
};
use dbflux_components::icons::AppIcon;
use dbflux_components::tokens::MenuMetrics;
use dbflux_ui_base::keymap::{chord_display_parts, effective_keymap, key_chord_from_gpui};
use gpui::prelude::FluentBuilder;
use gpui::{deferred, *};

/// Distance from the menu's edge at which a submenu hangs off its item: the
/// menu width less a small overlap, so the two read as one surface.
const SUBMENU_OFFSET: Pixels = px(172.0);

/// Widths of the submenu flyouts, sized to their longest labels.
const FILTER_SUBMENU_WIDTH: Pixels = px(280.0);
const ORDER_SUBMENU_WIDTH: Pixels = px(220.0);
const GENERATE_SQL_SUBMENU_WIDTH: Pixels = px(180.0);
const COPY_QUERY_SUBMENU_WIDTH: Pixels = px(160.0);

/// The frame a submenu hangs in.
///
/// It sits beside its row, on the right unless the menu is against the
/// panel's right edge, and `anchored` slides it back into the window when
/// it would run past the bottom or a side: a long filter list opened from a
/// row near the bottom of the window is shifted up rather than cut off.
fn submenu_frame(open_left: bool, flyout: Div) -> Div {
    let (frame, corner) = if open_left {
        (div().absolute().right(SUBMENU_OFFSET), Anchor::TopRight)
    } else {
        (div().absolute().left(SUBMENU_OFFSET), Anchor::TopLeft)
    };
    // Deferred above the parent menu (priority 1): in a shared layer GPUI
    // paints icon sprites after quads, so the parent rows' chevrons would
    // otherwise show through the flyout's background.
    frame.top(-MenuMetrics::PADDING_Y).child(
        deferred(
            anchored()
                .anchor(corner)
                .snap_to_window_with_margin(MenuMetrics::ROW_INSET)
                .child(flyout),
        )
        .with_priority(2),
    )
}

/// The flyout surface of a submenu: the shared menu frame at a fixed width.
fn submenu_flyout(width: Pixels, cx: &App) -> Div {
    menu_frame(cx).w(width).occlude()
}

/// The shortcut shown on a menu row, formatted like the other keycaps in the
/// app (`Ctrl C`, `Delete`).
///
/// Row actions the data table binds come from its GPUI bindings; the ones
/// dispatched through the app keymap (inspect row, view value) come from the
/// Results layer.
fn action_shortcut(action: ContextMenuAction, cx: &App) -> Option<SharedString> {
    if let Some(keystroke) = context_menu_keystroke(action, cx) {
        let label = key_chord_from_gpui(&keystroke)
            .to_string()
            .replace('+', " ");

        return Some(label.into());
    }

    let command = match action {
        ContextMenuAction::InspectRow => Command::ToggleRowInspector,
        ContextMenuAction::ViewValue => Command::ToggleValuePanel,
        _ => return None,
    };

    effective_keymap()
        .chord_for_command(ContextId::Results, command)
        .map(|chord| chord_display_parts(chord).join(" ").into())
}

impl DataGridPanel {
    /// Render the DBeaver-style flat menu opened from a column header.
    /// Ordering actions are listed first, followed by all filter operators in
    /// the same panel (no nested flyouts).
    pub(super) fn render_column_header_menu_items(
        &self,
        menu: &TableContextMenu,
        backend: Option<FilterBackend>,
        cx: &mut Context<Self>,
    ) -> Vec<AnyElement> {
        let col_name = self
            .result
            .columns
            .get(menu.col)
            .map(|column| column.name.clone())
            .unwrap_or_default();
        let remove_ordering = dbflux_i18n::t!("document.data.context_menu.order.remove");
        let order_title = dbflux_i18n::t!("document.data.context_menu.order.title");
        let filter_title = dbflux_i18n::t!("document.data.context_menu.filter.title");
        let (_, _, filter_items, value_ops_count) = self.build_filter_items(menu, backend, cx);

        let mut rows: Vec<AnyElement> = Vec::new();
        rows.push(
            render_menu_header(
                &MenuItem::header(order_title).icon(AppIcon::ArrowUpDown),
                cx,
            )
            .into_any_element(),
        );

        let mut action_index = 0usize;
        let order_items = [
            (
                format!("{} ASC", col_name),
                ContextMenuAction::Order(dbflux_core::SortDirection::Ascending),
                AppIcon::ArrowUp,
                false,
            ),
            (
                format!("{} DESC", col_name),
                ContextMenuAction::Order(dbflux_core::SortDirection::Descending),
                AppIcon::ArrowDown,
                false,
            ),
            (
                remove_ordering,
                ContextMenuAction::RemoveOrdering,
                AppIcon::X,
                true,
            ),
        ];

        for (idx, (label, action, icon, is_danger)) in order_items.into_iter().enumerate() {
            if idx == 2 {
                rows.push(render_separator(cx).into_any_element());
            }
            rows.push(Self::column_menu_action_row(
                label,
                action,
                icon,
                is_danger,
                action_index,
                menu.selected_index,
                cx,
            ));
            action_index += 1;
        }

        rows.push(render_separator(cx).into_any_element());
        rows.push(
            render_menu_header(
                &MenuItem::header(filter_title).icon(AppIcon::ListFilter),
                cx,
            )
            .into_any_element(),
        );

        let remove_filter_index = filter_items.len().saturating_sub(1);
        for (idx, (label, action)) in filter_items.into_iter().enumerate() {
            if (value_ops_count > 0 && idx == value_ops_count) || idx == remove_filter_index {
                rows.push(render_separator(cx).into_any_element());
            }
            let is_danger = matches!(action, ContextMenuAction::RemoveFilter);
            rows.push(Self::column_menu_action_row(
                label,
                action,
                if is_danger {
                    AppIcon::X
                } else {
                    AppIcon::ListFilter
                },
                is_danger,
                action_index,
                menu.selected_index,
                cx,
            ));
            action_index += 1;
        }

        rows
    }

    fn column_menu_action_row(
        label: String,
        action: ContextMenuAction,
        icon: AppIcon,
        is_danger: bool,
        action_index: usize,
        selected_index: usize,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let mut item = MenuItem::new(label).icon(icon);
        if is_danger {
            item = item.danger();
        }

        menu_row(
            SharedString::from(format!("column-menu-action-{action_index}")),
            &item,
            action_index == selected_index,
            cx,
        )
        .on_mouse_move(cx.listener(move |this, _, _, cx| {
            if let Some(menu) = this.context_menu.as_mut()
                && menu.selected_index != action_index
            {
                menu.selected_index = action_index;
                cx.notify();
            }
        }))
        .on_click(cx.listener(move |this, _, window, cx| {
            this.handle_context_menu_action(action, window, cx);
        }))
        .into_any_element()
    }

    /// Renders the flat list of visible menu items (Copy, Paste, Edit, Add Row, ...)
    /// built from `build_context_menu_items`, including separators.
    pub(super) fn render_menu_item_rows(
        selected_index: usize,
        visible_items: &[ContextMenuItem],
        menu_items: &mut Vec<AnyElement>,
        visual_index: &mut usize,
        cx: &mut Context<Self>,
    ) {
        for item in visible_items {
            if item.is_separator {
                menu_items.push(render_separator(cx).into_any_element());
                *visual_index += 1;
                continue;
            }

            let Some(action) = item.action else {
                *visual_index += 1;
                continue;
            };

            let current_index = *visual_index;

            let mut row_item = MenuItem::new(item.label.clone());
            if let Some(icon) = item.icon {
                row_item = row_item.icon(icon);
            }
            if item.is_danger {
                row_item = row_item.danger();
            }
            if let Some(shortcut) = action_shortcut(action, cx) {
                row_item = row_item.shortcut(shortcut);
            }

            menu_items.push(
                menu_row(
                    item.label.clone(),
                    &row_item,
                    current_index == selected_index,
                    cx,
                )
                .on_mouse_move(cx.listener(move |this, _, _, cx| {
                    if let Some(ref mut menu) = this.context_menu
                        && (menu.selected_index != current_index || menu.any_submenu_open())
                    {
                        menu.selected_index = current_index;
                        menu.close_submenus();
                        cx.notify();
                    }
                }))
                .on_click(cx.listener(move |this, _, window, cx| {
                    this.handle_context_menu_action(action, window, cx);
                }))
                .into_any_element(),
            );

            *visual_index += 1;
        }
    }

    /// Renders the "Filter" submenu trigger and, when open, its flyout of
    /// value-based filter operators plus the "Remove filter" action.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn render_filter_submenu_section(
        &self,
        menu: &TableContextMenu,
        submenus_open_left: bool,
        backend: Option<FilterBackend>,
        has_filter: bool,
        selected_index: usize,
        menu_items: &mut Vec<AnyElement>,
        visual_index: &mut usize,
        cx: &mut Context<Self>,
    ) {
        if !has_filter {
            return;
        }

        menu_items.push(render_separator(cx).into_any_element());
        *visual_index += 1;

        let filter_submenu_open = menu.filter_submenu_open;
        let filter_index = *visual_index;
        let filter_selected = selected_index == filter_index;
        let submenu_selected_index = menu.submenu_selected_index;

        let (_col_name_display, filter_submenu_count, filter_items, value_ops_count) =
            self.build_filter_items(menu, backend, cx);

        let filter_title = dbflux_i18n::t!("document.data.context_menu.filter.title");
        let cell_value_label = dbflux_i18n::t!("document.data.context_menu.filter.cell_value");

        let trigger = MenuItem::new(filter_title)
            .icon(AppIcon::ListFilter)
            .submenu();

        menu_items.push(
            menu_row(
                "filter-trigger",
                &trigger,
                filter_selected || filter_submenu_open,
                cx,
            )
            .on_mouse_move(cx.listener(move |this, _, _, cx| {
                // Hovering opens the submenu, as native menus do; the
                // flyout is a child of this row, so moving into it keeps
                // bubbling here and the guard leaves it open.
                if let Some(ref mut menu) = this.context_menu
                    && !(menu.selected_index == filter_index && menu.filter_submenu_open)
                {
                    menu.selected_index = filter_index;
                    menu.close_submenus();
                    menu.filter_submenu_open = true;
                    menu.submenu_selected_index = 0;
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
            .when(filter_submenu_open, |d: Stateful<Div>| {
                d.child(submenu_frame(
                    submenus_open_left,
                    Self::build_filter_submenu_flyout(
                        filter_items,
                        value_ops_count,
                        filter_submenu_count,
                        submenu_selected_index,
                        cell_value_label,
                        cx,
                    ),
                ))
            })
            .into_any_element(),
        );
        *visual_index += 1;
    }

    /// Builds the absolute-positioned flyout listing filter operators for the
    /// current cell value plus the "Remove filter" action.
    fn build_filter_submenu_flyout(
        filter_items: Vec<(String, ContextMenuAction)>,
        value_ops_count: usize,
        filter_submenu_count: usize,
        submenu_selected_index: usize,
        cell_value_label: String,
        cx: &mut Context<Self>,
    ) -> Div {
        let value_section_separator_idx = (value_ops_count > 0).then_some(value_ops_count);
        let remove_separator_idx = filter_submenu_count.saturating_sub(1);

        let mut elements: Vec<AnyElement> = Vec::new();

        if value_ops_count > 0 {
            elements.push(
                render_menu_header(&MenuItem::header(cell_value_label), cx).into_any_element(),
            );
        }

        for (idx, (label, action)) in filter_items.into_iter().enumerate() {
            // Separators between the value operators and the IS NULL section,
            // and before "Remove filter".
            if value_section_separator_idx == Some(idx) || idx == remove_separator_idx {
                elements.push(render_separator(cx).into_any_element());
            }

            let is_remove = matches!(action, ContextMenuAction::RemoveFilter);
            let item = if is_remove {
                MenuItem::new(label).icon(AppIcon::X).danger()
            } else {
                MenuItem::new(label).icon(AppIcon::ListFilter)
            };

            elements.push(
                Self::submenu_action_row(
                    SharedString::from(format!("filter-{}", idx)),
                    &item,
                    idx,
                    submenu_selected_index,
                    action,
                    cx,
                )
                .into_any_element(),
            );
        }

        submenu_flyout(FILTER_SUBMENU_WIDTH, cx).children(elements)
    }

    /// A row inside a submenu flyout: hover moves the submenu selection and a
    /// click runs `action`.
    fn submenu_action_row(
        id: SharedString,
        item: &MenuItem,
        idx: usize,
        submenu_selected_index: usize,
        action: ContextMenuAction,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        menu_row(id, item, idx == submenu_selected_index, cx)
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
    }

    /// Renders the "Order" submenu trigger and its ASC/DESC/Remove ordering flyout.
    /// Only applicable to SQL table views (see `has_order` at the call site).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn render_order_submenu_section(
        &self,
        menu: &TableContextMenu,
        submenus_open_left: bool,
        has_order: bool,
        selected_index: usize,
        menu_items: &mut Vec<AnyElement>,
        visual_index: &mut usize,
        cx: &mut Context<Self>,
    ) {
        if !has_order {
            return;
        }

        let order_submenu_open = menu.order_submenu_open;
        let order_index = *visual_index;
        let order_selected = selected_index == order_index;
        let submenu_selected_index = menu.submenu_selected_index;

        let col_name_for_order = self
            .result
            .columns
            .get(menu.col)
            .map(|c| c.name.clone())
            .unwrap_or_default();

        let order_title = dbflux_i18n::t!("document.data.context_menu.order.title");
        let remove_ordering_label = dbflux_i18n::t!("document.data.context_menu.order.remove");

        let trigger = MenuItem::new(order_title)
            .icon(AppIcon::ArrowUpDown)
            .submenu();

        menu_items.push(
            menu_row(
                "order-trigger",
                &trigger,
                order_selected || order_submenu_open,
                cx,
            )
            .on_mouse_move(cx.listener(move |this, _, _, cx| {
                // Hovering opens the submenu, as native menus do; the
                // flyout is a child of this row, so moving into it keeps
                // bubbling here and the guard leaves it open.
                if let Some(ref mut menu) = this.context_menu
                    && !(menu.selected_index == order_index && menu.order_submenu_open)
                {
                    menu.selected_index = order_index;
                    menu.close_submenus();
                    menu.order_submenu_open = true;
                    menu.submenu_selected_index = 0;
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
            .when(order_submenu_open, |d: Stateful<Div>| {
                d.child(submenu_frame(
                    submenus_open_left,
                    Self::build_order_submenu_flyout(
                        &col_name_for_order,
                        submenu_selected_index,
                        remove_ordering_label,
                        cx,
                    ),
                ))
            })
            .into_any_element(),
        );
        *visual_index += 1;
    }

    /// Builds the absolute-positioned flyout listing ASC/DESC ordering plus
    /// "Remove ordering" for the current column.
    fn build_order_submenu_flyout(
        col_name_for_order: &str,
        submenu_selected_index: usize,
        remove_ordering_label: String,
        cx: &mut Context<Self>,
    ) -> Div {
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
                remove_ordering_label,
                ContextMenuAction::RemoveOrdering,
                AppIcon::X,
            ),
        ];

        let mut elements: Vec<AnyElement> = Vec::new();

        for (idx, (label, action, icon)) in order_items.into_iter().enumerate() {
            let is_remove = matches!(action, ContextMenuAction::RemoveOrdering);

            if is_remove {
                elements.push(render_separator(cx).into_any_element());
            }

            let mut item = MenuItem::new(label).icon(icon);
            if is_remove {
                item = item.danger();
            }

            elements.push(
                Self::submenu_action_row(
                    SharedString::from(format!("order-{}", idx)),
                    &item,
                    idx,
                    submenu_selected_index,
                    action,
                    cx,
                )
                .into_any_element(),
            );
        }

        submenu_flyout(ORDER_SUBMENU_WIDTH, cx).children(elements)
    }

    /// Renders the "Generate SQL" submenu trigger (SELECT WHERE / INSERT / UPDATE / DELETE
    /// templates). Only present for table views, never for the document view.
    pub(super) fn render_generate_sql_submenu_section(
        is_document_view: bool,
        menu: &TableContextMenu,
        submenus_open_left: bool,
        selected_index: usize,
        menu_items: &mut Vec<AnyElement>,
        visual_index: &mut usize,
        cx: &mut Context<Self>,
    ) {
        if is_document_view {
            return;
        }

        menu_items.push(render_separator(cx).into_any_element());
        *visual_index += 1; // Separator takes an index slot

        let sql_submenu_open = menu.sql_submenu_open;
        let gen_sql_index = *visual_index;
        let gen_sql_selected = selected_index == gen_sql_index;
        let submenu_selected_index = menu.submenu_selected_index;

        let generate_sql_title = dbflux_i18n::t!("document.data.context_menu.generate_sql.title");

        let trigger = MenuItem::new(generate_sql_title)
            .icon(AppIcon::Code)
            .submenu();

        menu_items.push(
            menu_row(
                "generate-sql-trigger",
                &trigger,
                gen_sql_selected || sql_submenu_open,
                cx,
            )
            .on_mouse_move(cx.listener(move |this, _, _, cx| {
                // Hovering opens the submenu, as native menus do; the
                // flyout is a child of this row, so moving into it keeps
                // bubbling here and the guard leaves it open.
                if let Some(ref mut menu) = this.context_menu
                    && !(menu.selected_index == gen_sql_index && menu.sql_submenu_open)
                {
                    menu.selected_index = gen_sql_index;
                    menu.close_submenus();
                    menu.sql_submenu_open = true;
                    menu.submenu_selected_index = 0;
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
            .when(sql_submenu_open, |d: Stateful<Div>| {
                d.child(submenu_frame(
                    submenus_open_left,
                    Self::build_generate_sql_submenu_flyout(submenu_selected_index, cx),
                ))
            })
            .into_any_element(),
        );
    }

    /// Builds the absolute-positioned flyout listing SELECT WHERE / INSERT / UPDATE /
    /// DELETE template generators.
    fn build_generate_sql_submenu_flyout(
        submenu_selected_index: usize,
        cx: &mut Context<Self>,
    ) -> Div {
        let rows: Vec<AnyElement> = [
            ("SELECT WHERE", ContextMenuAction::GenerateSelectWhere),
            ("INSERT", ContextMenuAction::GenerateInsert),
            ("UPDATE", ContextMenuAction::GenerateUpdate),
            ("DELETE", ContextMenuAction::GenerateDelete),
        ]
        .into_iter()
        .enumerate()
        .map(|(idx, (label, action))| {
            Self::submenu_action_row(
                SharedString::from(label),
                &MenuItem::new(label).icon(AppIcon::Code),
                idx,
                submenu_selected_index,
                action,
                cx,
            )
            .into_any_element()
        })
        .collect();

        submenu_flyout(GENERATE_SQL_SUBMENU_WIDTH, cx).children(rows)
    }

    /// Renders the "Copy as Query" submenu trigger (INSERT / UPDATE / DELETE templates
    /// for the current row), gated on driver support for query generation.
    pub(super) fn render_copy_query_submenu_section(
        &self,
        menu: &TableContextMenu,
        submenus_open_left: bool,
        selected_index: usize,
        menu_items: &mut Vec<AnyElement>,
        visual_index: &mut usize,
        cx: &mut Context<Self>,
    ) {
        if !self.has_copy_query_support() {
            return;
        }

        menu_items.push(render_separator(cx).into_any_element());
        *visual_index += 1;

        let copy_query_label = self.copy_query_submenu_label(cx);
        let copy_submenu_open = menu.copy_query_submenu_open;
        let copy_query_index = *visual_index;
        let copy_query_selected = selected_index == copy_query_index;
        let submenu_selected_index = menu.submenu_selected_index;

        let trigger = MenuItem::new(copy_query_label)
            .icon(AppIcon::Table)
            .submenu();

        menu_items.push(
            menu_row(
                "copy-query-trigger",
                &trigger,
                copy_query_selected || copy_submenu_open,
                cx,
            )
            .on_mouse_move(cx.listener(move |this, _, _, cx| {
                // Hovering opens the submenu, as native menus do; the
                // flyout is a child of this row, so moving into it keeps
                // bubbling here and the guard leaves it open.
                if let Some(ref mut menu) = this.context_menu
                    && !(menu.selected_index == copy_query_index && menu.copy_query_submenu_open)
                {
                    menu.selected_index = copy_query_index;
                    menu.close_submenus();
                    menu.copy_query_submenu_open = true;
                    menu.submenu_selected_index = 0;
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
            .when(copy_submenu_open, |d: Stateful<Div>| {
                d.child(submenu_frame(
                    submenus_open_left,
                    Self::build_copy_query_submenu_flyout(submenu_selected_index, cx),
                ))
            })
            .into_any_element(),
        );
    }

    /// Builds the absolute-positioned flyout listing INSERT / UPDATE / DELETE
    /// copy-as-query templates for the current row.
    fn build_copy_query_submenu_flyout(
        submenu_selected_index: usize,
        cx: &mut Context<Self>,
    ) -> Div {
        let rows: Vec<AnyElement> = [
            ("INSERT", ContextMenuAction::CopyAsInsert),
            ("UPDATE", ContextMenuAction::CopyAsUpdate),
            ("DELETE", ContextMenuAction::CopyAsDelete),
        ]
        .into_iter()
        .enumerate()
        .map(|(idx, (label, action))| {
            Self::submenu_action_row(
                SharedString::from(format!("copy-{}", label)),
                &MenuItem::new(label).icon(AppIcon::Columns),
                idx,
                submenu_selected_index,
                action,
                cx,
            )
            .into_any_element()
        })
        .collect();

        submenu_flyout(COPY_QUERY_SUBMENU_WIDTH, cx).children(rows)
    }

    /// Renders driver-supplied row actions (e.g. Kill / Cancel) as flat items at the
    /// bottom of the menu. Each item emits `RowActionRequested` directly on click
    /// rather than routing through `handle_context_menu_action`.
    pub(super) fn render_row_actions_section(
        menu: &TableContextMenu,
        selected_index: usize,
        menu_items: &mut Vec<AnyElement>,
        visual_index: &mut usize,
        cx: &mut Context<Self>,
    ) {
        if menu.row_actions.is_empty() {
            return;
        }

        let row = menu.row;
        let position = menu.position;

        menu_items.push(render_separator(cx).into_any_element());
        *visual_index += 1;

        for (action_slot, action) in menu.row_actions.iter().cloned().enumerate() {
            let current_index = *visual_index;
            let is_danger = action.is_destructive;

            let action_id = action.id.clone();
            let action_label = action.label.clone();
            let is_destructive = action.is_destructive;

            let mut item = MenuItem::new(action.label.clone()).icon(if is_danger {
                AppIcon::Power
            } else {
                AppIcon::Zap
            });
            if is_danger {
                item = item.danger();
            }

            menu_items.push(
                menu_row(
                    SharedString::from(format!("row-action-{}", action_slot)),
                    &item,
                    current_index == selected_index,
                    cx,
                )
                .on_mouse_move(cx.listener(move |this, _, _, cx| {
                    if let Some(ref mut menu) = this.context_menu
                        && (menu.selected_index != current_index || menu.any_submenu_open())
                    {
                        menu.selected_index = current_index;
                        menu.close_submenus();
                        cx.notify();
                    }
                }))
                .on_click(cx.listener(move |this, _, window, cx| {
                    let row_values = this.collect_row_values(row, cx);
                    this.context_menu = None;
                    this.restore_focus_after_context_menu(false, window, cx);
                    cx.emit(DataGridEvent::RowActionRequested {
                        row,
                        action_id: action_id.clone(),
                        action_label: action_label.clone(),
                        is_destructive,
                        row_values,
                        position,
                    });
                    cx.notify();
                }))
                .into_any_element(),
            );
            *visual_index += 1;
        }
    }

    /// Close the menu without running anything and hand focus back.
    fn dismiss_context_menu(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let is_document_view = self
            .context_menu
            .as_ref()
            .map(|menu| menu.is_document_view)
            .unwrap_or(false);

        self.context_menu = None;
        self.restore_focus_after_context_menu(is_document_view, window, cx);
        cx.notify();
    }

    /// Wraps the assembled `menu_items` in the deferred, window-level overlay: a
    /// full-size click-catcher (closes the menu) plus the positioned menu surface.
    pub(super) fn render_context_menu_overlay(
        &self,
        menu_x: Pixels,
        menu_y: Pixels,
        menu_width: Pixels,
        menu_items: Vec<AnyElement>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        // Use deferred() to render at window level for correct positioning
        deferred(
            div()
                .id("context-menu-overlay")
                .absolute()
                .top_0()
                .left_0()
                .size_full()
                .track_focus(&self.focus.context_menu_focus)
                // The grid reports the ContextMenu context while the menu is
                // open, so the keymap's menu keys arrive here first.
                .on_action(cx.listener(
                    |this, action: &dbflux_ui_base::keymap::RunCommand, window, cx| {
                        let handled = dbflux_ui_base::keymap::run_command(action)
                            .is_some_and(|command| this.dispatch_menu_command(command, window, cx));

                        if !handled {
                            cx.propagate();
                        }
                    },
                ))
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(|this, _, window, cx| this.dismiss_context_menu(window, cx)),
                )
                .on_mouse_down(
                    MouseButton::Right,
                    cx.listener(|this, _, window, cx| this.dismiss_context_menu(window, cx)),
                )
                // The overlay covers the grid, so a wheel here would scroll
                // nothing; closing instead keeps the menu from hanging over a
                // grid that has scrolled away from under it.
                .on_scroll_wheel(
                    cx.listener(|this, _, window, cx| this.dismiss_context_menu(window, cx)),
                )
                .child(
                    menu_frame(cx)
                        .id("context-menu")
                        .absolute()
                        .left(menu_x)
                        .top(menu_y)
                        .w(menu_width)
                        // No overflow clip here: the submenus are children
                        // of their rows and hang outside this panel. Long
                        // labels are truncated by their own rows instead.
                        .occlude()
                        .children(menu_items),
                ),
        )
        .with_priority(1)
    }
}

#[cfg(test)]
mod tests {
    /// The menu panel must not clip its contents: submenus are absolutely
    /// positioned children of their rows and hang past the panel's right
    /// edge, so a clip on the panel hides every one of them.
    #[test]
    fn menu_panel_does_not_clip_its_submenus() {
        let source = include_str!("sections.rs");
        let panel_start = source
            .find(".id(\"context-menu\")")
            .expect("the menu panel is built in this file");
        let panel = &source[panel_start..];
        let panel_end = panel
            .find(".children(menu_items)")
            .expect("the panel takes the menu items");
        assert!(
            !panel[..panel_end].contains(".overflow_hidden()"),
            "the menu panel clips its submenus"
        );
    }

    #[test]
    fn context_menu_sections_keys_resolve_in_both_locales() {
        let keys = [
            "document.data.context_menu.filter.title",
            "document.data.context_menu.filter.cell_value",
            "document.data.context_menu.order.title",
            "document.data.context_menu.order.remove",
            "document.data.context_menu.generate_sql.title",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn context_menu_filter_title_differs_between_locales() {
        let en = dbflux_i18n::t!("document.data.context_menu.filter.title", locale = "en");
        let es = dbflux_i18n::t!("document.data.context_menu.filter.title", locale = "es");

        assert_eq!(en, "Filter");
        assert_ne!(en, es);
    }

    #[test]
    fn render_menu_item_rows_hoists_translations_out_of_per_row_loop() {
        let source = include_str!("sections.rs");

        let function_name = "pub(super) fn render_menu_item_rows(";
        let start = source
            .find(function_name)
            .unwrap_or_else(|| panic!("{function_name} not found in sections.rs"));
        let after_signature = &source[start + function_name.len()..];
        let end = after_signature
            .find("\n    /// ")
            .unwrap_or(after_signature.len());
        let body = &after_signature[..end];

        assert!(
            !body.contains("dbflux_i18n::t!("),
            "render_menu_item_rows must not call t! per row; hoist any translated \
             label onto ContextMenuItem before this loop runs"
        );
    }
}
