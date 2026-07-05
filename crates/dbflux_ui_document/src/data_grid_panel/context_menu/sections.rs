use super::{ContextMenuItem, DataGridEvent, DataGridPanel, FilterBackend, TableContextMenu};
use dbflux_app::keymap::ContextId;
use dbflux_components::components::data_table::ContextMenuAction;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text, surface_raised};
use dbflux_components::tokens::{FontSizes, Heights, Radii, Spacing};
use gpui::prelude::FluentBuilder;
use gpui::{deferred, *};

impl DataGridPanel {
    /// Renders the flat list of visible menu items (Copy, Paste, Edit, Add Row, ...)
    /// built from `build_context_menu_items`, including separators.
    pub(super) fn render_menu_item_rows(
        theme: &gpui_component::theme::Theme,
        selected_index: usize,
        visible_items: &[ContextMenuItem],
        menu_items: &mut Vec<AnyElement>,
        visual_index: &mut usize,
        cx: &mut Context<Self>,
    ) {
        for item in visible_items {
            if item.is_separator {
                menu_items.push(
                    div()
                        .h(px(1.0))
                        .mx(Spacing::SM)
                        .my(Spacing::XS)
                        .bg(theme.border)
                        .into_any_element(),
                );
                *visual_index += 1;
                continue;
            }

            let Some(action) = item.action else {
                *visual_index += 1;
                continue;
            };

            let is_selected = *visual_index == selected_index;
            let is_danger = item.is_danger;
            let label = item.label;
            let icon = item.icon;
            let current_index = *visual_index;

            let label_color = if is_danger {
                theme.danger
            } else {
                theme.foreground
            };

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
                    .when(is_selected, |d| {
                        d.bg(if is_danger {
                            theme.danger.opacity(0.1)
                        } else {
                            theme.accent
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
                        d.child(Icon::new(icon).small().color(if is_danger {
                            theme.danger
                        } else if is_selected {
                            theme.accent_foreground
                        } else {
                            theme.muted_foreground
                        }))
                    })
                    .when(icon.is_none(), |d| d.pl(px(20.0)))
                    .child(Text::caption(label).color(if is_selected {
                        if is_danger {
                            theme.danger
                        } else {
                            theme.accent_foreground
                        }
                    } else {
                        label_color
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
        backend: Option<FilterBackend>,
        has_filter: bool,
        selected_index: usize,
        theme: &gpui_component::theme::Theme,
        menu_items: &mut Vec<AnyElement>,
        visual_index: &mut usize,
        cx: &mut Context<Self>,
    ) {
        if !has_filter {
            return;
        }

        menu_items.push(
            div()
                .h(px(1.0))
                .mx(Spacing::SM)
                .my(Spacing::XS)
                .bg(theme.border)
                .into_any_element(),
        );
        *visual_index += 1;

        let filter_submenu_open = menu.filter_submenu_open;
        let submenu_fg = theme.foreground;
        let submenu_hover = theme.secondary;
        let filter_index = *visual_index;
        let filter_selected = selected_index == filter_index;
        let submenu_selected_index = menu.submenu_selected_index;

        let (_col_name_display, filter_submenu_count, filter_items, value_ops_count) =
            self.build_filter_items(menu, backend, cx);

        let filter_label_color = if filter_selected && !filter_submenu_open {
            theme.accent_foreground
        } else {
            submenu_fg
        };

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
                        .child(
                            Icon::new(AppIcon::ListFilter)
                                .small()
                                .color(filter_label_color),
                        )
                        .child(Text::caption("Filter").color(filter_label_color)),
                )
                .child(Icon::new(AppIcon::ChevronRight).small().color(
                    if filter_selected && !filter_submenu_open {
                        theme.accent_foreground
                    } else {
                        theme.muted_foreground
                    },
                ))
                .when(filter_submenu_open, |d: Stateful<Div>| {
                    d.child(Self::build_filter_submenu_flyout(
                        filter_items,
                        value_ops_count,
                        filter_submenu_count,
                        submenu_selected_index,
                        theme,
                        cx,
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
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> Div {
        let submenu_bg = theme.popover;
        let submenu_border = theme.border;
        let submenu_fg = theme.foreground;
        let submenu_hover = theme.secondary;

        let value_section_separator_idx = (value_ops_count > 0).then_some(value_ops_count);
        let remove_separator_idx = filter_submenu_count.saturating_sub(1);

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
                        .child(Text::caption("Cell value").font_size(FontSizes::XS)),
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
                        let is_remove = matches!(action, ContextMenuAction::RemoveFilter);
                        let label_shared = SharedString::from(format!("filter-{}", idx));

                        let item_color = if is_remove {
                            theme.danger
                        } else if is_submenu_selected {
                            theme.accent_foreground
                        } else {
                            submenu_fg
                        };

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
                                .when(is_submenu_selected && !is_remove, |d| d.bg(theme.accent))
                                .when(is_submenu_selected && is_remove, |d| {
                                    d.bg(theme.danger.opacity(0.1))
                                })
                                .when(!is_submenu_selected, |d| d.hover(|d| d.bg(submenu_hover)))
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
                                .child(Text::caption(label.clone()).color(item_color))
                                .into_any_element(),
                        );

                        elements
                    })
                    .collect::<Vec<_>>(),
            )
    }

    /// Renders the "Order" submenu trigger and its ASC/DESC/Remove ordering flyout.
    /// Only applicable to SQL table views (see `has_order` at the call site).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn render_order_submenu_section(
        &self,
        menu: &TableContextMenu,
        has_order: bool,
        selected_index: usize,
        theme: &gpui_component::theme::Theme,
        menu_items: &mut Vec<AnyElement>,
        visual_index: &mut usize,
        cx: &mut Context<Self>,
    ) {
        if !has_order {
            return;
        }

        let submenu_fg = theme.foreground;
        let submenu_hover = theme.secondary;
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

        let order_label_color = if order_selected && !order_submenu_open {
            theme.accent_foreground
        } else {
            submenu_fg
        };

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
                        .child(
                            Icon::new(AppIcon::ArrowUpDown)
                                .small()
                                .color(order_label_color),
                        )
                        .child(Text::caption("Order").color(order_label_color)),
                )
                .child(Icon::new(AppIcon::ChevronRight).small().color(
                    if order_selected && !order_submenu_open {
                        theme.accent_foreground
                    } else {
                        theme.muted_foreground
                    },
                ))
                .when(order_submenu_open, |d: Stateful<Div>| {
                    d.child(Self::build_order_submenu_flyout(
                        &col_name_for_order,
                        submenu_selected_index,
                        theme,
                        cx,
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
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> Div {
        let submenu_bg = theme.popover;
        let submenu_border = theme.border;
        let submenu_fg = theme.foreground;
        let submenu_hover = theme.secondary;

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
                        let is_remove = matches!(action, ContextMenuAction::RemoveOrdering);

                        let order_item_color = if is_remove {
                            theme.danger
                        } else if is_submenu_selected {
                            theme.accent_foreground
                        } else {
                            submenu_fg
                        };

                        elements.push(
                            div()
                                .id(SharedString::from(format!("order-{}", idx)))
                                .flex()
                                .items_center()
                                .gap(Spacing::SM)
                                .h(Heights::ROW_COMPACT)
                                .px(Spacing::SM)
                                .mx(Spacing::XS)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .when(is_submenu_selected && !is_remove, |d| d.bg(theme.accent))
                                .when(is_submenu_selected && is_remove, |d| {
                                    d.bg(theme.danger.opacity(0.1))
                                })
                                .when(!is_submenu_selected, |d| d.hover(|d| d.bg(submenu_hover)))
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
                                .child(Icon::new(icon).small().color(if is_remove {
                                    theme.danger
                                } else if is_submenu_selected {
                                    theme.accent_foreground
                                } else {
                                    theme.muted_foreground
                                }))
                                .child(Text::caption(label).color(order_item_color))
                                .into_any_element(),
                        );

                        elements
                    })
                    .collect::<Vec<_>>(),
            )
    }

    /// Renders the "Generate SQL" submenu trigger (SELECT WHERE / INSERT / UPDATE / DELETE
    /// templates). Only present for table views, never for the document view.
    pub(super) fn render_generate_sql_submenu_section(
        is_document_view: bool,
        menu: &TableContextMenu,
        selected_index: usize,
        theme: &gpui_component::theme::Theme,
        menu_items: &mut Vec<AnyElement>,
        visual_index: &mut usize,
        cx: &mut Context<Self>,
    ) {
        if is_document_view {
            return;
        }

        // Add separator before "Generate SQL"
        menu_items.push(
            div()
                .h(px(1.0))
                .mx(Spacing::SM)
                .my(Spacing::XS)
                .bg(theme.border)
                .into_any_element(),
        );
        *visual_index += 1; // Separator takes an index slot

        // "Generate SQL" submenu trigger
        let sql_submenu_open = menu.sql_submenu_open;
        let submenu_fg = theme.foreground;
        let submenu_hover = theme.secondary;
        let gen_sql_index = *visual_index; // Index for Generate SQL item
        let gen_sql_selected = selected_index == gen_sql_index;
        let submenu_selected_index = menu.submenu_selected_index;

        let gen_sql_label_color = if gen_sql_selected && !sql_submenu_open {
            theme.accent_foreground
        } else {
            submenu_fg
        };

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
                        .child(Icon::new(AppIcon::Code).small().color(gen_sql_label_color))
                        .child(Text::caption("Generate SQL").color(gen_sql_label_color)),
                )
                .child(Icon::new(AppIcon::ChevronRight).small().color(
                    if gen_sql_selected && !sql_submenu_open {
                        theme.accent_foreground
                    } else {
                        theme.muted_foreground
                    },
                ))
                // Submenu appears to the right
                .when(sql_submenu_open, |d: Stateful<Div>| {
                    d.child(Self::build_generate_sql_submenu_flyout(
                        submenu_selected_index,
                        theme,
                        cx,
                    ))
                })
                .into_any_element(),
        );
    }

    /// Builds the absolute-positioned flyout listing SELECT WHERE / INSERT / UPDATE /
    /// DELETE template generators.
    fn build_generate_sql_submenu_flyout(
        submenu_selected_index: usize,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> Div {
        let submenu_bg = theme.popover;
        let submenu_border = theme.border;
        let submenu_fg = theme.foreground;
        let submenu_hover = theme.secondary;

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
                    let sql_item_color = if is_submenu_selected {
                        theme.accent_foreground
                    } else {
                        submenu_fg
                    };

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
                        .when(is_submenu_selected, |d| d.bg(theme.accent))
                        .when(!is_submenu_selected, |d| d.hover(|d| d.bg(submenu_hover)))
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
                            Icon::new(AppIcon::Code)
                                .small()
                                .color(if is_submenu_selected {
                                    theme.accent_foreground
                                } else {
                                    theme.muted_foreground
                                }),
                        )
                        .child(Text::caption(label).color(sql_item_color))
                })
                .collect::<Vec<_>>(),
            )
    }

    /// Renders the "Copy as Query" submenu trigger (INSERT / UPDATE / DELETE templates
    /// for the current row), gated on driver support for query generation.
    pub(super) fn render_copy_query_submenu_section(
        &self,
        menu: &TableContextMenu,
        selected_index: usize,
        theme: &gpui_component::theme::Theme,
        menu_items: &mut Vec<AnyElement>,
        visual_index: &mut usize,
        cx: &mut Context<Self>,
    ) {
        if !self.has_copy_query_support() {
            return;
        }

        menu_items.push(
            div()
                .h(px(1.0))
                .mx(Spacing::SM)
                .my(Spacing::XS)
                .bg(theme.border)
                .into_any_element(),
        );
        *visual_index += 1;

        let copy_query_label = self.copy_query_submenu_label(cx);
        let copy_submenu_open = menu.copy_query_submenu_open;
        let submenu_fg = theme.foreground;
        let submenu_hover = theme.secondary;
        let copy_query_index = *visual_index;
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
                        .child(Icon::new(AppIcon::Columns).small().color(
                            if copy_query_selected && !copy_submenu_open {
                                theme.accent_foreground
                            } else {
                                submenu_fg
                            },
                        ))
                        .child(copy_query_label),
                )
                .child(Icon::new(AppIcon::ChevronRight).small().color(
                    if copy_query_selected && !copy_submenu_open {
                        theme.accent_foreground
                    } else {
                        theme.muted_foreground
                    },
                ))
                .when(copy_submenu_open, |d: Stateful<Div>| {
                    d.child(Self::build_copy_query_submenu_flyout(
                        submenu_selected_index,
                        theme,
                        cx,
                    ))
                })
                .into_any_element(),
        );
    }

    /// Builds the absolute-positioned flyout listing INSERT / UPDATE / DELETE
    /// copy-as-query templates for the current row.
    fn build_copy_query_submenu_flyout(
        submenu_selected_index: usize,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> Div {
        let submenu_bg = theme.popover;
        let submenu_border = theme.border;
        let submenu_fg = theme.foreground;
        let submenu_hover = theme.secondary;

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
                    let copy_item_color = if is_submenu_selected {
                        theme.accent_foreground
                    } else {
                        submenu_fg
                    };
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
                        .when(is_submenu_selected, |d| d.bg(theme.accent))
                        .when(!is_submenu_selected, |d| d.hover(|d| d.bg(submenu_hover)))
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
                            Icon::new(AppIcon::Columns)
                                .small()
                                .color(if is_submenu_selected {
                                    theme.accent_foreground
                                } else {
                                    theme.muted_foreground
                                }),
                        )
                        .child(Text::caption(label).color(copy_item_color))
                })
                .collect::<Vec<_>>(),
            )
    }

    /// Renders driver-supplied row actions (e.g. Kill / Cancel) as flat items at the
    /// bottom of the menu. Each item emits `RowActionRequested` directly on click
    /// rather than routing through `handle_context_menu_action`.
    pub(super) fn render_row_actions_section(
        menu: &TableContextMenu,
        selected_index: usize,
        theme: &gpui_component::theme::Theme,
        menu_items: &mut Vec<AnyElement>,
        visual_index: &mut usize,
        cx: &mut Context<Self>,
    ) {
        if menu.row_actions.is_empty() {
            return;
        }

        let row = menu.row;
        let position = menu.position;

        menu_items.push(
            div()
                .h(px(1.0))
                .mx(Spacing::SM)
                .my(Spacing::XS)
                .bg(theme.border)
                .into_any_element(),
        );
        *visual_index += 1;

        for (action_slot, action) in menu.row_actions.iter().cloned().enumerate() {
            let current_index = *visual_index;
            let is_selected = current_index == selected_index;
            let is_danger = action.is_destructive;

            let label_color = if is_danger {
                theme.danger
            } else {
                theme.foreground
            };

            let action_id = action.id.clone();
            let action_label = action.label.clone();
            let is_destructive = action.is_destructive;

            menu_items.push(
                div()
                    .id(SharedString::from(format!("row-action-{}", action_slot)))
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .h(Heights::ROW_COMPACT)
                    .px(Spacing::SM)
                    .mx(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .text_size(FontSizes::SM)
                    .when(is_selected, |d| {
                        d.bg(if is_danger {
                            theme.danger.opacity(0.1)
                        } else {
                            theme.accent
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
                    .child(
                        Icon::new(if is_danger {
                            AppIcon::Power
                        } else {
                            AppIcon::Zap
                        })
                        .small()
                        .color(if is_selected {
                            if is_danger {
                                theme.danger
                            } else {
                                theme.accent_foreground
                            }
                        } else {
                            label_color
                        }),
                    )
                    .child(Text::caption(action.label.clone()).color(if is_selected {
                        if is_danger {
                            theme.danger
                        } else {
                            theme.accent_foreground
                        }
                    } else {
                        label_color
                    }))
                    .into_any_element(),
            );
            *visual_index += 1;
        }
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
                .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                    use dbflux_app::keymap::KeyChord;
                    use dbflux_ui_base::keymap::{default_keymap, key_chord_from_gpui};

                    let chord = key_chord_from_gpui(&event.keystroke);
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
                    surface_raised(cx)
                        .id("context-menu")
                        .absolute()
                        .left(menu_x)
                        .top(menu_y)
                        .w(menu_width)
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
}
