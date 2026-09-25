use super::render_tree::{TreeRenderParams, render_tree_item};
use super::*;
use crate::connection_failure::ConnectionFailure;
use dbflux_components::controls::Button;
use dbflux_components::icons::DriverIconTone;
use dbflux_components::primitives::{Chamfer, ChamferRing, Icon, Kbd, Text};
use dbflux_components::tokens::{
    ChamferCut, ChromeColors, Fields, HeaderMetrics, ShellMetrics, SyntaxColors,
};

/// Section label of the sidebar header for `tab`, tinted while the sidebar
/// has keyboard focus.
fn sidebar_header_label(tab: SidebarTab, focused: bool, tint: Hsla) -> Text {
    let title = match tab {
        SidebarTab::Connections => dbflux_i18n::t!("sidebar.tabs.connections"),
        SidebarTab::Scripts => dbflux_i18n::t!("sidebar.tabs.scripts"),
    };

    let label = Text::label(title).font_size(ShellMetrics::SECTION_LABEL_FONT);

    if focused { label.color(tint) } else { label }
}

impl Sidebar {
    /// Header of the active view (AppByzTable): its section label, then the
    /// add menu and the filter buttons.
    fn render_header(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let tint = ChromeColors::tint(cx.theme());
        let sidebar_for_add = cx.entity().clone();
        let sidebar_for_filter = cx.entity().clone();

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(HeaderMetrics::PANEL_GAP)
            .h(ShellMetrics::SIDEBAR_HEADER_HEIGHT)
            .pl(HeaderMetrics::PANEL_PADDING_LEFT)
            .pr(HeaderMetrics::PANEL_PADDING_RIGHT)
            .child(sidebar_header_label(
                self.active_tab,
                self.connections_focused,
                tint,
            ))
            .child(div().flex_1())
            .child(
                Button::new("sidebar-add", dbflux_i18n::t!("sidebar.header.add"))
                    .small()
                    .icon(AppIcon::Plus)
                    .icon_size(ShellMetrics::SIDEBAR_FILTER_ICON)
                    .icon_only()
                    .tab_stop(false)
                    .on_click(move |_, _, cx| {
                        sidebar_for_add.update(cx, |this, cx| {
                            this.toggle_add_menu(cx);
                        });
                    }),
            )
            .child(
                Button::new("sidebar-filter", dbflux_i18n::t!("sidebar.header.filter"))
                    .small()
                    .icon(AppIcon::ListFilter)
                    .icon_size(ShellMetrics::SIDEBAR_FILTER_ICON)
                    .icon_only()
                    .tab_stop(false)
                    .on_click(move |_, window, cx| {
                        sidebar_for_filter.update(cx, |this, cx| {
                            this.focus_active_search(window, cx);
                        });
                    }),
            )
    }

    /// The filter field of the active view: a 30 px chamfered field with a
    /// search icon, the frameless input and the `/` keycap that focuses it.
    fn render_filter_field(
        &self,
        input: &Entity<InputState>,
        query_is_empty: bool,
        focused: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        let mut shape = Chamfer::new(ChamferCut::CONTROL)
            .fill(theme.popover)
            .border(theme.border);

        if focused {
            shape = shape.ring(ChamferRing::focus(ChromeColors::tint(theme)));
        }

        div()
            .flex_shrink_0()
            .px(ShellMetrics::SIDEBAR_FILTER_PADDING_X)
            .pb(ShellMetrics::SIDEBAR_FILTER_PADDING_BOTTOM)
            .child(
                div()
                    .relative()
                    .flex()
                    .items_center()
                    .gap(Fields::GAP)
                    .h(Fields::HEIGHT)
                    .px(Fields::PADDING_X)
                    .text_size(Fields::TEXT)
                    .child(shape)
                    .child(
                        Icon::new(AppIcon::Search)
                            .size(ShellMetrics::SIDEBAR_FILTER_ICON)
                            .color(theme.muted_foreground),
                    )
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .child(Input::new(input).xsmall().appearance(false).cleanable(true)),
                    )
                    .when(query_is_empty, |field| field.child(Kbd::new("/"))),
            )
    }

    fn render_action_bars(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .flex_shrink_0()
            .when(self.pending_delete_item.is_some(), |el| {
                el.child(
                    div()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .border_b_1()
                        .border_color(theme.border)
                        .bg(theme.danger.opacity(0.15))
                        .flex()
                        .items_center()
                        .justify_center()
                        .child(
                            Text::body(dbflux_i18n::t!("sidebar.confirm.delete_hint"))
                                .font_size(FontSizes::SM),
                        ),
                )
            })
    }

    fn render_connections_content(
        &self,
        tree_params: TreeRenderParams,
        sidebar_entity: &Entity<Self>,
        filter_field: AnyElement,
    ) -> impl IntoElement {
        let has_entries = self.visible_entry_count > 0;
        let sidebar_for_root_drop = sidebar_entity.clone();
        let sidebar_for_clear_drop = sidebar_entity.clone();
        let sidebar_for_hover_clear = sidebar_entity.clone();

        div()
            .flex_1()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div()
                    // Clear row hover when the pointer enters the search bar.
                    // Workaround for GPUI 0.2.2 lacking on_mouse_leave.
                    .on_mouse_move(move |_, _, cx| {
                        sidebar_for_hover_clear.update(cx, |this, cx| {
                            if this.hovered_item_id.is_some() {
                                this.hovered_item_id = None;
                                cx.notify();
                            }
                        });
                    })
                    .child(filter_field),
            )
            .when(has_entries, |el| {
                el.child(
                    div()
                        .flex_1()
                        .overflow_hidden()
                        .on_drop(move |state: &SidebarDragState, _, cx| {
                            sidebar_for_root_drop.update(cx, |this, cx| {
                                this.stop_auto_scroll(cx);
                                this.clear_drop_target(cx);
                                this.clear_drag_hover_folder(cx);
                                this.handle_drop(state, None, cx);
                            });
                        })
                        .on_drag_move::<SidebarDragState>(move |_, _, cx| {
                            sidebar_for_clear_drop.update(cx, |this, cx| {
                                this.stop_auto_scroll(cx);
                                this.clear_drop_target(cx);
                                this.clear_drag_hover_folder(cx);
                            });
                        })
                        .child(tree(
                            &self.tree_state,
                            move |ix, entry, selected, _window, cx| {
                                render_tree_item(&tree_params, ix, entry, selected, cx)
                            },
                        )),
                )
            })
            .when(!has_entries, |el| {
                el.child(
                    div()
                        .flex_1()
                        .flex()
                        .flex_col()
                        .items_center()
                        .justify_center()
                        .gap(Spacing::SM)
                        .px(Spacing::MD)
                        .child(
                            Text::body(dbflux_i18n::t!("sidebar.empty.connections_title"))
                                .muted_foreground(),
                        )
                        .child(
                            Text::body(dbflux_i18n::t!("sidebar.empty.connections_hint"))
                                .muted_foreground(),
                        ),
                )
            })
    }

    fn render_scripts_content(
        &mut self,
        filter_field: AnyElement,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let syntax_colors = SyntaxColors::for_current(cx);
        let sidebar_entity = cx.entity().clone();
        let sidebar_for_root_drop = sidebar_entity.clone();
        let sidebar_for_clear_drop = sidebar_entity.clone();

        let has_entries = self
            .app_state
            .read(cx)
            .scripts_directory()
            .map(|d| !d.is_empty())
            .unwrap_or(false);

        let has_search = !self.scripts_search_query.is_empty();

        let tree_params = TreeRenderParams {
            connections: Vec::new(),
            connect_failures: HashMap::new(),
            failure_details: HashMap::new(),
            connecting: HashSet::new(),
            code_gen_capabilities: HashMap::new(),
            active_id: None,
            profile_icons: HashMap::new(),
            profile_icon_colors: HashMap::new(),
            connection_latencies: HashMap::new(),
            active_databases: HashMap::new(),
            sidebar_entity: sidebar_entity.clone(),
            multi_selection: self.scripts_multi_selection.clone(),
            pending_delete: self.pending_delete_item.clone(),
            drop_target: None,
            scripts_drop_target: self.scripts_drop_target.clone(),
            editing_id: None,
            editing_script_path: self.editing_script_path.clone(),
            rename_input: self.rename_input.clone(),
            gutter_metadata: self.scripts_gutter_metadata.clone(),
            line_color: tree_nav::tree_line_color(theme),
            hovered_item_id: self.hovered_item_id.clone(),
            color_teal: syntax_colors.table(),
            color_yellow: syntax_colors.view(),
            color_blue: syntax_colors.column(),
            color_purple: syntax_colors.type_item(),
            color_gray: syntax_colors.folder_dim(),
            color_orange: syntax_colors.database(),
            color_schema: syntax_colors.schema(),
            color_green: theme.success,
        };

        div()
            .flex_1()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(filter_field)
            // Tree or empty state
            .when(has_entries || has_search, |el| {
                el.child(
                    div()
                        .flex_1()
                        .overflow_hidden()
                        .on_drop(move |state: &ScriptsDragState, _, cx| {
                            sidebar_for_root_drop.update(cx, |this, cx| {
                                this.scripts_drop_target = None;
                                this.handle_script_drop_to_root_with_position(state, cx);
                            });
                        })
                        .on_drag_move::<ScriptsDragState>(move |_, _, cx| {
                            sidebar_for_clear_drop.update(cx, |this, cx| {
                                this.scripts_drop_target = None;
                                cx.notify();
                            });
                        })
                        .child(tree(
                            &self.scripts_tree_state,
                            move |ix, entry, selected, _window, cx| {
                                render_tree_item(&tree_params, ix, entry, selected, cx)
                            },
                        )),
                )
            })
            .when(!has_entries && !has_search, |el| {
                el.child(
                    div()
                        .flex_1()
                        .flex()
                        .flex_col()
                        .items_center()
                        .justify_center()
                        .gap(Spacing::SM)
                        .px(Spacing::MD)
                        .child(
                            Text::body(dbflux_i18n::t!("sidebar.empty.scripts_title"))
                                .muted_foreground(),
                        )
                        .child(
                            Text::body(dbflux_i18n::t!("sidebar.empty.scripts_hint"))
                                .muted_foreground(),
                        ),
                )
            })
    }
}

impl Render for Sidebar {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        dbflux_ui_base::toast::flush_pending_toast(self.pending_toast.take(), window, cx);

        if let Some(item_id) = self.pending_rename_item.take() {
            self.start_rename(&item_id, window, cx);
        }

        if let Some(item_id) = self.pending_child_picker_item.take() {
            self.open_child_picker_modal(&item_id, window, cx);
        }

        let (search_input, query_is_empty) = match self.active_tab {
            SidebarTab::Connections => (
                self.connections_search_input.clone(),
                self.connections_search_query.is_empty(),
            ),
            SidebarTab::Scripts => (
                self.scripts_search_input.clone(),
                self.scripts_search_query.is_empty(),
            ),
        };
        let search_focused = search_input.read(cx).focus_handle(cx).is_focused(window);
        let filter_field = self
            .render_filter_field(&search_input, query_is_empty, search_focused, cx)
            .into_any_element();

        let theme = cx.theme();
        let syntax_colors = SyntaxColors::for_current(cx);
        let state = self.app_state.read(cx);
        let active_id = state.active_connection_id();
        let connections = state.connections().keys().copied().collect::<Vec<_>>();

        let profile_icons: HashMap<Uuid, AppIcon> = state
            .profiles()
            .iter()
            .filter_map(|p| {
                state.drivers().get(&p.driver_id()).map(|driver| {
                    let metadata = driver.metadata();
                    (p.id, AppIcon::for_driver(metadata.icon, metadata.category))
                })
            })
            .collect();

        let profile_icon_colors: HashMap<Uuid, Hsla> = state
            .profiles()
            .iter()
            .filter_map(|p| {
                state.drivers().get(&p.driver_id()).map(|driver| {
                    let metadata = driver.metadata();
                    (
                        p.id,
                        DriverIconTone::for_driver(metadata.icon, metadata.category).resolve(cx),
                    )
                })
            })
            .collect();

        let connect_failures: HashMap<Uuid, SharedString> = state
            .profiles()
            .iter()
            .filter_map(|profile| {
                state.connect_failure(profile.id).map(|error| {
                    (
                        profile.id,
                        SharedString::from(crate::labels::connect_failed_tooltip_label(error)),
                    )
                })
            })
            .collect();

        let failure_details: HashMap<Uuid, ConnectionFailure> = state
            .profiles()
            .iter()
            .filter_map(|profile| {
                state
                    .connect_failure(profile.id)
                    .map(|error| (profile.id, ConnectionFailure::from_error(error)))
            })
            .collect();

        let connecting: HashSet<Uuid> = state
            .profiles()
            .iter()
            .map(|profile| profile.id)
            .filter(|profile_id| state.is_operation_pending(*profile_id, None))
            .collect();

        let code_gen_capabilities: HashMap<Uuid, CodeGenCapabilities> = state
            .connections()
            .iter()
            .map(|(profile_id, connected)| {
                (*profile_id, connected.connection.code_gen_capabilities())
            })
            .collect();

        let active_databases = self.active_databases.clone();
        let sidebar_entity = cx.entity().clone();
        let multi_selection = self.multi_selection.clone();
        let pending_delete = self.pending_delete_item.clone();

        let tree_params = TreeRenderParams {
            connections,
            connect_failures,
            failure_details,
            connecting,
            code_gen_capabilities,
            active_id,
            profile_icons,
            profile_icon_colors,
            connection_latencies: HashMap::new(),
            active_databases,
            sidebar_entity: sidebar_entity.clone(),
            multi_selection,
            pending_delete,
            drop_target: self.drop_target.clone(),
            scripts_drop_target: None,
            editing_id: self.editing_id,
            editing_script_path: None,
            rename_input: self.rename_input.clone(),
            gutter_metadata: self.gutter_metadata.clone(),
            line_color: tree_nav::tree_line_color(theme),
            hovered_item_id: self.hovered_item_id.clone(),
            color_teal: syntax_colors.table(),
            color_yellow: syntax_colors.view(),
            color_blue: syntax_colors.column(),
            color_purple: syntax_colors.type_item(),
            color_gray: syntax_colors.folder_dim(),
            color_orange: syntax_colors.database(),
            color_schema: syntax_colors.schema(),
            color_green: theme.success,
        };

        let active_tab = self.active_tab;

        let sidebar_for_footer_hover = sidebar_entity.clone();
        let sidebar_for_header_hover = sidebar_entity.clone();
        let sidebar_background = theme.sidebar;

        let content = match active_tab {
            SidebarTab::Connections => self
                .render_connections_content(tree_params, &sidebar_entity, filter_field)
                .into_any_element(),
            SidebarTab::Scripts => self
                .render_scripts_content(filter_field, cx)
                .into_any_element(),
        };

        // No right border here — the outer `SidebarDock` already paints
        // `border_r_1`. A second border on this inner container produced the
        // visible double-line between the sidebar and the workspace.
        div()
            .relative()
            .flex()
            .flex_col()
            .size_full()
            .bg(sidebar_background)
            .child(
                // Header: clear row hover when mouse enters this region.
                div()
                    .on_mouse_move(move |_, _, cx| {
                        sidebar_for_header_hover.update(cx, |this, cx| {
                            if this.hovered_item_id.is_some() {
                                this.hovered_item_id = None;
                                cx.notify();
                            }
                        });
                    })
                    .child(self.render_header(cx)),
            )
            .child(self.render_action_bars(cx))
            .child(content)
            .child(
                // Footer: clear row hover when mouse enters this region.
                div()
                    .on_mouse_move(move |_, _, cx| {
                        sidebar_for_footer_hover.update(cx, |this, cx| {
                            if this.hovered_item_id.is_some() {
                                this.hovered_item_id = None;
                                cx.notify();
                            }
                        });
                    })
                    .child(self.render_footer(cx)),
            )
            .when(self.add_menu_open, |el| el.child(self.render_add_menu(cx)))
    }
}

#[cfg(test)]
mod tests {
    use super::sidebar_header_label;
    use crate::SidebarTab;
    use dbflux_components::primitives::TextVariant;
    use dbflux_components::tokens::ShellMetrics;

    #[test]
    fn header_label_names_the_active_view_as_a_section_label() {
        let connections =
            sidebar_header_label(SidebarTab::Connections, false, gpui::red()).inspect();
        let scripts = sidebar_header_label(SidebarTab::Scripts, false, gpui::red()).inspect();

        for inspection in [connections, scripts] {
            assert_eq!(inspection.variant, TextVariant::Label);
            assert_eq!(
                inspection.size_override,
                Some(ShellMetrics::SECTION_LABEL_FONT)
            );
            assert!(inspection.uses_role_default_color);
        }
    }

    #[test]
    fn header_label_takes_the_tint_while_the_sidebar_has_focus() {
        let focused = sidebar_header_label(SidebarTab::Connections, true, gpui::red()).inspect();

        assert!(focused.has_custom_color_override);
    }
}
