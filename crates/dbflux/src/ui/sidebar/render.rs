use super::render_tree::{TreeRenderParams, render_tree_item};
use super::*;

impl Render for Sidebar {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        crate::ui::toast::flush_pending_toast(self.pending_toast.take(), window, cx);

        if let Some(item_id) = self.pending_rename_item.take() {
            self.start_rename(&item_id, window, cx);
        }

        let theme = cx.theme();
        let state = self.app_state.read(cx);
        let active_id = state.active_connection_id();
        let connections = state.connections().keys().copied().collect::<Vec<_>>();

        // Pre-compute profile_id -> Icon map for use in the tree closure
        // (closure requires 'static, so we can't borrow state inside it)
        let profile_icons: HashMap<Uuid, dbflux_core::Icon> = state
            .profiles()
            .iter()
            .filter_map(|p| {
                state
                    .drivers()
                    .get(&p.kind())
                    .map(|driver| (p.id, driver.metadata().icon))
            })
            .collect();

        let active_databases = self.active_databases.clone();
        let sidebar_entity = cx.entity().clone();
        let multi_selection = self.multi_selection.clone();
        let pending_delete = self.pending_delete_item.clone();

        let color_green: Hsla = gpui::green();

        let tree_params = TreeRenderParams {
            connections,
            active_id,
            profile_icons,
            active_databases,
            sidebar_entity: sidebar_entity.clone(),
            multi_selection,
            pending_delete,
            drop_target: self.drop_target.clone(),
            color_teal: gpui::rgb(0x4EC9B0).into(),
            color_yellow: gpui::rgb(0xDCDCAA).into(),
            color_blue: gpui::rgb(0x9CDCFE).into(),
            color_purple: gpui::rgb(0xC586C0).into(),
            color_gray: gpui::rgb(0x808080).into(),
            color_orange: gpui::rgb(0xCE9178).into(),
            color_schema: gpui::rgb(0x569CD6).into(),
            color_green,
        };

        let sidebar_for_root_drop = sidebar_entity.clone();
        let sidebar_for_clear_drop = sidebar_entity.clone();

        div()
            .relative()
            .flex()
            .flex_col()
            .size_full()
            .border_r_1()
            .border_color(theme.border)
            .bg(theme.sidebar)
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .px(Spacing::SM)
                    .h(Heights::TOOLBAR)
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .font_weight(if self.connections_focused {
                                FontWeight::BOLD
                            } else {
                                FontWeight::SEMIBOLD
                            })
                            .text_color(if self.connections_focused {
                                theme.primary
                            } else {
                                theme.muted_foreground
                            })
                            .child("CONNECTIONS"),
                    )
                    .child({
                        let sidebar_for_toggle = sidebar_entity.clone();
                        let hover_bg = theme.secondary;
                        div()
                            .id("add-button")
                            .w(Heights::ICON_LG)
                            .h(Heights::ICON_LG)
                            .flex()
                            .items_center()
                            .justify_center()
                            .rounded(Radii::SM)
                            .text_size(FontSizes::LG)
                            .text_color(theme.muted_foreground)
                            .cursor_pointer()
                            .hover(move |d| d.bg(hover_bg).text_color(theme.foreground))
                            .on_click(move |_, _, cx| {
                                sidebar_for_toggle.update(cx, |this, cx| {
                                    this.toggle_add_menu(cx);
                                });
                            })
                            .child("+")
                    }),
            )
            .when(self.pending_delete_item.is_some(), |el| {
                el.child(
                    div()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .border_b_1()
                        .border_color(theme.border)
                        .bg(gpui::rgb(0x5C1F1F))
                        .flex()
                        .items_center()
                        .justify_center()
                        .text_size(FontSizes::XS)
                        .text_color(theme.foreground)
                        .child("Press x to confirm delete, ESC to cancel"),
                )
            })
            .when(self.editing_id.is_some(), |el| {
                let rename_input = self.rename_input.clone();
                let sidebar_confirm = sidebar_entity.clone();
                let sidebar_cancel = sidebar_entity.clone();

                el.child(
                    div()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .border_b_1()
                        .border_color(theme.border)
                        .bg(theme.sidebar)
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .child(
                            div().flex_1().child(
                                Input::new(&rename_input)
                                    .xsmall()
                                    .appearance(false)
                                    .cleanable(false),
                            ),
                        )
                        .child(
                            div()
                                .id("rename-confirm")
                                .px(Spacing::XS)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(color_green)
                                .hover(|d| d.bg(theme.secondary))
                                .on_click(move |_, _, cx| {
                                    sidebar_confirm.update(cx, |this, cx| {
                                        this.commit_rename(cx);
                                    });
                                })
                                .child("\u{2713}"),
                        )
                        .child(
                            div()
                                .id("rename-cancel")
                                .px(Spacing::XS)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(theme.muted_foreground)
                                .hover(|d| d.bg(theme.secondary))
                                .on_click(move |_, _, cx| {
                                    sidebar_cancel.update(cx, |this, cx| {
                                        this.cancel_rename(cx);
                                    });
                                })
                                .child("\u{2715}"),
                        ),
                )
            })
            .child({
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
                    ))
            })
            .child(self.render_footer(cx))
            // Add menu dropdown
            .when(self.add_menu_open, |el| el.child(self.render_add_menu(cx)))
    }
}
