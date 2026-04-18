use super::*;
use dbflux_components::primitives::Text;
use gpui::prelude::FluentBuilder;
use gpui_component::ActiveTheme;

impl ConnectionManagerWindow {
    fn selected_hook_ids(&self, cx: &Context<Self>) -> Vec<String> {
        let pre_connect = Self::merge_hook_ids(
            self.conn_pre_hook_dropdown
                .read(cx)
                .selected_value()
                .map(|value| value.to_string()),
            self.conn_pre_hook_extra_input.read(cx).value().to_string(),
        );

        let post_connect = Self::merge_hook_ids(
            self.conn_post_hook_dropdown
                .read(cx)
                .selected_value()
                .map(|value| value.to_string()),
            self.conn_post_hook_extra_input.read(cx).value().to_string(),
        );

        let pre_disconnect = Self::merge_hook_ids(
            self.conn_pre_disconnect_hook_dropdown
                .read(cx)
                .selected_value()
                .map(|value| value.to_string()),
            self.conn_pre_disconnect_hook_extra_input
                .read(cx)
                .value()
                .to_string(),
        );

        let post_disconnect = Self::merge_hook_ids(
            self.conn_post_disconnect_hook_dropdown
                .read(cx)
                .selected_value()
                .map(|value| value.to_string()),
            self.conn_post_disconnect_hook_extra_input
                .read(cx)
                .value()
                .to_string(),
        );

        let mut selected = Vec::new();

        for hook_id in pre_connect
            .into_iter()
            .chain(post_connect)
            .chain(pre_disconnect)
            .chain(post_disconnect)
        {
            if !selected.iter().any(|existing| existing == &hook_id) {
                selected.push(hook_id);
            }
        }

        selected
    }

    fn has_process_run_hook_selected(&self, cx: &Context<Self>) -> bool {
        let selected = self.selected_hook_ids(cx);
        if selected.is_empty() {
            return false;
        }

        let hook_definitions = self.app_state.read(cx).hook_definitions().clone();

        selected.into_iter().any(|hook_id| {
            hook_definitions.get(&hook_id).is_some_and(|hook| {
                matches!(
                    &hook.kind,
                    dbflux_core::HookKind::Lua {
                        capabilities: dbflux_core::LuaCapabilities {
                            process_run: true,
                            ..
                        },
                        ..
                    }
                )
            })
        })
    }

    pub(super) fn render_hooks_rows(&self, _muted: Hsla, cx: &Context<Self>) -> Div {
        let show_process_run_warning = self.has_process_run_hook_selected(cx);

        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(Text::caption("Select reusable hooks configured in Settings -> Hooks"))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(div().w(px(160.0)).text_sm().child("Pre-connect hook"))
                    .child(
                        div()
                            .w(px(240.0))
                            .child(self.conn_pre_hook_dropdown.clone()),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(div().w(px(160.0)).child(Text::caption("Extra pre-connect"))),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(div().w(px(160.0)).text_sm().child("Post-connect hook"))
                    .child(
                        div()
                            .w(px(240.0))
                            .child(self.conn_post_hook_dropdown.clone()),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(div().w(px(160.0)).child(Text::caption("Extra post-connect"))),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(div().w(px(160.0)).text_sm().child("Pre-disconnect hook"))
                    .child(
                        div()
                            .w(px(240.0))
                            .child(self.conn_pre_disconnect_hook_dropdown.clone()),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(div().w(px(160.0)).child(Text::caption("Extra pre-disconnect"))),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(div().w(px(160.0)).text_sm().child("Post-disconnect hook"))
                    .child(
                        div()
                            .w(px(240.0))
                            .child(self.conn_post_disconnect_hook_dropdown.clone()),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(div().w(px(160.0)).child(Text::caption("Extra post-disconnect"))),
            )
            .when(show_process_run_warning, |this| {
                let theme = cx.theme();
                this.child(
                    div()
                        .rounded(px(4.0))
                        .border_1()
                        .border_color(theme.warning.opacity(0.3))
                        .bg(theme.warning.opacity(0.1))
                        .p_2()
                        .child(
                            Text::caption("Selected hook enables Lua process.run and can execute external programs with your user permissions")
                                .text_color(theme.warning),
                        ),
                )
            })
    }
}
