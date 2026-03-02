use super::*;
use gpui_component::Sizable;
use gpui_component::input::Input;

impl ConnectionManagerWindow {
    pub(super) fn render_hooks_rows(&self, muted: Hsla) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .text_xs()
                    .text_color(muted)
                    .child("Select reusable hooks configured in Settings -> Hooks"),
            )
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
                    .child(
                        div()
                            .w(px(160.0))
                            .text_xs()
                            .text_color(muted)
                            .child("Extra pre-connect"),
                    )
                    .child(
                        div()
                            .w(px(240.0))
                            .child(Input::new(&self.conn_pre_hook_extra_input).small()),
                    ),
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
                    .child(
                        div()
                            .w(px(160.0))
                            .text_xs()
                            .text_color(muted)
                            .child("Extra post-connect"),
                    )
                    .child(
                        div()
                            .w(px(240.0))
                            .child(Input::new(&self.conn_post_hook_extra_input).small()),
                    ),
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
                    .child(
                        div()
                            .w(px(160.0))
                            .text_xs()
                            .text_color(muted)
                            .child("Extra pre-disconnect"),
                    )
                    .child(
                        div()
                            .w(px(240.0))
                            .child(Input::new(&self.conn_pre_disconnect_hook_extra_input).small()),
                    ),
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
                    .child(
                        div()
                            .w(px(160.0))
                            .text_xs()
                            .text_color(muted)
                            .child("Extra post-disconnect"),
                    )
                    .child(
                        div()
                            .w(px(240.0))
                            .child(Input::new(&self.conn_post_disconnect_hook_extra_input).small()),
                    ),
            )
    }
}
