use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

use super::{AccessTabMode, ActiveTab, ConnectionManagerWindow, EditState, FormFocus};

impl ConnectionManagerWindow {
    pub(super) fn render_access_tab(&mut self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let theme = cx.theme().clone();
        let ring_color = theme.ring;
        let show_focus =
            self.edit_state == EditState::Navigating && self.active_tab == ActiveTab::Access;

        self.access_method_dropdown.update(cx, |dropdown, cx| {
            let focus_color = if show_focus && self.form_focus == FormFocus::AccessMethod {
                Some(ring_color)
            } else {
                None
            };

            dropdown.set_focus_ring(focus_color, cx);
        });

        self.auth_profile_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_focus_ring(None, cx);
        });

        let mut sections = vec![self
            .render_section(
                "Access",
                div().flex().flex_col().gap_2().child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(
                            div()
                                .text_sm()
                                .font_weight(gpui::FontWeight::MEDIUM)
                                .child("Access Method"),
                        )
                        .child(
                            div()
                                .min_w(px(240.0))
                                .child(self.access_method_dropdown.clone()),
                        ),
                ),
                &theme,
            )
            .into_any_element()];

        match self.access_tab_mode {
            AccessTabMode::Direct => {
                sections.push(
                    div()
                        .flex()
                        .flex_col()
                        .gap_3()
                        .child(
                            div()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child("Direct connections use the database fields from the Main tab."),
                        )
                        .child(
                            div()
                                .flex()
                                .flex_col()
                                .gap_1()
                                .child(
                                    div()
                                        .text_sm()
                                        .font_weight(gpui::FontWeight::MEDIUM)
                                        .child("Auth Profile (optional)"),
                                )
                                .child(
                                    div()
                                        .min_w(px(280.0))
                                        .child(self.auth_profile_dropdown.clone()),
                                )
                                .child(
                                    div()
                                        .text_xs()
                                        .text_color(theme.muted_foreground)
                                        .child("Used for resolving Secret/Parameter/Auth value sources in Direct mode."),
                                ),
                        )
                        .into_any_element(),
                );
            }
            AccessTabMode::Ssh => sections.extend(self.render_ssh_tab(cx)),
            AccessTabMode::Proxy => sections.extend(self.render_proxy_tab(cx)),
            AccessTabMode::Ssm => sections.push(self.render_ssm_access_section(cx)),
        }

        sections
    }

    fn render_ssm_access_section(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let ring_color = theme.ring;
        let show_focus =
            self.edit_state == EditState::Navigating && self.active_tab == ActiveTab::Access;

        self.render_section(
            "SSM Port Forwarding",
            div()
                .flex()
                .flex_col()
                .gap_3()
                .child(self.render_ssm_value_field(
                    "Instance ID",
                    &self.input_ssm_instance_id,
                    self.ssm_instance_id_value_source_selector.clone(),
                    true,
                    show_focus && self.form_focus == FormFocus::SsmInstanceId,
                    ring_color,
                    FormFocus::SsmInstanceId,
                    cx,
                ))
                .child(self.render_ssm_value_field(
                    "Region",
                    &self.input_ssm_region,
                    self.ssm_region_value_source_selector.clone(),
                    true,
                    show_focus && self.form_focus == FormFocus::SsmRegion,
                    ring_color,
                    FormFocus::SsmRegion,
                    cx,
                ))
                .child(self.render_ssm_value_field(
                    "Remote Port",
                    &self.input_ssm_remote_port,
                    self.ssm_remote_port_value_source_selector.clone(),
                    false,
                    show_focus && self.form_focus == FormFocus::SsmRemotePort,
                    ring_color,
                    FormFocus::SsmRemotePort,
                    cx,
                ))
                .child(
                    div()
                        .text_xs()
                        .text_color(theme.muted_foreground)
                        .child("DBFlux and the OS auto-assign the local tunnel port. Only Remote Port is configurable here."),
                )
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(
                            div()
                                .text_sm()
                                .font_weight(gpui::FontWeight::MEDIUM)
                                .child("Auth Profile"),
                        )
                        .child(
                            div()
                                .min_w(px(280.0))
                                .child(self.auth_profile_dropdown.clone()),
                        ),
                ),
            &theme,
        )
        .into_any_element()
    }

    #[allow(clippy::too_many_arguments)]
    fn render_ssm_value_field(
        &self,
        label: &str,
        input: &Entity<gpui_component::input::InputState>,
        selector: Entity<crate::ui::components::value_source_selector::ValueSourceSelector>,
        required: bool,
        focused: bool,
        ring_color: gpui::Hsla,
        field: FormFocus,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        div()
            .rounded(px(4.0))
            .border_2()
            .when(focused, |d| d.border_color(ring_color))
            .when(!focused, |d| d.border_color(gpui::transparent_black()))
            .p(px(2.0))
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap_2()
                    .h(px(28.0))
                    .mb_1()
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(gpui::FontWeight::MEDIUM)
                                    .child(label.to_string()),
                            )
                            .when(required, |d| {
                                d.child(div().text_sm().text_color(gpui::rgb(0xEF4444)).child("*"))
                            }),
                    )
                    .child(
                        div()
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, _, cx| {
                                    this.begin_inline_editor_interaction(cx);
                                }),
                            )
                            .child(selector),
                    ),
            )
            .child(
                div()
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, window, cx| {
                            this.enter_edit_mode_for_field(field, window, cx);
                        }),
                    )
                    .child(gpui_component::input::Input::new(input)),
            )
            .into_any_element()
    }
}
