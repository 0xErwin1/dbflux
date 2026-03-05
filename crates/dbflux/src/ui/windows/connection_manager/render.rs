use crate::keymap::ContextId;
use crate::ui::icons::AppIcon;
use crate::ui::windows::ssh_shared::SshAuthSelection;
use dbflux_core::{FormFieldDef, FormFieldKind, FormTab};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Disableable;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::input::{Input, InputState};
use gpui_component::{Icon, IconName};

use super::{ActiveTab, ConnectionManagerWindow, EditState, FormFocus, TestStatus, View};

impl ConnectionManagerWindow {
    pub(super) fn render_password_field(
        &self,
        password_focused: bool,
        checkbox_focused: bool,
        show_save_checkbox: bool,
        save_password: bool,
        ring_color: Hsla,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();

        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child("Password"),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(
                        div()
                            .flex_1()
                            .rounded(px(4.0))
                            .border_2()
                            .when(password_focused, |d| d.border_color(ring_color))
                            .when(!password_focused, |d| {
                                d.border_color(gpui::transparent_black())
                            })
                            .p(px(2.0))
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, window, cx| {
                                    this.enter_edit_mode_for_field(FormFocus::Password, window, cx);
                                }),
                            )
                            .child(Input::new(&self.input_password)),
                    )
                    .child(
                        Self::render_password_toggle(self.show_password, "toggle-password", &theme)
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.show_password = !this.show_password;
                                cx.notify();
                            })),
                    )
                    .when(show_save_checkbox, |d| {
                        d.child(
                            div()
                                .flex()
                                .items_center()
                                .gap_2()
                                .rounded(px(4.0))
                                .border_2()
                                .when(checkbox_focused, |dd| dd.border_color(ring_color))
                                .when(!checkbox_focused, |dd| {
                                    dd.border_color(gpui::transparent_black())
                                })
                                .p(px(2.0))
                                .child(
                                    Checkbox::new("save-password")
                                        .checked(save_password)
                                        .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                            this.form_save_password = *checked;
                                            cx.notify();
                                        })),
                                )
                                .child(div().text_sm().child("Save")),
                        )
                    }),
            )
            .into_any_element()
    }

    pub(super) fn render_readonly_row(
        &self,
        label: &str,
        value: &str,
        theme: &gpui_component::Theme,
    ) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .gap_3()
            .child(
                div()
                    .w(px(100.0))
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(theme.muted_foreground)
                    .child(label.to_string()),
            )
            .child(
                div()
                    .text_sm()
                    .text_color(theme.foreground)
                    .child(value.to_string()),
            )
    }

    pub(super) fn render_section(
        &self,
        title: &str,
        content: impl IntoElement,
        theme: &gpui_component::Theme,
    ) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .text_xs()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(theme.muted_foreground)
                    .child(title.to_uppercase()),
            )
            .child(content)
    }

    pub(super) fn render_form(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let Some(driver) = &self.selected_driver else {
            return div().into_any_element();
        };

        let driver_name = driver.display_name().to_string();
        let supports_ssh = self.supports_ssh();
        let validation_errors = self.validation_errors.clone();
        let test_status = self.test_status;
        let test_error = self.test_error.clone();
        let is_editing = self.editing_profile_id.is_some();
        let title = if is_editing {
            format!("Edit {} Connection", driver_name)
        } else {
            format!("New {} Connection", driver_name)
        };

        let show_focus = self.edit_state == EditState::Navigating;
        let focus = self.form_focus;
        let test_focused = show_focus && focus == FormFocus::TestConnection;
        let save_focused = show_focus && focus == FormFocus::Save;

        let supports_proxy = self.supports_proxy();
        let tab_bar = self
            .render_tab_bar(supports_ssh, supports_proxy, cx)
            .into_any_element();

        let tab_content: Vec<AnyElement> = match self.active_tab {
            ActiveTab::Main => self.render_main_tab(cx),
            ActiveTab::Settings => self.render_settings_tab(cx),
            ActiveTab::Ssh if supports_ssh => self.render_ssh_tab(cx),
            ActiveTab::Ssh => self.render_main_tab(cx),
            ActiveTab::Proxy if supports_proxy => self.render_proxy_tab(cx),
            ActiveTab::Proxy => self.render_main_tab(cx),
        };

        let theme = cx.theme();
        let border_color = theme.border;
        let ring_color = theme.ring;

        div()
            .flex()
            .flex_col()
            .size_full()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .p_3()
                    .border_b_1()
                    .border_color(border_color)
                    .when(!is_editing, |d| {
                        d.child(Button::new("back").ghost().label("<").small().on_click(
                            cx.listener(|this, _, window, cx| {
                                this.back_to_driver_select(window, cx);
                            }),
                        ))
                    })
                    .child({
                        let brand_icon = self
                            .selected_driver
                            .as_ref()
                            .map(|driver| AppIcon::from_icon(driver.metadata().icon));

                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .when_some(brand_icon, |el, icon| {
                                el.child(
                                    svg()
                                        .path(icon.path())
                                        .size_6()
                                        .text_color(theme.foreground),
                                )
                            })
                            .child(
                                div()
                                    .text_lg()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .child(title),
                            )
                    })
                    .child(div().flex_1())
                    .child(self.form_field_input_inline(
                        "Name",
                        &self.input_name,
                        show_focus && focus == FormFocus::Name,
                        ring_color,
                        FormFocus::Name,
                        cx,
                    )),
            )
            .child(tab_bar)
            .child(
                div()
                    .id("form-scroll-content")
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_h_0()
                    .overflow_scroll()
                    .track_scroll(&self.form_scroll_handle)
                    .gap_4()
                    .p_4()
                    .when(!validation_errors.is_empty(), |d| {
                        d.child(div().child(
                            div().p_2().rounded(px(4.0)).bg(gpui::rgb(0x7F1D1D)).child(
                                div().flex().flex_col().gap_1().children(
                                    validation_errors.iter().map(|err| {
                                        div()
                                            .text_sm()
                                            .text_color(gpui::rgb(0xFCA5A5))
                                            .child(err.clone())
                                    }),
                                ),
                            ),
                        ))
                    })
                    .children(tab_content),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .p_3()
                    .border_t_1()
                    .border_color(border_color)
                    .when(test_status != TestStatus::None, |d| {
                        let (bg, text_color, message) = match test_status {
                            TestStatus::Testing => (
                                gpui::rgb(0x1E3A5F),
                                gpui::rgb(0x93C5FD),
                                "Testing connection...".to_string(),
                            ),
                            TestStatus::Success => (
                                gpui::rgb(0x14532D),
                                gpui::rgb(0x86EFAC),
                                "Connection successful!".to_string(),
                            ),
                            TestStatus::Failed => (
                                gpui::rgb(0x7F1D1D),
                                gpui::rgb(0xFCA5A5),
                                test_error.unwrap_or_else(|| "Connection failed".to_string()),
                            ),
                            TestStatus::None => unreachable!(),
                        };

                        d.child(
                            div()
                                .p_2()
                                .rounded(px(4.0))
                                .bg(bg)
                                .child(div().text_sm().text_color(text_color).child(message)),
                        )
                    })
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_end()
                            .gap_2()
                            .child(
                                div()
                                    .rounded(px(4.0))
                                    .border_2()
                                    .when(test_focused, |d| d.border_color(ring_color))
                                    .when(!test_focused, |d| {
                                        d.border_color(gpui::transparent_black())
                                    })
                                    .child(
                                        Button::new("test-connection")
                                            .ghost()
                                            .icon(Icon::new(IconName::ExternalLink))
                                            .label("Test Connection")
                                            .small()
                                            .disabled(test_status == TestStatus::Testing)
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.test_connection(window, cx);
                                            })),
                                    ),
                            )
                            .child(
                                div()
                                    .rounded(px(4.0))
                                    .border_2()
                                    .when(save_focused, |d| d.border_color(ring_color))
                                    .when(!save_focused, |d| {
                                        d.border_color(gpui::transparent_black())
                                    })
                                    .child(
                                        Button::new("save-connection")
                                            .primary()
                                            .icon(Icon::new(IconName::Check))
                                            .label("Save")
                                            .small()
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.save_profile(window, cx);
                                            })),
                                    ),
                            ),
                    ),
            )
            .into_any_element()
    }

    fn render_form_field(
        &self,
        field_def: &FormFieldDef,
        is_ssh_tab: bool,
        show_focus: bool,
        ring_color: Hsla,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let field_focus = Self::field_id_to_focus(&field_def.id, is_ssh_tab);
        let focused = show_focus && field_focus == Some(self.form_focus);

        match &field_def.kind {
            FormFieldKind::Text
            | FormFieldKind::Password
            | FormFieldKind::Number
            | FormFieldKind::FilePath => {
                let Some(input_state) = self.input_state_for_field(&field_def.id) else {
                    return div().into_any_element();
                };

                let field_enabled = self.is_field_enabled(field_def);

                div()
                    .rounded(px(4.0))
                    .border_2()
                    .when(focused, |d| d.border_color(ring_color))
                    .when(!focused, |d| d.border_color(gpui::transparent_black()))
                    .p(px(2.0))
                    .when(!field_enabled, |d| d.opacity(0.5))
                    .when(field_enabled, |d| {
                        d.on_mouse_down(
                            MouseButton::Left,
                            cx.listener(move |this, _, window, cx| {
                                if let Some(field) = field_focus {
                                    this.enter_edit_mode_for_field(field, window, cx);
                                }
                            }),
                        )
                    })
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .mb_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child(field_def.label.clone()),
                            )
                            .when(field_def.required && field_enabled, |d| {
                                d.child(div().text_sm().text_color(gpui::rgb(0xEF4444)).child("*"))
                            }),
                    )
                    .child(Input::new(input_state).disabled(!field_enabled))
                    .into_any_element()
            }

            FormFieldKind::Checkbox => {
                let field_id = field_def.id.clone();
                let is_checked = if field_id == "ssh_enabled" {
                    self.ssh_enabled
                } else {
                    self.checkbox_states
                        .get(&field_id)
                        .copied()
                        .unwrap_or(false)
                };

                let checkbox_id = gpui::SharedString::from(field_id.clone());
                div()
                    .rounded(px(4.0))
                    .border_2()
                    .when(focused, |d| d.border_color(ring_color))
                    .when(!focused, |d| d.border_color(gpui::transparent_black()))
                    .p(px(2.0))
                    .child(
                        Checkbox::new(checkbox_id)
                            .checked(is_checked)
                            .label(field_def.label.as_str())
                            .on_click(cx.listener(move |this, checked: &bool, window, cx| {
                                if field_id == "ssh_enabled" {
                                    this.ssh_enabled = *checked;
                                } else {
                                    this.checkbox_states.insert(field_id.clone(), *checked);
                                }
                                window.focus(&this.focus_handle);
                                cx.notify();
                            })),
                    )
                    .into_any_element()
            }

            FormFieldKind::Select { options } => {
                if field_def.id == "ssh_auth_method" {
                    let selected_index = match self.ssh_auth_method {
                        SshAuthSelection::PrivateKey => 0,
                        SshAuthSelection::Password => 1,
                    };

                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(
                            div()
                                .text_sm()
                                .font_weight(FontWeight::MEDIUM)
                                .child(field_def.label.clone()),
                        )
                        .child(
                            div()
                                .flex()
                                .gap_2()
                                .children(options.iter().enumerate().map(|(idx, opt)| {
                                    let is_selected = idx == selected_index;
                                    div()
                                        .flex()
                                        .items_center()
                                        .gap_1()
                                        .cursor_pointer()
                                        .on_mouse_down(
                                            MouseButton::Left,
                                            cx.listener(move |this, _, window, cx| {
                                                this.ssh_auth_method = if idx == 0 {
                                                    SshAuthSelection::PrivateKey
                                                } else {
                                                    SshAuthSelection::Password
                                                };
                                                window.focus(&this.focus_handle);
                                                cx.notify();
                                            }),
                                        )
                                        .child(
                                            div()
                                                .w(px(16.0))
                                                .h(px(16.0))
                                                .rounded(px(3.0))
                                                .border_2()
                                                .border_color(cx.theme().muted_foreground)
                                                .flex()
                                                .items_center()
                                                .justify_center()
                                                .when(is_selected, |d| {
                                                    d.bg(cx.theme().ring)
                                                        .border_color(cx.theme().ring)
                                                })
                                                .when(is_selected, |d| {
                                                    d.child(
                                                        div()
                                                            .w(px(8.0))
                                                            .h(px(8.0))
                                                            .rounded(px(1.0))
                                                            .bg(gpui::white()),
                                                    )
                                                }),
                                        )
                                        .child(div().text_sm().child(opt.label.clone()))
                                        .into_any_element()
                                })),
                        )
                        .into_any_element()
                } else {
                    div().into_any_element()
                }
            }
        }
    }

    pub(super) fn render_form_tab(
        &mut self,
        tab: &FormTab,
        is_ssh_tab: bool,
        show_focus: bool,
        ring_color: Hsla,
        cx: &mut Context<Self>,
    ) -> Vec<AnyElement> {
        let theme = cx.theme().clone();
        let mut sections: Vec<AnyElement> = Vec::new();

        for section in &tab.sections {
            let fields: Vec<&FormFieldDef> = section
                .fields
                .iter()
                .filter(|field| field.id != "password" || is_ssh_tab)
                .collect();

            if fields.is_empty() {
                continue;
            }

            let mut field_elements: Vec<AnyElement> = Vec::new();
            let mut i = 0;
            while i < fields.len() {
                let field = fields[i];

                if field.id == "host" && i + 1 < fields.len() && fields[i + 1].id == "port" {
                    let port_field = fields[i + 1];
                    let host_element = self
                        .render_form_field(field, is_ssh_tab, show_focus, ring_color, cx)
                        .into_any_element();
                    let port_element = self
                        .render_form_field(port_field, is_ssh_tab, show_focus, ring_color, cx)
                        .into_any_element();

                    field_elements.push(
                        div()
                            .flex()
                            .gap_2()
                            .child(div().flex_1().child(host_element))
                            .child(div().w(px(100.0)).child(port_element))
                            .into_any_element(),
                    );
                    i += 2;
                } else {
                    field_elements.push(
                        self.render_form_field(field, is_ssh_tab, show_focus, ring_color, cx)
                            .into_any_element(),
                    );
                    i += 1;
                }
            }

            sections.push(
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(
                        div()
                            .text_xs()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(theme.muted_foreground)
                            .child(section.title.to_uppercase()),
                    )
                    .children(field_elements)
                    .into_any_element(),
            );
        }

        sections
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn form_field_input(
        &self,
        label: &str,
        input: &Entity<InputState>,
        required: bool,
        focused: bool,
        ring_color: Hsla,
        field: FormFocus,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .rounded(px(4.0))
            .border_2()
            .when(focused, |d| d.border_color(ring_color))
            .when(!focused, |d| d.border_color(gpui::transparent_black()))
            .p(px(2.0))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, window, cx| {
                    this.enter_edit_mode_for_field(field, window, cx);
                }),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .mb_1()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .child(label.to_string()),
                    )
                    .when(required, |d| {
                        d.child(div().text_sm().text_color(gpui::rgb(0xEF4444)).child("*"))
                    }),
            )
            .child(Input::new(input))
    }

    fn form_field_input_inline(
        &self,
        label: &str,
        input: &Entity<InputState>,
        focused: bool,
        ring_color: Hsla,
        field: FormFocus,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child(format!("{}:", label)),
            )
            .child(
                div()
                    .w(px(200.0))
                    .rounded(px(4.0))
                    .border_2()
                    .when(focused, |d| d.border_color(ring_color))
                    .when(!focused, |d| d.border_color(gpui::transparent_black()))
                    .p(px(2.0))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, window, cx| {
                            this.enter_edit_mode_for_field(field, window, cx);
                        }),
                    )
                    .child(Input::new(input)),
            )
    }

    pub(super) fn render_password_toggle(
        show: bool,
        toggle_id: &'static str,
        theme: &gpui_component::theme::Theme,
    ) -> Stateful<Div> {
        let secondary = theme.secondary;
        let muted_foreground = theme.muted_foreground;

        let icon_path = if show {
            AppIcon::EyeOff.path()
        } else {
            AppIcon::Eye.path()
        };

        div()
            .id(toggle_id)
            .w(px(32.0))
            .h(px(32.0))
            .flex()
            .items_center()
            .justify_center()
            .rounded(px(4.0))
            .cursor_pointer()
            .hover(move |d| d.bg(secondary))
            .child(svg().path(icon_path).size_4().text_color(muted_foreground))
    }
}

impl Render for ConnectionManagerWindow {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(path) = self.pending_ssh_key_path.take() {
            self.input_ssh_key_path.update(cx, |state, cx| {
                state.set_value(path, window, cx);
            });
        }

        if let Some(proxy_id) = self.pending_proxy_selection.take() {
            let proxy = self
                .app_state
                .read(cx)
                .proxies()
                .iter()
                .find(|p| p.id == proxy_id)
                .cloned();
            if let Some(proxy) = proxy {
                self.apply_proxy(&proxy, cx);
            }
        }

        if let Some(tunnel_id) = self.pending_ssh_tunnel_selection.take() {
            let tunnel = self
                .app_state
                .read(cx)
                .ssh_tunnels()
                .iter()
                .find(|t| t.id == tunnel_id)
                .cloned();
            if let Some(tunnel) = tunnel {
                let secret = self.app_state.read(cx).get_ssh_tunnel_secret(&tunnel);
                self.apply_ssh_tunnel(&tunnel, secret, window, cx);
            }
        }

        let show_password = self.show_password;
        let show_ssh_passphrase = self.show_ssh_passphrase;
        let show_ssh_password = self.show_ssh_password;

        self.input_password.update(cx, |state, cx| {
            state.set_masked(!show_password, window, cx);
        });
        self.input_ssh_key_passphrase.update(cx, |state, cx| {
            state.set_masked(!show_ssh_passphrase, window, cx);
        });
        self.input_ssh_password.update(cx, |state, cx| {
            state.set_masked(!show_ssh_password, window, cx);
        });

        let theme = cx.theme();

        div()
            .id("connection-manager")
            .key_context(ContextId::ConnectionManager.as_gpui_context())
            .track_focus(&self.focus_handle)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, window, cx| {
                    if this.edit_state == EditState::Navigating {
                        window.focus(&this.focus_handle);
                        cx.notify();
                    }
                }),
            )
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                if this.handle_key_event(event, window, cx) {
                    cx.stop_propagation();
                }
            }))
            .size_full()
            .bg(theme.background)
            .child(match self.view {
                View::DriverSelect => self.render_driver_select(window, cx).into_any_element(),
                View::EditForm => self.render_form(window, cx).into_any_element(),
            })
    }
}

impl Focusable for ConnectionManagerWindow {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}
