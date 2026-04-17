use crate::keymap::ContextId;
use crate::platform;
use crate::ui::icons::AppIcon;
use crate::ui::windows::ssh_shared::SshAuthSelection;
use dbflux_components::controls::Button;
use dbflux_components::primitives::Text;
use dbflux_core::{FormFieldDef, FormFieldKind, FormTab};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::checkbox::Checkbox;
use gpui_component::input::{Input, InputState};
use gpui_component::{Icon, IconName};

use super::{ActiveTab, ConnectionManagerWindow, EditState, FormFocus, TestStatus, View};

impl ConnectionManagerWindow {
    pub(super) fn render_password_field(
        &self,
        show_focus: bool,
        show_save_checkbox: bool,
        save_password: bool,
        ring_color: Hsla,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();
        let focus = self.form_focus;
        let password_source_is_literal =
            self.password_value_source_selector.read(cx).is_literal(cx);

        let selector_focused = show_focus && focus == FormFocus::PasswordValueSource;
        let password_focused = show_focus && focus == FormFocus::Password;
        let toggle_focused = show_focus && focus == FormFocus::PasswordToggle;
        let checkbox_focused = show_focus && focus == FormFocus::PasswordSave;

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
                            .w(px(170.0))
                            .rounded(px(4.0))
                            .border_2()
                            .when(selector_focused, |d| d.border_color(ring_color))
                            .when(!selector_focused, |d| {
                                d.border_color(gpui::transparent_black())
                            })
                            .p(px(2.0))
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, window, cx| {
                                    this.enter_edit_mode_for_field(
                                        FormFocus::PasswordValueSource,
                                        window,
                                        cx,
                                    );
                                }),
                            )
                            .child(self.password_value_source_selector.clone()),
                    )
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
                    .when(password_source_is_literal, |d| {
                        d.child(
                            div()
                                .rounded(px(4.0))
                                .border_2()
                                .when(toggle_focused, |dd| dd.border_color(ring_color))
                                .when(!toggle_focused, |dd| {
                                    dd.border_color(gpui::transparent_black())
                                })
                                .child(
                                    Self::render_password_toggle(
                                        self.show_password,
                                        "toggle-password",
                                        &theme,
                                    )
                                    .on_click(cx.listener(
                                        |this, _, _, cx| {
                                            this.show_password = !this.show_password;
                                            cx.notify();
                                        },
                                    )),
                                ),
                        )
                    })
                    .when(show_save_checkbox && password_source_is_literal, |d| {
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
        _theme: &gpui_component::Theme,
    ) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .gap_3()
            .child(
                div()
                    .w(px(100.0))
                    .child(Text::caption(label.to_string()).font_weight(FontWeight::MEDIUM)),
            )
            .child(Text::body(value.to_string()))
    }

    pub(super) fn render_section(
        &self,
        title: &str,
        content: impl IntoElement,
        _theme: &gpui_component::Theme,
    ) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(Text::caption(title.to_uppercase()))
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

        let tab_bar = self.render_tab_bar(cx).into_any_element();

        let tab_content: Vec<AnyElement> = match self.active_tab {
            ActiveTab::Main => self.render_main_tab(cx),
            ActiveTab::Access if !self.uses_file_form() => self.render_access_tab(cx),
            ActiveTab::Access => self.render_main_tab(cx),
            ActiveTab::Settings => self.render_settings_tab(cx),
            ActiveTab::Mcp => self.render_mcp_tab(cx),
        };

        let theme = cx.theme();
        let border_color = theme.border;
        let ring_color = theme.ring;
        let danger_color = theme.danger;
        let danger_bg = theme.danger.opacity(0.15);
        let info_color = theme.info;
        let info_bg = theme.info.opacity(0.15);
        let success_color = theme.success;
        let success_bg = theme.success.opacity(0.15);
        let muted_fg = theme.muted_foreground;
        let muted_bg = theme.muted_foreground.opacity(0.15);

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
                        d.child(
                            Button::new("back", "<")
                                .ghost()
                                .small()
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.back_to_driver_select(window, cx);
                                })),
                        )
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
                            div().p_2().rounded(px(4.0)).bg(danger_bg).child(
                                div().flex().flex_col().gap_1().children(
                                    validation_errors.iter().map(|err| {
                                        Text::body(err.clone()).text_color(danger_color)
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
                            TestStatus::Testing => {
                                (info_bg, info_color, "Testing connection...".to_string())
                            }
                            TestStatus::Success => (
                                success_bg,
                                success_color,
                                "Connection successful!".to_string(),
                            ),
                            TestStatus::Failed => (
                                danger_bg,
                                danger_color,
                                test_error.unwrap_or_else(|| "Connection failed".to_string()),
                            ),
                            TestStatus::None => {
                                (muted_bg, muted_fg, "No test status available".to_string())
                            }
                        };

                        d.child(
                            div()
                                .p_2()
                                .rounded(px(4.0))
                                .bg(bg)
                                .child(Text::body(message).text_color(text_color)),
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
                                        Button::new("test-connection", "Test Connection")
                                            .ghost()
                                            .icon(Icon::new(IconName::ExternalLink))
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
                                        Button::new("save-connection", "Save")
                                            .primary()
                                            .icon(Icon::new(IconName::Check))
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
        let danger_color = cx.theme().danger;
        if !is_ssh_tab && field_def.id == "profile" && self.selected_driver_id() == Some("dynamodb")
        {
            let field_enabled = self.is_field_enabled(field_def);

            return div()
                .flex()
                .flex_col()
                .gap_1()
                .when(!field_enabled, |d| d.opacity(0.5))
                .child(
                    div()
                        .text_sm()
                        .font_weight(FontWeight::MEDIUM)
                        .child(field_def.label.clone()),
                )
                .child(
                    div()
                        .when(field_enabled, |d| {
                            d.on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, _, cx| {
                                    this.begin_inline_editor_interaction(cx);
                                    this.auth_profile_dropdown.update(cx, |dropdown, cx| {
                                        dropdown.open(cx);
                                    });
                                }),
                            )
                        })
                        .child(self.auth_profile_dropdown.clone()),
                )
                .into_any_element();
        }

        let field_focus = Self::field_id_to_focus(&field_def.id, is_ssh_tab);
        let focused = show_focus && field_focus == Some(self.form_focus);

        match &field_def.kind {
            FormFieldKind::Text | FormFieldKind::Password | FormFieldKind::Number => {
                let Some(input_state) = self.input_state_for_field(&field_def.id) else {
                    return div().into_any_element();
                };

                let field_enabled = self.is_field_enabled(field_def);

                if !is_ssh_tab && (field_def.id == "database" || field_def.id == "user") {
                    let (selector, selector_focus, input_focus) = if field_def.id == "database" {
                        (
                            self.database_value_source_selector.clone(),
                            FormFocus::DatabaseValueSource,
                            FormFocus::Database,
                        )
                    } else {
                        (
                            self.user_value_source_selector.clone(),
                            FormFocus::UserValueSource,
                            FormFocus::User,
                        )
                    };

                    let selector_focused = show_focus && self.form_focus == selector_focus;
                    let input_focused = show_focus && self.form_focus == input_focus;

                    return div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .when(!field_enabled, |d| d.opacity(0.5))
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_1()
                                .child(
                                    div()
                                        .text_sm()
                                        .font_weight(FontWeight::MEDIUM)
                                        .child(field_def.label.clone()),
                                )
                                .when(field_def.required && field_enabled, |d| {
                                    d.child(div().text_sm().text_color(danger_color).child("*"))
                                }),
                        )
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_2()
                                .child(
                                    div()
                                        .w(px(170.0))
                                        .rounded(px(4.0))
                                        .border_2()
                                        .when(selector_focused, |d| d.border_color(ring_color))
                                        .when(!selector_focused, |d| {
                                            d.border_color(gpui::transparent_black())
                                        })
                                        .p(px(2.0))
                                        .when(field_enabled, |d| {
                                            d.on_mouse_down(
                                                MouseButton::Left,
                                                cx.listener(move |this, _, window, cx| {
                                                    this.enter_edit_mode_for_field(
                                                        selector_focus,
                                                        window,
                                                        cx,
                                                    );
                                                }),
                                            )
                                        })
                                        .child(selector),
                                )
                                .child(
                                    div()
                                        .flex_1()
                                        .rounded(px(4.0))
                                        .border_2()
                                        .when(input_focused, |d| d.border_color(ring_color))
                                        .when(!input_focused, |d| {
                                            d.border_color(gpui::transparent_black())
                                        })
                                        .p(px(2.0))
                                        .when(field_enabled, |d| {
                                            d.on_mouse_down(
                                                MouseButton::Left,
                                                cx.listener(move |this, _, window, cx| {
                                                    this.enter_edit_mode_for_field(
                                                        input_focus,
                                                        window,
                                                        cx,
                                                    );
                                                }),
                                            )
                                        })
                                        .child(Input::new(input_state).disabled(!field_enabled)),
                                ),
                        )
                        .into_any_element();
                }

                let fallback_input_focus = input_state.clone();

                div()
                    .rounded(px(4.0))
                    .border_2()
                    .when(focused, |d| d.border_color(ring_color))
                    .when(!focused, |d| d.border_color(gpui::transparent_black()))
                    .p(px(2.0))
                    .when(!field_enabled, |d| d.opacity(0.5))
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
                                            .font_weight(FontWeight::MEDIUM)
                                            .child(field_def.label.clone()),
                                    )
                                    .when(field_def.required && field_enabled, |d| {
                                        d.child(div().text_sm().text_color(danger_color).child("*"))
                                    }),
                            ),
                    )
                    .child(
                        div()
                            .when_some(
                                field_focus.and_then(|field| field_enabled.then_some(field)),
                                |d, field| {
                                    d.on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(move |this, _, window, cx| {
                                            this.enter_edit_mode_for_field(field, window, cx);
                                        }),
                                    )
                                },
                            )
                            .when(field_enabled && field_focus.is_none(), |d| {
                                let fallback_input_focus = fallback_input_focus.clone();
                                d.on_mouse_down(
                                    MouseButton::Left,
                                    cx.listener(move |this, _, window, cx| {
                                        this.begin_inline_editor_interaction(cx);
                                        fallback_input_focus.update(cx, |state, cx| {
                                            state.focus(window, cx);
                                        });
                                    }),
                                )
                            })
                            .child(Input::new(input_state).disabled(!field_enabled)),
                    )
                    .into_any_element()
            }

            FormFieldKind::FilePath => {
                let Some(input_state) = self.input_state_for_field(&field_def.id) else {
                    return div().into_any_element();
                };

                let browse_focused =
                    show_focus && Some(FormFocus::FileBrowse) == Some(self.form_focus);

                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child(field_def.label.clone()),
                            )
                            .when(field_def.required, |d| {
                                d.child(div().text_sm().text_color(danger_color).child("*"))
                            }),
                    )
                    .child(
                        div()
                            .flex()
                            .gap_2()
                            .child(
                                div()
                                    .flex_1()
                                    .rounded(px(4.0))
                                    .border_2()
                                    .when(focused, |d| d.border_color(ring_color))
                                    .when(!focused, |d| d.border_color(gpui::transparent_black()))
                                    .p(px(2.0))
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(move |this, _, window, cx| {
                                            if let Some(field) = field_focus {
                                                this.enter_edit_mode_for_field(field, window, cx);
                                            }
                                        }),
                                    )
                                    .child(Input::new(input_state)),
                            )
                            .child(
                                div()
                                    .rounded(px(4.0))
                                    .border_2()
                                    .when(browse_focused, |d| d.border_color(ring_color))
                                    .when(!browse_focused, |d| {
                                        d.border_color(gpui::transparent_black())
                                    })
                                    .child(
                                        Button::new("browse-file-path", "Browse")
                                            .small()
                                            .ghost()
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.browse_file_path(window, cx);
                                            })),
                                    ),
                            ),
                    )
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

                if field.id == "uri"
                    && i + 2 < fields.len()
                    && fields[i + 1].id == "host"
                    && fields[i + 2].id == "port"
                {
                    i += 1;
                    continue;
                }

                if field.id == "host" && i + 1 < fields.len() && fields[i + 1].id == "port" {
                    let port_field = fields[i + 1];

                    let uri_field = if i > 0 && fields[i - 1].id == "uri" {
                        Some(fields[i - 1])
                    } else {
                        None
                    };

                    field_elements.push(self.render_host_port_row(
                        field, port_field, uri_field, show_focus, ring_color, cx,
                    ));
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
                    .child(Text::caption(section.title.to_uppercase()))
                    .children(field_elements)
                    .into_any_element(),
            );
        }

        sections
    }

    fn render_host_port_row(
        &self,
        host_field: &FormFieldDef,
        port_field: &FormFieldDef,
        uri_field: Option<&FormFieldDef>,
        show_focus: bool,
        ring_color: Hsla,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let danger_color = cx.theme().danger;
        let Some(host_input) = self.input_state_for_field("host") else {
            return div().into_any_element();
        };

        let Some(port_input) = self.input_state_for_field("port") else {
            return div().into_any_element();
        };

        let uri_mode_active = self
            .checkbox_states
            .get("use_uri")
            .copied()
            .unwrap_or(false);

        let using_uri = uri_mode_active && uri_field.is_some();

        let (primary_label, primary_required, primary_enabled, primary_input) = if using_uri {
            let Some(uri_field) = uri_field else {
                return div().into_any_element();
            };

            let Some(uri_input) = self.input_state_for_field("uri") else {
                return div().into_any_element();
            };

            (
                uri_field.label.clone(),
                uri_field.required,
                self.is_field_enabled(uri_field),
                uri_input,
            )
        } else {
            (
                host_field.label.clone(),
                host_field.required,
                self.is_field_enabled(host_field),
                host_input,
            )
        };

        let port_enabled = !using_uri && self.is_field_enabled(port_field);

        let selector_focused = show_focus && self.form_focus == FormFocus::HostValueSource;
        let input_focused = show_focus && self.form_focus == FormFocus::Host;
        let port_focused = show_focus && self.form_focus == FormFocus::Port;

        div()
            .flex()
            .flex_col()
            .gap_1()
            .when(!primary_enabled, |d| d.opacity(0.5))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .child(primary_label),
                    )
                    .when(primary_required && primary_enabled, |d| {
                        d.child(div().text_sm().text_color(danger_color).child("*"))
                    }),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(
                        div()
                            .w(px(170.0))
                            .rounded(px(4.0))
                            .border_2()
                            .when(selector_focused, |d| d.border_color(ring_color))
                            .when(!selector_focused, |d| {
                                d.border_color(gpui::transparent_black())
                            })
                            .p(px(2.0))
                            .when(primary_enabled, |d| {
                                d.on_mouse_down(
                                    MouseButton::Left,
                                    cx.listener(|this, _, window, cx| {
                                        this.enter_edit_mode_for_field(
                                            FormFocus::HostValueSource,
                                            window,
                                            cx,
                                        );
                                    }),
                                )
                            })
                            .child(self.host_value_source_selector.clone()),
                    )
                    .child(
                        div()
                            .flex_1()
                            .rounded(px(4.0))
                            .border_2()
                            .when(input_focused, |d| d.border_color(ring_color))
                            .when(!input_focused, |d| {
                                d.border_color(gpui::transparent_black())
                            })
                            .p(px(2.0))
                            .when(primary_enabled, |d| {
                                d.on_mouse_down(
                                    MouseButton::Left,
                                    cx.listener(|this, _, window, cx| {
                                        this.enter_edit_mode_for_field(FormFocus::Host, window, cx);
                                    }),
                                )
                            })
                            .child(Input::new(primary_input).disabled(!primary_enabled)),
                    )
                    .when(!using_uri, |d| {
                        d.child(
                            div()
                                .w(px(110.0))
                                .rounded(px(4.0))
                                .border_2()
                                .when(port_focused, |dd| dd.border_color(ring_color))
                                .when(!port_focused, |dd| {
                                    dd.border_color(gpui::transparent_black())
                                })
                                .p(px(2.0))
                                .when(port_enabled, |dd| {
                                    dd.on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, window, cx| {
                                            this.enter_edit_mode_for_field(
                                                FormFocus::Port,
                                                window,
                                                cx,
                                            );
                                        }),
                                    )
                                })
                                .child(Input::new(port_input).disabled(!port_enabled)),
                        )
                    }),
            )
            .into_any_element()
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
        let danger_color = cx.theme().danger;
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
                        d.child(div().text_sm().text_color(danger_color).child("*"))
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

        if let Some(path) = self.pending_file_path.take()
            && let Some(input) = self.driver_inputs.get("path").cloned()
        {
            input.update(cx, |state, cx| {
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

        self.apply_pending_auth_profile(window, cx);
        self.apply_pending_ssm_auth_profile();

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

        if self.normalize_focus_for_state(cx) {
            cx.notify();
        }

        let show_password = self.show_password;
        let password_source_is_literal =
            self.password_value_source_selector.read(cx).is_literal(cx);
        let show_ssh_passphrase = self.show_ssh_passphrase;
        let show_ssh_password = self.show_ssh_password;

        self.input_password.update(cx, |state, cx| {
            let should_mask = password_source_is_literal && !show_password;
            state.set_masked(should_mask, window, cx);
        });
        self.input_ssh_key_passphrase.update(cx, |state, cx| {
            state.set_masked(!show_ssh_passphrase, window, cx);
        });
        self.input_ssh_password.update(cx, |state, cx| {
            state.set_masked(!show_ssh_password, window, cx);
        });

        let csd_title_bar = platform::render_csd_title_bar(window, cx, "Connection Manager");

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
            .when_some(csd_title_bar, |el, title_bar| el.child(title_bar))
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
