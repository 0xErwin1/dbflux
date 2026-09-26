use crate::settings::layout;
use crate::ssh_shared::SshAuthSelection;
use crate::tokens::{ConnectionFormMetrics, FormMetrics};
use dbflux_app::keymap::ContextId;
use dbflux_components::components::form_renderer;
use dbflux_components::controls::{Button, Checkbox, Input, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{
    BannerBlock, BannerVariant, FocusShape, Icon as AppIconElement, Label, SegmentedControl,
    SegmentedItem, Text, focus_ring,
};
use dbflux_components::semantic::BannerColors as SemBannerColors;
use dbflux_components::tokens::{ChamferCut, ChromeColors, Heights};
use dbflux_core::{FormFieldDef, FormFieldKind, FormTab};
use dbflux_ui_base::platform;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

use super::{
    ActiveTab, ConnectionManagerWindow, DismissEvent, EditState, FormFocus, TestStatus, View,
    cm_field_id,
};

impl ConnectionManagerWindow {
    /// Build a form row of the connection form: the label in the 170 px
    /// column (with the required marker), the control on the right and an
    /// optional muted help line under it.
    pub(super) fn field_row_cm(
        label: impl Into<SharedString>,
        required: bool,
        control: impl IntoElement,
        help: Option<impl Into<SharedString>>,
        cx: &App,
    ) -> Div {
        let label_el = Label::new(label)
            .required(required)
            .color(cx.theme().foreground);

        div()
            .flex()
            .items_start()
            .gap(FormMetrics::ROW_GAP)
            .py(FormMetrics::ROW_PADDING_Y)
            .child(
                div()
                    .w(ConnectionFormMetrics::LABEL_WIDTH)
                    .flex_shrink_0()
                    .pt(FormMetrics::LABEL_PADDING_TOP)
                    .child(label_el),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .flex()
                    .flex_col()
                    .gap(FormMetrics::HELP_GAP)
                    .child(control)
                    .when_some(help, |column, help| {
                        column.child(layout::help_text(help.into()))
                    }),
            )
    }

    pub(super) fn render_focus_shell(
        &self,
        focused: bool,
        ring_color: Hsla,
        child: impl IntoElement,
        cx: &mut Context<Self>,
    ) -> gpui::Div {
        focus_ring(
            focused,
            FocusShape::Chamfer(ChamferCut::CONTROL),
            Some(ring_color),
            child,
            cx,
        )
    }

    pub(super) fn render_control_focus_shell(
        &self,
        focused: bool,
        ring_color: Hsla,
        child: impl IntoElement,
        cx: &mut Context<Self>,
    ) -> gpui::Div {
        focus_ring(
            focused,
            FocusShape::Chamfer(ChamferCut::CONTROL),
            Some(ring_color),
            child,
            cx,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn render_password_field(
        &self,
        show_focus: bool,
        show_save_checkbox: bool,
        save_password: bool,
        _ring_color: Hsla,
        help_text: Option<String>,
        label: &str,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let focus = self.form_focus;
        let password_source_is_literal = self
            .form
            .password_value_source_selector
            .read(cx)
            .is_literal(cx);

        let selector_focused = show_focus && focus == FormFocus::PasswordValueSource;
        let password_focused = show_focus && focus == FormFocus::Password;
        let toggle_focused = show_focus && focus == FormFocus::PasswordToggle;
        let checkbox_focused = show_focus && focus == FormFocus::PasswordSave;

        let toggle = password_source_is_literal.then(|| {
            let (icon, toggle_label) = if self.form.show_password {
                (
                    AppIcon::EyeOff,
                    dbflux_i18n::t!("settings.field.hide_secret"),
                )
            } else {
                (AppIcon::Eye, dbflux_i18n::t!("settings.field.show_secret"))
            };

            Button::new("toggle-password", toggle_label)
                .ghost()
                .small()
                .icon(icon)
                .icon_only()
                .tab_stop(false)
                .focused(toggle_focused)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.form.show_password = !this.form.show_password;
                    cx.notify();
                }))
        });

        let mut password_input = Input::new(&self.form.input_password)
            .id(cm_field_id("password"))
            .aria_label(label.to_string())
            .secret(true);

        if let Some(toggle) = toggle {
            password_input = password_input.suffix(toggle);
        }

        let controls = layout::inline_controls()
            .child(
                layout::cursor_ring(
                    selector_focused,
                    div()
                        .w(ConnectionFormMetrics::SOURCE_SELECT_WIDTH)
                        .child(self.form.password_value_source_selector.clone()),
                    cx,
                )
                .flex_shrink_0()
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(|this, _, window, cx| {
                        this.enter_edit_mode_for_field(FormFocus::PasswordValueSource, window, cx);
                    }),
                ),
            )
            .child(
                layout::field_frame(password_focused, None, true, password_input, cx)
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, _, window, cx| {
                            this.enter_edit_mode_for_field(FormFocus::Password, window, cx);
                        }),
                    ),
            )
            .when(show_save_checkbox && password_source_is_literal, |row| {
                row.child(
                    layout::cursor_ring(
                        checkbox_focused,
                        Checkbox::new("save-password")
                            .checked(save_password)
                            .label(dbflux_i18n::t!("connection_manager.action.save"))
                            .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                this.form.form_save_password = *checked;
                                cx.notify();
                            })),
                        cx,
                    )
                    .flex_shrink_0(),
                )
            });

        Self::field_row_cm(label.to_string(), false, controls, help_text, cx).into_any_element()
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
            .gap(FormMetrics::ROW_GAP)
            .py(FormMetrics::ROW_PADDING_Y)
            .child(
                div()
                    .w(ConnectionFormMetrics::LABEL_WIDTH)
                    .flex_shrink_0()
                    .child(Label::new(label.to_string())),
            )
            .child(Text::body(value.to_string()))
    }

    pub(super) fn render_section(
        &self,
        title: &str,
        content: impl IntoElement,
        _theme: &gpui_component::Theme,
        cx: &App,
    ) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .child(dbflux_components::composites::section_header(
                title.to_string(),
                None,
                cx,
            ))
            .child(content)
    }

    pub(super) fn render_form(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let Some(driver) = &self.form.selected_driver else {
            return div().into_any_element();
        };

        let driver_name = driver.display_name().to_string();
        let validation_errors = self.validation_errors.clone();
        let is_editing = self.editing_profile_id.is_some();
        let title = if is_editing {
            crate::labels::connection_manager_window_title_edit(&driver_name)
        } else {
            crate::labels::connection_manager_window_title_new(&driver_name)
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

        let test_banner = self.render_test_banner(cx);
        let header = self
            .render_form_header(title, is_editing, show_focus, cx)
            .into_any_element();
        let theme = cx.theme();
        let border_color = theme.border;

        div()
            .flex()
            .flex_col()
            .size_full()
            .child(header)
            .child(tab_bar)
            .child(
                div()
                    .id("form-scroll-content")
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_h_0()
                    .bg(theme.popover)
                    .overflow_scroll()
                    .track_scroll(&self.form_scroll_handle)
                    .px(ConnectionFormMetrics::PADDING_X)
                    .pt(ConnectionFormMetrics::BODY_PADDING_TOP)
                    .pb(ConnectionFormMetrics::BANNER_MARGIN_BOTTOM)
                    .when(!validation_errors.is_empty(), |d| {
                        let combined = validation_errors.join("\n");
                        d.child(
                            div().py(FormMetrics::ROW_PADDING_Y).child(
                                BannerBlock::new(
                                    BannerVariant::Danger,
                                    dbflux_i18n::t!("connection_manager.banner.correct_following"),
                                )
                                .with_body(combined),
                            ),
                        )
                    })
                    .children(tab_content),
            )
            .when_some(test_banner, |form, banner| {
                form.child(
                    div()
                        .flex_shrink_0()
                        .bg(cx.theme().popover)
                        .px(ConnectionFormMetrics::PADDING_X)
                        .pb(ConnectionFormMetrics::BANNER_MARGIN_BOTTOM)
                        .child(banner),
                )
            })
            .child(
                div()
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .gap(FormMetrics::INLINE_GAP)
                    .h(crate::tokens::SettingsMetrics::FOOTER_HEIGHT)
                    .px(crate::tokens::SettingsMetrics::FOOTER_PADDING_X)
                    .border_t_1()
                    .border_color(border_color)
                    .bg(cx.theme().background)
                    .child(
                        Button::new(
                            "test-connection",
                            dbflux_i18n::t!("connection_manager.action.test_connection"),
                        )
                        .small()
                        .secondary()
                        .icon(AppIcon::Plug)
                        .focused(test_focused)
                        .disabled(self.test_status == TestStatus::Testing)
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.test_connection(window, cx);
                        })),
                    )
                    .child(div().flex_1())
                    .child(
                        Button::new(
                            "footer-cancel",
                            dbflux_i18n::t!("connection_manager.driver_select.cancel"),
                        )
                        .small()
                        .secondary()
                        .on_click(cx.listener(|_, _, window, cx| {
                            cx.emit(DismissEvent);
                            window.remove_window();
                        })),
                    )
                    .child(
                        Button::new(
                            "save-connection",
                            dbflux_i18n::t!("connection_manager.action.save"),
                        )
                        .small()
                        .primary()
                        .icon(AppIcon::Check)
                        .kbd("Ctrl S")
                        .focused(save_focused)
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.save_profile(window, cx);
                        })),
                    ),
            )
            .into_any_element()
    }

    /// Header of the form (P1ConnForm): back button, driver logo, title and
    /// subtitle, and the connection name field on the right.
    fn render_form_header(
        &self,
        title: String,
        is_editing: bool,
        show_focus: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let brand_icon = self.form.selected_driver.as_ref().map(|driver| {
            let metadata = driver.metadata();
            AppIcon::for_driver(metadata.icon, metadata.category)
        });
        let name_focused = show_focus && self.form_focus == FormFocus::Name;
        let name_label = dbflux_i18n::t!("connection_manager.field.name");
        let theme = cx.theme();
        let muted = theme.muted_foreground;
        let strong = ChromeColors::strong(theme);
        let border = theme.border;

        let name_field = layout::field_frame(
            name_focused,
            Some(ConnectionFormMetrics::NAME_FIELD_WIDTH),
            false,
            Input::new(&self.form.input_name)
                .id(cm_field_id("name"))
                .aria_label(name_label)
                .prefix(
                    AppIconElement::new(AppIcon::Pencil)
                        .size(Heights::ICON_SM)
                        .color(muted),
                ),
            cx,
        )
        .on_mouse_down(
            MouseButton::Left,
            cx.listener(|this, _, window, cx| {
                this.enter_edit_mode_for_field(FormFocus::Name, window, cx);
            }),
        );

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(ConnectionFormMetrics::HEADER_GAP)
            .py(ConnectionFormMetrics::HEADER_PADDING_Y)
            .px(ConnectionFormMetrics::PADDING_X)
            .border_b_1()
            .border_color(border)
            .when(!is_editing, |header| {
                header.child(
                    Button::new("back", dbflux_i18n::t!("connection_manager.action.back"))
                        .secondary()
                        .icon(AppIcon::ChevronLeft)
                        .icon_only()
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.back_to_driver_select(window, cx);
                        })),
                )
            })
            .when_some(brand_icon, |header, icon| {
                header.child(
                    AppIconElement::new(icon)
                        .size(ConnectionFormMetrics::HEADER_LOGO)
                        .color(theme.foreground),
                )
            })
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(ConnectionFormMetrics::TITLE_GAP)
                    .child(
                        Text::title(title)
                            .font_size(ConnectionFormMetrics::TITLE_FONT)
                            .text_color(strong),
                    )
                    .child(
                        Text::body(dbflux_i18n::t!("connection_manager.form.required_hint"))
                            .font_size(ConnectionFormMetrics::SUBTITLE_FONT)
                            .muted_foreground(),
                    ),
            )
            .child(div().flex_1())
            .child(name_field)
    }

    /// Test-connection result banner shown above the footer.
    fn render_test_banner(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        let test_status = self.test_status;
        let test_error = self.test_error.clone();
        let test_result_body = self
            .test_result
            .as_ref()
            .map(|r| r.format_body())
            .filter(|s| !s.is_empty());
        let banners = SemBannerColors::for_current(cx);

        let banner = match test_status {
            TestStatus::None => return None,
            TestStatus::Testing => BannerBlock::new(
                BannerVariant::Info,
                dbflux_i18n::t!("connection_manager.banner.testing_connection"),
            )
            .with_icon(
                AppIconElement::new(AppIcon::Loader)
                    .size(Heights::ICON_SM)
                    .color(banners.info_fg),
            ),
            TestStatus::Success => {
                let mut banner = BannerBlock::new(
                    BannerVariant::Success,
                    dbflux_i18n::t!("connection_manager.banner.connection_successful"),
                )
                .with_icon(
                    AppIconElement::new(AppIcon::CircleCheck)
                        .size(Heights::ICON_SM)
                        .color(banners.success_fg),
                );
                if let Some(body) = test_result_body {
                    banner = banner.with_body(body);
                }
                banner
            }
            TestStatus::SuccessWithWarning => {
                let mut banner = BannerBlock::new(
                    BannerVariant::Warning,
                    dbflux_i18n::t!("connection_manager.banner.connection_successful_warnings"),
                )
                .with_icon(
                    AppIconElement::new(AppIcon::Info)
                        .size(Heights::ICON_SM)
                        .color(banners.warning_fg),
                );
                if let Some(body) = test_error.or(test_result_body) {
                    banner = banner.with_body(body);
                }
                banner
            }
            TestStatus::Failed => {
                let message = test_error.unwrap_or_else(|| {
                    dbflux_i18n::t!("connection_manager.banner.connection_failed")
                });
                let message_to_copy = message.clone();
                BannerBlock::new(
                    BannerVariant::Danger,
                    dbflux_i18n::t!("connection_manager.banner.connection_failed"),
                )
                .with_body(message)
                .with_icon(
                    AppIconElement::new(AppIcon::Info)
                        .size(Heights::ICON_SM)
                        .color(banners.error_fg),
                )
                .with_actions(
                    Button::new(
                        "copy-test-connection-error",
                        dbflux_i18n::t!("connection_manager.action.copy"),
                    )
                    .ghost()
                    .small()
                    .icon(AppIcon::Copy)
                    .on_click(move |_, _, cx| {
                        cx.write_to_clipboard(ClipboardItem::new_string(message_to_copy.clone()));
                    }),
                )
            }
        };

        Some(banner.into_any_element())
    }

    /// Frame around a control that the form's keyboard cursor can land on:
    /// the cursor ring, and a press that moves the cursor to `field` and
    /// starts editing it.
    #[allow(clippy::too_many_arguments)]
    fn cm_control_frame(
        &self,
        focused: bool,
        field: Option<FormFocus>,
        enabled: bool,
        width: Option<Pixels>,
        mono: bool,
        control: impl IntoElement,
        cx: &mut Context<Self>,
    ) -> Div {
        layout::field_frame(focused, width, mono, control, cx)
            .when(!enabled, |frame| {
                frame.opacity(dbflux_components::tokens::Fields::DISABLED_OPACITY)
            })
            .when_some(field.filter(|_| enabled), |frame, field| {
                frame.on_mouse_down(
                    MouseButton::Left,
                    cx.listener(move |this, _, window, cx| {
                        this.enter_edit_mode_for_field(field, window, cx);
                    }),
                )
            })
    }

    fn render_form_field(
        &self,
        field_def: &FormFieldDef,
        is_ssh_tab: bool,
        show_focus: bool,
        _ring_color: Hsla,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let field_focus = Self::field_id_to_focus(&field_def.id, is_ssh_tab);
        let focused = show_focus && field_focus == Some(self.form_focus);

        match &field_def.kind {
            // WriteOnly fields behave identically to Password in connection forms:
            // the input is masked and starts empty.
            FormFieldKind::Text
            | FormFieldKind::Password
            | FormFieldKind::WriteOnly
            | FormFieldKind::Number => {
                let Some(input_state) = self.input_state_for_field(&field_def.id) else {
                    return div().into_any_element();
                };

                let field_enabled = self.is_field_enabled(field_def);

                if !is_ssh_tab && (field_def.id == "database" || field_def.id == "user") {
                    let (selector, selector_focus, input_focus) = if field_def.id == "database" {
                        (
                            self.form.database_value_source_selector.clone(),
                            FormFocus::DatabaseValueSource,
                            FormFocus::Database,
                        )
                    } else {
                        (
                            self.form.user_value_source_selector.clone(),
                            FormFocus::UserValueSource,
                            FormFocus::User,
                        )
                    };

                    let selector_focused = show_focus && self.form_focus == selector_focus;
                    let input_focused = show_focus && self.form_focus == input_focus;

                    let control = layout::inline_controls()
                        .child(self.cm_control_frame(
                            selector_focused,
                            Some(selector_focus),
                            field_enabled,
                            Some(ConnectionFormMetrics::SOURCE_SELECT_WIDTH),
                            false,
                            selector,
                            cx,
                        ))
                        .child(
                            self.cm_control_frame(
                                input_focused,
                                Some(input_focus),
                                field_enabled,
                                None,
                                true,
                                Input::new(input_state)
                                    .id(cm_field_id(&field_def.id))
                                    .aria_label(field_def.label.clone())
                                    .disabled(!field_enabled),
                                cx,
                            ),
                        );

                    return Self::field_row_cm(
                        field_def.label.clone(),
                        field_def.required && field_enabled,
                        control,
                        field_def.help.clone(),
                        cx,
                    )
                    .into_any_element();
                }

                let help_text = field_def.help.clone();
                let is_secret = form_renderer::is_secret_field(&field_def.kind);

                let input = Input::new(input_state)
                    .id(cm_field_id(&field_def.id))
                    .aria_label(field_def.label.clone())
                    .disabled(!field_enabled)
                    .secret(is_secret);

                let control = match field_focus {
                    Some(field) => self.cm_control_frame(
                        focused,
                        Some(field),
                        field_enabled,
                        None,
                        !is_secret,
                        input,
                        cx,
                    ),
                    None => {
                        let fallback_input_focus = input_state.clone();

                        self.cm_control_frame(
                            false,
                            None,
                            field_enabled,
                            None,
                            !is_secret,
                            input,
                            cx,
                        )
                        .when(field_enabled, |frame| {
                            frame.on_mouse_down(
                                MouseButton::Left,
                                cx.listener(move |this, _, window, cx| {
                                    this.begin_inline_editor_interaction(cx);
                                    fallback_input_focus.update(cx, |state, cx| {
                                        state.focus(window, cx);
                                    });
                                }),
                            )
                        })
                    }
                };

                Self::field_row_cm(
                    field_def.label.clone(),
                    field_def.required && field_enabled,
                    control,
                    help_text,
                    cx,
                )
                .into_any_element()
            }

            FormFieldKind::FilePath => {
                let Some(input_state) = self.input_state_for_field(&field_def.id) else {
                    return div().into_any_element();
                };

                let browse_focused = show_focus && self.form_focus == FormFocus::FileBrowse;

                let control = layout::inline_controls()
                    .child(
                        self.cm_control_frame(
                            focused,
                            field_focus,
                            true,
                            None,
                            true,
                            Input::new(input_state)
                                .id(cm_field_id(&field_def.id))
                                .aria_label(field_def.label.clone()),
                            cx,
                        ),
                    )
                    .child(
                        Button::new(
                            "browse-file-path",
                            dbflux_i18n::t!("connection_manager.action.browse"),
                        )
                        .small()
                        .secondary()
                        .icon(AppIcon::Folder)
                        .focused(browse_focused)
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.browse_file_path(window, cx);
                        })),
                    );

                Self::field_row_cm(
                    field_def.label.clone(),
                    field_def.required,
                    control,
                    field_def.help.clone(),
                    cx,
                )
                .into_any_element()
            }

            FormFieldKind::Checkbox if field_def.id == "use_uri" => {
                self.render_use_uri_selector(focused, cx).into_any_element()
            }

            FormFieldKind::Checkbox => {
                let field_id = field_def.id.clone();
                let is_checked = if field_id == "ssh_enabled" {
                    self.access.ssh_enabled
                } else {
                    self.form
                        .checkbox_states
                        .get(&field_id)
                        .copied()
                        .unwrap_or(false)
                };

                let checkbox_id = gpui::SharedString::from(field_id.clone());
                let control = layout::check_row(
                    layout::cursor_ring(
                        focused,
                        Checkbox::new(checkbox_id)
                            .checked(is_checked)
                            .label(field_def.label.as_str())
                            .on_click(cx.listener(move |this, checked: &bool, window, cx| {
                                if field_id == "ssh_enabled" {
                                    this.access.ssh_enabled = *checked;
                                } else {
                                    this.form.checkbox_states.insert(field_id.clone(), *checked);
                                }
                                window.focus(&this.focus_handle, cx);
                                cx.notify();
                            })),
                        cx,
                    ),
                    field_def.help.clone().map(SharedString::from),
                );

                // Checkboxes carry their own label; the label column stays
                // empty so the box lines up with the other controls.
                Self::field_row_cm("", false, control, None::<&str>, cx).into_any_element()
            }

            FormFieldKind::Select { options } => {
                if field_def.id == "ssh_auth_method" {
                    let selected_index = match self.access.ssh_auth_method {
                        SshAuthSelection::PrivateKey => 0,
                        SshAuthSelection::Password => 1,
                    };

                    let items: Vec<SegmentedItem> = options
                        .iter()
                        .enumerate()
                        .map(|(idx, opt)| SegmentedItem::new(idx.to_string(), opt.label.clone()))
                        .collect();
                    let entity = cx.entity();

                    let control = SegmentedControl::new(
                        items,
                        selected_index.to_string(),
                        move |selected, window, cx| {
                            let method = if selected.as_ref() == "0" {
                                SshAuthSelection::PrivateKey
                            } else {
                                SshAuthSelection::Password
                            };

                            entity.update(cx, |this, cx| {
                                this.access.ssh_auth_method = method;
                                window.focus(&this.focus_handle, cx);
                                cx.notify();
                            });
                        },
                    );

                    Self::field_row_cm(
                        field_def.label.clone(),
                        false,
                        div().flex().child(control.focused(focused)),
                        None::<&str>,
                        cx,
                    )
                    .into_any_element()
                } else {
                    let field_id = field_def.id.clone();
                    let field_enabled = self.is_field_enabled(field_def);
                    let selected_value = self
                        .form
                        .select_values
                        .get(&field_id)
                        .cloned()
                        .unwrap_or_else(|| field_def.default_value.clone());

                    let items: Vec<SegmentedItem> = options
                        .iter()
                        .map(|opt| SegmentedItem::new(opt.value.clone(), opt.label.clone()))
                        .collect();
                    let entity = cx.entity();

                    let control = SegmentedControl::new(
                        items,
                        selected_value,
                        move |selected, window, cx| {
                            if !field_enabled {
                                return;
                            }

                            let field_id = field_id.clone();
                            let value = selected.to_string();

                            entity.update(cx, |this, cx| {
                                this.form.select_values.insert(field_id, value);
                                window.focus(&this.focus_handle, cx);
                                cx.notify();
                            });
                        },
                    );

                    Self::field_row_cm(
                        field_def.label.clone(),
                        field_def.required && field_enabled,
                        div()
                            .id(cm_field_id(&field_def.id))
                            .flex()
                            .when(!field_enabled, |row| {
                                row.opacity(dbflux_components::tokens::Fields::DISABLED_OPACITY)
                            })
                            .child(control.focused(focused)),
                        field_def.help.clone(),
                        cx,
                    )
                    .into_any_element()
                }
            }

            FormFieldKind::AuthProfileRef { .. } => {
                let field_enabled = self.is_field_enabled(field_def);

                let dropdown = div()
                    .w(ConnectionFormMetrics::DATABASE_FIELD_WIDTH)
                    .when(!field_enabled, |d| {
                        d.opacity(dbflux_components::tokens::Fields::DISABLED_OPACITY)
                    })
                    .when(field_enabled, |d| {
                        d.on_mouse_down(
                            MouseButton::Left,
                            cx.listener(|this, _, _, cx| {
                                this.begin_inline_editor_interaction(cx);
                            }),
                        )
                    })
                    .child(self.auth_profile.auth_profile_dropdown.clone());

                Self::field_row_cm(
                    field_def.label.clone(),
                    field_def.required && field_enabled,
                    dropdown,
                    field_def.help.clone(),
                    cx,
                )
                .into_any_element()
            }

            // DynamicSelect is not used in driver connection forms.
            FormFieldKind::DynamicSelect { .. } => div().into_any_element(),
        }
    }

    /// "Enter as": the driver's URI option drawn as a two-segment control
    /// (Fields / Connection URI) instead of a checkbox.
    fn render_use_uri_selector(&self, focused: bool, cx: &mut Context<Self>) -> impl IntoElement {
        let uri_mode = self
            .form
            .checkbox_states
            .get("use_uri")
            .copied()
            .unwrap_or(false);
        let entity = cx.entity();

        let control = SegmentedControl::new(
            vec![
                SegmentedItem::new(
                    "fields",
                    dbflux_i18n::t!("connection_manager.enter_as.fields"),
                )
                .icon(AppIcon::Layers),
                SegmentedItem::new("uri", dbflux_i18n::t!("connection_manager.enter_as.uri"))
                    .icon(AppIcon::Link2),
            ],
            if uri_mode { "uri" } else { "fields" },
            move |selected, window, cx| {
                let use_uri = selected.as_ref() == "uri";

                entity.update(cx, |this, cx| {
                    let current = this
                        .form
                        .checkbox_states
                        .get("use_uri")
                        .copied()
                        .unwrap_or(false);

                    if current == use_uri {
                        return;
                    }

                    this.form
                        .checkbox_states
                        .insert("use_uri".to_string(), use_uri);

                    if use_uri {
                        this.sync_fields_to_uri(window, cx);
                    } else {
                        this.sync_uri_to_fields(window, cx);
                    }

                    window.focus(&this.focus_handle, cx);
                    cx.notify();
                });
            },
        );

        Self::field_row_cm(
            dbflux_i18n::t!("connection_manager.enter_as.label"),
            false,
            div()
                .id("cm-field-use_uri")
                .flex()
                .child(control.focused(focused)),
            None::<&str>,
            cx,
        )
    }

    /// Renders the sections of a driver form tab.
    ///
    /// The driver's `password` field is not rendered from its definition: the
    /// host-owned secret input (`secret_field`) is placed at that position
    /// instead and taken out of the option. When the form declares no
    /// `password` field, `secret_field` is left for the caller to place.
    pub(super) fn render_form_tab(
        &mut self,
        tab: &FormTab,
        is_ssh_tab: bool,
        show_focus: bool,
        ring_color: Hsla,
        secret_field: &mut Option<AnyElement>,
        cx: &mut Context<Self>,
    ) -> Vec<AnyElement> {
        let mut sections: Vec<AnyElement> = Vec::new();

        for section in &tab.sections {
            let fields: Vec<&FormFieldDef> = section.fields.iter().collect();

            if fields.is_empty() {
                continue;
            }

            let mut field_elements: Vec<AnyElement> = Vec::new();
            let mut i = 0;
            while i < fields.len() {
                let field = fields[i];

                if field.id == "password" && !is_ssh_tab {
                    if let Some(secret_field) = secret_field.take() {
                        field_elements.push(secret_field);
                    }

                    i += 1;
                    continue;
                }

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
                    .child(dbflux_components::composites::section_header(
                        section.title.clone(),
                        None,
                        cx,
                    ))
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
        _ring_color: Hsla,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let Some(host_input) = self.input_state_for_field("host") else {
            return div().into_any_element();
        };

        let Some(port_input) = self.input_state_for_field("port") else {
            return div().into_any_element();
        };

        let uri_mode_active = self
            .form
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

        let primary_field_id = match uri_field {
            Some(uri_field) if using_uri => &uri_field.id,
            _ => &host_field.id,
        };

        let port_enabled = !using_uri && self.is_field_enabled(port_field);

        let selector_focused = show_focus && self.form_focus == FormFocus::HostValueSource;
        let input_focused = show_focus && self.form_focus == FormFocus::Host;
        let port_focused = show_focus && self.form_focus == FormFocus::Port;

        let control = layout::inline_controls()
            .child(self.cm_control_frame(
                selector_focused,
                Some(FormFocus::HostValueSource),
                primary_enabled,
                Some(ConnectionFormMetrics::SOURCE_SELECT_WIDTH),
                false,
                self.form.host_value_source_selector.clone(),
                cx,
            ))
            .child(
                self.cm_control_frame(
                    input_focused,
                    Some(FormFocus::Host),
                    primary_enabled,
                    None,
                    true,
                    Input::new(primary_input)
                        .id(cm_field_id(primary_field_id))
                        .aria_label(primary_label.clone())
                        .disabled(!primary_enabled),
                    cx,
                ),
            )
            .when(!using_uri, |row| {
                row.child(
                    self.cm_control_frame(
                        port_focused,
                        Some(FormFocus::Port),
                        port_enabled,
                        Some(ConnectionFormMetrics::PORT_FIELD_WIDTH),
                        true,
                        Input::new(port_input)
                            .id(cm_field_id(&port_field.id))
                            .aria_label(port_field.label.clone())
                            .disabled(!port_enabled),
                        cx,
                    ),
                )
            });

        Self::field_row_cm(
            primary_label,
            primary_required && primary_enabled,
            control,
            None::<&str>,
            cx,
        )
        .into_any_element()
    }

    /// Labelled text field used by the Access tab: the label over the
    /// framed input.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn form_field_input(
        &self,
        label: &str,
        field_id: &str,
        input: &Entity<InputState>,
        required: bool,
        focused: bool,
        _ring_color: Hsla,
        field: FormFocus,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap(FormMetrics::HELP_GAP)
            .child(Label::new(label.to_string()).required(required))
            .child(
                self.cm_control_frame(
                    focused,
                    Some(field),
                    true,
                    None,
                    true,
                    Input::new(input)
                        .id(cm_field_id(field_id))
                        .aria_label(label.to_string()),
                    cx,
                ),
            )
    }

    /// Eye button that shows or hides a secret field's value.
    pub(super) fn render_password_toggle(
        show: bool,
        toggle_id: &'static str,
        _theme: &gpui_component::theme::Theme,
    ) -> Button {
        let (icon, label) = if show {
            (
                AppIcon::EyeOff,
                dbflux_i18n::t!("settings.field.hide_secret"),
            )
        } else {
            (AppIcon::Eye, dbflux_i18n::t!("settings.field.show_secret"))
        };

        Button::new(toggle_id, label)
            .ghost()
            .small()
            .icon(icon)
            .icon_only()
            .tab_stop(false)
    }
}

impl Render for ConnectionManagerWindow {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(path) = self.pending.ssh_key_path.take() {
            self.access.input_ssh_key_path.update(cx, |state, cx| {
                state.set_value(path, window, cx);
            });
        }

        if let Some(path) = self.pending.file_path.take()
            && let Some(input) = self.form.driver_inputs.get("path").cloned()
        {
            input.update(cx, |state, cx| {
                state.set_value(path, window, cx);
            });
        }

        if let Some(path) = self.pending.ssl_ca_cert_path.take() {
            self.form.ssl_ca_cert_input.update(cx, |state, cx| {
                state.set_value(path, window, cx);
            });
        }

        if let Some(path) = self.pending.ssl_client_cert_path.take() {
            self.form.ssl_client_cert_input.update(cx, |state, cx| {
                state.set_value(path, window, cx);
            });
        }

        if let Some(path) = self.pending.ssl_client_key_path.take() {
            self.form.ssl_client_key_input.update(cx, |state, cx| {
                state.set_value(path, window, cx);
            });
        }

        if let Some(proxy_id) = self.pending.proxy_selection.take() {
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

        if let Some(tunnel_id) = self.pending.ssh_tunnel_selection.take() {
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

        let show_password = self.form.show_password;
        let password_source_is_literal = self
            .form
            .password_value_source_selector
            .read(cx)
            .is_literal(cx);
        let show_ssh_passphrase = self.form.show_ssh_passphrase;
        let show_ssh_password = self.form.show_ssh_password;

        let secret_placeholder = self.secret_field_label(cx);

        self.form.input_password.update(cx, |state, cx| {
            let should_mask = password_source_is_literal && !show_password;
            state.set_masked(should_mask, window, cx);

            if state.presentation().placeholder().as_ref() != secret_placeholder.as_str() {
                state.set_placeholder(secret_placeholder, window, cx);
            }
        });
        self.access
            .input_ssh_key_passphrase
            .update(cx, |state, cx| {
                state.set_masked(!show_ssh_passphrase, window, cx);
            });
        self.access.input_ssh_password.update(cx, |state, cx| {
            state.set_masked(!show_ssh_password, window, cx);
        });

        let csd_title_bar = platform::render_csd_title_bar(
            window,
            cx,
            &dbflux_i18n::t!("connection_manager.window_title"),
        );

        let theme = cx.theme();

        div()
            .id("connection-manager")
            .key_context(ContextId::ConnectionManager.as_gpui_context())
            .track_focus(&self.focus_handle)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, window, cx| {
                    // In the driver-picker and import views, embedded inputs must
                    // remain freely clickable, so don't reclaim window focus here.
                    if matches!(this.view, View::DriverSelect | View::Import) {
                        return;
                    }
                    if this.edit_state == EditState::Navigating {
                        window.focus(&this.focus_handle, cx);
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
            .text_size(dbflux_components::tokens::FontSizes::BASE)
            .when_some(csd_title_bar, |el, title_bar| el.child(title_bar))
            .child(match self.view {
                View::DriverSelect => self.render_driver_select(window, cx).into_any_element(),
                View::EditForm => self.render_form(window, cx).into_any_element(),
                View::Import => div()
                    .relative()
                    .size_full()
                    .child(self.render_driver_select(window, cx))
                    .child(self.import_panel.clone())
                    .into_any_element(),
            })
    }
}

impl Focusable for ConnectionManagerWindow {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}
