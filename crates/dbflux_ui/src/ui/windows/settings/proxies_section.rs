use super::form_section::FormSection;
use super::layout;
use super::proxies::ProxyFormNav;
use super::section_trait::SectionFocusEvent;
use super::SettingsSection;
use super::SettingsSectionId;
use crate::app::{AppStateChanged, AppStateEntity};
use dbflux_components::controls::Button;
use dbflux_components::controls::{GpuiInput as Input, InputEvent, InputState};
use dbflux_components::primitives::focus_frame;
use dbflux_components::primitives::{Icon as FluxIcon, Label};
use dbflux_components::typography::{Body, MonoCaption, MonoMeta, PanelTitle};
use dbflux_core::{ProxyKind, ProxyProfile};
use gpui::prelude::*;
use gpui::*;
use gpui_component::checkbox::Checkbox;
use gpui_component::dialog::Dialog;
use gpui_component::{ActiveTheme, Icon, IconName, Sizable};
use uuid::Uuid;

#[derive(Clone, Copy, PartialEq)]
pub(super) enum ProxyAuthSelection {
    None,
    Basic,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) enum ProxyFocus {
    ProfileList,
    Form,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) enum ProxyFormField {
    Name,
    KindHttp,
    KindHttps,
    KindSocks5,
    Host,
    Port,
    AuthNone,
    AuthBasic,
    Username,
    Password,
    NoProxy,
    Enabled,
    SaveSecret,
    SaveButton,
    DeleteButton,
}

#[derive(Clone, Copy)]
pub(super) enum PendingProxyAction {
    ClearForm,
    EditIndex(usize),
}

pub(super) struct ProxiesSection {
    pub(super) app_state: Entity<AppStateEntity>,
    pub(super) editing_proxy_id: Option<Uuid>,
    pub(super) input_proxy_name: Entity<InputState>,
    pub(super) input_proxy_host: Entity<InputState>,
    pub(super) input_proxy_port: Entity<InputState>,
    pub(super) input_proxy_username: Entity<InputState>,
    pub(super) input_proxy_password: Entity<InputState>,
    pub(super) input_proxy_no_proxy: Entity<InputState>,
    pub(super) proxy_kind: ProxyKind,
    pub(super) proxy_auth_selection: ProxyAuthSelection,
    pub(super) proxy_save_secret: bool,
    pub(super) proxy_enabled: bool,
    pub(super) show_proxy_password: bool,
    pub(super) proxy_focus: ProxyFocus,
    pub(super) proxy_selected_idx: Option<usize>,
    pub(super) proxy_form_field: ProxyFormField,
    pub(super) proxy_editing_field: bool,
    pub(super) content_focused: bool,
    pub(super) switching_input: bool,
    pub(super) pending_delete_proxy_id: Option<Uuid>,
    pub(super) pending_discard_action: Option<PendingProxyAction>,
    pub(super) pending_sync_from_app_state: bool,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<SectionFocusEvent> for ProxiesSection {}

impl ProxiesSection {
    pub(super) fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let input_proxy_name = cx.new(|cx| InputState::new(window, cx).placeholder("Proxy name"));
        let input_proxy_host =
            cx.new(|cx| InputState::new(window, cx).placeholder("proxy.example.com"));
        let input_proxy_port = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("8080")
                .default_value("8080")
        });
        let input_proxy_username = cx.new(|cx| InputState::new(window, cx).placeholder("username"));
        let input_proxy_password = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("password")
                .masked(true)
        });
        let input_proxy_no_proxy =
            cx.new(|cx| InputState::new(window, cx).placeholder("localhost, 127.0.0.1, .internal"));

        let subscription = cx.subscribe(&app_state, |this, _, _: &AppStateChanged, cx| {
            this.pending_sync_from_app_state = true;
            cx.notify();
        });

        let blur_name = cx.subscribe(&input_proxy_name, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Blur) {
                if this.switching_input {
                    this.switching_input = false;
                    return;
                }
                cx.emit(SectionFocusEvent::RequestFocusReturn);
            }
        });

        let blur_host = cx.subscribe(&input_proxy_host, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Blur) {
                if this.switching_input {
                    this.switching_input = false;
                    return;
                }
                cx.emit(SectionFocusEvent::RequestFocusReturn);
            }
        });

        let blur_port = cx.subscribe(&input_proxy_port, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Blur) {
                if this.switching_input {
                    this.switching_input = false;
                    return;
                }
                cx.emit(SectionFocusEvent::RequestFocusReturn);
            }
        });

        let blur_username =
            cx.subscribe(&input_proxy_username, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Blur) {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            });

        let blur_password =
            cx.subscribe(&input_proxy_password, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Blur) {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            });

        let blur_no_proxy =
            cx.subscribe(&input_proxy_no_proxy, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Blur) {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            });

        Self {
            app_state,
            editing_proxy_id: None,
            input_proxy_name,
            input_proxy_host,
            input_proxy_port,
            input_proxy_username,
            input_proxy_password,
            input_proxy_no_proxy,
            proxy_kind: ProxyKind::Http,
            proxy_auth_selection: ProxyAuthSelection::None,
            proxy_save_secret: false,
            proxy_enabled: true,
            show_proxy_password: false,
            proxy_focus: ProxyFocus::ProfileList,
            proxy_selected_idx: None,
            proxy_form_field: ProxyFormField::Name,
            proxy_editing_field: false,
            content_focused: false,
            switching_input: false,
            pending_delete_proxy_id: None,
            pending_discard_action: None,
            pending_sync_from_app_state: false,
            _subscriptions: vec![
                subscription,
                blur_name,
                blur_host,
                blur_port,
                blur_username,
                blur_password,
                blur_no_proxy,
            ],
        }
    }

    fn render_password_toggle(
        show: bool,
        toggle_id: &'static str,
        theme: &gpui_component::theme::Theme,
    ) -> Stateful<Div> {
        let icon_name = if show {
            IconName::EyeOff
        } else {
            IconName::Eye
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
            .hover({
                let secondary = theme.secondary;
                move |div| div.bg(secondary)
            })
            .child(
                FluxIcon::new(icon_name)
                    .size(px(16.0))
                    .color(theme.muted_foreground),
            )
    }

    fn render_proxy_field(
        &self,
        label: &str,
        input: &Entity<InputState>,
        is_focused: bool,
        primary: Hsla,
        field: ProxyFormField,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(Label::new(label.to_string()))
            .child(
                focus_frame(
                    is_focused,
                    Some(primary),
                    layout::compact_input_shell(Input::new(input).small()),
                    cx,
                )
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(move |this, _, window, cx| {
                        this.switching_input = true;
                        this.proxy_focus = ProxyFocus::Form;
                        this.proxy_form_field = field;
                        this.proxy_focus_current_field(window, cx);
                        cx.notify();
                    }),
                ),
            )
    }

    fn render_proxy_kind_selector(
        &self,
        is_form_focused: bool,
        current_field: ProxyFormField,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let primary = theme.primary;
        let border = theme.border;
        let current_kind = self.proxy_kind;

        let kinds = [
            (ProxyFormField::KindHttp, ProxyKind::Http, "HTTP"),
            (ProxyFormField::KindHttps, ProxyKind::Https, "HTTPS"),
            (ProxyFormField::KindSocks5, ProxyKind::Socks5, "SOCKS5"),
        ];

        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(Label::new("Protocol"))
            .child(div().flex().gap_4().children(kinds.into_iter().map(
                |(form_field, kind, label)| {
                    let is_focused = is_form_focused && current_field == form_field;

                    div()
                        .id(SharedString::from(format!("proxy-kind-{}", label)))
                        .flex()
                        .items_center()
                        .gap_2()
                        .px_2()
                        .py_1()
                        .rounded(px(4.0))
                        .cursor_pointer()
                        .border_1()
                        .border_color(if is_focused {
                            primary
                        } else {
                            transparent_black()
                        })
                        .on_click(cx.listener(move |this, _, window, cx| {
                            this.proxy_kind = kind;
                            this.input_proxy_port.update(cx, |state, cx| {
                                state.set_value(kind.default_port().to_string(), window, cx);
                            });
                            this.validate_proxy_form_field();
                            cx.notify();
                        }))
                        .child(Self::render_radio_button(
                            current_kind == kind,
                            primary,
                            border,
                        ))
                        .child(div().text_sm().child(label))
                },
            )))
    }

    fn render_proxy_auth_selector(
        &self,
        is_form_focused: bool,
        current_field: ProxyFormField,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let primary = theme.primary;
        let border = theme.border;
        let current_auth = self.proxy_auth_selection;

        let is_none_focused = is_form_focused && current_field == ProxyFormField::AuthNone;
        let is_basic_focused = is_form_focused && current_field == ProxyFormField::AuthBasic;

        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(Label::new("Authentication"))
            .child(
                div()
                    .flex()
                    .gap_4()
                    .child(
                        div()
                            .id("proxy-auth-none")
                            .flex()
                            .items_center()
                            .gap_2()
                            .px_2()
                            .py_1()
                            .rounded(px(4.0))
                            .cursor_pointer()
                            .border_1()
                            .border_color(if is_none_focused {
                                primary
                            } else {
                                transparent_black()
                            })
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.proxy_auth_selection = ProxyAuthSelection::None;
                                this.validate_proxy_form_field();
                                cx.notify();
                            }))
                            .child(Self::render_radio_button(
                                current_auth == ProxyAuthSelection::None,
                                primary,
                                border,
                            ))
                            .child(div().text_sm().child("None")),
                    )
                    .child(
                        div()
                            .id("proxy-auth-basic")
                            .flex()
                            .items_center()
                            .gap_2()
                            .px_2()
                            .py_1()
                            .rounded(px(4.0))
                            .cursor_pointer()
                            .border_1()
                            .border_color(if is_basic_focused {
                                primary
                            } else {
                                transparent_black()
                            })
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.proxy_auth_selection = ProxyAuthSelection::Basic;
                                this.validate_proxy_form_field();
                                cx.notify();
                            }))
                            .child(Self::render_radio_button(
                                current_auth == ProxyAuthSelection::Basic,
                                primary,
                                border,
                            ))
                            .child(div().text_sm().child("Basic")),
                    ),
            )
    }

    fn render_proxy_auth_fields(
        &self,
        keyring_available: bool,
        is_form_focused: bool,
        current_field: ProxyFormField,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let primary = theme.primary;

        let is_save_secret_focused = is_form_focused && current_field == ProxyFormField::SaveSecret;
        let is_password_focused = is_form_focused && current_field == ProxyFormField::Password;

        let save_checkbox = if keyring_available {
            Some(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .pb(px(2.0))
                    .px_2()
                    .py_1()
                    .rounded(px(4.0))
                    .border_1()
                    .border_color(if is_save_secret_focused {
                        primary
                    } else {
                        transparent_black()
                    })
                    .child(
                        Checkbox::new("proxy-save-secret")
                            .checked(self.proxy_save_secret)
                            .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                this.proxy_save_secret = *checked;
                                cx.notify();
                            })),
                    )
                    .child(div().text_sm().child("Save")),
            )
        } else {
            None
        };

        let password_toggle =
            Self::render_password_toggle(self.show_proxy_password, "toggle-proxy-password", &theme)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.show_proxy_password = !this.show_proxy_password;
                    cx.notify();
                }));

        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(self.render_proxy_field(
                "Username",
                &self.input_proxy_username,
                is_form_focused && current_field == ProxyFormField::Username,
                primary,
                ProxyFormField::Username,
                cx,
            ))
            .child(
                div()
                    .flex()
                    .items_end()
                    .gap_3()
                    .child(
                        div()
                            .flex_1()
                            .flex()
                            .items_end()
                            .gap_1()
                            .child(div().flex_1().child(self.render_proxy_field(
                                "Password",
                                &self.input_proxy_password,
                                is_password_focused,
                                primary,
                                ProxyFormField::Password,
                                cx,
                            )))
                            .child(password_toggle),
                    )
                    .when_some(save_checkbox, |div, checkbox| div.child(checkbox)),
            )
    }

    fn render_proxy_list(
        &self,
        proxies: &[ProxyProfile],
        editing_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let is_list_focused = self.proxy_focus == ProxyFocus::ProfileList;
        let is_new_button_focused = is_list_focused && self.proxy_selected_idx.is_none();

        div()
            .w(px(250.0))
            .h_full()
            .border_r_1()
            .border_color(theme.border)
            .flex()
            .flex_col()
            .child(
                div().p_2().border_b_1().border_color(theme.border).child(
                    div()
                        .rounded(px(4.0))
                        .border_1()
                        .border_color(if is_new_button_focused {
                            theme.primary
                        } else {
                            transparent_black()
                        })
                        .child(
                            Button::new("new-proxy", "New Proxy")
                                .icon(Icon::new(IconName::Plus))
                                .small()
                                .w_full()
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.request_proxy_action(
                                        PendingProxyAction::ClearForm,
                                        window,
                                        cx,
                                    );
                                })),
                        ),
                ),
            )
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .p_2()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .when(proxies.is_empty(), |root: Div| {
                        root.child(
                            div()
                                .p_4()
                                .child(Body::new("No saved proxies").color(theme.muted_foreground)),
                        )
                    })
                    .children(proxies.iter().enumerate().map(|(idx, proxy)| {
                        let proxy_id = proxy.id;
                        let is_selected = editing_id == Some(proxy_id);
                        let is_focused = is_list_focused && self.proxy_selected_idx == Some(idx);
                        let subtitle =
                            format!("{}://{}:{}", proxy.kind.scheme(), proxy.host, proxy.port);

                        div()
                            .id(SharedString::from(format!("proxy-item-{}", proxy_id)))
                            .px_3()
                            .py_2()
                            .rounded(px(4.0))
                            .cursor_pointer()
                            .border_1()
                            .border_color(if is_focused && !is_selected {
                                theme.primary
                            } else {
                                transparent_black()
                            })
                            .when(is_selected, |div| div.bg(theme.secondary))
                            .hover({
                                let secondary = theme.secondary;
                                move |div| div.bg(secondary)
                            })
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.request_proxy_action(
                                    PendingProxyAction::EditIndex(idx),
                                    window,
                                    cx,
                                );
                                this.proxy_focus = ProxyFocus::Form;
                                this.proxy_form_field = ProxyFormField::Name;
                            }))
                            .child(
                                div()
                                    .flex()
                                    .items_start()
                                    .gap_2()
                                    .child(
                                        div()
                                            .flex()
                                            .items_center()
                                            .gap_1()
                                            .mt(px(2.0))
                                            .child(
                                                FluxIcon::new(IconName::Globe)
                                                    .size(px(14.0))
                                                    .color(theme.muted_foreground),
                                            )
                                            .when(!proxy.enabled, |root| {
                                                root.child(MonoCaption::new("off"))
                                            }),
                                    )
                                    .child(
                                        div()
                                            .flex()
                                            .flex_col()
                                            .gap_1()
                                            .child(Body::new(proxy.name.clone()))
                                            .child(MonoMeta::new(subtitle)),
                                    ),
                            )
                    })),
            )
    }

    fn render_proxy_form(
        &self,
        editing_id: Option<Uuid>,
        keyring_available: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let primary = theme.primary;
        let _muted_foreground = theme.muted_foreground;

        let is_form_focused = self.proxy_focus == ProxyFocus::Form;
        let field = self.proxy_form_field;

        layout::sticky_form_shell(
            PanelTitle::new(layout::editor_panel_title("Proxy", editing_id.is_some())),
            div()
                .flex()
                .flex_col()
                .gap_4()
                .child(self.render_proxy_field(
                    "Name",
                    &self.input_proxy_name,
                    is_form_focused && field == ProxyFormField::Name,
                    primary,
                    ProxyFormField::Name,
                    cx,
                ))
                .child(self.render_proxy_kind_selector(is_form_focused, field, cx))
                .child(
                    div()
                        .flex()
                        .gap_3()
                        .child(div().flex_1().child(self.render_proxy_field(
                            "Host",
                            &self.input_proxy_host,
                            is_form_focused && field == ProxyFormField::Host,
                            primary,
                            ProxyFormField::Host,
                            cx,
                        )))
                        .child(div().w(px(80.0)).child(self.render_proxy_field(
                            "Port",
                            &self.input_proxy_port,
                            is_form_focused && field == ProxyFormField::Port,
                            primary,
                            ProxyFormField::Port,
                            cx,
                        ))),
                )
                .child(self.render_proxy_auth_selector(is_form_focused, field, cx))
                .when(
                    self.proxy_auth_selection == ProxyAuthSelection::Basic,
                    |div: Div| {
                        div.child(self.render_proxy_auth_fields(
                            keyring_available,
                            is_form_focused,
                            field,
                            cx,
                        ))
                    },
                )
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(self.render_proxy_field(
                            "No Proxy",
                            &self.input_proxy_no_proxy,
                            is_form_focused && field == ProxyFormField::NoProxy,
                            primary,
                            ProxyFormField::NoProxy,
                            cx,
                        ))
                        .child(
                            Body::new("Comma-separated hosts/CIDRs to bypass the proxy")
                                .color(theme.muted_foreground),
                        ),
                )
                .child({
                    let is_enabled_focused = is_form_focused && field == ProxyFormField::Enabled;

                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .px_2()
                        .py_1()
                        .rounded(px(4.0))
                        .border_1()
                        .border_color(if is_enabled_focused {
                            primary
                        } else {
                            transparent_black()
                        })
                        .child(
                            Checkbox::new("proxy-enabled")
                                .checked(self.proxy_enabled)
                                .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                    this.proxy_enabled = *checked;
                                    cx.notify();
                                })),
                        )
                        .child(Body::new("Enabled"))
                }),
            div()
                .flex()
                .gap_2()
                .justify_end()
                .when(editing_id.is_some(), |root| {
                    let proxy_id = editing_id.expect("checked is_some");
                    let is_delete_focused =
                        is_form_focused && field == ProxyFormField::DeleteButton;

                    root.child(
                        div()
                            .rounded(px(4.0))
                            .border_1()
                            .border_color(if is_delete_focused {
                                primary
                            } else {
                                transparent_black()
                            })
                            .child(
                                Button::new("delete-proxy", "Delete")
                                    .small()
                                    .danger()
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.request_delete_proxy(proxy_id, cx);
                                    })),
                            ),
                    )
                })
                .child(div().flex_1())
                .child({
                    let is_save_focused = is_form_focused && field == ProxyFormField::SaveButton;

                    div()
                        .rounded(px(4.0))
                        .border_1()
                        .border_color(if is_save_focused {
                            primary
                        } else {
                            transparent_black()
                        })
                        .child(
                            Button::new(
                                "save-proxy",
                                if editing_id.is_some() {
                                    "Update"
                                } else {
                                    "Create"
                                },
                            )
                            .small()
                            .primary()
                            .on_click(cx.listener(
                                |this, _, window, cx| {
                                    this.save_proxy(window, cx);
                                },
                            )),
                        )
                }),
            &theme,
        )
    }

    fn render_radio_button(selected: bool, primary: Hsla, border: Hsla) -> Div {
        div()
            .size_4()
            .rounded_full()
            .border_1()
            .border_color(if selected { primary } else { border })
            .flex()
            .items_center()
            .justify_center()
            .child(div().size_2().rounded_full().bg(if selected {
                primary
            } else {
                transparent_black()
            }))
    }
}

impl FormSection for ProxiesSection {
    type Focus = ProxyFocus;
    type FormField = ProxyFormField;

    fn focus_area(&self) -> Self::Focus {
        self.proxy_focus
    }

    fn set_focus_area(&mut self, focus: Self::Focus) {
        self.proxy_focus = focus;
    }

    fn form_field(&self) -> Self::FormField {
        self.proxy_form_field
    }

    fn set_form_field(&mut self, field: Self::FormField) {
        self.proxy_form_field = field;
    }

    fn editing_field(&self) -> bool {
        self.proxy_editing_field
    }

    fn set_editing_field(&mut self, editing: bool) {
        self.proxy_editing_field = editing;
    }

    fn switching_input(&self) -> bool {
        self.switching_input
    }

    fn set_switching_input(&mut self, switching: bool) {
        self.switching_input = switching;
    }

    fn content_focused(&self) -> bool {
        self.content_focused
    }

    fn list_focus() -> Self::Focus {
        ProxyFocus::ProfileList
    }

    fn form_focus() -> Self::Focus {
        ProxyFocus::Form
    }

    fn first_form_field() -> Self::FormField {
        ProxyFormField::Name
    }

    fn form_rows(&self) -> Vec<Vec<Self::FormField>> {
        ProxyFormNav::new(
            self.proxy_auth_selection,
            self.editing_proxy_id,
            self.proxy_form_field,
        )
        .form_rows()
    }

    fn is_input_field(field: Self::FormField) -> bool {
        ProxyFormNav::is_input_field(field)
    }

    fn focus_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        ProxiesSection::proxy_focus_current_field(self, window, cx);
    }

    fn activate_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        ProxiesSection::proxy_activate_current_field(self, window, cx);
    }

    fn tab_next(&mut self) {
        let mut nav = ProxyFormNav::new(
            self.proxy_auth_selection,
            self.editing_proxy_id,
            self.proxy_form_field,
        );
        nav.tab_next();
        self.proxy_form_field = nav.field();
    }

    fn tab_prev(&mut self) {
        let mut nav = ProxyFormNav::new(
            self.proxy_auth_selection,
            self.editing_proxy_id,
            self.proxy_form_field,
        );
        nav.tab_prev();
        self.proxy_form_field = nav.field();
    }

    fn validate_form_field(&mut self) {
        let mut nav = ProxyFormNav::new(
            self.proxy_auth_selection,
            self.editing_proxy_id,
            self.proxy_form_field,
        );
        nav.validate_field();
        self.proxy_form_field = nav.field();
    }
}

impl SettingsSection for ProxiesSection {
    fn section_id(&self) -> SettingsSectionId {
        SettingsSectionId::Proxies
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        ProxiesSection::handle_key_event(self, event, window, cx);
    }

    fn focus_in(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = true;
        cx.notify();
    }

    fn focus_out(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = false;
        self.proxy_editing_field = false;
        cx.notify();
    }

    fn is_dirty(&self, cx: &App) -> bool {
        self.has_unsaved_proxy_changes(cx)
    }
}

impl Render for ProxiesSection {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.pending_sync_from_app_state {
            self.pending_sync_from_app_state = false;
            self.sync_from_app_state(window, cx);
        }

        let show_proxy_password = self.show_proxy_password;
        self.input_proxy_password.update(cx, |state, cx| {
            state.set_masked(!show_proxy_password, window, cx);
        });

        let theme = cx.theme();
        let (proxies, keyring_available) = {
            let state = self.app_state.read(cx);
            (state.proxies().to_vec(), state.secret_store_available())
        };

        let editing_id = self.editing_proxy_id;
        let show_proxy_delete = self.pending_delete_proxy_id.is_some();
        let show_discard_confirm = self.pending_discard_action.is_some();

        let (proxy_delete_name, proxy_affected_count) = self
            .pending_delete_proxy_id
            .map(|proxy_id| {
                let name = self
                    .app_state
                    .read(cx)
                    .proxies()
                    .iter()
                    .find(|proxy| proxy.id == proxy_id)
                    .map(|proxy| proxy.name.clone())
                    .unwrap_or_default();
                let count = self.profiles_using_proxy(proxy_id, cx);
                (name, count)
            })
            .unwrap_or_default();

        div()
            .size_full()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(layout::section_header(
                "Proxy Profiles",
                "Manage proxy configurations for database connections",
                theme,
            ))
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .flex()
                    .overflow_hidden()
                    .child(self.render_proxy_list(&proxies, editing_id, cx))
                    .child(self.render_proxy_form(editing_id, keyring_available, cx)),
            )
            .when(show_proxy_delete, |element| {
                let entity = cx.entity().clone();
                let entity_cancel = entity.clone();

                let body = if proxy_affected_count > 0 {
                    format!(
                        "Are you sure you want to delete \"{}\"? {} connection{} using this proxy will be updated.",
                        proxy_delete_name,
                        proxy_affected_count,
                        if proxy_affected_count == 1 { "" } else { "s" }
                    )
                } else {
                    format!("Are you sure you want to delete \"{}\"?", proxy_delete_name)
                };

                element.child(
                    Dialog::new(window, cx)
                        .title("Delete Proxy")
                        .confirm()
                        .on_ok(move |_, _, cx| {
                            entity.update(cx, |section, cx| {
                                section.confirm_delete_proxy(cx);
                            });
                            true
                        })
                        .on_cancel(move |_, _, cx| {
                            entity_cancel.update(cx, |section, cx| {
                                section.cancel_delete_proxy(cx);
                            });
                            true
                        })
                        .child(div().text_sm().child(body)),
                )
            })
            .when(show_discard_confirm, |element| {
                let entity = cx.entity().clone();
                let entity_cancel = entity.clone();

                element.child(
                    Dialog::new(window, cx)
                        .title("Discard Proxy Changes")
                        .confirm()
                        .on_ok(move |_, window, cx| {
                            entity.update(cx, |section, cx| {
                                section.confirm_discard_changes(window, cx);
                            });
                            true
                        })
                        .on_cancel(move |_, _, cx| {
                            entity_cancel.update(cx, |section, cx| {
                                section.cancel_discard_changes(cx);
                            });
                            true
                        })
                        .child(
                            div()
                                .text_sm()
                                .child("You have unsaved proxy changes. Discard them?"),
                        ),
                )
            })
    }
}
