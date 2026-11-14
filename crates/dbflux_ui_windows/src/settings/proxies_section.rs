use super::SettingsSection;
use super::SettingsSectionId;
use super::form_section::FormSection;
use super::layout;
use super::proxies::ProxyFormNav;
use super::section_trait::{SectionFocusEvent, SectionPortabilityEvent};
use crate::connection_manager::ExportTarget;
use crate::labels::proxies_delete_body;
use crate::tokens::{FormMetrics, SettingsMetrics};
use dbflux_components::controls::{Button, Checkbox, Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::{ConfirmOutcome, ConfirmRequest, ModalConfirm, ModalVariant};
use dbflux_components::primitives::{Badge, BadgeTone, SegmentedControl, SegmentedItem};
use dbflux_core::{ProxyKind, ProxyProfile};
use dbflux_ui_base::{AppStateChanged, AppStateEntity};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
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
    ExportButton,
    SaveButton,
    DeleteButton,
}

#[derive(Clone, Copy, Debug, PartialEq)]
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
    /// Confirmation of the proxy delete, opened by `render` while a delete
    /// is pending.
    delete_confirm: Entity<ModalConfirm>,
    /// Confirmation of discarding unsaved proxy changes, opened by `render`
    /// while a discard is pending.
    discard_confirm: Entity<ModalConfirm>,
    /// Outcomes of the two confirmations, drained in `render` where a
    /// `Window` is available (the outcome subscriptions recorded them
    /// without one).
    pending_delete_confirm_resolution: Option<ConfirmOutcome>,
    pending_discard_confirm_resolution: Option<ConfirmOutcome>,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<SectionFocusEvent> for ProxiesSection {}
impl EventEmitter<SectionPortabilityEvent> for ProxiesSection {}

impl ProxiesSection {
    /// Ask the coordinator to export the proxy currently loaded in the form.
    pub(super) fn request_export(&mut self, cx: &mut Context<Self>) {
        if let Some(id) = self.editing_proxy_id {
            cx.emit(SectionPortabilityEvent::OpenExport(ExportTarget::Proxy(id)));
        }
    }

    /// Ask the coordinator to open the import wizard.
    pub(super) fn request_import(&mut self, cx: &mut Context<Self>) {
        cx.emit(SectionPortabilityEvent::OpenImport);
    }

    pub(super) fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let input_proxy_name = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("settings.proxies.placeholder_name"))
        });
        let input_proxy_host =
            cx.new(|cx| InputState::new(window, cx).placeholder("proxy.example.com"));
        let input_proxy_port = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("8080")
                .default_value("8080")
        });
        let input_proxy_username = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("settings.proxies.placeholder_username"))
        });
        let input_proxy_password = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("settings.proxies.placeholder_password"))
                .masked(true)
        });
        let input_proxy_no_proxy =
            cx.new(|cx| InputState::new(window, cx).placeholder("localhost, 127.0.0.1, .internal"));

        let delete_confirm = cx.new(|cx| ModalConfirm::new(cx));
        let discard_confirm = cx.new(|cx| ModalConfirm::new(cx));
        let delete_confirm_subscription =
            cx.subscribe(&delete_confirm, |this, _, outcome: &ConfirmOutcome, cx| {
                this.pending_delete_confirm_resolution = Some(*outcome);
                cx.notify();
            });
        let discard_confirm_subscription =
            cx.subscribe(&discard_confirm, |this, _, outcome: &ConfirmOutcome, cx| {
                this.pending_discard_confirm_resolution = Some(*outcome);
                cx.notify();
            });

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
            delete_confirm,
            discard_confirm,
            pending_delete_confirm_resolution: None,
            pending_discard_confirm_resolution: None,
            _subscriptions: vec![
                subscription,
                delete_confirm_subscription,
                discard_confirm_subscription,
                blur_name,
                blur_host,
                blur_port,
                blur_username,
                blur_password,
                blur_no_proxy,
            ],
        }
    }

    fn is_cursor_on(&self, field: ProxyFormField) -> bool {
        self.content_focused
            && self.proxy_focus == ProxyFocus::Form
            && self.proxy_form_field == field
            && !self.proxy_editing_field
    }

    /// Text field of the detail form, framed for the keyboard cursor.
    #[allow(clippy::too_many_arguments)]
    fn render_proxy_input(
        &self,
        input: &Entity<InputState>,
        field: ProxyFormField,
        label: String,
        width: Option<Rems>,
        mono: bool,
        suffix: Option<AnyElement>,
        cx: &mut Context<Self>,
    ) -> Div {
        let element_id = format!("proxy-{}", proxy_field_id(field));

        let mut control = Input::new(input)
            .id(SharedString::from(element_id))
            .aria_label(label)
            .secret(field == ProxyFormField::Password);

        if let Some(suffix) = suffix {
            control = control.suffix(suffix);
        }

        layout::field_frame(self.is_cursor_on(field), width, mono, control, cx).on_mouse_down(
            MouseButton::Left,
            cx.listener(move |this, _, window, cx| {
                this.switching_input = true;
                this.proxy_focus = ProxyFocus::Form;
                this.proxy_form_field = field;
                this.proxy_focus_current_field(window, cx);
                cx.notify();
            }),
        )
    }

    fn render_proxy_kind_selector(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let entity = cx.entity();
        let kinds = [
            (ProxyFormField::KindHttp, ProxyKind::Http, "http"),
            (ProxyFormField::KindHttps, ProxyKind::Https, "https"),
            (ProxyFormField::KindSocks5, ProxyKind::Socks5, "socks5"),
        ];

        let items = vec![
            SegmentedItem::new("http", dbflux_i18n::t!("settings.proxies.kind.http")),
            SegmentedItem::new("https", dbflux_i18n::t!("settings.proxies.kind.https")),
            SegmentedItem::new("socks5", dbflux_i18n::t!("settings.proxies.kind.socks5")),
        ];

        let active = kinds
            .iter()
            .find(|(_, kind, _)| *kind == self.proxy_kind)
            .map(|(_, _, id)| *id)
            .unwrap_or("http");

        let control = SegmentedControl::new(items, active, move |selected, window, cx| {
            let Some((field, kind, _)) = kinds
                .iter()
                .find(|(_, _, id)| *id == selected.as_ref())
                .copied()
            else {
                return;
            };

            entity.update(cx, |this, cx| {
                this.proxy_focus = ProxyFocus::Form;
                this.proxy_form_field = field;
                this.proxy_kind = kind;
                this.input_proxy_port.update(cx, |state, cx| {
                    state.set_value(kind.default_port().to_string(), window, cx);
                });
                this.validate_proxy_form_field();
                cx.notify();
            });
        });

        let cursor_item = kinds
            .iter()
            .find(|(field, _, _)| self.is_cursor_on(*field))
            .map(|(_, _, id)| *id);

        let control = control
            .focused(cursor_item.is_some())
            .when_some(cursor_item, |control, id| control.focused_item(id));

        layout::form_row(
            dbflux_i18n::t!("settings.proxies.field.protocol"),
            div().flex().child(control),
            None,
        )
    }

    fn render_proxy_auth_selector(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let entity = cx.entity();
        let active = match self.proxy_auth_selection {
            ProxyAuthSelection::None => "none",
            ProxyAuthSelection::Basic => "basic",
        };

        let control = SegmentedControl::new(
            vec![
                SegmentedItem::new("none", dbflux_i18n::t!("settings.proxies.auth.none")),
                SegmentedItem::new("basic", dbflux_i18n::t!("settings.proxies.auth.basic"))
                    .icon(AppIcon::Lock),
            ],
            active,
            move |selected, _window, cx| {
                let (selection, field) = if selected.as_ref() == "basic" {
                    (ProxyAuthSelection::Basic, ProxyFormField::AuthBasic)
                } else {
                    (ProxyAuthSelection::None, ProxyFormField::AuthNone)
                };

                entity.update(cx, |this, cx| {
                    this.proxy_focus = ProxyFocus::Form;
                    this.proxy_form_field = field;
                    this.proxy_auth_selection = selection;
                    this.validate_proxy_form_field();
                    cx.notify();
                });
            },
        );

        let cursor_item = if self.is_cursor_on(ProxyFormField::AuthNone) {
            Some("none")
        } else if self.is_cursor_on(ProxyFormField::AuthBasic) {
            Some("basic")
        } else {
            None
        };

        let control = control
            .focused(cursor_item.is_some())
            .when_some(cursor_item, |control, id| control.focused_item(id));

        layout::form_row(
            dbflux_i18n::t!("settings.proxies.field.authentication"),
            div().flex().child(control),
            None,
        )
    }

    fn render_proxy_auth_fields(
        &self,
        keyring_available: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let (toggle_icon, toggle_label) = if self.show_proxy_password {
            (
                AppIcon::EyeOff,
                dbflux_i18n::t!("settings.field.hide_secret"),
            )
        } else {
            (AppIcon::Eye, dbflux_i18n::t!("settings.field.show_secret"))
        };

        let password_toggle = Button::new("toggle-proxy-password", toggle_label)
            .ghost()
            .inline()
            .icon(toggle_icon)
            .icon_only()
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| {
                this.show_proxy_password = !this.show_proxy_password;
                cx.notify();
            }))
            .into_any_element();

        let save_checkbox = keyring_available.then(|| {
            layout::cursor_ring(
                self.is_cursor_on(ProxyFormField::SaveSecret),
                Checkbox::new("proxy-save-secret")
                    .checked(self.proxy_save_secret)
                    .label(dbflux_i18n::t!(
                        "settings.ssh_tunnels.field.keep_in_keyring"
                    ))
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.proxy_focus = ProxyFocus::Form;
                        this.proxy_form_field = ProxyFormField::SaveSecret;
                        this.proxy_save_secret = *checked;
                        cx.notify();
                    })),
                cx,
            )
        });

        let password = layout::inline_controls()
            .gap(FormMetrics::ROW_GAP)
            .child(self.render_proxy_input(
                &self.input_proxy_password,
                ProxyFormField::Password,
                dbflux_i18n::t!("settings.proxies.field.password"),
                Some(SettingsMetrics::SELECT_WIDTH),
                false,
                Some(password_toggle),
                cx,
            ))
            .children(save_checkbox);

        div()
            .flex()
            .flex_col()
            .child(layout::form_row(
                dbflux_i18n::t!("settings.proxies.field.username"),
                self.render_proxy_input(
                    &self.input_proxy_username,
                    ProxyFormField::Username,
                    dbflux_i18n::t!("settings.proxies.field.username"),
                    Some(SettingsMetrics::TEXT_FIELD_WIDTH),
                    true,
                    None,
                    cx,
                ),
                None,
            ))
            .child(layout::form_row(
                dbflux_i18n::t!("settings.proxies.field.password"),
                password,
                None,
            ))
    }

    fn render_proxy_list(
        &self,
        proxies: &[ProxyProfile],
        editing_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement + use<> {
        let is_list_focused = self.content_focused && self.proxy_focus == ProxyFocus::ProfileList;
        let is_new_button_focused = is_list_focused && self.proxy_selected_idx.is_none();

        let toolbar = layout::master_list_toolbar(vec![
            Button::new("new-proxy", dbflux_i18n::t!("settings.proxies.new"))
                .primary()
                .icon(AppIcon::Plus)
                .focused(is_new_button_focused)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.request_proxy_action(PendingProxyAction::ClearForm, window, cx);
                }))
                .into_any_element(),
            Button::new(
                "import-proxy",
                dbflux_i18n::t!("settings.proxies.action.import"),
            )
            .secondary()
            .icon(AppIcon::Download)
            .on_click(cx.listener(|this, _, _, cx| {
                this.request_import(cx);
            }))
            .into_any_element(),
        ]);

        let rows: Vec<AnyElement> = proxies
            .iter()
            .enumerate()
            .map(|(idx, proxy)| {
                let proxy_id = proxy.id;
                let is_selected = editing_id == Some(proxy_id);
                let is_focused = is_list_focused && self.proxy_selected_idx == Some(idx);
                let detail = format!("{}://{}:{}", proxy.kind.scheme(), proxy.host, proxy.port);

                let trailing = (!proxy.enabled).then(|| {
                    Badge::new(
                        dbflux_i18n::t!("settings.proxies.status.disabled_caption"),
                        BadgeTone::Neutral,
                    )
                    .into_any_element()
                });

                layout::master_list_row(
                    SharedString::from(format!("proxy-item-{}", proxy_id)),
                    layout::MasterRow {
                        icon: Some(AppIcon::Globe),
                        title: proxy.name.clone().into(),
                        detail: Some(detail.into()),
                        trailing,
                    },
                    is_selected,
                    is_focused,
                    cx,
                )
                .on_click(cx.listener(move |this, _, window, cx| {
                    this.request_proxy_action(PendingProxyAction::EditIndex(idx), window, cx);
                    this.proxy_focus = ProxyFocus::Form;
                    this.proxy_form_field = ProxyFormField::Name;
                }))
                .into_any_element()
            })
            .collect();

        let body = if rows.is_empty() {
            layout::master_list_empty(dbflux_i18n::t!("settings.proxies.empty")).into_any_element()
        } else {
            div().flex().flex_col().children(rows).into_any_element()
        };

        layout::master_list_panel("proxy-list", toolbar, body, cx)
    }

    fn render_proxy_form(
        &self,
        keyring_available: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();

        let host_port = layout::inline_controls()
            .child(self.render_proxy_input(
                &self.input_proxy_host,
                ProxyFormField::Host,
                dbflux_i18n::t!("settings.proxies.field.host"),
                None,
                true,
                None,
                cx,
            ))
            .child(self.render_proxy_input(
                &self.input_proxy_port,
                ProxyFormField::Port,
                dbflux_i18n::t!("settings.proxies.field.port"),
                Some(SettingsMetrics::PORT_FIELD_WIDTH),
                true,
                None,
                cx,
            ));

        let enabled = layout::cursor_ring(
            self.is_cursor_on(ProxyFormField::Enabled),
            Checkbox::new("proxy-enabled")
                .checked(self.proxy_enabled)
                .label(dbflux_i18n::t!("settings.proxies.field.enabled"))
                .on_click(cx.listener(|this, checked: &bool, _, cx| {
                    this.proxy_focus = ProxyFocus::Form;
                    this.proxy_form_field = ProxyFormField::Enabled;
                    this.proxy_enabled = *checked;
                    cx.notify();
                })),
            cx,
        );

        layout::sticky_form_shell(
            dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.proxies.group.proxy"),
                Some(AppIcon::Globe.into()),
                cx,
            ),
            div()
                .flex()
                .flex_col()
                .child(layout::form_row(
                    dbflux_i18n::t!("settings.proxies.field.name"),
                    self.render_proxy_input(
                        &self.input_proxy_name,
                        ProxyFormField::Name,
                        dbflux_i18n::t!("settings.proxies.field.name"),
                        Some(SettingsMetrics::TEXT_FIELD_WIDTH),
                        false,
                        None,
                        cx,
                    ),
                    None,
                ))
                .child(self.render_proxy_kind_selector(cx))
                .child(layout::form_row(
                    dbflux_i18n::t!("settings.proxies.field.host_port"),
                    host_port,
                    None,
                ))
                .child(layout::form_row(
                    dbflux_i18n::t!("settings.proxies.field.no_proxy"),
                    self.render_proxy_input(
                        &self.input_proxy_no_proxy,
                        ProxyFormField::NoProxy,
                        dbflux_i18n::t!("settings.proxies.field.no_proxy"),
                        None,
                        true,
                        None,
                        cx,
                    ),
                    Some(dbflux_i18n::t!("settings.proxies.hint.no_proxy_list").into()),
                ))
                .child(layout::check_row(enabled, None))
                .child(dbflux_components::composites::section_header(
                    dbflux_i18n::t!("settings.proxies.group.authentication"),
                    Some(AppIcon::KeyRound.into()),
                    cx,
                ))
                .child(self.render_proxy_auth_selector(cx))
                .when(
                    self.proxy_auth_selection == ProxyAuthSelection::Basic,
                    |form| form.child(self.render_proxy_auth_fields(keyring_available, cx)),
                ),
            None,
            &theme,
        )
    }

    /// Export and Delete, on the left of the footer, for a saved proxy.
    fn render_section_footer_leading_actions(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        let proxy_id = self.editing_proxy_id?;

        Some(
            layout::inline_controls()
                .child(
                    Button::new(
                        "export-proxy",
                        dbflux_i18n::t!("settings.proxies.action.export"),
                    )
                    .secondary()
                    .icon(AppIcon::ExternalLink)
                    .focused(self.is_cursor_on(ProxyFormField::ExportButton))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.request_export(cx);
                    })),
                )
                .child(
                    Button::new(
                        "delete-proxy",
                        dbflux_i18n::t!("settings.proxies.action.delete"),
                    )
                    .danger()
                    .icon(AppIcon::Delete)
                    .focused(self.is_cursor_on(ProxyFormField::DeleteButton))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.request_delete_proxy(proxy_id, cx);
                    })),
                )
                .into_any_element(),
        )
    }

    fn render_section_footer_actions(
        &self,
        editing_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        Button::new(
            "save-proxy",
            if editing_id.is_some() {
                dbflux_i18n::t!("settings.proxies.action.update")
            } else {
                dbflux_i18n::t!("settings.proxies.action.create")
            },
        )
        .primary()
        .icon(AppIcon::Check)
        .when_some(crate::settings::save_shortcut(), Button::kbd)
        .focused(self.is_cursor_on(ProxyFormField::SaveButton))
        .on_click(cx.listener(|this, _, window, cx| {
            this.save_proxy(window, cx);
        }))
        .into_any_element()
    }
}

/// Element-id suffix of a detail form field.
fn proxy_field_id(field: ProxyFormField) -> &'static str {
    match field {
        ProxyFormField::Name => "name",
        ProxyFormField::Host => "host",
        ProxyFormField::Port => "port",
        ProxyFormField::Username => "username",
        ProxyFormField::Password => "password",
        ProxyFormField::NoProxy => "no-proxy",
        _ => "control",
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

    fn render_footer_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        Some(self.render_section_footer_actions(self.editing_proxy_id, cx))
    }

    fn render_footer_leading_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        self.render_section_footer_leading_actions(cx)
    }

    fn save_from_shortcut(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.save_proxy(window, cx);
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

        let (proxies, keyring_available) = {
            let state = self.app_state.read(cx);
            (state.proxies().to_vec(), state.secret_store_available())
        };

        let editing_id = self.editing_proxy_id;

        // A confirmation resolved while it was open; run the outcome here
        // where a `Window` is available (the outcome subscription recorded it
        // without one).
        if let Some(outcome) = self.pending_delete_confirm_resolution.take() {
            match outcome {
                ConfirmOutcome::Confirmed => self.confirm_delete_proxy(cx),
                ConfirmOutcome::Cancelled => self.cancel_delete_proxy(cx),
            }
        }
        if let Some(outcome) = self.pending_discard_confirm_resolution.take() {
            match outcome {
                ConfirmOutcome::Confirmed => self.confirm_discard_changes(window, cx),
                ConfirmOutcome::Cancelled => self.cancel_discard_changes(cx),
            }
        }

        // Open the delete confirmation while a delete is pending; opening
        // only when the request changed keeps later renders from stealing
        // focus back.
        if let Some(proxy_id) = self.pending_delete_proxy_id {
            let (name, affected_count) = {
                let state = self.app_state.read(cx);
                let name = state
                    .proxies()
                    .iter()
                    .find(|proxy| proxy.id == proxy_id)
                    .map(|proxy| proxy.name.clone())
                    .unwrap_or_default();
                let count = self.profiles_using_proxy(proxy_id, cx);
                (name, count)
            };
            let body = proxies_delete_body(&name, affected_count);
            let needs_open = !self.delete_confirm.read(cx).is_visible()
                || self.delete_confirm.read(cx).current_body() != Some(body.as_str());
            if needs_open {
                self.delete_confirm.update(cx, |confirm, cx| {
                    confirm.open(
                        ConfirmRequest {
                            title: dbflux_i18n::t!("settings.proxies.delete_dialog_title"),
                            body,
                            variant: ModalVariant::Danger,
                            confirm_label: dbflux_i18n::t!("settings.proxies.action.delete"),
                        },
                        cx,
                    );
                });
            }
        }

        // Open the discard confirmation while a discard is pending, with the
        // same changed-request guard.
        if self.pending_discard_action.is_some() {
            let body = dbflux_i18n::t!("settings.proxies.discard_dialog.body");
            let needs_open = !self.discard_confirm.read(cx).is_visible()
                || self.discard_confirm.read(cx).current_body() != Some(body.as_str());
            if needs_open {
                self.discard_confirm.update(cx, |confirm, cx| {
                    confirm.open(
                        ConfirmRequest {
                            title: dbflux_i18n::t!("settings.proxies.discard_dialog_title"),
                            body,
                            variant: ModalVariant::Default,
                            confirm_label: dbflux_i18n::t!("settings.discard.confirm"),
                        },
                        cx,
                    );
                });
            }
        }

        layout::split_section_shell(
            cx.theme().border,
            dbflux_components::composites::page_header(
                dbflux_i18n::t!("settings.proxies.section_title"),
                dbflux_i18n::t!("settings.proxies.section_description"),
                cx,
            ),
            self.render_proxy_list(&proxies, editing_id, cx),
            self.render_proxy_form(keyring_available, cx),
        )
        .child(self.delete_confirm.clone())
        .child(self.discard_confirm.clone())
    }
}

#[cfg(test)]
mod tests {
    const PROXIES_KEYS: &[&str] = &[
        "settings.proxies.placeholder_name",
        "settings.proxies.placeholder_username",
        "settings.proxies.placeholder_password",
        "settings.proxies.section_title",
        "settings.proxies.section_description",
        "settings.proxies.empty",
        "settings.proxies.new",
        "settings.proxies.panel_title",
        "settings.proxies.kind.http",
        "settings.proxies.kind.https",
        "settings.proxies.kind.socks5",
        "settings.proxies.field.name",
        "settings.proxies.field.protocol",
        "settings.proxies.field.authentication",
        "settings.proxies.field.username",
        "settings.proxies.field.password",
        "settings.proxies.field.host",
        "settings.proxies.field.port",
        "settings.proxies.field.no_proxy",
        "settings.proxies.field.enabled",
        "settings.proxies.hint.no_proxy_list",
        "settings.proxies.auth.none",
        "settings.proxies.auth.basic",
        "settings.proxies.status.disabled_caption",
        "settings.proxies.action.save",
        "settings.proxies.action.import",
        "settings.proxies.action.export",
        "settings.proxies.action.delete",
        "settings.proxies.action.update",
        "settings.proxies.action.create",
        "settings.proxies.delete_dialog_title",
        "settings.proxies.delete_dialog.body_none",
        "settings.proxies.delete_dialog.body_one",
        "settings.proxies.delete_dialog.body_many",
        "settings.proxies.discard_dialog_title",
        "settings.proxies.discard_dialog.body",
    ];

    #[test]
    fn proxies_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in PROXIES_KEYS {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(
                    !value.is_empty(),
                    "key {key} resolved empty for locale {locale}"
                );
                assert_ne!(value, *key, "key {key} did not resolve for locale {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "key {key} fell back to the raw locale-qualified form for locale {locale}"
                );
            }
        }
    }

    #[test]
    fn proxies_section_title_differs_between_locales() {
        let english = dbflux_i18n::t!("settings.proxies.section_title", locale = "en");
        let spanish = dbflux_i18n::t!("settings.proxies.section_title", locale = "es");

        assert_eq!(english, "Proxies");
        assert_eq!(spanish, "Perfiles de proxy");
        assert_ne!(english, spanish);
    }

    #[test]
    fn proxies_kind_labels_stay_untranslated_protocol_names() {
        for locale in ["en", "es"] {
            assert_eq!(
                dbflux_i18n::t!("settings.proxies.kind.http", locale = locale),
                "HTTP"
            );
            assert_eq!(
                dbflux_i18n::t!("settings.proxies.kind.https", locale = locale),
                "HTTPS"
            );
            assert_eq!(
                dbflux_i18n::t!("settings.proxies.kind.socks5", locale = locale),
                "SOCKS5"
            );
        }
    }

    #[test]
    fn proxies_auth_labels_differ_between_locales() {
        let none_en = dbflux_i18n::t!("settings.proxies.auth.none", locale = "en");
        let none_es = dbflux_i18n::t!("settings.proxies.auth.none", locale = "es");
        let basic_en = dbflux_i18n::t!("settings.proxies.auth.basic", locale = "en");
        let basic_es = dbflux_i18n::t!("settings.proxies.auth.basic", locale = "es");

        assert_eq!(none_en, "None");
        assert_eq!(none_es, "Ninguna");
        assert_eq!(basic_en, "Basic");
        assert_eq!(basic_es, "Básica");
        assert_ne!(none_en, none_es);
        assert_ne!(basic_en, basic_es);
    }
}

#[cfg(test)]
mod confirm_tests {
    // Explicit imports: a parent glob would make `#[test]` resolve to
    // `gpui::test`, whose expansion recurses without bound.
    use super::{PendingProxyAction, ProxiesSection};
    use dbflux_components::modals::confirm::{CONFIRM_CANCEL_ID, CONFIRM_CONFIRM_ID};
    use dbflux_core::{ProxyKind, ProxyProfile};
    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_ui_base::AppStateEntity;
    use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
    use gpui::{
        AccessibilityFrame, AppContext as _, Bounds, Entity, FrameObserver, ParentElement as _,
        Pixels, TestAppContext, VisualTestContext,
    };
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    /// Keeps the latest rendered frame of the window it observes.
    #[derive(Default)]
    struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

    impl FrameObserver for FrameCapture {
        fn accessibility_updated(&self, frame: &AccessibilityFrame) {
            *self.0.lock().expect("frame capture lock") = Some(frame.clone());
        }
    }

    impl FrameCapture {
        fn bounds_of(&self, id: &str) -> Option<Bounds<Pixels>> {
            let frame = self.0.lock().expect("frame capture lock").clone()?;
            frame
                .nodes()
                .find(|(_, node)| node.id() == id)
                .map(|(_, node)| node.bounds())
        }
    }

    struct Harness {
        app_state: Entity<AppStateEntity>,
        section: Entity<ProxiesSection>,
        capture: Arc<FrameCapture>,
    }

    struct SectionView {
        section: Entity<ProxiesSection>,
    }

    impl gpui::Render for SectionView {
        fn render(
            &mut self,
            _window: &mut gpui::Window,
            _cx: &mut gpui::Context<Self>,
        ) -> impl gpui::IntoElement {
            gpui::div().child(self.section.clone())
        }
    }

    fn open<'a>(cx: &'a mut TestAppContext) -> (Harness, &'a mut VisualTestContext) {
        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);
        cx.update(|cx| {
            let host = cx.new(|_| ToastHost::new());
            cx.set_global(ToastGlobal { host });
        });

        let app_state: Entity<AppStateEntity> = cx.update(|cx| {
            cx.new(|_| {
                let runtime = StorageRuntime::in_memory().expect("in-memory storage");
                AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
            })
        });

        let capture = Arc::new(FrameCapture::default());
        let slot = Rc::new(RefCell::new(None));
        let capture_for_view = capture.clone();
        let app_for_view = app_state.clone();
        let slot_for_view = slot.clone();
        let (_view, window) = cx.add_window_view(move |window, cx| {
            window.observe_frames(&capture_for_view);
            let section = cx.new(|cx| ProxiesSection::new(app_for_view.clone(), window, cx));
            slot_for_view.borrow_mut().replace(section.clone());
            SectionView { section }
        });
        window.run_until_parked();

        let section = slot.borrow().clone().expect("section built");
        (
            Harness {
                app_state,
                section,
                capture,
            },
            window,
        )
    }

    /// Adds one proxy to the saved state and returns its id.
    fn add_proxy(harness: &Harness, window: &mut VisualTestContext) -> ProxyProfile {
        let proxy = ProxyProfile::new("corp", ProxyKind::Http);
        window.update(|_, cx| {
            harness
                .app_state
                .update(cx, |state, _| state.add_proxy(proxy.clone()));
        });
        proxy
    }

    #[gpui::test]
    fn the_proxy_delete_confirmation_cancels_and_confirms(cx: &mut TestAppContext) {
        let (harness, window) = open(cx);
        let proxy = add_proxy(&harness, window);

        window.update(|_, cx| {
            harness
                .section
                .update(cx, |section, cx| section.request_delete_proxy(proxy.id, cx));
        });
        window.run_until_parked();
        assert!(
            window.update(|_, cx| {
                harness
                    .section
                    .read(cx)
                    .delete_confirm
                    .read(cx)
                    .is_visible()
            }),
            "a pending delete opens the confirmation"
        );
        let cancel = harness
            .capture
            .bounds_of(CONFIRM_CANCEL_ID)
            .expect("the confirmation renders a cancel control");
        let confirm = harness
            .capture
            .bounds_of(CONFIRM_CONFIRM_ID)
            .expect("the confirmation renders a confirm control");
        assert_ne!(cancel, confirm, "the two controls are distinct elements");

        // Cancelling keeps the proxy and clears the pending delete.
        window.update(|_, cx| {
            harness.section.update(cx, |section, cx| {
                section
                    .delete_confirm
                    .update(cx, |modal, cx| modal.cancel(cx));
            });
        });
        window.run_until_parked();
        window.update(|_, cx| {
            let section = harness.section.read(cx);
            assert!(
                !section.delete_confirm.read(cx).is_visible(),
                "cancelling closes the confirmation"
            );
            assert_eq!(
                section.pending_delete_proxy_id, None,
                "cancelling clears the pending delete"
            );
            assert_eq!(
                harness.app_state.read(cx).proxies().len(),
                1,
                "cancelling keeps the proxy"
            );
        });

        // Re-opening and confirming deletes the proxy.
        window.update(|_, cx| {
            harness
                .section
                .update(cx, |section, cx| section.request_delete_proxy(proxy.id, cx));
        });
        window.run_until_parked();
        window.update(|_, cx| {
            harness.section.update(cx, |section, cx| {
                section
                    .delete_confirm
                    .update(cx, |modal, cx| modal.confirm(cx));
            });
        });
        window.run_until_parked();
        window.update(|_, cx| {
            let section = harness.section.read(cx);
            assert!(
                !section.delete_confirm.read(cx).is_visible(),
                "confirming closes the confirmation"
            );
            assert!(
                harness.app_state.read(cx).proxies().is_empty(),
                "confirming deletes the proxy"
            );
        });
    }

    #[gpui::test]
    fn the_proxy_discard_confirmation_cancels_and_confirms(cx: &mut TestAppContext) {
        let (harness, window) = open(cx);
        add_proxy(&harness, window);

        // Stage a pending switch to the saved proxy, as the dirty form does.
        window.update(|_, cx| {
            harness.section.update(cx, |section, cx| {
                section.pending_discard_action = Some(PendingProxyAction::EditIndex(0));
                cx.notify();
            });
        });
        window.run_until_parked();
        assert!(
            window.update(|_, cx| {
                harness
                    .section
                    .read(cx)
                    .discard_confirm
                    .read(cx)
                    .is_visible()
            }),
            "a pending discard opens the confirmation"
        );
        let cancel = harness
            .capture
            .bounds_of(CONFIRM_CANCEL_ID)
            .expect("the confirmation renders a cancel control");
        let confirm = harness
            .capture
            .bounds_of(CONFIRM_CONFIRM_ID)
            .expect("the confirmation renders a confirm control");
        assert_ne!(cancel, confirm, "the two controls are distinct elements");

        // Cancelling keeps the unsaved state and clears the pending action.
        window.update(|_, cx| {
            harness.section.update(cx, |section, cx| {
                section
                    .discard_confirm
                    .update(cx, |modal, cx| modal.cancel(cx));
            });
        });
        window.run_until_parked();
        window.update(|_, cx| {
            let section = harness.section.read(cx);
            assert!(
                !section.discard_confirm.read(cx).is_visible(),
                "cancelling closes the confirmation"
            );
            assert_eq!(
                section.pending_discard_action, None,
                "cancelling clears the pending discard"
            );
            assert_eq!(
                section.editing_proxy_id, None,
                "cancelling keeps the form untouched"
            );
        });

        // Re-opening and confirming applies the staged action.
        window.update(|_, cx| {
            harness.section.update(cx, |section, cx| {
                section.pending_discard_action = Some(PendingProxyAction::EditIndex(0));
                cx.notify();
            });
        });
        window.run_until_parked();
        window.update(|_, cx| {
            harness.section.update(cx, |section, cx| {
                section
                    .discard_confirm
                    .update(cx, |modal, cx| modal.confirm(cx));
            });
        });
        window.run_until_parked();
        window.update(|_, cx| {
            let section = harness.section.read(cx);
            assert!(
                !section.discard_confirm.read(cx).is_visible(),
                "confirming closes the confirmation"
            );
            assert_eq!(
                section.pending_discard_action, None,
                "confirming clears the pending discard"
            );
            assert!(
                section.editing_proxy_id.is_some(),
                "confirming applies the staged action"
            );
        });
    }
}
