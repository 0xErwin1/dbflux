use crate::app::{AppState, AppStateChanged};
use dbflux_core::{SshAuthMethod, SshTunnelConfig, SshTunnelProfile};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::dialog::Dialog;
use gpui_component::input::{Input, InputState};
use gpui_component::{Icon, IconName};
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Clone, Copy, PartialEq)]
enum SettingsSection {
    SshTunnels,
}

#[derive(Clone, Copy, PartialEq)]
enum SshAuthSelection {
    PrivateKey,
    Password,
}

pub struct SettingsWindow {
    app_state: Entity<AppState>,
    active_section: SettingsSection,
    editing_tunnel_id: Option<Uuid>,

    input_tunnel_name: Entity<InputState>,
    input_ssh_host: Entity<InputState>,
    input_ssh_port: Entity<InputState>,
    input_ssh_user: Entity<InputState>,
    input_ssh_key_path: Entity<InputState>,
    input_ssh_key_passphrase: Entity<InputState>,
    input_ssh_password: Entity<InputState>,
    ssh_auth_method: SshAuthSelection,
    form_save_secret: bool,

    pending_ssh_key_path: Option<String>,
    pending_delete_tunnel_id: Option<Uuid>,
    _subscriptions: Vec<Subscription>,
}

impl SettingsWindow {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input_tunnel_name = cx.new(|cx| InputState::new(window, cx).placeholder("Tunnel name"));
        let input_ssh_host = cx.new(|cx| InputState::new(window, cx).placeholder("hostname"));
        let input_ssh_port = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("22")
                .default_value("22")
        });
        let input_ssh_user = cx.new(|cx| InputState::new(window, cx).placeholder("username"));
        let input_ssh_key_path =
            cx.new(|cx| InputState::new(window, cx).placeholder("~/.ssh/id_rsa"));
        let input_ssh_key_passphrase =
            cx.new(|cx| InputState::new(window, cx).placeholder("passphrase"));
        let input_ssh_password = cx.new(|cx| InputState::new(window, cx).placeholder("password"));

        let subscription = cx.subscribe(&app_state, |this, _app_state, _event, cx| {
            this.editing_tunnel_id = None;
            cx.notify();
        });

        Self {
            app_state,
            active_section: SettingsSection::SshTunnels,
            editing_tunnel_id: None,
            input_tunnel_name,
            input_ssh_host,
            input_ssh_port,
            input_ssh_user,
            input_ssh_key_path,
            input_ssh_key_passphrase,
            input_ssh_password,
            ssh_auth_method: SshAuthSelection::PrivateKey,
            form_save_secret: false,
            pending_ssh_key_path: None,
            pending_delete_tunnel_id: None,
            _subscriptions: vec![subscription],
        }
    }

    fn clear_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.editing_tunnel_id = None;
        self.ssh_auth_method = SshAuthSelection::PrivateKey;
        self.form_save_secret = false;

        self.input_tunnel_name
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_host
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_port
            .update(cx, |s, cx| s.set_value("22", window, cx));
        self.input_ssh_user
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_key_path
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_key_passphrase
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_password
            .update(cx, |s, cx| s.set_value("", window, cx));

        cx.notify();
    }

    fn edit_tunnel(
        &mut self,
        tunnel: &SshTunnelProfile,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.editing_tunnel_id = Some(tunnel.id);

        self.input_tunnel_name
            .update(cx, |s, cx| s.set_value(&tunnel.name, window, cx));
        self.input_ssh_host
            .update(cx, |s, cx| s.set_value(&tunnel.config.host, window, cx));
        self.input_ssh_port.update(cx, |s, cx| {
            s.set_value(tunnel.config.port.to_string(), window, cx)
        });
        self.input_ssh_user
            .update(cx, |s, cx| s.set_value(&tunnel.config.user, window, cx));

        match &tunnel.config.auth_method {
            SshAuthMethod::PrivateKey { key_path } => {
                self.ssh_auth_method = SshAuthSelection::PrivateKey;
                let path_str = key_path
                    .as_ref()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_default();
                self.input_ssh_key_path
                    .update(cx, |s, cx| s.set_value(&path_str, window, cx));

                if let Some(secret) = self.app_state.read(cx).get_ssh_tunnel_secret(tunnel) {
                    self.input_ssh_key_passphrase
                        .update(cx, |s, cx| s.set_value(&secret, window, cx));
                }
            }
            SshAuthMethod::Password => {
                self.ssh_auth_method = SshAuthSelection::Password;
                if let Some(secret) = self.app_state.read(cx).get_ssh_tunnel_secret(tunnel) {
                    self.input_ssh_password
                        .update(cx, |s, cx| s.set_value(&secret, window, cx));
                }
            }
        }

        self.form_save_secret = tunnel.save_secret;
        cx.notify();
    }

    fn save_tunnel(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let name = self.input_tunnel_name.read(cx).value().to_string();
        if name.trim().is_empty() {
            return;
        }

        let host = self.input_ssh_host.read(cx).value().to_string();
        let port = self.input_ssh_port.read(cx).value().parse().unwrap_or(22);
        let user = self.input_ssh_user.read(cx).value().to_string();

        let auth_method = match self.ssh_auth_method {
            SshAuthSelection::PrivateKey => {
                let key_path_str = self.input_ssh_key_path.read(cx).value().to_string();
                let key_path = if key_path_str.trim().is_empty() {
                    None
                } else {
                    Some(Self::expand_path(&key_path_str))
                };
                SshAuthMethod::PrivateKey { key_path }
            }
            SshAuthSelection::Password => SshAuthMethod::Password,
        };

        let secret = match self.ssh_auth_method {
            SshAuthSelection::PrivateKey => {
                self.input_ssh_key_passphrase.read(cx).value().to_string()
            }
            SshAuthSelection::Password => self.input_ssh_password.read(cx).value().to_string(),
        };

        let config = SshTunnelConfig {
            host,
            port,
            user,
            auth_method,
        };

        let tunnel = SshTunnelProfile {
            id: self.editing_tunnel_id.unwrap_or_else(Uuid::new_v4),
            name,
            config,
            save_secret: self.form_save_secret,
        };

        let is_edit = self.editing_tunnel_id.is_some();

        self.app_state.update(cx, |state, cx| {
            if tunnel.save_secret && !secret.is_empty() {
                state.save_ssh_tunnel_secret(&tunnel, &secret);
            }

            if is_edit {
                state.update_ssh_tunnel(tunnel);
            } else {
                state.add_ssh_tunnel(tunnel);
            }

            cx.emit(AppStateChanged);
        });

        self.clear_form(window, cx);
    }

    fn request_delete_tunnel(&mut self, tunnel_id: Uuid, cx: &mut Context<Self>) {
        self.pending_delete_tunnel_id = Some(tunnel_id);
        cx.notify();
    }

    fn confirm_delete_tunnel(&mut self, cx: &mut Context<Self>) {
        let Some(tunnel_id) = self.pending_delete_tunnel_id.take() else {
            return;
        };

        self.app_state.update(cx, |state, cx| {
            if let Some(idx) = state.ssh_tunnels.iter().position(|t| t.id == tunnel_id) {
                state.remove_ssh_tunnel(idx);
            }
            cx.emit(AppStateChanged);
        });

        if self.editing_tunnel_id == Some(tunnel_id) {
            self.editing_tunnel_id = None;
        }
        cx.notify();
    }

    fn cancel_delete_tunnel(&mut self, cx: &mut Context<Self>) {
        self.pending_delete_tunnel_id = None;
        cx.notify();
    }

    fn browse_ssh_key(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let this = cx.entity().clone();

        let start_dir = dirs::home_dir().map(|h| h.join(".ssh")).unwrap_or_default();

        let task = cx.background_executor().spawn(async move {
            let dialog = rfd::FileDialog::new()
                .set_title("Select SSH Private Key")
                .set_directory(&start_dir);

            dialog.pick_file()
        });

        cx.spawn(async move |_this, cx| {
            let path = task.await;

            if let Some(path) = path {
                cx.update(|cx| {
                    this.update(cx, |this, cx| {
                        this.pending_ssh_key_path = Some(path.to_string_lossy().to_string());
                        cx.notify();
                    });
                })
                .ok();
            }
        })
        .detach();
    }

    fn expand_path(path: &str) -> PathBuf {
        if let Some(rest) = path.strip_prefix("~/")
            && let Some(home) = dirs::home_dir()
        {
            return home.join(rest);
        }
        PathBuf::from(path)
    }

    fn render_sidebar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let active = self.active_section;

        div()
            .w(px(180.0))
            .h_full()
            .border_r_1()
            .border_color(theme.border)
            .bg(theme.sidebar)
            .flex()
            .flex_col()
            .p_2()
            .gap_1()
            .child(
                div()
                    .id("section-ssh-tunnels")
                    .px_3()
                    .py_2()
                    .rounded(px(4.0))
                    .text_sm()
                    .cursor_pointer()
                    .when(active == SettingsSection::SshTunnels, |d| {
                        d.bg(theme.secondary).font_weight(FontWeight::MEDIUM)
                    })
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.active_section = SettingsSection::SshTunnels;
                        cx.notify();
                    }))
                    .child("SSH Tunnels"),
            )
    }

    fn render_ssh_tunnels_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let tunnels = self.app_state.read(cx).ssh_tunnels.clone();
        let editing_id = self.editing_tunnel_id;
        let keyring_available = self.app_state.read(cx).secret_store_available();

        div()
            .flex_1()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div()
                    .p_4()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("SSH Tunnels"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.muted_foreground)
                            .child("Manage reusable SSH tunnel configurations"),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .flex()
                    .overflow_hidden()
                    .child(self.render_tunnel_list(&tunnels, editing_id, cx))
                    .child(self.render_tunnel_form(editing_id, keyring_available, cx)),
            )
    }

    fn render_tunnel_list(
        &self,
        tunnels: &[SshTunnelProfile],
        editing_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .w(px(250.0))
            .h_full()
            .border_r_1()
            .border_color(theme.border)
            .flex()
            .flex_col()
            .child(
                div().p_2().border_b_1().border_color(theme.border).child(
                    Button::new("new-tunnel")
                        .icon(Icon::new(IconName::Plus))
                        .label("New Tunnel")
                        .small()
                        .w_full()
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.clear_form(window, cx);
                        })),
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
                    .when(tunnels.is_empty(), |d: Div| {
                        d.child(
                            div()
                                .p_4()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child("No saved tunnels"),
                        )
                    })
                    .children(tunnels.iter().map(|tunnel| {
                        let tunnel_id = tunnel.id;
                        let is_selected = editing_id == Some(tunnel_id);
                        let tunnel_clone = tunnel.clone();

                        div()
                            .id(SharedString::from(format!("tunnel-item-{}", tunnel_id)))
                            .px_3()
                            .py_2()
                            .rounded(px(4.0))
                            .cursor_pointer()
                            .when(is_selected, |d| d.bg(theme.secondary))
                            .hover(|d| d.bg(theme.secondary))
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.edit_tunnel(&tunnel_clone, window, cx);
                            }))
                            .child(
                                div()
                                    .flex()
                                    .items_start()
                                    .gap_2()
                                    .child(
                                        Icon::new(IconName::SquareTerminal)
                                            .size(px(14.0))
                                            .text_color(theme.muted_foreground)
                                            .mt(px(2.0)),
                                    )
                                    .child(
                                        div()
                                            .flex()
                                            .flex_col()
                                            .gap_1()
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .font_weight(FontWeight::MEDIUM)
                                                    .child(tunnel.name.clone()),
                                            )
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(theme.muted_foreground)
                                                    .child(format!(
                                                        "{}@{}:{}",
                                                        tunnel.config.user,
                                                        tunnel.config.host,
                                                        tunnel.config.port
                                                    )),
                                            ),
                                    ),
                            )
                    })),
            )
    }

    fn render_tunnel_form(
        &self,
        editing_id: Option<Uuid>,
        keyring_available: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let auth_method = self.ssh_auth_method;
        let save_secret = self.form_save_secret;

        let title = if editing_id.is_some() {
            "Edit Tunnel"
        } else {
            "New Tunnel"
        };

        let auth_selector = self
            .render_auth_selector(auth_method, cx)
            .into_any_element();
        let auth_fields = self
            .render_auth_fields(auth_method, keyring_available, save_secret, cx)
            .into_any_element();

        let theme = cx.theme();

        div()
            .flex_1()
            .h_full()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div().p_4().border_b_1().border_color(theme.border).child(
                    div()
                        .text_base()
                        .font_weight(FontWeight::MEDIUM)
                        .child(title),
                ),
            )
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_4()
                    .child(self.render_form_field("Name", &self.input_tunnel_name, true))
                    .child(
                        div()
                            .flex()
                            .gap_3()
                            .child(div().flex_1().child(self.render_form_field(
                                "Host",
                                &self.input_ssh_host,
                                true,
                            )))
                            .child(div().w(px(80.0)).child(self.render_form_field(
                                "Port",
                                &self.input_ssh_port,
                                false,
                            ))),
                    )
                    .child(self.render_form_field("Username", &self.input_ssh_user, true))
                    .child(auth_selector)
                    .child(auth_fields),
            )
            .child(
                div()
                    .p_4()
                    .border_t_1()
                    .border_color(theme.border)
                    .flex()
                    .gap_2()
                    .justify_end()
                    .when(editing_id.is_some(), |d| {
                        let tunnel_id = editing_id.unwrap();
                        d.child(
                            Button::new("delete-tunnel")
                                .label("Delete")
                                .small()
                                .danger()
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.request_delete_tunnel(tunnel_id, cx);
                                })),
                        )
                        .child(div().flex_1())
                    })
                    .child(
                        Button::new("save-tunnel")
                            .label(if editing_id.is_some() {
                                "Update"
                            } else {
                                "Create"
                            })
                            .small()
                            .primary()
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.save_tunnel(window, cx);
                            })),
                    ),
            )
    }

    fn render_form_field(
        &self,
        label: &str,
        input: &Entity<InputState>,
        _required: bool,
    ) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child(label.to_string()),
            )
            .child(Input::new(input).small())
    }

    fn render_auth_selector(
        &self,
        current: SshAuthSelection,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let primary = theme.primary;
        let border = theme.border;

        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child("Authentication"),
            )
            .child(
                div()
                    .flex()
                    .gap_4()
                    .child(
                        div()
                            .id("auth-key")
                            .flex()
                            .items_center()
                            .gap_2()
                            .cursor_pointer()
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.ssh_auth_method = SshAuthSelection::PrivateKey;
                                cx.notify();
                            }))
                            .child(self.render_radio(
                                current == SshAuthSelection::PrivateKey,
                                primary,
                                border,
                            ))
                            .child(div().text_sm().child("Private Key")),
                    )
                    .child(
                        div()
                            .id("auth-pw")
                            .flex()
                            .items_center()
                            .gap_2()
                            .cursor_pointer()
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.ssh_auth_method = SshAuthSelection::Password;
                                cx.notify();
                            }))
                            .child(self.render_radio(
                                current == SshAuthSelection::Password,
                                primary,
                                border,
                            ))
                            .child(div().text_sm().child("Password")),
                    ),
            )
    }

    fn render_radio(&self, selected: bool, primary: Hsla, border: Hsla) -> impl IntoElement {
        div()
            .w(px(16.0))
            .h(px(16.0))
            .rounded_full()
            .border_2()
            .border_color(if selected { primary } else { border })
            .when(selected, |d| {
                d.child(
                    div()
                        .absolute()
                        .top(px(3.0))
                        .left(px(3.0))
                        .w(px(6.0))
                        .h(px(6.0))
                        .rounded_full()
                        .bg(primary),
                )
            })
    }

    fn render_auth_fields(
        &self,
        auth_method: SshAuthSelection,
        keyring_available: bool,
        save_secret: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        let save_checkbox = if keyring_available {
            Some(
                Checkbox::new("save-secret")
                    .checked(save_secret)
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.form_save_secret = *checked;
                        cx.notify();
                    })),
            )
        } else {
            None
        };

        match auth_method {
            SshAuthSelection::PrivateKey => div()
                .flex()
                .flex_col()
                .gap_3()
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(
                            div()
                                .text_sm()
                                .font_weight(FontWeight::MEDIUM)
                                .child("Private Key Path"),
                        )
                        .child(
                            div()
                                .flex()
                                .gap_2()
                                .child(
                                    div()
                                        .flex_1()
                                        .child(Input::new(&self.input_ssh_key_path).small()),
                                )
                                .child(
                                    Button::new("browse-key")
                                        .label("Browse")
                                        .small()
                                        .ghost()
                                        .on_click(cx.listener(|this, _, window, cx| {
                                            this.browse_ssh_key(window, cx);
                                        })),
                                ),
                        )
                        .child(
                            div()
                                .text_xs()
                                .text_color(theme.muted_foreground)
                                .child("Leave empty to use SSH agent"),
                        ),
                )
                .child(
                    div()
                        .flex()
                        .items_end()
                        .gap_3()
                        .child(div().flex_1().child(self.render_form_field(
                            "Key Passphrase",
                            &self.input_ssh_key_passphrase,
                            false,
                        )))
                        .when_some(save_checkbox, |d, checkbox| {
                            d.child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap_2()
                                    .pb(px(2.0))
                                    .child(checkbox)
                                    .child(div().text_sm().child("Save")),
                            )
                        }),
                )
                .into_any_element(),

            SshAuthSelection::Password => div()
                .flex()
                .flex_col()
                .gap_3()
                .child(
                    div()
                        .flex()
                        .items_end()
                        .gap_3()
                        .child(div().flex_1().child(self.render_form_field(
                            "Password",
                            &self.input_ssh_password,
                            false,
                        )))
                        .when_some(save_checkbox, |d, checkbox| {
                            d.child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap_2()
                                    .pb(px(2.0))
                                    .child(checkbox)
                                    .child(div().text_sm().child("Save")),
                            )
                        }),
                )
                .into_any_element(),
        }
    }
}

pub struct DismissEvent;

impl EventEmitter<DismissEvent> for SettingsWindow {}

impl Render for SettingsWindow {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(path) = self.pending_ssh_key_path.take() {
            self.input_ssh_key_path.update(cx, |state, cx| {
                state.set_value(path, window, cx);
            });
        }

        let theme = cx.theme();
        let show_delete_confirm = self.pending_delete_tunnel_id.is_some();

        let tunnel_name = self
            .pending_delete_tunnel_id
            .and_then(|id| {
                self.app_state
                    .read(cx)
                    .ssh_tunnels
                    .iter()
                    .find(|t| t.id == id)
                    .map(|t| t.name.clone())
            })
            .unwrap_or_default();

        div()
            .size_full()
            .bg(theme.background)
            .flex()
            .child(self.render_sidebar(cx))
            .child(match self.active_section {
                SettingsSection::SshTunnels => {
                    self.render_ssh_tunnels_section(cx).into_any_element()
                }
            })
            .when(show_delete_confirm, |el| {
                let this = cx.entity().clone();
                let this_cancel = this.clone();

                el.child(
                    Dialog::new(window, cx)
                        .title("Delete SSH Tunnel")
                        .confirm()
                        .on_ok(move |_, _, cx| {
                            this.update(cx, |settings, cx| {
                                settings.confirm_delete_tunnel(cx);
                            });
                            true
                        })
                        .on_cancel(move |_, _, cx| {
                            this_cancel.update(cx, |settings, cx| {
                                settings.cancel_delete_tunnel(cx);
                            });
                            true
                        })
                        .child(div().text_sm().child(format!(
                            "Are you sure you want to delete \"{}\"?",
                            tunnel_name
                        ))),
                )
            })
    }
}
