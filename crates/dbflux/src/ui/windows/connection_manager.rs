use crate::app::AppState;
use dbflux_core::{ConnectionProfile, DbConfig, DbDriver, DbKind, SshAuthMethod, SshTunnelConfig};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::input::{Input, InputState};
use gpui_component::list::ListItem;
use gpui_component::ActiveTheme;
use gpui_component::Disableable;
use log::info;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Copy, PartialEq)]
enum View {
    DriverSelect,
    EditForm,
}

#[derive(Clone, Copy, PartialEq)]
enum FormTab {
    Main,
    Ssh,
}

#[derive(Clone, Copy, PartialEq)]
enum TestStatus {
    None,
    Testing,
    Success,
    Failed,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum SshAuthSelection {
    PrivateKey,
    Password,
}

#[derive(Clone)]
struct DriverInfo {
    kind: DbKind,
    name: String,
    description: String,
}

pub struct ConnectionManagerWindow {
    app_state: Entity<AppState>,
    view: View,
    active_tab: FormTab,
    available_drivers: Vec<DriverInfo>,
    selected_driver: Option<Arc<dyn DbDriver>>,
    form_save_password: bool,
    form_save_ssh_secret: bool,
    editing_profile_id: Option<uuid::Uuid>,

    input_name: Entity<InputState>,
    input_host: Entity<InputState>,
    input_port: Entity<InputState>,
    input_user: Entity<InputState>,
    input_password: Entity<InputState>,
    input_database: Entity<InputState>,
    input_path: Entity<InputState>,

    ssh_enabled: bool,
    ssh_auth_method: SshAuthSelection,
    input_ssh_host: Entity<InputState>,
    input_ssh_port: Entity<InputState>,
    input_ssh_user: Entity<InputState>,
    input_ssh_key_path: Entity<InputState>,
    input_ssh_key_passphrase: Entity<InputState>,
    input_ssh_password: Entity<InputState>,

    validation_errors: Vec<String>,
    test_status: TestStatus,
    test_error: Option<String>,
}

impl ConnectionManagerWindow {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let available_drivers: Vec<DriverInfo> = app_state
            .read(cx)
            .drivers
            .values()
            .map(|driver| DriverInfo {
                kind: driver.kind(),
                name: driver.display_name().to_string(),
                description: driver.description().to_string(),
            })
            .collect();

        let input_name = cx.new(|cx| InputState::new(window, cx).placeholder("Connection name"));
        let input_host = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("localhost")
                .default_value("localhost")
        });
        let input_port = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("5432")
                .default_value("5432")
        });
        let input_user = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("postgres")
                .default_value("postgres")
        });
        let input_password = cx.new(|cx| InputState::new(window, cx).placeholder("Password"));
        let input_database = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("postgres")
                .default_value("postgres")
        });
        let input_path =
            cx.new(|cx| InputState::new(window, cx).placeholder("/path/to/database.db"));

        let input_ssh_host =
            cx.new(|cx| InputState::new(window, cx).placeholder("bastion.example.com"));
        let input_ssh_port = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("22")
                .default_value("22")
        });
        let input_ssh_user = cx.new(|cx| InputState::new(window, cx).placeholder("ec2-user"));
        let input_ssh_key_path =
            cx.new(|cx| InputState::new(window, cx).placeholder("~/.ssh/id_rsa"));
        let input_ssh_key_passphrase =
            cx.new(|cx| InputState::new(window, cx).placeholder("Key passphrase (optional)"));
        let input_ssh_password =
            cx.new(|cx| InputState::new(window, cx).placeholder("SSH password"));

        Self {
            app_state,
            view: View::DriverSelect,
            active_tab: FormTab::Main,
            available_drivers,
            selected_driver: None,
            form_save_password: false,
            form_save_ssh_secret: false,
            editing_profile_id: None,
            input_name,
            input_host,
            input_port,
            input_user,
            input_password,
            input_database,
            input_path,
            ssh_enabled: false,
            ssh_auth_method: SshAuthSelection::PrivateKey,
            input_ssh_host,
            input_ssh_port,
            input_ssh_user,
            input_ssh_key_path,
            input_ssh_key_passphrase,
            input_ssh_password,
            validation_errors: Vec::new(),
            test_status: TestStatus::None,
            test_error: None,
        }
    }

    pub fn new_for_edit(
        app_state: Entity<AppState>,
        profile: &ConnectionProfile,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut instance = Self::new(app_state.clone(), window, cx);
        instance.editing_profile_id = Some(profile.id);

        let driver = app_state.read(cx).drivers.get(&profile.kind()).cloned();
        instance.selected_driver = driver;
        instance.form_save_password = profile.save_password;
        instance.view = View::EditForm;

        instance.input_name.update(cx, |state, cx| {
            state.set_value(&profile.name, window, cx);
        });

        match &profile.config {
            DbConfig::Postgres {
                host,
                port,
                user,
                database,
                ssh_tunnel,
                ..
            } => {
                instance.input_host.update(cx, |state, cx| {
                    state.set_value(host, window, cx);
                });
                instance.input_port.update(cx, |state, cx| {
                    state.set_value(port.to_string(), window, cx);
                });
                instance.input_user.update(cx, |state, cx| {
                    state.set_value(user, window, cx);
                });
                instance.input_database.update(cx, |state, cx| {
                    state.set_value(database, window, cx);
                });

                if let Some(ssh) = ssh_tunnel {
                    instance.ssh_enabled = true;
                    instance.input_ssh_host.update(cx, |state, cx| {
                        state.set_value(&ssh.host, window, cx);
                    });
                    instance.input_ssh_port.update(cx, |state, cx| {
                        state.set_value(ssh.port.to_string(), window, cx);
                    });
                    instance.input_ssh_user.update(cx, |state, cx| {
                        state.set_value(&ssh.user, window, cx);
                    });

                    match &ssh.auth_method {
                        SshAuthMethod::PrivateKey { key_path } => {
                            instance.ssh_auth_method = SshAuthSelection::PrivateKey;
                            if let Some(path) = key_path {
                                let path_str: String = path.to_string_lossy().into_owned();
                                instance.input_ssh_key_path.update(cx, |state, cx| {
                                    state.set_value(path_str, window, cx);
                                });
                            }
                        }
                        SshAuthMethod::Password => {
                            instance.ssh_auth_method = SshAuthSelection::Password;
                        }
                    }

                    if let Some(ssh_secret) = app_state.read(cx).get_ssh_password(profile) {
                        match instance.ssh_auth_method {
                            SshAuthSelection::PrivateKey => {
                                instance.input_ssh_key_passphrase.update(cx, |state, cx| {
                                    state.set_value(&ssh_secret, window, cx);
                                });
                            }
                            SshAuthSelection::Password => {
                                instance.input_ssh_password.update(cx, |state, cx| {
                                    state.set_value(&ssh_secret, window, cx);
                                });
                            }
                        }
                        instance.form_save_ssh_secret = true;
                    }
                }
            }
            DbConfig::SQLite { path } => {
                let path_str = path.to_string_lossy().to_string();
                instance.input_path.update(cx, |state, cx| {
                    state.set_value(&path_str, window, cx);
                });
            }
        }

        instance
    }

    fn select_driver(&mut self, kind: DbKind, window: &mut Window, cx: &mut Context<Self>) {
        let driver = self.app_state.read(cx).drivers.get(&kind).cloned();
        self.selected_driver = driver;
        self.form_save_password = false;
        self.ssh_enabled = false;
        self.ssh_auth_method = SshAuthSelection::PrivateKey;
        self.form_save_ssh_secret = false;
        self.active_tab = FormTab::Main;
        self.validation_errors.clear();
        self.test_status = TestStatus::None;
        self.test_error = None;

        self.input_name.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });

        match kind {
            DbKind::Postgres => {
                self.input_host.update(cx, |state, cx| {
                    state.set_value("localhost", window, cx);
                });
                self.input_port.update(cx, |state, cx| {
                    state.set_value("5432", window, cx);
                });
                self.input_user.update(cx, |state, cx| {
                    state.set_value("postgres", window, cx);
                });
                self.input_password.update(cx, |state, cx| {
                    state.set_value("", window, cx);
                });
                self.input_database.update(cx, |state, cx| {
                    state.set_value("postgres", window, cx);
                });
                self.input_ssh_host.update(cx, |state, cx| {
                    state.set_value("", window, cx);
                });
                self.input_ssh_port.update(cx, |state, cx| {
                    state.set_value("22", window, cx);
                });
                self.input_ssh_user.update(cx, |state, cx| {
                    state.set_value("", window, cx);
                });
                self.input_ssh_key_path.update(cx, |state, cx| {
                    state.set_value("", window, cx);
                });
                self.input_ssh_key_passphrase.update(cx, |state, cx| {
                    state.set_value("", window, cx);
                });
                self.input_ssh_password.update(cx, |state, cx| {
                    state.set_value("", window, cx);
                });
            }
            DbKind::SQLite => {
                self.input_path.update(cx, |state, cx| {
                    state.set_value("", window, cx);
                });
            }
        }

        self.view = View::EditForm;
        cx.notify();
    }

    fn back_to_driver_select(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.view = View::DriverSelect;
        self.selected_driver = None;
        self.validation_errors.clear();
        self.test_status = TestStatus::None;
        self.test_error = None;
        cx.notify();
    }

    fn selected_kind(&self) -> Option<DbKind> {
        self.selected_driver.as_ref().map(|d| d.kind())
    }

    fn validate_form(&mut self, cx: &mut Context<Self>) -> bool {
        self.validation_errors.clear();

        let name = self.input_name.read(cx).value().to_string();
        if name.trim().is_empty() {
            self.validation_errors
                .push("Connection name is required".to_string());
        }

        let Some(kind) = self.selected_kind() else {
            self.validation_errors
                .push("No driver selected".to_string());
            return false;
        };

        match kind {
            DbKind::Postgres => {
                let host = self.input_host.read(cx).value().to_string();
                if host.trim().is_empty() {
                    self.validation_errors.push("Host is required".to_string());
                }

                let port_str = self.input_port.read(cx).value().to_string();
                if port_str.trim().is_empty() {
                    self.validation_errors.push("Port is required".to_string());
                } else if port_str.parse::<u16>().is_err() {
                    self.validation_errors
                        .push("Port must be a valid number (1-65535)".to_string());
                }

                let user = self.input_user.read(cx).value().to_string();
                if user.trim().is_empty() {
                    self.validation_errors.push("User is required".to_string());
                }

                let database = self.input_database.read(cx).value().to_string();
                if database.trim().is_empty() {
                    self.validation_errors
                        .push("Database name is required".to_string());
                }

                if self.ssh_enabled {
                    let ssh_host = self.input_ssh_host.read(cx).value().to_string();
                    if ssh_host.trim().is_empty() {
                        self.validation_errors
                            .push("SSH Host is required when SSH is enabled".to_string());
                    }

                    let ssh_user = self.input_ssh_user.read(cx).value().to_string();
                    if ssh_user.trim().is_empty() {
                        self.validation_errors
                            .push("SSH User is required when SSH is enabled".to_string());
                    }

                    let ssh_port_str = self.input_ssh_port.read(cx).value().to_string();
                    if !ssh_port_str.trim().is_empty() && ssh_port_str.parse::<u16>().is_err() {
                        self.validation_errors
                            .push("SSH Port must be a valid number".to_string());
                    }
                }
            }
            DbKind::SQLite => {
                let path = self.input_path.read(cx).value().to_string();
                if path.trim().is_empty() {
                    self.validation_errors
                        .push("Database path is required".to_string());
                }
            }
        }

        self.validation_errors.is_empty()
    }

    fn expand_path(path_str: &str) -> PathBuf {
        if path_str.starts_with("~/") {
            std::env::var("HOME")
                .map(|home| PathBuf::from(home).join(&path_str[2..]))
                .unwrap_or_else(|_| PathBuf::from(path_str))
        } else {
            PathBuf::from(path_str)
        }
    }

    fn build_ssh_config(&self, cx: &Context<Self>) -> Option<SshTunnelConfig> {
        if !self.ssh_enabled {
            return None;
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

        Some(SshTunnelConfig {
            host,
            port,
            user,
            auth_method,
        })
    }

    fn build_config(&self, cx: &Context<Self>) -> Option<DbConfig> {
        let kind = self.selected_kind()?;

        Some(match kind {
            DbKind::Postgres => {
                let port = self.input_port.read(cx).value().parse().unwrap_or(5432);
                DbConfig::Postgres {
                    host: self.input_host.read(cx).value().to_string(),
                    port,
                    user: self.input_user.read(cx).value().to_string(),
                    database: self.input_database.read(cx).value().to_string(),
                    ssl_mode: dbflux_core::SslMode::Prefer,
                    ssh_tunnel: self.build_ssh_config(cx),
                }
            }
            DbKind::SQLite => {
                let path = self.input_path.read(cx).value().to_string();
                DbConfig::SQLite {
                    path: PathBuf::from(path),
                }
            }
        })
    }

    fn build_profile(&self, cx: &Context<Self>) -> Option<ConnectionProfile> {
        let name = self.input_name.read(cx).value().to_string();
        let config = self.build_config(cx)?;

        let mut profile = if let Some(existing_id) = self.editing_profile_id {
            let mut p = ConnectionProfile::new(name, config);
            p.id = existing_id;
            p
        } else {
            ConnectionProfile::new(name, config)
        };

        profile.save_password = self.form_save_password;
        Some(profile)
    }

    fn get_ssh_secret(&self, cx: &Context<Self>) -> Option<String> {
        if !self.ssh_enabled {
            return None;
        }

        let secret = match self.ssh_auth_method {
            SshAuthSelection::PrivateKey => {
                self.input_ssh_key_passphrase.read(cx).value().to_string()
            }
            SshAuthSelection::Password => self.input_ssh_password.read(cx).value().to_string(),
        };

        if secret.is_empty() {
            None
        } else {
            Some(secret)
        }
    }

    fn save_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.validate_form(cx) {
            cx.notify();
            return;
        }

        let Some(profile) = self.build_profile(cx) else {
            return;
        };

        let password = self.input_password.read(cx).value().to_string();
        let ssh_secret = self.get_ssh_secret(cx);
        let is_edit = self.editing_profile_id.is_some();

        info!(
            "{} profile: {}, save_password={}, password_len={}, ssh_enabled={}, ssh_auth={:?}",
            if is_edit { "Updating" } else { "Saving" },
            profile.name,
            profile.save_password,
            password.len(),
            self.ssh_enabled,
            self.ssh_auth_method
        );

        self.app_state.update(cx, |state, _| {
            if profile.save_password && !password.is_empty() {
                info!("Saving password to keyring for profile {}", profile.id);
                state.save_password(&profile, &password);
            } else if !profile.save_password {
                state.delete_password(&profile);
            }

            if self.form_save_ssh_secret {
                if let Some(ref secret) = ssh_secret {
                    info!("Saving SSH secret to keyring for profile {}", profile.id);
                    state.save_ssh_password(&profile, secret);
                }
            } else {
                state.delete_ssh_password(&profile);
            }

            if is_edit {
                state.update_profile(profile);
            } else {
                state.add_profile(profile);
            }
        });

        cx.emit(DismissEvent);
        window.remove_window();
    }

    fn test_connection(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.validate_form(cx) {
            cx.notify();
            return;
        }

        self.test_status = TestStatus::Testing;
        self.test_error = None;
        cx.notify();

        let Some(profile) = self.build_profile(cx) else {
            self.test_status = TestStatus::Failed;
            self.test_error = Some("Failed to build profile".to_string());
            cx.notify();
            return;
        };

        let password = self.input_password.read(cx).value().to_string();
        let password_opt = if password.is_empty() {
            None
        } else {
            Some(password)
        };

        let Some(driver) = &self.selected_driver else {
            self.test_status = TestStatus::Failed;
            self.test_error = Some("No driver selected".to_string());
            cx.notify();
            return;
        };

        match driver.connect_with_password(&profile, password_opt.as_deref()) {
            Ok(_connection) => {
                info!("Test connection successful for {}", profile.name);
                self.test_status = TestStatus::Success;
                self.test_error = None;
            }
            Err(e) => {
                info!("Test connection failed: {:?}", e);
                self.test_status = TestStatus::Failed;
                self.test_error = Some(format!("{:?}", e));
            }
        }

        cx.notify();
    }

    fn render_driver_select(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let drivers = self.available_drivers.clone();

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
                    .border_color(theme.border)
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("New Connection"),
                    ),
            )
            .child(
                div().flex_1().p_3().child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(
                            div()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .mb_2()
                                .child("Select database type"),
                        )
                        .children(drivers.into_iter().enumerate().map(|(idx, driver_info)| {
                            let kind = driver_info.kind;
                            ListItem::new(("driver", idx))
                                .py(px(8.0))
                                .on_click(cx.listener(move |this, _, window, cx| {
                                    this.select_driver(kind, window, cx);
                                }))
                                .child(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .gap_1()
                                        .child(
                                            div()
                                                .text_sm()
                                                .font_weight(FontWeight::SEMIBOLD)
                                                .child(driver_info.name),
                                        )
                                        .child(
                                            div()
                                                .text_xs()
                                                .text_color(theme.muted_foreground)
                                                .child(driver_info.description),
                                        ),
                                )
                        })),
                ),
            )
    }

    fn render_tab_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let active_tab = self.active_tab;

        div()
            .flex()
            .items_center()
            .border_b_1()
            .border_color(theme.border)
            .child(
                div()
                    .id("tab-main")
                    .px_4()
                    .py_2()
                    .cursor_pointer()
                    .border_b_2()
                    .when(active_tab == FormTab::Main, |d| {
                        d.border_color(theme.primary).text_color(theme.foreground)
                    })
                    .when(active_tab != FormTab::Main, |d| {
                        d.border_color(gpui::transparent_black())
                            .text_color(theme.muted_foreground)
                    })
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.active_tab = FormTab::Main;
                        cx.notify();
                    }))
                    .child(div().text_sm().child("Main")),
            )
            .child(
                div()
                    .id("tab-ssh")
                    .px_4()
                    .py_2()
                    .cursor_pointer()
                    .border_b_2()
                    .when(active_tab == FormTab::Ssh, |d| {
                        d.border_color(theme.primary).text_color(theme.foreground)
                    })
                    .when(active_tab != FormTab::Ssh, |d| {
                        d.border_color(gpui::transparent_black())
                            .text_color(theme.muted_foreground)
                    })
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.active_tab = FormTab::Ssh;
                        cx.notify();
                    }))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .child(div().text_sm().child("SSH"))
                            .when(self.ssh_enabled, |d| {
                                d.child(
                                    div()
                                        .w(px(6.0))
                                        .h(px(6.0))
                                        .rounded_full()
                                        .bg(gpui::rgb(0x22C55E)),
                                )
                            }),
                    ),
            )
    }

    fn render_main_tab(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_postgres = self.selected_kind() == Some(DbKind::Postgres);
        let keyring_available = self.app_state.read(cx).secret_store_available();
        let requires_password = self
            .selected_driver
            .as_ref()
            .map(|d| d.requires_password())
            .unwrap_or(true);
        let save_password = self.form_save_password;

        div()
            .flex()
            .flex_col()
            .gap_4()
            .p_4()
            .when(is_postgres, |d| {
                d.child(
                    self.render_section(
                        "Server",
                        div()
                            .flex()
                            .flex_col()
                            .gap_3()
                            .child(
                                div()
                                    .flex()
                                    .gap_3()
                                    .child(div().flex_1().child(self.form_field_input(
                                        "Host",
                                        &self.input_host,
                                        true,
                                    )))
                                    .child(div().w(px(80.0)).child(self.form_field_input(
                                        "Port",
                                        &self.input_port,
                                        true,
                                    ))),
                            )
                            .child(self.form_field_input("Database", &self.input_database, true)),
                        theme,
                    ),
                )
                .child(
                    self.render_section(
                        "Authentication",
                        div()
                            .flex()
                            .flex_col()
                            .gap_3()
                            .child(self.form_field_input("Username", &self.input_user, true))
                            .child(
                                div()
                                    .flex()
                                    .items_end()
                                    .gap_3()
                                    .child(div().flex_1().child(self.form_field_input(
                                        "Password",
                                        &self.input_password,
                                        false,
                                    )))
                                    .when(keyring_available && requires_password, |d| {
                                        d.child(
                                            div()
                                                .flex()
                                                .items_center()
                                                .gap_2()
                                                .pb(px(2.0))
                                                .child(
                                                    Checkbox::new("save-password")
                                                        .checked(save_password)
                                                        .on_click(cx.listener(
                                                            |this, checked: &bool, _, cx| {
                                                                this.form_save_password = *checked;
                                                                cx.notify();
                                                            },
                                                        )),
                                                )
                                                .child(div().text_sm().child("Save")),
                                        )
                                    }),
                            ),
                        theme,
                    ),
                )
            })
            .when(!is_postgres, |d| {
                d.child(self.render_section(
                    "Database",
                    self.form_field_input("File Path", &self.input_path, true),
                    theme,
                ))
            })
    }

    fn render_ssh_tab(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let ssh_enabled = self.ssh_enabled;
        let ssh_auth_method = self.ssh_auth_method;
        let keyring_available = self.app_state.read(cx).secret_store_available();
        let save_ssh_secret = self.form_save_ssh_secret;

        let ssh_toggle = Checkbox::new("ssh-enabled")
            .checked(ssh_enabled)
            .on_click(cx.listener(|this, checked: &bool, _, cx| {
                this.ssh_enabled = *checked;
                cx.notify();
            }));

        let (auth_selector, auth_inputs) = if ssh_enabled {
            let selector = self
                .render_ssh_auth_selector(ssh_auth_method, cx)
                .into_any_element();
            let inputs = self
                .render_ssh_auth_inputs(ssh_auth_method, keyring_available, save_ssh_secret, cx)
                .into_any_element();
            (Some(selector), Some(inputs))
        } else {
            (None, None)
        };

        let theme = cx.theme();
        let muted_fg = theme.muted_foreground;

        div()
            .flex()
            .flex_col()
            .gap_4()
            .p_4()
            .child(
                div().flex().items_center().gap_2().child(ssh_toggle).child(
                    div()
                        .text_sm()
                        .font_weight(FontWeight::MEDIUM)
                        .child("Use SSH Tunnel"),
                ),
            )
            .when(ssh_enabled, |d| {
                d.child(
                    self.render_section(
                        "SSH Server",
                        div()
                            .flex()
                            .flex_col()
                            .gap_3()
                            .child(
                                div()
                                    .flex()
                                    .gap_3()
                                    .child(div().flex_1().child(self.form_field_input(
                                        "Host",
                                        &self.input_ssh_host,
                                        true,
                                    )))
                                    .child(div().w(px(80.0)).child(self.form_field_input(
                                        "Port",
                                        &self.input_ssh_port,
                                        false,
                                    ))),
                            )
                            .child(self.form_field_input("Username", &self.input_ssh_user, true)),
                        theme,
                    ),
                )
            })
            .when_some(auth_selector, |d, selector| {
                d.child(self.render_section("Authentication", selector, theme))
            })
            .when_some(auth_inputs, |d, inputs| d.child(inputs))
            .when(!ssh_enabled, |d| {
                d.child(
                    div().flex_1().flex().items_center().justify_center().child(
                        div().text_sm().text_color(muted_fg).child(
                            "Enable SSH tunnel to configure connection through a bastion host",
                        ),
                    ),
                )
            })
    }

    fn render_ssh_auth_selector(
        &self,
        current: SshAuthSelection,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let click_key = cx.listener(|this, _, _, cx| {
            this.ssh_auth_method = SshAuthSelection::PrivateKey;
            cx.notify();
        });
        let click_pw = cx.listener(|this, _, _, cx| {
            this.ssh_auth_method = SshAuthSelection::Password;
            cx.notify();
        });

        let theme = cx.theme();
        let primary = theme.primary;
        let border = theme.border;

        div()
            .flex()
            .gap_4()
            .child(
                div()
                    .id("auth-private-key")
                    .flex()
                    .items_center()
                    .gap_2()
                    .cursor_pointer()
                    .on_click(click_key)
                    .child(self.render_radio_button(
                        current == SshAuthSelection::PrivateKey,
                        primary,
                        border,
                    ))
                    .child(div().text_sm().child("Private Key")),
            )
            .child(
                div()
                    .id("auth-password")
                    .flex()
                    .items_center()
                    .gap_2()
                    .cursor_pointer()
                    .on_click(click_pw)
                    .child(self.render_radio_button(
                        current == SshAuthSelection::Password,
                        primary,
                        border,
                    ))
                    .child(div().text_sm().child("Password")),
            )
    }

    fn render_radio_button(&self, selected: bool, primary: Hsla, border: Hsla) -> impl IntoElement {
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

    fn render_ssh_auth_inputs(
        &self,
        auth_method: SshAuthSelection,
        keyring_available: bool,
        save_ssh_secret: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let passphrase_checkbox = if keyring_available {
            Some(
                Checkbox::new("save-ssh-passphrase")
                    .checked(save_ssh_secret)
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.form_save_ssh_secret = *checked;
                        cx.notify();
                    })),
            )
        } else {
            None
        };

        let password_checkbox = if keyring_available {
            Some(
                Checkbox::new("save-ssh-password")
                    .checked(save_ssh_secret)
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.form_save_ssh_secret = *checked;
                        cx.notify();
                    })),
            )
        } else {
            None
        };

        let theme = cx.theme();
        let muted_fg = theme.muted_foreground;

        match auth_method {
            SshAuthSelection::PrivateKey => div()
                .flex()
                .flex_col()
                .gap_3()
                .child(self.form_field_input("Private Key Path", &self.input_ssh_key_path, false))
                .child(
                    div()
                        .text_xs()
                        .text_color(muted_fg)
                        .child("Leave empty to use SSH agent or default keys (~/.ssh/id_rsa)"),
                )
                .child(
                    div()
                        .flex()
                        .items_end()
                        .gap_3()
                        .child(div().flex_1().child(self.form_field_input(
                            "Key Passphrase",
                            &self.input_ssh_key_passphrase,
                            false,
                        )))
                        .when_some(passphrase_checkbox, |d, checkbox| {
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
                .child(
                    div()
                        .text_xs()
                        .text_color(muted_fg)
                        .child("Leave empty if key has no passphrase"),
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
                        .child(div().flex_1().child(self.form_field_input(
                            "SSH Password",
                            &self.input_ssh_password,
                            true,
                        )))
                        .when_some(password_checkbox, |d, checkbox| {
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

    fn render_section(
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

    fn render_form(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(driver) = &self.selected_driver else {
            return div().into_any_element();
        };

        let kind = driver.kind();
        let driver_name = driver.display_name().to_string();
        let is_postgres = kind == DbKind::Postgres;
        let validation_errors = self.validation_errors.clone();
        let test_status = self.test_status;
        let test_error = self.test_error.clone();
        let is_editing = self.editing_profile_id.is_some();
        let title = if is_editing {
            format!("Edit {} Connection", driver_name)
        } else {
            format!("New {} Connection", driver_name)
        };

        let tab_bar = if is_postgres {
            Some(self.render_tab_bar(cx).into_any_element())
        } else {
            None
        };

        let tab_content = match (is_postgres, self.active_tab) {
            (true, FormTab::Main) => self.render_main_tab(cx).into_any_element(),
            (true, FormTab::Ssh) => self.render_ssh_tab(cx).into_any_element(),
            (false, _) => self.render_main_tab(cx).into_any_element(),
        };

        let theme = cx.theme();
        let border_color = theme.border;

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
                        d.child(Button::new("back").ghost().label("<").compact().on_click(
                            cx.listener(|this, _, window, cx| {
                                this.back_to_driver_select(window, cx);
                            }),
                        ))
                    })
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child(title),
                    )
                    .child(div().flex_1())
                    .child(self.form_field_input_inline("Name", &self.input_name)),
            )
            .when_some(tab_bar, |d, tab_bar| d.child(tab_bar))
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .when(!validation_errors.is_empty(), |d: Div| {
                        d.child(div().p_3().child(
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
                    .child(tab_content),
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
                                Button::new("test-connection")
                                    .ghost()
                                    .label("Test Connection")
                                    .compact()
                                    .disabled(test_status == TestStatus::Testing)
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.test_connection(window, cx);
                                    })),
                            )
                            .child(
                                Button::new("save-connection")
                                    .primary()
                                    .label("Save")
                                    .compact()
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.save_profile(window, cx);
                                    })),
                            ),
                    ),
            )
            .into_any_element()
    }

    fn form_field_input(
        &self,
        label: &str,
        input: &Entity<InputState>,
        required: bool,
    ) -> impl IntoElement {
        div()
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

    fn form_field_input_inline(&self, label: &str, input: &Entity<InputState>) -> impl IntoElement {
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
            .child(div().w(px(200.0)).child(Input::new(input)))
    }
}

pub struct DismissEvent;

impl EventEmitter<DismissEvent> for ConnectionManagerWindow {}

impl Render for ConnectionManagerWindow {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .size_full()
            .bg(theme.background)
            .child(match self.view {
                View::DriverSelect => self.render_driver_select(window, cx).into_any_element(),
                View::EditForm => self.render_form(window, cx).into_any_element(),
            })
    }
}
