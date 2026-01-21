use crate::app::AppState;
use dbflux_core::{ConnectionProfile, DbConfig, DbDriver, DbKind};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Disableable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::input::{Input, InputState};
use gpui_component::list::ListItem;
use log::info;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Copy, PartialEq)]
enum View {
    DriverSelect,
    EditForm,
}

#[derive(Clone, Copy, PartialEq)]
enum TestStatus {
    None,
    Testing,
    Success,
    Failed,
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
    available_drivers: Vec<DriverInfo>,
    selected_driver: Option<Arc<dyn DbDriver>>,
    form_save_password: bool,
    editing_profile_id: Option<uuid::Uuid>,

    input_name: Entity<InputState>,
    input_host: Entity<InputState>,
    input_port: Entity<InputState>,
    input_user: Entity<InputState>,
    input_password: Entity<InputState>,
    input_database: Entity<InputState>,
    input_path: Entity<InputState>,

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

        Self {
            app_state,
            view: View::DriverSelect,
            available_drivers,
            selected_driver: None,
            form_save_password: false,
            editing_profile_id: None,
            input_name,
            input_host,
            input_port,
            input_user,
            input_password,
            input_database,
            input_path,
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
                ..
            } => {
                instance.input_host.update(cx, |state, cx| {
                    state.set_value(host, window, cx);
                });
                instance.input_port.update(cx, |state, cx| {
                    state.set_value(&port.to_string(), window, cx);
                });
                instance.input_user.update(cx, |state, cx| {
                    state.set_value(user, window, cx);
                });
                instance.input_database.update(cx, |state, cx| {
                    state.set_value(database, window, cx);
                });
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
                    ssh_tunnel: None,
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

    fn save_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.validate_form(cx) {
            cx.notify();
            return;
        }

        let Some(profile) = self.build_profile(cx) else {
            return;
        };

        let password = self.input_password.read(cx).value().to_string();
        let is_edit = self.editing_profile_id.is_some();

        info!(
            "{} profile: {}, save_password={}, password_len={}",
            if is_edit { "Updating" } else { "Saving" },
            profile.name,
            profile.save_password,
            password.len()
        );

        self.app_state.update(cx, |state, _| {
            if profile.save_password && !password.is_empty() {
                info!("Saving password to keyring for profile {}", profile.id);
                state.save_password(&profile, &password);
            } else if !profile.save_password {
                state.delete_password(&profile);
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

    fn render_form(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let Some(driver) = &self.selected_driver else {
            return div().into_any_element();
        };

        let kind = driver.kind();
        let driver_name = driver.display_name().to_string();
        let requires_password = driver.requires_password();
        let is_postgres = kind == DbKind::Postgres;
        let keyring_available = self.app_state.read(cx).secret_store_available();
        let save_password = self.form_save_password;
        let validation_errors = self.validation_errors.clone();
        let test_status = self.test_status;
        let test_error = self.test_error.clone();

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
                        Button::new("back")
                            .ghost()
                            .label("<")
                            .compact()
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.back_to_driver_select(window, cx);
                            })),
                    )
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child(format!("New {} Connection", driver_name)),
                    ),
            )
            .child(
                div().flex_1().p_3().child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_3()
                        .when(!validation_errors.is_empty(), |d| {
                            d.child(div().p_2().rounded(px(4.0)).bg(gpui::rgb(0x7F1D1D)).child(
                                div().flex().flex_col().gap_1().children(
                                    validation_errors.iter().map(|err| {
                                        div()
                                            .text_sm()
                                            .text_color(gpui::rgb(0xFCA5A5))
                                            .child(err.clone())
                                    }),
                                ),
                            ))
                        })
                        .child(self.form_field_input("Name", &self.input_name, true))
                        .when(is_postgres, |d| {
                            d.child(self.form_field_input("Host", &self.input_host, true))
                                .child(self.form_field_input("Port", &self.input_port, true))
                                .child(self.form_field_input("User", &self.input_user, true))
                                .child(self.form_field_input(
                                    "Password",
                                    &self.input_password,
                                    false,
                                ))
                                .child(self.form_field_input(
                                    "Database",
                                    &self.input_database,
                                    true,
                                ))
                                .when(keyring_available && requires_password, |d| {
                                    d.child(
                                        div()
                                            .flex()
                                            .items_center()
                                            .gap_2()
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
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .child("Save password in system keyring"),
                                            ),
                                    )
                                })
                        })
                        .when(!is_postgres, |d| {
                            d.child(self.form_field_input("Database Path", &self.input_path, true))
                        }),
                ),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .p_3()
                    .border_t_1()
                    .border_color(theme.border)
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
