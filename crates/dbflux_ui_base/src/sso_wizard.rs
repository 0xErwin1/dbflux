use crate::app_state_entity::{AppStateChanged, AppStateEntity, AuthProfileCreated};
use crate::modal_frame::ModalFrame;
use crate::platform;
use dbflux_components::controls::InputState;
use dbflux_components::controls::{Button, Input};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Text;
#[cfg(feature = "aws")]
use dbflux_components::tokens::Radii;
use dbflux_components::tokens::Spacing;
use dbflux_core::AuthProfile;
use gpui::prelude::FluentBuilder;
use gpui::*;
#[cfg(feature = "aws")]
use gpui_component::ActiveTheme;
use uuid::Uuid;

#[cfg(feature = "aws")]
use dbflux_aws::{
    AwsSsoAccount, list_sso_account_roles_blocking, list_sso_accounts_blocking, login_sso_blocking,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum WizardStep {
    Start,
    Account,
    Role,
    Confirm,
}

pub struct SsoWizard {
    app_state: Entity<AppStateEntity>,
    visible: bool,
    step: WizardStep,
    focus_handle: FocusHandle,
    input_profile_name: Entity<InputState>,
    input_start_url: Entity<InputState>,
    input_region: Entity<InputState>,
    input_account_id: Entity<InputState>,
    input_role_name: Entity<InputState>,
    status: Option<String>,

    #[cfg(feature = "aws")]
    discovered_accounts: Vec<AwsSsoAccount>,
    #[cfg(feature = "aws")]
    discovered_roles: Vec<String>,
    #[cfg(feature = "aws")]
    accounts_loading: bool,
    #[cfg(feature = "aws")]
    roles_loading: bool,
}

pub enum SsoWizardEvent {
    ProfileCreated { profile_id: Uuid },
}

impl SsoWizard {
    pub fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        Self {
            app_state,
            visible: false,
            step: WizardStep::Start,
            focus_handle: cx.focus_handle(),
            input_profile_name: cx
                .new(|cx| InputState::new(window, cx).placeholder("profile-name")),
            input_start_url: cx
                .new(|cx| InputState::new(window, cx).placeholder("https://...awsapps.com/start")),
            input_region: cx.new(|cx| InputState::new(window, cx).placeholder("us-east-1")),
            input_account_id: cx.new(|cx| InputState::new(window, cx).placeholder("123456789012")),
            input_role_name: cx.new(|cx| InputState::new(window, cx).placeholder("ReadOnlyRole")),
            status: None,

            #[cfg(feature = "aws")]
            discovered_accounts: Vec::new(),
            #[cfg(feature = "aws")]
            discovered_roles: Vec::new(),
            #[cfg(feature = "aws")]
            accounts_loading: false,
            #[cfg(feature = "aws")]
            roles_loading: false,
        }
    }

    pub fn open(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.visible = true;
        self.step = WizardStep::Start;
        self.status = None;
        self.focus_handle.focus(window);
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        cx.notify();
    }

    fn next(&mut self, cx: &mut Context<Self>) {
        self.step = match self.step {
            WizardStep::Start => WizardStep::Account,
            WizardStep::Account => WizardStep::Role,
            WizardStep::Role => WizardStep::Confirm,
            WizardStep::Confirm => WizardStep::Confirm,
        };
        cx.notify();
    }

    fn back(&mut self, cx: &mut Context<Self>) {
        self.step = match self.step {
            WizardStep::Start => WizardStep::Start,
            WizardStep::Account => WizardStep::Start,
            WizardStep::Role => WizardStep::Account,
            WizardStep::Confirm => WizardStep::Role,
        };
        cx.notify();
    }

    fn save_profile(&mut self, cx: &mut Context<Self>) {
        let profile_name = self.input_profile_name.read(cx).value().trim().to_string();
        let start_url = self.input_start_url.read(cx).value().trim().to_string();
        let region = self.input_region.read(cx).value().trim().to_string();
        let account_id = self.input_account_id.read(cx).value().trim().to_string();
        let role_name = self.input_role_name.read(cx).value().trim().to_string();

        if profile_name.is_empty() || start_url.is_empty() || region.is_empty() {
            self.status = Some(dbflux_i18n::t!("sso_wizard.status.missing_required"));
            cx.notify();
            return;
        }

        let mut fields = std::collections::HashMap::new();
        fields.insert("profile_name".to_string(), profile_name.clone());
        fields.insert("sso_start_url".to_string(), start_url);
        fields.insert("region".to_string(), region);
        if !account_id.is_empty() {
            fields.insert("sso_account_id".to_string(), account_id);
        }
        if !role_name.is_empty() {
            fields.insert("sso_role_name".to_string(), role_name);
        }

        let profile_id = Uuid::new_v4();

        self.app_state.update(cx, |state, cx| {
            state.add_auth_profile(AuthProfile {
                id: profile_id,
                name: profile_name,
                provider_id: "aws-sso".to_string(),
                fields,
                // AWS SSO profiles carry only non-secret config; no key material.
                secret_fields: std::collections::HashMap::new(),
                enabled: true,
                read_only: false,
                dangling_origin: None,
            });
            cx.emit(AuthProfileCreated { profile_id });
            cx.emit(AppStateChanged);
        });

        cx.emit(SsoWizardEvent::ProfileCreated { profile_id });

        self.status = Some(dbflux_i18n::t!("sso_wizard.status.profile_created"));
        self.visible = false;
        cx.notify();
    }

    #[cfg(feature = "aws")]
    fn discover_accounts(&mut self, cx: &mut Context<Self>) {
        if self.accounts_loading {
            return;
        }

        let profile_name = self.input_profile_name.read(cx).value().trim().to_string();
        let start_url = self.input_start_url.read(cx).value().trim().to_string();
        let region = self.input_region.read(cx).value().trim().to_string();

        if profile_name.is_empty() || start_url.is_empty() || region.is_empty() {
            self.status = Some(dbflux_i18n::t!(
                "sso_wizard.status.fill_before_account_discovery"
            ));
            cx.notify();
            return;
        }

        self.accounts_loading = true;
        self.status = Some(dbflux_i18n::t!("sso_wizard.status.discovering_accounts"));
        cx.notify();

        let this = cx.entity().clone();
        let task = cx.background_executor().spawn(async move {
            let _ = login_sso_blocking(Uuid::nil(), &profile_name, &start_url);
            list_sso_accounts_blocking(&profile_name, &region, &start_url)
        });

        cx.spawn(async move |_entity, cx| {
            let result = task.await;
            let _ = cx.update(|cx| {
                this.update(cx, |this, cx| {
                    this.accounts_loading = false;
                    match result {
                        Ok(accounts) => {
                            this.discovered_accounts = accounts;
                            this.status =
                                Some(dbflux_i18n::t!("sso_wizard.status.accounts_discovered"));
                        }
                        Err(error) => {
                            this.discovered_accounts.clear();
                            this.status = Some(dbflux_i18n::t!(
                                "sso_wizard.status.accounts_failed",
                                error = error
                            ));
                        }
                    }
                    cx.notify();
                });
            });
        })
        .detach();
    }

    #[cfg(feature = "aws")]
    fn discover_roles_for_selected_account(&mut self, cx: &mut Context<Self>) {
        if self.roles_loading {
            return;
        }

        let profile_name = self.input_profile_name.read(cx).value().trim().to_string();
        let start_url = self.input_start_url.read(cx).value().trim().to_string();
        let region = self.input_region.read(cx).value().trim().to_string();
        let account_id = self.input_account_id.read(cx).value().trim().to_string();

        if profile_name.is_empty()
            || start_url.is_empty()
            || region.is_empty()
            || account_id.is_empty()
        {
            self.status = Some(dbflux_i18n::t!(
                "sso_wizard.status.fill_before_role_discovery"
            ));
            cx.notify();
            return;
        }

        self.roles_loading = true;
        self.status = Some(dbflux_i18n::t!("sso_wizard.status.discovering_roles"));
        cx.notify();

        let this = cx.entity().clone();
        let task = cx.background_executor().spawn(async move {
            list_sso_account_roles_blocking(&profile_name, &region, &start_url, &account_id)
        });

        cx.spawn(async move |_entity, cx| {
            let result = task.await;
            let _ = cx.update(|cx| {
                this.update(cx, |this, cx| {
                    this.roles_loading = false;
                    match result {
                        Ok(roles) => {
                            this.discovered_roles = roles;
                            this.status =
                                Some(dbflux_i18n::t!("sso_wizard.status.roles_discovered"));
                        }
                        Err(error) => {
                            this.discovered_roles.clear();
                            this.status = Some(dbflux_i18n::t!(
                                "sso_wizard.status.roles_failed",
                                error = error
                            ));
                        }
                    }
                    cx.notify();
                });
            });
        })
        .detach();
    }
}

impl EventEmitter<SsoWizardEvent> for SsoWizard {}

impl Render for SsoWizard {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let title_text = dbflux_i18n::t!("sso_wizard.title");
        let csd_title_bar = platform::render_csd_title_bar(_window, cx, &title_text);

        let content = if !self.visible {
            div().into_any_element()
        } else {
            self.render_visible(_window, cx)
        };

        div()
            .size_full()
            .when_some(csd_title_bar, |el, title_bar| el.child(title_bar))
            .child(content)
            .into_any_element()
    }
}

impl SsoWizard {
    fn render_visible(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        let close_entity = cx.entity().downgrade();
        let close = move |_window: &mut Window, cx: &mut App| {
            let _ = close_entity.update(cx, |this, cx| this.close(cx));
        };

        let mut frame = ModalFrame::new("sso-wizard", &self.focus_handle, close)
            .title(dbflux_i18n::t!("sso_wizard.title"))
            .icon(AppIcon::Lock)
            .width(px(680.0));

        let step_title = match self.step {
            WizardStep::Start => dbflux_i18n::t!("sso_wizard.step.start"),
            WizardStep::Account => dbflux_i18n::t!("sso_wizard.step.account"),
            WizardStep::Role => dbflux_i18n::t!("sso_wizard.step.role"),
            WizardStep::Confirm => dbflux_i18n::t!("sso_wizard.step.confirm"),
        };

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .child(Text::body(step_title))
            .child(match self.step {
                WizardStep::Start => div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::SM)
                    .child(Input::new(&self.input_profile_name))
                    .child(Input::new(&self.input_start_url))
                    .child(Input::new(&self.input_region))
                    .into_any_element(),
                WizardStep::Account => {
                    #[cfg_attr(not(feature = "aws"), allow(unused_mut))]
                    let mut account_step = div()
                        .flex()
                        .flex_col()
                        .gap(Spacing::SM)
                        .child(Input::new(&self.input_account_id))
                        .child(Text::caption(dbflux_i18n::t!("sso_wizard.account.hint")));

                    #[cfg(feature = "aws")]
                    {
                        let query = self.input_account_id.read(cx).value().trim().to_lowercase();

                        let filtered_accounts = self
                            .discovered_accounts
                            .iter()
                            .filter(|account| {
                                if query.is_empty() {
                                    return true;
                                }

                                account.account_id.to_lowercase().contains(&query)
                                    || account.account_name.to_lowercase().contains(&query)
                            })
                            .cloned()
                            .collect::<Vec<_>>();

                        let discover_label = if self.accounts_loading {
                            dbflux_i18n::t!("sso_wizard.account.discovering")
                        } else {
                            dbflux_i18n::t!("sso_wizard.account.discover_button")
                        };

                        account_step = account_step
                            .child(
                                Button::new("sso-wizard-discover-accounts", discover_label)
                                    .ghost()
                                    .disabled(self.accounts_loading)
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.discover_accounts(cx);
                                    })),
                            )
                            .when(
                                !self.accounts_loading
                                    && !self.discovered_accounts.is_empty()
                                    && filtered_accounts.is_empty(),
                                |d| {
                                    d.child(Text::caption(dbflux_i18n::t!(
                                        "sso_wizard.account.no_matches"
                                    )))
                                },
                            )
                            .child(div().flex().flex_col().gap_1().children(
                                filtered_accounts.iter().map(|account| {
                                    let label = format!(
                                        "{} ({})",
                                        account.account_name, account.account_id
                                    );
                                    let account_id = account.account_id.clone();

                                    div()
                                        .px(Spacing::SM)
                                        .py(Spacing::XS)
                                        .rounded(Radii::SM)
                                        .border_1()
                                        .border_color(cx.theme().border)
                                        .cursor_pointer()
                                        .on_mouse_down(
                                            MouseButton::Left,
                                            cx.listener(move |this, _, window, cx| {
                                                this.input_account_id.update(cx, |state, cx| {
                                                    state.set_value(account_id.clone(), window, cx);
                                                });
                                                cx.notify();
                                            }),
                                        )
                                        .child(label)
                                }),
                            ));
                    }

                    account_step.into_any_element()
                }
                WizardStep::Role => {
                    #[cfg_attr(not(feature = "aws"), allow(unused_mut))]
                    let mut role_step = div()
                        .flex()
                        .flex_col()
                        .gap(Spacing::SM)
                        .child(Input::new(&self.input_role_name))
                        .child(Text::caption(dbflux_i18n::t!("sso_wizard.role.hint")));

                    #[cfg(feature = "aws")]
                    {
                        let query = self.input_role_name.read(cx).value().trim().to_lowercase();

                        let filtered_roles = self
                            .discovered_roles
                            .iter()
                            .filter(|role| {
                                if query.is_empty() {
                                    return true;
                                }

                                role.to_lowercase().contains(&query)
                            })
                            .cloned()
                            .collect::<Vec<_>>();

                        let discover_label = if self.roles_loading {
                            dbflux_i18n::t!("sso_wizard.role.discovering")
                        } else {
                            dbflux_i18n::t!("sso_wizard.role.discover_button")
                        };

                        role_step = role_step
                            .child(
                                Button::new("sso-wizard-discover-roles", discover_label)
                                    .ghost()
                                    .disabled(self.roles_loading)
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.discover_roles_for_selected_account(cx);
                                    })),
                            )
                            .when(
                                !self.roles_loading
                                    && !self.discovered_roles.is_empty()
                                    && filtered_roles.is_empty(),
                                |d| {
                                    d.child(Text::caption(dbflux_i18n::t!(
                                        "sso_wizard.role.no_matches"
                                    )))
                                },
                            )
                            .child(div().flex().flex_col().gap_1().children(
                                filtered_roles.iter().map(|role| {
                                    let role_name = role.clone();

                                    div()
                                        .px(Spacing::SM)
                                        .py(Spacing::XS)
                                        .rounded(Radii::SM)
                                        .border_1()
                                        .border_color(cx.theme().border)
                                        .cursor_pointer()
                                        .on_mouse_down(
                                            MouseButton::Left,
                                            cx.listener(move |this, _, window, cx| {
                                                this.input_role_name.update(cx, |state, cx| {
                                                    state.set_value(role_name.clone(), window, cx);
                                                });
                                                cx.notify();
                                            }),
                                        )
                                        .child(role.clone())
                                }),
                            ));
                    }

                    role_step.into_any_element()
                }
                WizardStep::Confirm => div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(Text::body(dbflux_i18n::t!(
                        "sso_wizard.confirm.profile",
                        value = self.input_profile_name.read(cx).value()
                    )))
                    .child(Text::body(dbflux_i18n::t!(
                        "sso_wizard.confirm.start_url",
                        value = self.input_start_url.read(cx).value()
                    )))
                    .child(Text::body(dbflux_i18n::t!(
                        "sso_wizard.confirm.region",
                        value = self.input_region.read(cx).value()
                    )))
                    .child(Text::body(dbflux_i18n::t!(
                        "sso_wizard.confirm.account",
                        value = self.input_account_id.read(cx).value()
                    )))
                    .child(Text::body(dbflux_i18n::t!(
                        "sso_wizard.confirm.role",
                        value = self.input_role_name.read(cx).value()
                    )))
                    .into_any_element(),
            })
            .when_some(self.status.clone(), |d, status| {
                d.child(Text::caption(status))
            })
            .child(
                div()
                    .flex()
                    .justify_end()
                    .gap(Spacing::SM)
                    .child(
                        Button::new("sso-wizard-back", dbflux_i18n::t!("sso_wizard.back"))
                            .ghost()
                            .disabled(matches!(self.step, WizardStep::Start))
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.back(cx);
                            })),
                    )
                    .child({
                        let next_label = if matches!(self.step, WizardStep::Confirm) {
                            dbflux_i18n::t!("sso_wizard.save")
                        } else {
                            dbflux_i18n::t!("sso_wizard.next")
                        };
                        Button::new("sso-wizard-next", next_label)
                            .ghost()
                            .on_click(cx.listener(|this, _, _, cx| {
                                if matches!(this.step, WizardStep::Confirm) {
                                    this.save_profile(cx);
                                } else {
                                    this.next(cx);
                                }
                            }))
                    }),
            );

        frame = frame.child(body);
        frame.render(cx)
    }
}

#[cfg(test)]
mod tests {
    const SSO_WIZARD_KEYS: &[&str] = &[
        "sso_wizard.title",
        "sso_wizard.step.start",
        "sso_wizard.step.account",
        "sso_wizard.step.role",
        "sso_wizard.step.confirm",
        "sso_wizard.account.hint",
        "sso_wizard.account.discovering",
        "sso_wizard.account.discover_button",
        "sso_wizard.account.no_matches",
        "sso_wizard.role.hint",
        "sso_wizard.role.discovering",
        "sso_wizard.role.discover_button",
        "sso_wizard.role.no_matches",
        "sso_wizard.confirm.profile",
        "sso_wizard.confirm.start_url",
        "sso_wizard.confirm.region",
        "sso_wizard.confirm.account",
        "sso_wizard.confirm.role",
        "sso_wizard.back",
        "sso_wizard.next",
        "sso_wizard.save",
        "sso_wizard.status.missing_required",
        "sso_wizard.status.fill_before_account_discovery",
        "sso_wizard.status.discovering_accounts",
        "sso_wizard.status.accounts_discovered",
        "sso_wizard.status.accounts_failed",
        "sso_wizard.status.fill_before_role_discovery",
        "sso_wizard.status.discovering_roles",
        "sso_wizard.status.roles_discovered",
        "sso_wizard.status.roles_failed",
        "sso_wizard.status.profile_created",
    ];

    #[test]
    fn sso_wizard_catalog_keys_resolve() {
        for key in SSO_WIZARD_KEYS.iter().copied() {
            let english = dbflux_i18n::t!(key);
            let spanish = dbflux_i18n::t!(key, locale = "es");

            assert!(!english.is_empty(), "empty English translation for {key}");
            assert_ne!(english, key, "missing English translation for {key}");
            assert!(!spanish.is_empty(), "empty Spanish translation for {key}");
            assert_ne!(spanish, key, "missing Spanish translation for {key}");
        }
    }

    #[test]
    fn sso_wizard_step_title_differs_between_locales() {
        let english = dbflux_i18n::t!("sso_wizard.step.start");
        let spanish = dbflux_i18n::t!("sso_wizard.step.start", locale = "es");

        assert_ne!(english, spanish);
    }
}
