use crate::ui::icons::AppIcon;
use crate::ui::labels::{
    login_browser_open_failed_message, login_elapsed_message, login_sign_in_prompt,
};
use dbflux_components::controls::Button;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::{Radii, Spacing};
use dbflux_core::PipelineState;
use dbflux_ui_base::modal_frame::ModalFrame;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use std::time::{Duration, Instant};

const SSO_LOGIN_TIMEOUT: Duration = Duration::from_secs(300);
const LOGIN_SUCCESS_AUTO_CLOSE_DELAY: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub enum LoginModalState {
    Idle,
    WaitingForBrowser {
        provider_name: String,
        profile_name: String,
        verification_url: Option<String>,
        launch_error: Option<String>,
        started_at: Instant,
    },
    Success,
    Failed {
        error: String,
        provider_name: Option<String>,
    },
    Cancelled,
}

pub enum LoginModalEvent {
    OpenAuthProfilesSettings,
}

pub struct LoginModal {
    visible: bool,
    state: LoginModalState,
    focus_handle: FocusHandle,
    last_provider_name: Option<String>,
    timeout_generation: u64,
    success_generation: u64,
}

fn failed_state_shows_open_auth_profiles_button(provider_name: Option<&str>) -> bool {
    provider_name.is_some()
}

impl LoginModal {
    pub fn new(_window: &mut Window, cx: &mut Context<Self>) -> Self {
        Self {
            visible: false,
            state: LoginModalState::Idle,
            focus_handle: cx.focus_handle(),
            last_provider_name: None,
            timeout_generation: 0,
            success_generation: 0,
        }
    }

    pub fn apply_pipeline_state(
        &mut self,
        profile_name: &str,
        state: &PipelineState,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match state {
            PipelineState::WaitingForLogin {
                provider_name,
                verification_url,
            } => {
                log::debug!(
                    "[login_modal] WaitingForLogin — provider='{}' url={:?}",
                    provider_name,
                    verification_url
                );
                self.visible = true;
                self.last_provider_name = Some(provider_name.clone());
                self.state = LoginModalState::WaitingForBrowser {
                    provider_name: provider_name.clone(),
                    profile_name: profile_name.to_string(),
                    verification_url: verification_url.clone(),
                    launch_error: None,
                    started_at: Instant::now(),
                };
                self.focus_handle.focus(window);
                self.schedule_timeout(cx);
            }
            PipelineState::Failed { stage, error } => {
                self.visible = true;
                self.state = LoginModalState::Failed {
                    provider_name: self.last_provider_name.clone(),
                    error: format!("{}: {}", stage, error),
                };
                self.focus_handle.focus(window);
            }
            PipelineState::Cancelled => {
                self.visible = false;
                self.state = LoginModalState::Cancelled;
            }
            PipelineState::Connected
            | PipelineState::ResolvingValues { .. }
            | PipelineState::OpeningAccess { .. }
            | PipelineState::Connecting { .. }
            | PipelineState::FetchingSchema => {
                if self.visible {
                    self.state = LoginModalState::Success;
                    self.visible = true;
                    self.schedule_success_close(cx);
                }
            }
            PipelineState::Idle | PipelineState::Authenticating { .. } => {}
        }

        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.state = LoginModalState::Cancelled;
        cx.notify();
    }

    fn schedule_timeout(&mut self, cx: &mut Context<Self>) {
        self.timeout_generation += 1;
        let generation = self.timeout_generation;
        let this = cx.entity().clone();

        cx.spawn(async move |_entity, cx| {
            cx.background_executor().timer(SSO_LOGIN_TIMEOUT).await;

            let _ = cx.update(|cx| {
                this.update(cx, |this, cx| {
                    if generation != this.timeout_generation {
                        return;
                    }

                    if let LoginModalState::WaitingForBrowser { provider_name, .. } = &this.state {
                        let provider_name = provider_name.clone();
                        this.state = LoginModalState::Failed {
                            provider_name: Some(provider_name),
                            error: dbflux_i18n::t!("login.error.timed_out"),
                        };
                        this.visible = true;
                        cx.notify();
                    }
                });
            });
        })
        .detach();
    }

    fn schedule_success_close(&mut self, cx: &mut Context<Self>) {
        self.success_generation += 1;
        let generation = self.success_generation;
        let this = cx.entity().clone();

        cx.spawn(async move |_entity, cx| {
            cx.background_executor()
                .timer(LOGIN_SUCCESS_AUTO_CLOSE_DELAY)
                .await;

            let _ = cx.update(|cx| {
                this.update(cx, |this, cx| {
                    if generation != this.success_generation {
                        return;
                    }

                    if matches!(this.state, LoginModalState::Success) {
                        this.close(cx);
                    }
                });
            });
        })
        .detach();
    }

    pub fn open_manual(
        &mut self,
        provider_name: impl Into<String>,
        profile_name: impl Into<String>,
        verification_url: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let provider_name = provider_name.into();
        self.visible = true;
        self.last_provider_name = Some(provider_name.clone());
        self.state = LoginModalState::WaitingForBrowser {
            provider_name,
            profile_name: profile_name.into(),
            verification_url,
            launch_error: None,
            started_at: Instant::now(),
        };
        self.focus_handle.focus(window);
        self.schedule_timeout(cx);
        cx.notify();
    }

    fn open_browser(&mut self, cx: &mut Context<Self>) {
        if let LoginModalState::WaitingForBrowser {
            verification_url,
            launch_error,
            ..
        } = &mut self.state
        {
            let Some(url) = verification_url.clone() else {
                *launch_error = Some(dbflux_i18n::t!("login.error.no_url"));
                cx.notify();
                return;
            };

            match open::that(&url) {
                Ok(_) => {
                    *launch_error = None;
                }
                Err(error) => match open::that_detached(&url) {
                    Ok(_) => {
                        *launch_error = None;
                    }
                    Err(detached_error) => {
                        *launch_error =
                            Some(login_browser_open_failed_message(error, detached_error));
                    }
                },
            }

            cx.notify();
        }
    }

    fn copy_url(&self, cx: &mut Context<Self>) {
        if let LoginModalState::WaitingForBrowser {
            verification_url: Some(url),
            ..
        } = &self.state
        {
            cx.write_to_clipboard(ClipboardItem::new_string(url.clone()));
        }
    }

    fn open_auth_profiles_settings(&mut self, cx: &mut Context<Self>) {
        cx.emit(LoginModalEvent::OpenAuthProfilesSettings);
    }
}

impl EventEmitter<LoginModalEvent> for LoginModal {}

impl Render for LoginModal {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let theme = cx.theme();

        let entity = cx.entity().downgrade();
        let close = move |_window: &mut Window, cx: &mut App| {
            entity.update(cx, |this, cx| this.close(cx)).ok();
        };

        let mut frame = ModalFrame::new("sso-login-modal", &self.focus_handle, close)
            .title(dbflux_i18n::t!("login.window_title"))
            .icon(AppIcon::Lock)
            .width(px(640.0))
            .max_height(px(500.0));

        frame = match &self.state {
            LoginModalState::WaitingForBrowser {
                provider_name,
                profile_name,
                verification_url,
                launch_error,
                started_at,
            } => {
                let has_url = verification_url.is_some();
                let elapsed = started_at.elapsed().as_secs();
                let url_display = verification_url
                    .clone()
                    .unwrap_or_else(|| dbflux_i18n::t!("login.error.no_url_provided"));

                frame.child(
                    div()
                        .flex()
                        .flex_col()
                        .gap(Spacing::MD)
                        .p(Spacing::MD)
                        .child(Text::body(login_sign_in_prompt(
                            provider_name,
                            profile_name,
                        )))
                        .child(Text::caption(dbflux_i18n::t!("login.body.instructions")))
                        .child(
                            div()
                                .p(Spacing::SM)
                                .rounded(Radii::SM)
                                .border_1()
                                .border_color(theme.border)
                                .bg(theme.secondary)
                                .child(Text::caption(dbflux_i18n::t!("login.field.start_url")))
                                .child(div().mt_1().child(Text::body(url_display))),
                        )
                        .when_some(launch_error.clone(), |el, error| {
                            el.child(Text::caption(error).warning())
                        })
                        .child(Text::caption(login_elapsed_message(elapsed)))
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .justify_end()
                                .gap(Spacing::SM)
                                .child(
                                    Button::new(
                                        "sso-open-browser",
                                        dbflux_i18n::t!("login.action.open_browser"),
                                    )
                                    .when(has_url, |b| b.primary())
                                    .on_click(cx.listener(
                                        |this, _, _, cx| {
                                            this.open_browser(cx);
                                        },
                                    )),
                                )
                                .child(
                                    Button::new(
                                        "sso-copy-url",
                                        dbflux_i18n::t!("login.action.copy_url"),
                                    )
                                    .on_click(cx.listener(
                                        |this, _, _, cx| {
                                            this.copy_url(cx);
                                        },
                                    )),
                                )
                                .child(
                                    Button::new(
                                        "sso-cancel",
                                        dbflux_i18n::t!("login.action.cancel"),
                                    )
                                    .on_click(cx.listener(
                                        |this, _, _, cx| {
                                            this.close(cx);
                                        },
                                    )),
                                ),
                        ),
                )
            }
            LoginModalState::Failed {
                error,
                provider_name,
            } => {
                let show_auth_profiles_button =
                    failed_state_shows_open_auth_profiles_button(provider_name.as_deref());

                let error_content = div()
                    .p(Spacing::MD)
                    .flex()
                    .flex_col()
                    .gap(Spacing::MD)
                    .child(Text::body(dbflux_i18n::t!("login.banner.connection_failed")).warning())
                    .child(Text::body(error.clone()))
                    .child(
                        div().flex().justify_end().child(
                            Button::new("sso-failed-close", dbflux_i18n::t!("login.action.close"))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.close(cx);
                                })),
                        ),
                    );

                if show_auth_profiles_button {
                    frame.child(error_content).child(
                        div().flex().justify_end().child(
                            Button::new(
                                "login-open-auth-profiles",
                                dbflux_i18n::t!("login.action.open_auth_profiles"),
                            )
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.open_auth_profiles_settings(cx);
                            })),
                        ),
                    )
                } else {
                    frame.child(error_content)
                }
            }
            LoginModalState::Success => frame.child(
                div()
                    .p(Spacing::MD)
                    .flex()
                    .flex_col()
                    .gap(Spacing::MD)
                    .child(Text::body(dbflux_i18n::t!("login.banner.completed")).success())
                    .child(Text::body(dbflux_i18n::t!("login.banner.closing"))),
            ),
            _ => frame,
        };

        frame.render(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[::core::prelude::v1::test]
    fn failed_state_offers_auth_profiles_recovery_for_provider_backed_login() {
        assert!(failed_state_shows_open_auth_profiles_button(Some(
            "Custom OIDC"
        )));
        assert!(!failed_state_shows_open_auth_profiles_button(None));
    }

    const LOGIN_CATALOG_KEYS: &[&str] = &[
        "login.window_title",
        "login.field.start_url",
        "login.action.open_browser",
        "login.action.copy_url",
        "login.action.cancel",
        "login.action.close",
        "login.action.open_auth_profiles",
        "login.banner.connection_failed",
        "login.banner.completed",
        "login.banner.closing",
        "login.body.sign_in_prompt",
        "login.body.instructions",
        "login.body.elapsed",
        "login.body.browser_open_failed",
        "login.error.timed_out",
        "login.error.no_url",
        "login.error.no_url_provided",
    ];

    #[::core::prelude::v1::test]
    fn login_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in LOGIN_CATALOG_KEYS {
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

    #[::core::prelude::v1::test]
    fn login_window_title_differs_between_locales() {
        let english = dbflux_i18n::t!("login.window_title", locale = "en");
        let spanish = dbflux_i18n::t!("login.window_title", locale = "es");

        assert_eq!(english, "Connection Flow");
        assert_eq!(spanish, "Flujo de conexión");
        assert_ne!(english, spanish);
    }
}
