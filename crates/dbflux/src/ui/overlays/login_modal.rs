use crate::ui::components::modal_frame::ModalFrame;
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Radii, Spacing};
use dbflux_core::PipelineState;
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
    OpenSsoWizard,
}

pub struct LoginModal {
    visible: bool,
    state: LoginModalState,
    focus_handle: FocusHandle,
    last_provider_name: Option<String>,
    timeout_generation: u64,
    success_generation: u64,
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
                            error: "Login timed out after 5 minutes".to_string(),
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
                *launch_error = Some("No login URL is available for this provider.".to_string());
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
                        *launch_error = Some(format!(
                            "Could not open browser automatically. Open the URL manually. ({}; fallback failed: {})",
                            error, detached_error
                        ));
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

    fn open_sso_wizard(&mut self, cx: &mut Context<Self>) {
        cx.emit(LoginModalEvent::OpenSsoWizard);
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
            .title("Connection Flow")
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
                    .unwrap_or_else(|| "Login URL not provided by provider".to_string());

                frame.child(
                    div()
                        .flex()
                        .flex_col()
                        .gap(Spacing::MD)
                        .p(Spacing::MD)
                        .child(
                            div()
                                .text_size(FontSizes::SM)
                                .text_color(theme.foreground)
                                .child(format!(
                                    "Sign in with {} to continue connecting \"{}\".",
                                    provider_name, profile_name
                                )),
                        )
                        .child(
                            div()
                                .text_size(FontSizes::XS)
                                .text_color(theme.muted_foreground)
                                .child(
                                    "Open the login URL in your browser and finish authentication. DBFlux will continue automatically once the login completes.",
                                ),
                        )
                        .child(
                            div()
                                .p(Spacing::SM)
                                .rounded(Radii::SM)
                                .border_1()
                                .border_color(theme.border)
                                .bg(theme.secondary)
                                .child(
                                    div()
                                        .text_size(FontSizes::XS)
                                        .text_color(theme.muted_foreground)
                                        .child("Start URL"),
                                )
                                .child(
                                    div()
                                        .mt_1()
                                        .text_size(FontSizes::SM)
                                        .text_color(theme.foreground)
                                        .child(url_display),
                                ),
                        )
                        .when_some(launch_error.clone(), |el, error| {
                            el.child(
                                div()
                                    .text_size(FontSizes::XS)
                                    .text_color(theme.warning)
                                    .child(error),
                            )
                        })
                        .child(
                            div()
                                .text_size(FontSizes::XS)
                                .text_color(theme.muted_foreground)
                                .child(format!(
                                    "Login can take up to 5 minutes. Elapsed: {}s",
                                    elapsed
                                )),
                        )
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .justify_end()
                                .gap(Spacing::SM)
                                .child(
                                    div()
                                        .id("sso-open-browser")
                                        .flex()
                                        .items_center()
                                        .gap(Spacing::XS)
                                        .px(Spacing::MD)
                                        .py(Spacing::SM)
                                        .rounded(Radii::SM)
                                        .cursor_pointer()
                                        .bg(if has_url { theme.primary } else { theme.secondary })
                                        .text_size(FontSizes::SM)
                                        .text_color(if has_url {
                                            theme.primary_foreground
                                        } else {
                                            theme.muted_foreground
                                        })
                                        .hover(|d| d.opacity(0.9))
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.open_browser(cx);
                                        }))
                                        .child(svg().path(AppIcon::Link2.path()).size_4())
                                        .child("Open Browser"),
                                )
                                .child(
                                    div()
                                        .id("sso-copy-url")
                                        .flex()
                                        .items_center()
                                        .gap(Spacing::XS)
                                        .px(Spacing::MD)
                                        .py(Spacing::SM)
                                        .rounded(Radii::SM)
                                        .cursor_pointer()
                                        .bg(theme.secondary)
                                        .hover(|d| d.bg(theme.muted))
                                        .text_size(FontSizes::SM)
                                        .text_color(theme.foreground)
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.copy_url(cx);
                                        }))
                                        .child(svg().path(AppIcon::Copy.path()).size_4())
                                        .child("Copy URL"),
                                )
                                .child(
                                    div()
                                        .id("sso-cancel")
                                        .flex()
                                        .items_center()
                                        .gap(Spacing::XS)
                                        .px(Spacing::MD)
                                        .py(Spacing::SM)
                                        .rounded(Radii::SM)
                                        .cursor_pointer()
                                        .bg(theme.secondary)
                                        .hover(|d| d.bg(theme.muted))
                                        .text_size(FontSizes::SM)
                                        .text_color(theme.foreground)
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.close(cx);
                                        }))
                                        .child("Cancel"),
                                ),
                        ),
                )
            }
            LoginModalState::Failed {
                error,
                provider_name,
            } => {
                let show_sso_wizard_button = provider_name
                    .as_ref()
                    .is_some_and(|p| p.contains("AWS SSO"));

                let error_content = div()
                    .p(Spacing::MD)
                    .flex()
                    .flex_col()
                    .gap(Spacing::MD)
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .text_color(theme.warning)
                            .child("Connection failed"),
                    )
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .text_color(theme.foreground)
                            .child(error.clone()),
                    )
                    .child(
                        div().flex().justify_end().child(
                            div()
                                .id("sso-failed-close")
                                .px(Spacing::MD)
                                .py(Spacing::SM)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .bg(theme.secondary)
                                .hover(|d| d.bg(theme.muted))
                                .text_size(FontSizes::SM)
                                .text_color(theme.foreground)
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.close(cx);
                                }))
                                .child("Close"),
                        ),
                    );

                if show_sso_wizard_button {
                    frame.child(error_content).child(
                        div().flex().justify_end().child(
                            div()
                                .id("sso-open-wizard")
                                .px(Spacing::MD)
                                .py(Spacing::SM)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .bg(theme.secondary)
                                .hover(|d| d.bg(theme.muted))
                                .text_size(FontSizes::SM)
                                .text_color(theme.foreground)
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.open_sso_wizard(cx);
                                }))
                                .child("Open AWS SSO Wizard"),
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
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .text_color(theme.success)
                            .child("Login completed"),
                    )
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .text_color(theme.foreground)
                            .child("Authentication succeeded. Closing this dialog..."),
                    ),
            ),
            _ => frame,
        };

        frame.render(cx)
    }
}
