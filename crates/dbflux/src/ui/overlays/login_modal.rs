use crate::ui::components::modal_frame::ModalFrame;
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Radii, Spacing};
use dbflux_core::PipelineState;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

#[derive(Debug, Clone)]
pub enum LoginModalState {
    Idle,
    WaitingForBrowser {
        provider_name: String,
        profile_name: String,
        verification_url: Option<String>,
        launch_error: Option<String>,
    },
    Success,
    Failed {
        error: String,
    },
    Cancelled,
}

pub struct LoginModal {
    visible: bool,
    state: LoginModalState,
    focus_handle: FocusHandle,
}

impl LoginModal {
    pub fn new(_window: &mut Window, cx: &mut Context<Self>) -> Self {
        Self {
            visible: false,
            state: LoginModalState::Idle,
            focus_handle: cx.focus_handle(),
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
                self.state = LoginModalState::WaitingForBrowser {
                    provider_name: provider_name.clone(),
                    profile_name: profile_name.to_string(),
                    verification_url: verification_url.clone(),
                    launch_error: None,
                };
                self.focus_handle.focus(window);
            }
            PipelineState::Failed { stage, error } => {
                self.visible = true;
                self.state = LoginModalState::Failed {
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
                    self.visible = false;
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
                Err(error) => {
                    *launch_error = Some(format!(
                        "Could not open browser automatically. Open the URL manually. ({})",
                        error
                    ));
                }
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
}

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
            } => {
                let has_url = verification_url.is_some();
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
                                .child("Login can take up to 5 minutes. Keep this window open while completing SSO in your browser."),
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
            LoginModalState::Failed { error } => frame.child(
                div()
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
                    ),
            ),
            _ => frame,
        };

        frame.render(cx)
    }
}
