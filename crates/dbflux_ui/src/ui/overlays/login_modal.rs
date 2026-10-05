use crate::ui::icons::AppIcon;
use crate::ui::labels::{
    login_browser_open_failed_message, login_progress_label, login_sign_in_prompt, login_user_code,
};
use dbflux_components::controls::Button;
use dbflux_components::modals::{Modal, modal_field, modal_lead, modal_value_field};
use dbflux_components::primitives::{BannerBlock, BannerVariant, Spinner};
use dbflux_components::tokens::{ChromeColors, ModalMetrics};
use dbflux_core::PipelineState;
use dbflux_core::keymap_types::ContextId;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use std::time::{Duration, Instant};

/// Width of the sign-in dialog (P1Flows).
const LOGIN_MODAL_WIDTH: Pixels = px(560.0);

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
    spinner_frame: usize,
    _spinner_task: Option<Task<()>>,
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
            spinner_frame: 0,
            _spinner_task: None,
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
                self.focus_handle.focus(window, cx);
                self.schedule_timeout(cx);
                self.start_spinner(cx);
            }
            PipelineState::Failed { stage, error } => {
                self.visible = true;
                self.state = LoginModalState::Failed {
                    provider_name: self.last_provider_name.clone(),
                    error: format!("{}: {}", stage, error),
                };
                self.focus_handle.focus(window, cx);
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

            cx.update(|cx| {
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

    /// Advances the waiting indicator every `Spinner::INTERVAL_MS` while the
    /// modal waits for the browser. Replacing the stored task cancels a loop
    /// left over from an earlier login, so the spinner never runs twice as
    /// fast. The elapsed caption reads whole seconds from `started_at`, so
    /// these extra renders cannot move it ahead of the wall clock.
    fn start_spinner(&mut self, cx: &mut Context<Self>) {
        self.spinner_frame = 0;

        self._spinner_task = Some(cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor()
                    .timer(Duration::from_millis(Spinner::INTERVAL_MS))
                    .await;

                let still_waiting = this
                    .update(cx, |modal, cx| {
                        let waiting = modal.visible
                            && matches!(modal.state, LoginModalState::WaitingForBrowser { .. });

                        if waiting {
                            modal.spinner_frame = Spinner::next_frame(modal.spinner_frame);
                            cx.notify();
                        }

                        waiting
                    })
                    .unwrap_or(false);

                if !still_waiting {
                    break;
                }
            }
        }));
    }

    fn schedule_success_close(&mut self, cx: &mut Context<Self>) {
        self.success_generation += 1;
        let generation = self.success_generation;
        let this = cx.entity().clone();

        cx.spawn(async move |_entity, cx| {
            cx.background_executor()
                .timer(LOGIN_SUCCESS_AUTO_CLOSE_DELAY)
                .await;

            cx.update(|cx| {
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
        self.focus_handle.focus(window, cx);
        self.schedule_timeout(cx);
        self.start_spinner(cx);
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

/// `text` with each of `names` drawn in the strong color, for the sign-in
/// sentence that names the provider and the connection.
fn emphasized(text: String, names: &[&str], cx: &App) -> StyledText {
    let strong = ChromeColors::strong(cx.theme());

    let mut highlights: Vec<(std::ops::Range<usize>, HighlightStyle)> = names
        .iter()
        .filter(|name| !name.is_empty())
        .filter_map(|name| text.find(name).map(|start| start..start + name.len()))
        .map(|range| {
            (
                range,
                HighlightStyle {
                    color: Some(strong),
                    ..Default::default()
                },
            )
        })
        .collect();

    highlights.sort_by_key(|(range, _)| range.start);
    highlights.dedup_by(|later, earlier| later.0.start < earlier.0.end);

    StyledText::new(text).with_highlights(highlights)
}

impl LoginModal {
    fn render_waiting_body(
        &self,
        provider_name: &str,
        profile_name: &str,
        verification_url: Option<&String>,
        launch_error: Option<&String>,
        started_at: Instant,
        cx: &mut Context<Self>,
    ) -> Div {
        let theme = cx.theme();
        let elapsed = started_at
            .elapsed()
            .as_secs()
            .min(SSO_LOGIN_TIMEOUT.as_secs());
        let fraction = elapsed as f32 / SSO_LOGIN_TIMEOUT.as_secs() as f32;

        let user_code = verification_url.and_then(|url| login_user_code(url));

        let copy_button = verification_url.is_some().then(|| {
            Button::new(
                "sso-copy-url-inline",
                dbflux_i18n::t!("login.action.copy_url"),
            )
            .ghost()
            .inline()
            .icon(AppIcon::Copy)
            .icon_only()
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| this.copy_url(cx)))
            .into_any_element()
        });

        let url_field = modal_value_field(
            verification_url.map(|url| SharedString::from(url.clone())),
            dbflux_i18n::t!("login.error.no_url_provided"),
            copy_button,
            cx,
        );

        let progress = div()
            .id("login-waiting-indicator")
            .flex()
            .items_center()
            .gap(ModalMetrics::PROGRESS_GAP)
            .child(Spinner::new(self.spinner_frame))
            .child(
                div()
                    .flex_shrink_0()
                    .text_color(theme.foreground)
                    .child(dbflux_i18n::t!("login.body.waiting")),
            )
            .child(
                div()
                    .flex_1()
                    .h(ModalMetrics::PROGRESS_HEIGHT)
                    .bg(theme.secondary)
                    .child(
                        div()
                            .h_full()
                            .w(relative(fraction.clamp(0.0, 1.0)))
                            .bg(theme.primary),
                    ),
            )
            .child(
                div()
                    .flex_shrink_0()
                    .font_family(dbflux_components::fonts::editor_family(cx))
                    .text_size(ModalMetrics::META_FONT)
                    .text_color(theme.muted_foreground)
                    .child(login_progress_label(elapsed, SSO_LOGIN_TIMEOUT.as_secs())),
            );

        div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(modal_lead(
                emphasized(
                    login_sign_in_prompt(provider_name, profile_name),
                    &[provider_name, profile_name],
                    cx,
                ),
                cx,
            ))
            .child(modal_field(
                dbflux_i18n::t!("login.field.verification_url"),
                url_field,
                cx,
            ))
            .when_some(user_code, |body, code| {
                body.child(
                    div()
                        .flex()
                        .items_center()
                        .gap(ModalMetrics::DEVICE_CODE_GAP)
                        .child(
                            div()
                                .font_family(dbflux_components::fonts::display_family(cx))
                                .font_weight(FontWeight::BLACK)
                                .text_size(ModalMetrics::DEVICE_CODE_FONT)
                                .letter_spacing(
                                    dbflux_components::fonts::ui_px(
                                        cx,
                                        ModalMetrics::DEVICE_CODE_FONT,
                                    ) * ModalMetrics::DEVICE_CODE_TRACKING_EM,
                                )
                                .text_color(ChromeColors::strong(theme))
                                .child(code),
                        )
                        .child(
                            div()
                                .text_size(ModalMetrics::FIELD_LABEL_FONT)
                                .text_color(theme.muted_foreground)
                                .child(dbflux_i18n::t!("login.body.code_hint")),
                        ),
                )
            })
            .when_some(launch_error.cloned(), |body, error| {
                body.child(BannerBlock::new(BannerVariant::Warning, error))
            })
            .child(progress)
    }
}

impl Render for LoginModal {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let entity = cx.entity().downgrade();
        let close = move |_window: &mut Window, cx: &mut App| {
            entity.update(cx, |this, cx| this.close(cx)).ok();
        };

        let frame = Modal::new(dbflux_i18n::t!("login.window_title"))
            .id("sso-login-modal")
            .focus_handle(&self.focus_handle)
            .on_close(close)
            .key_context(ContextId::SqlPreviewModal.as_gpui_context())
            .icon(AppIcon::Lock)
            .width(LOGIN_MODAL_WIDTH);

        let frame = match self.state.clone() {
            LoginModalState::WaitingForBrowser {
                provider_name,
                profile_name,
                verification_url,
                launch_error,
                started_at,
            } => {
                let has_url = verification_url.is_some();

                let footer = div()
                    .flex()
                    .items_center()
                    .gap(ModalMetrics::FOOTER_GAP)
                    .child(
                        Button::new("sso-cancel", dbflux_i18n::t!("login.action.cancel"))
                            .on_click(cx.listener(|this, _, _, cx| this.close(cx))),
                    )
                    .child(
                        Button::new("sso-copy-url", dbflux_i18n::t!("login.action.copy_url"))
                            .icon(AppIcon::Copy)
                            .disabled(!has_url)
                            .on_click(cx.listener(|this, _, _, cx| this.copy_url(cx))),
                    )
                    .child(
                        Button::new(
                            "sso-open-browser",
                            dbflux_i18n::t!("login.action.open_browser"),
                        )
                        .primary()
                        .icon(AppIcon::ExternalLink)
                        .disabled(!has_url)
                        .on_click(cx.listener(|this, _, _, cx| this.open_browser(cx))),
                    );

                frame
                    .body(self.render_waiting_body(
                        &provider_name,
                        &profile_name,
                        verification_url.as_ref(),
                        launch_error.as_ref(),
                        started_at,
                        cx,
                    ))
                    .footer(footer)
            }
            LoginModalState::Failed {
                error,
                provider_name,
            } => {
                let show_auth_profiles_button =
                    failed_state_shows_open_auth_profiles_button(provider_name.as_deref());

                let footer = div()
                    .flex()
                    .items_center()
                    .gap(ModalMetrics::FOOTER_GAP)
                    .when(show_auth_profiles_button, |footer| {
                        footer.child(
                            Button::new(
                                "login-open-auth-profiles",
                                dbflux_i18n::t!("login.action.open_auth_profiles"),
                            )
                            .icon(AppIcon::Settings)
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.open_auth_profiles_settings(cx);
                            })),
                        )
                    })
                    .child(
                        Button::new("sso-failed-close", dbflux_i18n::t!("login.action.close"))
                            .primary()
                            .on_click(cx.listener(|this, _, _, cx| this.close(cx))),
                    );

                frame
                    .body(
                        BannerBlock::new(
                            BannerVariant::Danger,
                            dbflux_i18n::t!("login.banner.connection_failed"),
                        )
                        .with_body(error),
                    )
                    .footer(footer)
            }
            LoginModalState::Success => frame.body(
                BannerBlock::new(
                    BannerVariant::Success,
                    dbflux_i18n::t!("login.banner.completed"),
                )
                .with_body(dbflux_i18n::t!("login.banner.closing")),
            ),
            LoginModalState::Idle | LoginModalState::Cancelled => frame,
        };

        frame.into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LoginModal, SSO_LOGIN_TIMEOUT, failed_state_shows_open_auth_profiles_button,
        login_progress_label,
    };
    use crate::ui::labels::login_user_code;
    use dbflux_core::PipelineState;
    use gpui::{AccessibilityFrame, FrameObserver, TestAppContext, VisualTestContext};
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    /// Keeps the latest rendered accessibility frame of the window it observes.
    #[derive(Default)]
    struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

    impl FrameObserver for FrameCapture {
        fn accessibility_updated(&self, frame: &AccessibilityFrame) {
            *self.0.lock().expect("frame capture lock") = Some(frame.clone());
        }
    }

    fn latest_frame(capture: &FrameCapture) -> AccessibilityFrame {
        capture
            .0
            .lock()
            .expect("frame capture lock")
            .clone()
            .expect("the window rendered a frame")
    }

    fn has_node(frame: &AccessibilityFrame, id: &str) -> bool {
        frame.nodes().any(|(_, node)| node.id() == id)
    }

    fn frame_shows_text(frame: &AccessibilityFrame, text: &str) -> bool {
        frame
            .nodes()
            .any(|(_, node)| node.content_text().contains(text))
    }

    /// While the modal waits for the browser it labels the URL it shows as the
    /// verification URL and shows the waiting indicator. A failed login
    /// removes the indicator.
    #[gpui::test]
    fn waiting_state_labels_the_verification_url_and_shows_progress(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        let capture = Arc::new(FrameCapture::default());
        let capture_for_window = capture.clone();
        let (modal, visual) = cx.add_window_view(move |window, cx| {
            window.observe_frames(&capture_for_window);
            LoginModal::new(window, cx)
        });

        visual.update(|window, cx| {
            modal.update(cx, |modal, cx| {
                modal.open_manual(
                    "AWS SSO",
                    "dev",
                    Some("https://device.sso.us-east-1.amazonaws.com/".to_string()),
                    window,
                    cx,
                );
            });
            window.refresh();
        });
        visual.run_until_parked();

        let waiting = latest_frame(&capture);
        assert!(
            has_node(&waiting, "login-waiting-indicator"),
            "the waiting indicator is rendered while waiting for the browser"
        );
        assert!(frame_shows_text(
            &waiting,
            &dbflux_i18n::t!("login.field.verification_url")
        ));
        assert!(frame_shows_text(
            &waiting,
            "https://device.sso.us-east-1.amazonaws.com/"
        ));

        visual.update(|window, cx| {
            modal.update(cx, |modal, cx| {
                let failed = PipelineState::Failed {
                    stage: "Authenticating".to_string(),
                    error: "denied".to_string(),
                };
                modal.apply_pipeline_state("dev", &failed, window, cx);
            });
            window.refresh();
        });
        visual.run_until_parked();

        let failed = latest_frame(&capture);
        assert!(
            !has_node(&failed, "login-waiting-indicator"),
            "the waiting indicator is gone once the login failed"
        );
        assert!(frame_shows_text(
            &failed,
            &dbflux_i18n::t!("login.banner.connection_failed")
        ));
    }

    fn shown_elapsed_seconds(frame: &AccessibilityFrame) -> u64 {
        (0..=SSO_LOGIN_TIMEOUT.as_secs())
            .find(|seconds| {
                frame_shows_text(
                    frame,
                    &login_progress_label(*seconds, SSO_LOGIN_TIMEOUT.as_secs()),
                )
            })
            .expect("the elapsed caption is rendered")
    }

    /// The waiting indicator animates on its own timer, and the extra renders
    /// it causes do not move the elapsed caption: each spinner tick is driven
    /// by a full second of test clock, far more than the wall-clock time the
    /// test takes, and the caption never shows more seconds than have really
    /// elapsed.
    #[gpui::test]
    fn waiting_indicator_animates_without_advancing_the_elapsed_caption(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        let capture = Arc::new(FrameCapture::default());
        let capture_for_window = capture.clone();
        let (modal, visual) = cx.add_window_view(move |window, cx| {
            window.observe_frames(&capture_for_window);
            LoginModal::new(window, cx)
        });

        let wall_clock_start = Instant::now();

        visual.update(|window, cx| {
            modal.update(cx, |modal, cx| {
                modal.open_manual("AWS SSO", "dev", None, window, cx);
            });
            window.refresh();
        });
        visual.run_until_parked();

        let spinner_frame =
            |visual: &mut VisualTestContext| visual.update(|_, cx| modal.read(cx).spinner_frame);

        let first = latest_frame(&capture);
        assert!(
            has_node(&first, "login-waiting-indicator"),
            "the waiting indicator is rendered"
        );
        let mut spinner_frames = HashSet::from([spinner_frame(visual)]);
        let mut shown_seconds = vec![shown_elapsed_seconds(&first)];

        const TEST_CLOCK_PER_TICK: Duration = Duration::from_secs(1);
        let ticks = 20;
        for _ in 0..ticks {
            visual.executor().advance_clock(TEST_CLOCK_PER_TICK);
            visual.run_until_parked();

            let frame = latest_frame(&capture);
            spinner_frames.insert(spinner_frame(visual));
            shown_seconds.push(shown_elapsed_seconds(&frame));
        }

        let test_clock_advanced = TEST_CLOCK_PER_TICK * ticks;
        let wall_clock_seconds = wall_clock_start.elapsed().as_secs();

        assert!(
            spinner_frames.len() > 1,
            "the waiting indicator did not animate: {spinner_frames:?}"
        );
        assert!(
            test_clock_advanced.as_secs() > wall_clock_seconds,
            "the test ran too slowly to tell spinner ticks from wall-clock seconds"
        );
        assert!(
            shown_seconds
                .iter()
                .all(|seconds| *seconds <= wall_clock_seconds),
            "the elapsed caption ran ahead of the wall clock ({wall_clock_seconds}s): \
             {shown_seconds:?}"
        );
    }

    #[test]
    fn progress_label_counts_minutes_and_seconds() {
        assert_eq!(login_progress_label(64, 300), "1:04 / 5:00");
        assert_eq!(login_progress_label(0, 300), "0:00 / 5:00");
    }

    #[test]
    fn user_code_is_read_from_the_verification_url() {
        assert_eq!(
            login_user_code("https://device.sso.eu-west-1.amazonaws.com/?user_code=KQXR-TWPB")
                .as_deref(),
            Some("KQXR-TWPB")
        );
        assert_eq!(
            login_user_code("https://example.com/device?foo=1&user_code=ABCD").as_deref(),
            Some("ABCD")
        );
        assert_eq!(login_user_code("https://example.com/device"), None);
        assert_eq!(login_user_code("https://example.com/?user_code="), None);
    }

    #[test]
    fn failed_state_offers_auth_profiles_recovery_for_provider_backed_login() {
        assert!(failed_state_shows_open_auth_profiles_button(Some(
            "Custom OIDC"
        )));
        assert!(!failed_state_shows_open_auth_profiles_button(None));
    }

    const LOGIN_CATALOG_KEYS: &[&str] = &[
        "login.window_title",
        "login.field.verification_url",
        "login.action.open_browser",
        "login.action.copy_url",
        "login.action.cancel",
        "login.action.close",
        "login.action.open_auth_profiles",
        "login.banner.connection_failed",
        "login.banner.completed",
        "login.banner.closing",
        "login.body.sign_in_prompt",
        "login.body.waiting",
        "login.body.code_hint",
        "login.body.browser_open_failed",
        "login.error.timed_out",
        "login.error.no_url",
        "login.error.no_url_provided",
    ];

    #[test]
    fn login_keys_resolve_in_every_locale() {
        for locale in ["en", "es", "ko", "zh_Hans"] {
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

    #[test]
    fn login_window_title_differs_between_locales() {
        let english = dbflux_i18n::t!("login.window_title", locale = "en");
        let spanish = dbflux_i18n::t!("login.window_title", locale = "es");

        assert_eq!(english, "Sign in to continue");
        assert_eq!(spanish, "Inicia sesión para continuar");
        assert_ne!(english, spanish);
    }
}
