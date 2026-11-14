use super::*;
use dbflux_app::updates::{self, StartupDialog};
use dbflux_ui_base::updates::{UpdateDialogRequest, run_update_check};

#[cfg(feature = "telemetry")]
fn telemetry_startup_dialog(dialog: StartupDialog, undisclosed: bool) -> StartupDialog {
    if undisclosed {
        StartupDialog::Welcome
    } else {
        dialog
    }
}

#[cfg(all(test, feature = "telemetry"))]
mod welcome_tests {
    use super::{StartupDialog, telemetry_startup_dialog, updates};
    use dbflux_app::updates::UpdateSettings;

    #[test]
    fn welcome_undisclosed_install_overrides_disabled_whats_new() {
        assert!(matches!(
            telemetry_startup_dialog(StartupDialog::None, true),
            StartupDialog::Welcome
        ));
        assert!(matches!(
            telemetry_startup_dialog(StartupDialog::None, false),
            StartupDialog::None
        ));
        assert!(matches!(
            telemetry_startup_dialog(StartupDialog::WhatsNew { since: None }, true),
            StartupDialog::Welcome
        ));
    }

    #[test]
    fn welcome_undisclosed_install_overrides_every_startup_dialog() {
        let since = updates::parse_version("0.8.0");
        assert!(since.is_some());

        for dialog in [
            StartupDialog::None,
            StartupDialog::Welcome,
            StartupDialog::WhatsNew { since: None },
            StartupDialog::WhatsNew { since },
        ] {
            assert_eq!(
                telemetry_startup_dialog(dialog.clone(), true),
                StartupDialog::Welcome,
                "undisclosed over {dialog:?}"
            );
            assert_eq!(
                telemetry_startup_dialog(dialog.clone(), false),
                dialog,
                "a dismissed disclosure keeps the stored decision"
            );
        }
    }

    /// Every stored preference that suppresses a startup dialog, combined
    /// with every version history: none of them hides the disclosure.
    #[test]
    fn welcome_undisclosed_install_ignores_every_suppressing_preference() {
        const RUNNING: &str = "0.9.0";

        for welcome_shown in [false, true] {
            for show_whats_new_after_update in [false, true] {
                for check_for_updates_on_startup in [false, true] {
                    for last_run_version in [None, Some("0.8.0"), Some(RUNNING), Some("invalid")] {
                        let settings = UpdateSettings {
                            check_for_updates_on_startup,
                            show_whats_new_after_update,
                            last_run_version: last_run_version.map(str::to_owned),
                            skipped_update_version: Some(RUNNING.to_owned()),
                            welcome_shown,
                        };
                        let stored = updates::startup_dialog(&settings, RUNNING);

                        assert_eq!(
                            telemetry_startup_dialog(stored.clone(), true),
                            StartupDialog::Welcome,
                            "undisclosed with {settings:?}"
                        );
                        assert_eq!(
                            telemetry_startup_dialog(stored.clone(), false),
                            stored,
                            "disclosed with {settings:?}"
                        );
                    }
                }
            }
        }

        let suppressed = UpdateSettings {
            check_for_updates_on_startup: false,
            show_whats_new_after_update: false,
            last_run_version: Some(RUNNING.to_owned()),
            skipped_update_version: None,
            welcome_shown: true,
        };
        assert_eq!(
            updates::startup_dialog(&suppressed, RUNNING),
            StartupDialog::None,
            "the stored preferences alone open nothing"
        );
    }
}

#[cfg(feature = "telemetry")]
impl TelemetryWake {
    pub(in crate::ui::views::workspace) fn wake(&self) {
        self.pending.set(true);
        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }
}

#[cfg(feature = "telemetry")]
impl Workspace {
    fn start_telemetry(&mut self, cx: &mut Context<Self>) {
        use dbflux_app::telemetry::{Preparation, TelemetryConfig};
        if self._telemetry_task.is_some() {
            return;
        }
        let isolated = cfg!(debug_assertions)
            && std::env::var("DBFLUX_TELEMETRY_ISOLATED").as_deref() == Ok("1");
        let initialized = TelemetryConfig::from_environment(isolated).and_then(|config| {
            self.app_state.update(cx, |state, _| {
                state.initialize_telemetry(config, chrono::Utc::now())
            })
        });
        if initialized.is_err() {
            log::warn!("telemetry.startup_unavailable");
            return;
        }
        let locale = dbflux_i18n::detect_system_locale().unwrap_or_else(|| "und".to_owned());
        let wake = self.telemetry_wake.clone();
        self._telemetry_task = Some(cx.spawn(async move |workspace, cx| {
            let mut failure_logged = false;
            loop {
                let prepared = workspace.update(cx, |workspace, cx| {
                    workspace.app_state.update(cx, |state, _| {
                        state
                            .telemetry_mut()
                            .map(|scheduler| scheduler.prepare(chrono::Utc::now(), &locale))
                    })
                });
                let job = match prepared {
                    Ok(Some(Ok(Preparation::Job(job)))) => Some(job),
                    Ok(Some(Ok(Preparation::Skipped(_)))) => None,
                    Ok(_) => {
                        if !failure_logged {
                            log::warn!("telemetry.prepare_unavailable");
                            failure_logged = true;
                        }
                        None
                    }
                    Err(_) => break,
                };
                if let Some(job) = job {
                    let result = cx
                        .background_executor()
                        .spawn(async move {
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.attempt()))
                        })
                        .await;
                    match result {
                        Ok(completion) => {
                            let completed = workspace.update(cx, |workspace, cx| {
                                workspace.app_state.update(cx, |state, _| {
                                    state.telemetry_mut().map(|scheduler| {
                                        scheduler.complete(completion, chrono::Utc::now())
                                    })
                                })
                            });
                            match completed {
                                Err(_) => break,
                                Ok(Some(Ok(_))) => {}
                                Ok(_) => {
                                    if !failure_logged {
                                        log::warn!("telemetry.complete_unavailable");
                                        failure_logged = true;
                                    }
                                }
                            }
                        }
                        Err(_) => log::warn!("telemetry.worker_panicked"),
                    }
                }
                let mut timer = std::pin::pin!(
                    cx.background_executor()
                        .timer(std::time::Duration::from_secs(60))
                );
                std::future::poll_fn(|context| {
                    use std::future::Future;
                    *wake.waker.borrow_mut() = Some(context.waker().clone());
                    if wake.pending.replace(false) {
                        return std::task::Poll::Ready(());
                    }
                    timer.as_mut().poll(context)
                })
                .await;
                wake.waker.borrow_mut().take();
            }
        }));
    }

    pub(in crate::ui::views::workspace) fn dismiss_telemetry_disclosure(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
        let result = self.app_state.update(cx, |state, _| {
            state
                .telemetry_mut()
                .ok_or(dbflux_app::telemetry::TelemetryError::Storage)?
                .disclosure_dismissed(chrono::Utc::now())
        });
        match result {
            Ok(()) => self.telemetry_wake.wake(),
            Err(_) => report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("updates.welcome.telemetry_save_failed"),
                ),
                cx,
            ),
        }
    }
}

impl Workspace {
    /// Runs once per app start, for the main window only: opens the welcome
    /// or "What is new" dialog when due, records this start, and checks for
    /// a newer release when the user allows it.
    pub fn start_update_flow(&mut self, cx: &mut Context<Self>) {
        let settings = self.app_state.read(cx).update_settings().clone();
        let current_version = updates::current_version();
        let dialog = updates::startup_dialog(&settings, current_version);
        #[cfg(feature = "telemetry")]
        let dialog = {
            self.start_telemetry(cx);
            let undisclosed = self
                .app_state
                .read(cx)
                .telemetry()
                .is_none_or(|scheduler| !scheduler.is_disclosure_dismissed());
            telemetry_startup_dialog(dialog, undisclosed)
        };

        let recorded = updates::record_startup(&settings, current_version, &dialog);
        if recorded != settings {
            let result = self
                .app_state
                .update(cx, |state, _| state.set_update_settings(recorded));
            if let Err(error) = result {
                log::warn!("Failed to record this start in the update settings: {error}");
            }
        }

        match dialog {
            StartupDialog::Welcome => self.open_update_dialog(UpdateDialogRequest::Welcome, cx),
            StartupDialog::WhatsNew { since } => self.open_update_dialog(
                UpdateDialogRequest::WhatsNew {
                    since: since.map(|version| version.to_string()),
                },
                cx,
            ),
            StartupDialog::None => {}
        }

        if settings.check_for_updates_on_startup {
            run_update_check(&self.app_state, cx);
        }
    }

    /// Opens the dialog another window asked for, if no other workspace took
    /// it first.
    pub(in crate::ui::views::workspace) fn open_requested_update_dialog(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        let request = self
            .app_state
            .update(cx, |state, _| state.pending_update_dialog.take());

        if let Some(request) = request {
            self.open_update_dialog(request, cx);
        }
    }

    fn open_update_dialog(&mut self, request: UpdateDialogRequest, cx: &mut Context<Self>) {
        match request {
            UpdateDialogRequest::Welcome => {
                self.whats_new_dialog
                    .update(cx, |dialog, cx| dialog.close(cx));
                self.welcome_dialog.update(cx, |dialog, cx| dialog.open(cx));
            }
            UpdateDialogRequest::WhatsNew { since } => {
                self.welcome_dialog.update(cx, |dialog, cx| dialog.hide(cx));
                self.whats_new_dialog
                    .update(cx, |dialog, cx| dialog.open(since, cx));
            }
        }

        cx.notify();
    }
}
