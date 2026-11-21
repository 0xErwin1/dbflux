use super::*;
use dbflux_app::updates::{self, StartupDialog};
use dbflux_ui_base::updates::{UpdateDialogRequest, run_update_check};

/// Welcome shows only when a scheduler exists and its disclosure was never
/// dismissed. `None` means telemetry never initialized, and reading that as
/// "never disclosed" reopened Welcome on every launch, forever.
#[cfg(feature = "telemetry")]
fn telemetry_undisclosed(disclosure_dismissed: Option<bool>) -> bool {
    disclosure_dismissed == Some(false)
}

#[cfg(feature = "telemetry")]
fn telemetry_startup_dialog(dialog: StartupDialog, undisclosed: bool) -> StartupDialog {
    if undisclosed {
        StartupDialog::Welcome
    } else {
        dialog
    }
}

/// The dialog this start opens: the stored preferences decide, except that an
/// install which never acknowledged the telemetry disclosure always gets
/// Welcome, whatever those preferences say.
#[cfg(feature = "telemetry")]
fn startup_dialog_for_disclosure(
    settings: &updates::UpdateSettings,
    current_version: &str,
    disclosure_dismissed: Option<bool>,
) -> StartupDialog {
    telemetry_startup_dialog(
        updates::startup_dialog(settings, current_version),
        telemetry_undisclosed(disclosure_dismissed),
    )
}

/// Welcome displacing a pending "What is new" must not consume it: recording
/// the running version as already run would drop that release's notes, since
/// dismissing the disclosure never reopens them. Keeping the previous version
/// makes the notes open on the launch after the disclosure is confirmed.
#[cfg(feature = "telemetry")]
fn record_startup_keeping_whats_new(
    settings: &updates::UpdateSettings,
    current_version: &str,
    shown: &StartupDialog,
) -> updates::UpdateSettings {
    let mut recorded = updates::record_startup(settings, current_version, shown);
    let displaced = *shown == StartupDialog::Welcome
        && matches!(
            updates::startup_dialog(settings, current_version),
            StartupDialog::WhatsNew { .. }
        );
    if displaced {
        recorded.last_run_version = settings.last_run_version.clone();
    }
    recorded
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
                                workspace.app_state.update(cx, |state, cx| {
                                    let completed = state.telemetry_mut().map(|scheduler| {
                                        scheduler.complete(completion, chrono::Utc::now())
                                    });
                                    // An applied completion may have confirmed a
                                    // report; open Settings sections refresh theirs.
                                    if matches!(
                                        completed,
                                        Some(Ok(
                                            dbflux_app::telemetry::CompletionDisposition::Applied
                                        ))
                                    ) {
                                        cx.emit(dbflux_ui_base::AppStateChanged);
                                    }
                                    completed
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
        #[cfg(not(feature = "telemetry"))]
        let dialog = updates::startup_dialog(&settings, current_version);
        #[cfg(feature = "telemetry")]
        let dialog = {
            self.start_telemetry(cx);
            startup_dialog_for_disclosure(
                &settings,
                current_version,
                self.app_state
                    .read(cx)
                    .telemetry()
                    .map(|scheduler| scheduler.is_disclosure_dismissed()),
            )
        };

        #[cfg(feature = "telemetry")]
        let recorded = record_startup_keeping_whats_new(&settings, current_version, &dialog);
        #[cfg(not(feature = "telemetry"))]
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

#[cfg(test)]
#[cfg(feature = "telemetry")]
mod tests;
