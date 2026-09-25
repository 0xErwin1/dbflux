//! UI glue for the update check: runs it off the foreground thread, records
//! the outcome on [`AppStateEntity`], and raises the one-per-version notice.
//!
//! The decisions themselves (which release is newer, what the install source
//! is, which dialog opens at startup) live in `dbflux_app::updates`; this
//! module only wires them to toasts, settings persistence and the dialog
//! request that the workspace picks up.

use dbflux_app::updates::{
    self, AvailableUpdate, InstallSource, PackageManager, UpdateCheckOutcome, UpdateCheckState,
};
use dbflux_core::ReleaseChannel;
use dbflux_core::chrono::Utc;
use gpui::{App, Entity};

use crate::app_state_entity::{AppStateChanged, AppStateEntity};
use crate::toast::{Toast, ToastAction};
use crate::user_error::{ErrorKind, UserFacingError, report_error};

/// A dialog the workspace should open on behalf of another window (Settings)
/// or of the startup flow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateDialogRequest {
    /// "What is new": every release after `since`, or the running release
    /// alone when `since` is `None`.
    WhatsNew {
        since: Option<String>,
    },
    Welcome,
}

/// Emitted when [`AppStateEntity::pending_update_dialog`] is set. Every
/// workspace hears it, and the first one to `take()` the request opens it.
#[derive(Debug, Clone, Copy)]
pub struct UpdateDialogRequested;

impl AppStateEntity {
    pub fn request_update_dialog(
        &mut self,
        request: UpdateDialogRequest,
        cx: &mut gpui::Context<Self>,
    ) {
        self.pending_update_dialog = Some(request);
        cx.emit(UpdateDialogRequested);
        cx.notify();
    }

    /// The newer release the status bar should advertise: the latest check
    /// found one and the user has not skipped it.
    pub fn visible_update(&self) -> Option<&AvailableUpdate> {
        let update = self.update_check().available_update()?;
        let skipped = self.update_settings().skipped_update_version.as_deref();

        (skipped != Some(update.label.as_str())).then_some(update)
    }
}

/// Asks GitHub for a newer release on a background thread and records the
/// result. A second call while one is running does nothing. Failures are only
/// logged: the check is a courtesy, never an error the user has to handle.
pub fn run_update_check(app_state: &Entity<AppStateEntity>, cx: &mut App) {
    let already_running = app_state.read(cx).update_check().in_progress;
    if already_running {
        return;
    }

    app_state.update(cx, |state, cx| {
        let mut check = state.update_check().clone();
        check.in_progress = true;
        state.set_update_check(check);
        cx.emit(AppStateChanged);
        cx.notify();
    });

    let task = cx.background_executor().spawn(async move {
        updates::fetch_available_update(ReleaseChannel::current(), updates::current_version())
    });

    let app_state = app_state.clone();
    cx.spawn(async move |cx| {
        let result = task.await;

        cx.update(|cx| {
            let outcome = match result {
                Ok(Some(update)) => UpdateCheckOutcome::Available(update),
                Ok(None) => UpdateCheckOutcome::UpToDate,
                Err(error) => {
                    log::warn!("Update check failed: {error}");
                    UpdateCheckOutcome::Failed
                }
            };

            record_outcome(&app_state, outcome, cx);
        });
    })
    .detach();
}

fn record_outcome(app_state: &Entity<AppStateEntity>, outcome: UpdateCheckOutcome, cx: &mut App) {
    let notice = app_state.update(cx, |state, cx| {
        state.set_update_check(UpdateCheckState {
            outcome: Some(outcome),
            checked_at: Some(Utc::now()),
            in_progress: false,
        });

        let notice = state
            .visible_update()
            .cloned()
            .filter(|update| state.notified_update_label.as_deref() != Some(update.label.as_str()));
        if let Some(update) = &notice {
            state.notified_update_label = Some(update.label.clone());
        }

        cx.emit(AppStateChanged);
        cx.notify();
        notice
    });

    if let Some(update) = notice {
        update_toast(
            app_state,
            &update,
            updates::install_source::current_install_source(),
        )
        .push(cx);
    }
}

/// Stores `label` as the skipped release, which hides its notice and chip.
pub fn skip_update(app_state: &Entity<AppStateEntity>, label: &str, cx: &mut App) {
    let result = app_state.update(cx, |state, cx| {
        let settings = updates::UpdateSettings {
            skipped_update_version: Some(label.to_string()),
            ..state.update_settings().clone()
        };
        let result = state.set_update_settings(settings);

        cx.emit(AppStateChanged);
        cx.notify();
        result
    });

    if let Err(error) = result {
        report_error(
            UserFacingError::new(
                ErrorKind::Storage,
                dbflux_i18n::t!("updates.toast.skip_error", error = error),
            ),
            cx,
        );
    }
}

/// The version the user runs, shown in notices and the settings status line.
pub fn running_version_label() -> String {
    updates::display_version(updates::current_version())
}

/// Lowercase channel name as printed next to the version.
pub fn channel_label(channel: ReleaseChannel) -> &'static str {
    match channel {
        ReleaseChannel::Stable => "stable",
        ReleaseChannel::Rc => "rc",
        ReleaseChannel::Nightly => "nightly",
    }
}

/// The body line of the notice: where the download opens, or how a
/// package-manager install is updated.
pub fn update_toast_body(install_source: InstallSource) -> String {
    let current = running_version_label();

    match install_source {
        InstallSource::Direct => dbflux_i18n::t!("updates.toast.download_body", current = current),
        InstallSource::PackageManager(PackageManager::Nix) => {
            dbflux_i18n::t!("updates.toast.nix_body", current = current)
        }
        InstallSource::PackageManager(PackageManager::Aur) => {
            dbflux_i18n::t!("updates.toast.aur_body", current = current)
        }
        InstallSource::PackageManager(PackageManager::Flatpak) => {
            dbflux_i18n::t!("updates.toast.flatpak_body", current = current)
        }
    }
}

/// Builds the update notice. Direct installs get Download / Release notes /
/// Skip; package-manager installs get the rebuild hint with Release notes /
/// Skip this version, since a download would bypass the package manager.
pub fn update_toast(
    app_state: &Entity<AppStateEntity>,
    update: &AvailableUpdate,
    install_source: InstallSource,
) -> Toast {
    let mut toast = Toast::info(dbflux_i18n::t!(
        "updates.toast.title",
        version = update.label
    ))
    .body(update_toast_body(install_source))
    .not_collapsible();

    let is_direct = install_source == InstallSource::Direct;

    if is_direct {
        let download_url = update.download_url.clone();
        toast = toast.action(
            ToastAction::new("update-download", dbflux_i18n::t!("updates.toast.download"))
                .primary()
                .on_click(move |cx| cx.open_url(&download_url)),
        );
    }

    let notes_url = update.notes_url.clone();
    toast = toast.action(
        ToastAction::new(
            "update-release-notes",
            dbflux_i18n::t!("updates.toast.release_notes"),
        )
        .on_click(move |cx| cx.open_url(&notes_url)),
    );

    let skip_label = if is_direct {
        dbflux_i18n::t!("updates.toast.skip")
    } else {
        dbflux_i18n::t!("updates.toast.skip_version")
    };
    let app_state = app_state.clone();
    let label = update.label.clone();
    toast.action(
        ToastAction::new("update-skip", skip_label)
            .dismisses()
            .on_click(move |cx| skip_update(&app_state, &label, cx)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn toast_body_follows_the_install_source() {
        let current = running_version_label();

        let direct = update_toast_body(InstallSource::Direct);
        assert!(direct.contains(&current));
        assert_eq!(
            direct,
            dbflux_i18n::t!("updates.toast.download_body", current = current)
        );

        for (manager, key) in [
            (PackageManager::Nix, "updates.toast.nix_body"),
            (PackageManager::Aur, "updates.toast.aur_body"),
            (PackageManager::Flatpak, "updates.toast.flatpak_body"),
        ] {
            assert_eq!(
                update_toast_body(InstallSource::PackageManager(manager)),
                dbflux_i18n::t!(key, current = current)
            );
        }
    }

    #[test]
    fn channel_labels_are_lowercase_names() {
        assert_eq!(channel_label(ReleaseChannel::Stable), "stable");
        assert_eq!(channel_label(ReleaseChannel::Rc), "rc");
        assert_eq!(channel_label(ReleaseChannel::Nightly), "nightly");
    }

    #[test]
    fn update_copy_resolves_in_every_locale() {
        for key in [
            "updates.toast.title",
            "updates.toast.download_body",
            "updates.toast.nix_body",
            "updates.toast.aur_body",
            "updates.toast.flatpak_body",
            "updates.toast.download",
            "updates.toast.release_notes",
            "updates.toast.skip",
            "updates.toast.skip_version",
            "updates.toast.skip_error",
            "updates.status_bar.available",
            "updates.status_bar.tooltip",
        ] {
            for locale in ["en", "es", "ko", "zh_Hans"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty for {locale}");
                assert_ne!(value, key, "{key} missing in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing in {locale}"
                );
            }
        }
    }
}
