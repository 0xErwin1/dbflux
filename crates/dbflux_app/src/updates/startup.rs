//! Which dialog, if any, opens when the app starts.

use std::cmp::Ordering;

use semver::Version;

use super::{UpdateSettings, parse_version};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupDialog {
    None,
    /// First run: the welcome dialog, never "What is new".
    Welcome,
    /// First start of a newer version: every release after `since`, or only
    /// the running release when no previous version was recorded (an install
    /// that predates the version bookkeeping).
    WhatsNew {
        since: Option<Version>,
    },
}

/// Decides the startup dialog from the stored settings and the running
/// version.
///
/// The welcome dialog wins until it has been shown once. After that, "What is
/// new" opens when the setting is on and the running version is newer than
/// the one recorded at the previous start. Build metadata (the nightly commit)
/// does not count as newer. An existing install upgraded into the version
/// bookkeeping has the welcome marked as shown but no recorded version; it
/// gets "What is new" for the running release, once.
pub fn startup_dialog(settings: &UpdateSettings, current_version: &str) -> StartupDialog {
    if !settings.welcome_shown {
        return StartupDialog::Welcome;
    }

    if !settings.show_whats_new_after_update {
        return StartupDialog::None;
    }

    let Some(current) = parse_version(current_version) else {
        return StartupDialog::None;
    };
    let Some(last_run) = settings.last_run_version.as_deref() else {
        return StartupDialog::WhatsNew { since: None };
    };
    let Some(since) = parse_version(last_run) else {
        return StartupDialog::None;
    };

    if current.cmp_precedence(&since) == Ordering::Greater {
        StartupDialog::WhatsNew { since: Some(since) }
    } else {
        StartupDialog::None
    }
}

/// The settings to persist once the startup decision has been acted on: the
/// running version becomes the last run version, and the welcome dialog is
/// marked as shown when it was the one opened.
pub fn record_startup(
    settings: &UpdateSettings,
    current_version: &str,
    dialog: &StartupDialog,
) -> UpdateSettings {
    UpdateSettings {
        last_run_version: Some(current_version.to_string()),
        welcome_shown: settings.welcome_shown || *dialog == StartupDialog::Welcome,
        ..settings.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(
        last_run: Option<&str>,
        welcome_shown: bool,
        show_whats_new: bool,
    ) -> UpdateSettings {
        UpdateSettings {
            last_run_version: last_run.map(str::to_string),
            welcome_shown,
            show_whats_new_after_update: show_whats_new,
            ..UpdateSettings::default()
        }
    }

    #[test]
    fn first_run_shows_welcome_not_whats_new() {
        assert_eq!(
            startup_dialog(&settings(None, false, true), "0.8.0"),
            StartupDialog::Welcome
        );
        assert_eq!(
            startup_dialog(&settings(Some("0.7.0"), false, true), "0.8.0"),
            StartupDialog::Welcome
        );
    }

    #[test]
    fn newer_version_shows_whats_new_once() {
        let upgraded = settings(Some("0.7.0"), true, true);
        assert_eq!(
            startup_dialog(&upgraded, "0.8.0"),
            StartupDialog::WhatsNew {
                since: Some(Version::new(0, 7, 0))
            }
        );

        let recorded = record_startup(&upgraded, "0.8.0", &startup_dialog(&upgraded, "0.8.0"));
        assert_eq!(recorded.last_run_version.as_deref(), Some("0.8.0"));
        assert_eq!(startup_dialog(&recorded, "0.8.0"), StartupDialog::None);
    }

    #[test]
    fn same_older_or_unparseable_versions_show_nothing() {
        assert_eq!(
            startup_dialog(&settings(Some("0.8.0"), true, true), "0.8.0"),
            StartupDialog::None
        );
        assert_eq!(
            startup_dialog(&settings(Some("0.9.0"), true, true), "0.8.0"),
            StartupDialog::None
        );
        assert_eq!(
            startup_dialog(&settings(Some("garbage"), true, true), "0.8.0"),
            StartupDialog::None
        );
        assert_eq!(
            startup_dialog(
                &settings(Some("0.9.0-nightly+aaa1111"), true, true),
                "0.9.0-nightly+bbb2222"
            ),
            StartupDialog::None
        );
    }

    #[test]
    fn disabled_setting_suppresses_whats_new_but_still_records_the_version() {
        let upgraded = settings(Some("0.7.0"), true, false);
        let dialog = startup_dialog(&upgraded, "0.8.0");
        assert_eq!(dialog, StartupDialog::None);

        let recorded = record_startup(&upgraded, "0.8.0", &dialog);
        assert_eq!(recorded.last_run_version.as_deref(), Some("0.8.0"));
    }

    #[test]
    fn existing_install_without_a_recorded_version_sees_the_running_release_once() {
        let upgraded = settings(None, true, true);
        let dialog = startup_dialog(&upgraded, "0.8.0");
        assert_eq!(dialog, StartupDialog::WhatsNew { since: None });

        let recorded = record_startup(&upgraded, "0.8.0", &dialog);
        assert_eq!(recorded.last_run_version.as_deref(), Some("0.8.0"));
        assert_eq!(startup_dialog(&recorded, "0.8.0"), StartupDialog::None);

        let disabled = settings(None, true, false);
        assert_eq!(startup_dialog(&disabled, "0.8.0"), StartupDialog::None);
    }

    #[test]
    fn recording_welcome_marks_it_shown() {
        let fresh = settings(None, false, true);
        let recorded = record_startup(&fresh, "0.8.0", &StartupDialog::Welcome);

        assert!(recorded.welcome_shown);
        assert_eq!(recorded.last_run_version.as_deref(), Some("0.8.0"));
        assert_eq!(startup_dialog(&recorded, "0.8.0"), StartupDialog::None);
    }
}
