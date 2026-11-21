use super::{
    StartupDialog, record_startup_keeping_whats_new, startup_dialog_for_disclosure,
    telemetry_startup_dialog, telemetry_undisclosed, updates,
};
use dbflux_app::updates::UpdateSettings;
use gpui::AppContext as _;

/// A scheduler that failed to initialize is `None`, and that must not be
/// read as "never disclosed": showing Welcome on every launch forever is
/// the bug this pins.
#[test]
fn failed_telemetry_init_does_not_reopen_welcome() {
    assert!(!telemetry_undisclosed(None));
    assert!(telemetry_undisclosed(Some(false)), "never disclosed");
    assert!(
        !telemetry_undisclosed(Some(true)),
        "already disclosed stays quiet"
    );
}

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

/// One app start over the real telemetry state: decides the dialog the
/// way `start_update_flow` does, then records the start.
fn start_once(
    app_state: &gpui::Entity<dbflux_ui_base::AppStateEntity>,
    settings: &mut UpdateSettings,
    cx: &mut gpui::TestAppContext,
) -> StartupDialog {
    cx.update(|cx| {
        app_state
            .update(cx, |state, _| {
                state.initialize_telemetry(
                    dbflux_app::telemetry::TelemetryConfig::default(),
                    dbflux_core::chrono::Utc::now(),
                )
            })
            .expect("telemetry initialized");
        let dismissed = app_state
            .read(cx)
            .telemetry()
            .map(|scheduler| scheduler.is_disclosure_dismissed());
        let dialog = startup_dialog_for_disclosure(settings, "0.11.0", dismissed);
        *settings = record_startup_keeping_whats_new(settings, "0.11.0", &dialog);
        dialog
    })
}

/// A v0.10 install whose user switched the welcome and what-is-new dialogs
/// off and has no telemetry row yet: Welcome opens once, and never again
/// after the disclosure is acknowledged, with the same preferences.
#[gpui::test]
fn v010_upgrade_sees_welcome_once_then_never_again(cx: &mut gpui::TestAppContext) {
    let app_state = crate::ui::overlays::updates::test_support::test_app_state(cx);
    let mut settings = UpdateSettings {
        check_for_updates_on_startup: true,
        show_whats_new_after_update: false,
        last_run_version: Some("0.10.0".to_owned()),
        skipped_update_version: None,
        welcome_shown: true,
    };
    assert_eq!(
        updates::startup_dialog(&settings, "0.11.0"),
        StartupDialog::None,
        "the v0.10 preferences alone open nothing"
    );

    assert_eq!(
        start_once(&app_state, &mut settings, cx),
        StartupDialog::Welcome
    );
    assert_eq!(
        start_once(&app_state, &mut settings, cx),
        StartupDialog::Welcome,
        "closing the app without acknowledging keeps the disclosure due"
    );

    cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state
                .telemetry_mut()
                .expect("scheduler")
                .disclosure_dismissed(dbflux_core::chrono::Utc::now())
        })
    })
    .expect("disclosure dismissed");

    assert_eq!(
        start_once(&app_state, &mut settings, cx),
        StartupDialog::None,
        "an acknowledged disclosure never reopens Welcome"
    );
    assert!(settings.welcome_shown && !settings.show_whats_new_after_update);

    // A restart builds new app state over the same storage; the stored
    // acknowledgement, not the previous in-memory scheduler, keeps Welcome away.
    let restarted = cx.update(|cx| {
        let path = app_state
            .read(cx)
            .storage_runtime()
            .dbflux_db_path()
            .to_path_buf();
        let runtime = dbflux_storage::StorageRuntime::for_path(path).expect("storage reopened");
        cx.new(|_| {
            dbflux_ui_base::AppStateEntity::new_with_storage_runtime(runtime)
                .expect("restarted app state")
        })
    });
    assert_eq!(
        start_once(&restarted, &mut settings, cx),
        StartupDialog::None,
        "the acknowledgement survives a restart"
    );
}

/// Welcome replacing a pending "What is new" keeps it due: the notes open on
/// the launch after the disclosure is confirmed, once.
#[gpui::test]
fn displaced_whats_new_opens_after_the_disclosure_is_confirmed(cx: &mut gpui::TestAppContext) {
    let app_state = crate::ui::overlays::updates::test_support::test_app_state(cx);
    let mut settings = UpdateSettings {
        check_for_updates_on_startup: true,
        show_whats_new_after_update: true,
        last_run_version: Some("0.10.0".to_owned()),
        skipped_update_version: None,
        welcome_shown: true,
    };
    let previous = updates::parse_version("0.10.0");
    assert!(previous.is_some());

    assert_eq!(
        start_once(&app_state, &mut settings, cx),
        StartupDialog::Welcome
    );
    assert_eq!(
        settings.last_run_version.as_deref(),
        Some("0.10.0"),
        "the displaced notes stay due"
    );

    cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state
                .telemetry_mut()
                .expect("scheduler")
                .disclosure_dismissed(dbflux_core::chrono::Utc::now())
        })
    })
    .expect("disclosure dismissed");

    assert_eq!(
        start_once(&app_state, &mut settings, cx),
        StartupDialog::WhatsNew { since: previous }
    );
    assert_eq!(
        start_once(&app_state, &mut settings, cx),
        StartupDialog::None,
        "the notes open once"
    );
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
