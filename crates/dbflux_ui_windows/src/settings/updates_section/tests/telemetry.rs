// Explicit imports: the parent's `gpui::*` glob would make `#[test]`
// resolve to `gpui::test`, whose expansion recurses without bound.
use super::super::{ReportState, UpdatesRow, UpdatesSection};
use dbflux_app::telemetry::{TelemetryConfig, TelemetryLastReport, TelemetryLevel};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{AppContext as _, Entity, ParentElement as _, TestAppContext, VisualTestContext};

/// A real v1 ping payload (closed wire schema), so the UI test never
/// normalizes a fake report shape.
const REPORT_PAYLOAD_FIXTURE: &str = r#"{"schema":"dbflux.telemetry-event/v1","install_id":"0d8f8a2e-9c34-4f5a-8d1e-2b3c4d5e6f70","delivery_id":"1e9f9b3f-0d45-406b-9e2f-3c4d5e6f7081","sent_at":"2026-01-01T00:00:00Z","version":"1.2.3","channel":"stable","os":"linux","arch":"amd64","event":"ping"}"#;

struct Harness {
    app_state: Entity<AppStateEntity>,
    section: Entity<UpdatesSection>,
}

fn open(cx: &mut TestAppContext) -> (Harness, &mut VisualTestContext) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });

    let app_state: Entity<AppStateEntity> = cx.update(|cx| {
        cx.new(|_| {
            let runtime = StorageRuntime::in_memory().expect("in-memory storage");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        })
    });
    open_on(cx, app_state)
}

/// Opens the section over an existing app state, so a test can change the
/// saved state between two section lifetimes.
fn open_on(
    cx: &mut TestAppContext,
    app_state: Entity<AppStateEntity>,
) -> (Harness, &mut VisualTestContext) {
    cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state.initialize_telemetry(TelemetryConfig::default(), dbflux_core::chrono::Utc::now())
        })
    })
    .expect("telemetry initialized");
    mount(cx, app_state)
}

/// Opens the section over an app state whose telemetry was never initialized.
fn mount(
    cx: &mut TestAppContext,
    app_state: Entity<AppStateEntity>,
) -> (Harness, &mut VisualTestContext) {
    struct SectionView {
        section: Entity<UpdatesSection>,
    }

    impl gpui::Render for SectionView {
        fn render(
            &mut self,
            _window: &mut gpui::Window,
            _cx: &mut gpui::Context<Self>,
        ) -> impl gpui::IntoElement {
            gpui::div().child(self.section.clone())
        }
    }

    let slot = std::rc::Rc::new(std::cell::RefCell::new(None));
    let slot_for_view = slot.clone();
    let app_for_view = app_state.clone();
    let (_view, window) = cx.add_window_view(move |window, cx| {
        let section = cx.new(|cx| UpdatesSection::new(app_for_view.clone(), window, cx));
        slot_for_view.borrow_mut().replace(section.clone());
        SectionView { section }
    });
    window.run_until_parked();

    let section = slot.borrow().clone().expect("section built");
    (Harness { app_state, section }, window)
}

fn saved_level(harness: &Harness, window: &mut VisualTestContext) -> Option<TelemetryLevel> {
    window.update(|_, cx| harness.app_state.read(cx).telemetry_level())
}

fn staged_level(harness: &Harness, window: &mut VisualTestContext) -> TelemetryLevel {
    window.update(|_, cx| {
        harness
            .section
            .read(cx)
            .telemetry_level
            .expect("telemetry available")
    })
}

fn masked_id(harness: &Harness, window: &mut VisualTestContext) -> Option<String> {
    window.update(|_, cx| harness.app_state.read(cx).telemetry_install_id_masked())
}

/// The install-id reset runs through the reusable confirmation: the row
/// opens it with both resolutions available, cancelling leaves the
/// install id untouched, and confirming rotates the identity.
#[gpui::test]
fn the_install_reset_confirmation_cancels_and_confirms(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    let before = masked_id(&harness, window);

    let confirm_visible = |window: &mut VisualTestContext| {
        window.update(|_, cx| {
            harness
                .section
                .read(cx)
                .install_reset_confirm
                .read(cx)
                .is_visible()
        })
    };
    let open_confirmation = |window: &mut VisualTestContext| {
        window.update(|_, cx| {
            harness.section.update(cx, |section, cx| {
                section.activate(UpdatesRow::InstallReset, cx)
            });
        });
        window.run_until_parked();
    };

    open_confirmation(window);
    assert!(
        confirm_visible(window),
        "the reset row opens the confirmation"
    );
    assert!(
        window.update(|window, cx| harness
            .section
            .read(cx)
            .install_reset_confirm
            .read(cx)
            .contains_focus(window, cx)),
        "opening the confirmation moves focus into it"
    );

    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section
                .install_reset_confirm
                .update(cx, |confirm, cx| confirm.cancel(cx));
        });
    });
    window.run_until_parked();
    assert!(
        !confirm_visible(window),
        "cancelling closes the confirmation"
    );
    assert_eq!(
        masked_id(&harness, window),
        before,
        "cancelling leaves the install id untouched"
    );

    open_confirmation(window);
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section
                .install_reset_confirm
                .update(cx, |confirm, cx| confirm.confirm(cx));
        });
    });
    window.run_until_parked();
    assert!(
        !confirm_visible(window),
        "confirming closes the confirmation"
    );
    assert_ne!(
        masked_id(&harness, window),
        before,
        "confirming rotates the installation identity"
    );
}

#[gpui::test]
fn the_staged_level_does_not_change_the_saved_level_until_save(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    let app_state = harness.app_state.clone();
    window
        .update(|_, cx| {
            app_state.update(cx, |state, _| {
                state.set_telemetry_level(TelemetryLevel::Basic, dbflux_core::chrono::Utc::now())
            })
        })
        .expect("saved basic");

    // Reopening stages the saved level.
    let (harness, window) = open_on(cx, app_state);
    assert_eq!(saved_level(&harness, window), Some(TelemetryLevel::Basic));
    assert_eq!(staged_level(&harness, window), TelemetryLevel::Basic);

    // Activating cycles to Off; nothing is persisted and nothing is queued.
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section.activate(UpdatesRow::TelemetryLevel, cx)
        });
    });
    assert_eq!(staged_level(&harness, window), TelemetryLevel::Off);
    assert_eq!(
        saved_level(&harness, window),
        Some(TelemetryLevel::Basic),
        "staging never writes"
    );

    // Save applies the staged level.
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| section.save(cx));
    });
    window.run_until_parked();
    assert_eq!(saved_level(&harness, window), Some(TelemetryLevel::Off));
}

#[gpui::test]
fn a_new_install_stages_basic_and_the_keys_step_through_three_levels(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    assert_eq!(saved_level(&harness, window), Some(TelemetryLevel::Basic));
    assert_eq!(staged_level(&harness, window), TelemetryLevel::Basic);

    let press = |window: &mut VisualTestContext, key: &str| {
        let event = gpui::KeyDownEvent {
            keystroke: gpui::Keystroke::parse(key).expect("keystroke"),
            is_held: false,
            prefer_character_input: false,
        };
        window.update(|_, cx| {
            harness.section.update(cx, |section, cx| {
                section.cursor = super::super::ROWS
                    .iter()
                    .position(|row| *row == UpdatesRow::TelemetryLevel)
                    .expect("level row");
                section.handle_key(&event, cx);
            });
        });
    };

    for (key, expected) in [
        ("right", TelemetryLevel::Basic),
        ("right", TelemetryLevel::Basic),
        ("left", TelemetryLevel::Minimal),
        ("left", TelemetryLevel::Off),
        ("left", TelemetryLevel::Off),
        ("enter", TelemetryLevel::Minimal),
        ("enter", TelemetryLevel::Basic),
        ("enter", TelemetryLevel::Off),
    ] {
        press(window, key);
        assert_eq!(staged_level(&harness, window), expected, "{key}");
    }

    assert_eq!(
        saved_level(&harness, window),
        Some(TelemetryLevel::Basic),
        "staging never writes"
    );
}

#[test]
fn basic_is_the_default_level_and_the_hint_names_it_in_every_locale() {
    const MINIMAL_DEFAULT: [&str; 4] = [
        "Minimal, the default",
        "Mínimo, el valor predeterminado",
        "「최소」가 기본값",
        "「最小」是默认值",
    ];

    assert_eq!(super::super::DEFAULT_TELEMETRY_LEVEL, TelemetryLevel::Basic);
    for (index, locale) in ["en", "es", "ko", "zh_Hans"].iter().enumerate() {
        let hint = dbflux_i18n::t!("updates.settings.level_hint", locale = locale);
        let basic = dbflux_i18n::t!("updates.settings.level_basic", locale = locale);
        assert!(
            !hint.contains(MINIMAL_DEFAULT[index]),
            "{locale}: the hint must not call Minimal the default, got: {hint}"
        );
        assert!(
            hint.contains(&basic),
            "{locale}: the hint must name Basic, got: {hint}"
        );
    }
}

#[gpui::test]
fn the_level_control_renders_three_segments(cx: &mut TestAppContext) {
    let (_harness, window) = open(cx);
    let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(window);
    window.update(|window, _| window.refresh());
    let frame = capture.frame(window);

    for id in [
        "segmented-telemetry-level-off",
        "segmented-telemetry-level-minimal",
        "segmented-telemetry-level-basic",
    ] {
        assert!(
            frame.nodes().any(|(_, node)| node.id() == id),
            "{id} must be in the painted frame"
        );
    }
}

#[gpui::test]
fn reset_defaults_stages_basic_without_touching_storage_or_the_install_id(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    let app_state = harness.app_state.clone();
    window
        .update(|_, cx| {
            app_state.update(cx, |state, _| {
                state.set_telemetry_level(TelemetryLevel::Off, dbflux_core::chrono::Utc::now())
            })
        })
        .expect("saved off");

    let (harness, window) = open_on(cx, app_state);
    let before = masked_id(&harness, window);

    window.update(|_, cx| {
        harness
            .section
            .update(cx, |section, cx| section.reset_to_defaults(cx));
    });

    assert_eq!(staged_level(&harness, window), TelemetryLevel::Basic);
    assert_eq!(
        saved_level(&harness, window),
        Some(TelemetryLevel::Off),
        "reset stages, never persists"
    );
    assert_eq!(masked_id(&harness, window), before, "reset keeps the id");
}

#[gpui::test]
fn the_report_viewer_opens_only_over_a_cached_report(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    // Without a cached report the action is a no-op: no fake viewer, and
    // focus never moves into it.
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section.activate(UpdatesRow::ViewReport, cx)
        });
    });
    window.run_until_parked();
    assert!(
        !window.update(|_, cx| {
            harness
                .section
                .read(cx)
                .report_preview
                .read(cx)
                .is_visible()
        }),
        "no viewer without a report"
    );
    assert!(
        !window.update(|window, cx| harness
            .section
            .read(cx)
            .report_preview
            .read(cx)
            .contains_focus(window, cx)),
        "focus stays out of the viewer without a report"
    );

    window.update(|_, cx| {
        harness.section.update(cx, |section, _| {
            section.report_state = ReportState::Ready(TelemetryLastReport {
                payload: REPORT_PAYLOAD_FIXTURE.to_owned(),
                payload_len: REPORT_PAYLOAD_FIXTURE.len(),
                level: Some("basic".to_owned()),
                ack_at_epoch_ms: 1_767_225_600_000,
                correlation: Some(uuid::Uuid::nil()),
            });
        });
    });
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section.activate(UpdatesRow::ViewReport, cx)
        });
    });
    window.run_until_parked();
    assert!(
        window.update(|_, cx| {
            harness
                .section
                .read(cx)
                .report_preview
                .read(cx)
                .is_visible()
        }),
        "the viewer opens over the cached report"
    );
    assert!(
        window.update(|window, cx| harness
            .section
            .read(cx)
            .report_preview
            .read(cx)
            .contains_focus(window, cx)),
        "opening the viewer moves focus into it"
    );
}

/// Failed and Loading must never read "No report sent yet": only a
/// successful Ok(None) lookup may claim that. Buttons stay disabled in
/// every non-Ready state, so a failed lookup can neither open the viewer
/// nor invent metadata or a payload.
#[test]
fn report_states_have_distinct_truthful_labels() {
    use super::super::report_meta_label;

    let none = report_meta_label(&ReportState::None);
    let loading = report_meta_label(&ReportState::Loading);
    let failed = report_meta_label(&ReportState::Failed);

    assert_eq!(
        failed,
        dbflux_i18n::t!("updates.settings.report_load_failed"),
        "a failed lookup must say so, not pretend nothing was sent"
    );
    assert_eq!(
        loading,
        dbflux_i18n::t!("updates.settings.checking_last_report"),
        "a running lookup must say it is checking"
    );
    assert_eq!(none, dbflux_i18n::t!("updates.settings.no_report_sent_yet"));
    assert_ne!(failed, none);
    assert_ne!(loading, none);
    assert_ne!(failed, loading);

    let ready = report_meta_label(&ReportState::Ready(TelemetryLastReport {
        payload: REPORT_PAYLOAD_FIXTURE.to_owned(),
        payload_len: REPORT_PAYLOAD_FIXTURE.len(),
        level: Some("basic".to_owned()),
        ack_at_epoch_ms: 1_767_225_600_000,
        correlation: Some(uuid::Uuid::nil()),
    }));
    assert_eq!(
        ready,
        dbflux_i18n::t!(
            "updates.settings.last_sent",
            when = super::super::report_time_label(1_767_225_600_000),
            level = super::super::report_level_label(Some("basic")),
            size = super::super::report_size_label(REPORT_PAYLOAD_FIXTURE.len())
        ),
        "a cached report keeps its real metadata"
    );
    assert_ne!(ready, none);
}

/// A ping delivered before the Minimal level existed is stored with the
/// `"off"` marker; the last-report line must not claim Off sent it.
#[test]
fn a_legacy_off_ping_row_reads_as_minimal_in_the_last_report_line() {
    use super::super::report_meta_label;

    let line = report_meta_label(&ReportState::Ready(TelemetryLastReport {
        payload: REPORT_PAYLOAD_FIXTURE.to_owned(),
        payload_len: REPORT_PAYLOAD_FIXTURE.len(),
        level: Some("off".to_owned()),
        ack_at_epoch_ms: 1_767_225_600_000,
        correlation: Some(uuid::Uuid::nil()),
    }));

    assert_eq!(
        line,
        dbflux_i18n::t!(
            "updates.settings.last_sent",
            when = super::super::report_time_label(1_767_225_600_000),
            level = dbflux_i18n::t!("updates.settings.level_minimal_display"),
            size = super::super::report_size_label(REPORT_PAYLOAD_FIXTURE.len())
        )
    );
    assert!(!line.contains(&dbflux_i18n::t!("updates.settings.level_off_display")));
}

#[gpui::test]
fn a_missing_scheduler_invalidates_the_cached_report(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    window.update(|_, cx| {
        let unavailable = cx.new(|_| {
            let runtime = StorageRuntime::in_memory().expect("in-memory storage");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        });
        assert!(unavailable.read(cx).telemetry().is_none());
        harness.section.update(cx, |section, cx| {
            section.report_state = ReportState::Ready(TelemetryLastReport {
                payload: REPORT_PAYLOAD_FIXTURE.to_owned(),
                payload_len: REPORT_PAYLOAD_FIXTURE.len(),
                level: None,
                ack_at_epoch_ms: 1_767_225_600_000,
                correlation: Some(uuid::Uuid::nil()),
            });
            section.app_state = unavailable;
            section.refresh_last_report(cx);
            assert!(
                matches!(section.report_state, ReportState::Failed),
                "an unavailable scheduler leaves the report unknown, not absent"
            );
            section.activate(UpdatesRow::ViewReport, cx);
            assert!(!section.report_preview.read(cx).is_visible());
            assert_eq!(
                section.report_meta_label(),
                dbflux_i18n::t!("updates.settings.report_load_failed")
            );
        });
    });
}

#[gpui::test]
fn a_failed_lookup_never_opens_the_viewer(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    window.update(|_, cx| {
        harness.section.update(cx, |section, _| {
            section.report_state = ReportState::Failed;
        });
    });
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section.activate(UpdatesRow::ViewReport, cx)
        });
    });
    window.run_until_parked();
    assert!(
        !window.update(|_, cx| {
            harness
                .section
                .read(cx)
                .report_preview
                .read(cx)
                .is_visible()
        }),
        "a failed lookup opens no viewer"
    );
    assert!(
        !window.update(|_, cx| {
            harness
                .section
                .read(cx)
                .report_meta_label()
                .contains(REPORT_PAYLOAD_FIXTURE)
        }),
        "the failed row never shows payload bytes"
    );
}

#[gpui::test]
fn only_a_successful_empty_lookup_says_no_report_sent_yet(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    // The constructor's refresh runs against the working in-memory audit:
    // a real Ok(None) is the only path to the None state.
    window.run_until_parked();
    let state = window.update(|_, cx| {
        let state = harness.section.read(cx).report_state.clone();
        super::super::report_meta_label(&state)
    });
    assert_eq!(
        state,
        dbflux_i18n::t!("updates.settings.no_report_sent_yet"),
        "Ok(None) after a real query is the truthful no-report state"
    );

    // A later failed refresh must not keep claiming "no report sent yet".
    window.update(|_, cx| {
        harness.section.update(cx, |section, _| {
            section.report_state = ReportState::Failed;
        });
    });
    let label = window.update(|_, cx| harness.section.read(cx).report_meta_label());
    assert_ne!(
        label,
        dbflux_i18n::t!("updates.settings.no_report_sent_yet"),
        "a failed lookup is not a truthful no-report claim"
    );
}

#[test]
fn report_labels_are_bounded_and_readable() {
    use super::super::{report_level_label, report_size_label};

    assert_eq!(
        report_level_label(Some("basic")),
        dbflux_i18n::t!("updates.settings.level_basic_display")
    );
    assert_eq!(
        report_level_label(Some("minimal")),
        dbflux_i18n::t!("updates.settings.level_minimal_display")
    );
    for legacy in [Some("off"), None] {
        assert_eq!(
            report_level_label(legacy),
            dbflux_i18n::t!("updates.settings.level_minimal_display"),
            "a delivered report never reads as off: {legacy:?}"
        );
    }
    assert_eq!(report_size_label(512), "512 B");
    assert_eq!(report_size_label(2048), "2.0 KiB");
}

#[gpui::test]
fn an_unavailable_scheduler_is_not_dirty_and_update_only_save_succeeds(cx: &mut TestAppContext) {
    let (_, window) = open(cx);
    let app_state: Entity<AppStateEntity> = window.update(|_, cx| {
        cx.new(|_| {
            let runtime = StorageRuntime::in_memory().expect("in-memory storage");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        })
    });
    let (harness, window) = mount(cx, app_state);

    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            assert!(section.telemetry_level.is_none());
            assert!(!section.has_unsaved_changes(cx), "nothing edited yet");
            section.stage_telemetry_level_at(0);
            assert!(section.telemetry_level.is_none(), "nothing to stage");
            assert!(!section.has_unsaved_changes(cx));

            let before = section.saved_settings(cx).check_for_updates_on_startup;
            section.activate(UpdatesRow::CheckOnStartup, cx);
            assert!(
                section.has_unsaved_changes(cx),
                "the update preference is dirty"
            );
            section.save(cx);
            assert_eq!(
                section.saved_settings(cx).check_for_updates_on_startup,
                !before,
                "the update preference saves without a telemetry module"
            );
            assert!(!section.has_unsaved_changes(cx));
        });
    });
}

#[gpui::test]
fn saving_an_update_preference_keeps_an_off_stored_elsewhere(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    assert_eq!(staged_level(&harness, window), TelemetryLevel::Basic);

    // Welcome stores Off while this section stays open.
    window.update(|_, cx| {
        harness.app_state.update(cx, |state, cx| {
            state
                .set_telemetry_level(TelemetryLevel::Off, dbflux_core::chrono::Utc::now())
                .expect("Off stored");
            cx.emit(dbflux_ui_base::AppStateChanged);
        });
    });
    window.run_until_parked();
    assert_eq!(
        staged_level(&harness, window),
        TelemetryLevel::Off,
        "an unedited draft follows the stored level"
    );

    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section.activate(UpdatesRow::CheckOnStartup, cx);
            section.save(cx);
        });
    });
    window.run_until_parked();
    assert_eq!(saved_level(&harness, window), Some(TelemetryLevel::Off));
}

#[gpui::test]
fn a_superseded_report_lookup_is_discarded(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            let stale = section.report_generation;
            section.report_generation += 1;
            section.report_state = ReportState::Ready(TelemetryLastReport {
                payload: REPORT_PAYLOAD_FIXTURE.to_owned(),
                payload_len: REPORT_PAYLOAD_FIXTURE.len(),
                level: None,
                ack_at_epoch_ms: 1_767_225_600_000,
                correlation: None,
            });

            section.apply_report_lookup(stale, Ok(None), cx);
            assert!(
                matches!(section.report_state, ReportState::Ready(_)),
                "an older lookup must not restore \"No report sent yet\""
            );
            section.apply_report_lookup(section.report_generation, Ok(None), cx);
            assert!(matches!(section.report_state, ReportState::None));
        });
    });
}
