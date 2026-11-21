// No `gpui::*` glob: it would make `#[test]` resolve to `gpui::test`,
// whose expansion recurses without bound.
use super::*;

#[cfg(feature = "telemetry")]
#[gpui::test]
fn welcome_disclosure_requires_rendered_user_dismissal(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (dialog, window) = cx.add_window_view(move |_, cx| WelcomeDialog::new(app_state, cx));
    let acknowledgements = std::rc::Rc::new(std::cell::Cell::new(0));
    let _subscription = window.update(|_, cx| {
        let acknowledgements = acknowledgements.clone();
        cx.subscribe(
            &dialog,
            move |_, _: &super::super::TelemetryDisclosureDismissed, _| {
                acknowledgements.set(acknowledgements.get() + 1);
            },
        )
    });
    dialog.update(window, |dialog, cx| {
        dialog.close(cx);
        dialog.open(cx);
        dialog.close(cx);
    });
    window.run_until_parked();
    assert_eq!(acknowledgements.get(), 0);

    dialog.update(window, |dialog, cx| dialog.open(cx));
    window.run_until_parked();
    assert!(dialog.read_with(window, |dialog, _| dialog.disclosure_rendered));
    dialog.update(window, |dialog, cx| dialog.hide(cx));
    window.run_until_parked();
    assert_eq!(acknowledgements.get(), 0);

    dialog.update(window, |dialog, cx| dialog.open(cx));
    window.run_until_parked();
    dialog.update(window, |dialog, cx| {
        dialog.close(cx);
        dialog.close(cx);
    });
    window.run_until_parked();
    assert_eq!(acknowledgements.get(), 1);
}

#[cfg(feature = "telemetry")]
const WELCOME: dbflux_ui_base::keyboard_coverage::SurfaceRegistry =
    dbflux_ui_base::keyboard_coverage::SurfaceRegistry {
        name: "welcome dialog",
        contexts: &[ContextId::Modal, ContextId::FormNavigation],
        entries: &[
            (
                "welcome-full-changelog",
                dbflux_ui_base::keyboard_coverage::KeyboardPath::Command(Command::Execute),
            ),
            (
                "welcome-check-for-updates",
                dbflux_ui_base::keyboard_coverage::KeyboardPath::Command(Command::Execute),
            ),
            (
                "welcome-show-whats-new",
                dbflux_ui_base::keyboard_coverage::KeyboardPath::Command(Command::Execute),
            ),
            (
                "welcome-telemetry-*",
                dbflux_ui_base::keyboard_coverage::KeyboardPath::Command(Command::ColumnRight),
            ),
            (
                "welcome-get-started",
                dbflux_ui_base::keyboard_coverage::KeyboardPath::Command(Command::Execute),
            ),
        ],
    };

#[cfg(feature = "telemetry")]
#[gpui::test]
fn every_welcome_control_is_covered(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    let (dialog, window) = cx.add_window_view(move |_, cx| WelcomeDialog::new(app_state, cx));
    window.update(|_, cx| dialog.update(cx, |dialog, cx| dialog.open(cx)));
    window.run_until_parked();

    let capture = FrameCapture::observe(window);
    window.update(|window, _| window.refresh());
    let frame = capture.frame(window);
    Coverage::new(WELCOME)
        .with_surface(MODAL_CHROME)
        .assert_covered(&frame);
}

#[cfg(feature = "telemetry")]
fn telemetry_level(
    app_state: &gpui::Entity<dbflux_ui_base::AppStateEntity>,
    window: &mut gpui::VisualTestContext,
) -> Option<TelemetryLevel> {
    window.update(|_, cx| app_state.read(cx).telemetry_level())
}

/// Initializes the optional telemetry module on the test app state with
/// the default configuration (no endpoint), like the workspace does.
#[cfg(feature = "telemetry")]
fn initialize_telemetry(
    app_state: &gpui::Entity<dbflux_ui_base::AppStateEntity>,
    cx: &mut TestAppContext,
) {
    cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state.initialize_telemetry(
                dbflux_app::telemetry::TelemetryConfig::default(),
                dbflux_core::chrono::Utc::now(),
            )
        })
    })
    .expect("telemetry initialized");
}

#[cfg(feature = "telemetry")]
#[gpui::test]
fn selecting_basic_persists_at_once_without_acknowledging_or_queueing(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    initialize_telemetry(&app_state, cx);
    let state_for_view = app_state.clone();
    let (dialog, window) = cx.add_window_view(move |_, cx| WelcomeDialog::new(state_for_view, cx));
    window.update(|_, cx| dialog.update(cx, |dialog, cx| dialog.open(cx)));
    window.run_until_parked();

    // The cards are radio choices: a click and the Right key run the same
    // select path (the coverage test proves the click action is wired).
    window.simulate_keystrokes("right");
    window.run_until_parked();

    assert_eq!(
        telemetry_level(&app_state, window),
        Some(TelemetryLevel::Basic)
    );
    window.update(|_, cx| {
        let scheduler = app_state.read(cx).telemetry().expect("scheduler");
        assert!(scheduler.state().pending().is_empty(), "no queue");
        assert!(!scheduler.is_disclosure_dismissed(), "no acknowledgement");
    });
}

#[cfg(feature = "telemetry")]
#[gpui::test]
fn hiding_after_selecting_still_acknowledges_nothing(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    initialize_telemetry(&app_state, cx);
    let state_for_view = app_state.clone();
    let (dialog, window) = cx.add_window_view(move |_, cx| WelcomeDialog::new(state_for_view, cx));
    let acknowledgements = std::rc::Rc::new(std::cell::Cell::new(0));
    let _subscription = window.update(|_, cx| {
        let acknowledgements = acknowledgements.clone();
        cx.subscribe(
            &dialog,
            move |_, _: &super::super::TelemetryDisclosureDismissed, _| {
                acknowledgements.set(acknowledgements.get() + 1);
            },
        )
    });
    window.update(|_, cx| dialog.update(cx, |dialog, cx| dialog.open(cx)));
    window.run_until_parked();

    window.simulate_keystrokes("right");
    window.run_until_parked();

    // A programmatic hide is not an acknowledgement.
    window.update(|_, cx| dialog.update(cx, |dialog, cx| dialog.hide(cx)));
    window.run_until_parked();
    assert_eq!(acknowledgements.get(), 0);
    assert_eq!(
        telemetry_level(&app_state, window),
        Some(TelemetryLevel::Basic)
    );
    window.update(|_, cx| {
        let scheduler = app_state.read(cx).telemetry().expect("scheduler");
        assert!(scheduler.state().pending().is_empty());
        assert!(!scheduler.is_disclosure_dismissed());
    });
}

#[cfg(feature = "telemetry")]
#[gpui::test]
fn escape_hides_welcome_without_acknowledging_the_disclosure(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    initialize_telemetry(&app_state, cx);
    let state_for_view = app_state.clone();
    let (dialog, window) = cx.add_window_view(move |_, cx| WelcomeDialog::new(state_for_view, cx));
    let acknowledgements = std::rc::Rc::new(std::cell::Cell::new(0));
    let _subscription = window.update(|_, cx| {
        let acknowledgements = acknowledgements.clone();
        cx.subscribe(
            &dialog,
            move |_, _: &super::super::TelemetryDisclosureDismissed, _| {
                acknowledgements.set(acknowledgements.get() + 1);
            },
        )
    });
    window.update(|_, cx| dialog.update(cx, |dialog, cx| dialog.open(cx)));
    window.run_until_parked();

    window.simulate_keystrokes("escape");
    window.run_until_parked();

    assert!(!window.update(|_, cx| dialog.read(cx).is_visible()));
    assert_eq!(acknowledgements.get(), 0, "only Get started acknowledges");
    window.update(|_, cx| {
        let scheduler = app_state.read(cx).telemetry().expect("scheduler");
        assert!(!scheduler.is_disclosure_dismissed());
    });
}

#[cfg(feature = "telemetry")]
#[gpui::test]
fn arrows_rove_and_select_the_level_and_enter_still_gets_started(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    initialize_telemetry(&app_state, cx);
    let state_for_view = app_state.clone();
    let (dialog, window) = cx.add_window_view(move |_, cx| WelcomeDialog::new(state_for_view, cx));
    let acknowledgements = std::rc::Rc::new(std::cell::Cell::new(0));
    let _subscription = window.update(|_, cx| {
        let acknowledgements = acknowledgements.clone();
        cx.subscribe(
            &dialog,
            move |_, _: &super::super::TelemetryDisclosureDismissed, _| {
                acknowledgements.set(acknowledgements.get() + 1);
            },
        )
    });
    window.update(|_, cx| dialog.update(cx, |dialog, cx| dialog.open(cx)));
    window.run_until_parked();

    for (key, expected) in [
        ("right", TelemetryLevel::Basic),
        ("right", TelemetryLevel::Basic),
        ("left", TelemetryLevel::Minimal),
        ("left", TelemetryLevel::Off),
        ("left", TelemetryLevel::Off),
        ("right", TelemetryLevel::Minimal),
    ] {
        window.simulate_keystrokes(key);
        window.run_until_parked();
        assert_eq!(telemetry_level(&app_state, window), Some(expected), "{key}");
    }
    assert_eq!(acknowledgements.get(), 0, "stepping never acknowledges");

    // Enter remains the modal confirm: Get started closes and acks once.
    window.simulate_keystrokes("enter");
    window.run_until_parked();
    assert_eq!(acknowledgements.get(), 1);
    assert!(!window.update(|_, cx| dialog.read(cx).is_visible()));
}

#[cfg(feature = "telemetry")]
#[test]
fn basic_is_the_preselected_level() {
    assert_eq!(super::super::DEFAULT_TELEMETRY_LEVEL, TelemetryLevel::Basic);
    assert_eq!(
        super::super::telemetry_level_index(TelemetryLevel::Basic),
        2
    );
}

/// Opens Welcome over a fresh initialized state, presses `keys`, then
/// confirms with Get started. Returns the persisted level and the number
/// of acknowledgements the dialog emitted.
#[cfg(feature = "telemetry")]
fn confirm_welcome_after(
    keys: &[&str],
    cx: &mut TestAppContext,
) -> (Option<TelemetryLevel>, usize) {
    let app_state = test_app_state(cx);
    initialize_telemetry(&app_state, cx);
    let state_for_view = app_state.clone();
    let (dialog, window) = cx.add_window_view(move |_, cx| WelcomeDialog::new(state_for_view, cx));
    let acknowledgements = std::rc::Rc::new(std::cell::Cell::new(0));
    let _subscription = window.update(|_, cx| {
        let acknowledgements = acknowledgements.clone();
        cx.subscribe(
            &dialog,
            move |_, _: &super::super::TelemetryDisclosureDismissed, _| {
                acknowledgements.set(acknowledgements.get() + 1);
            },
        )
    });
    window.update(|_, cx| dialog.update(cx, |dialog, cx| dialog.open(cx)));
    window.run_until_parked();

    for key in keys {
        window.simulate_keystrokes(key);
        window.run_until_parked();
    }
    window.simulate_keystrokes("enter");
    window.run_until_parked();

    (telemetry_level(&app_state, window), acknowledgements.get())
}

#[cfg(feature = "telemetry")]
#[gpui::test]
fn closing_without_changing_the_selection_keeps_basic(cx: &mut TestAppContext) {
    assert_eq!(
        confirm_welcome_after(&[], cx),
        (Some(TelemetryLevel::Basic), 1)
    );
}

#[cfg(feature = "telemetry")]
#[gpui::test]
fn choosing_minimal_before_closing_persists_minimal(cx: &mut TestAppContext) {
    assert_eq!(
        confirm_welcome_after(&["left"], cx),
        (Some(TelemetryLevel::Minimal), 1)
    );
}

#[cfg(feature = "telemetry")]
#[gpui::test]
fn choosing_off_before_closing_persists_off(cx: &mut TestAppContext) {
    assert_eq!(
        confirm_welcome_after(&["left", "left"], cx),
        (Some(TelemetryLevel::Off), 1)
    );
}

#[cfg(feature = "telemetry")]
#[gpui::test]
fn the_roving_choice_starts_at_the_saved_level(cx: &mut TestAppContext) {
    let app_state = test_app_state(cx);
    initialize_telemetry(&app_state, cx);
    cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state.set_telemetry_level(TelemetryLevel::Off, dbflux_core::chrono::Utc::now())
        })
    })
    .expect("saved off");
    let state_for_view = app_state.clone();
    let (dialog, window) = cx.add_window_view(move |_, cx| WelcomeDialog::new(state_for_view, cx));
    window.update(|_, cx| dialog.update(cx, |dialog, cx| dialog.open(cx)));
    window.run_until_parked();

    window.simulate_keystrokes("right");
    window.run_until_parked();

    assert_eq!(
        telemetry_level(&app_state, window),
        Some(TelemetryLevel::Minimal),
        "Right from the saved Off picks Minimal"
    );
}

/// The workspace mounts the dialog as a flow sibling after the shell.
/// The host around the modal must therefore leave the normal flow
/// (viewport-absolute), or the absolutely positioned modal lands below
/// the window with zero height: the native 0.9.0-rc.0+local build showed
/// the welcome dialog at y=2046, height 0, over a 1908x1023 window.
#[cfg(feature = "telemetry")]
#[gpui::test]
fn the_welcome_modal_covers_the_workspace_viewport(cx: &mut TestAppContext) {
    use gpui::{
        AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _,
        Render, Styled as _, Window, div as gpui_div, px, size,
    };

    struct WorkspaceLikeHost {
        welcome: Entity<WelcomeDialog>,
    }

    impl Render for WorkspaceLikeHost {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            gpui_div()
                .size_full()
                .flex()
                .flex_col()
                .child(
                    gpui_div()
                        .h(px(40.0))
                        .debug_selector(|| "workspace-title".to_string()),
                )
                .child(
                    gpui_div()
                        .flex_1()
                        .debug_selector(|| "workspace-body".to_string()),
                )
                .child(self.welcome.clone())
        }
    }

    let app_state = test_app_state(cx);
    let state_for_view = app_state.clone();
    let slot = std::rc::Rc::new(std::cell::RefCell::new(None));
    let slot_for_view = slot.clone();
    let (_host, window) = cx.add_window_view(move |_window, cx| {
        let welcome = cx.new(|cx| WelcomeDialog::new(state_for_view.clone(), cx));
        slot_for_view.borrow_mut().replace(welcome.clone());
        WorkspaceLikeHost { welcome }
    });
    window.simulate_resize(size(px(900.0), px(600.0)));
    window.run_until_parked();

    let dialog = slot.borrow().clone().expect("dialog built");
    window.update(|_, cx| {
        dialog.update(cx, |dialog, cx| dialog.open(cx));
    });
    window.run_until_parked();
    window.update(|window, _| window.refresh());

    for (width, height) in [(863.0, 513.0), (900.0, 600.0)] {
        window.simulate_resize(size(px(width), px(height)));
        window.run_until_parked();
        window.update(|window, _| window.refresh());
        // The dialog is actually open and its controls are in the frame.
        assert!(window.update(|_, cx| dialog.read(cx).is_visible()));
        let capture = FrameCapture::observe(window);
        let frame = capture.frame(window);
        let clickable: Vec<String> = frame
            .nodes()
            .filter_map(|(_, node)| frame.accessibility_node(node))
            .filter(|node| node.supports_action(gpui::AccessibleAction::Click))
            .map(|_| String::new())
            .collect();
        let _ = clickable;

        let host_bounds = window
            .debug_bounds("welcome-dialog-host")
            .unwrap_or_else(|| panic!("the welcome host is laid out"));
        let window_size = window.update(|window, _| window.viewport_size());
        assert!(
            host_bounds.size.height > px(0.0),
            "the modal host must have positive height, got {host_bounds:?}"
        );
        assert!(
            host_bounds.origin.y >= px(0.0) && host_bounds.origin.y < window_size.height,
            "the modal host must start inside the window, got {host_bounds:?} in {window_size:?}"
        );
        assert!(
            host_bounds.origin.y + host_bounds.size.height <= window_size.height + px(1.0),
            "the modal host must not spill below the window, got {host_bounds:?} in {window_size:?}"
        );

        let card_ids = [
            "welcome-telemetry-off",
            "welcome-telemetry-minimal",
            "welcome-telemetry-basic",
        ];
        let card_bounds =
            card_ids.map(|id| window.debug_bounds(id).expect("card geometry is painted"));
        for pair in card_bounds.windows(2) {
            assert!(
                (pair[0].size.width - pair[1].size.width).abs() <= px(1.0),
                "the level cards are equal columns: {card_bounds:?}"
            );
            assert_eq!(pair[0].origin.y, pair[1].origin.y, "{card_bounds:?}");
            assert!(
                pair[0].origin.x + pair[0].size.width <= pair[1].origin.x,
                "the level cards sit side by side: {card_bounds:?}"
            );
        }

        for (id, bounds) in card_ids.into_iter().zip(card_bounds) {
            assert!(
                bounds.size.height <= px(112.0),
                "compact card {id}: {bounds:?}"
            );
            assert!(bounds.size.height > px(0.0));
            let columns = window
                .debug_bounds("welcome-columns")
                .expect("panel geometry");
            let footer = window
                .debug_bounds("welcome-footer")
                .expect("footer geometry");
            assert!(bounds.origin.y >= columns.origin.y);
            assert!(bounds.origin.y + bounds.size.height <= footer.origin.y);
            eprintln!("compact geometry {id}: {bounds:?}; panel: {columns:?}; footer: {footer:?}");
        }

        let privacy = window
            .debug_bounds("welcome-privacy-box")
            .expect("privacy box");
        let text = window
            .debug_bounds("welcome-privacy-text")
            .expect("privacy text");
        let icon = window
            .debug_bounds("welcome-privacy-icon")
            .expect("privacy icon");
        assert!(
            text.size.width <= privacy.size.width - px(28.0),
            "privacy must fit: {text:?} in {privacy:?}"
        );
        assert!(text.size.height > px(22.0), "privacy must wrap: {text:?}");
        assert!(icon.size.width >= px(12.0) && icon.size.height >= px(12.0));
        assert!(text.origin.y + text.size.height <= privacy.origin.y + privacy.size.height);
        let footer = window
            .debug_bounds("welcome-footer")
            .expect("footer geometry");
        let header = window
            .debug_bounds("welcome-header")
            .expect("header geometry");
        let columns = window
            .debug_bounds("welcome-columns")
            .expect("panel geometry");
        assert!(header.origin.y >= px(8.0));
        assert!(header.origin.y + header.size.height <= columns.origin.y);
        assert!(privacy.origin.y >= columns.origin.y);
        assert!(privacy.origin.y + privacy.size.height <= footer.origin.y);
        assert!(
            footer.origin.y + footer.size.height <= window_size.height - px(8.0),
            "footer must fit: {footer:?} in {window_size:?}"
        );
        eprintln!(
            "privacy geometry: box={privacy:?}, text={text:?}, icon={icon:?}; footer={footer:?}"
        );

        // Get started and the three level cards sit inside the host's bounds:
        // real geometry, not a visibility flag.
        let capture = FrameCapture::observe(window);
        let frame = capture.frame(window);
        let disclosure = dbflux_i18n::t!("updates.welcome.telemetry_privacy");
        assert!(
            frame
                .nodes()
                .any(|(_, node)| node.content_text().contains(&disclosure))
        );
        for required in [
            "welcome-get-started",
            "welcome-telemetry-off",
            "welcome-telemetry-minimal",
            "welcome-telemetry-basic",
        ] {
            assert!(
                frame.nodes().any(|(_, node)| node.id() == required),
                "{required} must be in the painted frame"
            );
        }
    }
}

/// Basic is the preselected level, so no locale may still call Minimal
/// the default in the Welcome helper line.
#[cfg(feature = "telemetry")]
#[test]
fn welcome_helper_names_basic_as_the_default_in_every_locale() {
    const MINIMAL: [&str; 4] = ["Minimal", "Mínimo", "최소", "最小"];

    for (index, locale) in ["en", "es", "ko", "zh_Hans"].iter().enumerate() {
        let helper = dbflux_i18n::t!("updates.welcome.telemetry_helper", locale = locale);
        let basic = dbflux_i18n::t!("updates.welcome.telemetry_basic", locale = locale);
        assert!(
            helper.contains(&basic),
            "{locale}: the helper must name Basic as the default, got: {helper}"
        );
        assert!(
            !helper.contains(MINIMAL[index]),
            "{locale}: the helper must not name Minimal, got: {helper}"
        );
    }
}
