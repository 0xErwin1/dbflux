use super::*;

const PROBE_FAILURE: &str = "probe refused the connection";

/// Wraps a registered driver and fails every connect, so the probe of a
/// connection test fails inside the driver.
struct FailingProbeDriver(Arc<dyn DbDriver>);

impl DbDriver for FailingProbeDriver {
    fn kind(&self) -> dbflux_core::DbKind {
        self.0.kind()
    }

    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        self.0.metadata()
    }

    fn form_definition(&self) -> &dbflux_core::DriverFormDef {
        self.0.form_definition()
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        self.0.driver_key()
    }

    fn build_config(
        &self,
        values: &dbflux_core::FormValues,
    ) -> Result<DbConfig, dbflux_core::DbError> {
        self.0.build_config(values)
    }

    fn extract_values(&self, config: &DbConfig) -> dbflux_core::FormValues {
        self.0.extract_values(config)
    }

    fn connect_with_secrets(
        &self,
        _profile: &ConnectionProfile,
        _password: Option<&SecretString>,
        _ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn dbflux_core::Connection>, dbflux_core::DbError> {
        Err(dbflux_core::DbError::connection_failed(PROBE_FAILURE))
    }

    fn test_connection(&self, _profile: &ConnectionProfile) -> Result<(), dbflux_core::DbError> {
        Err(dbflux_core::DbError::connection_failed(PROBE_FAILURE))
    }
}

fn settle_with(
    window: &WindowHandle<ConnectionManagerWindow>,
    cx: &mut TestAppContext,
    outcome: TestConnectionOutcome,
    cancelled: bool,
) {
    window
        .update(cx, |manager, _window, cx| {
            manager.apply_test_settlement(
                TestSettlement::from_outcome("settled".to_string(), outcome),
                cancelled,
                cx,
            );
        })
        .expect("window available");
}

fn failed_probe_outcome(failure: TestConnectionFailure) -> TestConnectionOutcome {
    TestConnectionOutcome {
        probe: Err(failure),
        cleanup: Ok(()),
    }
}

#[::core::prelude::v1::test]
fn an_uncancelled_driver_probe_failure_is_reported_once_and_shown_inline() {
    let mut cx = TestAppContext::single();
    let host = init_form_test_runtime(&mut cx);
    let app_state = test_app_state(
        &mut cx,
        SecretStoreFixture::new(PasswordSaveOutcome::Success),
    );
    let window = open_new_profile_window(app_state.clone(), &mut cx);
    window
        .update(&mut cx, |manager, _window, _cx| {
            let inner = manager.form.selected_driver.clone().expect("driver");
            manager.form.selected_driver = Some(Arc::new(FailingProbeDriver(inner)));
        })
        .expect("swap in the failing driver");

    window
        .update(&mut cx, |manager, window, cx| {
            manager.test_connection(window, cx)
        })
        .expect("test connection starts");
    cx.run_until_parked();

    assert!(matches!(
        test_connection_task(&app_state, &mut cx)
            .expect("task")
            .status,
        dbflux_core::TaskStatus::Failed(_)
    ));
    window
        .update(&mut cx, |manager, _, _| {
            assert!(matches!(manager.test_status, TestStatus::Failed));
            assert!(
                manager
                    .test_error
                    .as_deref()
                    .is_some_and(|error| error.contains(PROBE_FAILURE)),
                "the inline error stays: {:?}",
                manager.test_error
            );
        })
        .expect("window available");
    assert_eq!(
        cx.update(|cx| host.read(cx).toast_count()),
        1,
        "the driver failure is reported once"
    );
    window
        .update(&mut cx, |_, window, _| window.remove_window())
        .expect("close the test window");
}

#[::core::prelude::v1::test]
fn an_aborting_hook_failure_is_shown_inline_and_reported_once() {
    let mut cx = TestAppContext::single();
    let host = init_form_test_runtime(&mut cx);
    let app_state = test_app_state(
        &mut cx,
        SecretStoreFixture::new(PasswordSaveOutcome::Success),
    );
    let window = open_new_profile_window(app_state, &mut cx);

    settle_with(
        &window,
        &mut cx,
        failed_probe_outcome(TestConnectionFailure::Hook("hook aborted".to_string())),
        false,
    );

    window
        .update(&mut cx, |manager, _, _| {
            assert!(matches!(manager.test_status, TestStatus::Failed));
            assert_eq!(manager.test_error.as_deref(), Some("hook aborted"));
        })
        .expect("window available");
    assert_eq!(
        cx.update(|cx| host.read(cx).toast_count()),
        1,
        "the aborting hook failure is reported once"
    );
    window
        .update(&mut cx, |_, window, _| window.remove_window())
        .expect("close the test window");
}

#[::core::prelude::v1::test]
fn a_cancelled_probe_with_failed_cleanup_surfaces_the_cleanup_failure() {
    let mut cx = TestAppContext::single();
    let host = init_form_test_runtime(&mut cx);
    let app_state = test_app_state(
        &mut cx,
        SecretStoreFixture::new(PasswordSaveOutcome::Success),
    );
    let window = open_new_profile_window(app_state, &mut cx);

    settle_with(
        &window,
        &mut cx,
        TestConnectionOutcome {
            probe: Err(TestConnectionFailure::Probe("Cancelled".to_string())),
            cleanup: Err("detached hook still running".to_string()),
        },
        true,
    );

    window
        .update(&mut cx, |manager, _, _| {
            assert!(matches!(manager.test_status, TestStatus::Failed));
            let error = manager.test_error.as_deref().expect("cleanup error shown");
            assert!(error.contains("detached hook still running"), "{error}");
            assert!(
                !error.contains("Cancelled"),
                "the cancelled probe outcome stays separate: {error}"
            );
        })
        .expect("window available");
    assert_eq!(
        cx.update(|cx| host.read(cx).toast_count()),
        1,
        "the cleanup failure is reported once"
    );
    window
        .update(&mut cx, |_, window, _| window.remove_window())
        .expect("close the test window");
}

#[::core::prelude::v1::test]
fn a_cancelled_probe_with_clean_cleanup_stays_quiet() {
    let mut cx = TestAppContext::single();
    let host = init_form_test_runtime(&mut cx);
    let app_state = test_app_state(
        &mut cx,
        SecretStoreFixture::new(PasswordSaveOutcome::Success),
    );
    let window = open_new_profile_window(app_state, &mut cx);

    settle_with(
        &window,
        &mut cx,
        failed_probe_outcome(TestConnectionFailure::Probe("Cancelled".to_string())),
        true,
    );

    window
        .update(&mut cx, |manager, _, _| {
            assert!(matches!(manager.test_status, TestStatus::None));
            assert!(manager.test_error.is_none());
        })
        .expect("window available");
    assert_eq!(cx.update(|cx| host.read(cx).toast_count()), 0);
    window
        .update(&mut cx, |_, window, _| window.remove_window())
        .expect("close the test window");
}

#[::core::prelude::v1::test]
fn a_second_test_activation_while_one_is_running_is_rejected() {
    let mut cx = TestAppContext::single();
    let _host = init_form_test_runtime(&mut cx);
    let app_state = test_app_state(
        &mut cx,
        SecretStoreFixture::new(PasswordSaveOutcome::Success),
    );
    let window = open_new_profile_window(app_state.clone(), &mut cx);

    for _ in 0..2 {
        window
            .update(&mut cx, |manager, window, cx| {
                manager.test_connection(window, cx)
            })
            .expect("test connection starts");
    }

    let started = cx.read(|cx| {
        app_state
            .read(cx)
            .tasks()
            .recent_tasks(10)
            .into_iter()
            .filter(|task| task.kind == dbflux_core::TaskKind::TestConnection)
            .count()
    });
    assert_eq!(started, 1, "the repeat activation starts no second test");
    window
        .update(&mut cx, |manager, _, _| {
            assert!(matches!(manager.test_status, TestStatus::Testing));
        })
        .expect("window available");

    cx.run_until_parked();
    window
        .update(&mut cx, |_, window, _| window.remove_window())
        .expect("close the test window");
}
