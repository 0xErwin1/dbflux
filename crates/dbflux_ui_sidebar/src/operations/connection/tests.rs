use super::wait_for_connection_teardown;
use dbflux_core::CancelToken;
use gpui::TestAppContext;
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

type GatedTeardownThread = (
    std::thread::JoinHandle<Result<(), dbflux_core::DbError>>,
    Arc<(Mutex<bool>, Condvar)>,
);

/// Spawns a thread that blocks until the returned gate is released,
/// standing in for a driver teardown stuck on cancel/close work.
fn gated_teardown_thread() -> GatedTeardownThread {
    let gate = Arc::new((Mutex::new(false), Condvar::new()));

    let thread_gate = gate.clone();
    // The JoinHandle type is fixed by `wait_for_connection_teardown` and the
    // public `ConnectionTeardownHandle` alias in dbflux_core; boxing the Err
    // here would change the production API shape these tests exercise.
    #[expect(
        clippy::result_large_err,
        reason = "the thread must return the production Result<(), DbError> because \
                  wait_for_connection_teardown and the public ConnectionTeardownHandle \
                  alias fix that type"
    )]
    let handle = std::thread::spawn(move || {
        let (lock, condvar) = &*thread_gate;

        let mut released = lock.lock().expect("gate lock");
        while !*released {
            released = condvar.wait(released).expect("gate wait");
        }
        Ok(())
    });

    (handle, gate)
}

fn release_gate(gate: &Arc<(Mutex<bool>, Condvar)>) {
    let (lock, condvar) = &**gate;
    *lock.lock().expect("gate lock") = true;
    condvar.notify_all();
}

const CONNECTION_TOAST_KEYS: [&str; 13] = [
    "sidebar.task.connecting",
    "sidebar.task.disconnecting",
    "sidebar.task.connection_hook_cancelled",
    "sidebar.task.post_connect_hook_cancelled",
    "sidebar.task.disconnect_hook_cancelled",
    "sidebar.task.post_disconnect_hook_cancelled",
    "sidebar.toast.connection_already_pending",
    "sidebar.toast.background_task_limit",
    "sidebar.toast.connection_cancelled_by_hook",
    "sidebar.toast.connection_cancelled_by_post_connect_hook",
    "sidebar.toast.disconnect_cancelled_by_hook",
    "sidebar.toast.disconnected_hook_cancelled",
    "sidebar.toast.profile_not_found",
];

#[test]
fn connection_toast_keys_resolve_in_both_locales() {
    for key in CONNECTION_TOAST_KEYS {
        for locale in ["en", "es"] {
            let value = dbflux_i18n::t!(key, locale = locale);

            assert_ne!(value, key, "missing translation for {locale}.{key}");
            assert_ne!(
                value,
                format!("{locale}.{key}"),
                "translation fell back to the miss sentinel for {locale}.{key}"
            );
        }
    }
}

#[test]
fn connected_toast_differs_between_locales() {
    let english = crate::labels::connected_toast_label("prod-db", 0);
    let spanish_key = dbflux_i18n::t!("sidebar.toast.connected.plain", locale = "es");

    assert_ne!(english, spanish_key);
    assert!(english.contains("prod-db"));
}

#[gpui::test]
fn wait_for_connection_teardown_waits_until_thread_completes(cx: &mut TestAppContext) {
    let (teardown, gate) = gated_teardown_thread();
    let cancel_token = CancelToken::new();

    let (done_sender, done_receiver) = mpsc::channel();
    cx.update(|cx| {
        cx.spawn(async move |cx| {
            let warning = wait_for_connection_teardown(teardown, &cancel_token, cx).await;
            done_sender.send(warning).expect("test completion receiver");
        })
        .detach();
    });

    cx.run_until_parked();
    cx.executor().advance_clock(Duration::from_millis(200));
    cx.run_until_parked();
    assert!(
        done_receiver.try_recv().is_err(),
        "wait must not complete while the teardown thread is still running"
    );

    release_gate(&gate);

    // The teardown is a real OS thread while timers use the fake test
    // clock, so retry a few polls to absorb scheduling latency.
    let mut warning = None;
    for _ in 0..100 {
        cx.executor().advance_clock(Duration::from_millis(50));
        cx.run_until_parked();

        match done_receiver.try_recv() {
            Ok(result) => {
                warning = Some(result);
                break;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(5)),
        }
    }

    assert_eq!(
        warning,
        Some(None),
        "wait must complete without a warning once the teardown thread finishes"
    );
}

#[gpui::test]
fn wait_for_connection_teardown_returns_cleanup_error_once(cx: &mut TestAppContext) {
    let teardown = {
        #[expect(
            clippy::result_large_err,
            reason = "wait_for_connection_teardown joins a JoinHandle<Result<(), DbError>> \
                      fixed by the public ConnectionTeardownHandle alias, so the spawned \
                      closure must construct the production-sized DbError"
        )]
        std::thread::spawn(|| Err(dbflux_core::DbError::query_failed("close failed")))
    };
    // The helper polls on the executor's virtual clock while the thread
    // runs in real time; let the thread finish first so the poll loop
    // cannot outrun it and the test observes the join result.
    while !teardown.is_finished() {
        std::thread::yield_now();
    }
    let cancel_token = CancelToken::new();
    let (done_sender, done_receiver) = mpsc::channel();
    cx.update(|cx| {
        cx.spawn(async move |cx| {
            done_sender
                .send(wait_for_connection_teardown(teardown, &cancel_token, cx).await)
                .expect("test completion receiver");
        })
        .detach();
    });
    for _ in 0..10 {
        cx.executor().advance_clock(Duration::from_millis(50));
        cx.run_until_parked();
        if let Ok(warning) = done_receiver.try_recv() {
            assert_eq!(
                warning.as_deref(),
                Some("connection cleanup failed: close failed")
            );
            return;
        }
    }
    panic!("cleanup error was not reported");
}

#[gpui::test]
fn wait_for_connection_teardown_stops_on_cancellation(cx: &mut TestAppContext) {
    let (teardown, gate) = gated_teardown_thread();
    let cancel_token = CancelToken::new();
    cancel_token.cancel();

    let (done_sender, done_receiver) = mpsc::channel();
    cx.update(|cx| {
        cx.spawn(async move |cx| {
            let warning = wait_for_connection_teardown(teardown, &cancel_token, cx).await;
            done_sender.send(warning).expect("test completion receiver");
        })
        .detach();
    });

    cx.run_until_parked();
    assert_eq!(
        done_receiver.try_recv().ok(),
        Some(None),
        "a cancelled disconnect must stop waiting even while teardown is running"
    );

    release_gate(&gate);
}

// --- Prompting for a password that is neither saved nor sourced ---

use crate::Sidebar;
use crate::table_loading::object_tree_adapter_tests::{AdapterFakeConnection, test_app_state};
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_ui_base::app_state_entity::AppStateEntity;
use gpui::{Entity, VisualTestContext};
use std::sync::LazyLock;
use uuid::Uuid;

type RecordedPasswords = Arc<Mutex<Vec<Option<String>>>>;

static PASSWORD_DRIVER_FORM: LazyLock<dbflux_core::DriverFormDef> =
    LazyLock::new(|| dbflux_core::DriverFormDef { tabs: vec![] });

/// A driver that authenticates with a password and records the password
/// every connect receives.
struct PasswordRecordingDriver {
    metadata: dbflux_core::DriverMetadata,
    passwords: RecordedPasswords,
    script: ConnectScript,
}

/// What a driver connect does besides recording the password: runs the
/// closure, which may fail the connect, and counts the schema fetches of the
/// connection it returns.
#[derive(Clone, Default)]
struct ConnectScript {
    on_connect: Arc<Mutex<Option<ConnectHook>>>,
    schema_fetches: Arc<Mutex<Vec<Arc<std::sync::atomic::AtomicUsize>>>>,
}

type ConnectHook = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

impl dbflux_core::DbDriver for PasswordRecordingDriver {
    fn kind(&self) -> dbflux_core::DbKind {
        dbflux_core::DbKind::Postgres
    }

    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        &self.metadata
    }

    fn form_definition(&self) -> &dbflux_core::DriverFormDef {
        &PASSWORD_DRIVER_FORM
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        "builtin:sidebar-password-test".into()
    }

    fn build_config(
        &self,
        _: &dbflux_core::FormValues,
    ) -> Result<dbflux_core::DbConfig, dbflux_core::DbError> {
        Ok(dbflux_core::DbConfig::default_postgres())
    }

    fn extract_values(&self, _: &dbflux_core::DbConfig) -> dbflux_core::FormValues {
        dbflux_core::FormValues::new()
    }

    fn connect_with_secrets(
        &self,
        _: &dbflux_core::ConnectionProfile,
        password: Option<&SecretString>,
        _: Option<&SecretString>,
    ) -> Result<Box<dyn dbflux_core::Connection>, dbflux_core::DbError> {
        self.passwords
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(password.map(|password| password.expose_secret().to_string()));

        let hook = self
            .script
            .on_connect
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(hook) = hook {
            hook().map_err(dbflux_core::DbError::connection_failed)?;
        }

        let fake = AdapterFakeConnection::lazy();
        self.script
            .schema_fetches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(fake.schema_fetch_counter());

        match Arc::try_unwrap(fake) {
            Ok(connection) => Ok(Box::new(connection)),
            Err(_) => Err(dbflux_core::DbError::NotSupported(
                "fake connection shared".into(),
            )),
        }
    }

    fn test_connection(
        &self,
        _: &dbflux_core::ConnectionProfile,
    ) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }
}

struct PasswordFixture {
    state: Entity<AppStateEntity>,
    sidebar: Entity<Sidebar>,
    profile_id: Uuid,
    passwords: RecordedPasswords,
    prompts: Arc<Mutex<Vec<Uuid>>>,
    script: ConnectScript,
}

fn password_fixture(
    save_password: bool,
    cx: &mut TestAppContext,
) -> (PasswordFixture, &mut VisualTestContext) {
    let state = test_app_state(cx);
    let passwords: RecordedPasswords = Arc::new(Mutex::new(Vec::new()));
    let script = ConnectScript::default();
    let metadata = dbflux_core::DriverMetadataBuilder::new(
        "sidebar-password-test",
        "SidebarPasswordTest",
        dbflux_core::DatabaseCategory::Relational,
        dbflux_core::QueryLanguage::Sql,
    )
    .capabilities(dbflux_core::DriverCapabilities::AUTHENTICATION)
    .build();

    let mut profile =
        dbflux_core::ConnectionProfile::new("orders", dbflux_core::DbConfig::default_postgres());
    profile.save_password = save_password;
    let profile_id = profile.id;

    state.update(cx, |state, _| {
        state.inner.facade.connections.drivers.insert(
            "postgres".into(),
            Arc::new(PasswordRecordingDriver {
                metadata,
                passwords: passwords.clone(),
                script: script.clone(),
            }),
        );
        state.profiles_mut().push(profile);
        state
            .connection_tree_mut()
            .add_node(dbflux_core::ConnectionTreeNode::new_connection_ref(
                profile_id, None, 1000,
            ));
    });

    let (sidebar, cx) = cx.add_window_view(|window, cx| Sidebar::new(state.clone(), window, cx));

    let prompts = Arc::new(Mutex::new(Vec::new()));
    cx.update(|_, cx| {
        let prompts = prompts.clone();
        cx.subscribe(&sidebar, move |_, event: &crate::SidebarEvent, _| {
            if let crate::SidebarEvent::RequestConnectionPassword { prompt } = event {
                prompts.lock().expect("prompts").push(prompt.profile_id);
            }
        })
        .detach();
    });

    (
        PasswordFixture {
            state,
            sidebar,
            profile_id,
            passwords,
            prompts,
            script,
        },
        cx,
    )
}

fn recorded(passwords: &RecordedPasswords) -> Vec<Option<String>> {
    passwords.lock().expect("recorded passwords").clone()
}

#[gpui::test]
fn connect_asks_for_an_unsaved_password_before_connecting(cx: &mut TestAppContext) {
    let (fixture, cx) = password_fixture(false, cx);
    let profile_id = fixture.profile_id;

    fixture
        .sidebar
        .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
    cx.run_until_parked();

    assert_eq!(*fixture.prompts.lock().expect("prompts"), vec![profile_id]);
    assert!(
        recorded(&fixture.passwords).is_empty(),
        "nothing connects before the prompt"
    );
    fixture.state.read_with(cx, |state, _| {
        assert!(!state.connections().contains_key(&profile_id));
        assert!(!state.is_operation_pending(profile_id, None));
    });
}

#[gpui::test]
fn prompted_password_connects_and_serves_the_live_session(cx: &mut TestAppContext) {
    let (fixture, cx) = password_fixture(false, cx);
    let profile_id = fixture.profile_id;

    fixture.sidebar.update(cx, |sidebar, cx| {
        sidebar.connect_to_profile_with_password(profile_id, SecretString::from(""), false, cx)
    });
    cx.run_until_parked();

    assert_eq!(recorded(&fixture.passwords), vec![Some(String::new())]);
    fixture.state.read_with(cx, |state, _| {
        assert!(state.connections().contains_key(&profile_id));
        assert_eq!(
            state
                .inner
                .facade
                .connections
                .session_password(profile_id)
                .map(|password| password.expose_secret().to_string()),
            Some(String::new()),
            "per-database connections and switches reuse the prompted password"
        );
        assert!(
            state.remembered_connection_password(profile_id).is_none(),
            "an unchecked remember box keeps nothing past the session"
        );
    });
    assert!(fixture.prompts.lock().expect("prompts").is_empty());
}

#[gpui::test]
fn remembered_password_connects_without_asking_again(cx: &mut TestAppContext) {
    let (fixture, cx) = password_fixture(false, cx);
    let profile_id = fixture.profile_id;

    fixture.state.update(cx, |state, _| {
        state.remember_connection_password(profile_id, SecretString::from("typed"));
    });
    fixture
        .sidebar
        .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
    cx.run_until_parked();

    assert!(fixture.prompts.lock().expect("prompts").is_empty());
    assert_eq!(
        recorded(&fixture.passwords),
        vec![Some("typed".to_string())]
    );
    fixture.state.read_with(cx, |state, _| {
        assert!(state.connections().contains_key(&profile_id))
    });
}

#[gpui::test]
fn remember_box_keeps_the_prompted_password_for_the_session(cx: &mut TestAppContext) {
    let (fixture, cx) = password_fixture(false, cx);
    let profile_id = fixture.profile_id;

    fixture.sidebar.update(cx, |sidebar, cx| {
        sidebar.connect_to_profile_with_password(profile_id, SecretString::from("typed"), true, cx)
    });
    cx.run_until_parked();

    fixture.state.read_with(cx, |state, _| {
        assert_eq!(
            state
                .remembered_connection_password(profile_id)
                .map(|password| password.expose_secret().to_string()),
            Some("typed".to_string())
        );
    });
}

#[gpui::test]
fn saved_password_profile_connects_without_asking(cx: &mut TestAppContext) {
    let (fixture, cx) = password_fixture(true, cx);
    let profile_id = fixture.profile_id;

    fixture
        .sidebar
        .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
    cx.run_until_parked();

    assert!(fixture.prompts.lock().expect("prompts").is_empty());
    assert_eq!(recorded(&fixture.passwords).len(), 1);
    fixture.state.read_with(cx, |state, _| {
        assert!(state.connections().contains_key(&profile_id))
    });
}

/// Clicks the row twice through the real element. The events are
/// dispatched without parking the scheduler, so a connect the click starts
/// stays in flight until the test lets the executor run.
fn double_click_row(item_id: &str, cx: &mut VisualTestContext) {
    use gpui::{InputEvent, Modifiers, MouseButton, MouseDownEvent, MouseUpEvent};

    let selector: &'static str = Box::leak(format!("row-{item_id}").into_boxed_str());
    let bounds = cx.debug_bounds(selector).expect("the profile row is drawn");
    let position = bounds.center();

    for click_count in [1, 2] {
        let down = MouseDownEvent {
            position,
            modifiers: Modifiers::default(),
            button: MouseButton::Left,
            click_count,
            first_mouse: false,
        };
        let up = MouseUpEvent {
            position,
            modifiers: Modifiers::default(),
            button: MouseButton::Left,
            click_count,
        };

        cx.update(|window, cx| {
            window.dispatch_event(down.to_platform_input(), cx);
            window.dispatch_event(up.to_platform_input(), cx);
        });
    }
}

fn last_toast_title(cx: &mut VisualTestContext) -> Option<String> {
    cx.update(|_, cx| {
        cx.global::<dbflux_ui_base::toast::ToastGlobal>()
            .host
            .read(cx)
            .last_toast_title()
    })
}

/// The number of toasts on the stack and the title of the newest.
fn toast_stack(cx: &mut VisualTestContext) -> (usize, Option<String>) {
    cx.update(|_, cx| {
        let host = cx
            .global::<dbflux_ui_base::toast::ToastGlobal>()
            .host
            .read(cx);
        (host.toast_count(), host.last_toast_title())
    })
}

#[gpui::test]
fn double_click_on_a_connection_connects_once_without_an_error(cx: &mut TestAppContext) {
    let (fixture, cx) = password_fixture(true, cx);
    let profile_id = fixture.profile_id;
    let item_id = dbflux_core::SchemaNodeId::Profile { profile_id }.to_string();
    let pending_title = crate::labels::connection_already_pending_toast_label();

    cx.run_until_parked();

    // The connect runs on the background executor, which does not advance
    // until the scheduler is parked: the first attempt holds its pending
    // slot while the second double click arrives.
    double_click_row(&item_id, cx);
    fixture.state.read_with(cx, |state, _| {
        assert!(state.is_operation_pending(profile_id, None));
    });
    assert!(recorded(&fixture.passwords).is_empty());

    double_click_row(&item_id, cx);
    fixture.state.read_with(cx, |state, _| {
        assert!(
            state.is_operation_pending(profile_id, None),
            "the second request met the first attempt's pending slot"
        );
    });
    let (during_count, during_title) = toast_stack(cx);
    assert_ne!(during_title, Some(pending_title.clone()));

    cx.run_until_parked();

    assert_eq!(recorded(&fixture.passwords).len(), 1);
    fixture.state.read_with(cx, |state, _| {
        assert!(state.connections().contains_key(&profile_id));
    });
    // An earlier error toast would sit under the success one, so the
    // whole stack is counted, not only its top.
    let (count, title) = toast_stack(cx);
    assert_ne!(title, Some(pending_title));
    assert_eq!(
        count,
        during_count + 1,
        "only the success toast was added; no error hides beneath it"
    );
}

#[gpui::test]
fn double_click_while_a_connect_is_in_flight_reports_nothing(cx: &mut TestAppContext) {
    let (fixture, cx) = password_fixture(true, cx);
    let profile_id = fixture.profile_id;
    let item_id = dbflux_core::SchemaNodeId::Profile { profile_id }.to_string();

    fixture.state.update(cx, |state, _| {
        assert!(state.start_pending_operation(profile_id, None));
    });
    double_click_row(&item_id, cx);
    cx.run_until_parked();

    assert!(recorded(&fixture.passwords).is_empty());
    assert_ne!(
        last_toast_title(cx),
        Some(crate::labels::connection_already_pending_toast_label())
    );
}

#[gpui::test]
fn connect_requested_while_pending_reports_nothing(cx: &mut TestAppContext) {
    let (fixture, cx) = password_fixture(true, cx);
    let profile_id = fixture.profile_id;

    fixture.state.update(cx, |state, _| {
        assert!(state.start_pending_operation(profile_id, None));
    });
    fixture
        .sidebar
        .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
    cx.run_until_parked();

    assert!(recorded(&fixture.passwords).is_empty());
    assert_ne!(
        last_toast_title(cx),
        Some(crate::labels::connection_already_pending_toast_label())
    );
}

// --- The connect pipeline as a parent task with its stages as children ---

use dbflux_core::{TaskKind, TaskSnapshot, TaskStatus};
use std::future::Future;
use std::pin::Pin;

type AuthFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, dbflux_core::DbError>> + Send + 'a>>;

/// An auth provider whose session is either valid or waits for a login
/// that never ends, so a connect can be held inside its auth stage.
struct ScriptedAuthProvider {
    login_never_ends: bool,
}

impl dbflux_core::DynAuthProvider for ScriptedAuthProvider {
    fn provider_id(&self) -> &str {
        "scripted-auth"
    }

    fn display_name(&self) -> &str {
        "Test Auth"
    }

    fn form_def(&self) -> &dbflux_core::AuthFormDef {
        static FORM: std::sync::OnceLock<dbflux_core::AuthFormDef> = std::sync::OnceLock::new();
        FORM.get_or_init(|| dbflux_core::AuthFormDef { tabs: vec![] })
    }

    fn validate_session<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _profile: &'life1 dbflux_core::AuthProfile,
    ) -> AuthFuture<'async_trait, dbflux_core::AuthSessionState>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let state = if self.login_never_ends {
            dbflux_core::AuthSessionState::LoginRequired
        } else {
            dbflux_core::AuthSessionState::Valid { expires_at: None }
        };
        Box::pin(async move { Ok(state) })
    }

    fn login<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _profile: &'life1 dbflux_core::AuthProfile,
        _url_callback: dbflux_core::auth::UrlCallback,
    ) -> AuthFuture<'async_trait, dbflux_core::AuthSession>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(std::future::pending())
    }

    fn resolve_credentials<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _profile: &'life1 dbflux_core::AuthProfile,
    ) -> AuthFuture<'async_trait, dbflux_core::ResolvedCredentials>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { Ok(dbflux_core::ResolvedCredentials::default()) })
    }
}

fn true_hook() -> dbflux_core::ConnectionHook {
    dbflux_core::ConnectionHook {
        enabled: true,
        kind: dbflux_core::HookKind::Command {
            command: "true".to_string(),
            args: Vec::new(),
        },
        cwd: None,
        env: Default::default(),
        inherit_env: true,
        env_denylist: Vec::new(),
        timeout_ms: None,
        execution_mode: Default::default(),
        ready_signal: None,
        on_failure: Default::default(),
    }
}

/// A pipeline connect for a profile shaped by `shape`, with the scripted
/// auth provider registered.
fn pipeline_fixture(
    login_never_ends: bool,
    shape: impl FnOnce(&mut dbflux_core::ConnectionProfile),
    cx: &mut TestAppContext,
) -> (PasswordFixture, &mut VisualTestContext) {
    let (fixture, cx) = password_fixture(true, cx);
    let profile_id = fixture.profile_id;

    fixture.state.update(cx, |state, _| {
        state.register_auth_provider(Arc::new(ScriptedAuthProvider { login_never_ends }));
        let auth = dbflux_core::AuthProfile::new(
            "scripted",
            "scripted-auth",
            std::collections::HashMap::new(),
        );
        let auth_id = auth.id;
        state.add_auth_profile(auth);

        if let Some(profile) = state.profiles_mut().iter_mut().find(|p| p.id == profile_id) {
            profile.auth_profile_id = Some(auth_id);
            shape(profile);
        }
    });

    (fixture, cx)
}

fn connect_attempts(state: &AppStateEntity) -> Vec<TaskSnapshot> {
    state
        .tasks()
        .recent_tasks(usize::MAX)
        .into_iter()
        .filter(|task| task.kind == TaskKind::Connect && task.parent_id.is_none())
        .collect()
}

/// The (name, status) of the stages of the only connect attempt.
fn stage_rows(fixture: &PasswordFixture, cx: &mut VisualTestContext) -> Vec<(String, TaskStatus)> {
    fixture.state.read_with(cx, |state, _| {
        let attempts = connect_attempts(state);
        assert_eq!(attempts.len(), 1, "one connect attempt");
        state
            .child_tasks(attempts[0].id)
            .into_iter()
            .map(|stage| (stage.description, stage.status))
            .collect()
    })
}

fn attempt_status(fixture: &PasswordFixture, cx: &mut VisualTestContext) -> TaskStatus {
    fixture
        .state
        .read_with(cx, |state, _| connect_attempts(state)[0].status.clone())
}

fn statuses(rows: &[(String, TaskStatus)]) -> Vec<TaskStatus> {
    rows.iter().map(|(_, status)| status.clone()).collect()
}

#[gpui::test]
fn a_connect_registers_its_stages_in_order_under_one_task(cx: &mut TestAppContext) {
    let (fixture, cx) = pipeline_fixture(
        false,
        |profile| {
            profile.value_refs.insert(
                "host".to_string(),
                dbflux_core::values::ValueRef::literal("db"),
            );
            profile.access_kind = Some(dbflux_core::AccessKind::Ssh {
                ssh_tunnel_profile_id: Uuid::new_v4(),
            });
            profile.hooks = Some(dbflux_core::ConnectionHooks {
                pre_connect: vec![true_hook()],
                post_connect: vec![true_hook()],
                ..Default::default()
            });
        },
        cx,
    );
    let profile_id = fixture.profile_id;

    fixture
        .sidebar
        .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));

    let rows = stage_rows(&fixture, cx);
    let names: Vec<&str> = rows.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
        [
            "Pre-connect hooks",
            "Authenticating via Test Auth",
            "Resolving values",
            "Opening tunnel",
            "Connecting driver",
            "Fetching schema",
            "Post-connect hooks",
        ]
    );
    assert_eq!(statuses(&rows), vec![TaskStatus::Pending; 7]);
    assert_eq!(attempt_status(&fixture, cx), TaskStatus::Running);
}

#[gpui::test]
fn a_profile_without_auth_or_hooks_registers_only_the_stages_that_apply(cx: &mut TestAppContext) {
    let (fixture, cx) = pipeline_fixture(
        false,
        |profile| {
            profile.auth_profile_id = None;
            profile.access_kind = Some(dbflux_core::AccessKind::Direct);
        },
        cx,
    );
    let profile_id = fixture.profile_id;

    fixture
        .sidebar
        .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));

    let names: Vec<String> = stage_rows(&fixture, cx)
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    assert_eq!(names, ["Connecting driver", "Fetching schema"]);
}

#[gpui::test]
fn a_successful_connect_completes_every_stage_and_registers_the_connection(
    cx: &mut TestAppContext,
) {
    let (fixture, cx) = pipeline_fixture(false, |_| {}, cx);
    let profile_id = fixture.profile_id;
    let watchers = Arc::new(Mutex::new(Vec::new()));
    cx.update(|_, cx| {
        let watchers = watchers.clone();
        cx.subscribe(
            &fixture.sidebar,
            move |_, event: &crate::SidebarEvent, _| {
                if let crate::SidebarEvent::PipelineStarted { watcher, .. } = event {
                    watchers.lock().expect("watchers").push(watcher.clone());
                }
            },
        )
        .detach();
    });

    fixture
        .sidebar
        .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
    cx.run_until_parked();

    let rows = stage_rows(&fixture, cx);
    assert_eq!(
        rows.iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        [
            "Authenticating via Test Auth",
            "Connecting driver",
            "Fetching schema"
        ]
    );
    assert_eq!(statuses(&rows), vec![TaskStatus::Completed; 3]);
    assert_eq!(attempt_status(&fixture, cx), TaskStatus::Completed);
    fixture.state.read_with(cx, |state, _| {
        assert!(state.connections().contains_key(&profile_id));
        assert!(!state.is_operation_pending(profile_id, None));
    });
    assert_eq!(recorded(&fixture.passwords).len(), 1);

    let watchers = watchers.lock().expect("watchers");
    assert_eq!(watchers.len(), 1, "one pipeline was announced");
    assert!(matches!(
        *watchers[0].borrow(),
        dbflux_core::PipelineState::Connected
    ));
}

#[gpui::test]
fn cancelling_during_authentication_skips_the_later_stages_and_connects_nothing(
    cx: &mut TestAppContext,
) {
    let (fixture, cx) = pipeline_fixture(true, |_| {}, cx);
    let profile_id = fixture.profile_id;

    fixture
        .sidebar
        .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
    cx.run_until_parked();

    assert_eq!(
        statuses(&stage_rows(&fixture, cx)),
        [
            TaskStatus::Running,
            TaskStatus::Pending,
            TaskStatus::Pending
        ],
        "the pipeline waits for the sign-in"
    );

    fixture.state.update(cx, |state, _| {
        assert_eq!(
            state.cancel_running_connect_tasks_for_profile(profile_id),
            1
        );
    });
    assert_eq!(
        statuses(&stage_rows(&fixture, cx)),
        [
            TaskStatus::Cancelling,
            TaskStatus::Skipped,
            TaskStatus::Skipped
        ],
        "the stage waits for the worker; the rest never started"
    );
    assert_eq!(attempt_status(&fixture, cx), TaskStatus::Cancelling);

    cx.run_until_parked();

    assert_eq!(
        statuses(&stage_rows(&fixture, cx)),
        [
            TaskStatus::Cancelled,
            TaskStatus::Skipped,
            TaskStatus::Skipped
        ]
    );
    assert_eq!(attempt_status(&fixture, cx), TaskStatus::Cancelled);
    fixture.state.read_with(cx, |state, _| {
        assert!(!state.connections().contains_key(&profile_id));
        assert!(!state.is_operation_pending(profile_id, None));
    });
    assert!(recorded(&fixture.passwords).is_empty());
}

#[gpui::test]
fn a_connect_for_a_profile_that_has_a_connect_task_is_refused_by_the_task_manager(
    cx: &mut TestAppContext,
) {
    let (fixture, cx) = pipeline_fixture(false, |_| {}, cx);
    let profile_id = fixture.profile_id;

    // Only the task is registered; the pending-operation set stays empty,
    // so a refusal can only come from the task manager.
    fixture.state.update(cx, |state, _| {
        state
            .try_start_task(
                dbflux_core::TaskRequest::new(TaskKind::Connect, "connecting").target(
                    dbflux_core::TaskTarget {
                        profile_id,
                        database: None,
                    },
                ),
            )
            .expect("first connect task starts");
    });

    fixture
        .sidebar
        .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));
    cx.run_until_parked();

    fixture.state.read_with(cx, |state, _| {
        assert_eq!(connect_attempts(state).len(), 1, "no second attempt");
        assert!(!state.connections().contains_key(&profile_id));
    });
    assert!(recorded(&fixture.passwords).is_empty());
}

/// Starts a connect whose driver runs `during_connect` once it is called,
/// with the attempt's own token in hand, so the test can cancel while the
/// driver connects.
fn connect_with_driver_script(
    fixture: &PasswordFixture,
    during_connect: impl Fn(&dbflux_core::CancelToken) -> Result<(), String> + Send + Sync + 'static,
    cx: &mut VisualTestContext,
) {
    let profile_id = fixture.profile_id;

    fixture
        .sidebar
        .update(cx, |sidebar, cx| sidebar.connect_to_profile(profile_id, cx));

    let token = fixture.state.read_with(cx, |state, _| {
        let attempt = connect_attempts(state).remove(0).id;
        state.tasks().cancel_token(attempt).expect("attempt token")
    });
    *fixture.script.on_connect.lock().expect("connect script") =
        Some(Arc::new(move || during_connect(&token)));
}

#[gpui::test]
fn a_cancel_during_the_driver_connect_skips_the_schema_fetch(cx: &mut TestAppContext) {
    let (fixture, cx) = pipeline_fixture(false, |_| {}, cx);
    connect_with_driver_script(
        &fixture,
        |token| {
            token.cancel();
            Ok(())
        },
        cx,
    );

    cx.run_until_parked();

    let fetches = fixture
        .script
        .schema_fetches
        .lock()
        .expect("schema fetches")
        .iter()
        .map(|count| count.load(std::sync::atomic::Ordering::SeqCst))
        .collect::<Vec<_>>();
    assert_eq!(
        fetches,
        [0],
        "a cancelled connect must not fetch the schema"
    );
    assert_eq!(attempt_status(&fixture, cx), TaskStatus::Cancelled);
    fixture.state.read_with(cx, |state, _| {
        assert!(!state.connections().contains_key(&fixture.profile_id));
        assert!(!state.is_operation_pending(fixture.profile_id, None));
    });
}

#[gpui::test]
fn a_driver_error_after_a_cancel_reads_cancelled_and_reports_no_failure(cx: &mut TestAppContext) {
    let (fixture, cx) = pipeline_fixture(false, |_| {}, cx);
    connect_with_driver_script(
        &fixture,
        |token| {
            token.cancel();
            Err("timed out".to_string())
        },
        cx,
    );

    let ((), events) = dbflux_ui_base::user_error::capture::capture_user_error_events(|| {
        cx.run_until_parked();
    });

    assert_eq!(attempt_status(&fixture, cx), TaskStatus::Cancelled);
    assert!(events.is_empty(), "a cancel is not an error: {events:?}");
    fixture.state.read_with(cx, |state, _| {
        assert_eq!(
            state.connect_failure(fixture.profile_id),
            None,
            "the sidebar must not show the cancelled connect as failed"
        );
        assert!(!state.is_operation_pending(fixture.profile_id, None));
    });
}

#[gpui::test]
fn a_driver_failure_fails_its_stage_and_is_reported_once(cx: &mut TestAppContext) {
    let (fixture, cx) = pipeline_fixture(false, |_| {}, cx);
    connect_with_driver_script(&fixture, |_| Err("refused".to_string()), cx);

    let ((), events) = dbflux_ui_base::user_error::capture::capture_user_error_events(|| {
        cx.run_until_parked();
    });

    assert_eq!(events.len(), 1, "reported exactly once: {events:?}");
    let rows = stage_rows(&fixture, cx);
    let statuses = statuses(&rows);
    assert_eq!(statuses[0], TaskStatus::Completed);
    assert!(
        matches!(statuses[1], TaskStatus::Failed(_)),
        "the driver stage failed: {statuses:?}"
    );
    assert_eq!(statuses[2], TaskStatus::Skipped);
    assert!(matches!(
        attempt_status(&fixture, cx),
        TaskStatus::Failed(_)
    ));
    fixture.sidebar.read_with(cx, |sidebar, _| {
        assert!(
            sidebar.pending_toast.is_none(),
            "no manual toast next to the reported error"
        );
    });
    fixture.state.read_with(cx, |state, _| {
        assert!(state.connect_failure(fixture.profile_id).is_some());
        assert!(!state.is_operation_pending(fixture.profile_id, None));
    });
}
