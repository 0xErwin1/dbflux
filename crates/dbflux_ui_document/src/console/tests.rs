//! The console against a fake Redis connection: activation, execution
//! routing, the governance ceiling of a confirmed command, audit rows and
//! the shared history Up/Down recalls from.

use super::{NativeConsole, NativeConsoleEvent, NativeConsoleTarget};
use dbflux_app::keymap::ContextId;
use dbflux_audit::query::AuditQueryFilter;
use dbflux_core::{DbKind, ExecutionClassification, HistoryEntry, QueryResult};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_test_support::fake_driver::FakeDriver;
use dbflux_ui_base::AppStateEntity;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use uuid::Uuid;

const CONNECTION_NAME: &str = "cache";

struct Harness {
    driver: FakeDriver,
    app_state: Entity<AppStateEntity>,
    console: Entity<NativeConsole>,
    events: Rc<RefCell<Vec<NativeConsoleEvent>>>,
}

/// A console over a connected fake Redis profile, database `0`.
fn open(cx: &mut TestAppContext) -> (Harness, &mut VisualTestContext) {
    crate::keyboard_test_support::init_keyboard_runtime(cx);

    let driver = FakeDriver::new(DbKind::Redis)
        .with_query_result(
            "GET greeting",
            QueryResult::text("hello".to_string(), Duration::ZERO),
        )
        .with_default_result(QueryResult::text("(integer) 2".to_string(), Duration::ZERO));
    let profile_id = Uuid::new_v4();

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let runtime = StorageRuntime::in_memory().expect("in-memory storage");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        })
    });

    cx.update(|cx| {
        app_state.update(cx, |state, _| {
            let profile = dbflux_core::ConnectionProfile::new(
                CONNECTION_NAME,
                dbflux_core::DbConfig::SQLite {
                    path: std::path::PathBuf::from(":memory:"),
                    connection_id: None,
                },
            );
            let connection = driver.connect_arc(&profile).expect("fake connection");
            let connected = dbflux_core::ConnectedProfile {
                profile,
                connection,
                schema: None,
                mutation_policy: dbflux_core::MutationPolicy::default(),
                read_only_reason: None,
                database_schemas: Default::default(),
                table_details: Default::default(),
                collection_children: Default::default(),
                schema_types: Default::default(),
                schema_columns: Default::default(),
                schema_indexes: Default::default(),
                schema_foreign_keys: Default::default(),
                schema_routines: Default::default(),
                dependents_cache: Default::default(),
                active_database: None,
                redis_key_cache: Default::default(),
                database_connections: Default::default(),
                proxy_tunnel: None,
            };
            state.connections_mut().insert(profile_id, connected);
        });
    });

    let events = Rc::new(RefCell::new(Vec::new()));
    let console_holder: Rc<RefCell<Option<Entity<NativeConsole>>>> = Rc::new(RefCell::new(None));

    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
        let events = events.clone();
        let console_holder = console_holder.clone();

        move |window, cx| {
            let profile = dbflux_core::NativeConsoleProfile::for_language(
                &dbflux_core::QueryLanguage::RedisCommands,
            );
            let console = cx.new(|cx| {
                NativeConsole::new(
                    NativeConsoleTarget {
                        profile_id,
                        database: Some("0".to_string()),
                        label: "db0".to_string(),
                    },
                    profile,
                    ContextId::KeyValue,
                    app_state,
                    window,
                    cx,
                )
            });

            cx.subscribe(&console, move |_, _, event: &NativeConsoleEvent, _| {
                events.borrow_mut().push(event.clone());
            })
            .detach();

            *console_holder.borrow_mut() = Some(console.clone());
            ConsoleHost { console }
        }
    });

    let console = console_holder.borrow().clone().expect("console created");

    (
        Harness {
            driver,
            app_state,
            console,
            events,
        },
        window,
    )
}

struct ConsoleHost {
    console: Entity<NativeConsole>,
}

impl gpui::Render for ConsoleHost {
    fn render(
        &mut self,
        _window: &mut gpui::Window,
        _cx: &mut gpui::Context<Self>,
    ) -> impl gpui::IntoElement {
        self.console.clone()
    }
}

fn type_and_submit(harness: &Harness, window: &mut VisualTestContext, command: &str) {
    window.update(|window, cx| {
        harness.console.update(cx, |console, cx| {
            console.input.update(cx, |state, cx| {
                state.set_value(command.to_string(), window, cx)
            });
            console.submit(window, cx);
        })
    });
    window.run_until_parked();
}

fn audit_actions(harness: &Harness, window: &mut VisualTestContext) -> Vec<String> {
    window.update(|_, cx| {
        harness
            .app_state
            .read(cx)
            .audit_service()
            .query_extended(&AuditQueryFilter::default())
            .expect("audit query")
            .into_iter()
            .filter_map(|event| event.action)
            .collect()
    })
}

#[gpui::test]
fn toggling_opens_the_console_with_focus_in_its_input(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    let open = window.update(|window, cx| {
        harness
            .console
            .update(cx, |console, cx| console.toggle(window, cx))
    });

    assert!(open);
    assert!(window.update(|_, cx| harness.console.read(cx).input_has_focus()));

    let open = window.update(|window, cx| {
        harness
            .console
            .update(cx, |console, cx| console.toggle(window, cx))
    });
    assert!(!open);
    assert!(!window.update(|_, cx| harness.console.read(cx).input_has_focus()));
}

#[gpui::test]
fn a_command_runs_through_execute_and_is_audited_and_remembered(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    type_and_submit(&harness, window, "GET greeting");

    let requests = harness.driver.stats().executed_requests;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].sql, "GET greeting");
    assert_eq!(requests[0].database.as_deref(), Some("0"));
    assert_eq!(
        requests[0].confirmed_ceiling, None,
        "an ordinary command authorises nothing"
    );

    let last_output = window.update(|_, cx| {
        harness
            .console
            .read(cx)
            .transcript
            .last()
            .map(|entry| (entry.prompt.clone(), entry.output[0].text.clone()))
    });
    assert_eq!(last_output, Some(("0>".to_string(), "hello".to_string())));

    assert!(
        audit_actions(&harness, window)
            .iter()
            .any(|action| action == "query_execute"),
        "the execution is audited as an editor query is"
    );

    let recorded = window.update(|_, cx| {
        harness
            .app_state
            .read(cx)
            .history_entries()
            .iter()
            .any(|entry| {
                entry.sql == "GET greeting"
                    && entry.connection_name.as_deref() == Some(CONNECTION_NAME)
            })
    });
    assert!(recorded, "the command lands in the shared history");

    let events = harness.events.borrow().clone();
    assert!(
        events.contains(&NativeConsoleEvent::Executed {
            succeeded: true,
            classification: ExecutionClassification::Read,
        }),
        "{events:?}"
    );
}

#[gpui::test]
fn a_confirmed_command_carries_the_ceiling_it_authorised(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    type_and_submit(&harness, window, "DEL user:1 user:2");

    assert!(
        window.update(|_, cx| harness.console.read(cx).has_pending()),
        "a multi-key delete asks first"
    );
    assert!(harness.driver.stats().executed_requests.is_empty());

    window.update(|_, cx| {
        harness
            .console
            .update(cx, |console, cx| console.confirm_pending(cx))
    });
    window.run_until_parked();

    let requests = harness.driver.stats().executed_requests;
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].confirmed_ceiling,
        Some(ExecutionClassification::Destructive)
    );

    let actions = audit_actions(&harness, window);
    assert!(
        actions
            .iter()
            .any(|action| action == "dangerous_query_confirmed"),
        "{actions:?}"
    );
}

#[gpui::test]
fn enter_in_the_empty_input_answers_a_pending_confirmation(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    window.update(|window, cx| {
        harness
            .console
            .update(cx, |console, cx| console.toggle(window, cx))
    });

    type_and_submit(&harness, window, "DEL user:1 user:2");
    type_and_submit(&harness, window, "");

    assert!(!window.update(|_, cx| harness.console.read(cx).has_pending()));
    assert_eq!(harness.driver.stats().executed_requests.len(), 1);
}

#[gpui::test]
fn up_recalls_shared_history_and_cancelled_commands(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    window.update(|_, cx| {
        harness.app_state.update(cx, |state, _| {
            state.add_history_entry(HistoryEntry::new(
                "DBSIZE".to_string(),
                Some("0".to_string()),
                Some(CONNECTION_NAME.to_string()),
                Duration::ZERO,
                None,
            ));
            state.add_history_entry(HistoryEntry::new(
                "SELECT 1".to_string(),
                None,
                Some("another connection".to_string()),
                Duration::ZERO,
                None,
            ));
        })
    });

    type_and_submit(&harness, window, "DEL user:1 user:2");
    window.update(|_, cx| {
        harness
            .console
            .update(cx, |console, cx| console.cancel_pending(cx))
    });

    let recalled = |window: &mut VisualTestContext| {
        window.update(|window, cx| {
            harness.console.update(cx, |console, cx| {
                console.recall_history(true, window, cx);
                console.input.read(cx).value().to_string()
            })
        })
    };

    assert_eq!(recalled(window), "DEL user:1 user:2");
    assert_eq!(recalled(window), "DBSIZE");
    assert_eq!(
        recalled(window),
        "DBSIZE",
        "another connection's history is not offered"
    );
}

/// The open console with a command waiting for confirmation: its header and
/// both buttons have a keyboard path.
#[gpui::test]
fn the_native_console_is_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::NATIVE_CONSOLE;
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};

    let (harness, window) = open(cx);

    window.update(|window, cx| {
        harness
            .console
            .update(cx, |console, cx| console.toggle(window, cx))
    });
    type_and_submit(&harness, window, "DEL user:1 user:2");
    assert!(window.update(|_, cx| harness.console.read(cx).has_pending()));

    let capture = FrameCapture::observe(window);
    let checked = Coverage::new(NATIVE_CONSOLE).assert_covered(&capture.frame(window));

    for id in [
        "native-console-header",
        "native-console-run-anyway",
        "native-console-cancel",
    ] {
        assert!(checked.iter().any(|checked| checked == id), "{checked:?}");
    }
}
