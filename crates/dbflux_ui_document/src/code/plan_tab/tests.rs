//! The Plan tab inside the code document: what Explain explains, what it asks
//! the driver for, how the tab sits among the result tabs, and what the
//! keyboard, the pane actions and the inspector rail get from it.

// Explicit imports rather than the parent glob: combining `use super::*` with
// `#[gpui::test]` sends the gpui_macros expansion into unbounded recursion.
use crate::code::{
    CodeDocument, ExecutionSessionContext, ResultOrigin, SqlQueryFocus, SqlQueryLayout,
};
use crate::handle::DocumentEvent;
use crate::keyboard_coverage::{CODE_EDITOR_CHROME, DATA_GRID, QUERY_PLAN};
use crate::keyboard_test_support::{connected_app_state, host_document};
use crate::query_plan::{PlanRunMode, PlanState, PlanViewEvent, PlanViewMode};
use dbflux_app::keymap::Command;
use dbflux_components::accessibility::FrameCapture;
use dbflux_core::{
    ColumnKind, ColumnMeta, DbError, DbKind, PlanFormat, PlanNode, QueryLanguage, QueryPlan,
    QueryPlanMode, QueryPlanRequest, QueryResult, Value,
};
use dbflux_test_support::FakeDriver;
use dbflux_ui_base::keyboard_coverage::Coverage;
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{AppContext as _, Entity, Modifiers, TestAppContext, VisualTestContext};
use std::cell::RefCell;
use std::ops::Range;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

const SCRIPT: &str =
    "SELECT count(*) FROM orders;\n\nSELECT id, email\nFROM customers\nWHERE active;\n\nSELECT 1;";

fn sample_plan() -> QueryPlan {
    let mut scan = PlanNode::new("Seq Scan");
    scan.relation = Some("customers".into());
    scan.estimated_total_cost = Some(12.5);
    scan.estimated_rows = Some(40.0);
    scan.description = Some("Reads every row of the table.".into());

    let mut limit = PlanNode::new("Limit");
    limit.estimated_total_cost = Some(12.6);
    limit.estimated_rows = Some(10.0);
    limit.children = vec![scan];

    let mut plan = QueryPlan::new(vec![limit], "{\"plan\": true}", PlanFormat::Json);
    plan.warnings.push("Reads the whole table.".into());
    plan
}

fn one_row_result() -> Arc<QueryResult> {
    let columns = vec![ColumnMeta {
        name: "id".to_string(),
        type_name: "text".to_string(),
        kind: ColumnKind::Text,
        nullable: true,
        is_primary_key: false,
    }];

    Arc::new(QueryResult::table(
        columns,
        vec![vec![Value::Text("v".to_string())]],
        None,
        Duration::ZERO,
    ))
}

struct Doc<'a> {
    document: Entity<CodeDocument>,
    window: &'a mut VisualTestContext,
    driver: FakeDriver,
    frame: Arc<FrameCapture>,
    events: Rc<RefCell<Vec<String>>>,
    app_state: Entity<dbflux_ui_base::AppStateEntity>,
    profile_id: uuid::Uuid,
}

fn open<'a>(cx: &'a mut TestAppContext, driver: FakeDriver, content: &str) -> Doc<'a> {
    crate::keyboard_test_support::init_keyboard_runtime(cx);
    cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });
    let (app_state, profile_id) = connected_app_state(cx, &driver, "shop-pg");

    let content = content.to_string();
    let state_for_document = app_state.clone();
    let (host, window) = host_document(
        cx,
        move |window, cx| {
            cx.new(|cx| {
                let mut document = CodeDocument::new_with_language(
                    state_for_document,
                    None,
                    QueryLanguage::Sql,
                    window,
                    cx,
                );
                document.connection_id = Some(profile_id);
                document.set_content(&content, window, cx);
                document
            })
        },
        |document, cx| document.active_context(cx),
        |document, command, window, cx| document.dispatch_command(command, window, cx),
    );
    let document = window.update(|_, cx| host.read(cx).document.clone());

    let events = Rc::new(RefCell::new(Vec::new()));
    window.update(|_, cx| {
        let seen = events.clone();
        cx.subscribe(&document, move |_, event: &DocumentEvent, _| {
            let label = match event {
                DocumentEvent::OpenInspector { title, .. } => format!("open:{title}"),
                DocumentEvent::CloseInspector => "close".to_string(),
                DocumentEvent::ExecutionStarted => "started".to_string(),
                DocumentEvent::ExecutionFinished => "finished".to_string(),
                _ => return,
            };
            seen.borrow_mut().push(label);
        })
        .detach();
    });
    let frame = FrameCapture::observe(window);

    Doc {
        document,
        window,
        driver,
        frame,
        events,
        app_state,
        profile_id,
    }
}

fn pg(plan: Option<QueryPlan>) -> FakeDriver {
    let driver = FakeDriver::new(DbKind::SQLite);
    match plan {
        Some(plan) => driver.with_query_plan(plan),
        None => driver,
    }
}

impl Doc<'_> {
    fn settle(&mut self) {
        for _ in 0..3 {
            self.window.run_until_parked();
            self.window.update(|window, _| window.refresh());
        }
        self.window.run_until_parked();
    }

    fn cursor_at(&mut self, offset: usize) {
        self.select(offset..offset);
    }

    fn select(&mut self, range: Range<usize>) {
        self.window.update(|_, cx| {
            self.document.update(cx, |document, cx| {
                document
                    .editor
                    .input_state
                    .update(cx, |state, cx| state.set_selected_range(range, cx));
            });
        });
    }

    fn explain(&mut self) {
        self.window.update(|window, cx| {
            self.document
                .update(cx, |document, cx| document.run_explain(window, cx));
        });
    }

    fn explain_and_settle(&mut self) {
        self.explain();
        self.settle();
    }

    fn press(&mut self, command: Command) -> bool {
        let handled = self.window.update(|window, cx| {
            self.document.update(cx, |document, cx| {
                document.dispatch_command(command, window, cx)
            })
        });
        self.settle();
        handled
    }

    fn has(&mut self, id: &'static str) -> bool {
        self.window.debug_bounds(id).is_some()
    }

    fn click(&mut self, id: &'static str) {
        let bounds = self
            .window
            .debug_bounds(id)
            .unwrap_or_else(|| panic!("`{id}` is drawn"));
        self.window
            .simulate_click(bounds.center(), Modifiers::default());
        self.settle();
    }

    fn requests(&self) -> Vec<String> {
        self.driver
            .stats()
            .query_plan_requests
            .into_iter()
            .map(|request| request.query)
            .collect()
    }

    fn modes(&self) -> Vec<QueryPlanMode> {
        self.driver
            .stats()
            .query_plan_requests
            .into_iter()
            .map(|request| request.mode)
            .collect()
    }

    fn plan_state<T>(&mut self, read: impl FnOnce(&PlanState) -> T) -> T {
        self.window.update(|_, cx| {
            let view = self
                .document
                .read(cx)
                .plan
                .view()
                .expect("the document has a Plan tab");
            read(view.read(cx).state())
        })
    }

    fn has_plan_tab(&mut self) -> bool {
        self.window
            .update(|_, cx| self.document.read(cx).plan.tab.is_some())
    }

    fn plan_shown(&mut self) -> bool {
        self.window
            .update(|_, cx| self.document.read(cx).plan_shown())
    }

    fn add_result_tab(&mut self) {
        self.window.update(|window, cx| {
            self.document.update(cx, |document, cx| {
                document.result_tabs.run_in_new_tab = true;
                document.setup_data_grid(
                    one_row_result(),
                    "SELECT 1".into(),
                    None,
                    None,
                    window,
                    cx,
                );
                document.layout = SqlQueryLayout::Split;
            });
        });
        self.settle();
    }

    /// Opens the cell menu of the first result tab and chooses its
    /// "Explain this query" row.
    fn explain_from_result_menu(&mut self) {
        self.window.update(|window, cx| {
            let grid = self.document.read(cx).result_tabs.result_tabs[0]
                .grid
                .clone();
            grid.update(cx, |grid, cx| {
                grid.open_context_menu_on_selection_for_test(window, cx);
                grid.activate_menu_row_for_test("explain-this-query", window, cx);
            });
        });
        self.settle();
    }

    /// Connects a second profile named `name` through `driver` and returns
    /// its id.
    fn connect_other(&mut self, driver: &FakeDriver, name: &str) -> uuid::Uuid {
        let other = uuid::Uuid::new_v4();
        let profile = dbflux_core::ConnectionProfile::new(
            name,
            dbflux_core::DbConfig::SQLite {
                path: std::path::PathBuf::from(":memory:"),
                connection_id: None,
            },
        );
        let connection = driver.connect_arc(&profile).expect("fake connection");

        self.window.update(|_, cx| {
            self.app_state.update(cx, |state, _| {
                state.connections_mut().insert(
                    other,
                    dbflux_core::ConnectedProfile {
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
                    },
                );
            });
        });
        other
    }

    /// A result tab for "SELECT 1" that ran on the connection the document
    /// started with, then points the document at `other`.
    fn result_from_origin_then_switch_to(&mut self, other: uuid::Uuid) {
        self.result_from_origin_database(None);
        self.window.update(|_, cx| {
            self.document
                .update(cx, |document, _| document.connection_id = Some(other));
        });
        self.settle();
    }

    /// A result tab for "SELECT 1" that ran on the starting connection while
    /// it was on `database`.
    fn result_from_origin_database(&mut self, database: Option<&str>) {
        self.window.update(|window, cx| {
            let origin_connection = self
                .app_state
                .read(cx)
                .connections()
                .get(&self.profile_id)
                .map(|connected| connected.connection.clone())
                .expect("the origin is connected");

            self.document.update(cx, |document, cx| {
                document.result_tabs.run_in_new_tab = true;
                document.setup_data_grid(
                    one_row_result(),
                    "SELECT 1".into(),
                    None,
                    Some(ResultOrigin {
                        connection_id: self.profile_id,
                        context: ExecutionSessionContext {
                            root: origin_connection,
                            database: database.map(str::to_string),
                        },
                        exec_ctx: Default::default(),
                        language: QueryLanguage::Sql,
                    }),
                    window,
                    cx,
                );
                document.layout = SqlQueryLayout::Split;
            });
        });
        self.settle();
    }

    fn disconnect(&mut self, id: uuid::Uuid) {
        self.window.update(|_, cx| {
            self.app_state.update(cx, |state, _| {
                state.connections_mut().remove(&id);
            });
        });
    }

    fn events(&self) -> Vec<String> {
        self.events.borrow().clone()
    }

    fn menu(&mut self) -> Vec<String> {
        self.window.update(|_, cx| {
            self.document
                .read(cx)
                .pane_actions(&self.document, cx)
                .iter()
                .map(|action| action.id.to_string())
                .collect()
        })
    }
}

fn ready(state: &PlanState) -> bool {
    matches!(state, PlanState::Ready(_))
}

#[gpui::test]
fn explain_plans_the_statement_under_the_cursor_without_running_it(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), SCRIPT);
    doc.cursor_at(SCRIPT.find("customers").expect("second statement"));

    doc.explain_and_settle();

    assert_eq!(
        doc.requests(),
        ["SELECT id, email\nFROM customers\nWHERE active"],
        "the one statement the cursor is in, sent as a plan request"
    );
    assert!(
        doc.driver.stats().executed_requests.is_empty(),
        "nothing is run, and no `EXPLAIN ` text is built"
    );
    assert!(doc.plan_state(ready));
    assert!(doc.plan_shown());
}

#[gpui::test]
fn explaining_a_result_plans_the_query_that_produced_it_not_the_buffer(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), SCRIPT);
    doc.add_result_tab();

    doc.explain_from_result_menu();

    assert_eq!(
        doc.requests(),
        ["SELECT 1"],
        "the result's own query, sent as a plan request"
    );
    assert!(
        doc.driver.stats().executed_requests.is_empty(),
        "nothing is run"
    );
    assert!(doc.plan_state(ready));
    assert!(doc.plan_shown());
}

#[gpui::test]
fn explaining_a_result_plans_on_the_connection_that_produced_it(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), SCRIPT);
    let other_driver = pg(Some(sample_plan()));
    let other = doc.connect_other(&other_driver, "reporting");
    doc.result_from_origin_then_switch_to(other);

    doc.explain_from_result_menu();

    assert_eq!(doc.requests(), ["SELECT 1"], "the origin's driver is asked");
    assert!(
        other_driver.stats().query_plan_requests.is_empty(),
        "the connection the document moved to is not"
    );
    assert!(doc.plan_state(ready));
}

#[gpui::test]
fn the_editor_explain_keeps_using_the_documents_current_connection(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 2");
    let other_driver = pg(Some(sample_plan()));
    let other = doc.connect_other(&other_driver, "reporting");
    doc.result_from_origin_then_switch_to(other);

    doc.explain_and_settle();

    assert!(doc.requests().is_empty());
    let asked: Vec<String> = other_driver
        .stats()
        .query_plan_requests
        .into_iter()
        .map(|request| request.query)
        .collect();
    assert_eq!(asked, ["SELECT 2"]);
}

#[gpui::test]
fn explaining_a_result_whose_connection_is_gone_says_so_and_asks_nobody(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), SCRIPT);
    let other_driver = pg(Some(sample_plan()));
    let other = doc.connect_other(&other_driver, "reporting");
    doc.result_from_origin_then_switch_to(other);
    let origin = doc.profile_id;
    doc.disconnect(origin);

    doc.explain_from_result_menu();

    assert!(doc.requests().is_empty());
    assert!(other_driver.stats().query_plan_requests.is_empty());
    assert!(doc.plan_shown());
    let message = doc.plan_state(|state| match state {
        PlanState::Failed(failure) => Some(failure.summary.clone()),
        _ => None,
    });
    assert_eq!(
        message.as_deref(),
        Some(
            "The connection this result came from is no longer connected. Reconnect it and run the query again to explain it."
        )
    );
}

#[gpui::test]
fn explain_plans_the_selection_when_there_is_one(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), SCRIPT);
    let start = SCRIPT.find("SELECT 1").expect("last statement");
    doc.select(start..start + "SELECT 1".len());

    doc.explain_and_settle();

    assert_eq!(doc.requests(), ["SELECT 1"]);
    assert!(doc.plan_state(ready));
}

#[gpui::test]
fn the_tab_says_which_lines_the_plan_covers(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), SCRIPT);

    doc.cursor_at(SCRIPT.find("customers").unwrap());
    doc.explain_and_settle();
    let scope = doc.window.update(|_, cx| {
        doc.document
            .read(cx)
            .plan
            .view()
            .unwrap()
            .read(cx)
            .scope()
            .to_string()
    });
    assert_eq!(scope, "Statement L3–5");

    let start = SCRIPT.find("SELECT 1").unwrap();
    doc.select(start..start + 8);
    doc.explain_and_settle();
    let scope = doc.window.update(|_, cx| {
        doc.document
            .read(cx)
            .plan
            .view()
            .unwrap()
            .read(cx)
            .scope()
            .to_string()
    });
    assert_eq!(scope, "Selection L7–7");
}

#[gpui::test]
fn a_single_statement_buffer_is_planned_whole(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT * FROM orders");

    doc.explain_and_settle();

    assert_eq!(doc.requests(), ["SELECT * FROM orders"]);
    assert!(doc.plan_state(ready));
}

#[gpui::test]
fn an_empty_buffer_explains_nothing(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "  \n");

    doc.explain_and_settle();

    assert!(doc.requests().is_empty());
    assert!(!doc.has_plan_tab());
}

#[gpui::test]
fn the_plan_opens_on_the_diagram_in_a_tab_beside_the_result_tabs(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT * FROM customers");
    doc.add_result_tab();
    assert!(!doc.has("result-tab-plan"), "no plan before Explain");

    doc.explain_and_settle();

    assert!(doc.has("result-tab-plan"));
    assert!(doc.has("close-result-tab-plan"));
    assert!(
        doc.has("plan-view") && doc.has("plan-node-0"),
        "the Diagram is the default"
    );
    let mode = doc
        .window
        .update(|_, cx| doc.document.read(cx).plan.view().unwrap().read(cx).mode());
    assert_eq!(mode, PlanViewMode::Diagram);
    let result_tabs = doc
        .window
        .update(|_, cx| doc.document.read(cx).result_tabs.result_tabs.len());
    assert_eq!(result_tabs, 1, "the result tab is still there");
}

#[gpui::test]
fn explain_with_no_results_opens_the_results_area_for_the_plan(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    let layout = doc.window.update(|_, cx| doc.document.read(cx).layout);
    assert_eq!(layout, SqlQueryLayout::EditorOnly);

    doc.explain_and_settle();

    let layout = doc.window.update(|_, cx| doc.document.read(cx).layout);
    assert_eq!(layout, SqlQueryLayout::Split);
    assert!(doc.has("result-tab-plan") && doc.has("plan-view"));
}

#[gpui::test]
fn explaining_again_replaces_the_plan_and_keeps_the_view(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), SCRIPT);
    doc.cursor_at(0);
    doc.explain_and_settle();
    doc.click("segmented-plan-view-tree");

    doc.cursor_at(SCRIPT.find("customers").unwrap());
    doc.explain_and_settle();

    assert_eq!(doc.requests().len(), 2);
    let tabs = doc
        .window
        .update(|_, cx| doc.document.read(cx).result_tab_total());
    assert_eq!(tabs, 1, "one Plan tab, replaced rather than added");
    let mode = doc
        .window
        .update(|_, cx| doc.document.read(cx).plan.view().unwrap().read(cx).mode());
    assert_eq!(mode, PlanViewMode::Tree, "the reader's view carries over");
    assert!(doc.has("plan-tree"));
}

#[gpui::test]
fn the_plan_is_loading_until_the_driver_answers_and_never_blocks(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");

    doc.explain();

    assert!(
        doc.plan_state(|state| matches!(state, PlanState::Loading)),
        "the tab is there, loading, before the driver call has run"
    );
    assert!(
        doc.requests().is_empty(),
        "the call is on the background executor"
    );
    let running = doc
        .window
        .update(|_, cx| doc.document.read(cx).runner.is_primary_active());
    assert!(running, "it is a task Cancel can stop");

    doc.settle();
    assert!(doc.plan_state(ready));
    let running = doc
        .window
        .update(|_, cx| doc.document.read(cx).runner.is_primary_active());
    assert!(!running, "the task is over");
    assert_eq!(doc.events(), ["started", "finished"]);
}

#[gpui::test]
fn cancel_while_loading_shows_cancelled_and_drops_the_late_answer(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.explain();
    assert!(doc.plan_state(|state| matches!(state, PlanState::Loading)));

    assert!(
        doc.press(Command::Cancel),
        "Escape cancels the loading plan"
    );
    assert!(doc.plan_state(|state| matches!(state, PlanState::Cancelled)));

    doc.settle();
    assert!(
        doc.plan_state(|state| matches!(state, PlanState::Cancelled)),
        "the driver's late answer does not bring the plan back"
    );
    let state = doc.window.update(|_, cx| doc.document.read(cx).state);
    assert_eq!(state, crate::types::DocumentState::Clean);
}

fn plan_task_status(doc: &mut Doc<'_>) -> dbflux_core::TaskStatus {
    doc.window.update(|_, cx| {
        doc.app_state
            .read(cx)
            .tasks()
            .recent_tasks(10)
            .into_iter()
            .find(|task| task.kind == dbflux_core::TaskKind::Query)
            .expect("the plan runs as a query task")
            .status
    })
}

#[gpui::test]
fn cancelling_a_plan_leaves_its_task_cancelling_until_the_driver_answers(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.explain();

    // Cancel without settling, so the driver call has not answered yet.
    let handled = doc.window.update(|window, cx| {
        doc.document.update(cx, |document, cx| {
            document.dispatch_command(Command::Cancel, window, cx)
        })
    });
    assert!(handled);
    assert_eq!(
        plan_task_status(&mut doc),
        dbflux_core::TaskStatus::Cancelling,
        "the explain is still running on the server"
    );

    doc.settle();
    assert_eq!(
        plan_task_status(&mut doc),
        dbflux_core::TaskStatus::Completed,
        "an explain the driver finished anyway is not reported as cancelled"
    );
}

#[gpui::test]
fn a_plan_whose_cancel_was_refused_still_settles_after_its_tab_closed(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.explain();

    // The driver cannot stop this explain: the cancel is refused and the task
    // keeps running, with nobody waiting for it once the tab is gone.
    doc.window.update(|_, cx| {
        let task_id = doc
            .document
            .read(cx)
            .execution
            .active_query_task
            .as_ref()
            .expect("the plan is the active task")
            .task_id;
        doc.app_state.update(cx, |state, _| {
            state.mark_task_point_of_no_return(task_id, "cannot be stopped");
        });
    });
    doc.window.update(|_, cx| {
        doc.document
            .update(cx, |document, cx| document.close_plan_tab(cx))
    });
    assert_eq!(
        plan_task_status(&mut doc),
        dbflux_core::TaskStatus::Running,
        "the refused cancel leaves the task running until the driver answers"
    );

    doc.settle();
    assert_eq!(
        plan_task_status(&mut doc),
        dbflux_core::TaskStatus::Completed,
        "the driver's answer settles the task even though the tab is gone"
    );
}

#[gpui::test]
fn the_loading_cancel_button_stops_the_plan_through_the_document(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.explain();

    // The button's click reaches the document as this event.
    doc.window.update(|_, cx| {
        let view = doc.document.read(cx).plan.view().expect("the Plan tab");
        view.update(cx, |_, cx| {
            cx.emit(crate::query_plan::PlanViewEvent::CancelRequested)
        });
    });

    assert!(doc.plan_state(|state| matches!(state, PlanState::Cancelled)));
    let running = doc
        .window
        .update(|_, cx| doc.document.read(cx).runner.is_primary_active());
    assert!(!running, "the task is cancelled");
}

#[gpui::test]
fn retry_after_cancel_plans_the_same_text_again(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.explain();
    doc.press(Command::Cancel);
    doc.settle();

    doc.click("plan-retry");

    assert!(doc.plan_state(ready));
    assert_eq!(doc.requests().last().map(String::as_str), Some("SELECT 1"));
}

#[gpui::test]
fn a_driver_without_plans_gets_the_unsupported_state(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(None), "SELECT 1");

    doc.explain_and_settle();

    assert!(doc.plan_state(|state| matches!(
        state,
        PlanState::Unsupported { connection } if connection == "shop-pg"
    )));
    assert!(doc.has("plan-state-unsupported"));
}

#[gpui::test]
fn more_than_one_statement_shows_the_drivers_message(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), SCRIPT);
    doc.select(0..SCRIPT.len());

    doc.explain_and_settle();

    let message = doc.plan_state(|state| match state {
        PlanState::MultiStatement { message } => Some(message.clone()),
        _ => None,
    });
    let message = message.expect("the multi-statement state");
    assert!(
        message.contains("exactly one statement") && message.contains("has 3"),
        "the driver's own message: {message}"
    );
    assert!(doc.has("plan-state-multi-statement"));
    assert!(
        doc.requests().len() == 1,
        "the driver was asked once and refused"
    );
}

#[gpui::test]
fn a_server_error_is_shown_in_the_tab_with_copy_and_audit(cx: &mut TestAppContext) {
    let driver = FakeDriver::new(DbKind::SQLite)
        .with_query_plan_error("relation \"order_archive\" does not exist");
    let mut doc = open(cx, driver, "SELECT * FROM order_archive");

    doc.explain_and_settle();

    let summary = doc.plan_state(|state| match state {
        PlanState::Failed(failure) => Some(failure.summary.clone()),
        _ => None,
    });
    assert_eq!(
        summary.as_deref(),
        Some("relation \"order_archive\" does not exist")
    );
    for id in [
        "plan-state-failed",
        "plan-retry",
        "plan-copy-error",
        "plan-view-in-audit",
    ] {
        assert!(doc.has(id), "{id}");
    }
}

#[gpui::test]
fn a_failed_plan_is_recorded_without_a_toast(cx: &mut TestAppContext) {
    let driver = FakeDriver::new(DbKind::SQLite).with_query_plan_error("boom");
    let mut doc = open(cx, driver, "SELECT 1");

    doc.explain_and_settle();

    let toasts = doc.window.update(|_, cx| {
        cx.try_global::<ToastGlobal>()
            .map(|global| global.host.read(cx).toast_count())
            .unwrap_or_default()
    });
    assert_eq!(toasts, 0, "the tab shows the error in place");
}

#[gpui::test]
fn the_explain_button_and_the_explain_command_open_the_plan(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.click("toolbar-explain-btn");
    assert!(doc.plan_state(ready), "the toolbar button");

    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 2");
    assert!(doc.press(Command::ExplainQuery), "<leader>e");
    assert!(doc.plan_state(ready));
}

#[gpui::test]
fn the_explain_button_is_disabled_with_a_reason_when_the_driver_has_no_plans(
    cx: &mut TestAppContext,
) {
    let driver = FakeDriver::new(DbKind::SQLite).with_explain_support(false);
    let mut doc = open(cx, driver, "SELECT 1");

    let reason = doc
        .window
        .update(|_, cx| doc.document.read(cx).explain_unavailable_reason(cx));
    assert_eq!(
        reason.as_deref(),
        Some("shop-pg does not provide query plans")
    );

    doc.click("toolbar-explain-btn");
    assert!(!doc.has_plan_tab(), "a disabled button does nothing");

    let bounds = doc
        .window
        .debug_bounds("toolbar-explain-btn")
        .expect("the button");
    doc.window
        .simulate_mouse_move(bounds.center(), None, Modifiers::default());
    doc.window
        .executor()
        .advance_clock(Duration::from_millis(600));
    doc.window.run_until_parked();
    let frame = doc.frame.frame(doc.window);
    let tooltip = frame
        .nodes()
        .find(|(_, node)| node.id() == "tooltip-popup")
        .map(|(_, node)| node.content_text().to_string());
    assert_eq!(
        tooltip.as_deref(),
        Some("shop-pg does not provide query plans")
    );

    let explain_enabled = doc.window.update(|_, cx| {
        doc.document
            .read(cx)
            .pane_actions(&doc.document, cx)
            .iter()
            .find(|action| action.id == "explain")
            .map(|action| action.enabled)
    });
    assert_eq!(
        explain_enabled,
        Some(false),
        "the menu entry is disabled too"
    );
}

#[gpui::test]
fn the_explain_button_is_enabled_for_a_driver_with_plans(cx: &mut TestAppContext) {
    let doc = open(cx, pg(Some(sample_plan())), "SELECT 1");

    let reason = doc
        .window
        .update(|_, cx| doc.document.read(cx).explain_unavailable_reason(cx));

    assert_eq!(reason, None);
}

#[gpui::test]
fn the_plan_tab_is_chosen_stepped_to_and_closed_like_a_result_tab(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.add_result_tab();
    doc.explain_and_settle();
    assert!(doc.plan_shown());

    doc.click("result-tab-plan");
    assert!(doc.plan_shown(), "clicking the shown tab keeps it");

    // A result tab is a click away and takes the area from the plan.
    let first = doc
        .window
        .update(|_, cx| doc.document.read(cx).result_tabs.result_tabs[0].id);
    let first_id: &'static str = Box::leak(format!("result-tab-{first}").into_boxed_str());
    doc.click(first_id);
    assert!(!doc.plan_shown());
    assert!(
        doc.has("result-tab-plan"),
        "the Plan tab stays in the header"
    );

    // Next result tab visits the plan, then wraps.
    assert!(doc.press(Command::NextResultTab));
    assert!(doc.plan_shown());
    assert!(doc.press(Command::NextResultTab));
    assert!(!doc.plan_shown());
    assert!(doc.press(Command::PrevResultTab));
    assert!(doc.plan_shown());

    // Close result tab closes the Plan tab while it is shown.
    assert!(doc.press(Command::CloseResultTab));
    assert!(!doc.has_plan_tab());
    assert!(!doc.plan_shown());
    let results = doc
        .window
        .update(|_, cx| doc.document.read(cx).result_tabs.result_tabs.len());
    assert_eq!(results, 1, "the result tab is left alone");
}

#[gpui::test]
fn closing_the_last_result_tab_shows_the_plan_and_closing_the_plan_hides_the_area(
    cx: &mut TestAppContext,
) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.add_result_tab();
    doc.explain_and_settle();
    doc.press(Command::NextResultTab);
    assert!(!doc.plan_shown());
    assert!(doc.press(Command::CloseResultTab));

    assert!(doc.plan_shown(), "the plan takes the area the result left");
    let layout = doc.window.update(|_, cx| doc.document.read(cx).layout);
    assert_eq!(layout, SqlQueryLayout::Split);

    doc.click("close-result-tab-plan");
    assert!(!doc.has_plan_tab());
    let layout = doc.window.update(|_, cx| doc.document.read(cx).layout);
    assert_eq!(layout, SqlQueryLayout::EditorOnly, "nothing left to show");
}

#[gpui::test]
fn a_new_result_takes_the_area_from_the_plan_but_the_tab_stays(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.explain_and_settle();
    assert!(doc.plan_shown());

    doc.add_result_tab();

    assert!(!doc.plan_shown());
    assert!(doc.has_plan_tab());
    assert!(doc.has("result-tab-plan"));
}

#[gpui::test]
fn the_keyboard_reaches_the_plan_from_the_editor_and_back(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.explain_and_settle();

    assert!(
        doc.press(Command::FocusDown),
        "from the editor into the results"
    );
    let focus = doc.window.update(|_, cx| doc.document.read(cx).focus_mode);
    assert_eq!(focus, SqlQueryFocus::Results);

    assert!(doc.press(Command::SelectNext));
    let events = doc.events();
    assert!(
        events
            .iter()
            .any(|event| event == "open:Limit" || event == "open:Seq Scan"),
        "selecting a node mounts the inspector in the rail: {events:?}"
    );

    assert!(doc.press(Command::CycleResultView));
    assert!(doc.has("plan-tree"));

    assert!(doc.press(Command::FocusUp), "back to the editor");
    let focus = doc.window.update(|_, cx| doc.document.read(cx).focus_mode);
    assert_eq!(focus, SqlQueryFocus::Editor);
}

#[gpui::test]
fn the_rail_follows_the_tab_you_are_on(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.add_result_tab();
    doc.explain_and_settle();
    doc.press(Command::FocusDown);
    doc.press(Command::SelectNext);
    let opened = doc
        .events()
        .iter()
        .filter(|e| e.starts_with("open:"))
        .count();
    assert_eq!(opened, 1);

    doc.press(Command::NextResultTab);
    assert_eq!(
        doc.events().last().map(String::as_str),
        Some("close"),
        "leaving hides the rail"
    );

    doc.press(Command::NextResultTab);
    assert_eq!(
        doc.events()
            .iter()
            .filter(|e| e.starts_with("open:"))
            .count(),
        2,
        "coming back puts the inspector back"
    );

    doc.window.update(|_, cx| {
        doc.document
            .update(cx, |document, cx| document.mark_inspector_closed(cx));
    });
    doc.press(Command::NextResultTab);
    doc.press(Command::NextResultTab);
    assert_eq!(
        doc.events()
            .iter()
            .filter(|e| e.starts_with("open:"))
            .count(),
        2,
        "a rail the user dismissed stays closed"
    );
}

#[gpui::test]
fn the_pane_actions_list_the_plan_controls_for_the_state_shown(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.explain();
    let loading = doc.menu();
    assert!(loading.contains(&"plan-cancel".to_string()), "{loading:?}");

    doc.settle();
    let diagram = doc.menu();
    for entry in [
        "plan-view-diagram",
        "plan-view-tree",
        "plan-view-raw",
        "plan-zoom-in",
        "plan-zoom-out",
        "plan-zoom-reset",
        "plan-fit",
    ] {
        assert!(
            diagram.contains(&entry.to_string()),
            "{entry} in {diagram:?}"
        );
    }
    assert!(!diagram.contains(&"plan-copy-raw".to_string()));

    doc.press(Command::FocusDown);
    doc.press(Command::CycleResultView);
    doc.press(Command::CycleResultView);
    let raw = doc.menu();
    assert!(raw.contains(&"plan-copy-raw".to_string()), "{raw:?}");
    assert!(!raw.contains(&"plan-fit".to_string()));
}

#[gpui::test]
fn a_pane_action_switches_the_view(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.explain_and_settle();

    let run = doc.window.update(|_, cx| {
        doc.document
            .read(cx)
            .pane_actions(&doc.document, cx)
            .into_iter()
            .find(|action| action.id == "plan-view-tree")
            .expect("the entry")
            .run
    });
    doc.window.update(|window, cx| match run {
        crate::pane::PaneActionRun::Callback(callback) => callback(window, cx),
        crate::pane::PaneActionRun::Command(_) => unreachable!("a callback entry"),
    });
    doc.settle();

    assert!(doc.has("plan-tree"));
}

fn assert_plan_frame_covered(doc: &mut Doc<'_>) {
    let menu = doc.menu();
    let frame = doc.frame.frame(doc.window);

    Coverage::new(CODE_EDITOR_CHROME)
        .with_menu_entries(menu)
        .with_surface(DATA_GRID)
        .with_surface(QUERY_PLAN)
        .delegate("exec-context-bar", "the context bar")
        .assert_covered(&frame);
}

#[gpui::test]
fn the_plan_chrome_is_covered(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.add_result_tab();
    doc.explain_and_settle();

    // Diagram, with a node selected.
    doc.click("plan-node-1");
    assert_plan_frame_covered(&mut doc);
    let frame = doc.frame.frame(doc.window);
    let ids: Vec<String> = frame
        .nodes()
        .map(|(_, node)| node.id().to_string())
        .collect();
    assert!(ids.iter().any(|id| id == "plan-node-0"), "{ids:?}");
    assert!(ids.iter().any(|id| id == "plan-fit"), "{ids:?}");

    // Tree.
    doc.press(Command::FocusDown);
    doc.press(Command::CycleResultView);
    assert_plan_frame_covered(&mut doc);
    let frame = doc.frame.frame(doc.window);
    assert!(
        frame
            .nodes()
            .any(|(_, node)| node.id() == "plan-tree-toggle-0"),
        "the tree's fold buttons are drawn"
    );

    // Raw.
    doc.press(Command::CycleResultView);
    assert_plan_frame_covered(&mut doc);

    // Tab strip buttons of the Plan tab are covered by the result tab entries.
    let frame = doc.frame.frame(doc.window);
    assert!(
        frame
            .nodes()
            .any(|(_, node)| node.id() == "result-tab-plan")
    );
}

#[gpui::test]
fn the_error_and_loading_states_are_covered(cx: &mut TestAppContext) {
    let driver = FakeDriver::new(DbKind::SQLite).with_query_plan_error("boom");
    let mut doc = open(cx, driver, "SELECT 1");

    doc.explain();
    doc.window.update(|window, _| window.refresh());
    doc.window.run_until_parked();
    assert_plan_frame_covered(&mut doc);

    doc.settle();
    assert_plan_frame_covered(&mut doc);
}

#[gpui::test]
fn every_plan_control_is_named_for_assistive_technology(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.explain_and_settle();
    doc.click("plan-node-0");

    let frame = doc.frame.frame(doc.window);
    let unnamed: Vec<String> = dbflux_components::accessibility::unnamed_clickables(&frame)
        .into_iter()
        .filter(|entry| entry.starts_with("plan-"))
        .collect();

    assert!(unnamed.is_empty(), "{unnamed:?}");
}

#[test]
fn every_string_the_plan_tab_uses_is_translated_in_every_language() {
    const SOURCES: [&str; 14] = [
        include_str!("../plan_tab.rs"),
        include_str!("../../query_plan/mod.rs"),
        include_str!("../../query_plan/actions.rs"),
        include_str!("../../query_plan/chrome.rs"),
        include_str!("../../query_plan/inspector.rs"),
        include_str!("../../query_plan/raw.rs"),
        include_str!("../../query_plan/states.rs"),
        include_str!("../../query_plan/tree.rs"),
        include_str!("../../query_plan/tooltip.rs"),
        include_str!("../../query_plan/diagram/mod.rs"),
        include_str!("../../query_plan/diagram/geometry.rs"),
        include_str!("../../query_plan/model.rs"),
        include_str!("../../query_plan/ids.rs"),
        include_str!("../render.rs"),
    ];

    let mut keys: Vec<String> = Vec::new();
    for source in SOURCES {
        let mut rest = source;
        while let Some(at) = rest.find("t!(\"") {
            rest = &rest[at + 4..];
            if let Some(end) = rest.find('"') {
                let key = &rest[..end];
                if key.starts_with("document.query_plan.") {
                    keys.push(key.to_string());
                }
            }
        }
    }
    keys.sort();
    keys.dedup();
    assert!(keys.len() > 60, "the scan found the keys: {}", keys.len());

    for key in &keys {
        for locale in ["en", "es", "ko", "pt_BR", "zh_Hans"] {
            let value = dbflux_i18n::t!(key.as_str(), locale = locale);
            assert!(!value.is_empty(), "{key} is empty in {locale}");
            assert_ne!(&value, key, "{key} has no entry in {locale}");
            assert_ne!(
                value,
                format!("{locale}.{key}"),
                "{key} is missing from the {locale} catalog"
            );
        }
    }
}

#[test]
fn the_translations_keep_the_placeholders_of_the_english_text() {
    for key in [
        "document.query_plan.scope.selection",
        "document.query_plan.scope.script",
        "document.query_plan.warnings.many",
        "document.query_plan.footer.nodes.one",
        "document.query_plan.footer.nodes.many",
        "document.query_plan.inspector.node",
        "document.query_plan.state.unsupported.body",
        "document.query_plan.explain.unavailable",
        "document.query_plan.state.editor_database_changed",
    ] {
        let english = dbflux_i18n::t!(key, locale = "en");
        let mut placeholders: Vec<&str> = english.matches("%{").collect();
        placeholders.sort_unstable();

        for locale in ["es", "ko", "pt_BR", "zh_Hans"] {
            let text = dbflux_i18n::t!(key, locale = locale);
            for name in english
                .split("%{")
                .skip(1)
                .filter_map(|part| part.split('}').next())
            {
                assert!(
                    text.contains(&format!("%{{{name}}}")),
                    "{key} in {locale} lost %{{{name}}}: {text}"
                );
            }
        }
    }
}

#[gpui::test]
fn a_batch_result_takes_the_area_from_the_plan_but_the_tab_stays(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1; SELECT 2");
    doc.explain_and_settle();
    assert!(doc.plan_shown());

    doc.window.update(|window, cx| {
        let mut batch = (*one_row_result()).clone();
        batch.additional_results.push((*one_row_result()).clone());
        doc.document.update(cx, |document, cx| {
            document.setup_data_grid(
                Arc::new(batch),
                "SELECT 1; SELECT 2".into(),
                None,
                None,
                window,
                cx,
            );
        });
    });
    doc.settle();

    let results = doc
        .window
        .update(|_, cx| doc.document.read(cx).result_tabs.result_tabs.len());
    assert_eq!(results, 2, "one tab per statement");
    assert!(!doc.plan_shown(), "the results are shown, not the plan");
    assert!(doc.has_plan_tab());
}

#[gpui::test]
fn a_plan_that_cannot_find_its_connection_is_reported_centrally(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.window.update(|_, cx| {
        cx.set_global(dbflux_ui_base::app_state_entity::AppStateGlobal {
            entity: doc.app_state.clone(),
        });
        doc.document
            .update(cx, |document, _| document.connection_id = None);
    });

    doc.window.update(|_, cx| {
        doc.document.update(cx, |document, cx| {
            document.start_plan(
                super::PlanTarget {
                    route: super::PlanRoute::Current,
                    text: "SELECT 1".into(),
                    language: QueryLanguage::Sql,
                    scope: "Statement L1–1".into(),
                    statement_count: 1,
                },
                cx,
            );
        });
    });

    let unread = doc
        .window
        .update(|_, cx| doc.app_state.read(cx).unread_error_count);
    assert_eq!(unread, 1, "the failure reaches the error badge");
    assert!(doc.requests().is_empty());
    assert!(!doc.has_plan_tab());
}

fn set_active_database(doc: &mut Doc<'_>, database: &str) {
    let id = doc.profile_id;
    doc.window.update(|_, cx| {
        doc.app_state.update(cx, |state, _| {
            if let Some(connected) = state.connections_mut().get_mut(&id) {
                connected.active_database = Some(database.to_string());
            }
        });
    });
}

#[gpui::test]
fn a_result_is_not_planned_on_a_shared_session_that_moved_to_another_database(
    cx: &mut TestAppContext,
) {
    let driver = FakeDriver::new(DbKind::MySQL).with_query_plan(sample_plan());
    let mut doc = open(cx, driver, "SELECT 2");
    doc.result_from_origin_database(Some("shop"));
    set_active_database(&mut doc, "reporting");

    doc.explain_from_result_menu();

    assert!(doc.requests().is_empty(), "nothing is planned");
    let message = doc.plan_state(|state| match state {
        PlanState::Failed(failure) => Some(failure.summary.clone()),
        _ => None,
    });
    let message = message.expect("the tab says why");
    assert!(
        message.contains("shop") && message.contains("reporting"),
        "{message}"
    );
}

#[gpui::test]
fn a_result_is_planned_on_a_shared_session_still_on_its_database(cx: &mut TestAppContext) {
    let driver = FakeDriver::new(DbKind::MySQL).with_query_plan(sample_plan());
    let mut doc = open(cx, driver, "SELECT 2");
    doc.result_from_origin_database(Some("shop"));
    set_active_database(&mut doc, "shop");

    doc.explain_from_result_menu();

    assert_eq!(doc.requests(), ["SELECT 1"]);
    assert!(doc.plan_state(ready));
}

fn set_editor_database(doc: &mut Doc<'_>, database: &str) {
    doc.window.update(|_, cx| {
        doc.document.update(cx, |document, _| {
            document.source.exec_ctx.database = Some(database.to_string());
        });
    });
}

#[gpui::test]
fn the_editor_explain_is_refused_on_a_shared_session_that_moved_to_another_database(
    cx: &mut TestAppContext,
) {
    let driver = FakeDriver::new(DbKind::MySQL).with_query_plan(sample_plan());
    let mut doc = open(cx, driver, "SELECT 2");
    set_editor_database(&mut doc, "shop");
    set_active_database(&mut doc, "reporting");

    doc.explain_and_settle();

    assert!(doc.requests().is_empty(), "nothing is planned");
    let message = doc
        .plan_state(|state| match state {
            PlanState::Failed(failure) => Some(failure.summary.clone()),
            _ => None,
        })
        .expect("the tab says why");
    assert!(
        message.contains("shop") && message.contains("reporting"),
        "{message}"
    );
    assert!(
        !message.contains("result"),
        "the editor wording does not talk about a result: {message}"
    );
}

#[gpui::test]
fn the_editor_explain_runs_on_a_shared_session_still_on_the_editors_database(
    cx: &mut TestAppContext,
) {
    let driver = FakeDriver::new(DbKind::MySQL).with_query_plan(sample_plan());
    let mut doc = open(cx, driver, "SELECT 2");
    set_editor_database(&mut doc, "shop");
    set_active_database(&mut doc, "shop");

    doc.explain_and_settle();

    assert_eq!(doc.requests(), ["SELECT 2"]);
    assert!(doc.plan_state(ready));
}

/// Starts a query after the Plan, the way running one does: it takes the
/// document's primary task (cancelling the Plan's) and becomes the active
/// query.
fn start_newer_query(doc: &mut Doc<'_>) -> uuid::Uuid {
    doc.window.update(|_, cx| {
        doc.document.update(cx, |document, cx| {
            let target = dbflux_core::TaskTarget {
                profile_id: document.connection_id.expect("a connection"),
                database: None,
            };
            let (task_id, _token) = document.runner.start_primary_for_target(
                dbflux_core::TaskKind::Query,
                "SELECT newer",
                Some(target),
                cx,
            );
            document.execution.active_query_task = Some(crate::code::ActiveQueryTask { task_id });
            document.state = crate::types::DocumentState::Executing;
            task_id
        })
    })
}

fn assert_newer_query_still_running(doc: &mut Doc<'_>, newer: uuid::Uuid) {
    doc.window.update(|_, cx| {
        let document = doc.document.read(cx);
        assert!(
            document.runner.is_primary_active(),
            "the newer query's task is not cancelled"
        );
        assert_eq!(
            document
                .execution
                .active_query_task
                .as_ref()
                .map(|task| task.task_id),
            Some(newer),
            "the newer query is still the active one"
        );
        assert_eq!(document.state, crate::types::DocumentState::Executing);
    });
}

#[gpui::test]
fn closing_a_loading_plan_leaves_a_newer_query_running(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.explain();
    let newer = start_newer_query(&mut doc);

    doc.window.update(|_, cx| {
        doc.document
            .update(cx, |document, cx| document.close_plan_tab(cx))
    });

    assert!(!doc.has_plan_tab());
    assert_newer_query_still_running(&mut doc, newer);
}

#[gpui::test]
fn refusing_a_plan_while_one_loads_leaves_a_newer_query_running(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.explain();
    let newer = start_newer_query(&mut doc);

    doc.window.update(|_, cx| {
        doc.document.update(cx, |document, cx| {
            let target = document.plan.tab.as_ref().expect("the tab").target.clone();
            document.show_plan_refused(target, "refused".into(), QueryPlanMode::Estimate, cx);
        })
    });

    assert!(doc.plan_state(|state| matches!(state, PlanState::Failed(_))));
    assert_newer_query_still_running(&mut doc, newer);
}

#[gpui::test]
fn cancelling_a_loading_plan_leaves_a_newer_query_running(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.explain();
    let newer = start_newer_query(&mut doc);

    doc.window.update(|_, cx| {
        let view = doc.document.read(cx).plan.view().expect("the Plan tab");
        view.update(cx, |_, cx| {
            cx.emit(crate::query_plan::PlanViewEvent::CancelRequested)
        });
    });

    assert!(doc.plan_state(|state| matches!(state, PlanState::Cancelled)));
    assert_newer_query_still_running(&mut doc, newer);
}

#[gpui::test]
fn a_plan_that_finishes_after_a_newer_query_started_leaves_the_documents_state_alone(
    cx: &mut TestAppContext,
) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.explain();
    doc.window.update(|_, cx| {
        doc.document.update(cx, |document, _| {
            let newer = document
                .execution
                .active_query_task
                .as_mut()
                .expect("the plan is the active task");
            newer.task_id = uuid::Uuid::new_v4();
            document.state = crate::types::DocumentState::Executing;
        });
    });

    doc.settle();

    assert!(doc.plan_state(ready), "its own tab still gets the plan");
    let state = doc.window.update(|_, cx| doc.document.read(cx).state);
    assert_eq!(state, crate::types::DocumentState::Executing);
    let active = doc
        .window
        .update(|_, cx| doc.document.read(cx).execution.active_query_task.is_some());
    assert!(active, "the newer query is still the active one");
    assert_eq!(doc.events(), ["started"], "no completion for the newer one");
}

#[gpui::test]
fn the_previous_query_error_does_not_cover_the_plan_but_comes_back_with_the_results(
    cx: &mut TestAppContext,
) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.window.update(|_, cx| {
        doc.document.update(cx, |document, _| {
            document
                .execution
                .execution_history
                .push(crate::code::ExecutionRecord {
                    id: uuid::Uuid::new_v4(),
                    started_at: std::time::Instant::now(),
                    finished_at: None,
                    result: None,
                    error: Some("relation \"missing\" does not exist".into()),
                    rows_affected: None,
                    is_script: false,
                });
            document.execution.active_execution_index = Some(0);
            document.layout = SqlQueryLayout::Split;
        });
    });
    doc.settle();
    assert!(doc.has("code-result-error"), "the error shows on its own");

    doc.explain_and_settle();

    assert!(doc.has("plan-view"));
    assert!(!doc.has("code-result-error"), "the plan is not covered");

    doc.window.update(|_, cx| {
        doc.document
            .update(cx, |document, cx| document.leave_plan_tab(cx));
    });
    doc.settle();
    assert!(
        doc.has("code-result-error"),
        "the error is kept for the results"
    );
}

fn analyzing(plan: QueryPlan) -> FakeDriver {
    pg(Some(plan))
        .with_explain_analyze_support(true)
        .with_analyzed_query_plan(analyzed_plan())
}

fn analyzed_plan() -> QueryPlan {
    let mut plan = sample_plan();
    plan.planning_time = Some(0.5);
    plan.execution_time = Some(12.0);
    plan.roots[0].actual_rows = Some(10.0);
    plan.roots[0].actual_loops = Some(1);
    plan.roots[0].actual_total_time = Some(12.0);
    plan
}

#[gpui::test]
fn the_run_mode_switch_needs_a_driver_that_can_analyze(cx: &mut TestAppContext) {
    let mut without = open(cx, pg(Some(sample_plan())), "SELECT 1");
    without.explain_and_settle();
    assert!(without.plan_state(ready));
    assert!(!without.has("segmented-plan-run-mode-analyze"));
    assert!(!without.has("segmented-plan-run-mode-estimate"));

    let mut with = open(cx, analyzing(sample_plan()), "SELECT 1");
    with.explain_and_settle();
    assert!(with.has("segmented-plan-run-mode-analyze"));
    assert!(with.has("segmented-plan-run-mode-estimate"));
}

#[gpui::test]
fn choosing_analyze_shows_the_gate_and_sends_nothing(cx: &mut TestAppContext) {
    let mut doc = open(cx, analyzing(sample_plan()), "SELECT 1");
    doc.explain_and_settle();
    assert_eq!(doc.requests().len(), 1);

    doc.click("segmented-plan-run-mode-analyze");

    assert!(doc.has("plan-analyze-gate"));
    assert!(doc.has("plan-analyze-run"));
    assert_eq!(doc.requests().len(), 1, "the gate asks the engine nothing");
    assert_eq!(doc.modes(), [QueryPlanMode::Estimate]);
}

#[gpui::test]
fn run_analyze_sends_an_analyzed_request_for_the_same_statement(cx: &mut TestAppContext) {
    let mut doc = open(cx, analyzing(sample_plan()), "SELECT 1");
    doc.explain_and_settle();
    doc.click("segmented-plan-run-mode-analyze");

    doc.click("plan-analyze-run");

    assert_eq!(
        doc.modes(),
        [QueryPlanMode::Estimate, QueryPlanMode::Analyze]
    );
    let requests = doc.requests();
    assert_eq!(requests[0], requests[1], "the same statement");
    assert!(doc.plan_state(ready));
    let analyzed = doc.window.update(|_, cx| {
        doc.document
            .read(cx)
            .plan
            .view()
            .unwrap()
            .read(cx)
            .is_analyzed()
    });
    assert!(analyzed);
    let tabs = doc
        .window
        .update(|_, cx| doc.document.read(cx).result_tab_total());
    assert_eq!(tabs, 1, "still one Plan tab");

    doc.click("segmented-plan-run-mode-estimate");
    let mode = doc.window.update(|_, cx| {
        doc.document
            .read(cx)
            .plan
            .view()
            .unwrap()
            .read(cx)
            .run_mode()
    });
    assert_eq!(mode, PlanRunMode::Estimate);
    assert_eq!(doc.requests().len(), 2, "going back asks nothing");
}

#[gpui::test]
fn analyze_from_a_result_menu_uses_the_result_query(cx: &mut TestAppContext) {
    let mut doc = open(cx, analyzing(sample_plan()), "SELECT 2");
    doc.add_result_tab();
    doc.explain_from_result_menu();
    doc.click("segmented-plan-run-mode-analyze");

    doc.click("plan-analyze-run");

    assert_eq!(
        doc.modes(),
        [QueryPlanMode::Estimate, QueryPlanMode::Analyze]
    );
    let requests = doc.requests();
    assert_eq!(
        requests[1], "SELECT 1",
        "the result's query, not the buffer"
    );
}

#[gpui::test]
fn a_write_statement_cannot_be_analyzed(cx: &mut TestAppContext) {
    let mut doc = open(
        cx,
        analyzing(sample_plan()),
        "UPDATE orders SET status = 'x' WHERE id = 2;",
    );
    doc.cursor_at(0);
    doc.explain_and_settle();
    assert!(doc.plan_state(ready), "the estimate still plans a write");

    assert!(doc.has("plan-analyze-refusal"));
    doc.click("segmented-plan-run-mode-analyze");

    assert!(!doc.has("plan-analyze-gate"));
    assert!(doc.has("plan-node-0") || doc.has("plan-diagram"));
    assert_eq!(
        doc.modes(),
        [QueryPlanMode::Estimate],
        "nothing else was sent"
    );
}

#[gpui::test]
fn a_read_statement_shows_no_refusal(cx: &mut TestAppContext) {
    let mut doc = open(cx, analyzing(sample_plan()), "SELECT 1");
    doc.explain_and_settle();

    assert!(!doc.has("plan-analyze-refusal"));
}

#[gpui::test]
fn cancelling_while_analyzing_stops_the_task_and_keeps_the_estimate(cx: &mut TestAppContext) {
    let mut doc = open(cx, analyzing(sample_plan()), "SELECT 1");
    doc.explain_and_settle();
    doc.click("segmented-plan-run-mode-analyze");

    doc.window.update(|_, cx| {
        let view = doc.document.read(cx).plan.view().expect("the Plan tab");
        view.update(cx, |_, cx| cx.emit(PlanViewEvent::AnalyzeRequested));
    });
    assert!(doc.plan_state(|state| matches!(state, PlanState::Loading)));
    let running = doc
        .window
        .update(|_, cx| doc.document.read(cx).runner.is_primary_active());
    assert!(running, "analyzing is a task Cancel can stop");

    // Cancel without settling, so the driver call has not run yet.
    let handled = doc.window.update(|window, cx| {
        doc.document.update(cx, |document, cx| {
            document.dispatch_command(Command::Cancel, window, cx)
        })
    });
    assert!(handled);
    assert!(doc.plan_state(|state| matches!(state, PlanState::Cancelled)));
    let running = doc
        .window
        .update(|_, cx| doc.document.read(cx).runner.is_primary_active());
    assert!(!running, "Cancel released the task");
    assert_eq!(
        doc.modes(),
        [QueryPlanMode::Estimate],
        "the analyzed request is still on its way"
    );

    doc.settle();
    assert_eq!(
        doc.modes(),
        [QueryPlanMode::Estimate, QueryPlanMode::Analyze],
        "the late answer did arrive"
    );
    assert!(
        doc.plan_state(|state| matches!(state, PlanState::Cancelled)),
        "the late answer is dropped"
    );

    doc.click("segmented-plan-run-mode-estimate");
    assert!(doc.plan_state(ready), "the estimate is one click away");
}

#[gpui::test]
fn the_plan_tab_says_analyzed_only_for_an_analyzed_plan(cx: &mut TestAppContext) {
    let mut doc = open(cx, analyzing(sample_plan()), "SELECT 1");
    doc.explain_and_settle();
    let meta = |doc: &mut Doc<'_>| {
        doc.window
            .update(|_, cx| doc.document.read(cx).plan_tab_meta(cx))
            .map(|meta| meta.to_string())
    };
    assert_eq!(meta(&mut doc), None);

    doc.click("segmented-plan-run-mode-analyze");
    doc.click("plan-analyze-run");
    assert_eq!(meta(&mut doc).as_deref(), Some("analyzed"));
}

#[gpui::test]
fn the_gate_and_the_run_mode_entries_are_in_the_pane_actions(cx: &mut TestAppContext) {
    let mut doc = open(cx, analyzing(sample_plan()), "SELECT 1");
    doc.explain_and_settle();
    let menu = doc.menu();
    assert!(
        menu.iter().any(|id| id == "plan-run-mode-analyze"),
        "{menu:?}"
    );
    assert!(
        menu.iter().any(|id| id == "plan-run-mode-estimate"),
        "{menu:?}"
    );

    doc.click("segmented-plan-run-mode-analyze");
    let menu = doc.menu();
    assert!(menu.iter().any(|id| id == "plan-analyze-run"), "{menu:?}");
}

fn failure_summary(doc: &mut Doc<'_>) -> Option<String> {
    doc.plan_state(|state| match state {
        PlanState::Failed(failure) => Some(failure.summary.clone()),
        _ => None,
    })
}

#[gpui::test]
fn analyze_is_refused_when_the_editor_moved_to_another_connection_and_keeps_the_estimate(
    cx: &mut TestAppContext,
) {
    let mut doc = open(cx, analyzing(sample_plan()), "SELECT 1");
    doc.explain_and_settle();
    doc.click("segmented-plan-run-mode-analyze");
    let other_driver = analyzing(sample_plan());
    let other = doc.connect_other(&other_driver, "production");
    doc.window.update(|_, cx| {
        doc.document
            .update(cx, |document, _| document.connection_id = Some(other));
    });

    doc.click("plan-analyze-run");

    assert_eq!(
        doc.modes(),
        [QueryPlanMode::Estimate],
        "nothing was analyzed"
    );
    assert!(other_driver.stats().query_plan_requests.is_empty());
    assert!(
        failure_summary(&mut doc).is_some_and(|summary| summary.contains("different connection")),
    );

    doc.click("segmented-plan-run-mode-estimate");
    assert!(doc.plan_state(ready), "the estimate is still there");
}

#[gpui::test]
fn analyze_does_not_take_over_a_query_that_is_still_running(cx: &mut TestAppContext) {
    let mut doc = open(cx, analyzing(sample_plan()), "SELECT 1");
    doc.explain_and_settle();
    doc.click("segmented-plan-run-mode-analyze");
    let newer = doc.window.update(|_, cx| {
        doc.document.update(cx, |document, cx| {
            let target = dbflux_core::TaskTarget {
                profile_id: document.connection_id.expect("a connection"),
                database: None,
            };
            let (task_id, _token) = document.runner.start_primary_for_target(
                dbflux_core::TaskKind::Query,
                "SELECT newer",
                Some(target),
                cx,
            );
            document.execution.active_query_task = Some(crate::code::ActiveQueryTask { task_id });
            document.state = crate::types::DocumentState::Executing;
            task_id
        })
    });

    doc.click("plan-analyze-run");

    assert_eq!(
        doc.modes(),
        [QueryPlanMode::Estimate],
        "nothing was analyzed"
    );
    assert!(failure_summary(&mut doc).is_some_and(|summary| summary.contains("still running")),);
    assert_newer_query_still_running(&mut doc, newer);

    doc.click("segmented-plan-run-mode-estimate");
    assert!(doc.plan_state(ready));
}

#[gpui::test]
fn analyze_from_a_result_whose_connection_went_away_keeps_the_estimate(cx: &mut TestAppContext) {
    let mut doc = open(cx, analyzing(sample_plan()), SCRIPT);
    let other_driver = analyzing(sample_plan());
    let other = doc.connect_other(&other_driver, "reporting");
    doc.result_from_origin_then_switch_to(other);
    doc.explain_from_result_menu();
    doc.click("segmented-plan-run-mode-analyze");
    let origin = doc.profile_id;
    doc.disconnect(origin);

    doc.click("plan-analyze-run");

    assert_eq!(doc.modes(), [QueryPlanMode::Estimate]);
    assert!(
        failure_summary(&mut doc).is_some_and(|summary| summary.contains("no longer connected")),
    );
    doc.click("segmented-plan-run-mode-estimate");
    assert!(doc.plan_state(ready), "the estimate is still there");
}

#[gpui::test]
fn a_read_only_document_does_not_offer_analyze(cx: &mut TestAppContext) {
    let mut doc = open(cx, analyzing(sample_plan()), "SELECT 1");
    doc.window.update(|_, cx| {
        doc.document
            .update(cx, |document, _| document.read_only = true);
    });

    doc.explain_and_settle();

    assert!(doc.plan_state(ready), "the estimate still plans");
    assert!(!doc.has("segmented-plan-run-mode-analyze"));
    assert!(!doc.has("plan-analyze-gate"));
}

#[gpui::test]
fn analyze_is_refused_on_another_database_than_the_estimate_planned_on(cx: &mut TestAppContext) {
    let mut doc = open(cx, analyzing(sample_plan()), "SELECT 1");
    set_editor_database(&mut doc, "shop");
    doc.explain_and_settle();
    doc.click("segmented-plan-run-mode-analyze");
    set_editor_database(&mut doc, "reporting");

    doc.click("plan-analyze-run");

    assert_eq!(
        doc.modes(),
        [QueryPlanMode::Estimate],
        "nothing was analyzed"
    );
    assert!(
        failure_summary(&mut doc).is_some_and(|summary| summary.contains("different database")),
    );
    doc.click("segmented-plan-run-mode-estimate");
    assert!(doc.plan_state(ready), "the estimate is still there");
}

#[test]
fn an_unsupported_estimate_settles_an_orphaned_plan_as_completed() {
    let unsupported = Err(dbflux_core::DbError::NotSupported("no explain".into()));

    assert_eq!(
        super::plan_task_outcome(QueryPlanMode::Estimate, &unsupported),
        Ok(())
    );
    assert!(super::plan_task_outcome(QueryPlanMode::Analyze, &unsupported).is_err());
    assert!(
        super::plan_task_outcome(
            QueryPlanMode::Estimate,
            &Err(dbflux_core::DbError::query_failed("boom"))
        )
        .is_err()
    );
}

/// Delegates to the fake connection and counts the cleanups the document asks
/// for after a cancelled plan.
struct CleanupCounting {
    inner: Arc<dyn dbflux_core::Connection>,
    cleanups: Arc<std::sync::atomic::AtomicUsize>,
}

impl dbflux_core::Connection for CleanupCounting {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        self.inner.metadata()
    }

    fn ping(&self) -> Result<(), DbError> {
        self.inner.ping()
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, request: &dbflux_core::QueryRequest) -> Result<QueryResult, DbError> {
        self.inner.execute(request)
    }

    fn cancel(&self, handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
        self.inner.cancel(handle)
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        self.inner.cancel_active()
    }

    fn cleanup_after_cancel(&self) -> Result<(), DbError> {
        self.cleanups
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn query_plan(&self, request: &QueryPlanRequest) -> Result<QueryPlan, DbError> {
        self.inner.query_plan(request)
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, DbError> {
        self.inner.schema()
    }

    fn kind(&self) -> DbKind {
        self.inner.kind()
    }

    fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
        self.inner.schema_loading_strategy()
    }

    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        self.inner.dialect()
    }
}

fn count_cleanups(doc: &mut Doc<'_>) -> Arc<std::sync::atomic::AtomicUsize> {
    let cleanups = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    doc.window.update(|_, cx| {
        let profile_id = doc.profile_id;
        let counter = cleanups.clone();
        doc.app_state.update(cx, |state, _| {
            let connected = state
                .connections_mut()
                .get_mut(&profile_id)
                .expect("the document's connection");
            connected.connection = Arc::new(CleanupCounting {
                inner: connected.connection.clone(),
                cleanups: counter,
            });
        });
    });
    cleanups
}

#[gpui::test]
fn an_accepted_plan_cancel_cleans_up_the_connection(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    let cleanups = count_cleanups(&mut doc);
    doc.explain();

    let handled = doc.window.update(|window, cx| {
        doc.document.update(cx, |document, cx| {
            document.dispatch_command(Command::Cancel, window, cx)
        })
    });
    assert!(handled);
    doc.settle();

    assert_eq!(
        cleanups.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the driver's cancelled state is reset once"
    );
}

#[gpui::test]
fn a_refused_plan_cancel_leaves_the_connection_alone(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    let cleanups = count_cleanups(&mut doc);
    doc.explain();
    refuse_cancel_of_active_task(&mut doc);

    doc.window.update(|_, cx| {
        doc.document
            .update(cx, |document, cx| document.close_plan_tab(cx))
    });
    doc.settle();

    assert_eq!(cleanups.load(std::sync::atomic::Ordering::SeqCst), 0);
}

fn refuse_cancel_of_active_task(doc: &mut Doc<'_>) {
    doc.window.update(|_, cx| {
        let task_id = doc
            .document
            .read(cx)
            .execution
            .active_query_task
            .as_ref()
            .expect("the plan is the active task")
            .task_id;
        doc.app_state.update(cx, |state, _| {
            state.mark_task_point_of_no_return(task_id, "cannot be stopped");
        });
    });
}

#[gpui::test]
fn a_closed_plan_that_settles_late_releases_the_document(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.explain();
    refuse_cancel_of_active_task(&mut doc);
    doc.window.update(|_, cx| {
        doc.document
            .update(cx, |document, cx| document.close_plan_tab(cx))
    });

    doc.settle();

    let (active, state) = doc.window.update(|_, cx| {
        let document = doc.document.read(cx);
        (
            document.execution.active_query_task.is_some(),
            document.state,
        )
    });
    assert!(!active, "the settled plan no longer is the active task");
    assert_eq!(state, crate::types::DocumentState::Clean);
}

#[gpui::test]
fn a_closed_plan_that_settles_late_keeps_a_newer_querys_state(cx: &mut TestAppContext) {
    let mut doc = open(cx, pg(Some(sample_plan())), "SELECT 1");
    doc.explain();
    refuse_cancel_of_active_task(&mut doc);
    doc.window.update(|_, cx| {
        doc.document
            .update(cx, |document, cx| document.close_plan_tab(cx))
    });
    let newer = doc.window.update(|_, cx| {
        doc.document.update(cx, |document, cx| {
            let (task_id, _token) = document.runner.start_primary_for_target(
                dbflux_core::TaskKind::Query,
                "newer",
                None,
                cx,
            );
            document.execution.active_query_task = Some(crate::code::ActiveQueryTask { task_id });
            document.state = crate::types::DocumentState::Executing;
            task_id
        })
    });

    doc.settle();

    let (active, state) = doc.window.update(|_, cx| {
        let document = doc.document.read(cx);
        (
            document
                .execution
                .active_query_task
                .as_ref()
                .map(|task| task.task_id),
            document.state,
        )
    });
    assert_eq!(active, Some(newer));
    assert_eq!(state, crate::types::DocumentState::Executing);
}
