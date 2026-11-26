//! "Explain this query" in the result cell menu: where it is offered, where
//! it sits and what choosing it hands to the host.

use super::DataGridPanel;
use crate::data_grid_panel::TableContextMenu;
use crate::data_grid_panel::context_menu::build_menu_tree;
use crate::keyboard_test_support::{connected_app_state, host_document, init_keyboard_runtime};
use dbflux_components::composites::{HoverIntent, MenuNav, MenuNode};
use dbflux_core::{ColumnKind, ColumnMeta, DbKind, QueryResult, Value};
use dbflux_test_support::fake_driver::FakeDriver;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext, px};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

const QUERY: &str = "SELECT ts, load FROM samples";

fn column(name: &str, kind: ColumnKind) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        type_name: "t".to_string(),
        kind,
        nullable: true,
        is_primary_key: false,
    }
}

fn result(chartable: bool) -> Arc<QueryResult> {
    let columns = if chartable {
        vec![
            column("ts", ColumnKind::Timestamp),
            column("load", ColumnKind::Float),
        ]
    } else {
        vec![column("name", ColumnKind::Text)]
    };
    let row = vec![Value::Null; columns.len()];

    Arc::new(QueryResult::table(columns, vec![row], None, Duration::ZERO))
}

struct Fixture<'a> {
    panel: Entity<DataGridPanel>,
    window: &'a mut VisualTestContext,
    asked: Rc<RefCell<Vec<String>>>,
}

fn fixture(
    cx: &mut TestAppContext,
    driver: FakeDriver,
    chartable: bool,
    hosted: bool,
) -> Fixture<'_> {
    init_keyboard_runtime(cx);
    let (app_state, profile_id) = connected_app_state(cx, &driver, "shop");
    let asked: Rc<RefCell<Vec<String>>> = Rc::default();

    let (host, window) = host_document(
        cx,
        {
            let asked = asked.clone();
            move |window, cx| {
                cx.new(|cx| {
                    let mut panel = DataGridPanel::new_for_result(
                        result(chartable),
                        QUERY.to_string(),
                        Some(profile_id),
                        app_state,
                        window,
                        cx,
                    );
                    if hosted {
                        panel.set_explain_host(Some(Rc::new(move |query, _| {
                            asked.borrow_mut().push(query);
                        })));
                    }
                    panel
                })
            }
        },
        |panel, cx| panel.active_context(cx),
        DataGridPanel::dispatch_command,
    );
    let panel = window.update(|_, cx| host.read(cx).document.clone());

    Fixture {
        panel,
        window,
        asked,
    }
}

impl Fixture<'_> {
    fn menu_ids(&mut self) -> Vec<String> {
        let menu = TableContextMenu {
            row: 0,
            col: 0,
            position: gpui::Point {
                x: px(100.0),
                y: px(100.0),
            },
            nav: MenuNav::new(),
            hover: HoverIntent::new(),
            is_document_view: false,
            is_column_header: false,
            doc_field_path: None,
            doc_field_value: None,
            row_actions: Vec::new(),
        };

        self.window.update(|_, cx| {
            self.panel.update(cx, |panel, cx| {
                build_menu_tree(panel, &menu, cx)
                    .iter()
                    .filter_map(|node: &MenuNode| node.id.as_deref().map(str::to_string))
                    .collect()
            })
        })
    }
}

#[gpui::test]
fn the_menu_lists_explain_right_after_chart_this_query(cx: &mut TestAppContext) {
    let mut fixture = fixture(cx, FakeDriver::new(DbKind::Postgres), true, true);

    let ids = fixture.menu_ids();
    let chart = ids
        .iter()
        .position(|id| id == "chart-this-query")
        .expect("the result is chartable");

    assert_eq!(
        ids.get(chart + 1).map(String::as_str),
        Some("explain-this-query")
    );
}

#[gpui::test]
fn a_result_that_cannot_be_charted_still_offers_explain(cx: &mut TestAppContext) {
    let mut fixture = fixture(cx, FakeDriver::new(DbKind::Postgres), false, true);

    let ids = fixture.menu_ids();

    assert!(!ids.iter().any(|id| id == "chart-this-query"));
    assert!(ids.iter().any(|id| id == "explain-this-query"), "{ids:?}");
}

#[gpui::test]
fn explain_is_hidden_when_the_driver_has_no_plans(cx: &mut TestAppContext) {
    let driver = FakeDriver::new(DbKind::Postgres).with_explain_support(false);
    let mut fixture = fixture(cx, driver, true, true);

    let ids = fixture.menu_ids();

    assert!(ids.iter().any(|id| id == "chart-this-query"));
    assert!(!ids.iter().any(|id| id == "explain-this-query"), "{ids:?}");
}

#[gpui::test]
fn explain_is_hidden_when_the_host_cannot_show_a_plan(cx: &mut TestAppContext) {
    let mut fixture = fixture(cx, FakeDriver::new(DbKind::Postgres), true, false);

    let ids = fixture.menu_ids();

    assert!(!ids.iter().any(|id| id == "explain-this-query"), "{ids:?}");
}

#[gpui::test]
fn choosing_explain_hands_the_result_query_to_the_host(cx: &mut TestAppContext) {
    let fixture = fixture(cx, FakeDriver::new(DbKind::Postgres), true, true);

    fixture.window.update(|window, cx| {
        fixture.panel.update(cx, |panel, cx| {
            panel.open_context_menu_on_selection_for_test(window, cx);
            panel.activate_menu_row_for_test("explain-this-query", window, cx);
        });
    });
    fixture.window.run_until_parked();

    assert_eq!(*fixture.asked.borrow(), [QUERY]);
    let menu_open = fixture
        .window
        .update(|_, cx| fixture.panel.read(cx).context_menu.is_some());
    assert!(!menu_open, "choosing the row closes the menu");
}
