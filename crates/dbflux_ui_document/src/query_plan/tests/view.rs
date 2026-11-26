//! The Plan tab's view: what it draws for a plan, how the view switch and the
//! keyboard move through it, what the inspector gets and when tooltips show.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use dbflux_app::keymap::Command;
use dbflux_components::accessibility::FrameCapture;
use dbflux_components::theme;
use dbflux_core::QueryPlan;
use gpui::{
    AnyView, AppContext as _, Context, Entity, IntoElement, Modifiers, ParentElement as _, Render,
    Styled as _, TestAppContext, VisualTestContext, Window, div, point,
};

use super::fixtures::{weekly_revenue, without_costs};
use crate::query_plan::inspector::PlanInspector;
use crate::query_plan::{PlanFailure, PlanState, PlanView, PlanViewEvent, PlanViewMode};

/// An id built at run time, for the `&'static str` the debug selectors take.
fn id(text: String) -> &'static str {
    Box::leak(text.into_boxed_str())
}

#[derive(Debug, Clone, PartialEq)]
enum Seen {
    Focused,
    Opened(String),
    Closed,
    Retry,
    Cancel,
}

struct Harness<'a> {
    view: Entity<PlanView>,
    window: &'a mut VisualTestContext,
    frame: Arc<FrameCapture>,
    events: Rc<RefCell<Vec<Seen>>>,
    inspectors: Rc<RefCell<Vec<AnyView>>>,
}

fn open(cx: &mut TestAppContext) -> Harness<'_> {
    cx.update(theme::init);

    let (view, window) = cx.add_window_view(|_, cx| {
        PlanView::new("Statement L7–16".to_string(), PlanViewMode::Diagram, cx)
    });
    let events = Rc::new(RefCell::new(Vec::new()));
    let inspectors = Rc::new(RefCell::new(Vec::new()));

    window.update(|_, cx| {
        let (seen, mounted) = (events.clone(), inspectors.clone());
        cx.subscribe(&view, move |_, event: &PlanViewEvent, _| {
            let record = match event {
                PlanViewEvent::Focused => Seen::Focused,
                PlanViewEvent::OpenInspector { title, content } => {
                    mounted.borrow_mut().push(content.clone());
                    Seen::Opened(title.to_string())
                }
                PlanViewEvent::CloseInspector => Seen::Closed,
                PlanViewEvent::RetryRequested => Seen::Retry,
                PlanViewEvent::CancelRequested => Seen::Cancel,
            };
            seen.borrow_mut().push(record);
        })
        .detach();
    });
    let frame = FrameCapture::observe(window);

    Harness {
        view,
        window,
        frame,
        events,
        inspectors,
    }
}

impl Harness<'_> {
    fn ready(&mut self, plan: &QueryPlan) {
        let plan = plan.clone();
        self.window.update(|_, cx| {
            self.view.update(cx, |view, cx| {
                view.set_ready(&plan, Duration::from_millis(4), cx)
            });
        });
        self.settle();
    }

    fn settle(&mut self) {
        for _ in 0..3 {
            self.window.run_until_parked();
            self.window.update(|window, _| window.refresh());
        }
        self.window.run_until_parked();
    }

    fn mode(&mut self) -> PlanViewMode {
        self.window.update(|_, cx| self.view.read(cx).mode())
    }

    fn selected(&mut self) -> Option<usize> {
        self.window.update(|_, cx| self.view.read(cx).selected())
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

    fn press(&mut self, command: Command) -> bool {
        let handled = self.window.update(|window, cx| {
            self.view
                .update(cx, |view, cx| view.dispatch_command(command, window, cx))
        });
        self.settle();
        handled
    }

    fn hover(&mut self, id: &'static str) {
        let bounds = self
            .window
            .debug_bounds(id)
            .unwrap_or_else(|| panic!("`{id}` is drawn"));
        self.window
            .simulate_mouse_move(bounds.center(), None, Modifiers::default());
        self.window
            .executor()
            .advance_clock(Duration::from_millis(600));
        self.window.run_until_parked();
    }

    /// Hovers an element drawn on the diagram surface. The surface is scaled
    /// and panned, and its debug bounds are in its own untransformed space, so
    /// the pointer goes where the element is on screen, as a real one would.
    fn hover_on_surface(&mut self, id: &'static str) {
        let bounds = self
            .window
            .debug_bounds(id)
            .unwrap_or_else(|| panic!("`{id}` is drawn"));
        let (origin, pan, zoom) = self.window.update(|_, cx| {
            let camera = &self.view.read(cx).camera;
            (camera.viewport_origin, camera.pan, camera.zoom)
        });
        let centre = bounds.center();
        let on_screen = point(
            origin.x + pan.x + (centre.x - origin.x) * zoom,
            origin.y + pan.y + (centre.y - origin.y) * zoom,
        );
        self.window
            .simulate_mouse_move(on_screen, None, Modifiers::default());
        self.window
            .executor()
            .advance_clock(Duration::from_millis(600));
        self.window.run_until_parked();
    }

    fn tooltip(&mut self) -> Option<String> {
        let frame = self.frame.frame(self.window);

        frame
            .nodes()
            .find(|(_, node)| node.id() == "tooltip-popup")
            .map(|(_, node)| node.content_text().to_string())
    }

    fn seen(&self) -> Vec<Seen> {
        self.events.borrow().clone()
    }
}

#[gpui::test]
fn a_ready_plan_opens_on_the_diagram_with_a_card_per_node(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());

    assert_eq!(plan.mode(), PlanViewMode::Diagram);
    for node in 0..6 {
        assert!(plan.has(id(format!("plan-node-{node}"))), "node {node}");
    }
    assert!(!plan.has("plan-tree"), "the Tree view is not drawn");
    assert!(!plan.has("plan-raw"), "the Raw view is not drawn");
    assert!(plan.has("plan-totals"));
    assert!(plan.has("plan-footer"));
}

#[gpui::test]
fn the_view_switch_moves_between_diagram_tree_and_raw(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());

    plan.click("segmented-plan-view-tree");
    assert_eq!(plan.mode(), PlanViewMode::Tree);
    assert!(plan.has("plan-tree") && !plan.has("plan-node-0"));
    for row in 0..6 {
        assert!(plan.has(id(format!("plan-tree-row-{row}"))), "row {row}");
    }

    plan.click("segmented-plan-view-raw");
    assert_eq!(plan.mode(), PlanViewMode::Raw);
    assert!(plan.has("plan-raw") && plan.has("plan-raw-copy"));

    plan.click("segmented-plan-view-diagram");
    assert_eq!(plan.mode(), PlanViewMode::Diagram);
    assert!(plan.has("plan-node-0"));
}

#[gpui::test]
fn the_cycle_command_visits_the_three_views_in_order(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());

    assert!(plan.press(Command::CycleResultView));
    assert_eq!(plan.mode(), PlanViewMode::Tree);
    assert!(plan.press(Command::CycleResultView));
    assert_eq!(plan.mode(), PlanViewMode::Raw);
    assert!(plan.press(Command::CycleResultView));
    assert_eq!(plan.mode(), PlanViewMode::Diagram);
}

#[gpui::test]
fn the_view_switch_does_nothing_while_the_plan_loads(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.settle();

    plan.click("segmented-plan-view-tree");

    assert_eq!(plan.mode(), PlanViewMode::Diagram);
    assert!(plan.has("plan-state-loading"));
    assert!(plan.has("plan-cancel"), "Cancel is offered while loading");
    assert!(!plan.press(Command::CycleResultView), "no plan, no cycling");
}

#[gpui::test]
fn cancel_while_loading_asks_the_document_to_stop_the_plan(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.settle();

    plan.click("plan-cancel");

    assert_eq!(plan.seen(), [Seen::Cancel]);
}

#[gpui::test]
fn the_tree_hides_the_columns_a_plan_has_no_data_for(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&without_costs());
    plan.click("segmented-plan-view-tree");

    assert!(plan.has("plan-tree-header-index"), "an index is used");
    assert!(!plan.has("plan-tree-header-cost"));
    assert!(!plan.has("plan-tree-header-self-cost"));
    assert!(!plan.has("plan-tree-header-rows"));

    let mut with_costs = open(cx);
    with_costs.ready(&weekly_revenue());
    with_costs.click("segmented-plan-view-tree");
    for column in ["index", "cost", "self-cost", "rows"] {
        assert!(
            with_costs.has(id(format!("plan-tree-header-{column}"))),
            "{column}"
        );
    }
}

#[gpui::test]
fn the_plan_warnings_band_shows_only_when_there_are_warnings(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    assert!(plan.has("plan-warnings"));

    let mut quiet = open(cx);
    quiet.ready(&without_costs());
    assert!(!quiet.has("plan-warnings"));
}

#[gpui::test]
fn the_zoom_controls_belong_to_the_diagram_only(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    for control in [
        "plan-zoom-out",
        "plan-zoom-reset",
        "plan-zoom-in",
        "plan-fit",
    ] {
        assert!(plan.has(control), "{control} in the diagram");
    }

    plan.click("segmented-plan-view-tree");
    assert!(!plan.has("plan-fit"));
}

#[gpui::test]
fn zoom_buttons_change_the_zoom_and_fit_restores_a_legible_one(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    let zoom = |plan: &mut Harness| plan.window.update(|_, cx| plan.view.read(cx).camera.zoom);

    let opened = zoom(&mut plan);
    plan.click("plan-zoom-in");
    let zoomed_in = zoom(&mut plan);
    assert!(zoomed_in > opened, "{opened} -> {zoomed_in}");

    plan.click("plan-zoom-reset");
    assert_eq!(zoom(&mut plan), 1.0);

    plan.click("plan-zoom-out");
    assert!(zoom(&mut plan) < 1.0);

    plan.click("plan-fit");
    assert!(
        zoom(&mut plan) >= crate::query_plan::diagram::geometry::MIN_FIT_ZOOM,
        "Fit never picks a zoom the text cannot be read at"
    );
}

#[gpui::test]
fn the_first_arrow_selects_the_costliest_node_and_opens_the_inspector(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    assert_eq!(plan.selected(), None, "nothing is selected on open");
    assert!(
        plan.seen().is_empty(),
        "opening the plan opens no inspector"
    );

    assert!(plan.press(Command::SelectNext));

    assert_eq!(plan.selected(), Some(4), "the Seq Scan");
    assert!(plan.seen().contains(&Seen::Opened("Seq Scan".into())));
    assert!(
        plan.window
            .update(|_, cx| plan.view.read(cx).inspector_open())
    );
}

#[gpui::test]
fn arrows_walk_the_diagram_from_the_leaves_to_the_root_and_back(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    plan.press(Command::SelectNext);
    assert_eq!(plan.selected(), Some(4));

    for expected in [3, 2, 1, 0] {
        assert!(plan.press(Command::ColumnRight));
        assert_eq!(
            plan.selected(),
            Some(expected),
            "right goes toward the root"
        );
    }

    assert!(plan.press(Command::ColumnLeft));
    assert_eq!(plan.selected(), Some(1), "left goes back toward the leaves");

    assert!(plan.press(Command::SelectFirst));
    assert_eq!(plan.selected(), Some(0));
    assert!(plan.press(Command::SelectLast));
    assert_eq!(plan.selected(), Some(5));
}

#[gpui::test]
fn up_and_down_move_between_the_two_scans_under_the_join(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    plan.press(Command::SelectNext);
    assert_eq!(plan.selected(), Some(4));

    plan.press(Command::SelectNext);
    let other = plan.selected();
    assert_eq!(other, Some(5), "the index scan is the node below or above");

    plan.press(Command::SelectPrev);
    assert_eq!(plan.selected(), Some(4));
}

#[gpui::test]
fn clicking_a_node_selects_it_and_mounts_its_inspector(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());

    plan.click("plan-node-5");

    assert_eq!(plan.selected(), Some(5));
    assert!(plan.seen().contains(&Seen::Opened("Index Scan".into())));
}

#[gpui::test]
fn the_inspector_holds_the_description_then_estimate_reads_and_details(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    plan.click("plan-node-5");

    let content = plan
        .inspectors
        .borrow()
        .last()
        .cloned()
        .expect("an inspector was mounted");
    let inspector = content
        .downcast::<PlanInspector>()
        .expect("the rail content is the plan inspector");
    let node = plan
        .window
        .update(|_, cx| inspector.read(cx).node().clone());
    assert_eq!(node.operation, "Index Scan");
    assert_eq!(node.index.as_deref(), Some("customers_pkey"));

    struct Mount(Entity<PlanInspector>);
    impl Render for Mount {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            div().size_full().child(self.0.clone())
        }
    }
    let (_mount, rail) = plan.window.add_window_view(|_, _| Mount(inspector));
    rail.run_until_parked();

    let top = |rail: &mut VisualTestContext, element: &'static str| {
        rail.debug_bounds(element)
            .unwrap_or_else(|| panic!("`{element}` is drawn"))
            .origin
            .y
    };
    let description = top(rail, "plan-inspector-description");
    let estimate = top(rail, "plan-inspector-section-estimate");
    let reads = top(rail, "plan-inspector-section-reads");
    let details = top(rail, "plan-inspector-section-details");
    assert!(
        description < estimate && estimate < reads && reads < details,
        "description {description:?}, Estimate {estimate:?}, Reads {reads:?}, Details {details:?}"
    );

    for field in [
        "startup-cost",
        "total-cost",
        "self-cost",
        "rows",
        "relation",
        "index",
    ] {
        assert!(
            rail.debug_bounds(id(format!("plan-inspector-field-{field}")))
                .is_some(),
            "{field}"
        );
    }
}

#[gpui::test]
fn the_inspector_follows_the_selection_and_the_close_button_ends_it(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    plan.click("plan-node-4");
    plan.click("plan-node-5");

    let opened: Vec<_> = plan
        .seen()
        .into_iter()
        .filter(|seen| matches!(seen, Seen::Opened(_)))
        .collect();
    assert_eq!(
        opened,
        [
            Seen::Opened("Seq Scan".into()),
            Seen::Opened("Index Scan".into())
        ]
    );
    assert_eq!(plan.inspectors.borrow().len(), 2);
    assert_eq!(
        plan.inspectors.borrow()[0].entity_id(),
        plan.inspectors.borrow()[1].entity_id(),
        "one inspector entity is reused"
    );

    assert!(plan.press(Command::Cancel), "Escape closes the inspector");
    assert_eq!(plan.seen().last(), Some(&Seen::Closed));
    assert!(!plan.press(Command::Cancel), "nothing left to close");
    assert_eq!(plan.selected(), Some(5), "the selection stays");
}

#[gpui::test]
fn the_tree_selects_rows_by_click_and_by_arrow(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    plan.click("segmented-plan-view-tree");

    plan.click("plan-tree-row-3");
    assert_eq!(plan.selected(), Some(3));

    assert!(plan.press(Command::SelectNext));
    assert_eq!(plan.selected(), Some(4));
    assert!(plan.press(Command::SelectPrev));
    assert!(plan.press(Command::SelectPrev));
    assert_eq!(plan.selected(), Some(2));

    plan.press(Command::SelectNext);
    assert!(
        plan.press(Command::ColumnLeft),
        "Left folds the Nested Loop"
    );
    assert!(!plan.has("plan-tree-row-4") && !plan.has("plan-tree-row-5"));
    assert!(plan.press(Command::ColumnRight), "Right opens it again");
    assert!(plan.has("plan-tree-row-4"));
}

#[gpui::test]
fn the_raw_view_has_no_selection_to_move(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    plan.click("segmented-plan-view-raw");

    assert!(!plan.press(Command::SelectNext));
    assert_eq!(plan.selected(), None);
}

#[gpui::test]
fn no_tooltip_shows_because_the_plan_opened_or_a_node_has_focus(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    plan.window
        .executor()
        .advance_clock(Duration::from_millis(2000));
    plan.settle();
    assert_eq!(plan.tooltip(), None, "opening the plan shows no tooltip");

    plan.press(Command::SelectNext);
    plan.window
        .executor()
        .advance_clock(Duration::from_millis(2000));
    plan.settle();
    assert_eq!(
        plan.tooltip(),
        None,
        "a node selected by keyboard shows none"
    );
}

#[gpui::test]
fn a_tooltip_shows_on_hover_after_the_delay_and_explains_the_column(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    plan.click("segmented-plan-view-tree");

    plan.hover("plan-tree-header-self-cost");

    let text = plan
        .tooltip()
        .expect("hovering the header shows its tooltip");
    assert!(text.contains("Self cost"), "{text}");
    assert!(text.contains("Node cost minus its children"), "{text}");
}

#[gpui::test]
fn the_tree_row_of_an_operation_with_a_description_explains_it_on_hover(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    plan.click("segmented-plan-view-tree");

    plan.hover("plan-tree-row-4-operation");

    let text = plan.tooltip().expect("the operation has a tooltip");
    assert!(text.contains("Seq Scan"), "{text}");
    assert!(
        text.contains("Reads every row of the table"),
        "the driver's own text: {text}"
    );
}

#[gpui::test]
fn the_costliest_marker_and_the_view_switch_explain_themselves(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    plan.click("segmented-plan-view-tree");

    plan.hover("plan-tree-row-4-costliest");
    let text = plan.tooltip().expect("the marker has a tooltip");
    assert!(text.contains("highest self cost"), "{text}");

    plan.hover("plan-view-tip-raw");
    let text = plan.tooltip().expect("the Raw switch has a tooltip");
    assert!(text.contains("exactly as the engine returned it"), "{text}");
}

#[gpui::test]
fn the_totals_in_the_switch_row_explain_themselves(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());

    plan.hover("plan-totals");

    let text = plan.tooltip().expect("the totals have a tooltip");
    assert!(text.contains("Plan totals"), "{text}");
}

#[gpui::test]
fn a_plan_without_totals_has_no_totals_tooltip(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&without_costs());

    plan.hover("plan-totals");

    assert_eq!(plan.tooltip(), None);
}

#[gpui::test]
fn the_view_switch_still_clicks_with_its_tooltip_areas_over_it(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    assert!(
        plan.has("plan-view-tip-tree"),
        "the tooltip areas are placed"
    );

    plan.click("plan-view-tip-tree");

    assert_eq!(plan.mode(), PlanViewMode::Tree);
}

#[gpui::test]
fn an_unsupported_driver_says_so_and_dims_the_views(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.window.update(|_, cx| {
        plan.view.update(cx, |view, cx| {
            view.set_unsupported("cache-redis".into(), cx)
        });
    });
    plan.settle();

    assert!(plan.has("plan-state-unsupported"));
    assert!(!plan.has("plan-node-0") && !plan.has("plan-tree"));
    plan.click("segmented-plan-view-tree");
    assert_eq!(plan.mode(), PlanViewMode::Diagram, "the switch is disabled");
}

#[gpui::test]
fn more_than_one_statement_shows_the_drivers_message(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.window.update(|_, cx| {
        plan.view.update(cx, |view, cx| {
            view.set_multi_statement(
                "A query plan covers exactly one statement; the query has 3.".into(),
                cx,
            )
        });
    });
    plan.settle();

    assert!(plan.has("plan-state-multi-statement"));
    assert!(!plan.has("plan-state-failed"));
}

#[gpui::test]
fn a_failed_plan_offers_retry_copy_and_the_audit(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.window.update(|_, cx| {
        plan.view.update(cx, |view, cx| {
            view.set_failed(
                PlanFailure {
                    summary: "relation \"order_archive\" does not exist".into(),
                    correlation_id: uuid::Uuid::nil(),
                    clipboard: "ERROR: relation \"order_archive\" does not exist".into(),
                },
                cx,
            )
        });
    });
    plan.settle();

    assert!(plan.has("plan-state-failed"));
    for control in ["plan-retry", "plan-copy-error", "plan-view-in-audit"] {
        assert!(plan.has(control), "{control}");
    }

    plan.click("plan-copy-error");
    let copied = plan
        .window
        .read_from_clipboard()
        .and_then(|item| item.text())
        .expect("the error is on the clipboard");
    assert_eq!(copied, "ERROR: relation \"order_archive\" does not exist");

    plan.click("plan-retry");
    assert_eq!(plan.seen(), [Seen::Retry]);
}

#[gpui::test]
fn a_cancelled_plan_offers_retry(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.window
        .update(|_, cx| plan.view.update(cx, |view, cx| view.set_cancelled(cx)));
    plan.settle();

    assert!(plan.has("plan-state-cancelled"));
    plan.click("plan-retry");
    assert_eq!(plan.seen(), [Seen::Retry]);
}

#[gpui::test]
fn the_raw_view_copies_the_indented_plan_text(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    plan.click("segmented-plan-view-raw");

    plan.click("plan-raw-copy");

    let copied = plan
        .window
        .read_from_clipboard()
        .and_then(|item| item.text())
        .expect("the raw plan is on the clipboard");
    assert_eq!(copied, "[\n  {\n    \"Plan\": {}\n  }\n]");
    assert!(plan.has("plan-raw-code-line-1") && plan.has("plan-raw-code-line-5"));
}

#[gpui::test]
fn a_new_plan_replaces_the_selection_and_the_previous_inspector(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    plan.click("plan-node-5");
    assert_eq!(plan.selected(), Some(5));

    plan.ready(&without_costs());

    assert_eq!(plan.selected(), None);
    assert_eq!(
        plan.seen().last(),
        Some(&Seen::Closed),
        "the rail is closed"
    );
    assert!(
        plan.window
            .update(|_, cx| matches!(plan.view.read(cx).state(), PlanState::Ready(_)))
    );
}

fn covered(plan: &mut Harness<'_>) -> Vec<String> {
    use crate::keyboard_coverage::QUERY_PLAN;
    use dbflux_ui_base::keyboard_coverage::Coverage;

    let menu: Vec<String> = plan.window.update(|_, cx| {
        crate::query_plan::pane_actions(&plan.view, cx)
            .iter()
            .map(|action| action.id.to_string())
            .collect()
    });
    let frame = plan.frame.frame(plan.window);

    Coverage::new(QUERY_PLAN)
        .with_menu_entries(menu)
        .assert_covered(&frame)
}

#[gpui::test]
fn the_loading_state_is_covered_and_lists_cancel_in_the_menu(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.settle();

    let checked = covered(&mut plan);

    assert!(checked.iter().any(|id| id == "plan-cancel"), "{checked:?}");
    let menu: Vec<String> = plan.window.update(|_, cx| {
        crate::query_plan::pane_actions(&plan.view, cx)
            .iter()
            .map(|action| action.id.to_string())
            .collect()
    });
    assert_eq!(menu, ["plan-cancel"]);
}

#[gpui::test]
fn the_error_and_cancelled_states_are_covered(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.window.update(|_, cx| {
        plan.view.update(cx, |view, cx| {
            view.set_failed(
                PlanFailure {
                    summary: "boom".into(),
                    correlation_id: uuid::Uuid::nil(),
                    clipboard: "boom".into(),
                },
                cx,
            )
        });
    });
    plan.settle();
    let checked = covered(&mut plan);
    for id in ["plan-retry", "plan-copy-error", "plan-view-in-audit"] {
        assert!(
            checked.iter().any(|checked| checked == id),
            "{id}: {checked:?}"
        );
    }

    plan.window
        .update(|_, cx| plan.view.update(cx, |view, cx| view.set_cancelled(cx)));
    plan.settle();
    let checked = covered(&mut plan);
    assert!(checked.iter().any(|id| id == "plan-retry"), "{checked:?}");
}

#[gpui::test]
fn the_ready_views_are_covered_by_the_menu_and_the_arrow_keys(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    plan.click("plan-node-1");
    let checked = covered(&mut plan);
    assert!(checked.iter().any(|id| id == "plan-node-1"), "{checked:?}");
    assert!(checked.iter().any(|id| id == "plan-fit"), "{checked:?}");

    plan.click("segmented-plan-view-tree");
    let checked = covered(&mut plan);
    assert!(
        checked.iter().any(|id| id == "plan-tree-row-0"),
        "{checked:?}"
    );
    assert!(
        checked.iter().any(|id| id == "plan-tree-toggle-0"),
        "{checked:?}"
    );

    plan.click("segmented-plan-view-raw");
    let checked = covered(&mut plan);
    assert!(
        checked.iter().any(|id| id == "plan-raw-copy"),
        "{checked:?}"
    );
}

#[gpui::test]
fn dragging_the_diagram_pans_it_and_stops_when_the_button_is_up(cx: &mut TestAppContext) {
    use gpui::{MouseButton, px};

    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    let pan = |plan: &mut Harness| plan.window.update(|_, cx| plan.view.read(cx).camera.pan);

    let area = plan
        .window
        .debug_bounds("plan-diagram")
        .expect("the diagram");
    let start = point(area.right() - px(10.0), area.bottom() - px(10.0));
    let before = pan(&mut plan);

    plan.window
        .simulate_mouse_down(start, MouseButton::Left, Modifiers::default());
    plan.window.simulate_mouse_move(
        start - point(px(60.0), px(30.0)),
        Some(MouseButton::Left),
        Modifiers::default(),
    );
    let dragged = pan(&mut plan);
    assert_eq!(dragged.x, before.x - px(60.0));
    assert_eq!(dragged.y, before.y - px(30.0));

    // The button came up outside the viewport, where its release is not seen.
    plan.window.simulate_mouse_move(
        start - point(px(120.0), px(90.0)),
        None,
        Modifiers::default(),
    );
    assert_eq!(pan(&mut plan), dragged, "no button, no pan");
}

#[gpui::test]
fn the_wheel_zooms_the_diagram(cx: &mut TestAppContext) {
    use gpui::{ScrollDelta, px};

    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    let zoom = |plan: &mut Harness| plan.window.update(|_, cx| plan.view.read(cx).camera.zoom);
    let before = zoom(&mut plan);

    let area = plan
        .window
        .debug_bounds("plan-diagram")
        .expect("the diagram");
    plan.window.simulate_event(gpui::ScrollWheelEvent {
        position: area.center(),
        delta: ScrollDelta::Pixels(point(px(0.0), px(40.0))),
        ..Default::default()
    });

    assert!(zoom(&mut plan) > before);
}

#[gpui::test]
fn hovering_an_operation_in_the_diagram_shows_its_description_after_the_delay(
    cx: &mut TestAppContext,
) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());
    assert_eq!(plan.tooltip(), None, "nothing before the hover");

    plan.hover_on_surface("plan-node-4-operation");

    let text = plan.tooltip().expect("the operation has a tooltip");
    assert!(text.contains("Seq Scan"), "{text}");
    assert!(text.contains("Reads every row of the table"), "{text}");
}

#[gpui::test]
fn hovering_a_diagram_cost_and_an_edge_label_explains_them(cx: &mut TestAppContext) {
    let mut plan = open(cx);
    plan.ready(&weekly_revenue());

    plan.hover_on_surface("plan-node-4-cost");
    let text = plan.tooltip().expect("the cost has a tooltip");
    assert!(text.contains("Estimated cost"), "{text}");

    plan.hover_on_surface("plan-edge-4");
    let text = plan.tooltip().expect("the edge label has a tooltip");
    assert!(text.contains("Estimated rows"), "{text}");
}
