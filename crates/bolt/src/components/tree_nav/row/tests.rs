use std::sync::{Arc, Mutex};

use gpui::{
    Context, IntoElement, Modifiers, Render, SharedString, TestAppContext, VisualTestContext,
    Window, div,
};

use super::{TreeNavChevron, TreeNavRow};
use crate::accessibility::{FrameCapture, accessibility_node, accessible_name};
use crate::icon::IconSource;
use crate::primitives::{Badge, BadgeTone};
use crate::tokens::{BASE_REM, TreeMetrics};

fn install_test_theme(cx: &mut TestAppContext) {
    cx.update(gpui_component::init);
    cx.update(|cx| {
        crate::theme::install(
            crate::brand::Brand::neutral(),
            crate::palette::PaletteMode::Dark,
            crate::density::Density::Default,
            cx,
        );
    });
}

fn icon(name: &str) -> IconSource {
    IconSource::Svg(SharedString::from(name.to_string()))
}

/// What the harness records: row clicks and chevron toggles.
#[derive(Default)]
struct Clicks {
    row: Vec<&'static str>,
    toggle: Vec<&'static str>,
}

/// A group row with a chevron and its child, a leaf with a trailing badge,
/// in a 300 px column whose clicks are recorded.
struct Harness {
    clicks: Arc<Mutex<Clicks>>,
}

impl Render for Harness {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        use gpui::Styled as _;
        use gpui::{ParentElement as _, StatefulInteractiveElement as _};

        let record = |clicks: &Arc<Mutex<Clicks>>, name: &'static str, toggle: bool| {
            let clicks = clicks.clone();
            move |_: &gpui::ClickEvent, _: &mut Window, _: &mut gpui::App| {
                let mut clicks = clicks.lock().expect("click recorder lock");
                if toggle {
                    clicks.toggle.push(name);
                } else {
                    clicks.row.push(name);
                }
            }
        };

        let group = TreeNavRow::new("tree-row-1", "Users", 0)
            .emphasized(true)
            .chevron(TreeNavChevron::new(
                true,
                icon("chevron-down"),
                "Collapse Users",
                record(&self.clicks, "group", true),
            ))
            .icon(icon("folder"), BadgeTone::Neutral)
            .count("1")
            .build(cx)
            .on_click(record(&self.clicks, "group", false));

        let leaf = TreeNavRow::new("tree-row-2", "Create user", 1)
            .selected(true)
            .chevron_spacer(true)
            .leading(div().child("NEW"))
            .trailing(Badge::new("201", BadgeTone::Success).id("tree-status-2"))
            .build(cx)
            .on_click(record(&self.clicks, "leaf", false));

        div().size_full().child(
            div()
                .flex()
                .flex_col()
                .w(gpui::px(300.0))
                .child(group)
                .child(leaf),
        )
    }
}

fn open(cx: &mut TestAppContext) -> (Arc<Mutex<Clicks>>, &mut VisualTestContext) {
    install_test_theme(cx);
    let clicks = Arc::new(Mutex::new(Clicks::default()));
    let (_, window) = cx.add_window_view({
        let clicks = clicks.clone();
        move |_, _| Harness { clicks }
    });
    window.run_until_parked();
    (clicks, window)
}

/// The row is a label named by its label, announcing its selected and
/// expanded states; the chevron is a button named by the caller's words;
/// the parts carry their ids.
#[gpui::test]
fn the_row_and_its_parts_carry_their_names_and_ids(cx: &mut TestAppContext) {
    let (_clicks, window) = open(cx);
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    assert_eq!(
        accessible_name(&frame, "tree-row-1").as_deref(),
        Some("Users")
    );
    assert_eq!(
        accessible_name(&frame, "tree-row-1-chevron").as_deref(),
        Some("Collapse Users")
    );
    let group = accessibility_node(&frame, "tree-row-1").expect("the group row has a node");
    assert_eq!(group.is_expanded(), Some(true));
    assert_eq!(group.is_selected(), Some(false));

    let leaf = accessibility_node(&frame, "tree-row-2").expect("the leaf row has a node");
    assert_eq!(leaf.is_selected(), Some(true));
    assert_eq!(
        leaf.is_expanded(),
        None,
        "a leaf announces no expanded state"
    );
    assert_eq!(
        accessible_name(&frame, "tree-status-2").as_deref(),
        Some("201")
    );

    for part in [
        "tree-row-1-icon",
        "tree-row-1-name",
        "tree-row-1-count",
        "tree-row-2-trailing",
    ] {
        assert!(window.debug_bounds(part).is_some(), "{part} is laid out");
    }
    assert!(
        window.debug_bounds("tree-row-2-chevron").is_none(),
        "a leaf draws no chevron"
    );
    assert!(
        window.debug_bounds("tree-row-2-count").is_none(),
        "a row without a count draws none"
    );
    let checked = crate::accessibility::assert_clickables_named(&frame);
    assert!(checked.contains(&"tree-row-1-chevron".to_string()));
}

/// The row is 26 px tall, a child is indented by one level, and the
/// trailing slot sits at the row's end.
#[gpui::test]
fn the_row_follows_the_tree_metrics(cx: &mut TestAppContext) {
    let (_clicks, window) = open(cx);
    let rem = gpui::px(BASE_REM);

    let group = window.debug_bounds("tree-row-1").expect("group laid out");
    let leaf = window.debug_bounds("tree-row-2").expect("leaf laid out");
    let group_name = window
        .debug_bounds("tree-row-1-name")
        .expect("group name laid out");
    let leaf_name = window
        .debug_bounds("tree-row-2-name")
        .expect("leaf name laid out");
    let trailing = window
        .debug_bounds("tree-row-2-trailing")
        .expect("trailing slot laid out");

    assert_eq!(group.size.height, TreeMetrics::ROW_HEIGHT.to_pixels(rem));
    assert!(
        leaf_name.left() > group_name.left(),
        "the child's name is indented past its parent's"
    );
    assert_eq!(
        trailing.right(),
        leaf.right() - TreeMetrics::PADDING_X,
        "the trailing slot ends at the row's padding"
    );
}

/// A click on the chevron toggles without reaching the row; a click on the
/// trailing slot reaches nothing; a click on the name reaches the row.
#[gpui::test]
fn chevron_and_trailing_clicks_stop_before_the_row(cx: &mut TestAppContext) {
    let (clicks, window) = open(cx);

    let chevron = window
        .debug_bounds("tree-row-1-chevron")
        .expect("chevron laid out");
    window.simulate_click(chevron.center(), Modifiers::default());
    let trailing = window
        .debug_bounds("tree-status-2")
        .expect("trailing badge laid out");
    window.simulate_click(trailing.center(), Modifiers::default());

    {
        let clicks = clicks.lock().expect("click recorder lock");
        assert_eq!(clicks.toggle, vec!["group"]);
        assert!(clicks.row.is_empty(), "neither click reached a row");
    }

    let name = window
        .debug_bounds("tree-row-2-name")
        .expect("leaf name laid out");
    window.simulate_click(name.center(), Modifiers::default());
    assert_eq!(
        clicks.lock().expect("click recorder lock").row,
        vec!["leaf"]
    );
}

/// A row whose name is replaced by the caller's element (a rename field).
struct RenameHarness;

impl Render for RenameHarness {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        use gpui::{InteractiveElement as _, ParentElement as _, Styled as _};

        let field = div()
            .debug_selector(|| "tree-rename-field".to_string())
            .flex_1()
            .min_w_0()
            .h(gpui::px(18.0));
        let row = TreeNavRow::new("tree-row-9", "Users", 0)
            .icon(icon("folder"), BadgeTone::Neutral)
            .name(field)
            .build(cx);

        div()
            .size_full()
            .child(div().flex().flex_col().w(gpui::px(300.0)).child(row))
    }
}

/// `.name(...)` lays the caller's element out in the name's slot, after the
/// icon and through the row's end, so no drawn label shares the slot.
#[gpui::test]
fn a_replaced_name_takes_the_name_slot(cx: &mut TestAppContext) {
    install_test_theme(cx);
    let (_, window) = cx.add_window_view(|_, _| RenameHarness);
    window.run_until_parked();

    let row = window.debug_bounds("tree-row-9").expect("row laid out");
    let icon = window
        .debug_bounds("tree-row-9-icon")
        .expect("icon laid out");
    let field = window
        .debug_bounds("tree-rename-field")
        .expect("the replacement is laid out");

    assert_eq!(
        field.left(),
        icon.right() + TreeMetrics::GAP,
        "the replacement follows the icon"
    );
    assert_eq!(
        field.right(),
        row.right() - TreeMetrics::PADDING_X,
        "the replacement fills the slot the drawn name would take"
    );
}
