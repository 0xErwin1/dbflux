use std::cell::RefCell;
use std::rc::Rc;

use gpui::{
    Context, IntoElement, Modifiers, ParentElement as _, Render, SharedString, Styled as _,
    TestAppContext, VisualTestContext, Window, div,
};

use super::*;

fn icon(name: &str) -> IconSource {
    IconSource::Svg(format!("icons/{name}.svg").into())
}

fn entries() -> Vec<RailEntry> {
    vec![
        RailEntry::new("connections", icon("database"), "Connections").active(true),
        RailEntry::new("settings", icon("settings"), "Settings").bottom(),
        RailEntry::new("scripts", icon("terminal"), "Scripts").count(0, BellUrgency::Info),
        RailEntry::new("approvals", icon("bot"), "Approvals").count(3, BellUrgency::Approval),
    ]
}

#[test]
fn partition_keeps_order_within_each_edge() {
    let entries = entries();
    let (top, bottom) = ActivityRail::partition(&entries);

    let top_ids: Vec<&str> = top.iter().map(|entry| entry.id.as_ref()).collect();
    let bottom_ids: Vec<&str> = bottom.iter().map(|entry| entry.id.as_ref()).collect();

    assert_eq!(top_ids, ["connections", "scripts", "approvals"]);
    assert_eq!(bottom_ids, ["settings"]);
}

#[test]
fn builders_set_state_placement_and_count() {
    let entry = RailEntry::new("audit", icon("fingerprint"), "Audit");
    assert!(!entry.active);
    assert_eq!(entry.placement, RailPlacement::Top);
    assert_eq!(entry.count, None);
    assert_eq!(entry.element_id().as_ref(), "rail-audit");
    assert_eq!(entry.count_element_id().as_ref(), "rail-audit-count");

    let entry = entry.active(true).bottom().count(4, BellUrgency::Error);
    assert!(entry.active);
    assert_eq!(entry.placement, RailPlacement::Bottom);
    assert_eq!(entry.count, Some((4, BellUrgency::Error)));
}

#[test]
fn entries_compare_by_icon() {
    let database = RailEntry::new("a", icon("database"), "A");
    assert_eq!(database, RailEntry::new("a", icon("database"), "A"));
    assert_ne!(database, RailEntry::new("a", icon("bot"), "A"));
}

#[test]
fn active_button_takes_the_tint_wash_and_idle_stays_transparent() {
    let theme = gpui_component::Theme::default();
    let tint = ChromeColors::tint(&theme);

    let active = rail_button_colors(&theme, true);
    assert_eq!(active.rest, tint.opacity(ShellMetrics::RAIL_ACTIVE_ALPHA));
    assert_eq!(active.icon, tint);

    let idle = rail_button_colors(&theme, false);
    assert_eq!(idle.rest, gpui::transparent_black());
    assert_eq!(idle.hover, theme.secondary);
    assert_eq!(idle.icon, theme.muted_foreground);
}

struct Host {
    selected: Rc<RefCell<Vec<SharedString>>>,
}

impl Render for Host {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        let selected = self.selected.clone();
        div().size_full().child(
            ActivityRail::new("rail", entries()).on_select(move |id, _, _| {
                selected.borrow_mut().push(id.clone());
            }),
        )
    }
}

fn open(cx: &mut TestAppContext) -> (Rc<RefCell<Vec<SharedString>>>, &mut VisualTestContext) {
    cx.update(gpui_component::init);
    cx.update(|cx| {
        crate::theme::install(
            crate::brand::Brand::neutral(),
            crate::palette::PaletteMode::Dark,
            crate::density::Density::Default,
            cx,
        );
    });

    let selected = Rc::new(RefCell::new(Vec::new()));
    let (_view, window) = cx.add_window_view({
        let selected = selected.clone();
        move |_, _| Host { selected }
    });
    window.run_until_parked();
    (selected, window)
}

#[gpui::test]
fn a_count_draws_its_badge_over_the_button(cx: &mut TestAppContext) {
    let (_selected, window) = open(cx);

    let button = window
        .debug_bounds("rail-approvals")
        .expect("the button is found by its element id");
    let badge = window
        .debug_bounds("rail-approvals-count")
        .expect("a non-zero count draws a badge");
    assert!(button.contains(&badge.center()) || badge.contains(&button.top_right()));

    assert!(
        window.debug_bounds("rail-scripts-count").is_none(),
        "a zero count draws no badge"
    );
    assert!(window.debug_bounds("rail-connections-count").is_none());
}

#[gpui::test]
fn clicking_a_button_selects_its_entry(cx: &mut TestAppContext) {
    let (selected, window) = open(cx);

    let settings = window
        .debug_bounds("rail-settings")
        .expect("button laid out");
    let scripts = window
        .debug_bounds("rail-scripts")
        .expect("button laid out");
    assert!(
        settings.top() > scripts.bottom(),
        "the bottom entry is pinned below the top entries"
    );

    window.simulate_click(scripts.center(), Modifiers::default());
    window.run_until_parked();
    assert_eq!(
        selected.borrow().as_slice(),
        [SharedString::from("scripts")]
    );
}
