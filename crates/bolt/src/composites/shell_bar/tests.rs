use std::cell::Cell;
use std::rc::Rc;

use gpui::{
    Context, FocusHandle, IntoElement, Modifiers, ParentElement as _, Render, Styled as _,
    TestAppContext, VisualTestContext, Window, div,
};

use super::*;
use crate::accessibility::{FrameCapture, accessibility_node, accessible_name};
use gpui::Role;

#[test]
fn shortcut_keys_join_into_one_keycap() {
    let keys: Vec<SharedString> = vec!["Ctrl".into(), "Shift".into(), "P".into()];
    assert_eq!(
        CommandSearch::shortcut_label(&keys).as_ref(),
        "Ctrl Shift P"
    );
}

#[test]
fn badge_hides_at_zero_and_caps_large_counts() {
    assert_eq!(NotificationBell::badge_text(0), None);
    assert_eq!(NotificationBell::badge_text(2).as_deref(), Some("2"));
    assert_eq!(NotificationBell::badge_text(99).as_deref(), Some("99"));
    assert_eq!(NotificationBell::badge_text(120).as_deref(), Some("99+"));
}

#[test]
fn badge_color_follows_the_urgency() {
    let theme = gpui_component::Theme::default();

    assert_eq!(
        NotificationBell::badge_colors(BellUrgency::None, &theme),
        None
    );
    assert_eq!(
        NotificationBell::badge_colors(BellUrgency::Info, &theme),
        Some((theme.secondary, theme.foreground, Some(theme.input)))
    );
    assert_eq!(
        NotificationBell::badge_colors(BellUrgency::Approval, &theme),
        Some((theme.primary, theme.primary_foreground, None))
    );
    assert_eq!(
        NotificationBell::badge_colors(BellUrgency::Error, &theme),
        Some((theme.danger, theme.danger_foreground, None))
    );
}

#[gpui::test]
fn shell_icons_default_until_a_product_installs_its_own(cx: &mut TestAppContext) {
    cx.update(|cx| {
        let icons = ShellIcons::current(cx);
        assert_eq!(icons.search, IconSource::from(IconName::Search));
        assert_eq!(icons.bell, IconSource::from(IconName::Bell));

        cx.set_global(ShellIcons {
            search: IconSource::Svg("icons/search.svg".into()),
            bell: IconSource::Svg("icons/bell.svg".into()),
        });
        let icons = ShellIcons::current(cx);
        assert_eq!(icons.search, IconSource::Svg("icons/search.svg".into()));
        assert_eq!(icons.bell, IconSource::Svg("icons/bell.svg".into()));
    });
}

struct Host {
    count: usize,
    opens: Rc<Cell<usize>>,
    bell_clicks: Rc<Cell<usize>>,
    focus: FocusHandle,
}

impl Render for Host {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        let opens = self.opens.clone();
        let bell_clicks = self.bell_clicks.clone();

        div()
            .size_full()
            .flex()
            .gap_4()
            .p_4()
            .child(
                CommandSearch::new("search", "Search or run a command")
                    .shortcut(["Ctrl", "K"])
                    .focus_handle(&self.focus)
                    .on_open(move |_, _| opens.set(opens.get() + 1)),
            )
            .child(
                NotificationBell::new("bell", "Notifications", self.count)
                    .urgency(BellUrgency::Error)
                    .on_click(move |_, _, _| bell_clicks.set(bell_clicks.get() + 1)),
            )
    }
}

struct Opened<'a> {
    opens: Rc<Cell<usize>>,
    bell_clicks: Rc<Cell<usize>>,
    focus: FocusHandle,
    window: &'a mut VisualTestContext,
}

fn open(count: usize, cx: &mut TestAppContext) -> Opened<'_> {
    cx.update(gpui_component::init);
    cx.update(|cx| {
        crate::theme::install(
            crate::brand::Brand::neutral(),
            crate::palette::PaletteMode::Dark,
            crate::density::Density::Default,
            cx,
        );
    });

    let opens = Rc::new(Cell::new(0));
    let bell_clicks = Rc::new(Cell::new(0));
    let focus = cx.update(|cx| cx.focus_handle());
    let (_view, window) = cx.add_window_view({
        let opens = opens.clone();
        let bell_clicks = bell_clicks.clone();
        let focus = focus.clone();
        move |_, _| Host {
            count,
            opens,
            bell_clicks,
            focus,
        }
    });
    window.run_until_parked();

    Opened {
        opens,
        bell_clicks,
        focus,
        window,
    }
}

#[gpui::test]
fn the_search_field_opens_on_click_and_on_enter(cx: &mut TestAppContext) {
    let opened = open(0, cx);
    let window = opened.window;

    let field = window
        .debug_bounds("search")
        .expect("the field is found by its id");
    let key = window
        .debug_bounds("search-key")
        .expect("the shortcut keycap is found under the field id");
    assert!(field.contains(&key.center()));

    window.simulate_click(field.center(), Modifiers::default());
    window.run_until_parked();
    assert_eq!(opened.opens.get(), 1);

    let focus = opened.focus.clone();
    window.update(|window, cx| focus.focus(window, cx));
    window.simulate_keystrokes("enter");
    assert_eq!(opened.opens.get(), 2);
}

#[gpui::test]
fn the_bell_draws_its_badge_only_with_something_unread(cx: &mut TestAppContext) {
    let opened = open(3, cx);
    let window = opened.window;

    let bell = window
        .debug_bounds("bell")
        .expect("the bell is found by its id");
    let badge = window
        .debug_bounds("notifications-badge")
        .expect("an unread count draws the badge");
    assert!(badge.left() > bell.left() && badge.top() < bell.center().y);

    window.simulate_click(bell.center(), Modifiers::default());
    window.run_until_parked();
    assert_eq!(opened.bell_clicks.get(), 1);
}

#[gpui::test]
fn the_search_field_is_a_button_and_its_keycap_reads_the_shortcut(cx: &mut TestAppContext) {
    let opened = open(0, cx);
    let window = opened.window;
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    let field = accessibility_node(&frame, "search").expect("field node");
    assert_eq!(field.role(), Role::Button);
    assert_eq!(
        accessible_name(&frame, "search").as_deref(),
        Some("Search or run a command")
    );
    assert_eq!(
        accessible_name(&frame, "search-key").as_deref(),
        Some("Ctrl K")
    );
}

#[gpui::test]
fn the_bell_badge_reads_the_count(cx: &mut TestAppContext) {
    let opened = open(2, cx);
    let window = opened.window;
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    assert_eq!(
        accessible_name(&frame, "notifications-badge").as_deref(),
        Some("2")
    );
}

#[gpui::test]
fn the_bell_badge_takes_the_id_the_product_chooses(cx: &mut TestAppContext) {
    struct CustomBadge;

    impl Render for CustomBadge {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().size_full().p_4().child(
                NotificationBell::new("bell", "Notifications", 5)
                    .urgency(BellUrgency::Info)
                    .badge_id("inbox-badge"),
            )
        }
    }

    open(0, cx);
    let (_view, window) = cx.add_window_view(|_, _| CustomBadge);
    window.run_until_parked();

    assert!(window.debug_bounds("inbox-badge").is_some());
    assert!(window.debug_bounds("notifications-badge").is_none());
}

#[gpui::test]
fn the_bell_is_a_button_named_by_its_label(cx: &mut TestAppContext) {
    let opened = open(0, cx);
    let window = opened.window;
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    let bell = accessibility_node(&frame, "bell").expect("bell node");
    assert_eq!(bell.role(), Role::Button);
    assert_eq!(
        accessible_name(&frame, "bell").as_deref(),
        Some("Notifications")
    );
}

#[gpui::test]
fn a_bell_without_a_click_handler_is_still_a_button(cx: &mut TestAppContext) {
    struct BareBell;

    impl Render for BareBell {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .size_full()
                .p_4()
                .child(NotificationBell::new("bell", "Notifications", 0))
        }
    }

    open(0, cx);
    let (_view, window) = cx.add_window_view(|_, _| BareBell);
    window.run_until_parked();
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    let bell = accessibility_node(&frame, "bell").expect("bell node");
    assert_eq!(bell.role(), Role::Button);
    assert_eq!(
        accessible_name(&frame, "bell").as_deref(),
        Some("Notifications")
    );
}

struct NarrowBar {
    shrinkable: bool,
}

impl Render for NarrowBar {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().p_4().child(
            div().flex().w(gpui::px(240.0)).child(
                CommandSearch::new("search", "Search or run a command")
                    .shortcut(["Ctrl", "K"])
                    .shrinkable(self.shrinkable),
            ),
        )
    }
}

fn search_width(shrinkable: bool, cx: &mut TestAppContext) -> gpui::Pixels {
    open(0, cx);
    let (_view, window) = cx.add_window_view(move |_, _| NarrowBar { shrinkable });
    window.run_until_parked();

    window
        .debug_bounds("search")
        .expect("the field is laid out")
        .size
        .width
}

#[gpui::test]
fn a_shrinkable_search_field_fits_a_narrow_title_bar(cx: &mut TestAppContext) {
    let width = search_width(true, cx);

    assert!(width <= gpui::px(240.0), "width {width:?}");
    assert!(width >= gpui::px(120.0), "width {width:?}");
}

#[gpui::test]
fn the_search_field_keeps_its_width_by_default(cx: &mut TestAppContext) {
    assert_eq!(search_width(false, cx), gpui::px(420.0));
}

#[gpui::test]
fn the_bell_hides_its_badge_at_zero(cx: &mut TestAppContext) {
    let opened = open(0, cx);

    assert!(opened.window.debug_bounds("bell").is_some());
    assert!(opened.window.debug_bounds("notifications-badge").is_none());
}
