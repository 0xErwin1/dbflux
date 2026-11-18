use super::{
    BadgeLabel, Dropdown, DropdownTriggerVariant, dropdown_dismiss_transition,
    dropdown_focus_ring_state, dropdown_menu_chrome, dropdown_selection_transition,
    dropdown_trigger_render_plan, menu_key,
};
use crate::keymap::{Command, CommandSet, KeyCommand, attach_key_commands};
use crate::primitives::BadgeTone;
use crate::tokens::{ChamferCut, ChromeEdgeRole};
use gpui::{Action, SharedString};

/// The keyboard tests' stand-in for a product keymap action: carries the
/// command id a key binding runs.
#[derive(Clone, Debug, PartialEq, Eq, Action)]
#[action(namespace = bolt_dropdown_test, no_json)]
struct TestKeyCommand {
    id: SharedString,
}

impl KeyCommand for TestKeyCommand {
    fn key_command_id(&self) -> &str {
        &self.id
    }
}

/// The seam the keyboard tests install: the stand-in action, bound under
/// the `TestDropdown` key context the way a product keymap binds its own
/// action.
fn test_key_commands() -> CommandSet {
    CommandSet::new("TestDropdown", attach_key_commands::<TestKeyCommand>)
        .map(Command::SelectNext, "test_next")
        .map(Command::SelectPrev, "test_prev")
        .map(Command::Execute, "test_execute")
        .map(Command::ExpandCollapse, "test_expand")
        .map(Command::Cancel, "test_cancel")
}

#[test]
fn selection_transition_closes_menu_and_clears_highlight() {
    let transition = dropdown_selection_transition(false, 3, 2);

    assert_eq!(
        transition,
        Some(super::DropdownSelectionTransition {
            selected_index: 2,
            open: false,
            highlighted_index: None,
        })
    );
}

#[test]
fn selection_transition_rejects_out_of_range_highlights() {
    let transition = dropdown_selection_transition(false, 2, 9);

    assert_eq!(transition, None);
}

#[test]
fn dismiss_transition_closes_open_menu_and_clears_highlight() {
    let transition = dropdown_dismiss_transition(true);

    assert_eq!(
        transition,
        Some(super::DropdownDismissTransition {
            open: false,
            highlighted_index: None,
        })
    );
}

#[test]
fn dismiss_transition_skips_already_closed_menu() {
    let transition = dropdown_dismiss_transition(false);

    assert_eq!(transition, None);
}

#[test]
fn toolbar_style_builder_selects_toolbar_trigger_variant() {
    let dropdown = Dropdown::new("toolbar-trigger").toolbar_style(true);

    assert_eq!(dropdown.trigger_variant(), DropdownTriggerVariant::Toolbar);
}

#[test]
fn compact_trigger_takes_precedence_over_toolbar_variant() {
    let dropdown = Dropdown::new("compact-trigger")
        .toolbar_style(true)
        .compact_trigger(true);

    assert_eq!(dropdown.trigger_variant(), DropdownTriggerVariant::Compact);
}

#[test]
fn focus_ring_builder_stores_custom_ring_color() {
    let color = gpui::black().opacity(0.25);
    let dropdown = Dropdown::new("ring-trigger").focus_ring_color(Some(color));

    assert_eq!(dropdown.focus_ring_color, Some(color));
    assert!(!dropdown.focus_ring_visible);
}

#[test]
fn focus_ring_builder_can_clear_the_ring_color() {
    let dropdown = Dropdown::new("ring-clear")
        .focus_ring_color(Some(gpui::black().opacity(0.25)))
        .focus_ring_color(None);

    assert!(dropdown.focus_ring_color.is_none());
}

#[test]
fn standard_trigger_draws_its_chamfered_shape() {
    let plan = dropdown_trigger_render_plan(DropdownTriggerVariant::Standard);

    assert!(plan.uses_chamfer_shape);
}

#[test]
fn toolbar_trigger_skips_the_chamfered_shape() {
    let plan = dropdown_trigger_render_plan(DropdownTriggerVariant::Toolbar);

    assert!(!plan.uses_chamfer_shape);
}

#[test]
fn compact_trigger_skips_the_chamfered_shape() {
    let plan = dropdown_trigger_render_plan(DropdownTriggerVariant::Compact);

    assert!(!plan.uses_chamfer_shape);
}

#[test]
fn chevron_trigger_takes_precedence_and_skips_the_chamfered_shape() {
    let dropdown = Dropdown::new("chevron-trigger")
        .toolbar_style(true)
        .compact_trigger(true)
        .chevron_trigger(crate::controls::ButtonVariant::Primary);

    assert_eq!(dropdown.trigger_variant(), DropdownTriggerVariant::Chevron);
    assert!(!dropdown_trigger_render_plan(DropdownTriggerVariant::Chevron).uses_chamfer_shape);
}

#[test]
fn hiding_runtime_focus_ring_keeps_last_custom_color() {
    let custom_color = gpui::black().opacity(0.25);
    let (focus_ring_color, focus_ring_visible) =
        dropdown_focus_ring_state(Some(custom_color), None);

    assert_eq!(focus_ring_color, Some(custom_color));
    assert!(!focus_ring_visible);
}

#[test]
fn showing_runtime_focus_ring_updates_color_and_visibility() {
    let custom_color = gpui::black().opacity(0.25);
    let (focus_ring_color, focus_ring_visible) =
        dropdown_focus_ring_state(None, Some(custom_color));

    assert_eq!(focus_ring_color, Some(custom_color));
    assert!(focus_ring_visible);
}

#[test]
fn dropdown_menu_uses_the_overlay_cut_and_the_strong_line() {
    let chrome = dropdown_menu_chrome();

    assert_eq!(chrome.edge, ChromeEdgeRole::Control);
    assert_eq!(chrome.cut, ChamferCut::OVERLAY);
}

#[test]
fn the_mapped_command_ids_name_their_menu_keys() {
    let keys = test_key_commands();

    assert_eq!(
        menu_key(&keys, "test_next"),
        Some(super::MenuKey::SelectNext)
    );
    assert_eq!(
        menu_key(&keys, "test_prev"),
        Some(super::MenuKey::SelectPrev)
    );
    assert_eq!(
        menu_key(&keys, "test_execute"),
        Some(super::MenuKey::Confirm)
    );
    assert_eq!(
        menu_key(&keys, "test_expand"),
        Some(super::MenuKey::Confirm)
    );
    assert_eq!(menu_key(&keys, "test_cancel"), Some(super::MenuKey::Cancel));
    assert_eq!(menu_key(&keys, "something_else"), None);
}

#[test]
fn the_label_badge_carries_the_text_and_the_tone() {
    let badge = BadgeLabel {
        label: "Staging".into(),
        tone: BadgeTone::Warning,
    };

    assert_eq!(badge.label, "Staging");
    assert_eq!(badge.tone, BadgeTone::Warning);
}

/// Keyboard tests against a real window: the test installs its own
/// [`CommandSet`] seam (a stand-in action carrying command ids, bound
/// under the `TestDropdown` context the way a product keymap binds its
/// action), and an owner view stands in for the pane hosting the dropdown
/// and records every command that reaches it.
mod keyboard {
    use super::super::{Dropdown, DropdownItem, DropdownSelectionChanged};
    use super::TestKeyCommand;
    use gpui::{
        AppContext as _, Context, Entity, FocusHandle, InteractiveElement as _, IntoElement,
        KeyBinding, ParentElement as _, Render, SharedString, Styled as _, TestAppContext,
        VisualTestContext, Window, div,
    };

    fn bind_test_keys(cx: &mut TestAppContext) {
        let context = Some("TestDropdown");
        let run = |id: &str| TestKeyCommand { id: id.into() };

        cx.update(|cx| {
            cx.set_global(super::test_key_commands());
            cx.bind_keys([
                KeyBinding::new("j", run("test_next"), context),
                KeyBinding::new("down", run("test_next"), context),
                KeyBinding::new("k", run("test_prev"), context),
                KeyBinding::new("up", run("test_prev"), context),
                KeyBinding::new("enter", run("test_execute"), context),
                KeyBinding::new("space", run("test_expand"), context),
                KeyBinding::new("escape", run("test_cancel"), context),
            ]);
        });
    }

    struct Owner {
        focus: FocusHandle,
        dropdown: Entity<Dropdown>,
        commands: Vec<SharedString>,
        selections: Vec<usize>,
        _subscription: gpui::Subscription,
    }

    impl Render for Owner {
        fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .size_full()
                .key_context("Owner")
                .track_focus(&self.focus)
                .on_action(cx.listener(|this, action: &TestKeyCommand, _window, _cx| {
                    this.commands.push(action.id.clone());
                }))
                .child(self.dropdown.clone())
        }
    }

    struct Setup<'a> {
        owner: Entity<Owner>,
        window: &'a mut VisualTestContext,
    }

    fn setup(cx: &mut TestAppContext) -> Setup<'_> {
        cx.update(gpui_component::init);
        bind_test_keys(cx);

        // The owner's own bindings on the same keys, like a pane whose
        // key handling drives a dropdown it keeps unfocused.
        cx.update(|cx| {
            cx.bind_keys([
                gpui::KeyBinding::new(
                    "j",
                    TestKeyCommand {
                        id: "test_next".into(),
                    },
                    Some("Owner"),
                ),
                gpui::KeyBinding::new(
                    "enter",
                    TestKeyCommand {
                        id: "test_execute".into(),
                    },
                    Some("Owner"),
                ),
            ]);
        });

        let (owner, window) = cx.add_window_view(|window, cx| {
            let dropdown = cx.new(|_cx| {
                Dropdown::new("keyboard-dropdown").items(vec![
                    DropdownItem::new("One"),
                    DropdownItem::new("Two"),
                    DropdownItem::new("Three"),
                ])
            });

            let subscription = cx.subscribe_in(
                &dropdown,
                window,
                |this: &mut Owner, _, event: &DropdownSelectionChanged, _window, _cx| {
                    this.selections.push(event.index);
                },
            );

            Owner {
                focus: cx.focus_handle(),
                dropdown,
                commands: Vec::new(),
                selections: Vec::new(),
                _subscription: subscription,
            }
        });
        window.run_until_parked();

        let setup = Setup { owner, window };
        let owner = setup.owner.clone();
        setup.window.update(|window, cx| {
            let focus = owner.read(cx).focus.clone();
            focus.focus(window, cx);
        });
        setup.window.run_until_parked();

        setup
    }

    impl Setup<'_> {
        fn dropdown(&mut self) -> Entity<Dropdown> {
            let owner = self.owner.clone();
            self.window.update(|_, cx| owner.read(cx).dropdown.clone())
        }

        fn focus_dropdown(&mut self) {
            let dropdown = self.dropdown();
            self.window.update(|window, cx| {
                dropdown.update(cx, |dropdown, cx| dropdown.focus(window, cx));
            });
            self.window.run_until_parked();
        }

        fn keys(&mut self, keystrokes: &str) {
            self.window.simulate_keystrokes(keystrokes);
            self.window.run_until_parked();
        }

        fn is_open(&mut self) -> bool {
            let dropdown = self.dropdown();
            self.window.update(|_, cx| dropdown.read(cx).is_open())
        }

        fn highlighted(&mut self) -> Option<usize> {
            let dropdown = self.dropdown();
            self.window
                .update(|_, cx| dropdown.read(cx).highlighted_index)
        }

        fn selected(&mut self) -> Option<usize> {
            let dropdown = self.dropdown();
            self.window.update(|_, cx| dropdown.read(cx).selected_index)
        }

        fn dropdown_focused(&mut self) -> bool {
            let dropdown = self.dropdown();
            self.window
                .update(|window, cx| dropdown.read(cx).is_focused(window))
        }

        fn owner_focused(&mut self) -> bool {
            let owner = self.owner.clone();
            self.window
                .update(|window, cx| owner.read(cx).focus.is_focused(window))
        }

        fn commands(&mut self) -> Vec<SharedString> {
            let owner = self.owner.clone();
            self.window.update(|_, cx| owner.read(cx).commands.clone())
        }

        fn selections(&mut self) -> Vec<usize> {
            let owner = self.owner.clone();
            self.window
                .update(|_, cx| owner.read(cx).selections.clone())
        }
    }

    #[gpui::test]
    fn a_focused_dropdown_opens_moves_and_selects_by_keys(cx: &mut TestAppContext) {
        let mut setup = setup(cx);
        setup.focus_dropdown();
        assert!(setup.dropdown_focused(), "the dropdown takes focus");

        setup.keys("enter");
        assert!(setup.is_open(), "Enter opens the menu");
        assert_eq!(setup.highlighted(), Some(0));

        setup.keys("j down");
        assert_eq!(setup.highlighted(), Some(2), "j and Down move down");

        setup.keys("k");
        assert_eq!(setup.highlighted(), Some(1), "k moves up");

        setup.keys("enter");
        assert!(!setup.is_open(), "Enter confirms and closes");
        assert_eq!(setup.selected(), Some(1));
        assert_eq!(setup.selections(), vec![1]);
        assert!(setup.owner_focused(), "focus returns to where it came from");
        assert!(setup.commands().is_empty(), "{:?}", setup.commands());
    }

    #[gpui::test]
    fn space_opens_and_escape_closes_without_selecting(cx: &mut TestAppContext) {
        let mut setup = setup(cx);
        setup.focus_dropdown();

        setup.keys("space");
        assert!(setup.is_open(), "Space opens the menu");

        setup.keys("down escape");
        assert!(!setup.is_open(), "Escape closes the menu");
        assert_eq!(setup.selected(), None, "Escape selects nothing");
        assert!(setup.selections().is_empty());
        assert!(setup.owner_focused(), "Escape hands focus back");
        assert!(setup.commands().is_empty(), "{:?}", setup.commands());
    }

    #[gpui::test]
    fn a_closed_focused_dropdown_lets_other_keys_through(cx: &mut TestAppContext) {
        let mut setup = setup(cx);
        setup.focus_dropdown();

        setup.keys("j");

        assert!(!setup.is_open());
        assert_eq!(setup.commands(), vec!["test_next"]);
    }

    #[gpui::test]
    fn an_unfocused_open_dropdown_leaves_the_keys_to_its_owner(cx: &mut TestAppContext) {
        let mut setup = setup(cx);
        let dropdown = setup.dropdown();
        setup
            .window
            .update(|_, cx| dropdown.update(cx, |dropdown, cx| dropdown.open(cx)));

        setup.keys("j enter");

        assert!(setup.is_open(), "the owner drives a dropdown it keeps");
        assert_eq!(setup.highlighted(), Some(0));
        assert_eq!(setup.commands(), vec!["test_next", "test_execute"]);
    }
}

mod menu_width {
    use super::super::{Dropdown, DropdownItem};
    use gpui::{
        AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _,
        Render, Styled as _, TestAppContext, VisualTestContext, Window, div, px,
    };

    const TRIGGER_WIDTH: f32 = 400.0;

    struct Host {
        dropdown: Entity<Dropdown>,
        trigger_width: f32,
        probe: Option<String>,
        renders: usize,
    }

    impl Render for Host {
        fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            self.renders += 1;

            div()
                .size_full()
                .child(div().w(px(self.trigger_width)).child(self.dropdown.clone()))
                .children(self.probe.clone().map(|text| {
                    // The label's natural width, drawn with the menu rows' text style.
                    div()
                        .absolute()
                        .whitespace_nowrap()
                        .font_family(crate::fonts::ui_family(cx))
                        .text_size(crate::density::font_base(cx))
                        .debug_selector(|| "label-probe".to_string())
                        .child(text)
                }))
        }
    }

    struct Opened<'a> {
        host: Entity<Host>,
        window: &'a mut VisualTestContext,
    }

    fn open_with<'a>(
        cx: &'a mut TestAppContext,
        labels: &[&str],
        probe: Option<String>,
    ) -> Opened<'a> {
        cx.update(gpui_component::init);
        let labels: Vec<String> = labels.iter().map(|label| (*label).to_string()).collect();
        let (host, window) = cx.add_window_view(|_window, cx| {
            let dropdown = cx.new(|_cx| {
                Dropdown::new("width-dropdown")
                    .items(labels.into_iter().map(DropdownItem::new).collect())
            });
            Host {
                dropdown,
                trigger_width: TRIGGER_WIDTH,
                probe,
                renders: 0,
            }
        });
        window.run_until_parked();

        let dropdown = host.update(window, |host, _cx| host.dropdown.clone());
        window.update(|_, cx| dropdown.update(cx, |dropdown, cx| dropdown.open(cx)));
        window.run_until_parked();

        Opened { host, window }
    }

    fn width(window: &mut VisualTestContext, selector: &'static str) -> f32 {
        window
            .debug_bounds(selector)
            .unwrap_or_else(|| panic!("{selector} is laid out"))
            .size
            .width
            .into()
    }

    #[gpui::test]
    fn a_menu_of_short_items_is_as_wide_as_its_trigger(cx: &mut TestAppContext) {
        let opened = open_with(cx, &["A", "B"], None);
        let trigger = width(&mut *opened.window, "width-dropdown");
        let menu = width(&mut *opened.window, "width-dropdown-menu");

        assert_eq!(trigger, TRIGGER_WIDTH);
        assert!(
            menu >= trigger,
            "menu {menu} px under a {trigger} px trigger"
        );
    }

    #[gpui::test]
    fn a_long_item_widens_the_menu_past_its_trigger(cx: &mut TestAppContext) {
        let long = "a very long item label ".repeat(20);
        let opened = open_with(cx, &["A", long.as_str()], Some(long.clone()));
        let trigger = width(&mut *opened.window, "width-dropdown");
        let menu = width(&mut *opened.window, "width-dropdown-menu");
        let natural = width(&mut *opened.window, "label-probe");

        assert!(
            menu > trigger,
            "menu {menu} px not past a {trigger} px trigger"
        );
        assert!(
            menu >= natural,
            "menu {menu} px narrower than its {natural} px label"
        );
    }

    fn resize_trigger(opened: &mut Opened<'_>, width: f32) {
        let host = opened.host.clone();
        opened.window.update(|_, cx| {
            host.update(cx, |host, cx| {
                host.trigger_width = width;
                cx.notify();
            })
        });
        // The width changes during that frame's prepaint, which asks for one more.
        opened.window.update(|_, _| {});
        opened.window.run_until_parked();
    }

    #[gpui::test]
    fn a_menu_follows_a_trigger_resized_while_it_is_open(cx: &mut TestAppContext) {
        let mut opened = open_with(cx, &["A", "B"], None);

        resize_trigger(&mut opened, 250.0);
        assert_eq!(width(&mut *opened.window, "width-dropdown"), 250.0);
        assert_eq!(width(&mut *opened.window, "width-dropdown-menu"), 250.0);

        resize_trigger(&mut opened, 500.0);
        assert_eq!(width(&mut *opened.window, "width-dropdown-menu"), 500.0);
    }

    #[gpui::test]
    fn an_unchanged_trigger_width_schedules_no_more_frames(cx: &mut TestAppContext) {
        let mut opened = open_with(cx, &["A", "B"], None);
        let renders = |opened: &mut Opened<'_>| {
            let host = opened.host.clone();
            opened.window.update(|_, cx| host.read(cx).renders)
        };

        // A window only draws again when something marked it dirty, so a
        // prepaint that refreshes on every frame shows up as extra renders.
        opened.window.update(|_, _| {});
        let settled = renders(&mut opened);
        for _ in 0..3 {
            opened.window.update(|_, _| {});
            opened.window.run_until_parked();
        }

        assert_eq!(
            renders(&mut opened),
            settled,
            "an idle open dropdown keeps redrawing"
        );
    }
}

/// The ids of an open menu: automation and tests pick a row by name instead
/// of by position.
mod menu_ids {
    use super::super::{Dropdown, DropdownItem};
    use gpui::{
        AppContext as _, Context, Entity, IntoElement, Modifiers, ParentElement as _, Render,
        Styled as _, TestAppContext, VisualTestContext, Window, div,
    };

    const DROPDOWN_ID: &str = "menu-ids-dropdown";

    /// A selector under the dropdown's id. `debug_bounds` takes a
    /// `&'static str`, and the id the test asserts on is built from the
    /// dropdown's own id rather than written out, so it leaks this one
    /// string.
    fn selector(suffix: &str) -> &'static str {
        Box::leak(format!("{DROPDOWN_ID}{suffix}").into_boxed_str())
    }

    struct Host {
        dropdown: Entity<Dropdown>,
    }

    impl Render for Host {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().size_full().child(self.dropdown.clone())
        }
    }

    fn open(cx: &mut TestAppContext) -> (Entity<Host>, Entity<Dropdown>, &mut VisualTestContext) {
        cx.update(gpui_component::init);

        let (host, window) = cx.add_window_view(|_window, cx| {
            let dropdown = cx.new(|_cx| {
                Dropdown::new(DROPDOWN_ID).items(vec![
                    DropdownItem::new("One"),
                    DropdownItem::new("Two"),
                    DropdownItem::new("Three"),
                ])
            });
            Host { dropdown }
        });
        window.run_until_parked();

        let dropdown = host.read_with(window, |host, _cx| host.dropdown.clone());
        window.update(|_, cx| dropdown.update(cx, |dropdown, cx| dropdown.open(cx)));
        window.run_until_parked();

        (host, dropdown, window)
    }

    #[gpui::test]
    fn every_open_row_carries_an_id_named_after_the_dropdown_and_its_index(
        cx: &mut TestAppContext,
    ) {
        let (_host, _dropdown, window) = open(cx);

        assert!(
            window.debug_bounds(selector("-menu-rows")).is_some(),
            "the list container is found by name"
        );

        for index in 0..3 {
            let row = selector(&format!("-item-{index}"));
            assert!(
                window.debug_bounds(row).is_some(),
                "row {index} is found by `{row}`"
            );
        }
        assert!(
            window.debug_bounds(selector("-item-3")).is_none(),
            "the list has three rows"
        );
    }

    #[test]
    fn a_row_forwards_the_icon_source_of_its_own_item() {
        let source = crate::icon::IconSource::Svg("icons/folder.svg".into());

        let with_icon = DropdownItem::new("One").icon(source.clone());
        assert_eq!(
            super::super::row_leading_icon(&with_icon),
            Some(source),
            "the row draws the source the item carries"
        );

        assert_eq!(
            super::super::row_leading_icon(&DropdownItem::new("Two")),
            None,
            "an item without an icon asks for none"
        );
    }

    #[gpui::test]
    fn a_row_draws_the_icon_its_item_carries_before_the_label(cx: &mut TestAppContext) {
        let (_host, dropdown, window) = open(cx);

        window.update(|_, cx| {
            dropdown.update(cx, |dropdown, cx| {
                dropdown.set_items(
                    vec![
                        DropdownItem::new("One")
                            .icon(crate::icon::IconSource::Svg("icons/folder.svg".into())),
                        DropdownItem::new("Two"),
                    ],
                    cx,
                );
            })
        });
        window.run_until_parked();

        let icon = window
            .debug_bounds(selector("-item-0-icon"))
            .expect("the first row draws its icon");
        let label = window
            .debug_bounds(selector("-item-0-label"))
            .expect("the first row draws its label");
        let row = window
            .debug_bounds(selector("-item-0"))
            .expect("the first row is laid out");
        let rem_size = window.update(|window, _cx| window.rem_size());
        assert_eq!(
            icon.size.width,
            crate::tokens::Fields::LEADING_ICON.to_pixels(rem_size),
            "the row draws the icon at the leading icon size"
        );
        assert!(
            icon.origin.x < row.center().x,
            "the icon sits at the start of the row"
        );
        assert!(
            icon.right() <= label.origin.x,
            "the icon ends before the label starts"
        );
        assert!(
            window.debug_bounds(selector("-item-1-icon")).is_none(),
            "a row without an icon draws none"
        );
    }

    #[gpui::test]
    fn clicking_a_row_by_its_id_selects_that_row(cx: &mut TestAppContext) {
        let (_host, dropdown, window) = open(cx);

        let bounds = window.debug_bounds(selector("-item-1"));
        let row = bounds.expect("the second row is laid out").center();

        window.simulate_click(row, Modifiers::default());
        window.run_until_parked();

        let selected = window.update(|_window, cx| dropdown.read(cx).selected());
        assert_eq!(selected, Some(1), "clicking the row by its id selects it");
    }
}

/// A wheel over an open menu whose rows overflow `Fields::MENU_MAX_HEIGHT`:
/// the rows scroll natively and the keyboard highlight stays where the keys
/// left it. The event goes through GPUI's own pipeline (`simulate_event`),
/// so the test keeps failing if the menu grows a wheel handler again — that
/// handler advanced the highlight a row and re-anchored it with
/// `scroll_to_item(FirstVisible)` on the same element, which pinned the
/// highlight to its viewport position while the rows streamed past.
mod menu_scroll {
    use super::super::{Dropdown, DropdownItem};
    use crate::tokens::Fields;
    use gpui::{
        AppContext as _, Context, Entity, IntoElement, Modifiers, ParentElement as _, Pixels,
        Render, ScrollDelta, ScrollWheelEvent, Styled as _, TestAppContext, TouchPhase,
        VisualTestContext, Window, div, point, px,
    };

    const DROPDOWN_ID: &str = "scroll-dropdown";

    /// Rows past the ones the menu shows, so the scrollable overflow is
    /// several whole rows and a wheel notch lands strictly inside it.
    const EXTRA_ROWS: usize = 8;

    /// How many rows the wheel carries: two, so the assertion is about the
    /// direction of the scroll and not the exact tracking of one notch.
    const WHEEL_ROWS: f32 = 2.0;

    /// A selector under the dropdown's id. `debug_bounds` takes a
    /// `&'static str`, and the id the test asserts on is built from the
    /// dropdown's own id rather than written out, so it leaks this one
    /// string.
    fn selector(suffix: &str) -> &'static str {
        Box::leak(format!("{DROPDOWN_ID}{suffix}").into_boxed_str())
    }

    /// The item count that overflows the menu: the rows that fit inside
    /// `MENU_MAX_HEIGHT` once its own padding is subtracted, plus the extra
    /// rows that make it scrollable by whole rows.
    fn menu_item_count(rem_size: Pixels) -> usize {
        let row_height = Fields::MENU_ROW_HEIGHT.to_pixels(rem_size);
        let usable = Fields::MENU_MAX_HEIGHT - 2.0 * Fields::MENU_PADDING_Y;
        let fitted = (usable / row_height).floor() as usize;
        fitted + EXTRA_ROWS
    }

    struct Overflow<'a> {
        dropdown: Entity<Dropdown>,
        item_count: usize,
        row_height: Pixels,
        window: &'a mut VisualTestContext,
    }

    impl Overflow<'_> {
        fn highlight(&mut self) -> Option<usize> {
            self.window
                .update(|_, cx| self.dropdown.read(cx).highlighted_index())
        }

        /// The vertical offset of the scrolling rows column. Scrolling the
        /// content down (a wheel with a negative y delta) takes the offset
        /// negative.
        fn rows_offset(&mut self) -> Pixels {
            self.window
                .update(|_, cx| self.dropdown.read(cx).menu_scroll_handle.offset().y)
        }

        /// A real wheel event, delivered through GPUI's event pipeline to
        /// the center of the rows column.
        fn wheel(&mut self) {
            let position = self
                .window
                .debug_bounds(selector("-menu-rows"))
                .expect("the rows column is laid out")
                .center();
            let rows = point(px(0.0), -(WHEEL_ROWS * self.row_height));
            self.window.simulate_event(ScrollWheelEvent {
                position,
                delta: ScrollDelta::Pixels(rows),
                modifiers: Modifiers::none(),
                touch_phase: TouchPhase::Moved,
            });
            self.window.run_until_parked();
        }
    }

    struct Host {
        dropdown: Entity<Dropdown>,
        item_count: usize,
    }

    impl Render for Host {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().size_full().child(self.dropdown.clone())
        }
    }

    fn open_overflowing(cx: &mut TestAppContext) -> Overflow<'_> {
        cx.update(gpui_component::init);

        let (host, window) = cx.add_window_view(|window, cx| {
            let rem_size = window.rem_size();
            let item_count = menu_item_count(rem_size);
            let dropdown = cx.new(|_cx| {
                Dropdown::new(DROPDOWN_ID).items(
                    (0..item_count)
                        .map(|index| DropdownItem::new(format!("Row {index}")))
                        .collect(),
                )
            });
            Host {
                dropdown,
                item_count,
            }
        });
        window.run_until_parked();

        let (dropdown, item_count) =
            host.read_with(window, |host, _cx| (host.dropdown.clone(), host.item_count));
        window.update(|_, cx| dropdown.update(cx, |dropdown, cx| dropdown.open(cx)));
        window.run_until_parked();

        let row_height =
            window.update(|window, _| Fields::MENU_ROW_HEIGHT.to_pixels(window.rem_size()));
        Overflow {
            dropdown,
            item_count,
            row_height,
            window,
        }
    }

    #[gpui::test]
    fn a_wheel_over_the_rows_scrolls_the_menu_without_moving_the_highlight(
        cx: &mut TestAppContext,
    ) {
        let mut overflow = open_overflowing(cx);

        // The overflow is real before anything is asserted about scrolling:
        // the last row is drawn below the bottom of the clipped rows column.
        let item_count = overflow.item_count;
        let rows = overflow
            .window
            .debug_bounds(selector("-menu-rows"))
            .expect("the rows column is laid out");
        let last_row = overflow
            .window
            .debug_bounds(selector(&format!("-item-{}", overflow.item_count - 1)))
            .expect("the last row is laid out");
        assert!(
            last_row.bottom() > rows.bottom(),
            "the {item_count} rows overflow the {rows:?} menu: last {last_row:?}"
        );

        let before = overflow.rows_offset();
        assert_eq!(before, px(0.0), "an opened menu starts at its first row");
        let highlight = overflow.highlight();
        assert_eq!(highlight, Some(0));

        overflow.wheel();

        let after = overflow.rows_offset();
        assert!(
            after < before,
            "the wheel scrolled the rows down: {before} -> {after}"
        );
        assert_eq!(
            overflow.highlight(),
            Some(0),
            "the wheel did not move the highlight"
        );
    }
}
