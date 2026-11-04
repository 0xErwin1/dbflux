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
    let dropdown =
        Dropdown::new("ring-trigger").focus_ring_color(Some(gpui::black().opacity(0.25)));

    assert!(dropdown.focus_ring_color.is_some());
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
