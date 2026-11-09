//! The details pane after the editor moved to the secret's tab: its Edit
//! value hands over instead of editing in the side pane, the header holds no
//! write, no action is drawn twice, the description edit says what it edits,
//! and the pane resizes by its left edge.

use super::super::SecretOpenRequest;
use super::*;

/// A listing the way production mounts it: the pane hands editing to the tab.
fn production_pane<'a>(cx: &'a mut TestAppContext) -> Opened<'a> {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.update_view(|view, _| view.set_edit_in_place(false));
    opened
        .window
        .simulate_resize(gpui::size(gpui::px(1500.0), gpui::px(1400.0)));
    opened.window.run_until_parked();
    opened
}

impl Opened<'_> {
    fn drawn(&mut self, id: &str) -> bool {
        self.observe_frame()
            .nodes()
            .any(|(_, node)| node.id() == id)
    }

    fn bounds(&mut self, id: &str) -> gpui::Bounds<gpui::Pixels> {
        self.observe_frame()
            .nodes()
            .find(|(_, node)| node.id() == id)
            .map(|(_, node)| node.bounds())
            .unwrap_or_else(|| panic!("`{id}` is drawn"))
    }

    fn label_of(&mut self, id: &str) -> String {
        let frame = self.observe_frame();

        frame
            .nodes()
            .find(|(_, node)| node.id() == id)
            .map(|(_, node)| {
                let text = node.content_text().to_string();

                if text.is_empty() {
                    frame
                        .accessibility_node(node)
                        .and_then(|node| node.label().map(str::to_string))
                        .unwrap_or_default()
                } else {
                    text
                }
            })
            .unwrap_or_else(|| panic!("`{id}` is drawn"))
    }

    fn pending_open(&mut self) -> Option<SecretOpenRequest> {
        self.update(|doc, _| doc.take_pending_open_secret())
    }

    /// Drags from the middle of the grip to `x` and releases.
    fn drag_grip_to(&mut self, x: gpui::Pixels) {
        let start = self.bounds("secret-details-grip").center();
        let at = |x: gpui::Pixels| gpui::point(x, start.y);

        self.window.simulate_event(gpui::MouseDownEvent {
            position: start,
            modifiers: gpui::Modifiers::default(),
            button: gpui::MouseButton::Left,
            click_count: 1,
            first_mouse: false,
        });
        self.window.run_until_parked();
        // A drag starts once the pointer has left the press point.
        self.window.simulate_event(gpui::MouseMoveEvent {
            position: at(start.x - gpui::px(20.0)), // guardrail-allow: test geometry, not spacing
            pressed_button: Some(gpui::MouseButton::Left),
            modifiers: gpui::Modifiers::default(),
        });
        self.window.run_until_parked();
        self.window.simulate_event(gpui::MouseMoveEvent {
            position: at(x),
            pressed_button: Some(gpui::MouseButton::Left),
            modifiers: gpui::Modifiers::default(),
        });
        self.window.run_until_parked();
        self.window.simulate_event(gpui::MouseUpEvent {
            position: at(x),
            button: gpui::MouseButton::Left,
            modifiers: gpui::Modifiers::default(),
            click_count: 1,
        });
        self.window.run_until_parked();
    }
}

/// Edit value in the pane never edits in the side pane and never only
/// reveals: every way to ask for it hands the secret to its tab with the
/// editor requested, and nothing is read or written by the pane.
#[gpui::test]
fn edit_value_in_the_pane_hands_the_edit_to_the_secrets_tab(cx: &mut TestAppContext) {
    let mut opened = production_pane(cx);
    opened.open_secret(PLAINTEXT_SECRET);

    // The bar's button, the `e` key and the pane action.
    opened.click("secret-action-edit-value");
    let from_bar = opened.pending_open().expect("the bar hands the edit over");

    assert!(opened.command(Command::EditValue));
    let from_key = opened.pending_open().expect("`e` hands the edit over");

    let action = opened.pane_action("secret-edit-value");
    opened.run_action(action);
    let from_action = opened
        .pending_open()
        .expect("the pane action hands it over");

    for request in [from_bar, from_key, from_action] {
        assert_eq!(request.name, PLAINTEXT_SECRET);
        assert_eq!(request.store_id, FAKE_SECRETS_STORE_ID);
        assert_eq!(request.version, None);
        assert!(request.edit, "the editor is requested");
    }

    assert!(!opened.read_view(|view| view.is_editing_value()));
    assert!(!opened.drawn("secret-value-editor-save"));
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::NotLoaded)));
    assert!(opened.driver.secret_store_calls().value_requests.is_empty());
    assert!(opened.driver.secret_store_calls().put_attempts.is_empty());
}

/// The row menu's Edit value does the same, and the expand button opens the
/// tab without asking for the editor.
#[gpui::test]
fn the_row_menu_and_the_expand_button_open_the_tab(cx: &mut TestAppContext) {
    let mut opened = production_pane(cx);
    opened.open_secret(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    opened.right_click(&format!("secret-row-{PLAINTEXT_SECRET}"));
    let actions = opened.menu_actions();
    let index = actions
        .iter()
        .position(|action| action == "EditValue")
        .unwrap_or_else(|| panic!("the menu offers Edit value: {actions:?}"));
    opened.click_menu_item(&format!("secret-menu-edit-value-item-{index}"));

    let request = opened.pending_open().expect("the menu hands the edit over");
    assert!(request.edit);
    assert!(!opened.read_view(|view| view.is_editing_value()));

    opened.click("secret-details-expand");
    let request = opened.pending_open().expect("expand opens the tab");
    assert_eq!(request.name, PLAINTEXT_SECRET);
    assert!(!request.edit, "expand does not ask for the editor");
}

/// The header carries the name, Copy name, the open-in-tab control and Close,
/// and no write; each write is drawn once, in the bar at the bottom.
#[gpui::test]
fn no_action_is_drawn_twice_in_the_pane(cx: &mut TestAppContext) {
    let mut opened = production_pane(cx);
    opened.open_secret(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    for id in [
        "secret-details-name",
        "secret-details-copy-name",
        "secret-details-expand",
        "secret-details-close",
        "secret-action-edit-value",
        "secret-action-rotate-now",
        "secret-action-delete",
    ] {
        assert!(opened.drawn(id), "`{id}` is drawn");
    }
    for id in [
        "secret-details-delete",
        "secret-value-edit",
        "secret-rotation-rotate-now",
    ] {
        assert!(!opened.drawn(id), "`{id}` duplicates a bar action");
    }

    let mut labels = Vec::new();
    for id in [
        "secret-action-edit-value",
        "secret-action-rotate-now",
        "secret-action-delete",
        "secret-rotation-edit",
        "secret-tags-edit",
        "secret-details-edit",
    ] {
        if opened.drawn(id) {
            labels.push(opened.label_of(id));
        }
    }
    let mut unique = labels.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), labels.len(), "{labels:?}");
}

/// The description edit says what it changes, and so do the others.
#[gpui::test]
fn each_edit_is_labelled_by_what_it_changes(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    opened
        .window
        .simulate_resize(gpui::size(gpui::px(1500.0), gpui::px(1400.0)));
    opened.load_everything();
    opened.select("/app/prod/db/host");
    opened.window.run_until_parked();

    assert_eq!(
        opened.label_of("secret-action-edit-description"),
        "Edit description"
    );
    assert_eq!(opened.label_of("secret-tags-edit"), "Edit tags");

    let mut opened = production_pane(cx);
    opened.open_secret(KEY_VALUE_SECRET);
    assert_eq!(opened.label_of("secret-rotation-edit"), "Edit rotation");
    assert_eq!(opened.label_of("secret-details-edit"), "Edit description");
}

/// Dragging the pane's left edge resizes it, wider to the left, within bounds.
#[gpui::test]
fn the_details_pane_resizes_by_its_left_edge(cx: &mut TestAppContext) {
    let mut opened = production_pane(cx);
    opened.open_secret(KEY_VALUE_SECRET);

    let before = opened.bounds("secret-details");
    let grip = opened.bounds("secret-details-grip");
    assert!(
        (grip.right() - before.left()).abs() < gpui::px(1.0), // guardrail-allow: test geometry, not spacing
        "the gutter sits against the pane's left edge: {grip:?} / {before:?}"
    );

    opened.drag_grip_to(grip.center().x - gpui::px(100.0));
    let widened = opened.bounds("secret-details");
    assert_eq!(widened.size.width, before.size.width + gpui::px(100.0));

    opened.drag_grip_to(gpui::px(0.0));
    assert_eq!(opened.bounds("secret-details").size.width, gpui::px(900.0));

    opened.drag_grip_to(gpui::px(1499.0));
    assert_eq!(opened.bounds("secret-details").size.width, gpui::px(280.0));
}

/// The gutter has the keyboard path a resize needs: focused, Left widens the
/// pane and Right narrows it, by the splitter's step.
#[gpui::test]
fn the_gutter_resizes_with_the_arrow_keys(cx: &mut TestAppContext) {
    let mut opened = production_pane(cx);
    opened.open_secret(KEY_VALUE_SECRET);
    let before = opened.bounds("secret-details").size.width;

    let doc = opened.doc.clone();
    opened.window.update(|window, cx| {
        let view = doc.read(cx).view.clone();
        let handle = view.read(cx).details_grip_focus().clone();
        window.focus(&handle, cx);
    });
    opened.window.simulate_keystrokes("left");
    opened.window.run_until_parked();
    let wider = opened.bounds("secret-details").size.width;
    assert!(wider > before, "{before:?} -> {wider:?}");

    opened.window.simulate_keystrokes("right");
    opened.window.run_until_parked();
    assert_eq!(opened.bounds("secret-details").size.width, before);
}

/// #20: expanding a pane that shows an older version opens the tab on that
/// version, so retrieving there shows the value the pane showed.
#[gpui::test]
fn expanding_a_pane_that_shows_a_version_opens_the_tab_on_it(cx: &mut TestAppContext) {
    let mut opened = production_pane(cx);
    opened.open_secret(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.activate_version(1, cx));
    let shown = opened
        .read_view(|view| view.value_version().map(str::to_string))
        .expect("the pane shows an older version");

    opened.click("secret-details-expand");

    let request = opened.pending_open().expect("expand opens the tab");
    assert_eq!(request.version.as_deref(), Some(shown.as_str()));
    assert!(!request.edit);

    // The current version opens the current tab.
    opened.update_view(|view, cx| view.back_to_current_version(cx));
    opened.click("secret-details-expand");
    assert_eq!(opened.pending_open().expect("request").version, None);
}
