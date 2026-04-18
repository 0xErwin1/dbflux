use dbflux_components::composites::control_shell;
use dbflux_components::composites::{
    render_menu_container as render_shared_menu_container, render_menu_item,
};
use dbflux_components::controls::{Input, InputState};
use dbflux_components::primitives::focus_frame;
use dbflux_ui::ui::components::context_menu::{MenuItem, render_menu_container};
use dbflux_ui::ui::icons::AppIcon;
use dbflux_ui::ui::theme;
use gpui::prelude::*;
use gpui::{
    AnyElement, AppContext, Context, Entity, Modifiers, Render, TestAppContext, Window, div, px,
};
use gpui_component::Root;
use std::cell::RefCell;
use std::rc::Rc;

struct SharedContextMenuHarness {
    audit_items: Vec<MenuItem>,
    key_value_items: Vec<MenuItem>,
}

impl SharedContextMenuHarness {
    fn new() -> Self {
        Self {
            audit_items: vec![
                MenuItem::new("Copy Row as CSV").icon(AppIcon::Layers),
                MenuItem::new("Copy Summary").icon(AppIcon::Layers),
                MenuItem::separator(),
                MenuItem::new("Filter by Correlation").icon(AppIcon::ListFilter),
            ],
            key_value_items: vec![
                MenuItem::new("Copy Key").icon(AppIcon::Columns),
                MenuItem::new("Copy as Command").icon(AppIcon::Code),
                MenuItem::new("Rename").icon(AppIcon::Pencil),
                MenuItem::new("New Key").icon(AppIcon::Plus),
                MenuItem::new("Delete Key").icon(AppIcon::Delete).danger(),
            ],
        }
    }
}

impl Render for SharedContextMenuHarness {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let key_value_items: Vec<AnyElement> = self
            .key_value_items
            .iter()
            .enumerate()
            .map(|(idx, item)| {
                render_menu_item(
                    "kv-context-menu",
                    item,
                    idx,
                    idx == 4,
                    |_, _, _| {},
                    |_| {},
                    cx,
                )
                .into_any_element()
            })
            .collect();

        div()
            .size_full()
            .flex()
            .gap(px(24.0))
            .child(div().w(px(220.0)).child(render_menu_container(
                "audit-context-menu",
                &self.audit_items,
                Some(3),
                |_, _| {},
                |_, _| {},
                cx,
            )))
            .child(
                div().w(px(220.0)).child(
                    render_shared_menu_container(key_value_items, cx)
                        .debug_selector(|| "kv-context-menu-panel".to_string()),
                ),
            )
    }
}

struct FocusWrapperHarness {
    first_input: Entity<InputState>,
    second_input: Entity<InputState>,
}

impl FocusWrapperHarness {
    fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let first_input = cx.new(|cx| InputState::new(window, cx).placeholder("First"));
        let second_input = cx.new(|cx| InputState::new(window, cx).placeholder("Second"));

        Self {
            first_input,
            second_input,
        }
    }

    fn focus_first_input(&self, window: &mut Window, cx: &mut Context<Self>) {
        self.first_input
            .update(cx, |state, cx| state.focus(window, cx));
    }
}

impl Render for FocusWrapperHarness {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .flex()
            .flex_col()
            .gap(px(12.0))
            .child(
                div()
                    .id("first-shell")
                    .debug_selector(|| "first-shell".to_string())
                    .w(px(220.0))
                    .child(focus_frame(
                        false,
                        None,
                        control_shell(Input::new(&self.first_input).w_full(), cx),
                        cx,
                    )),
            )
            .child(
                div()
                    .id("second-shell")
                    .debug_selector(|| "second-shell".to_string())
                    .w(px(220.0))
                    .child(focus_frame(
                        false,
                        None,
                        control_shell(Input::new(&self.second_input).w_full(), cx),
                        cx,
                    )),
            )
    }
}

#[gpui::test]
fn audit_and_key_value_context_menus_share_runtime_panel_and_row_geometry(cx: &mut TestAppContext) {
    cx.update(theme::init);

    let (_, cx) = cx.add_window_view(|_, _| SharedContextMenuHarness::new());

    let audit_panel_bounds = cx
        .debug_bounds("audit-context-menu-panel")
        .expect("audit context menu should render");
    let key_value_panel_bounds = cx
        .debug_bounds("kv-context-menu-panel")
        .expect("key-value context menu should render");

    assert!(audit_panel_bounds.size.width > px(0.0));
    assert!(key_value_panel_bounds.size.width > px(0.0));
    assert!(audit_panel_bounds.size.height > px(0.0));
    assert!(key_value_panel_bounds.size.height > px(0.0));

    let audit_refresh_bounds = cx
        .debug_bounds("audit-context-menu-item-0")
        .expect("audit first row should render");
    let key_value_refresh_bounds = cx
        .debug_bounds("kv-context-menu-item-0")
        .expect("key-value first row should render");

    assert_eq!(
        audit_refresh_bounds.size.height,
        key_value_refresh_bounds.size.height
    );
    assert_eq!(
        audit_refresh_bounds.size.width,
        key_value_refresh_bounds.size.width
    );

    let audit_separator_bounds = cx
        .debug_bounds("audit-context-menu-separator-2")
        .expect("audit separator should render");

    assert!(audit_separator_bounds.size.height > px(0.0));
    assert!(audit_separator_bounds.size.width > px(0.0));
    assert!(audit_separator_bounds.size.width < audit_panel_bounds.size.width);

    let audit_delete_bounds = cx
        .debug_bounds("audit-context-menu-item-3")
        .expect("audit danger row should render");
    let key_value_delete_bounds = cx
        .debug_bounds("kv-context-menu-item-4")
        .expect("key-value danger row should render");

    assert_eq!(
        audit_delete_bounds.size.height,
        key_value_delete_bounds.size.height
    );
    assert_eq!(
        audit_delete_bounds.size.width,
        key_value_delete_bounds.size.width
    );
}

#[gpui::test]
fn wrapped_inputs_preserve_arrow_editing_and_tab_blur_transitions(cx: &mut TestAppContext) {
    cx.update(gpui_component::init);
    cx.update(theme::init);

    let harness = Rc::new(RefCell::new(None));
    let harness_handle = harness.clone();

    let (_, cx) = cx.add_window_view(|window, cx| {
        let focus_harness = cx.new(|cx| FocusWrapperHarness::new(window, cx));
        harness_handle.replace(Some(focus_harness.clone()));
        Root::new(focus_harness, window, cx)
    });
    let harness = harness
        .borrow()
        .clone()
        .expect("focus harness should exist");

    cx.update(|window, cx| {
        harness.update(cx, |this, cx| this.focus_first_input(window, cx));
    });

    cx.simulate_input("ab");
    cx.simulate_keystrokes("left left");
    cx.simulate_input("x");

    let first_value =
        cx.update(|_, app| harness.read(app).first_input.read(app).value().to_string());

    assert_eq!(first_value, "xab");

    cx.simulate_keystrokes("tab");
    cx.simulate_input("z");

    let first_value_after_tab =
        cx.update(|_, app| harness.read(app).first_input.read(app).value().to_string());
    let second_value =
        cx.update(|_, app| harness.read(app).second_input.read(app).value().to_string());

    assert_eq!(first_value_after_tab, "xab");
    assert_eq!(second_value, "z");
}

#[gpui::test]
fn clicking_between_wrapped_inputs_blurs_once_without_stealing_focus(cx: &mut TestAppContext) {
    cx.update(gpui_component::init);
    cx.update(theme::init);

    let harness = Rc::new(RefCell::new(None));
    let harness_handle = harness.clone();

    let (_, cx) = cx.add_window_view(|window, cx| {
        let focus_harness = cx.new(|cx| FocusWrapperHarness::new(window, cx));
        harness_handle.replace(Some(focus_harness.clone()));
        Root::new(focus_harness, window, cx)
    });
    let harness = harness
        .borrow()
        .clone()
        .expect("focus harness should exist");

    cx.update(|window, cx| {
        harness.update(cx, |this, cx| this.focus_first_input(window, cx));
    });

    cx.simulate_input("first");

    let second_shell_bounds = cx
        .debug_bounds("second-shell")
        .expect("second wrapped input should render");

    cx.simulate_click(second_shell_bounds.center(), Modifiers::none());
    cx.simulate_input("second");

    let first_value =
        cx.update(|_, app| harness.read(app).first_input.read(app).value().to_string());
    let second_value =
        cx.update(|_, app| harness.read(app).second_input.read(app).value().to_string());

    assert_eq!(first_value, "first");
    assert_eq!(second_value, "second");
}
