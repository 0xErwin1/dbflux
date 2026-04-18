use dbflux_components::tokens::Heights;
use dbflux_ui::app_state_entity::AppStateEntity;
use dbflux_ui::ui::document::{AuditDocument, KeyValueDocument};
use dbflux_ui::ui::theme;
use gpui::prelude::*;
use gpui::{AppContext, Context, Entity, Modifiers, Render, TestAppContext, Window, div, px};
use uuid::Uuid;

fn dropdown_menu_selector(id: &str) -> String {
    format!("{id}-menu")
}

fn assert_pixels_close(actual: gpui::Pixels, expected: gpui::Pixels, message: &str) {
    assert!(
        actual >= expected - px(1.0) && actual <= expected + px(1.0),
        "{message}: expected {expected:?}, got {actual:?}"
    );
}

struct ProductionRefreshDropdownHarness {
    audit_document: Entity<AuditDocument>,
    key_value_document: Entity<KeyValueDocument>,
}

impl ProductionRefreshDropdownHarness {
    fn new(app_state: Entity<AppStateEntity>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let audit_document = cx.new(|cx| AuditDocument::new(app_state.clone(), window, cx));
        let key_value_document = cx.new(|cx| {
            KeyValueDocument::new(Uuid::nil(), "0".to_string(), app_state.clone(), window, cx)
        });

        Self {
            audit_document,
            key_value_document,
        }
    }
}

impl Render for ProductionRefreshDropdownHarness {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .flex()
            .flex_col()
            .gap(px(16.0))
            .child(div().h(px(320.0)).child(self.audit_document.clone()))
            .child(div().h(px(320.0)).child(self.key_value_document.clone()))
    }
}

#[gpui::test]
fn audit_and_key_value_refresh_dropdowns_share_compact_trigger_geometry(cx: &mut TestAppContext) {
    cx.update(theme::init);

    let app_state = cx.update(|cx| cx.new(|_| AppStateEntity::new()));

    let (_, window) = cx
        .add_window_view(|window, cx| ProductionRefreshDropdownHarness::new(app_state, window, cx));

    for menu_selector in ["audit-auto-refresh-menu", "kv-auto-refresh-menu"] {
        assert!(
            window.debug_bounds(menu_selector).is_none(),
            "{menu_selector} should not render before opening"
        );
    }

    let audit_trigger_bounds = window
        .debug_bounds("audit-auto-refresh")
        .expect("audit refresh dropdown should render");

    let kv_trigger_bounds = window
        .debug_bounds("kv-auto-refresh")
        .expect("key-value refresh dropdown should render");

    assert_eq!(audit_trigger_bounds.size.width, px(28.0));
    assert_eq!(kv_trigger_bounds.size.width, px(28.0));
    assert!(audit_trigger_bounds.size.height > px(0.0));
    assert!(kv_trigger_bounds.size.height > px(0.0));
    assert!(audit_trigger_bounds.size.height <= Heights::BUTTON);
    assert!(kv_trigger_bounds.size.height <= Heights::BUTTON);
}

#[gpui::test]
fn audit_and_key_value_refresh_dropdowns_share_menu_render_bounds(cx: &mut TestAppContext) {
    cx.update(theme::init);

    let app_state = cx.update(|cx| cx.new(|_| AppStateEntity::new()));

    let (_, window) = cx
        .add_window_view(|window, cx| ProductionRefreshDropdownHarness::new(app_state, window, cx));

    for menu_selector in ["audit-auto-refresh-menu", "kv-auto-refresh-menu"] {
        assert!(
            window.debug_bounds(menu_selector).is_none(),
            "{menu_selector} should not render before opening"
        );
    }

    let audit_trigger_bounds = window
        .debug_bounds("audit-auto-refresh")
        .expect("audit refresh dropdown should render");

    let kv_trigger_bounds = window
        .debug_bounds("kv-auto-refresh")
        .expect("key-value refresh dropdown should render");

    window.simulate_click(audit_trigger_bounds.center(), Modifiers::none());

    let audit_menu_bounds = window
        .debug_bounds("audit-auto-refresh-menu")
        .expect("audit refresh menu should open");

    window.simulate_click(kv_trigger_bounds.center(), Modifiers::none());

    let kv_menu_bounds = window
        .debug_bounds("kv-auto-refresh-menu")
        .expect("key-value refresh menu should open");

    assert_eq!(
        dropdown_menu_selector("audit-auto-refresh"),
        "audit-auto-refresh-menu"
    );
    assert_eq!(
        dropdown_menu_selector("kv-auto-refresh"),
        "kv-auto-refresh-menu"
    );

    assert_eq!(audit_menu_bounds.size.width, kv_menu_bounds.size.width);
    assert!(audit_menu_bounds.size.width >= audit_trigger_bounds.size.width);
    assert!(kv_menu_bounds.size.width >= kv_trigger_bounds.size.width);
    assert_eq!(audit_menu_bounds.origin.x, audit_trigger_bounds.origin.x);
    assert_eq!(kv_menu_bounds.origin.x, kv_trigger_bounds.origin.x);
    assert_pixels_close(
        audit_menu_bounds.origin.y,
        audit_trigger_bounds.origin.y + audit_trigger_bounds.size.height + px(4.0),
        "audit menu should render below its trigger",
    );
    assert_pixels_close(
        kv_menu_bounds.origin.y,
        kv_trigger_bounds.origin.y + kv_trigger_bounds.size.height + px(4.0),
        "key-value menu should render below its trigger",
    );

    assert!(audit_menu_bounds.size.width >= px(28.0));
    assert!(audit_menu_bounds.size.height >= px(200.0));
    assert!(kv_menu_bounds.size.height >= px(200.0));
}
