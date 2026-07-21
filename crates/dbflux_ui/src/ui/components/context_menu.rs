//! UI-layer wrapper around the shared menu-popup helpers.
//!
//! The actual implementation lives in `dbflux_components::composites::menu_popup`
//! so that document and sidebar crates (which cannot depend on `dbflux_ui`) can
//! render the same chrome. This module preserves the historical `dbflux_ui`
//! call sites by re-exporting under the original names.

pub use dbflux_components::composites::MenuItem;
use dbflux_components::composites::render_menu_items;
pub use dbflux_components::composites::render_menu_overlay;
use gpui::*;

/// Render the floating menu container for a slice of [`MenuItem`]s.
///
/// Thin alias kept for historical call sites. New code should call
/// `dbflux_components::composites::render_menu_items` directly.
pub fn render_menu_container(
    panel_id: &str,
    items: &[MenuItem],
    selected_index: Option<usize>,
    on_click: impl Fn(usize, &mut App) + 'static,
    on_hover: impl Fn(usize, &mut App) + 'static,
    cx: &mut App,
) -> Div {
    render_menu_items(panel_id, items, selected_index, on_click, on_hover, cx).debug_selector({
        let panel_id = panel_id.to_string();
        move || format!("{panel_id}-panel")
    })
}

#[cfg(test)]
mod tests {
    use super::MenuItem;
    use crate::ui::icons::AppIcon;

    #[test]
    fn new_item_has_default_flags() {
        let item = MenuItem::new("Test");
        assert_eq!(item.label.as_ref(), "Test");
        assert!(item.icon.is_none());
        assert!(!item.is_separator);
        assert!(!item.is_danger);
        assert!(!item.has_submenu);
    }

    #[test]
    fn icon_builder_sets_icon() {
        let item = MenuItem::new("Edit").icon(AppIcon::Pencil);
        assert!(item.icon.is_some());
    }

    #[test]
    fn danger_builder_sets_flag() {
        let item = MenuItem::new("Delete").danger();
        assert!(item.is_danger);
        assert!(!item.has_submenu);
    }

    #[test]
    fn submenu_builder_sets_flag() {
        let item = MenuItem::new("More").submenu();
        assert!(item.has_submenu);
        assert!(!item.is_danger);
    }

    #[test]
    fn separator_has_empty_label_and_no_icon() {
        let item = MenuItem::separator();
        assert!(item.is_separator);
        assert!(item.label.is_empty());
        assert!(item.icon.is_none());
        assert!(!item.is_danger);
        assert!(!item.has_submenu);
    }

    #[test]
    fn builders_can_be_chained() {
        let item = MenuItem::new("Dangerous Sub")
            .icon(AppIcon::Delete)
            .danger()
            .submenu();

        assert_eq!(item.label.as_ref(), "Dangerous Sub");
        assert!(item.icon.is_some());
        assert!(item.is_danger);
        assert!(item.has_submenu);
    }
}
