pub use dbflux_components::composites::MenuItem;

use dbflux_components::composites::{
    render_menu_container as render_components_menu_container,
    render_menu_item as render_components_menu_item,
    render_separator as render_components_separator,
};
use gpui::*;

/// Full-screen transparent overlay that dismisses the menu on any click outside.
pub fn render_menu_overlay(
    id: impl Into<ElementId>,
    on_dismiss: impl Fn(&MouseDownEvent, &mut App) + 'static,
) -> Stateful<Div> {
    let on_dismiss = std::rc::Rc::new(on_dismiss);
    let on_dismiss_right = on_dismiss.clone();

    div()
        .id(id)
        .absolute()
        .top_0()
        .left_0()
        .size_full()
        .on_mouse_down(MouseButton::Left, move |event, _, cx| {
            on_dismiss(event, cx);
        })
        .on_mouse_down(MouseButton::Right, move |event, _, cx| {
            on_dismiss_right(event, cx);
        })
}

pub fn render_menu_container(
    panel_id: &str,
    items: &[MenuItem],
    selected_index: Option<usize>,
    on_click: impl Fn(usize, &mut App) + 'static,
    on_hover: impl Fn(usize, &mut App) + 'static,
    cx: &mut App,
) -> Div {
    let on_click = std::rc::Rc::new(on_click);
    let on_hover = std::rc::Rc::new(on_hover);

    let children: Vec<AnyElement> = items
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            if item.is_separator {
                return render_components_separator(cx).into_any_element();
            }

            let on_click = on_click.clone();
            let on_hover = on_hover.clone();

            render_components_menu_item(
                panel_id,
                item,
                idx,
                selected_index == Some(idx),
                move |_, _, cx| on_click(idx, cx),
                move |cx| on_hover(idx, cx),
                cx,
            )
            .into_any_element()
        })
        .collect();

    render_components_menu_container(children, cx)
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
