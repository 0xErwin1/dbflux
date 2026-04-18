pub use dbflux_components::composites::MenuItem;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{FontSizes, Heights, Radii, Spacing};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

use crate::ui::icons::AppIcon;

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
    cx: &App,
) -> Div {
    let theme = cx.theme();

    let on_click = std::rc::Rc::new(on_click);
    let on_hover = std::rc::Rc::new(on_hover);

    let children: Vec<AnyElement> = items
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            if item.is_separator {
                return render_separator(theme).into_any_element();
            }

            let on_click = on_click.clone();
            let on_hover = on_hover.clone();

            render_menu_item(
                panel_id,
                idx,
                item,
                selected_index == Some(idx),
                theme,
                move |cx| on_click(idx, cx),
                move |cx| on_hover(idx, cx),
            )
            .into_any_element()
        })
        .collect();

    div()
        .min_w(px(160.0))
        .bg(theme.popover)
        .border_1()
        .border_color(theme.border)
        .rounded(Radii::MD)
        .shadow_lg()
        .py(Spacing::XS)
        .on_mouse_down(MouseButton::Left, |_, _, cx| {
            cx.stop_propagation();
        })
        .on_mouse_down(MouseButton::Right, |_, _, cx| {
            cx.stop_propagation();
        })
        .children(children)
}

fn render_separator(theme: &gpui_component::Theme) -> Div {
    div()
        .h(px(1.0))
        .mx(Spacing::SM)
        .my(Spacing::XS)
        .bg(theme.border)
}

fn render_menu_item(
    panel_id: &str,
    idx: usize,
    item: &MenuItem,
    is_selected: bool,
    theme: &gpui_component::Theme,
    on_click: impl Fn(&mut App) + 'static,
    on_hover: impl Fn(&mut App) + 'static,
) -> Stateful<Div> {
    let is_danger = item.is_danger;
    let has_submenu = item.has_submenu;
    let icon = item.icon.clone();
    let label = item.label.clone();

    let fg = if is_danger {
        theme.danger
    } else {
        theme.foreground
    };

    let icon_color = if is_danger {
        theme.danger
    } else if is_selected {
        theme.accent_foreground
    } else {
        theme.muted_foreground
    };

    let item_id = SharedString::from(format!("{}-item-{}", panel_id, idx));

    let has_no_icon = icon.is_none();

    div()
        .id(item_id)
        .flex()
        .items_center()
        .gap(Spacing::SM)
        .h(Heights::ROW_COMPACT)
        .px(Spacing::SM)
        .mx(Spacing::XS)
        .rounded(Radii::SM)
        .cursor_pointer()
        .text_size(FontSizes::SM)
        .text_color(fg)
        .when(is_selected, |d| {
            d.bg(if is_danger {
                theme.danger.opacity(0.1)
            } else {
                theme.accent
            })
            .text_color(if is_danger {
                theme.danger
            } else {
                theme.accent_foreground
            })
        })
        .when(!is_selected, |d| {
            let hover_bg = if is_danger {
                theme.danger.opacity(0.1)
            } else {
                theme.secondary
            };
            d.hover(move |d| d.bg(hover_bg))
        })
        .on_mouse_move(move |_, _, cx| {
            on_hover(cx);
        })
        .on_click(move |_, _, cx| {
            on_click(cx);
        })
        .when_some(icon, move |d, icon| {
            d.child(Icon::new(icon).small().color(icon_color))
        })
        .when(has_no_icon, |d| d.pl(px(20.0)))
        .child(
            div()
                .flex_1()
                .truncate()
                .child(Text::body(label).text_color(fg)),
        )
        .when(has_submenu, |d| {
            d.child(
                Icon::new(AppIcon::ChevronRight)
                    .small()
                    .color(if is_selected {
                        theme.accent_foreground
                    } else {
                        theme.muted_foreground
                    }),
            )
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
