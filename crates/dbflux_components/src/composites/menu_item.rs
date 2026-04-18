use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::{ActiveTheme, IconName};

use crate::icon::IconSource;
use crate::primitives::{Icon, Text};
use crate::tokens::{FontSizes, Heights, Radii, Spacing};

pub(crate) const DEFAULT_MENU_CONTAINER_MIN_WIDTH: Pixels = px(160.0);

/// Purely visual menu item data type.
///
/// Consumers map clicked indices to their own action types.
/// Use builder methods to configure flags, then pass to
/// [`render_menu_item`] or [`render_menu_container`].
pub struct MenuItem {
    pub label: SharedString,
    pub icon: Option<IconSource>,
    pub is_separator: bool,
    pub is_danger: bool,
    pub has_submenu: bool,
    pub disabled: bool,
    pub shortcut: Option<SharedString>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MenuItemColorRole {
    Foreground,
    Muted,
    Danger,
    AccentForeground,
    AccentForegroundMuted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MenuItemBackgroundRole {
    Accent,
    DangerTint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MenuItemVisualState {
    text_color: MenuItemColorRole,
    icon_color: MenuItemColorRole,
    shortcut_color: MenuItemColorRole,
    submenu_color: MenuItemColorRole,
    background: Option<MenuItemBackgroundRole>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MenuItemInteractionState {
    interactive: bool,
    hoverable: bool,
    selected: bool,
}

fn menu_item_interaction_state(item: &MenuItem, is_selected: bool) -> MenuItemInteractionState {
    let interactive = !item.disabled && !item.is_separator;

    MenuItemInteractionState {
        interactive,
        hoverable: interactive && !is_selected,
        selected: interactive && is_selected,
    }
}

fn menu_item_visual_state(item: &MenuItem, is_selected: bool) -> MenuItemVisualState {
    if item.disabled {
        return MenuItemVisualState {
            text_color: MenuItemColorRole::Muted,
            icon_color: MenuItemColorRole::Muted,
            shortcut_color: MenuItemColorRole::Muted,
            submenu_color: MenuItemColorRole::Muted,
            background: None,
        };
    }

    let text_color = if item.is_danger {
        MenuItemColorRole::Danger
    } else if is_selected {
        MenuItemColorRole::AccentForeground
    } else {
        MenuItemColorRole::Foreground
    };

    let icon_color = if item.is_danger {
        MenuItemColorRole::Danger
    } else if is_selected {
        MenuItemColorRole::AccentForeground
    } else {
        MenuItemColorRole::Muted
    };

    let shortcut_color = if is_selected && !item.is_danger {
        MenuItemColorRole::AccentForegroundMuted
    } else {
        MenuItemColorRole::Muted
    };

    let submenu_color = if is_selected && !item.is_danger {
        MenuItemColorRole::AccentForeground
    } else {
        MenuItemColorRole::Muted
    };

    let background = if is_selected {
        Some(if item.is_danger {
            MenuItemBackgroundRole::DangerTint
        } else {
            MenuItemBackgroundRole::Accent
        })
    } else {
        None
    };

    MenuItemVisualState {
        text_color,
        icon_color,
        shortcut_color,
        submenu_color,
        background,
    }
}

fn resolve_menu_item_color(role: MenuItemColorRole, theme: &gpui_component::Theme) -> Hsla {
    match role {
        MenuItemColorRole::Foreground => theme.foreground,
        MenuItemColorRole::Muted => theme.muted_foreground,
        MenuItemColorRole::Danger => theme.danger,
        MenuItemColorRole::AccentForeground => theme.accent_foreground,
        MenuItemColorRole::AccentForegroundMuted => theme.accent_foreground.opacity(0.7),
    }
}

fn resolve_menu_item_background(
    role: MenuItemBackgroundRole,
    theme: &gpui_component::Theme,
) -> Hsla {
    match role {
        MenuItemBackgroundRole::Accent => theme.accent,
        MenuItemBackgroundRole::DangerTint => theme.danger.opacity(0.1),
    }
}

#[allow(dead_code)]
impl MenuItem {
    pub fn new(label: impl Into<SharedString>) -> Self {
        Self {
            label: label.into(),
            icon: None,
            is_separator: false,
            is_danger: false,
            has_submenu: false,
            disabled: false,
            shortcut: None,
        }
    }

    pub fn icon(mut self, icon: impl Into<IconSource>) -> Self {
        self.icon = Some(icon.into());
        self
    }

    pub fn danger(mut self) -> Self {
        self.is_danger = true;
        self
    }

    pub fn submenu(mut self) -> Self {
        self.has_submenu = true;
        self
    }

    pub fn disabled(mut self) -> Self {
        self.disabled = true;
        self
    }

    pub fn shortcut(mut self, shortcut: impl Into<SharedString>) -> Self {
        self.shortcut = Some(shortcut.into());
        self
    }

    pub fn separator() -> Self {
        Self {
            label: SharedString::default(),
            icon: None,
            is_separator: true,
            is_danger: false,
            has_submenu: false,
            disabled: false,
            shortcut: None,
        }
    }
}

/// Render a single menu item at a given index.
///
/// `panel_id` is used to construct a unique element ID (`{panel_id}-item-{index}`).
/// `is_selected` controls the highlight state.
/// `on_click` fires when the item is clicked.
/// `on_hover` fires when the mouse moves over the item.
#[allow(clippy::type_complexity)]
pub fn render_menu_item(
    panel_id: &str,
    item: &MenuItem,
    index: usize,
    is_selected: bool,
    on_click: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    on_hover: impl Fn(&mut App) + 'static,
    cx: &App,
) -> Stateful<Div> {
    let theme = cx.theme();

    let is_danger = item.is_danger;
    let has_submenu = item.has_submenu;
    let is_disabled = item.disabled;
    let label = item.label.clone();
    let icon = item.icon.clone();
    let shortcut = item.shortcut.clone();
    let interaction_state = menu_item_interaction_state(item, is_selected);
    let visual_state = menu_item_visual_state(item, interaction_state.selected);
    let icon_color = resolve_menu_item_color(visual_state.icon_color, theme);
    let text_color = resolve_menu_item_color(visual_state.text_color, theme);

    let item_id = SharedString::from(format!("{}-item-{}", panel_id, index));

    let has_no_icon = icon.is_none();
    let mut row = div()
        .id(item_id)
        .flex()
        .items_center()
        .gap(Spacing::SM)
        .h(Heights::ROW_COMPACT)
        .px(Spacing::SM)
        .mx(Spacing::XS)
        .rounded(Radii::SM)
        .text_size(FontSizes::SM)
        .text_color(text_color)
        .when(interaction_state.interactive, |d| d.cursor_pointer())
        .when(is_disabled, |d| d.opacity(0.6))
        .when_some(visual_state.background, |d, background| {
            d.bg(resolve_menu_item_background(background, theme))
        })
        .when(interaction_state.hoverable, |d| {
            let hover_bg = if is_danger {
                theme.danger.opacity(0.1)
            } else {
                theme.secondary
            };
            d.hover(move |d| d.bg(hover_bg))
        })
        .when(interaction_state.interactive, |d| {
            d.on_mouse_move(move |_, _, cx| {
                on_hover(cx);
            })
            .on_click(on_click)
        })
        .when_some(icon, |d, icon| {
            d.child(Icon::new(icon).small().color(icon_color))
        })
        .when(has_no_icon, |d| d.pl(px(20.0)))
        .child(
            div()
                .flex_1()
                .truncate()
                .child(Text::body(label).text_color(text_color)),
        );

    if let Some(sc) = shortcut {
        row = row.child(
            Text::caption(sc)
                .text_color(resolve_menu_item_color(visual_state.shortcut_color, theme)),
        );
    }

    if has_submenu {
        row = row.child(
            Icon::new(IconSource::Named(IconName::ChevronRight))
                .small()
                .color(resolve_menu_item_color(visual_state.submenu_color, theme)),
        );
    }

    row
}

/// Render a thin horizontal separator line.
pub fn render_separator(cx: &App) -> Div {
    let theme = cx.theme();

    div()
        .h(px(1.0))
        .mx(Spacing::SM)
        .my(Spacing::XS)
        .bg(theme.border)
}

/// Render the popup panel container for a menu.
pub fn render_menu_container(children: Vec<impl IntoElement>, cx: &App) -> Div {
    render_menu_container_with_min_width(children, DEFAULT_MENU_CONTAINER_MIN_WIDTH, cx)
}

/// Render the popup panel container for a menu with a caller-controlled minimum width.
pub fn render_menu_container_with_min_width(
    children: Vec<impl IntoElement>,
    min_width: Pixels,
    cx: &App,
) -> Div {
    let theme = cx.theme();

    div()
        .min_w(min_width)
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

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_MENU_CONTAINER_MIN_WIDTH, MenuItem, MenuItemBackgroundRole, MenuItemColorRole,
        menu_item_interaction_state, menu_item_visual_state,
    };
    use gpui::px;

    #[test]
    fn shortcut_builder_preserves_shortcut_text() {
        let item = MenuItem::new("Duplicate").shortcut("Ctrl+D");

        assert_eq!(
            item.shortcut.as_ref().map(|shortcut| shortcut.as_ref()),
            Some("Ctrl+D")
        );
    }

    #[test]
    fn submenu_builder_preserves_submenu_affordance() {
        let item = MenuItem::new("More").submenu();

        assert!(item.has_submenu);
    }

    #[test]
    fn selected_danger_item_uses_danger_visual_state() {
        let item = MenuItem::new("Delete").danger().shortcut("Del").submenu();
        let state = menu_item_visual_state(&item, true);

        assert_eq!(state.background, Some(MenuItemBackgroundRole::DangerTint));
        assert_eq!(state.text_color, MenuItemColorRole::Danger);
        assert_eq!(state.icon_color, MenuItemColorRole::Danger);
        assert_eq!(state.shortcut_color, MenuItemColorRole::Muted);
        assert_eq!(state.submenu_color, MenuItemColorRole::Muted);
    }

    #[test]
    fn selected_regular_item_uses_accent_visual_state() {
        let item = MenuItem::new("Open").shortcut("Enter").submenu();
        let state = menu_item_visual_state(&item, true);

        assert_eq!(state.background, Some(MenuItemBackgroundRole::Accent));
        assert_eq!(state.text_color, MenuItemColorRole::AccentForeground);
        assert_eq!(state.icon_color, MenuItemColorRole::AccentForeground);
        assert_eq!(
            state.shortcut_color,
            MenuItemColorRole::AccentForegroundMuted
        );
        assert_eq!(state.submenu_color, MenuItemColorRole::AccentForeground);
    }

    #[test]
    fn disabled_items_render_muted_and_unselected() {
        let item = MenuItem::new("Delete")
            .danger()
            .shortcut("Del")
            .submenu()
            .disabled();

        let state = menu_item_visual_state(&item, true);

        assert_eq!(state.background, None);
        assert_eq!(state.text_color, MenuItemColorRole::Muted);
        assert_eq!(state.icon_color, MenuItemColorRole::Muted);
        assert_eq!(state.shortcut_color, MenuItemColorRole::Muted);
        assert_eq!(state.submenu_color, MenuItemColorRole::Muted);
    }

    #[test]
    fn disabled_items_do_not_behave_as_active_items() {
        let item = MenuItem::new("Open").disabled();

        let state = menu_item_interaction_state(&item, true);

        assert!(!state.interactive);
        assert!(!state.hoverable);
        assert!(!state.selected);
    }

    #[test]
    fn default_menu_container_min_width_preserves_shared_baseline() {
        assert_eq!(DEFAULT_MENU_CONTAINER_MIN_WIDTH, px(160.0));
    }
}
