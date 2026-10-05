use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::{ActiveTheme, IconName};

use crate::icon::IconSource;
use crate::primitives::{Chamfer, Icon, key_label};
use crate::tokens::{
    Borders, ChamferCut, ChromeColorSlot, ChromeColors, ChromeEdgeRole, KbdMetrics, MenuMetrics,
};

pub(crate) const DEFAULT_MENU_CONTAINER_MIN_WIDTH: Pixels = px(160.0);

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct MenuChromeInspection {
    pub container_background: ChromeColorSlot,
    pub container_edge: ChromeEdgeRole,
    pub cut: Pixels,
    pub separator_edge: ChromeEdgeRole,
}

/// Menu frame roles: raised fill, line-2 border, overlay cut, line separators.
pub(crate) fn inspect_menu_chrome() -> MenuChromeInspection {
    MenuChromeInspection {
        container_background: ChromeColorSlot::Secondary,
        container_edge: ChromeEdgeRole::Control,
        cut: ChamferCut::OVERLAY,
        separator_edge: ChromeEdgeRole::Popover,
    }
}

/// Purely visual menu item data type.
///
/// Consumers map clicked indices to their own action types.
/// Use builder methods to configure flags, then pass to
/// [`render_menu_item`] or [`render_menu_container`].
pub struct MenuItem {
    pub label: SharedString,
    pub icon: Option<IconSource>,
    pub is_separator: bool,
    /// A non-interactive caption row naming what the menu acts on.
    pub is_header: bool,
    /// Color of a header row's icon; muted when `None`.
    pub header_icon_color: Option<Hsla>,
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
    Strong,
    Tint,
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
    submenu_color: MenuItemColorRole,
    /// The shortcut hint is plain muted text on every row, selected or not.
    shortcut_color: MenuItemColorRole,
    background: Option<MenuItemBackgroundRole>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MenuItemInteractionState {
    interactive: bool,
    hoverable: bool,
    selected: bool,
}

fn menu_item_interaction_state(item: &MenuItem, is_selected: bool) -> MenuItemInteractionState {
    let interactive = !item.disabled && !item.is_separator && !item.is_header;

    MenuItemInteractionState {
        interactive,
        hoverable: interactive && !is_selected,
        selected: interactive && is_selected,
    }
}

fn menu_item_visual_state(item: &MenuItem, is_selected: bool) -> MenuItemVisualState {
    if item.disabled {
        return MenuItemVisualState {
            text_color: MenuItemColorRole::Foreground,
            icon_color: MenuItemColorRole::Muted,
            submenu_color: MenuItemColorRole::Muted,
            shortcut_color: MenuItemColorRole::Muted,
            background: None,
        };
    }

    if item.is_danger {
        return MenuItemVisualState {
            text_color: MenuItemColorRole::Danger,
            icon_color: MenuItemColorRole::Danger,
            submenu_color: MenuItemColorRole::Danger,
            shortcut_color: MenuItemColorRole::Muted,
            background: is_selected.then_some(MenuItemBackgroundRole::DangerTint),
        };
    }

    if is_selected {
        return MenuItemVisualState {
            text_color: MenuItemColorRole::Strong,
            icon_color: MenuItemColorRole::Tint,
            submenu_color: MenuItemColorRole::Tint,
            shortcut_color: MenuItemColorRole::Muted,
            background: Some(MenuItemBackgroundRole::Accent),
        };
    }

    MenuItemVisualState {
        text_color: MenuItemColorRole::Foreground,
        icon_color: MenuItemColorRole::Muted,
        submenu_color: MenuItemColorRole::Muted,
        shortcut_color: MenuItemColorRole::Muted,
        background: None,
    }
}

fn resolve_menu_item_color(role: MenuItemColorRole, theme: &gpui_component::Theme) -> Hsla {
    match role {
        MenuItemColorRole::Foreground => theme.foreground,
        MenuItemColorRole::Muted => theme.muted_foreground,
        MenuItemColorRole::Danger => theme.danger,
        MenuItemColorRole::Strong => ChromeColors::strong(theme),
        MenuItemColorRole::Tint => ChromeColors::tint(theme),
    }
}

fn resolve_menu_item_background(
    role: MenuItemBackgroundRole,
    theme: &gpui_component::Theme,
) -> Hsla {
    match role {
        MenuItemBackgroundRole::Accent => {
            ChromeColors::tint(theme).opacity(MenuMetrics::SELECTED_ALPHA)
        }
        MenuItemBackgroundRole::DangerTint => theme.danger.opacity(MenuMetrics::SELECTED_ALPHA),
    }
}

#[allow(dead_code)]
impl MenuItem {
    pub fn new(label: impl Into<SharedString>) -> Self {
        Self {
            label: label.into(),
            icon: None,
            is_separator: false,
            is_header: false,
            header_icon_color: None,
            is_danger: false,
            has_submenu: false,
            disabled: false,
            shortcut: None,
        }
    }

    /// A caption row at the top of a menu, for example the column and row a
    /// cell menu acts on. Not selectable.
    pub fn header(label: impl Into<SharedString>) -> Self {
        Self {
            is_header: true,
            ..Self::new(label)
        }
    }

    pub fn icon(mut self, icon: impl Into<IconSource>) -> Self {
        self.icon = Some(icon.into());
        self
    }

    /// Color of a header row's icon, for example the FK color when the menu
    /// acts on a foreign-key column.
    pub fn header_icon_color(mut self, color: Hsla) -> Self {
        self.header_icon_color = Some(color);
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
            is_separator: true,
            ..Self::new(SharedString::default())
        }
    }
}

/// Visual row of a menu, without handlers: icon, label, then the shortcut as
/// plain muted mono text or the submenu chevron (IslMenu, DSApp "Context
/// menu"). The hint stays plain on the highlighted row too.
///
/// Hand-rolled menus that need their own listeners or a flyout child start
/// from this and chain `on_click` / `on_mouse_move` / `child` onto it, so
/// every row shares the same geometry and states. The selected row gets a
/// cut-4 tint wash (danger wash for danger rows); unselected rows show the
/// same wash while hovered.
pub fn menu_row(
    id: impl Into<ElementId>,
    item: &MenuItem,
    is_selected: bool,
    cx: &App,
) -> Stateful<Div> {
    let theme = cx.theme();

    let interaction_state = menu_item_interaction_state(item, is_selected);
    let visual_state = menu_item_visual_state(item, interaction_state.selected);
    let icon_color = resolve_menu_item_color(visual_state.icon_color, theme);
    let text_color = resolve_menu_item_color(visual_state.text_color, theme);
    let submenu_color = resolve_menu_item_color(visual_state.submenu_color, theme);
    let shortcut_color = resolve_menu_item_color(visual_state.shortcut_color, theme);

    let wash = if item.is_danger {
        resolve_menu_item_background(MenuItemBackgroundRole::DangerTint, theme)
    } else {
        resolve_menu_item_background(MenuItemBackgroundRole::Accent, theme)
    };

    let mut shape = Chamfer::new(ChamferCut::KEYCAP);

    if let Some(background) = visual_state.background {
        shape = shape.fill(resolve_menu_item_background(background, theme));
    } else if interaction_state.hoverable {
        shape = shape.fill_hover(wash).interactive("menu-row-shape");
    }

    div()
        .id(id)
        .relative()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(MenuMetrics::ROW_GAP)
        .h(MenuMetrics::ROW_HEIGHT)
        .mx(MenuMetrics::ROW_INSET)
        .px(MenuMetrics::ROW_PADDING_X)
        .font_family(crate::fonts::ui_family(cx))
        .text_size(MenuMetrics::ROW_FONT)
        .whitespace_nowrap()
        .text_color(text_color)
        .when(interaction_state.interactive, |row| row.cursor_pointer())
        .when(item.disabled, |row| {
            row.opacity(MenuMetrics::DISABLED_OPACITY)
        })
        .child(shape)
        .child(match item.icon.clone() {
            Some(icon) => Icon::new(icon)
                .size(MenuMetrics::ICON)
                .color(icon_color)
                .into_any_element(),
            None => div()
                .flex_shrink_0()
                .size(MenuMetrics::ICON)
                .into_any_element(),
        })
        .child(
            div()
                .flex_1()
                .min_w_0()
                .truncate()
                .child(item.label.clone()),
        )
        .when_some(item.shortcut.as_ref(), |row, shortcut| {
            row.child(
                div()
                    .flex_shrink_0()
                    .font_family(crate::fonts::editor_family(cx))
                    .text_size(KbdMetrics::FONT)
                    .text_color(shortcut_color)
                    .child(key_label(shortcut)),
            )
        })
        .when(item.has_submenu, |row| {
            row.child(
                Icon::new(IconSource::Named(IconName::ChevronRight))
                    .size(MenuMetrics::SUBMENU_ICON)
                    .color(submenu_color),
            )
        })
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
    let interaction_state = menu_item_interaction_state(item, is_selected);

    let item_selector = format!("{}-item-{}", panel_id, index);
    let item_id = SharedString::from(item_selector.clone());

    menu_row(item_id, item, is_selected, cx)
        .debug_selector(move || item_selector.clone())
        .when(interaction_state.interactive, |row| {
            row.on_mouse_move(move |_, _, cx| {
                on_hover(cx);
            })
            .on_click(on_click)
        })
}

/// Render a menu header row: an optional icon and a short mono caption naming
/// what the menu acts on (`workspace_id · row 2`).
pub fn render_menu_header(item: &MenuItem, cx: &App) -> Div {
    let theme = cx.theme();

    div()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(MenuMetrics::HEADER_GAP)
        .pt(MenuMetrics::HEADER_PADDING_TOP)
        .pb(MenuMetrics::HEADER_PADDING_BOTTOM)
        .px(MenuMetrics::HEADER_PADDING_X)
        .font_family(crate::fonts::editor_family(cx))
        .text_size(MenuMetrics::HEADER_FONT)
        .text_color(theme.muted_foreground)
        .whitespace_nowrap()
        .when_some(item.icon.clone(), |header, icon| {
            header.child(
                Icon::new(icon)
                    .size(MenuMetrics::HEADER_ICON)
                    .color(item.header_icon_color.unwrap_or(theme.muted_foreground)),
            )
        })
        .child(div().min_w_0().truncate().child(item.label.clone()))
}

/// Render a thin horizontal separator line.
pub fn render_separator(cx: &App) -> Div {
    let theme = cx.theme();
    let chrome = inspect_menu_chrome();

    div()
        .flex_shrink_0()
        .h(Borders::THIN)
        .mx(MenuMetrics::SEPARATOR_MARGIN_X)
        .my(MenuMetrics::SEPARATOR_MARGIN_Y)
        .bg(chrome.separator_edge.resolve(theme))
}

/// The frame of a floating menu: raised fill, line-2 border, cut 12, deep
/// shadow and the menu's vertical padding. Stops mouse-down propagation so a
/// click inside never reaches the dismiss overlay behind it.
///
/// Hand-rolled menus and submenu flyouts use this so they share the frame of
/// [`render_menu_container`]; size it with `.w(...)` or `.min_w(...)`.
pub fn menu_frame(cx: &App) -> Div {
    let theme = cx.theme();
    let chrome = inspect_menu_chrome();

    div()
        .relative()
        .flex()
        .flex_col()
        .py(MenuMetrics::PADDING_Y)
        .shadow_lg()
        .on_mouse_down(MouseButton::Left, |_, _, cx| {
            cx.stop_propagation();
        })
        .on_mouse_down(MouseButton::Right, |_, _, cx| {
            cx.stop_propagation();
        })
        .child(
            Chamfer::new(chrome.cut)
                .fill(chrome.container_background.resolve(theme))
                .border(chrome.container_edge.resolve(theme)),
        )
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
    menu_frame(cx).min_w(min_width).children(children)
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_MENU_CONTAINER_MIN_WIDTH, MenuItem, MenuItemBackgroundRole, MenuItemColorRole,
        inspect_menu_chrome, menu_item_interaction_state, menu_item_visual_state,
    };
    use crate::tokens::{ChamferCut, ChromeColorSlot, ChromeEdgeRole};
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
        assert_eq!(state.submenu_color, MenuItemColorRole::Danger);
    }

    #[test]
    fn unselected_danger_item_keeps_danger_text_without_wash() {
        let state = menu_item_visual_state(&MenuItem::new("Delete Row").danger(), false);

        assert_eq!(state.background, None);
        assert_eq!(state.text_color, MenuItemColorRole::Danger);
        assert_eq!(state.icon_color, MenuItemColorRole::Danger);
    }

    #[test]
    fn selected_regular_item_uses_tint_wash_strong_text_and_tint_icon() {
        let item = MenuItem::new("Open").shortcut("Enter").submenu();
        let state = menu_item_visual_state(&item, true);

        assert_eq!(state.background, Some(MenuItemBackgroundRole::Accent));
        assert_eq!(state.text_color, MenuItemColorRole::Strong);
        assert_eq!(state.icon_color, MenuItemColorRole::Tint);
        assert_eq!(state.submenu_color, MenuItemColorRole::Tint);
    }

    #[test]
    fn shortcut_hint_stays_plain_muted_text_on_every_row() {
        let item = MenuItem::new("Copy").shortcut("Ctrl C");
        let danger = MenuItem::new("Drop table").danger().shortcut("x");

        for selected in [false, true] {
            assert_eq!(
                menu_item_visual_state(&item, selected).shortcut_color,
                MenuItemColorRole::Muted
            );
            assert_eq!(
                menu_item_visual_state(&danger, selected).shortcut_color,
                MenuItemColorRole::Muted
            );
        }
    }

    #[test]
    fn unselected_regular_item_uses_body_text_and_muted_icon() {
        let state = menu_item_visual_state(&MenuItem::new("Copy"), false);

        assert_eq!(state.background, None);
        assert_eq!(state.text_color, MenuItemColorRole::Foreground);
        assert_eq!(state.icon_color, MenuItemColorRole::Muted);
    }

    #[test]
    fn disabled_items_render_unselected() {
        let item = MenuItem::new("Delete")
            .danger()
            .shortcut("Del")
            .submenu()
            .disabled();

        let state = menu_item_visual_state(&item, true);

        assert_eq!(state.background, None);
        assert_eq!(state.icon_color, MenuItemColorRole::Muted);
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
    fn header_items_are_not_interactive() {
        let item = MenuItem::header("workspace_id · row 2");

        assert!(item.is_header);
        assert!(!menu_item_interaction_state(&item, true).interactive);
    }

    #[test]
    fn default_menu_container_min_width_preserves_shared_baseline() {
        assert_eq!(DEFAULT_MENU_CONTAINER_MIN_WIDTH, px(160.0));
    }

    #[test]
    fn menu_chrome_uses_raised_fill_line_two_edge_and_overlay_cut() {
        let chrome = inspect_menu_chrome();

        assert_eq!(chrome.container_background, ChromeColorSlot::Secondary);
        assert_eq!(chrome.container_edge, ChromeEdgeRole::Control);
        assert_eq!(chrome.cut, ChamferCut::OVERLAY);
        assert_eq!(chrome.separator_edge, ChromeEdgeRole::Popover);
    }
}
