use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::{ActiveTheme, IconName};

use crate::icon::IconSource;
use crate::primitives::{Icon, Text};
use crate::tokens::{FontSizes, Heights, Radii, Spacing};

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
    cx: &mut App,
) -> Stateful<Div> {
    let theme = cx.theme();

    let is_danger = item.is_danger;
    let has_submenu = item.has_submenu;
    let label = item.label.clone();
    let icon = item.icon.clone();
    let shortcut = item.shortcut.clone();

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

    let text_color = if is_selected && !is_danger {
        theme.accent_foreground
    } else {
        fg
    };

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
        .cursor_pointer()
        .text_size(FontSizes::SM)
        .text_color(text_color)
        .when(is_selected, |d| {
            d.bg(if is_danger {
                theme.danger.opacity(0.1)
            } else {
                theme.accent
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
        .on_click(on_click)
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
        row = row.child(Text::caption(sc).text_color(if is_selected && !is_danger {
            theme.accent_foreground.opacity(0.7)
        } else {
            theme.muted_foreground
        }));
    }

    if has_submenu {
        row = row.child(
            Icon::new(IconSource::Named(IconName::ChevronRight))
                .small()
                .color(if is_selected && !is_danger {
                    theme.accent_foreground
                } else {
                    theme.muted_foreground
                }),
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
    let theme = cx.theme();

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
