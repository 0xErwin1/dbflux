//! Controls of the application title bar: the command search trigger and the
//! notification bell.

use gpui::prelude::*;
use gpui::{
    App, ClickEvent, Div, ElementId, FocusHandle, FontWeight, Hsla, KeyDownEvent, SharedString,
    Stateful, Window, div,
};
use gpui_component::tooltip::Tooltip;
use gpui_component::{ActiveTheme, IconName};

use crate::controls::is_activation_key;
use crate::icon::IconSource;
use crate::primitives::{Chamfer, ChamferRing, Icon, Kbd};
use crate::tokens::{ButtonMetrics, ChamferCut, ChromeColors, NotificationMetrics, ShellMetrics};

/// Key context the command search field sets on itself.
pub const COMMAND_SEARCH_KEY_CONTEXT: &str = "CommandSearch";

/// The icons a product draws in its title bar. Install it once at startup;
/// without it the field and the bell use gpui-component's search and bell.
#[derive(Clone, Debug)]
pub struct ShellIcons {
    pub search: IconSource,
    pub bell: IconSource,
}

impl gpui::Global for ShellIcons {}

impl Default for ShellIcons {
    fn default() -> Self {
        Self {
            search: IconName::Search.into(),
            bell: IconName::Bell.into(),
        }
    }
}

impl ShellIcons {
    /// The installed icons, or the defaults when the product installed none.
    pub fn current(cx: &App) -> Self {
        cx.try_global::<Self>().cloned().unwrap_or_default()
    }
}

/// A count badge over the top-right corner of a shell button (the bell, a
/// rail button): `colors` are the fill, text and optional edge.
pub(crate) fn count_badge(
    id: impl Into<ElementId>,
    text: SharedString,
    (fill, text_color, edge): (Hsla, Hsla, Option<Hsla>),
) -> Stateful<Div> {
    let id = id.into();
    let mut badge_shape = Chamfer::new(ChamferCut::KEYCAP).fill(fill);
    if let Some(edge) = edge {
        badge_shape = badge_shape.border(edge);
    }

    div()
        .debug_selector(crate::selector::id_selector(&id))
        .id(id)
        .absolute()
        .top(ShellMetrics::BELL_BADGE_OFFSET)
        .right(ShellMetrics::BELL_BADGE_OFFSET)
        .flex()
        .items_center()
        .justify_center()
        .min_w(ShellMetrics::BELL_BADGE_MIN_WIDTH)
        .h(ShellMetrics::BELL_BADGE_HEIGHT)
        .px(ShellMetrics::BELL_BADGE_PADDING_X)
        .text_size(ShellMetrics::BELL_BADGE_FONT)
        .font_weight(FontWeight::BOLD)
        .text_color(text_color)
        .child(badge_shape)
        .child(div().relative().child(text))
}

type ClickHandler = Box<dyn Fn(&ClickEvent, &mut Window, &mut App) + 'static>;
type ActivateHandler = Box<dyn Fn(&mut Window, &mut App) + 'static>;

/// The "Search or run a command" field of the title bar.
///
/// It is a trigger, not an editor: a click, or Enter or Space while it has
/// focus, runs `on_open`, which opens the command palette. Layout: 420 by
/// 30 px, cut 6, the well fill (window ground) with a line border, search
/// icon, the placeholder, and the palette's chord as one keycap on the right.
#[derive(IntoElement)]
pub struct CommandSearch {
    id: ElementId,
    placeholder: SharedString,
    shortcut: Option<SharedString>,
    focus_handle: Option<FocusHandle>,
    on_open: Option<ActivateHandler>,
}

impl CommandSearch {
    pub fn new(id: impl Into<ElementId>, placeholder: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            placeholder: placeholder.into(),
            shortcut: None,
            focus_handle: None,
            on_open: None,
        }
    }

    /// The palette's chord, shown as one keycap ("Ctrl Shift P").
    pub fn shortcut(mut self, keys: impl IntoIterator<Item = impl Into<SharedString>>) -> Self {
        let keys: Vec<SharedString> = keys.into_iter().map(Into::into).collect();

        if !keys.is_empty() {
            self.shortcut = Some(Self::shortcut_label(&keys));
        }

        self
    }

    /// Makes the field a tab stop that opens on Enter or Space.
    pub fn focus_handle(mut self, handle: &FocusHandle) -> Self {
        self.focus_handle = Some(handle.clone());
        self
    }

    pub fn on_open(mut self, handler: impl Fn(&mut Window, &mut App) + 'static) -> Self {
        self.on_open = Some(Box::new(handler));
        self
    }

    /// Keys of a chord joined into the single keycap the title bar shows.
    pub fn shortcut_label(keys: &[SharedString]) -> SharedString {
        keys.iter()
            .map(|key| key.as_ref())
            .collect::<Vec<_>>()
            .join(" ")
            .into()
    }
}

impl RenderOnce for CommandSearch {
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        let search_icon = ShellIcons::current(cx).search;
        let theme = cx.theme();
        let focused = self
            .focus_handle
            .as_ref()
            .is_some_and(|handle| handle.is_focused(window));

        let mut shape = Chamfer::new(ChamferCut::CONTROL)
            .fill(theme.background)
            .border(theme.border)
            .fill_hover(theme.secondary)
            .fill_active(theme.secondary_hover)
            .interactive("command-search-chamfer");

        if focused {
            shape = shape.ring(ChamferRing::focus(ChromeColors::tint(theme)));
        }

        let on_open = self.on_open.map(std::rc::Rc::new);
        let key_id = SharedString::from(format!("{}-key", self.id));

        div()
            .debug_selector(crate::selector::id_selector(&self.id))
            .id(self.id)
            .key_context(COMMAND_SEARCH_KEY_CONTEXT)
            .aria_label(self.placeholder.clone())
            .relative()
            .flex()
            .flex_shrink_0()
            .w(ShellMetrics::COMMAND_SEARCH_WIDTH)
            .items_center()
            .gap(ShellMetrics::COMMAND_SEARCH_GAP)
            .h(ShellMetrics::COMMAND_SEARCH_HEIGHT)
            .px(ShellMetrics::COMMAND_SEARCH_PADDING_X)
            .text_size(ShellMetrics::COMMAND_SEARCH_FONT)
            .text_color(theme.muted_foreground)
            .cursor_pointer()
            .child(shape)
            .child(
                Icon::new(search_icon)
                    .size(ShellMetrics::COMMAND_SEARCH_ICON)
                    .color(theme.muted_foreground),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .overflow_hidden()
                    .whitespace_nowrap()
                    .text_ellipsis()
                    .child(self.placeholder),
            )
            .when_some(self.shortcut, |field, shortcut| {
                field.child(Kbd::new(shortcut).id(key_id))
            })
            .when_some(self.focus_handle, |field, handle| {
                field.track_focus(&handle)
            })
            .when_some(on_open, |field, on_open| {
                let on_key = on_open.clone();

                field
                    .on_mouse_down(gpui::MouseButton::Left, |_, window, _| {
                        window.prevent_default();
                    })
                    .on_click(move |_, window, cx| on_open(window, cx))
                    .on_key_down(move |event: &KeyDownEvent, window, cx| {
                        if is_activation_key(&event.keystroke) {
                            cx.stop_propagation();
                            on_key(window, cx);
                        }
                    })
            })
    }
}

/// Which badge the bell wears: the most urgent unread notification decides
/// it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BellUrgency {
    /// Nothing unread: a muted bell on no fill, no badge.
    #[default]
    None,
    /// Only updates or finished tasks: a raised badge with a line-2 edge.
    Info,
    /// An approval waits: a badge in the brand accent with its foreground.
    Approval,
    /// An error is unread: a danger badge. Errors outrank approvals.
    Error,
}

/// The bell at the right end of the title bar: a 34 by 30 px button, cut 6.
/// With something unread it sits on the tint wash with a tint icon and a
/// count badge over its top-right corner; with nothing unread it is a muted
/// ghost button. While its popover is open the wash deepens to 26 %.
#[derive(IntoElement)]
pub struct NotificationBell {
    id: ElementId,
    label: SharedString,
    count: usize,
    urgency: BellUrgency,
    open: bool,
    on_click: Option<ClickHandler>,
}

impl NotificationBell {
    pub fn new(id: impl Into<ElementId>, label: impl Into<SharedString>, count: usize) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            count,
            urgency: BellUrgency::None,
            open: false,
            on_click: None,
        }
    }

    pub fn urgency(mut self, urgency: BellUrgency) -> Self {
        self.urgency = urgency;
        self
    }

    /// Whether the popover the bell opens is on screen.
    pub fn open(mut self, open: bool) -> Self {
        self.open = open;
        self
    }

    pub fn on_click(
        mut self,
        handler: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.on_click = Some(Box::new(handler));
        self
    }

    /// Text of the count badge; `None` hides it. Counts past 99 collapse to
    /// "99+" so the badge keeps its size.
    pub fn badge_text(count: usize) -> Option<SharedString> {
        match count {
            0 => None,
            1..=99 => Some(count.to_string().into()),
            _ => Some("99+".into()),
        }
    }

    /// Fill, text and edge colors of the count badge for an urgency, or
    /// `None` when the bell shows no badge.
    pub fn badge_colors(
        urgency: BellUrgency,
        theme: &gpui_component::Theme,
    ) -> Option<(Hsla, Hsla, Option<Hsla>)> {
        match urgency {
            BellUrgency::None => None,
            BellUrgency::Info => Some((theme.secondary, theme.foreground, Some(theme.input))),
            BellUrgency::Approval => Some((theme.primary, theme.primary_foreground, None)),
            BellUrgency::Error => Some((theme.danger, theme.danger_foreground, None)),
        }
    }
}

impl RenderOnce for NotificationBell {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let bell_icon = ShellIcons::current(cx).bell;
        let theme = cx.theme();
        let badge = Self::badge_text(self.count).zip(Self::badge_colors(self.urgency, theme));
        let label = self.label.clone();

        let tint = ChromeColors::tint(theme);
        let quiet = self.urgency == BellUrgency::None && !self.open;

        let shape = if quiet {
            Chamfer::new(ChamferCut::CONTROL)
                .fill_hover(theme.secondary)
                .fill_active(theme.secondary_hover)
        } else {
            let rest = if self.open {
                NotificationMetrics::BELL_OPEN_ALPHA
            } else {
                ButtonMetrics::SOFT_FILL_REST
            };

            Chamfer::new(ChamferCut::CONTROL)
                .fill(tint.opacity(rest))
                .fill_hover(tint.opacity(ButtonMetrics::SOFT_FILL_HOVER))
                .fill_active(tint.opacity(ButtonMetrics::SOFT_FILL_PRESSED))
        };
        let icon_color = if quiet { theme.muted_foreground } else { tint };

        div()
            .debug_selector(crate::selector::id_selector(&self.id))
            .id(self.id)
            .aria_label(self.label)
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .justify_center()
            .w(ShellMetrics::BELL_WIDTH)
            .h(ShellMetrics::BELL_HEIGHT)
            .cursor_pointer()
            .child(shape.interactive("notification-bell-chamfer"))
            .child(
                Icon::new(bell_icon)
                    .size(ShellMetrics::BELL_ICON)
                    .color(icon_color),
            )
            .when_some(badge, |bell, (text, colors)| {
                bell.child(count_badge("notifications-badge", text, colors))
            })
            .tooltip(move |window, cx| Tooltip::new(label.clone()).build(window, cx))
            .when_some(self.on_click, |bell, handler| bell.on_click(handler))
    }
}

#[cfg(test)]
mod tests;
