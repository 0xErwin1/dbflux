//! The notifications center popover that opens under the title-bar bell
//! (IslNotifications, IslNotificationStates).
//!
//! The popover is presentational: the caller builds the rows, their action
//! buttons and every handler, so this module knows nothing about approvals,
//! errors or updates.

use std::rc::Rc;

use gpui::prelude::*;
use gpui::{
    AnyElement, App, ClickEvent, ElementId, FontWeight, Hsla, MouseButton, Pixels, SharedString,
    Window, div,
};
use gpui_component::ActiveTheme;

use crate::controls::{Button, ButtonSize};
use crate::icons::AppIcon;
use crate::primitives::{Badge, BadgeTone, Chamfer, Icon, Text, status_diamond};
use crate::tokens::{ChamferCut, ChromeColors, NotificationMetrics};
use crate::typography::AppFonts;

type ClickHandler = Rc<dyn Fn(&ClickEvent, &mut Window, &mut App) + 'static>;

/// Color of a row's icon.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotificationIconTone {
    /// Approvals: the tint.
    Accent,
    Danger,
    Success,
    /// Read rows and finished tasks.
    Muted,
}

impl NotificationIconTone {
    fn color(self, theme: &gpui_component::Theme) -> Hsla {
        match self {
            Self::Accent => ChromeColors::tint(theme),
            Self::Danger => theme.danger,
            Self::Success => theme.success,
            Self::Muted => theme.muted_foreground,
        }
    }
}

/// One notification row: unread diamond, icon square, title with an
/// optional chip, a meta line and the row's action buttons. A click on the
/// row outside its buttons runs `on_open`.
pub struct NotificationRow {
    id: ElementId,
    icon: AppIcon,
    icon_tone: NotificationIconTone,
    title: SharedString,
    title_mono: bool,
    chip: Option<(SharedString, BadgeTone)>,
    meta: SharedString,
    meta_code: Option<SharedString>,
    unread: bool,
    actions: Vec<Button>,
    on_open: Option<ClickHandler>,
}

impl NotificationRow {
    pub fn new(
        id: impl Into<ElementId>,
        icon: AppIcon,
        icon_tone: NotificationIconTone,
        title: impl Into<SharedString>,
    ) -> Self {
        Self {
            id: id.into(),
            icon,
            icon_tone,
            title: title.into(),
            title_mono: false,
            chip: None,
            meta: SharedString::default(),
            meta_code: None,
            unread: false,
            actions: Vec::new(),
            on_open: None,
        }
    }

    /// Draws the title in the mono face (tool names).
    pub fn mono_title(mut self) -> Self {
        self.title_mono = true;
        self
    }

    pub fn chip(mut self, label: impl Into<SharedString>, tone: BadgeTone) -> Self {
        self.chip = Some((label.into(), tone));
        self
    }

    pub fn meta(mut self, meta: impl Into<SharedString>) -> Self {
        self.meta = meta.into();
        self
    }

    /// A mono fragment appended to the meta line after a middle dot (a
    /// short correlation id).
    pub fn meta_code(mut self, code: impl Into<SharedString>) -> Self {
        self.meta_code = Some(code.into());
        self
    }

    pub fn unread(mut self, unread: bool) -> Self {
        self.unread = unread;
        self
    }

    /// An action button, drawn inline (24 px) under the meta line.
    pub fn action(mut self, button: Button) -> Self {
        self.actions.push(button.size(ButtonSize::Inline));
        self
    }

    pub fn on_open(
        mut self,
        handler: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.on_open = Some(Rc::new(handler));
        self
    }

    fn render(self, cx: &App) -> AnyElement {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let unread = self.unread;

        let icon_color = if unread {
            self.icon_tone.color(theme)
        } else {
            theme.muted_foreground
        };
        let title_color = if unread {
            ChromeColors::strong(theme)
        } else {
            theme.foreground
        };
        let title_weight = if unread {
            FontWeight::SEMIBOLD
        } else {
            FontWeight::NORMAL
        };

        let marker = if unread {
            div()
                .flex_shrink_0()
                .mt(NotificationMetrics::DIAMOND_OFFSET_TOP)
                .child(status_diamond(tint, NotificationMetrics::DIAMOND))
        } else {
            div().flex_shrink_0().w(NotificationMetrics::DIAMOND)
        };

        let icon_box = div()
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .justify_center()
            .size(NotificationMetrics::ICON_BOX)
            .child(Chamfer::new(ChamferCut::KEYCAP).fill(theme.secondary))
            .child(
                Icon::new(self.icon)
                    .size(NotificationMetrics::ICON)
                    .color(icon_color),
            );

        let title = div()
            .min_w_0()
            .overflow_hidden()
            .whitespace_nowrap()
            .text_ellipsis()
            .text_size(NotificationMetrics::TITLE_FONT)
            .font_weight(title_weight)
            .text_color(title_color)
            .when(self.title_mono, |title| title.font_family(AppFonts::MONO))
            .child(self.title);

        let title_line = div()
            .flex()
            .items_center()
            .gap(NotificationMetrics::TITLE_GAP)
            .min_w_0()
            .child(title)
            .when_some(self.chip, |line, (label, tone)| {
                line.child(Badge::new(label, tone))
            });

        let meta_line = div()
            .text_size(NotificationMetrics::META_FONT)
            .text_color(theme.muted_foreground)
            .child(self.meta)
            .when_some(self.meta_code, |line, code| {
                line.child(" \u{b7} ")
                    .child(div().font_family(AppFonts::MONO).child(code))
            });
        let meta_line = div().flex().child(meta_line);

        let has_actions = !self.actions.is_empty();
        let actions = div()
            .flex()
            .gap(NotificationMetrics::ACTIONS_GAP)
            .mt(NotificationMetrics::ACTIONS_MARGIN_TOP)
            .children(self.actions);

        let content = div()
            .flex()
            .flex_col()
            .gap(NotificationMetrics::TEXT_GAP)
            .flex_grow_1()
            .min_w_0()
            .child(title_line)
            .child(meta_line)
            .when(has_actions, |content| content.child(actions));

        div()
            .id(self.id)
            .flex()
            .gap(NotificationMetrics::ROW_GAP)
            .pt(NotificationMetrics::ROW_PADDING_Y)
            .pb(NotificationMetrics::ROW_PADDING_Y)
            .pl(NotificationMetrics::ROW_PADDING_LEFT)
            .pr(NotificationMetrics::ROW_PADDING_RIGHT)
            .border_b_1()
            .border_color(theme.table_row_border)
            .when(!unread, |row| {
                row.opacity(NotificationMetrics::READ_OPACITY)
            })
            .child(marker)
            .child(icon_box)
            .child(content)
            .when_some(self.on_open, |row, handler| {
                row.cursor_pointer()
                    .on_click(move |event, window, cx| handler(event, window, cx))
            })
            .into_any_element()
    }
}

/// A section of rows under an uppercase label and its count.
pub struct NotificationGroupSection {
    label: SharedString,
    count: Option<usize>,
    rows: Vec<NotificationRow>,
}

impl NotificationGroupSection {
    /// `count` is shown after the label; "Earlier" passes `None`.
    pub fn new(label: impl Into<SharedString>, count: Option<usize>) -> Self {
        Self {
            label: label.into(),
            count,
            rows: Vec::new(),
        }
    }

    pub fn row(mut self, row: NotificationRow) -> Self {
        self.rows.push(row);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn render(self, cx: &App) -> AnyElement {
        let theme = cx.theme();

        let header = div()
            .flex()
            .items_center()
            .gap(NotificationMetrics::GROUP_GAP)
            .pt(NotificationMetrics::GROUP_PADDING_TOP)
            .pb(NotificationMetrics::GROUP_PADDING_BOTTOM)
            .pl(NotificationMetrics::GROUP_PADDING_LEFT)
            .pr(NotificationMetrics::GROUP_PADDING_RIGHT)
            .child(Text::label(self.label).font_size(NotificationMetrics::GROUP_FONT))
            .when_some(self.count, |header, count| {
                header.child(
                    div()
                        .font_family(AppFonts::MONO)
                        .text_size(NotificationMetrics::COUNT_FONT)
                        .text_color(theme.muted_foreground)
                        .child(count.to_string()),
                )
            });

        div()
            .flex()
            .flex_col()
            .child(header)
            .children(self.rows.into_iter().map(|row| row.render(cx)))
            .into_any_element()
    }
}

/// A filter chip of the popover: label and count, the raised fill and
/// strong text when selected.
pub struct NotificationFilterChip {
    id: ElementId,
    label: SharedString,
    count: usize,
    selected: bool,
    on_select: Option<ClickHandler>,
}

impl NotificationFilterChip {
    pub fn new(
        id: impl Into<ElementId>,
        label: impl Into<SharedString>,
        count: usize,
        selected: bool,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            count,
            selected,
            on_select: None,
        }
    }

    pub fn on_select(
        mut self,
        handler: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.on_select = Some(Rc::new(handler));
        self
    }

    fn render(self, cx: &App) -> AnyElement {
        let theme = cx.theme();
        let text_color = if self.selected {
            ChromeColors::strong(theme)
        } else {
            theme.muted_foreground
        };

        let mut shape = Chamfer::new(ChamferCut::KEYCAP)
            .fill_hover(theme.secondary)
            .fill_active(theme.secondary_hover)
            .interactive("notification-filter-chamfer");
        if self.selected {
            shape = shape.fill(theme.secondary);
        }

        div()
            .id(self.id)
            .aria_label(self.label.clone())
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(NotificationMetrics::CHIP_GAP)
            .h(NotificationMetrics::CHIP_HEIGHT)
            .px(NotificationMetrics::CHIP_PADDING_X)
            .text_size(NotificationMetrics::CHIP_FONT)
            .text_color(text_color)
            .cursor_pointer()
            .child(shape)
            .child(div().relative().child(self.label))
            .child(
                div()
                    .relative()
                    .font_family(AppFonts::MONO)
                    .text_size(NotificationMetrics::COUNT_FONT)
                    .text_color(theme.muted_foreground)
                    .child(self.count.to_string()),
            )
            .when_some(self.on_select, |chip, handler| {
                chip.on_click(move |event, window, cx| handler(event, window, cx))
            })
            .into_any_element()
    }
}

/// The popover under the bell: a header with the unread count, "Mark all
/// read" and close; the filter chips; the grouped rows; and a footer with
/// "Clear read". With no rows at all it shows the "up to date" state
/// instead of the chips, rows and footer.
#[derive(IntoElement)]
pub struct NotificationPopover {
    id: ElementId,
    title: SharedString,
    close_label: SharedString,
    unread_label: Option<SharedString>,
    mark_all_read: Option<Button>,
    filters: Vec<NotificationFilterChip>,
    groups: Vec<NotificationGroupSection>,
    footer_hint: SharedString,
    clear_read_label: SharedString,
    clear_read_enabled: bool,
    empty: Option<(SharedString, SharedString)>,
    max_height: Option<Pixels>,
    on_close: Option<ClickHandler>,
    on_clear_read: Option<ClickHandler>,
}

impl NotificationPopover {
    pub fn new(id: impl Into<ElementId>, title: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            title: title.into(),
            close_label: SharedString::default(),
            unread_label: None,
            mark_all_read: None,
            filters: Vec::new(),
            groups: Vec::new(),
            footer_hint: SharedString::default(),
            clear_read_label: SharedString::default(),
            clear_read_enabled: false,
            empty: None,
            max_height: None,
            on_close: None,
            on_clear_read: None,
        }
    }

    /// "N unread" beside the title; `None` hides it.
    pub fn unread_label(mut self, label: Option<SharedString>) -> Self {
        self.unread_label = label;
        self
    }

    /// The header's "Mark all read" button; drawn inline.
    pub fn mark_all_read(mut self, button: Button) -> Self {
        self.mark_all_read = Some(button.size(ButtonSize::Inline));
        self
    }

    pub fn filter(mut self, chip: NotificationFilterChip) -> Self {
        self.filters.push(chip);
        self
    }

    /// A group of rows; empty groups are skipped.
    pub fn group(mut self, group: NotificationGroupSection) -> Self {
        if !group.is_empty() {
            self.groups.push(group);
        }
        self
    }

    pub fn footer(
        mut self,
        hint: impl Into<SharedString>,
        clear_read_label: impl Into<SharedString>,
        clear_read_enabled: bool,
    ) -> Self {
        self.footer_hint = hint.into();
        self.clear_read_label = clear_read_label.into();
        self.clear_read_enabled = clear_read_enabled;
        self
    }

    /// Shows the "up to date" state with this title and message instead of
    /// the chips, groups and footer.
    pub fn empty(
        mut self,
        title: impl Into<SharedString>,
        message: impl Into<SharedString>,
    ) -> Self {
        self.empty = Some((title.into(), message.into()));
        self
    }

    pub fn max_height(mut self, height: Pixels) -> Self {
        self.max_height = Some(height);
        self
    }

    pub fn on_close(
        mut self,
        label: impl Into<SharedString>,
        handler: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.close_label = label.into();
        self.on_close = Some(Rc::new(handler));
        self
    }

    pub fn on_clear_read(
        mut self,
        handler: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.on_clear_read = Some(Rc::new(handler));
        self
    }

    fn render_header(
        title: SharedString,
        unread_label: Option<SharedString>,
        mark_all_read: Option<Button>,
        close_label: SharedString,
        on_close: Option<ClickHandler>,
        cx: &App,
    ) -> impl IntoElement {
        let theme = cx.theme();

        let close = Button::new("notifications-close", close_label)
            .icon(AppIcon::CircleX)
            .icon_only()
            .when_some(on_close, |button, handler| {
                button.on_click(move |event, window, cx| handler(event, window, cx))
            });

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(NotificationMetrics::HEADER_GAP)
            .h(NotificationMetrics::HEADER_HEIGHT)
            .pl(NotificationMetrics::HEADER_PADDING_LEFT)
            .pr(NotificationMetrics::HEADER_PADDING_RIGHT)
            .border_b_1()
            .border_color(theme.border)
            .child(
                Icon::new(AppIcon::Bell)
                    .size(NotificationMetrics::HEADER_ICON)
                    .color(ChromeColors::tint(theme)),
            )
            .child(
                div()
                    .font_weight(FontWeight::BOLD)
                    .text_color(ChromeColors::strong(theme))
                    .child(title),
            )
            .when_some(unread_label, |header, label| {
                header.child(
                    div()
                        .id("notifications-unread-count")
                        .font_family(AppFonts::MONO)
                        .text_size(NotificationMetrics::UNREAD_FONT)
                        .text_color(theme.muted_foreground)
                        .child(label),
                )
            })
            .child(div().flex_grow_1())
            .children(mark_all_read)
            .child(close)
    }

    fn render_empty(title: SharedString, message: SharedString, cx: &App) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .id("notifications-empty")
            .flex()
            .flex_col()
            .items_center()
            .gap(NotificationMetrics::EMPTY_GAP)
            .pt(NotificationMetrics::EMPTY_PADDING_TOP)
            .pb(NotificationMetrics::EMPTY_PADDING_BOTTOM)
            .px(NotificationMetrics::EMPTY_PADDING_X)
            .text_center()
            .child(
                div()
                    .relative()
                    .flex()
                    .items_center()
                    .justify_center()
                    .size(NotificationMetrics::EMPTY_ICON_BOX)
                    .child(Chamfer::new(ChamferCut::INPUT).fill(theme.secondary))
                    .child(
                        Icon::new(AppIcon::Check)
                            .size(NotificationMetrics::EMPTY_ICON)
                            .color(theme.muted_foreground),
                    ),
            )
            .child(
                div()
                    .font_weight(FontWeight::BOLD)
                    .text_color(ChromeColors::strong(theme))
                    .child(title),
            )
            .child(
                div()
                    .text_size(NotificationMetrics::EMPTY_MESSAGE_FONT)
                    .line_height(NotificationMetrics::EMPTY_MESSAGE_LINE_HEIGHT)
                    .text_color(theme.muted_foreground)
                    .child(message),
            )
    }

    fn render_footer(
        hint: SharedString,
        clear_read_label: SharedString,
        clear_read_enabled: bool,
        on_clear_read: Option<ClickHandler>,
        cx: &App,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let strong = ChromeColors::strong(theme);

        let clear_read = div()
            .id("notifications-clear-read")
            .aria_label(clear_read_label.clone())
            .text_color(if clear_read_enabled {
                tint
            } else {
                theme.muted_foreground
            })
            .child(clear_read_label)
            .when(clear_read_enabled, |link| {
                link.cursor_pointer()
                    .hover(move |style| style.text_color(strong))
                    .when_some(on_clear_read, |link, handler| {
                        link.on_click(move |event, window, cx| handler(event, window, cx))
                    })
            });

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .h(NotificationMetrics::FOOTER_HEIGHT)
            .px(NotificationMetrics::FOOTER_PADDING_X)
            .border_t_1()
            .border_color(theme.border)
            .text_size(NotificationMetrics::FOOTER_FONT)
            .text_color(theme.muted_foreground)
            .child(hint)
            .child(div().flex_grow_1())
            .child(clear_read)
    }
}

impl RenderOnce for NotificationPopover {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let shadow = NotificationMetrics::shadow(theme);
        let fill = crate::composites::island_fill(theme);
        let edge = theme.input;

        let header = Self::render_header(
            self.title.clone(),
            self.unread_label,
            self.mark_all_read,
            self.close_label,
            self.on_close,
            cx,
        );

        let body: AnyElement = match self.empty {
            Some((title, message)) => Self::render_empty(title, message, cx).into_any_element(),
            None => {
                let chips = div()
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .gap(NotificationMetrics::CHIP_GAP)
                    .pt(NotificationMetrics::CHIPS_PADDING_TOP)
                    .px(NotificationMetrics::CHIPS_PADDING_X)
                    .pb(NotificationMetrics::CHIPS_PADDING_BOTTOM)
                    .children(self.filters.into_iter().map(|chip| chip.render(cx)));

                let list = div()
                    .id("notifications-list")
                    .flex()
                    .flex_col()
                    .flex_grow_1()
                    .flex_shrink_1()
                    .min_h_0()
                    .overflow_y_scroll()
                    .children(self.groups.into_iter().map(|group| group.render(cx)));

                let footer = Self::render_footer(
                    self.footer_hint,
                    self.clear_read_label,
                    self.clear_read_enabled,
                    self.on_clear_read,
                    cx,
                );

                div()
                    .flex()
                    .flex_col()
                    .flex_grow_1()
                    .flex_shrink_1()
                    .min_h_0()
                    .child(chips)
                    .child(list)
                    .child(footer)
                    .into_any_element()
            }
        };

        div()
            .id(self.id)
            .aria_label(self.title)
            .relative()
            .flex()
            .flex_col()
            .w(NotificationMetrics::POPOVER_WIDTH)
            .when_some(self.max_height, |popover, height| popover.max_h(height))
            .font_family(AppFonts::INTERFACE)
            .text_color(theme.foreground)
            .shadow(vec![shadow])
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .on_mouse_down(MouseButton::Right, |_, _, cx| cx.stop_propagation())
            .child(Chamfer::new(ChamferCut::OVERLAY).fill(fill).border(edge))
            .child(
                div()
                    .relative()
                    .flex()
                    .flex_col()
                    .flex_grow_1()
                    .flex_shrink_1()
                    .min_h_0()
                    .child(header)
                    .child(body),
            )
    }
}
