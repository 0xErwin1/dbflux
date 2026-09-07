//! Domain-free master-detail list panel: a scrollable column of rows (each
//! with an id, label, optional detail/badge) plus an optional "New" and
//! "Secondary" action, shaped like the hand-rendered list panels in the
//! settings sections (Proxies, SSH Tunnels, MCP). Callers own selection,
//! focus, and scroll state; this module only renders.

use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;

use crate::primitives::{Icon, Text, focus_frame};
use crate::tokens::{Radii, Spacing, Widths};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BadgeTone {
    Neutral,
    Accent,
    Success,
    Danger,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MasterDetailItem {
    pub id: SharedString,
    pub label: SharedString,
    pub detail: Option<SharedString>,
    pub badge: Option<(SharedString, BadgeTone)>,
    pub selected: bool,
    pub focused: bool,
}

#[derive(Clone, Debug)]
pub struct MasterDetailAction {
    pub label: SharedString,
    pub enabled: bool,
    pub focused: bool,
}

#[derive(Clone, Debug)]
pub struct MasterDetailListConfig {
    pub id: SharedString,
    pub width: Pixels,
    pub new_action: Option<MasterDetailAction>,
    pub secondary_action: Option<MasterDetailAction>,
    pub empty_message: Option<SharedString>,
}

impl Default for MasterDetailListConfig {
    fn default() -> Self {
        Self {
            id: SharedString::from("master-detail-list"),
            width: Widths::SETTINGS_LIST_PANEL,
            new_action: None,
            secondary_action: None,
            empty_message: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MasterDetailActionKind {
    New,
    Secondary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowKind {
    Selected,
    Focused,
    Plain,
}

/// Derives a row's visual kind from its selection and list-cursor state.
///
/// `Selected` takes precedence over `Focused` when both are true: selection
/// marks the item currently open in the detail form, which stays the more
/// prominent signal even while the keyboard cursor also sits on that row.
pub fn master_detail_row_kind(selected: bool, focused: bool) -> RowKind {
    if selected {
        RowKind::Selected
    } else if focused {
        RowKind::Focused
    } else {
        RowKind::Plain
    }
}

struct RowColors {
    primary: Hsla,
    secondary: Hsla,
    list_even: Hsla,
    muted_foreground: Hsla,
    accent: Hsla,
    success: Hsla,
    danger: Hsla,
    border: Hsla,
}

fn badge_color(tone: BadgeTone, colors: &RowColors) -> Hsla {
    match tone {
        BadgeTone::Neutral => colors.muted_foreground,
        BadgeTone::Accent => colors.accent,
        BadgeTone::Success => colors.success,
        BadgeTone::Danger => colors.danger,
    }
}

fn render_action_button(
    kind: MasterDetailActionKind,
    action: MasterDetailAction,
    colors: &RowColors,
    cx: &App,
    on_action: impl Fn(MasterDetailActionKind, &mut Window, &mut App) + 'static,
) -> impl IntoElement {
    let element_id = match kind {
        MasterDetailActionKind::New => "master-detail-list-new-action",
        MasterDetailActionKind::Secondary => "master-detail-list-secondary-action",
    };

    div()
        .id(element_id)
        .rounded(Radii::SM)
        .child(focus_frame(
            action.focused,
            None,
            div()
                .flex()
                .items_center()
                .justify_center()
                .gap(Spacing::XS)
                .px(Spacing::SM)
                .py(Spacing::XS)
                .rounded(Radii::SM)
                .when(action.enabled, |el| {
                    el.cursor_pointer().hover({
                        let secondary = colors.secondary;
                        move |style| style.bg(secondary)
                    })
                })
                .when(!action.enabled, |el| el.opacity(0.5))
                .child(Icon::new(crate::icons::AppIcon::Plus).size(px(14.0)))
                .child(Text::body(action.label)),
            cx,
        ))
        .when(action.enabled, |el| {
            el.on_click(move |_event, window, cx| on_action(kind, window, cx))
        })
}

fn render_row<S>(
    item: MasterDetailItem,
    index: usize,
    colors: &RowColors,
    cx: &App,
    on_select: S,
) -> Div
where
    S: Fn(usize, &mut Window, &mut App) + 'static,
{
    let kind = master_detail_row_kind(item.selected, item.focused);

    let ring_color = match kind {
        RowKind::Selected | RowKind::Focused => Some(colors.primary),
        RowKind::Plain => None,
    };

    let row = div()
        .id(SharedString::from(format!("master-detail-row-{}", item.id)))
        .rounded(Radii::SM)
        .bg(colors.list_even)
        .cursor_pointer()
        .when(kind == RowKind::Selected, |el| el.bg(colors.secondary))
        .hover({
            let secondary = colors.secondary;
            move |style| style.bg(secondary)
        })
        .on_click(move |_event, window, cx| on_select(index, window, cx))
        .child(
            div()
                .flex()
                .items_start()
                .justify_between()
                .gap(Spacing::SM)
                .px(Spacing::SM)
                .py(Spacing::XS)
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .min_w_0()
                        .gap(Spacing::XXS)
                        .child(Text::body(item.label))
                        .when_some(item.detail, |el, detail| el.child(Text::caption(detail))),
                )
                .when_some(item.badge, |el, (label, tone)| {
                    el.child(Text::caption(label).color(badge_color(tone, colors)))
                }),
        );

    focus_frame(ring_color.is_some(), ring_color, row, cx).rounded(Radii::SM)
}

pub fn render_master_detail_list<S, A>(
    config: &MasterDetailListConfig,
    items: &[MasterDetailItem],
    scroll_handle: &ScrollHandle,
    on_select: S,
    on_action: A,
    cx: &App,
) -> impl IntoElement
where
    S: Fn(usize, &mut Window, &mut App) + Clone + 'static,
    A: Fn(MasterDetailActionKind, &mut Window, &mut App) + Clone + 'static,
{
    let theme = cx.theme();
    let colors = RowColors {
        primary: theme.primary,
        secondary: theme.secondary,
        list_even: theme.list_even,
        muted_foreground: theme.muted_foreground,
        accent: theme.accent,
        success: theme.success,
        danger: theme.danger,
        border: theme.border,
    };

    let header = if config.new_action.is_some() || config.secondary_action.is_some() {
        Some(
            div()
                .p(Spacing::SM)
                .border_b_1()
                .border_color(colors.border)
                .flex()
                .flex_col()
                .gap(Spacing::SM)
                .when_some(config.new_action.clone(), |el, action| {
                    let on_action = on_action.clone();
                    el.child(render_action_button(
                        MasterDetailActionKind::New,
                        action,
                        &colors,
                        cx,
                        move |kind, window, cx| on_action(kind, window, cx),
                    ))
                })
                .when_some(config.secondary_action.clone(), |el, action| {
                    let on_action = on_action.clone();
                    el.child(render_action_button(
                        MasterDetailActionKind::Secondary,
                        action,
                        &colors,
                        cx,
                        move |kind, window, cx| on_action(kind, window, cx),
                    ))
                }),
        )
    } else {
        None
    };

    let body = div()
        .id(SharedString::from(format!("{}-body", config.id)))
        .track_scroll(scroll_handle)
        .flex_1()
        .min_h_0()
        .overflow_y_scrollbar()
        .p(Spacing::SM)
        .flex()
        .flex_col()
        .gap(Spacing::XS)
        .when(items.is_empty(), |el| {
            if let Some(message) = config.empty_message.clone() {
                el.child(
                    div()
                        .p(Spacing::LG)
                        .child(Text::body(message).color(colors.muted_foreground)),
                )
            } else {
                el
            }
        })
        .children(items.iter().cloned().enumerate().map(|(index, item)| {
            let on_select = on_select.clone();
            render_row(item, index, &colors, cx, move |idx, window, cx| {
                on_select(idx, window, cx)
            })
        }));

    div()
        .id(config.id.clone())
        .w(config.width)
        .h_full()
        .border_r_1()
        .border_color(colors.border)
        .flex()
        .flex_col()
        .when_some(header, |el, header| el.child(header))
        .child(body)
}

#[cfg(test)]
mod tests {
    use super::{RowKind, master_detail_row_kind};

    #[test]
    fn plain_row_when_neither_selected_nor_focused() {
        assert_eq!(master_detail_row_kind(false, false), RowKind::Plain);
    }

    #[test]
    fn selected_row_when_selected_only() {
        assert_eq!(master_detail_row_kind(true, false), RowKind::Selected);
    }

    #[test]
    fn focused_row_when_focused_only() {
        assert_eq!(master_detail_row_kind(false, true), RowKind::Focused);
    }

    #[test]
    fn selected_wins_when_both_selected_and_focused() {
        assert_eq!(master_detail_row_kind(true, true), RowKind::Selected);
    }
}
