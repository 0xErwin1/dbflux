//! Domain-free master-detail list panel: a scrollable column of rows (each
//! with an id, label, optional detail/badge) plus an optional "New" and
//! "Secondary" action, shaped like the hand-rendered list panels in the
//! settings sections (Proxies, SSH Tunnels, MCP). Callers own selection,
//! focus, and scroll state; this module only renders.

use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;

use crate::composites::ListRow;
use crate::controls::Button;
use crate::icons::AppIcon;
use crate::primitives::{Badge, BadgeTone, Icon, Text};
use crate::tokens::{ChromeColors, MasterListMetrics, Widths};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MasterDetailItem {
    pub id: SharedString,
    /// Icon before the label, drawn in the tint while the row is selected.
    pub icon: Option<AppIcon>,
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

/// Toolbar button: the New action is the primary button, the secondary
/// action a secondary one.
fn render_action_button(
    kind: MasterDetailActionKind,
    action: MasterDetailAction,
    on_action: impl Fn(MasterDetailActionKind, &mut Window, &mut App) + 'static,
) -> impl IntoElement {
    let (element_id, button) = match kind {
        MasterDetailActionKind::New => (
            "master-detail-list-new-action",
            Button::new("master-detail-list-new-action", action.label).primary(),
        ),
        MasterDetailActionKind::Secondary => (
            "master-detail-list-secondary-action",
            Button::new("master-detail-list-secondary-action", action.label).secondary(),
        ),
    };

    div().id(element_id).child(
        button
            .icon(AppIcon::Plus)
            .focused(action.focused)
            .disabled(!action.enabled)
            .on_click(move |_event, window, cx| on_action(kind, window, cx)),
    )
}

/// A row: `ListRow` with the selection bar; the icon and the semibold label
/// on the first line with the badge on its right, the mono detail under it.
fn render_row<S>(item: MasterDetailItem, index: usize, cx: &App, on_select: S) -> Stateful<Div>
where
    S: Fn(usize, &mut Window, &mut App) + 'static,
{
    let theme = cx.theme();
    let icon_color = if item.selected {
        ChromeColors::tint(theme)
    } else {
        theme.muted_foreground
    };

    ListRow::new(SharedString::from(format!("master-detail-row-{}", item.id)))
        .selected(item.selected)
        .selection_bar(true)
        .focused(item.focused && !item.selected)
        .build(cx)
        .flex()
        .flex_col()
        .gap(MasterListMetrics::ROW_LINE_GAP)
        .py(MasterListMetrics::ROW_PADDING_Y)
        .px(MasterListMetrics::ROW_PADDING_X)
        .border_b_1()
        .border_color(theme.table_row_border)
        .on_click(move |_event, window, cx| on_select(index, window, cx))
        .child(
            div()
                .flex()
                .items_center()
                .gap(MasterListMetrics::ROW_ICON_GAP)
                .when_some(item.icon, |line, icon| {
                    line.child(
                        Icon::new(icon)
                            .size(MasterListMetrics::ROW_ICON)
                            .color(icon_color),
                    )
                })
                .child(
                    div()
                        .flex_1()
                        .min_w_0()
                        .overflow_hidden()
                        .whitespace_nowrap()
                        .text_ellipsis()
                        .child(
                            Text::body(item.label)
                                .font_weight(FontWeight::SEMIBOLD)
                                .text_color(ChromeColors::strong(theme)),
                        ),
                )
                .when_some(item.badge, |line, (label, tone)| {
                    line.child(Badge::new(label, tone))
                }),
        )
        .when_some(item.detail, |row, detail| {
            row.child(
                div()
                    .overflow_hidden()
                    .whitespace_nowrap()
                    .text_ellipsis()
                    .child(
                        Text::code(detail)
                            .font_size(MasterListMetrics::ROW_DETAIL_FONT)
                            .muted_foreground(),
                    ),
            )
        })
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

    let header = (config.new_action.is_some() || config.secondary_action.is_some()).then(|| {
        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(MasterListMetrics::TOOLBAR_GAP)
            .p(MasterListMetrics::TOOLBAR_PADDING)
            .when_some(config.new_action.clone(), |el, action| {
                let on_action = on_action.clone();
                el.child(render_action_button(
                    MasterDetailActionKind::New,
                    action,
                    move |kind, window, cx| on_action(kind, window, cx),
                ))
            })
            .when_some(config.secondary_action.clone(), |el, action| {
                let on_action = on_action.clone();
                el.child(render_action_button(
                    MasterDetailActionKind::Secondary,
                    action,
                    move |kind, window, cx| on_action(kind, window, cx),
                ))
            })
    });

    let body = div()
        .id(SharedString::from(format!("{}-body", config.id)))
        .track_scroll(scroll_handle)
        .flex_1()
        .min_h_0()
        .overflow_y_scrollbar()
        .flex()
        .flex_col()
        .when(items.is_empty(), |el| {
            if let Some(message) = config.empty_message.clone() {
                el.child(
                    div()
                        .px(MasterListMetrics::ROW_PADDING_X)
                        .py(MasterListMetrics::ROW_PADDING_Y)
                        .child(Text::body(message).muted_foreground()),
                )
            } else {
                el
            }
        })
        .children(items.iter().cloned().enumerate().map(|(index, item)| {
            let on_select = on_select.clone();
            render_row(item, index, cx, move |idx, window, cx| {
                on_select(idx, window, cx)
            })
        }));

    div()
        .id(config.id.clone())
        .w(config.width)
        .h_full()
        .flex_shrink_0()
        .border_r_1()
        .border_color(theme.border)
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
