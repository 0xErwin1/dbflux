//! Chrome shared by the document views: the header and toolbar rows, the
//! toolbar search field and time presets, the detail fields under an
//! expanded row, and the footer with its pager (P1Audit, P1Approvals,
//! P1Dashboard, P1Chart, P1Buckets, P1Objects, P1ObjectEditor, P2Series).

use dbflux_components::common::time_range::{TimeRange, TimeRangePanel};
use dbflux_components::composites::BreadcrumbSegment;
use dbflux_components::controls::{GpuiInput, InputState};
use dbflux_components::icon::IconSource;
use dbflux_components::icons::{AppIcon, DriverIconTone};
use dbflux_components::primitives::{
    Chamfer, ChamferRing, Icon, Kbd, SegmentedControl, SegmentedItem,
};
use dbflux_components::tokens::{
    ChamferCut, ChromeColors, DocumentMetrics, Fields, FontSizes, ResultMetrics,
};
use dbflux_components::typography::AppFonts;
use dbflux_ui_base::AppStateEntity;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;

/// A document row: `height` tall (pixels or rems), 14 px side padding, 8 px
/// gap and a line along the bottom. The header, toolbar and filter rows of
/// every document view start from this.
pub(crate) fn document_bar(height: impl Into<AbsoluteLength>, cx: &App) -> Div {
    let height: AbsoluteLength = height.into();

    div()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(DocumentMetrics::GAP)
        .h(height)
        .px(DocumentMetrics::PADDING_X)
        .border_b_1()
        .border_color(cx.theme().border)
}

/// The leading icon and bold title of a document header.
pub(crate) fn document_title(
    icon: impl Into<IconSource>,
    icon_color: Hsla,
    title: impl Into<SharedString>,
    cx: &App,
) -> Div {
    div()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(DocumentMetrics::GAP)
        .child(
            Icon::new(icon)
                .size(DocumentMetrics::TITLE_ICON)
                .color(icon_color),
        )
        .child(
            div()
                .whitespace_nowrap()
                .text_size(DocumentMetrics::TITLE_FONT)
                .font_weight(FontWeight::BOLD)
                .text_color(ChromeColors::strong(cx.theme()))
                .child(title.into()),
        )
}

/// The muted one-line description after a document title, at the 13 px
/// body size rather than whatever the host inherits.
pub(crate) fn document_subtitle(text: impl Into<SharedString>, cx: &App) -> Div {
    div()
        .min_w_0()
        .truncate()
        .text_size(FontSizes::BASE)
        .text_color(cx.theme().muted_foreground)
        .child(text.into())
}

/// The breadcrumb segment of a saved connection: its name after the driver
/// logo in the driver's palette color. `None` when the profile is unknown.
pub(crate) fn connection_segment(
    app_state: &Entity<AppStateEntity>,
    profile_id: uuid::Uuid,
    cx: &App,
) -> Option<BreadcrumbSegment> {
    let state = app_state.read(cx);
    let profile = state
        .profiles()
        .iter()
        .find(|profile| profile.id == profile_id)?;

    let mut segment = BreadcrumbSegment::new(profile.name.clone());

    if let Some(driver) = state.drivers().get(&profile.driver_id()) {
        let metadata = driver.metadata();
        segment = segment.icon(
            AppIcon::for_driver(metadata.icon, metadata.category),
            Some(DriverIconTone::for_driver(metadata.icon, metadata.category).resolve(cx)),
        );
    }

    Some(segment)
}

/// A 1 px vertical rule between groups of toolbar controls.
pub(crate) fn toolbar_rule(cx: &App) -> Div {
    div()
        .flex_shrink_0()
        .w(px(1.0))
        .h(DocumentMetrics::TOOLBAR_RULE_HEIGHT)
        .mx(DocumentMetrics::TOOLBAR_RULE_MARGIN_X)
        .bg(cx.theme().border)
}

/// A toolbar search field: a 30 px chamfered field on the ground with a
/// search icon, the frameless input and the `/` keycap while it is empty.
pub(crate) fn search_field(
    input: &Entity<InputState>,
    width: Option<Pixels>,
    focused: bool,
    cx: &App,
) -> Div {
    let theme = cx.theme();
    let is_empty = input.read(cx).value().is_empty();

    let mut shape = Chamfer::new(ChamferCut::CONTROL)
        .fill(theme.background)
        .border(theme.border);

    if focused {
        shape = shape.ring(ChamferRing::focus(ChromeColors::tint(theme)));
    }

    div()
        .relative()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(Fields::GAP)
        .h(Fields::HEIGHT)
        .px(Fields::PADDING_X)
        .text_size(Fields::TEXT)
        .map(|field| match width {
            Some(width) => field.w(width),
            None => field.flex_1().min_w_0(),
        })
        .child(shape)
        .child(
            Icon::new(AppIcon::Search)
                .size(DocumentMetrics::SEARCH_ICON)
                .color(theme.muted_foreground),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .child(GpuiInput::new(input).xsmall().appearance(false)),
        )
        .when(is_empty, |field| field.child(Kbd::new("/")))
}

/// Presets offered by a time-range segmented control, in display order.
pub(crate) const TIME_PRESETS: [TimeRange; 5] = [
    TimeRange::Last15min,
    TimeRange::LastHour,
    TimeRange::Last6Hours,
    TimeRange::Last24Hours,
    TimeRange::Last7Days,
];

/// Index of `range` in the `TimeRangePanel` preset list.
pub(crate) fn time_preset_index(range: TimeRange) -> usize {
    match range {
        TimeRange::Last15min => 0,
        TimeRange::LastHour => 1,
        TimeRange::Last6Hours => 2,
        TimeRange::Last24Hours => 3,
        TimeRange::Last7Days => 4,
        TimeRange::Custom => 5,
    }
}

fn time_preset_id(range: TimeRange) -> SharedString {
    SharedString::from(format!("time-preset-{}", time_preset_index(range)))
}

/// The time presets as a segmented control (15m, 1h, 6h, 24h, 7d and,
/// when `with_custom`, Custom). `on_select` receives the preset index in
/// the `TimeRangePanel` list.
pub(crate) fn time_preset_control(
    selected: Option<TimeRange>,
    with_custom: bool,
    on_select: impl Fn(usize, &mut Window, &mut App) + 'static,
) -> SegmentedControl {
    let mut items: Vec<SegmentedItem> = TIME_PRESETS
        .iter()
        .map(|range| {
            SegmentedItem::new(time_preset_id(*range), TimeRangePanel::preset_label(*range))
        })
        .collect();

    if with_custom {
        items.push(
            SegmentedItem::new(
                time_preset_id(TimeRange::Custom),
                TimeRangePanel::preset_label(TimeRange::Custom),
            )
            .icon(AppIcon::Clock),
        );
    }

    let active = selected.map(time_preset_id).unwrap_or_default();

    SegmentedControl::new(items, active, move |id, window, cx| {
        let index = (0..=5).find(|index| {
            TimeRangePanel::time_range_for_index(*index)
                .is_some_and(|range| time_preset_id(range) == *id)
        });

        if let Some(index) = index {
            on_select(index, window, cx);
        }
    })
}

/// One field of a detail block: an 11 px muted label over a 12.5 px mono
/// value in the strong color.
pub(crate) fn detail_field(
    label: impl Into<SharedString>,
    value: impl IntoElement,
    cx: &App,
) -> Div {
    let theme = cx.theme();

    div()
        .flex()
        .flex_col()
        .min_w_0()
        .gap(DocumentMetrics::DETAIL_LABEL_GAP)
        .child(
            div()
                .text_size(DocumentMetrics::DETAIL_LABEL_FONT)
                .text_color(theme.muted_foreground)
                .child(label.into()),
        )
        .child(
            div()
                .min_w_0()
                .truncate()
                .font_family(AppFonts::MONO)
                .text_size(DocumentMetrics::TABLE_CELL_FONT)
                .text_color(ChromeColors::strong(theme))
                .child(value),
        )
}

/// The footer of a document: 36 px on the ground, a line above, 14 px side
/// padding and gap, 12 px muted text.
pub(crate) fn document_footer(cx: &App) -> Div {
    let theme = cx.theme();

    div()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(ResultMetrics::FOOTER_GAP)
        .h(ResultMetrics::FOOTER_HEIGHT)
        .px(DocumentMetrics::PADDING_X)
        .border_t_1()
        .border_color(theme.border)
        .bg(theme.background)
        .text_size(ResultMetrics::FOOTER_FONT)
        .text_color(theme.muted_foreground)
}

/// A footer item: a 13 px icon before its label.
pub(crate) fn footer_item(
    icon: impl Into<IconSource>,
    label: impl Into<SharedString>,
    cx: &App,
) -> Div {
    div()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(ResultMetrics::FOOTER_ITEM_GAP)
        .child(
            Icon::new(icon)
                .size(ResultMetrics::FOOTER_ICON)
                .color(cx.theme().muted_foreground),
        )
        .child(label.into())
}

/// One arrow of a footer pager; disabled arrows fade and ignore clicks.
pub(crate) fn pager_arrow(
    id: &'static str,
    icon: AppIcon,
    enabled: bool,
    cx: &App,
) -> Stateful<Div> {
    let theme = cx.theme();
    let muted = theme.muted_foreground;

    div()
        .id(id)
        .flex()
        .items_center()
        .when(!enabled, |arrow| arrow.opacity(Fields::DISABLED_OPACITY))
        .when(enabled, |arrow| arrow.cursor_pointer())
        .child(
            Icon::new(icon)
                .size(ResultMetrics::FOOTER_ICON)
                .color(if enabled { theme.foreground } else { muted }),
        )
}

/// A footer pager: previous arrow, the current page in the strong color,
/// `/ total`, and the next arrow, in mono.
pub(crate) fn footer_pager(
    page: u32,
    total_pages: Option<u64>,
    previous: Stateful<Div>,
    next: Stateful<Div>,
    cx: &App,
) -> Div {
    div()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(ResultMetrics::PAGER_GAP)
        .font_family(AppFonts::MONO)
        .child(previous)
        .child(
            div()
                .text_color(ChromeColors::strong(cx.theme()))
                .child(page.to_string()),
        )
        .when_some(total_pages, |pager, total| {
            pager.child(format!("/ {total}"))
        })
        .child(next)
}

/// A key hint in a footer: a keycap followed by what it does.
pub(crate) fn footer_key_hint(key: impl Into<SharedString>, label: impl Into<SharedString>) -> Div {
    div()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(DocumentMetrics::GAP)
        .child(Kbd::new(key))
        .child(label.into())
}

#[cfg(test)]
mod tests {
    use super::{TIME_PRESETS, time_preset_index};
    use dbflux_components::common::time_range::{TimeRange, TimeRangePanel};

    #[test]
    fn time_preset_indices_match_the_panel_list() {
        for range in TIME_PRESETS.iter().copied().chain([TimeRange::Custom]) {
            assert_eq!(
                TimeRangePanel::time_range_for_index(time_preset_index(range)),
                Some(range)
            );
        }
    }
}
