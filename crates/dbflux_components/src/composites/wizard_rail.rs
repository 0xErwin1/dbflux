//! Generic left phase rail shared by the wizard family (Migrate, Export,
//! Import): a vertical list of entries with a checkmark on completed steps, a
//! highlight on the current one, and optional click-to-return navigation.
//! Domain-free — callers supply their own phase enum's labels and completion
//! state via [`RailItem`]; this module only renders.

use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

use crate::icons::AppIcon;
use crate::primitives::{Icon, Text};
use crate::tokens::Spacing;

/// One rail row's presentation state: a completed entry shows a checkmark and
/// (when `on_select` is provided) is clickable for back-navigation; the
/// current entry is highlighted. Callers derive this from their own phase
/// ordering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RailItem {
    pub label: SharedString,
    pub completed: bool,
    pub current: bool,
}

/// The rail's marker/label colors, resolved once per render from the theme.
/// `current` and `done` use the theme's bright action/success colors (not the
/// dark `accent`, which is a low-contrast highlight background on dark
/// themes), so the current phase reads as the most prominent entry.
#[derive(Clone, Copy)]
struct RailColors {
    current: Hsla,
    done: Hsla,
    muted: Hsla,
    hover_bg: Hsla,
}

/// Renders a wizard's left phase rail from `items` in order. `on_select`, when
/// `Some`, is invoked with the clicked entry's index and makes completed
/// entries clickable for back-navigation; `None` (or a caller that never
/// marks entries completed) renders a display-only progress rail with no
/// hover/click affordance.
pub fn render_wizard_rail<F>(items: &[RailItem], on_select: Option<F>, cx: &App) -> impl IntoElement
where
    F: Fn(usize, &mut Window, &mut App) + Clone + 'static,
{
    let theme = cx.theme();
    let colors = RailColors {
        current: theme.primary,
        done: theme.success,
        muted: theme.muted_foreground,
        hover_bg: theme.secondary,
    };
    let border = theme.border;

    let entries = items
        .iter()
        .cloned()
        .enumerate()
        .map(move |(index, item)| render_rail_entry(index, item, colors, on_select.clone()));

    div()
        .flex()
        .flex_col()
        .gap(Spacing::XS)
        .p(Spacing::MD)
        .min_w(px(180.0))
        .border_r_1()
        .border_color(border)
        .children(entries)
}

fn render_rail_entry<F>(
    index: usize,
    item: RailItem,
    colors: RailColors,
    on_select: Option<F>,
) -> impl IntoElement
where
    F: Fn(usize, &mut Window, &mut App) + Clone + 'static,
{
    let marker = if item.completed {
        Icon::new(AppIcon::CircleCheck)
            .size(px(14.0))
            .color(colors.done)
            .into_any_element()
    } else {
        let dot_color = if item.current {
            colors.current
        } else {
            colors.muted
        };
        div()
            .size(px(8.0)) // guardrail-allow: decorative status-dot diameter, not a spacing token
            .rounded_full()
            .bg(dot_color)
            .into_any_element()
    };

    // The current entry is the most prominent: bright action color plus a
    // heavier weight. Every other label stays at full foreground contrast (a
    // check/dot marker conveys completed vs. pending), so the rail reads
    // clearly instead of as dim, low-contrast text.
    let mut label = Text::body(item.label.clone());
    if item.current {
        label = label
            .color(colors.current)
            .font_weight(FontWeight::SEMIBOLD);
    }

    div()
        .id(SharedString::from(format!("wizard-rail-{index}")))
        .flex()
        .items_center()
        .gap(Spacing::SM)
        .px(Spacing::SM)
        .py(Spacing::XS)
        .rounded_md()
        .child(
            div()
                .w(px(16.0)) // guardrail-allow: fixed rail marker gutter width for label alignment
                .flex()
                .items_center()
                .justify_center()
                .child(marker),
        )
        .child(label)
        .when_some(on_select.filter(|_| item.completed), |el, on_select| {
            el.cursor_pointer()
                .hover(|style| style.bg(colors.hover_bg))
                .on_click(move |_event, window, app| on_select(index, window, app))
        })
}
