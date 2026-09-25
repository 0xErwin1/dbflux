//! Generic left phase rail shared by the wizard family (Migrate, Export,
//! Import): a vertical list of entries with a checkmark on completed steps, a
//! highlight on the current one, and optional click-to-return navigation.
//! Domain-free — callers supply their own phase enum's labels and completion
//! state via [`RailItem`]; this module only renders.
//!
//! The same family also shares its modal size ([`WIZARD_MODAL_WIDTH`],
//! [`WIZARD_MODAL_HEIGHT_FRACTION`]) and the running step's determinate
//! progress bar ([`render_wizard_progress_bar`]), so the three wizards open at
//! one size and report progress the same way.

use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

use crate::icons::AppIcon;
use crate::primitives::{Chamfer, Icon};
use crate::tokens::{ChamferCut, ChromeColors, StepperMetrics};

/// Width of the phase rail (P1Migrate).
pub const WIZARD_RAIL_WIDTH: Pixels = px(220.0);

/// Modal width every data wizard opens at.
pub const WIZARD_MODAL_WIDTH: Pixels = px(1000.0);

/// Modal height every data wizard opens at, as a fraction of the window
/// height, so the running step's per-table list has room on tall windows.
pub const WIZARD_MODAL_HEIGHT_FRACTION: f32 = 0.8;

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

/// Presentation state of one step, derived from its [`RailItem`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StepState {
    Completed,
    Current,
    Pending,
}

impl StepState {
    fn of(item: &RailItem) -> Self {
        if item.current {
            Self::Current
        } else if item.completed {
            Self::Completed
        } else {
            Self::Pending
        }
    }
}

/// Renders a wizard's left phase rail from `items` in order (P1Migrate): one
/// 44 px row per step with a cut-4 badge. A completed step shows a success
/// check on a raised badge, the current step its number on a byzantine
/// badge with a bold strong label, a pending step its number on a panel
/// badge with a muted label.
///
/// `on_select`, when `Some`, is invoked with the clicked entry's index and
/// makes completed entries clickable for back-navigation; `None` (or a
/// caller that never marks entries completed) renders a display-only
/// progress rail with no hover/click affordance.
pub fn render_wizard_rail<F>(items: &[RailItem], on_select: Option<F>, cx: &App) -> impl IntoElement
where
    F: Fn(usize, &mut Window, &mut App) + Clone + 'static,
{
    let theme = cx.theme();

    let entries: Vec<AnyElement> = items
        .iter()
        .enumerate()
        .map(|(index, item)| render_rail_entry(index, item, on_select.clone(), cx))
        .collect();

    div()
        .flex()
        .flex_col()
        .flex_shrink_0()
        .w(WIZARD_RAIL_WIDTH)
        .pt(StepperMetrics::RAIL_PADDING_TOP)
        .bg(theme.background)
        .border_r_1()
        .border_color(theme.border)
        .children(entries)
}

/// The cut-4 badge of one step: a check once completed, the step number
/// otherwise.
fn render_step_badge(index: usize, state: StepState, cx: &App) -> Div {
    let theme = cx.theme();

    let (fill, content_color) = match state {
        StepState::Completed => (theme.secondary, theme.success),
        StepState::Current => (theme.primary, theme.primary_foreground),
        StepState::Pending => (theme.popover, theme.muted_foreground),
    };

    let content = match state {
        StepState::Completed => Icon::new(AppIcon::Check)
            .size(StepperMetrics::BADGE_ICON)
            .color(content_color)
            .into_any_element(),
        StepState::Current | StepState::Pending => div()
            .font_weight(FontWeight::BOLD)
            .text_color(content_color)
            .child(SharedString::from((index + 1).to_string()))
            .into_any_element(),
    };

    div()
        .relative()
        .flex()
        .flex_shrink_0()
        .items_center()
        .justify_center()
        .size(StepperMetrics::BADGE)
        .child(Chamfer::new(ChamferCut::KEYCAP).fill(fill))
        .child(content)
}

fn render_rail_entry<F>(index: usize, item: &RailItem, on_select: Option<F>, cx: &App) -> AnyElement
where
    F: Fn(usize, &mut Window, &mut App) + Clone + 'static,
{
    let theme = cx.theme();
    let state = StepState::of(item);

    let label_color = match state {
        StepState::Current => ChromeColors::strong(theme),
        StepState::Completed => theme.foreground,
        StepState::Pending => theme.muted_foreground,
    };
    let hover_fill = theme.list_hover;

    div()
        .id(SharedString::from(format!("wizard-rail-{index}")))
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(StepperMetrics::GAP)
        .h(StepperMetrics::RAIL_STEP_HEIGHT)
        .px(StepperMetrics::RAIL_PADDING_X)
        .text_size(StepperMetrics::FONT)
        .text_color(label_color)
        .when(state == StepState::Current, |row| {
            row.font_weight(FontWeight::BOLD)
        })
        .child(render_step_badge(index, state, cx))
        .child(div().min_w_0().truncate().child(item.label.clone()))
        .when_some(
            on_select.filter(|_| state == StepState::Completed),
            |row, on_select| {
                row.cursor_pointer()
                    .hover(move |style| style.bg(hover_fill))
                    .on_click(move |_event, window, app| on_select(index, window, app))
            },
        )
        .into_any_element()
}

/// Fraction of rows done for the running step's progress bar, or `None` when
/// the total is unknown or zero — the caller then shows only the row counter.
pub fn wizard_progress_fraction(rows_done: u64, estimated_total: Option<u64>) -> Option<f32> {
    match estimated_total {
        Some(total) if total > 0 => Some((rows_done as f32 / total as f32).clamp(0.0, 1.0)),
        _ => None,
    }
}

/// Renders a full-width determinate progress bar filled to `fraction`
/// (clamped to `0.0..=1.0`). Callers render it only when a total is known;
/// without one they keep a text-only row counter.
pub fn render_wizard_progress_bar(fraction: f32, cx: &App) -> impl IntoElement {
    let theme = cx.theme();

    div()
        .w_full()
        .h(px(6.0)) // guardrail-allow: progress-bar track height
        .rounded_full()
        .bg(theme.muted)
        .child(
            div()
                .h_full()
                .w(relative(fraction.clamp(0.0, 1.0)))
                .rounded_full()
                .bg(theme.primary),
        )
}

#[cfg(test)]
mod tests {
    use super::{RailItem, StepState, wizard_progress_fraction};

    fn item(completed: bool, current: bool) -> RailItem {
        RailItem {
            label: "Step".into(),
            completed,
            current,
        }
    }

    #[test]
    fn current_wins_over_completed_and_the_rest_are_pending() {
        assert_eq!(StepState::of(&item(true, true)), StepState::Current);
        assert_eq!(StepState::of(&item(true, false)), StepState::Completed);
        assert_eq!(StepState::of(&item(false, false)), StepState::Pending);
    }

    #[test]
    fn progress_fraction_is_none_without_a_positive_total() {
        assert_eq!(wizard_progress_fraction(10, None), None);
        assert_eq!(wizard_progress_fraction(10, Some(0)), None);
    }

    #[test]
    fn progress_fraction_divides_and_clamps_to_one() {
        assert_eq!(wizard_progress_fraction(25, Some(100)), Some(0.25));
        assert_eq!(wizard_progress_fraction(150, Some(100)), Some(1.0));
    }
}
