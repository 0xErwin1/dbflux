//! `SegmentedControl` — a horizontal row of mutually-exclusive option segments.
//!
//! Matches the `.seg-ctl` CSS spec: 28 px height, shared outer border, no
//! inter-segment gap, inner segment dividers, and the active segment filled
//! with the theme primary color.

use std::sync::Arc;

use gpui::prelude::*;
use gpui::{App, SharedString, Window, div};
use gpui_component::ActiveTheme;

use crate::tokens::{FontSizes, Heights, Radii, Spacing};
use crate::typography::AppFonts;

/// A single option within a `SegmentedControl`.
#[derive(Debug, Clone)]
pub struct SegmentedItem {
    pub id: SharedString,
    pub label: SharedString,
}

impl SegmentedItem {
    pub fn new(id: impl Into<SharedString>, label: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
        }
    }
}

/// Selects the next active id given the current items, the current active id,
/// and the id of the segment that was clicked.
///
/// This is a pure helper so it can be unit-tested without a GPUI context.
pub fn new_active_id(items: &[SegmentedItem], _current: &str, clicked: &str) -> SharedString {
    items
        .iter()
        .find(|item| item.id.as_ref() == clicked)
        .map(|item| item.id.clone())
        .unwrap_or_else(|| SharedString::from(clicked.to_string()))
}

/// A horizontal row of mutually-exclusive labeled segments.
///
/// Visual spec (from `.seg-ctl`):
/// - Height: 28 px (`Heights::CONTROL`)
/// - Outer border: 1 px solid `theme.input`
/// - No gap between segments — inner dividers only
/// - Active segment: `theme.primary` background, `theme.primary_foreground` text
/// - Inactive segment: transparent background, `theme.muted_foreground` text,
///   hover lifts to `theme.list_hover`
/// - Font: `AppFonts::BODY`, `FontSizes::XS`
#[derive(IntoElement)]
pub struct SegmentedControl {
    items: Vec<SegmentedItem>,
    active_id: SharedString,
    on_select: Arc<dyn Fn(&SharedString, &mut Window, &mut App)>,
}

impl SegmentedControl {
    pub fn new(
        items: Vec<SegmentedItem>,
        active_id: impl Into<SharedString>,
        on_select: impl Fn(&SharedString, &mut Window, &mut App) + 'static,
    ) -> Self {
        Self {
            items,
            active_id: active_id.into(),
            on_select: Arc::new(on_select),
        }
    }
}

impl RenderOnce for SegmentedControl {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        if self.items.is_empty() {
            return div().into_any_element();
        }

        let theme = cx.theme().clone();
        let count = self.items.len();
        let active_id = self.active_id.clone();
        let on_select = self.on_select;

        let segments = self.items.into_iter().enumerate().map(|(idx, item)| {
            let is_active = item.id == active_id;
            let is_last = idx == count - 1;

            let bg = if is_active {
                theme.primary
            } else {
                gpui::transparent_black()
            };
            let fg = if is_active {
                theme.primary_foreground
            } else {
                theme.muted_foreground
            };
            let hover_bg = theme.list_hover;

            let clicked_id = item.id.clone();
            let on_select = on_select.clone();

            let mut segment = div()
                .h(Heights::CONTROL)
                .px(Spacing::SM)
                .flex()
                .items_center()
                .justify_center()
                .cursor_pointer()
                .bg(bg)
                .text_color(fg)
                .font_family(AppFonts::BODY)
                .text_size(FontSizes::XS)
                .when(!is_active, move |d| d.hover(move |d| d.bg(hover_bg)))
                .when(!is_last, |d| d.border_r_1().border_color(theme.input))
                .id(SharedString::from(format!(
                    "seg-ctl-item-{}",
                    clicked_id.as_ref()
                )))
                .on_mouse_down(gpui::MouseButton::Left, move |_, window, cx| {
                    on_select(&clicked_id, window, cx);
                });

            // Apply outer radii only on the first and last segments.
            if count == 1 {
                segment = segment.rounded(Radii::SM);
            } else if idx == 0 {
                segment = segment.rounded_tl(Radii::SM).rounded_bl(Radii::SM);
            } else if is_last {
                segment = segment.rounded_tr(Radii::SM).rounded_br(Radii::SM);
            }

            segment.child(item.label).into_any_element()
        });

        div()
            .flex()
            .items_center()
            .h(Heights::CONTROL)
            .border_1()
            .border_color(theme.input)
            .rounded(Radii::SM)
            .overflow_hidden()
            .children(segments)
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn items() -> Vec<SegmentedItem> {
        vec![
            SegmentedItem::new("disable", "disable"),
            SegmentedItem::new("allow", "allow"),
            SegmentedItem::new("prefer", "prefer"),
            SegmentedItem::new("require", "require"),
        ]
    }

    #[test]
    fn clicking_another_item_returns_its_id() {
        let result = new_active_id(&items(), "prefer", "require");
        assert_eq!(result.as_ref(), "require");
    }

    #[test]
    fn clicking_current_active_returns_same_id() {
        let result = new_active_id(&items(), "prefer", "prefer");
        assert_eq!(result.as_ref(), "prefer");
    }

    #[test]
    fn clicking_first_item_returns_first_id() {
        let result = new_active_id(&items(), "require", "disable");
        assert_eq!(result.as_ref(), "disable");
    }

    #[test]
    fn empty_items_list_returns_clicked_id_without_panic() {
        let empty: Vec<SegmentedItem> = vec![];
        let result = new_active_id(&empty, "", "require");
        assert_eq!(result.as_ref(), "require");
    }

    #[test]
    fn active_id_not_in_items_returns_id_unchanged() {
        let result = new_active_id(&items(), "unknown", "allow");
        assert_eq!(result.as_ref(), "allow");
    }
}
