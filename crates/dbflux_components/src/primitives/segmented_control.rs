//! `SegmentedControl` — a horizontal row of mutually-exclusive option segments.
//!
//! A chamfered track (ground fill, 1 px line, 2 px padding) holding 26 px
//! segments. The active segment is a raised thumb with a 4 px cut; each
//! segment shows an optional icon before its label.

use std::sync::Arc;

use gpui::prelude::*;
use gpui::{App, SharedString, Window, div};
use gpui_component::ActiveTheme;

use crate::icons::AppIcon;
use crate::primitives::{Chamfer, Icon};
use crate::tokens::{ChamferCut, Fields, FontSizes};
use crate::typography::AppFonts;

/// A single option within a `SegmentedControl`.
#[derive(Debug, Clone)]
pub struct SegmentedItem {
    pub id: SharedString,
    pub label: SharedString,
    pub icon: Option<AppIcon>,
}

impl SegmentedItem {
    pub fn new(id: impl Into<SharedString>, label: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            icon: None,
        }
    }

    /// Draws `icon` before the label.
    pub fn icon(mut self, icon: AppIcon) -> Self {
        self.icon = Some(icon);
        self
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

/// A horizontal row of mutually-exclusive segments.
///
/// - Track: `ChamferCut::CONTROL`, `theme.background` fill, 1 px `theme.border`
///   line, `Fields::SEGMENT_TRACK_PADDING` inside.
/// - Segment: `Fields::SEGMENT_HEIGHT` tall, icon and label 6 px apart,
///   `FontSizes::XS` in the interface face.
/// - Active segment: raised thumb (`theme.secondary`, `ChamferCut::KEYCAP`),
///   strong text.
/// - Inactive segment: muted text; hover lays the hover wash under it.
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
        let active_id = self.active_id.clone();
        let on_select = self.on_select;

        let segments = self.items.into_iter().map(|item| {
            let is_active = item.id == active_id;
            let segment_id = SharedString::from(format!("seg-ctl-item-{}", item.id.as_ref()));

            let thumb = if is_active {
                Chamfer::new(ChamferCut::KEYCAP).fill(theme.secondary)
            } else {
                Chamfer::new(ChamferCut::KEYCAP)
                    .fill_hover(theme.accent)
                    .interactive(SharedString::from(format!("{segment_id}-thumb")))
            };

            let text_color = if is_active {
                theme.accent_foreground
            } else {
                theme.muted_foreground
            };

            let clicked_id = item.id.clone();
            let on_select = on_select.clone();

            div()
                .id(segment_id)
                .relative()
                .flex()
                .items_center()
                .justify_center()
                .gap(Fields::SEGMENT_GAP)
                .h(Fields::SEGMENT_HEIGHT)
                .px(Fields::SEGMENT_PADDING_X)
                .cursor_pointer()
                .text_color(text_color)
                .child(thumb)
                .when_some(item.icon, |this, icon| {
                    this.child(Icon::new(icon).size(Fields::SEGMENT_ICON).color(text_color))
                })
                .child(item.label)
                .on_mouse_down(gpui::MouseButton::Left, move |_, window, cx| {
                    on_select(&clicked_id, window, cx);
                })
                .into_any_element()
        });

        div()
            .relative()
            .flex()
            .flex_none()
            .items_center()
            .p(Fields::SEGMENT_TRACK_PADDING)
            .font_family(AppFonts::INTERFACE)
            .text_size(FontSizes::XS)
            .child(
                Chamfer::new(ChamferCut::CONTROL)
                    .fill(theme.background)
                    .border(theme.border),
            )
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
