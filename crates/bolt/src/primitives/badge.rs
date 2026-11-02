use gpui::prelude::*;
use gpui::{App, ElementId, FontWeight, Hsla, SharedString, Window, div};
use gpui_component::ActiveTheme;

use crate::icon::IconSource;
use crate::primitives::{Chamfer, Icon};
use crate::tokens::{ChamferCut, ChromeColors, Feedback, SyntaxColors};

/// Semantic tone shared by every badge-like label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BadgeTone {
    Neutral,
    Accent,
    Info,
    Success,
    Warning,
    Danger,
    /// The NULL violet, for kinds that are neither state nor risk (a trigger
    /// among dependent objects).
    Violet,
}

impl BadgeTone {
    /// Label color for the tone. Accent resolves to the tint so it stays
    /// readable on the dark ground.
    pub fn text_color(self, theme: &gpui_component::Theme) -> Hsla {
        match self {
            Self::Neutral => theme.muted_foreground,
            Self::Accent => ChromeColors::tint(theme),
            Self::Info => theme.info,
            Self::Success => theme.success,
            Self::Warning => theme.warning,
            Self::Danger => theme.danger,
            Self::Violet => {
                if theme.mode.is_dark() {
                    SyntaxColors::dark().number
                } else {
                    SyntaxColors::light().number
                }
            }
        }
    }

    /// Fill behind the label: the raised surface for Neutral, a wash of the
    /// label color at `alpha` for every other tone.
    pub fn fill(self, theme: &gpui_component::Theme, alpha: f32) -> Hsla {
        match self {
            Self::Neutral => theme.secondary,
            _ => self.text_color(theme).opacity(alpha),
        }
    }
}

/// Stateless badge: a short label on a 4 px chamfer, 20 px tall, with an
/// optional 11 px icon before it.
#[derive(IntoElement)]
pub struct Badge {
    tone: BadgeTone,
    label: SharedString,
    icon: Option<IconSource>,
    id: Option<ElementId>,
    aria_label: Option<SharedString>,
}

impl Badge {
    pub fn new(label: impl Into<SharedString>, tone: BadgeTone) -> Self {
        Self {
            tone,
            label: label.into(),
            icon: None,
            id: None,
            aria_label: None,
        }
    }

    /// Accessible name of the badge instead of its label, for a badge whose
    /// text needs context (`3` read as `3 headers`). It needs [`Self::id`]:
    /// a badge without an id has no accessibility node to carry it.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.aria_label = Some(label.into());
        self
    }

    /// Sets the element id (and debug selector), so tests and automation
    /// find the badge by name.
    pub fn id(mut self, id: impl Into<ElementId>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Draws `icon` in the label color before the label.
    pub fn icon(mut self, icon: impl Into<IconSource>) -> Self {
        self.icon = Some(icon.into());
        self
    }
}

impl RenderOnce for Badge {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let fill = self.tone.fill(theme, Feedback::BADGE_FILL_ALPHA);
        let text_color = self.tone.text_color(theme);
        let accessible_name = self.aria_label.unwrap_or_else(|| self.label.clone());

        let badge = div()
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(Feedback::BADGE_GAP)
            .h(Feedback::BADGE_HEIGHT)
            .px(Feedback::BADGE_PADDING_X)
            .child(Chamfer::new(ChamferCut::KEYCAP).fill(fill))
            .when_some(self.icon, |badge, icon| {
                badge.child(Icon::new(icon).size(Feedback::BADGE_ICON).color(text_color))
            })
            .child(
                div()
                    .whitespace_nowrap()
                    .text_size(Feedback::BADGE_FONT)
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(text_color)
                    .child(self.label),
            );

        crate::selector::with_optional_label(badge, self.id, accessible_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neutral_badge_uses_the_raised_surface() {
        let theme = gpui_component::Theme::default();

        assert_eq!(BadgeTone::Neutral.fill(&theme, 0.14), theme.secondary);
        assert_eq!(
            BadgeTone::Neutral.text_color(&theme),
            theme.muted_foreground
        );
    }

    #[test]
    fn colored_tones_wash_their_label_color() {
        let theme = gpui_component::Theme::default();

        for tone in [
            BadgeTone::Accent,
            BadgeTone::Info,
            BadgeTone::Success,
            BadgeTone::Warning,
            BadgeTone::Danger,
        ] {
            let text = tone.text_color(&theme);
            assert_eq!(tone.fill(&theme, 0.14), text.opacity(0.14));
        }
    }

    fn badges(_cx: &mut App) -> gpui::AnyElement {
        div()
            .child(Badge::new("GET", BadgeTone::Info).id("method"))
            .child(
                Badge::new("3", BadgeTone::Neutral)
                    .id("count")
                    .aria_label("3 headers"),
            )
            .into_any_element()
    }

    #[gpui::test]
    fn a_badge_with_an_id_is_a_label_named_by_its_text(cx: &mut gpui::TestAppContext) {
        use crate::accessibility::{accessibility_node, accessible_name, render_frame};

        let frame = render_frame(cx, badges);

        let method = accessibility_node(&frame, "method").expect("badge node");
        assert_eq!(method.role(), gpui::Role::Label);
        assert_eq!(accessible_name(&frame, "method").as_deref(), Some("GET"));
        assert_eq!(
            accessible_name(&frame, "count").as_deref(),
            Some("3 headers")
        );
    }
}
