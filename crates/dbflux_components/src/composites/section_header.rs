use gpui::prelude::*;
use gpui::{App, SharedString, div};
use gpui_component::ActiveTheme;

use crate::primitives::Text;
use crate::tokens::Spacing;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SectionHeaderVariant {
    Settings,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SectionHeaderTextInspection {
    pub variant: crate::primitives::TextVariant,
    pub uses_role_default_color: bool,
    pub uses_muted_foreground_override: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SectionHeaderInspection {
    pub padding_x: gpui::Pixels,
    pub padding_y: gpui::Pixels,
    pub has_bottom_border: bool,
    pub has_action: bool,
    pub title: SectionHeaderTextInspection,
    pub subtitle: SectionHeaderTextInspection,
}

pub fn inspect_section_header(
    variant: SectionHeaderVariant,
    has_action: bool,
) -> SectionHeaderInspection {
    let title = match variant {
        SectionHeaderVariant::Settings => Text::headline_1("Section"),
    };

    let subtitle = match variant {
        SectionHeaderVariant::Settings => Text::body_sm("Subtitle").muted_foreground(),
    };

    SectionHeaderInspection {
        padding_x: Spacing::XL,
        padding_y: Spacing::LG,
        has_bottom_border: true,
        has_action,
        title: SectionHeaderTextInspection {
            variant: crate::primitives::TextVariant::Headline1,
            uses_role_default_color: title.uses_role_default_color(),
            uses_muted_foreground_override: title.uses_muted_foreground_override(),
        },
        subtitle: SectionHeaderTextInspection {
            variant: crate::primitives::TextVariant::BodySm,
            uses_role_default_color: subtitle.uses_role_default_color(),
            uses_muted_foreground_override: subtitle.uses_muted_foreground_override(),
        },
    }
}

pub fn section_header_variant(
    title: impl Into<SharedString>,
    subtitle: impl Into<SharedString>,
    variant: SectionHeaderVariant,
    cx: &App,
) -> gpui::Div {
    section_header_variant_with_action(title, subtitle, variant, None, cx)
}

pub fn section_header_variant_with_action(
    title: impl Into<SharedString>,
    subtitle: impl Into<SharedString>,
    variant: SectionHeaderVariant,
    action: Option<gpui::AnyElement>,
    cx: &App,
) -> gpui::Div {
    let theme = cx.theme();
    let inspection = inspect_section_header(variant, action.is_some());

    let text_block = div()
        .flex_1()
        .min_w_0()
        .child(Text::headline_1(title))
        .child(
            div()
                .mt_1()
                .child(Text::body_sm(subtitle).muted_foreground()),
        );

    let mut content = div()
        .flex()
        .items_center()
        .justify_between()
        .gap(Spacing::MD)
        .child(text_block);

    if let Some(action) = action {
        content = content.child(action);
    }

    let mut header = div()
        .px(inspection.padding_x)
        .py(inspection.padding_y)
        .child(content);

    if inspection.has_bottom_border {
        header = header.border_b_1().border_color(theme.border);
    }

    header
}

/// Render a settings-style section header with title, subtitle, and bottom border.
///
/// Returns a `Div` so callers can chain additional GPUI attributes.
pub fn section_header(
    title: impl Into<SharedString>,
    subtitle: impl Into<SharedString>,
    cx: &App,
) -> gpui::Div {
    section_header_variant(title, subtitle, SectionHeaderVariant::Settings, cx)
}

/// Render a section header with a right-aligned action element.
pub fn section_header_with_action(
    title: impl Into<SharedString>,
    subtitle: impl Into<SharedString>,
    action: impl IntoElement,
    cx: &App,
) -> gpui::Div {
    section_header_variant_with_action(
        title,
        subtitle,
        SectionHeaderVariant::Settings,
        Some(action.into_any_element()),
        cx,
    )
}

#[cfg(test)]
mod tests {
    use super::{SectionHeaderVariant, inspect_section_header};
    use crate::primitives::TextVariant;
    use crate::tokens::Spacing;

    #[test]
    fn settings_section_header_uses_canonical_settings_text_roles() {
        let inspection = inspect_section_header(SectionHeaderVariant::Settings, false);

        assert_eq!(inspection.title.variant, TextVariant::Headline1);
        assert!(inspection.title.uses_role_default_color);

        assert_eq!(inspection.subtitle.variant, TextVariant::BodySm);
        assert!(inspection.subtitle.uses_muted_foreground_override);
    }

    #[test]
    fn settings_section_header_keeps_shared_spacing_and_action_support() {
        let without_action = inspect_section_header(SectionHeaderVariant::Settings, false);
        assert_eq!(without_action.padding_x, Spacing::XL);
        assert_eq!(without_action.padding_y, Spacing::LG);
        assert!(without_action.has_bottom_border);
        assert!(!without_action.has_action);

        let with_action = inspect_section_header(SectionHeaderVariant::Settings, true);
        assert!(with_action.has_action);
    }
}
