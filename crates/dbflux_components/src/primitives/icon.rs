use gpui::prelude::*;
use gpui::{App, Hsla, Pixels, Transformation, Window, svg};
use gpui_component::{ActiveTheme, IconNamed};

use crate::icon::IconSource;
use crate::tokens::Heights;

#[derive(Clone, Copy)]
enum IconTone {
    Explicit(Hsla),
    Muted,
    Primary,
    Warning,
    Danger,
}

/// Stateless SVG icon primitive with consistent sizing and color support.
///
/// Defaults to `Heights::ICON_SM` (16px) and `theme.muted_foreground`.
/// Use builder methods to override size or color.
#[derive(IntoElement)]
pub struct Icon {
    source: IconSource,
    size: Pixels,
    tone: Option<IconTone>,
    transformation: Option<Transformation>,
}

impl Icon {
    /// Create an icon from any source (named or SVG path).
    ///
    /// `source` accepts `IconSource`, `IconName`, or any type that
    /// implements `Into<IconSource>`.
    pub fn new(source: impl Into<IconSource>) -> Self {
        Self {
            source: source.into(),
            size: Heights::ICON_SM,
            tone: None,
            transformation: None,
        }
    }

    /// Override the icon size.
    pub fn size(mut self, size: Pixels) -> Self {
        self.size = size;
        self
    }

    /// Override the icon color (default: `theme.muted_foreground`).
    pub fn color(mut self, color: impl Into<Hsla>) -> Self {
        self.tone = Some(IconTone::Explicit(color.into()));
        self
    }

    pub fn muted(mut self) -> Self {
        self.tone = Some(IconTone::Muted);
        self
    }

    pub fn primary(mut self) -> Self {
        self.tone = Some(IconTone::Primary);
        self
    }

    pub fn warning(mut self) -> Self {
        self.tone = Some(IconTone::Warning);
        self
    }

    pub fn danger(mut self) -> Self {
        self.tone = Some(IconTone::Danger);
        self
    }

    pub fn with_transformation(mut self, transformation: Transformation) -> Self {
        self.transformation = Some(transformation);
        self
    }

    /// Convenience: small icon (16px).
    pub fn small(self) -> Self {
        self.size(Heights::ICON_SM)
    }

    /// Convenience: medium icon (20px).
    pub fn medium(self) -> Self {
        self.size(Heights::ICON_MD)
    }

    /// Convenience: large icon (24px).
    pub fn large(self) -> Self {
        self.size(Heights::ICON_LG)
    }
}

impl RenderOnce for Icon {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let color = match self.tone.unwrap_or(IconTone::Muted) {
            IconTone::Explicit(color) => color,
            IconTone::Muted => theme.muted_foreground,
            IconTone::Primary => theme.primary,
            IconTone::Warning => theme.warning,
            IconTone::Danger => theme.danger,
        };

        let icon = match self.source {
            IconSource::Named(name) => svg().path(name.path()).size(self.size).text_color(color),
            IconSource::Svg(path) => svg().path(path).size(self.size).text_color(color),
        };

        match self.transformation {
            Some(transformation) => icon.with_transformation(transformation),
            None => icon,
        }
    }
}
