use gpui::prelude::*;
use gpui::{App, Hsla, Pixels, Window, div};
use gpui_component::ActiveTheme;

use crate::tokens::Radii;

/// Background variant controlling the surface color.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SurfaceVariant {
    /// Main panel background — `theme.background`.
    Panel,
    /// Slightly elevated card — `theme.secondary`.
    Card,
    /// Popover / dropdown surface — `theme.popover`.
    Raised,
    /// Modal backdrop — semi-transparent black.
    Overlay,
}

/// Stateless surface primitive. Applies the correct background, optional
/// border, and corner radius from the active theme.
#[derive(IntoElement)]
pub struct Surface {
    variant: SurfaceVariant,
    bg_override: Option<Hsla>,
    border_color_override: Option<Hsla>,
    rounded_override: Option<Pixels>,
    show_border: bool,
}

impl Surface {
    pub fn panel() -> Self {
        Self {
            variant: SurfaceVariant::Panel,
            bg_override: None,
            border_color_override: None,
            rounded_override: None,
            show_border: true,
        }
    }

    pub fn card() -> Self {
        Self {
            variant: SurfaceVariant::Card,
            bg_override: None,
            border_color_override: None,
            rounded_override: None,
            show_border: true,
        }
    }

    pub fn raised() -> Self {
        Self {
            variant: SurfaceVariant::Raised,
            bg_override: None,
            border_color_override: None,
            rounded_override: None,
            show_border: true,
        }
    }

    pub fn overlay() -> Self {
        Self {
            variant: SurfaceVariant::Overlay,
            bg_override: None,
            border_color_override: None,
            rounded_override: None,
            show_border: false,
        }
    }

    /// Override the background color (replaces the variant default).
    pub fn bg(mut self, color: impl Into<Hsla>) -> Self {
        self.bg_override = Some(color.into());
        self
    }

    /// Override the border color (replaces the theme default).
    pub fn border_color(mut self, color: impl Into<Hsla>) -> Self {
        self.border_color_override = Some(color.into());
        self
    }

    /// Override the corner radius (replaces the variant default).
    pub fn rounded(mut self, radius: Pixels) -> Self {
        self.rounded_override = Some(radius);
        self
    }

    /// Disable the border.
    pub fn no_border(mut self) -> Self {
        self.show_border = false;
        self
    }

    fn default_bg(&self, theme: &gpui_component::Theme) -> Hsla {
        match self.variant {
            SurfaceVariant::Panel => theme.background,
            SurfaceVariant::Card => theme.secondary,
            SurfaceVariant::Raised => theme.popover,
            SurfaceVariant::Overlay => gpui::black().opacity(0.5),
        }
    }

    fn default_radius(&self) -> Pixels {
        match self.variant {
            SurfaceVariant::Panel => Radii::LG,
            SurfaceVariant::Card => Radii::LG,
            SurfaceVariant::Raised => Radii::MD,
            SurfaceVariant::Overlay => Radii::LG,
        }
    }
}

impl RenderOnce for Surface {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let bg = self.bg_override.unwrap_or_else(|| self.default_bg(theme));
        let radius = self
            .rounded_override
            .unwrap_or_else(|| self.default_radius());

        let mut el = div().bg(bg).rounded(radius);

        if self.show_border {
            let border_color = self.border_color_override.unwrap_or(theme.border);
            el = el.border_1().border_color(border_color);
        }

        el
    }
}
