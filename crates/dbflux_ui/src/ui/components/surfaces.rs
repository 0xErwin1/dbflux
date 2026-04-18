//! Shared visual surface primitives for the DBFlux UI.
//!
//! These primitives intentionally own only visual treatment: background, border,
//! hover background, text/icon color, radius, and optional fixed height.
//! Call sites still own layout, spacing, sizing, ids, and event handlers.

use crate::ui::components::surfaces_style::{
    BorderEdge, PanelBarStyleKind, ThemeColorSlot, panel_bar_style_defaults,
    tab_trigger_style_defaults,
};
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{Heights, Radii};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

#[derive(Clone, Copy)]
enum HeightMode {
    Default,
    Auto,
    Custom(Pixels),
}

fn theme_slot(theme: &gpui_component::Theme, slot: ThemeColorSlot) -> Hsla {
    match slot {
        ThemeColorSlot::Background => theme.background,
        ThemeColorSlot::Secondary => theme.secondary,
        ThemeColorSlot::SecondaryHover => theme.secondary_hover,
        ThemeColorSlot::TabBar => theme.tab_bar,
        ThemeColorSlot::Border => theme.border,
        ThemeColorSlot::MutedForeground => theme.muted_foreground,
        ThemeColorSlot::Foreground => theme.foreground,
        ThemeColorSlot::Primary => theme.primary,
        ThemeColorSlot::Accent => theme.accent,
        ThemeColorSlot::Transparent => gpui::transparent_black(),
    }
}

fn apply_border_style(div: Div, edge: BorderEdge, border_color: Hsla) -> Div {
    match edge {
        BorderEdge::None => div.border_color(border_color),
        BorderEdge::Top => div.border_t_1().border_color(border_color),
        BorderEdge::Bottom => div.border_b_1().border_color(border_color),
        BorderEdge::All => div.border_1().border_color(border_color),
    }
}

// ─── Surface ────────────────────────────────────────────────────────────────

#[derive(IntoElement)]
pub struct Surface {
    background_override: Option<Hsla>,
    border_color_override: Option<Hsla>,
    border_edge: BorderEdge,
    hover_background: Option<Hsla>,
    radius: Option<Pixels>,
    children: Vec<AnyElement>,
}

impl Default for Surface {
    fn default() -> Self {
        Self {
            background_override: None,
            border_color_override: None,
            border_edge: BorderEdge::All,
            hover_background: None,
            radius: None,
            children: Vec::new(),
        }
    }
}

impl Surface {
    pub fn new() -> Self {
        Self {
            border_edge: BorderEdge::All,
            ..Self::default()
        }
    }

    pub fn background_override(mut self, color: Hsla) -> Self {
        self.background_override = Some(color);
        self
    }

    pub fn border_color_override(mut self, color: Hsla) -> Self {
        self.border_color_override = Some(color);
        self
    }

    pub fn border_edge(mut self, edge: BorderEdge) -> Self {
        self.border_edge = edge;
        self
    }

    pub fn hover_background(mut self, color: Hsla) -> Self {
        self.hover_background = Some(color);
        self
    }

    pub fn rounded(mut self, radius: Pixels) -> Self {
        self.radius = Some(radius);
        self
    }

    pub fn child(mut self, child: impl IntoElement) -> Self {
        self.children.push(child.into_any_element());
        self
    }
}

impl RenderOnce for Surface {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let background = self.background_override.unwrap_or(theme.secondary);
        let border_color = self.border_color_override.unwrap_or(theme.border);

        let base = apply_border_style(div().bg(background), self.border_edge, border_color);
        let base = if let Some(radius) = self.radius {
            base.rounded(radius)
        } else {
            base
        };

        if let Some(hover_background) = self.hover_background {
            base.hover(move |el| el.bg(hover_background))
                .children(self.children)
                .into_any_element()
        } else {
            base.children(self.children).into_any_element()
        }
    }
}

// ─── PanelBar ───────────────────────────────────────────────────────────────

pub enum PanelBarVariant {
    Toolbar,
    RowCompact,
    Footer,
}

#[derive(IntoElement)]
pub struct PanelBar {
    variant: PanelBarVariant,
    background_override: Option<Hsla>,
    border_color_override: Option<Hsla>,
    hover_background: Option<Hsla>,
    height_mode: HeightMode,
    font_family_override: Option<SharedString>,
    children: Vec<AnyElement>,
}

impl PanelBar {
    pub fn new(variant: PanelBarVariant) -> Self {
        Self {
            variant,
            background_override: None,
            border_color_override: None,
            hover_background: None,
            height_mode: HeightMode::Default,
            font_family_override: None,
            children: Vec::new(),
        }
    }

    pub fn background_override(mut self, color: Hsla) -> Self {
        self.background_override = Some(color);
        self
    }

    pub fn border_color_override(mut self, color: Hsla) -> Self {
        self.border_color_override = Some(color);
        self
    }

    pub fn hover_background(mut self, color: Hsla) -> Self {
        self.hover_background = Some(color);
        self
    }

    pub fn height_override(mut self, height: Pixels) -> Self {
        self.height_mode = HeightMode::Custom(height);
        self
    }

    pub fn auto_height(mut self) -> Self {
        self.height_mode = HeightMode::Auto;
        self
    }

    pub fn font_family(mut self, family: impl Into<SharedString>) -> Self {
        self.font_family_override = Some(family.into());
        self
    }

    pub fn child(mut self, child: impl IntoElement) -> Self {
        self.children.push(child.into_any_element());
        self
    }
}

impl RenderOnce for PanelBar {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let kind = match self.variant {
            PanelBarVariant::Toolbar => PanelBarStyleKind::Toolbar,
            PanelBarVariant::RowCompact => PanelBarStyleKind::RowCompact,
            PanelBarVariant::Footer => PanelBarStyleKind::Footer,
        };
        let defaults = panel_bar_style_defaults(kind);

        let background = self
            .background_override
            .unwrap_or(theme_slot(theme, defaults.background));
        let border_color = self
            .border_color_override
            .unwrap_or(theme_slot(theme, defaults.border_color));
        let hover_background = self.hover_background.or_else(|| {
            defaults
                .hover_background
                .map(|slot| theme_slot(theme, slot))
        });

        let base = apply_border_style(div().bg(background), defaults.border_edge, border_color);

        let base = match self.height_mode {
            HeightMode::Default => match defaults.height {
                Some(height) => base.h(height),
                None => base,
            },
            HeightMode::Auto => base,
            HeightMode::Custom(height) => base.h(height),
        };

        let base = if let Some(font_family) = self.font_family_override {
            base.font_family(font_family)
        } else {
            base
        };

        if let Some(hover_background) = hover_background {
            base.hover(move |el| el.bg(hover_background))
                .children(self.children)
                .into_any_element()
        } else {
            base.children(self.children).into_any_element()
        }
    }
}

// ─── TabTrigger ─────────────────────────────────────────────────────────────

#[derive(IntoElement)]
pub struct TabTrigger {
    active: bool,
    active_border_color_override: Option<Hsla>,
    active_background_override: Option<Hsla>,
    inactive_background_override: Option<Hsla>,
    hover_background_override: Option<Hsla>,
    font_family_override: Option<SharedString>,
    children: Vec<AnyElement>,
}

impl TabTrigger {
    pub fn new(active: bool) -> Self {
        Self {
            active,
            active_border_color_override: None,
            active_background_override: None,
            inactive_background_override: None,
            hover_background_override: None,
            font_family_override: None,
            children: Vec::new(),
        }
    }

    pub fn active_border_color_override(mut self, color: Hsla) -> Self {
        self.active_border_color_override = Some(color);
        self
    }

    pub fn active_background_override(mut self, color: Hsla) -> Self {
        self.active_background_override = Some(color);
        self
    }

    pub fn inactive_background_override(mut self, color: Hsla) -> Self {
        self.inactive_background_override = Some(color);
        self
    }

    pub fn hover_background_override(mut self, color: Hsla) -> Self {
        self.hover_background_override = Some(color);
        self
    }

    pub fn font_family(mut self, family: impl Into<SharedString>) -> Self {
        self.font_family_override = Some(family.into());
        self
    }

    pub fn child(mut self, child: impl IntoElement) -> Self {
        self.children.push(child.into_any_element());
        self
    }
}

impl RenderOnce for TabTrigger {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let defaults = tab_trigger_style_defaults();

        let active_border_color = self
            .active_border_color_override
            .unwrap_or(theme_slot(theme, defaults.active_border));
        let active_background = self.active_background_override.or_else(|| {
            defaults
                .active_background
                .map(|slot| theme_slot(theme, slot))
        });
        let inactive_background = self.inactive_background_override.or_else(|| {
            defaults
                .inactive_background
                .map(|slot| theme_slot(theme, slot))
        });
        let hover_background = self.hover_background_override.or_else(|| {
            defaults
                .hover_background
                .map(|slot| theme_slot(theme, slot))
        });

        if self.active {
            let base = div().border_b_2().border_color(active_border_color);
            let base = if let Some(active_background) = active_background {
                base.bg(active_background)
            } else {
                base
            };
            let base = if let Some(font_family) = self.font_family_override {
                base.font_family(font_family)
            } else {
                base
            };

            base.children(self.children).into_any_element()
        } else {
            let base = div()
                .border_b_2()
                .border_color(theme_slot(theme, defaults.inactive_border));
            let base = if let Some(inactive_background) = inactive_background {
                base.bg(inactive_background)
            } else {
                base
            };
            let base = if let Some(hover_background) = hover_background {
                base.hover(move |el| el.bg(hover_background))
            } else {
                base
            };
            let base = if let Some(font_family) = self.font_family_override {
                base.font_family(font_family)
            } else {
                base
            };

            base.children(self.children).into_any_element()
        }
    }
}

// ─── IconButton ─────────────────────────────────────────────────────────────

#[derive(IntoElement)]
pub struct IconButton {
    icon: AppIcon,
    icon_color_override: Option<Hsla>,
    hover_background_override: Option<Hsla>,
    icon_size: Option<Pixels>,
    children: Vec<AnyElement>,
}

impl IconButton {
    pub fn new(icon: AppIcon) -> Self {
        Self {
            icon,
            icon_color_override: None,
            hover_background_override: None,
            icon_size: None,
            children: Vec::new(),
        }
    }

    pub fn icon_color_override(mut self, color: Hsla) -> Self {
        self.icon_color_override = Some(color);
        self
    }

    pub fn hover_background_override(mut self, color: Hsla) -> Self {
        self.hover_background_override = Some(color);
        self
    }

    pub fn icon_size(mut self, size: Pixels) -> Self {
        self.icon_size = Some(size);
        self
    }

    pub fn child(mut self, child: impl IntoElement) -> Self {
        self.children.push(child.into_any_element());
        self
    }
}

impl RenderOnce for IconButton {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let icon_color = self.icon_color_override.unwrap_or(theme.muted_foreground);
        let hover_background = self
            .hover_background_override
            .unwrap_or(theme.secondary_hover);
        let icon_size = self.icon_size.unwrap_or(Heights::ICON_SM);

        div()
            .rounded(Radii::MD)
            .hover(move |el| el.bg(hover_background))
            .child(
                svg()
                    .path(self.icon.path())
                    .size(icon_size)
                    .text_color(icon_color),
            )
            .children(self.children)
    }
}
