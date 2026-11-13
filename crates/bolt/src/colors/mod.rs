//! The neutral and semantic color roles of the installed palette.
//!
//! Screens read neutrals through [`Colors::for_current`] (or the crate-root
//! [`crate::colors`]) instead of a `gpui_component::Theme` field, the same
//! way they read accent roles through [`crate::brand`]. Each role holds the
//! value [`crate::theme::apply_theme`] installs for it, so reading a role and
//! reading the theme field it maps to draw the same color.

use gpui::{App, Hsla, rgb};

use crate::{
    palette::{self, PaletteMode},
    theme::{rgb_to_hsla, rgb_to_hsla_alpha},
};

/// The neutral, wash and semantic color roles of one palette.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Colors {
    /// The window ground the islands sit on.
    pub desk: Hsla,
    /// Inset fills below the panel level: title bar, sidebar, tab bar, editor.
    pub background: Hsla,
    /// Island, popover and table fill.
    pub panel: Hsla,
    /// Raised controls and secondary fills.
    pub raised: Hsla,
    /// Default border.
    pub line: Hsla,
    /// Stronger border: input and control edges.
    pub line_2: Hsla,
    /// Divider between data rows.
    pub row_divider: Hsla,
    /// Titles, selected items and data values.
    pub strong: Hsla,
    /// Body text.
    pub foreground: Hsla,
    /// Metadata, labels, counts and descriptions.
    pub muted: Hsla,
    /// Hover wash over any surface.
    pub hover: Hsla,
    /// Wash of every other data row.
    pub alternating_row: Hsla,
    /// The hairline along an island outline.
    pub island_edge: Hsla,
    /// The dimmed desk behind a modal.
    pub scrim: Hsla,
    /// Success state.
    pub success: Hsla,
    /// Informational state and links.
    pub info: Hsla,
    /// Warning state.
    pub warning: Hsla,
    /// Errors and destructive actions.
    pub danger: Hsla,
    /// Text drawn on a solid success, info, warning or danger fill.
    pub on_semantic: Hsla,
}

impl Colors {
    /// The roles of the dark palette.
    pub fn dark() -> Self {
        Self {
            desk: rgb(0x050507).into(),
            background: rgb_to_hsla(0x09090B),
            panel: rgb_to_hsla(0x100F13),
            raised: rgb_to_hsla(0x1A181E),
            line: rgb_to_hsla(0x232128),
            line_2: rgb_to_hsla(0x37333D),
            row_divider: rgb_to_hsla(0x18161B),
            strong: rgb_to_hsla(0xF7F4F7),
            foreground: rgb_to_hsla(0xC6C3CC),
            muted: rgb_to_hsla(0x8E8996),
            hover: rgb_to_hsla_alpha(0xFFFFFF, 0.04),
            alternating_row: rgb_to_hsla_alpha(0xFFFFFF, 0.012),
            island_edge: Hsla::from(rgb(0xFFFFFF)).opacity(0.05),
            scrim: rgb_to_hsla_alpha(0x050507, 0.62),
            success: rgb_to_hsla(0x7BE0A0),
            info: rgb_to_hsla(0x6EA8FF),
            warning: rgb_to_hsla(0xFFC23D),
            danger: rgb_to_hsla(0xFF6B5E),
            on_semantic: rgb_to_hsla(0x09090B),
        }
    }

    /// The roles of the light palette.
    pub fn light() -> Self {
        Self {
            desk: rgb(0xE6E1E9).into(),
            background: rgb_to_hsla(0xF6F4F7),
            panel: rgb_to_hsla(0xFFFFFF),
            raised: rgb_to_hsla(0xEEEAF0),
            line: rgb_to_hsla(0xE3DEE6),
            line_2: rgb_to_hsla(0xCBC4D1),
            row_divider: rgb_to_hsla(0xF0ECF2),
            strong: rgb_to_hsla(0x141118),
            foreground: rgb_to_hsla(0x3B3740),
            muted: rgb_to_hsla(0x6B6572),
            hover: rgb_to_hsla_alpha(0x141118, 0.04),
            alternating_row: rgb_to_hsla_alpha(0x141118, 0.015),
            island_edge: Hsla::from(rgb(0x141118)).opacity(0.06),
            scrim: rgb_to_hsla_alpha(0x1E1423, 0.28),
            success: rgb_to_hsla(0x1C7F45),
            info: rgb_to_hsla(0x1F5FD1),
            warning: rgb_to_hsla(0xB7791F),
            danger: rgb_to_hsla(0xC7362B),
            on_semantic: rgb_to_hsla(0xFFFFFF),
        }
    }

    /// The roles of the palette the product published; dark until it
    /// publishes one, and for an unresolved `System` request.
    pub fn for_current(cx: &App) -> Self {
        match palette::resolved_mode(cx) {
            PaletteMode::Light => Self::light(),
            PaletteMode::Dark | PaletteMode::System => Self::dark(),
        }
    }
}

#[cfg(test)]
mod tests;
