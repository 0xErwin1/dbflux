//! How dense a product renders its chrome.
//!
//! Two tiers: the baseline [`Density::Default`] and the optional
//! [`Density::Compact`]. A product maps its own style setting onto a
//! `Density` and hands it to the theme at startup; a product that never asks
//! for compact simply never receives it.

use gpui::{Pixels, px};

/// The density tier a product renders at.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Density {
    /// The baseline tier: square corners, roomy chrome.
    #[default]
    Default,
    /// The compact tier: subtle radii, tighter chrome.
    Compact,
}

impl Density {
    /// Corner radius of a control.
    pub fn radius(self) -> Pixels {
        match self {
            Density::Default => px(0.0),
            Density::Compact => px(2.0),
        }
    }

    /// Corner radius of a large surface, such as a popover shell.
    pub fn radius_lg(self) -> Pixels {
        match self {
            Density::Default => px(0.0),
            Density::Compact => px(3.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_baseline_tier_keeps_square_corners() {
        assert_eq!(Density::default(), Density::Default);
        assert_eq!(Density::Default.radius(), px(0.0));
        assert_eq!(Density::Default.radius_lg(), px(0.0));
    }

    #[test]
    fn the_compact_tier_applies_the_token_radii() {
        assert_eq!(Density::Compact.radius(), px(2.0));
        assert_eq!(Density::Compact.radius_lg(), px(3.0));
    }
}
