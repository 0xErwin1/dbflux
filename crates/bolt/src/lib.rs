//! Bolt, the product-agnostic design system.
//!
//! Bolt owns everything that does not depend on the product: color roles and washes,
//! typography roles, spacing, cut corners and chrome metrics. A product supplies only its
//! brand (its accent and name) and any token of its own, then installs it at startup.
//!
//! This crate never names a product, a driver, a database or a product-core type, so a
//! second product consumes it as a dependency swap.

#![allow(clippy::type_complexity)]
// Required by the `#[gpui::test]` attribute macro expansion in test modules.
#![recursion_limit = "512"]

pub mod brand;

/// Accessibility checks for rendered surfaces: the accessible-name helpers a
/// surface test asserts with. Gated like the product crates' `test-support`
/// features, so a normal build of the design system carries none of it.
#[cfg(any(test, feature = "test-support"))]
pub mod accessibility;

pub mod colors;
pub mod composites;
pub mod controls;
pub mod density;
pub mod fonts;
pub mod icon;
pub mod modals;
pub mod palette;
pub mod primitives;
pub mod selector;
pub mod semantic;
pub mod theme;
pub mod tokens;
pub mod typography;

pub use colors::Colors;

/// The neutral and semantic color roles of the installed palette.
pub fn colors(cx: &gpui::App) -> Colors {
    Colors::for_current(cx)
}
