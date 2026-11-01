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
pub mod chart;
pub mod components;
pub mod composites;
pub mod data;

/// Accessibility checks for rendered surfaces: the accessible-name helpers a
/// surface test asserts with. Gated like the product crates' `test-support`
/// features, so a normal build of the design system carries none of it.
#[cfg(any(test, feature = "test-support"))]
pub mod accessibility;

pub mod colors;
pub mod command_palette;
pub mod controls;
pub mod density;
pub mod fonts;

/// The form seam: the edit state a form host moves between, the per-field
/// facts and the command-driven two-dimensional navigation over the host's
/// own field rows.
pub mod forms;

pub mod icon;

/// The keyboard-coverage rule as a reusable registry: a product fills in its
/// surfaces, element ids and commands over its own keyboard vocabulary, and
/// a rendered-frame check fails when a clickable element has no keyboard
/// path, or a registered command has no key.
pub mod keyboard_coverage;

pub mod keymap;

/// The keyboard engine: key chords, keymap layers, user overrides, conflict
/// detection and shortcut recording, over a product-supplied context
/// vocabulary. A consumer that only needs the mechanics takes the
/// `bolt_keys` crate directly; this re-export gives the umbrella crate's
/// users the whole keyboard surface.
pub use bolt_keys as keys;
pub mod modals;
pub mod palette;
pub mod primitives;
pub mod selector;
pub mod semantic;
pub mod shell;
pub mod strings;

pub mod theme;
/// The toast stack: the severity model, its throttle and bounded stack, and
/// the presentation shell. Content and product wiring stay product-side.
pub mod toast;
pub mod tokens;
pub mod typography;
pub mod vim;

pub use colors::Colors;

/// The neutral and semantic color roles of the installed palette.
pub fn colors(cx: &gpui::App) -> Colors {
    Colors::for_current(cx)
}
