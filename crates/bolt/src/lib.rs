//! Bolt, the design system shared by DBFlux and Relay.
//!
//! Bolt owns everything that does not depend on the product: color roles and washes,
//! typography roles, spacing, cut corners and chrome metrics. A product supplies only its
//! brand (its accent and name) and any token of its own.
//!
//! This crate never names a product, a driver, a database or a `dbflux_core` type, so a
//! second product consumes it as a dependency swap. See "Design system" in
//! `ARCHITECTURE.md`.

#![allow(clippy::type_complexity)]
// Required by the `#[gpui::test]` attribute macro expansion in test modules.
#![recursion_limit = "512"]

pub mod fonts;
pub mod palette;
pub mod tokens;
pub mod typography;
