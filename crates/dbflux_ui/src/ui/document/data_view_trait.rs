//! `DataView` trait for view kinds hosted inside `ResultPanel`.
//!
//! `DataView` is the minimal interface that `ResultPanel` needs to drive its
//! chrome (mode bar selection, focus delegation, keyboard context). Implementors
//! are also `impl Render` GPUI types; rendering goes through the standard GPUI
//! path and is NOT part of this trait.
//!
//! This file lives in `dbflux_ui` (not `dbflux_components`) to avoid circular
//! dependencies: `DataView` references `ContextId` and `ResultViewMode`, which
//! are defined in `dbflux_ui` and would require `dbflux_components` to depend
//! on `dbflux_ui` if moved there.
//!
//! # Current state (Arc 0 — stub)
//!
//! The trait is declared with its full intended surface. No implementations
//! exist yet; they are added in later arcs alongside each migrated document.

use super::result_view::ResultViewMode;
use crate::keymap::ContextId;
use gpui::{App, FocusHandle};

/// Minimum interface for a view component hosted inside `ResultPanel`.
///
/// Implementors are GPUI `Render` entities. `ResultPanel` drives its chrome
/// (mode bar, focus root, context ID forwarding) through this trait without
/// knowing the concrete view type.
///
/// Rendering is NOT part of the trait — each implementor is registered with
/// `ResultPanel` as an `AnyView`, and GPUI renders it through the standard
/// `Render` dispatch.
pub trait DataView: 'static {
    /// The set of result view modes this view supports.
    ///
    /// `ResultPanel` displays only the modes returned here in its mode bar.
    /// The slice must be non-empty and must include the view's default mode.
    fn available_view_modes(&self, cx: &App) -> &[ResultViewMode];

    /// The focus handle for this view's primary interactive region.
    ///
    /// `ResultPanel` delegates keyboard focus here when the document receives
    /// `FocusTarget::Document`.
    fn focus_handle(&self, cx: &App) -> FocusHandle;

    /// The active keyboard context for this view.
    ///
    /// Forwarded by `ResultPanel` to the enclosing document, which in turn
    /// reports it as its own `active_context`.
    fn active_context(&self, cx: &App) -> ContextId;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Structural compile-time test: the trait object is constructable and the
    /// method signatures are valid. This is a shape-only test; actual
    /// implementations come in later arcs.
    ///
    /// We verify the trait can be named and referenced as `dyn DataView`.
    #[test]
    fn data_view_trait_is_object_safe() {
        // `dyn DataView` must be object-safe (all methods take `&self` and
        // return sized values). If this compiles, the constraint is satisfied.
        let _: Option<Box<dyn DataView>> = None;
    }
}
