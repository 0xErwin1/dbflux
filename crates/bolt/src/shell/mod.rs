//! The master-detail shell primitives the products build by hand: the
//! activity rail of view buttons at the left edge of the main surface.
//!
//! The module owns only what the products genuinely share. It never knows
//! what a rail entry opens: the host lists entries from its own state and
//! wires the handler.
//!
//! # Convergence record
//!
//! This module once also carried a `HeaderStrip`, a `NavRow` and a
//! `MasterDetail` frame. Each was deleted because it could not reproduce
//! the surfaces the products already render, and a surviving primitive
//! supersedes it:
//!
//! - The header strip is superseded by the `panel_header` /
//!   `panel_header_with_actions` composite in [`crate::composites`]: the
//!   call sites chain on the returned `Div`, and its actions row and its
//!   title-overflow behavior differ from the strip's.
//! - The nav row is superseded by the products' `ListRow` row primitive and
//!   their tree gutter: the call sites lay out and chain on the row
//!   themselves, and `ListRow` carries the focused-cursor state the nav row
//!   lacked.
//! - The master-detail frame is superseded by the products'
//!   `render_master_detail_list` panel: it renders the list's rows, toolbar
//!   and empty state, which the bare frame could not reproduce.
//!
//! The activity rail absorbed its duplicate instead: the products'
//! `ActivityRail` is a thin adapter that forwards to [`Rail`] with its own
//! entry type and key context, so the rail has one implementation.

mod rail;

pub use rail::{Rail, RailButtonColors, RailEntry, RailPlacement, rail_button_colors};
