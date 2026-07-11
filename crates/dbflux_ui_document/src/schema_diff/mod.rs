//! Schema diff and DDL apply subsystem.
//!
//! `apply` executes the DDL statements generated for a reviewed schema diff.
//! The document, pane, and view live here as the subsystem grows.

pub mod apply;
pub mod diff_source;
pub mod pane;
pub mod view;

pub use diff_source::{DiffMode, RiskBadge, SourcePicker};
pub use view::SchemaDiffDocument;
