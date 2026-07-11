//! Schema diff and DDL apply subsystem.
//!
//! `apply` executes the DDL statements generated for a reviewed schema diff.
//! The document, pane, and view live here as the subsystem grows.

pub mod apply;
