//! Shared composites that a product assembles from the primitives: the
//! panel headers and the control shell. Each is the single implementation
//! of its surface; a product reaches them through its own components crate.

pub mod control_shell;
pub mod header;
