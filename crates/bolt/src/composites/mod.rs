//! Shared composites that a product assembles from the primitives: the
//! panel headers, the control shell and the list row. Each is the single
//! implementation of its surface; a product reaches them through its own
//! components crate.

pub mod control_shell;
pub mod header;
pub mod list_row;
