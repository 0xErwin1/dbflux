//! Accessibility checks for rendered surfaces, moved to `bolt`.
//!
//! The checks themselves live in `bolt::accessibility`; this module
//! re-exports them at the crate's `accessibility` path so call sites keep
//! compiling. It exists under the same gate as before (`test` or the crate's
//! `test-support` feature), which `bolt` answers through its own
//! `test-support` feature.

pub use bolt::accessibility::*;
