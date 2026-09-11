#![allow(clippy::result_large_err)]
//! Sandboxed JavaScript script runtime for mongosh-style multi-statement
//! scripts, embedding QuickJS via `rquickjs`.
//!
//! See `crates/dbflux_driver_mongodb` for the driver that routes JS-looking
//! input into [`engine::ScriptEngine`].

mod binding;
mod engine;
mod limits;
mod scan;

pub use engine::{ScriptEngineError, ScriptRunConfig, ScriptRunOutcome, run};
pub use scan::{StaticScanOutcome, static_scan};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
