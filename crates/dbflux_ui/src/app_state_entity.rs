//! Compat shim: `AppStateEntity` and its event types now live in `dbflux_ui_base`.
#[cfg(feature = "mcp")]
pub use dbflux_ui_base::McpRuntimeEventRaised;
pub use dbflux_ui_base::{AppStateChanged, AppStateEntity, AuthProfileCreated};
