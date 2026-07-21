#![recursion_limit = "2048"]
//! `dbflux_ui` — GPUI UI layer for DBFlux.
//!
//! This crate contains all GPUI-dependent code:
//! - UI components, views, overlays, and windows
//! - The `AppStateEntity` wrapper
//! - IPC server and platform utilities
//! - Keymap actions and dispatcher

pub mod app;
pub mod assets;
pub mod ipc_server;
pub mod keymap;
pub mod ui;

// Re-exports for external consumers that previously used dbflux_ui::{platform, ui::theme}
pub use dbflux_components::theme;
pub use dbflux_ui_base::platform;

// Re-exports for convenience
#[cfg(feature = "mcp")]
pub use dbflux_ui_base::McpRuntimeEventRaised;
pub use dbflux_ui_base::{AppStateChanged, AppStateEntity, AuthProfileCreated};
