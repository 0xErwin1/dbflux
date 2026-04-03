//! `dbflux_ui` — GPUI UI layer for DBFlux.
//!
//! This crate contains all GPUI-dependent code:
//! - UI components, views, overlays, and windows
//! - The `AppStateEntity` wrapper
//! - IPC server and platform utilities
//! - Keymap actions and dispatcher

pub mod app;
pub mod app_state_entity;
pub mod assets;
pub mod ipc_server;
pub mod keymap;
pub mod platform;
pub mod ui;

// Re-exports for convenience
#[cfg(feature = "mcp")]
pub use app_state_entity::McpRuntimeEventRaised;
pub use app_state_entity::{AppStateChanged, AppStateEntity, AuthProfileCreated};
