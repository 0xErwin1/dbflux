//! dbflux_ui_base — app-state-aware UI base layer.
//!
//! This crate holds types that require both `dbflux_app` domain knowledge and
//! GPUI integration, sitting between the pure `dbflux_components` layer and the
//! full `dbflux_ui` crate.

pub mod app_state_entity;
pub mod keymap;

#[cfg(feature = "mcp")]
pub use app_state_entity::McpRuntimeEventRaised;
pub use app_state_entity::{AppStateChanged, AppStateEntity, AuthProfileCreated};
pub use keymap::{default_keymap, key_chord_from_gpui};
