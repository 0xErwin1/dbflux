//! Compatibility module re-exporting types from dbflux_app.
//!
//! This module exists to ease the transition of UI code that previously
//! used `crate::app::AppState` when it was in the dbflux crate.
//! New code should use `dbflux_app::AppState` directly or `AppStateEntity`
//! from the parent crate.

pub use dbflux_app::AppState;
pub use dbflux_core::ConnectedProfile;

// Re-export event types from the parent crate
pub use crate::app_state_entity::{
    AppStateChanged, AppStateEntity, AuthProfileCreated, McpRuntimeEventRaised,
};
