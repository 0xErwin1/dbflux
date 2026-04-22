//! Compatibility module re-exporting types from dbflux_app.
//!
//! This module exists to ease the transition of UI code that previously
//! used `crate::app::AppState` when it was in the dbflux crate.
//! New code should use `dbflux_app::AppState` directly or `AppStateEntity`
//! from the parent crate.

pub use dbflux_app::AppState;
pub use dbflux_core::ConnectedProfile;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalDriverStage {
    Config,
    Launch,
    Probe,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalDriverDiagnostic {
    pub socket_id: String,
    pub stage: ExternalDriverStage,
    pub summary: String,
    pub details: Option<String>,
}

// Re-export event types from the parent crate
pub use crate::app_state_entity::{
    AppStateChanged, AppStateEntity, AuthProfileCreated, McpRuntimeEventRaised,
};
