//! `AppStateEntity` — GPUI entity wrapper for `AppState`.
//!
//! This module provides `AppStateEntity`, which wraps the pure `AppState` from `dbflux_app`
//! and adds GPUI-specific state (like the settings window handle) and event types.

use dbflux_app::AppState;
use gpui::{EventEmitter, WindowHandle};
use gpui_component::Root;
use uuid::Uuid;

// ============================================================================
// GPUI-coupled event types — moved from dbflux/src/app.rs
// ============================================================================

/// Emitted when the app state changes in ways that require UI updates.
pub struct AppStateChanged;

/// Emitted when an auth profile is created (used to update the sidebar).
#[derive(Clone)]
pub struct AuthProfileCreated {
    pub profile_id: Uuid,
}

/// Emitted when an MCP runtime event occurs.
#[cfg(feature = "mcp")]
#[derive(Clone)]
pub struct McpRuntimeEventRaised {
    #[allow(dead_code)]
    pub event: dbflux_mcp::McpRuntimeEvent,
}

// ============================================================================
// AppStateEntity — the main GPUI entity wrapping AppState
// ============================================================================

/// A GPUI entity wrapping `AppState` with additional GPUI-specific state.
///
/// `AppStateEntity` holds:
/// - The inner `AppState` (pure domain state)
/// - The settings window handle (to reuse a single settings window)
///
/// This struct implements `Deref<Target=AppState>` so all `AppState` methods
/// are directly accessible via the wrapper.
pub struct AppStateEntity {
    /// Inner application state (pure, no GPUI dependencies).
    pub inner: AppState,

    /// Handle to the settings window, if one is open.
    /// Used to focus/reuse an existing settings window rather than opening multiple.
    pub settings_window: Option<WindowHandle<Root>>,
}

impl AppStateEntity {
    /// Creates a new `AppStateEntity` wrapping a fresh `AppState`.
    pub fn new() -> Self {
        Self {
            inner: AppState::new(),
            settings_window: None,
        }
    }
}

impl Default for AppStateEntity {
    fn default() -> Self {
        Self::new()
    }
}

impl std::ops::Deref for AppStateEntity {
    type Target = AppState;

    fn deref(&self) -> &AppState {
        &self.inner
    }
}

impl std::ops::DerefMut for AppStateEntity {
    fn deref_mut(&mut self) -> &mut AppState {
        &mut self.inner
    }
}

// ============================================================================
// EventEmitter implementations — GPUI-coupled, must stay in dbflux_ui
// ============================================================================

impl EventEmitter<AppStateChanged> for AppStateEntity {}
impl EventEmitter<AuthProfileCreated> for AppStateEntity {}

#[cfg(feature = "mcp")]
impl EventEmitter<McpRuntimeEventRaised> for AppStateEntity {}
