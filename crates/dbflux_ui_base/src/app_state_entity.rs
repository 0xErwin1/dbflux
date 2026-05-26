//! `AppStateEntity` — GPUI entity wrapper for `AppState`.
//!
//! This module provides `AppStateEntity`, which wraps the pure `AppState` from `dbflux_app`
//! and adds GPUI-specific state (like the settings window handle) and event types.

use dbflux_app::AppState;
use dbflux_components::SavedChartManager;
use dbflux_storage::bootstrap::StorageRuntime;
use gpui::{EventEmitter, WindowHandle};
use gpui_component::Root;
use uuid::Uuid;

// ============================================================================
// GPUI-coupled event types
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

    /// Saved-chart manager — load once at startup, mutated via `upsert`/`remove`.
    pub saved_charts: SavedChartManager,

    /// Set by the Connection Manager after editing a profile that is currently
    /// connected. The sidebar consumes this on the next `AppStateChanged` to
    /// surface a "Reconnect now / Later" prompt — the edit itself is already
    /// persisted regardless of the user's choice.
    pub pending_edit_reconnect_prompt: Option<Uuid>,

    /// Set by the reconnect prompt action when the user chooses to reconnect.
    /// Picked up by the sidebar on `AppStateChanged` to drive
    /// disconnect + connect for that profile.
    pub pending_reconnect_request: Option<Uuid>,
}

impl AppStateEntity {
    /// Creates a new `AppStateEntity` wrapping a fresh `AppState`.
    pub fn new() -> Self {
        Self {
            inner: AppState::new(),
            settings_window: None,
            saved_charts: SavedChartManager::load(),
            pending_edit_reconnect_prompt: None,
            pending_reconnect_request: None,
        }
    }

    /// Creates a new `AppStateEntity` with a caller-provided storage runtime.
    pub fn new_with_storage_runtime(storage_runtime: StorageRuntime) -> Self {
        Self {
            inner: AppState::new_with_storage_runtime(storage_runtime),
            settings_window: None,
            saved_charts: SavedChartManager::load(),
            pending_edit_reconnect_prompt: None,
            pending_reconnect_request: None,
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
// EventEmitter implementations — GPUI-coupled, must travel with the type
// ============================================================================

impl EventEmitter<AppStateChanged> for AppStateEntity {}
impl EventEmitter<AuthProfileCreated> for AppStateEntity {}

#[cfg(feature = "mcp")]
impl EventEmitter<McpRuntimeEventRaised> for AppStateEntity {}
