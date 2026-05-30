//! `AppStateEntity` — GPUI entity wrapper for `AppState`.
//!
//! This module provides `AppStateEntity`, which wraps the pure `AppState` from `dbflux_app`
//! and adds GPUI-specific state (like the settings window handle) and event types.

use std::sync::Arc;

use dbflux_app::AppState;
use dbflux_core::observability::EventSeverity;
use dbflux_storage::bootstrap::StorageRuntime;
use gpui::{Entity, EventEmitter, Global, WindowHandle};
use gpui_component::Root;
use uuid::Uuid;

use crate::dashboard_manager::DashboardManager;
use crate::saved_chart_manager::SavedChartManager;

// ============================================================================
// GPUI-coupled event types
// ============================================================================

/// Emitted when the app state changes in ways that require UI updates.
pub struct AppStateChanged;

/// Emitted each time `report_error` fires for a new user-facing failure.
///
/// Subscribers (e.g., `StatusBar`) use this to update the unread-error badge
/// without depending on `AppStateChanged` polling.
#[derive(Clone, Copy)]
pub struct UserErrorReported {
    pub correlation_id: Uuid,
    pub severity: EventSeverity,
}

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
/// - `SavedChartManager` — SQLite-backed chart cache
/// - `DashboardManager` — SQLite-backed dashboard cache
///
/// This struct implements `Deref<Target=AppState>` so all `AppState` methods
/// are directly accessible via the wrapper.
pub struct AppStateEntity {
    /// Inner application state (pure, no GPUI dependencies).
    pub inner: AppState,

    /// Handle to the settings window, if one is open.
    /// Used to focus/reuse an existing settings window rather than opening multiple.
    pub settings_window: Option<WindowHandle<Root>>,

    /// Saved-chart manager — loaded from SQLite on startup; mutated via `upsert`/`remove`.
    pub saved_charts: SavedChartManager,

    /// Dashboard manager — loaded from SQLite on startup; mutated via `upsert_dashboard`
    /// and `replace_panels`.
    pub dashboards: DashboardManager,

    /// Set by the Connection Manager after editing a profile that is currently
    /// connected. The sidebar consumes this on the next `AppStateChanged` to
    /// surface a "Reconnect now / Later" prompt — the edit itself is already
    /// persisted regardless of the user's choice.
    pub pending_edit_reconnect_prompt: Option<Uuid>,

    /// Set by the reconnect prompt action when the user chooses to reconnect.
    /// Picked up by the sidebar on `AppStateChanged` to drive
    /// disconnect + connect for that profile.
    pub pending_reconnect_request: Option<Uuid>,

    /// Count of user-facing errors reported since the last `clear_unread_errors`
    /// call. Ephemeral — resets to 0 on every app start. The audit log is the
    /// durable record; this counter only drives the status-bar badge.
    pub unread_error_count: u32,
}

impl AppStateEntity {
    /// Creates a new `AppStateEntity` wrapping a fresh `AppState`.
    ///
    /// Repositories are read from the `AppState`'s internally constructed storage
    /// runtime. This path is used in production where the default DB location is
    /// used (`~/.local/share/dbflux/dbflux.db`).
    pub fn new() -> Self {
        let inner = AppState::new();

        let saved_charts = SavedChartManager::new(Arc::clone(&inner.saved_charts_repo));
        let dashboards = DashboardManager::new(
            Arc::clone(&inner.dashboards_repo),
            Arc::clone(&inner.dashboard_panels_repo),
        );

        Self {
            inner,
            settings_window: None,
            saved_charts,
            dashboards,
            pending_edit_reconnect_prompt: None,
            pending_reconnect_request: None,
            unread_error_count: 0,
        }
    }

    /// Creates a new `AppStateEntity` with a caller-provided storage runtime.
    ///
    /// The provided `StorageRuntime` is passed to `AppState`, which runs
    /// migrations and opens the viz connection. Managers are then constructed
    /// from the resulting repository `Arc`s.
    pub fn new_with_storage_runtime(storage_runtime: StorageRuntime) -> Self {
        let inner = AppState::new_with_storage_runtime(storage_runtime);

        let saved_charts = SavedChartManager::new(Arc::clone(&inner.saved_charts_repo));
        let dashboards = DashboardManager::new(
            Arc::clone(&inner.dashboards_repo),
            Arc::clone(&inner.dashboard_panels_repo),
        );

        Self {
            inner,
            settings_window: None,
            saved_charts,
            dashboards,
            pending_edit_reconnect_prompt: None,
            pending_reconnect_request: None,
            unread_error_count: 0,
        }
    }

    /// Increments the unread-error counter and notifies subscribers.
    ///
    /// Called exclusively by `report_error` — no other path should increment
    /// the counter to avoid double-counting.
    pub fn note_user_error(
        &mut self,
        correlation_id: Uuid,
        severity: EventSeverity,
        cx: &mut gpui::Context<Self>,
    ) {
        self.unread_error_count = self.unread_error_count.saturating_add(1);
        cx.emit(UserErrorReported {
            correlation_id,
            severity,
        });
        cx.emit(AppStateChanged);
        cx.notify();
    }

    /// Resets the unread-error counter to zero.
    ///
    /// Called when the user opens the audit panel via the badge click,
    /// acknowledging all pending error notifications.
    pub fn clear_unread_errors(&mut self, cx: &mut gpui::Context<Self>) {
        if self.unread_error_count == 0 {
            return;
        }
        self.unread_error_count = 0;
        cx.emit(AppStateChanged);
        cx.notify();
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
impl EventEmitter<UserErrorReported> for AppStateEntity {}

#[cfg(feature = "mcp")]
impl EventEmitter<McpRuntimeEventRaised> for AppStateEntity {}

// ============================================================================
// AppStateGlobal — GPUI global wrapper for the AppStateEntity
// ============================================================================

/// GPUI global that holds the `AppStateEntity` handle so that `report_error`
/// can reach it without requiring callers to pass the entity explicitly.
///
/// Registered in workspace startup via `cx.set_global(AppStateGlobal { entity })`.
pub struct AppStateGlobal {
    pub entity: Entity<AppStateEntity>,
}

impl Global for AppStateGlobal {}
