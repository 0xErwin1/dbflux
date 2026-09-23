//! `AppStateEntity` — GPUI entity wrapper for `AppState`.
//!
//! This module provides `AppStateEntity`, which wraps the pure `AppState` from `dbflux_app`
//! and adds GPUI-specific state (like the settings window handle) and event types.

use std::path::PathBuf;
use std::sync::Arc;

use dbflux_app::{
    AppState, app_state::ScriptsDirectoryDiagnostic, config_loader::HookLoadDiagnostic,
};
use dbflux_core::observability::EventSeverity;
use dbflux_storage::bootstrap::StorageRuntime;
use gpui::{Entity, EventEmitter, Global, WindowHandle};
use gpui_component::Root;
use uuid::Uuid;

use crate::dashboard_manager::DashboardManager;
use crate::object_tree::{ObjectTreeCoordinator, ObjectTreeEvent};
use crate::saved_chart_manager::SavedChartManager;
use crate::saved_query_manager::SavedQueryManager;
use crate::schema_snapshot_manager::SchemaSnapshotManager;
use crate::user_error::{ErrorKind, UserFacingError};

/// Drains startup hook-load diagnostics into safe, actionable user-facing errors.
///
/// The durable row remains protected by the configuration loader; this boundary
/// deliberately omits diagnostic payload text to avoid exposing stored data.
pub fn drain_hook_load_diagnostics(
    diagnostics: &mut Vec<HookLoadDiagnostic>,
) -> Vec<UserFacingError> {
    std::mem::take(diagnostics)
        .into_iter()
        .map(|diagnostic| {
            let summary = match diagnostic.row_name {
                Some(row_name) => dbflux_i18n::t!(
                    "diagnostics.hook_load.repair_summary_named",
                    row_name = row_name,
                    row_id = diagnostic.row_id
                ),
                None => dbflux_i18n::t!(
                    "diagnostics.hook_load.repair_summary_unnamed",
                    row_id = diagnostic.row_id
                ),
            };

            UserFacingError::new(ErrorKind::Config, summary)
                .with_suggested_action(dbflux_i18n::t!("diagnostics.hook_load.repair_action"))
        })
        .collect()
}

/// Drains startup scripts-directory diagnostics into safe, actionable errors.
///
/// The recorded failure text includes filesystem paths, so this boundary
/// deliberately omits diagnostic payload detail to avoid exposing it.
pub fn drain_scripts_directory_diagnostics(
    diagnostics: &mut Vec<ScriptsDirectoryDiagnostic>,
) -> Vec<UserFacingError> {
    std::mem::take(diagnostics)
        .into_iter()
        .map(|_diagnostic| {
            UserFacingError::new(
                ErrorKind::Config,
                dbflux_i18n::t!("diagnostics.scripts_directory.summary"),
            )
            .with_suggested_action(dbflux_i18n::t!("diagnostics.scripts_directory.action"))
        })
        .collect()
}

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

/// Requests that the workspace open the audit document, optionally filtered
/// by a specific `correlation_id`.
///
/// Emitted by the status-bar badge click (with `None`, opens with the default
/// user-error filter) and by the "View in Audit" action on error toasts (with
/// the toast's correlation id). Keeping a single event keeps the audit-open
/// path uniform — the workspace subscribes once.
#[derive(Clone, Copy)]
pub struct OpenAuditRequested(pub Option<Uuid>);

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

/// Context passed to a [`SaveTargetProvider`] so a test or alternate UI can
/// decide where a Save As should write. Production leaves the provider unset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SaveTargetRequest<'a> {
    /// File name (with extension) suggested to the user.
    pub suggested_name: &'a str,
    /// Human-readable language/format name for picker filtering.
    pub language_name: &'a str,
    /// Default extension for the picker.
    pub default_extension: &'a str,
}

/// What a save-target picker did. Separating selection from fallback lets a
/// caller model the same states for native dialogs, test pickers, and the
/// no-native-dialog export fallback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SaveTargetOutcome {
    /// A concrete destination was selected.
    Selected { path: PathBuf, used_fallback: bool },
    /// The user dismissed the picker.
    Cancelled,
    /// No destination could be produced; the string is a user-facing message.
    Failed(String),
}

/// Overridable Save As implementation owned by a single [`AppStateEntity`].
///
/// The provider is per-entity, so tests do not mutate any process-global dialog
/// state. Production leaves it unset and uses the shared native/fallback
/// resolver in [`crate::file_dialog`].
pub type SaveTargetProvider =
    Arc<dyn for<'a> Fn(SaveTargetRequest<'a>) -> gpui::Task<SaveTargetOutcome> + Send + Sync>;

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

    /// Saved visual query manager — loaded from SQLite on startup; mutated via
    /// `save`, `rename`, `fork`, `delete`, and `import_to`.
    pub saved_queries: SavedQueryManager,

    /// Persisted schema-snapshot manager — mutated via `capture`/`capture_deep`
    /// (on-connect auto-capture and the explicit deep-capture path).
    pub schema_snapshots: SchemaSnapshotManager,

    /// Shared DB object-tree coordinator. ONE instance per entity: the
    /// sidebar and the wizard adapters both reach it through this entity, so
    /// identical requests dedupe into one driver execution and every
    /// subscriber is notified of each settle via [`ObjectTreeEvent`].
    pub object_tree: ObjectTreeCoordinator,

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

    pub hook_load_diagnostics: Vec<HookLoadDiagnostic>,

    pub scripts_directory_diagnostics: Vec<ScriptsDirectoryDiagnostic>,

    /// Optional per-entity Save As override. `None` preserves the normal
    /// native-dialog / fallback-export behavior.
    save_target_override: Option<SaveTargetProvider>,
}

impl AppStateEntity {
    /// Creates a new `AppStateEntity` wrapping a fresh `AppState`.
    ///
    /// Repositories are read from the `AppState`'s internally constructed storage
    /// runtime. This path is used in production where the default DB location is
    /// used (`~/.local/share/dbflux/dbflux.db`).
    pub fn new() -> Result<Self, dbflux_storage::error::StorageError> {
        let mut inner = AppState::new()?;

        let saved_charts = SavedChartManager::new(Arc::clone(&inner.saved_charts_repo));
        let dashboards = DashboardManager::new(
            Arc::clone(&inner.dashboards_repo),
            Arc::clone(&inner.dashboard_panels_repo),
        );
        let saved_queries = SavedQueryManager::new(Arc::clone(&inner.saved_query_repo));
        let schema_snapshots = SchemaSnapshotManager::new(Arc::clone(&inner.schema_snapshot_repo));
        let hook_load_diagnostics = inner.take_hook_load_diagnostics();
        let scripts_directory_diagnostics = inner.take_scripts_directory_diagnostics();

        Ok(Self {
            inner,
            settings_window: None,
            saved_charts,
            dashboards,
            saved_queries,
            schema_snapshots,
            object_tree: ObjectTreeCoordinator::default(),
            pending_edit_reconnect_prompt: None,
            pending_reconnect_request: None,
            unread_error_count: 0,
            hook_load_diagnostics,
            scripts_directory_diagnostics,
            save_target_override: None,
        })
    }

    /// Creates a new `AppStateEntity` with a caller-provided storage runtime.
    ///
    /// The provided `StorageRuntime` is passed to `AppState`, which runs
    /// migrations and opens the viz connection. Managers are then constructed
    /// from the resulting repository `Arc`s.
    pub fn new_with_storage_runtime(
        storage_runtime: StorageRuntime,
    ) -> Result<Self, dbflux_storage::error::StorageError> {
        let mut inner = AppState::new_with_storage_runtime(storage_runtime)?;

        let saved_charts = SavedChartManager::new(Arc::clone(&inner.saved_charts_repo));
        let dashboards = DashboardManager::new(
            Arc::clone(&inner.dashboards_repo),
            Arc::clone(&inner.dashboard_panels_repo),
        );
        let saved_queries = SavedQueryManager::new(Arc::clone(&inner.saved_query_repo));
        let schema_snapshots = SchemaSnapshotManager::new(Arc::clone(&inner.schema_snapshot_repo));
        let hook_load_diagnostics = inner.take_hook_load_diagnostics();
        let scripts_directory_diagnostics = inner.take_scripts_directory_diagnostics();

        Ok(Self {
            inner,
            settings_window: None,
            saved_charts,
            dashboards,
            saved_queries,
            schema_snapshots,
            object_tree: ObjectTreeCoordinator::default(),
            pending_edit_reconnect_prompt: None,
            pending_reconnect_request: None,
            unread_error_count: 0,
            hook_load_diagnostics,
            scripts_directory_diagnostics,
            save_target_override: None,
        })
    }

    /// Returns the per-entity Save As override, if one was installed for tests
    /// or an alternate embedding.
    pub fn save_target_override(&self) -> Option<SaveTargetProvider> {
        self.save_target_override.clone()
    }

    /// Installs a per-entity Save As override. Intended for tests and embeds;
    /// production leaves it unset and uses the shared native/fallback resolver.
    pub fn with_save_target_override(mut self, provider: SaveTargetProvider) -> Self {
        self.save_target_override = Some(provider);
        self
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

    /// Emits an `OpenAuditRequested` event so the workspace opens (or focuses)
    /// the audit document with the matching correlation filter.
    pub fn request_open_audit(
        &mut self,
        correlation_id: Option<Uuid>,
        cx: &mut gpui::Context<Self>,
    ) {
        cx.emit(OpenAuditRequested(correlation_id));
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
impl EventEmitter<ObjectTreeEvent> for AppStateEntity {}
impl EventEmitter<UserErrorReported> for AppStateEntity {}
impl EventEmitter<OpenAuditRequested> for AppStateEntity {}

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

#[cfg(test)]
mod tests {
    use super::*;

    fn diagnostic(row_id: &str, row_name: Option<&str>, message: &str) -> HookLoadDiagnostic {
        HookLoadDiagnostic {
            row_id: row_id.to_string(),
            row_name: row_name.map(str::to_string),
            message: message.to_string(),
        }
    }

    fn scripts_diagnostic(message: &str) -> ScriptsDirectoryDiagnostic {
        ScriptsDirectoryDiagnostic {
            message: message.to_string(),
        }
    }

    #[test]
    fn draining_no_hook_load_diagnostics_emits_no_errors() {
        let mut diagnostics = Vec::new();

        let errors = drain_hook_load_diagnostics(&mut diagnostics);

        assert!(errors.is_empty());
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn draining_hook_load_diagnostics_reports_repairable_safe_context_once() {
        let secret_payload = r#"{\"token\":\"do-not-expose\"}"#;
        let mut diagnostics = vec![diagnostic(
            "legacy-row-42",
            Some("Nightly backup"),
            secret_payload,
        )];

        let errors = drain_hook_load_diagnostics(&mut diagnostics);

        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].kind, crate::user_error::ErrorKind::Config);
        let expected_summary = dbflux_i18n::t!(
            "diagnostics.hook_load.repair_summary_named",
            row_name = "Nightly backup",
            row_id = "legacy-row-42"
        );
        let expected_action = dbflux_i18n::t!("diagnostics.hook_load.repair_action");

        assert_eq!(errors[0].summary, expected_summary);
        assert_eq!(
            errors[0].suggested_action.as_deref(),
            Some(expected_action.as_str())
        );
        assert!(!errors[0].summary.contains(secret_payload));
        assert!(
            !errors[0]
                .suggested_action
                .as_deref()
                .unwrap_or_default()
                .contains(secret_payload)
        );
        assert!(diagnostics.is_empty());

        assert!(drain_hook_load_diagnostics(&mut diagnostics).is_empty());
    }

    #[test]
    fn draining_hook_load_diagnostic_without_name_keeps_row_id_context() {
        let mut diagnostics = vec![diagnostic("legacy-row-43", None, "invalid payload")];

        let errors = drain_hook_load_diagnostics(&mut diagnostics);

        assert_eq!(errors.len(), 1);
        assert_eq!(
            errors[0].summary,
            dbflux_i18n::t!(
                "diagnostics.hook_load.repair_summary_unnamed",
                row_id = "legacy-row-43"
            )
        );
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn draining_no_scripts_directory_diagnostics_emits_no_errors() {
        let mut diagnostics = Vec::new();

        let errors = drain_scripts_directory_diagnostics(&mut diagnostics);

        assert!(errors.is_empty());
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn draining_scripts_directory_diagnostic_reports_degradation_once() {
        let secret_path = "/home/someone/private-dir/dbflux/scripts";
        let mut diagnostics = vec![scripts_diagnostic(secret_path)];

        let errors = drain_scripts_directory_diagnostics(&mut diagnostics);

        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].kind, crate::user_error::ErrorKind::Config);
        let expected_summary = dbflux_i18n::t!("diagnostics.scripts_directory.summary");
        let expected_action = dbflux_i18n::t!("diagnostics.scripts_directory.action");

        assert_eq!(errors[0].summary, expected_summary);
        assert_eq!(
            errors[0].suggested_action.as_deref(),
            Some(expected_action.as_str())
        );
        assert!(!errors[0].summary.contains(secret_path));
        assert!(
            !errors[0]
                .suggested_action
                .as_deref()
                .unwrap_or_default()
                .contains(secret_path)
        );
        assert!(diagnostics.is_empty());

        assert!(drain_scripts_directory_diagnostics(&mut diagnostics).is_empty());
    }
}
