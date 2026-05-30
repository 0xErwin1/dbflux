//! dbflux_ui_base — app-state-aware UI base layer.
//!
//! This crate holds types that require both `dbflux_app` domain knowledge and
//! GPUI integration, sitting between the pure `dbflux_components` layer and the
//! full `dbflux_ui` crate.

#![recursion_limit = "1024"]

pub mod app_state_entity;
pub mod async_ext;
pub mod dashboard_manager;
pub mod file_dialog;
pub mod keymap;
pub mod modal_frame;
pub mod modals;
pub mod platform;
pub mod saved_chart_manager;
pub mod sql_preview_modal;
pub mod sso_wizard;
pub mod toast;
pub mod user_error;

mod style_guardrails;

#[cfg(feature = "mcp")]
pub use app_state_entity::McpRuntimeEventRaised;
pub use app_state_entity::{
    AppStateChanged, AppStateEntity, AppStateGlobal, AuthProfileCreated, OpenAuditRequested,
    UserErrorReported,
};
pub use async_ext::AsyncUpdateResultExt;
pub use dashboard_manager::{
    Dashboard, DashboardManager, DashboardPanel, DashboardPanelDraft, DashboardPanelKind,
};
pub use keymap::{default_keymap, key_chord_from_gpui};
pub use saved_chart_manager::SavedChartManager;
pub use user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
