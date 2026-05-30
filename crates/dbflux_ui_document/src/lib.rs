//! Document subsystem — tabs, panes, and all document types for DBFlux.
#![allow(unused_imports)]

mod add_member_modal;
mod audit;
pub mod chart;
pub mod chart_document;
mod chrome;
mod code;
pub mod dashboard;
mod data_document;
mod data_grid_panel;
mod data_view;
pub mod data_view_trait;
pub mod dedup;
mod style_guardrails;

#[cfg(feature = "mcp")]
mod governance;

mod handle;
pub mod history_modal;
pub mod instance_inspector;
mod key_value;
mod new_key_modal;
pub mod pane;
pub mod refresh;
mod result_view;
pub mod tab_bar;
mod tab_manager;
mod task_runner;
mod types;

pub use instance_inspector::InspectorPanel;

pub use audit::AuditDocument;
pub use chart_document::ChartDocument;
pub use code::CodeDocument;
pub use dashboard::{DashboardDocument, DashboardPanelSlot, PanelGridPos};
pub use data_document::DataDocument;
pub use data_grid_panel::{DataGridEvent, DataGridPanel, DataSource};
pub use data_view::{DataViewConfig, DataViewMode};
pub use data_view_trait::DataView;

#[cfg(feature = "mcp")]
pub use governance::McpApprovalsView;

pub use dedup::DocumentKey;
pub use handle::DocumentEvent;
pub use key_value::KeyValueDocument;
pub use pane::{BoxedDocEventCallback, CodeSessionTabSnapshot, PaneHandle};
pub use result_view::ResultViewMode;
pub use tab_bar::{TabBar, TabBarEvent};
pub use tab_manager::{Tab, TabManager, TabManagerEvent};
pub use task_runner::DocumentTaskRunner;
pub use types::{
    DataSourceKind, DocumentIcon, DocumentId, DocumentKind, DocumentMetaSnapshot, DocumentState,
};
