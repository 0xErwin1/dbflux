#![allow(unused_imports)]

mod add_member_modal;
mod audit;
mod chrome;
mod code;
mod data_document;
mod data_grid_panel;
mod data_view;

#[cfg(feature = "mcp")]
mod governance;

mod handle;
mod key_value;
mod new_key_modal;
mod result_view;
pub mod tab_bar;
mod tab_manager;
mod task_runner;
mod types;

pub use audit::AuditDocument;
pub use code::CodeDocument;
pub use data_document::DataDocument;
pub use data_grid_panel::{DataGridEvent, DataGridPanel, DataSource};
pub use data_view::{DataViewConfig, DataViewMode};

#[cfg(feature = "mcp")]
pub use governance::{McpApprovalsView, McpAuditView};

pub use handle::{DocumentEvent, DocumentHandle};
pub use key_value::{KeyValueDocument, KeyValueDocumentEvent};
pub use result_view::ResultViewMode;
pub use tab_bar::{TabBar, TabBarEvent};
pub use tab_manager::{TabManager, TabManagerEvent};
pub use task_runner::DocumentTaskRunner;
pub use types::{
    DataSourceKind, DocumentIcon, DocumentId, DocumentKind, DocumentMetaSnapshot, DocumentState,
};
