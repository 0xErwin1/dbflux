#![allow(dead_code)]

use uuid::Uuid;

pub use dbflux_core::document_id::DocumentId;

/// Supported document types.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DocumentKind {
    /// SQL script with editor + embedded results.
    Script,
    /// Data grid (table browser or promoted result).
    Data,
    // Legacy (kept for compatibility during migration)
    SqlQuery,
    TableView,
    // v0.4+ (Redis)
    RedisKeyBrowser,
    RedisKey,
    RedisConsole,
    // v0.5+ (MongoDB)
    MongoCollection,
    // Global audit viewer
    Audit,
    // Standalone chart document
    Chart,
    // Dashboard document (named collection of chart panels)
    Dashboard,
}

/// Source kind for DataDocument (affects icon and behavior).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum DataSourceKind {
    /// Table browser (server-side pagination).
    #[default]
    Table,
    Collection,
    /// Promoted query result (static data).
    QueryResult,
}

/// Document icon (enum for type-safety).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DocumentIcon {
    Sql,
    Script,
    Table,
    Redis,
    RedisKey,
    Terminal,
    Mongo,
    Collection,
    Audit,
    Chart,
    Dashboard,
}

impl DocumentIcon {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Sql => "file-code",
            Self::Script => "file-text",
            Self::Table => "table",
            Self::Redis => "database",
            Self::RedisKey => "key",
            Self::Terminal => "terminal",
            Self::Mongo => "database",
            Self::Collection => "folder",
            Self::Audit => "shield",
            Self::Chart => "bar-chart-2",
            Self::Dashboard => "layout-dashboard",
        }
    }
}

/// Document state.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum DocumentState {
    #[default]
    Clean,
    Modified,
    Executing,
    Loading,
    Error,
}

/// Metadata snapshot for TabBar (cheap Clone).
#[derive(Clone, Debug)]
pub struct DocumentMetaSnapshot {
    pub id: DocumentId,
    pub kind: DocumentKind,
    pub title: String,
    pub icon: DocumentIcon,
    pub state: DocumentState,
    pub closable: bool,
    pub connection_id: Option<Uuid>,
}
