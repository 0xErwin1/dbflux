#![allow(dead_code)]

use uuid::Uuid;

/// Unique identifier for a document.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct DocumentId(pub Uuid);

impl DocumentId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for DocumentId {
    fn default() -> Self {
        Self::new()
    }
}

/// Supported document types.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DocumentKind {
    SqlQuery,
    TableView,
    // v0.3.2+ (Redis)
    RedisKeyBrowser,
    RedisKey,
    RedisConsole,
    // v0.3.3+ (MongoDB)
    MongoCollection,
}

/// Document icon (enum for type-safety).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DocumentIcon {
    Sql,
    Table,
    Redis,
    RedisKey,
    Terminal,
    Mongo,
    Collection,
}

impl DocumentIcon {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Sql => "file-code",
            Self::Table => "table",
            Self::Redis => "database",
            Self::RedisKey => "key",
            Self::Terminal => "terminal",
            Self::Mongo => "database",
            Self::Collection => "folder",
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
