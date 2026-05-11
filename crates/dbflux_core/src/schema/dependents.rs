use serde::{Deserialize, Serialize};

/// The kind of object that depends on a given table.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RelationKind {
    View,
    MaterializedView,
    ForeignKeyChild,
    Trigger,
}

/// A reference to an object that depends on a specific table.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RelationRef {
    pub kind: RelationKind,
    /// Fully-qualified name of the dependent object (e.g. `public.user_summary`).
    pub qualified_name: String,
}
