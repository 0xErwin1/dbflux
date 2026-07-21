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
