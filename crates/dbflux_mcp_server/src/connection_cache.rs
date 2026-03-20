use std::collections::HashMap;
use std::sync::Arc;

use dbflux_core::Connection;

/// Caches live driver connections keyed by `connection_id` (profile UUID as string).
///
/// Connections are established lazily on first use and reused for subsequent calls.
/// The cache is single-threaded and lives for the lifetime of the server process.
pub struct ConnectionCache {
    inner: HashMap<String, Arc<dyn Connection>>,
}

impl ConnectionCache {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Returns the cached connection for `connection_id`, or `None` if not yet established.
    pub fn get(&self, connection_id: &str) -> Option<Arc<dyn Connection>> {
        self.inner.get(connection_id).cloned()
    }

    /// Inserts or replaces the connection for `connection_id`.
    pub fn insert(&mut self, connection_id: String, connection: Arc<dyn Connection>) {
        self.inner.insert(connection_id, connection);
    }
}
