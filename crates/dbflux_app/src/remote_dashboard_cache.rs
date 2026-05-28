//! Session-scoped in-memory cache for upstream dashboard listings.
//!
//! Stores the `DashboardRef` list returned by `DashboardSource::list_dashboards`
//! keyed by connection profile UUID. Mutation happens through short-lived
//! `std::sync::Mutex` critical sections (HashMap insert/remove only; no IO
//! inside the lock).
//!
//! # Lifecycle
//!
//! Created once when `AppState` is constructed and held as
//! `Arc<RemoteDashboardCache>`. NOT persisted across restarts. Dashboards are
//! browsed read-only, so this cache holds only the listing, never a dashboard
//! body — bodies are fetched on demand each time a dashboard is opened.
//!
//! # Fetch dispatch
//!
//! The cache holds only cached data and invalidation. The actual driver call
//! (`DashboardSource::list_dashboards`) happens in the sidebar GPUI layer,
//! which resolves the connection from `AppStateEntity`, spawns a background
//! task, writes the result via `store`, and calls `cx.notify()`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dbflux_core::DashboardRef;
use uuid::Uuid;

/// Session-scoped cache for upstream dashboard listings.
///
/// Thread-safe via `std::sync::Mutex`. Critical sections are O(1) HashMap
/// operations only — no IO or driver calls inside the lock.
pub struct RemoteDashboardCache {
    inner: Mutex<HashMap<Uuid, Arc<Vec<DashboardRef>>>>,
}

impl RemoteDashboardCache {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
        })
    }

    /// Return the cached dashboard listing for `profile_id`, if a fetch has
    /// completed. `None` means the caller should spawn a fetch and `store` it.
    pub fn peek(&self, profile_id: Uuid) -> Option<Arc<Vec<DashboardRef>>> {
        self.inner
            .lock()
            .expect("RemoteDashboardCache lock poisoned")
            .get(&profile_id)
            .cloned()
    }

    /// Store a completed dashboard listing.
    pub fn store(&self, profile_id: Uuid, dashboards: Vec<DashboardRef>) {
        self.inner
            .lock()
            .expect("RemoteDashboardCache lock poisoned")
            .insert(profile_id, Arc::new(dashboards));
    }

    /// Remove the cached listing for `profile_id` (on disconnect or refresh).
    pub fn invalidate(&self, profile_id: Uuid) {
        self.inner
            .lock()
            .expect("RemoteDashboardCache lock poisoned")
            .remove(&profile_id);
    }
}

impl Default for RemoteDashboardCache {
    fn default() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dref(name: &str) -> DashboardRef {
        DashboardRef {
            name: name.to_string(),
            last_modified: None,
        }
    }

    #[test]
    fn peek_returns_none_before_any_fetch() {
        let cache = RemoteDashboardCache::new();
        assert!(cache.peek(Uuid::new_v4()).is_none());
    }

    #[test]
    fn store_then_peek_returns_listing() {
        let cache = RemoteDashboardCache::new();
        let id = Uuid::new_v4();
        cache.store(id, vec![dref("prod"), dref("staging")]);

        let listing = cache.peek(id).expect("listing present");
        assert_eq!(listing.len(), 2);
        assert_eq!(listing[0].name, "prod");
    }

    #[test]
    fn invalidate_clears_only_target_profile() {
        let cache = RemoteDashboardCache::new();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        cache.store(a, vec![dref("a")]);
        cache.store(b, vec![dref("b")]);

        cache.invalidate(a);

        assert!(cache.peek(a).is_none());
        assert!(cache.peek(b).is_some());
    }

    #[test]
    fn store_overwrites_previous_listing() {
        let cache = RemoteDashboardCache::new();
        let id = Uuid::new_v4();
        cache.store(id, vec![dref("old")]);
        cache.store(id, vec![dref("new1"), dref("new2")]);

        let listing = cache.peek(id).unwrap();
        assert_eq!(listing.len(), 2);
        assert_eq!(listing[0].name, "new1");
    }
}
