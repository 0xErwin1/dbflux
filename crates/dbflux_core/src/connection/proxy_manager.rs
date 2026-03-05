use crate::ProxyProfile;
use crate::connection::item_manager::{DefaultFilename, ItemManager};

pub type ProxyManager = ItemManager<ProxyProfile>;

impl DefaultFilename for ProxyManager {
    fn meta() -> (&'static str, &'static str) {
        ("proxies.json", "proxy profiles")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProxyAuth, ProxyKind, ProxyProfile, ProxyStore};

    fn temp_manager() -> (tempfile::TempDir, ProxyManager) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proxies.json");
        let store = ProxyStore::from_path(path);
        (
            dir,
            ProxyManager::with_store(store, "proxy profiles").unwrap(),
        )
    }

    fn sample_proxy(name: &str) -> ProxyProfile {
        ProxyProfile {
            id: uuid::Uuid::new_v4(),
            name: name.to_string(),
            kind: ProxyKind::Http,
            host: "proxy.local".to_string(),
            port: 8080,
            auth: ProxyAuth::None,
            no_proxy: None,
            enabled: true,
            save_secret: false,
        }
    }

    #[test]
    fn add_persists_to_disk() {
        let (dir, mut mgr) = temp_manager();
        let proxy = sample_proxy("Corporate Proxy");
        let id = proxy.id;

        mgr.add(proxy);
        assert_eq!(mgr.items.len(), 1);

        let fresh_store = ProxyStore::from_path(dir.path().join("proxies.json"));
        let loaded = fresh_store.load().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, id);
    }

    #[test]
    fn remove_by_index() {
        let (_dir, mut mgr) = temp_manager();
        let p0 = sample_proxy("Zero");
        let p1 = sample_proxy("One");
        let p2 = sample_proxy("Two");
        let id0 = p0.id;
        let id2 = p2.id;

        mgr.add(p0);
        mgr.add(p1);
        mgr.add(p2);
        assert_eq!(mgr.items.len(), 3);

        let removed = mgr.remove(1);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().name, "One");
        assert_eq!(mgr.items.len(), 2);
        assert_eq!(mgr.items[0].id, id0);
        assert_eq!(mgr.items[1].id, id2);
    }

    #[test]
    fn remove_out_of_bounds_noop() {
        let (_dir, mut mgr) = temp_manager();
        mgr.add(sample_proxy("Only"));

        let result = mgr.remove(99);
        assert!(result.is_none());
        assert_eq!(mgr.items.len(), 1);
    }

    #[test]
    fn update_existing_proxy() {
        let (dir, mut mgr) = temp_manager();
        let mut proxy = sample_proxy("Original");
        let id = proxy.id;
        mgr.add(proxy.clone());

        proxy.name = "Updated".to_string();
        proxy.host = "new-proxy.local".to_string();
        mgr.update(proxy);

        assert_eq!(mgr.items[0].name, "Updated");
        assert_eq!(mgr.items[0].host, "new-proxy.local");

        let fresh_store = ProxyStore::from_path(dir.path().join("proxies.json"));
        let loaded = fresh_store.load().unwrap();
        assert_eq!(loaded[0].id, id);
        assert_eq!(loaded[0].name, "Updated");
    }

    #[test]
    fn update_nonexistent_is_noop() {
        let (dir, mut mgr) = temp_manager();
        mgr.add(sample_proxy("Existing"));

        let ghost = sample_proxy("Ghost");
        mgr.update(ghost);

        assert_eq!(mgr.items.len(), 1);
        assert_eq!(mgr.items[0].name, "Existing");

        let fresh_store = ProxyStore::from_path(dir.path().join("proxies.json"));
        let loaded = fresh_store.load().unwrap();
        assert_eq!(loaded.len(), 1);
    }
}
