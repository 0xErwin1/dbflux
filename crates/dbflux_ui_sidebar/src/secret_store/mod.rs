//! The stores of a secret-store connection (`DatabaseCategory::SecretStore`)
//! and the path tree of its hierarchical stores.
//!
//! Each store a connection exposes gets one node, labelled from its
//! descriptor. A store that advertises `SecretStoreCapabilities::HIERARCHICAL`
//! expands one level at a time: a level is listed through
//! `SecretStoreSource::list`, which never fetches a value, and only its
//! sub-paths are kept. The secrets themselves are listed by the store's
//! document, never in the sidebar.

use crate::Sidebar;
use dbflux_core::{
    DbError, SchemaNodeId, SecretListRequest, SecretStoreCapabilities, SecretStoreDescriptor,
    SecretStoreSource, TaskKind,
};
use gpui::{Context, Task};
use gpui_component::tree::TreeItem;
use std::collections::HashMap;
use uuid::Uuid;

/// One listed level of a hierarchical store: `path` is `None` for its root.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SecretPathKey {
    pub(crate) profile_id: Uuid,
    pub(crate) store_id: String,
    pub(crate) path: Option<String>,
}

impl SecretPathKey {
    pub(crate) fn new(profile_id: Uuid, store_id: &str, path: Option<&str>) -> Self {
        Self {
            profile_id,
            store_id: store_id.to_string(),
            path: path.map(str::to_string),
        }
    }
}

/// The sub-paths listed for each level, in the order the store returned them.
pub(crate) type SecretPathCache = HashMap<SecretPathKey, Vec<String>>;

/// In-flight level listings, keyed like the cache.
pub(crate) type PendingSecretPathFetches = HashMap<SecretPathKey, Task<()>>;

/// One node per store, in the connection's order. A hierarchical store
/// carries its root level: the listed sub-paths, or a loading row until the
/// level is listed. A flat store has no children.
pub(crate) fn build_secret_store_nodes(
    profile_id: Uuid,
    stores: &[SecretStoreDescriptor],
    cache: &SecretPathCache,
) -> Vec<TreeItem> {
    stores
        .iter()
        .map(|store| {
            let id = SchemaNodeId::SecretStore {
                profile_id,
                store_id: store.id.clone(),
            }
            .to_string();

            let node = TreeItem::new(id, store.label.clone());

            if !store.supports(SecretStoreCapabilities::HIERARCHICAL) {
                return node;
            }

            let key = SecretPathKey::new(profile_id, &store.id, None);
            node.children(build_path_children(&key, cache))
        })
        .collect()
}

fn build_path_children(key: &SecretPathKey, cache: &SecretPathCache) -> Vec<TreeItem> {
    let Some(child_paths) = cache.get(key) else {
        let level = key.path.as_deref().unwrap_or_default();
        return vec![Sidebar::loading_placeholder(
            key.profile_id,
            &format!("{}{}", key.store_id, level),
            "secret-paths-loading",
        )];
    };

    child_paths
        .iter()
        .map(|path| {
            let id = SchemaNodeId::SecretPath {
                profile_id: key.profile_id,
                store_id: key.store_id.clone(),
                path: path.clone(),
            }
            .to_string();

            let child_key = SecretPathKey::new(key.profile_id, &key.store_id, Some(path));

            TreeItem::new(id, path_label(path)).children(build_path_children(&child_key, cache))
        })
        .collect()
}

/// A path as the tree shows it: the full path without its trailing
/// separator (`/app/prod/` reads `/app/prod`).
fn path_label(path: &str) -> String {
    match path.strip_suffix('/') {
        Some(trimmed) if !trimmed.is_empty() => trimmed.to_string(),
        _ => path.to_string(),
    }
}

/// Lists every page of one level and returns its sub-paths, each once, in
/// the order they first appeared. Entries are dropped: the sidebar only
/// shows the path tree.
fn list_child_paths(
    store: &dyn SecretStoreSource,
    path: Option<&str>,
) -> Result<Vec<String>, String> {
    let mut child_paths: Vec<String> = Vec::new();
    let mut page_token = None;

    loop {
        let request = SecretListRequest {
            path: path.map(str::to_string),
            page_token: page_token.take(),
            ..SecretListRequest::default()
        };

        let page = store.list(&request).map_err(|error| error.to_string())?;

        for child in page.child_paths {
            if !child_paths.contains(&child) {
                child_paths.push(child);
            }
        }

        match page.next_page_token {
            Some(token) => page_token = Some(token),
            None => return Ok(child_paths),
        }
    }
}

impl Sidebar {
    /// Lists one level of a hierarchical store for the path tree.
    /// Deduplicated per level, so an expand/collapse burst lists it once.
    pub(crate) fn spawn_fetch_secret_paths(&mut self, key: SecretPathKey, cx: &mut Context<Self>) {
        if self.secret_path_cache.contains_key(&key)
            || self.pending_secret_path_fetches.contains_key(&key)
        {
            return;
        }

        let Some((connection, profile_name)) = self
            .app_state
            .read(cx)
            .connections()
            .get(&key.profile_id)
            .map(|connected| (connected.connection.clone(), connected.profile.name.clone()))
        else {
            return;
        };

        let app_state = self.app_state.clone();
        let load_task_id = app_state.update(cx, |state, _| {
            let (task_id, _) = state.start_task_for_profile(
                TaskKind::LoadSchema,
                crate::labels::listing_secret_paths_task_label(&profile_name),
                Some(key.profile_id),
            );
            state.tasks_mut().mark_uncancellable(task_id);
            task_id
        });

        let sidebar = cx.entity().clone();

        let background_task = cx.background_executor().spawn({
            let key = key.clone();
            async move {
                let store = connection.secret_store(&key.store_id).ok_or_else(|| {
                    DbError::NotSupported(format!("secret store {}", key.store_id)).to_string()
                })?;

                list_child_paths(store, key.path.as_deref())
            }
        });

        let task = cx.spawn({
            let key = key.clone();
            async move |_this, cx| {
                let result = background_task.await;

                cx.update(|cx| {
                    match &result {
                        Ok(_) => {
                            app_state.update(cx, |state, _| state.complete_task(load_task_id));
                        }
                        Err(message) => {
                            let details = crate::labels::list_secret_paths_failed_label(message);
                            app_state.update(cx, |state, _| {
                                state.fail_task_with_details(
                                    load_task_id,
                                    message.clone(),
                                    details,
                                );
                            });
                        }
                    }

                    sidebar.update(cx, |sidebar, cx| {
                        sidebar.pending_secret_path_fetches.remove(&key);

                        match result {
                            Ok(child_paths) => {
                                sidebar.secret_path_cache.insert(key, child_paths);
                            }
                            Err(message) => {
                                log::warn!(
                                    "Failed to list secret paths for {}: {}",
                                    key.profile_id,
                                    message
                                );
                            }
                        }

                        sidebar.rebuild_tree_with_overrides(cx);
                    });
                });
            }
        });

        self.pending_secret_path_fetches.insert(key, task);
    }

    /// Drops every listed level of `profile_id`, so the next expansion lists
    /// them again.
    pub(crate) fn clear_secret_path_cache(&mut self, profile_id: Uuid) {
        self.secret_path_cache
            .retain(|key, _| key.profile_id != profile_id);
        self.pending_secret_path_fetches
            .retain(|key, _| key.profile_id != profile_id);
    }
}

#[cfg(test)]
mod tests;
