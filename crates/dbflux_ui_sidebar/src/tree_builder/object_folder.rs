//! The content of a database whose objects sit in one folder, walked from the
//! [`ObjectFolder`] the schema snapshot declares rather than from its paradigm.

use super::*;
use dbflux_core::{ObjectFolder, SchemaSnapshot};
use std::borrow::Cow;

/// The connection state an object folder is built from.
pub(super) struct FolderSources<'a> {
    pub(super) profile_id: Uuid,
    pub(super) connected: &'a ConnectedProfile,
    pub(super) capabilities: DriverCapabilities,
    pub(super) connection_category: DatabaseCategory,
    pub(super) metric_cache: Option<&'a dbflux_app::MetricCatalogCache>,
    pub(super) metric_fetch_errors: &'a HashMap<String, String>,
}

/// Where the objects of one database stand in the caches.
enum FolderObjects<'a> {
    /// Known objects, possibly none.
    Listed(Cow<'a, [TableInfo]>),
    /// The database has its own connection, whose snapshot lists nothing for
    /// this folder.
    Unlisted,
    /// Nothing describes the database yet.
    Unloaded,
}

/// The children of a database row in an object-folder layout: its folder
/// content once its objects are known, or a loading row while a fetch for it
/// runs.
pub(super) fn database_children(
    sources: &FolderSources<'_>,
    folder: ObjectFolder,
    snapshot: &SchemaSnapshot,
    database: &str,
    uses_lazy_loading: bool,
    is_pending: bool,
    is_current: bool,
) -> Vec<TreeItem> {
    let objects = folder_objects(
        sources.connected,
        folder,
        snapshot,
        database,
        uses_lazy_loading,
        is_current,
    );

    match objects {
        FolderObjects::Listed(objects) => folder_content(sources, folder, database, &objects),
        FolderObjects::Unlisted => Vec::new(),
        FolderObjects::Unloaded if is_pending => {
            vec![database_loading_item(sources.profile_id, database)]
        }
        FolderObjects::Unloaded => Vec::new(),
    }
}

/// Picks the cache that describes the objects of `database`: the lazily
/// fetched schema, then the snapshot of the database's own connection, then
/// the connect-time snapshot for the current database.
fn folder_objects<'a>(
    connected: &'a ConnectedProfile,
    folder: ObjectFolder,
    snapshot: &SchemaSnapshot,
    database: &str,
    uses_lazy_loading: bool,
    is_current: bool,
) -> FolderObjects<'a> {
    if uses_lazy_loading {
        if let Some(loaded) = connected.database_schemas.get(database) {
            return FolderObjects::Listed(Cow::Borrowed(&loaded.tables));
        }

        if is_current && folder.snapshot_lists_current_objects() {
            return FolderObjects::Listed(Cow::Owned(snapshot.folder_objects(database)));
        }

        return FolderObjects::Unloaded;
    }

    if let Some(slot) = connected.database_connections.get(database) {
        return match slot.schema.as_ref() {
            Some(slot_snapshot) if folder.database_connection_lists_objects() => {
                FolderObjects::Listed(Cow::Owned(slot_snapshot.folder_objects(database)))
            }
            _ => FolderObjects::Unlisted,
        };
    }

    if is_current {
        return FolderObjects::Listed(Cow::Owned(snapshot.folder_objects(database)));
    }

    FolderObjects::Unloaded
}

/// The rows under a database whose objects are known: the object folder,
/// the metric catalog when the driver offers one, and the indexes of every
/// object when the folder lists object details.
pub(super) fn folder_content(
    sources: &FolderSources<'_>,
    folder: ObjectFolder,
    database: &str,
    objects: &[TableInfo],
) -> Vec<TreeItem> {
    let mut content = Vec::new();

    if let Some(objects_folder) = objects_folder(sources, folder, database, objects) {
        content.push(objects_folder);
    }

    if sources
        .capabilities
        .contains(DriverCapabilities::METRIC_CATALOG)
    {
        content.push(build_metrics_folder(
            sources.profile_id,
            database,
            sources.metric_cache,
            sources.metric_fetch_errors,
        ));
    }

    if folder.lists_object_details()
        && let Some(indexes_folder) =
            build_db_collection_indexes_folder(sources.profile_id, database, objects)
    {
        content.push(indexes_folder);
    }

    content
}

/// The folder holding one row per object, named and expanded by the folder's
/// category. `None` when the database has no objects.
fn objects_folder(
    sources: &FolderSources<'_>,
    folder: ObjectFolder,
    database: &str,
    objects: &[TableInfo],
) -> Option<TreeItem> {
    if objects.is_empty() {
        return None;
    }

    let category = folder
        .label_category()
        .unwrap_or(sources.connection_category);

    let object_items: Vec<TreeItem> = objects
        .iter()
        .map(|object| {
            if folder.lists_object_details() {
                Sidebar::build_collection_item(
                    sources.profile_id,
                    database,
                    object,
                    &sources.connected.table_details,
                    &sources.connected.collection_children,
                )
            } else {
                object_leaf(sources.profile_id, database, &object.name)
            }
        })
        .collect();

    Some(
        TreeItem::new(
            SchemaNodeId::CollectionsFolder {
                profile_id: sources.profile_id,
                database: database.to_string(),
            }
            .to_string(),
            crate::labels::container_folder_label(category, objects.len()),
        )
        .expanded(category.default_expand_container())
        .children(object_items),
    )
}

fn object_leaf(profile_id: Uuid, database: &str, name: &str) -> TreeItem {
    TreeItem::new(
        SchemaNodeId::Collection {
            profile_id,
            database: database.to_string(),
            name: name.to_string(),
        }
        .to_string(),
        name.to_string(),
    )
}

fn database_loading_item(profile_id: Uuid, database: &str) -> TreeItem {
    TreeItem::new(
        SchemaNodeId::Loading {
            profile_id,
            database: database.to_owned(),
        }
        .to_string(),
        dbflux_i18n::t!("sidebar.tree.status.loading"),
    )
}

fn build_metrics_folder(
    profile_id: Uuid,
    database_name: &str,
    metric_catalog_cache: Option<&dbflux_app::MetricCatalogCache>,
    metric_fetch_errors: &HashMap<String, String>,
) -> TreeItem {
    let parent_id = SchemaNodeId::MetricsFolder {
        profile_id,
        database: database_name.to_string(),
    }
    .to_string();

    let children = if let Some(err_msg) = metric_fetch_errors.get(&parent_id) {
        let retry_id = format!("metrics-retry|{}|{}", profile_id, database_name);
        vec![Sidebar::error_retry_placeholder(&retry_id, err_msg)]
    } else {
        Sidebar::build_metric_namespace_children(profile_id, database_name, metric_catalog_cache)
    };

    TreeItem::new(parent_id, crate::labels::metrics_folder_label())
        .expanded(false)
        .children(children)
}

fn build_db_collection_indexes_folder(
    profile_id: Uuid,
    database_name: &str,
    tables: &[TableInfo],
) -> Option<TreeItem> {
    let all_index_items: Vec<TreeItem> = tables
        .iter()
        .filter_map(|coll| {
            let doc_indexes = match coll.indexes.as_ref()? {
                IndexData::Document(v) => v,
                IndexData::Relational(v) => {
                    return Some(
                        v.iter()
                            .map(|idx| {
                                let unique_marker = if idx.is_unique { " UNIQUE" } else { "" };
                                let pk_marker = if idx.is_primary { " PK" } else { "" };
                                let cols = idx.columns.join(", ");
                                let label = format!(
                                    "{}.{} ({}){}{}",
                                    coll.name, idx.name, cols, unique_marker, pk_marker
                                );
                                TreeItem::new(
                                    SchemaNodeId::CollectionIndex {
                                        profile_id,
                                        collection: coll.name.to_string(),
                                        name: idx.name.clone(),
                                    }
                                    .to_string(),
                                    label,
                                )
                            })
                            .collect::<Vec<_>>(),
                    );
                }
            };

            Some(
                doc_indexes
                    .iter()
                    .map(|idx| {
                        let label = format!("{}.{}", coll.name, format_collection_index_label(idx));
                        TreeItem::new(
                            SchemaNodeId::CollectionIndex {
                                profile_id,
                                collection: coll.name.to_string(),
                                name: idx.name.clone(),
                            }
                            .to_string(),
                            label,
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .flatten()
        .collect();

    if all_index_items.is_empty() {
        return None;
    }

    Some(
        TreeItem::new(
            SchemaNodeId::DatabaseIndexesFolder {
                profile_id,
                database: database_name.to_string(),
            }
            .to_string(),
            crate::labels::indexes_folder_label(all_index_items.len()),
        )
        .expanded(false)
        .children(all_index_items),
    )
}
