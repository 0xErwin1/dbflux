//! How a schema snapshot lays out the objects of its databases in a tree.
//!
//! [`SchemaSnapshot::database_layout`] is the one place that maps a
//! [`DataStructure`] paradigm to a tree shape. Tree consumers walk the
//! returned description instead of matching the paradigm themselves.

use super::types::{CollectionInfo, DataStructure, IndexData, SchemaSnapshot, TableInfo};
use crate::DatabaseCategory;

/// How the objects of a snapshot's databases are arranged in a tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseLayout {
    /// Each database groups its tables and views under schemas.
    Schemas,
    /// Each database lists its objects in one folder.
    Folder(ObjectFolder),
    /// The snapshot lists numbered keyspaces in place of databases, and a
    /// keyspace holds no objects of its own in the tree.
    Keyspaces,
}

/// The folder that lists the objects of one database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectFolder {
    /// Collections, each listing its sampled fields and its indexes.
    Collections,
    /// Measurements, listed as leaves.
    Measurements,
}

impl ObjectFolder {
    /// The category whose presentation names the folder and decides whether
    /// it opens expanded. `None` means the connection's own category, since
    /// the same collection folder reads "Collections" or "Log groups"
    /// depending on the driver.
    pub fn label_category(self) -> Option<DatabaseCategory> {
        match self {
            ObjectFolder::Collections => None,
            ObjectFolder::Measurements => Some(DatabaseCategory::TimeSeries),
        }
    }

    /// Whether each object lists its fields and indexes, and the database a
    /// folder with the indexes of all its objects.
    pub fn lists_object_details(self) -> bool {
        match self {
            ObjectFolder::Collections => true,
            ObjectFolder::Measurements => false,
        }
    }

    /// Whether the connect-time snapshot already lists the objects of the
    /// current database, so a lazily loaded database shows them before its
    /// own fetch.
    pub fn snapshot_lists_current_objects(self) -> bool {
        match self {
            ObjectFolder::Collections => false,
            ObjectFolder::Measurements => true,
        }
    }

    /// Whether the snapshot of a database's own connection lists the objects
    /// of that database.
    pub fn database_connection_lists_objects(self) -> bool {
        match self {
            ObjectFolder::Collections => false,
            ObjectFolder::Measurements => true,
        }
    }
}

impl SchemaSnapshot {
    /// How this snapshot's databases arrange their objects. The reserved wire
    /// slots carry no objects and fall back to the schema layout.
    pub fn database_layout(&self) -> DatabaseLayout {
        match self.structure {
            DataStructure::Relational(_)
            | DataStructure::ReservedGraph
            | DataStructure::ReservedWideColumn => DatabaseLayout::Schemas,
            DataStructure::Document(_) => DatabaseLayout::Folder(ObjectFolder::Collections),
            DataStructure::TimeSeries(_) => DatabaseLayout::Folder(ObjectFolder::Measurements),
            DataStructure::KeyValue(_) => DatabaseLayout::Keyspaces,
        }
    }

    /// The objects this snapshot lists in the folder of `database`, as table
    /// rows scoped to that database. Collections that name another database
    /// are left out. Empty for layouts without an object folder.
    pub fn folder_objects(&self, database: &str) -> Vec<TableInfo> {
        // The snapshot only lists the current database's objects; another
        // database has none here.
        if self
            .current_database()
            .is_some_and(|current| current != database)
        {
            return Vec::new();
        }

        match &self.structure {
            DataStructure::Document(schema) => schema
                .collections
                .iter()
                .filter(|collection| {
                    collection
                        .database
                        .as_deref()
                        .is_none_or(|owner| owner == database)
                })
                .map(|collection| collection_row(collection, database))
                .collect(),
            DataStructure::TimeSeries(schema) => schema
                .measurements
                .iter()
                .map(|measurement| object_row(&measurement.name, database))
                .collect(),
            DataStructure::Relational(_)
            | DataStructure::KeyValue(_)
            | DataStructure::ReservedGraph
            | DataStructure::ReservedWideColumn => Vec::new(),
        }
    }
}

fn collection_row(collection: &CollectionInfo, database: &str) -> TableInfo {
    TableInfo {
        indexes: collection.indexes.clone().map(IndexData::Document),
        sample_fields: collection.sample_fields.clone(),
        presentation: collection.presentation,
        child_items: collection.child_items.clone(),
        ..object_row(&collection.name, database)
    }
}

fn object_row(name: &str, database: &str) -> TableInfo {
    TableInfo {
        name: name.to_string(),
        schema: Some(database.to_string()),
        columns: None,
        indexes: None,
        foreign_keys: None,
        constraints: None,
        sample_fields: None,
        presentation: Default::default(),
        child_items: None,
        storage_hints: None,
        pseudo_columns: Box::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::{DatabaseLayout, ObjectFolder};
    use crate::{
        CollectionIndexInfo, CollectionInfo, CollectionPresentation, DatabaseCategory,
        DocumentSchema, IndexData, IndexDirection, KeyValueSchema, MeasurementInfo,
        RelationalSchema, SchemaSnapshot, TimeSeriesSchema,
    };

    fn collection(name: &str, database: Option<&str>) -> CollectionInfo {
        CollectionInfo {
            name: name.to_string(),
            database: database.map(str::to_string),
            document_count: None,
            avg_document_size: None,
            sample_fields: None,
            indexes: None,
            validator: None,
            is_capped: false,
            presentation: CollectionPresentation::DataGrid,
            child_items: None,
        }
    }

    #[test]
    fn every_paradigm_maps_to_one_layout() {
        assert_eq!(
            SchemaSnapshot::relational(RelationalSchema::default()).database_layout(),
            DatabaseLayout::Schemas
        );
        assert_eq!(
            SchemaSnapshot::document(DocumentSchema::default()).database_layout(),
            DatabaseLayout::Folder(ObjectFolder::Collections)
        );
        assert_eq!(
            SchemaSnapshot::time_series(TimeSeriesSchema::default()).database_layout(),
            DatabaseLayout::Folder(ObjectFolder::Measurements)
        );
        assert_eq!(
            SchemaSnapshot::key_value(KeyValueSchema::default()).database_layout(),
            DatabaseLayout::Keyspaces
        );
        assert_eq!(
            SchemaSnapshot::default().database_layout(),
            DatabaseLayout::Schemas
        );
    }

    #[test]
    fn folders_describe_how_their_objects_are_listed() {
        assert_eq!(ObjectFolder::Collections.label_category(), None);
        assert_eq!(
            ObjectFolder::Measurements.label_category(),
            Some(DatabaseCategory::TimeSeries)
        );
        assert!(ObjectFolder::Collections.lists_object_details());
        assert!(!ObjectFolder::Measurements.lists_object_details());
        assert!(!ObjectFolder::Collections.snapshot_lists_current_objects());
        assert!(ObjectFolder::Measurements.snapshot_lists_current_objects());
        assert!(!ObjectFolder::Collections.database_connection_lists_objects());
        assert!(ObjectFolder::Measurements.database_connection_lists_objects());
    }

    #[test]
    fn document_folder_objects_keep_the_collections_of_the_database() {
        let mut orders = collection("orders", Some("shop"));
        orders.presentation = CollectionPresentation::EventStream;
        orders.indexes = Some(vec![CollectionIndexInfo {
            name: "_id_".to_string(),
            keys: vec![("_id".to_string(), IndexDirection::Ascending)],
            is_unique: true,
            is_sparse: false,
            expire_after_seconds: None,
        }]);

        let snapshot = SchemaSnapshot::document(DocumentSchema {
            databases: Vec::new(),
            current_database: Some("shop".to_string()),
            collections: vec![
                orders,
                collection("shared", None),
                collection("elsewhere", Some("other")),
            ],
        });

        let objects = snapshot.folder_objects("shop");

        let names: Vec<&str> = objects.iter().map(|row| row.name.as_str()).collect();
        assert_eq!(names, ["orders", "shared"]);
        assert!(
            objects
                .iter()
                .all(|row| row.schema.as_deref() == Some("shop"))
        );
        assert_eq!(objects[0].presentation, CollectionPresentation::EventStream);
        assert!(matches!(
            objects[0].indexes.as_ref(),
            Some(IndexData::Document(indexes)) if indexes.len() == 1
        ));
    }

    #[test]
    fn time_series_folder_objects_are_the_measurements() {
        let snapshot = SchemaSnapshot::time_series(TimeSeriesSchema {
            measurements: vec![
                MeasurementInfo {
                    name: "cpu".to_string(),
                    tags: Vec::new(),
                    fields: Vec::new(),
                },
                MeasurementInfo {
                    name: "mem".to_string(),
                    tags: Vec::new(),
                    fields: Vec::new(),
                },
            ],
            ..TimeSeriesSchema::default()
        });

        let names: Vec<String> = snapshot
            .folder_objects("monitoring")
            .into_iter()
            .map(|row| row.name)
            .collect();
        assert_eq!(names, ["cpu", "mem"]);
    }

    #[test]
    fn layouts_without_a_folder_list_no_objects() {
        assert!(
            SchemaSnapshot::relational(RelationalSchema::default())
                .folder_objects("app")
                .is_empty()
        );
        assert!(
            SchemaSnapshot::key_value(KeyValueSchema::default())
                .folder_objects("db0")
                .is_empty()
        );
    }

    #[test]
    fn folder_objects_list_nothing_for_a_database_other_than_the_current_one() {
        let documents = SchemaSnapshot::document(DocumentSchema {
            databases: Vec::new(),
            current_database: Some("app".to_string()),
            collections: vec![collection("orders", Some("app")), collection("loose", None)],
        });
        assert_eq!(documents.folder_objects("app").len(), 2);
        assert!(documents.folder_objects("other").is_empty());

        let series = SchemaSnapshot::time_series(TimeSeriesSchema {
            databases: Vec::new(),
            current_database: Some("app".to_string()),
            measurements: vec![MeasurementInfo {
                name: "cpu".to_string(),
                tags: Vec::new(),
                fields: Vec::new(),
            }],
            retention_policies: Vec::new(),
        });
        assert_eq!(series.folder_objects("app").len(), 1);
        assert!(series.folder_objects("other").is_empty());
    }
}
