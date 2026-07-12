//! Wizard-owned tree selection model backing the source/target table
//! pickers: deterministic node ids over connection/database/schema/table
//! payloads (looked up by id — never parsed back out of the id string),
//! per-node lazy-load state, and the wizard's own checked-table
//! multi-select. `TreeNav` itself is a pure nav model that renders nothing,
//! so checkbox state has no business living there (design ADR #3) — it
//! lives here instead. Pure data, no GPUI, unit testable without a wizard
//! entity. `source_target` turns live metadata into `TreeNavNode`s keyed by
//! these same ids and hands them to `TreeNav::set_nodes`.

use std::collections::{HashMap, HashSet};

use dbflux_core::TableRef;
use gpui::SharedString;
use uuid::Uuid;

/// What a tree node id refers to, looked up in [`TreeModel::payload`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreePayload {
    Connection(Uuid),
    Database {
        profile_id: Uuid,
        database: String,
    },
    Schema {
        profile_id: Uuid,
        database: String,
        schema: String,
    },
    Table {
        profile_id: Uuid,
        database: String,
        schema: Option<String>,
        table: TableRef,
    },
}

/// Per-node lazy-load state. Reused by the Tables Mapping grid for its own
/// target-table-existence lookup, so `Loaded` carries no payload here — the
/// grid reads the result from its own row state.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum NodeLoad {
    #[default]
    NotLoaded,
    Loading,
    Loaded,
    Failed(String),
}

pub fn connection_node_id(profile_id: Uuid) -> SharedString {
    SharedString::from(format!("conn:{profile_id}"))
}

pub fn database_node_id(profile_id: Uuid, database: &str) -> SharedString {
    SharedString::from(format!("db:{profile_id}:{database}"))
}

pub fn schema_node_id(profile_id: Uuid, database: &str, schema: &str) -> SharedString {
    SharedString::from(format!("schema:{profile_id}:{database}:{schema}"))
}

pub fn table_node_id(
    profile_id: Uuid,
    database: &str,
    schema: Option<&str>,
    table: &str,
) -> SharedString {
    match schema {
        Some(schema) => {
            SharedString::from(format!("table:{profile_id}:{database}:{schema}:{table}"))
        }
        None => SharedString::from(format!("table:{profile_id}:{database}::{table}")),
    }
}

/// The ids a caller should hand to `TreeNav` after seeding pre-selection:
/// which ancestor group ids to expand, and which table row to move the
/// cursor to via `TreeNav::select_by_id`.
pub struct SeedResult {
    pub expand: HashSet<SharedString>,
    pub cursor: Option<SharedString>,
}

#[derive(Default)]
pub struct TreeModel {
    node_payloads: HashMap<SharedString, TreePayload>,
    node_load: HashMap<SharedString, NodeLoad>,
    checked: HashSet<SharedString>,
}

impl TreeModel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_payload(&mut self, id: SharedString, payload: TreePayload) {
        self.node_payloads.insert(id, payload);
    }

    pub fn payload(&self, id: &str) -> Option<&TreePayload> {
        self.node_payloads.get(id)
    }

    pub fn set_load(&mut self, id: SharedString, load: NodeLoad) {
        self.node_load.insert(id, load);
    }

    pub fn load(&self, id: &str) -> NodeLoad {
        self.node_load.get(id).cloned().unwrap_or_default()
    }

    /// Flips `id`'s checked state and returns the new state — mirrors the
    /// table-leaf `Selected(id)` toggle action (Space/Enter/click) the
    /// caller wires from `TreeNav::activate`.
    pub fn toggle_checked(&mut self, id: &SharedString) -> bool {
        if self.checked.remove(id) {
            false
        } else {
            self.checked.insert(id.clone());
            true
        }
    }

    pub fn is_checked(&self, id: &str) -> bool {
        self.checked.contains(id)
    }

    pub fn checked_count(&self) -> usize {
        self.checked.len()
    }

    pub fn checked_ids(&self) -> impl Iterator<Item = &SharedString> {
        self.checked.iter()
    }

    pub fn clear_checked(&mut self) {
        self.checked.clear();
    }

    /// Seeds pre-selection from `MigrateWizard::open`'s `source_tables`:
    /// every table becomes checked, and the returned [`SeedResult`] carries
    /// the ancestor ids (schema, if any, database, connection) to expand and
    /// the first table's id to move the cursor to, so the caller's
    /// `TreeNav` shows the pre-checked tables already visible and selected.
    pub fn seed_source_selection(
        &mut self,
        profile_id: Uuid,
        database: &str,
        source_tables: &[TableRef],
    ) -> SeedResult {
        let mut expand = HashSet::new();
        expand.insert(connection_node_id(profile_id));
        expand.insert(database_node_id(profile_id, database));

        let mut cursor = None;
        for table in source_tables {
            if let Some(schema) = table.schema.as_deref() {
                expand.insert(schema_node_id(profile_id, database, schema));
            }
            let id = table_node_id(profile_id, database, table.schema.as_deref(), &table.name);
            self.checked.insert(id.clone());
            if cursor.is_none() {
                cursor = Some(id);
            }
        }

        SeedResult { expand, cursor }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid(seed: u8) -> Uuid {
        Uuid::from_bytes([seed; 16])
    }

    #[test]
    fn payload_variants_round_trip_through_the_model() {
        let mut model = TreeModel::new();
        let profile_id = uuid(1);

        model.insert_payload(
            connection_node_id(profile_id),
            TreePayload::Connection(profile_id),
        );
        model.insert_payload(
            database_node_id(profile_id, "app"),
            TreePayload::Database {
                profile_id,
                database: "app".to_string(),
            },
        );
        model.insert_payload(
            schema_node_id(profile_id, "app", "public"),
            TreePayload::Schema {
                profile_id,
                database: "app".to_string(),
                schema: "public".to_string(),
            },
        );
        model.insert_payload(
            table_node_id(profile_id, "app", Some("public"), "users"),
            TreePayload::Table {
                profile_id,
                database: "app".to_string(),
                schema: Some("public".to_string()),
                table: TableRef {
                    schema: Some("public".to_string()),
                    name: "users".to_string(),
                },
            },
        );

        assert_eq!(
            model.payload(&connection_node_id(profile_id)),
            Some(&TreePayload::Connection(profile_id))
        );
        assert_eq!(
            model.payload(&database_node_id(profile_id, "app")),
            Some(&TreePayload::Database {
                profile_id,
                database: "app".to_string(),
            })
        );
        assert_eq!(
            model.payload(&schema_node_id(profile_id, "app", "public")),
            Some(&TreePayload::Schema {
                profile_id,
                database: "app".to_string(),
                schema: "public".to_string(),
            })
        );
        assert_eq!(
            model.payload(&table_node_id(profile_id, "app", Some("public"), "users")),
            Some(&TreePayload::Table {
                profile_id,
                database: "app".to_string(),
                schema: Some("public".to_string()),
                table: TableRef {
                    schema: Some("public".to_string()),
                    name: "users".to_string(),
                },
            })
        );
        assert_eq!(model.payload("missing"), None);
    }

    #[test]
    fn node_load_transitions_not_loaded_loading_loaded_or_failed() {
        let mut model = TreeModel::new();
        let id = SharedString::from("schema:x");

        assert_eq!(model.load(&id), NodeLoad::NotLoaded);

        model.set_load(id.clone(), NodeLoad::Loading);
        assert_eq!(model.load(&id), NodeLoad::Loading);

        model.set_load(id.clone(), NodeLoad::Loaded);
        assert_eq!(model.load(&id), NodeLoad::Loaded);

        let failing_id = SharedString::from("schema:y");
        model.set_load(failing_id.clone(), NodeLoad::Loading);
        model.set_load(failing_id.clone(), NodeLoad::Failed("boom".to_string()));
        assert_eq!(
            model.load(&failing_id),
            NodeLoad::Failed("boom".to_string())
        );
    }

    #[test]
    fn toggle_checked_flips_state_and_reports_the_new_value() {
        let mut model = TreeModel::new();
        let id = SharedString::from("table:1");

        assert!(!model.is_checked(&id));
        assert!(model.toggle_checked(&id));
        assert!(model.is_checked(&id));
        assert_eq!(model.checked_count(), 1);

        assert!(!model.toggle_checked(&id));
        assert!(!model.is_checked(&id));
        assert_eq!(model.checked_count(), 0);
    }

    #[test]
    fn clear_checked_empties_the_set() {
        let mut model = TreeModel::new();
        model.toggle_checked(&SharedString::from("a"));
        model.toggle_checked(&SharedString::from("b"));
        assert_eq!(model.checked_count(), 2);

        model.clear_checked();
        assert_eq!(model.checked_count(), 0);
    }

    #[test]
    fn seed_source_selection_checks_every_table_and_expands_its_ancestor_path() {
        let mut model = TreeModel::new();
        let profile_id = uuid(2);
        let source_tables = vec![
            TableRef {
                schema: Some("public".to_string()),
                name: "users".to_string(),
            },
            TableRef {
                schema: Some("public".to_string()),
                name: "orders".to_string(),
            },
            TableRef {
                schema: None,
                name: "no_schema_table".to_string(),
            },
        ];

        let seed = model.seed_source_selection(profile_id, "app", &source_tables);

        assert!(model.is_checked(&table_node_id(profile_id, "app", Some("public"), "users")));
        assert!(model.is_checked(&table_node_id(profile_id, "app", Some("public"), "orders")));
        assert!(model.is_checked(&table_node_id(profile_id, "app", None, "no_schema_table")));
        assert_eq!(model.checked_count(), 3);

        assert!(seed.expand.contains(&connection_node_id(profile_id)));
        assert!(seed.expand.contains(&database_node_id(profile_id, "app")));
        assert!(
            seed.expand
                .contains(&schema_node_id(profile_id, "app", "public"))
        );
        assert_eq!(
            seed.cursor,
            Some(table_node_id(profile_id, "app", Some("public"), "users"))
        );
    }

    #[test]
    fn seed_source_selection_with_no_tables_expands_only_the_connection_and_database() {
        let mut model = TreeModel::new();
        let profile_id = uuid(3);

        let seed = model.seed_source_selection(profile_id, "app", &[]);

        assert_eq!(model.checked_count(), 0);
        assert_eq!(seed.expand.len(), 2);
        assert!(seed.cursor.is_none());
    }
}
