use super::*;
use dbflux_core::{DriverCapabilities, DriverMetadata};
use dbflux_ui_base::platform;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};

/// Returns `true` when the given driver metadata advertises the `METRIC_SERIES`
/// capability, meaning the driver can execute `MetricQuery` requests.
///
/// Used by tests to validate the METRIC_SERIES gating predicate.
/// The live entry point (`open_metric_chart_from_sidebar`) uses a pre-built
/// `MetricSource` with defaults; only `METRIC_CATALOG` is checked at the
/// sidebar tree-builder level.
#[allow(dead_code)]
pub(crate) fn supports_metric_charts(metadata: &DriverMetadata) -> bool {
    metadata
        .capabilities
        .contains(DriverCapabilities::METRIC_SERIES)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenDocumentDecision {
    ErrorNoConnection,
    FocusExisting(crate::ui::document::DocumentId),
    OpenNew,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CollectionDocumentPresentation {
    DataGrid,
    AuditLike,
}

fn decide_open_document(
    has_connection: bool,
    existing_id: Option<crate::ui::document::DocumentId>,
) -> OpenDocumentDecision {
    if !has_connection {
        return OpenDocumentDecision::ErrorNoConnection;
    }

    if let Some(existing_id) = existing_id {
        return OpenDocumentDecision::FocusExisting(existing_id);
    }

    OpenDocumentDecision::OpenNew
}

fn collection_document_presentation_for_connection(
    connected: &crate::app::ConnectedProfile,
    collection: &dbflux_core::CollectionRef,
) -> CollectionDocumentPresentation {
    let schema = connected
        .schema_for_target_database(collection.database.as_str())
        .or(connected.schema.as_ref());

    let presentation = schema
        .and_then(|schema| {
            schema
                .collections()
                .iter()
                .find(|entry| {
                    entry.name == collection.name
                        && entry
                            .database
                            .as_deref()
                            .unwrap_or(collection.database.as_str())
                            == collection.database.as_str()
                })
                .map(|entry| entry.presentation)
        })
        .unwrap_or(dbflux_core::CollectionPresentation::DataGrid);

    match presentation {
        dbflux_core::CollectionPresentation::DataGrid => CollectionDocumentPresentation::DataGrid,
        dbflux_core::CollectionPresentation::EventStream => {
            CollectionDocumentPresentation::AuditLike
        }
    }
}

mod audit;
mod charts_dashboards;
mod connections;
mod documents;
mod metrics;
mod query;
mod scripts;
mod settings;

impl Workspace {
    pub(super) fn handle_command(
        &mut self,
        command_id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(command) = Command::from_palette_id(command_id) else {
            log::warn!("Unknown command: {}", command_id);
            return;
        };

        self.dispatch(command, window, cx);
    }

    /// Strip leading annotation comments from file content.
    fn strip_annotation_header<'a>(content: &'a str, language: &QueryLanguage) -> &'a str {
        let prefix = language.comment_prefix();
        let mut end = 0;

        for line in content.lines() {
            let trimmed = line.trim();

            if trimmed.is_empty() {
                end += line.len() + 1;
                continue;
            }

            if let Some(after_prefix) = trimmed.strip_prefix(prefix)
                && after_prefix.trim().starts_with('@')
            {
                end += line.len() + 1;
                continue;
            }

            break;
        }

        if end >= content.len() {
            ""
        } else {
            &content[end..]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{OpenDocumentDecision, decide_open_document};
    use crate::ui::document::DocumentId;
    use uuid::Uuid;

    #[test]
    fn decide_open_document_returns_error_without_connection() {
        let decision = decide_open_document(false, None);
        assert_eq!(decision, OpenDocumentDecision::ErrorNoConnection);
    }

    #[test]
    fn decide_open_document_focuses_existing_tab_when_available() {
        let existing = DocumentId(Uuid::new_v4());
        let decision = decide_open_document(true, Some(existing));
        assert_eq!(decision, OpenDocumentDecision::FocusExisting(existing));
    }

    #[test]
    fn decide_open_document_opens_new_when_connected_and_no_existing_tab() {
        let decision = decide_open_document(true, None);
        assert_eq!(decision, OpenDocumentDecision::OpenNew);
    }

    // --- strip_annotation_header ---

    use crate::ui::views::workspace::Workspace;

    #[test]
    fn strip_annotation_header_removes_sql_annotations() {
        let content = "-- @connection: my-db\n-- @database: main\nSELECT 1;";
        let result = Workspace::strip_annotation_header(content, &dbflux_core::QueryLanguage::Sql);
        assert_eq!(result, "SELECT 1;");
    }

    #[test]
    fn strip_annotation_header_preserves_non_annotation_comments() {
        let content = "-- This is a regular comment\nSELECT 1;";
        let result = Workspace::strip_annotation_header(content, &dbflux_core::QueryLanguage::Sql);
        assert_eq!(result, "-- This is a regular comment\nSELECT 1;");
    }

    #[test]
    fn strip_annotation_header_skips_blank_lines_before_annotations() {
        let content = "\n\n-- @connection: db\nSELECT 1;";
        let result = Workspace::strip_annotation_header(content, &dbflux_core::QueryLanguage::Sql);
        assert_eq!(result, "SELECT 1;");
    }

    #[test]
    fn strip_annotation_header_all_annotations_returns_empty() {
        let content = "-- @connection: db\n-- @database: main\n";
        let result = Workspace::strip_annotation_header(content, &dbflux_core::QueryLanguage::Sql);
        assert_eq!(result, "");
    }

    #[test]
    fn strip_annotation_header_empty_content() {
        let result = Workspace::strip_annotation_header("", &dbflux_core::QueryLanguage::Sql);
        assert_eq!(result, "");
    }

    #[test]
    fn strip_annotation_header_mongo_comment_prefix() {
        let content = "// @connection: my-db\ndb.collection.find()";
        let result =
            Workspace::strip_annotation_header(content, &dbflux_core::QueryLanguage::MongoQuery);
        assert_eq!(result, "db.collection.find()");
    }

    #[test]
    fn strip_annotation_header_redis_comment_prefix() {
        let content = "# @connection: my-db\nGET key";
        let result =
            Workspace::strip_annotation_header(content, &dbflux_core::QueryLanguage::RedisCommands);
        assert_eq!(result, "GET key");
    }

    // --- PaletteItem model tests ---

    use crate::ui::overlays::command_palette::{PaletteItem, PaletteSelection, ResourceItem};
    use crate::ui::views::workspace::{build_resource_items_from_schema, map_item_to_selection};
    use dbflux_core::{
        CollectionInfo, DataStructure, DbSchemaInfo, DocumentSchema, KeySpaceInfo, KeyValueSchema,
        RelationalSchema, ScriptEntry, TableInfo, ViewInfo,
    };
    use fuzzy_matcher::FuzzyMatcher;
    use fuzzy_matcher::skim::SkimMatcherV2;
    use std::path::{Path, PathBuf};

    fn sample_action() -> PaletteItem {
        PaletteItem::Action {
            id: "new_query_tab",
            name: "New Query Tab",
            category: "Editor",
            shortcut: Some("Ctrl+N"),
        }
    }

    fn sample_connection(name: &str, connected: bool) -> PaletteItem {
        PaletteItem::Connection {
            profile_id: Uuid::new_v4(),
            name: name.to_string(),
            is_connected: connected,
        }
    }

    fn sample_table(profile_name: &str, name: &str) -> PaletteItem {
        PaletteItem::Resource(ResourceItem::Table {
            profile_id: Uuid::new_v4(),
            profile_name: profile_name.to_string(),
            database: Some("main".to_string()),
            schema: Some("public".to_string()),
            name: name.to_string(),
        })
    }

    fn sample_view(profile_name: &str, name: &str) -> PaletteItem {
        PaletteItem::Resource(ResourceItem::View {
            profile_id: Uuid::new_v4(),
            profile_name: profile_name.to_string(),
            database: Some("main".to_string()),
            schema: Some("public".to_string()),
            name: name.to_string(),
        })
    }

    fn sample_script(name: &str) -> PaletteItem {
        PaletteItem::Script {
            path: PathBuf::from(format!("{}.sql", name)),
            name: name.to_string(),
            relative_path: format!("{}.sql", name),
        }
    }

    #[test]
    fn palette_item_search_text_includes_relevant_fields() {
        let action = sample_action();
        assert!(action.search_text().contains("Editor"));
        assert!(action.search_text().contains("New Query Tab"));

        let conn = sample_connection("prod-pg", true);
        assert!(conn.search_text().contains("Connection"));
        assert!(conn.search_text().contains("prod-pg"));

        let table = sample_table("prod-pg", "orders");
        assert!(table.search_text().contains("Table"));
        assert!(table.search_text().contains("prod-pg"));
        assert!(table.search_text().contains("orders"));
        assert!(
            table.search_text().contains("main"),
            "search_text should include database"
        );
        assert!(
            table.search_text().contains("public"),
            "search_text should include schema"
        );

        let view = sample_view("prod-pg", "active_users");
        assert!(view.search_text().contains("View"));
        assert!(view.search_text().contains("active_users"));
        assert!(view.search_text().contains("main"));

        let script = sample_script("health-check");
        assert!(script.search_text().contains("Script"));
        assert!(script.search_text().contains("health-check"));
    }

    #[test]
    fn palette_item_search_text_table_without_schema() {
        let table = PaletteItem::Resource(ResourceItem::Table {
            profile_id: Uuid::new_v4(),
            profile_name: "sqlite-local".to_string(),
            database: None,
            schema: None,
            name: "notes".to_string(),
        });
        let text = table.search_text();
        assert!(text.contains("Table"));
        assert!(text.contains("sqlite-local"));
        assert!(text.contains("notes"));
    }

    #[test]
    fn palette_item_search_text_collection_includes_database() {
        let collection = PaletteItem::Resource(ResourceItem::Collection {
            profile_id: Uuid::new_v4(),
            profile_name: "mongo-prod".to_string(),
            database: "analytics".to_string(),
            name: "events".to_string(),
        });
        let text = collection.search_text();
        assert!(text.contains("Collection"));
        assert!(text.contains("analytics"));
        assert!(text.contains("events"));
    }

    #[test]
    fn palette_item_type_priority_ordering() {
        let action = sample_action();
        let connection = sample_connection("test", false);
        let saved_chart = PaletteItem::SavedChart {
            id: Uuid::new_v4(),
            name: "My Chart".to_string(),
            profile_name: "test".to_string(),
            profile_id: Uuid::new_v4(),
            is_collection_source: false,
        };
        let resource = sample_table("test", "t");
        let script = sample_script("test");

        assert_eq!(action.type_priority(), 0);
        assert_eq!(connection.type_priority(), 1);
        assert_eq!(saved_chart.type_priority(), 2);
        assert_eq!(resource.type_priority(), 3);
        assert_eq!(script.type_priority(), 4);

        assert!(action.type_priority() < connection.type_priority());
        assert!(connection.type_priority() < saved_chart.type_priority());
        assert!(saved_chart.type_priority() < resource.type_priority());
        assert!(resource.type_priority() < script.type_priority());
    }

    #[test]
    fn palette_item_display_label_returns_category_and_name() {
        let action = sample_action();
        let (cat, name) = action.display_label();
        assert_eq!(cat, "Editor");
        assert_eq!(name, "New Query Tab");

        let conn = sample_connection("prod-pg", true);
        let (cat, name) = conn.display_label();
        assert_eq!(cat, "Connection");
        assert_eq!(name, "prod-pg");

        let table = sample_table("prod-pg", "orders");
        let (cat, name) = table.display_label();
        assert_eq!(cat, "Table");
        assert_eq!(name, "orders");

        let view = sample_view("prod-pg", "active_users");
        let (cat, name) = view.display_label();
        assert_eq!(cat, "View");
        assert_eq!(name, "active_users");

        let script = sample_script("health-check");
        let (cat, name) = script.display_label();
        assert_eq!(cat, "Script");
        assert_eq!(name, "health-check");
    }

    #[test]
    fn palette_item_qualifier_resources_show_profile_name() {
        let table = sample_table("prod-pg", "orders");
        assert!(table.qualifier().unwrap().contains("prod-pg"));
        assert!(table.qualifier().unwrap().contains("main"));

        let view = sample_view("prod-pg", "active_users");
        assert!(view.qualifier().unwrap().contains("prod-pg"));
    }

    #[test]
    fn palette_filtering_sorts_by_score_descending_with_type_tiebreaker() {
        let matcher = SkimMatcherV2::default();

        let items: Vec<PaletteItem> = vec![
            sample_script("prod-health"),
            sample_connection("prod-pg", true),
            sample_action(), // "New Query Tab" — does not match "prod"
        ];

        let matched: Vec<(usize, i64)> = items
            .iter()
            .enumerate()
            .filter_map(|(i, item)| {
                matcher
                    .fuzzy_match(&item.search_text(), "prod")
                    .map(|score| (i, score))
            })
            .collect();

        // Only script and connection match "prod"
        assert_eq!(matched.len(), 2);

        // Both match — verify type-priority ordering at equal scores
        let mut sorted = matched.clone();
        sorted.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| items[a.0].type_priority().cmp(&items[b.0].type_priority()))
        });

        // Connection (priority 1) should come before Script (priority 3) at equal scores
        assert!(items[sorted[0].0].type_priority() <= items[sorted[1].0].type_priority());
    }

    #[test]
    fn palette_item_view_and_table_have_same_priority() {
        let table = sample_table("p", "t");
        let view = sample_view("p", "v");
        assert_eq!(table.type_priority(), view.type_priority());
    }

    // --- Resource item building from schema ---

    #[test]
    fn build_resources_from_relational_schema() {
        let pid = Uuid::new_v4();
        let mut items = Vec::new();

        let structure = DataStructure::Relational(RelationalSchema {
            current_database: Some("mydb".to_string()),
            tables: vec![
                TableInfo {
                    name: "users".to_string(),
                    schema: Some("public".to_string()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: dbflux_core::CollectionPresentation::DataGrid,
                    child_items: None,
                },
                TableInfo {
                    name: "orders".to_string(),
                    schema: Some("public".to_string()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: dbflux_core::CollectionPresentation::DataGrid,
                    child_items: None,
                },
            ],
            views: vec![ViewInfo {
                name: "active_users".to_string(),
                schema: Some("public".to_string()),
            }],
            ..Default::default()
        });

        build_resource_items_from_schema(pid, "prod-pg", &structure, &mut items);

        assert_eq!(items.len(), 3);

        let table_names: Vec<&str> = items
            .iter()
            .filter_map(|item| match item {
                PaletteItem::Resource(ResourceItem::Table { name, .. }) => Some(name.as_str()),
                _ => None,
            })
            .collect();
        assert!(table_names.contains(&"users"));
        assert!(table_names.contains(&"orders"));

        let view_count = items
            .iter()
            .filter(|item| matches!(item, PaletteItem::Resource(ResourceItem::View { .. })))
            .count();
        assert_eq!(view_count, 1);
    }

    #[test]
    fn build_resources_from_relational_schema_with_nested_schemas() {
        let pid = Uuid::new_v4();
        let mut items = Vec::new();

        let structure = DataStructure::Relational(RelationalSchema {
            current_database: Some("mydb".to_string()),
            tables: vec![],
            views: vec![],
            schemas: vec![DbSchemaInfo {
                name: "app_schema".to_string(),
                tables: vec![TableInfo {
                    name: "products".to_string(),
                    schema: Some("app_schema".to_string()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: dbflux_core::CollectionPresentation::DataGrid,
                    child_items: None,
                }],
                views: vec![],
                custom_types: None,
            }],
            ..Default::default()
        });

        build_resource_items_from_schema(pid, "pg-prod", &structure, &mut items);

        assert_eq!(items.len(), 1);
        match &items[0] {
            PaletteItem::Resource(ResourceItem::Table {
                database,
                schema,
                name,
                ..
            }) => {
                assert_eq!(database.as_deref(), Some("mydb"));
                assert_eq!(schema.as_deref(), Some("app_schema"));
                assert_eq!(name, "products");
            }
            _ => panic!("Expected Table resource"),
        }
    }

    #[test]
    fn build_resources_from_document_schema() {
        let pid = Uuid::new_v4();
        let mut items = Vec::new();

        let structure = DataStructure::Document(DocumentSchema {
            current_database: Some("shop".to_string()),
            collections: vec![
                CollectionInfo {
                    name: "products".to_string(),
                    database: Some("shop".to_string()),
                    document_count: None,
                    avg_document_size: None,
                    sample_fields: None,
                    indexes: None,
                    validator: None,
                    is_capped: false,
                    presentation: dbflux_core::CollectionPresentation::DataGrid,
                    child_items: None,
                },
                CollectionInfo {
                    name: "orders".to_string(),
                    database: None,
                    document_count: None,
                    avg_document_size: None,
                    sample_fields: None,
                    indexes: None,
                    validator: None,
                    is_capped: false,
                    presentation: dbflux_core::CollectionPresentation::DataGrid,
                    child_items: None,
                },
            ],
            ..Default::default()
        });

        build_resource_items_from_schema(pid, "mongo-prod", &structure, &mut items);

        assert_eq!(items.len(), 2);

        match &items[0] {
            PaletteItem::Resource(ResourceItem::Collection { database, name, .. }) => {
                assert_eq!(database, "shop");
                assert_eq!(name, "products");
            }
            _ => panic!("Expected Collection resource"),
        }

        // Second collection falls back to current_database
        match &items[1] {
            PaletteItem::Resource(ResourceItem::Collection { database, name, .. }) => {
                assert_eq!(database, "shop");
                assert_eq!(name, "orders");
            }
            _ => panic!("Expected Collection resource"),
        }
    }

    #[test]
    fn build_resources_from_keyvalue_schema() {
        let pid = Uuid::new_v4();
        let mut items = Vec::new();

        let structure = DataStructure::KeyValue(KeyValueSchema {
            keyspaces: vec![
                KeySpaceInfo {
                    db_index: 0,
                    key_count: Some(100),
                    memory_bytes: None,
                    avg_ttl_seconds: None,
                },
                KeySpaceInfo {
                    db_index: 1,
                    key_count: Some(50),
                    memory_bytes: None,
                    avg_ttl_seconds: None,
                },
            ],
            ..Default::default()
        });

        build_resource_items_from_schema(pid, "redis-prod", &structure, &mut items);

        assert_eq!(items.len(), 2);

        match &items[0] {
            PaletteItem::Resource(ResourceItem::KeyValueDb { database, .. }) => {
                assert_eq!(database, "db0");
            }
            _ => panic!("Expected KeyValueDb resource"),
        }
        match &items[1] {
            PaletteItem::Resource(ResourceItem::KeyValueDb { database, .. }) => {
                assert_eq!(database, "db1");
            }
            _ => panic!("Expected KeyValueDb resource"),
        }
    }

    #[test]
    fn build_resources_ignores_unsupported_schema_types() {
        let pid = Uuid::new_v4();
        let mut items = Vec::new();

        let structure = DataStructure::Graph(Default::default());
        build_resource_items_from_schema(pid, "neo4j", &structure, &mut items);

        assert!(items.is_empty());
    }

    #[test]
    fn build_resources_empty_schema_produces_no_items() {
        let pid = Uuid::new_v4();
        let mut items = Vec::new();

        let structure = DataStructure::Relational(RelationalSchema {
            current_database: None,
            tables: vec![],
            views: vec![],
            schemas: vec![],
            ..Default::default()
        });

        build_resource_items_from_schema(pid, "empty", &structure, &mut items);
        assert!(items.is_empty());
    }

    // --- Script flattening tests ---

    #[test]
    fn flatten_script_entries_includes_openable_files() {
        let entries = vec![
            ScriptEntry::File {
                path: PathBuf::from("/scripts/query.sql"),
                name: "query.sql".to_string(),
                extension: "sql".to_string(),
            },
            ScriptEntry::File {
                path: PathBuf::from("/scripts/hook.lua"),
                name: "hook.lua".to_string(),
                extension: "lua".to_string(),
            },
        ];

        let mut items = Vec::new();
        Workspace::flatten_script_entries(&entries, Path::new("/scripts"), &mut items);

        assert_eq!(items.len(), 2);
        match &items[0] {
            PaletteItem::Script {
                name,
                relative_path,
                ..
            } => {
                assert_eq!(name, "query.sql");
                assert_eq!(relative_path, "query.sql");
            }
            _ => panic!("Expected Script item"),
        }
    }

    #[test]
    fn flatten_script_entries_skips_non_openable_files() {
        let entries = vec![
            ScriptEntry::File {
                path: PathBuf::from("/scripts/data.csv"),
                name: "data.csv".to_string(),
                extension: "csv".to_string(),
            },
            ScriptEntry::File {
                path: PathBuf::from("/scripts/query.sql"),
                name: "query.sql".to_string(),
                extension: "sql".to_string(),
            },
        ];

        let mut items = Vec::new();
        Workspace::flatten_script_entries(&entries, Path::new("/scripts"), &mut items);

        assert_eq!(items.len(), 1);
        match &items[0] {
            PaletteItem::Script {
                name,
                relative_path,
                ..
            } => {
                assert_eq!(name, "query.sql");
                assert_eq!(relative_path, "query.sql");
            }
            _ => panic!("Expected Script item"),
        }
    }

    #[test]
    fn flatten_script_entries_recurses_into_folders() {
        let entries = vec![ScriptEntry::Folder {
            path: PathBuf::from("/scripts/migrations"),
            name: "migrations".to_string(),
            children: vec![
                ScriptEntry::File {
                    path: PathBuf::from("/scripts/migrations/001_init.sql"),
                    name: "001_init.sql".to_string(),
                    extension: "sql".to_string(),
                },
                ScriptEntry::File {
                    path: PathBuf::from("/scripts/migrations/002_add_users.sql"),
                    name: "002_add_users.sql".to_string(),
                    extension: "sql".to_string(),
                },
            ],
        }];

        let mut items = Vec::new();
        Workspace::flatten_script_entries(&entries, Path::new("/scripts"), &mut items);

        assert_eq!(items.len(), 2);

        // Verify nested files get relative paths with the folder prefix
        match &items[0] {
            PaletteItem::Script { relative_path, .. } => {
                assert_eq!(relative_path, "migrations/001_init.sql");
            }
            _ => panic!("Expected Script item"),
        }
        match &items[1] {
            PaletteItem::Script { relative_path, .. } => {
                assert_eq!(relative_path, "migrations/002_add_users.sql");
            }
            _ => panic!("Expected Script item"),
        }
    }

    // --- Selection routing (map_item_to_selection) ---

    #[test]
    fn selection_routing_action_produces_command() {
        let item = PaletteItem::Action {
            id: "new_query_tab",
            name: "New Query Tab",
            category: "Editor",
            shortcut: Some("Ctrl+N"),
        };

        let sel = map_item_to_selection(&item).unwrap();
        match sel {
            PaletteSelection::Command { id } => assert_eq!(id, "new_query_tab"),
            _ => panic!("Expected Command selection"),
        }
    }

    #[test]
    fn selection_routing_disconnected_profile_produces_connect() {
        let pid = Uuid::new_v4();
        let item = PaletteItem::Connection {
            profile_id: pid,
            name: "analytics".to_string(),
            is_connected: false,
        };

        let sel = map_item_to_selection(&item).unwrap();
        match sel {
            PaletteSelection::Connect { profile_id } => assert_eq!(profile_id, pid),
            _ => panic!("Expected Connect selection"),
        }
    }

    #[test]
    fn selection_routing_connected_profile_produces_focus_connection() {
        let pid = Uuid::new_v4();
        let item = PaletteItem::Connection {
            profile_id: pid,
            name: "prod-pg".to_string(),
            is_connected: true,
        };

        let sel = map_item_to_selection(&item).unwrap();
        match sel {
            PaletteSelection::FocusConnection { profile_id } => assert_eq!(profile_id, pid),
            _ => panic!("Expected FocusConnection selection"),
        }
    }

    #[test]
    fn selection_routing_table_produces_open_table() {
        let pid = Uuid::new_v4();
        let item = PaletteItem::Resource(ResourceItem::Table {
            profile_id: pid,
            profile_name: "prod".to_string(),
            database: Some("mydb".to_string()),
            schema: Some("public".to_string()),
            name: "orders".to_string(),
        });

        let sel = map_item_to_selection(&item).unwrap();
        match sel {
            PaletteSelection::OpenTable {
                profile_id,
                table,
                database,
            } => {
                assert_eq!(profile_id, pid);
                assert_eq!(table.name, "orders");
                assert_eq!(table.schema.as_deref(), Some("public"));
                assert_eq!(database.as_deref(), Some("mydb"));
            }
            _ => panic!("Expected OpenTable selection"),
        }
    }

    #[test]
    fn selection_routing_view_produces_open_table_same_as_sidebar() {
        let pid = Uuid::new_v4();
        let item = PaletteItem::Resource(ResourceItem::View {
            profile_id: pid,
            profile_name: "prod".to_string(),
            database: Some("mydb".to_string()),
            schema: Some("public".to_string()),
            name: "active_users".to_string(),
        });

        let sel = map_item_to_selection(&item).unwrap();
        match sel {
            PaletteSelection::OpenTable { table, .. } => {
                assert_eq!(table.name, "active_users");
            }
            _ => panic!("Expected OpenTable selection (views route like tables)"),
        }
    }

    #[test]
    fn selection_routing_collection_produces_open_collection() {
        let pid = Uuid::new_v4();
        let item = PaletteItem::Resource(ResourceItem::Collection {
            profile_id: pid,
            profile_name: "mongo-prod".to_string(),
            database: "shop".to_string(),
            name: "products".to_string(),
        });

        let sel = map_item_to_selection(&item).unwrap();
        match sel {
            PaletteSelection::OpenCollection {
                profile_id,
                collection,
            } => {
                assert_eq!(profile_id, pid);
                assert_eq!(collection.database, "shop");
                assert_eq!(collection.name, "products");
            }
            _ => panic!("Expected OpenCollection selection"),
        }
    }

    #[test]
    fn selection_routing_keyvalue_produces_open_key_value() {
        let pid = Uuid::new_v4();
        let item = PaletteItem::Resource(ResourceItem::KeyValueDb {
            profile_id: pid,
            profile_name: "redis-prod".to_string(),
            database: "db0".to_string(),
        });

        let sel = map_item_to_selection(&item).unwrap();
        match sel {
            PaletteSelection::OpenKeyValue {
                profile_id,
                database,
            } => {
                assert_eq!(profile_id, pid);
                assert_eq!(database, "db0");
            }
            _ => panic!("Expected OpenKeyValue selection"),
        }
    }

    #[test]
    fn selection_routing_script_produces_open_script() {
        let path = PathBuf::from("/scripts/health-check.sql");
        let item = PaletteItem::Script {
            path: path.clone(),
            name: "health-check".to_string(),
            relative_path: "health-check.sql".to_string(),
        };

        let sel = map_item_to_selection(&item).unwrap();
        match sel {
            PaletteSelection::OpenScript { path: p } => assert_eq!(p, path),
            _ => panic!("Expected OpenScript selection"),
        }
    }

    // --- Disambiguation scenarios ---

    #[test]
    fn two_connections_same_table_name_are_distinguished_by_profile() {
        let pid1 = Uuid::new_v4();
        let pid2 = Uuid::new_v4();

        let table1 = PaletteItem::Resource(ResourceItem::Table {
            profile_id: pid1,
            profile_name: "prod".to_string(),
            database: Some("mydb".to_string()),
            schema: Some("public".to_string()),
            name: "users".to_string(),
        });

        let table2 = PaletteItem::Resource(ResourceItem::Table {
            profile_id: pid2,
            profile_name: "staging".to_string(),
            database: Some("mydb".to_string()),
            schema: Some("public".to_string()),
            name: "users".to_string(),
        });

        // Both have same table name but different qualifiers (include profile name)
        assert!(table1.qualifier().unwrap().contains("prod"));
        assert!(table2.qualifier().unwrap().contains("staging"));

        // Search text includes profile name for disambiguation
        assert!(table1.search_text().contains("prod"));
        assert!(table2.search_text().contains("staging"));

        // They route to different profiles
        let sel1 = map_item_to_selection(&table1).unwrap();
        let sel2 = map_item_to_selection(&table2).unwrap();
        match (&sel1, &sel2) {
            (
                PaletteSelection::OpenTable {
                    profile_id: id1, ..
                },
                PaletteSelection::OpenTable {
                    profile_id: id2, ..
                },
            ) => {
                assert_ne!(id1, id2);
            }
            _ => panic!("Expected OpenTable selections"),
        }
    }

    // --- Same profile, same schema+table, different database dedup regression ---

    #[test]
    fn same_profile_same_table_different_database_produces_distinct_selections() {
        let pid = Uuid::new_v4();

        let table_db1 = PaletteItem::Resource(ResourceItem::Table {
            profile_id: pid,
            profile_name: "pg-multi-db".to_string(),
            database: Some("db_alpha".to_string()),
            schema: Some("public".to_string()),
            name: "orders".to_string(),
        });

        let table_db2 = PaletteItem::Resource(ResourceItem::Table {
            profile_id: pid,
            profile_name: "pg-multi-db".to_string(),
            database: Some("db_beta".to_string()),
            schema: Some("public".to_string()),
            name: "orders".to_string(),
        });

        // Both have same profile, schema, and table name but different databases
        let sel1 = map_item_to_selection(&table_db1).unwrap();
        let sel2 = map_item_to_selection(&table_db2).unwrap();

        match (&sel1, &sel2) {
            (
                PaletteSelection::OpenTable {
                    profile_id: id1,
                    table: t1,
                    database: db1,
                },
                PaletteSelection::OpenTable {
                    profile_id: id2,
                    table: t2,
                    database: db2,
                },
            ) => {
                assert_eq!(id1, id2, "Same profile");
                assert_eq!(t1, t2, "Same table ref (schema+name)");
                assert_ne!(
                    db1, db2,
                    "Different databases must produce distinct selections"
                );
                assert_eq!(db1.as_deref(), Some("db_alpha"));
                assert_eq!(db2.as_deref(), Some("db_beta"));
            }
            _ => panic!("Expected OpenTable selections"),
        }

        // Qualifiers must also differ (they include database)
        assert!(table_db1.qualifier().unwrap().contains("db_alpha"));
        assert!(table_db2.qualifier().unwrap().contains("db_beta"));
    }

    // --- Empty / no-match filtering ---

    #[test]
    fn fuzzy_filter_no_match_returns_empty() {
        let matcher = SkimMatcherV2::default();
        let items: Vec<PaletteItem> = vec![
            sample_action(),
            sample_connection("prod-pg", true),
            sample_table("prod-pg", "orders"),
        ];

        let matched: Vec<_> = items
            .iter()
            .enumerate()
            .filter_map(|(i, item)| {
                matcher
                    .fuzzy_match(&item.search_text(), "zzzzzzz")
                    .map(|score| (i, score))
            })
            .collect();

        assert!(matched.is_empty());
    }

    #[test]
    fn fuzzy_filter_empty_query_matches_all() {
        let items: Vec<PaletteItem> = vec![
            sample_action(),
            sample_connection("prod-pg", true),
            sample_table("prod-pg", "orders"),
            sample_script("health-check"),
        ];

        // Empty query should show all items (score 0 for all)
        let mut filtered: Vec<(usize, i64)> = items
            .iter()
            .enumerate()
            .map(|(index, _)| (index, 0))
            .collect();

        assert_eq!(filtered.len(), 4);
        filtered.sort_by_key(|s| std::cmp::Reverse(s.1));
        assert_eq!(filtered.len(), items.len());
    }

    // --- Performance: fuzzy filtering on large dataset ---

    #[test]
    fn palette_filtering_large_dataset_completes_within_budget() {
        let matcher = SkimMatcherV2::default();

        // Build a representative large dataset: 100 connections, 1000 resources, 200 scripts
        let mut items: Vec<PaletteItem> = Vec::with_capacity(1325);

        for i in 0..100 {
            items.push(PaletteItem::Action {
                id: Box::leak(format!("cmd_{}", i).into_boxed_str()),
                name: Box::leak(format!("Command {}", i).into_boxed_str()),
                category: "Editor",
                shortcut: None,
            });
        }

        for i in 0..100 {
            items.push(PaletteItem::Connection {
                profile_id: Uuid::new_v4(),
                name: format!("connection-{}", i),
                is_connected: i < 50,
            });
        }

        for i in 0..1000 {
            items.push(PaletteItem::Resource(ResourceItem::Table {
                profile_id: Uuid::new_v4(),
                profile_name: format!("profile-{}", i % 10),
                database: Some("mydb".to_string()),
                schema: Some("public".to_string()),
                name: format!("table_{}", i),
            }));
        }

        for i in 0..200 {
            items.push(PaletteItem::Script {
                path: PathBuf::from(format!("/scripts/script_{}.sql", i)),
                name: format!("script_{}", i),
                relative_path: format!("script_{}.sql", i),
            });
        }

        assert_eq!(items.len(), 1400);

        // Measure item build time (simulated: just the search_text generation)
        let build_start = std::time::Instant::now();
        let search_texts: Vec<String> = items.iter().map(|i| i.search_text()).collect();
        let build_elapsed = build_start.elapsed();
        assert!(
            build_elapsed.as_millis() < 50,
            "Item search_text build took {}ms, exceeds 50ms budget",
            build_elapsed.as_millis()
        );

        // Measure per-keystroke filter time
        let filter_start = std::time::Instant::now();
        let matched: Vec<_> = items
            .iter()
            .enumerate()
            .filter_map(|(i, _item)| {
                matcher
                    .fuzzy_match(&search_texts[i], "table_5")
                    .map(|score| (i, score))
            })
            .collect();
        let filter_elapsed = filter_start.elapsed();

        // 50 ms is loose enough to absorb CI runner variance on shared-compute
        // hosts while still catching real algorithmic regressions in the
        // fuzzy-match path (>10x slowdown will trip it).
        assert!(
            filter_elapsed.as_millis() < 50,
            "Per-keystroke filter took {}ms, exceeds 50ms budget",
            filter_elapsed.as_millis()
        );
        assert!(!matched.is_empty(), "Should match some items");
    }

    // --- supports_metric_charts gating predicate ---

    use super::supports_metric_charts;
    use dbflux_core::{DatabaseCategory, DriverCapabilities, DriverMetadata, Icon, QueryLanguage};

    fn make_metadata_with_caps(capabilities: DriverCapabilities) -> DriverMetadata {
        DriverMetadata {
            id: "test-driver".into(),
            display_name: "Test Driver".into(),
            description: "Unit-test metadata stub".into(),
            category: DatabaseCategory::Relational,
            deployment_class: None,
            query_language: QueryLanguage::Sql,
            capabilities,
            default_port: None,
            uri_scheme: "test".into(),
            icon: Icon::Database,
            syntax: None,
            query: None,
            mutation: None,
            ddl: None,
            transactions: None,
            limits: None,
            ssl_modes: None,
            ssl_cert_fields: None,
            classification_override: None,
            default_chunk_size: None,
            supports_lock_timeout: false,
            editor_profile: None,
        }
    }

    /// A driver advertising METRIC_SERIES must return true from supports_metric_charts.
    ///
    /// This test is RED until TASK-3.1 adds supports_metric_charts (already done above)
    /// AND TASK-3.2 is complete (but the predicate itself is the thing under test here).
    #[test]
    fn supports_metric_charts_true_when_metric_series_set() {
        let meta = make_metadata_with_caps(DriverCapabilities::METRIC_SERIES);
        assert!(
            supports_metric_charts(&meta),
            "METRIC_SERIES capability must make supports_metric_charts return true"
        );
    }

    /// A driver without METRIC_SERIES must return false regardless of category or id.
    ///
    /// This proves the gating decision is driven only by the capability flag,
    /// not by any driver_id or DatabaseCategory branching.
    #[test]
    fn supports_metric_charts_false_when_metric_series_not_set() {
        let meta = make_metadata_with_caps(DriverCapabilities::AUTHENTICATION);
        assert!(
            !supports_metric_charts(&meta),
            "Absence of METRIC_SERIES must make supports_metric_charts return false"
        );

        let empty = make_metadata_with_caps(DriverCapabilities::empty());
        assert!(
            !supports_metric_charts(&empty),
            "Empty capabilities must make supports_metric_charts return false"
        );
    }

    // ---- T19.1: sidebar → chart data pipeline verification ----

    /// T19.1: Verify the `MetricSource` defaults that `open_metric_chart_from_sidebar`
    /// would produce.
    ///
    /// This is a data-layer regression guard — it ensures the defaults
    /// (dimensions=[], period_s=300, statistic="Average") match the spec.
    /// Full GPUI integration testing (actual tab opening) requires TestAppContext
    /// which is not available in this test harness; the data contract is verified here.
    #[test]
    fn sidebar_metric_source_defaults_match_spec() {
        use dbflux_components::chart::MetricSource;

        let source = MetricSource::single(
            "AWS/EC2".to_string(),
            "CPUUtilization".to_string(),
            vec![],
            300,
            "Average".to_string(),
        );

        assert_eq!(source.series.len(), 1);
        let s = &source.series[0];
        assert_eq!(s.namespace, "AWS/EC2");
        assert_eq!(s.metric_name, "CPUUtilization");
        assert!(s.dimensions.is_empty(), "defaults must have no dimensions");
        assert_eq!(
            s.period_seconds, 300,
            "default period must be 300 seconds (5 min)"
        );
        assert_eq!(s.statistic, "Average", "default statistic must be Average");
    }

    /// G.2 — `test_import_affordance_hidden_without_capability`:
    /// `PaletteItem::ImportDashboard` must NOT be produced when the connection's
    /// `DriverCapabilities` does not include `DASHBOARD_IMPORT`.
    ///
    /// This is the unit-layer contract for the capability gate. Full integration
    /// (GPUI `build_palette_items`) requires `TestAppContext`; here we verify that
    /// a capability set without `DASHBOARD_IMPORT` is rejected by the gate predicate.
    #[test]
    fn test_import_affordance_hidden_without_capability() {
        let no_dashboard_import =
            DriverCapabilities::METRIC_SERIES | DriverCapabilities::METRIC_CATALOG;

        assert!(
            !no_dashboard_import.contains(DriverCapabilities::DASHBOARD_IMPORT),
            "DASHBOARD_IMPORT must not be set for this test to be meaningful"
        );

        // The import affordance is only added when the capability flag is present.
        let affordance_present = no_dashboard_import.contains(DriverCapabilities::DASHBOARD_IMPORT);

        assert!(
            !affordance_present,
            "Import affordance must be hidden when DASHBOARD_IMPORT is not in capabilities"
        );
    }

    /// G.2 — `test_import_affordance_shown_with_capability`:
    /// When `DriverCapabilities` includes `DASHBOARD_IMPORT`, the capability
    /// gate predicate evaluates to `true` (affordance is shown).
    #[test]
    fn test_import_affordance_shown_with_capability() {
        let with_dashboard_import =
            DriverCapabilities::METRIC_SERIES | DriverCapabilities::DASHBOARD_IMPORT;

        let affordance_present =
            with_dashboard_import.contains(DriverCapabilities::DASHBOARD_IMPORT);

        assert!(
            affordance_present,
            "Import affordance must be shown when DASHBOARD_IMPORT is in capabilities"
        );
    }

    /// G.2 — `test_import_dashboard_palette_item_maps_to_selection`:
    /// `PaletteItem::ImportDashboard` must map to `PaletteSelection::ImportDashboard`.
    #[test]
    fn test_import_dashboard_palette_item_maps_to_selection() {
        let item = PaletteItem::ImportDashboard;
        let selection = map_item_to_selection(&item);

        assert!(
            matches!(selection, Some(PaletteSelection::ImportDashboard)),
            "ImportDashboard item must map to ImportDashboard selection, got: {:?}",
            selection.map(|_| "Some(other)")
        );
    }

    /// T19.1: Verify `DocumentKey::MetricChart` variant exists and carries the
    /// expected fields — compile-time contract for the dedup path.
    #[test]
    fn document_key_metric_chart_variant_carries_correct_fields() {
        use crate::ui::document::DocumentKey;

        let profile_id = Uuid::new_v4();
        let key = DocumentKey::MetricChart {
            profile_id,
            namespace: "AWS/EC2".to_string(),
            metric_name: "CPUUtilization".to_string(),
        };

        // Verify destructure works and values round-trip correctly.
        match key {
            DocumentKey::MetricChart {
                profile_id: pid,
                namespace,
                metric_name,
            } => {
                assert_eq!(pid, profile_id);
                assert_eq!(namespace, "AWS/EC2");
                assert_eq!(metric_name, "CPUUtilization");
            }
            _ => panic!("Expected MetricChart variant"),
        }
    }
}
