use super::{SecretPathKey, build_secret_store_nodes};
use crate::table_loading::object_tree_adapter_tests::test_app_state;
use crate::{Sidebar, SidebarEvent};
use dbflux_core::{
    Connection, DatabaseCategory, DbError, DriverCapabilities, SchemaNodeId, SchemaSnapshot,
    SecretDetails, SecretListPage, SecretListRequest, SecretStoreCapabilities,
    SecretStoreDescriptor, SecretStoreSource, SecretValue,
};
use dbflux_ui_base::AppStateEntity;
use gpui::{Entity, TestAppContext, VisualTestContext};
use gpui_component::tree::TreeItem;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// The `(profile_id, store_id, path)` of every `OpenSecretStore` event.
type EventLog = Arc<Mutex<Vec<(Uuid, String, Option<String>)>>>;

const PROFILE_ID: Uuid = Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0042);

/// A store that answers every listing from a fixed page table keyed by the
/// requested path and page token, and records each request.
struct PagedStore {
    descriptor: SecretStoreDescriptor,
    pages: HashMap<(Option<String>, Option<String>), SecretListPage>,
    requests: Arc<Mutex<Vec<SecretListRequest>>>,
}

impl SecretStoreSource for PagedStore {
    fn descriptor(&self) -> &SecretStoreDescriptor {
        &self.descriptor
    }

    fn list(&self, request: &SecretListRequest) -> Result<SecretListPage, DbError> {
        if let Ok(mut requests) = self.requests.lock() {
            requests.push(request.clone());
        }

        self.pages
            .get(&(request.path.clone(), request.page_token.clone()))
            .cloned()
            .ok_or_else(|| DbError::query_failed("no such page"))
    }

    fn describe(&self, _name: &str) -> Result<SecretDetails, DbError> {
        Err(DbError::NotSupported("describe".into()))
    }

    fn get_value(&self, _name: &str, _version: Option<&str>) -> Result<SecretValue, DbError> {
        Err(DbError::NotSupported("get_value".into()))
    }
}

struct StoreConnection {
    metadata: dbflux_core::DriverMetadata,
    stores: Vec<PagedStore>,
}

impl Connection for StoreConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        &self.metadata
    }

    fn ping(&self) -> Result<(), DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(
        &self,
        _request: &dbflux_core::QueryRequest,
    ) -> Result<dbflux_core::QueryResult, DbError> {
        Err(DbError::NotSupported("secret store fixture".into()))
    }

    fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        Ok(SchemaSnapshot::default())
    }

    fn kind(&self) -> dbflux_core::DbKind {
        dbflux_core::DbKind::AwsSecrets
    }

    fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
        dbflux_core::SchemaLoadingStrategy::SingleDatabase
    }

    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        &dbflux_core::DefaultSqlDialect
    }

    fn secret_stores(&self) -> Vec<&dyn SecretStoreSource> {
        self.stores
            .iter()
            .map(|store| store as &dyn SecretStoreSource)
            .collect()
    }
}

fn metadata(category: DatabaseCategory) -> dbflux_core::DriverMetadata {
    dbflux_core::DriverMetadata {
        id: "secret-fixture".to_string(),
        display_name: "Secret fixture".to_string(),
        description: "secret store fixture".to_string(),
        category,
        transfer_family: dbflux_core::TransferFamily::Incompatible,
        deployment_class: None,
        query_language: dbflux_core::QueryLanguage::Custom("Secrets".into()),
        capabilities: DriverCapabilities::SECRET_STORE,
        default_port: None,
        uri_scheme: "secret-fixture".to_string(),
        icon: dbflux_core::Icon::Database,
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

fn page(child_paths: &[&str], next_page_token: Option<&str>) -> SecretListPage {
    SecretListPage {
        entries: Vec::new(),
        child_paths: child_paths.iter().map(|path| path.to_string()).collect(),
        next_page_token: next_page_token.map(str::to_string),
    }
}

fn flat_descriptor() -> SecretStoreDescriptor {
    SecretStoreDescriptor::new(
        "secrets-manager",
        "Secrets Manager",
        SecretStoreCapabilities::VERSIONS | SecretStoreCapabilities::ROTATION,
    )
}

fn hierarchical_descriptor() -> SecretStoreDescriptor {
    SecretStoreDescriptor::new(
        "parameter-store",
        "Parameter Store",
        SecretStoreCapabilities::HIERARCHICAL | SecretStoreCapabilities::TYPED_VALUES,
    )
}

/// A flat store and a hierarchical one whose root lists `/app/` and
/// `/shared/` over two pages, repeating `/app/` on the second, and whose
/// `/app/` level lists `/app/prod/`.
fn store_connection(
    category: DatabaseCategory,
    requests: &Arc<Mutex<Vec<SecretListRequest>>>,
) -> StoreConnection {
    let pages = HashMap::from([
        ((None, None), page(&["/app/"], Some("2"))),
        (
            (None, Some("2".to_string())),
            page(&["/app/", "/shared/"], None),
        ),
        (
            (Some("/app/".to_string()), None),
            page(&["/app/prod/"], None),
        ),
    ]);

    StoreConnection {
        metadata: metadata(category),
        stores: vec![
            PagedStore {
                descriptor: flat_descriptor(),
                pages: HashMap::new(),
                requests: requests.clone(),
            },
            PagedStore {
                descriptor: hierarchical_descriptor(),
                pages,
                requests: requests.clone(),
            },
        ],
    }
}

fn connect(
    state: &Entity<AppStateEntity>,
    connection: StoreConnection,
    cx: &mut TestAppContext,
) -> Uuid {
    let mut profile =
        dbflux_core::ConnectionProfile::new("secrets", dbflux_core::DbConfig::default_postgres());
    profile.id = PROFILE_ID;

    state.update(cx, |state, _| {
        state.profiles_mut().push(profile.clone());
        state
            .connection_tree_mut()
            .add_node(dbflux_core::ConnectionTreeNode::new_connection_ref(
                PROFILE_ID, None, 1000,
            ));
        state.apply_connect_profile(
            profile,
            Arc::new(connection),
            Some(SchemaSnapshot::default()),
            None,
            false,
            dbflux_core::WritePrivilege::Unknown,
        );
    });

    PROFILE_ID
}

fn find<'a>(items: &'a [TreeItem], id: &str) -> Option<&'a TreeItem> {
    items.iter().find_map(|item| {
        if item.id.as_ref() == id {
            return Some(item);
        }
        find(&item.children, id)
    })
}

fn child_ids(item: &TreeItem) -> Vec<String> {
    item.children
        .iter()
        .map(|child| child.id.to_string())
        .collect()
}

fn store_id(store_id: &str) -> String {
    SchemaNodeId::SecretStore {
        profile_id: PROFILE_ID,
        store_id: store_id.to_string(),
    }
    .to_string()
}

fn path_id(path: &str) -> String {
    SchemaNodeId::SecretPath {
        profile_id: PROFILE_ID,
        store_id: "parameter-store".to_string(),
        path: path.to_string(),
    }
    .to_string()
}

fn tree_items(sidebar: &Entity<Sidebar>, cx: &mut VisualTestContext) -> Vec<TreeItem> {
    sidebar.update(cx, |sidebar, cx| {
        sidebar.build_tree_items_with_overrides(cx)
    })
}

/// Each store a secret-store connection exposes gets one node labelled from
/// its descriptor, with no count: a flat store is a leaf and a hierarchical
/// one waits on its path listing.
#[test]
fn every_store_gets_a_node_labelled_from_its_descriptor() {
    let nodes = build_secret_store_nodes(
        PROFILE_ID,
        &[flat_descriptor(), hierarchical_descriptor()],
        &HashMap::new(),
    );

    let summary: Vec<(String, String, usize)> = nodes
        .iter()
        .map(|node| {
            (
                node.id.to_string(),
                node.label.to_string(),
                node.children.len(),
            )
        })
        .collect();

    assert_eq!(
        summary,
        vec![
            (
                store_id("secrets-manager"),
                "Secrets Manager".to_string(),
                0
            ),
            (
                store_id("parameter-store"),
                "Parameter Store".to_string(),
                1
            ),
        ]
    );

    let placeholder = &nodes[1].children[0];
    assert!(
        placeholder.id.as_ref().parse::<SchemaNodeId>().is_err(),
        "the loading row is not a navigable node: {}",
        placeholder.id
    );
}

/// A listed path level shows one node per sub-path, labelled with the path
/// without its trailing separator.
#[test]
fn a_listed_level_shows_its_sub_paths() {
    let cache = HashMap::from([(
        SecretPathKey::new(PROFILE_ID, "parameter-store", None),
        vec!["/app/".to_string(), "/shared/".to_string()],
    )]);

    let nodes = build_secret_store_nodes(PROFILE_ID, &[hierarchical_descriptor()], &cache);
    let labels: Vec<String> = nodes[0]
        .children
        .iter()
        .map(|child| child.label.to_string())
        .collect();

    assert_eq!(
        child_ids(&nodes[0]),
        vec![path_id("/app/"), path_id("/shared/")]
    );
    assert_eq!(labels, vec!["/app", "/shared"]);
}

/// The stores appear under a connection of the secret-store category only:
/// the same connection under another category shows none of them.
#[gpui::test]
async fn store_nodes_appear_only_under_a_secret_store_connection(cx: &mut TestAppContext) {
    for (category, expected) in [
        (DatabaseCategory::SecretStore, true),
        (DatabaseCategory::KeyValue, false),
    ] {
        let state = test_app_state(cx);
        let requests = Arc::default();
        connect(&state, store_connection(category, &requests), cx);

        let (sidebar, cx) =
            cx.add_window_view(|window, cx| Sidebar::new(state.clone(), window, cx));
        let items = tree_items(&sidebar, cx);

        assert_eq!(
            find(&items, &store_id("secrets-manager")).is_some(),
            expected,
            "{category:?}"
        );
        assert_eq!(
            find(&items, &store_id("parameter-store")).is_some(),
            expected,
            "{category:?}"
        );
    }
}

/// Expanding a hierarchical store lists its root through `list` without
/// values, follows every page and merges the sub-paths a later page
/// repeats; expanding a path lists that path.
#[gpui::test]
async fn expanding_a_store_lists_its_path_tree(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let requests: Arc<Mutex<Vec<SecretListRequest>>> = Arc::default();
    connect(
        &state,
        store_connection(DatabaseCategory::SecretStore, &requests),
        cx,
    );

    let (sidebar, cx) = cx.add_window_view(|window, cx| Sidebar::new(state.clone(), window, cx));

    sidebar.update(cx, |sidebar, cx| {
        sidebar.set_expanded(&store_id("parameter-store"), true, cx)
    });
    cx.run_until_parked();

    let items = tree_items(&sidebar, cx);
    let store = find(&items, &store_id("parameter-store")).expect("store node");
    assert_eq!(
        child_ids(store),
        vec![path_id("/app/"), path_id("/shared/")]
    );

    sidebar.update(cx, |sidebar, cx| {
        sidebar.set_expanded(&path_id("/app/"), true, cx)
    });
    cx.run_until_parked();

    let items = tree_items(&sidebar, cx);
    let app = find(&items, &path_id("/app/")).expect("path node");
    assert_eq!(child_ids(app), vec![path_id("/app/prod/")]);

    let requests = requests.lock().expect("request log").clone();
    let shapes: Vec<(Option<String>, bool, Option<String>)> = requests
        .iter()
        .map(|request| {
            (
                request.path.clone(),
                request.recursive,
                request.page_token.clone(),
            )
        })
        .collect();
    assert_eq!(
        shapes,
        vec![
            (None, false, None),
            (None, false, Some("2".to_string())),
            (Some("/app/".to_string()), false, None),
        ]
    );
}

/// Activating a store opens its document, and activating a path opens the
/// store's document at that path.
#[gpui::test]
async fn activating_a_store_or_a_path_opens_the_store_document(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let requests = Arc::default();
    connect(
        &state,
        store_connection(DatabaseCategory::SecretStore, &requests),
        cx,
    );

    let (sidebar, cx) = cx.add_window_view(|window, cx| Sidebar::new(state.clone(), window, cx));

    let events: EventLog = Arc::default();
    let _subscription = cx.update(|_, cx| {
        let events = events.clone();
        cx.subscribe(&sidebar, move |_, event: &SidebarEvent, _| {
            if let SidebarEvent::OpenSecretStore {
                profile_id,
                store_id,
                path,
            } = event
            {
                events.lock().expect("event log").push((
                    *profile_id,
                    store_id.clone(),
                    path.clone(),
                ));
            }
        })
    });

    sidebar.update(cx, |sidebar, cx| {
        sidebar.execute_item(&store_id("secrets-manager"), cx);
        sidebar.execute_item(&path_id("/app/prod/"), cx);
    });
    cx.run_until_parked();

    assert_eq!(
        events.lock().expect("event log").clone(),
        vec![
            (PROFILE_ID, "secrets-manager".to_string(), None),
            (
                PROFILE_ID,
                "parameter-store".to_string(),
                Some("/app/prod/".to_string())
            ),
        ]
    );
}
