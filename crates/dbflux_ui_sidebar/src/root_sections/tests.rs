use super::{
    ProfileRootSections, RootSectionListing, build_root_section_children,
    build_root_section_folder, section_folder_label,
};
use crate::table_loading::object_tree_adapter_tests::test_app_state;
use crate::{ContextMenuAction, Sidebar, SidebarEvent};
use dbflux_core::{
    Connection, DatabaseCategory, DbError, DriverCapabilities, ExecutionClassification,
    LocalizedText, MutationPolicy, ObjectStoreConnection, ReadOnlyReason, RootItemPresentation,
    RootSectionAction, RootSectionDef, RootSectionItem, RootSectionPage, RootSectionPresentation,
    RootSectionSource, SchemaNodeId, SchemaNodeKind, SchemaSnapshot,
};
use dbflux_ui_base::AppStateEntity;
use gpui::{Entity, TestAppContext, VisualTestContext};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

const PROFILE_ID: Uuid = Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0085);

/// The `buckets` section as the S3 driver declares it.
pub(crate) fn buckets_section() -> RootSectionDef {
    RootSectionDef {
        id: "buckets".to_string(),
        label: LocalizedText::keyed("sidebar.tree.folder.buckets", "Buckets"),
        counted_label_key: Some("sidebar.tree.container.object_storage".to_string()),
        empty_label: LocalizedText::keyed("sidebar.tree.empty.buckets", "No buckets"),
        presentation: Some(RootSectionPresentation::ObjectContainerTable),
        item_presentation: RootItemPresentation::ObjectBrowser,
        opens_on_profile_activation: true,
        item_actions: vec![
            RootSectionAction {
                id: "delete_bucket".to_string(),
                label: LocalizedText::keyed(
                    "sidebar.root_section.action.delete_bucket",
                    "Delete bucket",
                ),
                description: None,
                classification: ExecutionClassification::AdminDestructive,
            },
            RootSectionAction {
                id: "inspect".to_string(),
                label: LocalizedText::plain("Inspect"),
                description: None,
                classification: ExecutionClassification::Read,
            },
        ],
    }
}

pub(crate) fn items(names: &[&str]) -> Vec<RootSectionItem> {
    names
        .iter()
        .map(|name| RootSectionItem {
            id: name.to_string(),
            label: name.to_string(),
        })
        .collect()
}

fn listing(names: &[&str], next_page_token: Option<&str>) -> RootSectionListing {
    RootSectionListing {
        items: items(names),
        next_page_token: next_page_token.map(str::to_string),
    }
}

fn item_node(item_id: &str) -> String {
    SchemaNodeId::RootSectionItem {
        profile_id: PROFILE_ID,
        section_id: "buckets".to_string(),
        item_id: item_id.to_string(),
    }
    .to_string()
}

fn section_node() -> String {
    SchemaNodeId::RootSection {
        profile_id: PROFILE_ID,
        section_id: "buckets".to_string(),
    }
    .to_string()
}

/// Before the listing resolves the section shows a single non-clickable
/// loading placeholder, never an empty node.
#[test]
fn section_shows_a_loading_placeholder_until_its_first_page() {
    let children = build_root_section_children(PROFILE_ID, &buckets_section(), None);

    assert_eq!(children.len(), 1);
    assert_eq!(
        children[0].label.as_ref(),
        dbflux_i18n::t!("sidebar.tree.status.loading")
    );
    assert!(
        children[0].id.as_ref().parse::<SchemaNodeId>().is_err(),
        "the placeholder must not parse as a node: {}",
        children[0].id
    );
}

/// Items render flat, one clickable row each, with no children.
#[test]
fn items_render_one_flat_row_each() {
    let children = build_root_section_children(
        PROFILE_ID,
        &buckets_section(),
        Some(&listing(&["prod-logs", "media.assets"], None)),
    );

    assert!(children.iter().all(|item| item.children.is_empty()));
    let ids: Vec<String> = children.iter().map(|item| item.id.to_string()).collect();
    assert_eq!(ids, vec![item_node("prod-logs"), item_node("media.assets")]);
}

#[test]
fn folder_wraps_the_rows_and_counts_them() {
    let folder = build_root_section_folder(
        PROFILE_ID,
        &buckets_section(),
        Some(&listing(&["avatars", "exports"], None)),
        None,
    );

    assert_eq!(folder.id.as_ref(), section_node());
    assert_eq!(
        folder.label.as_ref(),
        crate::labels::container_folder_label(DatabaseCategory::ObjectStorage, 2)
    );
    assert_eq!(folder.children.len(), 2);
}

#[test]
fn folder_shows_a_plain_label_while_the_listing_loads() {
    let folder = build_root_section_folder(PROFILE_ID, &buckets_section(), None, None);

    assert_eq!(
        folder.label.as_ref(),
        dbflux_i18n::t!("sidebar.tree.folder.buckets")
    );
    assert_eq!(folder.children.len(), 1);
}

/// A section with no items says so instead of rendering nothing.
#[test]
fn an_empty_listing_reports_it() {
    let children =
        build_root_section_children(PROFILE_ID, &buckets_section(), Some(&listing(&[], None)));

    assert_eq!(children.len(), 1);
    assert_eq!(
        children[0].label.as_ref(),
        dbflux_i18n::t!("sidebar.tree.empty.buckets")
    );
}

/// A listing with a next page ends with a load more row; the last page has
/// none.
#[test]
fn a_load_more_row_follows_a_listing_with_a_next_page() {
    let paged = build_root_section_children(
        PROFILE_ID,
        &buckets_section(),
        Some(&listing(&["a", "b"], Some("page-2"))),
    );
    let last_page =
        build_root_section_children(PROFILE_ID, &buckets_section(), Some(&listing(&["a"], None)));

    let last = paged.last().expect("rows");
    assert_eq!(paged.len(), 3);
    assert_eq!(
        last.id.as_ref().parse::<SchemaNodeId>(),
        Ok(SchemaNodeId::RootSectionLoadMore {
            profile_id: PROFILE_ID,
            section_id: "buckets".to_string(),
        })
    );
    assert_eq!(
        last.label.as_ref(),
        dbflux_i18n::t!("sidebar.tree.status.load_more")
    );
    assert!(
        last_page
            .iter()
            .all(|row| crate::parse_node_kind(row.id.as_ref())
                != SchemaNodeKind::RootSectionLoadMore)
    );
}

/// Keys the catalog does not know fall back to the declared text, and a
/// count without a known counted key uses the generic count format.
#[test]
fn unknown_label_keys_fall_back_to_the_declared_text() {
    let section = RootSectionDef {
        id: "topics".to_string(),
        label: LocalizedText::keyed("no.such.topics.label", "Topics"),
        counted_label_key: Some("no.such.topics.counted".to_string()),
        empty_label: LocalizedText::keyed("no.such.topics.empty", "No topics"),
        ..buckets_section()
    };

    assert_eq!(section_folder_label(&section, None), "Topics");
    assert_eq!(
        section_folder_label(&section, Some(3)),
        dbflux_i18n::t!(
            "sidebar.tree.status.remote_listing_count",
            label = "Topics",
            count = 3
        )
    );

    let children = build_root_section_children(PROFILE_ID, &section, Some(&listing(&[], None)));
    assert_eq!(children[0].label.as_ref(), "No topics");
}

/// A counted key whose catalog text has no count placeholder still shows the
/// count, through the generic count format.
#[test]
fn a_counted_label_without_a_count_placeholder_keeps_the_count() {
    let section = RootSectionDef {
        counted_label_key: Some("sidebar.tree.folder.buckets".to_string()),
        ..buckets_section()
    };

    assert_eq!(
        section_folder_label(&section, Some(2)),
        dbflux_i18n::t!(
            "sidebar.tree.status.remote_listing_count",
            label = dbflux_i18n::t!("sidebar.tree.folder.buckets"),
            count = 2
        )
    );
}

// --- connected fixtures ---

/// Every declaration, `list_items` and `execute_action` call the fixture
/// received.
#[derive(Default)]
struct SourceLog {
    declarations: usize,
    listings: Vec<(String, Option<String>)>,
    actions: Vec<(String, String, String)>,
}

struct SectionConnection {
    metadata: dbflux_core::DriverMetadata,
    sections: Vec<RootSectionDef>,
    pages: Mutex<HashMap<Option<String>, RootSectionPage>>,
    failing_declarations: AtomicUsize,
    failing_listings: AtomicUsize,
    fail_actions: bool,
    has_object_store: bool,
    log: Arc<Mutex<SourceLog>>,
}

impl SectionConnection {
    /// A connection whose `buckets` section lists `alpha` then, on a second
    /// page, `beta`.
    fn new(log: &Arc<Mutex<SourceLog>>) -> Self {
        let first = RootSectionPage {
            items: items(&["alpha"]),
            next_page_token: Some("2".to_string()),
        };
        let second = RootSectionPage {
            items: items(&["beta"]),
            next_page_token: None,
        };

        Self {
            metadata: metadata(),
            sections: vec![buckets_section()],
            pages: Mutex::new(HashMap::from([
                (None, first),
                (Some("2".to_string()), second),
            ])),
            failing_declarations: AtomicUsize::new(0),
            failing_listings: AtomicUsize::new(0),
            fail_actions: false,
            has_object_store: true,
            log: log.clone(),
        }
    }

    fn failing_actions(mut self) -> Self {
        self.fail_actions = true;
        self
    }

    fn declaring(mut self, sections: Vec<RootSectionDef>) -> Self {
        self.sections = sections;
        self
    }

    fn without_object_store(mut self) -> Self {
        self.has_object_store = false;
        self
    }

    fn failing_first_declarations(self, count: usize) -> Self {
        self.failing_declarations.store(count, Ordering::SeqCst);
        self
    }

    fn failing_first_listings(self, count: usize) -> Self {
        self.failing_listings.store(count, Ordering::SeqCst);
        self
    }
}

/// Consumes one of the remaining scripted failures, if any is left.
fn take_failure(remaining: &AtomicUsize) -> bool {
    remaining
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
            left.checked_sub(1)
        })
        .is_ok()
}

impl RootSectionSource for SectionConnection {
    fn sections(&self) -> Result<Vec<RootSectionDef>, DbError> {
        if let Ok(mut log) = self.log.lock() {
            log.declarations += 1;
        }

        if take_failure(&self.failing_declarations) {
            return Err(DbError::connection_failed("driver host unavailable"));
        }
        Ok(self.sections.clone())
    }

    fn list_items(
        &self,
        section_id: &str,
        page_token: Option<&str>,
    ) -> Result<RootSectionPage, DbError> {
        if let Ok(mut log) = self.log.lock() {
            log.listings
                .push((section_id.to_string(), page_token.map(str::to_string)));
        }

        if take_failure(&self.failing_listings) {
            return Err(DbError::query_failed("listing unavailable"));
        }

        self.pages
            .lock()
            .map_err(|_| DbError::query_failed("fixture pages poisoned"))?
            .get(&page_token.map(str::to_string))
            .cloned()
            .ok_or_else(|| DbError::query_failed("no such page"))
    }

    fn execute_action(
        &self,
        section_id: &str,
        item_id: &str,
        action_id: &str,
    ) -> Result<(), DbError> {
        if let Ok(mut log) = self.log.lock() {
            log.actions.push((
                section_id.to_string(),
                item_id.to_string(),
                action_id.to_string(),
            ));
        }

        if self.fail_actions {
            return Err(DbError::query_failed("bucket is not empty"));
        }

        if let Ok(mut pages) = self.pages.lock() {
            for page in pages.values_mut() {
                page.items.retain(|item| item.id != item_id);
            }
        }
        Ok(())
    }
}

#[allow(clippy::result_large_err)]
fn unused<T>() -> Result<T, DbError> {
    Err(DbError::NotSupported("root section fixture".into()))
}

/// The sections present items in the object browser, which needs an object
/// store; none of its operations is reached by these tests.
impl ObjectStoreConnection for SectionConnection {
    fn list_buckets(&self) -> Result<Vec<dbflux_core::BucketInfo>, DbError> {
        unused()
    }

    fn list_objects(
        &self,
        _bucket: &str,
        _prefix: &str,
        _continuation_token: Option<&str>,
    ) -> Result<dbflux_core::ObjectListingPage, DbError> {
        unused()
    }

    fn head_object(
        &self,
        _bucket: &str,
        _key: &str,
    ) -> Result<dbflux_core::ObjectMetadata, DbError> {
        unused()
    }

    fn get_object(&self, _bucket: &str, _key: &str) -> Result<Vec<u8>, DbError> {
        unused()
    }

    fn download_object(
        &self,
        _bucket: &str,
        _key: &str,
        _dest: &std::path::Path,
    ) -> Result<u64, DbError> {
        unused()
    }

    fn put_object(
        &self,
        _bucket: &str,
        _key: &str,
        _bytes: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<(), DbError> {
        unused()
    }

    fn upload_object(
        &self,
        _bucket: &str,
        _key: &str,
        _source_path: &std::path::Path,
        _content_type: Option<&str>,
    ) -> Result<(), DbError> {
        unused()
    }

    fn delete_object(&self, _bucket: &str, _key: &str) -> Result<(), DbError> {
        unused()
    }

    fn delete_prefix(
        &self,
        _bucket: &str,
        _prefix: &str,
    ) -> Result<dbflux_core::DeletePrefixOutcome, DbError> {
        unused()
    }

    fn copy_object(&self, _bucket: &str, _src_key: &str, _dest_key: &str) -> Result<(), DbError> {
        unused()
    }

    fn presign(
        &self,
        _bucket: &str,
        _key: &str,
        _method: dbflux_core::PresignMethod,
        _expiry: std::time::Duration,
    ) -> Result<String, DbError> {
        unused()
    }

    fn get_bucket_details(&self, _bucket: &str) -> Result<dbflux_core::BucketDetails, DbError> {
        unused()
    }

    fn estimate_bucket_size(
        &self,
        _bucket: &str,
        _object_cap: u64,
    ) -> Result<dbflux_core::BucketSizeEstimate, DbError> {
        unused()
    }

    fn list_object_versions(
        &self,
        _bucket: &str,
        _key: &str,
    ) -> Result<Vec<dbflux_core::ObjectVersionSummary>, DbError> {
        unused()
    }

    fn create_bucket(
        &self,
        _bucket: &str,
        _options: dbflux_core::BucketCreateOptions,
    ) -> Result<dbflux_core::BucketCreateOutcome, DbError> {
        unused()
    }

    fn delete_bucket(&self, _bucket: &str) -> Result<(), DbError> {
        unused()
    }
}

impl Connection for SectionConnection {
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
        Err(DbError::NotSupported("root section fixture".into()))
    }

    fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        Ok(SchemaSnapshot::default())
    }

    fn kind(&self) -> dbflux_core::DbKind {
        dbflux_core::DbKind::S3
    }

    fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
        dbflux_core::SchemaLoadingStrategy::SingleDatabase
    }

    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        &dbflux_core::DefaultSqlDialect
    }

    fn object_store_api(&self) -> Option<&dyn ObjectStoreConnection> {
        self.has_object_store
            .then_some(self as &dyn ObjectStoreConnection)
    }

    fn root_section_source(&self) -> Option<&dyn RootSectionSource> {
        Some(self)
    }
}

fn metadata() -> dbflux_core::DriverMetadata {
    dbflux_core::DriverMetadata {
        id: "section-fixture".to_string(),
        display_name: "Section fixture".to_string(),
        description: "root section fixture".to_string(),
        category: DatabaseCategory::ObjectStorage,
        transfer_family: dbflux_core::TransferFamily::Incompatible,
        deployment_class: None,
        query_language: dbflux_core::QueryLanguage::Custom("Sections".into()),
        capabilities: DriverCapabilities::ROOT_SECTIONS,
        default_port: None,
        uri_scheme: "section-fixture".to_string(),
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

fn connect(state: &Entity<AppStateEntity>, connection: SectionConnection, cx: &mut TestAppContext) {
    let mut profile =
        dbflux_core::ConnectionProfile::new("sections", dbflux_core::DbConfig::default_postgres());
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
}

/// A sidebar over `connection`, connected, before anything is fetched.
fn connected_sidebar(
    connection: SectionConnection,
    cx: &mut TestAppContext,
) -> (Entity<Sidebar>, &mut VisualTestContext) {
    let state = test_app_state(cx);
    connect(&state, connection, cx);

    cx.add_window_view(|window, cx| Sidebar::new(state.clone(), window, cx))
}

/// A sidebar over `connection` whose declarations and first page are loaded.
fn loaded_sidebar(
    connection: SectionConnection,
    cx: &mut TestAppContext,
) -> (Entity<Sidebar>, &mut VisualTestContext) {
    let (sidebar, cx) = connected_sidebar(connection, cx);
    sidebar.update(cx, |sidebar, cx| {
        sidebar.spawn_fetch_root_sections(PROFILE_ID, cx)
    });
    cx.run_until_parked();

    (sidebar, cx)
}

fn app_state(sidebar: &Entity<Sidebar>, cx: &mut VisualTestContext) -> Entity<AppStateEntity> {
    sidebar.update(cx, |sidebar, _| sidebar.app_state.clone())
}

fn cached_ids(sidebar: &Entity<Sidebar>, cx: &mut VisualTestContext) -> Vec<String> {
    sidebar.update(cx, |sidebar, _| {
        sidebar
            .root_section_cache
            .get(&PROFILE_ID)
            .and_then(|cached: &ProfileRootSections| cached.listings.get("buckets"))
            .map(|listing| listing.items.iter().map(|item| item.id.clone()).collect())
            .unwrap_or_default()
    })
}

fn cached_section_ids(
    sidebar: &Entity<Sidebar>,
    cx: &mut VisualTestContext,
) -> Option<Vec<String>> {
    sidebar.update(cx, |sidebar, _| {
        sidebar.root_section_cache.get(&PROFILE_ID).map(|cached| {
            cached
                .sections
                .iter()
                .map(|section| section.id.clone())
                .collect()
        })
    })
}

/// The built connections tree, depth first, as `(id, label)` rows.
fn tree_rows(sidebar: &Entity<Sidebar>, cx: &mut VisualTestContext) -> Vec<(String, String)> {
    fn walk(items: &[gpui_component::tree::TreeItem], rows: &mut Vec<(String, String)>) {
        for item in items {
            rows.push((item.id.to_string(), item.label.to_string()));
            walk(&item.children, rows);
        }
    }

    let items = sidebar.update(cx, |sidebar, cx| {
        sidebar.build_tree_items_with_overrides(cx)
    });
    let mut rows = Vec::new();
    walk(&items, &mut rows);
    rows
}

fn last_toast(cx: &mut VisualTestContext) -> Option<String> {
    cx.update(|_, cx| {
        cx.global::<dbflux_ui_base::toast::ToastGlobal>()
            .host
            .read(cx)
            .last_toast_title()
    })
}

fn profile_node() -> String {
    SchemaNodeId::Profile {
        profile_id: PROFILE_ID,
    }
    .to_string()
}

/// Records every root section open event the sidebar emits.
fn record_open_events(
    sidebar: &Entity<Sidebar>,
    cx: &mut VisualTestContext,
) -> (Arc<Mutex<Vec<String>>>, gpui::Subscription) {
    let events: Arc<Mutex<Vec<String>>> = Arc::default();
    let subscription = cx.update(|_, cx| {
        let events = events.clone();
        cx.subscribe(sidebar, move |_, event: &SidebarEvent, _| {
            let entry = match event {
                SidebarEvent::OpenRootSection {
                    section_id,
                    presentation,
                    ..
                } => format!("section {section_id} {presentation:?}"),
                SidebarEvent::OpenRootSectionItem {
                    item_id,
                    presentation,
                    ..
                } => format!("item {item_id} {presentation:?}"),
                _ => return,
            };
            events.lock().expect("event log").push(entry);
        })
    });

    (events, subscription)
}

fn set_mutation_policy(
    sidebar: &Entity<Sidebar>,
    policy: MutationPolicy,
    reason: Option<ReadOnlyReason>,
    cx: &mut VisualTestContext,
) {
    let state = app_state(sidebar, cx);
    state.update(cx, |state, _| {
        if let Some(connected) = state.connections_mut().get_mut(&PROFILE_ID) {
            connected.mutation_policy = policy;
            connected.read_only_reason = reason;
        }
    });
}

#[gpui::test]
async fn load_more_appends_the_next_page(cx: &mut TestAppContext) {
    let log = Arc::default();
    let (sidebar, cx) = loaded_sidebar(SectionConnection::new(&log), cx);
    assert_eq!(cached_ids(&sidebar, cx), vec!["alpha"]);

    let load_more = SchemaNodeId::RootSectionLoadMore {
        profile_id: PROFILE_ID,
        section_id: "buckets".to_string(),
    }
    .to_string();
    sidebar.update(cx, |sidebar, cx| sidebar.execute_item(&load_more, cx));
    cx.run_until_parked();

    assert_eq!(cached_ids(&sidebar, cx), vec!["alpha", "beta"]);
    assert_eq!(
        log.lock().expect("source log").listings,
        vec![
            ("buckets".to_string(), None),
            ("buckets".to_string(), Some("2".to_string())),
        ]
    );
}

#[gpui::test]
async fn the_item_menu_lists_open_and_every_declared_action(cx: &mut TestAppContext) {
    let log = Arc::default();
    let (sidebar, cx) = loaded_sidebar(SectionConnection::new(&log), cx);

    let entries: Vec<(String, bool, Option<String>)> = sidebar.update(cx, |sidebar, cx| {
        sidebar
            .build_context_menu_items(SchemaNodeKind::RootSectionItem, &item_node("alpha"), cx)
            .into_iter()
            .filter(|item| !item.is_separator && !item.is_header)
            .map(|item| {
                let action_id = match item.action {
                    ContextMenuAction::RootSectionAction(id) => Some(id),
                    _ => None,
                };
                (item.label, item.is_danger, action_id)
            })
            .collect()
    });

    assert_eq!(
        entries,
        vec![
            (dbflux_i18n::t!("sidebar.menu.open"), false, None),
            (
                dbflux_i18n::t!("sidebar.root_section.action.delete_bucket"),
                true,
                Some("delete_bucket".to_string())
            ),
            ("Inspect".to_string(), false, Some("inspect".to_string())),
        ]
    );
}

/// An `AdminDestructive` action waits for confirmation and runs only once
/// confirmed; the section is listed again afterwards, without the item.
#[gpui::test]
async fn a_destructive_action_runs_only_after_confirmation(cx: &mut TestAppContext) {
    let log: Arc<Mutex<SourceLog>> = Arc::default();
    let (sidebar, cx) = loaded_sidebar(SectionConnection::new(&log), cx);
    assert_eq!(cached_ids(&sidebar, cx), vec!["alpha"]);

    sidebar.update(cx, |sidebar, cx| {
        sidebar.request_root_section_action(&item_node("alpha"), "delete_bucket", cx)
    });
    cx.run_until_parked();

    let confirm = sidebar.update(cx, |sidebar, _| {
        sidebar
            .delete_modal_state()
            .and_then(|state| state.root_action)
            .map(|copy| (copy.title, copy.confirm_label))
    });
    assert_eq!(
        confirm,
        Some((
            dbflux_i18n::t!("sidebar.root_section.action.delete_bucket"),
            dbflux_i18n::t!("sidebar.root_section.action.delete_bucket"),
        ))
    );
    assert!(log.lock().expect("source log").actions.is_empty());

    sidebar.update(cx, |sidebar, cx| sidebar.confirm_modal_delete(cx));
    cx.run_until_parked();

    {
        let log = log.lock().expect("source log");
        assert_eq!(
            log.actions,
            vec![(
                "buckets".to_string(),
                "alpha".to_string(),
                "delete_bucket".to_string()
            )]
        );
        assert_eq!(
            log.listings.last(),
            Some(&("buckets".to_string(), None)),
            "a successful action lists the section again"
        );
        assert_eq!(log.listings.len(), 2);
    }
    assert!(
        cached_ids(&sidebar, cx).is_empty(),
        "the refreshed listing replaces the cached items"
    );
}

/// A `Read` action runs right away, with no confirmation.
#[gpui::test]
async fn a_read_action_runs_without_confirmation(cx: &mut TestAppContext) {
    let log: Arc<Mutex<SourceLog>> = Arc::default();
    let (sidebar, cx) = loaded_sidebar(SectionConnection::new(&log), cx);

    sidebar.update(cx, |sidebar, cx| {
        sidebar.request_root_section_action(&item_node("alpha"), "inspect", cx)
    });
    cx.run_until_parked();

    assert!(!sidebar.update(cx, |sidebar, _| sidebar.has_delete_modal()));
    assert_eq!(
        log.lock().expect("source log").actions,
        vec![(
            "buckets".to_string(),
            "alpha".to_string(),
            "inspect".to_string()
        )]
    );
}

/// A failed action is reported to the user and does not list the section
/// again.
#[gpui::test]
async fn a_failed_action_is_reported(cx: &mut TestAppContext) {
    let log: Arc<Mutex<SourceLog>> = Arc::default();
    let (sidebar, cx) = loaded_sidebar(SectionConnection::new(&log).failing_actions(), cx);

    sidebar.update(cx, |sidebar, cx| {
        sidebar.request_root_section_action(&item_node("alpha"), "inspect", cx)
    });
    cx.run_until_parked();

    assert_eq!(
        last_toast(cx),
        Some(dbflux_i18n::t!(
            "sidebar.toast.root_section_action_failed",
            action = "Inspect",
            item = "alpha",
            error = DbError::query_failed("bucket is not empty")
        ))
    );
    assert_eq!(log.lock().expect("source log").listings.len(), 1);
}

/// Activating the section folder, an item, or the connection row opens the
/// declared presentations.
#[gpui::test]
async fn activation_opens_the_declared_presentations(cx: &mut TestAppContext) {
    let log = Arc::default();
    let (sidebar, cx) = loaded_sidebar(SectionConnection::new(&log), cx);
    let (events, _subscription) = record_open_events(&sidebar, cx);

    sidebar.update(cx, |sidebar, cx| {
        sidebar.execute_item(&section_node(), cx);
        sidebar.execute_item(&item_node("alpha"), cx);
        sidebar.execute_item(&profile_node(), cx);
    });
    cx.run_until_parked();

    assert_eq!(
        events.lock().expect("event log").clone(),
        vec![
            "section buckets ObjectContainerTable",
            "item alpha ObjectBrowser",
            "section buckets ObjectContainerTable",
        ]
    );
}

/// A declared section id with the node id separator would be misparsed by
/// every node built from it, so it is dropped and reported; the other
/// sections still load.
#[gpui::test]
async fn a_section_id_with_the_node_separator_is_rejected_and_reported(cx: &mut TestAppContext) {
    let log = Arc::default();
    let separated = RootSectionDef {
        id: "reports|daily".to_string(),
        ..buckets_section()
    };
    let connection = SectionConnection::new(&log).declaring(vec![buckets_section(), separated]);
    let (sidebar, cx) = loaded_sidebar(connection, cx);

    assert_eq!(
        cached_section_ids(&sidebar, cx),
        Some(vec!["buckets".to_string()])
    );
    assert_eq!(
        last_toast(cx),
        Some(dbflux_i18n::t!(
            "sidebar.toast.root_section_invalid_id",
            section = "reports|daily"
        ))
    );
    assert!(
        tree_rows(&sidebar, cx)
            .iter()
            .all(|(id, _)| !id.contains("reports")),
        "the rejected section renders no row"
    );
}

/// Both declared presentations open object-store documents, so a connection
/// without an object store hides the sections instead of opening documents
/// whose every load fails.
#[gpui::test]
async fn sections_that_need_an_object_store_are_hidden_without_one(cx: &mut TestAppContext) {
    let log = Arc::default();
    let (sidebar, cx) = loaded_sidebar(SectionConnection::new(&log).without_object_store(), cx);

    assert_eq!(cached_section_ids(&sidebar, cx), Some(Vec::new()));
    assert!(
        tree_rows(&sidebar, cx)
            .iter()
            .all(|(id, _)| !id.starts_with("RSC")),
        "no section row renders"
    );
    assert!(
        log.lock().expect("source log").listings.is_empty(),
        "a hidden section is never listed"
    );
}

/// A failed declaration fetch is shown as a retry row and not cached:
/// activating the row asks the driver again.
#[gpui::test]
async fn failed_declarations_are_shown_and_asked_again(cx: &mut TestAppContext) {
    let log: Arc<Mutex<SourceLog>> = Arc::default();
    let connection = SectionConnection::new(&log).failing_first_declarations(1);
    let (sidebar, cx) = loaded_sidebar(connection, cx);

    assert_eq!(cached_section_ids(&sidebar, cx), None);
    let retry_label = crate::labels::error_retry_label(
        &DbError::connection_failed("driver host unavailable").to_string(),
    );
    let retry_row = tree_rows(&sidebar, cx)
        .into_iter()
        .find(|(_, label)| *label == retry_label)
        .map(|(id, _)| id)
        .expect("the failed declarations show a retry row");

    sidebar.update(cx, |sidebar, cx| sidebar.execute_item(&retry_row, cx));
    cx.run_until_parked();

    assert_eq!(log.lock().expect("source log").declarations, 2);
    assert_eq!(
        cached_section_ids(&sidebar, cx),
        Some(vec!["buckets".to_string()])
    );
    assert_eq!(cached_ids(&sidebar, cx), vec!["alpha"]);
}

/// A connection row activated before the declarations arrive still opens
/// the section that opens on profile activation, once they land.
#[gpui::test]
async fn a_profile_activation_before_the_declarations_load_opens_the_section_once_they_land(
    cx: &mut TestAppContext,
) {
    let log = Arc::default();
    let (sidebar, cx) = connected_sidebar(SectionConnection::new(&log), cx);
    let (events, _subscription) = record_open_events(&sidebar, cx);

    sidebar.update(cx, |sidebar, cx| sidebar.execute_item(&profile_node(), cx));
    cx.run_until_parked();

    assert_eq!(
        events.lock().expect("event log").clone(),
        vec!["section buckets ObjectContainerTable"]
    );
}

/// A confirmation answered after the profile reconnected runs on neither
/// connection: the request is bound to the session it was made on.
#[gpui::test]
async fn a_confirmed_action_does_not_run_on_a_replaced_connection(cx: &mut TestAppContext) {
    let first_log: Arc<Mutex<SourceLog>> = Arc::default();
    let second_log: Arc<Mutex<SourceLog>> = Arc::default();
    let (sidebar, cx) = loaded_sidebar(SectionConnection::new(&first_log), cx);

    sidebar.update(cx, |sidebar, cx| {
        sidebar.request_root_section_action(&item_node("alpha"), "delete_bucket", cx)
    });
    cx.run_until_parked();
    assert!(sidebar.update(cx, |sidebar, _| sidebar.has_delete_modal()));

    let replacement: Arc<dyn Connection> = Arc::new(SectionConnection::new(&second_log));
    let state = app_state(&sidebar, cx);
    state.update(cx, |state, _| {
        if let Some(connected) = state.connections_mut().get_mut(&PROFILE_ID) {
            connected.connection = replacement;
        }
    });

    sidebar.update(cx, |sidebar, cx| sidebar.confirm_modal_delete(cx));
    cx.run_until_parked();

    assert!(first_log.lock().expect("first log").actions.is_empty());
    assert!(second_log.lock().expect("second log").actions.is_empty());
    assert_eq!(
        last_toast(cx),
        Some(dbflux_i18n::t!(
            "sidebar.toast.root_section_action_connection_changed",
            action = dbflux_i18n::t!("sidebar.root_section.action.delete_bucket"),
            item = "alpha"
        ))
    );
}

/// A failed first page shows a retry row instead of loading forever, and
/// activating it lists the section again.
#[gpui::test]
async fn a_failed_listing_shows_a_retry_row_that_lists_again(cx: &mut TestAppContext) {
    let log: Arc<Mutex<SourceLog>> = Arc::default();
    let (sidebar, cx) = loaded_sidebar(SectionConnection::new(&log).failing_first_listings(1), cx);

    let rows = tree_rows(&sidebar, cx);
    assert!(
        rows.iter()
            .all(|(_, label)| *label != dbflux_i18n::t!("sidebar.tree.status.loading")),
        "a failed listing must not stay on the loading row: {rows:?}"
    );
    let retry_label =
        crate::labels::error_retry_label(&DbError::query_failed("listing unavailable").to_string());
    let retry_row = rows
        .into_iter()
        .find(|(_, label)| *label == retry_label)
        .map(|(id, _)| id)
        .expect("the failed listing shows a retry row");

    sidebar.update(cx, |sidebar, cx| sidebar.execute_item(&retry_row, cx));
    cx.run_until_parked();

    assert_eq!(cached_ids(&sidebar, cx), vec!["alpha"]);
    assert_eq!(log.lock().expect("source log").listings.len(), 2);
}

/// A refresh that replaces a fetch still in flight settles that fetch's
/// task, so no listing task is left running.
#[gpui::test]
async fn a_refresh_settles_the_task_of_the_fetch_it_replaces(cx: &mut TestAppContext) {
    let log: Arc<Mutex<SourceLog>> = Arc::default();
    let (sidebar, cx) = loaded_sidebar(SectionConnection::new(&log), cx);

    sidebar.update(cx, |sidebar, cx| {
        sidebar.refresh_root_section(PROFILE_ID, "buckets", cx);
        sidebar.refresh_root_section(PROFILE_ID, "buckets", cx);
    });
    cx.run_until_parked();

    let state = app_state(&sidebar, cx);
    let running: Vec<String> = state.update(cx, |state, _| {
        state
            .running_tasks()
            .into_iter()
            .map(|task| task.description)
            .collect()
    });
    assert!(running.is_empty(), "tasks left running: {running:?}");
    assert_eq!(cached_ids(&sidebar, cx), vec!["alpha"]);
}

/// A read-only connection refuses every action above `Read` before asking
/// for confirmation, and still runs `Read` actions.
#[gpui::test]
async fn a_read_only_connection_refuses_actions_above_read(cx: &mut TestAppContext) {
    let log: Arc<Mutex<SourceLog>> = Arc::default();
    let (sidebar, cx) = loaded_sidebar(SectionConnection::new(&log), cx);
    set_mutation_policy(
        &sidebar,
        MutationPolicy::ReadOnly,
        Some(ReadOnlyReason::ProfileSetting),
        cx,
    );

    sidebar.update(cx, |sidebar, cx| {
        sidebar.request_root_section_action(&item_node("alpha"), "delete_bucket", cx)
    });
    cx.run_until_parked();

    assert!(!sidebar.update(cx, |sidebar, _| sidebar.has_delete_modal()));
    assert!(log.lock().expect("source log").actions.is_empty());
    assert_eq!(
        last_toast(cx),
        Some(dbflux_i18n::t!(
            "sidebar.toast.root_section_action_refused.read_only_profile",
            action = dbflux_i18n::t!("sidebar.root_section.action.delete_bucket")
        ))
    );

    sidebar.update(cx, |sidebar, cx| {
        sidebar.request_root_section_action(&item_node("alpha"), "inspect", cx)
    });
    cx.run_until_parked();

    assert_eq!(
        log.lock().expect("source log").actions,
        vec![(
            "buckets".to_string(),
            "alpha".to_string(),
            "inspect".to_string()
        )]
    );
}

/// A connection that requires approval refuses actions above `Read`, since
/// root section actions have no approval route yet; a policy that changes
/// while the confirmation is open is checked again when it is answered.
#[gpui::test]
async fn an_approval_connection_refuses_actions_above_read(cx: &mut TestAppContext) {
    let log: Arc<Mutex<SourceLog>> = Arc::default();
    let (sidebar, cx) = loaded_sidebar(SectionConnection::new(&log), cx);

    sidebar.update(cx, |sidebar, cx| {
        sidebar.request_root_section_action(&item_node("alpha"), "delete_bucket", cx)
    });
    cx.run_until_parked();
    assert!(sidebar.update(cx, |sidebar, _| sidebar.has_delete_modal()));

    set_mutation_policy(&sidebar, MutationPolicy::ApprovalRequired, None, cx);
    sidebar.update(cx, |sidebar, cx| sidebar.confirm_modal_delete(cx));
    cx.run_until_parked();

    assert!(log.lock().expect("source log").actions.is_empty());
    assert_eq!(
        last_toast(cx),
        Some(dbflux_i18n::t!(
            "sidebar.toast.root_section_action_refused.approval_unavailable",
            action = dbflux_i18n::t!("sidebar.root_section.action.delete_bucket")
        ))
    );
}

/// An action that succeeded but whose audit event was not recorded reports
/// the audit failure on its own, and still lists the section again.
#[gpui::test]
async fn an_audit_failure_is_reported_without_failing_the_action(cx: &mut TestAppContext) {
    let log: Arc<Mutex<SourceLog>> = Arc::default();
    let (sidebar, cx) = loaded_sidebar(SectionConnection::new(&log), cx);
    let request = super::RootSectionActionRequest {
        profile_id: PROFILE_ID,
        section_id: "buckets".to_string(),
        section_presentation: Some(RootSectionPresentation::ObjectContainerTable),
        item_id: "alpha".to_string(),
        item_label: "alpha".to_string(),
        action: buckets_section()
            .item_action("delete_bucket")
            .cloned()
            .expect("the declared delete action"),
    };

    sidebar.update(cx, |sidebar, cx| {
        sidebar.finish_root_section_action(
            &request,
            super::actions::RootSectionActionOutcome {
                result: Ok(()),
                audit_error: Some("database is locked".to_string()),
            },
            cx,
        );
    });
    cx.run_until_parked();

    assert_eq!(
        last_toast(cx),
        Some(dbflux_i18n::t!(
            "sidebar.toast.root_section_action_audit_failed",
            action = dbflux_i18n::t!("sidebar.root_section.action.delete_bucket"),
            item = "alpha",
            error = "database is locked"
        ))
    );
    assert_eq!(
        log.lock().expect("source log").listings.len(),
        2,
        "the successful action still lists the section again"
    );
}

/// Two declarations with the same id would share one folder node, so the
/// second never shows; the first is kept and the repeat reported.
#[gpui::test]
async fn a_repeated_section_id_keeps_the_first_and_is_reported(cx: &mut TestAppContext) {
    let log = Arc::default();
    let repeated = RootSectionDef {
        label: dbflux_core::LocalizedText::plain("Repeated"),
        ..buckets_section()
    };
    let connection = SectionConnection::new(&log).declaring(vec![buckets_section(), repeated]);
    let (sidebar, cx) = loaded_sidebar(connection, cx);

    let cached = sidebar.update(cx, |sidebar, _| {
        sidebar.root_section_cache.get(&PROFILE_ID).cloned()
    });
    assert_eq!(
        cached.map(|cached| cached.sections),
        Some(vec![buckets_section()])
    );
    assert_eq!(
        last_toast(cx),
        Some(dbflux_i18n::t!(
            "sidebar.toast.root_section_invalid_id",
            section = "buckets"
        ))
    );
}

/// A connection row activated while the declarations fail still opens its
/// section once a retry loads them.
#[gpui::test]
async fn a_profile_activation_survives_a_failed_declaration_fetch(cx: &mut TestAppContext) {
    let log = Arc::default();
    let connection = SectionConnection::new(&log).failing_first_declarations(1);
    let (sidebar, cx) = connected_sidebar(connection, cx);
    let (events, _subscription) = record_open_events(&sidebar, cx);

    sidebar.update(cx, |sidebar, cx| sidebar.execute_item(&profile_node(), cx));
    cx.run_until_parked();
    assert!(events.lock().expect("event log").is_empty());

    let retry_row = format!("{}{PROFILE_ID}", super::DECLARATIONS_RETRY_PREFIX);
    sidebar.update(cx, |sidebar, cx| sidebar.execute_item(&retry_row, cx));
    cx.run_until_parked();

    assert_eq!(
        events.lock().expect("event log").clone(),
        vec!["section buckets ObjectContainerTable"]
    );
}

/// A single click on a retry row fetches again, like the keyboard does.
#[gpui::test]
async fn a_single_click_on_a_retry_row_fetches_again(cx: &mut TestAppContext) {
    let log: Arc<Mutex<SourceLog>> = Arc::default();
    let connection = SectionConnection::new(&log)
        .failing_first_declarations(1)
        .failing_first_listings(1);
    let (sidebar, cx) = loaded_sidebar(connection, cx);

    let declarations_retry = format!("{}{PROFILE_ID}", super::DECLARATIONS_RETRY_PREFIX);
    sidebar.update(cx, |sidebar, cx| {
        sidebar.handle_item_click(&declarations_retry, 1, false, false, cx)
    });
    cx.run_until_parked();
    assert_eq!(log.lock().expect("source log").declarations, 2);
    assert!(cached_ids(&sidebar, cx).is_empty());

    let listing_retry = format!("{}{PROFILE_ID}|buckets", super::LISTING_RETRY_PREFIX);
    sidebar.update(cx, |sidebar, cx| {
        sidebar.handle_item_click(&listing_retry, 1, false, false, cx)
    });
    cx.run_until_parked();
    assert_eq!(cached_ids(&sidebar, cx), vec!["alpha"]);
}
