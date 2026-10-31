use super::{
    ProfileRootSections, RootSectionListing, build_root_section_children,
    build_root_section_folder, section_folder_label,
};
use crate::table_loading::object_tree_adapter_tests::test_app_state;
use crate::{ContextMenuAction, Sidebar, SidebarEvent};
use dbflux_core::{
    Connection, DatabaseCategory, DbError, DriverCapabilities, ExecutionClassification,
    LocalizedText, RootItemPresentation, RootSectionAction, RootSectionDef, RootSectionItem,
    RootSectionPage, RootSectionPresentation, RootSectionSource, SchemaNodeId, SchemaNodeKind,
    SchemaSnapshot,
};
use dbflux_ui_base::AppStateEntity;
use gpui::{Entity, TestAppContext, VisualTestContext};
use std::collections::HashMap;
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
    let folder = build_root_section_folder(PROFILE_ID, &buckets_section(), None);

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

// --- connected fixtures ---

/// Every `list_items` and `execute_action` call the fixture received.
#[derive(Default)]
struct SourceLog {
    listings: Vec<(String, Option<String>)>,
    actions: Vec<(String, String, String)>,
}

struct SectionConnection {
    metadata: dbflux_core::DriverMetadata,
    pages: HashMap<Option<String>, RootSectionPage>,
    fail_actions: bool,
    log: Arc<Mutex<SourceLog>>,
}

impl RootSectionSource for SectionConnection {
    fn sections(&self) -> Vec<RootSectionDef> {
        vec![buckets_section()]
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

        self.pages
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
        Ok(())
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

/// A connection whose `buckets` section lists `alpha` then, on a second
/// page, `beta`.
fn section_connection(fail_actions: bool, log: &Arc<Mutex<SourceLog>>) -> SectionConnection {
    let first = RootSectionPage {
        items: items(&["alpha"]),
        next_page_token: Some("2".to_string()),
    };
    let second = RootSectionPage {
        items: items(&["beta"]),
        next_page_token: None,
    };

    SectionConnection {
        metadata: metadata(),
        pages: HashMap::from([(None, first), (Some("2".to_string()), second)]),
        fail_actions,
        log: log.clone(),
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

/// A sidebar over a connected fixture whose declarations and first page are
/// loaded.
fn loaded_sidebar<'a>(
    fail_actions: bool,
    log: &Arc<Mutex<SourceLog>>,
    cx: &'a mut TestAppContext,
) -> (Entity<Sidebar>, &'a mut VisualTestContext) {
    let state = test_app_state(cx);
    connect(&state, section_connection(fail_actions, log), cx);

    let (sidebar, cx) = cx.add_window_view(|window, cx| Sidebar::new(state.clone(), window, cx));
    sidebar.update(cx, |sidebar, cx| {
        sidebar.spawn_fetch_root_sections(PROFILE_ID, cx)
    });
    cx.run_until_parked();

    (sidebar, cx)
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

#[gpui::test]
async fn load_more_appends_the_next_page(cx: &mut TestAppContext) {
    let log = Arc::default();
    let (sidebar, cx) = loaded_sidebar(false, &log, cx);
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
    let (sidebar, cx) = loaded_sidebar(false, &log, cx);

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
/// confirmed; the section is listed again afterwards.
#[gpui::test]
async fn a_destructive_action_runs_only_after_confirmation(cx: &mut TestAppContext) {
    let log: Arc<Mutex<SourceLog>> = Arc::default();
    let (sidebar, cx) = loaded_sidebar(false, &log, cx);

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

/// A `Read` action runs right away, with no confirmation.
#[gpui::test]
async fn a_read_action_runs_without_confirmation(cx: &mut TestAppContext) {
    let log: Arc<Mutex<SourceLog>> = Arc::default();
    let (sidebar, cx) = loaded_sidebar(false, &log, cx);

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
    let (sidebar, cx) = loaded_sidebar(true, &log, cx);

    sidebar.update(cx, |sidebar, cx| {
        sidebar.request_root_section_action(&item_node("alpha"), "inspect", cx)
    });
    cx.run_until_parked();

    let last_toast = cx.update(|_, cx| {
        cx.global::<dbflux_ui_base::toast::ToastGlobal>()
            .host
            .read(cx)
            .last_toast_title()
    });
    assert_eq!(
        last_toast,
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
    let (sidebar, cx) = loaded_sidebar(false, &log, cx);

    let events: Arc<Mutex<Vec<String>>> = Arc::default();
    let _subscription = cx.update(|_, cx| {
        let events = events.clone();
        cx.subscribe(&sidebar, move |_, event: &SidebarEvent, _| {
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

    sidebar.update(cx, |sidebar, cx| {
        sidebar.execute_item(&section_node(), cx);
        sidebar.execute_item(&item_node("alpha"), cx);
        sidebar.execute_item(
            &SchemaNodeId::Profile {
                profile_id: PROFILE_ID,
            }
            .to_string(),
            cx,
        );
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
