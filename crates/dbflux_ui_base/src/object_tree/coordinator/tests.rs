//! Behavioral tests for the shared object-tree coordinator and hierarchy
//! projection. All tests run a real `AppStateEntity` over in-memory storage
//! with controllable fake connections, exercising actual coordinator
//! dispatch (no registry-only bypass): driver work happens through the fake
//! connection on the background executor and is applied through the fenced
//! core seams.

use std::sync::{Arc, Mutex};

use dbflux_core::{DatabaseInfo, SchemaLoadingStrategy};
use gpui::{AppContext, Entity, IntoElement, Render, TestAppContext, div};
use uuid::Uuid;

use super::{
    ObjectTreeEvent, ObjectTreeOutcome, ObjectTreeRejection, ObjectTreeRequestKey,
    ObjectTreeRequestStatus,
};
use crate::app_state_entity::AppStateEntity;
use crate::object_tree::test_support::{
    DRIVER_KEY, FakeTreeConnection, InstallTestDriver, connect_profile, db_schema, loaded_details,
    relational_schema, table, test_app_state,
};
use crate::object_tree::{
    NodeContent, ObjectTreeKey, database_display_label, project_database_node, project_object_tree,
};

// Test-local producer with controllable authority; its schema fetches use
// the real shared coordinator/manager path through the inner fake.
struct EnumerationSnapshotConnection {
    inner: Arc<FakeTreeConnection>,
    authority: Mutex<dbflux_core::SchemaSnapshotAuthority>,
}

impl dbflux_core::Connection for EnumerationSnapshotConnection {
    fn schema_snapshot_authority(&self) -> dbflux_core::SchemaSnapshotAuthority {
        *self.authority.lock().expect("authority")
    }
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        &self.inner.metadata
    }
    fn ping(&self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }
    fn close(&mut self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }
    fn execute(
        &self,
        _req: &dbflux_core::QueryRequest,
    ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
        Err(dbflux_core::DbError::NotSupported("test connection".into()))
    }
    fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }
    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }
    fn kind(&self) -> dbflux_core::DbKind {
        self.inner.kind
    }
    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        self.inner.strategy
    }
    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        &dbflux_core::DefaultSqlDialect
    }
    fn schema_for_database(
        &self,
        database: &str,
    ) -> Result<dbflux_core::DbSchemaInfo, dbflux_core::DbError> {
        dbflux_core::Connection::schema_for_database(self.inner.as_ref(), database)
    }
}

#[gpui::test]
fn enumeration_only_primary_uses_applied_lazy_schema_for_current_database(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    let schema = relational_schema(
        vec![DatabaseInfo {
            name: "app".into(),
            is_current: true,
        }],
        Some("app"),
        vec![],
        vec![],
    );
    fake.schemas.lock().expect("schemas").insert(
        "app".into(),
        db_schema("app", vec![table(Some("public"), "users")]),
    );
    connect_profile(
        &state,
        cx,
        profile_id,
        Arc::new(EnumerationSnapshotConnection {
            inner: fake,
            authority: Mutex::new(dbflux_core::SchemaSnapshotAuthority::EnumerationOnly),
        }),
        Some(schema),
    );
    state.update(cx, |state, _| {
        let params = state
            .prepare_fetch_explicit_database_schema(profile_id, "app")
            .expect("prepare");
        let fetched = params.execute().expect("fetch");
        assert_eq!(
            state.apply_fetch_explicit_database_schema(fetched),
            dbflux_core::ApplyFetchOutcome::Applied
        );
    });
    let projected = state.read_with(cx, |state, _| {
        project_database_node(state, profile_id, "app").expect("database")
    });
    assert_eq!(
        projected.state,
        NodeContent::Loaded,
        "applied schema must not be masked by enumeration-only primary"
    );
    assert!(
        projected
            .find(&table_key(profile_id, "app", "users"))
            .is_some()
    );
}

#[gpui::test]
fn primary_authority_and_session_replacement_override_stale_lazy_cache(cx: &mut TestAppContext) {
    use dbflux_core::SchemaSnapshotAuthority as Authority;
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    let primary = relational_schema(
        vec![DatabaseInfo {
            name: "app".into(),
            is_current: true,
        }],
        Some("app"),
        vec![],
        vec![],
    );
    let connection = Arc::new(EnumerationSnapshotConnection {
        inner: fake,
        authority: Mutex::new(Authority::Authoritative),
    });
    connect_profile(
        &state,
        cx,
        profile_id,
        connection.clone(),
        Some(primary.clone()),
    );
    state.update(cx, |state, _| {
        state.set_database_schema(
            profile_id,
            "app".into(),
            db_schema("app", vec![table(Some("public"), "stale")]),
        )
    });
    let projected = state.read_with(cx, |state, _| {
        project_database_node(state, profile_id, "app").expect("database")
    });
    assert_eq!(projected.state, NodeContent::Empty);
    assert!(
        projected
            .find(&table_key(profile_id, "app", "stale"))
            .is_none()
    );
    state.read_with(cx, |state, _| {
        let projected =
            crate::object_tree::project_database_with_metadata(state, profile_id, "app")
                .expect("database");
        assert!(
            projected
                .table(&table_key(profile_id, "app", "stale"))
                .is_none()
        );
    });

    // Same underlying Arc is installed with a different declaration; an old
    // session's authority must not be retained across this generation change.
    *connection.authority.lock().expect("authority") = Authority::EnumerationOnly;
    connect_profile(&state, cx, profile_id, connection.clone(), Some(primary));
    state.update(cx, |state, _| {
        state.set_database_schema(
            profile_id,
            "app".into(),
            db_schema("app", vec![table(Some("public"), "fresh")]),
        )
    });
    let projected = state.read_with(cx, |state, _| {
        project_database_node(state, profile_id, "app").expect("database")
    });
    assert!(
        projected
            .find(&table_key(profile_id, "app", "fresh"))
            .is_some()
    );
    state.read_with(cx, |state, _| {
        let projected =
            crate::object_tree::project_database_with_metadata(state, profile_id, "app")
                .expect("database");
        assert_eq!(
            projected
                .table(&table_key(profile_id, "app", "fresh"))
                .map(|table| table.name.as_str()),
            Some("fresh")
        );
    });
    connect_profile(
        &state,
        cx,
        profile_id,
        connection,
        Some(relational_schema(
            vec![DatabaseInfo {
                name: "app".into(),
                is_current: true,
            }],
            Some("app"),
            vec![],
            vec![],
        )),
    );
    let projected = state.read_with(cx, |state, _| {
        project_database_node(state, profile_id, "app").expect("database")
    });
    assert_eq!(
        projected.state,
        NodeContent::Unloaded,
        "reinstall clears lazy cache"
    );
}

#[gpui::test]
fn populated_primary_and_different_database_do_not_borrow_lazy_content(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    connect_profile(
        &state,
        cx,
        profile_id,
        fake,
        Some(relational_schema(
            vec![
                DatabaseInfo {
                    name: "app".into(),
                    is_current: true,
                },
                DatabaseInfo {
                    name: "other".into(),
                    is_current: false,
                },
            ],
            Some("app"),
            vec![db_schema("public", vec![table(Some("public"), "primary")])],
            vec![],
        )),
    );
    state.update(cx, |state, _| {
        state.set_database_schema(
            profile_id,
            "app".into(),
            db_schema("app", vec![table(Some("public"), "stale")]),
        )
    });
    state.read_with(cx, |state, _| {
        let app = project_database_node(state, profile_id, "app").expect("app");
        assert!(app.find(&table_key(profile_id, "app", "primary")).is_some());
        assert!(app.find(&table_key(profile_id, "app", "stale")).is_none());
        let other = project_database_node(state, profile_id, "other").expect("other");
        assert_eq!(other.state, NodeContent::Unloaded);
    });
}

fn list_key(profile_id: Uuid) -> ObjectTreeRequestKey {
    ObjectTreeRequestKey::DatabaseList { profile_id }
}

fn schema_request_key(profile_id: Uuid, database: &str) -> ObjectTreeRequestKey {
    ObjectTreeRequestKey::DatabaseSchema {
        profile_id,
        database: database.to_string(),
    }
}

fn details_request_key(profile_id: Uuid, database: &str, table_name: &str) -> ObjectTreeRequestKey {
    ObjectTreeRequestKey::TableDetails {
        profile_id,
        database: database.to_string(),
        schema: Some("public".to_string()),
        table: table_name.to_string(),
    }
}

fn profile_key(profile_id: Uuid) -> ObjectTreeKey {
    ObjectTreeKey::Profile { profile_id }
}

fn database_key(profile_id: Uuid, database: &str) -> ObjectTreeKey {
    ObjectTreeKey::Database {
        profile_id,
        database: database.to_string(),
    }
}

fn schema_tree_key(profile_id: Uuid, database: &str, schema: &str) -> ObjectTreeKey {
    ObjectTreeKey::Schema {
        profile_id,
        database: database.to_string(),
        schema: schema.to_string(),
    }
}

fn table_key(profile_id: Uuid, database: &str, table_name: &str) -> ObjectTreeKey {
    ObjectTreeKey::Table {
        profile_id,
        database: database.to_string(),
        schema: Some("public".to_string()),
        table: table_name.to_string(),
    }
}

/// One consumer's event recorder. Each instance subscribes independently, so
/// "both consumers notified" means both recorders observed the same settle.
struct Recorder {
    events: Arc<Mutex<Vec<ObjectTreeEvent>>>,
    _marker: Entity<Marker>,
}

struct Marker {
    _subscription: gpui::Subscription,
}

impl Render for Marker {
    fn render(
        &mut self,
        _window: &mut gpui::Window,
        _cx: &mut gpui::Context<Self>,
    ) -> impl IntoElement {
        div()
    }
}

impl Recorder {
    fn new(state: &Entity<AppStateEntity>, cx: &mut TestAppContext) -> Self {
        let events: Arc<Mutex<Vec<ObjectTreeEvent>>> = Arc::default();
        let marker = cx.update(|cx| {
            let sink = events.clone();
            let observed = state.clone();
            cx.new(|cx| Marker {
                _subscription: cx.subscribe(&observed, move |_, _, event: &ObjectTreeEvent, _| {
                    sink.lock().expect("recorder events").push(event.clone());
                }),
            })
        });
        Self {
            events,
            _marker: marker,
        }
    }

    fn events(&self) -> Vec<ObjectTreeEvent> {
        self.events.lock().expect("recorder events").clone()
    }
}

#[gpui::test]
fn pending_generation_is_bound_to_attempt_not_current_profile(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    connect_profile(&state, cx, profile_id, fake.clone(), None);
    let key = list_key(profile_id);
    let old = state.read_with(cx, |state, _| {
        state
            .profile_session_generation(profile_id)
            .expect("connected")
    });
    assert_eq!(
        state.read_with(cx, |state, _| state
            .object_tree_pending_session_generation(&key)),
        None
    );
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched
        );
        assert_eq!(
            state.object_tree_request(key.clone(), cx),
            ObjectTreeRequestStatus::Pending
        );
        assert_eq!(
            state.object_tree_pending_session_generation(&key),
            Some(Some(old)),
            "same-session consumer shares the original attempt"
        );
    });
    connect_profile(&state, cx, profile_id, fake, None);
    state.read_with(cx, |state, _| {
        assert_ne!(state.profile_session_generation(profile_id), Some(old));
        assert_eq!(
            state.object_tree_pending_session_generation(&key),
            Some(Some(old)),
            "same-Arc reinstall cannot relabel pending work"
        );
    });
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(key.clone(), cx),
            ObjectTreeRequestStatus::Pending
        );
        assert_eq!(
            state.object_tree_pending_session_generation(&key),
            Some(Some(old)),
            "mere inspection/dedup cannot cancel old work"
        );
    });
    cx.run_until_parked();
    assert_eq!(
        state.read_with(cx, |state, _| state
            .object_tree_pending_session_generation(&key)),
        None
    );
}

#[gpui::test]
fn two_consumers_requesting_the_same_database_list_run_the_driver_once_and_both_are_notified(
    cx: &mut TestAppContext,
) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    *fake.databases.lock().expect("databases") = vec![
        DatabaseInfo {
            name: "app".into(),
            is_current: true,
        },
        DatabaseInfo {
            name: "analytics".into(),
            is_current: false,
        },
    ];
    connect_profile(&state, cx, profile_id, fake.clone(), None);

    let recorder_a = Recorder::new(&state, cx);
    let recorder_b = Recorder::new(&state, cx);

    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(list_key(profile_id), cx),
            ObjectTreeRequestStatus::Dispatched,
            "first consumer dispatches the work"
        );
    });
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(list_key(profile_id), cx),
            ObjectTreeRequestStatus::Pending,
            "second consumer joins the same attempt"
        );
    });

    cx.run_until_parked();

    assert_eq!(fake.list_calls(), 1, "driver work must be deduplicated");
    assert!(
        !state.read_with(cx, |state, _| state
            .object_tree_is_pending(&list_key(profile_id))),
        "pending state must be released after completion"
    );
    assert_eq!(
        state.read_with(cx, |state, _| state
            .object_tree_outcome(&list_key(profile_id))
            .cloned()),
        Some(ObjectTreeOutcome::Applied),
    );

    for (name, recorder) in [("a", &recorder_a), ("b", &recorder_b)] {
        assert_eq!(
            recorder.events(),
            vec![ObjectTreeEvent {
                key: list_key(profile_id),
                outcome: ObjectTreeOutcome::Applied,
            }],
            "consumer {name} must be notified"
        );
    }
}

#[gpui::test]
fn cached_database_list_request_returns_cached_without_driver_calls(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    *fake.databases.lock().expect("databases") = vec![DatabaseInfo {
        name: "app".into(),
        is_current: true,
    }];
    connect_profile(&state, cx, profile_id, fake.clone(), None);

    // Populate the list through the core seam, as any prior request would.
    state.update(cx, |state, _| {
        let params = state
            .prepare_fetch_database_list(profile_id)
            .expect("prepare database list");
        let fetched = params.execute().expect("fetch database list");
        assert_eq!(
            state.apply_fetch_database_list(fetched),
            dbflux_core::ApplyFetchOutcome::Applied
        );
    });

    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(list_key(profile_id), cx),
            ObjectTreeRequestStatus::Cached,
        );
    });

    let calls_before_request = fake.list_calls();
    assert_eq!(calls_before_request, 1, "the manual population ran once");
    assert_eq!(
        state.read_with(cx, |state, _| state
            .object_tree_outcome(&list_key(profile_id))
            .cloned()),
        Some(ObjectTreeOutcome::Cached),
    );
}

#[gpui::test]
fn failed_schema_request_releases_pending_then_retry_recovers(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    fake.schemas.lock().expect("schemas").insert(
        "analytics".to_string(),
        db_schema("analytics", vec![table(Some("analytics"), "events")]),
    );
    fake.schema_failures
        .lock()
        .expect("failures")
        .insert("analytics".to_string(), 1);
    connect_profile(&state, cx, profile_id, fake.clone(), None);

    let key = schema_request_key(profile_id, "analytics");
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched,
        );
    });
    cx.run_until_parked();

    let outcome = state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned());
    assert!(
        matches!(&outcome, Some(ObjectTreeOutcome::Failed(message))
            if message.contains("introspection for 'analytics' failed")),
        "the failure must be observable for retry UIs, got {outcome:?}"
    );
    assert!(
        !state.read_with(cx, |state, _| state.object_tree_is_pending(&key)),
        "failed requests must release pending state"
    );
    assert!(
        !state.read_with(cx, |state, _| state
            .connections()
            .get(&profile_id)
            .expect("connected")
            .cache_contains(&dbflux_core::CacheKey::database_schema("analytics"))),
        "the failed fetch must not write the cache"
    );

    // The consumer-visible recovery path: retry the same request.
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_retry(key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched,
        );
    });
    cx.run_until_parked();

    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
        Some(ObjectTreeOutcome::Applied),
    );
    assert_eq!(fake.schema_calls(), vec!["analytics", "analytics"]);
}

#[gpui::test]
fn invalidated_in_flight_schema_request_is_rejected_releases_pending_and_retry_applies(
    cx: &mut TestAppContext,
) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    fake.schemas.lock().expect("schemas").insert(
        "analytics".to_string(),
        db_schema("analytics", vec![table(Some("analytics"), "events")]),
    );
    connect_profile(&state, cx, profile_id, fake.clone(), None);

    let key = schema_request_key(profile_id, "analytics");
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched,
        );
    });

    // Invalidate while the fetch is in flight: the result must never be
    // applied and must not hide the invalidation behind a cached success.
    state.update(cx, |state, _| {
        state.invalidate_database_schema(profile_id, "analytics");
    });

    cx.run_until_parked();

    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
        Some(ObjectTreeOutcome::Rejected(
            ObjectTreeRejection::RequestInvalidated
        )),
    );
    assert!(
        !state.read_with(cx, |state, _| state.object_tree_is_pending(&key)),
        "a rejected result must release pending state instead of sticking in Loading"
    );
    assert!(
        !state.read_with(cx, |state, _| state
            .connections()
            .get(&profile_id)
            .expect("connected")
            .cache_contains(&dbflux_core::CacheKey::database_schema("analytics"))),
        "a rejected result must not be cached"
    );

    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_retry(key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched,
        );
    });
    cx.run_until_parked();

    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
        Some(ObjectTreeOutcome::Applied),
        "a fresh retry after invalidation must apply against the current revision"
    );
}

#[gpui::test]
fn reconnect_during_table_details_fetch_rejects_the_stale_result_and_retry_applies(
    cx: &mut TestAppContext,
) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    fake.details.lock().expect("details").insert(
        (
            "app".to_string(),
            Some("public".to_string()),
            "users".to_string(),
        ),
        loaded_details(Some("public"), "users"),
    );
    connect_profile(
        &state,
        cx,
        profile_id,
        fake.clone(),
        Some(relational_schema(
            Vec::new(),
            Some("app"),
            Vec::new(),
            Vec::new(),
        )),
    );

    let key = details_request_key(profile_id, "app", "users");
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched,
        );
    });

    // Reconnect: the session generation changes, fencing the in-flight fetch.
    let replacement = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    replacement.details.lock().expect("details").insert(
        (
            "app".to_string(),
            Some("public".to_string()),
            "users".to_string(),
        ),
        loaded_details(Some("public"), "users"),
    );
    let mut profile = dbflux_core::ConnectionProfile::new(
        "test-profile",
        dbflux_core::DbConfig::default_postgres(),
    );
    profile.id = profile_id;
    profile.set_driver_id(DRIVER_KEY);
    state.update(cx, |state, _| {
        state.apply_connect_profile(
            profile,
            replacement.clone() as Arc<dyn dbflux_core::Connection>,
            Some(relational_schema(
                Vec::new(),
                Some("app"),
                Vec::new(),
                Vec::new(),
            )),
            None,
            false,
            dbflux_core::WritePrivilege::Unknown,
        );
    });

    cx.run_until_parked();

    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
        Some(ObjectTreeOutcome::Rejected(
            ObjectTreeRejection::ConnectionReplaced
        )),
    );
    assert!(
        !state.read_with(cx, |state, _| state.object_tree_is_pending(&key)),
        "no stale write and no endless Loading after a reconnect"
    );

    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_retry(key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched,
        );
    });
    cx.run_until_parked();

    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
        Some(ObjectTreeOutcome::Applied),
    );
    assert_eq!(
        replacement.details_calls(),
        1,
        "the retry must run against the new session's connection"
    );
}

#[gpui::test]
fn missing_per_database_slot_is_installed_once_for_two_dependent_requests_without_active_context_change(
    cx: &mut TestAppContext,
) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();

    let driver = InstallTestDriver::new();
    state.update(cx, |state, _| {
        state
            .facade
            .connections
            .drivers
            .insert(DRIVER_KEY.to_string(), driver.clone());
    });

    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::ConnectionPerDatabase);
    connect_profile(
        &state,
        cx,
        profile_id,
        fake.clone(),
        Some(relational_schema(
            Vec::new(),
            Some("app"),
            Vec::new(),
            Vec::new(),
        )),
    );

    let primary_connection: Arc<dyn dbflux_core::Connection> = state.read_with(cx, |state, _| {
        state
            .connections()
            .get(&profile_id)
            .expect("connected")
            .connection
            .clone()
    });
    let active_before = state.read_with(cx, |state, _| state.get_active_database(profile_id));

    let schema_request = schema_request_key(profile_id, "analytics");
    let details_request = details_request_key(profile_id, "analytics", "slot_table");
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(schema_request.clone(), cx),
            ObjectTreeRequestStatus::WaitingForSlotInstall,
        );
    });
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(details_request.clone(), cx),
            ObjectTreeRequestStatus::WaitingForSlotInstall,
            "a second dependent request shares the same slot preparation"
        );
    });

    cx.run_until_parked();

    assert_eq!(
        driver.connect_calls(),
        1,
        "two dependent requests must share ONE per-database connection installation"
    );
    for key in [&schema_request, &details_request] {
        assert_eq!(
            state.read_with(cx, |state, _| state.object_tree_outcome(key).cloned()),
            Some(ObjectTreeOutcome::Applied),
            "both waiters must resume and apply after the install"
        );
        assert!(!state.read_with(cx, |state, _| state.object_tree_is_pending(key)));
    }

    state.read_with(cx, |state, _| {
        let connected = state.connections().get(&profile_id).expect("connected");
        assert!(
            connected.database_connection("analytics").is_some(),
            "the slot must be installed"
        );
        assert!(
            connected.database_schemas.contains_key("analytics"),
            "the resumed schema fetch must have applied"
        );
        assert!(
            connected.table_details.contains_key(&(
                "analytics".to_string(),
                Some("public".to_string()),
                "slot_table".to_string()
            )),
            "the resumed details fetch must have applied"
        );
    });
    assert_eq!(
        state.read_with(cx, |state, _| state.get_active_database(profile_id)),
        active_before,
        "the guarded install must not change the active query database"
    );
    let primary_after: Arc<dyn dbflux_core::Connection> = state.read_with(cx, |state, _| {
        state
            .connections()
            .get(&profile_id)
            .expect("connected")
            .connection
            .clone()
    });
    assert!(
        Arc::ptr_eq(&primary_connection, &primary_after),
        "the guarded install must not replace the primary connection"
    );
}

#[gpui::test]
fn cancel_releases_pending_state_and_the_attempt_never_writes(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    fake.schemas.lock().expect("schemas").insert(
        "analytics".to_string(),
        db_schema("analytics", vec![table(Some("analytics"), "events")]),
    );
    connect_profile(&state, cx, profile_id, fake.clone(), None);

    let key = schema_request_key(profile_id, "analytics");
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched,
        );
    });
    state.update(cx, |state, cx| state.object_tree_cancel(&key, cx));

    cx.run_until_parked();

    assert!(
        !state.read_with(cx, |state, _| state.object_tree_is_pending(&key)),
        "cancel must release pending state"
    );
    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
        Some(ObjectTreeOutcome::Cancelled),
    );
    assert_eq!(
        fake.schema_calls(),
        Vec::<String>::new(),
        "a cancelled attempt must not run or write driver work"
    );
    assert!(!state.read_with(cx, |state, _| {
        state
            .connections()
            .get(&profile_id)
            .expect("connected")
            .cache_contains(&dbflux_core::CacheKey::database_schema("analytics"))
    }),);

    // After a cancel the node is retryable.
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_retry(key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched,
        );
    });
    cx.run_until_parked();
    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
        Some(ObjectTreeOutcome::Applied),
    );
}

#[gpui::test]
fn cancel_profile_releases_every_request_of_that_profile(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    *fake.databases.lock().expect("databases") = vec![DatabaseInfo {
        name: "app".into(),
        is_current: true,
    }];
    fake.schemas.lock().expect("schemas").insert(
        "app".to_string(),
        db_schema("app", vec![table(Some("app"), "users")]),
    );
    connect_profile(&state, cx, profile_id, fake.clone(), None);

    let list = list_key(profile_id);
    let schema = schema_request_key(profile_id, "app");
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(list.clone(), cx),
            ObjectTreeRequestStatus::Dispatched
        );
        assert_eq!(
            state.object_tree_request(schema.clone(), cx),
            ObjectTreeRequestStatus::Dispatched
        );
    });

    state.update(cx, |state, cx| {
        state.object_tree_cancel_profile(profile_id, cx)
    });
    cx.run_until_parked();

    for key in [&list, &schema] {
        assert!(
            !state.read_with(cx, |state, _| state.object_tree_is_pending(key)),
            "profile cleanup must release every pending request"
        );
        assert_eq!(
            state.read_with(cx, |state, _| state.object_tree_outcome(key).cloned()),
            Some(ObjectTreeOutcome::Cancelled),
        );
    }
    assert_eq!(
        fake.list_calls(),
        0,
        "released attempts must not run driver work"
    );
    assert!(
        state.read_with(cx, |state, _| state.get_database_list(profile_id).is_none()),
        "cleanup must not write results after release"
    );
}

// --- Shared hierarchy projection ---

#[gpui::test]
fn projection_groups_schemas_schemaless_tables_and_is_key_stable_across_rebuilds(
    cx: &mut TestAppContext,
) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    connect_profile(
        &state,
        cx,
        profile_id,
        fake.clone(),
        Some(relational_schema(
            vec![DatabaseInfo {
                name: "app".into(),
                is_current: true,
            }],
            Some("app"),
            vec![db_schema(
                "public",
                vec![
                    table(Some("public"), "users"),
                    table(Some("public"), "orders"),
                ],
            )],
            Vec::new(),
        )),
    );

    let (first, second) = state.read_with(cx, |state, _| {
        (project_object_tree(state), project_object_tree(state))
    });
    assert_eq!(
        first, second,
        "rebuilds from the same caches must be stable"
    );

    let profile = first.find(&profile_key(profile_id)).expect("profile node");
    let database = first
        .find(&database_key(profile_id, "app"))
        .expect("database node");
    let schema = first
        .find(&schema_tree_key(profile_id, "app", "public"))
        .expect("schema node");
    assert_eq!(schema.children.len(), 2, "schema tables must be grouped");
    assert_eq!(schema.state, NodeContent::Loaded);
    assert_eq!(database.state, NodeContent::Loaded);
    assert_eq!(profile.label, "test-profile");
}

#[gpui::test]
fn identical_object_names_in_different_contexts_get_distinct_keys(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let first_profile = Uuid::new_v4();
    let second_profile = Uuid::new_v4();
    let fake_a = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    let fake_b = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    connect_profile(
        &state,
        cx,
        first_profile,
        fake_a,
        Some(relational_schema(
            Vec::new(),
            Some("app"),
            vec![db_schema("public", vec![table(Some("public"), "users")])],
            Vec::new(),
        )),
    );
    connect_profile(
        &state,
        cx,
        second_profile,
        fake_b,
        Some(relational_schema(
            Vec::new(),
            Some("app"),
            vec![db_schema("public", vec![table(Some("public"), "users")])],
            Vec::new(),
        )),
    );

    let snapshot = state.read_with(cx, |state, _| project_object_tree(state));
    assert_eq!(snapshot.roots.len(), 2);

    let users_a = snapshot
        .find(&table_key(first_profile, "app", "users"))
        .expect("first profile's users");
    let users_b = snapshot
        .find(&table_key(second_profile, "app", "users"))
        .expect("second profile's users");
    assert_ne!(users_a.key, users_b.key);
}

#[gpui::test]
fn type_only_lazy_schemas_project_selected_type_metadata(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    connect_profile(
        &state,
        cx,
        profile_id,
        FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase),
        None,
    );
    state.update(cx, |state, _| {
        let mut info = db_schema("app", Vec::new());
        info.custom_types = Some(vec![
            dbflux_core::CustomTypeInfo {
                name: "status".into(),
                schema: Some("audit".into()),
                kind: dbflux_core::CustomTypeKind::Enum,
                enum_values: Some(vec!["new".into()]),
                base_type: None,
            },
            dbflux_core::CustomTypeInfo {
                name: "state".into(),
                schema: None,
                kind: dbflux_core::CustomTypeKind::Enum,
                enum_values: None,
                base_type: None,
            },
        ]);
        state.set_database_schema(profile_id, "app".into(), info);
    });
    state.read_with(cx, |state, _| {
        let projected =
            crate::object_tree::project_database_with_metadata(state, profile_id, "app")
                .expect("database");
        let audit = projected
            .node
            .find(&schema_tree_key(profile_id, "app", "audit"))
            .expect("type-only audit schema");
        assert_eq!(audit.state, NodeContent::Loaded);
        assert_eq!(audit.label, "audit");
        assert_eq!(
            projected.schema_types("audit").map(|types| types.len()),
            Some(1)
        );
        assert_eq!(
            projected.schema_types("app").map(|types| types.len()),
            Some(1)
        );
    });
}

#[gpui::test]
fn views_project_under_their_schema_and_database_without_becoming_tables(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::SingleDatabase);
    let mut public = db_schema("public", vec![table(Some("public"), "users")]);
    public.views.push(dbflux_core::ViewInfo {
        name: "report".into(),
        schema: Some("public".into()),
    });
    let mut snapshot = relational_schema(Vec::new(), Some("app"), vec![public], Vec::new());
    if let dbflux_core::DataStructure::Relational(ref mut relational) = snapshot.structure {
        relational.views.push(dbflux_core::ViewInfo {
            name: "summary".into(),
            schema: None,
        });
    }
    connect_profile(&state, cx, profile_id, fake, Some(snapshot));

    let database = state.read_with(cx, |state, _| {
        project_database_node(state, profile_id, "app").expect("database")
    });
    let schema = database
        .children
        .iter()
        .find(|child| child.label == "public")
        .expect("public schema");
    assert_eq!(schema.children.len(), 2, "view is a distinct schema child");
    assert_eq!(schema.children[1].label, "report");
    assert_eq!(
        database.children.last().map(|node| node.label.as_str()),
        Some("summary")
    );
    assert_eq!(
        database.children.last().map(|node| node.key.parent()),
        Some(Some(database.key))
    );
}

#[gpui::test]
fn schemaless_tables_render_directly_under_the_implicit_database(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::SingleDatabase);
    connect_profile(
        &state,
        cx,
        profile_id,
        fake,
        Some(relational_schema(
            Vec::new(),
            None,
            Vec::new(),
            vec![table(None, "metrics"), table(None, "events")],
        )),
    );

    let snapshot = state.read_with(cx, |state, _| project_object_tree(state));
    let database = snapshot
        .find(&database_key(profile_id, ""))
        .expect("implicit empty-named database node");
    assert_eq!(
        database.label,
        database_display_label(""),
        "implicit database gets the shared fallback label"
    );
    assert_eq!(
        database.children.len(),
        2,
        "schema-less tables are direct children"
    );
    assert_eq!(database.state, NodeContent::Loaded);
    assert!(
        database
            .children
            .iter()
            .all(|node| matches!(&node.key, ObjectTreeKey::Table { schema: None, .. }))
    );
}

#[gpui::test]
fn empty_database_list_suppresses_stale_current_and_active_database(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    connect_profile(
        &state,
        cx,
        profile_id,
        fake.clone(),
        Some(relational_schema(
            vec![DatabaseInfo {
                name: "stale".into(),
                is_current: true,
            }],
            Some("stale"),
            Vec::new(),
            Vec::new(),
        )),
    );
    state.update(cx, |state, _| {
        state
            .connections_mut()
            .get_mut(&profile_id)
            .expect("connected")
            .active_database = Some("other_stale".into());
        state.set_database_schema(
            profile_id,
            "stale".into(),
            db_schema("stale", vec![table(None, "ghost")]),
        );
    });

    state.update(cx, |state, _| {
        let params = state
            .prepare_fetch_database_list(profile_id)
            .expect("prepare database list");
        let fetched = params.execute().expect("fetch database list");
        assert_eq!(
            state.apply_fetch_database_list(fetched),
            dbflux_core::ApplyFetchOutcome::Applied,
            "an empty list must remain a cached success"
        );
    });

    let snapshot = state.read_with(cx, |state, _| project_object_tree(state));
    let profile = snapshot
        .find(&ObjectTreeKey::Profile { profile_id })
        .expect("profile");
    assert_eq!(profile.state, NodeContent::Empty);
    assert!(
        profile.children.is_empty(),
        "cached empty list must exclude stale current, active, and cached schemas"
    );
    assert!(snapshot.find(&database_key(profile_id, "stale")).is_none());
    assert!(
        snapshot
            .find(&database_key(profile_id, "other_stale"))
            .is_none()
    );
}

#[gpui::test]
fn cached_empty_single_database_projects_authoritative_primary_content(cx: &mut TestAppContext) {
    for current in [Some("main"), None] {
        let state = test_app_state(cx);
        let profile_id = Uuid::new_v4();
        let fake = FakeTreeConnection::new(SchemaLoadingStrategy::SingleDatabase);
        connect_profile(
            &state,
            cx,
            profile_id,
            Arc::new(EnumerationSnapshotConnection {
                inner: fake,
                authority: Mutex::new(dbflux_core::SchemaSnapshotAuthority::Authoritative),
            }),
            Some(relational_schema(
                Vec::new(),
                current,
                vec![db_schema("main", vec![table(Some("public"), "users")])],
                Vec::new(),
            )),
        );
        state.update(cx, |state, _| {
            let fetched = state
                .prepare_fetch_database_list(profile_id)
                .expect("prepare")
                .execute()
                .expect("fetch");
            assert_eq!(
                state.apply_fetch_database_list(fetched),
                dbflux_core::ApplyFetchOutcome::Applied
            );
        });
        let name = current.unwrap_or("");
        let snapshot = state.read_with(cx, |state, _| project_object_tree(state));
        let database = snapshot
            .find(&database_key(profile_id, name))
            .expect("implicit primary database");
        assert_eq!(database.state, NodeContent::Loaded);
        assert!(
            database
                .find(&table_key(profile_id, name, "users"))
                .is_some()
        );
    }
}

#[gpui::test]
fn listed_databases_without_cached_content_are_unloaded(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    // The snapshot lists databases but describes neither one.
    connect_profile(
        &state,
        cx,
        profile_id,
        fake,
        Some(relational_schema(
            vec![
                DatabaseInfo {
                    name: "app".into(),
                    is_current: false,
                },
                DatabaseInfo {
                    name: "analytics".into(),
                    is_current: false,
                },
            ],
            None,
            Vec::new(),
            Vec::new(),
        )),
    );

    let snapshot = state.read_with(cx, |state, _| project_object_tree(state));
    for name in ["app", "analytics"] {
        let database = snapshot
            .find(&database_key(profile_id, name))
            .unwrap_or_else(|| panic!("database node '{name}' must exist"));
        assert_eq!(
            database.state,
            NodeContent::Unloaded,
            "'{name}' has no cached content yet"
        );
    }
}

// --- Correction regressions (verifier defects 1-4 and missing boundaries) ---

/// Wires BOTH production globals to the SAME owner entity, as the real
/// workspace startup does: `report_error` resolves `AppStateGlobal` and
/// updates the owning `AppStateEntity` while deferred from inside its own
/// update.
fn wire_production_globals(state: &Entity<AppStateEntity>, cx: &mut TestAppContext) {
    cx.update(|cx| {
        let host = cx.new(|_| crate::toast::ToastHost::new());
        cx.set_global(crate::toast::ToastGlobal { host });
        cx.set_global(crate::app_state_entity::AppStateGlobal {
            entity: state.clone(),
        });
    });
}

#[gpui::test]
fn centralized_report_runs_after_owner_update_with_production_globals_wired_to_same_owner(
    cx: &mut TestAppContext,
) {
    let state = test_app_state(cx);
    wire_production_globals(&state, cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    fake.schema_failures
        .lock()
        .expect("failures")
        .insert("analytics".to_string(), 1);
    connect_profile(&state, cx, profile_id, fake.clone(), None);

    let key = schema_request_key(profile_id, "analytics");
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched,
        );
    });
    cx.run_until_parked();

    let outcome = state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned());
    assert!(
        matches!(&outcome, Some(ObjectTreeOutcome::Failed(message))
            if message.contains("introspection for 'analytics' failed")),
        "the driver failure must settle Failed, got {outcome:?}"
    );
    assert!(
        !state.read_with(cx, |state, _| state.object_tree_is_pending(&key)),
        "failed settlement must release pending state"
    );
    assert_eq!(
        state.read_with(cx, |state, _| state.unread_error_count),
        1,
        "the centralized seam must report exactly once through the same owner entity"
    );
}

#[gpui::test]
fn stale_schema_error_after_same_arc_reconnect_is_rejected_without_reporting(
    cx: &mut TestAppContext,
) {
    let state = test_app_state(cx);
    wire_production_globals(&state, cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    connect_profile(&state, cx, profile_id, fake.clone(), None);
    let key = schema_request_key(profile_id, "analytics");
    let recorder = Recorder::new(&state, cx);
    let old_attempt = state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched
        );
        state.object_tree.fetch_attempt(&key).expect("old attempt")
    });
    connect_profile(&state, cx, profile_id, fake.clone(), None);
    fake.schemas.lock().expect("schemas").insert(
        "analytics".into(),
        db_schema("analytics", vec![table(Some("public"), "fresh")]),
    );
    state.update(cx, |state, cx| {
        state.object_tree_settle_database_schema(
            key.clone(),
            old_attempt,
            Err(dbflux_core::DbError::NotSupported(
                "old session error".into(),
            )),
            cx,
        );
        assert!(!state.object_tree_is_pending(&key));
        assert_eq!(
            state.object_tree_outcome(&key),
            Some(&ObjectTreeOutcome::Rejected(
                ObjectTreeRejection::ConnectionReplaced
            ))
        );
        assert_eq!(
            state.object_tree_request(key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched
        );
        let new_attempt = state.object_tree.fetch_attempt(&key).expect("new attempt");
        state.object_tree_settle_database_schema(
            key.clone(),
            old_attempt,
            Err(dbflux_core::DbError::NotSupported("late old error".into())),
            cx,
        );
        assert_eq!(state.object_tree.fetch_attempt(&key), Some(new_attempt));
    });
    cx.run_until_parked();
    assert_eq!(state.read_with(cx, |state, _| state.unread_error_count), 0);
    assert!(recorder.events().iter().any(|event| event.key == key
        && event.outcome == ObjectTreeOutcome::Rejected(ObjectTreeRejection::ConnectionReplaced)));
    assert!(matches!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
        Some(ObjectTreeOutcome::Applied)
    ));
}

#[gpui::test]
fn stale_list_and_details_errors_reject_old_session_and_preserve_new_attempt(
    cx: &mut TestAppContext,
) {
    let state = test_app_state(cx);
    wire_production_globals(&state, cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    connect_profile(&state, cx, profile_id, fake.clone(), None);
    let keys = [
        list_key(profile_id),
        details_request_key(profile_id, "app", "users"),
    ];
    let recorder = Recorder::new(&state, cx);
    let generation = state.read_with(cx, |state, _| state.profile_session_generation(profile_id));
    let old: Vec<u64> = state.update(cx, |state, _| {
        keys.iter()
            .map(|key| state.object_tree.begin_fetch(key.clone(), None, generation))
            .collect()
    });
    connect_profile(&state, cx, profile_id, fake, None);
    state.update(cx, |state, cx| {
        state.object_tree_settle_database_list(
            keys[0].clone(),
            old[0],
            Err(dbflux_core::DbError::NotSupported("old list".into())),
            cx,
        );
        state.object_tree_settle_table_details(
            keys[1].clone(),
            old[1],
            Err(dbflux_core::DbError::NotSupported("old details".into())),
            cx,
        );
        for key in &keys {
            assert!(!state.object_tree_is_pending(key));
            assert_eq!(
                state.object_tree_outcome(key),
                Some(&ObjectTreeOutcome::Rejected(
                    ObjectTreeRejection::ConnectionReplaced
                ))
            );
        }
        let current = state.profile_session_generation(profile_id);
        let newest: Vec<_> = keys
            .iter()
            .map(|key| state.object_tree.begin_fetch(key.clone(), None, current))
            .collect();
        state.object_tree_settle_database_list(
            keys[0].clone(),
            old[0],
            Err(dbflux_core::DbError::NotSupported("late list".into())),
            cx,
        );
        state.object_tree_settle_table_details(
            keys[1].clone(),
            old[1],
            Err(dbflux_core::DbError::NotSupported("late details".into())),
            cx,
        );
        for (key, attempt) in keys.iter().zip(newest) {
            assert_eq!(state.object_tree.fetch_attempt(key), Some(attempt));
        }
    });
    cx.run_until_parked();
    assert_eq!(state.read_with(cx, |state, _| state.unread_error_count), 0);
    assert_eq!(
        recorder
            .events()
            .iter()
            .filter(|event| keys.contains(&event.key)
                && event.outcome
                    == ObjectTreeOutcome::Rejected(ObjectTreeRejection::ConnectionReplaced))
            .count(),
        2
    );
    state.update(cx, |state, cx| {
        let newest: Vec<_> = keys
            .iter()
            .map(|key| state.object_tree.fetch_attempt(key).expect("newest"))
            .collect();
        state.object_tree_settle_database_list(
            keys[0].clone(),
            newest[0],
            Err(dbflux_core::DbError::NotSupported("current list".into())),
            cx,
        );
        state.object_tree_settle_table_details(
            keys[1].clone(),
            newest[1],
            Err(dbflux_core::DbError::NotSupported("current details".into())),
            cx,
        );
    });
    cx.run_until_parked();
    assert_eq!(state.read_with(cx, |state, _| state.unread_error_count), 2);
    for key in &keys {
        assert!(matches!(
            state.read_with(cx, |state, _| state.object_tree_outcome(key).cloned()),
            Some(ObjectTreeOutcome::Failed(_))
        ));
        assert!(!state.read_with(cx, |state, _| state.object_tree_is_pending(key)));
    }
}

#[gpui::test]
fn slot_aba_during_install_rejects_waiters_without_extra_install(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let driver = InstallTestDriver::new();
    state.update(cx, |state, _| {
        state
            .facade
            .connections
            .drivers
            .insert(DRIVER_KEY.to_string(), driver.clone());
    });
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::ConnectionPerDatabase);
    connect_profile(
        &state,
        cx,
        profile_id,
        fake.clone(),
        Some(relational_schema(
            Vec::new(),
            Some("app"),
            Vec::new(),
            Vec::new(),
        )),
    );

    let key = schema_request_key(profile_id, "analytics");
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(key.clone(), cx),
            ObjectTreeRequestStatus::WaitingForSlotInstall,
        );
    });

    // External slot churn while the install is in flight: the target is
    // added and removed again (ABA). The old install must NOT resurrect it.
    state.update(cx, |state, _| {
        let churn: Arc<dyn dbflux_core::Connection> =
            FakeTreeConnection::new(SchemaLoadingStrategy::ConnectionPerDatabase);
        state.add_database_connection(profile_id, "analytics".to_string(), churn, None);
        assert!(state.remove_database_connection(profile_id, "analytics"));
    });

    cx.run_until_parked();

    assert_eq!(
        driver.connect_calls(),
        1,
        "an ABA-churned target must not trigger another install"
    );
    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
        Some(ObjectTreeOutcome::Rejected(
            ObjectTreeRejection::TargetSlotReplaced
        )),
        "waiters must settle Rejected and wait for an explicit retry"
    );
    assert!(!state.read_with(cx, |state, _| state.object_tree_is_pending(&key)));
    assert!(
        !state.read_with(cx, |state, _| state
            .connections()
            .get(&profile_id)
            .expect("connected")
            .database_connection("analytics")
            .is_some()),
        "the removed target must stay removed"
    );

    // Explicit retry is the only way forward: a fresh request may install.
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_retry(key.clone(), cx),
            ObjectTreeRequestStatus::WaitingForSlotInstall,
        );
    });
    cx.run_until_parked();
    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
        Some(ObjectTreeOutcome::Applied),
    );
    assert_eq!(
        driver.connect_calls(),
        2,
        "the explicit retry installs once"
    );
}

#[gpui::test]
fn newer_slot_present_during_install_resumes_waiters_correctly(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let driver = InstallTestDriver::new();
    state.update(cx, |state, _| {
        state
            .facade
            .connections
            .drivers
            .insert(DRIVER_KEY.to_string(), driver.clone());
    });
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::ConnectionPerDatabase);
    connect_profile(
        &state,
        cx,
        profile_id,
        fake.clone(),
        Some(relational_schema(
            Vec::new(),
            Some("app"),
            Vec::new(),
            Vec::new(),
        )),
    );

    let key = schema_request_key(profile_id, "analytics");
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(key.clone(), cx),
            ObjectTreeRequestStatus::WaitingForSlotInstall,
        );
    });

    // A genuinely usable newer slot appears while the install is in flight.
    let slot_connection = FakeTreeConnection::new(SchemaLoadingStrategy::ConnectionPerDatabase);
    slot_connection.schemas.lock().expect("schemas").insert(
        "analytics".to_string(),
        db_schema("analytics", vec![table(Some("analytics"), "slot_table")]),
    );
    state.update(cx, |state, _| {
        state.add_database_connection(
            profile_id,
            "analytics".to_string(),
            slot_connection.clone() as Arc<dyn dbflux_core::Connection>,
            None,
        );
    });

    cx.run_until_parked();

    assert_eq!(
        driver.connect_calls(),
        1,
        "the superseded install must not be repeated"
    );
    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
        Some(ObjectTreeOutcome::Applied),
        "waiters resume against the genuinely present newer slot"
    );
    assert!(state.read_with(cx, |state, _| {
        state
            .connections()
            .get(&profile_id)
            .expect("connected")
            .database_schemas
            .contains_key("analytics")
    }));
    assert_eq!(
        slot_connection.schema_calls(),
        vec!["analytics"],
        "the resumed fetch must run against the installed slot's connection"
    );
}

#[gpui::test]
fn populated_target_slot_snapshot_projects_loaded_tables(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    connect_profile(
        &state,
        cx,
        profile_id,
        fake.clone(),
        Some(relational_schema(
            vec![
                DatabaseInfo {
                    name: "app".into(),
                    is_current: true,
                },
                DatabaseInfo {
                    name: "analytics".into(),
                    is_current: false,
                },
            ],
            Some("app"),
            vec![db_schema(
                "public",
                vec![table(Some("public"), "app_table")],
            )],
            Vec::new(),
        )),
    );

    // The target slot carries its own populated snapshot, exactly like the
    // wizard's current per-database source.
    state.update(cx, |state, _| {
        let slot_connection = FakeTreeConnection::new(SchemaLoadingStrategy::ConnectionPerDatabase);
        state.add_database_connection(
            profile_id,
            "analytics".to_string(),
            slot_connection,
            Some(relational_schema(
                Vec::new(),
                Some("analytics"),
                vec![db_schema(
                    "reporting",
                    vec![table(Some("reporting"), "slot_table")],
                )],
                Vec::new(),
            )),
        );
    });

    let snapshot = state.read_with(cx, |state, _| project_object_tree(state));
    let analytics = snapshot
        .find(&database_key(profile_id, "analytics"))
        .expect("analytics node");
    assert_eq!(
        analytics.state,
        NodeContent::Loaded,
        "a populated slot snapshot must project as loaded without a fetch"
    );
    let schema = analytics
        .children
        .iter()
        .find(|node| node.label == "reporting")
        .expect("the slot's own schema");
    assert_eq!(
        schema.children.len(),
        1,
        "the slot's tables must project, not the primary database's"
    );
    assert_eq!(schema.children[0].label, "slot_table");
}

#[gpui::test]
fn empty_target_slot_snapshot_projects_empty_not_unloaded(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    connect_profile(
        &state,
        cx,
        profile_id,
        fake.clone(),
        Some(relational_schema(
            vec![DatabaseInfo {
                name: "analytics".into(),
                is_current: false,
            }],
            Some("app"),
            Vec::new(),
            Vec::new(),
        )),
    );

    // An empty slot snapshot is authoritative: the database genuinely has
    // no relational content. It must not fall back to the primary's tables
    // nor show as Unloaded.
    state.update(cx, |state, _| {
        let slot_connection = FakeTreeConnection::new(SchemaLoadingStrategy::ConnectionPerDatabase);
        state.add_database_connection(
            profile_id,
            "analytics".to_string(),
            slot_connection,
            Some(relational_schema(
                Vec::new(),
                Some("analytics"),
                Vec::new(),
                Vec::new(),
            )),
        );
    });

    let (first, second) = state.read_with(cx, |state, _| {
        (project_object_tree(state), project_object_tree(state))
    });
    assert_eq!(first, second, "projection stays deterministic");
    let analytics = first
        .find(&database_key(profile_id, "analytics"))
        .expect("analytics node");
    assert_eq!(analytics.state, NodeContent::Empty);
    assert!(analytics.children.is_empty());
}

#[gpui::test]
fn object_tree_key_parent_links_tables_to_their_container() {
    let profile_id = Uuid::new_v4();
    let profile = profile_key(profile_id);
    let database = database_key(profile_id, "app");
    let schema = schema_tree_key(profile_id, "app", "public");
    let schemaless_table = ObjectTreeKey::Table {
        profile_id,
        database: "app".to_string(),
        schema: None,
        table: "metrics".to_string(),
    };
    let qualified_table = ObjectTreeKey::Table {
        profile_id,
        database: "app".to_string(),
        schema: Some("public".to_string()),
        table: "users".to_string(),
    };

    assert_eq!(
        schemaless_table.parent(),
        Some(database.clone()),
        "a schema-less table's parent is its database"
    );
    assert_eq!(
        qualified_table.parent(),
        Some(schema.clone()),
        "a schema-qualified table's parent is its schema"
    );
    assert_eq!(schema.parent(), Some(database.clone()));
    assert_eq!(database.parent(), Some(profile.clone()));
    assert_eq!(profile.parent(), None);
    assert_ne!(
        schemaless_table.parent(),
        Some(schema.clone()),
        "the schema-less table must never attach to a same-named schema"
    );
}

#[gpui::test]
fn stale_install_error_rejects_old_waiters_without_poisoning_new_session(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    wire_production_globals(&state, cx);
    let profile_id = Uuid::new_v4();
    let driver = InstallTestDriver::new();
    state.update(cx, |state, _| {
        state
            .facade
            .connections
            .drivers
            .insert(DRIVER_KEY.to_string(), driver);
    });
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::ConnectionPerDatabase);
    connect_profile(
        &state,
        cx,
        profile_id,
        fake.clone(),
        Some(relational_schema(
            Vec::new(),
            Some("app"),
            Vec::new(),
            Vec::new(),
        )),
    );
    let schema = schema_request_key(profile_id, "analytics");
    let details = details_request_key(profile_id, "analytics", "slot_table");
    let install_key = super::ObjectTreeInstallKey {
        profile_id,
        database: "analytics".into(),
    };
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(schema.clone(), cx),
            ObjectTreeRequestStatus::WaitingForSlotInstall
        );
        assert_eq!(
            state.object_tree_request(details.clone(), cx),
            ObjectTreeRequestStatus::WaitingForSlotInstall
        );
    });
    let old_attempt = state.read_with(cx, |state, _| {
        state
            .object_tree
            .install_attempt(&install_key)
            .expect("old install")
    });
    connect_profile(
        &state,
        cx,
        profile_id,
        fake,
        Some(relational_schema(
            Vec::new(),
            Some("app"),
            Vec::new(),
            Vec::new(),
        )),
    );
    state.update(cx, |state, cx| {
        let fresh_key = details_request_key(profile_id, "analytics", "fresh_table");
        assert_eq!(
            state.object_tree_request(fresh_key.clone(), cx),
            ObjectTreeRequestStatus::WaitingForSlotInstall
        );
        state.object_tree_settle_install(
            install_key.clone(),
            old_attempt,
            Err("old install error".into()),
            cx,
        );
        assert_ne!(
            state.object_tree_outcome(&fresh_key),
            Some(&ObjectTreeOutcome::Failed(
                "Failed to open the database: old install error".into()
            ))
        );
        for key in [&schema, &details] {
            assert!(!state.object_tree_is_pending(key));
            assert_eq!(
                state.object_tree_outcome(key),
                Some(&ObjectTreeOutcome::Rejected(
                    ObjectTreeRejection::ConnectionReplaced
                ))
            );
        }
        assert_eq!(
            state.object_tree_request(schema.clone(), cx),
            ObjectTreeRequestStatus::WaitingForSlotInstall
        );
        let new_attempt = state
            .object_tree
            .install_attempt(&install_key)
            .expect("new install");
        state.object_tree_settle_install(
            install_key.clone(),
            old_attempt,
            Err("late old error".into()),
            cx,
        );
        assert_eq!(
            state.object_tree.install_attempt(&install_key),
            Some(new_attempt)
        );
        assert!(state.object_tree_is_pending(&schema));
    });
    cx.run_until_parked();
    assert_eq!(state.read_with(cx, |state, _| state.unread_error_count), 0);
    assert!(matches!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&schema).cloned()),
        Some(ObjectTreeOutcome::Applied | ObjectTreeOutcome::Cached)
    ));
    assert!(!state.read_with(cx, |state, _| state.object_tree_is_pending(&schema)));
}

#[gpui::test]
fn shared_install_failure_fails_all_waiters_reports_once_and_allows_retry(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    wire_production_globals(&state, cx);
    let profile_id = Uuid::new_v4();
    let driver = InstallTestDriver::new();
    driver.set_fail_connect(true);
    state.update(cx, |state, _| {
        state
            .facade
            .connections
            .drivers
            .insert(DRIVER_KEY.to_string(), driver.clone());
    });
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::ConnectionPerDatabase);
    connect_profile(
        &state,
        cx,
        profile_id,
        fake.clone(),
        Some(relational_schema(
            Vec::new(),
            Some("app"),
            Vec::new(),
            Vec::new(),
        )),
    );

    let schema = schema_request_key(profile_id, "analytics");
    let details = details_request_key(profile_id, "analytics", "slot_table");
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(schema.clone(), cx),
            ObjectTreeRequestStatus::WaitingForSlotInstall,
        );
        assert_eq!(
            state.object_tree_request(details.clone(), cx),
            ObjectTreeRequestStatus::WaitingForSlotInstall,
        );
    });
    cx.run_until_parked();

    for key in [&schema, &details] {
        assert!(
            matches!(
                state.read_with(cx, |state, _| state.object_tree_outcome(key).cloned()),
                Some(ObjectTreeOutcome::Failed(_))
            ),
            "both waiters must resolve Failed, got {:?}",
            state.read_with(cx, |state, _| state.object_tree_outcome(key).cloned())
        );
        assert!(!state.read_with(cx, |state, _| state.object_tree_is_pending(key)));
    }
    assert_eq!(
        state.read_with(cx, |state, _| state.unread_error_count),
        1,
        "one shared preparation failing is reported exactly once"
    );

    // Fix the driver and retry explicitly: both waiters recover.
    driver.set_fail_connect(false);
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_retry(schema.clone(), cx),
            ObjectTreeRequestStatus::WaitingForSlotInstall,
        );
    });
    cx.run_until_parked();
    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&schema).cloned()),
        Some(ObjectTreeOutcome::Applied),
    );
}

#[gpui::test]
fn install_disconnect_rejects_waiters_without_error_report(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    wire_production_globals(&state, cx);
    let profile_id = Uuid::new_v4();
    let driver = InstallTestDriver::new();
    state.update(cx, |state, _| {
        state
            .facade
            .connections
            .drivers
            .insert(DRIVER_KEY.to_string(), driver.clone());
    });
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::ConnectionPerDatabase);
    connect_profile(
        &state,
        cx,
        profile_id,
        fake.clone(),
        Some(relational_schema(
            Vec::new(),
            Some("app"),
            Vec::new(),
            Vec::new(),
        )),
    );

    let key = schema_request_key(profile_id, "analytics");
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(key.clone(), cx),
            ObjectTreeRequestStatus::WaitingForSlotInstall,
        );
    });

    // The profile disconnects while the guarded install is in flight.
    state.update(cx, |state, _| {
        state.disconnect(profile_id);
    });

    cx.run_until_parked();

    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
        Some(ObjectTreeOutcome::Rejected(
            ObjectTreeRejection::ProfileDisconnected
        )),
    );
    assert!(!state.read_with(cx, |state, _| state.object_tree_is_pending(&key)));
    assert_eq!(
        state.read_with(cx, |state, _| state.unread_error_count),
        0,
        "a disconnect rejection is not a user-triggered failure to report"
    );
}

#[gpui::test]
fn cancelling_one_install_waiter_lets_the_other_resume(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let driver = InstallTestDriver::new();
    state.update(cx, |state, _| {
        state
            .facade
            .connections
            .drivers
            .insert(DRIVER_KEY.to_string(), driver.clone());
    });
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::ConnectionPerDatabase);
    connect_profile(
        &state,
        cx,
        profile_id,
        fake.clone(),
        Some(relational_schema(
            Vec::new(),
            Some("app"),
            Vec::new(),
            Vec::new(),
        )),
    );

    let cancelled = schema_request_key(profile_id, "analytics");
    let live = details_request_key(profile_id, "analytics", "slot_table");
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(cancelled.clone(), cx),
            ObjectTreeRequestStatus::WaitingForSlotInstall,
        );
        assert_eq!(
            state.object_tree_request(live.clone(), cx),
            ObjectTreeRequestStatus::WaitingForSlotInstall,
        );
    });

    state.update(cx, |state, cx| state.object_tree_cancel(&cancelled, cx));
    cx.run_until_parked();

    assert_eq!(
        state.read_with(cx, |state, _| state
            .object_tree_outcome(&cancelled)
            .cloned()),
        Some(ObjectTreeOutcome::Cancelled),
        "the cancelled waiter stays cancelled after the install settles"
    );
    assert!(!state.read_with(cx, |state, _| state.object_tree_is_pending(&cancelled)));
    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&live).cloned()),
        Some(ObjectTreeOutcome::Applied),
        "the surviving waiter must still resume and apply"
    );
    assert!(!state.read_with(cx, |state, _| state.object_tree_is_pending(&live)));
    assert_eq!(
        driver.connect_calls(),
        1,
        "cancelling one waiter must not restart the shared install"
    );
}

#[gpui::test]
fn same_key_retry_supersedes_the_cancelled_attempt_without_stale_settle(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    fake.schemas.lock().expect("schemas").insert(
        "analytics".to_string(),
        db_schema("analytics", vec![table(Some("analytics"), "events")]),
    );
    connect_profile(&state, cx, profile_id, fake.clone(), None);

    let key = schema_request_key(profile_id, "analytics");
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched,
        );
    });
    // Overlapping retry: attempt 1 is superseded before it ever runs. The
    // dropped task cannot settle, so it cannot clobber attempt 2's state.
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_retry(key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched,
        );
    });
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(key.clone(), cx),
            ObjectTreeRequestStatus::Pending,
            "a second consumer joins the new attempt without a third driver run"
        );
    });

    cx.run_until_parked();

    assert_eq!(
        fake.schema_calls(),
        vec!["analytics"],
        "only the superseding attempt may run"
    );
    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
        Some(ObjectTreeOutcome::Applied),
        "the cancelled attempt must not overwrite the new attempt's outcome"
    );
    assert!(!state.read_with(cx, |state, _| state.object_tree_is_pending(&key)));
}

// ── Explicit per-database projection (narrow wrapper, T3 correction) ──
//
// The root projection deliberately names databases only from the cached
// list, the snapshot, and the implicit fallback. `project_database_node`
// reuses the same database builder for one explicitly known database, so a
// consumer that knows the identity independently of enumeration (the
// wizard's resolved source) still gets the shared hierarchy — without
// global enumeration changing.

#[gpui::test]
fn explicit_database_projection_loads_a_database_the_root_enumeration_cannot_name(
    cx: &mut TestAppContext,
) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    // Neither a snapshot nor a database list names "app"; only the
    // per-database schema cache will hold it after the load.
    fake.schemas.lock().expect("schemas").insert(
        "app".to_string(),
        db_schema("app", vec![table(Some("public"), "users")]),
    );
    connect_profile(&state, cx, profile_id, fake.clone(), None);

    let before = state.read_with(cx, |state, _| {
        project_database_node(state, profile_id, "app")
    });
    assert_eq!(
        before.as_ref().map(|node| node.state),
        Some(NodeContent::Unloaded),
        "before any load the explicit node is honestly unloaded"
    );

    let key = schema_request_key(profile_id, "app");
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched,
        );
    });
    cx.run_until_parked();

    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
        Some(ObjectTreeOutcome::Applied),
    );
    assert!(!state.read_with(cx, |state, _| state.object_tree_is_pending(&key)));

    let node = state
        .read_with(cx, |state, _| {
            project_database_node(state, profile_id, "app")
        })
        .expect("node after load");
    assert_eq!(node.state, NodeContent::Loaded);
    assert_eq!(node.children.len(), 1, "the schema group is projected");
    assert_eq!(node.children[0].children.len(), 1, "the table is projected");
    assert_eq!(node.children[0].children[0].label, "users");
}

#[gpui::test]
fn authoritative_empty_cached_list_keeps_root_enumeration_but_explicit_projection_serves_the_known_database(
    cx: &mut TestAppContext,
) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    // The driver answers the list with an authoritative empty success, while
    // "app" has real per-database schema content.
    fake.schemas.lock().expect("schemas").insert(
        "app".to_string(),
        db_schema("app", vec![table(Some("public"), "users")]),
    );
    connect_profile(&state, cx, profile_id, fake.clone(), None);

    let list_key = list_key(profile_id);
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(list_key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched,
        );
    });
    let schema_key = schema_request_key(profile_id, "app");
    state.update(cx, |state, cx| {
        assert_eq!(
            state.object_tree_request(schema_key.clone(), cx),
            ObjectTreeRequestStatus::Dispatched,
        );
    });
    cx.run_until_parked();

    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&list_key).cloned()),
        Some(ObjectTreeOutcome::Applied),
    );
    assert_eq!(
        state.read_with(cx, |state, _| state
            .object_tree_outcome(&schema_key)
            .cloned()),
        Some(ObjectTreeOutcome::Applied),
    );

    // Global root enumeration still honors the authoritative empty list: no
    // database names were resurrected from the schema cache.
    let snapshot = state.read_with(cx, |state, _| project_object_tree(state));
    let root = snapshot.profile(profile_id).expect("root");
    assert!(
        root.children.is_empty(),
        "root enumeration must not union per-database cache keys"
    );

    // The explicit projection of the known database serves its content.
    let node = state
        .read_with(cx, |state, _| {
            project_database_node(state, profile_id, "app")
        })
        .expect("explicit node");
    assert_eq!(node.state, NodeContent::Loaded);
    assert_eq!(node.children.len(), 1);
}

#[gpui::test]
fn explicit_database_projection_distinguishes_loaded_empty_from_unloaded(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    // The driver answers "blank" with an empty (but successful) schema.
    fake.schemas
        .lock()
        .expect("schemas")
        .insert("blank".to_string(), db_schema("blank", Vec::new()));
    connect_profile(&state, cx, profile_id, fake.clone(), None);

    let before = state.read_with(cx, |state, _| {
        project_database_node(state, profile_id, "blank")
    });
    assert_eq!(
        before.as_ref().map(|node| node.state),
        Some(NodeContent::Unloaded)
    );

    // A genuinely empty database answer is applied and cached (the fake's
    // driver call is synchronous, so prepare/execute/apply run in one
    // foreground update, like the existing coordinator tests do).
    let outcome = state.update(cx, |state, _| {
        let params = state
            .prepare_fetch_explicit_database_schema(profile_id, "blank")
            .expect("prepare schema load");
        let fetched = params.execute().expect("execute schema load");
        state.apply_fetch_explicit_database_schema(fetched)
    });
    assert_eq!(outcome, dbflux_core::ApplyFetchOutcome::Applied,);

    let after = state.read_with(cx, |state, _| {
        project_database_node(state, profile_id, "blank")
    });
    assert_eq!(
        after.as_ref().map(|node| node.state),
        Some(NodeContent::Empty),
        "a cached empty answer is a loaded empty, not an unloaded node"
    );
}

#[gpui::test]
fn explicit_database_projection_yields_nothing_for_a_disconnected_profile(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = FakeTreeConnection::new(SchemaLoadingStrategy::LazyPerDatabase);
    fake.schemas.lock().expect("schemas").insert(
        "app".to_string(),
        db_schema("app", vec![table(Some("public"), "users")]),
    );
    connect_profile(&state, cx, profile_id, fake.clone(), None);

    let before = state.read_with(cx, |state, _| {
        project_database_node(state, profile_id, "app")
    });
    assert!(before.is_some(), "connected profiles project");

    state.update(cx, |state, _| {
        state.disconnect(profile_id);
    });

    let after = state.read_with(cx, |state, _| {
        project_database_node(state, profile_id, "app")
    });
    assert!(
        after.is_none(),
        "a disconnected profile must not leave a stale explicit projection"
    );
}
