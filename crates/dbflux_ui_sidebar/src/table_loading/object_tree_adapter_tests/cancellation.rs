use super::*;
use crate::PendingAction;

fn status_of(
    state: &Entity<AppStateEntity>,
    cx: &mut TestAppContext,
    task_id: dbflux_core::TaskId,
) -> dbflux_core::TaskStatus {
    state.read_with(cx, |state, _| {
        state.tasks().get(task_id).expect("task").status
    })
}

fn running_task_of_kind(
    state: &Entity<AppStateEntity>,
    cx: &mut TestAppContext,
    kind: dbflux_core::TaskKind,
) -> dbflux_core::TaskId {
    state.read_with(cx, |state, _| {
        state
            .tasks()
            .running_tasks()
            .into_iter()
            .find(|task| task.kind == kind)
            .expect("running task")
            .id
    })
}

/// Cancels a task whose blocking call is still running and checks that
/// it reads `Cancelling` until the worker confirms, then `Cancelled`.
fn assert_cancel_settles_honestly(
    state: &Entity<AppStateEntity>,
    cx: &mut TestAppContext,
    task_id: dbflux_core::TaskId,
) {
    assert!(
        state.read_with(cx, |state, _| state
            .tasks()
            .get(task_id)
            .expect("task")
            .is_cancellable),
        "the task offers a cancel"
    );
    state.update(cx, |state, _| assert!(state.cancel_task(task_id)));
    assert_eq!(
        status_of(state, cx, task_id),
        dbflux_core::TaskStatus::Cancelling,
        "the blocking call has not finished"
    );
    cx.run_until_parked();
    assert_eq!(
        status_of(state, cx, task_id),
        dbflux_core::TaskStatus::Cancelled
    );
}

fn lazy_refresh_fixture(
    cx: &mut TestAppContext,
) -> (
    Entity<AppStateEntity>,
    Uuid,
    gpui::WindowHandle<crate::Sidebar>,
    String,
) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = AdapterFakeConnection::lazy();
    fake.set_schema(
        "app",
        fake_db_schema("app", vec![fake_table("public", "fresh")]),
    );
    connect_profile(
        &state,
        cx,
        profile_id,
        fake,
        Some(snapshot_naming(vec![
            dbflux_core::DatabaseInfo {
                name: "other".into(),
                is_current: true,
            },
            dbflux_core::DatabaseInfo {
                name: "app".into(),
                is_current: false,
            },
        ])),
    );
    state.update(cx, |state, _| {
        state.set_active_database(profile_id, Some("other".into()));
        state.set_database_schema(
            profile_id,
            "app".into(),
            fake_db_schema("app", vec![fake_table("public", "held")]),
        );
    });
    let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
    let item_id = SchemaNodeId::Database {
        profile_id,
        name: "app".into(),
    }
    .to_string();
    window
        .update(cx, |sidebar, _, cx| {
            sidebar.refresh_schema_database(&item_id, cx)
        })
        .expect("sidebar alive");
    (state, profile_id, window, item_id)
}

fn app_table_names(
    state: &Entity<AppStateEntity>,
    cx: &mut TestAppContext,
    profile_id: Uuid,
) -> Vec<String> {
    state.read_with(cx, |state, _| {
        state.connections()[&profile_id].database_schemas["app"]
            .tables
            .iter()
            .map(|table| table.name.clone())
            .collect()
    })
}

#[gpui::test]
async fn a_cancelled_database_refresh_reads_cancelling_then_cancelled_and_drops_its_result(
    cx: &mut TestAppContext,
) {
    let (state, profile_id, _window, _item_id) = lazy_refresh_fixture(cx);
    let task_id = running_task_of_kind(&state, cx, dbflux_core::TaskKind::SchemaRefresh);

    assert_cancel_settles_honestly(&state, cx, task_id);

    assert_eq!(app_table_names(&state, cx, profile_id), vec!["held"]);
}

#[gpui::test]
async fn an_uncancelled_database_refresh_keeps_its_result_and_completes(cx: &mut TestAppContext) {
    let (state, profile_id, _window, _item_id) = lazy_refresh_fixture(cx);
    let task_id = running_task_of_kind(&state, cx, dbflux_core::TaskKind::SchemaRefresh);

    cx.run_until_parked();

    assert_eq!(
        status_of(&state, cx, task_id),
        dbflux_core::TaskStatus::Completed
    );
    assert_eq!(app_table_names(&state, cx, profile_id), vec!["fresh"]);
}

async fn view_refresh_fixture(
    cx: &mut TestAppContext,
) -> (Entity<AppStateEntity>, dbflux_core::TaskId) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = AdapterFakeConnection::single_database();
    let mut snapshot = snapshot_naming(vec![dbflux_core::DatabaseInfo {
        name: "app".into(),
        is_current: true,
    }]);
    if let dbflux_core::DataStructure::Relational(relational) = &mut snapshot.structure {
        let mut schema = fake_db_schema("public", Vec::new());
        schema.views.push(dbflux_core::ViewInfo {
            name: "report".into(),
            schema: Some("public".into()),
        });
        relational.schemas.push(schema);
    }
    *fake.primary_snapshot.lock().expect("snapshot") = Some(snapshot.clone());
    connect_profile(&state, cx, profile_id, fake, Some(snapshot));
    let view_id = SchemaNodeId::View {
        profile_id,
        database: Some("app".into()),
        schema: "public".into(),
        name: "report".into(),
    }
    .to_string();
    let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
    window
        .update(cx, |sidebar, _, cx| {
            sidebar.refresh_schema_object(&view_id, cx)
        })
        .expect("sidebar alive");
    let task_id = running_task_of_kind(&state, cx, dbflux_core::TaskKind::SchemaRefresh);
    (state, task_id)
}

#[gpui::test]
async fn a_cancelled_view_refresh_reads_cancelling_then_cancelled(cx: &mut TestAppContext) {
    let (state, task_id) = view_refresh_fixture(cx).await;

    assert_cancel_settles_honestly(&state, cx, task_id);
}

#[gpui::test]
async fn an_uncancelled_view_refresh_completes(cx: &mut TestAppContext) {
    let (state, task_id) = view_refresh_fixture(cx).await;

    cx.run_until_parked();

    assert_eq!(
        status_of(&state, cx, task_id),
        dbflux_core::TaskStatus::Completed
    );
}

#[gpui::test]
async fn a_cancelled_table_refresh_reads_cancelling_then_cancelled(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    let fake = AdapterFakeConnection::single_database();
    connect_profile(
        &state,
        cx,
        profile_id,
        fake,
        Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
            name: "app".into(),
            is_current: true,
        }])),
    );
    state.update(cx, |state, _| {
        state.set_table_details(
            profile_id,
            "app".into(),
            Some("public".into()),
            "users".into(),
            loaded_details("app", Some("public"), "users"),
        );
    });
    let table_id = SchemaNodeId::Table {
        profile_id,
        database: Some("app".into()),
        schema: "public".into(),
        name: "users".into(),
    }
    .to_string();
    let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
    window
        .update(cx, |sidebar, _, cx| {
            sidebar.refresh_schema_object(&table_id, cx);
        })
        .expect("sidebar alive");
    let task_id = running_task_of_kind(&state, cx, dbflux_core::TaskKind::SchemaRefresh);

    assert_cancel_settles_honestly(&state, cx, task_id);

    window
        .update(cx, |sidebar, _, _| {
            assert!(!sidebar.loading_items.contains(&table_id));
        })
        .expect("sidebar alive");
}

#[gpui::test]
async fn a_cancelled_table_refresh_does_not_run_its_queued_pending_action(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    connect_profile(
        &state,
        cx,
        profile_id,
        AdapterFakeConnection::single_database(),
        Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
            name: "app".into(),
            is_current: true,
        }])),
    );
    state.update(cx, |state, _| {
        state.set_table_details(
            profile_id,
            "app".into(),
            Some("public".into()),
            "users".into(),
            loaded_details("app", Some("public"), "users"),
        );
    });
    let table_id = SchemaNodeId::Table {
        profile_id,
        database: Some("app".into()),
        schema: "public".into(),
        name: "users".into(),
    }
    .to_string();
    let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
    window
        .update(cx, |sidebar, _, cx| {
            sidebar.refresh_schema_object(&table_id, cx);
            assert!(matches!(
                sidebar.ensure_table_details(
                    &table_id,
                    PendingAction::ViewSchema {
                        item_id: table_id.clone(),
                    },
                    cx,
                ),
                TableDetailsStatus::Loading
            ));
            assert!(sidebar.pending_actions.contains_key(&table_id));
        })
        .expect("sidebar alive");
    let task_id = running_task_of_kind(&state, cx, dbflux_core::TaskKind::SchemaRefresh);

    assert_cancel_settles_honestly(&state, cx, task_id);

    window
        .update(cx, |sidebar, _, _| {
            assert!(
                !sidebar.pending_actions.contains_key(&table_id),
                "the cancelled consumer must drop its queued action"
            );
            assert_ne!(
                sidebar.expansion_overrides.get(&table_id),
                Some(&true),
                "a cancelled refresh must not run the queued ViewSchema action"
            );
        })
        .expect("sidebar alive");
    let key = ObjectTreeRequestKey::TableDetails {
        profile_id,
        database: "app".into(),
        schema: Some("public".into()),
        table: "users".into(),
    };
    assert_eq!(
        state.read_with(cx, |state, _| state.object_tree_outcome(&key).cloned()),
        Some(ObjectTreeOutcome::Applied),
        "the shared coordinator result stays available to other consumers"
    );
}

#[gpui::test]
async fn an_action_queued_after_a_table_refresh_was_cancelled_still_runs(cx: &mut TestAppContext) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    connect_profile(
        &state,
        cx,
        profile_id,
        AdapterFakeConnection::single_database(),
        Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
            name: "app".into(),
            is_current: true,
        }])),
    );
    state.update(cx, |state, _| {
        state.set_table_details(
            profile_id,
            "app".into(),
            Some("public".into()),
            "users".into(),
            loaded_details("app", Some("public"), "users"),
        );
    });
    let table_id = SchemaNodeId::Table {
        profile_id,
        database: Some("app".into()),
        schema: "public".into(),
        name: "users".into(),
    }
    .to_string();
    let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
    window
        .update(cx, |sidebar, _, cx| {
            sidebar.refresh_schema_object(&table_id, cx);
        })
        .expect("sidebar alive");
    let task_id = running_task_of_kind(&state, cx, dbflux_core::TaskKind::SchemaRefresh);

    state.update(cx, |state, _| assert!(state.cancel_task(task_id)));
    window
        .update(cx, |sidebar, _, cx| {
            assert!(matches!(
                sidebar.ensure_table_details(
                    &table_id,
                    PendingAction::ViewSchema {
                        item_id: table_id.clone(),
                    },
                    cx,
                ),
                TableDetailsStatus::Loading
            ));
        })
        .expect("sidebar alive");
    cx.run_until_parked();

    assert_eq!(
        status_of(&state, cx, task_id),
        dbflux_core::TaskStatus::Cancelled
    );
    window
        .update(cx, |sidebar, _, _| {
            assert!(!sidebar.pending_actions.contains_key(&table_id));
            assert_eq!(
                sidebar.expansion_overrides.get(&table_id),
                Some(&true),
                "an action queued after the cancel runs once the shared details arrive"
            );
        })
        .expect("sidebar alive");
}

fn collection_children_fixture(
    cx: &mut TestAppContext,
) -> (Entity<AppStateEntity>, Uuid, dbflux_core::TaskId) {
    let state = test_app_state(cx);
    let profile_id = Uuid::new_v4();
    connect_profile(
        &state,
        cx,
        profile_id,
        AdapterFakeConnection::single_database(),
        Some(snapshot_naming(vec![dbflux_core::DatabaseInfo {
            name: "app".into(),
            is_current: true,
        }])),
    );
    let collection_id = SchemaNodeId::Collection {
        profile_id,
        database: "app".to_string(),
        name: "events".to_string(),
    }
    .to_string();
    let window = cx.add_window(|window, cx| crate::Sidebar::new(state.clone(), window, cx));
    window
        .update(cx, |sidebar, _, cx| {
            sidebar.spawn_fetch_collection_children(
                profile_id,
                "app",
                "events",
                PendingAction::ExpandCollection {
                    item_id: collection_id,
                },
                cx,
            )
        })
        .expect("sidebar alive");
    let task_id = running_task_of_kind(&state, cx, dbflux_core::TaskKind::LoadSchema);
    (state, profile_id, task_id)
}

fn cached_child_count(
    state: &Entity<AppStateEntity>,
    cx: &mut TestAppContext,
    profile_id: Uuid,
) -> usize {
    state.read_with(cx, |state, _| {
        state.connections()[&profile_id]
            .collection_children
            .get(&("app".to_string(), "events".to_string()))
            .map_or(0, |cache| cache.items.len())
    })
}

#[gpui::test]
async fn a_cancelled_collection_children_load_reads_cancelling_then_cancelled_and_drops_its_page(
    cx: &mut TestAppContext,
) {
    let (state, profile_id, task_id) = collection_children_fixture(cx);

    assert_cancel_settles_honestly(&state, cx, task_id);

    assert_eq!(cached_child_count(&state, cx, profile_id), 0);
}

#[gpui::test]
async fn an_uncancelled_collection_children_load_keeps_its_page_and_completes(
    cx: &mut TestAppContext,
) {
    let (state, profile_id, task_id) = collection_children_fixture(cx);

    cx.run_until_parked();

    assert_eq!(
        status_of(&state, cx, task_id),
        dbflux_core::TaskStatus::Completed
    );
    assert_eq!(cached_child_count(&state, cx, profile_id), 1);
}
