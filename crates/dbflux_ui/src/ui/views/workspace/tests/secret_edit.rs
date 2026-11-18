//! The `e` key reaches the secret store through the workspace's own
//! dispatch. The keymap resolving `e` and the document handling
//! `Command::EditValue` are each tested on their own; only the routing arm in
//! `dispatch/documents.rs` joins them, and without it the key does nothing.

use super::new_workspace_with;
use crate::keymap::{Command, CommandDispatcher};
use crate::ui::document::{SecretStoreDocument, Tab};
use crate::ui::views::workspace::FocusTarget;
use dbflux_core::{ConnectedProfile, ConnectionProfile, DbConfig, DbKind, MutationPolicy};
use dbflux_test_support::fake_driver::{FAKE_SECRETS_STORE_ID, FakeDriver};
use dbflux_ui_base::AppStateEntity;
use gpui::{AppContext as _, Entity, TestAppContext};

fn connected_app_state(
    cx: &mut TestAppContext,
    driver: &FakeDriver,
) -> (Entity<AppStateEntity>, uuid::Uuid) {
    let profile_id = uuid::Uuid::new_v4();

    let app_state: Entity<AppStateEntity> = cx.update(|cx| {
        cx.new(|_| {
            let runtime =
                dbflux_storage::bootstrap::StorageRuntime::in_memory().expect("in-memory storage");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        })
    });

    let profile = ConnectionProfile::new(
        "prod-aws",
        DbConfig::SQLite {
            path: std::path::PathBuf::from(":memory:"),
            connection_id: None,
        },
    );
    let connection = driver.connect_arc(&profile).expect("fake connection");

    cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state.connections_mut().insert(
                profile_id,
                ConnectedProfile {
                    profile,
                    connection,
                    schema: None,
                    mutation_policy: MutationPolicy::default(),
                    read_only_reason: None,
                    database_schemas: Default::default(),
                    table_details: Default::default(),
                    collection_children: Default::default(),
                    schema_types: Default::default(),
                    schema_columns: Default::default(),
                    schema_indexes: Default::default(),
                    schema_foreign_keys: Default::default(),
                    schema_routines: Default::default(),
                    dependents_cache: Default::default(),
                    active_database: None,
                    redis_key_cache: Default::default(),
                    database_connections: Default::default(),
                    proxy_tunnel: None,
                },
            );
        });
    });

    (app_state, profile_id)
}

/// With an editable secret selected in the active listing, the workspace's
/// dispatch of `Command::EditValue` reaches the store, which hands the edit to
/// the secret's own tab; the editor is drawn there, not in the side pane.
#[gpui::test]
fn e_opens_the_secret_editor_through_the_workspace_dispatch(cx: &mut TestAppContext) {
    let driver = FakeDriver::new(DbKind::AwsSecrets);
    let (app_state, profile_id) = connected_app_state(cx, &driver);
    let (workspace, app_state, window) = new_workspace_with(cx, app_state);

    let profile_state = app_state.clone();
    let doc = window.update(|window, cx| {
        let doc = cx.new(|cx| {
            SecretStoreDocument::new(
                profile_id,
                FAKE_SECRETS_STORE_ID.to_string(),
                None,
                profile_state,
                window,
                cx,
            )
        });
        let pane = SecretStoreDocument::into_pane(doc.clone(), cx);
        workspace.update(cx, |workspace, cx| {
            workspace.tab_manager.update(cx, |manager, cx| {
                manager.open(Tab::Pane(Box::new(pane)), cx);
            });
            workspace.set_focus(FocusTarget::Document, window, cx);
        });
        doc
    });
    window.run_until_parked();

    // Retrieve each row's value until the editor opens; the fixtures hold
    // exactly the plaintext secret the editor opens on.
    let rows = window.update(|_, cx| doc.read(cx).rows());
    let mut opened_editor = false;
    for row in rows {
        window.update(|window, cx| {
            doc.update(cx, |doc, cx| doc.select_row(row, cx));
            workspace.update(cx, |workspace, cx| {
                workspace.dispatch(Command::ToggleValuePanel, window, cx);
            });
        });
        window.run_until_parked();
        assert!(!editor_is_drawn(window), "retrieving opens no editor");

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.dispatch(Command::EditValue, window, cx);
            });
        });
        window.run_until_parked();

        if editor_is_drawn(window) {
            opened_editor = true;
            break;
        }
    }

    assert!(
        opened_editor,
        "the workspace routes `e` to the active secret store, which opens the editor in a tab"
    );
}

/// Whether the secret editor's Save control is drawn.
fn editor_is_drawn(window: &mut gpui::VisualTestContext) -> bool {
    let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(window);
    let frame = capture.frame(window);

    frame
        .nodes()
        .any(|(_, node)| node.id() == "secret-value-editor-save")
}
