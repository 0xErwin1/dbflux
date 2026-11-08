//! A secret opens in its own tab from the listing's expand button and from
//! the row menu. The listing only raises the request; the workspace drains it
//! on render and opens, or focuses, the deduplicated tab.

use super::new_workspace_with;
use crate::keymap::CommandDispatcher;
use crate::ui::document::{DocumentKey, SecretStoreDocument, Tab};
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

const SECRET: &str = "prod/api/credentials";
const PLAINTEXT: &str = "prod/notify/webhook-token";

struct Opened {
    workspace: Entity<crate::ui::views::workspace::Workspace>,
    listing: Entity<SecretStoreDocument>,
    profile_id: uuid::Uuid,
}

fn open_listing(cx: &mut TestAppContext) -> (Opened, &mut gpui::VisualTestContext) {
    let driver = FakeDriver::new(DbKind::AwsSecrets);
    let (app_state, profile_id) = connected_app_state(cx, &driver);
    let (workspace, app_state, window) = new_workspace_with(cx, app_state);

    let listing = window.update(|window, cx| {
        let listing = cx.new(|cx| {
            SecretStoreDocument::new(
                profile_id,
                FAKE_SECRETS_STORE_ID.to_string(),
                None,
                app_state.clone(),
                window,
                cx,
            )
        });
        let pane = SecretStoreDocument::into_pane(listing.clone(), cx);
        workspace.update(cx, |workspace, cx| {
            workspace.tab_manager.update(cx, |manager, cx| {
                manager.open(Tab::Pane(Box::new(pane)), cx);
            });
            workspace.set_focus(FocusTarget::Document, window, cx);
        });
        listing
    });
    window.simulate_resize(gpui::size(gpui::px(1500.0), gpui::px(1400.0)));
    window.run_until_parked();

    (
        Opened {
            workspace,
            listing,
            profile_id,
        },
        window,
    )
}

fn secret_key(profile_id: uuid::Uuid) -> DocumentKey {
    DocumentKey::Secret {
        profile_id,
        store_id: FAKE_SECRETS_STORE_ID.to_string(),
        name: SECRET.to_string(),
        version: None,
        region: None,
    }
}

fn tab_count(opened: &Opened, window: &mut gpui::VisualTestContext) -> usize {
    window.update(|_, cx| {
        opened
            .workspace
            .read(cx)
            .tab_manager
            .read(cx)
            .documents()
            .len()
    })
}

fn has_secret_tab(opened: &Opened, window: &mut gpui::VisualTestContext) -> bool {
    let key = secret_key(opened.profile_id);

    window.update(|_, cx| {
        opened
            .workspace
            .read(cx)
            .tab_manager
            .read(cx)
            .find_by_key(&key, cx)
            .is_some()
    })
}

fn select_secret(opened: &Opened, window: &mut gpui::VisualTestContext) {
    select(opened, window, SECRET);
}

fn select(opened: &Opened, window: &mut gpui::VisualTestContext, name: &str) {
    let listing = opened.listing.clone();
    let name = name.to_string();
    window.update(|_, cx| {
        listing.update(cx, |listing, cx| {
            listing.select_row(
                crate::ui::document::secret_store::SecretRowKey::Secret(name),
                cx,
            )
        })
    });
    window.run_until_parked();
}

fn is_drawn(window: &mut gpui::VisualTestContext, id: &str) -> bool {
    let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(window);

    capture
        .frame(window)
        .nodes()
        .any(|(_, node)| node.id() == id)
}

fn click(window: &mut gpui::VisualTestContext, id: &str) {
    let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(window);
    let bounds = capture
        .frame(window)
        .nodes()
        .find(|(_, node)| node.id() == id)
        .map(|(_, node)| node.bounds())
        .unwrap_or_else(|| panic!("`{id}` is drawn"));

    window.simulate_click(bounds.center(), gpui::Modifiers::default());
    window.run_until_parked();
}

/// The details pane's expand button opens the secret in its own tab, and a
/// second press focuses that tab instead of opening another.
#[gpui::test]
fn the_expand_button_opens_the_secret_in_its_own_tab(cx: &mut TestAppContext) {
    let (opened, window) = open_listing(cx);
    select_secret(&opened, window);
    assert_eq!(tab_count(&opened, window), 1);
    assert!(!has_secret_tab(&opened, window));

    click(window, "secret-details-expand");

    assert_eq!(tab_count(&opened, window), 2);
    assert!(has_secret_tab(&opened, window));
    assert!(
        window.update(|_, cx| opened
            .workspace
            .read(cx)
            .tab_manager
            .read(cx)
            .active_id()
            .is_some()),
        "the new tab is the active one"
    );

    // Back on the listing, the same press focuses the tab that exists.
    let listing_id = window.update(|_, cx| opened.listing.read(cx).id());
    window.update(|_, cx| {
        opened.workspace.update(cx, |workspace, cx| {
            workspace
                .tab_manager
                .update(cx, |manager, cx| manager.activate(listing_id, cx));
        })
    });
    window.run_until_parked();
    click(window, "secret-details-expand");

    assert_eq!(
        tab_count(&opened, window),
        2,
        "no second tab for one secret"
    );
}

/// The row menu's Open in tab does the same through the same request.
#[gpui::test]
fn the_row_menu_opens_the_secret_in_its_own_tab(cx: &mut TestAppContext) {
    let (opened, window) = open_listing(cx);

    let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(window);
    let row = capture
        .frame(window)
        .nodes()
        .find(|(_, node)| node.id() == format!("secret-row-{SECRET}"))
        .map(|(_, node)| node.bounds())
        .expect("the row is drawn");
    window.simulate_event(gpui::MouseDownEvent {
        position: row.center(),
        modifiers: gpui::Modifiers::default(),
        button: gpui::MouseButton::Right,
        click_count: 1,
        first_mouse: false,
    });
    window.run_until_parked();

    click(window, "secret-menu-open-in-tab-item-0");

    assert_eq!(tab_count(&opened, window), 2);
    assert!(has_secret_tab(&opened, window));
}

/// The pane's Edit value opens, or focuses, the secret's tab with the editor
/// active: the value is retrieved there through the audited path first, the
/// side pane never hosts the editor, and the editor is not drawn until then.
#[gpui::test]
fn edit_value_in_the_pane_opens_the_tab_with_the_editor(cx: &mut TestAppContext) {
    let (opened, window) = open_listing(cx);
    select(&opened, window, PLAINTEXT);
    assert!(!is_drawn(window, "secret-value-editor-save"));

    click(window, "secret-action-edit-value");

    assert_eq!(tab_count(&opened, window), 2);
    assert!(
        is_drawn(window, "secret-value-editor-save"),
        "the new tab shows the editor"
    );

    // The same press again focuses the tab that exists; nothing opens twice.
    let listing_id = window.update(|_, cx| opened.listing.read(cx).id());
    window.update(|_, cx| {
        opened.workspace.update(cx, |workspace, cx| {
            workspace
                .tab_manager
                .update(cx, |manager, cx| manager.activate(listing_id, cx));
        })
    });
    window.run_until_parked();
    click(window, "secret-action-edit-value");

    assert_eq!(tab_count(&opened, window), 2);
}

/// `e` on the listing does the same through the workspace's own dispatch.
#[gpui::test]
fn the_e_key_in_the_listing_opens_the_tab_with_the_editor(cx: &mut TestAppContext) {
    let (opened, window) = open_listing(cx);
    select(&opened, window, PLAINTEXT);

    window.update(|window, cx| {
        opened.workspace.update(cx, |workspace, cx| {
            workspace.dispatch(crate::keymap::Command::EditValue, window, cx);
        });
    });
    window.run_until_parked();

    assert_eq!(tab_count(&opened, window), 2);
    assert!(is_drawn(window, "secret-value-editor-save"));
}
