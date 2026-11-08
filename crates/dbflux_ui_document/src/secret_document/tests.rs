//! The document of one secret, driven through the rendered UI. The write-safety
//! logic is the shared view's and is tested with the listing; these tests
//! cover what the document adds: its identity, the two states in which it
//! leaves the store alone, the follow of the other views of the same secret,
//! and that the shared view's gates are the ones in force inside a tab.

use super::{SecretDocument, SecretDocumentBlock};
use crate::dedup::DocumentKey;
use crate::keyboard_coverage::{SECRET_DOCUMENT, SECRET_STORE};
use crate::keyboard_test_support::init_keyboard_runtime;
use crate::secret_store::changes::{self, SecretChange, SecretChangeKind};
use crate::secret_store::tests::{
    CURRENT_REGION, KEY_VALUE_SECRET, OTHER_REGION, connected_aws_app_state,
};
use crate::secret_store::{DetailsState, SecretStoreDocument, ValueState};
use dbflux_app::keymap::Command;
use dbflux_core::{ConnectionProfile, DbConfig, DbKind};
use dbflux_test_support::fake_driver::{FAKE_SECRET_PASSWORD, FAKE_SECRETS_STORE_ID, FakeDriver};
use dbflux_ui_base::app_state_entity::AppStateGlobal;
use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};
use dbflux_ui_base::{AppStateChanged, AppStateEntity};
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use uuid::Uuid;

struct Opened<'a> {
    doc: Entity<SecretDocument>,
    window: &'a mut VisualTestContext,
    driver: FakeDriver,
    app_state: Entity<AppStateEntity>,
    profile_id: Uuid,
}

fn open_document<'a>(cx: &'a mut TestAppContext, name: &str, region: Option<&str>) -> Opened<'a> {
    init_keyboard_runtime(cx);

    let driver =
        FakeDriver::new(DbKind::AwsSecrets).with_region_switch(&[CURRENT_REGION, OTHER_REGION]);
    let (app_state, profile_id) = connected_aws_app_state(cx, &driver, "prod-aws");

    cx.update(|cx| {
        cx.set_global(AppStateGlobal {
            entity: app_state.clone(),
        });
    });

    let state = app_state.clone();
    let name = name.to_string();
    let region = region.map(str::to_string);

    let (doc, window) = cx.add_window_view(move |_window, cx| {
        SecretDocument::new(
            profile_id,
            FAKE_SECRETS_STORE_ID.to_string(),
            name,
            None,
            region,
            state,
            cx,
        )
    });
    window.simulate_resize(gpui::size(gpui::px(1500.0), gpui::px(1400.0)));
    window.run_until_parked();

    Opened {
        doc,
        window,
        driver,
        app_state,
        profile_id,
    }
}

impl Opened<'_> {
    fn block(&mut self) -> Option<SecretDocumentBlock> {
        let doc = self.doc.clone();
        self.window.update(|_, cx| doc.read(cx).block(cx))
    }

    fn frame(&mut self) -> gpui::AccessibilityFrame {
        FrameCapture::observe(self.window).frame(self.window)
    }

    fn is_drawn(&mut self, id: &str) -> bool {
        self.frame().nodes().any(|(_, node)| node.id() == id)
    }

    fn rendered_text(&mut self) -> String {
        let frame = self.frame();

        frame
            .nodes()
            .map(|(_, node)| {
                let accessible = frame.accessibility_node(node);
                [
                    accessible.and_then(|node| node.label()).unwrap_or_default(),
                    accessible.and_then(|node| node.value()).unwrap_or_default(),
                    node.content_text(),
                ]
                .join(" | ")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn click(&mut self, id: &str) {
        let bounds = self
            .frame()
            .nodes()
            .find(|(_, node)| node.id() == id)
            .map(|(_, node)| node.bounds())
            .unwrap_or_else(|| panic!("`{id}` is drawn"));

        self.window
            .simulate_click(bounds.center(), gpui::Modifiers::default());
        self.window.run_until_parked();
    }

    fn command(&mut self, command: Command) -> bool {
        let doc = self.doc.clone();
        let handled = self.window.update(|window, cx| {
            doc.update(cx, |doc, cx| doc.dispatch_command(command, window, cx))
        });
        self.window.run_until_parked();
        handled
    }

    fn read_view<R>(&mut self, f: impl FnOnce(&crate::secret_store::SecretView) -> R) -> R {
        let doc = self.doc.clone();
        self.window.update(|_, cx| {
            let view = doc.read(cx).view().clone();
            f(view.read(cx))
        })
    }

    fn unread_errors(&mut self) -> u32 {
        let state = self.app_state.clone();
        self.window
            .update(|_, cx| state.read(cx).unread_error_count)
    }

    fn toasts(&mut self) -> usize {
        self.window.update(|_, cx| {
            cx.global::<dbflux_ui_base::toast::ToastGlobal>()
                .host
                .clone()
                .read(cx)
                .toast_count()
        })
    }

    /// Moves the profile's live connection to `region` and announces it.
    fn move_connection_to(&mut self, region: &str) -> FakeDriver {
        self.swap_connection(region, true)
    }

    fn swap_connection(&mut self, region: &str, announce: bool) -> FakeDriver {
        let driver =
            FakeDriver::new(DbKind::AwsSecrets).with_region_switch(&[CURRENT_REGION, OTHER_REGION]);
        let profile = ConnectionProfile::new(
            "prod-aws",
            DbConfig::AwsSecrets {
                region: region.to_string(),
                profile: None,
                access_key_id: None,
                endpoint: None,
            },
        );
        let connection = driver.connect_arc(&profile).expect("connect");
        let (state, profile_id) = (self.app_state.clone(), self.profile_id);

        self.window.update(|_, cx| {
            state.update(cx, |state, cx| {
                if let Some(connected) = state.connections_mut().get_mut(&profile_id) {
                    connected.connection = connection.clone();
                }
                if announce {
                    cx.emit(AppStateChanged);
                }
            });
        });
        self.window.run_until_parked();

        driver
    }

    fn foreign_origin(&mut self) -> gpui::EntityId {
        self.window.update(|_, cx| cx.new(|_| ()).entity_id())
    }

    fn announce(&mut self, kind: SecretChangeKind, name: &str) {
        let origin = self.foreign_origin();
        let change = SecretChange {
            profile_id: self.profile_id,
            store_id: FAKE_SECRETS_STORE_ID.to_string(),
            region: Some(CURRENT_REGION.to_string()),
            name: name.to_string(),
            kind,
            origin,
        };

        self.window.update(|_, cx| changes::publish(change, cx));
        self.window.run_until_parked();
    }
}

fn secret_key(region: Option<&str>) -> DocumentKey {
    DocumentKey::Secret {
        profile_id: Uuid::nil(),
        store_id: FAKE_SECRETS_STORE_ID.to_string(),
        name: KEY_VALUE_SECRET.to_string(),
        version: None,
        region: region.map(str::to_string),
    }
}

/// The tab is one per (connection, store, name, version, region): the same
/// name in another region is another secret.
#[gpui::test]
fn the_dedup_key_includes_the_region(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    let (doc, profile_id) = (opened.doc.clone(), opened.profile_id);

    let matches = |key: DocumentKey, opened: &mut Opened<'_>| {
        let doc = doc.clone();
        opened.window.update(|_, cx| {
            let pane = SecretDocument::into_pane(doc, cx);
            pane.matches_dedup_key(&key, cx)
        })
    };
    let key = |region: Option<&str>, name: &str, version: Option<&str>| DocumentKey::Secret {
        profile_id,
        store_id: FAKE_SECRETS_STORE_ID.to_string(),
        name: name.to_string(),
        version: version.map(str::to_string),
        region: region.map(str::to_string),
    };

    assert!(matches(
        key(Some(CURRENT_REGION), KEY_VALUE_SECRET, None),
        &mut opened
    ));
    assert!(
        !matches(key(Some(OTHER_REGION), KEY_VALUE_SECRET, None), &mut opened),
        "the same name in another region is another tab"
    );
    assert!(!matches(key(None, KEY_VALUE_SECRET, None), &mut opened));
    assert!(!matches(
        key(Some(CURRENT_REGION), "prod/db/master", None),
        &mut opened
    ));
    assert!(!matches(
        key(Some(CURRENT_REGION), KEY_VALUE_SECRET, Some("v1")),
        &mut opened
    ));
    assert!(
        !matches(secret_key(Some(CURRENT_REGION)), &mut opened),
        "another connection is another tab"
    );
}

/// The tab shows the secret's details and history, and its value stays
/// masked until an audited Retrieve.
#[gpui::test]
fn the_value_stays_masked_until_an_audited_retrieve(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));

    assert_eq!(opened.block(), None);
    assert!(opened.read_view(|view| matches!(view.details(), DetailsState::Loaded(_))));
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::NotLoaded)));
    assert!(opened.driver.secret_store_calls().value_requests.is_empty());

    let text = opened.rendered_text();
    assert!(text.contains(KEY_VALUE_SECRET), "{text}");
    assert!(!text.contains(FAKE_SECRET_PASSWORD), "{text}");
    for id in [
        "secret-document-header",
        "secret-document-name",
        "secret-document-subtitle",
        "secret-document-copy-arn",
        "secret-document-refresh",
        "secret-document-delete",
        "secret-retrieve-value",
    ] {
        assert!(opened.is_drawn(id), "`{id}` is drawn");
    }

    opened.click("secret-retrieve-value");

    assert!(opened.read_view(|view| matches!(view.value(), ValueState::Loaded(_))));
    assert_eq!(opened.driver.secret_store_calls().value_requests.len(), 1);

    let state = opened.app_state.clone();
    let actions: Vec<String> = opened.window.update(|_, cx| {
        let filter = dbflux_audit::query::AuditQueryFilter {
            category: Some("secret_store".to_string()),
            ..Default::default()
        };
        state
            .read(cx)
            .audit_service()
            .query_extended(&filter)
            .expect("audit query")
            .into_iter()
            .filter_map(|event| event.action)
            .collect()
    });
    assert_eq!(actions, vec!["secret_value_retrieved"]);
}

/// A connection on another region than the key's: the tab reads nothing and
/// writes nothing, says why inline and raises no toast. The reads resume
/// once the region matches.
#[gpui::test]
fn a_region_mismatch_blocks_reads_and_writes_until_the_region_matches(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(OTHER_REGION));

    assert_eq!(
        opened.block(),
        Some(SecretDocumentBlock::RegionMismatch {
            live: Some(CURRENT_REGION.to_string())
        })
    );
    assert!(opened.is_drawn("secret-document-region-mismatch"));
    for id in ["secret-retrieve-value", "secret-document-delete"] {
        assert!(!opened.is_drawn(id), "`{id}` is not offered");
    }
    let text = opened.rendered_text();
    assert!(
        text.contains(OTHER_REGION) && text.contains(CURRENT_REGION),
        "{text}"
    );
    assert!(opened.read_view(|view| matches!(view.details(), DetailsState::Idle)));

    for command in [
        Command::ToggleValuePanel,
        Command::EditValue,
        Command::Delete,
        Command::RefreshSchema,
    ] {
        opened.command(command);
    }

    let calls = opened.driver.secret_store_calls();
    assert!(calls.described.is_empty(), "{:?}", calls.described);
    assert!(calls.value_requests.is_empty());
    assert!(calls.put_attempts.is_empty());
    assert!(calls.mutation_attempts.is_empty());
    assert_eq!(opened.unread_errors(), 0, "the explanation is inline");
    assert_eq!(opened.toasts(), 0);

    let replacement = opened.move_connection_to(OTHER_REGION);

    assert_eq!(opened.block(), None);
    assert!(opened.read_view(|view| matches!(view.details(), DetailsState::Loaded(_))));
    assert!(!replacement.secret_store_calls().described.is_empty());
    assert!(opened.is_drawn("secret-retrieve-value"));
    assert!(!opened.is_drawn("secret-document-region-mismatch"));

    // And back: the tab goes quiet again.
    opened.move_connection_to(CURRENT_REGION);
    assert!(opened.block().is_some());
    assert!(!opened.is_drawn("secret-retrieve-value"));
}

/// A secret deleted from the listing is reflected in its open document: the
/// tab says so, offers to close, and writes nothing.
#[gpui::test]
fn a_removed_secret_says_so_offers_close_and_writes_nothing(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    assert_eq!(opened.block(), None);

    opened.announce(SecretChangeKind::Removed, KEY_VALUE_SECRET);

    assert_eq!(opened.block(), Some(SecretDocumentBlock::Removed));
    assert!(opened.is_drawn("secret-document-removed"));
    assert!(opened.is_drawn("secret-document-close"));
    assert!(!opened.is_drawn("secret-retrieve-value"));
    assert!(!opened.is_drawn("secret-document-delete"));

    for command in [
        Command::ToggleValuePanel,
        Command::EditValue,
        Command::Delete,
    ] {
        opened.command(command);
    }
    assert!(opened.driver.secret_store_calls().put_attempts.is_empty());
    assert!(
        opened
            .driver
            .secret_store_calls()
            .mutation_attempts
            .is_empty()
    );
    assert_eq!(opened.unread_errors(), 0);

    let doc = opened.doc.clone();
    let actions = opened
        .window
        .update(|_, cx| doc.read(cx).pane_actions(&doc, cx));
    assert_eq!(
        actions.iter().map(|a| a.id.to_string()).collect::<Vec<_>>(),
        vec!["secret-document-close"],
        "the menu offers close alone"
    );

    // An announcement about another secret leaves the tab alone.
    let mut other = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    other.announce(SecretChangeKind::Removed, "prod/db/master");
    assert_eq!(other.block(), None);
}

/// A write from the document pins its session like the details pane does: a
/// connection replaced under an open dialog writes nothing to either session
/// and reports the refusal once.
#[gpui::test]
fn a_write_from_the_document_is_pinned_to_its_session(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));

    opened.click("secret-document-delete");
    opened.click("secret-delete-window-14");
    opened.window.simulate_input(KEY_VALUE_SECRET);
    opened.window.run_until_parked();
    assert!(opened.read_view(|view| view.dialog_is_open()));

    // The replacement is not announced, so the dialog is still open when the
    // user confirms: the pin, not the close, is what refuses.
    let replacement = opened.swap_connection(CURRENT_REGION, false);
    opened.click("secret-delete-confirm");

    assert!(
        opened
            .driver
            .secret_store_calls()
            .mutation_attempts
            .is_empty()
    );
    assert!(
        replacement
            .secret_store_calls()
            .mutation_attempts
            .is_empty()
    );
    assert_eq!(opened.unread_errors(), 1, "the refusal is reported once");
    assert!(!opened.read_view(|view| view.dialog_is_open()));
}

/// A delete confirmed in the document is reflected in the listing of the
/// same store, and the document shows the state the listing shows.
#[gpui::test]
fn a_delete_from_the_document_reflects_in_the_listing(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    let (state, profile_id) = (opened.app_state.clone(), opened.profile_id);

    let listing = opened.window.update(|window, cx| {
        cx.new(|cx| {
            SecretStoreDocument::new(
                profile_id,
                FAKE_SECRETS_STORE_ID.to_string(),
                None,
                state,
                window,
                cx,
            )
        })
    });
    opened.window.run_until_parked();

    let pending = |opened: &mut Opened<'_>, listing: &Entity<SecretStoreDocument>| {
        let listing = listing.clone();
        opened.window.update(|_, cx| {
            listing
                .read(cx)
                .listing()
                .find(KEY_VALUE_SECRET)
                .is_some_and(|summary| summary.is_pending_deletion())
        })
    };
    assert!(!pending(&mut opened, &listing));

    opened.click("secret-document-delete");
    opened.click("secret-delete-window-14");
    opened.window.simulate_input(KEY_VALUE_SECRET);
    opened.window.run_until_parked();
    opened.click("secret-delete-confirm");

    assert_eq!(
        opened.driver.secret_store_calls().mutation_attempts.len(),
        1
    );
    assert!(opened.read_view(|view| {
        view.selected_summary()
            .is_some_and(|summary| summary.is_pending_deletion())
    }));
    assert!(
        pending(&mut opened, &listing),
        "the listing shows the state"
    );
}

/// A write the document announces makes the listing read the store again.
#[gpui::test]
fn a_change_announced_by_the_document_reloads_the_listing(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    let (state, profile_id) = (opened.app_state.clone(), opened.profile_id);

    let listing = opened.window.update(|window, cx| {
        cx.new(|cx| {
            SecretStoreDocument::new(
                profile_id,
                FAKE_SECRETS_STORE_ID.to_string(),
                None,
                state,
                window,
                cx,
            )
        })
    });
    opened.window.run_until_parked();

    let lists = |opened: &mut Opened<'_>| opened.driver.secret_store_calls().list_requests.len();
    let before = lists(&mut opened);

    let doc = opened.doc.clone();
    opened.window.update(|_, cx| {
        doc.update(cx, |doc, cx| {
            doc.announce(KEY_VALUE_SECRET.to_string(), SecretChangeKind::Changed, cx)
        })
    });
    opened.window.run_until_parked();

    assert!(
        lists(&mut opened) > before,
        "the listing read the store again"
    );
    drop(listing);
}

/// Every clickable element of the tab has a keyboard path: the tab's own
/// registry composed with the sections it shares with the listing's pane.
#[gpui::test]
fn the_document_controls_are_covered(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    let doc = opened.doc.clone();
    let entries: Vec<String> = opened.window.update(|_, cx| {
        doc.read(cx)
            .pane_actions(&doc, cx)
            .into_iter()
            .map(|action| action.id.to_string())
            .collect()
    });

    let checked = Coverage::new(SECRET_DOCUMENT)
        .with_surface(SECRET_STORE)
        .with_menu_entries(entries.clone())
        .assert_covered(&opened.frame());
    assert!(
        checked.iter().any(|id| id == "secret-document-delete"),
        "{checked:?}"
    );

    // The blocked states draw their own controls.
    opened.announce(SecretChangeKind::Removed, KEY_VALUE_SECRET);
    let entries: Vec<String> = opened.window.update(|_, cx| {
        doc.read(cx)
            .pane_actions(&doc, cx)
            .into_iter()
            .map(|action| action.id.to_string())
            .collect()
    });
    let checked = Coverage::new(SECRET_DOCUMENT)
        .with_surface(SECRET_STORE)
        .with_menu_entries(entries)
        .assert_covered(&opened.frame());
    assert!(
        checked.iter().any(|id| id == "secret-document-close"),
        "{checked:?}"
    );
}
