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
    CURRENT_REGION, KEY_VALUE_SECRET, OTHER_REGION, PLAINTEXT_SECRET, connected_aws_app_state,
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
    open_document_with(cx, name, region, false)
}

/// Opens the tab the way the workspace does for an Edit value handed over by
/// the pane: the editor is requested before the details have landed.
fn open_document_with<'a>(
    cx: &'a mut TestAppContext,
    name: &str,
    region: Option<&str>,
    edit: bool,
) -> Opened<'a> {
    open_document_in(cx, FAKE_SECRETS_STORE_ID, name, region, edit, true)
}

/// The general opener: any store, and `settle` false leaves the tab as it is
/// on its first frame, before the describe landed.
fn open_document_in<'a>(
    cx: &'a mut TestAppContext,
    store_id: &str,
    name: &str,
    region: Option<&str>,
    edit: bool,
    settle: bool,
) -> Opened<'a> {
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
    let store_id = store_id.to_string();
    let region = region.map(str::to_string);

    let (doc, window) = cx.add_window_view(move |_window, cx| {
        let mut document = SecretDocument::new(profile_id, store_id, name, None, region, state, cx);

        if edit {
            document.begin_edit(cx);
        }

        document
    });
    window.simulate_resize(gpui::size(gpui::px(1500.0), gpui::px(1400.0)));

    if settle {
        window.run_until_parked();
    }

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

    fn error_summaries(&mut self) -> Vec<String> {
        use dbflux_ui_base::notifications::{NotificationKey, NotificationSources};

        let state = self.app_state.clone();
        self.window.update(|_, cx| {
            let state = state.read(cx);
            state
                .notifications
                .snapshot(&NotificationSources::default())
                .entries
                .iter()
                .filter_map(|entry| match &entry.key {
                    NotificationKey::Error(id) => state
                        .notifications
                        .error(*id)
                        .map(|error| error.summary.clone()),
                    _ => None,
                })
                .collect()
        })
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

    fn audit_actions(&mut self) -> Vec<String> {
        let state = self.app_state.clone();
        self.window.update(|_, cx| {
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
        })
    }

    fn document_state(&mut self) -> crate::types::DocumentState {
        let doc = self.doc.clone();
        self.window.update(|_, cx| doc.read(cx).state(cx))
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

/// Every clickable element of the tab has a keyboard path: the tab's own
/// registry composed with the sections it shares with the listing's pane.
#[gpui::test]
fn the_document_controls_are_covered(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    opened.click("secret-retrieve-value");
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
    for id in [
        "secret-document-delete",
        "secret-value-edit",
        "secret-rotation-rotate-now",
    ] {
        assert!(
            checked.iter().any(|checked| checked == id),
            "{id}: {checked:?}"
        );
    }

    // The editor, which the tab alone hosts.
    opened.command(Command::EditValue);
    let editing_entries: Vec<String> = opened.window.update(|_, cx| {
        doc.read(cx)
            .pane_actions(&doc, cx)
            .into_iter()
            .map(|action| action.id.to_string())
            .collect()
    });
    let checked = Coverage::new(SECRET_DOCUMENT)
        .with_surface(SECRET_STORE)
        .with_menu_entries(editing_entries)
        .assert_covered(&opened.frame());
    assert!(
        checked.iter().any(|id| id == "secret-value-editor-discard"),
        "{checked:?}"
    );
    opened.click("secret-value-editor-discard");

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

/// The tab owns no bottom bar: each write is drawn once, where the artboard
/// puts it (the header's Delete, the value section's Edit value, the
/// Rotation section's Rotate immediately).
#[gpui::test]
fn the_tab_draws_each_action_once_without_a_bottom_bar(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, PLAINTEXT_SECRET, Some(CURRENT_REGION));
    opened.click("secret-retrieve-value");

    for id in [
        "secret-document-delete",
        "secret-document-refresh",
        "secret-value-edit",
        "secret-rotation-rotate-now",
        "secret-rotation-edit",
        "secret-tags-edit",
    ] {
        assert!(opened.is_drawn(id), "`{id}` is drawn");
    }
    for id in [
        "secret-action-bar",
        "secret-action-edit-value",
        "secret-action-rotate-now",
        "secret-action-delete",
        "secret-details-delete",
    ] {
        assert!(!opened.is_drawn(id), "`{id}` would draw an action twice");
    }
}

/// Edit value in the tab, whether it was pressed there or handed over by the
/// pane before the details landed, retrieves the value first (audited, once)
/// and opens the editor. It never only reveals.
#[gpui::test]
fn edit_value_in_the_tab_retrieves_first_and_opens_the_editor(cx: &mut TestAppContext) {
    for handed_over in [true, false] {
        let mut opened =
            open_document_with(cx, PLAINTEXT_SECRET, Some(CURRENT_REGION), handed_over);

        if !handed_over {
            assert!(!opened.is_drawn("secret-value-editor-save"));
            assert!(opened.driver.secret_store_calls().value_requests.is_empty());
            assert!(opened.command(Command::EditValue));
        }

        assert!(
            opened.is_drawn("secret-value-editor-save"),
            "handed over: {handed_over}"
        );
        assert!(opened.read_view(|view| view.is_editing_value()));
        assert_eq!(opened.driver.secret_store_calls().value_requests.len(), 1);
        assert_eq!(opened.audit_actions(), vec!["secret_value_retrieved"]);
    }
}

/// The editor in the tab saves as a new version through the governed seam,
/// shows the unsaved text on the tab, and lets go of it once saved.
#[gpui::test]
fn a_save_from_the_tab_writes_through_the_seam_and_clears_the_unsaved_state(
    cx: &mut TestAppContext,
) {
    let mut opened = open_document_with(cx, PLAINTEXT_SECRET, Some(CURRENT_REGION), true);
    assert_eq!(opened.document_state(), crate::types::DocumentState::Clean);

    opened.window.simulate_input("!");
    opened.window.run_until_parked();

    assert_eq!(
        opened.document_state(),
        crate::types::DocumentState::Modified
    );
    let doc = opened.doc.clone();
    assert!(opened.window.update(|_, cx| {
        let view = doc.read(cx).view().clone();
        view.read(cx).has_unsaved_value_edits(cx)
    }));

    opened.click("secret-value-editor-save");

    assert_eq!(opened.driver.secret_store_calls().put_requests.len(), 1);
    assert!(
        opened
            .audit_actions()
            .contains(&"secret_value_written".to_string())
    );
    assert_ne!(
        opened.document_state(),
        crate::types::DocumentState::Modified
    );
}

/// A save is pinned to the session the value came from: a connection
/// replaced under the open editor refuses it, writes to neither session and
/// reports the refusal once.
#[gpui::test]
fn a_save_from_the_tab_is_pinned_to_its_session(cx: &mut TestAppContext) {
    let mut opened = open_document_with(cx, PLAINTEXT_SECRET, Some(CURRENT_REGION), true);
    opened.window.simulate_input("!");
    opened.window.run_until_parked();

    let replacement = opened.swap_connection(CURRENT_REGION, false);
    opened.click("secret-value-editor-save");

    assert!(opened.driver.secret_store_calls().put_attempts.is_empty());
    assert!(replacement.secret_store_calls().put_attempts.is_empty());
    assert_eq!(opened.unread_errors(), 1, "the refusal is reported once");
}

// -- Layout, as artboards 02 and 06 draw it ----------------------------------

impl Opened<'_> {
    fn bounds(&mut self, id: &str) -> gpui::Bounds<gpui::Pixels> {
        self.frame()
            .nodes()
            .find(|(_, node)| node.id() == id)
            .map(|(_, node)| node.bounds())
            .unwrap_or_else(|| panic!("`{id}` is drawn"))
    }

    fn text_of(&mut self, id: &str) -> String {
        self.frame()
            .nodes()
            .find(|(_, node)| node.id() == id)
            .map(|(_, node)| node.content_text().to_string())
            .unwrap_or_else(|| panic!("`{id}` is drawn"))
    }

    fn inside(&mut self, id: &str, card: &str) -> bool {
        let (inner, outer) = (self.bounds(id), self.bounds(card));

        outer.contains(&inner.center())
    }
}

/// Two columns of bordered cards: the value and its versions on the left,
/// the rotation, details and tags in a fixed column on the right.
#[gpui::test]
fn the_tab_lays_out_two_columns_of_cards(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    opened.click("secret-retrieve-value");

    for id in [
        "secret-document-value-card",
        "secret-document-versions-card",
        "secret-document-rotation-card",
        "secret-document-details-card",
        "secret-document-tags-card",
    ] {
        assert!(opened.is_drawn(id), "`{id}` is drawn");
    }

    let value = opened.bounds("secret-document-value-card");
    let versions = opened.bounds("secret-document-versions-card");
    let rotation = opened.bounds("secret-document-rotation-card");
    let details = opened.bounds("secret-document-details-card");
    let tags = opened.bounds("secret-document-tags-card");

    assert_eq!(value.left(), versions.left(), "one left column");
    assert!(value.bottom() <= versions.top());
    assert_eq!(rotation.left(), details.left(), "one right column");
    assert_eq!(details.left(), tags.left());
    assert!(rotation.bottom() <= details.top() && details.bottom() <= tags.top());
    assert!(
        rotation.left() >= value.right(),
        "the right column sits beside"
    );
    assert_eq!(rotation.size.width, gpui::px(470.0)); // guardrail-allow: test geometry, not spacing
}

/// The value card's title row holds Reveal all, Copy JSON and Edit value; the
/// switch and the retrieval time sit below; nothing is drawn under the keys
/// and the tab-only line is the footer's alone.
#[gpui::test]
fn the_value_card_keeps_its_actions_in_the_title_row(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    opened.click("secret-retrieve-value");

    let switch = opened.bounds("segmented-secret-value-view-plaintext");
    for id in [
        "secret-value-hide-all",
        "secret-value-copy-all",
        "secret-value-edit",
    ] {
        assert!(opened.inside(id, "secret-document-value-card"), "{id}");
        assert!(
            opened.bounds(id).bottom() <= switch.top(),
            "`{id}` is above the switch row"
        );
    }
    let last_field = opened.bounds("secret-value-copy-0");
    assert!(opened.bounds("secret-value-hide-all").bottom() < last_field.top());

    let text = opened.rendered_text();
    assert!(text.contains("Retrieved"), "{text}");
    assert_eq!(
        text.matches("Value visible in this tab only").count(),
        1,
        "only the footer says it: {text}"
    );
}

/// Roll back is a labelled button in the tab's versions table.
#[gpui::test]
fn roll_back_is_a_labelled_button(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));

    assert_eq!(opened.text_of("secret-version-rollback-1"), "Roll back");
}

/// The rotation card lists what is known and ends with its two actions; the
/// details card has the ARN with a copy button and "Edit description" in its
/// title row; the tags are chips.
#[gpui::test]
fn the_side_cards_hold_what_the_artboard_lists(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));

    for id in ["secret-rotation-edit", "secret-rotation-rotate-now"] {
        assert!(opened.inside(id, "secret-document-rotation-card"), "{id}");
    }
    for id in ["secret-details-edit", "secret-document-details-copy-arn"] {
        assert!(opened.inside(id, "secret-document-details-card"), "{id}");
    }
    assert!(opened.inside("secret-tags-edit", "secret-document-tags-card"));

    let edit_row = opened.bounds("secret-details-edit");
    let card = opened.bounds("secret-document-details-card");
    assert!(
        edit_row.top() < card.top() + gpui::px(48.0), // guardrail-allow: test geometry, not spacing
        "in the title row"
    );
    assert_eq!(opened.text_of("secret-details-edit"), "Edit description");

    let text = opened.rendered_text();
    for label in [
        "Status",
        "Schedule",
        "Last rotated",
        "ARN",
        "Encryption key",
    ] {
        assert!(text.contains(label), "{label}: {text}");
    }
}

/// The header badge says whether rotation is on, in words.
#[gpui::test]
fn the_header_badge_says_rotation_on_or_off(cx: &mut TestAppContext) {
    let mut on = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    assert_eq!(on.text_of("secret-document-rotation"), "Rotation on");
    drop(on);

    let mut off = open_document(cx, PLAINTEXT_SECRET, Some(CURRENT_REGION));
    assert_eq!(off.text_of("secret-document-rotation"), "Rotation off");
}

/// The footer hints follow the artboard's order, in lowercase, with the
/// keymap's own bindings.
#[gpui::test]
fn the_footer_hints_follow_the_artboards_order(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    let text = opened.rendered_text();

    let at = |word: &str| {
        text.find(word)
            .unwrap_or_else(|| panic!("`{word}` in the footer: {text}"))
    };
    assert!(at("reveal") < at("copy ARN"), "{text}");
    assert!(at("copy ARN") < at("edit"), "{text}");
    assert!(at("edit") < at("roll back"), "{text}");
}

/// While a value is edited the tab is one header row, the editor and a
/// status bar: no separate toolbar row, no paragraph under the editor.
#[gpui::test]
fn the_editing_tab_is_a_header_row_the_editor_and_a_status_bar(cx: &mut TestAppContext) {
    let mut opened = open_document_with(cx, PLAINTEXT_SECRET, Some(CURRENT_REGION), true);
    opened.window.simulate_input("!");
    opened.window.run_until_parked();

    let name = opened.bounds("secret-document-name");
    for id in [
        "secret-value-editor-modified",
        "secret-value-editor-find",
        "secret-value-editor-discard",
        "secret-value-editor-save",
    ] {
        let bounds = opened.bounds(id);
        assert!(
            (bounds.center().y - name.center().y).abs() < gpui::px(24.0), // guardrail-allow: test geometry, not spacing
            "`{id}` shares the header row"
        );
    }
    assert!(opened.is_drawn("secret-document-subtitle"));

    let editor = opened.bounds("secret-value-editor");
    let status = opened.bounds("secret-document-editor-status");
    assert!(editor.bottom() <= status.top() + gpui::px(1.0)); // guardrail-allow: test geometry, not spacing
    assert!(
        editor.size.height > gpui::px(300.0), // guardrail-allow: test geometry, not spacing
        "the editor fills the tab: {editor:?}"
    );
    assert!(
        status.bottom() > opened.bounds("secret-document").bottom() - gpui::px(48.0), // guardrail-allow: test geometry, not spacing
        "the status bar is at the bottom"
    );

    let text = opened.rendered_text();
    assert!(text.contains("Saving creates a new version"), "{text}");
    assert!(text.contains("1 line changed"), "{text}");
    assert!(text.contains("Ln "), "{text}");
    assert!(
        !text.contains("Saving writes a new version"),
        "no paragraph: {text}"
    );
    assert!(!opened.is_drawn("secret-document-delete"));
}

/// A long value does not leave the editor stranded at a fixed height: it
/// fills the tab down to the status bar and scrolls, so the last line is
/// reachable and the caret reports it.
#[gpui::test]
fn a_long_value_fills_the_tab_and_scrolls_to_its_last_line(cx: &mut TestAppContext) {
    let mut opened = open_document_with(cx, PLAINTEXT_SECRET, Some(CURRENT_REGION), true);
    let long: String = (1..=140)
        .map(|line| format!("KEY_{line}=value-{line}"))
        .collect::<Vec<_>>()
        .join("\n");
    let doc = opened.doc.clone();
    opened.window.update(|window, cx| {
        let view = doc.read(cx).view().clone();
        view.update(cx, |view, cx| view.replace_editor_text(&long, window, cx));
    });
    opened.window.run_until_parked();

    let editor = opened.bounds("secret-value-editor");
    let status = opened.bounds("secret-document-editor-status");
    assert!(
        status.top() - editor.bottom() < gpui::px(4.0), // guardrail-allow: test geometry, not spacing
        "the editor reaches the status bar: {editor:?} / {status:?}"
    );
    let laid_out = opened
        .window
        .update(|_, cx| {
            let view = doc.read(cx).view().clone();
            view.read(cx).editor_viewport_height(cx)
        })
        .expect("an open editor");
    assert!(
        laid_out > gpui::px(500.0), // guardrail-allow: test geometry, not spacing
        "the text area fills the tab, not the old 14-row cap: {laid_out:?}"
    );

    // The caret at the end of the value is on line 140, in view.
    opened.window.simulate_keystrokes("ctrl-end");
    opened.window.run_until_parked();
    let line = opened.window.update(|_, cx| {
        let view = doc.read(cx).view().clone();
        view.read(cx).editor_caret_line(cx)
    });
    assert_eq!(line, Some(140));
    assert!(opened.rendered_text().contains("Ln 140"));
}

// -- One secret in two hosts ---------------------------------------------------

/// A tab and the listing's pane over the same secret, both with the value
/// loaded.
struct Pair {
    listing: Entity<SecretStoreDocument>,
}

impl Opened<'_> {
    fn open_listing_pane(&mut self, name: &str) -> Pair {
        let (state, profile_id) = (self.app_state.clone(), self.profile_id);
        let listing = self.window.update(|window, cx| {
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
        self.window.run_until_parked();

        let name = name.to_string();
        let pane = listing.clone();
        self.window.update(|_, cx| {
            pane.update(cx, |listing, cx| {
                listing.select_row(crate::secret_store::SecretRowKey::Secret(name), cx);
                let view = listing.view().clone();
                view.update(cx, |view, cx| view.retrieve_selected_value(cx));
            })
        });
        self.window.run_until_parked();

        Pair { listing }
    }

    fn pane_view<R>(
        &mut self,
        pair: &Pair,
        f: impl FnOnce(
            &mut crate::secret_store::SecretView,
            &mut gpui::Context<crate::secret_store::SecretView>,
        ) -> R,
    ) -> R {
        let listing = pair.listing.clone();
        let result = self.window.update(|_, cx| {
            let view = listing.read(cx).view().clone();
            view.update(cx, f)
        });
        self.window.run_until_parked();
        result
    }

    fn pane_value_loaded(&mut self, pair: &Pair) -> bool {
        self.pane_view(pair, |view, _| {
            matches!(view.value(), ValueState::Loaded(_))
        })
    }

    fn tab_value_loaded(&mut self) -> bool {
        self.read_view(|view| matches!(view.value(), ValueState::Loaded(_)))
    }
}

/// Rotates the secret from the pane and confirms.
fn rotate_in_pane(opened: &mut Opened<'_>, pair: &Pair) {
    let listing = pair.listing.clone();
    opened.window.update(|window, cx| {
        let view = listing.read(cx).view().clone();
        view.update(cx, |view, cx| {
            view.rotate_selected_now(window, cx);
            view.confirm_dialog(window, cx);
        });
    });
    opened.window.run_until_parked();
}

/// Rotates the secret from the tab and confirms.
fn rotate_in_tab(opened: &mut Opened<'_>) {
    let doc = opened.doc.clone();
    opened.window.update(|window, cx| {
        let view = doc.read(cx).view().clone();
        view.update(cx, |view, cx| {
            view.rotate_selected_now(window, cx);
            view.confirm_dialog(window, cx);
        });
    });
    opened.window.run_until_parked();
}

/// A write in the tab leaves the pane's retrieved value stale: the pane goes
/// back to "Value not loaded" and re-retrieves on demand, through the audited
/// path.
#[gpui::test]
fn a_rotation_in_the_tab_drops_the_panes_loaded_value(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    opened.click("secret-retrieve-value");
    let pair = opened.open_listing_pane(KEY_VALUE_SECRET);
    assert!(opened.pane_value_loaded(&pair) && opened.tab_value_loaded());

    rotate_in_tab(&mut opened);

    assert!(
        !opened.pane_value_loaded(&pair),
        "the pane dropped the value"
    );
    let before = opened.driver.secret_store_calls().value_requests.len();
    opened.pane_view(&pair, |view, cx| view.retrieve_selected_value(cx));
    assert!(opened.pane_value_loaded(&pair));
    assert_eq!(
        opened.driver.secret_store_calls().value_requests.len(),
        before + 1,
        "a new audited retrieval"
    );
}

/// And the other way: a write in the pane drops the tab's value.
#[gpui::test]
fn a_rotation_in_the_pane_drops_the_tabs_loaded_value(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    opened.click("secret-retrieve-value");
    let pair = opened.open_listing_pane(KEY_VALUE_SECRET);
    assert!(opened.tab_value_loaded());

    rotate_in_pane(&mut opened, &pair);

    assert!(!opened.tab_value_loaded(), "the tab dropped the value");
}

/// A roll back in the tab drops the pane's loaded value.
#[gpui::test]
fn a_roll_back_in_the_tab_drops_the_panes_loaded_value(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    opened.click("secret-retrieve-value");
    let pair = opened.open_listing_pane(KEY_VALUE_SECRET);

    let doc = opened.doc.clone();
    opened.window.update(|window, cx| {
        let view = doc.read(cx).view().clone();
        view.update(cx, |view, cx| {
            view.open_roll_back(1, window, cx);
            view.confirm_dialog(window, cx);
        });
    });
    opened.window.run_until_parked();

    assert!(!opened.pane_value_loaded(&pair));
}

/// A roll back in the pane drops the tab's loaded value.
#[gpui::test]
fn a_roll_back_in_the_pane_drops_the_tabs_loaded_value(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    opened.click("secret-retrieve-value");
    let pair = opened.open_listing_pane(KEY_VALUE_SECRET);

    let listing = pair.listing.clone();
    opened.window.update(|window, cx| {
        let view = listing.read(cx).view().clone();
        view.update(cx, |view, cx| {
            view.open_roll_back(1, window, cx);
            view.confirm_dialog(window, cx);
        });
    });
    opened.window.run_until_parked();

    assert!(!opened.tab_value_loaded());
}

/// A save from the tab's editor drops the pane's value too.
#[gpui::test]
fn a_save_in_the_tab_drops_the_panes_loaded_value(cx: &mut TestAppContext) {
    let mut opened = open_document_with(cx, PLAINTEXT_SECRET, Some(CURRENT_REGION), true);
    let pair = opened.open_listing_pane(PLAINTEXT_SECRET);
    assert!(opened.pane_value_loaded(&pair));

    opened.window.simulate_input("!");
    opened.window.run_until_parked();
    opened.click("secret-value-editor-save");

    assert_eq!(opened.driver.secret_store_calls().put_requests.len(), 1);
    assert!(!opened.pane_value_loaded(&pair));
}

/// A tab editor with unsaved text keeps it when the secret changes elsewhere,
/// says so inline, and Save still writes through the gates.
#[gpui::test]
fn an_editor_with_unsaved_text_keeps_it_when_the_secret_changes_elsewhere(cx: &mut TestAppContext) {
    let mut opened = open_document_with(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION), true);
    let pair = opened.open_listing_pane(KEY_VALUE_SECRET);
    // The value is JSON, so the edit has to stay valid JSON to be saved.
    opened.window.simulate_keystrokes("ctrl-a");
    opened.window.simulate_input("{\"changed\": 1}");
    opened.window.run_until_parked();
    assert!(!opened.is_drawn("secret-value-editor-changed-elsewhere"));

    rotate_in_pane(&mut opened, &pair);

    assert!(
        opened.read_view(|view| view.is_editing_value()),
        "the text is kept"
    );
    assert!(opened.is_drawn("secret-value-editor-changed-elsewhere"));
    let before = opened.driver.secret_store_calls().put_requests.len();
    opened.click("secret-value-editor-save");
    assert_eq!(
        opened.driver.secret_store_calls().put_requests.len(),
        before + 1,
        "Save still writes a new version"
    );
}

/// A clean open editor closes when the secret changes elsewhere.
#[gpui::test]
fn a_clean_editor_closes_when_the_secret_changes_elsewhere(cx: &mut TestAppContext) {
    let mut opened = open_document_with(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION), true);
    let pair = opened.open_listing_pane(KEY_VALUE_SECRET);
    assert!(opened.read_view(|view| view.is_editing_value()));

    rotate_in_pane(&mut opened, &pair);

    assert!(!opened.read_view(|view| view.is_editing_value()));
    assert!(!opened.tab_value_loaded());
}

/// The region moves under a tab that holds a value and an open editor: every
/// entry of the `m` menu is disabled, and running one anyway neither writes
/// nor raises an error or a toast.
#[gpui::test]
fn a_region_mismatch_disables_the_menu_and_running_save_is_silent(cx: &mut TestAppContext) {
    let mut opened = open_document_with(cx, PLAINTEXT_SECRET, Some(CURRENT_REGION), true);
    opened.window.simulate_input("!");
    opened.window.run_until_parked();
    assert!(opened.read_view(|view| view.is_editing_value()));

    // The switch installs a new driver: a put that went there would not show
    // on the original one.
    let replacement = opened.move_connection_to(OTHER_REGION);
    assert!(opened.block().is_some());

    let doc = opened.doc.clone();
    let actions = opened
        .window
        .update(|_, cx| doc.read(cx).pane_actions(&doc, cx));
    for id in [
        "secret-editor-save",
        "secret-editor-find",
        "secret-editor-discard",
        "secret-copy-value",
        "secret-reveal-all",
    ] {
        if let Some(action) = actions.iter().find(|action| action.id.as_ref() == id) {
            assert!(!action.enabled, "`{id}` is disabled");
        }
    }
    assert!(
        actions.iter().all(|action| !action.enabled),
        "{:?}",
        actions
            .iter()
            .filter(|action| action.enabled)
            .map(|action| action.id.to_string())
            .collect::<Vec<_>>()
    );

    let writes = opened.driver.secret_store_calls().put_attempts.len();
    for action in actions {
        // Copying the name touches neither the store nor the value; its
        // confirmation toast is not what this test is about.
        if action.id.as_ref() == "secret-copy-name" {
            continue;
        }

        if let crate::pane::PaneActionRun::Callback(run) = action.run {
            opened.window.update(|window, cx| run(window, cx));
        }
    }
    opened.window.run_until_parked();
    opened.command(Command::SaveQuery);

    assert_eq!(
        opened.driver.secret_store_calls().put_attempts.len(),
        writes
    );
    assert!(
        replacement.secret_store_calls().put_attempts.is_empty(),
        "nothing reached the replacement driver either"
    );
    assert_eq!(
        opened.unread_errors(),
        0,
        "no error: {:?}",
        opened.error_summaries()
    );
    let toast = opened.window.update(|_, cx| {
        cx.global::<dbflux_ui_base::toast::ToastGlobal>()
            .host
            .clone()
            .read(cx)
            .last_toast_title()
    });
    assert_eq!(opened.toasts(), 0, "no toast: {toast:?}");
}

fn audited_retrievals(opened: &mut Opened<'_>) -> usize {
    opened
        .audit_actions()
        .iter()
        .filter(|action| action.as_str() == "secret_value_retrieved")
        .count()
}

/// a dirty follower keeps its text; once it discards, the value it
/// shows must not be the stale one it retrieved before the write.
#[gpui::test]
fn a_dirty_follower_shows_no_stale_value_after_discard(cx: &mut TestAppContext) {
    let mut opened = open_document_with(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION), true);
    let pair = opened.open_listing_pane(KEY_VALUE_SECRET);
    opened.window.simulate_keystrokes("ctrl-a");
    opened.window.simulate_input("{\"changed\": 1}");
    opened.window.run_until_parked();

    rotate_in_pane(&mut opened, &pair);
    assert!(opened.read_view(|view| view.is_editing_value()));

    let doc = opened.doc.clone();
    opened.window.update(|_, cx| {
        let view = doc.read(cx).view().clone();
        view.update(cx, |view, cx| view.discard_value_edit(cx));
    });
    opened.window.run_until_parked();

    assert!(!opened.read_view(|view| view.is_editing_value()));
    assert!(
        !opened.tab_value_loaded(),
        "after discard the tab still shows the value retrieved before the write"
    );
}

/// the dirty follower's save lands, then the pane (the writer of the
/// rotation) follows the tab's save in turn.
#[gpui::test]
fn a_dirty_followers_save_is_followed_by_the_other_host(cx: &mut TestAppContext) {
    let mut opened = open_document_with(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION), true);
    let pair = opened.open_listing_pane(KEY_VALUE_SECRET);
    opened.window.simulate_keystrokes("ctrl-a");
    opened.window.simulate_input("{\"changed\": 2}");
    opened.window.run_until_parked();
    rotate_in_pane(&mut opened, &pair);

    opened.pane_view(&pair, |view, cx| view.retrieve_selected_value(cx));
    assert!(opened.pane_value_loaded(&pair));

    let puts = opened.driver.secret_store_calls().put_requests.len();
    opened.click("secret-value-editor-save");
    assert_eq!(
        opened.driver.secret_store_calls().put_requests.len(),
        puts + 1
    );
    assert!(!opened.is_drawn("secret-value-editor-changed-elsewhere"));
    assert!(
        !opened.pane_value_loaded(&pair),
        "pane follows the tab's save"
    );
    assert_eq!(opened.unread_errors(), 0);
}

/// the re-retrieval after a follow is audited exactly once.
#[gpui::test]
fn retrieving_after_following_a_change_is_audited_once(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    opened.click("secret-retrieve-value");
    let pair = opened.open_listing_pane(KEY_VALUE_SECRET);
    rotate_in_pane(&mut opened, &pair);
    assert!(!opened.tab_value_loaded());

    let before = audited_retrievals(&mut opened);
    opened.click("secret-retrieve-value");
    assert!(opened.tab_value_loaded());
    assert_eq!(audited_retrievals(&mut opened), before + 1);
}

/// a write to another secret leaves the pane's loaded value
/// of a different secret alone.
#[gpui::test]
fn a_write_to_another_secret_keeps_the_loaded_value(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    opened.click("secret-retrieve-value");
    let pair = opened.open_listing_pane(PLAINTEXT_SECRET);
    assert!(opened.pane_value_loaded(&pair));

    rotate_in_tab(&mut opened);

    assert!(opened.pane_value_loaded(&pair), "other secret untouched");
}

/// The primary save chord, pressed in a keymap host after the region moved
/// under the open editor of a tab, writes nothing, raises no error and shows
/// no toast, in either session.
#[gpui::test]
fn the_save_chord_of_a_region_mismatched_editor_is_silent_through_the_host(
    cx: &mut TestAppContext,
) {
    use crate::keyboard_test_support::host_document;

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
    let (host, window) = host_document(
        cx,
        move |_window, cx| {
            cx.new(|cx| {
                let mut document = SecretDocument::new(
                    profile_id,
                    FAKE_SECRETS_STORE_ID.to_string(),
                    PLAINTEXT_SECRET.to_string(),
                    None,
                    Some(CURRENT_REGION.to_string()),
                    state,
                    cx,
                );
                document.begin_edit(cx);
                document
            })
        },
        |doc, cx| doc.active_context(cx),
        |doc, command, window, cx| doc.dispatch_command(command, window, cx),
    );
    let doc = window.update(|_, cx| host.read(cx).document.clone());
    window.update(|window, _| window.activate_window());
    window.simulate_resize(gpui::size(gpui::px(1500.0), gpui::px(1400.0))); // guardrail-allow: test geometry, not spacing
    window.run_until_parked();
    window.simulate_input("!");
    window.run_until_parked();

    // The region moves under the open editor.
    let replacement =
        FakeDriver::new(DbKind::AwsSecrets).with_region_switch(&[CURRENT_REGION, OTHER_REGION]);
    let moved = ConnectionProfile::new(
        "prod-aws",
        DbConfig::AwsSecrets {
            region: OTHER_REGION.to_string(),
            profile: None,
            access_key_id: None,
            endpoint: None,
        },
    );
    let connection = replacement.connect_arc(&moved).expect("connect");
    window.update(|_, cx| {
        app_state.update(cx, |state, cx| {
            if let Some(connected) = state.connections_mut().get_mut(&profile_id) {
                connected.connection = connection.clone();
            }
            cx.emit(AppStateChanged);
        });
    });
    window.run_until_parked();
    assert!(window.update(|_, cx| doc.read(cx).block(cx).is_some()));

    let chord = if cfg!(target_os = "macos") {
        "cmd-s"
    } else {
        "ctrl-s"
    };
    window.simulate_keystrokes(chord);
    window.run_until_parked();

    // The blocked tab draws no editor, so the keyboard context falls back to
    // the results one, where the save chord is not bound: it does not even
    // reach the host. Either way nothing below may happen.
    let reached = window.update(|_, cx| host.read(cx).commands.clone());
    assert!(
        !reached.contains(&Command::SaveQuery),
        "the chord is not bound once the editor is gone: {reached:?}"
    );
    assert!(driver.secret_store_calls().put_attempts.is_empty());
    assert!(replacement.secret_store_calls().put_attempts.is_empty());
    let errors = window.update(|_, cx| app_state.read(cx).unread_error_count);
    assert_eq!(errors, 0);
    let toasts = window.update(|_, cx| {
        cx.global::<dbflux_ui_base::toast::ToastGlobal>()
            .host
            .clone()
            .read(cx)
            .toast_count()
    });
    assert_eq!(toasts, 0);
}

/// A re-read after a session change wrote nothing, so the other host's
/// loaded value is not stale and stays.
#[gpui::test]
fn a_re_read_that_wrote_nothing_keeps_the_other_hosts_value(cx: &mut TestAppContext) {
    let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
    opened.click("secret-retrieve-value");
    let pair = opened.open_listing_pane(KEY_VALUE_SECRET);
    assert!(opened.tab_value_loaded());

    opened.pane_view(&pair, |view, cx| view.refresh_selection_for_session(cx));

    assert!(opened.tab_value_loaded(), "no write, no drop");
    assert!(opened.pane_value_loaded(&pair));
}

// -- Review round: notifications, close and quit, deletion under a draft -------

mod review_round {
    use super::*;
    use crate::handle::DocumentEvent;
    use crate::pane::{CloseDisposition, QuitDisposition};
    use crate::secret_store::{SecretRowKey, WriteEffect};
    use dbflux_core::DbError;
    use std::cell::RefCell;
    use std::rc::Rc;

    /// Types over the whole editor so the text differs from the stored one.
    /// The value is JSON, so the edit stays valid JSON to be savable.
    fn make_dirty(opened: &mut Opened<'_>) {
        opened.window.simulate_keystrokes("ctrl-a");
        opened.window.simulate_input("{\"changed\": 1}");
        opened.window.run_until_parked();
    }

    fn dirty_tab<'a>(cx: &'a mut TestAppContext) -> Opened<'a> {
        let mut opened = open_document_with(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION), true);
        make_dirty(&mut opened);
        opened
    }

    fn record_events(opened: &mut Opened<'_>) -> Rc<RefCell<Vec<String>>> {
        let log = Rc::new(RefCell::new(Vec::new()));
        let doc = opened.doc.clone();
        let sink = log.clone();
        opened.window.update(|_, cx| {
            cx.subscribe(&doc, move |_, event: &DocumentEvent, _| {
                sink.borrow_mut().push(format!("{event:?}"));
            })
            .detach();
        });
        log
    }

    fn failure(
        kind: crate::secret_store::SecretWriteFailureKind,
    ) -> crate::secret_store::SecretWriteFailure {
        crate::secret_store::SecretWriteFailure {
            error: DbError::Timeout,
            correlation_id: None,
            kind,
            audit_unrecorded: false,
        }
    }

    fn select_in_pane(opened: &mut Opened<'_>, pair: &Pair, name: &str) {
        let listing = pair.listing.clone();
        let name = name.to_string();
        opened.window.update(|_, cx| {
            listing.update(cx, |listing, cx| {
                listing.select_row(SecretRowKey::Secret(name), cx)
            })
        });
        opened.window.run_until_parked();
    }

    /// #6: another view's change that arrives while a save is in flight is
    /// not erased when the save's answer lands.
    #[gpui::test]
    fn a_save_answer_keeps_an_external_change_that_arrived_meanwhile(cx: &mut TestAppContext) {
        let mut opened = dirty_tab(cx);
        let origin = opened.foreign_origin();
        let doc = opened.doc.clone();
        let (profile_id, region) = (opened.profile_id, CURRENT_REGION.to_string());

        opened.window.update(|_, cx| {
            let view = doc.read(cx).view().clone();
            view.update(cx, |view, cx| view.save_value_edit(cx));
            changes::publish(
                SecretChange {
                    profile_id,
                    store_id: FAKE_SECRETS_STORE_ID.to_string(),
                    region: Some(region),
                    name: KEY_VALUE_SECRET.to_string(),
                    kind: SecretChangeKind::Changed,
                    origin,
                },
                cx,
            );
        });
        opened.window.run_until_parked();

        assert!(
            !opened.tab_value_loaded(),
            "the invalidation survived the answer"
        );
        assert!(
            !opened.read_view(|view| view.is_saving_value_edit()),
            "the saving state was released"
        );
    }

    /// #7: a region switch that hides the connection while a save is in
    /// flight is a replaced session: the editor is kept and the user is told.
    #[gpui::test]
    fn a_save_answer_after_a_region_switch_keeps_the_editor(cx: &mut TestAppContext) {
        let mut opened = dirty_tab(cx);
        let doc = opened.doc.clone();

        opened.window.update(|_, cx| {
            let view = doc.read(cx).view().clone();
            view.update(cx, |view, cx| view.save_value_edit(cx));
        });
        opened.swap_connection(OTHER_REGION, false);
        opened.window.run_until_parked();

        assert!(
            opened.read_view(|view| view.is_editing_value()),
            "the editor stays: the write landed on the previous session"
        );
        assert!(
            opened
                .error_summaries()
                .iter()
                .any(|summary| summary.contains("previous")),
            "the user is told where the write went: {:?}",
            opened.error_summaries()
        );
    }

    /// #8: a save that may have landed tells the other views.
    #[gpui::test]
    fn a_save_that_may_have_landed_makes_the_other_views_drop_their_value(cx: &mut TestAppContext) {
        use crate::secret_store::SecretWriteFailureKind::{AuditLost, Indeterminate};

        for kind in [AuditLost, Indeterminate] {
            let mut opened = open_document_with(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION), true);
            let pair = opened.open_listing_pane(KEY_VALUE_SECRET);
            make_dirty(&mut opened);
            assert!(opened.pane_value_loaded(&pair));

            let doc = opened.doc.clone();
            opened.window.update(|_, cx| {
                let view = doc.read(cx).view().clone();
                let (generation, connection, scope) = view.update(cx, |view, cx| {
                    let connection = view.connection(cx).expect("connected");
                    let scope = view.change_scope(connection.current_region(), cx);
                    (view.write_generation_for_test(), connection, scope)
                });
                crate::secret_store::land_save_answer(
                    &view,
                    &scope,
                    generation,
                    KEY_VALUE_SECRET,
                    Err(failure(kind)),
                    &connection,
                    cx,
                );
            });
            opened.window.run_until_parked();

            assert!(!opened.pane_value_loaded(&pair), "{kind:?}");
            assert!(
                opened.read_view(|view| view.is_editing_value()),
                "text kept: {kind:?}"
            );
        }
    }

    /// #9: a write announces the secret it wrote, not whatever the writer has
    /// selected when the answer lands.
    #[gpui::test]
    fn a_write_announces_its_target_not_the_current_selection(cx: &mut TestAppContext) {
        let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
        opened.click("secret-retrieve-value");
        let pair = opened.open_listing_pane(KEY_VALUE_SECRET);
        select_in_pane(&mut opened, &pair, PLAINTEXT_SECRET);

        opened.pane_view(&pair, |view, cx| {
            view.refresh_after_write(KEY_VALUE_SECRET, WriteEffect::Versions, cx)
        });

        assert!(
            !opened.tab_value_loaded(),
            "the tab of the written secret followed"
        );
    }

    /// #24: a removal is published even when the writer moved its selection.
    #[gpui::test]
    fn a_removal_is_published_when_the_selection_moved(cx: &mut TestAppContext) {
        let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
        let pair = opened.open_listing_pane(KEY_VALUE_SECRET);
        select_in_pane(&mut opened, &pair, PLAINTEXT_SECRET);

        opened.pane_view(&pair, |view, cx| {
            view.refresh_after_write(KEY_VALUE_SECRET, WriteEffect::Removed, cx)
        });

        assert_eq!(opened.block(), Some(SecretDocumentBlock::Removed));
        let selected = opened
            .window
            .update(|_, cx| pair.listing.read(cx).selected().cloned());
        assert_eq!(
            selected,
            Some(SecretRowKey::Secret(PLAINTEXT_SECRET.to_string())),
            "the new selection is untouched"
        );
    }

    /// #16: a write whose tab closed mid-flight still reaches the others.
    #[gpui::test]
    fn a_write_landing_after_its_tab_closed_still_announces(cx: &mut TestAppContext) {
        let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
        let pair = opened.open_listing_pane(KEY_VALUE_SECRET);
        assert!(opened.pane_value_loaded(&pair));

        // A second tab, built outside the window's root so dropping it frees it.
        let (state, profile_id) = (opened.app_state.clone(), opened.profile_id);
        let doc = opened.window.update(|_, cx| {
            cx.new(|cx| {
                SecretDocument::new(
                    profile_id,
                    FAKE_SECRETS_STORE_ID.to_string(),
                    KEY_VALUE_SECRET.to_string(),
                    None,
                    Some(CURRENT_REGION.to_string()),
                    state,
                    cx,
                )
            })
        });
        opened.window.run_until_parked();
        opened.window.update(|window, cx| {
            let view = doc.read(cx).view().clone();
            view.update(cx, |view, cx| {
                view.rotate_selected_now(window, cx);
                view.confirm_dialog(window, cx);
            });
        });
        // The tab closes before the answer lands.
        drop(doc);
        opened.window.run_until_parked();

        assert!(!opened.pane_value_loaded(&pair), "the pane was told");
    }

    /// #16: a save whose tab closed before the answer landed still announces
    /// it, from the scope captured when it was sent.
    #[gpui::test]
    fn a_save_landing_after_its_tab_closed_still_announces(cx: &mut TestAppContext) {
        let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
        let pair = opened.open_listing_pane(KEY_VALUE_SECRET);
        assert!(opened.pane_value_loaded(&pair));

        let (state, profile_id) = (opened.app_state.clone(), opened.profile_id);
        let doc = opened.window.update(|_, cx| {
            cx.new(|cx| {
                SecretDocument::new(
                    profile_id,
                    FAKE_SECRETS_STORE_ID.to_string(),
                    KEY_VALUE_SECRET.to_string(),
                    None,
                    Some(CURRENT_REGION.to_string()),
                    state,
                    cx,
                )
            })
        });
        opened.window.run_until_parked();
        opened.window.update(|window, cx| {
            let view = doc.read(cx).view().clone();
            view.update(cx, |view, cx| {
                view.retrieve_selected_value(cx);
            });
            let _ = window;
        });
        opened.window.run_until_parked();
        opened.window.update(|window, cx| {
            let view = doc.read(cx).view().clone();
            view.update(cx, |view, cx| {
                view.start_value_edit(window, cx);
                view.replace_editor_text("{\"changed\": 3}", window, cx);
                view.save_value_edit(cx);
            });
        });
        // The tab closes before the answer lands: the close hook runs, and
        // the view must hold neither the value nor the editor's text any
        // more, though the write in flight keeps the view alive.
        let view = opened.window.update(|_, cx| {
            let view = doc.read(cx).view().clone();
            SecretDocument::into_pane(doc.clone(), cx).on_close(cx);
            view
        });
        opened.window.update(|_, cx| {
            let view = view.read(cx);
            assert!(!view.is_editing_value(), "the draft is gone");
            assert!(
                matches!(view.value(), ValueState::NotLoaded),
                "the value is gone"
            );
        });
        drop(doc);
        opened.window.run_until_parked();

        assert_eq!(opened.driver.secret_store_calls().put_requests.len(), 1);
        assert!(!opened.pane_value_loaded(&pair), "the pane was told");
        opened.window.update(|_, cx| {
            let view = view.read(cx);
            assert!(!view.is_editing_value() && matches!(view.value(), ValueState::NotLoaded));
        });
    }

    /// #14: a tab with unsaved edits needs a decision at quit; a clean one
    /// does not.
    #[gpui::test]
    fn a_dirty_tab_needs_a_quit_decision(cx: &mut TestAppContext) {
        let mut opened = open_document_with(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION), true);
        let doc = opened.doc.clone();
        let disposition = |opened: &mut Opened<'_>| {
            let doc = doc.clone();
            opened
                .window
                .update(|_, cx| SecretDocument::into_pane(doc, cx).quit_disposition(cx))
        };
        assert_eq!(disposition(&mut opened), QuitDisposition::Clean);

        make_dirty(&mut opened);
        assert_eq!(disposition(&mut opened), QuitDisposition::NeedsDecision);
    }

    /// #14: Save in the quit dialog saves through the normal gates and
    /// reports the outcome; Discard drops the draft.
    #[gpui::test]
    fn the_quit_save_writes_through_the_gates_and_reports_the_outcome(cx: &mut TestAppContext) {
        let mut opened = dirty_tab(cx);
        let log = record_events(&mut opened);
        let doc = opened.doc.clone();

        let started = opened.window.update(|window, cx| {
            SecretDocument::into_pane(doc.clone(), cx).save_for_quit(window, cx)
        });
        opened.window.run_until_parked();

        assert!(started);
        assert_eq!(opened.driver.secret_store_calls().put_requests.len(), 1);
        assert!(
            log.borrow()
                .iter()
                .any(|event| event.contains("SaveFinished") && event.contains("true")),
            "{:?}",
            log.borrow()
        );
        assert!(
            !log.borrow()
                .iter()
                .any(|event| event.contains("RequestClose")),
            "a quit save does not close the tab"
        );
    }

    #[gpui::test]
    fn the_quit_discard_drops_the_draft(cx: &mut TestAppContext) {
        let mut opened = dirty_tab(cx);
        let doc = opened.doc.clone();

        opened
            .window
            .update(|_, cx| SecretDocument::into_pane(doc.clone(), cx).discard_for_quit(cx));

        assert!(!opened.read_view(|view| view.is_editing_value()));
    }

    /// #15: Save in the close dialog writes, then closes the tab.
    #[gpui::test]
    fn the_close_save_writes_then_asks_to_close(cx: &mut TestAppContext) {
        let mut opened = dirty_tab(cx);
        let log = record_events(&mut opened);
        let doc = opened.doc.clone();

        let started = opened.window.update(|window, cx| {
            SecretDocument::into_pane(doc.clone(), cx).save_for_close(window, cx)
        });
        opened.window.run_until_parked();

        assert!(started);
        assert_eq!(opened.driver.secret_store_calls().put_requests.len(), 1);
        assert!(
            log.borrow()
                .iter()
                .any(|event| event.contains("RequestClose")),
            "{:?}",
            log.borrow()
        );
    }

    /// #15: a refused save does not start, and the tab stays open.
    #[gpui::test]
    fn a_refused_close_save_keeps_the_tab_open(cx: &mut TestAppContext) {
        let mut opened = dirty_tab(cx);
        let log = record_events(&mut opened);
        opened.move_connection_to(OTHER_REGION);
        let doc = opened.doc.clone();

        let started = opened.window.update(|window, cx| {
            SecretDocument::into_pane(doc.clone(), cx).save_for_close(window, cx)
        });
        opened.window.run_until_parked();

        assert!(!started);
        assert!(opened.driver.secret_store_calls().put_requests.is_empty());
        assert!(
            !log.borrow()
                .iter()
                .any(|event| event.contains("RequestClose"))
        );
        let _ = CloseDisposition::KeepOpen;
    }

    /// #10: a deletion under unsaved edits keeps the text, read-only, says so
    /// and refuses writes; Discard drops it.
    #[gpui::test]
    fn a_deleted_secret_keeps_the_unsaved_text_read_only(cx: &mut TestAppContext) {
        let mut opened = dirty_tab(cx);

        opened.announce(SecretChangeKind::Removed, KEY_VALUE_SECRET);

        assert_eq!(opened.block(), Some(SecretDocumentBlock::Removed));
        assert!(
            opened.is_drawn("secret-document-removed"),
            "says it was deleted"
        );
        assert!(
            opened.read_view(|view| view.is_editing_value()),
            "the draft is kept"
        );
        assert!(opened.is_drawn("secret-value-editor"), "and shown");

        opened.command(Command::SaveQuery);
        assert!(opened.driver.secret_store_calls().put_attempts.is_empty());
        assert_eq!(opened.unread_errors(), 0);

        opened.click("secret-value-editor-discard");
        assert!(!opened.read_view(|view| view.is_editing_value()));
        assert!(opened.is_drawn("secret-document-removed"));
    }
}

mod review_round_ui {
    use super::*;
    use dbflux_test_support::fake_driver::FAKE_PARAMETER_STORE_ID;

    const PARAMETER: &str = "/app/prod/db/host";

    fn width_of(opened: &mut Opened<'_>, id: &str) -> gpui::Pixels {
        opened.bounds(id).size.width
    }

    /// #3: the row menu offers Edit value before the value was retrieved, as
    /// the bar does.
    #[gpui::test]
    fn the_row_menu_offers_edit_value_for_an_unretrieved_secret(cx: &mut TestAppContext) {
        let opened = open_document(cx, PLAINTEXT_SECRET, Some(CURRENT_REGION));
        let doc = opened.doc.clone();
        let items = opened.window.update(|_, cx| {
            let view = doc.read(cx).view().clone();
            view.read(cx).menu_actions_for_test(PLAINTEXT_SECRET)
        });
        assert!(items.iter().any(|item| item == "EditValue"), "{items:?}");
    }

    /// #4: below a width the two columns stack and nothing overlaps; wide,
    /// they sit side by side.
    #[gpui::test]
    fn the_columns_stack_when_the_tab_is_narrow(cx: &mut TestAppContext) {
        let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
        let wide_value = opened.bounds("secret-document-value-card");
        let wide_rotation = opened.bounds("secret-document-rotation-card");
        assert!(
            wide_rotation.left() >= wide_value.right(),
            "side by side when wide"
        );

        opened
            .window
            .simulate_resize(gpui::size(gpui::px(650.0), gpui::px(1400.0))); // guardrail-allow: test geometry, not spacing
        opened.window.run_until_parked();

        let value = opened.bounds("secret-document-value-card");
        let versions = opened.bounds("secret-document-versions-card");
        let rotation = opened.bounds("secret-document-rotation-card");
        assert!(
            rotation.top() >= versions.bottom() - gpui::px(1.0), // guardrail-allow: test geometry, not spacing
            "the side column moved below the main one: {versions:?} / {rotation:?}"
        );
        assert!(
            width_of(&mut opened, "secret-document-versions-card")
                >= value.size.width - gpui::px(1.0)
        ); // guardrail-allow: test geometry, not spacing
        assert!(
            opened.is_drawn("secret-version-0-labels"),
            "staging labels stay visible"
        );
    }

    /// #5: Last changed shows in the tab for a store without typed values.
    #[gpui::test]
    fn the_tab_shows_last_changed(cx: &mut TestAppContext) {
        let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
        assert!(opened.rendered_text().contains("Last changed"));
    }

    /// #23: a plain-history store's tab offers Edit description in its
    /// Details card.
    #[gpui::test]
    fn a_parameter_tab_offers_edit_description_in_its_details_card(cx: &mut TestAppContext) {
        let mut opened = open_document_in(
            cx,
            FAKE_PARAMETER_STORE_ID,
            PARAMETER,
            Some(CURRENT_REGION),
            false,
            true,
        );
        assert!(opened.is_drawn("secret-details-edit"));
        assert!(opened.inside("secret-details-edit", "secret-document-details-card"));
        assert_eq!(opened.text_of("secret-details-edit"), "Edit description");
    }

    /// #13: a removed tab lets the menu commands through, so its Close entry
    /// is reachable from the keyboard; store commands stay blocked.
    #[gpui::test]
    fn a_removed_tab_lets_the_menu_commands_through(cx: &mut TestAppContext) {
        let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
        opened.announce(SecretChangeKind::Removed, KEY_VALUE_SECRET);
        assert_eq!(opened.block(), Some(SecretDocumentBlock::Removed));

        assert!(
            !opened.command(Command::OpenPaneActions),
            "falls through to the workspace"
        );
        assert!(!opened.command(Command::OpenContextMenu));
        assert!(
            opened.command(Command::EditValue),
            "store commands stay swallowed"
        );
    }

    /// #17: before the describe lands the tab shows it is loading and keeps
    /// Refresh; a failed describe shows the error and a retry.
    #[gpui::test]
    fn a_new_tab_shows_loading_then_a_failure_with_retry(cx: &mut TestAppContext) {
        let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
        let doc = opened.doc.clone();
        opened.window.update(|_, cx| {
            let view = doc.read(cx).view().clone();
            view.update(cx, |view, cx| view.load_details_for_test(cx));
        });
        opened.window.run_until_parked();
        assert!(
            opened.is_drawn("secret-document-loading"),
            "loading is shown"
        );
        assert!(opened.is_drawn("secret-document-refresh"), "Refresh stays");

        opened.click("secret-document-refresh");
        assert!(!opened.is_drawn("secret-document-loading"));

        let doc = opened.doc.clone();
        opened.window.update(|_, cx| {
            let view = doc.read(cx).view().clone();
            view.update(cx, |view, cx| view.fail_details_for_test("denied", cx));
        });
        opened.window.run_until_parked();
        assert!(opened.is_drawn("secret-document-details-failed"));
        let before = opened.driver.secret_store_calls().described.len();
        opened.click("secret-document-retry");
        assert!(
            opened.driver.secret_store_calls().described.len() > before,
            "retry reads again"
        );
    }
}

mod review_round_focus {
    use super::*;
    use dbflux_app::keymap::ContextId;

    /// #25: asking again for the editor of a tab that already has it open
    /// puts the keyboard back in it, without recreating it or losing its text.
    #[gpui::test]
    fn a_repeated_edit_request_refocuses_the_open_editor(cx: &mut TestAppContext) {
        let mut opened = open_document_with(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION), true);
        opened.window.update(|window, _| window.activate_window());
        opened.window.simulate_keystrokes("ctrl-a");
        opened.window.simulate_input("{\"kept\": 1}");
        opened.window.run_until_parked();
        let typed = opened.editor_text();

        assert!(opened.command(Command::Cancel), "focus leaves the editor");
        opened.window.run_until_parked();
        assert_eq!(
            opened.doc.read_with_active_context(opened.window),
            ContextId::Results
        );

        let doc = opened.doc.clone();
        opened
            .window
            .update(|_, cx| doc.update(cx, |doc, cx| doc.begin_edit(cx)));
        opened.window.run_until_parked();

        assert_eq!(
            opened.doc.read_with_active_context(opened.window),
            ContextId::TextInput,
            "the editor has the keyboard again"
        );
        assert_eq!(opened.editor_text(), typed);
    }

    impl Opened<'_> {
        fn editor_text(&mut self) -> Option<String> {
            let doc = self.doc.clone();
            self.window.update(|_, cx| {
                let view = doc.read(cx).view().clone();
                view.read(cx).editor_text_for_test(cx)
            })
        }
    }

    trait ActiveContext {
        fn read_with_active_context(&self, window: &mut VisualTestContext) -> ContextId;
    }

    impl ActiveContext for Entity<SecretDocument> {
        fn read_with_active_context(&self, window: &mut VisualTestContext) -> ContextId {
            window.update(|_, cx| self.read(cx).active_context(cx))
        }
    }
}

mod scheduled_deletion {
    use super::*;
    use dbflux_core::SecretDeleteRequest;

    fn dirty_tab_and_pane<'a>(cx: &'a mut TestAppContext) -> (Opened<'a>, Pair) {
        let mut opened = open_document_with(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION), true);
        let pair = opened.open_listing_pane(KEY_VALUE_SECRET);
        opened.window.simulate_keystrokes("ctrl-a");
        opened.window.simulate_input("{\"draft\": 1}");
        opened.window.run_until_parked();
        (opened, pair)
    }

    /// Sends a 7-day scheduled delete to the store and lands it in the pane,
    /// as the confirmed dialog does.
    fn schedule_from_the_pane(opened: &mut Opened<'_>, pair: &Pair) {
        let connection = opened.pane_view(pair, |view, cx| view.connection(cx).expect("connected"));
        let store = connection
            .secret_store(FAKE_SECRETS_STORE_ID)
            .expect("the store");
        let deletion = store
            .delete_secret(&SecretDeleteRequest::Scheduled {
                name: KEY_VALUE_SECRET.to_string(),
                recovery_window_days: 7,
            })
            .expect("scheduled");
        opened.pane_view(pair, |view, cx| {
            view.land_scheduled_delete_for_test(KEY_VALUE_SECRET, deletion, cx)
        });
    }

    fn history_listings(opened: &mut Opened<'_>) -> usize {
        opened.driver.secret_store_calls().version_listings.len()
    }

    /// A scheduled deletion under a dirty draft keeps the draft: read-only
    /// under the deletion notice, Save refused, Discard drops it.
    #[gpui::test]
    fn a_scheduled_deletion_keeps_the_draft_read_only(cx: &mut TestAppContext) {
        let (mut opened, pair) = dirty_tab_and_pane(cx);

        schedule_from_the_pane(&mut opened, &pair);
        // The failure a follower used to record announces an app-state change.
        let state = opened.app_state.clone();
        opened
            .window
            .update(|_, cx| state.update(cx, |_, cx| cx.emit(AppStateChanged)));
        opened.window.run_until_parked();

        assert!(
            opened.read_view(|view| view.is_editing_value()),
            "the draft is kept"
        );
        assert!(opened.is_drawn("secret-value-editor"), "and shown");
        assert!(
            opened.is_drawn("secret-document-deletion-scheduled"),
            "under the notice"
        );

        let puts = opened.driver.secret_store_calls().put_attempts.len();
        opened.command(Command::SaveQuery);
        assert_eq!(opened.driver.secret_store_calls().put_attempts.len(), puts);
        assert!(
            opened
                .error_summaries()
                .iter()
                .any(|summary| summary.contains("Deletion is scheduled")),
            "Save says why it is refused: {:?}",
            opened.error_summaries()
        );

        opened.click("secret-value-editor-discard");
        assert!(!opened.read_view(|view| view.is_editing_value()));
    }

    /// The tab that issued the delete keeps its own draft too, and the
    /// editor is drawn read-only (the frame is the evidence, not view state).
    #[gpui::test]
    fn the_writing_tab_keeps_its_own_draft_under_the_deletion_notice(cx: &mut TestAppContext) {
        let (mut opened, pair) = dirty_tab_and_pane(cx);
        let connection =
            opened.pane_view(&pair, |view, cx| view.connection(cx).expect("connected"));
        let deletion = connection
            .secret_store(FAKE_SECRETS_STORE_ID)
            .expect("the store")
            .delete_secret(&SecretDeleteRequest::Scheduled {
                name: KEY_VALUE_SECRET.to_string(),
                recovery_window_days: 7,
            })
            .expect("scheduled");
        let doc = opened.doc.clone();
        opened.window.update(|_, cx| {
            let view = doc.read(cx).view().clone();
            view.update(cx, |view, cx| {
                view.land_scheduled_delete_for_test(KEY_VALUE_SECRET, deletion, cx)
            });
        });
        opened.window.run_until_parked();

        assert!(opened.is_drawn("secret-value-editor"), "the draft is drawn");
        assert!(opened.is_drawn("secret-document-deletion-scheduled"));
        assert!(opened.is_drawn("secret-restore"), "with Restore");
        assert!(opened.read_view(|view| view.draft_is_locked()));
    }

    /// After Restore the draft is editable again, intact, and Save writes
    /// through the gates.
    #[gpui::test]
    fn a_restore_unlocks_the_draft_and_save_works(cx: &mut TestAppContext) {
        let (mut opened, pair) = dirty_tab_and_pane(cx);
        let typed = opened.editor_text_for_draft();
        schedule_from_the_pane(&mut opened, &pair);
        opened.pane_view(&pair, |view, cx| view.restore_selected(cx));
        opened.window.run_until_parked();

        assert!(!opened.is_drawn("secret-document-deletion-scheduled"));
        assert_eq!(opened.editor_text_for_draft(), typed, "the draft is intact");

        opened.click("secret-value-editor-save");
        assert_eq!(opened.driver.secret_store_calls().put_requests.len(), 1);
    }

    /// Nothing reads from the secret after the notification of its scheduled
    /// deletion, and nothing is reported.
    #[gpui::test]
    fn followers_do_not_read_a_secret_pending_deletion(cx: &mut TestAppContext) {
        let (mut opened, pair) = dirty_tab_and_pane(cx);
        let before = history_listings(&mut opened);
        let values = opened.driver.secret_store_calls().value_requests.len();

        schedule_from_the_pane(&mut opened, &pair);

        assert_eq!(
            history_listings(&mut opened),
            before,
            "no history call after the delete"
        );
        assert_eq!(
            opened.driver.secret_store_calls().value_requests.len(),
            values
        );
        assert_eq!(opened.unread_errors(), 0, "{:?}", opened.error_summaries());
        assert_eq!(opened.toasts(), 1, "only the delete's own toast");
    }

    /// A follower whose history read fails records it inline: no toast, no
    /// error badge.
    #[gpui::test]
    fn a_failing_follower_history_never_toasts(cx: &mut TestAppContext) {
        let mut opened = open_document(cx, KEY_VALUE_SECRET, Some(CURRENT_REGION));
        opened
            .driver
            .set_secret_versions_error(KEY_VALUE_SECRET, "denied");
        let toasts = opened.toasts();

        opened.announce(SecretChangeKind::Changed, KEY_VALUE_SECRET);

        assert_eq!(opened.toasts(), toasts, "recorded inline, not toasted");
        assert!(opened.read_view(|view| matches!(
            view.versions(),
            crate::secret_store::VersionsState::Failed(_)
        )));
    }

    impl Opened<'_> {
        fn editor_text_for_draft(&mut self) -> Option<String> {
            let doc = self.doc.clone();
            self.window.update(|_, cx| {
                let view = doc.read(cx).view().clone();
                view.read(cx).editor_text_for_test(cx)
            })
        }
    }
}
