// The adopted verification probes run under the tests submodule, beside the
// module's own tests.
#[path = "write_probes.rs"]
mod write_probes;
// The write actions of the details pane, driven through the rendered UI.
mod write_ui;
// Follow-ups to the extracted single-secret view.
mod view_followups;
// The pane's hand-over of editing, its header, labels and resizing.
mod pane_chrome;

use super::listing::{
    AUTO_CONTINUE_ENTRY_LIMIT, LARGEST_PAGE_ENTRIES, SecretListing, child_path_label,
    keeps_loading, parent_path, path_segments,
};
use super::render::loaded_label;
use super::value::{
    MASK, RetrievalTarget, RetrievedValue, ValueContent, ValueFailure, encode_base64,
    value_retrieval_event,
};
use super::write::{
    SECRET_VALUE_WRITE_FAILED_ACTION, SECRET_VALUE_WRITE_REFUSED_ACTION,
    SECRET_VALUE_WRITTEN_ACTION, SecretWriteFailure, put_secret_value,
};
use super::{
    DetailsState, PathMode, SecretRowKey, SecretStoreDocument, SecretView, StoreCall,
    VALUE_RETRIEVE_FAILED_ACTION, VALUE_RETRIEVED_ACTION, ValueState, ValueView, VersionsState,
};
use crate::keyboard_test_support::{connected_app_state, init_keyboard_runtime};
use dbflux_app::keymap::Command;
use dbflux_core::observability::{EventCategory, EventRecord, EventSink, EventSinkError};
use dbflux_core::secrecy::ExposeSecret;
use dbflux_core::{
    Connection, DbError, DbKind, FormattedError, MutationPolicy, ReadOnlyReason, SecretDetails,
    SecretListPage, SecretListRequest, SecretPayload, SecretPutRequest, SecretRotation,
    SecretStoreCapabilities, SecretStoreDescriptor, SecretStoreSource, SecretSummary, SecretValue,
    SecretVersionStage,
};
use dbflux_test_support::fake_driver::{
    FAKE_KMS_DENIED_MESSAGE, FAKE_PARAMETER_STORE_ID, FAKE_SECRET_BINARY, FAKE_SECRET_PASSWORD,
    FAKE_SECRET_PLAINTEXT, FAKE_SECRETS_STORE_ID, FakeDriver,
};
use dbflux_ui_base::app_state_entity::AppStateGlobal;
use dbflux_ui_base::{AppStateChanged, AppStateEntity};
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use super::RegionListState;

pub(crate) const KEY_VALUE_SECRET: &str = "prod/api/credentials";
const MANAGED_SECRET: &str = "prod/db/master";
const PENDING_DELETION_SECRET: &str = "prod/deploy/legacy-token";
pub(crate) const PLAINTEXT_SECRET: &str = "prod/notify/webhook-token";
const BINARY_SECRET: &str = "prod/tls/signing-key";
const DENIED_PARAMETER: &str = "/app/prod/db/password";
/// A `.env` file a user stored in a secret: the line breaks are the point.
const MULTILINE_SECRET: &str = "PORT=3000\nENV=prod\nDATABASE_URL=postgres://app@db:5432/app";
/// JSON that does not open as key/value, because an object and an array are
/// not scalar values. Its keys are in the order pretty-printing keeps.
const NESTED_JSON_SECRET: &str =
    r#"{"database":{"host":"db.internal","port":5432},"flags":["a,b","c"]}"#;

struct Opened<'a> {
    doc: Entity<SecretStoreDocument>,
    window: &'a mut VisualTestContext,
    driver: FakeDriver,
    app_state: Entity<AppStateEntity>,
}

fn open<'a>(cx: &'a mut TestAppContext, store_id: &str, path: Option<&str>) -> Opened<'a> {
    open_with(cx, FakeDriver::new(DbKind::AwsSecrets), store_id, path)
}

fn open_with<'a>(
    cx: &'a mut TestAppContext,
    driver: FakeDriver,
    store_id: &str,
    path: Option<&str>,
) -> Opened<'a> {
    init_keyboard_runtime(cx);

    let (app_state, profile_id) = connected_app_state(cx, &driver, "prod-aws");

    // `report_error` reaches the error seam through the `AppStateGlobal`
    // registration; without it a reported failure is invisible to these
    // tests.
    cx.update(|cx| {
        cx.set_global(AppStateGlobal {
            entity: app_state.clone(),
        });
    });

    let store_id = store_id.to_string();
    let path = path.map(str::to_string);
    let state = app_state.clone();

    let (doc, window) = cx.add_window_view(move |window, cx| {
        SecretStoreDocument::new(profile_id, store_id, path, state, window, cx)
    });
    window.run_until_parked();

    // The view-level tests exercise the editor where the harness mounts the
    // view; the pane's hand-over to the secret's tab is tested apart.
    window.update(|_, cx| {
        let view = doc.read(cx).view.clone();
        view.update(cx, |view, _| view.set_edit_in_place(true));
    });

    Opened {
        doc,
        window,
        driver,
        app_state,
    }
}

impl Opened<'_> {
    fn update<R>(
        &mut self,
        f: impl FnOnce(&mut SecretStoreDocument, &mut gpui::Context<SecretStoreDocument>) -> R,
    ) -> R {
        let doc = self.doc.clone();
        let result = self.window.update(|_, cx| doc.update(cx, f));
        self.window.run_until_parked();
        result
    }

    fn read<R>(&mut self, f: impl FnOnce(&SecretStoreDocument) -> R) -> R {
        let doc = self.doc.clone();
        self.window.update(|_, cx| f(doc.read(cx)))
    }

    /// Reads the document with the app it lives in, for what needs both.
    fn read_cx<R>(&mut self, f: impl FnOnce(&SecretStoreDocument, &gpui::App) -> R) -> R {
        let doc = self.doc.clone();
        self.window.update(|_, cx| f(doc.read(cx), cx))
    }

    /// Reads the view of the selected secret, which owns what the document
    /// used to hold itself: details, value, history, editor and dialog.
    fn read_view<R>(&mut self, f: impl FnOnce(&SecretView) -> R) -> R {
        let doc = self.doc.clone();
        self.window.update(|_, cx| {
            let view = doc.read(cx).view.clone();
            f(view.read(cx))
        })
    }

    /// Updates the view of the selected secret, then lets the window settle.
    fn update_view<R>(
        &mut self,
        f: impl FnOnce(&mut SecretView, &mut gpui::Context<SecretView>) -> R,
    ) -> R {
        let doc = self.doc.clone();
        let result = self.window.update(|_, cx| {
            let view = doc.read(cx).view.clone();
            view.update(cx, f)
        });
        self.window.run_until_parked();
        result
    }

    /// Shows `text` as the retrieved value, the way a store returning a
    /// multi-line secret would.
    fn show_value(&mut self, text: &str) {
        let value = RetrievedValue::new(&SecretValue::text(None, text.to_string()));
        self.update_view(|view, cx| {
            view.value = ValueState::Loaded(Box::new(value));
            // As if retrieved from the session that is live now.
            view.value_session = view.connection(cx);
            cx.notify();
        });
    }

    /// The rendered-frame path of every element the window drew.
    fn rendered_paths(&mut self) -> Vec<String> {
        let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(self.window);
        let frame = capture.frame(self.window);

        frame
            .nodes()
            .map(|(_, node)| node.path().to_string())
            .collect()
    }

    fn clipboard(&mut self) -> Option<String> {
        self.window
            .update(|_, cx| cx.read_from_clipboard().and_then(|item| item.text()))
    }

    fn select(&mut self, name: &str) {
        let row = SecretRowKey::Secret(name.to_string());
        self.update(|doc, cx| doc.select_row(row, cx));
    }

    fn command(&mut self, command: Command) -> bool {
        let doc = self.doc.clone();
        let handled = self.window.update(|window, cx| {
            doc.update(cx, |doc, cx| doc.dispatch_command(command, window, cx))
        });
        self.window.run_until_parked();
        handled
    }

    fn load_everything(&mut self) {
        while self.read(|doc| doc.listing().has_more()) {
            self.update(|doc, cx| doc.load_more(cx));
        }
    }

    fn entry_names(&mut self) -> Vec<String> {
        self.read(|doc| {
            doc.listing()
                .entries
                .iter()
                .map(|entry| entry.name.clone())
                .collect()
        })
    }

    fn audit_events(&mut self) -> Vec<String> {
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
                .iter()
                .map(|event| format!("{event:?}"))
                .collect()
        })
    }

    /// Every label, value, description and text of the rendered frame.
    fn rendered_text(&mut self) -> String {
        let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(self.window);
        let frame = capture.frame(self.window);

        frame
            .nodes()
            .map(|(_, node)| {
                let accessible = frame.accessibility_node(node);
                [
                    accessible.and_then(|node| node.label()).unwrap_or_default(),
                    accessible.and_then(|node| node.value()).unwrap_or_default(),
                    accessible
                        .and_then(|node| node.description())
                        .unwrap_or_default(),
                    node.content_text(),
                ]
                .join(" | ")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The accessibility update the frame produces, read the way the
    /// automation bridge reads it: every node of the update, hidden or not,
    /// with its label, value, description and rendered text.
    fn accessibility_update_text(&mut self) -> String {
        let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(self.window);
        let frame = capture.frame(self.window);

        frame
            .nodes()
            .map(|(_, node)| {
                let accessible = frame.accessibility_node(node);
                [
                    accessible.and_then(|node| node.label()).unwrap_or_default(),
                    accessible.and_then(|node| node.value()).unwrap_or_default(),
                    accessible
                        .and_then(|node| node.description())
                        .unwrap_or_default(),
                    node.content_text(),
                ]
                .join(" | ")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

// -- Listing ---------------------------------------------------------------

/// Pages append, entries already listed are not repeated, sub-paths a later
/// page repeats are merged, and a page of a replaced load is dropped.
#[test]
fn listing_pages_append_and_merge_their_sub_paths() {
    let mut listing = SecretListing::default();

    let first = listing.begin(true);
    assert!(listing.apply_page(
        first,
        SecretListPage {
            entries: vec![SecretSummary::new("/app/a")],
            child_paths: vec!["/app/db/".to_string()],
            next_page_token: Some("2".to_string()),
        }
    ));

    let second = listing.begin(false);
    assert!(listing.apply_page(
        second,
        SecretListPage {
            entries: vec![SecretSummary::new("/app/a"), SecretSummary::new("/app/b")],
            child_paths: vec!["/app/db/".to_string(), "/app/flags/".to_string()],
            next_page_token: None,
        }
    ));

    assert!(
        !listing.apply_page(first, SecretListPage::default()),
        "stale page"
    );

    let names: Vec<&str> = listing
        .entries
        .iter()
        .map(|entry| entry.name.as_str())
        .collect();
    assert_eq!(names, vec!["/app/a", "/app/b"]);
    assert_eq!(listing.child_paths, vec!["/app/db/", "/app/flags/"]);
    assert!(!listing.has_more());
}

#[test]
fn paths_split_into_navigable_segments() {
    assert_eq!(parent_path("/app/prod/"), Some("/app/".to_string()));
    assert_eq!(parent_path("/app/"), None);
    assert_eq!(
        path_segments("/app/prod/"),
        vec![
            ("app".to_string(), "/app/".to_string()),
            ("prod".to_string(), "/app/prod/".to_string()),
        ]
    );
    assert_eq!(child_path_label("/app/prod/db/"), "db/");
}

/// Opening a flat store lists its first page without reading a value, and
/// the next page appends to it with the store's page token.
#[gpui::test]
fn the_listing_loads_its_following_pages_on_its_own(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);

    assert_eq!(opened.entry_names().len(), 6);
    assert!(!opened.read(|doc| doc.listing().has_more()));
    assert!(!opened.read(|doc| doc.rows().contains(&SecretRowKey::LoadMore)));

    let calls = opened.driver.secret_store_calls();
    let tokens: Vec<Option<String>> = calls
        .list_requests
        .iter()
        .map(|(_, request)| request.page_token.clone())
        .collect();
    assert_eq!(tokens, vec![None, Some("4".to_string())]);
    assert!(
        calls.value_requests.is_empty(),
        "{:?}",
        calls.value_requests
    );
}

/// Every page request asks for the largest page the store will give, instead
/// of leaving the size to the service: the service default is what made a
/// listing of a few hundred secrets look like it ended after twenty.
#[gpui::test]
fn every_page_request_asks_for_the_largest_page(cx: &mut TestAppContext) {
    let opened = open(cx, FAKE_SECRETS_STORE_ID, None);

    let calls = opened.driver.secret_store_calls();
    assert!(!calls.list_requests.is_empty());

    for (store, request) in &calls.list_requests {
        assert_eq!(
            request.page_size,
            Some(LARGEST_PAGE_ENTRIES),
            "{store} page request left the page size to the service"
        );
    }
}

/// The continuation is bounded: past the limit the manual row carries the
/// rest, and a listing with nothing left never continues.
#[test]
fn the_continuation_stops_at_the_entry_limit() {
    assert!(keeps_loading(0, true));
    assert!(keeps_loading(AUTO_CONTINUE_ENTRY_LIMIT - 1, true));
    assert!(!keeps_loading(AUTO_CONTINUE_ENTRY_LIMIT, true));
    assert!(!keeps_loading(0, false));
}

/// The search runs on the store: the filter goes out as `name_filter` and
/// the listing starts over.
#[gpui::test]
fn the_search_sends_the_name_filter_to_the_store(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);

    opened.update(|doc, cx| doc.apply_name_filter("staging/".to_string(), cx));

    assert_eq!(opened.entry_names(), vec!["staging/api/credentials"]);

    let calls = opened.driver.secret_store_calls();
    let last = calls
        .list_requests
        .last()
        .map(|(_, request)| request.clone());
    assert_eq!(
        last.and_then(|request| request.name_filter),
        Some("staging/".to_string())
    );
}

/// A hierarchical store lists one level with its sub-paths first, and the
/// Flat mode lists every entry below the path in one recursive listing.
#[gpui::test]
fn a_hierarchical_store_browses_levels_and_lists_flat(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));
    opened.load_everything();

    let rows = opened.read(|doc| doc.rows());
    assert_eq!(
        rows,
        vec![
            SecretRowKey::Path("/app/prod/db/".to_string()),
            SecretRowKey::Path("/app/prod/feature-flags/".to_string()),
            SecretRowKey::Secret("/app/prod/region".to_string()),
        ]
    );

    let first = opened
        .driver
        .secret_store_calls()
        .list_requests
        .first()
        .map(|(_, request)| (request.path.clone(), request.recursive));
    assert_eq!(first, Some((Some("/app/prod/".to_string()), false)));

    assert!(opened.command(Command::CycleDocumentView));
    opened.load_everything();

    assert_eq!(opened.read(|doc| doc.path_mode()), PathMode::Flat);
    assert!(opened.read(|doc| doc.listing().child_paths.is_empty()));
    assert_eq!(opened.entry_names().len(), 5);

    let recursive = opened
        .driver
        .secret_store_calls()
        .list_requests
        .last()
        .map(|(_, request)| request.recursive);
    assert_eq!(recursive, Some(true));

    assert!(opened.command(Command::CycleDocumentView));
    assert!(opened.command(Command::ColumnLeft));
    assert_eq!(
        opened.read(|doc| doc.path().map(str::to_string)),
        Some("/app/".to_string())
    );
}

// -- Details ---------------------------------------------------------------

/// Selecting a secret describes it; neither the listing nor the describe
/// reads its value.
#[gpui::test]
fn selecting_a_secret_describes_it_without_reading_its_value(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);

    opened.select(KEY_VALUE_SECRET);

    let resource_id = opened.read_view(|view| match view.details() {
        DetailsState::Loaded(details) => details.resource_id.clone(),
        other => panic!("details not loaded: {other:?}"),
    });
    assert_eq!(
        resource_id,
        Some(format!("arn:fake:secret:{KEY_VALUE_SECRET}"))
    );

    let calls = opened.driver.secret_store_calls();
    assert_eq!(
        calls.described,
        vec![(
            FAKE_SECRETS_STORE_ID.to_string(),
            KEY_VALUE_SECRET.to_string()
        )]
    );
    assert!(calls.value_requests.is_empty());
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::NotLoaded)));
}

// -- Value -----------------------------------------------------------------

/// Retrieve value reads the value once, shows a key/value secret as its
/// pairs, and records one audit event that carries no part of the value.
#[gpui::test]
fn retrieve_value_reads_once_and_audits_without_the_value(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);

    assert!(opened.command(Command::ToggleValuePanel));

    let keys = opened.read_view(|view| match view.value() {
        ValueState::Loaded(value) => {
            assert_eq!(value.view, ValueView::KeyValue);
            value
                .fields()
                .iter()
                .map(|(key, _)| key.map(str::to_string))
                .collect::<Vec<_>>()
        }
        other => panic!("value not loaded: {other:?}"),
    });
    assert_eq!(
        keys,
        vec![
            Some("username".to_string()),
            Some("password".to_string()),
            Some("port".to_string())
        ]
    );

    assert_eq!(
        opened.driver.secret_store_calls().value_requests,
        vec![(
            FAKE_SECRETS_STORE_ID.to_string(),
            KEY_VALUE_SECRET.to_string()
        )]
    );

    let events = opened.audit_events();
    assert_eq!(events.len(), 1, "{events:?}");
    let event = &events[0];
    assert!(event.contains(VALUE_RETRIEVED_ACTION), "{event}");
    assert!(event.contains(FAKE_SECRETS_STORE_ID), "{event}");
    assert!(event.contains(KEY_VALUE_SECRET), "{event}");
    assert!(!event.contains(FAKE_SECRET_PASSWORD), "{event}");
    assert!(!event.contains("api-user"), "{event}");
}

/// The audit event names the store, the secret and the version, and its
/// fields and `details_json` hold no value, on success and on failure.
#[test]
fn the_retrieval_event_never_carries_the_value() {
    let profile_id = Uuid::from_u128(7);
    let target = RetrievalTarget {
        profile_id,
        driver_id: "aws_secrets",
        store_id: FAKE_SECRETS_STORE_ID,
        store_label: "Secrets Manager",
        name: KEY_VALUE_SECRET,
    };

    let success = value_retrieval_event(&target, Some("v-current"), None);
    assert_eq!(
        success.category,
        dbflux_core::observability::EventCategory::SecretStore
    );
    assert_eq!(success.action, VALUE_RETRIEVED_ACTION);
    assert_eq!(success.object_type.as_deref(), Some(FAKE_SECRETS_STORE_ID));
    assert_eq!(success.object_id.as_deref(), Some(KEY_VALUE_SECRET));
    assert_eq!(success.connection_id, Some(profile_id.to_string()));
    assert_eq!(
        success.details_json.as_deref(),
        Some(r#"{"version":"v-current"}"#)
    );
    dbflux_audit::AuditService::validate_event(&success).expect("valid event");

    let correlation_id = Uuid::from_u128(9);
    let failure = ValueFailure {
        message: FAKE_KMS_DENIED_MESSAGE.to_string(),
        correlation_id,
        decrypt_denied: true,
    };
    let failed = value_retrieval_event(&target, None, Some(&failure));
    assert_eq!(failed.action, VALUE_RETRIEVE_FAILED_ACTION);
    assert_eq!(failed.details_json, None);
    assert_eq!(failed.correlation_id, Some(correlation_id.to_string()));
    dbflux_audit::AuditService::validate_event(&failed).expect("valid event");

    for event in [success, failed] {
        let serialized = serde_json::to_string(&event).expect("serialize event");
        assert!(!serialized.contains(FAKE_SECRET_PASSWORD), "{serialized}");
    }
}

/// A one-level JSON object opens as key/value pairs in source order; nested
/// JSON and other text open as plaintext; a binary value opens as base64.
#[test]
fn each_value_shape_chooses_its_view() {
    let key_value = ValueContent::from_value(&SecretValue::text(
        None,
        format!(r#"{{"user":"a","password":"{FAKE_SECRET_PASSWORD}","port":5432}}"#),
    ));
    match &key_value {
        ValueContent::KeyValue { pairs, .. } => {
            let keys: Vec<&str> = pairs.iter().map(|(key, _)| key.as_str()).collect();
            assert_eq!(keys, vec!["user", "password", "port"]);
            assert_eq!(pairs[2].1.expose_secret(), "5432");
        }
        other => panic!("expected key/value, got {other:?}"),
    }

    let nested = ValueContent::from_value(&SecretValue::text(None, r#"{"a":{"b":1}}"#));
    assert!(matches!(nested, ValueContent::Plaintext(_)));

    let plaintext = ValueContent::from_value(&SecretValue::text(None, FAKE_SECRET_PLAINTEXT));
    assert!(matches!(plaintext, ValueContent::Plaintext(_)));

    match ValueContent::from_value(&SecretValue::binary(None, FAKE_SECRET_BINARY.to_vec())) {
        ValueContent::Binary { base64, size } => {
            assert_eq!(base64.expose_secret(), "3q2+7wBC");
            assert_eq!(size, FAKE_SECRET_BINARY.len());
        }
        other => panic!("expected binary, got {other:?}"),
    }
}

#[test]
fn base64_follows_rfc_4648() {
    for (input, expected) in [
        ("", ""),
        ("f", "Zg=="),
        ("fo", "Zm8="),
        ("foo", "Zm9v"),
        ("foob", "Zm9vYg=="),
        ("fooba", "Zm9vYmE="),
        ("foobar", "Zm9vYmFy"),
    ] {
        assert_eq!(encode_base64(input.as_bytes()), expected, "{input}");
    }
}

/// A plaintext secret has no key/value view, and a binary one is shown as
/// base64.
#[gpui::test]
fn plaintext_and_binary_secrets_open_in_their_views(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();

    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.update_view(|view, cx| view.set_value_view(ValueView::KeyValue, cx));

    opened.read_view(|view| match view.value() {
        ValueState::Loaded(value) => {
            assert_eq!(value.view, ValueView::Plaintext);
            assert!(!value.content.has_key_value_view());
            assert_eq!(value.fields()[0].1.expose_secret(), FAKE_SECRET_PLAINTEXT);
        }
        other => panic!("value not loaded: {other:?}"),
    });

    opened.select(BINARY_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    opened.read_view(|view| match view.value() {
        ValueState::Loaded(value) => {
            assert!(value.content.is_binary());
            assert_eq!(value.fields()[0].1.expose_secret(), "3q2+7wBC");
        }
        other => panic!("value not loaded: {other:?}"),
    });
}

/// A secret another service manages says so and stays retrievable.
#[gpui::test]
fn a_managed_secret_is_read_only_and_says_why(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(MANAGED_SECRET);

    let text = opened.rendered_text();
    assert!(
        text.contains(&dbflux_i18n::t!(
            "document.secret_store.managed.body",
            name = MANAGED_SECRET,
            service = "rds"
        )),
        "{text}"
    );
    assert!(opened.read_view(|view| view.can_retrieve_value()));
}

/// The value of a secret whose deletion is pending cannot be retrieved: the
/// action is disabled and the key does nothing.
#[gpui::test]
fn a_pending_deletion_disables_retrieval(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PENDING_DELETION_SECRET);

    assert!(!opened.read_view(|view| view.can_retrieve_value()));

    let enabled = opened.window.update(|_, cx| {
        opened
            .doc
            .read(cx)
            .pane_actions(&opened.doc, cx)
            .into_iter()
            .find(|action| action.id.as_ref() == "secret-retrieve-value")
            .map(|action| action.enabled)
    });
    assert_eq!(enabled, Some(false));

    opened.command(Command::ToggleValuePanel);

    assert!(opened.driver.secret_store_calls().value_requests.is_empty());
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::NotLoaded)));
}

/// A value the store refuses to decrypt opens the error state with the
/// store's message and the audit correlation id, never an empty value, and
/// the failure is audited.
#[gpui::test]
fn a_denied_decryption_shows_the_error_and_never_an_empty_value(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    opened.load_everything();
    opened.select(DENIED_PARAMETER);

    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    let correlation_id = opened.read_view(|view| match view.value() {
        ValueState::Failed(failure) => {
            assert!(failure.decrypt_denied);
            assert!(
                failure.message.contains(FAKE_KMS_DENIED_MESSAGE),
                "{}",
                failure.message
            );
            failure.correlation_id
        }
        other => panic!("expected the error state, got {other:?}"),
    });

    let events = opened.audit_events();
    assert_eq!(events.len(), 1, "{events:?}");
    assert!(
        events[0].contains(VALUE_RETRIEVE_FAILED_ACTION),
        "{}",
        events[0]
    );
    assert!(
        events[0].contains(&correlation_id.to_string()),
        "{}",
        events[0]
    );

    let text = opened.rendered_text();
    assert!(
        text.contains(&dbflux_i18n::t!(
            "document.secret_store.value.failed_decrypt"
        )),
        "{text}"
    );
}

/// The retrieved value is dropped when another row is selected and when the
/// listing is refreshed.
#[gpui::test]
fn the_value_is_dropped_on_selection_change_and_refresh(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);

    opened.select(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::Loaded(_))));

    opened.select(MANAGED_SECRET);
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::NotLoaded)));
    assert!(opened.read_view(|view| view.value_inputs.is_empty()));

    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::Loaded(_))));

    assert!(opened.command(Command::RefreshSchema));
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::NotLoaded)));
    assert!(opened.read_view(|view| view.value_inputs.is_empty()));
}

/// A masked or revealed value never reaches the accessibility tree that UI
/// automation reads: every value field is a secret-marked input.
#[gpui::test]
fn values_never_reach_the_accessibility_tree(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();

    for name in [KEY_VALUE_SECRET, PLAINTEXT_SECRET] {
        opened.select(name);
        opened.update_view(|view, cx| view.retrieve_selected_value(cx));

        let masked = opened.rendered_text();
        assert!(masked.contains("secret-value-field-0") || !masked.is_empty());

        assert!(opened.command(Command::ExpandCollapse));
        assert!(opened.read_view(|view| match view.value() {
            ValueState::Loaded(value) => value.is_revealed(0),
            _ => false,
        }));

        let revealed = opened.rendered_text();

        for secret in [FAKE_SECRET_PASSWORD, "api-user", FAKE_SECRET_PLAINTEXT] {
            assert!(
                !masked.contains(secret),
                "{name}: masked frame holds {secret}"
            );
            assert!(
                !revealed.contains(secret),
                "{name}: revealed frame holds {secret}"
            );
        }
    }
}

#[test]
fn the_loaded_count_agrees_with_its_number() {
    assert_eq!(super::render::loaded_label(1), "1 secret loaded");
    assert_eq!(super::render::loaded_label(27), "27 secrets loaded");
    assert_eq!(super::render::loaded_label(0), "0 secrets loaded");
}

fn rotation(enabled: bool, schedule: Option<&str>) -> SecretRotation {
    SecretRotation {
        enabled,
        schedule: schedule.map(str::to_string),
        window: None,
        function: None,
        last_rotated: None,
        next_rotation: None,
    }
}

/// The rotation column is a chip: success-toned with the schedule in words
/// while rotation is on, a short form for a cron schedule, neutral "Off"
/// otherwise.
#[test]
fn the_rotation_chip_reads_the_schedule_and_the_tone() {
    use super::list::rotation_chip;
    use dbflux_components::primitives::BadgeTone;

    let chip = rotation_chip(Some(&rotation(true, Some("rate(30 days)"))));
    assert_eq!(
        (chip.label.as_str(), chip.tone),
        ("Every 30 days", BadgeTone::Success)
    );

    let chip = rotation_chip(Some(&rotation(true, Some("cron(0 4 ? * SUN *)"))));
    assert_eq!(
        (chip.label.as_str(), chip.tone),
        ("On a schedule", BadgeTone::Success)
    );

    let chip = rotation_chip(Some(&rotation(true, None)));
    assert_eq!(
        (chip.label.as_str(), chip.tone),
        ("Enabled", BadgeTone::Success)
    );

    for off in [Some(rotation(false, Some("rate(30 days)"))), None] {
        let chip = rotation_chip(off.as_ref());
        assert_eq!(
            (chip.label.as_str(), chip.tone),
            ("Off", BadgeTone::Neutral)
        );
    }
}

/// "Oct 1, 03:12" in the current year; the year joins it otherwise.
#[test]
fn the_short_time_drops_the_year_only_in_the_current_year() {
    use super::list::short_time_label;
    use dbflux_core::chrono::{Local, TimeZone as _};

    let time = Local
        .with_ymd_and_hms(2026, 10, 1, 3, 12, 0)
        .single()
        .expect("a local time");

    assert_eq!(short_time_label(time, 2026), "Oct 1, 03:12");
    assert_eq!(short_time_label(time, 2027), "Oct 1, 2026, 03:12");
}

/// A hierarchical store counts paths and parameters under the listed path; a
/// flat listing has no paths; any other store counts secrets.
#[test]
fn the_loaded_summary_names_what_the_store_holds() {
    use super::list::loaded_summary;

    let summary = |hierarchical, mode, path: Option<&str>, paths, entries| {
        loaded_summary(hierarchical, mode, path, paths, entries)
    };

    assert_eq!(
        summary(true, PathMode::Tree, Some("/app/prod/shop/"), 2, 7),
        "2 paths · 7 parameters loaded under /app/prod/shop/"
    );
    assert_eq!(
        summary(true, PathMode::Tree, None, 1, 1),
        "1 path · 1 parameter loaded under /"
    );
    assert_eq!(
        summary(true, PathMode::Flat, Some("/app/"), 3, 5),
        "5 parameters under /app/"
    );
    assert_eq!(
        summary(false, PathMode::Tree, None, 0, 5),
        "5 secrets loaded"
    );
}

/// The footer of a Parameter Store level shows the counts the listing holds,
/// and its key hints name what each key does: Ctrl+C copies the ARN.
#[gpui::test]
fn the_footer_counts_paths_and_parameters_and_names_the_keys(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));

    let (paths, parameters) =
        opened.read(|doc| (doc.listing().child_paths.len(), doc.listing().entries.len()));
    assert!(
        paths > 0 && parameters > 0,
        "the level holds paths and parameters"
    );

    let text = opened.rendered_text();
    let expected = format!(
        "{paths} path{} · {parameters} parameter{} loaded under /app/prod/",
        if paths == 1 { "" } else { "s" },
        if parameters == 1 { "" } else { "s" }
    );
    assert!(text.contains(&expected), "{expected}: {text}");
    assert!(!text.contains("secrets loaded"), "{text}");
    assert!(text.contains("copy ARN"), "{text}");
    assert!(!text.contains("copy ID"), "{text}");
    assert!(text.contains("up"), "{text}");

    let status = opened
        .window
        .update(|_, cx| opened.doc.read(cx).loaded_summary());
    assert_eq!(status, expected);
}

/// A flat store keeps counting secrets, and its rows show the rotation chips
/// and the short change time.
#[gpui::test]
fn the_secrets_footer_and_rows_follow_the_design(cx: &mut TestAppContext) {
    use dbflux_core::chrono::{Datelike as _, Local, TimeZone as _, Utc};

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    let count = opened.read(|doc| doc.listing().entries.len());

    // The row drops the year in the current year, so the expected text must
    // use the year the render saw: render again if New Year fell in between.
    let (text, year) = loop {
        let before = Local::now().year();
        let text = opened.rendered_text();

        if Local::now().year() == before {
            break (text, before);
        }
    };

    assert!(text.contains(&loaded_label(count)), "{text}");
    assert!(text.contains("Every 30 days"), "{text}");
    assert!(text.contains("Off"), "{text}");
    assert!(text.contains("copy ARN"), "{text}");
    assert!(
        !text.contains("2026-09-20"),
        "the long form is gone: {text}"
    );

    // The fixture's change time, in local time and under the current-year
    // rule, in the row of its own secret.
    let changed = Utc
        .with_ymd_and_hms(2026, 9, 20, 3, 0, 0)
        .single()
        .expect("fixture time");
    let local = changed.with_timezone(&Local);
    let expected = if local.year() == year {
        local.format("%b %-d, %H:%M").to_string()
    } else {
        local.format("%b %-d, %Y, %H:%M").to_string()
    };
    assert!(
        text.lines()
            .any(|line| line.contains("prod/api/credentials") && line.contains(&expected)),
        "{expected} in the row of prod/api/credentials: {text}"
    );
}

/// The breadcrumb keeps its natural width when the document narrows: the
/// search field gives way first, so the path never reads "aws-pr…".
#[gpui::test]
fn the_breadcrumb_keeps_its_width_while_the_search_narrows(cx: &mut TestAppContext) {
    use dbflux_ui_base::keyboard_coverage::FrameCapture;

    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));

    let widths = |opened: &mut Opened<'_>| -> (f32, f32) {
        let capture = FrameCapture::observe(opened.window);
        let frame = capture.frame(opened.window);
        let width_of = |id: &str| {
            frame
                .nodes()
                .find(|(_, node)| node.id() == id)
                .map(|(_, node)| f32::from(node.bounds().size.width))
                .unwrap_or_else(|| panic!("no element {id}"))
        };

        (width_of("secret-identity"), width_of("secret-search"))
    };

    opened
        .window
        .simulate_resize(gpui::size(gpui::px(1500.0), gpui::px(800.0)));
    opened.window.run_until_parked();
    let (wide_breadcrumb, wide_search) = widths(&mut opened);

    opened
        .window
        .simulate_resize(gpui::size(gpui::px(1100.0), gpui::px(800.0)));
    opened.window.run_until_parked();
    let (narrow_breadcrumb, narrow_search) = widths(&mut opened);

    assert!(
        narrow_search < wide_search,
        "the search narrows: {wide_search} -> {narrow_search}"
    );
    assert_eq!(
        wide_breadcrumb, narrow_breadcrumb,
        "the breadcrumb does not narrow"
    );
}

/// Down to the document's minimum width (listing minimum plus details
/// minimum) Refresh stays on screen and keeps its width, as do the region
/// chip and the store identity. The search narrows first, to its minimum;
/// only then does the path give way.
#[gpui::test]
fn the_header_narrows_search_first_then_path_and_keeps_refresh(cx: &mut TestAppContext) {
    use dbflux_components::tokens::SecretStoreMetrics;
    use dbflux_ui_base::keyboard_coverage::FrameCapture;

    let mut opened = open_region_store(
        cx,
        FAKE_PARAMETER_STORE_ID,
        Some("/app/prod/"),
        &[OTHER_REGION, CURRENT_REGION],
        None,
    );

    // (width, right edge) of refresh, region, path and search.
    let measure = |opened: &mut Opened<'_>, width: f32| -> Vec<(f32, f32)> {
        opened
            .window
            .simulate_resize(gpui::size(gpui::px(width), gpui::px(800.0)));
        opened.window.run_until_parked();

        let capture = FrameCapture::observe(opened.window);
        let frame = capture.frame(opened.window);

        [
            "secret-refresh",
            "secret-region",
            "secret-path",
            "secret-search",
        ]
        .iter()
        .map(|id| {
            let suffix = format!(".{id}");
            let bounds = frame
                .nodes()
                .filter(|(_, node)| node.path().ends_with(&suffix))
                .min_by_key(|(_, node)| node.path().len())
                .map(|(_, node)| node.bounds())
                .unwrap_or_else(|| panic!("no element {id} at {width}"));

            (
                f32::from(bounds.size.width),
                f32::from(bounds.origin.x + bounds.size.width),
            )
        })
        .collect()
    };

    let rem = opened.window.update(|window, _| window.rem_size());
    let minimum = f32::from(SecretStoreMetrics::LISTING_MIN_WIDTH.to_pixels(rem))
        + f32::from(SecretStoreMetrics::DETAILS_MIN_WIDTH);
    let search_minimum = f32::from(SecretStoreMetrics::SEARCH_MIN_WIDTH.to_pixels(rem));

    let wide = measure(&mut opened, 1500.0);
    let mut previous_path = wide[2].0;
    let mut path_shrunk_above_search_minimum = false;
    let mut width = 1500.0;

    while width >= minimum {
        let sizes = measure(&mut opened, width);

        assert!(
            sizes[0].1 <= width + 0.5,
            "Refresh is pushed out of view at {width}: right edge {}",
            sizes[0].1
        );
        assert_eq!(sizes[0].0, wide[0].0, "Refresh keeps its width at {width}");
        assert_eq!(
            sizes[1].0, wide[1].0,
            "the region keeps its width at {width}"
        );

        if sizes[2].0 < previous_path - 0.5 && sizes[3].0 > search_minimum + 1.0 {
            path_shrunk_above_search_minimum = true;
        }
        previous_path = sizes[2].0;
        width -= 50.0;
    }

    assert!(
        !path_shrunk_above_search_minimum,
        "the path narrowed while the search was still above its minimum"
    );

    let narrowest = measure(&mut opened, minimum);
    assert!(
        narrowest[2].0 < wide[2].0,
        "the path gives way at the minimum width: {} -> {}",
        wide[2].0,
        narrowest[2].0
    );
    assert!(
        narrowest[3].0 <= search_minimum + 1.0,
        "the search reached its minimum: {}",
        narrowest[3].0
    );
}

/// The status bar counts what the footer counts, not secrets on a
/// hierarchical store.
#[gpui::test]
fn the_status_bar_uses_the_footer_counts(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));
    let expected = opened.read(|doc| doc.loaded_summary());
    assert!(expected.contains("loaded under /app/prod/"), "{expected}");

    let segments: Vec<String> = opened.window.update(|_, cx| {
        opened
            .doc
            .read(cx)
            .status_segments(cx)
            .into_iter()
            .map(|segment| segment.text.to_string())
            .collect()
    });

    assert!(segments.contains(&expected), "{segments:?}");
    assert!(
        !segments.iter().any(|text| text.contains("secrets loaded")),
        "{segments:?}"
    );
}

#[test]
fn a_numeric_version_reads_with_a_v_and_an_opaque_one_is_kept() {
    use super::render::version_label;

    assert_eq!(version_label(Some("3")), "v3");
    assert_eq!(version_label(Some("11")), "v11");
    assert_eq!(
        version_label(Some("a1b2c3d4-0000-4000-8000-000000000000")),
        "a1b2c3d4-0000-4000-8000-000000000000"
    );
    assert_eq!(version_label(None), "—");
}

/// The status bar names the listed entries as the footer does: parameters on
/// a hierarchical store, secrets otherwise.
#[test]
fn the_list_timing_uses_the_footer_noun() {
    use super::render::call_timing_label;
    use super::{CallTiming, StoreCall};

    let timing = |count, hierarchical| {
        call_timing_label(CallTiming {
            call: StoreCall::List {
                count,
                hierarchical,
            },
            millis: 13,
        })
    };

    assert_eq!(timing(4, true), "List · 4 parameters · 13 ms");
    assert_eq!(timing(1, true), "List · 1 parameter · 13 ms");
    assert_eq!(timing(4, false), "List · 4 secrets · 13 ms");
    assert_eq!(timing(1, false), "List · 1 secret · 13 ms");
}

/// The listing records which noun its call used.
#[gpui::test]
fn the_recorded_list_call_follows_the_store_kind(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));
    let call = opened.read(|doc| doc.last_call().map(|timing| timing.call));
    assert!(
        matches!(
            call,
            Some(StoreCall::List {
                hierarchical: true,
                ..
            })
        ),
        "{call:?}"
    );
    drop(opened);

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    let call = opened.read(|doc| doc.last_call().map(|timing| timing.call));
    assert!(
        matches!(
            call,
            Some(StoreCall::List {
                hierarchical: false,
                ..
            })
        ),
        "{call:?}"
    );
}

/// Window-relative (left, right, width) of the outermost element named `id`.
fn edges(opened: &mut Opened<'_>, id: &str) -> (f32, f32, f32) {
    use dbflux_ui_base::keyboard_coverage::FrameCapture;

    let capture = FrameCapture::observe(opened.window);
    let frame = capture.frame(opened.window);
    let suffix = format!(".{id}");
    let bounds = frame
        .nodes()
        .filter(|(_, node)| node.path().ends_with(&suffix))
        .min_by_key(|(_, node)| node.path().len())
        .map(|(_, node)| node.bounds())
        .unwrap_or_else(|| panic!("no element {id}"));

    let left = f32::from(bounds.origin.x);
    let width = f32::from(bounds.size.width);
    (left, left + width, width)
}

fn resize(opened: &mut Opened<'_>, width: f32) {
    opened
        .window
        .simulate_resize(gpui::size(gpui::px(width), gpui::px(800.0)));
    opened.window.run_until_parked();
}

fn minimum_document_width(opened: &mut Opened<'_>) -> f32 {
    use dbflux_components::tokens::SecretStoreMetrics;

    let rem = opened.window.update(|window, _| window.rem_size());
    f32::from(SecretStoreMetrics::LISTING_MIN_WIDTH.to_pixels(rem))
        + f32::from(SecretStoreMetrics::DETAILS_MIN_WIDTH)
}

/// With a long profile name in a narrow document the identity segment gives
/// way (its full text stays reachable), and the region chip stays inside the
/// header at its natural width.
#[gpui::test]
fn a_long_profile_name_never_pushes_the_region_out(cx: &mut TestAppContext) {
    let name = "production-payments-eu-west-primary-account-cluster-01";
    let mut opened = open_named_region_store(
        cx,
        name,
        FAKE_PARAMETER_STORE_ID,
        Some("/app/prod/"),
        &[OTHER_REGION, CURRENT_REGION],
        None,
    );

    resize(&mut opened, 1500.0);
    let wide_region = edges(&mut opened, "secret-region").2;
    let wide_identity = edges(&mut opened, "secret-identity").2;

    let minimum = minimum_document_width(&mut opened);
    let mut width = 1500.0;
    while width >= minimum {
        resize(&mut opened, width);
        let region = edges(&mut opened, "secret-region");
        let refresh = edges(&mut opened, "secret-refresh");

        assert_eq!(
            region.2, wide_region,
            "the region keeps its width at {width}"
        );
        assert!(
            region.1 <= refresh.0 + 0.5,
            "the region runs into Refresh at {width}: {} > {}",
            region.1,
            refresh.0
        );
        width -= 50.0;
    }

    resize(&mut opened, minimum);
    let identity = edges(&mut opened, "secret-identity");
    assert!(
        identity.2 < wide_identity,
        "the identity gives way at the minimum width"
    );
    assert!(
        identity.2 > 20.0, // guardrail-allow: test threshold, not spacing
        "the identity keeps its icon and a few characters: {}",
        identity.2
    );
    let refresh = edges(&mut opened, "secret-refresh");
    assert!(
        refresh.1 <= minimum + 0.5,
        "Refresh stays inside the document"
    );
    assert!(
        opened.rendered_text().contains(name),
        "the full name stays reachable: {}",
        opened.rendered_text()
    );
}

/// Hovers the element `id` and returns the text of the tooltip it draws.
fn hover_tooltip(opened: &mut Opened<'_>, id: &str) -> String {
    use dbflux_ui_base::keyboard_coverage::FrameCapture;

    let bounds = opened.element_bounds(id);
    let capture = FrameCapture::observe(opened.window);

    opened
        .window
        .simulate_mouse_move(bounds.center(), None, gpui::Modifiers::default());
    opened.window.run_until_parked();
    opened
        .window
        .simulate_mouse_move(bounds.center(), None, gpui::Modifiers::default());
    opened
        .window
        .executor()
        .advance_clock(std::time::Duration::from_millis(600));
    opened.window.run_until_parked();

    let frame = capture.frame(opened.window);

    frame
        .nodes()
        .find(|(_, node)| node.id() == "tooltip-popup")
        .map(|(_, node)| node.content_text().to_string())
        .unwrap_or_else(|| panic!("hovering `{id}` draws a tooltip"))
}

/// Pointer users read what the clipped identity and the clipped footer
/// summary hide from the tooltip, not only from the accessible name.
#[gpui::test]
fn clipped_identity_and_footer_summary_show_their_full_text_on_hover(cx: &mut TestAppContext) {
    let name = "production-payments-eu-west-primary-account-cluster-01";
    let mut opened = open_named_region_store(
        cx,
        name,
        FAKE_PARAMETER_STORE_ID,
        Some("/app/prod/"),
        &[OTHER_REGION, CURRENT_REGION],
        None,
    );

    resize(&mut opened, 1500.0);
    let wide_identity = edges(&mut opened, "secret-identity").2;
    let wide_summary = edges(&mut opened, "secret-footer-summary").2;

    let minimum = minimum_document_width(&mut opened);
    resize(&mut opened, minimum);
    assert!(edges(&mut opened, "secret-identity").2 < wide_identity);
    let summary_width = edges(&mut opened, "secret-footer-summary").2;
    assert!(summary_width < wide_summary);
    assert!(
        summary_width > 20.0, // guardrail-allow: test threshold, not spacing
        "the summary keeps its icon and a few characters: {summary_width}"
    );

    let identity = hover_tooltip(&mut opened, "secret-identity");
    assert!(identity.contains(name), "{identity}");
    assert!(identity.contains("Parameter Store"), "{identity}");

    let summary = opened.read(|doc| doc.loaded_summary());
    assert_eq!(hover_tooltip(&mut opened, "secret-footer-summary"), summary);
}

/// Refresh with unsaved edits starts neither the region discovery nor the
/// listing.
#[gpui::test]
fn refresh_under_unsaved_edits_starts_nothing(cx: &mut TestAppContext) {
    let mut opened = open_region_doc(
        cx,
        &[OTHER_REGION, CURRENT_REGION],
        Some("AccessDenied".to_string()),
    );
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);

    let regions_before = opened.driver.available_regions_calls();
    let lists_before = opened.driver.secret_store_calls().list_requests.len();

    opened.update(|doc, cx| doc.refresh(cx));
    opened.window.run_until_parked();

    assert_eq!(opened.driver.available_regions_calls(), regions_before);
    assert_eq!(
        opened.driver.secret_store_calls().list_requests.len(),
        lists_before
    );
    assert!(opened.read_view(|view| view.is_editing_value()));
}

/// A loaded but empty region list leaves nothing to pick, so the region
/// action stays disabled.
#[gpui::test]
fn the_region_action_needs_at_least_one_region(cx: &mut TestAppContext) {
    let mut empty = open_with_regions(cx, &[]);
    assert!(matches!(
        empty.read(|doc| doc.regions().clone()),
        RegionListState::Loaded(_)
    ));
    assert!(!empty.pane_action("secret-region").enabled);

    let mut listed = open_with_regions(cx, &[OTHER_REGION, CURRENT_REGION]);
    assert!(listed.pane_action("secret-region").enabled);
}

/// The footer's Copy ARN shortcut says why it cannot copy while the details
/// load or after they failed, and still copies once they are there.
#[gpui::test]
fn copy_arn_says_why_it_is_unavailable(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    opened.select(HISTORY_PARAMETER);

    let message = |opened: &mut Opened<'_>| {
        opened.command(Command::ResultsCopyCell);
        let reported = opened.reported_errors();
        reported
            .first()
            .map(|(_, summary)| summary.clone())
            .unwrap_or_default()
    };

    opened.update_view(|view, cx| {
        view.details = DetailsState::Loading;
        cx.notify();
    });
    assert_eq!(
        message(&mut opened),
        dbflux_i18n::t!("document.secret_store.context_menu.arn_loading")
    );

    opened.update_view(|view, cx| {
        view.details = DetailsState::Failed("describe refused".to_string());
        cx.notify();
    });
    assert_eq!(
        message(&mut opened),
        dbflux_i18n::t!(
            "document.secret_store.context_menu.arn_failed",
            error = "describe refused"
        )
    );
}

/// The rotation chip stays inside its column while the listing narrows, so it
/// never reaches Last changed.
#[gpui::test]
fn the_rotation_chip_stays_in_its_cell(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    let chip = "secret-rotation-chip-prod/api/credentials";
    let cell = "secret-rotation-cell-prod/api/credentials";

    let minimum = minimum_document_width(&mut opened);
    let mut width = 1500.0;
    let mut clipped = false;
    while width >= minimum {
        resize(&mut opened, width);
        let chip_edges = edges(&mut opened, chip);
        let cell_edges = edges(&mut opened, cell);

        assert!(
            chip_edges.1 <= cell_edges.1 + 0.5,
            "the chip runs past its cell at {width}: {} > {}",
            chip_edges.1,
            cell_edges.1
        );
        clipped |= cell_edges.2 < 60.0;
        width -= 50.0;
    }

    assert!(clipped, "the listing narrows enough to squeeze the cell");
}

/// The footer summary truncates while the key hints keep their width and
/// stay inside the listing.
#[gpui::test]
fn the_footer_summary_truncates_and_the_hints_keep_their_space(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));
    let hint = "secret-footer-hint-open";

    resize(&mut opened, 1500.0);
    let wide_hint = edges(&mut opened, hint).2;
    let wide_summary = edges(&mut opened, "secret-footer-summary").2;

    let minimum = minimum_document_width(&mut opened);
    let mut width = 1500.0;
    while width >= minimum {
        resize(&mut opened, width);
        let hint_edges = edges(&mut opened, hint);
        let summary = edges(&mut opened, "secret-footer-summary");

        assert_eq!(
            hint_edges.2, wide_hint,
            "the hint keeps its width at {width}"
        );
        assert!(
            summary.1 <= edges(&mut opened, "secret-footer-hint-open").0 + 0.5,
            "the summary runs into the hints at {width}"
        );
        assert!(
            hint_edges.1 <= width,
            "the hints leave the window at {width}"
        );
        width -= 50.0;
    }

    resize(&mut opened, minimum);
    assert!(
        edges(&mut opened, "secret-footer-summary").2 < wide_summary,
        "the summary gives way at the minimum width"
    );
    assert!(
        opened.rendered_text().contains("loaded under /app/prod/"),
        "the full summary stays reachable"
    );
}

/// At the minimum width every footer element, the summary and each key hint
/// that is drawn, ends inside the listing, and a hint is never cut: the ones
/// that do not fit are dropped, lowest priority first.
#[gpui::test]
fn the_footer_stays_inside_the_listing_at_the_minimum_width(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));
    let names = ["open", "up", "retrieve", "copy_arn", "filter"];
    let paths = |opened: &mut Opened<'_>| opened.rendered_paths();

    resize(&mut opened, 1500.0);
    let natural: Vec<f32> = names
        .iter()
        .map(|name| edges(&mut opened, &format!("secret-footer-hint-{name}")).2)
        .collect();

    let minimum = minimum_document_width(&mut opened);
    let mut width = 1500.0;
    while width >= minimum {
        resize(&mut opened, width);
        let listing = edges(&mut opened, "secret-listing").1;
        let drawn = paths(&mut opened);
        let present = |name: &str| {
            let id = format!("secret-footer-hint-{name}");
            drawn.iter().any(|path| path.ends_with(&id))
        };

        // Dropped lowest priority first: filter, then copy ARN, then up.
        assert!(
            present("filter") <= present("copy_arn"),
            "filter outlived copy ARN at {width}"
        );
        assert!(
            present("copy_arn") <= present("up"),
            "copy ARN outlived up at {width}"
        );

        for (name, natural) in names.iter().zip(&natural) {
            if !present(name) {
                continue;
            }

            let hint = edges(&mut opened, &format!("secret-footer-hint-{name}"));
            assert_eq!(hint.2, *natural, "{name} is cut at {width}");
            assert!(
                hint.1 <= listing + 0.5,
                "{name} overflows at {width}: {} > {listing}",
                hint.1
            );
        }

        let summary = edges(&mut opened, "secret-footer-summary");
        assert!(summary.1 <= listing + 0.5, "summary overflows at {width}");
        width -= 25.0;
    }

    assert!(
        !paths(&mut opened)
            .iter()
            .any(|path| path.ends_with("secret-footer-hint-filter")),
        "the lowest-priority hint is dropped at the minimum width"
    );

    for kept in ["open", "retrieve"] {
        let id = format!("secret-footer-hint-{kept}");
        assert!(
            paths(&mut opened).iter().any(|path| path.ends_with(&id)),
            "{kept} is always drawn"
        );
    }

    resize(&mut opened, 1500.0);
    for name in names {
        let id = format!("secret-footer-hint-{name}");
        assert!(
            paths(&mut opened).iter().any(|path| path.ends_with(&id)),
            "{name} returns when the room does"
        );
    }
}

/// A write landing after the connection is gone reloads the history on its
/// own: that is a disconnect, so it shows the inline not-connected state and
/// raises no toast, notification or audit row of its own.
#[gpui::test]
fn a_post_save_history_reload_without_a_connection_stays_silent(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);
    let generation = opened.read_view(|view| view.value_write_generation);

    let profile_id = opened.read(|doc| doc.profile_id);
    let app_state = opened.app_state.clone();
    opened.window.update(|_, cx| {
        app_state.update(cx, |state, _| state.connections_mut().remove(&profile_id));
    });
    opened.update_view(|view, _| view.missing_connection_reported = true);

    let written = SecretValue {
        version: Some("v-written".to_string()),
        payload: dbflux_core::SecretPayload::Text(EDITED_VALUE.into()),
    };
    let saved_on = opened.read_view(|view| view.value_session.clone().expect("a retrieved value"));
    let ((), events) = dbflux_ui_base::user_error::capture::capture_user_error_events(|| {
        opened.update_view(|view, cx| {
            view.apply_value_write(generation, PLAINTEXT_SECRET, Ok(written), &saved_on, cx)
        });
    });

    assert!(
        opened.reported_errors().is_empty(),
        "{:?}",
        opened.reported_errors()
    );
    assert_eq!(opened.toast_count(), 0);
    assert!(
        events.is_empty(),
        "no user_error event, so no row: {events:?}"
    );
    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::NotConnected)));
}

/// With the current version unknown, the shown version is named and can go
/// back, but the pane does not claim it is not the current one.
#[gpui::test]
fn an_unknown_current_version_is_not_called_older(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.activate_version(1, cx));
    opened.strip_reported_versions();
    opened.fail_history();

    let text = opened.rendered_text();
    assert!(text.contains("Showing version"), "{text}");
    assert!(text.contains("Back to current"), "{text}");
    assert!(!text.contains("not the current version"), "{text}");
}

/// A region list that loaded empty offers nothing to pick: the configured
/// region stays visible as a plain chip.
#[gpui::test]
fn an_empty_region_list_keeps_the_configured_region_chip(cx: &mut TestAppContext) {
    let mut opened = open_with_regions(cx, &[]);
    let paths = opened.rendered_paths();

    assert!(
        paths
            .iter()
            .any(|path| path.ends_with("secret-region-plain")),
        "{paths:?}"
    );
    assert!(opened.rendered_text().contains(CURRENT_REGION));
}

/// Hint widths cached under one typography are forgotten when it changes, so
/// a hint dropped at a large font is measured again at a smaller one.
#[test]
fn footer_hint_widths_are_remeasured_when_the_typography_changes() {
    use super::render::FooterFit;

    let fit = FooterFit::default();
    fit.track_typography(gpui::px(48.0));
    fit.remember_hint("filter", gpui::px(120.0));
    assert_eq!(fit.hint_width("filter"), Some(gpui::px(120.0)));

    fit.track_typography(gpui::px(48.0));
    assert_eq!(
        fit.hint_width("filter"),
        Some(gpui::px(120.0)),
        "unchanged typography keeps the measurements"
    );

    fit.track_typography(gpui::px(36.0));
    assert_eq!(fit.hint_width("filter"), None);
}

/// Refresh asks for the regions again after a failed discovery, and a
/// successful answer brings the picker back.
#[gpui::test]
fn refresh_retries_a_failed_region_discovery(cx: &mut TestAppContext) {
    let mut opened = open_region_doc(
        cx,
        &[OTHER_REGION, CURRENT_REGION],
        Some("AccessDenied".to_string()),
    );
    assert!(matches!(
        opened.read(|doc| doc.regions().clone()),
        RegionListState::Failed(_)
    ));
    assert_eq!(opened.driver.available_regions_calls(), 1);

    opened.driver.set_region_list_error(None);
    opened.update(|doc, cx| doc.refresh(cx));
    opened.window.run_until_parked();

    assert_eq!(opened.driver.available_regions_calls(), 2);
    assert!(matches!(
        opened.read(|doc| doc.regions().clone()),
        RegionListState::Loaded(_)
    ));

    use dbflux_ui_base::keyboard_coverage::{FrameCapture, clickable_elements};
    let capture = FrameCapture::observe(opened.window);
    let frame = capture.frame(opened.window);
    assert!(
        clickable_elements(&frame)
            .into_iter()
            .any(|element| element.path.ends_with("secret-region.dropdown-trigger")),
        "the picker is interactive again"
    );

    // A refresh with a loaded list does not ask again.
    opened.update(|doc, cx| doc.refresh(cx));
    assert_eq!(opened.driver.available_regions_calls(), 2);
}

#[gpui::test]
fn the_reveal_all_button_names_what_it_will_do(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();

    opened.select(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    let masked = opened.rendered_text();
    assert!(masked.contains("Reveal all"), "masked: {masked}");
    assert!(!masked.contains("Hide all"), "masked: {masked}");

    assert!(opened.command(Command::ExpandCollapse));

    let revealed = opened.rendered_text();
    assert!(revealed.contains("Hide all"), "revealed: {revealed}");
    assert!(!revealed.contains("Reveal all"), "revealed: {revealed}");
}

// -- Multi-line value viewer -----------------------------------------------

/// A `.env` stored in a secret keeps a line break per assignment: the value
/// opens in the viewer with one line each, in the order it was stored.
#[test]
fn a_multiline_value_opens_in_the_viewer_with_every_line() {
    let value = RetrievedValue::new(&SecretValue::text(None, MULTILINE_SECRET.to_string()));

    assert!(value.is_multiline());
    assert_eq!(
        value.viewer_lines(true),
        vec![
            "PORT=3000",
            "ENV=prod",
            "DATABASE_URL=postgres://app@db:5432/app",
        ]
    );
}

/// Nothing of the value is drawn before it is revealed: the viewer shows one
/// mask per value line, so the line count shows and the text never does.
#[test]
fn the_viewer_masks_one_line_per_value_line() {
    let value = RetrievedValue::new(&SecretValue::text(None, MULTILINE_SECRET.to_string()));

    let masked = value.viewer_lines(false);

    assert_eq!(masked.len(), MULTILINE_SECRET.lines().count());
    assert!(masked.iter().all(|line| line == MASK), "{masked:?}");
    assert!(!masked.join("\n").contains("PORT=3000"), "{masked:?}");
}

/// JSON that does not open as key/value — nested objects, arrays — is drawn
/// pretty-printed with two spaces, and Copy value still copies the text the
/// store returned.
#[test]
fn nested_json_is_drawn_pretty_printed() {
    let value = RetrievedValue::new(&SecretValue::text(None, NESTED_JSON_SECRET.to_string()));

    assert!(value.is_multiline());
    assert_eq!(
        value.viewer_lines(true),
        vec![
            "{",
            "  \"database\": {",
            "    \"host\": \"db.internal\",",
            "    \"port\": 5432",
            "  },",
            "  \"flags\": [",
            "    \"a,b\",",
            "    \"c\"",
            "  ]",
            "}",
        ]
    );
    assert_eq!(
        value.content.full_text().expose_secret(),
        NESTED_JSON_SECRET,
        "formatting is view only"
    );
}

/// A one-line value, a key/value secret and a binary one keep the field they
/// always had: the viewer is for text with more than one line only.
#[test]
fn values_that_fit_one_line_keep_their_field() {
    for text in [
        FAKE_SECRET_PLAINTEXT,
        // One-level JSON that opens as key/value, one line per pair.
        "{\n  \"user\": \"a\",\n  \"port\": 5432\n}",
    ] {
        let value = RetrievedValue::new(&SecretValue::text(None, text.to_string()));
        assert!(!value.is_multiline(), "{text}");
        assert!(value.viewer_lines(true).is_empty(), "{text}");
    }

    let binary = RetrievedValue::new(&SecretValue::binary(None, FAKE_SECRET_BINARY.to_vec()));
    assert!(!binary.is_multiline());
    assert!(binary.viewer_lines(true).is_empty());
}

/// The viewer draws one row per line, masked until revealed, and neither
/// state puts the text in the frame UI automation reads. Copy value copies
/// the stored text, formatting and all.
#[gpui::test]
fn the_viewer_draws_the_lines_and_never_leaks_the_value(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.show_value(MULTILINE_SECRET);

    let paths = opened.rendered_paths();
    assert!(
        paths
            .iter()
            .any(|path| path.ends_with("secret-value-viewer")),
        "{paths:?}"
    );
    let lines = MULTILINE_SECRET.lines().count();
    for index in 0..lines {
        assert!(
            paths
                .iter()
                .any(|path| path.ends_with(&format!("secret-value-viewer-line-{index}"))),
            "line {index} is missing: {paths:?}"
        );
    }

    let masked = opened.rendered_text();
    assert!(opened.command(Command::ExpandCollapse));
    let revealed = opened.rendered_text();

    for line in MULTILINE_SECRET.lines() {
        assert!(!masked.contains(line), "masked frame holds {line}");
        assert!(!revealed.contains(line), "revealed frame holds {line}");
    }

    opened.update_view(|view, cx| view.copy_full_value(cx));
    assert_eq!(opened.clipboard().as_deref(), Some(MULTILINE_SECRET));
}

/// The viewer keeps about fourteen lines visible and wraps a long line at the
/// pane width instead of clipping it, so the sections below it keep their
/// place.
#[gpui::test]
fn the_viewer_scrolls_after_about_fourteen_lines_and_wraps_long_ones(cx: &mut TestAppContext) {
    use dbflux_components::fonts;
    use dbflux_components::tokens::DocumentMetrics;

    /// Longer than the details pane: it has to wrap to be readable.
    const LONG_LINE: &str = "DATABASE_URL postgres app secret db.internal 5432 app sslmode require application_name dbflux connect_timeout ten keepalives_idle thirty pool_max twenty";
    let many_lines = (0..40)
        .map(|index| format!("KEY_{index}=value"))
        .collect::<Vec<_>>()
        .join("\n");

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.show_value(&format!("{LONG_LINE}\n{many_lines}"));
    assert!(opened.command(Command::ExpandCollapse), "reveal the value");

    let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(opened.window);
    let frame = capture.frame(opened.window);
    let bounds = |suffix: &str| {
        frame
            .nodes()
            .find(|(_, node)| node.path().ends_with(suffix))
            .map(|(_, node)| node.bounds().size.height)
    };

    let line_height = opened
        .window
        .update(|_, cx| fonts::ui_px(cx, DocumentMetrics::DETAIL_BLOCK_FONT))
        * DocumentMetrics::DETAIL_BLOCK_LINE_HEIGHT;

    let viewer = bounds("secret-value-viewer").expect("the viewer is rendered");
    let visible = line_height * super::view::details::VIEWER_VISIBLE_LINES;
    assert!(
        viewer >= visible - gpui::px(1.0),
        "the viewer keeps its lines visible: {viewer:?} at {line_height:?} per line"
    );
    assert!(
        viewer <= visible + DocumentMetrics::DETAIL_BLOCK_PADDING_Y * 2.0 + gpui::px(1.0),
        "the viewer is bounded to those lines: {viewer:?} at {line_height:?} per line"
    );

    let first_line = bounds("secret-value-viewer-line-0").expect("the first line is rendered");
    assert!(
        first_line > line_height * 2.0,
        "the long first line wraps instead of being clipped: {first_line:?}"
    );

    let last_line = bounds("secret-value-viewer-line-40").expect("the last line is rendered");
    assert!(
        (last_line - line_height).abs() < gpui::px(1.0),
        "a short line takes one row: {last_line:?} at {line_height:?}"
    );
}

/// The viewer scrolls: a wheel over the box moves its lines instead of the
/// details body, so a long value stays inside its own bounded box.
#[gpui::test]
fn the_viewer_scrolls_its_own_lines(cx: &mut TestAppContext) {
    let many_lines = (0..40)
        .map(|index| format!("KEY_{index}=value"))
        .collect::<Vec<_>>()
        .join("\n");

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.show_value(&many_lines);
    assert!(opened.command(Command::ExpandCollapse), "reveal the value");

    let bounds = |opened: &mut Opened<'_>, suffix: &str| {
        let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(opened.window);
        let frame = capture.frame(opened.window);

        frame
            .nodes()
            .find(|(_, node)| node.path().ends_with(suffix))
            .map(|(_, node)| node.bounds())
            .unwrap_or_else(|| panic!("{suffix} is rendered"))
    };

    let viewer = bounds(&mut opened, "secret-value-viewer");
    let before = bounds(&mut opened, "secret-value-viewer-line-0").origin.y;

    opened.window.simulate_event(gpui::ScrollWheelEvent {
        position: viewer.center(),
        delta: gpui::ScrollDelta::Pixels(gpui::point(gpui::px(0.0), gpui::px(-120.0))),
        ..Default::default()
    });
    opened.window.run_until_parked();

    let after = bounds(&mut opened, "secret-value-viewer-line-0").origin.y;
    assert!(
        after < before,
        "the wheel scrolls the viewer's own lines: {before:?} then {after:?}"
    );
}

/// A short value gets a box its own height: the fourteen lines are a bound,
/// not a reserved block that would push the sections below it down.
#[gpui::test]
fn a_short_value_takes_a_short_box(cx: &mut TestAppContext) {
    use dbflux_components::fonts;
    use dbflux_components::tokens::DocumentMetrics;

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.show_value("PORT=3000\nENV=prod");

    let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(opened.window);
    let frame = capture.frame(opened.window);
    let viewer = frame
        .nodes()
        .find(|(_, node)| node.path().ends_with("secret-value-viewer"))
        .map(|(_, node)| node.bounds().size.height)
        .expect("the viewer is rendered");

    let line_height = opened
        .window
        .update(|_, cx| fonts::ui_px(cx, DocumentMetrics::DETAIL_BLOCK_FONT))
        * DocumentMetrics::DETAIL_BLOCK_LINE_HEIGHT;

    assert!(
        (viewer - line_height * 2.0).abs() < gpui::px(1.0),
        "two lines take two rows: {viewer:?} at {line_height:?} per line"
    );
}

/// A pretty-printed JSON line longer than the pane wraps like any other line.
#[gpui::test]
fn a_long_pretty_printed_json_line_wraps(cx: &mut TestAppContext) {
    use dbflux_components::fonts;
    use dbflux_components::tokens::DocumentMetrics;

    let long_value = "postgres://app:secret@db.internal:5432/app sslmode require pool_max twenty";
    let secret = format!(r#"{{"target":{{"url":"{long_value}"}}}}"#);

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.show_value(&secret);
    assert!(opened.command(Command::ExpandCollapse), "reveal the value");

    let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(opened.window);
    let frame = capture.frame(opened.window);
    let height = |suffix: &str| {
        frame
            .nodes()
            .find(|(_, node)| node.path().ends_with(suffix))
            .map(|(_, node)| node.bounds().size.height)
    };

    let line_height = opened
        .window
        .update(|_, cx| fonts::ui_px(cx, DocumentMetrics::DETAIL_BLOCK_FONT))
        * DocumentMetrics::DETAIL_BLOCK_LINE_HEIGHT;

    // `{`, the nested object, the long `url` line, then the closers.
    let long_line = height("secret-value-viewer-line-2").expect("the long line is rendered");
    assert!(
        long_line > line_height * 2.0,
        "the long JSON line wraps: {long_line:?} at {line_height:?} per line"
    );
}

/// A single-line plaintext secret keeps the single-line field: no viewer, no
/// gutter, and the frame names the field it always named.
#[gpui::test]
fn a_single_line_plaintext_secret_keeps_its_single_line_field(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    let paths = opened.rendered_paths();

    assert!(
        paths
            .iter()
            .any(|path| path.ends_with("secret-value-field-0")),
        "{paths:?}"
    );
    assert!(
        !paths
            .iter()
            .any(|path| path.contains("secret-value-viewer")),
        "{paths:?}"
    );
}

// -- Value editor -----------------------------------------------------------

/// The text the editor of a saved value is opened with; a value a test
/// writes back, kept apart from the fixtures so a leak assertion can name it.
const EDITED_VALUE: &str = "rotated webhook token 77b1";

impl Opened<'_> {
    /// Opens the editor on the selected secret's loaded value.
    fn start_edit(&mut self) {
        let doc = self.doc.clone();
        self.window.update(|window, cx| {
            doc.update(cx, |doc, cx| {
                doc.view
                    .update(cx, |view, cx| view.start_value_edit(window, cx))
            })
        });
        self.window.run_until_parked();
    }

    /// The state of the open editor, if one is open.
    fn editing(&mut self) -> Option<(String, bool)> {
        self.read_view(|view| {
            let edit = view.value_edit.as_ref()?;
            Some((edit.original.expose_secret().to_string(), edit.saving))
        })
    }

    /// What the open editor's find panel holds: the search text and the
    /// replacement, as the editor component reports its search session.
    fn find_fields(&mut self) -> (String, String) {
        let editor = self.read_view(|view| {
            view.value_edit
                .as_ref()
                .expect("editor open")
                .editor
                .clone()
        });

        self.window.update(|_, cx| {
            let session = editor.read(cx).search_session().clone();
            (session.query, session.replacement)
        })
    }

    /// The text the open editor holds.
    fn editor_text(&mut self) -> String {
        let editor = self.read_view(|view| {
            view.value_edit
                .as_ref()
                .expect("editor open")
                .editor
                .clone()
        });
        self.window
            .update(|_, cx| editor.read(cx).value().to_string())
    }

    /// Replaces the text of the open editor.
    fn set_editor_text(&mut self, text: &str) {
        let editor = self.read_view(|view| {
            view.value_edit
                .as_ref()
                .expect("editor open")
                .editor
                .clone()
        });
        let text = text.to_string();
        self.window.update(|window, cx| {
            editor.update(cx, |state, cx| state.set_value(&text, window, cx));
        });
        self.window.run_until_parked();
    }

    /// Saves the open editor the way Save and the pane action do.
    fn save_edit(&mut self) {
        self.update_view(|view, cx| view.save_value_edit(cx));
    }

    /// Every error the user-facing seam reported this session, as
    /// `(correlation id, summary)`.
    fn reported_errors(&mut self) -> Vec<(uuid::Uuid, String)> {
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
                    NotificationKey::Error(id) => {
                        let error = state.notifications.error(*id)?;
                        Some((*id, error.summary.clone()))
                    }
                    _ => None,
                })
                .collect()
        })
    }

    /// How many toasts the stack holds.
    fn toast_count(&mut self) -> usize {
        self.window.update(|_, cx| {
            let host = cx
                .global::<dbflux_ui_base::toast::ToastGlobal>()
                .host
                .clone();
            host.read(cx).toast_count()
        })
    }

    /// The recorded audit events as records, for the fields a formatted
    /// dump would make awkward to read.
    /// The recorded audit events as DTOs, for the fields a formatted dump
    /// would make awkward to read.
    fn audit_records(&mut self) -> Vec<dbflux_storage::AuditEventDto> {
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
        })
    }

    /// The write requests the fake store received, as `(name, text)`.
    fn written_values(&mut self) -> Vec<(String, String)> {
        use dbflux_core::secrecy::ExposeSecret as _;

        self.driver
            .secret_store_calls()
            .put_requests
            .iter()
            .map(|(_, request)| match &request.payload {
                SecretPayload::Text(text) => {
                    (request.name.clone(), text.expose_secret().to_string())
                }
                SecretPayload::Binary(_) => (request.name.clone(), String::new()),
            })
            .collect()
    }
}

/// The editor opens with the stored text: the value the store returned, not
/// a formatted view of it, and Save, Discard and the editor are drawn.
#[gpui::test]
fn the_editor_opens_with_the_stored_text(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    assert!(!opened.read_view(|view| view.is_editing_value()));

    // The action that opens the editor is drawn while the value keeps its
    // presentation.
    assert!(opened.rendered_text().contains("Edit value"));

    opened.start_edit();

    assert_eq!(opened.editor_text(), FAKE_SECRET_PLAINTEXT);
    let (original, saving) = opened.editing().expect("the editor is open");
    assert_eq!(original, FAKE_SECRET_PLAINTEXT);
    assert!(!saving);

    let text = opened.rendered_text();
    assert!(text.contains("Save as new version"), "{text}");
    assert!(text.contains("Discard"), "{text}");

    let paths = opened.rendered_paths();
    assert!(
        paths
            .iter()
            .any(|path| path.ends_with("secret-value-editor")),
        "{paths:?}"
    );
    assert!(
        paths
            .iter()
            .any(|path| path.ends_with("secret-value-editor-save")),
        "{paths:?}"
    );
    assert!(
        paths
            .iter()
            .any(|path| path.ends_with("secret-value-editor-discard")),
        "{paths:?}"
    );
}

/// A save with nothing changed writes nothing: the store is not called, no
/// audit event is added and the editor stays open.
#[gpui::test]
fn an_unchanged_save_writes_nothing(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    let events_before = opened.audit_records().len();

    opened.save_edit();

    assert!(opened.driver.secret_store_calls().put_attempts.is_empty());
    assert_eq!(
        opened.audit_records().len(),
        events_before,
        "no write attempt"
    );
    assert!(opened.reported_errors().is_empty());
    assert!(
        opened.read_view(|view| view.is_editing_value()),
        "the editor stays open"
    );
}

/// The JSON gate follows the stored value, not the editor: a plaintext
/// value takes any edit, and only a value the store kept as JSON must stay
/// JSON.
#[gpui::test]
fn the_json_gate_applies_only_to_a_stored_json_value(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);

    // A plaintext value is written back exactly as edited, whatever it
    // holds — JSON syntax included.
    opened.show_value("host = db.internal # not JSON, braces }{ and all");
    opened.start_edit();
    opened.set_editor_text("{oops not json]");
    opened.save_edit();
    assert_eq!(
        opened.written_values(),
        vec![(PLAINTEXT_SECRET.to_string(), "{oops not json]".to_string())]
    );

    // A JSON value keeps the gate.
    opened.show_value(r#"["token-a","token-b"]"#);
    opened.start_edit();
    opened.set_editor_text("{oops not json]");
    opened.save_edit();
    assert_eq!(opened.written_values().len(), 1, "the JSON value is gated");
}

/// A changed save runs through the governed seam once, shows the value and
/// version the seam returned, closes the editor and refreshes the details,
/// so the version the pane reports is the one the store now holds.
#[gpui::test]
fn a_changed_save_writes_a_new_version_through_the_seam(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);

    opened.save_edit();

    assert_eq!(
        opened.written_values(),
        vec![(PLAINTEXT_SECRET.to_string(), EDITED_VALUE.to_string())]
    );

    let events = opened.audit_records();
    assert_eq!(events.len(), 2, "the retrieval and the write: {events:?}");
    assert_eq!(
        events[1].action.as_deref(),
        Some(SECRET_VALUE_WRITTEN_ACTION),
        "{events:?}"
    );
    assert!(
        events[1]
            .details_json
            .as_deref()
            .is_some_and(|json| json.contains("v-written-1")),
        "{events:?}"
    );

    opened.read_view(|view| {
        assert!(!view.is_editing_value(), "the editor closed");
        match &view.value {
            ValueState::Loaded(value) => {
                assert_eq!(value.version.as_deref(), Some("v-written-1"));
                assert_eq!(value.content.full_text().expose_secret(), EDITED_VALUE);
                assert!(!value.is_revealed(0), "the saved value is masked again");
            }
            other => panic!("value not loaded: {other:?}"),
        }
    });

    let described: Vec<String> = opened
        .driver
        .secret_store_calls()
        .described
        .iter()
        .map(|(_, name)| name.clone())
        .collect();
    assert_eq!(
        described,
        vec![PLAINTEXT_SECRET.to_string(), PLAINTEXT_SECRET.to_string()],
        "the details refresh after the write"
    );
}

/// A read-only connection refuses the save: the refusal reads as a decision,
/// the store is never called, and the editor stays open.
#[gpui::test]
fn a_read_only_connection_refuses_the_save_without_calling_the_store(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.set_policy(
        MutationPolicy::ReadOnly,
        Some(ReadOnlyReason::ProfileSetting),
    );
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);

    opened.save_edit();

    assert!(opened.driver.secret_store_calls().put_attempts.is_empty());
    let events = opened.audit_records();
    assert_eq!(events.len(), 2, "the retrieval and the refusal: {events:?}");
    assert_eq!(
        events[1].action.as_deref(),
        Some(SECRET_VALUE_WRITE_REFUSED_ACTION),
        "{events:?}"
    );

    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("read-only"), "{}", errors[0].1);

    assert!(
        opened.read_view(|view| view.is_editing_value()),
        "the editor stays open"
    );
}

/// The text is written back exactly as edited: a value the store kept as
/// compact JSON stays compact, and an indented one keeps its indentation.
#[gpui::test]
fn the_stored_json_layout_is_preserved(cx: &mut TestAppContext) {
    let compact = r#"["token-a","token-b"]"#;
    let compact_edited = r#"["token-a","token-c"]"#;
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.show_value(compact);
    opened.start_edit();
    assert_eq!(
        opened.editor_text(),
        "[\n  \"token-a\",\n  \"token-b\"\n]",
        "Formatted opens the stored text indented"
    );
    opened.set_editor_text("[\n  \"token-a\",\n  \"token-c\"\n]");
    opened.save_edit();
    assert_eq!(
        opened.written_values(),
        vec![(PLAINTEXT_SECRET.to_string(), compact_edited.to_string())]
    );

    let indented = "[\n  \"token-a\",\n  \"token-b\"\n]";
    let indented_edited = "[\n  \"token-a\",\n  \"token-c\"\n]";
    let mut reopened = open(cx, FAKE_SECRETS_STORE_ID, None);
    reopened.select(PLAINTEXT_SECRET);
    reopened.show_value(indented);
    reopened.start_edit();
    reopened.set_editor_text(indented_edited);
    reopened.save_edit();
    assert_eq!(
        reopened.written_values(),
        vec![(PLAINTEXT_SECRET.to_string(), indented_edited.to_string())]
    );
}

/// A value the store kept as JSON is refused when the edited text no longer
/// parses: the refusal names the parse problem and nothing is written.
#[gpui::test]
fn an_invalid_json_edit_is_refused_and_names_the_problem(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.show_value(r#"["token-a","token-b"]"#);
    opened.start_edit();
    opened.set_editor_text("[\"token-a\" oops");

    opened.save_edit();

    assert!(opened.driver.secret_store_calls().put_attempts.is_empty());
    assert!(opened.audit_records().is_empty(), "nothing was attempted");
    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("JSON"), "{}", errors[0].1);
    assert!(
        opened.read_view(|view| view.is_editing_value()),
        "the editor stays open"
    );
}

/// The edited text reaches no node of the accessibility update: the probe
/// reads every node the frame produced, hidden or not, the way the
/// automation bridge reads it, while the text is edited and after it is
/// saved.
#[gpui::test]
fn the_edited_text_reaches_no_node_of_the_accessibility_update(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);

    let editing_text = opened.accessibility_update_text();
    let editing_paths = opened.rendered_paths();
    assert!(!editing_text.contains(EDITED_VALUE), "{editing_text}");
    assert!(
        !editing_text.contains(FAKE_SECRET_PLAINTEXT),
        "{editing_text}"
    );
    assert!(
        !editing_paths.iter().any(|path| path.contains(EDITED_VALUE)),
        "{editing_paths:?}"
    );

    opened.save_edit();

    let saved_text = opened.accessibility_update_text();
    let saved_paths = opened.rendered_paths();
    assert!(!saved_text.contains(EDITED_VALUE), "{saved_text}");
    assert!(
        !saved_paths.iter().any(|path| path.contains(EDITED_VALUE)),
        "{saved_paths:?}"
    );
}

/// The connection is replaced while the editor is open — a region switch, a
/// reconnect, a credentials refresh: the save refuses instead of landing
/// the value on a session the user never saw, and the editor keeps its
/// text.
#[gpui::test]
fn a_replaced_connection_refuses_the_save(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);

    let replacement_driver = FakeDriver::new(DbKind::AwsSecrets);
    let replacement = {
        // Same shape the fixture's profile has, so the replacement reports
        // the same region and nothing but the session identity moves.
        let profile = dbflux_core::ConnectionProfile::new(
            "prod-aws",
            dbflux_core::DbConfig::SQLite {
                path: std::path::PathBuf::from(":memory:"),
                connection_id: None,
            },
        );
        replacement_driver.connect_arc(&profile).expect("connect")
    };
    let app_state = opened.app_state.clone();
    let doc = opened.doc.clone();
    opened.window.update(|_, cx| {
        let profile_id = doc.read(cx).profile_id;
        app_state.update(cx, |state, _| {
            if let Some(connected) = state.connections_mut().get_mut(&profile_id) {
                connected.connection = replacement.clone();
            }
        });
    });

    opened.save_edit();

    assert!(
        opened.driver.secret_store_calls().put_attempts.is_empty(),
        "the old session was not written"
    );
    assert!(
        replacement_driver
            .secret_store_calls()
            .put_attempts
            .is_empty(),
        "the replacement session was not written"
    );
    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].1.contains("was replaced"), "{}", errors[0].1);
    assert!(
        opened.read_view(|view| view.is_editing_value()),
        "the editor stays open"
    );
}

/// Every transition that would drop the open editor — another selection, a
/// refresh — is refused while it holds unsaved text, and works again once
/// the editor is discarded.
#[gpui::test]
fn unsaved_edits_block_the_transitions_that_would_drop_them(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);

    opened.select(MANAGED_SECRET);
    assert_eq!(
        opened.read(|doc| doc.selected().cloned()),
        Some(SecretRowKey::Secret(PLAINTEXT_SECRET.to_string())),
        "the selection stayed"
    );
    assert!(opened.read_view(|view| view.is_editing_value()));

    let lists_before = opened.driver.secret_store_calls().list_requests.len();
    assert!(opened.command(Command::RefreshSchema));
    assert_eq!(
        opened.driver.secret_store_calls().list_requests.len(),
        lists_before,
        "the refresh was refused"
    );
    assert!(opened.read_view(|view| view.is_editing_value()));
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::Loaded(_))));

    let errors: Vec<String> = opened
        .reported_errors()
        .into_iter()
        .map(|(_, summary)| summary)
        .collect();
    assert!(
        errors
            .iter()
            .all(|summary| summary.contains("unsaved edits")),
        "{errors:?}"
    );

    opened.update_view(|view, cx| view.discard_value_edit(cx));
    opened.select(MANAGED_SECRET);
    assert!(
        !opened.read_view(|view| view.is_editing_value()),
        "the selection works again after Discard"
    );
}

/// The tab itself reports unsaved value edits, so the workspace's dirty dot
/// and its unsaved-changes prompt ask before a close discards them.
#[gpui::test]
fn the_tab_reports_unsaved_edits_and_asks_before_closing(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);

    let (summary, status) = opened.window.update(|_, cx| {
        let doc = opened.doc.clone();
        (
            doc.read(cx).view.read(cx).unsaved_change_summary(cx),
            doc.read(cx).state(cx),
        )
    });
    assert_eq!(summary.as_deref(), Some("Unsaved value edits"));
    assert_eq!(status, crate::types::DocumentState::Modified);

    opened.update_view(|view, cx| view.discard_value_edit(cx));
    let (summary, status) = opened.window.update(|_, cx| {
        let doc = opened.doc.clone();
        (
            doc.read(cx).view.read(cx).unsaved_change_summary(cx),
            doc.read(cx).state(cx),
        )
    });
    assert_eq!(summary, None);
    assert_eq!(status, crate::types::DocumentState::Clean);
}

/// Text typed while a save is in flight survives the landing: the editor
/// stays open over it with the saved text as its baseline, so nothing the
/// user typed after Save is lost, and the next Save writes it as its own
/// version.
#[gpui::test]
fn edits_typed_during_the_save_survive_the_landing(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);

    // Save without waiting for the landing to restock the executor.
    let doc = opened.doc.clone();
    opened.window.update(|_, cx| {
        doc.update(cx, |doc, cx| {
            doc.view.update(cx, |view, cx| view.save_value_edit(cx))
        })
    });
    // Typed while the write is in flight; the helper's park lets the
    // landing run after it.
    let continued = format!("{EDITED_VALUE}\nENV=staging");
    opened.set_editor_text(&continued);

    opened.read_view(|view| {
        assert!(view.is_editing_value(), "the editor stayed open");
        if let Some(edit) = &view.value_edit {
            assert!(!edit.saving);
            assert_eq!(edit.original.expose_secret(), EDITED_VALUE);
        } else {
            panic!("no editor");
        }
    });
    assert_eq!(opened.editor_text(), continued);
    assert_eq!(
        opened.written_values(),
        vec![(PLAINTEXT_SECRET.to_string(), EDITED_VALUE.to_string())]
    );
    opened.read_view(|view| match &view.value {
        ValueState::Loaded(value) => {
            assert_eq!(value.version.as_deref(), Some("v-written-1"));
            assert_eq!(value.content.full_text().expose_secret(), EDITED_VALUE);
        }
        other => panic!("value not loaded: {other:?}"),
    });

    // The next Save writes what was typed as its own version, and the
    // editor closes: this time it still holds what was saved.
    opened.save_edit();
    let continued = continued.clone();
    assert_eq!(
        opened.written_values(),
        vec![
            (PLAINTEXT_SECRET.to_string(), EDITED_VALUE.to_string()),
            (PLAINTEXT_SECRET.to_string(), continued)
        ]
    );
    assert!(!opened.read_view(|view| view.is_editing_value()));
}

/// Discard sits out while a save is in flight — the pane action is
/// disabled and the guard makes a stale action a no-op — so the landing
/// still reads what was typed after Save.
#[gpui::test]
fn discard_sits_out_while_a_save_is_in_flight(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);
    opened.update_view(|view, _cx| {
        if let Some(edit) = view.value_edit.as_mut() {
            edit.saving = true;
        }
    });

    let mut enabled = |id: &str| {
        opened.window.update(|_, cx| {
            opened
                .doc
                .read(cx)
                .pane_actions(&opened.doc, cx)
                .into_iter()
                .find(|action| action.id.as_ref() == id)
                .map(|action| action.enabled)
        })
    };
    assert_eq!(enabled("secret-editor-discard"), Some(false));
    assert_eq!(enabled("secret-editor-save"), Some(false));

    opened.update_view(|view, cx| view.discard_value_edit(cx));
    assert!(
        opened.read_view(|view| view.is_editing_value()),
        "the in-flight save's editor is not discarded"
    );

    opened.update_view(|view, _cx| {
        if let Some(edit) = view.value_edit.as_mut() {
            edit.saving = false;
        }
    });
    opened.update_view(|view, cx| view.discard_value_edit(cx));
    assert!(!opened.read_view(|view| view.is_editing_value()));
}

/// The secret's details are re-checked at save time: a secret that has
/// meanwhile become managed, or whose deletion is scheduled, refuses with
/// the vocabulary the uneditable cases use, and nothing is written.
#[gpui::test]
fn a_details_change_refuses_the_save(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);

    // A describe that landed while the editor was open now reports another
    // service as the owner.
    opened.update_view(|view, _cx| {
        if let DetailsState::Loaded(details) = &mut view.details {
            details.summary.owning_service = Some("rds".to_string());
        }
    });
    opened.save_edit();

    assert!(opened.driver.secret_store_calls().put_attempts.is_empty());
    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors
            .iter()
            .all(|(_, summary)| summary.contains("managed by")),
        "{errors:?}"
    );
    assert!(opened.read_view(|view| view.is_editing_value()));

    // The ownership clears again, but a deletion is scheduled meanwhile.
    opened.update_view(|view, _cx| {
        if let DetailsState::Loaded(details) = &mut view.details {
            details.summary.owning_service = None;
            details.summary.deletion_scheduled_for = Some(dbflux_core::chrono::Utc::now());
        }
    });
    opened.save_edit();

    assert!(opened.driver.secret_store_calls().put_attempts.is_empty());
    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert!(
        errors
            .iter()
            .any(|(_, summary)| summary.contains("Deletion is scheduled")),
        "{errors:?}"
    );
    assert!(opened.read_view(|view| view.is_editing_value()));
}

/// A failed write is audited once: the seam records the event, so the
/// failure report must not emit the error seam's own tracing event, which
/// the audit bridge would turn into a second `user_error` row. The audit
/// trail keeps exactly the seam's row, and the user-facing notification
/// still goes out.
///
/// The regression this pins only surfaces where the audit bridge runs (the
/// app binary); in a test process without it the `user_error` emission
/// would be invisible, so the report path itself must never send one —
/// that is what `report_seam_write_failure` exists for.
#[gpui::test]
fn a_failed_write_audits_once_and_still_reaches_the_user(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.driver = opened
        .driver
        .clone()
        .with_secret_write_error(PLAINTEXT_SECRET, "put was denied by the fixture");
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);

    opened.save_edit();

    let events = opened.audit_records();
    assert_eq!(
        events.len(),
        2,
        "the retrieval and the write's own failure row: {events:?}"
    );
    assert_eq!(
        events[1].action.as_deref(),
        Some(SECRET_VALUE_WRITE_FAILED_ACTION)
    );
    assert!(
        events
            .iter()
            .all(|event| event.action.as_deref() != Some("user_error")),
        "no second row from the error seam: {events:?}"
    );

    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].1, "put was denied by the fixture");
}

/// Save drives through the interface: the button starts disabled, typed
/// input enables it, and clicking it runs the governed save.
#[gpui::test]
fn the_save_button_drives_the_save_from_the_interface(cx: &mut TestAppContext) {
    use dbflux_ui_base::keyboard_coverage::FrameCapture;

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    // The input lands where the editor put the keyboard.
    opened.start_edit();

    let save_button = |opened: &mut Opened<'_>| {
        let capture = FrameCapture::observe(opened.window);
        let frame = capture.frame(opened.window);
        let (_, node) = frame
            .nodes()
            .find(|(_, node)| node.path().ends_with("secret-value-editor-save"))
            .expect("Save is drawn");
        (
            node.bounds(),
            frame
                .accessibility_node(node)
                .map(|node| node.is_disabled()),
        )
    };

    let (_, disabled) = save_button(&mut opened);
    assert_eq!(disabled, Some(true), "an unchanged editor cannot save");

    opened.window.simulate_input("typed by hand");
    opened.window.run_until_parked();

    let (bounds, disabled) = save_button(&mut opened);
    assert_eq!(disabled, Some(false), "typing enables Save");

    // The click is a real one: press and release on the button's bounds.
    opened
        .window
        .simulate_click(bounds.center(), gpui::Modifiers::default());
    opened.window.run_until_parked();

    // The text lands at the editor's caret, which starts at the stored
    // value's first character.
    assert_eq!(
        opened.written_values(),
        vec![(
            PLAINTEXT_SECRET.to_string(),
            format!("typed by hand{FAKE_SECRET_PLAINTEXT}")
        )]
    );
    assert!(!opened.read_view(|view| view.is_editing_value()));
    let events = opened.audit_records();
    assert_eq!(
        events.last().and_then(|event| event.action.as_deref()),
        Some(SECRET_VALUE_WRITTEN_ACTION)
    );
}

/// The Edit value control is covered while it is drawn: the capture runs
/// before any editor is open, so the editor's own controls cannot mask a
/// misspelled registry row for the button.
#[gpui::test]
fn the_edit_value_control_is_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::SECRET_STORE;
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    let entries: Vec<String> = opened.window.update(|_, cx| {
        opened
            .doc
            .read(cx)
            .pane_actions(&opened.doc, cx)
            .into_iter()
            .map(|action| action.id.to_string())
            .collect()
    });

    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries)
        .assert_covered(&capture.frame(opened.window));

    assert!(
        checked.iter().any(|id| id == "secret-action-edit-value"),
        "{checked:?}"
    );

    // The row menu's Edit value entry, covered while the menu is open over
    // the selected secret whose value is loaded — the state the entry is
    // offered in.
    opened.right_click(&format!("secret-row-{PLAINTEXT_SECRET}"));
    let entries: Vec<String> = opened.window.update(|_, cx| {
        opened
            .doc
            .read(cx)
            .pane_actions(&opened.doc, cx)
            .into_iter()
            .map(|action| action.id.to_string())
            .collect()
    });

    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries)
        .assert_covered(&capture.frame(opened.window));

    assert!(
        checked
            .iter()
            .any(|id| id.starts_with("secret-menu-edit-value")),
        "{checked:?}"
    );
}

/// The row menu offers Edit value for a value that can be edited, and
/// choosing it opens the same editor the details pane's button opens.
#[gpui::test]
fn the_row_menu_offers_edit_value_and_opens_the_editor(cx: &mut TestAppContext) {
    use super::context_menu::SecretMenuAction;

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    // Right-clicking the already-selected row keeps its loaded value, which
    // is the state the entry is offered in.
    opened.right_click(&format!("secret-row-{PLAINTEXT_SECRET}"));
    assert!(
        opened
            .rendered_text()
            .contains(&dbflux_i18n::t!("document.secret_store.edit.open")),
        "the menu offers Edit value: {:?}",
        opened.rendered_text()
    );

    let index = opened.read(|doc| {
        doc.context_menu
            .as_ref()
            .expect("menu open")
            .items
            .iter()
            .position(|item| matches!(item.action, SecretMenuAction::EditValue))
            .expect("the Edit value entry")
    });

    for _ in 0..index {
        assert!(opened.command(Command::MenuDown));
    }
    assert!(opened.command(Command::MenuSelect));

    assert!(
        opened.read_view(|view| view.is_editing_value()),
        "the menu opens the editor"
    );
    assert_eq!(opened.editor_text(), FAKE_SECRET_PLAINTEXT);
    assert!(
        opened.read(|doc| doc.context_menu.is_none()),
        "the menu closed after the choice"
    );
}

/// The editor's Save, Discard and pane entries have a keyboard path while
/// the editor is open.
#[gpui::test]
fn the_editor_controls_are_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::SECRET_STORE;
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    // Save is disabled while the text is unchanged, so the coverage check
    // runs with an edit in it.
    opened.set_editor_text(EDITED_VALUE);

    let entries: Vec<String> = opened.window.update(|_, cx| {
        opened
            .doc
            .read(cx)
            .pane_actions(&opened.doc, cx)
            .into_iter()
            .map(|action| action.id.to_string())
            .collect()
    });

    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries)
        .assert_covered(&capture.frame(opened.window));

    assert!(
        checked.iter().any(|id| id == "secret-value-editor-save"),
        "{checked:?}"
    );
    assert!(
        checked.iter().any(|id| id == "secret-value-editor-discard"),
        "{checked:?}"
    );
}

// -- Region switch ---------------------------------------------------------

pub(crate) const CURRENT_REGION: &str = "us-east-1";
pub(crate) const OTHER_REGION: &str = "eu-west-1";

/// An app state whose profile is an `aws_secrets` connection the manager can
/// switch by region: the fake driver is registered under the built-in driver
/// id the profile resolves to, so `prepare_switch_region` finds it.
pub(crate) fn connected_aws_app_state(
    cx: &mut TestAppContext,
    driver: &FakeDriver,
    name: &str,
) -> (Entity<AppStateEntity>, Uuid) {
    let profile_id = Uuid::new_v4();

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let runtime =
                dbflux_storage::bootstrap::StorageRuntime::in_memory().expect("in-memory storage");
            dbflux_ui_base::AppStateEntity::new_with_storage_runtime(runtime)
                .expect("test storage setup")
        })
    });

    let profile = dbflux_core::ConnectionProfile::new(
        name,
        dbflux_core::DbConfig::AwsSecrets {
            region: CURRENT_REGION.to_string(),
            profile: None,
            access_key_id: None,
            endpoint: None,
        },
    );
    let connection = driver.connect_arc(&profile).expect("fake connection");

    cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state
                .facade
                .connections
                .drivers
                .insert("aws_secrets".to_string(), Arc::new(driver.clone()));
            state.connections_mut().insert(
                profile_id,
                dbflux_core::ConnectedProfile {
                    profile,
                    connection,
                    schema: None,
                    mutation_policy: dbflux_core::MutationPolicy::default(),
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

fn open_region_doc<'a>(
    cx: &'a mut TestAppContext,
    regions: &[&str],
    list_error: Option<String>,
) -> Opened<'a> {
    open_region_store(cx, FAKE_SECRETS_STORE_ID, None, regions, list_error)
}

fn open_region_store<'a>(
    cx: &'a mut TestAppContext,
    store_id: &str,
    path: Option<&str>,
    regions: &[&str],
    list_error: Option<String>,
) -> Opened<'a> {
    open_named_region_store(cx, "prod-aws", store_id, path, regions, list_error)
}

fn open_named_region_store<'a>(
    cx: &'a mut TestAppContext,
    profile_name: &str,
    store_id: &str,
    path: Option<&str>,
    regions: &[&str],
    list_error: Option<String>,
) -> Opened<'a> {
    init_keyboard_runtime(cx);

    let driver = FakeDriver::new(DbKind::AwsSecrets).with_region_switch(regions);
    driver.set_region_list_error(list_error);
    let (app_state, profile_id) = connected_aws_app_state(cx, &driver, profile_name);

    // `report_error` reaches the error seam through the `AppStateGlobal`
    // registration; without it a reported failure is invisible to these
    // tests.
    cx.update(|cx| {
        cx.set_global(AppStateGlobal {
            entity: app_state.clone(),
        });
    });

    let state = app_state.clone();
    let store_id = store_id.to_string();
    let path = path.map(str::to_string);

    let (doc, window) = cx.add_window_view(move |window, cx| {
        SecretStoreDocument::new(profile_id, store_id, path, state, window, cx)
    });
    window.run_until_parked();

    // The view-level tests exercise the editor where the harness mounts the
    // view; the pane's hand-over to the secret's tab is tested apart.
    window.update(|_, cx| {
        let view = doc.read(cx).view.clone();
        view.update(cx, |view, _| view.set_edit_in_place(true));
    });

    Opened {
        doc,
        window,
        driver,
        app_state,
    }
}

fn open_with_regions<'a>(cx: &'a mut TestAppContext, regions: &[&str]) -> Opened<'a> {
    open_region_doc(cx, regions, None)
}

/// The region picker only exists when the connection's driver advertises
/// `REGION_SWITCH`; a driver without it renders nothing regional.
#[gpui::test]
fn the_region_control_is_offered_only_when_the_driver_supports_it(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    assert!(!opened.read(|doc| doc.supports_region_switch()));
    let text = opened.rendered_text();
    assert!(!text.contains(CURRENT_REGION), "{text}");
    drop(opened);

    let mut opened = open_with_regions(cx, &[OTHER_REGION, CURRENT_REGION]);
    assert!(opened.read(|doc| doc.supports_region_switch()));
    let text = opened.rendered_text();
    assert!(text.contains(CURRENT_REGION), "{text}");
}

/// With the capability, the picker offers exactly the regions the
/// connection lists, sorted, with the current one shown as its value.
#[gpui::test]
fn the_region_control_offers_the_regions_the_connection_lists(cx: &mut TestAppContext) {
    let mut opened = open_with_regions(cx, &[OTHER_REGION, "ap-south-1", CURRENT_REGION]);

    let regions = opened.read(|doc| match doc.regions() {
        RegionListState::Loaded(regions) => regions.clone(),
        other => panic!("regions not loaded: {other:?}"),
    });
    assert_eq!(
        regions,
        vec![
            "ap-south-1".to_string(),
            OTHER_REGION.to_string(),
            CURRENT_REGION.to_string(),
        ]
    );

    let text = opened.rendered_text();
    assert!(text.contains(CURRENT_REGION), "{text}");
}

/// A store that cannot list its regions (no EC2 behind it, or an IAM user
/// without `ec2:DescribeRegions`) is not an error the user caused: no toast,
/// no error text in the header, no retry. The region the connection is
/// configured with stays visible as a plain chip, without a dropdown.
#[gpui::test]
fn a_failed_region_list_falls_back_to_the_configured_region_quietly(cx: &mut TestAppContext) {
    let message = "AccessDenied: not authorized to perform ec2:DescribeRegions";
    let mut opened = open_region_doc(cx, &[CURRENT_REGION], Some(message.to_string()));

    assert!(matches!(
        opened.read(|doc| doc.regions().clone()),
        RegionListState::Failed(_)
    ));
    assert_eq!(opened.driver.available_regions_calls(), 1);

    let text = opened.rendered_text();
    assert!(!text.contains(message), "{text}");
    assert!(
        !text.contains(&dbflux_i18n::t!("document.secret_store.region.retry")),
        "{text}"
    );
    assert!(!text.contains("Retry"), "{text}");
    assert!(
        text.contains(CURRENT_REGION),
        "the configured region stays visible: {text}"
    );

    let reported = opened
        .window
        .update(|_, cx| opened.app_state.read(cx).unread_error_count);
    assert_eq!(reported, 0, "a missing region list is not a user error");
    assert!(opened.reported_errors().is_empty());

    use dbflux_ui_base::keyboard_coverage::{FrameCapture, clickable_elements};
    let capture = FrameCapture::observe(opened.window);
    let frame = capture.frame(opened.window);
    let paths: Vec<String> = clickable_elements(&frame)
        .into_iter()
        .map(|element| element.path)
        .collect();
    assert!(
        !paths.iter().any(|path| path.contains("secret-region")),
        "no picker and no retry without a region list: {paths:?}"
    );
}

/// A successful switch reconnects the profile under the chosen region,
/// reloads the listing from its first page and drops the selection, its
/// details and any retrieved value.
#[gpui::test]
fn switching_the_region_reloads_the_listing(cx: &mut TestAppContext) {
    let mut opened = open_with_regions(cx, &[OTHER_REGION, CURRENT_REGION]);
    opened.select(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::Loaded(_))));

    let lists_before = opened
        .driver
        .secret_store_calls()
        .list_requests
        .iter()
        .filter(|(_, request)| request.page_token.is_none())
        .count();

    let region = OTHER_REGION.to_string();
    opened.update(|doc, cx| doc.switch_region(region, cx));

    assert_eq!(
        opened.read(|doc| doc.region().map(str::to_string)),
        Some(OTHER_REGION.to_string())
    );

    let profile_id = opened.read(|doc| doc.connection_id().expect("connection"));
    let app_state = opened.app_state.clone();
    let live_region = opened.window.update(|_, cx| {
        app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .and_then(|connected| connected.connection.current_region())
    });
    assert_eq!(live_region.as_deref(), Some(OTHER_REGION));

    let calls = opened.driver.secret_store_calls();
    // The switch asks the store for a first page again (a fresh page token);
    // the listing's own continuations keep their cost out of the assertion.
    let first_pages_after = calls
        .list_requests
        .iter()
        .filter(|(_, request)| request.page_token.is_none())
        .count();
    assert_eq!(first_pages_after, lists_before + 1);

    assert!(opened.read(|doc| doc.selected().is_none()));
    assert!(opened.read_view(|view| matches!(view.details(), DetailsState::Idle)));
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::NotLoaded)));
    assert!(!opened.read(|doc| doc.region_switch_in_flight()));
}

/// A region change this document did not start — another document of the same
/// profile switched, or the profile reconnected elsewhere — is adopted: the
/// listing reloads its first page under the live region and the selection,
/// its details and any retrieved value are dropped.
#[gpui::test]
fn a_region_change_installed_elsewhere_is_adopted(cx: &mut TestAppContext) {
    let mut opened = open_with_regions(cx, &[OTHER_REGION, CURRENT_REGION]);
    opened.select(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::Loaded(_))));

    let profile_id = opened.read(|doc| doc.connection_id().expect("connection"));
    let app_state = opened.app_state.clone();

    // Another surface moves the live connection in place and announces it.
    let first_pages_before = opened
        .driver
        .secret_store_calls()
        .list_requests
        .iter()
        .filter(|(_, request)| request.page_token.is_none())
        .count();

    opened.window.update(|_, cx| {
        let result = {
            let params = app_state
                .read(cx)
                .prepare_switch_region(profile_id, OTHER_REGION)
                .expect("prepare switch");
            params.execute().expect("reconnect")
        };

        app_state.update(cx, |state, cx| {
            assert!(state.apply_switch_region(
                result.profile_id,
                result.original_profile,
                result.connection,
                result.schema,
                result.session_generation,
            ));
            cx.emit(AppStateChanged);
        });
    });
    opened.window.run_until_parked();

    assert_eq!(
        opened.read(|doc| doc.region().map(str::to_string)),
        Some(OTHER_REGION.to_string())
    );
    assert!(opened.read(|doc| doc.selected().is_none()));
    assert!(opened.read_view(|view| matches!(view.details(), DetailsState::Idle)));
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::NotLoaded)));

    // The adopted region reloads from the first page; the listing's own
    // continuations keep their cost out of the assertion.
    let first_pages_after = opened
        .driver
        .secret_store_calls()
        .list_requests
        .iter()
        .filter(|(_, request)| request.page_token.is_none())
        .count();
    assert_eq!(first_pages_after, first_pages_before + 1);
}

/// A switch whose reconnect finished after the profile's connection was
/// replaced elsewhere is refused: the document keeps the region, the listing,
/// the selection, its details and the retrieved value it already shows,
/// clears its in-flight flag and reports neither success nor an error.
#[gpui::test]
fn a_refused_region_switch_leaves_the_document_exactly_as_it_was(cx: &mut TestAppContext) {
    let mut opened = open_with_regions(cx, &[OTHER_REGION, CURRENT_REGION]);
    opened.select(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::Loaded(_))));

    let entries_before = opened.entry_names();
    let errors_before = opened
        .window
        .update(|_, cx| opened.app_state.read(cx).unread_error_count);

    let profile_id = opened.read(|doc| doc.connection_id().expect("connection"));
    let app_state = opened.app_state.clone();
    let doc = opened.doc.clone();

    // The switch reconnects on the background executor; before it lands, the
    // profile's connection is replaced elsewhere and the session generation
    // moves on.
    opened.window.update(|_, cx| {
        doc.update(cx, |doc, cx| {
            doc.switch_region(OTHER_REGION.to_string(), cx)
        })
    });
    assert!(opened.read(|doc| doc.region_switch_in_flight()));

    let (current_profile, current_connection) = opened.window.update(|_, cx| {
        let connected = app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .expect("connected");
        (connected.profile.clone(), connected.connection.clone())
    });
    opened.window.update(|_, cx| {
        app_state.update(cx, |state, _| {
            state.apply_switch_database(profile_id, current_profile, current_connection, None);
        });
    });

    // The in-flight switch lands against a session that no longer exists.
    opened.window.run_until_parked();

    assert!(!opened.read(|doc| doc.region_switch_in_flight()));
    assert_eq!(
        opened.read(|doc| doc.region().map(str::to_string)),
        Some(CURRENT_REGION.to_string())
    );
    assert_eq!(opened.entry_names(), entries_before);
    assert!(opened.read(|doc| doc.selected().is_some()));
    assert!(opened.read_view(|view| matches!(view.details(), DetailsState::Loaded(_))));
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::Loaded(_))));

    let errors_after = opened
        .window
        .update(|_, cx| opened.app_state.read(cx).unread_error_count);
    assert_eq!(errors_after, errors_before, "a refusal is not an error");
}

/// A failed reconnect keeps the region, the stores and the listing exactly
/// as they were; picking the region already in use is ignored before
/// anything reconnects.
#[gpui::test]
fn a_failed_region_switch_keeps_the_current_region_and_listing(cx: &mut TestAppContext) {
    let mut opened = open_with_regions(cx, &[OTHER_REGION, CURRENT_REGION]);
    let entries_before = opened.entry_names();
    let lists_before = opened.driver.secret_store_calls().list_requests.len();

    opened
        .driver
        .set_connect_error(Some("network unreachable".to_string()));

    let region = OTHER_REGION.to_string();
    opened.update(|doc, cx| doc.switch_region(region, cx));

    assert_eq!(
        opened.read(|doc| doc.region().map(str::to_string)),
        Some(CURRENT_REGION.to_string())
    );
    assert!(!opened.read(|doc| doc.region_switch_in_flight()));
    assert_eq!(opened.entry_names(), entries_before);
    assert_eq!(
        opened.driver.secret_store_calls().list_requests.len(),
        lists_before
    );

    opened.update(|doc, cx| doc.switch_region(CURRENT_REGION.to_string(), cx));

    assert_eq!(
        opened.read(|doc| doc.region().map(str::to_string)),
        Some(CURRENT_REGION.to_string())
    );
    assert_eq!(
        opened.driver.secret_store_calls().list_requests.len(),
        lists_before
    );
}

/// Every clickable element of the listing and the details pane has a
/// keyboard path.
#[gpui::test]
fn the_secret_store_document_is_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::SECRET_STORE;
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    let entries: Vec<String> = opened.window.update(|_, cx| {
        opened
            .doc
            .read(cx)
            .pane_actions(&opened.doc, cx)
            .into_iter()
            .map(|action| action.id.to_string())
            .collect()
    });

    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries)
        .assert_covered(&capture.frame(opened.window));

    assert!(
        checked.iter().any(|id| id.starts_with("secret-row-")),
        "{checked:?}"
    );
    assert!(
        checked.iter().any(|id| id == "secret-value-reveal-0"),
        "{checked:?}"
    );
}

/// The region picker is only rendered by a region-capable driver, so covering
/// it needs a document of its own: the surface above renders a driver without
/// `REGION_SWITCH` and says nothing about it.
#[gpui::test]
fn the_region_picker_is_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::SECRET_STORE;
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture, clickable_elements};

    /// Checks the surface and returns the path of every clickable element, so
    /// the assertion names the control the way the registry does.
    fn checked_paths(opened: &mut Opened<'_>) -> Vec<String> {
        let entries: Vec<String> = opened.window.update(|_, cx| {
            opened
                .doc
                .read(cx)
                .pane_actions(&opened.doc, cx)
                .into_iter()
                .map(|action| action.id.to_string())
                .collect()
        });

        let capture = FrameCapture::observe(opened.window);
        let frame = capture.frame(opened.window);
        Coverage::new(SECRET_STORE)
            .with_menu_entries(entries)
            .assert_covered(&frame);

        clickable_elements(&frame)
            .into_iter()
            .map(|element| element.path)
            .collect()
    }

    let mut switchable = open_with_regions(cx, &[OTHER_REGION, CURRENT_REGION]);
    let paths = checked_paths(&mut switchable);
    assert!(
        paths
            .iter()
            .any(|path| path.ends_with("secret-region.dropdown-trigger")),
        "the picker renders for a region-capable driver: {paths:?}"
    );

    // The picker is also a pane action: the entry above validates that the
    // registry's Menu path names a real entry.
    let action_ids: Vec<String> = switchable.window.update(|_, cx| {
        switchable
            .doc
            .read(cx)
            .pane_actions(&switchable.doc, cx)
            .into_iter()
            .map(|action| action.id.to_string())
            .collect()
    });
    assert!(
        action_ids.iter().any(|id| id == "secret-region"),
        "the picker has a pane action: {action_ids:?}"
    );
}

/// The tab is found by its connection and store only, and a path opened
/// from the sidebar navigates the existing tab.
#[gpui::test]
fn the_tab_dedups_by_store_and_navigates_to_a_path(cx: &mut TestAppContext) {
    use crate::dedup::DocumentKey;
    use crate::tab_manager::Tab;

    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, None);
    let profile_id = opened.read(|doc| doc.connection_id().expect("connection"));
    let doc = opened.doc.clone();

    let tab = opened
        .window
        .update(|_, cx| Tab::Pane(Box::new(SecretStoreDocument::into_pane(doc, cx))));

    let matches = |key: DocumentKey, opened: &mut Opened<'_>| {
        opened
            .window
            .update(|_, cx| tab.matches_dedup_key(&key, cx))
    };

    assert!(matches(
        DocumentKey::SecretStore {
            profile_id,
            store_id: FAKE_PARAMETER_STORE_ID.to_string(),
        },
        &mut opened
    ));
    assert!(!matches(
        DocumentKey::SecretStore {
            profile_id,
            store_id: FAKE_SECRETS_STORE_ID.to_string(),
        },
        &mut opened
    ));
    assert!(!matches(
        DocumentKey::SecretStore {
            profile_id: Uuid::new_v4(),
            store_id: FAKE_PARAMETER_STORE_ID.to_string(),
        },
        &mut opened
    ));

    opened
        .window
        .update(|_, cx| tab.navigate_secret_path(Some("/app/prod/".to_string()), cx));
    opened.window.run_until_parked();

    assert_eq!(
        opened.read(|doc| doc.path().map(str::to_string)),
        Some("/app/prod/".to_string())
    );
    assert!(opened.read(|doc| {
        doc.listing()
            .child_paths
            .contains(&"/app/prod/db/".to_string())
    }));
}

/// While discovery is in flight the picker cannot be offered, and the region
/// must not disappear from the document: the breadcrumb keeps naming the
/// region the listing is using until the picker is there to replace it.
#[gpui::test]
fn the_breadcrumb_names_the_region_while_the_list_loads(cx: &mut TestAppContext) {
    use dbflux_ui_base::keyboard_coverage::{FrameCapture, clickable_elements};

    let mut opened = open_with_regions(cx, &[OTHER_REGION, CURRENT_REGION]);

    let triggers = |opened: &mut Opened<'_>| -> Vec<String> {
        let capture = FrameCapture::observe(opened.window);
        let frame = capture.frame(opened.window);
        clickable_elements(&frame)
            .into_iter()
            .map(|element| element.path)
            .filter(|path| path.ends_with("secret-region.dropdown-trigger"))
            .collect()
    };

    assert_eq!(triggers(&mut opened).len(), 1, "loaded before the reload");

    // A real discovery resolves on the next frame the test reads, so the
    // loading state is set directly: this is what a slow `DescribeRegions`
    // leaves the document in until the answer lands.
    opened.update(|doc, _cx| doc.mark_regions_loading());

    assert!(
        triggers(&mut opened).is_empty(),
        "a region list that is loading offers no picker"
    );
    assert!(
        opened.rendered_text().contains(CURRENT_REGION),
        "the region stays visible while the list loads: {}",
        opened.rendered_text()
    );
}

/// A discovery asked of a connection that has since been replaced belongs to
/// a session that no longer exists: its answer is dropped, not rendered.
#[gpui::test]
fn a_discovery_result_from_a_replaced_connection_is_dropped(cx: &mut TestAppContext) {
    let mut opened = open_with_regions(cx, &[OTHER_REGION, CURRENT_REGION]);
    let stale = opened.read(|doc| doc.region_discovery_generation().wrapping_sub(1));

    opened.update(|doc, cx| {
        doc.apply_regions(stale, Ok(vec!["mars-north-1".to_string()]), cx);
    });

    let text = opened.rendered_text();
    assert!(text.contains(CURRENT_REGION), "{text}");
    assert!(!text.contains("mars-north-1"), "{text}");
}

// -- Row context menu ------------------------------------------------------

impl Opened<'_> {
    /// Presses the secondary button on the rendered element `element_id`, the
    /// way a user right-clicks a row.
    fn right_click(&mut self, element_id: &str) {
        let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(self.window);
        let frame = capture.frame(self.window);

        let bounds = frame
            .nodes()
            .find(|(_, node)| node.id() == element_id)
            .map(|(_, node)| node.bounds())
            .unwrap_or_else(|| panic!("`{element_id}` is drawn"));

        self.window.simulate_event(gpui::MouseDownEvent {
            position: bounds.center(),
            modifiers: gpui::Modifiers::default(),
            button: gpui::MouseButton::Right,
            click_count: 1,
            first_mouse: false,
        });
        self.window.run_until_parked();
    }

    /// Left-clicks the drawn menu entry `item_id`.
    fn click_menu_item(&mut self, item_id: &str) {
        let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(self.window);
        let bounds = capture
            .frame(self.window)
            .nodes()
            .find(|(_, node)| node.id() == item_id)
            .map(|(_, node)| node.bounds())
            .unwrap_or_else(|| panic!("`{item_id}` is drawn"));

        self.window
            .simulate_click(bounds.center(), gpui::Modifiers::default());
        self.window.run_until_parked();
    }

    /// The open menu's entries as `SecretMenuAction` names, empty when no
    /// menu is open.
    fn menu_actions(&mut self) -> Vec<String> {
        self.read(|doc| {
            doc.context_menu
                .as_ref()
                .map(|menu| {
                    menu.items
                        .iter()
                        .map(|item| format!("{:?}", item.action))
                        .collect()
                })
                .unwrap_or_default()
        })
    }

    /// The pane actions' ids, the menu entries the coverage test supplies.
    fn pane_action_ids(&mut self) -> Vec<String> {
        self.window.update(|_, cx| {
            self.doc
                .read(cx)
                .pane_actions(&self.doc, cx)
                .into_iter()
                .map(|action| action.id.to_string())
                .collect()
        })
    }

    /// A fresh accessibility frame of the current render.
    fn observe_frame(&mut self) -> gpui::AccessibilityFrame {
        let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(self.window);
        capture.frame(self.window)
    }
}

/// Right-clicking a secret row opens the menu anchored at the pointer, the
/// row becomes the selection, and the menu holds the read items and the write
/// entries: Rotate now, Edit tags and Delete.
/// Edit value is offered for a row whose value was not retrieved too: it
/// retrieves the value first, through the audited path.
#[gpui::test]
fn a_secondary_click_opens_a_secret_row_menu(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);

    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));

    assert_eq!(
        opened.read(|doc| doc.selected().cloned()),
        Some(SecretRowKey::Secret(KEY_VALUE_SECRET.to_string())),
        "the right-clicked row becomes the selection"
    );
    assert_eq!(
        opened.menu_actions(),
        vec![
            "OpenInTab",
            "RetrieveValue",
            "CopyName",
            "CopyArn",
            "EditValue",
            "RotateNow",
            "EditTags",
            "Delete"
        ],
        "the menu lists the read items and the write entries"
    );

    let text = opened.rendered_text();
    for label in [
        "Retrieve value",
        "Copy name",
        "Copy ARN",
        "Rotate now",
        "Edit tags",
        "Delete",
    ] {
        assert!(text.contains(label), "{label} missing: {text}");
    }
}

/// A secret managed by another service offers the read items, and Delete
/// drawn disabled with the reason; a secret whose deletion is scheduled
/// offers Restore, and Delete disabled the same way. Nothing implies an
/// edit.
#[gpui::test]
fn restricted_secrets_disable_delete_and_say_why(cx: &mut TestAppContext) {
    for (name, expected) in [
        (
            MANAGED_SECRET,
            vec![
                "OpenInTab",
                "RetrieveValue",
                "CopyName",
                "CopyArn",
                "RotateNow",
                "EditTags",
                "Delete",
            ],
        ),
        (
            PENDING_DELETION_SECRET,
            vec![
                "OpenInTab",
                "RetrieveValue",
                "CopyName",
                "CopyArn",
                "RotateNow",
                "EditTags",
                "Restore",
                "Delete",
            ],
        ),
    ] {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);

        opened.right_click(&format!("secret-row-{name}"));

        assert!(
            opened.read(|doc| matches!(
                doc.selected(),
                Some(SecretRowKey::Secret(selected)) if selected == name
            )),
            "{name} becomes the selection"
        );
        assert_eq!(opened.menu_actions(), expected, "{name}");

        let delete = opened.read(|doc| {
            doc.context_menu
                .as_ref()
                .expect("menu open")
                .items
                .iter()
                .find(|item| item.action == super::context_menu::SecretMenuAction::Delete)
                .map(|item| (item.disabled, item.detail.clone()))
                .expect("the menu offers Delete")
        });
        assert!(delete.0, "{name}: Delete is disabled");
        assert!(
            delete.1.is_some_and(|reason| !reason.is_empty()),
            "{name}: Delete says why"
        );
    }
}

/// A secret whose deletion is scheduled cannot be retrieved (the document
/// blocks it), so its Retrieve value entry is disabled and says why instead
/// of copying or fetching nothing.
#[gpui::test]
fn a_secret_pending_deletion_keeps_retrieve_value_disabled(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);

    opened.right_click(&format!("secret-row-{PENDING_DELETION_SECRET}"));

    let detail = opened.read(|doc| {
        doc.context_menu
            .as_ref()
            .expect("menu open")
            .items
            .get(1)
            .map(|item| (item.disabled, item.detail.clone()))
            .expect("Retrieve value is the second entry")
    });
    assert!(
        detail.0,
        "Retrieve value is disabled while deletion is pending"
    );
    let detail = detail.1.unwrap_or_default();
    assert!(
        detail.contains("2026"),
        "the reason names the scheduled deletion: {detail}"
    );
    assert!(
        opened.rendered_text().contains("Deletion scheduled"),
        "the frame shows the reason"
    );

    // A normal secret keeps the entry enabled.
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));
    assert!(
        !opened.read(|doc| doc
            .context_menu
            .as_ref()
            .expect("menu open")
            .items
            .get(1)
            .map(|item| item.disabled)
            .expect("Retrieve value is the second entry")),
        "Retrieve value is enabled for a normal secret"
    );
}

/// A parameter row under a path offers Copy name, Retrieve value, Edit tags
/// and Delete.
#[gpui::test]
fn a_parameter_row_offers_copy_name_and_retrieve_value(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));

    opened.right_click("secret-row-/app/prod/region");

    assert_eq!(
        opened.menu_actions(),
        vec![
            "OpenInTab",
            "RetrieveValue",
            "CopyName",
            "EditValue",
            "EditTags",
            "Delete"
        ],
        "a parameter row offers Copy name, Retrieve value, Edit tags and Delete"
    );
    let text = opened.rendered_text();
    assert!(text.contains("Copy name"), "{text}");
    assert!(text.contains("Retrieve value"), "{text}");
    assert!(
        !text.contains("Copy ARN"),
        "parameters carry no ARN item: {text}"
    );
}

/// A path row offers Copy path and Refresh.
#[gpui::test]
fn a_path_row_offers_copy_path_and_refresh(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));

    opened.right_click("secret-path-row-/app/prod/db/");

    assert_eq!(
        opened.read(|doc| doc.selected().cloned()),
        Some(SecretRowKey::Path("/app/prod/db/".to_string())),
        "the right-clicked path becomes the selection"
    );
    assert_eq!(
        opened.menu_actions(),
        vec!["CopyPath", "Refresh"],
        "a path row offers Copy path and Refresh"
    );
    let text = opened.rendered_text();
    assert!(text.contains("Copy path"), "{text}");
    assert!(text.contains("Refresh"), "{text}");
}

/// Choosing a menu item runs it through the actions the toolbar and the
/// footer keys already use, and closes the menu.
#[gpui::test]
fn choosing_a_menu_item_runs_it_and_closes_the_menu(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);

    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));

    // Copy name is the third entry of the menu, after Open in tab and
    // Retrieve value.
    opened.click_menu_item("secret-menu-copy-name-item-2");

    assert_eq!(opened.clipboard(), Some(KEY_VALUE_SECRET.to_string()));
    assert!(
        !opened
            .rendered_paths()
            .iter()
            .any(|path| path.contains("secret-menu-")),
        "the menu closed after the choice"
    );
}

/// Copy path copies the path, from the path row's own menu.
#[gpui::test]
fn copy_path_copies_the_right_clicked_path(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));

    opened.right_click("secret-path-row-/app/prod/db/");
    opened.click_menu_item("secret-menu-copy-path-item-0");

    assert_eq!(opened.clipboard(), Some("/app/prod/db/".to_string()));
}

/// The menu closes on Escape and on a click outside, and a menu open over
/// the listing never swallows a later left click that should reach it.
#[gpui::test]
fn escape_and_a_click_outside_close_the_menu(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));
    assert!(
        !opened.menu_actions().is_empty(),
        "the menu is open before it closes"
    );

    assert!(opened.command(Command::Cancel));
    assert!(
        !opened
            .rendered_paths()
            .iter()
            .any(|path| path.contains("secret-menu-")),
        "Escape closes the menu"
    );

    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));
    let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(opened.window);
    let frame = capture.frame(opened.window);
    let outside = frame
        .nodes()
        .find(|(_, node)| node.id() == "secret-refresh")
        .map(|(_, node)| node.bounds())
        .expect("the refresh control is drawn");
    opened.window.simulate_event(gpui::MouseDownEvent {
        position: outside.center(),
        modifiers: gpui::Modifiers::default(),
        button: gpui::MouseButton::Left,
        click_count: 1,
        first_mouse: false,
    });
    opened.window.run_until_parked();

    assert!(
        !opened
            .rendered_paths()
            .iter()
            .any(|path| path.contains("secret-menu-")),
        "a click outside closes the menu"
    );
}

/// The keyboard owns the menu: MenuSelect runs the highlighted entry
/// through the same action a click would, and MenuDown moves the highlight.
#[gpui::test]
fn the_keyboard_runs_the_menu(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));

    // Retrieve value is the second entry, after Open in tab.
    assert!(opened.command(Command::MenuDown));
    assert!(opened.command(Command::MenuSelect));
    assert!(
        opened.read_view(|view| matches!(view.value(), ValueState::Loading))
            || opened.read_view(|view| matches!(view.value(), ValueState::Loaded(_))),
        "MenuSelect on the highlighted Retrieve value retrieves"
    );
    assert!(
        !opened
            .rendered_paths()
            .iter()
            .any(|path| path.contains("secret-menu-")),
        "the menu closed after the choice"
    );

    // Reopened, MenuDown moves the highlight to the next entry.
    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));
    assert!(opened.command(Command::MenuDown));
    assert_eq!(
        opened.read(|doc| doc.context_menu.as_ref().expect("menu open").selected_index),
        1
    );
}

/// Menu entries of both rows have a keyboard path: the coverage check runs
/// while each menu is open, so the new entries fail until they are listed.
#[gpui::test]
fn the_right_click_menu_items_are_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::SECRET_STORE;
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};

    let secrets_ids;
    let secrets_checked_ids;
    {
        let mut secrets = open(cx, FAKE_SECRETS_STORE_ID, None);
        secrets.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));
        let checked = Coverage::new(SECRET_STORE)
            .with_menu_entries(secrets.pane_action_ids())
            .assert_covered(&secrets.observe_frame());
        assert!(
            checked
                .iter()
                .any(|id| id == "secret-menu-retrieve-value-item-1"),
            "{checked:?}"
        );
        assert!(
            checked
                .iter()
                .any(|id| id == "secret-menu-open-in-tab-item-0"),
            "{checked:?}"
        );

        let secrets_checked = Coverage::new(SECRET_STORE)
            .with_menu_entries(secrets.pane_action_ids())
            .assert_covered(&secrets.observe_frame());
        secrets_ids = secrets.pane_action_ids();
        secrets_checked_ids = secrets_checked;
    }

    let mut parameters = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));
    parameters.right_click("secret-path-row-/app/prod/db/");
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(parameters.pane_action_ids())
        .assert_covered(&parameters.observe_frame());
    assert!(
        checked
            .iter()
            .any(|id| id == "secret-menu-copy-path-item-0"),
        "{checked:?}"
    );

    // The copy entries the registry names are entries the pane actions
    // really build (the Menu paths must name existing entries), and the
    // menu's own ARN entry names the copy-ARN command the footer key uses.
    assert!(
        secrets_ids.iter().any(|id| id == "secret-copy-name"),
        "{secrets_ids:?}"
    );
    assert!(
        parameters
            .pane_action_ids()
            .iter()
            .any(|id| id == "secret-copy-path"),
        "the path pane action exists"
    );
    assert!(
        secrets_checked_ids
            .iter()
            .any(|id| id == "secret-menu-copy-arn-item-3"),
        "{secrets_checked_ids:?}"
    );
}

// -- Review follow-ups (menu findings) --------------------------------------

impl Opened<'_> {
    /// Opens the menu over `row` at an arbitrary window position, the way
    /// `open_context_menu` would place it for a click there.
    fn open_menu_at(&mut self, row: SecretRowKey, position: gpui::Point<gpui::Pixels>) {
        let doc = self.doc.clone();
        self.window.update(|window, cx| {
            doc.update(cx, |doc, cx| {
                doc.open_context_menu(row, position, window, cx)
            });
        });
        self.window.run_until_parked();
    }

    fn viewport_size(&mut self) -> gpui::Size<gpui::Pixels> {
        self.window.update(|window, _cx| window.viewport_size())
    }

    fn menu_bounds(&mut self) -> Option<gpui::Bounds<gpui::Pixels>> {
        let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(self.window);
        capture
            .frame(self.window)
            .nodes()
            .find(|(_, node)| node.id() == "secret-context-menu")
            .map(|(_, node)| node.bounds())
    }
}

/// Retrieve value and Refresh show the chord the keymap binds, resolved the
/// way the data table's menu rows resolve theirs; the copy actions, which
/// no chord covers, show no invented hint.
#[gpui::test]
fn the_menu_entries_carry_the_shortcuts_the_keymap_binds(cx: &mut TestAppContext) {
    use super::render::listing_shortcut;

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));

    let shortcuts: Vec<Option<String>> = opened.read(|doc| {
        doc.context_menu
            .as_ref()
            .expect("menu open")
            .items
            .iter()
            .map(|item| item.shortcut.as_ref().map(|text| text.to_string()))
            .collect()
    });

    let expected = |command: Command| -> Option<String> {
        dbflux_ui_base::keymap::effective_keymap()
            .chord_for_command(dbflux_app::keymap::ContextId::Results, command)
            .map(|chord| dbflux_ui_base::keymap::chord_display_parts(chord).join(" "))
    };

    assert_eq!(
        shortcuts[0], None,
        "Open in tab has no chord and shows none"
    );
    assert_eq!(
        shortcuts[1].as_deref().map(str::to_string),
        expected(Command::ToggleValuePanel),
        "Retrieve value shows the chord the keymap binds"
    );
    assert_eq!(shortcuts[2], None, "Copy name has no chord and shows none");
    assert_eq!(shortcuts[3], None, "Copy ARN has no chord and shows none");

    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));
    opened.right_click("secret-path-row-/app/prod/db/");
    let shortcuts: Vec<Option<String>> = opened.read(|doc| {
        doc.context_menu
            .as_ref()
            .expect("menu open")
            .items
            .iter()
            .map(|item| item.shortcut.as_ref().map(|text| text.to_string()))
            .collect()
    });
    assert_eq!(shortcuts[0], None, "Copy path has no chord and shows none");
    assert_eq!(
        shortcuts[1].as_deref().map(str::to_string),
        expected(Command::RefreshSchema),
        "Refresh shows the chord the keymap binds"
    );
}

/// A describe that failed is not the same as one still arriving: the Copy
/// ARN entry says the failure the value section says its own, and neither
/// state lets the entry run.
#[gpui::test]
fn a_failed_describe_is_not_reported_as_loading(cx: &mut TestAppContext) {
    // Still arriving: the describe runs again with the right-click.
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    opened.update_view(|view, _cx| view.details = DetailsState::Loading);
    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));

    let detail = opened.read(|doc| {
        doc.context_menu
            .as_ref()
            .expect("menu open")
            .items
            .iter()
            .find(|item| item.action == super::context_menu::SecretMenuAction::CopyArn)
            .map(|item| (item.disabled, item.detail.clone()))
            .expect("the menu offers Copy ARN")
    });
    let loading = dbflux_i18n::t!("document.secret_store.context_menu.arn_loading").to_string();
    assert_eq!(
        detail.1.as_deref().map(str::to_string),
        Some(loading),
        "an arriving describe says so"
    );
    assert!(
        detail.0,
        "the entry stays disabled while the ARN is unknown"
    );

    // Failed: the reason carries the describe's error, not the loading line.
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    opened.update_view(|view, _cx| {
        view.details = DetailsState::Failed("Permission denied".to_string())
    });
    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));

    let detail = opened.read(|doc| {
        doc.context_menu
            .as_ref()
            .expect("menu open")
            .items
            .iter()
            .find(|item| item.action == super::context_menu::SecretMenuAction::CopyArn)
            .map(|item| (item.disabled, item.detail.clone()))
            .expect("the menu offers Copy ARN")
    });
    assert!(detail.0, "the entry stays disabled after a failed describe");
    let detail = detail.1.unwrap_or_default();
    assert!(
        detail.contains("Permission denied"),
        "the reason says the failure the value section says: {detail}"
    );
}

/// Enter on an entry the document blocks leaves the menu open and runs
/// nothing: the user keeps the menu instead of a silent no-op.
#[gpui::test]
fn enter_on_a_disabled_entry_leaves_the_menu_open(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.right_click(&format!("secret-row-{PENDING_DELETION_SECRET}"));

    let before = opened.read(|doc| doc.selected().cloned());
    // Open in tab is first; Retrieve value, the disabled entry, is second.
    assert!(opened.command(Command::MenuDown));
    assert!(opened.command(Command::MenuSelect));

    assert!(
        !opened.menu_actions().is_empty(),
        "the menu stays open on a disabled entry"
    );
    assert_eq!(opened.read(|doc| doc.selected().cloned()), before);
    assert!(
        opened.read_view(|view| matches!(view.value(), ValueState::NotLoaded)),
        "a disabled Retrieve value runs no retrieval"
    );
}

/// The panel opens at the secondary click and never leaves the window: a
/// click near the bottom-right corner keeps it fully visible.
#[gpui::test]
fn the_menu_opens_at_the_click_and_stays_inside_the_window(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));

    let click = {
        let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(opened.window);
        capture
            .frame(opened.window)
            .nodes()
            .find(|(_, node)| node.id() == format!("secret-row-{KEY_VALUE_SECRET}"))
            .map(|(_, node)| node.bounds().center())
            .expect("the right-clicked row is drawn")
    };

    let bounds = opened.menu_bounds().expect("the menu is rendered");

    assert!(
        (bounds.origin.x - click.x).abs() < gpui::px(1.5)
            && (bounds.origin.y - click.y).abs() < gpui::px(1.5),
        "the panel anchors at the click: {bounds:?} vs {click:?}"
    );

    let viewport = opened.viewport_size();
    assert!(
        bounds.bottom_right().x <= viewport.width && bounds.bottom_right().y <= viewport.height,
        "the panel stays inside the window: {bounds:?} vs {viewport:?}"
    );

    // A click near the bottom-right corner: the anchor must fit the window
    // instead of drawing off it.
    assert!(opened.command(Command::Cancel));
    // A click six pixels inside the corner, not a layout size: no token
    // describes a pointer position.
    let corner = gpui::point(
        viewport.width - gpui::px(6.0), // guardrail-allow: pointer position, not spacing
        viewport.height - gpui::px(6.0), // guardrail-allow: pointer position, not spacing
    );
    opened.open_menu_at(SecretRowKey::Secret(KEY_VALUE_SECRET.to_string()), corner);

    assert!(
        !opened.menu_actions().is_empty(),
        "the menu opens from the corner click"
    );
    let bounds = opened.menu_bounds().expect("the menu is rendered");
    assert!(
        bounds.bottom_right().x <= viewport.width && bounds.bottom_right().y <= viewport.height,
        "a corner click clamps inside the window: {bounds:?} vs {viewport:?}"
    );
    assert!(
        bounds.bottom_right().x >= corner.x || bounds.origin.x <= corner.x,
        "{bounds:?}"
    );
}

/// Switching tabs closes the menu of the tab that lost focus and drops a
/// queued entry; returning focus with the menu reopened keeps it.
#[gpui::test]
fn switching_away_from_the_tab_closes_the_menu(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));

    opened.update(|doc, _cx| doc.set_active_tab(false));

    assert!(
        opened.menu_actions().is_empty(),
        "a deactivated tab's menu is closed"
    );
    assert!(
        !opened
            .rendered_paths()
            .iter()
            .any(|path| path.contains("secret-menu-")),
        "the frame no longer draws the menu"
    );

    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));
    opened.update(|doc, _cx| doc.set_active_tab(true));

    assert!(
        !opened.menu_actions().is_empty(),
        "an active tab keeps its open menu"
    );
}

/// A reload of the listing — a refresh, a region switch — dismisses the menu
/// and drops a queued entry along with the selection they acted on.
#[gpui::test]
fn a_listing_reload_dismisses_the_menu_and_drops_the_pending_action(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));
    assert!(!opened.menu_actions().is_empty());

    opened.update(|doc, cx| doc.load_first_page(cx));
    opened.window.run_until_parked();

    assert!(
        opened.menu_actions().is_empty(),
        "the reload dismisses the menu"
    );
    assert!(
        opened.read(|doc| doc.pending_menu_action.is_none()),
        "the reload drops the queued entry"
    );
    assert!(
        opened.read(|doc| doc.selected().is_none()),
        "the reload dropped the selection"
    );
}

/// The copy actions are reachable from the keyboard without a right click:
/// the pane actions `m` lists Copy name and Copy path for the selection, the
/// same actions the row menu dispatches to.
#[gpui::test]
fn the_copy_actions_are_reachable_without_a_right_click(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);

    let ids = opened.pane_action_ids();
    assert!(
        ids.iter().any(|id| id == "secret-copy-name"),
        "Copy name is a pane action: {ids:?}"
    );

    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));
    opened.right_click("secret-path-row-/app/prod/db/");

    let ids = opened.pane_action_ids();
    assert!(
        ids.iter().any(|id| id == "secret-copy-path"),
        "Copy path is a pane action: {ids:?}"
    );

    // From a secret selection the path entry is not offered, and the other
    // way round: each names the selection it acts on.
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    assert!(
        !opened
            .pane_action_ids()
            .iter()
            .any(|id| id == "secret-copy-path"),
        "Copy path is not offered for a secret selection"
    );
}

// -- Writing through the governance seam -------------------------------------

/// The value the write tests submit. Every assertion that inspects an
/// audit event or an error also checks that this text never shows up in it.
const WRITTEN_VALUE: &str = "rotated-token-9d41";

/// Version the fake flat store reports for the first write it accepts.
const WRITTEN_VERSION: &str = "v-written-1";

/// A store whose writes fail with an error that echoes the submitted value,
/// the way a careless driver would leak it; the seam must keep the value out
/// of the error and the audit trail anyway.
struct EchoingStore {
    descriptor: SecretStoreDescriptor,
    put_calls: AtomicUsize,
}

impl EchoingStore {
    fn new() -> Self {
        Self {
            descriptor: SecretStoreDescriptor::new("echo", "Echo", SecretStoreCapabilities::WRITE),
            put_calls: AtomicUsize::new(0),
        }
    }
}

impl SecretStoreSource for EchoingStore {
    fn descriptor(&self) -> &SecretStoreDescriptor {
        &self.descriptor
    }

    fn list(&self, _request: &SecretListRequest) -> Result<SecretListPage, DbError> {
        Err(DbError::NotSupported(
            "listing is not part of this test".to_string(),
        ))
    }

    fn describe(&self, _name: &str) -> Result<SecretDetails, DbError> {
        Err(DbError::NotSupported(
            "describing is not part of this test".to_string(),
        ))
    }

    fn get_value(&self, _name: &str, _version: Option<&str>) -> Result<SecretValue, DbError> {
        Err(DbError::NotSupported(
            "reading is not part of this test".to_string(),
        ))
    }

    fn put_value(&self, request: &SecretPutRequest) -> Result<SecretValue, DbError> {
        self.put_calls.fetch_add(1, Ordering::Relaxed);

        let leak = match &request.payload {
            SecretPayload::Text(text) => text.expose_secret().to_owned(),
            SecretPayload::Binary(bytes) => {
                String::from_utf8_lossy(bytes.expose_secret()).into_owned()
            }
        };

        Err(DbError::query_failed(format!(
            "the service rejected the submitted value {leak} for {}",
            request.name
        )))
    }
}

/// The simplest in-memory audit recorder: the events the seam hands it, in
/// order.
#[derive(Default)]
struct RecordingSink(Mutex<Vec<EventRecord>>);

impl RecordingSink {
    fn events(&self) -> Vec<EventRecord> {
        match self.0.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
}

impl EventSink for RecordingSink {
    fn record(&self, event: EventRecord) -> Result<EventRecord, EventSinkError> {
        match self.0.lock() {
            Ok(mut guard) => guard.push(event.clone()),
            Err(poisoned) => poisoned.into_inner().push(event.clone()),
        }

        Ok(event)
    }
}

/// Runs `request` through the seam against `store`, under the policy the
/// caller resolved.
#[allow(clippy::result_large_err)] // test helper mirrors the seam's signature
fn write_through(
    store: &dyn SecretStoreSource,
    sink: &RecordingSink,
    policy: MutationPolicy,
    read_only_reason: Option<ReadOnlyReason>,
    request: &SecretPutRequest,
) -> Result<SecretValue, SecretWriteFailure> {
    put_secret_value(
        store,
        Uuid::from_u128(11),
        "aws_secrets",
        policy,
        read_only_reason,
        sink,
        request,
    )
}

/// A store that accepts a write and reports the version back, the way a
/// healthy store does.
struct SucceedingStore {
    descriptor: SecretStoreDescriptor,
    put_calls: AtomicUsize,
}

impl SucceedingStore {
    fn new() -> Self {
        Self {
            descriptor: SecretStoreDescriptor::new(
                "succeed",
                "Succeed",
                SecretStoreCapabilities::WRITE,
            ),
            put_calls: AtomicUsize::new(0),
        }
    }
}

impl SecretStoreSource for SucceedingStore {
    fn descriptor(&self) -> &SecretStoreDescriptor {
        &self.descriptor
    }

    fn list(&self, _request: &SecretListRequest) -> Result<SecretListPage, DbError> {
        Err(DbError::NotSupported(
            "listing is not part of this test".to_string(),
        ))
    }

    fn describe(&self, _name: &str) -> Result<SecretDetails, DbError> {
        Err(DbError::NotSupported(
            "describing is not part of this test".to_string(),
        ))
    }

    fn get_value(&self, _name: &str, _version: Option<&str>) -> Result<SecretValue, DbError> {
        Err(DbError::NotSupported(
            "reading is not part of this test".to_string(),
        ))
    }

    fn put_value(&self, request: &SecretPutRequest) -> Result<SecretValue, DbError> {
        self.put_calls.fetch_add(1, Ordering::Relaxed);
        Ok(SecretValue {
            version: Some(WRITTEN_VERSION.to_string()),
            payload: request.payload.clone(),
        })
    }
}

/// Through the seam, an allowed write's audit event classifies the
/// operation, names the new version and the store, validates against the
/// audit schema, and carries neither the value nor a stage: a `Current`
/// write is every store's default meaning.
#[test]
fn the_seam_classifies_a_write_and_audits_its_version_without_the_value() {
    let store = SucceedingStore::new();
    let sink = RecordingSink::default();
    let request = SecretPutRequest::text(PLAINTEXT_SECRET, WRITTEN_VALUE);

    let written = write_through(&store, &sink, MutationPolicy::Allowed, None, &request)
        .expect("an allowed write succeeds");
    assert_eq!(written.version.as_deref(), Some(WRITTEN_VERSION));
    assert_eq!(store.put_calls.load(Ordering::Relaxed), 1);

    let events = sink.events();
    assert_eq!(events.len(), 1, "{events:?}");
    let event = &events[0];
    assert_eq!(event.action, SECRET_VALUE_WRITTEN_ACTION);
    assert_eq!(event.category, EventCategory::SecretStore);
    assert_eq!(event.object_type.as_deref(), Some("succeed"));
    assert_eq!(event.object_id.as_deref(), Some(PLAINTEXT_SECRET));
    assert_eq!(event.connection_id, Some(Uuid::from_u128(11).to_string()));
    assert_eq!(
        event.correlation_id, None,
        "a success carries no correlation id"
    );

    let details: serde_json::Value =
        serde_json::from_str(event.details_json.as_deref().expect("details"))
            .expect("details_json is valid JSON");
    assert_eq!(
        details,
        serde_json::json!({ "classification": "write", "version": WRITTEN_VERSION })
    );
    dbflux_audit::AuditService::validate_event(event).expect("valid event");

    let serialized = serde_json::to_string(event).expect("serialize event");
    assert!(!serialized.contains(WRITTEN_VALUE), "{serialized}");
}

/// A staged write records its stage; a `Current` write to the same store
/// records none, because a store without staging labels has no stage to
/// name and the probe G6 shape keeps the event honest.
#[test]
fn a_write_records_a_stage_only_when_it_is_staged() {
    let store = SucceedingStore::new();

    let sink = RecordingSink::default();
    let staged = SecretPutRequest::text(PLAINTEXT_SECRET, WRITTEN_VALUE)
        .with_stage(SecretVersionStage::Pending);
    write_through(&store, &sink, MutationPolicy::Allowed, None, &staged)
        .expect("the staged write succeeds");

    let events = sink.events();
    assert_eq!(events.len(), 1, "{events:?}");
    let details: serde_json::Value =
        serde_json::from_str(events[0].details_json.as_deref().expect("details"))
            .expect("details_json is valid JSON");
    assert_eq!(details["stage"], serde_json::json!("pending"));

    let sink = RecordingSink::default();
    let current = SecretPutRequest::text(PLAINTEXT_SECRET, WRITTEN_VALUE);
    write_through(&store, &sink, MutationPolicy::Allowed, None, &current)
        .expect("the current write succeeds");

    let events = sink.events();
    assert_eq!(events.len(), 1, "{events:?}");
    let details: serde_json::Value =
        serde_json::from_str(events[0].details_json.as_deref().expect("details"))
            .expect("details_json is valid JSON");
    assert!(details.get("stage").is_none(), "{details}");
}

/// A failed write's audit event carries the correlation id the seam minted
/// as the failure's first catch site, the way a retrieval failure does.
#[test]
fn a_failed_write_audits_with_a_correlation_id_like_a_retrieval_failure() {
    let store = EchoingStore::new();
    let sink = RecordingSink::default();
    let request = SecretPutRequest::text(PLAINTEXT_SECRET, WRITTEN_VALUE);

    write_through(&store, &sink, MutationPolicy::Allowed, None, &request)
        .expect_err("the echoing store refuses its writes");

    let events = sink.events();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0].action, SECRET_VALUE_WRITE_FAILED_ACTION);
    assert!(events[0].correlation_id.is_some(), "{events:?}");
}

/// The stored-form text of a payload, as the careless drivers the failure
/// tests simulate would echo it.
fn payload_text(request: &SecretPutRequest) -> String {
    match &request.payload {
        SecretPayload::Text(text) => text.expose_secret().to_owned(),
        SecretPayload::Binary(bytes) => String::from_utf8_lossy(bytes.expose_secret()).into_owned(),
    }
}

/// The error a failure-shaped store's write returns, built from the
/// request the seam handed it.
type ErrorBehaviour = Box<dyn Fn(&SecretPutRequest) -> DbError + Send + Sync>;

/// A store whose writes fail with an error the test builds, so every error
/// shape the seam must scrub has a probe of its own.
struct ErrorStore {
    descriptor: SecretStoreDescriptor,
    error: ErrorBehaviour,
}

impl Default for ErrorStore {
    fn default() -> Self {
        Self {
            descriptor: SecretStoreDescriptor::new(
                "errors",
                "Errors",
                SecretStoreCapabilities::WRITE,
            ),
            error: Box::new(|_| DbError::NotSupported("no behaviour configured".into())),
        }
    }
}

impl SecretStoreSource for ErrorStore {
    fn descriptor(&self) -> &SecretStoreDescriptor {
        &self.descriptor
    }

    fn list(&self, _request: &SecretListRequest) -> Result<SecretListPage, DbError> {
        Err(DbError::NotSupported(
            "listing is not part of this test".to_string(),
        ))
    }

    fn describe(&self, _name: &str) -> Result<SecretDetails, DbError> {
        Err(DbError::NotSupported(
            "describing is not part of this test".to_string(),
        ))
    }

    fn get_value(&self, _name: &str, _version: Option<&str>) -> Result<SecretValue, DbError> {
        Err(DbError::NotSupported(
            "reading is not part of this test".to_string(),
        ))
    }

    fn put_value(&self, request: &SecretPutRequest) -> Result<SecretValue, DbError> {
        Err((self.error)(request))
    }
}

/// Every message-carrying error shape is scrubbed of the submitted value:
/// the fields a `FormattedError` renders (message, detail, hint, code and
/// location), the plain `String` variants, and an `io::Error` message. A
/// binary value is scrubbed in both forms a formatter may echo it in, its
/// lossy UTF-8 rendering and its base64.
#[test]
fn every_failure_shape_is_scrubbed_of_the_value() {
    let echoed: Vec<ErrorBehaviour> = vec![
        Box::new(|request| {
            let value = payload_text(request);
            DbError::PermissionDenied(
                FormattedError::new(format!("the service rejected the value {value}"))
                    .with_detail(format!("value {value} was not accepted"))
                    .with_hint(format!("check the value {value}"))
                    .with_code(format!("AccessDenied:{value}"))
                    .with_location(dbflux_core::ErrorLocation {
                        schema: None,
                        table: Some(format!("secrets_{value}")),
                        column: Some(value.clone()),
                        constraint: Some(format!("constraint_{value}")),
                    }),
            )
        }),
        Box::new(|request| {
            DbError::NotSupported(format!(
                "the store refused the write and echoed {}",
                payload_text(request)
            ))
        }),
        Box::new(|request| {
            DbError::ValueResolutionFailed(format!(
                "the resolver rejected the value {}",
                payload_text(request)
            ))
        }),
        Box::new(|request| {
            DbError::IoError(std::io::Error::other(format!(
                "socket write failed while carrying {}",
                payload_text(request)
            )))
        }),
    ];

    for error in echoed {
        let store = ErrorStore {
            error,
            ..ErrorStore::default()
        };
        let sink = RecordingSink::default();
        let request = SecretPutRequest::text(PLAINTEXT_SECRET, WRITTEN_VALUE);

        let surfaced = write_through(&store, &sink, MutationPolicy::Allowed, None, &request)
            .expect_err("the injected failure surfaces");

        let message = surfaced.to_string();
        assert!(!message.contains(WRITTEN_VALUE), "{message}");
        assert!(message.contains("[redacted]"), "{message}");

        let events = sink.events();
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(events[0].action, SECRET_VALUE_WRITE_FAILED_ACTION);
        assert!(
            !events[0]
                .error_message
                .as_deref()
                .unwrap_or_default()
                .contains(WRITTEN_VALUE),
            "audit error_message leaked: {:?}",
            events[0].error_message
        );
    }

    // A binary payload echoed in either derived form: lossy UTF-8 or base64.
    let base64 = encode_base64(WRITTEN_VALUE.as_bytes());
    for (form, needle) in [
        ("lossy utf-8", WRITTEN_VALUE.to_string()),
        ("base64", base64.clone()),
    ] {
        let store = ErrorStore {
            error: Box::new(move |_| {
                DbError::query_failed(format!("bad blob echoed as {form}: {needle}"))
            }),
            ..ErrorStore::default()
        };
        let sink = RecordingSink::default();
        let request = SecretPutRequest::binary("prod/blob", WRITTEN_VALUE.as_bytes().to_vec());

        let surfaced = write_through(&store, &sink, MutationPolicy::Allowed, None, &request)
            .expect_err("the injected failure surfaces");

        let message = surfaced.to_string();
        assert!(!message.contains(WRITTEN_VALUE), "{form}: {message}");
        assert!(!message.contains(&base64), "{form}: {message}");
        assert!(message.contains("[redacted]"), "{form}: {message}");
    }
}

/// A driver error that echoes the submitted value reaches neither the
/// caller nor the audit trail with the value in it.
#[test]
fn a_driver_error_that_echoes_the_value_is_scrubbed() {
    let store = EchoingStore::new();
    let sink = RecordingSink::default();
    let request = SecretPutRequest::text(PLAINTEXT_SECRET, WRITTEN_VALUE);

    let error = write_through(&store, &sink, MutationPolicy::Allowed, None, &request)
        .expect_err("the echoing store refuses its writes");

    let message = error.to_string();
    assert!(
        message.contains("rejected the submitted value"),
        "the driver's message survives the scrub: {message}"
    );
    assert!(!message.contains(WRITTEN_VALUE), "{message}");

    let events = sink.events();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0].action, SECRET_VALUE_WRITE_FAILED_ACTION);
    assert!(!format!("{events:?}").contains(WRITTEN_VALUE), "{events:?}");
}

impl Opened<'_> {
    /// Resolves the connected profile's mutation policy to `policy`, the
    /// way the profile resolver does at connect time.
    fn set_policy(&mut self, policy: MutationPolicy, read_only_reason: Option<ReadOnlyReason>) {
        let doc = self.doc.clone();
        let state = self.app_state.clone();
        self.window.update(|_, cx| {
            let profile_id = doc.read(cx).profile_id;
            state.update(cx, |state, _| {
                if let Some(connected) = state.connections_mut().get_mut(&profile_id) {
                    connected.mutation_policy = policy;
                    connected.read_only_reason = read_only_reason;
                }
            });
        });
    }

    /// Runs `request` through the governance seam with the connection's
    /// resolved policy and the app's audit service, the way the editor will
    /// call it.
    #[allow(clippy::result_large_err)] // test helper mirrors the seam's signature
    fn governed_write(
        &mut self,
        request: &SecretPutRequest,
    ) -> Result<SecretValue, SecretWriteFailure> {
        let doc = self.doc.clone();
        let state = self.app_state.clone();
        self.window.update(|_, cx| {
            let profile_id = doc.read(cx).profile_id;
            let store_id = doc.read(cx).store_id().to_string();

            let (connection, policy, reason) = {
                let connected = state
                    .read(cx)
                    .connections()
                    .get(&profile_id)
                    .expect("connected profile");
                (
                    Arc::clone(&connected.connection),
                    connected.mutation_policy,
                    connected.read_only_reason,
                )
            };

            let audit = state.read(cx).audit_service();
            let store = connection
                .secret_store(&store_id)
                .expect("the fake connection exposes the store");

            put_secret_value(
                store,
                profile_id,
                connection.metadata().id.as_str(),
                policy,
                reason,
                audit,
                request,
            )
        })
    }
}

/// A read-only profile refuses the write through the seam before the store
/// is called, records one refused audit event, and the refusal names the
/// setting without the value.
#[gpui::test]
fn a_read_only_profile_refuses_the_write_before_it_reaches_the_driver(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.set_policy(
        MutationPolicy::ReadOnly,
        Some(ReadOnlyReason::ProfileSetting),
    );

    let request = SecretPutRequest::text(PLAINTEXT_SECRET, WRITTEN_VALUE);
    let error = opened
        .governed_write(&request)
        .expect_err("a read-only profile refuses the write");

    let message = error.to_string();
    assert!(message.contains("read-only"), "{message}");
    assert!(message.contains(PLAINTEXT_SECRET), "{message}");
    assert!(!message.contains(WRITTEN_VALUE), "{message}");

    let calls = opened.driver.secret_store_calls();
    assert!(calls.put_attempts.is_empty(), "{calls:?}");
    assert!(calls.put_requests.is_empty(), "{calls:?}");

    let events = opened.audit_events();
    assert_eq!(events.len(), 1, "{events:?}");
    assert!(
        events[0].contains(SECRET_VALUE_WRITE_REFUSED_ACTION),
        "{events:?}"
    );
    assert!(
        events.iter().all(|event| !event.contains(WRITTEN_VALUE)),
        "{events:?}"
    );
}

/// An approval-required profile must not silently allow the write: the seam
/// refuses it, records the refusal and never calls the store until an
/// approval flow for secret writes exists.
#[gpui::test]
fn an_approval_required_profile_does_not_silently_allow_the_write(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.set_policy(MutationPolicy::ApprovalRequired, None);

    let request = SecretPutRequest::text(PLAINTEXT_SECRET, WRITTEN_VALUE);
    let error = opened
        .governed_write(&request)
        .expect_err("an approval-required profile refuses the write");

    let message = error.to_string();
    assert!(message.contains("approval"), "{message}");
    assert!(message.contains(PLAINTEXT_SECRET), "{message}");
    assert!(!message.contains(WRITTEN_VALUE), "{message}");

    let calls = opened.driver.secret_store_calls();
    assert!(calls.put_attempts.is_empty(), "{calls:?}");

    let events = opened.audit_events();
    assert_eq!(events.len(), 1, "{events:?}");
    assert!(
        events[0].contains(SECRET_VALUE_WRITE_REFUSED_ACTION),
        "{events:?}"
    );
}

/// An allowed write reaches the store once, returns the new version, and
/// records exactly one audit event that carries the version and not the
/// value.
#[gpui::test]
fn an_allowed_write_reaches_the_store_once_and_audits_the_new_version_without_the_value(
    cx: &mut TestAppContext,
) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);

    let request = SecretPutRequest::text(PLAINTEXT_SECRET, WRITTEN_VALUE);
    let written = opened
        .governed_write(&request)
        .expect("an allowed write succeeds");
    assert_eq!(written.version.as_deref(), Some(WRITTEN_VERSION));

    let calls = opened.driver.secret_store_calls();
    assert_eq!(calls.put_attempts.len(), 1, "{calls:?}");
    assert_eq!(calls.put_requests.len(), 1, "{calls:?}");

    let events = opened.audit_events();
    assert_eq!(events.len(), 1, "{events:?}");
    assert!(
        events[0].contains(SECRET_VALUE_WRITTEN_ACTION),
        "{events:?}"
    );
    assert!(events[0].contains(WRITTEN_VERSION), "{events:?}");
    assert!(
        events.iter().all(|event| !event.contains(WRITTEN_VALUE)),
        "{events:?}"
    );
}

/// A write the store fails records the failure event and no success event,
/// and the surfaced error carries the store's message without the value.
#[gpui::test]
fn a_failing_write_audits_the_failure_and_no_success_event(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.driver = opened
        .driver
        .clone()
        .with_secret_write_error(PLAINTEXT_SECRET, "not authorized to put the secret");

    let request = SecretPutRequest::text(PLAINTEXT_SECRET, WRITTEN_VALUE);
    let error = opened
        .governed_write(&request)
        .expect_err("the injected write failure surfaces");

    let message = error.to_string();
    assert!(
        message.contains("not authorized to put the secret"),
        "{message}"
    );
    assert!(!message.contains(WRITTEN_VALUE), "{message}");

    let calls = opened.driver.secret_store_calls();
    assert_eq!(calls.put_attempts.len(), 1, "{calls:?}");
    assert!(calls.put_requests.is_empty(), "{calls:?}");

    let events = opened.audit_events();
    assert_eq!(events.len(), 1, "{events:?}");
    assert!(
        events[0].contains(SECRET_VALUE_WRITE_FAILED_ACTION),
        "{events:?}"
    );
    assert!(
        events
            .iter()
            .all(|event| !event.contains(SECRET_VALUE_WRITTEN_ACTION)),
        "{events:?}"
    );
    assert!(
        events.iter().all(|event| !event.contains(WRITTEN_VALUE)),
        "{events:?}"
    );
}

/// A JSON-escaped form of the value — newlines as \n, quotes as \", a
/// backslash as \\, a tab as \t — is the same leak as the verbatim value,
/// and the seam scrubs it: a service echoing a request body reaches the
/// caller with the escape-snipped marker, not the value.
#[test]
fn an_escaped_form_of_the_value_is_scrubbed() {
    let json_escaped = |value: &str| {
        let quoted = serde_json::to_string(value).expect("a &str always serializes");
        quoted[1..quoted.len() - 1].to_string()
    };

    for value in [
        "tab\tseparated",
        "line\nbroken",
        "quoted\"value",
        "back\\slash",
    ] {
        // The store models a service that failed and echoed the request
        // body it received, JSON escapes and all.
        let escaped = json_escaped(value);
        let store = ErrorStore {
            error: Box::new({
                let escaped = escaped.clone();
                move |_| {
                    DbError::query_failed(format!(
                        "rejected the request body {{\"value\":\"{escaped}\"}}"
                    ))
                }
            }),
            ..ErrorStore::default()
        };
        let sink = RecordingSink::default();
        let request = SecretPutRequest::text(PLAINTEXT_SECRET, value);

        let failure = write_through(&store, &sink, MutationPolicy::Allowed, None, &request)
            .expect_err("the echo failure surfaces");

        let message = failure.to_string();
        assert!(!message.contains(value), "{value}: {message}");
        assert!(!message.contains(&escaped), "{value}: {message}");
        assert!(message.contains("[redacted]"), "{value}: {message}");

        let events = sink.events();
        assert_eq!(events.len(), 1, "{value}");
        let recorded = events[0].error_message.as_deref().unwrap_or_default();
        assert!(!recorded.contains(value), "{value}: {recorded}");
        assert!(!recorded.contains(&escaped), "{value}: {recorded}");
    }
}

/// A failed write returns the correlation id its audit event recorded, so
/// the caller can point at the same row when it reports, the way a
/// retrieval failure does.
#[test]
fn a_failing_write_returns_the_correlation_id_its_audit_event_recorded() {
    let store = EchoingStore::new();
    let sink = RecordingSink::default();
    let request = SecretPutRequest::text(PLAINTEXT_SECRET, WRITTEN_VALUE);

    let failure = write_through(&store, &sink, MutationPolicy::Allowed, None, &request)
        .expect_err("the echoing store refuses its writes");
    let event = &sink.events()[0];

    assert_eq!(event.action, SECRET_VALUE_WRITE_FAILED_ACTION);
    assert!(
        failure.correlation_id.is_some(),
        "no id minted: {failure:?}"
    );
    assert_eq!(
        failure.correlation_id.map(|id| id.to_string()),
        event.correlation_id
    );
}

// -- Value editor: line numbers and Formatted / Raw -------------------------

mod editor_layout {
    use super::*;
    use crate::secret_store::view::editor::EditMode;
    use crate::secret_store::view::editor::json_layout::{minify, pretty};

    /// Nested, so it opens as plaintext (not key/value), with a string that
    /// holds repeated spaces and escaped quotes, and a number a parse and
    /// re-serialize would normalize.
    const COMPACT: &str = r#"{"db":{"host":"h","note":"a  b \"q\" 77b1-secret"},"n":[1,2.50]}"#;

    const FORMATTED: &str = "{\n  \"db\": {\n    \"host\": \"h\",\n    \"note\": \"a  b \\\"q\\\" 77b1-secret\"\n  },\n  \"n\": [\n    1,\n    2.50\n  ]\n}";

    const COMPACT_EDITED: &str =
        r#"{"db":{"host":"h2","note":"a  b \"q\" 77b1-secret"},"n":[1,2.50]}"#;

    impl Opened<'_> {
        fn edit_mode(&mut self) -> Option<EditMode> {
            self.read_view(|view| view.edit_mode())
        }

        fn set_mode(&mut self, mode: EditMode) {
            let doc = self.doc.clone();
            self.window.update(|window, cx| {
                doc.update(cx, |doc, cx| {
                    doc.view
                        .update(cx, |view, cx| view.set_edit_mode(mode, window, cx))
                })
            });
            self.window.run_until_parked();
        }

        /// The bounds of the element whose path ends with `suffix`.
        pub(super) fn element_bounds(&mut self, suffix: &str) -> gpui::Bounds<gpui::Pixels> {
            let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(self.window);
            let frame = capture.frame(self.window);

            frame
                .nodes()
                .find(|(_, node)| node.path().ends_with(suffix))
                .map(|(_, node)| node.bounds())
                .expect("the element is rendered")
        }

        pub(super) fn dirty(&mut self) -> bool {
            let doc = self.doc.clone();
            self.window
                .update(|_, cx| doc.read(cx).view.read(cx).has_unsaved_value_edits(cx))
        }

        fn has_path(&mut self, suffix: &str) -> bool {
            self.rendered_paths()
                .iter()
                .any(|path| path.ends_with(suffix))
        }

        fn edit_json(&mut self, stored: &str) {
            self.select(PLAINTEXT_SECRET);
            self.show_value(stored);
            self.start_edit();
        }
    }

    #[test]
    fn pretty_and_minify_change_only_insignificant_whitespace() {
        assert_eq!(pretty(COMPACT), FORMATTED);
        assert_eq!(minify(FORMATTED), COMPACT);
        assert_eq!(minify(&pretty(COMPACT)), COMPACT);
        assert_eq!(pretty("{}"), "{}");
        assert_eq!(
            pretty(r#"{"a":[],"b":{}}"#),
            "{\n  \"a\": [],\n  \"b\": {}\n}"
        );
        assert_eq!(pretty("42"), "42");
        assert_eq!(minify(" [ 1 , 2 ] "), "[1,2]");
    }

    #[gpui::test]
    fn the_toggle_is_offered_for_stored_json_only(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_json("host = db.internal\nport = 5432");
        assert_eq!(opened.edit_mode(), None, "plaintext has no mode");
        assert!(!opened.has_path("segmented-secret-editor-mode"));
        assert!(!opened.read_view(|view| view.can_toggle_edit_mode()));

        let mut json = open(cx, FAKE_SECRETS_STORE_ID, None);
        json.edit_json(COMPACT);
        assert_eq!(json.edit_mode(), Some(EditMode::Formatted));
        assert!(json.read_view(|view| view.can_toggle_edit_mode()));
        assert!(json.has_path("segmented-secret-editor-mode-formatted"));
        assert!(json.has_path("segmented-secret-editor-mode-raw"));
    }

    #[gpui::test]
    fn a_json_value_opens_formatted_and_switches_losslessly(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_json(COMPACT);
        assert_eq!(opened.editor_text(), FORMATTED);

        opened.set_mode(EditMode::Raw);
        assert_eq!(opened.editor_text(), COMPACT, "Raw shows the stored text");

        opened.set_mode(EditMode::Formatted);
        assert_eq!(opened.editor_text(), FORMATTED);
        assert!(!opened.dirty());
    }

    #[gpui::test]
    fn switching_with_an_edit_carries_the_edit_when_it_parses(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_json(COMPACT);
        opened.set_editor_text(&FORMATTED.replace("\"h\"", "\"h2\""));

        opened.set_mode(EditMode::Raw);
        assert_eq!(opened.editor_text(), COMPACT_EDITED);

        opened.set_mode(EditMode::Formatted);
        assert_eq!(opened.editor_text(), pretty(COMPACT_EDITED));

        // Text that does not parse is kept as typed.
        opened.set_editor_text("{\"db\": oops");
        opened.set_mode(EditMode::Raw);
        assert_eq!(opened.editor_text(), "{\"db\": oops");
    }

    #[gpui::test]
    fn formatted_save_of_compact_json_writes_compact(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_json(COMPACT);
        opened.set_editor_text(&FORMATTED.replace("\"h\"", "\"h2\""));

        opened.save_edit();

        assert_eq!(
            opened.written_values(),
            vec![(PLAINTEXT_SECRET.to_string(), COMPACT_EDITED.to_string())]
        );
    }

    #[gpui::test]
    fn formatted_save_of_multiline_json_writes_the_text_as_edited(cx: &mut TestAppContext) {
        let stored = "{\n    \"db\": {\n        \"host\": \"h\"\n    }\n}";
        let edited = "{\n    \"db\": {\n        \"host\": \"h2\"\n    }\n}";
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_json(stored);
        assert_eq!(opened.editor_text(), stored, "its own layout is kept");

        opened.set_editor_text(edited);
        opened.save_edit();

        assert_eq!(
            opened.written_values(),
            vec![(PLAINTEXT_SECRET.to_string(), edited.to_string())]
        );
    }

    #[gpui::test]
    fn raw_save_writes_the_text_verbatim(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_json(COMPACT);
        opened.set_mode(EditMode::Raw);
        let spaced = r#"{ "db": { "host": "h3" } }"#;
        opened.set_editor_text(spaced);

        opened.save_edit();

        assert_eq!(
            opened.written_values(),
            vec![(PLAINTEXT_SECRET.to_string(), spaced.to_string())]
        );
    }

    #[gpui::test]
    fn an_unchanged_save_is_a_no_op_in_either_mode(cx: &mut TestAppContext) {
        for stored in [COMPACT, r#"{"db": {"host": "h"}}"#, "{\n    \"a\": [1]\n}"] {
            let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
            opened.edit_json(stored);
            let events = opened.audit_records().len();

            opened.save_edit();
            opened.set_mode(EditMode::Raw);
            assert_eq!(opened.editor_text(), stored);
            opened.save_edit();
            opened.set_mode(EditMode::Formatted);
            opened.save_edit();

            assert!(opened.driver.secret_store_calls().put_attempts.is_empty());
            assert_eq!(opened.audit_records().len(), events);
            assert!(opened.reported_errors().is_empty());
            assert!(!opened.dirty());
        }
    }

    #[gpui::test]
    fn invalid_json_blocks_save_in_both_modes(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_json(COMPACT);

        opened.set_editor_text("{\"db\": oops");
        opened.save_edit();
        opened.set_mode(EditMode::Raw);
        opened.set_editor_text("{\"db\": oops");
        opened.save_edit();

        assert!(opened.driver.secret_store_calls().put_attempts.is_empty());
        let errors = opened.reported_errors();
        assert_eq!(errors.len(), 2, "{errors:?}");
        assert!(errors.iter().all(|(_, summary)| summary.contains("JSON")));
    }

    #[gpui::test]
    fn the_value_reaches_no_accessibility_node_in_either_mode(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_json(COMPACT);

        for mode in [EditMode::Formatted, EditMode::Raw] {
            opened.set_mode(mode);
            let text = opened.accessibility_update_text();
            let paths = opened.rendered_paths();
            assert!(!text.contains("77b1-secret"), "{mode:?}: {text}");
            assert!(!text.contains("host"), "{mode:?}: {text}");
            assert!(!paths.iter().any(|path| path.contains("77b1")));
        }
    }

    #[gpui::test]
    fn the_mode_has_a_keyboard_path(cx: &mut TestAppContext) {
        use crate::keyboard_coverage::SECRET_STORE;
        use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};

        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_json(COMPACT);
        opened.set_editor_text(&FORMATTED.replace("\"h\"", "\"h2\""));

        let entries: Vec<String> = opened.window.update(|_, cx| {
            opened
                .doc
                .read(cx)
                .pane_actions(&opened.doc, cx)
                .into_iter()
                .map(|action| action.id.to_string())
                .collect()
        });
        assert!(entries.iter().any(|id| id == "secret-editor-mode"));

        let capture = FrameCapture::observe(opened.window);
        let checked = Coverage::new(SECRET_STORE)
            .with_menu_entries(entries)
            .assert_covered(&capture.frame(opened.window));
        assert!(
            checked
                .iter()
                .any(|id| id == "segmented-secret-editor-mode-raw"),
            "{checked:?}"
        );

        assert!(opened.command(Command::CycleResultView));
        assert_eq!(opened.edit_mode(), Some(EditMode::Raw));
        assert!(opened.command(Command::CycleResultView));
        assert_eq!(opened.edit_mode(), Some(EditMode::Formatted));
    }
}

// -- Value editor: artboard 06 chrome ---------------------------------------

mod editor_chrome {
    use super::*;
    use crate::secret_store::view::editor::EditMode;

    const THREE: &str = "alpha-77b1\nbeta-77b1\ngamma-77b1";

    impl Opened<'_> {
        fn edit_text(&mut self, stored: &str, edited: &str) {
            // Focus events reach listeners only while the window is active.
            self.window.update(|window, _| window.activate_window());
            self.window.run_until_parked();
            self.rendered_paths();
            self.select(PLAINTEXT_SECRET);
            self.show_value(stored);
            self.start_edit();
            self.set_editor_text(edited);
        }

        fn rect(&mut self, suffix: &str) -> gpui::Bounds<gpui::Pixels> {
            let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(self.window);
            let frame = capture.frame(self.window);

            frame
                .nodes()
                .find(|(_, node)| node.path().ends_with(suffix))
                .map(|(_, node)| node.bounds())
                .unwrap_or_else(|| panic!("{suffix} is rendered"))
        }

        fn exists(&mut self, suffix: &str) -> bool {
            self.rendered_paths()
                .iter()
                .any(|path| path.ends_with(suffix))
        }

        pub(super) fn footer_text(&mut self) -> String {
            let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(self.window);
            let frame = capture.frame(self.window);

            frame
                .nodes()
                .filter(|(_, node)| node.path().contains("secret-value-editor-footer"))
                .map(|(_, node)| node.content_text())
                .collect::<Vec<_>>()
                .join(" ")
        }
    }

    #[gpui::test]
    fn the_modified_chip_follows_the_edit(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_text(THREE, THREE);
        assert!(!opened.exists("secret-value-editor-modified"));

        opened.set_editor_text("alpha-77b1\nbeta-77b1");
        assert!(opened.exists("secret-value-editor-modified"));
        assert!(opened.rendered_text().contains("modified"));
    }

    #[gpui::test]
    fn the_header_row_holds_the_toggle_discard_and_save(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_text(r#"{"a":{"b":1}}"#, r#"{"a":{"b":2}}"#);

        let editor = opened.rect("secret-value-editor");
        let toggle = opened.rect("segmented-secret-editor-mode");
        let discard = opened.rect("secret-value-editor-discard");
        let save = opened.rect("secret-value-editor-save");

        for control in [toggle, discard, save] {
            assert!(
                control.bottom() <= editor.top(),
                "{control:?} above {editor:?}"
            );
        }
        // The details pane is narrower than the artboard's tab, so the
        // controls wrap, but Discard and Save stay together, in that order,
        // and never above the toggle.
        assert!((discard.center().y - save.center().y).abs() < gpui::px(4.)); // guardrail-allow: test tolerance, not spacing
        assert!(discard.right() <= save.left());
        assert!(toggle.top() <= discard.top());
    }

    #[gpui::test]
    fn save_shows_the_keycap_of_a_bound_chord_only(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_text(THREE, "x");

        let context = opened.read_cx(|doc, cx| doc.active_context(cx));
        let bound =
            dbflux_ui_base::effective_keymap().shortcut_for_command(context, Command::SaveQuery);
        let bound = bound.expect("Save is bound where the editor dispatches it");

        assert!(
            opened
                .rendered_text()
                .to_lowercase()
                .contains(&bound.to_lowercase()),
            "{bound}"
        );
        assert!(opened.command(Command::SaveQuery), "the chord runs Save");
    }

    /// Ctrl S has to resolve to Save in the context the document reports
    /// while its editor holds the keyboard; in the Results context it is
    /// unbound, which is why Save never answered the chord before.
    #[gpui::test]
    fn ctrl_s_resolves_to_save_in_the_editor_context(cx: &mut TestAppContext) {
        use dbflux_app::keymap::{ContextId, KeyChord, Modifiers};

        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_text(THREE, "changed");

        let keymap = dbflux_ui_base::effective_keymap();
        let context = opened.read_cx(|doc, cx| doc.active_context(cx));
        assert_eq!(context, ContextId::TextInput);

        let save = KeyChord::new("s", Modifiers::primary());
        assert_eq!(keymap.resolve(context, &save), Some(Command::SaveQuery));
        assert_eq!(
            keymap.resolve(ContextId::Results, &save),
            None,
            "the bug: Results never bound Save"
        );

        // The command the chord resolves to saves.
        assert!(opened.command(Command::SaveQuery));
        assert_eq!(
            opened.written_values(),
            vec![(PLAINTEXT_SECRET.to_string(), "changed".to_string())]
        );
    }

    /// While the editor holds the keyboard its text keys are text: bare
    /// letters resolve to no command, so `e`, `j` and `k` insert, and Escape
    /// still leaves the editor.
    #[gpui::test]
    fn editor_keys_stay_text_and_escape_leaves(cx: &mut TestAppContext) {
        use dbflux_app::keymap::{KeyChord, Modifiers};

        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_text("", "");
        let keymap = dbflux_ui_base::effective_keymap();
        let context = opened.read_cx(|doc, cx| doc.active_context(cx));

        for key in ["e", "j", "k"] {
            assert_eq!(
                keymap.resolve(context, &KeyChord::new(key, Modifiers::none())),
                None,
                "{key} must not be a command while editing"
            );
        }
        assert_eq!(
            keymap.resolve(context, &KeyChord::new("escape", Modifiers::none())),
            Some(Command::Cancel)
        );

        opened.window.simulate_input("ejk");
        opened.window.run_until_parked();
        assert_eq!(opened.editor_text(), "ejk");
        assert!(opened.read_view(|view| view.is_editing_value()));

        assert!(opened.command(Command::Cancel), "Escape leaves the editor");
        opened.rendered_paths();
        let context = opened.read_cx(|doc, cx| doc.active_context(cx));
        assert_eq!(context, dbflux_app::keymap::ContextId::Results);
        assert!(
            opened.read_view(|view| view.is_editing_value()),
            "leaving is not discarding"
        );
    }

    #[gpui::test]
    fn the_footer_reports_the_edit_without_its_text(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.select(PLAINTEXT_SECRET);
        opened.update_view(|view, cx| view.retrieve_selected_value(cx));
        let version = opened
            .read_view(|view| match &view.value {
                ValueState::Loaded(value) => value.version.clone(),
                _ => None,
            })
            .expect("the fixture reports a version");
        opened.start_edit();
        opened.set_editor_text("abc\ndef");

        let footer = opened.footer_text();
        assert!(footer.contains("UTF-8"), "{footer}");
        assert!(footer.contains("7 B"), "bytes Save would write: {footer}");
        assert!(
            footer.contains(&crate::secret_store::view::details::short_version_id(
                &version
            )),
            "{footer}"
        );
        assert!(footer.contains("2 lines changed"), "{footer}");
        assert!(
            footer.contains("Ln 1, Col 1") || footer.contains("Ln 2, Col"),
            "{footer}"
        );
        assert!(
            !footer.contains("abc") && !footer.contains("def"),
            "{footer}"
        );

        opened.set_editor_text("abc\ndef");
        let editor =
            opened.read_view(|view| view.value_edit.as_ref().expect("editor").editor.clone());
        opened.window.update(|window, cx| {
            editor.update(cx, |state, cx| {
                state.set_cursor_position(lsp_types::Position::new(1, 2), window, cx)
            });
        });
        opened.window.run_until_parked();
        assert!(opened.footer_text().contains("Ln 2, Col 3"));
    }

    /// Find opens the component's panel over the text and leaves the value
    /// out of the accessibility tree and the rendered frame.
    #[gpui::test]
    fn find_opens_the_panel_without_exposing_the_value(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_text(THREE, THREE);
        let before = opened.rendered_paths().len();

        let doc = opened.doc.clone();
        opened.window.update(|window, cx| {
            doc.update(cx, |doc, cx| {
                doc.view
                    .update(cx, |view, cx| view.find_in_value(window, cx))
            })
        });
        opened.window.run_until_parked();

        let after = opened.rendered_paths();
        assert!(after.len() > before, "the find panel was drawn");
        let text = opened.accessibility_update_text();
        assert!(!text.contains("alpha-77b1"), "{text}");
        assert!(opened.exists("secret-value-editor-find"));
    }

    /// Find over a selection copies the selection into the panel's search
    /// field; that field must not carry it into the accessibility tree either.
    #[gpui::test]
    fn find_over_a_selection_leaves_the_value_out_of_the_tree(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_text(THREE, THREE);
        opened.window.simulate_keystrokes("ctrl-a");
        opened.window.run_until_parked();

        let doc = opened.doc.clone();
        opened.window.update(|window, cx| {
            doc.update(cx, |doc, cx| {
                doc.view
                    .update(cx, |view, cx| view.find_in_value(window, cx))
            })
        });
        opened.window.run_until_parked();

        // The selection reached the search field: the test exercises the
        // path that copies it there, not an empty panel.
        assert!(opened.find_fields().0.contains("alpha-77b1"));

        let text = opened.accessibility_update_text();
        assert!(!text.contains("alpha-77b1"), "{text}");
        assert!(opened.exists("secret-value-editor-find"));
    }

    /// Focus decides the context, not a flag a keystroke sets: after Escape
    /// leaves the editor, a click back into it restores the editor's chords
    /// with no key in between.
    #[gpui::test]
    fn clicking_back_into_the_editor_restores_ctrl_s(cx: &mut TestAppContext) {
        use dbflux_app::keymap::{ContextId, KeyChord, Modifiers};

        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_text(THREE, "changed");
        opened.rendered_paths();
        assert!(opened.command(Command::Cancel));
        // Focus events are delivered with the frame, as in the running app.
        opened.rendered_paths();
        assert_eq!(
            opened.read_cx(|doc, cx| doc.active_context(cx)),
            ContextId::Results,
            "Escape left the editor"
        );

        // A focus change that edits nothing: the textarea has no state
        // change to announce, so only focus can say the editor is back.
        let state =
            opened.read_view(|view| view.value_edit.as_ref().expect("editor").editor.clone());
        opened.window.update(|window, cx| {
            let handle = gpui::Focusable::focus_handle(state.read(cx), cx);
            window.focus(&handle, cx);
        });
        opened.window.run_until_parked();
        opened.rendered_paths();
        assert_eq!(
            opened.read_cx(|doc, cx| doc.active_context(cx)),
            dbflux_app::keymap::ContextId::TextInput,
            "focus alone restores the editor's context"
        );

        assert!(opened.command(Command::Cancel));
        opened.rendered_paths();
        let editor = opened.rect("secret-value-editor");
        opened
            .window
            .simulate_click(editor.center(), gpui::Modifiers::default());
        opened.window.run_until_parked();

        let context = opened.read_cx(|doc, cx| doc.active_context(cx));
        assert_eq!(context, ContextId::TextInput);
        let save = KeyChord::new("s", Modifiers::primary());
        let command = dbflux_ui_base::effective_keymap().resolve(context, &save);
        assert_eq!(command, Some(Command::SaveQuery));
        assert!(opened.command(Command::SaveQuery));
        assert_eq!(
            opened.written_values(),
            vec![(PLAINTEXT_SECRET.to_string(), "changed".to_string())]
        );
    }

    /// `Col` counts characters, not bytes or UTF-16 units: `ñ` is two bytes
    /// and an emoji is two UTF-16 units and four bytes, each one column.
    #[gpui::test]
    fn the_caret_column_counts_characters(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_text("x", "ñ😀x");
        let editor =
            opened.read_view(|view| view.value_edit.as_ref().expect("editor").editor.clone());
        opened.window.update(|window, cx| {
            editor.update(cx, |state, cx| {
                state.set_cursor_position(lsp_types::Position::new(0, 2), window, cx)
            });
        });
        opened.window.run_until_parked();

        let footer = opened.footer_text();
        assert!(footer.contains("Ln 1, Col 3"), "{footer}");
    }

    #[gpui::test]
    fn the_footer_counts_a_single_changed_line_in_the_singular(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_text(THREE, "alpha-77b1\nbeta-X\ngamma-77b1");
        assert!(opened.footer_text().contains("1 line changed"));
    }

    #[gpui::test]
    fn chrome_exposes_no_value_to_the_accessibility_tree(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.edit_text(
            r#"{"a":{"k":"alpha-77b1"}}"#,
            r#"{"a":{"k":"alpha-77b1"},"b":"beta-77b1"}"#,
        );

        for mode in [EditMode::Formatted, EditMode::Raw] {
            let doc = opened.doc.clone();
            opened.window.update(|window, cx| {
                doc.update(cx, |doc, cx| {
                    doc.view
                        .update(cx, |view, cx| view.set_edit_mode(mode, window, cx))
                })
            });
            opened.window.run_until_parked();

            let text = opened.accessibility_update_text();
            assert!(
                !text.contains("alpha-77b1") && !text.contains("beta-77b1"),
                "{text}"
            );
            assert!(text.contains("modified"), "the chip is announced: {text}");
            assert!(text.contains("UTF-8"), "the footer is announced: {text}");
        }
    }
}

// -- The `e` chord ------------------------------------------------------------

/// `e` is the keyboard path of Edit value: with a plaintext value loaded it
/// opens the editor on the stored text.
#[gpui::test]
fn the_edit_value_command_opens_the_editor_on_the_stored_text(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    assert!(opened.command(Command::EditValue));

    assert!(opened.read_view(|view| view.is_editing_value()));
    assert_eq!(opened.editor_text(), FAKE_SECRET_PLAINTEXT);
}

/// With nothing editable the command falls through and opens nothing: the
/// secret is managed by another service. A value that is not loaded is
/// retrieved first, like the pane action does.
#[gpui::test]
fn the_edit_value_command_falls_through_when_nothing_can_be_edited(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);

    opened.select(PLAINTEXT_SECRET);
    assert!(
        opened.command(Command::EditValue),
        "the value is not loaded: it is retrieved first"
    );
    assert!(opened.read_view(|view| view.is_editing_value()));

    opened.update_view(|view, _| view.value_edit = None);

    opened.select(MANAGED_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    assert!(!opened.command(Command::EditValue), "the secret is managed");
    assert!(!opened.read_view(|view| view.is_editing_value()));
}

/// The row menu's Edit value entry draws the `e` keycap the keymap binds:
/// the stored hint and a rendered node that holds exactly that text, which
/// the frame does not contain before the menu opens.
#[gpui::test]
fn the_row_menu_draws_the_e_keycap_on_edit_value(cx: &mut TestAppContext) {
    use super::context_menu::SecretMenuAction;

    fn keycaps(opened: &mut Opened<'_>) -> usize {
        let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(opened.window);
        let frame = capture.frame(opened.window);
        frame
            .nodes()
            .filter(|(_, node)| node.content_text() == "e")
            .count()
    }

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    let before = keycaps(&mut opened);

    opened.right_click(&format!("secret-row-{PLAINTEXT_SECRET}"));

    let shortcut = opened.read(|doc| {
        doc.context_menu
            .as_ref()
            .expect("menu open")
            .items
            .iter()
            .find(|item| matches!(item.action, SecretMenuAction::EditValue))
            .expect("the Edit value entry")
            .shortcut
            .as_ref()
            .map(|text| text.to_string())
    });

    assert_eq!(shortcut.as_deref(), Some("e"));
    assert_eq!(
        keycaps(&mut opened),
        before + 1,
        "the menu draws one more `e` keycap"
    );
}

/// An `e` press that can edit but has no connection to edit through is not
/// swallowed: it is reported with the same wording a save without a
/// connection uses, and opens nothing.
#[gpui::test]
fn the_edit_value_command_reports_a_missing_connection(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    let profile_id = opened.read(|doc| doc.profile_id);
    opened.update(|doc, cx| {
        doc.app_state.update(cx, |state, _| {
            state.connections_mut().remove(&profile_id);
        });
    });
    assert!(opened.read_view(|view| view.can_edit_value()));
    assert!(opened.reported_errors().is_empty());

    assert!(opened.command(Command::EditValue));

    assert!(!opened.read_view(|view| view.is_editing_value()));
    // The editor found the loss first; the history retry it triggers stays
    // quiet, so the loss is reported once.
    let reported = opened.reported_errors();
    assert_eq!(reported.len(), 1, "{reported:?}");
    assert!(
        reported[0].1.contains(&dbflux_i18n::t!(
            "document.secret_store.error.connection_unavailable"
        )),
        "{reported:?}"
    );
}

/// The button, the row menu and the pane action open the editor through
/// `start_value_edit` itself, so a missing connection is reported there, not
/// only for the `e` key.
#[gpui::test]
fn start_value_edit_reports_a_missing_connection(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    let profile_id = opened.read(|doc| doc.profile_id);
    opened.update(|doc, cx| {
        doc.app_state.update(cx, |state, _| {
            state.connections_mut().remove(&profile_id);
        });
    });
    assert!(opened.read_view(|view| view.can_edit_value()));

    opened.start_edit();

    assert!(!opened.read_view(|view| view.is_editing_value()));
    // The editor found the loss first; the history retry it triggers stays
    // quiet, so the loss is reported once.
    let reported = opened.reported_errors();
    assert_eq!(reported.len(), 1, "{reported:?}");
    assert!(
        reported[0].1.contains(&dbflux_i18n::t!(
            "document.secret_store.error.connection_unavailable"
        )),
        "{reported:?}"
    );
}

// -- Version history ---------------------------------------------------------

const PREVIOUS_PASSWORD: &str = "fake-previous-2b8d";
const OLDEST_PASSWORD: &str = "fake-oldest-90e1";
const HISTORY_PARAMETER: &str = "/app/prod/db/host";

impl Opened<'_> {
    /// Runs the pane action `id` the way the actions menu does.
    fn run_pane_action(&mut self, id: &str) {
        let doc = self.doc.clone();
        let action = self
            .window
            .update(|_, cx| doc.read(cx).pane_actions(&doc, cx))
            .into_iter()
            .find(|action| action.id.as_ref() == id)
            .unwrap_or_else(|| panic!("`{id}` is a pane action"));

        assert!(action.enabled, "`{id}` is enabled");

        match action.run {
            crate::pane::PaneActionRun::Callback(run) => {
                self.window.update(|window, cx| run(window, cx));
            }
            crate::pane::PaneActionRun::Command(_) => panic!("`{id}` is a callback"),
        }
        self.window.run_until_parked();
    }

    fn version_ids(&mut self) -> Vec<String> {
        self.read_view(|view| match view.versions() {
            VersionsState::Loaded(versions) => {
                versions.iter().map(|version| version.id.clone()).collect()
            }
            other => panic!("versions not loaded: {other:?}"),
        })
    }

    fn shown_password(&mut self) -> Option<String> {
        self.read_view(|view| match view.value() {
            ValueState::Loaded(value) => value
                .fields()
                .iter()
                .find(|(key, _)| *key == Some("password"))
                .map(|(_, field)| field.expose_secret().to_string()),
            _ => None,
        })
    }
}

impl Opened<'_> {
    /// The text and top edge of the drawn element `element_id`.
    fn element_text_and_top(&mut self, element_id: &str) -> (String, f32) {
        let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(self.window);
        let frame = capture.frame(self.window);

        frame
            .nodes()
            .find(|(_, node)| node.id() == element_id)
            .map(|(_, node)| {
                (
                    node.content_text().to_string(),
                    f32::from(node.bounds().origin.y),
                )
            })
            .unwrap_or_else(|| panic!("`{element_id}` is drawn"))
    }

    /// The text of the version rows, top to bottom, asserting that they are
    /// drawn in index order.
    fn version_rows(&mut self, count: usize) -> Vec<String> {
        let rows: Vec<(String, f32)> = (0..count)
            .map(|index| self.element_text_and_top(&format!("secret-version-row-{index}")))
            .collect();

        assert!(
            rows.windows(2).all(|pair| pair[0].1 < pair[1].1),
            "rows are drawn top to bottom in index order: {rows:?}"
        );
        rows.into_iter().map(|(text, _)| text).collect()
    }
}

/// The Versions section lists what the store reported, newest first, with the
/// id truncated, the labels as chips and "no labels" for a version without.
#[gpui::test]
fn the_versions_section_lists_the_versions_newest_first_with_labels(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);

    assert_eq!(
        opened.version_ids(),
        vec!["v-current", "v-previous", "v-oldest"]
    );
    assert_eq!(
        opened.driver.secret_store_calls().version_listings,
        vec![(
            FAKE_SECRETS_STORE_ID.to_string(),
            KEY_VALUE_SECRET.to_string()
        )]
    );

    let rows = opened.version_rows(3);
    for (row, expected) in rows.iter().zip([
        ["v-curren…", "AWSCURRENT"],
        ["v-previo…", "AWSPREVIOUS"],
        ["v-oldest", "no labels"],
    ]) {
        for text in expected {
            assert!(row.contains(text), "`{text}` is in its row: {row}");
        }
    }
    assert!(!rows[2].contains("AWS"), "{}", rows[2]);

    let text = opened.rendered_text();
    for expected in ["VERSIONS", "3 versions", "Staging labels", "Created"] {
        assert!(text.contains(expected), "`{expected}` is drawn: {text}");
    }

    let paths = opened.rendered_paths();
    for id in [
        "secret-versions",
        "secret-version-row-0",
        "secret-version-row-1",
        "secret-version-row-2",
    ] {
        assert!(
            paths.iter().any(|path| path.ends_with(id)),
            "`{id}` is drawn: {paths:?}"
        );
    }
    assert!(
        !text.contains(PREVIOUS_PASSWORD) && !text.contains(FAKE_SECRET_PASSWORD),
        "a history row holds no value"
    );
}

/// A store without staging labels draws its history as `vN`, the principal
/// and the date, and no label column.
#[gpui::test]
fn the_history_section_lists_the_version_the_author_and_the_date(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    opened.select(HISTORY_PARAMETER);

    assert_eq!(opened.version_ids(), vec!["3", "2", "1"]);

    let rows = opened.version_rows(3);
    let date = |month: u32| {
        super::render::format_time(
            dbflux_core::chrono::TimeZone::with_ymd_and_hms(
                &dbflux_core::chrono::Utc,
                2026,
                month,
                1,
                10,
                0,
                0,
            )
            .single()
            .expect("a valid date"),
        )
    };
    for (row, expected) in rows.iter().zip([
        ["v3", "arn:fake:user/deployer", date(9).as_str()],
        ["v2", "arn:fake:user/ana", date(8).as_str()],
        ["v1", "arn:fake:user/deployer", date(7).as_str()],
    ]) {
        for text in expected {
            assert!(row.contains(text), "`{text}` is in its row: {row}");
        }
    }

    let text = opened.rendered_text();
    for expected in ["HISTORY", "3 versions"] {
        assert!(text.contains(expected), "`{expected}` is drawn: {text}");
    }
    assert!(
        !text.contains("Staging labels") && !text.contains("no labels"),
        "a store without labels has no label column: {text}"
    );
}

/// A store that does not advertise VERSIONS shows no section and is never
/// asked for a history.
#[gpui::test]
fn a_store_without_versions_shows_no_section(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.update(|doc, cx| {
        let store = SecretStoreDescriptor::new(
            FAKE_SECRETS_STORE_ID,
            "Secrets Manager",
            SecretStoreCapabilities::empty(),
        );
        doc.store = store.clone();
        doc.view.update(cx, |view, _| view.store = store);
    });
    opened.select(KEY_VALUE_SECRET);

    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::NotSupported)));
    assert!(
        opened
            .driver
            .secret_store_calls()
            .version_listings
            .is_empty()
    );
    assert!(
        !opened
            .rendered_paths()
            .iter()
            .any(|path| path.contains("secret-version")),
        "no versions section is drawn"
    );
    assert!(
        !opened
            .pane_action_ids()
            .iter()
            .any(|id| id.starts_with("secret-version")),
        "no versions entry is offered"
    );
}

/// Activating a version that is not the current one reads exactly that
/// version through the retrieval path, audits it like any retrieval, names
/// the version in the value section and keeps the value out of every
/// observable surface.
#[gpui::test]
fn activating_an_older_version_retrieves_exactly_that_version(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);

    opened.update_view(|view, cx| view.activate_version(1, cx));

    assert_eq!(
        opened.driver.secret_store_calls().value_version_requests,
        vec![(
            FAKE_SECRETS_STORE_ID.to_string(),
            KEY_VALUE_SECRET.to_string(),
            Some("v-previous".to_string())
        )]
    );
    assert_eq!(
        opened.read_view(|view| view.value_version().map(str::to_string)),
        Some("v-previous".to_string())
    );
    assert_eq!(opened.shown_password().as_deref(), Some(PREVIOUS_PASSWORD));

    let events = opened.audit_events();
    assert_eq!(events.len(), 1, "{events:?}");
    assert!(events[0].contains(VALUE_RETRIEVED_ACTION), "{}", events[0]);
    assert!(events[0].contains("v-previous"), "{}", events[0]);
    assert!(!events[0].contains(PREVIOUS_PASSWORD), "{}", events[0]);
    assert!(!events[0].contains("api-user"), "{}", events[0]);

    let text = opened.rendered_text();
    assert!(text.contains("Showing version v-previous"), "{text}");
    assert!(text.contains("Back to current"), "{text}");
    assert!(
        opened
            .rendered_paths()
            .iter()
            .any(|path| path.ends_with("secret-version-back-to-current"))
    );
}

/// The current version's row names its own id, because the current version
/// can rotate between the listing and the click. Once the pane learns of the
/// rotation, the old payload on screen is an older value: it shows its
/// version and the way back, and Back to current reads the new current one.
#[gpui::test]
fn activating_the_current_version_retrieves_the_current_value(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    opened.rotate_current(KEY_VALUE_SECRET, "rotated after the history loaded");

    opened.update_view(|view, cx| view.activate_version(0, cx));

    assert_eq!(
        opened.driver.secret_store_calls().value_version_requests,
        vec![(
            FAKE_SECRETS_STORE_ID.to_string(),
            KEY_VALUE_SECRET.to_string(),
            Some("v-current".to_string())
        )]
    );
    assert_eq!(
        opened.shown_password().as_deref(),
        Some(FAKE_SECRET_PASSWORD),
        "the selected row's payload, not the rotated one"
    );
    assert_eq!(
        opened.read_view(|view| view.value_version().map(str::to_string)),
        Some("v-current".to_string()),
        "the retrieved version id is kept"
    );
    assert!(
        !opened.rendered_text().contains("Back to current"),
        "as far as the pane knows the row is still current"
    );

    opened.update_view(|view, cx| {
        view.describe(KEY_VALUE_SECRET.to_string(), cx);
        view.load_versions(KEY_VALUE_SECRET.to_string(), cx);
    });

    let text = opened.rendered_text();
    assert!(text.contains("Showing version v-current"), "{text}");
    assert!(text.contains("Back to current"), "{text}");
    assert!(opened.read_view(|view| view.can_return_to_current()));
    assert_eq!(
        opened.shown_password().as_deref(),
        Some(FAKE_SECRET_PASSWORD)
    );

    opened.update_view(|view, cx| view.back_to_current_version(cx));
    assert!(opened.read_view(|view| view.value_version().is_none()));
    assert!(!opened.rendered_text().contains("Back to current"));
}

/// Without a rotation, the current version's row shows no older-value banner.
#[gpui::test]
fn the_current_row_shows_no_older_value_banner(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);

    opened.update_view(|view, cx| view.activate_version(0, cx));

    let text = opened.rendered_text();
    assert!(!text.contains("Showing version"), "{text}");
    assert!(!text.contains("Back to current"), "{text}");
    assert!(!opened.read_view(|view| view.can_return_to_current()));
}

/// A describe that lands late with another version id neither moves the
/// highlight nor drops the banner of the version that was retrieved.
#[gpui::test]
fn a_late_describe_does_not_change_the_version_shown(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.activate_version(1, cx));

    let mut details = opened.read_view(|view| match view.details() {
        DetailsState::Loaded(details) => (**details).clone(),
        other => panic!("{other:?}"),
    });
    details.summary.version = Some("v-oldest".to_string());
    let generation = opened.read_view(|view| view.details_generation);
    opened.update_view(|view, cx| {
        view.apply_details(generation, KEY_VALUE_SECRET, Ok(details), 0, cx)
    });

    assert_eq!(
        opened.read_view(|view| view.value_version().map(str::to_string)),
        Some("v-previous".to_string())
    );
    let text = opened.rendered_text();
    assert!(text.contains("Showing version v-previous"), "{text}");
    assert_eq!(opened.shown_password().as_deref(), Some(PREVIOUS_PASSWORD));
}

/// Only the store's own markers name the current version: with neither a
/// described version nor a flagged history row there is none, whatever the
/// listing or the first row says.
#[gpui::test]
fn the_current_version_is_never_guessed(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);

    opened.update(|doc, cx| {
        for entry in &mut doc.listing.entries {
            entry.version = Some("listing-version".to_string());
        }
        doc.view.update(cx, |view, _| {
            view.details = DetailsState::Loading;
            if let VersionsState::Loaded(versions) = &mut view.versions {
                for version in versions.iter_mut() {
                    version.is_current = false;
                }
            }
        });
    });

    assert_eq!(
        opened.read_view(|view| view.current_version_id().map(str::to_string)),
        None
    );
}

/// Back to current is one action: it retrieves the current value, drops the
/// version banner and audits the retrieval.
#[gpui::test]
fn back_to_current_returns_to_the_current_value_in_one_action(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.activate_version(2, cx));
    assert_eq!(opened.shown_password().as_deref(), Some(OLDEST_PASSWORD));

    opened.update_view(|view, cx| view.back_to_current_version(cx));

    assert_eq!(
        opened.shown_password().as_deref(),
        Some(FAKE_SECRET_PASSWORD)
    );
    assert_eq!(
        opened.read_view(|view| view.value_version().map(str::to_string)),
        None
    );
    let text = opened.rendered_text();
    assert!(!text.contains("Back to current"), "{text}");
    assert!(!text.contains("Showing version"), "{text}");

    let requests = opened.driver.secret_store_calls().value_version_requests;
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert_eq!(requests[1].2, None);
    assert_eq!(opened.audit_events().len(), 2);
}

/// A history that lands for a secret the user already left, or from a load a
/// newer one replaced, never renders.
#[gpui::test]
fn a_stale_versions_response_does_not_render(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();
    opened.select(KEY_VALUE_SECRET);
    let stale_generation = opened.read_view(|view| view.versions_generation);
    opened.select(PLAINTEXT_SECRET);

    let foreign = vec![dbflux_core::SecretVersionInfo {
        id: "stale-version-id".to_string(),
        ..Default::default()
    }];

    opened.update_view(|view, cx| {
        view.apply_versions(stale_generation, KEY_VALUE_SECRET, Ok(foreign.clone()), cx);
    });
    let current_generation = opened.read_view(|view| view.versions_generation);
    opened.update_view(|view, cx| {
        view.apply_versions(
            current_generation,
            KEY_VALUE_SECRET,
            Ok(foreign.clone()),
            cx,
        );
    });
    opened.update_view(|view, cx| {
        view.apply_versions(
            stale_generation,
            PLAINTEXT_SECRET,
            Err(DbError::query_failed("stale failure")),
            cx,
        );
    });

    // The load the selection actually started is the one that shows.
    assert_eq!(opened.version_ids(), vec!["v-current"]);

    let text = opened.rendered_text();
    assert!(!text.contains("stale-ve"), "{text}");
    assert!(!text.contains("stale failure"), "{text}");
    assert!(opened.read_view(|view| !matches!(view.versions(), VersionsState::Failed(_))));

    // Selection change and refresh both drop the history and invalidate its
    // load.
    opened.select(KEY_VALUE_SECRET);
    let before = opened.read_view(|view| view.versions_generation);
    opened.update(|doc, cx| doc.refresh(cx));
    assert_ne!(opened.read_view(|view| view.versions_generation), before);
    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::Idle)));
}

/// A refused history says why and leaves the value section alone: no value,
/// and the secret still retrieves.
#[gpui::test]
fn a_failed_history_shows_the_reason_and_no_value(cx: &mut TestAppContext) {
    let driver = FakeDriver::new(DbKind::AwsSecrets).with_secret_versions_error(
        KEY_VALUE_SECRET,
        "not authorized to perform ListSecretVersionIds",
    );
    let mut opened = open_with(cx, driver, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);

    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::Failed(_))));
    let text = opened.rendered_text();
    assert!(text.contains("The versions could not be loaded"), "{text}");
    assert!(
        text.contains("not authorized to perform ListSecretVersionIds"),
        "{text}"
    );
    assert!(!text.contains(FAKE_SECRET_PASSWORD), "{text}");
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::NotLoaded)));
    assert!(opened.read_view(|view| matches!(view.details(), DetailsState::Loaded(_))));

    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    assert_eq!(
        opened.shown_password().as_deref(),
        Some(FAKE_SECRET_PASSWORD)
    );
}

/// A real click on a version row retrieves that version.
#[gpui::test]
fn clicking_a_version_row_retrieves_that_version(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);

    opened.click_menu_item("secret-version-row-1");

    assert_eq!(
        opened.driver.secret_store_calls().value_version_requests,
        vec![(
            FAKE_SECRETS_STORE_ID.to_string(),
            KEY_VALUE_SECRET.to_string(),
            Some("v-previous".to_string())
        )]
    );

    opened.click_menu_item("secret-version-back-to-current");
    assert_eq!(
        opened.shown_password().as_deref(),
        Some(FAKE_SECRET_PASSWORD)
    );
}

/// The keyboard reaches both controls through the pane actions.
#[gpui::test]
fn the_pane_actions_retrieve_a_version_and_go_back(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);

    let ids = opened.pane_action_ids();
    for expected in [
        "secret-version-retrieve-0",
        "secret-version-retrieve-1",
        "secret-version-retrieve-2",
        "secret-version-current",
    ] {
        assert!(ids.iter().any(|id| id == expected), "{expected}: {ids:?}");
    }

    let actions = opened
        .window
        .update(|_, cx| opened.doc.read(cx).pane_actions(&opened.doc, cx));
    let back = actions
        .iter()
        .find(|action| action.id.as_ref() == "secret-version-current")
        .expect("listed");
    assert!(!back.enabled, "nothing to go back from yet");

    opened.run_pane_action("secret-version-retrieve-2");
    assert_eq!(opened.shown_password().as_deref(), Some(OLDEST_PASSWORD));

    opened.run_pane_action("secret-version-current");
    assert_eq!(
        opened.shown_password().as_deref(),
        Some(FAKE_SECRET_PASSWORD)
    );
}

/// Every control of the history has a keyboard path in the registry.
#[gpui::test]
fn the_version_controls_are_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::SECRET_STORE;
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};

    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.activate_version(1, cx));

    let entries = opened.pane_action_ids();
    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries)
        .assert_covered(&capture.frame(opened.window));

    for expected in ["secret-version-row-1", "secret-version-back-to-current"] {
        assert!(
            checked.iter().any(|id| id == expected),
            "`{expected}` is checked: {checked:?}"
        );
    }
}

/// The accessibility tree and the rendered frame hold no value of any
/// version, masked or revealed.
#[gpui::test]
fn version_values_never_reach_the_accessibility_tree(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);

    for (index, shown) in [
        (1, PREVIOUS_PASSWORD),
        (2, OLDEST_PASSWORD),
        (0, FAKE_SECRET_PASSWORD),
    ] {
        opened.update_view(|view, cx| view.activate_version(index, cx));
        assert_eq!(
            opened.shown_password().as_deref(),
            Some(shown),
            "the probe runs against a loaded value"
        );
        let masked = opened.accessibility_update_text();
        assert!(opened.command(Command::ExpandCollapse));
        let revealed = opened.accessibility_update_text();

        for secret in [
            FAKE_SECRET_PASSWORD,
            PREVIOUS_PASSWORD,
            OLDEST_PASSWORD,
            "api-user",
        ] {
            assert!(
                !masked.contains(secret),
                "version {index}: masked holds {secret}"
            );
            assert!(
                !revealed.contains(secret),
                "version {index}: revealed holds {secret}"
            );
        }
    }
}

// -- Version history review fixes ---------------------------------------------

impl Opened<'_> {
    fn live_connection(&mut self) -> Arc<dyn Connection> {
        let app_state = self.app_state.clone();
        let profile_id = self.read(|doc| doc.profile_id);
        self.window.update(|_, cx| {
            app_state
                .read(cx)
                .connections()
                .get(&profile_id)
                .expect("connected")
                .connection
                .clone()
        })
    }

    /// Writes a new current version straight to the fake store, the way a
    /// rotation or another client does, without telling the pane.
    fn rotate_current(&mut self, name: &str, text: &str) {
        let connection = self.live_connection();
        connection
            .secret_store(FAKE_SECRETS_STORE_ID)
            .expect("store")
            .put_value(&SecretPutRequest::text(name, text))
            .expect("rotate");
    }

    /// Replaces the profile's live connection with a fresh session of the
    /// same shape, and returns the driver behind it.
    fn replace_connection(&mut self) -> FakeDriver {
        let replacement_driver = FakeDriver::new(DbKind::AwsSecrets);
        let profile = dbflux_core::ConnectionProfile::new(
            "prod-aws",
            dbflux_core::DbConfig::SQLite {
                path: std::path::PathBuf::from(":memory:"),
                connection_id: None,
            },
        );
        let replacement = replacement_driver.connect_arc(&profile).expect("connect");
        let app_state = self.app_state.clone();
        let profile_id = self.read(|doc| doc.profile_id);

        self.window.update(|_, cx| {
            app_state.update(cx, |state, _| {
                if let Some(connected) = state.connections_mut().get_mut(&profile_id) {
                    connected.connection = replacement.clone();
                }
            });
        });

        replacement_driver
    }

    fn announce_state_change(&mut self) {
        let app_state = self.app_state.clone();
        self.window
            .update(|_, cx| app_state.update(cx, |_, cx| cx.emit(AppStateChanged)));
        self.window.run_until_parked();
    }

    fn pane_action(&mut self, id: &str) -> crate::pane::PaneAction {
        let doc = self.doc.clone();
        self.window
            .update(|_, cx| doc.read(cx).pane_actions(&doc, cx))
            .into_iter()
            .find(|action| action.id.as_ref() == id)
            .unwrap_or_else(|| panic!("`{id}` is a pane action"))
    }

    fn run_action(&mut self, action: crate::pane::PaneAction) {
        match action.run {
            crate::pane::PaneActionRun::Callback(run) => {
                self.window.update(|window, cx| run(window, cx));
            }
            crate::pane::PaneActionRun::Command(_) => panic!("a callback action"),
        }
        self.window.run_until_parked();
    }

    /// Leaves the history failed, so no version is known to be current while
    /// the shown value stays.
    fn fail_history(&mut self) {
        self.update_view(|view, cx| {
            view.versions = VersionsState::Failed("history unavailable".to_string());
            cx.notify();
        });
    }

    fn strip_reported_versions(&mut self) {
        self.update(|doc, cx| {
            doc.view.update(cx, |view, _| {
                if let DetailsState::Loaded(details) = &mut view.details {
                    details.summary.version = None;
                }
            });
            for entry in &mut doc.listing.entries {
                entry.version = None;
            }
            cx.notify();
        });
    }
}

/// Without a version in the describe or the listing, the current version is
/// the one the history marks: the `AWSCURRENT` row on a store with labels
/// even when a pending version heads the list, the highest number otherwise.
#[gpui::test]
fn the_current_version_falls_back_to_the_history(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    assert_eq!(
        opened.read_view(|view| view.current_version_id().map(str::to_string)),
        Some("v-current".to_string()),
        "the describe's version wins when present"
    );

    let connection = opened.live_connection();
    connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("store")
        .put_value(
            &SecretPutRequest::text(KEY_VALUE_SECRET, "staged")
                .with_stage(SecretVersionStage::Pending),
        )
        .expect("pending write");
    opened.update_view(|view, cx| view.load_versions(KEY_VALUE_SECRET.to_string(), cx));
    assert_eq!(opened.version_ids()[0], "v-written-1");
    opened.strip_reported_versions();

    assert_eq!(
        opened.read_view(|view| view.current_version_id().map(str::to_string)),
        Some("v-current".to_string())
    );
    assert!(opened.read_view(|view| view.is_current_version("v-current")));
}

#[gpui::test]
fn the_current_parameter_version_falls_back_to_the_highest_number(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    opened.select(HISTORY_PARAMETER);
    opened.strip_reported_versions();

    assert_eq!(
        opened.read_view(|view| view.current_version_id().map(str::to_string)),
        Some("3".to_string())
    );
}

/// Selecting a version or going back to current while the editor holds
/// unsaved text is refused with the unsaved-edits warning, and the editor
/// keeps its text and no store call is made.
#[gpui::test]
fn version_transitions_are_refused_under_unsaved_edits(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);
    let requests_before = opened
        .driver
        .secret_store_calls()
        .value_version_requests
        .len();

    opened.update_view(|view, cx| view.activate_version(0, cx));
    opened.update_view(|view, cx| {
        view.value_version = Some("older".to_string());
        view.back_to_current_version(cx);
    });

    assert_eq!(opened.editor_text(), EDITED_VALUE);
    assert!(opened.read_view(|view| view.is_editing_value()));
    assert_eq!(
        opened
            .driver
            .secret_store_calls()
            .value_version_requests
            .len(),
        requests_before
    );
    let errors = opened.reported_errors();
    assert_eq!(errors.len(), 2, "{errors:?}");
    for (_, summary) in &errors {
        assert!(summary.contains("unsaved edits"), "{summary}");
    }

    opened.update_view(|view, cx| view.discard_value_edit(cx));
    opened.update_view(|view, cx| view.activate_version(0, cx));
    assert_eq!(
        opened
            .driver
            .secret_store_calls()
            .value_version_requests
            .len(),
        requests_before + 1,
        "without edits the transition runs"
    );
}

/// The history stays on screen while the describe is slow or has failed.
#[gpui::test]
fn the_versions_section_renders_without_the_describe(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);

    for state in [
        DetailsState::Loading,
        DetailsState::Failed("describe refused".to_string()),
    ] {
        opened.update_view(|view, cx| {
            view.details = state;
            cx.notify();
        });
        let rows = opened.version_rows(3);
        assert_eq!(rows.len(), 3);
    }

    opened.update_view(|view, cx| {
        view.versions = VersionsState::Loading;
        cx.notify();
    });
    assert!(opened.rendered_text().contains("describe refused"));
    assert!(
        opened
            .rendered_paths()
            .iter()
            .any(|path| path.ends_with("secret-versions"))
    );

    opened.update_view(|view, cx| {
        view.versions = VersionsState::Failed("history refused".to_string());
        cx.notify();
    });
    assert!(opened.rendered_text().contains("history refused"));
}

/// While a historical retrieval is in flight, Back to current is not
/// available, in the banner or the pane actions, and no value shows.
#[gpui::test]
fn back_to_current_is_unavailable_while_a_historical_retrieval_is_in_flight(
    cx: &mut TestAppContext,
) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| {
        view.value_version = Some("v-previous".to_string());
        view.value = ValueState::Loading;
        cx.notify();
    });

    assert!(!opened.read_view(|view| view.can_return_to_current()));
    assert!(!opened.pane_action("secret-version-current").enabled);
    assert!(opened.shown_password().is_none());

    opened.update_view(|view, cx| view.back_to_current_version(cx));
    assert!(
        opened
            .driver
            .secret_store_calls()
            .value_version_requests
            .is_empty()
    );

    opened.update_view(|view, cx| {
        view.value = ValueState::NotLoaded;
        cx.notify();
    });
    opened.update_view(|view, cx| view.activate_version(1, cx));
    assert!(opened.read_view(|view| view.can_return_to_current()));
    assert!(opened.pane_action("secret-version-current").enabled);
}

/// A replaced connection — even in the same region — invalidates the
/// history: a response of the old session is dropped, the history is loaded
/// again from the new session, also while unsaved edits block the listing
/// refresh.
#[gpui::test]
fn a_replaced_connection_reloads_the_history(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);
    let old_generation = opened.read_view(|view| view.versions_generation);

    let replacement = opened.replace_connection();
    opened.announce_state_change();

    assert_eq!(
        replacement.secret_store_calls().version_listings,
        vec![(
            FAKE_SECRETS_STORE_ID.to_string(),
            PLAINTEXT_SECRET.to_string()
        )]
    );
    assert!(
        opened.read_view(|view| view.is_editing_value()),
        "the edit is kept"
    );

    let foreign = vec![dbflux_core::SecretVersionInfo {
        id: "old-session-version".to_string(),
        ..Default::default()
    }];
    opened.update_view(|view, cx| {
        view.apply_versions(old_generation, PLAINTEXT_SECRET, Ok(foreign), cx);
    });
    assert!(
        !opened
            .version_ids()
            .contains(&"old-session-version".to_string())
    );
}

/// A history listed from the old session is never retrieved against the new
/// connection, even before the pane heard about the replacement.
#[gpui::test]
fn a_version_of_a_replaced_session_is_not_retrieved(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    let replacement = opened.replace_connection();

    opened.update_view(|view, cx| view.activate_version(1, cx));

    assert!(
        replacement
            .secret_store_calls()
            .value_version_requests
            .is_empty()
    );
    assert!(
        opened
            .driver
            .secret_store_calls()
            .value_version_requests
            .is_empty()
    );
    assert_eq!(replacement.secret_store_calls().version_listings.len(), 1);
    assert_eq!(opened.version_ids().len(), 3);
}

/// A landed save makes the saved text the current version: the pane stops
/// showing a historical one, and the history is listed again with the new
/// version; a listing still in flight is invalidated.
#[gpui::test]
fn a_save_reloads_the_history_and_clears_the_shown_version(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));
    opened.update_view(|view, _cx| view.value_version = Some("older".to_string()));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);
    let generation_before = opened.read_view(|view| view.versions_generation);

    opened.save_edit();

    assert_eq!(
        opened.read_view(|view| view.value_version().map(str::to_string)),
        None
    );
    assert_ne!(
        opened.read_view(|view| view.versions_generation),
        generation_before
    );
    assert_eq!(opened.version_ids()[0], "v-written-1");
    assert_eq!(opened.driver.secret_store_calls().version_listings.len(), 2);

    let foreign = vec![dbflux_core::SecretVersionInfo {
        id: "in-flight-version".to_string(),
        ..Default::default()
    }];
    opened.update_view(|view, cx| {
        view.apply_versions(generation_before, PLAINTEXT_SECRET, Ok(foreign), cx);
    });
    assert_eq!(opened.version_ids()[0], "v-written-1");
}

/// A menu built before the history changed does not activate a row of the
/// new history.
#[gpui::test]
fn a_stale_pane_action_does_not_activate_a_version(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    let action = opened.pane_action("secret-version-retrieve-2");

    opened.select(PLAINTEXT_SECRET);
    opened.select(KEY_VALUE_SECRET);
    opened.run_action(action);

    assert!(
        opened
            .driver
            .secret_store_calls()
            .value_version_requests
            .is_empty()
    );

    let fresh = opened.pane_action("secret-version-retrieve-2");
    opened.run_action(fresh);
    assert_eq!(opened.shown_password().as_deref(), Some(OLDEST_PASSWORD));
}

/// A response of the old session that lands after the connection was
/// replaced, with its generation still current because the pane has not
/// heard of the replacement, is dropped and the history is listed again.
#[gpui::test]
fn a_history_response_of_a_replaced_session_is_dropped(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    let replacement = opened.replace_connection();
    let generation = opened.read_view(|view| view.versions_generation);
    let foreign = vec![dbflux_core::SecretVersionInfo {
        id: "old-session-version".to_string(),
        ..Default::default()
    }];

    opened
        .update_view(|view, cx| view.apply_versions(generation, KEY_VALUE_SECRET, Ok(foreign), cx));

    assert!(
        !opened
            .version_ids()
            .contains(&"old-session-version".to_string())
    );
    assert_eq!(replacement.secret_store_calls().version_listings.len(), 1);
}

/// A history failure lands inline in the section and is recorded (audit
/// trail, notification center) without a toast, and only when it still
/// applies: a superseded request's failure is dropped.
#[gpui::test]
fn a_history_failure_shows_inline_only_when_it_applies(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    let stale = opened
        .read_view(|view| view.versions_generation)
        .wrapping_sub(1);
    let current = opened.read_view(|view| view.versions_generation);

    opened.update_view(|view, cx| {
        view.apply_versions(
            stale,
            KEY_VALUE_SECRET,
            Err(DbError::query_failed("stale")),
            cx,
        );
    });
    assert!(opened.reported_errors().is_empty());
    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::Loaded(_))));

    opened.update_view(|view, cx| {
        view.apply_versions(
            current,
            KEY_VALUE_SECRET,
            Err(DbError::query_failed("denied")),
            cx,
        );
    });
    assert_eq!(opened.reported_errors().len(), 1, "recorded once");
    assert_eq!(opened.toast_count(), 0, "and not as a toast");
    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::Failed(_))));
}

/// A refused history shows inline, is recorded once and raises no toast.
#[gpui::test]
fn a_refused_history_is_recorded_without_a_toast(cx: &mut TestAppContext) {
    let driver = FakeDriver::new(DbKind::AwsSecrets)
        .with_secret_versions_error(KEY_VALUE_SECRET, "not authorized");
    let mut opened = open_with(cx, driver, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);

    assert_eq!(opened.reported_errors().len(), 1);
    assert_eq!(opened.toast_count(), 0);
    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::Failed(_))));
}

/// A connection that went away and came back (here with a new session)
/// retries the selected secret's history instead of staying failed.
#[gpui::test]
fn the_history_is_retried_when_the_connection_returns(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    let profile_id = opened.read(|doc| doc.profile_id);
    let app_state = opened.app_state.clone();
    let removed = opened.window.update(|_, cx| {
        app_state.update(cx, |state, _| state.connections_mut().remove(&profile_id))
    });
    opened.announce_state_change();
    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::NotConnected)));
    assert!(opened.reported_errors().is_empty());
    assert_eq!(opened.toast_count(), 0);

    let replacement_driver = FakeDriver::new(DbKind::AwsSecrets);
    let mut connected = removed.expect("was connected");
    connected.connection = replacement_driver
        .connect_arc(&connected.profile)
        .expect("connect");
    opened.window.update(|_, cx| {
        app_state.update(cx, |state, _| {
            state.connections_mut().insert(profile_id, connected);
        });
    });
    opened.announce_state_change();

    assert_eq!(
        replacement_driver
            .secret_store_calls()
            .version_listings
            .len(),
        1
    );
    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::Loaded(_))));

    opened.announce_state_change();
    assert_eq!(
        replacement_driver
            .secret_store_calls()
            .version_listings
            .len(),
        1,
        "a live session is not reloaded"
    );
}

/// Back to current stays in the pane actions whatever the history is doing,
/// enabled by the same rule as the banner button.
#[gpui::test]
fn back_to_current_stays_a_pane_action_while_the_history_loads_or_fails(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.activate_version(1, cx));

    for state in [
        VersionsState::Loading,
        VersionsState::Failed("history refused".to_string()),
    ] {
        opened.update_view(|view, _cx| view.versions = state);
        let action = opened.pane_action("secret-version-current");
        assert!(action.enabled, "a historical value is shown");
    }
}

/// A history retry that finds no connection is a disconnect, not a failure:
/// it reports nothing however often the state changes.
#[gpui::test]
fn a_history_retry_without_a_connection_reports_nothing(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    assert!(opened.reported_errors().is_empty());

    let profile_id = opened.read(|doc| doc.profile_id);
    let app_state = opened.app_state.clone();
    opened.window.update(|_, cx| {
        app_state.update(cx, |state, _| state.connections_mut().remove(&profile_id));
    });
    opened.announce_state_change();
    opened.announce_state_change();

    let errors = opened.reported_errors();
    assert!(errors.is_empty(), "{errors:?}");
    assert_eq!(opened.toast_count(), 0);
    assert!(opened.audit_records().is_empty(), "no audit row");
    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::NotConnected)));
}

/// A historical retrieval that fails replaces the loaded historical value
/// with its error: neither the older-value banner nor Back to current stays.
#[gpui::test]
fn a_failed_historical_retrieval_shows_no_older_value_banner(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.activate_version(1, cx));

    assert_eq!(opened.shown_password().as_deref(), Some(PREVIOUS_PASSWORD));
    let text = opened.rendered_text();
    assert!(text.contains("Showing version"), "{text}");
    assert!(opened.read_view(|view| view.can_return_to_current()));

    // The fake store has no such version, so the retrieval fails.
    opened.update_view(|view, cx| view.retrieve_version(Some("v-vanished".to_string()), cx));

    assert_eq!(opened.shown_password(), None);
    assert!(opened.read_view(|view| matches!(view.value(), ValueState::Failed(_))));
    let text = opened.rendered_text();
    assert!(text.contains("no version v-vanished"), "{text}");
    assert!(!text.contains("Showing version"), "{text}");
    assert!(!text.contains("Back to current"), "{text}");
    assert!(!opened.read_view(|view| view.can_return_to_current()));
    assert!(!opened.pane_action("secret-version-current").enabled);
}

/// A history that failed while an older version is shown leaves the current
/// version unknown; the older value still names its version and can go back.
#[gpui::test]
fn an_older_value_keeps_its_banner_when_the_current_version_is_unknown(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);
    opened.update_view(|view, cx| view.activate_version(1, cx));
    opened.strip_reported_versions();
    opened.fail_history();

    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::Failed(_))));
    assert_eq!(
        opened.read_view(|view| view.current_version_id().map(str::to_string)),
        None
    );
    assert_eq!(opened.shown_password().as_deref(), Some(PREVIOUS_PASSWORD));
    assert_eq!(
        opened.read_view(|view| view.shown_older_version().map(str::to_string)),
        Some("v-previous".to_string())
    );
    assert!(opened.read_view(|view| view.can_return_to_current()));

    let text = opened.rendered_text();
    assert!(text.contains("Showing version"), "{text}");
    assert!(text.contains("v-previous"), "{text}");
    assert!(text.contains("Back to current"), "{text}");
}

/// The automatic reload a connection change triggers stays quiet about a
/// loss the editor already reported; a listing the user asks for reports it.
#[gpui::test]
fn an_explicit_history_retry_reports_a_missing_connection_again(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update_view(|view, cx| view.retrieve_selected_value(cx));

    let profile_id = opened.read(|doc| doc.profile_id);
    let app_state = opened.app_state.clone();
    opened.window.update(|_, cx| {
        app_state.update(cx, |state, _| state.connections_mut().remove(&profile_id));
    });
    opened.start_edit();
    assert_eq!(opened.reported_errors().len(), 1);

    opened.announce_state_change();
    assert_eq!(opened.reported_errors().len(), 1, "automatic reload");

    opened.update_view(|view, cx| view.load_versions(PLAINTEXT_SECRET.to_string(), cx));
    let reported = opened.reported_errors();
    assert_eq!(reported.len(), 2, "{reported:?}");
    assert!(opened.read_view(|view| matches!(view.versions(), VersionsState::Failed(_))));
}

// -- Value editor: JSON key/value secrets -----------------------------------

mod editor_key_value {
    use super::*;
    use crate::secret_store::view::editor::EditMode;

    const EDITED: &str = r#"{"username":"api-user2","password":"fake-password-7f3a","port":5432}"#;

    fn opened_key_value<'a>(cx: &'a mut TestAppContext) -> Opened<'a> {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.window.update(|window, _| window.activate_window());
        opened.window.run_until_parked();
        opened.select(KEY_VALUE_SECRET);
        opened.update_view(|view, cx| view.retrieve_selected_value(cx));
        opened
    }

    fn has_path(opened: &mut Opened<'_>, suffix: &str) -> bool {
        opened
            .rendered_paths()
            .iter()
            .any(|path| path.ends_with(suffix))
    }

    #[gpui::test]
    fn edit_value_is_offered_for_a_key_value_secret(cx: &mut TestAppContext) {
        let mut opened = opened_key_value(cx);
        assert!(opened.read_view(|view| view.can_edit_value()));
        assert!(has_path(&mut opened, "secret-action-edit-value"));

        opened.update_view(|view, cx| view.set_value_view(ValueView::Plaintext, cx));
        assert!(has_path(&mut opened, "secret-action-edit-value"));

        opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));
        assert!(
            opened
                .rendered_text()
                .contains(&dbflux_i18n::t!("document.secret_store.edit.open"))
        );
    }

    #[gpui::test]
    fn the_chord_and_the_pane_action_open_the_editor(cx: &mut TestAppContext) {
        let mut opened = opened_key_value(cx);
        let actions: Vec<String> = opened.window.update(|_, cx| {
            opened
                .doc
                .read(cx)
                .pane_actions(&opened.doc, cx)
                .into_iter()
                .map(|action| action.id.to_string())
                .collect()
        });
        assert!(actions.iter().any(|id| id == "secret-edit-value"));

        assert!(opened.command(Command::EditValue));
        assert!(opened.read_view(|view| view.is_editing_value()));
    }

    #[gpui::test]
    fn the_editor_opens_formatted_on_the_stored_text(cx: &mut TestAppContext) {
        let mut opened = opened_key_value(cx);
        opened.start_edit();

        assert_eq!(
            opened.read_view(|view| view.edit_mode()),
            Some(EditMode::Formatted)
        );
        let text = opened.editor_text();
        assert!(text.contains("\"username\": \"api-user\""), "{text}");
        assert!(text.contains('\n'), "indented for editing: {text}");
    }

    #[gpui::test]
    fn an_edited_pair_saves_compact_and_the_pairs_show_the_new_value(cx: &mut TestAppContext) {
        let mut opened = opened_key_value(cx);
        opened.start_edit();
        let formatted = opened.editor_text().replace("api-user", "api-user2");
        opened.set_editor_text(&formatted);

        opened.save_edit();

        assert_eq!(
            opened.written_values(),
            vec![(KEY_VALUE_SECRET.to_string(), EDITED.to_string())]
        );
        opened.read_view(|view| {
            assert!(!view.is_editing_value());
            let ValueState::Loaded(value) = &view.value else {
                panic!("value not loaded");
            };
            assert_eq!(value.view, ValueView::KeyValue);
            let usernames: Vec<String> = value
                .fields()
                .into_iter()
                .filter(|(key, _)| *key == Some("username"))
                .map(|(_, field)| field.expose_secret().to_string())
                .collect();
            assert_eq!(usernames, vec!["api-user2".to_string()]);
            assert!(!value.is_revealed(0), "masked again");
        });
    }

    #[gpui::test]
    fn invalid_json_blocks_save(cx: &mut TestAppContext) {
        let mut opened = opened_key_value(cx);
        opened.start_edit();
        opened.set_editor_text("{\"username\": oops");

        opened.save_edit();

        assert!(opened.driver.secret_store_calls().put_attempts.is_empty());
        let errors = opened.reported_errors();
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].1.contains("JSON"));
    }

    #[gpui::test]
    fn binary_and_managed_values_stay_read_only(cx: &mut TestAppContext) {
        for name in [BINARY_SECRET, MANAGED_SECRET] {
            let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
            opened.select(name);
            opened.update_view(|view, cx| view.retrieve_selected_value(cx));
            assert!(
                opened.read_view(|view| matches!(view.value, ValueState::Loaded(_))),
                "{name} loaded"
            );
            assert!(!opened.read_view(|view| view.can_edit_value()), "{name}");
            assert!(
                !opened.read_view(|view| view.can_request_edit_value()),
                "{name}"
            );
        }
    }

    #[gpui::test]
    fn the_key_value_editor_reaches_no_accessibility_node(cx: &mut TestAppContext) {
        let mut opened = opened_key_value(cx);
        opened.start_edit();
        for mode in [EditMode::Formatted, EditMode::Raw] {
            let doc = opened.doc.clone();
            opened.window.update(|window, cx| {
                doc.update(cx, |doc, cx| {
                    doc.view
                        .update(cx, |view, cx| view.set_edit_mode(mode, window, cx))
                })
            });
            opened.window.run_until_parked();
            let text = opened.accessibility_update_text();
            assert!(!text.contains("fake-password-7f3a"), "{mode:?}: {text}");
            assert!(!text.contains("api-user"), "{mode:?}: {text}");
        }
    }
}

// -- Value editor: review findings ------------------------------------------

mod editor_review {
    use super::*;
    use crate::keyboard_test_support::host_document;
    use crate::secret_store::view::editor::EditMode;
    use dbflux_app::keymap::ContextId;

    /// The document hosted the way the workspace hosts it: the key context
    /// comes from the document and every keymap command is routed to it, so
    /// a test presses real keys.
    fn open_hosted<'a>(cx: &'a mut TestAppContext) -> Opened<'a> {
        init_keyboard_runtime(cx);

        let driver = FakeDriver::new(DbKind::AwsSecrets);
        let (app_state, profile_id) = connected_app_state(cx, &driver, "prod-aws");
        cx.update(|cx| {
            cx.set_global(AppStateGlobal {
                entity: app_state.clone(),
            });
        });

        let state = app_state.clone();
        let (host, window) = host_document(
            cx,
            move |window, cx| {
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
            },
            |doc, cx| doc.active_context(cx),
            |doc, command, window, cx| doc.dispatch_command(command, window, cx),
        );
        let doc = window.update(|_, cx| host.read(cx).document.clone());
        window.update(|window, _| window.activate_window());
        window.run_until_parked();

        Opened {
            doc,
            window,
            driver,
            app_state,
        }
    }

    fn save_chord() -> &'static str {
        if cfg!(target_os = "macos") {
            "cmd-s"
        } else {
            "ctrl-s"
        }
    }

    fn start(opened: &mut Opened<'_>, stored: &str, edited: &str) {
        opened.select(PLAINTEXT_SECRET);
        opened.show_value(stored);
        opened.start_edit();
        opened.set_editor_text(edited);
        opened.rendered_paths();
    }

    fn exists(opened: &mut Opened<'_>, suffix: &str) -> bool {
        opened
            .rendered_paths()
            .iter()
            .any(|path| path.ends_with(suffix))
    }

    fn count_with(opened: &mut Opened<'_>, needle: &str) -> usize {
        opened
            .rendered_paths()
            .iter()
            .filter(|path| path.contains(needle))
            .count()
    }

    fn open_find(opened: &mut Opened<'_>) {
        let doc = opened.doc.clone();
        opened.window.update(|window, cx| {
            doc.update(cx, |doc, cx| {
                doc.view
                    .update(cx, |view, cx| view.find_in_value(window, cx))
            })
        });
        opened.window.run_until_parked();
        opened.rendered_paths();
    }

    /// The real chord, through the window's key dispatch to the focused
    /// textarea, reaches the store.
    #[gpui::test]
    fn the_save_chord_typed_into_the_textarea_writes(cx: &mut TestAppContext) {
        let mut opened = open_hosted(cx);
        start(&mut opened, "stored text", "typed text");

        opened.window.simulate_keystrokes(save_chord());
        opened.window.run_until_parked();

        assert_eq!(
            opened.written_values(),
            vec![(PLAINTEXT_SECRET.to_string(), "typed text".to_string())]
        );
    }

    /// With Vim mode on, the editor starts in Normal mode: `j` moves instead
    /// of typing, `i` inserts, the first Escape returns to Normal and keeps
    /// the keyboard in the editor, and the second leaves it.
    #[gpui::test]
    fn vim_mode_edits_the_secret_and_escape_steps_out(cx: &mut TestAppContext) {
        use dbflux_app::keymap::ContextId;

        let mut opened = open_hosted(cx);
        opened
            .window
            .update(|_, cx| dbflux_components::vim::set_vim_enabled(cx, true));
        start(&mut opened, "abc\ndef", "abc\ndef");

        opened.window.simulate_keystrokes("j");
        opened.window.run_until_parked();
        assert_eq!(
            opened.editor_text(),
            "abc\ndef",
            "`j` moved, it did not type"
        );

        opened.window.simulate_keystrokes("i");
        opened.window.simulate_input("X");
        opened.window.run_until_parked();
        assert!(opened.editor_text().contains('X'));

        opened.window.simulate_keystrokes("escape");
        opened.window.run_until_parked();
        assert_eq!(
            opened.read_cx(|doc, cx| doc.active_context(cx)),
            ContextId::TextInput,
            "the first Escape only leaves Insert mode"
        );
        opened.window.simulate_keystrokes("x");
        opened.window.run_until_parked();
        assert_eq!(opened.editor_text(), "abc\ndef", "`x` deleted the typed X");

        opened.window.simulate_keystrokes("escape");
        opened.window.run_until_parked();
        assert_eq!(
            opened.read_cx(|doc, cx| doc.active_context(cx)),
            ContextId::Results,
            "Escape in Normal mode leaves the editor"
        );
    }

    /// Turning the setting off updates an open editor at once: keys type
    /// again.
    #[gpui::test]
    fn vim_mode_follows_the_setting_live_in_the_secret_editor(cx: &mut TestAppContext) {
        let mut opened = open_hosted(cx);
        opened
            .window
            .update(|_, cx| dbflux_components::vim::set_vim_enabled(cx, true));
        start(&mut opened, "abc", "abc");

        opened
            .window
            .update(|_, cx| dbflux_components::vim::set_vim_enabled(cx, false));
        opened.window.run_until_parked();
        opened.window.simulate_input("j");
        opened.window.run_until_parked();
        assert!(opened.editor_text().contains('j'), "typed with Vim off");
        let typed = opened.editor_text();

        opened
            .window
            .update(|_, cx| dbflux_components::vim::set_vim_enabled(cx, true));
        opened.window.run_until_parked();
        opened.window.simulate_keystrokes("l");
        opened.window.run_until_parked();
        assert_eq!(opened.editor_text(), typed, "`l` moved, it did not type");
    }

    /// The save chord pressed in Normal mode writes through the gates.
    #[gpui::test]
    fn the_save_chord_in_vim_normal_mode_writes(cx: &mut TestAppContext) {
        let mut opened = open_hosted(cx);
        opened
            .window
            .update(|_, cx| dbflux_components::vim::set_vim_enabled(cx, true));
        start(&mut opened, "abc", "abc");

        opened.window.simulate_keystrokes("i");
        opened.window.simulate_input("X");
        opened.window.simulate_keystrokes("escape");
        opened.window.run_until_parked();
        let typed = opened.editor_text();
        assert_ne!(typed, "abc");

        opened.window.simulate_keystrokes(save_chord());
        opened.window.run_until_parked();

        assert_eq!(
            opened.written_values(),
            vec![(PLAINTEXT_SECRET.to_string(), typed)]
        );
    }

    /// Focus inside the Find input is still the editor: the context stays
    /// the text input's and the chord still saves.
    #[gpui::test]
    fn the_save_chord_typed_into_find_still_writes(cx: &mut TestAppContext) {
        let mut opened = open_hosted(cx);
        start(&mut opened, "stored text", "typed text");

        open_find(&mut opened);
        let editor =
            opened.read_view(|view| view.value_edit.as_ref().expect("editor").editor.clone());
        let (textarea_focused, editor_contains_focus) = opened.window.update(|window, cx| {
            let textarea = gpui::Focusable::focus_handle(editor.read(cx), cx);
            (
                textarea.is_focused(window),
                textarea.contains_focused(window, cx),
            )
        });
        let wrapper_has_focus =
            opened.read_view(|view| view.value_edit.as_ref().is_some_and(|edit| edit.focused));
        assert!(
            !textarea_focused && !editor_contains_focus,
            "focus left the textarea for the Find input"
        );
        assert!(wrapper_has_focus, "the editor still counts as focused");
        assert_eq!(
            opened.read_cx(|doc, cx| doc.active_context(cx)),
            ContextId::TextInput
        );

        opened.window.simulate_keystrokes(save_chord());
        opened.window.run_until_parked();

        assert_eq!(
            opened.written_values(),
            vec![(PLAINTEXT_SECRET.to_string(), "typed text".to_string())]
        );
    }

    /// While the editor holds the keyboard, the search command opens the
    /// value's Find, not the secret-name search.
    #[gpui::test]
    fn focus_search_in_the_editor_opens_the_value_find(cx: &mut TestAppContext) {
        let mut opened = open_hosted(cx);
        start(&mut opened, "stored text", "typed text");
        let before = opened.rendered_paths().len();

        assert!(opened.command(Command::FocusSearch));
        opened.rendered_paths();

        assert!(
            !opened.read(|doc| doc.focus_mode == crate::secret_store::SecretFocusMode::Search),
            "the secret-name search did not take the key"
        );
        assert!(opened.rendered_paths().len() > before, "Find panel drawn");
        assert_eq!(
            opened.read_cx(|doc, cx| doc.active_context(cx)),
            ContextId::TextInput
        );
    }

    /// Indentation Formatted added is not a saveable change: no chip, no
    /// marker, no changed-line count, and Save stays off.
    #[gpui::test]
    fn formatted_only_whitespace_is_not_modified(cx: &mut TestAppContext) {
        let mut opened = open_hosted(cx);
        opened.select(PLAINTEXT_SECRET);
        opened.show_value("[1,2]");
        opened.start_edit();
        opened.set_editor_text("[ 1 , 2 ]");
        opened.rendered_paths();

        assert!(!exists(&mut opened, "secret-value-editor-modified"));
        assert_eq!(count_with(&mut opened, "-marker"), 0);
        assert!(!opened.dirty());

        // In Raw mode the same text is a change.
        let doc = opened.doc.clone();
        opened.window.update(|window, cx| {
            doc.update(cx, |doc, cx| {
                doc.view
                    .update(cx, |view, cx| view.set_edit_mode(EditMode::Raw, window, cx))
            })
        });
        opened.set_editor_text("[ 1 , 2 ]");
        opened.rendered_paths();
        assert!(exists(&mut opened, "secret-value-editor-modified"));
    }

    /// The caret is where the test put it, in bytes, before the footer is
    /// asked: a byte-offset footer would pass `ñ` and fail after the emoji.
    #[gpui::test]
    fn the_caret_is_verified_by_byte_offset(cx: &mut TestAppContext) {
        let mut opened = open_hosted(cx);
        start(&mut opened, "x", "ñ😀x");
        let editor =
            opened.read_view(|view| view.value_edit.as_ref().expect("editor").editor.clone());

        for (character, bytes, column) in
            [(1u32, 2usize, "Col 2"), (2, 6, "Col 3"), (3, 7, "Col 4")]
        {
            opened.window.update(|window, cx| {
                editor.update(cx, |state, cx| {
                    state.set_cursor_position(lsp_types::Position::new(0, character), window, cx)
                });
            });
            opened.window.run_until_parked();
            opened.rendered_paths();

            let offset = opened.window.update(|_, cx| editor.read(cx).cursor());
            assert_eq!(offset, bytes, "caret byte offset");
            assert!(opened.footer_text().contains(column), "{column}");
        }
    }

    const UUID_VERSION: &str = "2f004766-3e70-4b10-ad74-b8419267a117";

    /// A UUID version reads short in the footer, every item shares one row,
    /// no line starts with a separator, and the full id is the tooltip.
    #[gpui::test]
    fn the_footer_shortens_a_uuid_version_and_stays_on_one_row(cx: &mut TestAppContext) {
        let mut opened = open_hosted(cx);
        opened.select(PLAINTEXT_SECRET);
        opened.show_value("a\nb");
        opened.update_view(|view, _cx| {
            if let ValueState::Loaded(value) = &mut view.value {
                value.version = Some(UUID_VERSION.to_string());
            }
        });
        opened.start_edit();
        opened.set_editor_text("a\nchanged");

        let footer = opened.footer_text();
        assert!(footer.contains("2f004766…"), "{footer}");
        assert!(!footer.contains(UUID_VERSION), "{footer}");

        let tops: Vec<f32> = [
            "secret-value-editor-footer-summary",
            "secret-value-editor-footer-version",
        ]
        .iter()
        .map(|id| f32::from(opened.element_bounds(id).origin.y))
        .collect();
        assert!(
            (tops[0] - tops[1]).abs() < 1.5, // guardrail-allow: test tolerance, not spacing
            "the summary and the version share one row: {tops:?}"
        );

        // No wrapped line starts with a separator: the first item of each
        // group is drawn without one.
        let summary = opened
            .element_text_and_top("secret-value-editor-footer-summary")
            .0;
        let position = opened
            .element_text_and_top("secret-value-editor-footer-position")
            .0;
        assert!(!summary.trim_start().starts_with('·'), "{summary}");
        assert!(!position.trim_start().starts_with('·'), "{position}");

        assert_eq!(
            hover_tooltip(&mut opened, "secret-value-editor-footer-version"),
            UUID_VERSION
        );
    }

    /// The Formatted hint describes what Save does for both stored layouts.
    #[gpui::test]
    fn the_formatted_hint_describes_both_layouts(cx: &mut TestAppContext) {
        let mut opened = open_hosted(cx);
        start(&mut opened, "[1,2]", "[1,3]");

        let text = opened.rendered_text();
        assert!(text.contains("on one line back on one line"), "{text}");
        assert!(text.contains("several lines as it is here"), "{text}");
    }
}

// -- The details pane against its design -------------------------------------

mod details_design {
    use super::*;
    use crate::secret_store::value::ValueKind;

    fn plain(text: &str) -> RetrievedValue {
        RetrievedValue::new(&SecretValue::text(None, text.to_string()))
    }

    fn kind_chip(text: &str) -> String {
        plain(text).kind_label()
    }

    #[test]
    fn the_kind_of_a_value_comes_from_its_content() {
        let dotenv = "# runtime\nPORT=3000\n\nENV=prod\nexport URL=postgres://a@b/c?x=1\n";
        assert_eq!(plain(dotenv).kind(), ValueKind::Dotenv { lines: 5 });
        assert_eq!(kind_chip(dotenv), "dotenv · 5 lines");

        assert_eq!(kind_chip("TOKEN=abc"), "dotenv · 1 line");
        assert_eq!(kind_chip(r#"{"a":1,"b":{"c":2},"d":[]}"#), "JSON · 3 keys");
        assert_eq!(kind_chip(r#"{"a":1}"#), "JSON · 1 key");
        assert_eq!(kind_chip("[1,2,3]"), "JSON · 3 items");
        assert_eq!(kind_chip("[1]"), "JSON · 1 item");
        assert_eq!(kind_chip("just a token"), "text · 1 line");
        assert_eq!(kind_chip("one\ntwo\nthree"), "text · 3 lines");
    }

    #[test]
    fn text_that_only_looks_like_assignments_is_not_dotenv() {
        assert_eq!(kind_chip("PORT=3000\nnot an assignment"), "text · 2 lines");
        assert_eq!(kind_chip("YWJjZA=="), "text · 1 line");
        assert_eq!(kind_chip("1BAD=key"), "text · 1 line");
        assert_eq!(kind_chip("# only a comment"), "text · 1 line");
        assert_eq!(kind_chip(""), "text · 0 lines");
    }

    #[test]
    fn the_chip_of_a_binary_value_names_no_content() {
        let binary = RetrievedValue::new(&SecretValue::binary(None, vec![1, 2, 3]));
        assert_eq!(binary.kind_label(), "binary");
    }

    fn close_details(opened: &mut Opened<'_>) {
        let doc = opened.doc.clone();
        opened
            .window
            .update(|window, cx| doc.update(cx, |doc, cx| doc.close_details(window, cx)));
        opened.window.run_until_parked();
    }

    #[gpui::test]
    fn closing_with_unsaved_edits_is_refused_by_the_shared_guard(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.select(PLAINTEXT_SECRET);
        opened.update_view(|view, cx| view.retrieve_selected_value(cx));
        opened.start_edit();
        opened.set_editor_text("an unsaved edit");

        close_details(&mut opened);

        assert!(opened.read(|doc| {
            matches!(doc.selected(), Some(SecretRowKey::Secret(name)) if name == PLAINTEXT_SECRET)
        }));
        assert_eq!(opened.editor_text(), "an unsaved edit");
        assert!(opened.read_view(|view| matches!(view.value(), ValueState::Loaded(_))));
    }

    #[gpui::test]
    fn closing_a_focused_clean_editor_moves_focus_to_the_document(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.window.update(|window, _| window.activate_window());
        opened.window.run_until_parked();
        opened.rendered_paths();
        opened.select(PLAINTEXT_SECRET);
        opened.update_view(|view, cx| view.retrieve_selected_value(cx));
        opened.start_edit();
        opened.rendered_paths();
        assert!(opened.read_view(|view| view.value_edit.as_ref().is_some_and(|edit| edit.focused)));

        close_details(&mut opened);

        let doc = opened.doc.clone();
        let focused = opened
            .window
            .update(|window, cx| doc.read(cx).focus_handle.is_focused(window));
        assert!(focused, "the document owns focus after the editor is gone");
    }

    #[gpui::test]
    fn closing_without_a_focused_editor_leaves_focus_alone(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.window.update(|window, _| window.activate_window());
        opened.select(KEY_VALUE_SECRET);
        opened.rendered_paths();
        let doc = opened.doc.clone();
        opened
            .window
            .update(|window, cx| window.focus(&cx.focus_handle(), cx));
        let before = opened
            .window
            .update(|window, cx| doc.read(cx).focus_handle.is_focused(window));
        assert!(!before);

        close_details(&mut opened);

        let after = opened
            .window
            .update(|window, cx| doc.read(cx).focus_handle.is_focused(window));
        assert!(!after, "no focus change without a focused editor");
    }

    #[test]
    fn dotenv_values_may_start_with_an_equals_sign() {
        assert_eq!(
            kind_chip("USER=service\nPASSWORD==secret"),
            "dotenv · 2 lines"
        );
        assert_eq!(kind_chip("YWJj=="), "text · 1 line");
        assert_eq!(kind_chip("YWJjZA=="), "text · 1 line");
    }

    /// A line whose value is only `=` characters is an assignment when other
    /// lines are assignments too; the base64 reading holds only for a value
    /// that is one bare token.
    #[test]
    fn a_padding_only_line_among_assignments_keeps_the_value_a_dotenv() {
        assert_eq!(kind_chip("A=1\nPAD=="), "dotenv · 2 lines");
        assert_eq!(kind_chip("PAD==\nA=1"), "dotenv · 2 lines");
        assert_eq!(kind_chip("# padding\nA=1\nPAD=\n"), "dotenv · 3 lines");
        assert_eq!(kind_chip("YWJj=="), "text · 1 line");
        assert_eq!(
            kind_chip("# a comment\nYWJj=="),
            "dotenv · 2 lines",
            "a comment line makes it more than one bare token"
        );
    }

    fn node_bounds(opened: &mut Opened<'_>, id: &str) -> Option<gpui::Bounds<gpui::Pixels>> {
        let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(opened.window);
        let frame = capture.frame(opened.window);
        frame
            .nodes()
            .find(|(_, node)| node.id() == id)
            .map(|(_, node)| node.bounds())
    }

    fn node_text(opened: &mut Opened<'_>, id: &str) -> Option<String> {
        let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(opened.window);
        let frame = capture.frame(opened.window);
        frame
            .nodes()
            .find(|(_, node)| node.id() == id)
            .map(|(_, node)| node.content_text().to_string())
    }

    fn click(opened: &mut Opened<'_>, id: &str) {
        let bounds = node_bounds(opened, id).unwrap_or_else(|| panic!("`{id}` is drawn"));
        opened
            .window
            .simulate_click(bounds.center(), gpui::Modifiers::default());
        opened.window.run_until_parked();
    }

    #[gpui::test]
    fn the_pane_header_names_the_secret_with_copy_expand_and_close(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.select(KEY_VALUE_SECRET);

        let paths = opened.rendered_paths();
        let has = |id: &str| paths.iter().any(|path| path.ends_with(id));

        assert!(has("secret-details-header"), "{paths:?}");
        assert!(has("secret-details-name"));
        assert!(has("secret-details-copy-name"));
        assert!(has("secret-details-close"));
        assert!(has("secret-details-expand"), "{paths:?}");
        assert_eq!(
            node_text(&mut opened, "secret-details-name").as_deref(),
            Some(KEY_VALUE_SECRET)
        );
    }

    #[gpui::test]
    fn the_header_copies_the_name_and_closes_the_pane(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.select(KEY_VALUE_SECRET);

        opened.rendered_paths();
        click(&mut opened, "secret-details-copy-name");
        assert_eq!(opened.clipboard().as_deref(), Some(KEY_VALUE_SECRET));

        click(&mut opened, "secret-details-close");
        assert!(opened.read(|doc| doc.selected().is_none()));
        assert!(
            node_bounds(&mut opened, "secret-details-header").is_none(),
            "the pane shows its empty state"
        );
    }

    #[gpui::test]
    fn closing_the_pane_drops_the_retrieved_value(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.select(KEY_VALUE_SECRET);
        opened.update_view(|view, cx| view.retrieve_selected_value(cx));
        assert!(opened.read_view(|view| matches!(view.value(), ValueState::Loaded(_))));

        close_details(&mut opened);

        assert!(opened.read_view(|view| matches!(view.value(), ValueState::NotLoaded)));
        assert!(opened.read(|doc| doc.selected().is_none()));
    }

    #[gpui::test]
    fn the_header_controls_are_keyboard_reachable(cx: &mut TestAppContext) {
        use crate::keyboard_coverage::SECRET_STORE;
        use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};

        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.select(KEY_VALUE_SECRET);

        let entries: Vec<String> = opened.window.update(|_, cx| {
            opened
                .doc
                .read(cx)
                .pane_actions(&opened.doc, cx)
                .into_iter()
                .map(|action| action.id.to_string())
                .collect()
        });

        let capture = FrameCapture::observe(opened.window);
        let checked = Coverage::new(SECRET_STORE)
            .with_menu_entries(entries)
            .assert_covered(&capture.frame(opened.window));

        for id in ["secret-details-copy-name", "secret-details-close"] {
            assert!(checked.iter().any(|checked| checked == id), "{checked:?}");
        }
    }

    #[gpui::test]
    fn the_value_not_loaded_card_has_a_compact_retrieve_button(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.select(KEY_VALUE_SECRET);

        let text = opened.rendered_text();
        assert!(text.contains("Value not loaded"), "{text}");

        let card = node_bounds(&mut opened, "secret-value-card").expect("the card is drawn");
        let button = node_bounds(&mut opened, "secret-retrieve-value").expect("the button");
        assert!(
            button.size.width < card.size.width * 0.7,
            "a compact button, not a bar: {:?} in {:?}",
            button.size.width,
            card.size.width
        );
        assert!(node_bounds(&mut opened, "secret-value-card-lock").is_some());
    }

    #[gpui::test]
    fn a_plaintext_value_offers_the_two_tab_control_with_key_value_muted(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.select(PLAINTEXT_SECRET);
        opened.update_view(|view, cx| view.retrieve_selected_value(cx));

        let paths = opened.rendered_paths();
        let has = |id: &str| paths.iter().any(|path| path.ends_with(id));

        assert!(has("segmented-secret-value-view-key-value"), "{paths:?}");
        assert!(has("segmented-secret-value-view-plaintext"));
        assert!(!has("secret-value-view-key-value-unavailable"));

        let text = opened.rendered_text();
        assert!(
            !text.contains("Key/value opens only when the value parses"),
            "the paragraph is gone: {text}"
        );
    }

    #[test]
    fn key_value_is_muted_with_its_reason_when_the_value_is_not_an_object() {
        use crate::secret_store::view::details::value_view_items;

        let items = value_view_items(false);
        let key_value = items.first().expect("the first tab");
        assert_eq!(key_value.id.as_ref(), "key-value");
        assert!(key_value.disabled);
        assert!(
            key_value
                .disabled_reason
                .as_ref()
                .is_some_and(|reason| reason.starts_with("Key/value is not available")),
            "{:?}",
            key_value.disabled_reason
        );

        let plaintext = items.get(1).expect("the second tab");
        assert_eq!(plaintext.id.as_ref(), "plaintext");
        assert!(!plaintext.disabled);

        let items = value_view_items(true);
        assert!(items.iter().all(|item| !item.disabled));
    }

    #[gpui::test]
    fn clicking_the_muted_key_value_tab_changes_nothing(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.select(PLAINTEXT_SECRET);
        opened.update_view(|view, cx| view.retrieve_selected_value(cx));
        opened.rendered_paths();

        let before = opened.read_view(|view| view.value_view_sets);
        click(&mut opened, "segmented-secret-value-view-key-value");
        assert_eq!(
            opened.read_view(|view| view.value_view_sets),
            before,
            "the muted tab never reaches the view setter"
        );

        assert!(opened.read_view(|view| match view.value() {
            ValueState::Loaded(value) => value.view == ValueView::Plaintext,
            _ => false,
        }));
    }

    #[gpui::test]
    fn a_key_value_secret_keeps_both_tabs_enabled_and_every_key_masked(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.select(KEY_VALUE_SECRET);
        opened.update_view(|view, cx| view.retrieve_selected_value(cx));

        let text = opened.rendered_text();
        assert!(!text.contains("Key/value is not available"), "{text}");
        assert!(!text.contains(FAKE_SECRET_PASSWORD));
        assert!(!text.contains("api-user"), "{text}");
        assert!(!text.contains("5432"), "{text}");
    }

    #[gpui::test]
    fn the_kind_chip_sits_beside_value_and_holds_no_value_text(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.select(KEY_VALUE_SECRET);

        assert!(
            node_bounds(&mut opened, "secret-value-kind").is_none(),
            "no chip before the value is retrieved"
        );

        opened.update_view(|view, cx| view.retrieve_selected_value(cx));
        assert_eq!(
            node_text(&mut opened, "secret-value-kind").as_deref(),
            Some("JSON · 3 keys")
        );

        opened.show_value(MULTILINE_SECRET);
        assert_eq!(
            node_text(&mut opened, "secret-value-kind").as_deref(),
            Some("dotenv · 3 lines")
        );

        opened.show_value("a plain token");
        assert_eq!(
            node_text(&mut opened, "secret-value-kind").as_deref(),
            Some("text · 1 line")
        );

        let announced = opened.accessibility_update_text();
        assert!(announced.contains("text · 1 line"), "{announced}");
        assert!(!announced.contains("a plain token"), "{announced}");
    }

    #[gpui::test]
    fn the_header_and_chip_reach_the_accessibility_tree_without_a_value(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.select(KEY_VALUE_SECRET);
        opened.update_view(|view, cx| view.retrieve_selected_value(cx));
        assert!(opened.command(Command::ExpandCollapse));

        let announced = opened.accessibility_update_text();
        assert!(announced.contains("Copy name"), "{announced}");
        assert!(announced.contains("Close"), "{announced}");
        assert!(announced.contains("JSON · 3 keys"), "{announced}");

        for secret in [FAKE_SECRET_PASSWORD, "api-user"] {
            assert!(!announced.contains(secret), "{announced}");
        }
    }

    #[gpui::test]
    fn tags_are_key_value_chips_in_a_wrapping_row_with_an_edit_button(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.select(KEY_VALUE_SECRET);

        assert_eq!(
            node_text(&mut opened, "secret-tag-0").as_deref(),
            Some("team=payments")
        );
        assert_eq!(
            node_text(&mut opened, "secret-tag-1").as_deref(),
            Some("env=prod")
        );

        let paths = opened.rendered_paths();
        assert!(
            paths.iter().any(|path| path.contains("secret-tags-edit")),
            "the Tags section ends with its Edit button"
        );
    }

    #[gpui::test]
    fn the_version_row_of_a_typed_store_reads_v3(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
        opened.select(DENIED_PARAMETER);

        let text = opened.rendered_text();
        assert!(text.contains("Version v3"), "{text}");
    }

    #[test]
    fn last_retrieved_is_a_date_even_across_a_local_offset() {
        use crate::secret_store::view::details::last_retrieved_label;
        use dbflux_core::chrono::{FixedOffset, TimeZone, Utc};

        let midnight = Utc.with_ymd_and_hms(2026, 10, 19, 0, 0, 0).unwrap();
        let behind = FixedOffset::west_opt(5 * 3600).unwrap();
        assert_eq!(
            midnight
                .with_timezone(&behind)
                .format("%Y-%m-%d")
                .to_string(),
            "2026-10-18",
            "a local conversion would show the previous day"
        );

        assert_eq!(last_retrieved_label(Some(midnight)), "2026-10-19");
        assert_eq!(last_retrieved_label(None), "—");
    }

    #[gpui::test]
    fn the_value_actions_share_a_row_at_the_default_width(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.select(PLAINTEXT_SECRET);
        opened.update_view(|view, cx| view.retrieve_selected_value(cx));
        opened.rendered_paths();

        let top = |opened: &mut Opened<'_>, id: &str| {
            node_bounds(opened, id)
                .unwrap_or_else(|| panic!("`{id}` is drawn"))
                .origin
                .y
        };

        let reveal = top(&mut opened, "secret-value-hide-all");
        let copy = top(&mut opened, "secret-value-copy-all");
        assert_eq!(reveal, copy);
        assert!(
            node_bounds(&mut opened, "secret-value-edit").is_none(),
            "the bottom bar owns Edit value in the pane"
        );
    }
}

// -- The find panel of a redacted editor ---------------------------------------

mod find_panel_redaction {
    use super::*;
    use gpui::{IntoElement, ParentElement as _, Render, Styled as _, Window, div};
    use gpui_component::input::{Editor, EditorState};

    const SECRETISH: &str = "alpha-77b1";

    /// the replace field of a redacted editor's find panel withholds what
    /// is typed into it.
    #[gpui::test]
    fn the_find_panels_replace_field_stays_out_of_the_tree(cx: &mut TestAppContext) {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
        opened.window.update(|window, _| window.activate_window());
        opened.window.run_until_parked();
        opened.select(PLAINTEXT_SECRET);
        opened.show_value("seed");
        opened.start_edit();
        opened.set_editor_text("seed");

        let doc = opened.doc.clone();
        opened.window.update(|window, cx| {
            doc.update(cx, |doc, cx| {
                doc.view
                    .update(cx, |view, cx| view.find_in_value(window, cx))
            })
        });
        opened.window.run_until_parked();
        opened.window.simulate_input("zzsearchzz");
        opened.window.run_until_parked();
        opened.window.update(|window, cx| {
            window.dispatch_action(Box::new(gpui_component::input::Replace), cx)
        });
        opened.window.run_until_parked();
        opened.window.simulate_input("zzreplacezz");
        opened.window.run_until_parked();

        // Both fields hold what was typed, so the absence below is redaction
        // and not a field that never received the text.
        // The search field holds what was typed, and the replace field is
        // drawn: the plain editor's twin of this test shows the same typing
        // reaches both fields' accessibility values when nothing redacts them.
        assert_eq!(opened.find_fields().0, "zzsearchzz");
        let fields = opened
            .rendered_paths()
            .iter()
            .filter(|path| path.contains("search-panel") && path.ends_with("input-state"))
            .count();
        assert_eq!(fields, 2, "the search and the replace field are drawn");

        let text = opened.accessibility_update_text();
        assert!(!text.contains("zzsearchzz"), "{text}");
        assert!(!text.contains("zzreplacezz"), "{text}");
    }

    /// The twin of the redaction test: with nothing redacting them, the same
    /// typing puts both texts in the accessibility tree, so the absence the
    /// redacted editor shows is the redaction and not a field that never got
    /// the text.
    #[gpui::test]
    fn a_plain_editors_find_fields_expose_what_is_typed_into_them(cx: &mut TestAppContext) {
        init_keyboard_runtime(cx);
        let (view, window) = cx.add_window_view(|window, cx| {
            let editor = cx.new(|cx| EditorState::new(window, cx).searchable(true));
            editor.update(cx, |state, cx| {
                state.set_value("seed", window, cx);
                state.focus(window, cx);
            });
            Plain { editor }
        });
        window.update(|window, _| window.activate_window());
        window.run_until_parked();
        let editor = window.update(|_, cx| view.read(cx).editor.clone());
        window.update(|window, cx| crate::object_text::open_find_panel(&editor, window, cx));
        window.run_until_parked();
        window.simulate_input("zzsearchzz");
        window.run_until_parked();
        window.update(|window, cx| {
            window.dispatch_action(Box::new(gpui_component::input::Replace), cx)
        });
        window.run_until_parked();
        window.simulate_input("zzreplacezz");
        window.run_until_parked();

        let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(window);
        let frame = capture.frame(window);
        let values: Vec<String> = frame
            .nodes()
            .filter_map(|(_, node)| frame.accessibility_node(node).and_then(|n| n.value()))
            .map(|value| value.to_string())
            .collect();
        assert!(
            values.iter().any(|value| value.contains("zzsearchzz")),
            "{values:?}"
        );
        assert!(
            values.iter().any(|value| value.contains("zzreplacezz")),
            "{values:?}"
        );
    }

    struct Plain {
        editor: Entity<EditorState>,
    }

    impl Render for Plain {
        fn render(&mut self, _: &mut Window, _: &mut gpui::Context<Self>) -> impl IntoElement {
            div()
                .size_full()
                .child(Editor::new(&self.editor).h(gpui::px(300.0)))
        }
    }

    /// a non-redacted editor still fills the find field
    /// from the selection (the value is exposed twice: editor + search field).
    #[gpui::test]
    fn a_plain_editor_still_fills_find_from_the_selection(cx: &mut TestAppContext) {
        init_keyboard_runtime(cx);
        let (view, window) = cx.add_window_view(|window, cx| {
            let editor = cx.new(|cx| EditorState::new(window, cx).searchable(true));
            editor.update(cx, |state, cx| {
                state.set_value(SECRETISH, window, cx);
                state.focus(window, cx);
            });
            Plain { editor }
        });
        window.update(|window, _| window.activate_window());
        window.run_until_parked();
        window.simulate_keystrokes("ctrl-a");
        window.run_until_parked();
        let editor = window.update(|_, cx| view.read(cx).editor.clone());
        window.update(|window, cx| crate::object_text::open_find_panel(&editor, window, cx));
        window.run_until_parked();

        let capture = dbflux_ui_base::keyboard_coverage::FrameCapture::observe(window);
        let frame = capture.frame(window);
        let values: Vec<String> = frame
            .nodes()
            .filter_map(|(_, node)| frame.accessibility_node(node).and_then(|n| n.value()))
            .map(|value| value.to_string())
            .collect();
        let hits = values
            .iter()
            .filter(|value| value.contains(SECRETISH))
            .count();
        assert!(
            hits >= 2,
            "editor and search field both expose it: {values:?}"
        );
    }
}
