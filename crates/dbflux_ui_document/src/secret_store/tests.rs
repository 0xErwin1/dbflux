// The adopted verification probes run under the tests submodule, beside the
// module's own tests.
#[path = "write_probes.rs"]
mod write_probes;

use super::listing::{
    AUTO_CONTINUE_ENTRY_LIMIT, LARGEST_PAGE_ENTRIES, SecretListing, child_path_label,
    keeps_loading, parent_path, path_segments,
};
use super::value::{
    MASK, RetrievalTarget, RetrievedValue, ValueContent, ValueFailure, encode_base64,
    value_retrieval_event,
};
use super::write::{
    SECRET_VALUE_WRITE_FAILED_ACTION, SECRET_VALUE_WRITE_REFUSED_ACTION,
    SECRET_VALUE_WRITTEN_ACTION, SecretWriteFailure, put_secret_value,
};
use super::{
    DetailsState, PathMode, SecretRowKey, SecretStoreDocument, VALUE_RETRIEVE_FAILED_ACTION,
    VALUE_RETRIEVED_ACTION, ValueState, ValueView,
};
use crate::keyboard_test_support::{connected_app_state, init_keyboard_runtime};
use dbflux_app::keymap::Command;
use dbflux_core::observability::{EventCategory, EventRecord, EventSink, EventSinkError};
use dbflux_core::secrecy::ExposeSecret;
use dbflux_core::{
    Connection, DbError, DbKind, FormattedError, MutationPolicy, ReadOnlyReason, SecretDetails,
    SecretListPage, SecretListRequest, SecretPayload, SecretPutRequest, SecretStoreCapabilities,
    SecretStoreDescriptor, SecretStoreSource, SecretSummary, SecretValue, SecretVersionStage,
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

const KEY_VALUE_SECRET: &str = "prod/api/credentials";
const MANAGED_SECRET: &str = "prod/db/master";
const PENDING_DELETION_SECRET: &str = "prod/deploy/legacy-token";
const PLAINTEXT_SECRET: &str = "prod/notify/webhook-token";
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
    init_keyboard_runtime(cx);

    let driver = FakeDriver::new(DbKind::AwsSecrets);
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

    /// Shows `text` as the retrieved value, the way a store returning a
    /// multi-line secret would.
    fn show_value(&mut self, text: &str) {
        let value = RetrievedValue::new(&SecretValue::text(None, text.to_string()));
        self.update(|doc, cx| {
            doc.value = ValueState::Loaded(Box::new(value));
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

    let resource_id = opened.read(|doc| match doc.details() {
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
    assert!(opened.read(|doc| matches!(doc.value(), ValueState::NotLoaded)));
}

// -- Value -----------------------------------------------------------------

/// Retrieve value reads the value once, shows a key/value secret as its
/// pairs, and records one audit event that carries no part of the value.
#[gpui::test]
fn retrieve_value_reads_once_and_audits_without_the_value(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(KEY_VALUE_SECRET);

    assert!(opened.command(Command::ToggleValuePanel));

    let keys = opened.read(|doc| match doc.value() {
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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
    opened.update(|doc, cx| doc.set_value_view(ValueView::KeyValue, cx));

    opened.read(|doc| match doc.value() {
        ValueState::Loaded(value) => {
            assert_eq!(value.view, ValueView::Plaintext);
            assert!(!value.content.has_key_value_view());
            assert_eq!(value.fields()[0].1.expose_secret(), FAKE_SECRET_PLAINTEXT);
        }
        other => panic!("value not loaded: {other:?}"),
    });

    opened.select(BINARY_SECRET);
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));

    opened.read(|doc| match doc.value() {
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
    assert!(opened.read(|doc| doc.can_retrieve_value()));
}

/// The value of a secret whose deletion is pending cannot be retrieved: the
/// action is disabled and the key does nothing.
#[gpui::test]
fn a_pending_deletion_disables_retrieval(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PENDING_DELETION_SECRET);

    assert!(!opened.read(|doc| doc.can_retrieve_value()));

    let enabled = opened.window.update(|_, cx| {
        opened
            .doc
            .read(cx)
            .pane_actions(&opened.doc)
            .into_iter()
            .find(|action| action.id.as_ref() == "secret-retrieve-value")
            .map(|action| action.enabled)
    });
    assert_eq!(enabled, Some(false));

    opened.command(Command::ToggleValuePanel);

    assert!(opened.driver.secret_store_calls().value_requests.is_empty());
    assert!(opened.read(|doc| matches!(doc.value(), ValueState::NotLoaded)));
}

/// A value the store refuses to decrypt opens the error state with the
/// store's message and the audit correlation id, never an empty value, and
/// the failure is audited.
#[gpui::test]
fn a_denied_decryption_shows_the_error_and_never_an_empty_value(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/db/"));
    opened.load_everything();
    opened.select(DENIED_PARAMETER);

    opened.update(|doc, cx| doc.retrieve_selected_value(cx));

    let correlation_id = opened.read(|doc| match doc.value() {
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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
    assert!(opened.read(|doc| matches!(doc.value(), ValueState::Loaded(_))));

    opened.select(MANAGED_SECRET);
    assert!(opened.read(|doc| matches!(doc.value(), ValueState::NotLoaded)));
    assert!(opened.read(|doc| doc.value_inputs.is_empty()));

    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
    assert!(opened.read(|doc| matches!(doc.value(), ValueState::Loaded(_))));

    assert!(opened.command(Command::RefreshSchema));
    assert!(opened.read(|doc| matches!(doc.value(), ValueState::NotLoaded)));
    assert!(opened.read(|doc| doc.value_inputs.is_empty()));
}

/// A masked or revealed value never reaches the accessibility tree that UI
/// automation reads: every value field is a secret-marked input.
#[gpui::test]
fn values_never_reach_the_accessibility_tree(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();

    for name in [KEY_VALUE_SECRET, PLAINTEXT_SECRET] {
        opened.select(name);
        opened.update(|doc, cx| doc.retrieve_selected_value(cx));

        let masked = opened.rendered_text();
        assert!(masked.contains("secret-value-field-0") || !masked.is_empty());

        assert!(opened.command(Command::ExpandCollapse));
        assert!(opened.read(|doc| match doc.value() {
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

#[gpui::test]
fn the_reveal_all_button_names_what_it_will_do(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.load_everything();

    opened.select(KEY_VALUE_SECRET);
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));

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

    opened.update(|doc, cx| doc.copy_full_value(cx));
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
    let visible = line_height * super::details::VIEWER_VISIBLE_LINES;
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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));

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
        self.window
            .update(|window, cx| doc.update(cx, |doc, cx| doc.start_value_edit(window, cx)));
        self.window.run_until_parked();
    }

    /// The state of the open editor, if one is open.
    fn editing(&mut self) -> Option<(String, bool)> {
        self.read(|doc| {
            let edit = doc.value_edit.as_ref()?;
            Some((edit.original.expose_secret().to_string(), edit.saving))
        })
    }

    /// The text the open editor holds.
    fn editor_text(&mut self) -> String {
        let editor = self.read(|doc| doc.value_edit.as_ref().expect("editor open").editor.clone());
        self.window
            .update(|_, cx| editor.read(cx).value().to_string())
    }

    /// Replaces the text of the open editor.
    fn set_editor_text(&mut self, text: &str) {
        let editor = self.read(|doc| doc.value_edit.as_ref().expect("editor open").editor.clone());
        let text = text.to_string();
        self.window.update(|window, cx| {
            editor.update(cx, |state, cx| state.set_value(&text, window, cx));
        });
        self.window.run_until_parked();
    }

    /// Saves the open editor the way Save and the pane action do.
    fn save_edit(&mut self) {
        self.update(|doc, cx| doc.save_value_edit(cx));
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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));

    assert!(!opened.read(|doc| doc.is_editing_value()));

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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
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
        opened.read(|doc| doc.is_editing_value()),
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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
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

    opened.read(|doc| {
        assert!(!doc.is_editing_value(), "the editor closed");
        match &doc.value {
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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
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
        opened.read(|doc| doc.is_editing_value()),
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
        compact,
        "the editor opens with the stored text"
    );
    opened.set_editor_text(compact_edited);
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
        opened.read(|doc| doc.is_editing_value()),
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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
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
        opened.read(|doc| doc.is_editing_value()),
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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);

    opened.select(MANAGED_SECRET);
    assert_eq!(
        opened.read(|doc| doc.selected().cloned()),
        Some(SecretRowKey::Secret(PLAINTEXT_SECRET.to_string())),
        "the selection stayed"
    );
    assert!(opened.read(|doc| doc.is_editing_value()));

    let lists_before = opened.driver.secret_store_calls().list_requests.len();
    assert!(opened.command(Command::RefreshSchema));
    assert_eq!(
        opened.driver.secret_store_calls().list_requests.len(),
        lists_before,
        "the refresh was refused"
    );
    assert!(opened.read(|doc| doc.is_editing_value()));
    assert!(opened.read(|doc| matches!(doc.value(), ValueState::Loaded(_))));

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

    opened.update(|doc, cx| doc.discard_value_edit(cx));
    opened.select(MANAGED_SECRET);
    assert!(
        !opened.read(|doc| doc.is_editing_value()),
        "the selection works again after Discard"
    );
}

/// The tab itself reports unsaved value edits, so the workspace's dirty dot
/// and its unsaved-changes prompt ask before a close discards them.
#[gpui::test]
fn the_tab_reports_unsaved_edits_and_asks_before_closing(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);

    let (summary, status) = opened.window.update(|_, cx| {
        let doc = opened.doc.clone();
        (
            doc.read(cx).unsaved_change_summary(cx),
            doc.read(cx).state(cx),
        )
    });
    assert_eq!(summary.as_deref(), Some("Unsaved value edits"));
    assert_eq!(status, crate::types::DocumentState::Modified);

    opened.update(|doc, cx| doc.discard_value_edit(cx));
    let (summary, status) = opened.window.update(|_, cx| {
        let doc = opened.doc.clone();
        (
            doc.read(cx).unsaved_change_summary(cx),
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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);

    // Save without waiting for the landing to restock the executor.
    let doc = opened.doc.clone();
    opened
        .window
        .update(|_, cx| doc.update(cx, |doc, cx| doc.save_value_edit(cx)));
    // Typed while the write is in flight; the helper's park lets the
    // landing run after it.
    let continued = format!("{EDITED_VALUE}\nENV=staging");
    opened.set_editor_text(&continued);

    opened.read(|doc| {
        assert!(doc.is_editing_value(), "the editor stayed open");
        if let Some(edit) = &doc.value_edit {
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
    opened.read(|doc| match &doc.value {
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
    assert!(!opened.read(|doc| doc.is_editing_value()));
}

/// Discard sits out while a save is in flight — the pane action is
/// disabled and the guard makes a stale action a no-op — so the landing
/// still reads what was typed after Save.
#[gpui::test]
fn discard_sits_out_while_a_save_is_in_flight(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);
    opened.update(|doc, _cx| {
        if let Some(edit) = doc.value_edit.as_mut() {
            edit.saving = true;
        }
    });

    let mut enabled = |id: &str| {
        opened.window.update(|_, cx| {
            opened
                .doc
                .read(cx)
                .pane_actions(&opened.doc)
                .into_iter()
                .find(|action| action.id.as_ref() == id)
                .map(|action| action.enabled)
        })
    };
    assert_eq!(enabled("secret-editor-discard"), Some(false));
    assert_eq!(enabled("secret-editor-save"), Some(false));

    opened.update(|doc, cx| doc.discard_value_edit(cx));
    assert!(
        opened.read(|doc| doc.is_editing_value()),
        "the in-flight save's editor is not discarded"
    );

    opened.update(|doc, _cx| {
        if let Some(edit) = doc.value_edit.as_mut() {
            edit.saving = false;
        }
    });
    opened.update(|doc, cx| doc.discard_value_edit(cx));
    assert!(!opened.read(|doc| doc.is_editing_value()));
}

/// The secret's details are re-checked at save time: a secret that has
/// meanwhile become managed, or whose deletion is scheduled, refuses with
/// the vocabulary the uneditable cases use, and nothing is written.
#[gpui::test]
fn a_details_change_refuses_the_save(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.select(PLAINTEXT_SECRET);
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
    opened.start_edit();
    opened.set_editor_text(EDITED_VALUE);

    // A describe that landed while the editor was open now reports another
    // service as the owner.
    opened.update(|doc, _cx| {
        if let DetailsState::Loaded(details) = &mut doc.details {
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
    assert!(opened.read(|doc| doc.is_editing_value()));

    // The ownership clears again, but a deletion is scheduled meanwhile.
    opened.update(|doc, _cx| {
        if let DetailsState::Loaded(details) = &mut doc.details {
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
    assert!(opened.read(|doc| doc.is_editing_value()));
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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
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
    assert!(!opened.read(|doc| doc.is_editing_value()));
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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));

    let entries: Vec<String> = opened.window.update(|_, cx| {
        opened
            .doc
            .read(cx)
            .pane_actions(&opened.doc)
            .into_iter()
            .map(|action| action.id.to_string())
            .collect()
    });

    let capture = FrameCapture::observe(opened.window);
    let checked = Coverage::new(SECRET_STORE)
        .with_menu_entries(entries)
        .assert_covered(&capture.frame(opened.window));

    assert!(
        checked.iter().any(|id| id == "secret-value-edit"),
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
            .pane_actions(&opened.doc)
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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));

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
        opened.read(|doc| doc.is_editing_value()),
        "the menu opens the editor"
    );
    assert_eq!(opened.editor_text(), FAKE_SECRET_PLAINTEXT);
    assert!(
        !opened
            .rendered_text()
            .contains(&dbflux_i18n::t!("document.secret_store.edit.open")),
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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
    opened.start_edit();
    // Save is disabled while the text is unchanged, so the coverage check
    // runs with an edit in it.
    opened.set_editor_text(EDITED_VALUE);

    let entries: Vec<String> = opened.window.update(|_, cx| {
        opened
            .doc
            .read(cx)
            .pane_actions(&opened.doc)
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

const CURRENT_REGION: &str = "us-east-1";
const OTHER_REGION: &str = "eu-west-1";

/// An app state whose profile is an `aws_secrets` connection the manager can
/// switch by region: the fake driver is registered under the built-in driver
/// id the profile resolves to, so `prepare_switch_region` finds it.
fn connected_aws_app_state(
    cx: &mut TestAppContext,
    driver: &FakeDriver,
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
        "prod-aws",
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
    init_keyboard_runtime(cx);

    let driver = FakeDriver::new(DbKind::AwsSecrets).with_region_switch(regions);
    driver.set_region_list_error(list_error);
    let (app_state, profile_id) = connected_aws_app_state(cx, &driver);

    // `report_error` reaches the error seam through the `AppStateGlobal`
    // registration; without it a reported failure is invisible to these
    // tests.
    cx.update(|cx| {
        cx.set_global(AppStateGlobal {
            entity: app_state.clone(),
        });
    });

    let state = app_state.clone();
    let store_id = FAKE_SECRETS_STORE_ID.to_string();

    let (doc, window) = cx.add_window_view(move |window, cx| {
        SecretStoreDocument::new(profile_id, store_id, None, state, window, cx)
    });
    window.run_until_parked();

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

/// A failed region listing is reported through the error seam, shown with a
/// retry and never as an empty picker; the region itself stays visible as a
/// breadcrumb while the picker cannot be offered. Retrying asks the
/// connection again.
#[gpui::test]
fn a_failed_region_list_is_reported_and_retriable(cx: &mut TestAppContext) {
    let message = "AccessDenied: not authorized to perform ec2:DescribeRegions";
    let mut opened = open_region_doc(cx, &[CURRENT_REGION], Some(message.to_string()));

    assert!(matches!(
        opened.read(|doc| doc.regions().clone()),
        RegionListState::Failed(text) if text.contains(message)
    ));

    let text = opened.rendered_text();
    assert!(text.contains(message), "{text}");
    assert!(
        text.contains(&dbflux_i18n::t!("document.secret_store.region.retry")),
        "{text}"
    );
    assert!(
        text.contains(CURRENT_REGION),
        "the region stays visible while the picker cannot be offered: {text}"
    );

    let reported = opened
        .window
        .update(|_, cx| opened.app_state.read(cx).unread_error_count);
    assert_eq!(reported, 1, "the failed listing must reach the error seam");

    opened.driver.set_region_list_error(None);
    opened.update(|doc, cx| doc.load_regions(cx));

    assert_eq!(opened.driver.available_regions_calls(), 2);
    assert!(matches!(
        opened.read(|doc| doc.regions().clone()),
        RegionListState::Loaded(_)
    ));
}

/// A successful switch reconnects the profile under the chosen region,
/// reloads the listing from its first page and drops the selection, its
/// details and any retrieved value.
#[gpui::test]
fn switching_the_region_reloads_the_listing(cx: &mut TestAppContext) {
    let mut opened = open_with_regions(cx, &[OTHER_REGION, CURRENT_REGION]);
    opened.select(KEY_VALUE_SECRET);
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
    assert!(opened.read(|doc| matches!(doc.value(), ValueState::Loaded(_))));

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
    assert!(opened.read(|doc| matches!(doc.details(), DetailsState::Idle)));
    assert!(opened.read(|doc| matches!(doc.value(), ValueState::NotLoaded)));
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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
    assert!(opened.read(|doc| matches!(doc.value(), ValueState::Loaded(_))));

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
    assert!(opened.read(|doc| matches!(doc.details(), DetailsState::Idle)));
    assert!(opened.read(|doc| matches!(doc.value(), ValueState::NotLoaded)));

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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));
    assert!(opened.read(|doc| matches!(doc.value(), ValueState::Loaded(_))));

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
    assert!(opened.read(|doc| matches!(doc.details(), DetailsState::Loaded(_))));
    assert!(opened.read(|doc| matches!(doc.value(), ValueState::Loaded(_))));

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
    opened.update(|doc, cx| doc.retrieve_selected_value(cx));

    let entries: Vec<String> = opened.window.update(|_, cx| {
        opened
            .doc
            .read(cx)
            .pane_actions(&opened.doc)
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

/// The region picker and the retry of a failed region listing are only
/// rendered by a region-capable driver, so covering them needs documents of
/// their own: the surface above renders a driver without `REGION_SWITCH` and
/// says nothing about either control.
#[gpui::test]
fn the_region_controls_are_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::SECRET_STORE;
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture, clickable_elements};

    /// Checks the surface and returns the path of every clickable element, so
    /// the assertion names the control the way the registry does.
    fn checked_paths(opened: &mut Opened<'_>) -> Vec<String> {
        let entries: Vec<String> = opened.window.update(|_, cx| {
            opened
                .doc
                .read(cx)
                .pane_actions(&opened.doc)
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
    drop(switchable);

    let mut failing = open_region_doc(cx, &[CURRENT_REGION], Some("AccessDenied".to_string()));
    let paths = checked_paths(&mut failing);
    assert!(
        paths
            .iter()
            .any(|path| path.ends_with("secret-region-retry")),
        "a failed region listing renders its retry: {paths:?}"
    );

    // Both controls are pane actions too: the entries above validate that the
    // registry's Menu paths name real entries.
    let action_ids: Vec<String> = failing.window.update(|_, cx| {
        failing
            .doc
            .read(cx)
            .pane_actions(&failing.doc)
            .into_iter()
            .map(|action| action.id.to_string())
            .collect()
    });
    assert!(
        action_ids.iter().any(|id| id == "secret-region"),
        "the picker has a pane action: {action_ids:?}"
    );
    assert!(
        action_ids.iter().any(|id| id == "secret-region-retry"),
        "the retry has a pane action: {action_ids:?}"
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
                .pane_actions(&self.doc)
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
/// row becomes the selection, and the menu holds the read-only items only:
/// the write path does not exist yet, so no edit action may appear.
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
        vec!["RetrieveValue", "CopyName", "CopyArn"],
        "the menu lists the read-only items"
    );

    let text = opened.rendered_text();
    for label in ["Retrieve value", "Copy name", "Copy ARN"] {
        assert!(text.contains(label), "{label} missing: {text}");
    }
}

/// A secret managed by another service and a secret with a scheduled
/// deletion offer the same read-only items; nothing implies an edit.
#[gpui::test]
fn restricted_secrets_offer_the_same_read_only_items(cx: &mut TestAppContext) {
    for name in [MANAGED_SECRET, PENDING_DELETION_SECRET] {
        let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);

        opened.right_click(&format!("secret-row-{name}"));

        assert!(
            opened.read(|doc| matches!(
                doc.selected(),
                Some(SecretRowKey::Secret(selected)) if selected == name
            )),
            "{name} becomes the selection"
        );
        assert_eq!(
            opened.menu_actions(),
            vec!["RetrieveValue", "CopyName", "CopyArn"],
            "{name} offers the same read-only items"
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
            .first()
            .map(|item| (item.disabled, item.detail.clone()))
            .expect("Retrieve value is the first entry")
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
            .first()
            .map(|item| item.disabled)
            .expect("Retrieve value is the first entry")),
        "Retrieve value is enabled for a normal secret"
    );
}

/// A parameter row under a path offers Copy name and Retrieve value.
#[gpui::test]
fn a_parameter_row_offers_copy_name_and_retrieve_value(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_PARAMETER_STORE_ID, Some("/app/prod/"));

    opened.right_click("secret-row-/app/prod/region");

    assert_eq!(
        opened.menu_actions(),
        vec!["RetrieveValue", "CopyName"],
        "a parameter row offers Copy name and Retrieve value"
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

    // Copy name is the second entry of the menu.
    opened.click_menu_item("secret-menu-copy-name-item-1");

    assert_eq!(opened.clipboard(), Some(KEY_VALUE_SECRET.to_string()));
    assert!(
        !opened.rendered_text().contains("Copy name"),
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
        !opened.rendered_text().contains("Copy name"),
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
        !opened.rendered_text().contains("Copy name"),
        "a click outside closes the menu"
    );
}

/// The keyboard owns the menu: MenuSelect runs the highlighted entry
/// through the same action a click would, and MenuDown moves the highlight.
#[gpui::test]
fn the_keyboard_runs_the_menu(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);
    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));

    // Retrieve value is the first entry: selecting it right away retrieves.
    assert!(opened.command(Command::MenuSelect));
    assert!(
        opened.read(|doc| matches!(doc.value(), ValueState::Loading))
            || opened.read(|doc| matches!(doc.value(), ValueState::Loaded(_))),
        "MenuSelect on the highlighted Retrieve value retrieves"
    );
    assert!(
        !opened.rendered_text().contains("Copy name"),
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
                .any(|id| id == "secret-menu-retrieve-value-item-0"),
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
            .any(|id| id == "secret-menu-copy-arn-item-2"),
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
        shortcuts[0].as_deref().map(str::to_string),
        expected(Command::ToggleValuePanel),
        "Retrieve value shows the chord the keymap binds"
    );
    assert_eq!(shortcuts[1], None, "Copy name has no chord and shows none");
    assert_eq!(shortcuts[2], None, "Copy ARN has no chord and shows none");

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
    opened.update(|doc, _cx| doc.details = DetailsState::Loading);
    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));

    let detail = opened.read(|doc| {
        doc.context_menu
            .as_ref()
            .expect("menu open")
            .items
            .last()
            .map(|item| (item.disabled, item.detail.clone()))
            .expect("Copy ARN is the last entry")
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
    opened.update(|doc, _cx| doc.details = DetailsState::Failed("Permission denied".to_string()));
    opened.right_click(&format!("secret-row-{KEY_VALUE_SECRET}"));

    let detail = opened.read(|doc| {
        doc.context_menu
            .as_ref()
            .expect("menu open")
            .items
            .last()
            .map(|item| (item.disabled, item.detail.clone()))
            .expect("Copy ARN is the last entry")
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
    assert!(opened.command(Command::MenuSelect));

    assert!(
        !opened.menu_actions().is_empty(),
        "the menu stays open on a disabled entry"
    );
    assert_eq!(opened.read(|doc| doc.selected().cloned()), before);
    assert!(
        opened.read(|doc| matches!(doc.value(), ValueState::NotLoaded)),
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
        !opened.rendered_text().contains("Copy name"),
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
