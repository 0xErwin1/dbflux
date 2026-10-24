use super::listing::{SecretListing, child_path_label, parent_path, path_segments};
use super::value::{
    RetrievalTarget, ValueContent, ValueFailure, encode_base64, value_retrieval_event,
};
use super::{
    DetailsState, PathMode, SecretRowKey, SecretStoreDocument, VALUE_RETRIEVE_FAILED_ACTION,
    VALUE_RETRIEVED_ACTION, ValueState, ValueView,
};
use crate::keyboard_test_support::{connected_app_state, init_keyboard_runtime};
use dbflux_app::keymap::Command;
use dbflux_core::secrecy::ExposeSecret as _;
use dbflux_core::{DbKind, SecretListPage, SecretSummary, SecretValue};
use dbflux_test_support::fake_driver::{
    FAKE_KMS_DENIED_MESSAGE, FAKE_PARAMETER_STORE_ID, FAKE_SECRET_BINARY, FAKE_SECRET_PASSWORD,
    FAKE_SECRET_PLAINTEXT, FAKE_SECRETS_STORE_ID, FakeDriver,
};
use dbflux_ui_base::AppStateEntity;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use uuid::Uuid;

const KEY_VALUE_SECRET: &str = "prod/api/credentials";
const MANAGED_SECRET: &str = "prod/db/master";
const PENDING_DELETION_SECRET: &str = "prod/deploy/legacy-token";
const PLAINTEXT_SECRET: &str = "prod/notify/webhook-token";
const BINARY_SECRET: &str = "prod/tls/signing-key";
const DENIED_PARAMETER: &str = "/app/prod/db/password";

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
fn the_listing_is_paged_and_never_reads_a_value(cx: &mut TestAppContext) {
    let mut opened = open(cx, FAKE_SECRETS_STORE_ID, None);

    assert_eq!(opened.entry_names().len(), 4);
    assert!(opened.read(|doc| doc.listing().has_more()));
    assert!(opened.read(|doc| doc.rows().contains(&SecretRowKey::LoadMore)));

    assert!(opened.command(Command::ResultsNextPage));

    assert_eq!(opened.entry_names().len(), 6);
    assert!(!opened.read(|doc| doc.listing().has_more()));

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
