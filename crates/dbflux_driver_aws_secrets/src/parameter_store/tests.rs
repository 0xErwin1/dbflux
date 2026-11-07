use aws_sdk_ssm::config::{BehaviorVersion, Builder as ConfigBuilder, Credentials, Region};
use aws_sdk_ssm::types::{ParameterMetadata, ParameterType};
use dbflux_core::{
    DbError, SecretPutRequest, SecretStoreCapabilities, SecretStoreSource, SecretVersionStage,
};

use super::hierarchy::{group_level, normalize_path, path_filter_value};
use super::{
    ParameterStore, existing_key_id, find_parameter_on_pages, list_versions_on_pages,
    overwrite_type, page_versions, put_parameter_call, put_text, sort_newest_first,
};
use crate::driver::AwsSecretsProfileConfig;

use std::cell::RefCell;
use std::rc::Rc;

fn group(path: &str, names: &[&str]) -> (Vec<String>, Vec<String>) {
    let items = names.iter().map(|name| name.to_string()).collect();
    group_level(path, items, |name| name.as_str())
}

#[test]
fn normalize_path_maps_missing_and_bare_paths_to_the_root() {
    assert_eq!(normalize_path(None), "/");
    assert_eq!(normalize_path(Some("")), "/");
    assert_eq!(normalize_path(Some("/")), "/");
}

#[test]
fn normalize_path_adds_the_leading_and_trailing_slash() {
    assert_eq!(normalize_path(Some("/app/prod/")), "/app/prod/");
    assert_eq!(normalize_path(Some("/app/prod")), "/app/prod/");
    assert_eq!(normalize_path(Some("app/prod")), "/app/prod/");
}

#[test]
fn path_filter_is_absent_at_the_root_and_drops_the_trailing_slash_elsewhere() {
    assert_eq!(path_filter_value("/"), None);
    assert_eq!(
        path_filter_value("/app/prod/"),
        Some("/app/prod".to_string())
    );
}

#[test]
fn root_level_splits_entries_from_child_paths() {
    let (entries, child_paths) = group(
        "/",
        &["plain", "/top", "/app/prod/db", "/app/dev/db", "/other/x"],
    );

    assert_eq!(entries, ["plain", "/top"]);
    assert_eq!(child_paths, ["/app/", "/other/"]);
}

#[test]
fn nested_level_returns_direct_entries_and_distinct_child_paths() {
    let (entries, child_paths) = group(
        "/app/",
        &[
            "/app/name",
            "/app/prod/db/password",
            "/app/prod/db/user",
            "/app/dev/url",
            "/app/prod/url",
        ],
    );

    assert_eq!(entries, ["/app/name"]);
    assert_eq!(child_paths, ["/app/prod/", "/app/dev/"]);
}

#[test]
fn names_outside_the_path_are_dropped() {
    let (entries, child_paths) = group("/app/", &["/application/x", "/other/app/y", "plain"]);

    assert!(entries.is_empty());
    assert!(child_paths.is_empty());
}

// --- Reading a parameter's metadata across pages ---

/// A canned `DescribeParameters` listing: each call answers the next page
/// and records the token it was asked with, the way a paged service would.
fn paged_fetch(
    pages: Vec<Vec<ParameterMetadata>>,
    received_tokens: Rc<RefCell<Vec<Option<String>>>>,
) -> impl FnMut(Option<String>) -> Result<(Vec<ParameterMetadata>, Option<String>), DbError> {
    let mut next_page = 0;
    move |token| {
        received_tokens.borrow_mut().push(token);
        let page = pages.get(next_page).cloned().unwrap_or_default();
        next_page += 1;
        let next_token = (next_page < pages.len()).then(|| format!("token-{}", next_page - 1));
        Ok((page, next_token))
    }
}

fn parameter_named(name: &str) -> ParameterMetadata {
    ParameterMetadata::builder().name(name).build()
}

/// The parameter the listing names is answered with, wherever its page sits.
#[test]
fn a_metadata_read_walks_pages_until_it_finds_the_parameter() {
    let tokens = Rc::new(RefCell::new(Vec::new()));
    let found = find_parameter_on_pages(
        "/app/prod/db/password",
        paged_fetch(
            vec![
                vec![parameter_named("/app/prod/db/host")],
                vec![parameter_named("/app/prod/db/password")],
                vec![parameter_named("/app/prod/db/port")],
            ],
            tokens.clone(),
        ),
    )
    .expect("the listing is walked")
    .expect("the parameter sits on a later page");
    assert_eq!(found.name(), Some("/app/prod/db/password"));

    // The walk asked page two with the token page one carried, not with an
    // empty request that would answer the first page again.
    assert_eq!(
        tokens.borrow().as_slice(),
        [None, Some("token-0".to_string())]
    );
}

/// A parameter on no page is absent: the walk ends when a page without the
/// parameter also ends the listing, and the caller turns that into the
/// not-found error.
#[test]
fn a_metadata_read_of_an_absent_parameter_walks_the_whole_listing() {
    let tokens = Rc::new(RefCell::new(Vec::new()));
    let absent = find_parameter_on_pages(
        "/app/prod/db/missing",
        paged_fetch(
            vec![
                vec![parameter_named("/app/prod/db/host")],
                vec![parameter_named("/app/prod/db/port")],
            ],
            tokens.clone(),
        ),
    )
    .expect("the listing is walked");
    assert!(absent.is_none());
    // Two pages: the second page ends the listing, so the walk stops there.
    assert_eq!(
        tokens.borrow().as_slice(),
        [None, Some("token-0".to_string())]
    );
}

/// A fetch that fails fails the read: the walk never turns a service error
/// into a missing parameter.
#[test]
fn a_failing_page_fails_the_metadata_read() {
    let error = find_parameter_on_pages("/app/prod/db/host", |_token| {
        Err(DbError::query_failed("DescribeParameters is denied"))
    })
    .expect_err("a service error propagates");

    assert!(error.to_string().contains("denied"), "{error}");
}

/// The real Parameter Store with a client that never reaches the network in
/// these tests: the refusals they check come before any call.
fn offline_store() -> ParameterStore {
    let config = ConfigBuilder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("test", "test", None, None, "unit"))
        .build();

    ParameterStore::new(
        aws_sdk_ssm::Client::from_conf(config),
        AwsSecretsProfileConfig {
            region: "us-east-1".to_string(),
            profile: None,
            access_key_id: None,
            endpoint: None,
        },
    )
}

/// The existing type is passed explicitly on every overwrite: `String`,
/// `StringList` and `SecureString` all reach the service by name. The SDK's
/// `PutParameter` model makes the type optional when updating; this driver
/// does not rely on how the service resolves an omitted one. An unknown
/// type refuses instead of guessing, naming the store and the parameter.
#[test]
fn an_overwrite_passes_the_existing_parameter_type_explicitly() {
    assert_eq!(
        overwrite_type(Some(&ParameterType::String), "/app/prod/db/host").expect("String"),
        ParameterType::String
    );
    assert_eq!(
        overwrite_type(Some(&ParameterType::StringList), "/app/prod/db/hosts").expect("StringList"),
        ParameterType::StringList
    );
    assert_eq!(
        overwrite_type(Some(&ParameterType::SecureString), "/app/prod/db/password")
            .expect("SecureString"),
        ParameterType::SecureString
    );

    let error = overwrite_type(None, "/app/prod/db/host").expect_err("an unknown type refuses");
    let rendered = error.to_string();
    assert!(rendered.contains("Parameter Store"), "{rendered}");
    assert!(rendered.contains("/app/prod/db/host"), "{rendered}");
}

/// A `SecureString` overwrite re-encrypts the value, so it keeps the key the
/// parameter is already encrypted with: a parameter whose key is unknown
/// refuses instead of silently moving the value to the account default key,
/// naming the store and the parameter.
#[test]
fn a_secure_string_overwrite_keeps_its_key_and_refuses_an_unknown_one() {
    let keyed = ParameterMetadata::builder()
        .name("/app/prod/db/password")
        .key_id("alias/app-prod")
        .build();
    assert_eq!(
        existing_key_id(&keyed, "/app/prod/db/password").expect("the key is known"),
        "alias/app-prod"
    );

    let unkeyed = ParameterMetadata::builder()
        .name("/app/prod/db/password")
        .build();
    let error = existing_key_id(&unkeyed, "/app/prod/db/password")
        .expect_err("an unknown key refuses the overwrite");
    let rendered = error.to_string();
    assert!(rendered.contains("Parameter Store"), "{rendered}");
    assert!(rendered.contains("/app/prod/db/password"), "{rendered}");
}

/// The call shape a `PutParameter` overwrite sends, decided from the
/// metadata alone: a `StringList` keeps `StringList` and takes no key, a
/// keyed `SecureString` keeps `SecureString` and its key, and an unkeyed
/// `SecureString` refuses. The type is always sent and the key is always
/// decided.
#[test]
fn a_put_parameter_call_carries_the_existing_type_and_key() {
    let request = SecretPutRequest::text("/app/prod/db/hosts", "a,b");

    let string_list = ParameterMetadata::builder()
        .name("/app/prod/db/hosts")
        .r#type(ParameterType::StringList)
        .data_type("text")
        .build();
    let call = put_parameter_call(&string_list, &request).expect("a StringList overwrite");
    assert_eq!(call.parameter_type, ParameterType::StringList);
    assert_eq!(call.key_id, None);
    assert_eq!(call.value, "a,b");

    let keyed = ParameterMetadata::builder()
        .name("/app/prod/db/password")
        .r#type(ParameterType::SecureString)
        .data_type("text")
        .key_id("alias/app-prod")
        .build();
    let password = SecretPutRequest::text("/app/prod/db/password", "rotated");
    let call = put_parameter_call(&keyed, &password).expect("a keyed SecureString overwrite");
    assert_eq!(call.parameter_type, ParameterType::SecureString);
    assert_eq!(call.key_id.as_deref(), Some("alias/app-prod"));

    let unkeyed = ParameterMetadata::builder()
        .name("/app/prod/db/password")
        .r#type(ParameterType::SecureString)
        .data_type("text")
        .build();
    let error = put_parameter_call(&unkeyed, &password)
        .expect_err("an unkeyed SecureString refuses the overwrite");
    let rendered = error.to_string();
    assert!(rendered.contains("Parameter Store"), "{rendered}");
    assert!(rendered.contains("/app/prod/db/password"), "{rendered}");
    // The value the refused write carried must never surface in its error.
    assert!(!rendered.contains("rotated"), "{rendered}");
}

/// The data type travels with the type: the overwrite sends the parameter's
/// existing `DataType` explicitly, so the service never applies its own
/// default, and a parameter whose metadata reports no data type refuses
/// rather than letting the service default it. The refusal names the store
/// and the parameter.
#[test]
fn an_overwrite_carries_the_existing_data_type_and_refuses_when_it_is_unknown() {
    let request = SecretPutRequest::text("/app/prod/db/host", "db.prod.internal");

    let typed = ParameterMetadata::builder()
        .name("/app/prod/db/host")
        .r#type(ParameterType::String)
        .data_type("text")
        .build();
    let call = put_parameter_call(&typed, &request).expect("a typed overwrite");
    assert_eq!(call.data_type, "text");

    let untyped = ParameterMetadata::builder()
        .name("/app/prod/db/host")
        .r#type(ParameterType::String)
        .build();
    let error = put_parameter_call(&untyped, &request)
        .expect_err("a parameter without a reported data type refuses the overwrite");
    let rendered = error.to_string();
    assert!(rendered.contains("Parameter Store"), "{rendered}");
    assert!(rendered.contains("/app/prod/db/host"), "{rendered}");
    assert!(!rendered.contains("db.prod.internal"), "{rendered}");
}

/// The store behind the real parameter asserts it can take writes through
/// its own descriptor, not a literal built for the test.
#[test]
fn the_real_parameter_store_advertises_write() {
    assert!(
        offline_store()
            .descriptor()
            .supports(SecretStoreCapabilities::WRITE)
    );
}

/// A refusal the store decides before any call names the store and the
/// parameter, and a refusal for a text write never carries the value that
/// write would have stored.
#[test]
fn the_real_store_refuses_a_staged_write_before_any_call() {
    let store = offline_store();
    let staged = SecretPutRequest::text("/app/prod/db/host", "hunter2")
        .with_stage(SecretVersionStage::Pending);

    let error = store
        .put_value(&staged)
        .expect_err("a staged write has no parameter-store shape");

    match &error {
        DbError::NotSupported(message) => {
            assert!(message.contains("Parameter Store"), "{message}");
            assert!(message.contains("/app/prod/db/host"), "{message}");
            assert!(!message.contains("hunter2"), "{message}");
        }
        other => panic!("expected a not-supported refusal, got {other:?}"),
    }
}

#[test]
fn the_real_store_refuses_a_binary_write_without_any_call() {
    let store = offline_store();
    let binary = SecretPutRequest::binary("/app/prod/db/host", vec![0_u8, 159]);

    let error = store
        .put_value(&binary)
        .expect_err("a binary value has no parameter-store shape");

    match &error {
        DbError::NotSupported(message) => {
            assert!(message.contains("Parameter Store"), "{message}");
            assert!(message.contains("/app/prod/db/host"), "{message}");
        }
        other => panic!("expected a not-supported refusal, got {other:?}"),
    }
}

/// A staged write has no parameter-store shape: the store versions by
/// number, not by staging labels.
#[test]
fn a_pending_write_is_refused_on_the_parameter_store() {
    let named = SecretPutRequest::text("/app/prod/db/host", "db");
    assert!(put_text(&named).is_ok());

    let staged =
        SecretPutRequest::text("/app/prod/db/host", "db").with_stage(SecretVersionStage::Pending);

    match put_text(&staged) {
        Err(DbError::NotSupported(message)) => {
            assert!(message.contains("staging"), "{message}");
            assert!(message.contains("/app/prod/db/host"), "{message}");
        }
        other => panic!("expected a not-supported refusal, got {other:?}"),
    }
}

#[test]
fn a_binary_value_is_refused_on_the_parameter_store() {
    let binary = SecretPutRequest::binary("/app/prod/db/host", vec![0_u8, 1]);

    match put_text(&binary) {
        Err(DbError::NotSupported(message)) => {
            assert!(message.contains("text"), "{message}");
            assert!(message.contains("/app/prod/db/host"), "{message}");
        }
        other => panic!("expected a not-supported refusal, got {other:?}"),
    }
}

/// A write failure must never carry the value it was writing.
#[test]
fn a_parameter_write_failure_never_carries_the_value() {
    let staged = SecretPutRequest::text("/app/prod/db/host", "hunter2")
        .with_stage(SecretVersionStage::Pending);
    let refusal = put_text(&staged).expect_err("a refusal for a staged write");

    let rendered = refusal.to_string();
    assert!(rendered.contains("/app/prod/db/host"), "{rendered}");
    assert!(!rendered.contains("hunter2"), "{rendered}");
}

fn versions_from_history(
    entries: &[aws_sdk_ssm::types::ParameterHistory],
) -> Vec<dbflux_core::SecretVersionInfo> {
    let mut versions: Vec<_> = page_versions(entries).collect();
    sort_newest_first(&mut versions);
    versions
}

const HISTORY_SENTINEL: &str = "sentinel-value-must-never-surface-4c1f";

fn history_entry(
    version: i64,
    labels: &[&str],
    modified_secs: Option<i64>,
    user: Option<&str>,
) -> aws_sdk_ssm::types::ParameterHistory {
    let mut builder = aws_sdk_ssm::types::ParameterHistory::builder()
        .name("/app/prod/db/password")
        .version(version)
        .value(HISTORY_SENTINEL);

    for label in labels {
        builder = builder.labels(*label);
    }

    if let Some(secs) = modified_secs {
        builder = builder.last_modified_date(aws_smithy_types::DateTime::from_secs(secs));
    }

    if let Some(user) = user {
        builder = builder.last_modified_user(user);
    }

    builder.build()
}

#[test]
fn history_is_listed_newest_version_first_with_labels_and_author() {
    let entries = [
        history_entry(1, &[], Some(1_000), Some("arn:aws:iam::1:user/ana")),
        history_entry(3, &["prod"], Some(3_000), Some("arn:aws:iam::1:user/bo")),
        history_entry(2, &["stable", "prod-old"], Some(2_000), None),
    ];

    let versions = versions_from_history(&entries);

    let ids: Vec<&str> = versions.iter().map(|version| version.id.as_str()).collect();
    assert_eq!(ids, ["3", "2", "1"]);
    assert_eq!(versions[0].labels, ["prod"]);
    assert_eq!(
        versions[0].modified_by.as_deref(),
        Some("arn:aws:iam::1:user/bo")
    );
    assert_eq!(versions[1].labels, ["stable", "prod-old"]);
    assert_eq!(versions[1].modified_by, None);
    assert!(versions[2].labels.is_empty());
    assert!(versions[2].created.is_some());
}

#[test]
fn a_history_row_never_carries_the_parameter_value() {
    let entries = [
        history_entry(2, &["prod"], Some(2_000), Some("arn:aws:iam::1:user/bo")),
        history_entry(1, &[], Some(1_000), None),
    ];

    let versions = versions_from_history(&entries);

    assert!(!format!("{versions:?}").contains(HISTORY_SENTINEL));
}

#[test]
fn a_history_id_is_the_selector_get_value_builds() {
    let versions = versions_from_history(&[history_entry(7, &[], None, None)]);

    assert_eq!(
        format!("/app/prod/db/password:{}", versions[0].id),
        "/app/prod/db/password:7"
    );
}

#[test]
fn history_paging_forwards_the_token_and_keeps_the_later_pages() {
    let tokens = std::cell::RefCell::new(Vec::new());

    let versions = list_versions_on_pages(|token| {
        tokens.borrow_mut().push(token.clone());
        Ok(match token.as_deref() {
            None => (
                page_versions(&[history_entry(1, &[], Some(1_000), None)]).collect(),
                Some("page-2".to_string()),
            ),
            Some(_) => (
                page_versions(&[history_entry(2, &["prod"], Some(2_000), None)]).collect(),
                None,
            ),
        })
    })
    .expect("both pages are read");

    assert_eq!(*tokens.borrow(), [None, Some("page-2".to_string())]);
    let ids: Vec<&str> = versions.iter().map(|version| version.id.as_str()).collect();
    assert_eq!(ids, ["2", "1"]);
    assert!(!format!("{versions:?}").contains(HISTORY_SENTINEL));
}
