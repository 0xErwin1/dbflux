use aws_sdk_ssm::config::{BehaviorVersion, Builder as ConfigBuilder, Credentials, Region};
use aws_sdk_ssm::types::{ParameterMetadata, ParameterType};
use dbflux_core::{
    DbError, SecretCreateRequest, SecretDeleteRequest, SecretPayload, SecretPutRequest,
    SecretStoreCapabilities, SecretStoreSource, SecretVersionStage,
};

use super::hierarchy::{group_level, normalize_path, path_filter_value};
use super::{
    ParameterStore, confirmed_deletion_name, description_write_error,
    ensure_unchanged_while_reading, ensure_written_after, existing_key_id, find_parameter_on_pages,
    list_versions_on_pages, overwrite_type, page_versions, plan_create, plan_description_write,
    put_parameter_call, put_text, sort_newest_first, ssm_tags,
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

#[test]
fn a_parameter_is_deleted_only_with_its_exact_name_typed_back() {
    let request = SecretDeleteRequest::irreversible("/app/prod/db/host", "/app/prod/db/host");
    let confirmed = confirmed_deletion_name(&request).expect("the exact name confirms");
    assert_eq!(confirmed, "/app/prod/db/host");

    for typed in [
        "",
        "/app/prod/db/hos",
        "/APP/PROD/DB/HOST",
        "/app/prod/db/host ",
    ] {
        let error = confirmed_deletion_name(&SecretDeleteRequest::irreversible(
            "/app/prod/db/host",
            typed,
        ))
        .expect_err("anything but the exact name is refused before the call");

        assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
    }
}

#[test]
fn a_parameter_has_no_recovery_window_to_schedule() {
    let error = confirmed_deletion_name(&SecretDeleteRequest::scheduled("/app/prod/db/host", 30))
        .expect_err("Parameter Store deletes at once");

    assert!(matches!(error, DbError::NotSupported(_)), "{error}");
    assert!(error.to_string().contains("/app/prod/db/host"), "{error}");
}

fn described_parameter() -> aws_sdk_ssm::types::builders::ParameterMetadataBuilder {
    ParameterMetadata::builder()
        .name("/app/prod/db/host")
        .r#type(ParameterType::String)
        .data_type("text")
        .tier(aws_sdk_ssm::types::ParameterTier::Standard)
        .version(3)
}

#[test]
fn a_description_write_re_sends_everything_the_parameter_has() {
    let metadata = described_parameter().allowed_pattern("^[a-z.]+$").build();

    let plan = plan_description_write(&metadata, "/app/prod/db/host", "Primary host")
        .expect("a plain String can be rewritten");

    assert_eq!(plan.description, "Primary host");
    assert_eq!(plan.parameter_type, ParameterType::String);
    assert_eq!(plan.data_type, "text");
    assert_eq!(plan.tier, aws_sdk_ssm::types::ParameterTier::Standard);
    assert_eq!(plan.allowed_pattern.as_deref(), Some("^[a-z.]+$"));
    assert_eq!(plan.key_id, None);
    assert_eq!(plan.read_version, 3);
}

#[test]
fn a_secure_string_description_write_keeps_its_key_and_refuses_an_unknown_one() {
    let keyed = described_parameter()
        .r#type(ParameterType::SecureString)
        .key_id("alias/app-prod")
        .tier(aws_sdk_ssm::types::ParameterTier::Advanced)
        .build();

    let plan = plan_description_write(&keyed, "/app/prod/db/host", "Password")
        .expect("a keyed SecureString");
    assert_eq!(plan.key_id.as_deref(), Some("alias/app-prod"));
    assert_eq!(plan.tier, aws_sdk_ssm::types::ParameterTier::Advanced);

    let unkeyed = described_parameter()
        .r#type(ParameterType::SecureString)
        .build();
    let error = plan_description_write(&unkeyed, "/app/prod/db/host", "Password")
        .expect_err("an unknown key would re-encrypt under the account default");
    assert!(error.to_string().contains("encryption key"), "{error}");
}

#[test]
fn a_description_write_fails_closed_on_anything_it_cannot_preserve() {
    let policy = aws_sdk_ssm::types::ParameterInlinePolicy::builder()
        .policy_text(r#"{"Type":"Expiration"}"#)
        .policy_type("Expiration")
        .policy_status("Pending")
        .build();

    for (label, metadata) in [
        ("policies", described_parameter().policies(policy).build()),
        (
            "no type",
            ParameterMetadata::builder()
                .name("/app/prod/db/host")
                .data_type("text")
                .tier(aws_sdk_ssm::types::ParameterTier::Standard)
                .version(3)
                .build(),
        ),
        (
            "no data type",
            ParameterMetadata::builder()
                .name("/app/prod/db/host")
                .r#type(ParameterType::String)
                .tier(aws_sdk_ssm::types::ParameterTier::Standard)
                .version(3)
                .build(),
        ),
        (
            "no tier",
            ParameterMetadata::builder()
                .name("/app/prod/db/host")
                .r#type(ParameterType::String)
                .data_type("text")
                .version(3)
                .build(),
        ),
    ] {
        let error =
            plan_description_write(&metadata, "/app/prod/db/host", "text").expect_err(label);

        assert!(
            error.to_string().contains("/app/prod/db/host"),
            "{label}: {error}"
        );
    }
}

#[test]
fn a_description_longer_than_the_parameter_store_takes_is_refused() {
    let metadata = described_parameter().build();

    plan_description_write(&metadata, "/app/prod/db/host", &"d".repeat(1024))
        .expect("1024 characters fit");
    let error = plan_description_write(&metadata, "/app/prod/db/host", &"d".repeat(1025))
        .expect_err("1025 do not");

    assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
}

#[test]
fn a_description_write_notices_a_value_that_changed_while_it_was_being_read() {
    ensure_unchanged_while_reading(3, 3, "/app/prod/db/host").expect("same version");
    let error = ensure_unchanged_while_reading(3, 4, "/app/prod/db/host")
        .expect_err("a newer version was written meanwhile");
    assert!(error.to_string().contains("changed"), "{error}");

    ensure_written_after(3, 4, "/app/prod/db/host").expect("the next version");
    let error = ensure_written_after(3, 5, "/app/prod/db/host")
        .expect_err("another version landed between the read and the write");
    assert!(error.to_string().contains("history"), "{error}");
}

#[test]
fn a_parameter_tag_becomes_an_ssm_tag_and_a_bad_one_never_gets_that_far() {
    let tags = ssm_tags(&[
        dbflux_core::SecretTag::new("env", "prod"),
        dbflux_core::SecretTag::new("owner", ""),
    ])
    .expect("tags");

    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0].key(), "env");
    assert_eq!(tags[0].value(), "prod");
    assert_eq!(tags[1].value(), "");
}

#[test]
fn a_parameter_description_cannot_be_cleared_through_a_rewrite() {
    let metadata = described_parameter().build();

    let error = plan_description_write(&metadata, "/app/prod/db/host", "")
        .expect_err("an empty description is not offered");

    assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
    assert!(error.to_string().contains("/app/prod/db/host"), "{error}");
}

fn create_plan(request: &SecretCreateRequest) -> Result<super::CreateParameterPlan, DbError> {
    plan_create(request, "us-east-1")
}

#[test]
fn a_parameter_create_defaults_to_a_string_and_takes_the_services_own_tier() {
    let plan = create_plan(&SecretCreateRequest::text(
        "/app/prod/db/host",
        "create-value-probe",
    ))
    .expect("a bare create");

    assert_eq!(plan.parameter_type, ParameterType::String);
    assert!(plan.tier.is_none(), "the service default applies");
    assert!(plan.key_id.is_none());
    assert!(plan.description.is_none());
}

#[test]
fn a_parameter_create_carries_its_type_tier_key_description_and_tags() {
    let request = SecretCreateRequest::text("/app/prod/db/password", "create-value-probe")
        .with_value_type("SecureString")
        .with_tier("Intelligent-Tiering")
        .with_encryption_key("alias/app-prod")
        .with_description("Database password")
        .with_tag("app", "shop");

    let plan = create_plan(&request).expect("a complete create");

    assert_eq!(plan.parameter_type, ParameterType::SecureString);
    assert_eq!(
        plan.tier,
        Some(aws_sdk_ssm::types::ParameterTier::IntelligentTiering)
    );
    assert_eq!(plan.key_id.as_deref(), Some("alias/app-prod"));
    assert_eq!(plan.description.as_deref(), Some("Database password"));
    assert_eq!(plan.tags.len(), 1);

    for (value_type, expected) in [
        ("String", ParameterType::String),
        ("StringList", ParameterType::StringList),
    ] {
        let plan =
            create_plan(&SecretCreateRequest::text("/app/list", "a,b").with_value_type(value_type))
                .expect("a type");
        assert_eq!(plan.parameter_type, expected);
    }
}

#[test]
fn a_parameter_create_refuses_what_the_service_would_before_any_call() {
    for request in [
        SecretCreateRequest::text("/app/x", "create-value-probe").with_value_type("Binary"),
        SecretCreateRequest::text("/app/x", "create-value-probe").with_tier("Platinum"),
        SecretCreateRequest::text("/app/x", "create-value-probe").with_encryption_key("alias/k"),
        SecretCreateRequest::text("/app/has space", "create-value-probe"),
        SecretCreateRequest::text("/aws/reserved", "create-value-probe"),
        SecretCreateRequest::text("AWS-prefixed", "create-value-probe"),
        SecretCreateRequest::text("/ssm/reserved", "create-value-probe"),
        SecretCreateRequest::text("/app/x", "create-value-probe")
            .with_description("d".repeat(1025)),
        SecretCreateRequest::text(
            format!("/{}", vec!["a"; 16].join("/")),
            "create-value-probe",
        ),
    ] {
        let error = create_plan(&request).expect_err("refused");

        assert!(
            matches!(error, DbError::ConstraintViolation(_)),
            "{request:?}: {error}"
        );
        assert!(!error.to_string().contains("create-value-probe"), "{error}");
    }

    create_plan(&SecretCreateRequest::text(
        format!("/{}", vec!["a"; 15].join("/")),
        "create-value-probe",
    ))
    .expect("fifteen levels are the maximum");
}

fn text_payload(value: &str) -> SecretPayload {
    SecretPayload::Text(dbflux_core::secrecy::SecretString::from(value.to_string()))
}

/// The value as typed, and the form a serializer writes for it inside a
/// JSON request body (`"` as `\"`, a newline as `\n`).
const RESENT: &str = "pa\"ss\nword";
const RESENT_JSON: &str = "pa\\\"ss\\nword";

#[test]
fn a_description_write_error_never_renders_the_resent_value_in_any_form() {
    let error = description_write_error(
        DbError::query_failed(format!("bad {RESENT_JSON} / {RESENT}")),
        &text_payload(RESENT),
    );

    let rendered = error.to_string();
    assert!(!rendered.contains(RESENT), "{rendered}");
    assert!(!rendered.contains(RESENT_JSON), "{rendered}");
    assert!(rendered.contains("[redacted]"), "{rendered}");
}

#[test]
fn a_description_write_error_is_scrubbed_in_the_code_and_location_fields_too() {
    let formatted = dbflux_core::FormattedError::new("rejected")
        .with_detail(format!("detail {RESENT}"))
        .with_hint(format!("hint {RESENT_JSON}"))
        .with_code(format!("code:{RESENT}"))
        .with_location(dbflux_core::ErrorLocation {
            schema: Some(RESENT.to_string()),
            table: Some(format!("t {RESENT_JSON}")),
            column: Some(RESENT.to_string()),
            constraint: Some(RESENT_JSON.to_string()),
        });

    let error = description_write_error(DbError::QueryFailed(formatted), &text_payload(RESENT));

    let rendered = format!("{error:?}");
    assert!(!rendered.contains("word"), "{rendered}");
    assert!(!rendered.contains("pa\\\"ss"), "{rendered}");
}

#[test]
fn the_highest_version_number_is_marked_current_whatever_the_page_order() {
    let versions = versions_from_history(&[
        history_entry(2, &[], Some(2_000), None),
        history_entry(10, &[], Some(3_000), None),
        history_entry(9, &[], Some(1_000), None),
    ]);

    let flags: Vec<(&str, bool)> = versions
        .iter()
        .map(|version| (version.id.as_str(), version.is_current))
        .collect();
    assert_eq!(flags, [("10", true), ("9", false), ("2", false)]);
}

#[test]
fn a_paged_history_marks_one_current_version_across_pages() {
    let first = versions_from_history(&[history_entry(1, &[], Some(1_000), None)]);
    let second = versions_from_history(&[history_entry(2, &[], Some(2_000), None)]);
    let mut pages = vec![(first, Some("next".to_string())), (second, None)].into_iter();

    let versions = list_versions_on_pages(|_| Ok(pages.next().expect("a page"))).unwrap();

    let current: Vec<&str> = versions
        .iter()
        .filter(|version| version.is_current)
        .map(|version| version.id.as_str())
        .collect();
    assert_eq!(current, ["2"]);
}

// --- Creating: names ---

#[test]
fn a_hierarchical_parameter_name_needs_its_leading_slash() {
    for name in ["app/prod/token", "a/b"] {
        let error = create_plan(&SecretCreateRequest::text(name, "create-value-probe"))
            .expect_err("refused");

        assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
        assert!(error.to_string().contains("leading slash"), "{error}");
    }

    create_plan(&SecretCreateRequest::text("plain", "v")).expect("a flat name needs none");
    create_plan(&SecretCreateRequest::text("/app/prod/token", "v")).expect("a path with its slash");
}

#[test]
fn a_parameter_name_leaves_room_for_the_arn_the_service_counts_with_it() {
    let longest_flat = "n".repeat(1011 - "arn:aws:ssm:us-east-1:123456789012:parameter/".len());

    plan_create(&SecretCreateRequest::text(&longest_flat, "v"), "us-east-1")
        .expect("a name that fits with its ARN");
    let error = plan_create(
        &SecretCreateRequest::text(format!("{longest_flat}n"), "v"),
        "us-east-1",
    )
    .expect_err("one more character does not");
    assert!(error.to_string().contains("ARN"), "{error}");

    let in_china = "arn:aws-cn:ssm:cn-north-1:123456789012:parameter/".len();
    plan_create(
        &SecretCreateRequest::text("n".repeat(1011 - in_china), "v"),
        "cn-north-1",
    )
    .expect("the China prefix is longer than the standard one and still fits");
    plan_create(
        &SecretCreateRequest::text("n".repeat(1011 - in_china + 1), "v"),
        "cn-north-1",
    )
    .expect_err("the partition's own prefix counts");
}

#[test]
fn a_description_rewrite_of_an_image_parameter_is_refused_before_any_read() {
    let metadata = described_parameter().data_type("aws:ec2:image").build();

    let error = plan_description_write(&metadata, "/app/ami", "latest image")
        .expect_err("the service validates the image asynchronously");

    assert!(error.to_string().contains("aws:ec2:image"), "{error}");
}

// --- A description rewrite against an endpoint that answers from a script ---

mod scripted_endpoint {
    use std::collections::{HashMap, VecDeque};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    /// A loopback HTTP endpoint answering JSON calls by their
    /// `X-Amz-Target` operation, one scripted answer per call in order, and
    /// recording the operation and body of every request it receives.
    pub(super) struct ScriptedEndpoint {
        pub(super) url: String,
        requests: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl ScriptedEndpoint {
        pub(super) fn start(script: Vec<(&'static str, &'static str)>) -> Self {
            let mut answers: HashMap<String, VecDeque<String>> = HashMap::new();
            for (operation, body) in script {
                answers
                    .entry(operation.to_string())
                    .or_default()
                    .push_back(body.to_string());
            }

            let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
            let url = format!("http://{}", listener.local_addr().expect("address"));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let recorded = Arc::clone(&requests);

            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { return };
                    let (operation, body) = read_request(&mut stream);
                    if let Ok(mut recorded) = recorded.lock() {
                        recorded.push((operation.clone(), body));
                    }

                    let answer = answers
                        .get_mut(&operation)
                        .and_then(VecDeque::pop_front)
                        .unwrap_or_else(|| {
                            r#"{"__type":"InternalServerError","message":"unscripted"}"#.to_string()
                        });
                    let status = if answer.contains("__type") {
                        "400 Bad Request"
                    } else {
                        "200 OK"
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/x-amz-json-1.1\r\n\
                         content-length: {}\r\nconnection: close\r\n\r\n{answer}",
                        answer.len()
                    );
                    stream.write_all(response.as_bytes()).ok();
                }
            });

            Self { url, requests }
        }

        pub(super) fn operations(&self) -> Vec<String> {
            self.requests
                .lock()
                .map(|requests| requests.iter().map(|(name, _)| name.clone()).collect())
                .unwrap_or_default()
        }

        pub(super) fn body_of(&self, operation: &str) -> Option<String> {
            self.requests.lock().ok().and_then(|requests| {
                requests
                    .iter()
                    .find(|(name, _)| name == operation)
                    .map(|(_, body)| body.clone())
            })
        }
    }

    fn read_request(stream: &mut std::net::TcpStream) -> (String, String) {
        let mut received = Vec::new();
        let mut chunk = [0u8; 4096];

        let header_end = loop {
            let read = stream.read(&mut chunk).unwrap_or(0);
            if read == 0 {
                break received.len();
            }
            received.extend_from_slice(&chunk[..read]);
            if let Some(position) = received.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };

        let headers = String::from_utf8_lossy(&received[..header_end]).to_string();
        let header = |name: &str| {
            headers
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.trim()
                        .eq_ignore_ascii_case(name)
                        .then(|| value.trim().to_string())
                })
                .unwrap_or_default()
        };
        let length: usize = header("content-length").parse().unwrap_or(0);

        while received.len() < header_end + length {
            let read = stream.read(&mut chunk).unwrap_or(0);
            if read == 0 {
                break;
            }
            received.extend_from_slice(&chunk[..read]);
        }

        let operation = header("x-amz-target")
            .rsplit('.')
            .next()
            .unwrap_or_default()
            .to_string();

        (
            operation,
            String::from_utf8_lossy(&received[header_end..]).to_string(),
        )
    }
}

use scripted_endpoint::ScriptedEndpoint;

fn store_at(url: &str) -> ParameterStore {
    let config = ConfigBuilder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("test", "test", None, None, "unit"))
        .endpoint_url(url)
        .retry_config(aws_sdk_ssm::config::retry::RetryConfig::disabled())
        .build();

    ParameterStore::new(
        aws_sdk_ssm::Client::from_conf(config),
        AwsSecretsProfileConfig {
            region: "us-east-1".to_string(),
            profile: None,
            access_key_id: None,
            endpoint: Some(url.to_string()),
        },
    )
}

const DESCRIBED: &str = r#"{"Parameters":[{"Name":"/app/prod/db/host","Type":"SecureString","KeyId":"alias/custom","Version":3,"Tier":"Standard","DataType":"text","AllowedPattern":"^[a-z0-9]+$","Description":"old"}]}"#;
const READ_AT_3: &str = r#"{"Parameter":{"Name":"/app/prod/db/host","Type":"SecureString","Value":"hunter2","Version":3}}"#;
const READ_AT_4: &str = r#"{"Parameter":{"Name":"/app/prod/db/host","Type":"SecureString","Value":"changed","Version":4}}"#;

#[test]
fn a_description_rewrite_sends_everything_the_parameter_has() {
    let endpoint = ScriptedEndpoint::start(vec![
        ("DescribeParameters", DESCRIBED),
        ("GetParameter", READ_AT_3),
        ("GetParameter", READ_AT_3),
        ("PutParameter", r#"{"Version":4,"Tier":"Standard"}"#),
    ]);

    let change = store_at(&endpoint.url)
        .update_description(&dbflux_core::SecretDescriptionUpdate::new(
            "/app/prod/db/host",
            "new description",
        ))
        .expect("the rewrite lands");

    assert_eq!(change.version.as_deref(), Some("4"));
    let body = endpoint.body_of("PutParameter").expect("a put");
    for expected in [
        r#""Name":"/app/prod/db/host""#,
        r#""Value":"hunter2""#,
        r#""Type":"SecureString""#,
        r#""KeyId":"alias/custom""#,
        r#""Tier":"Standard""#,
        r#""DataType":"text""#,
        r#""AllowedPattern":"^[a-z0-9]+$""#,
        r#""Description":"new description""#,
        r#""Overwrite":true"#,
    ] {
        assert!(body.contains(expected), "{expected} missing from {body}");
    }
}

#[test]
fn a_description_rewrite_is_refused_when_the_version_moves_just_before_the_write() {
    let endpoint = ScriptedEndpoint::start(vec![
        ("DescribeParameters", DESCRIBED),
        ("GetParameter", READ_AT_3),
        ("GetParameter", READ_AT_4),
        ("PutParameter", r#"{"Version":5,"Tier":"Standard"}"#),
    ]);

    let error = store_at(&endpoint.url)
        .update_description(&dbflux_core::SecretDescriptionUpdate::new(
            "/app/prod/db/host",
            "new description",
        ))
        .expect_err("the parameter moved");

    assert!(error.to_string().contains("changed"), "{error}");
    assert!(error.to_string().contains("nothing was written"), "{error}");
    assert!(
        !endpoint.operations().contains(&"PutParameter".to_string()),
        "{:?}",
        endpoint.operations()
    );
}

/// The create dialog renders what the descriptor declares, so every declared
/// type and tier must be one `plan_create` takes, and the defaults are the
/// ones the dialog always started with.
#[test]
fn the_parameter_store_declares_the_choices_its_create_accepts() {
    let store = offline_store();
    let choices = store
        .descriptor()
        .create_choices
        .as_ref()
        .expect("a typed store declares its choices");

    assert_eq!(
        choices.value_types(),
        ["String", "StringList", "SecureString"]
    );
    assert_eq!(choices.default_value_type(), "SecureString");
    assert!(choices.takes_encryption_key("SecureString"));
    assert!(!choices.takes_encryption_key("String"));
    assert_eq!(
        choices.tiers(),
        ["Standard", "Advanced", "Intelligent-Tiering"]
    );
    assert_eq!(choices.default_tier(), Some("Standard"));

    for value_type in choices.value_types() {
        for tier in choices.tiers() {
            let request = SecretCreateRequest::text("/app/token", "v")
                .with_value_type(value_type.as_str())
                .with_tier(tier.as_str());

            plan_create(&request, "us-east-1")
                .unwrap_or_else(|error| panic!("{value_type}/{tier}: {error}"));
        }
    }
}
