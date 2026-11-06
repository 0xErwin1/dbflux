use aws_sdk_ssm::config::{BehaviorVersion, Builder as ConfigBuilder, Credentials, Region};
use aws_sdk_ssm::types::{ParameterMetadata, ParameterType};
use dbflux_core::{
    DbError, SecretPutRequest, SecretStoreCapabilities, SecretStoreSource, SecretVersionStage,
};

use super::hierarchy::{group_level, normalize_path, path_filter_value};
use super::{ParameterStore, existing_key_id, overwrite_type, put_parameter_call, put_text};
use crate::driver::AwsSecretsProfileConfig;

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
        .build();
    let call = put_parameter_call(&string_list, &request).expect("a StringList overwrite");
    assert_eq!(call.parameter_type, ParameterType::StringList);
    assert_eq!(call.key_id, None);
    assert_eq!(call.value, "a,b");

    let keyed = ParameterMetadata::builder()
        .name("/app/prod/db/password")
        .r#type(ParameterType::SecureString)
        .key_id("alias/app-prod")
        .build();
    let password = SecretPutRequest::text("/app/prod/db/password", "rotated");
    let call = put_parameter_call(&keyed, &password).expect("a keyed SecureString overwrite");
    assert_eq!(call.parameter_type, ParameterType::SecureString);
    assert_eq!(call.key_id.as_deref(), Some("alias/app-prod"));

    let unkeyed = ParameterMetadata::builder()
        .name("/app/prod/db/password")
        .r#type(ParameterType::SecureString)
        .build();
    let error = put_parameter_call(&unkeyed, &password)
        .expect_err("an unkeyed SecureString refuses the overwrite");
    let rendered = error.to_string();
    assert!(rendered.contains("Parameter Store"), "{rendered}");
    assert!(rendered.contains("/app/prod/db/password"), "{rendered}");
    assert!(!rendered.contains("a,b"), "{rendered}");
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
