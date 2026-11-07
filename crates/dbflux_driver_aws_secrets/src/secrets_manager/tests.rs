use std::collections::HashMap;

use dbflux_core::SecretPayload;
use dbflux_core::SecretVersionStage;
use dbflux_core::secrecy::ExposeSecret;
use dbflux_core::secrecy::SecretSlice;
use dbflux_core::secrecy::SecretString;

use super::*;

#[test]
fn a_response_without_a_value_yields_no_secret_value() {
    assert!(secret_value(Some("v1".to_string()), None, None).is_none());
}

#[test]
fn secret_string_becomes_a_text_value_with_its_version() {
    let value = secret_value(Some("v1".to_string()), Some("hunter2".to_string()), None)
        .expect("text value");

    assert_eq!(value.version.as_deref(), Some("v1"));
    match &value.payload {
        SecretPayload::Text(text) => assert_eq!(text.expose_secret(), "hunter2"),
        SecretPayload::Binary(_) => panic!("expected a text payload"),
    }
}

#[test]
fn secret_binary_becomes_a_binary_value() {
    let value = secret_value(None, None, Some(Blob::new(vec![0_u8, 1, 2]))).expect("binary");

    assert!(value.is_binary());
    match &value.payload {
        SecretPayload::Binary(bytes) => assert_eq!(bytes.expose_secret(), &[0_u8, 1, 2]),
        SecretPayload::Text(_) => panic!("expected a binary payload"),
    }
}

#[test]
fn rotation_schedule_prefers_the_expression_and_falls_back_to_days() {
    let expression = RotationRulesType::builder()
        .schedule_expression("cron(0 4 ? * SUN *)")
        .automatically_after_days(30)
        .build();
    assert_eq!(
        rotation_schedule(&expression).as_deref(),
        Some("cron(0 4 ? * SUN *)")
    );

    let days = RotationRulesType::builder()
        .automatically_after_days(30)
        .build();
    assert_eq!(rotation_schedule(&days).as_deref(), Some("rate(30 days)"));
}

#[test]
fn version_labels_are_sorted_and_distinct() {
    let versions = HashMap::from([
        (
            "b".to_string(),
            vec!["AWSPREVIOUS".to_string(), "custom".to_string()],
        ),
        ("a".to_string(), vec!["AWSCURRENT".to_string()]),
        ("c".to_string(), vec!["custom".to_string()]),
    ]);

    assert_eq!(
        version_labels(&versions),
        ["AWSCURRENT", "AWSPREVIOUS", "custom"]
    );
    assert_eq!(current_version(&versions).as_deref(), Some("a"));
}

/// A wrapped token carries the filter of its listing and the service token,
/// so the next request asks with the filter its token belongs to.
#[test]
fn a_page_token_carries_the_filter_of_its_listing() {
    let wrapped = encode_page_token(&FilterNameStringType::Name, "eyJhIjoiYiJ9");
    let (key, token) = decode_page_token(&wrapped).expect("a wrapped token decodes");
    assert!(matches!(key, FilterNameStringType::Name));
    assert_eq!(token, "eyJhIjoiYiJ9");

    let wrapped = encode_page_token(&FilterNameStringType::All, "eyJhIjoiYiJ9");
    let (key, token) = decode_page_token(&wrapped).expect("a wrapped token decodes");
    assert!(matches!(key, FilterNameStringType::All));
    assert_eq!(token, "eyJhIjoiYiJ9");
}

/// A service token is opaque, so a token that was not wrapped here is never
/// read as one of ours, whatever it starts with.
#[test]
fn an_unwrapped_token_is_not_read_as_a_wrapped_one() {
    for token in ["eyJhIjoiYiJ9", "name-abc", "sst1n", "sst1n:", "sst1n:zz"] {
        assert_eq!(decode_page_token(token), None, "{token}");
    }
    let (key, token, in_name_listing) = page_filter(Some("eyJhIjoiYiJ9"));
    assert!(matches!(key, FilterNameStringType::All));
    assert_eq!(token.as_deref(), Some("eyJhIjoiYiJ9"));
    assert!(!in_name_listing);
}

/// The prefix fallback runs for a search that matched nothing, and only then.
#[test]
fn the_prefix_fallback_runs_only_for_an_empty_first_page_of_a_search() {
    assert!(needs_prefix_fallback(
        true,
        false,
        Some("b2back"),
        true,
        false
    ));

    assert!(!needs_prefix_fallback(
        false,
        false,
        Some("b2back"),
        true,
        false
    ));
    assert!(!needs_prefix_fallback(true, false, None, true, false));
    assert!(!needs_prefix_fallback(
        true,
        false,
        Some("b2back"),
        false,
        false
    ));
    assert!(!needs_prefix_fallback(
        true,
        false,
        Some("b2back"),
        true,
        true
    ));
}

/// An empty page that carries a next token is part of a filtered listing, not
/// its end: falling back would drop the secrets the later pages hold.
#[test]
fn an_empty_page_with_a_next_token_keeps_the_listing() {
    assert!(!needs_prefix_fallback(
        true,
        true,
        Some("b2back"),
        true,
        false
    ));
}

/// A write carries an optional stage; only a staged write names its label
/// explicitly, and `Current` keeps the service default that moves
/// `AWSCURRENT` to the new version.
#[test]
fn a_write_stage_maps_to_its_service_label() {
    assert_eq!(write_stage_label(None), None);
    assert_eq!(write_stage_label(Some(SecretVersionStage::Current)), None);
    assert_eq!(
        write_stage_label(Some(SecretVersionStage::Pending)),
        Some(PENDING_STAGE)
    );
}

/// The real Secrets Manager store, built with a client that never reaches
/// the network in this test: the descriptor is read, not called.
fn offline_store() -> SecretsManagerStore {
    let config = aws_sdk_secretsmanager::config::Builder::new()
        .behavior_version(aws_sdk_secretsmanager::config::BehaviorVersion::latest())
        .region(aws_sdk_secretsmanager::config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_secretsmanager::config::Credentials::new(
            "test", "test", None, None, "unit",
        ))
        .build();

    SecretsManagerStore::new(
        aws_sdk_secretsmanager::Client::from_conf(config),
        AwsSecretsProfileConfig {
            region: "us-east-1".to_string(),
            profile: None,
            access_key_id: None,
            endpoint: None,
        },
    )
}

/// The store behind the real secrets asserts it can take writes through its
/// own descriptor, not a literal built for the test.
#[test]
fn the_real_secrets_manager_store_advertises_write() {
    assert!(
        offline_store()
            .descriptor()
            .supports(SecretStoreCapabilities::WRITE)
    );
}

#[test]
fn a_payload_maps_onto_the_exclusive_put_fields() {
    let text = put_payload(&SecretPayload::Text(SecretString::from("hunter2")));
    assert!(matches!(text, PutPayload::Text(ref value) if value == "hunter2"));

    let bytes = put_payload(&SecretPayload::Binary(SecretSlice::from(vec![0_u8, 1, 2])));
    assert!(matches!(bytes, PutPayload::Binary(ref value) if value == &vec![0_u8, 1, 2]));
}

/// A managed secret refuses with the store, the owning service and the
/// secret named; an unmanaged one does not refuse, so the write proceeds.
#[test]
fn a_managed_secret_refuses_and_an_unmanaged_one_proceeds() {
    let refusal = managed_write_refusal_for(Some("rds"), "prod/db/master")
        .expect("a managed secret refuses the write");

    match &refusal {
        DbError::PermissionDenied(formatted) => {
            assert!(
                formatted.message.contains("Secrets Manager"),
                "{}",
                formatted.message
            );
            assert!(formatted.message.contains("rds"), "{}", formatted.message);
            assert!(
                formatted.message.contains("prod/db/master"),
                "{}",
                formatted.message
            );
            assert!(formatted.hint.is_some(), "{}", formatted.message);
        }
        other => panic!("expected a permission denial, got {other}"),
    }

    assert!(managed_write_refusal_for(None, "prod/db/master").is_none());
}

/// A fallback listing keeps the `Name` filter on every page, so a token of one
/// filter is never sent with the other.
#[test]
fn a_fallback_listing_keeps_its_filter_on_every_page() {
    let wrapped_name = encode_page_token(&FilterNameStringType::Name, "abc");
    let (key, token, in_name_listing) = page_filter(Some(&wrapped_name));
    assert!(matches!(key, FilterNameStringType::Name));
    assert_eq!(token.as_deref(), Some("abc"));
    assert!(in_name_listing);

    let wrapped_all = encode_page_token(&FilterNameStringType::All, "abc");
    let (key, token, in_name_listing) = page_filter(Some(&wrapped_all));
    assert!(matches!(key, FilterNameStringType::All));
    assert_eq!(token.as_deref(), Some("abc"));
    assert!(!in_name_listing);

    let (key, token, in_name_listing) = page_filter(None);
    assert!(matches!(key, FilterNameStringType::All));
    assert!(token.is_none());
    assert!(!in_name_listing);
}

fn version_entry(
    id: &str,
    stages: &[&str],
    created_secs: Option<i64>,
) -> aws_sdk_secretsmanager::types::SecretVersionsListEntry {
    let mut builder =
        aws_sdk_secretsmanager::types::SecretVersionsListEntry::builder().version_id(id);

    for stage in stages {
        builder = builder.version_stages(*stage);
    }

    if let Some(secs) = created_secs {
        builder = builder.created_date(SmithyDateTime::from_secs(secs));
    }

    builder.build()
}

#[test]
fn versions_are_listed_newest_first_whatever_order_the_service_answers() {
    let entries = [
        version_entry("v-old", &[], Some(1_000)),
        version_entry("v-current", &["AWSCURRENT"], Some(3_000)),
        version_entry("v-previous", &["AWSPREVIOUS"], Some(2_000)),
    ];

    let versions = versions_from_entries(&entries);

    let ids: Vec<&str> = versions.iter().map(|version| version.id.as_str()).collect();
    assert_eq!(ids, ["v-current", "v-previous", "v-old"]);
    assert_eq!(versions[0].labels, ["AWSCURRENT"]);
    assert_eq!(versions[1].labels, ["AWSPREVIOUS"]);
}

#[test]
fn a_version_with_no_staging_label_is_kept() {
    let versions = versions_from_entries(&[version_entry("v-orphan", &[], Some(1_000))]);

    assert_eq!(versions.len(), 1);
    assert!(versions[0].labels.is_empty());
    assert_eq!(versions[0].id, "v-orphan");
    assert!(versions[0].created.is_some());
    assert_eq!(versions[0].modified_by, None);
}

#[test]
fn a_version_without_a_creation_date_sorts_last() {
    let versions = versions_from_entries(&[
        version_entry("v-undated", &[], None),
        version_entry("v-dated", &["AWSCURRENT"], Some(5)),
    ]);

    assert_eq!(versions[0].id, "v-dated");
    assert_eq!(versions[1].id, "v-undated");
    assert_eq!(versions[1].created, None);
}

#[test]
fn an_entry_without_a_version_id_is_dropped() {
    let entry = aws_sdk_secretsmanager::types::SecretVersionsListEntry::builder().build();

    assert!(versions_from_entries(&[entry]).is_empty());
}

#[test]
fn tied_creation_dates_keep_the_service_order() {
    let versions = versions_from_entries(&[
        version_entry("v-first", &["AWSPREVIOUS"], Some(1_000)),
        version_entry("v-second", &["AWSCURRENT"], Some(1_000)),
    ]);

    let ids: Vec<&str> = versions.iter().map(|version| version.id.as_str()).collect();
    assert_eq!(ids, ["v-first", "v-second"]);
}
