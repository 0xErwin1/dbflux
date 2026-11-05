use std::collections::HashMap;

use dbflux_core::SecretPayload;
use dbflux_core::secrecy::ExposeSecret;

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

/// The fallback listing is paged with a token the service returned for the
/// `Name` filter, marked so the next request asks with that same filter.
#[test]
fn a_prefix_page_token_carries_its_service_token() {
    assert_eq!(
        decode_prefix_token(&encode_prefix_token("eyJhIjoiYiJ9")),
        Some("eyJhIjoiYiJ9")
    );
}

/// A token the service returned for the `all` filter is passed back untouched.
#[test]
fn a_service_page_token_is_not_read_as_a_prefix_one() {
    assert_eq!(decode_prefix_token("eyJhIjoiYiJ9"), None);
    assert_eq!(decode_prefix_token("name-abc"), None);
    assert_eq!(decode_prefix_token("xname|abc"), None);
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

/// A fallback listing keeps the `Name` filter on every page, so a token of one
/// filter is never sent with the other.
#[test]
fn a_fallback_listing_keeps_its_filter_on_every_page() {
    let (key, token, in_prefix_listing) = page_filter(Some("name|abc"));
    assert!(matches!(key, FilterNameStringType::Name));
    assert_eq!(token.as_deref(), Some("abc"));
    assert!(in_prefix_listing);

    let (key, token, in_prefix_listing) = page_filter(Some("abc"));
    assert!(matches!(key, FilterNameStringType::All));
    assert_eq!(token.as_deref(), Some("abc"));
    assert!(!in_prefix_listing);

    let (key, token, in_prefix_listing) = page_filter(None);
    assert!(matches!(key, FilterNameStringType::All));
    assert!(token.is_none());
    assert!(!in_prefix_listing);
}
