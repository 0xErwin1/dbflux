use secrecy::ExposeSecret;

use super::*;

fn exposed(pairs: Vec<(String, SecretString)>) -> Vec<(String, String)> {
    pairs
        .into_iter()
        .map(|(key, value)| (key, value.expose_secret().to_string()))
        .collect()
}

#[test]
fn flat_json_object_becomes_key_value_pairs_in_order() {
    let value = SecretValue::text(
        None,
        r#"{"username":"shop_app","port":5432,"tls":true,"note":null}"#,
    );

    let pairs = value.key_value_pairs().map(exposed);

    assert_eq!(
        pairs,
        Some(vec![
            ("username".to_string(), "shop_app".to_string()),
            ("port".to_string(), "5432".to_string()),
            ("tls".to_string(), "true".to_string()),
            ("note".to_string(), String::new()),
        ])
    );
}

#[test]
fn nested_json_array_or_plain_text_is_not_key_value() {
    for text in [
        r#"{"db":{"host":"x"}}"#,
        r#"{"hosts":["a","b"]}"#,
        r#"["a","b"]"#,
        r#"{}"#,
        "postgres://shop_app:secret@host:5432/shop",
        "42",
    ] {
        let value = SecretValue::text(None, text);
        assert!(value.key_value_pairs().is_none(), "{text}");
    }
}

#[test]
fn binary_value_is_never_key_value() {
    let value = SecretValue::binary(Some("v1".to_string()), br#"{"a":"b"}"#.to_vec());

    assert!(value.is_binary());
    assert!(value.key_value_pairs().is_none());
}

#[test]
fn debug_output_redacts_the_value() {
    let text = SecretValue::text(Some("v7".to_string()), "hunter2");
    let binary = SecretValue::binary(None, b"hunter2".to_vec());

    for rendered in [format!("{text:?}"), format!("{binary:?}")] {
        assert!(!rendered.contains("hunter2"), "{rendered}");
    }
    assert!(format!("{text:?}").contains("v7"));
}

#[test]
fn summary_flags_managed_and_pending_deletion() {
    let mut summary = SecretSummary::new("rds!db-7f3a");
    assert!(!summary.is_managed());
    assert!(!summary.is_pending_deletion());

    summary.owning_service = Some("rds".to_string());
    summary.deletion_scheduled_for = Some(Utc::now());

    assert!(summary.is_managed());
    assert!(summary.is_pending_deletion());
}

#[test]
fn descriptor_reports_its_capabilities() {
    let descriptor = SecretStoreDescriptor::new(
        "parameters",
        "Parameter Store",
        SecretStoreCapabilities::VERSIONS | SecretStoreCapabilities::HIERARCHICAL,
    );

    assert!(descriptor.supports(SecretStoreCapabilities::HIERARCHICAL));
    assert!(!descriptor.supports(SecretStoreCapabilities::ROTATION));
}
