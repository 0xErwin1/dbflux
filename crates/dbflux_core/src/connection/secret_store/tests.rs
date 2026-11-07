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

// --- Writing a value ---

fn descriptor_with(capabilities: SecretStoreCapabilities) -> SecretStoreDescriptor {
    SecretStoreDescriptor::new("test-store", "Test store", capabilities)
}

fn put_refusal_message(error: DbError) -> String {
    match error {
        DbError::NotSupported(message) => message,
        other => panic!("expected a NotSupported refusal, got {other}"),
    }
}

/// A store whose `put_value` starts with the capability gate every real
/// implementation uses. Without the gate it would write, which is the
/// success the seam must refuse.
struct GateOnlyStore(SecretStoreDescriptor);

impl SecretStoreSource for GateOnlyStore {
    fn descriptor(&self) -> &SecretStoreDescriptor {
        &self.0
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
        self.0.ensure_can_write(&request.name)?;
        Ok(SecretValue::text(
            Some("v-bypassed".to_string()),
            "the gate must stop this",
        ))
    }
}

/// A store that advertises `WRITE` but does not implement `put_value`, so
/// the trait default answers.
struct DefaultOnlyStore(SecretStoreDescriptor);

impl SecretStoreSource for DefaultOnlyStore {
    fn descriptor(&self) -> &SecretStoreDescriptor {
        &self.0
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
}

#[test]
fn a_descriptor_without_write_refuses_a_put_before_any_call() {
    let error = descriptor_with(SecretStoreCapabilities::VERSIONS)
        .ensure_can_write("prod/api/credentials")
        .expect_err("a store that cannot write must refuse");

    let message = put_refusal_message(error);
    assert!(message.contains("test-store"), "{message}");
    assert!(message.contains("prod/api/credentials"), "{message}");
}

#[test]
fn a_descriptor_with_write_accepts_a_put() {
    descriptor_with(SecretStoreCapabilities::WRITE | SecretStoreCapabilities::VERSIONS)
        .ensure_can_write("prod/api/credentials")
        .expect("a store that advertises WRITE accepts puts");
}

#[test]
fn a_store_that_does_not_advertise_write_refuses_even_an_implemented_put() {
    let store = GateOnlyStore(descriptor_with(SecretStoreCapabilities::VERSIONS));
    let request = SecretPutRequest::text("/app/prod/db/host", "hunter2");

    let error = store
        .put_value(&request)
        .expect_err("the gate must refuse a write to a store without WRITE");

    let message = put_refusal_message(error);
    assert!(message.contains("test-store"), "{message}");
    assert!(!message.contains("hunter2"), "{message}");
}

#[test]
fn the_default_put_value_refuses_instead_of_succeeding_silently() {
    let store = DefaultOnlyStore(descriptor_with(SecretStoreCapabilities::WRITE));
    let request = SecretPutRequest::text("prod/api/credentials", "hunter2");

    let error = store
        .put_value(&request)
        .expect_err("an unimplemented put must refuse");

    let message = put_refusal_message(error);
    assert!(message.contains("test-store"), "{message}");
}

#[test]
fn a_put_request_redacts_its_value_from_debug() {
    let request = SecretPutRequest::text("prod/api/credentials", "hunter2")
        .with_stage(SecretVersionStage::Pending);
    let binary = SecretPutRequest::binary("prod/tls/signing-key", b"hunter2".to_vec());

    for rendered in [format!("{request:?}"), format!("{binary:?}")] {
        assert!(!rendered.contains("hunter2"), "{rendered}");
    }
    // A binary payload must not leak as its decimal byte array either.
    let decimal_bytes = format!("{:?}", "hunter2".as_bytes());
    assert!(
        !format!("{binary:?}").contains(&decimal_bytes),
        "{decimal_bytes}"
    );
    assert!(format!("{request:?}").contains("prod/api/credentials"));
    assert_eq!(request.stage, Some(SecretVersionStage::Pending));
}

// --- Listing versions ---

#[test]
fn the_default_list_versions_refuses_naming_the_store_and_the_secret() {
    let store = DefaultOnlyStore(descriptor_with(SecretStoreCapabilities::VERSIONS));

    let error = store
        .list_versions("prod/api/credentials")
        .expect_err("an unimplemented history must refuse, not answer empty");

    let message = put_refusal_message(error);
    assert!(message.contains("test-store"), "{message}");
    assert!(message.contains("prod/api/credentials"), "{message}");
}
