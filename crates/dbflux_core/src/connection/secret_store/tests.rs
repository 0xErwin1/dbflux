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

// --- Create choices ---

#[test]
fn a_descriptor_declares_no_create_choices_unless_the_store_gives_them() {
    let descriptor = SecretStoreDescriptor::new(
        "plain",
        "Plain",
        SecretStoreCapabilities::WRITE | SecretStoreCapabilities::CREATE,
    );

    assert_eq!(descriptor.create_choices, None);
}

#[test]
fn create_choices_keep_the_order_the_store_declared_and_its_defaults() {
    let choices =
        SecretCreateChoices::new(["String", "StringList", "SecureString"], "SecureString")
            .with_encrypted_value_type("SecureString")
            .with_tiers(["Standard", "Advanced"], "Standard");
    let descriptor =
        descriptor_with(SecretStoreCapabilities::TYPED_VALUES).with_create_choices(choices.clone());

    assert_eq!(descriptor.create_choices.as_ref(), Some(&choices));
    assert_eq!(
        choices.value_types(),
        ["String", "StringList", "SecureString"]
    );
    assert_eq!(choices.default_value_type(), "SecureString");
    assert_eq!(choices.tiers(), ["Standard", "Advanced"]);
    assert_eq!(choices.default_tier(), Some("Standard"));
}

#[test]
fn only_the_encrypted_value_type_takes_an_encryption_key() {
    let choices =
        SecretCreateChoices::new(["Plain", "Sealed"], "Plain").with_encrypted_value_type("Sealed");

    assert!(choices.takes_encryption_key("Sealed"));
    assert!(!choices.takes_encryption_key("Plain"));
    assert!(!choices.takes_encryption_key("Unknown"));

    let none_declared = SecretCreateChoices::new(["Plain"], "Plain");
    assert!(!none_declared.takes_encryption_key("Plain"));

    let not_offered =
        SecretCreateChoices::new(["Plain"], "Plain").with_encrypted_value_type("Sealed");
    assert!(!not_offered.takes_encryption_key("Sealed"));
}

#[test]
fn a_default_the_store_does_not_offer_falls_back_to_its_first_choice() {
    let choices = SecretCreateChoices::new(["A", "B"], "Z").with_tiers(["Low", "High"], "Z");

    assert_eq!(choices.default_value_type(), "A");
    assert_eq!(choices.default_tier(), Some("Low"));
}

#[test]
fn a_store_without_tiers_declares_none() {
    let choices = SecretCreateChoices::new(["A"], "A");

    assert!(choices.tiers().is_empty());
    assert_eq!(choices.default_tier(), None);
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

// --- Rolling back ---

#[test]
fn a_descriptor_missing_one_required_capability_refuses_naming_the_operation() {
    let required = SecretStoreCapabilities::WRITE | SecretStoreCapabilities::VERSION_LABELS;

    let error = descriptor_with(SecretStoreCapabilities::WRITE)
        .ensure_can(required, "roll back", "prod/api/credentials")
        .expect_err("a store without version labels cannot roll back");

    let message = put_refusal_message(error);
    assert!(message.contains("test-store"), "{message}");
    assert!(message.contains("roll back"), "{message}");
    assert!(message.contains("prod/api/credentials"), "{message}");

    let error = descriptor_with(SecretStoreCapabilities::VERSION_LABELS)
        .ensure_can(required, "roll back", "prod/api/credentials")
        .expect_err("a store that cannot write cannot roll back");
    assert!(put_refusal_message(error).contains("roll back"));

    descriptor_with(required)
        .ensure_can(required, "roll back", "prod/api/credentials")
        .expect("a store with both capabilities rolls back");
}

#[test]
fn a_rollback_request_needs_a_version_to_roll_back_to() {
    SecretRollbackRequest::new("prod/api/credentials", "v-previous")
        .validate()
        .expect("a named version is a valid target");

    for blank in ["", "   "] {
        let error = SecretRollbackRequest::new("prod/api/credentials", blank)
            .validate()
            .expect_err("a blank version cannot be rolled back to");
        assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
    }
}

#[test]
fn the_default_roll_back_refuses_instead_of_succeeding_silently() {
    let store = DefaultOnlyStore(descriptor_with(
        SecretStoreCapabilities::WRITE | SecretStoreCapabilities::VERSION_LABELS,
    ));

    let error = store
        .roll_back(&SecretRollbackRequest::new(
            "prod/api/credentials",
            "v-previous",
        ))
        .expect_err("an unimplemented roll back must refuse");

    let message = put_refusal_message(error);
    assert!(message.contains("test-store"), "{message}");
    assert!(message.contains("prod/api/credentials"), "{message}");
}

// --- Deleting and restoring ---

#[test]
fn a_scheduled_deletion_accepts_a_recovery_window_of_seven_to_thirty_days() {
    for days in [7, 14, 30, DEFAULT_RECOVERY_WINDOW_DAYS] {
        SecretDeleteRequest::scheduled("prod/api/credentials", days)
            .validate()
            .unwrap_or_else(|error| panic!("{days} days: {error}"));
    }
    assert_eq!(DEFAULT_RECOVERY_WINDOW_DAYS, 30);

    for days in [0, 1, 6, 31, 365] {
        let error = SecretDeleteRequest::scheduled("prod/api/credentials", days)
            .validate()
            .expect_err("a window outside 7 to 30 days is refused");
        assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
        assert!(error.to_string().contains("7"), "{error}");
        assert!(error.to_string().contains("30"), "{error}");
    }
}

#[test]
fn an_irreversible_deletion_needs_the_exact_name_typed_back() {
    SecretDeleteRequest::irreversible("/app/prod/db/host", "/app/prod/db/host")
        .validate()
        .expect("the exact name confirms");

    for typed in [
        "",
        "/app/prod/db/hos",
        "/APP/PROD/DB/HOST",
        " /app/prod/db/host",
        "/app/prod/db/host ",
        "/app/prod/db/host/",
    ] {
        let request = SecretDeleteRequest::irreversible("/app/prod/db/host", typed);

        assert!(!request.confirmation_matches(), "{typed:?}");
        let error = request
            .validate()
            .expect_err("anything but the exact name is refused");
        assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
        assert!(error.to_string().contains("/app/prod/db/host"), "{error}");
    }
}

#[test]
fn a_scheduled_deletion_is_never_an_unconfirmed_irreversible_one() {
    let scheduled = SecretDeleteRequest::scheduled("prod/api/credentials", 30);

    assert!(scheduled.confirmation_matches(), "nothing to confirm");
    assert_eq!(scheduled.name(), "prod/api/credentials");
    assert!(!scheduled.is_irreversible());
    assert!(
        SecretDeleteRequest::irreversible("a", "a").is_irreversible(),
        "the type, not a flag, says it cannot be recovered"
    );
}

#[test]
fn the_defaults_refuse_a_delete_and_a_restore_instead_of_succeeding_silently() {
    let store = DefaultOnlyStore(descriptor_with(
        SecretStoreCapabilities::WRITE
            | SecretStoreCapabilities::DELETE
            | SecretStoreCapabilities::SOFT_DELETE,
    ));

    let error = store
        .delete_secret(&SecretDeleteRequest::scheduled("prod/api/credentials", 30))
        .expect_err("an unimplemented delete must refuse");
    let message = put_refusal_message(error);
    assert!(message.contains("test-store"), "{message}");
    assert!(message.contains("prod/api/credentials"), "{message}");

    let error = store
        .restore_secret("prod/api/credentials")
        .expect_err("an unimplemented restore must refuse");
    assert!(put_refusal_message(error).contains("prod/api/credentials"));
}

// --- Rotation ---

const LAMBDA: &str = "arn:aws:lambda:eu-west-1:441012345678:function:SecretsManagerRDSRotation";

#[test]
fn a_rotation_schedule_is_a_rate_or_a_cron_expression() {
    for valid in [
        "rate(1 day)",
        "rate(30 days)",
        "rate(4 hours)",
        "rate(12 hours)",
        "cron(0 16 1,15 * ? *)",
        "cron(0/15 8-17 ? * MON-FRI *)",
        "cron(0 12 L * ? *)",
    ] {
        validate_rotation_schedule(valid).unwrap_or_else(|error| panic!("{valid}: {error}"));
    }

    for invalid in [
        "",
        "30 days",
        "rate()",
        "rate(30)",
        "rate(0 days)",
        "rate(-1 days)",
        "rate(1 days)",
        "rate(2 day)",
        "rate(3 hours)",
        "rate(30 weeks)",
        "rate(30 days",
        "rate(30  days extra)",
        "rate(1.5 days)",
        " rate(30 days)",
        "rate(30 days) ",
        "cron(0 16 1,15 * ?)",
        "cron(0 16 1,15 * ? * *)",
        "cron(0 16 1,15 * ? $)",
        "cron()",
        "RATE(30 days)",
    ] {
        let error = validate_rotation_schedule(invalid)
            .expect_err("a schedule the service would reject is refused locally");
        assert!(error.contains("rate("), "{invalid:?}: {error}");
    }
}

#[test]
fn a_rotation_window_is_whole_hours_up_to_a_day() {
    for valid in ["1h", "4h", "24h"] {
        validate_rotation_window(valid).unwrap_or_else(|error| panic!("{valid}: {error}"));
    }

    for invalid in ["", "0h", "25h", "4", "4H", "4 h", "90m", "-1h", "1.5h", "h"] {
        validate_rotation_window(invalid).expect_err(invalid);
    }
}

#[test]
fn a_rotation_function_is_a_lambda_function_arn() {
    for valid in [
        LAMBDA,
        "arn:aws:lambda:eu-west-1:441012345678:function:rotate:live",
        "arn:aws-us-gov:lambda:us-gov-west-1:441012345678:function:rotate",
    ] {
        validate_rotation_function(valid).unwrap_or_else(|error| panic!("{valid}: {error}"));
    }

    for invalid in [
        "",
        "rotate-api-credentials",
        "arn:aws:s3:::bucket",
        "arn:aws:lambda:eu-west-1:441012345678:layer:rotate:1",
        "arn:aws:lambda:eu-west-1:441012345678:function:",
        "arn:aws:lambda::441012345678:function:rotate",
        "arn:aws:lambda:eu-west-1:44101:function:rotate",
        "arn:aws:sns:eu-west-1:441012345678:function:rotate",
    ] {
        validate_rotation_function(invalid).expect_err(invalid);
    }
}

#[test]
fn enabling_rotation_needs_a_valid_schedule_and_checks_every_field_it_carries() {
    SecretRotationUpdate::enable("prod/api", "rate(30 days)")
        .with_window("4h")
        .with_function(LAMBDA)
        .validate()
        .expect("a complete update");

    SecretRotationUpdate::enable("prod/api", "cron(0 16 1 * ? *)")
        .validate()
        .expect("the window and the function are optional");

    for update in [
        SecretRotationUpdate::enable("prod/api", "every month"),
        SecretRotationUpdate::enable("prod/api", "rate(30 days)").with_window("90m"),
        SecretRotationUpdate::enable("prod/api", "rate(30 days)").with_function("rotate-fn"),
        SecretRotationUpdate {
            schedule: None,
            ..SecretRotationUpdate::enable("prod/api", "rate(30 days)")
        },
    ] {
        let error = update.validate().expect_err("an invalid update is refused");
        assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
        assert!(error.to_string().contains("prod/api"), "{error}");
    }
}

#[test]
fn disabling_rotation_carries_no_schedule() {
    SecretRotationUpdate::disable("prod/api")
        .validate()
        .expect("a bare disable");

    for update in [
        SecretRotationUpdate {
            schedule: Some("rate(30 days)".to_string()),
            ..SecretRotationUpdate::disable("prod/api")
        },
        SecretRotationUpdate::disable("prod/api").with_window("4h"),
        SecretRotationUpdate::disable("prod/api").with_function(LAMBDA),
    ] {
        let error = update
            .validate()
            .expect_err("a disable with settings is refused");
        assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
    }
}

#[test]
fn the_defaults_refuse_a_rotation_instead_of_succeeding_silently() {
    let store = DefaultOnlyStore(descriptor_with(
        SecretStoreCapabilities::WRITE | SecretStoreCapabilities::ROTATION,
    ));

    let error = store
        .rotate_now("prod/api/credentials")
        .expect_err("an unimplemented rotation must refuse");
    let message = put_refusal_message(error);
    assert!(message.contains("test-store"), "{message}");
    assert!(message.contains("prod/api/credentials"), "{message}");

    let error = store
        .update_rotation(&SecretRotationUpdate::disable("prod/api/credentials"))
        .expect_err("an unimplemented edit must refuse");
    assert!(put_refusal_message(error).contains("prod/api/credentials"));
}

// --- Tags and description ---

#[test]
fn a_tag_key_is_one_to_128_characters_without_the_reserved_prefix() {
    for key in [
        "env",
        "cost-center",
        "team name",
        "a:b/c=d+e@f.g_h",
        "über",
        &"k".repeat(128),
    ] {
        validate_tag(key, "value").unwrap_or_else(|error| panic!("{key}: {error}"));
    }

    for key in [
        "",
        &"k".repeat(129),
        "aws:created",
        "AWS:created",
        "Aws:anything",
        "bad#key",
        "bad,key",
        "tab\tkey",
    ] {
        let error = validate_tag(key, "value").expect_err("an invalid key is refused");
        assert!(error.contains("key"), "{key:?}: {error}");
    }

    // `aws` without the colon is an ordinary key.
    validate_tag("aws", "value").expect("only the aws: prefix is reserved");
    validate_tag("awsome", "value").expect("only the aws: prefix is reserved");
}

#[test]
fn a_tag_value_is_zero_to_256_characters() {
    for value in ["", "prod", "a value with spaces", &"v".repeat(256)] {
        validate_tag("env", value).unwrap_or_else(|error| panic!("{value}: {error}"));
    }

    for value in [&"v".repeat(257), "bad#value", "line\nbreak"] {
        let error = validate_tag("env", value).expect_err("an invalid value is refused");
        assert!(error.contains("value"), "{error}");
    }
}

#[test]
fn a_tag_update_sets_and_removes_keys_and_changes_something() {
    SecretTagsUpdate::new("prod/api")
        .set("env", "prod")
        .set("owner", "")
        .remove("legacy")
        .validate()
        .expect("a valid update");

    SecretTagsUpdate::new("prod/api")
        .remove("legacy")
        .validate()
        .expect("removing alone is an update");

    for update in [
        SecretTagsUpdate::new("prod/api"),
        SecretTagsUpdate::new("prod/api")
            .set("env", "a")
            .set("env", "b"),
        SecretTagsUpdate::new("prod/api")
            .set("env", "a")
            .remove("env"),
        SecretTagsUpdate::new("prod/api").set("aws:env", "a"),
        SecretTagsUpdate::new("prod/api").set("", "a"),
        SecretTagsUpdate::new("prod/api").remove(""),
        SecretTagsUpdate::new("prod/api").remove("aws:env"),
    ] {
        let error = update.validate().expect_err("an invalid update is refused");
        assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
        assert!(error.to_string().contains("prod/api"), "{error}");
    }
}

#[test]
fn a_tag_update_cannot_set_more_than_fifty_tags() {
    let mut update = SecretTagsUpdate::new("prod/api");
    for index in 0..50 {
        update = update.set(format!("key-{index}"), "value");
    }
    update.validate().expect("fifty tags fit");

    let error = update
        .set("one-too-many", "value")
        .validate()
        .expect_err("fifty-one tags do not");
    assert!(error.to_string().contains("50"), "{error}");
}

#[test]
fn a_tag_update_debug_names_the_keys_for_diagnosis() {
    let rendered = format!("{:?}", SecretTagsUpdate::new("prod/api").set("env", "prod"));

    assert!(rendered.contains("prod/api"), "{rendered}");
    assert!(rendered.contains("env"), "{rendered}");
}

#[test]
fn the_defaults_refuse_tags_and_description_instead_of_succeeding_silently() {
    let store = DefaultOnlyStore(descriptor_with(
        SecretStoreCapabilities::WRITE
            | SecretStoreCapabilities::TAGS
            | SecretStoreCapabilities::DESCRIPTION,
    ));

    let error = store
        .update_tags(&SecretTagsUpdate::new("prod/api/credentials").set("env", "prod"))
        .expect_err("an unimplemented tag edit must refuse");
    let message = put_refusal_message(error);
    assert!(message.contains("test-store"), "{message}");
    assert!(message.contains("prod/api/credentials"), "{message}");

    let error = store
        .update_description(&SecretDescriptionUpdate::new(
            "prod/api/credentials",
            "text",
        ))
        .expect_err("an unimplemented description edit must refuse");
    assert!(put_refusal_message(error).contains("prod/api/credentials"));
}

// --- Creating ---

#[test]
fn a_create_request_needs_a_name_a_text_value_and_valid_tags() {
    SecretCreateRequest::text("prod/api/credentials", "create-value")
        .validate()
        .expect("a bare create");

    SecretCreateRequest::text("/app/prod/db/host", "create-value")
        .with_description("Primary host")
        .with_value_type("SecureString")
        .with_tier("Advanced")
        .with_encryption_key("alias/app-prod")
        .with_tag("env", "prod")
        .with_tag("owner", "")
        .validate()
        .expect("a complete create");

    for request in [
        SecretCreateRequest::text("", "create-value"),
        SecretCreateRequest::text("   ", "create-value"),
        SecretCreateRequest::text("prod/api", ""),
        SecretCreateRequest::binary("prod/api", vec![1, 2, 3]),
        SecretCreateRequest::text("prod/api", "v").with_tag("aws:env", "x"),
        SecretCreateRequest::text("prod/api", "v")
            .with_tag("env", "a")
            .with_tag("env", "b"),
        SecretCreateRequest::text("prod/api", "v").with_tag("", "x"),
    ] {
        let error = request
            .validate()
            .expect_err("an invalid request is refused");
        assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
    }
}

#[test]
fn a_create_request_cannot_carry_more_than_fifty_tags() {
    let mut request = SecretCreateRequest::text("prod/api", "create-value");
    for index in 0..50 {
        request = request.with_tag(format!("key-{index}"), "v");
    }
    request.validate().expect("fifty fit");

    let error = request
        .with_tag("one-too-many", "v")
        .validate()
        .expect_err("fifty-one do not");
    assert!(error.to_string().contains("50"), "{error}");
}

#[test]
fn a_create_request_never_renders_its_value() {
    let request = SecretCreateRequest::text("prod/api/credentials", "create-value-probe")
        .with_description("Payments")
        .with_tag("env", "prod");
    let binary = SecretCreateRequest::binary("prod/tls/key", b"create-value-probe".to_vec());

    for rendered in [format!("{request:?}"), format!("{binary:?}")] {
        assert!(!rendered.contains("create-value-probe"), "{rendered}");
    }
    assert!(format!("{request:?}").contains("prod/api/credentials"));
}

#[test]
fn the_default_create_secret_refuses_instead_of_succeeding_silently() {
    let store = DefaultOnlyStore(descriptor_with(
        SecretStoreCapabilities::WRITE | SecretStoreCapabilities::CREATE,
    ));

    let error = store
        .create_secret(&SecretCreateRequest::text("prod/api/new", "create-value"))
        .expect_err("an unimplemented create must refuse");

    let message = put_refusal_message(error);
    assert!(message.contains("test-store"), "{message}");
    assert!(message.contains("prod/api/new"), "{message}");
    assert!(!message.contains("create-value"), "{message}");
}

// --- Blank names ---

#[test]
fn every_request_type_refuses_a_blank_or_whitespace_name() {
    for name in ["", "   ", "\t\n"] {
        let refusals = [
            SecretRollbackRequest::new(name, "v-1").validate(),
            SecretDeleteRequest::scheduled(name, 30).validate(),
            SecretDeleteRequest::irreversible(name, name).validate(),
            SecretRotationUpdate::disable(name).validate(),
            SecretRotationUpdate::enable(name, "rate(30 days)").validate(),
            SecretTagsUpdate::new(name).set("env", "prod").validate(),
            SecretDescriptionUpdate::new(name, "text").validate(),
            SecretCreateRequest::text(name, "value").validate(),
            require_name(name),
        ];

        for refusal in refusals {
            let error = refusal.expect_err("a blank name is refused");
            assert!(
                matches!(error, DbError::ConstraintViolation(_)),
                "{name:?}: {error}"
            );
        }
    }

    require_name("prod/api").expect("a real name passes");
}

// --- Adversarial inputs ---

mod adversarial_inputs {
    use crate::{
        SecretCreateRequest, SecretDeleteRequest, SecretRotationUpdate, SecretTagsUpdate,
        validate_rotation_schedule, validate_tag,
    };

    #[test]
    fn debug_of_a_create_never_renders_the_value() {
        let request = SecretCreateRequest::text("n", "vp-secret-\"q\"\nline").with_tag("k", "v");
        assert!(!format!("{request:?} {request:#?}").contains("vp-secret"));
    }

    #[test]
    fn irreversible_confirmation_refuses_every_near_miss() {
        for typed in ["", " /a", "/a ", "/A", "/a\u{200b}", "\u{ff0f}a"] {
            assert!(
                SecretDeleteRequest::irreversible("/a", typed)
                    .validate()
                    .is_err(),
                "{typed:?}"
            );
        }
    }

    #[test]
    fn recovery_window_bounds() {
        for days in [0u32, 6, 31, u32::MAX] {
            assert!(
                SecretDeleteRequest::scheduled("n", days)
                    .validate()
                    .is_err()
            );
        }
        for days in [7u32, 30] {
            assert!(SecretDeleteRequest::scheduled("n", days).validate().is_ok());
        }
    }

    #[test]
    fn schedules_tags_and_rotation_limits() {
        for bad in [
            "rate(0 days)",
            "rate(3 hours)",
            "rate(1 days)",
            "rate(2 day)",
            "cron(0 0 * * ?)",
            "",
        ] {
            assert!(validate_rotation_schedule(bad).is_err(), "{bad}");
        }
        assert!(validate_tag("AWS:x", "v").is_err());
        assert!(
            SecretTagsUpdate::new("n")
                .remove("aws:x")
                .validate()
                .is_err()
        );
        assert!(
            SecretRotationUpdate::enable("n", "rate(1 day)")
                .with_window("25h")
                .validate()
                .is_err()
        );
        assert!(
            SecretRotationUpdate::enable("n", "rate(1 day)")
                .with_function("arn:aws:lambda:us-east-1:123:function:f")
                .validate()
                .is_err()
        );
    }
}

// --- Scrubbing a submitted value from an error ---

fn scrubbed_message(message: &str, values: &[&str]) -> String {
    match scrub_echoed_values(DbError::query_failed(message), values) {
        DbError::QueryFailed(formatted) => formatted.message,
        other => panic!("expected QueryFailed, got {other:?}"),
    }
}

#[test]
fn a_value_is_scrubbed_in_every_json_nesting_depth() {
    let value = "pa\"ss\\word\nline";
    let escape = |text: &str| {
        text.replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
    };
    let mut echoed = value.to_string();

    for _ in 0..=4 {
        let message = format!("body {{\"Value\":\"{echoed}\"}} rejected");
        assert_eq!(
            scrubbed_message(&message, &[value]),
            "body {\"Value\":\"[redacted]\"} rejected"
        );
        echoed = escape(&echoed);
    }
}

#[test]
fn short_and_unicode_escapes_of_one_value_are_scrubbed_together() {
    let value = "a\u{8}b\u{c}c\u{1f600}";

    for echoed in [
        "a\\bb\\fc\\ud83d\\ude00",
        "a\\u0008b\\u000cc\\ud83d\\ude00",
        "a\\u0008b\\fc\\uD83D\\uDE00",
    ] {
        assert_eq!(
            scrubbed_message(&format!("echo {echoed} end"), &[value]),
            "echo [redacted] end",
            "{echoed}"
        );
    }
}

#[test]
fn overlapping_occurrences_are_removed_as_one_region() {
    assert_eq!(
        scrubbed_message("got abcdefg end", &["abcd", "cdefg"]),
        "got [redacted] end"
    );
    assert_eq!(
        scrubbed_message("got abcdefg end", &["cdefg", "abcd"]),
        "got [redacted] end"
    );
}

#[test]
fn an_empty_value_and_text_without_the_value_change_nothing() {
    assert_eq!(scrubbed_message("plain message", &[""]), "plain message");
    assert_eq!(
        scrubbed_message("plain message", &["absent"]),
        "plain message"
    );
    assert_eq!(
        scrubbed_message("keeps a \\q and a trailing \\", &["absent"]),
        "keeps a \\q and a trailing \\"
    );
}

#[test]
fn a_binary_payload_is_scrubbed_as_text_and_as_base64() {
    let payload = SecretPayload::Binary(SecretSlice::from(b"topsecret".to_vec()));
    let error = DbError::query_failed(format!(
        "saw topsecret and {} end",
        encode_base64(b"topsecret")
    ));

    let rendered = scrub_submitted_payload(error, &payload).to_string();

    assert_eq!(rendered, "saw [redacted] and [redacted] end");
}

#[test]
fn a_tag_accepts_unicode_separators_but_not_control_characters() {
    validate_tag("cost\u{a0}center", "a\u{2003}b").expect("separators are allowed");
    validate_tag("kostenstelle-ü", "値").expect("letters are allowed");

    for bad in ["tab\there", "line\nbreak", "next\u{85}line"] {
        validate_tag(bad, "value").expect_err("a control character is refused");
        validate_tag("env", bad).expect_err("a control character is refused");
    }
}

#[test]
fn a_rotation_window_must_fit_inside_an_hourly_interval() {
    validate_rotation_window_fits("rate(4 hours)", "4h").expect("equal fits");
    validate_rotation_window_fits("rate(12 hours)", "3h").expect("shorter fits");
    validate_rotation_window_fits("rate(1 day)", "24h").expect("a day holds any window");
    validate_rotation_window_fits("rate(30 days)", "24h").expect("a day holds any window");
    validate_rotation_window_fits("cron(0 8 1 * ? *)", "24h").expect("no interval to compare");

    let error = validate_rotation_window_fits("rate(4 hours)", "24h").expect_err("too long");
    assert!(error.contains("4h"), "{error}");
    assert!(error.contains("rate(4 hours)"), "{error}");

    let update = SecretRotationUpdate::enable("prod/api", "rate(4 hours)").with_window("24h");
    assert!(matches!(
        update
            .validate()
            .expect_err("an incompatible pair is refused"),
        DbError::ConstraintViolation(_)
    ));
}

fn names(keys: &[&str]) -> Vec<String> {
    keys.iter().map(ToString::to_string).collect()
}

#[test]
fn a_tag_replacement_at_the_quota_removes_before_it_adds() {
    let full: Vec<String> = (0..MAX_TAGS).map(|index| format!("name-{index}")).collect();
    let swap = SecretTagsUpdate::new("prod/api")
        .set("new-name", "v")
        .remove("name-0");

    assert_eq!(swap.tags_after(&full), MAX_TAGS);
    assert!(swap.removes_first(&full));

    let overwrite = SecretTagsUpdate::new("prod/api")
        .set("name-1", "v")
        .remove("name-0");
    assert!(!overwrite.removes_first(&full), "an overwrite adds no key");

    let add_only = SecretTagsUpdate::new("prod/api").set("new-name", "v");
    assert_eq!(add_only.tags_after(&full), MAX_TAGS + 1);
    assert!(!add_only.removes_first(&full), "nothing to remove first");

    let roomy = names(&["a", "b"]);
    assert!(!swap.removes_first(&roomy));
    assert_eq!(swap.tags_after(&roomy), 3);
}

#[test]
fn a_tag_update_that_ends_past_the_quota_is_refused_before_any_call() {
    let full: Vec<String> = (0..MAX_TAGS).map(|index| format!("name-{index}")).collect();

    SecretTagsUpdate::new("prod/api")
        .set("new-name", "v")
        .remove("name-0")
        .check_quota(&full)
        .expect("a replacement ends at the quota");

    let error = SecretTagsUpdate::new("prod/api")
        .set("new-name", "v")
        .check_quota(&full)
        .expect_err("one more than the quota");
    assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
    assert!(error.to_string().contains("51"), "{error}");
}
