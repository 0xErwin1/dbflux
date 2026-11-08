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
fn a_one_day_interval_is_reported_in_the_singular_a_schedule_edit_accepts() {
    let one_day = RotationRulesType::builder()
        .automatically_after_days(1)
        .build();

    let schedule = rotation_schedule(&one_day).expect("a schedule");

    assert_eq!(schedule, "rate(1 day)");
    dbflux_core::validate_rotation_schedule(&schedule).expect("the reported schedule is reusable");
}

#[test]
fn a_cancelled_rotation_reports_the_version_it_left_pending() {
    let stages = |entries: &[(&str, &[&str])]| -> HashMap<String, Vec<String>> {
        entries
            .iter()
            .map(|(version, labels)| {
                (
                    version.to_string(),
                    labels.iter().map(ToString::to_string).collect(),
                )
            })
            .collect()
    };

    assert_eq!(
        unfinished_rotation_version(Some(&stages(&[
            ("v-pending", &["AWSPENDING"]),
            ("v-current", &["AWSCURRENT"]),
        ])))
        .as_deref(),
        Some("v-pending")
    );
    assert_eq!(
        unfinished_rotation_version(Some(&stages(&[
            ("v-done", &["AWSPENDING", "AWSCURRENT"]),
            ("v-old", &["AWSPREVIOUS"]),
        ]))),
        None,
        "a pending label on the current version is a finished rotation"
    );
    assert_eq!(unfinished_rotation_version(None), None);
}

#[test]
fn a_preserved_window_is_checked_against_the_new_interval() {
    let update = SecretRotationUpdate::enable("prod/api", "rate(4 hours)");

    let error = plan_rotation_update(&update, true, Some(LAMBDA), Some("24h"))
        .expect_err("the secret's own 24h window no longer fits");
    assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
    assert!(error.to_string().contains("4h"), "{error}");

    plan_rotation_update(
        &update.clone().with_window("4h"),
        true,
        Some(LAMBDA),
        Some("24h"),
    )
    .expect("naming a compatible window resolves it");
    plan_rotation_update(&update, true, Some(LAMBDA), Some("3h")).expect("a shorter window fits");
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

/// A secret has neither a type nor a tier, so the store declares no choices
/// for a create dialog to render.
#[test]
fn the_secrets_manager_declares_no_create_choices() {
    assert_eq!(offline_store().descriptor().create_choices, None);
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
fn a_version_holding_the_pending_stage_is_marked_pending() {
    let versions = versions_from_entries(&[
        version_entry("v-pending", &["AWSPENDING"], Some(3_000)),
        version_entry("v-current", &["AWSCURRENT"], Some(2_000)),
        version_entry("v-old", &[], Some(1_000)),
    ]);

    let pending: Vec<(&str, bool)> = versions
        .iter()
        .map(|version| (version.id.as_str(), version.is_pending))
        .collect();
    assert_eq!(
        pending,
        [("v-pending", true), ("v-current", false), ("v-old", false)]
    );
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

#[test]
fn version_paging_forwards_the_token_and_keeps_the_later_pages() {
    let tokens = std::cell::RefCell::new(Vec::new());

    let versions = list_versions_on_pages(|token| {
        tokens.borrow_mut().push(token.clone());
        Ok(match token.as_deref() {
            None => (
                vec![version_entry("v-new", &["AWSCURRENT"], Some(3_000))],
                Some("page-2".to_string()),
            ),
            Some(_) => (vec![version_entry("v-old", &[], Some(1_000))], None),
        })
    })
    .expect("both pages are read");

    assert_eq!(*tokens.borrow(), [None, Some("page-2".to_string())]);
    let ids: Vec<&str> = versions.iter().map(|version| version.id.as_str()).collect();
    assert_eq!(ids, ["v-new", "v-old"]);
}

fn stages(entries: &[(&str, &[&str])]) -> HashMap<String, Vec<String>> {
    entries
        .iter()
        .map(|(version, labels)| {
            (
                version.to_string(),
                labels.iter().map(ToString::to_string).collect(),
            )
        })
        .collect()
}

#[test]
fn a_roll_back_moves_current_from_the_version_that_holds_it() {
    let versions = stages(&[
        ("v-current", &["AWSCURRENT"]),
        ("v-previous", &["AWSPREVIOUS"]),
        ("v-oldest", &[]),
    ]);

    let plan = plan_rollback(&versions, "prod/api", "v-oldest").expect("an earlier version");

    assert_eq!(plan.from_version, "v-current");
    assert_eq!(plan.to_version, "v-oldest");
}

#[test]
fn rolling_back_to_the_previous_version_is_a_roll_back_too() {
    let versions = stages(&[
        ("v-current", &["AWSCURRENT"]),
        ("v-previous", &["AWSPREVIOUS"]),
    ]);

    let plan = plan_rollback(&versions, "prod/api", "v-previous").expect("previous");

    assert_eq!(plan.from_version, "v-current");
}

#[test]
fn a_roll_back_to_the_current_version_is_refused() {
    let versions = stages(&[("v-current", &["AWSCURRENT"]), ("v-previous", &[])]);

    let error = plan_rollback(&versions, "prod/api", "v-current")
        .expect_err("the current version is not an earlier one");

    assert!(error.to_string().contains("already the current"), "{error}");
    assert!(error.to_string().contains("prod/api"), "{error}");
}

#[test]
fn a_roll_back_to_an_unknown_version_or_without_a_current_one_is_refused() {
    let versions = stages(&[("v-current", &["AWSCURRENT"])]);
    let error = plan_rollback(&versions, "prod/api", "v-missing").expect_err("unknown");
    assert!(matches!(error, DbError::ObjectNotFound(_)), "{error}");

    let versions = stages(&[("v-a", &[]), ("v-b", &["pre-migration"])]);
    let error = plan_rollback(&versions, "prod/api", "v-a").expect_err("no current");
    assert!(error.to_string().contains("no current version"), "{error}");

    let error = plan_rollback(&HashMap::new(), "prod/api", "v-a").expect_err("no versions");
    assert!(matches!(error, DbError::ObjectNotFound(_)), "{error}");
}

#[test]
fn stages_cover_deprecated_versions_with_an_empty_label_list() {
    let entries = [
        SecretVersionsListEntry::builder()
            .version_id("v-current")
            .version_stages("AWSCURRENT")
            .build(),
        SecretVersionsListEntry::builder()
            .version_id("v-deprecated")
            .build(),
        SecretVersionsListEntry::builder().build(),
    ];

    let stages = stages_by_version(&entries);

    assert_eq!(stages.len(), 2, "an entry without an id is dropped");
    assert_eq!(stages["v-current"], ["AWSCURRENT"]);
    assert!(stages["v-deprecated"].is_empty());

    let plan = plan_rollback(&stages, "prod/api", "v-deprecated").expect("deprecated target");
    assert_eq!(plan.from_version, "v-current");
}

#[test]
fn a_scheduled_deletion_sends_its_window_and_nothing_else_is_ever_sent() {
    let days = deletion_window(&SecretDeleteRequest::scheduled("prod/api", 14)).expect("14 days");
    assert_eq!(days, 14);

    for out_of_range in [0, 6, 31] {
        let error = deletion_window(&SecretDeleteRequest::scheduled("prod/api", out_of_range))
            .expect_err("a window outside 7 to 30 days is refused before the call");
        assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
    }
}

#[test]
fn secrets_manager_never_deletes_without_recovery() {
    // Even with the exact name typed back: the service's force-delete is
    // not offered at all, so there is nothing to confirm.
    let error = deletion_window(&SecretDeleteRequest::irreversible("prod/api", "prod/api"))
        .expect_err("an irreversible deletion is refused");

    assert!(matches!(error, DbError::NotSupported(_)), "{error}");
    assert!(error.to_string().contains("recovery window"), "{error}");
    assert!(error.to_string().contains("prod/api"), "{error}");
}

#[test]
fn the_deletion_date_comes_from_the_service_answer() {
    let when = SmithyDateTime::from_secs(1_790_000_000);

    let deletion = deletion_from_date(Some(&when));
    assert_eq!(
        deletion.deletion_scheduled_for.map(|date| date.timestamp()),
        Some(1_790_000_000)
    );

    assert!(deletion_from_date(None).deletion_scheduled_for.is_none());
}

#[test]
fn only_a_secret_that_is_not_scheduled_can_be_scheduled_and_only_a_scheduled_one_restored() {
    let when = SmithyDateTime::from_secs(1_790_000_000);

    ensure_not_scheduled(None, "prod/api").expect("a live secret can be scheduled");
    let error = ensure_not_scheduled(Some(&when), "prod/api").expect_err("already scheduled");
    assert!(error.to_string().contains("already scheduled"), "{error}");
    assert!(error.to_string().contains("prod/api"), "{error}");

    ensure_scheduled(Some(&when), "prod/api").expect("a scheduled secret can be restored");
    let error = ensure_scheduled(None, "prod/api").expect_err("nothing to restore");
    assert!(error.to_string().contains("not scheduled"), "{error}");
    assert!(error.to_string().contains("prod/api"), "{error}");
}

const LAMBDA: &str = "arn:aws:lambda:eu-west-1:441012345678:function:rotate";

#[test]
fn enabling_rotation_keeps_the_function_the_secret_has_and_needs_one_otherwise() {
    let update = SecretRotationUpdate::enable("prod/api", "rate(30 days)").with_window("4h");

    let plan = plan_rotation_update(&update, false, Some(LAMBDA), None).expect("a function exists");
    assert_eq!(
        plan,
        RotationPlan::Enable {
            schedule: "rate(30 days)".to_string(),
            window: Some("4h".to_string()),
            function: None,
        }
    );

    let error = plan_rotation_update(&update, false, None, None).expect_err("no function anywhere");
    assert!(error.to_string().contains("rotation function"), "{error}");

    let plan = plan_rotation_update(&update.with_function(LAMBDA), false, None, None)
        .expect("the update names one");
    assert!(matches!(
        plan,
        RotationPlan::Enable { function: Some(function), .. } if function == LAMBDA
    ));
}

#[test]
fn an_invalid_rotation_update_is_refused_before_a_plan_exists() {
    let update = SecretRotationUpdate::enable("prod/api", "every month");

    let error =
        plan_rotation_update(&update, true, Some(LAMBDA), None).expect_err("invalid schedule");

    assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
}

#[test]
fn rotation_is_turned_off_only_when_it_is_on() {
    let update = SecretRotationUpdate::disable("prod/api");

    assert_eq!(
        plan_rotation_update(&update, true, Some(LAMBDA), None).expect("on"),
        RotationPlan::Disable
    );

    let error = plan_rotation_update(&update, false, Some(LAMBDA), None).expect_err("already off");
    assert!(error.to_string().contains("not enabled"), "{error}");
}

#[test]
fn rotating_now_needs_rotation_enabled_with_a_function() {
    ensure_rotatable(Some(true), Some(LAMBDA), "prod/api").expect("enabled");

    for (enabled, function) in [(Some(false), Some(LAMBDA)), (None, None)] {
        let error = ensure_rotatable(enabled, function, "prod/api")
            .expect_err("rotation is not enabled; rotating would switch it on");
        assert!(error.to_string().contains("not enabled"), "{error}");
        assert!(error.to_string().contains("prod/api"), "{error}");
    }

    let error = ensure_rotatable(Some(true), None, "prod/api").expect_err("no function");
    assert!(error.to_string().contains("rotation function"), "{error}");
}

#[test]
fn tags_become_secrets_manager_tags_keeping_empty_values() {
    let built = sdk_tags(&[SecretTag::new("env", "prod"), SecretTag::new("owner", "")]);

    assert_eq!(built.len(), 2);
    assert_eq!(built[0].key(), Some("env"));
    assert_eq!(built[0].value(), Some("prod"));
    assert_eq!(built[1].key(), Some("owner"));
    assert_eq!(built[1].value(), Some(""));
}

#[test]
fn a_description_longer_than_two_thousand_forty_eight_characters_is_refused() {
    ensure_description_fits(&"d".repeat(2048), "prod/api").expect("2048 fit");
    ensure_description_fits("", "prod/api").expect("clearing is a description");

    let error = ensure_description_fits(&"d".repeat(2049), "prod/api").expect_err("2049 do not");

    assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
    assert!(error.to_string().contains("prod/api"), "{error}");
}

#[test]
fn a_secret_name_is_one_to_512_allowed_characters() {
    for valid in [
        "a",
        "prod/api/credentials",
        "a_b+c=d.e@f-g",
        &"n".repeat(512),
    ] {
        validate_secret_name(valid).unwrap_or_else(|error| panic!("{valid}: {error}"));
    }

    for invalid in [
        "",
        "has space",
        "über",
        "semi;colon",
        "star*",
        &"n".repeat(513),
    ] {
        let error = validate_secret_name(invalid).expect_err("an invalid name is refused");
        assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
    }
}

#[test]
fn a_secrets_manager_create_takes_no_type_or_tier_and_a_bounded_description() {
    let plain = SecretCreateRequest::text("prod/api", "create-value-probe")
        .with_description("Payments")
        .with_encryption_key("alias/payments")
        .with_tag("env", "prod");
    validate_create(&plain).expect("a valid create");

    for request in [
        SecretCreateRequest::text("prod/api", "create-value-probe").with_value_type("String"),
        SecretCreateRequest::text("prod/api", "create-value-probe").with_tier("Standard"),
    ] {
        let error = validate_create(&request).expect_err("no type or tier");
        assert!(matches!(error, DbError::NotSupported(_)), "{error}");
        assert!(!error.to_string().contains("create-value-probe"), "{error}");
    }

    let long = SecretCreateRequest::text("prod/api", "create-value-probe")
        .with_description("d".repeat(2049));
    assert!(matches!(
        validate_create(&long),
        Err(DbError::ConstraintViolation(_))
    ));

    let nameless = SecretCreateRequest::text("bad name", "create-value-probe");
    let error = validate_create(&nameless).expect_err("a bad name");
    assert!(!error.to_string().contains("create-value-probe"), "{error}");
}

#[test]
fn editing_only_the_schedule_keeps_the_existing_window() {
    let update = SecretRotationUpdate::enable("prod/api", "rate(7 days)");

    let plan = plan_rotation_update(&update, true, Some(LAMBDA), Some("4h")).expect("planned");

    assert_eq!(
        plan,
        RotationPlan::Enable {
            schedule: "rate(7 days)".to_string(),
            window: Some("4h".to_string()),
            function: None,
        }
    );
}

#[test]
fn a_named_window_wins_and_no_window_anywhere_sends_none() {
    let named = SecretRotationUpdate::enable("prod/api", "rate(7 days)").with_window("2h");
    let plan = plan_rotation_update(&named, true, Some(LAMBDA), Some("4h")).expect("planned");
    assert!(matches!(plan, RotationPlan::Enable { window: Some(window), .. } if window == "2h"));

    let bare = SecretRotationUpdate::enable("prod/api", "rate(7 days)");
    let plan = plan_rotation_update(&bare, true, Some(LAMBDA), None).expect("planned");
    assert!(matches!(plan, RotationPlan::Enable { window: None, .. }));
}

#[test]
fn the_rotation_rules_carry_the_schedule_and_the_window() {
    let rules = rotation_rules("rate(7 days)".to_string(), Some("4h".to_string()));

    assert_eq!(rules.schedule_expression(), Some("rate(7 days)"));
    assert_eq!(rules.duration(), Some("4h"));
}

fn offline_client() -> aws_sdk_secretsmanager::Client {
    let config = aws_sdk_secretsmanager::config::Builder::new()
        .behavior_version(aws_sdk_secretsmanager::config::BehaviorVersion::latest())
        .region(aws_sdk_secretsmanager::config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_secretsmanager::config::Credentials::new(
            "test", "test", None, None, "unit",
        ))
        .endpoint_url("http://127.0.0.1:1")
        .build();

    aws_sdk_secretsmanager::Client::from_conf(config)
}

#[test]
fn the_delete_call_carries_the_window_and_never_forces_the_deletion() {
    let client = offline_client();

    for days in [7, 14, 30] {
        let request = SecretDeleteRequest::scheduled("prod/api", days);
        let window = deletion_window(&request).expect("valid window");
        let call = delete_secret_call(&client, request.name(), window);
        let input = call.as_input();

        assert_eq!(input.get_secret_id().as_deref(), Some("prod/api"));
        assert_eq!(*input.get_recovery_window_in_days(), Some(i64::from(days)));
        assert_eq!(
            *input.get_force_delete_without_recovery(),
            None,
            "force delete is never sent"
        );
    }
}

#[test]
fn rolling_back_onto_a_pending_rotation_version_is_refused() {
    let versions = stages(&[
        ("v-current", &["AWSCURRENT"]),
        ("v-pending", &["AWSPENDING"]),
        ("v-old", &[]),
    ]);

    let error = plan_rollback(&versions, "prod/api", "v-pending")
        .expect_err("it would promote an unfinished rotation");

    assert!(error.to_string().contains("AWSPENDING"), "{error}");
    assert!(error.to_string().contains("rotation"), "{error}");
    plan_rollback(&versions, "prod/api", "v-old").expect("an ordinary version still rolls back");
}

#[test]
fn the_version_holding_the_current_stage_is_marked_current() {
    let versions = versions_from_entries(&[
        version_entry("v-pending", &["AWSPENDING"], Some(4_000)),
        version_entry("v-current", &["AWSCURRENT"], Some(3_000)),
        version_entry("v-old", &[], Some(1_000)),
    ]);

    let current: Vec<&str> = versions
        .iter()
        .filter(|version| version.is_current)
        .map(|version| version.id.as_str())
        .collect();
    assert_eq!(current, ["v-current"]);
}

#[test]
fn a_described_deletion_reports_the_scheduled_date_and_no_request_moment() {
    let scheduled = SmithyDateTime::from_secs(1_800_000_000);
    let output = DescribeSecretOutput::builder()
        .name("prod/api/key")
        .deleted_date(scheduled)
        .build();

    let details = details_from_describe("prod/api/key", &output);

    assert_eq!(
        details.summary.deletion_scheduled_for,
        smithy_datetime_to_chrono(&scheduled)
    );
    assert!(
        details.deleted_at.is_none(),
        "DeletedDate is when the deletion takes effect, not when it was requested"
    );
}

#[test]
fn a_secret_that_is_not_being_deleted_reports_no_deletion() {
    let output = DescribeSecretOutput::builder().name("prod/api/key").build();

    let details = details_from_describe("prod/api/key", &output);

    assert!(details.summary.deletion_scheduled_for.is_none());
    assert!(details.deleted_at.is_none());
}
