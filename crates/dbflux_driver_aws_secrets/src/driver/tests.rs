use super::*;

fn form_values(pairs: &[(&str, &str)]) -> FormValues {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

fn form_field_ids() -> Vec<String> {
    AWS_SECRETS_FORM
        .tabs
        .iter()
        .flat_map(|tab| tab.sections.iter())
        .flat_map(|section| section.fields.iter())
        .map(|field| field.id.clone())
        .collect()
}

#[test]
fn metadata_declares_the_secret_store_category_and_capability() {
    assert_eq!(AWS_SECRETS_METADATA.id, "aws_secrets");
    assert_eq!(AWS_SECRETS_METADATA.category, DatabaseCategory::SecretStore);
    assert!(
        AWS_SECRETS_METADATA
            .capabilities
            .contains(DriverCapabilities::SECRET_STORE | DriverCapabilities::AUTHENTICATION)
    );
}

#[test]
fn form_declares_aws_fields_without_path_style() {
    assert_eq!(
        form_field_ids(),
        ["region", "profile", "access_key_id", "password", "endpoint"]
    );

    let profile = AWS_SECRETS_FORM
        .tabs
        .iter()
        .flat_map(|tab| tab.sections.iter())
        .flat_map(|section| section.fields.iter())
        .find(|field| field.id == "profile")
        .expect("profile field");
    assert_eq!(
        profile.kind,
        FormFieldKind::AuthProfileRef { provider_id: None }
    );
}

#[test]
fn build_config_requires_region() {
    let error = AwsSecretsDriver::new()
        .build_config(&form_values(&[]))
        .expect_err("region is required");

    match error {
        DbError::InvalidProfile(message) => assert!(message.to_lowercase().contains("region")),
        other => panic!("expected InvalidProfile, got {other:?}"),
    }
}

#[test]
fn build_config_rejects_a_profile_and_static_keys_together() {
    let error = AwsSecretsDriver::new()
        .build_config(&form_values(&[
            ("region", "us-east-1"),
            ("profile", "11111111-1111-1111-1111-111111111111"),
            ("access_key_id", "AKIAEXAMPLE"),
        ]))
        .expect_err("both credential sources must be rejected");

    match error {
        DbError::InvalidProfile(message) => {
            assert!(message.contains("Profile"), "{message}");
            assert!(message.contains("Access Key ID"), "{message}");
        }
        other => panic!("expected InvalidProfile, got {other:?}"),
    }
}

#[test]
fn build_config_then_extract_values_round_trips() {
    let driver = AwsSecretsDriver::new();
    let original = form_values(&[
        ("region", " eu-west-1 "),
        ("access_key_id", "AKIAROUNDTRIP"),
        ("endpoint", "http://localhost:4566"),
    ]);

    let config = driver.build_config(&original).expect("valid config");
    match &config {
        DbConfig::AwsSecrets {
            region,
            profile,
            access_key_id,
            endpoint,
        } => {
            assert_eq!(region, "eu-west-1");
            assert_eq!(profile, &None);
            assert_eq!(access_key_id.as_deref(), Some("AKIAROUNDTRIP"));
            assert_eq!(endpoint.as_deref(), Some("http://localhost:4566"));
        }
        other => panic!("expected DbConfig::AwsSecrets, got {other:?}"),
    }

    let extracted = driver.extract_values(&config);
    assert_eq!(
        extracted.get("region").map(String::as_str),
        Some("eu-west-1")
    );
    assert_eq!(extracted.get("profile").map(String::as_str), Some(""));
    assert_eq!(
        extracted.get("access_key_id").map(String::as_str),
        Some("AKIAROUNDTRIP")
    );
    assert_eq!(
        extracted.get("endpoint").map(String::as_str),
        Some("http://localhost:4566")
    );
}

#[test]
fn extract_values_ignores_other_configs() {
    let values = AwsSecretsDriver::new().extract_values(&DbConfig::default_s3());

    assert!(values.is_empty());
}

#[test]
fn profile_config_rejects_a_blank_region() {
    let error = profile_config(&DbConfig::AwsSecrets {
        region: "  ".to_string(),
        profile: None,
        access_key_id: None,
        endpoint: None,
    })
    .expect_err("blank region");

    assert!(matches!(error, DbError::InvalidProfile(_)));
}

fn secrets_config(region: &str) -> DbConfig {
    DbConfig::AwsSecrets {
        region: region.to_string(),
        profile: Some("11111111-1111-1111-1111-111111111111".to_string()),
        access_key_id: None,
        endpoint: Some("http://localhost:4566".to_string()),
    }
}

#[test]
fn metadata_advertises_the_region_switch() {
    assert!(
        AWS_SECRETS_METADATA
            .capabilities
            .contains(DriverCapabilities::REGION_SWITCH)
    );
}

#[test]
fn with_region_rewrites_only_the_region() {
    let config = secrets_config("us-east-1");

    let switched = AwsSecretsDriver::new()
        .with_region(&config, " eu-west-1 ")
        .expect("the aws secrets driver supports a region");

    match (&config, &switched) {
        (
            DbConfig::AwsSecrets {
                profile: before_profile,
                access_key_id: before_keys,
                endpoint: before_endpoint,
                ..
            },
            DbConfig::AwsSecrets {
                region,
                profile,
                access_key_id,
                endpoint,
            },
        ) => {
            assert_eq!(region, "eu-west-1");
            assert_eq!(profile, before_profile);
            assert_eq!(access_key_id, before_keys);
            assert_eq!(endpoint, before_endpoint);
        }
        other => panic!("expected two DbConfig::AwsSecrets, got {other:?}"),
    }
}

#[test]
fn with_region_refuses_an_empty_region() {
    let config = secrets_config("us-east-1");

    assert!(
        AwsSecretsDriver::new()
            .with_region(&config, "   ")
            .is_none()
    );
}

#[test]
fn with_region_ignores_another_drivers_config() {
    let config = DbConfig::default_s3();

    assert!(
        AwsSecretsDriver::new()
            .with_region(&config, "eu-west-1")
            .is_none()
    );
}

#[test]
fn region_names_come_back_sorted_trimmed_and_distinct() {
    let names = normalize_regions([
        "us-west-2".to_string(),
        " eu-west-1 ".to_string(),
        "us-west-2".to_string(),
        String::new(),
        "ap-south-1".to_string(),
    ]);

    assert_eq!(names, ["ap-south-1", "eu-west-1", "us-west-2"]);
}

#[test]
fn the_region_catalog_fetches_once_and_reuses_the_answer() {
    let catalog = RegionCatalog::new();
    let mut fetches = 0;

    for _ in 0..2 {
        let regions = catalog
            .regions(|| {
                fetches += 1;
                Ok(vec!["eu-west-1".to_string()])
            })
            .expect("regions");
        assert_eq!(regions, ["eu-west-1"]);
    }

    assert_eq!(fetches, 1);
}

#[test]
fn the_region_catalog_does_not_cache_a_failure() {
    let catalog = RegionCatalog::new();

    let failed = catalog.regions(|| {
        Err(DbError::PermissionDenied(dbflux_core::FormattedError::new(
            "denied",
        )))
    });
    assert!(failed.is_err());

    let regions = catalog
        .regions(|| Ok(vec!["eu-west-1".to_string()]))
        .expect("the retry reaches the provider");

    assert_eq!(regions, ["eu-west-1"]);
}

/// Two concurrent callers of a cold catalog share one fetch: the lock is held
/// from the cache check through the cache write, so the second caller waits
/// for the first one's answer instead of asking the provider again.
#[test]
fn concurrent_callers_of_a_cold_catalog_share_one_fetch() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let catalog = Arc::new(RegionCatalog::new());
    let fetches = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(std::sync::Barrier::new(2));

    let callers: Vec<_> = (0..2)
        .map(|_| {
            let catalog = catalog.clone();
            let fetches = fetches.clone();
            let start = start.clone();

            std::thread::spawn(move || {
                start.wait();
                catalog
                    .regions(|| {
                        fetches.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(std::time::Duration::from_millis(50));
                        Ok(vec!["eu-west-1".to_string()])
                    })
                    .expect("regions")
            })
        })
        .collect();

    for caller in callers {
        let regions = caller.join().expect("caller thread");
        assert_eq!(regions, ["eu-west-1"]);
    }

    assert_eq!(fetches.load(Ordering::SeqCst), 1);
}
