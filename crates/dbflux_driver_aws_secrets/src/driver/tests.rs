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
