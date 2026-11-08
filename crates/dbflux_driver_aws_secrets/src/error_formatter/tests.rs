use super::*;
use dbflux_core::secrecy::{SecretSlice, SecretString};
use dbflux_core::{SecretPayload, scrub_submitted_payload};

fn config() -> AwsSecretsProfileConfig {
    AwsSecretsProfileConfig {
        region: "us-east-1".to_string(),
        profile: None,
        access_key_id: None,
        endpoint: None,
    }
}

fn classify(code: &str, message: &str, target: ErrorTarget) -> DbError {
    classify_operation_error(format_from_code(Some(code), message, &config(), &target))
}

fn secret_target() -> ErrorTarget<'static> {
    ErrorTarget::Secret {
        store: "secrets-manager",
        name: "prod/db",
    }
}

#[test]
fn access_denied_maps_to_permission_denied_with_the_aws_message() {
    for code in ["AccessDeniedException", "AccessDenied"] {
        match classify(code, "User is not authorized", secret_target()) {
            DbError::PermissionDenied(details) => {
                assert_eq!(details.message, "User is not authorized");
                assert!(details.hint.unwrap_or_default().contains("IAM"));
            }
            other => panic!("expected PermissionDenied for {code}, got {other:?}"),
        }
    }
}

/// A denied `ec2:DescribeRegions` comes back as EC2's own
/// `UnauthorizedOperation` code, and the region catalog must read it as a
/// permission error, not a generic failure.
#[test]
fn unauthorized_operation_maps_to_permission_denied() {
    match classify(
        "UnauthorizedOperation",
        "not authorized to perform: ec2:DescribeRegions",
        ErrorTarget::None,
    ) {
        DbError::PermissionDenied(details) => {
            assert_eq!(details.code.as_deref(), Some("UnauthorizedOperation"));
            assert_eq!(
                details.message,
                "not authorized to perform: ec2:DescribeRegions"
            );
        }
        other => panic!("expected PermissionDenied, got {other:?}"),
    }
}

#[test]
fn kms_decrypt_denial_maps_to_permission_denied_and_names_kms() {
    let cases = [
        (
            "AccessDeniedException",
            "User: arn:aws:iam::1:user/a is not authorized to perform: kms:Decrypt",
        ),
        (
            "DecryptionFailure",
            "Secrets Manager can't decrypt the protected secret text using the provided KMS key.",
        ),
        ("KMSAccessDeniedException", "Access to KMS is not allowed"),
    ];

    for (code, message) in cases {
        match classify(code, message, secret_target()) {
            DbError::PermissionDenied(details) => {
                assert_eq!(details.message, message);
                assert!(
                    details.hint.unwrap_or_default().contains("kms:Decrypt"),
                    "hint for {code} must name kms:Decrypt"
                );
            }
            other => panic!("expected PermissionDenied for {code}, got {other:?}"),
        }
    }
}

#[test]
fn missing_secrets_and_parameters_map_to_object_not_found() {
    for code in [
        "ResourceNotFoundException",
        "ParameterNotFound",
        "ParameterVersionNotFound",
        "InvalidResourceId",
    ] {
        assert!(
            matches!(
                classify(code, "not found", secret_target()),
                DbError::ObjectNotFound(_)
            ),
            "{code} must map to ObjectNotFound"
        );
    }
}

#[test]
fn credential_errors_map_to_auth_failed() {
    for code in [
        "UnrecognizedClientException",
        "InvalidSignatureException",
        "ExpiredTokenException",
        "InvalidClientTokenId",
    ] {
        let formatted = format_from_code(
            Some(code),
            "bad",
            &config(),
            &ErrorTarget::Store("parameter-store"),
        );
        assert!(
            matches!(classify_connection_error(formatted), DbError::AuthFailed(_)),
            "{code} must map to AuthFailed"
        );
    }
}

#[test]
fn throttling_is_retriable() {
    let formatted = format_from_code(
        Some("ThrottlingException"),
        "Rate exceeded",
        &config(),
        &ErrorTarget::Store("parameter-store"),
    );

    assert!(formatted.retriable);
}

#[test]
fn unknown_code_falls_back_to_query_failed() {
    assert!(matches!(
        classify(
            "SomethingNew",
            "unexpected",
            ErrorTarget::Store("parameter-store")
        ),
        DbError::QueryFailed(_)
    ));
}

#[test]
fn connection_errors_without_a_known_code_map_to_connection_failed() {
    let formatted = format_from_code(
        Some("SomethingNew"),
        "x",
        &config(),
        &ErrorTarget::Store("parameter-store"),
    );

    assert!(matches!(
        classify_connection_error(formatted),
        DbError::ConnectionFailed(_)
    ));
}

#[test]
fn detail_names_the_store_the_secret_and_the_endpoint() {
    let config = AwsSecretsProfileConfig {
        endpoint: Some("http://localhost:4566".to_string()),
        ..config()
    };

    let formatted = format_from_code(
        Some("ResourceNotFoundException"),
        "not found",
        &config,
        &secret_target(),
    );

    let detail = formatted.detail.unwrap_or_default();
    assert!(detail.contains("store=secrets-manager"), "{detail}");
    assert!(detail.contains("name=\"prod/db\""), "{detail}");
    assert!(detail.contains("localhost:4566"), "{detail}");
}

#[test]
fn missing_credentials_dispatch_message_is_actionable() {
    let formatted = format_sdk_message(
        "No credentials found in credential chain",
        &config(),
        &ErrorTarget::Store("parameter-store"),
    );

    assert!(
        formatted
            .hint
            .unwrap_or_default()
            .to_lowercase()
            .contains("credentials")
    );
}

/// A denied region listing has no store behind it, so the permission hint
/// names the EC2 action instead of pointing at secret or parameter IAM.
#[test]
fn a_denied_region_listing_names_ec2_describe_regions() {
    let formatted = format_from_code(
        Some("UnauthorizedOperation"),
        "You are not authorized to perform this operation.",
        &config(),
        &ErrorTarget::None,
    );

    assert_eq!(
        formatted.hint.as_deref(),
        Some("Check that these credentials may call ec2:DescribeRegions.")
    );
}

/// A denial names the object of the store it belongs to: a reader looking for
/// `secretsmanager:GetSecretValue` should not be sent to the parameter page.
#[test]
fn a_denied_secret_names_the_secret_permission() {
    let formatted = format_from_code(
        Some("AccessDeniedException"),
        "not allowed",
        &config(),
        &ErrorTarget::Store("secrets-manager"),
    );

    assert_eq!(
        formatted.hint.as_deref(),
        Some("Check the IAM permissions for this secret.")
    );
}

#[test]
fn a_denied_parameter_names_the_parameter_permission() {
    let formatted = format_from_code(
        Some("AccessDeniedException"),
        "not allowed",
        &config(),
        &ErrorTarget::Secret {
            store: "parameter-store",
            name: "/app/prod/db",
        },
    );

    assert_eq!(
        formatted.hint.as_deref(),
        Some("Check the IAM permissions for this parameter.")
    );
}

#[test]
fn a_denied_store_this_driver_does_not_know_stays_generic() {
    let formatted = format_from_code(
        Some("AccessDenied"),
        "not allowed",
        &config(),
        &ErrorTarget::Store("some-other-store"),
    );

    assert_eq!(
        formatted.hint.as_deref(),
        Some("Check the IAM permissions for this item.")
    );
}

// --- Scrubbing a submitted value from a put error ---

// --- Scrubbing a submitted value from a put error ---

fn text_payload(value: &str) -> SecretPayload {
    SecretPayload::Text(SecretString::from(value.to_string()))
}

fn binary_payload(bytes: &[u8]) -> SecretPayload {
    SecretPayload::Binary(SecretSlice::from(bytes.to_vec()))
}

/// A put error must never carry the value the write submitted: a service
/// message that echoes it would print the very secret the write carried.
#[test]
fn a_put_error_loses_the_text_value_it_submitted() {
    let value = "s3cr3t-hunter2";
    let error = DbError::QueryFailed(
        FormattedError::new(format!(
            "Validation failed for value {value}; value {value} rejected"
        ))
        .with_hint(format!("Check the value {value}"))
        .with_detail(format!("submitted={value}")),
    );

    let scrubbed = scrub_submitted_payload(error, &text_payload(value));
    let rendered = scrubbed.to_string();
    assert!(!rendered.contains(value), "{rendered}");
    assert!(rendered.contains("[redacted]"), "{rendered}");
    assert!(rendered.contains("Validation failed"), "{rendered}");

    match &scrubbed {
        DbError::QueryFailed(formatted) => {
            assert!(
                formatted
                    .hint
                    .as_deref()
                    .unwrap_or_default()
                    .contains("[redacted]")
            );
            assert!(
                formatted
                    .detail
                    .as_deref()
                    .unwrap_or_default()
                    .contains("[redacted]")
            );
        }
        other => panic!("expected QueryFailed, got {other:?}"),
    }
}

/// A binary value a service echoed as received bytes is scrubbed in its
/// lossy UTF-8 rendering.
#[test]
fn a_put_error_loses_the_binary_value_it_submitted() {
    let error = DbError::PermissionDenied(FormattedError::new("Invalid parameter value topsecret"));

    let scrubbed = scrub_submitted_payload(error, &binary_payload(b"topsecret"));
    let rendered = scrubbed.to_string();
    assert!(!rendered.contains("topsecret"), "{rendered}");
    assert!(rendered.contains("[redacted]"), "{rendered}");
}

/// A provider that echoes the request body inside another JSON document
/// writes the value escaped once per level of nesting; every level must go.
#[test]
fn a_put_error_loses_a_value_echoed_once_and_twice_json_escaped() {
    let escape = |text: &str| {
        text.replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
    };
    let value = "pa\"ss\\word\nline";
    let once = escape(value);
    let twice = escape(&once);
    let thrice = escape(&twice);

    for echoed in [&once, &twice, &thrice] {
        let error = DbError::QueryFailed(
            FormattedError::new(format!("rejected body {{\"Value\":\"{echoed}\"}}"))
                .with_detail(format!("echo={echoed}")),
        );

        let rendered = scrub_submitted_payload(error, &text_payload(value)).to_string();

        for form in [value, &once, &twice, &thrice] {
            assert!(!rendered.contains(form), "{form}: {rendered}");
        }
        assert!(rendered.contains("[redacted]"), "{rendered}");
    }
}

/// Short and Unicode escapes of a control character, and a mix of both in one
/// echo, are the same value.
#[test]
fn a_put_error_loses_a_value_whose_control_characters_are_escaped_differently() {
    let value = "a\u{8}b\u{c}c\nd";

    for echoed in [
        "a\\bb\\fc\\nd",
        "a\\u0008b\\u000cc\\u000ad",
        "a\\bb\\u000cc\\nd",
        "a\\u0008b\\fc\\u000Ad",
    ] {
        let error = DbError::QueryFailed(FormattedError::new(format!("echo {echoed} end")));

        let rendered = scrub_submitted_payload(error, &text_payload(value)).to_string();

        assert_eq!(rendered, "echo [redacted] end", "{echoed}");
    }
}

/// An empty value scrubs nothing: replacing an empty string would mangle
/// every character of the message.
#[test]
fn an_empty_submitted_value_leaves_the_error_untouched() {
    let error = DbError::NotSupported("store stores values as text only".to_string());

    let scrubbed = scrub_submitted_payload(error, &text_payload(""));
    match &scrubbed {
        DbError::NotSupported(message) => {
            assert_eq!(message, "store stores values as text only");
        }
        other => panic!("expected NotSupported, got {other:?}"),
    }
}

/// Errors that carry no message the value could hide in come back unchanged.
#[test]
fn errors_without_a_message_pass_through_the_scrub() {
    let scrubbed = scrub_submitted_payload(DbError::Timeout, &text_payload("hunter2"));
    assert!(matches!(scrubbed, DbError::Timeout));

    let scrubbed = scrub_submitted_payload(DbError::Cancelled, &text_payload("hunter2"));
    assert!(matches!(scrubbed, DbError::Cancelled));
}

#[test]
fn a_name_that_already_exists_is_a_constraint_violation_with_the_aws_message() {
    for code in ["ResourceExistsException", "ParameterAlreadyExists"] {
        match classify(code, "The name is taken", secret_target()) {
            DbError::ConstraintViolation(formatted) => {
                assert!(formatted.message.contains("The name is taken"), "{code}");
                assert_eq!(formatted.code.as_deref(), Some(code));
            }
            other => panic!("{code}: expected a constraint violation, got {other:?}"),
        }
    }
}
