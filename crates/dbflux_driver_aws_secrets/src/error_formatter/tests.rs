use super::*;

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
