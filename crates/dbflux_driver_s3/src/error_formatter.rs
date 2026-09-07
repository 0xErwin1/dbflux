use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use dbflux_core::{DbError, FormattedError};

use crate::driver::S3ProfileConfig;

/// Formats `aws-sdk-s3` errors into structured, actionable messages and maps
/// them onto the closest matching `DbError` variant.
///
/// Every S3 operation (`ListBuckets`, `ListObjectsV2`, `DeleteBucket`, and the
/// object-body operations landing in later batches) raises a distinct
/// generated error type, but they all implement `ProvideErrorMetadata`, so a
/// single generic helper (`format_service_error`) covers every call site
/// instead of duplicating the code/message extraction per operation.
pub(crate) struct S3ErrorFormatter;

/// The bucket/object an S3 operation was scoped to, so a permission or
/// not-found error can name what was denied or missing instead of only
/// carrying AWS's own message, which for `AccessDenied` in particular is
/// typically just "Access Denied" with no target information at all.
pub(crate) enum ErrorTarget<'a> {
    /// Connection/account-level operations (e.g. `ListBuckets`) with no
    /// single bucket or object in scope.
    None,
    Bucket(&'a str),
    Object {
        bucket: &'a str,
        key: &'a str,
    },
}

impl ErrorTarget<'_> {
    fn detail_fragment(&self) -> Option<String> {
        match self {
            ErrorTarget::None => None,
            ErrorTarget::Bucket(bucket) => Some(format!("bucket=\"{bucket}\"")),
            ErrorTarget::Object { bucket, key } => {
                Some(format!("bucket=\"{bucket}\", key=\"{key}\""))
            }
        }
    }
}

impl S3ErrorFormatter {
    fn format_from_code(
        &self,
        code: Option<&str>,
        message: &str,
        config: &S3ProfileConfig,
        target: &ErrorTarget,
    ) -> FormattedError {
        let mut formatted = FormattedError::new(message.to_string());

        if let Some(code_value) = code {
            formatted = formatted.with_code(code_value.to_string());
        }

        let hint = match code {
            Some(
                "InvalidAccessKeyId"
                | "SignatureDoesNotMatch"
                | "ExpiredToken"
                | "TokenRefreshRequired"
                | "InvalidToken"
                | "AuthorizationHeaderMalformed",
            ) => Some(
                "Check AWS credentials (static access key, profile, or SSO session) and retry.",
            ),
            Some("AccessDenied") => {
                Some("Check IAM permissions for this bucket or object, and the bucket policy.")
            }
            Some("NoSuchBucket") => Some(
                "Check the bucket name and the configured region — bucket names are region-scoped.",
            ),
            Some("NoSuchKey") => Some("Check the object key — it may have been moved or deleted."),
            Some("BucketAlreadyExists") => Some(
                "Bucket names are globally unique across all AWS accounts. Choose a different name.",
            ),
            Some("BucketAlreadyOwnedByYou") => {
                Some("A bucket with this name already exists in your account in another region.")
            }
            Some("BucketNotEmpty") => {
                Some("Empty the bucket (delete every object and version) before deleting it.")
            }
            Some("PermanentRedirect") => Some(
                "This bucket lives in a different region than the one configured. Update the connection's region.",
            ),
            Some("SlowDown" | "RequestTimeout" | "InternalError" | "ServiceUnavailable") => Some(
                "Request was throttled or the service is temporarily unavailable. Retry with backoff.",
            ),
            _ => None,
        };

        if let Some(hint_value) = hint {
            formatted = formatted.with_hint(hint_value);
        }

        if code.is_some_and(|value| {
            matches!(
                value,
                "SlowDown" | "RequestTimeout" | "InternalError" | "ServiceUnavailable"
            )
        }) {
            formatted = formatted.with_retriable(true);
        }

        formatted.with_detail(with_target_prefix(target, config.diagnostic_detail()))
    }

    fn format_sdk_message(
        &self,
        message: &str,
        config: &S3ProfileConfig,
        target: &ErrorTarget,
    ) -> FormattedError {
        let lower = message.to_lowercase();

        let formatted = if lower.contains("credential") || lower.contains("token") {
            FormattedError::new("AWS credentials were not found or are invalid.").with_hint(
                "Configure credentials via a static access key, an AWS profile, or SSO login.",
            )
        } else if lower.contains("dispatch failure") || lower.contains("dispatch") {
            FormattedError::new("AWS SDK dispatch failure (transient error).")
                .with_hint("This is usually a temporary issue. Try the operation again, or refresh AWS credentials if using SSO.")
                .with_retriable(true)
        } else if lower.contains("timed out") || lower.contains("timeout") {
            FormattedError::new("Connection to S3 timed out.")
                .with_hint("Check network connectivity, endpoint reachability, and region.")
                .with_retriable(true)
        } else if lower.contains("dns")
            || lower.contains("resolve")
            || lower.contains("endpoint")
            || lower.contains("connection refused")
        {
            FormattedError::new("Unable to reach the S3 endpoint.").with_hint(
                "Check the endpoint override, path-style setting, and region configuration.",
            )
        } else {
            FormattedError::new(message.to_string())
        };

        formatted.with_detail(with_target_prefix(target, config.diagnostic_detail()))
    }

    /// Format any S3 SDK error into a `FormattedError`, extracting the
    /// service-reported code and message when available and falling back to
    /// the SDK's own transport/dispatch message otherwise. `target` names the
    /// bucket/object the failing operation was scoped to, when known, so
    /// `AccessDenied`/`NoSuchBucket`/`NoSuchKey` errors identify what was
    /// denied or missing instead of only carrying AWS's generic message.
    pub(crate) fn format_service_error<E>(
        &self,
        error: &SdkError<E>,
        config: &S3ProfileConfig,
        target: ErrorTarget,
    ) -> FormattedError
    where
        E: ProvideErrorMetadata,
    {
        if let Some(service_error) = error.as_service_error() {
            let code = service_error.code();
            let message = service_error.message().unwrap_or("S3 service error");
            return self.format_from_code(code, message, config, &target);
        }

        self.format_sdk_message(&error.to_string(), config, &target)
    }
}

/// Prepends the target fragment (when any) to the region/endpoint diagnostic
/// detail, so the UI's error message identifies which bucket/object the
/// operation was scoped to.
fn with_target_prefix(target: &ErrorTarget, diagnostic_detail: String) -> String {
    match target.detail_fragment() {
        Some(fragment) => format!("{fragment}, {diagnostic_detail}"),
        None => diagnostic_detail,
    }
}

pub(crate) static S3_ERROR_FORMATTER: S3ErrorFormatter = S3ErrorFormatter;

/// Map a formatted S3 error onto the `DbError` variant that best fits a
/// connect/probe-time failure (used by `connect_with_secrets`, `test_connection`,
/// and `Connection::ping`).
pub(crate) fn classify_connection_error(formatted: FormattedError) -> DbError {
    match formatted.code.as_deref() {
        Some(
            "InvalidAccessKeyId"
            | "SignatureDoesNotMatch"
            | "ExpiredToken"
            | "TokenRefreshRequired"
            | "InvalidToken"
            | "AuthorizationHeaderMalformed",
        ) => DbError::AuthFailed(formatted),
        Some("AccessDenied") => DbError::PermissionDenied(formatted),
        _ => DbError::ConnectionFailed(formatted),
    }
}

/// Map a formatted S3 error onto the `DbError` variant that best fits an
/// operation carried out on an already-established connection (list, delete,
/// etc.).
pub(crate) fn classify_query_error(formatted: FormattedError) -> DbError {
    match formatted.code.as_deref() {
        Some(
            "InvalidAccessKeyId"
            | "SignatureDoesNotMatch"
            | "ExpiredToken"
            | "TokenRefreshRequired"
            | "InvalidToken"
            | "AuthorizationHeaderMalformed",
        ) => DbError::AuthFailed(formatted),
        Some("AccessDenied") => DbError::PermissionDenied(formatted),
        Some("NoSuchBucket" | "NoSuchKey") => DbError::ObjectNotFound(formatted),
        Some("BucketAlreadyExists" | "BucketAlreadyOwnedByYou" | "BucketNotEmpty") => {
            DbError::ConstraintViolation(formatted)
        }
        _ => DbError::QueryFailed(formatted),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> S3ProfileConfig {
        S3ProfileConfig {
            region: "us-east-1".to_string(),
            profile: None,
            access_key_id: None,
            endpoint: None,
            path_style: false,
        }
    }

    #[test]
    fn no_such_bucket_maps_to_object_not_found() {
        let formatter = S3ErrorFormatter;
        let formatted = formatter.format_from_code(
            Some("NoSuchBucket"),
            "The bucket does not exist",
            &config(),
            &ErrorTarget::None,
        );

        match classify_query_error(formatted) {
            DbError::ObjectNotFound(details) => {
                assert!(
                    details
                        .hint
                        .unwrap_or_default()
                        .to_lowercase()
                        .contains("bucket name")
                );
            }
            other => panic!("expected ObjectNotFound, got {other:?}"),
        }
    }

    #[test]
    fn no_such_key_maps_to_object_not_found() {
        let formatter = S3ErrorFormatter;
        let formatted = formatter.format_from_code(
            Some("NoSuchKey"),
            "The specified key does not exist",
            &config(),
            &ErrorTarget::None,
        );

        assert!(matches!(
            classify_query_error(formatted),
            DbError::ObjectNotFound(_)
        ));
    }

    #[test]
    fn access_denied_maps_to_permission_denied() {
        let formatter = S3ErrorFormatter;
        let formatted = formatter.format_from_code(
            Some("AccessDenied"),
            "Access Denied",
            &config(),
            &ErrorTarget::None,
        );

        match classify_query_error(formatted) {
            DbError::PermissionDenied(details) => {
                assert!(
                    details
                        .hint
                        .unwrap_or_default()
                        .to_lowercase()
                        .contains("iam")
                );
            }
            other => panic!("expected PermissionDenied, got {other:?}"),
        }
    }

    /// T45: an `AccessDenied` on a specific object names the bucket and key
    /// in the detail, not just AWS's generic "Access Denied" message.
    #[test]
    fn access_denied_on_an_object_names_bucket_and_key() {
        let formatter = S3ErrorFormatter;
        let formatted = formatter.format_from_code(
            Some("AccessDenied"),
            "Access Denied",
            &config(),
            &ErrorTarget::Object {
                bucket: "prod-bucket",
                key: "reports/q3.csv",
            },
        );

        let detail = formatted.detail.unwrap_or_default();
        assert!(detail.contains("prod-bucket"));
        assert!(detail.contains("reports/q3.csv"));
    }

    /// T45: an `AccessDenied` scoped only to a bucket (no object) names the
    /// bucket without inventing a key.
    #[test]
    fn access_denied_on_a_bucket_names_the_bucket_only() {
        let formatter = S3ErrorFormatter;
        let formatted = formatter.format_from_code(
            Some("AccessDenied"),
            "Access Denied",
            &config(),
            &ErrorTarget::Bucket("prod-bucket"),
        );

        let detail = formatted.detail.unwrap_or_default();
        assert!(detail.contains("prod-bucket"));
    }

    #[test]
    fn invalid_access_key_maps_to_auth_failed() {
        let formatter = S3ErrorFormatter;
        let formatted = formatter.format_from_code(
            Some("InvalidAccessKeyId"),
            "The AWS Access Key Id you provided does not exist",
            &config(),
            &ErrorTarget::None,
        );

        assert!(matches!(
            classify_connection_error(formatted),
            DbError::AuthFailed(_)
        ));
    }

    #[test]
    fn bucket_not_empty_maps_to_constraint_violation() {
        let formatter = S3ErrorFormatter;
        let formatted = formatter.format_from_code(
            Some("BucketNotEmpty"),
            "The bucket is not empty",
            &config(),
            &ErrorTarget::None,
        );

        match classify_query_error(formatted) {
            DbError::ConstraintViolation(details) => {
                assert!(
                    details
                        .hint
                        .unwrap_or_default()
                        .to_lowercase()
                        .contains("empty")
                );
            }
            other => panic!("expected ConstraintViolation, got {other:?}"),
        }
    }

    #[test]
    fn throttling_hint_is_retriable() {
        let formatter = S3ErrorFormatter;
        let formatted = formatter.format_from_code(
            Some("SlowDown"),
            "Please reduce your request rate",
            &config(),
            &ErrorTarget::None,
        );

        assert!(formatted.retriable);
    }

    #[test]
    fn unknown_code_falls_back_to_query_failed() {
        let formatter = S3ErrorFormatter;
        let formatted = formatter.format_from_code(
            Some("SomeNewException"),
            "Unhandled",
            &config(),
            &ErrorTarget::None,
        );

        assert!(matches!(
            classify_query_error(formatted),
            DbError::QueryFailed(_)
        ));
    }

    #[test]
    fn missing_credentials_dispatch_message_is_actionable() {
        let formatter = S3ErrorFormatter;
        let formatted = formatter.format_sdk_message(
            "No credentials found in credential chain",
            &config(),
            &ErrorTarget::None,
        );

        assert!(
            formatted
                .hint
                .unwrap_or_default()
                .to_lowercase()
                .contains("credentials")
        );
    }

    #[test]
    fn detail_includes_endpoint_when_configured() {
        let formatter = S3ErrorFormatter;
        let config = S3ProfileConfig {
            region: "us-east-1".to_string(),
            profile: None,
            access_key_id: None,
            endpoint: Some("http://localhost:9000".to_string()),
            path_style: true,
        };

        let formatted = formatter.format_from_code(
            Some("NoSuchBucket"),
            "not found",
            &config,
            &ErrorTarget::None,
        );
        assert!(
            formatted
                .detail
                .unwrap_or_default()
                .contains("localhost:9000")
        );
    }
}
