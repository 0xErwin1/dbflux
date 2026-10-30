use aws_sdk_secretsmanager::error::{ProvideErrorMetadata, SdkError};
use dbflux_core::{DbError, FormattedError};

use crate::driver::AwsSecretsProfileConfig;

/// The store and secret an operation was scoped to, so a permission or
/// not-found error names what was denied or missing instead of carrying
/// only AWS's own message.
pub(crate) enum ErrorTarget<'a> {
    Store(&'a str),
    Secret {
        store: &'a str,
        name: &'a str,
    },
    /// An operation that belongs to no store, such as listing the regions the
    /// connection can move to.
    None,
}

impl ErrorTarget<'_> {
    fn detail_fragment(&self) -> String {
        match self {
            ErrorTarget::Store(store) => format!("store={store}"),
            ErrorTarget::Secret { store, name } => format!("store={store}, name=\"{name}\""),
            ErrorTarget::None => String::new(),
        }
    }
}

/// Formats any Secrets Manager or SSM SDK error. Both SDKs share the smithy
/// `SdkError` and `ProvideErrorMetadata` types, so one helper covers every
/// operation of both services.
pub(crate) fn format_service_error<E>(
    error: &SdkError<E>,
    config: &AwsSecretsProfileConfig,
    target: &ErrorTarget,
) -> FormattedError
where
    E: ProvideErrorMetadata,
{
    if let Some(service_error) = error.as_service_error() {
        let message = service_error.message().unwrap_or("AWS service error");
        return format_from_code(service_error.code(), message, config, target);
    }

    format_sdk_message(&error.to_string(), config, target)
}

const AUTH_CODES: &[&str] = &[
    "UnrecognizedClientException",
    "InvalidSignatureException",
    "SignatureDoesNotMatch",
    "IncompleteSignature",
    "MissingAuthenticationToken",
    "InvalidClientTokenId",
    "InvalidAccessKeyId",
    "ExpiredToken",
    "ExpiredTokenException",
];

/// EC2 reports a denied `DescribeRegions` as `UnauthorizedOperation`, so a
/// region catalog fetched without the permission reads as a denial.
const PERMISSION_CODES: &[&str] = &[
    "AccessDeniedException",
    "AccessDenied",
    "UnauthorizedOperation",
];

/// Codes for a value the caller may not decrypt. Secrets Manager reports a
/// missing `kms:Decrypt` as `DecryptionFailure`.
const KMS_PERMISSION_CODES: &[&str] = &["DecryptionFailure", "KMSAccessDeniedException"];

/// Codes for a KMS key that exists but cannot be used in its current state.
const KMS_KEY_STATE_CODES: &[&str] = &[
    "InvalidKeyId",
    "KMSDisabledException",
    "KMSInvalidStateException",
    "KMSNotFoundException",
];

const NOT_FOUND_CODES: &[&str] = &[
    "ResourceNotFoundException",
    "ParameterNotFound",
    "ParameterVersionNotFound",
    "InvalidResourceId",
];

const RETRIABLE_CODES: &[&str] = &[
    "ThrottlingException",
    "Throttling",
    "TooManyRequestsException",
    "InternalServiceError",
    "InternalServerError",
    "InternalFailure",
    "ServiceUnavailable",
];

const KMS_DECRYPT_HINT: &str =
    "Reading this value also needs kms:Decrypt on the KMS key it is encrypted with.";

fn has_code(code: Option<&str>, codes: &[&str]) -> bool {
    code.is_some_and(|code| codes.contains(&code))
}

/// A permission error that names KMS is a denied decrypt, not a denied read
/// of the secret itself.
fn is_kms_denial(code: Option<&str>, message: &str) -> bool {
    has_code(code, KMS_PERMISSION_CODES)
        || (has_code(code, PERMISSION_CODES) && message.to_lowercase().contains("kms"))
}

/// The policy a permission denial points at.
///
/// Each store needs its own permission, and a denial that belongs to no store
/// is the region catalog: EC2 reports it as `UnauthorizedOperation`, and
/// `ec2:DescribeRegions` has nothing to do with secrets or parameters. Naming
/// the wrong policy sends the reader to the wrong page of their IAM console.
fn permission_hint(target: &ErrorTarget) -> &'static str {
    match target {
        ErrorTarget::None => "Check that these credentials may call ec2:DescribeRegions.",
        ErrorTarget::Store(store) => store_permission_hint(store),
        ErrorTarget::Secret { store, .. } => store_permission_hint(store),
    }
}

fn store_permission_hint(store: &str) -> &'static str {
    match store {
        crate::secrets_manager::SECRETS_MANAGER_ID => {
            "Check the IAM permissions for this secret."
        }
        crate::parameter_store::PARAMETER_STORE_ID => {
            "Check the IAM permissions for this parameter."
        }
        _ => "Check the IAM permissions for this item.",
    }
}

fn hint_for(code: Option<&str>, message: &str, target: &ErrorTarget) -> Option<&'static str> {
    if is_kms_denial(code, message) {
        return Some(KMS_DECRYPT_HINT);
    }

    if has_code(code, AUTH_CODES) {
        return Some(
            "Check AWS credentials (static access key, profile, or SSO session) and retry.",
        );
    }

    if has_code(code, PERMISSION_CODES) {
        return Some(permission_hint(target));
    }

    if has_code(code, KMS_KEY_STATE_CODES) {
        return Some("Check that the KMS key exists, is enabled, and is in this region.");
    }

    if has_code(code, NOT_FOUND_CODES) {
        return Some(
            "Check the name or version. It may have been deleted or live in another region.",
        );
    }

    if has_code(code, RETRIABLE_CODES) {
        return Some(
            "Request was throttled or the service is temporarily unavailable. Retry with backoff.",
        );
    }

    None
}

pub(crate) fn format_from_code(
    code: Option<&str>,
    message: &str,
    config: &AwsSecretsProfileConfig,
    target: &ErrorTarget,
) -> FormattedError {
    let mut formatted = FormattedError::new(message.to_string());

    if let Some(code) = code {
        formatted = formatted.with_code(code.to_string());
    }

    if let Some(hint) = hint_for(code, message, target) {
        formatted = formatted.with_hint(hint);
    }

    if has_code(code, RETRIABLE_CODES) {
        formatted = formatted.with_retriable(true);
    }

    formatted.with_detail(with_target_prefix(target, config.diagnostic_detail()))
}

pub(crate) fn format_sdk_message(
    message: &str,
    config: &AwsSecretsProfileConfig,
    target: &ErrorTarget,
) -> FormattedError {
    let lower = message.to_lowercase();

    let formatted = if lower.contains("credential") || lower.contains("token") {
        FormattedError::new("AWS credentials were not found or are invalid.").with_hint(
            "Configure credentials via a static access key, an AWS profile, or SSO login.",
        )
    } else if lower.contains("dispatch") {
        FormattedError::new("AWS SDK dispatch failure (transient error).")
            .with_hint("This is usually a temporary issue. Try the operation again, or refresh AWS credentials if using SSO.")
            .with_retriable(true)
    } else if lower.contains("timed out") || lower.contains("timeout") {
        FormattedError::new("Connection to AWS timed out.")
            .with_hint("Check network connectivity, endpoint reachability, and region.")
            .with_retriable(true)
    } else if lower.contains("dns")
        || lower.contains("resolve")
        || lower.contains("endpoint")
        || lower.contains("connection refused")
    {
        FormattedError::new("Unable to reach the AWS endpoint.")
            .with_hint("Check the endpoint override and region configuration.")
    } else {
        FormattedError::new(message.to_string())
    };

    formatted.with_detail(with_target_prefix(target, config.diagnostic_detail()))
}

fn with_target_prefix(target: &ErrorTarget, diagnostic_detail: String) -> String {
    let fragment = target.detail_fragment();

    if fragment.is_empty() {
        return diagnostic_detail;
    }

    format!("{fragment}, {diagnostic_detail}")
}

/// Maps a formatted error onto the `DbError` that fits a connect or probe
/// failure.
pub(crate) fn classify_connection_error(formatted: FormattedError) -> DbError {
    let code = formatted.code.as_deref();

    if has_code(code, AUTH_CODES) {
        return DbError::AuthFailed(formatted);
    }

    if has_code(code, PERMISSION_CODES) {
        return DbError::PermissionDenied(formatted);
    }

    DbError::ConnectionFailed(formatted)
}

/// Maps a formatted error onto the `DbError` that fits an operation on an
/// established connection. A denied KMS decrypt is a permission error, so a
/// value the caller may not decrypt never reads as empty.
pub(crate) fn classify_operation_error(formatted: FormattedError) -> DbError {
    let code = formatted.code.as_deref();

    if has_code(code, AUTH_CODES) {
        return DbError::AuthFailed(formatted);
    }

    if has_code(code, PERMISSION_CODES) || has_code(code, KMS_PERMISSION_CODES) {
        return DbError::PermissionDenied(formatted);
    }

    if has_code(code, NOT_FOUND_CODES) {
        return DbError::ObjectNotFound(formatted);
    }

    DbError::QueryFailed(formatted)
}

#[cfg(test)]
mod tests;
