/// AWS Secrets Manager provider implementing `SecretProvider`.
///
/// Resolves secret values from AWS Secrets Manager. Supports both plain
/// string secrets and JSON secrets with field extraction via `json_key`.
/// The locator format is `<secret-name-or-arn>` with an optional
/// `@<region>` suffix to override the default region.
use aws_config::SdkConfig;
use secrecy::SecretString;

use dbflux_core::DbError;
use dbflux_core::values::ProviderError;

pub struct AwsSecretsManagerProvider {
    sdk_config: SdkConfig,
}

impl AwsSecretsManagerProvider {
    pub fn new(sdk_config: SdkConfig) -> Self {
        Self { sdk_config }
    }
}

impl dbflux_core::values::SecretProvider for AwsSecretsManagerProvider {
    fn provider_id(&self) -> &'static str {
        "aws-secrets-manager"
    }

    fn display_name(&self) -> &'static str {
        "AWS Secrets Manager"
    }

    fn get_secret(
        &self,
        locator: &str,
        json_key: Option<&str>,
    ) -> impl std::future::Future<Output = Result<SecretString, DbError>> + Send {
        let sdk_config = self.sdk_config.clone();
        let locator = locator.to_string();
        let json_key = json_key.map(|s| s.to_string());

        async move {
            let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);

            std::thread::spawn(move || {
                let result = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                {
                    Err(err) => Err(DbError::ValueResolutionFailed(format!(
                        "Failed to create Tokio runtime for Secrets Manager: {}",
                        err
                    ))),
                    Ok(rt) => {
                        rt.block_on(get_secret_inner(&sdk_config, &locator, json_key.as_deref()))
                    }
                };

                let _ = result_tx.send(result);
            });

            // Non-blocking poll — yields to the executor between checks so GPUI
            // can continue processing events while the secret is being fetched.
            loop {
                match result_rx.try_recv() {
                    Ok(result) => return result,
                    Err(std::sync::mpsc::TryRecvError::Empty) => {
                        futures_lite::future::yield_now().await;
                    }
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        return Err(DbError::ValueResolutionFailed(
                            "Secrets Manager thread terminated unexpectedly".to_string(),
                        ));
                    }
                }
            }
        }
    }
}

/// Inner async implementation of secret fetching, called from within a
/// dedicated Tokio runtime. Separated so the outer `get_secret` can safely
/// dispatch this through `std::thread::spawn` from any async executor.
async fn get_secret_inner(
    sdk_config: &SdkConfig,
    locator: &str,
    json_key: Option<&str>,
) -> Result<SecretString, DbError> {
    let (secret_id, region_override) = parse_locator(locator);

    let config = match region_override {
        Some(region) => {
            let creds_provider = sdk_config
                .credentials_provider()
                .ok_or_else(|| {
                    DbError::ValueResolutionFailed(
                        "No credentials provider in SDK config".to_string(),
                    )
                })?
                .clone();

            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .credentials_provider(creds_provider)
                .region(aws_config::Region::new(region.to_string()))
                .load()
                .await
        }
        None => sdk_config.clone(),
    };

    let client = aws_sdk_secretsmanager::Client::new(&config);

    let output = client
        .get_secret_value()
        .secret_id(secret_id)
        .send()
        .await
        .map_err(|err| map_secretsmanager_error(err, locator))?;

    let secret_string = output.secret_string().ok_or_else(|| {
        DbError::ValueResolutionFailed(format!(
            "AWS Secrets Manager: secret '{}' is binary, only string secrets are supported",
            locator,
        ))
    })?;

    match json_key {
        Some(key) => extract_json_field(secret_string, key, locator),
        None => Ok(SecretString::from(secret_string.to_string())),
    }
}

fn map_secretsmanager_error(
    err: aws_sdk_secretsmanager::error::SdkError<
        aws_sdk_secretsmanager::operation::get_secret_value::GetSecretValueError,
    >,
    locator: &str,
) -> DbError {
    let (code, message, recovery_hint) = match err.as_service_error() {
        Some(service_err) => {
            let code = service_err.meta().code().unwrap_or("Unknown").to_string();
            let message = service_err
                .meta()
                .message()
                .unwrap_or("No message")
                .to_string();

            let hint = match code.as_str() {
                "ResourceNotFoundException" => Some(format!(
                    "Check that secret '{}' exists and the region is correct",
                    locator
                )),
                "AccessDeniedException" => Some(
                    "Verify your IAM permissions include secretsmanager:GetSecretValue".to_string(),
                ),
                "DecryptionFailure" => Some(
                    "Verify your IAM permissions include kms:Decrypt for the secret's KMS key"
                        .to_string(),
                ),
                _ => None,
            };

            (code, message, hint)
        }
        None => ("SdkError".to_string(), err.to_string(), None),
    };

    ProviderError {
        provider: "aws".to_string(),
        service: "secretsmanager".to_string(),
        operation: "GetSecretValue".to_string(),
        code,
        message,
        recovery_hint,
        retriable: false,
    }
    .into()
}

/// Parses a locator string into `(secret_id, optional_region)`.
/// Format: `<secret-name-or-arn>` or `<secret-name-or-arn>@<region>`.
fn parse_locator(locator: &str) -> (&str, Option<&str>) {
    match locator.rsplit_once('@') {
        Some((id, region)) if !region.is_empty() && !id.is_empty() => (id, Some(region)),
        _ => (locator, None),
    }
}

#[allow(clippy::result_large_err)]
fn extract_json_field(
    secret_string: &str,
    key: &str,
    locator: &str,
) -> Result<SecretString, DbError> {
    let parsed: serde_json::Value = serde_json::from_str(secret_string).map_err(|err| {
        DbError::ValueResolutionFailed(format!(
            "AWS Secrets Manager: secret '{}' is not valid JSON (json_key '{}' requested): {}",
            locator, key, err
        ))
    })?;

    let field = parsed.get(key).ok_or_else(|| {
        DbError::ValueResolutionFailed(format!(
            "AWS Secrets Manager: field '{}' not found in secret '{}'",
            key, locator
        ))
    })?;

    let value = match field {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    };

    Ok(SecretString::from(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_locator_without_region() {
        let (id, region) = parse_locator("my-secret");
        assert_eq!(id, "my-secret");
        assert!(region.is_none());
    }

    #[test]
    fn parse_locator_with_region() {
        let (id, region) = parse_locator("my-secret@us-east-1");
        assert_eq!(id, "my-secret");
        assert_eq!(region, Some("us-east-1"));
    }

    #[test]
    fn parse_locator_arn_without_region() {
        let arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:my-secret-AbCdEf";
        let (id, region) = parse_locator(arn);
        assert_eq!(id, arn);
        assert!(region.is_none());
    }

    #[test]
    fn parse_locator_arn_with_region_override() {
        let locator = "arn:aws:secretsmanager:us-east-1:123456789012:secret:my-secret@eu-west-1";
        let (id, region) = parse_locator(locator);
        assert_eq!(
            id,
            "arn:aws:secretsmanager:us-east-1:123456789012:secret:my-secret"
        );
        assert_eq!(region, Some("eu-west-1"));
    }

    #[test]
    fn parse_locator_trailing_at_returns_no_region() {
        let (id, region) = parse_locator("my-secret@");
        assert_eq!(id, "my-secret@");
        assert!(region.is_none());
    }

    #[test]
    fn extract_json_field_string_value() {
        let json = r#"{"username":"admin","password":"secret123"}"#;
        let result = extract_json_field(json, "password", "test-locator").unwrap();
        assert_eq!(secrecy::ExposeSecret::expose_secret(&result), "secret123");
    }

    #[test]
    fn extract_json_field_numeric_value() {
        let json = r#"{"port":5432,"host":"db.example.com"}"#;
        let result = extract_json_field(json, "port", "test-locator").unwrap();
        assert_eq!(secrecy::ExposeSecret::expose_secret(&result), "5432");
    }

    #[test]
    fn extract_json_field_missing_key() {
        let json = r#"{"username":"admin"}"#;
        let result = extract_json_field(json, "password", "test-locator");
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("password"));
        assert!(err_msg.contains("not found"));
    }

    #[test]
    fn extract_json_field_invalid_json() {
        let result = extract_json_field("not-json", "key", "test-locator");
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("not valid JSON"));
    }
}
