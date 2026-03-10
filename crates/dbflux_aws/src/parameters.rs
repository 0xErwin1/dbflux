/// AWS SSM Parameter Store provider implementing `ParameterProvider`.
///
/// Resolves parameter values from AWS Systems Manager Parameter Store.
/// SecureString parameters are automatically decrypted.
use aws_config::SdkConfig;

use dbflux_core::DbError;
use dbflux_core::values::ProviderError;

pub struct AwsSsmParameterProvider {
    sdk_config: SdkConfig,
}

impl AwsSsmParameterProvider {
    pub fn new(sdk_config: SdkConfig) -> Self {
        Self { sdk_config }
    }
}

impl dbflux_core::values::ParameterProvider for AwsSsmParameterProvider {
    fn provider_id(&self) -> &'static str {
        "aws-ssm"
    }

    fn display_name(&self) -> &'static str {
        "AWS SSM Parameter Store"
    }

    fn get_parameter(
        &self,
        name: &str,
        json_key: Option<&str>,
    ) -> impl std::future::Future<Output = Result<String, DbError>> + Send {
        let sdk_config = self.sdk_config.clone();
        let name = name.to_string();
        let json_key = json_key.map(ToString::to_string);

        async move {
            let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);

            std::thread::spawn(move || {
                let result = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                {
                    Err(err) => Err(DbError::ValueResolutionFailed(format!(
                        "Failed to create Tokio runtime for SSM Parameter Store: {}",
                        err
                    ))),
                    Ok(rt) => {
                        rt.block_on(get_parameter_inner(&sdk_config, &name, json_key.as_deref()))
                    }
                };

                let _ = result_tx.send(result);
            });

            // Non-blocking poll — yields to the executor between checks so GPUI
            // can continue processing events while the parameter is being fetched.
            loop {
                match result_rx.try_recv() {
                    Ok(result) => return result,
                    Err(std::sync::mpsc::TryRecvError::Empty) => {
                        futures_lite::future::yield_now().await;
                    }
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        return Err(DbError::ValueResolutionFailed(
                            "SSM Parameter Store thread terminated unexpectedly".to_string(),
                        ));
                    }
                }
            }
        }
    }
}

/// Inner async implementation of parameter fetching, called from within a
/// dedicated Tokio runtime. Separated so the outer `get_parameter` can safely
/// dispatch this through `std::thread::spawn` from any async executor.
async fn get_parameter_inner(
    sdk_config: &SdkConfig,
    name: &str,
    json_key: Option<&str>,
) -> Result<String, DbError> {
    let client = aws_sdk_ssm::Client::new(sdk_config);

    let output = client
        .get_parameter()
        .name(name)
        .with_decryption(true)
        .send()
        .await
        .map_err(|err| map_ssm_error(err, name))?;

    let parameter = output.parameter().ok_or_else(|| {
        DbError::ValueResolutionFailed(format!(
            "AWS SSM Parameter Store: parameter '{}' returned no data",
            name
        ))
    })?;

    let value = parameter.value().ok_or_else(|| {
        DbError::ValueResolutionFailed(format!(
            "AWS SSM Parameter Store: parameter '{}' has no value",
            name
        ))
    })?;

    match json_key {
        Some(key) => extract_json_field(value, key, name),
        None => Ok(value.to_string()),
    }
}

#[allow(clippy::result_large_err)]
fn extract_json_field(parameter_value: &str, key: &str, name: &str) -> Result<String, DbError> {
    let parsed: serde_json::Value = serde_json::from_str(parameter_value).map_err(|err| {
        DbError::ValueResolutionFailed(format!(
            "AWS SSM Parameter Store: parameter '{}' is not valid JSON (json_key '{}' requested): {}",
            name, key, err
        ))
    })?;

    let field = parsed.get(key).ok_or_else(|| {
        DbError::ValueResolutionFailed(format!(
            "AWS SSM Parameter Store: field '{}' not found in parameter '{}'",
            key, name
        ))
    })?;

    let value = match field {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    };

    Ok(value)
}

fn map_ssm_error(
    err: aws_sdk_ssm::error::SdkError<aws_sdk_ssm::operation::get_parameter::GetParameterError>,
    name: &str,
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
                "ParameterNotFound" => Some(format!(
                    "Check that parameter '{}' exists and the region is correct",
                    name
                )),
                "AccessDeniedException" => {
                    Some("Verify your IAM permissions include ssm:GetParameter".to_string())
                }
                "InvalidKeyId" => Some(
                    "Verify your IAM permissions include kms:Decrypt for the parameter's KMS key"
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
        service: "ssm".to_string(),
        operation: "GetParameter".to_string(),
        code,
        message,
        recovery_hint,
        retriable: false,
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::extract_json_field;

    #[test]
    fn extract_json_field_string_value() {
        let json = r#"{"region":"us-east-1","port":5432}"#;
        let result = extract_json_field(json, "region", "/db/config").unwrap();
        assert_eq!(result, "us-east-1");
    }

    #[test]
    fn extract_json_field_numeric_value() {
        let json = r#"{"region":"us-east-1","port":5432}"#;
        let result = extract_json_field(json, "port", "/db/config").unwrap();
        assert_eq!(result, "5432");
    }

    #[test]
    fn extract_json_field_missing_key() {
        let json = r#"{"region":"us-east-1"}"#;
        let result = extract_json_field(json, "port", "/db/config");
        assert!(result.is_err());
    }
}
