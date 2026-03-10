mod cache;
mod resolver;

use std::fmt;
use std::future::Future;

use secrecy::ExposeSecret;
use serde::{Deserialize, Deserializer, Serialize};

use crate::DbError;
use crate::auth::ResolvedCredentials;

pub use cache::{CacheKey as ValueCacheKey, CachedValue, ValueCache};
pub use resolver::CompositeValueResolver;

/// Structured error from a value provider (Secrets Manager, SSM, etc.)
/// with enough context to display actionable diagnostics in the UI.
#[derive(Debug, Clone)]
pub struct ProviderError {
    pub provider: String,
    pub service: String,
    pub operation: String,
    pub code: String,
    pub message: String,
    pub recovery_hint: Option<String>,
    pub retriable: bool,
}

impl fmt::Display for ProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}/{}] {} ({}): {}",
            self.provider, self.service, self.operation, self.code, self.message
        )?;

        if let Some(hint) = &self.recovery_hint {
            write!(f, " — {}", hint)?;
        }

        Ok(())
    }
}

impl From<ProviderError> for DbError {
    fn from(err: ProviderError) -> Self {
        DbError::ValueResolutionFailed(err.to_string())
    }
}

pub trait SecretProvider: Send + Sync {
    fn provider_id(&self) -> &'static str;

    fn display_name(&self) -> &'static str;

    fn get_secret(
        &self,
        locator: &str,
        json_key: Option<&str>,
    ) -> impl Future<Output = Result<secrecy::SecretString, DbError>> + Send;
}

pub trait ParameterProvider: Send + Sync {
    fn provider_id(&self) -> &'static str;

    fn display_name(&self) -> &'static str;

    fn get_parameter(
        &self,
        name: &str,
        json_key: Option<&str>,
    ) -> impl Future<Output = Result<String, DbError>> + Send;
}

#[async_trait::async_trait]
pub trait DynSecretProvider: Send + Sync {
    fn provider_id(&self) -> &'static str;

    fn display_name(&self) -> &'static str;

    async fn get_secret(
        &self,
        locator: &str,
        json_key: Option<&str>,
    ) -> Result<secrecy::SecretString, DbError>;
}

#[async_trait::async_trait]
impl<T: SecretProvider> DynSecretProvider for T {
    fn provider_id(&self) -> &'static str {
        SecretProvider::provider_id(self)
    }

    fn display_name(&self) -> &'static str {
        SecretProvider::display_name(self)
    }

    async fn get_secret(
        &self,
        locator: &str,
        json_key: Option<&str>,
    ) -> Result<secrecy::SecretString, DbError> {
        SecretProvider::get_secret(self, locator, json_key).await
    }
}

#[async_trait::async_trait]
pub trait DynParameterProvider: Send + Sync {
    fn provider_id(&self) -> &'static str;

    fn display_name(&self) -> &'static str;

    async fn get_parameter(&self, name: &str, json_key: Option<&str>) -> Result<String, DbError>;
}

#[async_trait::async_trait]
impl<T: ParameterProvider> DynParameterProvider for T {
    fn provider_id(&self) -> &'static str {
        ParameterProvider::provider_id(self)
    }

    fn display_name(&self) -> &'static str {
        ParameterProvider::display_name(self)
    }

    async fn get_parameter(&self, name: &str, json_key: Option<&str>) -> Result<String, DbError> {
        ParameterProvider::get_parameter(self, name, json_key).await
    }
}

#[derive(Debug, Default)]
pub struct ResolveContext<'a> {
    pub credentials: Option<&'a ResolvedCredentials>,
    pub auth_session: Option<&'a crate::auth::AuthSession>,
    pub profile_name: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum ValueRef {
    Literal {
        value: String,
    },
    Env {
        key: String,
    },
    Secret {
        provider: String,
        locator: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        json_key: Option<String>,
    },
    Parameter {
        provider: String,
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        json_key: Option<String>,
    },
    Auth {
        field: String,
    },
}

impl ValueRef {
    pub fn literal(value: impl Into<String>) -> Self {
        Self::Literal {
            value: value.into(),
        }
    }

    pub fn env(key: impl Into<String>) -> Self {
        Self::Env { key: key.into() }
    }

    pub fn secret(
        provider: impl Into<String>,
        locator: impl Into<String>,
        json_key: Option<String>,
    ) -> Self {
        Self::Secret {
            provider: provider.into(),
            locator: locator.into(),
            json_key,
        }
    }

    pub fn parameter(provider: impl Into<String>, name: impl Into<String>) -> Self {
        Self::Parameter {
            provider: provider.into(),
            name: name.into(),
            json_key: None,
        }
    }

    pub fn parameter_with_key(
        provider: impl Into<String>,
        name: impl Into<String>,
        json_key: Option<String>,
    ) -> Self {
        Self::Parameter {
            provider: provider.into(),
            name: name.into(),
            json_key,
        }
    }

    pub fn auth(field: impl Into<String>) -> Self {
        Self::Auth {
            field: field.into(),
        }
    }

    pub fn try_literal(&self) -> Option<&str> {
        match self {
            Self::Literal { value } => Some(value),
            _ => None,
        }
    }

    pub fn needs_resolution(&self) -> bool {
        !matches!(self, Self::Literal { .. })
    }
}

impl Default for ValueRef {
    fn default() -> Self {
        Self::Literal {
            value: String::new(),
        }
    }
}

/// Backward-compatible wrapper that deserializes bare JSON strings as
/// `ValueRef::Literal` while still accepting the full tagged-object form.
#[derive(Debug, Clone, Serialize)]
pub struct FieldValue(pub ValueRef);

impl<'de> Deserialize<'de> for FieldValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = serde_json::Value::deserialize(deserializer)?;

        match &raw {
            serde_json::Value::String(s) => Ok(FieldValue(ValueRef::Literal { value: s.clone() })),
            serde_json::Value::Object(_) => {
                let value_ref: ValueRef =
                    serde_json::from_value(raw).map_err(serde::de::Error::custom)?;
                Ok(FieldValue(value_ref))
            }
            other => Err(serde::de::Error::custom(format!(
                "expected string or object for FieldValue, got {}",
                value_type_name(other),
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ValueOrigin {
    Literal,
    EnvVar {
        name: String,
    },
    SecretProvider {
        provider: String,
        locator_summary: String,
    },
    ParameterProvider {
        provider: String,
        name: String,
    },
    AuthCredential {
        field: String,
    },
}

#[derive(Clone)]
pub struct ResolvedValue {
    value: secrecy::SecretString,
    origin: ValueOrigin,
}

impl ResolvedValue {
    pub fn new(value: impl Into<String>, origin: ValueOrigin) -> Self {
        Self {
            value: secrecy::SecretString::from(value.into()),
            origin,
        }
    }

    pub fn expose_secret(&self) -> &str {
        self.value.expose_secret()
    }

    pub fn origin(&self) -> &ValueOrigin {
        &self.origin
    }

    pub fn into_secret(self) -> secrecy::SecretString {
        self.value
    }
}

impl fmt::Debug for ResolvedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedValue")
            .field("value", &"[REDACTED]")
            .field("origin", &self.origin)
            .finish()
    }
}

fn value_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_value_from_bare_string() {
        let fv: FieldValue = serde_json::from_str(r#""hello""#).expect("deserialize string");
        assert_eq!(fv.0.try_literal(), Some("hello"));
        assert!(!fv.0.needs_resolution());
    }

    #[test]
    fn field_value_from_tagged_object() {
        let json = r#"{"source":"env","key":"DB_HOST"}"#;
        let fv: FieldValue = serde_json::from_str(json).expect("deserialize object");
        assert!(fv.0.needs_resolution());
        match &fv.0 {
            ValueRef::Env { key } => assert_eq!(key, "DB_HOST"),
            other => panic!("expected Env, got {:?}", other),
        }
    }

    #[test]
    fn value_ref_default_is_empty_literal() {
        let v = ValueRef::default();
        assert_eq!(v.try_literal(), Some(""));
        assert!(!v.needs_resolution());
    }

    #[test]
    fn provider_error_display_includes_all_fields() {
        let err = ProviderError {
            provider: "aws".to_string(),
            service: "secretsmanager".to_string(),
            operation: "GetSecretValue".to_string(),
            code: "ResourceNotFoundException".to_string(),
            message: "Secret not found".to_string(),
            recovery_hint: Some("Check the secret name or ARN".to_string()),
            retriable: false,
        };

        let display = err.to_string();
        assert!(display.contains("aws"));
        assert!(display.contains("secretsmanager"));
        assert!(display.contains("GetSecretValue"));
        assert!(display.contains("ResourceNotFoundException"));
        assert!(display.contains("Secret not found"));
        assert!(display.contains("Check the secret name or ARN"));
    }

    #[test]
    fn provider_error_converts_to_db_error() {
        let err = ProviderError {
            provider: "aws".to_string(),
            service: "ssm".to_string(),
            operation: "GetParameter".to_string(),
            code: "ParameterNotFound".to_string(),
            message: "Parameter does not exist".to_string(),
            recovery_hint: None,
            retriable: false,
        };

        let db_err: DbError = err.into();
        match db_err {
            DbError::ValueResolutionFailed(msg) => {
                assert!(msg.contains("ParameterNotFound"));
            }
            other => panic!("expected ValueResolutionFailed, got {:?}", other),
        }
    }

    #[test]
    fn provider_error_display_without_hint() {
        let err = ProviderError {
            provider: "aws".to_string(),
            service: "ssm".to_string(),
            operation: "GetParameter".to_string(),
            code: "InternalError".to_string(),
            message: "Internal server error".to_string(),
            recovery_hint: None,
            retriable: true,
        };

        let display = err.to_string();
        assert!(!display.contains("—"));
    }
}
