use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use serde::de::Deserializer;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `AuthProfile` uses a custom `Deserialize` impl to handle migration from
/// the legacy `"config"` format (see impl below). `Serialize` is derived
/// normally and always writes the new `"fields"` format.
#[derive(Debug, Clone, Serialize)]
pub struct AuthProfile {
    pub id: Uuid,
    pub name: String,
    pub provider_id: String,
    pub fields: HashMap<String, String>,
    pub enabled: bool,
}

/// Provider ID rewrites for old `config.type` values.
fn rewrite_provider_id(old_provider_id: &str, config_type: &str) -> String {
    match config_type {
        "aws_sso" => "aws-sso".to_string(),
        "aws_shared_credentials" => "aws-shared-credentials".to_string(),
        "aws_static_credentials" => "aws-static-credentials".to_string(),
        _ => old_provider_id.to_string(),
    }
}

impl<'de> Deserialize<'de> for AuthProfile {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        let mut value = serde_json::Value::deserialize(deserializer)?;

        let obj = value
            .as_object_mut()
            .ok_or_else(|| D::Error::custom("AuthProfile must be a JSON object"))?;

        let id: Uuid = obj
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| D::Error::custom("missing field 'id'"))
            .and_then(|s| Uuid::parse_str(s).map_err(|e| D::Error::custom(e.to_string())))?;

        let name = obj
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| D::Error::custom("missing field 'name'"))?
            .to_string();

        let enabled = obj.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true);

        // Determine fields and provider_id, handling both new and old formats.
        let (fields, provider_id) = if let Some(fields_val) = obj.get("fields") {
            // New format: "fields" key is already a flat map.
            let fields: HashMap<String, String> =
                serde_json::from_value(fields_val.clone()).map_err(D::Error::custom)?;

            let provider_id = obj
                .get("provider_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            (fields, provider_id)
        } else if let Some(config_val) = obj.get("config") {
            // Old format: "config" is an object with a "type" discriminant.
            let config_obj = config_val
                .as_object()
                .ok_or_else(|| D::Error::custom("'config' must be a JSON object"))?;

            let config_type = config_obj
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let old_provider_id = obj
                .get("provider_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let provider_id = rewrite_provider_id(old_provider_id, config_type);

            // Flatten config object into fields, dropping the "type" key.
            let mut fields = HashMap::new();
            for (key, val) in config_obj {
                if key == "type" {
                    continue;
                }
                let field_str = match val {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                fields.insert(key.clone(), field_str);
            }

            (fields, provider_id)
        } else {
            // No fields and no config: empty fields, read provider_id as-is.
            let provider_id = obj
                .get("provider_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            (HashMap::new(), provider_id)
        };

        Ok(AuthProfile {
            id,
            name,
            provider_id,
            fields,
            enabled,
        })
    }
}

impl AuthProfile {
    pub fn new(
        name: impl Into<String>,
        provider_id: impl Into<String>,
        fields: HashMap<String, String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            provider_id: provider_id.into(),
            fields,
            enabled: true,
        }
    }

    pub fn secret_ref(&self) -> String {
        format!("dbflux:auth:{}", self.id)
    }
}

#[derive(Debug, Clone)]
pub struct AuthProfileSummary {
    pub id: Uuid,
    pub name: String,
    pub provider_id: String,
}

impl From<&AuthProfile> for AuthProfileSummary {
    fn from(profile: &AuthProfile) -> Self {
        Self {
            id: profile.id,
            name: profile.name.clone(),
            provider_id: profile.provider_id.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum AuthSessionState {
    Valid {
        expires_at: Option<chrono::DateTime<chrono::Utc>>,
    },
    Expired,
    LoginRequired,
}

#[derive(Clone)]
pub struct AuthSession {
    pub provider_id: String,
    pub profile_id: Uuid,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,

    /// Provider-specific opaque data (e.g., AWS `SdkConfig`) that
    /// downstream components (secret/parameter providers) can downcast.
    pub data: Option<Arc<dyn Any + Send + Sync>>,
}

impl std::fmt::Debug for AuthSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthSession")
            .field("provider_id", &self.provider_id)
            .field("profile_id", &self.profile_id)
            .field("expires_at", &self.expires_at)
            .field("data", &self.data.as_ref().map(|_| "<opaque>"))
            .finish()
    }
}

#[derive(Default)]
pub struct ResolvedCredentials {
    /// Plain-text credential fields (e.g. `"region"`, `"access_key_id"`).
    pub fields: HashMap<String, String>,

    /// Secret credential fields — values are redacted in logs.
    /// Keys use the same naming convention as `fields`
    /// (e.g. `"secret_access_key"`, `"session_token"`).
    pub secret_fields: HashMap<String, secrecy::SecretString>,

    /// Provider-specific opaque data (e.g., AWS `SdkConfig`) that
    /// downstream value providers can downcast to build their clients.
    pub provider_data: Option<Arc<dyn Any + Send + Sync>>,
}

impl std::fmt::Debug for ResolvedCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Print secret_fields key names only — never expose the values.
        let secret_keys: Vec<&str> = self.secret_fields.keys().map(String::as_str).collect();

        f.debug_struct("ResolvedCredentials")
            .field("fields", &self.fields)
            .field("secret_fields", &secret_keys)
            .field(
                "provider_data",
                &self.provider_data.as_ref().map(|_| "<opaque>"),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_uuid() -> String {
        "00000000-0000-0000-0000-000000000001".to_string()
    }

    #[test]
    fn old_aws_sso_config_migrates_correctly() {
        let json = serde_json::json!({
            "id": make_uuid(),
            "name": "My SSO",
            "provider_id": "aws",
            "config": {
                "type": "aws_sso",
                "profile_name": "dev",
                "region": "us-east-1",
                "sso_start_url": "https://example.awsapps.com/start",
                "sso_account_id": "123456789012",
                "sso_role_name": "DevRole"
            },
            "enabled": true
        });

        let profile: AuthProfile = serde_json::from_value(json).unwrap();

        assert_eq!(profile.provider_id, "aws-sso");
        assert_eq!(profile.fields["profile_name"], "dev");
        assert_eq!(profile.fields["region"], "us-east-1");
        assert_eq!(
            profile.fields["sso_start_url"],
            "https://example.awsapps.com/start"
        );
        assert_eq!(profile.fields["sso_account_id"], "123456789012");
        assert_eq!(profile.fields["sso_role_name"], "DevRole");
        assert!(!profile.fields.contains_key("type"));
    }

    #[test]
    fn old_aws_shared_credentials_migrates() {
        let json = serde_json::json!({
            "id": make_uuid(),
            "name": "Shared",
            "provider_id": "aws",
            "config": {
                "type": "aws_shared_credentials",
                "profile_name": "default",
                "region": "eu-west-1"
            },
            "enabled": true
        });

        let profile: AuthProfile = serde_json::from_value(json).unwrap();

        assert_eq!(profile.provider_id, "aws-shared-credentials");
        assert_eq!(profile.fields["profile_name"], "default");
        assert_eq!(profile.fields["region"], "eu-west-1");
    }

    #[test]
    fn old_aws_static_credentials_migrates() {
        let json = serde_json::json!({
            "id": make_uuid(),
            "name": "Static",
            "provider_id": "aws",
            "config": {
                "type": "aws_static_credentials",
                "region": "ap-southeast-1"
            },
            "enabled": true
        });

        let profile: AuthProfile = serde_json::from_value(json).unwrap();

        assert_eq!(profile.provider_id, "aws-static-credentials");
        assert_eq!(profile.fields["region"], "ap-southeast-1");
    }

    #[test]
    fn new_format_roundtrip() {
        let mut fields = HashMap::new();
        fields.insert("profile_name".to_string(), "prod".to_string());
        fields.insert("region".to_string(), "us-west-2".to_string());

        let original = AuthProfile {
            id: Uuid::parse_str(&make_uuid()).unwrap(),
            name: "Prod".to_string(),
            provider_id: "aws-sso".to_string(),
            fields: fields.clone(),
            enabled: true,
        };

        let serialized = serde_json::to_value(&original).unwrap();
        let deserialized: AuthProfile = serde_json::from_value(serialized).unwrap();

        assert_eq!(deserialized.id, original.id);
        assert_eq!(deserialized.name, original.name);
        assert_eq!(deserialized.provider_id, original.provider_id);
        assert_eq!(deserialized.fields, original.fields);
        assert_eq!(deserialized.enabled, original.enabled);
    }

    #[test]
    fn old_provider_id_aws_is_rewritten() {
        let json = serde_json::json!({
            "id": make_uuid(),
            "name": "Test",
            "provider_id": "aws",
            "config": {
                "type": "aws_sso",
                "profile_name": "test",
                "region": "us-east-1",
                "sso_start_url": "https://x.awsapps.com/start",
                "sso_account_id": "000000000000",
                "sso_role_name": "TestRole"
            },
            "enabled": true
        });

        let profile: AuthProfile = serde_json::from_value(json).unwrap();
        assert_eq!(profile.provider_id, "aws-sso");
    }
}
