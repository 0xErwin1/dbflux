use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthProfileConfig {
    AwsSso {
        profile_name: String,
        region: String,
        sso_start_url: String,
        sso_account_id: String,
        sso_role_name: String,
    },
    AwsSharedCredentials {
        profile_name: String,
        region: String,
    },
    AwsStaticCredentials {
        region: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthProfile {
    pub id: Uuid,
    pub name: String,
    pub provider_id: String,
    pub config: AuthProfileConfig,

    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl AuthProfile {
    pub fn new(
        name: impl Into<String>,
        provider_id: impl Into<String>,
        config: AuthProfileConfig,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            provider_id: provider_id.into(),
            config,
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
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<secrecy::SecretString>,
    pub session_token: Option<secrecy::SecretString>,
    pub region: Option<String>,
    pub extra: HashMap<String, String>,

    /// Provider-specific opaque data (e.g., AWS `SdkConfig`) that
    /// downstream value providers can downcast to build their clients.
    pub provider_data: Option<Arc<dyn Any + Send + Sync>>,
}

impl std::fmt::Debug for ResolvedCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedCredentials")
            .field("access_key_id", &self.access_key_id)
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("region", &self.region)
            .field("extra", &self.extra)
            .field(
                "provider_data",
                &self.provider_data.as_ref().map(|_| "<opaque>"),
            )
            .finish()
    }
}
