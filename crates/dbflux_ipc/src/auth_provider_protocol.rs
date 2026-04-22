use std::collections::HashMap;

use dbflux_core::auth::{AuthFormDef, AuthProfile, AuthSession, AuthSessionState, ResolvedCredentials};
use dbflux_core::chrono;
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::envelope::ProtocolVersion;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthProviderHelloRequest {
    pub client_name: String,
    pub client_version: String,
    pub supported_versions: Vec<ProtocolVersion>,
    #[serde(default)]
    pub auth_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthProviderHelloResponse {
    pub server_name: String,
    pub server_version: String,
    pub selected_version: ProtocolVersion,
    pub provider_id: String,
    pub display_name: String,
    pub form_definition: AuthFormDef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateSessionRequest {
    pub profile_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginRequest {
    pub profile_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveCredentialsRequest {
    pub profile_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginUrlProgress {
    pub verification_url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthProviderRpcErrorCode {
    InvalidRequest,
    UnsupportedMethod,
    VersionMismatch,
    Timeout,
    Transport,
    Provider,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthProviderRpcError {
    pub code: AuthProviderRpcErrorCode,
    pub message: String,
    pub retriable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthProviderRequestBody {
    Hello(AuthProviderHelloRequest),
    ValidateSession(ValidateSessionRequest),
    Login(LoginRequest),
    ResolveCredentials(ResolveCredentialsRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthSessionStateDto {
    Valid { expires_at: Option<chrono::DateTime<chrono::Utc>> },
    Expired,
    LoginRequired,
}

impl From<AuthSessionState> for AuthSessionStateDto {
    fn from(value: AuthSessionState) -> Self {
        match value {
            AuthSessionState::Valid { expires_at } => Self::Valid { expires_at },
            AuthSessionState::Expired => Self::Expired,
            AuthSessionState::LoginRequired => Self::LoginRequired,
        }
    }
}

impl From<AuthSessionStateDto> for AuthSessionState {
    fn from(value: AuthSessionStateDto) -> Self {
        match value {
            AuthSessionStateDto::Valid { expires_at } => Self::Valid { expires_at },
            AuthSessionStateDto::Expired => Self::Expired,
            AuthSessionStateDto::LoginRequired => Self::LoginRequired,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthSessionDto {
    pub provider_id: String,
    pub profile_id: Uuid,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<&AuthSession> for AuthSessionDto {
    fn from(value: &AuthSession) -> Self {
        Self {
            provider_id: value.provider_id.clone(),
            profile_id: value.profile_id,
            expires_at: value.expires_at,
        }
    }
}

impl From<AuthSessionDto> for AuthSession {
    fn from(value: AuthSessionDto) -> Self {
        Self {
            provider_id: value.provider_id,
            profile_id: value.profile_id,
            expires_at: value.expires_at,
            data: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedCredentialsDto {
    pub fields: HashMap<String, String>,
    pub secret_fields: HashMap<String, String>,
}

impl From<&ResolvedCredentials> for ResolvedCredentialsDto {
    fn from(value: &ResolvedCredentials) -> Self {
        Self {
            fields: value.fields.clone(),
            secret_fields: value
                .secret_fields
                .iter()
                .map(|(key, value)| (key.clone(), value.expose_secret().to_string()))
                .collect(),
        }
    }
}

impl From<ResolvedCredentialsDto> for ResolvedCredentials {
    fn from(value: ResolvedCredentialsDto) -> Self {
        Self {
            fields: value.fields,
            secret_fields: value
                .secret_fields
                .into_iter()
                .map(|(key, value)| (key, SecretString::from(value)))
                .collect(),
            provider_data: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthProviderResponseBody {
    Hello(AuthProviderHelloResponse),
    SessionState { state: AuthSessionStateDto },
    LoginUrlProgress(LoginUrlProgress),
    LoginResult { session: AuthSessionDto },
    Credentials { credentials: ResolvedCredentialsDto },
    Error(AuthProviderRpcError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthProviderRequestEnvelope {
    pub protocol_version: ProtocolVersion,
    pub request_id: u64,
    pub body: AuthProviderRequestBody,
}

impl AuthProviderRequestEnvelope {
    pub fn new(protocol_version: ProtocolVersion, request_id: u64, body: AuthProviderRequestBody) -> Self {
        Self {
            protocol_version,
            request_id,
            body,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthProviderResponseEnvelope {
    pub protocol_version: ProtocolVersion,
    pub request_id: u64,
    pub done: bool,
    pub body: AuthProviderResponseBody,
}

impl AuthProviderResponseEnvelope {
    pub fn ok(protocol_version: ProtocolVersion, request_id: u64, body: AuthProviderResponseBody) -> Self {
        Self {
            protocol_version,
            request_id,
            done: true,
            body,
        }
    }

    pub fn login_url_progress(
        protocol_version: ProtocolVersion,
        request_id: u64,
        verification_url: Option<String>,
    ) -> Self {
        Self {
            protocol_version,
            request_id,
            done: false,
            body: AuthProviderResponseBody::LoginUrlProgress(LoginUrlProgress { verification_url }),
        }
    }

    pub fn error(
        protocol_version: ProtocolVersion,
        request_id: u64,
        code: AuthProviderRpcErrorCode,
        message: impl Into<String>,
        retriable: bool,
    ) -> Self {
        Self {
            protocol_version,
            request_id,
            done: true,
            body: AuthProviderResponseBody::Error(AuthProviderRpcError {
                code,
                message: message.into(),
                retriable,
            }),
        }
    }
}

impl AuthProviderRpcError {
    pub fn into_db_error(self) -> dbflux_core::DbError {
        match self.code {
            AuthProviderRpcErrorCode::UnsupportedMethod => dbflux_core::DbError::NotSupported(self.message),
            AuthProviderRpcErrorCode::Timeout => dbflux_core::DbError::Timeout,
            AuthProviderRpcErrorCode::Transport | AuthProviderRpcErrorCode::VersionMismatch => {
                dbflux_core::DbError::connection_failed(self.message)
            }
            AuthProviderRpcErrorCode::InvalidRequest
            | AuthProviderRpcErrorCode::Provider
            | AuthProviderRpcErrorCode::Internal => dbflux_core::DbError::QueryFailed(self.message.into()),
        }
    }
}

pub fn parse_auth_profile(profile_json: &str) -> Result<AuthProfile, AuthProviderRpcError> {
    serde_json::from_str(profile_json).map_err(|error| AuthProviderRpcError {
        code: AuthProviderRpcErrorCode::InvalidRequest,
        message: format!("invalid auth profile payload: {error}"),
        retriable: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::secrecy::ExposeSecret;

    #[test]
    fn auth_provider_error_mapping_preserves_transport_timeout_and_unsupported_method() {
        let transport = AuthProviderRpcError {
            code: AuthProviderRpcErrorCode::Transport,
            message: "socket unavailable".to_string(),
            retriable: false,
        }
        .into_db_error();

        let timeout = AuthProviderRpcError {
            code: AuthProviderRpcErrorCode::Timeout,
            message: "too slow".to_string(),
            retriable: true,
        }
        .into_db_error();

        let unsupported = AuthProviderRpcError {
            code: AuthProviderRpcErrorCode::UnsupportedMethod,
            message: "login not implemented".to_string(),
            retriable: false,
        }
        .into_db_error();

        assert!(matches!(transport, dbflux_core::DbError::ConnectionFailed(_)));
        assert!(matches!(timeout, dbflux_core::DbError::Timeout));
        assert!(matches!(unsupported, dbflux_core::DbError::NotSupported(_)));
    }

    #[test]
    fn resolved_credentials_dto_round_trips_without_provider_data() {
        let mut credentials = ResolvedCredentials::default();
        credentials.fields.insert("region".to_string(), "us-east-1".to_string());
        credentials.secret_fields.insert(
            "token".to_string(),
            SecretString::from("secret-token".to_string()),
        );

        let dto = ResolvedCredentialsDto::from(&credentials);
        let restored = ResolvedCredentials::from(dto);

        assert_eq!(restored.fields.get("region").map(String::as_str), Some("us-east-1"));
        assert_eq!(
            restored.secret_fields.get("token").map(|value| value.expose_secret()),
            Some("secret-token")
        );
        assert!(restored.provider_data.is_none());
    }

    #[test]
    fn login_progress_responses_stay_open_until_terminal_message() {
        let progress = AuthProviderResponseEnvelope::login_url_progress(
            ProtocolVersion::new(1, 0),
            7,
            Some("https://verify.example".to_string()),
        );

        assert!(!progress.done);

        let AuthProviderResponseBody::LoginUrlProgress(progress_body) = progress.body else {
            panic!("expected login progress response");
        };

        assert_eq!(progress_body.verification_url.as_deref(), Some("https://verify.example"));
    }
}
