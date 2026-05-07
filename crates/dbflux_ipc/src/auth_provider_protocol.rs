use std::collections::HashMap;

use dbflux_core::auth::{
    AuthFormDef, AuthProfile, AuthProviderCapabilities, AuthSession, AuthSessionState,
    ResolvedCredentials,
};
use dbflux_core::chrono;
use dbflux_core::SelectOption;
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
pub struct AuthProviderHelloResponseV1_1 {
    pub server_name: String,
    pub server_version: String,
    pub selected_version: ProtocolVersion,
    pub provider_id: String,
    pub display_name: String,
    pub form_definition: AuthFormDef,
    pub capabilities: AuthProviderCapabilities,
}

/// Hello response for protocol v1.2.
///
/// Adds `secret_dependency_opt_in` which declares whether the provider opts in
/// to receiving `Password`-kind field values in `FetchFieldOptions` requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthProviderHelloResponseV1_2 {
    pub server_name: String,
    pub server_version: String,
    pub selected_version: ProtocolVersion,
    pub provider_id: String,
    pub display_name: String,
    pub form_definition: AuthFormDef,
    pub capabilities: AuthProviderCapabilities,
    /// When `true`, the host MAY include values for `Password`-kind fields in
    /// `FetchFieldOptions` requests, provided those fields appear in the
    /// target field's `depends_on` list.  Defaults to `false`.
    #[serde(default)]
    pub secret_dependency_opt_in: bool,
}

/// Request to fetch the available options for a `DynamicSelect` field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchFieldOptionsRequest {
    /// JSON-serialized `AuthProfile` (with secret fields stripped by the host
    /// unless `secret_dependency_opt_in` is `true` for the relevant fields).
    pub profile_json: String,
    /// ID of the `DynamicSelect` field whose options are being requested.
    pub field_id: String,
    /// Current values of fields listed in the target field's `depends_on`,
    /// after secret stripping enforced by the host.
    pub dependencies: HashMap<String, String>,
    /// Serialized `AuthSession.data` value, if a session is active.
    pub session: Option<serde_json::Value>,
}

/// Successful response to a `FetchFieldOptions` request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchFieldOptionsResponse {
    /// Options to populate the dropdown.
    pub options: Vec<SelectOption>,
    /// How long (in seconds) the host may cache these options.
    ///
    /// `None` means do not cache.
    pub cache_hint_seconds: Option<u32>,
}

/// Error returned when a `FetchFieldOptions` request fails.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FetchFieldOptionsError {
    /// The user has never logged in — no session exists.
    NeedsLogin,
    /// A session existed but is no longer valid.
    SessionExpired,
    /// Retry-eligible failure (network error, 5xx, etc.).
    Transient(String),
    /// Non-retriable failure (misconfiguration, 4xx, etc.).
    Permanent(String),
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
    /// Fetch the available options for a `DynamicSelect` field (protocol v1.2+).
    FetchDynamicOptions(FetchFieldOptionsRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthSessionStateDto {
    Valid {
        expires_at: Option<chrono::DateTime<chrono::Utc>>,
    },
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
    /// Opaque session data for providers that need to pass state (e.g. SSO
    /// access tokens) to `FetchFieldOptions` without re-authenticating.
    ///
    /// Populated only when `AuthSession.data` downcasts to
    /// `Arc<serde_json::Value>`.  AWS `Arc<SdkConfig>` is not JSON-serializable
    /// and maps to `None`.  Never persisted to disk or keyring.
    #[serde(default)]
    pub session_data: Option<serde_json::Value>,
}

impl From<&AuthSession> for AuthSessionDto {
    fn from(value: &AuthSession) -> Self {
        // Attempt to downcast the opaque data to Arc<serde_json::Value>.
        // Providers that store non-JSON data (e.g. Arc<SdkConfig>) will get None
        // because we can only clone-downcast through a new Arc.
        let session_data = value.data.as_ref().and_then(|arc| {
            arc.clone()
                .downcast::<serde_json::Value>()
                .ok()
                .map(|typed_arc| (*typed_arc).clone())
        });

        Self {
            provider_id: value.provider_id.clone(),
            profile_id: value.profile_id,
            expires_at: value.expires_at,
            session_data,
        }
    }
}

impl From<AuthSessionDto> for AuthSession {
    fn from(value: AuthSessionDto) -> Self {
        use std::sync::Arc;

        // Restore session data as Arc<serde_json::Value> so callers can
        // downcast it back when needed.
        let data = value
            .session_data
            .map(|json_val| Arc::new(json_val) as Arc<dyn std::any::Any + Send + Sync>);

        Self {
            provider_id: value.provider_id,
            profile_id: value.profile_id,
            expires_at: value.expires_at,
            data,
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
    HelloV1_1(AuthProviderHelloResponseV1_1),
    /// Hello response for protocol v1.2, carrying `secret_dependency_opt_in`.
    HelloV1_2(AuthProviderHelloResponseV1_2),
    SessionState { state: AuthSessionStateDto },
    LoginUrlProgress(LoginUrlProgress),
    LoginResult { session: AuthSessionDto },
    Credentials { credentials: ResolvedCredentialsDto },
    /// Successful response to `FetchDynamicOptions` (protocol v1.2+).
    DynamicOptions(FetchFieldOptionsResponse),
    Error(AuthProviderRpcError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthProviderRequestEnvelope {
    pub protocol_version: ProtocolVersion,
    pub request_id: u64,
    pub body: AuthProviderRequestBody,
}

impl AuthProviderRequestEnvelope {
    pub fn new(
        protocol_version: ProtocolVersion,
        request_id: u64,
        body: AuthProviderRequestBody,
    ) -> Self {
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
    pub fn ok(
        protocol_version: ProtocolVersion,
        request_id: u64,
        body: AuthProviderResponseBody,
    ) -> Self {
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
            AuthProviderRpcErrorCode::UnsupportedMethod => {
                dbflux_core::DbError::NotSupported(self.message)
            }
            AuthProviderRpcErrorCode::Timeout => dbflux_core::DbError::Timeout,
            AuthProviderRpcErrorCode::Transport | AuthProviderRpcErrorCode::VersionMismatch => {
                dbflux_core::DbError::connection_failed(self.message)
            }
            AuthProviderRpcErrorCode::InvalidRequest
            | AuthProviderRpcErrorCode::Provider
            | AuthProviderRpcErrorCode::Internal => {
                dbflux_core::DbError::QueryFailed(self.message.into())
            }
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
    use dbflux_core::auth::{AuthProviderCapabilities, AuthProviderLoginCapabilities};
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

        assert!(matches!(
            transport,
            dbflux_core::DbError::ConnectionFailed(_)
        ));
        assert!(matches!(timeout, dbflux_core::DbError::Timeout));
        assert!(matches!(unsupported, dbflux_core::DbError::NotSupported(_)));
    }

    #[test]
    fn resolved_credentials_dto_round_trips_without_provider_data() {
        let mut credentials = ResolvedCredentials::default();
        credentials
            .fields
            .insert("region".to_string(), "us-east-1".to_string());
        credentials.secret_fields.insert(
            "token".to_string(),
            SecretString::from("secret-token".to_string()),
        );

        let dto = ResolvedCredentialsDto::from(&credentials);
        let restored = ResolvedCredentials::from(dto);

        assert_eq!(
            restored.fields.get("region").map(String::as_str),
            Some("us-east-1")
        );
        assert_eq!(
            restored
                .secret_fields
                .get("token")
                .map(|value| value.expose_secret()),
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

        assert_eq!(
            progress_body.verification_url.as_deref(),
            Some("https://verify.example")
        );
    }

    #[test]
    fn auth_provider_hello_v1_1_preserves_capabilities() {
        let response = AuthProviderHelloResponseV1_1 {
            server_name: "fake-auth-provider".to_string(),
            server_version: "0.0.0-test".to_string(),
            selected_version: ProtocolVersion::new(1, 1),
            provider_id: "rpc-auth".to_string(),
            display_name: "RPC Auth".to_string(),
            form_definition: AuthFormDef { tabs: vec![] },
            capabilities: AuthProviderCapabilities {
                login: AuthProviderLoginCapabilities {
                    supported: true,
                    verification_url_progress: true,
                },
            },
        };

        assert!(response.capabilities.login.supported);
        assert!(response.capabilities.login.verification_url_progress);
    }

    #[test]
    fn fetch_field_options_request_round_trips_via_serde() {
        let mut dependencies = HashMap::new();
        dependencies.insert("region".to_string(), "us-east-1".to_string());

        let request = FetchFieldOptionsRequest {
            profile_json: r#"{"provider_id":"rpc-auth"}"#.to_string(),
            field_id: "environment".to_string(),
            dependencies,
            session: Some(serde_json::json!({"token": "abc123"})),
        };

        let serialized = serde_json::to_string(&request).expect("serialize request");
        let deserialized: FetchFieldOptionsRequest =
            serde_json::from_str(&serialized).expect("deserialize request");

        assert_eq!(deserialized.field_id, "environment");
        assert_eq!(
            deserialized.dependencies.get("region").map(String::as_str),
            Some("us-east-1")
        );
        assert!(deserialized.session.is_some());
    }

    #[test]
    fn fetch_field_options_response_round_trips_via_serde() {
        use dbflux_core::SelectOption;

        let response = FetchFieldOptionsResponse {
            options: vec![
                SelectOption::new("dev", "Development"),
                SelectOption::new("prod", "Production"),
            ],
            cache_hint_seconds: Some(300),
        };

        let serialized = serde_json::to_string(&response).expect("serialize response");
        let deserialized: FetchFieldOptionsResponse =
            serde_json::from_str(&serialized).expect("deserialize response");

        assert_eq!(deserialized.options.len(), 2);
        assert_eq!(deserialized.cache_hint_seconds, Some(300));
    }

    /// FR-005: `AuthSession.data` stored as `Arc<serde_json::Value>` must survive
    /// round-trip through `AuthSessionDto` without loss.  Providers that store
    /// non-JSON data (e.g. `Arc<SdkConfig>`) are expected to produce `None`.
    #[test]
    fn auth_session_json_data_survives_dto_round_trip() {
        use std::sync::Arc;
        use dbflux_core::auth::AuthSession;

        let original_data = serde_json::json!({"access_token": "tok-xyz", "expires_in": 3600});

        let session = AuthSession {
            provider_id: "rpc-auth".to_string(),
            profile_id: uuid::Uuid::nil(),
            expires_at: None,
            data: Some(Arc::new(original_data.clone()) as Arc<dyn std::any::Any + Send + Sync>),
        };

        let dto = AuthSessionDto::from(&session);
        assert_eq!(
            dto.session_data.as_ref(),
            Some(&original_data),
            "DTO must carry the JSON value verbatim"
        );

        let restored = AuthSession::from(dto);

        let restored_json = restored
            .data
            .expect("data must survive round-trip")
            .downcast::<serde_json::Value>()
            .expect("must downcast to serde_json::Value");

        assert_eq!(
            *restored_json, original_data,
            "JSON value must be bit-for-bit equal after round-trip"
        );
    }
}
