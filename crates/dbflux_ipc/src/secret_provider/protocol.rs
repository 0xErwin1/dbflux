use std::fmt;

use serde::{Deserialize, Serialize};

use crate::auth_provider_protocol::ResolvedCredentialsDto;
use crate::envelope::ProtocolVersion;

const REDACTED: &str = "[REDACTED]";

#[derive(Clone, Serialize, Deserialize)]
pub struct SecretProviderHelloRequest {
    pub client_name: String,
    pub client_version: String,
    pub supported_versions: Vec<ProtocolVersion>,
    pub auth_token: Option<String>,
}

impl fmt::Debug for SecretProviderHelloRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretProviderHelloRequest")
            .field("client_name", &self.client_name)
            .field("client_version", &self.client_version)
            .field("supported_versions", &self.supported_versions)
            .field("auth_token", &self.auth_token.as_ref().map(|_| REDACTED))
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretProviderHelloResponse {
    pub server_name: String,
    pub server_version: String,
    pub selected_version: ProtocolVersion,
    pub manifest: SecretProviderManifest,
}

/// Whether the host forwards the connection's resolved auth-profile credentials.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthProfileDependency {
    #[default]
    None,
    Optional,
    Required,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretProviderCapabilities {
    #[serde(default)]
    pub get_secret: bool,
    #[serde(default)]
    pub json_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretProviderManifest {
    pub manifest_version: u16,
    pub provider_id: String,
    pub display_name: String,
    pub capabilities: SecretProviderCapabilities,
    #[serde(default)]
    pub auth_profile_dependency: AuthProfileDependency,
    #[serde(default)]
    pub cache_ttl_seconds: Option<u32>,
    #[serde(default)]
    pub request_timeout_ms: Option<u32>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct GetSecretRequest {
    pub locator: String,
    pub json_key: Option<String>,
    pub credentials: Option<ResolvedCredentialsDto>,
}

impl fmt::Debug for GetSecretRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GetSecretRequest")
            .field("locator", &self.locator)
            .field("json_key", &self.json_key)
            .field("credentials", &self.credentials.as_ref().map(|_| REDACTED))
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SecretProviderRequestBody {
    Hello(SecretProviderHelloRequest),
    GetSecret(GetSecretRequest),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecretProviderRpcErrorCode {
    NotFound,
    AccessDenied,
    Locked,
    InvalidLocator,
    Unavailable,
    Timeout,
    UnsupportedMethod,
    VersionMismatch,
    InvalidRequest,
    Internal,
}

impl SecretProviderRpcErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotFound => "NotFound",
            Self::AccessDenied => "AccessDenied",
            Self::Locked => "Locked",
            Self::InvalidLocator => "InvalidLocator",
            Self::Unavailable => "Unavailable",
            Self::Timeout => "Timeout",
            Self::UnsupportedMethod => "UnsupportedMethod",
            Self::VersionMismatch => "VersionMismatch",
            Self::InvalidRequest => "InvalidRequest",
            Self::Internal => "Internal",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretProviderRpcError {
    pub code: SecretProviderRpcErrorCode,
    pub message: String,
    pub retriable: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum SecretProviderResponseBody {
    Hello(SecretProviderHelloResponse),
    Secret { value: String },
    Error(SecretProviderRpcError),
}

impl fmt::Debug for SecretProviderResponseBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hello(hello) => formatter.debug_tuple("Hello").field(hello).finish(),
            Self::Secret { .. } => formatter
                .debug_struct("Secret")
                .field("value", &REDACTED)
                .finish(),
            Self::Error(error) => formatter.debug_tuple("Error").field(error).finish(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretProviderRequestEnvelope {
    pub protocol_version: ProtocolVersion,
    pub request_id: u64,
    pub body: SecretProviderRequestBody,
}

impl SecretProviderRequestEnvelope {
    pub fn new(
        protocol_version: ProtocolVersion,
        request_id: u64,
        body: SecretProviderRequestBody,
    ) -> Self {
        Self {
            protocol_version,
            request_id,
            body,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretProviderResponseEnvelope {
    pub protocol_version: ProtocolVersion,
    pub request_id: u64,
    pub body: SecretProviderResponseBody,
}

impl SecretProviderResponseEnvelope {
    pub fn new(
        protocol_version: ProtocolVersion,
        request_id: u64,
        body: SecretProviderResponseBody,
    ) -> Self {
        Self {
            protocol_version,
            request_id,
            body,
        }
    }
}
