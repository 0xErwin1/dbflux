//! External secret providers reached over the `secret_provider_rpc` IPC family.

mod client;
mod host;
mod manifest;
mod protocol;

#[cfg(test)]
mod tests;

pub use client::{
    MISSING_CREDENTIALS_CODE, PROTOCOL_CODE, RpcSecretProvider, SOURCE_LOST_CODES, TIMEOUT_CODE,
    TRANSPORT_CODE, is_source_lost_code, is_source_lost_error, negotiate_secret_provider_version,
};
pub use host::shutdown_managed_secret_provider_hosts;
pub use manifest::{
    BUILT_IN_SECRET_PROVIDER_IDS, DEFAULT_REQUEST_TIMEOUT_MS, MAX_PROVIDER_ID_LENGTH,
    MAX_REQUEST_TIMEOUT_MS, MIN_REQUEST_TIMEOUT_MS, ManifestError, SUPPORTED_MANIFEST_VERSION,
    ValidatedManifest, normalize_request_timeout, validate_manifest,
};
pub use protocol::{
    AuthProfileDependency, GetSecretRequest, SecretProviderCapabilities,
    SecretProviderHelloRequest, SecretProviderHelloResponse, SecretProviderManifest,
    SecretProviderRequestBody, SecretProviderRequestEnvelope, SecretProviderResponseBody,
    SecretProviderResponseEnvelope, SecretProviderRpcError, SecretProviderRpcErrorCode,
};
