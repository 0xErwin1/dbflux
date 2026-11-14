use std::fmt;
use std::time::Duration;

use super::protocol::{AuthProfileDependency, SecretProviderCapabilities, SecretProviderManifest};

/// Provider ids owned by the built-in secret providers; external providers may not claim them.
pub const BUILT_IN_SECRET_PROVIDER_IDS: [&str; 2] = ["aws-secrets-manager", "aws-ssm"];

pub const SUPPORTED_MANIFEST_VERSION: u16 = 1;
pub const MAX_PROVIDER_ID_LENGTH: usize = 64;
pub const DEFAULT_REQUEST_TIMEOUT_MS: u32 = 15_000;
pub const MIN_REQUEST_TIMEOUT_MS: u32 = 1;
pub const MAX_REQUEST_TIMEOUT_MS: u32 = 60_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedManifest {
    pub provider_id: String,
    pub display_name: String,
    pub capabilities: SecretProviderCapabilities,
    pub auth_profile_dependency: AuthProfileDependency,
    pub cache_ttl: Option<Duration>,
    pub request_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestError {
    UnsupportedManifestVersion(u16),
    EmptyProviderId,
    ProviderIdTooLong(usize),
    InvalidProviderId(String),
    ReservedProviderId(String),
    EmptyDisplayName,
    MissingGetSecretCapability,
}

impl fmt::Display for ManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedManifestVersion(version) => write!(
                formatter,
                "unsupported secret-provider manifest version {version} (expected {SUPPORTED_MANIFEST_VERSION})"
            ),
            Self::EmptyProviderId => write!(formatter, "secret-provider id must not be empty"),
            Self::ProviderIdTooLong(length) => write!(
                formatter,
                "secret-provider id is {length} characters long (maximum {MAX_PROVIDER_ID_LENGTH})"
            ),
            Self::InvalidProviderId(id) => write!(
                formatter,
                "secret-provider id '{id}' must use only lowercase letters, digits, '.', '_' or '-'"
            ),
            Self::ReservedProviderId(id) => {
                write!(formatter, "secret-provider id '{id}' is already in use")
            }
            Self::EmptyDisplayName => {
                write!(formatter, "secret-provider display name must not be empty")
            }
            Self::MissingGetSecretCapability => write!(
                formatter,
                "secret provider does not declare the get_secret capability"
            ),
        }
    }
}

impl std::error::Error for ManifestError {}

pub fn validate_manifest(
    manifest: &SecretProviderManifest,
    reserved_ids: &[&str],
) -> Result<ValidatedManifest, ManifestError> {
    if manifest.manifest_version != SUPPORTED_MANIFEST_VERSION {
        return Err(ManifestError::UnsupportedManifestVersion(
            manifest.manifest_version,
        ));
    }

    validate_provider_id(&manifest.provider_id, reserved_ids)?;

    let display_name = manifest.display_name.trim();
    if display_name.is_empty() {
        return Err(ManifestError::EmptyDisplayName);
    }

    if !manifest.capabilities.get_secret {
        return Err(ManifestError::MissingGetSecretCapability);
    }

    Ok(ValidatedManifest {
        provider_id: manifest.provider_id.clone(),
        display_name: display_name.to_string(),
        capabilities: manifest.capabilities,
        auth_profile_dependency: manifest.auth_profile_dependency,
        cache_ttl: manifest
            .cache_ttl_seconds
            .map(|seconds| Duration::from_secs(u64::from(seconds))),
        request_timeout: normalize_request_timeout(manifest.request_timeout_ms),
    })
}

pub fn normalize_request_timeout(request_timeout_ms: Option<u32>) -> Duration {
    let milliseconds = request_timeout_ms
        .unwrap_or(DEFAULT_REQUEST_TIMEOUT_MS)
        .clamp(MIN_REQUEST_TIMEOUT_MS, MAX_REQUEST_TIMEOUT_MS);

    Duration::from_millis(u64::from(milliseconds))
}

fn validate_provider_id(provider_id: &str, reserved_ids: &[&str]) -> Result<(), ManifestError> {
    if provider_id.is_empty() {
        return Err(ManifestError::EmptyProviderId);
    }

    let length = provider_id.chars().count();
    if length > MAX_PROVIDER_ID_LENGTH {
        return Err(ManifestError::ProviderIdTooLong(length));
    }

    let is_valid = provider_id.chars().all(|character| {
        character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || matches!(character, '.' | '_' | '-')
    });
    if !is_valid {
        return Err(ManifestError::InvalidProviderId(provider_id.to_string()));
    }

    if reserved_ids.contains(&provider_id) {
        return Err(ManifestError::ReservedProviderId(provider_id.to_string()));
    }

    Ok(())
}
