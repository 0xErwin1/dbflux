use thiserror::Error;

/// Errors produced by the portability pipeline.
#[derive(Debug, Error)]
pub enum PortabilityError {
    /// The bundle bytes could not be parsed as valid TOML.
    #[error("bundle parse error: {0}")]
    Parse(#[from] toml::de::Error),

    /// The bundle was serialized to TOML but the process failed.
    #[error("bundle serialize error: {0}")]
    Serialize(#[from] toml::ser::Error),

    /// The bundle declares an unsupported or incompatible format version.
    #[error("unsupported bundle format version {version}")]
    UnsupportedVersion { version: u32 },

    /// Decryption failed, most likely due to a wrong passphrase.
    ///
    /// This is a recoverable error: the caller should re-prompt rather than abort.
    #[error("decryption failed: {0}")]
    Decryption(String),

    /// The bundle requires encryption support but it was compiled out.
    #[cfg(not(feature = "encryption"))]
    #[error(
        "this build does not include encryption support; cannot read or write encrypted bundles"
    )]
    EncryptionUnavailable,

    /// A required secret was not available from the caller-supplied reader.
    #[error("secret not available for ref: {secret_ref}")]
    SecretUnavailable { secret_ref: String },

    /// A resolution choice required for import was not provided.
    #[error("missing resolution choice for ref: {local_id}")]
    MissingResolution { local_id: String },

    /// The import plan could not be applied because of inconsistent choices.
    #[error("invalid resolution choices: {reason}")]
    InvalidChoices { reason: String },

    /// The secrets section of the bundle is missing when decrypted secrets are expected.
    #[error("secrets section missing from bundle")]
    MissingSecrets,

    /// Plaintext-force export was attempted without explicit opt-in.
    ///
    /// Callers must set `EncryptionChoice::Plaintext` and accept the warning.
    #[error("plaintext export requires explicit force opt-in")]
    PlaintextForceMissing,
}
