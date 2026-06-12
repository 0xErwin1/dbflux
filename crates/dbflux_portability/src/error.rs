use thiserror::Error;

/// Errors produced by the portability pipeline.
#[derive(Debug, Error)]
pub enum PortabilityError {
    /// The bundle bytes could not be parsed as valid TOML.
    #[error("bundle parse error: {0}")]
    Parse(#[from] toml::de::Error),

    /// The bundle bytes are not valid UTF-8 or have another format-level problem
    /// that is detected before TOML parsing begins.
    ///
    /// Distinct from `Decryption`: the input is simply not a readable bundle,
    /// not a passphrase failure. (R-ROB-3 / L4 / ADR-10)
    #[error("bundle format error: {0}")]
    Format(String),

    /// The bundle was serialized to TOML but the process failed.
    #[error("bundle serialize error: {0}")]
    Serialize(#[from] toml::ser::Error),

    /// The bundle declares an unsupported or incompatible format version.
    #[error("unsupported bundle format version {version}")]
    UnsupportedVersion { version: u32 },

    /// Encryption of the secrets section failed (serialize, armor, or I/O error).
    ///
    /// This is distinct from `Decryption`: a caller that catches this error should
    /// treat it as a hard failure rather than re-prompting for a passphrase.
    #[error("encryption failed: {0}")]
    Encryption(String),

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
    /// Callers must pass `EncryptionChoice::Plaintext { forced: true }` to acknowledge
    /// the security implications of writing secrets in cleartext.
    #[error("plaintext export requires explicit force opt-in")]
    PlaintextForceMissing,

    /// SSH key embedding was requested but the encryption choice is plaintext.
    ///
    /// Private-key bytes may only be embedded when the bundle's `[secrets]`
    /// section is passphrase-encrypted. Emitting key bytes in cleartext is
    /// rejected unconditionally regardless of the force flag (R-SEC-2 / H1).
    #[error("SSH key embedding requires passphrase encryption; cannot embed private-key bytes in a cleartext bundle")]
    SshKeyEmbedRequiresEncryption,

    /// The bundle header `encryption` mode conflicts with the `[secrets]` section variant.
    ///
    /// For example: `encryption = "age-passphrase"` paired with a plaintext `[secrets]`
    /// table, or `encryption = "none"` paired with an encrypted ciphertext blob.
    /// Such a bundle is malformed and must be rejected before any plan or apply step.
    #[error("bundle encryption mode '{declared}' does not match secrets section variant '{found}'")]
    ModeMismatch { declared: String, found: String },
}

impl PortabilityError {
    /// Returns `true` when this error indicates that the encryption feature was
    /// compiled out of this build.
    ///
    /// Using this predicate instead of matching on `Display` output avoids
    /// brittle string matching and works correctly in both `encryption`-enabled
    /// (always `false`) and `encryption`-disabled builds.
    pub fn is_encryption_unavailable(&self) -> bool {
        #[cfg(not(feature = "encryption"))]
        {
            matches!(self, PortabilityError::EncryptionUnavailable)
        }
        #[cfg(feature = "encryption")]
        {
            let _ = self;
            false
        }
    }
}
