/// Import pipeline declarations.
///
/// `parse` and `decrypt` open a bundle from bytes; `plan` computes conflicts
/// and required resolutions; `apply` produces a set of remapped entities and
/// secret writes that the app layer persists through repositories and
/// `SecretManager::set_by_ref`.
///
/// Full implementations land in Slice 3. These stubs define the public API
/// surface and are compiled-checked here.
use secrecy::SecretString;

use crate::{
    DestSnapshot, ImportActions, ImportPlan, ParsedBundle, PortabilityError, ResolutionChoices,
};

/// Parse the bundle TOML bytes into `ParsedBundle`.
///
/// Extracts all plaintext metadata. When `bundle.encryption = "age-passphrase"`,
/// the secrets section remains sealed until `decrypt()` is called.
///
/// Returns `PortabilityError::Parse` for invalid TOML.
/// Returns `PortabilityError::UnsupportedVersion` for unknown `format_version`.
pub fn parse(bytes: &[u8]) -> Result<ParsedBundle, PortabilityError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| PortabilityError::Decryption(format!("bundle is not valid UTF-8: {e}")))?;

    let bundle: crate::bundle::Bundle = toml::from_str(text).map_err(PortabilityError::Parse)?;

    if bundle.bundle.format_version != crate::bundle::CURRENT_FORMAT_VERSION {
        return Err(PortabilityError::UnsupportedVersion {
            version: bundle.bundle.format_version,
        });
    }

    Ok(ParsedBundle {
        bundle,
        decrypted_secrets: None,
    })
}

/// Decrypt the secrets section of a previously parsed bundle.
///
/// Must be called when `bundle.encryption = "age-passphrase"` before `plan()`
/// can process secrets. A wrong passphrase returns `PortabilityError::Decryption`,
/// which is recoverable — the caller should re-prompt.
///
/// This is a no-op (returns `Ok(())`) when `encryption = "none"` or when the
/// bundle has no secrets section.
pub fn decrypt(
    parsed: &mut ParsedBundle,
    passphrase: &SecretString,
) -> Result<(), PortabilityError> {
    use crate::bundle::{EncryptionMode, SecretsSection};

    if parsed.bundle.bundle.encryption == EncryptionMode::None {
        if let Some(SecretsSection::Plaintext { values }) = &parsed.bundle.secrets {
            parsed.decrypted_secrets = Some(values.clone());
        }
        return Ok(());
    }

    #[cfg(feature = "encryption")]
    {
        if let Some(SecretsSection::Encrypted { ciphertext }) = &parsed.bundle.secrets {
            let secrets = crate::encryption::decrypt_secrets(ciphertext, passphrase)?;
            parsed.decrypted_secrets = Some(secrets);
        }
        Ok(())
    }

    #[cfg(not(feature = "encryption"))]
    {
        let _passphrase = passphrase;
        Err(PortabilityError::EncryptionUnavailable)
    }
}

/// Compute the import plan: conflict detection and required resolutions.
///
/// Runs the conflict-identity predicates against `dest` for each referenced
/// auth/proxy/ssh entry and collects every unresolved required reference
/// (including AWS references not found via reflection on the target).
///
/// Implementation lands in Slice 3.
pub fn plan(_parsed: &ParsedBundle, _dest: &DestSnapshot<'_>) -> ImportPlan {
    ImportPlan::default()
}

/// Apply the resolution choices to produce remapped entities and secret writes.
///
/// This function is PURE: it does not touch the OS keyring, SQLite, or any I/O.
/// All side effects (repository inserts, `SecretManager::set_by_ref` calls) are
/// performed by the app layer after inspecting the returned `ImportActions`.
///
/// Every entity receives a fresh `Uuid::new_v4()`. All intra-bundle references
/// (auth_profile_id, access_kind, secret keys) are rewritten to the newly minted
/// UUIDs before being returned.
///
/// Implementation lands in Slice 3.
pub fn apply(
    _parsed: &ParsedBundle,
    _plan: &ImportPlan,
    _choices: &ResolutionChoices,
) -> Result<ImportActions, PortabilityError> {
    Err(PortabilityError::InvalidChoices {
        reason: "apply not yet implemented (Slice 3)".to_string(),
    })
}
