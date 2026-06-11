//! Age-passphrase encryption for the `[secrets]` section.
//!
//! The entire secrets map is serialized to JSON, then encrypted as a single AEAD
//! unit using age passphrase mode (scrypt KDF). The ciphertext is stored as ASCII
//! armor so it embeds cleanly inside a TOML string field.
//!
//! This module is compiled only when the `encryption` feature is enabled. When the
//! feature is absent, the only reachable code paths are the plaintext ones; any
//! bundle with `encryption = "age-passphrase"` will produce
//! `PortabilityError::EncryptionUnavailable` at decrypt time.

use std::collections::HashMap;

use age::secrecy::SecretString;

use crate::PortabilityError;

/// Encrypt the secrets map with an age passphrase.
///
/// Returns the ASCII-armor ciphertext string suitable for embedding in the
/// bundle's `[secrets].ciphertext` TOML field.
pub fn encrypt_secrets(
    secrets: &HashMap<String, String>,
    passphrase: &SecretString,
) -> Result<String, PortabilityError> {
    use age::Encryptor;
    use age::armor::{ArmoredWriter, Format};
    use std::io::Write;

    let plaintext = serde_json::to_vec(secrets)
        .map_err(|e| PortabilityError::Encryption(format!("secrets serialize: {e}")))?;

    let encryptor = Encryptor::with_user_passphrase(passphrase.clone());

    let mut ciphertext = Vec::new();
    let armored = ArmoredWriter::wrap_output(&mut ciphertext, Format::AsciiArmor)
        .map_err(|e| PortabilityError::Encryption(e.to_string()))?;

    let mut writer = encryptor
        .wrap_output(armored)
        .map_err(|e| PortabilityError::Encryption(e.to_string()))?;

    writer
        .write_all(&plaintext)
        .map_err(|e| PortabilityError::Encryption(e.to_string()))?;

    let armored_writer = writer
        .finish()
        .map_err(|e| PortabilityError::Encryption(e.to_string()))?;

    armored_writer
        .finish()
        .map_err(|e| PortabilityError::Encryption(e.to_string()))?;

    String::from_utf8(ciphertext).map_err(|e| PortabilityError::Encryption(e.to_string()))
}

/// Decrypt the age-encrypted secrets ciphertext.
///
/// Returns the plaintext secrets map or `PortabilityError::Decryption` when the
/// passphrase is wrong or the ciphertext is corrupt. This is a recoverable error:
/// the caller should re-prompt for the passphrase.
pub fn decrypt_secrets(
    ciphertext: &str,
    passphrase: &SecretString,
) -> Result<HashMap<String, String>, PortabilityError> {
    use age::Decryptor;
    use age::armor::ArmoredReader;
    use std::io::Read;

    let identity = age::scrypt::Identity::new(passphrase.clone());
    let armored = ArmoredReader::new(ciphertext.as_bytes());

    let decryptor =
        Decryptor::new(armored).map_err(|e| PortabilityError::Decryption(e.to_string()))?;

    let mut reader = decryptor
        .decrypt(std::iter::once(&identity as &dyn age::Identity))
        .map_err(|e| PortabilityError::Decryption(e.to_string()))?;

    let mut plaintext = Vec::new();
    reader
        .read_to_end(&mut plaintext)
        .map_err(|e| PortabilityError::Decryption(e.to_string()))?;

    serde_json::from_slice(&plaintext)
        .map_err(|e| PortabilityError::Decryption(format!("secrets deserialize: {e}")))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn passphrase(s: &str) -> SecretString {
        SecretString::from(s.to_string())
    }

    #[test]
    fn encrypt_decrypt_round_trip() {
        let mut secrets = HashMap::new();
        secrets.insert(
            "conn:aaaaaaaa-0000-0000-0000-000000000001:password".to_string(),
            "s3cr3t_password".to_string(),
        );
        secrets.insert(
            "ssh_tunnel:bbbbbbbb-0000-0000-0000-000000000002:private_key".to_string(),
            "base64encodedkeydata".to_string(),
        );

        let passphrase = passphrase("correct-horse-battery-staple");

        let ciphertext = encrypt_secrets(&secrets, &passphrase).expect("encrypt");

        assert!(
            ciphertext.starts_with("-----BEGIN AGE ENCRYPTED FILE-----"),
            "output must be age ASCII armor; got: {}",
            &ciphertext[..60.min(ciphertext.len())]
        );

        let decrypted = decrypt_secrets(&ciphertext, &passphrase).expect("decrypt");

        assert_eq!(
            secrets, decrypted,
            "round-trip must reproduce original secrets"
        );
    }

    #[test]
    fn decrypt_wrong_passphrase_returns_error() {
        let mut secrets = HashMap::new();
        secrets.insert("key".to_string(), "value".to_string());

        let ciphertext = encrypt_secrets(&secrets, &passphrase("correct")).expect("encrypt");

        let result = decrypt_secrets(&ciphertext, &passphrase("wrong"));

        assert!(
            result.is_err(),
            "wrong passphrase must produce a decryption error"
        );
    }

    #[test]
    fn empty_secrets_map_round_trips() {
        let secrets: HashMap<String, String> = HashMap::new();
        let pp = passphrase("empty-test");

        let ciphertext = encrypt_secrets(&secrets, &pp).expect("encrypt");
        let decrypted = decrypt_secrets(&ciphertext, &pp).expect("decrypt");

        assert!(decrypted.is_empty());
    }
}
