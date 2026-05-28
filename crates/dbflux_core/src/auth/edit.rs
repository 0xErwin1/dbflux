//! Edit-session types for AWS profile write-back.
//!
//! These types are part of the optimistic-concurrency contract between the
//! Settings UI and the AWS write-back layer (spec R9.3, design section 11).
//! They live in `dbflux_core` so both `dbflux_aws` (write seam) and
//! `dbflux_ui_windows` (Settings UI) share the same types without creating
//! a direct dependency between those crates.
//!
//! # Security invariant
//!
//! `AwsSectionHash` carries only a SHA-256 digest — never raw section bytes
//! or secret values. `AuthSaveOutcome` carries only file identifiers, never
//! key material. No `Debug` or `Display` impl on these types prints secrets.

/// Identifies which AWS credential file a write targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwsEditFile {
    /// `~/.aws/config`
    Config,
    /// `~/.aws/credentials`
    Credentials,
}

/// Opaque SHA-256 digest over the raw bytes of one section in an AWS file.
///
/// Computed backend-side at edit-open time and re-checked inside the atomic
/// write transform before any bytes are written (spec R9.3.1–R9.3.5).
///
/// The inner `[u8; 32]` is the only thing that crosses the seam; section
/// contents and secrets are never included.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct AwsSectionHash(pub [u8; 32]);

impl std::fmt::Debug for AwsSectionHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Print as a hex string to keep the Debug representation legible and
        // unambiguous. The bytes are a SHA-256 digest — there is no secret
        // material here — but a hex rendering is cleaner than a byte array.
        write!(f, "AwsSectionHash(")?;
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        write!(f, ")")
    }
}

/// Snapshot token captured at edit-open time (spec R9.3.1).
///
/// Holds the per-section SHA-256 hashes needed for optimistic-concurrency
/// conflict detection. Passed back by the UI to the write seam at save time.
///
/// Fields are `None` when the provider does not write to that file (e.g. an
/// SSO-only profile has no credentials section).
///
/// # Security invariant
///
/// This struct carries no secret material. A `Debug` print is safe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsEditSnapshot {
    /// Hash of the `[profile NAME]` (or `[sso-session NAME]`) block in
    /// `~/.aws/config`, captured when the edit form was opened.
    ///
    /// `None` when the provider does not write to `~/.aws/config` or when
    /// the section did not exist on disk at snapshot time.
    pub config_section: Option<AwsSectionHash>,

    /// Hash of the bare `[NAME]` block in `~/.aws/credentials`, captured when
    /// the edit form was opened.
    ///
    /// `None` when the provider does not write to `~/.aws/credentials` or
    /// when the credentials section did not exist on disk at snapshot time.
    pub credentials_section: Option<AwsSectionHash>,
}

/// Result returned by the edit-save seam after an attempted write (spec R9.3.4,
/// R9.3.5, design section 11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthSaveOutcome {
    /// All targeted file sections were written successfully.
    Saved,

    /// The targeted section in `file` was modified on disk between edit-open
    /// and save. No bytes were written for that file. The UI should offer a
    /// Reload action (spec R9.3.6).
    Conflict {
        /// Which file's section hash did not match the snapshot.
        file: AwsEditFile,
    },

    /// An edit spanning both files where one succeeded and the other
    /// conflicted. The files are independent; the successful write is NOT
    /// rolled back. The UI should surface which file was written and which
    /// needs reload (spec S35).
    PartialSaved {
        /// The file whose section was successfully written.
        written: AwsEditFile,
        /// The file whose section hash did not match the snapshot; not written.
        conflicted: AwsEditFile,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    // E1.1 — Test-first: types compile and have the expected shapes.

    #[test]
    fn aws_section_hash_is_32_bytes() {
        let hash = AwsSectionHash([0u8; 32]);
        assert_eq!(hash.0.len(), 32);
    }

    #[test]
    fn aws_section_hash_derives_eq() {
        let a = AwsSectionHash([1u8; 32]);
        let b = AwsSectionHash([1u8; 32]);
        let c = AwsSectionHash([2u8; 32]);

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn aws_section_hash_debug_does_not_contain_secret_material() {
        // The debug representation must be a hex digest string only — no
        // raw bytes that could coincide with secret material, no "secret",
        // no key-material keywords.
        let hash = AwsSectionHash([0xABu8; 32]);
        let repr = format!("{hash:?}");

        // Should look like: AwsSectionHash(abababab...)
        assert!(repr.starts_with("AwsSectionHash("));
        assert!(!repr.contains("secret"));
        assert!(!repr.contains("key"));
    }

    #[test]
    fn aws_edit_file_variants_are_distinct() {
        assert_ne!(AwsEditFile::Config, AwsEditFile::Credentials);
        // Both variants must derive Debug and Copy without issues.
        let _config = AwsEditFile::Config;
        let _creds = AwsEditFile::Credentials;
        let copy = _config;
        assert_eq!(copy, AwsEditFile::Config);
    }

    #[test]
    fn aws_edit_snapshot_carries_two_optional_hashes() {
        let snapshot = AwsEditSnapshot {
            config_section: Some(AwsSectionHash([1u8; 32])),
            credentials_section: None,
        };

        assert!(snapshot.config_section.is_some());
        assert!(snapshot.credentials_section.is_none());
    }

    #[test]
    fn aws_edit_snapshot_with_both_sections() {
        let snapshot = AwsEditSnapshot {
            config_section: Some(AwsSectionHash([1u8; 32])),
            credentials_section: Some(AwsSectionHash([2u8; 32])),
        };

        assert_eq!(snapshot.config_section, Some(AwsSectionHash([1u8; 32])));
        assert_eq!(
            snapshot.credentials_section,
            Some(AwsSectionHash([2u8; 32]))
        );
    }

    #[test]
    fn auth_save_outcome_saved_variant() {
        let outcome = AuthSaveOutcome::Saved;
        assert_eq!(outcome, AuthSaveOutcome::Saved);
    }

    #[test]
    fn auth_save_outcome_conflict_variant_carries_file() {
        let outcome = AuthSaveOutcome::Conflict {
            file: AwsEditFile::Config,
        };
        let AuthSaveOutcome::Conflict { file } = outcome else {
            panic!("expected Conflict variant");
        };
        assert_eq!(file, AwsEditFile::Config);
    }

    #[test]
    fn auth_save_outcome_partial_saved_variant() {
        let outcome = AuthSaveOutcome::PartialSaved {
            written: AwsEditFile::Config,
            conflicted: AwsEditFile::Credentials,
        };
        let AuthSaveOutcome::PartialSaved {
            written,
            conflicted,
        } = outcome
        else {
            panic!("expected PartialSaved variant");
        };
        assert_eq!(written, AwsEditFile::Config);
        assert_eq!(conflicted, AwsEditFile::Credentials);
    }

    #[test]
    fn auth_save_outcome_debug_repr_contains_no_secret_material() {
        // Regression guard: none of the outcome variants should contain secret
        // patterns. This is trivially true now but documents the invariant.
        let outcomes = [
            AuthSaveOutcome::Saved,
            AuthSaveOutcome::Conflict {
                file: AwsEditFile::Config,
            },
            AuthSaveOutcome::PartialSaved {
                written: AwsEditFile::Config,
                conflicted: AwsEditFile::Credentials,
            },
        ];

        for outcome in &outcomes {
            let repr = format!("{outcome:?}");
            assert!(
                !repr.contains("AKIA"),
                "outcome debug must not contain AKIA"
            );
            assert!(
                !repr.contains("aws_secret_access_key"),
                "outcome debug must not contain secret key name"
            );
        }
    }
}
