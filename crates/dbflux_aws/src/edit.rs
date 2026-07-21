//! AWS-internal edit snapshot types.
//!
//! These types are private to `dbflux_aws` — they implement the
//! optimistic-concurrency contract for AWS file-backed providers.  Nothing
//! here is exported from the crate.

pub(crate) const AWS_CONFIG_PATH: &str = "~/.aws/config";
pub(crate) const AWS_CREDENTIALS_PATH: &str = "~/.aws/credentials";

pub(crate) const AWS_TARGET_ID_CONFIG: &str = "config";
pub(crate) const AWS_TARGET_ID_CREDENTIALS: &str = "credentials";

/// Opaque SHA-256 digest over the raw bytes of one section in an AWS file.
///
/// Computed at edit-open time and re-checked inside the atomic write transform
/// before any bytes are written.  The inner `[u8; 32]` is the only thing that
/// crosses the seam; section contents and secrets are never included.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct AwsSectionHash(pub [u8; 32]);

impl std::fmt::Debug for AwsSectionHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AwsSectionHash(")?;
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        write!(f, ")")
    }
}

/// Snapshot token captured at edit-open time.
///
/// Holds the per-section SHA-256 hashes needed for optimistic-concurrency
/// conflict detection.  Passed to `AuthEditSnapshot::new(aws_snap)` before
/// being returned to the UI.  Fields are `None` when the provider does not
/// write to the corresponding file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AwsEditSnapshot {
    pub config_section: Option<AwsSectionHash>,
    pub credentials_section: Option<AwsSectionHash>,
}

/// Identifies which AWS credential file a write targets.
///
/// Used internally by providers to construct `AuthEditTarget` values; not
/// exposed from the crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AwsEditFileKind {
    Config,
    Credentials,
}

impl AwsEditFileKind {
    /// Convert to a provider-neutral `AuthEditTarget` with the canonical
    /// AWS file paths as the label.
    pub(crate) fn to_target(self) -> dbflux_core::auth::AuthEditTarget {
        match self {
            AwsEditFileKind::Config => dbflux_core::auth::AuthEditTarget {
                id: AWS_TARGET_ID_CONFIG.to_string(),
                label: AWS_CONFIG_PATH.to_string(),
            },
            AwsEditFileKind::Credentials => dbflux_core::auth::AuthEditTarget {
                id: AWS_TARGET_ID_CREDENTIALS.to_string(),
                label: AWS_CREDENTIALS_PATH.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::auth::AuthEditTarget;

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
    fn aws_section_hash_debug_is_hex() {
        let hash = AwsSectionHash([0xABu8; 32]);
        let repr = format!("{hash:?}");
        assert!(repr.starts_with("AwsSectionHash("));
        assert!(!repr.contains("secret"));
    }

    // Task 2.2: to_target() returns correct id and label for each variant.
    #[test]
    fn aws_edit_file_kind_config_to_target() {
        let target = AwsEditFileKind::Config.to_target();
        assert_eq!(
            target,
            AuthEditTarget {
                id: "config".to_string(),
                label: "~/.aws/config".to_string(),
            }
        );
    }

    #[test]
    fn aws_edit_file_kind_credentials_to_target() {
        let target = AwsEditFileKind::Credentials.to_target();
        assert_eq!(
            target,
            AuthEditTarget {
                id: "credentials".to_string(),
                label: "~/.aws/credentials".to_string(),
            }
        );
    }
}
