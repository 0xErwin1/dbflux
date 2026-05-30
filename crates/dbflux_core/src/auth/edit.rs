//! Edit-session types for auth provider write-back.
//!
//! The `AuthEditSnapshot` / `AuthEditTarget` / `AuthSaveOutcome` triad is the
//! provider-neutral edit seam shared between the Settings UI and any auth
//! provider that supports file-backed editing.  Provider-specific internals
//! live in each provider's own crate.
//!
//! # Security invariant
//!
//! `AuthEditSnapshot` carries only opaque provider-internal data behind an
//! `Arc<dyn Any>`. `AuthEditTarget` carries only human-readable path strings,
//! never key material. No `Debug` or `Display` impl on these types prints secrets.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// Opaque snapshot token captured at edit-open time.
///
/// Wraps provider-internal state (e.g. section hashes, last-modified stamps)
/// behind a type-erased `Arc` so that `dbflux_core` does not need to know the
/// concrete type.  The provider that created the snapshot must downcast via
/// `downcast_ref` to recover its internal state at save time.
///
/// Passing a snapshot from provider A to provider B's `save_edit` results in a
/// failed downcast; the provider MUST handle `None` defensively.
///
/// # Security invariant
///
/// The `Debug` impl prints only the literal string `"AuthEditSnapshot(opaque)"`,
/// never the inner value — regardless of what the provider stored.
#[derive(Clone)]
pub struct AuthEditSnapshot(Arc<dyn Any + Send + Sync>);

impl AuthEditSnapshot {
    /// Wrap `inner` in an opaque snapshot token.
    pub fn new<T: Any + Send + Sync>(inner: T) -> Self {
        Self(Arc::new(inner))
    }

    /// Attempt to recover the original `T` by downcasting.
    ///
    /// Returns `None` when the inner type does not match `T` — for example
    /// when a snapshot from a different provider is passed in.
    pub fn downcast_ref<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.0.downcast_ref::<T>()
    }
}

impl fmt::Debug for AuthEditSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AuthEditSnapshot(opaque)")
    }
}

/// Identifies a file or resource targeted by an edit-save operation.
///
/// `id` is a stable token for provider-internal logic (e.g. `"config"`,
/// `"credentials"`). `label` is the human-readable path shown in the UI
/// (e.g. `"<provider-config-path>"`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthEditTarget {
    pub id: String,
    pub label: String,
}

/// Result returned by the edit-save seam after an attempted write.
///
/// Providers return `Conflict` or `PartialSaved` to surface optimistic-
/// concurrency failures when a file was modified externally between
/// `open_edit_snapshot` and `save_edit`.
#[derive(Clone, Debug)]
pub enum AuthSaveOutcome {
    /// All targeted resources were written successfully.
    Saved,

    /// The targeted resource was modified on disk between edit-open and save.
    /// No bytes were written. The UI should offer a Reload action.
    Conflict {
        /// The resource whose on-disk state did not match the snapshot.
        target: AuthEditTarget,
    },

    /// An edit spanning two resources where one succeeded and the other
    /// conflicted. The successful write is NOT rolled back. The UI should
    /// surface which resource was written and which needs reload.
    PartialSaved {
        /// The resource whose section was successfully written.
        written: AuthEditTarget,
        /// The resource whose section hash did not match the snapshot.
        conflicted: AuthEditTarget,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    // S-SNAP-2: Debug output is exactly "AuthEditSnapshot(opaque)" regardless of inner value.
    #[test]
    fn snapshot_debug_is_opaque() {
        let snap = AuthEditSnapshot::new(42u32);
        assert_eq!(format!("{snap:?}"), "AuthEditSnapshot(opaque)");
    }

    // S-SNAP-1: Downcasting with the wrong type returns None; does not panic.
    #[test]
    fn snapshot_cross_type_downcast_returns_none() {
        struct ProviderA {
            value: u32,
        }
        struct ProviderB;

        let snap = AuthEditSnapshot::new(ProviderA { value: 42 });
        assert!(snap.downcast_ref::<ProviderB>().is_none());
        assert_eq!(snap.downcast_ref::<ProviderA>().unwrap().value, 42);
    }

    // S-TGT-1 / S-TGT-2: AuthEditTarget constructs with id and label fields.
    #[test]
    fn auth_edit_target_fields() {
        let target = AuthEditTarget {
            id: "config".to_string(),
            label: "path/to/config".to_string(),
        };
        assert_eq!(target.id, "config");
        assert_eq!(target.label, "path/to/config");
    }

    // AuthSaveOutcome variant construction and matches! pattern.
    #[test]
    fn auth_save_outcome_saved_matches() {
        let outcome = AuthSaveOutcome::Saved;
        assert!(matches!(outcome, AuthSaveOutcome::Saved));
    }

    #[test]
    fn auth_save_outcome_conflict_carries_target() {
        let outcome = AuthSaveOutcome::Conflict {
            target: AuthEditTarget {
                id: "config".to_string(),
                label: "path/to/config".to_string(),
            },
        };
        let AuthSaveOutcome::Conflict { target } = outcome else {
            panic!("expected Conflict variant");
        };
        assert_eq!(target.id, "config");
        assert_eq!(target.label, "path/to/config");
    }

    #[test]
    fn auth_save_outcome_partial_saved_carries_both_targets() {
        let outcome = AuthSaveOutcome::PartialSaved {
            written: AuthEditTarget {
                id: "config".to_string(),
                label: "path/to/config".to_string(),
            },
            conflicted: AuthEditTarget {
                id: "credentials".to_string(),
                label: "path/to/secondary".to_string(),
            },
        };
        let AuthSaveOutcome::PartialSaved {
            written,
            conflicted,
        } = outcome
        else {
            panic!("expected PartialSaved variant");
        };
        assert_eq!(written.id, "config");
        assert_eq!(conflicted.id, "credentials");
    }

    #[test]
    fn auth_save_outcome_debug_contains_no_secret_material() {
        let target = AuthEditTarget {
            id: "config".to_string(),
            label: "path/to/config".to_string(),
        };
        let outcomes: &[AuthSaveOutcome] = &[
            AuthSaveOutcome::Saved,
            AuthSaveOutcome::Conflict {
                target: target.clone(),
            },
            AuthSaveOutcome::PartialSaved {
                written: target.clone(),
                conflicted: AuthEditTarget {
                    id: "credentials".to_string(),
                    label: "path/to/secondary".to_string(),
                },
            },
        ];

        for outcome in outcomes {
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

    // Snapshot clone works correctly.
    #[test]
    fn snapshot_clone_shares_inner() {
        let snap = AuthEditSnapshot::new(99u32);
        let clone = snap.clone();
        assert_eq!(clone.downcast_ref::<u32>(), Some(&99u32));
    }
}
