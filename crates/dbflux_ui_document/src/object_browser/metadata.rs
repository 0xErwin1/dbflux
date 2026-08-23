//! Object metadata model for `ObjectBrowserDocument`'s preview pane.
//!
//! Pure data + formatting: `head_object` results, the preview gate derived
//! from them, and the lazily-fetched version list. The GPUI plumbing lives in
//! `data.rs` and the rendering in `preview.rs`.

use crate::buckets_table::{BucketDetailsState, format_bytes};
use dbflux_core::{ObjectMetadata, ObjectVersionSummary, VersioningStatus};

/// Storage classes whose objects must be restored before their bytes can be
/// read. Preview never fetches bytes for these, regardless of object size.
const ARCHIVED_STORAGE_CLASSES: [&str; 2] = ["GLACIER", "DEEP_ARCHIVE"];

pub(super) fn is_archived_storage_class(storage_class: Option<&str>) -> bool {
    storage_class.is_some_and(|class| {
        let class = class.to_uppercase();
        ARCHIVED_STORAGE_CLASSES.contains(&class.as_str())
    })
}

/// Whether the previewed object's bytes may be fetched, decided from
/// `head_object` alone — no body request is ever issued to find out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreviewGate {
    Allowed,
    TooLarge { size_bytes: u64, limit_bytes: u64 },
    Archived,
}

impl PreviewGate {
    /// Explanation shown in place of the preview, or `None` when the object
    /// is previewable. Delegates to the translated, exhaustive
    /// `crate::labels::preview_gate_message` so every variant routes through
    /// the catalog.
    pub fn message(&self) -> Option<String> {
        crate::labels::preview_gate_message(self)
    }
}

/// Archived storage classes win over the size check: an archived object is
/// never fetched even when it would fit under the limit.
pub fn evaluate_preview_gate(metadata: &ObjectMetadata, limit_bytes: u64) -> PreviewGate {
    if is_archived_storage_class(metadata.storage_class.as_deref()) {
        return PreviewGate::Archived;
    }

    if metadata.size_bytes > limit_bytes {
        return PreviewGate::TooLarge {
            size_bytes: metadata.size_bytes,
            limit_bytes,
        };
    }

    PreviewGate::Allowed
}

/// Metadata panel state for the object currently held in the preview pane.
#[derive(Clone, Debug, PartialEq)]
pub enum ObjectMetadataState {
    Loading,
    Loaded {
        metadata: Box<ObjectMetadata>,
        gate: PreviewGate,
    },
    Error(String),
}

/// Version list state. Versions are fetched only when the user asks for them
/// (`list_object_versions` is a separate, billed API call).
#[derive(Clone, Debug, Default, PartialEq)]
pub enum ObjectVersionsState {
    #[default]
    Idle,
    Loading,
    Loaded(Vec<ObjectVersionSummary>),
    Error(String),
}

/// Versions only exist on buckets that have (or had) versioning turned on;
/// anywhere else `list_object_versions` would always come back empty, so the
/// versions row does not offer the lookup.
pub(super) fn versioning_tracks_history(details: &BucketDetailsState) -> bool {
    match details {
        BucketDetailsState::Loaded(details) => matches!(
            details.versioning,
            VersioningStatus::Enabled | VersioningStatus::Suspended
        ),
        _ => false,
    }
}

/// Size line of the metadata panel: human-readable, with the exact byte count
/// alongside it — S3 sizes are frequently compared against exact values.
pub(super) fn format_size_detail(size_bytes: u64) -> String {
    format!(
        "{} ({} bytes)",
        format_bytes(size_bytes),
        group_digits(size_bytes)
    )
}

fn group_digits(value: u64) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);

    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }

    grouped
}

/// Shortened version id for the versions list; full ids are opaque and long.
pub(super) fn short_version_id(version_id: &str) -> String {
    const KEEP: usize = 12;

    if version_id.chars().count() <= KEEP {
        return version_id.to_string();
    }

    let head: String = version_id.chars().take(KEEP).collect();
    format!("{head}…")
}

#[cfg(test)]
mod tests {
    use super::{
        ObjectVersionsState, PreviewGate, evaluate_preview_gate, format_size_detail,
        is_archived_storage_class, short_version_id, versioning_tracks_history,
    };
    use crate::buckets_table::BucketDetailsState;
    use dbflux_core::{BucketDetails, ObjectMetadata, VersioningStatus};

    fn metadata(size_bytes: u64, storage_class: Option<&str>) -> ObjectMetadata {
        ObjectMetadata {
            key: "logs/app.log".to_string(),
            size_bytes,
            content_type: Some("text/plain".to_string()),
            last_modified: None,
            etag: Some("\"abc\"".to_string()),
            storage_class: storage_class.map(|class| class.to_string()),
            encryption: Some("AES256".to_string()),
            version_count: None,
        }
    }

    /// T26/T28: an object at or below the configured limit is previewable.
    #[test]
    fn gate_allows_objects_within_the_limit() {
        assert_eq!(
            evaluate_preview_gate(&metadata(1024, Some("STANDARD")), 2048),
            PreviewGate::Allowed
        );
        assert_eq!(
            evaluate_preview_gate(&metadata(2048, Some("STANDARD")), 2048),
            PreviewGate::Allowed
        );
    }

    /// T28: past the limit the gate reports the size that was refused, so the
    /// panel can explain the refusal without a second lookup.
    #[test]
    fn gate_refuses_objects_over_the_limit() {
        assert_eq!(
            evaluate_preview_gate(&metadata(4096, Some("STANDARD")), 2048),
            PreviewGate::TooLarge {
                size_bytes: 4096,
                limit_bytes: 2048,
            }
        );
    }

    /// T26: archived tiers are refused before the size check — a small
    /// GLACIER object still must not trigger a body fetch.
    #[test]
    fn gate_refuses_archived_objects_regardless_of_size() {
        assert_eq!(
            evaluate_preview_gate(&metadata(1, Some("GLACIER")), 2048),
            PreviewGate::Archived
        );
        assert_eq!(
            evaluate_preview_gate(&metadata(1, Some("deep_archive")), 2048),
            PreviewGate::Archived
        );
    }

    /// T26: infrequent-access tiers are readable without a restore.
    #[test]
    fn only_the_archived_tiers_are_treated_as_archived() {
        assert!(!is_archived_storage_class(Some("STANDARD_IA")));
        assert!(!is_archived_storage_class(Some("GLACIER_IR")));
        assert!(!is_archived_storage_class(None));
        assert!(is_archived_storage_class(Some("GLACIER")));
    }

    /// T26/T28: both refusals explain themselves; an allowed object has
    /// nothing to explain.
    #[test]
    fn gate_messages_cover_both_refusals() {
        assert_eq!(PreviewGate::Allowed.message(), None);
        assert!(
            PreviewGate::Archived
                .message()
                .is_some_and(|message| message.contains("archived"))
        );
        assert!(
            PreviewGate::TooLarge {
                size_bytes: 20 * 1024 * 1024,
                limit_bytes: 10 * 1024 * 1024,
            }
            .message()
            .is_some_and(|message| message.contains("10.0 MiB"))
        );
    }

    /// T26: the size row carries both the human-readable size and the exact
    /// byte count, grouped for readability.
    #[test]
    fn size_detail_pairs_human_and_exact_bytes() {
        assert_eq!(format_size_detail(1_468_006), "1.4 MiB (1,468,006 bytes)");
        assert_eq!(format_size_detail(512), "512 B (512 bytes)");
    }

    /// T26: the versions row is only offered when the bucket actually keeps
    /// version history.
    #[test]
    fn versions_are_offered_only_for_versioned_buckets() {
        let enabled = BucketDetailsState::Loaded(BucketDetails {
            region: "us-east-1".to_string(),
            versioning: VersioningStatus::Enabled,
        });
        let disabled = BucketDetailsState::Loaded(BucketDetails {
            region: "us-east-1".to_string(),
            versioning: VersioningStatus::Disabled,
        });

        assert!(versioning_tracks_history(&enabled));
        assert!(!versioning_tracks_history(&disabled));
        assert!(!versioning_tracks_history(&BucketDetailsState::NotLoaded));
        assert!(!versioning_tracks_history(&BucketDetailsState::Error(
            "denied".to_string()
        )));
    }

    /// T26: version ids are truncated for display but short ids stay intact.
    #[test]
    fn version_ids_are_truncated_for_display() {
        assert_eq!(short_version_id("abc123"), "abc123");
        assert_eq!(short_version_id("abcdefghijklmnopqrst"), "abcdefghijkl…");
    }

    /// T26: versions start un-fetched — selecting an object must not trigger
    /// a `list_object_versions` call on its own.
    #[test]
    fn versions_start_idle() {
        assert_eq!(ObjectVersionsState::default(), ObjectVersionsState::Idle);
    }
}
