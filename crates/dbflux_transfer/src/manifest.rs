//! `TransferManifest`: the `manifest.json` written once per export folder,
//! describing every table exported so Import can recreate tables, load
//! order, and column shapes without re-querying the source.

use std::path::Path;

use dbflux_core::TransferColumn;
use serde::{Deserialize, Serialize};

use crate::pipeline::TransferError;

/// Top-level `manifest.json` document for one export folder.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransferManifest {
    pub version: u32,
    pub source: ManifestSource,
    /// RFC 3339 timestamp of when the export ran.
    pub created_at: String,
    pub tables: Vec<ManifestTable>,
}

impl TransferManifest {
    /// Manifest schema version written by this build. Bump when the shape of
    /// `TransferManifest`/`ManifestTable` changes in a way Import must branch
    /// on, or when the on-disk table file format changes incompatibly (e.g.
    /// v1 -> v2: JSON table files switched from a single top-level array to
    /// NDJSON) — `read_manifest` rejects any other version outright rather
    /// than letting `FileSource` silently misparse an old bundle.
    pub const CURRENT_VERSION: u32 = 2;
}

/// Identifies the connection an export was taken from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestSource {
    pub driver: String,
    pub database: String,
    pub schema: Option<String>,
}

/// One exported table: enough for Import to recreate it, load its file in the
/// right position relative to other tables, and detect column shape drift.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestTable {
    pub schema: Option<String>,
    pub name: String,
    /// File name relative to the manifest, e.g. `public.users.csv`.
    pub file: String,
    /// File format extension, e.g. `"csv"` or `"json"`.
    pub format: String,
    pub columns: Vec<TransferColumn>,
    pub row_count: u64,
    /// Position in FK load order (parents before children). For this slice's
    /// Export flow (no cross-table ordering constraint) this is simply the
    /// table's position in the export list.
    pub fk_order_index: usize,
}

/// Reads and parses `manifest.json` at `path`. Import (T20/R3) calls this
/// first, before touching any target table — a missing or malformed
/// manifest must fail the whole import with zero writes, not partway
/// through loading tables. Also rejects a manifest whose `version` does not
/// match [`TransferManifest::CURRENT_VERSION`], so an older-format bundle
/// (e.g. v1's single-array JSON table files) fails fast instead of being
/// silently misread as the current NDJSON format.
pub fn read_manifest(path: &Path) -> Result<TransferManifest, TransferError> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| TransferError::Source(format!("{}: {e}", path.display())))?;

    let manifest: TransferManifest = serde_json::from_str(&contents)
        .map_err(|e| TransferError::Source(format!("{}: invalid manifest: {e}", path.display())))?;

    if manifest.version != TransferManifest::CURRENT_VERSION {
        return Err(TransferError::Source(format!(
            "{}: unsupported manifest version {} (this build reads version {})",
            path.display(),
            manifest.version,
            TransferManifest::CURRENT_VERSION
        )));
    }

    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest() -> TransferManifest {
        TransferManifest {
            version: TransferManifest::CURRENT_VERSION,
            source: ManifestSource {
                driver: "postgres".to_string(),
                database: "app".to_string(),
                schema: Some("public".to_string()),
            },
            created_at: "2026-07-07T10:00:00+00:00".to_string(),
            tables: vec![ManifestTable {
                schema: Some("public".to_string()),
                name: "users".to_string(),
                file: "public.users.csv".to_string(),
                format: "csv".to_string(),
                columns: vec![
                    TransferColumn {
                        name: "id".to_string(),
                        type_name: Some("int4".to_string()),
                        nullable: false,
                        is_primary_key: true,
                    },
                    TransferColumn {
                        name: "email".to_string(),
                        type_name: Some("text".to_string()),
                        nullable: true,
                        is_primary_key: false,
                    },
                ],
                row_count: 42,
                fk_order_index: 0,
            }],
        }
    }

    #[test]
    fn manifest_round_trips_through_json() {
        let manifest = sample_manifest();

        let json = serde_json::to_string_pretty(&manifest).expect("serialize manifest");
        let round_tripped: TransferManifest =
            serde_json::from_str(&json).expect("deserialize manifest");

        assert_eq!(round_tripped, manifest);
    }

    #[test]
    fn manifest_with_no_tables_round_trips() {
        let manifest = TransferManifest {
            version: TransferManifest::CURRENT_VERSION,
            source: ManifestSource {
                driver: "sqlite".to_string(),
                database: "main".to_string(),
                schema: None,
            },
            created_at: "2026-07-07T10:00:00+00:00".to_string(),
            tables: Vec::new(),
        };

        let json = serde_json::to_string(&manifest).expect("serialize manifest");
        let round_tripped: TransferManifest =
            serde_json::from_str(&json).expect("deserialize manifest");

        assert_eq!(round_tripped, manifest);
    }

    fn temp_manifest_path(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "dbflux_transfer_read_manifest_test_{label}_{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir.join("manifest.json")
    }

    #[test]
    fn read_manifest_fails_fast_when_the_file_is_missing() {
        let path = temp_manifest_path("missing");
        std::fs::remove_file(&path).ok();

        let result = read_manifest(&path);

        assert!(result.is_err());
    }

    #[test]
    fn read_manifest_fails_fast_when_the_file_is_malformed() {
        let path = temp_manifest_path("malformed");
        std::fs::write(&path, "{ not valid json").expect("write malformed manifest");

        let result = read_manifest(&path);

        assert!(result.is_err());
        std::fs::remove_file(&path).ok();
    }

    /// JD-W1 regression: an older-format manifest (e.g. v1's single-array
    /// JSON table files) must be rejected outright rather than silently
    /// misread as the current NDJSON format.
    #[test]
    fn read_manifest_rejects_a_mismatched_version() {
        let path = temp_manifest_path("mismatched_version");
        let mut manifest = sample_manifest();
        manifest.version = TransferManifest::CURRENT_VERSION - 1;
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&manifest).expect("serialize manifest"),
        )
        .expect("write manifest");

        let result = read_manifest(&path);

        assert!(
            result.is_err(),
            "a manifest version mismatch must fail fast, not be silently misread"
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn read_manifest_parses_a_valid_manifest() {
        let path = temp_manifest_path("valid");
        let manifest = sample_manifest();
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&manifest).expect("serialize manifest"),
        )
        .expect("write manifest");

        let parsed = read_manifest(&path).expect("read_manifest must succeed");

        assert_eq!(parsed, manifest);
        std::fs::remove_file(&path).ok();
    }
}
