//! `TransferManifest`: the `manifest.json` written once per export folder,
//! describing every table exported so Import (a later slice) can recreate
//! tables, load order, and column shapes without re-querying the source.

use dbflux_core::TransferColumn;
use serde::{Deserialize, Serialize};

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
    /// `TransferManifest`/`ManifestTable` changes in a way Import must branch on.
    pub const CURRENT_VERSION: u32 = 1;
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
}
