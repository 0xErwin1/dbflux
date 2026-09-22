use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{TableCreationMetadata, TableInfo};

/// How much detail a captured [`SchemaSnapshotRecord`] carries per table.
///
/// `Shallow` snapshots reuse the already-loaded table list from
/// `Connection::schema()` (no extra driver queries); `Deep` snapshots
/// back-fill full column/index/constraint detail via `table_details()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotDepth {
    Shallow,
    Deep,
}

/// A persisted point-in-time capture of a relational schema.
///
/// Distinct from [`crate::SchemaSnapshot`] (the live-connection schema tree
/// returned by `Connection::schema()`): this type is durable, keyed by
/// profile/database, and carries a stable digest (`fingerprint`) usable for
/// cross-process/cross-build deduplication via
/// [`crate::SchemaFingerprint::stable_hex`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSnapshotRecord {
    pub id: Uuid,
    pub profile_id: Uuid,
    pub database: Option<String>,
    /// Unix epoch milliseconds when the snapshot was captured.
    pub captured_at: i64,
    /// Combined stable digest over `tables`, used for dedup against the
    /// most recently stored snapshot for the same profile/database.
    pub fingerprint: String,
    pub depth: SnapshotDepth,
    pub tables: Vec<TableInfo>,
    /// Structured creation metadata captured with `Deep` snapshots, kept
    /// separate from `tables` because `TableInfo` is a legacy wire shape.
    /// Each entry carries its own `(schema, table)` identity components; it
    /// must be associated to table rows by those components, never by a
    /// concatenated key. Shallow captures carry an empty vec.
    #[serde(default)]
    pub creation_metadata: Vec<TableCreationMetadata>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IdentitySpec, MetadataCompleteness};

    #[test]
    fn snapshot_depth_round_trips_through_serde() {
        for depth in [SnapshotDepth::Shallow, SnapshotDepth::Deep] {
            let json = serde_json::to_string(&depth).expect("serialize");
            let back: SnapshotDepth = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(depth, back);
        }
    }

    #[test]
    fn snapshot_depth_serializes_to_snake_case() {
        assert_eq!(
            serde_json::to_string(&SnapshotDepth::Shallow).unwrap(),
            "\"shallow\""
        );
        assert_eq!(
            serde_json::to_string(&SnapshotDepth::Deep).unwrap(),
            "\"deep\""
        );
    }

    #[test]
    fn schema_snapshot_record_round_trips_through_serde() {
        let record = SchemaSnapshotRecord {
            id: Uuid::now_v7(),
            profile_id: Uuid::now_v7(),
            database: Some("app_db".to_string()),
            captured_at: 1_700_000_000_000,
            fingerprint: "abc123".to_string(),
            depth: SnapshotDepth::Shallow,
            tables: vec![TableInfo {
                name: "users".to_string(),
                schema: Some("public".to_string()),
                columns: None,
                indexes: None,
                foreign_keys: None,
                constraints: None,
                sample_fields: None,
                presentation: Default::default(),
                child_items: None,
                storage_hints: None,
            }],
            creation_metadata: Vec::new(),
        };

        let json = serde_json::to_string(&record).expect("serialize");
        let back: SchemaSnapshotRecord = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(back.id, record.id);
        assert_eq!(back.profile_id, record.profile_id);
        assert_eq!(back.database, record.database);
        assert_eq!(back.fingerprint, record.fingerprint);
        assert_eq!(back.depth, record.depth);
        assert_eq!(back.tables.len(), 1);
        assert_eq!(back.tables[0].name, "users");
        assert!(back.creation_metadata.is_empty());
    }

    #[test]
    fn snapshot_record_json_without_metadata_field_still_loads() {
        // Shape persisted by builds before creation metadata existed; the
        // field is serde-default so old JSON stays readable.
        let legacy_json = serde_json::json!({
            "id": Uuid::now_v7(),
            "profile_id": Uuid::now_v7(),
            "database": "app_db",
            "captured_at": 1_700_000_000_000i64,
            "fingerprint": "abc123",
            "depth": "shallow",
            "tables": [],
        });

        let record: SchemaSnapshotRecord =
            serde_json::from_value(legacy_json).expect("deserialize legacy record");

        assert!(record.creation_metadata.is_empty());
        assert_eq!(record.depth, SnapshotDepth::Shallow);
    }

    #[test]
    fn snapshot_record_roundtrips_creation_metadata() {
        let record = SchemaSnapshotRecord {
            id: Uuid::now_v7(),
            profile_id: Uuid::now_v7(),
            database: Some("app_db".to_string()),
            captured_at: 1_700_000_000_000,
            fingerprint: "abc123".to_string(),
            depth: SnapshotDepth::Deep,
            tables: vec![],
            creation_metadata: vec![TableCreationMetadata {
                schema: Some("sales".to_string()),
                table: "orders".to_string(),
                completeness: MetadataCompleteness::Complete,
                identity: Some(IdentitySpec {
                    column: "id".to_string(),
                    seed: "99999999999999999999999999999999999999".to_string(),
                    increment: "1".to_string(),
                }),
                primary_key: None,
                blockers: Vec::new(),
            }],
        };

        let json = serde_json::to_string(&record).expect("serialize");
        let back: SchemaSnapshotRecord = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(back.creation_metadata, record.creation_metadata);
    }
}
