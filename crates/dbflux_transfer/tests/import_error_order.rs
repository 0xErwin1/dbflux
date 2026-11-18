//! Folder import reports a missing table file before a missing target table,
//! as it did before loose-file import shared this code.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use dbflux_core::{CancelToken, Connection, ConnectionProfile, DbConfig, DbDriver, TransferColumn};
use dbflux_driver_sqlite::SqliteDriver;
use dbflux_export::FormatOptions;
use dbflux_transfer::import::{ImportOptions, ImportTablePlan, run_import};
use dbflux_transfer::manifest::{ManifestSource, ManifestTable, TransferManifest};
use dbflux_transfer::{TableMappingMode, TableTransferStatus};

#[test]
fn a_missing_table_file_is_reported_before_a_missing_target_table() {
    let dir = std::env::temp_dir().join(format!("import_error_order_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let manifest = TransferManifest {
        version: TransferManifest::CURRENT_VERSION,
        source: ManifestSource {
            driver: "sqlite".into(),
            database: "main".into(),
            schema: None,
        },
        created_at: "2026-01-01T00:00:00Z".into(),
        tables: vec![ManifestTable {
            schema: None,
            name: "p".into(),
            file: "absent.csv".into(),
            format: "csv".into(),
            columns: vec![TransferColumn {
                name: "a".into(),
                type_name: None,
                nullable: true,
                is_primary_key: false,
            }],
            row_count: 1,
            fk_order_index: 0,
            options: FormatOptions::default(),
        }],
    };
    std::fs::write(
        dir.join("manifest.json"),
        serde_json::to_string(&manifest).unwrap(),
    )
    .unwrap();
    let profile = ConnectionProfile::new(
        "p",
        DbConfig::SQLite {
            path: dir.join("t.db"),
            connection_id: None,
        },
    );
    let connection: Arc<dyn Connection> = Arc::from(SqliteDriver::new().connect(&profile).unwrap());

    for mode in [TableMappingMode::Existing, TableMappingMode::Truncate] {
        let plans = vec![ImportTablePlan {
            source_table: "p".into(),
            target_schema: None,
            target_table: "nosuch".into(),
            mapping_mode: mode,
            column_overrides: None,
        }];
        let options = ImportOptions {
            segment_size: 10,
            target_database: "main".into(),
            destructive_confirmed: true,
        };
        let outcome = run_import(
            &connection,
            &dir,
            &plans,
            &options,
            &CancelToken::new(),
            |_, _, _| {},
        )
        .unwrap();
        let TableTransferStatus::Failed { error } = &outcome.tables[0].status else {
            panic!("{mode:?} must fail");
        };
        assert!(
            error.starts_with("source error:") && error.contains("absent.csv"),
            "{mode:?}: {error}"
        );
    }

    std::fs::remove_dir_all(&dir).ok();
}
