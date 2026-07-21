//! T22 — per-table column-mapping review state: the auto-detected pairing
//! (by name, mirroring `dbflux_transfer`'s `AutoColumnMap`) rendered as an
//! adjustable list, with unmatched-source columns surfaced as a non-blocking
//! warning. Pure data model — no GPUI — so it is unit-testable without a
//! wizard entity; `mod.rs` wraps it with the live `Dropdown` controls users
//! adjust it through.

use dbflux_core::TransferColumn;
use dbflux_transfer::manifest::ManifestTable;
use dbflux_transfer::{ColumnMappingOverride, TableMappingMode};

/// Ordered (label, mode) pairs for the mapping-mode picker. `Truncate` is
/// filtered out by [`mapping_mode_options`] when the target lacks
/// `DriverCapabilities::TRUNCATE_TABLE` — unavailable, not a runtime error
/// (mirrors the `DISABLE_FK_CHECKS` missing-capability pattern from R7).
const MAPPING_MODE_OPTIONS: &[(&str, TableMappingMode)] = &[
    ("Create", TableMappingMode::Create),
    ("Existing (insert only)", TableMappingMode::Existing),
    ("Recreate (drop + create)", TableMappingMode::Recreate),
    ("Skip", TableMappingMode::Skip),
    ("Truncate (empty + insert)", TableMappingMode::Truncate),
];

pub fn mapping_mode_options(supports_truncate: bool) -> Vec<(&'static str, TableMappingMode)> {
    MAPPING_MODE_OPTIONS
        .iter()
        .copied()
        .filter(|(_, mode)| supports_truncate || *mode != TableMappingMode::Truncate)
        .collect()
}

pub fn default_mapping_mode(target_exists: bool) -> TableMappingMode {
    if target_exists {
        TableMappingMode::Existing
    } else {
        TableMappingMode::Create
    }
}

/// One manifest table's target location, mapping mode, and adjustable column
/// bindings (index into `source_columns`, per `target_columns` slot).
pub struct TableImportConfig {
    pub source_table: String,
    pub source_columns: Vec<TransferColumn>,
    pub target_schema: Option<String>,
    pub target_table: String,
    pub target_columns: Vec<TransferColumn>,
    pub target_exists: bool,
    pub mapping_mode: TableMappingMode,
    /// `bindings[target_index] == Some(source_index)` — mirrors
    /// `AutoColumnMap`'s internal shape but kept explicit here so the UI can
    /// render and adjust it directly.
    pub bindings: Vec<Option<usize>>,
}

impl TableImportConfig {
    pub fn new(
        manifest_table: &ManifestTable,
        target_exists: bool,
        target_columns: Vec<TransferColumn>,
    ) -> Self {
        let target_columns = if target_columns.is_empty() {
            manifest_table.columns.clone()
        } else {
            target_columns
        };
        let bindings = auto_map_bindings(&manifest_table.columns, &target_columns);

        Self {
            source_table: manifest_table.name.clone(),
            source_columns: manifest_table.columns.clone(),
            target_schema: manifest_table.schema.clone(),
            target_table: manifest_table.name.clone(),
            target_columns,
            target_exists,
            mapping_mode: default_mapping_mode(target_exists),
            bindings,
        }
    }

    /// Rebinds `target_index` to `source_index` (or clears it to always-NULL
    /// when `None`) — the T22 "user override replaces a pair" action.
    pub fn set_binding(&mut self, target_index: usize, source_index: Option<usize>) {
        if let Some(slot) = self.bindings.get_mut(target_index) {
            *slot = source_index;
        }
    }

    /// Source columns with no bound target column — the R5 "unmatched
    /// source" warning, non-blocking (the import still proceeds).
    pub fn unmatched_source_names(&self) -> Vec<String> {
        self.source_columns
            .iter()
            .enumerate()
            .filter(|(index, _)| !self.bindings.contains(&Some(*index)))
            .map(|(_, col)| col.name.clone())
            .collect()
    }

    pub fn is_destructive(&self) -> bool {
        matches!(
            self.mapping_mode,
            TableMappingMode::Recreate | TableMappingMode::Truncate
        )
    }

    pub fn to_overrides(&self) -> Vec<ColumnMappingOverride> {
        self.target_columns
            .iter()
            .zip(self.bindings.iter())
            .map(|(target, binding)| ColumnMappingOverride {
                target_column: target.name.clone(),
                source_column: binding
                    .and_then(|index| self.source_columns.get(index))
                    .map(|col| col.name.clone()),
            })
            .collect()
    }
}

fn auto_map_bindings(
    source_columns: &[TransferColumn],
    target_columns: &[TransferColumn],
) -> Vec<Option<usize>> {
    target_columns
        .iter()
        .map(|target| {
            source_columns
                .iter()
                .position(|src| src.name == target.name)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str) -> TransferColumn {
        TransferColumn {
            name: name.to_string(),
            type_name: Some("text".to_string()),
            nullable: true,
            is_primary_key: false,
        }
    }

    fn manifest_table(columns: Vec<TransferColumn>) -> ManifestTable {
        ManifestTable {
            schema: Some("public".to_string()),
            name: "users".to_string(),
            file: "public.users.csv".to_string(),
            format: "csv".to_string(),
            columns,
            row_count: 3,
            fk_order_index: 0,
        }
    }

    #[test]
    fn mapping_mode_options_excludes_truncate_when_unsupported() {
        let options = mapping_mode_options(false);
        assert!(
            !options
                .iter()
                .any(|(_, mode)| *mode == TableMappingMode::Truncate)
        );

        let options = mapping_mode_options(true);
        assert!(
            options
                .iter()
                .any(|(_, mode)| *mode == TableMappingMode::Truncate)
        );
    }

    #[test]
    fn default_mapping_mode_is_existing_when_target_exists_else_create() {
        assert_eq!(default_mapping_mode(true), TableMappingMode::Existing);
        assert_eq!(default_mapping_mode(false), TableMappingMode::Create);
    }

    #[test]
    fn new_config_auto_maps_by_name_and_reports_unmatched_source() {
        let table = manifest_table(vec![column("id"), column("legacy_x")]);
        let target_columns = vec![column("id"), column("y")];

        let config = TableImportConfig::new(&table, true, target_columns);

        assert_eq!(config.mapping_mode, TableMappingMode::Existing);
        assert_eq!(
            config.unmatched_source_names(),
            vec!["legacy_x".to_string()]
        );
    }

    #[test]
    fn set_binding_rebinds_a_target_column_and_clears_the_unmatched_warning() {
        let table = manifest_table(vec![column("id"), column("legacy_x")]);
        let target_columns = vec![column("id"), column("y")];
        let mut config = TableImportConfig::new(&table, true, target_columns);
        assert_eq!(
            config.unmatched_source_names(),
            vec!["legacy_x".to_string()]
        );

        // "y" (target index 1) has no default binding — bind it to
        // "legacy_x" (source index 1).
        config.set_binding(1, Some(1));

        assert!(config.unmatched_source_names().is_empty());
        let overrides = config.to_overrides();
        assert_eq!(overrides[1].target_column, "y");
        assert_eq!(overrides[1].source_column, Some("legacy_x".to_string()));
    }

    #[test]
    fn set_binding_with_none_clears_a_previously_matched_target() {
        let table = manifest_table(vec![column("id")]);
        let target_columns = vec![column("id")];
        let mut config = TableImportConfig::new(&table, true, target_columns);

        config.set_binding(0, None);

        let overrides = config.to_overrides();
        assert_eq!(overrides[0].source_column, None);
        assert_eq!(config.unmatched_source_names(), vec!["id".to_string()]);
    }

    #[test]
    fn is_destructive_is_true_only_for_recreate_and_truncate() {
        let table = manifest_table(vec![column("id")]);
        let mut config = TableImportConfig::new(&table, true, vec![column("id")]);

        config.mapping_mode = TableMappingMode::Existing;
        assert!(!config.is_destructive());
        config.mapping_mode = TableMappingMode::Recreate;
        assert!(config.is_destructive());
        config.mapping_mode = TableMappingMode::Truncate;
        assert!(config.is_destructive());
    }

    #[test]
    fn empty_target_columns_falls_back_to_manifest_columns() {
        let table = manifest_table(vec![column("id"), column("email")]);

        let config = TableImportConfig::new(&table, false, Vec::new());

        assert_eq!(config.target_columns.len(), 2);
        assert!(config.unmatched_source_names().is_empty());
    }
}
