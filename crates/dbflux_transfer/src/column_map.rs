//! Automatic source-to-target column mapping.

use dbflux_core::{TransferColumn, Value};

use crate::pipeline::ColumnMap;

/// Resolves a source-to-target column mapping once, by name, and projects
/// rows accordingly.
///
/// Unmatched source columns are dropped and recorded as a non-blocking
/// warning (surfaced once via [`crate::pipeline::TransferReport::warnings`]).
/// Unmatched target columns receive `Value::Null` for every row; this is not
/// treated as a warning since the target side commonly has columns the
/// source table does not (e.g. an auto-populated audit column).
pub struct AutoColumnMap {
    /// For each target column, in target order: the index into a source row
    /// to read, or `None` when no source column matched.
    target_from_source: Vec<Option<usize>>,
    /// The target column shape `project` emits values into, in that same
    /// order — what the sink must be `begin()`-ed with.
    target_columns: Vec<TransferColumn>,
    warnings: Vec<String>,
}

/// One user-adjusted column pairing from the Import column-mapping review
/// step (T22): binds `target_column` to `source_column`, or clears it to
/// always-`NULL` when `source_column` is `None` — overriding whatever the
/// by-name auto-map resolved for that target column. Columns not mentioned
/// here keep their auto-detected pairing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMappingOverride {
    pub target_column: String,
    pub source_column: Option<String>,
}

impl AutoColumnMap {
    pub fn new(source_columns: &[TransferColumn], target_columns: &[TransferColumn]) -> Self {
        let target_from_source = auto_map(source_columns, target_columns);
        let warnings = unmatched_source_warnings(source_columns, &target_from_source);

        Self {
            target_from_source,
            target_columns: target_columns.to_vec(),
            warnings,
        }
    }

    /// Builds the by-name auto-map, then applies `overrides` on top — each
    /// override replaces one target column's source binding regardless of
    /// what auto-mapping resolved for it. Unmatched-source warnings are
    /// recomputed from the final (post-override) mapping, so a source
    /// column an override rescues out of "unmatched" no longer warns.
    pub fn with_overrides(
        source_columns: &[TransferColumn],
        target_columns: &[TransferColumn],
        overrides: &[ColumnMappingOverride],
    ) -> Self {
        let mut target_from_source = auto_map(source_columns, target_columns);

        for override_entry in overrides {
            let Some(target_index) = target_columns
                .iter()
                .position(|c| c.name == override_entry.target_column)
            else {
                continue;
            };

            #[expect(
                clippy::indexing_slicing,
                reason = "target_index is a position within `target_columns` and auto_map returns exactly one entry per target column"
            )]
            {
                target_from_source[target_index] = override_entry
                    .source_column
                    .as_ref()
                    .and_then(|name| source_columns.iter().position(|c| &c.name == name));
            }
        }

        let warnings = unmatched_source_warnings(source_columns, &target_from_source);

        Self {
            target_from_source,
            target_columns: target_columns.to_vec(),
            warnings,
        }
    }
}

impl AutoColumnMap {
    /// `source_columns` with each one that maps onto a target column taking
    /// that column's type name, so values read from a file without type
    /// names decode as the column they land in. A source column no target
    /// column reads keeps its own type name.
    pub(crate) fn typed_source_columns(
        &self,
        source_columns: &[TransferColumn],
    ) -> Vec<TransferColumn> {
        source_columns
            .iter()
            .enumerate()
            .map(|(source_index, source)| {
                let mut target_types = self
                    .target_from_source
                    .iter()
                    .zip(&self.target_columns)
                    .filter(|(bound, _)| **bound == Some(source_index))
                    .map(|(_, target)| &target.type_name);

                // A source decoded under one target's type would hand that
                // decoded value to every target, so a source bound to targets
                // of different types keeps its own type and each target
                // receives the raw value.
                let type_name = match target_types.next() {
                    None => source.type_name.clone(),
                    Some(first) if target_types.all(|other| other == first) => first.clone(),
                    Some(_) => source.type_name.clone(),
                };

                TransferColumn {
                    type_name,
                    ..source.clone()
                }
            })
            .collect()
    }
}

fn auto_map(
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

fn unmatched_source_warnings(
    source_columns: &[TransferColumn],
    target_from_source: &[Option<usize>],
) -> Vec<String> {
    source_columns
        .iter()
        .enumerate()
        .filter(|(index, _)| !target_from_source.contains(&Some(*index)))
        .map(|(_, src)| {
            format!(
                "source column '{}' has no matching target column and was skipped",
                src.name
            )
        })
        .collect()
}

impl ColumnMap for AutoColumnMap {
    fn project(&self, src: &[Value]) -> Vec<Value> {
        self.target_from_source
            .iter()
            .map(|source_index| match source_index {
                Some(index) => src.get(*index).cloned().unwrap_or(Value::Null),
                None => Value::Null,
            })
            .collect()
    }

    fn target_columns(&self) -> &[TransferColumn] {
        &self.target_columns
    }

    fn warnings(&self) -> &[String] {
        &self.warnings
    }
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

    #[test]
    fn unmatched_source_column_is_skipped_and_warned() {
        let source = vec![column("a"), column("b"), column("x")];
        let target = vec![column("a"), column("b")];

        let map = AutoColumnMap::new(&source, &target);
        let projected = map.project(&[Value::Int(1), Value::Int(2), Value::Int(3)]);

        assert_eq!(projected, vec![Value::Int(1), Value::Int(2)]);
        assert_eq!(map.warnings().len(), 1);
        assert!(map.warnings()[0].contains('x'));
    }

    #[test]
    fn unmatched_target_column_gets_null_with_no_warning() {
        let source = vec![column("a"), column("b")];
        let target = vec![column("a"), column("b"), column("y")];

        let map = AutoColumnMap::new(&source, &target);
        let projected = map.project(&[Value::Int(1), Value::Int(2)]);

        assert_eq!(projected, vec![Value::Int(1), Value::Int(2), Value::Null]);
        assert!(map.warnings().is_empty());
    }

    #[test]
    fn override_rebinds_a_target_column_to_an_explicit_source_column() {
        let source = vec![column("first_name"), column("last_name")];
        let target = vec![column("name")];
        let overrides = vec![ColumnMappingOverride {
            target_column: "name".to_string(),
            source_column: Some("first_name".to_string()),
        }];

        let map = AutoColumnMap::with_overrides(&source, &target, &overrides);
        let projected = map.project(&[
            Value::Text("Ada".to_string()),
            Value::Text("Lovelace".to_string()),
        ]);

        assert_eq!(projected, vec![Value::Text("Ada".to_string())]);
    }

    #[test]
    fn override_with_no_source_clears_the_target_column_to_null() {
        let source = vec![column("a")];
        let target = vec![column("a")];
        let overrides = vec![ColumnMappingOverride {
            target_column: "a".to_string(),
            source_column: None,
        }];

        let map = AutoColumnMap::with_overrides(&source, &target, &overrides);
        let projected = map.project(&[Value::Int(1)]);

        assert_eq!(projected, vec![Value::Null]);
    }

    #[test]
    fn override_rescuing_an_unmatched_source_column_into_a_spare_target_clears_its_warning() {
        // "a" auto-matches by name; "legacy_a" has no match, so it warns
        // until an override binds it into the otherwise-unused "extra" slot.
        let source = vec![column("a"), column("legacy_a")];
        let target = vec![column("a"), column("extra")];
        let overrides = vec![ColumnMappingOverride {
            target_column: "extra".to_string(),
            source_column: Some("legacy_a".to_string()),
        }];

        let map = AutoColumnMap::with_overrides(&source, &target, &overrides);

        assert!(
            map.warnings().is_empty(),
            "the rebound source column must no longer warn as unmatched: {:?}",
            map.warnings()
        );
    }

    #[test]
    fn override_naming_an_unknown_target_column_is_ignored() {
        let source = vec![column("a")];
        let target = vec![column("a")];
        let overrides = vec![ColumnMappingOverride {
            target_column: "does_not_exist".to_string(),
            source_column: Some("a".to_string()),
        }];

        let map = AutoColumnMap::with_overrides(&source, &target, &overrides);
        let projected = map.project(&[Value::Int(7)]);

        assert_eq!(projected, vec![Value::Int(7)]);
    }

    #[test]
    fn matched_columns_project_in_target_order_regardless_of_source_order() {
        let source = vec![column("b"), column("a")];
        let target = vec![column("a"), column("b")];

        let map = AutoColumnMap::new(&source, &target);
        // Source row is [b_value, a_value] per source column order.
        let projected = map.project(&[
            Value::Text("b_value".to_string()),
            Value::Text("a_value".to_string()),
        ]);

        assert_eq!(
            projected,
            vec![
                Value::Text("a_value".to_string()),
                Value::Text("b_value".to_string())
            ]
        );
        assert!(map.warnings().is_empty());
    }

    fn typed(name: &str, type_name: Option<&str>) -> TransferColumn {
        TransferColumn {
            name: name.to_string(),
            type_name: type_name.map(str::to_string),
            nullable: true,
            is_primary_key: false,
        }
    }

    #[test]
    fn source_columns_take_the_type_of_the_target_column_they_map_onto() {
        let source = vec![
            typed("a", None),
            typed("b", None),
            typed("loose", Some("text")),
        ];
        let target = vec![typed("b", Some("integer")), typed("a", Some("real"))];

        let typed_sources = AutoColumnMap::new(&source, &target).typed_source_columns(&source);

        assert_eq!(
            typed_sources
                .iter()
                .map(|c| (c.name.as_str(), c.type_name.as_deref()))
                .collect::<Vec<_>>(),
            vec![
                ("a", Some("real")),
                ("b", Some("integer")),
                ("loose", Some("text"))
            ],
            "an unmapped source column keeps its own type"
        );
    }

    /// A source column bound to targets of different types keeps its own
    /// (absent) type, so its raw text reaches every target instead of being
    /// decoded once under the first target's type.
    #[test]
    fn a_source_column_feeding_targets_of_different_types_stays_untyped() {
        let source = vec![typed("code", None)];
        let target = vec![
            typed("numeric_code", Some("integer")),
            typed("original_code", Some("text")),
        ];
        let overrides = vec![
            ColumnMappingOverride {
                target_column: "numeric_code".to_string(),
                source_column: Some("code".to_string()),
            },
            ColumnMappingOverride {
                target_column: "original_code".to_string(),
                source_column: Some("code".to_string()),
            },
        ];

        let typed_sources = AutoColumnMap::with_overrides(&source, &target, &overrides)
            .typed_source_columns(&source);

        assert_eq!(typed_sources[0].type_name, None);
    }

    #[test]
    fn a_source_column_feeding_targets_of_the_same_type_takes_that_type() {
        let source = vec![typed("code", None)];
        let target = vec![typed("a", Some("integer")), typed("b", Some("integer"))];
        let overrides = vec![
            ColumnMappingOverride {
                target_column: "a".to_string(),
                source_column: Some("code".to_string()),
            },
            ColumnMappingOverride {
                target_column: "b".to_string(),
                source_column: Some("code".to_string()),
            },
        ];

        let typed_sources = AutoColumnMap::with_overrides(&source, &target, &overrides)
            .typed_source_columns(&source);

        assert_eq!(typed_sources[0].type_name.as_deref(), Some("integer"));
    }

    #[test]
    fn an_override_decides_which_target_type_a_source_column_takes() {
        let source = vec![typed("first_name", None)];
        let target = vec![typed("name", Some("varchar(40)"))];
        let overrides = vec![ColumnMappingOverride {
            target_column: "name".to_string(),
            source_column: Some("first_name".to_string()),
        }];

        let typed_sources = AutoColumnMap::with_overrides(&source, &target, &overrides)
            .typed_source_columns(&source);

        assert_eq!(typed_sources[0].type_name.as_deref(), Some("varchar(40)"));
    }
}
