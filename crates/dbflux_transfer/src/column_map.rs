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
    warnings: Vec<String>,
}

impl AutoColumnMap {
    pub fn new(source_columns: &[TransferColumn], target_columns: &[TransferColumn]) -> Self {
        let target_from_source = target_columns
            .iter()
            .map(|target| {
                source_columns
                    .iter()
                    .position(|src| src.name == target.name)
            })
            .collect();

        let warnings = source_columns
            .iter()
            .filter(|src| !target_columns.iter().any(|target| target.name == src.name))
            .map(|src| {
                format!(
                    "source column '{}' has no matching target column and was skipped",
                    src.name
                )
            })
            .collect();

        Self {
            target_from_source,
            warnings,
        }
    }
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
}
