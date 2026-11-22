use crate::TableInfo;

/// How the primary key a grid edits by compares with a fresh read of its table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrimaryKeyCheck {
    /// The fresh key holds the same columns as the grid's.
    Same,
    /// The table's key is no longer the grid's. `after` is empty when the key
    /// was dropped.
    Changed {
        before: Vec<String>,
        after: Vec<String>,
    },
    /// The fresh read lists no columns, which a real table never does: the
    /// table is gone or no longer resolves.
    TableGone,
}

/// Compare the primary key a grid saves by (`grid_key`) with the columns of
/// the table as the database reports it now. Column order does not matter:
/// a row identity is a set of named values.
pub fn check_primary_key(grid_key: &[String], fresh: &TableInfo) -> PrimaryKeyCheck {
    let columns = match fresh.columns.as_deref() {
        Some(columns) if !columns.is_empty() => columns,
        _ => return PrimaryKeyCheck::TableGone,
    };

    let after: Vec<String> = columns
        .iter()
        .filter(|column| column.is_primary_key)
        .map(|column| column.name.clone())
        .collect();

    let mut sorted_grid = grid_key.to_vec();
    let mut sorted_fresh = after.clone();
    sorted_grid.sort_unstable();
    sorted_fresh.sort_unstable();

    if sorted_grid == sorted_fresh {
        PrimaryKeyCheck::Same
    } else {
        PrimaryKeyCheck::Changed {
            before: grid_key.to_vec(),
            after,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnInfo, TableInfo};

    fn col(name: &str, type_name: &str, nullable: bool, is_pk: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            type_name: type_name.to_string(),
            nullable,
            is_primary_key: is_pk,
            default_value: None,
            enum_values: None,
        }
    }

    fn make_table(name: &str, columns: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            schema: Some("public".to_string()),
            columns: Some(columns),
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: Default::default(),
            child_items: None,
            storage_hints: None,
            pseudo_columns: Box::default(),
        }
    }

    // --- check_primary_key ---

    fn names(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn primary_key_check_same_when_the_grid_key_matches() {
        let fresh = make_table(
            "orders",
            vec![
                col("id", "integer", false, true),
                col("name", "text", true, false),
            ],
        );
        assert_eq!(
            check_primary_key(&names(&["id"]), &fresh),
            PrimaryKeyCheck::Same
        );
    }

    #[test]
    fn primary_key_check_ignores_key_column_order() {
        let fresh = make_table(
            "orders",
            vec![
                col("tenant", "integer", false, true),
                col("id", "integer", false, true),
            ],
        );
        assert_eq!(
            check_primary_key(&names(&["id", "tenant"]), &fresh),
            PrimaryKeyCheck::Same
        );
    }

    #[test]
    fn primary_key_check_reports_a_different_key() {
        let fresh = make_table(
            "orders",
            vec![
                col("id", "integer", false, false),
                col("code", "text", false, true),
            ],
        );
        assert_eq!(
            check_primary_key(&names(&["id"]), &fresh),
            PrimaryKeyCheck::Changed {
                before: names(&["id"]),
                after: names(&["code"]),
            }
        );
    }

    #[test]
    fn primary_key_check_reports_a_dropped_key() {
        let fresh = make_table("orders", vec![col("id", "integer", false, false)]);
        assert_eq!(
            check_primary_key(&names(&["id"]), &fresh),
            PrimaryKeyCheck::Changed {
                before: names(&["id"]),
                after: Vec::new(),
            }
        );
    }

    #[test]
    fn primary_key_check_reports_a_table_without_columns_as_gone() {
        let mut fresh = make_table("orders", Vec::new());
        assert_eq!(
            check_primary_key(&names(&["id"]), &fresh),
            PrimaryKeyCheck::TableGone
        );

        fresh.columns = None;
        assert_eq!(
            check_primary_key(&names(&["id"]), &fresh),
            PrimaryKeyCheck::TableGone
        );
    }
}
