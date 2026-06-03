use std::sync::Arc;
use std::time::Duration;

use dbflux_components::modals::{MutationConfirmHardRequest, MutationConfirmRequest};
use dbflux_core::{
    Connection, FilterNode, QueryRequest, Value, VisualMutationSpec, render_filter_node_sql,
};

/// Controls which mutation confirmation modal opens, and carries its request payload.
///
/// Used as the `pending_mutation_modal` field on `DataGridPanel`. The render cycle
/// reads this via `.take()` to open the appropriate modal entity.
#[derive(Debug)]
pub enum PendingMutationModal {
    /// Light confirmation (E-1): no type-to-confirm, no opt-in checkbox.
    Light(MutationConfirmRequest),
    /// Hard confirmation (E-2/E-3/E-4/E-6): danger variant with TypeToConfirm + opt-in.
    Hard(MutationConfirmHardRequest),
}

/// Fetches up to 5 sample rows for the mutation confirmation preview.
///
/// Builds the SELECT using the connection's dialect for correct identifier quoting and
/// placeholder style. The filter is rendered through `render_filter_node_sql` so the
/// WHERE clause is valid SQL, not a literal `<filter>` placeholder.
///
/// Returns `(column_names, rows)` on success, or an empty result on failure or timeout.
/// The deadline is 2 seconds per spec DR-9.
pub fn fetch_sample_rows(
    connection: Arc<dyn Connection>,
    spec: &VisualMutationSpec,
) -> (Vec<String>, Vec<Vec<String>>) {
    let dialect = connection.dialect();
    let qualified_table = dialect.qualified_table(spec.from.schema.as_deref(), &spec.from.name);

    let mut params: Vec<Value> = Vec::new();
    let mut param_idx: usize = 1;
    let where_clause =
        render_filter_node_sql(spec.filter.as_ref(), dialect, &mut params, &mut param_idx);

    let sql = match where_clause {
        Some(w) if !w.is_empty() => {
            format!("SELECT * FROM {} WHERE {} LIMIT 5", qualified_table, w)
        }
        _ => format!("SELECT * FROM {} LIMIT 5", qualified_table),
    };

    let (tx, rx) = std::sync::mpsc::channel::<Option<(Vec<String>, Vec<Vec<String>>)>>();

    std::thread::spawn(move || {
        let mut request = QueryRequest::new(sql);
        request.params = params;
        let result = connection.execute(&request).ok().map(|qr| {
            let col_names: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();
            let rows: Vec<Vec<String>> = qr
                .rows
                .iter()
                .map(|row| row.iter().map(|v| format!("{}", v)).collect())
                .collect();
            (col_names, rows)
        });
        // The receiver may have already timed out and been dropped; drop the send error.
        let _drop_send = tx.send(result);
    });

    match rx.recv_timeout(Duration::from_secs(2)) {
        Ok(Some(data)) => data,
        _ => (Vec::new(), Vec::new()),
    }
}

/// Returns `true` when the filter uniquely identifies a single row by primary key.
///
/// PK-unique means every PK column appears as a direct `Eq` predicate in a single
/// top-level `AND` conjunction (or as a lone top-level `Eq` for a single-column PK).
///
/// Any `OR` group at the top level disqualifies: `id = 5 OR status = 'X'` can match
/// multiple rows even if `id` is the PK. Nested groups are not traversed.
///
/// Used to determine whether a DELETE needs a hard confirmation modal or can use the
/// lighter variant (spec DR-9.1 vs DR-9.2).
pub fn filter_is_pk_unique(filter: &Option<FilterNode>, pk_cols: &[&str]) -> bool {
    use dbflux_core::{BoolOp, Comparator, FilterNode, PredicateValue};

    if pk_cols.is_empty() {
        return false;
    }

    let Some(node) = filter else {
        return false;
    };

    fn direct_eq_columns(node: &FilterNode) -> Option<Vec<String>> {
        use dbflux_core::{BoolOp, Comparator, FilterNode, PredicateValue};
        match node {
            FilterNode::Predicate(p) => {
                if matches!(p.comparator, Comparator::Eq)
                    && matches!(p.value, PredicateValue::Single(_))
                {
                    Some(vec![p.column.clone()])
                } else {
                    None
                }
            }
            FilterNode::Group { op, children } => {
                if *op == BoolOp::Or {
                    return None;
                }
                let mut cols = Vec::new();
                for child in children {
                    match child {
                        FilterNode::Predicate(p)
                            if matches!(p.comparator, Comparator::Eq)
                                && matches!(p.value, PredicateValue::Single(_)) =>
                        {
                            cols.push(p.column.clone());
                        }
                        _ => {
                            return None;
                        }
                    }
                }
                Some(cols)
            }
        }
    }

    let eq_cols = match direct_eq_columns(node) {
        Some(cols) => cols,
        None => return false,
    };

    pk_cols
        .iter()
        .all(|pk| eq_cols.iter().any(|ec| ec.eq_ignore_ascii_case(pk)))
}

/// Builds a `PendingMutationModal` for the given spec and estimated row count.
///
/// Selects `Light` (spec DR-9.1) when the spec is a DELETE and the filter uniquely
/// identifies a single row by primary key (all PK columns with equality predicates)
/// and `est_rows == Some(1)`. Uses `Hard` for all other cases.
pub fn build_pending_modal(
    spec: &VisualMutationSpec,
    sql_preview: String,
    est_rows: Option<u64>,
    sample_columns: Vec<String>,
    sample_rows: Option<Vec<Vec<String>>>,
    pk_cols: &[&str],
) -> PendingMutationModal {
    use dbflux_core::MutationKind;

    let table_name = spec.from.name.clone();
    let is_delete = matches!(spec.kind, MutationKind::Delete);

    let pk_unique_delete =
        is_delete && est_rows == Some(1) && filter_is_pk_unique(&spec.filter, pk_cols);

    let use_hard = if pk_unique_delete {
        false
    } else {
        is_delete || est_rows.map(|n| n > 1).unwrap_or(true)
    };

    let summary = match &spec.kind {
        MutationKind::Delete => {
            let row_desc = match est_rows {
                Some(n) => format!("{} rows", n),
                None => "rows".to_string(),
            };
            format!("Delete {} from \"{}\"", row_desc, table_name)
        }
        MutationKind::Update { assignments } => {
            let col_count = assignments.len();
            format!(
                "Update {} column{} in \"{}\"",
                col_count,
                if col_count == 1 { "" } else { "s" },
                table_name
            )
        }
    };

    if use_hard {
        PendingMutationModal::Hard(MutationConfirmHardRequest {
            summary,
            type_to_confirm: table_name,
            sql_preview,
            sample_rows,
            sample_columns,
            require_opt_in: true,
        })
    } else {
        PendingMutationModal::Light(MutationConfirmRequest {
            summary,
            sql_preview,
            sample_rows,
            sample_columns,
        })
    }
}

/// Formats a `Value` for display in the sample-rows preview.
///
/// Exposed here so it can be used without importing `Value`'s `Display` impl.
#[allow(dead_code)]
pub fn format_value(value: &Value) -> String {
    format!("{}", value)
}

#[cfg(test)]
mod tests {
    use dbflux_core::{BoolOp, Comparator, FilterNode, LiteralValue, Predicate, PredicateValue};

    use super::filter_is_pk_unique;

    fn pred_eq(col: &str) -> FilterNode {
        FilterNode::Predicate(Predicate {
            source_alias: "t".to_string(),
            column: col.to_string(),
            comparator: Comparator::Eq,
            value: PredicateValue::Single(LiteralValue::Integer(1)),
            node_id: 0,
        })
    }

    fn pred_ne(col: &str) -> FilterNode {
        FilterNode::Predicate(Predicate {
            source_alias: "t".to_string(),
            column: col.to_string(),
            comparator: Comparator::Neq,
            value: PredicateValue::Single(LiteralValue::Integer(1)),
            node_id: 0,
        })
    }

    fn or_group(children: Vec<FilterNode>) -> FilterNode {
        FilterNode::Group {
            op: BoolOp::Or,
            children,
        }
    }

    fn and_group(children: Vec<FilterNode>) -> FilterNode {
        FilterNode::Group {
            op: BoolOp::And,
            children,
        }
    }

    // DR-9.1: OR group at top level must NOT be considered PK-unique.
    //   filter = id = 5 OR status = 'X', pk_cols = ["id"] → false
    #[test]
    fn filter_with_or_group_containing_pk_eq_is_not_pk_unique() {
        let filter = Some(or_group(vec![pred_eq("id"), pred_eq("status")]));
        assert!(
            !filter_is_pk_unique(&filter, &["id"]),
            "OR group must not be considered PK-unique even if PK col has Eq predicate"
        );
    }

    // DR-9.1: AND group with all PK cols as direct Eq children → true.
    //   filter = a = 1 AND b = 2, pk_cols = ["a", "b"] → true
    #[test]
    fn filter_with_and_group_containing_all_pk_cols_eq_is_pk_unique() {
        let filter = Some(and_group(vec![pred_eq("a"), pred_eq("b")]));
        assert!(
            filter_is_pk_unique(&filter, &["a", "b"]),
            "AND group with all PK cols as direct Eq children must be PK-unique"
        );
    }

    // DR-9.1: AND group missing one PK col → false.
    //   filter = a = 1 AND c = 3, pk_cols = ["a", "b"] → false
    #[test]
    fn filter_with_and_group_missing_pk_col_is_not_pk_unique() {
        let filter = Some(and_group(vec![pred_eq("a"), pred_eq("c")]));
        assert!(
            !filter_is_pk_unique(&filter, &["a", "b"]),
            "AND group missing PK col 'b' must not be PK-unique"
        );
    }

    // Single Eq predicate matches 1-column PK → true.
    #[test]
    fn single_eq_predicate_for_single_pk_col_is_pk_unique() {
        let filter = Some(pred_eq("id"));
        assert!(filter_is_pk_unique(&filter, &["id"]));
    }

    // Single non-Eq predicate for PK → false.
    #[test]
    fn single_neq_predicate_is_not_pk_unique() {
        let filter = Some(pred_ne("id"));
        assert!(!filter_is_pk_unique(&filter, &["id"]));
    }

    // None filter is never PK-unique.
    #[test]
    fn none_filter_is_not_pk_unique() {
        assert!(!filter_is_pk_unique(&None, &["id"]));
    }

    // Empty pk_cols → never PK-unique.
    #[test]
    fn empty_pk_cols_is_not_pk_unique() {
        let filter = Some(pred_eq("id"));
        assert!(!filter_is_pk_unique(&filter, &[]));
    }

    // AND group where one child is a non-Eq predicate → false (mixed group rejected).
    #[test]
    fn and_group_with_non_eq_child_is_not_pk_unique() {
        let filter = Some(and_group(vec![pred_eq("id"), pred_ne("status")]));
        assert!(
            !filter_is_pk_unique(&filter, &["id"]),
            "AND group with non-Eq child must not be PK-unique"
        );
    }
}
