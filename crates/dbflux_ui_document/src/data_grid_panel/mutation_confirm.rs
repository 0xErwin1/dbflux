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
/// The filter must contain equality predicates (`Eq` comparator) against literal values
/// for every column listed in `pk_cols`. Used to determine whether a DELETE needs a
/// hard confirmation modal or can use the lighter variant (spec DR-9.1 vs DR-9.2).
pub fn filter_is_pk_unique(filter: &Option<FilterNode>, pk_cols: &[&str]) -> bool {
    use dbflux_core::{Comparator, FilterNode, Predicate, PredicateValue};

    if pk_cols.is_empty() {
        return false;
    }

    let Some(node) = filter else {
        return false;
    };

    fn collect_eq_columns(node: &FilterNode, cols: &mut Vec<String>) {
        use dbflux_core::FilterNode;
        match node {
            FilterNode::Predicate(p) => {
                if matches!(p.comparator, Comparator::Eq)
                    && matches!(p.value, PredicateValue::Single(_))
                {
                    cols.push(p.column.clone());
                }
            }
            FilterNode::Group { children, .. } => {
                for child in children {
                    collect_eq_columns(child, cols);
                }
            }
        }
    }

    let mut eq_cols: Vec<String> = Vec::new();
    collect_eq_columns(node, &mut eq_cols);

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
