use std::sync::Arc;
use std::time::Duration;

use dbflux_components::modals::{MutationConfirmHardRequest, MutationConfirmRequest};
use dbflux_core::{Connection, FilterNode, QueryRequest, Value, VisualMutationSpec};

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
/// Runs synchronously on a background thread via `std::thread::spawn`.
/// Returns `(column_names, rows)` on success, or an empty result on failure or timeout.
///
/// The deadline is 2 seconds per spec DR-9.
pub fn fetch_sample_rows(
    connection: Arc<dyn Connection>,
    table: &str,
    filter: &Option<FilterNode>,
    schema: Option<&str>,
) -> (Vec<String>, Vec<Vec<String>>) {
    let qualified = match schema {
        Some(s) => format!("\"{}\".\"{}\"", s, table),
        None => format!("\"{}\"", table),
    };

    let where_clause = filter
        .as_ref()
        .map(|_| "WHERE <filter>".to_string())
        .unwrap_or_default();

    let sql = if where_clause.is_empty() {
        format!("SELECT * FROM {} LIMIT 5", qualified)
    } else {
        format!("SELECT * FROM {} {} LIMIT 5", qualified, where_clause)
    };

    let (tx, rx) = std::sync::mpsc::channel::<Option<(Vec<String>, Vec<Vec<String>>)>>();

    std::thread::spawn(move || {
        let request = QueryRequest::new(sql);
        let result = connection.execute(&request).ok().map(|qr| {
            let col_names: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();
            let rows: Vec<Vec<String>> = qr
                .rows
                .iter()
                .map(|row| row.iter().map(|v| format!("{}", v)).collect())
                .collect();
            (col_names, rows)
        });
        let _ = tx.send(result);
    });

    match rx.recv_timeout(Duration::from_secs(2)) {
        Ok(Some(data)) => data,
        _ => (Vec::new(), Vec::new()),
    }
}

/// Builds a `PendingMutationModal` for the given spec and estimated row count.
///
/// Uses `Hard` when the spec is a DELETE (no filter = all rows) or when the
/// row count exceeds 1,000. Uses `Light` otherwise.
pub fn build_pending_modal(
    spec: &VisualMutationSpec,
    sql_preview: String,
    est_rows: Option<u64>,
    sample_columns: Vec<String>,
    sample_rows: Option<Vec<Vec<String>>>,
) -> PendingMutationModal {
    use dbflux_core::MutationKind;

    let table_name = spec.from.name.clone();
    let is_delete = matches!(spec.kind, MutationKind::Delete);
    let is_large = est_rows.map(|n| n > 1_000).unwrap_or(false);
    let use_hard = is_delete || is_large;

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
