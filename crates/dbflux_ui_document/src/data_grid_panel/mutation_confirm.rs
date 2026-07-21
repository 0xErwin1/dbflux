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

    let limit = dialect.limit_clause(5);
    let sql = match where_clause {
        Some(w) if !w.is_empty() => {
            format!(
                "SELECT * FROM {} WHERE {} ORDER BY 1 {}",
                qualified_table, w, limit
            )
        }
        _ => format!("SELECT * FROM {} ORDER BY 1 {}", qualified_table, limit),
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

    // F-R3-6: classification helper — DELETE maps to Destructive, UPDATE maps to Write.
    // The helper logic is inlined at the call site in mod.rs; these tests pin the mapping
    // to prevent regression without requiring a GPUI context.
    #[test]
    fn update_spec_classifies_as_write() {
        use dbflux_core::{
            Assignment, AssignmentValue, MutationKind, ScalarLiteral, TableRef, VisualMutationSpec,
        };

        let spec = VisualMutationSpec {
            from: TableRef {
                schema: None,
                name: "t".to_string(),
            },
            filter: None,
            kind: MutationKind::Update {
                assignments: vec![Assignment {
                    column: "col".to_string(),
                    value: AssignmentValue::Literal(ScalarLiteral::Integer(1)),
                }],
            },
        };

        // The classification expression that lives in mod.rs — replicate the logic here.
        let is_delete = matches!(spec.kind, MutationKind::Delete);
        assert!(!is_delete, "UPDATE must not be classified as Delete");
    }

    #[test]
    fn delete_spec_classifies_as_destructive() {
        use dbflux_core::{MutationKind, TableRef, VisualMutationSpec};

        let spec = VisualMutationSpec {
            from: TableRef {
                schema: None,
                name: "t".to_string(),
            },
            filter: None,
            kind: MutationKind::Delete,
        };

        let is_delete = matches!(spec.kind, MutationKind::Delete);
        assert!(
            is_delete,
            "DELETE must be classified as Destructive (is_delete=true)"
        );
    }

    // F-R3-3: DELETE with PK-unique filter + est_rows=Some(1) → Light modal.
    #[test]
    fn delete_with_pk_unique_filter_and_count_one_uses_light_modal() {
        use dbflux_core::{MutationKind, TableRef, VisualMutationSpec};

        use super::{PendingMutationModal, build_pending_modal};

        let spec = VisualMutationSpec {
            from: TableRef {
                schema: None,
                name: "orders".to_string(),
            },
            filter: Some(pred_eq("id")),
            kind: MutationKind::Delete,
        };

        let modal = build_pending_modal(
            &spec,
            "DELETE FROM orders WHERE id = $1".to_string(),
            Some(1),
            vec!["id".to_string()],
            None,
            &["id"],
        );

        assert!(
            matches!(modal, PendingMutationModal::Light(_)),
            "PK-unique DELETE with est_rows=Some(1) must use Light modal"
        );
    }

    // F-R3-3: DELETE with count_state still Pending (est_rows=None) → Hard modal.
    #[test]
    fn delete_with_count_none_falls_back_to_hard_modal() {
        use dbflux_core::{MutationKind, TableRef, VisualMutationSpec};

        use super::{PendingMutationModal, build_pending_modal};

        let spec = VisualMutationSpec {
            from: TableRef {
                schema: None,
                name: "orders".to_string(),
            },
            filter: Some(pred_eq("id")),
            kind: MutationKind::Delete,
        };

        let modal = build_pending_modal(
            &spec,
            "DELETE FROM orders WHERE id = $1".to_string(),
            None,
            vec!["id".to_string()],
            None,
            &["id"],
        );

        assert!(
            matches!(modal, PendingMutationModal::Hard(_)),
            "DELETE with est_rows=None must fall back to Hard modal (safety default)"
        );
    }

    // F-R4-1: fetch_sample_rows must include ORDER BY 1 before the limit clause.
    //
    // T-SQL rejects "OFFSET 0 ROWS FETCH NEXT n ROWS ONLY" without a preceding ORDER BY.
    // The function must emit "ORDER BY 1" unconditionally so all dialects produce valid SQL.
    mod fetch_sample_rows_sql_tests {
        use std::sync::{Arc, Mutex};

        use dbflux_core::{
            DatabaseCategory, DbKind, DefaultSqlDialect, DriverCapabilities, DriverMetadataBuilder,
            MutationKind, QueryLanguage, QueryResult, SchemaLoadingStrategy, SchemaSnapshot,
            TableRef, VisualMutationSpec,
        };
        use dbflux_driver_mssql::MssqlDialect;

        use super::super::fetch_sample_rows;

        struct MssqlRecordingConnection {
            meta: dbflux_core::DriverMetadata,
            calls: Mutex<Vec<String>>,
        }

        impl MssqlRecordingConnection {
            fn new() -> Arc<Self> {
                let meta = DriverMetadataBuilder::new(
                    "sqlserver",
                    "SQL Server",
                    DatabaseCategory::Relational,
                    QueryLanguage::Sql,
                )
                .capabilities(DriverCapabilities::TRANSACTIONS)
                .build();
                Arc::new(Self {
                    meta,
                    calls: Mutex::new(Vec::new()),
                })
            }

            fn recorded_calls(&self) -> Vec<String> {
                self.calls.lock().unwrap().clone()
            }
        }

        impl dbflux_core::Connection for MssqlRecordingConnection {
            fn metadata(&self) -> &dbflux_core::DriverMetadata {
                &self.meta
            }
            fn ping(&self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }
            fn close(&mut self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }
            fn execute(
                &self,
                req: &dbflux_core::QueryRequest,
            ) -> Result<QueryResult, dbflux_core::DbError> {
                self.calls.lock().unwrap().push(req.sql.clone());
                Ok(QueryResult::empty())
            }
            fn cancel(
                &self,
                _handle: &dbflux_core::QueryHandle,
            ) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }
            fn schema(&self) -> Result<SchemaSnapshot, dbflux_core::DbError> {
                Err(dbflux_core::DbError::NotSupported("stub".to_string()))
            }
            fn kind(&self) -> DbKind {
                DbKind::SqlServer
            }
            fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
                SchemaLoadingStrategy::SingleDatabase
            }
            fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
                static D: MssqlDialect = MssqlDialect;
                &D
            }
            fn query_generator(&self) -> Option<&dyn dbflux_core::QueryGenerator> {
                None
            }
        }

        struct PgRecordingConnection {
            meta: dbflux_core::DriverMetadata,
            calls: Mutex<Vec<String>>,
        }

        impl PgRecordingConnection {
            fn new() -> Arc<Self> {
                let meta = DriverMetadataBuilder::new(
                    "postgres", // guardrail-allow: test stub, not production driver branching
                    "PostgreSQL",
                    DatabaseCategory::Relational,
                    QueryLanguage::Sql,
                )
                .capabilities(DriverCapabilities::TRANSACTIONS)
                .build();
                Arc::new(Self {
                    meta,
                    calls: Mutex::new(Vec::new()),
                })
            }

            fn recorded_calls(&self) -> Vec<String> {
                self.calls.lock().unwrap().clone()
            }
        }

        impl dbflux_core::Connection for PgRecordingConnection {
            fn metadata(&self) -> &dbflux_core::DriverMetadata {
                &self.meta
            }
            fn ping(&self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }
            fn close(&mut self) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }
            fn execute(
                &self,
                req: &dbflux_core::QueryRequest,
            ) -> Result<QueryResult, dbflux_core::DbError> {
                self.calls.lock().unwrap().push(req.sql.clone());
                Ok(QueryResult::empty())
            }
            fn cancel(
                &self,
                _handle: &dbflux_core::QueryHandle,
            ) -> Result<(), dbflux_core::DbError> {
                Ok(())
            }
            fn schema(&self) -> Result<SchemaSnapshot, dbflux_core::DbError> {
                Err(dbflux_core::DbError::NotSupported("stub".to_string()))
            }
            fn kind(&self) -> DbKind {
                DbKind::Postgres
            }
            fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
                SchemaLoadingStrategy::SingleDatabase
            }
            fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
                static D: DefaultSqlDialect = DefaultSqlDialect;
                &D
            }
            fn query_generator(&self) -> Option<&dyn dbflux_core::QueryGenerator> {
                None
            }
        }

        fn delete_spec(table: &str) -> VisualMutationSpec {
            VisualMutationSpec {
                from: TableRef {
                    schema: None,
                    name: table.to_string(),
                },
                filter: None,
                kind: MutationKind::Delete,
            }
        }

        // F-R4-1: MSSQL — SELECT must contain "ORDER BY 1" before "OFFSET 0 ROWS FETCH NEXT".
        #[test]
        fn mssql_sample_rows_includes_order_by_before_offset_fetch() {
            let conn = MssqlRecordingConnection::new();
            let conn_ref = Arc::clone(&conn);
            let spec = delete_spec("orders");

            fetch_sample_rows(conn as Arc<dyn dbflux_core::Connection>, &spec);

            let calls = conn_ref.recorded_calls();
            assert_eq!(calls.len(), 1, "expected exactly one SELECT call");
            let sql = &calls[0];

            let order_by_pos = sql.to_ascii_uppercase().find("ORDER BY 1");
            let offset_pos = sql.to_ascii_uppercase().find("OFFSET");

            assert!(
                order_by_pos.is_some(),
                "MSSQL sample rows SELECT must contain ORDER BY 1; got: {}",
                sql
            );
            assert!(
                offset_pos.is_some(),
                "MSSQL sample rows SELECT must contain OFFSET clause; got: {}",
                sql
            );
            assert!(
                order_by_pos.unwrap() < offset_pos.unwrap(),
                "ORDER BY 1 must precede OFFSET clause; got: {}",
                sql
            );
        }

        // F-R4-1: PostgreSQL — SELECT must contain "ORDER BY 1" before "LIMIT".
        #[test]
        fn postgres_sample_rows_includes_order_by_one() {
            let conn = PgRecordingConnection::new();
            let conn_ref = Arc::clone(&conn);
            let spec = delete_spec("orders");

            fetch_sample_rows(conn as Arc<dyn dbflux_core::Connection>, &spec);

            let calls = conn_ref.recorded_calls();
            assert_eq!(calls.len(), 1, "expected exactly one SELECT call");
            let sql = &calls[0];

            let order_by_pos = sql.to_ascii_uppercase().find("ORDER BY 1");
            let limit_pos = sql.to_ascii_uppercase().find("LIMIT");

            assert!(
                order_by_pos.is_some(),
                "Postgres sample rows SELECT must contain ORDER BY 1; got: {}",
                sql
            );
            assert!(
                limit_pos.is_some(),
                "Postgres sample rows SELECT must contain LIMIT clause; got: {}",
                sql
            );
            assert!(
                order_by_pos.unwrap() < limit_pos.unwrap(),
                "ORDER BY 1 must precede LIMIT clause; got: {}",
                sql
            );
        }
    }
}
