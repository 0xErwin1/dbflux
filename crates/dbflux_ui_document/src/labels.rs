//! Shared translated label helpers for the document subsystem.
//!
//! Grouped in one module so every document type resolves its user-facing
//! strings through `dbflux_i18n::t!` with the same count-based pluralization
//! convention instead of duplicating locale bucket selection per call site.

/// Label for the data grid's edit bar, with the pending-edit count
/// interpolated.
///
/// Uses the singular catalog bucket only for exactly one pending edit;
/// every other count, including zero, uses the plural bucket. Zero maps to
/// the dedicated "clean" bucket instead of the plural one.
pub(crate) fn unsaved_changes_label(count: usize) -> String {
    match count {
        0 => dbflux_i18n::t!("document.data.grid.edit_bar.clean"),
        1 => dbflux_i18n::t!("document.data.grid.edit_bar.dirty.one", count = count),
        _ => dbflux_i18n::t!("document.data.grid.edit_bar.dirty.many", count = count),
    }
}

/// Label for a [`dbflux_core::RefreshPolicy`], mirroring
/// `RefreshPolicy::label()` in English while routing every arm through the
/// translation catalog.
///
/// A named interval renders its seconds directly (`"{every_secs}s"`), which
/// is a unit suffix, not translated prose, so it stays outside the catalog.
/// Manual and any interval outside the named set fall back to their
/// respective `document.shared.refresh.*` catalog entries.
pub(crate) fn refresh_policy_label(policy: dbflux_core::RefreshPolicy) -> String {
    use dbflux_core::RefreshPolicy;

    match policy {
        RefreshPolicy::Manual => dbflux_i18n::t!("document.shared.refresh.off"),
        RefreshPolicy::Interval { every_secs } if RefreshPolicy::ALL.contains(&policy) => {
            format!("{every_secs}s")
        }
        RefreshPolicy::Interval { .. } => dbflux_i18n::t!("document.shared.refresh.custom"),
    }
}

/// Label for a [`crate::result_view::ResultViewMode`] shown in the
/// status-bar result-view mode chips.
pub(crate) fn result_view_mode_label(mode: crate::result_view::ResultViewMode) -> String {
    use crate::result_view::ResultViewMode;

    match mode {
        ResultViewMode::Table => dbflux_i18n::t!("document.data.grid.views.table"),
        ResultViewMode::Chart => dbflux_i18n::t!("document.data.grid.views.chart"),
        ResultViewMode::Json => dbflux_i18n::t!("document.data.grid.views.json"),
        ResultViewMode::Text => dbflux_i18n::t!("document.data.grid.views.text"),
        ResultViewMode::Raw => dbflux_i18n::t!("document.data.grid.views.raw"),
    }
}

/// Label for a [`dbflux_export::ExportFormat`] shown in the export menu and
/// the export trigger button.
pub(crate) fn export_format_label(format: dbflux_export::ExportFormat) -> String {
    use dbflux_export::ExportFormat;

    match format {
        ExportFormat::Csv => dbflux_i18n::t!("document.data.grid.export.format.csv"),
        ExportFormat::JsonPretty => {
            dbflux_i18n::t!("document.data.grid.export.format.json_pretty")
        }
        ExportFormat::JsonCompact => {
            dbflux_i18n::t!("document.data.grid.export.format.json_compact")
        }
        ExportFormat::Text => dbflux_i18n::t!("document.data.grid.export.format.text"),
        ExportFormat::Binary => dbflux_i18n::t!("document.data.grid.export.format.binary"),
        ExportFormat::Hex => dbflux_i18n::t!("document.data.grid.export.format.hex"),
        ExportFormat::Base64 => dbflux_i18n::t!("document.data.grid.export.format.base64"),
    }
}

/// Label for the status bar's row count, with the count interpolated.
///
/// Uses the singular catalog bucket only for exactly one row; every other
/// count, including zero, uses the plural bucket.
pub(crate) fn row_count_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!("document.data.grid.status.rows.one", count = count)
    } else {
        dbflux_i18n::t!("document.data.grid.status.rows.many", count = count)
    }
}

/// Label for the status bar's pending-change pill, with the count
/// interpolated. Distinct from [`pending_edits_summary`], which breaks the
/// count down by insert/update/delete for the tab tooltip.
///
/// Uses the singular catalog bucket only for exactly one pending change;
/// every other count uses the plural bucket.
pub(crate) fn pending_change_count_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "document.data.grid.status.pending_changes.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "document.data.grid.status.pending_changes.many",
            count = count
        )
    }
}

/// Short summary of pending inserts, updates, and deletes for the tab
/// tooltip, one chip per kind joined the same way the pre-i18n literal
/// format string did.
///
/// Returns `None` when every count is zero. Each chip uses the singular
/// catalog bucket only for exactly one edit of that kind; every other
/// count, including zero, uses the plural bucket.
pub(crate) fn pending_edits_summary(
    inserted: usize,
    updated: usize,
    deleted: usize,
) -> Option<String> {
    if inserted == 0 && updated == 0 && deleted == 0 {
        return None;
    }

    Some(
        [
            pending_inserted_label(inserted),
            pending_updated_label(updated),
            pending_deleted_label(deleted),
        ]
        .join(" · "),
    )
}

fn pending_inserted_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!("document.data.grid.pending.inserted.one", count = count)
    } else {
        dbflux_i18n::t!("document.data.grid.pending.inserted.many", count = count)
    }
}

fn pending_updated_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!("document.data.grid.pending.updated.one", count = count)
    } else {
        dbflux_i18n::t!("document.data.grid.pending.updated.many", count = count)
    }
}

fn pending_deleted_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!("document.data.grid.pending.deleted.one", count = count)
    } else {
        dbflux_i18n::t!("document.data.grid.pending.deleted.many", count = count)
    }
}

/// Title and body copy for the chart dock's degraded-state card, keyed by
/// the chart auto-detection outcome.
///
/// `None` shares the `NoTimeColumn` copy because the dock renders the
/// degraded card before detection has run at least once, and both cases
/// point the user at the same recovery action (pick a time column).
pub(crate) fn chart_degraded_copy(
    detection: &Option<dbflux_components::chart::ChartDetection>,
) -> (String, String) {
    use dbflux_components::chart::ChartDetection;

    match detection {
        Some(ChartDetection::NoTimeColumn) | None => (
            dbflux_i18n::t!("document.data.chart_dock.degraded.no_time_column.title"),
            dbflux_i18n::t!("document.data.chart_dock.degraded.no_time_column.body"),
        ),
        Some(ChartDetection::NoNumericSeries) => (
            dbflux_i18n::t!("document.data.chart_dock.degraded.no_numeric_series.title"),
            dbflux_i18n::t!("document.data.chart_dock.degraded.no_numeric_series.body"),
        ),
        Some(ChartDetection::EmptyResult) => (
            dbflux_i18n::t!("document.data.chart_dock.degraded.no_data.title"),
            dbflux_i18n::t!("document.data.chart_dock.degraded.no_data.body"),
        ),
        Some(ChartDetection::Ok { .. }) => (
            dbflux_i18n::t!("document.data.chart_dock.degraded.build_failed.title"),
            dbflux_i18n::t!("document.data.chart_dock.degraded.build_failed.body"),
        ),
    }
}

/// Row/column shape summary shown above the chart dock's degraded-state
/// column chips, with the row and column counts pluralized independently.
pub(crate) fn chart_dock_shape_label(rows: usize, columns: usize) -> String {
    let rows_label = if rows == 1 {
        dbflux_i18n::t!("document.data.chart_dock.rail.shape.rows.one", count = rows)
    } else {
        dbflux_i18n::t!(
            "document.data.chart_dock.rail.shape.rows.many",
            count = rows
        )
    };
    let columns_label = if columns == 1 {
        dbflux_i18n::t!(
            "document.data.chart_dock.rail.shape.columns.one",
            count = columns
        )
    } else {
        dbflux_i18n::t!(
            "document.data.chart_dock.rail.shape.columns.many",
            count = columns
        )
    };

    dbflux_i18n::t!(
        "document.data.chart_dock.rail.shape.template",
        rows = rows_label,
        columns = columns_label
    )
}

/// WHY-panel explanation text for the chart rail's configure tab, with the
/// numeric- and timestamp-like column counts pluralized independently.
pub(crate) fn chart_rail_why_text(numeric_columns: usize, timestamp_columns: usize) -> String {
    let numeric = if numeric_columns == 1 {
        dbflux_i18n::t!(
            "document.data.chart_dock.configure.why.numeric.one",
            count = numeric_columns
        )
    } else {
        dbflux_i18n::t!(
            "document.data.chart_dock.configure.why.numeric.many",
            count = numeric_columns
        )
    };
    let timestamp = if timestamp_columns == 1 {
        dbflux_i18n::t!(
            "document.data.chart_dock.configure.why.timestamp.one",
            count = timestamp_columns
        )
    } else {
        dbflux_i18n::t!(
            "document.data.chart_dock.configure.why.timestamp.many",
            count = timestamp_columns
        )
    };

    dbflux_i18n::t!(
        "document.data.chart_dock.configure.why.template",
        numeric = numeric,
        timestamp = timestamp
    )
}

/// Item kind affected by a bulk delete, selecting the plural noun used in
/// the completion toast and the partial-failure catalog buckets.
pub(crate) enum MutationItemKind {
    Row,
    Document,
}

/// Confirmation-modal summary for a DELETE mutation, with the estimated row
/// count interpolated when known.
///
/// Uses the singular catalog bucket only for exactly one row; every other
/// known count uses the plural bucket. `None` (the row count has not been
/// estimated yet) renders through the dedicated "unknown" bucket with no
/// count at all.
pub(crate) fn delete_rows_label(est_rows: Option<u64>, table: &str) -> String {
    match est_rows {
        Some(1) => dbflux_i18n::t!(
            "document.data.mutation.confirm.delete.summary.one",
            count = 1,
            table = table
        ),
        Some(count) => dbflux_i18n::t!(
            "document.data.mutation.confirm.delete.summary.many",
            count = count,
            table = table
        ),
        None => dbflux_i18n::t!(
            "document.data.mutation.confirm.delete.summary.unknown",
            table = table
        ),
    }
}

/// Confirmation-modal summary for an UPDATE mutation, with the affected
/// column count interpolated.
///
/// Uses the singular catalog bucket only for exactly one column; every
/// other count, including zero, uses the plural bucket.
pub(crate) fn update_columns_label(column_count: usize, table: &str) -> String {
    if column_count == 1 {
        dbflux_i18n::t!(
            "document.data.mutation.confirm.update.summary.one",
            count = column_count,
            table = table
        )
    } else {
        dbflux_i18n::t!(
            "document.data.mutation.confirm.update.summary.many",
            count = column_count,
            table = table
        )
    }
}

/// Toast/error text for a batch delete that stopped partway through after
/// hitting an error, reporting how many items succeeded before the failure.
pub(crate) fn partial_delete_label(
    kind: MutationItemKind,
    done: usize,
    total: usize,
    error: &str,
) -> String {
    let key = match kind {
        MutationItemKind::Row => "document.data.mutation.toast.partial_delete.row",
        MutationItemKind::Document => "document.data.mutation.toast.partial_delete.document",
    };

    dbflux_i18n::t!(key, done = done, total = total, error = error)
}

/// Toast text for a batch delete that completed in full, with the number of
/// deleted items interpolated.
pub(crate) fn bulk_delete_success_label(kind: MutationItemKind, count: usize) -> String {
    let key = match kind {
        MutationItemKind::Row => "document.data.mutation.toast.rows_deleted",
        MutationItemKind::Document => "document.data.mutation.toast.documents_deleted",
    };

    dbflux_i18n::t!(key, count = count)
}

/// Label for the context menu's "Copy as ..." submenu trigger, keyed by the
/// active connection's query language.
///
/// `None` covers both an unresolved connection and a `QueryResult` source
/// (which has no connection to query), and shares the generic "Copy as
/// Query" bucket with any query language that has no dedicated wording.
pub(crate) fn copy_query_language_label(language: Option<dbflux_core::QueryLanguage>) -> String {
    match language {
        Some(dbflux_core::QueryLanguage::Sql) => {
            dbflux_i18n::t!("document.data.context_menu.submenu.copy_query.sql")
        }
        Some(dbflux_core::QueryLanguage::MongoQuery) => {
            dbflux_i18n::t!("document.data.context_menu.submenu.copy_query.query")
        }
        Some(dbflux_core::QueryLanguage::RedisCommands) => {
            dbflux_i18n::t!("document.data.context_menu.submenu.copy_query.command")
        }
        _ => dbflux_i18n::t!("document.data.context_menu.submenu.copy_query.query"),
    }
}

/// Title and body copy for the row-delete confirmation modal, with the
/// affected row count interpolated.
///
/// Uses the singular catalog bucket only for exactly one row; every other
/// count uses the plural bucket.
pub(crate) fn delete_confirm_copy(count: usize) -> (String, String) {
    if count == 1 {
        (
            dbflux_i18n::t!("document.data.context_menu.delete_confirm.title.one"),
            dbflux_i18n::t!("document.data.context_menu.delete_confirm.description.one"),
        )
    } else {
        (
            dbflux_i18n::t!(
                "document.data.context_menu.delete_confirm.title.many",
                count = count
            ),
            dbflux_i18n::t!(
                "document.data.context_menu.delete_confirm.description.many",
                count = count
            ),
        )
    }
}

/// Label for the code editor toolbar's run-shortcut caption.
///
/// `shortcut` is the platform-specific key chord (e.g. `"Cmd+Enter"`), which
/// stays a literal outside the catalog. Only the surrounding "(selection/full)"
/// qualifier, shown for query languages that support connection context, is
/// translated.
pub(crate) fn code_toolbar_shortcut_hint_label(shortcut: &str, with_selection: bool) -> String {
    if with_selection {
        dbflux_i18n::t!(
            "document.code.toolbar.shortcut_hint_with_selection",
            shortcut = shortcut
        )
    } else {
        shortcut.to_string()
    }
}

/// Label for the live script output header's line count.
///
/// Uses the singular catalog bucket only for exactly one line; every other
/// count, including zero, uses the plural bucket.
pub(crate) fn live_output_lines_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!("document.code.output.lines.one", count = count)
    } else {
        dbflux_i18n::t!("document.code.output.lines.many", count = count)
    }
}

/// Label for the live script output truncation notice, with the line limit
/// interpolated.
pub(crate) fn live_output_truncated_label(limit: usize) -> String {
    dbflux_i18n::t!("document.code.output.truncated", limit = limit)
}

/// Label for the collapsed results bar's tab count.
///
/// Uses the singular catalog bucket only for exactly one result tab; every
/// other count, including zero, uses the plural bucket.
pub(crate) fn result_tab_count_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!("document.code.result.count.one", count = count)
    } else {
        dbflux_i18n::t!("document.code.result.count.many", count = count)
    }
}

/// Label for the "run entire script" confirmation modal body, with the
/// statement count interpolated.
///
/// Uses the singular catalog bucket only for exactly one statement; every
/// other count, including zero, uses the plural bucket.
pub(crate) fn script_confirm_message_label(statement_count: usize) -> String {
    if statement_count == 1 {
        dbflux_i18n::t!(
            "document.code.script_confirm.message.one",
            count = statement_count
        )
    } else {
        dbflux_i18n::t!(
            "document.code.script_confirm.message.many",
            count = statement_count
        )
    }
}

/// Label for the query builder's mode-switch bar entry (SELECT / UPDATE /
/// DELETE).
///
/// Every arm routes through the catalog for translation consistency, but
/// the `en`/`es` catalog values stay byte-identical because these are SQL
/// statement names, not prose.
pub(crate) fn builder_mode_label(
    mode: crate::query_builder::mutation_state::BuilderMode,
) -> String {
    use crate::query_builder::mutation_state::BuilderMode;

    match mode {
        BuilderMode::Select => dbflux_i18n::t!("document.query_builder.mode.select"),
        BuilderMode::Update => dbflux_i18n::t!("document.query_builder.mode.update"),
        BuilderMode::Delete => dbflux_i18n::t!("document.query_builder.mode.delete"),
    }
}

/// Label for the query builder's SQL preview line-count status line, with
/// the line count interpolated.
///
/// Uses the singular catalog bucket only for exactly one line; every other
/// count, including zero, uses the plural bucket.
pub(crate) fn valid_lines_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "document.query_builder.status.valid_lines.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "document.query_builder.status.valid_lines.many",
            count = count
        )
    }
}

/// Label for the query builder footer's incomplete-aggregate-row warning,
/// with the row count interpolated.
///
/// Uses the singular catalog bucket only for exactly one incomplete row;
/// every other count uses the plural bucket.
pub(crate) fn incomplete_aggregate_rows_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "document.query_builder.status.incomplete_aggregate_rows.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "document.query_builder.status.incomplete_aggregate_rows.many",
            count = count
        )
    }
}

/// Title for the dangerous-query confirmation modal, one per
/// `DangerousQueryKind` variant.
///
/// Exhaustive by construction (no wildcard arm) so a new variant added to
/// `dbflux_core::DangerousQueryKind` fails this crate's build until its
/// catalog key is added here.
pub(crate) fn dangerous_query_title(kind: dbflux_core::DangerousQueryKind) -> String {
    use dbflux_core::DangerousQueryKind;

    match kind {
        DangerousQueryKind::DeleteNoWhere => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.delete_no_where.title")
        }
        DangerousQueryKind::UpdateNoWhere => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.update_no_where.title")
        }
        DangerousQueryKind::Truncate => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.truncate.title")
        }
        DangerousQueryKind::Drop => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.drop.title")
        }
        DangerousQueryKind::Alter => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.alter.title")
        }
        DangerousQueryKind::Script => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.script.title")
        }
        DangerousQueryKind::MongoDeleteMany => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.mongo_delete_many.title")
        }
        DangerousQueryKind::MongoUpdateMany => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.mongo_update_many.title")
        }
        DangerousQueryKind::MongoDropCollection => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.mongo_drop_collection.title")
        }
        DangerousQueryKind::MongoDropDatabase => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.mongo_drop_database.title")
        }
        DangerousQueryKind::RedisFlushAll => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.redis_flush_all.title")
        }
        DangerousQueryKind::RedisFlushDb => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.redis_flush_db.title")
        }
        DangerousQueryKind::RedisMultiDelete => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.redis_multi_delete.title")
        }
        DangerousQueryKind::RedisKeysPattern => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.redis_keys_pattern.title")
        }
        DangerousQueryKind::RawExpressionInSet => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.raw_expression_in_set.title")
        }
    }
}

/// Body for the dangerous-query confirmation modal, one per
/// `DangerousQueryKind` variant.
///
/// The English catalog value must stay identical to
/// `DangerousQueryKind::message()` (see the parity test below); the Spanish
/// value is an independent translation of the same warning.
pub(crate) fn dangerous_query_body(kind: dbflux_core::DangerousQueryKind) -> String {
    use dbflux_core::DangerousQueryKind;

    match kind {
        DangerousQueryKind::DeleteNoWhere => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.delete_no_where.body")
        }
        DangerousQueryKind::UpdateNoWhere => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.update_no_where.body")
        }
        DangerousQueryKind::Truncate => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.truncate.body")
        }
        DangerousQueryKind::Drop => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.drop.body")
        }
        DangerousQueryKind::Alter => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.alter.body")
        }
        DangerousQueryKind::Script => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.script.body")
        }
        DangerousQueryKind::MongoDeleteMany => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.mongo_delete_many.body")
        }
        DangerousQueryKind::MongoUpdateMany => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.mongo_update_many.body")
        }
        DangerousQueryKind::MongoDropCollection => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.mongo_drop_collection.body")
        }
        DangerousQueryKind::MongoDropDatabase => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.mongo_drop_database.body")
        }
        DangerousQueryKind::RedisFlushAll => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.redis_flush_all.body")
        }
        DangerousQueryKind::RedisFlushDb => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.redis_flush_db.body")
        }
        DangerousQueryKind::RedisMultiDelete => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.redis_multi_delete.body")
        }
        DangerousQueryKind::RedisKeysPattern => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.redis_keys_pattern.body")
        }
        DangerousQueryKind::RawExpressionInSet => {
            dbflux_i18n::t!("document.code.dangerous_query.kind.raw_expression_in_set.body")
        }
    }
}

/// Label for a `dbflux_core::Comparator` shown in filter/join predicate rows.
///
/// Every arm routes through the catalog for translation consistency, but the
/// `en`/`es` catalog values stay byte-identical because these are SQL
/// operators, not prose.
pub(crate) fn comparator_label(comparator: dbflux_core::Comparator) -> String {
    use dbflux_core::Comparator;

    match comparator {
        Comparator::Eq => dbflux_i18n::t!("document.query_builder.comparator.eq"),
        Comparator::Neq => dbflux_i18n::t!("document.query_builder.comparator.neq"),
        Comparator::Gt => dbflux_i18n::t!("document.query_builder.comparator.gt"),
        Comparator::Lt => dbflux_i18n::t!("document.query_builder.comparator.lt"),
        Comparator::Gte => dbflux_i18n::t!("document.query_builder.comparator.gte"),
        Comparator::Lte => dbflux_i18n::t!("document.query_builder.comparator.lte"),
        Comparator::Like => dbflux_i18n::t!("document.query_builder.comparator.like"),
        Comparator::ILike => dbflux_i18n::t!("document.query_builder.comparator.ilike"),
        Comparator::In => dbflux_i18n::t!("document.query_builder.comparator.in"),
        Comparator::IsNull => dbflux_i18n::t!("document.query_builder.comparator.is_null"),
        Comparator::IsNotNull => {
            dbflux_i18n::t!("document.query_builder.comparator.is_not_null")
        }
    }
}

/// Label for a `dbflux_core::JoinKind` shown in the join-kind dropdown.
///
/// Every arm routes through the catalog for translation consistency, but the
/// `en`/`es` catalog values stay byte-identical because these are SQL join
/// keywords, not prose.
pub(crate) fn join_kind_label(kind: dbflux_core::JoinKind) -> String {
    use dbflux_core::JoinKind;

    match kind {
        JoinKind::Inner => dbflux_i18n::t!("document.query_builder.join.kind.inner"),
        JoinKind::Left => dbflux_i18n::t!("document.query_builder.join.kind.left"),
        JoinKind::Right => dbflux_i18n::t!("document.query_builder.join.kind.right"),
        JoinKind::Full => dbflux_i18n::t!("document.query_builder.join.kind.full"),
    }
}

/// Display text for a `dbflux_core::AggFn` shown in aggregate function
/// dropdowns and the "+ function" quick-add buttons.
///
/// Every arm routes through the catalog for translation consistency, but the
/// `en`/`es` catalog values stay byte-identical because these are SQL
/// aggregate function names, not prose.
pub(crate) fn agg_fn_display(function: dbflux_core::AggFn) -> String {
    use dbflux_core::AggFn;

    match function {
        AggFn::CountStar => dbflux_i18n::t!("document.query_builder.aggregate.fn.count_star"),
        AggFn::Count => dbflux_i18n::t!("document.query_builder.aggregate.fn.count"),
        AggFn::CountDistinct => {
            dbflux_i18n::t!("document.query_builder.aggregate.fn.count_distinct")
        }
        AggFn::Sum => dbflux_i18n::t!("document.query_builder.aggregate.fn.sum"),
        AggFn::Avg => dbflux_i18n::t!("document.query_builder.aggregate.fn.avg"),
        AggFn::Min => dbflux_i18n::t!("document.query_builder.aggregate.fn.min"),
        AggFn::Max => dbflux_i18n::t!("document.query_builder.aggregate.fn.max"),
    }
}

/// Label for a `dbflux_core::BoolOp` shown on the AND/OR group-toggle button
/// in the Filters and Joins sections.
///
/// Every arm routes through the catalog for translation consistency, but the
/// `en`/`es` catalog values stay byte-identical because these are SQL boolean
/// keywords, not prose.
pub(crate) fn bool_op_label(op: dbflux_core::BoolOp) -> String {
    use dbflux_core::BoolOp;

    match op {
        BoolOp::And => dbflux_i18n::t!("document.query_builder.filters.bool_op.and"),
        BoolOp::Or => dbflux_i18n::t!("document.query_builder.filters.bool_op.or"),
    }
}

/// Label for a `dbflux_core::VisualSortDirection` shown on sort-direction
/// toggle buttons.
///
/// Every arm routes through the catalog for translation consistency, but the
/// `en`/`es` catalog values stay byte-identical because these are SQL sort
/// keywords, not prose.
pub(crate) fn sort_direction_label(direction: dbflux_core::VisualSortDirection) -> String {
    use dbflux_core::VisualSortDirection;

    match direction {
        VisualSortDirection::Asc => dbflux_i18n::t!("document.query_builder.sort.direction.asc"),
        VisualSortDirection::Desc => {
            dbflux_i18n::t!("document.query_builder.sort.direction.desc")
        }
    }
}

/// Label for an `AssignmentValue` kind-cycle button in the mutation
/// assignments section.
///
/// `Null` and `Default` render the literal SQL keywords `NULL`/`DEFAULT`
/// (byte-identical across locales); `Literal` and `Expression` are UI
/// concept names and translate normally.
pub(crate) fn assignment_value_kind_label(value: &dbflux_core::AssignmentValue) -> String {
    use dbflux_core::AssignmentValue;

    match value {
        AssignmentValue::Literal(_) => {
            dbflux_i18n::t!("document.query_builder.assignments.kind.literal")
        }
        AssignmentValue::Expression(_) => {
            dbflux_i18n::t!("document.query_builder.assignments.kind.raw_sql")
        }
        AssignmentValue::Null => dbflux_i18n::t!("document.query_builder.assignments.kind.null"),
        AssignmentValue::Default => {
            dbflux_i18n::t!("document.query_builder.assignments.kind.default")
        }
    }
}

/// Label for an `ExecutionMode` shown on the execution-mode segmented
/// control.
pub(crate) fn execution_mode_label(
    mode: crate::data_grid_panel::mutation_executor::ExecutionMode,
) -> String {
    use crate::data_grid_panel::mutation_executor::ExecutionMode;

    match mode {
        ExecutionMode::SingleTransaction => {
            dbflux_i18n::t!("document.query_builder.execution.mode.single_tx")
        }
        ExecutionMode::ChunkedTransaction => {
            dbflux_i18n::t!("document.query_builder.execution.mode.chunked_tx")
        }
        ExecutionMode::DirectAutocommit => {
            dbflux_i18n::t!("document.query_builder.execution.mode.direct")
        }
    }
}

/// Label for the mutation execution section's row-count estimate state.
pub(crate) fn execution_count_state_label(
    state: &crate::data_grid_panel::mutation_executor::CountState,
) -> String {
    use crate::data_grid_panel::mutation_executor::{CountState, CountUnknownReason};

    match state {
        CountState::Counting => dbflux_i18n::t!("document.query_builder.execution.counting"),
        CountState::Done(n) => {
            dbflux_i18n::t!("document.query_builder.execution.rows_estimated", count = n)
        }
        CountState::Unknown { reason } => match reason {
            CountUnknownReason::TimedOut => {
                dbflux_i18n::t!("document.query_builder.execution.timed_out")
            }
            CountUnknownReason::Failed(message) => {
                dbflux_i18n::t!("document.query_builder.execution.failed", message = message)
            }
        },
    }
}

/// Label for a [`crate::history_modal::HistoryTab`] shown on the history
/// modal's tab bar.
pub(crate) fn history_tab_label(tab: crate::history_modal::HistoryTab) -> String {
    use crate::history_modal::HistoryTab;

    match tab {
        HistoryTab::Recent => dbflux_i18n::t!("document.key_value.history_modal.tabs.recent"),
        HistoryTab::Saved => dbflux_i18n::t!("document.key_value.history_modal.tabs.saved"),
    }
}

/// Label for the history modal footer's visible-item count.
///
/// Uses the singular catalog bucket only for exactly one item; every other
/// count, including zero, uses the plural bucket.
pub(crate) fn history_items_count_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "document.key_value.history_modal.footer.items.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "document.key_value.history_modal.footer.items.many",
            count = count
        )
    }
}

/// Title for the add-member modal, keyed by the target key's [`dbflux_core::KeyType`].
///
/// Every non-collection key type (`String`, `Bytes`, `Json`, `Unknown`)
/// shares the generic fallback bucket, mirroring the pre-i18n wildcard arm.
pub(crate) fn add_member_modal_title(key_type: dbflux_core::KeyType) -> String {
    use dbflux_core::KeyType;

    match key_type {
        KeyType::Hash => dbflux_i18n::t!("document.key_value.add_member_modal.title.hash"),
        KeyType::Stream => dbflux_i18n::t!("document.key_value.add_member_modal.title.stream"),
        KeyType::List => dbflux_i18n::t!("document.key_value.add_member_modal.title.list"),
        KeyType::Set => dbflux_i18n::t!("document.key_value.add_member_modal.title.set"),
        KeyType::SortedSet => {
            dbflux_i18n::t!("document.key_value.add_member_modal.title.sorted_set")
        }
        _ => dbflux_i18n::t!("document.key_value.add_member_modal.title.default"),
    }
}

/// Label for the add-member modal's row-list section header, keyed by the
/// target key's [`dbflux_core::KeyType`].
pub(crate) fn add_member_modal_section_label(key_type: dbflux_core::KeyType) -> String {
    use dbflux_core::KeyType;

    match key_type {
        KeyType::Hash | KeyType::Stream => {
            dbflux_i18n::t!("document.key_value.add_member_modal.section.fields")
        }
        KeyType::SortedSet | KeyType::List | KeyType::Set => {
            dbflux_i18n::t!("document.key_value.add_member_modal.section.members")
        }
        _ => dbflux_i18n::t!("document.key_value.add_member_modal.section.fields"),
    }
}

/// Field/value input placeholders for a new add-member row, keyed by the
/// target key's [`dbflux_core::KeyType`].
///
/// Reuses the same catalog entries as the new-key modal's field/member/score
/// placeholders since both surfaces describe the same input concepts.
/// `List`/`Set` rows have no second input, so the value placeholder is empty.
pub(crate) fn add_member_modal_placeholders(key_type: dbflux_core::KeyType) -> (String, String) {
    use dbflux_core::KeyType;

    match key_type {
        KeyType::Hash | KeyType::Stream => (
            dbflux_i18n::t!("document.key_value.new_key.field_placeholder"),
            dbflux_i18n::t!("document.key_value.new_key.value.placeholder"),
        ),
        KeyType::SortedSet => (
            dbflux_i18n::t!("document.key_value.new_key.member_placeholder"),
            dbflux_i18n::t!("document.key_value.new_key.score_placeholder"),
        ),
        KeyType::List | KeyType::Set => (
            dbflux_i18n::t!("document.key_value.new_key.member_placeholder"),
            String::new(),
        ),
        _ => (
            dbflux_i18n::t!("document.key_value.new_key.field_placeholder"),
            dbflux_i18n::t!("document.key_value.new_key.value.placeholder"),
        ),
    }
}

/// Label for a [`dbflux_core::EventCategory`] shown in the audit viewer's
/// detail pane.
///
/// Exhaustive by construction (no wildcard arm) so a new variant added to
/// `dbflux_core::EventCategory` fails this crate's build until its catalog
/// key is added here.
pub(crate) fn audit_category_label(category: dbflux_core::EventCategory) -> String {
    use dbflux_core::EventCategory;

    match category {
        EventCategory::Config => dbflux_i18n::t!("document.audit.category.config"),
        EventCategory::Connection => dbflux_i18n::t!("document.audit.category.connection"),
        EventCategory::Query => dbflux_i18n::t!("document.audit.category.query"),
        EventCategory::Hook => dbflux_i18n::t!("document.audit.category.hook"),
        EventCategory::Script => dbflux_i18n::t!("document.audit.category.script"),
        EventCategory::System => dbflux_i18n::t!("document.audit.category.system"),
        EventCategory::Mcp => dbflux_i18n::t!("document.audit.category.mcp"),
        EventCategory::Governance => dbflux_i18n::t!("document.audit.category.governance"),
        EventCategory::ObjectStorage => {
            dbflux_i18n::t!("document.audit.category.object_storage")
        }
    }
}

/// Label for a [`dbflux_core::EventOutcome`] shown in the audit viewer's
/// detail pane.
///
/// Exhaustive by construction (no wildcard arm) so a new variant added to
/// `dbflux_core::EventOutcome` fails this crate's build until its catalog
/// key is added here.
pub(crate) fn audit_outcome_label(outcome: dbflux_core::EventOutcome) -> String {
    use dbflux_core::EventOutcome;

    match outcome {
        EventOutcome::Success => dbflux_i18n::t!("document.audit.outcome.success"),
        EventOutcome::Failure => dbflux_i18n::t!("document.audit.outcome.failure"),
        EventOutcome::Cancelled => dbflux_i18n::t!("document.audit.outcome.cancelled"),
        EventOutcome::Pending => dbflux_i18n::t!("document.audit.outcome.pending"),
    }
}

/// Label for a [`dbflux_core::EventSeverity`] shown in the audit viewer's
/// detail pane.
///
/// Exhaustive by construction (no wildcard arm) so a new variant added to
/// `dbflux_core::EventSeverity` fails this crate's build until its catalog
/// key is added here.
pub(crate) fn audit_level_label(level: dbflux_core::EventSeverity) -> String {
    use dbflux_core::EventSeverity;

    match level {
        EventSeverity::Trace => dbflux_i18n::t!("document.audit.level.trace"),
        EventSeverity::Debug => dbflux_i18n::t!("document.audit.level.debug"),
        EventSeverity::Info => dbflux_i18n::t!("document.audit.level.info"),
        EventSeverity::Warn => dbflux_i18n::t!("document.audit.level.warn"),
        EventSeverity::Error => dbflux_i18n::t!("document.audit.level.error"),
        EventSeverity::Fatal => dbflux_i18n::t!("document.audit.level.fatal"),
    }
}

/// Label for a [`dbflux_core::EventActorType`] shown in the audit viewer's
/// detail pane.
///
/// Exhaustive by construction (no wildcard arm) so a new variant added to
/// `dbflux_core::EventActorType` fails this crate's build until its catalog
/// key is added here.
pub(crate) fn audit_actor_type_label(actor_type: dbflux_core::EventActorType) -> String {
    use dbflux_core::EventActorType;

    match actor_type {
        EventActorType::User => dbflux_i18n::t!("document.audit.actor.user"),
        EventActorType::System => dbflux_i18n::t!("document.audit.actor.system"),
        EventActorType::App => dbflux_i18n::t!("document.audit.actor.app"),
        EventActorType::McpClient => dbflux_i18n::t!("document.audit.actor.mcp_client"),
        EventActorType::Hook => dbflux_i18n::t!("document.audit.actor.hook"),
        EventActorType::Script => dbflux_i18n::t!("document.audit.actor.script"),
        EventActorType::ExternalDriver => {
            dbflux_i18n::t!("document.audit.actor.external_driver")
        }
        EventActorType::ExternalAuthProvider => {
            dbflux_i18n::t!("document.audit.actor.external_auth_provider")
        }
    }
}

/// Qualifies a table name with its schema for the schema-diff description
/// helpers, mirroring `schema_diff::view::qualified` (kept as a small local
/// copy since that helper is private to its own module). Object names are
/// data, never translated.
fn qualified_table_name(schema: Option<&str>, name: &str) -> String {
    match schema {
        Some(schema) => format!("{schema}.{name}"),
        None => name.to_string(),
    }
}

/// Human-readable description of a single [`dbflux_core::SchemaChange`] for
/// the schema-diff row list, mirroring the pre-i18n `describe_change`
/// output for `en` while routing every arm through the translation catalog.
///
/// Exhaustive by construction (no wildcard arm) so a new `SchemaChange`
/// variant fails this crate's build until its catalog key is added here.
/// Column/index names, type names, and default values are data and are
/// interpolated verbatim, never translated.
pub(crate) fn schema_change_description(change: &dbflux_core::SchemaChange) -> String {
    use dbflux_core::SchemaChange;

    match change {
        SchemaChange::ColumnAdded(column) => dbflux_i18n::t!(
            "document.schema_diff.change.column_added",
            name = column.name.as_str(),
            type_name = column.type_name.as_str()
        ),
        SchemaChange::ColumnRemoved(column) => dbflux_i18n::t!(
            "document.schema_diff.change.column_removed",
            name = column.name.as_str()
        ),
        SchemaChange::ColumnTypeChanged { before, after } => dbflux_i18n::t!(
            "document.schema_diff.change.type_changed",
            column = before.name.as_str(),
            before = before.type_name.as_str(),
            after = after.type_name.as_str()
        ),
        SchemaChange::NullabilityChanged { column, after, .. } => {
            if *after {
                dbflux_i18n::t!(
                    "document.schema_diff.change.nullable",
                    column = column.as_str()
                )
            } else {
                dbflux_i18n::t!(
                    "document.schema_diff.change.not_null",
                    column = column.as_str()
                )
            }
        }
        SchemaChange::DefaultChanged { column, after, .. } => match after {
            Some(value) => dbflux_i18n::t!(
                "document.schema_diff.change.default_set",
                column = column.as_str(),
                value = value.as_str()
            ),
            None => dbflux_i18n::t!(
                "document.schema_diff.change.default_dropped",
                column = column.as_str()
            ),
        },
        SchemaChange::PrimaryKeyChanged { .. } => {
            dbflux_i18n::t!("document.schema_diff.change.primary_key_changed")
        }
        SchemaChange::ForeignKeyChanged => {
            dbflux_i18n::t!("document.schema_diff.change.foreign_key_changed")
        }
        SchemaChange::IndexAdded(index) => dbflux_i18n::t!(
            "document.schema_diff.change.index_added",
            name = index.name.as_str()
        ),
        SchemaChange::IndexRemoved(index) => dbflux_i18n::t!(
            "document.schema_diff.change.index_removed",
            name = index.name.as_str()
        ),
    }
}

/// Human-readable description of a single
/// [`crate::schema_diff::apply::TableLevelAction`] for the schema-diff row
/// list, mirroring the pre-i18n `describe_table_action` output for `en`
/// while routing every arm through the translation catalog.
///
/// Exhaustive by construction (no wildcard arm) so a new `TableLevelAction`
/// variant fails this crate's build until its catalog key is added here.
pub(crate) fn table_action_description(
    action: &crate::schema_diff::apply::TableLevelAction,
) -> String {
    use crate::schema_diff::apply::TableLevelAction;

    match action {
        TableLevelAction::Create(info) => dbflux_i18n::t!(
            "document.schema_diff.table_action.create",
            table = qualified_table_name(info.schema.as_deref(), &info.name)
        ),
        TableLevelAction::Drop(table) => dbflux_i18n::t!(
            "document.schema_diff.table_action.drop",
            table = qualified_table_name(table.schema.as_deref(), &table.name)
        ),
    }
}

/// Explanation shown in place of the object browser's preview pane when
/// [`crate::object_browser::PreviewGate`] refuses to fetch the object's
/// bytes, or `None` when the object is previewable.
///
/// Exhaustive by construction (no wildcard arm) so a new `PreviewGate`
/// variant fails this crate's build until its catalog key is added here.
/// Sizes are object-store data and are interpolated verbatim, never translated.
pub(crate) fn preview_gate_message(gate: &crate::object_browser::PreviewGate) -> Option<String> {
    use crate::buckets_table::format_bytes;
    use crate::object_browser::PreviewGate;

    match gate {
        PreviewGate::Allowed => None,
        PreviewGate::TooLarge {
            size_bytes,
            limit_bytes,
        } => Some(dbflux_i18n::t!(
            "document.object_browser.gate.too_large",
            size = format_bytes(*size_bytes),
            limit = format_bytes(*limit_bytes)
        )),
        PreviewGate::Archived => Some(dbflux_i18n::t!("document.object_browser.gate.archived")),
    }
}

/// Footer summary for the object browser listing: how many folders and
/// objects are shown, and their total size. The size is S3 data and stays
/// outside the catalog.
pub(crate) fn object_browser_status_summary(
    folders: usize,
    objects: usize,
    total_bytes: u64,
) -> String {
    let folders_label = if folders == 1 {
        dbflux_i18n::t!(
            "document.object_browser.status.folders.one",
            count = folders
        )
    } else {
        dbflux_i18n::t!(
            "document.object_browser.status.folders.many",
            count = folders
        )
    };
    let objects_label = if objects == 1 {
        dbflux_i18n::t!(
            "document.object_browser.status.objects.one",
            count = objects
        )
    } else {
        dbflux_i18n::t!(
            "document.object_browser.status.objects.many",
            count = objects
        )
    };

    format!(
        "{folders_label} · {objects_label} · {}",
        crate::buckets_table::format_bytes(total_bytes)
    )
}

/// Version count shown in the object preview pane's metadata row when the
/// version list has been fetched on demand.
pub(crate) fn object_browser_versions_count_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "document.object_browser.preview.versions.count.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "document.object_browser.preview.versions.count.many",
            count = count
        )
    }
}

/// Error shown in the preview pane when a fetched image's bytes fail the
/// header-guess probe before any decode is attempted. `cause` is the
/// underlying decoder error and is interpolated verbatim, never translated.
pub(crate) fn image_header_error(cause: &str) -> String {
    dbflux_i18n::t!(
        "document.object_browser.preview.body.image_header_error",
        error = cause
    )
}

/// Error shown in the preview pane when a fetched image's bytes have a
/// recognised header but fail to fully decode. `cause` is the underlying
/// decoder error and is interpolated verbatim, never translated.
pub(crate) fn image_decode_error(cause: &str) -> String {
    dbflux_i18n::t!(
        "document.object_browser.preview.body.image_decode_error",
        error = cause
    )
}

/// Segment label for a [`crate::object_browser::PresignMethodChoice`].
/// Exhaustive by construction so a new method fails this crate's build until
/// its catalog key is added here.
pub(crate) fn presign_method_label(choice: crate::object_browser::PresignMethodChoice) -> String {
    use crate::object_browser::PresignMethodChoice;

    match choice {
        PresignMethodChoice::Get => dbflux_i18n::t!("document.object_browser.presign.method.get"),
        PresignMethodChoice::Put => dbflux_i18n::t!("document.object_browser.presign.method.put"),
    }
}

/// Segment label for a [`crate::object_browser::PresignExpiry`].
/// Exhaustive by construction so a new expiry choice fails this crate's
/// build until its catalog key is added here.
pub(crate) fn presign_expiry_label(expiry: crate::object_browser::PresignExpiry) -> String {
    use crate::object_browser::PresignExpiry;

    match expiry {
        PresignExpiry::FifteenMinutes => {
            dbflux_i18n::t!("document.object_browser.presign.expiry.fifteen_minutes")
        }
        PresignExpiry::OneHour => {
            dbflux_i18n::t!("document.object_browser.presign.expiry.one_hour")
        }
        PresignExpiry::TwelveHours => {
            dbflux_i18n::t!("document.object_browser.presign.expiry.twelve_hours")
        }
        PresignExpiry::SevenDays => {
            dbflux_i18n::t!("document.object_browser.presign.expiry.seven_days")
        }
    }
}

/// Toast text for a completed recursive-prefix delete, with the deleted
/// object count interpolated. The target URI is S3 data and stays outside
/// the catalog.
pub(crate) fn delete_prefix_deleted_toast(count: u64, uri: &str) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "document.object_browser.delete_prefix.deleted_toast.one",
            count = count,
            uri = uri
        )
    } else {
        dbflux_i18n::t!(
            "document.object_browser.delete_prefix.deleted_toast.many",
            count = count,
            uri = uri
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MutationItemKind, add_member_modal_placeholders, add_member_modal_section_label,
        add_member_modal_title, agg_fn_display, assignment_value_kind_label,
        audit_actor_type_label, audit_category_label, audit_level_label, audit_outcome_label,
        bool_op_label, builder_mode_label, bulk_delete_success_label, chart_degraded_copy,
        chart_dock_shape_label, chart_rail_why_text, code_toolbar_shortcut_hint_label,
        comparator_label, copy_query_language_label, dangerous_query_body, dangerous_query_title,
        delete_confirm_copy, delete_prefix_deleted_toast, delete_rows_label,
        execution_count_state_label, execution_mode_label, history_items_count_label,
        history_tab_label, image_decode_error, image_header_error, incomplete_aggregate_rows_label,
        join_kind_label, live_output_lines_label, live_output_truncated_label,
        object_browser_status_summary, object_browser_versions_count_label, partial_delete_label,
        pending_change_count_label, pending_edits_summary, presign_expiry_label,
        presign_method_label, preview_gate_message, refresh_policy_label, result_tab_count_label,
        row_count_label, schema_change_description, script_confirm_message_label,
        sort_direction_label, table_action_description, unsaved_changes_label,
        update_columns_label, valid_lines_label,
    };
    use crate::object_browser::{PresignExpiry, PresignMethodChoice, PreviewGate};
    use crate::schema_diff::apply::TableLevelAction;
    use dbflux_components::chart::ChartDetection;
    use dbflux_core::{
        ColumnSnapshot, DangerousQueryKind, EventActorType, EventCategory, EventOutcome,
        EventSeverity, IndexSnapshot, QueryLanguage, RefreshPolicy, SchemaChange, TableInfo,
        TableRef,
    };

    const ALL_DANGEROUS_QUERY_KINDS: &[DangerousQueryKind] = &[
        DangerousQueryKind::DeleteNoWhere,
        DangerousQueryKind::UpdateNoWhere,
        DangerousQueryKind::Truncate,
        DangerousQueryKind::Drop,
        DangerousQueryKind::Alter,
        DangerousQueryKind::Script,
        DangerousQueryKind::MongoDeleteMany,
        DangerousQueryKind::MongoUpdateMany,
        DangerousQueryKind::MongoDropCollection,
        DangerousQueryKind::MongoDropDatabase,
        DangerousQueryKind::RedisFlushAll,
        DangerousQueryKind::RedisFlushDb,
        DangerousQueryKind::RedisMultiDelete,
        DangerousQueryKind::RedisKeysPattern,
        DangerousQueryKind::RawExpressionInSet,
    ];

    #[test]
    fn unsaved_changes_label_zero_one_many() {
        let zero = unsaved_changes_label(0);
        let one = unsaved_changes_label(1);
        let many = unsaved_changes_label(2);

        assert_eq!(zero, dbflux_i18n::t!("document.data.grid.edit_bar.clean"));
        assert!(one.contains('1'));
        assert!(many.contains('2'));
        assert_ne!(one, many);
    }

    #[test]
    fn refresh_policy_label_covers_all_variants() {
        for policy in RefreshPolicy::ALL {
            assert_eq!(refresh_policy_label(*policy), policy.label());
        }

        assert_eq!(refresh_policy_label(RefreshPolicy::Manual), "Off");

        let custom = RefreshPolicy::Interval { every_secs: 7 };
        assert_eq!(refresh_policy_label(custom), "Custom");
    }

    #[test]
    fn document_namespace_present_in_both_catalogs() {
        let english = dbflux_i18n::t!("document.tabs.menu.close", locale = "en");
        let spanish = dbflux_i18n::t!("document.tabs.menu.close", locale = "es");

        assert_ne!(english, spanish);
        assert_ne!(english, "en.document.tabs.menu.close");
        assert_ne!(spanish, "es.document.tabs.menu.close");
    }

    #[test]
    fn pending_edits_summary_zero_is_none() {
        assert_eq!(pending_edits_summary(0, 0, 0), None);
    }

    #[test]
    fn pending_edits_summary_matches_pre_i18n_output_for_plural_combos() {
        // Combos chosen away from count == 1 so the plural bucket alone
        // reproduces the pre-i18n literal `"{inserts} inserts · {updates}
        // updates · {deletes} deletes"` format string exactly.
        assert_eq!(
            pending_edits_summary(2, 3, 4).as_deref(),
            Some("2 inserts · 3 updates · 4 deletes")
        );
        assert_eq!(
            pending_edits_summary(0, 5, 0).as_deref(),
            Some("0 inserts · 5 updates · 0 deletes")
        );
    }

    #[test]
    fn pending_edits_summary_uses_singular_bucket_for_exactly_one() {
        let summary = pending_edits_summary(1, 1, 1).expect("non-zero counts");

        assert_eq!(summary, "1 insert · 1 update · 1 delete");
    }

    #[test]
    fn row_count_label_one_many() {
        assert_eq!(row_count_label(1), "1 row");
        assert_eq!(row_count_label(2), "2 rows");
        assert_eq!(row_count_label(0), "0 rows");
    }

    #[test]
    fn pending_change_count_label_one_many() {
        assert_eq!(pending_change_count_label(1), "1 pending change");
        assert_eq!(pending_change_count_label(2), "2 pending changes");
    }

    #[test]
    fn chart_dock_part1_keys_resolve_in_both_locales() {
        let keys = [
            "document.data.chart_dock.toolbar.apply",
            "document.data.chart_dock.save.title",
            "document.data.chart_dock.save.name_placeholder",
            "document.data.chart_dock.save.cancel",
            "document.data.chart_dock.save.save",
            "document.data.chart_dock.degraded.no_time_column.title",
            "document.data.chart_dock.degraded.no_time_column.body",
            "document.data.chart_dock.degraded.no_numeric_series.title",
            "document.data.chart_dock.degraded.no_numeric_series.body",
            "document.data.chart_dock.degraded.no_data.title",
            "document.data.chart_dock.degraded.no_data.body",
            "document.data.chart_dock.degraded.build_failed.title",
            "document.data.chart_dock.degraded.build_failed.body",
            "document.data.chart_dock.degraded.open_table_tab",
            "document.data.chart_dock.degraded.pick_time_column",
            "document.data.chart_dock.degraded.hide_picker",
            "document.data.chart_dock.picker.x_axis_label",
            "document.data.chart_dock.picker.y_axis_label",
            "document.data.chart_dock.picker.apply",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn chart_dock_degraded_title_differs_between_locales() {
        for detection in [
            None,
            Some(ChartDetection::NoTimeColumn),
            Some(ChartDetection::NoNumericSeries),
            Some(ChartDetection::EmptyResult),
            Some(ChartDetection::Ok {
                time_col: 0,
                numeric_cols: vec![1],
            }),
        ] {
            let (title, body) = chart_degraded_copy(&detection);

            assert!(!title.is_empty());
            assert!(!body.is_empty());
        }

        let (en_title, _) = chart_degraded_copy(&Some(ChartDetection::NoTimeColumn));
        assert_eq!(en_title, "No time column detected");
    }

    #[test]
    fn chart_degraded_copy_none_matches_no_time_column() {
        let none_copy = chart_degraded_copy(&None);
        let no_time_copy = chart_degraded_copy(&Some(ChartDetection::NoTimeColumn));

        assert_eq!(none_copy, no_time_copy);
    }

    #[test]
    fn chart_dock_part2_keys_resolve_in_both_locales() {
        let keys = [
            "document.data.chart_dock.rail.shape.rows.one",
            "document.data.chart_dock.rail.shape.rows.many",
            "document.data.chart_dock.rail.shape.columns.one",
            "document.data.chart_dock.rail.shape.columns.many",
            "document.data.chart_dock.configure.why.numeric.one",
            "document.data.chart_dock.configure.why.numeric.many",
            "document.data.chart_dock.configure.why.timestamp.one",
            "document.data.chart_dock.configure.why.timestamp.many",
            "document.data.chart_dock.configure.why.title",
            "document.data.chart_dock.configure.time_column.title",
            "document.data.chart_dock.configure.series.title",
            "document.data.chart_dock.configure.axis_stacking.title",
            "document.data.chart_dock.configure.axis_stacking.y_axis",
            "document.data.chart_dock.configure.axis_stacking.y_axis_value",
            "document.data.chart_dock.configure.axis_stacking.stack",
            "document.data.chart_dock.configure.axis_stacking.stack_value",
            "document.data.chart_dock.configure.axis_stacking.interpolation",
            "document.data.chart_dock.configure.axis_stacking.interpolation_value",
            "document.data.chart_dock.configure.reset",
            "document.data.chart_dock.stats.rebuilding",
            "document.data.chart_dock.stats.no_stats",
            "document.data.chart_dock.stats.unavailable",
            "document.data.chart_dock.stats.title",
            "document.data.chart_dock.stats.window.title",
            "document.data.chart_dock.stats.window.start",
            "document.data.chart_dock.stats.window.end",
            "document.data.chart_dock.stats.window.span",
            "document.data.chart_dock.stats.window.points",
            "document.data.chart_dock.stats.source.title",
            "document.data.chart_dock.stats.source.measurement",
            "document.data.chart_dock.stats.source.field",
            "document.data.chart_dock.stats.source.host",
            "document.data.chart_dock.stats.source.region",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn chart_dock_configure_title_differs_between_locales() {
        let en = dbflux_i18n::t!(
            "document.data.chart_dock.configure.why.title",
            locale = "en"
        );
        let es = dbflux_i18n::t!(
            "document.data.chart_dock.configure.why.title",
            locale = "es"
        );

        assert_eq!(en, "Why this panel");
        assert_ne!(en, es);
    }

    #[test]
    fn chart_dock_shape_label_zero_one_many() {
        assert_eq!(chart_dock_shape_label(0, 0), "0 rows × 0 columns");
        assert_eq!(chart_dock_shape_label(1, 1), "1 row × 1 column");
        assert_eq!(chart_dock_shape_label(2, 5), "2 rows × 5 columns");
    }

    #[test]
    fn chart_rail_why_text_zero_one_many() {
        let zero = chart_rail_why_text(0, 0);
        let one = chart_rail_why_text(1, 1);
        let many = chart_rail_why_text(3, 2);

        assert_eq!(
            zero,
            "The result has 0 numeric columns and 0 timestamp-like columns. \
             Pick which one is the time axis and which series to plot."
        );
        assert_eq!(
            one,
            "The result has 1 numeric column and 1 timestamp-like column. \
             Pick which one is the time axis and which series to plot."
        );
        assert!(many.contains("3 numeric columns"));
        assert!(many.contains("2 timestamp-like columns"));
    }

    #[test]
    fn delete_rows_label_unknown_one_many() {
        let unknown = delete_rows_label(None, "orders");
        let one = delete_rows_label(Some(1), "orders");
        let many = delete_rows_label(Some(3), "orders");

        assert_eq!(unknown, "Delete rows from \"orders\"");
        assert_eq!(one, "Delete 1 row from \"orders\"");
        assert_eq!(many, "Delete 3 rows from \"orders\"");
    }

    #[test]
    fn update_columns_label_zero_one_many() {
        let zero = update_columns_label(0, "orders");
        let one = update_columns_label(1, "orders");
        let many = update_columns_label(2, "orders");

        assert_eq!(zero, "Update 0 columns in \"orders\"");
        assert_eq!(one, "Update 1 column in \"orders\"");
        assert_eq!(many, "Update 2 columns in \"orders\"");
    }

    #[test]
    fn partial_delete_label_rows_and_documents() {
        let rows = partial_delete_label(MutationItemKind::Row, 2, 5, "connection lost");
        let documents = partial_delete_label(MutationItemKind::Document, 1, 3, "timeout");

        assert_eq!(rows, "Deleted 2 of 5 row(s), then failed: connection lost");
        assert_eq!(
            documents,
            "Deleted 1 of 3 document(s), then failed: timeout"
        );
    }

    #[test]
    fn bulk_delete_success_label_rows_and_documents() {
        assert_eq!(
            bulk_delete_success_label(MutationItemKind::Row, 4),
            "4 row(s) deleted"
        );
        assert_eq!(
            bulk_delete_success_label(MutationItemKind::Document, 1),
            "1 document(s) deleted"
        );
    }

    #[test]
    fn mutation_confirm_keys_resolve_in_both_locales() {
        let keys = [
            "document.data.mutation.confirm.delete.summary.one",
            "document.data.mutation.confirm.delete.summary.many",
            "document.data.mutation.confirm.delete.summary.unknown",
            "document.data.mutation.confirm.update.summary.one",
            "document.data.mutation.confirm.update.summary.many",
            "document.data.mutation.error.update_document_unsupported_id",
            "document.data.mutation.error.update_document_failed",
            "document.data.mutation.error.save_row_unsupported_pk",
            "document.data.mutation.error.save_row_identity_failed",
            "document.data.mutation.error.save_row_unsupported_values",
            "document.data.mutation.error.save_failed",
            "document.data.mutation.error.save_document_unsupported_id",
            "document.data.mutation.error.insert_failed",
            "document.data.mutation.error.insert_no_values",
            "document.data.mutation.error.delete_document_unsupported_id",
            "document.data.mutation.error.delete_failed",
            "document.data.mutation.error.delete_no_primary_key",
            "document.data.mutation.error.delete_identity_failed",
            "document.data.mutation.error.bulk_delete_no_rows_identified",
            "document.data.mutation.error.bulk_delete_no_documents_identified",
            "document.data.mutation.toast.document_updated",
            "document.data.mutation.toast.saved",
            "document.data.mutation.toast.document_inserted",
            "document.data.mutation.toast.row_inserted",
            "document.data.mutation.toast.document_deleted",
            "document.data.mutation.toast.row_deleted",
            "document.data.mutation.toast.rows_deleted",
            "document.data.mutation.toast.documents_deleted",
            "document.data.mutation.toast.partial_delete.row",
            "document.data.mutation.toast.partial_delete.document",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn mutation_confirm_title_differs_between_locales() {
        let en = dbflux_i18n::t!(
            "document.data.mutation.confirm.delete.summary.many",
            locale = "en"
        );
        let es = dbflux_i18n::t!(
            "document.data.mutation.confirm.delete.summary.many",
            locale = "es"
        );

        assert_ne!(en, es);
    }

    #[test]
    fn copy_query_submenu_label_covers_all_variants() {
        assert_eq!(
            copy_query_language_label(Some(QueryLanguage::Sql)),
            "Copy as SQL"
        );
        assert_eq!(
            copy_query_language_label(Some(QueryLanguage::MongoQuery)),
            "Copy as Query"
        );
        assert_eq!(
            copy_query_language_label(Some(QueryLanguage::RedisCommands)),
            "Copy as Command"
        );
        assert_eq!(copy_query_language_label(None), "Copy as Query");
    }

    #[test]
    fn delete_confirm_copy_singular_and_plural() {
        let (one_title, one_body) = delete_confirm_copy(1);
        let (many_title, many_body) = delete_confirm_copy(3);

        assert_eq!(one_title, "Delete row?");
        assert_eq!(one_body, "This action cannot be undone.");
        assert_eq!(many_title, "Delete 3 rows?");
        assert!(many_body.contains('3'));
        assert_ne!(one_title, many_title);
    }

    #[test]
    fn context_menu_keys_resolve_in_both_locales() {
        let keys = [
            "document.data.context_menu.item.copy",
            "document.data.context_menu.item.view_document",
            "document.data.context_menu.item.add_document",
            "document.data.context_menu.item.duplicate_document",
            "document.data.context_menu.item.delete_document",
            "document.data.context_menu.item.paste",
            "document.data.context_menu.item.edit",
            "document.data.context_menu.item.edit_in_modal",
            "document.data.context_menu.item.set_default",
            "document.data.context_menu.item.set_null",
            "document.data.context_menu.item.add_row",
            "document.data.context_menu.item.inspect_row",
            "document.data.context_menu.item.duplicate_row",
            "document.data.context_menu.item.delete_row",
            "document.data.context_menu.item.chart_this_query",
            "document.data.context_menu.submenu.copy_query.sql",
            "document.data.context_menu.submenu.copy_query.query",
            "document.data.context_menu.submenu.copy_query.command",
            "document.data.context_menu.delete_confirm.title.one",
            "document.data.context_menu.delete_confirm.title.many",
            "document.data.context_menu.delete_confirm.description.one",
            "document.data.context_menu.delete_confirm.description.many",
            "document.data.context_menu.delete_confirm.cancel",
            "document.data.context_menu.delete_confirm.delete",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn context_menu_delete_confirm_title_differs_between_locales() {
        let en = dbflux_i18n::t!(
            "document.data.context_menu.delete_confirm.title.many",
            locale = "en"
        );
        let es = dbflux_i18n::t!(
            "document.data.context_menu.delete_confirm.title.many",
            locale = "es"
        );

        assert_ne!(en, es);
    }

    #[test]
    fn code_render_keys_resolve_in_both_locales() {
        let keys = [
            "document.code.toolbar.refresh",
            "document.code.toolbar.cancel",
            "document.code.toolbar.checking",
            "document.code.toolbar.run",
            "document.code.toolbar.shortcut_hint_with_selection",
            "document.code.toolbar.new_tab",
            "document.code.toolbar.selection",
            "document.code.toolbar.read_only",
            "document.code.toolbar.saved",
            "document.code.toolbar.save",
            "document.code.toolbar.formatter_unavailable",
            "document.code.toolbar.query_history",
            "document.code.toolbar.explain_query",
            "document.code.toolbar.open_in_chart",
            "document.code.output.running",
            "document.code.output.stopped",
            "document.code.output.output",
            "document.code.output.lines.one",
            "document.code.output.lines.many",
            "document.code.output.truncated",
            "document.code.result.count.one",
            "document.code.result.count.many",
            "document.code.result.loading.title",
            "document.code.result.loading.body",
            "document.code.result.error.title",
            "document.code.result.empty",
            "document.code.result.awaiting_connection",
            "document.code.script_confirm.title",
            "document.code.script_confirm.message.one",
            "document.code.script_confirm.message.many",
            "document.code.script_confirm.cancel",
            "document.code.script_confirm.run",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn code_toolbar_run_differs_between_locales() {
        let en = dbflux_i18n::t!("document.code.toolbar.run", locale = "en");
        let es = dbflux_i18n::t!("document.code.toolbar.run", locale = "es");

        assert_eq!(en, "Run");
        assert_ne!(en, es);
    }

    #[test]
    fn code_output_running_differs_between_locales() {
        let en = dbflux_i18n::t!("document.code.output.running", locale = "en");
        let es = dbflux_i18n::t!("document.code.output.running", locale = "es");

        assert_eq!(en, "Running...");
        assert_ne!(en, es);
    }

    #[test]
    fn code_result_empty_differs_between_locales() {
        let en = dbflux_i18n::t!("document.code.result.empty", locale = "en");
        let es = dbflux_i18n::t!("document.code.result.empty", locale = "es");

        assert_eq!(en, "Run a query to see results");
        assert_ne!(en, es);
    }

    #[test]
    fn code_script_confirm_title_differs_between_locales() {
        let en = dbflux_i18n::t!("document.code.script_confirm.title", locale = "en");
        let es = dbflux_i18n::t!("document.code.script_confirm.title", locale = "es");

        assert_eq!(en, "Run entire script");
        assert_ne!(en, es);
    }

    #[test]
    fn code_toolbar_shortcut_hint_label_with_and_without_selection() {
        let plain = code_toolbar_shortcut_hint_label("Ctrl+Enter", false);
        let with_selection = code_toolbar_shortcut_hint_label("Ctrl+Enter", true);

        assert_eq!(plain, "Ctrl+Enter");
        assert!(with_selection.contains("Ctrl+Enter"));
        assert_ne!(with_selection, plain);
    }

    #[test]
    fn live_output_lines_label_one_many() {
        assert_eq!(live_output_lines_label(1), "1 line");
        assert_eq!(live_output_lines_label(2), "2 lines");
        assert_eq!(live_output_lines_label(0), "0 lines");
    }

    #[test]
    fn live_output_truncated_label_interpolates_limit() {
        let label = live_output_truncated_label(5000);

        assert_eq!(label, "(truncated at 5000 lines)");
    }

    #[test]
    fn result_tab_count_label_one_many() {
        assert_eq!(result_tab_count_label(1), "1 result");
        assert_eq!(result_tab_count_label(2), "2 results");
    }

    #[test]
    fn script_confirm_message_label_one_many() {
        let one = script_confirm_message_label(1);
        let many = script_confirm_message_label(3);

        assert!(one.contains('1'));
        assert!(one.contains("statement in order"));
        assert!(many.contains('3'));
        assert!(many.contains("statements in order"));
        assert_ne!(one, many);
    }

    #[test]
    fn valid_lines_label_zero_one_many() {
        assert_eq!(valid_lines_label(1), "valid · 1 line");
        assert_eq!(valid_lines_label(2), "valid · 2 lines");
        assert_eq!(valid_lines_label(0), "valid · 0 lines");
    }

    #[test]
    fn incomplete_aggregate_rows_label_one_many() {
        let one = incomplete_aggregate_rows_label(1);
        let many = incomplete_aggregate_rows_label(3);

        assert!(one.contains('1'));
        assert!(one.contains("aggregate row is incomplete"));
        assert!(many.contains('3'));
        assert!(many.contains("aggregate rows are incomplete"));
        assert_ne!(one, many);
    }

    #[test]
    fn builder_mode_label_keeps_sql_keywords_literal_and_identical_across_locales() {
        use crate::query_builder::mutation_state::BuilderMode;

        assert_eq!(builder_mode_label(BuilderMode::Select), "SELECT");
        assert_eq!(builder_mode_label(BuilderMode::Update), "UPDATE");
        assert_eq!(builder_mode_label(BuilderMode::Delete), "DELETE");

        for key in [
            "document.query_builder.mode.select",
            "document.query_builder.mode.update",
            "document.query_builder.mode.delete",
        ] {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert_eq!(en, es);
        }
    }

    #[test]
    fn query_builder_chrome_and_status_keys_resolve_in_both_locales() {
        let keys = [
            "document.query_builder.chrome.save",
            "document.query_builder.chrome.reset",
            "document.query_builder.chrome.untitled_query",
            "document.query_builder.status.limit",
            "document.query_builder.status.offset",
            "document.query_builder.status.run",
            "document.query_builder.status.apply_update",
            "document.query_builder.status.open_in_editor",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key);
                assert_ne!(value, format!("{locale}.{key}"));
                assert!(!value.is_empty());
            }
        }
    }

    #[test]
    fn dangerous_query_body_matches_core_message_in_en() {
        for kind in ALL_DANGEROUS_QUERY_KINDS {
            let body = dbflux_i18n::t!(dangerous_query_body_key(*kind), locale = "en");

            assert_eq!(
                body,
                kind.message(),
                "en body for {kind:?} must match DangerousQueryKind::message()"
            );
        }
    }

    #[test]
    fn dangerous_query_copy_differs_between_locales() {
        // Titles for pure SQL/Redis command names (TRUNCATE, DROP, ALTER,
        // FLUSHALL, FLUSHDB) are legitimately identical across locales —
        // only the body sentence carries the translation for those kinds.
        let title_may_stay_literal = |kind: DangerousQueryKind| {
            matches!(
                kind,
                DangerousQueryKind::Truncate
                    | DangerousQueryKind::Drop
                    | DangerousQueryKind::Alter
                    | DangerousQueryKind::RedisFlushAll
                    | DangerousQueryKind::RedisFlushDb
                    | DangerousQueryKind::MongoDropDatabase
            )
        };

        for kind in ALL_DANGEROUS_QUERY_KINDS {
            let title_en = dbflux_i18n::t!(dangerous_query_title_key(*kind), locale = "en");
            let title_es = dbflux_i18n::t!(dangerous_query_title_key(*kind), locale = "es");
            let body_en = dbflux_i18n::t!(dangerous_query_body_key(*kind), locale = "en");
            let body_es = dbflux_i18n::t!(dangerous_query_body_key(*kind), locale = "es");

            if !title_may_stay_literal(*kind) {
                assert_ne!(title_en, title_es, "title for {kind:?} did not translate");
            }
            assert_ne!(body_en, body_es, "body for {kind:?} did not translate");

            assert_eq!(dangerous_query_title(*kind), title_en);
            assert_eq!(dangerous_query_body(*kind), body_en);
        }
    }

    #[test]
    fn dangerous_query_keys_resolve_in_both_locales() {
        let mut keys = vec![
            "document.code.dangerous_query.fallback.title".to_string(),
            "document.code.dangerous_query.fallback.body".to_string(),
            "document.code.dangerous_query.dont_ask_again".to_string(),
            "document.code.dangerous_query.cancel".to_string(),
            "document.code.dangerous_query.run_anyway".to_string(),
        ];

        for kind in ALL_DANGEROUS_QUERY_KINDS {
            keys.push(dangerous_query_title_key(*kind).to_string());
            keys.push(dangerous_query_body_key(*kind).to_string());
        }

        for key in &keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, *key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    fn dangerous_query_title_key(kind: DangerousQueryKind) -> &'static str {
        match kind {
            DangerousQueryKind::DeleteNoWhere => {
                "document.code.dangerous_query.kind.delete_no_where.title"
            }
            DangerousQueryKind::UpdateNoWhere => {
                "document.code.dangerous_query.kind.update_no_where.title"
            }
            DangerousQueryKind::Truncate => "document.code.dangerous_query.kind.truncate.title",
            DangerousQueryKind::Drop => "document.code.dangerous_query.kind.drop.title",
            DangerousQueryKind::Alter => "document.code.dangerous_query.kind.alter.title",
            DangerousQueryKind::Script => "document.code.dangerous_query.kind.script.title",
            DangerousQueryKind::MongoDeleteMany => {
                "document.code.dangerous_query.kind.mongo_delete_many.title"
            }
            DangerousQueryKind::MongoUpdateMany => {
                "document.code.dangerous_query.kind.mongo_update_many.title"
            }
            DangerousQueryKind::MongoDropCollection => {
                "document.code.dangerous_query.kind.mongo_drop_collection.title"
            }
            DangerousQueryKind::MongoDropDatabase => {
                "document.code.dangerous_query.kind.mongo_drop_database.title"
            }
            DangerousQueryKind::RedisFlushAll => {
                "document.code.dangerous_query.kind.redis_flush_all.title"
            }
            DangerousQueryKind::RedisFlushDb => {
                "document.code.dangerous_query.kind.redis_flush_db.title"
            }
            DangerousQueryKind::RedisMultiDelete => {
                "document.code.dangerous_query.kind.redis_multi_delete.title"
            }
            DangerousQueryKind::RedisKeysPattern => {
                "document.code.dangerous_query.kind.redis_keys_pattern.title"
            }
            DangerousQueryKind::RawExpressionInSet => {
                "document.code.dangerous_query.kind.raw_expression_in_set.title"
            }
        }
    }

    fn dangerous_query_body_key(kind: DangerousQueryKind) -> &'static str {
        match kind {
            DangerousQueryKind::DeleteNoWhere => {
                "document.code.dangerous_query.kind.delete_no_where.body"
            }
            DangerousQueryKind::UpdateNoWhere => {
                "document.code.dangerous_query.kind.update_no_where.body"
            }
            DangerousQueryKind::Truncate => "document.code.dangerous_query.kind.truncate.body",
            DangerousQueryKind::Drop => "document.code.dangerous_query.kind.drop.body",
            DangerousQueryKind::Alter => "document.code.dangerous_query.kind.alter.body",
            DangerousQueryKind::Script => "document.code.dangerous_query.kind.script.body",
            DangerousQueryKind::MongoDeleteMany => {
                "document.code.dangerous_query.kind.mongo_delete_many.body"
            }
            DangerousQueryKind::MongoUpdateMany => {
                "document.code.dangerous_query.kind.mongo_update_many.body"
            }
            DangerousQueryKind::MongoDropCollection => {
                "document.code.dangerous_query.kind.mongo_drop_collection.body"
            }
            DangerousQueryKind::MongoDropDatabase => {
                "document.code.dangerous_query.kind.mongo_drop_database.body"
            }
            DangerousQueryKind::RedisFlushAll => {
                "document.code.dangerous_query.kind.redis_flush_all.body"
            }
            DangerousQueryKind::RedisFlushDb => {
                "document.code.dangerous_query.kind.redis_flush_db.body"
            }
            DangerousQueryKind::RedisMultiDelete => {
                "document.code.dangerous_query.kind.redis_multi_delete.body"
            }
            DangerousQueryKind::RedisKeysPattern => {
                "document.code.dangerous_query.kind.redis_keys_pattern.body"
            }
            DangerousQueryKind::RawExpressionInSet => {
                "document.code.dangerous_query.kind.raw_expression_in_set.body"
            }
        }
    }

    #[test]
    fn comparator_label_covers_all_variants_and_stays_identical_across_locales() {
        use dbflux_core::Comparator;

        let cases = [
            (Comparator::Eq, "="),
            (Comparator::Neq, "≠"),
            (Comparator::Gt, ">"),
            (Comparator::Lt, "<"),
            (Comparator::Gte, "≥"),
            (Comparator::Lte, "≤"),
            (Comparator::Like, "LIKE"),
            (Comparator::ILike, "ILIKE"),
            (Comparator::In, "IN"),
            (Comparator::IsNull, "IS NULL"),
            (Comparator::IsNotNull, "IS NOT NULL"),
        ];

        for (comparator, expected) in cases {
            assert_eq!(comparator_label(comparator), expected);
        }
    }

    #[test]
    fn join_kind_label_covers_all_variants_and_stays_identical_across_locales() {
        use dbflux_core::JoinKind;

        let cases = [
            (JoinKind::Inner, "INNER"),
            (JoinKind::Left, "LEFT"),
            (JoinKind::Right, "RIGHT"),
            (JoinKind::Full, "FULL"),
        ];

        for (kind, expected) in cases {
            assert_eq!(join_kind_label(kind), expected);
        }
    }

    #[test]
    fn agg_fn_display_covers_all_variants_and_stays_identical_across_locales() {
        use dbflux_core::AggFn;

        let cases = [
            (AggFn::CountStar, "COUNT(*)"),
            (AggFn::Count, "COUNT"),
            (AggFn::CountDistinct, "COUNT DISTINCT"),
            (AggFn::Sum, "SUM"),
            (AggFn::Avg, "AVG"),
            (AggFn::Min, "MIN"),
            (AggFn::Max, "MAX"),
        ];

        for (function, expected) in cases {
            assert_eq!(agg_fn_display(function), expected);
        }
    }

    #[test]
    fn bool_op_label_covers_all_variants_and_stays_identical_across_locales() {
        use dbflux_core::BoolOp;

        assert_eq!(bool_op_label(BoolOp::And), "AND");
        assert_eq!(bool_op_label(BoolOp::Or), "OR");
    }

    #[test]
    fn sort_direction_label_covers_all_variants_and_stays_identical_across_locales() {
        use dbflux_core::VisualSortDirection;

        assert_eq!(sort_direction_label(VisualSortDirection::Asc), "ASC");
        assert_eq!(sort_direction_label(VisualSortDirection::Desc), "DESC");
    }

    #[test]
    fn query_builder_sql_literal_keys_resolve_identically_in_both_locales() {
        let keys = [
            "document.query_builder.comparator.eq",
            "document.query_builder.comparator.neq",
            "document.query_builder.comparator.gt",
            "document.query_builder.comparator.lt",
            "document.query_builder.comparator.gte",
            "document.query_builder.comparator.lte",
            "document.query_builder.comparator.like",
            "document.query_builder.comparator.ilike",
            "document.query_builder.comparator.in",
            "document.query_builder.comparator.is_null",
            "document.query_builder.comparator.is_not_null",
            "document.query_builder.join.kind.inner",
            "document.query_builder.join.kind.left",
            "document.query_builder.join.kind.right",
            "document.query_builder.join.kind.full",
            "document.query_builder.aggregate.fn.count_star",
            "document.query_builder.aggregate.fn.count",
            "document.query_builder.aggregate.fn.count_distinct",
            "document.query_builder.aggregate.fn.sum",
            "document.query_builder.aggregate.fn.avg",
            "document.query_builder.aggregate.fn.min",
            "document.query_builder.aggregate.fn.max",
            "document.query_builder.filters.bool_op.and",
            "document.query_builder.filters.bool_op.or",
            "document.query_builder.sort.direction.asc",
            "document.query_builder.sort.direction.desc",
            "document.query_builder.assignments.kind.null",
            "document.query_builder.assignments.kind.default",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");

            assert_ne!(en, key);
            assert_ne!(en, format!("en.{key}"));
            assert!(!en.is_empty());
            assert_eq!(en, es, "SQL literal key {key} must match across locales");
        }
    }

    #[test]
    fn assignment_value_kind_label_covers_all_variants() {
        use dbflux_core::{AssignmentValue, ScalarLiteral};

        assert_eq!(
            assignment_value_kind_label(&AssignmentValue::Literal(ScalarLiteral::Text(
                String::new()
            ))),
            "Literal"
        );
        assert_eq!(
            assignment_value_kind_label(&AssignmentValue::Expression(String::new())),
            "Raw SQL"
        );
        assert_eq!(assignment_value_kind_label(&AssignmentValue::Null), "NULL");
        assert_eq!(
            assignment_value_kind_label(&AssignmentValue::Default),
            "DEFAULT"
        );

        for key in [
            "document.query_builder.assignments.kind.literal",
            "document.query_builder.assignments.kind.raw_sql",
        ] {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");

            assert_ne!(en, es, "prose kind label {key} did not translate");
        }
    }

    #[test]
    fn execution_mode_label_covers_all_variants_and_translates_per_locale() {
        use crate::data_grid_panel::mutation_executor::ExecutionMode;

        for mode in [
            ExecutionMode::SingleTransaction,
            ExecutionMode::ChunkedTransaction,
            ExecutionMode::DirectAutocommit,
        ] {
            let en = execution_mode_label(mode);
            let key = match mode {
                ExecutionMode::SingleTransaction => {
                    "document.query_builder.execution.mode.single_tx"
                }
                ExecutionMode::ChunkedTransaction => {
                    "document.query_builder.execution.mode.chunked_tx"
                }
                ExecutionMode::DirectAutocommit => "document.query_builder.execution.mode.direct",
            };
            let es = dbflux_i18n::t!(key, locale = "es");

            assert!(!en.is_empty());
            assert_ne!(en, es, "execution mode label {key} did not translate");
        }
    }

    #[test]
    fn execution_count_state_label_zero_one_many_and_reasons() {
        use crate::data_grid_panel::mutation_executor::{CountState, CountUnknownReason};

        let counting = execution_count_state_label(&CountState::Counting);
        let done_one = execution_count_state_label(&CountState::Done(1));
        let done_many = execution_count_state_label(&CountState::Done(42));
        let timed_out = execution_count_state_label(&CountState::Unknown {
            reason: CountUnknownReason::TimedOut,
        });
        let failed = execution_count_state_label(&CountState::Unknown {
            reason: CountUnknownReason::Failed("boom".to_string()),
        });

        assert!(counting.contains("Counting"));
        assert!(done_one.contains('1'));
        assert!(done_many.contains("42"));
        assert_ne!(done_one, done_many);
        assert!(timed_out.contains("chunked"));
        assert!(failed.contains("boom"));
    }

    #[test]
    fn history_tab_label_covers_both_variants() {
        use crate::history_modal::HistoryTab;

        assert_eq!(history_tab_label(HistoryTab::Recent), "Recent");
        assert_eq!(history_tab_label(HistoryTab::Saved), "Saved");
        assert_ne!(
            history_tab_label(HistoryTab::Recent),
            history_tab_label(HistoryTab::Saved)
        );
    }

    #[test]
    fn history_items_count_label_one_many() {
        assert_eq!(history_items_count_label(1), "1 item");
        assert_eq!(history_items_count_label(2), "2 items");
        assert_eq!(history_items_count_label(0), "0 items");
    }

    #[test]
    fn history_modal_keys_resolve_in_both_locales() {
        let keys = [
            "document.key_value.history_modal.search_placeholder",
            "document.key_value.history_modal.tabs.recent",
            "document.key_value.history_modal.tabs.saved",
            "document.key_value.history_modal.empty.recent",
            "document.key_value.history_modal.empty.saved",
            "document.key_value.history_modal.footer.items.one",
            "document.key_value.history_modal.footer.items.many",
            "document.key_value.history_modal.save.title",
            "document.key_value.history_modal.save.name_placeholder",
            "document.key_value.history_modal.save.name_required",
            "document.key_value.history_modal.save.success_toast",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn history_modal_save_title_differs_between_locales() {
        let en = dbflux_i18n::t!("document.key_value.history_modal.save.title", locale = "en");
        let es = dbflux_i18n::t!("document.key_value.history_modal.save.title", locale = "es");

        assert_eq!(en, "Save Query");
        assert_ne!(en, es);
    }

    #[test]
    fn add_member_modal_title_covers_every_key_type() {
        use dbflux_core::KeyType;

        assert_eq!(add_member_modal_title(KeyType::Hash), "Add Hash Fields");
        assert_eq!(add_member_modal_title(KeyType::Stream), "Add Stream Entry");
        assert_eq!(add_member_modal_title(KeyType::List), "Add List Members");
        assert_eq!(add_member_modal_title(KeyType::Set), "Add Set Members");
        assert_eq!(
            add_member_modal_title(KeyType::SortedSet),
            "Add Sorted Set Members"
        );
        assert_eq!(add_member_modal_title(KeyType::String), "Add Member");
    }

    #[test]
    fn add_member_modal_section_label_covers_every_key_type() {
        use dbflux_core::KeyType;

        assert_eq!(add_member_modal_section_label(KeyType::Hash), "Fields");
        assert_eq!(add_member_modal_section_label(KeyType::Stream), "Fields");
        assert_eq!(
            add_member_modal_section_label(KeyType::SortedSet),
            "Members"
        );
        assert_eq!(add_member_modal_section_label(KeyType::List), "Members");
        assert_eq!(add_member_modal_section_label(KeyType::Set), "Members");
        assert_eq!(add_member_modal_section_label(KeyType::String), "Fields");
    }

    #[test]
    fn add_member_modal_placeholders_cover_every_key_type() {
        use dbflux_core::KeyType;

        assert_eq!(
            add_member_modal_placeholders(KeyType::Hash),
            ("Enter Field".to_string(), "Enter Value".to_string())
        );
        assert_eq!(
            add_member_modal_placeholders(KeyType::SortedSet),
            ("Enter Member".to_string(), "Enter Score".to_string())
        );
        assert_eq!(
            add_member_modal_placeholders(KeyType::List),
            ("Enter Member".to_string(), String::new())
        );
    }

    #[test]
    fn add_member_modal_keys_resolve_in_both_locales() {
        let keys = [
            "document.key_value.add_member_modal.title.hash",
            "document.key_value.add_member_modal.title.stream",
            "document.key_value.add_member_modal.title.list",
            "document.key_value.add_member_modal.title.set",
            "document.key_value.add_member_modal.title.sorted_set",
            "document.key_value.add_member_modal.title.default",
            "document.key_value.add_member_modal.section.fields",
            "document.key_value.add_member_modal.section.members",
            "document.key_value.add_member_modal.error.at_least_one_entry",
            "document.key_value.add_member_modal.error.prefix",
            "document.key_value.add_member_modal.cancel",
            "document.key_value.add_member_modal.submit",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn add_member_modal_title_differs_between_locales() {
        let en = dbflux_i18n::t!(
            "document.key_value.add_member_modal.title.hash",
            locale = "en"
        );
        let es = dbflux_i18n::t!(
            "document.key_value.add_member_modal.title.hash",
            locale = "es"
        );

        assert_eq!(en, "Add Hash Fields");
        assert_ne!(en, es);
    }

    const ALL_EVENT_CATEGORIES: &[EventCategory] = &[
        EventCategory::Config,
        EventCategory::Connection,
        EventCategory::Query,
        EventCategory::Hook,
        EventCategory::Script,
        EventCategory::System,
        EventCategory::Mcp,
        EventCategory::Governance,
        EventCategory::ObjectStorage,
    ];

    const ALL_EVENT_OUTCOMES: &[EventOutcome] = &[
        EventOutcome::Success,
        EventOutcome::Failure,
        EventOutcome::Cancelled,
        EventOutcome::Pending,
    ];

    const ALL_EVENT_SEVERITIES: &[EventSeverity] = &[
        EventSeverity::Trace,
        EventSeverity::Debug,
        EventSeverity::Info,
        EventSeverity::Warn,
        EventSeverity::Error,
        EventSeverity::Fatal,
    ];

    const ALL_EVENT_ACTOR_TYPES: &[EventActorType] = &[
        EventActorType::User,
        EventActorType::System,
        EventActorType::App,
        EventActorType::McpClient,
        EventActorType::Hook,
        EventActorType::Script,
        EventActorType::ExternalDriver,
        EventActorType::ExternalAuthProvider,
    ];

    fn audit_category_key(category: EventCategory) -> &'static str {
        match category {
            EventCategory::Config => "document.audit.category.config",
            EventCategory::Connection => "document.audit.category.connection",
            EventCategory::Query => "document.audit.category.query",
            EventCategory::Hook => "document.audit.category.hook",
            EventCategory::Script => "document.audit.category.script",
            EventCategory::System => "document.audit.category.system",
            EventCategory::Mcp => "document.audit.category.mcp",
            EventCategory::Governance => "document.audit.category.governance",
            EventCategory::ObjectStorage => "document.audit.category.object_storage",
        }
    }

    fn audit_outcome_key(outcome: EventOutcome) -> &'static str {
        match outcome {
            EventOutcome::Success => "document.audit.outcome.success",
            EventOutcome::Failure => "document.audit.outcome.failure",
            EventOutcome::Cancelled => "document.audit.outcome.cancelled",
            EventOutcome::Pending => "document.audit.outcome.pending",
        }
    }

    fn audit_level_key(level: EventSeverity) -> &'static str {
        match level {
            EventSeverity::Trace => "document.audit.level.trace",
            EventSeverity::Debug => "document.audit.level.debug",
            EventSeverity::Info => "document.audit.level.info",
            EventSeverity::Warn => "document.audit.level.warn",
            EventSeverity::Error => "document.audit.level.error",
            EventSeverity::Fatal => "document.audit.level.fatal",
        }
    }

    fn audit_actor_type_key(actor_type: EventActorType) -> &'static str {
        match actor_type {
            EventActorType::User => "document.audit.actor.user",
            EventActorType::System => "document.audit.actor.system",
            EventActorType::App => "document.audit.actor.app",
            EventActorType::McpClient => "document.audit.actor.mcp_client",
            EventActorType::Hook => "document.audit.actor.hook",
            EventActorType::Script => "document.audit.actor.script",
            EventActorType::ExternalDriver => "document.audit.actor.external_driver",
            EventActorType::ExternalAuthProvider => "document.audit.actor.external_auth_provider",
        }
    }

    #[test]
    fn audit_category_label_covers_all_variants_and_keys_resolve_in_both_locales() {
        for category in ALL_EVENT_CATEGORIES {
            let key = audit_category_key(*category);

            assert_eq!(audit_category_label(*category), dbflux_i18n::t!(key));

            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn audit_outcome_label_covers_all_variants_and_keys_resolve_in_both_locales() {
        for outcome in ALL_EVENT_OUTCOMES {
            let key = audit_outcome_key(*outcome);

            assert_eq!(audit_outcome_label(*outcome), dbflux_i18n::t!(key));

            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn audit_level_label_covers_all_variants_and_keys_resolve_in_both_locales() {
        for level in ALL_EVENT_SEVERITIES {
            let key = audit_level_key(*level);

            assert_eq!(audit_level_label(*level), dbflux_i18n::t!(key));

            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn audit_actor_type_label_covers_all_variants_and_keys_resolve_in_both_locales() {
        for actor_type in ALL_EVENT_ACTOR_TYPES {
            let key = audit_actor_type_key(*actor_type);

            assert_eq!(audit_actor_type_label(*actor_type), dbflux_i18n::t!(key));

            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn audit_category_label_differs_between_locales() {
        let en = dbflux_i18n::t!("document.audit.category.query", locale = "en");
        let es = dbflux_i18n::t!("document.audit.category.query", locale = "es");

        assert_eq!(en, "Query");
        assert_ne!(en, es);
    }

    #[test]
    fn audit_outcome_label_differs_between_locales() {
        let en = dbflux_i18n::t!("document.audit.outcome.success", locale = "en");
        let es = dbflux_i18n::t!("document.audit.outcome.success", locale = "es");

        assert_eq!(en, "Success");
        assert_ne!(en, es);
    }

    #[test]
    fn audit_level_label_differs_between_locales() {
        let en = dbflux_i18n::t!("document.audit.level.warn", locale = "en");
        let es = dbflux_i18n::t!("document.audit.level.warn", locale = "es");

        assert_eq!(en, "Warning");
        assert_ne!(en, es);
    }

    #[test]
    fn audit_actor_type_label_differs_between_locales() {
        let en = dbflux_i18n::t!("document.audit.actor.mcp_client", locale = "en");
        let es = dbflux_i18n::t!("document.audit.actor.mcp_client", locale = "es");

        assert_eq!(en, "MCP Client");
        assert_ne!(en, es);
    }

    // ── i18n: schema_change_description / table_action_description ────────

    fn column(name: &str, type_name: &str) -> ColumnSnapshot {
        ColumnSnapshot {
            name: name.to_string(),
            type_name: type_name.to_string(),
            nullable: true,
            is_primary_key: false,
            default_value: None,
        }
    }

    fn index(name: &str) -> IndexSnapshot {
        IndexSnapshot {
            name: name.to_string(),
            columns: vec!["id".to_string()],
            is_unique: false,
        }
    }

    /// Every `SchemaChange` construction the exhaustive match must cover,
    /// including both branches of `NullabilityChanged` and `DefaultChanged`.
    fn all_schema_changes() -> Vec<SchemaChange> {
        vec![
            SchemaChange::ColumnAdded(column("email", "text")),
            SchemaChange::ColumnRemoved(column("legacy", "text")),
            SchemaChange::ColumnTypeChanged {
                before: column("id", "integer"),
                after: column("id", "bigint"),
            },
            SchemaChange::NullabilityChanged {
                column: "email".to_string(),
                before: false,
                after: true,
            },
            SchemaChange::NullabilityChanged {
                column: "email".to_string(),
                before: true,
                after: false,
            },
            SchemaChange::DefaultChanged {
                column: "status".to_string(),
                before: None,
                after: Some("'active'".to_string()),
            },
            SchemaChange::DefaultChanged {
                column: "status".to_string(),
                before: Some("'active'".to_string()),
                after: None,
            },
            SchemaChange::PrimaryKeyChanged {
                before: vec!["id".to_string()],
                after: vec!["uuid".to_string()],
            },
            SchemaChange::ForeignKeyChanged,
            SchemaChange::IndexAdded(index("idx_email")),
            SchemaChange::IndexRemoved(index("idx_email")),
        ]
    }

    #[test]
    fn schema_change_description_matches_pre_i18n_english_output() {
        let expected = [
            "Add column email text",
            "Drop column legacy",
            "Change id type integer → bigint",
            "Make email nullable",
            "Make email NOT NULL",
            "Set default on status to 'active'",
            "Drop default on status",
            "Change primary key",
            "Change foreign keys",
            "Add index idx_email",
            "Drop index idx_email",
        ];

        for (change, expected) in all_schema_changes().iter().zip(expected) {
            assert_eq!(
                schema_change_description(change),
                expected,
                "unexpected description for {change:?}"
            );
        }
    }

    #[test]
    fn schema_change_description_keys_resolve_in_both_locales() {
        const SCHEMA_CHANGE_KEYS: &[&str] = &[
            "document.schema_diff.change.column_added",
            "document.schema_diff.change.column_removed",
            "document.schema_diff.change.default_dropped",
            "document.schema_diff.change.default_set",
            "document.schema_diff.change.foreign_key_changed",
            "document.schema_diff.change.index_added",
            "document.schema_diff.change.index_removed",
            "document.schema_diff.change.not_null",
            "document.schema_diff.change.nullable",
            "document.schema_diff.change.primary_key_changed",
            "document.schema_diff.change.type_changed",
        ];

        for key in SCHEMA_CHANGE_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(*key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, *key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn schema_change_description_differs_between_locales() {
        let en = dbflux_i18n::t!("document.schema_diff.change.column_removed", locale = "en");
        let es = dbflux_i18n::t!("document.schema_diff.change.column_removed", locale = "es");

        assert_ne!(en, es);
    }

    #[test]
    fn table_action_description_matches_pre_i18n_english_output() {
        let table_info = TableInfo {
            name: "orders".to_string(),
            schema: Some("public".to_string()),
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: Default::default(),
            child_items: None,
            storage_hints: None,
        };
        let create = TableLevelAction::Create(table_info);
        let drop = TableLevelAction::Drop(TableRef {
            schema: Some("public".to_string()),
            name: "orders".to_string(),
        });

        assert_eq!(
            table_action_description(&create),
            "Create table public.orders"
        );
        assert_eq!(table_action_description(&drop), "Drop table public.orders");
    }

    #[test]
    fn table_action_description_keys_resolve_in_both_locales() {
        for key in [
            "document.schema_diff.table_action.create",
            "document.schema_diff.table_action.drop",
        ] {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn table_action_description_differs_between_locales() {
        let en = dbflux_i18n::t!("document.schema_diff.table_action.create", locale = "en");
        let es = dbflux_i18n::t!("document.schema_diff.table_action.create", locale = "es");

        assert_ne!(en, es);
    }

    /// T19: every `document.object_browser.*` key resolves in both locales.
    #[test]
    fn object_browser_keys_resolve_in_both_locales() {
        let keys = [
            "document.object_browser.toolbar.tree",
            "document.object_browser.toolbar.upload",
            "document.object_browser.toolbar.new_folder",
            "document.object_browser.toolbar.refresh",
            "document.object_browser.columns.key",
            "document.object_browser.columns.size",
            "document.object_browser.columns.class",
            "document.object_browser.columns.last_modified",
            "document.object_browser.status.folders.one",
            "document.object_browser.status.folders.many",
            "document.object_browser.status.objects.one",
            "document.object_browser.status.objects.many",
            "document.object_browser.status.retry",
            "document.object_browser.status.tree_mode",
            "document.object_browser.status.load_more",
            "document.object_browser.status.loading_more",
            "document.object_browser.status.key_hint.open",
            "document.object_browser.status.key_hint.preview",
            "document.object_browser.status.key_hint.up",
            "document.object_browser.status.key_hint.filter",
            "document.object_browser.status.key_hint.delete",
            "document.object_browser.status.key_hint.rename",
            "document.object_browser.empty.loading",
            "document.object_browser.empty.filtered",
            "document.object_browser.empty.bucket",
            "document.object_browser.empty.prefix",
            "document.object_browser.gate.too_large",
            "document.object_browser.gate.archived",
            "document.object_browser.preview.header.open_in_editor",
            "document.object_browser.preview.header.open_in_system_viewer",
            "document.object_browser.preview.body.fit_to_width",
            "document.object_browser.preview.body.loading",
            "document.object_browser.preview.body.loading_metadata",
            "document.object_browser.preview.body.unpreviewable.pdf",
            "document.object_browser.preview.body.unpreviewable.generic",
            "document.object_browser.preview.versions.loading",
            "document.object_browser.preview.versions.view",
            "document.object_browser.preview.versions.count.one",
            "document.object_browser.preview.versions.count.many",
            "document.object_browser.preview.action.download",
            "document.object_browser.preview.action.open",
            "document.object_browser.preview.action.copy_uri",
            "document.object_browser.preview.action.presign",
            "document.object_browser.preview.action.delete",
            "document.object_browser.metadata.section",
            "document.object_browser.metadata.key",
            "document.object_browser.metadata.size",
            "document.object_browser.metadata.content_type",
            "document.object_browser.metadata.last_modified",
            "document.object_browser.metadata.etag",
            "document.object_browser.metadata.storage_class",
            "document.object_browser.metadata.encryption",
            "document.object_browser.metadata.versions",
            "document.object_browser.error.connection_unavailable",
            "document.object_browser.error.api_unavailable",
            "document.object_browser.preview.body.svg_invalid_utf8",
            "document.object_browser.preview.body.svg_missing_root",
            "document.object_browser.preview.body.image_header_error",
            "document.object_browser.preview.body.image_decode_error",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    /// T19: at least one object browser key must actually diverge between
    /// locales, so the parity loop above cannot pass on English fallbacks
    /// copied verbatim into `es.yml`.
    #[test]
    fn object_browser_toolbar_upload_differs_between_locales() {
        let en = dbflux_i18n::t!("document.object_browser.toolbar.upload", locale = "en");
        let es = dbflux_i18n::t!("document.object_browser.toolbar.upload", locale = "es");

        assert_ne!(en, es);
    }

    /// T19: the gate is exhaustive over every `PreviewGate` variant, and the
    /// size-bound explanation interpolates the exact refused/limit sizes.
    #[test]
    fn preview_gate_message_covers_all_variants() {
        assert_eq!(preview_gate_message(&PreviewGate::Allowed), None);

        let too_large = preview_gate_message(&PreviewGate::TooLarge {
            size_bytes: 20 * 1024 * 1024,
            limit_bytes: 10 * 1024 * 1024,
        })
        .expect("TooLarge always explains itself");
        assert!(too_large.contains("10.0 MiB"));
        assert!(too_large.contains("20.0 MiB"));

        let archived =
            preview_gate_message(&PreviewGate::Archived).expect("Archived explains itself");
        assert!(!archived.is_empty());
    }

    /// T19: both refusal explanations translate. The `t!` macro has no arm
    /// combining named interpolation with an explicit `locale =` override
    /// (only `(key)` / `(key, locale=)` / `(key, name=value+)`), so the
    /// interpolated-value coverage above and this locale-divergence check
    /// stay two separate assertions, matching the schema_diff PR 17
    /// precedent.
    #[test]
    fn preview_gate_message_differs_between_locales() {
        let too_large_en = dbflux_i18n::t!("document.object_browser.gate.too_large", locale = "en");
        let too_large_es = dbflux_i18n::t!("document.object_browser.gate.too_large", locale = "es");

        assert_ne!(too_large_en, too_large_es);

        let archived_en = preview_gate_message(&PreviewGate::Archived).unwrap();
        let archived_es = dbflux_i18n::t!("document.object_browser.gate.archived", locale = "es");

        assert_ne!(archived_en, archived_es);
    }

    /// T19: the "connection dropped" and "no object-store API" fallbacks
    /// used across the object browser's background loaders translate and
    /// diverge between locales.
    #[test]
    fn object_browser_error_keys_differ_between_locales() {
        let connection_en = dbflux_i18n::t!(
            "document.object_browser.error.connection_unavailable",
            locale = "en"
        );
        let connection_es = dbflux_i18n::t!(
            "document.object_browser.error.connection_unavailable",
            locale = "es"
        );
        assert_ne!(connection_en, connection_es);

        let api_en = dbflux_i18n::t!(
            "document.object_browser.error.api_unavailable",
            locale = "en"
        );
        let api_es = dbflux_i18n::t!(
            "document.object_browser.error.api_unavailable",
            locale = "es"
        );
        assert_ne!(api_en, api_es);
    }

    /// T19: the SVG body-validation refusals translate and diverge between
    /// locales.
    #[test]
    fn object_browser_svg_validation_keys_differ_between_locales() {
        let utf8_en = dbflux_i18n::t!(
            "document.object_browser.preview.body.svg_invalid_utf8",
            locale = "en"
        );
        let utf8_es = dbflux_i18n::t!(
            "document.object_browser.preview.body.svg_invalid_utf8",
            locale = "es"
        );
        assert_ne!(utf8_en, utf8_es);

        let root_en = dbflux_i18n::t!(
            "document.object_browser.preview.body.svg_missing_root",
            locale = "en"
        );
        let root_es = dbflux_i18n::t!(
            "document.object_browser.preview.body.svg_missing_root",
            locale = "es"
        );
        assert_ne!(root_en, root_es);
    }

    /// T19: the image decode-failure helpers interpolate the underlying
    /// decoder cause verbatim into the translated prefix.
    #[test]
    fn image_error_helpers_interpolate_the_cause() {
        let header = image_header_error("truncated header");
        assert!(header.contains("truncated header"));
        assert_ne!(header, "truncated header");

        let decode = image_decode_error("unsupported color type");
        assert!(decode.contains("unsupported color type"));
        assert_ne!(decode, "unsupported color type");
    }

    /// T19: every `PresignMethodChoice` variant has a translated segment
    /// label, and no variant resolves to an empty or key-fallback string.
    #[test]
    fn presign_method_label_covers_all_variants() {
        for choice in PresignMethodChoice::all() {
            let label = presign_method_label(choice);
            assert!(!label.is_empty(), "{choice:?} resolved empty");
        }

        assert_ne!(
            presign_method_label(PresignMethodChoice::Get),
            presign_method_label(PresignMethodChoice::Put)
        );
    }

    /// T19: every `PresignExpiry` variant has a translated segment label.
    #[test]
    fn presign_expiry_label_covers_all_variants() {
        for expiry in PresignExpiry::all() {
            let label = presign_expiry_label(expiry);
            assert!(!label.is_empty(), "{expiry:?} resolved empty");
        }
    }

    /// T19: the presign method/expiry labels translate and diverge between
    /// locales.
    #[test]
    fn presign_method_and_expiry_keys_differ_between_locales() {
        let get_en = dbflux_i18n::t!("document.object_browser.presign.method.get", locale = "en");
        let get_es = dbflux_i18n::t!("document.object_browser.presign.method.get", locale = "es");
        assert_ne!(get_en, get_es);

        let one_hour_en = dbflux_i18n::t!(
            "document.object_browser.presign.expiry.one_hour",
            locale = "en"
        );
        let one_hour_es = dbflux_i18n::t!(
            "document.object_browser.presign.expiry.one_hour",
            locale = "es"
        );
        assert_ne!(one_hour_en, one_hour_es);
    }

    /// T19: every `document.object_browser.presign.*` key resolves in both
    /// locales.
    #[test]
    fn presign_keys_resolve_in_both_locales() {
        let keys = [
            "document.object_browser.presign.title",
            "document.object_browser.presign.method_field_label",
            "document.object_browser.presign.expiry_field_label",
            "document.object_browser.presign.signing",
            "document.object_browser.presign.close",
            "document.object_browser.presign.copy_url",
            "document.object_browser.presign.copied_toast",
            "document.object_browser.presign.signing_identity_fallback",
            "document.object_browser.presign.method.get",
            "document.object_browser.presign.method.put",
            "document.object_browser.presign.expiry.fifteen_minutes",
            "document.object_browser.presign.expiry.one_hour",
            "document.object_browser.presign.expiry.twelve_hours",
            "document.object_browser.presign.expiry.seven_days",
            "document.object_browser.presign.warning.capability.get",
            "document.object_browser.presign.warning.capability.put",
            "document.object_browser.presign.warning.until_instant",
            "document.object_browser.presign.warning.until_it_expires",
            "document.object_browser.presign.warning.body",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    /// T19: the footer summary covers the singular/plural boundary
    /// independently for folders and objects.
    #[test]
    fn object_browser_status_summary_covers_singular_and_plural() {
        assert_eq!(
            object_browser_status_summary(1, 2, 2048),
            "1 folder · 2 objects · 2.0 KiB"
        );
        assert_eq!(
            object_browser_status_summary(0, 0, 0),
            "0 folders · 0 objects · 0 B"
        );
        assert_eq!(
            object_browser_status_summary(2, 1, 512),
            "2 folders · 1 object · 512 B"
        );
    }

    /// T19: the on-demand version count uses the singular bucket only for
    /// exactly one version.
    #[test]
    fn object_browser_versions_count_label_covers_singular_and_plural() {
        assert_eq!(object_browser_versions_count_label(1), "1 version");
        assert_eq!(object_browser_versions_count_label(3), "3 versions");
    }

    /// T20a: every `document.object_browser.{rename,create_folder,delete,
    /// delete_prefix}.*` key resolves in both locales.
    #[test]
    fn object_browser_modal_keys_resolve_in_both_locales() {
        let keys = [
            "document.object_browser.create_folder.title",
            "document.object_browser.create_folder.name_placeholder",
            "document.object_browser.create_folder.location",
            "document.object_browser.create_folder.hint",
            "document.object_browser.create_folder.cancel",
            "document.object_browser.create_folder.confirm",
            "document.object_browser.create_folder.confirm_in_progress",
            "document.object_browser.create_folder.created_toast",
            "document.object_browser.create_folder.error.empty",
            "document.object_browser.create_folder.error.leading_trailing_slash",
            "document.object_browser.create_folder.error.consecutive_slashes",
            "document.object_browser.delete.title",
            "document.object_browser.delete.body",
            "document.object_browser.delete.unknown_size",
            "document.object_browser.delete.cancel",
            "document.object_browser.delete.confirm",
            "document.object_browser.delete.deleted_toast",
            "document.object_browser.delete_prefix.versioning_note",
            "document.object_browser.delete_prefix.deleted_toast.one",
            "document.object_browser.delete_prefix.deleted_toast.many",
            "document.object_browser.rename.title",
            "document.object_browser.rename.name_placeholder",
            "document.object_browser.rename.cancel",
            "document.object_browser.rename.confirm",
            "document.object_browser.rename.confirm_in_progress",
            "document.object_browser.rename.renamed_toast",
            "document.object_browser.rename.error.empty",
            "document.object_browser.rename.error.contains_slash",
            "document.object_browser.rename.error.unchanged",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    /// T20a: the single-object delete title and the rename title translate
    /// to their exact wording and diverge between locales.
    #[test]
    fn object_browser_modal_titles_have_exact_translated_text() {
        assert_eq!(
            dbflux_i18n::t!("document.object_browser.delete.title", locale = "en"),
            "Delete object?"
        );
        assert_eq!(
            dbflux_i18n::t!("document.object_browser.delete.title", locale = "es"),
            "¿Eliminar objeto?"
        );
        assert_ne!(
            dbflux_i18n::t!("document.object_browser.rename.title", locale = "en"),
            dbflux_i18n::t!("document.object_browser.rename.title", locale = "es")
        );
    }

    /// T20a: the recursive-delete toast uses the singular catalog bucket
    /// only for exactly one deleted object.
    #[test]
    fn delete_prefix_deleted_toast_covers_singular_and_plural() {
        assert_eq!(
            delete_prefix_deleted_toast(1, "s3://my-bucket/logs/"),
            "Deleted 1 object under s3://my-bucket/logs/"
        );
        assert_eq!(
            delete_prefix_deleted_toast(4, "s3://my-bucket/logs/"),
            "Deleted 4 objects under s3://my-bucket/logs/"
        );
    }
}
