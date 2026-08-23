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

#[cfg(test)]
mod tests {
    use super::{
        MutationItemKind, bulk_delete_success_label, chart_degraded_copy, chart_dock_shape_label,
        chart_rail_why_text, delete_rows_label, partial_delete_label, pending_change_count_label,
        pending_edits_summary, refresh_policy_label, row_count_label, unsaved_changes_label,
        update_columns_label,
    };
    use dbflux_components::chart::ChartDetection;
    use dbflux_core::RefreshPolicy;

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
}
