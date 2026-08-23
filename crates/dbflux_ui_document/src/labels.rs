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

#[cfg(test)]
mod tests {
    use super::{
        pending_change_count_label, pending_edits_summary, refresh_policy_label, row_count_label,
        unsaved_changes_label,
    };
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
}
