//! Translated label helpers for sidebar chrome (tabs, footer, tree folders).

use dbflux_core::DatabaseCategory;

/// Translated label for a schema-tree container folder, e.g. `"Tables (12)"`.
///
/// Not yet wired into `tree_builder.rs` — that lands in a follow-up slice.
#[allow(dead_code)]
pub(crate) fn container_folder_label(category: DatabaseCategory, count: usize) -> String {
    match category {
        DatabaseCategory::Relational => {
            dbflux_i18n::t!("sidebar.tree.container.relational", count = count)
        }
        DatabaseCategory::Document => {
            dbflux_i18n::t!("sidebar.tree.container.document", count = count)
        }
        DatabaseCategory::KeyValue => {
            dbflux_i18n::t!("sidebar.tree.container.key_value", count = count)
        }
        DatabaseCategory::Graph => dbflux_i18n::t!("sidebar.tree.container.graph", count = count),
        DatabaseCategory::TimeSeries => {
            dbflux_i18n::t!("sidebar.tree.container.time_series", count = count)
        }
        DatabaseCategory::WideColumn => {
            dbflux_i18n::t!("sidebar.tree.container.wide_column", count = count)
        }
        DatabaseCategory::LogStream => {
            dbflux_i18n::t!("sidebar.tree.container.log_stream", count = count)
        }
        DatabaseCategory::ObjectStorage => {
            dbflux_i18n::t!("sidebar.tree.container.object_storage", count = count)
        }
    }
}

/// Translated footer summary of connected vs. idle connections, e.g.
/// `"2 connected · 5 idle"`.
pub(crate) fn footer_counts_label(connected: usize, idle: usize) -> String {
    dbflux_i18n::t!(
        "sidebar.status.connection_summary",
        connected = connected,
        idle = idle
    )
}

/// Translated page indicator for the collection child picker, e.g.
/// `"Page 1/3 (1-50)"`. `page` and `pages` are 1-based, `from`/`to` are the
/// 1-based inclusive row range shown on the current page.
pub(crate) fn page_label(page: usize, pages: usize, from: usize, to: usize) -> String {
    if pages == 0 {
        return dbflux_i18n::t!("sidebar.overlay.child_picker.page_label_empty");
    }

    dbflux_i18n::t!(
        "sidebar.overlay.child_picker.page_label",
        current = page,
        total = pages,
        start = from,
        end = to
    )
}

/// Translated child-picker modal title, e.g. `"Event streams: orders"`.
pub(crate) fn child_picker_title(collection: &str) -> String {
    dbflux_i18n::t!("sidebar.overlay.child_picker.title", name = collection)
}

/// Translated toast headline reporting a connection profile was updated,
/// e.g. `"'prod-db' updated"`.
pub(crate) fn profile_updated_label(name: &str) -> String {
    dbflux_i18n::t!("sidebar.toast.edit_reconnect_updated", name = name)
}

/// Translated label for the Export Table(s) context menu item, e.g.
/// `"Export Table…"` for a single table or `"Export 3 Tables…"` for many.
pub(crate) fn export_tables_label(count: usize) -> String {
    if count > 1 {
        dbflux_i18n::t!("sidebar.menu.export_tables_many", count = count)
    } else {
        dbflux_i18n::t!("sidebar.menu.export_table")
    }
}

/// Translated label for the Migrate Table(s) context menu item, e.g.
/// `"Migrate Table…"` for a single table or `"Migrate 3 Tables…"` for many.
pub(crate) fn migrate_tables_label(count: usize) -> String {
    if count > 1 {
        dbflux_i18n::t!("sidebar.menu.migrate_tables_many", count = count)
    } else {
        dbflux_i18n::t!("sidebar.menu.migrate_table")
    }
}

#[cfg(test)]
mod tests {
    use dbflux_core::DatabaseCategory;

    const ALL_CATEGORIES: [DatabaseCategory; 8] = [
        DatabaseCategory::Relational,
        DatabaseCategory::Document,
        DatabaseCategory::KeyValue,
        DatabaseCategory::Graph,
        DatabaseCategory::TimeSeries,
        DatabaseCategory::WideColumn,
        DatabaseCategory::LogStream,
        DatabaseCategory::ObjectStorage,
    ];

    const CONTAINER_KEYS: [&str; 8] = [
        "sidebar.tree.container.relational",
        "sidebar.tree.container.document",
        "sidebar.tree.container.key_value",
        "sidebar.tree.container.graph",
        "sidebar.tree.container.time_series",
        "sidebar.tree.container.wide_column",
        "sidebar.tree.container.log_stream",
        "sidebar.tree.container.object_storage",
    ];

    const SLICE_KEYS: [&str; 10] = [
        "sidebar.tabs.connections",
        "sidebar.tabs.scripts",
        "sidebar.confirm.delete_hint",
        "sidebar.empty.connections_title",
        "sidebar.empty.connections_hint",
        "sidebar.empty.scripts_title",
        "sidebar.empty.scripts_hint",
        "sidebar.status.connection_summary",
        "sidebar.tree.container.relational",
        "sidebar.tree.container.log_stream",
    ];

    const OVERLAY_KEYS: [&str; 23] = [
        "sidebar.filter.connections_placeholder",
        "sidebar.filter.scripts_placeholder",
        "sidebar.filter.stream_placeholder",
        "sidebar.overlay.add_folder",
        "sidebar.overlay.add_connection",
        "sidebar.overlay.add_script_file",
        "sidebar.overlay.add_script_folder",
        "sidebar.overlay.import_file",
        "sidebar.overlay.child_picker.title",
        "sidebar.overlay.child_picker.column_name",
        "sidebar.overlay.child_picker.column_last_event",
        "sidebar.overlay.child_picker.empty",
        "sidebar.overlay.child_picker.prev",
        "sidebar.overlay.child_picker.next",
        "sidebar.overlay.child_picker.page_label",
        "sidebar.overlay.child_picker.page_label_empty",
        "sidebar.overlay.child_picker.unsupported",
        "sidebar.toast.edit_reconnect_updated",
        "sidebar.toast.edit_reconnect_body",
        "sidebar.toast.edit_reconnect_now",
        "sidebar.toast.edit_reconnect_later",
        "sidebar.status.profile_fallback_name",
        "sidebar.tree.status.loading",
    ];

    #[test]
    fn container_folder_label_matches_container_name_for_every_category() {
        for category in ALL_CATEGORIES {
            let label = super::container_folder_label(category, 3);
            assert_eq!(label, format!("{} (3)", category.container_name()));
        }
    }

    #[test]
    fn container_folder_label_uses_the_given_count() {
        let label = super::container_folder_label(DatabaseCategory::Document, 7);
        assert_eq!(label, "Collections (7)");
    }

    #[test]
    fn footer_counts_label_reports_connected_and_idle_counts() {
        let label = super::footer_counts_label(2, 5);
        assert!(label.contains("2 connected"));
        assert!(label.contains("5 idle"));
    }

    #[test]
    fn footer_counts_label_reports_zero_counts() {
        let label = super::footer_counts_label(0, 0);
        assert!(label.contains("0 connected"));
        assert!(label.contains("0 idle"));
    }

    #[test]
    fn slice_translation_keys_resolve_in_every_shipped_locale() {
        for key in SLICE_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "translation fell back to the miss sentinel for {locale}.{key}"
                );
            }
        }
    }

    #[test]
    fn sidebar_tabs_connections_diverges_between_locales() {
        let english = dbflux_i18n::t!("sidebar.tabs.connections", locale = "en");
        let spanish = dbflux_i18n::t!("sidebar.tabs.connections", locale = "es");

        assert_eq!(english, "CONNECTIONS");
        assert_eq!(spanish, "CONEXIONES");
        assert_ne!(english, spanish);
    }

    #[test]
    fn sidebar_tree_container_relational_diverges_between_locales() {
        let english = dbflux_i18n::t!("sidebar.tree.container.relational", locale = "en");
        let spanish = dbflux_i18n::t!("sidebar.tree.container.relational", locale = "es");

        assert_ne!(english, spanish);
    }

    #[test]
    fn container_keys_cover_every_database_category() {
        for (category, key) in ALL_CATEGORIES.iter().zip(CONTAINER_KEYS.iter()) {
            let expected = super::container_folder_label(*category, 1);
            let translated = dbflux_i18n::t!(key, count = 1);

            assert_eq!(expected, translated);
        }
    }

    #[test]
    fn overlay_keys_resolve_in_both_locales() {
        for key in OVERLAY_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "translation fell back to the miss sentinel for {locale}.{key}"
                );
            }
        }
    }

    #[test]
    fn overlay_prev_differs_between_locales() {
        let english = dbflux_i18n::t!("sidebar.overlay.child_picker.prev", locale = "en");
        let spanish = dbflux_i18n::t!("sidebar.overlay.child_picker.prev", locale = "es");

        assert_eq!(english, "Prev");
        assert_eq!(spanish, "Anterior");
        assert_ne!(english, spanish);
    }

    #[test]
    fn page_label_reports_current_page_and_visible_range() {
        let label = super::page_label(1, 3, 1, 50);

        assert!(label.contains('1'));
        assert!(label.contains('3'));
        assert!(label.contains("50"));
        assert_eq!(
            label,
            dbflux_i18n::t!(
                "sidebar.overlay.child_picker.page_label",
                current = 1,
                total = 3,
                start = 1,
                end = 50
            )
        );
    }

    #[test]
    fn page_label_falls_back_to_empty_variant_when_there_are_no_pages() {
        let label = super::page_label(0, 0, 0, 0);

        assert_eq!(
            label,
            dbflux_i18n::t!("sidebar.overlay.child_picker.page_label_empty")
        );
    }

    #[test]
    fn child_picker_title_includes_the_collection_name() {
        let title = super::child_picker_title("orders");

        assert!(title.contains("orders"));
        assert_eq!(
            title,
            dbflux_i18n::t!("sidebar.overlay.child_picker.title", name = "orders")
        );
    }

    #[test]
    fn profile_updated_label_includes_the_profile_name() {
        let label = super::profile_updated_label("prod-db");

        assert!(label.contains("prod-db"));
        assert_eq!(
            label,
            dbflux_i18n::t!("sidebar.toast.edit_reconnect_updated", name = "prod-db")
        );
    }

    #[test]
    fn export_tables_label_one_vs_many() {
        let singular = super::export_tables_label(1);
        let plural = super::export_tables_label(3);

        assert_eq!(singular, dbflux_i18n::t!("sidebar.menu.export_table"));
        assert_eq!(
            plural,
            dbflux_i18n::t!("sidebar.menu.export_tables_many", count = 3)
        );
        assert!(plural.contains('3'));
        assert_ne!(singular, plural);
    }

    #[test]
    fn migrate_tables_label_one_vs_many() {
        let singular = super::migrate_tables_label(1);
        let plural = super::migrate_tables_label(3);

        assert_eq!(singular, dbflux_i18n::t!("sidebar.menu.migrate_table"));
        assert_eq!(
            plural,
            dbflux_i18n::t!("sidebar.menu.migrate_tables_many", count = 3)
        );
        assert!(plural.contains('3'));
        assert_ne!(singular, plural);
    }
}
