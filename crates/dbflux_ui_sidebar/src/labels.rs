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
}
