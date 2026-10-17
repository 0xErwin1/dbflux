//! Presentation of a [`DatabaseCategory`], kept in one table so that adding a
//! category is a single arm here instead of a `match` in every UI crate.

use dbflux_core::DatabaseCategory;

use crate::icons::{AppIcon, DriverIconTone};

/// How the UI presents a database category when no driver-specific value applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CategoryPresentation {
    /// Icon for a driver that declares the generic `Icon::Database`.
    pub fallback_icon: AppIcon,
    /// Logo tone for a driver that declares the generic `Icon::Database`.
    pub fallback_tone: DriverIconTone,
    /// Translation key of the schema-tree container folder, with a `%{count}`
    /// placeholder, e.g. `"Tables (12)"`.
    pub container_label_key: &'static str,
}

pub const fn category_presentation(category: DatabaseCategory) -> CategoryPresentation {
    let (fallback_icon, fallback_tone, container_label_key) = match category {
        // The reserved categories are never declared by a driver; they only
        // keep wire indices, so they borrow the relational look.
        DatabaseCategory::Relational
        | DatabaseCategory::ReservedGraph
        | DatabaseCategory::ReservedWideColumn => (
            AppIcon::Database,
            DriverIconTone::Info,
            "sidebar.tree.container.relational",
        ),
        DatabaseCategory::Document => (
            AppIcon::Braces,
            DriverIconTone::Success,
            "sidebar.tree.container.document",
        ),
        DatabaseCategory::KeyValue => (
            AppIcon::KeyRound,
            DriverIconTone::Danger,
            "sidebar.tree.container.key_value",
        ),
        DatabaseCategory::TimeSeries => (
            AppIcon::Database,
            DriverIconTone::Violet,
            "sidebar.tree.container.time_series",
        ),
        DatabaseCategory::LogStream => (
            AppIcon::Database,
            DriverIconTone::Warning,
            "sidebar.tree.container.log_stream",
        ),
        DatabaseCategory::ObjectStorage => (
            AppIcon::Boxes,
            DriverIconTone::Warning,
            "sidebar.tree.container.object_storage",
        ),
    };

    CategoryPresentation {
        fallback_icon,
        fallback_tone,
        container_label_key,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_CATEGORIES: [DatabaseCategory; 6] = [
        DatabaseCategory::Relational,
        DatabaseCategory::Document,
        DatabaseCategory::KeyValue,
        DatabaseCategory::TimeSeries,
        DatabaseCategory::LogStream,
        DatabaseCategory::ObjectStorage,
    ];

    #[test]
    fn container_label_key_resolves_in_every_shipped_locale() {
        for category in ALL_CATEGORIES {
            let key = category_presentation(category).container_label_key;
            for locale in ["en", "es", "ko"] {
                let value = dbflux_i18n::t!(key, locale = locale);
                assert!(
                    value.contains("%{count}"),
                    "{key} in {locale} must resolve to a template with a count, got {value:?}"
                );
            }
        }
    }

    #[test]
    fn container_label_keys_are_distinct_per_category() {
        let keys: std::collections::HashSet<_> = ALL_CATEGORIES
            .iter()
            .map(|category| category_presentation(*category).container_label_key)
            .collect();
        assert_eq!(keys.len(), ALL_CATEGORIES.len());
    }

    #[test]
    fn fallback_icon_and_tone_keep_the_category_mapping() {
        let expected = [
            (
                DatabaseCategory::Relational,
                AppIcon::Database,
                DriverIconTone::Info,
            ),
            (
                DatabaseCategory::Document,
                AppIcon::Braces,
                DriverIconTone::Success,
            ),
            (
                DatabaseCategory::KeyValue,
                AppIcon::KeyRound,
                DriverIconTone::Danger,
            ),
            (
                DatabaseCategory::TimeSeries,
                AppIcon::Database,
                DriverIconTone::Violet,
            ),
            (
                DatabaseCategory::LogStream,
                AppIcon::Database,
                DriverIconTone::Warning,
            ),
            (
                DatabaseCategory::ObjectStorage,
                AppIcon::Boxes,
                DriverIconTone::Warning,
            ),
        ];

        for (category, icon, tone) in expected {
            let presentation = category_presentation(category);
            assert_eq!(presentation.fallback_icon, icon, "{category:?}");
            assert_eq!(presentation.fallback_tone, tone, "{category:?}");
        }
    }
}
