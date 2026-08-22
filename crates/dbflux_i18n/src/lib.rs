rust_i18n::i18n!("locales", fallback = "en");

/// A language DBFlux ships a translation catalog for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    English,
    Spanish,
}

impl Language {
    /// The stable identifier persisted to settings storage.
    pub fn as_storage_str(self) -> &'static str {
        match self {
            Language::English => "en",
            Language::Spanish => "es",
        }
    }

    /// Parses a persisted storage identifier back into a `Language`.
    ///
    /// Returns `None` for anything other than the exact stored identifiers,
    /// including unsupported languages and locale strings with region tags.
    pub fn from_storage_str(value: &str) -> Option<Language> {
        match value {
            "en" => Some(Language::English),
            "es" => Some(Language::Spanish),
            _ => None,
        }
    }

    /// The `rust-i18n` locale code, currently identical to the storage string.
    pub fn locale_code(self) -> &'static str {
        self.as_storage_str()
    }
}

/// The user's language choice: either follow the OS locale, or pin an
/// explicit `Language` regardless of what the OS reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LanguagePreference {
    System,
    Explicit(Language),
}

impl LanguagePreference {
    /// The stable identifier persisted to settings storage. `System` is the
    /// empty string so an unset/legacy field defaults to following the OS.
    pub fn as_storage_str(self) -> &'static str {
        match self {
            LanguagePreference::System => "",
            LanguagePreference::Explicit(language) => language.as_storage_str(),
        }
    }

    /// Parses a persisted storage identifier back into a `LanguagePreference`.
    ///
    /// Any value that is not a recognized `Language` storage string
    /// (including an unset field) resolves to `System`.
    pub fn from_storage_str(value: &str) -> LanguagePreference {
        match Language::from_storage_str(value) {
            Some(language) => LanguagePreference::Explicit(language),
            None => LanguagePreference::System,
        }
    }
}

/// Resolves the effective UI `Language` from a persisted preference and the
/// detected system locale.
///
/// Precedence: a valid persisted `Language` wins outright. Otherwise, the
/// system locale's primary subtag (the part before `-` or `_`) is matched
/// case-insensitively against the supported languages. If neither source
/// yields a supported language, English is the default.
pub fn resolve(persisted: Option<&str>, system: Option<&str>) -> Language {
    if let Some(persisted) = persisted
        && let Some(language) = Language::from_storage_str(persisted)
    {
        return language;
    }

    if let Some(system) = system {
        let primary_subtag = system
            .split(['-', '_'])
            .next()
            .unwrap_or(system)
            .to_ascii_lowercase();

        if let Some(language) = Language::from_storage_str(&primary_subtag) {
            return language;
        }
    }

    Language::English
}

/// Detects the OS-reported locale (for example `"en-US"` or `"es-ES"`).
///
/// Returns `None` when the platform cannot report a locale.
pub fn detect_system_locale() -> Option<String> {
    sys_locale::get_locale()
}

/// Sets the process-wide active locale used by [`translate`] and [`t!`].
///
/// `rust-i18n` stores the active locale in a process-global, so this is
/// intended to run once at startup after resolving the effective
/// [`Language`], not on every translation lookup.
pub fn set_locale(language: Language) {
    rust_i18n::set_locale(language.locale_code());
}

/// Translates `key` using the process-wide active locale set by [`set_locale`].
///
/// Falls back to the configured fallback locale, then to the key itself,
/// when no translation is found.
pub fn translate(key: &str) -> String {
    use std::ops::Deref;

    crate::_rust_i18n_translate(rust_i18n::locale().deref(), key).into_owned()
}

/// Translates `key` for an explicit `locale`, ignoring the process-wide
/// active locale.
pub fn translate_in(locale: &str, key: &str) -> String {
    crate::_rust_i18n_translate(locale, key).into_owned()
}

/// Translates a catalog key, optionally against an explicit locale or with
/// `%{name}` placeholder interpolation.
///
/// `rust-i18n`'s own `t!` expands to a crate-local `_rust_i18n_t!` alias that
/// cannot be re-exported from this crate, so `dbflux_i18n` defines its own
/// macro on top of [`translate`] / [`translate_in`].
#[macro_export]
macro_rules! t {
    ($key:expr) => {
        $crate::translate($key)
    };
    ($key:expr, locale = $locale:expr) => {
        $crate::translate_in($locale, $key)
    };
    ($key:expr, $($name:ident = $value:expr),+ $(,)?) => {{
        let mut out = $crate::translate($key);
        $(
            out = out.replace(concat!("%{", stringify!($name), "}"), &$value.to_string());
        )+
        out
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_prefers_valid_persisted_language() {
        assert_eq!(resolve(Some("es"), Some("en-US")), Language::Spanish);
    }

    #[test]
    fn resolve_empty_persisted_falls_back_to_system() {
        assert_eq!(resolve(Some(""), Some("es-ES")), Language::Spanish);
    }

    #[test]
    fn resolve_invalid_persisted_falls_back_to_system() {
        assert_eq!(resolve(Some("de"), Some("es-419")), Language::Spanish);
        assert_eq!(resolve(Some("de"), Some("fr-FR")), Language::English);
    }

    #[test]
    fn resolve_unsupported_system_subtag_falls_back_to_english() {
        assert_eq!(resolve(None, Some("fr-FR")), Language::English);
        assert_eq!(resolve(None, None), Language::English);
    }

    #[test]
    fn resolve_es419_maps_to_spanish() {
        assert_eq!(resolve(None, Some("es-419")), Language::Spanish);
    }

    #[test]
    fn explicit_locale_translate_differs_from_default() {
        let english = t!("settings.general.save.button", locale = "en");
        let spanish = t!("settings.general.save.button", locale = "es");

        assert_eq!(english, "Save");
        assert_eq!(spanish, "Guardar");
        assert_ne!(english, spanish);
    }

    #[test]
    fn t_macro_interpolates_named_placeholders() {
        // Relies on the process-wide default locale ("en"); no test in this
        // suite calls `set_locale`, so the default is stable across tests.
        let message = t!("settings.general.save.error", error = "disk full");

        assert_eq!(message, "Failed to save general settings: disk full");
    }

    #[test]
    fn language_preference_storage_str_roundtrip_including_system() {
        assert_eq!(LanguagePreference::System.as_storage_str(), "");
        assert_eq!(
            LanguagePreference::from_storage_str(""),
            LanguagePreference::System
        );

        assert_eq!(
            LanguagePreference::Explicit(Language::English).as_storage_str(),
            "en"
        );
        assert_eq!(
            LanguagePreference::from_storage_str("en"),
            LanguagePreference::Explicit(Language::English)
        );

        assert_eq!(
            LanguagePreference::Explicit(Language::Spanish).as_storage_str(),
            "es"
        );
        assert_eq!(
            LanguagePreference::from_storage_str("es"),
            LanguagePreference::Explicit(Language::Spanish)
        );
    }

    fn flatten_catalog_keys(value: &serde_yaml::Value, prefix: String, out: &mut Vec<String>) {
        match value {
            serde_yaml::Value::Mapping(mapping) => {
                for (key, nested) in mapping {
                    let key = key.as_str().expect("catalog keys must be strings");
                    let path = if prefix.is_empty() {
                        key.to_string()
                    } else {
                        format!("{prefix}.{key}")
                    };
                    flatten_catalog_keys(nested, path, out);
                }
            }
            _ => out.push(prefix),
        }
    }

    fn flatten_catalog_values(
        value: &serde_yaml::Value,
        out: &mut Vec<(String, serde_yaml::Value)>,
    ) {
        flatten_catalog_values_with_prefix(value, String::new(), out);
    }

    fn flatten_catalog_values_with_prefix(
        value: &serde_yaml::Value,
        prefix: String,
        out: &mut Vec<(String, serde_yaml::Value)>,
    ) {
        match value {
            serde_yaml::Value::Mapping(mapping) => {
                for (key, nested) in mapping {
                    let key = key.as_str().expect("catalog keys must be strings");
                    let path = if prefix.is_empty() {
                        key.to_string()
                    } else {
                        format!("{prefix}.{key}")
                    };
                    flatten_catalog_values_with_prefix(nested, path, out);
                }
            }
            other => out.push((prefix, other.clone())),
        }
    }

    #[test]
    fn catalog_keys_match_between_en_and_es() {
        let en: serde_yaml::Value =
            serde_yaml::from_str(include_str!("../locales/en.yml")).expect("valid en.yml");
        let es: serde_yaml::Value =
            serde_yaml::from_str(include_str!("../locales/es.yml")).expect("valid es.yml");

        let mut en_keys = Vec::new();
        flatten_catalog_keys(&en, String::new(), &mut en_keys);
        let mut es_keys = Vec::new();
        flatten_catalog_keys(&es, String::new(), &mut es_keys);

        let en_set: std::collections::BTreeSet<_> = en_keys.into_iter().collect();
        let es_set: std::collections::BTreeSet<_> = es_keys.into_iter().collect();

        let missing_in_es: Vec<_> = en_set.difference(&es_set).cloned().collect();
        let missing_in_en: Vec<_> = es_set.difference(&en_set).cloned().collect();

        assert!(
            missing_in_es.is_empty() && missing_in_en.is_empty(),
            "catalog key mismatch — missing in es.yml: {missing_in_es:?}, missing in en.yml: {missing_in_en:?}"
        );
    }

    #[test]
    fn catalog_has_no_empty_values() {
        let en: serde_yaml::Value =
            serde_yaml::from_str(include_str!("../locales/en.yml")).expect("valid en.yml");
        let es: serde_yaml::Value =
            serde_yaml::from_str(include_str!("../locales/es.yml")).expect("valid es.yml");

        let mut entries = Vec::new();
        flatten_catalog_values(&en, &mut entries);
        flatten_catalog_values(&es, &mut entries);

        let empty_keys: Vec<_> = entries
            .iter()
            .filter(|(_, value)| {
                value
                    .as_str()
                    .map(|text| text.trim().is_empty())
                    .unwrap_or(true)
            })
            .map(|(key, _)| key.clone())
            .collect();

        assert!(
            empty_keys.is_empty(),
            "catalog has empty or non-string values for keys: {empty_keys:?}"
        );
    }
}
