rust_i18n::i18n!("locales", fallback = "en");

/// A language DBFlux ships a translation catalog for.
///
/// The set of available languages is derived at runtime from the catalog
/// files under `locales/` via [`Language::available`], so adding a new
/// catalog (for example `zh.yml`) makes the language available with no code
/// changes here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Language(&'static str);

impl Language {
    /// The fallback language. Always available, regardless of which other
    /// catalogs are present.
    pub const ENGLISH: Language = Language("en");

    /// Every language DBFlux ships a translation catalog for, sorted with
    /// English first and the rest alphabetically by storage identifier.
    pub fn available() -> &'static [Language] {
        static AVAILABLE: std::sync::OnceLock<Vec<Language>> = std::sync::OnceLock::new();

        AVAILABLE.get_or_init(|| {
            let mut codes = rust_i18n::available_locales!();
            codes.sort_by(|a, b| match (*a, *b) {
                ("en", "en") => std::cmp::Ordering::Equal,
                ("en", _) => std::cmp::Ordering::Less,
                (_, "en") => std::cmp::Ordering::Greater,
                _ => a.cmp(b),
            });

            codes.into_iter().map(Language).collect()
        })
    }

    /// The stable identifier persisted to settings storage.
    pub fn as_storage_str(self) -> &'static str {
        self.0
    }

    /// Parses a persisted storage identifier back into a `Language`.
    ///
    /// Returns `None` for anything other than an identifier of a currently
    /// available language, including unsupported languages and locale
    /// strings with region tags.
    pub fn from_storage_str(value: &str) -> Option<Language> {
        Language::available()
            .iter()
            .copied()
            .find(|language| language.0 == value)
    }

    /// The `rust-i18n` locale code, currently identical to the storage string.
    pub fn locale_code(self) -> &'static str {
        self.0
    }

    /// The language's own name in that exact locale, for example `"English"`
    /// or `"Español"`.
    ///
    /// Every shipped catalog is required to define a nonempty
    /// `language.native_name`; catalog contract tests enforce that metadata
    /// independently from ordinary translation fallback.
    pub fn native_name(self) -> String {
        translate_in(self.0, "language.native_name")
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

fn normalized_subtags(locale: &str) -> Vec<String> {
    locale
        .split(['-', '_'])
        .filter(|subtag| !subtag.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn match_available_locale<'a>(requested: &str, available: &'a [&'a str]) -> Option<&'a str> {
    let requested_subtags = normalized_subtags(requested);
    let requested_primary = requested_subtags.first()?;

    if let Some(exact) = available
        .iter()
        .copied()
        .find(|candidate| normalized_subtags(candidate) == requested_subtags)
    {
        return Some(exact);
    }

    if requested_primary == "zh"
        && let Some(variant) = requested_subtags.get(1)
    {
        let target = match variant.as_str() {
            "hans" | "cn" | "sg" => Some("zh-hans"),
            "hant" | "tw" | "hk" | "mo" => Some("zh-hant"),
            _ => None,
        };

        if let Some(target) = target {
            return available
                .iter()
                .copied()
                .find(|candidate| normalized_subtags(candidate).join("-") == target);
        }
    }

    let has_explicit_script = requested_subtags
        .iter()
        .skip(1)
        .any(|subtag| subtag.len() == 4 && subtag.bytes().all(|byte| byte.is_ascii_alphabetic()));
    if has_explicit_script {
        return None;
    }

    let mut primary_matches = available
        .iter()
        .copied()
        .filter(|candidate| normalized_subtags(candidate).first() == Some(requested_primary));
    let matched = primary_matches.next()?;
    if primary_matches.next().is_some() {
        None
    } else {
        Some(matched)
    }
}

/// Resolves the effective UI `Language` from a persisted preference and the
/// detected system locale.
///
/// Precedence: a valid canonical persisted `Language` wins outright. Otherwise,
/// the system locale is normalized case-insensitively with `-` and `_` treated
/// alike. Exact locale/script matches and Chinese region aliases are preferred;
/// primary-language fallback is used only when it identifies one available
/// locale unambiguously. If neither source yields a supported language, English
/// is the default.
pub fn resolve(persisted: Option<&str>, system: Option<&str>) -> Language {
    if let Some(persisted) = persisted
        && let Some(language) = Language::from_storage_str(persisted)
    {
        return language;
    }

    if let Some(system) = system {
        let available_codes: Vec<_> = Language::available()
            .iter()
            .map(|language| language.locale_code())
            .collect();
        if let Some(locale) = match_available_locale(system, &available_codes)
            && let Some(language) = Language::from_storage_str(locale)
        {
            return language;
        }
    }

    Language::ENGLISH
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

    fn spanish() -> Language {
        Language::from_storage_str("es").expect("es.yml ships a catalog for Spanish")
    }

    #[test]
    fn resolve_prefers_valid_persisted_language() {
        assert_eq!(resolve(Some("es"), Some("en-US")), spanish());
    }

    #[test]
    fn resolve_empty_persisted_falls_back_to_system() {
        assert_eq!(resolve(Some(""), Some("es-ES")), spanish());
    }

    #[test]
    fn resolve_invalid_persisted_falls_back_to_system() {
        assert_eq!(resolve(Some("de"), Some("es-419")), spanish());
        assert_eq!(resolve(Some("de"), Some("fr-FR")), Language::ENGLISH);
    }

    #[test]
    fn resolve_unsupported_system_subtag_falls_back_to_english() {
        assert_eq!(resolve(None, Some("fr-FR")), Language::ENGLISH);
        assert_eq!(resolve(None, None), Language::ENGLISH);
    }

    #[test]
    fn resolve_es419_maps_to_spanish() {
        assert_eq!(resolve(None, Some("es-419")), spanish());
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
    fn language_preference_storage_str_round_trips_system_and_every_available_locale() {
        assert_eq!(LanguagePreference::System.as_storage_str(), "");
        assert_eq!(
            LanguagePreference::from_storage_str(""),
            LanguagePreference::System
        );

        for &language in Language::available() {
            let storage_id = language.as_storage_str();
            assert_eq!(
                LanguagePreference::from_storage_str(storage_id),
                LanguagePreference::Explicit(language),
                "available locale {storage_id} must round-trip through preferences"
            );
        }
    }

    #[test]
    fn available_contains_en_and_es_with_en_first() {
        let available = Language::available();

        assert_eq!(available[0].as_storage_str(), "en");
        assert!(
            available
                .iter()
                .any(|language| language.as_storage_str() == "es")
        );
    }

    #[test]
    fn from_storage_str_round_trips_every_available_language_and_rejects_noncanonical_ids() {
        for &language in Language::available() {
            let storage_id = language.as_storage_str();
            assert_eq!(Language::from_storage_str(storage_id), Some(language));
        }

        assert_eq!(Language::from_storage_str("zz"), None);
        assert_eq!(Language::from_storage_str("ES"), None);
        assert_eq!(Language::from_storage_str("es-ES"), None);
    }

    #[test]
    fn locale_matching_normalizes_case_and_separators() {
        let available = ["en", "es", "pt-BR"];

        assert_eq!(match_available_locale("ES_419", &available), Some("es"));
        assert_eq!(match_available_locale("pt_br", &available), Some("pt-BR"));
    }

    #[test]
    fn locale_matching_maps_chinese_regions_without_shipping_chinese_catalogs() {
        let available = ["en", "zh-Hans", "zh-Hant"];

        for locale in ["zh-Hans", "zh-CN", "zh_SG"] {
            assert_eq!(
                match_available_locale(locale, &available),
                Some("zh-Hans"),
                "{locale} must select Simplified Chinese"
            );
        }
        for locale in ["zh-Hant", "zh-TW", "zh_HK", "ZH-mo"] {
            assert_eq!(
                match_available_locale(locale, &available),
                Some("zh-Hant"),
                "{locale} must select Traditional Chinese"
            );
        }
    }

    #[test]
    fn locale_matching_uses_primary_language_only_when_unambiguous() {
        assert_eq!(match_available_locale("es-MX", &["en", "es"]), Some("es"));
        assert_eq!(
            match_available_locale("zh", &["en", "zh-Hans", "zh-Hant"]),
            None
        );
        assert_eq!(
            match_available_locale("sr-Latn", &["en", "sr-Cyrl"]),
            None,
            "an explicit script must not be discarded"
        );
    }

    #[test]
    fn from_storage_str_empty_resolves_to_system_preference() {
        assert_eq!(Language::from_storage_str(""), None);
        assert_eq!(
            LanguagePreference::from_storage_str(""),
            LanguagePreference::System
        );
    }

    #[test]
    fn native_name_returns_the_language_own_name() {
        assert_eq!(Language::ENGLISH.native_name(), "English");
        assert_eq!(spanish().native_name(), "Español");
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

    fn catalog(locale: &str) -> serde_yaml::Value {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("locales")
            .join(format!("{locale}.yml"));
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        serde_yaml::from_str(&source).expect("shipped catalog must be valid YAML")
    }

    #[test]
    fn shipped_catalogs_define_nonempty_exact_native_names() {
        for language in Language::available() {
            let locale = language.locale_code();
            let catalog = catalog(locale);
            let native_name = catalog
                .get("language")
                .and_then(|language| language.get("native_name"))
                .and_then(serde_yaml::Value::as_str);

            assert!(
                native_name.is_some_and(|name| !name.trim().is_empty()),
                "shipped locale {locale} must define a nonempty exact language.native_name"
            );
        }
    }

    #[test]
    fn partial_catalogs_may_omit_fallback_keys_but_must_not_add_unknown_keys() {
        let en = catalog("en");
        let mut en_keys = Vec::new();
        flatten_catalog_keys(&en, String::new(), &mut en_keys);
        let en_set: std::collections::BTreeSet<_> = en_keys.into_iter().collect();

        for language in Language::available()
            .iter()
            .filter(|language| **language != Language::ENGLISH)
        {
            let locale = language.locale_code();
            let translated = catalog(locale);
            let mut translated_keys = Vec::new();
            flatten_catalog_keys(&translated, String::new(), &mut translated_keys);
            let translated_set: std::collections::BTreeSet<_> =
                translated_keys.into_iter().collect();
            let unknown: Vec<_> = translated_set.difference(&en_set).cloned().collect();

            assert!(
                unknown.is_empty(),
                "catalog {locale} contains keys unknown to the English fallback: {unknown:?}"
            );
        }
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
