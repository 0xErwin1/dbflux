//! Registers the tree-sitter grammars the code-editor highlighter needs.
//!
//! `gpui-component`'s `tree-sitter-languages` feature bundles roughly thirty
//! grammars, but DBFlux only ever opens the editor with the modes
//! `QueryLanguage::editor_mode()` can return, plus `json` for the document
//! tree, cell editor, document preview, and dashboard import modals.
//! Registering those individually with `LanguageRegistry::register` keeps
//! the release binary from linking grammars nothing in the app requests.

use gpui_component::highlighter::{LanguageConfig, LanguageRegistry};

/// Every language name the code editor is asked to highlight: the modes
/// `QueryLanguage::editor_mode()` can return, plus `json`.
#[cfg(test)]
const REGISTERED_LANGUAGES: &[&str] = &[
    "sql",
    "javascript",
    "cypher",
    "lua",
    "python",
    "bash",
    "plaintext",
    "json",
];

/// Registers the grammars DBFlux's editors actually request. Must run once,
/// before any `InputState::code_editor(...)` call — call alongside
/// `gpui_component::init`.
pub fn register_languages() {
    let registry = LanguageRegistry::singleton();

    registry.register(
        "sql",
        &LanguageConfig::new(
            "sql",
            tree_sitter::Language::new(tree_sitter_sequel::LANGUAGE),
            vec![],
            tree_sitter_sequel::HIGHLIGHTS_QUERY,
            "",
            "",
        ),
    );

    registry.register(
        "javascript",
        &LanguageConfig::new(
            "javascript",
            tree_sitter::Language::new(tree_sitter_javascript::LANGUAGE),
            vec![],
            tree_sitter_javascript::HIGHLIGHT_QUERY,
            "",
            "",
        ),
    );

    registry.register(
        "cypher",
        &LanguageConfig::new(
            "cypher",
            tree_sitter::Language::new(tree_sitter_cypher::LANGUAGE),
            vec![],
            tree_sitter_cypher::HIGHLIGHTS_QUERY,
            "",
            "",
        ),
    );

    registry.register(
        "lua",
        &LanguageConfig::new(
            "lua",
            tree_sitter::Language::new(tree_sitter_lua::LANGUAGE),
            vec![],
            tree_sitter_lua::HIGHLIGHTS_QUERY,
            "",
            "",
        ),
    );

    registry.register(
        "python",
        &LanguageConfig::new(
            "python",
            tree_sitter::Language::new(tree_sitter_python::LANGUAGE),
            vec![],
            tree_sitter_python::HIGHLIGHTS_QUERY,
            "",
            "",
        ),
    );

    registry.register(
        "bash",
        &LanguageConfig::new(
            "bash",
            tree_sitter::Language::new(tree_sitter_bash::LANGUAGE),
            vec![],
            tree_sitter_bash::HIGHLIGHT_QUERY,
            "",
            "",
        ),
    );

    // Empty highlight query: parses with the JSON grammar but emits no
    // highlight spans, matching gpui-component's own `Language::Plain`,
    // which only exists behind the `tree-sitter-languages` feature we no
    // longer enable. Registered under both names because the object-store
    // text editor's unknown-extension fallback (`object_text::editor_language`)
    // resolves to "text", while `QueryLanguage::editor_mode()` uses "plaintext".
    let plaintext = LanguageConfig::new(
        "plaintext",
        tree_sitter::Language::new(tree_sitter_json::LANGUAGE),
        vec![],
        "",
        "",
        "",
    );
    registry.register("plaintext", &plaintext);
    registry.register("text", &plaintext);
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::QueryLanguage;

    /// The guard: every `QueryLanguage::editor_mode()` value except
    /// "plaintext" must have a grammar registered, so a future driver with a
    /// new query language fails this test instead of silently shipping
    /// unhighlighted.
    #[test]
    fn every_editor_mode_except_plaintext_is_registered() {
        register_languages();
        let registry = LanguageRegistry::singleton();

        for language in all_query_languages() {
            let mode = language.editor_mode();
            if mode == "plaintext" {
                continue;
            }

            assert!(
                REGISTERED_LANGUAGES.contains(&mode),
                "QueryLanguage {language:?} returns editor_mode {mode:?}, \
                 which has no registered grammar"
            );
            assert!(
                registry.language(mode).is_some(),
                "editor_mode {mode:?} is not registered in LanguageRegistry"
            );
        }
    }

    fn all_query_languages() -> Vec<QueryLanguage> {
        vec![
            QueryLanguage::Sql,
            QueryLanguage::CloudWatchLogsInsightsQl,
            QueryLanguage::OpenSearchPpl,
            QueryLanguage::OpenSearchSql,
            QueryLanguage::MongoQuery,
            QueryLanguage::RedisCommands,
            QueryLanguage::Cypher,
            QueryLanguage::InfluxQuery,
            QueryLanguage::Flux,
            QueryLanguage::Cql,
            QueryLanguage::Lua,
            QueryLanguage::Python,
            QueryLanguage::Bash,
            QueryLanguage::Custom("custom".to_string()),
        ]
    }
}
