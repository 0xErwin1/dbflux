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
            &sql_highlights_query(),
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

/// SQL captures that correct the grammar's own query. They come first
/// because the first capture of a node wins, and each falls back to an
/// existing role (`keyword.conditional` to `keyword`).
const SQL_HIGHLIGHT_FIXES: &str = r#"
; The grammar's number patterns use Lua `%d`, which a regex never matches,
; so numbers fell through to the string capture.
((literal) @number
  (#match? @number "^[-+]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][-+]?[0-9]+)?$"))
; `conditional` has no style, so these had no color.
[
  (keyword_case)
  (keyword_when)
  (keyword_then)
  (keyword_else)
] @keyword.conditional
; The grammar captures these as `type.qualifier`, which falls back to the
; type color.
[
 (keyword_restrict)
 (keyword_unbounded)
 (keyword_unique)
 (keyword_cascade)
 (keyword_delayed)
 (keyword_high_priority)
 (keyword_low_priority)
 (keyword_ignore)
 (keyword_nothing)
 (keyword_check)
 (keyword_option)
 (keyword_local)
 (keyword_cascaded)
 (keyword_wait)
 (keyword_nowait)
 (keyword_metadata)
 (keyword_incremental)
 (keyword_bin_pack)
 (keyword_noscan)
 (keyword_stats)
 (keyword_statistics)
 (keyword_maxvalue)
 (keyword_minvalue)
] @keyword.modifier
"#;

fn sql_highlights_query() -> String {
    format!(
        "{SQL_HIGHLIGHT_FIXES}\n{}",
        tree_sitter_sequel::HIGHLIGHTS_QUERY
    )
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
    /// Highlight roles of each token in `source`, by capture name.
    fn sql_roles(source: &str) -> Vec<(String, String)> {
        use gpui_component::highlighter::{HighlightTheme, SyntaxHighlighter};

        let names = [
            "keyword",
            "function",
            "type",
            "variable",
            "string",
            "number",
            "operator",
            "punctuation.delimiter",
            "punctuation.bracket",
            "attribute",
            "comment",
        ];
        let mut syntax = serde_json::Map::new();
        for (index, name) in names.iter().enumerate() {
            syntax.insert(
                name.to_string(),
                serde_json::json!({ "color": format!("#0000{:02x}", index + 1) }),
            );
        }
        let theme: HighlightTheme = serde_json::from_value(serde_json::json!({
            "name": "probe",
            "appearance": "dark",
            "style": { "syntax": syntax },
        }))
        .expect("probe theme");

        register_languages();
        let rope = gpui_component::Rope::from(source);
        let mut highlighter = SyntaxHighlighter::new("sql");
        highlighter.update(None, &rope, None);

        highlighter
            .styles(&(0..source.len()), &theme)
            .into_iter()
            .filter_map(|(range, style)| {
                let color = style.color?;
                let index = names.iter().position(|name| {
                    theme.style.syntax.style(name).and_then(|style| style.color) == Some(color)
                })?;
                let text = source[range].trim().to_string();
                (!text.is_empty()).then(|| (text, names[index].to_string()))
            })
            .collect()
    }

    #[test]
    fn sql_numbers_and_keywords_take_their_roles() {
        let roles = sql_roles(
            "SELECT 1, 1.5, 1e3, '7' FROM t WHERE CASE WHEN x THEN 1 ELSE 0 END = 1;\n\
             CREATE TABLE u (id integer UNIQUE REFERENCES t ON DELETE CASCADE);",
        );
        let role_of = |text: &str| {
            roles
                .iter()
                .find(|(token, _)| token == text)
                .map(|(_, role)| role.as_str())
        };

        for number in ["1", "1.5", "1e3"] {
            assert_eq!(role_of(number), Some("number"), "{number}");
        }
        assert_eq!(role_of("'7'"), Some("string"));
        for keyword in ["CASE", "WHEN", "THEN", "ELSE", "UNIQUE", "CASCADE"] {
            assert_eq!(role_of(keyword), Some("keyword"), "{keyword}");
        }
    }
}
