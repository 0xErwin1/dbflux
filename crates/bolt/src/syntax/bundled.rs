//! The grammars bolt bundles, each behind its own cargo feature.
//!
//! | Feature | Grammar | Crate |
//! |---|---|---|
//! | `syntax-json` | `json` | `tree-sitter-json` |
//! | `syntax-javascript` | `javascript` | `tree-sitter-javascript` |
//! | `syntax-bash` | `bash` | `tree-sitter-bash` |
//! | `syntax-python` | `python` | `tree-sitter-python` |
//! | `syntax-xml` | `xml` | `tree-sitter-xml` |
//! | `syntax-html` | `html` | `tree-sitter-html` |

use super::Grammar;

/// JSON. Keys take `property`: the crate's own query captures them as
/// `string.special.key`, which the highlighter would paint as a string.
#[cfg(feature = "syntax-json")]
pub fn json() -> Grammar {
    Grammar::new(
        "json",
        tree_sitter_json::LANGUAGE,
        include_str!("queries/json.scm"),
    )
}

/// JavaScript, with the crate's highlights query.
#[cfg(feature = "syntax-javascript")]
pub fn javascript() -> Grammar {
    Grammar::new(
        "javascript",
        tree_sitter_javascript::LANGUAGE,
        tree_sitter_javascript::HIGHLIGHT_QUERY,
    )
}

/// Bash, with the crate's highlights query.
#[cfg(feature = "syntax-bash")]
pub fn bash() -> Grammar {
    Grammar::new(
        "bash",
        tree_sitter_bash::LANGUAGE,
        tree_sitter_bash::HIGHLIGHT_QUERY,
    )
}

/// Python, with the crate's highlights query.
#[cfg(feature = "syntax-python")]
pub fn python() -> Grammar {
    Grammar::new(
        "python",
        tree_sitter_python::LANGUAGE,
        tree_sitter_python::HIGHLIGHTS_QUERY,
    )
}

/// XML documents (not DTDs), with the crate's highlights query.
#[cfg(feature = "syntax-xml")]
pub fn xml() -> Grammar {
    Grammar::new(
        "xml",
        tree_sitter_xml::LANGUAGE_XML,
        tree_sitter_xml::XML_HIGHLIGHT_QUERY,
    )
}

/// HTML, with the crate's highlights and injections: a script element is
/// highlighted as `javascript` and a style element as `css` when those are
/// registered.
#[cfg(feature = "syntax-html")]
pub fn html() -> Grammar {
    Grammar::new(
        "html",
        tree_sitter_html::LANGUAGE,
        tree_sitter_html::HIGHLIGHTS_QUERY,
    )
    .injections(tree_sitter_html::INJECTIONS_QUERY)
}

/// Every grammar the enabled features bundle.
#[allow(clippy::vec_init_then_push, unused_mut)]
pub fn all() -> Vec<Grammar> {
    let mut grammars = Vec::new();
    #[cfg(feature = "syntax-json")]
    grammars.push(json());
    #[cfg(feature = "syntax-javascript")]
    grammars.push(javascript());
    #[cfg(feature = "syntax-bash")]
    grammars.push(bash());
    #[cfg(feature = "syntax-python")]
    grammars.push(python());
    #[cfg(feature = "syntax-xml")]
    grammars.push(xml());
    #[cfg(feature = "syntax-html")]
    grammars.push(html());
    grammars
}
