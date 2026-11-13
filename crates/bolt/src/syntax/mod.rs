//! Tree-sitter grammars for the code highlighter.
//!
//! The code editor and every highlighted view resolve a language name through
//! one shared registry. [`Grammar`] registers a grammar there without the
//! product touching gpui-component's highlighter types, and [`bundled`] holds
//! the grammars bolt ships behind its opt-in `syntax-*` features.

pub mod bundled;
mod highlight;

pub use highlight::{highlight, overlay};

use gpui::SharedString;
use gpui_component::highlighter::{LanguageConfig, LanguageRegistry};

/// The tree-sitter crate the grammars are built on, so a product names
/// `tree_sitter::Language` without pinning the version itself.
pub use tree_sitter;

/// A tree-sitter grammar and its queries, ready to register.
#[derive(Clone, Debug)]
pub struct Grammar {
    name: SharedString,
    language: tree_sitter::Language,
    highlights: String,
    injections: String,
    locals: String,
}

impl Grammar {
    /// A grammar named `name` that highlights with the `highlights` query.
    ///
    /// The query's capture names are the syntax roles (`keyword`, `string`,
    /// `property`, ...); a capture without a style of its own takes its
    /// prefix's, so `keyword.modifier` reads as `keyword`.
    pub fn new(
        name: impl Into<SharedString>,
        language: impl Into<tree_sitter::Language>,
        highlights: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            language: language.into(),
            highlights: highlights.into(),
            injections: String::new(),
            locals: String::new(),
        }
    }

    /// An injections query: the nodes it captures as `injection.content` are
    /// highlighted with the grammar registered under the name it sets as
    /// `injection.language`. An injected language nobody registered stays
    /// unhighlighted.
    pub fn injections(mut self, query: impl Into<String>) -> Self {
        self.injections = query.into();
        self
    }

    /// A locals query, for grammars whose highlights tell a local variable
    /// from a global one.
    pub fn locals(mut self, query: impl Into<String>) -> Self {
        self.locals = query.into();
        self
    }

    /// The name the grammar registers under.
    pub fn name(&self) -> &SharedString {
        &self.name
    }

    /// Register the grammar under its own name, replacing any grammar
    /// registered there before. Call it before the first editor or view asks
    /// for the language.
    pub fn register(&self) {
        self.register_as(&self.name);
    }

    /// Register the grammar under `name` as well, such as `text` beside
    /// `plaintext`.
    pub fn register_as(&self, name: &str) {
        LanguageRegistry::singleton().register(
            name,
            &LanguageConfig::new(
                self.name.clone(),
                self.language.clone(),
                Vec::new(),
                &self.highlights,
                &self.injections,
                &self.locals,
            ),
        );
    }
}

/// Whether a grammar is registered under `name`.
pub fn is_registered(name: &str) -> bool {
    LanguageRegistry::singleton().language(name).is_some()
}

/// Register every grammar the enabled `syntax-*` features bundle.
pub fn register_bundled() {
    for grammar in bundled::all() {
        grammar.register();
    }
}

#[cfg(test)]
mod tests;
