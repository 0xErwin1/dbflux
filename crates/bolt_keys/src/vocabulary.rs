//! The context vocabulary: the context tree a product supplies as data.
//!
//! A context is a keyboard scope: a window root or an element that sets its
//! own identifier. The product owns the tree — which contexts exist, how
//! they nest, what predicate each applies under by default, which ones keep
//! the global chords, and which are element contexts — and hands it to the
//! engine as one [`KeymapVocabulary`] value. The engine never names a
//! context; it refers to contexts by their persisted id strings, so a key
//! context rename is a product decision that cannot leak into, or be
//! changed by, the engine.

use std::borrow::Cow;

/// One context descriptor of the product's context tree.
///
/// `id` is the persisted context name (the string users' keybinding
/// overrides store); `parent` is `None` for a root. `default_predicate` is
/// the context predicate a binding of the context applies under unless the
/// user moved it. `inherits_global_chords` marks the text-entry contexts
/// that keep the global layer's chords without inheriting the layer, and
/// `is_element_context` marks the contexts an element sets on itself, whose
/// bindings are registered after the window roots' so a longer key sequence
/// keeps waiting over an earlier single-key binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContextSpec {
    pub id: &'static str,
    pub parent: Option<&'static str>,
    pub default_predicate: &'static str,
    pub inherits_global_chords: bool,
    pub is_element_context: bool,
}

impl ContextSpec {
    /// A non-element context that does not keep the global chords.
    pub fn new(
        id: &'static str,
        parent: Option<&'static str>,
        default_predicate: &'static str,
    ) -> Self {
        Self {
            id,
            parent,
            default_predicate,
            inherits_global_chords: false,
            is_element_context: false,
        }
    }

    /// Marks the context as keeping the global chords.
    pub fn keeps_global_chords(mut self) -> Self {
        self.inherits_global_chords = true;
        self
    }

    /// Marks the context as an element context.
    pub fn as_element(mut self) -> Self {
        self.is_element_context = true;
        self
    }
}

/// The product's context tree, in the order the product declares it.
///
/// The declaration order is the canonical order: the engine lists bindings
/// layer by layer in this order (the settings page, the registration order
/// of the generated native bindings) and builds effective keymaps in it.
/// The vocabulary also knows which context is the global one, the root
/// every other context may fall back to, and — for the persisted rows —
/// how an action id maps to the command id the product stores (the two
/// differ only for commands that carry an argument; the stored id names the
/// command's family and the row's default keys pick the binding).
#[derive(Debug, Clone)]
pub struct KeymapVocabulary {
    contexts: Vec<ContextSpec>,
    global_id: &'static str,
    command_storage_id: fn(Cow<'static, str>) -> Cow<'static, str>,
}

/// The default storage mapping: the action id as it is.
fn identity_command_id(action_id: Cow<'static, str>) -> Cow<'static, str> {
    action_id
}

impl KeymapVocabulary {
    /// A vocabulary of `contexts` in canonical order, with `global_id`
    /// naming the global context.
    ///
    /// A spec whose `parent` names no context of the vocabulary is treated
    /// as a root: the lookup stops there.
    pub fn new(contexts: Vec<ContextSpec>, global_id: &'static str) -> Self {
        Self {
            contexts,
            global_id,
            command_storage_id: identity_command_id,
        }
    }

    /// Sets how an action id maps to the command id the product persists.
    /// The default is the identity; a product whose commands carry an
    /// argument installs its own mapping so stored rows keep their shape.
    pub fn with_command_storage_id(
        mut self,
        command_storage_id: fn(Cow<'static, str>) -> Cow<'static, str>,
    ) -> Self {
        self.command_storage_id = command_storage_id;
        self
    }

    /// The command id the product persists for the binding named by
    /// `action_id`.
    pub fn storage_command_id(&self, action_id: Cow<'static, str>) -> Cow<'static, str> {
        (self.command_storage_id)(action_id)
    }

    /// The spec of context `id`, or `None` when the vocabulary does not
    /// declare it.
    pub fn get(&self, id: &str) -> Option<&ContextSpec> {
        self.contexts.iter().find(|spec| spec.id == id)
    }

    /// The id of the context's parent, or `None` for a root, an undeclared
    /// context, or a context whose parent is undeclared.
    pub fn parent_of(&self, id: &str) -> Option<&'static str> {
        let parent = self.get(id)?.parent?;
        self.get(parent).map(|_| parent)
    }

    /// The default predicate of context `id`. An undeclared context has no
    /// default predicate; a binding under it applies under its slot's own
    /// stored predicate.
    pub fn default_predicate(&self, id: &str) -> Option<&'static str> {
        self.get(id).map(|spec| spec.default_predicate)
    }

    /// Whether context `id` keeps the global chords without inheriting the
    /// global layer.
    pub fn inherits_global_chords(&self, id: &str) -> bool {
        self.get(id).is_some_and(|spec| spec.inherits_global_chords)
    }

    /// Whether context `id` is an element context.
    pub fn is_element_context(&self, id: &str) -> bool {
        self.get(id).is_some_and(|spec| spec.is_element_context)
    }

    /// The context ids in canonical order.
    pub fn context_ids(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.contexts.iter().map(|spec| spec.id)
    }

    /// The id of the global context, the root every other context may fall
    /// back to.
    pub fn global_id(&self) -> &'static str {
        self.global_id
    }

    /// The id of the context whose default predicate is `predicate`, or
    /// `None` when no context declares it.
    pub fn context_for_default_predicate(&self, predicate: &str) -> Option<&'static str> {
        self.contexts
            .iter()
            .find(|spec| spec.default_predicate == predicate)
            .map(|spec| spec.id)
    }

    /// Whether a key pressed in one of the two contexts can reach bindings
    /// of the other: they are the same context, or one inherits from the
    /// other.
    pub fn contexts_overlap(&self, first: &str, second: &str) -> bool {
        first == second || self.is_ancestor(first, second) || self.is_ancestor(second, first)
    }

    /// Whether `ancestor` is `context` itself or one of its ancestors.
    pub fn is_ancestor(&self, ancestor: &str, context: &str) -> bool {
        let mut current = self.parent_of(context);

        while let Some(parent) = current {
            if parent == ancestor {
                return true;
            }
            current = self.parent_of(parent);
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vocabulary() -> KeymapVocabulary {
        KeymapVocabulary::new(
            vec![
                ContextSpec::new("global", None, "Global"),
                ContextSpec::new("editor", Some("global"), "Editor"),
                ContextSpec::new("sidebar", Some("global"), "Sidebar"),
                ContextSpec::new("text_input", None, "TextInput").keeps_global_chords(),
                ContextSpec::new("command_palette", None, "CommandPalette"),
                ContextSpec::new("document_tree", Some("results"), "DocumentTree").as_element(),
                ContextSpec::new("results", Some("global"), "Results"),
            ],
            "global",
        )
    }

    #[test]
    fn parents_walk_up_to_the_root() {
        let vocabulary = vocabulary();

        assert_eq!(vocabulary.parent_of("editor"), Some("global"));
        assert_eq!(vocabulary.parent_of("document_tree"), Some("results"));
        assert_eq!(vocabulary.parent_of("global"), None);
        assert_eq!(vocabulary.parent_of("command_palette"), None);
    }

    #[test]
    fn an_undeclared_parent_stops_the_chain() {
        let vocabulary = KeymapVocabulary::new(
            vec![ContextSpec::new("orphan", Some("missing"), "Orphan")],
            "orphan",
        );

        assert_eq!(vocabulary.parent_of("orphan"), None);
        assert!(vocabulary.contexts_overlap("orphan", "missing") == ("orphan" == "missing"));
    }

    #[test]
    fn overlap_follows_the_tree() {
        let vocabulary = vocabulary();

        assert!(vocabulary.contexts_overlap("editor", "editor"));
        assert!(vocabulary.contexts_overlap("global", "editor"));
        assert!(vocabulary.contexts_overlap("global", "document_tree"));
        assert!(vocabulary.contexts_overlap("results", "document_tree"));
        assert!(!vocabulary.contexts_overlap("editor", "sidebar"));
        assert!(!vocabulary.contexts_overlap("text_input", "global"));
        assert!(!vocabulary.contexts_overlap("command_palette", "global"));
    }

    #[test]
    fn contexts_are_found_by_their_default_predicate() {
        let vocabulary = vocabulary();

        assert_eq!(
            vocabulary.context_for_default_predicate("DocumentTree"),
            Some("document_tree")
        );
        assert_eq!(
            vocabulary.context_for_default_predicate("Sidebar"),
            Some("sidebar")
        );
        assert_eq!(vocabulary.context_for_default_predicate("Nonsense"), None);
    }

    #[test]
    fn flags_and_order_come_from_the_specs() {
        let vocabulary = vocabulary();

        assert!(vocabulary.inherits_global_chords("text_input"));
        assert!(!vocabulary.inherits_global_chords("editor"));
        assert!(vocabulary.is_element_context("document_tree"));
        assert!(!vocabulary.is_element_context("results"));
        assert_eq!(vocabulary.global_id(), "global");

        let ids: Vec<&str> = vocabulary.context_ids().collect();
        assert_eq!(
            ids,
            vec![
                "global",
                "editor",
                "sidebar",
                "text_input",
                "command_palette",
                "document_tree",
                "results",
            ]
        );
    }
}
