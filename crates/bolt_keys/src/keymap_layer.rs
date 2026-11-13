//! Keymap layers and hierarchical lookup across contexts.
//!
//! A [`KeymapLayer`] holds the bindings declared for one context; a
//! [`KeymapStack`] holds one layer per context and resolves keys by walking
//! the context tree of the [`KeymapVocabulary`] the product supplied.
//! Key dispatch itself goes through the product's native key bindings; the
//! stack answers lookups such as the keys a command has, for shortcut
//! labels.

use std::borrow::Cow;
use std::collections::HashMap;

use super::vocabulary::KeymapVocabulary;
use super::{KeyChord, KeySequence};

/// A single layer of keybindings for a specific context.
///
/// Besides the lookup table the layer keeps the order in which key sequences
/// were first bound, so lists built from it (the settings page) follow the
/// order the keymap declares them in. Commands are action-id strings of the
/// product's own vocabulary; the engine compares and stores them as given.
#[derive(Clone)]
pub struct KeymapLayer {
    context: &'static str,
    bindings: HashMap<KeySequence, Cow<'static, str>>,
    order: Vec<KeySequence>,
    /// Bindings that match a narrower predicate than the context's own.
    predicates: HashMap<KeySequence, &'static str>,
}

impl KeymapLayer {
    pub fn new(context: &'static str) -> Self {
        Self {
            context,
            bindings: HashMap::new(),
            order: Vec::new(),
            predicates: HashMap::new(),
        }
    }

    /// Binds `keys` (a chord or a key sequence) to `command`. Binding the
    /// same keys again replaces their command and keeps their original
    /// position.
    pub fn bind(&mut self, keys: impl Into<KeySequence>, command: impl Into<Cow<'static, str>>) {
        let keys = keys.into();
        let command = command.into();

        if self.bindings.insert(keys.clone(), command).is_none() {
            self.order.push(keys);
        }
    }

    /// Binds `keys` to `command` under `predicate` instead of the context's
    /// default predicate, for a binding that must stay out of part of the
    /// context (a text field inside it, for example).
    pub fn bind_with_predicate(
        &mut self,
        keys: impl Into<KeySequence>,
        command: impl Into<Cow<'static, str>>,
        predicate: &'static str,
    ) {
        let keys = keys.into();
        let command = command.into();
        self.predicates.insert(keys.clone(), predicate);
        self.bind(keys, command);
    }

    /// The default predicate of the binding on `keys`.
    pub fn predicate_for(&self, vocabulary: &KeymapVocabulary, keys: &KeySequence) -> &'static str {
        self.predicates.get(keys).copied().unwrap_or_else(|| {
            vocabulary
                .default_predicate(self.context)
                .unwrap_or_default()
        })
    }

    /// Bindings of this layer in the order their keys were first bound.
    pub fn ordered_bindings(&self) -> impl Iterator<Item = (&KeySequence, &Cow<'static, str>)> {
        self.order
            .iter()
            .filter_map(|keys| self.bindings.get(keys).map(|command| (keys, command)))
    }

    /// The command bound to the single chord `chord`.
    pub fn get(&self, chord: &KeyChord) -> Option<Cow<'static, str>> {
        self.get_sequence(&KeySequence::from(chord.clone()))
    }

    pub fn get_sequence(&self, keys: &KeySequence) -> Option<Cow<'static, str>> {
        self.bindings.get(keys).cloned()
    }

    pub fn context(&self) -> &'static str {
        self.context
    }

    #[allow(dead_code)]
    pub fn bindings(&self) -> &HashMap<KeySequence, Cow<'static, str>> {
        &self.bindings
    }

    /// This layer with the leader placeholder in its keys replaced by
    /// `leader`, keeping the order and predicates of its bindings.
    pub(super) fn with_leader(&self, leader: &KeyChord) -> KeymapLayer {
        let mut layer = KeymapLayer::new(self.context);

        for (keys, command) in self.ordered_bindings() {
            let resolved = keys.with_leader(leader);

            if let Some(predicate) = self.predicates.get(keys) {
                layer.bind_with_predicate(resolved, command.clone(), predicate);
            } else {
                layer.bind(resolved, command.clone());
            }
        }

        layer
    }
}

/// Manages keybindings across all contexts with hierarchical resolution.
///
/// When resolving keys, the stack first checks the current context, then
/// falls back to parent contexts (ending at the global context) if no match
/// is found. A context that keeps the global chords (see
/// [`ContextSpec::inherits_global_chords`]) falls back last to the global
/// bindings on a Ctrl or Cmd chord.
#[derive(Clone, Default)]
pub struct KeymapStack {
    layers: HashMap<&'static str, KeymapLayer>,
}

impl KeymapStack {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a layer to the stack.
    pub fn add_layer(&mut self, layer: KeymapLayer) {
        self.layers.insert(layer.context, layer);
    }

    /// Returns the layer holding the bindings declared for `context` itself.
    pub fn layer(&self, context: &'static str) -> Option<&KeymapLayer> {
        self.layers.get(&context)
    }

    /// This stack with every leader placeholder replaced by `leader`: the
    /// keys the user presses, for lookups and labels.
    pub fn with_leader(&self, leader: &KeyChord) -> KeymapStack {
        KeymapStack {
            layers: self
                .layers
                .iter()
                .map(|(context, layer)| (*context, layer.with_leader(leader)))
                .collect(),
        }
    }

    /// Resolves a single chord to a command, checking the given context
    /// first, then falling back to parent contexts.
    pub fn resolve(
        &self,
        vocabulary: &KeymapVocabulary,
        context: &'static str,
        chord: &KeyChord,
    ) -> Option<Cow<'static, str>> {
        self.resolve_sequence(vocabulary, context, &KeySequence::from(chord.clone()))
    }

    /// Resolves a key sequence to a command, checking the given context
    /// first, then falling back to parent contexts.
    pub fn resolve_sequence(
        &self,
        vocabulary: &KeymapVocabulary,
        context: &'static str,
        keys: &KeySequence,
    ) -> Option<Cow<'static, str>> {
        self.lookup_chain(vocabulary, context)
            .into_iter()
            .filter(|(_, chords_only)| !chords_only || keys.is_global_chord())
            .find_map(|(layer, _)| layer.get_sequence(keys))
    }

    /// The layers a key pressed in `context` reaches, in lookup order: the
    /// context's own, its ancestors', and last the global layer when the
    /// context keeps only its chords (the flag is `true` for that entry).
    fn lookup_chain<'a>(
        &'a self,
        vocabulary: &KeymapVocabulary,
        context: &'static str,
    ) -> Vec<(&'a KeymapLayer, bool)> {
        let mut chain = Vec::new();
        let mut current = Some(context);

        while let Some(ctx) = current {
            if let Some(layer) = self.layers.get(&ctx) {
                chain.push((layer, false));
            }
            current = vocabulary.parent_of(ctx);
        }

        if vocabulary.inherits_global_chords(context)
            && let Some(global) = self.layers.get(vocabulary.global_id())
        {
            chain.push((global, true));
        }

        chain
    }

    /// Returns all keybindings for a given context, including inherited ones.
    pub fn bindings_for_context(
        &self,
        vocabulary: &KeymapVocabulary,
        context: &'static str,
    ) -> Vec<(KeySequence, Cow<'static, str>, &'static str)> {
        let mut result = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for (layer, chords_only) in self.lookup_chain(vocabulary, context) {
            for (keys, command) in layer.ordered_bindings() {
                if chords_only && !keys.is_global_chord() {
                    continue;
                }

                if seen.insert(keys.clone()) {
                    result.push((keys.clone(), command.clone(), layer.context()));
                }
            }
        }

        result
    }

    /// Returns the shortcut string for a command in the given context, if any.
    pub fn shortcut_for_command(
        &self,
        vocabulary: &KeymapVocabulary,
        context: &'static str,
        command: &str,
    ) -> Option<String> {
        self.keys_for_command(vocabulary, context, command)
            .map(|keys| keys.to_string())
    }

    /// Returns the keys bound to a command in the given context, falling
    /// back to parent contexts the same way [`KeymapStack::resolve`] does.
    ///
    /// When one layer binds the command to several key sequences, the one
    /// bound first is returned.
    pub fn keys_for_command(
        &self,
        vocabulary: &KeymapVocabulary,
        context: &'static str,
        command: &str,
    ) -> Option<&KeySequence> {
        self.lookup_chain(vocabulary, context)
            .into_iter()
            .find_map(|(layer, chords_only)| {
                layer
                    .ordered_bindings()
                    .find(|(keys, bound_command)| {
                        bound_command.as_ref() == command
                            && (!chords_only || keys.is_global_chord())
                    })
                    .map(|(keys, _)| keys)
            })
    }

    /// The first chord of the keys bound to `command`, see
    /// [`KeymapStack::keys_for_command`].
    pub fn chord_for_command(
        &self,
        vocabulary: &KeymapVocabulary,
        context: &'static str,
        command: &str,
    ) -> Option<&KeyChord> {
        self.keys_for_command(vocabulary, context, command)
            .map(KeySequence::first)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ContextSpec, Modifiers};

    fn vocabulary() -> KeymapVocabulary {
        KeymapVocabulary::new(
            vec![
                ContextSpec::new("global", None, "Global"),
                ContextSpec::new("editor", Some("global"), "Editor"),
                ContextSpec::new("sidebar", Some("global"), "Sidebar"),
                ContextSpec::new("text_input", None, "TextInput").keeps_global_chords(),
                ContextSpec::new("confirm_modal", None, "ConfirmModal"),
                ContextSpec::new("command_palette", None, "CommandPalette"),
                ContextSpec::new("document_tree", None, "DocumentTree").as_element(),
                ContextSpec::new("vim_normal", None, "VimNormal"),
            ],
            "global",
        )
    }

    #[test]
    fn resolves_in_context() {
        let mut stack = KeymapStack::new();

        let mut global = KeymapLayer::new("global");
        global.bind(
            KeyChord::new("p", Modifiers::ctrl_shift()),
            "toggle_command_palette",
        );

        let mut sidebar = KeymapLayer::new("sidebar");
        sidebar.bind(KeyChord::new("j", Modifiers::none()), "select_next");

        stack.add_layer(global);
        stack.add_layer(sidebar);

        let chord_j = KeyChord::new("j", Modifiers::none());
        assert_eq!(
            stack.resolve(&vocabulary(), "sidebar", &chord_j),
            Some(Cow::Borrowed("select_next"))
        );
        assert_eq!(stack.resolve(&vocabulary(), "editor", &chord_j), None);
    }

    #[test]
    fn with_leader_resolves_the_placeholder_and_keeps_order_and_predicates() {
        let leader_a = KeySequence::new(vec![
            KeyChord::leader(),
            KeyChord::new("a", Modifiers::none()),
        ])
        .expect("two chords");
        let leader_r = KeySequence::new(vec![
            KeyChord::leader(),
            KeyChord::new("r", Modifiers::none()),
        ])
        .expect("two chords");

        let mut layer = KeymapLayer::new("vim_normal");
        layer.bind(leader_a, "open_pane_actions");
        layer.bind_with_predicate(leader_r, "run_query", "VimNormal");

        let mut stack = KeymapStack::new();
        stack.add_layer(layer);

        let resolved = stack.with_leader(&KeyChord::new(",", Modifiers::none()));
        let layer = resolved.layer("vim_normal").expect("layer kept");
        let bindings: Vec<(String, &str)> = layer
            .ordered_bindings()
            .map(|(keys, command)| (keys.to_storage_string(), command.as_ref()))
            .collect();

        assert_eq!(
            bindings,
            vec![
                (", a".to_string(), "open_pane_actions"),
                (", r".to_string(), "run_query"),
            ]
        );
        assert_eq!(
            layer.predicate_for(&vocabulary(), &KeySequence::parse(", r").expect("valid"),),
            "VimNormal"
        );
    }

    #[test]
    fn falls_back_to_global() {
        let mut stack = KeymapStack::new();

        let mut global = KeymapLayer::new("global");
        global.bind(
            KeyChord::new("p", Modifiers::ctrl_shift()),
            "toggle_command_palette",
        );

        stack.add_layer(global);

        let chord = KeyChord::new("p", Modifiers::ctrl_shift());

        assert_eq!(
            stack.resolve(&vocabulary(), "sidebar", &chord),
            Some(Cow::Borrowed("toggle_command_palette"))
        );
        assert_eq!(
            stack.resolve(&vocabulary(), "editor", &chord),
            Some(Cow::Borrowed("toggle_command_palette"))
        );
    }

    #[test]
    fn a_detached_context_does_not_fall_back() {
        let mut stack = KeymapStack::new();

        let mut global = KeymapLayer::new("global");
        global.bind(
            KeyChord::new("p", Modifiers::ctrl_shift()),
            "toggle_command_palette",
        );

        let palette = KeymapLayer::new("command_palette");

        stack.add_layer(global);
        stack.add_layer(palette);

        let chord = KeyChord::new("p", Modifiers::ctrl_shift());
        assert_eq!(
            stack.resolve(&vocabulary(), "command_palette", &chord),
            None
        );
    }

    #[test]
    fn text_entry_contexts_fall_back_to_global_chords_only() {
        let mut global = KeymapLayer::new("global");
        global.bind(
            KeyChord::new("p", Modifiers::ctrl_shift()),
            "toggle_command_palette",
        );
        global.bind(KeyChord::new("tab", Modifiers::none()), "cycle_focus");
        global.bind(KeyChord::new("h", Modifiers::alt()), "toggle_history");

        let mut text_input = KeymapLayer::new("text_input");
        text_input.bind(KeyChord::new("escape", Modifiers::none()), "cancel");

        let mut stack = KeymapStack::new();
        stack.add_layer(global);
        stack.add_layer(text_input);

        let palette = KeyChord::new("p", Modifiers::ctrl_shift());
        let tab = KeyChord::new("tab", Modifiers::none());
        let alt_h = KeyChord::new("h", Modifiers::alt());

        assert_eq!(
            stack.resolve(&vocabulary(), "text_input", &palette),
            Some(Cow::Borrowed("toggle_command_palette"))
        );
        assert_eq!(stack.resolve(&vocabulary(), "text_input", &tab), None);
        assert_eq!(stack.resolve(&vocabulary(), "text_input", &alt_h), None);
        assert_eq!(
            stack.resolve(&vocabulary(), "confirm_modal", &palette),
            None
        );

        assert_eq!(
            stack.keys_for_command(&vocabulary(), "text_input", "toggle_command_palette"),
            Some(&KeySequence::from(palette.clone()))
        );
        assert_eq!(
            stack.keys_for_command(&vocabulary(), "text_input", "cycle_focus"),
            None
        );

        let listed: Vec<String> = stack
            .bindings_for_context(&vocabulary(), "text_input")
            .into_iter()
            .map(|(_, command, _)| command.into_owned())
            .collect();
        assert_eq!(listed, vec!["cancel", "toggle_command_palette"]);
    }

    #[test]
    fn sequences_resolve_only_as_a_whole() {
        let mut tree = KeymapLayer::new("document_tree");
        let d_d = KeySequence::parse("d d").expect("valid sequence");
        tree.bind(d_d.clone(), "delete");

        let mut stack = KeymapStack::new();
        stack.add_layer(tree);

        assert_eq!(
            stack.resolve_sequence(&vocabulary(), "document_tree", &d_d),
            Some(Cow::Borrowed("delete"))
        );
        assert_eq!(
            stack.resolve(
                &vocabulary(),
                "document_tree",
                &KeyChord::new("d", Modifiers::none())
            ),
            None
        );
        assert_eq!(
            stack.keys_for_command(&vocabulary(), "document_tree", "delete"),
            Some(&d_d)
        );
    }

    #[test]
    fn chord_for_command_prefers_context_then_falls_back_to_parent() {
        let mut stack = KeymapStack::new();

        let mut global = KeymapLayer::new("global");
        global.bind(
            KeyChord::new("n", Modifiers::ctrl_shift()),
            "open_connection_manager",
        );

        let mut sidebar = KeymapLayer::new("sidebar");
        sidebar.bind(
            KeyChord::new("c", Modifiers::none()),
            "open_connection_manager",
        );

        stack.add_layer(global);
        stack.add_layer(sidebar);

        let global_chord = KeyChord::new("n", Modifiers::ctrl_shift());
        let sidebar_chord = KeyChord::new("c", Modifiers::none());

        assert_eq!(
            stack.chord_for_command(&vocabulary(), "global", "open_connection_manager"),
            Some(&global_chord)
        );
        assert_eq!(
            stack.chord_for_command(&vocabulary(), "editor", "open_connection_manager"),
            Some(&global_chord)
        );
        assert_eq!(
            stack.chord_for_command(&vocabulary(), "sidebar", "open_connection_manager"),
            Some(&sidebar_chord)
        );
        assert_eq!(
            stack.chord_for_command(&vocabulary(), "command_palette", "open_connection_manager"),
            None
        );
        assert_eq!(
            stack.shortcut_for_command(&vocabulary(), "global", "open_connection_manager"),
            Some(global_chord.to_string())
        );
    }
}
