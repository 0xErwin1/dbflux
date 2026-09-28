use std::collections::HashMap;

use super::{Command, ContextId, KeyChord, KeySequence};

/// A single layer of keybindings for a specific context.
///
/// Besides the lookup table the layer keeps the order in which key sequences
/// were first bound, so lists built from it (the settings page) follow the
/// order the keymap declares them in.
#[derive(Clone)]
pub struct KeymapLayer {
    context: ContextId,
    bindings: HashMap<KeySequence, Command>,
    order: Vec<KeySequence>,
    /// Bindings that match a narrower predicate than the context's own.
    predicates: HashMap<KeySequence, &'static str>,
}

impl KeymapLayer {
    pub fn new(context: ContextId) -> Self {
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
    pub fn bind(&mut self, keys: impl Into<KeySequence>, command: Command) {
        let keys = keys.into();

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
        command: Command,
        predicate: &'static str,
    ) {
        let keys = keys.into();
        self.predicates.insert(keys.clone(), predicate);
        self.bind(keys, command);
    }

    /// The default predicate of the binding on `keys`.
    pub fn predicate_for(&self, keys: &KeySequence) -> &'static str {
        self.predicates
            .get(keys)
            .copied()
            .unwrap_or_else(|| self.context.default_predicate())
    }

    /// Bindings of this layer in the order their keys were first bound.
    pub fn ordered_bindings(&self) -> impl Iterator<Item = (&KeySequence, Command)> {
        self.order
            .iter()
            .filter_map(|keys| self.bindings.get(keys).map(|command| (keys, *command)))
    }

    /// The command bound to the single chord `chord`.
    pub fn get(&self, chord: &KeyChord) -> Option<Command> {
        self.get_sequence(&KeySequence::from(chord.clone()))
    }

    pub fn get_sequence(&self, keys: &KeySequence) -> Option<Command> {
        self.bindings.get(keys).copied()
    }

    pub fn context(&self) -> ContextId {
        self.context
    }

    #[allow(dead_code)]
    pub fn bindings(&self) -> &HashMap<KeySequence, Command> {
        &self.bindings
    }

    /// This layer with the leader placeholder in its keys replaced by
    /// `leader`, keeping the order and predicates of its bindings.
    fn with_leader(&self, leader: &KeyChord) -> KeymapLayer {
        let mut layer = KeymapLayer::new(self.context);

        for (keys, command) in self.ordered_bindings() {
            let resolved = keys.with_leader(leader);

            if let Some(predicate) = self.predicates.get(keys) {
                layer.bind_with_predicate(resolved, command, predicate);
            } else {
                layer.bind(resolved, command);
            }
        }

        layer
    }
}

/// Manages keybindings across all contexts with hierarchical resolution.
///
/// When resolving keys, the stack first checks the current context, then
/// falls back to parent contexts (ending at Global) if no match is found.
/// A context that keeps the global chords (see
/// [`ContextId::inherits_global_chords`]) falls back last to the global
/// bindings on a Ctrl or Cmd chord.
/// Key dispatch itself goes through GPUI's keymap (see
/// `dbflux_ui_base::keymap`); the stack answers lookups such as the keys a
/// command has, for shortcut labels.
#[derive(Clone)]
pub struct KeymapStack {
    layers: HashMap<ContextId, KeymapLayer>,
}

impl KeymapStack {
    pub fn new() -> Self {
        Self {
            layers: HashMap::new(),
        }
    }

    /// Adds a layer to the stack.
    pub fn add_layer(&mut self, layer: KeymapLayer) {
        self.layers.insert(layer.context, layer);
    }

    /// Returns the layer holding the bindings declared for `context` itself.
    pub fn layer(&self, context: ContextId) -> Option<&KeymapLayer> {
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
    pub fn resolve(&self, context: ContextId, chord: &KeyChord) -> Option<Command> {
        self.resolve_sequence(context, &KeySequence::from(chord.clone()))
    }

    /// Resolves a key sequence to a command, checking the given context
    /// first, then falling back to parent contexts.
    pub fn resolve_sequence(&self, context: ContextId, keys: &KeySequence) -> Option<Command> {
        self.lookup_chain(context)
            .into_iter()
            .filter(|(_, chords_only)| !chords_only || keys.is_global_chord())
            .find_map(|(layer, _)| layer.get_sequence(keys))
    }

    /// The layers a key pressed in `context` reaches, in lookup order: the
    /// context's own, its ancestors', and last the global layer when the
    /// context keeps only its chords (the flag is `true` for that entry).
    fn lookup_chain(&self, context: ContextId) -> Vec<(&KeymapLayer, bool)> {
        let mut chain = Vec::new();
        let mut current = Some(context);

        while let Some(ctx) = current {
            if let Some(layer) = self.layers.get(&ctx) {
                chain.push((layer, false));
            }
            current = ctx.parent();
        }

        if context.inherits_global_chords()
            && let Some(global) = self.layers.get(&ContextId::Global)
        {
            chain.push((global, true));
        }

        chain
    }

    /// Returns all keybindings for a given context, including inherited ones.
    pub fn bindings_for_context(
        &self,
        context: ContextId,
    ) -> Vec<(KeySequence, Command, ContextId)> {
        let mut result = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for (layer, chords_only) in self.lookup_chain(context) {
            for (keys, cmd) in layer.ordered_bindings() {
                if chords_only && !keys.is_global_chord() {
                    continue;
                }

                if seen.insert(keys.clone()) {
                    result.push((keys.clone(), cmd, layer.context()));
                }
            }
        }

        result
    }

    /// Returns the shortcut string for a command in the given context, if any.
    pub fn shortcut_for_command(&self, context: ContextId, command: Command) -> Option<String> {
        self.keys_for_command(context, command)
            .map(|keys| keys.to_string())
    }

    /// Returns the keys bound to a command in the given context, falling
    /// back to parent contexts the same way [`KeymapStack::resolve`] does.
    ///
    /// When one layer binds the command to several key sequences, the one
    /// bound first is returned.
    pub fn keys_for_command(&self, context: ContextId, command: Command) -> Option<&KeySequence> {
        self.lookup_chain(context)
            .into_iter()
            .find_map(|(layer, chords_only)| {
                layer
                    .ordered_bindings()
                    .find(|(keys, bound_command)| {
                        *bound_command == command && (!chords_only || keys.is_global_chord())
                    })
                    .map(|(keys, _)| keys)
            })
    }

    /// The first chord of the keys bound to `command`, see
    /// [`KeymapStack::keys_for_command`].
    pub fn chord_for_command(&self, context: ContextId, command: Command) -> Option<&KeyChord> {
        self.keys_for_command(context, command)
            .map(KeySequence::first)
    }
}

impl Default for KeymapStack {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keymap::Modifiers;

    #[test]
    fn test_resolve_in_context() {
        let mut stack = KeymapStack::new();

        let mut global = KeymapLayer::new(ContextId::Global);
        global.bind(
            KeyChord::new("p", Modifiers::ctrl_shift()),
            Command::ToggleCommandPalette,
        );

        let mut sidebar = KeymapLayer::new(ContextId::Sidebar);
        sidebar.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);

        stack.add_layer(global);
        stack.add_layer(sidebar);

        let chord_j = KeyChord::new("j", Modifiers::none());
        assert_eq!(
            stack.resolve(ContextId::Sidebar, &chord_j),
            Some(Command::SelectNext)
        );
        assert_eq!(stack.resolve(ContextId::Editor, &chord_j), None);
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

        let mut layer = KeymapLayer::new(ContextId::VimNormal);
        layer.bind(leader_a, Command::OpenPaneActions);
        layer.bind_with_predicate(leader_r, Command::RunQuery, "VimNormal");

        let mut stack = KeymapStack::new();
        stack.add_layer(layer);

        let resolved = stack.with_leader(&KeyChord::new(",", Modifiers::none()));
        let layer = resolved.layer(ContextId::VimNormal).expect("layer kept");
        let bindings: Vec<(String, Command)> = layer
            .ordered_bindings()
            .map(|(keys, command)| (keys.to_storage_string(), command))
            .collect();

        assert_eq!(
            bindings,
            vec![
                (", a".to_string(), Command::OpenPaneActions),
                (", r".to_string(), Command::RunQuery),
            ]
        );
        assert_eq!(
            layer.predicate_for(&KeySequence::parse(", r").expect("valid")),
            "VimNormal"
        );
    }

    #[test]
    fn test_fallback_to_global() {
        let mut stack = KeymapStack::new();

        let mut global = KeymapLayer::new(ContextId::Global);
        global.bind(
            KeyChord::new("p", Modifiers::ctrl_shift()),
            Command::ToggleCommandPalette,
        );

        stack.add_layer(global);

        let chord = KeyChord::new("p", Modifiers::ctrl_shift());

        assert_eq!(
            stack.resolve(ContextId::Sidebar, &chord),
            Some(Command::ToggleCommandPalette)
        );
        assert_eq!(
            stack.resolve(ContextId::Editor, &chord),
            Some(Command::ToggleCommandPalette)
        );
    }

    #[test]
    fn test_modal_no_fallback() {
        let mut stack = KeymapStack::new();

        let mut global = KeymapLayer::new(ContextId::Global);
        global.bind(
            KeyChord::new("p", Modifiers::ctrl_shift()),
            Command::ToggleCommandPalette,
        );

        let palette = KeymapLayer::new(ContextId::CommandPalette);

        stack.add_layer(global);
        stack.add_layer(palette);

        let chord = KeyChord::new("p", Modifiers::ctrl_shift());
        assert_eq!(stack.resolve(ContextId::CommandPalette, &chord), None);
    }

    #[test]
    fn text_entry_contexts_fall_back_to_global_chords_only() {
        let mut global = KeymapLayer::new(ContextId::Global);
        global.bind(
            KeyChord::new("p", Modifiers::ctrl_shift()),
            Command::ToggleCommandPalette,
        );
        global.bind(
            KeyChord::new("tab", Modifiers::none()),
            Command::CycleFocusForward,
        );
        global.bind(
            KeyChord::new("h", Modifiers::alt()),
            Command::ToggleHistoryDropdown,
        );

        let mut text_input = KeymapLayer::new(ContextId::TextInput);
        text_input.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

        let mut stack = KeymapStack::new();
        stack.add_layer(global);
        stack.add_layer(text_input);

        let palette = KeyChord::new("p", Modifiers::ctrl_shift());
        let tab = KeyChord::new("tab", Modifiers::none());
        let alt_h = KeyChord::new("h", Modifiers::alt());

        assert_eq!(
            stack.resolve(ContextId::TextInput, &palette),
            Some(Command::ToggleCommandPalette)
        );
        assert_eq!(stack.resolve(ContextId::TextInput, &tab), None);
        assert_eq!(stack.resolve(ContextId::TextInput, &alt_h), None);
        assert_eq!(stack.resolve(ContextId::ConfirmModal, &palette), None);

        assert_eq!(
            stack.keys_for_command(ContextId::TextInput, Command::ToggleCommandPalette),
            Some(&KeySequence::from(palette.clone()))
        );
        assert_eq!(
            stack.keys_for_command(ContextId::TextInput, Command::CycleFocusForward),
            None
        );

        let listed: Vec<Command> = stack
            .bindings_for_context(ContextId::TextInput)
            .into_iter()
            .map(|(_, command, _)| command)
            .collect();
        assert_eq!(listed, vec![Command::Cancel, Command::ToggleCommandPalette]);
    }

    #[test]
    fn sequences_resolve_only_as_a_whole() {
        let mut tree = KeymapLayer::new(ContextId::DocumentTree);
        let d_d = KeySequence::parse("d d").expect("valid sequence");
        tree.bind(d_d.clone(), Command::Delete);

        let mut stack = KeymapStack::new();
        stack.add_layer(tree);

        assert_eq!(
            stack.resolve_sequence(ContextId::DocumentTree, &d_d),
            Some(Command::Delete)
        );
        assert_eq!(
            stack.resolve(
                ContextId::DocumentTree,
                &KeyChord::new("d", Modifiers::none())
            ),
            None
        );
        assert_eq!(
            stack.keys_for_command(ContextId::DocumentTree, Command::Delete),
            Some(&d_d)
        );
    }

    #[test]
    fn chord_for_command_prefers_context_then_falls_back_to_parent() {
        let mut stack = KeymapStack::new();

        let mut global = KeymapLayer::new(ContextId::Global);
        global.bind(
            KeyChord::new("n", Modifiers::ctrl_shift()),
            Command::OpenConnectionManager,
        );

        let mut sidebar = KeymapLayer::new(ContextId::Sidebar);
        sidebar.bind(
            KeyChord::new("c", Modifiers::none()),
            Command::OpenConnectionManager,
        );

        stack.add_layer(global);
        stack.add_layer(sidebar);

        let global_chord = KeyChord::new("n", Modifiers::ctrl_shift());
        let sidebar_chord = KeyChord::new("c", Modifiers::none());

        assert_eq!(
            stack.chord_for_command(ContextId::Global, Command::OpenConnectionManager),
            Some(&global_chord)
        );
        assert_eq!(
            stack.chord_for_command(ContextId::Editor, Command::OpenConnectionManager),
            Some(&global_chord)
        );
        assert_eq!(
            stack.chord_for_command(ContextId::Sidebar, Command::OpenConnectionManager),
            Some(&sidebar_chord)
        );
        assert_eq!(
            stack.chord_for_command(ContextId::CommandPalette, Command::OpenConnectionManager),
            None
        );
        assert_eq!(
            stack.shortcut_for_command(ContextId::Global, Command::OpenConnectionManager),
            Some(global_chord.to_string())
        );
    }
}
