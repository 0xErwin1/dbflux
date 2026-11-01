//! The typed keymap over the product's vocabulary: a builder for a
//! context's bindings, the stack that resolves keys across the context
//! tree, and the borrowed typed view of a built layer.
//!
//! The mechanics live in `bolt_keys`, which stores commands as action-id
//! strings. This module is the vocabulary adapter: it stores and answers
//! `Command` values, converts ids at the edges, and delegates every lookup
//! to the engine with the product's context vocabulary.

use std::collections::HashMap;

use bolt_keys::{KeyChord, KeySequence, KeymapStack as EngineStack};

use super::vocabulary::{command_for, context_for, vocabulary};
use super::{Command, ContextId};

/// A single layer of keybindings for a specific context.
///
/// Besides the lookup table the layer keeps the order in which key sequences
/// were first bound, so lists built from it (the settings page) follow the
/// order the keymap declares them in.
///
/// The layer is a builder: [`KeymapStack::add_layer`] converts it to the
/// engine's layer, which holds the commands as action-id strings; a built
/// layer is read back as a [`KeymapLayerView`] of the stack.
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

    /// The engine layer for `self`: the same bindings, with the commands as
    /// their action ids.
    pub(super) fn into_engine(self) -> bolt_keys::KeymapLayer {
        let mut layer = bolt_keys::KeymapLayer::new(self.context.id());

        for keys in &self.order {
            let command = self
                .bindings
                .get(keys)
                .map(|command| command.action_id())
                .unwrap_or_default();

            if let Some(predicate) = self.predicates.get(keys) {
                layer.bind_with_predicate(keys.clone(), command, predicate);
            } else {
                layer.bind(keys.clone(), command);
            }
        }

        layer
    }
}

/// A borrowed, typed view of one layer of a [`KeymapStack`].
///
/// The stack stores the engine's layers, whose commands are action-id
/// strings; the view converts them back to `Command` values as it iterates.
pub struct KeymapLayerView<'a> {
    layer: &'a bolt_keys::KeymapLayer,
}

impl KeymapLayerView<'_> {
    /// Bindings of this layer in the order their keys were first bound.
    pub fn ordered_bindings(&self) -> impl Iterator<Item = (&KeySequence, Command)> + '_ {
        self.layer.ordered_bindings().filter_map(|(keys, command)| {
            command_for(command.as_ref()).map(|command| (keys, command))
        })
    }

    /// The command bound to the single chord `chord`.
    pub fn get(&self, chord: &KeyChord) -> Option<Command> {
        self.layer
            .get(chord)
            .and_then(|command| command_for(&command))
    }

    /// The command bound to the key sequence `keys`.
    pub fn get_sequence(&self, keys: &KeySequence) -> Option<Command> {
        self.layer
            .get_sequence(keys)
            .and_then(|command| command_for(&command))
    }

    /// The context the layer declares its bindings for.
    pub fn context(&self) -> ContextId {
        context_for(self.layer.context())
    }

    /// The default predicate of the binding on `keys`.
    pub fn predicate_for(&self, keys: &KeySequence) -> &'static str {
        self.layer.predicate_for(vocabulary(), keys)
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
#[derive(Clone, Default)]
pub struct KeymapStack(EngineStack);

impl KeymapStack {
    pub fn new() -> Self {
        Self(EngineStack::new())
    }

    /// Adds a layer to the stack.
    pub fn add_layer(&mut self, layer: KeymapLayer) {
        self.0.add_layer(layer.into_engine());
    }

    /// A typed view of the layer holding the bindings declared for
    /// `context` itself, or `None` when the stack has no layer for it.
    pub fn layer(&self, context: ContextId) -> Option<KeymapLayerView<'_>> {
        self.0
            .layer(context.id())
            .map(|layer| KeymapLayerView { layer })
    }

    /// This stack with every leader placeholder replaced by `leader`: the
    /// keys the user presses, for lookups and labels.
    pub fn with_leader(&self, leader: &KeyChord) -> KeymapStack {
        KeymapStack(self.0.with_leader(leader))
    }

    /// Resolves a single chord to a command, checking the given context
    /// first, then falling back to parent contexts.
    pub fn resolve(&self, context: ContextId, chord: &KeyChord) -> Option<Command> {
        self.0
            .resolve(vocabulary(), context.id(), chord)
            .and_then(|command| command_for(&command))
    }

    /// Resolves a key sequence to a command, checking the given context
    /// first, then falling back to parent contexts.
    pub fn resolve_sequence(&self, context: ContextId, keys: &KeySequence) -> Option<Command> {
        self.0
            .resolve_sequence(vocabulary(), context.id(), keys)
            .and_then(|command| command_for(&command))
    }

    /// Returns all keybindings for a given context, including inherited ones.
    pub fn bindings_for_context(
        &self,
        context: ContextId,
    ) -> Vec<(KeySequence, Command, ContextId)> {
        self.0
            .bindings_for_context(vocabulary(), context.id())
            .into_iter()
            .filter_map(|(keys, command, layer_context)| {
                Some((
                    keys,
                    command_for(command.as_ref())?,
                    context_for(layer_context),
                ))
            })
            .collect()
    }

    /// Returns the shortcut string for a command in the given context, if any.
    pub fn shortcut_for_command(&self, context: ContextId, command: Command) -> Option<String> {
        self.0
            .shortcut_for_command(vocabulary(), context.id(), &command.action_id())
    }

    /// Returns the keys bound to a command in the given context, falling
    /// back to parent contexts the same way [`KeymapStack::resolve`] does.
    ///
    /// When one layer binds the command to several key sequences, the one
    /// bound first is returned.
    pub fn keys_for_command(&self, context: ContextId, command: Command) -> Option<&KeySequence> {
        self.0
            .keys_for_command(vocabulary(), context.id(), &command.action_id())
    }

    /// The first chord of the keys bound to `command`, see
    /// [`KeymapStack::keys_for_command`].
    pub fn chord_for_command(&self, context: ContextId, command: Command) -> Option<&KeyChord> {
        self.0
            .chord_for_command(vocabulary(), context.id(), &command.action_id())
    }

    /// The engine stack: the same layers with commands as action ids.
    pub(super) fn engine(&self) -> &EngineStack {
        &self.0
    }

    /// Wraps an engine stack built outside this module.
    pub(super) fn from_engine_stack(engine: EngineStack) -> Self {
        Self(engine)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bolt_keys::Modifiers;

    #[test]
    fn a_typed_layer_round_trips_through_the_engine() {
        let mut layer = KeymapLayer::new(ContextId::Sidebar);
        layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
        layer.bind_with_predicate(
            KeyChord::new("r", Modifiers::ctrl()),
            Command::RunQuery,
            "Sidebar",
        );

        let mut stack = KeymapStack::new();
        stack.add_layer(layer.clone());

        let view = stack.layer(ContextId::Sidebar).expect("layer kept");
        let bindings: Vec<(String, Command)> = view
            .ordered_bindings()
            .map(|(keys, command)| (keys.to_storage_string(), command))
            .collect();

        assert_eq!(
            bindings,
            vec![
                ("j".to_string(), Command::SelectNext),
                ("ctrl+r".to_string(), Command::RunQuery),
            ]
        );
        assert_eq!(view.context(), ContextId::Sidebar);
        assert_eq!(
            view.predicate_for(&KeySequence::parse("ctrl+r").expect("valid")),
            "Sidebar"
        );
        assert_eq!(
            view.get(&KeyChord::new("j", Modifiers::none())),
            Some(Command::SelectNext)
        );

        let built = stack.layer(ContextId::Editor);
        assert!(built.is_none(), "no layer was added for the editor");

        let resolved = stack.resolve(ContextId::Sidebar, &KeyChord::new("j", Modifiers::none()));
        assert_eq!(resolved, Some(Command::SelectNext));

        assert_eq!(
            layer.get_sequence(&KeySequence::parse("j").expect("valid")),
            Some(Command::SelectNext)
        );
    }
}
