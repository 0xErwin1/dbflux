use std::collections::HashMap;

use super::{Command, ContextId, KeyChord};

/// A single layer of keybindings for a specific context.
pub struct KeymapLayer {
    context: ContextId,
    bindings: HashMap<KeyChord, Command>,
}

impl KeymapLayer {
    pub fn new(context: ContextId) -> Self {
        Self {
            context,
            bindings: HashMap::new(),
        }
    }

    pub fn bind(&mut self, chord: KeyChord, command: Command) {
        self.bindings.insert(chord, command);
    }

    pub fn get(&self, chord: &KeyChord) -> Option<Command> {
        self.bindings.get(chord).copied()
    }

    #[allow(dead_code)]
    pub fn context(&self) -> ContextId {
        self.context
    }

    #[allow(dead_code)]
    pub fn bindings(&self) -> &HashMap<KeyChord, Command> {
        &self.bindings
    }
}

/// Manages keybindings across all contexts with hierarchical resolution.
///
/// When resolving a key chord, the stack first checks the current context,
/// then falls back to parent contexts (ending at Global) if no match is found.
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

    /// Resolves a key chord to a command, checking the given context first,
    /// then falling back to parent contexts.
    pub fn resolve(&self, context: ContextId, chord: &KeyChord) -> Option<Command> {
        let mut current = Some(context);

        while let Some(ctx) = current {
            if let Some(layer) = self.layers.get(&ctx)
                && let Some(cmd) = layer.get(chord)
            {
                return Some(cmd);
            }
            current = ctx.parent();
        }

        None
    }

    /// Returns all keybindings for a given context, including inherited ones.
    #[allow(dead_code)]
    pub fn bindings_for_context(&self, context: ContextId) -> Vec<(KeyChord, Command, ContextId)> {
        let mut result = Vec::new();
        let mut seen_chords = std::collections::HashSet::new();
        let mut current = Some(context);

        while let Some(ctx) = current {
            if let Some(layer) = self.layers.get(&ctx) {
                for (chord, cmd) in layer.bindings() {
                    if seen_chords.insert(chord.clone()) {
                        result.push((chord.clone(), *cmd, ctx));
                    }
                }
            }
            current = ctx.parent();
        }

        result
    }

    /// Returns the shortcut string for a command in the given context, if any.
    #[allow(dead_code)]
    pub fn shortcut_for_command(&self, context: ContextId, command: Command) -> Option<String> {
        let mut current = Some(context);

        while let Some(ctx) = current {
            if let Some(layer) = self.layers.get(&ctx) {
                for (chord, cmd) in layer.bindings() {
                    if *cmd == command {
                        return Some(chord.to_string());
                    }
                }
            }
            current = ctx.parent();
        }

        None
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
}
