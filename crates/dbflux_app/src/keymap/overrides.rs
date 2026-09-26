//! User keybinding overrides applied on top of the default keymap.
//!
//! An override targets one binding of the default keymap, a [`BindingSlot`],
//! and either moves it to another chord or removes its shortcut. Bindings
//! without an override keep their default chord, so a new default added by a
//! later release reaches users who customized other bindings.

use std::collections::HashMap;

use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_storage::error::StorageError;
use dbflux_storage::repositories::keybinding_overrides::KeybindingOverrideDto;

use super::{Command, ContextId, KeyChord, KeymapLayer, KeymapStack};

/// One binding of the default keymap: the context whose layer declares it,
/// the command it runs and the chord it has by default.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BindingSlot {
    pub context: ContextId,
    pub command: Command,
    pub default_chord: KeyChord,
}

impl BindingSlot {
    pub fn new(context: ContextId, command: Command, default_chord: KeyChord) -> Self {
        Self {
            context,
            command,
            default_chord,
        }
    }

    /// The row key: context and default keys in the stored text forms. The
    /// context is the context id; the stored column also accepts predicate
    /// expressions, which the keymap does not use yet.
    fn storage_key(&self) -> (String, String, String) {
        (
            self.context.id().to_string(),
            self.command.id().to_string(),
            KeyChord::sequence_to_storage_string(std::slice::from_ref(&self.default_chord)),
        )
    }
}

/// Every binding of `defaults`, layer by layer in [`ContextId::all_variants`]
/// order and, inside a layer, in declaration order.
pub fn default_slots(defaults: &KeymapStack) -> Vec<BindingSlot> {
    ContextId::all_variants()
        .iter()
        .filter_map(|context| defaults.layer(*context))
        .flat_map(|layer| {
            layer
                .ordered_bindings()
                .map(|(chord, command)| BindingSlot::new(layer.context(), command, chord.clone()))
        })
        .collect()
}

/// Whether a key pressed in one of the two contexts can reach bindings of the
/// other: they are the same context, or one inherits from the other.
pub fn contexts_overlap(first: ContextId, second: ContextId) -> bool {
    first == second || is_ancestor(first, second) || is_ancestor(second, first)
}

fn is_ancestor(ancestor: ContextId, context: ContextId) -> bool {
    let mut current = context.parent();

    while let Some(parent) = current {
        if parent == ancestor {
            return true;
        }
        current = parent.parent();
    }

    false
}

/// The set of user overrides, keyed by the default binding each replaces.
///
/// A value of `None` means the user removed the binding's shortcut. An
/// override that would restore the default chord is not stored, so
/// [`KeymapOverrides::len`] counts only bindings that really differ.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeymapOverrides {
    entries: HashMap<BindingSlot, Option<KeyChord>>,
}

impl KeymapOverrides {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn is_overridden(&self, slot: &BindingSlot) -> bool {
        self.entries.contains_key(slot)
    }

    /// The chord the binding has once overrides apply, `None` when unbound.
    pub fn effective_chord(&self, slot: &BindingSlot) -> Option<KeyChord> {
        match self.entries.get(slot) {
            Some(chord) => chord.clone(),
            None => Some(slot.default_chord.clone()),
        }
    }

    /// Gives `slot` the chord `chord`, or removes its shortcut with `None`.
    pub fn set(&mut self, slot: BindingSlot, chord: Option<KeyChord>) {
        if chord.as_ref() == Some(&slot.default_chord) {
            self.entries.remove(&slot);
        } else {
            self.entries.insert(slot, chord);
        }
    }

    /// Moves `slot` to `chord` and removes the shortcut of every binding in
    /// `replaced`, the bindings that held the chord before. Each of them gets
    /// its chord back with [`KeymapOverrides::reset`].
    pub fn rebind(&mut self, slot: BindingSlot, chord: KeyChord, replaced: &[BindingSlot]) {
        for other in replaced {
            self.set(other.clone(), None);
        }
        self.set(slot, Some(chord));
    }

    /// Restores the default chord of `slot`.
    pub fn reset(&mut self, slot: &BindingSlot) {
        self.entries.remove(slot);
    }

    /// Restores the default keymap.
    pub fn reset_all(&mut self) {
        self.entries.clear();
    }

    /// Builds the effective keymap: `defaults` with every override applied.
    ///
    /// Moved bindings are bound after the untouched ones of their layer, so a
    /// stored override wins over a default that uses the same chord.
    pub fn apply(&self, defaults: &KeymapStack) -> KeymapStack {
        let mut stack = KeymapStack::new();

        for context in ContextId::all_variants() {
            let Some(default_layer) = defaults.layer(*context) else {
                continue;
            };

            let mut layer = KeymapLayer::new(*context);
            let mut moved = Vec::new();

            for (chord, command) in default_layer.ordered_bindings() {
                let slot = BindingSlot::new(*context, command, chord.clone());

                match self.entries.get(&slot) {
                    None => layer.bind(chord.clone(), command),
                    Some(Some(new_chord)) => moved.push((new_chord.clone(), command)),
                    Some(None) => {}
                }
            }

            for (chord, command) in moved {
                layer.bind(chord, command);
            }

            stack.add_layer(layer);
        }

        stack
    }

    /// Bindings other than `slot` that `chord` would collide with: bindings
    /// whose effective chord is `chord` in the same context as `slot` or in a
    /// context that inherits from it or that it inherits from.
    pub fn conflicts_for(
        &self,
        defaults: &KeymapStack,
        slot: &BindingSlot,
        chord: &KeyChord,
    ) -> Vec<BindingSlot> {
        default_slots(defaults)
            .into_iter()
            .filter(|other| other != slot && contexts_overlap(other.context, slot.context))
            .filter(|other| self.effective_chord(other).as_ref() == Some(chord))
            .collect()
    }

    /// Conflicts the effective keymap already has, one entry per binding
    /// that collides with at least one other.
    ///
    /// Only collisions that involve an overridden binding count: the default
    /// keymap deliberately lets some contexts shadow a parent chord.
    pub fn existing_conflicts(
        &self,
        defaults: &KeymapStack,
    ) -> Vec<(BindingSlot, Vec<BindingSlot>)> {
        let slots = default_slots(defaults);

        slots
            .iter()
            .filter_map(|slot| {
                let chord = self.effective_chord(slot)?;

                let others: Vec<BindingSlot> = slots
                    .iter()
                    .filter(|other| {
                        *other != slot
                            && contexts_overlap(other.context, slot.context)
                            && (self.is_overridden(slot) || self.is_overridden(other))
                            && self.effective_chord(other).as_ref() == Some(&chord)
                    })
                    .cloned()
                    .collect();

                (!others.is_empty()).then(|| (slot.clone(), others))
            })
            .collect()
    }

    /// Rows to persist, sorted so the stored order is stable.
    pub fn to_dtos(&self) -> Vec<KeybindingOverrideDto> {
        let mut dtos: Vec<KeybindingOverrideDto> = self
            .entries
            .iter()
            .map(|(slot, chord)| {
                let (context, command_id, default_keys) = slot.storage_key();

                KeybindingOverrideDto {
                    context,
                    command_id,
                    default_keys,
                    keys: chord.as_ref().map(|chord| {
                        KeyChord::sequence_to_storage_string(std::slice::from_ref(chord))
                    }),
                }
            })
            .collect();

        dtos.sort_by(|first, second| {
            (&first.context, &first.command_id, &first.default_keys).cmp(&(
                &second.context,
                &second.command_id,
                &second.default_keys,
            ))
        });

        dtos
    }

    /// Rebuilds the overrides from stored rows.
    ///
    /// A row whose default binding no longer exists (a later release changed
    /// or removed it), whose keys do not parse, or whose keys are a sequence
    /// of more than one chord (the keymap binds single chords only) is
    /// skipped with a warning, and that binding keeps its current default.
    pub fn from_dtos(defaults: &KeymapStack, dtos: &[KeybindingOverrideDto]) -> Self {
        let slots_by_key: HashMap<(String, String, String), BindingSlot> = default_slots(defaults)
            .into_iter()
            .map(|slot| (slot.storage_key(), slot))
            .collect();

        let mut overrides = Self::new();

        for dto in dtos {
            let key = (
                dto.context.clone(),
                dto.command_id.clone(),
                dto.default_keys.clone(),
            );

            let Some(slot) = slots_by_key.get(&key) else {
                log::warn!(
                    "Ignoring keybinding override for {}/{} ({}): the default binding no longer exists",
                    dto.context,
                    dto.command_id,
                    dto.default_keys
                );
                continue;
            };

            let chord = match dto
                .keys
                .as_deref()
                .map(KeyChord::sequence_from_storage_string)
            {
                None => None,
                Some(Ok(sequence)) => match <[KeyChord; 1]>::try_from(sequence) {
                    Ok([chord]) => Some(chord),
                    Err(_) => {
                        log::warn!(
                            "Ignoring keybinding override for {}/{}: key sequences are not supported yet",
                            dto.context,
                            dto.command_id
                        );
                        continue;
                    }
                },
                Some(Err(error)) => {
                    log::warn!(
                        "Ignoring keybinding override for {}/{}: invalid keys: {error}",
                        dto.context,
                        dto.command_id
                    );
                    continue;
                }
            };

            overrides.set(slot.clone(), chord);
        }

        overrides
    }
}

/// Reads the stored overrides and matches them against `defaults`. A read
/// failure is logged and yields no overrides, so the default keymap applies.
pub fn load_keymap_overrides(runtime: &StorageRuntime, defaults: &KeymapStack) -> KeymapOverrides {
    match runtime.keybinding_overrides().list() {
        Ok(rows) => KeymapOverrides::from_dtos(defaults, &rows),
        Err(error) => {
            log::warn!("Failed to load keybinding overrides, using the default keymap: {error}");
            KeymapOverrides::new()
        }
    }
}

/// Replaces the stored overrides with `overrides`.
pub fn save_keymap_overrides(
    runtime: &StorageRuntime,
    overrides: &KeymapOverrides,
) -> Result<(), StorageError> {
    runtime
        .keybinding_overrides()
        .replace_all(&overrides.to_dtos())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keymap::Modifiers;

    fn chord(key: &str, modifiers: Modifiers) -> KeyChord {
        KeyChord::new(key, modifiers)
    }

    fn defaults() -> KeymapStack {
        let mut global = KeymapLayer::new(ContextId::Global);
        global.bind(
            chord("p", Modifiers::ctrl_shift()),
            Command::ToggleCommandPalette,
        );
        global.bind(chord("n", Modifiers::ctrl()), Command::NewQueryTab);

        let mut editor = KeymapLayer::new(ContextId::Editor);
        editor.bind(chord("s", Modifiers::ctrl()), Command::SaveQuery);
        editor.bind(chord("p", Modifiers::ctrl()), Command::OpenSavedQueries);

        let mut sidebar = KeymapLayer::new(ContextId::Sidebar);
        sidebar.bind(chord("j", Modifiers::none()), Command::SelectNext);
        sidebar.bind(chord("down", Modifiers::none()), Command::SelectNext);

        let mut stack = KeymapStack::new();
        stack.add_layer(global);
        stack.add_layer(editor);
        stack.add_layer(sidebar);
        stack
    }

    fn slot(context: ContextId, command: Command, default_chord: KeyChord) -> BindingSlot {
        BindingSlot::new(context, command, default_chord)
    }

    fn new_query_tab() -> BindingSlot {
        slot(
            ContextId::Global,
            Command::NewQueryTab,
            chord("n", Modifiers::ctrl()),
        )
    }

    fn save_query() -> BindingSlot {
        slot(
            ContextId::Editor,
            Command::SaveQuery,
            chord("s", Modifiers::ctrl()),
        )
    }

    #[test]
    fn default_slots_follow_context_and_declaration_order() {
        let slots = default_slots(&defaults());

        assert_eq!(slots.len(), 6);
        assert_eq!(slots[0].command, Command::ToggleCommandPalette);
        assert_eq!(slots[1].command, Command::NewQueryTab);
        assert_eq!(slots[2].context, ContextId::Sidebar);
        assert_eq!(slots[2].default_chord, chord("j", Modifiers::none()));
        assert_eq!(slots[4].context, ContextId::Editor);
    }

    #[test]
    fn apply_moves_rebound_bindings_and_drops_unbound_ones() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        overrides.set(new_query_tab(), Some(chord("t", Modifiers::ctrl())));
        overrides.set(save_query(), None);

        let effective = overrides.apply(&defaults);

        assert_eq!(
            effective.resolve(ContextId::Global, &chord("t", Modifiers::ctrl())),
            Some(Command::NewQueryTab)
        );
        assert_eq!(
            effective.resolve(ContextId::Global, &chord("n", Modifiers::ctrl())),
            None
        );
        assert_eq!(
            effective.resolve(ContextId::Editor, &chord("s", Modifiers::ctrl())),
            None
        );
        assert_eq!(
            effective.resolve(ContextId::Editor, &chord("t", Modifiers::ctrl())),
            Some(Command::NewQueryTab),
            "the editor still inherits the moved global binding"
        );
        assert_eq!(
            effective.resolve(ContextId::Sidebar, &chord("down", Modifiers::none())),
            Some(Command::SelectNext),
            "bindings without an override keep their default"
        );
    }

    #[test]
    fn setting_the_default_chord_is_not_an_override() {
        let mut overrides = KeymapOverrides::new();
        overrides.set(new_query_tab(), Some(chord("t", Modifiers::ctrl())));
        assert_eq!(overrides.len(), 1);

        overrides.set(new_query_tab(), Some(chord("n", Modifiers::ctrl())));
        assert!(overrides.is_empty());
    }

    #[test]
    fn conflicts_cover_the_same_and_inherited_contexts_only() {
        let defaults = defaults();
        let overrides = KeymapOverrides::new();

        let same_context = overrides.conflicts_for(
            &defaults,
            &new_query_tab(),
            &chord("p", Modifiers::ctrl_shift()),
        );
        assert_eq!(same_context.len(), 1);
        assert_eq!(same_context[0].command, Command::ToggleCommandPalette);

        let child_context =
            overrides.conflicts_for(&defaults, &new_query_tab(), &chord("s", Modifiers::ctrl()));
        assert_eq!(child_context, vec![save_query()]);

        let sibling_context =
            overrides.conflicts_for(&defaults, &save_query(), &chord("j", Modifiers::none()));
        assert!(
            sibling_context.is_empty(),
            "the sidebar and the editor never see each other's keys"
        );

        let own_chord =
            overrides.conflicts_for(&defaults, &save_query(), &chord("s", Modifiers::ctrl()));
        assert!(own_chord.is_empty());
    }

    #[test]
    fn conflicts_use_effective_chords() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        overrides.set(save_query(), Some(chord("r", Modifiers::ctrl())));

        assert!(
            overrides
                .conflicts_for(&defaults, &new_query_tab(), &chord("s", Modifiers::ctrl()))
                .is_empty()
        );
        assert_eq!(
            overrides.conflicts_for(&defaults, &new_query_tab(), &chord("r", Modifiers::ctrl())),
            vec![save_query()]
        );
    }

    #[test]
    fn rebind_replaces_the_other_binding_and_its_reset_restores_it() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        let taken = chord("s", Modifiers::ctrl());

        let conflicts = overrides.conflicts_for(&defaults, &new_query_tab(), &taken);
        overrides.rebind(new_query_tab(), taken.clone(), &conflicts);

        assert_eq!(overrides.effective_chord(&save_query()), None);
        assert_eq!(
            overrides.effective_chord(&new_query_tab()),
            Some(taken.clone())
        );
        assert!(overrides.existing_conflicts(&defaults).is_empty());

        overrides.reset(&save_query());

        let conflicts = overrides.existing_conflicts(&defaults);
        assert_eq!(conflicts.len(), 2, "both sides of the collision are listed");

        overrides.reset_all();
        assert!(overrides.is_empty());
        assert!(overrides.existing_conflicts(&defaults).is_empty());
    }

    #[test]
    fn overrides_round_trip_through_storage_rows() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        overrides.set(new_query_tab(), Some(chord("+", Modifiers::ctrl())));
        overrides.set(save_query(), None);

        let dtos = overrides.to_dtos();
        assert_eq!(dtos.len(), 2);
        assert_eq!(dtos[0].context, "editor");
        assert_eq!(dtos[0].default_keys, "ctrl+s");
        assert_eq!(dtos[0].keys, None);
        assert_eq!(dtos[1].keys.as_deref(), Some("ctrl++"));

        assert_eq!(KeymapOverrides::from_dtos(&defaults, &dtos), overrides);
    }

    #[test]
    fn overrides_round_trip_through_storage() {
        let runtime = StorageRuntime::in_memory().expect("in-memory storage");
        let defaults = defaults();

        assert!(load_keymap_overrides(&runtime, &defaults).is_empty());

        let mut overrides = KeymapOverrides::new();
        overrides.set(new_query_tab(), Some(chord("t", Modifiers::ctrl_shift())));
        overrides.set(save_query(), None);
        save_keymap_overrides(&runtime, &overrides).expect("should save");

        assert_eq!(load_keymap_overrides(&runtime, &defaults), overrides);

        overrides.reset(&save_query());
        save_keymap_overrides(&runtime, &overrides).expect("should save again");

        let reloaded = load_keymap_overrides(&runtime, &defaults);
        assert_eq!(reloaded.len(), 1);
        assert!(!reloaded.is_overridden(&save_query()));
    }

    #[test]
    fn stale_or_invalid_rows_are_skipped() {
        let defaults = defaults();
        let row = |default_keys: &str, keys: &str| KeybindingOverrideDto {
            context: "global".to_string(),
            command_id: "new_query_tab".to_string(),
            default_keys: default_keys.to_string(),
            keys: Some(keys.to_string()),
        };
        let dtos = vec![
            row("ctrl+q", "ctrl+t"),
            row("ctrl+n", ""),
            row("ctrl+n", "ctrl+k ctrl+t"),
        ];

        assert!(KeymapOverrides::from_dtos(&defaults, &dtos).is_empty());
    }
}
