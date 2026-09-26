//! User keybinding overrides applied on top of the default keymap.
//!
//! An override targets one binding of the default keymap, a [`BindingSlot`],
//! and moves it to other keys, removes its shortcut, or moves it to another
//! context predicate. Bindings without an override keep their defaults, so a
//! new default added by a later release reaches users who customized other
//! bindings.

use std::borrow::Cow;
use std::collections::HashMap;

use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_storage::error::StorageError;
use dbflux_storage::repositories::keybinding_overrides::KeybindingOverrideDto;

use super::{Command, ContextId, KeySequence, KeymapLayer, KeymapStack};

/// One binding of the default keymap: the context whose layer declares it,
/// the command it runs, the keys it has by default and the context predicate
/// it applies in by default.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BindingSlot {
    pub context: ContextId,
    pub command: Command,
    pub default_keys: KeySequence,
    pub default_predicate: &'static str,
}

impl BindingSlot {
    /// A binding under its context's default predicate.
    pub fn new(context: ContextId, command: Command, default_keys: impl Into<KeySequence>) -> Self {
        Self {
            context,
            command,
            default_keys: default_keys.into(),
            default_predicate: context.default_predicate(),
        }
    }

    /// A binding under a narrower predicate than its context's own.
    pub fn with_predicate(mut self, default_predicate: &'static str) -> Self {
        self.default_predicate = default_predicate;
        self
    }

    /// The row key: context id, command id and default keys in their stored
    /// text forms.
    fn storage_key(&self) -> (String, String, String) {
        (
            self.context.id().to_string(),
            self.command.id().to_string(),
            self.default_keys.to_storage_string(),
        )
    }
}

/// What the user changed about one default binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingOverride {
    /// The keys of the binding; `None` removes its shortcut.
    pub keys: Option<KeySequence>,
    /// The context predicate of the binding; `None` keeps the default one.
    pub predicate: Option<String>,
}

/// Decides whether two context predicates can hold for the same key press,
/// so that bindings with the same keys under them collide.
///
/// Evaluating predicates needs GPUI's predicate language, which this crate
/// does not depend on; `dbflux_ui_base::keymap` supplies the real
/// implementation. [`ContextTreeOverlap`] covers default predicates only.
pub trait PredicateOverlap {
    fn overlaps(&self, first: &str, second: &str) -> bool;
}

/// Overlap from the context tree: two contexts' default predicates overlap
/// when the contexts are the same or one inherits from the other. Any other
/// predicate overlaps only with an identical one.
pub struct ContextTreeOverlap;

impl PredicateOverlap for ContextTreeOverlap {
    fn overlaps(&self, first: &str, second: &str) -> bool {
        match (
            context_for_default_predicate(first),
            context_for_default_predicate(second),
        ) {
            (Some(first), Some(second)) => contexts_overlap(first, second),
            _ => first == second,
        }
    }
}

fn context_for_default_predicate(predicate: &str) -> Option<ContextId> {
    ContextId::all_variants()
        .iter()
        .copied()
        .find(|context| context.default_predicate() == predicate)
}

/// Every binding of `defaults`, layer by layer in [`ContextId::all_variants`]
/// order and, inside a layer, in declaration order.
pub fn default_slots(defaults: &KeymapStack) -> Vec<BindingSlot> {
    ContextId::all_variants()
        .iter()
        .filter_map(|context| defaults.layer(*context))
        .flat_map(|layer| {
            layer.ordered_bindings().map(|(keys, command)| {
                BindingSlot::new(layer.context(), command, keys.clone())
                    .with_predicate(layer.predicate_for(keys))
            })
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

/// One binding of the keymap in force: a default binding with its overrides
/// applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveBinding {
    pub slot: BindingSlot,
    pub keys: KeySequence,
    /// Context predicate in GPUI's key context language.
    pub predicate: String,
    /// Whether the user changed the binding. User bindings are registered
    /// after the defaults so that they win over a default bound to the same
    /// keys at the same depth.
    pub is_user: bool,
}

/// The set of user overrides, keyed by the default binding each replaces.
///
/// An override that would restore the default keys and predicate is not
/// stored, so [`KeymapOverrides::len`] counts only bindings that really
/// differ.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeymapOverrides {
    entries: HashMap<BindingSlot, BindingOverride>,
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

    /// The keys the binding has once overrides apply, `None` when unbound.
    pub fn effective_keys(&self, slot: &BindingSlot) -> Option<KeySequence> {
        match self.entries.get(slot) {
            Some(binding_override) => binding_override.keys.clone(),
            None => Some(slot.default_keys.clone()),
        }
    }

    /// The context predicate the binding has once overrides apply.
    pub fn effective_predicate<'a>(&'a self, slot: &BindingSlot) -> Cow<'a, str> {
        match self
            .entries
            .get(slot)
            .and_then(|binding_override| binding_override.predicate.as_deref())
        {
            Some(predicate) => Cow::Borrowed(predicate),
            None => Cow::Borrowed(slot.default_predicate),
        }
    }

    /// The predicate the user gave the binding, `None` when it keeps the
    /// default one.
    pub fn custom_predicate(&self, slot: &BindingSlot) -> Option<&str> {
        self.entries
            .get(slot)
            .and_then(|binding_override| binding_override.predicate.as_deref())
    }

    fn store(&mut self, slot: BindingSlot, binding_override: BindingOverride) {
        let is_default = binding_override.keys.as_ref() == Some(&slot.default_keys)
            && binding_override.predicate.is_none();

        if is_default {
            self.entries.remove(&slot);
        } else {
            self.entries.insert(slot, binding_override);
        }
    }

    /// Gives `slot` the keys `keys`, or removes its shortcut with `None`,
    /// keeping its predicate.
    pub fn set(&mut self, slot: BindingSlot, keys: Option<KeySequence>) {
        let predicate = self.custom_predicate(&slot).map(str::to_string);
        self.store(slot, BindingOverride { keys, predicate });
    }

    /// Moves `slot` to the context predicate `predicate`, keeping its keys.
    /// The default predicate, or `None`, restores the default context.
    pub fn set_predicate(&mut self, slot: BindingSlot, predicate: Option<String>) {
        let keys = self.effective_keys(&slot);
        let predicate = predicate.filter(|predicate| predicate != slot.default_predicate);
        self.store(slot, BindingOverride { keys, predicate });
    }

    /// Moves `slot` to `keys` and removes the shortcut of every binding in
    /// `replaced`, the bindings that held the keys before. Each of them gets
    /// its keys back with [`KeymapOverrides::reset`].
    pub fn rebind(&mut self, slot: BindingSlot, keys: KeySequence, replaced: &[BindingSlot]) {
        for other in replaced {
            self.set(other.clone(), None);
        }
        self.set(slot, Some(keys));
    }

    /// Restores the default keys and predicate of `slot`.
    pub fn reset(&mut self, slot: &BindingSlot) {
        self.entries.remove(slot);
    }

    /// Restores the default keymap.
    pub fn reset_all(&mut self) {
        self.entries.clear();
    }

    /// Builds the effective keymap: `defaults` with every override applied.
    ///
    /// A binding stays in the layer of the context that declares it, even
    /// when the user moved it to another predicate, so lookups such as the
    /// keys shown next to a command keep finding it. Moved bindings are
    /// bound after the untouched ones of their layer, so a stored override
    /// wins over a default that uses the same keys.
    pub fn apply(&self, defaults: &KeymapStack) -> KeymapStack {
        let mut stack = KeymapStack::new();

        for context in ContextId::all_variants() {
            let Some(default_layer) = defaults.layer(*context) else {
                continue;
            };

            let mut layer = KeymapLayer::new(*context);
            let mut moved = Vec::new();

            for (keys, command) in default_layer.ordered_bindings() {
                let slot = BindingSlot::new(*context, command, keys.clone())
                    .with_predicate(default_layer.predicate_for(keys));

                match self.entries.get(&slot) {
                    None => layer.bind(keys.clone(), command),
                    Some(binding_override) => {
                        if let Some(new_keys) = &binding_override.keys {
                            moved.push((new_keys.clone(), command));
                        }
                    }
                }
            }

            for (keys, command) in moved {
                layer.bind(keys, command);
            }

            stack.add_layer(layer);
        }

        stack
    }

    /// Every bound binding of the keymap in force, in registration order:
    /// the defaults the user did not change, then the user's bindings.
    ///
    /// Inside each group, the contexts a window root sets come first, in
    /// [`ContextId::all_variants`] order so that `Global` precedes the
    /// contexts that inherit it, and the contexts an element sets on itself
    /// come last. GPUI lets a later binding of a longer key sequence keep
    /// waiting for its next key over an earlier single-key binding, so the
    /// element bindings (`d d` in a table) must follow the root ones (`d`).
    pub fn effective_bindings(&self, defaults: &KeymapStack) -> Vec<EffectiveBinding> {
        let mut slots = default_slots(defaults);
        slots.sort_by_key(|slot| slot.context.is_element_context());

        let (user, untouched): (Vec<BindingSlot>, Vec<BindingSlot>) =
            slots.into_iter().partition(|slot| self.is_overridden(slot));

        untouched
            .into_iter()
            .chain(user)
            .filter_map(|slot| {
                let keys = self.effective_keys(&slot)?;
                let predicate = self.effective_predicate(&slot).into_owned();
                let is_user = self.is_overridden(&slot);

                Some(EffectiveBinding {
                    slot,
                    keys,
                    predicate,
                    is_user,
                })
            })
            .collect()
    }

    /// Bindings other than `slot` that `keys` under `predicate` would collide
    /// with: bindings of another command whose effective keys are `keys` and
    /// whose predicate can hold at the same time.
    pub fn conflicts_for(
        &self,
        defaults: &KeymapStack,
        slot: &BindingSlot,
        keys: &KeySequence,
        predicate: &str,
        overlap: &dyn PredicateOverlap,
    ) -> Vec<BindingSlot> {
        default_slots(defaults)
            .into_iter()
            .filter(|other| other != slot && other.command != slot.command)
            .filter(|other| self.effective_keys(other).as_ref() == Some(keys))
            .filter(|other| overlap.overlaps(predicate, &self.effective_predicate(other)))
            .collect()
    }

    /// Bindings whose keys start with `keys` or that `keys` start with, in a
    /// predicate that can hold at the same time. Pressing the shorter one
    /// makes the keyboard wait for the timeout before running it.
    pub fn prefix_conflicts_for(
        &self,
        defaults: &KeymapStack,
        slot: &BindingSlot,
        keys: &KeySequence,
        predicate: &str,
        overlap: &dyn PredicateOverlap,
    ) -> Vec<BindingSlot> {
        default_slots(defaults)
            .into_iter()
            .filter(|other| other != slot)
            .filter(|other| {
                self.effective_keys(other).is_some_and(|other_keys| {
                    keys.is_prefix_of(&other_keys) || other_keys.is_prefix_of(keys)
                })
            })
            .filter(|other| overlap.overlaps(predicate, &self.effective_predicate(other)))
            .collect()
    }

    /// Conflicts the effective keymap already has, one entry per binding
    /// that collides with at least one other.
    ///
    /// Only collisions that involve an overridden binding count: the default
    /// keymap deliberately lets some contexts shadow a parent binding.
    pub fn existing_conflicts(
        &self,
        defaults: &KeymapStack,
        overlap: &dyn PredicateOverlap,
    ) -> Vec<(BindingSlot, Vec<BindingSlot>)> {
        let slots = default_slots(defaults);

        slots
            .iter()
            .filter_map(|slot| {
                let keys = self.effective_keys(slot)?;
                let predicate = self.effective_predicate(slot);

                let others: Vec<BindingSlot> = slots
                    .iter()
                    .filter(|other| {
                        *other != slot
                            && other.command != slot.command
                            && (self.is_overridden(slot) || self.is_overridden(other))
                            && self.effective_keys(other).as_ref() == Some(&keys)
                            && overlap.overlaps(&predicate, &self.effective_predicate(other))
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
            .map(|(slot, binding_override)| {
                let (context, command_id, default_keys) = slot.storage_key();

                KeybindingOverrideDto {
                    context,
                    command_id,
                    default_keys,
                    keys: binding_override
                        .keys
                        .as_ref()
                        .map(KeySequence::to_storage_string),
                    predicate: binding_override.predicate.clone(),
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
    /// or removed it), or whose keys do not parse, is skipped with a warning,
    /// and that binding keeps its current default. Predicates are stored
    /// only after they parse, so they are taken as they are.
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

            let keys = match dto.keys.as_deref().map(KeySequence::from_storage_string) {
                None => None,
                Some(Ok(keys)) => Some(keys),
                Some(Err(error)) => {
                    log::warn!(
                        "Ignoring keybinding override for {}/{}: invalid keys: {error}",
                        dto.context,
                        dto.command_id
                    );
                    continue;
                }
            };

            let predicate = dto
                .predicate
                .clone()
                .filter(|predicate| !predicate.trim().is_empty());

            overrides.store(slot.clone(), BindingOverride { keys, predicate });
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
    use crate::keymap::{KeyChord, Modifiers};

    fn chord(key: &str, modifiers: Modifiers) -> KeyChord {
        KeyChord::new(key, modifiers)
    }

    fn keys(chord: KeyChord) -> KeySequence {
        KeySequence::from(chord)
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

        let mut tree = KeymapLayer::new(ContextId::DocumentTree);
        tree.bind(
            KeySequence::parse("d d").expect("valid sequence"),
            Command::Delete,
        );

        let mut stack = KeymapStack::new();
        stack.add_layer(global);
        stack.add_layer(editor);
        stack.add_layer(sidebar);
        stack.add_layer(tree);
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

    fn select_next_j() -> BindingSlot {
        slot(
            ContextId::Sidebar,
            Command::SelectNext,
            chord("j", Modifiers::none()),
        )
    }

    #[test]
    fn default_slots_follow_context_and_declaration_order() {
        let slots = default_slots(&defaults());

        assert_eq!(slots.len(), 7);
        assert_eq!(slots[0].command, Command::ToggleCommandPalette);
        assert_eq!(slots[1].command, Command::NewQueryTab);
        assert_eq!(slots[2].context, ContextId::Sidebar);
        assert_eq!(slots[2].default_keys, keys(chord("j", Modifiers::none())));
        assert_eq!(slots[4].context, ContextId::Editor);
        assert_eq!(slots[6].context, ContextId::DocumentTree);
    }

    #[test]
    fn apply_moves_rebound_bindings_and_drops_unbound_ones() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        overrides.set(new_query_tab(), Some(keys(chord("t", Modifiers::ctrl()))));
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
    fn sequences_can_replace_single_chords() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        let sequence = KeySequence::parse("ctrl+k ctrl+s").expect("valid sequence");
        overrides.set(save_query(), Some(sequence.clone()));

        let effective = overrides.apply(&defaults);

        assert_eq!(
            effective.resolve_sequence(ContextId::Editor, &sequence),
            Some(Command::SaveQuery)
        );
        assert_eq!(
            effective.keys_for_command(ContextId::Editor, Command::SaveQuery),
            Some(&sequence)
        );
    }

    #[test]
    fn setting_the_default_keys_is_not_an_override() {
        let mut overrides = KeymapOverrides::new();
        overrides.set(new_query_tab(), Some(keys(chord("t", Modifiers::ctrl()))));
        assert_eq!(overrides.len(), 1);

        overrides.set(new_query_tab(), Some(keys(chord("n", Modifiers::ctrl()))));
        assert!(overrides.is_empty());
    }

    #[test]
    fn predicates_override_and_restore_the_default_context() {
        let mut overrides = KeymapOverrides::new();
        let slot = save_query();

        assert_eq!(
            overrides.effective_predicate(&slot),
            ContextId::Editor.default_predicate()
        );

        overrides.set_predicate(
            slot.clone(),
            Some("Editor && vim_mode == normal".to_string()),
        );
        assert_eq!(
            overrides.effective_predicate(&slot),
            "Editor && vim_mode == normal"
        );
        assert_eq!(
            overrides.effective_keys(&slot),
            Some(slot.default_keys.clone()),
            "moving the predicate keeps the keys"
        );

        overrides.set(slot.clone(), Some(keys(chord("r", Modifiers::ctrl()))));
        assert_eq!(
            overrides.effective_predicate(&slot),
            "Editor && vim_mode == normal",
            "changing the keys keeps the predicate"
        );

        overrides.set(slot.clone(), Some(slot.default_keys.clone()));
        overrides.set_predicate(
            slot.clone(),
            Some(ContextId::Editor.default_predicate().to_string()),
        );
        assert!(overrides.is_empty(), "the defaults are not an override");
    }

    #[test]
    fn effective_bindings_put_user_and_element_bindings_last() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        overrides.set(new_query_tab(), Some(keys(chord("t", Modifiers::ctrl()))));
        overrides.set(save_query(), None);

        let bindings = overrides.effective_bindings(&defaults);
        let commands: Vec<(Command, bool)> = bindings
            .iter()
            .map(|binding| (binding.slot.command, binding.is_user))
            .collect();

        assert_eq!(
            commands,
            vec![
                (Command::ToggleCommandPalette, false),
                (Command::SelectNext, false),
                (Command::SelectNext, false),
                (Command::OpenSavedQueries, false),
                (Command::Delete, false),
                (Command::NewQueryTab, true),
            ],
            "unbound SaveQuery is dropped, the tree comes after the root contexts"
        );
        assert_eq!(bindings[4].predicate, "DocumentTree");
        assert_eq!(bindings[0].predicate, ContextId::Global.default_predicate());
    }

    #[test]
    fn conflicts_cover_the_same_and_inherited_contexts_only() {
        let defaults = defaults();
        let overrides = KeymapOverrides::new();

        let same_context = overrides.conflicts_for(
            &defaults,
            &new_query_tab(),
            &keys(chord("p", Modifiers::ctrl_shift())),
            ContextId::Global.default_predicate(),
            &ContextTreeOverlap,
        );
        assert_eq!(same_context.len(), 1);
        assert_eq!(same_context[0].command, Command::ToggleCommandPalette);

        let child_context = overrides.conflicts_for(
            &defaults,
            &new_query_tab(),
            &keys(chord("s", Modifiers::ctrl())),
            ContextId::Global.default_predicate(),
            &ContextTreeOverlap,
        );
        assert_eq!(child_context, vec![save_query()]);

        let sibling_context = overrides.conflicts_for(
            &defaults,
            &save_query(),
            &keys(chord("j", Modifiers::none())),
            ContextId::Editor.default_predicate(),
            &ContextTreeOverlap,
        );
        assert!(
            sibling_context.is_empty(),
            "the sidebar and the editor never see each other's keys"
        );

        let own_keys = overrides.conflicts_for(
            &defaults,
            &save_query(),
            &keys(chord("s", Modifiers::ctrl())),
            ContextId::Editor.default_predicate(),
            &ContextTreeOverlap,
        );
        assert!(own_keys.is_empty());
    }

    #[test]
    fn a_moved_predicate_changes_what_the_binding_conflicts_with() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        overrides.set_predicate(
            select_next_j(),
            Some(ContextId::Editor.default_predicate().to_string()),
        );

        let conflicts = overrides.conflicts_for(
            &defaults,
            &save_query(),
            &keys(chord("j", Modifiers::none())),
            ContextId::Editor.default_predicate(),
            &ContextTreeOverlap,
        );
        assert_eq!(conflicts, vec![select_next_j()]);
    }

    #[test]
    fn prefix_conflicts_find_sequences_sharing_a_start() {
        let defaults = defaults();
        let overrides = KeymapOverrides::new();
        let tree_slot = slot(
            ContextId::DocumentTree,
            Command::PreviewDocument,
            chord("e", Modifiers::none()),
        );

        let prefixes = overrides.prefix_conflicts_for(
            &defaults,
            &tree_slot,
            &keys(chord("d", Modifiers::none())),
            "DocumentTree",
            &ContextTreeOverlap,
        );
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0].command, Command::Delete);

        assert!(
            overrides
                .prefix_conflicts_for(
                    &defaults,
                    &tree_slot,
                    &keys(chord("d", Modifiers::none())),
                    ContextId::Editor.default_predicate(),
                    &ContextTreeOverlap,
                )
                .is_empty()
        );
    }

    #[test]
    fn conflicts_use_effective_keys() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        overrides.set(save_query(), Some(keys(chord("r", Modifiers::ctrl()))));

        assert!(
            overrides
                .conflicts_for(
                    &defaults,
                    &new_query_tab(),
                    &keys(chord("s", Modifiers::ctrl())),
                    ContextId::Global.default_predicate(),
                    &ContextTreeOverlap,
                )
                .is_empty()
        );
        assert_eq!(
            overrides.conflicts_for(
                &defaults,
                &new_query_tab(),
                &keys(chord("r", Modifiers::ctrl())),
                ContextId::Global.default_predicate(),
                &ContextTreeOverlap,
            ),
            vec![save_query()]
        );
    }

    #[test]
    fn rebind_replaces_the_other_binding_and_its_reset_restores_it() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        let taken = keys(chord("s", Modifiers::ctrl()));

        let conflicts = overrides.conflicts_for(
            &defaults,
            &new_query_tab(),
            &taken,
            ContextId::Global.default_predicate(),
            &ContextTreeOverlap,
        );
        overrides.rebind(new_query_tab(), taken.clone(), &conflicts);

        assert_eq!(overrides.effective_keys(&save_query()), None);
        assert_eq!(
            overrides.effective_keys(&new_query_tab()),
            Some(taken.clone())
        );
        assert!(
            overrides
                .existing_conflicts(&defaults, &ContextTreeOverlap)
                .is_empty()
        );

        overrides.reset(&save_query());

        let conflicts = overrides.existing_conflicts(&defaults, &ContextTreeOverlap);
        assert_eq!(conflicts.len(), 2, "both sides of the collision are listed");

        overrides.reset_all();
        assert!(overrides.is_empty());
        assert!(
            overrides
                .existing_conflicts(&defaults, &ContextTreeOverlap)
                .is_empty()
        );
    }

    #[test]
    fn overrides_round_trip_through_storage_rows() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        overrides.set(new_query_tab(), Some(keys(chord("+", Modifiers::ctrl()))));
        overrides.set(save_query(), None);
        overrides.set(
            select_next_j(),
            Some(KeySequence::parse("g j").expect("valid sequence")),
        );
        overrides.set_predicate(select_next_j(), Some("Sidebar && !Input".to_string()));

        let dtos = overrides.to_dtos();
        assert_eq!(dtos.len(), 3);
        assert_eq!(dtos[0].context, "editor");
        assert_eq!(dtos[0].default_keys, "ctrl+s");
        assert_eq!(dtos[0].keys, None);
        assert_eq!(dtos[1].keys.as_deref(), Some("ctrl++"));
        assert_eq!(dtos[2].keys.as_deref(), Some("g j"));
        assert_eq!(dtos[2].predicate.as_deref(), Some("Sidebar && !Input"));

        assert_eq!(KeymapOverrides::from_dtos(&defaults, &dtos), overrides);
    }

    #[test]
    fn overrides_round_trip_through_storage() {
        let runtime = StorageRuntime::in_memory().expect("in-memory storage");
        let defaults = defaults();

        assert!(load_keymap_overrides(&runtime, &defaults).is_empty());

        let mut overrides = KeymapOverrides::new();
        overrides.set(
            new_query_tab(),
            Some(keys(chord("t", Modifiers::ctrl_shift()))),
        );
        overrides.set(save_query(), None);
        overrides.set_predicate(new_query_tab(), Some("Global && !Modal".to_string()));
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
            predicate: None,
        };
        let dtos = vec![row("ctrl+q", "ctrl+t"), row("ctrl+n", "")];

        assert!(KeymapOverrides::from_dtos(&defaults, &dtos).is_empty());

        let sequence = KeymapOverrides::from_dtos(&defaults, &[row("ctrl+n", "ctrl+k ctrl+t")]);
        assert_eq!(
            sequence.effective_keys(&new_query_tab()),
            Some(KeySequence::parse("ctrl+k ctrl+t").expect("valid sequence")),
            "key sequences are kept"
        );
    }
}
