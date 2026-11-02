//! User keybinding overrides applied on top of the default keymap.
//!
//! An override targets one binding of the default keymap, a [`BindingSlot`],
//! and moves it to other keys, removes its shortcut, or moves it to another
//! context predicate. Bindings without an override keep their defaults, so a
//! new default added by a later release reaches users who customized other
//! bindings.
//!
//! The merging and the conflict detection live in `bolt_keys`, over the
//! engine's id-string slots; this module adapts the product's typed slots
//! (`ContextId`, `Command`) and keeps the storage half: reading and writing
//! the persisted rows through `dbflux_storage`.

use std::borrow::Cow;

use bolt_keys::OverrideRow;
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_storage::error::StorageError;
use dbflux_storage::repositories::keybinding_overrides::KeybindingOverrideDto;

use super::keymap_layer::KeymapStack;
use super::vocabulary::{command_for, context_for, vocabulary};
use super::{Command, ContextId, KeySequence};

pub use bolt_keys::PredicateOverlap;

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

    /// The engine slot: the same binding with its persisted identifiers.
    pub(super) fn engine(&self) -> bolt_keys::BindingSlot {
        bolt_keys::BindingSlot::new(
            vocabulary(),
            self.context.id(),
            self.command.action_id(),
            self.default_keys.clone(),
        )
        .with_predicate(self.default_predicate)
    }

    /// The typed slot for an engine slot, or `None` (logged) when an id does
    /// not name a product context or command.
    pub(super) fn from_engine(slot: &bolt_keys::BindingSlot) -> Option<Self> {
        Some(Self {
            context: context_for(slot.context),
            command: command_for(&slot.command)?,
            default_keys: slot.default_keys.clone(),
            default_predicate: slot.default_predicate,
        })
    }
}

pub use bolt_keys::BindingOverride;

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

/// Overlap from the context tree: two contexts' default predicates overlap
/// when the contexts are the same or one inherits from the other. Any other
/// predicate overlaps only with an identical one.
pub struct ContextTreeOverlap;

impl PredicateOverlap for ContextTreeOverlap {
    fn overlaps(&self, first: &str, second: &str) -> bool {
        let vocabulary = vocabulary();

        match (
            vocabulary.context_for_default_predicate(first),
            vocabulary.context_for_default_predicate(second),
        ) {
            (Some(first), Some(second)) => vocabulary.contexts_overlap(first, second),
            _ => first == second,
        }
    }
}

/// Whether a key pressed in one of the two contexts can reach bindings of the
/// other: they are the same context, or one inherits from the other.
pub fn contexts_overlap(first: ContextId, second: ContextId) -> bool {
    vocabulary().contexts_overlap(first.id(), second.id())
}

/// The set of user overrides, keyed by the default binding each replaces.
///
/// An override that would restore the default keys and predicate is not
/// stored, so [`KeymapOverrides::len`] counts only bindings that really
/// differ.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeymapOverrides(bolt_keys::KeymapOverrides);

impl KeymapOverrides {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn is_overridden(&self, slot: &BindingSlot) -> bool {
        self.0.is_overridden(&slot.engine())
    }

    /// The keys the binding has once overrides apply, `None` when unbound.
    pub fn effective_keys(&self, slot: &BindingSlot) -> Option<KeySequence> {
        self.0.effective_keys(&slot.engine())
    }

    /// The context predicate the binding has once overrides apply.
    pub fn effective_predicate<'a>(&'a self, slot: &BindingSlot) -> Cow<'a, str> {
        self.0.effective_predicate(&slot.engine())
    }

    /// The predicate the user gave the binding, `None` when it keeps the
    /// default one.
    pub fn custom_predicate(&self, slot: &BindingSlot) -> Option<&str> {
        self.0.custom_predicate(&slot.engine())
    }

    /// Gives `slot` the keys `keys`, or removes its shortcut with `None`,
    /// keeping its predicate.
    pub fn set(&mut self, slot: BindingSlot, keys: Option<KeySequence>) {
        self.0.set(slot.engine(), keys);
    }

    /// Moves `slot` to the context predicate `predicate`, keeping its keys.
    /// The default predicate, or `None`, restores the default context.
    pub fn set_predicate(&mut self, slot: BindingSlot, predicate: Option<String>) {
        self.0.set_predicate(slot.engine(), predicate);
    }

    /// Moves `slot` to `keys` and removes the shortcut of every binding in
    /// `replaced`, the bindings that held the keys before. Each of them gets
    /// its keys back with [`KeymapOverrides::reset`].
    pub fn rebind(&mut self, slot: BindingSlot, keys: KeySequence, replaced: &[BindingSlot]) {
        let replaced: Vec<bolt_keys::BindingSlot> =
            replaced.iter().map(BindingSlot::engine).collect();
        self.0.rebind(slot.engine(), keys, &replaced);
    }

    /// Restores the default keys and predicate of `slot`.
    pub fn reset(&mut self, slot: &BindingSlot) {
        self.0.reset(&slot.engine());
    }

    /// Restores the default keymap.
    pub fn reset_all(&mut self) {
        self.0.reset_all();
    }

    /// The engine overrides: the same entries with id-string slots.
    pub(super) fn engine(&self) -> &bolt_keys::KeymapOverrides {
        &self.0
    }

    /// Builds the effective keymap: `defaults` with every override applied.
    ///
    /// A binding stays in the layer of the context that declares it, even
    /// when the user moved it to another predicate, so lookups such as the
    /// keys shown next to a command keep finding it. Moved bindings are
    /// bound after the untouched ones of their layer, so a stored override
    /// wins over a default that uses the same keys.
    pub fn apply(&self, defaults: &KeymapStack) -> KeymapStack {
        KeymapStack::from_engine_stack(self.0.apply(vocabulary(), defaults.engine()))
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
        self.0
            .effective_bindings(vocabulary(), defaults.engine())
            .into_iter()
            .filter_map(|binding| {
                Some(EffectiveBinding {
                    slot: BindingSlot::from_engine(&binding.slot)?,
                    keys: binding.keys,
                    predicate: binding.predicate,
                    is_user: binding.is_user,
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
        self.0
            .conflicts_for(
                vocabulary(),
                defaults.engine(),
                &slot.engine(),
                keys,
                predicate,
                overlap,
            )
            .iter()
            .filter_map(BindingSlot::from_engine)
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
        self.0
            .prefix_conflicts_for(
                vocabulary(),
                defaults.engine(),
                &slot.engine(),
                keys,
                predicate,
                overlap,
            )
            .iter()
            .filter_map(BindingSlot::from_engine)
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
        self.0
            .existing_conflicts(vocabulary(), defaults.engine(), overlap)
            .into_iter()
            .filter_map(|(slot, others)| {
                let others: Vec<BindingSlot> =
                    others.iter().filter_map(BindingSlot::from_engine).collect();
                Some((BindingSlot::from_engine(&slot)?, others))
            })
            .collect()
    }

    /// Rows to persist, sorted so the stored order is stable.
    pub fn to_dtos(&self) -> Vec<KeybindingOverrideDto> {
        self.0
            .to_rows(vocabulary())
            .into_iter()
            .map(|row| KeybindingOverrideDto {
                context: row.context,
                command_id: row.command_id,
                default_keys: row.default_keys,
                keys: row.keys,
                predicate: row.predicate,
            })
            .collect()
    }

    /// Rebuilds the overrides from stored rows.
    ///
    /// A row whose default binding no longer exists (a later release changed
    /// or removed it), or whose keys do not parse, is skipped with a warning,
    /// and that binding keeps its current default. Predicates are stored
    /// only after they parse, so they are taken as they are.
    pub fn from_dtos(defaults: &KeymapStack, dtos: &[KeybindingOverrideDto]) -> Self {
        let rows: Vec<OverrideRow> = dtos
            .iter()
            .map(|dto| OverrideRow {
                context: dto.context.clone(),
                command_id: dto.command_id.clone(),
                default_keys: dto.default_keys.clone(),
                keys: dto.keys.clone(),
                predicate: dto.predicate.clone(),
            })
            .collect();

        Self(bolt_keys::KeymapOverrides::from_rows(
            vocabulary(),
            defaults.engine(),
            &rows,
        ))
    }
}

/// Every binding of `defaults`, layer by layer in [`ContextId::all_variants`]
/// order and, inside a layer, in declaration order.
pub fn default_slots(defaults: &KeymapStack) -> Vec<BindingSlot> {
    bolt_keys::default_slots(vocabulary(), defaults.engine())
        .iter()
        .filter_map(BindingSlot::from_engine)
        .collect()
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
    use crate::keymap::{KeyChord, KeymapLayer};
    use bolt_keys::Modifiers;

    fn chord(key: &str, modifiers: Modifiers) -> KeyChord {
        KeyChord::new(key, modifiers)
    }

    fn keys(chord: KeyChord) -> KeySequence {
        KeySequence::from(chord)
    }

    fn new_query_tab() -> BindingSlot {
        BindingSlot::new(
            ContextId::Global,
            Command::NewQueryTab,
            chord("n", Modifiers::ctrl()),
        )
    }

    fn save_query() -> BindingSlot {
        BindingSlot::new(
            ContextId::Editor,
            Command::SaveQuery,
            chord("s", Modifiers::ctrl()),
        )
    }

    #[test]
    fn typed_slots_round_trip_through_the_engine_with_their_identifiers() {
        let slot = BindingSlot::new(
            ContextId::Editor,
            Command::SaveQuery,
            chord("s", Modifiers::ctrl()),
        )
        .with_predicate("Editor && vim_mode == normal");

        let engine_slot = slot.engine();
        assert_eq!(engine_slot.context, "editor");
        assert_eq!(engine_slot.command, "save_query");
        assert_eq!(engine_slot.default_keys.to_storage_string(), "ctrl+s");
        assert_eq!(
            engine_slot.default_predicate,
            "Editor && vim_mode == normal"
        );

        assert_eq!(BindingSlot::from_engine(&engine_slot), Some(slot));
    }

    #[test]
    fn overrides_round_trip_through_storage() {
        let runtime = StorageRuntime::in_memory().expect("in-memory storage");
        let mut defaults = KeymapStack::new();

        let mut global = KeymapLayer::new(ContextId::Global);
        global.bind(chord("n", Modifiers::ctrl()), Command::NewQueryTab);
        let mut editor = KeymapLayer::new(ContextId::Editor);
        editor.bind(chord("s", Modifiers::ctrl()), Command::SaveQuery);
        defaults.add_layer(global);
        defaults.add_layer(editor);

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
    fn stored_rows_keep_the_persisted_identifier_texts() {
        let mut overrides = KeymapOverrides::new();
        overrides.set(new_query_tab(), Some(keys(chord("+", Modifiers::ctrl()))));
        overrides.set(save_query(), None);

        let dtos = overrides.to_dtos();
        assert_eq!(dtos.len(), 2);
        assert_eq!(dtos[0].context, "editor");
        assert_eq!(dtos[0].command_id, "save_query");
        assert_eq!(dtos[0].default_keys, "ctrl+s");
        assert_eq!(dtos[0].keys, None);
        assert_eq!(dtos[1].context, "global");
        assert_eq!(dtos[1].command_id, "new_query_tab");
        assert_eq!(dtos[1].default_keys, "ctrl+n");
        assert_eq!(dtos[1].keys.as_deref(), Some("ctrl++"));
    }
}
