//! User keybinding overrides applied on top of a default keymap.
//!
//! An override targets one binding of the default keymap, a [`BindingSlot`],
//! and moves it to other keys, removes its shortcut, or moves it to another
//! context predicate. Bindings without an override keep their defaults, so a
//! new default added by a later release reaches users who customized other
//! bindings.
//!
//! The storage half — reading and writing the persisted rows — stays with
//! the product: it converts its stored rows to and from the engine's
//! [`OverrideRow`], whose text fields are the persisted identifiers.

use std::borrow::Cow;
use std::collections::HashMap;

use super::vocabulary::KeymapVocabulary;
use super::{KeySequence, KeymapLayer, KeymapStack};

/// One binding of the default keymap: the context whose layer declares it,
/// the command it runs, the keys it has by default and the context predicate
/// it applies in by default.
///
/// `context` and `command` are the persisted id strings the product
/// assigned; the engine never interprets them.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BindingSlot {
    pub context: &'static str,
    pub command: Cow<'static, str>,
    pub default_keys: KeySequence,
    pub default_predicate: &'static str,
}

impl BindingSlot {
    /// A binding under its context's default predicate.
    pub fn new(
        vocabulary: &KeymapVocabulary,
        context: &'static str,
        command: impl Into<Cow<'static, str>>,
        default_keys: impl Into<KeySequence>,
    ) -> Self {
        Self {
            context,
            command: command.into(),
            default_keys: default_keys.into(),
            default_predicate: vocabulary.default_predicate(context).unwrap_or_default(),
        }
    }

    /// A binding under a narrower predicate than its context's own.
    pub fn with_predicate(mut self, default_predicate: &'static str) -> Self {
        self.default_predicate = default_predicate;
        self
    }

    /// The row key: context id, command id and default keys in their stored
    /// text forms.
    pub(super) fn storage_key(&self, vocabulary: &KeymapVocabulary) -> (String, String, String) {
        (
            self.context.to_string(),
            vocabulary
                .storage_command_id(self.command.clone())
                .into_owned(),
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
/// Evaluating predicates needs the host's predicate language, which this
/// crate does not depend on; the product supplies the real implementation.
/// [`ContextTreeOverlap`] covers default predicates only.
pub trait PredicateOverlap {
    fn overlaps(&self, first: &str, second: &str) -> bool;
}

/// Overlap from the context tree: two contexts' default predicates overlap
/// when the contexts are the same or one inherits from the other. Any other
/// predicate overlaps only with an identical one.
pub struct ContextTreeOverlap<'a>(&'a KeymapVocabulary);

impl<'a> ContextTreeOverlap<'a> {
    pub fn new(vocabulary: &'a KeymapVocabulary) -> Self {
        Self(vocabulary)
    }
}

impl PredicateOverlap for ContextTreeOverlap<'_> {
    fn overlaps(&self, first: &str, second: &str) -> bool {
        match (
            self.0.context_for_default_predicate(first),
            self.0.context_for_default_predicate(second),
        ) {
            (Some(first), Some(second)) => self.0.contexts_overlap(first, second),
            _ => first == second,
        }
    }
}

/// Whether a key pressed in one of the two contexts can reach bindings of
/// the other: they are the same context, or one inherits from the other.
pub fn contexts_overlap(vocabulary: &KeymapVocabulary, first: &str, second: &str) -> bool {
    vocabulary.contexts_overlap(first, second)
}

/// Every binding of `defaults`, layer by layer in the vocabulary's canonical
/// order and, inside a layer, in declaration order.
pub fn default_slots(vocabulary: &KeymapVocabulary, defaults: &KeymapStack) -> Vec<BindingSlot> {
    let mut slots = Vec::new();

    for context in vocabulary.context_ids() {
        let Some(layer) = defaults.layer(context) else {
            continue;
        };

        for (keys, command) in layer.ordered_bindings() {
            slots.push(
                BindingSlot::new(vocabulary, layer.context(), command.clone(), keys.clone())
                    .with_predicate(layer.predicate_for(vocabulary, keys)),
            );
        }
    }

    slots
}

/// One binding of the keymap in force: a default binding with its overrides
/// applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveBinding {
    pub slot: BindingSlot,
    pub keys: KeySequence,
    /// Context predicate in the host's key context language.
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

    pub(super) fn store(&mut self, slot: BindingSlot, binding_override: BindingOverride) {
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
    pub fn apply(&self, vocabulary: &KeymapVocabulary, defaults: &KeymapStack) -> KeymapStack {
        let mut stack = KeymapStack::new();

        for context in vocabulary.context_ids() {
            let Some(default_layer) = defaults.layer(context) else {
                continue;
            };

            let mut layer = KeymapLayer::new(context);
            let mut moved = Vec::new();

            for (keys, command) in default_layer.ordered_bindings() {
                let slot = BindingSlot::new(vocabulary, context, command.clone(), keys.clone())
                    .with_predicate(default_layer.predicate_for(vocabulary, keys));

                match self.entries.get(&slot) {
                    None => layer.bind(keys.clone(), command.clone()),
                    Some(binding_override) => {
                        if let Some(new_keys) = &binding_override.keys {
                            moved.push((new_keys.clone(), command));
                        }
                    }
                }
            }

            for (keys, command) in moved {
                layer.bind(keys, command.clone());
            }

            stack.add_layer(layer);
        }

        stack
    }

    /// Every bound binding of the keymap in force, in registration order:
    /// the defaults the user did not change, then the user's bindings.
    ///
    /// Inside each group, the contexts a window root sets come first, in the
    /// vocabulary's canonical order so that the global context precedes the
    /// contexts that inherit it, and the contexts an element sets on itself
    /// come last. The host's key resolver lets a later binding of a longer
    /// key sequence keep waiting for its next key over an earlier single-key
    /// binding, so the element bindings (`d d` in a table) must follow the
    /// root ones (`d`).
    pub fn effective_bindings(
        &self,
        vocabulary: &KeymapVocabulary,
        defaults: &KeymapStack,
    ) -> Vec<EffectiveBinding> {
        let mut slots = default_slots(vocabulary, defaults);
        slots.sort_by_key(|slot| vocabulary.is_element_context(slot.context));

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
        vocabulary: &KeymapVocabulary,
        defaults: &KeymapStack,
        slot: &BindingSlot,
        keys: &KeySequence,
        predicate: &str,
        overlap: &dyn PredicateOverlap,
    ) -> Vec<BindingSlot> {
        default_slots(vocabulary, defaults)
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
        vocabulary: &KeymapVocabulary,
        defaults: &KeymapStack,
        slot: &BindingSlot,
        keys: &KeySequence,
        predicate: &str,
        overlap: &dyn PredicateOverlap,
    ) -> Vec<BindingSlot> {
        default_slots(vocabulary, defaults)
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
        vocabulary: &KeymapVocabulary,
        defaults: &KeymapStack,
        overlap: &dyn PredicateOverlap,
    ) -> Vec<(BindingSlot, Vec<BindingSlot>)> {
        let slots = default_slots(vocabulary, defaults);

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
    ///
    /// The row fields are the persisted identifiers: the context id, the
    /// command id and the default keys name the overridden binding; the keys
    /// and the predicate are the user's change. The command id is the
    /// product's storage id for the binding's action id (see
    /// [`KeymapVocabulary::storage_command_id`]).
    pub fn to_rows(&self, vocabulary: &KeymapVocabulary) -> Vec<OverrideRow> {
        let mut rows: Vec<OverrideRow> = self
            .entries
            .iter()
            .map(|(slot, binding_override)| {
                let (context, command_id, default_keys) = slot.storage_key(vocabulary);

                OverrideRow {
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

        rows.sort_by(|first, second| {
            (&first.context, &first.command_id, &first.default_keys).cmp(&(
                &second.context,
                &second.command_id,
                &second.default_keys,
            ))
        });

        rows
    }

    /// Rebuilds the overrides from stored rows.
    ///
    /// A row whose default binding no longer exists (a later release changed
    /// or removed it), or whose keys do not parse, is skipped with a warning,
    /// and that binding keeps its current default. Predicates are stored
    /// only after they parse, so they are taken as they are.
    pub fn from_rows(
        vocabulary: &KeymapVocabulary,
        defaults: &KeymapStack,
        rows: &[OverrideRow],
    ) -> Self {
        let slots_by_key: HashMap<(String, String, String), BindingSlot> =
            default_slots(vocabulary, defaults)
                .into_iter()
                .map(|slot| (slot.storage_key(vocabulary), slot))
                .collect();

        let mut overrides = Self::new();

        for row in rows {
            let key = (
                row.context.clone(),
                row.command_id.clone(),
                row.default_keys.clone(),
            );

            let Some(slot) = slots_by_key.get(&key) else {
                log::warn!(
                    "Ignoring keybinding override for {}/{} ({}): the default binding no longer exists",
                    row.context,
                    row.command_id,
                    row.default_keys
                );
                continue;
            };

            let keys = match row.keys.as_deref().map(KeySequence::from_storage_string) {
                None => None,
                Some(Ok(keys)) => Some(keys),
                Some(Err(error)) => {
                    log::warn!(
                        "Ignoring keybinding override for {}/{}: invalid keys: {error}",
                        row.context,
                        row.command_id
                    );
                    continue;
                }
            };

            let predicate = row
                .predicate
                .clone()
                .filter(|predicate| !predicate.trim().is_empty());

            overrides.store(slot.clone(), BindingOverride { keys, predicate });
        }

        overrides
    }
}

/// One persisted keybinding override, in the engine's storage-neutral shape.
///
/// The fields hold exactly the text the product persists: `context`,
/// `command_id` and `default_keys` name the default binding, `keys` is the
/// user's replacement in the chord storage-string form (`None` removes the
/// shortcut) and `predicate` is the user's context predicate (`None` keeps
/// the default one).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverrideRow {
    pub context: String,
    pub command_id: String,
    pub default_keys: String,
    pub keys: Option<String>,
    pub predicate: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keymap_layer::{KeymapLayer, KeymapStack};
    use crate::{ContextSpec, KeyChord, KeymapVocabulary, Modifiers};

    fn vocabulary() -> KeymapVocabulary {
        KeymapVocabulary::new(
            vec![
                ContextSpec::new("global", None, "Global"),
                ContextSpec::new("sidebar", Some("global"), "Sidebar"),
                ContextSpec::new("editor", Some("global"), "Editor"),
                ContextSpec::new("document_tree", None, "DocumentTree").as_element(),
            ],
            "global",
        )
    }

    fn chord(key: &str, modifiers: Modifiers) -> KeyChord {
        KeyChord::new(key, modifiers)
    }

    fn keys(chord: KeyChord) -> KeySequence {
        KeySequence::from(chord)
    }

    fn defaults() -> KeymapStack {
        let mut global = KeymapLayer::new("global");
        global.bind(
            chord("p", Modifiers::ctrl_shift()),
            "toggle_command_palette",
        );
        global.bind(chord("n", Modifiers::ctrl()), "new_query_tab");

        let mut editor = KeymapLayer::new("editor");
        editor.bind(chord("s", Modifiers::ctrl()), "save_query");
        editor.bind(chord("p", Modifiers::ctrl()), "open_saved_queries");

        let mut sidebar = KeymapLayer::new("sidebar");
        sidebar.bind(chord("j", Modifiers::none()), "select_next");
        sidebar.bind(chord("down", Modifiers::none()), "select_next");

        let mut tree = KeymapLayer::new("document_tree");
        tree.bind(KeySequence::parse("d d").expect("valid sequence"), "delete");

        let mut stack = KeymapStack::new();
        stack.add_layer(global);
        stack.add_layer(editor);
        stack.add_layer(sidebar);
        stack.add_layer(tree);
        stack
    }

    fn slot(context: &'static str, command: &'static str, default_chord: KeyChord) -> BindingSlot {
        BindingSlot::new(&vocabulary(), context, command, default_chord)
    }

    fn new_query_tab() -> BindingSlot {
        slot("global", "new_query_tab", chord("n", Modifiers::ctrl()))
    }

    fn save_query() -> BindingSlot {
        slot("editor", "save_query", chord("s", Modifiers::ctrl()))
    }

    fn select_next_j() -> BindingSlot {
        slot("sidebar", "select_next", chord("j", Modifiers::none()))
    }

    #[test]
    fn default_slots_follow_context_and_declaration_order() {
        let slots = default_slots(&vocabulary(), &defaults());

        assert_eq!(slots.len(), 7);
        assert_eq!(slots[0].command, "toggle_command_palette");
        assert_eq!(slots[1].command, "new_query_tab");
        assert_eq!(slots[2].context, "sidebar");
        assert_eq!(slots[2].default_keys, keys(chord("j", Modifiers::none())));
        assert_eq!(slots[4].context, "editor");
        assert_eq!(slots[6].context, "document_tree");
    }

    #[test]
    fn apply_moves_rebound_bindings_and_drops_unbound_ones() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        overrides.set(new_query_tab(), Some(keys(chord("t", Modifiers::ctrl()))));
        overrides.set(save_query(), None);

        let effective = overrides.apply(&vocabulary(), &defaults);

        assert_eq!(
            effective.resolve(&vocabulary(), "global", &chord("t", Modifiers::ctrl())),
            Some(Cow::Borrowed("new_query_tab"))
        );
        assert_eq!(
            effective.resolve(&vocabulary(), "global", &chord("n", Modifiers::ctrl())),
            None
        );
        assert_eq!(
            effective.resolve(&vocabulary(), "editor", &chord("s", Modifiers::ctrl())),
            None
        );
        assert_eq!(
            effective.resolve(&vocabulary(), "editor", &chord("t", Modifiers::ctrl())),
            Some(Cow::Borrowed("new_query_tab")),
            "the editor still inherits the moved global binding"
        );
        assert_eq!(
            effective.resolve(&vocabulary(), "sidebar", &chord("down", Modifiers::none())),
            Some(Cow::Borrowed("select_next")),
            "bindings without an override keep their default"
        );
    }

    #[test]
    fn sequences_can_replace_single_chords() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        let sequence = KeySequence::parse("ctrl+k ctrl+s").expect("valid sequence");
        overrides.set(save_query(), Some(sequence.clone()));

        let effective = overrides.apply(&vocabulary(), &defaults);

        assert_eq!(
            effective.resolve_sequence(&vocabulary(), "editor", &sequence),
            Some(Cow::Borrowed("save_query"))
        );
        assert_eq!(
            effective.keys_for_command(&vocabulary(), "editor", "save_query"),
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

        assert_eq!(overrides.effective_predicate(&slot), "Editor");

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
        overrides.set_predicate(slot.clone(), Some("Editor".to_string()));
        assert!(overrides.is_empty(), "the defaults are not an override");
    }

    #[test]
    fn effective_bindings_put_user_and_element_bindings_last() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        overrides.set(new_query_tab(), Some(keys(chord("t", Modifiers::ctrl()))));
        overrides.set(save_query(), None);

        let bindings = overrides.effective_bindings(&vocabulary(), &defaults);
        let commands: Vec<(&str, bool)> = bindings
            .iter()
            .map(|binding| (binding.slot.command.as_ref(), binding.is_user))
            .collect();

        assert_eq!(
            commands,
            vec![
                ("toggle_command_palette", false),
                ("select_next", false),
                ("select_next", false),
                ("open_saved_queries", false),
                ("delete", false),
                ("new_query_tab", true),
            ],
            "unbound save_query is dropped, the tree comes after the root contexts"
        );
        assert_eq!(bindings[4].predicate, "DocumentTree");
        assert_eq!(bindings[0].predicate, "Global");
    }

    #[test]
    fn conflicts_cover_the_same_and_inherited_contexts_only() {
        let defaults = defaults();
        let overrides = KeymapOverrides::new();
        let vocabulary = vocabulary();

        let same_context = overrides.conflicts_for(
            &vocabulary,
            &defaults,
            &new_query_tab(),
            &keys(chord("p", Modifiers::ctrl_shift())),
            "Global",
            &ContextTreeOverlap::new(&vocabulary),
        );
        assert_eq!(same_context.len(), 1);
        assert_eq!(same_context[0].command, "toggle_command_palette");

        let child_context = overrides.conflicts_for(
            &vocabulary,
            &defaults,
            &new_query_tab(),
            &keys(chord("s", Modifiers::ctrl())),
            "Global",
            &ContextTreeOverlap::new(&vocabulary),
        );
        assert_eq!(child_context, vec![save_query()]);

        let sibling_context = overrides.conflicts_for(
            &vocabulary,
            &defaults,
            &save_query(),
            &keys(chord("j", Modifiers::none())),
            "Editor",
            &ContextTreeOverlap::new(&vocabulary),
        );
        assert!(
            sibling_context.is_empty(),
            "the sidebar and the editor never see each other's keys"
        );

        let own_keys = overrides.conflicts_for(
            &vocabulary,
            &defaults,
            &save_query(),
            &keys(chord("s", Modifiers::ctrl())),
            "Editor",
            &ContextTreeOverlap::new(&vocabulary),
        );
        assert!(own_keys.is_empty());
    }

    #[test]
    fn a_moved_predicate_changes_what_the_binding_conflicts_with() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        let vocabulary = vocabulary();
        overrides.set_predicate(select_next_j(), Some("Editor".to_string()));

        let conflicts = overrides.conflicts_for(
            &vocabulary,
            &defaults,
            &save_query(),
            &keys(chord("j", Modifiers::none())),
            "Editor",
            &ContextTreeOverlap::new(&vocabulary),
        );
        assert_eq!(conflicts, vec![select_next_j()]);
    }

    #[test]
    fn prefix_conflicts_find_sequences_sharing_a_start() {
        let defaults = defaults();
        let overrides = KeymapOverrides::new();
        let vocabulary = vocabulary();
        let tree_slot = slot(
            "document_tree",
            "preview_document",
            chord("e", Modifiers::none()),
        );

        let prefixes = overrides.prefix_conflicts_for(
            &vocabulary,
            &defaults,
            &tree_slot,
            &keys(chord("d", Modifiers::none())),
            "DocumentTree",
            &ContextTreeOverlap::new(&vocabulary),
        );
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0].command, "delete");

        assert!(
            overrides
                .prefix_conflicts_for(
                    &vocabulary,
                    &defaults,
                    &tree_slot,
                    &keys(chord("d", Modifiers::none())),
                    "Editor",
                    &ContextTreeOverlap::new(&vocabulary),
                )
                .is_empty()
        );
    }

    #[test]
    fn conflicts_use_effective_keys() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        let vocabulary = vocabulary();
        overrides.set(save_query(), Some(keys(chord("r", Modifiers::ctrl()))));

        assert!(
            overrides
                .conflicts_for(
                    &vocabulary,
                    &defaults,
                    &new_query_tab(),
                    &keys(chord("s", Modifiers::ctrl())),
                    "Global",
                    &ContextTreeOverlap::new(&vocabulary),
                )
                .is_empty()
        );
        assert_eq!(
            overrides.conflicts_for(
                &vocabulary,
                &defaults,
                &new_query_tab(),
                &keys(chord("r", Modifiers::ctrl())),
                "Global",
                &ContextTreeOverlap::new(&vocabulary),
            ),
            vec![save_query()]
        );
    }

    #[test]
    fn rebind_replaces_the_other_binding_and_its_reset_restores_it() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        let vocabulary = vocabulary();
        let taken = keys(chord("s", Modifiers::ctrl()));

        let conflicts = overrides.conflicts_for(
            &vocabulary,
            &defaults,
            &new_query_tab(),
            &taken,
            "Global",
            &ContextTreeOverlap::new(&vocabulary),
        );
        overrides.rebind(new_query_tab(), taken.clone(), &conflicts);

        assert_eq!(overrides.effective_keys(&save_query()), None);
        assert_eq!(
            overrides.effective_keys(&new_query_tab()),
            Some(taken.clone())
        );
        assert!(
            overrides
                .existing_conflicts(
                    &vocabulary,
                    &defaults,
                    &ContextTreeOverlap::new(&vocabulary)
                )
                .is_empty()
        );

        overrides.reset(&save_query());

        let conflicts = overrides.existing_conflicts(
            &vocabulary,
            &defaults,
            &ContextTreeOverlap::new(&vocabulary),
        );
        assert_eq!(conflicts.len(), 2, "both sides of the collision are listed");

        overrides.reset_all();
        assert!(overrides.is_empty());
        assert!(
            overrides
                .existing_conflicts(
                    &vocabulary,
                    &defaults,
                    &ContextTreeOverlap::new(&vocabulary)
                )
                .is_empty()
        );
    }

    #[test]
    fn overrides_round_trip_through_storage_rows_with_unchanged_identifiers() {
        let defaults = defaults();
        let mut overrides = KeymapOverrides::new();
        let vocabulary = vocabulary();
        overrides.set(new_query_tab(), Some(keys(chord("+", Modifiers::ctrl()))));
        overrides.set(save_query(), None);
        overrides.set(
            select_next_j(),
            Some(KeySequence::parse("g j").expect("valid sequence")),
        );
        overrides.set_predicate(select_next_j(), Some("Sidebar && !Input".to_string()));

        let rows = overrides.to_rows(&vocabulary);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].context, "editor");
        assert_eq!(rows[0].default_keys, "ctrl+s");
        assert_eq!(rows[0].keys, None);
        assert_eq!(rows[1].keys.as_deref(), Some("ctrl++"));
        assert_eq!(rows[2].keys.as_deref(), Some("g j"));
        assert_eq!(rows[2].predicate.as_deref(), Some("Sidebar && !Input"));

        assert_eq!(
            KeymapOverrides::from_rows(&vocabulary, &defaults, &rows),
            overrides
        );
    }

    #[test]
    fn stale_or_invalid_rows_are_skipped() {
        let defaults = defaults();
        let row = |default_keys: &str, keys: &str| OverrideRow {
            context: "global".to_string(),
            command_id: "new_query_tab".to_string(),
            default_keys: default_keys.to_string(),
            keys: Some(keys.to_string()),
            predicate: None,
        };
        let rows = vec![row("ctrl+q", "ctrl+t"), row("ctrl+n", "")];

        assert!(KeymapOverrides::from_rows(&vocabulary(), &defaults, &rows).is_empty());

        let sequence =
            KeymapOverrides::from_rows(&vocabulary(), &defaults, &[row("ctrl+n", "ctrl+k ctrl+t")]);
        assert_eq!(
            sequence.effective_keys(&new_query_tab()),
            Some(KeySequence::parse("ctrl+k ctrl+t").expect("valid sequence")),
            "key sequences are kept"
        );
    }
}
