//! The product's context vocabulary, installed from the product's own
//! context tree.
//!
//! The engine in `bolt_keys` is vocabulary-free: it knows contexts as
//! descriptor data and commands as action-id strings. This module derives
//! that data from `dbflux_core::keymap_types` — the persisted vocabulary —
//! once, in canonical order, and converts between the product's enums and
//! the engine's ids at the façade's edges. The id strings pass through
//! unchanged, so a stored override keeps naming the same binding.

use std::borrow::Cow;
use std::sync::LazyLock;

use bolt_keys::{ContextSpec, KeymapVocabulary};

use super::{Command, ContextId};

static CONTEXT_VOCABULARY: LazyLock<KeymapVocabulary> = LazyLock::new(|| {
    let contexts = ContextId::all_variants()
        .iter()
        .map(|context| ContextSpec {
            id: context.id(),
            parent: context.parent().map(|parent| parent.id()),
            default_predicate: context.default_predicate(),
            inherits_global_chords: context.inherits_global_chords(),
            is_element_context: context.is_element_context(),
        })
        .collect();

    KeymapVocabulary::new(contexts, ContextId::Global.id())
        .with_command_storage_id(storage_command_id)
});

/// The command id a stored row names for the binding carrying `action_id`:
/// [`Command::id`], which names the command's family and leaves the
/// argument to the row's default keys (`switch_to_tab_3` is stored as
/// `switch_to_tab`), exactly what the overrides always stored.
fn storage_command_id(action_id: Cow<'static, str>) -> Cow<'static, str> {
    match Command::from_action_id(&action_id) {
        Some(command) => Cow::Borrowed(command.id()),
        None => action_id,
    }
}

/// The product's context tree as engine data.
pub(super) fn vocabulary() -> &'static KeymapVocabulary {
    &CONTEXT_VOCABULARY
}

/// The command whose action id is `action_id`.
///
/// Ids enter the engine only through [`Command::action_id`], so `None`
/// cannot happen; it is logged rather than trusted.
pub(super) fn command_for(action_id: &str) -> Option<Command> {
    let command = Command::from_action_id(action_id);

    if command.is_none() {
        log::error!("Keymap engine holds unknown action id `{action_id}`");
    }

    command
}

/// The context whose id is `id`.
///
/// Ids enter the engine only through [`ContextId::id`], so the fallback
/// never fires; it is logged rather than trusted.
pub(super) fn context_for(id: &'static str) -> ContextId {
    let found = ContextId::all_variants()
        .iter()
        .copied()
        .find(|context| context.id() == id);

    match found {
        Some(context) => context,
        None => {
            log::error!("Keymap engine holds unknown context id `{id}`");
            ContextId::Global
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_installed_vocabulary_mirrors_the_product_context_tree() {
        let vocabulary = vocabulary();

        for context in ContextId::all_variants() {
            let spec = vocabulary
                .get(context.id())
                .unwrap_or_else(|| panic!("context {} is in the vocabulary", context.id()));

            assert_eq!(spec.id, context.id());
            assert_eq!(spec.parent, context.parent().map(|parent| parent.id()));
            assert_eq!(spec.default_predicate, context.default_predicate());
            assert_eq!(
                spec.inherits_global_chords,
                context.inherits_global_chords()
            );
            assert_eq!(spec.is_element_context, context.is_element_context());
        }

        assert_eq!(vocabulary.global_id(), ContextId::Global.id());
    }

    #[test]
    fn engine_lookups_convert_back_to_the_product_enums() {
        for command in Command::all_variants() {
            assert_eq!(command_for(&command.action_id()), Some(command));
        }

        for context in ContextId::all_variants() {
            assert_eq!(context_for(context.id()), *context);
        }
    }

    #[test]
    fn the_storage_command_id_is_what_the_overrides_always_stored() {
        let vocabulary = vocabulary();

        assert_eq!(
            vocabulary.storage_command_id(Cow::Borrowed("toggle_command_palette")),
            "toggle_command_palette"
        );
        assert_eq!(
            vocabulary.storage_command_id(Cow::Borrowed("switch_to_tab_3")),
            "switch_to_tab"
        );
    }
}
