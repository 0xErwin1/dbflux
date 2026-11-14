//! Bolt's keyboard engine: the mechanics every product's keymap shares.
//!
//! This crate carries the product-independent half of a keymap: key chords
//! and their persisted text form, keymap layers with hierarchical lookup,
//! user overrides merged on top of a default keymap, conflict detection
//! between bindings, and the state machine behind "record a new shortcut".
//!
//! It carries no vocabulary. Which contexts exist, how they nest, what
//! predicate each one applies under, and which action an id names are the
//! product's facts; the engine meets them as data — a [`KeymapVocabulary`]
//! of context descriptors and action-id strings the product supplies to
//! every lookup. Nothing here depends on GPUI, a storage layer, or a
//! product: the persistence half of overrides (reading and writing the
//! rows) stays with the product, which converts its stored rows to the
//! engine's [`OverrideRow`] shape.
//!
//! The umbrella `bolt` crate re-exports this engine as `bolt::keys`; a
//! consumer that only needs the keyboard mechanics can take the small
//! crate directly.

mod chord;
mod keymap_layer;
mod overrides;
mod recording;
mod vocabulary;

pub use chord::{KeyChord, KeySequence, LEADER_KEY, MAX_SEQUENCE_LENGTH, Modifiers, ParseError};
pub use keymap_layer::{KeymapLayer, KeymapStack};
pub use overrides::{
    BindingOverride, BindingSlot, ContextTreeOverlap, EffectiveBinding, KeymapOverrides,
    OverrideRow, PredicateOverlap, contexts_overlap, default_slots,
};
pub use recording::{KeybindingRecorder, RecordingOutcome, RecordingState, Replacement};
pub use vocabulary::{ContextSpec, KeymapVocabulary};
