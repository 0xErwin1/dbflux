//! Keymap domain types for DBFlux.
//!
//! This module contains pure domain types with no GPUI dependency.

mod chord;
mod focus;
mod keymap_layer;
mod overrides;
mod recording;

pub use chord::{KeyChord, KeySequence, LEADER_KEY, MAX_SEQUENCE_LENGTH, Modifiers, ParseError};
pub use dbflux_core::keymap_types::{Command, ContextId};
pub use focus::FocusTarget;
pub use keymap_layer::{KeymapLayer, KeymapStack};
pub use overrides::{
    BindingOverride, BindingSlot, ContextTreeOverlap, EffectiveBinding, KeymapOverrides,
    PredicateOverlap, contexts_overlap, default_slots, load_keymap_overrides,
    save_keymap_overrides,
};
pub use recording::{KeybindingRecorder, RecordingOutcome, RecordingState, Replacement};
