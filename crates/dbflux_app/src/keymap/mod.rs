//! The keymap: the product's vocabulary over Bolt's keyboard engine.
//!
//! The engine — key chords and their persisted text form, keymap layers
//! with hierarchical lookup, user overrides, conflict detection and the
//! shortcut recorder — lives in the `bolt_keys` crate and knows no product
//! fact: contexts are descriptor data and commands are action-id strings
//! (see `bolt::keys`). This module installs the product's context tree as
//! that engine's vocabulary, converts between `Command`/`ContextId` and the
//! engine's ids at the edges, and keeps the product halves that cannot move:
//! the GPUI-facing integration in `dbflux_ui_base::keymap`, and the storage
//! of user overrides, which reads and writes the persisted rows.
//!
//! Every binding is a GPUI [`KeyBinding`] with a context predicate in GPUI's
//! language (`Editor && vim_mode == normal`, `Sidebar && !Modal`, `A > B`),
//! so GPUI resolves precedence, key sequences (`g g`, `Ctrl+K Ctrl+S`) and
//! their timeout. Two kinds of key context feed those predicates:
//!
//! - A window root sets the identifier of the context that owns the keyboard
//!   (see `dbflux_ui_base::keymap::root_key_context`): the workspace computes
//!   it from its focus model, because keyboard focus often stays on the root
//!   while a panel is the active one. Bindings of these contexts dispatch
//!   [`RunCommand`], which the root handles.
//! - An element sets its own identifier on itself (`DataTable`, `Input`,
//!   `Modal`, `DocumentTree`, the modal editors). Its bindings dispatch the
//!   element's own actions and win over the root because they sit deeper.

mod chord;
mod focus;
mod keymap_layer;
mod overrides;
mod recording;
mod vocabulary;

pub use chord::{KeyChord, KeySequence, LEADER_KEY, MAX_SEQUENCE_LENGTH, Modifiers, ParseError};
pub use dbflux_core::keymap_types::{Command, ContextId};
pub use focus::FocusTarget;
pub use keymap_layer::{KeymapLayer, KeymapLayerView, KeymapStack};
pub use overrides::{
    BindingOverride, BindingSlot, ContextTreeOverlap, EffectiveBinding, KeymapOverrides,
    PredicateOverlap, contexts_overlap, default_slots, load_keymap_overrides,
    save_keymap_overrides,
};
pub use recording::{KeybindingRecorder, RecordingOutcome, RecordingState, Replacement};
