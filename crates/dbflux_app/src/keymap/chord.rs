//! The chord types: one key press with its modifiers, and sequences of
//! them, with the text form they persist in.
//!
//! The engine moved to the `bolt_keys` crate, which owns this module's
//! types — it needs no vocabulary and no product fact. The re-export keeps
//! the crate's public paths alive.

pub use bolt_keys::{
    KeyChord, KeySequence, LEADER_KEY, MAX_SEQUENCE_LENGTH, Modifiers, ParseError,
};
