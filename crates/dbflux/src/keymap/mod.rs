#![allow(clippy::module_inception)]

mod actions;
mod chord;
mod defaults;
mod dispatcher;

// Re-export pure keymap types from dbflux_app for backwards compatibility
pub use dbflux_app::keymap::{
    Command, ContextId, FocusTarget, KeyChord, KeymapLayer, KeymapStack, Modifiers,
};

#[allow(unused_imports)]
pub use dbflux_app::keymap::ParseError;

// Re-export GPUI conversion helpers from chord
pub use chord::key_chord_from_gpui;

// GPUI-coupled types that stay in dbflux
pub use actions::*;
pub use defaults::default_keymap;
pub use dispatcher::CommandDispatcher;
