//! GPUI-coupled keymap types for DBFlux.
//!
//! This module contains keymap types that depend on GPUI:
//! - `actions` — GPUI action definitions
//! - `dispatcher` — Command dispatcher trait
//! - `defaults` — Default keymap bindings
//! - `chord_ext` — GPUI keystroke conversion utilities

mod actions;
mod chord_ext;
mod defaults;
mod dispatcher;

// Re-export pure keymap types from dbflux_app
pub use dbflux_app::keymap::{
    Command, ContextId, FocusTarget, KeyChord, KeymapLayer, KeymapStack, Modifiers,
};

#[allow(unused_imports)]
pub use dbflux_app::keymap::ParseError;

// GPUI-coupled types that stay in dbflux_ui
pub use actions::*;
pub use defaults::default_keymap;
pub use dispatcher::CommandDispatcher;

// Re-export GPUUI conversion helpers
pub use chord_ext::key_chord_from_gpui;
