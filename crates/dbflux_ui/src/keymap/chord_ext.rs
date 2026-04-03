use dbflux_app::keymap::{KeyChord, Modifiers};
use gpui::Keystroke;

/// Creates a KeyChord from a GPUI Keystroke.
#[allow(dead_code)]
pub fn key_chord_from_gpui(keystroke: &Keystroke) -> KeyChord {
    KeyChord {
        key: keystroke.key.clone(),
        modifiers: modifiers_from_gpui(&keystroke.modifiers),
    }
}

/// Creates Modifiers from a GPUI Modifiers struct.
#[allow(dead_code)]
pub fn modifiers_from_gpui(mods: &gpui::Modifiers) -> Modifiers {
    Modifiers {
        ctrl: mods.control,
        alt: mods.alt,
        shift: mods.shift,
        platform: mods.platform,
    }
}
