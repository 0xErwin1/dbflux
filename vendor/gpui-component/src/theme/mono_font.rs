//! Keeps the theme's default monospace family on a font the machine has.
//!
//! GPUI panics on the first line it lays out in a family it cannot find, and
//! `Font::fallbacks` does not help there: it is a cascade list for missing
//! glyphs, consulted only after the family itself has loaded. The theme's
//! platform default is a bare name, so a machine without that font would crash
//! on its first code block. This probe swaps the default for an installed
//! alternative before any text is laid out.
//!
//! Only the platform default is probed. A family the application or a theme
//! file chose explicitly is used as-is: it is usually embedded through
//! `add_fonts`, and Windows lists families under their localized names, so an
//! English name for a CJK font would look missing when it is not.

use gpui::{App, SharedString};
use gpui_base::TypographyTokens;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// Monospace families tried, in order, when the platform default is missing.
const MONO_FONT_ALTERNATES: &[&str] = if cfg!(target_os = "macos") {
    &["Monaco", "Courier New"]
} else if cfg!(target_os = "windows") {
    &["Cascadia Mono", "Courier New"]
} else {
    &["Noto Sans Mono", "Liberation Mono", "Ubuntu Mono"]
};

/// The virtual family GPUI resolves on every platform; the last resort when
/// neither the default nor an alternate is installed.
const SYSTEM_UI_FONT: &str = ".SystemUIFont";

/// The monospace family the theme picks when none is configured.
pub(super) fn default_mono_font_family() -> SharedString {
    TypographyTokens::default().mono
}

/// Replaces the platform-default monospace family on the global theme with one
/// that is installed. A family chosen explicitly is left alone.
///
/// A platform may install its system fonts after the first window, so this runs
/// again when they arrive. The family it named itself is not treated as a choice
/// someone else made: a later pass revises it, and the theme keeps whatever the
/// last pass resolved.
pub(super) fn resolve_default_mono_font(cx: &mut App) {
    if !is_probe_choice(&cx.global::<super::Theme>().mono_font_family) {
        return;
    }
    let family = installed_default_mono_font_family(cx);
    cx.global_mut::<super::Theme>().mono_font_family = family.clone();
    *chosen() = Some(family);
}

/// The family this probe named on the theme, so a later pass can revise it.
static CHOSEN: Mutex<Option<SharedString>> = Mutex::new(None);

fn chosen() -> MutexGuard<'static, Option<SharedString>> {
    CHOSEN.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Whether `family` is one this probe chose, rather than one someone else set.
fn is_probe_choice(family: &SharedString) -> bool {
    family == &default_mono_font_family() || chosen().as_ref() == Some(family)
}

/// The installed stand-in for the platform default.
fn installed_default_mono_font_family(cx: &App) -> SharedString {
    let default = default_mono_font_family();
    let family = first_installed(&default, MONO_FONT_ALTERNATES, &installed_font_names(cx));
    if family != default {
        tracing::warn!("Monospace font {default:?} is not installed, using {family:?} instead.");
    }
    family
}

/// The families installed on the machine, listed once per process.
///
/// Enumerating fonts costs around a hundred milliseconds on macOS, and the
/// answer only changes while the process runs when the platform installs its
/// system fonts after the first window, which
/// [`forget_installed_font_names`] handles.
pub(super) fn installed_font_names(cx: &App) -> Arc<[String]> {
    if let Some(names) = names().clone() {
        return names;
    }
    let listed: Arc<[String]> = cx.text_system().all_font_names().into();
    *names() = Some(listed.clone());
    listed
}

/// Discards the listed families, so the next probe asks the text system again.
pub(super) fn forget_installed_font_names() {
    *names() = None;
}

/// The listed families, or `None` while they have not been asked for.
static INSTALLED_FONT_NAMES: Mutex<Option<Arc<[String]>>> = Mutex::new(None);

fn names() -> MutexGuard<'static, Option<Arc<[String]>>> {
    INSTALLED_FONT_NAMES
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// `default` when it is installed, else the first installed alternate, else
/// `.SystemUIFont`.
fn first_installed(default: &str, alternates: &[&str], installed: &[String]) -> SharedString {
    std::iter::once(default)
        .chain(alternates.iter().copied())
        .find(|candidate| installed.iter().any(|name| name == candidate))
        .unwrap_or(SYSTEM_UI_FONT)
        .to_string()
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn keeps_the_default_when_it_is_installed() {
        let installed = names(&["Monaco", "Menlo", ".SystemUIFont"]);
        assert_eq!(
            first_installed("Menlo", &["Monaco"], &installed),
            SharedString::from("Menlo")
        );
    }

    #[test]
    fn falls_through_the_alternates_in_order() {
        let installed = names(&["Courier New", "Monaco", ".SystemUIFont"]);
        assert_eq!(
            first_installed("Menlo", &["Monaco", "Courier New"], &installed),
            SharedString::from("Monaco")
        );
    }

    #[test]
    fn falls_back_to_the_system_font_when_nothing_is_installed() {
        let installed = names(&["Helvetica", ".SystemUIFont"]);
        assert_eq!(
            first_installed("Menlo", &["Monaco"], &installed),
            SharedString::from(".SystemUIFont")
        );
    }
}
