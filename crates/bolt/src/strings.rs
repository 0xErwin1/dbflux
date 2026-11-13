//! The text seam: how a product supplies the words Bolt's surfaces draw.
//!
//! Bolt never embeds a sentence or names a translation catalogue: a surface
//! asks for the text of a key, and the product answers it with a translator
//! installed once at startup — [`Strings`], a global holding two functions,
//! the shape the dropdown's one-off `DropdownStrings` used, generalised to
//! every surface. [`text`] answers a key; [`text_with`] answers a key with
//! named arguments, for the catalogues whose translations interpolate
//! (`%{name}` placeholders).
//!
//! A surface rendered before the product installs a translator draws no
//! text: the lookups return an empty string, the way the dropdown's
//! placeholder already behaved without its one-off installed.

use gpui::{App, Global, SharedString};

/// The product-installed translator.
///
/// Both functions read the product's active locale at call time, so a
/// locale change is picked up by the next render.
pub struct Strings {
    /// The text of `key`.
    pub text: fn(key: &str) -> SharedString,
    /// The text of `key` with each `%{name}` placeholder replaced by the
    /// value of the matching argument.
    pub text_with: fn(key: &str, args: &[(&str, &str)]) -> SharedString,
}

impl Global for Strings {}

/// Installs the product's translator. Call once at startup, before the first
/// window; installing again replaces the value.
pub fn install(cx: &mut App, strings: Strings) {
    cx.set_global(strings);
}

/// The text of `key`, or an empty string when no translator is installed.
pub fn text(cx: &App, key: &str) -> SharedString {
    match cx.try_global::<Strings>() {
        Some(strings) => (strings.text)(key),
        None => SharedString::from(""),
    }
}

/// The text of `key` with its `%{name}` placeholders replaced by the matching
/// argument values, or an empty string when no translator is installed.
pub fn text_with(cx: &App, key: &str, args: &[(&str, &str)]) -> SharedString {
    match cx.try_global::<Strings>() {
        Some(strings) => (strings.text_with)(key, args),
        None => SharedString::from(""),
    }
}

#[cfg(test)]
mod tests {
    use gpui::TestAppContext;

    use super::{Strings, install, text, text_with};

    /// A translator the tests read: the key back as text, with arguments
    /// appended so the interpolation path is observable.
    fn test_translator() -> Strings {
        Strings {
            text: |key| key.to_string().into(),
            text_with: |key, args| {
                let mut text = key.to_string();
                for (name, value) in args {
                    text.push_str(&format!(" {name}={value}"));
                }
                text.into()
            },
        }
    }

    #[gpui::test]
    fn an_app_without_a_translator_resolves_no_text(cx: &mut TestAppContext) {
        cx.update(|cx| {
            assert_eq!(text(cx, "palette.empty"), "");
            assert_eq!(text_with(cx, "palette.count", &[("n", "1")]), "");
        });
    }

    #[gpui::test]
    fn the_installed_translator_answers_every_lookup(cx: &mut TestAppContext) {
        cx.update(|cx| install(cx, test_translator()));
        cx.update(|cx| {
            assert_eq!(text(cx, "palette.empty"), "palette.empty");
            assert_eq!(
                text_with(cx, "palette.count", &[("n", "3")]),
                "palette.count n=3"
            );
        });
    }

    #[gpui::test]
    fn installing_again_replaces_the_translator(cx: &mut TestAppContext) {
        cx.update(|cx| install(cx, test_translator()));
        cx.update(|cx| {
            install(
                cx,
                Strings {
                    text: |_| "replaced".into(),
                    text_with: |_, _| "replaced".into(),
                },
            )
        });
        cx.update(|cx| assert_eq!(text(cx, "anything"), "replaced"));
    }
}
