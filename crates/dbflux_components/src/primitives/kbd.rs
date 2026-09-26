use gpui::prelude::*;
use gpui::{App, FontWeight, SharedString, Window, div};
use gpui_component::ActiveTheme;

use crate::primitives::Chamfer;
use crate::tokens::{Borders, ChamferCut, KbdMetrics};
use crate::typography::AppFonts;

/// Surface a keycap sits on, which decides how it is drawn.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum KbdTone {
    /// Raised keycap with a line-2 bottom edge, muted text, cut 4.
    #[default]
    Default,
    /// Translucent white keycap without a cut or edge, inheriting the text
    /// color of the filled button it sits on.
    OnFill,
}

/// Modifier names a key label can start with, compared case-insensitively.
const MODIFIER_NAMES: [&str; 8] = [
    "ctrl", "control", "alt", "option", "shift", "cmd", "super", "meta",
];

fn is_modifier(word: &str) -> bool {
    MODIFIER_NAMES
        .iter()
        .any(|modifier| modifier.eq_ignore_ascii_case(word))
}

/// `word` uppercased when it is a single lowercase ASCII letter.
fn uppercase_lone_letter(word: &str) -> String {
    let is_lone_letter = word.len() == 1 && word.chars().all(|c| c.is_ascii_lowercase());

    if is_lone_letter {
        word.to_ascii_uppercase()
    } else {
        word.to_string()
    }
}

/// The display form of a key label: a letter pressed together with a
/// modifier reads uppercase (`Ctrl c` and `ctrl+c` become `Ctrl C` and
/// `ctrl+C`), the way the keycaps are printed. A bare letter keeps its case,
/// because `r` and `R` (Shift R) are different keys.
pub fn key_label(label: &str) -> SharedString {
    let mut output = String::with_capacity(label.len());
    let mut word = String::new();
    let mut after_modifier = false;

    for character in label.chars().chain(std::iter::once(' ')) {
        let is_separator = character == ' ' || character == '+';

        if !is_separator || (word.is_empty() && character == '+') {
            word.push(character);
            continue;
        }

        if word.is_empty() {
            after_modifier = false;
        } else {
            if after_modifier {
                output.push_str(&uppercase_lone_letter(&word));
            } else {
                output.push_str(&word);
            }

            after_modifier = is_modifier(&word);
            word.clear();
        }

        output.push(character);
    }

    output.pop();
    output.into()
}

/// Keyboard shortcut: one keycap per key, keys of a chord joined by a thin
/// plus. JetBrains Mono, cut 4, bottom edge.
///
/// ```ignore
/// Kbd::new("Esc")
/// Kbd::new("Ctrl ↵")
/// Kbd::chord(["Ctrl", "Shift", "P"])
/// ```
#[derive(IntoElement)]
pub struct Kbd {
    keys: Vec<SharedString>,
    tone: KbdTone,
}

impl Kbd {
    /// A single keycap.
    pub fn new(key: impl Into<SharedString>) -> Self {
        Self {
            keys: vec![key_label(&key.into())],
            tone: KbdTone::Default,
        }
    }

    /// One keycap per key, joined by a plus.
    pub fn chord(keys: impl IntoIterator<Item = impl Into<SharedString>>) -> Self {
        let mut after_modifier = false;

        let keys = keys
            .into_iter()
            .map(|key| {
                let key: SharedString = key.into();

                let label = if after_modifier {
                    uppercase_lone_letter(&key).into()
                } else {
                    key_label(&key)
                };

                after_modifier = is_modifier(&key);
                label
            })
            .collect();

        Self {
            keys,
            tone: KbdTone::Default,
        }
    }

    pub fn tone(mut self, tone: KbdTone) -> Self {
        self.tone = tone;
        self
    }

    pub fn keys(&self) -> &[SharedString] {
        &self.keys
    }

    fn keycap(key: SharedString, tone: KbdTone, cx: &App) -> gpui::Div {
        let theme = cx.theme();

        let keycap = div()
            .relative()
            .flex()
            .items_center()
            .flex_shrink_0()
            .font_family(AppFonts::MONO)
            .text_size(KbdMetrics::FONT)
            .line_height(KbdMetrics::LINE_HEIGHT)
            .font_weight(FontWeight::MEDIUM);

        match tone {
            KbdTone::Default => keycap
                .px(KbdMetrics::PADDING_X)
                .py(KbdMetrics::PADDING_Y)
                .text_color(theme.muted_foreground)
                .child(
                    Chamfer::new(ChamferCut::KEYCAP)
                        .fill(theme.secondary)
                        .bottom_edge(theme.input, Borders::THIN),
                )
                .child(key),
            KbdTone::OnFill => keycap
                .px(KbdMetrics::ON_FILL_PADDING_X)
                .py(KbdMetrics::ON_FILL_PADDING_Y)
                .bg(gpui::white().opacity(KbdMetrics::ON_FILL_ALPHA))
                .child(key),
        }
    }
}

impl RenderOnce for Kbd {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let separator_color = cx.theme().muted_foreground;
        let key_count = self.keys.len();
        let tone = self.tone;

        let mut row = div().flex().items_center().gap(KbdMetrics::CHORD_GAP);

        for (index, key) in self.keys.into_iter().enumerate() {
            row = row.child(Self::keycap(key, tone, cx));

            if index + 1 < key_count {
                row = row.child(
                    div()
                        .font_family(AppFonts::MONO)
                        .text_size(KbdMetrics::FONT)
                        .font_weight(FontWeight::LIGHT)
                        .text_color(separator_color)
                        .child("+"),
                );
            }
        }

        row
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_one_keycap_and_chord_keeps_key_order() {
        assert_eq!(Kbd::new("Ctrl ↵").keys(), ["Ctrl ↵"]);

        let chord = Kbd::chord(["Ctrl", "Shift", "P"]);
        assert_eq!(chord.keys(), ["Ctrl", "Shift", "P"]);
        assert_eq!(chord.tone, KbdTone::Default);
    }

    #[test]
    fn letters_pressed_with_a_modifier_read_uppercase() {
        assert_eq!(key_label("Ctrl c"), "Ctrl C");
        assert_eq!(key_label("Ctrl+c"), "Ctrl+C");
        assert_eq!(key_label("Ctrl Shift p"), "Ctrl Shift P");
        assert_eq!(key_label("Ctrl k  Ctrl s"), "Ctrl K  Ctrl S");
        assert_eq!(Kbd::new("Ctrl n").keys(), ["Ctrl N"]);
        assert_eq!(Kbd::chord(["Ctrl", "e"]).keys(), ["Ctrl", "E"]);
    }

    #[test]
    fn bare_keys_and_named_keys_keep_their_text() {
        assert_eq!(key_label("r"), "r");
        assert_eq!(key_label("x"), "x");
        assert_eq!(key_label("Ctrl ↵"), "Ctrl ↵");
        assert_eq!(key_label("Esc"), "Esc");
        assert_eq!(key_label("Shift R"), "Shift R");
        assert_eq!(key_label("+"), "+");
        assert_eq!(key_label("Ctrl +"), "Ctrl +");
        assert_eq!(Kbd::chord(["g", "g"]).keys(), ["g", "g"]);
    }

    #[test]
    fn tone_is_stored() {
        assert_eq!(Kbd::new("↵").tone(KbdTone::OnFill).tone, KbdTone::OnFill);
    }
}
