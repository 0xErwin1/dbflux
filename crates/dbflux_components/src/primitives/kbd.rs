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
            keys: vec![key.into()],
            tone: KbdTone::Default,
        }
    }

    /// One keycap per key, joined by a plus.
    pub fn chord(keys: impl IntoIterator<Item = impl Into<SharedString>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
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
    fn tone_is_stored() {
        assert_eq!(Kbd::new("↵").tone(KbdTone::OnFill).tone, KbdTone::OnFill);
    }
}
