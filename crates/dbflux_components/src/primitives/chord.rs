//! `Chord` — a multi-key shortcut display composed of `KbdBadge` parts.
//!
//! Each key part is rendered using the same visual style as `KbdBadge`,
//! separated by a `+` label in muted color.
//!
//! # Usage
//!
//! ```ignore
//! Chord::new(["⌘", "K"])
//! Chord::new(["Ctrl", "Shift", "P"])
//! ```

use gpui::prelude::*;
use gpui::{App, SharedString, Window, div, px};
use gpui_component::ActiveTheme;

use crate::primitives::KbdBadge;
use crate::tokens::FontSizes;

/// A multi-part keyboard shortcut rendered as a horizontal sequence of
/// [`KbdBadge`] items separated by a muted `+` separator.
#[derive(IntoElement)]
pub struct Chord {
    parts: Vec<SharedString>,
}

impl Chord {
    pub fn new(parts: impl IntoIterator<Item = impl Into<SharedString>>) -> Self {
        Self {
            parts: parts.into_iter().map(Into::into).collect(),
        }
    }
}

impl RenderOnce for Chord {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let separator_color = theme.muted_foreground;
        let part_count = self.parts.len();

        let mut row = div().flex().items_center().gap(px(2.0));

        for (idx, part) in self.parts.into_iter().enumerate() {
            row = row.child(KbdBadge::new(part));

            if idx + 1 < part_count {
                row = row.child(
                    div()
                        .text_size(FontSizes::XS)
                        .text_color(separator_color)
                        .child("+"),
                );
            }
        }

        row
    }
}
