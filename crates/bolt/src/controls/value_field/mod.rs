//! `ValueField` — a value drawn like a text field, without a field state.

use gpui::prelude::*;
use gpui::{App, Div, ElementId, Hsla, Role, SharedString, Window, div};
use gpui_component::ActiveTheme;

use crate::primitives::Chamfer;
use crate::selector::id_selector;
use crate::tokens::{ChamferCut, Fields};

/// A value drawn in the frame of a text field: the cut-6 ground fill behind a
/// line, [`Fields::HEIGHT`] tall with [`Fields::PADDING_X`] of inset, the value
/// truncated to one line, filling the width it is given.
///
/// It holds the text, never an `Entity<InputState>`, so a page that shows many
/// effective values — a setting a profile resolves to, a computed path — draws
/// each one without a field state per value. The value is normalized when the
/// field is built — [`Self::new`] turns a `\r` or `\n` into a space — so it is
/// always one line and cannot draw past the field. [`Self::mono`] draws the value in
/// the editor face, for a value as it is written in a file.
///
/// With [`Self::id`] the value is a label node `<id>-value` named by its text,
/// so a test and a reader address it; without one the field draws no
/// accessibility node at all.
///
/// ```ignore
/// ValueField::new(effective_proxy_url)
///     .id("network-proxy-url")
///     .mono(true)
/// ```
#[derive(IntoElement)]
pub struct ValueField {
    id: Option<ElementId>,
    value: SharedString,
    mono: bool,
    text_color: Option<Hsla>,
}

impl ValueField {
    /// A field drawing `value`. The value is normalized once, here: a `\r` or
    /// `\n` reads as a space, so the field draws one line whatever the caller
    /// passes and the render makes no second copy of it.
    pub fn new(value: impl Into<SharedString>) -> Self {
        let value: SharedString = value.into();

        Self {
            id: None,
            value: one_line(&value),
            mono: false,
            text_color: None,
        }
    }

    /// Records `id` as the field's element id; the value is `<id>-value`.
    pub fn id(mut self, id: impl Into<ElementId>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Draws the value in the editor face instead of the interface face.
    pub fn mono(mut self, mono: bool) -> Self {
        self.mono = mono;
        self
    }

    /// Draws the value in `color` instead of the strong value color.
    pub fn text_color(mut self, color: Hsla) -> Self {
        self.text_color = Some(color);
        self
    }
}

impl RenderOnce for ValueField {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme().clone();
        let value = div()
            .flex_1()
            .min_w_0()
            .truncate()
            .child(self.value.clone());
        let value = match &self.id {
            Some(id) => {
                let value_id = ElementId::Name(format!("{id}-value").into());

                value
                    .debug_selector(id_selector(&value_id))
                    .id(value_id)
                    .role(Role::Label)
                    .aria_label(self.value.clone())
                    .into_any_element()
            }
            None => value.into_any_element(),
        };

        let field = value_field_base(cx)
            .w_full()
            .font_family(value_font(self.mono, cx))
            .text_color(value_color(&theme, self.text_color))
            .child(value);

        match self.id {
            Some(id) => field
                .debug_selector(id_selector(&id))
                .id(id)
                .into_any_element(),
            None => field.into_any_element(),
        }
    }
}

/// The frame of a value drawn like a text field — the ground fill cut 6 behind
/// a line, `Fields::HEIGHT` tall with `Fields::PADDING_X` of inset — held open
/// as a relative row of its parts. Shared by [`ValueField`] and
/// [`modal_value_field`](crate::modals::modal_value_field), so a read-only value
/// draws the same field on a page and in a dialog.
pub(crate) fn value_field_base(cx: &App) -> Div {
    let theme = cx.theme();

    div()
        .relative()
        .flex()
        .items_center()
        .gap(Fields::GAP)
        .h(Fields::HEIGHT)
        .px(Fields::PADDING_X)
        .text_size(Fields::TEXT)
        .child(
            Chamfer::new(ChamferCut::CONTROL)
                .fill(theme.background)
                .border(theme.border),
        )
}

/// A value as one line: a break of any kind reads as a space, so a value
/// carrying one cannot draw past a field's height. Shared by [`ValueField`]
/// and [`modal_value_field`](crate::modals::modal_value_field).
pub(crate) fn one_line(value: &str) -> SharedString {
    if !value.contains(['\r', '\n']) {
        return value.to_owned().into();
    }

    value.replace("\r\n", " ").replace(['\r', '\n'], " ").into()
}

/// The face a value draws in: the caller's, or the interface face.
fn value_font(mono: bool, cx: &App) -> SharedString {
    if mono {
        crate::fonts::editor_family(cx)
    } else {
        crate::fonts::ui_family(cx)
    }
}

/// The color a value draws in: the caller's, or the strong value color.
fn value_color(theme: &gpui_component::Theme, text_color: Option<Hsla>) -> Hsla {
    text_color.unwrap_or_else(|| crate::tokens::ChromeColors::strong(theme))
}

#[cfg(test)]
mod tests;
