//! `SearchField` — a framed search input with a trailing match count, and
//! [`SearchTrigger`], its closed state.

use gpui::prelude::*;
use gpui::{
    AnyElement, App, ElementId, Entity, Focusable as _, Hsla, Rems, Role, SharedString, Stateful,
    Window, div,
};
use gpui_component::{ActiveTheme, Theme};

use super::{Input, InputState};
use crate::composites::ShellIcons;
use crate::icon::IconSource;
use crate::primitives::{BadgeTone, Chamfer, ChamferRing, Icon};
use crate::selector::id_selector;
use crate::tokens::{ChamferCut, ChromeColors, Fields};

/// The count at the end of a [`SearchField`]: the caller's words for where
/// the search stands (`"3 of 12"`, `"no matches"`, `"error"`) and the tone
/// they draw in.
pub struct SearchCount {
    label: SharedString,
    tone: BadgeTone,
}

impl SearchCount {
    /// A muted count reading `label`.
    pub fn new(label: impl Into<SharedString>) -> Self {
        Self {
            label: label.into(),
            tone: BadgeTone::Neutral,
        }
    }

    /// The count's color, the text color of a badge in `tone`. A danger
    /// count also rings the field in danger: the search itself failed.
    /// While the input holds focus a failed field draws its border in
    /// danger and keeps the tint focus ring.
    pub fn tone(mut self, tone: BadgeTone) -> Self {
        self.tone = tone;
        self
    }
}

/// A 30 px field, cut 6, on the ground with a line border: the search icon,
/// the frameless input, then the optional [`SearchCount`] and any trailing
/// controls (previous and next buttons). While the input holds focus the
/// field carries the tint focus ring. The count truncates past
/// `Fields::COUNT_MAX_WIDTH` and the input never shrinks below
/// `Fields::SEARCH_INPUT_MIN_WIDTH`.
///
/// The field records `id`, the input `<id>-input` and the count
/// `<id>-count`, a label node named by the count's text so a reader hears
/// it. The input announces [`SearchField::aria_label`]. Every word is the
/// caller's: the count is formatted by the product in its own language.
///
/// A search a screen keeps closed until the user asks for it draws
/// [`SearchTrigger`] instead: the same frame and metrics, opening this field.
///
/// ```ignore
/// SearchField::new("body-search", &input)
///     .aria_label(search_label)
///     .width(ResultMetrics::SEARCH_WIDTH)
///     .count(SearchCount::new(format!("{current} of {total}")))
///     .trailing(previous_button)
///     .trailing(next_button)
/// ```
#[derive(IntoElement)]
pub struct SearchField {
    id: SharedString,
    input: Entity<InputState>,
    icon: Option<IconSource>,
    width: Option<Rems>,
    aria_label: Option<SharedString>,
    count: Option<SearchCount>,
    trailing: Vec<AnyElement>,
}

impl SearchField {
    pub fn new(id: impl Into<SharedString>, input: &Entity<InputState>) -> Self {
        Self {
            id: id.into(),
            input: input.clone(),
            icon: None,
            width: None,
            aria_label: None,
            count: None,
            trailing: Vec::new(),
        }
    }

    /// The leading icon, in place of the installed shell search icon.
    pub fn icon(mut self, icon: impl Into<IconSource>) -> Self {
        self.icon = Some(icon.into());
        self
    }

    /// A fixed width; without one the field fills the space it is given.
    pub fn width(mut self, width: Rems) -> Self {
        self.width = Some(width);
        self
    }

    /// The name the input announces.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.aria_label = Some(label.into());
        self
    }

    /// The count drawn after the input; `None` draws none.
    pub fn count(mut self, count: impl Into<Option<SearchCount>>) -> Self {
        self.count = count.into();
        self
    }

    /// An element drawn after the count, in call order.
    pub fn trailing(mut self, element: impl IntoElement) -> Self {
        self.trailing.push(element.into_any_element());
        self
    }
}

impl RenderOnce for SearchField {
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme().clone();
        let focused = self
            .input
            .read(cx)
            .focus_handle(cx)
            .contains_focused(window, cx);
        let failed = self
            .count
            .as_ref()
            .is_some_and(|count| count.tone == BadgeTone::Danger);

        let (border, ring) = field_frame(failed, focused, &theme);
        let mut shape = field_shape(&theme, border);
        if let Some(ring) = ring {
            shape = shape.ring(ring);
        }

        let icon = self.icon.unwrap_or_else(|| ShellIcons::current(cx).search);
        let mut input = Input::new(&self.input)
            .id(ElementId::Name(format!("{}-input", self.id).into()))
            .small()
            .appearance(false);
        if let Some(label) = self.aria_label {
            input = input.aria_label(label);
        }

        let count = self.count.map(|count| {
            let count_id = ElementId::Name(format!("{}-count", self.id).into());

            div()
                .debug_selector(id_selector(&count_id))
                .id(count_id)
                .role(Role::Label)
                .aria_label(count.label.clone())
                .flex_none()
                .max_w(Fields::COUNT_MAX_WIDTH)
                .overflow_hidden()
                .whitespace_nowrap()
                .text_ellipsis()
                .font_family(crate::fonts::editor_family(cx))
                .text_size(Fields::COUNT_TEXT)
                .text_color(count.tone.text_color(&theme))
                .child(count.label)
        });

        let id = ElementId::Name(self.id);

        field_shell(id, self.width)
            .child(shape)
            .child(
                Icon::new(icon)
                    .size(Fields::LEADING_ICON)
                    .color(theme.muted_foreground),
            )
            .child(
                div()
                    .flex_1()
                    .min_w(Fields::SEARCH_INPUT_MIN_WIDTH)
                    .child(input),
            )
            .children(count)
            .children(self.trailing)
    }
}

/// The frame both a search field and its closed trigger draw: the ground fill
/// cut 6 behind `border`.
pub(super) fn field_shape(theme: &Theme, border: Hsla) -> Chamfer {
    Chamfer::new(ChamferCut::CONTROL)
        .fill(theme.background)
        .border(border)
}

/// The metrics both a search field and its closed trigger draw: `id` as the
/// field's element, `Fields::HEIGHT` tall with `Fields::PADDING_X` of inset,
/// its parts `Fields::GAP` apart, at `Fields::TEXT`. `width` fixes the field;
/// without one it fills the space it is given.
pub(super) fn field_shell(id: ElementId, width: Option<Rems>) -> Stateful<gpui::Div> {
    div()
        .debug_selector(id_selector(&id))
        .id(id)
        .relative()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(Fields::GAP)
        .h(Fields::HEIGHT)
        .px(Fields::PADDING_X)
        .text_size(Fields::TEXT)
        .map(|field| match width {
            Some(width) => field.w(width),
            None => field.flex_1().min_w_0(),
        })
}

/// The field's border and ring for its state.
fn field_frame(failed: bool, focused: bool, theme: &Theme) -> (Hsla, Option<ChamferRing>) {
    if failed && focused {
        // The danger moves to the border so the focus ring stays visible.
        (
            theme.danger,
            Some(ChamferRing::focus(ChromeColors::tint(theme))),
        )
    } else if failed {
        (theme.border, Some(ChamferRing::outline(theme.danger)))
    } else if focused {
        (
            theme.border,
            Some(ChamferRing::focus(ChromeColors::tint(theme))),
        )
    } else {
        (theme.border, None)
    }
}

mod trigger;

pub use trigger::SearchTrigger;

#[cfg(test)]
mod tests;
