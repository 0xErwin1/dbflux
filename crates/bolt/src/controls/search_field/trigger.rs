//! `SearchTrigger` — the closed state of a [`super::SearchField`].

use gpui::prelude::*;
use gpui::{App, ClickEvent, ElementId, Rems, Role, SharedString, Window, div};
use gpui_component::ActiveTheme;

use super::{field_shape, field_shell};
use crate::composites::ShellIcons;
use crate::icon::IconSource;
use crate::primitives::{Icon, Kbd};
use crate::selector::id_selector;
use crate::tokens::Fields;

/// The closed state of a search field: the same frame, leading icon and
/// metrics as [`SearchField`](super::SearchField), drawing the placeholder
/// and an optional shortcut keycap instead of an input. The host opens the
/// field with [`Self::on_open`]; nothing here holds focus or an input state.
///
/// The trigger records `id`, the placeholder `<id>-placeholder` and the
/// keycap `<id>-key`, and is a button named by [`Self::aria_label`] or by its
/// placeholder. Every word is the caller's. A field narrower than its parts
/// truncates the placeholder first, and a field too narrow for the icon and
/// the keycap clips them at its own edge instead of painting past it.
///
/// ```ignore
/// SearchTrigger::new("body-search", search_label)
///     .icon(search_icon)
///     .shortcut("Ctrl F")
///     .width(ResultMetrics::SEARCH_WIDTH)
///     .on_open(move |_, window, cx| open_search("body", window, cx))
/// ```
#[derive(IntoElement)]
pub struct SearchTrigger {
    id: SharedString,
    placeholder: SharedString,
    icon: Option<IconSource>,
    shortcut: Option<SharedString>,
    width: Option<Rems>,
    aria_label: Option<SharedString>,
    on_open: Option<Box<dyn Fn(&ClickEvent, &mut Window, &mut App) + 'static>>,
}

impl SearchTrigger {
    /// A closed field `id` drawing `placeholder`, the words the open field's
    /// input shows while it is empty.
    pub fn new(id: impl Into<SharedString>, placeholder: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            placeholder: placeholder.into(),
            icon: None,
            shortcut: None,
            width: None,
            aria_label: None,
            on_open: None,
        }
    }

    /// The leading icon, in place of the installed shell search icon.
    pub fn icon(mut self, icon: impl Into<IconSource>) -> Self {
        self.icon = Some(icon.into());
        self
    }

    /// The chord the host binds to open the field ("Ctrl F"), drawn as one
    /// keycap at the end of the field.
    pub fn shortcut(mut self, keys: impl Into<SharedString>) -> Self {
        self.shortcut = Some(keys.into());
        self
    }

    /// A fixed width; without one the field fills the space it is given.
    pub fn width(mut self, width: Rems) -> Self {
        self.width = Some(width);
        self
    }

    /// The name the trigger announces, in place of its placeholder.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.aria_label = Some(label.into());
        self
    }

    /// Runs when the trigger is clicked: the host opens the field.
    pub fn on_open(
        mut self,
        handler: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.on_open = Some(Box::new(handler));
        self
    }
}

impl RenderOnce for SearchTrigger {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme().clone();
        let name = self.aria_label.unwrap_or_else(|| self.placeholder.clone());
        let icon = self.icon.unwrap_or_else(|| ShellIcons::current(cx).search);
        let placeholder_id = ElementId::Name(format!("{}-placeholder", self.id).into());
        let key_id = ElementId::Name(format!("{}-key", self.id).into());
        let field_id = ElementId::Name(self.id.clone());

        let field = field_shell(field_id, self.width)
            .font_family(crate::fonts::ui_family(cx))
            // A field narrower than its parts clips them instead of painting
            // the keycap past its own edge.
            .overflow_hidden()
            .role(Role::Button)
            .aria_label(name)
            .cursor_pointer()
            .child(field_shape(&theme, theme.border))
            .child(
                div().flex_none().child(
                    Icon::new(icon)
                        .size(Fields::LEADING_ICON)
                        .color(theme.muted_foreground),
                ),
            )
            .child(
                div()
                    .debug_selector(id_selector(&placeholder_id))
                    .id(placeholder_id)
                    .role(Role::Label)
                    .aria_label(self.placeholder.clone())
                    .flex_1()
                    .min_w_0()
                    .overflow_hidden()
                    .whitespace_nowrap()
                    .text_ellipsis()
                    .text_color(theme.muted_foreground)
                    .child(self.placeholder),
            )
            .when_some(self.shortcut, |field, shortcut| {
                // The keycap keeps its size; the placeholder truncates first.
                field.child(
                    div()
                        .flex_none()
                        .child(Kbd::new(shortcut.clone()).id(key_id).aria_label(shortcut)),
                )
            });

        match self.on_open {
            Some(on_open) => field.on_click(on_open),
            None => field,
        }
    }
}
