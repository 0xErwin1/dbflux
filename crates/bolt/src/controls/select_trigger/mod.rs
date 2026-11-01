//! `SelectTrigger` — the closed field of a select, without the menu: a
//! product that opens its own popup (a searchable list, a menu of its own)
//! draws the same trigger a [`Dropdown`](crate::controls::Dropdown) draws.

use gpui::prelude::*;
use gpui::{App, ClickEvent, Div, ElementId, Hsla, Role, SharedString, Stateful, Window, div};
use gpui_component::{ActiveTheme, IconName};

use crate::icon::IconSource;
use crate::primitives::{Chamfer, ChamferRing, Icon};
use crate::selector::id_selector;
use crate::tokens::{ChamferCut, ChromeColors, Fields};

/// What the field of a select trigger draws.
pub(crate) struct SelectField {
    pub label: SharedString,
    pub leading_icon: Option<IconSource>,
    pub disabled: bool,
    /// The focus ring color, when the ring shows.
    pub ring: Option<Hsla>,
    /// Id of the hover state of the field shape.
    pub shape_id: ElementId,
}

/// Lays the select field into `trigger`: the cut-6 raised field with a 1 px
/// line, an optional muted leading icon, the truncated label and the
/// chevron. Shared by `Dropdown`'s standard trigger and [`SelectTrigger`],
/// so both draw the same pixels.
pub(crate) fn select_field(trigger: Stateful<Div>, field: SelectField, cx: &App) -> Stateful<Div> {
    let theme = cx.theme();
    let opacity = if field.disabled {
        Fields::DISABLED_OPACITY
    } else {
        1.0
    };

    let mut shape = Chamfer::new(ChamferCut::CONTROL)
        .fill(theme.secondary.opacity(opacity))
        .border(theme.border.opacity(opacity));

    if !field.disabled {
        shape = shape
            .fill_hover(theme.secondary_hover)
            .interactive(field.shape_id);
    }

    if let Some(color) = field.ring {
        shape = shape.ring(ChamferRing::focus(color));
    }

    let text_color = if field.disabled {
        theme.muted_foreground
    } else {
        theme.accent_foreground
    };

    trigger
        .relative()
        .h(Fields::HEIGHT)
        .gap(Fields::GAP)
        .px(Fields::PADDING_X)
        .font_family(crate::fonts::ui_family(cx))
        .text_size(Fields::TEXT)
        .text_color(text_color)
        .child(shape)
        .when_some(field.leading_icon, |trigger, icon| {
            trigger.child(
                Icon::new(icon)
                    .size(Fields::LEADING_ICON)
                    .color(theme.muted_foreground),
            )
        })
        .child(div().flex_1().truncate().child(field.label))
        .child(
            Icon::new(IconName::ChevronDown)
                .size(Fields::CHEVRON)
                .color(theme.muted_foreground),
        )
}

/// The trigger of a select, drawn exactly like a standard `Dropdown`
/// trigger, for a product that owns the popup it opens.
///
/// It is a button named by its label (or [`Self::aria_label`]); the click
/// handler is the owner's. [`Self::focus_ring`] draws the tint focus ring,
/// for an owner that tracks focus (or an open popup) itself.
#[derive(IntoElement)]
pub struct SelectTrigger {
    id: ElementId,
    label: SharedString,
    aria_label: Option<SharedString>,
    leading_icon: Option<IconSource>,
    disabled: bool,
    ring: Option<Hsla>,
    on_click: Option<Box<dyn Fn(&ClickEvent, &mut Window, &mut App) + 'static>>,
}

impl SelectTrigger {
    pub fn new(id: impl Into<ElementId>, label: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            aria_label: None,
            leading_icon: None,
            disabled: false,
            ring: None,
            on_click: None,
        }
    }

    /// Names the trigger for assistive technology instead of its label.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.aria_label = Some(label.into());
        self
    }

    pub fn leading_icon(mut self, icon: impl Into<IconSource>) -> Self {
        self.leading_icon = Some(icon.into());
        self
    }

    /// A disabled trigger fades the field and ignores clicks.
    pub fn disabled(mut self, disabled: bool) -> Self {
        self.disabled = disabled;
        self
    }

    /// Draws the focus ring: `Some(color)` in that color, `None` hides it.
    pub fn focus_ring(mut self, color: Option<Hsla>) -> Self {
        self.ring = color;
        self
    }

    /// Draws the focus ring in the tint while `focused`.
    pub fn focused(self, focused: bool, cx: &App) -> Self {
        let tint = ChromeColors::tint(cx.theme());
        self.focus_ring(focused.then_some(tint))
    }

    pub fn on_click(
        mut self,
        handler: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.on_click = Some(Box::new(handler));
        self
    }
}

impl RenderOnce for SelectTrigger {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let disabled = self.disabled;
        let trigger = div()
            .debug_selector(id_selector(&self.id))
            .id(self.id)
            .role(Role::Button)
            .aria_label(self.aria_label.unwrap_or_else(|| self.label.clone()))
            .when(disabled, |trigger| trigger.aria_disabled(true))
            .flex()
            .items_center()
            .w_full()
            .when(disabled, |trigger| trigger.cursor_not_allowed())
            .when(!disabled, |trigger| trigger.cursor_pointer());

        let trigger = select_field(
            trigger,
            SelectField {
                label: self.label,
                leading_icon: self.leading_icon,
                disabled,
                ring: self.ring,
                shape_id: "select-trigger-shape".into(),
            },
            cx,
        );

        match self.on_click {
            Some(handler) if !disabled => trigger.on_click(handler),
            _ => trigger,
        }
    }
}

#[cfg(test)]
mod tests;
