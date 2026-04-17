use gpui::prelude::*;
use gpui::{div, App, SharedString, Window};
use gpui_component::ActiveTheme;
use gpui_component::IconName;

use crate::icon::IconSource;
use crate::primitives::Icon;
use crate::tokens::{FontSizes, Heights, Spacing};

/// A section with a clickable header that toggles visibility of its content.
///
/// Use [`CollapsibleSection::new`] to create the struct, then mount the entity
/// in your parent component. Toggle state is managed internally.
pub struct CollapsibleSection {
    title: SharedString,
    collapsed: bool,
    children: Vec<gpui::AnyElement>,
}

impl CollapsibleSection {
    pub fn new(title: impl Into<SharedString>, _cx: &mut App) -> Self {
        Self {
            title: title.into(),
            collapsed: false,
            children: Vec::new(),
        }
    }

    /// Set the initial collapsed state.
    pub fn collapsed(mut self, collapsed: bool) -> Self {
        self.collapsed = collapsed;
        self
    }

    /// Add a child element to the collapsible content area.
    pub fn child(mut self, child: impl IntoElement) -> Self {
        self.children.push(child.into_any_element());
        self
    }

    fn toggle(&mut self, cx: &mut gpui::Context<Self>) {
        self.collapsed = !self.collapsed;
        cx.notify();
    }
}

impl Render for CollapsibleSection {
    fn render(&mut self, _window: &mut Window, cx: &mut gpui::Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let title = self.title.clone();
        let collapsed = self.collapsed;

        let chevron = if collapsed {
            IconName::ChevronRight
        } else {
            IconName::ChevronDown
        };

        let header = div()
            .id("collapsible-header")
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .h(Heights::HEADER)
            .px(Spacing::SM)
            .bg(theme.secondary)
            .border_b_1()
            .border_color(theme.border)
            .cursor_pointer()
            .hover(|s| s.bg(theme.accent))
            .on_click(cx.listener(|this, _, _, cx| {
                this.toggle(cx);
            }))
            .child(
                Icon::new(IconSource::Named(chevron))
                    .size(Heights::ICON_SM)
                    .color(theme.muted_foreground),
            )
            .child(
                div()
                    .text_size(FontSizes::SM)
                    .font_weight(gpui::FontWeight::MEDIUM)
                    .text_color(theme.foreground)
                    .child(title),
            );

        let mut section = div().flex().flex_col().child(header);

        if !collapsed {
            for child in self.children.drain(..) {
                section = section.child(child);
            }
        }

        section
    }
}
