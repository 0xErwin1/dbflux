use std::sync::Arc;

use crate::composites::control_shell_with_padding;
use crate::primitives::focus_frame;
use crate::tokens::{FontSizes, Heights, Radii, Spacing};
use gpui::prelude::*;
use gpui::{
    ClickEvent, Context, Corner, ElementId, EventEmitter, Hsla, InteractiveElement, IntoElement,
    MouseButton, ParentElement, Render, ScrollHandle, ScrollWheelEvent, SharedString,
    StatefulInteractiveElement, Styled, Window, anchored, deferred, div, point, px,
};
use gpui_component::ActiveTheme;

#[derive(Clone, Debug)]
pub struct DropdownSelectionChanged {
    pub index: usize,
    #[allow(dead_code)]
    pub item: DropdownItem,
}

#[derive(Clone, Debug)]
pub struct DropdownDismissed;

#[derive(Clone, Debug)]
pub struct DropdownItem {
    pub label: SharedString,
    #[allow(dead_code)]
    pub value: SharedString,
}

impl DropdownItem {
    #[allow(dead_code)]
    pub fn new(label: impl Into<SharedString>) -> Self {
        let label = label.into();
        Self {
            value: label.clone(),
            label,
        }
    }

    pub fn with_value(label: impl Into<SharedString>, value: impl Into<SharedString>) -> Self {
        Self {
            label: label.into(),
            value: value.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DropdownTriggerVariant {
    Standard,
    Toolbar,
    Compact,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DropdownTriggerRenderPlan {
    uses_control_shell: bool,
    uses_internal_focus_frame: bool,
    reserves_legacy_focus_gutter: bool,
}

fn dropdown_trigger_render_plan(variant: DropdownTriggerVariant) -> DropdownTriggerRenderPlan {
    match variant {
        DropdownTriggerVariant::Standard => DropdownTriggerRenderPlan {
            uses_control_shell: true,
            uses_internal_focus_frame: true,
            reserves_legacy_focus_gutter: true,
        },
        DropdownTriggerVariant::Toolbar | DropdownTriggerVariant::Compact => {
            DropdownTriggerRenderPlan {
                uses_control_shell: false,
                uses_internal_focus_frame: false,
                reserves_legacy_focus_gutter: false,
            }
        }
    }
}

fn dropdown_focus_ring_state(
    current_color: Option<Hsla>,
    requested_color: Option<Hsla>,
) -> (Option<Hsla>, bool) {
    match requested_color {
        Some(color) => (Some(color), true),
        None => (current_color, false),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DropdownSelectionTransition {
    selected_index: usize,
    open: bool,
    highlighted_index: Option<usize>,
}

fn dropdown_selection_transition(
    disabled: bool,
    items_len: usize,
    index: usize,
) -> Option<DropdownSelectionTransition> {
    if disabled || index >= items_len {
        return None;
    }

    Some(DropdownSelectionTransition {
        selected_index: index,
        open: false,
        highlighted_index: None,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DropdownDismissTransition {
    open: bool,
    highlighted_index: Option<usize>,
}

fn dropdown_dismiss_transition(open: bool) -> Option<DropdownDismissTransition> {
    open.then_some(DropdownDismissTransition {
        open: false,
        highlighted_index: None,
    })
}

#[allow(clippy::type_complexity)]
pub struct Dropdown {
    id: ElementId,
    items: Vec<DropdownItem>,
    open: bool,
    selected_index: Option<usize>,
    highlighted_index: Option<usize>,
    disabled: bool,
    placeholder: SharedString,
    focus_ring_color: Option<Hsla>,
    focus_ring_visible: bool,
    compact_trigger: bool,
    toolbar_style: bool,
    menu_scroll_handle: ScrollHandle,
    on_select: Option<Arc<dyn Fn(usize, &DropdownItem, &mut Context<Self>) + Send + Sync>>,
}

#[allow(dead_code)]
impl Dropdown {
    const PAGE_STEP: usize = 8;

    pub fn new(id: impl Into<ElementId>) -> Self {
        Self {
            id: id.into(),
            items: Vec::new(),
            open: false,
            selected_index: None,
            highlighted_index: None,
            disabled: false,
            placeholder: "Select".into(),
            focus_ring_color: None,
            focus_ring_visible: false,
            compact_trigger: false,
            toolbar_style: false,
            menu_scroll_handle: ScrollHandle::new(),
            on_select: None,
        }
    }

    pub fn items(mut self, items: Vec<DropdownItem>) -> Self {
        self.items = items;
        self
    }

    pub fn placeholder(mut self, placeholder: impl Into<SharedString>) -> Self {
        self.placeholder = placeholder.into();
        self
    }

    pub fn selected_index(mut self, index: Option<usize>) -> Self {
        self.selected_index = index;
        self
    }

    pub fn disabled(mut self, disabled: bool) -> Self {
        self.disabled = disabled;
        self
    }

    #[allow(clippy::type_complexity)]
    pub fn on_select(
        mut self,
        handler: Arc<dyn Fn(usize, &DropdownItem, &mut Context<Self>) + Send + Sync>,
    ) -> Self {
        self.on_select = Some(handler);
        self
    }

    pub fn selected_label(&self) -> Option<SharedString> {
        self.selected_index
            .and_then(|index| self.items.get(index).map(|item| item.label.clone()))
    }

    pub fn selected_value(&self) -> Option<SharedString> {
        self.selected_index
            .and_then(|index| self.items.get(index).map(|item| item.value.clone()))
    }

    pub fn is_open(&self) -> bool {
        self.open
    }

    pub fn set_selected_index(&mut self, index: Option<usize>, cx: &mut Context<Self>) {
        if let Some(idx) = index {
            if idx < self.items.len() {
                self.selected_index = Some(idx);
                if self.open {
                    self.menu_scroll_handle.scroll_to_item(idx);
                }
                cx.notify();
            }
        } else {
            self.selected_index = None;
            cx.notify();
        }
    }

    pub fn set_items(&mut self, items: Vec<DropdownItem>, cx: &mut Context<Self>) {
        self.items = items;
        if let Some(selected) = self.selected_index
            && selected >= self.items.len()
        {
            self.selected_index = None;
        }
        cx.notify();
    }

    pub fn set_focus_ring(&mut self, color: Option<Hsla>, cx: &mut Context<Self>) {
        let (focus_ring_color, focus_ring_visible) =
            dropdown_focus_ring_state(self.focus_ring_color, color);

        self.focus_ring_color = focus_ring_color;
        self.focus_ring_visible = focus_ring_visible;

        cx.notify();
    }

    pub fn compact_trigger(mut self, compact: bool) -> Self {
        self.compact_trigger = compact;
        self
    }

    pub fn focus_ring_color(mut self, color: Option<Hsla>) -> Self {
        self.focus_ring_color = color;
        self
    }

    pub fn toolbar_style(mut self, toolbar: bool) -> Self {
        self.toolbar_style = toolbar;
        self
    }

    fn trigger_variant(&self) -> DropdownTriggerVariant {
        if self.compact_trigger {
            DropdownTriggerVariant::Compact
        } else if self.toolbar_style {
            DropdownTriggerVariant::Toolbar
        } else {
            DropdownTriggerVariant::Standard
        }
    }

    pub fn toggle_open(&mut self, cx: &mut Context<Self>) {
        if self.disabled || self.items.is_empty() {
            return;
        }
        self.open = !self.open;
        if self.open {
            self.highlighted_index = self.selected_index.or(Some(0));

            if let Some(index) = self.highlighted_index {
                self.menu_scroll_handle.scroll_to_item(index);
            }
        }
        cx.notify();
    }

    pub fn open(&mut self, cx: &mut Context<Self>) {
        if !self.disabled && !self.items.is_empty() {
            self.open = true;
            self.highlighted_index = self.selected_index.or(Some(0));

            if let Some(index) = self.highlighted_index {
                self.menu_scroll_handle.scroll_to_item(index);
            }

            cx.notify();
        }
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.open = false;
        self.highlighted_index = None;
        cx.notify();
    }

    pub fn select_next_item(&mut self, cx: &mut Context<Self>) {
        if self.items.is_empty() {
            return;
        }
        let next_index = self
            .highlighted_index
            .map(|i| (i + 1) % self.items.len())
            .unwrap_or(0);
        self.highlighted_index = Some(next_index);
        if self.open {
            self.menu_scroll_handle.scroll_to_item(next_index);
        }
        cx.notify();
    }

    pub fn select_prev_item(&mut self, cx: &mut Context<Self>) {
        if self.items.is_empty() {
            return;
        }
        let prev_index = self
            .highlighted_index
            .map(|i| if i == 0 { self.items.len() - 1 } else { i - 1 })
            .unwrap_or(self.items.len() - 1);
        self.highlighted_index = Some(prev_index);
        if self.open {
            self.menu_scroll_handle.scroll_to_item(prev_index);
        }
        cx.notify();
    }

    pub fn select_next_page(&mut self, cx: &mut Context<Self>) {
        if self.items.is_empty() {
            return;
        }

        let current = self.highlighted_index.unwrap_or(0);
        let next_index = (current + Self::PAGE_STEP).min(self.items.len() - 1);
        self.highlighted_index = Some(next_index);

        if self.open {
            self.menu_scroll_handle.scroll_to_item(next_index);
        }

        cx.notify();
    }

    pub fn select_prev_page(&mut self, cx: &mut Context<Self>) {
        if self.items.is_empty() {
            return;
        }

        let current = self.highlighted_index.unwrap_or(0);
        let prev_index = current.saturating_sub(Self::PAGE_STEP);
        self.highlighted_index = Some(prev_index);

        if self.open {
            self.menu_scroll_handle.scroll_to_item(prev_index);
        }

        cx.notify();
    }

    fn handle_menu_scroll_wheel(
        &mut self,
        event: &ScrollWheelEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let delta = event.delta.pixel_delta(px(1.0));
        if delta.y < px(0.0) {
            self.select_next_item(cx);
        } else if delta.y > px(0.0) {
            self.select_prev_item(cx);
        }
    }

    pub fn accept_selection(&mut self, cx: &mut Context<Self>) {
        if let Some(index) = self.highlighted_index {
            self.select_item(index, cx);
        }
    }

    fn select_item(&mut self, index: usize, cx: &mut Context<Self>) {
        let Some(transition) =
            dropdown_selection_transition(self.disabled, self.items.len(), index)
        else {
            return;
        };

        let item = match self.items.get(transition.selected_index) {
            Some(item) => item.clone(),
            None => return,
        };

        self.selected_index = Some(transition.selected_index);
        self.highlighted_index = transition.highlighted_index;
        self.open = transition.open;

        if let Some(on_select) = self.on_select.clone() {
            on_select(transition.selected_index, &item, cx);
        }

        cx.emit(DropdownSelectionChanged {
            index: transition.selected_index,
            item: item.clone(),
        });
        cx.notify();
    }

    fn handle_trigger_click(
        &mut self,
        _event: &ClickEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.toggle_open(cx);
    }

    fn handle_mouse_down_out(
        &mut self,
        _event: &gpui::MouseDownEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(transition) = dropdown_dismiss_transition(self.open) else {
            return;
        };

        self.open = transition.open;
        self.highlighted_index = transition.highlighted_index;
        cx.emit(DropdownDismissed);
        cx.notify();
    }

    fn render_menu(&self, cx: &Context<Self>) -> gpui::AnyElement {
        if !self.open || self.items.is_empty() {
            return div().into_any_element();
        }

        let theme = cx.theme();
        let is_disabled = self.disabled;

        let items: Vec<gpui::AnyElement> = self
            .items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let is_highlighted = self.highlighted_index == Some(index);
                let mut row = div()
                    .id(index)
                    .w_full()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .text_size(FontSizes::SM)
                    .whitespace_nowrap()
                    .text_color(theme.foreground)
                    .when(is_highlighted, |el| {
                        el.bg(theme.accent).text_color(theme.accent_foreground)
                    })
                    .when(!is_highlighted && !is_disabled, |el| {
                        el.hover(|s| s.bg(theme.list_active))
                    })
                    .child(item.label.clone());

                if is_disabled {
                    row = row.text_color(theme.muted_foreground).cursor_not_allowed();
                } else {
                    row = row.cursor_pointer().on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _event, _window, cx| {
                            this.select_item(index, cx);
                        }),
                    );
                }

                row.into_any_element()
            })
            .collect();

        let menu = div()
            .id("dropdown-menu")
            .min_w_full()
            .max_h(px(220.0))
            .p(Spacing::XS)
            .border_1()
            .border_color(theme.border)
            .bg(theme.popover)
            .rounded(Radii::MD)
            .overflow_scroll()
            .track_scroll(&self.menu_scroll_handle)
            .on_scroll_wheel(cx.listener(Self::handle_menu_scroll_wheel))
            .shadow_lg()
            .occlude()
            .children(items);

        deferred(
            anchored()
                .anchor(Corner::TopLeft)
                .offset(point(px(0.0), px(4.0)))
                .snap_to_window()
                .child(menu),
        )
        .with_priority(1)
        .into_any_element()
    }

    fn render_trigger(
        &self,
        label: SharedString,
        disabled: bool,
        cx: &mut Context<Self>,
    ) -> gpui::AnyElement {
        let theme = cx.theme();
        let variant = self.trigger_variant();
        let render_plan = dropdown_trigger_render_plan(variant);

        let mut trigger = div()
            .id("dropdown-trigger")
            .h(Heights::BUTTON)
            .flex()
            .items_center()
            .w_full()
            .when(disabled, |el| {
                el.text_color(theme.muted_foreground)
                    .cursor_not_allowed()
                    .opacity(0.5)
            })
            .when(!disabled, |el| {
                el.text_color(theme.foreground)
                    .cursor_pointer()
                    .hover(|s| s.bg(theme.accent.opacity(0.1)))
            });

        match variant {
            DropdownTriggerVariant::Compact => {
                trigger = trigger
                    .h_full()
                    .justify_center()
                    .px(Spacing::SM)
                    .text_size(FontSizes::SM)
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground)
                            .child("▾"),
                    );
            }
            DropdownTriggerVariant::Toolbar => {
                trigger = trigger
                    .justify_between()
                    .gap(Spacing::XS)
                    .px(Spacing::XS)
                    .text_size(FontSizes::XS)
                    .child(div().flex_1().truncate().child(label))
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground)
                            .child("▾"),
                    );
            }
            DropdownTriggerVariant::Standard => {
                trigger = trigger
                    .justify_between()
                    .gap(Spacing::SM)
                    .py(Spacing::XS)
                    .text_size(FontSizes::SM)
                    .child(div().flex_1().truncate().child(label))
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground)
                            .child("▾"),
                    );
            }
        }

        if !disabled {
            trigger = trigger.on_click(cx.listener(Self::handle_trigger_click));
        }

        let trigger = if render_plan.uses_control_shell {
            control_shell_with_padding(trigger, Spacing::MD, cx).into_any_element()
        } else {
            trigger.into_any_element()
        };

        let trigger = if render_plan.reserves_legacy_focus_gutter {
            div().w_full().p(px(2.0)).child(trigger).into_any_element()
        } else {
            trigger
        };

        if render_plan.uses_internal_focus_frame {
            focus_frame(self.focus_ring_visible, self.focus_ring_color, trigger, cx)
                .into_any_element()
        } else {
            trigger
        }
    }
}

impl Render for Dropdown {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_disabled = self.disabled;
        let disabled = is_disabled || self.items.is_empty();
        let label = self
            .selected_label()
            .unwrap_or_else(|| self.placeholder.clone());
        let variant = self.trigger_variant();
        let trigger = self.render_trigger(label, disabled, cx);

        let mut container = div()
            .id(self.id.clone())
            .w_full()
            .when(variant == DropdownTriggerVariant::Compact, |el| el.h_full())
            .child(trigger)
            .child(self.render_menu(cx));

        if self.open {
            container = container.on_mouse_down_out(cx.listener(Self::handle_mouse_down_out));
        }

        container
    }
}

impl EventEmitter<DropdownSelectionChanged> for Dropdown {}
impl EventEmitter<DropdownDismissed> for Dropdown {}

#[cfg(test)]
mod tests {
    use super::{
        Dropdown, DropdownTriggerVariant, dropdown_dismiss_transition, dropdown_focus_ring_state,
        dropdown_selection_transition, dropdown_trigger_render_plan,
    };

    #[test]
    fn selection_transition_closes_menu_and_clears_highlight() {
        let transition = dropdown_selection_transition(false, 3, 2);

        assert_eq!(
            transition,
            Some(super::DropdownSelectionTransition {
                selected_index: 2,
                open: false,
                highlighted_index: None,
            })
        );
    }

    #[test]
    fn selection_transition_rejects_out_of_range_highlights() {
        let transition = dropdown_selection_transition(false, 2, 9);

        assert_eq!(transition, None);
    }

    #[test]
    fn dismiss_transition_closes_open_menu_and_clears_highlight() {
        let transition = dropdown_dismiss_transition(true);

        assert_eq!(
            transition,
            Some(super::DropdownDismissTransition {
                open: false,
                highlighted_index: None,
            })
        );
    }

    #[test]
    fn dismiss_transition_skips_already_closed_menu() {
        let transition = dropdown_dismiss_transition(false);

        assert_eq!(transition, None);
    }

    #[test]
    fn toolbar_style_builder_selects_toolbar_trigger_variant() {
        let dropdown = Dropdown::new("toolbar-trigger").toolbar_style(true);

        assert_eq!(dropdown.trigger_variant(), DropdownTriggerVariant::Toolbar);
    }

    #[test]
    fn compact_trigger_takes_precedence_over_toolbar_variant() {
        let dropdown = Dropdown::new("compact-trigger")
            .toolbar_style(true)
            .compact_trigger(true);

        assert_eq!(dropdown.trigger_variant(), DropdownTriggerVariant::Compact);
    }

    #[test]
    fn focus_ring_builder_stores_custom_ring_color() {
        let dropdown =
            Dropdown::new("ring-trigger").focus_ring_color(Some(gpui::black().opacity(0.25)));

        assert!(dropdown.focus_ring_color.is_some());
        assert!(!dropdown.focus_ring_visible);
    }

    #[test]
    fn focus_ring_builder_can_clear_the_ring_color() {
        let dropdown = Dropdown::new("ring-clear")
            .focus_ring_color(Some(gpui::black().opacity(0.25)))
            .focus_ring_color(None);

        assert!(dropdown.focus_ring_color.is_none());
    }

    #[test]
    fn standard_trigger_uses_shared_shell() {
        let plan = dropdown_trigger_render_plan(DropdownTriggerVariant::Standard);

        assert!(plan.uses_control_shell);
        assert!(plan.uses_internal_focus_frame);
        assert!(plan.reserves_legacy_focus_gutter);
    }

    #[test]
    fn toolbar_trigger_skips_control_shell() {
        let plan = dropdown_trigger_render_plan(DropdownTriggerVariant::Toolbar);

        assert!(!plan.uses_control_shell);
        assert!(!plan.uses_internal_focus_frame);
        assert!(!plan.reserves_legacy_focus_gutter);
    }

    #[test]
    fn compact_trigger_skips_control_shell() {
        let plan = dropdown_trigger_render_plan(DropdownTriggerVariant::Compact);

        assert!(!plan.uses_control_shell);
        assert!(!plan.uses_internal_focus_frame);
        assert!(!plan.reserves_legacy_focus_gutter);
    }

    #[test]
    fn hiding_runtime_focus_ring_keeps_last_custom_color() {
        let custom_color = gpui::black().opacity(0.25);
        let (focus_ring_color, focus_ring_visible) =
            dropdown_focus_ring_state(Some(custom_color), None);

        assert_eq!(focus_ring_color, Some(custom_color));
        assert!(!focus_ring_visible);
    }

    #[test]
    fn showing_runtime_focus_ring_updates_color_and_visibility() {
        let custom_color = gpui::black().opacity(0.25);
        let (focus_ring_color, focus_ring_visible) =
            dropdown_focus_ring_state(None, Some(custom_color));

        assert_eq!(focus_ring_color, Some(custom_color));
        assert!(focus_ring_visible);
    }
}
