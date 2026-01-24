use std::sync::Arc;

use gpui::prelude::*;
use gpui::{
    Corner, ElementId, EventEmitter, Hsla, InteractiveElement, IntoElement, MouseButton,
    ParentElement, Render, SharedString, StatefulInteractiveElement, Styled, Window, anchored,
    deferred, div, point, px,
};
use gpui_component::ActiveTheme;

#[derive(Clone, Debug)]
pub struct DropdownSelectionChanged {
    pub index: usize,
    #[allow(dead_code)]
    pub item: DropdownItem,
}

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
    on_select: Option<Arc<dyn Fn(usize, &DropdownItem, &mut Context<Self>) + Send + Sync>>,
}

#[allow(dead_code)]
impl Dropdown {
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
        self.focus_ring_color = color;
        cx.notify();
    }

    pub fn toggle_open(&mut self, cx: &mut Context<Self>) {
        if self.disabled || self.items.is_empty() {
            return;
        }
        self.open = !self.open;
        if self.open {
            // When opening, highlight the selected item or first item
            self.highlighted_index = self.selected_index.or(Some(0));
        }
        cx.notify();
    }

    pub fn open(&mut self, cx: &mut Context<Self>) {
        if !self.disabled && !self.items.is_empty() {
            self.open = true;
            self.highlighted_index = self.selected_index.or(Some(0));
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
        cx.notify();
    }

    pub fn accept_selection(&mut self, cx: &mut Context<Self>) {
        if let Some(index) = self.highlighted_index {
            self.select_item(index, cx);
        }
    }

    fn select_item(&mut self, index: usize, cx: &mut Context<Self>) {
        if self.disabled {
            return;
        }
        let item = match self.items.get(index) {
            Some(item) => item.clone(),
            None => return,
        };
        self.selected_index = Some(index);
        self.highlighted_index = None;
        self.open = false;
        if let Some(on_select) = self.on_select.clone() {
            on_select(index, &item, cx);
        }
        cx.emit(DropdownSelectionChanged {
            index,
            item: item.clone(),
        });
        cx.notify();
    }

    fn handle_trigger_click(
        &mut self,
        _event: &gpui::ClickEvent,
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
        if self.open {
            self.open = false;
            self.highlighted_index = None;
            cx.notify();
        }
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
                    .px_2()
                    .py_1p5()
                    .text_sm()
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
            .min_w_full()
            .p_1()
            .border_1()
            .border_color(theme.border)
            .bg(theme.background)
            .rounded_md()
            .overflow_hidden()
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
}

impl Render for Dropdown {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_disabled = self.disabled;
        let disabled = is_disabled || self.items.is_empty();
        let label = self
            .selected_label()
            .unwrap_or_else(|| self.placeholder.clone());

        let focus_ring_color = self.focus_ring_color;

        let mut trigger = div()
            .id("dropdown-trigger")
            .flex()
            .items_center()
            .justify_between()
            .gap_2()
            .px_3()
            .py_1p5()
            .w_full()
            .rounded_md()
            .bg(theme.background)
            .border_1()
            .border_color(theme.input)
            .text_sm()
            .when(disabled, |el| {
                el.text_color(theme.muted_foreground)
                    .cursor_not_allowed()
                    .opacity(0.5)
            })
            .when(!disabled, |el| {
                el.text_color(theme.foreground)
                    .cursor_pointer()
                    .hover(|s| s.bg(theme.accent.opacity(0.1)))
            })
            .child(div().flex_1().truncate().child(label))
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child("▾"),
            );

        if !disabled {
            trigger = trigger.on_click(cx.listener(Self::handle_trigger_click));
        }

        // Focus ring wrapper
        let trigger_wrap = div()
            .id("trigger-wrap")
            .w_full()
            .flex()
            .flex_col()
            .rounded(px(4.0))
            .border_2()
            .when_some(focus_ring_color, |d, color| d.border_color(color))
            .when(focus_ring_color.is_none(), |d| {
                d.border_color(gpui::transparent_black())
            })
            .p(px(2.0))
            .child(trigger)
            .child(self.render_menu(cx));

        let mut container = div().id(self.id.clone()).w_full().child(trigger_wrap);

        if self.open {
            container = container.on_mouse_down_out(cx.listener(Self::handle_mouse_down_out));
        }

        container
    }
}

impl EventEmitter<DropdownSelectionChanged> for Dropdown {}
