use gpui::prelude::*;
use gpui::{
    Corner, ElementId, EventEmitter, IntoElement, MouseButton, ParentElement, Render, ScrollHandle,
    SharedString, StatefulInteractiveElement, Styled, Window, anchored, deferred, div, point, px,
};
use gpui_component::ActiveTheme;
use gpui_component::checkbox::Checkbox;

use super::dropdown::DropdownItem;

/// Emitted whenever the set of selected values changes.
#[derive(Clone, Debug)]
pub struct MultiSelectChanged {
    #[allow(dead_code)]
    pub selected_values: Vec<SharedString>,
}

pub struct MultiSelect {
    id: ElementId,
    items: Vec<DropdownItem>,
    selected_indices: Vec<usize>,
    open: bool,
    placeholder: SharedString,
    menu_scroll_handle: ScrollHandle,
}

impl MultiSelect {
    pub fn new(id: impl Into<ElementId>) -> Self {
        Self {
            id: id.into(),
            items: Vec::new(),
            selected_indices: Vec::new(),
            open: false,
            placeholder: "Select…".into(),
            menu_scroll_handle: ScrollHandle::new(),
        }
    }

    pub fn placeholder(mut self, placeholder: impl Into<SharedString>) -> Self {
        self.placeholder = placeholder.into();
        self
    }

    /// Replace the item list. Clears the selection if selected indices are now out of range.
    pub fn set_items(&mut self, items: Vec<DropdownItem>, cx: &mut Context<Self>) {
        self.items = items;
        self.selected_indices.retain(|&i| i < self.items.len());
        cx.notify();
    }

    /// Return the values of all currently selected items.
    pub fn selected_values(&self) -> Vec<SharedString> {
        self.selected_indices
            .iter()
            .filter_map(|&i| self.items.get(i).map(|item| item.value.clone()))
            .collect()
    }

    /// Set selection by matching values against the item list. Unknown values are ignored.
    pub fn set_selected_values(&mut self, values: &[String], cx: &mut Context<Self>) {
        self.selected_indices = values
            .iter()
            .filter_map(|v| {
                self.items
                    .iter()
                    .position(|item| item.value.as_ref() == v.as_str())
            })
            .collect();
        cx.notify();
    }

    pub fn clear_selection(&mut self, cx: &mut Context<Self>) {
        self.selected_indices.clear();
        self.open = false;
        cx.emit(MultiSelectChanged {
            selected_values: Vec::new(),
        });
        cx.notify();
    }

    fn toggle_index(&mut self, index: usize, cx: &mut Context<Self>) {
        if index >= self.items.len() {
            return;
        }

        if let Some(pos) = self.selected_indices.iter().position(|&i| i == index) {
            self.selected_indices.remove(pos);
        } else {
            self.selected_indices.push(index);
        }

        cx.emit(MultiSelectChanged {
            selected_values: self.selected_values(),
        });
        cx.notify();
    }

    fn toggle_open(&mut self, cx: &mut Context<Self>) {
        if self.items.is_empty() {
            return;
        }
        self.open = !self.open;
        cx.notify();
    }

    fn handle_mouse_down_out(
        &mut self,
        _event: &gpui::MouseDownEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.open {
            self.open = false;
            cx.notify();
        }
    }

    fn render_trigger_label(&self) -> SharedString {
        if self.selected_indices.is_empty() {
            return self.placeholder.clone();
        }

        let labels: Vec<&str> = self
            .selected_indices
            .iter()
            .filter_map(|&i| self.items.get(i).map(|item| item.label.as_ref()))
            .collect();

        if labels.len() <= 3 {
            labels.join(", ").into()
        } else {
            format!("{}, +{} more", labels[..2].join(", "), labels.len() - 2).into()
        }
    }

    fn render_menu(&self, cx: &Context<Self>) -> gpui::AnyElement {
        if !self.open || self.items.is_empty() {
            return div().into_any_element();
        }

        let theme = cx.theme();
        let has_selection = !self.selected_indices.is_empty();

        let items: Vec<gpui::AnyElement> = self
            .items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let checked = self.selected_indices.contains(&index);
                div()
                    .id(index)
                    .w_full()
                    .px_2()
                    .py_1p5()
                    .flex()
                    .items_center()
                    .gap_2()
                    .cursor_pointer()
                    .hover(|s| s.bg(theme.list_active))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _event, _window, cx| {
                            this.toggle_index(index, cx);
                        }),
                    )
                    .child(
                        Checkbox::new(SharedString::from(format!("ms-item-{}", index)))
                            .checked(checked),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .child(item.label.clone()),
                    )
                    .into_any_element()
            })
            .collect();

        let footer = div().p_1().pt_0().when(has_selection, |d| {
            d.child(
                div()
                    .id("ms-clear")
                    .w_full()
                    .px_2()
                    .py_1()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .cursor_pointer()
                    .rounded_sm()
                    .hover(|s| s.bg(theme.list_active).text_color(theme.foreground))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, _event, _window, cx| {
                            this.clear_selection(cx);
                        }),
                    )
                    .child("Clear all"),
            )
        });

        let menu = div()
            .id("ms-menu")
            .min_w_full()
            .max_h(px(220.0))
            .p_1()
            .border_1()
            .border_color(theme.border)
            .bg(theme.background)
            .rounded_md()
            .overflow_scroll()
            .track_scroll(&self.menu_scroll_handle)
            .shadow_lg()
            .occlude()
            .children(items)
            .child(footer);

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

impl Render for MultiSelect {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_empty = self.items.is_empty();
        let label = self.render_trigger_label();
        let has_selection = !self.selected_indices.is_empty();

        let trigger = div()
            .id("ms-trigger")
            .flex()
            .items_center()
            .justify_between()
            .gap_2()
            .w_full()
            .px_3()
            .py_1p5()
            .rounded_md()
            .bg(theme.background)
            .border_1()
            .border_color(theme.input)
            .text_sm()
            .when(is_empty, |el| {
                el.text_color(theme.muted_foreground)
                    .cursor_not_allowed()
                    .opacity(0.5)
            })
            .when(!is_empty, |el| {
                el.cursor_pointer()
                    .hover(|s| s.bg(theme.accent.opacity(0.1)))
            })
            .when(has_selection, |el| el.text_color(theme.foreground))
            .when(!has_selection, |el| el.text_color(theme.muted_foreground))
            .child(div().flex_1().truncate().child(label))
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(if self.open { "▴" } else { "▾" }),
            )
            .when(!is_empty, |el| {
                el.on_click(cx.listener(|this, _event, _window, cx| {
                    this.toggle_open(cx);
                }))
            });

        let trigger_wrap = div()
            .id("ms-trigger-wrap")
            .w_full()
            .flex()
            .flex_col()
            .rounded(px(4.0))
            .border_2()
            .border_color(gpui::transparent_black())
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

impl EventEmitter<MultiSelectChanged> for MultiSelect {}
