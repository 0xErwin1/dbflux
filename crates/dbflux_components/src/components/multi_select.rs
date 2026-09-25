use crate::controls::Checkbox;
use crate::icons::AppIcon;
use crate::primitives::{Chamfer, Icon};
use crate::tokens::{ChamferCut, Fields, FontSizes, Heights, Spacing};
use crate::typography::AppFonts;
use gpui::prelude::*;
use gpui::{
    Anchor, ElementId, EventEmitter, IntoElement, MouseButton, ParentElement, Render, ScrollHandle,
    SharedString, StatefulInteractiveElement, Styled, Window, anchored, deferred, div, point, px,
};
use gpui_component::ActiveTheme;

use crate::controls::DropdownItem;

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
    /// When true, the trigger omits its own border/background so it can be
    /// embedded inside an external shell (e.g. `control_shell`) without
    /// double-layering visual chrome.
    bare: bool,
    summary_name: Option<SharedString>,
    leading_icon: Option<AppIcon>,
}

impl MultiSelect {
    pub fn new(id: impl Into<ElementId>) -> Self {
        Self {
            id: id.into(),
            items: Vec::new(),
            selected_indices: Vec::new(),
            open: false,
            placeholder: dbflux_i18n::t!("controls.multi_select.placeholder").into(),
            menu_scroll_handle: ScrollHandle::new(),
            bare: false,
            summary_name: None,
            leading_icon: None,
        }
    }

    /// Shows the trigger as `name · count` (or `name · all` with nothing
    /// selected) instead of the selected labels, as the toolbar filters do.
    pub fn summary(mut self, name: impl Into<SharedString>) -> Self {
        self.summary_name = Some(name.into());
        self
    }

    /// Draws `icon` in the muted color before the trigger label.
    pub fn leading_icon(mut self, icon: AppIcon) -> Self {
        self.leading_icon = Some(icon);
        self
    }

    /// Suppress the trigger's own border and background.
    ///
    /// Use this when the MultiSelect is placed inside a container that already
    /// provides the visual shell (e.g. `control_shell`), to avoid stacking
    /// two sets of borders and backgrounds.
    pub fn bare(mut self) -> Self {
        self.bare = true;
        self
    }

    pub fn placeholder(mut self, placeholder: impl Into<SharedString>) -> Self {
        self.placeholder = placeholder.into();
        self
    }

    /// Update the placeholder text shown when no item is selected.
    pub fn set_placeholder(
        &mut self,
        placeholder: impl Into<SharedString>,
        cx: &mut Context<Self>,
    ) {
        self.placeholder = placeholder.into();
        cx.notify();
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

    pub fn toggle_open(&mut self, cx: &mut Context<Self>) {
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
        if let Some(name) = &self.summary_name {
            return summary_label(name, self.selected_indices.len());
        }

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
            format!(
                "{}, {}",
                labels[..2].join(", "),
                more_label(labels.len() - 2)
            )
            .into()
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
                    .id(("ms-item", index))
                    .relative()
                    .flex()
                    .items_center()
                    .gap(Fields::CHECKBOX_GAP)
                    .h(Fields::MENU_ROW_HEIGHT)
                    .mx(Fields::MENU_ROW_INSET)
                    .px(Fields::PADDING_X)
                    .cursor_pointer()
                    .whitespace_nowrap()
                    .text_color(if checked {
                        theme.accent_foreground
                    } else {
                        theme.foreground
                    })
                    .child(
                        Chamfer::new(ChamferCut::KEYCAP)
                            .fill_hover(theme.list_hover)
                            .interactive(("ms-row-shape", index)),
                    )
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
                    .child(item.label.clone())
                    .into_any_element()
            })
            .collect();

        let clear_row = has_selection.then(|| {
            div()
                .id("ms-clear")
                .relative()
                .flex()
                .items_center()
                .h(Fields::MENU_ROW_HEIGHT)
                .mx(Fields::MENU_ROW_INSET)
                .px(Fields::PADDING_X)
                .cursor_pointer()
                .text_color(theme.muted_foreground)
                .child(
                    Chamfer::new(ChamferCut::KEYCAP)
                        .fill_hover(theme.list_hover)
                        .interactive("ms-clear-shape"),
                )
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(|this, _event, _window, cx| {
                        this.clear_selection(cx);
                    }),
                )
                .child(dbflux_i18n::t!("controls.multi_select.clear_all"))
        });

        let rows = div()
            .id("ms-menu-rows")
            .max_h(Fields::MENU_MAX_HEIGHT)
            .py(Fields::MENU_PADDING_Y)
            .overflow_y_scroll()
            .track_scroll(&self.menu_scroll_handle)
            .children(items)
            .children(clear_row);

        let menu = div()
            .id("ms-menu")
            .relative()
            .min_w_full()
            .font_family(AppFonts::INTERFACE)
            .text_size(FontSizes::BASE)
            .shadow_lg()
            .occlude()
            .child(
                Chamfer::new(ChamferCut::OVERLAY)
                    .fill(theme.secondary)
                    .border(theme.input),
            )
            .child(rows);

        deferred(
            anchored()
                .anchor(Anchor::TopLeft)
                .offset(point(px(0.0), Spacing::XS))
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
        let has_selection = !self.selected_indices.is_empty() || self.summary_name.is_some();
        let bare = self.bare;

        let text_color = if is_empty {
            theme.muted_foreground
        } else if has_selection {
            theme.accent_foreground
        } else {
            theme.muted_foreground
        };

        // In bare mode the trigger omits its own shape to avoid double
        // chrome when embedded inside `control_shell`.
        let shape = (!bare).then(|| {
            let opacity = if is_empty {
                Fields::DISABLED_OPACITY
            } else {
                1.0
            };

            let mut shape = Chamfer::new(ChamferCut::CONTROL)
                .fill(theme.secondary.opacity(opacity))
                .border(theme.border.opacity(opacity));

            if !is_empty {
                shape = shape
                    .fill_hover(theme.secondary_hover)
                    .interactive("ms-trigger-shape");
            }

            shape
        });

        let trigger = div()
            .id("ms-trigger")
            .relative()
            .h(if bare {
                Heights::BUTTON
            } else {
                Fields::HEIGHT
            })
            .flex()
            .items_center()
            .gap(Fields::GAP)
            .w_full()
            .px(Fields::PADDING_X)
            .font_family(AppFonts::INTERFACE)
            .text_size(Fields::TEXT)
            .text_color(text_color)
            .children(shape)
            .when(is_empty, |el| el.cursor_not_allowed())
            .when(!is_empty, |el| el.cursor_pointer())
            .when_some(self.leading_icon, |el, icon| {
                el.child(
                    Icon::new(icon)
                        .size(Fields::LEADING_ICON)
                        .color(theme.muted_foreground),
                )
            })
            .child(div().flex_1().whitespace_nowrap().truncate().child(label))
            .child(
                Icon::new(if self.open {
                    AppIcon::ChevronUp
                } else {
                    AppIcon::ChevronDown
                })
                .size(Fields::CHEVRON)
                .color(theme.muted_foreground),
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

/// Trigger text of a summarized multi-select: `name · count`, or
/// `name · all` when nothing is selected (no filter applied).
fn summary_label(name: &str, selected: usize) -> SharedString {
    if selected == 0 {
        dbflux_i18n::t!("controls.multi_select.summary_all", name = name).into()
    } else {
        dbflux_i18n::t!(
            "controls.multi_select.summary_count",
            name = name,
            count = selected
        )
        .into()
    }
}

/// Label for the "+N more" trigger suffix shown when more than three items
/// are selected.
///
/// Uses the singular catalog bucket only for exactly one extra item; every
/// other count uses the plural bucket.
fn more_label(extra: usize) -> String {
    if extra == 1 {
        dbflux_i18n::t!("controls.multi_select.more.one", count = extra)
    } else {
        dbflux_i18n::t!("controls.multi_select.more.many", count = extra)
    }
}

#[cfg(test)]
mod tests {
    use super::more_label;

    #[test]
    fn multi_select_keys_resolve_in_both_locales() {
        let keys = [
            "controls.multi_select.placeholder",
            "controls.multi_select.clear_all",
            "controls.multi_select.more.one",
            "controls.multi_select.more.many",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(!en.is_empty() && en != key, "en missing for {key}");
            assert!(!es.is_empty() && es != key, "es missing for {key}");
        }
    }

    #[test]
    fn more_label_uses_plural_buckets() {
        let one = more_label(1);
        assert_eq!(
            one,
            dbflux_i18n::t!("controls.multi_select.more.one", count = 1)
        );

        let many = more_label(3);
        assert!(many.contains('3'));
        assert_eq!(
            many,
            dbflux_i18n::t!("controls.multi_select.more.many", count = 3)
        );
    }
}
