use gpui::prelude::*;
use gpui::{
    Anchor, App, ElementId, EventEmitter, FocusHandle, IntoElement, ParentElement, Render,
    ScrollHandle, SharedString, StatefulInteractiveElement, Styled, Subscription, Window, anchored,
    deferred, div, point, px,
};
use gpui_component::{ActiveTheme, IconName};
use std::rc::Rc;

use crate::controls::{Checkbox, DropdownItem};
use crate::icon::IconSource;
use crate::keymap::{Command, CommandSet, KeyCommandHandler};
use crate::primitives::{Chamfer, ChamferRing, Icon};
use crate::strings;
use crate::tokens::{ChamferCut, ChromeColors, Fields, FontSizes, Heights, Spacing};

/// The catalogue keys the multi-select draws; the product's catalogue answers
/// them through [`crate::strings`].
const PLACEHOLDER_KEY: &str = "controls.multi_select.placeholder";
const CLEAR_ALL_KEY: &str = "controls.multi_select.clear_all";
const SUMMARY_ALL_KEY: &str = "controls.multi_select.summary_all";
const SUMMARY_COUNT_KEY: &str = "controls.multi_select.summary_count";
const MORE_ONE_KEY: &str = "controls.multi_select.more.one";
const MORE_MANY_KEY: &str = "controls.multi_select.more.many";

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
    placeholder: Option<SharedString>,
    menu_scroll_handle: ScrollHandle,
    /// When true, the trigger omits its own border/background so it can be
    /// embedded inside an external shell (e.g. `control_shell`) without
    /// double-layering visual chrome.
    bare: bool,
    summary_name: Option<SharedString>,
    leading_icon: Option<IconSource>,
    /// Row the keyboard points at while the list is open.
    highlighted_index: Option<usize>,
    /// Created the first time the control is focused from the keyboard
    /// (see [`MultiSelect::focus`]).
    focus_handle: Option<FocusHandle>,
    /// Where focus was before [`MultiSelect::focus`], given back when the
    /// list closes from the keyboard.
    return_focus: Option<FocusHandle>,
    _focus_out: Option<Subscription>,
}

impl MultiSelect {
    pub fn new(id: impl Into<ElementId>) -> Self {
        Self {
            id: id.into(),
            items: Vec::new(),
            selected_indices: Vec::new(),
            open: false,
            placeholder: None,
            menu_scroll_handle: ScrollHandle::new(),
            bare: false,
            summary_name: None,
            leading_icon: None,
            highlighted_index: None,
            focus_handle: None,
            return_focus: None,
            _focus_out: None,
        }
    }

    /// Shows the trigger as `name · count` (or `name · all` with nothing
    /// selected) instead of the selected labels, as the toolbar filters do.
    pub fn summary(mut self, name: impl Into<SharedString>) -> Self {
        self.summary_name = Some(name.into());
        self
    }

    /// Draws `icon` in the muted color before the trigger label.
    pub fn leading_icon(mut self, icon: impl Into<IconSource>) -> Self {
        self.leading_icon = Some(icon.into());
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
        self.placeholder = Some(placeholder.into());
        self
    }

    /// Update the placeholder text shown when no item is selected.
    pub fn set_placeholder(
        &mut self,
        placeholder: impl Into<SharedString>,
        cx: &mut Context<Self>,
    ) {
        self.placeholder = Some(placeholder.into());
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
        self.highlighted_index = self.open.then_some(0);
        cx.notify();
    }

    pub fn is_open(&self) -> bool {
        self.open
    }

    /// Moves keyboard focus to the control, so it answers the keymap keys
    /// bound to the multi-select's context itself: Enter or Space opens the
    /// list, j / k and the arrows move, Space toggles the highlighted item,
    /// and Enter or Escape closes the list and gives focus back to the
    /// element that held it before.
    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let handle = self.ensure_focus_handle(window, cx);

        if !handle.contains_focused(window, cx) {
            self.return_focus = window.focused(cx);
        }

        handle.focus(window, cx);
        cx.notify();
    }

    /// Whether keyboard focus is on the control.
    pub fn is_focused(&self, window: &Window) -> bool {
        self.focus_handle
            .as_ref()
            .is_some_and(|handle| handle.is_focused(window))
    }

    fn ensure_focus_handle(&mut self, window: &mut Window, cx: &mut Context<Self>) -> FocusHandle {
        if let Some(handle) = &self.focus_handle {
            return handle.clone();
        }

        let handle = cx.focus_handle();

        self._focus_out = Some(
            cx.on_focus_out(&handle, window, |this, _event, _window, cx| {
                this.return_focus = None;
                this.close(cx);
            }),
        );

        self.focus_handle = Some(handle.clone());
        handle
    }

    fn close(&mut self, cx: &mut Context<Self>) {
        if self.open {
            self.open = false;
            self.highlighted_index = None;
            cx.notify();
        }
    }

    fn move_highlight(&mut self, forward: bool, cx: &mut Context<Self>) {
        let count = self.items.len();
        if count == 0 {
            return;
        }

        let next = match (self.highlighted_index, forward) {
            (Some(index), true) => (index + 1) % count,
            (Some(0), false) | (None, false) => count - 1,
            (Some(index), false) => index - 1,
            (None, true) => 0,
        };

        self.highlighted_index = Some(next);
        self.menu_scroll_handle.scroll_to_item(next);
        cx.notify();
    }

    fn give_focus_back(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(previous) = self.return_focus.take() else {
            return;
        };

        if self.is_focused(window) {
            previous.focus(window, cx);
        }
    }

    /// Answers the menu keys of the installed [`CommandSet`] while the
    /// control has focus. Answers whether the key was handled; the attach
    /// hook propagates the ones it was not, so a key the control has no use
    /// for in its current state reaches the pane around it.
    fn handle_key_command(
        &mut self,
        action_id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(command) = cx
            .try_global::<CommandSet>()
            .and_then(|keys| keys.command(action_id))
        else {
            return false;
        };

        match command {
            Command::SelectNext if self.open => {
                self.move_highlight(true, cx);
                true
            }
            Command::SelectPrev if self.open => {
                self.move_highlight(false, cx);
                true
            }
            Command::ExpandCollapse if self.open => {
                if let Some(index) = self.highlighted_index {
                    self.toggle_index(index, cx);
                }
                true
            }
            Command::Execute | Command::Cancel if self.open => {
                self.close(cx);
                self.give_focus_back(window, cx);
                true
            }
            Command::Execute | Command::ExpandCollapse if !self.items.is_empty() => {
                self.toggle_open(cx);
                true
            }
            Command::Cancel if self.return_focus.is_some() => {
                self.give_focus_back(window, cx);
                true
            }
            _ => false,
        }
    }

    fn handle_mouse_down_out(
        &mut self,
        _event: &gpui::MouseDownEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.close(cx);
    }

    fn render_trigger_label(&self, cx: &App) -> SharedString {
        if let Some(name) = &self.summary_name {
            return summary_label(cx, name, self.selected_indices.len());
        }

        if self.selected_indices.is_empty() {
            return self
                .placeholder
                .clone()
                .unwrap_or_else(|| strings::text(cx, PLACEHOLDER_KEY));
        }

        let labels: Vec<&str> = self
            .selected_indices
            .iter()
            .filter_map(|&i| self.items.get(i).map(|item| item.label.as_ref()))
            .collect();

        if labels.len() <= 3 {
            labels.join(", ").into()
        } else {
            #[expect(
                clippy::indexing_slicing,
                reason = "the else arm runs only when labels.len() > 3, so the ..2 slice is within 0..len"
            )]
            format!(
                "{}, {}",
                labels[..2].join(", "),
                more_label(cx, labels.len() - 2)
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
                let highlighted = self.highlighted_index == Some(index);

                let row_shape = if highlighted {
                    Chamfer::new(ChamferCut::KEYCAP)
                        .fill(ChromeColors::tint(theme).opacity(Fields::MENU_HIGHLIGHT_ALPHA))
                } else {
                    Chamfer::new(ChamferCut::KEYCAP)
                        .fill_hover(theme.list_hover)
                        .interactive(("ms-row-shape", index))
                };

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
                    .child(row_shape)
                    .on_click(cx.listener(move |this, _event, _window, cx| {
                        this.toggle_index(index, cx);
                    }))
                    .child(
                        Checkbox::new(SharedString::from(format!("ms-item-{}", index)))
                            .checked(checked)
                            .aria_label(item.label.clone()),
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
                .on_click(cx.listener(|this, _event, _window, cx| {
                    this.clear_selection(cx);
                }))
                .child(strings::text(cx, CLEAR_ALL_KEY))
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
            .font_family(crate::fonts::ui_family(cx))
            .text_size(FontSizes::BASE)
            .shadow_lg()
            .occlude()
            .on_mouse_down_out(cx.listener(Self::handle_mouse_down_out))
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
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let keyboard_focused = self.is_focused(window);
        let theme = cx.theme();
        let is_empty = self.items.is_empty();
        let label = self.render_trigger_label(cx);
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

            if keyboard_focused {
                shape = shape.ring(ChamferRing::focus(ChromeColors::tint(theme)));
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
            .font_family(crate::fonts::ui_family(cx))
            .text_size(Fields::TEXT)
            .text_color(text_color)
            .children(shape)
            .when(is_empty, |el| el.cursor_not_allowed())
            .when(!is_empty, |el| el.cursor_pointer())
            .when_some(self.leading_icon.clone(), |el, icon| {
                el.child(
                    Icon::new(icon)
                        .size(Fields::LEADING_ICON)
                        .color(theme.muted_foreground),
                )
            })
            .child(div().flex_1().whitespace_nowrap().truncate().child(label))
            .child(
                Icon::new(if self.open {
                    IconName::ChevronUp
                } else {
                    IconName::ChevronDown
                })
                .size(Fields::CHEVRON)
                .color(theme.muted_foreground),
            )
            // While open, the menu's on_mouse_down_out owns dismissal: a press
            // on the trigger lands outside the deferred menu and closes it, so
            // a trigger on_click would reopen the list on release.
            .when(!is_empty && !self.open, |el| {
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

        let mut element = div()
            .id(self.id.clone())
            .when_some(self.focus_handle.as_ref(), |element, handle| {
                element.track_focus(handle)
            })
            .w_full()
            .child(trigger_wrap);

        // The product's keymap seam, installed once per app: the multi-select
        // answers the keymap action under its key context and propagates the
        // keys it does not own. Without a seam it answers none.
        if let Some(keys) = cx.try_global::<CommandSet>() {
            let attach = keys.attach;
            let key_context = keys.key_context.clone();
            let handle = cx.entity().downgrade();
            let handler: KeyCommandHandler = Rc::new(move |action_id, window, cx| {
                let Some(entity) = handle.upgrade() else {
                    return false;
                };

                entity.update(cx, |this, cx| {
                    this.handle_key_command(action_id, window, cx)
                })
            });

            element = attach(element.key_context(key_context.as_ref()), handler);
        }

        element
    }
}

impl EventEmitter<MultiSelectChanged> for MultiSelect {}

/// Trigger text of a summarized multi-select: `name · count`, or
/// `name · all` when nothing is selected (no filter applied).
fn summary_label(cx: &App, name: &str, selected: usize) -> SharedString {
    if selected == 0 {
        strings::text_with(cx, SUMMARY_ALL_KEY, &[("name", name)])
    } else {
        strings::text_with(
            cx,
            SUMMARY_COUNT_KEY,
            &[("name", name), ("count", &selected.to_string())],
        )
    }
}

/// Label for the "+N more" trigger suffix shown when more than three items
/// are selected.
fn more_label(cx: &App, extra: usize) -> String {
    strings::text_with(cx, more_key(extra), &[("count", &extra.to_string())]).to_string()
}

/// The catalogue key of the "+N more" suffix: the singular bucket only for
/// exactly one extra item, the plural bucket for every other count.
fn more_key(extra: usize) -> &'static str {
    if extra == 1 {
        MORE_ONE_KEY
    } else {
        MORE_MANY_KEY
    }
}

#[cfg(test)]
mod tests {
    use super::more_key;

    /// The singular bucket of the "+N more" suffix is drawn only for exactly
    /// one extra item; every other count takes the plural bucket. The
    /// catalogue keys themselves are asserted against the product catalogue
    /// at the re-export path.
    #[test]
    fn more_key_picks_the_singular_bucket_only_for_exactly_one() {
        assert_eq!(more_key(1), "controls.multi_select.more.one");
        assert_eq!(more_key(0), "controls.multi_select.more.many");
        assert_eq!(more_key(2), "controls.multi_select.more.many");
        assert_eq!(more_key(30), "controls.multi_select.more.many");
    }
}
