use crate::actions::RunCommand;
use crate::controls::Checkbox;
use crate::icons::AppIcon;
use crate::primitives::{Chamfer, ChamferRing, Icon};
use crate::tokens::{ChamferCut, ChromeColors, Fields, FontSizes, Heights, Spacing};
use crate::typography::AppFonts;
use dbflux_core::keymap_types::{Command, ContextId};
use gpui::prelude::*;
use gpui::{
    Anchor, ElementId, EventEmitter, FocusHandle, IntoElement, ParentElement, Render, ScrollHandle,
    SharedString, StatefulInteractiveElement, Styled, Subscription, Window, anchored, deferred,
    div, point, px,
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
            placeholder: dbflux_i18n::t!("controls.multi_select.placeholder").into(),
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
        self.highlighted_index = self.open.then_some(0);
        cx.notify();
    }

    pub fn is_open(&self) -> bool {
        self.open
    }

    /// Moves keyboard focus to the control, so it answers the `Dropdown`
    /// keys itself: Enter or Space opens the list, j / k and the arrows move,
    /// Space toggles the highlighted item, and Enter or Escape closes the
    /// list and gives focus back to the element that held it before.
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

    /// Answers the `Dropdown` keys while the control has focus. A key it has
    /// no use for in its current state propagates to the pane around it.
    fn handle_run_command(
        &mut self,
        action: &RunCommand,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let handled = match Command::from_action_id(&action.command) {
            Some(Command::SelectNext) if self.open => {
                self.move_highlight(true, cx);
                true
            }
            Some(Command::SelectPrev) if self.open => {
                self.move_highlight(false, cx);
                true
            }
            Some(Command::ExpandCollapse) if self.open => {
                if let Some(index) = self.highlighted_index {
                    self.toggle_index(index, cx);
                }
                true
            }
            Some(Command::Execute | Command::Cancel) if self.open => {
                self.close(cx);
                self.give_focus_back(window, cx);
                true
            }
            Some(Command::Execute | Command::ExpandCollapse) if !self.items.is_empty() => {
                self.toggle_open(cx);
                true
            }
            Some(Command::Cancel) if self.return_focus.is_some() => {
                self.give_focus_back(window, cx);
                true
            }
            _ => false,
        };

        if !handled {
            cx.propagate();
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
                .on_click(cx.listener(|this, _event, _window, cx| {
                    this.clear_selection(cx);
                }))
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

        div()
            .id(self.id.clone())
            .key_context(ContextId::Dropdown.as_gpui_context())
            .when_some(self.focus_handle.as_ref(), |element, handle| {
                element.track_focus(handle)
            })
            .on_action(cx.listener(Self::handle_run_command))
            .w_full()
            .child(trigger_wrap)
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

    /// Keyboard tests against a real window, with the `Dropdown` keys the
    /// app keymap binds (see `bind_dropdown_keys_for_tests`).
    mod keyboard {
        use super::super::{MultiSelect, MultiSelectChanged};
        use crate::controls::{DropdownItem, bind_dropdown_keys_for_tests};
        use gpui::{
            AppContext as _, Context, Entity, FocusHandle, InteractiveElement as _, IntoElement,
            ParentElement as _, Render, SharedString, Styled as _, TestAppContext,
            VisualTestContext, Window, div,
        };

        struct Owner {
            focus: FocusHandle,
            multi_select: Entity<MultiSelect>,
            changes: Vec<Vec<SharedString>>,
            _subscription: gpui::Subscription,
        }

        impl Render for Owner {
            fn render(
                &mut self,
                _window: &mut Window,
                _cx: &mut Context<Self>,
            ) -> impl IntoElement {
                div()
                    .size_full()
                    .track_focus(&self.focus)
                    .child(self.multi_select.clone())
            }
        }

        fn setup(cx: &mut TestAppContext) -> (Entity<Owner>, &mut VisualTestContext) {
            cx.update(gpui_component::init);
            bind_dropdown_keys_for_tests(cx);

            let (owner, window) = cx.add_window_view(|window, cx| {
                let multi_select = cx.new(|cx| {
                    let mut multi_select = MultiSelect::new("keyboard-multi-select");
                    multi_select.set_items(
                        vec![
                            DropdownItem::new("One"),
                            DropdownItem::new("Two"),
                            DropdownItem::new("Three"),
                        ],
                        cx,
                    );
                    multi_select
                });

                let subscription = cx.subscribe_in(
                    &multi_select,
                    window,
                    |this: &mut Owner, _, event: &MultiSelectChanged, _window, _cx| {
                        this.changes.push(event.selected_values.clone());
                    },
                );

                Owner {
                    focus: cx.focus_handle(),
                    multi_select,
                    changes: Vec::new(),
                    _subscription: subscription,
                }
            });
            window.run_until_parked();

            window.update(|window, cx| {
                let focus = owner.read(cx).focus.clone();
                focus.focus(window, cx);

                let multi_select = owner.read(cx).multi_select.clone();
                multi_select.update(cx, |multi_select, cx| multi_select.focus(window, cx));
            });
            window.run_until_parked();

            (owner, window)
        }

        #[gpui::test]
        fn space_toggles_the_highlighted_item_and_escape_returns_focus(cx: &mut TestAppContext) {
            let (owner, window) = setup(cx);

            window.simulate_keystrokes("space");
            window.run_until_parked();
            let open = window.update(|_, cx| owner.read(cx).multi_select.read(cx).is_open());
            assert!(open, "Space opens the list");

            window.simulate_keystrokes("space j j space k k space");
            window.run_until_parked();

            let values =
                window.update(|_, cx| owner.read(cx).multi_select.read(cx).selected_values());
            assert_eq!(
                values,
                vec![SharedString::from("Three")],
                "One on, Three on, One off again"
            );

            window.simulate_keystrokes("escape");
            window.run_until_parked();

            let (open, owner_focused, changes) = window.update(|window, cx| {
                let owner = owner.read(cx);
                (
                    owner.multi_select.read(cx).is_open(),
                    owner.focus.is_focused(window),
                    owner.changes.len(),
                )
            });
            assert!(!open, "Escape closes the list");
            assert!(owner_focused, "Escape hands focus back");
            assert_eq!(changes, 3, "every toggle reports the new selection");
        }
    }

    /// Pointer tests that press and release the mouse on the rendered
    /// elements, located through the window's accessibility frame.
    mod pointer {
        use std::sync::{Arc, Mutex};

        use super::super::{MultiSelect, MultiSelectChanged};
        use crate::controls::DropdownItem;
        use gpui::{
            AccessibilityFrame, AppContext as _, Context, Entity, FrameObserver, IntoElement,
            Modifiers, MouseButton, ParentElement as _, Pixels, Point, Render, SharedString,
            Styled as _, TestAppContext, VisualTestContext, Window, div, point, px,
        };

        /// Keeps the latest rendered accessibility frame of the window it observes.
        #[derive(Default)]
        struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

        impl FrameObserver for FrameCapture {
            fn accessibility_updated(&self, frame: &AccessibilityFrame) {
                *self.0.lock().expect("frame capture lock") = Some(frame.clone());
            }
        }

        struct Owner {
            multi_select: Entity<MultiSelect>,
            changes: Vec<Vec<SharedString>>,
            _subscription: gpui::Subscription,
        }

        impl Render for Owner {
            fn render(
                &mut self,
                _window: &mut Window,
                _cx: &mut Context<Self>,
            ) -> impl IntoElement {
                div()
                    .size_full()
                    .child(div().w(px(200.0)).child(self.multi_select.clone()))
            }
        }

        fn setup(
            cx: &mut TestAppContext,
        ) -> (Arc<FrameCapture>, Entity<Owner>, &mut VisualTestContext) {
            cx.update(gpui_component::init);

            let capture = Arc::new(FrameCapture::default());
            let capture_for_window = capture.clone();
            let (owner, window) = cx.add_window_view(move |window, cx| {
                window.observe_frames(&capture_for_window);
                window.refresh();

                let multi_select = cx.new(|cx| {
                    let mut multi_select = MultiSelect::new("pointer-multi-select");
                    multi_select.set_items(
                        vec![
                            DropdownItem::new("One"),
                            DropdownItem::new("Two"),
                            DropdownItem::new("Three"),
                        ],
                        cx,
                    );
                    multi_select
                });

                let subscription = cx.subscribe_in(
                    &multi_select,
                    window,
                    |this: &mut Owner, _, event: &MultiSelectChanged, _window, _cx| {
                        this.changes.push(event.selected_values.clone());
                    },
                );

                Owner {
                    multi_select,
                    changes: Vec::new(),
                    _subscription: subscription,
                }
            });
            window.run_until_parked();

            (capture, owner, window)
        }

        /// Redraw the window so the next frame reflects the latest state.
        fn settle_frame(window: &mut VisualTestContext) {
            window.update(|window, _| window.refresh());
            window.run_until_parked();
        }

        fn center_of(capture: &FrameCapture, id: &str) -> Point<Pixels> {
            let guard = capture.0.lock().expect("frame capture lock");
            let frame = guard.as_ref().expect("the window rendered a frame");

            let found = frame
                .nodes()
                .find(|(_, node)| node.id() == id)
                .map(|(_, node)| node.bounds().center());

            found.unwrap_or_else(|| {
                let ids: Vec<&str> = frame.nodes().map(|(_, node)| node.id()).collect();
                panic!("element {id} is not rendered; frame holds {ids:?}")
            })
        }

        fn press_and_release(window: &mut VisualTestContext, position: Point<Pixels>) {
            window.simulate_mouse_down(position, MouseButton::Left, Modifiers::none());
            window.run_until_parked();
            window.simulate_mouse_up(position, MouseButton::Left, Modifiers::none());
            settle_frame(window);
        }

        fn is_open(owner: &Entity<Owner>, window: &mut VisualTestContext) -> bool {
            window.update(|_, cx| owner.read(cx).multi_select.read(cx).is_open())
        }

        fn open_menu(
            capture: &FrameCapture,
            owner: &Entity<Owner>,
            window: &mut VisualTestContext,
        ) {
            let trigger = center_of(capture, "ms-trigger");
            press_and_release(window, trigger);
            assert!(
                is_open(owner, window),
                "a click on the trigger opens the list"
            );
        }

        #[gpui::test]
        fn clicking_a_row_toggles_it_and_keeps_the_list_open(cx: &mut TestAppContext) {
            let (capture, owner, window) = setup(cx);
            open_menu(&capture, &owner, window);

            let row = center_of(&capture, "ms-menu-rows.ms-item-1");
            press_and_release(window, row);

            let (values, changes) = window.update(|_, cx| {
                let owner = owner.read(cx);
                (
                    owner.multi_select.read(cx).selected_values(),
                    owner.changes.clone(),
                )
            });
            assert_eq!(values, vec![SharedString::from("Two")]);
            assert_eq!(changes, vec![vec![SharedString::from("Two")]]);
            assert!(
                is_open(&owner, window),
                "the list stays open after a toggle"
            );
        }

        #[gpui::test]
        fn pressing_outside_the_list_closes_it(cx: &mut TestAppContext) {
            let (capture, owner, window) = setup(cx);
            open_menu(&capture, &owner, window);

            let viewport = window.update(|window, _| window.viewport_size());
            let outside = point(viewport.width * 0.95, viewport.height * 0.95);
            press_and_release(window, outside);

            assert!(!is_open(&owner, window), "a press outside closes the list");
        }

        #[gpui::test]
        fn clicking_the_trigger_while_open_closes_the_list(cx: &mut TestAppContext) {
            let (capture, owner, window) = setup(cx);
            open_menu(&capture, &owner, window);

            let trigger = center_of(&capture, "ms-trigger");
            press_and_release(window, trigger);

            assert!(
                !is_open(&owner, window),
                "a second trigger click closes the list"
            );
        }
    }

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
