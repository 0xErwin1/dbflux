use std::sync::Arc;

use crate::actions::RunCommand;
use crate::controls::{ButtonVariant, button_colors};
use crate::density;
use crate::icons::AppIcon;
use crate::primitives::{Chamfer, ChamferRing, EnvTag, Icon};
use crate::tokens::{ChamferCut, ChromeColors, ChromeEdgeRole, Fields, Heights, Spacing};
use dbflux_core::ConnectionEnvironment;
use dbflux_core::keymap_types::{Command, ContextId};
use gpui::prelude::*;
use gpui::{
    Anchor, ClickEvent, Context, ElementId, EventEmitter, FocusHandle, Hsla, InteractiveElement,
    IntoElement, ParentElement, Pixels, Render, ScrollHandle, ScrollWheelEvent, SharedString,
    StatefulInteractiveElement, Styled, Subscription, Window, anchored, deferred, div, point, px,
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
    /// A bare chevron that inherits the text color of the control hosting
    /// it and draws no hover of its own: the menu segment of a split button.
    Chevron,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DropdownTriggerRenderPlan {
    /// The trigger draws its own chamfered field shape and focus ring.
    uses_chamfer_shape: bool,
}

fn dropdown_trigger_render_plan(variant: DropdownTriggerVariant) -> DropdownTriggerRenderPlan {
    match variant {
        DropdownTriggerVariant::Standard => DropdownTriggerRenderPlan {
            uses_chamfer_shape: true,
        },
        DropdownTriggerVariant::Toolbar
        | DropdownTriggerVariant::Compact
        | DropdownTriggerVariant::Chevron => DropdownTriggerRenderPlan {
            uses_chamfer_shape: false,
        },
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

#[derive(Clone, Copy, Debug, PartialEq)]
struct DropdownMenuChromeInspection {
    edge: ChromeEdgeRole,
    cut: Pixels,
}

fn dropdown_menu_chrome() -> DropdownMenuChromeInspection {
    DropdownMenuChromeInspection {
        edge: ChromeEdgeRole::Control,
        cut: ChamferCut::OVERLAY,
    }
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
    chevron_trigger: Option<ButtonVariant>,
    toolbar_style: bool,
    mono_label: bool,
    leading_icon: Option<AppIcon>,
    label_environment: Option<ConnectionEnvironment>,
    menu_scroll_handle: ScrollHandle,
    on_select: Option<Arc<dyn Fn(usize, &DropdownItem, &mut Context<Self>) + Send + Sync>>,
    /// Created the first time the dropdown is focused from the keyboard
    /// (see [`Dropdown::focus`]); a dropdown only ever driven by its owner
    /// never takes focus.
    focus_handle: Option<FocusHandle>,
    /// Where focus was before [`Dropdown::focus`], given back when the menu
    /// is confirmed or dismissed from the keyboard.
    return_focus: Option<FocusHandle>,
    _focus_out: Option<Subscription>,
}

#[allow(dead_code)]
impl Dropdown {
    const PAGE_STEP: usize = 8;

    fn menu_debug_selector(&self) -> String {
        format!("{}-menu", self.id)
    }

    pub fn new(id: impl Into<ElementId>) -> Self {
        Self {
            id: id.into(),
            items: Vec::new(),
            open: false,
            selected_index: None,
            highlighted_index: None,
            disabled: false,
            placeholder: dbflux_i18n::t!("controls.dropdown.placeholder").into(),
            focus_ring_color: None,
            focus_ring_visible: false,
            compact_trigger: false,
            chevron_trigger: None,
            toolbar_style: false,
            mono_label: false,
            leading_icon: None,
            label_environment: None,
            menu_scroll_handle: ScrollHandle::new(),
            on_select: None,
            focus_handle: None,
            return_focus: None,
            _focus_out: None,
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

    #[cfg(test)]
    pub(crate) fn placeholder_text(&self) -> &SharedString {
        &self.placeholder
    }

    pub fn selected_index(mut self, index: Option<usize>) -> Self {
        self.selected_index = index;
        self
    }

    pub fn disabled(mut self, disabled: bool) -> Self {
        self.disabled = disabled;
        self
    }

    pub fn set_disabled(&mut self, disabled: bool, cx: &mut Context<Self>) {
        if self.disabled != disabled {
            self.disabled = disabled;
            cx.notify();
        }
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

    /// Renders the trigger as a lone chevron in the content color of a
    /// button of `variant`, with no hover fill of its own, for a dropdown
    /// hosted in the menu segment of a `SplitButton` of that variant, whose
    /// shape already draws the hover and pressed states. Takes precedence
    /// over the compact and toolbar triggers.
    pub fn chevron_trigger(mut self, variant: ButtonVariant) -> Self {
        self.chevron_trigger = Some(variant);
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

    /// Sets the toolbar trigger's label in the monospace face, for values
    /// that are identifiers (a connection, database or schema name).
    pub fn mono_label(mut self, mono: bool) -> Self {
        self.mono_label = mono;
        self
    }

    /// Draws `icon` in the muted color before the label of the standard
    /// trigger, as the toolbar selects do (a globe before the timezone).
    pub fn leading_icon(mut self, icon: AppIcon) -> Self {
        self.leading_icon = Some(icon);
        self
    }

    /// Draws the environment tag between the toolbar trigger's label and its
    /// chevron (AppByzEditor connection selector), or removes it with `None`.
    pub fn set_label_environment(
        &mut self,
        environment: Option<ConnectionEnvironment>,
        cx: &mut Context<Self>,
    ) {
        if self.label_environment != environment {
            self.label_environment = environment;
            cx.notify();
        }
    }

    pub fn label_environment(&self) -> Option<ConnectionEnvironment> {
        self.label_environment
    }

    fn trigger_variant(&self) -> DropdownTriggerVariant {
        if self.chevron_trigger.is_some() {
            DropdownTriggerVariant::Chevron
        } else if self.compact_trigger {
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

    /// Moves keyboard focus to the dropdown, so it answers the `Dropdown`
    /// keys itself: Enter or Space opens it, j / k and the arrows move, Enter
    /// or Space confirms and Escape closes. Confirming or closing from the
    /// keyboard gives focus back to the element that held it before.
    ///
    /// An owner that drives the dropdown from its own key handling (a
    /// focus ring over several controls) never calls this, so the keys keep
    /// reaching the owner.
    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let handle = self.ensure_focus_handle(window, cx);

        if !handle.contains_focused(window, cx) {
            self.return_focus = window.focused(cx);
        }

        handle.focus(window, cx);
        cx.notify();
    }

    /// [`Dropdown::focus`], then opens the menu.
    pub fn focus_and_open(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus(window, cx);
        self.open(cx);
    }

    /// Whether keyboard focus is on the dropdown.
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

        // Focus leaving by any other route (Tab, a click elsewhere) closes the
        // menu and forgets the element to go back to.
        self._focus_out = Some(
            cx.on_focus_out(&handle, window, |this, _event, _window, cx| {
                this.return_focus = None;

                if let Some(transition) = dropdown_dismiss_transition(this.open) {
                    this.open = transition.open;
                    this.highlighted_index = transition.highlighted_index;
                    cx.emit(DropdownDismissed);
                    cx.notify();
                }
            }),
        );

        self.focus_handle = Some(handle.clone());
        handle
    }

    fn give_focus_back(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(previous) = self.return_focus.take() else {
            return;
        };

        if self.is_focused(window) {
            previous.focus(window, cx);
        }
    }

    /// Answers the `Dropdown` keys while the dropdown has focus. A key it
    /// has no use for in its current state propagates to the pane around it.
    fn handle_run_command(
        &mut self,
        action: &RunCommand,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let handled = match Command::from_action_id(&action.command) {
            Some(Command::SelectNext) if self.open => {
                self.select_next_item(cx);
                true
            }
            Some(Command::SelectPrev) if self.open => {
                self.select_prev_item(cx);
                true
            }
            Some(Command::Execute | Command::ExpandCollapse) if self.open => {
                self.accept_selection(cx);
                self.give_focus_back(window, cx);
                true
            }
            Some(Command::Execute | Command::ExpandCollapse)
                if !self.disabled && !self.items.is_empty() =>
            {
                self.open(cx);
                true
            }
            Some(Command::Cancel) if self.open => {
                self.close(cx);
                cx.emit(DropdownDismissed);
                self.give_focus_back(window, cx);
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

    fn handle_trigger_click(
        &mut self,
        _event: &ClickEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        cx.stop_propagation();
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
        let chrome = dropdown_menu_chrome();

        let menu_font_size = density::font_base(cx);
        let highlight_fill = ChromeColors::tint(theme).opacity(Fields::MENU_HIGHLIGHT_ALPHA);

        let items: Vec<gpui::AnyElement> = self
            .items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let is_highlighted = self.highlighted_index == Some(index);

                let row_shape = if is_highlighted {
                    Chamfer::new(ChamferCut::KEYCAP).fill(highlight_fill)
                } else if is_disabled {
                    Chamfer::new(ChamferCut::KEYCAP)
                } else {
                    Chamfer::new(ChamferCut::KEYCAP)
                        .fill_hover(theme.list_hover)
                        .interactive(("dropdown-row-shape", index))
                };

                let text_color = if is_disabled {
                    theme.muted_foreground
                } else if is_highlighted {
                    theme.accent_foreground
                } else {
                    theme.foreground
                };

                let mut row = div()
                    .id(index)
                    .relative()
                    .flex()
                    .items_center()
                    .h(Fields::MENU_ROW_HEIGHT)
                    .mx(Fields::MENU_ROW_INSET)
                    .px(Fields::PADDING_X)
                    .font_family(crate::fonts::ui_family(cx))
                    .text_size(menu_font_size)
                    .whitespace_nowrap()
                    .text_color(text_color)
                    .child(row_shape)
                    .child(item.label.clone());

                if is_disabled {
                    row = row.cursor_not_allowed();
                } else {
                    row = row.cursor_pointer().on_click(cx.listener(
                        move |this, _event, window, cx| {
                            this.select_item(index, cx);
                            this.give_focus_back(window, cx);
                        },
                    ));
                }

                row.into_any_element()
            })
            .collect();

        let menu_debug_selector = self.menu_debug_selector();

        let rows = div()
            .id("dropdown-menu-rows")
            .max_h(Fields::MENU_MAX_HEIGHT)
            .py(Fields::MENU_PADDING_Y)
            .overflow_y_scroll()
            .track_scroll(&self.menu_scroll_handle)
            .on_scroll_wheel(cx.listener(Self::handle_menu_scroll_wheel))
            .children(items);

        let menu = div()
            .id("dropdown-menu")
            .debug_selector(move || menu_debug_selector.clone())
            .relative()
            .min_w_full()
            .shadow_lg()
            .occlude()
            .on_mouse_down_out(cx.listener(Self::handle_mouse_down_out))
            .child(
                Chamfer::new(chrome.cut)
                    .fill(theme.secondary)
                    .border(chrome.edge.resolve(theme)),
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

    fn render_trigger(
        &self,
        label: SharedString,
        disabled: bool,
        keyboard_focused: bool,
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
            .when(disabled, |el| el.cursor_not_allowed())
            .when(!disabled, |el| el.cursor_pointer());

        if !render_plan.uses_chamfer_shape && variant != DropdownTriggerVariant::Chevron {
            trigger = trigger
                .when(disabled, |el| {
                    el.text_color(theme.muted_foreground).opacity(0.5)
                })
                .when(!disabled, |el| {
                    el.text_color(theme.foreground)
                        .hover(|s| s.bg(theme.accent.opacity(0.1)))
                });
        }

        let font_sm = density::font_sm(cx);

        match variant {
            DropdownTriggerVariant::Chevron => {
                let host_variant = self.chevron_trigger.unwrap_or(ButtonVariant::Secondary);
                let (_, content_color) = button_colors(theme, host_variant, false);

                trigger = trigger.h_full().justify_center().child(
                    Icon::new(AppIcon::ChevronDown)
                        .size(Fields::SEGMENT_ICON)
                        .color(content_color),
                );
            }
            DropdownTriggerVariant::Compact => {
                trigger = trigger
                    .h_full()
                    .justify_center()
                    .px(Spacing::SM)
                    .font_family(crate::fonts::ui_family(cx))
                    .font_weight(gpui::FontWeight::MEDIUM)
                    .text_size(font_sm)
                    .child(
                        div()
                            .text_size(font_sm)
                            .text_color(theme.muted_foreground)
                            .child("▾"),
                    );
            }
            DropdownTriggerVariant::Toolbar => {
                trigger = trigger
                    .justify_between()
                    .gap(Fields::GAP)
                    .font_family(if self.mono_label {
                        crate::fonts::editor_family(cx)
                    } else {
                        crate::fonts::ui_family(cx)
                    })
                    .font_weight(gpui::FontWeight::MEDIUM)
                    .text_size(Fields::TEXT)
                    .child(div().flex_1().truncate().child(label))
                    .when_some(self.label_environment, |trigger, environment| {
                        trigger.child(EnvTag::for_environment(environment))
                    })
                    .child(
                        Icon::new(AppIcon::ChevronDown)
                            .size(Fields::CHEVRON)
                            .color(theme.muted_foreground),
                    );
            }
            DropdownTriggerVariant::Standard => {
                let opacity = if disabled {
                    Fields::DISABLED_OPACITY
                } else {
                    1.0
                };

                let mut shape = Chamfer::new(ChamferCut::CONTROL)
                    .fill(theme.secondary.opacity(opacity))
                    .border(theme.border.opacity(opacity));

                if !disabled {
                    shape = shape
                        .fill_hover(theme.secondary_hover)
                        .interactive("dropdown-trigger-shape");
                }

                if self.focus_ring_visible || keyboard_focused {
                    shape = shape.ring(ChamferRing::focus(
                        self.focus_ring_color
                            .unwrap_or_else(|| ChromeColors::tint(theme)),
                    ));
                }

                let text_color = if disabled {
                    theme.muted_foreground
                } else {
                    theme.accent_foreground
                };

                trigger = trigger
                    .relative()
                    .h(Fields::HEIGHT)
                    .gap(Fields::GAP)
                    .px(Fields::PADDING_X)
                    .font_family(crate::fonts::ui_family(cx))
                    .text_size(Fields::TEXT)
                    .text_color(text_color)
                    .child(shape)
                    .when_some(self.leading_icon, |trigger, icon| {
                        trigger.child(
                            Icon::new(icon)
                                .size(Fields::LEADING_ICON)
                                .color(theme.muted_foreground),
                        )
                    })
                    .child(div().flex_1().truncate().child(label))
                    .child(
                        Icon::new(AppIcon::ChevronDown)
                            .size(Fields::CHEVRON)
                            .color(theme.muted_foreground),
                    );
            }
        }

        // Only the closed trigger toggles open. While open, dismissal is owned
        // by the menu's on_mouse_down_out: a click on the trigger lands outside
        // the (deferred, snap-positioned) menu and closes it. Keeping on_click
        // here while open would re-toggle (down dismisses, up reopens),
        // producing the open/close flicker that surfaced after window resize.
        if !disabled && !self.open {
            trigger = trigger.on_click(cx.listener(Self::handle_trigger_click));
        }

        trigger.into_any_element()
    }
}

impl Render for Dropdown {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_disabled = self.disabled;
        let disabled = is_disabled || self.items.is_empty();
        let label = self
            .selected_label()
            .unwrap_or_else(|| self.placeholder.clone());
        let variant = self.trigger_variant();
        let keyboard_focused = self.is_focused(window);
        let trigger = self.render_trigger(label, disabled, keyboard_focused, cx);

        div()
            .id(self.id.clone())
            .debug_selector({
                let id = self.id.to_string();
                move || id.clone()
            })
            .key_context(ContextId::Dropdown.as_gpui_context())
            .when_some(self.focus_handle.as_ref(), |element, handle| {
                element.track_focus(handle)
            })
            .on_action(cx.listener(Self::handle_run_command))
            .w_full()
            .when(
                matches!(
                    variant,
                    DropdownTriggerVariant::Compact | DropdownTriggerVariant::Chevron
                ),
                |el| el.h_full(),
            )
            .child(trigger)
            .child(self.render_menu(cx))
    }
}

impl EventEmitter<DropdownSelectionChanged> for Dropdown {}
impl EventEmitter<DropdownDismissed> for Dropdown {}

/// The keys the app keymap binds in the `Dropdown` context (see the dropdown
/// layer in `dbflux_ui_base::keymap`), for component tests that run without
/// the app keymap.
#[cfg(test)]
pub(crate) fn bind_dropdown_keys_for_tests(cx: &mut gpui::TestAppContext) {
    use gpui::KeyBinding;

    let context = Some(ContextId::Dropdown.as_gpui_context());
    let run = |command: Command| RunCommand::new(command.id());

    cx.update(|cx| {
        cx.bind_keys([
            KeyBinding::new("j", run(Command::SelectNext), context),
            KeyBinding::new("down", run(Command::SelectNext), context),
            KeyBinding::new("k", run(Command::SelectPrev), context),
            KeyBinding::new("up", run(Command::SelectPrev), context),
            KeyBinding::new("enter", run(Command::Execute), context),
            KeyBinding::new("space", run(Command::ExpandCollapse), context),
            KeyBinding::new("escape", run(Command::Cancel), context),
        ]);
    });
}

#[cfg(test)]
mod tests {
    use super::{
        Dropdown, DropdownTriggerVariant, dropdown_dismiss_transition, dropdown_focus_ring_state,
        dropdown_menu_chrome, dropdown_selection_transition, dropdown_trigger_render_plan,
    };
    use crate::tokens::{ChamferCut, ChromeEdgeRole};

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
    fn standard_trigger_draws_its_chamfered_shape() {
        let plan = dropdown_trigger_render_plan(DropdownTriggerVariant::Standard);

        assert!(plan.uses_chamfer_shape);
    }

    #[test]
    fn toolbar_trigger_skips_the_chamfered_shape() {
        let plan = dropdown_trigger_render_plan(DropdownTriggerVariant::Toolbar);

        assert!(!plan.uses_chamfer_shape);
    }

    #[test]
    fn compact_trigger_skips_the_chamfered_shape() {
        let plan = dropdown_trigger_render_plan(DropdownTriggerVariant::Compact);

        assert!(!plan.uses_chamfer_shape);
    }

    #[test]
    fn chevron_trigger_takes_precedence_and_skips_the_chamfered_shape() {
        let dropdown = Dropdown::new("chevron-trigger")
            .toolbar_style(true)
            .compact_trigger(true)
            .chevron_trigger(crate::controls::ButtonVariant::Primary);

        assert_eq!(dropdown.trigger_variant(), DropdownTriggerVariant::Chevron);
        assert!(!dropdown_trigger_render_plan(DropdownTriggerVariant::Chevron).uses_chamfer_shape);
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

    #[test]
    fn dropdown_menu_uses_the_overlay_cut_and_the_strong_line() {
        let chrome = dropdown_menu_chrome();

        assert_eq!(chrome.edge, ChromeEdgeRole::Control);
        assert_eq!(chrome.cut, ChamferCut::OVERLAY);
    }

    /// Keyboard tests against a real window: the keys the app keymap binds
    /// in the `Dropdown` context are bound here the same way (see
    /// `bind_dropdown_keys_for_tests`), and an owner view stands in for the
    /// pane hosting the dropdown and records every command that reaches it.
    mod keyboard {
        use super::super::{
            Dropdown, DropdownItem, DropdownSelectionChanged, bind_dropdown_keys_for_tests,
        };
        use crate::actions::RunCommand;
        use dbflux_core::keymap_types::Command;
        use gpui::{
            AppContext as _, Context, Entity, FocusHandle, InteractiveElement as _, IntoElement,
            ParentElement as _, Render, Styled as _, TestAppContext, VisualTestContext, Window,
            div,
        };

        struct Owner {
            focus: FocusHandle,
            dropdown: Entity<Dropdown>,
            commands: Vec<Command>,
            selections: Vec<usize>,
            _subscription: gpui::Subscription,
        }

        impl Render for Owner {
            fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
                div()
                    .size_full()
                    .key_context("Owner")
                    .track_focus(&self.focus)
                    .on_action(cx.listener(|this, action: &RunCommand, _window, _cx| {
                        if let Some(command) = Command::from_action_id(&action.command) {
                            this.commands.push(command);
                        }
                    }))
                    .child(self.dropdown.clone())
            }
        }

        struct Setup<'a> {
            owner: Entity<Owner>,
            window: &'a mut VisualTestContext,
        }

        fn setup(cx: &mut TestAppContext) -> Setup<'_> {
            cx.update(gpui_component::init);
            bind_dropdown_keys_for_tests(cx);

            // The owner's own bindings on the same keys, like a pane whose
            // key handling drives a dropdown it keeps unfocused.
            cx.update(|cx| {
                cx.bind_keys([
                    gpui::KeyBinding::new(
                        "j",
                        RunCommand::new(Command::SelectNext.id()),
                        Some("Owner"),
                    ),
                    gpui::KeyBinding::new(
                        "enter",
                        RunCommand::new(Command::Execute.id()),
                        Some("Owner"),
                    ),
                ]);
            });

            let (owner, window) = cx.add_window_view(|window, cx| {
                let dropdown = cx.new(|_cx| {
                    Dropdown::new("keyboard-dropdown").items(vec![
                        DropdownItem::new("One"),
                        DropdownItem::new("Two"),
                        DropdownItem::new("Three"),
                    ])
                });

                let subscription = cx.subscribe_in(
                    &dropdown,
                    window,
                    |this: &mut Owner, _, event: &DropdownSelectionChanged, _window, _cx| {
                        this.selections.push(event.index);
                    },
                );

                Owner {
                    focus: cx.focus_handle(),
                    dropdown,
                    commands: Vec::new(),
                    selections: Vec::new(),
                    _subscription: subscription,
                }
            });
            window.run_until_parked();

            let setup = Setup { owner, window };
            let owner = setup.owner.clone();
            setup.window.update(|window, cx| {
                let focus = owner.read(cx).focus.clone();
                focus.focus(window, cx);
            });
            setup.window.run_until_parked();

            setup
        }

        impl Setup<'_> {
            fn dropdown(&mut self) -> Entity<Dropdown> {
                let owner = self.owner.clone();
                self.window.update(|_, cx| owner.read(cx).dropdown.clone())
            }

            fn focus_dropdown(&mut self) {
                let dropdown = self.dropdown();
                self.window.update(|window, cx| {
                    dropdown.update(cx, |dropdown, cx| dropdown.focus(window, cx));
                });
                self.window.run_until_parked();
            }

            fn keys(&mut self, keystrokes: &str) {
                self.window.simulate_keystrokes(keystrokes);
                self.window.run_until_parked();
            }

            fn is_open(&mut self) -> bool {
                let dropdown = self.dropdown();
                self.window.update(|_, cx| dropdown.read(cx).is_open())
            }

            fn highlighted(&mut self) -> Option<usize> {
                let dropdown = self.dropdown();
                self.window
                    .update(|_, cx| dropdown.read(cx).highlighted_index)
            }

            fn selected(&mut self) -> Option<usize> {
                let dropdown = self.dropdown();
                self.window.update(|_, cx| dropdown.read(cx).selected_index)
            }

            fn dropdown_focused(&mut self) -> bool {
                let dropdown = self.dropdown();
                self.window
                    .update(|window, cx| dropdown.read(cx).is_focused(window))
            }

            fn owner_focused(&mut self) -> bool {
                let owner = self.owner.clone();
                self.window
                    .update(|window, cx| owner.read(cx).focus.is_focused(window))
            }

            fn commands(&mut self) -> Vec<Command> {
                let owner = self.owner.clone();
                self.window.update(|_, cx| owner.read(cx).commands.clone())
            }

            fn selections(&mut self) -> Vec<usize> {
                let owner = self.owner.clone();
                self.window
                    .update(|_, cx| owner.read(cx).selections.clone())
            }
        }

        #[gpui::test]
        fn a_focused_dropdown_opens_moves_and_selects_by_keys(cx: &mut TestAppContext) {
            let mut setup = setup(cx);
            setup.focus_dropdown();
            assert!(setup.dropdown_focused(), "the dropdown takes focus");

            setup.keys("enter");
            assert!(setup.is_open(), "Enter opens the menu");
            assert_eq!(setup.highlighted(), Some(0));

            setup.keys("j down");
            assert_eq!(setup.highlighted(), Some(2), "j and Down move down");

            setup.keys("k");
            assert_eq!(setup.highlighted(), Some(1), "k moves up");

            setup.keys("enter");
            assert!(!setup.is_open(), "Enter confirms and closes");
            assert_eq!(setup.selected(), Some(1));
            assert_eq!(setup.selections(), vec![1]);
            assert!(setup.owner_focused(), "focus returns to where it came from");
            assert!(setup.commands().is_empty(), "{:?}", setup.commands());
        }

        #[gpui::test]
        fn space_opens_and_escape_closes_without_selecting(cx: &mut TestAppContext) {
            let mut setup = setup(cx);
            setup.focus_dropdown();

            setup.keys("space");
            assert!(setup.is_open(), "Space opens the menu");

            setup.keys("down escape");
            assert!(!setup.is_open(), "Escape closes the menu");
            assert_eq!(setup.selected(), None, "Escape selects nothing");
            assert!(setup.selections().is_empty());
            assert!(setup.owner_focused(), "Escape hands focus back");
            assert!(setup.commands().is_empty(), "{:?}", setup.commands());
        }

        #[gpui::test]
        fn a_closed_focused_dropdown_lets_other_keys_through(cx: &mut TestAppContext) {
            let mut setup = setup(cx);
            setup.focus_dropdown();

            setup.keys("j");

            assert!(!setup.is_open());
            assert_eq!(setup.commands(), vec![Command::SelectNext]);
        }

        #[gpui::test]
        fn an_unfocused_open_dropdown_leaves_the_keys_to_its_owner(cx: &mut TestAppContext) {
            let mut setup = setup(cx);
            let dropdown = setup.dropdown();
            setup
                .window
                .update(|_, cx| dropdown.update(cx, |dropdown, cx| dropdown.open(cx)));

            setup.keys("j enter");

            assert!(setup.is_open(), "the owner drives a dropdown it keeps");
            assert_eq!(setup.highlighted(), Some(0));
            assert_eq!(
                setup.commands(),
                vec![Command::SelectNext, Command::Execute]
            );
        }
    }

    #[test]
    fn dropdown_placeholder_key_resolves() {
        let en = dbflux_i18n::t!("controls.dropdown.placeholder", locale = "en");
        let es = dbflux_i18n::t!("controls.dropdown.placeholder", locale = "es");

        assert!(!en.is_empty() && en != "controls.dropdown.placeholder");
        assert!(!es.is_empty() && es != "controls.dropdown.placeholder");
        assert_ne!(en, es);
    }
}
