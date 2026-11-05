//! A select: a trigger that draws the current choice and a menu of the
//! choices, opened over the trigger.
//!
//! An open menu is found and driven by id, so automation and tests pick a row
//! by name instead of by position:
//!
//! - `<id>-menu` is the menu surface the rows are drawn in.
//! - `<id>-menu-rows` is the scrolling column of rows inside it.
//! - `<id>-item-<n>` is the `n`th row, numbered from 0 in the order of the
//!   items the dropdown was built with. Clicking that element selects that row,
//!   exactly as clicking the row itself does.
//!
//! The menu keys come from the product's keymap through [`crate::keymap`]: the
//! dropdown answers the actions the installed [`CommandSet`] maps onto its menu
//! commands and propagates the ones it does not own.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;

use gpui::prelude::*;
use gpui::{
    Anchor, App, ClickEvent, Context, ElementId, EventEmitter, FocusHandle, FontWeight, Hsla,
    InteractiveElement, IntoElement, ParentElement, Pixels, Render, Role, ScrollHandle,
    ScrollWheelEvent, SharedString, StatefulInteractiveElement, Styled, Subscription, Window,
    anchored, canvas, deferred, div, point, px,
};
use gpui_component::{ActiveTheme, IconName};

use crate::controls::select_trigger::{SelectField, select_field};
use crate::controls::{ButtonVariant, button_colors};
use crate::density;
use crate::icon::IconSource;
use crate::keymap::{Command, CommandSet};
use crate::primitives::{BadgeTone, Chamfer, Icon};
use crate::selector::id_selector;
use crate::tokens::{ChamferCut, ChromeColors, ChromeEdgeRole, Feedback, Fields, Heights, Spacing};

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
    /// Drawn before the label of the item's row in the open `Dropdown` menu.
    /// `MultiSelect` takes the same items and draws only their labels, so an
    /// item built with an icon shows none there. A product that shows the same
    /// set of sources in its rows and in its trigger (the connection selector)
    /// sets it per item, so a row says which driver it belongs to instead of
    /// relying on the label alone.
    pub icon: Option<IconSource>,
    /// Tone for [`DropdownItem::icon`], resolved against the theme when the row
    /// draws, so a light and dark switch recolors the icon of a menu that is
    /// already built. The muted foreground when unset.
    pub icon_tone: Option<BadgeTone>,
}

impl DropdownItem {
    #[allow(dead_code)]
    pub fn new(label: impl Into<SharedString>) -> Self {
        let label = label.into();
        Self {
            value: label.clone(),
            label,
            icon: None,
            icon_tone: None,
        }
    }

    pub fn with_value(label: impl Into<SharedString>, value: impl Into<SharedString>) -> Self {
        Self {
            label: label.into(),
            value: value.into(),
            icon: None,
            icon_tone: None,
        }
    }

    /// Draws `icon` before the row's label, in the muted color unless
    /// [`DropdownItem::icon_tone`] says otherwise.
    pub fn icon(mut self, icon: impl Into<IconSource>) -> Self {
        self.icon = Some(icon.into());
        self
    }

    pub fn icon_tone(mut self, tone: BadgeTone) -> Self {
        self.icon_tone = Some(tone);
        self
    }
}

/// The key of the placeholder a dropdown with no selection and no explicit
/// `.placeholder` draws; the product's catalogue answers it through
/// [`crate::strings`].
const PLACEHOLDER_KEY: &str = "controls.dropdown.placeholder";

fn dropdown_placeholder(cx: &App) -> SharedString {
    crate::strings::text(cx, PLACEHOLDER_KEY)
}

/// A small uppercase tag on a tone wash, drawn inside a control's label row
/// (the environment badge of a connection selector).
///
/// The text and the tone are data, so the hosting product decides what the
/// tag means; the dropdown only draws it between the label and the chevron.
#[derive(Clone, Debug, PartialEq, Eq, IntoElement)]
pub struct BadgeLabel {
    /// The tag text, uppercased when drawn.
    pub label: SharedString,
    pub tone: BadgeTone,
}

impl RenderOnce for BadgeLabel {
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let fill = self.tone.fill(theme, Feedback::ENV_TAG_FILL_ALPHA);
        let text_color = self.tone.text_color(theme);
        let label: SharedString = self.label.to_uppercase().into();

        div()
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .px(Feedback::ENV_TAG_PADDING_X)
            .py(Feedback::ENV_TAG_PADDING_Y)
            .child(Chamfer::new(ChamferCut::KEYCAP).fill(fill))
            .child(
                div()
                    .whitespace_nowrap()
                    .text_size(Feedback::ENV_TAG_FONT)
                    .font_weight(FontWeight::BOLD)
                    .letter_spacing(
                        Feedback::ENV_TAG_FONT.to_pixels(window.rem_size())
                            * Feedback::ENV_TAG_TRACKING_EM,
                    )
                    .text_color(text_color)
                    .child(label),
            )
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

/// One of the menu keys the installed [`CommandSet`] names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MenuKey {
    SelectNext,
    SelectPrev,
    Confirm,
    Cancel,
}

fn menu_key(keys: &CommandSet, action_id: &str) -> Option<MenuKey> {
    match keys.command(action_id)? {
        Command::SelectNext => Some(MenuKey::SelectNext),
        Command::SelectPrev => Some(MenuKey::SelectPrev),
        Command::Execute | Command::ExpandCollapse => Some(MenuKey::Confirm),
        Command::Cancel => Some(MenuKey::Cancel),
        Command::FocusSearch | Command::Save | Command::OpenInNewTab => None,
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
    placeholder: Option<SharedString>,
    aria_label: Option<SharedString>,
    focus_ring_color: Option<Hsla>,
    focus_ring_visible: bool,
    compact_trigger: bool,
    chevron_trigger: Option<ButtonVariant>,
    toolbar_style: bool,
    mono_label: bool,
    leading_icon: Option<IconSource>,
    label_badge: Option<BadgeLabel>,
    menu_scroll_handle: ScrollHandle,
    /// The trigger's laid-out width, recorded while it paints. The menu is
    /// anchored and sizes to its content, so it cannot resolve a percentage
    /// against the trigger and takes this as its minimum width instead.
    trigger_width: Rc<Cell<Pixels>>,
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

    fn menu_rows_debug_selector(&self) -> String {
        format!("{}-menu-rows", self.id)
    }

    fn item_debug_selector(&self, index: usize) -> String {
        format!("{}-item-{index}", self.id)
    }

    pub fn new(id: impl Into<ElementId>) -> Self {
        Self {
            id: id.into(),
            items: Vec::new(),
            open: false,
            selected_index: None,
            highlighted_index: None,
            disabled: false,
            placeholder: None,
            aria_label: None,
            focus_ring_color: None,
            focus_ring_visible: false,
            compact_trigger: false,
            chevron_trigger: None,
            toolbar_style: false,
            mono_label: false,
            leading_icon: None,
            label_badge: None,
            menu_scroll_handle: ScrollHandle::new(),
            trigger_width: Rc::new(Cell::new(px(0.0))),
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
        self.placeholder = Some(placeholder.into());
        self
    }

    /// Names the control for assistive technology, overriding the name it takes
    /// from its placeholder.
    ///
    /// A dropdown whose trigger draws only an icon (the chevron variant) has no
    /// text of its own to be named by, so it needs this; one that draws its
    /// label is already named by that text.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.aria_label = Some(label.into());
        self
    }

    #[cfg(test)]
    pub(crate) fn placeholder_text(&self) -> Option<&SharedString> {
        self.placeholder.as_ref()
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
    pub fn leading_icon(mut self, icon: impl Into<IconSource>) -> Self {
        self.leading_icon = Some(icon.into());
        self
    }

    /// Draws `badge` (a short uppercase tag, such as an environment label)
    /// between the toolbar trigger's label and its chevron, or removes it
    /// with `None`.
    pub fn set_label_badge(&mut self, badge: Option<BadgeLabel>, cx: &mut Context<Self>) {
        if self.label_badge != badge {
            self.label_badge = badge;
            cx.notify();
        }
    }

    pub fn label_badge(&self) -> Option<&BadgeLabel> {
        self.label_badge.as_ref()
    }

    /// The index of the item the keyboard highlight sits on while the menu
    /// is open.
    pub fn highlighted_index(&self) -> Option<usize> {
        self.highlighted_index
    }

    /// The index of the selected item.
    pub fn selected(&self) -> Option<usize> {
        self.selected_index
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

    /// Moves keyboard focus to the dropdown, so it answers the menu keys
    /// itself: Enter or Space opens it, j / k and the arrows move, Enter or
    /// Space confirms and Escape closes. Confirming or closing from the
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

    /// Answers the menu commands of the installed [`crate::keymap::CommandSet`]
    /// while the dropdown has focus. Answers whether the command was
    /// handled; the attach hook propagates the ones it was not.
    pub(crate) fn handle_key_command(
        &mut self,
        action_id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(key) = cx
            .try_global::<CommandSet>()
            .and_then(|keys| menu_key(keys, action_id))
        else {
            return false;
        };

        match key {
            MenuKey::SelectNext if self.open => {
                self.select_next_item(cx);
                true
            }
            MenuKey::SelectPrev if self.open => {
                self.select_prev_item(cx);
                true
            }
            MenuKey::Confirm if self.open => {
                self.accept_selection(cx);
                self.give_focus_back(window, cx);
                true
            }
            MenuKey::Confirm if !self.disabled && !self.items.is_empty() => {
                self.open(cx);
                true
            }
            MenuKey::Cancel if self.open => {
                self.close(cx);
                cx.emit(DropdownDismissed);
                self.give_focus_back(window, cx);
                true
            }
            MenuKey::Cancel if self.return_focus.is_some() => {
                self.give_focus_back(window, cx);
                true
            }
            _ => false,
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
                let item_selector = self.item_debug_selector(index);

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

                let icon_selector = format!("{item_selector}-icon");
                let label_selector = format!("{item_selector}-label");
                let icon = item.icon.clone();
                let icon_color = item
                    .icon_tone
                    .map(|tone| tone.text_color(theme))
                    .unwrap_or(theme.muted_foreground);

                let mut row = div()
                    .id(SharedString::from(item_selector.clone()))
                    .debug_selector(move || item_selector)
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
                    .child(row_shape);

                if let Some(icon) = icon {
                    row = row.gap(Fields::GAP).child(
                        div()
                            .flex_none()
                            .debug_selector(move || icon_selector)
                            .child(Icon::new(icon).size(Fields::LEADING_ICON).color(icon_color)),
                    );
                }

                row = row.child(
                    div()
                        .debug_selector(move || label_selector)
                        .child(item.label.clone()),
                );

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
        let rows_debug_selector = self.menu_rows_debug_selector();
        let rows_id = SharedString::from(rows_debug_selector.clone());

        let rows = div()
            .id(rows_id)
            .debug_selector(move || rows_debug_selector)
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
            .min_w(self.trigger_width.get())
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
            .relative()
            .role(Role::Button)
            // The trigger is named by the text it draws (the selection, or the
            // placeholder), unless the owner names it explicitly: a chevron
            // trigger draws nothing, and its value alone rarely says what the
            // control does.
            .aria_label(self.aria_label.clone().unwrap_or_else(|| label.clone()))
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
                    Icon::new(IconName::ChevronDown)
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
                    .font_weight(FontWeight::MEDIUM)
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
                    .font_weight(FontWeight::MEDIUM)
                    .text_size(Fields::TEXT)
                    .child(div().flex_1().truncate().child(label))
                    .when_some(self.label_badge.clone(), |trigger, badge| {
                        trigger.child(badge)
                    })
                    .child(
                        Icon::new(IconName::ChevronDown)
                            .size(Fields::CHEVRON)
                            .color(theme.muted_foreground),
                    );
            }
            DropdownTriggerVariant::Standard => {
                let ring = (self.focus_ring_visible || keyboard_focused).then(|| {
                    self.focus_ring_color
                        .unwrap_or_else(|| ChromeColors::tint(theme))
                });

                trigger = select_field(
                    trigger,
                    SelectField {
                        label,
                        label_style: Default::default(),
                        leading_icon: self.leading_icon.clone(),
                        disabled,
                        ring,
                        shape_id: "dropdown-trigger-shape".into(),
                    },
                    cx,
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

        let trigger_width = self.trigger_width.clone();
        let entity_id = cx.entity_id();
        trigger
            .child(
                canvas(
                    move |bounds, window, cx| {
                        // The menu read the width before this prepaint, so a
                        // changed width needs another frame. Notifying is a no-op
                        // while drawing, hence the defer. An unchanged width must
                        // not ask for one, or the window redraws forever.
                        if trigger_width.replace(bounds.size.width) != bounds.size.width {
                            window.defer(cx, move |_window, cx| cx.notify(entity_id));
                        }
                    },
                    |_, _, _, _| {},
                )
                .absolute()
                .size_full(),
            )
            .into_any_element()
    }

    fn resolved_placeholder(&self, cx: &App) -> SharedString {
        self.placeholder
            .clone()
            .unwrap_or_else(|| dropdown_placeholder(cx))
    }
}

impl Render for Dropdown {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_disabled = self.disabled;
        let disabled = is_disabled || self.items.is_empty();
        let label = self
            .selected_label()
            .unwrap_or_else(|| self.resolved_placeholder(cx));
        let variant = self.trigger_variant();
        let keyboard_focused = self.is_focused(window);
        let trigger = self.render_trigger(label, disabled, keyboard_focused, cx);

        let mut element = div()
            .id(self.id.clone())
            .debug_selector(id_selector(&self.id))
            .when_some(self.focus_handle.as_ref(), |element, handle| {
                element.track_focus(handle)
            })
            .w_full()
            .when(
                matches!(
                    variant,
                    DropdownTriggerVariant::Compact | DropdownTriggerVariant::Chevron
                ),
                |el| el.h_full(),
            )
            .child(trigger)
            .child(self.render_menu(cx));

        // The product's keymap seam, installed once per app: the dropdown
        // answers the keymap action under its key context and propagates
        // the keys it does not own. Without a seam it answers none.
        if let Some(keys) = cx.try_global::<CommandSet>() {
            let attach = keys.attach;
            let key_context = keys.key_context.clone();
            let handle = cx.entity().downgrade();
            let handler: crate::keymap::KeyCommandHandler =
                Rc::new(move |action_id, window, cx| {
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

impl EventEmitter<DropdownSelectionChanged> for Dropdown {}
impl EventEmitter<DropdownDismissed> for Dropdown {}

#[cfg(test)]
mod tests;
