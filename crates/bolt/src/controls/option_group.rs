//! `OptionGroup` — a vertical list of mutually-exclusive options.
//!
//! One option is selected at a time and the group reports it, like a radio
//! group. Each option is a radio indicator, a label and an optional muted
//! description under the label, clipped to one line.
//!
//! The indicator reuses [`super::Checkbox`]'s box: the same
//! `Fields::CHECKBOX_SIZE` box and `ChamferCut::KEYCAP` cut, the accent fill
//! with an accent-foreground dot when the option is chosen, the inset control
//! line otherwise, and the same tint focus ring. The ring sits outside the
//! accent fill (`ChamferFillKind::Filled`) and is drawn only on the chosen
//! option, while the group holds keyboard focus.
//!
//! The group is a single focusable stop with the radio-group role, named by
//! [`OptionGroup::aria_label`]. Each option is a radio button named by its
//! label and described by its description, toggled when chosen, and while the
//! group holds keyboard focus the chosen option is the group's focused node,
//! so assistive technology follows the choice as it moves. Up and Down (and
//! Left and Right) move the choice through the enabled options, reporting each
//! one; Space and Enter report the current choice, unless it is disabled; while
//! nothing is chosen yet — a `selected` id matching no option, as an empty
//! string does — the first arrow key chooses the first enabled option in that
//! direction, so the keyboard can make the first choice; a pointer press or the
//! accessibility click action chooses the option it lands on. An arrow key that
//! would move past either end is left to the host, so a form can move to the
//! next field from there. An option marked [`OptionGroupItem::disabled`] is
//! dimmed, is skipped by the arrow keys and answers neither a press nor an
//! accessibility click.
//!
//! Element ids derive from the group name, and a `:` or `%` inside a name is
//! escaped so no two groups or options can share one: `option-group:<group>`
//! for the group, `option-group:<group>:<id>` for an option and
//! `option-group:<group>:<id>:description` for its description (see
//! [`option_group_id`], [`option_group_item_id`] and
//! [`option_group_description_id`]).

use std::rc::Rc;

use gpui::prelude::*;
use gpui::{
    AccessibleAction, AnyElement, App, Div, ElementId, FocusHandle, Hsla, KeyDownEvent, Rems, Role,
    SharedString, Toggled, Window, div, relative,
};
use gpui_component::{ActiveTheme, Theme};

use crate::density;
use crate::primitives::{Chamfer, ChamferFillKind, ChamferRing, Text};
use crate::selector::id_selector;
use crate::tokens::{ChamferCut, ChromeColors, Fields, FontSizes};

type OptionChangeHandler = Rc<dyn Fn(&SharedString, &mut Window, &mut App)>;

/// One option of an [`OptionGroup`].
#[derive(Clone, Debug)]
pub struct OptionGroupItem {
    id: SharedString,
    label: SharedString,
    description: Option<SharedString>,
    disabled: bool,
}

impl OptionGroupItem {
    pub fn new(id: impl Into<SharedString>, label: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            description: None,
            disabled: false,
        }
    }

    /// Draws `description` as a smaller, muted line under the label, clipped
    /// to that one line.
    pub fn description(mut self, description: impl Into<SharedString>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Dims the option and keeps the keyboard and the pointer off it.
    pub fn disabled(mut self, disabled: bool) -> Self {
        self.disabled = disabled;
        self
    }
}

/// The option the arrow key `step` moves to from `selected` among the enabled
/// options of `items`, stopping at either end.
///
/// With no choice yet — `selected` matching none of `items`, as an empty string
/// does — the first arrow reaches the nearest enabled option, so the keyboard
/// can make the first choice: the first enabled option for `step > 0`, the last
/// one for `step < 0`. `None` when no enabled option lies that way.
fn stepped_option<'a>(
    items: &'a [OptionGroupItem],
    selected: &str,
    step: isize,
) -> Option<&'a SharedString> {
    let Some(current) = items.iter().position(|item| item.id.as_ref() == selected) else {
        let first = if step > 0 {
            items.iter().find(|item| !item.disabled)
        } else {
            items.iter().rev().find(|item| !item.disabled)
        };

        return first.map(|item| &item.id);
    };

    let mut index = current;

    loop {
        index = index.checked_add_signed(step)?;
        let item = items.get(index)?;

        if !item.disabled {
            return Some(&item.id);
        }
    }
}

/// Runs the group's keyboard behaviour: the arrow keys move the choice among
/// the enabled options, and Space or Enter reports the current one unless it is
/// disabled. Returns whether the group consumed the key.
fn handle_key(
    items: &[OptionGroupItem],
    selected: &str,
    on_change: &OptionChangeHandler,
    event: &KeyDownEvent,
    window: &mut Window,
    cx: &mut App,
) -> bool {
    let step = match event.keystroke.key.as_str() {
        "up" | "left" => -1,
        "down" | "right" => 1,
        _ => 0,
    };

    if step != 0 {
        let Some(next) = stepped_option(items, selected, step) else {
            return false;
        };

        on_change(next, window, cx);

        return true;
    }

    if !super::is_activation_key(&event.keystroke) {
        return false;
    }

    let Some(current) = items
        .iter()
        .find(|item| item.id.as_ref() == selected)
        .filter(|item| !item.disabled)
    else {
        return false;
    };

    on_change(&current.id, window, cx);

    true
}

/// Escapes the separators of an option group id inside a name, so the boundary
/// between the group, the option and the description cannot be forged by one.
fn escaped_id_part(part: &str) -> String {
    let mut escaped = String::with_capacity(part.len());

    for character in part.chars() {
        match character {
            '%' => escaped.push_str("%25"),
            ':' => escaped.push_str("%3A"),
            _ => escaped.push(character),
        }
    }

    escaped
}

/// Element and accessibility id of the group named `group`.
pub fn option_group_id(group: &str) -> SharedString {
    SharedString::from(format!("option-group:{}", escaped_id_part(group)))
}

/// Element and accessibility id of the option `item` of the group named
/// `group`.
pub fn option_group_item_id(group: &str, item: &str) -> SharedString {
    SharedString::from(format!(
        "{}:{}",
        option_group_id(group),
        escaped_id_part(item)
    ))
}

/// Element and accessibility id of the description of option `item` of the
/// group named `group`.
pub fn option_group_description_id(group: &str, item: &str) -> SharedString {
    SharedString::from(format!("{}:description", option_group_item_id(group, item)))
}

/// What every row of one group shares: the theme, the chosen option and the
/// handler the rows report to.
struct RowContext<'a> {
    group: &'a str,
    selected: &'a SharedString,
    focused: bool,
    theme: &'a Theme,
    tint: Hsla,
    label_size: Rems,
    on_change: OptionChangeHandler,
}

impl RowContext<'_> {
    /// One option: the indicator, the label and the description under it.
    fn render_row(&self, item: &OptionGroupItem) -> AnyElement {
        let theme = self.theme;
        let item_id = option_group_item_id(self.group, item.id.as_ref());
        let is_selected = item.id == *self.selected;
        let disabled = item.disabled;
        let opacity = if disabled {
            Fields::DISABLED_OPACITY
        } else {
            1.0
        };
        let label_color = if disabled {
            theme.muted_foreground
        } else {
            theme.foreground
        };
        let description = item.description.clone();
        let description_id = option_group_description_id(self.group, item.id.as_ref());

        let text = div()
            .flex()
            .flex_col()
            .flex_1()
            .min_w_0()
            .gap(Fields::CHECK_ROW_LINE_GAP)
            .child(
                div()
                    .min_h(Fields::CHECKBOX_SIZE)
                    .flex()
                    .items_center()
                    .line_height(relative(1.))
                    .text_size(self.label_size)
                    .text_color(label_color)
                    .child(item.label.clone()),
            )
            .when_some(description.clone(), |this, description| {
                this.child(
                    div()
                        .debug_selector(id_selector(&ElementId::from(description_id)))
                        .truncate()
                        .child(
                            Text::body(description)
                                .font_size(FontSizes::XS)
                                .muted_foreground(),
                        ),
                )
            });

        let click_id = item.id.clone();
        let a11y_id = click_id.clone();
        let on_change = self.on_change.clone();

        div()
            .debug_selector(id_selector(&ElementId::from(item_id.clone())))
            .id(item_id.clone())
            .role(Role::RadioButton)
            .accessibility_id(item_id)
            .aria_label(item.label.clone())
            .when_some(description, |this, description| {
                this.aria_description(description)
            })
            .aria_toggled(if is_selected {
                Toggled::True
            } else {
                Toggled::False
            })
            .when(disabled, |this| this.aria_disabled(true))
            .when(self.focused && is_selected, |this| {
                this.aria_active_descendant()
            })
            .relative()
            .flex()
            .items_start()
            .gap(Fields::CHECKBOX_GAP)
            .w_full()
            .py(Fields::CHECK_ROW_PADDING_Y)
            .when(disabled, |this| this.cursor_not_allowed())
            .when(!disabled, |this| {
                let a11y_change = on_change.clone();

                this.cursor_pointer()
                    .on_a11y_action(AccessibleAction::Click, move |_, window, cx| {
                        a11y_change(&a11y_id, window, cx);
                    })
                    .on_click(move |_, window, cx| {
                        on_change(&click_id, window, cx);
                    })
            })
            .child(self.indicator(is_selected, opacity))
            .child(text)
            .into_any_element()
    }

    /// The radio box of one option, with its dot and its focus ring.
    fn indicator(&self, is_selected: bool, opacity: f32) -> Div {
        let theme = self.theme;
        let fill_kind = if is_selected {
            ChamferFillKind::Filled
        } else {
            ChamferFillKind::Surface
        };
        let shape = if is_selected {
            Chamfer::new(ChamferCut::KEYCAP).fill(theme.primary.opacity(opacity))
        } else {
            Chamfer::new(ChamferCut::KEYCAP)
                .ring(ChamferRing::outline(theme.input.opacity(opacity)))
        };

        div()
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .justify_center()
            .size(Fields::CHECKBOX_SIZE)
            .child(shape)
            .when(is_selected, |this| {
                this.child(
                    div()
                        .size(Fields::RADIO_DOT)
                        .rounded_full()
                        .bg(theme.primary_foreground.opacity(opacity)),
                )
            })
            .when(self.focused && is_selected, |this| {
                this.child(
                    Chamfer::new(ChamferCut::KEYCAP)
                        .ring(ChamferRing::focus_for(self.tint, fill_kind)),
                )
            })
    }
}

/// A vertical list of mutually-exclusive options.
///
/// `group` names the control and every id below it, so it must be unique in
/// the window; `selected` is the id of the chosen option, or an id matching
/// none of them while nothing is chosen yet; `on_change` is called with the id
/// the user chooses.
#[derive(IntoElement)]
pub struct OptionGroup {
    group: SharedString,
    items: Vec<OptionGroupItem>,
    selected: SharedString,
    aria_label: Option<SharedString>,
    on_change: OptionChangeHandler,
}

impl OptionGroup {
    pub fn new(
        group: impl Into<SharedString>,
        items: Vec<OptionGroupItem>,
        selected: impl Into<SharedString>,
        on_change: impl Fn(&SharedString, &mut Window, &mut App) + 'static,
    ) -> Self {
        Self {
            group: group.into(),
            items,
            selected: selected.into(),
            aria_label: None,
            on_change: Rc::new(on_change),
        }
    }

    /// Names the control for assistive technology. A radio group has no text
    /// of its own to be named by, so give it the label its form row draws.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.aria_label = Some(label.into());
        self
    }

    fn focus_handle(&self, window: &mut Window, cx: &mut App) -> FocusHandle {
        let id = ElementId::from(option_group_id(&self.group));

        window
            .use_keyed_state(id, cx, |_, cx| cx.focus_handle())
            .read(cx)
            .clone()
    }

    /// The focusable container: the radio group the options hang from.
    fn group_element(
        &self,
        focus_handle: FocusHandle,
        rows: Vec<AnyElement>,
    ) -> gpui::Stateful<Div> {
        let group_id = option_group_id(&self.group);
        let group_element_id = ElementId::from(group_id.clone());

        div()
            .debug_selector(id_selector(&group_element_id))
            .id(group_element_id)
            .role(Role::RadioGroup)
            .accessibility_id(group_id)
            .when_some(self.aria_label.clone(), |this, label| {
                this.aria_label(label)
            })
            .track_focus(&focus_handle.tab_index(0).tab_stop(true))
            .flex()
            .flex_col()
            .children(rows)
    }
}

impl RenderOnce for OptionGroup {
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        let focus_handle = self.focus_handle(window, cx);
        let focused = focus_handle.is_focused(window);
        let rows: Vec<AnyElement> = {
            let context = RowContext {
                group: &self.group,
                selected: &self.selected,
                focused,
                theme: cx.theme(),
                tint: ChromeColors::tint(cx.theme()),
                label_size: density::font_base(cx),
                on_change: self.on_change.clone(),
            };

            self.items
                .iter()
                .map(|item| context.render_row(item))
                .collect()
        };

        let font_family = crate::fonts::ui_family(cx);
        let element = self
            .group_element(focus_handle, rows)
            .font_family(font_family);

        let items = self.items;
        let selected = self.selected;
        let on_change = self.on_change;

        element.on_key_down(move |event: &KeyDownEvent, window, cx| {
            if handle_key(&items, &selected, &on_change, event, window, cx) {
                cx.stop_propagation();
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use gpui::{
        AccessibilityFrame, AppContext as _, Context, Entity, FocusHandle, InteractiveElement as _,
        IntoElement, Modifiers, ParentElement as _, Render, Role, SharedString, Styled as _,
        TestAppContext, Toggled, VisualTestContext, Window, div, px,
    };

    use super::{
        OptionGroup, OptionGroupItem, option_group_description_id, option_group_id,
        option_group_item_id,
    };
    use crate::accessibility::render_frame;
    use crate::primitives::FOCUS_RING_SELECTOR;
    use crate::tokens::{BASE_REM, Fields};

    /// Every id the fixtures address, spelled out so a changed derivation fails
    /// a test instead of following the code.
    const GROUP: &str = "option-group:secret-mode";
    const OMIT: &str = "option-group:secret-mode:omit";
    const ENCRYPT: &str = "option-group:secret-mode:encrypt";
    const OMIT_DESCRIPTION: &str = "option-group:secret-mode:omit:description";

    fn items() -> Vec<OptionGroupItem> {
        vec![
            OptionGroupItem::new("omit", "Leave out")
                .description("The bundle lists them by name, empty."),
            OptionGroupItem::new("encrypt", "Encrypt")
                .description("Values are sealed with a passphrase."),
        ]
    }

    /// A group whose middle option cannot be chosen.
    fn gapped_items() -> Vec<OptionGroupItem> {
        vec![
            OptionGroupItem::new("omit", "Leave out"),
            OptionGroupItem::new("encrypt", "Encrypt").disabled(true),
            OptionGroupItem::new("seal", "Seal"),
        ]
    }

    struct Host {
        focus_handle: FocusHandle,
        selected: SharedString,
        changes: Arc<Mutex<Vec<String>>>,
        items: Vec<OptionGroupItem>,
    }

    impl Render for Host {
        fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            let entity = cx.entity();
            let changes = self.changes.clone();
            let items = self.items.clone();
            let group = OptionGroup::new(
                "secret-mode",
                items,
                self.selected.clone(),
                move |id, _, cx| {
                    changes
                        .lock()
                        .expect("change log lock")
                        .push(id.to_string());
                    let id = id.clone();
                    entity.update(cx, |host, cx| {
                        host.selected = id;
                        cx.notify();
                    });
                },
            )
            .aria_label("Secrets in a bundle");

            div()
                .track_focus(&self.focus_handle)
                .size_full()
                .child(group)
        }
    }

    /// Opens a window holding one group, selected on `omit`, and logs every
    /// option it reports.
    fn open(
        items: Vec<OptionGroupItem>,
        cx: &mut TestAppContext,
    ) -> (
        Entity<Host>,
        Arc<Mutex<Vec<String>>>,
        &mut VisualTestContext,
    ) {
        open_with("omit", items, cx)
    }

    /// Opens a window holding one group with `selected` as the choice it
    /// starts on, and logs every option it reports.
    fn open_with<'a>(
        selected: &str,
        items: Vec<OptionGroupItem>,
        cx: &'a mut TestAppContext,
    ) -> (
        Entity<Host>,
        Arc<Mutex<Vec<String>>>,
        &'a mut VisualTestContext,
    ) {
        let initial = SharedString::from(selected.to_string());
        cx.update(gpui_component::init);
        cx.update(|cx| {
            crate::theme::install(
                crate::brand::Brand::neutral(),
                crate::palette::PaletteMode::Dark,
                crate::density::Density::Default,
                cx,
            );
        });

        let changes = Arc::new(Mutex::new(Vec::new()));
        let log = changes.clone();
        let host_slot = std::rc::Rc::new(std::cell::RefCell::new(None));
        let slot = host_slot.clone();
        let (_root, window) = cx.add_window_view(move |window, cx| {
            let host = cx.new(move |cx| Host {
                focus_handle: cx.focus_handle(),
                selected: initial,
                changes: log,
                items,
            });
            slot.replace(Some(host.clone()));

            gpui_component::Root::new(host, window, cx)
        });
        window.run_until_parked();

        let host = host_slot.borrow().clone().expect("the host is built");
        window.update(|window, cx| host.read(cx).focus_handle.clone().focus(window, cx));
        window.run_until_parked();

        (host, changes, window)
    }

    fn selected(host: &Entity<Host>, window: &mut VisualTestContext) -> String {
        window.update(|_, cx| host.read(cx).selected.to_string())
    }

    fn reported(changes: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
        changes.lock().expect("change log lock").clone()
    }

    fn press(window: &mut VisualTestContext, keys: &str) {
        window.simulate_keystrokes(keys);
        window.run_until_parked();
    }

    fn center_of(window: &mut VisualTestContext, id: &'static str) -> gpui::Point<gpui::Pixels> {
        window
            .debug_bounds(id)
            .unwrap_or_else(|| panic!("{id} is laid out"))
            .center()
    }

    #[gpui::test]
    fn arrows_move_the_selection_and_report_each_option(cx: &mut TestAppContext) {
        let (host, changes, window) = open(items(), cx);
        press(window, "tab");

        press(window, "down");
        assert_eq!(selected(&host, window), "encrypt");
        assert_eq!(reported(&changes), ["encrypt"]);

        press(window, "up");
        assert_eq!(selected(&host, window), "omit");
        assert_eq!(reported(&changes), ["encrypt", "omit"]);

        press(window, "right");
        assert_eq!(selected(&host, window), "encrypt");

        press(window, "left");
        assert_eq!(selected(&host, window), "omit");
    }

    #[gpui::test]
    fn an_empty_choice_moves_to_the_first_or_last_enabled_option(cx: &mut TestAppContext) {
        let (host, changes, window) = open_with("", gapped_items(), cx);
        press(window, "tab");

        press(window, "down");
        assert_eq!(
            selected(&host, window),
            "omit",
            "with no choice yet, Down takes the first enabled option"
        );
        assert_eq!(reported(&changes), ["omit"]);

        let (host, changes, window) = open_with("", gapped_items(), cx);
        press(window, "tab");

        press(window, "up");
        assert_eq!(
            selected(&host, window),
            "seal",
            "with no choice yet, Up takes the last enabled option"
        );
        assert_eq!(reported(&changes), ["seal"]);

        let (host, changes, window) = open_with(
            "",
            vec![OptionGroupItem::new("omit", "Leave out").disabled(true)],
            cx,
        );
        press(window, "tab");

        press(window, "down");
        assert_eq!(selected(&host, window), "", "every option is disabled");
        assert!(reported(&changes).is_empty());
    }

    #[gpui::test]
    fn the_arrows_stop_at_the_ends(cx: &mut TestAppContext) {
        let (host, changes, window) = open(items(), cx);
        press(window, "tab");

        press(window, "up");
        assert_eq!(selected(&host, window), "omit");
        assert!(reported(&changes).is_empty());

        press(window, "down");
        press(window, "down");
        assert_eq!(selected(&host, window), "encrypt");
        assert_eq!(reported(&changes), ["encrypt"]);
    }

    #[gpui::test]
    fn space_and_enter_report_the_current_option(cx: &mut TestAppContext) {
        let (host, changes, window) = open(items(), cx);
        press(window, "tab");

        press(window, "space");
        press(window, "enter");

        assert_eq!(selected(&host, window), "omit");
        assert_eq!(reported(&changes), ["omit", "omit"]);
    }

    #[gpui::test]
    fn a_disabled_option_is_neither_crossed_nor_activated(cx: &mut TestAppContext) {
        let (host, changes, window) = open(gapped_items(), cx);
        press(window, "tab");

        press(window, "down");
        assert_eq!(
            selected(&host, window),
            "seal",
            "the keyboard skips the disabled option"
        );
        assert_eq!(reported(&changes), ["seal"]);

        let disabled = center_of(window, ENCRYPT);
        window.simulate_click(disabled, Modifiers::default());
        window.run_until_parked();

        assert_eq!(selected(&host, window), "seal");
        assert_eq!(
            reported(&changes),
            ["seal"],
            "a disabled option answers nothing"
        );
    }

    #[gpui::test]
    fn a_click_selects_the_option_and_the_keyboard_keeps_working(cx: &mut TestAppContext) {
        let (host, changes, window) = open(items(), cx);

        let encrypt = center_of(window, ENCRYPT);
        window.simulate_click(encrypt, Modifiers::default());
        window.run_until_parked();

        assert_eq!(selected(&host, window), "encrypt");
        assert_eq!(reported(&changes), ["encrypt"]);

        press(window, "up");
        assert_eq!(
            selected(&host, window),
            "omit",
            "the click left the group with the keyboard"
        );
    }

    #[gpui::test]
    fn the_selected_option_rings_only_after_the_keyboard_reaches_the_group(
        cx: &mut TestAppContext,
    ) {
        let (_host, _changes, window) = open(items(), cx);

        assert!(
            window.debug_bounds(FOCUS_RING_SELECTOR).is_none(),
            "an untouched group draws no ring"
        );

        press(window, "tab");
        let ring = window
            .debug_bounds(FOCUS_RING_SELECTOR)
            .expect("the keyboard-focused group rings");
        assert!(
            window
                .debug_bounds(OMIT)
                .expect("the selected option is laid out")
                .contains(&ring.center()),
            "the ring sits on the selected option"
        );

        press(window, "down");
        let ring = window
            .debug_bounds(FOCUS_RING_SELECTOR)
            .expect("the group keeps its ring");
        assert!(
            window
                .debug_bounds(ENCRYPT)
                .expect("the new option is laid out")
                .contains(&ring.center()),
            "the ring follows the selection"
        );

        let omit = center_of(window, OMIT);
        window.simulate_click(omit, Modifiers::default());
        window.run_until_parked();
        assert!(
            window.debug_bounds(FOCUS_RING_SELECTOR).is_none(),
            "a pointer selection shows no ring"
        );
    }

    fn find_node<'a>(frame: &'a AccessibilityFrame, id: &str) -> Option<&'a gpui::FrameNode> {
        frame
            .nodes()
            .find(|(_, node)| node.id() == id)
            .map(|(_, node)| node)
    }

    fn node<'a>(frame: &'a AccessibilityFrame, id: &str) -> &'a gpui::FrameNode {
        find_node(frame, id).unwrap_or_else(|| panic!("{id} is in the frame"))
    }

    fn accessible<'a>(frame: &'a AccessibilityFrame, id: &str) -> &'a gpui::accesskit::Node {
        frame
            .accessibility_node(node(frame, id))
            .unwrap_or_else(|| panic!("{id} is an accessible node"))
    }

    /// A group in a column narrow enough that a long description would wrap.
    struct NarrowHost;

    impl Render for NarrowHost {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().w(px(220.0)).child(OptionGroup::new(
                "secret-mode",
                vec![
                    OptionGroupItem::new("short", "Short").description("Sealed."),
                    OptionGroupItem::new("long", "Long").description(
                        "Values are sealed with a passphrase that nobody stores, and a bundle cannot be opened without it.",
                    ),
                ],
                "short",
                |_, _, _| {},
            ))
        }
    }

    fn surface(_cx: &mut gpui::App) -> gpui::AnyElement {
        OptionGroup::new("secret-mode", items(), "omit", |_, _, _| {})
            .aria_label("Secrets in a bundle")
            .into_any_element()
    }

    fn disabled_surface(_cx: &mut gpui::App) -> gpui::AnyElement {
        OptionGroup::new("secret-mode", gapped_items(), "omit", |_, _, _| {}).into_any_element()
    }

    #[gpui::test]
    fn the_group_is_a_radio_group_of_named_radio_buttons(cx: &mut TestAppContext) {
        let frame = render_frame(cx, surface);

        let group = accessible(&frame, GROUP);
        assert_eq!(group.role(), Role::RadioGroup);

        let omit = accessible(&frame, OMIT);
        assert_eq!(omit.role(), Role::RadioButton);
        assert_eq!(omit.label(), Some("Leave out"));
        assert_eq!(omit.toggled(), Some(Toggled::True));
        assert!(omit.supports_action(gpui::AccessibleAction::Click));

        assert_eq!(
            omit.description(),
            Some("The bundle lists them by name, empty."),
            "an option describes itself with its description"
        );

        let encrypt = accessible(&frame, ENCRYPT);
        assert_eq!(encrypt.label(), Some("Encrypt"));
        assert_eq!(encrypt.toggled(), Some(Toggled::False));
        assert_eq!(
            encrypt.description(),
            Some("Values are sealed with a passphrase.")
        );
        assert!(
            find_node(&frame, OMIT_DESCRIPTION)
                .and_then(|node| frame.accessibility_node(node))
                .is_none(),
            "the option describes itself, so the description is not a node of its own"
        );
    }

    #[gpui::test]
    fn a_description_draws_under_the_label(cx: &mut TestAppContext) {
        let (_host, _changes, window) = open(items(), cx);

        let option = window.debug_bounds(OMIT).expect("the option is laid out");
        let description = window
            .debug_bounds(OMIT_DESCRIPTION)
            .expect("the description is drawn");

        assert!(option.contains(&description.center()));
        assert!(
            description.top() > option.top() + Fields::CHECKBOX_SIZE.to_pixels(px(BASE_REM)),
            "the description sits under the label line"
        );
    }

    #[gpui::test]
    fn the_group_reports_the_selected_option_as_the_focused_node(cx: &mut TestAppContext) {
        let (_host, _changes, window) = open(items(), cx);
        let capture = crate::accessibility::FrameCapture::observe(window);

        press(window, "tab");
        let frame = capture.frame(window);
        assert_eq!(
            frame.tree().focus,
            node(&frame, OMIT).accessibility_id(),
            "the selected option is the focused node of the radio group"
        );

        press(window, "down");
        let frame = capture.frame(window);
        assert_eq!(
            frame.tree().focus,
            node(&frame, ENCRYPT).accessibility_id(),
            "the focused node follows the choice"
        );
    }

    #[gpui::test]
    fn a_long_description_keeps_to_one_line(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);
        let (_view, window) = cx.add_window_view(|_, _| NarrowHost);
        window.run_until_parked();

        let short = window
            .debug_bounds("option-group:secret-mode:short:description")
            .expect("the short description is drawn")
            .size
            .height;
        let long = window
            .debug_bounds("option-group:secret-mode:long:description")
            .expect("the long description is drawn")
            .size
            .height;

        assert_eq!(long, short, "a description is one line, however long it is");
        assert!(long > px(0.0));
    }

    #[gpui::test]
    fn space_and_enter_do_not_report_a_disabled_selection(cx: &mut TestAppContext) {
        let (host, changes, window) = open(
            vec![
                OptionGroupItem::new("omit", "Leave out").disabled(true),
                OptionGroupItem::new("encrypt", "Encrypt"),
            ],
            cx,
        );
        press(window, "tab");

        press(window, "space");
        press(window, "enter");

        assert!(reported(&changes).is_empty());
        assert_eq!(selected(&host, window), "omit");

        press(window, "down");
        assert_eq!(selected(&host, window), "encrypt");
    }

    #[gpui::test]
    fn a_disabled_option_is_reported_disabled(cx: &mut TestAppContext) {
        let frame = render_frame(cx, disabled_surface);

        assert!(accessible(&frame, ENCRYPT).is_disabled());
        assert!(!accessible(&frame, OMIT).is_disabled());
    }

    #[gpui::test]
    fn an_option_without_a_description_draws_no_description_node(cx: &mut TestAppContext) {
        let drawn = {
            let (_host, _changes, window) = open(items(), cx);
            window.debug_bounds(OMIT_DESCRIPTION).is_some()
        };
        assert!(
            drawn,
            "the selector finds the description an option with one draws"
        );

        let missing = {
            let (_host, _changes, window) = open(gapped_items(), cx);
            window.debug_bounds(OMIT_DESCRIPTION).is_none()
        };
        assert!(
            missing,
            "an option without a description draws no description node"
        );
    }

    #[test]
    fn ids_are_derived_from_the_group_and_the_option() {
        assert_eq!(option_group_id("secret-mode").as_ref(), GROUP);
        assert_eq!(option_group_item_id("secret-mode", "omit").as_ref(), OMIT);
        assert_eq!(
            option_group_description_id("secret-mode", "omit").as_ref(),
            OMIT_DESCRIPTION
        );
    }

    #[test]
    fn ids_do_not_collide_across_groups_items_and_descriptions() {
        assert_ne!(
            option_group_item_id("delivery-express", "express"),
            option_group_item_id("delivery", "express-express"),
            "the group and item boundary cannot be moved by a name"
        );
        assert_ne!(
            option_group_item_id("delivery", "express"),
            option_group_id("delivery-express")
        );
        assert_ne!(
            option_group_item_id("delivery", "omit-description"),
            option_group_description_id("delivery", "omit"),
            "an item named like a description is not that description"
        );
        assert_ne!(
            option_group_item_id("a:b", "c"),
            option_group_item_id("a", "b:c"),
            "a separator inside a name cannot move the boundary"
        );
        assert_ne!(
            option_group_item_id("a%3Ab", "c"),
            option_group_item_id("a:b", "c"),
            "an escaped separator is not the same name"
        );
    }
}
