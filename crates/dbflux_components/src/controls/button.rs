use gpui::prelude::*;
use gpui::{
    App, ClickEvent, ElementId, FocusHandle, FontWeight, Hsla, KeyDownEvent, KeyUpEvent,
    MouseButton, Pixels, SharedString, Window, div,
};
use gpui_component::ActiveTheme;
use gpui_component::theme::Theme;
use gpui_component::tooltip::Tooltip;

use crate::icon::IconSource;
use crate::primitives::{
    Chamfer, ChamferCorners, ChamferFillKind, ChamferRing, Icon, Kbd, KbdTone,
};
use crate::tokens::{ButtonMetrics, ChamferCut, ChromeColors, Fields};
use crate::typography::AppFonts;

/// Color treatment of a [`Button`] (DSStates).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ButtonVariant {
    /// Byzantine fill, white content. One per toolbar or dialog.
    Primary,
    /// Raised fill, body text.
    #[default]
    Secondary,
    /// Transparent until hovered.
    Ghost,
    /// Soft red fill, danger text.
    Danger,
}

impl ButtonVariant {
    /// Whether the rest fill is a solid or tinted fill, which puts the focus
    /// ring outside the shape.
    pub fn fill_kind(self) -> ChamferFillKind {
        match self {
            Self::Primary | Self::Danger => ChamferFillKind::Filled,
            Self::Secondary | Self::Ghost => ChamferFillKind::Surface,
        }
    }
}

/// Height of a [`Button`] (canonical sizes, canvas v43).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ButtonSize {
    /// 24 px, cut 6: inside table rows, list rows, chips and card rows.
    Inline,
    /// 30 px, cut 6: toolbars, footers, dialogs and forms. Same height as
    /// inputs and selects.
    #[default]
    Regular,
    /// 44 px, cut 10: a call-to-action a board draws large.
    Large,
}

impl ButtonSize {
    pub fn height(self) -> Pixels {
        match self {
            Self::Inline => ButtonMetrics::HEIGHT_INLINE,
            Self::Regular => ButtonMetrics::HEIGHT,
            Self::Large => ButtonMetrics::HEIGHT_LARGE,
        }
    }

    pub fn icon_only_width(self) -> Pixels {
        match self {
            Self::Inline => ButtonMetrics::ICON_ONLY_WIDTH_INLINE,
            Self::Regular => ButtonMetrics::ICON_ONLY_WIDTH,
            Self::Large => ButtonMetrics::ICON_ONLY_WIDTH_LARGE,
        }
    }

    pub fn cut(self) -> Pixels {
        match self {
            Self::Inline | Self::Regular => ChamferCut::CONTROL,
            Self::Large => ChamferCut::LARGE_CONTROL,
        }
    }

    pub fn font_size(self) -> Pixels {
        match self {
            Self::Inline => ButtonMetrics::FONT_INLINE,
            Self::Regular => ButtonMetrics::FONT,
            Self::Large => ButtonMetrics::FONT_LARGE,
        }
    }

    pub fn padding_x(self) -> Pixels {
        match self {
            Self::Inline => ButtonMetrics::PADDING_X_INLINE,
            Self::Regular => ButtonMetrics::PADDING_X,
            Self::Large => ButtonMetrics::PADDING_X_LARGE,
        }
    }

    pub fn gap(self) -> Pixels {
        match self {
            Self::Inline => ButtonMetrics::GAP_INLINE,
            Self::Regular | Self::Large => ButtonMetrics::GAP,
        }
    }

    /// Icon leading a label.
    pub fn icon(self) -> Pixels {
        match self {
            Self::Inline => ButtonMetrics::ICON_INLINE,
            Self::Regular | Self::Large => ButtonMetrics::ICON,
        }
    }

    /// Icon of an icon-only button.
    pub fn icon_only_icon(self) -> Pixels {
        match self {
            Self::Inline => ButtonMetrics::ICON_ONLY_INLINE,
            Self::Regular | Self::Large => ButtonMetrics::ICON_ONLY,
        }
    }
}

/// Fills of one button state set: rest, hover, pressed.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ButtonFills {
    pub rest: Hsla,
    pub hover: Hsla,
    pub pressed: Hsla,
}

/// Fills and content color of a variant (DSStates). A selected ghost or
/// secondary button takes a soft tint fill and tint content.
pub fn button_colors(theme: &Theme, variant: ButtonVariant, selected: bool) -> (ButtonFills, Hsla) {
    let soft = |color: Hsla| ButtonFills {
        rest: color.opacity(ButtonMetrics::SOFT_FILL_REST),
        hover: color.opacity(ButtonMetrics::SOFT_FILL_HOVER),
        pressed: color.opacity(ButtonMetrics::SOFT_FILL_PRESSED),
    };

    match variant {
        ButtonVariant::Primary => (
            ButtonFills {
                rest: theme.primary,
                hover: theme.primary_hover,
                pressed: theme.primary_active,
            },
            theme.primary_foreground,
        ),
        ButtonVariant::Danger => (soft(theme.danger), theme.danger),
        ButtonVariant::Secondary | ButtonVariant::Ghost if selected => {
            let tint = ChromeColors::tint(theme);
            (soft(tint), tint)
        }
        ButtonVariant::Secondary => (
            ButtonFills {
                rest: theme.secondary,
                hover: theme.secondary_hover,
                pressed: theme.secondary_active,
            },
            theme.foreground,
        ),
        ButtonVariant::Ghost => (
            ButtonFills {
                rest: gpui::transparent_black(),
                hover: theme.secondary,
                pressed: theme.secondary_hover,
            },
            theme.foreground,
        ),
    }
}

/// Whether a key event is a button activation key (Enter or Space without
/// modifiers), the same keys GPUI turns into a keyboard click.
pub fn is_activation_key(keystroke: &gpui::Keystroke) -> bool {
    (keystroke.key == "enter" || keystroke.key == "space") && !keystroke.modifiers.modified()
}

/// Per-button state kept across frames: the focus handle and whether an
/// activation key is held down.
struct ButtonState {
    focus_handle: FocusHandle,
    key_held: bool,
}

type ButtonClickHandler = Box<dyn Fn(&ClickEvent, &mut Window, &mut App) + 'static>;

/// The DBFlux button: a chamfered control with an optional leading icon, a
/// label, and an optional trailing keycap.
///
/// Every button is a GPUI tab stop, so Tab reaches it and the focus ring
/// traces the cut. Enter and Space activate a focused button and show the
/// pressed fill while held. A mouse press does not move focus, so clicking a
/// toolbar button leaves focus in the editor or grid it acts on.
///
/// An icon-only button is a flag: `.icon_only()` hides the label, and the
/// label becomes the tooltip unless `.tooltip()` sets another one.
#[derive(IntoElement)]
pub struct Button {
    id: ElementId,
    label: SharedString,
    variant: ButtonVariant,
    size: ButtonSize,
    icon: Option<IconSource>,
    icon_size: Option<Pixels>,
    icon_only: bool,
    trailing_icon: Option<IconSource>,
    kbd: Option<SharedString>,
    tooltip: Option<SharedString>,
    text_color: Option<Hsla>,
    disabled: bool,
    selected: bool,
    focused: bool,
    tab_stop: bool,
    focus_handle: Option<FocusHandle>,
    w_full: bool,
    corners: ChamferCorners,
    padding_left: Option<Pixels>,
    on_click: Option<ButtonClickHandler>,
}

impl Button {
    pub fn new(id: impl Into<ElementId>, label: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            variant: ButtonVariant::default(),
            size: ButtonSize::default(),
            icon: None,
            icon_size: None,
            icon_only: false,
            trailing_icon: None,
            kbd: None,
            tooltip: None,
            text_color: None,
            disabled: false,
            selected: false,
            focused: false,
            tab_stop: true,
            focus_handle: None,
            w_full: false,
            corners: ChamferCorners::default(),
            padding_left: None,
            on_click: None,
        }
    }

    pub fn variant(mut self, variant: ButtonVariant) -> Self {
        self.variant = variant;
        self
    }

    pub fn primary(self) -> Self {
        self.variant(ButtonVariant::Primary)
    }

    pub fn secondary(self) -> Self {
        self.variant(ButtonVariant::Secondary)
    }

    pub fn ghost(self) -> Self {
        self.variant(ButtonVariant::Ghost)
    }

    pub fn danger(self) -> Self {
        self.variant(ButtonVariant::Danger)
    }

    pub fn size(mut self, size: ButtonSize) -> Self {
        self.size = size;
        self
    }

    /// 24 px tall, for buttons inside table rows, list rows, chips and card
    /// rows.
    pub fn inline(self) -> Self {
        self.size(ButtonSize::Inline)
    }

    /// 44 px tall with a 10 px cut.
    pub fn large(self) -> Self {
        self.size(ButtonSize::Large)
    }

    /// Icon leading the label.
    pub fn icon(mut self, icon: impl Into<IconSource>) -> Self {
        self.icon = Some(icon.into());
        self
    }

    /// Overrides the icon size (by default 15 px beside a label and 16 px
    /// icon-only; 12 and 13 px inline).
    pub fn icon_size(mut self, size: Pixels) -> Self {
        self.icon_size = Some(size);
        self
    }

    /// Shows only the icon; the label becomes the tooltip.
    pub fn icon_only(mut self) -> Self {
        self.icon_only = true;
        self
    }

    /// Trailing keycap with the shortcut that runs the same action.
    /// A small icon after the label, such as the chevron of a button that
    /// opens a menu (AppByzTable "Export").
    pub fn trailing_icon(mut self, icon: impl Into<IconSource>) -> Self {
        self.trailing_icon = Some(icon.into());
        self
    }

    pub fn kbd(mut self, shortcut: impl Into<SharedString>) -> Self {
        self.kbd = Some(shortcut.into());
        self
    }

    pub fn tooltip(mut self, tooltip: impl Into<SharedString>) -> Self {
        self.tooltip = Some(tooltip.into());
        self
    }

    /// Overrides the content color of the variant.
    pub fn text_color(mut self, color: Hsla) -> Self {
        self.text_color = Some(color);
        self
    }

    pub fn disabled(mut self, disabled: bool) -> Self {
        self.disabled = disabled;
        self
    }

    /// Marks a toggle as on: ghost and secondary buttons take the tint.
    pub fn selected(mut self, selected: bool) -> Self {
        self.selected = selected;
        self
    }

    /// Shows the focus ring for a caller that tracks keyboard focus itself
    /// (toolbars with their own slot navigation). The button's own GPUI focus
    /// shows the ring as well.
    pub fn focused(mut self, focused: bool) -> Self {
        self.focused = focused;
        self
    }

    /// Whether Tab stops on this button (default `true`).
    pub fn tab_stop(mut self, tab_stop: bool) -> Self {
        self.tab_stop = tab_stop;
        self
    }

    /// Uses `handle` as the button's focus handle instead of one kept in
    /// element state, so the owner can move focus to the button (for example
    /// the default action of a dialog when it opens) and ask whether it holds
    /// focus.
    pub fn focus_handle(mut self, handle: &FocusHandle) -> Self {
        self.focus_handle = Some(handle.clone());
        self
    }

    pub fn w_full(mut self) -> Self {
        self.w_full = true;
        self
    }

    /// Which corners are cut; a split button cuts only the outer ones.
    pub(crate) fn corners(mut self, corners: ChamferCorners) -> Self {
        self.corners = corners;
        self
    }

    pub(crate) fn padding_left(mut self, padding: Pixels) -> Self {
        self.padding_left = Some(padding);
        self
    }

    pub(crate) fn current_variant(&self) -> ButtonVariant {
        self.variant
    }

    pub(crate) fn current_size(&self) -> ButtonSize {
        self.size
    }

    pub(crate) fn is_disabled(&self) -> bool {
        self.disabled
    }

    pub fn on_click(
        mut self,
        handler: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.on_click = Some(Box::new(handler));
        self
    }

    fn resolved_tooltip(&self) -> Option<SharedString> {
        self.tooltip
            .clone()
            .or_else(|| (self.icon_only && !self.label.is_empty()).then(|| self.label.clone()))
    }
}

impl RenderOnce for Button {
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        let state = window.use_keyed_state(self.id.clone(), cx, |_, cx| ButtonState {
            focus_handle: cx.focus_handle(),
            key_held: false,
        });
        let focus_handle = self
            .focus_handle
            .clone()
            .unwrap_or_else(|| state.read(cx).focus_handle.clone())
            .tab_stop(self.tab_stop && !self.disabled);
        let has_focus = focus_handle.is_focused(window);
        let held = has_focus && state.read(cx).key_held;

        let tooltip = self.resolved_tooltip();
        let theme = cx.theme();
        let (fills, content) = button_colors(theme, self.variant, self.selected);
        let content = self.text_color.unwrap_or(content);
        let ring_color = theme.ring;

        let Button {
            id,
            label,
            variant,
            size,
            icon,
            icon_size,
            icon_only,
            trailing_icon,
            kbd,
            disabled,
            focused,
            w_full,
            corners,
            padding_left,
            on_click,
            ..
        } = self;

        let mut shape = Chamfer::new(size.cut()).corners(corners).fill(fills.rest);

        if (focused || has_focus) && !disabled {
            shape = shape.ring(ChamferRing::focus_for(ring_color, variant.fill_kind()));
        }

        if !disabled {
            shape = shape
                .fill_hover(fills.hover)
                .fill_active(fills.pressed)
                .held(held)
                .interactive("button-chamfer");
        }

        let icon_size = icon_size.unwrap_or(if icon_only {
            size.icon_only_icon()
        } else {
            size.icon()
        });

        let mut button = div()
            .id(id)
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .justify_center()
            .gap(size.gap())
            .h(size.height())
            .font_family(AppFonts::INTERFACE)
            .font_weight(FontWeight::SEMIBOLD)
            .text_size(size.font_size())
            .text_color(content)
            .whitespace_nowrap()
            .track_focus(&focus_handle)
            .child(shape);

        button = if icon_only {
            button.w(size.icon_only_width())
        } else {
            button
                .px(size.padding_x())
                .when_some(padding_left, |button, padding| button.pl(padding))
        };

        if w_full {
            button = button.w_full();
        }

        if let Some(icon) = icon {
            button = button.child(Icon::new(icon).size(icon_size).color(content));
        }

        if !icon_only && !label.is_empty() {
            button = button.child(label);
        }

        if let Some(icon) = trailing_icon.filter(|_| !icon_only) {
            button = button.child(Icon::new(icon).size(Fields::CHEVRON).color(content));
        }

        if let Some(shortcut) = kbd {
            let tone = match variant {
                ButtonVariant::Primary => KbdTone::OnFill,
                _ => KbdTone::Default,
            };
            button = button.child(Kbd::new(shortcut).tone(tone));
        }

        if let Some(tip) = tooltip {
            button = button.tooltip(move |window, cx| Tooltip::new(tip.clone()).build(window, cx));
        }

        if disabled {
            return button.opacity(ButtonMetrics::DISABLED_OPACITY);
        }

        let state_on_down = state.clone();
        let state_on_up = state;

        button = button
            .cursor_pointer()
            .on_mouse_down(MouseButton::Left, |_, window, _| {
                window.prevent_default();
            })
            .on_key_down(move |event: &KeyDownEvent, _, cx| {
                if is_activation_key(&event.keystroke) && !event.is_held {
                    state_on_down.update(cx, |state, cx| {
                        state.key_held = true;
                        cx.notify();
                    });
                }
            })
            .on_key_up(move |event: &KeyUpEvent, _, cx| {
                if is_activation_key(&event.keystroke) {
                    state_on_up.update(cx, |state, cx| {
                        if state.key_held {
                            state.key_held = false;
                            cx.notify();
                        }
                    });
                }
            });

        if let Some(handler) = on_click {
            button = button.on_click(handler);
        }

        button
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::px;

    #[test]
    fn default_size_is_the_thirty_pixel_control() {
        assert_eq!(ButtonSize::default(), ButtonSize::Regular);
        assert_eq!(
            Button::new("save", "Save").current_size(),
            ButtonSize::Regular
        );
        assert_eq!(
            Button::new("save", "Save").inline().current_size(),
            ButtonSize::Inline
        );
        assert_eq!(ButtonSize::Regular.height(), Fields::HEIGHT);
    }

    #[test]
    fn size_table_matches_the_canonical_sizes() {
        let table = [
            (
                ButtonSize::Inline,
                24.0,
                24.0,
                6.0,
                10.0,
                6.0,
                12.0,
                12.0,
                13.0,
            ),
            (
                ButtonSize::Regular,
                30.0,
                32.0,
                6.0,
                12.0,
                8.0,
                12.5,
                15.0,
                16.0,
            ),
            (
                ButtonSize::Large,
                44.0,
                44.0,
                10.0,
                16.0,
                8.0,
                13.0,
                15.0,
                16.0,
            ),
        ];

        for (size, height, icon_width, cut, padding, gap, font, icon, icon_only) in table {
            assert_eq!(size.height(), px(height), "{size:?} height");
            assert_eq!(
                size.icon_only_width(),
                px(icon_width),
                "{size:?} icon width"
            );
            assert_eq!(size.cut(), px(cut), "{size:?} cut");
            assert_eq!(size.padding_x(), px(padding), "{size:?} padding");
            assert_eq!(size.gap(), px(gap), "{size:?} gap");
            assert_eq!(size.font_size(), px(font), "{size:?} font");
            assert_eq!(size.icon(), px(icon), "{size:?} icon");
            assert_eq!(
                size.icon_only_icon(),
                px(icon_only),
                "{size:?} icon-only icon"
            );
        }
    }

    #[test]
    fn filled_variants_put_the_ring_outside() {
        assert_eq!(ButtonVariant::Primary.fill_kind(), ChamferFillKind::Filled);
        assert_eq!(ButtonVariant::Danger.fill_kind(), ChamferFillKind::Filled);
        assert_eq!(
            ButtonVariant::Secondary.fill_kind(),
            ChamferFillKind::Surface
        );
        assert_eq!(ButtonVariant::Ghost.fill_kind(), ChamferFillKind::Surface);
    }

    #[test]
    fn icon_only_label_becomes_the_tooltip() {
        let icon_only = Button::new("save", "Save").icon_only();
        assert_eq!(icon_only.resolved_tooltip(), Some("Save".into()));

        let explicit = Button::new("save", "Save").icon_only().tooltip("Save file");
        assert_eq!(explicit.resolved_tooltip(), Some("Save file".into()));

        assert_eq!(Button::new("save", "Save").resolved_tooltip(), None);
        assert_eq!(
            Button::new("close", "").icon_only().resolved_tooltip(),
            None
        );
    }

    #[test]
    fn only_unmodified_enter_and_space_activate() {
        let parse = |source: &str| gpui::Keystroke::parse(source).expect("valid keystroke");

        assert!(is_activation_key(&parse("enter")));
        assert!(is_activation_key(&parse("space")));
        assert!(!is_activation_key(&parse("ctrl-enter")));
        assert!(!is_activation_key(&parse("a")));
    }

    #[gpui::test]
    fn variants_follow_the_states_board(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        cx.update(|cx| {
            crate::theme::apply_theme(
                dbflux_core::ThemeSetting::Dark,
                dbflux_core::AppStyle::Default,
                None,
                cx,
            );
            let theme = cx.theme();

            let (primary, primary_content) = button_colors(theme, ButtonVariant::Primary, false);
            assert_eq!(primary.rest, theme.primary);
            assert_eq!(primary.hover, theme.primary_hover);
            assert_eq!(primary.pressed, theme.primary_active);
            assert_eq!(primary_content, theme.primary_foreground);

            let (secondary, _) = button_colors(theme, ButtonVariant::Secondary, false);
            assert_eq!(secondary.rest, theme.secondary);
            assert_eq!(secondary.hover, theme.secondary_hover);
            assert_eq!(secondary.pressed, theme.secondary_active);

            let (ghost, _) = button_colors(theme, ButtonVariant::Ghost, false);
            assert_eq!(ghost.rest.a, 0.0);
            assert_eq!(ghost.hover, theme.secondary);

            let (danger, danger_content) = button_colors(theme, ButtonVariant::Danger, false);
            assert_eq!(danger.rest, theme.danger.opacity(0.14));
            assert_eq!(danger.hover, theme.danger.opacity(0.22));
            assert_eq!(danger.pressed, theme.danger.opacity(0.30));
            assert_eq!(danger_content, theme.danger);

            let (_, selected_content) = button_colors(theme, ButtonVariant::Ghost, true);
            assert_eq!(selected_content, ChromeColors::tint(theme));
        });
    }

    gpui::actions!(button_test, [HarnessEnter]);

    struct ToolbarHarness {
        focus_handle: FocusHandle,
        renders: usize,
        clicks: std::rc::Rc<std::cell::RefCell<Vec<&'static str>>>,
    }

    impl Render for ToolbarHarness {
        fn render(
            &mut self,
            _window: &mut Window,
            _cx: &mut gpui::Context<Self>,
        ) -> impl IntoElement {
            self.renders += 1;

            let first_clicks = self.clicks.clone();
            let second_clicks = self.clicks.clone();
            let parent_actions = self.clicks.clone();

            div()
                .track_focus(&self.focus_handle)
                .key_context("ToolbarHarness")
                .on_action(move |_: &HarnessEnter, _, _| parent_actions.borrow_mut().push("parent"))
                .flex()
                .child(
                    div().debug_selector(|| "first-button".to_string()).child(
                        Button::new("first", "First")
                            .on_click(move |_, _, _| first_clicks.borrow_mut().push("first")),
                    ),
                )
                .child(
                    Button::new("second", "Second")
                        .icon_only()
                        .on_click(move |_, _, _| second_clicks.borrow_mut().push("second")),
                )
        }
    }

    /// Presses and releases a key; `simulate_keystrokes` only sends the key
    /// down, and GPUI turns the release of Enter or Space into the click.
    fn press(window: &mut gpui::VisualTestContext, key: &str) {
        let keystroke = gpui::Keystroke::parse(key).expect("valid keystroke");

        window.simulate_event(KeyDownEvent {
            keystroke: keystroke.clone(),
            is_held: false,
            prefer_character_input: false,
        });
        window.simulate_event(KeyUpEvent { keystroke });
        window.run_until_parked();
    }

    fn open_toolbar(
        cx: &mut gpui::TestAppContext,
    ) -> (
        gpui::Entity<ToolbarHarness>,
        std::rc::Rc<std::cell::RefCell<Vec<&'static str>>>,
        &mut gpui::VisualTestContext,
    ) {
        cx.update(crate::theme::init);

        let clicks = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let harness_slot = std::rc::Rc::new(std::cell::RefCell::new(None));
        let (_, window) = cx.add_window_view({
            let clicks = clicks.clone();
            let harness_slot = harness_slot.clone();
            move |window, cx| {
                let harness = cx.new(|cx| ToolbarHarness {
                    focus_handle: cx.focus_handle(),
                    renders: 0,
                    clicks,
                });
                harness_slot.replace(Some(harness.clone()));
                gpui_component::Root::new(harness, window, cx)
            }
        });
        window.run_until_parked();

        let harness = harness_slot
            .borrow()
            .clone()
            .expect("the harness should be built");

        window.update(|window, cx| harness.read(cx).focus_handle.clone().focus(window, cx));
        window.run_until_parked();

        (harness, clicks, window)
    }

    #[gpui::test]
    fn default_button_renders_thirty_pixels_tall(cx: &mut gpui::TestAppContext) {
        let (_harness, _clicks, window) = open_toolbar(cx);

        let first = window
            .debug_bounds("first-button")
            .expect("the first button is laid out");

        assert_eq!(first.size.height, ButtonMetrics::HEIGHT);
    }

    #[gpui::test]
    fn focus_ring_shows_after_tab_and_not_after_a_click(cx: &mut gpui::TestAppContext) {
        let (_harness, clicks, window) = open_toolbar(cx);
        let ring = crate::primitives::FOCUS_RING_SELECTOR;

        window.simulate_keystrokes("tab");
        assert!(
            window.debug_bounds(ring).is_some(),
            "Tab onto a button shows its ring"
        );

        let first = window
            .debug_bounds("first-button")
            .expect("the first button is laid out")
            .center();
        window.simulate_click(first, gpui::Modifiers::default());
        window.run_until_parked();
        assert_eq!(*clicks.borrow(), vec!["first"]);
        assert!(
            window.debug_bounds(ring).is_none(),
            "a click leaves no ring on any button"
        );

        window.simulate_keystrokes("tab");
        assert!(
            window.debug_bounds(ring).is_some(),
            "the next Tab shows the ring again"
        );
    }

    #[gpui::test]
    fn tab_reaches_each_button_and_enter_activates_it(cx: &mut gpui::TestAppContext) {
        cx.update(crate::theme::init);

        let clicks = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let harness_slot = std::rc::Rc::new(std::cell::RefCell::new(None));
        let (_, window) = cx.add_window_view({
            let clicks = clicks.clone();
            let harness_slot = harness_slot.clone();
            move |window, cx| {
                let harness = cx.new(|cx| ToolbarHarness {
                    focus_handle: cx.focus_handle(),
                    renders: 0,
                    clicks,
                });
                harness_slot.replace(Some(harness.clone()));
                gpui_component::Root::new(harness, window, cx)
            }
        });
        window.run_until_parked();

        let harness = harness_slot
            .borrow()
            .clone()
            .expect("the harness should be built");

        window.update(|window, cx| harness.read(cx).focus_handle.clone().focus(window, cx));
        window.simulate_keystrokes("tab");
        press(window, "enter");
        assert_eq!(*clicks.borrow(), vec!["first"]);

        window.simulate_keystrokes("tab");
        press(window, "space");
        assert_eq!(*clicks.borrow(), vec!["first", "second"]);

        let before_press = window.update(|_, cx| harness.read(cx).renders);
        window.simulate_event(KeyDownEvent {
            keystroke: gpui::Keystroke::parse("enter").expect("valid keystroke"),
            is_held: false,
            prefer_character_input: false,
        });
        window.run_until_parked();
        let after_press = window.update(|_, cx| harness.read(cx).renders);
        assert!(
            after_press > before_press,
            "holding Enter must repaint the button with its pressed fill"
        );

        window.simulate_event(KeyUpEvent {
            keystroke: gpui::Keystroke::parse("enter").expect("valid keystroke"),
        });
        window.run_until_parked();
        assert_eq!(*clicks.borrow(), vec!["first", "second", "second"]);
        assert!(
            window.update(|_, cx| harness.read(cx).renders) > after_press,
            "releasing Enter must repaint the button with its rest fill"
        );
    }

    /// A focused button answers Enter and Space itself even when an ancestor
    /// binds the same keys (the keymap binds Enter in many window contexts);
    /// with focus on the ancestor, the binding runs.
    #[gpui::test]
    fn a_focused_button_takes_enter_before_ancestor_bindings(cx: &mut gpui::TestAppContext) {
        let (harness, clicks, window) = open_toolbar(cx);
        window.update(|_, cx| {
            cx.bind_keys([
                gpui::KeyBinding::new("enter", HarnessEnter, Some("ToolbarHarness")),
                gpui::KeyBinding::new("space", HarnessEnter, Some("ToolbarHarness")),
            ])
        });

        window.simulate_keystrokes("tab");
        press(window, "enter");
        press(window, "space");
        assert_eq!(*clicks.borrow(), vec!["first", "first"]);

        window.update(|window, cx| harness.read(cx).focus_handle.clone().focus(window, cx));
        window.run_until_parked();
        press(window, "enter");
        assert_eq!(*clicks.borrow(), vec!["first", "first", "parent"]);
    }
}
