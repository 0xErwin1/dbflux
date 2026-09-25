use crate::controls::Button;
use crate::icon::IconSource;
use crate::icons::AppIcon;
use crate::primitives::{Chamfer, Icon, Kbd, SurfaceRole, Text, inspect_surface_role, overlay_bg};
use crate::tokens::{ChamferCut, ChromeColors, ModalMetrics};
use dbflux_core::LogErr;
use gpui::prelude::*;
use gpui::{
    AnyElement, AnyWindowHandle, App, ElementId, FocusHandle, Hsla, KeyDownEvent, Keystroke,
    MouseButton, Pixels, SharedString, Window, div,
};
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;

type CloseHandler = Arc<dyn Fn(&mut Window, &mut App) + Send + Sync + 'static>;
type ConfirmHandler = Box<dyn Fn(&mut Window, &mut App) + 'static>;

/// Element id of the modal's close button.
pub const MODAL_CLOSE_ID: &str = "modal-close";

/// Default element id of the modal's backdrop.
pub const MODAL_BACKDROP_ID: &str = "modal-backdrop";

/// Debug selector of the padded body content, for layout tests.
pub const MODAL_BODY_SELECTOR: &str = "modal-body";

/// Debug selector of the footer, for layout tests.
pub const MODAL_FOOTER_SELECTOR: &str = "modal-footer";

/// Share of the viewport height a modal without a fixed height may take.
const MAX_VIEWPORT_HEIGHT: f32 = 0.9;

/// Share of the viewport width a modal may take.
const MAX_VIEWPORT_WIDTH: f32 = 0.95;

/// A key the modal answers while focus is inside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModalKey {
    Cancel,
    Confirm,
}

/// Maps a keystroke to the modal action it triggers. Only the bare keys
/// count: a chord such as Ctrl+Enter belongs to whatever binds it.
fn modal_key(keystroke: &Keystroke) -> Option<ModalKey> {
    if keystroke.modifiers.modified() {
        return None;
    }

    match keystroke.key.as_str() {
        "escape" => Some(ModalKey::Cancel),
        "enter" => Some(ModalKey::Confirm),
        _ => None,
    }
}

/// Tone of a modal.
///
/// - `Default`: tint header icon.
/// - `Danger`: red 2 px edge along the top of the card and a red header icon,
///   for destructive or critical actions. Pair it with a `Button::danger`
///   primary and, when the action cannot be undone, a `TypeToConfirm` body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModalVariant {
    Default,
    Danger,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ModalHeight {
    /// Fits the content, up to 90% of the viewport; the padded body scrolls.
    Auto,
    Fixed(Pixels),
    Max(Pixels),
    Fraction(f32),
}

/// Vertical placement of the card inside the backdrop.
#[derive(Clone, Copy, Debug, PartialEq)]
enum ModalPlacement {
    Centered,
    Top(Pixels),
}

/// The one modal of the app (P1Modals, DSAppPlan "Modal").
///
/// A cut-18 card on the panel fill with a line-2 border, over the per-theme
/// scrim. The header holds the icon, the title, optional extra content, the
/// Esc keycap and the close button; the body comes next; the footer holds the
/// actions, primary last.
///
/// Content goes in one of two ways:
/// - [`Modal::body`] and [`Modal::footer`]: a padded body that fits its
///   content and scrolls once the card reaches 90% of the viewport, then a
///   right-aligned footer. Use this for dialogs.
/// - [`Modal::child`]: raw children laid out in the card's column, for
///   editors and wizards that size and scroll their own content.
///
/// Dismissal and confirmation are owned by the modal:
/// - `on_close` is the cancel path. The close button and a click on the
///   backdrop call it; so does Escape while focus is inside the modal.
/// - `on_confirm` is the primary action. Enter calls it while
///   `confirm_enabled` is true, the same condition that should enable the
///   primary button. A multi-line editor keeps Enter for itself.
///
/// Keyboard handling needs focus inside the modal, which is why it is tied to
/// [`Modal::focus_handle`]; [`ModalFocus`] moves focus in when a modal opens
/// and hands it back when it closes. A modal with a [`Modal::key_context`]
/// leaves Escape to the keymap of that context, which dispatches
/// `actions::Cancel`, and the modal closes on that action instead.
#[derive(IntoElement)]
pub struct Modal {
    id: ElementId,
    title: SharedString,
    icon: Option<IconSource>,
    icon_color: Option<Hsla>,
    variant: ModalVariant,
    width: Pixels,
    height: ModalHeight,
    placement: ModalPlacement,
    header_extra: Option<AnyElement>,
    body: Option<AnyElement>,
    footer: Option<AnyElement>,
    children: Vec<AnyElement>,
    on_close: Option<CloseHandler>,
    on_confirm: Option<ConfirmHandler>,
    confirm_enabled: bool,
    focus_handle: Option<FocusHandle>,
    key_context: Option<SharedString>,
    block_scroll: bool,
}

impl Modal {
    pub fn new(title: impl Into<SharedString>) -> Self {
        Self {
            id: MODAL_BACKDROP_ID.into(),
            title: title.into(),
            icon: None,
            icon_color: None,
            variant: ModalVariant::Default,
            width: ModalMetrics::WIDTH,
            height: ModalHeight::Auto,
            placement: ModalPlacement::Centered,
            header_extra: None,
            body: None,
            footer: None,
            children: Vec::new(),
            on_close: None,
            on_confirm: None,
            confirm_enabled: true,
            focus_handle: None,
            key_context: None,
            block_scroll: false,
        }
    }

    /// Element id of the backdrop (default: [`MODAL_BACKDROP_ID`]).
    pub fn id(mut self, id: impl Into<ElementId>) -> Self {
        self.id = id.into();
        self
    }

    pub fn variant(mut self, variant: ModalVariant) -> Self {
        self.variant = variant;
        self
    }

    pub fn danger(self) -> Self {
        self.variant(ModalVariant::Danger)
    }

    /// Leading header icon, tinted for `Default` and red for `Danger`.
    pub fn icon(mut self, icon: impl Into<IconSource>) -> Self {
        self.icon = Some(icon.into());
        self
    }

    /// Replace the variant color of the header icon (for example warning).
    pub fn icon_color(mut self, color: impl Into<Hsla>) -> Self {
        self.icon_color = Some(color.into());
        self
    }

    /// Card width (default: `ModalMetrics::WIDTH`), capped at 95% of the viewport.
    pub fn width(mut self, width: Pixels) -> Self {
        self.width = width;
        self
    }

    pub fn height(mut self, height: Pixels) -> Self {
        self.height = ModalHeight::Fixed(height);
        self
    }

    pub fn max_height(mut self, height: Pixels) -> Self {
        self.height = ModalHeight::Max(height);
        self
    }

    /// Size the card as a fraction of the viewport height (e.g. `0.8` = 80%).
    pub fn height_fraction(mut self, fraction: f32) -> Self {
        self.height = ModalHeight::Fraction(fraction);
        self
    }

    /// Anchor the card `offset` below the top of the backdrop instead of
    /// centering it.
    pub fn top_offset(mut self, offset: Pixels) -> Self {
        self.placement = ModalPlacement::Top(offset);
        self
    }

    /// Extra header content placed after the title (a type badge, a count).
    pub fn header_extra(mut self, element: impl IntoElement) -> Self {
        self.header_extra = Some(element.into_any_element());
        self
    }

    /// The padded, scrolling body.
    pub fn body(mut self, body: impl IntoElement) -> Self {
        self.body = Some(body.into_any_element());
        self
    }

    /// The footer, right-aligned; put the primary action last.
    pub fn footer(mut self, footer: impl IntoElement) -> Self {
        self.footer = Some(footer.into_any_element());
        self
    }

    /// Raw content laid out in the card's column after the header.
    pub fn child(mut self, child: impl IntoElement) -> Self {
        self.children.push(child.into_any_element());
        self
    }

    /// Attach the cancel handler, called by the close button, a backdrop
    /// click and Escape. Without it the modal draws no close button.
    pub fn on_close(mut self, f: impl Fn(&mut Window, &mut App) + Send + Sync + 'static) -> Self {
        self.on_close = Some(Arc::new(f));
        self
    }

    /// Attach the confirm handler that Enter calls while `confirm_enabled`.
    pub fn on_confirm(mut self, f: impl Fn(&mut Window, &mut App) + 'static) -> Self {
        self.on_confirm = Some(Box::new(f));
        self
    }

    /// Whether Enter may confirm right now (default: `true`). Pass the same
    /// condition that enables the primary button.
    pub fn confirm_enabled(mut self, enabled: bool) -> Self {
        self.confirm_enabled = enabled;
        self
    }

    /// Track `handle` on the backdrop so Escape and Enter reach the modal
    /// whenever focus is inside it.
    pub fn focus_handle(mut self, handle: &FocusHandle) -> Self {
        self.focus_handle = Some(handle.clone());
        self
    }

    /// Key context of the backdrop. Escape is then left to that context's
    /// keymap and the modal closes on `actions::Cancel`.
    pub fn key_context(mut self, key_context: impl Into<SharedString>) -> Self {
        self.key_context = Some(key_context.into());
        self
    }

    /// Stop scroll-wheel events from reaching what is behind the modal.
    pub fn block_scroll(mut self) -> Self {
        self.block_scroll = true;
        self
    }
}

/// Keyboard focus for a modal rendered in a [`Modal`].
///
/// The modal only hears Escape and Enter while focus is inside it, so a modal
/// moves focus in when it opens and gives it back when it closes. Without the
/// hand-back, closing the modal would leave focus on an element that is no
/// longer rendered and the keyboard would stop responding until a click.
pub struct ModalFocus {
    handle: FocusHandle,
    focus_pending: bool,
    open: Rc<Cell<bool>>,
    previous: Option<(AnyWindowHandle, FocusHandle)>,
}

impl ModalFocus {
    pub fn new(cx: &mut App) -> Self {
        Self {
            handle: cx.focus_handle(),
            focus_pending: false,
            open: Rc::new(Cell::new(false)),
            previous: None,
        }
    }

    /// The handle to pass to [`Modal::focus_handle`].
    pub fn handle(&self) -> &FocusHandle {
        &self.handle
    }

    /// Moves focus into the modal on its next render. For modals that are
    /// opened without access to the window.
    pub fn focus_on_next_render(&mut self) {
        self.focus_pending = true;
    }

    /// Applies a request made with [`Self::focus_on_next_render`]. Call it at
    /// the start of the modal's `render`.
    pub fn apply_pending(&mut self, window: &mut Window, cx: &mut App) {
        if std::mem::take(&mut self.focus_pending) {
            self.focus(None, window, cx);
        }
    }

    /// Moves focus to `target`, or to the modal itself, and remembers what
    /// had focus before the modal opened.
    pub fn focus(&mut self, target: Option<&FocusHandle>, window: &mut Window, cx: &mut App) {
        self.focus_pending = false;
        self.open.set(true);

        if self.previous.is_none() {
            self.previous = window
                .focused(cx)
                .filter(|focused| focused != &self.handle)
                .map(|focused| (window.window_handle(), focused));
        }

        target.unwrap_or(&self.handle).focus(window, cx);
    }

    /// Gives focus back to what had it before the modal opened.
    ///
    /// Runs deferred, after the outcome the modal emitted has been handled,
    /// and only while focus is still inside the modal: a host that moved
    /// focus somewhere else in response to the outcome keeps its choice, and a
    /// modal reopened in the meantime keeps its focus.
    pub fn restore(&mut self, cx: &mut App) {
        self.focus_pending = false;
        self.open.set(false);

        let Some((window_handle, previous)) = self.previous.take() else {
            return;
        };

        let modal = self.handle.clone();
        let open = self.open.clone();

        cx.defer(move |cx| {
            if open.get() {
                return;
            }

            window_handle
                .update(cx, |_, window, cx| {
                    if modal.contains_focused(window, cx) {
                        previous.focus(window, cx);
                    }
                })
                .log_err();
        });
    }
}

impl RenderOnce for Modal {
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let line = theme.border;
        let surface = inspect_surface_role(SurfaceRole::Modal);
        let is_danger = self.variant == ModalVariant::Danger;
        let icon_color = self.icon_color.unwrap_or(if is_danger {
            theme.danger
        } else {
            ChromeColors::tint(theme)
        });

        // Cap the card to the viewport so a tall body scrolls inside the modal
        // instead of pushing the footer off-screen, and so a wide card shrinks
        // on narrow windows.
        let viewport = window.viewport_size();

        let close_handler = self.on_close;

        let close_control = close_handler.as_ref().map(|handler| {
            let handler = handler.clone();

            div()
                .flex()
                .items_center()
                .gap(ModalMetrics::FOOTER_GAP)
                .child(Kbd::new("Esc"))
                .child(
                    Button::new(MODAL_CLOSE_ID, "")
                        .ghost()
                        .small()
                        .icon(AppIcon::CircleX)
                        .icon_size(ModalMetrics::CLOSE_ICON)
                        .icon_only()
                        .on_click(move |_, window, cx| (handler)(window, cx)),
                )
        });

        let header = div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(ModalMetrics::HEADER_GAP)
            .h(ModalMetrics::HEADER_HEIGHT)
            .px(ModalMetrics::PADDING)
            .border_b_1()
            .border_color(line)
            .when_some(self.icon, |header, icon| {
                header.child(Icon::new(icon).size(ModalMetrics::ICON).color(icon_color))
            })
            .child(
                div()
                    .min_w_0()
                    .truncate()
                    .child(Text::heading(self.title).font_size(ModalMetrics::TITLE_SIZE)),
            )
            .when_some(self.header_extra, |header, extra| header.child(extra))
            .child(div().flex_1())
            .when_some(close_control, |header, close| header.child(close));

        let mut shape = Chamfer::new(ChamferCut::MODAL)
            .fill(surface.fill.resolve(theme))
            .border(surface.border.resolve(theme));

        if is_danger {
            shape = shape.top_edge(theme.danger, ModalMetrics::DANGER_EDGE);
        }

        let mut card = div()
            .relative()
            .w(self.width)
            .max_w(viewport.width * MAX_VIEWPORT_WIDTH)
            .flex()
            .flex_col()
            .overflow_hidden()
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .child(shape)
            .child(header)
            .children(self.children);

        card = match self.height {
            ModalHeight::Auto => card.max_h(viewport.height * MAX_VIEWPORT_HEIGHT),
            ModalHeight::Fixed(height) => card.h(height),
            ModalHeight::Max(height) => card.max_h(height),
            ModalHeight::Fraction(fraction) => card.h(gpui::relative(fraction)),
        };

        // `flex_1` lets the body take its content height and shrink to scroll
        // once the card reaches its maximum height. The minimum height sits on
        // an inner wrapper: on the scroll region itself, the scrollbar wrapper
        // sizes the region to that minimum instead of its content, and the
        // footer then covers the rest of the body.
        if let Some(body) = self.body {
            card = card.child(
                div()
                    .flex_1()
                    .p(ModalMetrics::PADDING)
                    .debug_selector(|| MODAL_BODY_SELECTOR.to_string())
                    .overflow_y_scrollbar()
                    .child(
                        div()
                            .min_h(ModalMetrics::BODY_MIN_HEIGHT - ModalMetrics::PADDING * 2.0)
                            .child(body),
                    ),
            );
        }

        if let Some(footer) = self.footer {
            card = card.child(
                div()
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .justify_end()
                    .gap(ModalMetrics::FOOTER_GAP)
                    .px(ModalMetrics::PADDING)
                    .py(ModalMetrics::FOOTER_PADDING_Y)
                    .border_t_1()
                    .border_color(line)
                    .debug_selector(|| MODAL_FOOTER_SELECTOR.to_string())
                    .child(footer),
            );
        }

        let close_for_backdrop = close_handler.clone();
        let close_for_keys = close_handler.clone();
        let close_for_action = close_handler;
        let confirm_for_keys = self.on_confirm;
        let confirm_enabled = self.confirm_enabled;
        let keymap_owns_escape = self.key_context.is_some();

        let mut backdrop = div()
            .id(self.id)
            .absolute()
            .inset_0()
            .bg(overlay_bg(theme))
            .flex()
            .justify_center()
            .on_mouse_down(MouseButton::Left, move |_, window, cx| {
                // Nothing behind the modal may react to a click on the scrim.
                cx.stop_propagation();

                if let Some(handler) = close_for_backdrop.as_ref() {
                    (handler)(window, cx);
                }
            });

        backdrop = match self.placement {
            ModalPlacement::Centered => backdrop.items_center(),
            ModalPlacement::Top(offset) => backdrop.items_start().pt(offset),
        };

        if let Some(handle) = self.focus_handle {
            backdrop = backdrop.track_focus(&handle);

            if !keymap_owns_escape {
                // Bubble phase: a multi-line editor has already consumed its
                // own Enter by the time the event reaches the backdrop, while
                // a single-line input lets Enter through.
                backdrop = backdrop.on_key_down(move |event: &KeyDownEvent, window, cx| {
                    let Some(key) = modal_key(&event.keystroke) else {
                        return;
                    };

                    // The modal owns these keys while it has focus; nothing
                    // behind it may act on them.
                    cx.stop_propagation();

                    match key {
                        ModalKey::Cancel => {
                            if let Some(handler) = close_for_keys.as_ref() {
                                (handler)(window, cx);
                            }
                        }
                        ModalKey::Confirm => {
                            if confirm_enabled && let Some(handler) = confirm_for_keys.as_ref() {
                                (handler)(window, cx);
                            }
                        }
                    }
                });
            }
        }

        if let Some(key_context) = self.key_context {
            backdrop = backdrop.key_context(key_context.as_ref()).on_action(
                move |_: &crate::actions::Cancel, window, cx| {
                    if let Some(handler) = close_for_action.as_ref() {
                        (handler)(window, cx);
                    }
                },
            );
        }

        if self.block_scroll {
            backdrop = backdrop.on_scroll_wheel(|_, _, cx| {
                cx.stop_propagation();
            });
        }

        backdrop.child(card)
    }
}

#[cfg(test)]
mod tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{MODAL_BACKDROP_ID, MODAL_CLOSE_ID, Modal, ModalFocus, ModalKey, modal_key};
    use crate::controls::{Input, InputState};
    use gpui::{
        AccessibilityFrame, AppContext as _, Bounds, Context, Entity, FocusHandle, FrameObserver,
        InteractiveElement as _, IntoElement, Keystroke, Modifiers, ParentElement as _, Pixels,
        Render, Styled as _, TestAppContext, VisualTestContext, Window, div, point, px,
    };
    use gpui_component::input::{Editor, EditorState};
    use std::sync::{Arc, Mutex};

    /// Keeps the latest rendered frame of the window it observes.
    #[derive(Default)]
    struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

    impl FrameObserver for FrameCapture {
        fn accessibility_updated(&self, frame: &AccessibilityFrame) {
            *self.0.lock().expect("frame capture lock") = Some(frame.clone());
        }
    }

    impl FrameCapture {
        fn bounds_of(&self, id: &str) -> Option<Bounds<Pixels>> {
            let frame = self.0.lock().expect("frame capture lock").clone()?;
            frame
                .nodes()
                .find(|(_, node)| node.id() == id)
                .map(|(_, node)| node.bounds())
        }
    }

    type Log = Arc<Mutex<Vec<&'static str>>>;

    struct Harness {
        focus: FocusHandle,
        confirm_enabled: bool,
        single_line: Entity<InputState>,
        multi_line: Entity<EditorState>,
        log: Log,
    }

    impl Render for Harness {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            let body = div()
                .flex()
                .flex_col()
                .child(Input::new(&self.single_line))
                .child(
                    div()
                        .id("harness-editor")
                        .h(px(80.0))
                        .child(Editor::new(&self.multi_line)),
                );

            let cancel_log = self.log.clone();
            let confirm_log = self.log.clone();

            div().size_full().child(
                Modal::new("Harness")
                    .body(body)
                    .footer(div())
                    .focus_handle(&self.focus)
                    .on_close(move |_, _| {
                        cancel_log.lock().expect("log lock").push("cancel");
                    })
                    .on_confirm(move |_, _| {
                        confirm_log.lock().expect("log lock").push("confirm");
                    })
                    .confirm_enabled(self.confirm_enabled),
            )
        }
    }

    struct Setup<'a> {
        view: Entity<Harness>,
        window: &'a mut VisualTestContext,
        capture: Arc<FrameCapture>,
        log: Log,
    }

    fn setup(cx: &mut TestAppContext, confirm_enabled: bool) -> Setup<'_> {
        cx.update(gpui_component::init);

        let log: Log = Arc::default();
        let capture = Arc::new(FrameCapture::default());
        let (view, window) = cx.add_window_view({
            let log = log.clone();
            let capture = capture.clone();
            move |window, cx| {
                window.observe_frames(&capture);
                Harness {
                    focus: cx.focus_handle(),
                    confirm_enabled,
                    single_line: cx.new(|cx| InputState::new(window, cx)),
                    multi_line: cx.new(|cx| EditorState::new(window, cx)),
                    log,
                }
            }
        });
        window.run_until_parked();

        Setup {
            view,
            window,
            capture,
            log,
        }
    }

    impl Setup<'_> {
        fn focus_shell(&mut self) {
            let view = self.view.clone();
            self.window.update(|window, cx| {
                let handle = view.read(cx).focus.clone();
                handle.focus(window, cx);
            });
            self.window.run_until_parked();
        }

        fn focus_single_line(&mut self) {
            let view = self.view.clone();
            self.window.update(|window, cx| {
                let input = view.read(cx).single_line.clone();
                input.update(cx, |state, cx| state.focus(window, cx));
            });
            self.window.run_until_parked();
        }

        fn focus_multi_line(&mut self) {
            let view = self.view.clone();
            self.window.update(|window, cx| {
                let editor = view.read(cx).multi_line.clone();
                editor.update(cx, |state, cx| state.focus(window, cx));
            });
            self.window.run_until_parked();
        }

        fn click(&mut self, position: gpui::Point<Pixels>) {
            self.window.simulate_click(position, Modifiers::default());
            self.window.run_until_parked();
        }

        fn events(&self) -> Vec<&'static str> {
            self.log.lock().expect("log lock").clone()
        }
    }

    #[test]
    fn only_bare_escape_and_enter_are_modal_keys() {
        let parse = |text: &str| Keystroke::parse(text).expect("valid keystroke");

        assert_eq!(modal_key(&parse("escape")), Some(ModalKey::Cancel));
        assert_eq!(modal_key(&parse("enter")), Some(ModalKey::Confirm));
        assert_eq!(modal_key(&parse("ctrl-enter")), None);
        assert_eq!(modal_key(&parse("shift-enter")), None);
        assert_eq!(modal_key(&parse("a")), None);
    }

    #[gpui::test]
    fn escape_cancels(cx: &mut TestAppContext) {
        let mut setup = setup(cx, true);
        setup.focus_shell();

        setup.window.simulate_keystrokes("escape");

        assert_eq!(setup.events(), ["cancel"]);
    }

    #[gpui::test]
    fn escape_from_a_focused_input_cancels(cx: &mut TestAppContext) {
        let mut setup = setup(cx, true);
        setup.focus_single_line();

        setup.window.simulate_keystrokes("escape");

        assert_eq!(setup.events(), ["cancel"]);
    }

    #[gpui::test]
    fn enter_confirms_when_the_modal_is_valid(cx: &mut TestAppContext) {
        let mut setup = setup(cx, true);
        setup.focus_shell();

        setup.window.simulate_keystrokes("enter");

        assert_eq!(setup.events(), ["confirm"]);
    }

    #[gpui::test]
    fn enter_does_nothing_while_the_modal_is_invalid(cx: &mut TestAppContext) {
        let mut setup = setup(cx, false);
        setup.focus_shell();

        setup.window.simulate_keystrokes("enter");

        assert!(setup.events().is_empty(), "{:?}", setup.events());
    }

    #[gpui::test]
    fn enter_from_a_single_line_input_confirms(cx: &mut TestAppContext) {
        let mut setup = setup(cx, true);
        setup.focus_single_line();

        setup.window.simulate_keystrokes("enter");

        assert_eq!(setup.events(), ["confirm"]);
    }

    #[gpui::test]
    fn enter_in_a_multi_line_editor_stays_in_the_editor(cx: &mut TestAppContext) {
        let mut setup = setup(cx, true);
        setup.focus_multi_line();

        setup.window.simulate_keystrokes("enter");

        assert!(setup.events().is_empty(), "{:?}", setup.events());
        let text = setup
            .window
            .update(|_, cx| setup.view.read(cx).multi_line.read(cx).value().to_string());
        assert_eq!(text, "\n", "the editor received the newline");
    }

    #[gpui::test]
    fn the_close_button_is_rendered_and_cancels(cx: &mut TestAppContext) {
        let mut setup = setup(cx, true);

        let close = setup
            .capture
            .bounds_of(MODAL_CLOSE_ID)
            .expect("the modal draws a close button");
        setup.click(close.center());

        assert_eq!(setup.events(), ["cancel"]);
    }

    #[gpui::test]
    fn a_backdrop_click_cancels(cx: &mut TestAppContext) {
        let mut setup = setup(cx, true);

        let backdrop = setup
            .capture
            .bounds_of(MODAL_BACKDROP_ID)
            .expect("the modal draws a backdrop");
        setup.click(backdrop.origin + point(px(2.0), px(2.0)));

        assert_eq!(setup.events(), ["cancel"]);
    }

    #[gpui::test]
    fn a_click_inside_the_card_does_not_cancel(cx: &mut TestAppContext) {
        let mut setup = setup(cx, true);

        let editor = setup
            .capture
            .bounds_of("harness-editor")
            .expect("the body is rendered");
        setup.click(editor.center());

        assert!(setup.events().is_empty(), "{:?}", setup.events());
    }

    #[gpui::test]
    fn closing_gives_focus_back_to_what_had_it(cx: &mut TestAppContext) {
        let setup = setup(cx, true);
        let (outside, mut modal_focus) = setup
            .window
            .update(|_, cx| (cx.focus_handle(), ModalFocus::new(cx)));

        setup.window.update(|window, cx| outside.focus(window, cx));
        let shell = setup.view.clone();
        setup.window.update(|window, cx| {
            let handle = shell.read(cx).focus.clone();
            modal_focus.handle = handle;
            modal_focus.focus(None, window, cx);
        });
        setup.window.run_until_parked();

        setup.window.update(|_, cx| modal_focus.restore(cx));
        setup.window.run_until_parked();

        let restored = setup.window.update(|window, _| outside.is_focused(window));
        assert!(restored, "focus returns to the element that had it");
    }
}
