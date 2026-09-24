use crate::icon::IconSource;
use crate::icons::AppIcon;
use crate::primitives::{IconButton, overlay_bg, surface_modal_container};
use crate::semantic::BannerColors as SemBannerColors;
use crate::tokens::{ChromeEdgeRole, FontSizes, Heights, Spacing};
use dbflux_core::LogErr;
use gpui::prelude::*;
use gpui::{
    AnyElement, AnyWindowHandle, App, FocusHandle, KeyDownEvent, Keystroke, MouseButton, Pixels,
    SharedString, Window, div, px,
};
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;

type CloseHandler = Box<dyn Fn(&mut Window, &mut App) + Send + Sync + 'static>;
type ConfirmHandler = Box<dyn Fn(&mut Window, &mut App) + 'static>;

/// Element id of the shell's close button.
pub const MODAL_SHELL_CLOSE_ID: &str = "modal-shell-close";

/// Element id of the shell's backdrop.
pub const MODAL_SHELL_BACKDROP_ID: &str = "modal-shell-backdrop";

/// Debug selector of the shell's body content, for layout tests.
pub const MODAL_SHELL_BODY_SELECTOR: &str = "modal-shell-body";

/// Debug selector of the shell's footer, for layout tests.
pub const MODAL_SHELL_FOOTER_SELECTOR: &str = "modal-shell-footer";

/// Smallest height of the body area, padding included.
const BODY_MIN_HEIGHT: Pixels = px(96.0);

/// A key the shell answers while focus is inside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShellKey {
    Cancel,
    Confirm,
}

/// Maps a keystroke to the shell action it triggers. Only the bare keys
/// count: a chord such as Ctrl+Enter belongs to whatever binds it.
fn shell_key(keystroke: &Keystroke) -> Option<ShellKey> {
    if keystroke.modifiers.modified() {
        return None;
    }

    match keystroke.key.as_str() {
        "escape" => Some(ShellKey::Cancel),
        "enter" => Some(ShellKey::Confirm),
        _ => None,
    }
}

/// Tone variant for `ModalShell`.
///
/// - `Default`: standard chrome with no accent border.
/// - `Danger`: red 2 px top-border signalling a destructive or critical action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModalVariant {
    Default,
    Danger,
}

/// Reusable modal shell providing:
/// - Scrim overlay (dimmed backdrop)
/// - Title bar with optional close button
/// - Body area that fits its content (min-height 96 px, 16 px padding) and
///   scrolls once the card reaches 90% of the viewport height
/// - Footer area (right-aligned, 12 px gap between items)
/// - Danger variant: 2 px red top-border accent
///
/// Use this as the chrome for any new modal. Pass pre-built `AnyElement`
/// values for `body` and `footer` to keep the component stateless.
///
/// S8 modals (e.g. drop-confirm, unsaved-changes) should use `ModalShell`
/// rather than implementing their own scrim/header/footer layout.
///
/// Dismissal and confirmation are owned by the shell, not by each modal:
/// - `on_close` is the cancel path. The X button, a click on the backdrop and,
///   when the shell has a focus handle, Escape all call it.
/// - `on_confirm` is the primary action. With a focus handle, Enter calls it
///   while `confirm_enabled` is true, the same condition that should enable
///   the primary button. A multi-line editor keeps Enter for itself, so Enter
///   confirms from a single-line input but not from a code or text editor.
///
/// Keyboard handling only works while focus is inside the shell, which is why
/// it is opt-in through [`ModalShell::focus_handle`]; [`ModalFocus`] moves focus
/// in when a modal opens and hands it back when it closes.
#[derive(IntoElement)]
pub struct ModalShell {
    title: SharedString,
    variant: ModalVariant,
    width: Pixels,
    body: AnyElement,
    footer: AnyElement,
    on_close: Option<CloseHandler>,
    on_confirm: Option<ConfirmHandler>,
    confirm_enabled: bool,
    focus_handle: Option<FocusHandle>,
}

impl ModalShell {
    pub fn new(title: impl Into<SharedString>, body: AnyElement, footer: AnyElement) -> Self {
        Self {
            title: title.into(),
            variant: ModalVariant::Default,
            width: px(460.0),
            body,
            footer,
            on_close: None,
            on_confirm: None,
            confirm_enabled: true,
            focus_handle: None,
        }
    }

    /// Set the danger variant (red accent top-border).
    pub fn variant(mut self, v: ModalVariant) -> Self {
        self.variant = v;
        self
    }

    /// Override the modal width (default: 460 px).
    pub fn width(mut self, w: Pixels) -> Self {
        self.width = w;
        self
    }

    /// Attach the cancel handler, called by the X button, a backdrop click and
    /// Escape. Without it the shell draws no close button.
    pub fn on_close(mut self, f: impl Fn(&mut Window, &mut App) + Send + Sync + 'static) -> Self {
        self.on_close = Some(Box::new(f));
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

    /// Track `handle` on the backdrop so Escape and Enter reach the shell
    /// whenever focus is inside the modal.
    pub fn focus_handle(mut self, handle: &FocusHandle) -> Self {
        self.focus_handle = Some(handle.clone());
        self
    }
}

/// Keyboard focus for a modal rendered in a [`ModalShell`].
///
/// The shell only hears Escape and Enter while focus is inside it, so a modal
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

    /// The handle to pass to [`ModalShell::focus_handle`].
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

    /// Moves focus to `target`, or to the shell itself, and remembers what
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

impl RenderOnce for ModalShell {
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let border_color = ChromeEdgeRole::ModalSeparator.resolve(theme);

        // Cap the card to the viewport so a tall body scrolls inside the shell
        // instead of pushing the footer off-screen, and so a wide card shrinks
        // on narrow windows.
        let viewport = window.viewport_size();
        let max_card_height = viewport.height * 0.9;
        let max_card_width = viewport.width * 0.95;

        // Danger accent: 2 px red top-border.
        let danger_accent = if self.variant == ModalVariant::Danger {
            Some(SemBannerColors::for_current(cx).error_fg)
        } else {
            None
        };

        let close_handler = self.on_close.map(Arc::new);

        let close_btn = close_handler.as_ref().map(|handler| {
            let h = handler.clone();
            IconButton::new(
                MODAL_SHELL_CLOSE_ID,
                IconSource::Svg(AppIcon::X.path().into()),
            )
            .icon_size(Heights::ICON_SM)
            .on_click(move |_, window, cx| (h)(window, cx))
            .into_any_element()
        });

        // Header bar (32 px toolbar height).
        let header = div()
            .flex()
            .items_center()
            .justify_between()
            .px(Spacing::MD)
            .h(Heights::TOOLBAR)
            .flex_shrink_0()
            .border_b_1()
            .border_color(border_color)
            .child(
                div().flex().items_center().gap(Spacing::SM).child(
                    div()
                        .text_size(FontSizes::SM)
                        .font_weight(gpui::FontWeight::SEMIBOLD)
                        .text_color(theme.foreground)
                        .child(self.title),
                ),
            )
            .when_some(close_btn, |h, btn| h.child(btn));

        // Body area. `flex_1` lets it take its content height and shrink to
        // scroll once the card reaches `max_card_height`. The minimum height
        // sits on an inner wrapper: on the scroll region itself, the
        // scrollbar wrapper sizes the region to that minimum instead of its
        // content, and the footer then covers the rest of the body.
        let body = div()
            .flex_1()
            .p(Spacing::LG)
            .debug_selector(|| MODAL_SHELL_BODY_SELECTOR.to_string())
            .overflow_y_scrollbar()
            .child(
                div()
                    .min_h(BODY_MIN_HEIGHT - Spacing::LG * 2.0)
                    .child(self.body),
            );

        // Footer (right-aligned, 12 px gap).
        let footer = div()
            .flex()
            .items_center()
            .justify_end()
            .gap(Spacing::MD)
            .px(Spacing::MD)
            .py(Spacing::SM)
            .flex_shrink_0()
            .border_t_1()
            .border_color(border_color)
            .debug_selector(|| MODAL_SHELL_FOOTER_SELECTOR.to_string())
            .child(self.footer);

        // Card container.
        let mut card = surface_modal_container(cx)
            .w(self.width)
            .max_w(max_card_width)
            .max_h(max_card_height)
            .shadow_lg()
            .overflow_hidden()
            .flex()
            .flex_col()
            .child(header)
            .child(body)
            .child(footer);

        if let Some(accent) = danger_accent {
            card = card.border_t_2().border_color(accent);
        }

        let close_for_overlay = close_handler.clone();
        let close_for_keys = close_handler;
        let confirm_for_keys = self.on_confirm;
        let confirm_enabled = self.confirm_enabled;

        // Scrim / overlay backdrop. Center the card on both axes so it
        // sits in the middle of the viewport instead of anchored to the
        // top (which made it feel off-screen on tall windows).
        div()
            .id(MODAL_SHELL_BACKDROP_ID)
            .when_some(self.focus_handle, |overlay, handle| {
                // Bubble phase: a multi-line editor has already consumed its
                // own Enter by the time the event reaches the backdrop, while
                // a single-line input lets Enter through.
                overlay
                    .track_focus(&handle)
                    .on_key_down(move |event: &KeyDownEvent, window, cx| {
                        let Some(key) = shell_key(&event.keystroke) else {
                            return;
                        };

                        // The modal owns these keys while it has focus; nothing
                        // behind it may act on them.
                        cx.stop_propagation();

                        match key {
                            ShellKey::Cancel => {
                                if let Some(handler) = close_for_keys.as_ref() {
                                    (handler)(window, cx);
                                }
                            }
                            ShellKey::Confirm => {
                                if confirm_enabled && let Some(handler) = confirm_for_keys.as_ref()
                                {
                                    (handler)(window, cx);
                                }
                            }
                        }
                    })
            })
            .absolute()
            .inset_0()
            .bg(overlay_bg(theme))
            .flex()
            .justify_center()
            .items_center()
            .on_mouse_down(MouseButton::Left, move |_, window, cx| {
                if let Some(ref handler) = close_for_overlay {
                    (handler)(window, cx);
                }
            })
            .child(
                div()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
                    .child(card),
            )
    }
}

#[cfg(test)]
mod tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{
        MODAL_SHELL_BACKDROP_ID, MODAL_SHELL_CLOSE_ID, ModalFocus, ModalShell, ShellKey, shell_key,
    };
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
                ModalShell::new("Harness", body.into_any_element(), div().into_any_element())
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
    fn only_bare_escape_and_enter_are_shell_keys() {
        let parse = |text: &str| Keystroke::parse(text).expect("valid keystroke");

        assert_eq!(shell_key(&parse("escape")), Some(ShellKey::Cancel));
        assert_eq!(shell_key(&parse("enter")), Some(ShellKey::Confirm));
        assert_eq!(shell_key(&parse("ctrl-enter")), None);
        assert_eq!(shell_key(&parse("shift-enter")), None);
        assert_eq!(shell_key(&parse("a")), None);
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
            .bounds_of(MODAL_SHELL_CLOSE_ID)
            .expect("the shell draws a close button");
        setup.click(close.center());

        assert_eq!(setup.events(), ["cancel"]);
    }

    #[gpui::test]
    fn a_backdrop_click_cancels(cx: &mut TestAppContext) {
        let mut setup = setup(cx, true);

        let backdrop = setup
            .capture
            .bounds_of(MODAL_SHELL_BACKDROP_ID)
            .expect("the shell draws a backdrop");
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
