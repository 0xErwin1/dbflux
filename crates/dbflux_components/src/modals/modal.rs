use crate::actions::{RunCommand, shortcut_label};
use crate::controls::Button;
use crate::icon::IconSource;
use crate::icons::AppIcon;
use crate::primitives::{Chamfer, Icon, Kbd, SurfaceRole, Text, inspect_surface_role, overlay_bg};
use crate::tokens::{ChamferCut, ChromeColors, ModalMetrics};
use dbflux_core::LogErr;
use dbflux_core::keymap_types::{Command, ContextId};
use gpui::prelude::*;
use gpui::{
    AnyElement, AnyWindowHandle, App, Div, ElementId, FocusHandle, Hsla, KeyContext, MouseButton,
    Pixels, ScrollHandle, SharedString, Stateful, Window, div, point, px,
};
use gpui_component::scroll::ScrollableElement;
use gpui_component::{ActiveTheme, FocusTrapElement};
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

/// Key context identifier every modal backdrop carries. The keymap's modal
/// keys are bound in it, and `!Modal` in a predicate keeps a binding away from
/// the panels behind an open modal.
pub const MODAL_KEY_CONTEXT: &str = "Modal";

/// Distance one arrow key press scrolls a [`Modal::scroll_with_keys`] region.
const KEY_SCROLL_STEP: Pixels = px(40.0);

/// A scroll a key asks of a [`Modal::scroll_with_keys`] region, from the
/// scroll actions the keymap binds in the modal's key context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScrollKey {
    LineUp,
    LineDown,
    PageUp,
    PageDown,
    Top,
    Bottom,
}

/// The vertical offset `key` moves a region to, clamped to its content.
///
/// Offsets follow gpui: `0` is the top and scrolling down makes the offset
/// more negative, down to `-max_offset`. A page is the viewport height less
/// one step, so a line of context stays in view.
fn scrolled_offset(key: ScrollKey, offset: Pixels, max_offset: Pixels, viewport: Pixels) -> Pixels {
    let page = (viewport - KEY_SCROLL_STEP).max(KEY_SCROLL_STEP);

    let target = match key {
        ScrollKey::LineUp => offset + KEY_SCROLL_STEP,
        ScrollKey::LineDown => offset - KEY_SCROLL_STEP,
        ScrollKey::PageUp => offset + page,
        ScrollKey::PageDown => offset - page,
        ScrollKey::Top => Pixels::ZERO,
        ScrollKey::Bottom => -max_offset,
    };

    target.min(Pixels::ZERO).max(-max_offset)
}

/// Key context of a modal backdrop: `Modal`, plus the owner's identifier.
fn modal_key_context(owner: Option<&SharedString>) -> KeyContext {
    let mut context = KeyContext::default();
    context.add(MODAL_KEY_CONTEXT);

    if let Some(owner) = owner {
        match KeyContext::parse(owner) {
            Ok(owner_context) => context.extend(&owner_context),
            Err(error) => log::error!("Invalid modal key context `{owner}`: {error}"),
        }
    }

    context
}

/// Scrolls the [`Modal::scroll_with_keys`] region on the scroll action `A`,
/// or lets the action through when the modal has no such region.
fn on_scroll_action<A: gpui::Action>(
    backdrop: Stateful<Div>,
    key: ScrollKey,
    scroll: Option<ScrollHandle>,
) -> Stateful<Div> {
    backdrop.on_action(move |_: &A, window, cx| {
        let Some(scroll) = scroll.as_ref() else {
            cx.propagate();
            return;
        };

        let offset = scroll.offset();
        let target = scrolled_offset(
            key,
            offset.y,
            scroll.max_offset().y,
            scroll.bounds().size.height,
        );

        if target != offset.y {
            scroll.set_offset(point(offset.x, target));
            window.refresh();
        }
    })
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
    defer_keys: bool,
    block_scroll: bool,
    cut: Pixels,
    fill: Option<Hsla>,
    border: Option<Hsla>,
    show_header: bool,
    key_scroll: Option<ScrollHandle>,
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
            defer_keys: false,
            block_scroll: false,
            cut: ChamferCut::MODAL,
            fill: None,
            border: None,
            show_header: true,
            key_scroll: None,
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
    /// whenever focus is inside it, and trap focus there: Tab and Shift+Tab
    /// cycle through the modal's controls and never leave it.
    pub fn focus_handle(mut self, handle: &FocusHandle) -> Self {
        self.focus_handle = Some(handle.clone());
        self
    }

    /// Extra key context identifier of the backdrop, next to `Modal`, for
    /// keymap bindings that belong to this modal only.
    pub fn key_context(mut self, key_context: impl Into<SharedString>) -> Self {
        self.key_context = Some(key_context.into());
        self
    }

    /// Leave Escape and Enter to the owner: instead of closing or confirming,
    /// the modal runs the keymap's Cancel or Execute command, so the owner
    /// can first leave a field or a nested state before closing.
    pub fn defer_keys_to_owner(mut self) -> Self {
        self.defer_keys = true;
        self
    }

    /// Stop scroll-wheel events from reaching what is behind the modal.
    pub fn block_scroll(mut self) -> Self {
        self.block_scroll = true;
        self
    }

    /// Corner cut of the card (default: `ChamferCut::MODAL`), for boards that
    /// draw a dialog with a different cut.
    pub fn cut(mut self, cut: Pixels) -> Self {
        self.cut = cut;
        self
    }

    /// Replace the card fill (default: the modal surface fill).
    pub fn fill(mut self, fill: impl Into<Hsla>) -> Self {
        self.fill = Some(fill.into());
        self
    }

    /// Replace the card border (default: the modal surface border), for
    /// boards whose dialog shows no outline.
    pub fn border(mut self, border: impl Into<Hsla>) -> Self {
        self.border = Some(border.into());
        self
    }

    /// Leave out the header bar, for dialogs whose content brings its own
    /// head. Escape and the backdrop still call `on_close`.
    pub fn without_header(mut self) -> Self {
        self.show_header = false;
        self
    }

    /// Scroll the region tracked by `handle` with the arrow keys, Page Up,
    /// Page Down, Home and End while focus is inside the modal and nothing
    /// inside it handled the key first. Needs [`Modal::focus_handle`].
    pub fn scroll_with_keys(mut self, handle: &ScrollHandle) -> Self {
        self.key_scroll = Some(handle.clone());
        self
    }
}

/// Keyboard focus for a modal rendered in a [`Modal`].
///
/// The modal only hears its keys while focus is inside it, so a modal
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
        let escape_label = shortcut_label(cx, ContextId::Modal.id(), Command::Cancel.id(), "Esc");

        let close_control = close_handler.as_ref().map(|handler| {
            let handler = handler.clone();

            div()
                .flex()
                .items_center()
                .gap(ModalMetrics::FOOTER_GAP)
                .when_some(escape_label.clone(), |control, label| {
                    control.child(Kbd::new(label))
                })
                .child(
                    Button::new(MODAL_CLOSE_ID, "")
                        .ghost()
                        .inline()
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

        let mut shape = Chamfer::new(self.cut)
            .fill(self.fill.unwrap_or_else(|| surface.fill.resolve(theme)))
            .border(self.border.unwrap_or_else(|| surface.border.resolve(theme)));

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
            .when(self.show_header, |card| card.child(header))
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
        let close_for_keys = close_handler;
        let confirm_for_keys: Option<Rc<ConfirmHandler>> = self.on_confirm.map(Rc::new);
        let confirm_enabled = self.confirm_enabled;
        let defer_keys = self.defer_keys;
        let key_scroll = self.key_scroll;

        let trap_id = self.id.clone();
        let mut backdrop = div()
            .id(self.id)
            .absolute()
            .inset_0()
            .bg(overlay_bg(theme))
            .flex()
            .justify_center()
            .key_context(modal_key_context(self.key_context.as_ref()))
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

        // The keymap binds Escape, Enter and the scroll keys in the `Modal`
        // key context. The backdrop sits below the window root, so these
        // bindings win over the panels behind the modal, and a focused input
        // inside the modal still answers its own keys first: a multi-line
        // editor keeps its Enter, a single-line input lets it through.
        backdrop = backdrop
            .on_action(move |_: &crate::actions::Cancel, window, cx| {
                if defer_keys {
                    window.dispatch_action(Box::new(RunCommand::new(Command::Cancel.id())), cx);
                    return;
                }

                match close_for_keys.as_ref() {
                    Some(handler) => (handler)(window, cx),
                    None => cx.propagate(),
                }
            })
            .on_action(move |_: &crate::actions::Execute, window, cx| {
                if defer_keys {
                    window.dispatch_action(Box::new(RunCommand::new(Command::Execute.id())), cx);
                    return;
                }

                match confirm_for_keys.as_ref() {
                    Some(handler) => {
                        if confirm_enabled {
                            (handler)(window, cx);
                        }
                    }
                    None => cx.propagate(),
                }
            });

        backdrop = on_scroll_action::<crate::actions::ScrollUp>(
            backdrop,
            ScrollKey::LineUp,
            key_scroll.clone(),
        );
        backdrop = on_scroll_action::<crate::actions::ScrollDown>(
            backdrop,
            ScrollKey::LineDown,
            key_scroll.clone(),
        );
        backdrop = on_scroll_action::<crate::actions::ScrollPageUp>(
            backdrop,
            ScrollKey::PageUp,
            key_scroll.clone(),
        );
        backdrop = on_scroll_action::<crate::actions::ScrollPageDown>(
            backdrop,
            ScrollKey::PageDown,
            key_scroll.clone(),
        );
        backdrop = on_scroll_action::<crate::actions::ScrollToTop>(
            backdrop,
            ScrollKey::Top,
            key_scroll.clone(),
        );
        backdrop = on_scroll_action::<crate::actions::ScrollToBottom>(
            backdrop,
            ScrollKey::Bottom,
            key_scroll,
        );

        if self.block_scroll {
            backdrop = backdrop.on_scroll_wheel(|_, _, cx| {
                cx.stop_propagation();
            });
        }

        let backdrop = backdrop.child(card);

        // The trap tracks the modal's focus handle and keeps Tab and Shift+Tab
        // cycling inside the modal, so they never reach the view behind it.
        match self.focus_handle {
            Some(handle) => backdrop.focus_trap(trap_id, &handle).into_any_element(),
            None => backdrop.into_any_element(),
        }
    }
}

/// The modal keys the app keymap binds in the `Modal` context (see the modal
/// layer in `dbflux_ui_base::keymap`), for component tests that run without
/// the app keymap.
#[cfg(test)]
pub(crate) fn bind_modal_keys_for_tests(cx: &mut gpui::TestAppContext) {
    use crate::actions;
    use gpui::KeyBinding;

    let context = Some(MODAL_KEY_CONTEXT);
    cx.update(|cx| {
        cx.bind_keys([
            KeyBinding::new("escape", actions::Cancel, context),
            KeyBinding::new("enter", actions::Execute, context),
            KeyBinding::new("up", actions::ScrollUp, context),
            KeyBinding::new("down", actions::ScrollDown, context),
            KeyBinding::new("pageup", actions::ScrollPageUp, context),
            KeyBinding::new("pagedown", actions::ScrollPageDown, context),
            KeyBinding::new("home", actions::ScrollToTop, context),
            KeyBinding::new("end", actions::ScrollToBottom, context),
        ]);
    });
}

#[cfg(test)]
mod tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{
        KEY_SCROLL_STEP, MODAL_BACKDROP_ID, MODAL_CLOSE_ID, Modal, ModalFocus, ScrollKey,
        scrolled_offset,
    };
    use crate::controls::{Input, InputState};
    use gpui::{
        AccessibilityFrame, AppContext as _, Bounds, Context, Entity, FocusHandle, Focusable as _,
        FrameObserver, InteractiveElement as _, IntoElement, Modifiers, ParentElement as _, Pixels,
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
        super::bind_modal_keys_for_tests(cx);

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
    fn scrolling_by_key_stays_inside_the_content() {
        let max = px(500.0);
        let viewport = px(200.0);

        assert_eq!(
            scrolled_offset(ScrollKey::LineDown, Pixels::ZERO, max, viewport),
            -KEY_SCROLL_STEP
        );
        assert_eq!(
            scrolled_offset(ScrollKey::LineUp, Pixels::ZERO, max, viewport),
            Pixels::ZERO
        );
        assert_eq!(
            scrolled_offset(ScrollKey::PageDown, Pixels::ZERO, max, viewport),
            -(viewport - KEY_SCROLL_STEP)
        );
        assert_eq!(
            scrolled_offset(ScrollKey::PageDown, px(-450.0), max, viewport),
            -max
        );
        assert_eq!(
            scrolled_offset(ScrollKey::Bottom, Pixels::ZERO, max, viewport),
            -max
        );
        assert_eq!(
            scrolled_offset(ScrollKey::Top, px(-300.0), max, viewport),
            Pixels::ZERO
        );
        assert_eq!(
            scrolled_offset(ScrollKey::LineDown, Pixels::ZERO, Pixels::ZERO, viewport),
            Pixels::ZERO
        );
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

    /// A modal over a view that has its own focusable input, rendered under
    /// the component `Root`, which answers Tab.
    struct TrapHarness {
        focus: FocusHandle,
        outside: Entity<InputState>,
        first: Entity<InputState>,
        second: Entity<InputState>,
    }

    impl Render for TrapHarness {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            let body = div()
                .flex()
                .flex_col()
                .child(Input::new(&self.first))
                .child(Input::new(&self.second));

            div().size_full().child(Input::new(&self.outside)).child(
                Modal::new("Trap")
                    .body(body)
                    .focus_handle(&self.focus)
                    .on_close(|_, _| {}),
            )
        }
    }

    #[gpui::test]
    fn tab_and_shift_tab_stay_inside_an_open_modal(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        let harness: std::rc::Rc<std::cell::RefCell<Option<Entity<TrapHarness>>>> =
            std::rc::Rc::default();
        let (_, window) = cx.add_window_view({
            let harness = harness.clone();
            move |window, cx| {
                let view = cx.new(|cx| TrapHarness {
                    focus: cx.focus_handle(),
                    outside: cx.new(|cx| InputState::new(window, cx)),
                    first: cx.new(|cx| InputState::new(window, cx)),
                    second: cx.new(|cx| InputState::new(window, cx)),
                });
                harness.replace(Some(view.clone()));
                gpui_component::Root::new(view, window, cx)
            }
        });
        let view = harness.borrow().clone().expect("harness view");
        window.run_until_parked();

        window.update(|window, cx| {
            let first = view.read(cx).first.clone();
            first.update(cx, |state, cx| state.focus(window, cx));
        });
        window.run_until_parked();

        let focus_inside = |window: &mut VisualTestContext| {
            window.update(|window, cx| {
                let view = view.read(cx);
                let outside_focused = view.outside.read(cx).focus_handle(cx).is_focused(window);
                view.focus.contains_focused(window, cx) && !outside_focused
            })
        };

        for step in 0..4 {
            window.simulate_keystrokes("tab");
            assert!(focus_inside(window), "Tab {step} left the modal");
        }

        for step in 0..4 {
            window.simulate_keystrokes("shift-tab");
            assert!(focus_inside(window), "Shift+Tab {step} left the modal");
        }
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
