//! Form popovers: a floating surface anchored to an element or to a rectangle
//! in window coordinates, with the parts of a small dialog.
//!
//! A popover is two pieces that work together:
//!
//! - [`PopoverState`], the entity a host owns. It holds whether the surface is
//!   open, the rectangle it points at, where focus goes when it opens and comes
//!   back from, and the hover-intent timers. A host opens it with
//!   [`PopoverState::open_at`] or [`PopoverState::open`], closes it with
//!   [`PopoverState::close`], and keeps its anchor fresh with
//!   [`PopoverState::set_anchor`].
//! - [`Popover`], the frame the host draws every frame. It draws nothing while
//!   the state is closed, so a host renders it unconditionally. Its parts are a
//!   title, a body the host supplies and a footer of buttons.
//!
//! ```ignore
//! let state = cx.new(|cx| PopoverState::new(cx).hover_intent(true));
//! state.update(cx, |state, _| {
//!     state.set_initial_focus(Some(name_input_focus.clone()));
//!     state.on_dismiss(|window, cx| { /* the host drops its draft */ });
//! });
//!
//! // Where the anchor is drawn, keep the rectangle the surface points at.
//! state.update(cx, |state, _| state.set_anchor(Some(trigger_bounds)));
//!
//! // In the host's render:
//! div().child(
//!     Popover::new("new-workspace", state.clone())
//!         .title("New workspace")
//!         .body(form_field)
//!         .footer(vec![cancel, create]),
//! )
//! ```
//!
//! # What the surface owns
//!
//! - **Parts and chrome.** A cut-12 overlay surface in the panel fill with the
//!   line-2 border, a padded column of title, body and footer, and a footer row
//!   of buttons right-aligned [`PopoverMetrics::FOOTER_GAP`] apart. Its
//!   accessibility role is a dialog, named by its title unless the host passes
//!   its own name, and every part carries a `<id>`, `<id>-title`, `<id>-body` or
//!   `<id>-footer` debug selector.
//! - **Placement.** With an anchor it lands one [`PopoverMetrics::ANCHOR_GAP`]
//!   under that rectangle, flips above it when there is no room below, and is
//!   snapped inside the window keeping [`PopoverMetrics::WINDOW_MARGIN`]. The
//!   anchor is a plain rectangle in window coordinates, so a host anchors the
//!   surface to a text range it measured inside a field as well as to an
//!   element. Without an anchor the surface stays where the host renders it, as
//!   `anchored()` does for a trigger.
//! - **Dismissal.** Escape closes it, and so does a click away from it.
//!   Escape is caught twice, so a host's own key bindings cannot swallow it: the
//!   frame answers the product's cancel action ([`Command::Cancel`], through the
//!   installed [`CommandSet`]) the way a `Dropdown` does, and a raw Escape that
//!   no binding claimed is caught on the way down with `capture_key_down`, before
//!   anything inside the body sees it. The click-away layer is drawn under the
//!   surface rather than around it, so a click in anything drawn above it — a
//!   `Dropdown` list opened from inside the body, for one — is a click inside and
//!   leaves the popover alone.
//! - **Focus.** Opening moves focus to the handle given to
//!   [`PopoverState::set_initial_focus`] (or to the surface itself, with nothing
//!   to focus) and remembers the element that had it; closing gives that element
//!   focus back while focus is still anywhere inside the surface.
//! - **Hover intent.** Built with [`PopoverState::hover_intent`], the surface
//!   opens [`HOVER_DELAY`] after the pointer comes to rest on its anchor, stays
//!   open while the pointer is over the anchor or the surface, and closes
//!   [`HOVER_GRACE`] after the pointer has left both. The host reports the
//!   anchor's hover with [`PopoverState::hover_anchor`]; once the surface is
//!   open it follows the pointer itself.

use std::rc::Rc;
use std::time::Duration;

use gpui::prelude::*;
use gpui::{
    AbsoluteLength, Anchor, AnyElement, App, Bounds, Context, Div, ElementId, Entity, FocusHandle,
    KeyDownEvent, MouseButton, Pixels, Point, Role, SharedString, Size, Stateful, Window, anchored,
    canvas, deferred, div, point, px,
};

use gpui_component::ActiveTheme;

use crate::icon::IconSource;
use crate::keymap::{Command, CommandSet, KeyCommandHandler};
use crate::primitives::{Icon, SurfaceRole, Text, surface};
use crate::selector::id_selector;
use crate::tokens::{ChromeColors, PopoverMetrics, Spacing};

/// How long the pointer rests on an anchor before a hover-intent popover
/// opens.
pub const HOVER_DELAY: Duration = Duration::from_millis(400);

/// How long the pointer may be off both the anchor and the surface before a
/// hover-intent popover closes. Short enough to read as a dismissal, long
/// enough to cross the gap between the anchor and the surface.
pub const HOVER_GRACE: Duration = Duration::from_millis(120);

/// Which side of its anchor a popover prefers to open on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PopoverSide {
    /// Under the anchor, flipping above it when there is no room.
    Below,
    /// Over the anchor, flipping under it when there is no room.
    Above,
}

impl PopoverSide {
    /// The other side, which a placement takes when the preferred one does not
    /// fit.
    pub fn flip(self) -> Self {
        match self {
            Self::Below => Self::Above,
            Self::Above => Self::Below,
        }
    }
}

/// Where a surface goes for an anchor, a size and a window.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PopoverPlacement {
    /// The window-coordinate point the surface's top-left corner sits at.
    pub origin: Point<Pixels>,
    /// The side the surface took, which is not the preferred one when it
    /// flipped.
    pub side: PopoverSide,
}

/// Places a surface of `size` against `anchor`, `gap` away from it, flipping to
/// the other side when the preferred one has no room and keeping `margin` from
/// the window's edges.
///
/// `size` is the laid-out size of the surface. A caller that does not know it
/// yet passes a zero size, which places the surface below the anchor and lets
/// the next frame, with a measured size, flip it.
pub fn place_popover(
    anchor: Bounds<Pixels>,
    size: Size<Pixels>,
    viewport: Size<Pixels>,
    gap: Pixels,
    margin: Pixels,
    preferred: PopoverSide,
) -> PopoverPlacement {
    let below = anchor.bottom() + gap;
    let above = anchor.top() - gap - size.height;
    let lowest = viewport.height - size.height - margin;

    let fits_below = below <= lowest;
    let fits_above = above >= margin;

    let side = if fits_for(preferred, fits_below, fits_above) {
        preferred
    } else if fits_for(preferred.flip(), fits_below, fits_above) {
        preferred.flip()
    } else {
        preferred
    };

    let top = match side {
        PopoverSide::Below => below,
        PopoverSide::Above => above,
    };

    PopoverPlacement {
        origin: point(
            clamp(anchor.left(), margin, viewport.width - size.width - margin),
            clamp(top, margin, lowest),
        ),
        side,
    }
}

/// Whether the room on `side` takes a surface.
fn fits_for(side: PopoverSide, fits_below: bool, fits_above: bool) -> bool {
    match side {
        PopoverSide::Below => fits_below,
        PopoverSide::Above => fits_above,
    }
}

/// `value` kept between `low` and `high`, pinned to `low` when the window is
/// smaller than the surface.
fn clamp(value: Pixels, low: Pixels, high: Pixels) -> Pixels {
    if high < low {
        return low;
    }

    value.max(low).min(high)
}

/// A timer a hover-intent popover is waiting on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimedAction {
    /// The pointer rested on the anchor long enough to open the surface.
    Open,
    /// The pointer left the anchor and the surface, and the grace elapsed.
    Close,
}

/// The hover-intent flags of a popover.
#[derive(Clone, Copy, Debug)]
struct HoverIntent {
    enabled: bool,
    /// The pointer is on the anchor, as the host last reported it.
    over_anchor: bool,
    /// The pointer is on the surface, as the surface last saw it.
    over_popover: bool,
    /// The grace timer is running.
    closing: bool,
}

impl HoverIntent {
    fn disabled() -> Self {
        Self {
            enabled: false,
            over_anchor: false,
            over_popover: false,
            closing: false,
        }
    }
}

/// What the surface calls once it has closed.
type DismissHandler = Rc<dyn Fn(&mut Window, &mut App)>;

/// The open state, anchor, focus and hover timers of a popover.
///
/// The host owns one per popover and renders a [`Popover`] with it; see the
/// module documentation for the whole contract.
pub struct PopoverState {
    open: bool,
    anchor: Option<Bounds<Pixels>>,
    side: PopoverSide,
    /// The surface's laid-out bounds, recorded while it paints. The next frame
    /// places the surface with this size, and the hover intent reads it to know
    /// where the surface is.
    frame: Option<Bounds<Pixels>>,
    /// The surface's own focus: what Escape reaches when nothing inside takes
    /// focus, and what opening focuses when the host names no field.
    focus: FocusHandle,
    initial_focus: Option<FocusHandle>,
    /// The element that had focus when the surface opened.
    return_focus: Option<FocusHandle>,
    dismiss: Option<DismissHandler>,
    hover: HoverIntent,
    /// Bumped by every timer the state schedules or cancels, so a timer that
    /// fires after its reason is gone runs nothing.
    generation: u64,
}

impl PopoverState {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            open: false,
            anchor: None,
            side: PopoverSide::Below,
            frame: None,
            focus: cx.focus_handle(),
            initial_focus: None,
            return_focus: None,
            dismiss: None,
            hover: HoverIntent::disabled(),
            generation: 0,
        }
    }

    /// Opens the surface while the pointer rests on its anchor, and closes it
    /// once the pointer has left the anchor and the surface for [`HOVER_GRACE`].
    /// Off by default: a popover opens where its host asks it to.
    pub fn hover_intent(mut self, enabled: bool) -> Self {
        self.hover.enabled = enabled;
        self
    }

    /// Names the handle focus moves to when the surface opens (a field in its
    /// body); without one, opening focuses the surface itself.
    pub fn set_initial_focus(&mut self, handle: Option<FocusHandle>) {
        self.initial_focus = handle;
    }

    /// Sets the handler the surface calls after it has closed, for the host to
    /// drop the draft it was showing. It runs after focus has been given back.
    pub fn on_dismiss(&mut self, handler: impl Fn(&mut Window, &mut App) + 'static) {
        self.dismiss = Some(Rc::new(handler));
    }

    /// Which side of the anchor the surface prefers; [`PopoverSide::Below`] by
    /// default. It still flips when the preferred side has no room.
    pub fn set_side(&mut self, side: PopoverSide, cx: &mut Context<Self>) {
        if self.side != side {
            self.side = side;
            cx.notify();
        }
    }

    pub fn is_open(&self) -> bool {
        self.open
    }

    /// The window-coordinate rectangle the surface points at, when the host set
    /// one.
    pub fn anchor(&self) -> Option<Bounds<Pixels>> {
        self.anchor
    }

    /// The side the surface prefers to open on.
    pub fn side(&self) -> PopoverSide {
        self.side
    }

    /// The surface's own focus handle, so the host can give the surface focus or
    /// read whether it holds it.
    pub fn focus_handle(&self) -> FocusHandle {
        self.focus.clone()
    }

    /// Points the surface at another rectangle, in window coordinates: an
    /// element's bounds, or the range a mark sits in as its host laid it out.
    ///
    /// A host calls it from the prepaint of the element it points at, so the
    /// surface follows its anchor as the layout moves. It schedules no redraw of
    /// its own.
    pub fn set_anchor(&mut self, anchor: Option<Bounds<Pixels>>) {
        self.anchor = anchor;
    }

    /// Opens the surface at the anchor the host last set, or where the host
    /// renders it when it set none.
    pub fn open(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.open {
            return;
        }

        self.open = true;
        self.hover.closing = false;
        self.generation = self.generation.wrapping_add(1);
        self.return_focus = window.focused(cx);

        let target = self
            .initial_focus
            .clone()
            .unwrap_or_else(|| self.focus.clone());
        target.focus(window, cx);
        cx.notify();
    }

    /// Points the surface at `anchor` and opens it there.
    pub fn open_at(&mut self, anchor: Bounds<Pixels>, window: &mut Window, cx: &mut Context<Self>) {
        self.anchor = Some(anchor);
        self.open(window, cx);
    }

    /// Opens `anchor`'s surface, or closes it when it is already open.
    pub fn toggle_at(
        &mut self,
        anchor: Bounds<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.open {
            self.close(window, cx);
        } else {
            self.open_at(anchor, window, cx);
        }
    }

    /// Closes the surface, gives focus back to the element that had it, and
    /// calls the host's dismiss handler.
    ///
    /// Focus goes back while it is still inside the surface: on the surface
    /// itself or on the handle given to [`PopoverState::set_initial_focus`]. A
    /// body that focused something of its own hands focus back from the dismiss
    /// handler, which runs after this.
    pub fn close(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.open {
            return;
        }

        self.open = false;
        self.frame = None;
        self.hover.over_popover = false;
        self.hover.closing = false;
        self.generation = self.generation.wrapping_add(1);

        // A hover-intent surface is usually dismissed while the pointer still
        // rests on the anchor that opened it. It waits for the pointer to leave
        // and come back rather than opening again on the spot.
        self.hover.over_anchor = self.hover.enabled;

        self.restore_focus(window, cx);

        if let Some(dismiss) = self.dismiss.clone() {
            dismiss(window, cx);
        }

        cx.notify();
    }

    /// Reports whether the pointer rests on the anchor, which a hover-intent
    /// surface answers after [`HOVER_DELAY`].
    ///
    /// A host wires it to the anchor's own hover: `true` when the pointer enters
    /// the element or text range the surface points at, `false` when it leaves.
    pub fn hover_anchor(&mut self, over: bool, window: &mut Window, cx: &mut Context<Self>) {
        if !self.hover.enabled || self.hover.over_anchor == over {
            return;
        }

        if self.open {
            self.track_pointer(over, self.hover.over_popover, window, cx);
            return;
        }

        self.hover.over_anchor = over;

        if over {
            self.schedule(HOVER_DELAY, TimedAction::Open, window, cx);
        } else {
            self.cancel_timer();
        }
    }

    /// Tells the surface where the pointer is, so it holds itself open while the
    /// pointer is on it and counts the grace once the pointer has left both it
    /// and the anchor.
    ///
    /// The surface calls this from its own pointer handlers; a host needs it
    /// only to drive a popover it draws itself.
    pub fn pointer_moved(
        &mut self,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.hover.enabled || !self.open {
            return;
        }

        let over_anchor = self.anchor.is_some_and(|anchor| anchor.contains(&position));
        let over_popover = self.frame.is_some_and(|frame| frame.contains(&position));

        self.track_pointer(over_anchor, over_popover, window, cx);
    }

    /// The surface's laid-out bounds, once it has painted.
    pub(crate) fn frame(&self) -> Option<Bounds<Pixels>> {
        self.frame
    }

    /// Records the surface's laid-out bounds while it paints, asking for another
    /// frame when its size changed so the placement can flip with it.
    pub(crate) fn record_frame(
        &mut self,
        bounds: Bounds<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.frame.map(|frame| frame.size) == Some(bounds.size) {
            return;
        }

        self.frame = Some(bounds);

        // The placement read the size before this prepaint, so a changed size
        // needs another frame. Notifying is a no-op while drawing, hence the
        // defer; an unchanged size must not ask for one, or the window redraws
        // forever.
        let entity_id = cx.entity_id();
        window.defer(cx, move |_window, cx| cx.notify(entity_id));
    }

    /// Keeps the hover flags in step with where the pointer is, holding the
    /// surface open while it is on either region and counting the grace once it
    /// is on neither.
    fn track_pointer(
        &mut self,
        over_anchor: bool,
        over_popover: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.hover.over_anchor = over_anchor;
        self.hover.over_popover = over_popover;

        if over_anchor || over_popover {
            self.hover.closing = false;
            self.cancel_timer();
        } else if self.open && !self.hover.closing {
            self.hover.closing = true;
            self.schedule(HOVER_GRACE, TimedAction::Close, window, cx);
        }
    }

    fn restore_focus(&mut self, window: &mut Window, cx: &mut App) {
        let Some(previous) = self.return_focus.take() else {
            return;
        };

        // Focus is ours while it sits on the surface itself, on the handle the
        // host opens it for, or on anything the body focused of its own (a
        // field that took the keyboard from the initial one).
        let inside = self.focus.contains_focused(window, cx);
        let ours = inside
            || window.focused(cx).is_some_and(|focused| {
                focused == self.focus || self.initial_focus.as_ref() == Some(&focused)
            });

        if ours {
            previous.focus(window, cx);
        }
    }

    /// Runs `action` after `after`, unless the state scheduled or cancelled
    /// something else in the meantime.
    fn schedule(
        &mut self,
        after: Duration,
        action: TimedAction,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.generation = self.generation.wrapping_add(1);
        let generation = self.generation;

        let timer = cx.spawn_in(window, async move |this, cx| {
            cx.background_executor().timer(after).await;

            let fired = this.update_in(cx, |this, window, cx| {
                if this.generation == generation {
                    this.run_timed(action, window, cx);
                }
            });

            if let Err(error) = fired {
                log::debug!("the popover closed before its hover timer fired: {error}");
            }
        });

        timer.detach();
    }

    /// Forgets the timer in flight, if any.
    fn cancel_timer(&mut self) {
        self.generation = self.generation.wrapping_add(1);
    }

    fn run_timed(&mut self, action: TimedAction, window: &mut Window, cx: &mut Context<Self>) {
        match action {
            TimedAction::Open if !self.open && self.hover.over_anchor => self.open(window, cx),
            TimedAction::Close
                if self.open && !self.hover.over_anchor && !self.hover.over_popover =>
            {
                self.close(window, cx)
            }
            TimedAction::Open | TimedAction::Close => {}
        }
    }
}

/// The surface a popover draws while its state is open: a title, the body the
/// host passes and a footer row of buttons.
///
/// The builder takes the id the surface and its parts are found by, and the
/// state it reads and closes through. Build it in the host's render, every
/// frame, where the surface should sit when it has no anchor.
#[derive(IntoElement)]
pub struct Popover {
    id: ElementId,
    state: Entity<PopoverState>,
    title: Option<SharedString>,
    icon: Option<IconSource>,
    aria_label: Option<SharedString>,
    body: Option<AnyElement>,
    footer: Vec<AnyElement>,
    width: AbsoluteLength,
}

impl Popover {
    pub fn new(id: impl Into<ElementId>, state: Entity<PopoverState>) -> Self {
        Self {
            id: id.into(),
            state,
            title: None,
            icon: None,
            aria_label: None,
            body: None,
            footer: Vec::new(),
            width: PopoverMetrics::WIDTH.into(),
        }
    }

    /// The heading of the surface; it also names the dialog for assistive
    /// technology unless [`Popover::aria_label`] replaces that name.
    pub fn title(mut self, title: impl Into<SharedString>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// A leading icon beside the title.
    pub fn icon(mut self, icon: impl Into<IconSource>) -> Self {
        self.icon = Some(icon.into());
        self
    }

    /// Names the dialog for assistive technology, for a surface whose title is
    /// not what the surface is about.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.aria_label = Some(label.into());
        self
    }

    /// The content under the title: the fields and controls of the form.
    pub fn body(mut self, body: impl IntoElement) -> Self {
        self.body = Some(body.into_any_element());
        self
    }

    /// The buttons along the right of the footer; the primary action goes last.
    /// A footer that mixes kinds of control passes each one as an `AnyElement`.
    pub fn footer<B: IntoElement>(mut self, buttons: impl IntoIterator<Item = B>) -> Self {
        self.footer = buttons
            .into_iter()
            .map(IntoElement::into_any_element)
            .collect();
        self
    }

    /// Replaces the default width ([`PopoverMetrics::WIDTH`]).
    pub fn width(mut self, width: impl Into<AbsoluteLength>) -> Self {
        self.width = width.into();
        self
    }

    /// The frame itself, without the layer that places it.
    fn into_frame(self, cx: &mut App) -> Stateful<Div> {
        let focus = self.state.read(cx).focus_handle();

        let name = self
            .aria_label
            .clone()
            .or_else(|| self.title.clone())
            .unwrap_or_default();

        // A part is found by its element id and by the debug selector of the
        // same name, as every id-bearing component records it.
        let part = |suffix: &str| {
            (
                SharedString::from(format!("{}-{suffix}", self.id)),
                format!("{}-{suffix}", self.id),
            )
        };
        let (title_id, title_selector) = part("title");
        let (body_id, body_selector) = part("body");
        let (footer_id, footer_selector) = part("footer");

        let icon_color = ChromeColors::strong(cx.theme());

        let mut frame = surface(SurfaceRole::Overlay, cx)
            .id(self.id.clone())
            .debug_selector(id_selector(&self.id))
            .role(Role::Dialog)
            .aria_label(name)
            .occlude()
            .flex()
            .flex_col()
            .w(self.width)
            .p(PopoverMetrics::PADDING)
            .gap(PopoverMetrics::GAP)
            .text_color(crate::colors(cx).foreground)
            .track_focus(&focus)
            .capture_key_down({
                let state = self.state.downgrade();
                move |event: &KeyDownEvent, window, cx| {
                    if event.keystroke.key != "escape" || event.keystroke.modifiers.modified() {
                        return;
                    }

                    cx.stop_propagation();

                    if let Some(state) = state.upgrade() {
                        state.update(cx, |state, cx| state.close(window, cx));
                    }
                }
            })
            .on_mouse_move({
                let state = self.state.clone();
                move |event, window, cx| {
                    let position = event.position;
                    state.update(cx, |state, cx| state.pointer_moved(position, window, cx));
                }
            })
            .child({
                // Records where the surface landed, so the next frame places it
                // with its real size and the hover intent knows where it is.
                let state = self.state.clone();
                canvas(
                    move |bounds, window, cx| {
                        state.update(cx, |state, cx| state.record_frame(bounds, window, cx));
                    },
                    |_, _, _, _| {},
                )
                .absolute()
                .size_full()
            });

        if let Some(title) = self.title {
            frame = frame.child(
                div()
                    .id(title_id)
                    .debug_selector(move || title_selector)
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .when_some(self.icon, |row, icon| {
                        row.child(
                            Icon::new(icon)
                                .size(PopoverMetrics::TITLE_ICON)
                                .color(icon_color),
                        )
                    })
                    .child(Text::heading(title).font_size(PopoverMetrics::TITLE_FONT)),
            );
        }

        if let Some(body) = self.body {
            frame = frame.child(
                div()
                    .id(body_id)
                    .debug_selector(move || body_selector)
                    .flex()
                    .flex_col()
                    .gap(PopoverMetrics::GAP)
                    .child(body),
            );
        }

        if !self.footer.is_empty() {
            frame = frame.child(
                div()
                    .id(footer_id)
                    .debug_selector(move || footer_selector)
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .justify_end()
                    .gap(PopoverMetrics::FOOTER_GAP)
                    .children(self.footer),
            );
        }

        // A product's keymap binds Escape to one of its own actions while the
        // field in the body has focus (a field's cancel) and while the shell
        // does (a shell-wide cancel). A binding is dispatched before any key
        // listener runs, so `capture_key_down` above never sees that Escape: the
        // frame answers the product's cancel action instead, from where it sits
        // on the dispatch path — inside every shell that would answer it, and
        // outside anything the body opened of its own, which gets the cancel
        // first and keeps the surface open.
        if let Some(keys) = cx.try_global::<CommandSet>() {
            let attach = keys.attach;
            let state = self.state.downgrade();
            let handler: KeyCommandHandler = Rc::new(move |action_id, window, cx| {
                let cancels = cx
                    .try_global::<CommandSet>()
                    .and_then(|keys| keys.command(action_id))
                    == Some(Command::Cancel);

                if !cancels {
                    return false;
                }

                let Some(state) = state.upgrade() else {
                    return false;
                };

                state.update(cx, |state, cx| state.close(window, cx));
                true
            });

            frame = attach(frame, handler);
        }

        frame
    }
}

impl RenderOnce for Popover {
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        let (open, anchor, side, measured) = {
            let state = self.state.read(cx);
            (state.is_open(), state.anchor(), state.side(), state.frame())
        };

        if !open {
            return div().into_any_element();
        }

        let state = self.state.clone();
        let id = self.id.clone();
        let viewport = window.viewport_size();
        let frame = self.into_frame(cx);

        let away_id = SharedString::from(format!("{id}-away"));
        let away_selector = format!("{id}-away");

        // The click-away layer sits under the surface: a click on anything drawn
        // above it, such as a dropdown list opened from the body, is a click
        // inside and never reaches this layer. It lets the rest of the window
        // react to the click as it did before, as a dismiss-on-outside does.
        // Closing is the layer's only activation, so it runs on the click; the
        // right button dismisses on the press, as a menu's blanket does.
        let away = anchored()
            .position(point(px(0.0), px(0.0)))
            .anchor(Anchor::TopLeft)
            .child(
                div()
                    .id(away_id)
                    .debug_selector(move || away_selector)
                    .w(viewport.width)
                    .h(viewport.height)
                    .on_mouse_move({
                        let state = state.clone();
                        move |event, window, cx| {
                            let position = event.position;
                            state.update(cx, |state, cx| state.pointer_moved(position, window, cx));
                        }
                    })
                    .on_click({
                        let state = state.clone();
                        move |_event, window, cx| {
                            state.update(cx, |state, cx| state.close(window, cx));
                        }
                    })
                    .on_mouse_down(MouseButton::Right, move |_event, window, cx| {
                        state.update(cx, |state, cx| state.close(window, cx));
                    }),
            );

        let size = measured.map_or(
            Size {
                width: px(0.0),
                height: px(0.0),
            },
            |frame| frame.size,
        );

        let mut placed = anchored()
            .anchor(Anchor::TopLeft)
            .snap_to_window_with_margin(PopoverMetrics::WINDOW_MARGIN);

        if let Some(anchor) = anchor {
            let placement = place_popover(
                anchor,
                size,
                viewport,
                PopoverMetrics::ANCHOR_GAP,
                PopoverMetrics::WINDOW_MARGIN,
                side,
            );

            placed = placed.position(placement.origin);
        }

        deferred(div().child(away).child(placed.child(frame)))
            .with_priority(1)
            .into_any_element()
    }
}

#[cfg(test)]
mod tests;
