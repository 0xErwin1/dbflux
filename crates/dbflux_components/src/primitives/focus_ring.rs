use gpui::prelude::*;
use gpui::{AnyElement, App, FocusHandle, Hsla, Pixels, Window, div, px};
use gpui_component::ActiveTheme;

use crate::primitives::{Chamfer, ChamferFillKind, ChamferRing};

/// Debug selector of a focus ring while it is shown, for tests that check
/// whether a control draws its ring (`VisualTestContext::debug_bounds`).
pub const FOCUS_RING_SELECTOR: &str = "focus-ring";

/// Debug selector of the focus marker of an item inside a composite control
/// (segment, chip, tab, list row) while it is shown.
pub const FOCUS_MARKER_SELECTOR: &str = "focus-marker";

/// Whether keyboard focus indication shows in `window`: true after a key
/// press, false after a pointer press or a touch.
///
/// This is the app's one focus-visible rule. The window tracks it and
/// repaints when it changes, so a ring drawn after a Tab stays while the
/// pointer moves, disappears on the next mouse press and comes back on the
/// next key press.
pub fn is_keyboard_modality(window: &Window) -> bool {
    window.keyboard_focus_visible()
}

/// Whether `focus_handle` should show its focus ring: it (or a descendant)
/// holds focus and focus is visible in `window`.
pub fn is_focus_visible(focus_handle: &FocusHandle, window: &Window) -> bool {
    focus_handle.is_focused(window) && is_keyboard_modality(window)
}

/// Whether a control focused by navigation state rather than by a focus
/// handle (form cursors, grid cells) should show its focus indication.
pub fn focus_visible(focused: bool, window: &Window) -> bool {
    focused && is_keyboard_modality(window)
}

/// Renders `child` only while focus is visible in the window.
///
/// Use it for focus indication that is not a `ChamferRing` (an item marker, a
/// bordered frame) inside render code that has no `Window`; the check runs
/// when the element renders.
#[derive(IntoElement)]
pub struct WhenFocusVisible {
    child: AnyElement,
}

impl WhenFocusVisible {
    pub fn new(child: impl IntoElement) -> Self {
        Self {
            child: child.into_any_element(),
        }
    }
}

impl RenderOnce for WhenFocusVisible {
    fn render(self, window: &mut Window, _cx: &mut App) -> impl IntoElement {
        if is_keyboard_modality(window) {
            self.child
        } else {
            gpui::Empty.into_any_element()
        }
    }
}

/// The focus marker of the keyboard-focused item inside a composite control
/// (segment, chip, tab): a 2 px `color` bar along the item's bottom edge,
/// inside it, shown only while focus is visible. The item must be
/// `.relative()`; composite containers draw no ring of their own.
pub fn focus_underline(color: Hsla) -> WhenFocusVisible {
    WhenFocusVisible::new(
        div()
            .absolute()
            .left_0()
            .right_0()
            .bottom_0()
            .h(crate::tokens::Borders::MEDIUM)
            .bg(color)
            .debug_selector(|| FOCUS_MARKER_SELECTOR.to_string()),
    )
}

/// Outline the focus ring follows.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FocusShape {
    /// Square outline, for surfaces that are never cut: grid cells, rows,
    /// code editors.
    Rect,
    /// Chamfer outline with the given cut, ring inside the bounds: outlined,
    /// ghost and data controls (inputs, dropdowns, secondary buttons).
    Chamfer(Pixels),
    /// Chamfer outline with the given cut, ring 2 px outside the bounds:
    /// filled controls (primary and danger buttons).
    FilledChamfer(Pixels),
}

impl FocusShape {
    fn cut(self) -> Pixels {
        match self {
            Self::Rect => px(0.0),
            Self::Chamfer(cut) | Self::FilledChamfer(cut) => cut,
        }
    }

    fn fill_kind(self) -> ChamferFillKind {
        match self {
            Self::FilledChamfer(_) => ChamferFillKind::Filled,
            Self::Rect | Self::Chamfer(_) => ChamferFillKind::Surface,
        }
    }

    /// The ring stroked for this shape: `Borders::FOCUS_RING` (1.5 px) of
    /// `color`, inset or outside depending on the fill kind.
    pub fn ring(self, color: Hsla) -> ChamferRing {
        ChamferRing::focus_for(color, self.fill_kind())
    }
}

/// The app's one keyboard-focus style: a tint ring, 1.5 px, that traces
/// `shape` around `child` when `focused` and focus is visible in the window.
///
/// Use it for controls whose focus is tracked by form navigation state rather
/// than by their own focus handle. Controls that own a focus handle
/// (`Button`, `Input`, `Dropdown`) draw this same ring themselves. `ring_color`
/// defaults to the theme tint. The ring is painted above `child` and does not
/// change its layout.
pub fn focus_ring(
    focused: bool,
    shape: FocusShape,
    ring_color: Option<Hsla>,
    child: impl IntoElement,
    cx: &App,
) -> gpui::Div {
    let color = ring_color.unwrap_or(cx.theme().ring);

    div().relative().child(child).when(focused, |frame| {
        frame.child(Chamfer::new(shape.cut()).ring(shape.ring(color)))
    })
}

#[cfg(test)]
mod tests {
    use gpui::{
        Context, FocusHandle, InteractiveElement as _, IntoElement, Modifiers, MouseButton,
        ParentElement as _, Render, Styled as _, TestAppContext, VisualTestContext, Window, div,
        px,
    };

    use super::{
        FOCUS_MARKER_SELECTOR, FOCUS_RING_SELECTOR, FocusShape, focus_ring, focus_underline,
        is_focus_visible, is_keyboard_modality,
    };
    use crate::tokens::{Borders, ChamferCut};

    struct ModalityHost {
        focus_handle: FocusHandle,
    }

    impl Render for ModalityHost {
        fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .track_focus(&self.focus_handle)
                .debug_selector(|| "modality-host".to_string())
                .size(px(200.0))
                .child(focus_ring(
                    true,
                    FocusShape::Chamfer(ChamferCut::CONTROL),
                    None,
                    div().size(px(80.0)),
                    cx,
                ))
                .child(
                    div()
                        .relative()
                        .size(px(40.0))
                        .child(focus_underline(gpui::red())),
                )
        }
    }

    fn open_host(cx: &mut TestAppContext) -> (gpui::Entity<ModalityHost>, &mut VisualTestContext) {
        cx.update(crate::theme::init);

        let (host, window) = cx.add_window_view(|_, cx| ModalityHost {
            focus_handle: cx.focus_handle(),
        });
        window.update(|window, cx| host.read(cx).focus_handle.clone().focus(window, cx));
        window.run_until_parked();

        (host, window)
    }

    fn focus_shown(host: &gpui::Entity<ModalityHost>, window: &mut VisualTestContext) -> bool {
        let modality = window.update(|window, _| is_keyboard_modality(window));
        let handle_visible =
            window.update(|window, cx| is_focus_visible(&host.read(cx).focus_handle, window));
        let ring = window.debug_bounds(FOCUS_RING_SELECTOR).is_some();
        let marker = window.debug_bounds(FOCUS_MARKER_SELECTOR).is_some();

        assert_eq!(modality, handle_visible);
        assert_eq!(modality, ring, "the ring follows the input modality");
        assert_eq!(
            modality, marker,
            "the item marker follows the input modality"
        );

        modality
    }

    fn press_mouse(window: &mut VisualTestContext) {
        let center = window
            .debug_bounds("modality-host")
            .expect("the host is laid out")
            .center();

        window.simulate_mouse_down(center, MouseButton::Left, Modifiers::default());
        window.simulate_mouse_up(center, MouseButton::Left, Modifiers::default());
        window.run_until_parked();
    }

    #[gpui::test]
    fn focus_shows_after_keys_and_hides_after_a_press(cx: &mut TestAppContext) {
        let (host, window) = open_host(cx);

        assert!(
            !focus_shown(&host, window),
            "a new window shows no focus until a key is pressed"
        );

        window.simulate_keystrokes("down");
        assert!(focus_shown(&host, window), "a key press shows focus");

        press_mouse(window);
        assert!(!focus_shown(&host, window), "a pointer press hides focus");

        window.simulate_keystrokes("tab");
        assert!(
            focus_shown(&host, window),
            "the next key press shows it again"
        );
    }

    #[gpui::test]
    fn moving_the_pointer_keeps_keyboard_focus_visible(cx: &mut TestAppContext) {
        let (host, window) = open_host(cx);
        let center = window
            .debug_bounds("modality-host")
            .expect("the host is laid out")
            .center();

        window.simulate_keystrokes("tab");
        assert!(focus_shown(&host, window));

        window.simulate_mouse_move(center, None, Modifiers::default());
        window.run_until_parked();
        assert!(
            focus_shown(&host, window),
            "moving the pointer leaves the ring on"
        );

        press_mouse(window);
        assert!(!focus_shown(&host, window), "only a press hides it");
    }

    #[test]
    fn outlined_shapes_keep_the_ring_inside() {
        for shape in [FocusShape::Rect, FocusShape::Chamfer(ChamferCut::CONTROL)] {
            let ring = shape.ring(gpui::red());
            assert_eq!(ring.thickness, Borders::FOCUS_RING, "{shape:?}");
            assert_eq!(ring.offset, -Borders::FOCUS_RING, "{shape:?}");
        }
    }

    #[test]
    fn filled_shapes_move_the_ring_two_pixels_outside() {
        let ring = FocusShape::FilledChamfer(ChamferCut::CONTROL).ring(gpui::red());

        assert_eq!(ring.thickness, Borders::FOCUS_RING);
        assert_eq!(ring.offset, Borders::MEDIUM);
    }

    #[test]
    fn rect_shape_has_no_cut() {
        assert_eq!(FocusShape::Rect.cut(), gpui::px(0.0));
        assert_eq!(
            FocusShape::Chamfer(ChamferCut::INPUT).cut(),
            ChamferCut::INPUT
        );
    }
}
