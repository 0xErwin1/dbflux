//! `LoadingState<T>` — generic primitive for representing async fetch state,
//! and `Spinner`, the bolt-shaped loading indicator.

use gpui::prelude::*;
use gpui::{
    App, Bounds, ContentMask, Hsla, IntoElement, PathBuilder, Pixels, SharedString, Window, canvas,
    div, point, px, size,
};

use gpui_component::ActiveTheme;

use crate::tokens::{Anim, ChromeColors, Feedback};

/// Generic async-fetch state. The caller renders every phase.
#[derive(Clone, Debug, PartialEq)]
pub enum LoadingState<T> {
    /// Fetch not yet started.
    Idle,
    /// Fetch in progress.
    Loading,
    /// Fetch resolved successfully.
    Loaded(T),
    /// Fetch failed with an error message.
    Failed { message: SharedString },
}

impl<T> LoadingState<T> {
    pub fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }

    pub fn is_loading(&self) -> bool {
        matches!(self, Self::Loading)
    }

    pub fn is_loaded(&self) -> bool {
        matches!(self, Self::Loaded(_))
    }

    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Failed { .. })
    }

    /// Map the loaded value without consuming self.
    pub fn loaded(&self) -> Option<&T> {
        match self {
            Self::Loaded(v) => Some(v),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Spinner
// ---------------------------------------------------------------------------

/// Number of animation frames in one strike of the bolt.
const SPINNER_FRAMES: usize = 8;

/// Outline of the brand bolt in its 12 x 14 view box
/// (`M7.5 0 1 8h4l-1 6 7-8.5H7L7.5 0Z`).
const BOLT_POINTS: [(f32, f32); 6] = [
    (7.5, 0.0),
    (1.0, 8.0),
    (5.0, 8.0),
    (4.0, 14.0),
    (11.0, 5.5),
    (7.0, 5.5),
];
const BOLT_VIEW_WIDTH: f32 = 12.0;
const BOLT_VIEW_HEIGHT: f32 = 14.0;

/// Bolt-shaped loading indicator.
///
/// The bolt sits as a faint tint outline and fills with the tint from top to
/// bottom, one step per frame, like a strike. Callers advance `frame` every
/// [`Spinner::INTERVAL_MS`] ms with [`Spinner::next_frame`] and call
/// `cx.notify()`. When the app asks for reduced motion the bolt is drawn
/// fully charged and does not move.
#[derive(IntoElement)]
pub struct Spinner {
    frame: usize,
}

impl Spinner {
    /// Create a spinner at the given animation frame.
    pub fn new(frame: usize) -> Self {
        Self {
            frame: frame % SPINNER_FRAMES,
        }
    }

    /// Advance frame mod the frame count — call this every `INTERVAL_MS` ms.
    pub fn next_frame(frame: usize) -> usize {
        (frame + 1) % SPINNER_FRAMES
    }

    pub const INTERVAL_MS: u64 = Anim::PULSE_INTERVAL_MS;

    /// Share of the bolt height that is charged at `frame`, from the top.
    fn charged_fraction(frame: usize) -> f32 {
        (frame % SPINNER_FRAMES + 1) as f32 / SPINNER_FRAMES as f32
    }
}

impl RenderOnce for Spinner {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let tint = ChromeColors::tint(cx.theme());
        let track = tint.opacity(Feedback::SPINNER_TRACK_ALPHA);
        let charged = if cx.reduce_motion() {
            1.0
        } else {
            Self::charged_fraction(self.frame)
        };

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .justify_center()
            .size(Feedback::SPINNER_BOX)
            .child(
                canvas(
                    |_, _, _| {},
                    move |bounds, _, window, _| {
                        paint_bolt(bounds, track, window);

                        let charged_bounds = Bounds::new(
                            bounds.origin,
                            size(bounds.size.width, bounds.size.height * charged),
                        );
                        window.with_content_mask(
                            Some(ContentMask {
                                bounds: charged_bounds,
                            }),
                            |window| paint_bolt(bounds, tint, window),
                        );
                    },
                )
                .w(Feedback::SPINNER_BOLT_WIDTH)
                .h(Feedback::SPINNER_BOLT_HEIGHT),
            )
    }
}

/// Fills the bolt scaled from its view box into `bounds`.
fn paint_bolt(bounds: Bounds<Pixels>, color: Hsla, window: &mut Window) {
    let scale_x = f32::from(bounds.size.width) / BOLT_VIEW_WIDTH;
    let scale_y = f32::from(bounds.size.height) / BOLT_VIEW_HEIGHT;
    let to_bounds = |(x, y): (f32, f32)| {
        point(
            bounds.origin.x + px(x * scale_x),
            bounds.origin.y + px(y * scale_y),
        )
    };

    let mut builder = PathBuilder::fill();
    let mut points = BOLT_POINTS.into_iter().map(to_bounds);

    if let Some(first) = points.next() {
        builder.move_to(first);
    }
    for next in points {
        builder.line_to(next);
    }
    builder.close();

    match builder.build() {
        Ok(path) => window.paint_path(path, color),
        Err(error) => log::warn!("Failed to build spinner bolt path: {error}"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::LoadingState;

    #[test]
    fn loading_state_transitions_are_exclusive() {
        let idle: LoadingState<i32> = LoadingState::Idle;
        assert!(idle.is_idle());
        assert!(!idle.is_loading());
        assert!(!idle.is_loaded());
        assert!(!idle.is_failed());

        let loading: LoadingState<i32> = LoadingState::Loading;
        assert!(loading.is_loading());
        assert!(!loading.is_idle());

        let loaded = LoadingState::Loaded(42i32);
        assert!(loaded.is_loaded());
        assert_eq!(loaded.loaded(), Some(&42i32));

        let failed: LoadingState<i32> = LoadingState::Failed {
            message: "oops".into(),
        };
        assert!(failed.is_failed());
        assert_eq!(failed.loaded(), None);
    }

    #[test]
    fn spinner_next_frame_wraps_around() {
        use super::{SPINNER_FRAMES, Spinner};

        assert_eq!(Spinner::next_frame(0), 1);
        assert_eq!(Spinner::next_frame(SPINNER_FRAMES - 2), SPINNER_FRAMES - 1);
        assert_eq!(Spinner::next_frame(SPINNER_FRAMES - 1), 0);
    }

    #[test]
    fn spinner_charges_the_bolt_from_top_to_full() {
        use super::{SPINNER_FRAMES, Spinner};

        let fractions: Vec<f32> = (0..SPINNER_FRAMES).map(Spinner::charged_fraction).collect();

        assert!(fractions.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(fractions[0] > 0.0);
        assert_eq!(fractions[SPINNER_FRAMES - 1], 1.0);
        assert_eq!(Spinner::new(SPINNER_FRAMES + 3).frame, 3);
    }
}
