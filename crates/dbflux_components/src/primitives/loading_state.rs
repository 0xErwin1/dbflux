//! `LoadingState<T>` — generic primitive for representing async fetch state.
//!
//! `LoadingBlock` renders the non-data phases (Idle, Loading, Failed).
//! The `Loaded(T)` variant is the caller's responsibility to render;
//! `LoadingBlock` deliberately has no knowledge of `T`.

use gpui::prelude::*;
use gpui::{App, IntoElement, SharedString, Window, div};
use gpui_component::ActiveTheme;

use crate::primitives::{BannerBlock, BannerVariant};
use crate::tokens::{Anim, FontSizes, Heights, Radii, Spacing};
use crate::typography::MonoCaption;

/// Generic async-fetch state.
///
/// Use `LoadingBlock` to render the Idle/Loading/Failed phases.
/// The caller renders `Loaded(T)` directly — `LoadingBlock` is not
/// involved at that point.
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

/// Spinner frame count — three-dot rotation cycles through 3 frames.
const SPINNER_FRAMES: usize = 3;

/// A minimal rotating-dots spinner.
///
/// Uses a `frame` field (0..SPINNER_FRAMES) driven by a timer at
/// `Anim::PULSE_INTERVAL_MS` intervals. Callers that need animation must
/// advance the frame via a stored `Task` on their entity and call `cx.notify()`.
///
/// The spinner renders as `⠋ ⠙ ⠹` (braille dots) cycling per frame.
#[derive(IntoElement)]
pub struct Spinner {
    frame: usize,
}

impl Spinner {
    /// Create a spinner at the given animation frame (0..SPINNER_FRAMES).
    pub fn new(frame: usize) -> Self {
        Self {
            frame: frame % SPINNER_FRAMES,
        }
    }

    /// Advance frame mod SPINNER_FRAMES — call this every `Anim::PULSE_INTERVAL_MS` ms.
    pub fn next_frame(frame: usize) -> usize {
        (frame + 1) % SPINNER_FRAMES
    }

    pub const INTERVAL_MS: u64 = Anim::PULSE_INTERVAL_MS;

    const GLYPHS: [&'static str; SPINNER_FRAMES] = ["⠋", "⠙", "⠹"];
}

impl RenderOnce for Spinner {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let glyph = Self::GLYPHS[self.frame];

        div()
            .flex()
            .items_center()
            .justify_center()
            .w(Heights::ICON_SM)
            .h(Heights::ICON_SM)
            .rounded(Radii::FULL)
            .text_size(FontSizes::SM)
            .text_color(theme.muted_foreground)
            .child(glyph)
    }
}

// ---------------------------------------------------------------------------
// LoadingBlock
// ---------------------------------------------------------------------------

/// Renders the non-data phases of a `LoadingState<()>`.
///
/// - `Idle` → nothing (returns an empty div)
/// - `Loading` → spinner + optional muted label
/// - `Failed` → `BannerBlock::Danger` with the error message
///
/// `Loaded` is never rendered by this element; the caller short-circuits on
/// `LoadingState::Loaded` and renders its own content.
#[derive(IntoElement)]
pub struct LoadingBlock {
    phase: LoadingPhase,
}

enum LoadingPhase {
    Idle,
    Loading {
        label: Option<SharedString>,
        frame: usize,
    },
    Failed {
        message: SharedString,
    },
}

impl LoadingBlock {
    /// Create a block that renders nothing (Idle state).
    pub fn idle() -> Self {
        Self {
            phase: LoadingPhase::Idle,
        }
    }

    /// Create a loading block with an optional label.
    ///
    /// `frame` is the current spinner animation frame (advance at
    /// `Spinner::INTERVAL_MS` ms intervals).
    pub fn loading(label: impl Into<Option<SharedString>>, frame: usize) -> Self {
        Self {
            phase: LoadingPhase::Loading {
                label: label.into(),
                frame,
            },
        }
    }

    /// Create a failure block with an error message.
    pub fn failed(message: impl Into<SharedString>) -> Self {
        Self {
            phase: LoadingPhase::Failed {
                message: message.into(),
            },
        }
    }

    /// Convenience: build a `LoadingBlock` directly from a `LoadingState<T>`.
    ///
    /// Returns `None` when the state is `Loaded` — the caller must render.
    pub fn from_state<T>(
        state: &LoadingState<T>,
        label: impl Into<Option<SharedString>>,
        frame: usize,
    ) -> Option<Self> {
        match state {
            LoadingState::Idle => Some(Self::idle()),
            LoadingState::Loading => Some(Self::loading(label, frame)),
            LoadingState::Failed { message } => Some(Self::failed(message.clone())),
            LoadingState::Loaded(_) => None,
        }
    }
}

impl RenderOnce for LoadingBlock {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();

        let inner: gpui::AnyElement = match self.phase {
            LoadingPhase::Idle => div().into_any_element(),

            LoadingPhase::Loading { label, frame } => div()
                .flex()
                .items_center()
                .gap(Spacing::XS)
                .py(Spacing::XS)
                .px(Spacing::SM)
                .child(Spinner::new(frame))
                .when_some(label, |d, text| {
                    d.child(MonoCaption::new(text).color(theme.muted_foreground))
                })
                .into_any_element(),

            LoadingPhase::Failed { message } => div()
                .p(Spacing::SM)
                .child(BannerBlock::new(BannerVariant::Danger, message))
                .into_any_element(),
        };

        div().child(inner)
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
        use super::Spinner;

        assert_eq!(Spinner::next_frame(0), 1);
        assert_eq!(Spinner::next_frame(1), 2);
        assert_eq!(Spinner::next_frame(2), 0); // wraps
    }

    #[test]
    fn loading_block_from_state_returns_none_for_loaded() {
        use super::LoadingBlock;
        use gpui::SharedString;

        let loaded: LoadingState<i32> = LoadingState::Loaded(1);
        let block = LoadingBlock::from_state(&loaded, None::<SharedString>, 0);
        assert!(block.is_none());
    }

    #[test]
    fn loading_block_from_state_returns_some_for_non_loaded() {
        use super::LoadingBlock;
        use gpui::SharedString;

        let idle: LoadingState<i32> = LoadingState::Idle;
        assert!(LoadingBlock::from_state(&idle, None::<SharedString>, 0).is_some());

        let loading: LoadingState<i32> = LoadingState::Loading;
        assert!(LoadingBlock::from_state(&loading, None::<SharedString>, 0).is_some());

        let failed: LoadingState<i32> = LoadingState::Failed {
            message: "e".into(),
        };
        assert!(LoadingBlock::from_state(&failed, None::<SharedString>, 0).is_some());
    }
}
