//! `StatusIndicator` — a status diamond with an optional label and latency.

use std::time::Duration;

use gpui::prelude::*;
use gpui::{
    AbsoluteLength, App, Bounds, Hsla, PathBuilder, Pixels, Rems, SharedString, Window, canvas,
    div, point,
};
use gpui_component::ActiveTheme;

use crate::tokens::{ChromeColors, Feedback};

/// State shown by a [`StatusIndicator`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Success color.
    Connected,
    /// Tint color: work in progress, or unsaved changes on a document tab.
    Busy,
    Warning,
    /// Danger color.
    Error,
    /// Muted color.
    Idle,
}

impl Status {
    pub fn color(self, theme: &gpui_component::Theme) -> Hsla {
        match self {
            Self::Connected => theme.success,
            Self::Busy => ChromeColors::tint(theme),
            Self::Warning => theme.warning,
            Self::Error => theme.danger,
            Self::Idle => theme.muted_foreground,
        }
    }
}

/// Formats a latency the way the sidebar and status bar show it ("12 ms").
/// A round trip under one millisecond, typical of a local server, reads
/// "<1 ms" instead of a misleading "0 ms".
pub fn format_latency(latency: Duration) -> SharedString {
    match latency.as_millis() {
        0 => "<1 ms".into(),
        millis => format!("{millis} ms").into(),
    }
}

/// Stateless status indicator: a 7 px diamond in the state color, followed by
/// an optional label and latency in the same color. Text size is inherited
/// from the parent.
#[derive(IntoElement)]
pub struct StatusIndicator {
    status: Status,
    label: Option<SharedString>,
    latency: Option<Duration>,
    diamond_size: Rems,
}

impl StatusIndicator {
    pub fn new(status: Status) -> Self {
        Self {
            status,
            label: None,
            latency: None,
            diamond_size: Feedback::STATUS_DIAMOND,
        }
    }

    /// Show a text label next to the diamond.
    pub fn label(mut self, text: impl Into<SharedString>) -> Self {
        self.label = Some(text.into());
        self
    }

    /// Show a round-trip latency ("12 ms") after the label.
    pub fn latency(mut self, latency: Duration) -> Self {
        self.latency = Some(latency);
        self
    }

    /// Use the 6 px diamond, sized for document tabs.
    pub fn compact(mut self) -> Self {
        self.diamond_size = Feedback::STATUS_DIAMOND_COMPACT;
        self
    }
}

impl RenderOnce for StatusIndicator {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let color = self.status.color(cx.theme());
        let latency = self.latency.map(format_latency);

        let diamond = div().flex_shrink_0().size(self.diamond_size).child(
            canvas(
                |_, _, _| {},
                move |bounds, _, window, _| paint_diamond(bounds, color, window),
            )
            .size_full(),
        );

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(Feedback::STATUS_GAP)
            .text_color(color)
            .child(diamond)
            .when_some(self.label, |el, label| el.child(label))
            .when_some(latency, |el, latency| el.child(latency))
    }
}

/// A bare diamond of `size` in `color`, for markers that carry a color
/// outside the [`Status`] set (environment chips).
pub fn status_diamond(color: Hsla, size: impl Into<AbsoluteLength>) -> impl IntoElement {
    let size: AbsoluteLength = size.into();

    div().flex_shrink_0().size(size).child(
        canvas(
            |_, _, _| {},
            move |bounds, _, window, _| paint_diamond(bounds, color, window),
        )
        .size_full(),
    )
}

/// Fills a diamond inscribed in `bounds` (its corners touch the edge midpoints).
fn paint_diamond(bounds: Bounds<Pixels>, color: Hsla, window: &mut Window) {
    let center = bounds.center();

    let mut builder = PathBuilder::fill();
    builder.move_to(point(center.x, bounds.top()));
    builder.line_to(point(bounds.right(), center.y));
    builder.line_to(point(center.x, bounds.bottom()));
    builder.line_to(point(bounds.left(), center.y));
    builder.close();

    match builder.build() {
        Ok(path) => window.paint_path(path, color),
        Err(error) => log::warn!("Failed to build status diamond path: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn states_map_to_their_palette_colors() {
        let theme = gpui_component::Theme::default();

        assert_eq!(Status::Connected.color(&theme), theme.success);
        assert_eq!(Status::Busy.color(&theme), ChromeColors::tint(&theme));
        assert_eq!(Status::Warning.color(&theme), theme.warning);
        assert_eq!(Status::Error.color(&theme), theme.danger);
        assert_eq!(Status::Idle.color(&theme), theme.muted_foreground);
    }

    #[test]
    fn latency_is_shown_in_whole_milliseconds() {
        assert_eq!(format_latency(Duration::from_micros(12_400)), "12 ms");
        assert_eq!(format_latency(Duration::from_micros(300)), "<1 ms");
    }

    #[test]
    fn compact_uses_the_tab_diamond() {
        assert_eq!(
            StatusIndicator::new(Status::Busy).diamond_size,
            Feedback::STATUS_DIAMOND
        );
        assert_eq!(
            StatusIndicator::new(Status::Busy).compact().diamond_size,
            Feedback::STATUS_DIAMOND_COMPACT
        );
    }
}
