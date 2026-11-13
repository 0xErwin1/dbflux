//! `TimelineStrip` — phase bars laid over one time axis.
//!
//! `TimelineHistogram` is its counting sibling: per-bucket bar heights rather
//! than phases, in `histogram`.

mod histogram;

pub use histogram::{TimelineBucket, TimelineHistogram, TimelineLegend};

use std::time::Duration;

use gpui::prelude::*;
use gpui::{App, ElementId, Role, SharedString, Window, div, relative};
use gpui_component::ActiveTheme;

use super::BadgeTone;
use crate::selector::id_selector;
use crate::tokens::TimelineMetrics;

/// One phase on a [`TimelineStrip`]: where it starts on the axis, how long
/// it lasts, and the tone it draws in.
pub struct TimelineBar {
    id: SharedString,
    label: SharedString,
    start: Duration,
    duration: Duration,
    tone: BadgeTone,
}

impl TimelineBar {
    /// A bar under `id`, named `label`, from `start` for `duration` on the
    /// strip's axis. It draws in the accent tone until [`TimelineBar::tone`]
    /// says otherwise.
    pub fn new(
        id: impl Into<SharedString>,
        label: impl Into<SharedString>,
        start: Duration,
        duration: Duration,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            start,
            duration,
            tone: BadgeTone::Accent,
        }
    }

    /// The bar's color, the solid color of a badge in `tone`.
    pub fn tone(mut self, tone: BadgeTone) -> Self {
        self.tone = tone;
        self
    }
}

/// A 10 px track on the inset fill holding [`TimelineBar`]s, each placed
/// by its start and sized by its duration over the strip's `span`, so bars
/// on separate strips with the same span share one axis.
///
/// The strip fills the width its parent gives it. A bar keeps a 2 px
/// minimum width so a phase too short for the axis still shows, and a bar
/// that runs past the span is cut at the strip's end. Each bar is a label
/// node under its own id, named by its label (`"DNS: 12 ms"`), which the
/// caller writes in its own words; the strip itself is a group under its id.
///
/// ```ignore
/// TimelineStrip::new("timeline-hop-1", total)
///     .bar(TimelineBar::new("timeline-hop-1-dns", dns_label, Duration::ZERO, dns).tone(BadgeTone::Info))
///     .bar(TimelineBar::new("timeline-hop-1-download", download_label, offset, download).tone(BadgeTone::Success))
/// ```
#[derive(IntoElement)]
pub struct TimelineStrip {
    id: SharedString,
    span: Duration,
    bars: Vec<TimelineBar>,
    aria_label: Option<SharedString>,
}

impl TimelineStrip {
    /// An empty strip under `id` whose axis runs from zero to `span`.
    pub fn new(id: impl Into<SharedString>, span: Duration) -> Self {
        Self {
            id: id.into(),
            span,
            bars: Vec::new(),
            aria_label: None,
        }
    }

    pub fn bar(mut self, bar: TimelineBar) -> Self {
        self.bars.push(bar);
        self
    }

    pub fn bars(mut self, bars: impl IntoIterator<Item = TimelineBar>) -> Self {
        self.bars.extend(bars);
        self
    }

    /// The name the strip announces as a whole.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.aria_label = Some(label.into());
        self
    }
}

impl RenderOnce for TimelineStrip {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme().clone();
        let id = ElementId::Name(self.id);
        let span = self.span;

        let bars = self.bars.into_iter().map(|bar| {
            let (start, width) = bar_extent(bar.start, bar.duration, span);
            let bar_id = ElementId::Name(bar.id);

            div()
                .debug_selector(id_selector(&bar_id))
                .id(bar_id)
                .role(Role::Label)
                .aria_label(bar.label)
                .absolute()
                .top_0()
                .bottom_0()
                .left(relative(start))
                .w(relative(width))
                .min_w(TimelineMetrics::MIN_BAR)
                .bg(bar.tone.text_color(&theme))
        });

        div()
            .debug_selector(id_selector(&id))
            .id(id)
            .role(Role::Group)
            .when_some(self.aria_label, |strip, label| strip.aria_label(label))
            .relative()
            .flex_1()
            .min_w_0()
            .h(TimelineMetrics::STRIP_HEIGHT)
            .overflow_hidden()
            .bg(theme.background)
            .children(bars)
    }
}

/// Where a bar from `start` for `duration` sits on an axis of `span`: its
/// left edge and its width, both as shares of the strip from 0 to 1. A bar
/// past the span starts at the end and has no width; one running past the
/// end is cut there. A zero span puts every bar at the start with no width,
/// so each draws at its minimum.
pub(crate) fn bar_extent(start: Duration, duration: Duration, span: Duration) -> (f32, f32) {
    if span.is_zero() {
        return (0.0, 0.0);
    }

    let span = span.as_secs_f64();
    let start_share = (start.as_secs_f64() / span).clamp(0.0, 1.0);
    let end_share = (start.saturating_add(duration).as_secs_f64() / span).clamp(start_share, 1.0);

    (start_share as f32, (end_share - start_share) as f32)
}

#[cfg(test)]
mod tests;
