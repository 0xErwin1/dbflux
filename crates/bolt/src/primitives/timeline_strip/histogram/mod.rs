//! `TimelineHistogram` — per-bucket counts drawn as a bar chart on one time
//! axis.
//!
//! The static half of the timeline a document shows above its table: one bar
//! per time bucket, sized by its share of the busiest bucket, with a
//! highlighted series stacked over the rest of the bucket and an optional
//! swatch legend. It draws no interaction: a document that lets the reader
//! drag across the bars to zoom owns that gesture itself.
//!
//! ```ignore
//! TimelineHistogram::new("events")
//!     .aria_label("Events per hour")
//!     .buckets(buckets.iter().map(|bucket| TimelineBucket::new(bucket.total, bucket.failed)))
//!     .legend([
//!         TimelineLegend::new("calls", BadgeTone::Accent),
//!         TimelineLegend::new("refused", BadgeTone::Danger),
//!     ])
//! ```

use gpui::prelude::*;
use gpui::{App, Div, ElementId, Pixels, Role, SharedString, Stateful, Window, div, px};
use gpui_component::ActiveTheme;

use super::BadgeTone;
use crate::selector::id_selector;
use crate::tokens::{Borders, DocumentMetrics};

/// One bucket of a [`TimelineHistogram`]: how many events fall in it, and how
/// many of those the highlighted series covers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TimelineBucket {
    total: i64,
    highlighted: i64,
}

impl TimelineBucket {
    /// A bucket of `total` events, `highlighted` of which are drawn over the
    /// rest. A count below zero is no events, and the highlighted part is cut
    /// at the bucket's own total so it can never grow past the bar.
    pub fn new(total: i64, highlighted: i64) -> Self {
        let total = total.max(0);

        Self {
            total,
            highlighted: highlighted.clamp(0, total),
        }
    }

    /// Every event in the bucket.
    pub fn total(&self) -> i64 {
        self.total
    }

    /// The events of the bucket drawn in the highlight tone.
    pub fn highlighted(&self) -> i64 {
        self.highlighted
    }
}

/// A legend entry of a [`TimelineHistogram`]: a swatch in `tone` and the label
/// the caller writes for it.
#[derive(Clone, Debug, PartialEq)]
pub struct TimelineLegend {
    label: SharedString,
    tone: BadgeTone,
}

impl TimelineLegend {
    /// An entry named `label` with a swatch in `tone`.
    pub fn new(label: impl Into<SharedString>, tone: BadgeTone) -> Self {
        Self {
            label: label.into(),
            tone,
        }
    }
}

/// One bar per bucket, `DocumentMetrics::TIMELINE_BAR_HEIGHT` tall and
/// `DocumentMetrics::TIMELINE_BAR_GAP` apart, with the highlighted part of
/// each bucket stacked over the rest and an optional swatch legend to the
/// right.
///
/// The bars fill the width the parent gives them, `TIMELINE_PADDING_Y` above
/// and below, and the legend sits `TIMELINE_LEGEND_GAP` to their right: its
/// entries are `TIMELINE_LEGEND_ROW_GAP` apart, each an `TIMELINE_SWATCH`
/// square and its label in `TIMELINE_LEGEND_FONT`, the legend at most
/// `TIMELINE_LEGEND_MAX_WIDTH` wide with a label too long for it cut with an
/// ellipsis. A bucket is sized by its share of the busiest bucket, and a bucket
/// with any event draws at least a hairline so it still shows: the bars are
/// `TIMELINE_BAR_GAP` apart until the strip holds so many buckets that the gaps
/// no longer fit, and then the gaps shrink to nothing before any populated bar
/// does.
///
/// The histogram paints no ground and no border: it is the strip inside a
/// region the caller frames. It is a group under its id, named by
/// `.aria_label(..)` when it has one; its bars are the elements
/// `<id>-bar-<n>`, `<id>-bar-<n>-rest` and `<id>-bar-<n>-highlight`, the gaps
/// between them `<id>-gap-<n>`, all numbered from 0 in the order the buckets
/// were given, and its legend entries the
/// elements `<id>-legend-<n>` and `<id>-swatch-<n>` inside the element
/// `<id>-legend`.
#[derive(IntoElement)]
pub struct TimelineHistogram {
    id: SharedString,
    buckets: Vec<TimelineBucket>,
    primary: BadgeTone,
    highlight: BadgeTone,
    legend: Vec<TimelineLegend>,
    aria_label: Option<SharedString>,
}

impl TimelineHistogram {
    /// An empty histogram under `id`. It grows one bar per bucket.
    pub fn new(id: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            buckets: Vec::new(),
            primary: BadgeTone::Accent,
            highlight: BadgeTone::Danger,
            legend: Vec::new(),
            aria_label: None,
        }
    }

    pub fn bucket(mut self, bucket: TimelineBucket) -> Self {
        self.buckets.push(bucket);
        self
    }

    /// The buckets in the order they are drawn, oldest first.
    pub fn buckets(mut self, buckets: impl IntoIterator<Item = TimelineBucket>) -> Self {
        self.buckets.extend(buckets);
        self
    }

    /// The tone of the whole count of a bucket; the accent by default.
    pub fn primary_tone(mut self, tone: BadgeTone) -> Self {
        self.primary = tone;
        self
    }

    /// The tone of the highlighted part stacked over it; the danger tone by
    /// default.
    pub fn highlight_tone(mut self, tone: BadgeTone) -> Self {
        self.highlight = tone;
        self
    }

    /// The legend entries, drawn in the order given. Without them the
    /// histogram is the bars alone.
    pub fn legend(mut self, entries: impl IntoIterator<Item = TimelineLegend>) -> Self {
        self.legend.extend(entries);
        self
    }

    /// The name the histogram announces as a whole.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.aria_label = Some(label.into());
        self
    }
}

impl RenderOnce for TimelineHistogram {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme().clone();
        let id = ElementId::Name(self.id.clone());
        let peak = peak(&self.buckets);
        let rest_color = self.primary.text_color(&theme);
        let highlight_color = self.highlight.text_color(&theme);

        // A bucket with events keeps a hairline whatever else is on the strip,
        // and the gaps between the bars are the ones that give way when they no
        // longer fit, rather than the bars.
        let last = self.buckets.len().saturating_sub(1);
        let bars = self.buckets.iter().enumerate().flat_map(|(index, bucket)| {
            let (rest, highlighted) = bucket_heights(*bucket, peak);
            let bar = part(&self.id, format!("bar-{index}"), div())
                .flex_1()
                .min_w(if bucket.total() > 0 {
                    Borders::THIN
                } else {
                    px(0.0)
                })
                .flex()
                .flex_col()
                .justify_end()
                .h(DocumentMetrics::TIMELINE_BAR_HEIGHT)
                .child(
                    part(&self.id, format!("bar-{index}-highlight"), div())
                        .h(highlighted)
                        .bg(highlight_color),
                )
                .child(
                    part(&self.id, format!("bar-{index}-rest"), div())
                        .h(rest)
                        .bg(rest_color),
                );
            let gap = (index < last).then(|| {
                part(&self.id, format!("gap-{index}"), div())
                    .w(DocumentMetrics::TIMELINE_BAR_GAP)
                    .min_w_0()
                    .flex_shrink_1()
            });

            std::iter::once(bar.into_any_element()).chain(gap.map(IntoElement::into_any_element))
        });

        let legend = (!self.legend.is_empty()).then(|| {
            let column = ElementId::Name(SharedString::from(format!("{}-legend", self.id)));

            div()
                .debug_selector(id_selector(&column))
                .id(column)
                .flex()
                .flex_col()
                .flex_shrink_0()
                .max_w(DocumentMetrics::TIMELINE_LEGEND_MAX_WIDTH)
                .gap(DocumentMetrics::TIMELINE_LEGEND_ROW_GAP)
                .text_size(DocumentMetrics::TIMELINE_LEGEND_FONT)
                .text_color(theme.muted_foreground)
                .children(self.legend.iter().enumerate().map(|(index, entry)| {
                    part(&self.id, format!("legend-{index}"), div())
                        .flex()
                        .items_center()
                        .min_w_0()
                        .gap(DocumentMetrics::GAP)
                        .child(
                            part(
                                &self.id,
                                format!("swatch-{index}"),
                                div().size(DocumentMetrics::TIMELINE_SWATCH),
                            )
                            .flex_none()
                            .bg(entry.tone.text_color(&theme)),
                        )
                        .child(div().min_w_0().truncate().child(entry.label.clone()))
                }))
        });

        div()
            .debug_selector(id_selector(&id))
            .id(id)
            .role(Role::Group)
            .when_some(self.aria_label, |histogram, label| {
                histogram.aria_label(label)
            })
            .flex()
            .items_end()
            .gap(DocumentMetrics::TIMELINE_LEGEND_GAP)
            .py(DocumentMetrics::TIMELINE_PADDING_Y)
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .flex()
                    .items_end()
                    .h(DocumentMetrics::TIMELINE_BAR_HEIGHT)
                    .children(bars),
            )
            .children(legend)
    }
}

/// `element` under the id `<histogram>-<suffix>`, which is also its debug
/// selector so a product's test finds it by name.
fn part(histogram: &SharedString, suffix: impl AsRef<str>, element: Div) -> Stateful<Div> {
    let id = ElementId::Name(SharedString::from(format!(
        "{histogram}-{}",
        suffix.as_ref()
    )));

    element.debug_selector(id_selector(&id)).id(id)
}

/// The tallest bucket's total, and at least one, so every bucket is sized by
/// its share of the busiest one and a histogram with no events is flat.
pub(super) fn peak(buckets: &[TimelineBucket]) -> i64 {
    buckets
        .iter()
        .map(|bucket| bucket.total)
        .max()
        .unwrap_or(0)
        .max(1)
}

/// The heights of the two segments of `bucket` when the busiest bucket holds
/// `peak` events: the rest of the bucket below and the highlighted part over
/// it. A count that rounds to nothing but has events still draws a hairline,
/// so a bucket is never invisible.
pub(super) fn bucket_heights(bucket: TimelineBucket, peak: i64) -> (Pixels, Pixels) {
    if bucket.total <= 0 {
        return (px(0.0), px(0.0));
    }

    // One height for the whole bucket, minimum included, split by the highlighted
    // share: the minimum belongs to the bucket, so a bucket with a highlighted
    // part is exactly as tall as the same bucket without one.
    let height = (DocumentMetrics::TIMELINE_BAR_HEIGHT
        * (bucket.total as f32 / peak.max(1) as f32))
        .max(Borders::THIN);
    let highlighted = height * (bucket.highlighted as f32 / bucket.total as f32);

    (height - highlighted, highlighted)
}

#[cfg(test)]
mod tests;
