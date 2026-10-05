//! `ReadEstimateBar`: the strip above a paged table that says what the next
//! read costs before it runs: "Will read ≈ 96 MiB · 1 of 13 row groups", a
//! bar split by column, and a chip for each of the largest columns.

use dbflux_core::{EstimateScope, PartUnit, ReadEstimate, TableProfile};
use gpui::prelude::*;
use gpui::{
    App, FontWeight, Hsla, IntoElement, ParentElement, Pixels, SharedString, Styled, Window, div,
    px,
};
use gpui_component::ActiveTheme;

use crate::components::column_facts::format_bytes;
use crate::icons::AppIcon;
use crate::primitives::{Chamfer, Icon};
use crate::tokens::{ChamferCut, ChromeColors, Fields, FontSizes, Spacing};

/// Columns named by a chip; the rest are counted in "+N more".
const MAX_CHIPS: usize = 4;

const BAR_WIDTH: Pixels = px(220.0);

/// Height of the bar split by column, and side of a chip's swatch.
const SWATCH: Pixels = px(8.0); // guardrail-allow: estimate swatch and bar height

const STRIP_HEIGHT: Pixels = px(40.0);

/// The color a column takes in the bar and on its chip: the largest columns
/// get the four accent tones, every other column the muted one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SegmentTone {
    Tint,
    Info,
    Success,
    Warning,
    Muted,
}

impl SegmentTone {
    fn for_rank(rank: usize) -> Self {
        match rank {
            0 => Self::Tint,
            1 => Self::Info,
            2 => Self::Success,
            3 => Self::Warning,
            _ => Self::Muted,
        }
    }

    fn color(self, theme: &gpui_component::Theme) -> Hsla {
        match self {
            Self::Tint => ChromeColors::tint(theme),
            Self::Info => theme.info,
            Self::Success => theme.success,
            Self::Warning => theme.warning,
            Self::Muted => theme.muted_foreground,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct EstimateChip {
    name: SharedString,
    size: SharedString,
    tone: SegmentTone,
}

/// A read estimate formatted for display. Build it once when the estimate
/// changes, keep it, and render a clone: every string is formatted here, not
/// while rendering.
#[derive(Clone, Debug, PartialEq, IntoElement)]
pub struct ReadEstimateBar {
    lead: SharedString,
    total: SharedString,
    scope: SharedString,
    /// The share of the total of each column with a chip, largest first,
    /// then the share of every other column together.
    segments: Vec<(f32, SegmentTone)>,
    chips: Vec<EstimateChip>,
    more: Option<SharedString>,
}

impl ReadEstimateBar {
    /// Formats `estimate`, naming its columns from `profile`. A column index
    /// the profile does not hold is named by its position (`#7`).
    pub fn new(estimate: &ReadEstimate, profile: &TableProfile) -> Self {
        let mut columns = estimate.per_column.clone();
        columns.sort_by(|(left_index, left_bytes), (right_index, right_bytes)| {
            right_bytes
                .cmp(left_bytes)
                .then(left_index.cmp(right_index))
        });

        let segments = if estimate.total_bytes == 0 {
            Vec::new()
        } else {
            bar_segments(&columns, estimate.total_bytes)
        };

        let chips = columns
            .iter()
            .take(MAX_CHIPS)
            .enumerate()
            .map(|(rank, (index, bytes))| EstimateChip {
                name: column_name(profile, *index),
                size: format_bytes(*bytes).into(),
                tone: SegmentTone::for_rank(rank),
            })
            .collect();

        let hidden = columns.len().saturating_sub(MAX_CHIPS);

        Self {
            lead: dbflux_i18n::t!("components.read_estimate.lead").into(),
            total: format!("≈ {}", format_bytes(estimate.total_bytes)).into(),
            scope: scope_label(&estimate.scope).into(),
            segments,
            chips,
            more: (hidden > 0).then(|| more_label(hidden).into()),
        }
    }

    /// The whole sentence, as a screen reader or a test reads it.
    pub fn summary(&self) -> String {
        format!("{} {} · {}", self.lead, self.total, self.scope)
    }

    /// The names on the chips, largest column first.
    pub fn chip_names(&self) -> Vec<SharedString> {
        self.chips.iter().map(|chip| chip.name.clone()).collect()
    }

    /// The "+N more" label for the columns without a chip.
    pub fn more_label(&self) -> Option<&SharedString> {
        self.more.as_ref()
    }
}

impl RenderOnce for ReadEstimateBar {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let muted = theme.muted_foreground;
        let strong = ChromeColors::strong(theme);

        let bar = (!self.segments.is_empty()).then(|| {
            div()
                .flex()
                .flex_shrink_0()
                .gap(px(1.0))
                .w(BAR_WIDTH)
                .h(SWATCH)
                .bg(theme.secondary)
                .children(self.segments.iter().map(|(fraction, tone)| {
                    div()
                        .h_full()
                        .w(BAR_WIDTH * *fraction)
                        .bg(tone.color(theme))
                }))
        });

        let chips = div()
            .flex()
            .items_center()
            .gap(Spacing::MD)
            .min_w_0()
            .overflow_hidden()
            .font_family(crate::fonts::editor_family(cx))
            .text_size(FontSizes::LABEL)
            .children(self.chips.iter().map(|chip| {
                div()
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .gap(Spacing::XS)
                    .whitespace_nowrap()
                    .child(div().size(SWATCH).bg(chip.tone.color(theme)))
                    .child(chip.name.clone())
                    .child(div().text_color(theme.foreground).child(chip.size.clone()))
            }))
            .when_some(self.more.clone(), |chips, more| {
                chips.child(div().flex_shrink_0().whitespace_nowrap().child(more))
            });

        div()
            .id("read-estimate-bar")
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(Spacing::MD)
            .h(STRIP_HEIGHT)
            .px(Spacing::MD)
            .text_size(FontSizes::XS)
            .text_color(muted)
            .child(
                Chamfer::new(ChamferCut::CONTROL)
                    .fill(theme.background)
                    .border(theme.input),
            )
            .child(
                Icon::new(AppIcon::Zap)
                    .size(Fields::LEADING_ICON)
                    .color(theme.warning),
            )
            .child(
                div()
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .gap(Spacing::XS)
                    .whitespace_nowrap()
                    .child(self.lead.clone())
                    .child(
                        div()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(strong)
                            .child(self.total.clone()),
                    )
                    .child("·")
                    .child(div().text_color(strong).child(self.scope.clone())),
            )
            .children(bar)
            .child(chips)
    }
}

/// One segment per column with a chip, then one for every other column
/// together: a segment per column would let the gaps between hundreds of
/// columns outgrow the bar.
fn bar_segments(columns: &[(usize, u64)], total_bytes: u64) -> Vec<(f32, SegmentTone)> {
    let share = |bytes: u64| bytes as f32 / total_bytes as f32;

    let mut segments: Vec<(f32, SegmentTone)> = columns
        .iter()
        .take(MAX_CHIPS)
        .enumerate()
        .map(|(rank, (_, bytes))| (share(*bytes), SegmentTone::for_rank(rank)))
        .collect();

    if columns.len() > MAX_CHIPS {
        let rest: u64 = columns
            .iter()
            .skip(MAX_CHIPS)
            .map(|(_, bytes)| *bytes)
            .sum();

        segments.push((share(rest), SegmentTone::Muted));
    }

    segments
}

fn column_name(profile: &TableProfile, index: usize) -> SharedString {
    profile
        .columns
        .get(index)
        .map_or_else(|| format!("#{index}"), |column| column.name.to_string())
        .into()
}

/// "k of n row groups", in the unit the estimate counts.
fn scope_label(scope: &EstimateScope) -> String {
    match scope {
        EstimateScope::Parts {
            touched,
            total,
            unit: PartUnit::RowGroups,
        } => dbflux_i18n::t!(
            "components.read_estimate.row_groups",
            touched = touched,
            total = total
        ),
    }
}

fn more_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!("components.read_estimate.more.one", count = count)
    } else {
        dbflux_i18n::t!("components.read_estimate.more.many", count = count)
    }
}

#[cfg(test)]
mod tests {
    use dbflux_core::{
        ColumnProfile, EstimateScope, PartUnit, ProfileSource, ReadEstimate, TableProfile,
    };

    use super::ReadEstimateBar;

    fn profile(names: &[&str]) -> TableProfile {
        TableProfile {
            columns: names
                .iter()
                .map(|name| ColumnProfile {
                    name: (*name).into(),
                    type_name: "INT64".into(),
                    badges: Vec::new(),
                    codec: None,
                    compressed_bytes: None,
                    uncompressed_bytes: None,
                    null_count: None,
                    distinct_count: None,
                    range: None,
                })
                .collect(),
            row_count: None,
            total_compressed_bytes: None,
            total_uncompressed_bytes: None,
            source_label: ProfileSource::FileFooter,
        }
    }

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn estimate_bar_names_the_scope() {
        let table = profile(&["site_id", "ts", "user_id", "path", "country", "duration_ms"]);
        let estimate = ReadEstimate {
            total_bytes: 96 * MIB,
            per_column: vec![
                (0, MIB),
                (1, 12 * MIB),
                (2, 25 * MIB),
                (3, 41 * MIB),
                (4, 4 * MIB),
                (5, 13 * MIB),
            ],
            scope: EstimateScope::Parts {
                touched: 1,
                total: 13,
                unit: PartUnit::RowGroups,
            },
        };

        let bar = ReadEstimateBar::new(&estimate, &table);

        assert_eq!(bar.summary(), "Will read ≈ 96 MiB · 1 of 13 row groups");
        assert_eq!(
            bar.chip_names(),
            vec!["path", "user_id", "duration_ms", "ts"],
            "the largest columns get a chip, largest first"
        );
        assert_eq!(bar.more_label().map(|more| more.as_ref()), Some("+2 more"));
    }

    #[test]
    fn a_column_missing_from_the_profile_is_named_by_position() {
        let table = profile(&["only"]);
        let estimate = ReadEstimate {
            total_bytes: 3 * MIB,
            per_column: vec![(0, MIB), (7, 2 * MIB)],
            scope: EstimateScope::Parts {
                touched: 2,
                total: 2,
                unit: PartUnit::RowGroups,
            },
        };

        let bar = ReadEstimateBar::new(&estimate, &table);

        assert_eq!(bar.chip_names(), vec!["#7", "only"]);
        assert_eq!(bar.more_label(), None);
        assert_eq!(bar.summary(), "Will read ≈ 3.0 MiB · 2 of 2 row groups");
    }

    #[test]
    fn a_wide_estimate_keeps_its_bar_segments_inside_the_bar() {
        let names: Vec<String> = (0..300).map(|index| format!("c{index}")).collect();
        let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let table = profile(&name_refs);
        let estimate = ReadEstimate {
            total_bytes: 300 * MIB,
            per_column: (0..300).map(|index| (index, MIB)).collect(),
            scope: EstimateScope::Parts {
                touched: 1,
                total: 1,
                unit: PartUnit::RowGroups,
            },
        };

        let bar = ReadEstimateBar::new(&estimate, &table);

        assert_eq!(
            bar.segments.len(),
            super::MAX_CHIPS + 1,
            "one segment per chip and one for every other column"
        );

        let covered: f32 = bar.segments.iter().map(|(fraction, _)| fraction).sum();
        assert!((covered - 1.0).abs() < 1e-4, "{covered}");
    }

    #[test]
    fn estimate_keys_resolve_in_every_locale() {
        for key in [
            "components.read_estimate.lead",
            "components.read_estimate.row_groups",
            "components.read_estimate.more.one",
            "components.read_estimate.more.many",
        ] {
            let english = dbflux_i18n::t!(key, locale = "en");
            assert!(
                !english.is_empty() && !english.ends_with(key),
                "en misses {key}"
            );

            for locale in ["es", "ko", "pt_BR", "zh_Hans"] {
                // A key missing from a catalog falls back to English.
                let text = dbflux_i18n::t!(key, locale = locale);
                assert_ne!(text, english, "{locale} misses {key}");
            }
        }
    }
}
