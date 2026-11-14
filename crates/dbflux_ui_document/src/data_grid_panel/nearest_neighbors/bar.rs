//! The neighbor bar: it takes the filter row's place while similar rows are
//! shown and says what they are similar to, by which column and distance,
//! how many, and how to go back.

use super::{NearestNeighborSearch, distance_label};
use crate::data_grid_panel::DataGridPanel;
use dbflux_components::components::filter_bar::FilterField;
use dbflux_components::controls::{Button, Input};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, SegmentedControl, SegmentedItem, Text};
use dbflux_components::tokens::{ChromeColors, Fields, Spacing};
use dbflux_core::VectorDistance;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::Sizable;

/// Width of the vector-column picker in the bar.
const COLUMN_PICKER_WIDTH: Rems = dbflux_components::tokens::ui(180.0);

/// Segment id of a distance in the bar's distance switch.
fn distance_segment(distance: VectorDistance) -> &'static str {
    match distance {
        VectorDistance::L2 => "l2",
        VectorDistance::Cosine => "cosine",
        VectorDistance::InnerProduct => "inner-product",
    }
}

fn distance_for_segment(id: &str) -> Option<VectorDistance> {
    VectorDistance::ALL
        .into_iter()
        .find(|distance| distance_segment(*distance) == id)
}

impl DataGridPanel {
    pub(in crate::data_grid_panel) fn render_neighbor_bar(
        &self,
        search: &NearestNeighborSearch,
        theme: &gpui_component::theme::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let tint = ChromeColors::tint(theme);

        let anchor_chip = div()
            .id("nearest-neighbors-anchor")
            .flex_none()
            .px(Spacing::SM)
            .py(px(2.0))
            .bg(tint.opacity(0.12))
            .text_color(tint)
            .font_family(dbflux_components::fonts::editor_family(cx))
            .child(Text::caption(search.anchor_label.clone()).color(tint));

        // No spinner in the switch: an icon would widen the pressed segment
        // and move the controls after it. The Refresh button shows the run.
        let distances = VectorDistance::ALL
            .into_iter()
            .map(|distance| {
                SegmentedItem::new(distance_segment(distance), distance_label(distance))
            })
            .collect();

        let entity = cx.entity().downgrade();
        let distance_switch = SegmentedControl::new(
            distances,
            distance_segment(search.distance),
            move |selected, _, cx| {
                let Some(distance) = distance_for_segment(selected) else {
                    return;
                };
                if let Some(panel) = entity.upgrade() {
                    panel.update(cx, |panel, cx| {
                        panel.set_nearest_neighbor_distance(distance, cx);
                    });
                }
            },
        )
        .group("nearest-distance");

        let k_field = FilterField::new("nearest-neighbors-k").limit(
            dbflux_i18n::t!("document.data.grid.nearest.k"),
            Input::new(&self.nearest.k_input).small().appearance(false),
        );

        div()
            .flex()
            .flex_1()
            .min_w(px(0.0))
            .items_center()
            .gap(Spacing::SM)
            .child(
                Icon::new(AppIcon::ChartScatter)
                    .size(Fields::FILTER_ICON)
                    .color(tint),
            )
            .child(
                div()
                    .id("nearest-neighbors-summary")
                    .role(Role::Label)
                    .aria_label(self.nearest_neighbors_summary().unwrap_or_default())
                    .flex()
                    .flex_none()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        Text::body_sm(dbflux_i18n::t!("document.data.grid.nearest.similar_to"))
                            .color(theme.foreground),
                    )
                    .child(anchor_chip)
                    .child(
                        Text::body_sm(dbflux_i18n::t!("document.data.grid.nearest.by"))
                            .color(theme.muted_foreground),
                    ),
            )
            .child(
                // A fixed slot: a longer column name truncates instead of
                // pushing the distance switch and K along the bar.
                div()
                    .debug_selector(|| "nearest-neighbors-column-slot".to_string())
                    .flex_none()
                    .w(COLUMN_PICKER_WIDTH)
                    .overflow_hidden()
                    .child(self.nearest.column_dropdown.clone()),
            )
            .child(distance_switch)
            .child(k_field)
            .child(div().flex_1())
            .when_some(self.nearest.error.clone(), |bar, error| {
                bar.child(
                    div()
                        .id("nearest-neighbors-error")
                        .role(Role::Alert)
                        .flex()
                        .min_w(px(0.0))
                        .items_center()
                        .gap(Spacing::XS)
                        .px(Spacing::SM)
                        .py(px(2.0))
                        .bg(theme.danger.opacity(0.12))
                        .child(
                            Icon::new(AppIcon::TriangleAlert)
                                .size(Fields::FILTER_ICON)
                                .color(theme.danger),
                        )
                        .child(
                            div()
                                .min_w(px(0.0))
                                .truncate()
                                .child(Text::caption(error).color(theme.danger)),
                        ),
                )
            })
            .child(
                Button::new(
                    "nearest-neighbors-clear",
                    dbflux_i18n::t!("document.data.grid.nearest.back_to_table"),
                )
                .ghost()
                .icon(AppIcon::ChevronLeft)
                .kbd("Esc")
                .tab_stop(false)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.clear_nearest_neighbors(window, cx);
                    this.focus_table(window, cx);
                })),
            )
    }
}
