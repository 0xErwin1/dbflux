use dbflux_components::controls::{Button, Input};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{SegmentedControl, SegmentedItem, Text};
use dbflux_components::tokens::BuilderMetrics;
use dbflux_core::{OrderByMode, VisualSortDirection};
use gpui::prelude::*;
use gpui::{AnyElement, Context, IntoElement, SharedString, div};

use crate::labels::sort_direction_label;
use crate::query_builder::keyboard::row_id;
use crate::query_builder::panel::QueryBuilderPanel;
use dbflux_components::composites::RailMark;

/// Renders the Sort and limit section of the Query Builder (IslBuilder).
///
/// Each sort row is a column dropdown and an ASC/DESC switch; the first row
/// also carries the limit field, as the board draws it. Further rows add a
/// multi-column ORDER BY and can be removed. The last line appends a sort by
/// picking its column and holds the offset field.
///
/// Drivers that order only on their sort key get the direction toggle
/// instead of the rows, and drivers that cannot order at all get only the
/// limit and offset fields.
pub fn render_sort(
    panel: &mut QueryBuilderPanel,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    let mode = panel.order_by_mode(cx);
    let container = div().flex().flex_col().gap(BuilderMetrics::ROW_GAP);

    match mode {
        OrderByMode::SortKeyOnly => container
            .child(render_sort_key_only(panel, cx))
            .child(limit_offset_row(panel))
            .into_any_element(),
        OrderByMode::None => container.child(limit_offset_row(panel)).into_any_element(),
        OrderByMode::AnyColumns => render_sort_rows(panel, container, cx).into_any_element(),
    }
}

fn render_sort_rows(
    panel: &mut QueryBuilderPanel,
    container: gpui::Div,
    cx: &mut Context<QueryBuilderPanel>,
) -> gpui::Div {
    panel.sync_sort_dropdowns(cx);

    let sort_rows = panel.sort_rows.clone();
    let dropdowns = panel.sort_column_dropdowns.clone();
    let mark = panel.rail_mark.clone();
    let paging = row_id::paging();

    let mut container = container;

    if sort_rows.is_empty() {
        let add = panel.sort_add_dropdown.clone();
        let limit = limit_field(panel).map(|field| mark.ring(&paging, "limit", field));

        return container
            .child(
                mark.row(
                    &paging,
                    div()
                        .flex()
                        .items_center()
                        .gap(BuilderMetrics::ROW_GAP)
                        .when_some(add, |row, add| {
                            row.child(mark.ring(
                                &paging,
                                "add",
                                column_slot(add.into_any_element()),
                            ))
                        })
                        .children(limit),
                ),
            )
            .child(offset_line(panel, None, &mark));
    }

    let first_row = row_id::sort(0);
    let mut limit = limit_field(panel).map(|field| mark.ring(&first_row, "limit", field));

    for (index, (row, dropdown)) in sort_rows.iter().zip(dropdowns).enumerate() {
        let sort_row = row_id::sort(index);
        let line = div()
            .flex()
            .items_center()
            .gap(BuilderMetrics::ROW_GAP)
            .child(mark.ring(
                &sort_row,
                "column",
                column_slot(dropdown.into_any_element()),
            ))
            .child(mark.ring_element(&sort_row, "dir", direction_switch(index, row.direction, cx)))
            .when(index == 0, |line| line.children(limit.take()))
            .child(div().flex_1())
            .child(
                Button::new(
                    ("qb-rm-sort", index),
                    dbflux_i18n::t!("document.query_builder.filters.remove"),
                )
                .ghost()
                .inline()
                .icon(AppIcon::CircleX)
                .icon_only()
                .tab_stop(false)
                .on_click(cx.listener(move |this, _event, _window, cx| {
                    this.remove_sort(index, cx);
                })),
            );

        container = container.child(mark.row(&sort_row, line));
    }

    let add = panel.sort_add_dropdown.clone().map(|add| {
        mark.ring(&paging, "add", column_slot(add.into_any_element()))
            .into_any_element()
    });

    container.child(offset_line(panel, add, &mark))
}

/// The ASC/DESC switch of the sort row at `index`.
fn direction_switch(
    index: usize,
    direction: VisualSortDirection,
    cx: &mut Context<QueryBuilderPanel>,
) -> SegmentedControl {
    let direction_id = move |direction: VisualSortDirection| {
        SharedString::from(format!("qb-sort-dir-{index}-{direction:?}"))
    };
    let current = direction_id(direction);
    let weak = cx.weak_entity();

    SegmentedControl::new(
        [VisualSortDirection::Asc, VisualSortDirection::Desc]
            .into_iter()
            .map(|direction| {
                SegmentedItem::new(direction_id(direction), sort_direction_label(direction))
            })
            .collect(),
        current.clone(),
        move |id, _, cx| {
            if *id == current {
                return;
            }

            if let Some(builder) = weak.upgrade() {
                builder.update(cx, |this, cx| this.toggle_sort_direction(index, cx));
            }
        },
    )
}

/// A column dropdown at the board's 150 px width.
fn column_slot(dropdown: AnyElement) -> gpui::Div {
    div()
        .w(BuilderMetrics::SORT_COLUMN_WIDTH)
        .flex_shrink_0()
        .child(dropdown)
}

/// The 70 px limit field, or `None` in tests that build no input state.
fn limit_field(panel: &QueryBuilderPanel) -> Option<gpui::Div> {
    panel.limit_input_state.as_ref().map(|state| {
        div()
            .w(BuilderMetrics::SORT_LIMIT_WIDTH)
            .flex_shrink_0()
            .child(
                Input::new(state)
                    .small()
                    .aria_label(dbflux_i18n::t!("document.query_builder.status.limit"))
                    .w_full(),
            )
    })
}

/// The last line: an optional leading control (the add-sort dropdown), then
/// the offset field at the right.
fn offset_line(
    panel: &QueryBuilderPanel,
    leading: Option<AnyElement>,
    mark: &RailMark,
) -> AnyElement {
    let paging = row_id::paging();

    mark.row(&paging, div())
        .flex()
        .items_center()
        .gap(BuilderMetrics::ROW_GAP)
        .children(leading)
        .child(div().flex_1())
        .when_some(panel.offset_input_state.as_ref(), |line, state| {
            line.child(Text::caption(dbflux_i18n::t!(
                "document.query_builder.status.offset"
            )))
            .child(
                mark.ring(&paging, "offset", div())
                    .w(BuilderMetrics::SORT_LIMIT_WIDTH)
                    .flex_shrink_0()
                    .child(
                        Input::new(state)
                            .small()
                            .aria_label(dbflux_i18n::t!("document.query_builder.status.offset"))
                            .w_full(),
                    ),
            )
        })
        .into_any_element()
}

/// Limit and offset side by side, for drivers without column sorting.
fn limit_offset_row(panel: &QueryBuilderPanel) -> AnyElement {
    let mark = panel.rail_mark.clone();
    let paging = row_id::paging();
    let limit = limit_field(panel).map(|field| {
        let field = mark.ring(&paging, "limit", field);
        div()
            .flex()
            .items_center()
            .gap(BuilderMetrics::ROW_GAP)
            .child(Text::caption(dbflux_i18n::t!(
                "document.query_builder.status.limit"
            )))
            .child(field)
            .into_any_element()
    });

    offset_line(panel, limit, &mark)
}

/// Renders the sort section for drivers that can order only on the sort key
/// (`OrderByMode::SortKeyOnly`, e.g. DynamoDB): a single ascending/descending
/// toggle with an inline hint, instead of the multi-column sort list.
fn render_sort_key_only(
    panel: &mut QueryBuilderPanel,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use gpui_component::ActiveTheme;

    let direction = panel.sort_key_direction();
    let dir_label = sort_direction_label(direction);

    let sort_key_label = panel
        .sort_key_column()
        .filter(|name| !name.is_empty())
        .map(|name| dbflux_i18n::t!("document.query_builder.sort.key_order_named", name = name))
        .unwrap_or_else(|| dbflux_i18n::t!("document.query_builder.sort.key_order"));

    let mark = panel.rail_mark.clone();

    div()
        .flex()
        .flex_col()
        .gap_1()
        .child(
            mark.row(&row_id::sort_key(), div())
                .flex()
                .flex_row()
                .gap_1()
                .items_center()
                .child(
                    div()
                        .flex_1()
                        .text_sm()
                        .child(SharedString::from(sort_key_label)),
                )
                .child(
                    Button::new("qb-sortkey-dir", dir_label)
                        .ghost()
                        .inline()
                        .on_click(cx.listener(move |this, _event, _window, cx| {
                            let next = match this.sort_key_direction() {
                                VisualSortDirection::Asc => VisualSortDirection::Desc,
                                VisualSortDirection::Desc => VisualSortDirection::Asc,
                            };
                            this.set_sort_key_direction(next, cx);
                        })),
                ),
        )
        .child(
            div()
                .text_xs()
                .text_color(cx.theme().muted_foreground)
                .child(dbflux_i18n::t!(
                    "document.query_builder.sort.key_limited_hint"
                )),
        )
}
