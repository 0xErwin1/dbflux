use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Chamfer;
use dbflux_components::tokens::{BuilderMetrics, ChamferCut, ChromeColors, Feedback};
use dbflux_components::typography::AppFonts;
use gpui::prelude::*;
use gpui::{App, Context, ElementId, FontWeight, Hsla, IntoElement, SharedString, Stateful, div};
use gpui_component::ActiveTheme;

use crate::query_builder::keyboard::row_id;
use crate::query_builder::panel::{ProjectionMode, QueryBuilderPanel};

/// A 20 px chip on a 4 px chamfer, as the Columns card draws its column
/// names (mono) and its "+ column" action.
fn column_chip(
    id: impl Into<ElementId>,
    label: impl Into<SharedString>,
    fill: Hsla,
    color: Hsla,
    mono: bool,
    cx: &App,
) -> Stateful<gpui::Div> {
    div()
        .id(id)
        .relative()
        .flex()
        .flex_shrink_0()
        .items_center()
        .h(Feedback::BADGE_HEIGHT)
        .px(Feedback::BADGE_PADDING_X)
        .cursor_pointer()
        .whitespace_nowrap()
        .text_size(Feedback::BADGE_FONT)
        .font_weight(FontWeight::SEMIBOLD)
        .text_color(color)
        .when(mono, |chip| {
            chip.font_family(dbflux_components::fonts::editor_family(cx))
        })
        .child(Chamfer::new(ChamferCut::KEYCAP).fill(fill))
        .child(label.into())
}

/// Renders the Columns section of the Query Builder.
///
/// Shows an "All columns (*)" checkbox; when unchecked, the selected columns
/// as chips (a click removes one) and a "+ column" chip that lists every
/// source-table column with its own checkbox. A free-text "alias.column" +
/// Add row remains below for columns from joined tables that are not in the
/// source's column list.
pub fn render_columns(
    panel: &mut QueryBuilderPanel,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::{Button, Checkbox};

    let all_active = panel.projection_mode == ProjectionMode::All;
    let source_alias = panel.current_spec.source.alias.clone();
    let available_columns = panel.available_columns.clone();
    let mark = panel.rail_mark.clone();

    let mut container = div().flex().flex_col().gap(BuilderMetrics::ROW_GAP).child(
        mark.row(
            &row_id::all_columns(),
            div().child(
                Checkbox::new("qb-all-columns")
                    .checked(all_active)
                    .label(dbflux_i18n::t!(
                        "document.query_builder.columns.all_columns"
                    ))
                    .on_click(cx.listener(|this, checked, _window, cx| {
                        this.set_all_columns(*checked, cx);
                    })),
            ),
        ),
    );

    if all_active {
        return container;
    }

    let theme = cx.theme().clone();
    let tint = ChromeColors::tint(&theme);
    let picker_open = panel.column_picker_open;

    // Selected columns as chips: a click removes the column.
    let chips = panel
        .projection_rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            let label = if row.source_alias == source_alias {
                row.column.clone()
            } else {
                format!("{}.{}", row.source_alias, row.column)
            };
            let alias_for_listener = row.source_alias.clone();
            let column_for_listener = row.column.clone();
            let chip_row = row_id::picked_column(&row.source_alias, &row.column);

            let chip = column_chip(
                ("qb-col-chip", index),
                label,
                theme.secondary,
                ChromeColors::strong(&theme),
                true,
                cx,
            )
            .on_click(cx.listener(move |this, _event, _window, cx| {
                this.toggle_column(&alias_for_listener, &column_for_listener, cx);
            }));

            mark.row(&chip_row, mark.ring(&chip_row, "chip", chip))
                .into_any_element()
        })
        .collect::<Vec<_>>();

    let add_chip = column_chip(
        "qb-col-add-chip",
        dbflux_i18n::t!("document.query_builder.columns.add_column"),
        tint.opacity(Feedback::BADGE_FILL_ALPHA),
        tint,
        false,
        cx,
    )
    .on_click(cx.listener(|this, _event, _window, cx| {
        this.column_picker_open = !this.column_picker_open;
        cx.notify();
    }));
    let entry_row = row_id::column_entry();
    let add_chip = mark.row(&entry_row, mark.ring(&entry_row, "add-chip", add_chip));

    container = container.child(
        div()
            .flex()
            .flex_wrap()
            .gap(BuilderMetrics::CHIP_GAP)
            .children(chips)
            .child(add_chip),
    );

    if !picker_open {
        return container;
    }

    let columns_grid = available_columns.chunks(2).enumerate().fold(
        div().flex().flex_col().gap(BuilderMetrics::ROW_GAP),
        |grid, (chunk_ix, chunk)| {
            let mut row = div().flex().flex_row().gap(BuilderMetrics::ROW_GAP);
            for (within_ix, col_name) in chunk.iter().enumerate() {
                let i = chunk_ix * 2 + within_ix;
                let alias_for_listener = source_alias.clone();
                let column_for_listener = col_name.clone();
                let checked = panel.is_column_selected(&source_alias, col_name);

                row = row.child(
                    mark.row(
                        &row_id::column_choice(col_name),
                        div().flex_1().min_w(gpui::px(0.0)),
                    )
                    .child(
                        Checkbox::new(("qb-col-toggle", i))
                            .checked(checked)
                            .label(col_name.clone())
                            .on_click(cx.listener(move |this, _checked, _window, cx| {
                                this.toggle_column(&alias_for_listener, &column_for_listener, cx);
                            })),
                    ),
                );
            }
            // Pad single-item row so layout stays balanced.
            if chunk.len() == 1 {
                row = row.child(div().flex_1());
            }
            grid.child(row)
        },
    );
    container = container.child(columns_grid);

    if let Some(add_state) = panel.add_column_input_state.as_ref() {
        container = container.child(
            mark.row(
                &entry_row,
                div()
                    .flex()
                    .flex_row()
                    .gap(BuilderMetrics::ROW_GAP)
                    .items_center(),
            )
            .child(mark.ring(&entry_row, "input", div().flex_1()).child(
                crate::completion_support::single_line_completion_editor(add_state, cx).w_full(),
            ))
            .child(
                mark.ring_element(
                    &entry_row,
                    "add",
                    Button::new("qb-add-col", dbflux_i18n::t!("document.shared.add"))
                        .icon(AppIcon::Plus)
                        .on_click(cx.listener(|this, _event, window, cx| {
                            this.add_column_from_entry(window, cx);
                        })),
                ),
            ),
        );
    }

    container
}
