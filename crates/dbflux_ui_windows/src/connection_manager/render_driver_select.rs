use crate::tokens::ConnectionFormMetrics;
use dbflux_components::controls::{Button, Input};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Chamfer, ChamferRing, Icon, Kbd, Text};
use dbflux_components::tokens::{ChamferCut, ChromeColors, Fields, ShellMetrics};
use dbflux_core::DatabaseCategory;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

use super::{ConnectionManagerWindow, DismissEvent, DriverInfo};

/// Display order for category sections in the picker.
const CATEGORY_ORDER: &[DatabaseCategory] = &[
    DatabaseCategory::Relational,
    DatabaseCategory::Document,
    DatabaseCategory::KeyValue,
    DatabaseCategory::WideColumn,
    DatabaseCategory::TimeSeries,
    DatabaseCategory::Graph,
    DatabaseCategory::LogStream,
    DatabaseCategory::ObjectStorage,
];

/// Column count used before the picker has been laid out once.
pub(super) const DEFAULT_GRID_COLUMNS: usize = 2;

/// Number of card columns that fit `available` width: as many cards of at
/// least `ConnectionFormMetrics::CARD_MIN_WIDTH` as fit with their gaps,
/// between one and `ConnectionFormMetrics::CARD_MAX_COLUMNS`. The layout
/// (`driver_section_grid`) and the keyboard navigator (`move_grid_focus`)
/// both use this count, so the rendered rows and the vertical step agree.
pub(super) fn grid_columns_for_width(available: Pixels) -> usize {
    let gap = f32::from(ConnectionFormMetrics::CARD_GAP);
    let card = f32::from(ConnectionFormMetrics::CARD_MIN_WIDTH);
    let fitting = ((f32::from(available) + gap) / (card + gap)).floor();

    if fitting.is_nan() || fitting < 1.0 {
        return 1;
    }

    (fitting as usize).min(ConnectionFormMetrics::CARD_MAX_COLUMNS)
}

impl ConnectionManagerWindow {
    pub(super) fn render_driver_select(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let available =
            window.viewport_size().width - ConnectionFormMetrics::PICKER_PADDING_X * 2.0;
        self.driver_grid_columns = grid_columns_for_width(available);

        let query = self.current_driver_filter(cx);
        let visible = visible_drivers(&self.available_drivers, &query);

        let focused_idx = self
            .driver_focus
            .index()
            .min(visible.len().saturating_sub(1));
        let focused_driver = visible.get(focused_idx).cloned();

        div()
            .flex()
            .flex_col()
            .size_full()
            .bg(cx.theme().popover)
            .child(self.render_picker_header(window, cx))
            .child(self.render_picker_body(&visible, focused_idx, cx))
            .child(self.render_picker_footer(focused_driver, cx))
    }

    /// Lowercased filter query, read live from the filter input each render.
    pub(super) fn current_driver_filter(&self, cx: &App) -> String {
        self.form
            .driver_filter_input
            .read(cx)
            .value()
            .to_string()
            .to_lowercase()
    }

    /// Header (P1DriverPicker): title and subtitle on the left, the filter
    /// field on the right.
    fn render_picker_header(
        &self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let filter = self.form.driver_filter_input.read(cx);
        let query_is_empty = filter.value().is_empty();
        let focused = filter.focus_handle(cx).contains_focused(window, cx);
        let theme = cx.theme();

        let mut shape = Chamfer::new(ChamferCut::CONTROL)
            .fill(theme.background)
            .border(theme.border);

        if focused {
            shape = shape.ring(ChamferRing::focus(ChromeColors::tint(theme)));
        }

        let filter_field = div()
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(Fields::GAP)
            .w(ConnectionFormMetrics::PICKER_FILTER_WIDTH)
            .h(Fields::HEIGHT)
            .px(Fields::PADDING_X)
            .text_size(Fields::TEXT)
            .child(shape)
            .child(
                Icon::new(AppIcon::Search)
                    .size(ShellMetrics::SIDEBAR_FILTER_ICON)
                    .color(theme.muted_foreground),
            )
            .child(
                div().flex_1().min_w_0().child(
                    Input::new(&self.form.driver_filter_input)
                        .id("cm-driver-filter")
                        .aria_label(dbflux_i18n::t!(
                            "connection_manager.driver_select.search_placeholder"
                        ))
                        .small()
                        .appearance(false)
                        .cleanable(true),
                ),
            )
            .when(query_is_empty, |field| field.child(Kbd::new("/")));

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(ConnectionFormMetrics::PICKER_HEADER_GAP)
            .py(ConnectionFormMetrics::PICKER_HEADER_PADDING_Y)
            .px(ConnectionFormMetrics::PICKER_PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(ConnectionFormMetrics::PICKER_TITLE_GAP)
                    .child(Text::title(dbflux_i18n::t!(
                        "connection_manager.driver_select.title"
                    )))
                    .child(
                        Text::body(dbflux_i18n::t!("connection_manager.driver_select.subtitle"))
                            .muted_foreground(),
                    ),
            )
            .child(div().flex_1())
            .child(filter_field)
    }

    fn render_picker_body(
        &self,
        visible: &[DriverInfo],
        focused_idx: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let mut body = div()
            .id("cm-driver-grid")
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .px(ConnectionFormMetrics::PICKER_PADDING_X)
            .pb(ConnectionFormMetrics::PICKER_BODY_PADDING_BOTTOM)
            .overflow_y_scroll();

        let mut cursor_index: usize = 0;
        for section in visible_sections(visible) {
            let Some(first_driver) = section.first() else {
                continue;
            };
            body = body.child(render_section_header(first_driver.category));

            let mut grid = driver_section_grid(self.driver_grid_columns);
            for driver in section {
                let is_focused = cursor_index == focused_idx;
                grid = grid.child(self.render_driver_card(driver, is_focused, cx));
                cursor_index += 1;
            }

            body = body.child(grid);
        }

        if visible.is_empty() {
            body = body.child(
                div()
                    .flex()
                    .items_center()
                    .justify_center()
                    .py(ConnectionFormMetrics::PICKER_CATEGORY_PADDING_TOP * 2.0)
                    .child(
                        Text::body(dbflux_i18n::t!(
                            "connection_manager.driver_select.empty_state"
                        ))
                        .muted_foreground(),
                    ),
            );
        }

        body
    }

    /// One driver card: logo, name and the default port (or the driver's
    /// description when it has no port) in a chamfered card; the card under
    /// the keyboard cursor gets an 8% tint wash and a check mark, plus the
    /// tint ring while focus is visible.
    fn render_driver_card(
        &self,
        driver: &DriverInfo,
        is_selected: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let driver_id_click = driver.id.clone();
        let detail = driver
            .default_port
            .map(|port| format!(":{port}"))
            .unwrap_or_else(|| driver.description.clone());

        let shape = if is_selected {
            Chamfer::new(ChamferCut::INPUT)
                .fill(tint.opacity(ConnectionFormMetrics::CARD_SELECTED_ALPHA))
                .ring(ChamferRing {
                    color: tint,
                    thickness: ConnectionFormMetrics::CARD_SELECTED_RING,
                    offset: -ConnectionFormMetrics::CARD_SELECTED_RING,
                    focus_visible: true,
                })
        } else {
            Chamfer::new(ChamferCut::INPUT)
                .fill(theme.background)
                .fill_hover(theme.secondary)
                .border(theme.border)
                .interactive(SharedString::from(format!(
                    "cm-driver-card-{}-shape",
                    driver.id
                )))
        };

        div()
            .id(SharedString::from(format!("cm-driver-card-{}", driver.id)))
            .relative()
            .min_w_0()
            .flex()
            .items_center()
            .gap(ConnectionFormMetrics::CARD_INNER_GAP)
            .p(ConnectionFormMetrics::CARD_PADDING)
            .cursor_pointer()
            .on_click(cx.listener(move |this, _, window, cx| {
                this.select_driver(&driver_id_click, window, cx);
            }))
            .child(shape)
            .child(
                Icon::new(AppIcon::for_driver(driver.icon, driver.category))
                    .size(ConnectionFormMetrics::CARD_LOGO)
                    .color(theme.foreground),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .flex()
                    .flex_col()
                    .gap(ConnectionFormMetrics::CARD_LINE_GAP)
                    .child(
                        Text::body(driver.name.clone())
                            .font_weight(FontWeight::BOLD)
                            .text_color(ChromeColors::strong(theme)),
                    )
                    .child(
                        div()
                            .overflow_hidden()
                            .whitespace_nowrap()
                            .text_ellipsis()
                            .child(
                                Text::code(detail)
                                    .font_size(ConnectionFormMetrics::CARD_DETAIL_FONT)
                                    .muted_foreground(),
                            ),
                    ),
            )
            .when(is_selected, |card| {
                card.child(
                    Icon::new(AppIcon::Check)
                        .size(ConnectionFormMetrics::CARD_CHECK)
                        .color(tint),
                )
            })
    }

    fn render_picker_footer(
        &self,
        focused_driver: Option<DriverInfo>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let cta_label = focused_driver
            .as_ref()
            .map(|d| crate::labels::driver_select_configure(&d.name))
            .unwrap_or_else(|| dbflux_i18n::t!("connection_manager.driver_select.configure"));
        let cta_id = focused_driver
            .as_ref()
            .map(|d| d.id.clone())
            .unwrap_or_default();
        let cta_disabled = focused_driver.is_none();

        let mut cta = Button::new("cm-driver-configure", cta_label)
            .primary()
            .icon(AppIcon::ChevronRight)
            .kbd("↵");
        if cta_disabled {
            cta = cta.disabled(true);
        } else {
            cta = cta.on_click(cx.listener(move |this, _, window, cx| {
                this.select_driver(&cta_id, window, cx);
            }));
        }

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(Fields::GAP)
            .h(crate::tokens::SettingsMetrics::FOOTER_HEIGHT)
            .px(crate::tokens::SettingsMetrics::FOOTER_PADDING_X)
            .border_t_1()
            .border_color(theme.border)
            .bg(theme.background)
            .child(
                Button::new(
                    "cm-driver-import",
                    dbflux_i18n::t!("connection_manager.driver_select.import_from_file"),
                )
                .secondary()
                .icon(AppIcon::Download)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.open_import(window, cx);
                })),
            )
            .child(
                Button::new(
                    "cm-driver-import-external",
                    dbflux_i18n::t!("connection_manager.driver_select.import_from_client"),
                )
                .secondary()
                .icon(AppIcon::ArrowLeftRight)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.open_import_external(window, cx);
                })),
            )
            .child(div().flex_1())
            .child(
                Button::new(
                    "cm-driver-cancel",
                    dbflux_i18n::t!("connection_manager.driver_select.cancel"),
                )
                .secondary()
                .kbd("Esc")
                .on_click(cx.listener(|_, _, window, cx| {
                    cx.emit(DismissEvent);
                    window.remove_window();
                })),
            )
            .child(cta)
    }
}

fn driver_matches_query(driver: &DriverInfo, query: &str) -> bool {
    if query.is_empty() {
        return true;
    }
    let port_str = driver
        .default_port
        .map(|p| p.to_string())
        .unwrap_or_default();
    driver.name.to_lowercase().contains(query)
        || driver.id.to_lowercase().contains(query)
        || driver.uri_scheme.to_lowercase().contains(query)
        || port_str.contains(query)
        || driver.description.to_lowercase().contains(query)
}

/// Category label above a section of cards: 16 px above, 8 px below.
fn render_section_header(category: DatabaseCategory) -> impl IntoElement {
    div()
        .pt(ConnectionFormMetrics::PICKER_CATEGORY_PADDING_TOP)
        .pb(ConnectionFormMetrics::PICKER_CATEGORY_PADDING_BOTTOM)
        .child(
            Text::label(category.display_name().to_string())
                .font_size(ShellMetrics::SECTION_LABEL_FONT),
        )
}

/// Build the ordered list of drivers visible in the picker for the given
/// query, in display order (category section grouping included).
pub(super) fn visible_drivers(drivers: &[DriverInfo], query: &str) -> Vec<DriverInfo> {
    let q = query.to_lowercase();
    let mut out = Vec::new();
    for category in CATEGORY_ORDER {
        let mut bucket: Vec<DriverInfo> = drivers
            .iter()
            .filter(|d| d.category == *category && driver_matches_query(d, &q))
            .cloned()
            .collect();
        bucket.sort_by_key(|d| d.name.to_lowercase());
        out.extend(bucket);
    }
    out
}

/// Split the ordered visible-driver list into its category sections, in
/// display order. `visible_drivers` already groups drivers by category, so
/// every section is a non-empty run of equal categories and categories with
/// no visible driver produce no section.
pub(super) fn visible_sections(visible: &[DriverInfo]) -> impl Iterator<Item = &[DriverInfo]> {
    visible.chunk_by(|left, right| left.category == right.category)
}

/// Card counts of the visible category sections, in display order.
pub(super) fn visible_section_sizes(visible: &[DriverInfo]) -> Vec<usize> {
    visible_sections(visible).map(<[DriverInfo]>::len).collect()
}

/// Container that lays out one category section's cards in `columns`
/// equal columns.
fn driver_section_grid(columns: usize) -> Div {
    div()
        .grid()
        .grid_cols(columns as u16)
        .gap(ConnectionFormMetrics::CARD_GAP)
}

/// Direction of a single 2D grid move.
#[derive(Clone, Copy)]
pub(super) enum GridDirection {
    Left,
    Right,
    Up,
    Down,
}

/// Compute the next focus index after a 2D move across the visible cards.
///
/// `section_sizes` holds the card count of each rendered category section in
/// display order; each section is laid out as its own grid of `columns`
/// columns, and the returned index is into the flattened visible list.
///
/// - Left and Right step through the flattened list, wrapping at both ends.
/// - Down moves to the same column in the next row of the section. From the
///   section's last row it moves to the first row of the next section (the
///   first section after the last one), clamped to that section's last card.
/// - Up mirrors Down: from a section's first row it moves to the last row of
///   the previous section (the last section before the first one), clamped
///   to that section's last card.
///
/// Empty sections are skipped. With no cards the result is 0.
pub(super) fn move_grid_focus(
    section_sizes: &[usize],
    columns: usize,
    current: usize,
    direction: GridDirection,
) -> usize {
    let sections: Vec<usize> = section_sizes
        .iter()
        .copied()
        .filter(|size| *size > 0)
        .collect();
    let total: usize = sections.iter().sum();
    if total == 0 {
        return 0;
    }

    let columns = columns.max(1);
    let last = total - 1;
    let current = current.min(last);

    match direction {
        GridDirection::Left => {
            if current == 0 {
                last
            } else {
                current - 1
            }
        }
        GridDirection::Right => {
            if current == last {
                0
            } else {
                current + 1
            }
        }
        GridDirection::Down => {
            let (section, offset) = locate_card(&sections, current);
            let (row, column) = (offset / columns, offset % columns);

            let (target_section, target_row) = if (row + 1) * columns < sections[section] {
                (section, row + 1)
            } else {
                ((section + 1) % sections.len(), 0)
            };

            card_at(&sections, columns, target_section, target_row, column)
        }
        GridDirection::Up => {
            let (section, offset) = locate_card(&sections, current);
            let (row, column) = (offset / columns, offset % columns);

            let (target_section, target_row) = if row > 0 {
                (section, row - 1)
            } else {
                let previous = (section + sections.len() - 1) % sections.len();
                (previous, (sections[previous] - 1) / columns)
            };

            card_at(&sections, columns, target_section, target_row, column)
        }
    }
}

/// Flat index of the card at `row`/`column` of `section`, clamped to the
/// section's last card when that row is shorter than `column`.
fn card_at(sections: &[usize], columns: usize, section: usize, row: usize, column: usize) -> usize {
    let section_start: usize = sections[..section].iter().sum();
    let offset = (row * columns + column).min(sections[section] - 1);
    section_start + offset
}

/// Section index and offset within that section of flat card `index`.
/// `sections` must be non-empty, hold only non-zero sizes, and sum past
/// `index`.
fn locate_card(sections: &[usize], index: usize) -> (usize, usize) {
    let mut start = 0;
    for (section, size) in sections.iter().enumerate() {
        if index < start + size {
            return (section, index - start);
        }
        start += size;
    }

    let last_section = sections.len() - 1;
    (last_section, sections[last_section] - 1)
}

#[cfg(test)]
mod category_order_tests {
    use dbflux_core::{DatabaseCategory, Icon};

    use super::DriverInfo;
    use super::visible_drivers;

    fn driver_of(category: DatabaseCategory) -> DriverInfo {
        DriverInfo {
            id: "test".to_string(),
            icon: Icon::Database,
            name: "Test".to_string(),
            description: String::new(),
            category,
            default_port: None,
            uri_scheme: "test".to_string(),
        }
    }

    /// A driver whose category is missing from `CATEGORY_ORDER` silently
    /// disappears from the picker, so every `DatabaseCategory` variant must
    /// be listed there.
    #[test]
    fn visible_drivers_never_drops_a_category() {
        let categories = [
            DatabaseCategory::Relational,
            DatabaseCategory::Document,
            DatabaseCategory::KeyValue,
            DatabaseCategory::Graph,
            DatabaseCategory::TimeSeries,
            DatabaseCategory::WideColumn,
            DatabaseCategory::LogStream,
            DatabaseCategory::ObjectStorage,
        ];

        let drivers: Vec<DriverInfo> = categories.iter().map(|c| driver_of(*c)).collect();

        assert_eq!(
            visible_drivers(&drivers, "").len(),
            drivers.len(),
            "a DatabaseCategory variant is missing from CATEGORY_ORDER"
        );
    }
}

#[cfg(test)]
mod grid_navigation_tests {
    use dbflux_core::{DatabaseCategory, Icon};
    use gpui::{
        Context, InteractiveElement, IntoElement, ParentElement, Render, Styled, TestAppContext,
        Window, div, px,
    };

    use super::{
        ConnectionFormMetrics, DriverInfo, GridDirection, driver_section_grid,
        grid_columns_for_width, move_grid_focus, visible_drivers, visible_section_sizes,
    };

    /// Column count and card width the section-grid harness lays out.
    const GRID_COLUMNS: usize = 3;
    const CARD_WIDTH: f32 = 240.0;

    use GridDirection::{Down, Left, Right, Up};

    fn driver(id: &str, category: DatabaseCategory) -> DriverInfo {
        DriverInfo {
            id: id.to_string(),
            icon: Icon::Database,
            name: id.to_string(),
            description: String::new(),
            category,
            default_port: None,
            uri_scheme: id.to_string(),
        }
    }

    // Two columns, sections of 3 and 2 cards:
    //   section 0:  0 1
    //               2
    //   section 1:  3 4
    const TWO_SECTIONS: &[usize] = &[3, 2];

    #[test]
    fn down_moves_within_a_section_and_clamps_to_a_short_last_row() {
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 0, Down), 2);
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 1, Down), 2);
    }

    #[test]
    fn down_from_a_sections_last_row_enters_the_next_section_in_the_same_column() {
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 2, Down), 3);
    }

    #[test]
    fn down_from_the_last_section_wraps_to_the_first_section() {
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 3, Down), 0);
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 4, Down), 1);
    }

    #[test]
    fn up_from_a_sections_first_row_enters_the_previous_sections_last_row() {
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 3, Up), 2);
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 4, Up), 2);
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 2, Up), 0);
    }

    #[test]
    fn up_from_the_first_section_wraps_to_the_last_section() {
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 0, Up), 3);
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 1, Up), 4);
    }

    #[test]
    fn left_and_right_follow_the_flattened_order_and_wrap_at_the_ends() {
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 0, Left), 4);
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 4, Right), 0);
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 1, Right), 2);
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 2, Right), 3);
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 3, Left), 2);
    }

    // Three columns, odd section sizes 4, 7 and 1:
    //   section 0:  0 1 2      section 1:  4  5  6      section 2:  11
    //               3                      7  8  9
    //                                      10
    const ODD_SECTIONS: &[usize] = &[4, 7, 1];

    #[test]
    fn odd_section_sizes_clamp_every_vertical_move_to_an_existing_card() {
        assert_eq!(move_grid_focus(ODD_SECTIONS, 3, 2, Down), 3);
        assert_eq!(move_grid_focus(ODD_SECTIONS, 3, 3, Down), 4);
        assert_eq!(move_grid_focus(ODD_SECTIONS, 3, 9, Down), 10);
        assert_eq!(move_grid_focus(ODD_SECTIONS, 3, 10, Down), 11);
        assert_eq!(move_grid_focus(ODD_SECTIONS, 3, 11, Down), 0);

        assert_eq!(move_grid_focus(ODD_SECTIONS, 3, 11, Up), 10);
        assert_eq!(move_grid_focus(ODD_SECTIONS, 3, 4, Up), 3);
        assert_eq!(move_grid_focus(ODD_SECTIONS, 3, 6, Up), 3);
        assert_eq!(move_grid_focus(ODD_SECTIONS, 3, 0, Up), 11);
    }

    #[test]
    fn a_single_section_wraps_vertically_within_itself() {
        assert_eq!(move_grid_focus(&[5], 2, 4, Down), 0);
        assert_eq!(move_grid_focus(&[5], 2, 3, Down), 4);
        assert_eq!(move_grid_focus(&[5], 2, 1, Up), 4);
    }

    #[test]
    fn empty_sections_are_skipped() {
        let with_empty = [2, 0, 0, 3];

        assert_eq!(move_grid_focus(&with_empty, 2, 0, Down), 2);
        assert_eq!(move_grid_focus(&with_empty, 2, 1, Down), 3);
        assert_eq!(move_grid_focus(&with_empty, 2, 2, Up), 0);
        assert_eq!(move_grid_focus(&with_empty, 2, 4, Down), 0);
    }

    #[test]
    fn no_visible_cards_keeps_focus_at_zero() {
        for direction in [Left, Right, Up, Down] {
            assert_eq!(move_grid_focus(&[], 2, 3, direction), 0);
            assert_eq!(move_grid_focus(&[0, 0], 2, 3, direction), 0);
        }
    }

    #[test]
    fn a_stale_focus_past_the_filtered_list_is_clamped_to_its_last_card() {
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 99, Down), 1);
        assert_eq!(move_grid_focus(TWO_SECTIONS, 2, 99, Right), 0);
    }

    #[test]
    fn section_sizes_follow_the_filter_and_drop_emptied_categories() {
        let drivers = vec![
            driver("postgres", DatabaseCategory::Relational),
            driver("mysql", DatabaseCategory::Relational),
            driver("sqlite", DatabaseCategory::Relational),
            driver("mongodb", DatabaseCategory::Document),
            driver("redis", DatabaseCategory::KeyValue),
            driver("valkey", DatabaseCategory::KeyValue),
        ];

        assert_eq!(
            visible_section_sizes(&visible_drivers(&drivers, "")),
            vec![3, 1, 2]
        );
        assert_eq!(
            visible_section_sizes(&visible_drivers(&drivers, "s")),
            vec![3, 1]
        );
        assert_eq!(
            visible_section_sizes(&visible_drivers(&drivers, "re")),
            vec![1, 1]
        );
        assert!(visible_section_sizes(&visible_drivers(&drivers, "zzz")).is_empty());
    }

    #[test]
    fn grid_columns_follow_the_available_width() {
        assert_eq!(grid_columns_for_width(px(100.0)), 1);
        assert_eq!(grid_columns_for_width(px(450.0)), 2);
        assert_eq!(grid_columns_for_width(px(700.0)), 3);
        assert_eq!(grid_columns_for_width(px(992.0)), 4);
    }

    #[test]
    fn grid_columns_never_exceed_the_design_maximum() {
        assert_eq!(
            grid_columns_for_width(px(4000.0)),
            ConnectionFormMetrics::CARD_MAX_COLUMNS
        );
    }

    #[test]
    fn a_zero_or_negative_width_still_lays_out_one_column() {
        assert_eq!(grid_columns_for_width(px(0.0)), 1);
        assert_eq!(grid_columns_for_width(px(-50.0)), 1);
    }

    struct SectionGridHarness;

    impl Render for SectionGridHarness {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            let cards = (0..=GRID_COLUMNS).map(|index| {
                div()
                    .debug_selector(move || format!("grid-card-{index}"))
                    .w(px(CARD_WIDTH))
                    .h(px(40.0))
            });

            let columns = GRID_COLUMNS as f32;
            let width =
                px(CARD_WIDTH) * columns + ConnectionFormMetrics::CARD_GAP * (columns - 1.0);

            div()
                .w(width)
                .child(driver_section_grid(GRID_COLUMNS).children(cards))
        }
    }

    /// A section grid renders exactly the columns it is given, which is what
    /// `move_grid_focus` steps by.
    #[gpui::test]
    fn section_grid_renders_grid_columns_cards_per_row(cx: &mut TestAppContext) {
        let (_, window) = cx.add_window_view(|_, _| SectionGridHarness);

        let mut card_bounds = |index: usize| {
            let selector: &'static str = format!("grid-card-{index}").leak();
            window
                .debug_bounds(selector)
                .unwrap_or_else(|| panic!("{selector} was not rendered"))
        };

        let first = card_bounds(0);
        for index in 1..GRID_COLUMNS {
            let card = card_bounds(index);
            assert_eq!(
                card.origin.y, first.origin.y,
                "card {index} left the first row"
            );
            assert_eq!(
                card.origin.x,
                first.origin.x + (px(CARD_WIDTH) + ConnectionFormMetrics::CARD_GAP) * index as f32,
                "card {index} is not in column {index}"
            );
        }

        let wrapped = card_bounds(GRID_COLUMNS);
        assert_eq!(wrapped.origin.x, first.origin.x);
        assert!(
            wrapped.origin.y > first.origin.y,
            "card {GRID_COLUMNS} should start the second row"
        );
    }
}
