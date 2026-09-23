use dbflux_components::controls::{Button, GpuiInput, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, IconButton, KbdBadge, Text};
use dbflux_components::tokens::FontSizes;
use dbflux_components::tokens::{Heights, Radii, Spacing};
use dbflux_core::DatabaseCategory;
use gpui::prelude::*;
use gpui::*;
use gpui_component::{ActiveTheme, Sizable};

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

/// Column count of every category section's card grid. The layout
/// (`driver_section_grid`) and the keyboard navigator (`move_grid_focus`) both
/// read it, so the rendered rows and the vertical step cannot disagree.
///
/// Two columns fit the Connection Manager's 600 px minimum window width:
/// `2 * CARD_WIDTH + CARD_GAP + 2 * PICKER_PADDING_X = 540 px`. A third
/// column needs 800 px, wider than the 700 px the window opens at.
pub(super) const GRID_COLUMNS: usize = 2;

const CARD_WIDTH: f32 = 248.0;

/// Horizontal and vertical gap between cards in a section grid.
const CARD_GAP: Pixels = Spacing::MD;

/// Horizontal padding of the scrollable picker body.
const PICKER_PADDING_X: Pixels = Spacing::LG;

impl ConnectionManagerWindow {
    pub(super) fn render_driver_select(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
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
            .child(self.render_picker_header(cx))
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

    fn render_picker_header(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let muted = theme.muted_foreground;

        div()
            .flex()
            .flex_row()
            .items_center()
            .justify_between()
            .gap_3()
            .px_4()
            .py_3()
            .border_b_1()
            .border_color(theme.border)
            .child(
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .gap_2()
                    .child(
                        IconButton::new("cm-driver-back", AppIcon::ChevronLeft.into()).on_click(
                            |_, window, _cx| {
                                window.remove_window();
                            },
                        ),
                    )
                    .child(
                        Icon::new(AppIcon::Database)
                            .size(Heights::ICON_MD)
                            .color(muted),
                    )
                    .child(
                        Text::heading(dbflux_i18n::t!("connection_manager.driver_select.title"))
                            .font_size(FontSizes::LG),
                    )
                    .child(div().text_size(FontSizes::SM).text_color(muted).child("·"))
                    .child(
                        Text::muted(dbflux_i18n::t!("connection_manager.driver_select.subtitle"))
                            .font_size(FontSizes::SM),
                    ),
            )
            .child(
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .gap_2()
                    .w(px(360.0))
                    .child(render_filter_input(&self.form.driver_filter_input))
                    .child(KbdBadge::new("/")),
            )
    }

    fn render_picker_body(
        &self,
        visible: &[DriverInfo],
        focused_idx: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let muted = cx.theme().muted_foreground;

        let mut body = div()
            .id("cm-driver-grid")
            .flex_1()
            .flex()
            .flex_col()
            .gap_4()
            .px(PICKER_PADDING_X)
            .py_3()
            .overflow_scroll();

        let mut cursor_index: usize = 0;
        for section in visible_sections(visible) {
            let Some(first_driver) = section.first() else {
                continue;
            };
            body = body.child(render_section_header(
                first_driver.category,
                section.len(),
                muted,
            ));

            let mut grid = driver_section_grid();
            for driver in section {
                let is_focused = cursor_index == focused_idx;
                grid = grid.child(self.render_driver_card(driver, is_focused, cx));
                cursor_index += 1;
            }

            body = body.child(grid);
        }

        if visible.is_empty() {
            body = body.child(div().flex().items_center().justify_center().py_8().child(
                Text::muted(dbflux_i18n::t!(
                    "connection_manager.driver_select.empty_state"
                )),
            ));
        }

        body
    }

    fn render_driver_card(
        &self,
        driver: &DriverInfo,
        is_focused: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let border_color = if is_focused {
            theme.primary
        } else {
            theme.border
        };

        let driver_id_click = driver.id.clone();
        let port_hint = driver.default_port.map(|p| format!(":{}", p));

        div()
            .id(SharedString::from(format!("cm-driver-card-{}", driver.id)))
            .w(px(CARD_WIDTH))
            .flex()
            .flex_col()
            .gap_3()
            .p_3()
            .rounded(Radii::MD)
            .border_1()
            .border_color(border_color)
            .bg(theme.secondary)
            .cursor_pointer()
            .hover(|s| s.border_color(theme.primary.opacity(0.6)))
            .on_click(cx.listener(move |this, _, window, cx| {
                this.select_driver(&driver_id_click, window, cx);
            }))
            .child(
                Icon::new(AppIcon::for_driver(driver.icon, driver.category))
                    .size(px(32.0))
                    .color(theme.foreground),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .child(Text::heading(driver.name.clone()).font_size(FontSizes::BASE))
                    .child(Text::muted(driver.description.clone()).font_size(FontSizes::XS)),
            )
            .when_some(port_hint, |card, hint| {
                card.child(div().h(px(1.0)).bg(theme.border)).child(
                    div()
                        .flex()
                        .flex_row()
                        .items_center()
                        .child(Text::muted(hint).font_size(FontSizes::XS)),
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

        div()
            .flex()
            .flex_row()
            .items_center()
            .justify_between()
            .px_4()
            .py_3()
            .border_t_1()
            .border_color(theme.border)
            .justify_end()
            .child(
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .gap_2()
                    .child(
                        Button::new(
                            "cm-driver-import",
                            dbflux_i18n::t!("connection_manager.driver_select.import_from_file"),
                        )
                        .small()
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.open_import(window, cx);
                        })),
                    )
                    .child(
                        Button::new(
                            "cm-driver-import-external",
                            dbflux_i18n::t!("connection_manager.driver_select.import_from_client"),
                        )
                        .small()
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.open_import_external(window, cx);
                        })),
                    )
                    .child(
                        Button::new(
                            "cm-driver-cancel",
                            dbflux_i18n::t!("connection_manager.driver_select.cancel"),
                        )
                        .small()
                        .on_click(cx.listener(|_, _, window, cx| {
                            cx.emit(DismissEvent);
                            window.remove_window();
                        })),
                    )
                    .child({
                        let mut cta = Button::new("cm-driver-configure", cta_label)
                            .primary()
                            .small();
                        if cta_disabled {
                            cta = cta.disabled(true);
                        } else {
                            cta = cta.on_click(cx.listener(move |this, _, window, cx| {
                                this.select_driver(&cta_id, window, cx);
                            }));
                        }
                        cta
                    }),
            )
    }
}

fn render_filter_input(state: &Entity<InputState>) -> impl IntoElement {
    // `Icon::new` defaults the color to `theme.muted_foreground` so the
    // magnifier renders in the same muted tone as in the screenshot without
    // requiring a theme lookup at this call site.
    GpuiInput::new(state)
        .id("cm-driver-filter")
        .small()
        .cleanable(true)
        .prefix(Icon::new(AppIcon::Search).size(Heights::ICON_SM))
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

fn render_section_header(
    category: DatabaseCategory,
    count: usize,
    muted: gpui::Hsla,
) -> impl IntoElement {
    div()
        .flex()
        .flex_row()
        .items_center()
        .gap_2()
        .pt_2()
        .child(
            div()
                .text_size(FontSizes::XS)
                .text_color(muted)
                .font_weight(gpui::FontWeight::SEMIBOLD)
                .child(SharedString::from(category.display_name().to_uppercase())),
        )
        .child(
            div()
                .px(Spacing::XS)
                .text_size(FontSizes::XS)
                .text_color(muted)
                .child(SharedString::from(count.to_string())),
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

/// Container that lays out one category section's cards in exactly
/// `GRID_COLUMNS` columns, each as wide as its card.
fn driver_section_grid() -> Div {
    div()
        .grid()
        .grid_cols_max_content(GRID_COLUMNS as u16)
        .gap(CARD_GAP)
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
        CARD_GAP, CARD_WIDTH, DriverInfo, GRID_COLUMNS, GridDirection, PICKER_PADDING_X,
        driver_section_grid, move_grid_focus, visible_drivers, visible_section_sizes,
    };

    use GridDirection::{Down, Left, Right, Up};

    /// Minimum width `open_connection_manager` gives the Connection Manager
    /// window; the picker grid must fit it without clipping a column.
    const MIN_WINDOW_WIDTH: f32 = 600.0;

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
    fn grid_columns_fit_the_minimum_window_width() {
        let columns = GRID_COLUMNS as f32;
        let required = columns * CARD_WIDTH
            + (columns - 1.0) * f32::from(CARD_GAP)
            + 2.0 * f32::from(PICKER_PADDING_X);

        assert!(
            required <= MIN_WINDOW_WIDTH,
            "{GRID_COLUMNS} columns need {required} px, wider than the {MIN_WINDOW_WIDTH} px minimum window"
        );
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

            div()
                .w(px(1600.0))
                .child(driver_section_grid().children(cards))
        }
    }

    /// Even with room for many more cards per row, a section grid renders
    /// exactly `GRID_COLUMNS` columns, which is what `move_grid_focus` steps by.
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
                first.origin.x + (px(CARD_WIDTH) + CARD_GAP) * index as f32,
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
