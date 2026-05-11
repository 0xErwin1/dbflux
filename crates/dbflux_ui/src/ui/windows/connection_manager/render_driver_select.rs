use crate::ui::icons::AppIcon;
use crate::ui::tokens::FontSizes;
use dbflux_components::controls::{Button, GpuiInput, InputState};
use dbflux_components::primitives::{Chord, Icon, IconButton, KbdBadge, Text};
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
];

/// Target column count for the card grid. Cards visually wrap, but the
/// keyboard navigator treats the visible list as a flat index whose vertical
/// step equals `GRID_COLUMNS`.
pub(super) const GRID_COLUMNS: usize = 4;

const CARD_WIDTH: f32 = 248.0;

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
        self.driver_filter_input
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
                    .child(Text::heading("New Connection").font_size(FontSizes::LG))
                    .child(div().text_size(FontSizes::SM).text_color(muted).child("·"))
                    .child(Text::muted("choose a database type").font_size(FontSizes::SM)),
            )
            .child(
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .gap_2()
                    .w(px(360.0))
                    .child(render_filter_input(&self.driver_filter_input))
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
            .px_4()
            .py_3()
            .overflow_scroll();

        let mut cursor_index: usize = 0;
        let mut rendered_any = false;
        for category in CATEGORY_ORDER {
            let section_drivers: Vec<&DriverInfo> =
                visible.iter().filter(|d| d.category == *category).collect();

            if section_drivers.is_empty() {
                continue;
            }
            rendered_any = true;

            body = body.child(render_section_header(
                *category,
                section_drivers.len(),
                muted,
            ));

            let mut grid = div().flex().flex_wrap().gap_3();
            for driver in section_drivers {
                let is_focused = cursor_index == focused_idx;
                grid = grid.child(self.render_driver_card(driver, is_focused, cx));
                cursor_index += 1;
            }

            body = body.child(grid);
        }

        if !rendered_any {
            body = body.child(
                div()
                    .flex()
                    .items_center()
                    .justify_center()
                    .py_8()
                    .child(Text::muted("No drivers match your filter")),
            );
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
                Icon::new(AppIcon::from_icon(driver.icon))
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
            .map(|d| format!("Configure {}", d.name))
            .unwrap_or_else(|| "Configure".to_string());
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
            .child(
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .gap_4()
                    .child(footer_hint(Chord::new(["↑", "↓", "←", "→"]), "navigate"))
                    .child(footer_hint(Chord::new(["/"]), "filter"))
                    .child(footer_hint(Chord::new(["↵"]), "select"))
                    .child(footer_hint(Chord::new(["Esc"]), "close")),
            )
            .child(
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .gap_2()
                    .child(
                        Button::new("cm-driver-cancel", "Cancel").on_click(cx.listener(
                            |_, _, window, cx| {
                                cx.emit(DismissEvent);
                                window.remove_window();
                            },
                        )),
                    )
                    .child({
                        let mut cta = Button::new("cm-driver-configure", cta_label).primary();
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

fn footer_hint(chord: Chord, label: &'static str) -> impl IntoElement {
    div()
        .flex()
        .flex_row()
        .items_center()
        .gap_1()
        .child(chord)
        .child(div().text_size(FontSizes::XS).child(label))
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

/// Direction of a single 2D grid move.
#[derive(Clone, Copy)]
pub(super) enum GridDirection {
    Left,
    Right,
    Up,
    Down,
}

/// Compute the next focus index after a 2D move across the flattened
/// visible-driver list. Sections share a single flat index, so vertical moves
/// can cross section boundaries when the column lines up.
pub(super) fn move_grid_focus(visible_count: usize, current: usize, dir: GridDirection) -> usize {
    if visible_count == 0 {
        return 0;
    }
    let last = visible_count - 1;
    let cur = current.min(last);

    match dir {
        GridDirection::Left => {
            if cur == 0 {
                last
            } else {
                cur - 1
            }
        }
        GridDirection::Right => {
            if cur == last {
                0
            } else {
                cur + 1
            }
        }
        GridDirection::Up => {
            if cur >= GRID_COLUMNS {
                cur - GRID_COLUMNS
            } else {
                let column = cur % GRID_COLUMNS;
                let rows = visible_count.div_ceil(GRID_COLUMNS);
                let mut candidate = (rows - 1) * GRID_COLUMNS + column;
                if candidate > last {
                    candidate = last;
                }
                candidate
            }
        }
        GridDirection::Down => {
            let candidate = cur + GRID_COLUMNS;
            if candidate <= last {
                candidate
            } else {
                cur % GRID_COLUMNS
            }
        }
    }
}
