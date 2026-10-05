use crate::tokens::{FormMetrics, SettingsMetrics};
use dbflux_components::composites::ListRow;
use dbflux_components::primitives::{FocusShape, Icon, Text, focus_ring};
use dbflux_components::tokens::{ChamferCut, Spacing};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;
use std::cell::Cell;
use std::rc::Rc;

pub(super) fn section_container(content: impl IntoElement) -> Div {
    div()
        .key_context(dbflux_components::key_contexts::SETTINGS_SECTION)
        .flex_1()
        .min_h_0()
        .flex()
        .flex_col()
        .overflow_hidden()
        .child(content)
}

/// Master-detail page: the page head over a line, then the master list and
/// the detail pane side by side.
pub(super) fn split_section_shell(
    line: Hsla,
    header: impl IntoElement,
    list: impl IntoElement,
    detail: impl IntoElement,
) -> Div {
    div()
        .size_full()
        .flex()
        .flex_col()
        .overflow_hidden()
        .child(
            div()
                .flex_shrink_0()
                .pb(SettingsMetrics::PAGE_HEAD_PADDING_BOTTOM - Spacing::SM)
                .border_b_1()
                .border_color(line)
                .child(header),
        )
        .child(
            div()
                .flex_1()
                .min_h_0()
                .flex()
                .overflow_hidden()
                .child(list)
                .child(detail),
        )
}

/// Single-form page: the page head, then the scrolling body padded to the
/// page margins.
pub(super) fn single_form_section_shell(header: impl IntoElement, body: impl IntoElement) -> Div {
    div()
        .size_full()
        .flex()
        .flex_col()
        .overflow_hidden()
        .child(header)
        .child(
            div()
                .flex_1()
                .min_h_0()
                .overflow_y_scrollbar()
                .px(SettingsMetrics::BODY_PADDING_X)
                .pb(Spacing::XL)
                .flex()
                .flex_col()
                .child(body),
        )
}

/// [`single_form_section_shell`] whose scrolling body follows `scroll`, so
/// the section can scroll a field into view. `viewport` receives the bounds
/// of the visible scrolling area, below the page head, every frame.
pub(super) fn scrolled_form_section_shell(
    header: impl IntoElement,
    body: impl IntoElement,
    scroll: &ScrollHandle,
    viewport: Rc<Cell<Bounds<Pixels>>>,
) -> Div {
    let viewport_recorder = canvas(move |bounds, _, _| viewport.set(bounds), |_, _, _, _| {})
        .absolute()
        .top_0()
        .left_0()
        .size_full();

    div()
        .size_full()
        .flex()
        .flex_col()
        .overflow_hidden()
        .child(header)
        .child(
            div()
                .debug_selector(|| "settings-form-viewport".to_string())
                .relative()
                .flex_1()
                .min_h_0()
                .child(viewport_recorder)
                .child(
                    div()
                        .id("settings-form-scroll")
                        .size_full()
                        .overflow_y_scroll()
                        .track_scroll(scroll)
                        .px(SettingsMetrics::BODY_PADDING_X)
                        .pb(Spacing::XL)
                        .flex()
                        .flex_col()
                        .child(body),
                )
                .vertical_scrollbar(scroll),
        )
}

/// Detail pane of a master-detail page: `header` (normally the first section
/// header) and the form body, scrolling together inside the page margins.
/// `footer` is drawn under the scrolling area, when present.
pub(super) fn sticky_form_shell(
    header: impl IntoElement,
    body: impl IntoElement,
    footer: Option<AnyElement>,
    _theme: &gpui_component::Theme,
) -> Div {
    div()
        .flex_1()
        .h_full()
        .min_w_0()
        .min_h_0()
        .flex()
        .flex_col()
        .overflow_hidden()
        .child(
            div()
                .flex_1()
                .min_h_0()
                .overflow_y_scrollbar()
                .px(SettingsMetrics::BODY_PADDING_X)
                .pt(SettingsMetrics::DETAIL_PADDING_TOP)
                .pb(Spacing::XL)
                .flex()
                .flex_col()
                .child(header)
                .child(body),
        )
        .when_some(footer, |shell, footer| {
            shell.child(
                div()
                    .flex_shrink_0()
                    .px(SettingsMetrics::BODY_PADDING_X)
                    .py(Spacing::MD)
                    .child(footer),
            )
        })
}

/// Form row of a settings page: the label in a fixed column on the left,
/// the control on the right and an optional muted helper line under it.
pub(super) fn form_row(
    label: impl Into<SharedString>,
    control: impl IntoElement,
    help: Option<SharedString>,
) -> Div {
    form_row_with_label_width(label, SettingsMetrics::FORM_LABEL_WIDTH, control, help)
}

/// [`form_row`] with a custom label column width.
pub(crate) fn form_row_with_label_width(
    label: impl Into<SharedString>,
    label_width: impl Into<AbsoluteLength>,
    control: impl IntoElement,
    help: Option<SharedString>,
) -> Div {
    div()
        .flex()
        .items_start()
        .gap(FormMetrics::ROW_GAP)
        .py(FormMetrics::ROW_PADDING_Y)
        .child(
            div()
                .w(label_width.into())
                .flex_shrink_0()
                .pt(FormMetrics::LABEL_PADDING_TOP)
                .child(Text::body(label)),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .flex()
                .flex_col()
                .gap(FormMetrics::HELP_GAP)
                .child(control)
                .when_some(help, |column, help| column.child(help_text(help))),
        )
}

/// Muted helper line under a control.
pub(crate) fn help_text(text: impl Into<SharedString>) -> Text {
    Text::body(text)
        .font_size(FormMetrics::HELP_FONT)
        .muted_foreground()
}

/// Checkbox row: the checkbox and its label, with an optional muted
/// description aligned under the label.
pub(crate) fn check_row(checkbox: impl IntoElement, description: Option<SharedString>) -> Div {
    div()
        .flex()
        .flex_col()
        .text_size(dbflux_components::tokens::FontSizes::BASE)
        .gap(FormMetrics::CHECK_ROW_LINE_GAP)
        .py(FormMetrics::CHECK_ROW_PADDING_Y)
        .child(checkbox)
        .when_some(description, |row, description| {
            row.child(
                div()
                    .flex()
                    .gap(dbflux_components::tokens::Fields::CHECKBOX_GAP)
                    .child(
                        div()
                            .flex_shrink_0()
                            .w(dbflux_components::tokens::Fields::CHECKBOX_SIZE),
                    )
                    .child(div().flex_1().min_w_0().child(help_text(description))),
            )
        })
}

/// Keyboard cursor ring around a form control or row that has no focus of
/// its own (the settings pages keep a virtual cursor).
pub(crate) fn cursor_ring(focused: bool, child: impl IntoElement, cx: &App) -> Div {
    focus_ring(
        focused,
        FocusShape::Chamfer(ChamferCut::CONTROL),
        None,
        child,
        cx,
    )
}

/// Frame of a text field in a form row: gives the field `width` when set,
/// capped at the row's width (otherwise the field fills the row), sets the
/// mono face for technical values, and draws the keyboard cursor ring while
/// `cursor` is set.
pub(crate) fn field_frame(
    cursor: bool,
    width: Option<Rems>,
    mono: bool,
    field: impl IntoElement,
    cx: &App,
) -> Div {
    let frame = cursor_ring(cursor, field, cx);

    // A fixed width never pushes past the row: narrow windows shrink the
    // field to the space left instead of clipping it.
    let frame = match width {
        Some(width) => frame.w(width).max_w_full().min_w_0(),
        None => frame.flex_1().min_w_0(),
    };

    frame.when(mono, |frame| {
        frame.font_family(dbflux_components::fonts::editor_family(cx))
    })
}

/// Controls placed side by side in one form row (host and port, a field
/// and its Browse button).
pub(crate) fn inline_controls() -> Div {
    div().flex().items_center().gap(FormMetrics::INLINE_GAP)
}

/// Toolbar at the top of a master list (New ..., Import).
pub(super) fn master_list_toolbar(children: Vec<AnyElement>) -> Div {
    div()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(FormMetrics::INLINE_GAP)
        .p(SettingsMetrics::LIST_TOOLBAR_PADDING)
        .children(children)
}

/// Master list column of a master-detail page: a fixed-width column with a
/// line on its right edge, the toolbar on top and the scrolling rows.
pub(super) fn master_list_panel(
    id: impl Into<ElementId>,
    toolbar: impl IntoElement,
    rows: impl IntoElement,
    cx: &App,
) -> Div {
    div()
        .w(SettingsMetrics::LIST_WIDTH)
        .h_full()
        .flex_shrink_0()
        .flex()
        .flex_col()
        .border_r_1()
        .border_color(cx.theme().border)
        .child(toolbar)
        .child(
            div()
                .id(id.into())
                .flex_1()
                .min_h_0()
                .overflow_y_scroll()
                .flex()
                .flex_col()
                .child(rows),
        )
}

/// Content of a master list row: the icon and the name on the first line,
/// an optional trailing element (a badge) on its right, and an optional mono
/// detail line under it.
pub(super) struct MasterRow {
    pub icon: Option<dbflux_components::icons::AppIcon>,
    pub title: SharedString,
    pub detail: Option<SharedString>,
    pub trailing: Option<AnyElement>,
}

/// A master list row built on `ListRow`: selected rows get the tint wash and
/// the left bar; the keyboard cursor draws the focus ring on any other row.
pub(super) fn master_list_row(
    id: impl Into<ElementId>,
    row: MasterRow,
    selected: bool,
    focused: bool,
    cx: &App,
) -> Stateful<Div> {
    let theme = cx.theme();
    let icon_color = if selected {
        dbflux_components::tokens::ChromeColors::tint(theme)
    } else {
        theme.muted_foreground
    };

    ListRow::new(id)
        .selected(selected)
        .selection_bar(true)
        .focused(focused && !selected)
        .build(cx)
        .flex()
        .flex_col()
        .gap(SettingsMetrics::LIST_ROW_LINE_GAP)
        .py(SettingsMetrics::LIST_ROW_PADDING_Y)
        .px(SettingsMetrics::LIST_ROW_PADDING_X)
        .border_b_1()
        .border_color(theme.table_row_border)
        .child(
            div()
                .flex()
                .items_center()
                .gap(FormMetrics::INLINE_GAP)
                .when_some(row.icon, |line, icon| {
                    line.child(
                        Icon::new(icon)
                            .size(SettingsMetrics::LIST_ROW_ICON)
                            .color(icon_color),
                    )
                })
                .child(
                    div()
                        .flex_1()
                        .min_w_0()
                        .overflow_hidden()
                        .whitespace_nowrap()
                        .text_ellipsis()
                        .child(
                            Text::body(row.title)
                                .font_weight(FontWeight::SEMIBOLD)
                                .text_color(dbflux_components::tokens::ChromeColors::strong(theme)),
                        ),
                )
                .when_some(row.trailing, |line, trailing| line.child(trailing)),
        )
        .when_some(row.detail, |column, detail| {
            column.child(
                div()
                    .overflow_hidden()
                    .whitespace_nowrap()
                    .text_ellipsis()
                    .child(
                        Text::code(detail)
                            .font_size(SettingsMetrics::LIST_ROW_META_FONT)
                            .muted_foreground(),
                    ),
            )
        })
}

/// Muted sentence shown in a master list that has no rows yet.
pub(super) fn master_list_empty(text: impl Into<SharedString>) -> Div {
    div()
        .px(SettingsMetrics::LIST_ROW_PADDING_X)
        .py(SettingsMetrics::LIST_ROW_PADDING_Y)
        .child(Text::body(text).muted_foreground())
}

#[cfg(test)]
mod tests {
    use std::fs;

    const SETTINGS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/settings");

    fn read_settings_file(name: &str) -> String {
        fs::read_to_string(format!("{SETTINGS_DIR}/{name}"))
            .unwrap_or_else(|error| panic!("failed to read {name}: {error}"))
    }

    fn representative_settings_section_sources() -> Vec<(&'static str, String)> {
        [
            "about_section.rs",
            "audit_section.rs",
            "general.rs",
            "hooks.rs",
            "mcp_section.rs",
            "rpc_services.rs",
        ]
        .into_iter()
        .map(|file_name| (file_name, read_settings_file(file_name)))
        .collect()
    }

    #[test]
    fn settings_layout_keeps_section_container_and_editor_helpers_only() {
        let source = read_settings_file("layout.rs");
        let production_source = source
            .split("#[cfg(test)]")
            .next()
            .expect("layout.rs should contain production code before tests");

        assert!(production_source.contains("pub(super) fn section_container("));
        assert!(production_source.contains("pub(super) fn split_section_shell("));
        assert!(production_source.contains("pub(super) fn single_form_section_shell("));
        assert!(production_source.contains("pub(super) fn sticky_form_shell("));
        assert!(production_source.contains("pub(super) fn form_row("));
        assert!(production_source.contains("pub(super) fn master_list_panel("));
    }

    #[test]
    fn settings_layout_no_longer_defines_a_section_header_shim() {
        let source = read_settings_file("layout.rs");
        let production_source = source
            .split("#[cfg(test)]")
            .next()
            .expect("layout.rs should contain production code before tests");

        assert!(!production_source.contains("pub(super) fn section_header("));
    }

    #[test]
    fn settings_sections_stop_passing_theme_into_local_section_header_helper() {
        for file_name in [
            "about_section.rs",
            "audit_section.rs",
            "auth_profiles_section.rs",
            "drivers.rs",
            "general.rs",
            "hooks.rs",
            "keybindings.rs",
            "mcp_section.rs",
            "proxies_section.rs",
            "rpc_services.rs",
            "ssh_tunnels_section.rs",
        ] {
            let source = read_settings_file(file_name);

            assert!(
                !source.contains("layout::section_header(")
                    || !source.contains(",\n                theme,")
                        && !source.contains(",\n                    theme,")
                        && !source.contains(",\n                &theme,")
                        && !source.contains(",\n                    &theme,"),
                "{file_name} still passes theme into layout::section_header"
            );
        }
    }

    #[test]
    fn settings_sections_stop_calling_layout_section_header() {
        for file_name in [
            "about_section.rs",
            "audit_section.rs",
            "auth_profiles_section.rs",
            "drivers.rs",
            "general.rs",
            "hooks.rs",
            "keybindings.rs",
            "mcp_section.rs",
            "proxies_section.rs",
            "rpc_services.rs",
            "ssh_tunnels_section.rs",
        ] {
            let source = read_settings_file(file_name);

            assert!(
                !source.contains("layout::section_header("),
                "{file_name} still calls layout::section_header"
            );
        }
    }

    #[test]
    fn representative_settings_sections_call_the_canonical_section_header_directly() {
        for (file_name, source) in representative_settings_section_sources() {
            assert!(
                source.contains("dbflux_components::composites::page_header("),
                "{file_name} should call the canonical section_header"
            );
            assert!(
                !source.contains("layout::section_header("),
                "{file_name} should not call the removed layout::section_header helper"
            );
        }
    }

    #[test]
    fn representative_settings_sections_keep_header_chrome_out_of_layout_rs() {
        let layout_source = read_settings_file("layout.rs");
        let production_source = layout_source
            .split("#[cfg(test)]")
            .next()
            .expect("layout.rs should contain production code before tests");

        assert!(!production_source.contains("section_header("));
        assert!(!production_source.contains("SectionHeaderVariant"));
    }
}
