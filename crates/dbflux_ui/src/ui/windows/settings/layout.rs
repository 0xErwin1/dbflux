use gpui::prelude::*;
use gpui::*;
use gpui_component::scroll::ScrollableElement;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StickyFooterLayout {
    FullWidth,
}

fn sticky_footer_layout() -> StickyFooterLayout {
    StickyFooterLayout::FullWidth
}

pub(super) fn compact_input_shell(child: impl IntoElement) -> Div {
    div().w_full().child(child)
}

pub(super) fn editor_panel_title(noun: &str, is_editing: bool) -> String {
    let prefix = if is_editing { "Edit" } else { "New" };

    format!("{} {}", prefix, noun)
}

pub(super) fn section_container(content: impl IntoElement) -> Div {
    div()
        .flex_1()
        .min_h_0()
        .flex()
        .flex_col()
        .overflow_hidden()
        .child(content)
}

pub(super) fn sticky_form_shell(
    header: impl IntoElement,
    body: impl IntoElement,
    footer: impl IntoElement,
    theme: &gpui_component::Theme,
) -> Div {
    div()
        .flex_1()
        .h_full()
        .min_h_0()
        .flex()
        .flex_col()
        .overflow_hidden()
        .child(
            div()
                .p_4()
                .border_b_1()
                .border_color(theme.border)
                .child(header),
        )
        .child(
            div()
                .flex_1()
                .min_h_0()
                .overflow_y_scrollbar()
                .p_4()
                .flex()
                .flex_col()
                .gap_5()
                .child(body),
        )
        .child(div().p_4().border_t_1().border_color(theme.border).child(
            match sticky_footer_layout() {
                StickyFooterLayout::FullWidth => div().w_full().child(footer),
            },
        ))
}

#[cfg(test)]
mod tests {
    use super::{
        compact_input_shell, editor_panel_title, sticky_footer_layout, StickyFooterLayout,
    };
    use gpui::div;
    use std::fs;

    const SETTINGS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/ui/windows/settings");

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
    fn editor_panel_title_uses_new_prefix_when_creating() {
        assert_eq!(editor_panel_title("Proxy", false), "New Proxy");
        assert_eq!(
            editor_panel_title("Auth Profile", false),
            "New Auth Profile"
        );
    }

    #[test]
    fn editor_panel_title_uses_edit_prefix_when_updating() {
        assert_eq!(editor_panel_title("Proxy", true), "Edit Proxy");
        assert_eq!(editor_panel_title("SSH Tunnel", true), "Edit SSH Tunnel");
    }

    #[test]
    fn sticky_form_footer_preserves_full_width_layout() {
        assert_eq!(sticky_footer_layout(), StickyFooterLayout::FullWidth);
    }

    #[test]
    fn compact_settings_inputs_skip_standard_control_shell() {
        let _ = compact_input_shell(div());
    }

    #[test]
    fn settings_layout_keeps_section_container_and_editor_helpers_only() {
        let source = read_settings_file("layout.rs");
        let production_source = source
            .split("#[cfg(test)]")
            .next()
            .expect("layout.rs should contain production code before tests");

        assert!(production_source.contains("pub(super) fn compact_input_shell("));
        assert!(production_source.contains("pub(super) fn editor_panel_title("));
        assert!(production_source.contains("pub(super) fn section_container("));
        assert!(production_source.contains("pub(super) fn sticky_form_shell("));
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
                source.contains("dbflux_components::composites::section_header("),
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
