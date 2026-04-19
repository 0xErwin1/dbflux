use std::fs;

const UI_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/ui");

fn read_ui_file(relative_path: &str) -> String {
    fs::read_to_string(format!("{UI_DIR}/{relative_path}"))
        .unwrap_or_else(|error| panic!("failed to read {relative_path}: {error}"))
}

fn read_ui_source(relative_path: &str) -> String {
    read_ui_file(relative_path)
        .split("#[cfg(test)]")
        .next()
        .unwrap_or_else(|| panic!("{relative_path} should contain production code before tests"))
        .to_string()
}

#[test]
fn ui_mod_wires_the_central_design_system_guardrail_module() {
    let source = read_ui_file("mod.rs");

    assert!(source.contains("mod design_system_guardrails;"));
}

#[test]
fn representative_overlays_reject_raw_scrim_regressions() {
    for relative_path in [
        "overlays/history_modal.rs",
        "overlays/command_palette.rs",
        "components/modal_frame.rs",
    ] {
        let source = read_ui_source(relative_path);

        assert!(
            !source.contains(".bg(gpui::black().opacity(0.5))"),
            "{relative_path} reintroduced a raw overlay scrim"
        );
    }
}

#[test]
fn settings_files_reject_the_removed_layout_section_header_helper() {
    for relative_path in [
        "windows/settings/about_section.rs",
        "windows/settings/audit_section.rs",
        "windows/settings/auth_profiles_section.rs",
        "windows/settings/drivers.rs",
        "windows/settings/general.rs",
        "windows/settings/hooks.rs",
        "windows/settings/keybindings.rs",
        "windows/settings/layout.rs",
        "windows/settings/mcp_section.rs",
        "windows/settings/proxies_section.rs",
        "windows/settings/rpc_services.rs",
        "windows/settings/ssh_tunnels_section.rs",
    ] {
        let source = read_ui_source(relative_path);

        assert!(
            !source.contains("layout::section_header(") && !source.contains("fn section_header("),
            "{relative_path} reintroduced the removed settings section-header helper"
        );
    }
}

#[test]
fn ui_keeps_shared_contract_helpers_out_of_feature_modules() {
    for (relative_path, forbidden_patterns) in [
        (
            "windows/settings/layout.rs",
            vec!["pub(super) fn section_header(", "fn section_header("],
        ),
        (
            "views/workspace/render.rs",
            vec![
                "fn background_tasks_panel_header(",
                "fn render_panel_header(",
                "fn panel_header_title(",
            ],
        ),
    ] {
        let source = read_ui_source(relative_path);

        for forbidden_pattern in forbidden_patterns {
            assert!(
                !source.contains(forbidden_pattern),
                "{relative_path} reintroduced shared contract helper `{forbidden_pattern}` outside dbflux_components"
            );
        }
    }
}
