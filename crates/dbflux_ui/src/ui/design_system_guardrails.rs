use std::fs;

const UI_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/ui");
const UI_DOCUMENT_SRC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../dbflux_ui_document/src");
const UI_WINDOWS_SRC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../dbflux_ui_windows/src");

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

fn read_document_source(relative_path: &str) -> String {
    fs::read_to_string(format!("{UI_DOCUMENT_SRC}/{relative_path}"))
        .unwrap_or_else(|error| panic!("failed to read document/{relative_path}: {error}"))
        .split("#[cfg(test)]")
        .next()
        .unwrap_or_else(|| {
            panic!("document/{relative_path} should contain production code before tests")
        })
        .to_string()
}

fn read_windows_source(relative_path: &str) -> String {
    fs::read_to_string(format!("{UI_WINDOWS_SRC}/{relative_path}"))
        .unwrap_or_else(|error| panic!("failed to read windows/{relative_path}: {error}"))
        .split("#[cfg(test)]")
        .next()
        .unwrap_or_else(|| {
            panic!("windows/{relative_path} should contain production code before tests")
        })
        .to_string()
}

#[test]
fn ui_mod_wires_the_central_design_system_guardrail_module() {
    let source = read_ui_file("mod.rs");

    assert!(source.contains("mod design_system_guardrails;"));
}

#[test]
fn representative_overlays_reject_raw_scrim_regressions() {
    // history_modal.rs now lives in dbflux_ui_document (moved in Step 3b)
    let history_source = read_document_source("history_modal.rs");
    assert!(
        !history_source.contains(".bg(gpui::black().opacity(0.5))"),
        "history_modal.rs reintroduced a raw overlay scrim"
    );

    for relative_path in ["overlays/command_palette.rs", "components/modal_frame.rs"] {
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
        "settings/about_section.rs",
        "settings/audit_section.rs",
        "settings/auth_profiles_section.rs",
        "settings/drivers.rs",
        "settings/general.rs",
        "settings/hooks.rs",
        "settings/keybindings.rs",
        "settings/layout.rs",
        "settings/mcp_section.rs",
        "settings/proxies_section.rs",
        "settings/rpc_services.rs",
        "settings/ssh_tunnels_section.rs",
    ] {
        let source = read_windows_source(relative_path);

        assert!(
            !source.contains("layout::section_header(") && !source.contains("fn section_header("),
            "{relative_path} reintroduced the removed settings section-header helper"
        );
    }
}

#[test]
fn ui_keeps_shared_contract_helpers_out_of_feature_modules() {
    let layout_source = read_windows_source("settings/layout.rs");
    for forbidden_pattern in ["pub(super) fn section_header(", "fn section_header("] {
        assert!(
            !layout_source.contains(forbidden_pattern),
            "settings/layout.rs reintroduced shared contract helper `{forbidden_pattern}` outside dbflux_components"
        );
    }

    let render_source = read_ui_source("views/workspace/render.rs");
    for forbidden_pattern in [
        "fn background_tasks_panel_header(",
        "fn render_panel_header(",
        "fn panel_header_title(",
    ] {
        assert!(
            !render_source.contains(forbidden_pattern),
            "views/workspace/render.rs reintroduced shared contract helper `{forbidden_pattern}` outside dbflux_components"
        );
    }
}
