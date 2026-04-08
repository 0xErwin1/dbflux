use std::fs;
use std::path::PathBuf;

fn read_workspace_file(relative_path: &str) -> String {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    fs::read_to_string(root.join(relative_path)).expect("file should be readable")
}

#[test]
fn access_manager_declares_all_access_modes_without_legacy_wording() {
    let access_manager = read_workspace_file("../dbflux_app/src/access_manager.rs");

    assert!(access_manager.contains("AccessKind::Direct"));
    assert!(access_manager.contains("AccessKind::Ssh"));
    assert!(access_manager.contains("AccessKind::Proxy"));
    assert!(access_manager.contains("AccessKind::Managed"));

    assert!(
        access_manager.contains("SSH tunnel profile '") && access_manager.contains("was not found"),
        "ssh failures should explain missing tunnel profile resolution"
    );
    assert!(
        access_manager.contains("Proxy profile '") && access_manager.contains("was not found"),
        "proxy failures should explain missing proxy profile resolution"
    );
    assert!(
        !access_manager.contains("legacy connect path"),
        "legacy wording should not remain in the app access manager"
    );
    assert!(
        access_manager.contains("Unknown managed access provider"),
        "unknown managed provider failures should remain explicit"
    );
}

#[test]
fn pipeline_labels_cover_all_access_modes_for_parity_evidence() {
    let pipeline = read_workspace_file("../dbflux_core/src/pipeline/mod.rs");

    assert!(pipeline.contains("AccessKind::Direct => \"Direct\""));
    assert!(pipeline.contains("AccessKind::Ssh { .. } => \"SSH tunnel\""));
    assert!(pipeline.contains("AccessKind::Proxy { .. } => \"Proxy\""));
    assert!(pipeline.contains("AccessKind::Managed { provider, params }"));
}
