use std::fs;
use std::path::{Path, PathBuf};

fn collect_rust_files(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();

        if path.is_dir() {
            collect_rust_files(&path, out);
            continue;
        }

        if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

#[test]
fn ui_does_not_compare_driver_id_directly() {
    let ui_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/ui");
    let mut files = Vec::new();
    collect_rust_files(&ui_root, &mut files);

    let mut violations = Vec::new();

    for file in files {
        let Ok(content) = fs::read_to_string(&file) else {
            continue;
        };

        if content.contains("metadata().id ==") {
            violations.push(file);
        }
    }

    assert!(
        violations.is_empty(),
        "Found direct driver-id comparisons in UI: {:?}",
        violations
    );
}

#[test]
fn ui_does_not_branch_on_db_kind() {
    let ui_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/ui");
    let mut files = Vec::new();
    collect_rust_files(&ui_root, &mut files);

    let mut violations = Vec::new();

    for file in files {
        let Ok(content) = fs::read_to_string(&file) else {
            continue;
        };

        if content.contains("DbKind::") {
            violations.push(file);
        }
    }

    assert!(
        violations.is_empty(),
        "Found DbKind-specific branching in UI: {:?}",
        violations
    );
}

#[test]
fn postgres_config_pattern_is_confined_to_core_and_driver() {
    let workspace = workspace_root();
    let crates_root = workspace.join("crates");
    let postgres_pattern = "DbConfig::".to_string() + "Postgres";

    let mut files = Vec::new();
    collect_rust_files(&crates_root, &mut files);

    let allowed_core = workspace.join("crates/dbflux_core");
    let allowed_postgres_driver = workspace.join("crates/dbflux_driver_postgres");
    let allowed_connection_form =
        workspace.join("crates/dbflux/src/ui/windows/connection_manager/form.rs");
    let allowed_test_support = workspace.join("crates/dbflux_test_support");

    let mut violations = Vec::new();

    for file in files {
        let Ok(content) = fs::read_to_string(&file) else {
            continue;
        };

        if !content.contains(&postgres_pattern) {
            continue;
        }

        let allowed = file.starts_with(&allowed_core)
            || file.starts_with(&allowed_postgres_driver)
            || file.starts_with(&allowed_test_support)
            || file == allowed_connection_form;

        if !allowed {
            violations.push(file);
        }
    }

    assert!(
        violations.is_empty(),
        "Found postgres config variant outside allowed modules: {:?}",
        violations
    );
}
