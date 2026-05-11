pub mod context_menu;
pub mod data_table;
pub mod document_tree;
pub mod dropdown;
pub mod filter_bar;
pub mod form_navigation;
pub mod form_renderer;
pub mod json_editor_view;
pub mod modal_frame;
pub mod multi_select;
pub mod toast;
pub mod tree_nav;
pub mod typography;
pub mod value_source_selector;

#[cfg(test)]
mod tests {
    use std::fs;

    const COMPONENTS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/ui/components");

    #[test]
    fn legacy_surface_modules_are_not_exported() {
        let source = fs::read_to_string(format!("{COMPONENTS_DIR}/mod.rs"))
            .unwrap_or_else(|error| panic!("failed to read components/mod.rs: {error}"));
        let production_source = source
            .split("#[cfg(test)]")
            .next()
            .expect("components/mod.rs should contain production code before tests");

        assert!(!production_source.contains("pub mod surfaces;"));
        assert!(!production_source.contains("pub mod surfaces_style;"));
    }

    #[test]
    fn legacy_surface_source_files_are_removed() {
        assert!(!std::path::Path::new(&format!("{COMPONENTS_DIR}/surfaces.rs")).exists());
        assert!(!std::path::Path::new(&format!("{COMPONENTS_DIR}/surfaces_style.rs")).exists());
    }
}
