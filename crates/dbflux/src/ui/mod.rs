pub mod command_palette;
pub mod components;
pub mod document;
pub mod dropdown;
pub mod editor;
pub mod history_modal;
pub mod icons;
pub mod results;
pub mod sidebar;
pub mod status_bar;
pub mod tasks_panel;
pub mod theme;
pub mod toast;
pub mod tokens;
pub mod windows;
pub mod workspace;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_modal_module_compiles() {
        let _ = history_modal::HistoryModal::new;
    }
}
