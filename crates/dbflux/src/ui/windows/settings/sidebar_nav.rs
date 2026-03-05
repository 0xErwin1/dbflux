use super::{SettingsFocus, SettingsSection, SettingsWindow};
use crate::ui::components::tree_nav::{TreeNav, TreeNavNode};
use crate::ui::icons::AppIcon;
use dbflux_core::{UiState, UiStateStore};
use gpui::SharedString;
use std::collections::HashSet;

impl SettingsWindow {
    pub(super) fn build_sidebar_tree() -> TreeNav {
        let nodes = vec![
            TreeNavNode::leaf("general", "General", Some(AppIcon::Settings)),
            TreeNavNode::leaf("keybindings", "Keybindings", Some(AppIcon::Keyboard)),
            TreeNavNode::group(
                "network",
                "Network",
                Some(AppIcon::Server),
                vec![
                    TreeNavNode::leaf("proxies", "Proxy", Some(AppIcon::Server)),
                    TreeNavNode::leaf(
                        "ssh-tunnels",
                        "SSH Tunnels",
                        Some(AppIcon::FingerprintPattern),
                    ),
                ],
            ),
            TreeNavNode::group(
                "connection",
                "Connection",
                Some(AppIcon::Link2),
                vec![
                    TreeNavNode::leaf("services", "Services", Some(AppIcon::Plug)),
                    TreeNavNode::leaf("hooks", "Hooks", Some(AppIcon::SquareTerminal)),
                    TreeNavNode::leaf("drivers", "Drivers", Some(AppIcon::Database)),
                ],
            ),
            TreeNavNode::leaf("about", "About", Some(AppIcon::Info)),
        ];

        let ui_state = UiStateStore::new()
            .and_then(|s| s.load())
            .unwrap_or_default();

        let mut expanded = HashSet::new();
        if !ui_state.settings_collapsed_network {
            expanded.insert(SharedString::from("network"));
        }
        if !ui_state.settings_collapsed_connection {
            expanded.insert(SharedString::from("connection"));
        }

        TreeNav::new(nodes, expanded)
    }

    pub(super) fn section_for_tree_id(id: &str) -> Option<SettingsSection> {
        match id {
            "general" => Some(SettingsSection::General),
            "keybindings" => Some(SettingsSection::Keybindings),
            "proxies" => Some(SettingsSection::Proxies),
            "ssh-tunnels" => Some(SettingsSection::SshTunnels),
            "services" => Some(SettingsSection::Services),
            "hooks" => Some(SettingsSection::Hooks),
            "drivers" => Some(SettingsSection::Drivers),
            "about" => Some(SettingsSection::About),
            _ => None,
        }
    }

    pub(super) fn tree_id_for_section(section: SettingsSection) -> &'static str {
        match section {
            SettingsSection::General => "general",
            SettingsSection::Keybindings => "keybindings",
            SettingsSection::Proxies => "proxies",
            SettingsSection::SshTunnels => "ssh-tunnels",
            SettingsSection::Services => "services",
            SettingsSection::Hooks => "hooks",
            SettingsSection::Drivers => "drivers",
            SettingsSection::About => "about",
        }
    }

    pub(super) fn focus_sidebar(&mut self) {
        self.focus_area = SettingsFocus::Sidebar;
        let id = Self::tree_id_for_section(self.active_section);
        self.sidebar_tree.select_by_id(id);
    }

    pub(super) fn sync_active_section_from_cursor(&mut self) {
        if let Some(row) = self.sidebar_tree.cursor_item()
            && let Some(section) = Self::section_for_tree_id(row.id.as_ref())
        {
            self.active_section = section;
        }
    }

    pub(super) fn persist_collapse_state(&self) {
        let expanded = self.sidebar_tree.expanded();

        let state = UiState {
            settings_collapsed_network: !expanded.contains("network"),
            settings_collapsed_connection: !expanded.contains("connection"),
        };

        let store = match UiStateStore::new() {
            Ok(s) => s,
            Err(e) => {
                log::error!("Failed to open UI state store: {}", e);
                return;
            }
        };

        if let Err(e) = store.save(&state) {
            log::error!("Failed to persist collapse state: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn section_for_tree_id_known_ids() {
        assert_eq!(
            SettingsWindow::section_for_tree_id("general"),
            Some(SettingsSection::General)
        );
        assert_eq!(
            SettingsWindow::section_for_tree_id("proxies"),
            Some(SettingsSection::Proxies)
        );
    }

    #[test]
    fn section_for_tree_id_unknown_returns_none() {
        assert_eq!(SettingsWindow::section_for_tree_id("nonexistent"), None);
    }

    #[test]
    fn tree_id_roundtrip_all_sections() {
        for section in [
            SettingsSection::General,
            SettingsSection::Keybindings,
            SettingsSection::Proxies,
            SettingsSection::SshTunnels,
            SettingsSection::Services,
            SettingsSection::Hooks,
            SettingsSection::Drivers,
            SettingsSection::About,
        ] {
            let id = SettingsWindow::tree_id_for_section(section);
            assert_eq!(SettingsWindow::section_for_tree_id(id), Some(section));
        }
    }
}
