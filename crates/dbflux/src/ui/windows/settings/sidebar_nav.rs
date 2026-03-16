use super::{SettingsCoordinator, SettingsFocus, SettingsSectionId};
use crate::ui::components::tree_nav::{TreeNav, TreeNavNode};
use crate::ui::icons::AppIcon;
use gpui::SharedString;
use std::collections::HashSet;

impl SettingsCoordinator {
    #[allow(clippy::result_large_err)]
    pub(super) fn build_sidebar_tree() -> TreeNav {
        let nodes = vec![
            TreeNavNode::leaf("general", "General", Some(AppIcon::Settings)),
            TreeNavNode::leaf("keybindings", "Keybindings", Some(AppIcon::Keyboard)),
            TreeNavNode::group(
                "security",
                "Security",
                Some(AppIcon::Lock),
                vec![TreeNavNode::leaf(
                    "auth-profiles",
                    "Auth Profiles",
                    Some(AppIcon::KeyRound),
                )],
            ),
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

        let mut expanded = HashSet::new();
        expanded.insert(SharedString::from("security"));
        expanded.insert(SharedString::from("network"));
        expanded.insert(SharedString::from("connection"));

        TreeNav::new(nodes, expanded)
    }

    pub(super) fn section_for_tree_id(id: &str) -> Option<SettingsSectionId> {
        match id {
            "general" => Some(SettingsSectionId::General),
            "keybindings" => Some(SettingsSectionId::Keybindings),
            "proxies" => Some(SettingsSectionId::Proxies),
            "ssh-tunnels" => Some(SettingsSectionId::SshTunnels),
            "auth-profiles" => Some(SettingsSectionId::AuthProfiles),
            "services" => Some(SettingsSectionId::Services),
            "hooks" => Some(SettingsSectionId::Hooks),
            "drivers" => Some(SettingsSectionId::Drivers),
            "about" => Some(SettingsSectionId::About),
            _ => None,
        }
    }

    pub(super) fn tree_id_for_section(section: SettingsSectionId) -> &'static str {
        match section {
            SettingsSectionId::General => "general",
            SettingsSectionId::Keybindings => "keybindings",
            SettingsSectionId::Proxies => "proxies",
            SettingsSectionId::SshTunnels => "ssh-tunnels",
            SettingsSectionId::AuthProfiles => "auth-profiles",
            SettingsSectionId::Services => "services",
            SettingsSectionId::Hooks => "hooks",
            SettingsSectionId::Drivers => "drivers",
            SettingsSectionId::About => "about",
        }
    }

    #[allow(dead_code)]
    pub(super) fn focus_sidebar(&mut self) {
        self.focus_area = SettingsFocus::Sidebar;
        self.sidebar_tree
            .select_by_id(Self::tree_id_for_section(self.active_section));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn section_for_tree_id_known_ids() {
        assert_eq!(
            SettingsCoordinator::section_for_tree_id("general"),
            Some(SettingsSectionId::General)
        );
        assert_eq!(
            SettingsCoordinator::section_for_tree_id("proxies"),
            Some(SettingsSectionId::Proxies)
        );
    }

    #[test]
    fn section_for_tree_id_unknown_returns_none() {
        assert_eq!(
            SettingsCoordinator::section_for_tree_id("nonexistent"),
            None
        );
    }

    #[test]
    fn tree_id_roundtrip_all_sections() {
        for section in [
            SettingsSectionId::General,
            SettingsSectionId::Keybindings,
            SettingsSectionId::Proxies,
            SettingsSectionId::SshTunnels,
            SettingsSectionId::AuthProfiles,
            SettingsSectionId::Services,
            SettingsSectionId::Hooks,
            SettingsSectionId::Drivers,
            SettingsSectionId::About,
        ] {
            let id = SettingsCoordinator::tree_id_for_section(section);
            assert_eq!(SettingsCoordinator::section_for_tree_id(id), Some(section));
        }
    }
}
