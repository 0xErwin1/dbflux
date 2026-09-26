use super::{SettingsCoordinator, SettingsFocus, SettingsSectionId};
use dbflux_components::components::tree_nav::{TreeNav, TreeNavNode};
use dbflux_components::icons::AppIcon;
use gpui::SharedString;
use std::collections::HashSet;

/// One navigation entry: tree id, label and icon.
type NavEntry = (&'static str, String, AppIcon);

/// One navigation group: tree id, label and its entries.
type NavGroup = (&'static str, String, Vec<NavEntry>);

impl SettingsCoordinator {
    /// Navigation tree of the settings window, keeping only the entries whose
    /// label, or whose group's label, contains `query` (case-insensitive), and
    /// the groups that still hold an entry. An empty query keeps every entry.
    pub(super) fn build_sidebar_tree(query: &str) -> TreeNav {
        let query = query.trim().to_lowercase();

        let groups: Vec<NavGroup> = vec![
            (
                "general-group",
                dbflux_i18n::t!("settings.nav.general_group"),
                vec![
                    (
                        "general",
                        dbflux_i18n::t!("settings.nav.general"),
                        AppIcon::Settings,
                    ),
                    (
                        "keybindings",
                        dbflux_i18n::t!("settings.nav.keybindings"),
                        AppIcon::Keyboard,
                    ),
                    (
                        "updates",
                        dbflux_i18n::t!("settings.nav.updates"),
                        AppIcon::Bell,
                    ),
                    (
                        "audit",
                        dbflux_i18n::t!("settings.nav.audit"),
                        AppIcon::FingerprintPattern,
                    ),
                    (
                        "about",
                        dbflux_i18n::t!("settings.nav.about"),
                        AppIcon::Info,
                    ),
                ],
            ),
            (
                "network",
                dbflux_i18n::t!("settings.nav.network"),
                vec![
                    (
                        "ssh-tunnels",
                        dbflux_i18n::t!("settings.nav.ssh_tunnels"),
                        AppIcon::Lock,
                    ),
                    (
                        "proxies",
                        dbflux_i18n::t!("settings.nav.proxies"),
                        AppIcon::Globe,
                    ),
                    (
                        "auth-profiles",
                        dbflux_i18n::t!("settings.nav.auth_profiles"),
                        AppIcon::KeyRound,
                    ),
                ],
            ),
            (
                "connection",
                dbflux_i18n::t!("settings.nav.connection"),
                vec![
                    (
                        "hooks",
                        dbflux_i18n::t!("settings.nav.hooks"),
                        AppIcon::SquareTerminal,
                    ),
                    (
                        "drivers",
                        dbflux_i18n::t!("settings.nav.drivers"),
                        AppIcon::Database,
                    ),
                    (
                        "services",
                        dbflux_i18n::t!("settings.nav.services"),
                        AppIcon::Plug,
                    ),
                ],
            ),
            #[cfg(feature = "mcp")]
            (
                "mcp-governance",
                dbflux_i18n::t!("settings.nav.mcp_governance"),
                vec![
                    (
                        "mcp-clients",
                        dbflux_i18n::t!("settings.nav.mcp_clients"),
                        AppIcon::Bot,
                    ),
                    (
                        "mcp-roles",
                        dbflux_i18n::t!("settings.nav.mcp_roles"),
                        AppIcon::Layers,
                    ),
                    (
                        "mcp-policies",
                        dbflux_i18n::t!("settings.nav.mcp_policies"),
                        AppIcon::Scale,
                    ),
                ],
            ),
        ];

        let mut expanded = HashSet::new();

        let nodes = groups
            .into_iter()
            .filter_map(|(group_id, group_label, leaves)| {
                let group_matches = group_label.to_lowercase().contains(&query);

                let leaves: Vec<TreeNavNode> = leaves
                    .into_iter()
                    .filter(|(_, label, _)| group_matches || label.to_lowercase().contains(&query))
                    .map(|(id, label, icon)| TreeNavNode::leaf(id, label, Some(icon)))
                    .collect();

                if leaves.is_empty() {
                    return None;
                }

                expanded.insert(SharedString::from(group_id));

                Some(TreeNavNode::group(group_id, group_label, None, leaves))
            })
            .collect();

        TreeNav::new(nodes, expanded)
    }

    pub(super) fn section_for_tree_id(id: &str) -> Option<SettingsSectionId> {
        match id {
            "general" => Some(SettingsSectionId::General),
            "audit" => Some(SettingsSectionId::Audit),
            #[cfg(feature = "mcp")]
            "mcp-clients" => Some(SettingsSectionId::McpClients),
            #[cfg(feature = "mcp")]
            "mcp-roles" => Some(SettingsSectionId::McpRoles),
            #[cfg(feature = "mcp")]
            "mcp-policies" => Some(SettingsSectionId::McpPolicies),
            "keybindings" => Some(SettingsSectionId::Keybindings),
            "updates" => Some(SettingsSectionId::Updates),
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
            SettingsSectionId::Audit => "audit",
            #[cfg(feature = "mcp")]
            SettingsSectionId::McpClients => "mcp-clients",
            #[cfg(feature = "mcp")]
            SettingsSectionId::McpRoles => "mcp-roles",
            #[cfg(feature = "mcp")]
            SettingsSectionId::McpPolicies => "mcp-policies",
            SettingsSectionId::Keybindings => "keybindings",
            SettingsSectionId::Updates => "updates",
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
            SettingsCoordinator::section_for_tree_id("audit"),
            Some(SettingsSectionId::Audit)
        );
        assert_eq!(
            SettingsCoordinator::section_for_tree_id("proxies"),
            Some(SettingsSectionId::Proxies)
        );
        #[cfg(feature = "mcp")]
        {
            assert_eq!(
                SettingsCoordinator::section_for_tree_id("mcp-clients"),
                Some(SettingsSectionId::McpClients)
            );
            assert_eq!(
                SettingsCoordinator::section_for_tree_id("mcp-policies"),
                Some(SettingsSectionId::McpPolicies)
            );
        }
    }

    #[test]
    fn section_for_tree_id_unknown_returns_none() {
        assert_eq!(
            SettingsCoordinator::section_for_tree_id("nonexistent"),
            None
        );
        assert_eq!(SettingsCoordinator::section_for_tree_id("mcp"), None);
    }

    #[test]
    fn tree_id_roundtrip_all_sections() {
        let mut sections = vec![
            SettingsSectionId::General,
            SettingsSectionId::Audit,
            SettingsSectionId::Keybindings,
            SettingsSectionId::Updates,
            SettingsSectionId::Proxies,
            SettingsSectionId::SshTunnels,
            SettingsSectionId::AuthProfiles,
            SettingsSectionId::Services,
            SettingsSectionId::Hooks,
            SettingsSectionId::Drivers,
            SettingsSectionId::About,
        ];

        #[cfg(feature = "mcp")]
        {
            sections.extend([
                SettingsSectionId::McpClients,
                SettingsSectionId::McpRoles,
                SettingsSectionId::McpPolicies,
            ]);
        }

        for section in sections {
            let id = SettingsCoordinator::tree_id_for_section(section);
            assert_eq!(SettingsCoordinator::section_for_tree_id(id), Some(section));
        }
    }

    #[test]
    fn services_tree_label_uses_neutral_rpc_wording() {
        let tree = SettingsCoordinator::build_sidebar_tree("");
        let services_row = tree
            .rows()
            .iter()
            .find(|row| row.id.as_ref() == "services")
            .expect("services row");

        assert_eq!(
            services_row.label.as_ref(),
            dbflux_i18n::t!("settings.nav.services")
        );
    }

    #[test]
    fn search_keeps_matching_entries_and_their_groups_only() {
        let tree = SettingsCoordinator::build_sidebar_tree("hook");
        let ids: Vec<&str> = tree.rows().iter().map(|row| row.id.as_ref()).collect();

        assert_eq!(ids, vec!["connection", "hooks"]);
    }

    #[test]
    fn search_is_case_insensitive_and_empty_query_keeps_everything() {
        let everything = SettingsCoordinator::build_sidebar_tree("");
        let upper = SettingsCoordinator::build_sidebar_tree("PROX");

        assert!(everything.rows().len() > upper.rows().len());
        assert!(upper.rows().iter().any(|row| row.id.as_ref() == "proxies"));
    }

    #[test]
    fn search_matching_a_group_keeps_all_of_its_entries() {
        let tree = SettingsCoordinator::build_sidebar_tree("network");
        let ids: Vec<&str> = tree.rows().iter().map(|row| row.id.as_ref()).collect();

        assert_eq!(
            ids,
            vec!["network", "ssh-tunnels", "proxies", "auth-profiles"]
        );
    }

    #[test]
    fn search_without_matches_yields_no_rows() {
        let tree = SettingsCoordinator::build_sidebar_tree("zzzz-no-such-setting");

        assert!(tree.rows().is_empty());
    }

    const NAV_CATALOG_KEYS: &[&str] = &[
        "settings.nav.general_group",
        "settings.nav.general",
        "settings.nav.keybindings",
        "settings.nav.updates",
        "settings.nav.audit",
        "settings.nav.about",
        "settings.nav.network",
        "settings.nav.ssh_tunnels",
        "settings.nav.proxies",
        "settings.nav.auth_profiles",
        "settings.nav.connection",
        "settings.nav.hooks",
        "settings.nav.drivers",
        "settings.nav.services",
        "settings.nav.mcp_governance",
        "settings.nav.mcp_clients",
        "settings.nav.mcp_roles",
        "settings.nav.mcp_policies",
    ];

    #[test]
    fn settings_nav_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in NAV_CATALOG_KEYS {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(
                    !value.is_empty(),
                    "key {key} resolved empty for locale {locale}"
                );
                assert_ne!(value, *key, "key {key} did not resolve for locale {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "key {key} fell back to the raw locale-qualified form for locale {locale}"
                );
            }
        }
    }

    #[test]
    fn settings_nav_keybindings_differs_between_locales() {
        let english = dbflux_i18n::t!("settings.nav.keybindings", locale = "en");
        let spanish = dbflux_i18n::t!("settings.nav.keybindings", locale = "es");

        assert_eq!(english, "Keybindings");
        assert_eq!(spanish, "Atajos de teclado");
        assert_ne!(english, spanish);
    }
}
