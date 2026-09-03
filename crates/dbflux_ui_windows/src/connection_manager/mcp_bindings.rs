//! Pure, entity-free helpers backing the Connection Manager MCP tab's
//! master-detail client list. Kept free of GPUI types so the binding-update
//! and filtering rules can be unit tested without a window/context.

use dbflux_core::ConnectionMcpPolicyBinding;

/// Ensures a `ConnectionMcpPolicyBinding` for `actor_id` exists in `bindings`
/// when `present` is true, and removes it otherwise. Idempotent either way.
///
/// Only reachable through the MCP tab's "allow this client" checkbox, which
/// only exists when the `mcp` feature is enabled.
#[cfg(feature = "mcp")]
pub fn set_binding_presence(
    bindings: &mut Vec<ConnectionMcpPolicyBinding>,
    actor_id: &str,
    present: bool,
) {
    let exists = bindings.iter().any(|binding| binding.actor_id == actor_id);

    if present && !exists {
        bindings.push(ConnectionMcpPolicyBinding {
            actor_id: actor_id.to_string(),
            role_ids: Vec::new(),
            policy_ids: Vec::new(),
        });
    } else if !present && exists {
        bindings.retain(|binding| binding.actor_id != actor_id);
    }
}

/// Writes deduped role/policy ids into the binding for `actor_id`. No-op
/// when that actor has no binding in `bindings`.
pub fn apply_selection(
    bindings: &mut [ConnectionMcpPolicyBinding],
    actor_id: &str,
    role_ids: Vec<String>,
    policy_ids: Vec<String>,
) {
    if let Some(binding) = bindings
        .iter_mut()
        .find(|binding| binding.actor_id == actor_id)
    {
        binding.role_ids = dedup_preserve_order(role_ids);
        binding.policy_ids = dedup_preserve_order(policy_ids);
    }
}

/// Merges a primary dropdown selection with a multi-select's extra values
/// into one deduped list, primary first. An empty primary is skipped.
pub fn merge_primary_and_extras(primary: Option<String>, extras: Vec<String>) -> Vec<String> {
    let mut merged = Vec::new();

    if let Some(primary) = primary
        && !primary.is_empty()
    {
        merged.push(primary);
    }

    merged.extend(extras);
    dedup_preserve_order(merged)
}

fn dedup_preserve_order(values: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

/// Counts bindings whose `actor_id` is not present in `known_actor_ids`,
/// i.e. bindings that reference a trusted client that no longer exists.
///
/// Only reachable from the MCP tab's trusted-client list, which only exists
/// when the `mcp` feature is enabled.
#[cfg(feature = "mcp")]
pub fn orphan_binding_count(
    bindings: &[ConnectionMcpPolicyBinding],
    known_actor_ids: &[String],
) -> usize {
    bindings
        .iter()
        .filter(|binding| !known_actor_ids.contains(&binding.actor_id))
        .count()
}

#[cfg(feature = "mcp")]
mod mcp_feature {
    use super::ConnectionMcpPolicyBinding;
    use dbflux_mcp::{PolicyRoleDto, ToolPolicyDto, TrustedClientDto};

    /// The union of tools and policy classes a binding effectively grants,
    /// mirroring the union `PolicyEngine::evaluate` computes from direct
    /// policies plus each assigned role's policies.
    #[derive(Debug, Clone, PartialEq, Eq, Default)]
    pub struct EffectivePermissions {
        pub tools: Vec<String>,
        pub classes: Vec<String>,
    }

    /// Filters `clients` by a case-insensitive substring match on name or id.
    /// An empty query returns every client.
    pub fn filter_clients<'a>(
        clients: &'a [TrustedClientDto],
        query: &str,
    ) -> Vec<&'a TrustedClientDto> {
        let query = query.trim().to_lowercase();

        if query.is_empty() {
            return clients.iter().collect();
        }

        clients
            .iter()
            .filter(|client| {
                client.name.to_lowercase().contains(&query)
                    || client.id.to_lowercase().contains(&query)
            })
            .collect()
    }

    /// Computes the sorted, deduped union of tools and classes that
    /// `binding` grants, resolving its direct policies plus the policies of
    /// every assigned role. Unknown role/policy ids are skipped.
    pub fn effective_permissions(
        binding: &ConnectionMcpPolicyBinding,
        roles: &[PolicyRoleDto],
        policies: &[ToolPolicyDto],
    ) -> EffectivePermissions {
        let mut policy_ids: Vec<&str> = binding.policy_ids.iter().map(String::as_str).collect();

        for role_id in &binding.role_ids {
            if let Some(role) = roles.iter().find(|role| &role.id == role_id) {
                policy_ids.extend(role.policy_ids.iter().map(String::as_str));
            }
        }

        let mut tools = std::collections::BTreeSet::new();
        let mut classes = std::collections::BTreeSet::new();

        for policy_id in policy_ids {
            let Some(policy) = policies.iter().find(|policy| policy.id == policy_id) else {
                continue;
            };

            tools.extend(policy.allowed_tools.iter().cloned());
            classes.extend(policy.allowed_classes.iter().cloned());
        }

        EffectivePermissions {
            tools: tools.into_iter().collect(),
            classes: classes.into_iter().collect(),
        }
    }
}

#[cfg(feature = "mcp")]
pub use mcp_feature::{effective_permissions, filter_clients};

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(actor_id: &str) -> ConnectionMcpPolicyBinding {
        ConnectionMcpPolicyBinding {
            actor_id: actor_id.to_string(),
            role_ids: Vec::new(),
            policy_ids: Vec::new(),
        }
    }

    #[cfg(feature = "mcp")]
    #[test]
    fn set_binding_presence_adds_missing_binding() {
        let mut bindings = Vec::new();

        set_binding_presence(&mut bindings, "agent-1", true);

        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].actor_id, "agent-1");
        assert!(bindings[0].role_ids.is_empty());
        assert!(bindings[0].policy_ids.is_empty());
    }

    #[cfg(feature = "mcp")]
    #[test]
    fn set_binding_presence_is_idempotent_when_adding_twice() {
        let mut bindings = Vec::new();

        set_binding_presence(&mut bindings, "agent-1", true);
        set_binding_presence(&mut bindings, "agent-1", true);

        assert_eq!(bindings.len(), 1);
    }

    #[cfg(feature = "mcp")]
    #[test]
    fn set_binding_presence_removes_existing_binding() {
        let mut bindings = vec![binding("agent-1"), binding("agent-2")];

        set_binding_presence(&mut bindings, "agent-1", false);

        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].actor_id, "agent-2");
    }

    #[cfg(feature = "mcp")]
    #[test]
    fn set_binding_presence_removal_is_idempotent() {
        let mut bindings = vec![binding("agent-1")];

        set_binding_presence(&mut bindings, "agent-1", false);
        set_binding_presence(&mut bindings, "agent-1", false);

        assert!(bindings.is_empty());
    }

    #[test]
    fn apply_selection_writes_deduped_ids_into_matching_binding() {
        let mut bindings = vec![binding("agent-1")];

        apply_selection(
            &mut bindings,
            "agent-1",
            vec!["reader".to_string(), "reader".to_string()],
            vec!["strict".to_string()],
        );

        assert_eq!(bindings[0].role_ids, vec!["reader".to_string()]);
        assert_eq!(bindings[0].policy_ids, vec!["strict".to_string()]);
    }

    #[test]
    fn apply_selection_is_a_no_op_when_actor_has_no_binding() {
        let mut bindings = vec![binding("agent-1")];

        apply_selection(
            &mut bindings,
            "agent-2",
            vec!["reader".to_string()],
            vec!["strict".to_string()],
        );

        assert!(bindings[0].role_ids.is_empty());
    }

    #[test]
    fn merge_primary_and_extras_puts_primary_first() {
        let merged = merge_primary_and_extras(
            Some("reader".to_string()),
            vec!["writer".to_string(), "admin".to_string()],
        );

        assert_eq!(merged, vec!["reader", "writer", "admin"]);
    }

    #[test]
    fn merge_primary_and_extras_skips_empty_primary() {
        let merged = merge_primary_and_extras(Some(String::new()), vec!["writer".to_string()]);

        assert_eq!(merged, vec!["writer"]);
    }

    #[test]
    fn merge_primary_and_extras_skips_none_primary() {
        let merged = merge_primary_and_extras(None, vec!["writer".to_string()]);

        assert_eq!(merged, vec!["writer"]);
    }

    #[test]
    fn merge_primary_and_extras_dedups_extras_against_primary() {
        let merged = merge_primary_and_extras(
            Some("reader".to_string()),
            vec!["reader".to_string(), "writer".to_string()],
        );

        assert_eq!(merged, vec!["reader", "writer"]);
    }

    #[cfg(feature = "mcp")]
    #[test]
    fn orphan_binding_count_counts_bindings_with_unknown_actor_ids() {
        let bindings = vec![binding("agent-1"), binding("agent-2"), binding("agent-3")];
        let known = vec!["agent-1".to_string(), "agent-3".to_string()];

        assert_eq!(orphan_binding_count(&bindings, &known), 1);
    }

    #[cfg(feature = "mcp")]
    #[test]
    fn orphan_binding_count_is_zero_when_every_binding_is_known() {
        let bindings = vec![binding("agent-1")];
        let known = vec!["agent-1".to_string()];

        assert_eq!(orphan_binding_count(&bindings, &known), 0);
    }

    #[cfg(feature = "mcp")]
    mod mcp_feature_tests {
        use super::super::{effective_permissions, filter_clients};
        use dbflux_mcp::{PolicyRoleDto, ToolPolicyDto, TrustedClientDto};

        fn client(id: &str, name: &str) -> TrustedClientDto {
            TrustedClientDto {
                id: id.to_string(),
                name: name.to_string(),
                issuer: None,
                active: true,
            }
        }

        #[test]
        fn filter_clients_matches_by_id() {
            let clients = vec![
                client("prod-agent", "Prod Agent"),
                client("dev-agent", "Dev Agent"),
            ];

            let filtered = filter_clients(&clients, "prod-a");

            assert_eq!(filtered.len(), 1);
            assert_eq!(filtered[0].id, "prod-agent");
        }

        #[test]
        fn filter_clients_matches_by_name_case_insensitively() {
            let clients = vec![
                client("prod-agent", "Prod Agent"),
                client("dev-agent", "Dev Agent"),
            ];

            let filtered = filter_clients(&clients, "DEV");

            assert_eq!(filtered.len(), 1);
            assert_eq!(filtered[0].id, "dev-agent");
        }

        #[test]
        fn filter_clients_returns_all_for_empty_query() {
            let clients = vec![
                client("prod-agent", "Prod Agent"),
                client("dev-agent", "Dev Agent"),
            ];

            let filtered = filter_clients(&clients, "");

            assert_eq!(filtered.len(), 2);
        }

        #[test]
        fn effective_permissions_unions_direct_and_role_policies() {
            let binding = super::super::ConnectionMcpPolicyBinding {
                actor_id: "agent-1".to_string(),
                role_ids: vec!["read-only".to_string()],
                policy_ids: vec!["direct-policy".to_string()],
            };
            let roles = vec![PolicyRoleDto {
                id: "read-only".to_string(),
                policy_ids: vec!["read-policy".to_string()],
            }];
            let policies = vec![
                ToolPolicyDto {
                    id: "direct-policy".to_string(),
                    allowed_tools: vec!["list_tables".to_string()],
                    allowed_classes: vec!["Metadata".to_string()],
                },
                ToolPolicyDto {
                    id: "read-policy".to_string(),
                    allowed_tools: vec!["read_query".to_string()],
                    allowed_classes: vec!["Read".to_string()],
                },
            ];

            let effective = effective_permissions(&binding, &roles, &policies);

            assert_eq!(
                effective.tools,
                vec!["list_tables".to_string(), "read_query".to_string()]
            );
            assert_eq!(
                effective.classes,
                vec!["Metadata".to_string(), "Read".to_string()]
            );
        }

        #[test]
        fn effective_permissions_skips_unknown_role_id() {
            let binding = super::super::ConnectionMcpPolicyBinding {
                actor_id: "agent-1".to_string(),
                role_ids: vec!["missing-role".to_string()],
                policy_ids: Vec::new(),
            };

            let effective = effective_permissions(&binding, &[], &[]);

            assert!(effective.tools.is_empty());
            assert!(effective.classes.is_empty());
        }

        #[test]
        fn effective_permissions_dedups_tools_shared_across_policies() {
            let binding = super::super::ConnectionMcpPolicyBinding {
                actor_id: "agent-1".to_string(),
                role_ids: vec!["role-a".to_string(), "role-b".to_string()],
                policy_ids: Vec::new(),
            };
            let roles = vec![
                PolicyRoleDto {
                    id: "role-a".to_string(),
                    policy_ids: vec!["shared-policy".to_string()],
                },
                PolicyRoleDto {
                    id: "role-b".to_string(),
                    policy_ids: vec!["shared-policy".to_string()],
                },
            ];
            let policies = vec![ToolPolicyDto {
                id: "shared-policy".to_string(),
                allowed_tools: vec!["read_query".to_string()],
                allowed_classes: vec!["Read".to_string()],
            }];

            let effective = effective_permissions(&binding, &roles, &policies);

            assert_eq!(effective.tools, vec!["read_query".to_string()]);
        }
    }
}
