/// Tests for the governance binding role/policy repositories, running the
/// repository SQL against the fully migrated schema.
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_storage::repositories::connection_profile_governance_binding_policies::ConnectionProfileGovernanceBindingPolicyDto;
use dbflux_storage::repositories::connection_profile_governance_binding_roles::ConnectionProfileGovernanceBindingRoleDto;
use dbflux_storage::repositories::connection_profile_governance_bindings::ConnectionProfileGovernanceBindingDto;

/// Builds a migrated runtime with one profile and one governance binding,
/// returning the runtime and the binding id.
fn runtime_with_binding() -> (StorageRuntime, String) {
    let runtime = StorageRuntime::in_memory().expect("runtime should initialize");

    let profile_id = uuid::Uuid::new_v4().to_string();
    runtime
        .open_dbflux_db()
        .expect("should open dbflux.db")
        .execute(
            "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, 'P')",
            [&profile_id],
        )
        .expect("should insert profile");

    let binding = ConnectionProfileGovernanceBindingDto {
        id: uuid::Uuid::new_v4().to_string(),
        profile_id,
        actor_id: "test-actor".to_string(),
        order_index: 0,
    };
    runtime
        .connection_profiles()
        .governance_bindings()
        .insert(&binding)
        .expect("should insert governance binding");

    (runtime, binding.id)
}

#[test]
fn governance_binding_roles_are_persisted_and_loaded_per_binding() {
    let (runtime, binding_id) = runtime_with_binding();
    let roles_repo = runtime.connection_profiles().governance_binding_roles();

    for role_id in ["role-b", "role-a"] {
        roles_repo
            .insert(&ConnectionProfileGovernanceBindingRoleDto::new(
                binding_id.clone(),
                role_id.to_string(),
            ))
            .expect("should insert binding role");
    }

    let role_ids: Vec<String> = roles_repo
        .get_for_binding(&binding_id)
        .expect("should load binding roles")
        .into_iter()
        .map(|role| role.role_id)
        .collect();
    assert_eq!(role_ids, ["role-a", "role-b"], "roles sorted by role_id");

    roles_repo
        .delete_for_binding(&binding_id)
        .expect("should delete binding roles");
    assert!(
        roles_repo
            .get_for_binding(&binding_id)
            .expect("should load binding roles after delete")
            .is_empty()
    );
}

#[test]
fn governance_binding_policies_are_persisted_and_loaded_per_binding() {
    let (runtime, binding_id) = runtime_with_binding();
    let policies_repo = runtime.connection_profiles().governance_binding_policies();

    for policy_id in ["policy-b", "policy-a"] {
        policies_repo
            .insert(&ConnectionProfileGovernanceBindingPolicyDto::new(
                binding_id.clone(),
                policy_id.to_string(),
            ))
            .expect("should insert binding policy");
    }

    let policy_ids: Vec<String> = policies_repo
        .get_for_binding(&binding_id)
        .expect("should load binding policies")
        .into_iter()
        .map(|policy| policy.policy_id)
        .collect();
    assert_eq!(
        policy_ids,
        ["policy-a", "policy-b"],
        "policies sorted by policy_id"
    );

    policies_repo
        .delete_for_binding(&binding_id)
        .expect("should delete binding policies");
    assert!(
        policies_repo
            .get_for_binding(&binding_id)
            .expect("should load binding policies after delete")
            .is_empty()
    );
}
