use dbflux_mcp::handlers::scripts::{ScriptHandler, ScriptHandlerError, ScriptLifecycleState};
use dbflux_policy::{
    ConnectionPolicyAssignment, ExecutionClassification, PolicyBindingScope, PolicyEngine,
    ToolPolicy,
};

fn run_script_allowed_engine() -> PolicyEngine {
    PolicyEngine::new(
        vec![ConnectionPolicyAssignment {
            actor_id: "agent-a".to_string(),
            scope: PolicyBindingScope {
                connection_id: "conn-a".to_string(),
            },
            role_ids: Vec::new(),
            policy_ids: vec!["policy-admin".to_string()],
        }],
        Vec::new(),
        vec![ToolPolicy {
            id: "policy-admin".to_string(),
            allowed_tools: vec!["run_script".to_string()],
            allowed_classes: vec![ExecutionClassification::Admin],
        }],
    )
}

#[test]
fn script_crud_and_run_follow_policy_and_lifecycle_gates() {
    let mut handler = ScriptHandler::default();
    let policy_engine = run_script_allowed_engine();

    let script = handler.create_script(
        "seed".to_string(),
        "print('v1')".to_string(),
        ScriptLifecycleState::Runnable,
    );

    assert_eq!(handler.list_scripts().len(), 1);

    let updated = handler
        .update_script(script.id, "print('v2')".to_string())
        .expect("update should succeed");
    assert_eq!(updated.body, "print('v2')");

    let executed = handler
        .run_script(&policy_engine, "agent-a", "conn-a", script.id)
        .expect("run should succeed for runnable script with policy allow");
    assert_eq!(executed.id, script.id);

    handler
        .delete_script(script.id)
        .expect("delete should succeed");
    assert!(handler.list_scripts().is_empty());
}

#[test]
fn run_script_is_denied_for_non_runnable_or_unauthorized_actor() {
    let mut handler = ScriptHandler::default();
    let policy_engine = run_script_allowed_engine();

    let draft = handler.create_script(
        "draft".to_string(),
        "print('draft')".to_string(),
        ScriptLifecycleState::Draft,
    );

    let not_runnable = handler.run_script(&policy_engine, "agent-a", "conn-a", draft.id);
    assert!(matches!(not_runnable, Err(ScriptHandlerError::NotRunnable)));

    let runnable = handler.create_script(
        "ops".to_string(),
        "print('ops')".to_string(),
        ScriptLifecycleState::Runnable,
    );

    let unauthorized = handler.run_script(&policy_engine, "agent-z", "conn-a", runnable.id);
    assert!(matches!(
        unauthorized,
        Err(ScriptHandlerError::PolicyDenied)
    ));
}
