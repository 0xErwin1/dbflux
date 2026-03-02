use dbflux_core::{
    CancelToken, ConnectionHook, ConnectionHookBindings, ConnectionHooks, ConnectionProfile,
    DbConfig, HookContext, HookPhase, HookPhaseOutcome, HookRunner,
};
use std::collections::HashMap;

// =========================================================================
// Helpers
// =========================================================================

fn echo_hook(message: &str) -> ConnectionHook {
    serde_json::from_value(serde_json::json!({
        "command": "echo",
        "args": [message]
    }))
    .unwrap()
}

fn failing_hook_warn() -> ConnectionHook {
    serde_json::from_value(serde_json::json!({
        "command": "false",
        "on_failure": "warn"
    }))
    .unwrap()
}

fn failing_hook_disconnect() -> ConnectionHook {
    serde_json::from_value(serde_json::json!({
        "command": "false",
        "on_failure": "disconnect"
    }))
    .unwrap()
}

fn disabled_echo() -> ConnectionHook {
    serde_json::from_value(serde_json::json!({
        "command": "echo",
        "args": ["disabled"],
        "enabled": false
    }))
    .unwrap()
}

fn test_profile() -> ConnectionProfile {
    ConnectionProfile::new("integration-test", DbConfig::default_postgres())
}

fn test_definitions() -> HashMap<String, ConnectionHook> {
    HashMap::from([
        ("setup-vpn".to_string(), echo_hook("vpn-up")),
        ("warm-cache".to_string(), echo_hook("cache-warmed")),
        ("cleanup".to_string(), echo_hook("cleaned")),
        ("fail-warn".to_string(), failing_hook_warn()),
        ("fail-disconnect".to_string(), failing_hook_disconnect()),
        ("disabled-hook".to_string(), disabled_echo()),
    ])
}

fn run_all_phases(hooks: &ConnectionHooks, context: &HookContext) -> Vec<HookPhaseOutcome> {
    let token = CancelToken::new();

    [
        HookPhase::PreConnect,
        HookPhase::PostConnect,
        HookPhase::PreDisconnect,
        HookPhase::PostDisconnect,
    ]
    .iter()
    .map(|&phase| HookRunner::run_phase(phase, hooks.phase_hooks(phase), context, &token, None))
    .collect()
}

fn execution_count(outcome: &HookPhaseOutcome) -> usize {
    match outcome {
        HookPhaseOutcome::Success { executions } => executions.len(),
        HookPhaseOutcome::CompletedWithWarnings { executions, .. } => executions.len(),
        HookPhaseOutcome::Aborted { executions, .. } => executions.len(),
    }
}

// =========================================================================
// Tests
// =========================================================================

#[test]
fn end_to_end_connect_disconnect_with_bindings() {
    let definitions = test_definitions();

    let mut profile = test_profile();
    profile.hook_bindings = Some(ConnectionHookBindings {
        pre_connect: vec!["setup-vpn".to_string()],
        post_connect: vec!["warm-cache".to_string()],
        pre_disconnect: vec!["cleanup".to_string()],
        post_disconnect: vec!["cleanup".to_string()],
    });

    let hooks = ConnectionHooks::resolve_from_bindings(&profile, &definitions);
    let context = HookContext::from_profile(&profile);
    let outcomes = run_all_phases(&hooks, &context);

    for (i, outcome) in outcomes.iter().enumerate() {
        assert!(
            matches!(outcome, HookPhaseOutcome::Success { .. }),
            "phase {} should succeed, got {:?}",
            i,
            outcome
        );
        assert_eq!(
            execution_count(outcome),
            1,
            "phase {} should have 1 execution",
            i
        );
    }
}

#[test]
fn binding_references_missing_hook_is_skipped() {
    let definitions = test_definitions();

    let mut profile = test_profile();
    profile.hook_bindings = Some(ConnectionHookBindings {
        pre_connect: vec!["setup-vpn".to_string(), "nonexistent".to_string()],
        ..Default::default()
    });

    let hooks = ConnectionHooks::resolve_from_bindings(&profile, &definitions);

    assert_eq!(
        hooks.pre_connect.len(),
        1,
        "missing binding should be skipped"
    );
    assert_eq!(hooks.pre_connect[0].args, vec!["vpn-up"]);
}

#[test]
fn profile_without_bindings_uses_inline_hooks() {
    let definitions = test_definitions();

    let mut profile = test_profile();
    profile.hooks = Some(ConnectionHooks {
        pre_connect: vec![echo_hook("inline-pre")],
        post_disconnect: vec![echo_hook("inline-post")],
        ..Default::default()
    });

    let hooks = ConnectionHooks::resolve_from_bindings(&profile, &definitions);
    let context = HookContext::from_profile(&profile);

    assert_eq!(hooks.pre_connect.len(), 1);
    assert_eq!(hooks.post_disconnect.len(), 1);

    let token = CancelToken::new();
    let outcome = HookRunner::run_phase(
        HookPhase::PreConnect,
        hooks.phase_hooks(HookPhase::PreConnect),
        &context,
        &token,
        None,
    );

    match outcome {
        HookPhaseOutcome::Success { executions } => {
            assert_eq!(executions.len(), 1);
            let stdout = executions[0].result.as_ref().unwrap().stdout.trim();
            assert_eq!(stdout, "inline-pre");
        }
        other => panic!("expected Success, got {:?}", other),
    }
}

#[test]
fn profile_without_any_hooks_runs_empty_phases() {
    let definitions = test_definitions();
    let profile = test_profile();

    let hooks = ConnectionHooks::resolve_from_bindings(&profile, &definitions);
    let context = HookContext::from_profile(&profile);
    let outcomes = run_all_phases(&hooks, &context);

    for (i, outcome) in outcomes.iter().enumerate() {
        assert!(
            matches!(outcome, HookPhaseOutcome::Success { executions } if executions.is_empty()),
            "phase {} should be empty Success, got {:?}",
            i,
            outcome
        );
    }
}

#[test]
fn mixed_failure_modes_across_phases() {
    let definitions = test_definitions();

    let mut profile = test_profile();
    profile.hook_bindings = Some(ConnectionHookBindings {
        pre_connect: vec!["fail-warn".to_string(), "setup-vpn".to_string()],
        post_connect: vec!["fail-disconnect".to_string(), "warm-cache".to_string()],
        ..Default::default()
    });

    let hooks = ConnectionHooks::resolve_from_bindings(&profile, &definitions);
    let context = HookContext::from_profile(&profile);
    let token = CancelToken::new();

    let pre = HookRunner::run_phase(
        HookPhase::PreConnect,
        hooks.phase_hooks(HookPhase::PreConnect),
        &context,
        &token,
        None,
    );

    match pre {
        HookPhaseOutcome::CompletedWithWarnings {
            executions,
            warnings,
        } => {
            assert_eq!(executions.len(), 2, "warn continues to next hook");
            assert_eq!(warnings.len(), 1);
        }
        other => panic!("expected CompletedWithWarnings, got {:?}", other),
    }

    let post = HookRunner::run_phase(
        HookPhase::PostConnect,
        hooks.phase_hooks(HookPhase::PostConnect),
        &context,
        &token,
        None,
    );

    match post {
        HookPhaseOutcome::Aborted { executions, .. } => {
            assert_eq!(executions.len(), 1, "disconnect aborts before second hook");
        }
        other => panic!("expected Aborted, got {:?}", other),
    }
}

#[test]
fn disabled_hooks_in_bindings_are_skipped() {
    let definitions = test_definitions();

    let mut profile = test_profile();
    profile.hook_bindings = Some(ConnectionHookBindings {
        pre_connect: vec!["disabled-hook".to_string()],
        ..Default::default()
    });

    let hooks = ConnectionHooks::resolve_from_bindings(&profile, &definitions);

    assert_eq!(
        hooks.pre_connect.len(),
        1,
        "resolve includes the hook regardless of enabled"
    );
    assert!(!hooks.pre_connect[0].enabled);

    let context = HookContext::from_profile(&profile);
    let token = CancelToken::new();

    let outcome = HookRunner::run_phase(
        HookPhase::PreConnect,
        hooks.phase_hooks(HookPhase::PreConnect),
        &context,
        &token,
        None,
    );

    match outcome {
        HookPhaseOutcome::Success { executions } => {
            assert_eq!(executions.len(), 0, "disabled hook should not execute");
        }
        other => panic!("expected Success with 0 executions, got {:?}", other),
    }
}
