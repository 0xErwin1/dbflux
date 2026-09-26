use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::assignments::ConnectionPolicyAssignment;
use crate::classification::ExecutionClassification;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyEvaluationRequest {
    pub actor_id: String,
    pub connection_id: String,
    pub tool_id: String,
    pub classification: ExecutionClassification,
}

/// Outcome of evaluating one request against every policy that applies to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyDecision {
    Allow,
    /// The call may run only after a person approves it through the pending
    /// execution queue.
    RequireApproval,
    Deny(PolicyDecisionReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyDecisionReason {
    NoAssignment,
    NoPolicy,
    ToolDenied,
    ClassificationDenied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyRole {
    pub id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub policy_ids: Vec<String>,
}

/// What one policy does with one execution class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClassDecision {
    Allow,
    Ask,
    Deny,
}

/// A tool allowlist plus a decision for each execution class.
///
/// The decision per class is encoded in two lists so policies serialized
/// before the Ask decision existed keep their meaning: a class listed in
/// `allowed_classes` is Allow, a class listed in `approval_classes` is Ask,
/// and a class in neither is Deny. A class listed in both is Ask, the
/// stricter of the two.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolPolicy {
    pub id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_classes: Vec<ExecutionClassification>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub approval_classes: Vec<ExecutionClassification>,
}

impl ToolPolicy {
    pub fn class_decision(&self, classification: ExecutionClassification) -> ClassDecision {
        if self.approval_classes.contains(&classification) {
            ClassDecision::Ask
        } else if self.allowed_classes.contains(&classification) {
            ClassDecision::Allow
        } else {
            ClassDecision::Deny
        }
    }
}

#[derive(Debug, Error)]
pub enum PolicyEngineError {
    #[error("role not found: {0}")]
    MissingRole(String),
}

#[derive(Debug, Clone, Default)]
pub struct PolicyEngine {
    assignments: Vec<ConnectionPolicyAssignment>,
    roles: HashMap<String, PolicyRole>,
    policies: HashMap<String, ToolPolicy>,
}

impl PolicyEngine {
    pub fn new(
        assignments: Vec<ConnectionPolicyAssignment>,
        roles: Vec<PolicyRole>,
        policies: Vec<ToolPolicy>,
    ) -> Self {
        Self {
            assignments,
            roles: roles
                .into_iter()
                .map(|role| (role.id.clone(), role))
                .collect(),
            policies: policies
                .into_iter()
                .map(|policy| (policy.id.clone(), policy))
                .collect(),
        }
    }

    /// Evaluates a request against the union of policies assigned to the actor
    /// on the connection, directly or through roles.
    ///
    /// Policies are additive grants, so among the policies that list the tool
    /// the most permissive class decision wins: Allow over Ask over Deny. Deny
    /// is the absence of a grant rather than a veto, which keeps every policy
    /// composed before the Ask decision existed evaluating exactly as before.
    pub fn evaluate(
        &self,
        request: &PolicyEvaluationRequest,
    ) -> Result<PolicyDecision, PolicyEngineError> {
        let mut policy_ids = HashSet::new();

        for assignment in self.assignments.iter().filter(|assignment| {
            assignment.applies_to(request.actor_id.as_str(), request.connection_id.as_str())
        }) {
            policy_ids.extend(assignment.policy_ids.iter().cloned());

            for role_id in &assignment.role_ids {
                let role = self
                    .roles
                    .get(role_id)
                    .ok_or_else(|| PolicyEngineError::MissingRole(role_id.clone()))?;

                policy_ids.extend(role.policy_ids.iter().cloned());
            }
        }

        if policy_ids.is_empty() {
            return Ok(PolicyDecision::Deny(PolicyDecisionReason::NoAssignment));
        }

        let mut has_tool_match = false;
        let mut requires_approval = false;

        for policy_id in policy_ids {
            let Some(policy) = self.policies.get(&policy_id) else {
                continue;
            };

            if !policy
                .allowed_tools
                .iter()
                .any(|tool| tool == &request.tool_id)
            {
                continue;
            }

            has_tool_match = true;

            match policy.class_decision(request.classification) {
                ClassDecision::Allow => return Ok(PolicyDecision::Allow),
                ClassDecision::Ask => requires_approval = true,
                ClassDecision::Deny => {}
            }
        }

        if requires_approval {
            Ok(PolicyDecision::RequireApproval)
        } else if has_tool_match {
            Ok(PolicyDecision::Deny(
                PolicyDecisionReason::ClassificationDenied,
            ))
        } else if self.policies.is_empty() {
            Ok(PolicyDecision::Deny(PolicyDecisionReason::NoPolicy))
        } else {
            Ok(PolicyDecision::Deny(PolicyDecisionReason::ToolDenied))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::assignments::{ConnectionPolicyAssignment, PolicyBindingScope};
    use crate::classification::ExecutionClassification;

    use super::{
        ClassDecision, PolicyDecision, PolicyDecisionReason, PolicyEngine, PolicyEngineError,
        PolicyEvaluationRequest, PolicyRole, ToolPolicy,
    };

    const ALL_CLASSES: [ExecutionClassification; 7] = [
        ExecutionClassification::Metadata,
        ExecutionClassification::Read,
        ExecutionClassification::Write,
        ExecutionClassification::Destructive,
        ExecutionClassification::AdminSafe,
        ExecutionClassification::Admin,
        ExecutionClassification::AdminDestructive,
    ];

    const ALL_DECISIONS: [ClassDecision; 3] = [
        ClassDecision::Allow,
        ClassDecision::Ask,
        ClassDecision::Deny,
    ];

    fn request(connection_id: &str) -> PolicyEvaluationRequest {
        PolicyEvaluationRequest {
            actor_id: "alice".to_string(),
            connection_id: connection_id.to_string(),
            tool_id: "read_query".to_string(),
            classification: ExecutionClassification::Read,
        }
    }

    #[test]
    fn allows_connection_scoped_policy_for_connection_a() {
        let engine = PolicyEngine::new(
            vec![ConnectionPolicyAssignment {
                actor_id: "alice".to_string(),
                scope: PolicyBindingScope {
                    connection_id: "A".to_string(),
                },
                role_ids: Vec::new(),
                policy_ids: vec!["read-a".to_string()],
            }],
            Vec::new(),
            vec![ToolPolicy {
                id: "read-a".to_string(),
                allowed_tools: vec!["read_query".to_string()],
                allowed_classes: vec![ExecutionClassification::Read],
                approval_classes: Vec::new(),
            }],
        );

        let decision = engine
            .evaluate(&request("A"))
            .expect("evaluation should succeed");

        assert_eq!(decision, PolicyDecision::Allow);
    }

    #[test]
    fn denies_same_actor_for_connection_b_without_assignment() {
        let engine = PolicyEngine::new(
            vec![ConnectionPolicyAssignment {
                actor_id: "alice".to_string(),
                scope: PolicyBindingScope {
                    connection_id: "A".to_string(),
                },
                role_ids: Vec::new(),
                policy_ids: vec!["read-a".to_string()],
            }],
            Vec::new(),
            vec![ToolPolicy {
                id: "read-a".to_string(),
                allowed_tools: vec!["read_query".to_string()],
                allowed_classes: vec![ExecutionClassification::Read],
                approval_classes: Vec::new(),
            }],
        );

        let decision = engine
            .evaluate(&request("B"))
            .expect("evaluation should succeed");

        assert_eq!(
            decision,
            PolicyDecision::Deny(PolicyDecisionReason::NoAssignment)
        );
    }

    #[test]
    fn denies_when_tool_matches_but_classification_not_allowed() {
        let engine = PolicyEngine::new(
            vec![ConnectionPolicyAssignment {
                actor_id: "alice".to_string(),
                scope: PolicyBindingScope {
                    connection_id: "A".to_string(),
                },
                role_ids: Vec::new(),
                policy_ids: vec!["read-a".to_string()],
            }],
            Vec::new(),
            vec![ToolPolicy {
                id: "read-a".to_string(),
                allowed_tools: vec!["read_query".to_string()],
                allowed_classes: vec![ExecutionClassification::Metadata],
                approval_classes: Vec::new(),
            }],
        );

        let decision = engine
            .evaluate(&request("A"))
            .expect("evaluation should succeed");

        assert_eq!(
            decision,
            PolicyDecision::Deny(PolicyDecisionReason::ClassificationDenied)
        );
    }

    fn read_query_assignment(
        role_ids: Vec<String>,
        policy_ids: Vec<String>,
    ) -> ConnectionPolicyAssignment {
        ConnectionPolicyAssignment {
            actor_id: "alice".to_string(),
            scope: PolicyBindingScope {
                connection_id: "A".to_string(),
            },
            role_ids,
            policy_ids,
        }
    }

    fn read_query_policy(id: &str) -> ToolPolicy {
        ToolPolicy {
            id: id.to_string(),
            allowed_tools: vec!["read_query".to_string()],
            allowed_classes: vec![ExecutionClassification::Read],
            approval_classes: Vec::new(),
        }
    }

    #[test]
    fn allows_via_role_resolved_policy() {
        let engine = PolicyEngine::new(
            vec![read_query_assignment(
                vec!["reader".to_string()],
                Vec::new(),
            )],
            vec![PolicyRole {
                id: "reader".to_string(),
                policy_ids: vec!["read-a".to_string()],
            }],
            vec![read_query_policy("read-a")],
        );

        let decision = engine
            .evaluate(&request("A"))
            .expect("evaluation should succeed");

        assert_eq!(decision, PolicyDecision::Allow);
    }

    #[test]
    fn missing_role_is_an_engine_error() {
        let engine = PolicyEngine::new(
            vec![read_query_assignment(
                vec!["ghost-role".to_string()],
                Vec::new(),
            )],
            Vec::new(),
            vec![read_query_policy("read-a")],
        );

        let error = engine
            .evaluate(&request("A"))
            .expect_err("a role_id absent from the roles set must surface an error");

        assert!(matches!(error, PolicyEngineError::MissingRole(role) if role == "ghost-role"));
    }

    #[test]
    fn tool_denied_when_policy_matches_but_tool_not_allowed() {
        let engine = PolicyEngine::new(
            vec![read_query_assignment(
                Vec::new(),
                vec!["write-only".to_string()],
            )],
            Vec::new(),
            vec![ToolPolicy {
                id: "write-only".to_string(),
                allowed_tools: vec!["write_query".to_string()],
                allowed_classes: vec![ExecutionClassification::Read],
                approval_classes: Vec::new(),
            }],
        );

        let decision = engine
            .evaluate(&request("A"))
            .expect("evaluation should succeed");

        assert_eq!(
            decision,
            PolicyDecision::Deny(PolicyDecisionReason::ToolDenied)
        );
    }

    #[test]
    fn no_policy_when_policy_set_is_empty() {
        let engine = PolicyEngine::new(
            vec![read_query_assignment(
                Vec::new(),
                vec!["read-a".to_string()],
            )],
            Vec::new(),
            Vec::new(),
        );

        let decision = engine
            .evaluate(&request("A"))
            .expect("evaluation should succeed");

        assert_eq!(
            decision,
            PolicyDecision::Deny(PolicyDecisionReason::NoPolicy)
        );
    }

    #[test]
    fn actor_draws_policies_from_both_direct_id_and_role() {
        // The direct policy_id grants a tool the request does not use; the role's
        // policy is the one that authorizes read_query. Allow must come from the
        // union of both sources, not from either alone.
        let engine = PolicyEngine::new(
            vec![read_query_assignment(
                vec!["reader".to_string()],
                vec!["meta-only".to_string()],
            )],
            vec![PolicyRole {
                id: "reader".to_string(),
                policy_ids: vec!["read-a".to_string()],
            }],
            vec![
                ToolPolicy {
                    id: "meta-only".to_string(),
                    allowed_tools: vec!["describe_table".to_string()],
                    allowed_classes: vec![ExecutionClassification::Metadata],
                    approval_classes: Vec::new(),
                },
                read_query_policy("read-a"),
            ],
        );

        let decision = engine
            .evaluate(&request("A"))
            .expect("evaluation should succeed");

        assert_eq!(decision, PolicyDecision::Allow);
    }

    /// A read_query policy that gives `classification` the `decision` and
    /// denies every other class.
    fn policy_with_decision(
        id: &str,
        classification: ExecutionClassification,
        decision: ClassDecision,
    ) -> ToolPolicy {
        let mut policy = ToolPolicy {
            id: id.to_string(),
            allowed_tools: vec!["read_query".to_string()],
            allowed_classes: Vec::new(),
            approval_classes: Vec::new(),
        };

        match decision {
            ClassDecision::Allow => policy.allowed_classes.push(classification),
            ClassDecision::Ask => policy.approval_classes.push(classification),
            ClassDecision::Deny => {}
        }

        policy
    }

    fn classified_request(classification: ExecutionClassification) -> PolicyEvaluationRequest {
        PolicyEvaluationRequest {
            classification,
            ..request("A")
        }
    }

    fn expected_decision(decision: ClassDecision) -> PolicyDecision {
        match decision {
            ClassDecision::Allow => PolicyDecision::Allow,
            ClassDecision::Ask => PolicyDecision::RequireApproval,
            ClassDecision::Deny => PolicyDecision::Deny(PolicyDecisionReason::ClassificationDenied),
        }
    }

    fn most_permissive(left: ClassDecision, right: ClassDecision) -> ClassDecision {
        if left == ClassDecision::Allow || right == ClassDecision::Allow {
            ClassDecision::Allow
        } else if left == ClassDecision::Ask || right == ClassDecision::Ask {
            ClassDecision::Ask
        } else {
            ClassDecision::Deny
        }
    }

    #[test]
    fn class_decision_reads_both_lists_and_prefers_ask_when_listed_twice() {
        let policy = ToolPolicy {
            id: "mixed".to_string(),
            allowed_tools: vec!["read_query".to_string()],
            allowed_classes: vec![
                ExecutionClassification::Read,
                ExecutionClassification::Write,
            ],
            approval_classes: vec![
                ExecutionClassification::Write,
                ExecutionClassification::Destructive,
            ],
        };

        assert_eq!(
            policy.class_decision(ExecutionClassification::Read),
            ClassDecision::Allow
        );
        assert_eq!(
            policy.class_decision(ExecutionClassification::Write),
            ClassDecision::Ask
        );
        assert_eq!(
            policy.class_decision(ExecutionClassification::Destructive),
            ClassDecision::Ask
        );
        assert_eq!(
            policy.class_decision(ExecutionClassification::Admin),
            ClassDecision::Deny
        );
    }

    #[test]
    fn single_policy_decision_matrix_covers_every_class() {
        for classification in ALL_CLASSES {
            for decision in ALL_DECISIONS {
                let engine = PolicyEngine::new(
                    vec![read_query_assignment(
                        Vec::new(),
                        vec!["policy".to_string()],
                    )],
                    Vec::new(),
                    vec![policy_with_decision("policy", classification, decision)],
                );

                let evaluated = engine
                    .evaluate(&classified_request(classification))
                    .expect("evaluation should succeed");

                assert_eq!(
                    evaluated,
                    expected_decision(decision),
                    "class {classification:?} with decision {decision:?}"
                );
            }
        }
    }

    #[test]
    fn decision_for_one_class_does_not_leak_into_other_classes() {
        for classification in ALL_CLASSES {
            for other in ALL_CLASSES
                .into_iter()
                .filter(|other| *other != classification)
            {
                let engine = PolicyEngine::new(
                    vec![read_query_assignment(
                        Vec::new(),
                        vec!["policy".to_string()],
                    )],
                    Vec::new(),
                    vec![policy_with_decision(
                        "policy",
                        classification,
                        ClassDecision::Ask,
                    )],
                );

                let evaluated = engine
                    .evaluate(&classified_request(other))
                    .expect("evaluation should succeed");

                assert_eq!(
                    evaluated,
                    PolicyDecision::Deny(PolicyDecisionReason::ClassificationDenied),
                    "Ask on {classification:?} must not affect {other:?}"
                );
            }
        }
    }

    #[test]
    fn role_and_direct_policies_compose_to_the_most_permissive_decision() {
        for classification in ALL_CLASSES {
            for role_decision in ALL_DECISIONS {
                for direct_decision in ALL_DECISIONS {
                    let engine = PolicyEngine::new(
                        vec![read_query_assignment(
                            vec!["role".to_string()],
                            vec!["direct".to_string()],
                        )],
                        vec![PolicyRole {
                            id: "role".to_string(),
                            policy_ids: vec!["from-role".to_string()],
                        }],
                        vec![
                            policy_with_decision("from-role", classification, role_decision),
                            policy_with_decision("direct", classification, direct_decision),
                        ],
                    );

                    let evaluated = engine
                        .evaluate(&classified_request(classification))
                        .expect("evaluation should succeed");

                    assert_eq!(
                        evaluated,
                        expected_decision(most_permissive(role_decision, direct_decision)),
                        "class {classification:?}, role {role_decision:?}, direct {direct_decision:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn ask_from_a_policy_without_the_tool_is_ignored() {
        let mut other_tool = policy_with_decision(
            "other-tool",
            ExecutionClassification::Read,
            ClassDecision::Ask,
        );
        other_tool.allowed_tools = vec!["write_query".to_string()];

        let engine = PolicyEngine::new(
            vec![read_query_assignment(
                Vec::new(),
                vec!["other-tool".to_string(), "deny-read".to_string()],
            )],
            Vec::new(),
            vec![
                other_tool,
                policy_with_decision(
                    "deny-read",
                    ExecutionClassification::Read,
                    ClassDecision::Deny,
                ),
            ],
        );

        let evaluated = engine
            .evaluate(&request("A"))
            .expect("evaluation should succeed");

        assert_eq!(
            evaluated,
            PolicyDecision::Deny(PolicyDecisionReason::ClassificationDenied)
        );
    }

    #[test]
    fn policy_serialized_before_ask_existed_deserializes_with_no_approval_classes() {
        let legacy = r#"{"id":"legacy","allowed_tools":["read_query"],"allowed_classes":["read"]}"#;

        let policy: ToolPolicy =
            serde_json::from_str(legacy).expect("legacy policy should deserialize");

        assert!(policy.approval_classes.is_empty());
        assert_eq!(
            policy.class_decision(ExecutionClassification::Read),
            ClassDecision::Allow
        );
        assert_eq!(
            policy.class_decision(ExecutionClassification::Write),
            ClassDecision::Deny
        );
    }
}
