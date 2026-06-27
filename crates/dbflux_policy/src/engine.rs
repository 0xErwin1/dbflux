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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyDecision {
    Allow,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolPolicy {
    pub id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_classes: Vec<ExecutionClassification>,
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

            if policy
                .allowed_classes
                .iter()
                .any(|class| class == &request.classification)
            {
                return Ok(PolicyDecision::Allow);
            }
        }

        if has_tool_match {
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
        PolicyDecision, PolicyDecisionReason, PolicyEngine, PolicyEngineError,
        PolicyEvaluationRequest, PolicyRole, ToolPolicy,
    };

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
                },
                read_query_policy("read-a"),
            ],
        );

        let decision = engine
            .evaluate(&request("A"))
            .expect("evaluation should succeed");

        assert_eq!(decision, PolicyDecision::Allow);
    }
}
