use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyBindingScope {
    pub connection_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionPolicyAssignment {
    pub actor_id: String,
    pub scope: PolicyBindingScope,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub role_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub policy_ids: Vec<String>,
}

impl ConnectionPolicyAssignment {
    pub fn applies_to(&self, actor_id: &str, connection_id: &str) -> bool {
        self.actor_id == actor_id && self.scope.connection_id == connection_id
    }
}

#[cfg(test)]
mod tests {
    use super::{ConnectionPolicyAssignment, PolicyBindingScope};

    fn assignment() -> ConnectionPolicyAssignment {
        ConnectionPolicyAssignment {
            actor_id: "alice".to_string(),
            scope: PolicyBindingScope {
                connection_id: "conn-a".to_string(),
            },
            role_ids: Vec::new(),
            policy_ids: Vec::new(),
        }
    }

    #[test]
    fn applies_to_matches_actor_and_connection() {
        assert!(assignment().applies_to("alice", "conn-a"));
    }

    #[test]
    fn applies_to_rejects_actor_mismatch() {
        assert!(!assignment().applies_to("bob", "conn-a"));
    }

    #[test]
    fn applies_to_rejects_connection_mismatch() {
        assert!(!assignment().applies_to("alice", "conn-b"));
    }

    #[test]
    fn applies_to_rejects_when_both_mismatch() {
        assert!(!assignment().applies_to("bob", "conn-b"));
    }
}
