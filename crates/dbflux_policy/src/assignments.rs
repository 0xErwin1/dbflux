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
