use serde::{Deserialize, Serialize};

pub const UNTRUSTED_CLIENT_AUDIT_REASON: &str = "untrusted client";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedClient {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub issuer: Option<String>,
    #[serde(default = "default_true")]
    pub active: bool,
}

impl TrustedClient {
    pub fn matches(&self, identity: &ClientIdentity) -> bool {
        if self.id != identity.client_id {
            return false;
        }

        match (&self.issuer, &identity.issuer) {
            (None, _) => true,
            (Some(expected), Some(actual)) => expected == actual,
            (Some(_), None) => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIdentity {
    pub client_id: String,
    pub issuer: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrustedClientMatch {
    Trusted(TrustedClient),
    Untrusted { reason: &'static str },
}

#[derive(Debug, Clone, Default)]
pub struct TrustedClientRegistry {
    clients: Vec<TrustedClient>,
}

impl TrustedClientRegistry {
    pub fn new(clients: Vec<TrustedClient>) -> Self {
        Self { clients }
    }

    pub fn replace_clients(&mut self, clients: Vec<TrustedClient>) {
        self.clients = clients;
    }

    pub fn evaluate(&self, identity: &ClientIdentity) -> TrustedClientMatch {
        let Some(client) = self.clients.iter().find(|client| client.matches(identity)) else {
            return TrustedClientMatch::Untrusted {
                reason: UNTRUSTED_CLIENT_AUDIT_REASON,
            };
        };

        if client.active {
            TrustedClientMatch::Trusted(client.clone())
        } else {
            TrustedClientMatch::Untrusted {
                reason: UNTRUSTED_CLIENT_AUDIT_REASON,
            }
        }
    }
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::{
        ClientIdentity, TrustedClient, TrustedClientMatch, TrustedClientRegistry,
        UNTRUSTED_CLIENT_AUDIT_REASON,
    };

    #[test]
    fn trusted_client_is_accepted_when_active() {
        let registry = TrustedClientRegistry::new(vec![TrustedClient {
            id: "agent-a".to_string(),
            name: "Agent A".to_string(),
            issuer: None,
            active: true,
        }]);

        let result = registry.evaluate(&ClientIdentity {
            client_id: "agent-a".to_string(),
            issuer: None,
        });

        assert!(matches!(result, TrustedClientMatch::Trusted(_)));
    }

    #[test]
    fn inactive_client_is_denied() {
        let registry = TrustedClientRegistry::new(vec![TrustedClient {
            id: "agent-a".to_string(),
            name: "Agent A".to_string(),
            issuer: None,
            active: false,
        }]);

        let result = registry.evaluate(&ClientIdentity {
            client_id: "agent-a".to_string(),
            issuer: None,
        });

        assert_eq!(
            result,
            TrustedClientMatch::Untrusted {
                reason: UNTRUSTED_CLIENT_AUDIT_REASON
            }
        );
    }

    #[test]
    fn issuer_mismatch_is_denied() {
        let registry = TrustedClientRegistry::new(vec![TrustedClient {
            id: "agent-a".to_string(),
            name: "Agent A".to_string(),
            issuer: Some("issuer-a".to_string()),
            active: true,
        }]);

        let result = registry.evaluate(&ClientIdentity {
            client_id: "agent-a".to_string(),
            issuer: Some("issuer-b".to_string()),
        });

        assert_eq!(
            result,
            TrustedClientMatch::Untrusted {
                reason: UNTRUSTED_CLIENT_AUDIT_REASON
            }
        );
    }

    #[test]
    fn unknown_client_is_denied_with_audit_reason() {
        let registry = TrustedClientRegistry::default();

        let result = registry.evaluate(&ClientIdentity {
            client_id: "unknown".to_string(),
            issuer: None,
        });

        assert_eq!(
            result,
            TrustedClientMatch::Untrusted {
                reason: UNTRUSTED_CLIENT_AUDIT_REASON
            }
        );
    }
}
