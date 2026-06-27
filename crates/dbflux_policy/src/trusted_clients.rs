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

    fn client(id: &str, issuer: Option<&str>) -> TrustedClient {
        TrustedClient {
            id: id.to_string(),
            name: id.to_string(),
            issuer: issuer.map(str::to_string),
            active: true,
        }
    }

    #[test]
    fn issuer_none_in_config_accepts_regardless_of_identity_issuer() {
        // TrustedClient::matches returns true for any identity issuer when the
        // configured issuer is None (the `(None, _) => true` arm), so a wildcard
        // client must accept a presented issuer it never declared.
        let registry = TrustedClientRegistry::new(vec![client("agent-a", None)]);

        let result = registry.evaluate(&ClientIdentity {
            client_id: "agent-a".to_string(),
            issuer: Some("some-issuer".to_string()),
        });

        assert!(matches!(result, TrustedClientMatch::Trusted(_)));
    }

    #[test]
    fn registry_with_multiple_clients_resolves_the_matching_one() {
        let registry = TrustedClientRegistry::new(vec![
            client("agent-a", Some("issuer-a")),
            client("agent-b", Some("issuer-b")),
        ]);

        let result = registry.evaluate(&ClientIdentity {
            client_id: "agent-b".to_string(),
            issuer: Some("issuer-b".to_string()),
        });

        match result {
            TrustedClientMatch::Trusted(matched) => assert_eq!(matched.id, "agent-b"),
            other => panic!("expected agent-b to be trusted, got {other:?}"),
        }
    }

    #[test]
    fn replace_clients_swaps_the_active_set() {
        let mut registry = TrustedClientRegistry::new(vec![client("agent-a", None)]);

        let identity_a = ClientIdentity {
            client_id: "agent-a".to_string(),
            issuer: None,
        };
        let identity_b = ClientIdentity {
            client_id: "agent-b".to_string(),
            issuer: None,
        };

        assert!(matches!(
            registry.evaluate(&identity_a),
            TrustedClientMatch::Trusted(_)
        ));

        registry.replace_clients(vec![client("agent-b", None)]);

        // The previously trusted client is gone, and the new one is now trusted.
        assert_eq!(
            registry.evaluate(&identity_a),
            TrustedClientMatch::Untrusted {
                reason: UNTRUSTED_CLIENT_AUDIT_REASON
            }
        );
        assert!(matches!(
            registry.evaluate(&identity_b),
            TrustedClientMatch::Trusted(_)
        ));
    }
}
