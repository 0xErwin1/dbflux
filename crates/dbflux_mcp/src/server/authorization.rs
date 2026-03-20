use dbflux_audit::{AuditError, AuditService};
use dbflux_policy::{
    ClientIdentity, ExecutionClassification, PolicyDecision, PolicyDecisionReason, PolicyEngine,
    PolicyEngineError, PolicyEvaluationRequest, TrustedClientMatch, TrustedClientRegistry,
};
use thiserror::Error;

use crate::server::request_context::RequestIdentity;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationRequest {
    pub identity: RequestIdentity,
    pub connection_id: String,
    pub tool_id: String,
    pub classification: ExecutionClassification,
    pub mcp_enabled_for_connection: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationOutcome {
    pub allowed: bool,
    pub deny_code: Option<&'static str>,
    pub deny_reason: Option<String>,
}

#[derive(Debug, Error)]
pub enum AuthorizationError {
    #[error("policy evaluation failed: {0}")]
    Policy(#[from] PolicyEngineError),
    #[error("audit append failed: {0}")]
    Audit(#[from] AuditError),
}

pub fn authorize_request(
    trusted_clients: &TrustedClientRegistry,
    policy_engine: &PolicyEngine,
    audit_service: &AuditService,
    request: &AuthorizationRequest,
    created_at_epoch_ms: i64,
) -> Result<AuthorizationOutcome, AuthorizationError> {
    if !request.mcp_enabled_for_connection {
        let reason = "connection not MCP-enabled".to_string();
        audit_service.append(
            &request.identity.client_id,
            &request.tool_id,
            "deny",
            Some(&reason),
            created_at_epoch_ms,
        )?;

        return Ok(AuthorizationOutcome {
            allowed: false,
            deny_code: Some("connection_not_mcp_enabled"),
            deny_reason: Some(reason),
        });
    }

    let identity = ClientIdentity {
        client_id: request.identity.client_id.clone(),
        issuer: request.identity.issuer.clone(),
    };

    if let TrustedClientMatch::Untrusted { reason } = trusted_clients.evaluate(&identity) {
        audit_service.append(
            &request.identity.client_id,
            &request.tool_id,
            "deny",
            Some(reason),
            created_at_epoch_ms,
        )?;

        return Ok(AuthorizationOutcome {
            allowed: false,
            deny_code: Some("untrusted_client"),
            deny_reason: Some(reason.to_string()),
        });
    }

    let decision = policy_engine.evaluate(&PolicyEvaluationRequest {
        actor_id: request.identity.client_id.clone(),
        connection_id: request.connection_id.clone(),
        tool_id: request.tool_id.clone(),
        classification: request.classification,
    })?;

    match decision {
        PolicyDecision::Allow => {
            audit_service.append(
                &request.identity.client_id,
                &request.tool_id,
                "allow",
                None,
                created_at_epoch_ms,
            )?;

            Ok(AuthorizationOutcome {
                allowed: true,
                deny_code: None,
                deny_reason: None,
            })
        }
        PolicyDecision::Deny(reason) => {
            let reason_text = format_policy_deny_reason(reason).to_string();
            audit_service.append(
                &request.identity.client_id,
                &request.tool_id,
                "deny",
                Some(&reason_text),
                created_at_epoch_ms,
            )?;

            Ok(AuthorizationOutcome {
                allowed: false,
                deny_code: Some("policy_denied"),
                deny_reason: Some(reason_text),
            })
        }
    }
}

fn format_policy_deny_reason(reason: PolicyDecisionReason) -> &'static str {
    match reason {
        PolicyDecisionReason::NoAssignment => "no matching connection-scoped assignment",
        PolicyDecisionReason::NoPolicy => "no matching policy",
        PolicyDecisionReason::ToolDenied => "tool denied by policy",
        PolicyDecisionReason::ClassificationDenied => "classification denied by policy",
    }
}
