use dbflux_audit::AuditService;
use dbflux_core::observability::{
    AuditContext, EventCategory, EventOrigin, EventOutcome, EventRecord, EventSeverity,
    actions::MCP_AUTHORIZE, new_correlation_id,
};
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
    /// Correlation ID that links the authorization event with the execution event.
    /// Generated once at the start of a request and shared across both events.
    pub correlation_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationOutcome {
    pub allowed: bool,
    pub deny_code: Option<&'static str>,
    pub deny_reason: Option<String>,
    /// Correlation ID shared with the execution audit event.
    pub correlation_id: Option<String>,
    /// Actor (client) ID for the execution audit event.
    pub actor_id: String,
}

#[derive(Debug, Error)]
pub enum AuthorizationError {
    #[error("policy evaluation failed: {0}")]
    Policy(#[from] PolicyEngineError),
    #[error("audit record failed: {0}")]
    AuditRecord(#[from] dbflux_audit::AuditError),
}

pub fn authorize_request(
    trusted_clients: &TrustedClientRegistry,
    policy_engine: &PolicyEngine,
    audit_service: &AuditService,
    request: &AuthorizationRequest,
    created_at_epoch_ms: i64,
) -> Result<AuthorizationOutcome, AuthorizationError> {
    // Use existing correlation_id or generate one for this request.
    let correlation_id = request
        .correlation_id
        .clone()
        .unwrap_or_else(new_correlation_id);

    let origin = EventOrigin::mcp();
    let severity = EventSeverity::Info;

    if !request.mcp_enabled_for_connection {
        let reason = "connection not MCP-enabled".to_string();

        let event = build_authorization_event(
            created_at_epoch_ms,
            severity,
            &request.identity.client_id,
            &request.connection_id,
            &request.tool_id,
            request.classification,
            EventOutcome::Failure,
            &correlation_id,
            origin,
        )
        .with_error("connection_not_mcp_enabled", &reason)
        .with_details_json(
            serde_json::json!({
                "classification": format!("{:?}", request.classification),
            })
            .to_string(),
        );

        audit_service.record(event)?;

        return Ok(AuthorizationOutcome {
            allowed: false,
            deny_code: Some("connection_not_mcp_enabled"),
            deny_reason: Some(reason),
            correlation_id: Some(correlation_id),
            actor_id: request.identity.client_id.clone(),
        });
    }

    let identity = ClientIdentity {
        client_id: request.identity.client_id.clone(),
        issuer: request.identity.issuer.clone(),
    };

    if let TrustedClientMatch::Untrusted { reason } = trusted_clients.evaluate(&identity) {
        let event = build_authorization_event(
            created_at_epoch_ms,
            severity,
            &request.identity.client_id,
            &request.connection_id,
            &request.tool_id,
            request.classification,
            EventOutcome::Failure,
            &correlation_id,
            origin,
        )
        .with_error("untrusted_client", reason)
        .with_details_json(
            serde_json::json!({
                "classification": format!("{:?}", request.classification),
            })
            .to_string(),
        );

        audit_service.record(event)?;

        return Ok(AuthorizationOutcome {
            allowed: false,
            deny_code: Some("untrusted_client"),
            deny_reason: Some(reason.to_string()),
            correlation_id: Some(correlation_id),
            actor_id: request.identity.client_id.clone(),
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
            let event = build_authorization_event(
                created_at_epoch_ms,
                severity,
                &request.identity.client_id,
                &request.connection_id,
                &request.tool_id,
                request.classification,
                EventOutcome::Success,
                &correlation_id,
                origin,
            )
            .with_details_json(
                serde_json::json!({
                    "classification": format!("{:?}", request.classification),
                })
                .to_string(),
            );

            audit_service.record(event)?;

            Ok(AuthorizationOutcome {
                allowed: true,
                deny_code: None,
                deny_reason: None,
                correlation_id: Some(correlation_id),
                actor_id: request.identity.client_id.clone(),
            })
        }
        PolicyDecision::Deny(reason) => {
            let reason_text = format_policy_deny_reason(reason).to_string();

            let event = build_authorization_event(
                created_at_epoch_ms,
                severity,
                &request.identity.client_id,
                &request.connection_id,
                &request.tool_id,
                request.classification,
                EventOutcome::Failure,
                &correlation_id,
                origin,
            )
            .with_error("policy_denied", &reason_text)
            .with_details_json(
                serde_json::json!({
                    "classification": format!("{:?}", request.classification),
                })
                .to_string(),
            );

            audit_service.record(event)?;

            Ok(AuthorizationOutcome {
                allowed: false,
                deny_code: Some("policy_denied"),
                deny_reason: Some(reason_text),
                correlation_id: Some(correlation_id),
                actor_id: request.identity.client_id.clone(),
            })
        }
    }
}

/// Builds a canonical MCP authorization event with all required fields set.
#[allow(clippy::too_many_arguments)]
fn build_authorization_event(
    ts_ms: i64,
    level: EventSeverity,
    actor_id: &str,
    connection_id: &str,
    tool_id: &str,
    classification: ExecutionClassification,
    outcome: EventOutcome,
    correlation_id: &str,
    origin: EventOrigin,
) -> EventRecord {
    let mut event = EventRecord::new(ts_ms, level, EventCategory::Mcp, outcome)
        .with_typed_action(MCP_AUTHORIZE)
        .with_summary(format!(
            "MCP authorization {}: tool={} classification={:?}",
            outcome.as_str(),
            tool_id,
            classification,
        ))
        .with_actor_id(actor_id)
        .with_object_ref("tool", tool_id);

    AuditContext::new()
        .with_origin(origin)
        .with_correlation_id(correlation_id)
        .with_connection_id(connection_id)
        .apply_to(&mut event);

    event
}

fn format_policy_deny_reason(reason: PolicyDecisionReason) -> &'static str {
    match reason {
        PolicyDecisionReason::NoAssignment => "no matching connection-scoped assignment",
        PolicyDecisionReason::NoPolicy => "no matching policy",
        PolicyDecisionReason::ToolDenied => "tool denied by policy",
        PolicyDecisionReason::ClassificationDenied => "classification denied by policy",
    }
}
