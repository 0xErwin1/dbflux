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

/// Deny code returned when a policy routes the call to the approval queue.
pub const APPROVAL_REQUIRED_CODE: &str = "approval_required";

/// Deny code returned for an MCP call that tries to resolve a pending
/// execution.
pub const SELF_APPROVAL_FORBIDDEN_CODE: &str = "self_approval_forbidden";

/// Tools that resolve pending executions. They are denied to every MCP
/// client whatever its policies say: approvals are resolved by a person in
/// the DBFlux UI, never by the agent whose calls are waiting.
pub const HUMAN_ONLY_APPROVAL_TOOLS: &[&str] = &["approve_execution", "reject_execution"];

const SELF_APPROVAL_FORBIDDEN_REASON: &str = "MCP clients cannot approve or reject pending executions; a person resolves them in DBFlux (Pending Approvals)";

const APPROVAL_REQUIRED_REASON: &str = "classification requires approval by policy";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationOutcome {
    pub allowed: bool,
    pub deny_code: Option<&'static str>,
    pub deny_reason: Option<String>,
    /// Correlation ID shared with the execution audit event.
    pub correlation_id: Option<String>,
    /// Actor (client) ID for the execution audit event.
    pub actor_id: String,
    /// Pending execution created for this call when a policy requires
    /// approval and the call was queued.
    pub pending_execution_id: Option<String>,
    /// Approved pending execution this call consumed to run.
    pub approved_execution_id: Option<String>,
}

#[derive(Debug, Error)]
pub enum AuthorizationError {
    #[error("policy evaluation failed: {0}")]
    Policy(#[from] PolicyEngineError),
    #[error("audit record failed: {0}")]
    AuditRecord(#[from] dbflux_audit::AuditError),
    #[error("approval flow failed: {0}")]
    Approval(String),
}

/// How a call that a policy sends to approval was resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalResolution {
    /// A person already approved this exact call; the approval was consumed
    /// and the call may run once.
    Granted { pending_id: String },
    /// The call was added to the pending execution queue.
    Queued { pending_id: String },
    /// The call only creates or reads approval-queue entries, so it runs
    /// without being queued itself.
    Exempt,
}

/// Connects authorization to the pending execution queue for calls whose
/// policy decision is `RequireApproval`.
pub trait ApprovalGate {
    fn resolve(
        &mut self,
        request: &AuthorizationRequest,
    ) -> Result<ApprovalResolution, AuthorizationError>;
}

/// Authorizes a request without an approval queue: a `RequireApproval`
/// decision is audited as pending and returned as not allowed with the
/// `approval_required` code, without creating a pending execution.
pub fn authorize_request(
    trusted_clients: &TrustedClientRegistry,
    policy_engine: &PolicyEngine,
    audit_service: &AuditService,
    request: &AuthorizationRequest,
    created_at_epoch_ms: i64,
) -> Result<AuthorizationOutcome, AuthorizationError> {
    authorize_request_with_approval(
        trusted_clients,
        policy_engine,
        audit_service,
        request,
        None,
        created_at_epoch_ms,
    )
}

/// Authorizes a request and routes a `RequireApproval` decision through
/// `approval_gate`: a consumed approval lets the call run, otherwise the call
/// is queued (or refused) and reported as not allowed.
///
/// Exactly one `mcp_authorize` audit event is recorded per call. Its outcome
/// is `success` for an allowed call (including one that consumed an
/// approval), `pending` for a queued call, and `failure` for a denied one.
/// `approve_execution` and `reject_execution` are denied before any policy
/// is consulted.
pub fn authorize_request_with_approval(
    trusted_clients: &TrustedClientRegistry,
    policy_engine: &PolicyEngine,
    audit_service: &AuditService,
    request: &AuthorizationRequest,
    approval_gate: Option<&mut dyn ApprovalGate>,
    created_at_epoch_ms: i64,
) -> Result<AuthorizationOutcome, AuthorizationError> {
    // Use existing correlation_id or generate one for this request.
    let correlation_id = request
        .correlation_id
        .clone()
        .unwrap_or_else(new_correlation_id);

    let origin = EventOrigin::mcp();
    let severity = EventSeverity::Info;

    if HUMAN_ONLY_APPROVAL_TOOLS.contains(&request.tool_id.as_str()) {
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
        .with_error(SELF_APPROVAL_FORBIDDEN_CODE, SELF_APPROVAL_FORBIDDEN_REASON)
        .with_details_json(
            serde_json::json!({
                "classification": format!("{:?}", request.classification),
            })
            .to_string(),
        );

        audit_service.record(event)?;

        return Ok(AuthorizationOutcome {
            allowed: false,
            deny_code: Some(SELF_APPROVAL_FORBIDDEN_CODE),
            deny_reason: Some(SELF_APPROVAL_FORBIDDEN_REASON.to_string()),
            correlation_id: Some(correlation_id),
            actor_id: request.identity.client_id.clone(),
            pending_execution_id: None,
            approved_execution_id: None,
        });
    }

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
            pending_execution_id: None,
            approved_execution_id: None,
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
            pending_execution_id: None,
            approved_execution_id: None,
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
                pending_execution_id: None,
                approved_execution_id: None,
            })
        }
        PolicyDecision::RequireApproval => resolve_approval(
            audit_service,
            request,
            approval_gate,
            created_at_epoch_ms,
            &correlation_id,
        ),
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
                pending_execution_id: None,
                approved_execution_id: None,
            })
        }
    }
}

/// Resolves a `RequireApproval` decision and records its authorization event.
fn resolve_approval(
    audit_service: &AuditService,
    request: &AuthorizationRequest,
    approval_gate: Option<&mut dyn ApprovalGate>,
    created_at_epoch_ms: i64,
    correlation_id: &str,
) -> Result<AuthorizationOutcome, AuthorizationError> {
    let resolution = match approval_gate {
        Some(gate) => Some(gate.resolve(request)?),
        None => None,
    };

    let classification = format!("{:?}", request.classification);

    let (outcome_kind, details, outcome) = match resolution {
        Some(ApprovalResolution::Granted { pending_id }) => (
            EventOutcome::Success,
            serde_json::json!({
                "classification": classification,
                "decision": "approved",
                "pending_execution_id": pending_id,
            }),
            AuthorizationOutcome {
                allowed: true,
                deny_code: None,
                deny_reason: None,
                correlation_id: Some(correlation_id.to_string()),
                actor_id: request.identity.client_id.clone(),
                pending_execution_id: None,
                approved_execution_id: Some(pending_id),
            },
        ),
        Some(ApprovalResolution::Queued { pending_id }) => (
            EventOutcome::Pending,
            serde_json::json!({
                "classification": classification,
                "decision": "approval_required",
                "pending_execution_id": pending_id,
            }),
            AuthorizationOutcome {
                allowed: false,
                deny_code: Some(APPROVAL_REQUIRED_CODE),
                deny_reason: Some(format!(
                    "{APPROVAL_REQUIRED_REASON}; queued as pending execution {pending_id}"
                )),
                correlation_id: Some(correlation_id.to_string()),
                actor_id: request.identity.client_id.clone(),
                pending_execution_id: Some(pending_id),
                approved_execution_id: None,
            },
        ),
        Some(ApprovalResolution::Exempt) => (
            EventOutcome::Success,
            serde_json::json!({
                "classification": classification,
                "decision": "approval_queue_tool",
            }),
            AuthorizationOutcome {
                allowed: true,
                deny_code: None,
                deny_reason: None,
                correlation_id: Some(correlation_id.to_string()),
                actor_id: request.identity.client_id.clone(),
                pending_execution_id: None,
                approved_execution_id: None,
            },
        ),
        None => (
            EventOutcome::Pending,
            serde_json::json!({
                "classification": classification,
                "decision": "approval_required",
            }),
            AuthorizationOutcome {
                allowed: false,
                deny_code: Some(APPROVAL_REQUIRED_CODE),
                deny_reason: Some(APPROVAL_REQUIRED_REASON.to_string()),
                correlation_id: Some(correlation_id.to_string()),
                actor_id: request.identity.client_id.clone(),
                pending_execution_id: None,
                approved_execution_id: None,
            },
        ),
    };

    let mut event = build_authorization_event(
        created_at_epoch_ms,
        EventSeverity::Info,
        &request.identity.client_id,
        &request.connection_id,
        &request.tool_id,
        request.classification,
        outcome_kind,
        correlation_id,
        EventOrigin::mcp(),
    )
    .with_details_json(details.to_string());

    if let (Some(code), Some(reason)) = (outcome.deny_code, outcome.deny_reason.as_deref()) {
        event = event.with_error(code, reason);
    }

    audit_service.record(event)?;

    Ok(outcome)
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
