use dbflux_approval::{ApprovalError, ApprovalService, ExecutionPlan, PendingExecution};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ApprovalHandlerError {
    #[error("invalid pending execution id: {0}")]
    InvalidPendingId(String),
    #[error(transparent)]
    Approval(#[from] ApprovalError),
}

pub fn request_execution(
    approval_service: &mut ApprovalService,
    plan: &ExecutionPlan,
) -> PendingExecution {
    approval_service.request_execution(plan)
}

pub fn approve_execution(
    approval_service: &mut ApprovalService,
    pending_id: &str,
) -> Result<ExecutionPlan, ApprovalHandlerError> {
    let pending_id = Uuid::parse_str(pending_id)
        .map_err(|_| ApprovalHandlerError::InvalidPendingId(pending_id.to_string()))?;

    let approved = approval_service.approve(pending_id)?;
    Ok(approved.replay_plan)
}

pub fn reject_execution(
    approval_service: &mut ApprovalService,
    pending_id: &str,
) -> Result<(), ApprovalHandlerError> {
    let pending_id = Uuid::parse_str(pending_id)
        .map_err(|_| ApprovalHandlerError::InvalidPendingId(pending_id.to_string()))?;

    approval_service.reject(pending_id)?;
    Ok(())
}
