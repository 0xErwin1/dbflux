use dbflux_approval::{
    ApprovalError, ApprovalService, ApprovedExecution, ExecutionPlan, PendingExecution,
    RejectedExecution,
};
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
) -> Result<ApprovedExecution, ApprovalHandlerError> {
    let pending_id = Uuid::parse_str(pending_id)
        .map_err(|_| ApprovalHandlerError::InvalidPendingId(pending_id.to_string()))?;

    approval_service.approve(pending_id).map_err(Into::into)
}

pub fn get_pending_execution(
    approval_service: &ApprovalService,
    pending_id: &str,
) -> Result<PendingExecution, ApprovalHandlerError> {
    let pending_id = Uuid::parse_str(pending_id)
        .map_err(|_| ApprovalHandlerError::InvalidPendingId(pending_id.to_string()))?;

    approval_service
        .list_pending()
        .into_iter()
        .find(|pending| pending.id == pending_id)
        .ok_or(ApprovalError::PendingNotFound(pending_id).into())
}

pub fn reject_execution(
    approval_service: &mut ApprovalService,
    pending_id: &str,
) -> Result<RejectedExecution, ApprovalHandlerError> {
    let pending_id = Uuid::parse_str(pending_id)
        .map_err(|_| ApprovalHandlerError::InvalidPendingId(pending_id.to_string()))?;

    approval_service.reject(pending_id).map_err(Into::into)
}
