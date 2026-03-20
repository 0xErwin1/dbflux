use thiserror::Error;
use uuid::Uuid;

use crate::store::{ExecutionPlan, InMemoryPendingExecutionStore, PendingExecution, PendingStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalDecision {
    Approve,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovedExecution {
    pub pending: PendingExecution,
    pub replay_plan: ExecutionPlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedExecution {
    pub pending: PendingExecution,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ApprovalError {
    #[error("pending execution not found: {0}")]
    PendingNotFound(Uuid),
    #[error("pending execution is not in pending state: {0}")]
    InvalidTransition(Uuid),
}

#[derive(Debug, Clone, Default)]
pub struct ApprovalService {
    store: InMemoryPendingExecutionStore,
}

impl ApprovalService {
    pub fn new(store: InMemoryPendingExecutionStore) -> Self {
        Self { store }
    }

    pub fn request_execution(&mut self, plan: &ExecutionPlan) -> PendingExecution {
        self.store.create_pending(plan)
    }

    pub fn list_pending(&self) -> Vec<PendingExecution> {
        self.store.list_pending()
    }

    pub fn approve(&mut self, pending_id: Uuid) -> Result<ApprovedExecution, ApprovalError> {
        let pending = self
            .store
            .get_pending(pending_id)
            .ok_or(ApprovalError::PendingNotFound(pending_id))?;

        if pending.status != PendingStatus::Pending {
            return Err(ApprovalError::InvalidTransition(pending_id));
        }

        let replay_plan = pending.plan.clone();
        let updated = self
            .store
            .update_status(pending_id, PendingStatus::Approved)
            .ok_or(ApprovalError::PendingNotFound(pending_id))?;

        Ok(ApprovedExecution {
            pending: updated,
            replay_plan,
        })
    }

    pub fn reject(&mut self, pending_id: Uuid) -> Result<RejectedExecution, ApprovalError> {
        let pending = self
            .store
            .get_pending(pending_id)
            .ok_or(ApprovalError::PendingNotFound(pending_id))?;

        if pending.status != PendingStatus::Pending {
            return Err(ApprovalError::InvalidTransition(pending_id));
        }

        let updated = self
            .store
            .update_status(pending_id, PendingStatus::Rejected)
            .ok_or(ApprovalError::PendingNotFound(pending_id))?;

        Ok(RejectedExecution { pending: updated })
    }
}

#[cfg(test)]
mod tests {
    use dbflux_policy::ExecutionClassification;

    use crate::store::{ExecutionPlan, InMemoryPendingExecutionStore, PendingStatus};

    use super::{ApprovalError, ApprovalService};

    fn sample_plan() -> ExecutionPlan {
        ExecutionPlan {
            connection_id: "conn-a".to_string(),
            actor_id: "alice".to_string(),
            tool_id: "request_execution".to_string(),
            classification: ExecutionClassification::Write,
            payload: serde_json::json!({"query": "update users set active = true"}),
        }
    }

    #[test]
    fn request_takes_snapshot_for_exact_replay() {
        let mut service = ApprovalService::new(InMemoryPendingExecutionStore::default());

        let mut mutable_plan = sample_plan();
        let pending = service.request_execution(&mutable_plan);
        mutable_plan.payload = serde_json::json!({"query": "drop table users"});

        let approved = service
            .approve(pending.id)
            .expect("approve should succeed for pending record");

        assert_eq!(approved.pending.status, PendingStatus::Approved);
        assert_eq!(
            approved.replay_plan.payload,
            serde_json::json!({"query": "update users set active = true"})
        );
    }

    #[test]
    fn reject_prevents_future_approval() {
        let mut service = ApprovalService::new(InMemoryPendingExecutionStore::default());

        let pending = service.request_execution(&sample_plan());
        service
            .reject(pending.id)
            .expect("reject should succeed for pending record");

        let result = service.approve(pending.id);
        assert_eq!(result, Err(ApprovalError::InvalidTransition(pending.id)));
    }
}
