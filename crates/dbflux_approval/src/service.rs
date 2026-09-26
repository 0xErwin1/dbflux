use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;
use uuid::Uuid;

use crate::store::{
    ExecutionPlan, PendingExecution, PendingExecutionStore, PendingStatus, PendingStoreError,
};

/// Default approval TTL: 24 hours in milliseconds.
pub const DEFAULT_APPROVAL_TTL_MS: i64 = 86_400_000;

/// Longest rejection reason kept, in characters. Longer reasons are truncated.
pub const MAX_REJECTION_REASON_CHARS: usize = 500;

/// Trims the reason a person typed when rejecting, drops it when blank and
/// caps it at [`MAX_REJECTION_REASON_CHARS`] characters.
pub fn normalize_rejection_reason(reason: Option<&str>) -> Option<String> {
    let trimmed = reason?.trim();

    if trimmed.is_empty() {
        return None;
    }

    Some(
        trimmed
            .chars()
            .take(MAX_REJECTION_REASON_CHARS)
            .collect::<String>()
            .trim_end()
            .to_string(),
    )
}

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

#[derive(Debug, Error)]
pub enum ApprovalError {
    #[error("pending execution not found: {0}")]
    PendingNotFound(Uuid),
    #[error("pending execution is not in pending state: {0}")]
    InvalidTransition(Uuid),
    #[error(transparent)]
    Store(#[from] PendingStoreError),
}

pub struct ApprovalService {
    store: Box<dyn PendingExecutionStore>,
    ttl_ms: i64,
}

impl ApprovalService {
    pub fn new(store: Box<dyn PendingExecutionStore>) -> Self {
        Self {
            store,
            ttl_ms: DEFAULT_APPROVAL_TTL_MS,
        }
    }

    /// Creates an `ApprovalService` with a custom TTL for pending executions.
    pub fn with_ttl(store: Box<dyn PendingExecutionStore>, ttl_ms: i64) -> Self {
        Self { store, ttl_ms }
    }

    /// Sets the TTL for newly created pending executions.
    pub fn set_ttl_ms(&mut self, ttl_ms: i64) {
        self.ttl_ms = ttl_ms;
    }

    pub fn request_execution(
        &mut self,
        plan: &ExecutionPlan,
    ) -> Result<PendingExecution, ApprovalError> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let expires_at = Some(now_ms + self.ttl_ms);
        Ok(self.store.create_pending(plan, expires_at)?)
    }

    pub fn list_pending(&self) -> Result<Vec<PendingExecution>, ApprovalError> {
        Ok(self.store.list_pending()?)
    }

    /// Returns the execution with `pending_id` whatever its status, so a
    /// requester can learn that it was rejected and why.
    pub fn get_execution(
        &self,
        pending_id: Uuid,
    ) -> Result<Option<PendingExecution>, ApprovalError> {
        Ok(self.store.get_execution(pending_id)?)
    }

    /// Uses the approval granted for `plan`, if one exists, so it cannot
    /// authorize a second call.
    pub fn consume_approved(
        &mut self,
        plan: &ExecutionPlan,
    ) -> Result<Option<PendingExecution>, ApprovalError> {
        Ok(self.store.consume_approved(plan)?)
    }

    /// Removes rejected, consumed and expired rows from the store.
    pub fn purge_terminal_and_expired(&mut self, now_ms: i64) -> Result<usize, ApprovalError> {
        Ok(self.store.purge_terminal_and_expired(now_ms)?)
    }

    pub fn approve(&mut self, pending_id: Uuid) -> Result<ApprovedExecution, ApprovalError> {
        let pending = self
            .store
            .get_pending(pending_id)?
            .ok_or(ApprovalError::PendingNotFound(pending_id))?;

        if pending.status != PendingStatus::Pending {
            return Err(ApprovalError::InvalidTransition(pending_id));
        }

        let replay_plan = pending.plan.clone();
        let updated = self
            .store
            .update_status(pending_id, PendingStatus::Approved)?
            .ok_or(ApprovalError::PendingNotFound(pending_id))?;

        Ok(ApprovedExecution {
            pending: updated,
            replay_plan,
        })
    }

    /// Rejects a pending execution. The reason is normalized with
    /// [`normalize_rejection_reason`] and stored so the requester can read it.
    pub fn reject(
        &mut self,
        pending_id: Uuid,
        reason: Option<&str>,
    ) -> Result<RejectedExecution, ApprovalError> {
        let pending = self
            .store
            .get_pending(pending_id)?
            .ok_or(ApprovalError::PendingNotFound(pending_id))?;

        if pending.status != PendingStatus::Pending {
            return Err(ApprovalError::InvalidTransition(pending_id));
        }

        let reason = normalize_rejection_reason(reason);
        let updated = self
            .store
            .record_rejection(pending_id, reason.as_deref())?
            .ok_or(ApprovalError::PendingNotFound(pending_id))?;

        Ok(RejectedExecution { pending: updated })
    }
}

#[cfg(test)]
mod tests {
    use dbflux_policy::ExecutionClassification;

    use crate::store::{ExecutionPlan, InMemoryPendingExecutionStore, PendingStatus};

    use super::ApprovalService;

    fn sample_plan() -> ExecutionPlan {
        ExecutionPlan {
            connection_id: "conn-a".to_string(),
            actor_id: "alice".to_string(),
            tool_id: "request_execution".to_string(),
            classification: ExecutionClassification::Write,
            payload: serde_json::json!({"query": "update users set active = true"}),
        }
    }

    fn service() -> ApprovalService {
        ApprovalService::new(Box::new(InMemoryPendingExecutionStore::default()))
    }

    #[test]
    fn request_takes_snapshot_for_exact_replay() {
        let mut service = service();

        let mut mutable_plan = sample_plan();
        let pending = service
            .request_execution(&mutable_plan)
            .expect("request_execution should succeed");
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
        let mut service = service();

        let pending = service
            .request_execution(&sample_plan())
            .expect("request_execution should succeed");
        service
            .reject(pending.id, None)
            .expect("reject should succeed for pending record");

        let result = service.approve(pending.id);
        assert!(
            matches!(
                result,
                Err(super::ApprovalError::InvalidTransition(_))
                    | Err(super::ApprovalError::PendingNotFound(_))
            ),
            "expected approval to fail for a rejected record, got {:?}",
            result
        );
    }

    // W2: request_execution must set a non-null expires_at approximately 24h ahead.
    #[test]
    fn request_execution_sets_expires_at_24h_ahead() {
        use std::time::{SystemTime, UNIX_EPOCH};
        let mut service = service();

        let before_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let pending = service
            .request_execution(&sample_plan())
            .expect("request_execution should succeed");

        let after_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let expires = pending
            .expires_at
            .expect("expires_at must be set by request_execution");

        assert!(
            expires >= before_ms + super::DEFAULT_APPROVAL_TTL_MS,
            "expires_at must be at least 24h from before the call"
        );
        assert!(
            expires <= after_ms + super::DEFAULT_APPROVAL_TTL_MS,
            "expires_at must not be more than 24h from after the call"
        );
    }

    // S1: approving an expired pending id returns PendingNotFound.
    #[test]
    fn approve_expired_pending_returns_not_found() {
        let mut service = service();
        service.set_ttl_ms(1);

        let pending = service
            .request_execution(&sample_plan())
            .expect("request_execution should succeed");

        std::thread::sleep(std::time::Duration::from_millis(10));

        let result = service.approve(pending.id);
        assert!(
            matches!(result, Err(super::ApprovalError::PendingNotFound(_))),
            "expected PendingNotFound for expired entry, got {:?}",
            result
        );
    }

    // W2: a pending row with expires_at in the past is purged and not listed.
    #[test]
    fn purge_removes_expired_entries_from_list() {
        let store = InMemoryPendingExecutionStore::default();
        let mut service = ApprovalService::new(Box::new(store));

        // Force TTL of 1ms so the row expires almost immediately.
        service.set_ttl_ms(1);

        let _pending = service
            .request_execution(&sample_plan())
            .expect("request_execution should succeed");

        // Sleep briefly so the entry expires.
        std::thread::sleep(std::time::Duration::from_millis(10));

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let purged = service
            .purge_terminal_and_expired(now_ms)
            .expect("purge should succeed");

        assert_eq!(purged, 1, "one expired entry should have been purged");

        let listed = service.list_pending().expect("list should succeed");
        assert!(
            listed.is_empty(),
            "expired entry must not appear after purge"
        );
    }

    #[test]
    fn approved_plan_is_consumed_once_and_rejected_plan_never() {
        let mut service = service();

        let approved = service
            .request_execution(&sample_plan())
            .expect("request_execution should succeed");
        service
            .approve(approved.id)
            .expect("approve should succeed");

        let consumed = service
            .consume_approved(&sample_plan())
            .expect("consume should succeed")
            .expect("approved plan should be consumable");
        assert_eq!(consumed.id, approved.id);
        assert_eq!(consumed.status, PendingStatus::Consumed);
        assert!(
            service
                .consume_approved(&sample_plan())
                .expect("consume should succeed")
                .is_none()
        );

        let rejected = service
            .request_execution(&sample_plan())
            .expect("request_execution should succeed");
        service
            .reject(rejected.id, None)
            .expect("reject should succeed");
        assert!(
            service
                .consume_approved(&sample_plan())
                .expect("consume should succeed")
                .is_none(),
            "a rejected plan must never authorize a call"
        );
    }

    #[test]
    fn reject_stores_the_reason_and_keeps_the_entry_readable() {
        let mut service = service();

        let pending = service
            .request_execution(&sample_plan())
            .expect("request_execution should succeed");
        let rejected = service
            .reject(pending.id, Some("  touches production rows  "))
            .expect("reject should succeed");

        assert_eq!(rejected.pending.status, PendingStatus::Rejected);
        assert_eq!(
            rejected.pending.rejection_reason.as_deref(),
            Some("touches production rows")
        );

        let stored = service
            .get_execution(pending.id)
            .expect("get_execution should succeed")
            .expect("a rejected entry stays readable until it is purged");
        assert_eq!(stored.status, PendingStatus::Rejected);
        assert_eq!(
            stored.rejection_reason.as_deref(),
            Some("touches production rows")
        );
    }

    #[test]
    fn rejection_reason_is_dropped_when_blank_and_capped_when_long() {
        assert_eq!(super::normalize_rejection_reason(None), None);
        assert_eq!(super::normalize_rejection_reason(Some("   ")), None);

        let long_reason = "é".repeat(super::MAX_REJECTION_REASON_CHARS + 50);
        let normalized = super::normalize_rejection_reason(Some(&long_reason))
            .expect("a long reason is kept, truncated");
        assert_eq!(
            normalized.chars().count(),
            super::MAX_REJECTION_REASON_CHARS
        );
    }
}
