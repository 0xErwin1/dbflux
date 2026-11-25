use crate::store::{PendingExecution, PendingStatus};

/// Where a stored pending execution stands at a point in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalOutcome {
    Pending,
    Approved,
    Rejected,
    /// Never decided, or approved, and its time to live has passed.
    Expired,
    /// Already used by the call it authorized.
    Consumed,
}

/// Classifies `execution` at `now_ms`. Expiry wins over `Pending` and
/// `Approved`: an approval nobody used in time no longer authorizes a call.
pub fn classify_execution(execution: &PendingExecution, now_ms: i64) -> ApprovalOutcome {
    let expired = execution
        .expires_at
        .is_some_and(|expires| expires <= now_ms);

    match execution.status {
        PendingStatus::Rejected => ApprovalOutcome::Rejected,
        PendingStatus::Consumed => ApprovalOutcome::Consumed,
        PendingStatus::Pending | PendingStatus::Approved if expired => ApprovalOutcome::Expired,
        PendingStatus::Pending => ApprovalOutcome::Pending,
        PendingStatus::Approved => ApprovalOutcome::Approved,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::ExecutionPlan;
    use dbflux_policy::ExecutionClassification;

    fn execution(status: PendingStatus, expires_at: Option<i64>) -> PendingExecution {
        PendingExecution {
            id: uuid::Uuid::new_v4(),
            status,
            plan: ExecutionPlan {
                connection_id: "c".into(),
                actor_id: "a".into(),
                tool_id: "t".into(),
                classification: ExecutionClassification::Write,
                payload: serde_json::Value::Null,
            },
            created_at: 0,
            expires_at,
            rejection_reason: None,
        }
    }

    #[test]
    fn classifies_every_status_and_lets_expiry_win_over_pending_and_approved() {
        let at = |status, expires| classify_execution(&execution(status, expires), 100);

        assert_eq!(
            at(PendingStatus::Pending, Some(200)),
            ApprovalOutcome::Pending
        );
        assert_eq!(at(PendingStatus::Pending, None), ApprovalOutcome::Pending);
        assert_eq!(
            at(PendingStatus::Pending, Some(100)),
            ApprovalOutcome::Expired
        );
        assert_eq!(
            at(PendingStatus::Approved, Some(200)),
            ApprovalOutcome::Approved
        );
        assert_eq!(
            at(PendingStatus::Approved, Some(50)),
            ApprovalOutcome::Expired
        );
        assert_eq!(
            at(PendingStatus::Rejected, Some(50)),
            ApprovalOutcome::Rejected
        );
        assert_eq!(
            at(PendingStatus::Consumed, Some(50)),
            ApprovalOutcome::Consumed
        );
    }
}
