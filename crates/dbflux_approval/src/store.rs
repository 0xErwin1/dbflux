use dbflux_policy::ExecutionClassification;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub connection_id: String,
    pub actor_id: String,
    pub tool_id: String,
    pub classification: ExecutionClassification,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingStatus {
    Pending,
    /// Approved and not yet used: the next identical call may run once.
    Approved,
    Rejected,
    /// Approved and already used by the call it authorized.
    Consumed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingExecution {
    pub id: Uuid,
    pub status: PendingStatus,
    pub plan: ExecutionPlan,
    pub created_at: i64,
    pub expires_at: Option<i64>,
}

#[derive(Debug, Error)]
pub enum PendingStoreError {
    #[error("pending store backend error: {0}")]
    Backend(String),
    #[error("failed to (de)serialize pending payload: {0}")]
    Serialization(String),
}

/// Pluggable storage for pending executions awaiting approval.
///
/// All methods are fallible so that backend failures (SQLite I/O, serde) surface
/// explicitly rather than being swallowed. Implementations must be `Send + Sync` so
/// they can be placed inside `Box<dyn PendingExecutionStore>` and shared across
/// async runtimes that require `Sync` (e.g. `rmcp`).
pub trait PendingExecutionStore: Send + Sync {
    fn create_pending(
        &mut self,
        plan: &ExecutionPlan,
        expires_at: Option<i64>,
    ) -> Result<PendingExecution, PendingStoreError>;

    fn get_pending(&self, id: Uuid) -> Result<Option<PendingExecution>, PendingStoreError>;

    fn update_status(
        &mut self,
        id: Uuid,
        status: PendingStatus,
    ) -> Result<Option<PendingExecution>, PendingStoreError>;

    /// Returns only entries whose status is `Pending` AND whose `expires_at` is
    /// either absent or in the future relative to the current wall-clock time.
    fn list_pending(&self) -> Result<Vec<PendingExecution>, PendingStoreError>;

    /// Finds the oldest approved, unexpired entry whose actor, connection,
    /// tool and payload equal `plan`'s and marks it `Consumed`, so one approval
    /// authorizes exactly one call. The classification is not compared: an
    /// approval requested through `request_execution` carries the escalated
    /// class of that tool, not the class of the call it approves.
    fn consume_approved(
        &mut self,
        plan: &ExecutionPlan,
    ) -> Result<Option<PendingExecution>, PendingStoreError>;

    /// Removes rows that are rejected or consumed OR whose `expires_at` is at
    /// or before `now_ms`. Approved rows that have not expired are kept,
    /// because they still authorize the call they were approved for. Call
    /// once at startup to prevent unbounded table growth.
    fn purge_terminal_and_expired(&mut self, now_ms: i64) -> Result<usize, PendingStoreError>;
}

/// Whether an approved entry authorizes `plan`: same actor, connection,
/// tool and payload.
pub fn approval_matches_plan(approved: &ExecutionPlan, plan: &ExecutionPlan) -> bool {
    approved.actor_id == plan.actor_id
        && approved.connection_id == plan.connection_id
        && approved.tool_id == plan.tool_id
        && approved.payload == plan.payload
}

fn now_epoch_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryPendingExecutionStore {
    entries: Vec<PendingExecution>,
}

impl PendingExecutionStore for InMemoryPendingExecutionStore {
    fn create_pending(
        &mut self,
        plan: &ExecutionPlan,
        expires_at: Option<i64>,
    ) -> Result<PendingExecution, PendingStoreError> {
        let pending = PendingExecution {
            id: Uuid::new_v4(),
            status: PendingStatus::Pending,
            plan: plan.clone(),
            created_at: now_epoch_ms(),
            expires_at,
        };

        self.entries.push(pending.clone());
        Ok(pending)
    }

    fn get_pending(&self, id: Uuid) -> Result<Option<PendingExecution>, PendingStoreError> {
        let now = now_epoch_ms();
        Ok(self
            .entries
            .iter()
            .find(|entry| {
                entry.id == id
                    && entry.status == PendingStatus::Pending
                    && entry.expires_at.is_none_or(|exp| exp > now)
            })
            .cloned())
    }

    fn update_status(
        &mut self,
        id: Uuid,
        status: PendingStatus,
    ) -> Result<Option<PendingExecution>, PendingStoreError> {
        let entry = self.entries.iter_mut().find(|entry| entry.id == id);
        match entry {
            Some(pending) => {
                pending.status = status;
                Ok(Some(pending.clone()))
            }
            None => Ok(None),
        }
    }

    fn list_pending(&self) -> Result<Vec<PendingExecution>, PendingStoreError> {
        let now = now_epoch_ms();
        Ok(self
            .entries
            .iter()
            .filter(|entry| {
                entry.status == PendingStatus::Pending
                    && entry.expires_at.is_none_or(|exp| exp > now)
            })
            .cloned()
            .collect())
    }

    fn consume_approved(
        &mut self,
        plan: &ExecutionPlan,
    ) -> Result<Option<PendingExecution>, PendingStoreError> {
        let now = now_epoch_ms();

        let entry = self
            .entries
            .iter_mut()
            .filter(|entry| {
                entry.status == PendingStatus::Approved
                    && entry.expires_at.is_none_or(|exp| exp > now)
                    && approval_matches_plan(&entry.plan, plan)
            })
            .min_by_key(|entry| entry.created_at);

        Ok(entry.map(|entry| {
            entry.status = PendingStatus::Consumed;
            entry.clone()
        }))
    }

    fn purge_terminal_and_expired(&mut self, now_ms: i64) -> Result<usize, PendingStoreError> {
        let before = self.entries.len();
        self.entries.retain(|entry| {
            let is_finished = matches!(
                entry.status,
                PendingStatus::Rejected | PendingStatus::Consumed
            );
            let is_expired = entry.expires_at.is_some_and(|exp| exp <= now_ms);
            !is_finished && !is_expired
        });
        Ok(before - self.entries.len())
    }
}

#[cfg(test)]
mod tests {
    use dbflux_policy::ExecutionClassification;

    use super::{
        ExecutionPlan, InMemoryPendingExecutionStore, PendingExecutionStore, PendingStatus,
    };

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
    fn create_pending_populates_created_at_and_id() {
        let mut store = InMemoryPendingExecutionStore::default();
        let entry = store
            .create_pending(&sample_plan(), None)
            .expect("create should succeed");

        assert_eq!(entry.status, PendingStatus::Pending);
        assert!(entry.created_at > 0, "created_at must be a real epoch ms");
        assert!(entry.expires_at.is_none());
    }

    #[test]
    fn list_pending_excludes_expired_entries() {
        let mut store = InMemoryPendingExecutionStore::default();

        let past_ms = 1_000i64;
        store
            .create_pending(&sample_plan(), Some(past_ms))
            .expect("create should succeed");

        let pending = store.list_pending().expect("list should succeed");
        assert!(
            pending.is_empty(),
            "expired entry must not appear in list_pending"
        );
    }

    #[test]
    fn list_pending_includes_non_expired_entries() {
        let mut store = InMemoryPendingExecutionStore::default();

        let far_future_ms = i64::MAX;
        store
            .create_pending(&sample_plan(), Some(far_future_ms))
            .expect("create should succeed");

        let pending = store.list_pending().expect("list should succeed");
        assert_eq!(
            pending.len(),
            1,
            "non-expired entry must appear in list_pending"
        );
    }

    #[test]
    fn update_status_round_trips() {
        let mut store = InMemoryPendingExecutionStore::default();
        let entry = store
            .create_pending(&sample_plan(), None)
            .expect("create should succeed");

        let updated = store
            .update_status(entry.id, PendingStatus::Approved)
            .expect("update should succeed")
            .expect("should find the entry");

        assert_eq!(updated.status, PendingStatus::Approved);
    }

    #[test]
    fn get_pending_returns_none_for_unknown_id() {
        let store = InMemoryPendingExecutionStore::default();
        let result = store
            .get_pending(uuid::Uuid::new_v4())
            .expect("get should succeed");
        assert!(result.is_none());
    }

    // S1: get_pending must return None for expired entries.
    #[test]
    fn get_pending_returns_none_for_expired_entry() {
        let mut store = InMemoryPendingExecutionStore::default();
        let entry = store
            .create_pending(&sample_plan(), Some(1_000i64))
            .expect("create should succeed");

        let result = store.get_pending(entry.id).expect("get should not error");
        assert!(
            result.is_none(),
            "expired entry must not be returned by get_pending"
        );
    }

    // S1: get_pending must return None for terminal (approved/rejected) entries.
    #[test]
    fn get_pending_returns_none_for_terminal_entry() {
        let mut store = InMemoryPendingExecutionStore::default();
        let entry = store
            .create_pending(&sample_plan(), None)
            .expect("create should succeed");

        store
            .update_status(entry.id, PendingStatus::Approved)
            .expect("update should succeed");

        let result = store.get_pending(entry.id).expect("get should not error");
        assert!(
            result.is_none(),
            "approved entry must not be returned by get_pending"
        );
    }

    #[test]
    fn consume_approved_uses_a_matching_approval_once() {
        let mut store = InMemoryPendingExecutionStore::default();
        let entry = store
            .create_pending(&sample_plan(), None)
            .expect("create should succeed");
        store
            .update_status(entry.id, PendingStatus::Approved)
            .expect("update should succeed");

        let consumed = store
            .consume_approved(&sample_plan())
            .expect("consume should succeed")
            .expect("the approved entry should match");

        assert_eq!(consumed.id, entry.id);
        assert_eq!(consumed.status, PendingStatus::Consumed);
        assert!(
            store
                .consume_approved(&sample_plan())
                .expect("consume should succeed")
                .is_none(),
            "an approval must authorize exactly one call"
        );
    }

    #[test]
    fn consume_approved_ignores_pending_rejected_and_different_payloads() {
        let mut store = InMemoryPendingExecutionStore::default();
        store
            .create_pending(&sample_plan(), None)
            .expect("create should succeed");
        let rejected = store
            .create_pending(&sample_plan(), None)
            .expect("create should succeed");
        store
            .update_status(rejected.id, PendingStatus::Rejected)
            .expect("update should succeed");

        let mut other_payload = sample_plan();
        other_payload.payload = serde_json::json!({"query": "drop table users"});
        let approved_other = store
            .create_pending(&other_payload, None)
            .expect("create should succeed");
        store
            .update_status(approved_other.id, PendingStatus::Approved)
            .expect("update should succeed");

        assert!(
            store
                .consume_approved(&sample_plan())
                .expect("consume should succeed")
                .is_none()
        );
    }

    #[test]
    fn consume_approved_ignores_classification_and_expired_approvals() {
        let mut store = InMemoryPendingExecutionStore::default();

        let expired = store
            .create_pending(&sample_plan(), Some(1_000i64))
            .expect("create should succeed");
        store
            .update_status(expired.id, PendingStatus::Approved)
            .expect("update should succeed");
        assert!(
            store
                .consume_approved(&sample_plan())
                .expect("consume should succeed")
                .is_none(),
            "an expired approval must not authorize a call"
        );

        let mut escalated = sample_plan();
        escalated.classification = ExecutionClassification::Admin;
        let approved = store
            .create_pending(&escalated, None)
            .expect("create should succeed");
        store
            .update_status(approved.id, PendingStatus::Approved)
            .expect("update should succeed");

        let consumed = store
            .consume_approved(&sample_plan())
            .expect("consume should succeed")
            .expect("classification is not part of the match");
        assert_eq!(consumed.id, approved.id);
    }

    #[test]
    fn purge_keeps_live_approvals_and_drops_finished_entries() {
        let mut store = InMemoryPendingExecutionStore::default();
        let approved = store
            .create_pending(&sample_plan(), None)
            .expect("create should succeed");
        store
            .update_status(approved.id, PendingStatus::Approved)
            .expect("update should succeed");
        let rejected = store
            .create_pending(&sample_plan(), None)
            .expect("create should succeed");
        store
            .update_status(rejected.id, PendingStatus::Rejected)
            .expect("update should succeed");
        let consumed = store
            .create_pending(&sample_plan(), None)
            .expect("create should succeed");
        store
            .update_status(consumed.id, PendingStatus::Consumed)
            .expect("update should succeed");

        let purged = store
            .purge_terminal_and_expired(now_epoch_ms_for_tests())
            .expect("purge should succeed");

        assert_eq!(purged, 2);
        assert!(
            store
                .consume_approved(&sample_plan())
                .expect("consume should succeed")
                .is_some(),
            "the live approval must survive the purge"
        );
    }

    fn now_epoch_ms_for_tests() -> i64 {
        super::now_epoch_ms()
    }
}
