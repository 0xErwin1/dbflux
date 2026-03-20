use dbflux_policy::ExecutionClassification;
use serde::{Deserialize, Serialize};
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
    Approved,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingExecution {
    pub id: Uuid,
    pub status: PendingStatus,
    pub plan: ExecutionPlan,
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryPendingExecutionStore {
    entries: Vec<PendingExecution>,
}

impl InMemoryPendingExecutionStore {
    pub fn create_pending(&mut self, plan: &ExecutionPlan) -> PendingExecution {
        let pending = PendingExecution {
            id: Uuid::new_v4(),
            status: PendingStatus::Pending,
            plan: plan.clone(),
        };

        self.entries.push(pending.clone());
        pending
    }

    pub fn get_pending(&self, pending_id: Uuid) -> Option<PendingExecution> {
        self.entries
            .iter()
            .find(|entry| entry.id == pending_id)
            .cloned()
    }

    pub fn update_status(
        &mut self,
        pending_id: Uuid,
        status: PendingStatus,
    ) -> Option<PendingExecution> {
        let pending = self
            .entries
            .iter_mut()
            .find(|entry| entry.id == pending_id)?;
        pending.status = status;
        Some(pending.clone())
    }

    pub fn list_pending(&self) -> Vec<PendingExecution> {
        self.entries
            .iter()
            .filter(|entry| entry.status == PendingStatus::Pending)
            .cloned()
            .collect()
    }
}
