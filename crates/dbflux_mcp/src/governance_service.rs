use dbflux_policy::{ConnectionPolicyAssignment, ExecutionClassification};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedClientDto {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub issuer: Option<String>,
    #[serde(default)]
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionPolicyAssignmentDto {
    pub connection_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assignments: Vec<ConnectionPolicyAssignment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingExecutionSummary {
    pub id: String,
    pub actor_id: String,
    pub connection_id: String,
    pub tool_id: String,
    pub classification: ExecutionClassification,
    pub status: String,
    pub created_at_epoch_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingExecutionDetail {
    pub summary: PendingExecutionSummary,
    pub plan: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditQuery {
    #[serde(default)]
    pub actor_id: Option<String>,
    #[serde(default)]
    pub tool_id: Option<String>,
    #[serde(default)]
    pub decision: Option<String>,
    #[serde(default)]
    pub start_epoch_ms: Option<i64>,
    #[serde(default)]
    pub end_epoch_ms: Option<i64>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditExportFormat {
    Csv,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub id: String,
    pub actor_id: String,
    pub tool_id: String,
    pub decision: String,
    pub reason: Option<String>,
    pub created_at_epoch_ms: i64,
}

#[derive(Debug, Error)]
pub enum GovernanceError {
    #[error("resource not found: {resource}")]
    NotFound { resource: String },
    #[error("validation error: {0}")]
    Validation(String),
    #[error("operation failed: {0}")]
    Operation(String),
}

pub trait McpGovernanceService {
    fn list_trusted_clients(&self) -> Result<Vec<TrustedClientDto>, GovernanceError>;

    fn upsert_trusted_client(
        &self,
        client: TrustedClientDto,
    ) -> Result<TrustedClientDto, GovernanceError>;

    fn delete_trusted_client(&self, client_id: &str) -> Result<(), GovernanceError>;

    fn list_connection_policy_assignments(
        &self,
    ) -> Result<Vec<ConnectionPolicyAssignmentDto>, GovernanceError>;

    fn save_connection_policy_assignment(
        &self,
        assignment: ConnectionPolicyAssignmentDto,
    ) -> Result<ConnectionPolicyAssignmentDto, GovernanceError>;

    fn list_pending_executions(&self) -> Result<Vec<PendingExecutionSummary>, GovernanceError>;

    fn get_pending_execution(
        &self,
        pending_id: &str,
    ) -> Result<PendingExecutionDetail, GovernanceError>;

    fn approve_pending_execution(&self, pending_id: &str) -> Result<AuditEntry, GovernanceError>;

    fn reject_pending_execution(&self, pending_id: &str) -> Result<AuditEntry, GovernanceError>;

    fn query_audit_entries(&self, query: &AuditQuery) -> Result<Vec<AuditEntry>, GovernanceError>;

    fn export_audit_entries(
        &self,
        query: &AuditQuery,
        format: AuditExportFormat,
    ) -> Result<String, GovernanceError>;
}
