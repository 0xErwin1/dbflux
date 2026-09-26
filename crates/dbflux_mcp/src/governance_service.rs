use dbflux_policy::{ConnectionPolicyAssignment, ExecutionClassification, PolicyRole, ToolPolicy};
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
pub struct PolicyRoleDto {
    pub id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub policy_ids: Vec<String>,
}

impl From<PolicyRole> for PolicyRoleDto {
    fn from(role: PolicyRole) -> Self {
        Self {
            id: role.id,
            policy_ids: role.policy_ids,
        }
    }
}

impl From<PolicyRoleDto> for PolicyRole {
    fn from(dto: PolicyRoleDto) -> Self {
        Self {
            id: dto.id,
            policy_ids: dto.policy_ids,
        }
    }
}

/// A tool policy as exchanged with the UI and persistence. A class listed in
/// `allowed_classes` is Allow, a class listed in `approval_classes` requires
/// approval (Ask), and a class in neither is Deny.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolPolicyDto {
    pub id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_classes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub approval_classes: Vec<String>,
}

/// The persisted id of an execution class, as stored in policies.
pub fn execution_class_id(classification: ExecutionClassification) -> &'static str {
    match classification {
        ExecutionClassification::Metadata => "metadata",
        ExecutionClassification::Read => "read",
        ExecutionClassification::Write => "write",
        ExecutionClassification::Destructive => "destructive",
        ExecutionClassification::Admin => "admin",
        ExecutionClassification::AdminSafe => "admin_safe",
        ExecutionClassification::AdminDestructive => "admin_destructive",
    }
}

/// Parses a persisted execution class id.
pub fn parse_execution_class_id(id: &str) -> Result<ExecutionClassification, String> {
    match id {
        "read" => Ok(ExecutionClassification::Read),
        "write" => Ok(ExecutionClassification::Write),
        "destructive" => Ok(ExecutionClassification::Destructive),
        "admin" => Ok(ExecutionClassification::Admin),
        "metadata" => Ok(ExecutionClassification::Metadata),
        "admin_safe" => Ok(ExecutionClassification::AdminSafe),
        "admin_destructive" => Ok(ExecutionClassification::AdminDestructive),
        _ => Err(format!("invalid classification: {}", id)),
    }
}

impl From<ToolPolicy> for ToolPolicyDto {
    fn from(policy: ToolPolicy) -> Self {
        let class_ids = |classes: &[ExecutionClassification]| {
            classes
                .iter()
                .map(|class| execution_class_id(*class).to_string())
                .collect()
        };

        Self {
            allowed_classes: class_ids(&policy.allowed_classes),
            approval_classes: class_ids(&policy.approval_classes),
            id: policy.id,
            allowed_tools: policy.allowed_tools,
        }
    }
}

impl TryFrom<ToolPolicyDto> for ToolPolicy {
    type Error = String;

    fn try_from(dto: ToolPolicyDto) -> Result<Self, Self::Error> {
        let parse_all = |ids: &[String]| {
            ids.iter()
                .map(|id| parse_execution_class_id(id))
                .collect::<Result<Vec<_>, _>>()
        };

        Ok(Self {
            allowed_classes: parse_all(&dto.allowed_classes)?,
            approval_classes: parse_all(&dto.approval_classes)?,
            id: dto.id,
            allowed_tools: dto.allowed_tools,
        })
    }
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

/// Lightweight outcome returned when an approval or rejection completes.
/// Replaces the former `AuditEntry` in approve/reject return positions so
/// that the governance trait no longer depends on audit-query DTOs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalOutcome {
    pub id: String,
    pub status: String,
    pub actor_id: String,
    pub timestamp_ms: i64,
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

    fn list_roles(&self) -> Result<Vec<PolicyRoleDto>, GovernanceError>;

    fn list_policies(&self) -> Result<Vec<ToolPolicyDto>, GovernanceError>;

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

    fn approve_pending_execution(
        &self,
        pending_id: &str,
    ) -> Result<ApprovalOutcome, GovernanceError>;

    /// Rejects a pending execution. `reason` is returned to the requesting
    /// agent and recorded in the audit event.
    fn reject_pending_execution(
        &self,
        pending_id: &str,
        reason: Option<&str>,
    ) -> Result<ApprovalOutcome, GovernanceError>;
}
