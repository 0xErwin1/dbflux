use std::collections::HashMap;

use dbflux_approval::{ApprovalService, ExecutionPlan, InMemoryPendingExecutionStore};
use dbflux_audit::AuditService;
use dbflux_policy::{
    ConnectionPolicyAssignment, ExecutionClassification, TrustedClient, TrustedClientRegistry,
};

use crate::governance_service::{
    AuditEntry, AuditExportFormat, AuditQuery, ConnectionPolicyAssignmentDto, GovernanceError,
    McpGovernanceService, PendingExecutionDetail, PendingExecutionSummary, TrustedClientDto,
};
use crate::handlers::{approval as approval_handler, audit as audit_handler};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpRuntimeEvent {
    TrustedClientsUpdated,
    ConnectionPolicyUpdated { connection_id: String },
    PendingExecutionsUpdated,
    AuditAppended,
}

pub struct McpRuntime {
    trusted_clients: HashMap<String, TrustedClientDto>,
    connection_policy_assignments: HashMap<String, ConnectionPolicyAssignmentDto>,
    approval_service: ApprovalService,
    audit_service: AuditService,
    pending_events: Vec<McpRuntimeEvent>,
}

impl McpRuntime {
    pub fn new(audit_service: AuditService) -> Self {
        Self {
            trusted_clients: HashMap::new(),
            connection_policy_assignments: HashMap::new(),
            approval_service: ApprovalService::new(InMemoryPendingExecutionStore::default()),
            audit_service,
            pending_events: Vec::new(),
        }
    }

    pub fn trusted_client_registry(&self) -> TrustedClientRegistry {
        let clients = self
            .trusted_clients
            .values()
            .cloned()
            .map(|client| TrustedClient {
                id: client.id,
                name: client.name,
                issuer: client.issuer,
                active: client.active,
            })
            .collect();

        TrustedClientRegistry::new(clients)
    }

    pub fn drain_events(&mut self) -> Vec<McpRuntimeEvent> {
        std::mem::take(&mut self.pending_events)
    }

    fn push_event(&mut self, event: McpRuntimeEvent) {
        self.pending_events.push(event);
    }

    pub fn audit_service(&self) -> &AuditService {
        &self.audit_service
    }
}

impl McpGovernanceService for McpRuntime {
    fn list_trusted_clients(&self) -> Result<Vec<TrustedClientDto>, GovernanceError> {
        let mut clients: Vec<_> = self.trusted_clients.values().cloned().collect();
        clients.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(clients)
    }

    fn upsert_trusted_client(
        &self,
        _client: TrustedClientDto,
    ) -> Result<TrustedClientDto, GovernanceError> {
        Err(GovernanceError::Operation(
            "upsert_trusted_client requires mutable runtime access".to_string(),
        ))
    }

    fn delete_trusted_client(&self, _client_id: &str) -> Result<(), GovernanceError> {
        Err(GovernanceError::Operation(
            "delete_trusted_client requires mutable runtime access".to_string(),
        ))
    }

    fn list_connection_policy_assignments(
        &self,
    ) -> Result<Vec<ConnectionPolicyAssignmentDto>, GovernanceError> {
        let mut assignments: Vec<_> = self
            .connection_policy_assignments
            .values()
            .cloned()
            .collect();
        assignments.sort_by(|left, right| left.connection_id.cmp(&right.connection_id));
        Ok(assignments)
    }

    fn save_connection_policy_assignment(
        &self,
        _assignment: ConnectionPolicyAssignmentDto,
    ) -> Result<ConnectionPolicyAssignmentDto, GovernanceError> {
        Err(GovernanceError::Operation(
            "save_connection_policy_assignment requires mutable runtime access".to_string(),
        ))
    }

    fn list_pending_executions(&self) -> Result<Vec<PendingExecutionSummary>, GovernanceError> {
        let entries = self
            .approval_service
            .list_pending()
            .into_iter()
            .map(|pending| PendingExecutionSummary {
                id: pending.id.to_string(),
                actor_id: pending.plan.actor_id,
                connection_id: pending.plan.connection_id,
                tool_id: pending.plan.tool_id,
                classification: pending.plan.classification,
                status: format!("{:?}", pending.status).to_ascii_lowercase(),
                created_at_epoch_ms: 0,
            })
            .collect();

        Ok(entries)
    }

    fn get_pending_execution(
        &self,
        pending_id: &str,
    ) -> Result<PendingExecutionDetail, GovernanceError> {
        let pending_id = uuid::Uuid::parse_str(pending_id)
            .map_err(|_| GovernanceError::Validation("invalid pending id".to_string()))?;

        let pending = self
            .approval_service
            .list_pending()
            .into_iter()
            .find(|pending| pending.id == pending_id)
            .ok_or_else(|| GovernanceError::NotFound {
                resource: format!("pending execution {pending_id}"),
            })?;

        Ok(PendingExecutionDetail {
            summary: PendingExecutionSummary {
                id: pending.id.to_string(),
                actor_id: pending.plan.actor_id,
                connection_id: pending.plan.connection_id,
                tool_id: pending.plan.tool_id,
                classification: pending.plan.classification,
                status: format!("{:?}", pending.status).to_ascii_lowercase(),
                created_at_epoch_ms: 0,
            },
            plan: pending.plan.payload,
        })
    }

    fn approve_pending_execution(&self, _pending_id: &str) -> Result<AuditEntry, GovernanceError> {
        Err(GovernanceError::Operation(
            "approve_pending_execution requires mutable runtime access".to_string(),
        ))
    }

    fn reject_pending_execution(&self, _pending_id: &str) -> Result<AuditEntry, GovernanceError> {
        Err(GovernanceError::Operation(
            "reject_pending_execution requires mutable runtime access".to_string(),
        ))
    }

    fn query_audit_entries(&self, query: &AuditQuery) -> Result<Vec<AuditEntry>, GovernanceError> {
        let events = audit_handler::query_audit_logs(&self.audit_service, query)
            .map_err(|error| GovernanceError::Operation(error.to_string()))?;

        Ok(events
            .into_iter()
            .map(|event| AuditEntry {
                id: event.id.to_string(),
                actor_id: event.actor_id,
                tool_id: event.tool_id,
                decision: event.decision,
                reason: event.reason,
                created_at_epoch_ms: event.created_at_epoch_ms,
            })
            .collect())
    }

    fn export_audit_entries(
        &self,
        query: &AuditQuery,
        format: AuditExportFormat,
    ) -> Result<String, GovernanceError> {
        audit_handler::export_audit_logs(&self.audit_service, query, format)
            .map_err(|error| GovernanceError::Operation(error.to_string()))
    }
}

impl McpRuntime {
    pub fn upsert_trusted_client_mut(
        &mut self,
        client: TrustedClientDto,
    ) -> Result<TrustedClientDto, GovernanceError> {
        if client.id.trim().is_empty() {
            return Err(GovernanceError::Validation(
                "trusted client id must not be empty".to_string(),
            ));
        }

        self.trusted_clients
            .insert(client.id.clone(), client.clone());
        self.push_event(McpRuntimeEvent::TrustedClientsUpdated);
        Ok(client)
    }

    pub fn delete_trusted_client_mut(&mut self, client_id: &str) -> Result<(), GovernanceError> {
        if self.trusted_clients.remove(client_id).is_none() {
            return Err(GovernanceError::NotFound {
                resource: format!("trusted client {client_id}"),
            });
        }

        self.push_event(McpRuntimeEvent::TrustedClientsUpdated);
        Ok(())
    }

    pub fn save_connection_policy_assignment_mut(
        &mut self,
        assignment: ConnectionPolicyAssignmentDto,
    ) -> Result<ConnectionPolicyAssignmentDto, GovernanceError> {
        if assignment.connection_id.trim().is_empty() {
            return Err(GovernanceError::Validation(
                "connection id must not be empty".to_string(),
            ));
        }

        self.connection_policy_assignments
            .insert(assignment.connection_id.clone(), assignment.clone());
        self.push_event(McpRuntimeEvent::ConnectionPolicyUpdated {
            connection_id: assignment.connection_id.clone(),
        });

        Ok(assignment)
    }

    pub fn approve_pending_execution_mut(
        &mut self,
        pending_id: &str,
    ) -> Result<AuditEntry, GovernanceError> {
        let replay_plan =
            approval_handler::approve_execution(&mut self.approval_service, pending_id)
                .map_err(|error| GovernanceError::Operation(error.to_string()))?;

        let event = self
            .audit_service
            .append(
                &replay_plan.actor_id,
                "approve_execution",
                "allow",
                None,
                now_epoch_ms(),
            )
            .map_err(|error| GovernanceError::Operation(error.to_string()))?;

        self.push_event(McpRuntimeEvent::PendingExecutionsUpdated);
        self.push_event(McpRuntimeEvent::AuditAppended);

        Ok(AuditEntry {
            id: event.id.to_string(),
            actor_id: event.actor_id,
            tool_id: event.tool_id,
            decision: event.decision,
            reason: event.reason,
            created_at_epoch_ms: event.created_at_epoch_ms,
        })
    }

    pub fn reject_pending_execution_mut(
        &mut self,
        pending_id: &str,
    ) -> Result<AuditEntry, GovernanceError> {
        approval_handler::reject_execution(&mut self.approval_service, pending_id)
            .map_err(|error| GovernanceError::Operation(error.to_string()))?;

        let event = self
            .audit_service
            .append(
                "system",
                "reject_execution",
                "deny",
                Some("rejected by approver"),
                now_epoch_ms(),
            )
            .map_err(|error| GovernanceError::Operation(error.to_string()))?;

        self.push_event(McpRuntimeEvent::PendingExecutionsUpdated);
        self.push_event(McpRuntimeEvent::AuditAppended);

        Ok(AuditEntry {
            id: event.id.to_string(),
            actor_id: event.actor_id,
            tool_id: event.tool_id,
            decision: event.decision,
            reason: event.reason,
            created_at_epoch_ms: event.created_at_epoch_ms,
        })
    }

    pub fn request_execution_mut(&mut self, plan: ExecutionPlan) -> PendingExecutionSummary {
        let pending = approval_handler::request_execution(&mut self.approval_service, &plan);
        self.push_event(McpRuntimeEvent::PendingExecutionsUpdated);

        PendingExecutionSummary {
            id: pending.id.to_string(),
            actor_id: pending.plan.actor_id,
            connection_id: pending.plan.connection_id,
            tool_id: pending.plan.tool_id,
            classification: pending.plan.classification,
            status: format!("{:?}", pending.status).to_ascii_lowercase(),
            created_at_epoch_ms: now_epoch_ms(),
        }
    }

    pub fn policy_assignments_for_engine(&self) -> Vec<ConnectionPolicyAssignment> {
        self.connection_policy_assignments
            .values()
            .flat_map(|assignment| {
                assignment
                    .assignments
                    .iter()
                    .map(move |binding| ConnectionPolicyAssignment {
                        actor_id: binding.actor_id.clone(),
                        scope: dbflux_policy::PolicyBindingScope {
                            connection_id: assignment.connection_id.clone(),
                        },
                        role_ids: binding.role_ids.clone(),
                        policy_ids: binding.policy_ids.clone(),
                    })
            })
            .collect()
    }

    pub fn classify_plan(
        &self,
        classification: ExecutionClassification,
        payload: serde_json::Value,
        actor_id: String,
        connection_id: String,
        tool_id: String,
    ) -> ExecutionPlan {
        ExecutionPlan {
            connection_id,
            actor_id,
            tool_id,
            classification,
            payload,
        }
    }
}

fn now_epoch_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    duration.as_millis() as i64
}
