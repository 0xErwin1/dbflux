use std::collections::HashMap;

use dbflux_approval::{ApprovalService, ExecutionPlan, InMemoryPendingExecutionStore};
use dbflux_audit::AuditService;
use dbflux_core::observability::{
    EventCategory, EventOrigin, EventOutcome, EventRecord, EventSeverity,
    actions::{MCP_APPROVE_EXECUTION, MCP_REJECT_EXECUTION},
};
use dbflux_policy::{
    ConnectionPolicyAssignment, ExecutionClassification, PolicyRole, ToolPolicy,
    TrustedClientRegistry,
};

use crate::governance_service::{
    AuditEntry, AuditExportFormat, AuditQuery, ConnectionPolicyAssignmentDto, GovernanceError,
    McpGovernanceService, PendingExecutionDetail, PendingExecutionSummary, PolicyRoleDto,
    ToolPolicyDto, TrustedClientDto,
};
use crate::handlers::{approval as approval_handler, audit as audit_handler};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpRuntimeEvent {
    TrustedClientsUpdated,
    RolesUpdated,
    PoliciesUpdated,
    ConnectionPolicyUpdated { connection_id: String },
    PendingExecutionsUpdated,
    AuditAppended,
}

pub struct McpRuntime {
    trusted_clients: HashMap<String, TrustedClientDto>,
    roles: HashMap<String, PolicyRoleDto>,
    policies: HashMap<String, ToolPolicyDto>,
    connection_policy_assignments: HashMap<String, ConnectionPolicyAssignmentDto>,
    approval_service: ApprovalService,
    audit_service: AuditService,
    pending_events: Vec<McpRuntimeEvent>,
    mcp_enabled: bool,
}

impl McpRuntime {
    pub fn new(audit_service: AuditService) -> Self {
        Self {
            trusted_clients: HashMap::new(),
            roles: HashMap::new(),
            policies: HashMap::new(),
            connection_policy_assignments: HashMap::new(),
            approval_service: ApprovalService::new(InMemoryPendingExecutionStore::default()),
            audit_service,
            pending_events: Vec::new(),
            mcp_enabled: true,
        }
    }

    pub fn trusted_client_registry(&self) -> TrustedClientRegistry {
        let clients = self
            .trusted_clients
            .values()
            .cloned()
            .map(|client| dbflux_policy::TrustedClient {
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

    pub fn set_mcp_enabled(&mut self, enabled: bool) {
        self.mcp_enabled = enabled;
    }

    pub fn is_mcp_enabled(&self) -> bool {
        self.mcp_enabled
    }

    /// Clears all runtime state and emits reset events.
    pub fn clear(&mut self) {
        self.trusted_clients.clear();
        self.roles.clear();
        self.policies.clear();
        self.connection_policy_assignments.clear();
        self.pending_events
            .push(McpRuntimeEvent::TrustedClientsUpdated);
        self.pending_events.push(McpRuntimeEvent::RolesUpdated);
        self.pending_events.push(McpRuntimeEvent::PoliciesUpdated);
    }

    fn push_event(&mut self, event: McpRuntimeEvent) {
        self.pending_events.push(event);
    }

    pub fn audit_service(&self) -> &AuditService {
        &self.audit_service
    }

    pub fn approval_service(&self) -> &ApprovalService {
        &self.approval_service
    }

    pub fn approval_service_mut(&mut self) -> &mut ApprovalService {
        &mut self.approval_service
    }

    pub fn roles_for_engine(&self) -> Vec<PolicyRole> {
        self.roles
            .values()
            .map(|dto| PolicyRole::from(dto.clone()))
            .collect()
    }

    pub fn policies_for_engine(&self) -> Vec<ToolPolicy> {
        self.policies
            .values()
            .filter_map(|dto| ToolPolicy::try_from(dto.clone()).ok())
            .collect()
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

    fn list_roles(&self) -> Result<Vec<PolicyRoleDto>, GovernanceError> {
        let mut roles: Vec<_> = self.roles.values().cloned().collect();
        roles.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(roles)
    }

    fn list_policies(&self) -> Result<Vec<ToolPolicyDto>, GovernanceError> {
        let mut policies: Vec<_> = self.policies.values().cloned().collect();
        policies.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(policies)
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
        let events = audit_handler::query_audit_logs_extended(&self.audit_service, query)
            .map_err(|error| GovernanceError::Operation(error.to_string()))?;

        Ok(events
            .into_iter()
            .map(|event| {
                let tool_id = event.legacy_tool_id();
                let decision = event.legacy_decision();

                AuditEntry {
                    id: event.id.to_string(),
                    actor_id: event.actor_id,
                    tool_id,
                    decision,
                    reason: event.reason,
                    created_at_epoch_ms: event.created_at_epoch_ms,
                }
            })
            .collect())
    }

    fn export_audit_entries(
        &self,
        query: &AuditQuery,
        format: AuditExportFormat,
    ) -> Result<String, GovernanceError> {
        audit_handler::export_audit_logs_extended(&self.audit_service, query, format)
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

    pub fn upsert_role_mut(
        &mut self,
        role: PolicyRoleDto,
    ) -> Result<PolicyRoleDto, GovernanceError> {
        if role.id.trim().is_empty() {
            return Err(GovernanceError::Validation(
                "role id must not be empty".to_string(),
            ));
        }

        self.roles.insert(role.id.clone(), role.clone());
        self.push_event(McpRuntimeEvent::RolesUpdated);
        Ok(role)
    }

    pub fn delete_role_mut(&mut self, role_id: &str) -> Result<(), GovernanceError> {
        if self.roles.remove(role_id).is_none() {
            return Err(GovernanceError::NotFound {
                resource: format!("role {role_id}"),
            });
        }

        self.push_event(McpRuntimeEvent::RolesUpdated);
        Ok(())
    }

    pub fn upsert_policy_mut(
        &mut self,
        policy: ToolPolicyDto,
    ) -> Result<ToolPolicyDto, GovernanceError> {
        if policy.id.trim().is_empty() {
            return Err(GovernanceError::Validation(
                "policy id must not be empty".to_string(),
            ));
        }

        self.policies.insert(policy.id.clone(), policy.clone());
        self.push_event(McpRuntimeEvent::PoliciesUpdated);
        Ok(policy)
    }

    pub fn delete_policy_mut(&mut self, policy_id: &str) -> Result<(), GovernanceError> {
        if self.policies.remove(policy_id).is_none() {
            return Err(GovernanceError::NotFound {
                resource: format!("policy {policy_id}"),
            });
        }

        self.push_event(McpRuntimeEvent::PoliciesUpdated);
        Ok(())
    }

    pub fn save_connection_policy_assignment_mut(
        &mut self,
        assignment: ConnectionPolicyAssignmentDto,
    ) -> Result<ConnectionPolicyAssignmentDto, GovernanceError> {
        // Empty connection_id is valid - it's used for tools without a specific connection
        // (e.g., list_connections, list_scripts, query_audit_logs)
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
        self.approve_pending_execution_as_mut(pending_id, "system")
    }

    pub fn approve_pending_execution_as_mut(
        &mut self,
        pending_id: &str,
        approver_actor_id: &str,
    ) -> Result<AuditEntry, GovernanceError> {
        self.approve_pending_execution_with_origin_mut(
            pending_id,
            approver_actor_id,
            EventOrigin::system(),
        )
    }

    pub fn approve_pending_execution_with_origin_mut(
        &mut self,
        pending_id: &str,
        approver_actor_id: &str,
        origin: EventOrigin,
    ) -> Result<AuditEntry, GovernanceError> {
        let pending = approval_handler::get_pending_execution(&self.approval_service, pending_id)
            .map_err(|error| GovernanceError::Operation(error.to_string()))?;
        let replay_plan = &pending.plan;

        let ts_ms = now_epoch_ms();
        let event = EventRecord::new(
            ts_ms,
            EventSeverity::Info,
            EventCategory::Mcp,
            EventOutcome::Success,
        )
        .with_typed_action(MCP_APPROVE_EXECUTION)
        .with_origin(origin)
        .with_summary(format!(
            "MCP execution approved: tool={} requester={} approver={}",
            replay_plan.tool_id, replay_plan.actor_id, approver_actor_id
        ))
        .with_actor_id(approver_actor_id)
        .with_object_ref("pending_execution", pending_id)
        .with_connection_context(
            &replay_plan.connection_id,
            "", // database_name not available
            "", // driver_id not available
        )
        .with_details_json(
            serde_json::json!({
                "requested_by": replay_plan.actor_id,
                "approved_by": approver_actor_id,
                "tool_id": replay_plan.tool_id,
            })
            .to_string(),
        );

        let recorded = self.record_audit_event(event)?;

        approval_handler::approve_execution(&mut self.approval_service, pending_id)
            .map_err(|error| GovernanceError::Operation(error.to_string()))?;

        self.push_event(McpRuntimeEvent::PendingExecutionsUpdated);

        Ok(self.audit_entry_from_recorded(recorded, "approve_execution", "allow", None))
    }

    pub fn reject_pending_execution_mut(
        &mut self,
        pending_id: &str,
    ) -> Result<AuditEntry, GovernanceError> {
        self.reject_pending_execution_as_mut(pending_id, "system", None)
    }

    pub fn reject_pending_execution_as_mut(
        &mut self,
        pending_id: &str,
        rejector_actor_id: &str,
        reason: Option<&str>,
    ) -> Result<AuditEntry, GovernanceError> {
        self.reject_pending_execution_with_origin_mut(
            pending_id,
            rejector_actor_id,
            reason,
            EventOrigin::system(),
        )
    }

    pub fn reject_pending_execution_with_origin_mut(
        &mut self,
        pending_id: &str,
        rejector_actor_id: &str,
        reason: Option<&str>,
        origin: EventOrigin,
    ) -> Result<AuditEntry, GovernanceError> {
        let pending = approval_handler::get_pending_execution(&self.approval_service, pending_id)
            .map_err(|error| GovernanceError::Operation(error.to_string()))?;
        let pending_plan = &pending.plan;
        let rejection_reason = reason.unwrap_or("rejected by approver");

        let ts_ms = now_epoch_ms();
        let event = EventRecord::new(
            ts_ms,
            EventSeverity::Warn,
            EventCategory::Mcp,
            EventOutcome::Failure,
        )
        .with_typed_action(MCP_REJECT_EXECUTION)
        .with_origin(origin)
        .with_summary(format!(
            "MCP execution rejected: tool={} requester={} rejector={}",
            pending_plan.tool_id, pending_plan.actor_id, rejector_actor_id
        ))
        .with_actor_id(rejector_actor_id)
        .with_object_ref("pending_execution", pending_id)
        .with_connection_context(
            &pending_plan.connection_id,
            "", // database_name not available
            "", // driver_id not available
        )
        .with_error("rejected", rejection_reason)
        .with_details_json(
            serde_json::json!({
                "requested_by": pending_plan.actor_id,
                "rejected_by": rejector_actor_id,
                "tool_id": pending_plan.tool_id,
                "reason": rejection_reason,
            })
            .to_string(),
        );

        let recorded = self.record_audit_event(event)?;

        approval_handler::reject_execution(&mut self.approval_service, pending_id)
            .map_err(|error| GovernanceError::Operation(error.to_string()))?;

        self.push_event(McpRuntimeEvent::PendingExecutionsUpdated);

        Ok(self.audit_entry_from_recorded(
            recorded,
            "reject_execution",
            "deny",
            Some(rejection_reason.to_string()),
        ))
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

    fn record_audit_event(&mut self, event: EventRecord) -> Result<EventRecord, GovernanceError> {
        let recorded = self
            .audit_service
            .record(event)
            .map_err(|error| GovernanceError::Operation(error.to_string()))?;

        if recorded.id.is_some() {
            self.push_event(McpRuntimeEvent::AuditAppended);
        }

        Ok(recorded)
    }

    fn audit_entry_from_recorded(
        &self,
        recorded: EventRecord,
        tool_id: &str,
        decision: &str,
        reason: Option<String>,
    ) -> AuditEntry {
        AuditEntry {
            id: recorded.id.map(|id| id.to_string()).unwrap_or_default(),
            actor_id: recorded.actor_id.unwrap_or_default(),
            tool_id: tool_id.to_string(),
            decision: decision.to_string(),
            reason,
            created_at_epoch_ms: recorded.ts_ms,
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

#[cfg(test)]
mod tests {
    use dbflux_core::observability::{
        EventCategory,
        actions::{MCP_APPROVE_EXECUTION, MCP_REJECT_EXECUTION},
    };
    use dbflux_policy::ConnectionPolicyAssignment;

    use crate::{ConnectionPolicyAssignmentDto, McpGovernanceService, TrustedClientDto};

    use super::{GovernanceError, McpRuntime, McpRuntimeEvent};

    fn runtime_for_tests(file_name: &str) -> McpRuntime {
        let path = dbflux_audit::temp_sqlite_path(file_name);
        let _ = std::fs::remove_file(&path);
        let audit = dbflux_audit::AuditService::new_sqlite(&path)
            .expect("audit service should initialize for runtime tests");

        McpRuntime::new(audit)
    }

    #[test]
    fn trait_mutating_methods_report_sanctioned_mutation_path() {
        let runtime = runtime_for_tests("dbflux-mcp-runtime-trait-mutations.sqlite");

        let upsert_error = runtime
            .upsert_trusted_client(TrustedClientDto {
                id: "agent-a".to_string(),
                name: "Agent A".to_string(),
                issuer: None,
                active: true,
            })
            .expect_err("trait upsert path should be rejected");

        let save_policy_error = runtime
            .save_connection_policy_assignment(ConnectionPolicyAssignmentDto {
                connection_id: "conn-a".to_string(),
                assignments: vec![ConnectionPolicyAssignment {
                    actor_id: "agent-a".to_string(),
                    scope: dbflux_policy::PolicyBindingScope {
                        connection_id: "conn-a".to_string(),
                    },
                    role_ids: Vec::new(),
                    policy_ids: vec!["policy-a".to_string()],
                }],
            })
            .expect_err("trait save policy path should be rejected");

        assert!(matches!(upsert_error, GovernanceError::Operation(_)));
        assert!(matches!(save_policy_error, GovernanceError::Operation(_)));
    }

    #[test]
    fn mutable_runtime_methods_apply_changes_and_emit_events() {
        let mut runtime = runtime_for_tests("dbflux-mcp-runtime-mutable-mutations.sqlite");

        runtime
            .upsert_trusted_client_mut(TrustedClientDto {
                id: "agent-a".to_string(),
                name: "Agent A".to_string(),
                issuer: None,
                active: true,
            })
            .expect("mutable trusted client upsert should succeed");

        runtime
            .save_connection_policy_assignment_mut(ConnectionPolicyAssignmentDto {
                connection_id: "conn-a".to_string(),
                assignments: vec![ConnectionPolicyAssignment {
                    actor_id: "agent-a".to_string(),
                    scope: dbflux_policy::PolicyBindingScope {
                        connection_id: "conn-a".to_string(),
                    },
                    role_ids: Vec::new(),
                    policy_ids: vec!["policy-a".to_string()],
                }],
            })
            .expect("mutable policy assignment save should succeed");

        let clients = runtime
            .list_trusted_clients()
            .expect("trusted clients should be listable");
        assert_eq!(clients.len(), 1);

        let assignments = runtime
            .list_connection_policy_assignments()
            .expect("policy assignments should be listable");
        assert_eq!(assignments.len(), 1);

        let events = runtime.drain_events();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, McpRuntimeEvent::TrustedClientsUpdated))
        );
        assert!(events.iter().any(|event| matches!(
            event,
            McpRuntimeEvent::ConnectionPolicyUpdated { connection_id }
            if connection_id == "conn-a"
        )));
    }

    #[test]
    fn approval_audit_events_store_pending_execution_object_id() {
        let mut runtime = runtime_for_tests("dbflux-mcp-runtime-approval-audit.sqlite");

        let pending = runtime.request_execution_mut(runtime.classify_plan(
            dbflux_policy::ExecutionClassification::Write,
            serde_json::json!({ "sql": "DELETE FROM users" }),
            "agent-a".to_string(),
            "conn-a".to_string(),
            "delete_rows".to_string(),
        ));

        runtime
            .approve_pending_execution_as_mut(&pending.id, "reviewer-a")
            .expect("approval should record an audit event");

        let stored = runtime
            .audit_service()
            .query_extended(&dbflux_audit::query::AuditQueryFilter {
                action: Some(MCP_APPROVE_EXECUTION.as_str().to_string()),
                category: Some(EventCategory::Mcp.as_str().to_string()),
                ..Default::default()
            })
            .expect("audit query should succeed");

        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].object_type.as_deref(), Some("pending_execution"));
        assert_eq!(stored[0].object_id.as_deref(), Some(pending.id.as_str()));
        assert_eq!(stored[0].actor_id, "reviewer-a");
    }

    #[test]
    fn rejection_audit_events_store_rejector_identity_and_reason() {
        let mut runtime = runtime_for_tests("dbflux-mcp-runtime-rejection-audit.sqlite");

        let pending = runtime.request_execution_mut(runtime.classify_plan(
            dbflux_policy::ExecutionClassification::Write,
            serde_json::json!({ "sql": "DELETE FROM users" }),
            "agent-a".to_string(),
            "conn-a".to_string(),
            "delete_rows".to_string(),
        ));

        runtime
            .reject_pending_execution_as_mut(&pending.id, "reviewer-b", Some("unsafe change"))
            .expect("rejection should record an audit event");

        let stored = runtime
            .audit_service()
            .query_extended(&dbflux_audit::query::AuditQueryFilter {
                action: Some(MCP_REJECT_EXECUTION.as_str().to_string()),
                category: Some(EventCategory::Mcp.as_str().to_string()),
                ..Default::default()
            })
            .expect("audit query should succeed");

        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].actor_id, "reviewer-b");
        assert_eq!(stored[0].error_message.as_deref(), Some("unsafe change"));
        assert_eq!(stored[0].actor_type.as_deref(), Some("system"));
        assert_eq!(stored[0].source_id.as_deref(), Some("system"));
    }

    #[test]
    fn approval_audit_events_use_local_and_mcp_origins_when_requested() {
        let mut runtime = runtime_for_tests("dbflux-mcp-runtime-origin-audit.sqlite");

        let local_pending = runtime.request_execution_mut(runtime.classify_plan(
            dbflux_policy::ExecutionClassification::Write,
            serde_json::json!({ "sql": "DELETE FROM users" }),
            "agent-a".to_string(),
            "conn-a".to_string(),
            "delete_rows".to_string(),
        ));

        runtime
            .approve_pending_execution_with_origin_mut(
                &local_pending.id,
                "local-reviewer",
                dbflux_core::observability::EventOrigin::local(),
            )
            .expect("local approval should record an audit event");

        let mcp_pending = runtime.request_execution_mut(runtime.classify_plan(
            dbflux_policy::ExecutionClassification::Write,
            serde_json::json!({ "sql": "DELETE FROM users" }),
            "agent-b".to_string(),
            "conn-b".to_string(),
            "delete_rows".to_string(),
        ));

        runtime
            .reject_pending_execution_with_origin_mut(
                &mcp_pending.id,
                "mcp-reviewer",
                Some("unsafe change"),
                dbflux_core::observability::EventOrigin::mcp(),
            )
            .expect("mcp rejection should record an audit event");

        let stored = runtime
            .audit_service()
            .query_extended(&dbflux_audit::query::AuditQueryFilter {
                category: Some(EventCategory::Mcp.as_str().to_string()),
                ..Default::default()
            })
            .expect("audit query should succeed");

        assert_eq!(stored.len(), 2);

        let local_event = stored
            .iter()
            .find(|event| event.actor_id == "local-reviewer")
            .expect("local event should exist");
        assert_eq!(local_event.actor_type.as_deref(), Some("user"));
        assert_eq!(local_event.source_id.as_deref(), Some("local"));

        let mcp_event = stored
            .iter()
            .find(|event| event.actor_id == "mcp-reviewer")
            .expect("mcp event should exist");
        assert_eq!(mcp_event.actor_type.as_deref(), Some("mcp_client"));
        assert_eq!(mcp_event.source_id.as_deref(), Some("mcp"));
    }

    #[test]
    fn disabled_audit_does_not_emit_audit_appended_or_fake_id() {
        let mut runtime = runtime_for_tests("dbflux-mcp-runtime-audit-disabled.sqlite");
        runtime.audit_service().set_enabled(false);

        let pending = runtime.request_execution_mut(runtime.classify_plan(
            dbflux_policy::ExecutionClassification::Write,
            serde_json::json!({ "sql": "DELETE FROM users" }),
            "agent-a".to_string(),
            "conn-a".to_string(),
            "delete_rows".to_string(),
        ));

        let recorded = runtime
            .approve_pending_execution_as_mut(&pending.id, "reviewer-a")
            .expect("approval should still succeed");

        assert!(recorded.id.is_empty());

        let events = runtime.drain_events();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, McpRuntimeEvent::PendingExecutionsUpdated))
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, McpRuntimeEvent::AuditAppended))
        );
    }

    #[test]
    fn approval_state_is_not_mutated_when_audit_persistence_fails() {
        let mut runtime = runtime_for_tests("dbflux-mcp-runtime-audit-failure.sqlite");
        runtime.audit_service().set_max_detail_bytes(1);

        let pending = runtime.request_execution_mut(runtime.classify_plan(
            dbflux_policy::ExecutionClassification::Write,
            serde_json::json!({ "sql": "DELETE FROM users" }),
            "agent-a".to_string(),
            "conn-a".to_string(),
            "delete_rows".to_string(),
        ));

        let error = runtime
            .approve_pending_execution_as_mut(&pending.id, "reviewer-a")
            .expect_err("approval should fail when audit persistence fails");

        assert!(error.to_string().contains("max_detail_bytes"));

        let still_pending = runtime.approval_service().list_pending();
        assert_eq!(still_pending.len(), 1);
        assert_eq!(still_pending[0].id.to_string(), pending.id);
    }
}
