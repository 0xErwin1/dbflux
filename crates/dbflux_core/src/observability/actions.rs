//! Typed audit action constants for DBFlux.
//!
//! All audit events MUST use these constants rather than bare string literals.
//! This ensures consistent action naming across the codebase.
//!
//! ## Action Naming Convention
//!
//! Actions follow the pattern `{category}_{verb}`:
//! - `query_execute` — query/script executed successfully
//! - `query_execute_failed` — query/script execution failed
//! - `connection_connect` — profile connected successfully
//! - `connection_disconnect` — profile disconnected
//! - `connection_connect_failed` — connection attempt failed
//! - `hook_execute` — hook ran successfully
//! - `hook_execute_failed` — hook failed
//! - `mcp_authorize` — MCP policy evaluation
//! - `mcp_approve_execution` — human approved pending execution
//! - `mcp_reject_execution` — human rejected pending execution
//! - `config_change` — settings/profile/hook mutation
//! - `system_startup` — application started
//! - `system_shutdown` — application shutting down
//! - `system_panic` — panic captured by global handler

use serde::{Deserialize, Serialize};

/// A typed audit action constant.
///
/// This newtype wraps a static string to prevent typos and ensure
/// all call sites use the canonical constants defined in this module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuditAction(&'static str);

impl AuditAction {
    /// Creates a new typed action from a static string.
    ///
    /// This is only for internal use when defining constants.
    const fn new(action: &'static str) -> Self {
        Self(action)
    }

    /// Returns the raw string value of this action.
    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for AuditAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ============================================================================
// MCP actions
// ============================================================================

/// Policy evaluation result for an MCP tool call.
pub const MCP_AUTHORIZE: AuditAction = AuditAction::new("mcp_authorize");
/// Human approved a pending MCP execution.
pub const MCP_APPROVE_EXECUTION: AuditAction = AuditAction::new("mcp_approve_execution");
/// Human rejected a pending MCP execution.
pub const MCP_REJECT_EXECUTION: AuditAction = AuditAction::new("mcp_reject_execution");
/// MCP tool handler executed (post-authorization).
pub const MCP_TOOL_EXECUTE: AuditAction = AuditAction::new("mcp_tool_execute");
/// MCP tool handler failed after authorization.
pub const MCP_TOOL_EXECUTE_FAILED: AuditAction = AuditAction::new("mcp_tool_execute_failed");

// ============================================================================
// Query and script actions
// ============================================================================

/// Query executed successfully.
pub const QUERY_EXECUTE: AuditAction = AuditAction::new("query_execute");
/// Query execution failed.
pub const QUERY_EXECUTE_FAILED: AuditAction = AuditAction::new("query_execute_failed");
/// Query was cancelled.
pub const QUERY_CANCEL: AuditAction = AuditAction::new("query_cancel");
/// User confirmed a dangerous query despite warning.
pub const DANGEROUS_QUERY_CONFIRMED: AuditAction = AuditAction::new("dangerous_query_confirmed");

/// Script executed successfully.
pub const SCRIPT_EXECUTE: AuditAction = AuditAction::new("script_execute");
/// Script execution failed.
pub const SCRIPT_EXECUTE_FAILED: AuditAction = AuditAction::new("script_execute_failed");

// ============================================================================
// Connection lifecycle actions
// ============================================================================

/// Profile connection started (pre-auth, post-hook).
pub const CONNECTION_CONNECTING: AuditAction = AuditAction::new("connection_connecting");
/// Profile connected successfully.
pub const CONNECTION_CONNECT: AuditAction = AuditAction::new("connection_connect");
/// Profile disconnected.
pub const CONNECTION_DISCONNECT: AuditAction = AuditAction::new("connection_disconnect");
/// Connection attempt failed.
pub const CONNECTION_CONNECT_FAILED: AuditAction = AuditAction::new("connection_connect_failed");

// ============================================================================
// Hook actions
// ============================================================================

/// Hook ran successfully.
pub const HOOK_EXECUTE: AuditAction = AuditAction::new("hook_execute");
/// Hook failed during execution.
pub const HOOK_EXECUTE_FAILED: AuditAction = AuditAction::new("hook_execute_failed");

// ============================================================================
// Configuration actions
// ============================================================================

/// A settings, profile, auth, or hook definition changed without a more specific subtype.
pub const CONFIG_CHANGE: AuditAction = AuditAction::new("config_change");
/// A config object was created.
pub const CONFIG_CREATE: AuditAction = AuditAction::new("config_create");
/// A config object was updated.
pub const CONFIG_UPDATE: AuditAction = AuditAction::new("config_update");
/// A config object was deleted.
pub const CONFIG_DELETE: AuditAction = AuditAction::new("config_delete");

// ============================================================================
// System lifecycle actions
// ============================================================================

/// Application started successfully.
pub const SYSTEM_STARTUP: AuditAction = AuditAction::new("system_startup");
/// Application initiated shutdown.
pub const SYSTEM_SHUTDOWN: AuditAction = AuditAction::new("system_shutdown");
/// Panic or unrecoverable failure captured by the global handler.
pub const SYSTEM_PANIC: AuditAction = AuditAction::new("system_panic");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_action_as_str() {
        assert_eq!(MCP_AUTHORIZE.as_str(), "mcp_authorize");
        assert_eq!(QUERY_EXECUTE.as_str(), "query_execute");
        assert_eq!(SYSTEM_PANIC.as_str(), "system_panic");
    }

    #[test]
    fn test_audit_action_display() {
        let action = CONNECTION_CONNECT;
        assert_eq!(format!("{}", action), "connection_connect");
    }

    #[test]
    fn test_audit_action_equality() {
        assert_eq!(MCP_AUTHORIZE, MCP_AUTHORIZE);
        assert_ne!(MCP_AUTHORIZE, MCP_APPROVE_EXECUTION);
    }

    #[test]
    fn test_audit_action_copy() {
        let action = QUERY_EXECUTE;
        let _ = action;
        let action2 = action;
        assert_eq!(action, action2);
    }
}
