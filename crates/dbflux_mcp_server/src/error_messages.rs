//! Rich, contextual error messages for MCP server operations.
//!
//! This module provides helper functions to create actionable error messages
//! that help both AI agents and human users understand exactly what went wrong
//! and how to fix it.

#![allow(dead_code)]

use std::path::Path;

/// Format a connection error with context and troubleshooting steps.
pub fn connection_error(
    connection_id: &str,
    driver: &str,
    error: impl std::fmt::Display,
) -> String {
    format!(
        "Failed to establish database connection: {}\n\
         \n\
         Connection ID: {}\n\
         Driver: {}\n\
         \n\
         Common causes:\n\
         • Incorrect credentials (username/password)\n\
         • Database server unreachable (wrong host/port)\n\
         • Firewall blocking connection\n\
         • Database not running\n\
         • SSL/TLS certificate issues\n\
         • Network timeout\n\
         \n\
         Troubleshooting:\n\
         • Verify credentials in DBFlux GUI → Edit Connection\n\
         • Check database server is running\n\
         • For cloud databases: verify network access rules/security groups\n\
         • For SSH tunnels: ensure tunnel is configured and active\n\
         • Test with DBFlux GUI first to isolate MCP-specific issues",
        error, connection_id, driver
    )
}

/// Format an invalid connection ID error.
pub fn invalid_connection_id(connection_id: &str) -> String {
    format!(
        "Invalid connection ID format: '{}'\n\
         \n\
         Connection IDs must be UUIDs (e.g., '550e8400-e29b-41d4-a716-446655440000').\n\
         \n\
         Resolution:\n\
         • Use 'list_connections' tool to get valid connection IDs\n\
         • Verify you copied the full UUID without extra spaces\n\
         • Connection IDs are returned as strings, not integers",
        connection_id
    )
}

/// Format a connection not found error.
pub fn connection_not_found(connection_id: &str) -> String {
    format!(
        "Connection not found: {}\n\
         \n\
         The connection ID is valid but no profile exists with this ID.\n\
         \n\
         Resolution:\n\
         • Run 'list_connections' to see available connections\n\
         • Verify connection was not deleted in DBFlux GUI\n\
         • Check if connection was created in a different DBFlux profile\n\
         • Connection profiles are stored in ~/.config/dbflux/profiles.json",
        connection_id
    )
}

/// Format a driver not available error.
pub fn driver_not_available(driver_id: &str, available_drivers: &[String]) -> String {
    let drivers_list = if available_drivers.is_empty() {
        "None (server built without driver features)".to_string()
    } else {
        available_drivers.join(", ")
    };

    format!(
        "Database driver not available: '{}'\n\
         \n\
         The connection profile requires a driver that is not enabled in this build.\n\
         \n\
         Supported drivers in this build: {}\n\
         \n\
         Resolution:\n\
         • Use a connection with a different driver (run 'list_connections')\n\
         • Rebuild server with driver: cargo build -p dbflux --features {}\n\
         • Check connection profile driver_id in ~/.config/dbflux/profiles.json",
        driver_id, drivers_list, driver_id
    )
}

/// Format a query execution error.
pub fn query_execution_error(
    tool: &str,
    connection_id: &str,
    database: Option<&str>,
    driver: &str,
    error: impl std::fmt::Display,
) -> String {
    let db_info = database
        .map(|db| format!("\nDatabase: {}", db))
        .unwrap_or_default();

    format!(
        "Query execution failed: {}\n\
         \n\
         Tool: {}\n\
         Connection: {}\
         {}\n\
         Driver: {}\n\
         \n\
         Common causes:\n\
         • SQL syntax error\n\
         • Permission denied (insufficient database user privileges)\n\
         • Table/column does not exist\n\
         • Query timeout\n\
         • Connection lost\n\
         \n\
         Troubleshooting:\n\
         • Verify SQL syntax for database type ({})\n\
         • Check user permissions with SHOW GRANTS or equivalent\n\
         • Test query in DBFlux GUI query editor\n\
         • For timeout: reduce query complexity or add LIMIT clause",
        error, tool, connection_id, db_info, driver, driver
    )
}

/// Format a schema operation error.
pub fn schema_operation_error(
    operation: &str,
    connection_id: &str,
    database: Option<&str>,
    schema: Option<&str>,
    object_name: Option<&str>,
    error: impl std::fmt::Display,
) -> String {
    let mut parts = vec![format!("Connection: {}", connection_id)];
    if let Some(db) = database {
        parts.push(format!("Database: {}", db));
    }
    if let Some(s) = schema {
        parts.push(format!("Schema: {}", s));
    }
    if let Some(obj) = object_name {
        parts.push(format!("Object: {}", obj));
    }

    let context = parts.join("\n");

    format!(
        "Failed to {}: {}\n\
         \n\
         {}\n\
         \n\
         Common causes:\n\
         • Database/schema/object does not exist\n\
         • Permission denied (need SELECT on information_schema)\n\
         • Incorrect names (check spelling and case sensitivity)\n\
         \n\
         Resolution:\n\
         • Use 'list_databases' to verify database exists\n\
         • Use 'list_schemas' to verify schema exists\n\
         • Use 'list_tables' to verify object exists\n\
         • Check permissions with SHOW GRANTS or equivalent",
        operation, error, context
    )
}

/// Format an authorization denied error.
pub fn authorization_denied(
    client_id: &str,
    connection_id: &str,
    tool_id: &str,
    reason: &str,
) -> String {
    format!(
        "Authorization denied: {}\n\
         \n\
         Client: {}\n\
         Connection: {}\n\
         Tool: {}\n\
         \n\
         Resolution:\n\
         • Open DBFlux GUI → Connection Manager\n\
         • Select connection → 'MCP' tab\n\
         • Under 'Policy Assignments', verify client '{}' has appropriate permissions\n\
         • Ensure roles/policies allow tool '{}'\n\
         • Check that connection has 'Enable MCP' checkbox enabled\n\
         \n\
         For help with policy configuration:\n\
         • Settings → MCP → Roles: define role capabilities\n\
         • Settings → MCP → Policies: define allowed tools and classifications",
        reason, client_id, connection_id, tool_id, client_id, tool_id
    )
}

/// Format a connection not MCP-enabled error.
pub fn connection_not_mcp_enabled(connection_id: &str) -> String {
    format!(
        "Connection '{}' does not allow MCP access\n\
         \n\
         This connection has MCP governance disabled.\n\
         \n\
         To enable:\n\
         1. Open DBFlux GUI\n\
         2. Go to Connection Manager → Select connection\n\
         3. Navigate to 'MCP' tab\n\
         4. Check 'Enable MCP for this connection'\n\
         5. Assign a trusted client, role, and policy\n\
         6. Save changes\n\
         \n\
         Why MCP might be disabled:\n\
         • Production database requiring extra approval\n\
         • Sensitive data requiring human oversight\n\
         • Connection not yet configured for AI access",
        connection_id
    )
}

/// Format a script operation error.
pub fn script_error(operation: &str, script_id: &str, error: impl std::fmt::Display) -> String {
    format!(
        "Failed to {} script: {}\n\
         \n\
         Script: {}\n\
         \n\
         Common causes:\n\
         • File does not exist or was deleted\n\
         • Permission denied (check file/directory permissions)\n\
         • Invalid filename (avoid special characters and spaces)\n\
         • Disk full\n\
         \n\
         Resolution:\n\
         • Use 'list_scripts' to see available scripts\n\
         • Verify file exists: ls -la ~/.local/share/dbflux/scripts/{}\n\
         • Check permissions: should be readable/writable by user\n\
         • For create: use alphanumeric names with hyphens/underscores",
        operation, error, script_id, script_id
    )
}

/// Format a configuration error.
pub fn config_error(
    operation: &str,
    config_path: Option<&Path>,
    error: impl std::fmt::Display,
) -> String {
    let path_info = config_path
        .map(|p| format!("Config path: {}", p.display()))
        .unwrap_or_else(|| "Config path: ~/.config/dbflux".to_string());

    format!(
        "Failed to {}: {}\n\
         \n\
         {}\n\
         \n\
         Common causes:\n\
         • dbflux.db is missing, corrupted, or inaccessible\n\
         • Insufficient permissions\n\
         • Directory does not exist\n\
         • Disk full\n\
         \n\
         Resolution:\n\
         • Check permissions: ls -la ~/.local/share/dbflux/\n\
         • Ensure directory exists: mkdir -p ~/.local/share/dbflux\n\
         • Verify disk space: df -h ~/.local\n\
         • Test integrity: sqlite3 ~/.local/share/dbflux/dbflux.db 'PRAGMA integrity_check;'",
        operation, error, path_info
    )
}

/// Format an audit operation error.
pub fn audit_error(operation: &str, error: impl std::fmt::Display) -> String {
    format!(
        "Audit operation failed: {}\n\
         \n\
         Operation: {}\n\
         \n\
         Common causes:\n\
         • Audit database corrupted\n\
         • Permission denied on audit file\n\
         • Disk full\n\
         \n\
         Resolution:\n\
         • Check audit database: ~/.local/share/dbflux/dbflux.db (aud_audit_events table)\n\
         • Verify permissions: chmod 644 ~/.local/share/dbflux/dbflux.db\n\
         • Check disk space: df -h ~/.local\n\
         • Test integrity: sqlite3 ~/.local/share/dbflux/dbflux.db 'PRAGMA integrity_check;'\n\
         \n\
         If database is corrupted (WARNING: loses audit history):\n\
         • rm ~/.local/share/dbflux/dbflux.db\n\
         • Server will recreate on next start",
        error, operation
    )
}

/// Format a missing required field error.
pub fn missing_required_field(tool_id: &str, field_name: &str) -> String {
    format!(
        "Missing required parameter: '{}'\n\
         \n\
         Tool: {}\n\
         \n\
         Resolution:\n\
         • Check tool schema: use 'tools/list' to see required fields\n\
         • Verify parameter name is exactly '{}' (case-sensitive)\n\
         • Ensure parameter value is not null or empty string\n\
         • Consult tool documentation for usage examples",
        field_name, tool_id, field_name
    )
}

/// Format an unknown tool error with suggestions.
pub fn unknown_tool(tool_id: &str, category: &str) -> String {
    format!(
        "Unknown tool: '{}'\n\
         \n\
         This tool is not recognized by the server.\n\
         Category: {}\n\
         \n\
         Common causes:\n\
         • Tool name misspelled (check capitalization)\n\
         • Tool not available in this server version\n\
         • Tool ID format incorrect\n\
         \n\
         Resolution:\n\
         • Use 'tools/list' method to see all available tools\n\
         • Verify exact tool name from the list\n\
         • Check MCP protocol version compatibility",
        tool_id, category
    )
}
