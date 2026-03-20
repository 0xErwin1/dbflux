pub mod approval;
pub mod audit;
pub mod discovery;
pub mod query;
pub mod schema;
pub mod scripts;

use std::sync::Arc;

use dbflux_core::Connection;

use crate::bootstrap::ServerState;

/// Resolves (or establishes) a connection for the given `connection_id`.
///
/// Looks up the cached connection first. If not present, finds the profile,
/// selects the driver, and calls `connect_with_secrets`. The new connection
/// is inserted into the cache before returning.
pub fn get_or_connect<'a>(
    state: &'a mut ServerState,
    connection_id: &str,
) -> Result<Arc<dyn Connection>, String> {
    if let Some(conn) = state.connection_cache.get(connection_id) {
        return Ok(conn);
    }

    let profile_uuid = connection_id
        .parse::<uuid::Uuid>()
        .map_err(|_| format!("Invalid connection_id: {connection_id}"))?;

    let profile = state
        .profile_manager
        .find_by_id(profile_uuid)
        .cloned()
        .ok_or_else(|| format!("Connection not found: {connection_id}"))?;

    let driver_id = profile.driver_id();

    let driver = state
        .driver_registry
        .get(&driver_id)
        .cloned()
        .ok_or_else(|| format!("Driver not available: {driver_id}"))?;

    let connection = driver
        .connect_with_secrets(&profile, None, None)
        .map_err(|e| format!("Connection failed: {e}"))?;

    let connection: Arc<dyn Connection> = Arc::from(connection);
    state
        .connection_cache
        .insert(connection_id.to_string(), connection.clone());

    Ok(connection)
}

/// Extracts a required `&str` field from a JSON args object.
pub fn require_str<'a>(args: &'a serde_json::Value, field: &str) -> Result<&'a str, String> {
    args.get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("Missing required field: {field}"))
}

/// Extracts an optional `&str` field from a JSON args object.
pub fn optional_str<'a>(args: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    args.get(field).and_then(serde_json::Value::as_str)
}
