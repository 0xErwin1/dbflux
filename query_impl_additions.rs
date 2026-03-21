// Add these methods inside the `impl DbFluxServer` block, before the closing `}`
// These implement the query execution logic migrated from handlers_old/query.rs

/// Implementation for execute_query
async fn execute_query_impl(
    state: ServerState,
    connection_id: &str,
    sql: &str,
    database: Option<&str>,
    limit: Option<u32>,
    offset: Option<u32>,
) -> Result<serde_json::Value, String> {
    use dbflux_core::QueryRequest;
    
    // Get or establish connection
    let connection = {
        let cache = state.connection_cache.read().await;
        if let Some(conn) = cache.get(connection_id) {
            conn
        } else {
            drop(cache);
            let profile_uuid = connection_id
                .parse::<uuid::Uuid>()
                .map_err(|_| crate::error_messages::invalid_connection_id(connection_id))?;
            
            let profile = {
                let pm = state.profile_manager.read().await;
                pm.find_by_id(profile_uuid)
                    .cloned()
                    .ok_or_else(|| crate::error_messages::connection_not_found(connection_id))?
            };
            
            let driver_id = profile.driver_id();
            let available_drivers: Vec<String> = state.driver_registry.keys().cloned().collect();
            
            let driver = state
                .driver_registry
                .get(&driver_id)
                .cloned()
                .ok_or_else(|| crate::error_messages::driver_not_available(&driver_id, &available_drivers))?;
            
            let connection = driver
                .connect_with_secrets(&profile, None, None)
                .map_err(|e| crate::error_messages::connection_error(connection_id, &driver_id, e))?;
            
            let connection: Arc<dyn dbflux_core::Connection> = Arc::from(connection);
            
            let mut cache = state.connection_cache.write().await;
            cache.insert(connection_id.to_string(), connection.clone());
            connection
        }
    };
    
    let driver = {
        let pm = state.profile_manager.read().await;
        connection_id
            .parse::<uuid::Uuid>()
            .ok()
            .and_then(|uuid| pm.find_by_id(uuid))
            .map(|p| p.driver_id())
            .unwrap_or_else(|| "unknown".to_string())
    };
    
    let mut request = QueryRequest::new(sql);
    if let Some(db) = database {
        request = request.with_database(Some(db.to_string()));
    }
    if let Some(l) = limit {
        request = request.with_limit(l);
    }
    if let Some(o) = offset {
        request = request.with_offset(o);
    }
    
    let result = connection.execute(&request).map_err(|e| {
        crate::error_messages::query_execution_error("execute_query", connection_id, database, &driver, e)
    })?;
    
    Ok(serialize_query_result(&result))
}

/// Implementation for explain_query
async fn explain_query_impl(
    state: ServerState,
    connection_id: &str,
    sql: Option<&str>,
    table_name: Option<&str>,
    database: Option<&str>,
) -> Result<serde_json::Value, String> {
    use dbflux_core::{ExplainRequest, TableRef};
    
    // Get or establish connection (same logic as execute_query_impl)
    let connection = {
        let cache = state.connection_cache.read().await;
        if let Some(conn) = cache.get(connection_id) {
            conn
        } else {
            drop(cache);
            let profile_uuid = connection_id
                .parse::<uuid::Uuid>()
                .map_err(|_| crate::error_messages::invalid_connection_id(connection_id))?;
            
            let profile = {
                let pm = state.profile_manager.read().await;
                pm.find_by_id(profile_uuid)
                    .cloned()
                    .ok_or_else(|| crate::error_messages::connection_not_found(connection_id))?
            };
            
            let driver_id = profile.driver_id();
            let available_drivers: Vec<String> = state.driver_registry.keys().cloned().collect();
            
            let driver = state
                .driver_registry
                .get(&driver_id)
                .cloned()
                .ok_or_else(|| crate::error_messages::driver_not_available(&driver_id, &available_drivers))?;
            
            let connection = driver
                .connect_with_secrets(&profile, None, None)
                .map_err(|e| crate::error_messages::connection_error(connection_id, &driver_id, e))?;
            
            let connection: Arc<dyn dbflux_core::Connection> = Arc::from(connection);
            
            let mut cache = state.connection_cache.write().await;
            cache.insert(connection_id.to_string(), connection.clone());
            connection
        }
    };
    
    let driver = {
        let pm = state.profile_manager.read().await;
        connection_id
            .parse::<uuid::Uuid>()
            .ok()
            .and_then(|uuid| pm.find_by_id(uuid))
            .map(|p| p.driver_id())
            .unwrap_or_else(|| "unknown".to_string())
    };
    
    let table_ref = TableRef {
        schema: None,
        name: table_name.unwrap_or("").to_string(),
    };
    
    let mut request = ExplainRequest::new(table_ref);
    if let Some(query) = sql {
        request = request.with_query(query);
    }
    
    let result = connection.explain(&request).map_err(|e| {
        crate::error_messages::query_execution_error("explain_query", connection_id, database, &driver, e)
    })?;
    
    Ok(serialize_query_result(&result))
}

/// Implementation for preview_mutation
async fn preview_mutation_impl(
    state: ServerState,
    connection_id: &str,
    sql: &str,
    database: Option<&str>,
) -> Result<serde_json::Value, String> {
    use dbflux_core::{ExplainRequest, TableRef};
    
    // Get or establish connection (same logic as execute_query_impl)
    let connection = {
        let cache = state.connection_cache.read().await;
        if let Some(conn) = cache.get(connection_id) {
            conn
        } else {
            drop(cache);
            let profile_uuid = connection_id
                .parse::<uuid::Uuid>()
                .map_err(|_| crate::error_messages::invalid_connection_id(connection_id))?;
            
            let profile = {
                let pm = state.profile_manager.read().await;
                pm.find_by_id(profile_uuid)
                    .cloned()
                    .ok_or_else(|| crate::error_messages::connection_not_found(connection_id))?
            };
            
            let driver_id = profile.driver_id();
            let available_drivers: Vec<String> = state.driver_registry.keys().cloned().collect();
            
            let driver = state
                .driver_registry
                .get(&driver_id)
                .cloned()
                .ok_or_else(|| crate::error_messages::driver_not_available(&driver_id, &available_drivers))?;
            
            let connection = driver
                .connect_with_secrets(&profile, None, None)
                .map_err(|e| crate::error_messages::connection_error(connection_id, &driver_id, e))?;
            
            let connection: Arc<dyn dbflux_core::Connection> = Arc::from(connection);
            
            let mut cache = state.connection_cache.write().await;
            cache.insert(connection_id.to_string(), connection.clone());
            connection
        }
    };
    
    let driver = {
        let pm = state.profile_manager.read().await;
        connection_id
            .parse::<uuid::Uuid>()
            .ok()
            .and_then(|uuid| pm.find_by_id(uuid))
            .map(|p| p.driver_id())
            .unwrap_or_else(|| "unknown".to_string())
    };
    
    let table_ref = TableRef {
        schema: None,
        name: String::new(),
    };
    
    let request = ExplainRequest::new(table_ref).with_query(sql);
    
    let result = connection.explain(&request).map_err(|e| {
        crate::error_messages::query_execution_error(
            "preview_mutation",
            connection_id,
            database,
            &driver,
            e,
        )
    })?;
    
    Ok(serde_json::json!({
        "preview": serialize_query_result(&result),
        "note": "This is an execution plan preview — the mutation was NOT executed.",
    }))
}

// ===== Module-level helper functions =====
// Add these OUTSIDE the impl block, after the ServerHandler implementation

/// Serialize a QueryResult into JSON for MCP responses
fn serialize_query_result(result: &dbflux_core::QueryResult) -> serde_json::Value {
    let columns: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
    
    let rows: Vec<serde_json::Value> = result
        .rows
        .iter()
        .map(|row| {
            let mut obj = serde_json::Map::new();
            for (col, cell) in columns.iter().zip(row.iter()) {
                obj.insert((*col).to_string(), value_to_json(cell));
            }
            serde_json::Value::Object(obj)
        })
        .collect();
    
    serde_json::json!({
        "columns": columns,
        "rows": rows,
        "row_count": result.rows.len(),
    })
}

/// Convert a dbflux_core::Value to serde_json::Value
fn value_to_json(value: &dbflux_core::Value) -> serde_json::Value {
    use dbflux_core::Value;
    
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(i),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(f.to_string())),
        Value::Text(s)
        | Value::Json(s)
        | Value::Decimal(s)
        | Value::ObjectId(s)
        | Value::Unsupported(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => serde_json::json!({ "_type": "bytes", "length": b.len() }),
        Value::DateTime(dt) => serde_json::Value::String(dt.to_rfc3339()),
        Value::Date(d) => serde_json::Value::String(d.to_string()),
        Value::Time(t) => serde_json::Value::String(t.to_string()),
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        Value::Document(doc) => {
            let map: serde_json::Map<_, _> = doc
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
    }
}
