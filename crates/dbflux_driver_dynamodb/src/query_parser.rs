use dbflux_core::DbError;

pub const DYNAMODB_MVP_COMMANDS: &[&str] = &["scan", "query", "put", "update", "delete"];

#[derive(Debug, Clone)]
pub enum DynamoCommandEnvelope {
    Scan {
        database: Option<String>,
        table: String,
        filter: Option<serde_json::Value>,
        limit: Option<u32>,
        offset: Option<u64>,
        read_options: DynamoReadOptions,
    },
    Query {
        database: Option<String>,
        table: String,
        filter: Option<serde_json::Value>,
        limit: Option<u32>,
        offset: Option<u64>,
        read_options: DynamoReadOptions,
    },
    Put {
        database: Option<String>,
        table: String,
        items: Vec<serde_json::Value>,
    },
    Update {
        database: Option<String>,
        table: String,
        key: serde_json::Value,
        update: serde_json::Value,
        many: bool,
        upsert: bool,
    },
    Delete {
        database: Option<String>,
        table: String,
        key: serde_json::Value,
        many: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DynamoFilterFallback {
    ClientSide,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamoReadOptions {
    pub index_name: Option<String>,
    pub consistent_read: bool,
    pub filter_fallback: DynamoFilterFallback,
}

impl Default for DynamoReadOptions {
    fn default() -> Self {
        Self {
            index_name: None,
            consistent_read: false,
            filter_fallback: DynamoFilterFallback::ClientSide,
        }
    }
}

pub fn is_supported_command(op: &str) -> bool {
    DYNAMODB_MVP_COMMANDS.contains(&op)
}

pub fn unsupported_command_message(op: &str) -> String {
    format!(
        "Unsupported DynamoDB operation '{op}'. MVP supports only: {}.",
        DYNAMODB_MVP_COMMANDS.join(", ")
    )
}

pub fn parse_command_envelope(input: &str) -> Result<DynamoCommandEnvelope, DbError> {
    let json: serde_json::Value = serde_json::from_str(input).map_err(|error| {
        DbError::syntax_error(format!(
            "Invalid DynamoDB command envelope JSON: {error}. Expected an object with fields like {{\"op\":\"scan\",\"table\":\"...\"}}"
        ))
    })?;

    let object = json
        .as_object()
        .ok_or_else(|| DbError::syntax_error("DynamoDB command envelope must be a JSON object"))?;

    let op = required_string(object, "op")?;
    if !is_supported_command(&op) {
        return Err(DbError::NotSupported(unsupported_command_message(&op)));
    }

    match op.as_str() {
        "scan" => {
            validate_allowed_fields(
                object,
                &[
                    "op",
                    "database",
                    "table",
                    "filter",
                    "limit",
                    "offset",
                    "index",
                    "consistent_read",
                    "allow_filter_fallback",
                    "require_server_filter",
                ],
            )?;

            let filter = optional_object_value(object, "filter")?;
            let read_options = parse_read_options(object, "scan", filter.is_some())?;

            Ok(DynamoCommandEnvelope::Scan {
                database: optional_string(object, "database")?,
                table: required_string(object, "table")?,
                filter,
                limit: optional_u32(object, "limit")?,
                offset: optional_u64(object, "offset")?,
                read_options,
            })
        }
        "query" => {
            validate_allowed_fields(
                object,
                &[
                    "op",
                    "database",
                    "table",
                    "filter",
                    "limit",
                    "offset",
                    "index",
                    "consistent_read",
                    "allow_filter_fallback",
                    "require_server_filter",
                ],
            )?;

            let filter = optional_object_value(object, "filter")?;
            let read_options = parse_read_options(object, "query", filter.is_some())?;

            Ok(DynamoCommandEnvelope::Query {
                database: optional_string(object, "database")?,
                table: required_string(object, "table")?,
                filter,
                limit: optional_u32(object, "limit")?,
                offset: optional_u64(object, "offset")?,
                read_options,
            })
        }
        "put" => {
            validate_allowed_fields(object, &["op", "database", "table", "item", "items"])?;

            let single_item = optional_object_value(object, "item")?;
            let many_items = optional_object_array_value(object, "items")?;

            let items = match (single_item, many_items) {
                (Some(_), Some(_)) => {
                    return Err(DbError::query_failed(
                        "DynamoDB put envelope accepts either 'item' or 'items', not both",
                    ));
                }
                (Some(item), None) => vec![item],
                (None, Some(items)) => items,
                (None, None) => {
                    return Err(DbError::query_failed(
                        "DynamoDB put envelope requires 'item' or 'items'",
                    ));
                }
            };

            if items.is_empty() {
                return Err(DbError::query_failed(
                    "DynamoDB put envelope requires at least one item",
                ));
            }

            Ok(DynamoCommandEnvelope::Put {
                database: optional_string(object, "database")?,
                table: required_string(object, "table")?,
                items,
            })
        }
        "update" => {
            validate_allowed_fields(
                object,
                &["op", "database", "table", "key", "update", "many", "upsert"],
            )?;

            Ok(DynamoCommandEnvelope::Update {
                database: optional_string(object, "database")?,
                table: required_string(object, "table")?,
                key: required_object_value(object, "key")?,
                update: required_object_value(object, "update")?,
                many: optional_bool(object, "many")?.unwrap_or(false),
                upsert: optional_bool(object, "upsert")?.unwrap_or(false),
            })
        }
        "delete" => {
            validate_allowed_fields(object, &["op", "database", "table", "key", "many"])?;

            Ok(DynamoCommandEnvelope::Delete {
                database: optional_string(object, "database")?,
                table: required_string(object, "table")?,
                key: required_object_value(object, "key")?,
                many: optional_bool(object, "many")?.unwrap_or(false),
            })
        }
        _ => Err(DbError::NotSupported(unsupported_command_message(&op))),
    }
}

fn validate_allowed_fields(
    object: &serde_json::Map<String, serde_json::Value>,
    allowed: &[&str],
) -> Result<(), DbError> {
    for key in object.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(DbError::query_failed(format!(
                "Unsupported field '{}' for DynamoDB '{}' command envelope",
                key,
                object
                    .get("op")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown")
            )));
        }
    }

    Ok(())
}

fn required_string(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<String, DbError> {
    let value = object
        .get(key)
        .ok_or_else(|| DbError::query_failed(format!("Missing required field '{key}'")))?;

    let string = value
        .as_str()
        .ok_or_else(|| DbError::query_failed(format!("Field '{key}' must be a string")))?
        .trim()
        .to_string();

    if string.is_empty() {
        return Err(DbError::query_failed(format!(
            "Field '{key}' must not be empty"
        )));
    }

    Ok(string)
}

fn optional_string(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<String>, DbError> {
    let Some(value) = object.get(key) else {
        return Ok(None);
    };

    let trimmed = value
        .as_str()
        .ok_or_else(|| DbError::query_failed(format!("Field '{key}' must be a string")))?
        .trim()
        .to_string();

    if trimmed.is_empty() {
        return Ok(None);
    }

    Ok(Some(trimmed))
}

fn required_object_value(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<serde_json::Value, DbError> {
    let value = object
        .get(key)
        .ok_or_else(|| DbError::query_failed(format!("Missing required field '{key}'")))?
        .clone();

    if !value.is_object() {
        return Err(DbError::query_failed(format!(
            "Field '{key}' must be a JSON object"
        )));
    }

    Ok(value)
}

fn optional_object_value(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<serde_json::Value>, DbError> {
    let Some(value) = object.get(key) else {
        return Ok(None);
    };

    if value.is_null() {
        return Ok(None);
    }

    if !value.is_object() {
        return Err(DbError::query_failed(format!(
            "Field '{key}' must be a JSON object when provided"
        )));
    }

    Ok(Some(value.clone()))
}

fn optional_object_array_value(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<Vec<serde_json::Value>>, DbError> {
    let Some(value) = object.get(key) else {
        return Ok(None);
    };

    if value.is_null() {
        return Ok(None);
    }

    let items = value
        .as_array()
        .ok_or_else(|| DbError::query_failed(format!("Field '{key}' must be a JSON array")))?;

    let mut parsed_items = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        if !item.is_object() {
            return Err(DbError::query_failed(format!(
                "Field '{key}[{index}]' must be a JSON object"
            )));
        }

        parsed_items.push(item.clone());
    }

    Ok(Some(parsed_items))
}

fn optional_bool(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<bool>, DbError> {
    let Some(value) = object.get(key) else {
        return Ok(None);
    };

    value
        .as_bool()
        .map(Some)
        .ok_or_else(|| DbError::query_failed(format!("Field '{key}' must be a boolean")))
}

fn parse_read_options(
    object: &serde_json::Map<String, serde_json::Value>,
    op: &str,
    has_filter: bool,
) -> Result<DynamoReadOptions, DbError> {
    let index_name = optional_non_empty_string(object, "index")?;
    let consistent_read = optional_bool(object, "consistent_read")?.unwrap_or(false);
    let allow_filter_fallback = optional_bool(object, "allow_filter_fallback")?.unwrap_or(true);
    let require_server_filter = optional_bool(object, "require_server_filter")?.unwrap_or(false);

    if allow_filter_fallback && require_server_filter {
        return Err(DbError::query_failed(format!(
            "DynamoDB '{op}' envelope cannot set both 'allow_filter_fallback' and 'require_server_filter' to true"
        )));
    }

    if require_server_filter && !has_filter {
        return Err(DbError::query_failed(format!(
            "DynamoDB '{op}' envelope requires a 'filter' when 'require_server_filter' is true"
        )));
    }

    let filter_fallback = if allow_filter_fallback {
        DynamoFilterFallback::ClientSide
    } else {
        DynamoFilterFallback::Reject
    };

    Ok(DynamoReadOptions {
        index_name,
        consistent_read,
        filter_fallback,
    })
}

fn optional_non_empty_string(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<String>, DbError> {
    let Some(value) = object.get(key) else {
        return Ok(None);
    };

    let trimmed = value
        .as_str()
        .ok_or_else(|| DbError::query_failed(format!("Field '{key}' must be a string")))?
        .trim();

    if trimmed.is_empty() {
        return Err(DbError::query_failed(format!(
            "Field '{key}' must not be empty when provided"
        )));
    }

    Ok(Some(trimmed.to_string()))
}

fn optional_u32(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<u32>, DbError> {
    let Some(value) = object.get(key) else {
        return Ok(None);
    };

    value
        .as_u64()
        .ok_or_else(|| DbError::query_failed(format!("Field '{key}' must be a positive integer")))
        .and_then(|number| {
            u32::try_from(number).map_err(|_| {
                DbError::query_failed(format!(
                    "Field '{key}' exceeds maximum supported integer range"
                ))
            })
        })
        .map(Some)
}

fn optional_u64(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<u64>, DbError> {
    let Some(value) = object.get(key) else {
        return Ok(None);
    };

    value
        .as_u64()
        .map(Some)
        .ok_or_else(|| DbError::query_failed(format!("Field '{key}' must be a positive integer")))
}

#[cfg(test)]
mod tests {
    use super::{
        DynamoCommandEnvelope, DynamoFilterFallback, parse_command_envelope,
        unsupported_command_message,
    };
    use dbflux_core::DbError;

    #[test]
    fn parse_scan_envelope_with_optional_fields() {
        let envelope = parse_command_envelope(
            r#"{"op":"scan","database":"dynamodb","table":"users","filter":{"pk":"A"},"limit":25,"offset":10}"#,
        )
        .expect("scan envelope should parse");

        match envelope {
            DynamoCommandEnvelope::Scan {
                database,
                table,
                filter,
                limit,
                offset,
                read_options,
            } => {
                assert_eq!(database.as_deref(), Some("dynamodb"));
                assert_eq!(table, "users");
                assert!(filter.is_some());
                assert_eq!(limit, Some(25));
                assert_eq!(offset, Some(10));
                assert_eq!(read_options.index_name, None);
                assert!(!read_options.consistent_read);
                assert_eq!(
                    read_options.filter_fallback,
                    DynamoFilterFallback::ClientSide
                );
            }
            other => panic!("expected scan envelope, got {other:?}"),
        }
    }

    #[test]
    fn parse_put_update_delete_envelopes() {
        let put = parse_command_envelope(
            r#"{"op":"put","table":"users","item":{"pk":"A","name":"Alice"}}"#,
        )
        .expect("put envelope should parse");
        assert!(matches!(
            put,
            DynamoCommandEnvelope::Put { ref items, .. } if items.len() == 1
        ));

        let put_many = parse_command_envelope(
            r#"{"op":"put","table":"users","items":[{"pk":"A"},{"pk":"B"}]}"#,
        )
        .expect("put-many envelope should parse");
        assert!(matches!(
            put_many,
            DynamoCommandEnvelope::Put { ref items, .. } if items.len() == 2
        ));

        let update = parse_command_envelope(
            r#"{"op":"update","table":"users","key":{"pk":"A"},"update":{"name":"Bob"}}"#,
        )
        .expect("update envelope should parse");
        assert!(matches!(update, DynamoCommandEnvelope::Update { .. }));

        let delete = parse_command_envelope(r#"{"op":"delete","table":"users","key":{"pk":"A"}}"#)
            .expect("delete envelope should parse");
        assert!(matches!(delete, DynamoCommandEnvelope::Delete { .. }));
    }

    #[test]
    fn malformed_json_maps_to_syntax_error() {
        let error = parse_command_envelope("{not-json").expect_err("invalid json must fail");
        assert!(matches!(error, DbError::SyntaxError(_)));
    }

    #[test]
    fn unsupported_op_maps_to_not_supported() {
        let error = parse_command_envelope(r#"{"op":"batch_write","table":"users"}"#)
            .expect_err("unsupported op must fail");

        match error {
            DbError::NotSupported(message) => {
                assert_eq!(message, unsupported_command_message("batch_write"));
            }
            other => panic!("expected NotSupported, got {other:?}"),
        }
    }

    #[test]
    fn invalid_or_unknown_fields_map_to_validation_errors() {
        let missing_table =
            parse_command_envelope(r#"{"op":"scan"}"#).expect_err("missing field should fail");
        assert!(matches!(missing_table, DbError::QueryFailed(_)));

        let wrong_limit =
            parse_command_envelope(r#"{"op":"scan","table":"users","limit":"twenty"}"#)
                .expect_err("wrong type should fail");
        assert!(matches!(wrong_limit, DbError::QueryFailed(_)));

        let unknown_field =
            parse_command_envelope(r#"{"op":"put","table":"users","item":{"pk":"A"},"foo":1}"#)
                .expect_err("unknown field should fail");
        assert!(matches!(unknown_field, DbError::QueryFailed(_)));

        let conflicting_put_payload = parse_command_envelope(
            r#"{"op":"put","table":"users","item":{"pk":"A"},"items":[{"pk":"B"}]}"#,
        )
        .expect_err("item and items cannot be combined");
        assert!(matches!(conflicting_put_payload, DbError::QueryFailed(_)));
    }

    #[test]
    fn parse_query_envelope_with_read_options() {
        let envelope = parse_command_envelope(
            r#"{"op":"query","table":"users","filter":{"pk":"USER#1"},"index":"gsi_users_by_status","consistent_read":true,"allow_filter_fallback":false}"#,
        )
        .expect("query envelope with read options should parse");

        match envelope {
            DynamoCommandEnvelope::Query { read_options, .. } => {
                assert_eq!(
                    read_options.index_name.as_deref(),
                    Some("gsi_users_by_status")
                );
                assert!(read_options.consistent_read);
                assert_eq!(read_options.filter_fallback, DynamoFilterFallback::Reject);
            }
            other => panic!("expected query envelope, got {other:?}"),
        }
    }

    #[test]
    fn read_option_types_are_validated() {
        let wrong_index_type = parse_command_envelope(r#"{"op":"scan","table":"users","index":1}"#)
            .expect_err("non-string index must fail");
        assert!(matches!(wrong_index_type, DbError::QueryFailed(_)));

        let wrong_consistent_type =
            parse_command_envelope(r#"{"op":"scan","table":"users","consistent_read":"yes"}"#)
                .expect_err("non-bool consistent_read must fail");
        assert!(matches!(wrong_consistent_type, DbError::QueryFailed(_)));

        let wrong_fallback_type =
            parse_command_envelope(r#"{"op":"scan","table":"users","allow_filter_fallback":"no"}"#)
                .expect_err("non-bool allow_filter_fallback must fail");
        assert!(matches!(wrong_fallback_type, DbError::QueryFailed(_)));
    }

    #[test]
    fn mutually_exclusive_or_unsupported_read_options_are_rejected() {
        let mutually_exclusive = parse_command_envelope(
            r#"{"op":"query","table":"users","filter":{"pk":"A"},"allow_filter_fallback":true,"require_server_filter":true}"#,
        )
        .expect_err("conflicting fallback options must fail");
        assert!(matches!(mutually_exclusive, DbError::QueryFailed(_)));

        let missing_filter =
            parse_command_envelope(r#"{"op":"scan","table":"users","require_server_filter":true}"#)
                .expect_err("require_server_filter without filter must fail");
        assert!(matches!(missing_filter, DbError::QueryFailed(_)));
    }
}
