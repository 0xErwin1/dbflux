//! MongoDB shell syntax parser.
//!
//! Parses `db.collection.method(...)` style queries and converts them to internal
//! `MongoQuery` representation. Falls back to JSON format for backward compatibility.

use bson::Document;
use dbflux_core::DbError;

use crate::driver::{MongoOperation, MongoQuery, json_array_to_bson_docs, json_to_bson_doc};

/// Parse error with byte-offset position (`offset` + `len` in bytes).
#[derive(Debug, Clone)]
pub struct MongoParseError {
    pub message: String,
    pub offset: usize,
    pub len: usize,
}

impl MongoParseError {
    fn new(message: impl Into<String>, offset: usize, len: usize) -> Self {
        Self {
            message: message.into(),
            offset,
            len,
        }
    }

    fn from_db_error(err: DbError, offset: usize) -> Self {
        Self {
            message: err.to_string(),
            offset,
            len: 0,
        }
    }
}

impl From<MongoParseError> for DbError {
    fn from(e: MongoParseError) -> Self {
        DbError::query_failed(e.message)
    }
}

/// Validate MongoDB query syntax without executing.
/// Returns collection name on success, error on parse failure.
pub fn validate_query(input: &str) -> Result<String, DbError> {
    let query = parse_query(input)?;
    Ok(query.collection)
}

/// Like `validate_query`, but returns positional `MongoParseError`s.
pub fn validate_query_positional(input: &str) -> Vec<MongoParseError> {
    match parse_query_positional(input) {
        Ok(_) => vec![],
        Err(e) => vec![e],
    }
}

fn parse_query_positional(input: &str) -> Result<MongoQuery, MongoParseError> {
    let trimmed = input.trim();
    let trim_offset = input.len() - input.trim_start().len();

    if trimmed.starts_with("db.") {
        return parse_shell_syntax_positional(trimmed, trim_offset);
    }

    parse_json_format(trimmed).map_err(|e| MongoParseError::from_db_error(e, trim_offset))
}

/// Parse a query string into a MongoQuery.
///
/// Supports two formats:
/// 1. Shell syntax: `db.collection.method({...})`
/// 2. JSON format: `{"collection": "...", "filter": {...}}`
pub fn parse_query(input: &str) -> Result<MongoQuery, DbError> {
    let trimmed = input.trim();

    // Try shell syntax first
    if trimmed.starts_with("db.") {
        return parse_shell_syntax(trimmed);
    }

    // Fall back to JSON format
    parse_json_format(trimmed)
}

/// Parse mongo shell syntax: `db.collection.method(...)`
fn parse_shell_syntax(input: &str) -> Result<MongoQuery, DbError> {
    // Strip "db." prefix
    let rest = input
        .strip_prefix("db.")
        .ok_or_else(|| DbError::query_failed("Expected 'db.' prefix".to_string()))?;

    // Find the first '.' after collection name to get collection and method
    let (collection, method_call) = split_collection_and_method(rest)?;

    // Parse method name and arguments
    let (method_name, args_str) = parse_method_call(method_call)?;

    let operation = parse_operation(method_name, args_str)?;

    Ok(MongoQuery {
        database: None,
        collection: collection.to_string(),
        operation,
    })
}

fn parse_shell_syntax_positional(
    input: &str,
    base_offset: usize,
) -> Result<MongoQuery, MongoParseError> {
    let rest = input.strip_prefix("db.").ok_or_else(|| {
        MongoParseError::new("Expected 'db.' prefix", base_offset, input.len().min(3))
    })?;
    let rest_offset = base_offset + 3;

    let (collection, method_call) = split_collection_and_method(rest)
        .map_err(|e| MongoParseError::from_db_error(e, rest_offset))?;

    if collection.is_empty() {
        return Err(MongoParseError::new(
            "Collection name cannot be empty",
            rest_offset,
            1,
        ));
    }

    let method_offset = rest_offset + collection.len() + 1; // +1 for the dot
    let (method_name, args_str) = parse_method_call(method_call)
        .map_err(|e| MongoParseError::from_db_error(e, method_offset))?;

    let method_name_len = method_name.len();
    let args_offset = method_offset + method_name_len + 1; // +1 for '('

    let operation = parse_operation(method_name, args_str).map_err(|e| {
        let msg = e.to_string();
        if msg.contains("Unsupported method") {
            MongoParseError::new(msg, method_offset, method_name_len)
        } else {
            MongoParseError::new(msg, args_offset, args_str.len())
        }
    })?;

    Ok(MongoQuery {
        database: None,
        collection: collection.to_string(),
        operation,
    })
}

/// Split "collection.method(...)" into collection name and "method(...)"
fn split_collection_and_method(input: &str) -> Result<(&str, &str), DbError> {
    // Collection names can contain dots in MongoDB, but methods always end with ()
    // We need to find the method by looking for the pattern .methodName(

    // Find the opening parenthesis
    let paren_pos = input
        .find('(')
        .ok_or_else(|| DbError::query_failed("Expected method call with ()".to_string()))?;

    // Find the last '.' before the parenthesis
    let method_dot = input[..paren_pos]
        .rfind('.')
        .ok_or_else(|| DbError::query_failed("Expected collection.method format".to_string()))?;

    let collection = &input[..method_dot];
    let method_call = &input[method_dot + 1..];

    if collection.is_empty() {
        return Err(DbError::query_failed(
            "Collection name cannot be empty".to_string(),
        ));
    }

    Ok((collection, method_call))
}

/// Parse "methodName(...)" into method name and arguments string
fn parse_method_call(input: &str) -> Result<(&str, &str), DbError> {
    let paren_pos = input
        .find('(')
        .ok_or_else(|| DbError::query_failed("Expected '(' in method call".to_string()))?;

    let method_name = &input[..paren_pos];

    // Find matching closing parenthesis
    let args_start = paren_pos + 1;
    let args_end = find_matching_paren(input, paren_pos)?;
    let args_str = &input[args_start..args_end];

    Ok((method_name, args_str))
}

/// Find the matching closing parenthesis, accounting for nested parens/braces/brackets
fn find_matching_paren(input: &str, open_pos: usize) -> Result<usize, DbError> {
    let bytes = input.as_bytes();
    let mut depth = 0;
    let mut in_string = false;
    let mut escape_next = false;
    let mut string_char = b'"';

    for (i, &byte) in bytes.iter().enumerate().skip(open_pos) {
        if escape_next {
            escape_next = false;
            continue;
        }

        if byte == b'\\' && in_string {
            escape_next = true;
            continue;
        }

        if (byte == b'"' || byte == b'\'') && !in_string {
            in_string = true;
            string_char = byte;
            continue;
        }

        if byte == string_char && in_string {
            in_string = false;
            continue;
        }

        if in_string {
            continue;
        }

        match byte {
            b'(' | b'{' | b'[' => depth += 1,
            b')' | b'}' | b']' => {
                depth -= 1;
                if depth == 0 && byte == b')' {
                    return Ok(i);
                }
            }
            _ => {}
        }
    }

    Err(DbError::query_failed("Unmatched parenthesis".to_string()))
}

/// Parse operation from method name and arguments
fn parse_operation(method_name: &str, args_str: &str) -> Result<MongoOperation, DbError> {
    match method_name {
        "find" => parse_find_operation(args_str),
        "findOne" => parse_find_one_operation(args_str),
        "aggregate" => parse_aggregate_operation(args_str),
        "count" | "countDocuments" => parse_count_operation(args_str),
        "insertOne" => parse_insert_one_operation(args_str),
        "insertMany" => parse_insert_many_operation(args_str),
        "updateOne" => parse_update_one_operation(args_str),
        "updateMany" => parse_update_many_operation(args_str),
        "replaceOne" => parse_replace_one_operation(args_str),
        "deleteOne" => parse_delete_one_operation(args_str),
        "deleteMany" => parse_delete_many_operation(args_str),
        "drop" => Ok(MongoOperation::Drop),
        _ => Err(DbError::query_failed(format!(
            "Unsupported method: {}. Supported: find, findOne, aggregate, count, countDocuments, \
             insertOne, insertMany, updateOne, updateMany, replaceOne, deleteOne, deleteMany, drop",
            method_name
        ))),
    }
}

/// Parse find operation arguments
fn parse_find_operation(args_str: &str) -> Result<MongoOperation, DbError> {
    let args = parse_arguments(args_str)?;

    let filter = args
        .first()
        .map(|s| parse_relaxed_json(s))
        .transpose()?
        .unwrap_or_default();

    let projection = args.get(1).map(|s| parse_relaxed_json(s)).transpose()?;

    Ok(MongoOperation::Find {
        filter,
        projection,
        sort: None,
        limit: None,
        skip: None,
    })
}

/// Parse findOne operation (find with limit 1)
fn parse_find_one_operation(args_str: &str) -> Result<MongoOperation, DbError> {
    let args = parse_arguments(args_str)?;

    let filter = args
        .first()
        .map(|s| parse_relaxed_json(s))
        .transpose()?
        .unwrap_or_default();

    let projection = args.get(1).map(|s| parse_relaxed_json(s)).transpose()?;

    Ok(MongoOperation::Find {
        filter,
        projection,
        sort: None,
        limit: Some(1),
        skip: None,
    })
}

/// Parse aggregate operation arguments
fn parse_aggregate_operation(args_str: &str) -> Result<MongoOperation, DbError> {
    let args = parse_arguments(args_str)?;

    let pipeline_str = args
        .first()
        .ok_or_else(|| DbError::query_failed("aggregate requires a pipeline array".to_string()))?;

    let pipeline_json: serde_json::Value = serde_json::from_str(pipeline_str)
        .map_err(|e| DbError::query_failed(format!("Invalid pipeline JSON: {}", e)))?;

    let pipeline = json_array_to_bson_docs(&pipeline_json)?;

    Ok(MongoOperation::Aggregate { pipeline })
}

/// Parse count/countDocuments operation arguments
fn parse_count_operation(args_str: &str) -> Result<MongoOperation, DbError> {
    let args = parse_arguments(args_str)?;

    let filter = args
        .first()
        .filter(|s| !s.is_empty())
        .map(|s| parse_relaxed_json(s))
        .transpose()?
        .unwrap_or_default();

    Ok(MongoOperation::Count { filter })
}

/// Parse insertOne operation arguments
fn parse_insert_one_operation(args_str: &str) -> Result<MongoOperation, DbError> {
    let args = parse_arguments(args_str)?;

    let document_str = args
        .first()
        .ok_or_else(|| DbError::query_failed("insertOne requires a document".to_string()))?;

    let document = parse_relaxed_json(document_str)?;

    Ok(MongoOperation::InsertOne { document })
}

/// Parse insertMany operation arguments
fn parse_insert_many_operation(args_str: &str) -> Result<MongoOperation, DbError> {
    let args = parse_arguments(args_str)?;

    let array_str = args.first().ok_or_else(|| {
        DbError::query_failed("insertMany requires an array of documents".to_string())
    })?;

    let array_json: serde_json::Value = serde_json::from_str(array_str)
        .map_err(|e| DbError::query_failed(format!("Invalid documents array: {}", e)))?;

    let documents = json_array_to_bson_docs(&array_json)?;

    Ok(MongoOperation::InsertMany { documents })
}

/// Parse updateOne operation arguments
fn parse_update_one_operation(args_str: &str) -> Result<MongoOperation, DbError> {
    parse_update_operation(args_str, false)
}

/// Parse updateMany operation arguments
fn parse_update_many_operation(args_str: &str) -> Result<MongoOperation, DbError> {
    parse_update_operation(args_str, true)
}

/// Parse update operation (shared by updateOne/updateMany)
fn parse_update_operation(args_str: &str, many: bool) -> Result<MongoOperation, DbError> {
    let args = parse_arguments(args_str)?;

    if args.len() < 2 {
        return Err(DbError::query_failed(
            "update requires filter and update documents".to_string(),
        ));
    }

    let filter = parse_relaxed_json(&args[0])?;
    let update = parse_relaxed_json(&args[1])?;

    // Parse options (third argument)
    let upsert = args
        .get(2)
        .and_then(|s| parse_relaxed_json(s).ok())
        .and_then(|doc| doc.get_bool("upsert").ok())
        .unwrap_or(false);

    if many {
        Ok(MongoOperation::UpdateMany {
            filter,
            update,
            upsert,
        })
    } else {
        Ok(MongoOperation::UpdateOne {
            filter,
            update,
            upsert,
        })
    }
}

/// Parse deleteOne operation arguments
fn parse_delete_one_operation(args_str: &str) -> Result<MongoOperation, DbError> {
    parse_delete_operation(args_str, false)
}

/// Parse deleteMany operation arguments
fn parse_delete_many_operation(args_str: &str) -> Result<MongoOperation, DbError> {
    parse_delete_operation(args_str, true)
}

/// Parse delete operation (shared by deleteOne/deleteMany)
fn parse_delete_operation(args_str: &str, many: bool) -> Result<MongoOperation, DbError> {
    let args = parse_arguments(args_str)?;

    let filter = args
        .first()
        .map(|s| parse_relaxed_json(s))
        .transpose()?
        .unwrap_or_default();

    if many {
        Ok(MongoOperation::DeleteMany { filter })
    } else {
        Ok(MongoOperation::DeleteOne { filter })
    }
}

/// Parse replaceOne operation arguments
fn parse_replace_one_operation(args_str: &str) -> Result<MongoOperation, DbError> {
    let args = parse_arguments(args_str)?;

    if args.len() < 2 {
        return Err(DbError::query_failed(
            "replaceOne requires filter and replacement documents".to_string(),
        ));
    }

    let filter = parse_relaxed_json(&args[0])?;
    let replacement = parse_relaxed_json(&args[1])?;

    // Parse options (third argument)
    let upsert = args
        .get(2)
        .and_then(|s| parse_relaxed_json(s).ok())
        .and_then(|doc| doc.get_bool("upsert").ok())
        .unwrap_or(false);

    Ok(MongoOperation::ReplaceOne {
        filter,
        replacement,
        upsert,
    })
}

/// Parse comma-separated arguments, handling nested braces/brackets/strings
fn parse_arguments(args_str: &str) -> Result<Vec<String>, DbError> {
    let trimmed = args_str.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let mut args = Vec::new();
    let mut current = String::new();
    let mut depth = 0;
    let mut in_string = false;
    let mut escape_next = false;
    let mut string_char = '"';

    for ch in trimmed.chars() {
        if escape_next {
            current.push(ch);
            escape_next = false;
            continue;
        }

        if ch == '\\' && in_string {
            current.push(ch);
            escape_next = true;
            continue;
        }

        if (ch == '"' || ch == '\'') && !in_string {
            in_string = true;
            string_char = ch;
            current.push(ch);
            continue;
        }

        if ch == string_char && in_string {
            in_string = false;
            current.push(ch);
            continue;
        }

        if in_string {
            current.push(ch);
            continue;
        }

        match ch {
            '{' | '[' | '(' => {
                depth += 1;
                current.push(ch);
            }
            '}' | ']' | ')' => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 => {
                let arg = current.trim().to_string();
                if !arg.is_empty() {
                    args.push(arg);
                }
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
    }

    let final_arg = current.trim().to_string();
    if !final_arg.is_empty() {
        args.push(final_arg);
    }

    Ok(args)
}

/// Parse relaxed JSON (MongoDB extended JSON / shell syntax)
fn parse_relaxed_json(input: &str) -> Result<Document, DbError> {
    let trimmed = input.trim();

    // Try standard JSON first
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(trimmed) {
        return json_to_bson_doc(&json);
    }

    // Try to fix common relaxed JSON patterns:
    // - Unquoted keys: {name: "value"} -> {"name": "value"}
    // - Single quotes: {'name': 'value'} -> {"name": "value"}
    let normalized = normalize_relaxed_json(trimmed);

    let json: serde_json::Value = serde_json::from_str(&normalized)
        .map_err(|e| DbError::query_failed(format!("Invalid JSON: {}", e)))?;

    json_to_bson_doc(&json)
}

/// Normalize relaxed JSON to strict JSON
fn normalize_relaxed_json(input: &str) -> String {
    let mut result = String::with_capacity(input.len() * 2);
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let ch = chars[i];

        // Handle strings (preserve as-is, just convert single to double quotes)
        if ch == '"' || ch == '\'' {
            let quote = ch;
            let target_quote = '"';
            result.push(target_quote);
            i += 1;

            while i < chars.len() {
                let inner = chars[i];
                if inner == '\\' && i + 1 < chars.len() {
                    result.push(inner);
                    result.push(chars[i + 1]);
                    i += 2;
                } else if inner == quote {
                    result.push(target_quote);
                    i += 1;
                    break;
                } else {
                    result.push(inner);
                    i += 1;
                }
            }
            continue;
        }

        // Check for unquoted key after { or ,
        if ch == '{' || ch == ',' {
            result.push(ch);
            i += 1;

            // Skip whitespace
            while i < chars.len() && chars[i].is_whitespace() {
                result.push(chars[i]);
                i += 1;
            }

            // Check if this looks like an unquoted key
            if i < chars.len() && is_key_start_char(chars[i]) {
                // Collect the key
                let key_start = i;
                while i < chars.len() && is_key_char(chars[i]) {
                    i += 1;
                }
                let key = &chars[key_start..i];

                // Skip whitespace after key
                while i < chars.len() && chars[i].is_whitespace() {
                    i += 1;
                }

                // Check if followed by colon (confirming it's a key)
                if i < chars.len() && chars[i] == ':' {
                    result.push('"');
                    for &c in key {
                        result.push(c);
                    }
                    result.push('"');
                } else {
                    // Not a key, output as-is
                    for &c in key {
                        result.push(c);
                    }
                }
            }
            continue;
        }

        result.push(ch);
        i += 1;
    }

    result
}

fn is_key_start_char(ch: char) -> bool {
    ch.is_alphabetic() || ch == '_' || ch == '$'
}

fn is_key_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_' || ch == '$'
}

/// Parse JSON format (backward compatibility)
fn parse_json_format(input: &str) -> Result<MongoQuery, DbError> {
    let json: serde_json::Value = serde_json::from_str(input)
        .map_err(|e| DbError::query_failed(format!("Invalid JSON: {}", e)))?;

    let obj = json
        .as_object()
        .ok_or_else(|| DbError::query_failed("Query must be a JSON object".to_string()))?;

    let database = obj
        .get("database")
        .and_then(|v| v.as_str())
        .map(String::from);

    let collection = obj
        .get("collection")
        .and_then(|v| v.as_str())
        .ok_or_else(|| DbError::query_failed("Missing 'collection' field".to_string()))?
        .to_string();

    // Determine operation type
    let operation = if obj.contains_key("aggregate") || obj.contains_key("pipeline") {
        let pipeline_val = obj
            .get("aggregate")
            .or_else(|| obj.get("pipeline"))
            .ok_or_else(|| DbError::query_failed("Missing pipeline for aggregate".to_string()))?;

        let pipeline = json_array_to_bson_docs(pipeline_val)?;
        MongoOperation::Aggregate { pipeline }
    } else if obj.contains_key("count") {
        let filter = obj
            .get("count")
            .and_then(|v| json_to_bson_doc(v).ok())
            .unwrap_or_default();
        MongoOperation::Count { filter }
    } else if obj.contains_key("replace") {
        let replace_obj = obj
            .get("replace")
            .and_then(|v| v.as_object())
            .ok_or_else(|| DbError::query_failed("replace must be an object".to_string()))?;

        let filter = replace_obj
            .get("filter")
            .ok_or_else(|| DbError::query_failed("replace.filter is required".to_string()))
            .and_then(json_to_bson_doc)?;

        let replacement = replace_obj
            .get("replacement")
            .ok_or_else(|| DbError::query_failed("replace.replacement is required".to_string()))
            .and_then(json_to_bson_doc)?;

        let upsert = replace_obj
            .get("upsert")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        MongoOperation::ReplaceOne {
            filter,
            replacement,
            upsert,
        }
    } else {
        // Default to find operation
        let filter = obj
            .get("filter")
            .and_then(|v| json_to_bson_doc(v).ok())
            .unwrap_or_default();

        let projection = obj.get("projection").and_then(|v| json_to_bson_doc(v).ok());

        let sort = obj.get("sort").and_then(|v| json_to_bson_doc(v).ok());
        let limit = obj.get("limit").and_then(|v| v.as_i64());
        let skip = obj.get("skip").and_then(|v| v.as_u64());

        MongoOperation::Find {
            filter,
            projection,
            sort,
            limit,
            skip,
        }
    };

    Ok(MongoQuery {
        database,
        collection,
        operation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_find() {
        let query = parse_query(r#"db.users.find({"name": "John"})"#).unwrap();
        assert_eq!(query.collection, "users");
        assert!(matches!(query.operation, MongoOperation::Find { .. }));
    }

    #[test]
    fn test_parse_find_empty() {
        let query = parse_query("db.users.find()").unwrap();
        assert_eq!(query.collection, "users");
        assert!(matches!(query.operation, MongoOperation::Find { .. }));
    }

    #[test]
    fn test_parse_find_with_projection() {
        let query = parse_query(r#"db.users.find({"active": true}, {"name": 1})"#).unwrap();
        assert_eq!(query.collection, "users");
        assert!(
            matches!(
                query.operation,
                MongoOperation::Find {
                    projection: Some(_),
                    ..
                }
            ),
            "Expected Find operation with projection, got: {:?}",
            query.operation
        );
    }

    #[test]
    fn test_parse_find_one() {
        let query = parse_query(r#"db.users.findOne({"_id": "123"})"#).unwrap();
        assert_eq!(query.collection, "users");
        assert!(
            matches!(query.operation, MongoOperation::Find { limit: Some(1), .. }),
            "Expected Find operation with limit=1, got: {:?}",
            query.operation
        );
    }

    #[test]
    fn test_parse_aggregate() {
        let query =
            parse_query(r#"db.orders.aggregate([{"$match": {"status": "active"}}])"#).unwrap();
        assert_eq!(query.collection, "orders");
        assert!(matches!(query.operation, MongoOperation::Aggregate { .. }));
    }

    #[test]
    fn test_parse_count() {
        let query = parse_query(r#"db.products.count({"active": true})"#).unwrap();
        assert_eq!(query.collection, "products");
        assert!(matches!(query.operation, MongoOperation::Count { .. }));
    }

    #[test]
    fn test_parse_count_documents() {
        let query = parse_query("db.products.countDocuments()").unwrap();
        assert_eq!(query.collection, "products");
        assert!(matches!(query.operation, MongoOperation::Count { .. }));
    }

    #[test]
    fn test_parse_insert_one() {
        let query = parse_query(r#"db.users.insertOne({"name": "Alice", "age": 30})"#).unwrap();
        assert_eq!(query.collection, "users");
        assert!(matches!(query.operation, MongoOperation::InsertOne { .. }));
    }

    #[test]
    fn test_parse_insert_many() {
        let query = parse_query(r#"db.users.insertMany([{"name": "A"}, {"name": "B"}])"#).unwrap();
        assert_eq!(query.collection, "users");
        if let MongoOperation::InsertMany { documents } = &query.operation {
            assert_eq!(documents.len(), 2);
        } else {
            panic!("Expected InsertMany operation, got: {:?}", query.operation);
        }
    }

    #[test]
    fn test_parse_update_one() {
        let query = parse_query(r#"db.users.updateOne({"_id": "123"}, {"$set": {"name": "Bob"}})"#)
            .unwrap();
        assert_eq!(query.collection, "users");
        assert!(matches!(query.operation, MongoOperation::UpdateOne { .. }));
    }

    #[test]
    fn test_parse_update_many() {
        let query =
            parse_query(r#"db.users.updateMany({"active": false}, {"$set": {"archived": true}})"#)
                .unwrap();
        assert_eq!(query.collection, "users");
        assert!(matches!(query.operation, MongoOperation::UpdateMany { .. }));
    }

    #[test]
    fn test_parse_replace_one() {
        let query = parse_query(
            r#"db.users.replaceOne({"_id": "123"}, {"_id": "123", "name": "New Name"})"#,
        )
        .unwrap();
        assert_eq!(query.collection, "users");
        assert!(matches!(query.operation, MongoOperation::ReplaceOne { .. }));
    }

    #[test]
    fn test_parse_json_format_replace() {
        let query = parse_query(
            r#"{"collection": "users", "replace": {"filter": {"_id": "123"}, "replacement": {"name": "New"}}}"#,
        )
        .unwrap();
        assert_eq!(query.collection, "users");
        assert!(matches!(query.operation, MongoOperation::ReplaceOne { .. }));
    }

    #[test]
    fn test_parse_delete_one() {
        let query = parse_query(r#"db.users.deleteOne({"_id": "123"})"#).unwrap();
        assert_eq!(query.collection, "users");
        assert!(matches!(query.operation, MongoOperation::DeleteOne { .. }));
    }

    #[test]
    fn test_parse_delete_many() {
        let query = parse_query(r#"db.users.deleteMany({"archived": true})"#).unwrap();
        assert_eq!(query.collection, "users");
        assert!(matches!(query.operation, MongoOperation::DeleteMany { .. }));
    }

    #[test]
    fn test_parse_drop() {
        let query = parse_query("db.temp_collection.drop()").unwrap();
        assert_eq!(query.collection, "temp_collection");
        assert!(matches!(query.operation, MongoOperation::Drop));
    }

    #[test]
    fn test_parse_collection_with_dots() {
        let query = parse_query("db.system.users.find()").unwrap();
        assert_eq!(query.collection, "system.users");
    }

    #[test]
    fn test_parse_relaxed_json_unquoted_keys() {
        let query = parse_query(r#"db.users.find({name: "John", active: true})"#).unwrap();
        assert_eq!(query.collection, "users");
    }

    #[test]
    fn test_parse_json_format_backward_compat() {
        let query = parse_query(r#"{"collection": "users", "filter": {"name": "John"}}"#).unwrap();
        assert_eq!(query.collection, "users");
        assert!(matches!(query.operation, MongoOperation::Find { .. }));
    }

    #[test]
    fn test_parse_json_format_aggregate() {
        let query =
            parse_query(r#"{"collection": "orders", "aggregate": [{"$match": {}}]}"#).unwrap();
        assert_eq!(query.collection, "orders");
        assert!(matches!(query.operation, MongoOperation::Aggregate { .. }));
    }

    #[test]
    fn test_unsupported_method() {
        let result = parse_query("db.users.unknownMethod()");
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_collection() {
        let result = parse_query("db.find()");
        assert!(result.is_err());
    }
}
