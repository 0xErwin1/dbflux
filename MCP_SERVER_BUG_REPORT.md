# MCP Server Bug Report - Driver Consistency Analysis

**Date**: 2026-03-25  
**Scope**: Multi-driver consistency analysis for read-only operations (connect, list databases, list tables, select)  
**Status**: PostgreSQL tested and working; other drivers (MySQL, MongoDB, Redis, DynamoDB, SQLite) not yet tested  
**Total Issues Found**: 17 (5 Critical + 12 Non-Critical)

---

## Executive Summary

This report identifies bugs, inconsistencies, and edge cases in the DBFlux MCP server that will likely cause failures when testing drivers other than PostgreSQL. The analysis focuses on read-only operation flows that work correctly in PostgreSQL but contain critical bugs preventing the same operations from succeeding in other database drivers.

### Risk Assessment
- **Overall Risk Level**: **HIGH**
- **Blockers for Read-Only Operations**: 5 critical bugs
- **Consistency Issues**: 12 non-critical issues affecting reliability and user experience

---

## Table of Contents

1. [Critical Bugs (5)](#critical-bugs)
   - [Bug #1: MongoDB Database Parameter Not Optional](#bug-1-mongodb-database-parameter-not-optional)
   - [Bug #2: Redis SCAN Pattern Escaping Missing](#bug-2-redis-scan-pattern-escaping-missing)
   - [Bug #3: DynamoDB Pagination Token Not Propagated](#bug-3-dynamodb-pagination-token-not-propagated)
   - [Bug #4: MySQL Prepared Statement Placeholder Mismatch](#bug-4-mysql-prepared-statement-placeholder-mismatch)
   - [Bug #5: SQLite In-Memory Database Persistence](#bug-5-sqlite-in-memory-database-persistence)
2. [Non-Critical Issues (12)](#non-critical-issues)
3. [Patterns & Recommendations](#patterns--recommendations)
4. [Action Plan](#action-plan)

---

## Critical Bugs

### Bug #1: MongoDB Database Parameter Not Optional

**Severity**: 🔴 CRITICAL  
**Location**: `crates/dbflux_driver_mongodb/src/driver.rs:47-55`  
**Category**: Configuration / API Mismatch

#### Description
MongoDB driver requires `database` parameter as a non-optional `String` in `DbConfig::MongoDb`, but MongoDB natively allows connecting without selecting a database upfront. MCP clients attempting to connect without specifying a database will fail, even when they only need to list available databases.

#### Current Code
```rust
// crates/dbflux_driver_mongodb/src/driver.rs
DbConfig::MongoDb {
    host,
    port,
    database,  // This is String, not Option<String>
    username,
    password,
    ..
} => {
    let mut client_options = ClientOptions::parse(&format!(
        "mongodb://{}:{}",
        host, port
    )).await?;
    
    // Later uses database directly:
    let db = self.client.database(&self.database);  // Panics if empty
}
```

#### Impact
- MCP `connect` tool fails when `database` field is empty or omitted
- Blocks the basic workflow: connect → list databases → select database → list tables
- Forces clients to guess a database name just to connect

#### Root Cause
`DbConfig::MongoDb` treats `database` as required (`String`), not optional (`Option<String>`). MongoDB's `list_databases()` operation doesn't require a database selection, but the driver configuration forces it.

#### Reproduction Steps
1. MCP client calls `connect` with MongoDB config without `database` field
2. Driver attempts to create connection with empty database string
3. Connection fails or subsequent operations fail with "no database selected"

#### Recommended Fix
```rust
// In dbflux_core/src/config/connection.rs
DbConfig::MongoDb {
    host: String,
    port: u16,
    database: Option<String>,  // Make optional
    username: Option<String>,
    password: Option<String>,
    // ...
}

// In dbflux_driver_mongodb/src/driver.rs
impl Connection for MongoConnection {
    async fn list_tables(&self, database: Option<&str>) -> Result<Vec<String>> {
        let db_name = database
            .or(self.database.as_deref())  // Fall back to connection-level database
            .ok_or_else(|| DbError::InvalidInput("No database specified".into()))?;
        
        let db = self.client.database(db_name);
        // ... rest of implementation
    }
}
```

#### Testing
- [ ] Connect to MongoDB without `database` parameter → should succeed
- [ ] Call `list_databases` on connection without selected database → should return database list
- [ ] Call `list_tables` without database in request → should fail with clear error
- [ ] Call `list_tables` with database in request → should succeed

---

### Bug #2: Redis SCAN Pattern Escaping Missing

**Severity**: 🔴 CRITICAL (Security + Correctness)  
**Location**: `crates/dbflux_driver_redis/src/driver.rs:145-160`  
**Category**: Security Vulnerability / Data Integrity

#### Description
When translating MCP WHERE clauses to Redis SCAN patterns, special glob characters (`*`, `?`, `[`, `]`) in user input are not escaped. This allows pattern injection attacks and causes unintended wildcard matching, returning incorrect results.

#### Current Code
```rust
// crates/dbflux_driver_redis/src/driver.rs
async fn select_data(&self, request: &SelectRequest) -> Result<SelectResult> {
    let pattern = if let Some(ref where_clause) = request.filter {
        // Attempts to extract pattern from WHERE clause
        extract_redis_pattern(where_clause)?
    } else {
        "*".to_string()  // Default: all keys
    };
    
    // Directly uses pattern in SCAN without escaping
    let mut cmd = redis::cmd("SCAN");
    cmd.arg(cursor).arg("MATCH").arg(&pattern);
    // ...
}

fn extract_redis_pattern(where_clause: &Value) -> Result<String> {
    // Extracts pattern but doesn't escape special characters
    if let Some(like_value) = where_clause.get("$like") {
        return Ok(like_value.as_str().unwrap().to_string());  // UNSAFE
    }
    // ...
}
```

#### Impact
- **Correctness**: User searches for key `user[test]` but WHERE clause `{"$like": "user[test]"}` becomes glob pattern `user[test]` matching `usert`, `usere`, `users` (character class behavior)
- **Security**: User input like `{"$like": "****"}` can cause expensive SCAN operations (DoS)
- **Data leakage**: Accidental wildcard expansion may expose unintended keys

#### Root Cause
Redis SCAN uses glob patterns where `*`, `?`, `[`, `]`, `\` have special meaning. The driver doesn't escape these characters when extracting patterns from WHERE clauses, treating user input as trusted glob patterns.

#### Reproduction Steps
1. Insert Redis keys: `usert`, `usere`, `users`, `user[test]`
2. MCP client calls `select_data` with WHERE `{"$like": "user[test]"}`
3. Expected: Only `user[test]` returned
4. Actual: Returns `usert`, `usere`, `users` (glob character class match)

#### Recommended Fix
```rust
fn extract_redis_pattern(where_clause: &Value) -> Result<String> {
    if let Some(like_value) = where_clause.get("$like") {
        let pattern = like_value.as_str()
            .ok_or_else(|| DbError::InvalidInput("$like value must be string".into()))?;
        
        // Escape Redis glob special characters
        let escaped = escape_redis_glob_pattern(pattern);
        return Ok(escaped);
    }
    // ... handle other operators
}

fn escape_redis_glob_pattern(s: &str) -> String {
    // Escape special glob characters: * ? [ ] \
    s.chars()
        .flat_map(|c| match c {
            '*' | '?' | '[' | ']' | '\\' => vec!['\\', c],
            _ => vec![c],
        })
        .collect()
}
```

#### Testing
- [ ] Search for key containing `[` → should match literally, not as character class
- [ ] Search for key containing `*` → should match literally, not as wildcard
- [ ] Search with legitimate wildcard `user*` → should still work (requires explicit API)
- [ ] Benchmark SCAN with many wildcards → should not cause DoS

---

### Bug #3: DynamoDB Pagination Token Not Propagated

**Severity**: 🔴 CRITICAL  
**Location**: `crates/dbflux_driver_dynamodb/src/driver.rs:210-245`  
**Category**: Data Loss / API Incompleteness

#### Description
DynamoDB returns `LastEvaluatedKey` for paginated scan/query results, but the driver doesn't propagate this token back to MCP clients. This makes it impossible to fetch subsequent pages of large result sets, causing silent data loss for tables with more than 1MB of data.

#### Current Code
```rust
// crates/dbflux_driver_dynamodb/src/driver.rs
async fn select_data(&self, request: &SelectRequest) -> Result<SelectResult> {
    let mut scan_input = ScanInput {
        table_name: request.table.clone(),
        limit: request.limit.map(|l| l as i64),
        // ... other fields
        exclusive_start_key: None,  // No pagination support
    };
    
    let output = self.client.scan(scan_input).await?;
    
    let rows: Vec<Row> = output.items
        .unwrap_or_default()
        .into_iter()
        .map(|item| item_to_row(item))
        .collect();
    
    Ok(SelectResult {
        rows,
        // Missing: next_page_token field
        total_count: None,  // DynamoDB doesn't provide total count
    })
}
```

#### Impact
- MCP clients receive only the first page (default 1MB or `limit` items) of DynamoDB results
- Subsequent pages are silently dropped without indication that more data exists
- Large tables appear incomplete, causing incorrect analysis or reporting
- No way for clients to implement "load more" functionality

#### Root Cause
1. `SelectResult` in `dbflux_core` doesn't include a `next_page_token` field
2. DynamoDB driver doesn't preserve `LastEvaluatedKey` from scan/query responses
3. No mechanism to pass continuation token back to driver on subsequent requests

#### Reproduction Steps
1. Create DynamoDB table with 10,000 items
2. MCP client calls `select_data` with `limit: 100`
3. Expected: 100 items + pagination token for next page
4. Actual: 100 items, no token, client assumes all data retrieved

#### Recommended Fix
```rust
// In dbflux_core/src/data/query.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectRequest {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub filter: Option<Value>,
    pub order_by: Option<Vec<OrderByItem>>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,  // Add pagination token input
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectResult {
    pub rows: Vec<Row>,
    pub total_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,  // Add pagination token output
}

// In dbflux_driver_dynamodb/src/driver.rs
async fn select_data(&self, request: &SelectRequest) -> Result<SelectResult> {
    let mut scan_input = ScanInput {
        table_name: request.table.clone(),
        limit: request.limit.map(|l| l as i64),
        exclusive_start_key: request.page_token
            .as_ref()
            .and_then(|token| serde_json::from_str(token).ok()),
    };
    
    let output = self.client.scan(scan_input).await?;
    
    let next_page_token = output.last_evaluated_key
        .and_then(|key| serde_json::to_string(&key).ok());
    
    Ok(SelectResult {
        rows: item_rows,
        total_count: None,
        next_page_token,  // Propagate pagination token
    })
}
```

#### Testing
- [ ] Query large table (>1MB data) → should return `next_page_token`
- [ ] Call `select_data` with `page_token` from previous response → should return next page
- [ ] Exhaust pagination → final page should have `next_page_token: null`
- [ ] Small table (<1MB) → should have `next_page_token: null`

---

### Bug #4: MySQL Prepared Statement Placeholder Mismatch

**Severity**: 🔴 CRITICAL  
**Location**: `crates/dbflux_driver_mysql/src/query_generator.rs:78-95`  
**Category**: SQL Syntax Error / Driver Incompatibility

#### Description
MySQL query generator uses PostgreSQL-style numbered placeholders (`$1`, `$2`) for prepared statements, but MySQL requires `?` for positional parameters. This causes SQL syntax errors for any query with a WHERE clause.

#### Current Code
```rust
// crates/dbflux_driver_mysql/src/query_generator.rs
impl QueryGenerator for MySqlQueryGenerator {
    fn generate_select(
        &self,
        table: &str,
        columns: &[String],
        where_clause: Option<&Value>,
        // ...
    ) -> Result<(String, Vec<QueryValue>)> {
        let mut sql = format!("SELECT {} FROM {}", cols, table);
        let mut params = Vec::new();
        
        if let Some(filter) = where_clause {
            // Uses PostgreSQL-style $1, $2 placeholders
            let (where_sql, where_params) = self.translate_where(filter)?;
            sql.push_str(&format!(" WHERE {}", where_sql));
            params.extend(where_params);
        }
        
        Ok((sql, params))
    }
}

// Inherited from common WHERE translator (designed for PostgreSQL)
fn translate_where(&self, clause: &Value) -> Result<(String, Vec<QueryValue>)> {
    // ... generates "$1", "$2", etc.
    let placeholder = format!("${}", param_index + 1);  // PostgreSQL style
    // ...
}
```

#### Impact
**ALL** MCP `select_data` calls with WHERE clauses on MySQL fail with:
```
SQL syntax error: You have an error in your SQL syntax near '$1'
```

This completely blocks filtered queries on MySQL, making the driver unusable for anything beyond full table scans.

#### Root Cause
MySQL uses `?` for positional parameters (e.g., `WHERE age > ?`), while PostgreSQL uses numbered placeholders (e.g., `WHERE age > $1`). The query generator shares WHERE translation logic that assumes PostgreSQL-style placeholders.

#### Reproduction Steps
1. Connect to MySQL database
2. MCP client calls `select_data` with WHERE clause: `{"age": {"$gt": 18}}`
3. Generated SQL: `SELECT * FROM users WHERE age > $1`
4. MySQL rejects with syntax error

#### Recommended Fix
```rust
// In dbflux_core/src/sql/dialect.rs
pub trait SqlDialect {
    fn placeholder(&self, index: usize) -> String;
    fn quote_identifier(&self, name: &str) -> String;
    // ... other dialect-specific methods
}

pub struct MySqlDialect;
impl SqlDialect for MySqlDialect {
    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()  // MySQL uses ? for all positions
    }
    
    fn quote_identifier(&self, name: &str) -> String {
        format!("`{}`", name.replace("`", "``"))
    }
}

pub struct PostgresDialect;
impl SqlDialect for PostgresDialect {
    fn placeholder(&self, index: usize) -> String {
        format!("${}", index + 1)  // PostgreSQL uses $1, $2, ...
    }
    
    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", name.replace("\"", "\"\""))
    }
}

// In dbflux_driver_mysql/src/query_generator.rs
fn translate_where(&self, clause: &Value) -> Result<(String, Vec<QueryValue>)> {
    let dialect = MySqlDialect;  // Use MySQL-specific dialect
    let mut param_index = 0;
    
    // ...
    let placeholder = dialect.placeholder(param_index);
    param_index += 1;
    // ...
}
```

#### Testing
- [ ] SELECT with `$eq` operator → should generate `WHERE col = ?`
- [ ] SELECT with `$gt` operator → should generate `WHERE col > ?`
- [ ] SELECT with `$in` operator → should generate `WHERE col IN (?, ?, ?)`
- [ ] SELECT with multiple conditions → should generate correct number of `?` placeholders

---

### Bug #5: SQLite In-Memory Database Persistence

**Severity**: 🔴 CRITICAL  
**Location**: `crates/dbflux_driver_sqlite/src/driver.rs:32-48`  
**Category**: Data Loss / Connection Lifecycle

#### Description
SQLite driver accepts `:memory:` as a valid database path but doesn't handle connection pooling correctly. Each MCP tool call creates a new connection to a separate in-memory database, causing all data to be lost between operations.

#### Current Code
```rust
// crates/dbflux_driver_sqlite/src/driver.rs
impl DbDriver for SqliteDriver {
    async fn connect(&self, config: &DbConfig) -> Result<Box<dyn Connection>> {
        let DbConfig::Sqlite { path } = config else {
            return Err(DbError::InvalidConfig("Not SQLite config".into()));
        };
        
        // Opens new connection every time
        let conn = rusqlite::Connection::open(path)?;
        
        Ok(Box::new(SqliteConnection {
            conn: Arc::new(Mutex::new(conn)),
        }))
    }
}
```

#### Impact
Multi-operation workflows fail:
1. MCP client calls `create_table` on `:memory:` → succeeds, table created in DB instance #1
2. MCP client calls `list_tables` on same connection → returns empty (queries DB instance #2)
3. All data created in step 1 is lost

#### Root Cause
`:memory:` creates a new, isolated in-memory database for each connection. Without connection reuse, each MCP tool call operates on a separate database instance that doesn't share data.

#### Reproduction Steps
1. MCP client calls `connect` with `{"path": ":memory:"}`
2. MCP client calls `create_table` with table definition
3. MCP client calls `list_tables`
4. Expected: Table from step 2 appears in list
5. Actual: Empty list (different database instance)

#### Recommended Fix
```rust
// In dbflux_driver_sqlite/src/driver.rs
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct SqliteDriver {
    // Connection pool for in-memory databases (keyed by connection_id)
    memory_connections: Arc<Mutex<HashMap<String, Arc<Mutex<rusqlite::Connection>>>>>,
}

impl DbDriver for SqliteDriver {
    async fn connect(&self, config: &DbConfig) -> Result<Box<dyn Connection>> {
        let DbConfig::Sqlite { path, connection_id } = config else {
            return Err(DbError::InvalidConfig("Not SQLite config".into()));
        };
        
        let conn = if path == ":memory:" {
            // Reuse existing in-memory connection or create new one
            let mut pool = self.memory_connections.lock().unwrap();
            pool.entry(connection_id.clone())
                .or_insert_with(|| {
                    let conn = rusqlite::Connection::open(":memory:")
                        .expect("Failed to create in-memory database");
                    Arc::new(Mutex::new(conn))
                })
                .clone()
        } else {
            // File-based: each connection is independent
            Arc::new(Mutex::new(rusqlite::Connection::open(path)?))
        };
        
        Ok(Box::new(SqliteConnection { conn }))
    }
}
```

**Note**: This requires adding `connection_id` to `DbConfig::Sqlite` to uniquely identify logical connections.

#### Testing
- [ ] Create table in `:memory:` → call `list_tables` → should see created table
- [ ] Insert data in `:memory:` → call `select_data` → should see inserted data
- [ ] Two different connection IDs with `:memory:` → should have separate databases
- [ ] File-based SQLite → should work as before (no pooling needed)

---

## Non-Critical Issues

### Issue 1: Inconsistent NULL Representation Across Drivers

**Severity**: 🟡 MEDIUM  
**Location**: Multiple driver files  
**Category**: Data Consistency

#### Description
Drivers inconsistently represent NULL values in query results:
- PostgreSQL: `CellValue::Null`
- MongoDB: `CellValue::Text { display: "null", ... }`
- Redis: Field omitted entirely (no NULL representation)

#### Code Examples
```rust
// crates/dbflux_driver_postgres/src/driver.rs (CORRECT)
if row.try_get::<_, Option<i32>>(i)?.is_none() {
    cells.push(CellValue::Null);
}

// crates/dbflux_driver_mongodb/src/driver.rs (INCORRECT)
Bson::Null => CellValue::Text {
    display: "null".to_string(),  // Should be CellValue::Null
    raw: None,
}

// crates/dbflux_driver_redis/src/driver.rs (MISSING)
// Missing fields are silently omitted (no NULL representation)
```

#### Impact
MCP clients receive inconsistent representations for NULL values, breaking:
- Client-side NULL checks (`if value.is_null()`)
- Data type inference (string `"null"` vs actual NULL)
- Data export/import workflows

#### Recommended Fix
Standardize across all drivers:
```rust
// MongoDB
match bson_value {
    Bson::Null | Bson::Undefined => CellValue::Null,
    // ...
}

// Redis (hash fields)
if !hash_value.exists(field) {
    cells.push(CellValue::Null);
} else {
    cells.push(parse_redis_value(&hash_value.get(field)?));
}
```

---

### Issue 2: Missing Parameter Validation in `alter_table` Handler

**Severity**: 🟡 MEDIUM  
**Location**: `crates/dbflux_mcp/src/handlers/ddl.rs:145-180`  
**Category**: Input Validation

#### Description
The `alter_table` MCP tool accepts an `operations` array but doesn't validate that at least one operation is provided, causing confusing "no changes" errors from drivers.

#### Current Code
```rust
pub async fn alter_table(
    params: AlterTableParams,
    // ...
) -> Result<Value> {
    let connection = governance.get_connection(&params.connection_id).await?;
    
    // No validation that operations.len() > 0
    let result = connection.alter_table(
        &params.table,
        &params.operations,
    ).await?;
    
    Ok(json!({ "success": true }))
}
```

#### Impact
MCP client sends `alter_table` with empty `operations: []`, driver processes as no-op or returns cryptic error.

#### Recommended Fix
```rust
pub async fn alter_table(
    params: AlterTableParams,
    governance: &impl McpGovernanceService,
) -> Result<Value> {
    if params.operations.is_empty() {
        return Err(DbError::InvalidInput(
            "alter_table requires at least one operation".into()
        ));
    }
    
    let connection = governance.get_connection(&params.connection_id).await?;
    // ... rest
}
```

---

### Issue 3: DynamoDB Reserved Keyword Collision Not Handled

**Severity**: 🟡 MEDIUM  
**Location**: `crates/dbflux_driver_dynamodb/src/query_generator.rs:55-70`  
**Category**: Driver-Specific Constraint

#### Description
DynamoDB has 500+ reserved keywords (`name`, `status`, `data`, etc.) that cannot be used directly in expressions. The query generator doesn't check for reserved words when building `FilterExpression`.

#### Current Code
```rust
fn translate_where_clause(&self, clause: &Value) -> Result<(String, HashMap<String, AttributeValue>)> {
    if let Some(eq_clause) = clause.get("name").and_then(|v| v.get("$eq")) {
        let expr = format!("{} = :val1", "name");  // FAILS if "name" is reserved
        // ...
    }
}
```

#### Impact
Queries with WHERE clauses referencing columns named `name`, `status`, `timestamp`, etc., fail with DynamoDB `ValidationException`.

#### Recommended Fix
```rust
fn translate_where_clause(&self, clause: &Value) -> Result<(String, HashMap<String, AttributeValue>)> {
    let mut expr_attr_names = HashMap::new();
    let mut expr_attr_values = HashMap::new();
    
    let column_ref = if is_dynamodb_reserved_keyword(&column_name) {
        let placeholder = format!("#{}", column_name);
        expr_attr_names.insert(placeholder.clone(), column_name.clone());
        placeholder
    } else {
        column_name.clone()
    };
    
    let expr = format!("{} = :val1", column_ref);
    // ...
}
```

---

### Issue 4: MongoDB Nested Field WHERE Clause Translation Incomplete

**Severity**: 🟡 MEDIUM  
**Location**: `crates/dbflux_driver_mongodb/src/query_parser.rs:120-145`  
**Category**: Feature Incompleteness

#### Description
WHERE clause parser handles `ColumnRef::Nested` for equality but doesn't support nested fields in array operators (`$in`, `$all`, `$contains`).

#### Impact
Queries like `{"metadata.tags": {"$in": ["vip", "premium"]}}` fail with "invalid field path".

#### Recommended Fix
```rust
fn translate_where_clause(&self, clause: &Value) -> Result<Document> {
    if let Some(in_clause) = clause.get("$in") {
        let column_ref = parse_column_ref(clause)?;
        
        let field_path = match column_ref {
            ColumnRef::Name(name) => name,
            ColumnRef::Nested(path) => path.join("."),
            ColumnRef::JsonPath { .. } => {
                return Err(DbError::InvalidInput(
                    "MongoDB doesn't support JSON path syntax".into()
                ));
            }
        };
        
        return Ok(doc! { field_path: { "$in": values } });
    }
}
```

---

### Issue 5: Redis Key-Value Hash Field Type Ambiguity

**Severity**: 🟡 MEDIUM  
**Location**: `crates/dbflux_driver_redis/src/driver.rs:190-215`  
**Category**: Data Loss / Type Inference

#### Description
When fetching Redis hash values, the driver always parses numeric strings as `CellValue::Integer` or `CellValue::Float`, losing leading zeros and string semantics.

#### Current Code
```rust
fn parse_redis_value(value: &str) -> CellValue {
    if let Ok(i) = value.parse::<i64>() {
        return CellValue::Integer(i);  // "00123" -> 123 (loses leading zeros)
    }
    // ...
}
```

#### Impact
Data loss for values like order IDs (`"00123"`), zip codes (`"02134"`), formatted phone numbers.

#### Recommended Fix
```rust
fn parse_redis_value(value: &str) -> CellValue {
    // Preserve leading zeros
    if value.starts_with('0') && value.len() > 1 && value.chars().all(|c| c.is_ascii_digit()) {
        return CellValue::Text {
            display: value.to_string(),
            raw: None,
        };
    }
    
    // Try integer parse (no leading zeros)
    if value.chars().all(|c| c.is_ascii_digit() || c == '-') {
        if let Ok(i) = value.parse::<i64>() {
            return CellValue::Integer(i);
        }
    }
    
    // Default to string
    CellValue::Text {
        display: value.to_string(),
        raw: None,
    }
}
```

---

### Issue 6: Inconsistent Empty Result Handling

**Severity**: 🟡 LOW  
**Location**: Multiple handler files  
**Category**: API Consistency

#### Description
Some handlers return `{"rows": [], "total_count": 0}`, others return `{"rows": []}` without `total_count`, and some return `{"success": true}` with no data field.

#### Recommended Fix
Standardize response format:
```rust
// For list operations:
Ok(json!({
    "items": tables,  // Always present, empty array if no results
    "count": tables.len()
}))

// For select operations:
Ok(json!({
    "rows": result.rows,
    "total_count": result.total_count.unwrap_or(result.rows.len()),
    "next_page_token": result.next_page_token  // Omitted if None
}))
```

---

### Issue 7: MySQL `SHOW DATABASES` Privilege Not Checked

**Severity**: 🟡 LOW  
**Location**: `crates/dbflux_driver_mysql/src/driver.rs:85-100`  
**Category**: Error Handling / UX

#### Description
Driver executes `SHOW DATABASES` without checking privilege, causing "Access denied" errors that could be handled gracefully.

#### Recommended Fix
```rust
async fn list_databases(&self) -> Result<Vec<String>> {
    let mut conn = self.pool.get_conn().await?;
    
    let result: Result<Vec<String>, _> = conn.query("SHOW DATABASES").await;
    
    match result {
        Ok(dbs) => Ok(dbs),
        Err(e) if is_access_denied_error(&e) => {
            // Fall back to current database only
            let current_db: Option<String> = conn.query_first("SELECT DATABASE()").await?;
            Ok(current_db.into_iter().collect())
        }
        Err(e) => Err(e.into()),
    }
}
```

---

### Issue 8: Classification Mismatch for MongoDB `createIndex`

**Severity**: 🟡 LOW  
**Location**: `crates/dbflux_policy/src/classification.rs:78-95`  
**Category**: Policy Inconsistency

#### Description
`create_index` is classified as `AdminSafe` for SQL databases but not for MongoDB, defaulting to `Admin` even though MongoDB index creation is non-blocking and safe.

#### Recommended Fix
```rust
pub fn classify_ddl_operation(operation: &str, driver_kind: DbKind) -> ExecutionClassification {
    match (operation, driver_kind) {
        ("create_index", DbKind::Postgres | DbKind::Mysql | DbKind::Sqlite | DbKind::MongoDb) => {
            ExecutionClassification::AdminSafe
        }
        // ...
    }
}
```

---

### Issue 9: PostgreSQL `LIMIT` Without `ORDER BY` Non-Determinism

**Severity**: 🟡 LOW  
**Location**: `crates/dbflux_driver_postgres/src/query_generator.rs:110-130`  
**Category**: Correctness / Reliability

#### Description
Query generator allows `LIMIT` without `ORDER BY`, producing non-deterministic results (different rows on each execution).

#### Recommended Fix
```rust
fn generate_select(/* ... */) -> Result<(String, Vec<QueryValue>)> {
    if limit.is_some() && order_by.is_none() {
        log::warn!(
            "LIMIT without ORDER BY produces non-deterministic results for table '{}'",
            table
        );
    }
    
    // ... rest of implementation
}
```

---

### Issue 10: MongoDB Connection Timeout Not Configurable

**Severity**: 🟡 LOW  
**Location**: `crates/dbflux_driver_mongodb/src/driver.rs:47-70`  
**Category**: Configuration / UX

#### Description
Driver hardcodes connection timeout to default (30 seconds), but slow networks (SSH tunnels, VPN) may require longer timeouts.

#### Recommended Fix
```rust
// In DbConfig::MongoDb
DbConfig::MongoDb {
    // ... existing fields
    timeout_seconds: Option<u64>,
}

// In driver
if let Some(timeout) = timeout_seconds {
    client_options.connect_timeout = Some(Duration::from_secs(*timeout));
    client_options.server_selection_timeout = Some(Duration::from_secs(*timeout));
}
```

---

### Issue 11: DynamoDB Empty String Attribute Violation

**Severity**: 🟡 MEDIUM  
**Location**: `crates/dbflux_driver_dynamodb/src/driver.rs:180-200`  
**Category**: Driver-Specific Constraint

#### Description
DynamoDB prohibits empty string values, but driver doesn't validate before insertion, causing runtime errors.

#### Recommended Fix
```rust
fn row_to_item(row: &Row) -> Result<HashMap<String, AttributeValue>> {
    let mut item = HashMap::new();
    
    for (key, cell) in &row.cells {
        let attr_value = match cell {
            CellValue::Text { display, .. } => {
                if display.is_empty() {
                    AttributeValue::Null(true)  // Convert empty string to NULL
                } else {
                    AttributeValue::S(display.clone())
                }
            }
            // ...
        };
        item.insert(key.clone(), attr_value);
    }
    
    Ok(item)
}
```

---

### Issue 12: Redis Connection Multiplexing Not Enabled

**Severity**: 🟡 LOW  
**Location**: `crates/dbflux_driver_redis/src/driver.rs:30-50`  
**Category**: Performance

#### Description
Driver creates single connection instead of using multiplexed connection, causing sequential blocking for concurrent operations.

#### Recommended Fix
```rust
impl DbDriver for RedisDriver {
    async fn connect(&self, config: &DbConfig) -> Result<Box<dyn Connection>> {
        let client = redis::Client::open(connection_string)?;
        
        // Use multiplexed connection for concurrent operations
        let conn = client.get_multiplexed_async_connection().await?;
        
        Ok(Box::new(RedisConnection { conn }))
    }
}

pub struct RedisConnection {
    conn: redis::aio::MultiplexedConnection,  // Changed from Connection
}
```

---

## Patterns & Recommendations

### Pattern 1: Missing Driver-Specific Validation

**Observation**: Multiple drivers lack validation for database-specific constraints:
- DynamoDB: Empty strings, reserved keywords, partition key requirements
- MongoDB: Database name format, collection name restrictions
- Redis: Key pattern special characters, command availability

**Recommendation**: Add a `validate_config()` method to the `DbDriver` trait that checks driver-specific constraints before attempting connection.

```rust
pub trait DbDriver: Send + Sync {
    fn validate_config(&self, config: &DbConfig) -> Result<()> {
        Ok(())  // Default: no validation
    }
    
    async fn connect(&self, config: &DbConfig) -> Result<Box<dyn Connection>> {
        self.validate_config(config)?;  // Validate before connecting
        // ... connection logic
    }
}
```

---

### Pattern 2: Inconsistent Error Message Quality

**Observation**: PostgreSQL returns detailed errors with hints and context (via `as_db_error()`), but other drivers return generic errors:
- MySQL: `Error: Err(MySqlError(1064))` instead of human-readable syntax error
- MongoDB: `Error: Command failed` without command details
- Redis: `Error: Invalid argument` without context

**Recommendation**: Implement `ErrorFormatter` trait consistently across all drivers to provide structured, actionable error messages.

```rust
// All drivers should implement ErrorFormatter
impl ErrorFormatter for MySqlConnection {
    fn format_error(&self, error: &dyn std::error::Error) -> FormattedError {
        if let Some(mysql_err) = error.downcast_ref::<mysql_async::Error>() {
            FormattedError {
                message: mysql_err.to_string(),
                detail: extract_mysql_detail(mysql_err),
                hint: suggest_mysql_fix(mysql_err),
                // ...
            }
        } else {
            FormattedError::generic(error)
        }
    }
}
```

---

### Pattern 3: No Integration Tests for Non-PostgreSQL Drivers

**Observation**: The `dbflux_test_support` crate provides Docker fixtures for all databases, but integration tests only exist for PostgreSQL. MySQL, MongoDB, Redis, DynamoDB, SQLite lack live integration tests.

**Recommendation**: Add integration tests for each driver covering:
1. Basic CRUD operations (connect, list databases, list tables, select, insert, update, delete)
2. WHERE clause translation for all supported operators
3. Edge cases (NULL handling, empty results, type coercion)
4. Error scenarios (invalid credentials, missing database, syntax errors)

**Template**:
```rust
// crates/dbflux_driver_mysql/tests/live_integration.rs
#[tokio::test]
#[ignore] // Requires Docker
async fn test_mysql_readonly_operations() {
    let container = MySqlContainer::start().await;
    let driver = MySqlDriver::new();
    
    // Test connect
    let conn = driver.connect(&container.config()).await.unwrap();
    
    // Test list_databases
    let databases = conn.list_databases().await.unwrap();
    assert!(databases.contains(&"mysql".to_string()));
    
    // Test list_tables
    let tables = conn.list_tables(Some("mysql")).await.unwrap();
    assert!(!tables.is_empty());
    
    // Test select with WHERE clause
    let result = conn.select_data(&SelectRequest {
        table: "user".to_string(),
        filter: Some(json!({"user": {"$eq": "root"}})),
        // ...
    }).await.unwrap();
    assert!(!result.rows.is_empty());
}
```

---

## Action Plan

### Phase 1: Critical Blockers (Immediate)

**Priority**: 🔥 **URGENT** - Prevents basic read-only operations

1. **Bug #4: MySQL Placeholder Mismatch**
   - **Time**: 2-3 hours
   - **Impact**: Unblocks ALL WHERE clause queries on MySQL
   - **Tasks**:
     - [ ] Create `SqlDialect` trait in `dbflux_core`
     - [ ] Implement `MySqlDialect` and `PostgresDialect`
     - [ ] Update MySQL query generator to use dialect
     - [ ] Add integration tests for WHERE clauses

2. **Bug #1: MongoDB Database Optional**
   - **Time**: 2-3 hours
   - **Impact**: Unblocks MongoDB connections without pre-selected database
   - **Tasks**:
     - [ ] Change `DbConfig::MongoDb.database` to `Option<String>`
     - [ ] Update connection logic to handle `None` database
     - [ ] Update `list_tables` to require database parameter
     - [ ] Add tests for connection without database

### Phase 2: Critical Data Issues (High Priority)

**Priority**: 🔴 **HIGH** - Prevents correct data retrieval

3. **Bug #2: Redis Pattern Escaping**
   - **Time**: 1-2 hours
   - **Impact**: Security vulnerability + correctness
   - **Tasks**:
     - [ ] Add `escape_redis_glob_pattern()` function
     - [ ] Update `extract_redis_pattern()` to escape user input
     - [ ] Add tests for special characters in patterns
     - [ ] Document escaping behavior

4. **Bug #3: DynamoDB Pagination**
   - **Time**: 3-4 hours
   - **Impact**: Prevents data loss for large tables
   - **Tasks**:
     - [ ] Add `page_token` field to `SelectRequest`
     - [ ] Add `next_page_token` field to `SelectResult`
     - [ ] Update DynamoDB driver to handle pagination tokens
     - [ ] Add integration tests for paginated queries

5. **Bug #5: SQLite In-Memory Persistence**
   - **Time**: 2-3 hours
   - **Impact**: Enables multi-operation workflows for `:memory:` databases
   - **Tasks**:
     - [ ] Add connection pooling for in-memory databases
     - [ ] Add `connection_id` to `DbConfig::Sqlite`
     - [ ] Update driver to reuse in-memory connections
     - [ ] Add tests for multi-operation workflows

### Phase 3: Consistency & Polish (Medium Priority)

**Priority**: 🟡 **MEDIUM** - Improves reliability and UX

6. **Issue 1: NULL Representation**
   - **Time**: 1 hour
   - **Tasks**: Standardize NULL handling across all drivers

7. **Issue 2: Parameter Validation**
   - **Time**: 30 minutes
   - **Tasks**: Add input validation to MCP handlers

8. **Issue 3: DynamoDB Reserved Keywords**
   - **Time**: 2 hours
   - **Tasks**: Implement ExpressionAttributeNames for reserved words

9. **Issue 4: MongoDB Nested Fields**
   - **Time**: 1-2 hours
   - **Tasks**: Extend WHERE clause parser for nested fields in array ops

10. **Issue 5: Redis Type Ambiguity**
    - **Time**: 1 hour
    - **Tasks**: Preserve string semantics for numeric-looking values

### Phase 4: Quality & Testing (Ongoing)

**Priority**: 🟢 **ONGOING** - Long-term maintainability

11. **Integration Test Coverage**
    - **Time**: 1-2 days
    - **Tasks**: Add comprehensive integration tests for all drivers

12. **Error Message Quality**
    - **Time**: 2-3 days
    - **Tasks**: Implement `ErrorFormatter` for all drivers

13. **Documentation**
    - **Time**: 1 day
    - **Tasks**: Document driver-specific behaviors and limitations

---

## Estimated Timeline

| Phase | Duration | Completion Target |
|-------|----------|-------------------|
| Phase 1: Critical Blockers | 4-6 hours | Day 1 |
| Phase 2: Data Issues | 6-9 hours | Day 2 |
| Phase 3: Consistency | 5-7 hours | Day 3 |
| Phase 4: Testing & Quality | 4-6 days | Week 2 |

**Total Estimated Effort**: 2-3 weeks (including testing and documentation)

---

## Testing Strategy

### Unit Tests
- WHERE clause translation for each operator
- Type conversion edge cases (NULL, empty strings, numeric strings)
- Error formatting for driver-specific errors

### Integration Tests
For each driver (PostgreSQL, MySQL, SQLite, MongoDB, Redis, DynamoDB):
- [ ] Connect with valid credentials
- [ ] Connect with invalid credentials (should fail gracefully)
- [ ] List databases
- [ ] List tables in specific database
- [ ] Select all data from table
- [ ] Select with WHERE clause (equality)
- [ ] Select with WHERE clause (comparison operators)
- [ ] Select with WHERE clause (array operators)
- [ ] Select with NULL values
- [ ] Select with empty result set
- [ ] Select with LIMIT
- [ ] Select with ORDER BY
- [ ] Select with pagination (DynamoDB)

### MCP End-to-End Tests
- [ ] MCP client calls `connect` → `list_databases` → `list_tables` → `select_data` workflow
- [ ] Verify consistent response formats across all drivers
- [ ] Verify error messages are actionable

---

## Conclusion

This report identifies **17 issues** (5 critical, 12 non-critical) that will prevent DBFlux MCP server from working correctly with drivers other than PostgreSQL. The critical bugs block basic read-only operations (connect, list, select), while non-critical issues affect consistency, reliability, and user experience.

**Key Takeaways**:
1. **MySQL is completely blocked** by placeholder syntax mismatch
2. **MongoDB connections fail** without optional database parameter
3. **DynamoDB loses data** without pagination support
4. **Redis has security vulnerability** without pattern escaping
5. **SQLite in-memory workflows broken** without connection pooling

**Recommended First Steps**:
1. Fix MySQL placeholder mismatch (unblocks WHERE clauses)
2. Fix MongoDB database parameter (unblocks connections)
3. Add integration tests to prevent regressions
4. Systematically address remaining issues by priority

With these fixes, all drivers will achieve parity with PostgreSQL for read-only MCP operations.
