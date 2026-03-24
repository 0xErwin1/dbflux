# WHERE Clause Syntax Guide

Complete reference for WHERE clause syntax in DBFlux MCP tools.

## Table of Contents

- [Overview](#overview)
- [Basic Syntax](#basic-syntax)
- [Comparison Operators](#comparison-operators)
- [Logical Operators](#logical-operators)
- [Pattern Matching](#pattern-matching)
- [NULL Handling](#null-handling)
- [Array and Collection Operations](#array-and-collection-operations)
- [Type Coercion](#type-coercion)
- [Advanced Examples](#advanced-examples)
- [Error Recovery](#error-recovery)
- [Driver-Specific Behavior](#driver-specific-behavior)
- [Limitations](#limitations)

## Overview

The WHERE clause syntax is a driver-agnostic JSON format for filtering records across relational, document, and key-value databases. It translates to:
- SQL WHERE clauses for PostgreSQL, MySQL, SQLite
- MongoDB query filters
- Redis SCAN patterns
- DynamoDB FilterExpression

This unified syntax allows AI agents to query any database without knowing driver-specific query languages.

## Basic Syntax

### Simple Equality

```json
{
  "column_name": "value"
}
```

Translates to:
- SQL: `WHERE column_name = 'value'`
- MongoDB: `{ column_name: "value" }`
- DynamoDB: `column_name = :val`

### Multiple Conditions (Implicit AND)

```json
{
  "status": "active",
  "role": "admin"
}
```

Translates to:
- SQL: `WHERE status = 'active' AND role = 'admin'`
- MongoDB: `{ status: "active", role: "admin" }`

## Comparison Operators

### Equality and Inequality

```json
{
  "age": { "$eq": 25 }
}
```

Translates to:
- SQL: `WHERE age = 25`
- MongoDB: `{ age: { $eq: 25 } }`

```json
{
  "age": { "$ne": 25 }
}
```

Translates to:
- SQL: `WHERE age != 25` (or `<>` depending on driver)
- MongoDB: `{ age: { $ne: 25 } }`

### Comparison

```json
{
  "age": { "$gt": 18 }
}
```

**Operators:**
- `$gt` — greater than (`>`)
- `$gte` — greater than or equal (`>=`)
- `$lt` — less than (`<`)
- `$lte` — less than or equal (`<=`)

**Example:**

```json
{
  "price": { "$gte": 100, "$lte": 500 }
}
```

Translates to:
- SQL: `WHERE price >= 100 AND price <= 500`
- MongoDB: `{ price: { $gte: 100, $lte: 500 } }`

### Range (IN / NOT IN)

```json
{
  "status": { "$in": ["pending", "approved"] }
}
```

Translates to:
- SQL: `WHERE status IN ('pending', 'approved')`
- MongoDB: `{ status: { $in: ["pending", "approved"] } }`

```json
{
  "status": { "$nin": ["rejected", "cancelled"] }
}
```

Translates to:
- SQL: `WHERE status NOT IN ('rejected', 'cancelled')`
- MongoDB: `{ status: { $nin: ["rejected", "cancelled"] } }`

## Logical Operators

### AND (Explicit)

```json
{
  "$and": [
    { "age": { "$gte": 18 } },
    { "status": "active" }
  ]
}
```

Translates to:
- SQL: `WHERE (age >= 18) AND (status = 'active')`
- MongoDB: `{ $and: [{ age: { $gte: 18 } }, { status: "active" }] }`

### OR

```json
{
  "$or": [
    { "role": "admin" },
    { "role": "moderator" }
  ]
}
```

Translates to:
- SQL: `WHERE (role = 'admin') OR (role = 'moderator')`
- MongoDB: `{ $or: [{ role: "admin" }, { role: "moderator" }] }`

### NOT

```json
{
  "$not": {
    "status": "deleted"
  }
}
```

Translates to:
- SQL: `WHERE NOT (status = 'deleted')`
- MongoDB: `{ $not: { status: "deleted" } }`

### Complex Nesting

```json
{
  "$and": [
    { "status": "active" },
    {
      "$or": [
        { "role": "admin" },
        { "permissions": { "$in": ["write", "delete"] } }
      ]
    }
  ]
}
```

Translates to:
- SQL: `WHERE (status = 'active') AND ((role = 'admin') OR (permissions IN ('write', 'delete')))`

## Pattern Matching

### LIKE (SQL) / Regex (MongoDB)

```json
{
  "email": { "$like": "%@example.com" }
}
```

Translates to:
- SQL: `WHERE email LIKE '%@example.com'`
- MongoDB: `{ email: /.*@example\.com$/ }`

**Wildcards:**
- `%` — matches any sequence of characters (SQL)
- `_` — matches a single character (SQL)
- MongoDB automatically converts `%` to `.*` and `_` to `.` in regex

**Case-insensitive:**

```json
{
  "name": { "$ilike": "john%" }
}
```

Translates to:
- PostgreSQL: `WHERE name ILIKE 'john%'`
- MySQL/SQLite: `WHERE LOWER(name) LIKE LOWER('john%')`
- MongoDB: `{ name: /^john.*/i }`

### Regular Expressions (MongoDB, PostgreSQL)

```json
{
  "email": { "$regex": "^[a-z]+@example\\.com$", "$options": "i" }
}
```

Translates to:
- PostgreSQL: `WHERE email ~* '^[a-z]+@example\.com$'`
- MongoDB: `{ email: { $regex: "^[a-z]+@example\\.com$", $options: "i" } }`

**Options:**
- `i` — case-insensitive
- `m` — multiline
- `s` — dotall (`.` matches newline)

**Note:** MySQL and SQLite do not support full regex syntax; use `$like` instead.

## NULL Handling

### IS NULL

```json
{
  "deleted_at": null
}
```

or explicitly:

```json
{
  "deleted_at": { "$eq": null }
}
```

Translates to:
- SQL: `WHERE deleted_at IS NULL`
- MongoDB: `{ deleted_at: null }`

### IS NOT NULL

```json
{
  "deleted_at": { "$ne": null }
}
```

Translates to:
- SQL: `WHERE deleted_at IS NOT NULL`
- MongoDB: `{ deleted_at: { $ne: null } }`

### EXISTS (MongoDB nested documents)

```json
{
  "metadata.last_login": { "$exists": true }
}
```

Translates to:
- MongoDB: `{ "metadata.last_login": { $exists: true } }`
- SQL (JSON column): `WHERE JSON_EXTRACT(metadata, '$.last_login') IS NOT NULL` (driver-specific)

## Array and Collection Operations

### Array Contains (ANY)

```json
{
  "tags": { "$contains": "featured" }
}
```

Translates to:
- PostgreSQL (array): `WHERE 'featured' = ANY(tags)`
- PostgreSQL (JSONB): `WHERE tags @> '["featured"]'`
- MongoDB: `{ tags: "featured" }`

### Array Overlap

```json
{
  "permissions": { "$overlap": ["read", "write"] }
}
```

Translates to:
- PostgreSQL (array): `WHERE permissions && ARRAY['read', 'write']`
- PostgreSQL (JSONB): `WHERE permissions ?| array['read', 'write']`
- MongoDB: `{ permissions: { $in: ["read", "write"] } }`

### Array Size

```json
{
  "items": { "$size": 3 }
}
```

Translates to:
- PostgreSQL: `WHERE array_length(items, 1) = 3`
- MongoDB: `{ items: { $size: 3 } }`

### All Elements Match

```json
{
  "scores": { "$all": [80, 90, 100] }
}
```

Translates to:
- MongoDB: `{ scores: { $all: [80, 90, 100] } }`
- PostgreSQL: `WHERE scores @> ARRAY[80, 90, 100]`

## Type Coercion

### Implicit Type Conversion

DBFlux automatically coerces JSON types to match column types:

```json
{
  "age": "25"
}
```

If `age` is an integer column, `"25"` is coerced to `25`.

**Coercion rules:**
- String → Number: `"123"` → `123`, `"3.14"` → `3.14`
- String → Boolean: `"true"` → `true`, `"false"` → `false`, `"1"` → `true`, `"0"` → `false`
- Number → String: `123` → `"123"`
- Boolean → Number: `true` → `1`, `false` → `0`

### Explicit Type Casting (SQL drivers)

```json
{
  "created_at": { "$cast": "timestamp", "$gte": "2024-01-01" }
}
```

Translates to:
- PostgreSQL: `WHERE created_at::timestamp >= '2024-01-01'`
- MySQL: `WHERE CAST(created_at AS DATETIME) >= '2024-01-01'`

## Advanced Examples

### Find users by email domain

```json
{
  "email": { "$like": "%@example.com" }
}
```

### Find records in date range

```json
{
  "$and": [
    { "created_at": { "$gte": "2024-01-01T00:00:00Z" } },
    { "created_at": { "$lt": "2024-02-01T00:00:00Z" } }
  ]
}
```

### Find active admins or moderators

```json
{
  "$and": [
    { "status": "active" },
    {
      "$or": [
        { "role": "admin" },
        { "role": "moderator" }
      ]
    }
  ]
}
```

### Find users with incomplete profiles

```json
{
  "$or": [
    { "email": null },
    { "phone": null },
    { "address": null }
  ]
}
```

### Find documents with nested field matching

```json
{
  "metadata.profile.age": { "$gte": 18 }
}
```

Translates to:
- MongoDB: `{ "metadata.profile.age": { $gte: 18 } }`
- PostgreSQL (JSONB): `WHERE (metadata->'profile'->>'age')::int >= 18`

### Find records with tag overlap

```json
{
  "tags": { "$overlap": ["urgent", "critical"] }
}
```

### Complex search with multiple criteria

```json
{
  "$and": [
    { "status": "published" },
    {
      "$or": [
        { "title": { "$ilike": "%urgent%" } },
        { "priority": { "$gte": 8 } }
      ]
    },
    { "author_id": { "$ne": null } },
    { "tags": { "$contains": "featured" } }
  ]
}
```

### Find records by JSON field value (PostgreSQL JSONB)

```json
{
  "config.notifications.email": true
}
```

Translates to:
- PostgreSQL: `WHERE (config->'notifications'->>'email')::boolean = true`

### Find records with case-insensitive partial match

```json
{
  "description": { "$ilike": "%database%" }
}
```

## Error Recovery

### Syntax Errors

If a WHERE clause has invalid syntax, DBFlux returns a structured error:

```json
{
  "error": "InvalidWhereClause",
  "message": "Unknown operator: $unknown",
  "location": "where.age.$unknown"
}
```

**Common syntax errors:**
- Unknown operator (e.g., `$unknown`)
- Invalid operator usage (e.g., `$in` with non-array value)
- Invalid nesting (e.g., `$and` with single condition)
- Type mismatch (e.g., `$gt` with string value on numeric column)

### Type Errors

If coercion fails, DBFlux returns:

```json
{
  "error": "TypeMismatch",
  "message": "Cannot coerce 'abc' to integer for column 'age'",
  "column": "age",
  "value": "abc"
}
```

### Column Not Found

If a column does not exist:

```json
{
  "error": "ColumnNotFound",
  "message": "Column 'unknown_column' does not exist in table 'users'",
  "column": "unknown_column"
}
```

**Best practices for AI agents:**
1. Use `describe_object` before querying to get column names and types
2. Quote column names if they may be case-sensitive or contain special characters
3. Validate operator usage (e.g., `$in` requires array, `$like` requires string)
4. Use explicit `$eq` for clarity when comparing with `null`

## Driver-Specific Behavior

### PostgreSQL

**Features:**
- Full regex support (`$regex` with `~`, `~*`, `!~`, `!~*`)
- JSONB operators (`@>`, `?`, `?|`, `?&`)
- Array operators (`&&`, `@>`, `<@`)
- Case-insensitive LIKE (`ILIKE`)
- Type casting (`::type`)

**Limitations:**
- Regex must be POSIX-compliant (not PCRE)
- JSONB path must be valid (`metadata->field->>subfield`)

### MySQL

**Features:**
- LIKE pattern matching (case-insensitive by default on `utf8_general_ci` collations)
- JSON_EXTRACT for JSON columns (`$.path`)
- REGEXP for basic regex (MySQL 8.0+)

**Limitations:**
- No ILIKE (emulated with `LOWER()`)
- Limited regex support (POSIX subset)
- No array types (use JSON arrays)

### SQLite

**Features:**
- LIKE pattern matching (case-insensitive for ASCII by default)
- JSON functions (`json_extract`, `json_array_length`)
- GLOB for glob patterns (case-sensitive)

**Limitations:**
- No regex support (use LIKE)
- No array types (use JSON arrays)
- Limited type system (dynamic typing)

### MongoDB

**Features:**
- Full query operators (`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$and`, `$or`, `$not`, `$exists`, `$size`, `$all`, `$regex`)
- Nested document queries (`field.subfield.subsubfield`)
- Array operators (`$elemMatch`, `$all`, `$size`)

**Limitations:**
- No SQL LIKE (use `$regex`)
- `$where` (JavaScript expressions) not supported for security

### Redis

**Features:**
- SCAN pattern matching (`*`, `?`, `[abc]`, `[^abc]`)
- Type filtering (STRING, LIST, SET, ZSET, HASH, STREAM)

**Limitations:**
- No WHERE clause support (Redis is key-value)
- Only pattern matching on keys

### DynamoDB

**Features:**
- FilterExpression with comparison operators (`=`, `<>`, `<`, `<=`, `>`, `>=`)
- Logical operators (`AND`, `OR`, `NOT`)
- Function operators (`attribute_exists`, `attribute_not_exists`, `begins_with`, `contains`)

**Limitations:**
- No LIKE or regex
- FilterExpression runs after query/scan (not indexed)
- Limited to simple conditions on non-key attributes

## Limitations

### General Limitations

1. **No subqueries**: WHERE clauses do not support subqueries or correlated queries.
   - Use multiple queries with `select_data` if needed.

2. **No joins in WHERE**: WHERE clauses apply to a single table.
   - Use `select_data` with `joins` parameter for cross-table filtering.

3. **No computed columns**: WHERE clauses cannot reference computed/virtual columns.
   - Filter results in application code after retrieval.

4. **No functions in WHERE**: WHERE clauses do not support database functions (e.g., `UPPER()`, `LENGTH()`).
   - Exception: Implicit coercion (e.g., `LOWER()` for case-insensitive LIKE on MySQL/SQLite).

5. **Limited JSON path depth**: Nested JSON paths are supported up to 10 levels.
   - Deeper paths may fail or be rejected depending on driver.

6. **No full-text search**: Use driver-specific tools for FTS (PostgreSQL `ts_query`, MongoDB `$text`).

### Performance Considerations

1. **Unindexed columns**: Filtering on unindexed columns requires full table scans.
   - Use `create_index` to add indexes before querying large tables.

2. **LIKE with leading wildcard**: `{ "email": { "$like": "%@example.com" } }` cannot use indexes.
   - Prefer suffix patterns (`email@%`) when possible.

3. **OR conditions**: Multiple `$or` branches may prevent index usage.
   - Consider separate queries with `UNION` if performance is critical.

4. **Type coercion overhead**: Implicit type coercion may degrade performance.
   - Use native column types in filters when possible.

5. **Large IN lists**: `$in` with thousands of values may be slow or hit query size limits.
   - Use temporary tables or batch queries instead.

### Security Considerations

1. **No SQL injection**: WHERE clauses are parameterized automatically.
   - Do not attempt to bypass parameterization with string concatenation.

2. **No arbitrary JavaScript**: `$where` (MongoDB) is disabled for security.
   - Use standard operators only.

3. **Column name validation**: Column names are validated against schema.
   - Invalid column names are rejected before query execution.

4. **Query timeout**: Long-running queries are subject to driver-specific timeouts.
   - Set `limit` to reduce query scope if needed.

## Best Practices for AI Agents

1. **Always use `describe_object` first**: Get column names, types, and indexes before querying.
2. **Prefer indexed columns**: Filter on primary keys and indexed columns for performance.
3. **Use explicit operators**: Prefer `{ "$eq": value }` over implicit `{ "column": value }` for clarity.
4. **Validate JSON structure**: Ensure WHERE clause is valid JSON before sending.
5. **Handle NULL explicitly**: Use `{ "$eq": null }` or `{ "$ne": null }` for NULL checks.
6. **Use `$and`/`$or` for complex logic**: Avoid implicit nesting; be explicit with logical operators.
7. **Test with `count_records`**: Validate WHERE clause with `count_records` before `select_data` on large tables.
8. **Use `limit` and `offset`**: Paginate large result sets to avoid memory issues.
9. **Quote special column names**: Use `"column-name"` for columns with hyphens or spaces.
10. **Check driver compatibility**: Review driver-specific behavior before using advanced operators.

## Summary

The WHERE clause syntax is a powerful, driver-agnostic filtering language for DBFlux MCP tools. By using JSON operators and logical composition, AI agents can query any database without knowing SQL, MongoDB query syntax, or DynamoDB filter expressions.

**Key takeaways:**
- Use comparison operators (`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`)
- Use logical operators (`$and`, `$or`, `$not`)
- Use pattern matching (`$like`, `$ilike`, `$regex`)
- Use NULL handling (`null`, `$eq: null`, `$ne: null`)
- Use array operators (`$contains`, `$overlap`, `$size`, `$all`)
- Always validate column names and types with `describe_object`
- Handle errors gracefully (syntax, type, column not found)
- Consider driver-specific behavior and limitations
- Follow best practices for performance and security

For more examples and driver-specific details, see:
- [DDL Safety Guide](./DDL_SAFETY.md)
- [MCP Server README](../README.md)
- [DBFlux Core Documentation](../../dbflux_core/README.md)
