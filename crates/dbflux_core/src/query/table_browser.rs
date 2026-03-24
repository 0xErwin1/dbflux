use serde::{Deserialize, Serialize};

use crate::{DbKind, DefaultSqlDialect, SqlDialect};

/// Sort direction for ORDER BY clauses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum SortDirection {
    #[default]
    Ascending,
    Descending,
}

/// Column with sort direction for ORDER BY clauses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderByColumn {
    pub column: ColumnRef,
    pub direction: SortDirection,
}

impl OrderByColumn {
    /// Create an OrderByColumn from a simple column name (for backward compatibility).
    pub fn from_name(name: &str, direction: SortDirection) -> Self {
        Self {
            column: ColumnRef::from_qualified(name),
            direction,
        }
    }

    /// Create an ascending sort column from a simple name.
    pub fn asc(column: impl Into<String>) -> Self {
        Self {
            column: ColumnRef::new(column),
            direction: SortDirection::Ascending,
        }
    }

    /// Create a descending sort column from a simple name.
    pub fn desc(column: impl Into<String>) -> Self {
        Self {
            column: ColumnRef::new(column),
            direction: SortDirection::Descending,
        }
    }

    /// Create an ascending sort column with table qualification.
    pub fn asc_qualified(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            column: ColumnRef::with_table(table, column),
            direction: SortDirection::Ascending,
        }
    }

    /// Create a descending sort column with table qualification.
    pub fn desc_qualified(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            column: ColumnRef::with_table(table, column),
            direction: SortDirection::Descending,
        }
    }
}

/// Escape an identifier by doubling the quote character.
fn escape_identifier(name: &str, quote_char: char) -> String {
    let quote_str = quote_char.to_string();
    let escaped = format!("{}{}", quote_char, quote_char);
    name.replace(&quote_str, &escaped)
}

/// Pagination strategy for table browsing.
///
/// Currently only supports OFFSET-based pagination.
/// Keyset pagination can be added later for better performance on large tables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Pagination {
    Offset { limit: u32, offset: u64 },
}

impl Default for Pagination {
    fn default() -> Self {
        Self::Offset {
            limit: 100,
            offset: 0,
        }
    }
}

impl Pagination {
    pub fn limit(&self) -> u32 {
        match self {
            Self::Offset { limit, .. } => *limit,
        }
    }

    pub fn offset(&self) -> u64 {
        match self {
            Self::Offset { offset, .. } => *offset,
        }
    }

    pub fn next_page(&self) -> Self {
        match self {
            Self::Offset { limit, offset } => Self::Offset {
                limit: *limit,
                offset: offset + *limit as u64,
            },
        }
    }

    pub fn prev_page(&self) -> Option<Self> {
        match self {
            Self::Offset { limit, offset } => {
                if *offset == 0 {
                    None
                } else {
                    Some(Self::Offset {
                        limit: *limit,
                        offset: offset.saturating_sub(*limit as u64),
                    })
                }
            }
        }
    }

    pub fn current_page(&self) -> u64 {
        match self {
            Self::Offset { limit, offset } => {
                if *limit == 0 {
                    1
                } else {
                    offset / *limit as u64 + 1
                }
            }
        }
    }

    pub fn is_first_page(&self) -> bool {
        match self {
            Self::Offset { offset, .. } => *offset == 0,
        }
    }

    pub fn with_limit(&self, new_limit: u32) -> Self {
        match self {
            Self::Offset { offset, .. } => Self::Offset {
                limit: new_limit,
                offset: *offset,
            },
        }
    }

    pub fn reset_offset(&self) -> Self {
        match self {
            Self::Offset { limit, .. } => Self::Offset {
                limit: *limit,
                offset: 0,
            },
        }
    }
}

/// Reference to a table (schema + name).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRef {
    pub schema: Option<String>,
    pub name: String,
}

impl TableRef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            schema: None,
            name: name.into(),
        }
    }
}

/// Reference to a collection (database + name) for document databases.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionRef {
    pub database: String,
    pub name: String,
}

impl CollectionRef {
    pub fn new(database: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            name: name.into(),
        }
    }

    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.database, self.name)
    }
}

impl TableRef {
    pub fn with_schema(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: Some(schema.into()),
            name: name.into(),
        }
    }

    pub fn from_qualified(qualified_name: &str) -> Self {
        if let Some((schema, table)) = qualified_name.split_once('.') {
            Self::with_schema(schema, table)
        } else {
            Self::new(qualified_name)
        }
    }

    pub fn qualified_name(&self) -> String {
        match &self.schema {
            Some(s) => format!("{}.{}", s, self.name),
            None => self.name.clone(),
        }
    }

    pub fn quoted(&self) -> String {
        match &self.schema {
            Some(s) => format!("\"{}\".\"{}\"", s, self.name),
            None => format!("\"{}\"", self.name),
        }
    }

    /// Quote using a `SqlDialect`, delegating to `dialect.qualified_table()`.
    pub fn quoted_with(&self, dialect: &dyn SqlDialect) -> String {
        dialect.qualified_table(self.schema.as_deref(), &self.name)
    }

    /// Quote identifier using the appropriate syntax for the database kind.
    /// - PostgreSQL/SQLite: double quotes ("schema"."table")
    /// - MySQL/MariaDB: backticks (`schema`.`table`)
    ///
    /// Escapes quote characters within identifiers by doubling them.
    pub fn quoted_for_kind(&self, kind: DbKind) -> String {
        let quote = match kind {
            DbKind::MySQL | DbKind::MariaDB => '`',
            _ => '"',
        };

        let escaped_name = escape_identifier(&self.name, quote);

        match &self.schema {
            Some(s) => {
                let escaped_schema = escape_identifier(s, quote);
                format!(
                    "{}{}{}.{}{}{}",
                    quote, escaped_schema, quote, quote, escaped_name, quote
                )
            }
            None => format!("{}{}{}", quote, escaped_name, quote),
        }
    }
}

/// Reference to a column with optional table qualification and alias.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub name: String,
    pub alias: Option<String>,
}

impl ColumnRef {
    /// Create a column reference with just a name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            table: None,
            name: name.into(),
            alias: None,
        }
    }

    /// Create a column reference with table and name.
    pub fn with_table(table: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            table: Some(table.into()),
            name: name.into(),
            alias: None,
        }
    }

    /// Add an alias to this column reference.
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Parse a qualified name like "table.column" or just "column".
    pub fn from_qualified(qualified_name: &str) -> Self {
        if let Some((table, column)) = qualified_name.split_once('.') {
            Self::with_table(table, column)
        } else {
            Self::new(qualified_name)
        }
    }

    /// Parse an expression with optional alias: "table.column AS alias" or "column alias".
    /// Handles both "AS alias" and just "alias" forms.
    pub fn from_qualified_with_alias(expr: &str) -> Self {
        let trimmed = expr.trim();

        // Try "AS" keyword first
        if let Some((col_part, alias_part)) = trimmed.split_once(" AS ") {
            return Self::from_qualified(col_part.trim()).with_alias(alias_part.trim());
        }

        // Try case-insensitive "as"
        if let Some((col_part, alias_part)) = trimmed.split_once(" as ") {
            return Self::from_qualified(col_part.trim()).with_alias(alias_part.trim());
        }

        // Try space-separated alias (only if there's exactly one space after the column name)
        if let Some(space_pos) = trimmed.rfind(' ') {
            let col_part = &trimmed[..space_pos];
            let alias_part = &trimmed[space_pos + 1..];

            // Only treat it as alias if alias_part doesn't contain special SQL keywords
            if !alias_part.is_empty() && !alias_part.contains(' ') {
                return Self::from_qualified(col_part).with_alias(alias_part);
            }
        }

        // No alias found
        Self::from_qualified(trimmed)
    }

    /// Create a wildcard column reference (*).
    pub fn wildcard() -> Self {
        Self::new("*")
    }

    /// Create a qualified wildcard (table.*).
    pub fn wildcard_for_table(table: impl Into<String>) -> Self {
        Self::with_table(table, "*")
    }

    /// Get the qualified name (table.column or just column).
    pub fn qualified_name(&self) -> String {
        match &self.table {
            Some(t) => format!("{}.{}", t, self.name),
            None => self.name.clone(),
        }
    }

    /// Quote using default SQL dialect (double quotes).
    pub fn quoted(&self) -> String {
        let dialect = DefaultSqlDialect;
        self.quoted_with(&dialect)
    }

    /// Quote using a specific SQL dialect.
    pub fn quoted_with(&self, dialect: &dyn SqlDialect) -> String {
        let quoted_col = dialect.quote_identifier(&self.name);

        let base = match &self.table {
            Some(t) => format!("{}.{}", dialect.quote_identifier(t), quoted_col),
            None => quoted_col,
        };

        match &self.alias {
            Some(a) => format!("{} AS {}", base, dialect.quote_identifier(a)),
            None => base,
        }
    }

    /// Quote identifier using the appropriate syntax for the database kind.
    /// - PostgreSQL/SQLite: double quotes ("table"."column")
    /// - MySQL/MariaDB: backticks (`table`.`column`)
    ///
    /// Escapes quote characters within identifiers by doubling them.
    pub fn quoted_for_kind(&self, kind: DbKind) -> String {
        let quote = match kind {
            DbKind::MySQL | DbKind::MariaDB => '`',
            _ => '"',
        };

        let escaped_name = escape_identifier(&self.name, quote);

        let base = match &self.table {
            Some(t) => {
                let escaped_table = escape_identifier(t, quote);
                format!(
                    "{}{}{}.{}{}{}",
                    quote, escaped_table, quote, quote, escaped_name, quote
                )
            }
            None => format!("{}{}{}", quote, escaped_name, quote),
        };

        match &self.alias {
            Some(a) => {
                let escaped_alias = escape_identifier(a, quote);
                format!("{} AS {}{}{}", base, quote, escaped_alias, quote)
            }
            None => base,
        }
    }

    /// Get the name that will appear in result sets.
    /// Returns alias if present, otherwise the column name.
    pub fn result_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }

    /// Check if this is a wildcard column (*).
    pub fn is_wildcard(&self) -> bool {
        self.name == "*"
    }
}

impl std::fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.qualified_name())?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {}", alias)?;
        }
        Ok(())
    }
}

impl From<&str> for ColumnRef {
    fn from(s: &str) -> Self {
        Self::from_qualified(s)
    }
}

/// State for table browsing with pagination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableBrowseRequest {
    pub table: TableRef,
    pub pagination: Pagination,
    pub order_by: Vec<OrderByColumn>,
    pub filter: Option<String>,
}

impl TableBrowseRequest {
    pub fn new(table: TableRef) -> Self {
        Self {
            table,
            pagination: Pagination::default(),
            order_by: Vec::new(),
            filter: None,
        }
    }

    pub fn with_pagination(mut self, pagination: Pagination) -> Self {
        self.pagination = pagination;
        self
    }

    pub fn with_order_by(mut self, columns: Vec<OrderByColumn>) -> Self {
        self.order_by = columns;
        self
    }

    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Build the SQL query using a `SqlDialect` for identifier quoting.
    pub fn build_sql_with(&self, dialect: &dyn SqlDialect) -> String {
        let mut sql = format!("SELECT * FROM {}", self.table.quoted_with(dialect));

        if let Some(ref filter) = self.filter {
            let trimmed = filter.trim();
            if !trimmed.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(trimmed);
            }
        }

        if !self.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            let quoted_cols: Vec<String> = self
                .order_by
                .iter()
                .map(|col| {
                    let dir = match col.direction {
                        SortDirection::Ascending => "ASC",
                        SortDirection::Descending => "DESC",
                    };
                    format!("{} {}", col.column.quoted_with(dialect), dir)
                })
                .collect();
            sql.push_str(&quoted_cols.join(", "));
        }

        sql.push_str(&format!(
            " LIMIT {} OFFSET {}",
            self.pagination.limit(),
            self.pagination.offset()
        ));

        sql
    }

    /// Build the SQL query for this browse request (PostgreSQL syntax).
    ///
    /// If no ORDER BY columns are specified, the query may return inconsistent
    /// results across pages. The caller should ensure proper ordering.
    pub fn build_sql(&self) -> String {
        self.build_sql_for_kind(DbKind::Postgres)
    }

    /// Build the SQL query for this browse request using the appropriate syntax
    /// for the given database kind.
    pub fn build_sql_for_kind(&self, kind: DbKind) -> String {
        let mut sql = format!("SELECT * FROM {}", self.table.quoted_for_kind(kind));

        if let Some(ref filter) = self.filter {
            let trimmed = filter.trim();
            if !trimmed.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(trimmed);
            }
        }

        if !self.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            let quoted_cols: Vec<String> = self
                .order_by
                .iter()
                .map(|col| {
                    let dir = match col.direction {
                        SortDirection::Ascending => "ASC",
                        SortDirection::Descending => "DESC",
                    };
                    format!("{} {}", col.column.quoted_for_kind(kind), dir)
                })
                .collect();
            sql.push_str(&quoted_cols.join(", "));
        }

        sql.push_str(&format!(
            " LIMIT {} OFFSET {}",
            self.pagination.limit(),
            self.pagination.offset()
        ));

        sql
    }
}

/// Request for browsing a document collection with pagination and optional filter.
///
/// This is the document-database equivalent of `TableBrowseRequest`.
/// Drivers translate this into their native query syntax internally.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionBrowseRequest {
    pub collection: CollectionRef,
    pub pagination: Pagination,
    pub filter: Option<serde_json::Value>,
}

impl CollectionBrowseRequest {
    pub fn new(collection: CollectionRef) -> Self {
        Self {
            collection,
            pagination: Pagination::default(),
            filter: None,
        }
    }

    pub fn with_pagination(mut self, pagination: Pagination) -> Self {
        self.pagination = pagination;
        self
    }

    pub fn with_filter(mut self, filter: serde_json::Value) -> Self {
        self.filter = Some(filter);
        self
    }
}

/// Request for counting rows in a table with an optional filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCountRequest {
    pub table: TableRef,
    pub filter: Option<String>,
}

impl TableCountRequest {
    pub fn new(table: TableRef) -> Self {
        Self {
            table,
            filter: None,
        }
    }

    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }
}

/// Request for counting documents in a collection with an optional filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionCountRequest {
    pub collection: CollectionRef,
    pub filter: Option<serde_json::Value>,
}

impl CollectionCountRequest {
    pub fn new(collection: CollectionRef) -> Self {
        Self {
            collection,
            filter: None,
        }
    }

    pub fn with_filter(mut self, filter: serde_json::Value) -> Self {
        self.filter = Some(filter);
        self
    }
}

/// Request for explaining a query execution plan.
///
/// Drivers translate this into their native EXPLAIN syntax:
/// - PostgreSQL: `EXPLAIN (FORMAT JSON, ANALYZE) ...`
/// - MySQL: `EXPLAIN FORMAT=JSON ...`
/// - SQLite: `EXPLAIN QUERY PLAN ...`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainRequest {
    pub table: TableRef,
    pub query: Option<String>,
}

impl ExplainRequest {
    pub fn new(table: TableRef) -> Self {
        Self { table, query: None }
    }

    pub fn with_query(mut self, query: impl Into<String>) -> Self {
        self.query = Some(query.into());
        self
    }
}

/// Request for describing a table's structure.
///
/// Drivers translate this into their native DESCRIBE syntax:
/// - PostgreSQL: `information_schema.columns` query
/// - MySQL: `DESCRIBE table`
/// - SQLite: `PRAGMA table_info(...)`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeRequest {
    pub table: TableRef,
}

impl DescribeRequest {
    pub fn new(table: TableRef) -> Self {
        Self { table }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DefaultSqlDialect;

    #[test]
    fn test_pagination_next_prev() {
        let p = Pagination::Offset {
            limit: 100,
            offset: 0,
        };
        assert!(p.is_first_page());
        assert_eq!(p.current_page(), 1);

        let p2 = p.next_page();
        assert_eq!(p2.offset(), 100);
        assert_eq!(p2.current_page(), 2);

        let p3 = p2.prev_page().unwrap();
        assert_eq!(p3.offset(), 0);

        assert!(p.prev_page().is_none());
    }

    #[test]
    fn test_table_ref() {
        let t = TableRef::from_qualified("public.users");
        assert_eq!(t.schema, Some("public".to_string()));
        assert_eq!(t.name, "users");
        assert_eq!(t.quoted(), "\"public\".\"users\"");

        let t2 = TableRef::new("simple");
        assert_eq!(t2.qualified_name(), "simple");
        assert_eq!(t2.quoted(), "\"simple\"");
    }

    #[test]
    fn test_build_sql() {
        let req = TableBrowseRequest::new(TableRef::from_qualified("public.users"))
            .with_pagination(Pagination::Offset {
                limit: 50,
                offset: 100,
            })
            .with_order_by(vec![OrderByColumn::asc("id")]);

        assert_eq!(
            req.build_sql(),
            "SELECT * FROM \"public\".\"users\" ORDER BY \"id\" ASC LIMIT 50 OFFSET 100"
        );
    }

    #[test]
    fn test_build_sql_with_filter() {
        let req = TableBrowseRequest::new(TableRef::new("orders"))
            .with_filter("status = 'active'")
            .with_order_by(vec![OrderByColumn::desc("created_at")]);

        assert_eq!(
            req.build_sql(),
            "SELECT * FROM \"orders\" WHERE status = 'active' ORDER BY \"created_at\" DESC LIMIT 100 OFFSET 0"
        );
    }

    #[test]
    fn test_identifier_escaping() {
        let req = TableBrowseRequest::new(TableRef::new("my\"table"))
            .with_order_by(vec![OrderByColumn::asc("col\"name")]);

        assert_eq!(
            req.build_sql(),
            "SELECT * FROM \"my\"\"table\" ORDER BY \"col\"\"name\" ASC LIMIT 100 OFFSET 0"
        );
    }

    #[test]
    fn test_mysql_identifier_escaping() {
        let req = TableBrowseRequest::new(TableRef::new("my`table"))
            .with_order_by(vec![OrderByColumn::asc("col`name")]);

        assert_eq!(
            req.build_sql_for_kind(DbKind::MySQL),
            "SELECT * FROM `my``table` ORDER BY `col``name` ASC LIMIT 100 OFFSET 0"
        );
    }

    #[test]
    fn test_build_sql_with_dialect() {
        let dialect = DefaultSqlDialect;
        let req = TableBrowseRequest::new(TableRef::from_qualified("public.users"))
            .with_pagination(Pagination::Offset {
                limit: 50,
                offset: 100,
            })
            .with_order_by(vec![OrderByColumn::asc("id")]);

        assert_eq!(
            req.build_sql_with(&dialect),
            "SELECT * FROM \"public\".\"users\" ORDER BY \"id\" ASC LIMIT 50 OFFSET 100"
        );
    }

    #[test]
    fn test_build_sql_with_dialect_filter() {
        let dialect = DefaultSqlDialect;
        let req = TableBrowseRequest::new(TableRef::new("orders"))
            .with_filter("status = 'active'")
            .with_order_by(vec![OrderByColumn::desc("created_at")]);

        assert_eq!(
            req.build_sql_with(&dialect),
            "SELECT * FROM \"orders\" WHERE status = 'active' ORDER BY \"created_at\" DESC LIMIT 100 OFFSET 0"
        );
    }

    #[test]
    fn test_quoted_with_dialect() {
        let dialect = DefaultSqlDialect;
        let t = TableRef::from_qualified("public.users");
        assert_eq!(t.quoted_with(&dialect), "\"public\".\"users\"");

        let t2 = TableRef::new("simple");
        assert_eq!(t2.quoted_with(&dialect), "\"simple\"");
    }
}

#[cfg(test)]
mod column_ref_tests {
    use super::*;

    // Construction tests
    #[test]
    fn test_new() {
        let col = ColumnRef::new("id");
        assert_eq!(col.name, "id");
        assert_eq!(col.table, None);
        assert_eq!(col.alias, None);
    }

    #[test]
    fn test_with_table() {
        let col = ColumnRef::with_table("users", "id");
        assert_eq!(col.name, "id");
        assert_eq!(col.table, Some("users".to_string()));
        assert_eq!(col.alias, None);
    }

    #[test]
    fn test_with_alias() {
        let col = ColumnRef::new("user_id").with_alias("id");
        assert_eq!(col.name, "user_id");
        assert_eq!(col.alias, Some("id".to_string()));
    }

    #[test]
    fn test_from_qualified() {
        let col = ColumnRef::from_qualified("users.id");
        assert_eq!(col.name, "id");
        assert_eq!(col.table, Some("users".to_string()));
        assert_eq!(col.alias, None);

        let col2 = ColumnRef::from_qualified("email");
        assert_eq!(col2.name, "email");
        assert_eq!(col2.table, None);
    }

    #[test]
    fn test_from_qualified_with_alias() {
        // Test with "AS" keyword
        let col = ColumnRef::from_qualified_with_alias("users.id AS user_id");
        assert_eq!(col.name, "id");
        assert_eq!(col.table, Some("users".to_string()));
        assert_eq!(col.alias, Some("user_id".to_string()));

        // Test with lowercase "as"
        let col2 = ColumnRef::from_qualified_with_alias("email as user_email");
        assert_eq!(col2.name, "email");
        assert_eq!(col2.table, None);
        assert_eq!(col2.alias, Some("user_email".to_string()));

        // Test with space-separated alias
        let col3 = ColumnRef::from_qualified_with_alias("status s");
        assert_eq!(col3.name, "status");
        assert_eq!(col3.alias, Some("s".to_string()));

        // Test with qualified name and space-separated alias
        let col4 = ColumnRef::from_qualified_with_alias("orders.total amount");
        assert_eq!(col4.name, "total");
        assert_eq!(col4.table, Some("orders".to_string()));
        assert_eq!(col4.alias, Some("amount".to_string()));

        // Test without alias
        let col5 = ColumnRef::from_qualified_with_alias("created_at");
        assert_eq!(col5.name, "created_at");
        assert_eq!(col5.alias, None);
    }

    // Wildcard tests
    #[test]
    fn test_wildcard() {
        let col = ColumnRef::wildcard();
        assert_eq!(col.name, "*");
        assert_eq!(col.table, None);
        assert!(col.is_wildcard());
    }

    #[test]
    fn test_wildcard_for_table() {
        let col = ColumnRef::wildcard_for_table("users");
        assert_eq!(col.name, "*");
        assert_eq!(col.table, Some("users".to_string()));
        assert!(col.is_wildcard());
    }

    // Formatting tests
    #[test]
    fn test_quoted() {
        let col = ColumnRef::new("id");
        assert_eq!(col.quoted(), "\"id\"");

        let col2 = ColumnRef::with_table("users", "email");
        assert_eq!(col2.quoted(), "\"users\".\"email\"");

        let col3 = ColumnRef::new("user_id").with_alias("id");
        assert_eq!(col3.quoted(), "\"user_id\" AS \"id\"");

        let col4 = ColumnRef::with_table("orders", "total").with_alias("amount");
        assert_eq!(col4.quoted(), "\"orders\".\"total\" AS \"amount\"");
    }

    #[test]
    fn test_quoted_with() {
        let dialect = DefaultSqlDialect;

        let col = ColumnRef::new("name");
        assert_eq!(col.quoted_with(&dialect), "\"name\"");

        let col2 = ColumnRef::with_table("users", "name");
        assert_eq!(col2.quoted_with(&dialect), "\"users\".\"name\"");

        let col3 = ColumnRef::new("first_name").with_alias("fname");
        assert_eq!(col3.quoted_with(&dialect), "\"first_name\" AS \"fname\"");
    }

    #[test]
    fn test_quoted_for_kind() {
        // PostgreSQL uses double quotes
        let col = ColumnRef::with_table("users", "id");
        assert_eq!(col.quoted_for_kind(DbKind::Postgres), "\"users\".\"id\"");

        // MySQL uses backticks
        assert_eq!(col.quoted_for_kind(DbKind::MySQL), "`users`.`id`");
        assert_eq!(col.quoted_for_kind(DbKind::MariaDB), "`users`.`id`");

        // SQLite uses double quotes
        assert_eq!(col.quoted_for_kind(DbKind::SQLite), "\"users\".\"id\"");

        // With alias
        let col2 = ColumnRef::new("email").with_alias("user_email");
        assert_eq!(
            col2.quoted_for_kind(DbKind::Postgres),
            "\"email\" AS \"user_email\""
        );
        assert_eq!(
            col2.quoted_for_kind(DbKind::MySQL),
            "`email` AS `user_email`"
        );
    }

    // Edge cases
    #[test]
    fn test_names_with_spaces() {
        let col = ColumnRef::new("first name");
        assert_eq!(col.quoted(), "\"first name\"");

        let col2 = ColumnRef::with_table("user info", "last name");
        assert_eq!(col2.quoted(), "\"user info\".\"last name\"");
    }

    #[test]
    fn test_result_name() {
        let col = ColumnRef::new("user_id");
        assert_eq!(col.result_name(), "user_id");

        let col2 = ColumnRef::new("user_id").with_alias("id");
        assert_eq!(col2.result_name(), "id");

        let col3 = ColumnRef::with_table("users", "email").with_alias("user_email");
        assert_eq!(col3.result_name(), "user_email");
    }

    #[test]
    fn test_display_trait() {
        let col = ColumnRef::new("id");
        assert_eq!(format!("{}", col), "id");

        let col2 = ColumnRef::with_table("users", "email");
        assert_eq!(format!("{}", col2), "users.email");

        let col3 = ColumnRef::new("user_id").with_alias("id");
        assert_eq!(format!("{}", col3), "user_id AS id");

        let col4 = ColumnRef::with_table("orders", "total").with_alias("amount");
        assert_eq!(format!("{}", col4), "orders.total AS amount");
    }

    #[test]
    fn test_from_str_trait() {
        let col: ColumnRef = "id".into();
        assert_eq!(col.name, "id");
        assert_eq!(col.table, None);

        let col2: ColumnRef = "users.email".into();
        assert_eq!(col2.name, "email");
        assert_eq!(col2.table, Some("users".to_string()));
    }

    #[test]
    fn test_qualified_name() {
        let col = ColumnRef::new("id");
        assert_eq!(col.qualified_name(), "id");

        let col2 = ColumnRef::with_table("users", "email");
        assert_eq!(col2.qualified_name(), "users.email");

        // qualified_name doesn't include alias
        let col3 = ColumnRef::new("user_id").with_alias("id");
        assert_eq!(col3.qualified_name(), "user_id");
    }
}

#[cfg(test)]
mod order_by_tests {
    use super::*;

    #[test]
    fn test_from_name() {
        let col = OrderByColumn::from_name("id", SortDirection::Ascending);
        assert_eq!(col.column.name, "id");
        assert_eq!(col.column.table, None);
        assert_eq!(col.direction, SortDirection::Ascending);

        // Test with qualified name
        let col2 = OrderByColumn::from_name("users.email", SortDirection::Descending);
        assert_eq!(col2.column.name, "email");
        assert_eq!(col2.column.table, Some("users".to_string()));
        assert_eq!(col2.direction, SortDirection::Descending);
    }

    #[test]
    fn test_asc_constructor() {
        let col = OrderByColumn::asc("created_at");
        assert_eq!(col.column.name, "created_at");
        assert_eq!(col.column.table, None);
        assert_eq!(col.direction, SortDirection::Ascending);
    }

    #[test]
    fn test_desc_constructor() {
        let col = OrderByColumn::desc("updated_at");
        assert_eq!(col.column.name, "updated_at");
        assert_eq!(col.column.table, None);
        assert_eq!(col.direction, SortDirection::Descending);
    }

    #[test]
    fn test_asc_qualified_constructor() {
        let col = OrderByColumn::asc_qualified("users", "email");
        assert_eq!(col.column.name, "email");
        assert_eq!(col.column.table, Some("users".to_string()));
        assert_eq!(col.direction, SortDirection::Ascending);
    }

    #[test]
    fn test_desc_qualified_constructor() {
        let col = OrderByColumn::desc_qualified("orders", "total");
        assert_eq!(col.column.name, "total");
        assert_eq!(col.column.table, Some("orders".to_string()));
        assert_eq!(col.direction, SortDirection::Descending);
    }

    #[test]
    fn test_order_by_with_column_ref_integration() {
        // Test that OrderByColumn works with ColumnRef features
        let col = OrderByColumn {
            column: ColumnRef::with_table("products", "price"),
            direction: SortDirection::Descending,
        };

        assert_eq!(col.column.qualified_name(), "products.price");
        assert_eq!(col.column.quoted(), "\"products\".\"price\"");
        assert_eq!(col.direction, SortDirection::Descending);
    }

    #[test]
    fn test_order_by_in_table_browse_request() {
        // Integration test: OrderByColumn with TableBrowseRequest
        let req = TableBrowseRequest::new(TableRef::new("products")).with_order_by(vec![
            OrderByColumn::desc("price"),
            OrderByColumn::asc("name"),
        ]);

        let sql = req.build_sql();
        assert!(sql.contains("ORDER BY \"price\" DESC, \"name\" ASC"));
    }

    #[test]
    fn test_order_by_qualified_in_sql() {
        // Test qualified columns in SQL generation
        let req = TableBrowseRequest::new(TableRef::new("orders"))
            .with_order_by(vec![OrderByColumn::asc_qualified("orders", "created_at")]);

        let sql = req.build_sql();
        assert!(sql.contains("ORDER BY \"orders\".\"created_at\" ASC"));
    }
}
