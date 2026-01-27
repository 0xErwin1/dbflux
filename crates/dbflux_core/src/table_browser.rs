use crate::DbKind;

/// Sort direction for ORDER BY clauses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortDirection {
    #[default]
    Ascending,
    Descending,
}

/// Column with sort direction for ORDER BY clauses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByColumn {
    pub name: String,
    pub direction: SortDirection,
}

impl OrderByColumn {
    pub fn asc(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            direction: SortDirection::Ascending,
        }
    }

    pub fn desc(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// State for table browsing with pagination.
#[derive(Debug, Clone)]
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
        let quote = match kind {
            DbKind::MySQL | DbKind::MariaDB => '`',
            _ => '"',
        };

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
                    let escaped = escape_identifier(&col.name, quote);
                    let dir = match col.direction {
                        SortDirection::Ascending => "ASC",
                        SortDirection::Descending => "DESC",
                    };
                    format!("{}{}{} {}", quote, escaped, quote, dir)
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
