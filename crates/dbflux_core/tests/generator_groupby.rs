use dbflux_core::{
    AggFn, Comparator, DefaultSqlDialect, FilterNode, GroupByEntry, LiteralValue, PlaceholderStyle,
    Predicate, PredicateValue, Projection, SelectQuery, SortEntry, SourceTable, SqlDialect, Value,
    VisualAggregateSpec, VisualQuerySpec, VisualSortDirection,
};

// =============================================================================
// Inline dialect definitions (same pattern as in-crate generator tests)
// =============================================================================

#[derive(Debug)]
struct SqliteDialect;

impl SqlDialect for SqliteDialect {
    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
        match schema {
            Some(s) => format!(
                "{}.{}",
                self.quote_identifier(s),
                self.quote_identifier(table)
            ),
            None => self.quote_identifier(table),
        }
    }

    fn value_to_literal(&self, value: &Value) -> String {
        DefaultSqlDialect.value_to_literal(value)
    }

    fn escape_string(&self, s: &str) -> String {
        s.replace('\'', "''")
    }

    fn placeholder_style(&self) -> PlaceholderStyle {
        PlaceholderStyle::QuestionMark
    }
}

#[derive(Debug)]
struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
        match schema {
            Some(s) => format!(
                "{}.{}",
                self.quote_identifier(s),
                self.quote_identifier(table)
            ),
            None => self.quote_identifier(table),
        }
    }

    fn value_to_literal(&self, value: &Value) -> String {
        DefaultSqlDialect.value_to_literal(value)
    }

    fn escape_string(&self, s: &str) -> String {
        s.replace('\'', "''")
    }

    fn placeholder_style(&self) -> PlaceholderStyle {
        PlaceholderStyle::DollarNumber
    }
}

#[derive(Debug)]
struct MySqlDialect;

impl SqlDialect for MySqlDialect {
    fn quote_identifier(&self, name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
        match schema {
            Some(s) => format!(
                "{}.{}",
                self.quote_identifier(s),
                self.quote_identifier(table)
            ),
            None => self.quote_identifier(table),
        }
    }

    fn value_to_literal(&self, value: &Value) -> String {
        DefaultSqlDialect.value_to_literal(value)
    }

    fn escape_string(&self, s: &str) -> String {
        s.replace('\'', "''")
    }

    fn placeholder_style(&self) -> PlaceholderStyle {
        PlaceholderStyle::QuestionMark
    }
}

#[derive(Debug)]
struct MssqlDialect;

impl SqlDialect for MssqlDialect {
    fn quote_identifier(&self, name: &str) -> String {
        format!("[{}]", name.replace(']', "]]"))
    }

    fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
        match schema {
            Some(s) => format!(
                "{}.{}",
                self.quote_identifier(s),
                self.quote_identifier(table)
            ),
            None => self.quote_identifier(table),
        }
    }

    fn value_to_literal(&self, value: &Value) -> String {
        DefaultSqlDialect.value_to_literal(value)
    }

    fn escape_string(&self, s: &str) -> String {
        s.replace('\'', "''")
    }

    fn placeholder_style(&self) -> PlaceholderStyle {
        PlaceholderStyle::AtSign
    }
}

// =============================================================================
// Spec builders
// =============================================================================

fn orders_spec() -> VisualQuerySpec {
    VisualQuerySpec {
        source: SourceTable {
            schema: None,
            table: "orders".to_string(),
            alias: "o".to_string(),
        },
        projection: Projection::All,
        joins: vec![],
        filter: None,
        group_by: vec![],
        aggregates: vec![],
        having: None,
        sort: vec![],
        limit: None,
        offset: 0,
    }
}

fn run_with_dialect(spec: &VisualQuerySpec, dialect: &dyn SqlDialect) -> SelectQuery {
    dbflux_core::select_query_from_spec(spec, dialect).expect("generation must succeed")
}

fn grouped_sum_spec() -> VisualQuerySpec {
    let mut spec = orders_spec();
    spec.group_by = vec![GroupByEntry {
        source_alias: "o".to_string(),
        column: "country".to_string(),
    }];
    spec.aggregates = vec![VisualAggregateSpec {
        function: AggFn::Sum,
        source_alias: Some("o".to_string()),
        column: Some("amount".to_string()),
        alias: "total".to_string(),
    }];
    spec
}

// =============================================================================
// Basic GROUP BY + SUM — per dialect
// =============================================================================

#[test]
fn sqlite_basic_group_by_sum() {
    let spec = grouped_sum_spec();
    let q = run_with_dialect(&spec, &SqliteDialect);
    assert!(
        q.sql.contains("\"o\".\"country\""),
        "sqlite: group-by column: {}",
        q.sql
    );
    assert!(
        q.sql.contains("SUM(\"o\".\"amount\") AS \"total\""),
        "sqlite: SUM aggregate: {}",
        q.sql
    );
    assert!(
        q.sql.contains("GROUP BY \"o\".\"country\""),
        "sqlite: GROUP BY: {}",
        q.sql
    );
    assert!(
        !q.sql.contains("HAVING"),
        "sqlite: must not have HAVING without having spec: {}",
        q.sql
    );
    assert!(q.params.is_empty());
}

#[test]
fn postgres_basic_group_by_sum() {
    let spec = grouped_sum_spec();
    let q = run_with_dialect(&spec, &PostgresDialect);
    assert!(
        q.sql.contains("\"o\".\"country\""),
        "postgres: group-by column: {}",
        q.sql
    );
    assert!(
        q.sql.contains("SUM(\"o\".\"amount\") AS \"total\""),
        "postgres: SUM aggregate: {}",
        q.sql
    );
    assert!(
        q.sql.contains("GROUP BY \"o\".\"country\""),
        "postgres: GROUP BY: {}",
        q.sql
    );
}

#[test]
fn mysql_basic_group_by_sum() {
    let spec = grouped_sum_spec();
    let q = run_with_dialect(&spec, &MySqlDialect);
    assert!(
        q.sql.contains("`o`.`country`"),
        "mysql: group-by column: {}",
        q.sql
    );
    assert!(
        q.sql.contains("SUM(`o`.`amount`) AS `total`"),
        "mysql: SUM aggregate: {}",
        q.sql
    );
    assert!(
        q.sql.contains("GROUP BY `o`.`country`"),
        "mysql: GROUP BY: {}",
        q.sql
    );
}

#[test]
fn mssql_basic_group_by_sum() {
    let spec = grouped_sum_spec();
    let q = run_with_dialect(&spec, &MssqlDialect);
    assert!(
        q.sql.contains("[o].[country]"),
        "mssql: group-by column: {}",
        q.sql
    );
    assert!(
        q.sql.contains("SUM([o].[amount]) AS [total]"),
        "mssql: SUM aggregate: {}",
        q.sql
    );
    assert!(
        q.sql.contains("GROUP BY [o].[country]"),
        "mssql: GROUP BY: {}",
        q.sql
    );
}

// =============================================================================
// HAVING clause — per dialect
// =============================================================================

fn grouped_sum_with_having_spec() -> VisualQuerySpec {
    let mut spec = grouped_sum_spec();
    spec.having = Some(FilterNode::Predicate(Predicate {
        source_alias: "".to_string(),
        column: "total".to_string(),
        comparator: Comparator::Gt,
        value: PredicateValue::Single(LiteralValue::Integer(1000)),
        node_id: 0,
    }));
    spec
}

#[test]
fn sqlite_having_clause() {
    let spec = grouped_sum_with_having_spec();
    let q = run_with_dialect(&spec, &SqliteDialect);
    assert!(
        q.sql.contains("HAVING"),
        "sqlite: must contain HAVING: {}",
        q.sql
    );
    assert!(
        q.sql.contains("\"total\""),
        "sqlite: HAVING must reference total: {}",
        q.sql
    );
    assert_eq!(q.params.len(), 1, "must have 1 param for HAVING value");
    assert_eq!(q.params[0], Value::Int(1000));
}

#[test]
fn postgres_having_clause() {
    let spec = grouped_sum_with_having_spec();
    let q = run_with_dialect(&spec, &PostgresDialect);
    assert!(
        q.sql.contains("HAVING"),
        "postgres: must contain HAVING: {}",
        q.sql
    );
    assert!(
        q.sql.contains("\"total\" > $1"),
        "postgres: HAVING predicate with $1 placeholder: {}",
        q.sql
    );
}

#[test]
fn mysql_having_clause() {
    let spec = grouped_sum_with_having_spec();
    let q = run_with_dialect(&spec, &MySqlDialect);
    assert!(q.sql.contains("HAVING"), "mysql: HAVING: {}", q.sql);
    assert!(q.sql.contains("`total`"), "mysql: total col: {}", q.sql);
}

#[test]
fn mssql_having_clause() {
    let spec = grouped_sum_with_having_spec();
    let q = run_with_dialect(&spec, &MssqlDialect);
    assert!(q.sql.contains("HAVING"), "mssql: HAVING: {}", q.sql);
    assert!(q.sql.contains("[total]"), "mssql: total col: {}", q.sql);
}

// =============================================================================
// COUNT(*) — per dialect
// =============================================================================

fn count_star_spec() -> VisualQuerySpec {
    let mut spec = orders_spec();
    spec.group_by = vec![GroupByEntry {
        source_alias: "o".to_string(),
        column: "country".to_string(),
    }];
    spec.aggregates = vec![VisualAggregateSpec {
        function: AggFn::CountStar,
        source_alias: None,
        column: None,
        alias: "cnt".to_string(),
    }];
    spec
}

#[test]
fn sqlite_count_star() {
    let spec = count_star_spec();
    let q = run_with_dialect(&spec, &SqliteDialect);
    assert!(
        q.sql.contains("COUNT(*) AS \"cnt\""),
        "sqlite: COUNT(*): {}",
        q.sql
    );
}

#[test]
fn postgres_count_star() {
    let spec = count_star_spec();
    let q = run_with_dialect(&spec, &PostgresDialect);
    assert!(
        q.sql.contains("COUNT(*) AS \"cnt\""),
        "postgres: COUNT(*): {}",
        q.sql
    );
}

#[test]
fn mysql_count_star() {
    let spec = count_star_spec();
    let q = run_with_dialect(&spec, &MySqlDialect);
    assert!(
        q.sql.contains("COUNT(*) AS `cnt`"),
        "mysql: COUNT(*): {}",
        q.sql
    );
}

#[test]
fn mssql_count_star() {
    let spec = count_star_spec();
    let q = run_with_dialect(&spec, &MssqlDialect);
    assert!(
        q.sql.contains("COUNT(*) AS [cnt]"),
        "mssql: COUNT(*): {}",
        q.sql
    );
}

// =============================================================================
// COUNT(DISTINCT col) — per dialect
// =============================================================================

fn count_distinct_spec() -> VisualQuerySpec {
    let mut spec = orders_spec();
    spec.group_by = vec![GroupByEntry {
        source_alias: "o".to_string(),
        column: "country".to_string(),
    }];
    spec.aggregates = vec![VisualAggregateSpec {
        function: AggFn::CountDistinct,
        source_alias: Some("o".to_string()),
        column: Some("customer_id".to_string()),
        alias: "distinct_customers".to_string(),
    }];
    spec
}

#[test]
fn sqlite_count_distinct() {
    let spec = count_distinct_spec();
    let q = run_with_dialect(&spec, &SqliteDialect);
    assert!(
        q.sql
            .contains("COUNT(DISTINCT \"o\".\"customer_id\") AS \"distinct_customers\""),
        "sqlite: COUNT DISTINCT: {}",
        q.sql
    );
}

#[test]
fn postgres_count_distinct() {
    let spec = count_distinct_spec();
    let q = run_with_dialect(&spec, &PostgresDialect);
    assert!(
        q.sql
            .contains("COUNT(DISTINCT \"o\".\"customer_id\") AS \"distinct_customers\""),
        "postgres: COUNT DISTINCT: {}",
        q.sql
    );
}

#[test]
fn mysql_count_distinct() {
    let spec = count_distinct_spec();
    let q = run_with_dialect(&spec, &MySqlDialect);
    assert!(
        q.sql
            .contains("COUNT(DISTINCT `o`.`customer_id`) AS `distinct_customers`"),
        "mysql: COUNT DISTINCT: {}",
        q.sql
    );
}

#[test]
fn mssql_count_distinct() {
    let spec = count_distinct_spec();
    let q = run_with_dialect(&spec, &MssqlDialect);
    assert!(
        q.sql
            .contains("COUNT(DISTINCT [o].[customer_id]) AS [distinct_customers]"),
        "mssql: COUNT DISTINCT: {}",
        q.sql
    );
}

// =============================================================================
// Identifier quoting with reserved-word column name
// =============================================================================

fn reserved_word_group_by_spec() -> VisualQuerySpec {
    let mut spec = orders_spec();
    spec.group_by = vec![GroupByEntry {
        source_alias: "o".to_string(),
        column: "select".to_string(),
    }];
    spec.aggregates = vec![VisualAggregateSpec {
        function: AggFn::Count,
        source_alias: Some("o".to_string()),
        column: Some("id".to_string()),
        alias: "cnt".to_string(),
    }];
    spec
}

#[test]
fn sqlite_reserved_word_column_is_quoted() {
    let spec = reserved_word_group_by_spec();
    let q = run_with_dialect(&spec, &SqliteDialect);
    assert!(
        q.sql.contains("\"select\""),
        "sqlite: reserved word must be quoted: {}",
        q.sql
    );
}

#[test]
fn mysql_reserved_word_column_is_quoted() {
    let spec = reserved_word_group_by_spec();
    let q = run_with_dialect(&spec, &MySqlDialect);
    assert!(
        q.sql.contains("`select`"),
        "mysql: reserved word must be quoted: {}",
        q.sql
    );
}

#[test]
fn mssql_reserved_word_column_is_quoted() {
    let spec = reserved_word_group_by_spec();
    let q = run_with_dialect(&spec, &MssqlDialect);
    assert!(
        q.sql.contains("[select]"),
        "mssql: reserved word must be quoted: {}",
        q.sql
    );
}

// =============================================================================
// ORDER BY filtering — stale sort entry dropped
// =============================================================================

#[test]
fn sort_on_non_grouped_column_is_dropped_from_output() {
    let mut spec = grouped_sum_spec();
    spec.sort = vec![SortEntry {
        source_alias: "o".to_string(),
        column: "created_at".to_string(),
        direction: VisualSortDirection::Desc,
    }];

    let q = run_with_dialect(&spec, &SqliteDialect);
    assert!(
        !q.sql.contains("ORDER BY"),
        "stale sort on non-grouped column must be dropped: {}",
        q.sql
    );
}

#[test]
fn sort_on_aggregate_alias_is_kept() {
    let mut spec = grouped_sum_spec();
    spec.sort = vec![SortEntry {
        source_alias: "".to_string(),
        column: "total".to_string(),
        direction: VisualSortDirection::Desc,
    }];

    let q = run_with_dialect(&spec, &SqliteDialect);
    assert!(
        q.sql.contains("ORDER BY"),
        "sort on aggregate alias must be kept: {}",
        q.sql
    );
    assert!(
        q.sql.contains("\"total\""),
        "sort must reference alias: {}",
        q.sql
    );
}

#[test]
fn sort_on_group_by_column_is_kept() {
    let mut spec = grouped_sum_spec();
    spec.sort = vec![SortEntry {
        source_alias: "o".to_string(),
        column: "country".to_string(),
        direction: VisualSortDirection::Asc,
    }];

    let q = run_with_dialect(&spec, &SqliteDialect);
    assert!(
        q.sql.contains("ORDER BY"),
        "sort on group-by column must be kept: {}",
        q.sql
    );
}

// =============================================================================
// build_count_of_grouped
// =============================================================================

#[test]
fn count_of_grouped_wraps_inner_query_without_limit_offset() {
    let mut spec = grouped_sum_spec();
    spec.limit = Some(50);
    spec.offset = 10;

    let q = dbflux_core::build_count_of_grouped_query(&spec, &SqliteDialect).expect("must succeed");

    assert!(
        q.sql.starts_with("SELECT COUNT(*) FROM ("),
        "must start with COUNT subquery: {}",
        q.sql
    );
    assert!(
        q.sql.ends_with(") AS \"_dbflux_count_subq\""),
        "must end with quoted subquery alias: {}",
        q.sql
    );
    assert!(
        !q.sql.contains("LIMIT"),
        "inner query must not have LIMIT: {}",
        q.sql
    );
    assert!(
        !q.sql.contains("OFFSET"),
        "inner query must not have OFFSET: {}",
        q.sql
    );
}

#[test]
fn count_of_grouped_postgres_uses_dollar_placeholders() {
    let mut spec = grouped_sum_spec();
    spec.having = Some(FilterNode::Predicate(Predicate {
        source_alias: "".to_string(),
        column: "total".to_string(),
        comparator: Comparator::Gt,
        value: PredicateValue::Single(LiteralValue::Integer(500)),
        node_id: 0,
    }));

    let q =
        dbflux_core::build_count_of_grouped_query(&spec, &PostgresDialect).expect("must succeed");

    assert!(
        q.sql.contains("$1"),
        "postgres: must use $N placeholders: {}",
        q.sql
    );
    assert!(
        q.sql.starts_with("SELECT COUNT(*) FROM ("),
        "must wrap in subquery: {}",
        q.sql
    );
}

// =============================================================================
// Ungrouped regression — output must be identical to pre-change behavior
// =============================================================================

#[test]
fn ungrouped_query_unchanged_sqlite() {
    let spec = orders_spec();
    let q = run_with_dialect(&spec, &SqliteDialect);
    assert_eq!(
        q.sql, "SELECT *\nFROM \"orders\" AS \"o\"",
        "ungrouped spec must produce unchanged output"
    );
    assert!(q.params.is_empty());
}

#[test]
fn ungrouped_query_with_filter_unchanged_postgres() {
    let mut spec = orders_spec();
    spec.filter = Some(FilterNode::Predicate(Predicate {
        source_alias: "o".to_string(),
        column: "status".to_string(),
        comparator: Comparator::Eq,
        value: PredicateValue::Single(LiteralValue::Text("active".to_string())),
        node_id: 0,
    }));

    let q = run_with_dialect(&spec, &PostgresDialect);
    assert!(
        q.sql.contains("WHERE"),
        "ungrouped filter must produce WHERE: {}",
        q.sql
    );
    assert!(
        !q.sql.contains("GROUP BY"),
        "ungrouped must not have GROUP BY: {}",
        q.sql
    );
    assert!(
        !q.sql.contains("HAVING"),
        "ungrouped must not have HAVING: {}",
        q.sql
    );
}

#[test]
fn ungrouped_query_unchanged_mssql() {
    let mut spec = orders_spec();
    spec.limit = Some(100);
    let q = run_with_dialect(&spec, &MssqlDialect);
    assert!(
        q.sql.contains("SELECT *"),
        "mssql ungrouped: SELECT *: {}",
        q.sql
    );
    assert!(
        !q.sql.contains("GROUP BY"),
        "mssql ungrouped: no GROUP BY: {}",
        q.sql
    );
}

// =============================================================================
// SUGGESTION 2: MySQL/MSSQL count-of-grouped coverage
// =============================================================================

#[test]
fn count_of_grouped_mysql_uses_backtick_quoting() {
    let mut spec = grouped_sum_spec();
    spec.limit = Some(25);
    spec.offset = 5;

    let q = dbflux_core::build_count_of_grouped_query(&spec, &MySqlDialect).expect("must succeed");

    assert!(
        q.sql.starts_with("SELECT COUNT(*) FROM ("),
        "mysql: must start with COUNT subquery: {}",
        q.sql
    );
    assert!(
        q.sql.ends_with(") AS `_dbflux_count_subq`"),
        "mysql: must end with backtick-quoted subquery alias: {}",
        q.sql
    );
    assert!(
        !q.sql.contains("LIMIT"),
        "mysql: inner query must not have LIMIT: {}",
        q.sql
    );
    assert!(
        q.sql.contains("`country`"),
        "mysql: group-by column must be backtick-quoted: {}",
        q.sql
    );
}

#[test]
fn count_of_grouped_mssql_uses_bracket_quoting() {
    let mut spec = grouped_sum_spec();
    spec.limit = Some(25);
    spec.offset = 5;

    let q = dbflux_core::build_count_of_grouped_query(&spec, &MssqlDialect).expect("must succeed");

    assert!(
        q.sql.starts_with("SELECT COUNT(*) FROM ("),
        "mssql: must start with COUNT subquery: {}",
        q.sql
    );
    assert!(
        q.sql.ends_with(") AS [_dbflux_count_subq]"),
        "mssql: must end with bracket-quoted subquery alias: {}",
        q.sql
    );
    assert!(
        !q.sql.contains("TOP"),
        "mssql: inner query must not have TOP (pagination): {}",
        q.sql
    );
    assert!(
        q.sql.contains("[country]"),
        "mssql: group-by column must be bracket-quoted: {}",
        q.sql
    );
}
