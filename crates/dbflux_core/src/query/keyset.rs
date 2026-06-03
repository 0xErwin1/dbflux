use crate::Value;
use crate::sql::dialect::{PlaceholderStyle, SqlDialect};

/// Builds a lexicographic keyset predicate for chunked DML pagination.
///
/// For a composite primary key `(a, b, c)` with last-seen values `(la, lb, lc)`,
/// the predicate expands to:
/// ```sql
/// (a > la) OR (a = la AND b > lb) OR (a = la AND b = lb AND c > lc)
/// ```
///
/// This is a pure function with no IO. `pk_cols` and `last_values` must have
/// the same length; panics in debug builds if they differ.
///
/// `params` is the mutable accumulator for bound values; `param_index` starts
/// at the current parameter counter and is incremented for each bound value
/// appended.
pub fn lower_keyset_predicate(
    pk_cols: &[&str],
    last_values: &[Value],
    dialect: &dyn SqlDialect,
    table_alias: &str,
    params: &mut Vec<Value>,
    param_index: &mut usize,
) -> String {
    debug_assert_eq!(
        pk_cols.len(),
        last_values.len(),
        "pk_cols and last_values must have equal length"
    );

    if pk_cols.is_empty() {
        return String::new();
    }

    let n = pk_cols.len();
    let mut clauses: Vec<String> = Vec::with_capacity(n);

    for prefix_len in 0..n {
        let mut parts: Vec<String> = Vec::with_capacity(prefix_len + 1);

        for eq_idx in 0..prefix_len {
            let col = qualified_col(dialect, table_alias, pk_cols[eq_idx]);
            let ph = placeholder(dialect, *param_index);
            params.push(last_values[eq_idx].clone());
            *param_index += 1;
            parts.push(format!("{col} = {ph}"));
        }

        let gt_col = qualified_col(dialect, table_alias, pk_cols[prefix_len]);
        let gt_ph = placeholder(dialect, *param_index);
        params.push(last_values[prefix_len].clone());
        *param_index += 1;
        parts.push(format!("{gt_col} > {gt_ph}"));

        if parts.len() == 1 {
            clauses.push(parts.remove(0));
        } else {
            clauses.push(format!("({})", parts.join(" AND ")));
        }
    }

    if clauses.len() == 1 {
        clauses.remove(0)
    } else {
        format!("({})", clauses.join(" OR "))
    }
}

fn qualified_col(dialect: &dyn SqlDialect, table_alias: &str, col: &str) -> String {
    format!(
        "{}.{}",
        dialect.quote_identifier(table_alias),
        dialect.quote_identifier(col)
    )
}

fn placeholder(dialect: &dyn SqlDialect, index: usize) -> String {
    match dialect.placeholder_style() {
        PlaceholderStyle::DollarNumber => format!("${}", index),
        PlaceholderStyle::AtSign => format!("@p{}", index),
        _ => "?".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DefaultSqlDialect, Value};

    static DIALECT: DefaultSqlDialect = DefaultSqlDialect;

    struct PgDialect;
    impl SqlDialect for PgDialect {
        fn quote_identifier(&self, name: &str) -> String {
            format!("\"{}\"", name)
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
            DIALECT.value_to_literal(value)
        }
        fn escape_string(&self, s: &str) -> String {
            s.replace('\'', "''")
        }
        fn placeholder_style(&self) -> PlaceholderStyle {
            PlaceholderStyle::DollarNumber
        }
    }

    struct MssqlDialect;
    impl SqlDialect for MssqlDialect {
        fn quote_identifier(&self, name: &str) -> String {
            format!("[{}]", name)
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
            DIALECT.value_to_literal(value)
        }
        fn escape_string(&self, s: &str) -> String {
            s.replace('\'', "''")
        }
        fn placeholder_style(&self) -> PlaceholderStyle {
            PlaceholderStyle::AtSign
        }
    }

    // T-09 / T-10 — lower_keyset_predicate (spec scenarios DR-10.9, F-6)

    #[test]
    fn single_pk_postgres_dollar_placeholder() {
        let mut params: Vec<Value> = Vec::new();
        let mut idx = 1usize;
        let pred = lower_keyset_predicate(
            &["id"],
            &[Value::Int(42)],
            &PgDialect,
            "orders",
            &mut params,
            &mut idx,
        );
        assert_eq!(pred, "\"orders\".\"id\" > $1");
        assert_eq!(params, vec![Value::Int(42)]);
        assert_eq!(idx, 2);
    }

    #[test]
    fn single_pk_mysql_question_mark() {
        let mut params: Vec<Value> = Vec::new();
        let mut idx = 1usize;
        let pred = lower_keyset_predicate(
            &["id"],
            &[Value::Int(10)],
            &DIALECT,
            "t",
            &mut params,
            &mut idx,
        );
        assert!(pred.contains("> ?"), "expected question mark, got: {pred}");
        assert_eq!(params, vec![Value::Int(10)]);
    }

    #[test]
    fn composite_pk_two_columns_postgres() {
        let mut params: Vec<Value> = Vec::new();
        let mut idx = 1usize;
        let pred = lower_keyset_predicate(
            &["year", "month"],
            &[Value::Int(2024), Value::Int(3)],
            &PgDialect,
            "t",
            &mut params,
            &mut idx,
        );
        assert!(
            pred.contains("OR"),
            "two-col predicate must have OR: {pred}"
        );
        assert!(pred.contains("\"year\" > $1"), "first branch: {pred}");
        assert!(pred.contains("\"year\" = $2"), "second branch eq: {pred}");
        assert!(pred.contains("\"month\" > $3"), "second branch gt: {pred}");
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn composite_pk_three_columns_postgres() {
        let mut params: Vec<Value> = Vec::new();
        let mut idx = 1usize;
        let pred = lower_keyset_predicate(
            &["a", "b", "c"],
            &[Value::Int(1), Value::Int(2), Value::Int(3)],
            &PgDialect,
            "t",
            &mut params,
            &mut idx,
        );
        assert!(
            pred.contains("OR"),
            "three-col must have multiple OR branches"
        );
        assert_eq!(
            params.len(),
            6,
            "3-col keyset expands to 1+2+3 = 6 bound values"
        );
    }

    #[test]
    fn mssql_at_sign_placeholder() {
        let mut params: Vec<Value> = Vec::new();
        let mut idx = 1usize;
        let pred = lower_keyset_predicate(
            &["id"],
            &[Value::Int(5)],
            &MssqlDialect,
            "t",
            &mut params,
            &mut idx,
        );
        assert!(pred.contains("@p1"), "MSSQL must use @p1: {pred}");
    }

    #[test]
    fn sqlite_question_mark_with_double_quote_identifiers() {
        let mut params: Vec<Value> = Vec::new();
        let mut idx = 1usize;
        let pred = lower_keyset_predicate(
            &["rowid"],
            &[Value::Int(100)],
            &DIALECT,
            "t",
            &mut params,
            &mut idx,
        );
        assert!(pred.contains("> ?"), "SQLite/default must use ?: {pred}");
    }

    #[test]
    fn param_index_advances_from_nonzero_start() {
        let mut params: Vec<Value> = Vec::new();
        let mut idx = 5usize;
        let pred = lower_keyset_predicate(
            &["id"],
            &[Value::Int(1)],
            &PgDialect,
            "t",
            &mut params,
            &mut idx,
        );
        assert!(pred.contains("$5"), "should start from idx=5: {pred}");
        assert_eq!(idx, 6);
    }
}
