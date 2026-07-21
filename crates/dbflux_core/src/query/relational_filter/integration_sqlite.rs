//! SQLite in-process integration tests for the relational filter pipeline.
//!
//! Each test spins up an in-memory SQLite database, seeds it with a small
//! schema and a few rows, runs `parse_and_resolve` + `count_query_from_spec`,
//! and executes the generated SQL via `rusqlite` to verify correctness.
//!
//! These tests are always included (`#[cfg(test)]` is handled by the caller
//! in `mod.rs`). No `#[ignore]` is needed — the SQLite DB is in-memory.

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::Value;
    use crate::query::relational_filter::{
        RelationalFilterError, count::count_query_from_spec, parse_and_resolve,
    };
    use crate::query::visual_query::SourceTable;
    use crate::schema::types::SchemaForeignKeyInfo;
    use crate::sql::dialect::DefaultSqlDialect;

    fn make_fk(
        from_table: &str,
        from_col: &str,
        to_table: &str,
        to_col: &str,
    ) -> SchemaForeignKeyInfo {
        SchemaForeignKeyInfo {
            name: format!("fk_{}_{}", from_table, from_col),
            table_name: from_table.to_string(),
            columns: vec![from_col.to_string()],
            referenced_schema: None,
            referenced_table: to_table.to_string(),
            referenced_columns: vec![to_col.to_string()],
            on_delete: None,
            on_update: None,
        }
    }

    fn make_source(table: &str) -> SourceTable {
        SourceTable {
            schema: None,
            table: table.to_string(),
            alias: table.to_string(),
        }
    }

    fn execute_query(
        conn: &Connection,
        sql: &str,
        params: &[Value],
    ) -> Vec<Vec<rusqlite::types::Value>> {
        let mapped: Vec<Box<dyn rusqlite::types::ToSql>> = params
            .iter()
            .map(|v| -> Box<dyn rusqlite::types::ToSql> {
                match v {
                    Value::Text(s) => Box::new(s.clone()),
                    Value::Int(i) => Box::new(*i),
                    Value::Float(f) => Box::new(*f),
                    Value::Bool(b) => Box::new(if *b { 1i64 } else { 0i64 }),
                    _ => Box::new(rusqlite::types::Null),
                }
            })
            .collect();

        let refs: Vec<&dyn rusqlite::types::ToSql> = mapped.iter().map(|b| b.as_ref()).collect();

        let mut stmt = conn.prepare(sql).expect("prepare SQL");
        let column_count = stmt.column_count();

        stmt.query_map(refs.as_slice(), |row| {
            let mut cols = Vec::with_capacity(column_count);
            for i in 0..column_count {
                cols.push(
                    row.get::<_, rusqlite::types::Value>(i)
                        .unwrap_or(rusqlite::types::Value::Null),
                );
            }
            Ok(cols)
        })
        .expect("query_map")
        .map(|r| r.expect("row"))
        .collect()
    }

    // S-01: single-hop relational filter returns correct rows
    #[test]
    fn sqlite_single_hop_returns_rows() {
        let conn = Connection::open_in_memory().unwrap();

        conn.execute_batch("
            CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);
            CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT, created_by_id INTEGER REFERENCES users(id));
            INSERT INTO users VALUES (1, 'alice@example.com');
            INSERT INTO users VALUES (2, 'bob@example.com');
            INSERT INTO posts VALUES (10, 'Hello', 1);
            INSERT INTO posts VALUES (11, 'World', 2);
            INSERT INTO posts VALUES (12, 'Third', 1);
        ").unwrap();

        let fks = [make_fk("posts", "created_by_id", "users", "id")];
        let lowering = parse_and_resolve(
            "created_by.email = 'alice@example.com'",
            make_source("posts"),
            &fks,
            &DefaultSqlDialect,
        )
        .expect("should resolve");

        assert_eq!(lowering.spec.joins.len(), 1);
        assert_eq!(lowering.diagnostics.join_count, 1);

        let select =
            crate::query::generator::build_select_query(&lowering.spec, &DefaultSqlDialect)
                .expect("build SQL");

        let rows = execute_query(&conn, &select.sql, &select.params);

        assert_eq!(rows.len(), 2, "alice has 2 posts: {}", select.sql);
    }

    // S-02: multi-hop relational filter (two hops)
    #[test]
    fn sqlite_multi_hop_returns_rows() {
        let conn = Connection::open_in_memory().unwrap();

        conn.execute_batch("
            CREATE TABLE organizations (id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, org_id INTEGER REFERENCES organizations(id));
            CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT, created_by_id INTEGER REFERENCES users(id));
            INSERT INTO organizations VALUES (1, 'Acme');
            INSERT INTO organizations VALUES (2, 'Other');
            INSERT INTO users VALUES (1, 'alice@example.com', 1);
            INSERT INTO users VALUES (2, 'bob@example.com', 2);
            INSERT INTO posts VALUES (10, 'Post A', 1);
            INSERT INTO posts VALUES (11, 'Post B', 2);
        ").unwrap();

        let fks = [
            make_fk("posts", "created_by_id", "users", "id"),
            make_fk("users", "org_id", "organizations", "id"),
        ];
        let lowering = parse_and_resolve(
            "created_by.organization.name = 'Acme'",
            make_source("posts"),
            &fks,
            &DefaultSqlDialect,
        )
        .expect("should resolve two hops");

        assert_eq!(lowering.spec.joins.len(), 2);

        let select =
            crate::query::generator::build_select_query(&lowering.spec, &DefaultSqlDialect)
                .expect("build SQL");

        let rows = execute_query(&conn, &select.sql, &select.params);

        assert_eq!(
            rows.len(),
            1,
            "only alice's post is from Acme: {}",
            select.sql
        );
    }

    // S-12: self-join produces distinct aliases
    #[test]
    fn sqlite_self_join_distinct_aliases() {
        let conn = Connection::open_in_memory().unwrap();

        conn.execute_batch("
            CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT, parent_id INTEGER REFERENCES categories(id));
            INSERT INTO categories VALUES (1, 'Root', NULL);
            INSERT INTO categories VALUES (2, 'Child', 1);
            INSERT INTO categories VALUES (3, 'Other Child', 1);
        ").unwrap();

        let fks = [make_fk("categories", "parent_id", "categories", "id")];
        let lowering = parse_and_resolve(
            "parent.name = 'Root'",
            make_source("categories"),
            &fks,
            &DefaultSqlDialect,
        )
        .expect("should resolve self-join");

        assert_eq!(lowering.spec.joins.len(), 1, "self-join needs one hop");

        let join = &lowering.spec.joins[0];
        assert_ne!(
            join.from_alias, join.to_alias,
            "self-join aliases must differ: from={} to={}",
            join.from_alias, join.to_alias
        );

        let select =
            crate::query::generator::build_select_query(&lowering.spec, &DefaultSqlDialect)
                .expect("build SQL");

        let rows = execute_query(&conn, &select.sql, &select.params);

        assert_eq!(
            rows.len(),
            2,
            "two child categories of Root: {}",
            select.sql
        );
    }

    // S-06: ambiguous FK returns AmbiguousPath error, not rows
    #[test]
    fn sqlite_ambiguous_fk_returns_error() {
        let fks = [
            make_fk("posts", "created_by_id", "users", "id"),
            make_fk("posts", "updated_by_id", "users", "id"),
        ];

        let result = parse_and_resolve(
            "user.email = 'x'",
            make_source("posts"),
            &fks,
            &DefaultSqlDialect,
        );

        assert!(
            matches!(result, Err(RelationalFilterError::Resolve(_))),
            "two FKs to same table should produce AmbiguousPath error"
        );
    }

    // S-13: count parity — count query and data query return consistent results
    #[test]
    fn sqlite_count_parity() {
        let conn = Connection::open_in_memory().unwrap();

        conn.execute_batch(
            "
            CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);
            CREATE TABLE posts (id INTEGER PRIMARY KEY, created_by_id INTEGER REFERENCES users(id));
            INSERT INTO users VALUES (1, 'alice@example.com');
            INSERT INTO users VALUES (2, 'bob@example.com');
            INSERT INTO posts VALUES (10, 1);
            INSERT INTO posts VALUES (11, 2);
            INSERT INTO posts VALUES (12, 1);
        ",
        )
        .unwrap();

        let fks = [make_fk("posts", "created_by_id", "users", "id")];
        let lowering = parse_and_resolve(
            "created_by.email = 'alice@example.com'",
            make_source("posts"),
            &fks,
            &DefaultSqlDialect,
        )
        .expect("should resolve");

        let select =
            crate::query::generator::build_select_query(&lowering.spec, &DefaultSqlDialect)
                .expect("build data SQL");
        let count = count_query_from_spec(&lowering.spec, &DefaultSqlDialect);

        let data_rows = execute_query(&conn, &select.sql, &select.params);
        let count_rows = execute_query(&conn, &count.sql, &count.params);

        let data_count = data_rows.len();
        let count_value = match count_rows.first().and_then(|r| r.first()) {
            Some(rusqlite::types::Value::Integer(n)) => *n as usize,
            other => panic!("unexpected count value: {:?}", other),
        };

        assert_eq!(
            data_count, count_value,
            "data query and count query must agree: data={} count={}",
            data_count, count_value
        );
        assert_eq!(data_count, 2, "alice has 2 posts");
    }
}
