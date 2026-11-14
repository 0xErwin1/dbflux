use super::*;

#[test]
fn sqlite_rebuild_accepts_distinct_foreign_key_relations_to_the_same_parent() -> Result<(), DbError>
{
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE parent (id INTEGER PRIMARY KEY); \
         CREATE TABLE child (
             a INTEGER,
             b INTEGER,
             payload TEXT,
             FOREIGN KEY (a) REFERENCES parent(id) ON DELETE CASCADE,
             FOREIGN KEY (b) REFERENCES parent(id) ON DELETE SET NULL
         )",
    ))?;
    let schema_before = connection.execute(&QueryRequest::new(
        "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'child'",
    ))?;

    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("child"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "payload".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        })?;

    assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
    assert_eq!(
        schema_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'child'",
            ))?
            .rows,
        "FK proof during prepare must remain read-only"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_rejects_catalog_sql_that_disagrees_with_cached_table_xinfo() -> Result<(), DbError>
{
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, retained TEXT, payload TEXT)",
    ))?;
    connection.execute(&QueryRequest::new("PRAGMA main.table_xinfo('t')"))?;
    connection.execute(&QueryRequest::new("PRAGMA writable_schema = ON"))?;
    connection.execute(&QueryRequest::new(
        "UPDATE main.sqlite_master \
         SET sql = 'CREATE TABLE t (id INTEGER PRIMARY KEY, retained INTEGER, payload TEXT)' \
         WHERE type = 'table' AND name = 't'",
    ))?;
    connection.execute(&QueryRequest::new("PRAGMA writable_schema = OFF"))?;
    let snapshot_before = connection.execute(&QueryRequest::new(
        "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
    ))?;

    let result = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("t"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "payload".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        });
    let error = match result {
        Ok(_) => panic!("rebuild must reject catalog and cached table_xinfo disagreement"),
        Err(error) => error,
    };

    assert!(
        error.to_string().contains("catalog proof"),
        "metadata disagreement must be actionable: {error}"
    );
    assert_eq!(
        snapshot_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
            ))?
            .rows,
        "prepare must not mutate the catalog snapshot"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_rejects_cached_autoindex_not_declared_by_catalog_sql() -> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT UNIQUE, note TEXT)",
    ))?;
    let autoindex_before = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
    assert!(
        !autoindex_before.rows.is_empty(),
        "fixture must expose its UNIQUE autoindex before catalog-only mutation"
    );
    connection.execute(&QueryRequest::new("PRAGMA writable_schema = ON"))?;
    connection.execute(&QueryRequest::new(
        "UPDATE main.sqlite_master \
         SET sql = 'CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT, note TEXT)' \
         WHERE type = 'table' AND name = 't'",
    ))?;
    connection.execute(&QueryRequest::new("PRAGMA writable_schema = OFF"))?;
    let catalog_after = connection.execute(&QueryRequest::new(
        "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
    ))?;
    assert!(
        catalog_after.rows[0][0]
            .to_string()
            .contains("payload TEXT, note TEXT"),
        "fixture must remove UNIQUE only from catalog SQL"
    );
    let autoindex_after = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
    assert_eq!(
        autoindex_before.rows, autoindex_after.rows,
        "fixture must retain cached engine index metadata without a schema-version bump"
    );

    let error = match connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("t"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "note".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        }) {
        Ok(_) => panic!("an undeclared cached autoindex must reject rebuild planning"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("index"),
        "the catalog proof must identify the unaccounted index: {error}"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_rejects_cached_explicit_index_metadata_that_disagrees_with_catalog_sql()
-> Result<(), DbError> {
    for (name, cached_sql, catalog_sql) in [
        (
            "unique",
            "CREATE UNIQUE INDEX idx ON t(a, b)",
            "CREATE INDEX idx ON t(a, b)",
        ),
        (
            "direction",
            "CREATE INDEX idx ON t(a ASC, b DESC)",
            "CREATE INDEX idx ON t(a DESC, b DESC)",
        ),
        (
            "collation",
            "CREATE INDEX idx ON t(a, b)",
            "CREATE INDEX idx ON t(a COLLATE BINARY, b)",
        ),
        (
            "name",
            "CREATE INDEX idx ON t(a, b)",
            "CREATE INDEX other_idx ON t(a, b)",
        ),
        (
            "target",
            "CREATE INDEX idx ON t(a, b)",
            "CREATE INDEX idx ON other(a, b)",
        ),
        (
            "key order",
            "CREATE INDEX idx ON t(a, b)",
            "CREATE INDEX idx ON t(b, a)",
        ),
    ] {
        let (connection, _, _db_path) = connect_sqlite()?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT COLLATE NOCASE, b TEXT, note TEXT); \
             CREATE TABLE other (a TEXT COLLATE NOCASE, b TEXT);",
        ))?;
        connection.execute(&QueryRequest::new(cached_sql))?;
        let index_before =
            connection.execute(&QueryRequest::new("PRAGMA main.index_xinfo('idx')"))?;
        assert!(
            !index_before.rows.is_empty(),
            "{name} fixture must warm the explicit index metadata"
        );
        connection.execute(&QueryRequest::new("PRAGMA writable_schema = ON"))?;
        connection.execute(&QueryRequest::new(format!(
            "UPDATE main.sqlite_master SET sql = '{}' WHERE type = 'index' AND name = 'idx'",
            catalog_sql
        )))?;
        connection.execute(&QueryRequest::new("PRAGMA writable_schema = OFF"))?;
        let catalog_after = connection.execute(&QueryRequest::new(
            "SELECT sql FROM main.sqlite_master WHERE type = 'index' AND name = 'idx'",
        ))?;
        assert_eq!(
            catalog_after.rows[0][0].to_string(),
            catalog_sql,
            "{name} fixture must alter only the catalog index SQL"
        );
        let index_after =
            connection.execute(&QueryRequest::new("PRAGMA main.index_xinfo('idx')"))?;
        assert_eq!(
            index_before.rows, index_after.rows,
            "{name} fixture must retain cached index metadata without a schema-version bump"
        );

        let error = match connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new("t"),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "note".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            }) {
            Ok(_) => panic!("catalog SQL must prove every explicit index detail"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("index"),
            "{name} disagreement must reject with index evidence: {error}"
        );
    }
    Ok(())
}

#[test]
fn sqlite_rebuild_accepts_table_integer_primary_key_desc_without_autoindex() -> Result<(), DbError>
{
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t(id INTEGER,note TEXT,PRIMARY KEY(id DESC)); \
         INSERT INTO t(id, note) VALUES (7, 'before')",
    ))?;
    let indexes = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
    assert!(
        indexes.rows.is_empty(),
        "table-level INTEGER PRIMARY KEY DESC must not create a physical autoindex"
    );
    let xinfo = connection.execute(&QueryRequest::new(
        "SELECT name, pk, hidden FROM pragma_table_xinfo('t') ORDER BY cid",
    ))?;
    assert_eq!(
        xinfo.rows[0],
        vec![Value::Text("id".to_string()), Value::Int(1), Value::Int(0)]
    );
    let rowid = connection.execute(&QueryRequest::new("SELECT rowid, id FROM main.t"))?;
    assert_eq!(rowid.rows, vec![vec![Value::Int(7), Value::Int(7)]]);
    let schema_before = connection.execute(&QueryRequest::new(
        "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
    ))?;
    let rows_before =
        connection.execute(&QueryRequest::new("SELECT rowid, id, note FROM main.t"))?;

    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("t"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "note".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        })?;

    assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
    assert_eq!(
        schema_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
            ))?
            .rows,
        "rebuild preparation must remain read-only"
    );
    assert_eq!(
        rows_before.rows,
        connection
            .execute(&QueryRequest::new("SELECT rowid, id, note FROM main.t"))?
            .rows,
        "rebuild preparation must preserve the rowid-alias fixture"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_triangulates_table_integer_primary_key_sort_orders_and_physical_keys()
-> Result<(), DbError> {
    for source in [
        "CREATE TABLE t(id INTEGER,note TEXT,PRIMARY KEY(id));",
        "CREATE TABLE t(id INTEGER,note TEXT,PRIMARY KEY(id ASC));",
        "CREATE TABLE t(id INTEGER,note TEXT,PRIMARY KEY(id DESC));",
    ] {
        let (connection, _, _db_path) = connect_sqlite()?;
        connection.execute(&QueryRequest::new(source))?;
        let indexes = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
        assert!(
            indexes.rows.is_empty(),
            "table-level INTEGER PRIMARY KEY sort order must retain rowid-alias semantics"
        );
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new("t"),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "note".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            })?;
        assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
    }
    for source in [
        "CREATE TABLE t(id INT,note TEXT,PRIMARY KEY(id DESC));",
        "CREATE TABLE t(id INTEGER,part TEXT,note TEXT,PRIMARY KEY(id DESC,part ASC));",
    ] {
        let (connection, _, _db_path) = connect_sqlite()?;
        connection.execute(&QueryRequest::new(source))?;
        let indexes = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
        assert_eq!(
            indexes.rows.len(),
            1,
            "non-INTEGER and composite primary keys must retain their physical autoindex"
        );
        assert_eq!(indexes.rows[0][2], Value::Int(1));
        assert_eq!(indexes.rows[0][3], Value::Text("pk".to_string()));
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new("t"),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "note".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            })?;
        assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
    }
    Ok(())
}

#[test]
fn sqlite_rebuild_preserves_inline_integer_primary_key_desc_physical_index_and_identity()
-> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t(id INTEGER PRIMARY KEY DESC,note TEXT); \
         INSERT INTO t(id, note) VALUES (7, 'before')",
    ))?;
    let indexes = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
    assert_eq!(
        indexes.rows.len(),
        1,
        "inline DESC fixture must create an autoindex"
    );
    assert_eq!(indexes.rows[0][2], Value::Int(1));
    assert_eq!(indexes.rows[0][3], Value::Text("pk".to_string()));
    let autoindex = indexes.rows[0][1].to_string();
    let keys = connection.execute(&QueryRequest::new(format!(
        "SELECT name, \"desc\" FROM pragma_index_xinfo('{autoindex}') \
         WHERE \"key\" = 1 ORDER BY seqno"
    )))?;
    assert_eq!(
        keys.rows,
        vec![vec![Value::Text("id".to_string()), Value::Int(1)]]
    );
    let rowid = connection.execute(&QueryRequest::new("SELECT rowid, id FROM main.t"))?;
    assert_eq!(rowid.rows, vec![vec![Value::Int(1), Value::Int(7)]]);

    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("t"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "note".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        })?;
    assert!(plan.preview().statements.iter().any(|statement| {
        statement.starts_with("INSERT INTO") && statement.contains("SELECT \"rowid\", \"id\"")
    }));
    Ok(())
}

#[test]
fn sqlite_rebuild_accepts_direction_coalesced_autoindexes() -> Result<(), DbError> {
    for (source, expected_origin) in [
        (
            "CREATE TABLE t(id INTEGER PRIMARY KEY,a TEXT,note TEXT,UNIQUE(a ASC),UNIQUE(a DESC));",
            "u",
        ),
        (
            "CREATE TABLE t(a TEXT,note TEXT,UNIQUE(a ASC),PRIMARY KEY(a DESC));",
            "pk",
        ),
    ] {
        let (connection, _, _db_path) = connect_sqlite()?;
        connection.execute(&QueryRequest::new(source))?;
        let schema_before = connection.execute(&QueryRequest::new(
            "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
        ))?;
        let indexes = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
        assert_eq!(
            indexes.rows.len(),
            1,
            "fixture must coalesce equivalent declarations into one physical autoindex"
        );
        let autoindex = &indexes.rows[0];
        assert_eq!(autoindex[2], Value::Int(1));
        assert_eq!(autoindex[3], Value::Text(expected_origin.to_string()));
        let keys = connection.execute(&QueryRequest::new(format!(
            "SELECT seqno, name, \"desc\", coll, \"key\" FROM pragma_index_xinfo('{}') \
             WHERE \"key\" = 1 ORDER BY seqno",
            autoindex[1]
        )))?;
        assert_eq!(
            keys.rows,
            vec![vec![
                Value::Int(0),
                Value::Text("a".to_string()),
                Value::Int(0),
                Value::Text("BINARY".to_string()),
                Value::Int(1),
            ]],
            "the first declaration must retain the physical autoindex key direction"
        );

        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new("t"),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "note".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            })?;

        assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
        assert_eq!(
            schema_before.rows,
            connection
                .execute(&QueryRequest::new(
                    "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
                ))?
                .rows,
            "rebuild preparation must remain read-only"
        );
    }
    Ok(())
}

#[test]
fn sqlite_rebuild_triangulates_autoindex_coalescing_order_and_integer_primary_keys()
-> Result<(), DbError> {
    for (source, expected_origin, expected_column, expected_descending) in [
        (
            "CREATE TABLE t(a TEXT,note TEXT,UNIQUE(a DESC),UNIQUE(a ASC));",
            "u",
            "a",
            1,
        ),
        (
            "CREATE TABLE t(a TEXT,note TEXT,PRIMARY KEY(a DESC),UNIQUE(a ASC));",
            "pk",
            "a",
            1,
        ),
        (
            "CREATE TABLE t(a TEXT,note TEXT,UNIQUE(a DESC),UNIQUE(a DESC));",
            "u",
            "a",
            1,
        ),
        (
            "CREATE TABLE t(id INTEGER PRIMARY KEY,a TEXT,note TEXT,UNIQUE(a));",
            "u",
            "a",
            0,
        ),
    ] {
        let (connection, _, _db_path) = connect_sqlite()?;
        connection.execute(&QueryRequest::new(source))?;
        let schema_before = connection.execute(&QueryRequest::new(
            "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
        ))?;
        let indexes = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
        assert_eq!(
            indexes.rows.len(),
            1,
            "fixture must expose exactly one coalesced or independent physical autoindex"
        );
        let autoindex = &indexes.rows[0];
        assert_eq!(autoindex[2], Value::Int(1));
        assert_eq!(autoindex[3], Value::Text(expected_origin.to_string()));
        let keys = connection.execute(&QueryRequest::new(format!(
            "SELECT seqno, name, \"desc\", coll, \"key\" FROM pragma_index_xinfo('{}') \
             WHERE \"key\" = 1 ORDER BY seqno",
            autoindex[1]
        )))?;
        assert_eq!(
            keys.rows,
            vec![vec![
                Value::Int(0),
                Value::Text(expected_column.to_string()),
                Value::Int(expected_descending),
                Value::Text("BINARY".to_string()),
                Value::Int(1),
            ]],
            "physical autoindex proof must retain the first declaration's complete key intent"
        );

        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new("t"),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "note".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            })?;
        assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
        assert_eq!(
            schema_before.rows,
            connection
                .execute(&QueryRequest::new(
                    "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
                ))?
                .rows,
            "rebuild preparation must remain read-only"
        );
    }
    Ok(())
}

#[test]
fn sqlite_rebuild_rejects_cached_autoindex_direction_that_disagrees_with_catalog_sql()
-> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t(id INTEGER PRIMARY KEY,a TEXT,note TEXT,UNIQUE(a ASC));",
    ))?;
    let indexes_before = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
    assert_eq!(
        indexes_before.rows.len(),
        1,
        "fixture must expose an autoindex"
    );
    let autoindex = indexes_before.rows[0][1].to_string();
    let keys_before = connection.execute(&QueryRequest::new(format!(
        "SELECT seqno, name, \"desc\", coll, \"key\" FROM pragma_index_xinfo('{autoindex}') \
         WHERE \"key\" = 1 ORDER BY seqno"
    )))?;
    assert_eq!(keys_before.rows[0][2], Value::Int(0));
    connection.execute(&QueryRequest::new("PRAGMA writable_schema = ON"))?;
    connection.execute(&QueryRequest::new(
        "UPDATE main.sqlite_master \
         SET sql = 'CREATE TABLE t(id INTEGER PRIMARY KEY,a TEXT,note TEXT,UNIQUE(a DESC))' \
         WHERE type = 'table' AND name = 't'",
    ))?;
    connection.execute(&QueryRequest::new("PRAGMA writable_schema = OFF"))?;
    let catalog_after = connection.execute(&QueryRequest::new(
        "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
    ))?;
    assert!(
        catalog_after.rows[0][0]
            .to_string()
            .contains("UNIQUE(a DESC)"),
        "fixture must change only catalog SQL to DESC"
    );
    let keys_after = connection.execute(&QueryRequest::new(format!(
        "SELECT seqno, name, \"desc\", coll, \"key\" FROM pragma_index_xinfo('{autoindex}') \
         WHERE \"key\" = 1 ORDER BY seqno"
    )))?;
    assert_eq!(
        keys_before.rows, keys_after.rows,
        "fixture must retain cached physical autoindex direction without a schema-version bump"
    );

    let result = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("t"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "note".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        });
    let error = match result {
        Ok(_) => panic!("a cached autoindex direction mismatch must reject rebuild planning"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("catalog proof"),
        "physical autoindex direction must remain part of the proof: {error}"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_proves_autoindex_coalescing_and_key_term_details() -> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE coalesced (
             id INTEGER PRIMARY KEY,
             key_value TEXT COLLATE NOCASE,
             note TEXT,
             UNIQUE (key_value),
             UNIQUE (key_value COLLATE NOCASE ASC)
         );
         CREATE TABLE integer_primary (
             id INTEGER PRIMARY KEY,
             unique_value TEXT COLLATE RTRIM UNIQUE,
             note TEXT
         );
         CREATE TABLE composite_primary (
             a TEXT COLLATE NOCASE,
             b INTEGER,
             note TEXT,
             PRIMARY KEY (a COLLATE NOCASE DESC, b ASC)
         );
         CREATE TABLE explicit_index (
             id INTEGER PRIMARY KEY,
             payload TEXT COLLATE NOCASE,
             note TEXT
         );
         CREATE UNIQUE INDEX \"ix: payload\" ON explicit_index(payload COLLATE NOCASE DESC)",
    ))?;
    let coalesced_indexes =
        connection.execute(&QueryRequest::new("PRAGMA main.index_list('coalesced')"))?;
    assert_eq!(
        coalesced_indexes.rows.len(),
        1,
        "SQLite must coalesce equivalent UNIQUE declarations into one physical autoindex"
    );

    for table in [
        "coalesced",
        "integer_primary",
        "composite_primary",
        "explicit_index",
    ] {
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new(table),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "note".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            })?;
        assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
        if table == "explicit_index" {
            assert!(plan.preview().statements.iter().any(|statement| {
                statement.contains(
                    "CREATE UNIQUE INDEX \"ix: payload\" ON explicit_index(payload COLLATE NOCASE DESC)",
                )
            }));
        }
    }
    Ok(())
}

#[test]
fn sqlite_rebuild_preserves_proven_quoted_explicit_index_sql() -> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT, note TEXT); \
         CREATE UNIQUE INDEX \"ix: payload\" ON t(payload DESC)",
    ))?;

    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("t"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "note".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        })?;

    assert!(plan.preview().statements.iter().any(|statement| {
        statement.contains("CREATE UNIQUE INDEX \"ix: payload\" ON t(payload DESC)")
    }));
    Ok(())
}

#[test]
fn sqlite_rebuild_rejects_all_shadowed_rowid_aliases() -> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t (rowid TEXT, _rowid_ TEXT, oid TEXT, payload TEXT)",
    ))?;

    let result = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("t"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "payload".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        });
    let error = match result {
        Ok(_) => panic!("all rowid aliases must be rejected without an INTEGER PRIMARY KEY"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("requires an INTEGER PRIMARY KEY")
    );
    Ok(())
}
