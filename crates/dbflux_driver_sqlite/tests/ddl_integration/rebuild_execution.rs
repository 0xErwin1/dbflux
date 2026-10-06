use super::*;

#[test]
fn sqlite_rebuild_prepare_is_read_only_and_execute_applies_requested_delta() -> Result<(), DbError>
{
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "PRAGMA foreign_keys = ON; \
         CREATE TABLE people (id INTEGER PRIMARY KEY, legacy TEXT DEFAULT 'NULL', retained TEXT NOT NULL DEFAULT ('keep')); \
         INSERT INTO people (id, legacy, retained) VALUES (0, 'old', 'kept'), (-7, 'older', 'also kept')",
    ))?;
    let schema_before = connection.execute(&QueryRequest::new(
        "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'people'",
    ))?;
    let rows_before = connection.execute(&QueryRequest::new(
        "SELECT id, legacy, retained FROM main.people ORDER BY id",
    ))?;
    let settings_before = connection.execute(&QueryRequest::new("PRAGMA foreign_keys"))?;
    let before = rebuild_boundary_snapshot(&*connection, "people")?;
    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("people"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "legacy".to_string(),
                new_type: Some("VARCHAR(32)".to_string()),
                nullable: Some(false),
                default: Some(OwnedDefaultSpec::Set("NULL".to_string())),
            }],
            expected_before: vec![TableAlterExpectedColumn {
                name: "legacy".to_string(),
                type_name: Some("TEXT".to_string()),
                nullable: Some(true),
                default: Some(Some("'NULL'".to_string())),
            }],
        })?;
    assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
    assert!(plan.preview().driver_managed);
    assert!(plan.preview().table_atomic);
    assert!(
        plan.preview()
            .statements
            .iter()
            .any(|statement| { statement.contains("VARCHAR(32) NOT NULL DEFAULT NULL") })
    );
    assert_eq!(
        schema_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'people'",
            ))?
            .rows,
        "rebuild preparation must not mutate the source schema"
    );
    assert_eq!(
        rows_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT id, legacy, retained FROM main.people ORDER BY id",
            ))?
            .rows,
        "rebuild preparation must not mutate populated source data"
    );
    assert_eq!(
        settings_before.rows,
        connection
            .execute(&QueryRequest::new("PRAGMA foreign_keys"))?
            .rows,
        "rebuild preparation must not mutate connection settings"
    );
    assert_eq!(
        before,
        rebuild_boundary_snapshot(&*connection, "people")?,
        "preparation must retain the complete people boundary"
    );

    plan.execute()?;

    assert_eq!(
        connection
            .execute(&QueryRequest::new(
                "SELECT type, \"notnull\", dflt_value FROM pragma_table_xinfo('people') WHERE name = 'legacy'",
            ))?
            .rows,
        vec![vec![
            Value::Text("VARCHAR(32)".to_string()),
            Value::Int(1),
            Value::Text("NULL".to_string()),
        ]],
        "execution must apply only the requested legacy-column definition"
    );
    assert_eq!(
        rows_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT id, legacy, retained FROM main.people ORDER BY id",
            ))?
            .rows,
        "execution must preserve populated source rows exactly"
    );
    assert_eq!(
        settings_before.rows,
        connection
            .execute(&QueryRequest::new("PRAGMA foreign_keys"))?
            .rows,
        "execution must restore connection settings"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_mixes_payload_alter_and_independent_drop_without_mutation() -> Result<(), DbError>
{
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE people (id INTEGER PRIMARY KEY, legacy TEXT, obsolete TEXT, retained TEXT);
         INSERT INTO people VALUES (1, 'old', 'remove', 'keep')",
    ))?;
    let before = connection.execute(&QueryRequest::new("SELECT * FROM main.people"))?;
    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("people"),
            operations: vec![
                TableAlterOperation::AlterColumn {
                    name: "legacy".to_string(),
                    new_type: Some("VARCHAR(16)".to_string()),
                    nullable: Some(false),
                    default: None,
                },
                TableAlterOperation::DropColumn {
                    name: "obsolete".to_string(),
                },
            ],
            expected_before: Vec::new(),
        })?;
    let create = plan
        .preview()
        .statements
        .iter()
        .find(|statement| statement.starts_with("CREATE TABLE"))
        .expect("rebuild preview must include replacement CREATE TABLE");
    assert!(create.contains("legacy VARCHAR(16) NOT NULL"));
    assert!(!create.contains("obsolete"));
    assert!(create.contains("retained TEXT"));
    assert_eq!(
        before.rows,
        connection
            .execute(&QueryRequest::new("SELECT * FROM main.people"))?
            .rows
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_uses_hidden_rowid_for_composite_primary_keys_and_shadowed_aliases()
-> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE composite (a INTEGER, b TEXT, p TEXT, PRIMARY KEY(a, b));
         INSERT INTO composite VALUES (-3, 'negative', 'payload');
         INSERT INTO composite VALUES (0, 'zero', 'payload');
         CREATE TABLE shadowed (rowid TEXT, p TEXT);
         INSERT INTO shadowed VALUES ('shadow', 'payload')",
    ))?;

    for (table, expected_identity) in [("composite", "\"rowid\""), ("shadowed", "\"_rowid_\"")] {
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new(table),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "p".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            })?;
        assert!(
            plan.preview()
                .statements
                .iter()
                .any(|statement| statement.starts_with("INSERT INTO")
                    && statement.contains(expected_identity)),
            "rebuild must retain the proven hidden identity for {table}"
        );
    }
    Ok(())
}

#[test]
fn sqlite_rebuild_rejects_global_objects_without_mutation() -> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE people (id INTEGER PRIMARY KEY, legacy TEXT, retained TEXT);
         INSERT INTO people VALUES (1, 'before', 'retained');
         CREATE TABLE audit (id INTEGER PRIMARY KEY, message TEXT);
         INSERT INTO audit VALUES (1, 'view source');
         CREATE VIEW audit_view AS SELECT message FROM audit",
    ))?;
    let before = rebuild_boundary_snapshot(&*connection, "people")?;

    let error = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("people"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "legacy".to_string(),
                new_type: Some("TEXT".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        })
        .err()
        .expect("rebuild must reject global view and trigger uncertainty");
    assert!(
        error.to_string().contains("view or trigger"),
        "global-object rejection must be actionable: {error}"
    );
    assert_eq!(
        before,
        rebuild_boundary_snapshot(&*connection, "people")?,
        "rejection must retain catalog, target schema and rows, and connection settings together"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_accepts_implicit_parent_primary_key_foreign_keys() -> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE parent (id INTEGER PRIMARY KEY); \
         CREATE TABLE child (\
             id INTEGER PRIMARY KEY,\
             parent_id INTEGER REFERENCES parent,\
             payload TEXT\
         ); \
         INSERT INTO parent (id) VALUES (7); \
         INSERT INTO child (id, parent_id, payload) VALUES (1, 7, 'kept')",
    ))?;
    let violations = connection.execute(&QueryRequest::new("PRAGMA main.foreign_key_check"))?;
    assert!(
        violations.rows.is_empty(),
        "fixture must start with no global foreign key violations"
    );
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
        "rebuild preparation must remain read-only"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_resolves_composite_implicit_parent_primary_keys_in_pk_order()
-> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE parent (a INTEGER, b INTEGER, PRIMARY KEY (b, a)); \
         CREATE TABLE child (\
             id INTEGER PRIMARY KEY,\
             x INTEGER,\
             y INTEGER,\
             payload TEXT,\
             FOREIGN KEY (x, y) REFERENCES parent\
         ); \
         INSERT INTO parent (a, b) VALUES (10, 20); \
         INSERT INTO child (id, x, y, payload) VALUES (1, 20, 10, 'kept')",
    ))?;
    let violations = connection.execute(&QueryRequest::new("PRAGMA main.foreign_key_check"))?;
    assert!(
        violations.rows.is_empty(),
        "fixture must prove implicit composite parent lookup uses primary-key ordinal order"
    );

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
    Ok(())
}

#[test]
fn sqlite_rebuild_preserves_mixed_foreign_key_declarations_and_related_table_metadata()
-> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE parent (id INTEGER PRIMARY KEY, payload TEXT); \
         CREATE TABLE mixed (\
             id INTEGER PRIMARY KEY,\
             inline_parent INTEGER REFERENCES parent ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,\
             table_parent INTEGER,\
             duplicate_parent INTEGER,\
             payload TEXT,\
             FOREIGN KEY (table_parent) REFERENCES parent ON DELETE SET NULL,\
             FOREIGN KEY (duplicate_parent) REFERENCES parent\
         ); \
         CREATE TABLE node (\
             id INTEGER PRIMARY KEY,\
             parent_id INTEGER REFERENCES node,\
             payload TEXT\
         ); \
         CREATE TABLE inbound (\
             id INTEGER PRIMARY KEY,\
             parent_id INTEGER REFERENCES parent\
         ) WITHOUT ROWID",
    ))?;
    let violations = connection.execute(&QueryRequest::new("PRAGMA main.foreign_key_check"))?;
    assert!(
        violations.rows.is_empty(),
        "mixed, self, and inbound fixtures must start globally foreign-key clean"
    );

    for table in ["mixed", "node", "parent"] {
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new(table),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "payload".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            })?;
        assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
        if table == "mixed" {
            assert!(plan.preview().statements.iter().any(|statement| {
                statement.contains("DEFERRABLE INITIALLY DEFERRED")
                    && statement.contains("ON DELETE CASCADE")
            }));
        }
    }
    Ok(())
}

#[test]
fn sqlite_rebuild_executes_preserving_foreign_key_timing_forms() -> Result<(), DbError> {
    for (label, timing) in [
        ("omitted timing", ""),
        ("DEFERRABLE", " DEFERRABLE"),
        (
            "DEFERRABLE INITIALLY IMMEDIATE",
            " DEFERRABLE INITIALLY IMMEDIATE",
        ),
        (
            "DEFERRABLE INITIALLY DEFERRED",
            " DEFERRABLE INITIALLY DEFERRED",
        ),
        ("NOT DEFERRABLE", " NOT DEFERRABLE"),
        (
            "NOT DEFERRABLE INITIALLY IMMEDIATE",
            " NOT DEFERRABLE INITIALLY IMMEDIATE",
        ),
        (
            "NOT DEFERRABLE INITIALLY DEFERRED",
            " NOT DEFERRABLE INITIALLY DEFERRED",
        ),
    ] {
        let (connection, _, _db_path) = connect_sqlite()?;
        connection.execute(&QueryRequest::new(format!(
            "PRAGMA foreign_keys = ON; \
             CREATE TABLE parent (id INTEGER PRIMARY KEY); \
             CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, payload TEXT, \
                FOREIGN KEY (parent_id) REFERENCES parent(id) {timing}); \
             INSERT INTO parent VALUES (7); \
             INSERT INTO child VALUES (1, 7, 'kept')"
        )))?;
        let violations = connection.execute(&QueryRequest::new("PRAGMA main.foreign_key_check"))?;
        assert!(
            violations.rows.is_empty(),
            "{label} fixture must start with valid foreign key data"
        );
        let schema_before = connection.execute(&QueryRequest::new(
            "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'child'",
        ))?;
        let before = rebuild_boundary_snapshot(&*connection, "child")?;
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
        assert!(
            plan.preview().statements.iter().any(|statement| {
                statement.starts_with("CREATE TABLE")
                    && statement.contains("FOREIGN KEY (parent_id) REFERENCES parent(id)")
                    && if timing.is_empty() {
                        !statement.contains("DEFERRABLE")
                    } else {
                        statement.contains(timing)
                    }
            }),
            "rebuild preview must preserve {label} exactly"
        );
        assert_eq!(
            schema_before.rows,
            connection
                .execute(&QueryRequest::new(
                    "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'child'",
                ))?
                .rows,
            "preparation must remain read-only for {label}"
        );
        assert_eq!(
            before,
            rebuild_boundary_snapshot(&*connection, "child")?,
            "preparation must retain the complete {label} boundary"
        );

        plan.execute()?;

        let schema = connection.execute(&QueryRequest::new(
            "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'child'",
        ))?;
        let expected_schema = format!(
            "CREATE TABLE \"child\" (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, \
             payload VARCHAR(9), FOREIGN KEY (parent_id) REFERENCES parent(id) {timing})"
        );
        assert_eq!(
            schema.rows,
            vec![vec![Value::Text(expected_schema)]],
            "execution must produce the exact requested schema for {label}"
        );
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT group_concat(metadata, ';') FROM ( \
                     SELECT printf('%d|%s|%s|%d|%s|%d|%d', cid, name, type, \"notnull\", \
                     COALESCE(dflt_value, '<NULL>'), pk, hidden) AS metadata \
                     FROM pragma_table_xinfo('child') ORDER BY cid)",
                ))?
                .rows,
            vec![vec![Value::Text(
                "0|id|INTEGER|0|<NULL>|1|0;1|parent_id|INTEGER|1|<NULL>|0|0;2|payload|VARCHAR(9)|0|<NULL>|0|0".to_string(),
            )]],
            "execution must retain exact column metadata and the requested payload type for {label}"
        );
        assert!(
            schema.rows[0][0]
                .to_string()
                .contains("FOREIGN KEY (parent_id) REFERENCES parent(id)"),
            "execution must preserve the foreign-key relationship for {label}"
        );
        if timing.is_empty() {
            assert!(
                !schema.rows[0][0].to_string().contains("DEFERRABLE"),
                "execution must preserve omitted timing for {label}"
            );
        } else {
            assert!(
                schema.rows[0][0].to_string().contains(timing),
                "execution must preserve {label}"
            );
        }
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT id, parent_id, payload FROM main.child ORDER BY id",
                ))?
                .rows,
            vec![vec![
                Value::Int(1),
                Value::Int(7),
                Value::Text("kept".to_string()),
            ]],
            "execution must preserve rows for {label}"
        );
        assert_eq!(
            before.foreign_keys,
            connection
                .execute(&QueryRequest::new("PRAGMA foreign_keys"))?
                .rows,
            "execution must restore the foreign-key setting for {label}"
        );
        assert!(
            connection
                .execute(&QueryRequest::new("PRAGMA main.foreign_key_check"))?
                .rows
                .is_empty(),
            "execution must remain foreign-key clean for {label}"
        );
    }
    Ok(())
}

#[test]
fn sqlite_rebuild_preview_declares_ordered_lifecycle_and_exact_streamed_comparison_intent()
-> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, p TEXT, a TEXT, b TEXT, retained TEXT, UNIQUE (id)); \
         CREATE INDEX t_retained_desc ON t(retained DESC); \
         INSERT INTO t VALUES (1, 'payload', 'drop-a', 'drop-b', 'keep')",
    ))?;
    let before = rebuild_boundary_snapshot(&*connection, "t")?;
    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("t"),
            operations: vec![
                TableAlterOperation::AlterColumn {
                    name: "p".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: Some(false),
                    default: Some(OwnedDefaultSpec::Set("NULL".to_string())),
                },
                TableAlterOperation::DropColumn {
                    name: "a".to_string(),
                },
                TableAlterOperation::DropColumn {
                    name: "b".to_string(),
                },
            ],
            expected_before: Vec::new(),
        })?;
    let statements = &plan.preview().statements;
    let position = |needle: &str| {
        statements
            .iter()
            .position(|statement| statement.contains(needle))
            .unwrap_or_else(|| panic!("preview must declare {needle}"))
    };
    let context = position("verify usable connection, autocommit, safe settings, source freshness");
    let save_foreign_keys = position("save foreign_keys; set foreign_keys = OFF");
    let begin = position("BEGIN IMMEDIATE;");
    let under_lock =
        position("under lock revalidate freshness and baseline foreign-key cleanliness");
    let create = statements
        .iter()
        .position(|statement| statement.starts_with("CREATE TABLE main.\"__dbflux_rebuild_t\""))
        .expect("preview must create the private replacement table");
    let copy = statements
        .iter()
        .position(|statement| statement.starts_with("INSERT INTO main.\"__dbflux_rebuild_t\""))
        .expect("preview must declare the explicit copy projection");
    let comparison =
        position("exact streamed comparison of ordered source/replacement projections");
    let finalization = position("statement finalization completes before destructive DROP");
    let drop_original = position("DROP TABLE main.\"t\";");
    let rename = position("ALTER TABLE main.\"__dbflux_rebuild_t\" RENAME TO \"t\";");
    let restore_index = position("restore exact index main.t_retained_desc");
    let final_checks = position("verify final schema, explicit indexes, non-target catalog");
    let commit = position("COMMIT;");
    let restore_foreign_keys = position("after transaction ends, restore prior foreign_keys");
    assert!(statements[context].contains("private replacement-name availability"));
    assert!(statements[save_foreign_keys].contains("foreign_keys readback"));
    assert!(statements[under_lock].contains("freshness"));
    assert!(statements[under_lock].contains("baseline foreign-key cleanliness"));
    assert_eq!(
        statements[copy],
        "INSERT INTO main.\"__dbflux_rebuild_t\" (\"id\", \"p\", \"retained\") SELECT \"id\", \"p\", \"retained\" FROM main.\"t\";"
    );
    for forbidden in ["CAST(", "COALESCE(", "OR IGNORE", "OR REPLACE"] {
        assert!(
            !statements[copy].contains(forbidden),
            "public copy projection must not use {forbidden}"
        );
    }
    assert!(statements[comparison].contains("ValueRef"));
    for policy in [
        "rowcount=true",
        "integer identity=true",
        "storage class=true",
        "exact integer=true",
        "REAL bits=true",
        "TEXT/BLOB bytes=true",
        "invalid UTF-8/NUL=true",
    ] {
        assert!(
            statements[comparison].contains(policy),
            "comparison must declare {policy}"
        );
    }
    assert!(statements[final_checks].contains("no private-name leak"));
    assert!(statements[final_checks].contains("foreign_key_check"));
    assert!(statements[final_checks].contains("one-row integrity_check result of ok"));
    assert!(
        context < save_foreign_keys
            && save_foreign_keys < begin
            && begin < under_lock
            && under_lock < create
            && create < copy
            && copy < comparison
            && comparison < finalization
            && finalization < drop_original
            && drop_original < rename
            && rename < restore_index
            && restore_index < final_checks
            && final_checks < commit
            && commit < restore_foreign_keys,
        "rebuild lifecycle intent must declare the complete ordered chain"
    );
    let failure_intent = statements
        .iter()
        .find(|statement| statement.starts_with("INTENT FAILURE:"))
        .expect("preview must state failure handling intent");
    for concept in [
        "rollback",
        "autocommit",
        "restore foreign_keys",
        "cleanup failure",
        "quarantine",
        "commit certainty",
    ] {
        assert!(
            failure_intent.contains(concept),
            "failure intent must declare {concept}"
        );
    }
    assert!(plan.preview().warnings.iter().any(|warning| {
        warning.contains("NOT EXECUTABLE") && warning.contains("illustrative lifecycle intent")
    }));
    assert!(plan.preview().warnings.iter().any(|warning| {
        warning.contains("Preparation remains read-only")
            && warning.contains("exact streamed ValueRef comparison")
    }));
    assert_eq!(
        before,
        rebuild_boundary_snapshot(&*connection, "t")?,
        "preparation must retain the complete boundary"
    );

    plan.execute()?;

    assert_eq!(
        connection
            .execute(&QueryRequest::new("SELECT id, p, retained FROM main.t"))?
            .rows,
        vec![vec![
            Value::Int(1),
            Value::Text("payload".to_string()),
            Value::Text("keep".to_string())
        ]],
        "execution must preserve the retained projection"
    );
    assert_eq!(
        connection
            .execute(&QueryRequest::new(
                "SELECT type, \"notnull\", dflt_value FROM pragma_table_xinfo('t') WHERE name = 'p'",
            ))?
            .rows,
        vec![vec![
            Value::Text("VARCHAR(9)".to_string()),
            Value::Int(1),
            Value::Text("NULL".to_string()),
        ]],
        "execution must apply the selected payload alteration"
    );
    assert_eq!(
        connection
            .execute(&QueryRequest::new(
                "SELECT COUNT(*) FROM pragma_table_xinfo('t') WHERE name IN ('a', 'b')",
            ))?
            .rows,
        vec![vec![Value::Int(0)]],
        "execution must apply only the selected drops"
    );
    assert_eq!(
        connection
            .execute(&QueryRequest::new(
                "SELECT sql FROM main.sqlite_master WHERE type = 'index' AND name = 't_retained_desc'",
            ))?
            .rows,
        vec![vec![Value::Text(
            "CREATE INDEX t_retained_desc ON t(retained DESC)".to_string(),
        )]],
        "execution must retain the unaffected explicit index"
    );
    assert_eq!(
        before.foreign_keys,
        connection
            .execute(&QueryRequest::new("PRAGMA foreign_keys"))?
            .rows,
        "execution must restore the foreign-key setting"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_prepare_and_execute_preserve_complete_boundaries() -> Result<(), DbError> {
    for (label, foreign_keys, insert) in [
        ("empty-fk-on", "ON", ""),
        (
            "populated-fk-on",
            "ON",
            "INSERT INTO t(id, legacy, a, b, retained) VALUES (-7, 'old', 'drop-a', 'drop-b', 'keep');",
        ),
        ("empty-fk-off", "OFF", ""),
        (
            "populated-fk-off",
            "OFF",
            "INSERT INTO t(id, legacy, a, b, retained) VALUES (-7, 'old', 'drop-a', 'drop-b', 'keep');",
        ),
    ] {
        let (connection, _, _db_path) = connect_sqlite()?;
        connection.execute(&QueryRequest::new(format!(
            "PRAGMA foreign_keys = {foreign_keys}; \
             CREATE TABLE t(id INTEGER PRIMARY KEY, legacy TEXT DEFAULT 'NULL', a TEXT, b TEXT, retained TEXT NOT NULL DEFAULT ('keep'), UNIQUE (id)); \
             CREATE TABLE main_rebuild_boundary(marker TEXT); \
             INSERT INTO main_rebuild_boundary VALUES ('main-sentinel'); \
             CREATE TEMP TABLE temp_rebuild_boundary(marker TEXT); \
             INSERT INTO temp_rebuild_boundary VALUES ('sentinel'); \
             {insert}"
        )))?;
        let before = rebuild_boundary_snapshot(&*connection, "t")?;
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new("t"),
                operations: vec![
                    TableAlterOperation::AlterColumn {
                        name: "legacy".to_string(),
                        new_type: Some("VARCHAR(32)".to_string()),
                        nullable: Some(false),
                        default: Some(OwnedDefaultSpec::Set("NULL".to_string())),
                    },
                    TableAlterOperation::DropColumn {
                        name: "a".to_string(),
                    },
                    TableAlterOperation::DropColumn {
                        name: "b".to_string(),
                    },
                ],
                expected_before: Vec::new(),
            })?;
        let create = plan
            .preview()
            .statements
            .iter()
            .find(|statement| statement.starts_with("CREATE TABLE"))
            .expect("rebuild lifecycle must include the rewritten CREATE TABLE");
        assert!(create.contains("legacy VARCHAR(32) NOT NULL DEFAULT NULL"));
        assert!(create.contains("retained TEXT NOT NULL DEFAULT ('keep')"));
        assert!(!create.contains(" a TEXT") && !create.contains(" b TEXT"));
        assert!(plan.preview().statements.iter().any(|statement| {
            statement.starts_with("INSERT INTO")
                && statement.contains("\"id\", \"legacy\", \"retained\"")
        }));
        assert_eq!(
            before,
            rebuild_boundary_snapshot(&*connection, "t")?,
            "prepare must retain the complete {label} boundary"
        );

        plan.execute()?;

        let after = rebuild_boundary_snapshot(&*connection, "t")?;
        let non_target_catalog = |catalog: &[Vec<Value>]| {
            catalog
                .iter()
                .filter(|entry| !matches!(entry.get(2), Some(Value::Text(table)) if table == "t"))
                .cloned()
                .collect::<Vec<_>>()
        };
        assert_eq!(
            non_target_catalog(&before.main_catalog),
            non_target_catalog(&after.main_catalog),
            "execution must preserve the complete unaffected MAIN catalog for {label}"
        );
        assert_eq!(
            before.temp_catalog, after.temp_catalog,
            "execution must preserve the complete TEMP catalog for {label}"
        );
        assert_eq!(
            before.databases, after.databases,
            "execution must retain databases for {label}"
        );
        assert_eq!(
            before.deferred_foreign_keys, after.deferred_foreign_keys,
            "execution must retain deferred foreign-key settings for {label}"
        );
        assert_ne!(
            before.main_schema_version, after.main_schema_version,
            "the MAIN schema version must advance for the requested delta in {label}"
        );
        assert_eq!(
            before.temp_schema_version, after.temp_schema_version,
            "the TEMP schema version must not change for {label}"
        );
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT group_concat(metadata, ';') FROM ( \
                     SELECT printf('%d|%s|%s|%d|%s|%d|%d', cid, name, type, \"notnull\", \
                     COALESCE(dflt_value, '<NULL>'), pk, hidden) AS metadata \
                     FROM pragma_table_xinfo('t') ORDER BY cid)",
                ))?
                .rows,
            vec![vec![Value::Text(
                "0|id|INTEGER|0|<NULL>|1|0;1|legacy|VARCHAR(32)|1|NULL|0|0;2|retained|TEXT|1|'keep'|0|0".to_string(),
            )]],
            "execution must retain exact column definitions and apply only the requested target delta for {label}"
        );
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT name, \"unique\", origin, partial FROM pragma_index_list('t') ORDER BY name",
                ))?
                .rows,
            vec![vec![
                Value::Text("sqlite_autoindex_t_1".to_string()),
                Value::Int(1),
                Value::Text("u".to_string()),
                Value::Int(0),
            ]],
            "execution must retain UNIQUE index metadata for {label}"
        );

        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT type, \"notnull\", dflt_value FROM pragma_table_xinfo('t') WHERE name = 'legacy'",
                ))?
                .rows,
            vec![vec![
                Value::Text("VARCHAR(32)".to_string()),
                Value::Int(1),
                Value::Text("NULL".to_string()),
            ]],
            "execution must apply the selected alteration for {label}"
        );
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT COUNT(*) FROM pragma_table_xinfo('t') WHERE name IN ('a', 'b')",
                ))?
                .rows,
            vec![vec![Value::Int(0)]],
            "execution must apply selected drops for {label}"
        );
        let expected_rows = if insert.is_empty() {
            Vec::new()
        } else {
            vec![vec![
                Value::Int(-7),
                Value::Text("old".to_string()),
                Value::Text("keep".to_string()),
            ]]
        };
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT id, legacy, retained FROM main.t",
                ))?
                .rows,
            expected_rows,
            "execution must preserve retained rows for {label}"
        );
        assert_eq!(
            before.foreign_keys,
            connection
                .execute(&QueryRequest::new("PRAGMA foreign_keys"))?
                .rows,
            "execution must restore the foreign-key setting for {label}"
        );
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT marker FROM main.main_rebuild_boundary",
                ))?
                .rows,
            vec![vec![Value::Text("main-sentinel".to_string())]],
            "execution must preserve the unaffected MAIN sentinel for {label}"
        );
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT marker FROM temp.temp_rebuild_boundary",
                ))?
                .rows,
            vec![vec![Value::Text("sentinel".to_string())]],
            "execution must preserve the TEMP sentinel for {label}"
        );
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT COUNT(*) FROM main.sqlite_master WHERE name = '__dbflux_rebuild_t'",
                ))?
                .rows,
            vec![vec![Value::Int(0)]],
            "execution must not leak its private replacement table for {label}"
        );
    }
    Ok(())
}

#[test]
fn sqlite_native_drop_rolls_back_all_selected_columns_when_later_drop_is_rejected()
-> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    for statement in [
        "CREATE TABLE people (id INTEGER PRIMARY KEY, first_drop TEXT, second_drop TEXT, retained TEXT)",
        "INSERT INTO people VALUES (1, 'first', 'second', 'kept')",
        "CREATE VIEW second_drop_view AS SELECT second_drop FROM people",
    ] {
        connection.execute(&QueryRequest::new(statement))?;
    }

    let schema_before = connection.execute(&QueryRequest::new(
        "SELECT type, name, sql FROM main.sqlite_master ORDER BY type, name",
    ))?;
    let data_before = connection.execute(&QueryRequest::new(
        "SELECT id, first_drop, second_drop, retained FROM main.people",
    ))?;
    let foreign_keys_before = connection.execute(&QueryRequest::new("PRAGMA foreign_keys"))?;

    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("people"),
            operations: vec![
                TableAlterOperation::DropColumn {
                    name: "first_drop".to_string(),
                },
                TableAlterOperation::DropColumn {
                    name: "second_drop".to_string(),
                },
            ],
            expected_before: Vec::new(),
        })?;

    let error = plan
        .execute()
        .expect_err("SQLite must reject the later view-dependent DROP at execution");
    assert!(
        error.to_string().contains("rolled back"),
        "native execution must report the rollback: {error}"
    );
    for column in ["first_drop", "second_drop", "retained"] {
        assert_eq!(
            connection
                .execute(&QueryRequest::new(format!(
                    "SELECT COUNT(*) FROM pragma_table_info('people') WHERE name = '{column}'"
                )))?
                .rows[0][0],
            Value::Int(1),
            "the failed later DROP must roll back every selected earlier column"
        );
    }
    assert_eq!(
        schema_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT type, name, sql FROM main.sqlite_master ORDER BY type, name",
            ))?
            .rows,
        "the failed native transaction must restore the complete main schema"
    );
    assert_eq!(
        data_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT id, first_drop, second_drop, retained FROM main.people",
            ))?
            .rows,
        "the failed native transaction must restore the complete table data"
    );
    assert_eq!(
        foreign_keys_before.rows,
        connection
            .execute(&QueryRequest::new("PRAGMA foreign_keys"))?
            .rows,
        "native execution must leave connection settings unchanged on rollback"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_executes_payload_alter_and_independent_drop_preserving_data_index_and_fk_state()
-> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "PRAGMA foreign_keys = ON;
         CREATE TABLE target (
            id INTEGER PRIMARY KEY,
            payload TEXT NOT NULL,
            retained TEXT NOT NULL,
            obsolete TEXT
         );
         CREATE INDEX target_retained_index ON target(retained);
         INSERT INTO target (id, payload, retained, obsolete) VALUES
            (-7, 'first payload', 'first retained', 'remove first'),
            (42, 'second payload', 'second retained', 'remove second')",
    ))?;

    let before = rebuild_boundary_snapshot(&*connection, "target")?;
    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("target"),
            operations: vec![
                TableAlterOperation::AlterColumn {
                    name: "payload".to_string(),
                    new_type: Some("VARCHAR(64)".to_string()),
                    nullable: Some(false),
                    default: Some(OwnedDefaultSpec::Set("'future'".to_string())),
                },
                TableAlterOperation::DropColumn {
                    name: "obsolete".to_string(),
                },
            ],
            expected_before: Vec::new(),
        })?;

    assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
    assert_eq!(
        before,
        rebuild_boundary_snapshot(&*connection, "target")?,
        "prepare must preserve the complete populated foreign-key-on boundary"
    );

    plan.execute()?;

    assert_eq!(
        connection
            .execute(&QueryRequest::new("PRAGMA main.table_xinfo('target')"))?
            .rows,
        vec![
            vec![
                Value::Int(0),
                Value::Text("id".to_string()),
                Value::Text("INTEGER".to_string()),
                Value::Int(0),
                Value::Null,
                Value::Int(1),
                Value::Int(0),
            ],
            vec![
                Value::Int(1),
                Value::Text("payload".to_string()),
                Value::Text("VARCHAR(64)".to_string()),
                Value::Int(1),
                Value::Text("'future'".to_string()),
                Value::Int(0),
                Value::Int(0),
            ],
            vec![
                Value::Int(2),
                Value::Text("retained".to_string()),
                Value::Text("TEXT".to_string()),
                Value::Int(1),
                Value::Null,
                Value::Int(0),
                Value::Int(0),
            ],
        ],
        "rebuild must produce the exact final target schema"
    );
    assert_eq!(
        connection
            .execute(&QueryRequest::new(
                "SELECT sql FROM main.sqlite_master \
                 WHERE type = 'index' AND name = 'target_retained_index'",
            ))?
            .rows,
        vec![vec![Value::Text(
            "CREATE INDEX target_retained_index ON target(retained)".to_string()
        )]],
        "rebuild must restore the unaffected explicit index SQL"
    );

    connection.execute(&QueryRequest::new(
        "INSERT INTO target (id, retained) VALUES (100, 'future retained')",
    ))?;
    assert_eq!(
        connection
            .execute(&QueryRequest::new(
                "SELECT id, payload, retained FROM target ORDER BY id",
            ))?
            .rows,
        vec![
            vec![
                Value::Int(-7),
                Value::Text("first payload".to_string()),
                Value::Text("first retained".to_string()),
            ],
            vec![
                Value::Int(42),
                Value::Text("second payload".to_string()),
                Value::Text("second retained".to_string()),
            ],
            vec![
                Value::Int(100),
                Value::Text("future".to_string()),
                Value::Text("future retained".to_string()),
            ],
        ],
        "rebuild must preserve retained identities and values while applying the default only to future inserts"
    );
    assert_eq!(
        before.foreign_keys,
        connection
            .execute(&QueryRequest::new("PRAGMA foreign_keys"))?
            .rows,
        "rebuild must restore the prior foreign_keys setting"
    );
    assert!(
        connection
            .execute(&QueryRequest::new("PRAGMA main.foreign_key_check"))?
            .rows
            .is_empty(),
        "rebuild must leave foreign-key clean"
    );
    assert_eq!(
        connection
            .execute(&QueryRequest::new("PRAGMA main.integrity_check"))?
            .rows,
        vec![vec![Value::Text("ok".to_string())]],
        "rebuild must leave integrity_check clean"
    );
    Ok(())
}
