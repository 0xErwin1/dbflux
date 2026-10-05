#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use dbflux_core::{
    CollectionBrowseRequest, CollectionCountRequest, CollectionRef, ConnectionProfile, DbConfig,
    DbDriver, DbError, DocumentDelete, DocumentFilter, DocumentInsert, DocumentUpdate,
    ExecutionClassification, Pagination, QueryRequest, ReadOnlyEnforcement, SchemaLoadingStrategy,
    Value,
};
use dbflux_driver_mongodb::MongoDriver;
use dbflux_test_support::containers;
use std::time::Duration;

fn connect_mongodb(uri: String) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let driver = MongoDriver::new();
    let profile = ConnectionProfile::new(
        "live-mongodb",
        DbConfig::MongoDB {
            use_uri: true,
            uri: Some(uri),
            host: String::new(),
            port: 27017,
            user: None,
            database: Some("testdb".to_string()),
            auth_database: None,
            ssl_mode: None,
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    );

    let connection =
        containers::retry_db_operation(Duration::from_secs(30), || -> Result<_, DbError> {
            let connection = driver.connect(&profile)?;
            connection.ping()?;
            Ok(connection)
        })?;

    Ok(connection)
}

// ---------------------------------------------------------------------------
// Basic connectivity
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_live_connect_ping_query_and_schema() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        let result = connection.execute(&QueryRequest::new("db.runCommand({\"ping\": 1})"))?;
        assert!(!result.rows.is_empty());

        assert_eq!(
            connection.schema_loading_strategy(),
            SchemaLoadingStrategy::LazyPerDatabase
        );

        connection.execute(&QueryRequest::new("db.test_col.insertOne({\"x\": 1})"))?;

        let databases = connection.list_databases()?;
        assert!(!databases.is_empty());

        let (handle, _) =
            connection.execute_with_handle(&QueryRequest::new("db.runCommand({\"ping\": 1})"))?;
        let cancel = connection.cancel(&handle);
        assert!(cancel.is_ok());

        let schema = connection.schema()?;
        assert!(schema.is_document());
        let _ = schema.databases();

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_query_safety_refuses_protected_mutations_before_effects() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;
        for (index, protection) in ["zero", "positive", "timeout"].iter().enumerate() {
            let query = format!(
                "db.query_safety.insertOne({{\"marker\": \"{}\"}})",
                protection
            );
            let mut request = QueryRequest::new(query);
            match index {
                0 => request.limit = Some(0),
                1 => request.limit = Some(3),
                _ => request.statement_timeout = Some(Duration::from_secs(1)),
            }
            let execution = connection.execute(&request);
            let result = connection.execute(&QueryRequest::new(format!(
                "db.query_safety.find({{\"marker\": \"{}\"}})",
                protection
            )))?;
            assert_eq!(
                result.rows.len(),
                0,
                "protected {protection} insert persisted {} rows before refusal check",
                result.rows.len()
            );
            assert!(
                matches!(execution, Err(DbError::NotSupported(_))),
                "protected {protection} insert must be refused"
            );
        }

        connection.execute(&QueryRequest::new(
            "db.query_safety.insertOne({\"marker\": \"control\"})",
        ))?;
        let result = connection.execute(&QueryRequest::new(
            "db.query_safety.find({\"marker\": \"control\"})",
        ))?;
        assert_eq!(result.rows.len(), 1);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Schema introspection
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_schema_introspection() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        connection.execute(&QueryRequest::new(
            "db.users.insertMany([{\"name\": \"alice\", \"age\": 30}, {\"name\": \"bob\", \"age\": 25}])",
        ))?;
        connection.execute(&QueryRequest::new(
            "db.orders.insertOne({\"user\": \"alice\", \"amount\": 42.5})",
        ))?;

        let databases = connection.list_databases()?;
        assert!(databases.iter().any(|d| d.name == "testdb"));

        let db_schema = connection.schema_for_database("testdb")?;
        assert!(!db_schema.tables.is_empty());

        let collection_names: Vec<&str> =
            db_schema.tables.iter().map(|t| t.name.as_str()).collect();
        assert!(collection_names.contains(&"users"));
        assert!(collection_names.contains(&"orders"));

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Document CRUD
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_document_crud() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        let insert = DocumentInsert::one(
            "crud_test".to_string(),
            serde_json::json!({"name": "alice", "value": 42}),
        )
        .with_database("testdb".to_string());
        let insert_result = connection.insert_document(&insert)?;
        assert_eq!(insert_result.affected_rows, 1);

        let insert_many = DocumentInsert::many(
            "crud_test".to_string(),
            vec![
                serde_json::json!({"name": "bob", "value": 10}),
                serde_json::json!({"name": "charlie", "value": 20}),
            ],
        )
        .with_database("testdb".to_string());
        let insert_many_result = connection.insert_document(&insert_many)?;
        assert_eq!(insert_many_result.affected_rows, 2);

        let update = DocumentUpdate::new(
            "crud_test".to_string(),
            DocumentFilter::new(serde_json::json!({"name": "alice"})),
            serde_json::json!({"$set": {"value": 99}}),
        )
        .with_database("testdb".to_string());
        let update_result = connection.update_document(&update)?;
        assert_eq!(update_result.affected_rows, 1);

        let result = connection.execute(&QueryRequest::new(
            "db.crud_test.find({\"name\": \"alice\"})",
        ))?;
        assert_eq!(result.rows.len(), 1);

        let delete = DocumentDelete::new(
            "crud_test".to_string(),
            DocumentFilter::new(serde_json::json!({"name": "alice"})),
        )
        .with_database("testdb".to_string());
        let delete_result = connection.delete_document(&delete)?;
        assert_eq!(delete_result.affected_rows, 1);

        let delete_many = DocumentDelete::new(
            "crud_test".to_string(),
            DocumentFilter::new(serde_json::json!({})),
        )
        .with_database("testdb".to_string())
        .many();
        let delete_many_result = connection.delete_document(&delete_many)?;
        assert_eq!(delete_many_result.affected_rows, 2);

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Browse and count collection
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_browse_and_count_collection() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        let docs: Vec<serde_json::Value> = (1..=25)
            .map(|i| serde_json::json!({"name": format!("item_{}", i), "index": i}))
            .collect();
        let insert = DocumentInsert::many("browse_test".to_string(), docs)
            .with_database("testdb".to_string());
        connection.insert_document(&insert)?;

        let collection_ref = CollectionRef::new("testdb", "browse_test");

        let count =
            connection.count_collection(&CollectionCountRequest::new(collection_ref.clone()))?;
        assert_eq!(count, 25);

        let filtered_count = connection.count_collection(
            &CollectionCountRequest::new(collection_ref.clone())
                .with_filter(serde_json::json!({"index": {"$lte": 10}})),
        )?;
        assert_eq!(filtered_count, 10);

        let page1 = connection.browse_collection(
            &CollectionBrowseRequest::new(collection_ref.clone()).with_pagination(
                Pagination::Offset {
                    limit: 10,
                    offset: 0,
                },
            ),
        )?;
        assert_eq!(page1.rows.len(), 10);

        let page2 = connection.browse_collection(
            &CollectionBrowseRequest::new(collection_ref.clone()).with_pagination(
                Pagination::Offset {
                    limit: 10,
                    offset: 10,
                },
            ),
        )?;
        assert_eq!(page2.rows.len(), 10);

        let filtered = connection.browse_collection(
            &CollectionBrowseRequest::new(collection_ref)
                .with_filter(serde_json::json!({"name": "item_5"}))
                .with_pagination(Pagination::Offset {
                    limit: 100,
                    offset: 0,
                }),
        )?;
        assert_eq!(filtered.rows.len(), 1);

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Cancel supported
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_cancel_returns_ok() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        let (handle, _) =
            connection.execute_with_handle(&QueryRequest::new("db.runCommand({\"ping\": 1})"))?;
        let cancel = connection.cancel(&handle);
        assert!(cancel.is_ok());

        assert!(connection.key_value_api().is_none());

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Multi-statement script execution (Phase 3 / mongo-js-script-runtime)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_script_runs_multiple_statements_and_yields_per_statement_results() -> Result<(), DbError>
{
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        let script = "db.script_users.insertOne({name: 'a'}); \
                       db.script_users.insertOne({name: 'b'}); \
                       db.script_users.find({});";

        let result = connection.execute(
            &QueryRequest::new(script).with_confirmed_ceiling(ExecutionClassification::Write),
        )?;

        assert_eq!(
            result.result_set_count(),
            3,
            "three statements must yield three result sets"
        );

        let ledger = result
            .metadata_extra
            .as_ref()
            .and_then(|extra| extra.get("script_operations"))
            .expect("script_operations must be present")
            .as_array()
            .expect("script_operations must be a JSON array");
        assert_eq!(ledger.len(), 3);

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_script_delete_many_behind_a_loop_is_classified_destructive_end_to_end()
-> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        connection.execute(&QueryRequest::new(
            "db.script_loop.insertMany([{\"a\": 1}, {\"a\": 2}])",
        ))?;

        let script = "for (let i = 0; i < 1; i++) { db.script_loop.deleteMany({}); }";

        // Under a Read ceiling, the loop-reached deleteMany must abort before
        // it ever reaches the server (T11's adversarial governance case).
        let blocked = connection.execute(
            &QueryRequest::new(script).with_confirmed_ceiling(ExecutionClassification::Read),
        )?;
        let failure = blocked
            .metadata_extra
            .as_ref()
            .and_then(|extra| extra.get("script_failure"))
            .expect("a Read ceiling must abort a Destructive statement reached inside a loop");
        assert!(
            failure["message"]
                .as_str()
                .unwrap_or_default()
                .contains("Destructive")
        );

        let remaining =
            connection.execute(&QueryRequest::new("db.script_loop.countDocuments({})"))?;
        assert_eq!(
            remaining.rows[0][0],
            Value::Int(2),
            "the blocked deleteMany must never have reached the server"
        );

        // Under a Destructive ceiling the same script proceeds.
        let allowed = connection.execute(
            &QueryRequest::new(script).with_confirmed_ceiling(ExecutionClassification::Destructive),
        )?;
        assert!(
            allowed
                .metadata_extra
                .as_ref()
                .and_then(|extra| extra.get("script_failure"))
                .is_none()
        );

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_script_computed_method_name_is_classified_destructive_under_read_ceiling()
-> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        // The method name is string-concatenation-computed at runtime, so no
        // static grep for "deleteMany" would find it — dispatch-boundary
        // classification must still catch it (T11 threat-matrix row).
        let script = "const m = \"deleteM\" + \"any\"; db.script_adversarial[m]({});";

        let result = connection.execute(
            &QueryRequest::new(script).with_confirmed_ceiling(ExecutionClassification::Read),
        )?;

        let failure = result
            .metadata_extra
            .as_ref()
            .and_then(|extra| extra.get("script_failure"))
            .expect("computed deleteMany must abort under a Read ceiling");
        assert!(
            failure["message"]
                .as_str()
                .unwrap_or_default()
                .contains("Destructive")
        );

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_script_mid_script_driver_failure_stops_later_statements() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        let script = "db.script_mid_fail.insertOne({_id: 1, a: 1}); \
                       db.script_mid_fail.insertOne({_id: 1, a: 2}); \
                       db.script_mid_fail.insertOne({_id: 2, a: 3});";

        let result = connection.execute(
            &QueryRequest::new(script).with_confirmed_ceiling(ExecutionClassification::Write),
        )?;

        // Statement 2 fails on a duplicate `_id`; statement 3 must never
        // dispatch, so only statement 1's result set is present.
        assert_eq!(
            result.result_set_count(),
            1,
            "only the first statement must have succeeded"
        );

        let failure = result
            .metadata_extra
            .as_ref()
            .and_then(|extra| extra.get("script_failure"))
            .expect("the duplicate-key error must be recorded as a script failure");
        assert_eq!(failure["index"], 1);

        let after =
            connection.execute(&QueryRequest::new("db.script_mid_fail.countDocuments({})"))?;
        assert_eq!(
            after.rows[0][0],
            Value::Int(1),
            "statement 3 must never have run"
        );

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_script_single_statement_behaves_identically_to_stage_one() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        // A single `db.` statement must still route through the stage-1
        // parser, unaffected by the script engine's existence.
        connection.execute(&QueryRequest::new(
            "db.script_stage_one.insertOne({\"a\": 1})",
        ))?;
        let result = connection.execute(&QueryRequest::new("db.script_stage_one.find({})"))?;

        assert!(!result.rows.is_empty());
        assert!(
            result
                .metadata_extra
                .as_ref()
                .and_then(|extra| extra.get("script_operations"))
                .is_none(),
            "a single statement must not go through the script ledger"
        );

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_read_only_requests_refuse_merge_and_insert_and_leave_the_data_unchanged()
-> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        connection.execute(&QueryRequest::new(
            "db.read_only_source.insertMany([{\"a\": 1}, {\"a\": 2}])",
        ))?;

        let read_only = |query: &str| {
            QueryRequest::new(query)
                .with_confirmed_ceiling(ExecutionClassification::Read)
                .with_read_only(ReadOnlyEnforcement::Required)
        };

        let count = |collection: &str| -> Result<Value, DbError> {
            let result = connection.execute(&QueryRequest::new(format!(
                "db.{collection}.countDocuments({{}})"
            )))?;
            Ok(result.rows[0][0].clone())
        };

        for query in [
            "db.read_only_source.aggregate([{\"$merge\": {\"into\": \"read_only_target\"}}])",
            "db.read_only_source.insertOne({\"a\": 3})",
        ] {
            let refused = connection.execute(&read_only(query));
            assert!(
                matches!(&refused, Err(DbError::QueryFailed(error)) if error.to_string().contains("ceiling")),
                "{query} must be refused by the read ceiling, got {refused:?}"
            );
        }

        for script in [
            "var n = 1; db.read_only_source.aggregate([{$facet: {copy: [{$merge: 'read_only_target'}]}}]);",
            "var n = 1; db.read_only_source.aggregate([{$merge: {into: 'read_only_target'}}]);",
            "var n = 1; db.read_only_source.insertOne({a: 3});",
        ] {
            let result = connection.execute(&read_only(script))?;
            let failure = result
                .metadata_extra
                .as_ref()
                .and_then(|extra| extra.get("script_failure"))
                .unwrap_or_else(|| panic!("{script} must stop under a read-only request"));
            assert!(
                failure["message"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("ceiling"),
                "{script}: {failure}"
            );
        }

        assert_eq!(count("read_only_source")?, Value::Int(2));
        assert_eq!(count("read_only_target")?, Value::Int(0));

        let read = connection.execute(&read_only("db.read_only_source.find({})"))?;
        assert_eq!(read.rows.len(), 2, "a read must still run read-only");

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Document field patches, replacement, reads by identity
// ---------------------------------------------------------------------------

/// Ids of every document of `collection`, in natural order.
fn document_ids(
    connection: &dyn dbflux_core::Connection,
    collection: &str,
) -> Result<Vec<Value>, DbError> {
    let page = connection.browse_collection(
        &CollectionBrowseRequest::new(CollectionRef::new("testdb", collection)).with_pagination(
            Pagination::Offset {
                limit: 1_000,
                offset: 0,
            },
        ),
    )?;
    let id_index = page
        .columns
        .iter()
        .position(|column| column.name == "_id")
        .expect("_id column");

    Ok(page.rows.iter().map(|row| row[id_index].clone()).collect())
}

/// Creates the field at `path` as a Decimal128 through a field patch, which is
/// the only typed write path (the shell parser reads plain JSON numbers). The
/// field must not exist yet: a patch keeps the type an existing field has.
fn set_decimal(
    connection: &dyn dbflux_core::Connection,
    collection: &str,
    id: &Value,
    path: &str,
    decimal: &str,
) -> Result<(), DbError> {
    connection.patch_document(&dbflux_core::DocumentPatchRequest {
        collection: CollectionRef::new("testdb", collection),
        identity: vec![("_id".to_string(), id.clone())],
        patch: dbflux_core::DocumentPatch {
            set: vec![(
                dbflux_core::parse_field_path(path),
                Value::Decimal(decimal.to_string()),
            )],
            unset: Vec::new(),
        },
    })?;

    Ok(())
}

fn seed_product(
    connection: &dyn dbflux_core::Connection,
    collection: &str,
) -> Result<Value, DbError> {
    connection.insert_document(
        &DocumentInsert::one(
            collection.to_string(),
            serde_json::json!({
                "sku": "CAT-00735",
                "price": {"currency": "USD"},
                "stock": 3,
                "legacy": true
            }),
        )
        .with_database("testdb".to_string()),
    )?;

    let id = document_ids(connection, collection)?
        .into_iter()
        .next()
        .expect("seeded document");
    set_decimal(connection, collection, &id, "price.amount", "405.00")?;

    Ok(id)
}

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_patch_document_sets_and_unsets_paths_keeping_types() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;
        let id = seed_product(connection.as_ref(), "patch_test")?;
        let collection = CollectionRef::new("testdb", "patch_test");
        let identity = vec![("_id".to_string(), id.clone())];

        let patch = dbflux_core::DocumentPatch {
            set: vec![(
                dbflux_core::parse_field_path("price.amount"),
                Value::Decimal("119.00".into()),
            )],
            unset: vec![dbflux_core::parse_field_path("legacy")],
        };
        let result = connection.patch_document(&dbflux_core::DocumentPatchRequest {
            collection: collection.clone(),
            identity: identity.clone(),
            patch,
        })?;
        assert_eq!(result.affected_rows, 1);

        let current = connection
            .fetch_document(&dbflux_core::DocumentFetchRequest {
                collection,
                identity,
            })?
            .expect("document still exists");

        assert_eq!(
            dbflux_core::value_at_path(&current, &dbflux_core::parse_field_path("price.amount")),
            Some(&Value::Decimal("119.00".into()))
        );
        assert_eq!(
            dbflux_core::value_at_path(&current, &dbflux_core::parse_field_path("price.currency")),
            Some(&Value::Text("USD".into()))
        );
        assert_eq!(
            dbflux_core::value_at_path(&current, &dbflux_core::parse_field_path("legacy")),
            None
        );

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_replace_document_keeps_identity_and_drops_missing_fields() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;
        let id = seed_product(connection.as_ref(), "replace_test")?;
        let collection = CollectionRef::new("testdb", "replace_test");
        let identity = vec![("_id".to_string(), id.clone())];

        let mut fields = std::collections::BTreeMap::new();
        fields.insert("_id".to_string(), id);
        fields.insert("sku".to_string(), Value::Text("CAT-00735".into()));
        fields.insert("stock".to_string(), Value::Int(9));

        connection.replace_document(&dbflux_core::DocumentReplaceRequest {
            collection: collection.clone(),
            identity: identity.clone(),
            document: Value::Document(fields),
        })?;

        let current = connection
            .fetch_document(&dbflux_core::DocumentFetchRequest {
                collection,
                identity,
            })?
            .expect("document still exists");

        let Value::Document(current_fields) = current else {
            panic!("expected a document");
        };
        assert_eq!(current_fields.get("stock"), Some(&Value::Int(9)));
        assert!(!current_fields.contains_key("price"));
        assert!(!current_fields.contains_key("legacy"));

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_fetch_document_reports_a_deleted_document_and_patch_refuses_it() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;
        let id = seed_product(connection.as_ref(), "fetch_test")?;
        let collection = CollectionRef::new("testdb", "fetch_test");
        let identity = vec![("_id".to_string(), id)];

        connection.execute(&QueryRequest::new("db.fetch_test.deleteMany({})"))?;

        let current = connection.fetch_document(&dbflux_core::DocumentFetchRequest {
            collection: collection.clone(),
            identity: identity.clone(),
        })?;
        assert!(current.is_none());

        let patched = connection.patch_document(&dbflux_core::DocumentPatchRequest {
            collection,
            identity,
            patch: dbflux_core::DocumentPatch {
                set: vec![(vec!["stock".to_string()], Value::Int(1))],
                unset: Vec::new(),
            },
        });
        assert!(patched.is_err());

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Query slots, schema sampling, count estimate
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_browse_applies_projection_and_sort() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        let docs: Vec<serde_json::Value> = (1..=5)
            .map(|i| serde_json::json!({"name": format!("item_{}", i), "rank": i, "secret": "x"}))
            .collect();
        connection.insert_document(
            &DocumentInsert::many("slots_test".to_string(), docs).with_database("testdb".into()),
        )?;

        let page = connection.browse_collection(
            &CollectionBrowseRequest::new(CollectionRef::new("testdb", "slots_test"))
                .with_projection(serde_json::json!({"secret": 0}))
                .with_sort(serde_json::json!({"rank": -1})),
        )?;

        assert!(!page.columns.iter().any(|column| column.name == "secret"));
        let rank_index = page
            .columns
            .iter()
            .position(|column| column.name == "rank")
            .expect("rank column");
        assert_eq!(page.rows[0][rank_index], Value::Int(5));
        assert_eq!(page.rows[4][rank_index], Value::Int(1));

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_sample_collection_schema_reports_types_presence_and_values() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        let docs: Vec<serde_json::Value> = (0..20)
            .map(|index| {
                let mut customer = serde_json::json!({
                    "tier": if index % 3 == 0 { "team" } else { "free" }
                });
                if index % 2 == 0 {
                    customer["company"] = serde_json::json!("Northwind");
                }
                let mut document = serde_json::json!({
                    "customer": customer,
                    "items": [{"sku": "a"}]
                });
                if index == 0 {
                    document["total"] = serde_json::json!(12.5);
                }
                document
            })
            .collect();
        connection.insert_document(
            &DocumentInsert::many("schema_test".to_string(), docs).with_database("testdb".into()),
        )?;

        for (index, id) in document_ids(connection.as_ref(), "schema_test")?
            .iter()
            .enumerate()
            .skip(1)
        {
            set_decimal(
                connection.as_ref(),
                "schema_test",
                id,
                "total",
                &format!("{index}.00"),
            )?;
        }

        let sample =
            connection.sample_collection_schema(&dbflux_core::CollectionSchemaRequest::new(
                CollectionRef::new("testdb", "schema_test"),
                100,
            ))?;

        assert_eq!(sample.sampled_documents, 20);
        assert_eq!(sample.total_documents, Some(20));
        assert_eq!(
            sample.fields.first().map(|field| field.path.as_str()),
            Some("_id")
        );

        let company = sample.field("customer.company").expect("company stats");
        assert_eq!(company.presence, 10);

        let total = sample.field("total").expect("total stats");
        assert_eq!(total.dominant_type(), Some("Decimal128"));
        assert!(total.has_mixed_types());

        let tier = sample.field("customer.tier").expect("tier stats");
        assert!(matches!(
            tier.summary,
            dbflux_core::FieldValueSummary::TopValues(_)
        ));

        assert!(sample.field("items.sku").is_some());

        let filtered =
            connection.sample_collection_schema(&dbflux_core::CollectionSchemaRequest {
                collection: CollectionRef::new("testdb", "schema_test"),
                sample_size: 100,
                filter: Some(serde_json::json!({"customer.tier": "team"})),
            })?;
        assert_eq!(filtered.sampled_documents, 7);

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_aggregate_collection_runs_match_and_group_with_a_cap() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        assert!(
            connection
                .document_features()
                .contains(dbflux_core::DocumentFeatures::AGGREGATE)
        );

        let docs: Vec<serde_json::Value> = (1..=10)
            .map(|index| {
                serde_json::json!({
                    "region": if index % 2 == 0 { "north" } else { "south" },
                    "paid": index <= 8,
                    "amount": index,
                })
            })
            .collect();
        connection.insert_document(
            &DocumentInsert::many("aggregate_test".to_string(), docs)
                .with_database("testdb".into()),
        )?;

        let collection = CollectionRef::new("testdb", "aggregate_test");
        let pipeline = dbflux_core::parse_aggregate_pipeline(
            "[{ $match: { paid: true } },
              { $group: { _id: '$region', orders: { $sum: 1 }, total: { $sum: '$amount' } } },
              { $sort: { _id: 1 } }]",
        )
        .expect("pipeline parses");

        let result = connection.aggregate_collection(
            &dbflux_core::CollectionAggregateRequest::new(collection.clone(), pipeline.clone(), 50),
        )?;

        let column = |name: &str| {
            result
                .columns
                .iter()
                .position(|column| column.name == name)
                .unwrap_or_else(|| panic!("{name} column"))
        };
        let (id, orders, total) = (column("_id"), column("orders"), column("total"));

        assert_eq!(result.rows.len(), 2);
        assert!(!result.rows_truncated());
        assert_eq!(result.rows[0][id], Value::Text("north".to_string()));
        assert_eq!(result.rows[0][orders], Value::Int(4));
        assert_eq!(result.rows[0][total], Value::Int(20));
        assert_eq!(result.rows[1][id], Value::Text("south".to_string()));
        assert_eq!(result.rows[1][total], Value::Int(16));

        let capped = connection.aggregate_collection(
            &dbflux_core::CollectionAggregateRequest::new(collection.clone(), pipeline, 1),
        )?;
        assert_eq!(capped.rows.len(), 1);
        assert!(capped.rows_truncated(), "a cut result says so");

        let written =
            connection.aggregate_collection(&dbflux_core::CollectionAggregateRequest::new(
                collection,
                vec![
                    serde_json::json!({ "$match": { "region": "north" } }),
                    serde_json::json!({ "$out": "aggregate_out_test" }),
                ],
                50,
            ))?;
        assert!(written.rows.is_empty(), "$out returns no documents");
        assert_eq!(
            connection.count_collection(&CollectionCountRequest::new(CollectionRef::new(
                "testdb",
                "aggregate_out_test"
            )))?,
            5,
            "$out stays the last stage, so every matched document is written"
        );

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_estimate_collection_count_is_estimated_only_without_a_filter() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        let docs: Vec<serde_json::Value> =
            (1..=12).map(|i| serde_json::json!({"index": i})).collect();
        connection.insert_document(
            &DocumentInsert::many("estimate_test".to_string(), docs).with_database("testdb".into()),
        )?;

        let collection = CollectionRef::new("testdb", "estimate_test");

        let unfiltered = connection
            .estimate_collection_count(&CollectionCountRequest::new(collection.clone()))?;
        assert_eq!(unfiltered.count, 12);
        assert!(!unfiltered.exact);

        let filtered = connection.estimate_collection_count(
            &CollectionCountRequest::new(collection)
                .with_filter(serde_json::json!({"index": {"$gt": 10}})),
        )?;
        assert_eq!(filtered.count, 2);
        assert!(filtered.exact);

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_patch_and_replace_keep_numeric_widths() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        connection.insert_document(
            &DocumentInsert::one(
                "width_test".to_string(),
                serde_json::json!({"wide": 3, "ratio": 1.5}),
            )
            .with_database("testdb".to_string()),
        )?;

        let id = document_ids(connection.as_ref(), "width_test")?
            .into_iter()
            .next()
            .expect("seeded document");
        let collection = CollectionRef::new("testdb", "width_test");
        let identity = vec![("_id".to_string(), id.clone())];

        // `narrow` is new, so its type is inferred (Int32); `price` is Decimal128.
        connection.patch_document(&dbflux_core::DocumentPatchRequest {
            collection: collection.clone(),
            identity: identity.clone(),
            patch: dbflux_core::DocumentPatch {
                set: vec![(vec!["narrow".to_string()], Value::Int(1))],
                unset: Vec::new(),
            },
        })?;
        set_decimal(connection.as_ref(), "width_test", &id, "price", "405.00")?;

        let count_of_type = |field: &str, bson_type: &str| -> Result<usize, DbError> {
            let result = connection.execute(&QueryRequest::new(format!(
                "db.width_test.find({{\"{field}\": {{\"$type\": \"{bson_type}\"}}}})"
            )))?;
            Ok(result.rows.len())
        };

        connection.patch_document(&dbflux_core::DocumentPatchRequest {
            collection: collection.clone(),
            identity: identity.clone(),
            patch: dbflux_core::DocumentPatch {
                set: vec![
                    (vec!["wide".to_string()], Value::Int(7)),
                    (vec!["narrow".to_string()], Value::Int(8)),
                    (vec!["ratio".to_string()], Value::Int(2)),
                    (vec!["price".to_string()], Value::Int(119)),
                ],
                unset: Vec::new(),
            },
        })?;

        assert_eq!(count_of_type("wide", "long")?, 1, "Int64 stays Int64");
        assert_eq!(count_of_type("narrow", "int")?, 1, "Int32 stays Int32");
        assert_eq!(count_of_type("ratio", "double")?, 1, "Double stays Double");
        assert_eq!(
            count_of_type("price", "decimal")?,
            1,
            "Decimal128 stays Decimal128"
        );

        let current = connection
            .fetch_document(&dbflux_core::DocumentFetchRequest {
                collection: collection.clone(),
                identity: identity.clone(),
            })?
            .expect("document exists");
        connection.replace_document(&dbflux_core::DocumentReplaceRequest {
            collection,
            identity,
            document: current,
        })?;

        assert_eq!(
            count_of_type("wide", "long")?,
            1,
            "a replacement keeps Int64"
        );
        assert_eq!(
            count_of_type("narrow", "int")?,
            1,
            "a replacement keeps Int32"
        );
        assert_eq!(count_of_type("ratio", "double")?, 1);
        assert_eq!(count_of_type("price", "decimal")?, 1);

        Ok(())
    })
}
