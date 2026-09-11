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
    ExecutionClassification, Pagination, QueryRequest, SchemaLoadingStrategy, Value,
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
