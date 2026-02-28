use dbflux_core::{
    CollectionBrowseRequest, CollectionCountRequest, CollectionRef, ConnectionProfile, DbConfig,
    DbDriver, DbError, DocumentDelete, DocumentFilter, DocumentInsert, DocumentUpdate, Pagination,
    QueryRequest, SchemaLoadingStrategy,
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
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    );

    let connection = containers::retry_db_operation(Duration::from_secs(30), || {
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
        assert!(matches!(cancel, Err(DbError::NotSupported(_))));

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
// Cancel not supported
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_cancel_not_supported() -> Result<(), DbError> {
    containers::with_mongodb_url(|uri| {
        let connection = connect_mongodb(uri)?;

        let (handle, _) =
            connection.execute_with_handle(&QueryRequest::new("db.runCommand({\"ping\": 1})"))?;
        let cancel = connection.cancel(&handle);
        assert!(matches!(cancel, Err(DbError::NotSupported(_))));

        assert!(connection.key_value_api().is_none());

        Ok(())
    })
}
